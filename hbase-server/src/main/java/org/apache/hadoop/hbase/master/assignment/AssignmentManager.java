/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.assignment;
import org.knobinjection.runtime.KnobRuntime;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.favored.FavoredNodesPromoter;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsAssignmentManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.balancer.FavoredStochasticBalancer;
import org.apache.hadoop.hbase.master.procedure.HBCKServerCrashProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.procedure.TruncateRegionProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureInMemoryChore;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.SequenceId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * The AssignmentManager is the coordinator for region assign/unassign operations.
 * <ul>
 * <li>In-memory states of regions and servers are stored in {@link RegionStates}.</li>
 * <li>hbase:meta state updates are handled by {@link RegionStateStore}.</li>
 * </ul>
 * Regions are created by CreateTable, Split, Merge. Regions are deleted by DeleteTable, Split,
 * Merge. Assigns are triggered by CreateTable, EnableTable, Split, Merge, ServerCrash. Unassigns
 * are triggered by DisableTable, Split, Merge
 */
@InterfaceAudience.Private
public class AssignmentManager {
  private static final Logger LOG = LoggerFactory.getLogger(AssignmentManager.class);

  // TODO: AMv2
  // - handle region migration from hbase1 to hbase2.
  // - handle sys table assignment first (e.g. acl, namespace)
  // - handle table priorities
  // - If ServerBusyException trying to update hbase:meta, we abort the Master
  // See updateRegionLocation in RegionStateStore.
  //
  // See also
  // https://docs.google.com/document/d/1eVKa7FHdeoJ1-9o8yZcOTAQbv0u0bblBlCCzVSIn69g/edit#heading=h.ystjyrkbtoq5
  // for other TODOs.

  public static final String BOOTSTRAP_THREAD_POOL_SIZE_CONF_KEY =
    "hbase.assignment.bootstrap.thread.pool.size";

  public static final String ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY =
    "hbase.assignment.dispatch.wait.msec";
  private static final int DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC = 150;

  public static final String ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY =
    "hbase.assignment.dispatch.wait.queue.max.size";
  private static final int DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX = 100;

  public static final String RIT_CHORE_INTERVAL_MSEC_CONF_KEY =
    "hbase.assignment.rit.chore.interval.msec";
  private static final int DEFAULT_RIT_CHORE_INTERVAL_MSEC = 60 * 1000;

  public static final String DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC_CONF_KEY =
    "hbase.assignment.dead.region.metric.chore.interval.msec";
  private static final int DEFAULT_DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC = 120 * 1000;

  public static final String ASSIGN_MAX_ATTEMPTS = "hbase.assignment.maximum.attempts";
  private static final int DEFAULT_ASSIGN_MAX_ATTEMPTS = Integer.MAX_VALUE;

  public static final String ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS =
    "hbase.assignment.retry.immediately.maximum.attempts";
  private static final int DEFAULT_ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS = 3;

  /** Region in Transition metrics threshold time */
  public static final String METRICS_RIT_STUCK_WARNING_THRESHOLD =
    "hbase.metrics.rit.stuck.warning.threshold";
  private static final int DEFAULT_RIT_STUCK_WARNING_THRESHOLD = 60 * 1000;
  public static final String UNEXPECTED_STATE_REGION = "Unexpected state for ";

  public static final String FORCE_REGION_RETAINMENT = "hbase.master.scp.retain.assignment.force";

  public static final boolean DEFAULT_FORCE_REGION_RETAINMENT = false;

  /** The wait time in millis before checking again if the region's previous RS is back online */
  public static final String FORCE_REGION_RETAINMENT_WAIT_INTERVAL =
    "hbase.master.scp.retain.assignment.force.wait-interval";

  public static final long DEFAULT_FORCE_REGION_RETAINMENT_WAIT_INTERVAL = 50;

  /**
   * The number of times to check if the region's previous RS is back online, before giving up and
   * proceeding with assignment on a new RS
   */
  public static final String FORCE_REGION_RETAINMENT_RETRIES =
    "hbase.master.scp.retain.assignment.force.retries";

  public static final int DEFAULT_FORCE_REGION_RETAINMENT_RETRIES = 600;

  private final ProcedureEvent<?> metaAssignEvent = new ProcedureEvent<>("meta assign");
  private final ProcedureEvent<?> metaLoadEvent = new ProcedureEvent<>("meta load");

  private final MetricsAssignmentManager metrics;
  private final RegionInTransitionChore ritChore;
  private final DeadServerMetricRegionChore deadMetricChore;
  private final MasterServices master;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final RegionStates regionStates = new RegionStates();
  private final RegionStateStore regionStateStore;

  /**
   * When the operator uses this configuration option, any version between the current cluster
   * version and the value of "hbase.min.version.move.system.tables" does not trigger any
   * auto-region movement. Auto-region movement here refers to auto-migration of system table
   * regions to newer server versions. It is assumed that the configured range of versions does not
   * require special handling of moving system table regions to higher versioned RegionServer. This
   * auto-migration is done by {@link #checkIfShouldMoveSystemRegionAsync()}. Example: Let's assume
   * the cluster is on version 1.4.0 and we have set "hbase.min.version.move.system.tables" as
   * "2.0.0". Now if we upgrade one RegionServer on 1.4.0 cluster to 1.6.0 (< 2.0.0), then
   * AssignmentManager will not move hbase:meta, hbase:namespace and other system table regions to
   * newly brought up RegionServer 1.6.0 as part of auto-migration. However, if we upgrade one
   * RegionServer on 1.4.0 cluster to 2.2.0 (> 2.0.0), then AssignmentManager will move all system
   * table regions to newly brought up RegionServer 2.2.0 as part of auto-migration done by
   * {@link #checkIfShouldMoveSystemRegionAsync()}. "hbase.min.version.move.system.tables" is
   * introduced as part of HBASE-22923.
   */
  private final String minVersionToMoveSysTables;

  private static final String MIN_VERSION_MOVE_SYS_TABLES_CONFIG =
    "hbase.min.version.move.system.tables";
  private static final String DEFAULT_MIN_VERSION_MOVE_SYS_TABLES_CONFIG = "";

  private final Map<ServerName, Set<byte[]>> rsReports = new HashMap<>();

  private final boolean shouldAssignRegionsWithFavoredNodes;
  private final int assignDispatchWaitQueueMaxSize;
  private final int assignDispatchWaitMillis;
  private final int assignMaxAttempts;
  private final int assignRetryImmediatelyMaxAttempts;

  private final MasterRegion masterRegion;

  private final Object checkIfShouldMoveSystemRegionLock = new Object();

  private Thread assignThread;

  private final boolean forceRegionRetainment;

  private final long forceRegionRetainmentWaitInterval;

  private final int forceRegionRetainmentRetries;

  public AssignmentManager(MasterServices master, MasterRegion masterRegion) {
    this(master, masterRegion, new RegionStateStore(master, masterRegion));
  }

  AssignmentManager(MasterServices master, MasterRegion masterRegion, RegionStateStore stateStore) {
    this.master = master;
    this.regionStateStore = stateStore;
    this.metrics = new MetricsAssignmentManager();
    this.masterRegion = masterRegion;

    final Configuration conf = master.getConfiguration();

    // Only read favored nodes if using the favored nodes load balancer.
    this.shouldAssignRegionsWithFavoredNodes = FavoredStochasticBalancer.class
      .isAssignableFrom(conf.getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, Object.class));

    this.assignDispatchWaitMillis =
      conf.getInt(ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY, DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC);
    this.assignDispatchWaitQueueMaxSize =
      conf.getInt(ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY, DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX);

    this.assignMaxAttempts =
      Math.max(1, conf.getInt(ASSIGN_MAX_ATTEMPTS, DEFAULT_ASSIGN_MAX_ATTEMPTS));
    this.assignRetryImmediatelyMaxAttempts = conf.getInt(ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS,
      DEFAULT_ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS);

    int ritChoreInterval =
      conf.getInt(RIT_CHORE_INTERVAL_MSEC_CONF_KEY, DEFAULT_RIT_CHORE_INTERVAL_MSEC);
    this.ritChore = new RegionInTransitionChore(ritChoreInterval);

    int deadRegionChoreInterval = conf.getInt(DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC_CONF_KEY,
      DEFAULT_DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC);
    if (((KnobRuntime.check(java.util.UUID.fromString("123000e2-fc6a-3fe7-8184-17b5cacfb624"))) ? ((deadRegionChoreInterval) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f104d295-172d-3b7a-a79f-0f3852ea07a9"))) ? ((deadRegionChoreInterval) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c76be71c-80d5-3cf0-b901-f1f289dd27f2"))) ? ((deadRegionChoreInterval) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5847154b-5d52-3204-b0ea-0e58941cb8bb"))) ? ((deadRegionChoreInterval) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7aa0b580-0275-379b-b4fb-b0cc9782ce91"))) ? ((deadRegionChoreInterval) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("19551f4b-8bc7-3f76-ba38-4546e64f09f6"))) ? ((deadRegionChoreInterval) > (0)) : (deadRegionChoreInterval > 0))))))))))))) {
      this.deadMetricChore = new DeadServerMetricRegionChore(deadRegionChoreInterval);
    } else {
      this.deadMetricChore = null;
    }
    minVersionToMoveSysTables =
      conf.get(MIN_VERSION_MOVE_SYS_TABLES_CONFIG, DEFAULT_MIN_VERSION_MOVE_SYS_TABLES_CONFIG);

    forceRegionRetainment =
      conf.getBoolean(FORCE_REGION_RETAINMENT, DEFAULT_FORCE_REGION_RETAINMENT);
    forceRegionRetainmentWaitInterval = conf.getLong(FORCE_REGION_RETAINMENT_WAIT_INTERVAL,
      DEFAULT_FORCE_REGION_RETAINMENT_WAIT_INTERVAL);
    forceRegionRetainmentRetries =
      conf.getInt(FORCE_REGION_RETAINMENT_RETRIES, DEFAULT_FORCE_REGION_RETAINMENT_RETRIES);
  }

  private void mirrorMetaLocations() throws IOException, KeeperException {
    // For compatibility, mirror the meta region state to zookeeper
    // And we still need to use zookeeper to publish the meta region locations to region
    // server, so they can serve as ClientMetaService
    ZKWatcher zk = master.getZooKeeper();
    if (zk == null || !zk.getRecoverableZooKeeper().getState().isAlive()) {
      // this is possible in tests, we do not provide a zk watcher or the zk watcher has been closed
      return;
    }
    Collection<RegionStateNode> metaStates = regionStates.getRegionStateNodes();
    for (RegionStateNode metaState : metaStates) {
      MetaTableLocator.setMetaLocation(zk, metaState.getRegionLocation(),
        metaState.getRegionInfo().getReplicaId(), metaState.getState());
    }
    int replicaCount = metaStates.size();
    // remove extra mirror locations
    for (String znode : zk.getMetaReplicaNodes()) {
      int replicaId = zk.getZNodePaths().getMetaReplicaIdFromZNode(znode);
      if (replicaId >= replicaCount) {
if(KnobRuntime.check(java.util.UUID.fromString("ddeed08a-b85b-3482-b724-78dd99145895"))) {
replicaId /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b3a2fbff-373b-390b-b00d-566bb264a566"))) {
try {
    java.lang.reflect.Field field = zk.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(zk));
    field.set(zk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4954eb51-33bb-369c-80eb-90bfb8b89fa0"))) {
replicaId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d1eba485-068f-328d-b3eb-6bc1d9d97826"))) {
replicaId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c45845fd-fed5-3e18-a2c3-0d0d7e6fc889"))) {
replicaId *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c6a88dc1-5166-3658-bea8-a43f1e1c1190"))) {
replicaId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3edec96d-ff30-3e29-8aaf-5e99221d698e"))) {
replicaId += 1;
}
        MetaTableLocator.deleteMetaLocation(zk, replicaId);
      }
    }
  }

  public void start() throws IOException, KeeperException {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    LOG.trace("Starting assignment manager");

    // Start the Assignment Thread
    if (KnobRuntime.check(java.util.UUID.fromString("72b0c182-3342-3b3c-8676-405bfc0d1dee"))) { mirrorMetaLocations(); } else if (KnobRuntime.check(java.util.UUID.fromString("9a44cbbd-b906-39e8-b70e-bc290dcf28be"))) { stopAssignmentThread(); } else { startAssignmentThread(); }
    // load meta region states.
    // here we are still in the early steps of active master startup. There is only one thread(us)
    // can access AssignmentManager and create region node, so here we do not need to lock the
    // region node.
    try (ResultScanner scanner =
      masterRegion.getScanner(new Scan().addFamily(HConstants.CATALOG_FAMILY))) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        RegionStateStore
          .visitMetaEntry((r, regionInfo, state, regionLocation, lastHost, openSeqNum) -> {
            RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
            regionNode.setState(state);
            regionNode.setLastHost(lastHost);
            regionNode.setRegionLocation(regionLocation);
            regionNode.setOpenSeqNum(openSeqNum);
            if (regionNode.getProcedure() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("81b0e6e7-3ffb-3159-a58e-7620481554e2"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca3621ca-81cc-31af-b079-0909816846d6"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              regionNode.getProcedure().stateLoaded(this, regionNode);
            }
            if (regionLocation != null) {
              // TODO: this could lead to some orphan server state nodes, as it is possible that the
              // region server is already dead and its SCP has already finished but we have
              // persisted an opening state on this region server. Finally the TRSP will assign the
              // region to another region server, so it will not cause critical problems, just waste
              // some memory as no one will try to cleanup these orphan server state nodes.
              regionStates.createServer(regionLocation);
              regionStates.addRegionToServer(regionNode);
            }
            if (RegionReplicaUtil.isDefaultReplica(regionInfo.getReplicaId())) {
              setMetaAssigned(regionInfo, state == State.OPEN);
            }
            LOG.debug("Loaded hbase:meta {}", regionNode);
          }, result);
      }
    }
    mirrorMetaLocations();
  }

  /**
   * Create RegionStateNode based on the TRSP list, and attach the TRSP to the RegionStateNode.
   * <p>
   * This is used to restore the RIT region list, so we do not need to restore it in the loadingMeta
   * method below. And it is also very important as now before submitting a TRSP, we need to attach
   * it to the RegionStateNode, which acts like a guard, so we need to restore this information at
   * the very beginning, before we start processing any procedures.
   */
  public void setupRIT(List<TransitRegionStateProcedure> procs) {
if(KnobRuntime.check(java.util.UUID.fromString("e0949ca3-70d9-3f39-857c-054e5899f505"))) {
return;
}
    procs.forEach(proc -> {
      RegionInfo regionInfo = proc.getRegion();
      RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
      TransitRegionStateProcedure existingProc = regionNode.getProcedure();
      if (existingProc != null) {
        // This is possible, as we will detach the procedure from the RSN before we
        // actually finish the procedure. This is because that, we will detach the TRSP from the RSN
        // during execution, at that time, the procedure has not been marked as done in the pv2
        // framework yet, so it is possible that we schedule a new TRSP immediately and when
        // arriving here, we will find out that there are multiple TRSPs for the region. But we can
        // make sure that, only the last one can take the charge, the previous ones should have all
        // been finished already. So here we will compare the proc id, the greater one will win.
        if (existingProc.getProcId() < proc.getProcId()) {
          // the new one wins, unset and set it to the new one below
          regionNode.unsetProcedure(existingProc);
        } else {
          // the old one wins, skip
          return;
        }
      }
      LOG.info("Attach {} to {} to restore RIT", proc, regionNode);
      regionNode.setProcedure(proc);
    });
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    LOG.info("Stopping assignment manager");

    // The AM is started before the procedure executor,
    // but the actual work will be loaded/submitted only once we have the executor
    final boolean hasProcExecutor = master.getMasterProcedureExecutor() != null;

    // Remove the RIT chore
    if (hasProcExecutor) {
      master.getMasterProcedureExecutor().removeChore(this.ritChore);
      if (this.deadMetricChore != null) {
        master.getMasterProcedureExecutor().removeChore(this.deadMetricChore);
      }
    }

    // Stop the Assignment Thread
    stopAssignmentThread();

    // Stop the RegionStateStore
    if (KnobRuntime.check(java.util.UUID.fromString("31ddfe3d-e96b-3e08-90e4-3a18561e924b"))) { startAssignmentThread(); } else if (KnobRuntime.check(java.util.UUID.fromString("ae50f0a8-6b3b-332b-82f0-2e302c930659"))) { stopAssignmentThread(); } else { regionStates.clear(); }

    // Update meta events (for testing)
    if (hasProcExecutor) {
      if (KnobRuntime.check(java.util.UUID.fromString("f5e5339a-d11e-35db-8aee-6cbfaaa22580"))) { stopAssignmentThread(); } else if (KnobRuntime.check(java.util.UUID.fromString("389f9324-f36b-3772-abd3-f9f3c8aab348"))) { startAssignmentThread(); } else { metaLoadEvent.suspend(); }
      for (RegionInfo hri : getMetaRegionSet()) {
        setMetaAssigned(hri, false);
      }
    }
  }

  public boolean isRunning() {
    return running.get();
  }

  public Configuration getConfiguration() {
    return master.getConfiguration();
  }

  public MetricsAssignmentManager getAssignmentManagerMetrics() {
    return metrics;
  }

  private LoadBalancer getBalancer() {
    return master.getLoadBalancer();
  }

  private MasterProcedureEnv getProcedureEnvironment() {
if(KnobRuntime.check(java.util.UUID.fromString("98f99c9d-71e1-3a13-b96b-ace02f341fe2"))) {
return null;
}
    return master.getMasterProcedureExecutor().getEnvironment();
  }

  private MasterProcedureScheduler getProcedureScheduler() {
    return getProcedureEnvironment().getProcedureScheduler();
  }

  int getAssignMaxAttempts() {
    return assignMaxAttempts;
  }

  public boolean isForceRegionRetainment() {
    return forceRegionRetainment;
  }

  public long getForceRegionRetainmentWaitInterval() {
    return forceRegionRetainmentWaitInterval;
  }

  public int getForceRegionRetainmentRetries() {
    return forceRegionRetainmentRetries;
  }

  int getAssignRetryImmediatelyMaxAttempts() {
if(KnobRuntime.check(java.util.UUID.fromString("78dabaf5-780d-31ae-a311-5f22f5544634"))) {
return 0;
}
    return assignRetryImmediatelyMaxAttempts;
  }

  public RegionStates getRegionStates() {
if(KnobRuntime.check(java.util.UUID.fromString("74a80b26-53c3-3f7c-a868-53ec5977aab2"))) {
return null;
}
    return regionStates;
  }

  /**
   * Returns the regions hosted by the specified server.
   * <p/>
   * Notice that, for SCP, after we submit the SCP, no one can change the region list for the
   * ServerStateNode so we do not need any locks here. And for other usage, this can only give you a
   * snapshot of the current region list for this server, which means, right after you get the
   * region list, new regions may be moved to this server or some regions may be moved out from this
   * server, so you should not use it critically if you need strong consistency.
   */
  public List<RegionInfo> getRegionsOnServer(ServerName serverName) {
    ServerStateNode serverInfo = regionStates.getServerNode(serverName);
    if (serverInfo == null) {
      return Collections.emptyList();
    }
    return serverInfo.getRegionInfoList();
  }

  private RegionInfo getRegionInfo(RegionStateNode rsn) {
    if (rsn.isSplit() && !rsn.getRegionInfo().isSplit()) {
      // see the comments in markRegionAsSplit on why we need to do this converting.
      return RegionInfoBuilder.newBuilder(rsn.getRegionInfo()).setSplit(true).setOffline(true)
        .build();
    } else {
      return rsn.getRegionInfo();
    }
  }

  private Stream<RegionStateNode> getRegionStateNodes(TableName tableName,
    boolean excludeOfflinedSplitParents) {
    Stream<RegionStateNode> stream = regionStates.getTableRegionStateNodes(tableName).stream();
    if (excludeOfflinedSplitParents) {
      return stream.filter(rsn -> !rsn.isSplit());
    } else {
      return stream;
    }
  }

  public List<RegionInfo> getTableRegions(TableName tableName,
    boolean excludeOfflinedSplitParents) {
    return getRegionStateNodes(tableName, excludeOfflinedSplitParents).map(this::getRegionInfo)
      .collect(Collectors.toList());
  }

  public List<Pair<RegionInfo, ServerName>> getTableRegionsAndLocations(TableName tableName,
    boolean excludeOfflinedSplitParents) {
if(KnobRuntime.check(java.util.UUID.fromString("110b0b6d-703c-376e-9a47-912f55bb5048"))) {
excludeOfflinedSplitParents = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("569c9363-40c9-3d25-b060-f2a2005cc612"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45ff7c2f-34a3-3a8d-8982-d21464107f0e"))) {
excludeOfflinedSplitParents = !excludeOfflinedSplitParents;
}
if(KnobRuntime.check(java.util.UUID.fromString("0aee641b-adab-3b22-ad54-b032708016ed"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa7510f6-7b14-3211-9713-4e3cd6665533"))) {
excludeOfflinedSplitParents = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("78575165-178b-30aa-9509-85cd36b663cb"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("152d80ac-35ce-3def-b203-7b70b1c14a9a"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4174b36-7e14-3089-b962-9c3edc86ac94"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return getRegionStateNodes(tableName, excludeOfflinedSplitParents)
      .map(rsn -> Pair.newPair(getRegionInfo(rsn), rsn.getRegionLocation()))
      .collect(Collectors.toList());
  }

  public RegionStateStore getRegionStateStore() {
    return regionStateStore;
  }

  public List<ServerName> getFavoredNodes(final RegionInfo regionInfo) {
if(KnobRuntime.check(java.util.UUID.fromString("fbc2a8bf-4cec-3072-bdc1-37e780386993"))) {
return null;
}
    return this.shouldAssignRegionsWithFavoredNodes
      ? ((FavoredStochasticBalancer) getBalancer()).getFavoredNodes(regionInfo)
      : ServerName.EMPTY_SERVER_LIST;
  }

  // ============================================================================================
  // Table State Manager helpers
  // ============================================================================================
  private TableStateManager getTableStateManager() {
if(KnobRuntime.check(java.util.UUID.fromString("78f8a72d-8067-33b6-8012-863c050367f0"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e25ec1e7-6465-342d-b0c9-75a540f7887c"))) ? (getTableStateManager()) : (master.getTableStateManager()));
  }

  private boolean isTableEnabled(final TableName tableName) {
    return getTableStateManager().isTableState(tableName, TableState.State.ENABLED);
  }

  private boolean isTableDisabled(final TableName tableName) {
    return getTableStateManager().isTableState(tableName, TableState.State.DISABLED,
      TableState.State.DISABLING);
  }

  // ============================================================================================
  // META Helpers
  // ============================================================================================
  private boolean isMetaRegion(final RegionInfo regionInfo) {
    return regionInfo.isMetaRegion();
  }

  public boolean isMetaRegion(final byte[] regionName) {
    return getMetaRegionFromName(regionName) != null;
  }

  public RegionInfo getMetaRegionFromName(final byte[] regionName) {
if(KnobRuntime.check(java.util.UUID.fromString("36f95f0c-9490-3dd5-a047-346bf652428f"))) {
return null;
}
    for (RegionInfo hri : getMetaRegionSet()) {
      if (Bytes.equals(hri.getRegionName(), regionName)) {
        return hri;
      }
    }
    return null;
  }

  public boolean isCarryingMeta(final ServerName serverName) {
    // TODO: handle multiple meta
    return isCarryingRegion(serverName, RegionInfoBuilder.FIRST_META_REGIONINFO);
  }

  private boolean isCarryingRegion(final ServerName serverName, final RegionInfo regionInfo) {
    // TODO: check for state?
    final RegionStateNode node = regionStates.getRegionStateNode(regionInfo);
    return (node != null && serverName.equals(node.getRegionLocation()));
  }

  private RegionInfo getMetaForRegion(final RegionInfo regionInfo) {
    // if (regionInfo.isMetaRegion()) return regionInfo;
    // TODO: handle multiple meta. if the region provided is not meta lookup
    // which meta the region belongs to.
    return RegionInfoBuilder.FIRST_META_REGIONINFO;
  }

  // TODO: handle multiple meta.
  private static final Set<RegionInfo> META_REGION_SET =
    Collections.singleton(RegionInfoBuilder.FIRST_META_REGIONINFO);

  public Set<RegionInfo> getMetaRegionSet() {
    return META_REGION_SET;
  }

  // ============================================================================================
  // META Event(s) helpers
  // ============================================================================================
  /**
   * Notice that, this only means the meta region is available on a RS, but the AM may still be
   * loading the region states from meta, so usually you need to check {@link #isMetaLoaded()} first
   * before checking this method, unless you can make sure that your piece of code can only be
   * executed after AM builds the region states.
   * @see #isMetaLoaded()
   */
  public boolean isMetaAssigned() {
    return metaAssignEvent.isReady();
  }

  public boolean isMetaRegionInTransition() {
    return !isMetaAssigned();
  }

  /**
   * Notice that this event does not mean the AM has already finished region state rebuilding. See
   * the comment of {@link #isMetaAssigned()} for more details.
   * @see #isMetaAssigned()
   */
  public boolean waitMetaAssigned(Procedure<?> proc, RegionInfo regionInfo) {
    return getMetaAssignEvent(getMetaForRegion(regionInfo)).suspendIfNotReady(proc);
  }

  private void setMetaAssigned(RegionInfo metaRegionInfo, boolean assigned) {
    assert isMetaRegion(metaRegionInfo) : "unexpected non-meta region " + metaRegionInfo;
    ProcedureEvent<?> metaAssignEvent = getMetaAssignEvent(metaRegionInfo);
    if (assigned) {
      metaAssignEvent.wake(getProcedureScheduler());
    } else {
      metaAssignEvent.suspend();
    }
  }

  private ProcedureEvent<?> getMetaAssignEvent(RegionInfo metaRegionInfo) {
    assert isMetaRegion(metaRegionInfo) : "unexpected non-meta region " + metaRegionInfo;
    // TODO: handle multiple meta.
    return metaAssignEvent;
  }

  /**
   * Wait until AM finishes the meta loading, i.e, the region states rebuilding.
   * @see #isMetaLoaded()
   * @see #waitMetaAssigned(Procedure, RegionInfo)
   */
  public boolean waitMetaLoaded(Procedure<?> proc) {
    return metaLoadEvent.suspendIfNotReady(proc);
  }

  /**
   * This method will be called in master initialization method after calling
   * {@link #processOfflineRegions()}, as in processOfflineRegions we will generate assign
   * procedures for offline regions, which may be conflict with creating table.
   * <p/>
   * This is a bit dirty, should be reconsidered after we decide whether to keep the
   * {@link #processOfflineRegions()} method.
   */
  public void wakeMetaLoadedEvent() {
    metaLoadEvent.wake(getProcedureScheduler());
    assert ((KnobRuntime.check(java.util.UUID.fromString("402b81fe-9dca-3c04-beba-247f32cbf837"))) ? (isMetaAssigned()) : (((KnobRuntime.check(java.util.UUID.fromString("ed87f302-9a74-3386-99c7-493367293643"))) ? (isRunning()) : (isMetaLoaded())))) : "expected meta to be loaded";
  }

  /**
   * Return whether AM finishes the meta loading, i.e, the region states rebuilding.
   * @see #isMetaAssigned()
   * @see #waitMetaLoaded(Procedure)
   */
  public boolean isMetaLoaded() {
    return metaLoadEvent.isReady();
  }

  /**
   * Start a new thread to check if there are region servers whose versions are higher than others.
   * If so, move all system table regions to RS with the highest version to keep compatibility. The
   * reason is, RS in new version may not be able to access RS in old version when there are some
   * incompatible changes.
   * <p>
   * This method is called when a new RegionServer is added to cluster only.
   * </p>
   */
  public void checkIfShouldMoveSystemRegionAsync() {
    // TODO: Fix this thread. If a server is killed and a new one started, this thread thinks that
    // it should 'move' the system tables from the old server to the new server but
    // ServerCrashProcedure is on it; and it will take care of the assign without dataloss.
    if (((KnobRuntime.check(java.util.UUID.fromString("a38021b7-2c7d-35cf-998a-e172582035ee"))) ? ((this.master.getServerManager().countOfRegionServers()) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d4cdddc2-9362-3b18-ad73-5cc9ffbaa8cc"))) ? ((this.master.getServerManager().countOfRegionServers()) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f34c1d06-acdb-3b78-8d30-d786299dcb77"))) ? ((this.master.getServerManager().countOfRegionServers()) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("7259e74f-baa0-34fa-a67a-461c55ea9df2"))) ? ((this.master.getServerManager().countOfRegionServers()) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("9699b380-29ed-3cd9-8098-bac9ce85270a"))) ? ((this.master.getServerManager().countOfRegionServers()) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("812a3e04-85fe-304a-8c30-12b9ab2c3117"))) ? ((this.master.getServerManager().countOfRegionServers()) <= (1)) : (this.master.getServerManager().countOfRegionServers() <= 1))))))))))))) {
      return;
    }
    // This thread used to run whenever there was a change in the cluster. The ZooKeeper
    // childrenChanged notification came in before the nodeDeleted message and so this method
    // cold run before a ServerCrashProcedure could run. That meant that this thread could see
    // a Crashed Server before ServerCrashProcedure and it could find system regions on the
    // crashed server and go move them before ServerCrashProcedure had a chance; could be
    // dataloss too if WALs were not recovered.
    new Thread(() -> {
      try {
        synchronized (checkIfShouldMoveSystemRegionLock) {
          List<RegionPlan> plans = new ArrayList<>();
          // TODO: I don't think this code does a good job if all servers in cluster have same
          // version. It looks like it will schedule unnecessary moves.
          for (ServerName server : getExcludedServersForSystemTable()) {
            if (master.getServerManager().isServerDead(server)) {
              // TODO: See HBASE-18494 and HBASE-18495. Though getExcludedServersForSystemTable()
              // considers only online servers, the server could be queued for dead server
              // processing. As region assignments for crashed server is handled by
              // ServerCrashProcedure, do NOT handle them here. The goal is to handle this through
              // regular flow of LoadBalancer as a favored node and not to have this special
              // handling.
              continue;
            }
            List<RegionInfo> regionsShouldMove = getSystemTables(server);
            if (!regionsShouldMove.isEmpty()) {
              for (RegionInfo regionInfo : regionsShouldMove) {
                // null value for dest forces destination server to be selected by balancer
                RegionPlan plan = new RegionPlan(regionInfo, server, null);
                if (regionInfo.isMetaRegion()) {
                  // Must move meta region first.
                  LOG.info("Async MOVE of {} to newer Server={}", regionInfo.getEncodedName(),
                    server);
                  moveAsync(plan);
                } else {
                  plans.add(plan);
                }
              }
            }
            for (RegionPlan plan : plans) {
              LOG.info("Async MOVE of {} to newer Server={}", plan.getRegionInfo().getEncodedName(),
                server);
              moveAsync(plan);
            }
          }
        }
      } catch (Throwable t) {
        LOG.error(t.toString(), t);
      }
    }).start();
  }

  private List<RegionInfo> getSystemTables(ServerName serverName) {
    ServerStateNode serverNode = regionStates.getServerNode(serverName);
    if (serverNode == null) {
      return Collections.emptyList();
    }
    return serverNode.getSystemRegionInfoList();
  }

  private void preTransitCheck(RegionStateNode regionNode, RegionState.State[] expectedStates)
    throws HBaseIOException {
    if (regionNode.getProcedure() != null) {
      throw new HBaseIOException(
        regionNode + " is currently in transition; pid=" + regionNode.getProcedure().getProcId());
    }
    if (!regionNode.isInState(expectedStates)) {
      throw new DoNotRetryRegionException(UNEXPECTED_STATE_REGION + regionNode);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("f1e6d814-7c1f-3e7c-bf12-a784fd91dc26"))) ? (isTableEnabled(regionNode.getTable())) : (isTableDisabled(regionNode.getTable())))) {
      throw new DoNotRetryIOException(regionNode.getTable() + " is disabled for " + regionNode);
    }
  }

  /**
   * Create an assign TransitRegionStateProcedure. Makes sure of RegionState. Throws exception if
   * not appropriate UNLESS override is set. Used by hbck2 but also by straightline
   * {@link #assign(RegionInfo, ServerName)} and {@link #assignAsync(RegionInfo, ServerName)}.
   * @see #createAssignProcedure(RegionStateNode, ServerName) for a version that does NO checking
   *      used when only when no checking needed.
   * @param override If false, check RegionState is appropriate for assign; if not throw exception.
   */
  private TransitRegionStateProcedure createAssignProcedure(RegionInfo regionInfo, ServerName sn,
    boolean override) throws IOException {
    RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
    regionNode.lock();
    try {
      if (override) {
        if (regionNode.getProcedure() != null) {
          regionNode.unsetProcedure(regionNode.getProcedure());
        }
      } else {
        preTransitCheck(regionNode, STATES_EXPECTED_ON_ASSIGN);
      }
      assert regionNode.getProcedure() == null;
      return regionNode.setProcedure(
        TransitRegionStateProcedure.assign(getProcedureEnvironment(), regionInfo, sn));
    } finally {
      regionNode.unlock();
    }
  }

  /**
   * Create an assign TransitRegionStateProcedure. Does NO checking of RegionState. Presumes
   * appriopriate state ripe for assign.
   * @see #createAssignProcedure(RegionInfo, ServerName, boolean)
   */
  private TransitRegionStateProcedure createAssignProcedure(RegionStateNode regionNode,
    ServerName targetServer) {
    regionNode.lock();
    try {
      return regionNode.setProcedure(TransitRegionStateProcedure.assign(getProcedureEnvironment(),
        regionNode.getRegionInfo(), targetServer));
    } finally {
      regionNode.unlock();
    }
  }

  public long assign(RegionInfo regionInfo, ServerName sn) throws IOException {
    TransitRegionStateProcedure proc = createAssignProcedure(regionInfo, sn, false);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
    return proc.getProcId();
  }

  public long assign(RegionInfo regionInfo) throws IOException {
    return assign(regionInfo, null);
  }

  /**
   * Submits a procedure that assigns a region to a target server without waiting for it to finish
   * @param regionInfo the region we would like to assign
   * @param sn         target server name
   */
  public Future<byte[]> assignAsync(RegionInfo regionInfo, ServerName sn) throws IOException {
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(),
      createAssignProcedure(regionInfo, sn, false));
  }

  /**
   * Submits a procedure that assigns a region without waiting for it to finish
   * @param regionInfo the region we would like to assign
   */
  public Future<byte[]> assignAsync(RegionInfo regionInfo) throws IOException {
    return assignAsync(regionInfo, null);
  }

  public long unassign(RegionInfo regionInfo) throws IOException {
    RegionStateNode regionNode = regionStates.getRegionStateNode(regionInfo);
    if (regionNode == null) {
      throw new UnknownRegionException("No RegionState found for " + regionInfo.getEncodedName());
    }
    TransitRegionStateProcedure proc;
    regionNode.lock();
    try {
      preTransitCheck(regionNode, STATES_EXPECTED_ON_UNASSIGN_OR_MOVE);
      proc = TransitRegionStateProcedure.unassign(getProcedureEnvironment(), regionInfo);
      regionNode.setProcedure(proc);
    } finally {
      regionNode.unlock();
    }
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
    return proc.getProcId();
  }

  public TransitRegionStateProcedure createMoveRegionProcedure(RegionInfo regionInfo,
    ServerName targetServer) throws HBaseIOException {
    RegionStateNode regionNode = this.regionStates.getRegionStateNode(regionInfo);
    if (regionNode == null) {
      throw new UnknownRegionException(
        "No RegionStateNode found for " + regionInfo.getEncodedName() + "(Closed/Deleted?)");
    }
    TransitRegionStateProcedure proc;
    regionNode.lock();
    try {
      preTransitCheck(regionNode, STATES_EXPECTED_ON_UNASSIGN_OR_MOVE);
      regionNode.checkOnline();
      proc = TransitRegionStateProcedure.move(getProcedureEnvironment(), regionInfo, targetServer);
      regionNode.setProcedure(proc);
    } finally {
      regionNode.unlock();
    }
    return proc;
  }

  public void move(RegionInfo regionInfo) throws IOException {
    TransitRegionStateProcedure proc = createMoveRegionProcedure(regionInfo, null);
    ProcedureSyncWait.submitAndWaitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public Future<byte[]> moveAsync(RegionPlan regionPlan) throws HBaseIOException {
    TransitRegionStateProcedure proc =
      createMoveRegionProcedure(regionPlan.getRegionInfo(), regionPlan.getDestination());
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  public Future<byte[]> balance(RegionPlan regionPlan) throws HBaseIOException {
    ServerName current =
      this.getRegionStates().getRegionAssignments().get(regionPlan.getRegionInfo());
    if (current == null || !current.equals(regionPlan.getSource())) {
      LOG.debug("Skip region plan {}, source server not match, current region location is {}",
        regionPlan, current == null ? "(null)" : current);
      return null;
    }
    return moveAsync(regionPlan);
  }

  // ============================================================================================
  // RegionTransition procedures helpers
  // ============================================================================================

  /**
   * Create round-robin assigns. Use on table creation to distribute out regions across cluster.
   * @return AssignProcedures made out of the passed in <code>hris</code> and a call to the balancer
   *         to populate the assigns with targets chosen using round-robin (default balancer
   *         scheme). If at assign-time, the target chosen is no longer up, thats fine, the
   *         AssignProcedure will ask the balancer for a new target, and so on.
   */
  public TransitRegionStateProcedure[] createRoundRobinAssignProcedures(List<RegionInfo> hris,
    List<ServerName> serversToExclude) {
    if (hris.isEmpty()) {
      return new TransitRegionStateProcedure[0];
    }

    if (
      serversToExclude != null && this.master.getServerManager().getOnlineServersList().size() == 1
    ) {
      LOG.debug("Only one region server found and hence going ahead with the assignment");
      serversToExclude = null;
    }
    try {
      // Ask the balancer to assign our regions. Pass the regions en masse. The balancer can do
      // a better job if it has all the assignments in the one lump.
      Map<ServerName, List<RegionInfo>> assignments = getBalancer().roundRobinAssignment(hris,
        this.master.getServerManager().createDestinationServersList(serversToExclude));
      // Return mid-method!
      return createAssignProcedures(assignments);
    } catch (HBaseIOException hioe) {
      LOG.warn("Failed roundRobinAssignment", hioe);
    }
    // If an error above, fall-through to this simpler assign. Last resort.
    return createAssignProcedures(hris);
  }

  /**
   * Create round-robin assigns. Use on table creation to distribute out regions across cluster.
   * @return AssignProcedures made out of the passed in <code>hris</code> and a call to the balancer
   *         to populate the assigns with targets chosen using round-robin (default balancer
   *         scheme). If at assign-time, the target chosen is no longer up, thats fine, the
   *         AssignProcedure will ask the balancer for a new target, and so on.
   */
  public TransitRegionStateProcedure[] createRoundRobinAssignProcedures(List<RegionInfo> hris) {
    return createRoundRobinAssignProcedures(hris, null);
  }

  static int compare(TransitRegionStateProcedure left, TransitRegionStateProcedure right) {
    if (left.getRegion().isMetaRegion()) {
      if (right.getRegion().isMetaRegion()) {
        return RegionInfo.COMPARATOR.compare(left.getRegion(), right.getRegion());
      }
      return -1;
    } else if (right.getRegion().isMetaRegion()) {
      return +1;
    }
    if (left.getRegion().getTable().isSystemTable()) {
      if (right.getRegion().getTable().isSystemTable()) {
        return RegionInfo.COMPARATOR.compare(left.getRegion(), right.getRegion());
      }
      return -1;
    } else if (right.getRegion().getTable().isSystemTable()) {
      return +1;
    }
    return RegionInfo.COMPARATOR.compare(left.getRegion(), right.getRegion());
  }

  /**
   * Create one TransitRegionStateProcedure to assign a region w/o specifying a target server. This
   * method is called from HBCK2.
   * @return an assign or null
   */
  public TransitRegionStateProcedure createOneAssignProcedure(RegionInfo ri, boolean override) {
    TransitRegionStateProcedure trsp = null;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("5640a765-4962-3fa2-b109-66a7c2b1bf8f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("92acfb59-147f-3372-b011-7a0579b59b97"))) {
override = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3d2c8dfc-ccd4-31eb-ad52-add8578932bb"))) {
override = true;
}
      trsp = createAssignProcedure(ri, null, override);
    } catch (IOException ioe) {
      LOG.info(
        "Failed {} assign, override={}"
          + (override ? "" : "; set override to by-pass state checks."),
        ri.getEncodedName(), override, ioe);
    }
    return trsp;
  }

  /**
   * Create one TransitRegionStateProcedure to unassign a region. This method is called from HBCK2.
   * @return an unassign or null
   */
  public TransitRegionStateProcedure createOneUnassignProcedure(RegionInfo ri, boolean override) {
    RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(ri);
    TransitRegionStateProcedure trsp = null;
    regionNode.lock();
    try {
      if (override) {
        if (regionNode.getProcedure() != null) {
          regionNode.unsetProcedure(regionNode.getProcedure());
        }
      } else {
        // This is where we could throw an exception; i.e. override is false.
        preTransitCheck(regionNode, STATES_EXPECTED_ON_UNASSIGN_OR_MOVE);
      }
      assert regionNode.getProcedure() == null;
      trsp =
        TransitRegionStateProcedure.unassign(getProcedureEnvironment(), regionNode.getRegionInfo());
      regionNode.setProcedure(trsp);
    } catch (IOException ioe) {
      // 'override' must be false here.
      LOG.info("Failed {} unassign, override=false; set override to by-pass state checks.",
        ri.getEncodedName(), ioe);
    } finally {
      regionNode.unlock();
    }
    return trsp;
  }

  /**
   * Create an array of TransitRegionStateProcedure w/o specifying a target server. Used as fallback
   * of caller is unable to do {@link #createAssignProcedures(Map)}.
   * <p/>
   * If no target server, at assign time, we will try to use the former location of the region if
   * one exists. This is how we 'retain' the old location across a server restart.
   * <p/>
   * Should only be called when you can make sure that no one can touch these regions other than
   * you. For example, when you are creating or enabling table. Presumes all Regions are in
   * appropriate state ripe for assign; no checking of Region state is done in here.
   * @see #createAssignProcedures(Map)
   */
  public TransitRegionStateProcedure[] createAssignProcedures(List<RegionInfo> hris) {
    return hris.stream().map(hri -> regionStates.getOrCreateRegionStateNode(hri))
      .map(regionNode -> createAssignProcedure(regionNode, null)).sorted(AssignmentManager::compare)
      .toArray(TransitRegionStateProcedure[]::new);
  }

  /**
   * Tied to {@link #createAssignProcedures(List)} in that it is called if caller is unable to run
   * this method. Presumes all Regions are in appropriate state ripe for assign; no checking of
   * Region state is done in here.
   * @param assignments Map of assignments from which we produce an array of AssignProcedures.
   * @return Assignments made from the passed in <code>assignments</code>
   * @see #createAssignProcedures(List)
   */
  private TransitRegionStateProcedure[]
    createAssignProcedures(Map<ServerName, List<RegionInfo>> assignments) {
    return assignments.entrySet().stream()
      .flatMap(e -> e.getValue().stream().map(hri -> regionStates.getOrCreateRegionStateNode(hri))
        .map(regionNode -> createAssignProcedure(regionNode, e.getKey())))
      .sorted(AssignmentManager::compare).toArray(TransitRegionStateProcedure[]::new);
  }

  // for creating unassign TRSP when disabling a table or closing excess region replicas
  private TransitRegionStateProcedure forceCreateUnssignProcedure(RegionStateNode regionNode) {
    regionNode.lock();
    try {
      if (regionNode.isInState(State.OFFLINE, State.CLOSED, State.SPLIT)) {
        return null;
      }
      // in general, a split parent should be in CLOSED or SPLIT state, but anyway, let's check it
      // here for safety
      if (regionNode.getRegionInfo().isSplit()) {
        LOG.warn("{} is a split parent but not in CLOSED or SPLIT state", regionNode);
        return null;
      }
      // As in DisableTableProcedure or ModifyTableProcedure, we will hold the xlock for table, so
      // we can make sure that this procedure has not been executed yet, as TRSP will hold the
      // shared lock for table all the time. So here we will unset it and when it is actually
      // executed, it will find that the attach procedure is not itself and quit immediately.
      if (regionNode.getProcedure() != null) {
        regionNode.unsetProcedure(regionNode.getProcedure());
      }
      return regionNode.setProcedure(TransitRegionStateProcedure.unassign(getProcedureEnvironment(),
        regionNode.getRegionInfo()));
    } finally {
      regionNode.unlock();
    }
  }

  /**
   * Called by DisableTableProcedure to unassign all the regions for a table.
   */
  public TransitRegionStateProcedure[] createUnassignProceduresForDisabling(TableName tableName) {
    return regionStates.getTableRegionStateNodes(tableName).stream()
      .map(this::forceCreateUnssignProcedure).filter(p -> p != null)
      .toArray(TransitRegionStateProcedure[]::new);
  }

  private int submitUnassignProcedure(TableName tableName,
    Function<RegionStateNode, Boolean> shouldSubmit, Consumer<RegionStateNode> logRIT,
    Consumer<TransitRegionStateProcedure> submit) {
    int inTransitionCount = 0;
    for (RegionStateNode regionNode : regionStates.getTableRegionStateNodes(tableName)) {
      regionNode.lock();
      try {
        if (shouldSubmit.apply(regionNode)) {
          if (regionNode.isInTransition()) {
            logRIT.accept(regionNode);
            inTransitionCount++;
            continue;
          }
          if (regionNode.isInState(State.OFFLINE, State.CLOSED, State.SPLIT)) {
            continue;
          }
          submit.accept(regionNode.setProcedure(TransitRegionStateProcedure
            .unassign(getProcedureEnvironment(), regionNode.getRegionInfo())));
        }
      } finally {
        regionNode.unlock();
      }
    }
    return inTransitionCount;
  }

  /**
   * Called by DsiableTableProcedure to unassign all regions for a table. Will skip submit unassign
   * procedure if the region is in transition, so you may need to call this method multiple times.
   * @param tableName the table for closing excess region replicas
   * @param submit    for submitting procedure
   * @return the number of regions in transition that we can not schedule unassign procedures
   */
  public int submitUnassignProcedureForDisablingTable(TableName tableName,
    Consumer<TransitRegionStateProcedure> submit) {
if(KnobRuntime.check(java.util.UUID.fromString("efaa11f0-8b2c-3d56-847e-8cd4e54ce9d8"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56afaa92-98f5-314f-b74f-a4f64e1f9e37"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31110ebb-be5f-37df-9db6-d114706f21b6"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b6da086-1c3b-326a-881e-6c02dd7b73c3"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("587e818e-8694-3bb2-b33a-c07a19f78283"))) {
return 0;
}
    return submitUnassignProcedure(tableName, rn -> true,
      rn -> LOG.debug("skip scheduling unassign procedure for {} when closing table regions "
        + "for disabling since it is in transition", rn),
      submit);
  }

  /**
   * Called by ModifyTableProcedure to unassign all the excess region replicas for a table. Will
   * skip submit unassign procedure if the region is in transition, so you may need to call this
   * method multiple times.
   * @param tableName       the table for closing excess region replicas
   * @param newReplicaCount the new replica count, should be less than current replica count
   * @param submit          for submitting procedure
   * @return the number of regions in transition that we can not schedule unassign procedures
   */
  public int submitUnassignProcedureForClosingExcessRegionReplicas(TableName tableName,
    int newReplicaCount, Consumer<TransitRegionStateProcedure> submit) {
if(KnobRuntime.check(java.util.UUID.fromString("3d4ad93c-6c45-36d1-bc66-fcfcd1ccdc35"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6241dc3e-fb0e-358e-b3cc-3ef2b97523a4"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5a96886-37af-3fcd-b61e-9c8da34fdcb7"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("354e497f-b1c6-3031-b125-a7cd51f288ad"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17f25c17-7358-3f6a-b1ba-9910a4eaec1e"))) {
return 0;
}
    return submitUnassignProcedure(tableName,
      rn -> rn.getRegionInfo().getReplicaId() >= newReplicaCount,
      rn -> LOG.debug("skip scheduling unassign procedure for {} when closing excess region "
        + "replicas since it is in transition", rn),
      submit);
  }

  private int numberOfUnclosedRegions(TableName tableName,
    Function<RegionStateNode, Boolean> shouldSubmit) {
    int unclosed = 0;
    for (RegionStateNode regionNode : regionStates.getTableRegionStateNodes(tableName)) {
      regionNode.lock();
      try {
        if (shouldSubmit.apply(regionNode)) {
          if (!regionNode.isInState(State.OFFLINE, State.CLOSED, State.SPLIT)) {
            unclosed++;
          }
        }
      } finally {
        regionNode.unlock();
      }
    }
    return unclosed;
  }

  public int numberOfUnclosedRegionsForDisabling(TableName tableName) {
    return numberOfUnclosedRegions(tableName, rn -> true);
  }

  public int numberOfUnclosedExcessRegionReplicas(TableName tableName, int newReplicaCount) {
    return numberOfUnclosedRegions(tableName,
      rn -> rn.getRegionInfo().getReplicaId() >= newReplicaCount);
  }

  public SplitTableRegionProcedure createSplitProcedure(final RegionInfo regionToSplit,
    final byte[] splitKey) throws IOException {
    return new SplitTableRegionProcedure(getProcedureEnvironment(), regionToSplit, splitKey);
  }

  public TruncateRegionProcedure createTruncateRegionProcedure(final RegionInfo regionToTruncate)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b858bf4c-7577-3c22-94aa-d61c8e76f29d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("808cee35-a1aa-3ab2-8369-260ee8f0e865"))) {
return null;
}
    return new TruncateRegionProcedure(getProcedureEnvironment(), regionToTruncate);
  }

  public MergeTableRegionsProcedure createMergeProcedure(RegionInfo... ris) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3cfa5b02-4bf8-37cd-882b-6688f6f9ed88"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("2503bd7f-90ae-32d9-bf3e-e021ce25bb1c"))) {
throw new java.io.IOException("Injected exception");
}
    return new MergeTableRegionsProcedure(getProcedureEnvironment(), ris, false);
  }

  /**
   * Delete the region states. This is called by "DeleteTable"
   */
  public void deleteTable(final TableName tableName) throws IOException {
    final ArrayList<RegionInfo> regions = regionStates.getTableRegionsInfo(tableName);
if(KnobRuntime.check(java.util.UUID.fromString("a3809ed9-6189-34d8-a7c0-c9200fdaed8e"))) {
throw new java.io.IOException("Injected exception");
}
    regionStateStore.deleteRegions(regions);
    for (int i = 0; i < regions.size(); ++i) {
      final RegionInfo regionInfo = regions.get(i);
      regionStates.deleteRegion(regionInfo);
    }
  }

  // ============================================================================================
  // RS Region Transition Report helpers
  // ============================================================================================
  private void reportRegionStateTransition(ReportRegionStateTransitionResponse.Builder builder,
    ServerStateNode serverNode, List<RegionStateTransition> transitionList) throws IOException {
    for (RegionStateTransition transition : transitionList) {
      switch (transition.getTransitionCode()) {
        case OPENED:
        case FAILED_OPEN:
        case CLOSED:
          assert transition.getRegionInfoCount() == 1 : transition;
          final RegionInfo hri = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
          long procId =
            transition.getProcIdCount() > 0 ? transition.getProcId(0) : Procedure.NO_PROC_ID;
          updateRegionTransition(serverNode, transition.getTransitionCode(), hri,
            transition.hasOpenSeqNum() ? transition.getOpenSeqNum() : HConstants.NO_SEQNUM, procId);
          break;
        case READY_TO_SPLIT:
        case SPLIT:
        case SPLIT_REVERTED:
          assert transition.getRegionInfoCount() == 3 : transition;
          final RegionInfo parent = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
          final RegionInfo splitA = ProtobufUtil.toRegionInfo(transition.getRegionInfo(1));
          final RegionInfo splitB = ProtobufUtil.toRegionInfo(transition.getRegionInfo(2));
if(KnobRuntime.check(java.util.UUID.fromString("566367a1-d868-31ec-a524-fd95218fcd26"))) {
throw new java.io.IOException("Injected exception");
}
          updateRegionSplitTransition(serverNode, transition.getTransitionCode(), parent, splitA,
            splitB);
          break;
        case READY_TO_MERGE:
        case MERGED:
        case MERGE_REVERTED:
          assert ((KnobRuntime.check(java.util.UUID.fromString("dfda70b9-5f34-3314-82d5-3cba4f142b08"))) ? ((transition.getProcIdCount()) == (3)) : (((KnobRuntime.check(java.util.UUID.fromString("1db5d48f-a5cf-30a2-8edc-762fecff8bcc"))) ? ((transition.getRegionInfoCount()) == (3)) : (((KnobRuntime.check(java.util.UUID.fromString("00497647-93f7-348b-a2e4-6f66af4b4784"))) ? ((transition.getProcIdCount()) != (3)) : (((KnobRuntime.check(java.util.UUID.fromString("e761fb61-517a-362f-b7a7-19046f86fb93"))) ? ((transition.getRegionInfoCount()) != (3)) : (transition.getRegionInfoCount() == 3)))))))) : transition;
          final RegionInfo merged = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
          final RegionInfo mergeA = ProtobufUtil.toRegionInfo(transition.getRegionInfo(1));
          final RegionInfo mergeB = ProtobufUtil.toRegionInfo(transition.getRegionInfo(2));
          updateRegionMergeTransition(serverNode, transition.getTransitionCode(), merged, mergeA,
            mergeB);
          break;
      }
    }
  }

  public ReportRegionStateTransitionResponse reportRegionStateTransition(
    final ReportRegionStateTransitionRequest req) throws PleaseHoldException {
if(KnobRuntime.check(java.util.UUID.fromString("86e63083-d43c-3e4f-abba-872d2bde6289"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40a9fe0b-0d72-3757-beb6-e67785dd594f"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5be2cf56-a2c5-3f8a-b9df-f6129e2ad6ea"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("2e5eb531-4c13-3456-ab88-fd4793dcd7a4"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06b8e03c-5655-31da-9db3-3b792c7f70cd"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    ReportRegionStateTransitionResponse.Builder builder =
      ReportRegionStateTransitionResponse.newBuilder();
    ServerName serverName = ProtobufUtil.toServerName(req.getServer());
if(KnobRuntime.check(java.util.UUID.fromString("fb252174-2e50-37e4-a647-db071c8736cd"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    ServerStateNode serverNode = regionStates.getServerNode(serverName);
    if (((KnobRuntime.check(java.util.UUID.fromString("45026b18-472e-37f3-94c3-7d5ffa9f8e61"))) ? ((serverNode) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0310d4b9-70b0-3f6f-ad07-e08a8ef66e7c"))) ? ((serverNode) != (null)) : (serverNode == null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("ff3020a5-02cc-3f74-9e2a-bfface9252f0"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("a12ea909-8fc1-3e18-8ab6-f10f6998e4c1"))) { LOG.debug("No server node for {}", serverName); } else { LOG.warn("No server node for {}", serverName); }
      builder.setErrorMessage("No server node for " + serverName);
      return builder.build();
    }
    // here we have to acquire a read lock instead of a simple exclusive lock. This is because that
    // we should not block other reportRegionStateTransition call from the same region server. This
    // is not only about performance, but also to prevent dead lock. Think of the meta region is
    // also on the same region server and you hold the lock which blocks the
    // reportRegionStateTransition for meta, and since meta is not online, you will block inside the
    // lock protection to wait for meta online...
    serverNode.readLock().lock();
    try {
      // we only accept reportRegionStateTransition if the region server is online, see the comment
      // above in submitServerCrash method and HBASE-21508 for more details.
      if (serverNode.isInState(ServerState.ONLINE)) {
        try {
          reportRegionStateTransition(builder, serverNode, req.getTransitionList());
        } catch (PleaseHoldException e) {
          LOG.trace("Failed transition ", e);
          throw e;
        } catch (UnsupportedOperationException | IOException e) {
          // TODO: at the moment we have a single error message and the RS will abort
          // if the master says that one of the region transitions failed.
          if (KnobRuntime.check(java.util.UUID.fromString("4806de4b-763d-365f-9f49-024351ad7d34"))) { LOG.trace("Failed transition", e); } else if (KnobRuntime.check(java.util.UUID.fromString("7f470824-a1d4-3861-a769-07b41244356a"))) { LOG.error("Failed transition", e); } else { LOG.warn("Failed transition", e); }
          builder.setErrorMessage("Failed transition " + e.getMessage());
        }
      } else {
        LOG.warn("The region server {} is already dead, skip reportRegionStateTransition call",
          serverName);
        builder.setErrorMessage("You are dead");
      }
    } finally {
      serverNode.readLock().unlock();
    }

    return builder.build();
  }

  private void updateRegionTransition(ServerStateNode serverNode, TransitionCode state,
    RegionInfo regionInfo, long seqId, long procId) throws IOException {
    checkMetaLoaded(regionInfo);

    RegionStateNode regionNode = regionStates.getRegionStateNode(regionInfo);
    if (regionNode == null) {
      // the table/region is gone. maybe a delete, split, merge
      throw new UnexpectedStateException(String.format(
        "Server %s was trying to transition region %s to %s. but Region is not known.",
        serverNode.getServerName(), regionInfo, state));
    }
    LOG.trace("Update region transition serverName={} region={} regionState={}",
      serverNode.getServerName(), regionNode, state);

    regionNode.lock();
    try {
      if (!reportTransition(regionNode, serverNode, state, seqId, procId)) {
        // Don't log WARN if shutting down cluster; during shutdown. Avoid the below messages:
        // 2018-08-13 10:45:10,551 WARN ...AssignmentManager: No matching procedure found for
        // rit=OPEN, location=ve0538.halxg.cloudera.com,16020,1533493000958,
        // table=IntegrationTestBigLinkedList, region=65ab289e2fc1530df65f6c3d7cde7aa5 transition
        // to CLOSED
        // These happen because on cluster shutdown, we currently let the RegionServers close
        // regions. This is the only time that region close is not run by the Master (so cluster
        // goes down fast). Consider changing it so Master runs all shutdowns.
        if (
          ((KnobRuntime.check(java.util.UUID.fromString("a8bb07cd-70f2-36e5-9778-e0af742c9814"))) ? (isMetaLoaded()) : (((KnobRuntime.check(java.util.UUID.fromString("89c5161c-5e77-3be9-9752-975ed64b0899"))) ? ((isMetaAssigned()) || (state.equals(TransitionCode.CLOSED))) : (((KnobRuntime.check(java.util.UUID.fromString("2d0b5a6c-6035-367b-85ff-876cdb984566"))) ? (isMetaAssigned()) : (((KnobRuntime.check(java.util.UUID.fromString("21a80977-43a8-3190-b833-dd6497256668"))) ? ((this.master.getServerManager().isClusterShutdown()) && (state.equals(TransitionCode.CLOSED))) : (((KnobRuntime.check(java.util.UUID.fromString("d210b38a-9356-399f-af1c-8cfb5a52a88d"))) ? ((isMetaAssigned()) && (state.equals(TransitionCode.CLOSED))) : (((KnobRuntime.check(java.util.UUID.fromString("daf3483d-074c-3e64-befc-521af5816a10"))) ? (state.equals(TransitionCode.CLOSED)) : (((KnobRuntime.check(java.util.UUID.fromString("50423636-fc1f-3a41-b5ab-46e231fdc3fe"))) ? ((isMetaLoaded()) || (state.equals(TransitionCode.CLOSED))) : (((KnobRuntime.check(java.util.UUID.fromString("7425f08e-62c2-32b0-92d3-0f69d49ad5a9"))) ? ((this.master.getServerManager().isClusterShutdown()) || (state.equals(TransitionCode.CLOSED))) : (((KnobRuntime.check(java.util.UUID.fromString("4a0214d8-50e5-310f-9395-ed8b3a465a95"))) ? ((isMetaLoaded()) && (state.equals(TransitionCode.CLOSED))) : (((KnobRuntime.check(java.util.UUID.fromString("dcdbddd8-ce39-3fbc-9e1a-74066b7d7190"))) ? (this.master.getServerManager().isClusterShutdown()) : (this.master.getServerManager().isClusterShutdown() && state.equals(TransitionCode.CLOSED)))))))))))))))))))))
        ) {
          LOG.info("RegionServer {} {}", state, regionNode.getRegionInfo().getEncodedName());
        } else {
          LOG.warn("No matching procedure found for {} transition on {} to {}",
            serverNode.getServerName(), regionNode, state);
        }
      }
    } finally {
      regionNode.unlock();
    }
  }

  private boolean reportTransition(RegionStateNode regionNode, ServerStateNode serverNode,
    TransitionCode state, long seqId, long procId) throws IOException {
    ServerName serverName = serverNode.getServerName();
    TransitRegionStateProcedure proc = regionNode.getProcedure();
    if (((KnobRuntime.check(java.util.UUID.fromString("68462775-6d1d-3529-901e-c6ad1b15ce2b"))) ? ((proc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d3101f39-dc6b-3a17-83d0-4f5c0f73dcc1"))) ? ((proc) != (null)) : (proc == null))))) {
      return false;
    }
if(KnobRuntime.check(java.util.UUID.fromString("f22a9904-a6ba-3f61-8ecc-42320c65b6c2"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("093d94eb-6bee-34bb-93cb-bec611969af8"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("696c0d1a-5754-3f2c-9396-1f923a62cce8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("62df2f23-2c40-396b-aee5-8c511df4c394"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7e1aaca-fee3-3bed-980d-b41975cf9b75"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f309fd04-ddb1-3739-bbce-04754a96d8ad"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1e211770-a5fa-3dde-b7ab-1ee3e15dfb70"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6623079b-db33-3a0e-8a98-5cfe1b1812c9"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f356c8e6-3ee1-33b2-b551-656f1b9e4772"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe62a1ee-beca-395b-a7ba-c5f50592a203"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f1ebf5f-57cf-3d0d-9487-6e2d07e63c50"))) {
seqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4f276ba9-92e5-3e89-8665-08746535a48c"))) {
seqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6cfb899-d6ad-3232-ae7a-4ed3f1366952"))) {
seqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a8d5e18-14f3-308e-9704-ec9444351432"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("32b4e8a4-2184-3b00-9227-dfa25e6cd4ba"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    proc.reportTransition(master.getMasterProcedureExecutor().getEnvironment(), regionNode,
      serverName, state, seqId, procId);
    return true;
  }

  private void updateRegionSplitTransition(final ServerStateNode serverNode,
    final TransitionCode state, final RegionInfo parent, final RegionInfo hriA,
    final RegionInfo hriB) throws IOException {
    checkMetaLoaded(parent);

    if (state != TransitionCode.READY_TO_SPLIT) {
      throw new UnexpectedStateException(
        "unsupported split regionState=" + state + " for parent region " + parent
          + " maybe an old RS (< 2.0) had the operation in progress");
    }

    // sanity check on the request
    if (!Bytes.equals(hriA.getEndKey(), hriB.getStartKey())) {
      throw new UnsupportedOperationException("unsupported split request with bad keys: parent="
        + parent + " hriA=" + hriA + " hriB=" + hriB);
    }

    if (!master.isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
      if (KnobRuntime.check(java.util.UUID.fromString("37f3cbbb-4def-397c-82e2-52142f624fe4"))) { LOG.info("Split switch is off! skip split of "); } else if (KnobRuntime.check(java.util.UUID.fromString("9df707ea-c20a-3007-94e5-474cb15a5260"))) { LOG.info(("Split switch is off! skip split of ") + (parent)); } else if (KnobRuntime.check(java.util.UUID.fromString("14b4fea7-6eb7-36d9-a1be-ad60b290b8f3"))) { LOG.trace(("Split switch is off! skip split of ") + (parent)); } else if (KnobRuntime.check(java.util.UUID.fromString("d74992e9-aadb-36b0-8333-85af6e81cb46"))) { LOG.debug(("Split switch is off! skip split of ") + (parent)); } else if (KnobRuntime.check(java.util.UUID.fromString("79404c27-73cf-3bcc-a153-50cec2b35cf0"))) { LOG.trace("Split switch is off! skip split of "); } else if (KnobRuntime.check(java.util.UUID.fromString("72ed8134-c2ba-309c-8d47-f05b188a493c"))) { LOG.debug("Split switch is off! skip split of "); } else { LOG.warn("Split switch is off! skip split of " + parent); }
      throw new DoNotRetryIOException(
        "Split region " + parent.getRegionNameAsString() + " failed due to split switch off");
    }

    // Submit the Split procedure
    final byte[] splitKey = hriB.getStartKey();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Split request from {}, parent={}, splitKey={}", serverNode.getServerName(), parent,
        Bytes.toStringBinary(splitKey));
    }
    // Processing this report happens asynchronously from other activities which can mutate
    // the region state. For example, a split procedure may already be running for this parent.
    // A split procedure cannot succeed if the parent region is no longer open, so we can
    // ignore it in that case.
    // Note that submitting more than one split procedure for a given region is
    // harmless -- the split is fenced in the procedure handling -- but it would be noisy in
    // the logs. Only one procedure can succeed. The other procedure(s) would abort during
    // initialization and report failure with WARN level logging.
    RegionState parentState = regionStates.getRegionState(parent);
    if (parentState != null && parentState.isOpened()) {
      master.getMasterProcedureExecutor().submitProcedure(createSplitProcedure(parent, splitKey));
    } else {
      LOG.info("Ignoring split request from {}, parent={} because parent is unknown or not open",
        serverNode.getServerName(), parent);
      return;
    }

    // If the RS is < 2.0 throw an exception to abort the operation, we are handling the split
    if (master.getServerManager().getVersionNumber(serverNode.getServerName()) < 0x0200000) {
      throw new UnsupportedOperationException(
        String.format("Split handled by the master: " + "parent=%s hriA=%s hriB=%s",
          parent.getShortNameToLog(), hriA, hriB));
    }
  }

  private void updateRegionMergeTransition(final ServerStateNode serverNode,
    final TransitionCode state, final RegionInfo merged, final RegionInfo hriA,
    final RegionInfo hriB) throws IOException {
    checkMetaLoaded(merged);

    if (state != TransitionCode.READY_TO_MERGE) {
      throw new UnexpectedStateException(
        "Unsupported merge regionState=" + state + " for regionA=" + hriA + " regionB=" + hriB
          + " merged=" + merged + " maybe an old RS (< 2.0) had the operation in progress");
    }

    if (!master.isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
      LOG.warn("Merge switch is off! skip merge of regionA=" + hriA + " regionB=" + hriB);
      throw new DoNotRetryIOException(
        "Merge of regionA=" + hriA + " regionB=" + hriB + " failed because merge switch is off");
    }

    // Submit the Merge procedure
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling merge request from RS=" + merged + ", merged=" + merged);
    }
    master.getMasterProcedureExecutor().submitProcedure(createMergeProcedure(hriA, hriB));

    // If the RS is < 2.0 throw an exception to abort the operation, we are handling the merge
    if (master.getServerManager().getVersionNumber(serverNode.getServerName()) < 0x0200000) {
      throw new UnsupportedOperationException(
        String.format("Merge not handled yet: regionState=%s merged=%s hriA=%s hriB=%s", state,
          merged, hriA, hriB));
    }
  }

  // ============================================================================================
  // RS Status update (report online regions) helpers
  // ============================================================================================
  /**
   * The master will call this method when the RS send the regionServerReport(). The report will
   * contains the "online regions". This method will check the the online regions against the
   * in-memory state of the AM, and we will log a warn message if there is a mismatch. This is
   * because that there is no fencing between the reportRegionStateTransition method and
   * regionServerReport method, so there could be race and introduce inconsistency here, but
   * actually there is no problem.
   * <p/>
   * Please see HBASE-21421 and HBASE-21463 for more details.
   */
  public void reportOnlineRegions(ServerName serverName, Set<byte[]> regionNames) {
    if (!isRunning()) {
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("ReportOnlineRegions {} regionCount={}, metaLoaded={} {}", serverName,
        regionNames.size(), isMetaLoaded(),
        regionNames.stream().map(Bytes::toStringBinary).collect(Collectors.toList()));
    }

    ServerStateNode serverNode = regionStates.getServerNode(serverName);
    if (serverNode == null) {
      LOG.warn("Got a report from server {} where its server node is null", serverName);
      return;
    }
    serverNode.readLock().lock();
    try {
      if (!serverNode.isInState(ServerState.ONLINE)) {
        LOG.warn("Got a report from a server result in state {}", serverNode);
        return;
      }
    } finally {
      serverNode.readLock().unlock();
    }

    // Track the regionserver reported online regions in memory.
    synchronized (rsReports) {
      rsReports.put(serverName, regionNames);
    }

    if (regionNames.isEmpty()) {
      // nothing to do if we don't have regions
      LOG.trace("no online region found on {}", serverName);
      return;
    }
    if (!isMetaLoaded()) {
      // we are still on startup, skip checking
      return;
    }
    // The Heartbeat tells us of what regions are on the region serve, check the state.
    checkOnlineRegionsReport(serverNode, regionNames);
  }

  /**
   * Close <code>regionName</code> on <code>sn</code> silently and immediately without using a
   * Procedure or going via hbase:meta. For case where a RegionServer's hosting of a Region is not
   * aligned w/ the Master's accounting of Region state. This is for cleaning up an error in
   * accounting.
   */
  private void closeRegionSilently(ServerName sn, byte[] regionName) {
    try {
      RegionInfo ri = MetaTableAccessor.parseRegionInfoFromRegionName(regionName);
      // Pass -1 for timeout. Means do not wait.
      ServerManager.closeRegionSilentlyAndWait(this.master.getClusterConnection(), sn, ri, -1);
    } catch (Exception e) {
      LOG.error("Failed trying to close {} on {}", Bytes.toStringBinary(regionName), sn, e);
    }
  }

  /**
   * Check that what the RegionServer reports aligns with the Master's image. If disagreement, we
   * will tell the RegionServer to expediently close a Region we do not think it should have.
   */
  private void checkOnlineRegionsReport(ServerStateNode serverNode, Set<byte[]> regionNames) {
if(KnobRuntime.check(java.util.UUID.fromString("432ea3ea-36ff-3301-aaf2-056a1121d502"))) {
return;
}
    ServerName serverName = serverNode.getServerName();
    for (byte[] regionName : regionNames) {
      if (!isRunning()) {
        return;
      }
      RegionStateNode regionNode = regionStates.getRegionStateNodeFromName(regionName);
      if (regionNode == null) {
        String regionNameAsStr = Bytes.toStringBinary(regionName);
if(KnobRuntime.check(java.util.UUID.fromString("568a2938-dde3-324c-9811-0e8fc9ab3c8d"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        LOG.warn("No RegionStateNode for {} but reported as up on {}; closing...", regionNameAsStr,
          serverName);
        closeRegionSilently(serverNode.getServerName(), regionName);
        continue;
      }
      final long lag = 1000;
      // This is just a fallback check designed to identify unexpected data inconsistencies, so we
      // use tryLock to attempt to acquire the lock, and if the lock cannot be acquired, we skip the
      // check. This will not cause any additional problems and also prevents the regionServerReport
      // call from being stuck for too long which may cause deadlock on region assignment.
      if (regionNode.tryLock()) {
        try {
          long diff = EnvironmentEdgeManager.currentTime() - regionNode.getLastUpdate();
          if (regionNode.isInState(State.OPENING, State.OPEN)) {
            // This is possible as a region server has just closed a region but the region server
            // report is generated before the closing, but arrive after the closing. Make sure
            // there
            // is some elapsed time so less false alarms.
            if (!regionNode.getRegionLocation().equals(serverName) && diff > lag) {
              LOG.warn("Reporting {} server does not match {} (time since last "
                + "update={}ms); closing...", serverName, regionNode, diff);
              closeRegionSilently(serverNode.getServerName(), regionName);
            }
          } else if (!regionNode.isInState(State.CLOSING, State.SPLITTING)) {
            // So, we can get report that a region is CLOSED or SPLIT because a heartbeat
            // came in at about same time as a region transition. Make sure there is some
            // elapsed time so less false alarms.
            if (diff > lag) {
              LOG.warn("Reporting {} state does not match {} (time since last update={}ms)",
                serverName, regionNode, diff);
            }
          }
        } finally {
          regionNode.unlock();
        }
      } else {
        LOG.warn(
          "Unable to acquire lock for regionNode {}. It is likely that another thread is currently holding the lock. To avoid deadlock, skip execution for now.",
          regionNode);
      }
    }
  }

  // ============================================================================================
  // RIT chore
  // ============================================================================================
  private static class RegionInTransitionChore extends ProcedureInMemoryChore<MasterProcedureEnv> {
    public RegionInTransitionChore(final int timeoutMsec) {
      super(timeoutMsec);
    }

    @Override
    protected void periodicExecute(final MasterProcedureEnv env) {
      final AssignmentManager am = env.getAssignmentManager();

      final RegionInTransitionStat ritStat = am.computeRegionInTransitionStat();
      if (ritStat.hasRegionsOverThreshold()) {
        for (RegionState hri : ritStat.getRegionOverThreshold()) {
          am.handleRegionOverStuckWarningThreshold(hri.getRegion());
        }
      }

      // update metrics
      am.updateRegionsInTransitionMetrics(ritStat);
    }
  }

  private static class DeadServerMetricRegionChore
    extends ProcedureInMemoryChore<MasterProcedureEnv> {
    public DeadServerMetricRegionChore(final int timeoutMsec) {
      super(timeoutMsec);
    }

    @Override
    protected void periodicExecute(final MasterProcedureEnv env) {
      final ServerManager sm = env.getMasterServices().getServerManager();
      final AssignmentManager am = env.getAssignmentManager();
      // To minimize inconsistencies we are not going to snapshot live servers in advance in case
      // new servers are added; OTOH we don't want to add heavy sync for a consistent view since
      // this is for metrics. Instead, we're going to check each regions as we go; to avoid making
      // too many checks, we maintain a local lists of server, limiting us to false negatives. If
      // we miss some recently-dead server, we'll just see it next time.
      Set<ServerName> recentlyLiveServers = new HashSet<>();
      int deadRegions = 0, unknownRegions = 0;
      for (RegionStateNode rsn : am.getRegionStates().getRegionStateNodes()) {
        if (rsn.getState() != State.OPEN) {
          continue; // Opportunistic check, should quickly skip RITs, offline tables, etc.
        }
        // Do not need to acquire region state lock as this is only for showing metrics.
        ServerName sn = rsn.getRegionLocation();
        State state = rsn.getState();
        if (state != State.OPEN) {
          continue; // Mostly skipping RITs that are already being take care of.
        }
        if (sn == null) {
          ++unknownRegions; // Opened on null?
          continue;
        }
        if (recentlyLiveServers.contains(sn)) {
          continue;
        }
        ServerManager.ServerLiveState sls = sm.isServerKnownAndOnline(sn);
        switch (sls) {
          case LIVE:
            recentlyLiveServers.add(sn);
            break;
          case DEAD:
            ++deadRegions;
            break;
          case UNKNOWN:
            ++unknownRegions;
            break;
          default:
            throw new AssertionError("Unexpected " + sls);
        }
      }
      if (deadRegions > 0 || unknownRegions > 0) {
        LOG.info("Found {} OPEN regions on dead servers and {} OPEN regions on unknown servers",
          deadRegions, unknownRegions);
      }

      am.updateDeadServerRegionMetrics(deadRegions, unknownRegions);
    }
  }

  public RegionInTransitionStat computeRegionInTransitionStat() {
    final RegionInTransitionStat rit = new RegionInTransitionStat(getConfiguration());
    rit.update(this);
    return rit;
  }

  public static class RegionInTransitionStat {
    private final int ritThreshold;

    private HashMap<String, RegionState> ritsOverThreshold = null;
    private long statTimestamp;
    private long oldestRITTime = 0;
    private int totalRITsTwiceThreshold = 0;
    private int totalRITs = 0;

    public RegionInTransitionStat(final Configuration conf) {
      this.ritThreshold =
        conf.getInt(METRICS_RIT_STUCK_WARNING_THRESHOLD, DEFAULT_RIT_STUCK_WARNING_THRESHOLD);
    }

    public int getRITThreshold() {
      return ritThreshold;
    }

    public long getTimestamp() {
      return statTimestamp;
    }

    public int getTotalRITs() {
      return totalRITs;
    }

    public long getOldestRITTime() {
      return oldestRITTime;
    }

    public int getTotalRITsOverThreshold() {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null ? m.size() : 0;
    }

    public boolean hasRegionsTwiceOverThreshold() {
      return totalRITsTwiceThreshold > 0;
    }

    public boolean hasRegionsOverThreshold() {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null && !m.isEmpty();
    }

    public Collection<RegionState> getRegionOverThreshold() {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null ? m.values() : Collections.emptySet();
    }

    public boolean isRegionOverThreshold(final RegionInfo regionInfo) {
      Map<String, RegionState> m = this.ritsOverThreshold;
      return m != null && m.containsKey(regionInfo.getEncodedName());
    }

    public boolean isRegionTwiceOverThreshold(final RegionInfo regionInfo) {
      Map<String, RegionState> m = this.ritsOverThreshold;
      if (m == null) {
        return false;
      }
      final RegionState state = m.get(regionInfo.getEncodedName());
      if (state == null) {
        return false;
      }
      return (statTimestamp - state.getStamp()) > (ritThreshold * 2);
    }

    protected void update(final AssignmentManager am) {
      final RegionStates regionStates = am.getRegionStates();
      this.statTimestamp = EnvironmentEdgeManager.currentTime();
      update(regionStates.getRegionsStateInTransition(), statTimestamp);
      update(regionStates.getRegionFailedOpen(), statTimestamp);

      if (LOG.isDebugEnabled() && ritsOverThreshold != null && !ritsOverThreshold.isEmpty()) {
        LOG.debug("RITs over threshold: {}",
          ritsOverThreshold.entrySet().stream()
            .map(e -> e.getKey() + ":" + e.getValue().getState().name())
            .collect(Collectors.joining("\n")));
      }
    }

    private void update(final Collection<RegionState> regions, final long currentTime) {
      for (RegionState state : regions) {
        totalRITs++;
        final long ritStartedMs = state.getStamp();
        if (ritStartedMs == 0) {
          // Don't output bogus values to metrics if they accidentally make it here.
          LOG.warn("The RIT {} has no start time", state.getRegion());
          continue;
        }
        final long ritTime = currentTime - ritStartedMs;
        if (ritTime > ritThreshold) {
          if (ritsOverThreshold == null) {
            ritsOverThreshold = new HashMap<String, RegionState>();
          }
          ritsOverThreshold.put(state.getRegion().getEncodedName(), state);
          totalRITsTwiceThreshold += (ritTime > (ritThreshold * 2)) ? 1 : 0;
        }
        if (oldestRITTime < ritTime) {
          oldestRITTime = ritTime;
        }
      }
    }
  }

  private void updateRegionsInTransitionMetrics(final RegionInTransitionStat ritStat) {
    metrics.updateRITOldestAge(ritStat.getOldestRITTime());
    metrics.updateRITCount(ritStat.getTotalRITs());
    metrics.updateRITCountOverThreshold(ritStat.getTotalRITsOverThreshold());
  }

  private void updateDeadServerRegionMetrics(int deadRegions, int unknownRegions) {
    metrics.updateDeadServerOpenRegions(deadRegions);
    metrics.updateUnknownServerOpenRegions(unknownRegions);
  }

  private void handleRegionOverStuckWarningThreshold(final RegionInfo regionInfo) {
    final RegionStateNode regionNode = regionStates.getRegionStateNode(regionInfo);
    // if (regionNode.isStuck()) {
    LOG.warn("STUCK Region-In-Transition {}", regionNode);
  }

  // ============================================================================================
  // TODO: Master load/bootstrap
  // ============================================================================================
  public void joinCluster() throws IOException {
    long startTime = System.nanoTime();
    LOG.debug("Joining cluster...");

    // Scan hbase:meta to build list of existing regions, servers, and assignment.
    // hbase:meta is online now or will be. Inside loadMeta, we keep trying. Can't make progress
    // w/o meta.
    loadMeta();

    while (master.getServerManager().countOfRegionServers() < 1) {
      LOG.info("Waiting for RegionServers to join; current count={}",
        master.getServerManager().countOfRegionServers());
      Threads.sleep(250);
    }
    LOG.info("Number of RegionServers={}", master.getServerManager().countOfRegionServers());

    // Start the chores
    master.getMasterProcedureExecutor().addChore(this.ritChore);
    master.getMasterProcedureExecutor().addChore(this.deadMetricChore);

    long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    LOG.info("Joined the cluster in {}", StringUtils.humanTimeDiff(costMs));
  }

  /**
   * Create assign procedure for offline regions. Just follow the old
   * processofflineServersWithOnlineRegions method. Since now we do not need to deal with dead
   * server any more, we only deal with the regions in OFFLINE state in this method. And this is a
   * bit strange, that for new regions, we will add it in CLOSED state instead of OFFLINE state, and
   * usually there will be a procedure to track them. The processofflineServersWithOnlineRegions is
   * a legacy from long ago, as things are going really different now, maybe we do not need this
   * method any more. Need to revisit later.
   */
  // Public so can be run by the Master as part of the startup. Needs hbase:meta to be online.
  // Needs to be done after the table state manager has been started.
  public void processOfflineRegions() {
    TransitRegionStateProcedure[] procs =
      regionStates.getRegionStateNodes().stream().filter(rsn -> rsn.isInState(State.OFFLINE))
        .filter(rsn -> isTableEnabled(rsn.getRegionInfo().getTable())).map(rsn -> {
          rsn.lock();
          try {
            if (rsn.getProcedure() != null) {
              return null;
            } else {
              return rsn.setProcedure(TransitRegionStateProcedure.assign(getProcedureEnvironment(),
                rsn.getRegionInfo(), null));
            }
          } finally {
            rsn.unlock();
          }
        }).filter(p -> p != null).toArray(TransitRegionStateProcedure[]::new);
    if (((KnobRuntime.check(java.util.UUID.fromString("0ce1946d-8774-3e19-abc5-65dcc7a38084"))) ? ((procs.length) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("622f11e9-a239-3cd6-bd62-77dc8418617e"))) ? ((procs.length) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fd345a48-6ca6-37c2-8022-c0752220a96c"))) ? ((procs.length) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9cf515ee-6fb9-32f9-b1ed-5cdd1105e29d"))) ? ((procs.length) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8b5481b3-2d09-3c55-aeac-e2dc119337bb"))) ? ((procs.length) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ed0e55ff-6b23-3f23-82b6-b15b00c37938"))) ? ((procs.length) == (0)) : (procs.length > 0))))))))))))) {
      master.getMasterProcedureExecutor().submitProcedures(procs);
    }
  }

  /*
   * AM internal RegionStateStore.RegionStateVisitor implementation. To be used when scanning META
   * table for region rows, using RegionStateStore utility methods. RegionStateStore methods will
   * convert Result into proper RegionInfo instances, but those would still need to be added into
   * AssignmentManager.regionStates in-memory cache. RegionMetaLoadingVisitor.visitRegionState
   * method provides the logic for adding RegionInfo instances as loaded from latest META scan into
   * AssignmentManager.regionStates.
   */
  private class RegionMetaLoadingVisitor implements RegionStateStore.RegionStateVisitor {

    @Override
    public void visitRegionState(Result result, final RegionInfo regionInfo, final State state,
      final ServerName regionLocation, final ServerName lastHost, final long openSeqNum) {
      if (
        state == null && regionLocation == null && lastHost == null
          && openSeqNum == SequenceId.NO_SEQUENCE_ID
      ) {
        // This is a row with nothing in it.
        LOG.warn("Skipping empty row={}", result);
        return;
      }
      State localState = state;
      if (localState == null) {
        // No region state column data in hbase:meta table! Are I doing a rolling upgrade from
        // hbase1 to hbase2? Am I restoring a SNAPSHOT or otherwise adding a region to hbase:meta?
        // In any of these cases, state is empty. For now, presume OFFLINE but there are probably
        // cases where we need to probe more to be sure this correct; TODO informed by experience.
        LOG.info(regionInfo.getEncodedName() + " regionState=null; presuming " + State.OFFLINE);
        localState = State.OFFLINE;
      }
      RegionStateNode regionNode = regionStates.getOrCreateRegionStateNode(regionInfo);
      // Do not need to lock on regionNode, as we can make sure that before we finish loading
      // meta, all the related procedures can not be executed. The only exception is for meta
      // region related operations, but here we do not load the informations for meta region.
      regionNode.setState(localState);
      regionNode.setLastHost(lastHost);
      regionNode.setRegionLocation(regionLocation);
      regionNode.setOpenSeqNum(openSeqNum);

      // Note: keep consistent with other methods, see region(Opening|Opened|Closing)
      // RIT/ServerCrash handling should take care of the transiting regions.
      if (
        localState.matches(State.OPEN, State.OPENING, State.CLOSING, State.SPLITTING, State.MERGING)
      ) {
        assert regionLocation != null : "found null region location for " + regionNode;
        // TODO: this could lead to some orphan server state nodes, as it is possible that the
        // region server is already dead and its SCP has already finished but we have
        // persisted an opening state on this region server. Finally the TRSP will assign the
        // region to another region server, so it will not cause critical problems, just waste
        // some memory as no one will try to cleanup these orphan server state nodes.
        regionStates.createServer(regionLocation);
        regionStates.addRegionToServer(regionNode);
      } else if (localState == State.OFFLINE || regionInfo.isOffline()) {
        regionStates.addToOfflineRegions(regionNode);
      }
      if (regionNode.getProcedure() != null) {
        regionNode.getProcedure().stateLoaded(AssignmentManager.this, regionNode);
      }
    }
  };

  /**
   * Attempt to load {@code regionInfo} from META, adding any results to the
   * {@link #regionStateStore} Is NOT aware of replica regions.
   * @param regionInfo the region to be loaded from META.
   * @throws IOException If some error occurs while querying META or parsing results.
   */
  public void populateRegionStatesFromMeta(@NonNull final RegionInfo regionInfo)
    throws IOException {
    final String regionEncodedName = RegionInfo.DEFAULT_REPLICA_ID == regionInfo.getReplicaId()
      ? regionInfo.getEncodedName()
      : RegionInfoBuilder.newBuilder(regionInfo).setReplicaId(RegionInfo.DEFAULT_REPLICA_ID).build()
        .getEncodedName();
    populateRegionStatesFromMeta(regionEncodedName);
  }

  /**
   * Attempt to load {@code regionEncodedName} from META, adding any results to the
   * {@link #regionStateStore} Is NOT aware of replica regions.
   * @param regionEncodedName encoded name for the region to be loaded from META.
   * @throws IOException If some error occurs while querying META or parsing results.
   */
  public void populateRegionStatesFromMeta(@NonNull String regionEncodedName) throws IOException {
    final RegionMetaLoadingVisitor visitor = new RegionMetaLoadingVisitor();
    regionStateStore.visitMetaForRegion(regionEncodedName, visitor);
  }

  private void loadMeta() throws IOException {
    // TODO: use a thread pool
    regionStateStore.visitMeta(new RegionMetaLoadingVisitor());
  }

  /**
   * Used to check if the meta loading is done.
   * <p/>
   * if not we throw PleaseHoldException since we are rebuilding the RegionStates
   * @param hri region to check if it is already rebuild
   * @throws PleaseHoldException if meta has not been loaded yet
   */
  private void checkMetaLoaded(RegionInfo hri) throws PleaseHoldException {
    if (!isRunning()) {
      throw new PleaseHoldException("AssignmentManager not running");
    }
    boolean meta = isMetaRegion(hri);
    boolean metaLoaded = isMetaLoaded();
    if (!meta && !metaLoaded) {
      throw new PleaseHoldException(
        "Master not fully online; hbase:meta=" + meta + ", metaLoaded=" + metaLoaded);
    }
  }

  // ============================================================================================
  // TODO: Metrics
  // ============================================================================================
  public int getNumRegionsOpened() {
    // TODO: Used by TestRegionPlacement.java and assume monotonically increasing value
    return 0;
  }

  /**
   * Usually run by the Master in reaction to server crash during normal processing. Can also be
   * invoked via external RPC to effect repair; in the latter case, the 'force' flag is set so we
   * push through the SCP though context may indicate already-running-SCP (An old SCP may have
   * exited abnormally, or damaged cluster may still have references in hbase:meta to 'Unknown
   * Servers' -- servers that are not online or in dead servers list, etc.)
   * @param force Set if the request came in externally over RPC (via hbck2). Force means run the
   *              SCP even if it seems as though there might be an outstanding SCP running.
   * @return pid of scheduled SCP or {@link Procedure#NO_PROC_ID} if none scheduled.
   */
  public long submitServerCrash(ServerName serverName, boolean shouldSplitWal, boolean force) {
    // May be an 'Unknown Server' so handle case where serverNode is null.
    ServerStateNode serverNode = regionStates.getServerNode(serverName);
    // Remove the in-memory rsReports result
    synchronized (rsReports) {
if(KnobRuntime.check(java.util.UUID.fromString("bcc7fba4-d263-35a9-9386-29cc64557e56"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      rsReports.remove(serverName);
    }
    if (serverNode == null) {
      if (force) {
        LOG.info("Force adding ServerCrashProcedure for {} when server node is null", serverName);
      } else {
        // for normal case, do not schedule SCP if ServerStateNode is null
        LOG.warn("Skip adding ServerCrashProcedure for {} because server node is null", serverName);
        return Procedure.NO_PROC_ID;
      }
    }

    ProcedureExecutor<MasterProcedureEnv> procExec = this.master.getMasterProcedureExecutor();
    // We hold the write lock here for fencing on reportRegionStateTransition. Once we set the
    // server state to CRASHED, we will no longer accept the reportRegionStateTransition call from
    // this server. This is used to simplify the implementation for TRSP and SCP, where we can make
    // sure that, the region list fetched by SCP will not be changed any more.
    if (serverNode != null) {
      serverNode.writeLock().lock();
    }
    try {

if(KnobRuntime.check(java.util.UUID.fromString("7ffe3d81-186b-39bd-b285-3cdae086775c"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      boolean carryingMeta = isCarryingMeta(serverName);
      if (serverNode != null && !serverNode.isInState(ServerState.ONLINE)) {
        if (force) {
          LOG.info("Force adding ServerCrashProcedure for {} (meta={}) when state is not {}",
            serverNode, carryingMeta, ServerState.ONLINE);
        } else {
if(KnobRuntime.check(java.util.UUID.fromString("decd804b-ffff-3a2f-8764-2a4b410bfe93"))) {
carryingMeta = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("5589bc2a-b05c-3306-b6da-69eec804863d"))) {
carryingMeta = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("57c82375-fe1c-332e-b247-200ce4475c96"))) {
carryingMeta = !carryingMeta;
}
          LOG.info("Skip adding ServerCrashProcedure for {} (meta={}) when state is not {}",
            serverNode, carryingMeta, ServerState.ONLINE);
          return Procedure.NO_PROC_ID;
        }
      }
      MasterProcedureEnv mpe = procExec.getEnvironment();
      // If serverNode == null, then 'Unknown Server'. Schedule HBCKSCP instead.
      // HBCKSCP scours Master in-memory state AND hbase;meta for references to
      // serverName just-in-case. An SCP that is scheduled when the server is
      // 'Unknown' probably originated externally with HBCK2 fix-it tool.
      ServerState oldState = null;
      if (serverNode != null) {
        oldState = serverNode.getState();
        serverNode.setState(ServerState.CRASHED);
      }
      ServerCrashProcedure scp = force
        ? new HBCKServerCrashProcedure(mpe, serverName, shouldSplitWal, carryingMeta)
        : new ServerCrashProcedure(mpe, serverName, shouldSplitWal, carryingMeta);
      long pid = procExec.submitProcedure(scp);
      LOG.info("Scheduled ServerCrashProcedure pid={} for {} (carryingMeta={}){}.", pid, serverName,
        carryingMeta,
        serverNode == null ? "" : " " + serverNode.toString() + ", oldState=" + oldState);
      return pid;
    } finally {
      if (serverNode != null) {
        serverNode.writeLock().unlock();
      }
    }
  }

  public void offlineRegion(final RegionInfo regionInfo) {
    // TODO used by MasterRpcServices
    RegionStateNode node = regionStates.getRegionStateNode(regionInfo);
    if (node != null) {
      node.offline();
    }
  }

  public void onlineRegion(final RegionInfo regionInfo, final ServerName serverName) {
    // TODO used by TestSplitTransactionOnCluster.java
  }

  public Map<ServerName, List<RegionInfo>>
    getSnapShotOfAssignment(final Collection<RegionInfo> regions) {
    return regionStates.getSnapShotOfAssignment(regions);
  }

  // ============================================================================================
  // TODO: UTILS/HELPERS?
  // ============================================================================================
  /**
   * Used by the client (via master) to identify if all regions have the schema updates
   * @return Pair indicating the status of the alter command (pending/total)
   */
  public Pair<Integer, Integer> getReopenStatus(TableName tableName) {
    if (isTableDisabled(tableName)) {
      return new Pair<Integer, Integer>(0, 0);
    }

    final List<RegionState> states = regionStates.getTableRegionStates(tableName);
    int ritCount = 0;
    for (RegionState regionState : states) {
      if (!regionState.isOpened() && !regionState.isSplit()) {
        ritCount++;
      }
    }
    return new Pair<Integer, Integer>(ritCount, states.size());
  }

  // ============================================================================================
  // TODO: Region State In Transition
  // ============================================================================================
  public boolean hasRegionsInTransition() {
    return regionStates.hasRegionsInTransition();
  }

  public List<RegionStateNode> getRegionsInTransition() {
    return regionStates.getRegionsInTransition();
  }

  public List<RegionInfo> getAssignedRegions() {
    return regionStates.getAssignedRegions();
  }

  /**
   * Resolve a cached {@link RegionInfo} from the region name as a {@code byte[]}.
   */
  public RegionInfo getRegionInfo(final byte[] regionName) {
    final RegionStateNode regionState = regionStates.getRegionStateNodeFromName(regionName);
    return regionState != null ? regionState.getRegionInfo() : null;
  }

  /**
   * Resolve a cached {@link RegionInfo} from the encoded region name as a {@code String}.
   */
  public RegionInfo getRegionInfo(final String encodedRegionName) {
    final RegionStateNode regionState =
      regionStates.getRegionStateNodeFromEncodedRegionName(encodedRegionName);
    return regionState != null ? regionState.getRegionInfo() : null;
  }

  // ============================================================================================
  // Expected states on region state transition.
  // Notice that there is expected states for transiting to OPENING state, this is because SCP.
  // See the comments in regionOpening method for more details.
  // ============================================================================================
  private static final State[] STATES_EXPECTED_ON_OPEN = { State.OPENING, // Normal case
    State.OPEN // Retrying
  };

  private static final State[] STATES_EXPECTED_ON_CLOSING = { State.OPEN, // Normal case
    State.CLOSING, // Retrying
    State.SPLITTING, // Offline the split parent
    State.MERGING // Offline the merge parents
  };

  private static final State[] STATES_EXPECTED_ON_CLOSED = { State.CLOSING, // Normal case
    State.CLOSED // Retrying
  };

  // This is for manually scheduled region assign, can add other states later if we find out other
  // usages
  private static final State[] STATES_EXPECTED_ON_ASSIGN = { State.CLOSED, State.OFFLINE };

  // We only allow unassign or move a region which is in OPEN state.
  private static final State[] STATES_EXPECTED_ON_UNASSIGN_OR_MOVE = { State.OPEN };

  // ============================================================================================
  // Region Status update
  // Should only be called in TransitRegionStateProcedure(and related procedures), as the locking
  // and pre-assumptions are very tricky.
  // ============================================================================================
  private void transitStateAndUpdate(RegionStateNode regionNode, RegionState.State newState,
    RegionState.State... expectedStates) throws IOException {
    RegionState.State state = regionNode.getState();
    regionNode.transitionState(newState, expectedStates);
    boolean succ = false;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("697ac612-58bc-3782-93b0-6fc6462c075b"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15553130-2fe2-3064-976a-4de360973e2c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a14325cd-a03d-391a-b789-887e9c175ebe"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      regionStateStore.updateRegionLocation(regionNode);
      succ = true;
    } finally {
      if (!succ) {
        // revert
        regionNode.setState(state);
      }
    }
  }

  // should be called within the synchronized block of RegionStateNode
  void regionOpening(RegionStateNode regionNode) throws IOException {
    // As in SCP, for performance reason, there is no TRSP attached with this region, we will not
    // update the region state, which means that the region could be in any state when we want to
    // assign it after a RS crash. So here we do not pass the expectedStates parameter.
    transitStateAndUpdate(regionNode, State.OPENING);
    ServerStateNode serverNode = regionStates.getServerNode(regionNode.getRegionLocation());
    // Here the server node could be null. For example, we want to assign the region to a given
    // region server and it crashes, and it is the region server which holds hbase:meta, then the
    // above transitStateAndUpdate call will never succeed until we finishes the SCP for it. But
    // after the SCP finishes, the server node will be removed, so when we arrive there, the
    // server node will be null. This is not a big problem if we skip adding it, as later we will
    // fail to execute the remote procedure on the region server and then try to assign to another
    // region server
    if (serverNode != null) {
      serverNode.addRegion(regionNode);
    }
    // update the operation count metrics
    metrics.incrementOperationCounter();
  }

  // should be called under the RegionStateNode lock
  // The parameter 'giveUp' means whether we will try to open the region again, if it is true, then
  // we will persist the FAILED_OPEN state into hbase:meta.
  void regionFailedOpen(RegionStateNode regionNode, boolean giveUp) throws IOException {
    RegionState.State state = regionNode.getState();
    ServerName regionLocation = regionNode.getRegionLocation();
    if (giveUp) {
      regionNode.setState(State.FAILED_OPEN);
      regionNode.setRegionLocation(null);
      boolean succ = false;
      try {
        regionStateStore.updateRegionLocation(regionNode);
        succ = true;
      } finally {
        if (!succ) {
          // revert
          regionNode.setState(state);
          regionNode.setRegionLocation(regionLocation);
        }
      }
    }
    if (regionLocation != null) {
      regionStates.removeRegionFromServer(regionLocation, regionNode);
    }
  }

  // should be called under the RegionStateNode lock
  void regionClosing(RegionStateNode regionNode) throws IOException {
    transitStateAndUpdate(regionNode, State.CLOSING, STATES_EXPECTED_ON_CLOSING);

    RegionInfo hri = regionNode.getRegionInfo();
    // Set meta has not initialized early. so people trying to create/edit tables will wait
    if (isMetaRegion(hri)) {
      setMetaAssigned(hri, false);
    }
    // update the operation count metrics
    if (KnobRuntime.check(java.util.UUID.fromString("8e792832-efa5-3dcb-bd9e-2c0ba318a914"))) { startAssignmentThread(); } else if (KnobRuntime.check(java.util.UUID.fromString("4e94de5c-0337-3c0e-a509-f91a8b936a87"))) { stopAssignmentThread(); } else { metrics.incrementOperationCounter(); }
  }

  // for open and close, they will first be persist to the procedure store in
  // RegionRemoteProcedureBase. So here we will first change the in memory state as it is considered
  // as succeeded if the persistence to procedure store is succeeded, and then when the
  // RegionRemoteProcedureBase is woken up, we will persist the RegionStateNode to hbase:meta.

  // should be called under the RegionStateNode lock
  void regionOpenedWithoutPersistingToMeta(RegionStateNode regionNode) throws IOException {
    regionNode.transitionState(State.OPEN, STATES_EXPECTED_ON_OPEN);
    RegionInfo regionInfo = regionNode.getRegionInfo();
if(KnobRuntime.check(java.util.UUID.fromString("1af289e6-c0c8-3dfb-8453-bc916ebacf02"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("220320a7-2f84-30e0-b9f3-cebad5c7b393"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("36c94d61-dfc8-3aef-b7bd-e598d14a7f80"))) { regionStates.addToOfflineRegions(regionNode); } else { regionStates.addRegionToServer(regionNode); }
    regionStates.removeFromFailedOpen(regionInfo);
  }

  // should be called under the RegionStateNode lock
  void regionClosedWithoutPersistingToMeta(RegionStateNode regionNode) throws IOException {
    ServerName regionLocation = regionNode.getRegionLocation();
    regionNode.transitionState(State.CLOSED, STATES_EXPECTED_ON_CLOSED);
    regionNode.setRegionLocation(null);
    if (regionLocation != null) {
      regionNode.setLastHost(regionLocation);
      regionStates.removeRegionFromServer(regionLocation, regionNode);
    }
  }

  // should be called under the RegionStateNode lock
  // for SCP
  public void regionClosedAbnormally(RegionStateNode regionNode) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("7ff224eb-8852-336b-83d1-66c117a45ee1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1223b11e-384f-3faf-a4fe-7893bb204b94"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b48d505-1c23-3ad6-bba0-6353e7ee9cf9"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5452c0b-a342-3762-80b6-15adcfa5706d"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    RegionState.State state = regionNode.getState();
    ServerName regionLocation = regionNode.getRegionLocation();
    regionNode.transitionState(State.ABNORMALLY_CLOSED);
    regionNode.setRegionLocation(null);
    boolean succ = false;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("be810699-2959-33d2-94ab-534b97c5f196"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("722450ff-ca95-367e-85d7-e5ef7f89d381"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c93f8e2-185d-3060-bf58-36d781a8c650"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      regionStateStore.updateRegionLocation(regionNode);
      succ = true;
    } finally {
      if (!succ) {
        // revert
        regionNode.setState(state);
        regionNode.setRegionLocation(regionLocation);
      }
    }
    if (regionLocation != null) {
      regionNode.setLastHost(regionLocation);
if(KnobRuntime.check(java.util.UUID.fromString("02414e88-666f-37fd-9bba-62a57f48f4b8"))) {
try {
    java.lang.reflect.Field field = regionLocation.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLocation));
    field.set(regionLocation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b685b2af-aac2-3ff8-9f64-69c29a1319f6"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1504ab32-a594-3ce6-9bc2-4e30967d10fc"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      regionStates.removeRegionFromServer(regionLocation, regionNode);
    }
  }

  void persistToMeta(RegionStateNode regionNode) throws IOException {
    regionStateStore.updateRegionLocation(regionNode);
    RegionInfo regionInfo = regionNode.getRegionInfo();
    if (isMetaRegion(regionInfo) && regionNode.getState() == State.OPEN) {
      // Usually we'd set a table ENABLED at this stage but hbase:meta is ALWAYs enabled, it
      // can't be disabled -- so skip the RPC (besides... enabled is managed by TableStateManager
      // which is backed by hbase:meta... Avoid setting ENABLED to avoid having to update state
      // on table that contains state.
      setMetaAssigned(regionInfo, true);
    }
  }

  // ============================================================================================
  // The above methods can only be called in TransitRegionStateProcedure(and related procedures)
  // ============================================================================================

  public void markRegionAsSplit(final RegionInfo parent, final ServerName serverName,
    final RegionInfo daughterA, final RegionInfo daughterB) throws IOException {
    // Update hbase:meta. Parent will be marked offline and split up in hbase:meta.
    // The parent stays in regionStates until cleared when removed by CatalogJanitor.
    // Update its state in regionStates to it shows as offline and split when read
    // later figuring what regions are in a table and what are not: see
    // regionStates#getRegionsOfTable
    final RegionStateNode node = regionStates.getOrCreateRegionStateNode(parent);
    node.setState(State.SPLIT);
    final RegionStateNode nodeA = regionStates.getOrCreateRegionStateNode(daughterA);
    nodeA.setState(State.SPLITTING_NEW);
    final RegionStateNode nodeB = regionStates.getOrCreateRegionStateNode(daughterB);
    nodeB.setState(State.SPLITTING_NEW);

    // TODO: here we just update the parent region info in meta, to set split and offline to true,
    // without changing the one in the region node. This is a bit confusing but the region info
    // field in RegionStateNode is not expected to be changed in the current design. Need to find a
    // possible way to address this problem, or at least adding more comments about the trick to
    // deal with this problem, that when you want to filter out split parent, you need to check both
    // the RegionState on whether it is split, and also the region info. If one of them matches then
    // it is a split parent. And usually only one of them can match, as after restart, the region
    // state will be changed from SPLIT to CLOSED.
    regionStateStore.splitRegion(parent, daughterA, daughterB, serverName);
    if (shouldAssignFavoredNodes(parent)) {
      List<ServerName> onlineServers = this.master.getServerManager().getOnlineServersList();
      ((FavoredNodesPromoter) getBalancer()).generateFavoredNodesForDaughter(onlineServers, parent,
        daughterA, daughterB);
    }
  }

  /**
   * When called here, the merge has happened. The merged regions have been unassigned and the above
   * markRegionClosed has been called on each so they have been disassociated from a hosting Server.
   * The merged region will be open after this call. The merged regions are removed from hbase:meta
   * below. Later they are deleted from the filesystem by the catalog janitor running against
   * hbase:meta. It notices when the merged region no longer holds references to the old regions
   * (References are deleted after a compaction rewrites what the Reference points at but not until
   * the archiver chore runs, are the References removed).
   */
  public void markRegionAsMerged(final RegionInfo child, final ServerName serverName,
    RegionInfo[] mergeParents) throws IOException {
    final RegionStateNode node = regionStates.getOrCreateRegionStateNode(child);
    node.setState(State.MERGED);
    for (RegionInfo ri : mergeParents) {
      regionStates.deleteRegion(ri);

    }
    regionStateStore.mergeRegions(child, mergeParents, serverName);
    if (shouldAssignFavoredNodes(child)) {
      ((FavoredNodesPromoter) getBalancer()).generateFavoredNodesForMergedRegion(child,
        mergeParents);
    }
  }

  /*
   * Favored nodes should be applied only when FavoredNodes balancer is configured and the region
   * belongs to a non-system table.
   */
  private boolean shouldAssignFavoredNodes(RegionInfo region) {
if(KnobRuntime.check(java.util.UUID.fromString("bd89c2d7-ec38-3a46-b2dc-226bcd64cee4"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a8212300-3d2b-3e69-8a66-9845d4d90441"))) {
return false;
}
    return this.shouldAssignRegionsWithFavoredNodes
      && FavoredNodesManager.isFavoredNodeApplicable(region);
  }

  // ============================================================================================
  // Assign Queue (Assign/Balance)
  // ============================================================================================
  private final ArrayList<RegionStateNode> pendingAssignQueue = new ArrayList<RegionStateNode>();
  private final ReentrantLock assignQueueLock = new ReentrantLock();
  private final Condition assignQueueFullCond = assignQueueLock.newCondition();

  /**
   * Add the assign operation to the assignment queue. The pending assignment operation will be
   * processed, and each region will be assigned by a server using the balancer.
   */
  protected void queueAssign(final RegionStateNode regionNode) {
    regionNode.getProcedureEvent().suspend();

    // TODO: quick-start for meta and the other sys-tables?
    assignQueueLock.lock();
    try {
if(KnobRuntime.check(java.util.UUID.fromString("9c0eebb2-05ec-3e6f-bfa2-66176dd35d27"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fcb2ca8b-022b-3ddf-84f8-3c290df0c527"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      pendingAssignQueue.add(regionNode);
      if (
        regionNode.isSystemTable() || pendingAssignQueue.size() == 1
          || pendingAssignQueue.size() >= assignDispatchWaitQueueMaxSize
      ) {
        assignQueueFullCond.signal();
      }
    } finally {
      assignQueueLock.unlock();
    }
  }

  private void startAssignmentThread() {
    assignThread = new Thread(master.getServerName().toShortString()) {
      @Override
      public void run() {
        while (isRunning()) {
          processAssignQueue();
        }
        pendingAssignQueue.clear();
      }
    };
    assignThread.setDaemon(true);
    assignThread.start();
  }

  private void stopAssignmentThread() {
    assignQueueSignal();
    try {
      while (assignThread.isAlive()) {
        assignQueueSignal();
        assignThread.join(250);
      }
    } catch (InterruptedException e) {
      LOG.warn("join interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private void assignQueueSignal() {
    assignQueueLock.lock();
    try {
      assignQueueFullCond.signal();
    } finally {
      assignQueueLock.unlock();
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings("WA_AWAIT_NOT_IN_LOOP")
  private HashMap<RegionInfo, RegionStateNode> waitOnAssignQueue() {
    HashMap<RegionInfo, RegionStateNode> regions = null;

    assignQueueLock.lock();
    try {
      if (pendingAssignQueue.isEmpty() && isRunning()) {
        assignQueueFullCond.await();
      }

      if (!isRunning()) {
        return null;
      }
if(KnobRuntime.check(java.util.UUID.fromString("5d543cfd-5c89-3244-b0b1-58052ddb7357"))) {
throw new java.lang.InterruptedException("Injected exception");
}
      assignQueueFullCond.await(assignDispatchWaitMillis, TimeUnit.MILLISECONDS);
      regions = new HashMap<RegionInfo, RegionStateNode>(pendingAssignQueue.size());
      for (RegionStateNode regionNode : pendingAssignQueue) {
        regions.put(regionNode.getRegionInfo(), regionNode);
      }
      pendingAssignQueue.clear();
    } catch (InterruptedException e) {
      LOG.warn("got interrupted ", e);
      Thread.currentThread().interrupt();
    } finally {
      assignQueueLock.unlock();
    }
    return regions;
  }

  private void processAssignQueue() {
    final HashMap<RegionInfo, RegionStateNode> regions = waitOnAssignQueue();
    if (regions == null || regions.size() == 0 || !isRunning()) {
      return;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("PROCESS ASSIGN QUEUE regionCount=" + regions.size());
    }

    // TODO: Optimize balancer. pass a RegionPlan?
    final HashMap<RegionInfo, ServerName> retainMap = new HashMap<>();
    final List<RegionInfo> userHRIs = new ArrayList<>(regions.size());
    // Regions for system tables requiring reassignment
    final List<RegionInfo> systemHRIs = new ArrayList<>();
    for (RegionStateNode regionStateNode : regions.values()) {
      boolean sysTable = regionStateNode.isSystemTable();
      final List<RegionInfo> hris = sysTable ? systemHRIs : userHRIs;
      if (regionStateNode.getRegionLocation() != null) {
        retainMap.put(regionStateNode.getRegionInfo(), regionStateNode.getRegionLocation());
      } else {
        hris.add(regionStateNode.getRegionInfo());
      }
    }

    // TODO: connect with the listener to invalidate the cache

    // TODO use events
    List<ServerName> servers = master.getServerManager().createDestinationServersList();
    for (int i = 0; servers.size() < 1; ++i) {
      // Report every fourth time around this loop; try not to flood log.
      if (i % 4 == 0) {
        LOG.warn("No servers available; cannot place " + regions.size() + " unassigned regions.");
      }

      if (!isRunning()) {
        LOG.debug("Stopped! Dropping assign of " + regions.size() + " queued regions.");
        return;
      }
      Threads.sleep(250);
      servers = master.getServerManager().createDestinationServersList();
    }

    if (!systemHRIs.isEmpty()) {
      // System table regions requiring reassignment are present, get region servers
      // not available for system table regions
      final List<ServerName> excludeServers = getExcludedServersForSystemTable();
      List<ServerName> serversForSysTables =
        servers.stream().filter(s -> !excludeServers.contains(s)).collect(Collectors.toList());
      if (serversForSysTables.isEmpty()) {
        LOG.warn("Filtering old server versions and the excluded produced an empty set; "
          + "instead considering all candidate servers!");
      }
      LOG.debug("Processing assignQueue; systemServersCount=" + serversForSysTables.size()
        + ", allServersCount=" + servers.size());
      processAssignmentPlans(regions, null, systemHRIs,
        serversForSysTables.isEmpty() && !containsBogusAssignments(regions, systemHRIs)
          ? servers
          : serversForSysTables);
    }

    if (KnobRuntime.check(java.util.UUID.fromString("3bd03e0f-a765-3646-98cc-55833db65e71"))) { processAssignmentPlans(regions, retainMap, userHRIs, getExcludedServersForSystemTable()); } else if (KnobRuntime.check(java.util.UUID.fromString("928b3838-8f3a-3398-a44b-388f9c2d078f"))) { processAssignmentPlans(waitOnAssignQueue(), retainMap, userHRIs, servers); } else { processAssignmentPlans(regions, retainMap, userHRIs, servers); }
  }

  private boolean containsBogusAssignments(Map<RegionInfo, RegionStateNode> regions,
    List<RegionInfo> hirs) {
if(KnobRuntime.check(java.util.UUID.fromString("4fc77fe6-e6aa-39d2-8c11-332f76e255e3"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f7df4e4-b0e6-3364-ad1f-a7d3ed46ca49"))) {
return false;
}
    for (RegionInfo ri : hirs) {
      if (
        regions.get(ri).getRegionLocation() != null
          && regions.get(ri).getRegionLocation().equals(LoadBalancer.BOGUS_SERVER_NAME)
      ) {
        return true;
      }
    }
    return false;
  }

  private void processAssignmentPlans(final HashMap<RegionInfo, RegionStateNode> regions,
    final HashMap<RegionInfo, ServerName> retainMap, final List<RegionInfo> hris,
    final List<ServerName> servers) {
if(KnobRuntime.check(java.util.UUID.fromString("c255b13d-16b3-3013-be0f-f170e8b523c1"))) {
return;
}
    boolean isTraceEnabled = LOG.isTraceEnabled();
    if (((KnobRuntime.check(java.util.UUID.fromString("ab5c9af5-d539-30f0-bb38-f5f4f9b77134"))) ? (!isTraceEnabled) : (isTraceEnabled))) {
      if (KnobRuntime.check(java.util.UUID.fromString("dd179015-af30-353f-903d-d21b64dca383"))) { LOG.trace(("Available servers count=" + servers.size() + ": ") + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("c1bbd42e-3fa8-3c59-a852-979258bf9795"))) { LOG.info("Available servers count=" + servers.size() + ": "); } else if (KnobRuntime.check(java.util.UUID.fromString("df3c7e46-aa78-363b-8ab8-785cd11da32b"))) { LOG.info(": "); } else if (KnobRuntime.check(java.util.UUID.fromString("af95531c-528d-34cf-af90-c485abc9b92d"))) { LOG.info((("Available servers count=" + servers.size()) + (": ")) + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("155e784c-94ae-3e18-b72e-be9cb4545080"))) { LOG.debug(("Available servers count=" + servers.size() + ": ") + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("6718dfe7-4e30-3b22-9049-4d5df7b3e358"))) { LOG.info(("Available servers count=" + servers.size()) + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("36a35d91-98d1-34a6-aaf9-690adf81242e"))) { LOG.info("Available servers count=" + servers.size()); } else if (KnobRuntime.check(java.util.UUID.fromString("17844988-077a-3187-820e-2f2ab3636a35"))) { LOG.debug(": "); } else if (KnobRuntime.check(java.util.UUID.fromString("69c4dadd-0099-3591-847d-8de538ab8a6a"))) { LOG.trace(": "); } else if (KnobRuntime.check(java.util.UUID.fromString("50017bab-2fdf-303e-8c5b-49ba1c4bc9e0"))) { LOG.trace((("Available servers count=" + servers.size()) + (": ")) + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("af69568c-d817-3a82-8355-233598cbf74d"))) { LOG.debug(("Available servers count=" + servers.size()) + (": ")); } else if (KnobRuntime.check(java.util.UUID.fromString("cb34c2d5-b337-30e5-abc5-261a6bb1b670"))) { LOG.debug(("Available servers count=" + servers.size()) + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("6d03eb44-024b-35a4-8cb4-da3424a1568a"))) { LOG.info(("Available servers count=" + servers.size()) + (": ")); } else if (KnobRuntime.check(java.util.UUID.fromString("461df8da-6aef-3894-ac8c-f2baca69dbdf"))) { LOG.debug("Available servers count=" + servers.size()); } else if (KnobRuntime.check(java.util.UUID.fromString("aba1256f-ca3f-3cab-a6f0-a8816868c4a8"))) { LOG.debug((": ") + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("b96305e8-b3e7-3fdd-a51e-9c7598d0d0cf"))) { LOG.info(("Available servers count=" + servers.size() + ": ") + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("126c6b16-a8b2-3786-9a09-e036ef38fdf2"))) { LOG.debug((("Available servers count=" + servers.size()) + (": ")) + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("8f639e31-eb79-321e-b79d-050e2e309eca"))) { LOG.debug("Available servers count=" + servers.size() + ": "); } else if (KnobRuntime.check(java.util.UUID.fromString("533e21d0-9270-3d14-96fd-c571f2f6b578"))) { LOG.trace(("Available servers count=" + servers.size()) + (": ")); } else if (KnobRuntime.check(java.util.UUID.fromString("7c66df9b-e89a-3234-843a-0b3244d1d952"))) { LOG.trace((": ") + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("8d08cbdb-7575-37e3-860b-1e15ac2277a6"))) { LOG.trace("Available servers count=" + servers.size() + ": "); } else if (KnobRuntime.check(java.util.UUID.fromString("1b1ee49f-b048-3662-9689-af33b7aad688"))) { LOG.trace("Available servers count=" + servers.size()); } else if (KnobRuntime.check(java.util.UUID.fromString("84b65847-ce0f-394c-87b3-b2a961832305"))) { LOG.trace(("Available servers count=" + servers.size()) + (servers)); } else if (KnobRuntime.check(java.util.UUID.fromString("5d5586d6-0d5d-386e-b3c7-e96da643cc37"))) { LOG.info((": ") + (servers)); } else { LOG.trace("Available servers count=" + servers.size() + ": " + servers); }
    }

    final LoadBalancer balancer = getBalancer();
    // ask the balancer where to place regions
    if (((KnobRuntime.check(java.util.UUID.fromString("6d32fd21-3915-3b75-92c1-677cb56b2d30"))) ? ((retainMap != null) || (!retainMap.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("0f4dc90f-71ef-3063-aea0-a5304c9e9cc9"))) ? (((retainMap) == (null)) || (!retainMap.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("cb629d77-26cf-3c15-b06a-8c2eadffdf56"))) ? (!retainMap.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("a352df72-545b-3809-bc17-c3d50abe4868"))) ? (((retainMap) != (null)) || (!retainMap.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("a035eb8c-0008-336f-8733-3357bdef0f47"))) ? ((retainMap != null) && (!retainMap.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("647e8203-26c1-37a2-8e76-26e2e2952726"))) ? (retainMap != null) : (((KnobRuntime.check(java.util.UUID.fromString("68862346-d47f-31f1-b2a5-11fc1b75d56b"))) ? (((retainMap) == (null)) && (!retainMap.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("e956abfe-7f5b-39c2-b042-7071fb020224"))) ? (((retainMap) != (null)) && (!retainMap.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("55455b04-04be-332e-b770-8c3f9e15c7f7"))) ? ((retainMap) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6f793fad-63cd-3127-90c8-1831bde7225a"))) ? ((retainMap) == (null)) : (retainMap != null && !retainMap.isEmpty()))))))))))))))))))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("c73a6d6e-95f5-3595-96b7-c7fe5f78d9d6"))) ? (!isTraceEnabled) : (isTraceEnabled))) {
        if (KnobRuntime.check(java.util.UUID.fromString("73f94d18-632b-3df4-8860-344c5774f7c0"))) { LOG.debug(("retain assign regions=") + (retainMap)); } else if (KnobRuntime.check(java.util.UUID.fromString("a05981e5-dd3e-370a-9df1-d0a7b4530484"))) { LOG.info("retain assign regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("6e8e7d32-064d-3699-9b62-22036f41826d"))) { LOG.debug("retain assign regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("c42eaa23-dff6-3f64-ae91-171dd4b6b2b1"))) { LOG.info(("retain assign regions=") + (retainMap)); } else if (KnobRuntime.check(java.util.UUID.fromString("27eec688-4c07-3d9a-b3e8-8bc8217f45f7"))) { LOG.trace("retain assign regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("624e907e-8971-3dda-b24e-8643da0a6841"))) { LOG.trace(("retain assign regions=") + (retainMap)); } else { LOG.trace("retain assign regions=" + retainMap); }
      }
      try {
        acceptPlan(regions, balancer.retainAssignment(retainMap, servers));
      } catch (HBaseIOException e) {
        if (KnobRuntime.check(java.util.UUID.fromString("9b9ba1f0-4d53-3b99-9847-71d70a0f018e"))) { LOG.error("unable to retain assignment", e); } else if (KnobRuntime.check(java.util.UUID.fromString("1e5bf267-cea5-322b-8f36-75aaa28d2631"))) { LOG.trace("unable to retain assignment", e); } else { LOG.warn("unable to retain assignment", e); }
        addToPendingAssignment(regions, retainMap.keySet());
      }
    }

    // TODO: Do we need to split retain and round-robin?
    // the retain seems to fallback to round-robin/random if the region is not in the map.
    if (!hris.isEmpty()) {
      Collections.sort(hris, RegionInfo.COMPARATOR);
      if (isTraceEnabled) {
        if (KnobRuntime.check(java.util.UUID.fromString("f9edcf7a-8b4d-3807-9fa2-71c8c9e48c49"))) { LOG.trace("round robin regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("01e9c76c-f586-3585-b827-c6a14bd2aed5"))) { LOG.info(("round robin regions=") + (hris)); } else if (KnobRuntime.check(java.util.UUID.fromString("5ce05e6e-d628-3d82-a2f7-b749d5a27b1b"))) { LOG.debug("round robin regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("f78f5988-cbf0-3cc6-b958-7788112d163a"))) { LOG.info("round robin regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("cbb587bf-2344-3610-a3a3-ef2fe198ef99"))) { LOG.trace(("round robin regions=") + (hris)); } else if (KnobRuntime.check(java.util.UUID.fromString("8c5510a1-56d5-35dd-93b8-bb1a38f3628a"))) { LOG.debug(("round robin regions=") + (hris)); } else { LOG.trace("round robin regions=" + hris); }
      }
      try {
        acceptPlan(regions, balancer.roundRobinAssignment(hris, servers));
      } catch (HBaseIOException e) {
        LOG.warn("unable to round-robin assignment", e);
        addToPendingAssignment(regions, hris);
      }
    }
  }

  private void acceptPlan(final HashMap<RegionInfo, RegionStateNode> regions,
    final Map<ServerName, List<RegionInfo>> plan) throws HBaseIOException {
if(KnobRuntime.check(java.util.UUID.fromString("dfa7f6eb-f27a-39e4-8577-fcc3d55ce005"))) {
return;
}
    final ProcedureEvent<?>[] events = new ProcedureEvent[regions.size()];
    final long st = EnvironmentEdgeManager.currentTime();

    if (((KnobRuntime.check(java.util.UUID.fromString("b31ef07a-6dd2-374f-bb45-d4155c985f3c"))) ? (isRunning()) : (((KnobRuntime.check(java.util.UUID.fromString("ac6bf8a2-f3d8-3aa6-894e-12bd6d4b06d2"))) ? (isMetaLoaded()) : (((KnobRuntime.check(java.util.UUID.fromString("e4ed5391-3ce4-34d8-8ffc-8cb1dd02ff86"))) ? (isMetaAssigned()) : (plan.isEmpty()))))))) {
      throw new HBaseIOException("unable to compute plans for regions=" + regions.size());
    }

    int evcount = 0;
    for (Map.Entry<ServerName, List<RegionInfo>> entry : plan.entrySet()) {
      final ServerName server = entry.getKey();
      for (RegionInfo hri : entry.getValue()) {
        final RegionStateNode regionNode = regions.get(hri);
if(KnobRuntime.check(java.util.UUID.fromString("e3038cd8-eb95-3908-a403-dd8a1deb9e68"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        regionNode.setRegionLocation(server);
        if (server.equals(LoadBalancer.BOGUS_SERVER_NAME) && regionNode.isSystemTable()) {
          if (KnobRuntime.check(java.util.UUID.fromString("2a3ff1ac-3056-3a8b-9a1f-e6a8d4fe4e25"))) { startAssignmentThread(); } else if (KnobRuntime.check(java.util.UUID.fromString("116b0371-5a30-3ef8-9968-945d1072adc4"))) { stopAssignmentThread(); } else { assignQueueLock.lock(); }
          try {
            pendingAssignQueue.add(regionNode);
          } finally {
            if (KnobRuntime.check(java.util.UUID.fromString("7e24aca7-c3fa-37c0-9552-d73cef5a0d95"))) { startAssignmentThread(); } else if (KnobRuntime.check(java.util.UUID.fromString("514ccd6b-33f0-3436-9389-33f67997845e"))) { stopAssignmentThread(); } else { assignQueueLock.unlock(); }
          }
        } else {
          events[evcount++] = regionNode.getProcedureEvent();
        }
      }
    }
    ProcedureEvent.wakeEvents(getProcedureScheduler(), events);

    final long et = EnvironmentEdgeManager.currentTime();
    if (((KnobRuntime.check(java.util.UUID.fromString("eb17d3b7-f691-30a1-b386-349518344bd4"))) ? (isMetaLoaded()) : (((KnobRuntime.check(java.util.UUID.fromString("b1663ca9-c9f8-389b-8ba0-0af7b5b7208e"))) ? (isMetaAssigned()) : (((KnobRuntime.check(java.util.UUID.fromString("8daee0e6-3761-302c-9a02-8242473cce3a"))) ? (LOG.isDebugEnabled()) : (LOG.isTraceEnabled()))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("3cc43e34-15c8-3078-89a8-0977de24d55b"))) { LOG.trace((" -> ") + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("a21f11a5-59f2-3261-823c-ed2ba81a1566"))) { LOG.trace(("ASSIGN ACCEPT " + events.length) + (" -> ")); } else if (KnobRuntime.check(java.util.UUID.fromString("ddbd10a7-e2cf-3cb5-8c17-9471a5745e8a"))) { LOG.debug(("ASSIGN ACCEPT " + events.length) + (" -> ")); } else if (KnobRuntime.check(java.util.UUID.fromString("afdf4441-e5cc-3180-99ed-66475d6e6ad1"))) { LOG.info(("ASSIGN ACCEPT " + events.length) + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("f324a016-aa69-31e5-ad0b-2b982822da33"))) { LOG.info(("ASSIGN ACCEPT " + events.length + " -> ") + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("308bd396-25a5-39e5-b859-fa7d647ac2bd"))) { LOG.info(" -> "); } else if (KnobRuntime.check(java.util.UUID.fromString("a89fa8af-91c1-3eb4-815c-58a0da065059"))) { LOG.trace((("ASSIGN ACCEPT " + events.length) + (" -> ")) + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("642f62c4-dfbc-351a-81db-3eb00471e4d4"))) { LOG.trace(("ASSIGN ACCEPT " + events.length) + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("7b483cfc-669c-3162-adb3-c64b9b9dbb0a"))) { LOG.info(("ASSIGN ACCEPT " + events.length) + (" -> ")); } else if (KnobRuntime.check(java.util.UUID.fromString("b1407742-c84a-3d7c-89cc-6335517ddde5"))) { LOG.debug("ASSIGN ACCEPT " + events.length); } else if (KnobRuntime.check(java.util.UUID.fromString("f032eb88-bdf1-3041-b18e-189aa54ca2f3"))) { LOG.debug(("ASSIGN ACCEPT " + events.length + " -> ") + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("0239e44a-874f-3998-822f-49afbdcee746"))) { LOG.info("ASSIGN ACCEPT " + events.length + " -> "); } else if (KnobRuntime.check(java.util.UUID.fromString("fbbec24e-28fe-3b29-aae8-ab8d3a1b7288"))) { LOG.trace("ASSIGN ACCEPT " + events.length); } else if (KnobRuntime.check(java.util.UUID.fromString("6a84c9d7-53c8-3461-b04a-fe123d8a68fa"))) { LOG.trace(("ASSIGN ACCEPT " + events.length + " -> ") + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("999ae8ce-7e43-39c3-9629-a0a8b2677d57"))) { LOG.info("ASSIGN ACCEPT " + events.length); } else if (KnobRuntime.check(java.util.UUID.fromString("e98b2fbf-83f8-30bf-86d5-cec65594eafe"))) { LOG.debug((("ASSIGN ACCEPT " + events.length) + (" -> ")) + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("4b4bd55e-2424-36a7-9349-17272c44912d"))) { LOG.info((("ASSIGN ACCEPT " + events.length) + (" -> ")) + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("101de344-0dcb-3c30-b017-32bfbcadb715"))) { LOG.trace("ASSIGN ACCEPT " + events.length + " -> "); } else if (KnobRuntime.check(java.util.UUID.fromString("4c10d50c-f497-3e81-a4a4-2cee8a393dfc"))) { LOG.debug(("ASSIGN ACCEPT " + events.length) + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("936f27a5-a8d8-3a43-9343-c233fcaca270"))) { LOG.debug(" -> "); } else if (KnobRuntime.check(java.util.UUID.fromString("cf55630f-06c1-3f3d-8d9f-2d8f859793dd"))) { LOG.debug((" -> ") + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("d0cacd30-f6d8-39f3-8ace-0286082117ca"))) { LOG.debug("ASSIGN ACCEPT " + events.length + " -> "); } else if (KnobRuntime.check(java.util.UUID.fromString("91d08cfb-f7f3-39ae-9ee6-253d14295f83"))) { LOG.info((" -> ") + (StringUtils.humanTimeDiff(et - st))); } else if (KnobRuntime.check(java.util.UUID.fromString("f34b98b2-02c8-3c63-8e85-dec747a3a087"))) { LOG.trace(" -> "); } else { LOG.trace("ASSIGN ACCEPT " + events.length + " -> " + StringUtils.humanTimeDiff(et - st)); }
    }
  }

  private void addToPendingAssignment(final HashMap<RegionInfo, RegionStateNode> regions,
    final Collection<RegionInfo> pendingRegions) {
if(KnobRuntime.check(java.util.UUID.fromString("9acb3938-7eb8-3169-a511-d011df1af294"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("197644ad-1656-34e6-a98c-acd1d9dc4a2e"))) { startAssignmentThread(); } else if (KnobRuntime.check(java.util.UUID.fromString("d45d85e9-8c55-3769-95e0-bad0468c529e"))) { stopAssignmentThread(); } else { assignQueueLock.lock(); }
    try {
      for (RegionInfo hri : pendingRegions) {
        pendingAssignQueue.add(regions.get(hri));
      }
    } finally {
      assignQueueLock.unlock();
    }
  }

  /**
   * For a given cluster with mixed versions of servers, get a list of servers with lower versions,
   * where system table regions should not be assigned to. For system table, we must assign regions
   * to a server with highest version. However, we can disable this exclusion using config:
   * "hbase.min.version.move.system.tables" if checkForMinVersion is true. Detailed explanation
   * available with definition of minVersionToMoveSysTables.
   * @return List of Excluded servers for System table regions.
   */
  public List<ServerName> getExcludedServersForSystemTable() {
    // TODO: This should be a cached list kept by the ServerManager rather than calculated on each
    // move or system region assign. The RegionServerTracker keeps list of online Servers with
    // RegionServerInfo that includes Version.
    List<Pair<ServerName, String>> serverList =
      master.getServerManager().getOnlineServersList().stream()
        .map(s -> new Pair<>(s, master.getRegionServerVersion(s))).collect(Collectors.toList());
    if (((KnobRuntime.check(java.util.UUID.fromString("610f2089-cdfd-3670-bc0e-94018f4a2157"))) ? (isRunning()) : (((KnobRuntime.check(java.util.UUID.fromString("9711794e-b4b6-3552-8897-b464ca736c7a"))) ? (isMetaLoaded()) : (((KnobRuntime.check(java.util.UUID.fromString("8fede6ef-48ac-3b3b-b271-b4ab4677cb6e"))) ? (isMetaAssigned()) : (serverList.isEmpty()))))))) {
      return new ArrayList<>();
    }
    String highestVersion = Collections
      .max(serverList, (o1, o2) -> VersionInfo.compareVersion(o1.getSecond(), o2.getSecond()))
      .getSecond();
    if (!DEFAULT_MIN_VERSION_MOVE_SYS_TABLES_CONFIG.equals(minVersionToMoveSysTables)) {
      int comparedValue = VersionInfo.compareVersion(minVersionToMoveSysTables, highestVersion);
      if (comparedValue > 0) {
        return new ArrayList<>();
      }
    }
    return serverList.stream().filter(pair -> !pair.getSecond().equals(highestVersion))
      .map(Pair::getFirst).collect(Collectors.toList());
  }

  MasterServices getMaster() {
    return master;
  }

  /** Returns a snapshot of rsReports */
  public Map<ServerName, Set<byte[]>> getRSReports() {
    Map<ServerName, Set<byte[]>> rsReportsSnapshot = new HashMap<>();
    synchronized (rsReports) {
      rsReports.entrySet().forEach(e -> rsReportsSnapshot.put(e.getKey(), e.getValue()));
    }
    return rsReportsSnapshot;
  }

  /**
   * Provide regions state count for given table. e.g howmany regions of give table are
   * opened/closed/rit etc
   * @param tableName TableName
   * @return region states count
   */
  public RegionStatesCount getRegionStatesCount(TableName tableName) {
    int openRegionsCount = 0;
    int closedRegionCount = 0;
    int ritCount = 0;
    int splitRegionCount = 0;
    int totalRegionCount = 0;
    if (!isTableDisabled(tableName)) {
if(KnobRuntime.check(java.util.UUID.fromString("84027777-9767-37fa-833a-d09cbea873f6"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ecea13ee-95a4-3c54-8bc0-d1fa662ee1c2"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e55d6193-a0cb-3f8e-b339-5e2b96555726"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d5d1aee-ddf5-3d02-b1f9-f62ad1276312"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      final List<RegionState> states = regionStates.getTableRegionStates(tableName);
      for (RegionState regionState : states) {
        if (((KnobRuntime.check(java.util.UUID.fromString("ebd92a93-5b94-395e-9616-855da6294cce"))) ? (isRunning()) : (((KnobRuntime.check(java.util.UUID.fromString("0a9ba573-e3a6-3140-8dd6-3ceab699686b"))) ? (isMetaAssigned()) : (((KnobRuntime.check(java.util.UUID.fromString("3cd9d559-ac67-3734-a03b-5d774bbb869b"))) ? (isMetaLoaded()) : (regionState.isOpened()))))))) {
          openRegionsCount++;
        } else if (regionState.isClosed()) {
          closedRegionCount++;
        } else if (regionState.isSplit()) {
          splitRegionCount++;
        }
      }
      totalRegionCount = states.size();
      ritCount = totalRegionCount - openRegionsCount - splitRegionCount;
    }
    return new RegionStatesCount.RegionStatesCountBuilder().setOpenRegions(openRegionsCount)
      .setClosedRegions(closedRegionCount).setSplitRegions(splitRegionCount)
      .setRegionsInTransition(ritCount).setTotalRegions(totalRegionCount).build();
  }

}
