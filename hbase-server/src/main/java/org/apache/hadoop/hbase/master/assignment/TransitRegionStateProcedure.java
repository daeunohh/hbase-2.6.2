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

import static org.apache.hadoop.hbase.io.hfile.CacheConfig.DEFAULT_EVICT_ON_CLOSE;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.EVICT_BLOCKS_ON_CLOSE_KEY;
import static org.apache.hadoop.hbase.master.LoadBalancer.BOGUS_SERVER_NAME;
import static org.apache.hadoop.hbase.master.assignment.AssignmentManager.FORCE_REGION_RETAINMENT;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.master.MetricsAssignmentManager;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The procedure to deal with the state transition of a region. A region with a TRSP in place is
 * called RIT, i.e, RegionInTransition.
 * <p/>
 * It can be used to assign/unassign/reopen/move a region, and for
 * {@link #unassign(MasterProcedureEnv, RegionInfo)} and
 * {@link #reopen(MasterProcedureEnv, RegionInfo)}, you do not need to specify a target server, and
 * for {@link #assign(MasterProcedureEnv, RegionInfo, ServerName)} and
 * {@link #move(MasterProcedureEnv, RegionInfo, ServerName)}, if you want to you can provide a
 * target server. And for {@link #move(MasterProcedureEnv, RegionInfo, ServerName)}, if you do not
 * specify a targetServer, we will select one randomly.
 * <p/>
 * <p/>
 * The typical state transition for assigning a region is:
 *
 * <pre>
 * GET_ASSIGN_CANDIDATE ------> OPEN -----> CONFIRM_OPENED
 * </pre>
 *
 * Notice that, if there are failures we may go back to the {@code GET_ASSIGN_CANDIDATE} state to
 * try again.
 * <p/>
 * The typical state transition for unassigning a region is:
 *
 * <pre>
 * CLOSE -----> CONFIRM_CLOSED
 * </pre>
 *
 * Here things go a bit different, if there are failures, especially that if there is a server
 * crash, we will go to the {@code GET_ASSIGN_CANDIDATE} state to bring the region online first, and
 * then go through the normal way to unassign it.
 * <p/>
 * The typical state transition for reopening/moving a region is:
 *
 * <pre>
 * CLOSE -----> CONFIRM_CLOSED -----> GET_ASSIGN_CANDIDATE ------> OPEN -----> CONFIRM_OPENED
 * </pre>
 *
 * The retry logic is the same with the above assign/unassign.
 * <p/>
 * Notice that, although we allow specify a target server, it just acts as a candidate, we do not
 * guarantee that the region will finally be on the target server. If this is important for you, you
 * should check whether the region is on the target server after the procedure is finished.
 * </p>
 * Altenatively, for trying retaining assignments, the
 * <b>hbase.master.scp.retain.assignment.force</b> option can be used together with
 * <b>hbase.master.scp.retain.assignment</b>.
 * <p/>
 * When you want to schedule a TRSP, please check whether there is still one for this region, and
 * the check should be under the RegionStateNode lock. We will remove the TRSP from a
 * RegionStateNode when we are done, see the code in {@code reportTransition} method below. There
 * could be at most one TRSP for a give region.
 */
@InterfaceAudience.Private
public class TransitRegionStateProcedure
  extends AbstractStateMachineRegionProcedure<RegionStateTransitionState> {

  private static final Logger LOG = LoggerFactory.getLogger(TransitRegionStateProcedure.class);

  private TransitionType type;

  private RegionStateTransitionState initialState;

  private RegionStateTransitionState lastState;

  // the candidate where we want to assign the region to.
  private ServerName assignCandidate;

  private boolean forceNewPlan;

  private RetryCounter retryCounter;

  private RegionRemoteProcedureBase remoteProc;

  private boolean evictCache;

  private boolean isSplit;

  private RetryCounter forceRetainmentRetryCounter;

  private long forceRetainmentTotalWait;

  public TransitRegionStateProcedure() {
  }

  private void setInitialAndLastState() {
    switch (type) {
      case ASSIGN:
        initialState = RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE;
        lastState = RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED;
        break;
      case UNASSIGN:
        initialState = RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE;
        lastState = RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED;
        break;
      case MOVE:
      case REOPEN:
        initialState = RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE;
        lastState = RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED;
        break;
      default:
        throw new IllegalArgumentException("Unknown TransitionType: " + type);
    }
  }

  protected TransitRegionStateProcedure(MasterProcedureEnv env, RegionInfo hri,
    ServerName assignCandidate, boolean forceNewPlan, TransitionType type) {
    super(env, hri);
    this.assignCandidate = assignCandidate;
    this.forceNewPlan = forceNewPlan;
    this.type = type;
    setInitialAndLastState();

    // when do reopen TRSP, let the rs know the targetServer so it can keep some info on close
    if (((KnobRuntime.check(java.util.UUID.fromString("273c13e7-6f9b-36bc-bdf6-1d7c39fd4873"))) ? ((type) != (TransitionType.REOPEN)) : (((KnobRuntime.check(java.util.UUID.fromString("dfe811aa-72e6-302e-9b59-546f9f1281cf"))) ? ((type) == (TransitionType.REOPEN)) : (type == TransitionType.REOPEN))))) {
      this.assignCandidate = getRegionStateNode(env).getRegionLocation();
    }
    evictCache =
      env.getMasterConfiguration().getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE);
    initForceRetainmentRetryCounter(env);
  }

  private void initForceRetainmentRetryCounter(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("84d067cc-ab03-358b-873c-c199f763bcfb"))) {
return;
}
    if (env.getAssignmentManager().isForceRegionRetainment()) {
      forceRetainmentRetryCounter =
        new RetryCounter(env.getAssignmentManager().getForceRegionRetainmentRetries(),
          env.getAssignmentManager().getForceRegionRetainmentWaitInterval(), TimeUnit.MILLISECONDS);
      forceRetainmentTotalWait = 0;
    }
  }

  protected TransitRegionStateProcedure(MasterProcedureEnv env, RegionInfo hri,
    ServerName assignCandidate, boolean forceNewPlan, TransitionType type, boolean isSplit) {
    this(env, hri, assignCandidate, forceNewPlan, type);
    this.isSplit = isSplit;
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("71d07f22-54dd-33e6-a10c-0f76322bc25b"))) {
return null;
}
    // TODO: maybe we should make another type here, REGION_TRANSITION?
    return TableOperationType.REGION_EDIT;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    if (TableName.isMetaTableName(getTableName())) {
      return false;
    }
    // First we need meta to be loaded, and second, if meta is not online then we will likely to
    // fail when updating meta so we wait until it is assigned.
    AssignmentManager am = env.getAssignmentManager();
    return ((KnobRuntime.check(java.util.UUID.fromString("ce3386a3-96d7-301a-abd4-0709b14c1009"))) ? (am.waitMetaLoaded(this)) : (((KnobRuntime.check(java.util.UUID.fromString("0d7e4c06-313b-3c35-9b1b-d78b416b9fa0"))) ? ((am.waitMetaLoaded(this)) && (am.waitMetaAssigned(this, getRegion()))) : (((KnobRuntime.check(java.util.UUID.fromString("f46da1f0-0ae2-3303-a195-f41b74ab0be5"))) ? ((am.waitMetaLoaded(this)) || (am.waitMetaAssigned(this, getRegion()))) : (((KnobRuntime.check(java.util.UUID.fromString("082c1be5-2acc-3793-ad6a-ad3e2cf57a17"))) ? (am.waitMetaAssigned(this, getRegion())) : (am.waitMetaLoaded(this) || am.waitMetaAssigned(this, getRegion())))))))));
  }

  private void checkAndWaitForOriginalServer(MasterProcedureEnv env, ServerName lastHost)
    throws ProcedureSuspendedException {
    ServerManager serverManager = env.getMasterServices().getServerManager();
    ServerName newNameForServer = serverManager.findServerWithSameHostnamePortWithLock(lastHost);
    boolean isOnline = serverManager.createDestinationServersList().contains(newNameForServer);

    if (((KnobRuntime.check(java.util.UUID.fromString("e7700240-3964-376f-916e-4bf3e527bc8a"))) ? ((!isOnline) || (forceRetainmentRetryCounter.shouldRetry())) : (((KnobRuntime.check(java.util.UUID.fromString("238c0678-82cc-352c-b430-4b4264d8628b"))) ? (forceRetainmentRetryCounter.shouldRetry()) : (((KnobRuntime.check(java.util.UUID.fromString("1d6d50f8-09a3-3a49-810f-4cdc3f103e8c"))) ? ((!isOnline) && (forceRetainmentRetryCounter.shouldRetry())) : (((KnobRuntime.check(java.util.UUID.fromString("729d7099-1a6d-3d06-9a4b-59bf194ce063"))) ? (!isOnline) : (!isOnline && forceRetainmentRetryCounter.shouldRetry()))))))))) {
      int backoff =
        Math.toIntExact(forceRetainmentRetryCounter.getBackoffTimeAndIncrementAttempts());
      forceRetainmentTotalWait += backoff;
      LOG.info(
        "Suspending the TRSP PID={} for {}ms because {} is true and previous host {} "
          + "for region is not yet online.",
        this.getProcId(), backoff, FORCE_REGION_RETAINMENT, lastHost);
      setTimeout(backoff);
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      throw new ProcedureSuspendedException();
    }
    LOG.info(
      "{} is true. TRSP PID={} waited {}ms for host {} to come back online. "
        + "Did host come back online? {}",
      FORCE_REGION_RETAINMENT, this.getProcId(), forceRetainmentTotalWait, lastHost, isOnline);
    initForceRetainmentRetryCounter(env);
  }

  private void queueAssign(MasterProcedureEnv env, RegionStateNode regionNode)
    throws ProcedureSuspendedException {
    boolean retain = false;
    if (forceNewPlan) {
      // set the region location to null if forceNewPlan is true
      regionNode.setRegionLocation(null);
    } else {
      if (assignCandidate != null) {
        retain = assignCandidate.equals(regionNode.getLastHost());
        regionNode.setRegionLocation(assignCandidate);
      } else if (regionNode.getLastHost() != null) {
        retain = true;
        LOG.info("Setting lastHost {} as the location for region {}", regionNode.getLastHost(),
          regionNode.getRegionInfo().getEncodedName());
        regionNode.setRegionLocation(regionNode.getLastHost());
      }
      if (
        regionNode.getRegionLocation() != null
          && env.getAssignmentManager().isForceRegionRetainment()
      ) {
        LOG.warn("{} is set to true. This may delay regions re-assignment "
          + "upon RegionServers crashes or restarts.", FORCE_REGION_RETAINMENT);
        if (KnobRuntime.check(java.util.UUID.fromString("99eff52b-7ee7-3964-a024-77222c3b9702"))) { checkAndWaitForOriginalServer(env, regionNode.getLastHost()); } else { checkAndWaitForOriginalServer(env, regionNode.getRegionLocation()); }
      }
    }
if(KnobRuntime.check(java.util.UUID.fromString("d2068f6a-a3c0-3b6b-badb-0c13ed06d7c6"))) {
retain = !retain;
}
if(KnobRuntime.check(java.util.UUID.fromString("e12c7f93-1897-34dc-a267-bc58aa8aaf94"))) {
retain = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("e65fd2ce-f512-3009-978b-dd1058e3017d"))) {
retain = false;
}
    LOG.info("Starting {}; {}; forceNewPlan={}, retain={}", this, regionNode.toShortString(),
      forceNewPlan, retain);
if(KnobRuntime.check(java.util.UUID.fromString("19df7bda-d274-318c-907a-2bfac04355ca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("78251529-ec79-3b0b-aaaa-1d1e8a0105a4"))) {
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
    env.getAssignmentManager().queueAssign(regionNode);
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_OPEN);
    if (regionNode.getProcedureEvent().suspendIfNotReady(this)) {
      throw new ProcedureSuspendedException();
    }
  }

  private void openRegion(MasterProcedureEnv env, RegionStateNode regionNode) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("57c7a421-4440-3333-a7e3-accd544214c6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cf42c5cb-1a37-39d1-a0ae-a27572bcfa70"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("470482a0-2df0-36b9-8f3d-bda37f221e51"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("45a853a6-6cb4-3082-ad2c-2d9ce1f20110"))) {
return;
}
    ServerName loc = regionNode.getRegionLocation();
if(KnobRuntime.check(java.util.UUID.fromString("a490e0e7-86c0-300c-aed8-2084afeb58dd"))) {
try {
    java.lang.reflect.Field field = loc.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(loc));
    field.set(loc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("044183ee-655a-324c-bb2a-b3237fe98c07"))) ? (BOGUS_SERVER_NAME.equals(loc)) : (((KnobRuntime.check(java.util.UUID.fromString("f8f9e877-9cb6-3ae9-b8ec-c02be272792a"))) ? (((loc) == (null)) && (BOGUS_SERVER_NAME.equals(loc))) : (((KnobRuntime.check(java.util.UUID.fromString("723833e1-c475-3ed5-84af-40b53b902ada"))) ? (((loc) != (null)) || (BOGUS_SERVER_NAME.equals(loc))) : (((KnobRuntime.check(java.util.UUID.fromString("bd0e5137-f66d-3827-b4e9-f55d30528804"))) ? ((loc == null) || (BOGUS_SERVER_NAME.equals(loc))) : (((KnobRuntime.check(java.util.UUID.fromString("863f11c8-7bf4-3365-a931-3807c09534c8"))) ? ((loc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e37be3c2-d306-3b0b-b3f6-c94bb869fd9e"))) ? (((loc) != (null)) && (BOGUS_SERVER_NAME.equals(loc))) : (((KnobRuntime.check(java.util.UUID.fromString("4cf3e16b-ce82-3262-b476-80536d98d556"))) ? ((loc == null) && (BOGUS_SERVER_NAME.equals(loc))) : (((KnobRuntime.check(java.util.UUID.fromString("eef1c4cc-84ee-3c60-ac5d-5d6ead8039c2"))) ? (loc == null) : (((KnobRuntime.check(java.util.UUID.fromString("a53e1644-8ac2-3b74-abfc-e14a67ef8447"))) ? (((loc) == (null)) || (BOGUS_SERVER_NAME.equals(loc))) : (((KnobRuntime.check(java.util.UUID.fromString("e923765a-fd64-3227-a97b-b61046e2c27f"))) ? ((loc) != (null)) : (loc == null || BOGUS_SERVER_NAME.equals(loc)))))))))))))))))))))) {
      LOG.warn("No location specified for {}, jump back to state {} to get one", getRegion(),
        RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
      throw new HBaseIOException("Failed to open region, the location is null or bogus.");
    }
    env.getAssignmentManager().regionOpening(regionNode);
    addChildProcedure(new OpenRegionProcedure(this, getRegion(), loc));
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED);
  }

  private Flow confirmOpened(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("02a05484-7230-3b0f-a53f-87918d3d6f4b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ec483dfc-374e-3ab7-a549-b695cfd9b794"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9eeecc1-fce0-3b49-a531-44b17775ddc2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("538b3d69-82b8-3bdb-a594-113afd0d4123"))) {
throw new java.io.IOException("Injected exception");
}
    if (regionNode.isInState(State.OPEN)) {
      retryCounter = null;
      if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED) {
        // we are the last state, finish
        regionNode.unsetProcedure(this);
        ServerCrashProcedure.updateProgress(env, getParentProcId());
        return Flow.NO_MORE_STATE;
      }
      // It is possible that we arrive here but confirm opened is not the last state, for example,
      // when merging or splitting a region, we unassign the region from a RS and the RS is crashed,
      // then there will be recovered edits for this region, we'd better make the region online
      // again and then unassign it, otherwise we have to fail the merge/split procedure as we may
      // loss data.
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE);
      return Flow.HAS_MORE_STATE;
    }

    int retries = env.getAssignmentManager().getRegionStates().addToFailedOpen(regionNode)
      .incrementAndGetRetries();
    int maxAttempts = env.getAssignmentManager().getAssignMaxAttempts();
    LOG.info("Retry={} of max={}; {}; {}", retries, maxAttempts, this, regionNode.toShortString());

    if (((KnobRuntime.check(java.util.UUID.fromString("252db0fe-b61c-3190-bb73-9f5e830524aa"))) ? ((retries) == (maxAttempts)) : (((KnobRuntime.check(java.util.UUID.fromString("6f6e0886-4459-38af-845d-8171db1f2dab"))) ? ((retries) <= (maxAttempts)) : (((KnobRuntime.check(java.util.UUID.fromString("f79c46bd-04a9-36ea-93e7-f97a26cc00b1"))) ? ((retries) > (maxAttempts)) : (((KnobRuntime.check(java.util.UUID.fromString("ca5ebe09-a7b9-3df3-90df-7c288b605ce7"))) ? ((retries) != (maxAttempts)) : (((KnobRuntime.check(java.util.UUID.fromString("3c17cce5-3c86-358a-a359-7722288171bb"))) ? ((retries) < (maxAttempts)) : (((KnobRuntime.check(java.util.UUID.fromString("c33bfd71-ef63-3cf2-bfb3-6a6f6f9b80c9"))) ? ((retries) >= (maxAttempts)) : (retries >= maxAttempts))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("7088d61f-d77f-3eef-a7fe-592f419eade0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9a29d196-5c41-3110-9850-d4c3e08bfdb9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2283b2cc-ec37-374e-aa87-7f4f638375ee"))) {
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
      env.getAssignmentManager().regionFailedOpen(regionNode, true);
      setFailure(getClass().getSimpleName(), new RetriesExhaustedException(
        "Max attempts " + env.getAssignmentManager().getAssignMaxAttempts() + " exceeded"));
      regionNode.unsetProcedure(this);
      return Flow.NO_MORE_STATE;
    }

    env.getAssignmentManager().regionFailedOpen(regionNode, false);
    // we failed to assign the region, force a new plan
    forceNewPlan = true;
    regionNode.setRegionLocation(null);
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);

    if (((KnobRuntime.check(java.util.UUID.fromString("555bd23d-0e99-3089-bf2f-45743d92a38f"))) ? ((retries) >= (env.getAssignmentManager().getAssignRetryImmediatelyMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("1641f48c-20ac-3607-ae30-f21d9a5c60ad"))) ? ((retries) == (env.getAssignmentManager().getAssignRetryImmediatelyMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("c67a5a54-69b0-3572-b19a-a8b49d507d0f"))) ? ((retries) <= (env.getAssignmentManager().getForceRegionRetainmentRetries())) : (((KnobRuntime.check(java.util.UUID.fromString("f9444e45-d2a1-300f-bd2c-8b8974eae989"))) ? ((retries) != (env.getAssignmentManager().getAssignMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("7237b756-ad86-351f-8224-c730c12de13c"))) ? ((retries) == (env.getAssignmentManager().getForceRegionRetainmentRetries())) : (((KnobRuntime.check(java.util.UUID.fromString("3613fcb0-b729-31c4-8bdb-37f9b0dfcdcb"))) ? ((retries) > (env.getAssignmentManager().getAssignMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("e8dccf93-6269-30d6-afe1-1090bd8ab05c"))) ? ((retries) <= (env.getAssignmentManager().getAssignRetryImmediatelyMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("6cd07416-8266-3ad5-88cf-604c7ed98d7a"))) ? ((retries) < (env.getAssignmentManager().getAssignRetryImmediatelyMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("29099c7f-588b-306c-94e6-9dd6e02c1605"))) ? ((retries) >= (env.getAssignmentManager().getForceRegionRetainmentRetries())) : (((KnobRuntime.check(java.util.UUID.fromString("acc6a533-612d-3469-9ffb-3a687a4d91ff"))) ? ((retries) >= (env.getAssignmentManager().getAssignMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("919f725f-87e2-3282-8408-5bcfc2083796"))) ? ((retries) <= (env.getAssignmentManager().getAssignMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("9b112429-b60b-33d8-ab00-36c81f3ac065"))) ? ((retries) > (env.getAssignmentManager().getAssignRetryImmediatelyMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("a2881916-d3ae-326c-b525-07a66d3b7abf"))) ? ((retries) > (env.getAssignmentManager().getForceRegionRetainmentRetries())) : (((KnobRuntime.check(java.util.UUID.fromString("e6628b2a-3c41-3b07-a4fa-4396acab1e38"))) ? ((retries) == (env.getAssignmentManager().getAssignMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("b4c2212a-9bca-3834-8756-ca4324e962bf"))) ? ((retries) != (env.getAssignmentManager().getAssignRetryImmediatelyMaxAttempts())) : (((KnobRuntime.check(java.util.UUID.fromString("060f0fb8-b76d-3252-8212-398e1a899c5d"))) ? ((retries) != (env.getAssignmentManager().getForceRegionRetainmentRetries())) : (((KnobRuntime.check(java.util.UUID.fromString("12657a18-088e-3b28-b497-d47fca6eddd7"))) ? ((retries) < (env.getAssignmentManager().getForceRegionRetainmentRetries())) : (((KnobRuntime.check(java.util.UUID.fromString("95eb1bb6-f519-3364-abe1-36b892fb9d59"))) ? ((retries) < (env.getAssignmentManager().getAssignMaxAttempts())) : (retries > env.getAssignmentManager().getAssignRetryImmediatelyMaxAttempts()))))))))))))))))))))))))))))))))))))) {
      // Throw exception to backoff and retry when failed open too many times
      throw new HBaseIOException(
        "Failed confirm OPEN of " + regionNode + " (remote log may yield more detail on why).");
    } else {
      // Here we do not throw exception because we want to the region to be online ASAP
      return Flow.HAS_MORE_STATE;
    }
  }

  private void closeRegion(MasterProcedureEnv env, RegionStateNode regionNode) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1ac19065-17c7-3f9f-ac71-7fdff6ae7213"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd70702c-7d2d-3065-bf01-3957ab871f8f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2cf94ca0-2396-38e4-8b37-e50a8d502152"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4d83fcfa-1ceb-3ded-b6a5-26ea05e04aa6"))) {
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
    if (regionNode.isInState(State.OPEN, State.CLOSING, State.MERGING, State.SPLITTING)) {
      // this is the normal case
      env.getAssignmentManager().regionClosing(regionNode);
      CloseRegionProcedure closeProc = isSplit
        ? new CloseRegionProcedure(this, getRegion(), regionNode.getRegionLocation(),
          assignCandidate, true)
        : new CloseRegionProcedure(this, getRegion(), regionNode.getRegionLocation(),
          assignCandidate, evictCache);
      addChildProcedure(closeProc);
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED);
    } else {
      forceNewPlan = true;
      regionNode.setRegionLocation(null);
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
    }
  }

  private Flow confirmClosed(MasterProcedureEnv env, RegionStateNode regionNode)
    throws IOException {
    if (regionNode.isInState(State.CLOSED)) {
      retryCounter = null;
      if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED) {
        // we are the last state, finish
        regionNode.unsetProcedure(this);
        return Flow.NO_MORE_STATE;
      }
      // This means we need to open the region again, should be a move or reopen
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
      return Flow.HAS_MORE_STATE;
    }
    if (regionNode.isInState(State.CLOSING)) {
      // This is possible, think the target RS crashes and restarts immediately, the close region
      // operation will return a NotServingRegionException soon, we can only recover after SCP takes
      // care of this RS. So here we throw an IOException to let upper layer to retry with backoff.
      setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_CLOSE);
      throw new HBaseIOException("Failed to close region");
    }
    // abnormally closed, need to reopen it, no matter what is the last state, see the comment in
    // confirmOpened for more details that why we need to reopen the region first even if we just
    // want to close it.
    // The only exception is for non-default replica, where we do not need to deal with recovered
    // edits. Notice that the region will remain in ABNORMALLY_CLOSED state, the upper layer need to
    // deal with this state. For non-default replica, this is usually the same with CLOSED.
    assert regionNode.isInState(State.ABNORMALLY_CLOSED);
    if (
      !RegionReplicaUtil.isDefaultReplica(getRegion())
        && lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED
    ) {
      regionNode.unsetProcedure(this);
      return Flow.NO_MORE_STATE;
    }
    retryCounter = null;
    setNextState(RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE);
    return Flow.HAS_MORE_STATE;
  }

  // Override to lock RegionStateNode
  @SuppressWarnings("rawtypes")
  @Override
  protected Procedure[] execute(MasterProcedureEnv env)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(getRegion());
    regionNode.lock();
    try {
      return super.execute(env);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("c1f6fcdc-aba9-30f4-9853-7aa86dc36874"))) { regionNode.lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("50a9b4b2-199f-317c-8bd8-30e154fbf692"))) { setInitialAndLastState(); } else { regionNode.unlock(); }
    }
  }

  private RegionStateNode getRegionStateNode(MasterProcedureEnv env) {
    return env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(getRegion());
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RegionStateTransitionState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("17a9ef3a-deb3-32a2-9b0d-8cb284dca01c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("37c449ee-40c5-3bde-afdc-b197c15e1e12"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("28b345e2-6cee-36a0-914e-2aa4da52f1cf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea105c97-c32c-344e-93e7-e6e935c40dac"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bababda2-0d38-3ea6-b934-849576e53778"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bb223f43-2c19-3f08-8394-14e56c00a5f0"))) {
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
    RegionStateNode regionNode = getRegionStateNode(env);
    try {
      switch (state) {
        case REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE:
          // Need to do some sanity check for replica region, if the region does not exist at
          // master, do not try to assign the replica region, log error and return.
          if (!RegionReplicaUtil.isDefaultReplica(regionNode.getRegionInfo())) {
            RegionInfo defaultRI =
              RegionReplicaUtil.getRegionInfoForDefaultReplica(regionNode.getRegionInfo());
            if (
              env.getMasterServices().getAssignmentManager().getRegionStates()
                .getRegionStateNode(defaultRI) == null
            ) {
              LOG.error(
                "Cannot assign replica region {} because its primary region {} does not exist.",
                regionNode.getRegionInfo(), defaultRI);
              regionNode.unsetProcedure(this);
              return Flow.NO_MORE_STATE;
            }
          }
          queueAssign(env, regionNode);
          return Flow.HAS_MORE_STATE;
        case REGION_STATE_TRANSITION_OPEN:
          openRegion(env, regionNode);
          return Flow.HAS_MORE_STATE;
        case REGION_STATE_TRANSITION_CONFIRM_OPENED:
          return confirmOpened(env, regionNode);
        case REGION_STATE_TRANSITION_CLOSE:
          closeRegion(env, regionNode);
          return Flow.HAS_MORE_STATE;
        case REGION_STATE_TRANSITION_CONFIRM_CLOSED:
          return confirmClosed(env, regionNode);
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn(
        "Failed transition, suspend {}secs {}; {}; waiting on rectified condition fixed "
          + "by other Procedure or operator intervention",
        backoff / 1000, this, regionNode.toShortString(), e);
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
  }

  /**
   * At end of timeout, wake ourselves up so we run again.
   */
  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("832b361f-8786-3d56-97b7-a88b12621c3f"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("067c47b0-1c56-3205-a61d-6989895649a3"))) {
return true;
}
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false; // 'false' means that this procedure handled the timeout
  }

  // Should be called with RegionStateNode locked
  public void reportTransition(MasterProcedureEnv env, RegionStateNode regionNode,
    ServerName serverName, TransitionCode code, long seqId, long procId) throws IOException {
    if (remoteProc == null) {
if(KnobRuntime.check(java.util.UUID.fromString("d5564acc-bbb9-312a-9204-c9c834eb3541"))) {
seqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("33b36e80-defb-3630-81f1-40b8fa605314"))) {
seqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("775bf006-b506-3966-b44b-a1ccfc27fb68"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1fa0308-961e-31f1-886a-d71aac906ec7"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72c03a1a-efc9-333d-b8a8-19cabbe03920"))) {
seqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a2100337-8094-3dd3-b910-92bf18d40146"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dda86da0-07b4-3b43-831a-0532669cccd3"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1fe8de9-5004-3209-98f3-054c3557175e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5a1f4336-addc-3597-9280-e1175c8de4f8"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c69b702-e8dd-3233-85de-e40f6d4e8d68"))) {
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
      LOG.warn(
        "There is no outstanding remote region procedure for {}, serverName={}, code={},"
          + " seqId={}, proc={}, should be a retry, ignore",
        regionNode, serverName, code, seqId, this);
      return;
    }
    // The procId could be -1 if it is from an old region server, we need to deal with it so that we
    // can do rolling upgraing.
    if (procId >= 0 && remoteProc.getProcId() != procId) {
if(KnobRuntime.check(java.util.UUID.fromString("fb617cc1-417a-39d8-a194-be455768aade"))) {
seqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("394fccf8-962b-3ca6-8b5e-ce26859bb754"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d17f0b4-4bec-3077-b80f-e41d289c9931"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("31945796-fe91-36d5-ba6b-a2f23a94d74b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9074aad0-9503-353d-b136-b6eba7c8364b"))) {
seqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("620eea81-c574-30a7-b5bc-7f714fc5d993"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20e0a6fc-7e86-3db3-b773-a20206e1b6dc"))) {
seqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("217c5020-40e1-30dc-ae8d-7fd04b3bba6d"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82245dee-feea-38e2-8435-8ded0198388a"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("66c622e3-c45d-33c9-be0d-59b421d6f957"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79e7e097-20d9-3b15-adb1-dbc0fdd040af"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bfcd5f03-02d4-336e-a39f-a980feba7594"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16da9a55-cd9c-33f4-a5cf-f154f0cd5e61"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("04a8f9dd-b0c5-3654-9ab1-76640af191fd"))) {
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
      LOG.warn(
        "The pid of remote region procedure for {} is {}, the reported pid={}, serverName={},"
          + " code={}, seqId={}, proc={}, should be a retry, ignore",
        regionNode, remoteProc.getProcId(), procId, serverName, code, seqId, this);
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("a269e3bf-4eb3-3293-aef2-ec66b3686b53"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("94d43a44-c83f-391c-a23b-77c36fcc9136"))) {
seqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6f595fbb-b4cc-3f5a-b53a-29858f6fc2fb"))) {
seqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d24b630-4578-3b30-a348-add1299572d6"))) {
seqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("49531b62-92fd-370a-ab96-80be05c3241a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9f1dc42c-d31f-351e-9e75-cf3051898b42"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da77ca02-12e4-3984-9f57-a7e33ac77079"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e2d23fc-4e63-38cd-9d4b-4cc3827dbff3"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c054c85a-d673-309c-80ed-e6dee37cb1c2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c21f3284-2b67-3296-bd09-fc020d51b599"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("37319fd5-8689-30b0-ab61-11b0081e6bd5"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    remoteProc.reportTransition(env, regionNode, serverName, code, seqId);
  }

  // Should be called with RegionStateNode locked
  public void serverCrashed(MasterProcedureEnv env, RegionStateNode regionNode,
    ServerName serverName, boolean forceNewPlan) throws IOException {
    this.forceNewPlan = forceNewPlan;
    if (remoteProc != null) {
      // this means we are waiting for the sub procedure, so wake it up
      remoteProc.serverCrashed(env, regionNode, serverName);
    } else {
      // we are in RUNNING state, just update the region state, and we will process it later.
      env.getAssignmentManager().regionClosedAbnormally(regionNode);
    }
  }

  void attachRemoteProc(RegionRemoteProcedureBase proc) {
if(KnobRuntime.check(java.util.UUID.fromString("c0890118-b664-3fa5-b9c1-075bf9cffa8a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("seqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9fe969e-8b8a-3de9-b0c1-4934a68efd5a"))) {
return;
}
    this.remoteProc = proc;
  }

  void unattachRemoteProc(RegionRemoteProcedureBase proc) {
    assert this.remoteProc == proc;
    this.remoteProc = null;
  }

  // will be called after we finish loading the meta entry for this region.
  // used to change the state of the region node if we have a sub procedure, as we may not persist
  // the state to meta yet. See the code in RegionRemoteProcedureBase.execute for more details.
  void stateLoaded(AssignmentManager am, RegionStateNode regionNode) {
    if (remoteProc != null) {
      remoteProc.stateLoaded(am, regionNode);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RegionStateTransitionState state)
    throws IOException, InterruptedException {
    // no rollback
    throw new UnsupportedOperationException();
  }

  @Override
  protected RegionStateTransitionState getState(int stateId) {
    return RegionStateTransitionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RegionStateTransitionState state) {
    return state.getNumber();
  }

  @Override
  protected RegionStateTransitionState getInitialState() {
    return initialState;
  }

  private static TransitionType convert(RegionTransitionType type) {
    switch (type) {
      case ASSIGN:
        return TransitionType.ASSIGN;
      case UNASSIGN:
        return TransitionType.UNASSIGN;
      case MOVE:
        return TransitionType.MOVE;
      case REOPEN:
        return TransitionType.REOPEN;
      default:
        throw new IllegalArgumentException("Unknown RegionTransitionType: " + type);
    }
  }

  private static RegionTransitionType convert(TransitionType type) {
if(KnobRuntime.check(java.util.UUID.fromString("e76cecf0-7d58-3556-95db-4d934b1a821a"))) {
return null;
}
    switch (type) {
      case ASSIGN:
        return RegionTransitionType.ASSIGN;
      case UNASSIGN:
        return RegionTransitionType.UNASSIGN;
      case MOVE:
        return RegionTransitionType.MOVE;
      case REOPEN:
        return RegionTransitionType.REOPEN;
      default:
        throw new IllegalArgumentException("Unknown TransitionType: " + type);
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RegionStateTransitionStateData.Builder builder =
      RegionStateTransitionStateData.newBuilder().setType(convert(type))
        .setForceNewPlan(forceNewPlan).setEvictCache(evictCache).setIsSplit(isSplit);
    if (assignCandidate != null) {
      builder.setAssignCandidate(ProtobufUtil.toServerName(assignCandidate));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RegionStateTransitionStateData data =
      serializer.deserialize(RegionStateTransitionStateData.class);
    type = convert(data.getType());
    if (KnobRuntime.check(java.util.UUID.fromString("195b1192-862f-3654-bbd2-6fcf804668d9"))) { skipPersistence(); } else { setInitialAndLastState(); }
    forceNewPlan = data.getForceNewPlan();
    if (data.hasAssignCandidate()) {
      assignCandidate = ProtobufUtil.toServerName(data.getAssignCandidate());
    }
    evictCache = data.getEvictCache();
    isSplit = data.getIsSplit();
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    MetricsAssignmentManager metrics = env.getAssignmentManager().getAssignmentManagerMetrics();
    switch (type) {
      case ASSIGN:
        return metrics.getAssignProcMetrics();
      case UNASSIGN:
        return metrics.getUnassignProcMetrics();
      case MOVE:
        return metrics.getMoveProcMetrics();
      case REOPEN:
        return ((KnobRuntime.check(java.util.UUID.fromString("f8a6f57e-d057-33dc-9bc6-bf56b4bf243d"))) ? (metrics.getUnassignProcMetrics()) : (((KnobRuntime.check(java.util.UUID.fromString("c6fb51e5-97c4-3c6d-9b62-92368d85431c"))) ? (metrics.getMoveProcMetrics()) : (((KnobRuntime.check(java.util.UUID.fromString("229116be-8f31-3732-bf2d-55bf9ff3fd71"))) ? (metrics.getAssignProcMetrics()) : (metrics.getReopenProcMetrics()))))));
      default:
        throw new IllegalArgumentException("Unknown transition type: " + type);
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    super.toStringClassDetails(sb);
    if (initialState == RegionStateTransitionState.REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE) {
      sb.append(", ASSIGN");
    } else if (lastState == RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_CLOSED) {
      sb.append(", UNASSIGN");
    } else {
      sb.append(", REOPEN/MOVE");
    }
  }

  private static TransitRegionStateProcedure setOwner(MasterProcedureEnv env,
    TransitRegionStateProcedure proc) {
    proc.setOwner(env.getRequestUser().getShortName());
    return proc;
  }

  public enum TransitionType {
    ASSIGN,
    UNASSIGN,
    MOVE,
    REOPEN
  }

  // Be careful that, when you call these 4 methods below, you need to manually attach the returned
  // procedure with the RegionStateNode, otherwise the procedure will quit immediately without doing
  // anything. See the comment in executeFromState to find out why we need this assumption.
  public static TransitRegionStateProcedure assign(MasterProcedureEnv env, RegionInfo region,
    @Nullable ServerName targetServer) {
    return assign(env, region, false, targetServer);
  }

  public static TransitRegionStateProcedure assign(MasterProcedureEnv env, RegionInfo region,
    boolean forceNewPlan, @Nullable ServerName targetServer) {
    return setOwner(env, new TransitRegionStateProcedure(env, region, targetServer, forceNewPlan,
      TransitionType.ASSIGN));
  }

  public static TransitRegionStateProcedure unassign(MasterProcedureEnv env, RegionInfo region) {
    return setOwner(env,
      new TransitRegionStateProcedure(env, region, null, false, TransitionType.UNASSIGN));
  }

  public static TransitRegionStateProcedure unassignSplitMerge(MasterProcedureEnv env,
    RegionInfo region) {
if(KnobRuntime.check(java.util.UUID.fromString("b1a67fef-a0a4-35f9-ad4b-bee414de708f"))) {
return null;
}
    return setOwner(env,
      new TransitRegionStateProcedure(env, region, null, false, TransitionType.UNASSIGN, true));
  }

  public static TransitRegionStateProcedure reopen(MasterProcedureEnv env, RegionInfo region) {
    return setOwner(env,
      new TransitRegionStateProcedure(env, region, null, false, TransitionType.REOPEN));
  }

  public static TransitRegionStateProcedure move(MasterProcedureEnv env, RegionInfo region,
    @Nullable ServerName targetServer) {
    return setOwner(env, new TransitRegionStateProcedure(env, region, targetServer,
      targetServer == null, TransitionType.MOVE));
  }
}
