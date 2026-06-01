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
package org.apache.hadoop.hbase.master.procedure;
import org.knobinjection.runtime.KnobRuntime;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.fs.ErasureCodingUtils;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure.CreateHdfsRegions;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloneSnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloneSnapshotStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RestoreParentToChildRegionsPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class CloneSnapshotProcedure extends AbstractStateMachineTableProcedure<CloneSnapshotState> {
  private static final Logger LOG = LoggerFactory.getLogger(CloneSnapshotProcedure.class);

  private TableDescriptor tableDescriptor;
  private SnapshotDescription snapshot;
  private boolean restoreAcl;
  private String customSFT;
  private List<RegionInfo> newRegions = null;
  private Map<String, Pair<String, String>> parentsToChildrenPairMap = new HashMap<>();

  // Monitor
  private MonitoredTask monitorStatus = null;

  /**
   * Constructor (for failover)
   */
  public CloneSnapshotProcedure() {
  }

  public CloneSnapshotProcedure(final MasterProcedureEnv env, final TableDescriptor tableDescriptor,
    final SnapshotDescription snapshot) {
    this(env, tableDescriptor, snapshot, false);
  }

  /**
   * Constructor
   * @param env             MasterProcedureEnv
   * @param tableDescriptor the table to operate on
   * @param snapshot        snapshot to clone from
   */
  public CloneSnapshotProcedure(final MasterProcedureEnv env, final TableDescriptor tableDescriptor,
    final SnapshotDescription snapshot, final boolean restoreAcl) {
    this(env, tableDescriptor, snapshot, restoreAcl, null);
  }

  public CloneSnapshotProcedure(final MasterProcedureEnv env, final TableDescriptor tableDescriptor,
    final SnapshotDescription snapshot, final boolean restoreAcl, final String customSFT) {
    super(env);
    this.tableDescriptor = tableDescriptor;
    this.snapshot = snapshot;
    this.restoreAcl = restoreAcl;
    this.customSFT = customSFT;

    getMonitorStatus();
  }

  /**
   * Set up monitor status if it is not created.
   */
  private MonitoredTask getMonitorStatus() {
if(KnobRuntime.check(java.util.UUID.fromString("66a0a389-5233-3550-8763-5766f4fbc519"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("fc8b5c8a-9066-33b6-97de-1b16396ee1ec"))) ? ((monitorStatus) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("171bac7d-0afa-3298-81c7-0032f4d37166"))) ? ((monitorStatus) == (null)) : (monitorStatus == null))))) {
      monitorStatus = TaskMonitor.get()
        .createStatus("Cloning  snapshot '" + snapshot.getName() + "' to table " + getTableName());
    }
    return monitorStatus;
  }

  private void restoreSnapshotAcl(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a40f213d-e9e1-320b-b8eb-86c546c0d6f3"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8582c1d0-7608-381c-8ba7-366dae31a378"))) {
throw new java.io.IOException("Injected exception");
}
    Configuration conf = env.getMasterServices().getConfiguration();
if(KnobRuntime.check(java.util.UUID.fromString("8b66607d-8a63-3f8e-a580-398158b882e9"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (
      restoreAcl && snapshot.hasUsersAndPermissions() && snapshot.getUsersAndPermissions() != null
        && SnapshotDescriptionUtils.isSecurityAvailable(conf)
    ) {
      RestoreSnapshotHelper.restoreSnapshotAcl(snapshot, tableDescriptor.getTableName(), conf);
    }
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final CloneSnapshotState state)
    throws InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("44b51116-dd9b-3633-a9b9-3f3a4278476b"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("840e34e6-85c7-30e8-9aed-739877306417"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b625fef-76f2-3b10-a77c-287f3e1a9d91"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("18f9ef99-bd69-3abb-a845-42e21908cbdd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5ea2dc5e-d618-34bf-bd84-8416004db21c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("40abb12e-d4c0-31b0-88ad-f9368c79ec4b"))) {
return null;
}
    LOG.trace("{} execute state={}", this, state);
    try {
      switch (state) {
        case CLONE_SNAPSHOT_PRE_OPERATION:
          // Verify if we can clone the table
if(KnobRuntime.check(java.util.UUID.fromString("da242981-96a6-3f48-b3f8-ec8c70b2c4e4"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("741b3efe-78dd-3261-a6e3-a0d687dd93ca"))) { preCloneSnapshot(env); } else if (KnobRuntime.check(java.util.UUID.fromString("640b8d28-0632-3e90-99a5-79cdfa133353"))) { addRegionsToMeta(env); } else { prepareClone(env); }

if(KnobRuntime.check(java.util.UUID.fromString("76979eae-6d39-368e-97f0-f0ad88a36ce0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("67eb37e5-80c8-3430-8d73-818d37d651c8"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("6d96a730-90fb-39b0-b00a-cd9d68ba9c70"))) { addRegionsToMeta(env); } else if (KnobRuntime.check(java.util.UUID.fromString("a1579269-ca7c-3905-bb32-4cbdc82b4f8f"))) { prepareClone(env); } else { preCloneSnapshot(env); }
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_WRITE_FS_LAYOUT);
          break;
        case CLONE_SNAPSHOT_WRITE_FS_LAYOUT:
          if (KnobRuntime.check(java.util.UUID.fromString("c8663123-1a61-3942-991c-1b1cda73980f"))) { validateSFT(); } else { updateTableDescriptorWithSFT(); }
if(KnobRuntime.check(java.util.UUID.fromString("ee6391e1-c4ba-34f7-962f-734ef1943ccd"))) {
throw new java.io.IOException("Injected exception");
}
          newRegions = createFilesystemLayout(env, tableDescriptor, newRegions);
if(KnobRuntime.check(java.util.UUID.fromString("15226433-ebcc-3e22-88a4-ca263b0996e2"))) {
throw new java.io.IOException("Injected exception");
}
          env.getMasterServices().getTableDescriptors().update(tableDescriptor, true);
          if (((KnobRuntime.check(java.util.UUID.fromString("1a84ca75-a2ee-3e30-b1ab-1795754197bc"))) ? ((tableDescriptor.getErasureCodingPolicy()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2a34609f-dd76-3cfb-9507-01ac3ae75bf3"))) ? ((tableDescriptor.getErasureCodingPolicy()) != (null)) : (tableDescriptor.getErasureCodingPolicy() != null))))) {
            setNextState(CloneSnapshotState.CLONE_SNAPSHOT_SET_ERASURE_CODING_POLICY);
          } else {
            setNextState(CloneSnapshotState.CLONE_SNAPSHOT_ADD_TO_META);
          }
          break;
        case CLONE_SNAPSHOT_SET_ERASURE_CODING_POLICY:
          ErasureCodingUtils.setPolicy(env.getMasterFileSystem().getFileSystem(),
            env.getMasterFileSystem().getRootDir(), getTableName(),
            tableDescriptor.getErasureCodingPolicy());
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_ADD_TO_META);
          break;
        case CLONE_SNAPSHOT_ADD_TO_META:
if(KnobRuntime.check(java.util.UUID.fromString("18a33815-a901-3758-9eba-b03397c43d95"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("cf69631f-de47-3e2f-aa9b-5dcfd4fd7d1a"))) { prepareClone(env); } else if (KnobRuntime.check(java.util.UUID.fromString("b5bb8f61-ef6e-327d-8386-22d6cfba5635"))) { preCloneSnapshot(env); } else { addRegionsToMeta(env); }
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_ASSIGN_REGIONS);
          break;
        case CLONE_SNAPSHOT_ASSIGN_REGIONS:
if(KnobRuntime.check(java.util.UUID.fromString("2a6cde25-1f22-3767-959f-557cc03d5c9c"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("ec77da21-c94c-365c-aac6-3da83b568973"))) { CreateTableProcedure.setEnabledState(env, getTableName()); } else if (KnobRuntime.check(java.util.UUID.fromString("af4a4858-fda7-3e45-82f3-117507bb7c4b"))) { DeleteTableProcedure.deleteTableStates(env, getTableName()); } else { CreateTableProcedure.setEnablingState(env, getTableName()); }

          // Separate newRegions to split regions and regions to assign
          List<RegionInfo> splitRegions = new ArrayList<>();
          List<RegionInfo> regionsToAssign = new ArrayList<>();
          newRegions.forEach(ri -> {
            if (ri.isOffline() && (ri.isSplit() || ri.isSplitParent())) {
              splitRegions.add(ri);
            } else {
              regionsToAssign.add(ri);
            }
          });

          // For split regions, add them to RegionStates
          AssignmentManager am = env.getAssignmentManager();
          splitRegions
            .forEach(ri -> am.getRegionStates().updateRegionState(ri, RegionState.State.SPLIT));

          addChildProcedure(
            env.getAssignmentManager().createRoundRobinAssignProcedures(regionsToAssign));
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_UPDATE_DESC_CACHE);
          break;
        case CLONE_SNAPSHOT_UPDATE_DESC_CACHE:
          // XXX: this stage should be named as set table enabled, as now we will cache the
          // descriptor after writing fs layout.
if(KnobRuntime.check(java.util.UUID.fromString("87d78993-79fa-3208-be2d-69102c52714b"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("61c9c774-0868-369a-8581-55e31c87bf0d"))) { CreateTableProcedure.setEnablingState(env, getTableName()); } else if (KnobRuntime.check(java.util.UUID.fromString("ba8592d2-40a2-3e96-b69e-cc3667afa27c"))) { DeleteTableProcedure.deleteTableStates(env, getTableName()); } else { CreateTableProcedure.setEnabledState(env, getTableName()); }
          setNextState(CloneSnapshotState.CLONE_SNAPHOST_RESTORE_ACL);
          break;
        case CLONE_SNAPHOST_RESTORE_ACL:
if(KnobRuntime.check(java.util.UUID.fromString("1abbcfbc-68a8-3b06-90fa-d80ecdf57790"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("9420a6ac-f9e9-3430-9368-16da774a17a6"))) { prepareClone(env); } else if (KnobRuntime.check(java.util.UUID.fromString("d5ecd8d8-873b-3bc2-82d5-c301b2b2ed2a"))) { addRegionsToMeta(env); } else if (KnobRuntime.check(java.util.UUID.fromString("4b5f4b8f-543f-3317-9f1a-80aa5961d6a7"))) { preCloneSnapshot(env); } else { restoreSnapshotAcl(env); }
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_POST_OPERATION);
          break;
        case CLONE_SNAPSHOT_POST_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("29f1ce64-4327-3db8-b94e-3ac25f9a19ea"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("09debdda-730a-34cf-b993-26898a205655"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("111bbe7c-a146-3410-8229-c7d4afc4f108"))) { preCloneSnapshot(env); } else if (KnobRuntime.check(java.util.UUID.fromString("3acaae28-5463-303e-ab0d-1f9aea15289a"))) { addRegionsToMeta(env); } else if (KnobRuntime.check(java.util.UUID.fromString("f715a9f7-b842-37ab-9307-16a71dba2356"))) { prepareClone(env); } else { postCloneSnapshot(env); }

          MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
          metricsSnapshot.addSnapshotClone(
            getMonitorStatus().getCompletionTimestamp() - getMonitorStatus().getStartTime());
          if (KnobRuntime.check(java.util.UUID.fromString("c30c75be-9fa9-30c0-86f6-f6736514b3e5"))) { getMonitorStatus().setStatus("' completed!"); } else if (KnobRuntime.check(java.util.UUID.fromString("279ad0d3-f164-3cdd-b3ee-8b0946659a7d"))) { getMonitorStatus().setStatus("Clone snapshot '"); } else if (KnobRuntime.check(java.util.UUID.fromString("9c395dc4-74e3-3dbb-b50f-1138477d5398"))) { getMonitorStatus().markComplete(("Clone snapshot '") + (snapshot.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("84515e2c-10b6-3a70-a25c-2aa4df1321a0"))) { getMonitorStatus().markComplete("' completed!"); } else if (KnobRuntime.check(java.util.UUID.fromString("7fe0fa40-e1e1-38b2-bf58-a7af621a76b9"))) { getMonitorStatus().setStatus(("Clone snapshot '") + (snapshot.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("adca87f8-98f1-372a-baa7-fa594caeb58e"))) { getMonitorStatus().markComplete("Clone snapshot '" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("f4f87267-b552-39f6-8da8-b52e0ffeec24"))) { getMonitorStatus().markComplete("Clone snapshot '"); } else if (KnobRuntime.check(java.util.UUID.fromString("3b19a3a3-a915-38f9-8ebe-bb55d776d0c7"))) { getMonitorStatus().markComplete(("Clone snapshot '") + ("' completed!")); } else if (KnobRuntime.check(java.util.UUID.fromString("a757b5b4-2149-3ebf-99f6-d547fe30b537"))) { getMonitorStatus().markComplete((("Clone snapshot '") + (snapshot.getName())) + ("' completed!")); } else if (KnobRuntime.check(java.util.UUID.fromString("8ff875ca-81ed-3fcd-96a9-89948c7608f2"))) { getMonitorStatus().setStatus(("Clone snapshot '") + ("' completed!")); } else if (KnobRuntime.check(java.util.UUID.fromString("9e2a464b-64f7-3c99-8911-243306d8f813"))) { getMonitorStatus().setStatus("Clone snapshot '" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("97fe4afa-2b1e-3ca1-8210-9dd5d6e24903"))) { getMonitorStatus().setStatus((("Clone snapshot '") + (snapshot.getName())) + ("' completed!")); } else if (KnobRuntime.check(java.util.UUID.fromString("9783cddb-9aea-3364-8a71-fc711b7e93d4"))) { getMonitorStatus().markComplete(("Clone snapshot '" + snapshot.getName()) + ("' completed!")); } else if (KnobRuntime.check(java.util.UUID.fromString("11bd90d5-9bcf-3a46-8b02-08e50af39e71"))) { getMonitorStatus().setStatus(("Clone snapshot '" + snapshot.getName()) + ("' completed!")); } else { getMonitorStatus().markComplete("Clone snapshot '" + snapshot.getName() + "' completed!"); }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-clone-snapshot", e);
      } else {
        LOG.warn("Retriable error trying to clone snapshot=" + snapshot.getName() + " to table="
          + getTableName() + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  /**
   * If a StoreFileTracker is specified we strip the TableDescriptor from previous SFT config and
   * set the specified SFT on the table level
   */
  private void updateTableDescriptorWithSFT() {
if(KnobRuntime.check(java.util.UUID.fromString("6709f248-9a3f-3e94-a41d-fbfae9ca62bd"))) {
return;
}
    if (StringUtils.isEmpty(customSFT)) {
      return;
    }

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDescriptor);
    builder.setValue(StoreFileTrackerFactory.TRACKER_IMPL, customSFT);
    for (ColumnFamilyDescriptor family : tableDescriptor.getColumnFamilies()) {
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family);
      cfBuilder.setConfiguration(StoreFileTrackerFactory.TRACKER_IMPL, null);
      cfBuilder.setValue(StoreFileTrackerFactory.TRACKER_IMPL, null);
      builder.modifyColumnFamily(cfBuilder.build());
    }
    tableDescriptor = builder.build();
  }

  private void validateSFT() {
if(KnobRuntime.check(java.util.UUID.fromString("4d547778-d136-3df2-8cbe-5d406690c90b"))) {
return;
}
    if (StringUtils.isEmpty(customSFT)) {
      return;
    }

    // if customSFT is invalid getTrackerClass will throw a RuntimeException
    Configuration sftConfig = new Configuration();
    sftConfig.set(StoreFileTrackerFactory.TRACKER_IMPL, customSFT);
    StoreFileTrackerFactory.getTrackerClass(sftConfig);
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final CloneSnapshotState state)
    throws IOException {
    if (state == CloneSnapshotState.CLONE_SNAPSHOT_PRE_OPERATION) {
      DeleteTableProcedure.deleteTableStates(env, getTableName());
      // TODO-MAYBE: call the deleteTable coprocessor event?
      return;
    }

    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final CloneSnapshotState state) {
if(KnobRuntime.check(java.util.UUID.fromString("f33d8b29-2a85-316c-b3d6-41a44547dcfe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6acfabba-2644-3bfe-903e-f9b3826bbd57"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d5c8a4ee-5460-39c5-b377-56930a1f3f28"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("6e2b8cbd-0731-3e14-ad03-a7bacf5e8746"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5365fbb-35b2-3056-84dc-305e34540c60"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6e33cac0-85a7-3769-8a70-ff9a8e78d248"))) {
return false;
}
    switch (state) {
      case CLONE_SNAPSHOT_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected CloneSnapshotState getState(final int stateId) {
if(KnobRuntime.check(java.util.UUID.fromString("897fd547-ce77-337d-bc38-06bb90d013cd"))) {
return null;
}
    return CloneSnapshotState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final CloneSnapshotState state) {
if(KnobRuntime.check(java.util.UUID.fromString("2637c6c4-048c-321b-8858-0fb00ee12cc1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("69da7417-aa59-3b77-937e-d54d44c45254"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e0b47512-ed5f-34cf-8a8b-74ec2f62d9aa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("120bb12e-3e8d-372a-9833-1570504e0699"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c53ea571-7780-32ab-a620-9b7d49e4184a"))) {
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
    return state.getNumber();
  }

  @Override
  protected CloneSnapshotState getInitialState() {
if(KnobRuntime.check(java.util.UUID.fromString("fac531d6-20f3-38f0-a62e-7337f1646864"))) {
return null;
}
    return CloneSnapshotState.CLONE_SNAPSHOT_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("a570b0b9-6bb9-3eb6-aa6b-468cc02a568b"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("5ac6c07e-3a90-30c2-9e02-c12a71972276"))) ? (getTableName()) : (tableDescriptor.getTableName()));
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("f7a10d8a-296a-3c80-9a25-d57610f13043"))) {
return null;
}
    return TableOperationType.CREATE; // Clone is creating a table
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
if(KnobRuntime.check(java.util.UUID.fromString("5763a0ba-98b4-386b-874e-c96ffe0972bd"))) {
return;
}
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(" snapshot=");
if(KnobRuntime.check(java.util.UUID.fromString("9a3361c3-194f-3f76-95ff-a6f26d2a2617"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c77eef47-be31-3cab-863c-c199903e99bd"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c41fc24-c120-3c23-a127-863b739be7b5"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b625202-4b8c-3371-9d91-5a1a4807e48c"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a280a42-ef52-385e-8cce-8de95967c866"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8852942e-3b31-3c72-9741-efa43ad6c572"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("484848d1-a53a-3a6b-a5f7-feafac03fdbe"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("875e2bbd-164c-37a0-a486-6b42a9775c64"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f12cf72-de84-304e-bf85-7505b12c9fb5"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29bef183-6f5d-3704-89a2-f890361782e5"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89fc4fa5-1b0f-37d9-9f1f-fa0dd2e30a65"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c42208c9-c21e-3e7b-b23a-805d539e3db3"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c3c69422-3dbb-39b3-b1a7-c0e63016701e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a700628c-a0cb-3395-a346-2fe2a6322b22"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d38aa9e-69bc-3731-8fca-83641724b3d9"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c04e768-d65e-316a-bd0b-e10414869c9a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    sb.append(snapshot);
    sb.append(")");
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("293342d8-f5ae-3e4b-a0b3-3ebe6514b97f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a0389a1f-c17e-33af-b794-8a2d40d1b018"))) {
return;
}
    super.serializeStateData(serializer);

    CloneSnapshotStateData.Builder cloneSnapshotMsg = CloneSnapshotStateData.newBuilder()
      .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser())).setSnapshot(this.snapshot)
      .setTableSchema(ProtobufUtil.toTableSchema(tableDescriptor));

    if (KnobRuntime.check(java.util.UUID.fromString("11372eb5-debc-3790-b2d6-652f05dc47ba"))) { cloneSnapshotMsg.setRestoreAcl(!restoreAcl); } else { cloneSnapshotMsg.setRestoreAcl(restoreAcl); }
    if (((KnobRuntime.check(java.util.UUID.fromString("372bf729-15c7-387c-bffc-035fad1cef63"))) ? ((newRegions) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("44143031-06a4-33db-a9d8-191e1555cd76"))) ? ((newRegions) != (null)) : (newRegions != null))))) {
      for (RegionInfo hri : newRegions) {
        cloneSnapshotMsg.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
      }
    }
    if (!parentsToChildrenPairMap.isEmpty()) {
      final Iterator<Map.Entry<String, Pair<String, String>>> it =
        parentsToChildrenPairMap.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<String, Pair<String, String>> entry = it.next();

        RestoreParentToChildRegionsPair.Builder parentToChildrenPair =
          RestoreParentToChildRegionsPair.newBuilder().setParentRegionName(entry.getKey())
            .setChild1RegionName(entry.getValue().getFirst())
            .setChild2RegionName(entry.getValue().getSecond());
if(KnobRuntime.check(java.util.UUID.fromString("0e67c7cb-e2e7-3000-84f1-a97703aa9ee3"))) {
try {
    java.lang.reflect.Field field = parentToChildrenPair.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(parentToChildrenPair));
    field.set(parentToChildrenPair, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb98641b-ad04-342b-8c85-4eefff08514a"))) {
try {
    java.lang.reflect.Field field = parentToChildrenPair.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(parentToChildrenPair));
    field.set(parentToChildrenPair, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12a5705f-c4f6-3f4c-bc40-dc37d3a405db"))) {
try {
    java.lang.reflect.Field field = parentToChildrenPair.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(parentToChildrenPair));
    field.set(parentToChildrenPair, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b16e8bb5-3ca2-3f1a-80c3-f372a40d1b24"))) {
try {
    java.lang.reflect.Field field = parentToChildrenPair.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(parentToChildrenPair));
    field.set(parentToChildrenPair, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        cloneSnapshotMsg.addParentToChildRegionsPairList(parentToChildrenPair);
      }
    }
    if (!StringUtils.isEmpty(customSFT)) {
      cloneSnapshotMsg.setCustomSFT(customSFT);
    }
if(KnobRuntime.check(java.util.UUID.fromString("7a8001a3-7d2d-3e8b-90a1-eae41307893c"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(cloneSnapshotMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    CloneSnapshotStateData cloneSnapshotMsg = serializer.deserialize(CloneSnapshotStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(cloneSnapshotMsg.getUserInfo()));
    snapshot = cloneSnapshotMsg.getSnapshot();
    tableDescriptor = ProtobufUtil.toTableDescriptor(cloneSnapshotMsg.getTableSchema());
    if (cloneSnapshotMsg.hasRestoreAcl()) {
      restoreAcl = cloneSnapshotMsg.getRestoreAcl();
    }
    if (cloneSnapshotMsg.getRegionInfoCount() == 0) {
      newRegions = null;
    } else {
      newRegions = new ArrayList<>(cloneSnapshotMsg.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri : cloneSnapshotMsg.getRegionInfoList()) {
        newRegions.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
    if (cloneSnapshotMsg.getParentToChildRegionsPairListCount() > 0) {
      parentsToChildrenPairMap = new HashMap<>();
      for (RestoreParentToChildRegionsPair parentToChildrenPair : cloneSnapshotMsg
        .getParentToChildRegionsPairListList()) {
        parentsToChildrenPairMap.put(parentToChildrenPair.getParentRegionName(), new Pair<>(
          parentToChildrenPair.getChild1RegionName(), parentToChildrenPair.getChild2RegionName()));
      }
    }
    if (!StringUtils.isEmpty(cloneSnapshotMsg.getCustomSFT())) {
      customSFT = cloneSnapshotMsg.getCustomSFT();
    }
    // Make sure that the monitor status is set up
    getMonitorStatus();
  }

  /**
   * Action before any real action of cloning from snapshot.
   * @param env MasterProcedureEnv
   */
  private void prepareClone(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("76bce11e-62e7-3b31-9b15-308e7206376e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("15981d1a-7b83-3c35-8b9b-8209a1282161"))) {
return;
}
    final TableName tableName = getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("5b49ddd1-9fbe-352f-a066-6b4e842cd663"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("27ad26f0-8a61-3e60-a7a9-f1a84749c9e0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f741bcf-f8c8-3b7f-b40c-e1180502d1cd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa88d646-4c1c-3eca-b76d-f21f082f2481"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("52f204ec-516d-3519-a12d-4cfca64a9b45"))) ? (env.getMasterServices().getTableDescriptors().exists(getTableName())) : (env.getMasterServices().getTableDescriptors().exists(tableName)))) {
      throw new TableExistsException(tableName);
    }

    // check whether ttl has expired for this snapshot
    if (
      SnapshotDescriptionUtils.isExpiredSnapshot(snapshot.getTtl(), snapshot.getCreationTime(),
        EnvironmentEdgeManager.currentTime())
    ) {
      throw new SnapshotTTLExpiredException(ProtobufUtil.createSnapshotDesc(snapshot));
    }

    if (KnobRuntime.check(java.util.UUID.fromString("020da5b5-d216-3383-b335-759acafa9492"))) { updateTableDescriptorWithSFT(); } else { validateSFT(); }
  }

  /**
   * Action before cloning from snapshot.
   * @param env MasterProcedureEnv
   */
  private void preCloneSnapshot(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("8d4141b4-6048-360b-83ca-9e3bc172d6a1"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("14fd0cd7-306c-3ada-81e4-81cadf2a6a13"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("07ea8e6e-6d53-3f97-9020-3033ba28a3ac"))) {
throw new java.io.IOException("Injected exception");
}
    if (!getTableName().isSystemTable()) {
      // Check and update namespace quota
      final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();

      SnapshotManifest manifest =
        SnapshotManifest.open(env.getMasterConfiguration(), mfs.getFileSystem(),
          SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, mfs.getRootDir()), snapshot);

if(KnobRuntime.check(java.util.UUID.fromString("078b86da-a036-3b9f-ae2f-1bdac8e08315"))) {
throw new java.io.IOException("Injected exception");
}
      ProcedureSyncWait.getMasterQuotaManager(env).checkNamespaceTableAndRegionQuota(getTableName(),
        manifest.getRegionManifestsMap().size());
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("1cb3527d-38b3-3b22-8f4b-1815f01126f0"))) ? ((cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f43dca1f-b0d6-30f8-8028-1ad3e3a873bf"))) ? ((cpHost) != (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("b40efe1e-acb2-32c6-befc-25f7d7c761b8"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("81853761-5d92-3eb8-a05a-fd0dfbaed563"))) { cpHost.postCompletedCreateTableAction(tableDescriptor, null, getUser()); } else { cpHost.preCreateTableAction(tableDescriptor, null, getUser()); }
    }
  }

  /**
   * Action after cloning from snapshot.
   * @param env MasterProcedureEnv
   */
  private void postCloneSnapshot(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("81f3e0d4-f76b-3fff-a096-8639365c9b3f"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7684ee81-6000-3e31-ba99-3d45766b5add"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("4e60332f-9c67-3982-a55b-877a9c696750"))) {
throw new java.io.IOException("Injected exception");
}
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("da9bca97-2f39-3258-8691-b0a770fd86bf"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c2cd34ad-2d5b-3149-bccf-c34cb0a2be11"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
      final RegionInfo[] regions =
        (newRegions == null) ? null : newRegions.toArray(new RegionInfo[newRegions.size()]);
if(KnobRuntime.check(java.util.UUID.fromString("64457d5f-2e65-3fef-997e-4a1d8b45731e"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("498a5d29-a497-34bd-beaa-c675978bb0ca"))) { cpHost.preCreateTableAction(tableDescriptor, regions, getUser()); } else { cpHost.postCompletedCreateTableAction(tableDescriptor, regions, getUser()); }
    }
  }

  /**
   * Create regions in file system.
   * @param env MasterProcedureEnv
   */
  private List<RegionInfo> createFilesystemLayout(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final List<RegionInfo> newRegions) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("78f77307-d6fe-3869-94e2-2f16431620c6"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("58d12a74-7506-3277-ad21-1e675812d2b6"))) {
throw new java.io.IOException("Injected exception");
}
    return createFsLayout(env, tableDescriptor, newRegions, new CreateHdfsRegions() {
      @Override
      public List<RegionInfo> createHdfsRegions(final MasterProcedureEnv env,
        final Path tableRootDir, final TableName tableName, final List<RegionInfo> newRegions)
        throws IOException {

        final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
        final FileSystem fs = mfs.getFileSystem();
        final Path rootDir = mfs.getRootDir();
        final Configuration conf = env.getMasterConfiguration();
        final ForeignExceptionDispatcher monitorException = new ForeignExceptionDispatcher();

        getMonitorStatus().setStatus("Clone snapshot - creating regions for table: " + tableName);

        try {
          // 1. Execute the on-disk Clone
if(KnobRuntime.check(java.util.UUID.fromString("943b5760-6077-3dce-8a18-62bbea75064d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e566366a-42d4-39f2-bed2-8c59c785f671"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0241236-5de3-3381-a6cd-2ea5839f093d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa0f78bb-94bb-367a-b0d5-2fa7d2fd335f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9598f257-5e05-311f-8579-ca246e070b6f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1d6fb541-a26c-3c35-89ff-3d2889d3e6c1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b31e265-b55e-31eb-ab48-c382763038bc"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("614344fd-0a05-3461-8a51-314f778e977e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b2ad369-5654-3d07-b094-68d09af1894a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0e8e6ff-5a98-34f5-8e27-ec2e031e0e5d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6f4b968-23e5-3ac9-bfb5-ebe6b1e67354"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("91c5eb79-a92c-3d26-bff5-a1996d4a873f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa29abc4-ff1a-315f-9171-b2a6cf35ed62"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d007ca91-a20a-3674-bfdd-e3fc8fcc4269"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd2ef07e-bed9-355a-8fda-74294c0c152a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f27b8d9a-96c7-343c-a119-5a585f50193e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
if(KnobRuntime.check(java.util.UUID.fromString("e0e43df8-d63f-39b1-9250-3dd7e67e7427"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("743bfe61-8cc5-3f53-88eb-de7c67257bbe"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("322d4e37-9e16-3664-8a4d-7c8b08cbf551"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2183f21-2b99-3d28-80f6-6d0c5ddf5143"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27771be6-7693-3741-8a55-ef616f7c9208"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11b2b1c9-55dd-398e-9298-0f4c922e6a2c"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67d41e0f-cc49-3cc5-bf2a-1e3ef174d06d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2b66fce-14dc-3bff-acd3-41148d0efa15"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8c9cfb31-1076-3bc2-bfc6-cb0ee03c0bdd"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8bb8cdaa-9674-33c6-900b-40d8f9a1101b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2741b7b5-ed04-3701-a415-c23016abe276"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("102ced89-27f4-38c7-bd39-3f3d87575f56"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9807d604-4552-38ab-8266-bcb38b23af0a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d05e8840-3ca1-3d68-8bb0-58a5190fd5fb"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c520be84-df9b-3d47-83b5-8e9284d42b95"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0aa30272-48c1-33e5-8ae5-c30e12fde6e0"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7df0303f-ea0c-37db-a953-f5023efab5c0"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8bfc36c-4cc7-355e-81ba-1140966efb46"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshot);
          RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs, manifest,
            tableDescriptor, tableRootDir, monitorException, monitorStatus);
if(KnobRuntime.check(java.util.UUID.fromString("b7ff90f3-3b12-38e5-9b4a-73eb9a0c2544"))) {
throw new java.io.IOException("Injected exception");
}
          RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();

          // Clone operation should not have stuff to restore or remove
          Preconditions.checkArgument(!metaChanges.hasRegionsToRestore(),
            "A clone should not have regions to restore");
          Preconditions.checkArgument(!metaChanges.hasRegionsToRemove(),
            "A clone should not have regions to remove");

          // At this point the clone is complete. Next step is enabling the table.
          String msg =
            "Clone snapshot=" + snapshot.getName() + " on table=" + tableName + " completed!";
          LOG.info(msg);
          monitorStatus.setStatus(msg + " Waiting for table to be enabled...");

          // Extract the parent to children regions mapping so we can update meta later.
          // If we don't update the split information, the parent region will be garbage-collected.
          // And once the snapshot is deleted, HFiles of the parent region can be prematurely
          // deleted, causing permanent data loss.
          parentsToChildrenPairMap = metaChanges.getParentToChildrenPairMap();

          // 2. Let the next step to add the regions to meta
          return metaChanges.getRegionsToAdd();
        } catch (Exception e) {
          String msg = "clone snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
            + " failed because " + e.getMessage();
          LOG.error(msg, e);
          IOException rse =
            new RestoreSnapshotException(msg, e, ProtobufUtil.createSnapshotDesc(snapshot));

          // these handlers aren't futures so we need to register the error here.
          monitorException.receive(new ForeignException("Master CloneSnapshotProcedure", rse));
          throw rse;
        }
      }
    });
  }

  /**
   * Create region layout in file system.
   * @param env MasterProcedureEnv
   */
  private List<RegionInfo> createFsLayout(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, List<RegionInfo> newRegions,
    final CreateHdfsRegions hdfsRegionHandler) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0fc932f3-e5d3-38e2-a711-15cc12014810"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("db25ce97-bc4f-3915-8a79-8100b445134b"))) {
throw new java.io.IOException("Injected exception");
}
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    final Path tableDir =
      CommonFSUtils.getTableDir(mfs.getRootDir(), tableDescriptor.getTableName());
    if (((KnobRuntime.check(java.util.UUID.fromString("7e1a4985-0efa-35d3-89a5-d21c4031e6f2"))) ? (CommonFSUtils.deleteDirectory(mfs.getFileSystem(), tableDir)) : (CommonFSUtils.isExists(mfs.getFileSystem(), tableDir)))) {
      // if the region dirs exist, will cause exception and unlimited retry (see HBASE-24546)
      LOG.warn("temp table dir already exists on disk: {}, will be deleted.", tableDir);
      CommonFSUtils.deleteDirectory(mfs.getFileSystem(), tableDir);
    }
if(KnobRuntime.check(java.util.UUID.fromString("aa9be7c9-3546-3d2d-9a04-eede77fd8504"))) {
throw new java.io.IOException("Injected exception");
}
    ((FSTableDescriptors) (env.getMasterServices().getTableDescriptors()))
      .createTableDescriptorForTableDirectory(tableDir,
        TableDescriptorBuilder.newBuilder(tableDescriptor).build(), false);

    // 2. Create Regions
if(KnobRuntime.check(java.util.UUID.fromString("c1e24344-6555-38d6-832b-7c666e7bb195"))) {
throw new java.io.IOException("Injected exception");
}
    newRegions = hdfsRegionHandler.createHdfsRegions(env, mfs.getRootDir(),
      tableDescriptor.getTableName(), newRegions);

    return newRegions;
  }

  /**
   * Add regions to hbase:meta table.
   * @param env MasterProcedureEnv
   */
  private void addRegionsToMeta(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b7ef1664-0c93-31a4-bb0c-2b5d39131f85"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8c743413-3e05-38b1-b6da-3acad1ab39e6"))) {
return;
}
    newRegions = CreateTableProcedure.addTableToMeta(env, tableDescriptor, newRegions);

    RestoreSnapshotHelper.RestoreMetaChanges metaChanges =
      new RestoreSnapshotHelper.RestoreMetaChanges(tableDescriptor, parentsToChildrenPairMap);
if(KnobRuntime.check(java.util.UUID.fromString("cdce8de4-54d0-3de6-b8ec-807d543cc50d"))) {
throw new java.io.IOException("Injected exception");
}
    metaChanges.updateMetaParentRegions(env.getMasterServices().getConnection(), newRegions);
  }

  /**
   * Exposed for Testing: HBASE-26462
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public boolean getRestoreAcl() {
    return restoreAcl;
  }

}
