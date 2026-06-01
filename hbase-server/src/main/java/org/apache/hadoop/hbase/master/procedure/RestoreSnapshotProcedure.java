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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.fs.ErasureCodingUtils;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RestoreParentToChildRegionsPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RestoreSnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RestoreSnapshotStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class RestoreSnapshotProcedure
  extends AbstractStateMachineTableProcedure<RestoreSnapshotState> {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreSnapshotProcedure.class);

  private TableDescriptor oldTableDescriptor;
  private TableDescriptor modifiedTableDescriptor;
  private List<RegionInfo> regionsToRestore = null;
  private List<RegionInfo> regionsToRemove = null;
  private List<RegionInfo> regionsToAdd = null;
  private Map<String, Pair<String, String>> parentsToChildrenPairMap = new HashMap<>();

  private SnapshotDescription snapshot;
  private boolean restoreAcl;

  // Monitor
  private MonitoredTask monitorStatus = null;

  /**
   * Constructor (for failover)
   */
  public RestoreSnapshotProcedure() {
  }

  public RestoreSnapshotProcedure(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final SnapshotDescription snapshot)
    throws HBaseIOException {
    this(env, tableDescriptor, snapshot, false);
  }

  public RestoreSnapshotProcedure(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final SnapshotDescription snapshot,
    final boolean restoreAcl) throws HBaseIOException {
    this(env, tableDescriptor, tableDescriptor, snapshot, restoreAcl);
  }

  /**
   * Constructor
   * @param env                     MasterProcedureEnv
   * @param modifiedTableDescriptor the table to operate on
   * @param snapshot                snapshot to restore from
   */
  public RestoreSnapshotProcedure(final MasterProcedureEnv env,
    final TableDescriptor oldTableDescriptor, final TableDescriptor modifiedTableDescriptor,
    final SnapshotDescription snapshot, final boolean restoreAcl) throws HBaseIOException {
    super(env);
    this.oldTableDescriptor = oldTableDescriptor;
    // This is the new schema we are going to write out as this modification.
    this.modifiedTableDescriptor = modifiedTableDescriptor;
    preflightChecks(env, null/* Table can be online when restore is called? */);
    // Snapshot information
    this.snapshot = snapshot;
    this.restoreAcl = restoreAcl;

    // Monitor
    getMonitorStatus();
  }

  /**
   * Set up monitor status if it is not created.
   */
  private MonitoredTask getMonitorStatus() {
    if (monitorStatus == null) {
      monitorStatus = TaskMonitor.get().createStatus(
        "Restoring  snapshot '" + snapshot.getName() + "' to table " + getTableName());
    }
    return monitorStatus;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final RestoreSnapshotState state)
    throws InterruptedException {
    LOG.trace("{} execute state={}", this, state);

    // Make sure that the monitor status is set up
    getMonitorStatus();

    try {
      switch (state) {
        case RESTORE_SNAPSHOT_PRE_OPERATION:
          // Verify if we can restore the table
          prepareRestore(env);
          setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_UPDATE_TABLE_DESCRIPTOR);
          break;
        case RESTORE_SNAPSHOT_UPDATE_TABLE_DESCRIPTOR:
          updateTableDescriptor(env);
          // for restore, table dir already exists. sync EC if necessary before doing the real
          // restore. this may be useful in certain restore scenarios where a user is explicitly
          // trying to disable EC for some reason as part of the restore.
          if (ErasureCodingUtils.needsSync(oldTableDescriptor, modifiedTableDescriptor)) {
            setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_SYNC_ERASURE_CODING_POLICY);
          } else {
            setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_WRITE_FS_LAYOUT);
          }
          break;
        case RESTORE_SNAPSHOT_SYNC_ERASURE_CODING_POLICY:
          ErasureCodingUtils.sync(env.getMasterFileSystem().getFileSystem(),
            env.getMasterFileSystem().getRootDir(), modifiedTableDescriptor);
          setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_WRITE_FS_LAYOUT);
          break;
        case RESTORE_SNAPSHOT_WRITE_FS_LAYOUT:
          restoreSnapshot(env);
          setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_UPDATE_META);
          break;
        case RESTORE_SNAPSHOT_UPDATE_META:
          updateMETA(env);
          setNextState(RestoreSnapshotState.RESTORE_SNAPSHOT_RESTORE_ACL);
          break;
        case RESTORE_SNAPSHOT_RESTORE_ACL:
          restoreSnapshotAcl(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("e0d6835f-4002-3eea-b714-f62d4a26e382"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c7c8d24f-b4ed-3b0f-a792-95cc10513862"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6197633b-a94b-389e-b2fd-17ca1f7b2518"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5d89b47-a851-3e64-881b-cb23165d17f9"))) {
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
      if (isRollbackSupported(state)) {
        setFailure("master-restore-snapshot", e);
      } else {
        LOG.warn("Retriable error trying to restore snapshot=" + snapshot.getName() + " to table="
          + getTableName() + " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final RestoreSnapshotState state)
    throws IOException {
    if (state == RestoreSnapshotState.RESTORE_SNAPSHOT_PRE_OPERATION) {
      // nothing to rollback
      return;
    }

    // The restore snapshot doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final RestoreSnapshotState state) {
    switch (state) {
      case RESTORE_SNAPSHOT_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected RestoreSnapshotState getState(final int stateId) {
    return RestoreSnapshotState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final RestoreSnapshotState state) {
    return state.getNumber();
  }

  @Override
  protected RestoreSnapshotState getInitialState() {
    return RestoreSnapshotState.RESTORE_SNAPSHOT_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
    return modifiedTableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT; // Restore is modifying a table
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    // TODO: We may be able to abort if the procedure is not started yet.
    return false;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(" snapshot=");
if(KnobRuntime.check(java.util.UUID.fromString("f2df053a-d8d9-3a62-a92c-971f92617523"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("de221464-4ac8-3673-8cda-d5ccb32280cb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2f86b1b7-e1a4-353c-aa38-6e62ec7b161b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f7021736-2af2-38bd-a2f7-2ab99d9cf875"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c4f94d09-2daf-3093-af44-d916dc5494dd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4efefcdd-5e5c-33bb-97fd-b1b25b81c0c9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36d99faf-e635-3b76-ba28-3118405791ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4b696125-35e2-3db6-851f-513ffd9d187c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d0e71b6c-1a08-383a-89bf-1ed40bd0aaaf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4f678294-3309-3376-8fe9-a869b4b1b7ae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fb191e04-2870-3871-a421-741d1999522a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6368e3c6-67eb-3b15-8141-c07ee6d14716"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9359e1d3-140d-3c18-9291-b27e4eb70e7a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f33a6b1-3673-3b44-a3d1-9bb0ac3c3ccd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d37c88f9-375f-330e-ba18-bfc88acad516"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("961ae26b-2a92-350f-8a1e-e8318167b690"))) {
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
    sb.append(snapshot);
    sb.append(")");
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    RestoreSnapshotStateData.Builder restoreSnapshotMsg = RestoreSnapshotStateData.newBuilder()
      .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser())).setSnapshot(this.snapshot)
      .setModifiedTableSchema(ProtobufUtil.toTableSchema(modifiedTableDescriptor))
      .setOldTableSchema(ProtobufUtil.toTableSchema(oldTableDescriptor));

    if (regionsToRestore != null) {
      for (RegionInfo hri : regionsToRestore) {
        if (KnobRuntime.check(java.util.UUID.fromString("77f39a8c-a914-3141-bfd7-721c6499a41d"))) { restoreSnapshotMsg.addRegionInfoForRemove(ProtobufUtil.toRegionInfo(hri)); } else if (KnobRuntime.check(java.util.UUID.fromString("86b2eb1a-0c27-3649-b0ad-da9b768b0c4e"))) { restoreSnapshotMsg.addRegionInfoForAdd(ProtobufUtil.toRegionInfo(hri)); } else { restoreSnapshotMsg.addRegionInfoForRestore(ProtobufUtil.toRegionInfo(hri)); }
      }
    }
    if (regionsToRemove != null) {
      for (RegionInfo hri : regionsToRemove) {
        if (KnobRuntime.check(java.util.UUID.fromString("bede3368-949c-3690-8b20-2e399e31c25a"))) { restoreSnapshotMsg.addRegionInfoForAdd(ProtobufUtil.toRegionInfo(hri)); } else if (KnobRuntime.check(java.util.UUID.fromString("50412fd8-5c79-3f45-81ac-cd5e611a0f8d"))) { restoreSnapshotMsg.addRegionInfoForRestore(ProtobufUtil.toRegionInfo(hri)); } else { restoreSnapshotMsg.addRegionInfoForRemove(ProtobufUtil.toRegionInfo(hri)); }
      }
    }
    if (regionsToAdd != null) {
      for (RegionInfo hri : regionsToAdd) {
        if (KnobRuntime.check(java.util.UUID.fromString("668dd157-8edc-309d-b210-ec4fc0796619"))) { restoreSnapshotMsg.addRegionInfoForRestore(ProtobufUtil.toRegionInfo(hri)); } else if (KnobRuntime.check(java.util.UUID.fromString("4cad07d9-1e69-36c0-8c59-b66d3cb2b46c"))) { restoreSnapshotMsg.addRegionInfoForRemove(ProtobufUtil.toRegionInfo(hri)); } else { restoreSnapshotMsg.addRegionInfoForAdd(ProtobufUtil.toRegionInfo(hri)); }
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
        restoreSnapshotMsg.addParentToChildRegionsPairList(parentToChildrenPair);
      }
    }
    restoreSnapshotMsg.setRestoreAcl(restoreAcl);
if(KnobRuntime.check(java.util.UUID.fromString("52859758-132c-32e1-add4-8304987883d3"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(restoreSnapshotMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    RestoreSnapshotStateData restoreSnapshotMsg =
      serializer.deserialize(RestoreSnapshotStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(restoreSnapshotMsg.getUserInfo()));
    snapshot = restoreSnapshotMsg.getSnapshot();
    oldTableDescriptor = ProtobufUtil.toTableDescriptor(restoreSnapshotMsg.getOldTableSchema());
    modifiedTableDescriptor =
      ProtobufUtil.toTableDescriptor(restoreSnapshotMsg.getModifiedTableSchema());

    if (restoreSnapshotMsg.getRegionInfoForRestoreCount() == 0) {
      regionsToRestore = null;
    } else {
      regionsToRestore = new ArrayList<>(restoreSnapshotMsg.getRegionInfoForRestoreCount());
      for (HBaseProtos.RegionInfo hri : restoreSnapshotMsg.getRegionInfoForRestoreList()) {
        regionsToRestore.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
    if (restoreSnapshotMsg.getRegionInfoForRemoveCount() == 0) {
      regionsToRemove = null;
    } else {
      regionsToRemove = new ArrayList<>(restoreSnapshotMsg.getRegionInfoForRemoveCount());
      for (HBaseProtos.RegionInfo hri : restoreSnapshotMsg.getRegionInfoForRemoveList()) {
        regionsToRemove.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
    if (restoreSnapshotMsg.getRegionInfoForAddCount() == 0) {
      regionsToAdd = null;
    } else {
      regionsToAdd = new ArrayList<>(restoreSnapshotMsg.getRegionInfoForAddCount());
      for (HBaseProtos.RegionInfo hri : restoreSnapshotMsg.getRegionInfoForAddList()) {
        regionsToAdd.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
    if (restoreSnapshotMsg.getParentToChildRegionsPairListCount() > 0) {
      for (RestoreParentToChildRegionsPair parentToChildrenPair : restoreSnapshotMsg
        .getParentToChildRegionsPairListList()) {
        parentsToChildrenPairMap.put(parentToChildrenPair.getParentRegionName(), new Pair<>(
          parentToChildrenPair.getChild1RegionName(), parentToChildrenPair.getChild2RegionName()));
      }
    }
    if (restoreSnapshotMsg.hasRestoreAcl()) {
      restoreAcl = restoreSnapshotMsg.getRestoreAcl();
    }
  }

  /**
   * Action before any real action of restoring from snapshot.
   * @param env MasterProcedureEnv
   */
  private void prepareRestore(final MasterProcedureEnv env) throws IOException {
    final TableName tableName = getTableName();
    // Checks whether the table exists
    if (!env.getMasterServices().getTableDescriptors().exists(tableName)) {
      throw new TableNotFoundException(tableName);
    }

    // check whether ttl has expired for this snapshot
    if (
      SnapshotDescriptionUtils.isExpiredSnapshot(snapshot.getTtl(), snapshot.getCreationTime(),
        EnvironmentEdgeManager.currentTime())
    ) {
      throw new SnapshotTTLExpiredException(ProtobufUtil.createSnapshotDesc(snapshot));
    }

    // Check whether table is disabled.
    env.getMasterServices().checkTableModifiable(tableName);

    // Check that we have at least 1 CF
    if (modifiedTableDescriptor.getColumnFamilyCount() == 0) {
      throw new DoNotRetryIOException(
        "Table " + getTableName().toString() + " should have at least one column family.");
    }

    if (!getTableName().isSystemTable()) {
      // Table already exist. Check and update the region quota for this table namespace.
      final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
      SnapshotManifest manifest =
        SnapshotManifest.open(env.getMasterConfiguration(), mfs.getFileSystem(),
          SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, mfs.getRootDir()), snapshot);
      int snapshotRegionCount = manifest.getRegionManifestsMap().size();
      int tableRegionCount =
        ProcedureSyncWait.getMasterQuotaManager(env).getRegionCountOfTable(tableName);

      if (snapshotRegionCount > 0 && tableRegionCount != snapshotRegionCount) {
if(KnobRuntime.check(java.util.UUID.fromString("fbe73140-b039-3187-a271-2e0bac040b98"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aede12c3-b699-3965-a628-bdeca1009f72"))) {
snapshotRegionCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("48d8d921-1f54-3e3f-bcc7-bb6d1bf1d994"))) {
snapshotRegionCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a059580f-f461-3c5d-af5a-bfe7e93fa137"))) {
snapshotRegionCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b2100f74-c874-36c1-b9e6-26d19560a29e"))) {
snapshotRegionCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e3ae59eb-b959-3628-ab94-ec9eeb9193c7"))) {
snapshotRegionCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9a61906d-28f7-3834-a3d3-d7a9c8be3f2f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02877093-68a6-32e0-a131-86c17f1bb298"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3a397a37-e685-3467-a9b7-6fb32a2a4559"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("535fc16d-9548-3581-a2da-5393b759bf5d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e8eee4eb-79f3-35ad-b5ee-b93bbfde5deb"))) {
snapshotRegionCount += 1;
}
        ProcedureSyncWait.getMasterQuotaManager(env).checkAndUpdateNamespaceRegionQuota(tableName,
          snapshotRegionCount);
      }
    }
  }

  /**
   * Update descriptor
   * @param env MasterProcedureEnv
   **/
  private void updateTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().update(modifiedTableDescriptor);
  }

  /**
   * Execute the on-disk Restore
   * @param env MasterProcedureEnv
   **/
  private void restoreSnapshot(final MasterProcedureEnv env) throws IOException {
    MasterFileSystem fileSystemManager = env.getMasterServices().getMasterFileSystem();
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    final ForeignExceptionDispatcher monitorException = new ForeignExceptionDispatcher();
    final Configuration conf = new Configuration(env.getMasterConfiguration());

    LOG.info("Starting restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot));
    try {
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
if(KnobRuntime.check(java.util.UUID.fromString("5c9a4604-074d-3a79-a08b-1ce98be831a5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7a6ea24c-c7c2-3fcb-835f-d6c8d3dd3436"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("54ce8521-baae-367e-967b-1f40662d5a7e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("df4ff241-203f-3a12-9606-ef3a32136eaf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9a610716-c159-37bc-974e-98c91e9fa76b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("96d3ec1c-fef0-3371-be91-2cf0e60e9e47"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("895b544a-33e9-3779-8f59-7413fda10e1e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a340f0a9-852a-3c1a-8e95-542825a77aaa"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c5f97fc4-0183-3464-83dd-6fcdb15f52c2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8847bde4-1bda-3c44-90af-97a3498cb5b7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b01213d9-8e65-3690-8427-fcfaad03dafb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9147c4d9-0506-311e-8961-6b3180aec41d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39285df8-ebd3-3378-a024-63fdd5c6d0ea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("010f08ec-42c0-334e-9683-c22279e6ec56"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f87e481-d6d4-33fb-9da1-4e04553e3065"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f32581f-7034-3dc4-a93e-17a4f3a08354"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5e3960d1-5bee-30e9-9f73-fb72f4a6e159"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("07713194-5670-3c6a-af98-a06fef870db2"))) {
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
      SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshot);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs, manifest,
        modifiedTableDescriptor, rootDir, monitorException, getMonitorStatus());

      RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();
      regionsToRestore = metaChanges.getRegionsToRestore();
      regionsToRemove = metaChanges.getRegionsToRemove();
      regionsToAdd = metaChanges.getRegionsToAdd();
      parentsToChildrenPairMap = metaChanges.getParentToChildrenPairMap();
    } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("e02b9a84-f78e-346e-8d61-7896a65148ca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef88c54d-c709-320d-b7df-8be4358b0a04"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b39c99cd-925d-3bfb-9198-19b2a710c78e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("57e8309a-0c9a-3d3d-95a8-15c6e496c724"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("417f3367-b168-34d2-b83b-c20a6304f1f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("68ab207f-2203-390c-b537-063c494b1d71"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aceddf41-0a6a-3dbb-b500-0f058ce4b6a5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f300c984-d215-3015-8b4f-25b70a85a29a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6830b90f-37e8-3b82-b1e1-8490b92592de"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("44b27322-7822-3e98-8b68-50b33febc6aa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9f22e26b-bb55-3bae-b06f-72d94be92153"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e33447b9-792e-3df8-965b-cf3fedb30592"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba2ddc23-1e52-3cf2-ae17-e86ae1452654"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3c33d870-5345-377a-80a3-e72bdb73c528"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ef55e24-f293-3c29-80b5-c92ae1b7db19"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c06fb754-d53f-35aa-b5e4-c3dfd65c2965"))) {
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
      String msg = "restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
        + " failed in on-disk restore. Try re-running the restore command.";
      LOG.error(msg, e);
      monitorException
        .receive(new ForeignException(env.getMasterServices().getServerName().toString(), e));
      throw new IOException(msg, e);
    }
  }

  /**
   * Apply changes to hbase:meta
   * @param env MasterProcedureEnv
   **/
  private void updateMETA(final MasterProcedureEnv env) throws IOException {
    try {
      Connection conn = env.getMasterServices().getConnection();
      int regionReplication = modifiedTableDescriptor.getRegionReplication();

      // 1. Prepare to restore
      getMonitorStatus().setStatus("Preparing to restore each region");

      // 2. Applies changes to hbase:meta and in-memory states
      // (2.1). Removes the current set of regions from META and in-memory states
      //
      // By removing also the regions to restore (the ones present both in the snapshot
      // and in the current state) we ensure that no extra fields are present in META
      // e.g. with a simple add addRegionToMeta() the splitA and splitB attributes
      // not overwritten/removed, so you end up with old informations
      // that are not correct after the restore.
      if (regionsToRemove != null) {
if(KnobRuntime.check(java.util.UUID.fromString("0d82f9a5-18ed-3f51-b749-ed850af08479"))) {
throw new java.io.IOException("Injected exception");
}
        MetaTableAccessor.deleteRegionInfos(conn, regionsToRemove);
if(KnobRuntime.check(java.util.UUID.fromString("c173d815-a6d9-3d1a-9efc-54f24877efe4"))) {
regionReplication = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0ec112f1-e16f-3478-a4b1-e6b378e2d5f6"))) {
regionReplication *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("04083f92-a5c6-3a6b-b7ce-21d1f837cf11"))) {
regionReplication += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("681f3e4c-850a-3871-9b9a-57369108e354"))) {
regionReplication = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6448059f-223b-3670-b884-6ccc5cc00133"))) {
regionReplication -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7dad6a6b-e46c-3336-8e84-b0fdea3f3d0e"))) {
regionReplication /= 2;
}
        if (KnobRuntime.check(java.util.UUID.fromString("0c23f737-3179-33c2-a814-73770a737a78"))) { addRegionsToInMemoryStates(regionsToRemove, env, regionReplication); } else { deleteRegionsFromInMemoryStates(regionsToRemove, env, regionReplication); }
      }

      // (2.2). Add the new set of regions to META and in-memory states
      //
      // At this point the old regions are no longer present in META.
      // and the set of regions present in the snapshot will be written to META.
      // All the information in hbase:meta are coming from the .regioninfo of each region present
      // in the snapshot folder.
      if (regionsToAdd != null) {
if(KnobRuntime.check(java.util.UUID.fromString("b8ecb1cd-74f9-387c-a899-4e1d244d3549"))) {
regionReplication /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("66065dd1-6dc1-3289-9224-dac783e5a41d"))) {
regionReplication += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("84ae759d-becd-3c88-bd41-64da4dbab7a9"))) {
regionReplication *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa29a06d-a4ba-3a66-b436-dafb2a51a8f6"))) {
regionReplication -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4913ad2a-2d90-3ec5-a88d-266981c9f189"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bbadbe53-46ec-3ce2-bd99-f7ea1d168c87"))) {
regionReplication = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f73ad795-9795-3f6e-9a75-b93a7e489df3"))) {
regionReplication = 0;
}
        if (KnobRuntime.check(java.util.UUID.fromString("dbb057ca-1393-37f7-98ba-402055c42ca9"))) { MetaTableAccessor.overwriteRegions(conn, regionsToAdd, regionReplication); } else { MetaTableAccessor.addRegionsToMeta(conn, regionsToAdd, regionReplication); }
if(KnobRuntime.check(java.util.UUID.fromString("91aa1617-9e55-3a55-84b1-ca616fa727fd"))) {
regionReplication *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3103d805-2729-300a-80f7-cdc4e8daa69b"))) {
regionReplication += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f3fd4b0-803e-3493-afb5-089362b823bb"))) {
regionReplication = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9438dda6-311f-372a-bc1b-65b3f33b86a6"))) {
regionReplication -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e345610b-7551-3548-8a45-b4716f40dc32"))) {
regionReplication = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c10fb53d-cc1a-3258-8ac0-d1ee2c78ff91"))) {
regionReplication /= 2;
}
        if (KnobRuntime.check(java.util.UUID.fromString("7c7592ef-6a41-3cf8-a3cd-f49b7d565408"))) { deleteRegionsFromInMemoryStates(regionsToAdd, env, regionReplication); } else { addRegionsToInMemoryStates(regionsToAdd, env, regionReplication); }
      }

      if (regionsToRestore != null) {
if(KnobRuntime.check(java.util.UUID.fromString("b0e5380d-8722-3b65-a798-d7289c3ef4bb"))) {
regionReplication *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7eb2e890-b0a5-354b-bd2a-a1760d57bc1f"))) {
regionReplication = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("03a5b8b7-e03a-3f48-8d86-07e286ead99d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("dd13e3e3-f14e-3abf-9cf4-88d957a42b15"))) {
regionReplication /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8efb6b23-6796-3d94-8bab-90764d0ffa27"))) {
regionReplication += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e6ad7735-2734-325e-ae7b-b69885fb0060"))) {
regionReplication = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bd560754-cbbd-3b9f-aead-dbc5ebddf782"))) {
regionReplication -= 1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("99c0a6eb-07e0-3ec0-99d7-e1945e424d0c"))) { MetaTableAccessor.addRegionsToMeta(conn, regionsToRestore, regionReplication); } else { MetaTableAccessor.overwriteRegions(conn, regionsToRestore, regionReplication); }

        deleteRegionsFromInMemoryStates(regionsToRestore, env, regionReplication);
if(KnobRuntime.check(java.util.UUID.fromString("6f7fa79b-ff20-37ce-a241-fa997225e9c2"))) {
regionReplication = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8801fa3e-ccd6-354a-a57b-9feaa50f919d"))) {
regionReplication /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e281d307-13cc-3fe9-8ae4-b0475c3e1521"))) {
regionReplication = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f285377e-d8af-3e00-8365-82cccd38b555"))) {
regionReplication += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bfa126aa-eb9a-3f36-9a6b-991effd70ed7"))) {
regionReplication *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab5c2e23-0944-335a-830e-9032f5450fb5"))) {
regionReplication -= 1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("3d88e1d2-8e8d-3504-9d54-03f11d7bf11a"))) { deleteRegionsFromInMemoryStates(regionsToRestore, env, regionReplication); } else { addRegionsToInMemoryStates(regionsToRestore, env, regionReplication); }
      }

      RestoreSnapshotHelper.RestoreMetaChanges metaChanges =
        new RestoreSnapshotHelper.RestoreMetaChanges(modifiedTableDescriptor,
          parentsToChildrenPairMap);
      metaChanges.updateMetaParentRegions(conn, regionsToAdd);

      // At this point the restore is complete.
      LOG.info("Restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
        + " on table=" + getTableName() + " completed!");
    } catch (IOException e) {
      final ForeignExceptionDispatcher monitorException = new ForeignExceptionDispatcher();
      String msg = "restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
        + " failed in meta update. Try re-running the restore command.";
      LOG.error(msg, e);
      monitorException
        .receive(new ForeignException(env.getMasterServices().getServerName().toString(), e));
      throw new IOException(msg, e);
    }

    monitorStatus.markComplete("Restore snapshot '" + snapshot.getName() + "'!");
    MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
    metricsSnapshot
      .addSnapshotRestore(monitorStatus.getCompletionTimestamp() - monitorStatus.getStartTime());
  }

  /**
   * Delete regions from in-memory states
   * @param regionInfos       regions to delete
   * @param env               MasterProcedureEnv
   * @param regionReplication the number of region replications
   */
  private void deleteRegionsFromInMemoryStates(List<RegionInfo> regionInfos, MasterProcedureEnv env,
    int regionReplication) {
    FavoredNodesManager fnm = env.getMasterServices().getFavoredNodesManager();

    env.getAssignmentManager().getRegionStates().deleteRegions(regionInfos);
    env.getMasterServices().getServerManager().removeRegions(regionInfos);
    if (((KnobRuntime.check(java.util.UUID.fromString("a55f04b4-f526-3884-a4f1-81a6cbd56027"))) ? ((fnm) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d82e45a9-d93f-3f14-9be1-e33a954542ef"))) ? ((fnm) != (null)) : (fnm != null))))) {
      fnm.deleteFavoredNodesForRegions(regionInfos);
    }

    // For region replicas
    if (regionReplication > 1) {
      for (RegionInfo regionInfo : regionInfos) {
        for (int i = 1; i < regionReplication; i++) {
          RegionInfo regionInfoForReplica =
            RegionReplicaUtil.getRegionInfoForReplica(regionInfo, i);
          env.getAssignmentManager().getRegionStates().deleteRegion(regionInfoForReplica);
          env.getMasterServices().getServerManager().removeRegion(regionInfoForReplica);
          if (fnm != null) {
            fnm.deleteFavoredNodesForRegion(regionInfoForReplica);
          }
        }
      }
    }
  }

  /**
   * Add regions to in-memory states
   * @param regionInfos       regions to add
   * @param env               MasterProcedureEnv
   * @param regionReplication the number of region replications
   */
  private void addRegionsToInMemoryStates(List<RegionInfo> regionInfos, MasterProcedureEnv env,
    int regionReplication) {
    AssignmentManager am = env.getAssignmentManager();
    for (RegionInfo regionInfo : regionInfos) {
      if (regionInfo.isSplit()) {
        am.getRegionStates().updateRegionState(regionInfo, RegionState.State.SPLIT);
      } else {
        am.getRegionStates().updateRegionState(regionInfo, RegionState.State.CLOSED);

        // For region replicas
        for (int i = 1; i < regionReplication; i++) {
          RegionInfo regionInfoForReplica =
            RegionReplicaUtil.getRegionInfoForReplica(regionInfo, i);
          am.getRegionStates().updateRegionState(regionInfoForReplica, RegionState.State.CLOSED);
        }
      }
    }
  }

  private void restoreSnapshotAcl(final MasterProcedureEnv env) throws IOException {
    if (
      restoreAcl && snapshot.hasUsersAndPermissions() && snapshot.getUsersAndPermissions() != null
        && SnapshotDescriptionUtils.isSecurityAvailable(env.getMasterServices().getConfiguration())
    ) {
      // restore acl of snapshot to table.
      RestoreSnapshotHelper.restoreSnapshotAcl(snapshot, TableName.valueOf(snapshot.getTable()),
        env.getMasterServices().getConfiguration());
    }
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
