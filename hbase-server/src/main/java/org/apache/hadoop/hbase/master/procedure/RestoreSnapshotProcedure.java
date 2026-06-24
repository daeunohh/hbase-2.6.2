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
if(KnobRuntime.check(java.util.UUID.fromString("c7f126a4-b17b-3b43-bf04-a50bda7d033c"))) {
try {
    java.lang.reflect.Field _knob_field_ = state.getClass().getDeclaredField("value");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(state));
    _knob_field_.set(state, oldValue * 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("fc573135-10c8-3dd4-8967-0111c1c562e0"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d02bfd76-f314-3b8d-9bfb-36aa82fcbf60"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e9b2b07f-7042-33e4-96fa-ae3d39c0612c"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db3262b5-89c0-3e5f-b1b4-b8b83c806867"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue * 2);
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
        restoreSnapshotMsg.addRegionInfoForRestore(ProtobufUtil.toRegionInfo(hri));
      }
    }
    if (regionsToRemove != null) {
      for (RegionInfo hri : regionsToRemove) {
        restoreSnapshotMsg.addRegionInfoForRemove(ProtobufUtil.toRegionInfo(hri));
      }
    }
    if (regionsToAdd != null) {
      for (RegionInfo hri : regionsToAdd) {
        restoreSnapshotMsg.addRegionInfoForAdd(ProtobufUtil.toRegionInfo(hri));
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
if(KnobRuntime.check(java.util.UUID.fromString("3a397a37-e685-3467-a9b7-6fb32a2a4559"))) {
throw new java.io.IOException("Injected exception");
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
if(KnobRuntime.check(java.util.UUID.fromString("861c59a7-1b8d-3ac1-8886-8d00ea30253f"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("maxFileSize_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b34e44ca-869d-3fc2-b883-f7ebf4a33b81"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2bf14c93-eb14-3f6f-930b-0ce7cfbde8c5"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b6924504-a1f2-376e-9353-4452b91b1a0f"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue / 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("a55e9cfd-2e8b-344b-b663-1a9259fc7795"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue / 2);
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
        MetaTableAccessor.deleteRegionInfos(conn, regionsToRemove);
        deleteRegionsFromInMemoryStates(regionsToRemove, env, regionReplication);
      }

      // (2.2). Add the new set of regions to META and in-memory states
      //
      // At this point the old regions are no longer present in META.
      // and the set of regions present in the snapshot will be written to META.
      // All the information in hbase:meta are coming from the .regioninfo of each region present
      // in the snapshot folder.
      if (regionsToAdd != null) {
        MetaTableAccessor.addRegionsToMeta(conn, regionsToAdd, regionReplication);
        addRegionsToInMemoryStates(regionsToAdd, env, regionReplication);
      }

      if (regionsToRestore != null) {
        MetaTableAccessor.overwriteRegions(conn, regionsToRestore, regionReplication);

        deleteRegionsFromInMemoryStates(regionsToRestore, env, regionReplication);
        addRegionsToInMemoryStates(regionsToRestore, env, regionReplication);
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
    if (fnm != null) {
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
