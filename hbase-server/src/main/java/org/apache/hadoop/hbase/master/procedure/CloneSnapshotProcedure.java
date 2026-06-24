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
    if (monitorStatus == null) {
      monitorStatus = TaskMonitor.get()
        .createStatus("Cloning  snapshot '" + snapshot.getName() + "' to table " + getTableName());
    }
    return monitorStatus;
  }

  private void restoreSnapshotAcl(MasterProcedureEnv env) throws IOException {
    Configuration conf = env.getMasterServices().getConfiguration();
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
if(KnobRuntime.check(java.util.UUID.fromString("40abb12e-d4c0-31b0-88ad-f9368c79ec4b"))) {
return null;
}
    LOG.trace("{} execute state={}", this, state);
    try {
      switch (state) {
        case CLONE_SNAPSHOT_PRE_OPERATION:
          // Verify if we can clone the table
          prepareClone(env);

if(KnobRuntime.check(java.util.UUID.fromString("67eb37e5-80c8-3430-8d73-818d37d651c8"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          preCloneSnapshot(env);
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_WRITE_FS_LAYOUT);
          break;
        case CLONE_SNAPSHOT_WRITE_FS_LAYOUT:
          updateTableDescriptorWithSFT();
          newRegions = createFilesystemLayout(env, tableDescriptor, newRegions);
          env.getMasterServices().getTableDescriptors().update(tableDescriptor, true);
          if (tableDescriptor.getErasureCodingPolicy() != null) {
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
          addRegionsToMeta(env);
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_ASSIGN_REGIONS);
          break;
        case CLONE_SNAPSHOT_ASSIGN_REGIONS:
          CreateTableProcedure.setEnablingState(env, getTableName());

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
          CreateTableProcedure.setEnabledState(env, getTableName());
          setNextState(CloneSnapshotState.CLONE_SNAPHOST_RESTORE_ACL);
          break;
        case CLONE_SNAPHOST_RESTORE_ACL:
if(KnobRuntime.check(java.util.UUID.fromString("1abbcfbc-68a8-3b06-90fa-d80ecdf57790"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("9420a6ac-f9e9-3430-9368-16da774a17a6"))) { prepareClone(env); } else { restoreSnapshotAcl(env); }
          setNextState(CloneSnapshotState.CLONE_SNAPSHOT_POST_OPERATION);
          break;
        case CLONE_SNAPSHOT_POST_OPERATION:
          postCloneSnapshot(env);

          MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
          metricsSnapshot.addSnapshotClone(
            getMonitorStatus().getCompletionTimestamp() - getMonitorStatus().getStartTime());
          getMonitorStatus().markComplete("Clone snapshot '" + snapshot.getName() + "' completed!");
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
    switch (state) {
      case CLONE_SNAPSHOT_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected CloneSnapshotState getState(final int stateId) {
    return CloneSnapshotState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final CloneSnapshotState state) {
    return state.getNumber();
  }

  @Override
  protected CloneSnapshotState getInitialState() {
    return CloneSnapshotState.CLONE_SNAPSHOT_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("a570b0b9-6bb9-3eb6-aa6b-468cc02a568b"))) {
return null;
}
    return tableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.CREATE; // Clone is creating a table
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(" snapshot=");
if(KnobRuntime.check(java.util.UUID.fromString("142b31cc-c4e4-3139-89e0-e1754984dcfe"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("26a9f844-f52c-3967-bf31-7dc49484e411"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1fd32910-9961-3f41-9759-5d8203ddba4c"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c3ea01a5-b1f8-3f25-90b5-16884deb399f"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("creationTime_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
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

    CloneSnapshotStateData.Builder cloneSnapshotMsg = CloneSnapshotStateData.newBuilder()
      .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser())).setSnapshot(this.snapshot)
      .setTableSchema(ProtobufUtil.toTableSchema(tableDescriptor));

    cloneSnapshotMsg.setRestoreAcl(restoreAcl);
    if (newRegions != null) {
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
    final TableName tableName = getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("30b3f677-4f81-3483-9751-e2dda822054a"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (env.getMasterServices().getTableDescriptors().exists(tableName)) {
      throw new TableExistsException(tableName);
    }

    // check whether ttl has expired for this snapshot
    if (
      SnapshotDescriptionUtils.isExpiredSnapshot(snapshot.getTtl(), snapshot.getCreationTime(),
        EnvironmentEdgeManager.currentTime())
    ) {
      throw new SnapshotTTLExpiredException(ProtobufUtil.createSnapshotDesc(snapshot));
    }

    validateSFT();
  }

  /**
   * Action before cloning from snapshot.
   * @param env MasterProcedureEnv
   */
  private void preCloneSnapshot(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("14fd0cd7-306c-3ada-81e4-81cadf2a6a13"))) {
return;
}
    if (!getTableName().isSystemTable()) {
      // Check and update namespace quota
      final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();

      SnapshotManifest manifest =
        SnapshotManifest.open(env.getMasterConfiguration(), mfs.getFileSystem(),
          SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, mfs.getRootDir()), snapshot);

      ProcedureSyncWait.getMasterQuotaManager(env).checkNamespaceTableAndRegionQuota(getTableName(),
        manifest.getRegionManifestsMap().size());
    }

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preCreateTableAction(tableDescriptor, null, getUser());
    }
  }

  /**
   * Action after cloning from snapshot.
   * @param env MasterProcedureEnv
   */
  private void postCloneSnapshot(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("da9bca97-2f39-3258-8691-b0a770fd86bf"))) ? ((cpHost) != (null)) : (cpHost != null))) {
      final RegionInfo[] regions =
        (newRegions == null) ? null : newRegions.toArray(new RegionInfo[newRegions.size()]);
if(KnobRuntime.check(java.util.UUID.fromString("64457d5f-2e65-3fef-997e-4a1d8b45731e"))) {
throw new java.io.IOException("Injected exception");
}
      cpHost.postCompletedCreateTableAction(tableDescriptor, regions, getUser());
    }
  }

  /**
   * Create regions in file system.
   * @param env MasterProcedureEnv
   */
  private List<RegionInfo> createFilesystemLayout(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final List<RegionInfo> newRegions) throws IOException {
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
if(KnobRuntime.check(java.util.UUID.fromString("7a7848ff-f0f3-30f3-b4c8-c2230946f9cf"))) {
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
          Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
if(KnobRuntime.check(java.util.UUID.fromString("4b338e08-a90d-3a17-bacb-01adc3dc7d73"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("18429426-2b86-319e-9f84-dc97505047d7"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f07b31e-e745-3127-a696-770f3717e443"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("creationTime_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8eb2e50-c2d5-33f1-b169-cf8e68b694d0"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("maxFileSize_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshot);
          RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs, manifest,
            tableDescriptor, tableRootDir, monitorException, monitorStatus);
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
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    final Path tableDir =
      CommonFSUtils.getTableDir(mfs.getRootDir(), tableDescriptor.getTableName());
    if (CommonFSUtils.isExists(mfs.getFileSystem(), tableDir)) {
      // if the region dirs exist, will cause exception and unlimited retry (see HBASE-24546)
      LOG.warn("temp table dir already exists on disk: {}, will be deleted.", tableDir);
      CommonFSUtils.deleteDirectory(mfs.getFileSystem(), tableDir);
    }
    ((FSTableDescriptors) (env.getMasterServices().getTableDescriptors()))
      .createTableDescriptorForTableDirectory(tableDir,
        TableDescriptorBuilder.newBuilder(tableDescriptor).build(), false);

    // 2. Create Regions
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
