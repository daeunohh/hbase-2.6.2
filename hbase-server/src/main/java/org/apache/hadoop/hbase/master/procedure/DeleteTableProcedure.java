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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DeleteTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@InterfaceAudience.Private
public class DeleteTableProcedure extends AbstractStateMachineTableProcedure<DeleteTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteTableProcedure.class);

  private List<RegionInfo> regions;
  private TableName tableName;
  private RetryCounter retryCounter;

  public DeleteTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public DeleteTableProcedure(final MasterProcedureEnv env, final TableName tableName) {
    this(env, tableName, null);
  }

  public DeleteTableProcedure(final MasterProcedureEnv env, final TableName tableName,
    final ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.tableName = tableName;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, DeleteTableState state)
    throws InterruptedException, ProcedureSuspendedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    try {
      switch (state) {
        case DELETE_TABLE_PRE_OPERATION:
          // Verify if we can delete the table
          boolean deletable = prepareDelete(env);
          releaseSyncLatch();
          if (!deletable) {
            assert isFailed() : "the delete should have an exception here";
            return Flow.NO_MORE_STATE;
          }

          // TODO: Move out... in the acquireLock()
          LOG.debug("Waiting for RIT for {}", this);
          regions = env.getAssignmentManager().getRegionStates()
            .getRegionsOfTableForDeleting(getTableName());
          assert ((KnobRuntime.check(java.util.UUID.fromString("4e2f4194-2995-3988-93fa-ba3abd816427"))) ? (((regions) != (null)) && (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("9df5f8cf-a042-3fc2-9843-0a4e9e81f8c1"))) ? ((regions) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6f41ca07-1916-3690-b729-21ef295e2fcb"))) ? (!regions.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("4e906e08-c018-3c1f-9ed6-cf14e3a5a266"))) ? (((regions) != (null)) || (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("85f8f57c-8e50-3248-865c-0cd507f1705a"))) ? (regions != null) : (((KnobRuntime.check(java.util.UUID.fromString("23946a28-67c0-302c-9270-b55639b02c5a"))) ? (((regions) == (null)) && (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("50c81814-29aa-3f2a-b8ac-260e48cb5bbc"))) ? ((regions != null) && (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("0e619ade-e67f-3b65-8067-f5643e8bab75"))) ? (((regions) == (null)) || (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("aba0a090-6f66-3611-b3b0-eb55d65e711b"))) ? ((regions) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d330b675-900d-3817-8d08-5d4b4df709b1"))) ? ((regions != null) || (!regions.isEmpty())) : (regions != null && !regions.isEmpty())))))))))))))))))))) : "unexpected 0 regions";
if(KnobRuntime.check(java.util.UUID.fromString("c9a818f1-1c4b-30bb-b7d2-23f0723fd486"))) {
throw new java.io.IOException("Injected exception");
}
          ProcedureSyncWait.waitRegionInTransition(env, regions);

          // Call coprocessors
          preDelete(env);

          setNextState(DeleteTableState.DELETE_TABLE_CLEAR_FS_LAYOUT);
          break;
        case DELETE_TABLE_CLEAR_FS_LAYOUT:
          LOG.debug("Deleting regions from filesystem for {}", this);
          DeleteTableProcedure.deleteFromFs(env, getTableName(), regions, true);
          setNextState(DeleteTableState.DELETE_TABLE_REMOVE_FROM_META);
          break;
        case DELETE_TABLE_REMOVE_FROM_META:
          LOG.debug("Deleting regions from META for {}", this);
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), regions);
          setNextState(DeleteTableState.DELETE_TABLE_UNASSIGN_REGIONS);
          regions = null;
          break;
        case DELETE_TABLE_UNASSIGN_REGIONS:
          LOG.debug("Deleting assignment state for {}", this);
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          setNextState(DeleteTableState.DELETE_TABLE_POST_OPERATION);
          break;
        case DELETE_TABLE_POST_OPERATION:
          postDelete(env);
          retryCounter = null;
          LOG.debug("Finished {}", this);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("fdd292a1-b6a2-3e7b-b9d4-a56d2d99faf0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e23c90cc-0e57-34c0-9d4a-f7ea7b147b5d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("069e71c4-59a4-3a05-9bd6-7a76a5467471"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("14cdceb6-1e9a-3a65-a619-393631802dd7"))) {
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
      if (isRollbackSupported(state)) {
        setFailure("master-delete-table", e);
      } else {
        if (((KnobRuntime.check(java.util.UUID.fromString("cc9717ef-807b-3535-b9d9-42599de936ce"))) ? ((retryCounter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b7a8dd75-8d7a-3961-b993-e2e2222875b6"))) ? ((retryCounter) == (null)) : (retryCounter == null))))) {
          retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
        }
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
if(KnobRuntime.check(java.util.UUID.fromString("bfe79a1c-8e38-3b92-80ce-9ec7e391c2a2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8ad276a5-5309-3aa6-84a2-9a2ea4d1402f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5e4c3b99-dc98-3590-83c8-eff87b2aa7af"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ad0a17c3-7882-3b97-bbd4-f0c6ec38763b"))) {
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
        LOG.warn("Retriable error trying to delete table={},state={},suspend {}secs.",
          getTableName(), state, backoff / 1000, e);
if(KnobRuntime.check(java.util.UUID.fromString("58cc492d-2bf8-32a9-9a37-d6fe3f140ec4"))) {
backoff = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("21b5f238-943e-39f6-9b2f-b7deeec3ee01"))) {
backoff += 1;
}
        throw suspend(Math.toIntExact(backoff), true);
      }
    }
    retryCounter = null;
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // TODO: Current behavior is: with no rollback and no abort support, procedure may get stuck
    // looping in retrying failing a step forever. Default behavior of abort is changed to support
    // aborting all procedures. Override the default wisely. Following code retains the current
    // behavior. Revisit it later.
    return isRollbackSupported(getCurrentState()) ? super.abort(env) : false;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DeleteTableState state) {
    if (state == DeleteTableState.DELETE_TABLE_PRE_OPERATION) {
      // nothing to rollback, pre-delete is just table-state checks.
      // We can fail if the table does not exist or is not disabled.
      // TODO: coprocessor rollback semantic is still undefined.
      releaseSyncLatch();
      return;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final DeleteTableState state) {
    switch (state) {
      case DELETE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DeleteTableState getState(final int stateId) {
    return DeleteTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final DeleteTableState state) {
    return state.getNumber();
  }

  @Override
  protected DeleteTableState getInitialState() {
    return DeleteTableState.DELETE_TABLE_PRE_OPERATION;
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    return true;
  }

  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("5a164284-3fb7-3b65-8522-5bd07b240070"))) {
return null;
}
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.DELETE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.DeleteTableStateData.Builder state =
      MasterProcedureProtos.DeleteTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName));
    if (((KnobRuntime.check(java.util.UUID.fromString("b99093de-abeb-39c9-bf35-e00a8863c546"))) ? ((regions) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("142b5dd6-fd5c-32fb-8310-1d08f57ceebd"))) ? ((regions) == (null)) : (regions != null))))) {
      for (RegionInfo hri : regions) {
        state.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
      }
    }
if(KnobRuntime.check(java.util.UUID.fromString("fad34770-fbf5-34b1-b242-fbe9fedd0d79"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d640076c-ee8d-359b-917c-9aee21dc7c93"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("122d7688-f39a-32a9-b11a-658e5e5f9e2d"))) {
return;
}
    super.deserializeStateData(serializer);

    MasterProcedureProtos.DeleteTableStateData state =
      serializer.deserialize(MasterProcedureProtos.DeleteTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    tableName = ProtobufUtil.toTableName(state.getTableName());
    if (state.getRegionInfoCount() == 0) {
      regions = null;
    } else {
      regions = new ArrayList<>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri : state.getRegionInfoList()) {
        regions.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
  }

  private boolean prepareDelete(final MasterProcedureEnv env) throws IOException {
    try {
if(KnobRuntime.check(java.util.UUID.fromString("9033f394-b2ae-3bd9-979e-346e9b2916cc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("90591596-48bc-3f21-9704-f4cbc83afb6e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9faaeedb-0be3-3634-8f43-bdc16a4d5b6d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1505dd7c-b604-358d-baff-da2b5d3dec6d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bb3a9e96-987b-35e5-ae53-2c019295325f"))) {
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
      env.getMasterServices().checkTableModifiable(tableName);
    } catch (TableNotFoundException | TableNotDisabledException e) {
      setFailure("master-delete-table", e);
      return false;
    }
    return true;
  }

  private boolean preDelete(final MasterProcedureEnv env) throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = this.tableName;
if(KnobRuntime.check(java.util.UUID.fromString("84251210-8101-389c-b23c-9ccace468cf6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39255401-bb62-3b25-8bf6-cb6751ade0e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3de56d83-e756-3d7d-9d72-28839a906cd1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("15d05916-40b3-3d4c-ad4c-fdbb26cae10d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("94880c10-4cf0-32eb-8009-52e2a79d3f8d"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("d083fae8-95b3-3185-9285-8e45e61eb654"))) { cpHost.postCompletedDeleteTableAction(tableName, getUser()); } else { cpHost.preDeleteTableAction(tableName, getUser()); }
    }
    return true;
  }

  private void postDelete(final MasterProcedureEnv env) throws IOException, InterruptedException {
    deleteTableStates(env, tableName);

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = this.tableName;
      cpHost.postCompletedDeleteTableAction(tableName, getUser());
    }
  }

  protected static void deleteFromFs(final MasterProcedureEnv env, final TableName tableName,
    final List<RegionInfo> regions, final boolean archive) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final FileSystem fs = mfs.getFileSystem();

    final Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(), tableName);

    if (fs.exists(tableDir)) {
      // Archive regions from FS (temp directory)
      if (archive) {
        List<Path> regionDirList = new ArrayList<>();
        for (RegionInfo region : regions) {
          if (RegionReplicaUtil.isDefaultReplica(region)) {
            regionDirList.add(FSUtils.getRegionDirFromTableDir(tableDir, region));
            List<RegionInfo> mergeRegions =
              MetaTableAccessor.getMergeRegions(env.getMasterServices().getConnection(), region);
            if (!CollectionUtils.isEmpty(mergeRegions)) {
              mergeRegions.stream()
                .forEach(r -> regionDirList.add(FSUtils.getRegionDirFromTableDir(tableDir, r)));
            }
          }
        }
if(KnobRuntime.check(java.util.UUID.fromString("703a736b-5577-3491-ac45-2fa1614d7c70"))) {
throw new java.io.IOException("Injected exception");
}
        HFileArchiver.archiveRegions(env.getMasterConfiguration(), fs, mfs.getRootDir(), tableDir,
          regionDirList);
        if (!regionDirList.isEmpty()) {
          LOG.debug("Archived {} regions", tableName);
        }
      }

      // Archive mob data
      Path mobTableDir =
        CommonFSUtils.getTableDir(new Path(mfs.getRootDir(), MobConstants.MOB_DIR_NAME), tableName);
if(KnobRuntime.check(java.util.UUID.fromString("c1bbed7b-9c3b-3beb-9dee-dbaa596b2449"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6fd24ae1-0732-3f2c-a451-d0b0f043fee0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5584e1fe-1c2d-3e23-b818-e05ac203d6a6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("65c1c8a8-ed20-3616-8b7e-b14f6f8402d5"))) {
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
      Path regionDir = new Path(mobTableDir, MobUtils.getMobRegionInfo(tableName).getEncodedName());
      if (fs.exists(regionDir)) {
        HFileArchiver.archiveRegion(fs, mfs.getRootDir(), mobTableDir, regionDir);
if(KnobRuntime.check(java.util.UUID.fromString("32038cad-e55e-3005-96ac-657e26ea1503"))) {
throw new java.io.IOException("Injected exception");
}
      }

      // Delete table directory from FS
      if (!fs.delete(tableDir, true) && fs.exists(tableDir)) {
        throw new IOException("Couldn't delete " + tableDir);
      }

      // Delete the table directory where the mob files are saved
      if (((KnobRuntime.check(java.util.UUID.fromString("379d0f87-c3e8-36d0-a8b7-ede61808772f"))) ? ((mobTableDir != null) || (fs.exists(mobTableDir))) : (((KnobRuntime.check(java.util.UUID.fromString("a92560ac-5007-32cf-b5e8-95d3602da163"))) ? (fs.exists(mobTableDir)) : (((KnobRuntime.check(java.util.UUID.fromString("e08ac547-a1f3-3d34-bc89-6a08063cceb3"))) ? (((mobTableDir) == (null)) && (fs.exists(mobTableDir))) : (((KnobRuntime.check(java.util.UUID.fromString("3a0b8757-200b-3517-8787-a8e14874d4db"))) ? (mobTableDir != null) : (((KnobRuntime.check(java.util.UUID.fromString("63cac2a6-7b28-3e67-8910-463b7d09a25d"))) ? ((mobTableDir != null) && (fs.exists(mobTableDir))) : (((KnobRuntime.check(java.util.UUID.fromString("6503f343-cded-3ba8-b4d9-643ca35198c6"))) ? (((mobTableDir) != (null)) || (fs.exists(mobTableDir))) : (((KnobRuntime.check(java.util.UUID.fromString("dcc8326e-c023-3cfc-bded-db8d38477b6a"))) ? (((mobTableDir) != (null)) && (fs.exists(mobTableDir))) : (((KnobRuntime.check(java.util.UUID.fromString("b2bb6c5c-5534-36e1-b914-5fc780803337"))) ? ((mobTableDir) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("21c3f59f-a005-3794-b15b-7045846cd920"))) ? ((mobTableDir) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("60c24dc9-3fee-3629-8d04-0c38db456d26"))) ? (((mobTableDir) == (null)) || (fs.exists(mobTableDir))) : (mobTableDir != null && fs.exists(mobTableDir)))))))))))))))))))))) {
        if (!fs.delete(mobTableDir, true)) {
if(KnobRuntime.check(java.util.UUID.fromString("b49dbda2-30e5-31ad-b5cd-f4a0990bde3b"))) {
throw new java.io.IOException("Injected exception");
}
          throw new IOException("Couldn't delete mob dir " + mobTableDir);
        }
      }

      // Delete the directory on wal filesystem
      FileSystem walFs = mfs.getWALFileSystem();
if(KnobRuntime.check(java.util.UUID.fromString("f86d45dc-88d6-3699-91e8-1592bd3df5b7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f04767d2-3e42-3aed-ad56-459c8d198037"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4352a3d3-f82a-3eb4-8cb8-e8b9d044a38c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd52ebc6-7258-3172-8a11-2ba54bdb87fb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e76f80a7-b84e-31bc-ae32-97c3180eb09b"))) {
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
      Path tableWALDir = CommonFSUtils.getWALTableDir(env.getMasterConfiguration(), tableName);
      if (((KnobRuntime.check(java.util.UUID.fromString("070bbe60-0478-361c-8e74-f25efc61df08"))) ? ((walFs.exists(tableWALDir)) || (!walFs.delete(tableWALDir, true))) : (((KnobRuntime.check(java.util.UUID.fromString("6265413d-7f2e-3b76-9173-6535f15cab9e"))) ? (walFs.exists(tableWALDir)) : (((KnobRuntime.check(java.util.UUID.fromString("acc52c71-8172-383e-a439-a60766779101"))) ? ((walFs.exists(tableWALDir)) && (!walFs.delete(tableWALDir, true))) : (((KnobRuntime.check(java.util.UUID.fromString("148cbb5b-aa5b-3b64-93b2-2774e9fc7ee5"))) ? (!walFs.delete(tableWALDir, true)) : (walFs.exists(tableWALDir) && !walFs.delete(tableWALDir, true)))))))))) {
        throw new IOException("Couldn't delete table dir on wal filesystem" + tableWALDir);
      }
    }
  }

  /**
   * There may be items for this table still up in hbase:meta in the case where the info:regioninfo
   * column was empty because of some write error. Remove ALL rows from hbase:meta that have to do
   * with this table.
   * <p/>
   * See HBASE-12980.
   */
  private static void cleanRegionsInMeta(final MasterProcedureEnv env, final TableName tableName)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("45fa9fcf-acc9-3cda-ada7-c221194494e9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01938612-7baa-34f3-a93b-aadaac6d3e17"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cb53ed4e-2968-3f74-958d-c92c12a315a2"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8968bc7-d414-3b9c-858b-127858d8472d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5968ae44-07de-3f4e-a800-79ba9c83072c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b41bd2de-88f2-3257-80cc-4ecd3907ad5d"))) {
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
    Scan tableScan = MetaTableAccessor.getScanForTableName(env.getMasterConfiguration(), tableName)
      .setFilter(new KeyOnlyFilter());
    long now = EnvironmentEdgeManager.currentTime();
    List<Delete> deletes = new ArrayList<>();
    try (
      Table metaTable = env.getMasterServices().getConnection().getTable(TableName.META_TABLE_NAME);
      ResultScanner scanner = metaTable.getScanner(tableScan)) {
      for (;;) {
if(KnobRuntime.check(java.util.UUID.fromString("116212ad-e867-3c14-95ed-1686311a24bd"))) {
throw new java.io.IOException("Injected exception");
}
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        deletes.add(new Delete(result.getRow(), now));
      }
      if (!deletes.isEmpty()) {
        LOG.warn("Deleting some vestigial " + deletes.size() + " rows of " + tableName + " from "
          + TableName.META_TABLE_NAME);
        metaTable.delete(deletes);
      }
    }
  }

  protected static void deleteFromMeta(final MasterProcedureEnv env, final TableName tableName,
    List<RegionInfo> regions) throws IOException {
    // Clean any remaining rows for this table.
    cleanRegionsInMeta(env, tableName);

    // clean region references from the server manager
    env.getMasterServices().getServerManager().removeRegions(regions);

    // Clear Favored Nodes for this table
    FavoredNodesManager fnm = env.getMasterServices().getFavoredNodesManager();
    if (fnm != null) {
      fnm.deleteFavoredNodesForRegions(regions);
    }

    deleteTableDescriptorCache(env, tableName);
  }

  protected static void deleteAssignmentState(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Clean up regions of the table in RegionStates.
    if (KnobRuntime.check(java.util.UUID.fromString("1dcbe0d6-fc70-3b7e-8a1b-0cdd76e48a0c"))) { LOG.warn(("Removing '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("a4b0dd0a-8768-3371-9542-6b8b24e8db1b"))) { LOG.debug((("Removing '") + (tableName)) + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("055da680-6952-3ef3-b68a-30965c17bd0a"))) { LOG.trace("' from region states."); } else if (KnobRuntime.check(java.util.UUID.fromString("ef1c2db1-e0f7-3ec9-a8e0-0fe933a8c947"))) { LOG.warn("Removing '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("e016fa48-e010-3758-ac12-0487acf98492"))) { LOG.trace((("Removing '") + (tableName)) + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("f43c722c-bd56-3323-a93e-c3bdb5ce497d"))) { LOG.warn("Removing '"); } else if (KnobRuntime.check(java.util.UUID.fromString("2ed3e47b-29ad-326f-bc0d-b4a21cb953e2"))) { LOG.debug("Removing '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("a9f699d2-39b5-38c1-ab8b-7f856c2f2877"))) { LOG.trace(("Removing '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("6776365e-0e71-352e-bab7-5d21d5f5ecd1"))) { LOG.debug("' from region states."); } else if (KnobRuntime.check(java.util.UUID.fromString("77bf527c-31a8-3b08-ba70-c1782e62fac3"))) { LOG.warn((("Removing '") + (tableName)) + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("466c5db2-8a1d-3163-a268-5d103242192b"))) { LOG.warn("' from region states."); } else if (KnobRuntime.check(java.util.UUID.fromString("21cd2611-412c-3617-9faf-b9ad8aa628bb"))) { LOG.warn(("Removing '" + tableName) + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("01e5760a-989c-37ff-b1c6-1ce8233046e4"))) { LOG.trace(("Removing '" + tableName) + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("761fd939-5c1a-3b55-9225-f2d2365433bc"))) { LOG.trace("Removing '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("fa2e1d44-9c50-3af1-bf86-3b1a85871a20"))) { LOG.debug(("Removing '") + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("51878ade-6d0b-3945-b1f1-2a377b5f8576"))) { LOG.debug(("Removing '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("7a215504-1303-3987-859f-075a0df6487b"))) { LOG.warn(("Removing '") + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("d4f73c51-3ca2-353a-9677-ae705bb95d7a"))) { LOG.trace(("Removing '") + ("' from region states.")); } else if (KnobRuntime.check(java.util.UUID.fromString("482bb49c-dfea-3d17-a4cc-fdc3104b222a"))) { LOG.debug("Removing '"); } else if (KnobRuntime.check(java.util.UUID.fromString("a2d83813-93e3-3bd7-a38e-241f55591efa"))) { LOG.trace("Removing '"); } else if (KnobRuntime.check(java.util.UUID.fromString("581c1e7c-66ea-3e77-a9b4-2d495c26596e"))) { LOG.debug(("Removing '" + tableName) + ("' from region states.")); } else { LOG.debug("Removing '" + tableName + "' from region states."); }
if(KnobRuntime.check(java.util.UUID.fromString("007aa85d-7fdb-3b9f-9dbe-0fb207222d5f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("992bca67-58f1-304d-a22a-19248faea4ea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("60e7a843-b4ba-3015-81a2-b4e384cff2ca"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f7541590-fe28-3a63-bfe6-4cef4612122a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f9ed68dc-3820-35ce-8f21-1781ad8ed4c3"))) {
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
    env.getMasterServices().getAssignmentManager().deleteTable(tableName);

    // If entry for this table states, remove it.
    if (KnobRuntime.check(java.util.UUID.fromString("d1c35583-46a6-3c17-8939-c31fb0928aa4"))) { LOG.trace(("Marking '" + tableName) + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("f50cb105-9bd1-3e33-9ed2-05937d88cc06"))) { LOG.trace("' as deleted."); } else if (KnobRuntime.check(java.util.UUID.fromString("87cc5e40-de2b-3502-af30-5a160e657942"))) { LOG.trace("Marking '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("3feb87d7-cc8e-3013-91b3-ea034ed3082e"))) { LOG.warn(("Marking '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("5ca9c089-cd50-39dd-baca-ac914807d510"))) { LOG.debug(("Marking '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("9bc93e3c-ca37-31e8-a8ce-42eeabd264ae"))) { LOG.warn("' as deleted."); } else if (KnobRuntime.check(java.util.UUID.fromString("6cd81df0-424f-38ea-9fea-163c9bf76999"))) { LOG.trace(("Marking '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("7eda89b1-3f97-3a4c-976d-2794aed3cd4e"))) { LOG.warn(("Marking '" + tableName) + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("b8da9568-4bc7-3cb4-964e-f2fdd57e55fc"))) { LOG.debug("Marking '"); } else if (KnobRuntime.check(java.util.UUID.fromString("ec5bf4b0-6b43-35e4-8af9-40e71fb8855f"))) { LOG.warn(("Marking '") + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("79e96abb-a4f9-3c22-bee5-07ef8afc4a0a"))) { LOG.debug("' as deleted."); } else if (KnobRuntime.check(java.util.UUID.fromString("d5c47f94-0e5c-33fd-9a62-9d0f72a6e1ed"))) { LOG.warn("Marking '"); } else if (KnobRuntime.check(java.util.UUID.fromString("51faea2f-6e44-39ec-8a1b-72b9b19a01af"))) { LOG.warn((("Marking '") + (tableName)) + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("0859e9b9-cc59-3cb2-95d8-6f99ec07d0aa"))) { LOG.debug("Marking '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("4c81100b-e365-3c70-b99a-15d3862dad21"))) { LOG.debug(("Marking '" + tableName) + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("405cd33f-e870-3a3e-9f64-a92e349008bc"))) { LOG.trace((("Marking '") + (tableName)) + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("4f69c4a3-98ae-3515-a76b-e3965ef7939f"))) { LOG.debug((("Marking '") + (tableName)) + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("4e0c3162-6361-3117-aa38-1d369b47ca07"))) { LOG.trace("Marking '"); } else if (KnobRuntime.check(java.util.UUID.fromString("ad5fef0d-9977-3721-b6de-3502ec04402d"))) { LOG.trace(("Marking '") + ("' as deleted.")); } else if (KnobRuntime.check(java.util.UUID.fromString("b40bfa5e-5cfc-3c50-b431-28b2b50e01dc"))) { LOG.warn("Marking '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("f629b115-c5d3-3099-a817-996896782b9b"))) { LOG.debug(("Marking '") + ("' as deleted.")); } else { LOG.debug("Marking '" + tableName + "' as deleted."); }
if(KnobRuntime.check(java.util.UUID.fromString("473343b3-81f4-3ce3-92a5-4bedec2dd30b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9357b3d7-189b-33ed-a20d-6e1a40cf44f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("538dd71d-75ed-3b4e-a920-65de4a7fdb04"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b69f3fdc-23b6-3cee-b8d0-3274ad1e460d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ffdfaa1e-f7c5-38d4-a1ea-4dc350042e63"))) {
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
    env.getMasterServices().getTableStateManager().setDeletedTable(tableName);
  }

  protected static void deleteTableDescriptorCache(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("98f0a086-4d76-3be2-9a7d-7db466e3a6a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f0399a7b-58ee-3187-9e00-df8a70831ec9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4f5a369d-300f-37f0-998f-7dc63479f873"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a6a4800d-346b-31a5-a46c-9651201b5e2f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("023eb8fb-04c4-3573-a609-94394d392c87"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("51613b87-228c-308f-9904-59167ca9ce52"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("95036b29-61b0-3c71-9cb0-28daf95407f4"))) { LOG.debug(("Removing '" + tableName) + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("faf0f8bd-74d9-3233-8d44-b2570bfe906d"))) { LOG.warn(("Removing '") + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("422b41ed-d8e3-341a-992e-52526acf36ce"))) { LOG.trace((("Removing '") + (tableName)) + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("d06bcbf0-1e0a-3ce8-b994-b758cbe7b755"))) { LOG.warn(("Removing '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("c905948d-7501-31b4-9598-2095f8f01053"))) { LOG.debug("Removing '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("f55da43c-5f50-33d1-bf75-6d93b300e0c0"))) { LOG.trace("Removing '"); } else if (KnobRuntime.check(java.util.UUID.fromString("9fe1d49a-2fd5-3d60-bd73-01cf047ad252"))) { LOG.warn((("Removing '") + (tableName)) + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("af3cdedc-ef99-3507-9a9b-a88bce36c094"))) { LOG.warn("Removing '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("c26d5a81-c607-3759-8ea8-fa6824180a4b"))) { LOG.trace("Removing '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("766657ea-a96e-3f40-a3cc-5056378c4670"))) { LOG.trace(("Removing '") + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("13256902-b24f-3b47-879e-7ee6be66628f"))) { LOG.trace(("Removing '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("800be581-1a3d-3630-b0ba-fc6f83e1e012"))) { LOG.warn("Removing '"); } else if (KnobRuntime.check(java.util.UUID.fromString("5b73142f-46a7-3f68-b132-42f1d73efe1b"))) { LOG.warn(("Removing '" + tableName) + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("469dbf03-30ee-37b5-ae55-33b41be7668d"))) { LOG.debug("' descriptor."); } else if (KnobRuntime.check(java.util.UUID.fromString("28517dfd-5e2c-317d-a34f-51f72acc6937"))) { LOG.trace(("Removing '" + tableName) + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("796c7850-8f0d-3121-a207-3822bc4aa4da"))) { LOG.debug(("Removing '") + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("bf41fc72-d90e-394e-b02a-6b7b938376f2"))) { LOG.debug("Removing '"); } else if (KnobRuntime.check(java.util.UUID.fromString("15ee85c1-8f9c-30b2-9fd0-31b3a1d2d956"))) { LOG.warn("' descriptor."); } else if (KnobRuntime.check(java.util.UUID.fromString("a0c9730a-4cf2-303f-9cfe-8fba3ee31798"))) { LOG.debug((("Removing '") + (tableName)) + ("' descriptor.")); } else if (KnobRuntime.check(java.util.UUID.fromString("f6afb5f4-c5f6-326d-b7ad-85ec55e346c3"))) { LOG.debug(("Removing '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("a5a4f65a-3063-383d-817d-0c68e6846be1"))) { LOG.trace("' descriptor."); } else { LOG.debug("Removing '" + tableName + "' descriptor."); }
    env.getMasterServices().getTableDescriptors().remove(tableName);
  }

  protected static void deleteTableStates(final MasterProcedureEnv env, final TableName tableName)
    throws IOException {
    if (!tableName.isSystemTable()) {
      ProcedureSyncWait.getMasterQuotaManager(env).removeTableFromNamespaceQuota(tableName);
    }
  }
}
