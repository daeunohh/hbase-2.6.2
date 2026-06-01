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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.fs.ErasureCodingUtils;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerValidationUtils;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@InterfaceAudience.Private
public class CreateTableProcedure extends AbstractStateMachineTableProcedure<CreateTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableProcedure.class);

  private static final int MAX_REGION_REPLICATION = 0x10000;

  private TableDescriptor tableDescriptor;
  private List<RegionInfo> newRegions;
  private RetryCounter retryCounter;

  public CreateTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public CreateTableProcedure(final MasterProcedureEnv env, final TableDescriptor tableDescriptor,
    final RegionInfo[] newRegions) {
    this(env, tableDescriptor, newRegions, null);
  }

  public CreateTableProcedure(final MasterProcedureEnv env, final TableDescriptor tableDescriptor,
    final RegionInfo[] newRegions, final ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.tableDescriptor = tableDescriptor;
    this.newRegions = newRegions != null ? Lists.newArrayList(newRegions) : null;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final CreateTableState state)
    throws InterruptedException, ProcedureSuspendedException {
    LOG.info("{} execute state={}", this, state);
    try {
      switch (state) {
        case CREATE_TABLE_PRE_OPERATION:
          // Verify if we can create the table
          boolean success = prepareCreate(env);
          releaseSyncLatch();

          if (!success) {
            assert ((KnobRuntime.check(java.util.UUID.fromString("42db98b1-848d-3080-b462-a97ce5dcd01c"))) ? (hasException()) : (isFailed())) : "the delete should have an exception here";
            return Flow.NO_MORE_STATE;
          }

          preCreate(env);
          setNextState(CreateTableState.CREATE_TABLE_WRITE_FS_LAYOUT);
          break;
        case CREATE_TABLE_WRITE_FS_LAYOUT:
          DeleteTableProcedure.deleteFromFs(env, getTableName(), newRegions, true);
if(KnobRuntime.check(java.util.UUID.fromString("6665c810-8aa7-352d-af63-30b61fc2daf1"))) {
throw new java.io.IOException("Injected exception");
}
          newRegions = createFsLayout(env, tableDescriptor, newRegions);
if(KnobRuntime.check(java.util.UUID.fromString("a60546c5-3cfa-3545-9381-987eb9e4b9e2"))) {
throw new java.io.IOException("Injected exception");
}
          env.getMasterServices().getTableDescriptors().update(tableDescriptor, true);
          if (((KnobRuntime.check(java.util.UUID.fromString("0c0e0666-12cc-31ab-b475-c5102a5d2a56"))) ? ((tableDescriptor.getErasureCodingPolicy()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("17add884-5ec2-3692-9e65-593d6c034270"))) ? ((tableDescriptor.getErasureCodingPolicy()) == (null)) : (tableDescriptor.getErasureCodingPolicy() != null))))) {
            setNextState(CreateTableState.CREATE_TABLE_SET_ERASURE_CODING_POLICY);
          } else {
            setNextState(CreateTableState.CREATE_TABLE_ADD_TO_META);
          }
          break;
        case CREATE_TABLE_SET_ERASURE_CODING_POLICY:
          ErasureCodingUtils.setPolicy(env.getMasterFileSystem().getFileSystem(),
            env.getMasterFileSystem().getRootDir(), getTableName(),
            tableDescriptor.getErasureCodingPolicy());
          setNextState(CreateTableState.CREATE_TABLE_ADD_TO_META);
          break;
        case CREATE_TABLE_ADD_TO_META:
          newRegions = addTableToMeta(env, tableDescriptor, newRegions);
          setNextState(CreateTableState.CREATE_TABLE_ASSIGN_REGIONS);
          break;
        case CREATE_TABLE_ASSIGN_REGIONS:
          setEnablingState(env, getTableName());
          addChildProcedure(
            env.getAssignmentManager().createRoundRobinAssignProcedures(newRegions));
          setNextState(CreateTableState.CREATE_TABLE_UPDATE_DESC_CACHE);
          break;
        case CREATE_TABLE_UPDATE_DESC_CACHE:
          // XXX: this stage should be named as set table enabled, as now we will cache the
          // descriptor after writing fs layout.
          setEnabledState(env, getTableName());
          setNextState(CreateTableState.CREATE_TABLE_POST_OPERATION);
          break;
        case CREATE_TABLE_POST_OPERATION:
          postCreate(env);
          retryCounter = null;
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-create-table", e);
      } else {
        if (retryCounter == null) {
          retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
        }
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
        LOG.warn("Retriable error trying to create table={},state={},suspend {}secs.",
          getTableName(), state, backoff / 1000, e);
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
  protected void rollbackState(final MasterProcedureEnv env, final CreateTableState state)
    throws IOException {
    if (state == CreateTableState.CREATE_TABLE_PRE_OPERATION) {
      // nothing to rollback, pre-create is just table-state checks.
      // We can fail if the table does exist or the descriptor is malformed.
      // TODO: coprocessor rollback semantic is still undefined.
      if (
        hasException()
          /* avoid NPE */ && getException().getCause().getClass() != TableExistsException.class
      ) {
        DeleteTableProcedure.deleteTableStates(env, getTableName());

        final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
        if (cpHost != null) {
          cpHost.postDeleteTable(getTableName());
        }
      }

      releaseSyncLatch();
      return;
    }

    // The procedure doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final CreateTableState state) {
    switch (state) {
      case CREATE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected CreateTableState getState(final int stateId) {
    return CreateTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final CreateTableState state) {
    return state.getNumber();
  }

  @Override
  protected CreateTableState getInitialState() {
    return CreateTableState.CREATE_TABLE_PRE_OPERATION;
  }

  @Override
  public TableName getTableName() {
    return tableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.CREATE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.CreateTableStateData.Builder state =
      MasterProcedureProtos.CreateTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableSchema(ProtobufUtil.toTableSchema(tableDescriptor));
    if (((KnobRuntime.check(java.util.UUID.fromString("6043b898-b1c2-3e52-a4c4-32b2c14e89e7"))) ? ((newRegions) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("66a3d9da-7add-3320-bde6-7a8aceae4725"))) ? ((newRegions) != (null)) : (newRegions != null))))) {
      for (RegionInfo hri : newRegions) {
        state.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
      }
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.CreateTableStateData state =
      serializer.deserialize(MasterProcedureProtos.CreateTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    tableDescriptor = ProtobufUtil.toTableDescriptor(state.getTableSchema());
    if (state.getRegionInfoCount() == 0) {
      newRegions = null;
    } else {
      newRegions = new ArrayList<>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri : state.getRegionInfoList()) {
        newRegions.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    if (getTableName().isSystemTable()) {
      // Creating system table is part of the initialization, so only wait for meta loaded instead
      // of waiting for master fully initialized.
      return env.getAssignmentManager().waitMetaLoaded(this);
    }
    return super.waitInitialized(env);
  }

  private boolean prepareCreate(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("facfd728-e5bc-31e1-92c4-89b0b120e4d5"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("fcd8683d-31c6-355c-a9da-0feaf5265d6b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("64535f38-6312-3e24-bc49-1d431c073755"))) {
return false;
}
    final TableName tableName = getTableName();
    if (env.getMasterServices().getTableDescriptors().exists(tableName)) {
      setFailure("master-create-table", new TableExistsException(getTableName()));
      return false;
    }

    // check that we have at least 1 CF
    if (tableDescriptor.getColumnFamilyCount() == 0) {
      setFailure("master-create-table", new DoNotRetryIOException(
        "Table " + getTableName().toString() + " should have at least one column family."));
      return false;
    }

    int regionReplicationCount = tableDescriptor.getRegionReplication();
    if (regionReplicationCount > MAX_REGION_REPLICATION) {
      setFailure("master-create-table", new IllegalArgumentException(
        "Region Replication cannot exceed " + MAX_REGION_REPLICATION + "."));
      return false;
    }

    // check for store file tracker configurations
    StoreFileTrackerValidationUtils.checkForCreateTable(env.getMasterConfiguration(),
      tableDescriptor);

    return true;
  }

  private void preCreate(final MasterProcedureEnv env) throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("de7b0a4d-e96b-3d41-b021-67f9f67909a8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8834f92d-21c5-3e9e-85e2-00f31ab218db"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4d9c3045-edb9-327a-91bf-34f9fdd30dab"))) {
throw new InterruptedException("Injected exception");
}
    if (!getTableName().isSystemTable()) {
if(KnobRuntime.check(java.util.UUID.fromString("d15a5586-169d-3fe5-9a8e-0fb2d5c049e3"))) {
throw new java.io.IOException("Injected exception");
}
      ProcedureSyncWait.getMasterQuotaManager(env).checkNamespaceTableAndRegionQuota(getTableName(),
        (newRegions != null ? newRegions.size() : 0));
    }

    tableDescriptor = StoreFileTrackerFactory.updateWithTrackerConfigs(env.getMasterConfiguration(),
      tableDescriptor);

    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("b308954e-d198-32eb-850e-75e80a821f70"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("65586dd4-c22b-337e-9e58-f5d2294676d0"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
      final RegionInfo[] regions =
        newRegions == null ? null : newRegions.toArray(new RegionInfo[newRegions.size()]);
      cpHost.preCreateTableAction(tableDescriptor, regions, getUser());
    }
  }

  private void postCreate(final MasterProcedureEnv env) throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("40a8642b-be8e-3aef-b3af-44a793c9541e"))) ? ((cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3543a003-fb51-3751-abf6-a82c411dffa6"))) ? ((cpHost) != (null)) : (cpHost != null))))) {
      final RegionInfo[] regions =
        (newRegions == null) ? null : newRegions.toArray(new RegionInfo[newRegions.size()]);
      cpHost.postCompletedCreateTableAction(tableDescriptor, regions, getUser());
    }
  }

  protected interface CreateHdfsRegions {
    List<RegionInfo> createHdfsRegions(final MasterProcedureEnv env, final Path tableRootDir,
      final TableName tableName, final List<RegionInfo> newRegions) throws IOException;
  }

  protected static List<RegionInfo> createFsLayout(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final List<RegionInfo> newRegions) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5f0fc557-1dda-3a23-b9e4-77438d5b127a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1fe6e3c1-e2b3-3347-a235-214b2f900f67"))) {
return null;
}
    return createFsLayout(env, tableDescriptor, newRegions, new CreateHdfsRegions() {
      @Override
      public List<RegionInfo> createHdfsRegions(final MasterProcedureEnv env,
        final Path tableRootDir, final TableName tableName, final List<RegionInfo> newRegions)
        throws IOException {
        RegionInfo[] regions =
          newRegions != null ? newRegions.toArray(new RegionInfo[newRegions.size()]) : null;
        return ModifyRegionUtils.createRegions(env.getMasterConfiguration(), tableRootDir,
          tableDescriptor, regions, null);
      }
    });
  }

  protected static List<RegionInfo> createFsLayout(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, List<RegionInfo> newRegions,
    final CreateHdfsRegions hdfsRegionHandler) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();

    // 1. Create Table Descriptor
    // using a copy of descriptor, table will be created enabling first
    final Path tableDir =
      CommonFSUtils.getTableDir(mfs.getRootDir(), tableDescriptor.getTableName());
    ((FSTableDescriptors) (env.getMasterServices().getTableDescriptors()))
      .createTableDescriptorForTableDirectory(tableDir, tableDescriptor, false);

    // 2. Create Regions
    newRegions = hdfsRegionHandler.createHdfsRegions(env, mfs.getRootDir(),
      tableDescriptor.getTableName(), newRegions);

    return newRegions;
  }

  protected static List<RegionInfo> addTableToMeta(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final List<RegionInfo> regions) throws IOException {
    assert (regions != null && regions.size() > 0) : "expected at least 1 region, got " + regions;

    ProcedureSyncWait.waitMetaRegions(env);

    // Add replicas if needed
    // we need to create regions with replicaIds starting from 1
    List<RegionInfo> newRegions =
      RegionReplicaUtil.addReplicas(regions, 1, tableDescriptor.getRegionReplication());

    // Add regions to META
if(KnobRuntime.check(java.util.UUID.fromString("f885351b-6b08-31b0-b40e-901f56f2b927"))) {
throw new java.io.IOException("Injected exception");
}
    addRegionsToMeta(env, tableDescriptor, newRegions);

    // Setup replication for region replicas if needed
    if (tableDescriptor.getRegionReplication() > 1) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("cf7dbf0f-6588-3708-be24-2f1f3ce95eb3"))) {
throw new java.io.IOException("Injected exception");
}
        ServerRegionReplicaUtil.setupRegionReplicaReplication(env.getMasterServices());
      } catch (ReplicationException e) {
        throw new HBaseIOException(e);
      }
    }
    return newRegions;
  }

  protected static void setEnablingState(final MasterProcedureEnv env, final TableName tableName)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6609d910-f368-35fb-96a8-03db028485d2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("babb72c6-30f7-3629-b5ef-5293765e30a2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("342f15e1-6cad-3729-bbcc-17b3e7c2133d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e1683c4c-500e-32e4-9d91-58886c89d1dd"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("be3dd703-4c5e-3b3d-ac56-ea4c3ff92abf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d2285f2b-ae99-3f5e-aec1-ccf78fcb3209"))) {
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
    // Mark the table as Enabling
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.ENABLING);
  }

  protected static void setEnabledState(final MasterProcedureEnv env, final TableName tableName)
    throws IOException {
    // Enable table
if(KnobRuntime.check(java.util.UUID.fromString("77116e72-abe8-3005-b7a5-62677ef5c7eb"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5220296f-56ff-3b65-ad87-752fa144e92a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bd866529-d474-394e-bebd-c707282a2d60"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c1bdd5dc-be4a-3c19-89e6-a62ed6d364d4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("daf25e60-71ea-349b-a95d-e88e389222e0"))) {
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
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.ENABLED);
  }

  /**
   * Add the specified set of regions to the hbase:meta table.
   */
  private static void addRegionsToMeta(final MasterProcedureEnv env,
    final TableDescriptor tableDescriptor, final List<RegionInfo> regionInfos) throws IOException {
    MetaTableAccessor.addRegionsToMeta(env.getMasterServices().getConnection(), regionInfos,
      tableDescriptor.getRegionReplication());
  }

  @Override
  protected boolean shouldWaitClientAck(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("636ad97c-c502-3252-a5f6-7cde73b9c145"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("76a2de89-bd0c-3aa1-a628-eb73b339bad1"))) {
return false;
}
    // system tables are created on bootstrap internally by the system
    // the client does not know about this procedures.
    return !getTableName().isSystemTable();
  }

  RegionInfo getFirstRegionInfo() {
    if (newRegions == null || newRegions.isEmpty()) {
      return null;
    }
    return newRegions.get(0);
  }
}
