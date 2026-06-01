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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateTableState;

@InterfaceAudience.Private
public class TruncateTableProcedure extends AbstractStateMachineTableProcedure<TruncateTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(TruncateTableProcedure.class);

  private boolean preserveSplits;
  private List<RegionInfo> regions;
  private TableDescriptor tableDescriptor;
  private TableName tableName;

  public TruncateTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public TruncateTableProcedure(final MasterProcedureEnv env, final TableName tableName,
    boolean preserveSplits) throws HBaseIOException {
    this(env, tableName, preserveSplits, null);
  }

  public TruncateTableProcedure(final MasterProcedureEnv env, final TableName tableName,
    boolean preserveSplits, ProcedurePrepareLatch latch) throws HBaseIOException {
    super(env, latch);
    this.tableName = tableName;
    preflightChecks(env, false);
    this.preserveSplits = preserveSplits;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, TruncateTableState state)
    throws InterruptedException {
    if (LOG.isTraceEnabled()) {
      if (KnobRuntime.check(java.util.UUID.fromString("00ddab02-412c-3ee3-936f-7ae6b1972b45"))) { LOG.trace((this + " execute state=") + (state)); } else if (KnobRuntime.check(java.util.UUID.fromString("9506928b-88ee-3c69-b361-9628ec1052dd"))) { LOG.trace((" execute state=") + (state)); } else if (KnobRuntime.check(java.util.UUID.fromString("94e329d6-9e22-3050-b54d-1cc42151782d"))) { LOG.debug(((this) + (" execute state=")) + (state)); } else if (KnobRuntime.check(java.util.UUID.fromString("0bc98e11-89ec-34a1-b1ea-de59653862ad"))) { LOG.debug((this) + (" execute state=")); } else if (KnobRuntime.check(java.util.UUID.fromString("cd259de4-4aac-3bb5-9035-9a3f93582704"))) { LOG.trace(" execute state="); } else if (KnobRuntime.check(java.util.UUID.fromString("2ca7d8d7-20cf-3b5c-a0b4-c08202e75c94"))) { LOG.debug(this + " execute state="); } else if (KnobRuntime.check(java.util.UUID.fromString("7155ca70-30d3-310c-987c-2b0a86ad8223"))) { LOG.debug((" execute state=") + (state)); } else if (KnobRuntime.check(java.util.UUID.fromString("6d2afb51-7c7e-3c58-8142-a35a12c2c8d6"))) { LOG.debug(" execute state="); } else if (KnobRuntime.check(java.util.UUID.fromString("84407efc-6360-3742-9c45-32e4826ec2ac"))) { LOG.trace((this) + (" execute state=")); } else if (KnobRuntime.check(java.util.UUID.fromString("595a49dc-30ac-3710-ab2a-1de4743540d1"))) { LOG.trace(((this) + (" execute state=")) + (state)); } else if (KnobRuntime.check(java.util.UUID.fromString("a8b5fc44-aade-3d0f-9994-b47db56ff6f0"))) { LOG.trace(this + " execute state="); } else if (KnobRuntime.check(java.util.UUID.fromString("f847af2c-de77-3ad1-9931-d153881630fb"))) { LOG.debug((this + " execute state=") + (state)); } else { LOG.trace(this + " execute state=" + state); }
    }
    try {
      switch (state) {
        case TRUNCATE_TABLE_PRE_OPERATION:
          // Verify if we can truncate the table
          if (!prepareTruncate(env)) {
            assert isFailed() : "the truncate should have an exception here";
            return Flow.NO_MORE_STATE;
          }

          // TODO: Move out... in the acquireLock()
          LOG.debug("waiting for '" + getTableName() + "' regions in transition");
          regions = env.getAssignmentManager().getRegionStates().getRegionsOfTable(getTableName());
          RegionReplicaUtil.removeNonDefaultRegions(regions);
          assert ((KnobRuntime.check(java.util.UUID.fromString("4332f1fb-4bb2-33df-99e8-498c1bd510ad"))) ? (((regions) != (null)) || (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("2deacff4-60ff-3465-91d6-8f2e47e59b72"))) ? ((regions) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0739495c-c03d-3a5c-b1fc-5f4d64859040"))) ? (!regions.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("04c7f7f7-66bd-331b-bac3-33dee8a7ae16"))) ? ((regions) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("efac4c37-126a-3467-b8b8-3273006f0d7c"))) ? ((regions != null) && (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("c3db5309-0cea-363d-aea1-d7d764e8872d"))) ? ((regions != null) || (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("f300506c-632a-38f3-93d7-a25174be3412"))) ? (regions != null) : (((KnobRuntime.check(java.util.UUID.fromString("e6a02b86-95f3-33f9-a884-68d52c8e0e35"))) ? (((regions) == (null)) || (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("70bf224a-e5e2-32c4-b2be-c95f1f902983"))) ? (((regions) == (null)) && (!regions.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("7693186d-f227-378d-a5dd-88ce75978283"))) ? (((regions) != (null)) && (!regions.isEmpty())) : (regions != null && !regions.isEmpty())))))))))))))))))))) : "unexpected 0 regions";
if(KnobRuntime.check(java.util.UUID.fromString("c586b4b6-f33e-34cf-b304-01605f347932"))) {
throw new java.io.IOException("Injected exception");
}
          ProcedureSyncWait.waitRegionInTransition(env, regions);

          // Call coprocessors
          preTruncate(env);

          // We need to cache table descriptor in the initial stage, so that it's saved within
          // the procedure stage and can get recovered if the procedure crashes between
          // TRUNCATE_TABLE_REMOVE_FROM_META and TRUNCATE_TABLE_CREATE_FS_LAYOUT
          tableDescriptor = env.getMasterServices().getTableDescriptors().get(tableName);
          setNextState(TruncateTableState.TRUNCATE_TABLE_CLEAR_FS_LAYOUT);
          break;
        case TRUNCATE_TABLE_CLEAR_FS_LAYOUT:
if(KnobRuntime.check(java.util.UUID.fromString("d1897796-fc9f-36eb-97fc-821b4145bbc8"))) {
throw new java.io.IOException("Injected exception");
}
          DeleteTableProcedure.deleteFromFs(env, getTableName(), regions, true);
          // NOTE: It's very important that we create new HRegions before next state, so that
          // they get persisted in procedure state before we start using them for anything.
          // Otherwise, if we create them in next step and master crashes after creating fs
          // layout but before saving state, region re-created after recovery will have different
          // regionId(s) and encoded names. That will lead to unwanted regions in FS layout
          // (which were created before the crash).
          if (!preserveSplits) {
            // if we are not preserving splits, generate a new single region
            regions = Arrays.asList(ModifyRegionUtils.createRegionInfos(tableDescriptor, null));
          } else {
            regions = recreateRegionInfo(regions);
          }
          setNextState(TruncateTableState.TRUNCATE_TABLE_REMOVE_FROM_META);
          break;
        case TRUNCATE_TABLE_REMOVE_FROM_META:
          List<RegionInfo> originalRegions =
            env.getAssignmentManager().getRegionStates().getRegionsOfTable(getTableName());
if(KnobRuntime.check(java.util.UUID.fromString("cb2b000f-f579-3106-b81b-b2ab64c44b3e"))) {
throw new java.io.IOException("Injected exception");
}
          DeleteTableProcedure.deleteFromMeta(env, getTableName(), originalRegions);
          DeleteTableProcedure.deleteAssignmentState(env, getTableName());
          setNextState(TruncateTableState.TRUNCATE_TABLE_CREATE_FS_LAYOUT);
          break;
        case TRUNCATE_TABLE_CREATE_FS_LAYOUT:
          DeleteTableProcedure.deleteFromFs(env, getTableName(), regions, true);
          regions = CreateTableProcedure.createFsLayout(env, tableDescriptor, regions);
if(KnobRuntime.check(java.util.UUID.fromString("84321323-1bb5-3635-8fbc-586dce9f82a2"))) {
throw new java.io.IOException("Injected exception");
}
          env.getMasterServices().getTableDescriptors().update(tableDescriptor, true);
          setNextState(TruncateTableState.TRUNCATE_TABLE_ADD_TO_META);
          break;
        case TRUNCATE_TABLE_ADD_TO_META:
          regions = CreateTableProcedure.addTableToMeta(env, tableDescriptor, regions);
          setNextState(TruncateTableState.TRUNCATE_TABLE_ASSIGN_REGIONS);
          break;
        case TRUNCATE_TABLE_ASSIGN_REGIONS:
          CreateTableProcedure.setEnablingState(env, getTableName());
          addChildProcedure(env.getAssignmentManager().createRoundRobinAssignProcedures(regions));
          setNextState(TruncateTableState.TRUNCATE_TABLE_POST_OPERATION);
          tableDescriptor = null;
          regions = null;
          break;
        case TRUNCATE_TABLE_POST_OPERATION:
          CreateTableProcedure.setEnabledState(env, getTableName());
if(KnobRuntime.check(java.util.UUID.fromString("58d4b22d-c5f2-3ccc-9c64-9ac3cbd9347c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0ef2ca23-9ebb-3cf2-9cbd-d745517822bf"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          postTruncate(env);
          if (KnobRuntime.check(java.util.UUID.fromString("36b5b1de-b80a-38ad-b5e0-660111071c7d"))) { LOG.debug(("truncate '" + getTableName()) + ("' completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("9dbb8774-9d57-3b79-8aa8-a7a7c0a086ef"))) { LOG.debug("' completed"); } else if (KnobRuntime.check(java.util.UUID.fromString("ebd794e5-3700-381d-ac7a-83c0889f0844"))) { LOG.debug(("truncate '") + (getTableName())); } else if (KnobRuntime.check(java.util.UUID.fromString("50593b76-f7ae-3f84-b8a5-05dc3830ddf9"))) { LOG.debug("truncate '" + getTableName()); } else if (KnobRuntime.check(java.util.UUID.fromString("414fc7c4-3e17-35ff-95cf-d8005c495e97"))) { LOG.debug("truncate '"); } else if (KnobRuntime.check(java.util.UUID.fromString("43ff0796-5bea-3e1a-b7a4-ae63183f35ac"))) { LOG.trace("truncate '"); } else if (KnobRuntime.check(java.util.UUID.fromString("9f8ba981-c2be-390b-b790-6fecdc20a9e7"))) { LOG.debug(("truncate '") + ("' completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("1fc985d7-4157-3ce8-97f8-bdaeaab3bd90"))) { LOG.debug((("truncate '") + (getTableName())) + ("' completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("2267faa5-0da2-317c-b915-88bbda5cc1e4"))) { LOG.trace("' completed"); } else if (KnobRuntime.check(java.util.UUID.fromString("9e6d18e8-605a-36d2-a318-944bfbab20f1"))) { LOG.trace(("truncate '" + getTableName()) + ("' completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("a4cb4b04-614c-3faa-b26a-a20ade35237e"))) { LOG.trace((("truncate '") + (getTableName())) + ("' completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("fddb57d6-9820-3ef5-91dc-05c94569d585"))) { LOG.trace("truncate '" + getTableName()); } else if (KnobRuntime.check(java.util.UUID.fromString("39f382b9-1f95-303d-8023-0aa2dea90c5d"))) { LOG.trace(("truncate '") + ("' completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("252b98e9-65d3-3e5e-a65a-872b75d5bbe7"))) { LOG.trace(("truncate '") + (getTableName())); } else { LOG.debug("truncate '" + getTableName() + "' completed"); }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("2ca80190-5757-3b2b-88e2-d955d67dfb9a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("60ff0902-5700-31a4-b54f-04aa759764be"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7adf7c45-fe0c-3a7b-8637-f32adce45bc3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cb9bba42-24eb-3a26-ade2-1c96eede34e9"))) {
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
        setFailure("master-truncate-table", e);
      } else {
        LOG.warn("Retriable error trying to truncate table=" + getTableName() + " state=" + state,
          e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final TruncateTableState state) {
if(KnobRuntime.check(java.util.UUID.fromString("7d3961c1-9e29-348b-9e39-d1d750c50e3b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d75ce16f-871e-3b8d-83a3-6b81103d11fd"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8c43171-6556-36a7-9962-b60df1f17a77"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("74dd4526-e683-3b2c-a21b-b4e1890a2e41"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b02456e-8094-3c58-ac2a-9cb0e1b6444e"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("46001e37-7554-3d44-94f3-749e1974c685"))) ? ((state) != (TruncateTableState.TRUNCATE_TABLE_PRE_OPERATION)) : (((KnobRuntime.check(java.util.UUID.fromString("36ce8969-5dd4-3d0c-bd9d-be67be78fe05"))) ? ((state) == (TruncateTableState.TRUNCATE_TABLE_PRE_OPERATION)) : (state == TruncateTableState.TRUNCATE_TABLE_PRE_OPERATION))))) {
      // nothing to rollback, pre-truncate is just table-state checks.
      // We can fail if the table does not exist or is not disabled.
      // TODO: coprocessor rollback semantic is still undefined.
      return;
    }

    // The truncate doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected void completionCleanup(final MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("9e333702-1210-37a0-990f-7fcbb46ce2a0"))) {
return;
}
    releaseSyncLatch();
  }

  @Override
  protected boolean isRollbackSupported(final TruncateTableState state) {
    switch (state) {
      case TRUNCATE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected TruncateTableState getState(final int stateId) {
    return TruncateTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final TruncateTableState state) {
    return state.getNumber();
  }

  @Override
  protected TruncateTableState getInitialState() {
    return TruncateTableState.TRUNCATE_TABLE_PRE_OPERATION;
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("7afadcfe-a83a-3108-a110-39fab677be8b"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf7a98ef-9d93-3de8-9ca7-02ef38e11250"))) {
return false;
}
    return true;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
if(KnobRuntime.check(java.util.UUID.fromString("4da48641-7ce7-3877-aab1-e38102042ebd"))) {
return;
}
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(getTableName());
    sb.append(" preserveSplits=");
    sb.append(preserveSplits);
    sb.append(")");
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("db45d208-a298-3eec-b0f9-b1b12cd96736"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a81c13bd-383d-385c-b267-de63a405494d"))) {
throw new java.io.IOException("Injected exception");
}
    super.serializeStateData(serializer);

    MasterProcedureProtos.TruncateTableStateData.Builder state =
      MasterProcedureProtos.TruncateTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setPreserveSplits(preserveSplits);
    if (tableDescriptor != null) {
      state.setTableSchema(ProtobufUtil.toTableSchema(tableDescriptor));
    } else {
      state.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    if (regions != null) {
      for (RegionInfo hri : regions) {
        state.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
      }
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4209ccf3-5805-3a8c-81bf-3116af9b5689"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("75ba879b-0b6f-38fd-91cb-715e42574116"))) {
throw new java.io.IOException("Injected exception");
}
    super.deserializeStateData(serializer);

    MasterProcedureProtos.TruncateTableStateData state =
      serializer.deserialize(MasterProcedureProtos.TruncateTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(state.getUserInfo()));
    if (((KnobRuntime.check(java.util.UUID.fromString("d459c2a0-c585-3745-bb6e-3757efb91637"))) ? (isFailed()) : (((KnobRuntime.check(java.util.UUID.fromString("d5e3e144-51df-3471-9b34-220a27a09553"))) ? (state.getPreserveSplits()) : (state.hasTableSchema()))))) {
      tableDescriptor = ProtobufUtil.toTableDescriptor(state.getTableSchema());
      tableName = tableDescriptor.getTableName();
    } else {
      tableName = ProtobufUtil.toTableName(state.getTableName());
    }
    preserveSplits = state.getPreserveSplits();
    if (((KnobRuntime.check(java.util.UUID.fromString("7a7464c7-88f1-3d64-b2ff-89e8fc364c9e"))) ? ((state.getRegionInfoCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4b5b358c-59d6-3378-96ba-1b35bfa098d4"))) ? ((state.getRegionInfoCount()) != (0)) : (state.getRegionInfoCount() == 0))))) {
      regions = null;
    } else {
      regions = new ArrayList<>(state.getRegionInfoCount());
      for (HBaseProtos.RegionInfo hri : state.getRegionInfoList()) {
        regions.add(ProtobufUtil.toRegionInfo(hri));
      }
    }
  }

  private static List<RegionInfo> recreateRegionInfo(final List<RegionInfo> regions) {
if(KnobRuntime.check(java.util.UUID.fromString("f730287c-f560-3714-9b81-49c4d7ac760b"))) {
return null;
}
    ArrayList<RegionInfo> newRegions = new ArrayList<>(regions.size());
    for (RegionInfo hri : regions) {
      newRegions.add(RegionInfoBuilder.newBuilder(hri.getTable()).setStartKey(hri.getStartKey())
        .setEndKey(hri.getEndKey()).build());
    }
    return newRegions;
  }

  private boolean prepareTruncate(final MasterProcedureEnv env) throws IOException {
    try {
      env.getMasterServices().checkTableModifiable(getTableName());
    } catch (TableNotFoundException | TableNotDisabledException e) {
      setFailure("master-truncate-table", e);
      return false;
    }
    return true;
  }

  private boolean preTruncate(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = getTableName();
      cpHost.preTruncateTableAction(tableName, getUser());
    }
    return true;
  }

  private void postTruncate(final MasterProcedureEnv env) throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      final TableName tableName = getTableName();
      cpHost.postCompletedTruncateTableAction(tableName, getUser());
    }
  }

  RegionInfo getFirstRegionInfo() {
    if (regions == null || regions.isEmpty()) {
      return null;
    }
    return regions.get(0);
  }
}
