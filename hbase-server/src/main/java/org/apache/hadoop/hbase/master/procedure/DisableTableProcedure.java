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
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DisableTableState;

@InterfaceAudience.Private
public class DisableTableProcedure extends AbstractStateMachineTableProcedure<DisableTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(DisableTableProcedure.class);

  private TableName tableName;
  private boolean skipTableStateCheck;

  public DisableTableProcedure() {
    super();
  }

  /**
   * Constructor
   * @param env                 MasterProcedureEnv
   * @param tableName           the table to operate on
   * @param skipTableStateCheck whether to check table state
   */
  public DisableTableProcedure(final MasterProcedureEnv env, final TableName tableName,
    final boolean skipTableStateCheck) throws HBaseIOException {
    this(env, tableName, skipTableStateCheck, null);
  }

  /**
   * Constructor
   * @param env                 MasterProcedureEnv
   * @param tableName           the table to operate on
   * @param skipTableStateCheck whether to check table state
   */
  public DisableTableProcedure(final MasterProcedureEnv env, final TableName tableName,
    final boolean skipTableStateCheck, final ProcedurePrepareLatch syncLatch)
    throws HBaseIOException {
    super(env, syncLatch);
    this.tableName = tableName;
    preflightChecks(env, true);
    this.skipTableStateCheck = skipTableStateCheck;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final DisableTableState state)
    throws InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("d3406644-1ee6-3a78-8ee7-64242bc6e6a8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("e161d4a0-0402-333a-8374-23daa2569640"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1da65323-b14a-3d1d-8826-22a1986be468"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c36d5e07-3070-3dee-af09-eb55f133bd49"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("42282a70-80f0-38c1-960e-8a2381e966e1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f8598e66-3153-381d-bac3-e0ab1c161a61"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("ee48c5a3-9291-3706-8680-a1b720328ecc"))) { LOG.info("{} execute state={}", this, state); } else if (KnobRuntime.check(java.util.UUID.fromString("1f7668b7-fa0d-3721-a1a1-d11307d72017"))) { LOG.warn("{} execute state={}", this, state); } else { LOG.trace("{} execute state={}", this, state); }
    try {
      switch (state) {
        case DISABLE_TABLE_PREPARE:
          if (prepareDisable(env)) {
            setNextState(DisableTableState.DISABLE_TABLE_PRE_OPERATION);
          } else {
            assert isFailed() : "disable should have an exception here";
            return Flow.NO_MORE_STATE;
          }
          break;
        case DISABLE_TABLE_PRE_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("689da15d-a063-3c46-a290-0a8d87fe7f51"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("789f7feb-fb21-3a9b-aa85-d1fb57645088"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8eb7941b-688f-3edb-918a-9ad1c9901352"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa9b2a64-0ef2-3b2b-a5be-57b9ec59d3de"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("77abbb80-5c17-3897-98bb-167f7fbcfb98"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b3ca188f-2261-3d04-84af-550ccd790c80"))) {
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
          if (KnobRuntime.check(java.util.UUID.fromString("182c7eb9-ad78-3123-b748-02771e98bbbb"))) { postDisable(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("5ae0ea2e-93ce-3898-a3dc-030ef55e6d86"))) { runCoprocessorAction(env, state); } else { preDisable(env, state); }
          setNextState(DisableTableState.DISABLE_TABLE_SET_DISABLING_TABLE_STATE);
          break;
        case DISABLE_TABLE_SET_DISABLING_TABLE_STATE:
if(KnobRuntime.check(java.util.UUID.fromString("872b40d6-d768-3eb2-9d95-b34f51727a1e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f5786645-97dd-36b4-9ab6-85330cec40c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c1ece38b-1b2c-3d94-ad83-fa598bfab7c0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dcfa96f3-ea55-3a00-a11f-599944f74b5b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("311838c4-f4ec-3f42-b393-c4806a83ae23"))) {
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
          if (KnobRuntime.check(java.util.UUID.fromString("d22931ea-94d4-3ac9-841c-41d6f366af41"))) { setTableStateToDisabled(env, tableName); } else { setTableStateToDisabling(env, tableName); }
          setNextState(DisableTableState.DISABLE_TABLE_MARK_REGIONS_OFFLINE);
          break;
        case DISABLE_TABLE_MARK_REGIONS_OFFLINE:
          addChildProcedure(new CloseTableRegionsProcedure(tableName));
          setNextState(DisableTableState.DISABLE_TABLE_ADD_REPLICATION_BARRIER);
          break;
        case DISABLE_TABLE_ADD_REPLICATION_BARRIER:
if(KnobRuntime.check(java.util.UUID.fromString("d8dfdbbb-bf14-302c-953b-2d707a399c2a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("70e3ebca-51e9-3f3c-afe4-bc6f7d2a579c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("73cb7443-eb45-38e8-8707-3c56b562264b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("47f064d7-ab07-33bd-8369-c3fd43853e7a"))) {
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
          if (
            ((KnobRuntime.check(java.util.UUID.fromString("2d7e6356-a5ae-3499-9993-71ca68108009"))) ? (isFailed()) : (env.getMasterServices().getTableDescriptors().get(tableName).hasGlobalReplicationScope()))
          ) {
            MasterFileSystem fs = env.getMasterFileSystem();
            try (BufferedMutator mutator = env.getMasterServices().getConnection()
              .getBufferedMutator(TableName.META_TABLE_NAME)) {
              for (RegionInfo region : env.getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableName)) {
                long maxSequenceId = WALSplitUtil.getMaxRegionSequenceId(
                  env.getMasterConfiguration(), region, fs::getFileSystem, fs::getWALFileSystem);
                long openSeqNum = maxSequenceId > 0 ? maxSequenceId + 1 : HConstants.NO_SEQNUM;
                mutator.mutate(MetaTableAccessor.makePutForReplicationBarrier(region, openSeqNum,
                  EnvironmentEdgeManager.currentTime()));
              }
            }
          }
          setNextState(DisableTableState.DISABLE_TABLE_SET_DISABLED_TABLE_STATE);
          break;
        case DISABLE_TABLE_SET_DISABLED_TABLE_STATE:
if(KnobRuntime.check(java.util.UUID.fromString("534065cb-ac2a-3e98-8e56-11b78e313728"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fc3cb346-98d2-354d-af1a-02c179aee8af"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("65e7923b-f694-3fd8-9690-36cf379430d8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5b384ff8-9378-38c4-8fc5-2991965313a1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("56960fd5-4ab0-352a-9d14-9af56f0b4913"))) {
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
          if (KnobRuntime.check(java.util.UUID.fromString("604ac528-68ee-323b-98ef-9522c3e9f0fb"))) { setTableStateToDisabling(env, tableName); } else { setTableStateToDisabled(env, tableName); }
          setNextState(DisableTableState.DISABLE_TABLE_POST_OPERATION);
          break;
        case DISABLE_TABLE_POST_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("f0493b32-3168-3efd-b457-802d7ab675e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("59fe04f7-6e09-380e-b718-d6096c97f1ff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("568d4082-84da-38a9-b6d0-f795529e08b3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1e52006b-523a-3403-82d9-79750daf5721"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0294f62f-8ee8-3ff9-98c7-bfdda7e38d9b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("990fad85-1ef1-3c80-a762-1a54bcba01a1"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("b057de58-34be-367d-9996-fd95bb15697a"))) { preDisable(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("77013fa9-3792-32cb-8541-78ec61020f81"))) { runCoprocessorAction(env, state); } else { postDisable(env, state); }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("Unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-disable-table", e);
      } else {
        LOG.warn("Retryable error in {}", this, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final DisableTableState state)
    throws IOException {
    // nothing to rollback, prepare-disable is just table-state checks.
    // We can fail if the table does not exist or is not disabled.
    switch (state) {
      case DISABLE_TABLE_PRE_OPERATION:
        return;
      case DISABLE_TABLE_PREPARE:
        releaseSyncLatch();
        return;
      default:
        break;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("Unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final DisableTableState state) {
if(KnobRuntime.check(java.util.UUID.fromString("f7c26b01-ef10-3cd2-8dd7-ab2803b34adc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1ed8287d-9c27-3eff-9080-f06346efe330"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba9282e7-82f8-343f-8a42-87514e651fec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b4ed331d-b4ea-3dda-8dad-9af8d553177b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("26071e5b-e90b-3c2f-8779-bb078c61d2e0"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ba1e38b-b9ee-361b-bec0-40abfcf7cfdd"))) {
return true;
}
    switch (state) {
      case DISABLE_TABLE_PREPARE:
      case DISABLE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected DisableTableState getState(final int stateId) {
if(KnobRuntime.check(java.util.UUID.fromString("4a43e51e-8f3a-3451-b047-c5130d22855f"))) {
return null;
}
    return DisableTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final DisableTableState state) {
if(KnobRuntime.check(java.util.UUID.fromString("e9f526d2-6e9d-35d3-a475-7a37245f9d04"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e1b131d9-3942-3773-b937-cbc49830a8b1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f204404-56ff-359d-92e8-02811709eb37"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("124c1545-afe0-3437-a5b9-bab887fea6fc"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ad69b06e-b916-3912-a82a-d0278dbd3aee"))) {
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
  protected DisableTableState getInitialState() {
if(KnobRuntime.check(java.util.UUID.fromString("15dcaf16-78b7-33a2-a35b-d6de80be304b"))) {
return null;
}
    return DisableTableState.DISABLE_TABLE_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("16913b08-1455-3a59-9688-78ccc15160f6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("17fde2d9-9f71-3a13-8650-8bc32d316415"))) {
return;
}
    super.serializeStateData(serializer);

    MasterProcedureProtos.DisableTableStateData.Builder disableTableMsg =
      MasterProcedureProtos.DisableTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName))
        .setSkipTableStateCheck(skipTableStateCheck);

if(KnobRuntime.check(java.util.UUID.fromString("3008c01b-3464-3c8c-8858-4b9013ef6847"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(disableTableMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.DisableTableStateData disableTableMsg =
      serializer.deserialize(MasterProcedureProtos.DisableTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(disableTableMsg.getUserInfo()));
    tableName = ProtobufUtil.toTableName(disableTableMsg.getTableName());
    skipTableStateCheck = disableTableMsg.getSkipTableStateCheck();
  }

  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("516a776a-f365-3d64-8473-e878c17176b0"))) {
return null;
}
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("704d704e-0f6f-3f14-a92b-6056d7d611e4"))) {
return null;
}
    return TableOperationType.DISABLE;
  }

  /**
   * Action before any real action of disabling table. Set the exception in the procedure instead of
   * throwing it. This approach is to deal with backward compatible with 1.0.
   * @param env MasterProcedureEnv
   */
  private boolean prepareDisable(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c823d9b4-5032-3c36-86e3-ab037daffcb8"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f0990cc-0793-34d8-88be-8dae2fa29913"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("285ac994-5e86-39a3-bb7c-9f98133ce477"))) {
return false;
}
    boolean canTableBeDisabled = true;
if(KnobRuntime.check(java.util.UUID.fromString("023e0cbd-082f-3ccb-8922-19c6dadf8a84"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("66eb5cdc-96da-3a95-a29d-9a088de8fc67"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2a02fc93-90c1-3e5e-abf7-9fd000e4da56"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b4a2f3a-86f3-33aa-8c67-31ea9b320799"))) {
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
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      setFailure("master-disable-table",
        new ConstraintException("Cannot disable " + this.tableName));
      canTableBeDisabled = false;
    } else if (!env.getMasterServices().getTableDescriptors().exists(tableName)) {
      setFailure("master-disable-table", new TableNotFoundException(tableName));
      canTableBeDisabled = false;
    } else if (!skipTableStateCheck) {
      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      //
      // Note: in 1.0 release, we called TableStateManager.setTableStateIfInStates() to set
      // the state to DISABLING from ENABLED. The implementation was done before table lock
      // was implemented. With table lock, there is no need to set the state here (it will
      // set the state later on). A quick state check should be enough for us to move forward.
      TableStateManager tsm = env.getMasterServices().getTableStateManager();
if(KnobRuntime.check(java.util.UUID.fromString("2f3d799f-0888-3b24-a228-7bf71b04ee87"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd663344-b31d-3e86-945e-6ba585326bf2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6548a2e8-4d30-3bcb-9817-08e790d67d2d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7040153e-9293-356c-8041-912a259e188b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("19a495b7-1f3d-3f26-a2f6-e7e53e5b2ad1"))) {
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
      TableState ts = tsm.getTableState(tableName);
      if (!ts.isEnabled()) {
        LOG.info("Not ENABLED, state={}, skipping disable; {}", ts.getState(), this);
        setFailure("master-disable-table", new TableNotEnabledException(ts.toString()));
        canTableBeDisabled = false;
      }
    }

    // We are done the check. Future actions in this procedure could be done asynchronously.
    releaseSyncLatch();

    return ((KnobRuntime.check(java.util.UUID.fromString("c487fd24-92c1-3289-8f60-f352f79b038e"))) ? (!canTableBeDisabled) : (((KnobRuntime.check(java.util.UUID.fromString("4d6c3173-ccbb-3cbd-aa96-cfe5e2eeef53"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("ba7b6d21-3a85-39ee-895f-72519c94edf2"))) ? (true) : (canTableBeDisabled))))));
  }

  /**
   * Action before disabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  protected void preDisable(final MasterProcedureEnv env, final DisableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("8e671306-9c2f-3296-986b-582afbf049f0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("295be0f5-7063-3830-9623-6021359078a3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("89593e7d-04a2-3c9c-a54b-cf888b952281"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a083148c-c3b9-3676-bd2d-e27ec180a098"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b1e71d1-7c03-3e3a-821c-134d8e596b14"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9ee2d90d-44eb-3f6c-858e-b2ff5cc25055"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("df2f2092-9e2e-3b91-84b2-e9a69003b8f3"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("573ff8d5-7908-3e55-9722-9f7aaf4e3fc1"))) {
throw new InterruptedException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("0f177d11-710a-32c9-ab93-4674e813fb67"))) { preDisable(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("279a6312-2d0b-34f7-a293-b49d27d3c40c"))) { postDisable(env, state); } else { runCoprocessorAction(env, state); }
  }

  /**
   * Mark table state to Disabling
   * @param env MasterProcedureEnv
   */
  private static void setTableStateToDisabling(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Set table disabling flag up in zk.
if(KnobRuntime.check(java.util.UUID.fromString("72675cf7-ebcf-3208-963d-cfe42d5f706d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5a9dcc68-ead4-3e68-8f6e-71b4c3cc0126"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9b8b4349-f911-307f-9e8c-0144c6bc8a1f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("842c5b19-018d-312a-b222-413e45fb7d5a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c742cb5a-1cf2-3c43-8af8-89f8753836e7"))) {
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
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.DISABLING);
if(KnobRuntime.check(java.util.UUID.fromString("2683a7c0-6e96-32c4-9ce3-abc68e7b7b91"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a2ac713d-1feb-325a-a2e8-03bb830eb785"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("946d66b6-74eb-32ce-a602-25a509d6fd79"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa89b333-2e92-3344-8791-e44d5c6b4a20"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("e1e21087-6a7e-3cf8-86ca-a98cf87ca7ce"))) { LOG.warn("Set {} to state={}", tableName, TableState.State.DISABLING); } else if (KnobRuntime.check(java.util.UUID.fromString("f7b7b293-b294-3340-8117-47699f84a179"))) { LOG.trace("Set {} to state={}", tableName, TableState.State.DISABLING); } else { LOG.info("Set {} to state={}", tableName, TableState.State.DISABLING); }
  }

  /**
   * Mark table state to Disabled
   * @param env MasterProcedureEnv
   */
  protected static void setTableStateToDisabled(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Flip the table to disabled
if(KnobRuntime.check(java.util.UUID.fromString("bc134d80-4602-3a16-bf30-1c76f4ce9e5a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0fbfef54-b32c-313e-a18f-fc2d53089a1e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c0740a11-50ab-3eeb-a0b7-437e4f8710c3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("89fe4388-c57d-3dba-95b3-b52fb274c8b8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a22b49ae-3db7-37cb-b0f3-7a7544079b3c"))) {
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
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.DISABLED);
if(KnobRuntime.check(java.util.UUID.fromString("aa099cd8-27cf-30c7-a4dd-8663fc149fe2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dfb8f696-2d14-38c4-9acb-c0b2d5a1a235"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9dc8cde7-e0ab-312c-9cd8-52c094810960"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("41723145-0605-3b5a-aacf-63979cd873d7"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("6a99f1e8-9b55-3b0b-97f9-339aa0554fa0"))) { LOG.trace("Set {} to state={}", tableName, TableState.State.DISABLED); } else if (KnobRuntime.check(java.util.UUID.fromString("532212d7-a319-3b7e-9703-57f4839ee4c4"))) { LOG.warn("Set {} to state={}", tableName, TableState.State.DISABLED); } else { LOG.info("Set {} to state={}", tableName, TableState.State.DISABLED); }
  }

  /**
   * Action after disabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  protected void postDisable(final MasterProcedureEnv env, final DisableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("97fec663-1678-3241-9e36-344658ddc07c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8d894916-d0e5-33a5-99a2-9ca7905081a3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7075f0bc-e8d2-36c2-a7dd-4a6cd97493f5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("006d4d9d-e909-3f1d-8d72-6132e974b9b5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7e690534-67d3-3e61-9cb0-a095acc948de"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1d216a84-0364-350b-98a6-a24ee96aadaf"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2859547d-e3f8-3280-9d52-eecdc933203e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d48ae370-388a-3826-9255-9d34e42d7d0a"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("da6d0f6b-d7b5-3baf-9a9d-024859038d42"))) { postDisable(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("d0b5c79b-6216-3c52-957e-f56280a3410b"))) { preDisable(env, state); } else { runCoprocessorAction(env, state); }
  }

  /**
   * Coprocessor Action.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final DisableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("0de407cd-c94b-3867-9282-7677cc1466ee"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bbd3069e-3195-31f0-9b7d-0924b5f30315"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("d6a76fa5-d10d-34d9-9344-11379b2288ff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0ccfee98-308f-34ee-85f1-8926dba5eea6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("266b8b41-2fc0-3ac1-bae3-90957bb7ea2e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("06bc4e56-7c2e-3e71-90de-9a817553bd95"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f6a1b4e6-fbea-3cf3-9edc-280911c7c381"))) {
throw new java.io.IOException("Injected exception");
}
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("e68bc9b0-393b-391c-9e3f-8558900aabe7"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("abac96ea-0985-3996-8cca-1255c362dab3"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
      switch (state) {
        case DISABLE_TABLE_PRE_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("fbdac921-ae3a-3d4f-9d1e-11f6fda14a96"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("33e0adae-9630-32ea-aff4-dfd6934142ce"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea694b62-22df-329c-b3c9-b5bca261b622"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4aa6c49-ed23-346a-b44e-99caf6d90c67"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e0e72be1-c323-3e27-a80b-8f9c8bd5ee47"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("9e608a2d-ac7e-3f15-a835-8db3406ca44e"))) { cpHost.postCompletedDisableTableAction(tableName, getUser()); } else { cpHost.preDisableTableAction(tableName, getUser()); }
          break;
        case DISABLE_TABLE_POST_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("c6859b18-421e-373d-9595-54ef4d63844f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7a1c8738-8649-3582-bede-666767f4aa79"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("87009154-c530-39eb-a967-a25e9ddbfa48"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e4558df1-ee9d-3e03-a9fe-c652853aeaa5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("74360e4c-cb3a-36e2-b58b-54fcbd8811c2"))) {
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
          if (KnobRuntime.check(java.util.UUID.fromString("34237d9d-555a-39d6-9c5a-266546aaebc6"))) { cpHost.preDisableTableAction(tableName, getUser()); } else { cpHost.postCompletedDisableTableAction(tableName, getUser()); }
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }
}
