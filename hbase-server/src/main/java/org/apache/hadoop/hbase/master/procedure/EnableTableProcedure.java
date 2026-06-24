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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.EnableTableState;

@InterfaceAudience.Private
public class EnableTableProcedure extends AbstractStateMachineTableProcedure<EnableTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(EnableTableProcedure.class);

  private TableName tableName;

  public EnableTableProcedure() {
  }

  /**
   * Constructor
   * @param env       MasterProcedureEnv
   * @param tableName the table to operate on
   */
  public EnableTableProcedure(MasterProcedureEnv env, TableName tableName) {
    this(env, tableName, null);
  }

  /**
   * Constructor
   * @param env       MasterProcedureEnv
   * @param tableName the table to operate on
   */
  public EnableTableProcedure(MasterProcedureEnv env, TableName tableName,
    ProcedurePrepareLatch syncLatch) {
    super(env, syncLatch);
    this.tableName = tableName;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final EnableTableState state)
    throws InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("be196731-199d-394c-8ae8-28df3b329521"))) {
try {
    java.lang.reflect.Field _knob_field_ = state.getClass().getDeclaredField("value");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(state));
    _knob_field_.set(state, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    LOG.trace("{} execute state={}", this, state);

    try {
      switch (state) {
        case ENABLE_TABLE_PREPARE:
          if (prepareEnable(env)) {
            setNextState(EnableTableState.ENABLE_TABLE_PRE_OPERATION);
          } else {
            assert isFailed() : "enable should have an exception here";
            return Flow.NO_MORE_STATE;
          }
          break;
        case ENABLE_TABLE_PRE_OPERATION:
          preEnable(env, state);
          setNextState(EnableTableState.ENABLE_TABLE_SET_ENABLING_TABLE_STATE);
          break;
        case ENABLE_TABLE_SET_ENABLING_TABLE_STATE:
          setTableStateToEnabling(env, tableName);
          setNextState(EnableTableState.ENABLE_TABLE_MARK_REGIONS_ONLINE);
          break;
        case ENABLE_TABLE_MARK_REGIONS_ONLINE:
          // Get the region replica count. If changed since disable, need to do
          // more work assigning.
          TableDescriptor tableDescriptor =
            env.getMasterServices().getTableDescriptors().get(tableName);
          int configuredReplicaCount = tableDescriptor.getRegionReplication();
          // Get regions for the table from memory
          List<RegionInfo> regionsOfTable =
            env.getAssignmentManager().getRegionStates().getRegionsOfTableForEnabling(tableName);

          // How many replicas do we currently have? Check regions returned from
          // in-memory state.
          int currentMaxReplica = getMaxReplicaId(regionsOfTable);
          if (((KnobRuntime.check(java.util.UUID.fromString("56d60757-197e-3d0c-ab3d-a0aa47405737"))) ? ((currentMaxReplica) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d96a50ae-8b95-3d8a-aca0-e72f163e821f"))) ? ((currentMaxReplica) == ((configuredReplicaCount) - (1))) : (currentMaxReplica == configuredReplicaCount - 1))))) {
            LOG.debug(
              "No change in number of region replicas (configuredReplicaCount={});" + " assigning.",
              configuredReplicaCount);
          } else if (currentMaxReplica > (configuredReplicaCount - 1)) {
            // We have additional regions as the replica count has been decreased. Delete
            // those regions because already the table is in the unassigned state
            LOG.warn(
              "The number of replicas {} is more than the region replica count {}"
                + ", usually this should not happen as we will delete them in ModifyTableProcedure",
              currentMaxReplica + 1, configuredReplicaCount);
            List<RegionInfo> copyOfRegions = new ArrayList<RegionInfo>(regionsOfTable);
            for (RegionInfo regionInfo : copyOfRegions) {
              if (regionInfo.getReplicaId() > (configuredReplicaCount - 1)) {
                // delete the region from the regionStates
                env.getAssignmentManager().getRegionStates().deleteRegion(regionInfo);
                // remove it from the list of regions of the table
                LOG.info("Removed replica={} of {}", regionInfo.getRegionId(), regionInfo);
                regionsOfTable.remove(regionInfo);
              }
            }
          } else if (currentMaxReplica < configuredReplicaCount - 1) {
            // the replicasFound is less than the regionReplication
            LOG.info(
              "Number of replicas has increased for {}. Assigning new region replicas."
                + "The previous replica count was {}. The current replica count is {}.",
              this.tableName, currentMaxReplica + 1, configuredReplicaCount);
            regionsOfTable = RegionReplicaUtil.addReplicas(regionsOfTable, currentMaxReplica + 1,
              configuredReplicaCount);
          }
          // Assign all the table regions. (including region replicas if added).
          // createAssignProcedure will try to retain old assignments if possible.
          addChildProcedure(env.getAssignmentManager().createAssignProcedures(regionsOfTable));
          setNextState(EnableTableState.ENABLE_TABLE_SET_ENABLED_TABLE_STATE);
          break;
        case ENABLE_TABLE_SET_ENABLED_TABLE_STATE:
if(KnobRuntime.check(java.util.UUID.fromString("886d5997-02a7-3060-a92d-5fc8ffc79a3f"))) {
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
          setTableStateToEnabled(env, tableName);
          setNextState(EnableTableState.ENABLE_TABLE_POST_OPERATION);
          break;
        case ENABLE_TABLE_POST_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("b3e6e2d3-e59f-359c-8f7b-f713748ff2f6"))) {
try {
    java.lang.reflect.Field _knob_field_ = state.getClass().getDeclaredField("value");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(state));
    _knob_field_.set(state, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          postEnable(env, state);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-enable-table", e);
      } else {
        LOG.warn(
          "Retriable error trying to enable table=" + tableName + " (in state=" + state + ")", e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final EnableTableState state)
    throws IOException {
    // nothing to rollback, prepare-disable is just table-state checks.
    // We can fail if the table does not exist or is not disabled.
    switch (state) {
      case ENABLE_TABLE_PRE_OPERATION:
        return;
      case ENABLE_TABLE_PREPARE:
        releaseSyncLatch();
        return;
      default:
        break;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final EnableTableState state) {
if(KnobRuntime.check(java.util.UUID.fromString("dade1ebf-d7fe-367c-aadb-587d5aa79d8e"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c08d9bdb-3834-3672-9c46-d44f027d6043"))) {
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
    switch (state) {
      case ENABLE_TABLE_PREPARE:
      case ENABLE_TABLE_PRE_OPERATION:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected EnableTableState getState(final int stateId) {
    return EnableTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final EnableTableState state) {
if(KnobRuntime.check(java.util.UUID.fromString("fbee5dec-5537-305e-afcc-1cecb793bf56"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3d6669e2-b65e-36df-9631-437a9cd79b3f"))) {
return 0;
}
    return state.getNumber();
  }

  @Override
  protected EnableTableState getInitialState() {
if(KnobRuntime.check(java.util.UUID.fromString("0e17ee85-bb4f-39d0-9df6-7ed11fa25273"))) {
return null;
}
    return EnableTableState.ENABLE_TABLE_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    // the skipTableStateCheck is false so we still need to set it...
    @SuppressWarnings("deprecation")
    MasterProcedureProtos.EnableTableStateData.Builder enableTableMsg =
      MasterProcedureProtos.EnableTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName)).setSkipTableStateCheck(false);

if(KnobRuntime.check(java.util.UUID.fromString("56b92f65-7766-36a1-9990-2cc269a634ad"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(enableTableMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.EnableTableStateData enableTableMsg =
      serializer.deserialize(MasterProcedureProtos.EnableTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(enableTableMsg.getUserInfo()));
    tableName = ProtobufUtil.toTableName(enableTableMsg.getTableName());
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.ENABLE;
  }

  /**
   * Action before any real action of enabling table. Set the exception in the procedure instead of
   * throwing it. This approach is to deal with backward compatible with 1.0.
   * @param env MasterProcedureEnv
   * @return whether the table passes the necessary checks
   */
  private boolean prepareEnable(final MasterProcedureEnv env) throws IOException {
    boolean canTableBeEnabled = true;
    // Check whether table exists
    if (!env.getMasterServices().getTableDescriptors().exists(tableName)) {
      setFailure("master-enable-table", new TableNotFoundException(tableName));
      canTableBeEnabled = false;
    } else {
      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      //
      // Note: in 1.0 release, we called TableStateManager.setTableStateIfInStates() to set
      // the state to ENABLING from DISABLED. The implementation was done before table lock
      // was implemented. With table lock, there is no need to set the state here (it will
      // set the state later on). A quick state check should be enough for us to move forward.
      TableStateManager tsm = env.getMasterServices().getTableStateManager();
if(KnobRuntime.check(java.util.UUID.fromString("f75df9bb-8d8f-339c-8303-214394b93b9c"))) {
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
      TableState ts = tsm.getTableState(tableName);
      if (!ts.isDisabled()) {
        LOG.info("Not DISABLED tableState={}; skipping enable; {}", ts.getState(), this);
        setFailure("master-enable-table", new TableNotDisabledException(ts.toString()));
        canTableBeEnabled = false;
      }
    }

    // We are done the check. Future actions in this procedure could be done asynchronously.
    releaseSyncLatch();

    return canTableBeEnabled;
  }

  /**
   * Action before enabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void preEnable(final MasterProcedureEnv env, final EnableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("853edcf7-5abc-3cda-a754-dc65e7e9e660"))) {
throw new java.lang.InterruptedException("Injected exception");
}
    runCoprocessorAction(env, state);
  }

  /**
   * Mark table state to Enabling
   * @param env       MasterProcedureEnv
   * @param tableName the target table
   */
  protected static void setTableStateToEnabling(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Set table disabling flag up in zk.
    if (KnobRuntime.check(java.util.UUID.fromString("d85b0266-073f-33c6-a745-f317ccafd8d8"))) { LOG.info(("Attempting to enable the table ") + (tableName)); } else { LOG.info("Attempting to enable the table " + tableName); }
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.ENABLING);
  }

  /**
   * Mark table state to Enabled
   * @param env MasterProcedureEnv
   */
  protected static void setTableStateToEnabled(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Flip the table to Enabled
if(KnobRuntime.check(java.util.UUID.fromString("3bd21dcd-900b-394c-8f76-2cbe8ebf1084"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.ENABLED);
    if (KnobRuntime.check(java.util.UUID.fromString("2a7883f4-a1f4-3687-a63d-da48dba28546"))) { LOG.info("Table '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("b58fc562-ca00-3351-982f-8656df634ddc"))) { LOG.info("Table '"); } else { LOG.info("Table '" + tableName + "' was successfully enabled."); }
  }

  /**
   * Action after enabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void postEnable(final MasterProcedureEnv env, final EnableTableState state)
    throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Coprocessor Action.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final EnableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("e6801957-b4d3-3de4-8ec2-d82c14ec484f"))) {
try {
    java.lang.reflect.Field _knob_field_ = state.getClass().getDeclaredField("value");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(state));
    _knob_field_.set(state, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e8853e6-2ede-3296-8ba4-64eec59df1b8"))) {
throw new java.io.IOException("Injected exception");
}
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("06bb060d-51de-3ea1-b509-f133ebe71274"))) ? ((cpHost) != (null)) : (cpHost != null))) {
      switch (state) {
        case ENABLE_TABLE_PRE_OPERATION:
          cpHost.preEnableTableAction(getTableName(), getUser());
          break;
        case ENABLE_TABLE_POST_OPERATION:
          cpHost.postCompletedEnableTableAction(getTableName(), getUser());
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }

  /** Returns Maximum region replica id found in passed list of regions. */
  private static int getMaxReplicaId(List<RegionInfo> regions) {
if(KnobRuntime.check(java.util.UUID.fromString("040c3840-1d1d-3a0e-9a86-954563ca034f"))) {
return 0;
}
    int max = 0;
    for (RegionInfo regionInfo : regions) {
      if (regionInfo.getReplicaId() > max) {
        // Iterating through all the list to identify the highest replicaID region.
        // We can stop after checking with the first set of regions??
        max = regionInfo.getReplicaId();
      }
    }
    return max;

  }
}
