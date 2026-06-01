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
if(KnobRuntime.check(java.util.UUID.fromString("329f650c-4bb2-3cf7-a671-a4852fb68bcd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("daf5561f-bd8c-38c8-ab75-1c53aa4a237c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1d692f4f-5464-32fc-b5bd-0c8eda33b2b2"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a615af86-1657-3eb4-9adc-12bd7b59ddda"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5ebb6e2f-6fda-358a-9d19-565052235d78"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6431bb4f-4fad-3966-bf18-4e906be8f924"))) {
throw new InterruptedException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("a58ea7a8-9e6c-3181-8ce5-024313e44667"))) { LOG.warn("{} execute state={}", this, state); } else if (KnobRuntime.check(java.util.UUID.fromString("34ca5722-0a53-344d-9e49-eb2d2187df54"))) { LOG.info("{} execute state={}", this, state); } else { LOG.trace("{} execute state={}", this, state); }

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
if(KnobRuntime.check(java.util.UUID.fromString("b9eda1d8-84c7-3ecf-9df1-d8b32a966e71"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c0dc7fbf-6df2-3152-8419-38e811648365"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("643fd66f-0381-37de-ab70-bb23cad18562"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4b7a25ff-933b-396c-8a45-cbd7105916eb"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("004781ba-7c4a-3b4b-a5b2-d4cbb27ee9f0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b4b986a9-3c1c-397e-a031-2476b2170d60"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("ef36fb8f-fa92-3e72-a22c-1a56ea71117b"))) { runCoprocessorAction(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("7e817096-d7ac-3558-afe5-f49530dc3fc5"))) { postEnable(env, state); } else { preEnable(env, state); }
          setNextState(EnableTableState.ENABLE_TABLE_SET_ENABLING_TABLE_STATE);
          break;
        case ENABLE_TABLE_SET_ENABLING_TABLE_STATE:
if(KnobRuntime.check(java.util.UUID.fromString("c6d2bb81-bb0b-38bb-b18e-bec46a049351"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0d78d765-3262-3c61-a144-2b8beff1604e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("804577ef-97f2-322a-a969-d7bc29192bac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6e5e2cac-e2c0-3a60-82f2-5699750879ec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ecc96db1-1009-3624-82fe-b7faabe7342b"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("a8e73364-f7a2-3e31-84f2-408a2d0b17f2"))) { setTableStateToEnabled(env, tableName); } else { setTableStateToEnabling(env, tableName); }
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
          if (((KnobRuntime.check(java.util.UUID.fromString("fe7d6ea8-edd8-38fe-a3e6-f9a68ba86474"))) ? ((currentMaxReplica) != (configuredReplicaCount - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("d96a50ae-8b95-3d8a-aca0-e72f163e821f"))) ? ((currentMaxReplica) == ((configuredReplicaCount) - (1))) : (((KnobRuntime.check(java.util.UUID.fromString("210aab49-7b16-32b7-a9dc-03f488bbf255"))) ? ((currentMaxReplica) == (configuredReplicaCount)) : (((KnobRuntime.check(java.util.UUID.fromString("da448d0a-50d9-370a-8b11-4c8d9002e836"))) ? ((currentMaxReplica) != ((configuredReplicaCount) - (1))) : (((KnobRuntime.check(java.util.UUID.fromString("fa6489a7-19bb-38f7-bd3b-35718695703e"))) ? ((currentMaxReplica) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f560512c-e8f2-3dfe-8d9e-a5bba518f001"))) ? ((currentMaxReplica) == (configuredReplicaCount - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("56d60757-197e-3d0c-ab3d-a0aa47405737"))) ? ((currentMaxReplica) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("caa634c1-ea61-33e3-9a90-4653cfdb63ea"))) ? ((currentMaxReplica) != (configuredReplicaCount)) : (currentMaxReplica == configuredReplicaCount - 1))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("a4061445-e926-3e28-a17c-95e1e7c995d6"))) {
configuredReplicaCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7385638f-64da-3e02-bcac-055202cc29ba"))) {
configuredReplicaCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e135302d-9e90-3c00-9156-c0bbbc364bd9"))) {
configuredReplicaCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("863b6169-32ab-3226-b8b3-3aced4a7185a"))) {
configuredReplicaCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("df8c0897-c694-3abc-b5c5-c253688b70ff"))) {
configuredReplicaCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e90a12ea-321d-3004-b03e-187afabd5bc3"))) {
configuredReplicaCount -= 1;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("45ae1471-a5a3-327f-8d57-82c6d5e0a238"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("379b3e7c-91ea-3fc1-b06e-8200858a9440"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f672d417-88af-3a50-9234-6171fc00ed3f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4fe629d1-4674-3d42-b40d-b23c242f14f6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("059ef017-3c50-3bce-bb68-7548328ef279"))) {
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
          if (KnobRuntime.check(java.util.UUID.fromString("ee989f2e-5187-3915-b64a-06df20c31c0b"))) { setTableStateToEnabling(env, tableName); } else { setTableStateToEnabled(env, tableName); }
          setNextState(EnableTableState.ENABLE_TABLE_POST_OPERATION);
          break;
        case ENABLE_TABLE_POST_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("3d51fb46-a9d0-35e0-af05-99f07ad3ba5c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eb5cdc3a-deda-3eff-8aff-ecb634071991"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b7d26e30-fc7b-3790-a4a5-6187029e8e2f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("292d5121-5aa0-3d33-94fa-f3902bf60bc9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f2d0af38-2931-3db2-828a-3d124c9c1c88"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("11128930-c0a9-3db2-b162-99f097d9ddcb"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("af93483e-39a4-3889-a321-15efe128a9a5"))) { preEnable(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("29ed3ef1-8861-3c74-a90f-1650479e0f24"))) { runCoprocessorAction(env, state); } else { postEnable(env, state); }
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
if(KnobRuntime.check(java.util.UUID.fromString("1fb822ee-ac82-3f5f-8664-c3edb9094884"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4c82381-4fc6-3c97-ba19-35712cc7cf9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("62013dd1-db2d-3d25-8cbb-9bc18f33a11b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dade1ebf-d7fe-367c-aadb-587d5aa79d8e"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b590c41-8124-3f28-b89f-0c3bca2f2e89"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3ea9246f-4466-3654-b6f2-102a34b71a3d"))) {
return false;
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
if(KnobRuntime.check(java.util.UUID.fromString("c6232127-92e9-3177-9a63-ce93fb95d7e3"))) {
return null;
}
    return EnableTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final EnableTableState state) {
if(KnobRuntime.check(java.util.UUID.fromString("c2e59c78-db1e-31cb-af39-57e24afba0f1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6faeef33-3a72-3a25-b5c4-5771b278e443"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7351458f-6982-308d-bcf6-4c998fb147b0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3d6669e2-b65e-36df-9631-437a9cd79b3f"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("84b41525-8f20-31a6-bd30-557b15b4609e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d8fc3ee9-81fa-3054-82b2-f0a740fad9f4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("807c0785-a04c-3248-b0ff-b5813a84745a"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("32cc0eb7-6e89-3ece-940d-4ec8be49693a"))) {
return null;
}
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("d80bb13c-8e21-3160-8961-afc96466b38c"))) {
return null;
}
    return TableOperationType.ENABLE;
  }

  /**
   * Action before any real action of enabling table. Set the exception in the procedure instead of
   * throwing it. This approach is to deal with backward compatible with 1.0.
   * @param env MasterProcedureEnv
   * @return whether the table passes the necessary checks
   */
  private boolean prepareEnable(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a77e3ce6-730d-363b-b544-ee7a50673425"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("865145c9-7943-3ea6-b5b9-a2b49203d3ac"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5bce7ebe-1ea3-3bcf-9f86-f7d72e141542"))) {
return true;
}
    boolean canTableBeEnabled = true;
    // Check whether table exists
if(KnobRuntime.check(java.util.UUID.fromString("a537f145-4d12-3c0f-b9be-3ef6f15a05c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79a17ba5-9c5e-300f-a72a-abfc406165f5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eaf2fc20-fa6c-3653-8ea8-422dbf344b2b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8633e066-3bf3-378e-b679-0979acd422ee"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("037c8c78-1cfc-3ccd-be2d-677d45fc3914"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("977149f0-77c6-3451-a767-260c39b2a1d8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f758bec5-f7a6-3000-ab4b-b08f604e72a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("add11bf0-0a21-3b56-b359-0f963aa9cafb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("96c34193-1327-30b2-834a-9472e1532131"))) {
throw new java.io.IOException("Injected exception");
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

    return ((KnobRuntime.check(java.util.UUID.fromString("c7c9e9b3-b09b-3038-b39c-ba0e6786d443"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("806f40fe-f76d-37eb-a9d3-d8a7a8229c79"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("757a91d3-5d3c-3d4a-8235-8da8b581db70"))) ? (!canTableBeEnabled) : (canTableBeEnabled))))));
  }

  /**
   * Action before enabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void preEnable(final MasterProcedureEnv env, final EnableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("2abec4ca-6adb-30ed-8653-ae065cba86d3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("644b9c56-86cf-3c12-be84-a4d3672d553e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("853edcf7-5abc-3cda-a754-dc65e7e9e660"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("963fb52a-b88a-3720-ba6c-10dbaee8a07d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("70757988-3bed-3a25-9786-6c1745cb572e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b8da4585-b3f7-35a1-a5c3-b2da73660b48"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ebebfd68-4b5b-3650-81c8-0f8e605cf83a"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("82c7501a-1a59-3dae-b58a-8e0d1625f52b"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("c7ceabbe-8204-3a08-8a5f-10e1e4d90ced"))) { postEnable(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("fd21bc11-ea93-3e5f-86f2-3c39c1257f88"))) { preEnable(env, state); } else { runCoprocessorAction(env, state); }
  }

  /**
   * Mark table state to Enabling
   * @param env       MasterProcedureEnv
   * @param tableName the target table
   */
  protected static void setTableStateToEnabling(final MasterProcedureEnv env,
    final TableName tableName) throws IOException {
    // Set table disabling flag up in zk.
    if (KnobRuntime.check(java.util.UUID.fromString("d85b0266-073f-33c6-a745-f317ccafd8d8"))) { LOG.info(("Attempting to enable the table ") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("d40de1d9-c6d0-31bb-93db-02261d108f55"))) { LOG.info("Attempting to enable the table "); } else { LOG.info("Attempting to enable the table " + tableName); }
if(KnobRuntime.check(java.util.UUID.fromString("e8419899-f1c7-3a68-b34a-f42867b42f9a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("929a3e10-1bcb-3c04-a99f-7e6378e4eb5b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("068e1120-c38f-3be2-8820-a866e2ea9ec4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2f696611-afdb-34ae-9796-60da86fb6686"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3dc516c8-0b2d-3a11-b82a-9a33b85c1500"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("74d3bfaf-e0d2-386a-aebb-f064ca57ac9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c07d192e-aeea-3d9d-a856-c37438d06342"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ca698b4d-f01a-357b-83cc-81710a7bf405"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("97962e14-cdfb-321b-b7a0-84a97f1a4a0e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4553aa4c-83b8-3b33-97ee-f593435c355a"))) {
throw new java.io.IOException("Injected exception");
}
    env.getMasterServices().getTableStateManager().setTableState(tableName,
      TableState.State.ENABLED);
    if (KnobRuntime.check(java.util.UUID.fromString("dbe08c83-923d-38b1-9202-58a91cf2f3c3"))) { LOG.info(("Table '") + ("' was successfully enabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("b58fc562-ca00-3351-982f-8656df634ddc"))) { LOG.info("Table '"); } else if (KnobRuntime.check(java.util.UUID.fromString("94a1ad7b-ca35-38df-bdc2-84f65a370874"))) { LOG.info(("Table '" + tableName) + ("' was successfully enabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("2713196f-5ace-31be-858c-658a931c9141"))) { LOG.info(("Table '") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("288d12da-9d28-352e-92db-b40ba2562af6"))) { LOG.info("' was successfully enabled."); } else if (KnobRuntime.check(java.util.UUID.fromString("2a7883f4-a1f4-3687-a63d-da48dba28546"))) { LOG.info("Table '" + tableName); } else if (KnobRuntime.check(java.util.UUID.fromString("1fe3db1c-4b30-30f5-9f3b-9e965941d1c7"))) { LOG.info((("Table '") + (tableName)) + ("' was successfully enabled.")); } else { LOG.info("Table '" + tableName + "' was successfully enabled."); }
  }

  /**
   * Action after enabling table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void postEnable(final MasterProcedureEnv env, final EnableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("ca0ee9af-c4f8-3102-8e5c-86e58a163446"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5a9ea933-b067-3e99-8c50-3cb8ca181d39"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d32f42a8-6037-3b2f-b6e6-100567dab4b8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("021ab8c7-13eb-3d31-9491-31975bab0f00"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("abd529b8-0c68-38ee-a202-418df35723fc"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("e71749f8-1b1a-3a6f-9ce8-1a159698517c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d4662b92-2a53-3588-848a-80cfc4e21f37"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2f13fdd5-130b-34c7-9c8f-a947326f64d0"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("bd8a9d41-853a-3457-b741-9ec02f506fea"))) { preEnable(env, state); } else if (KnobRuntime.check(java.util.UUID.fromString("c0465c63-0033-3ec2-963e-31cf601fdf41"))) { postEnable(env, state); } else { runCoprocessorAction(env, state); }
  }

  /**
   * Coprocessor Action.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final EnableTableState state)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("aeee3c57-0d5a-34fa-96ca-86e10e541875"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9c374dc1-877b-3b90-a430-6065ee0d32ad"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("9e8853e6-2ede-3296-8ba4-64eec59df1b8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("767d9fbf-7eec-3bbd-b571-f1988a78249d"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5b1bc4da-409b-3a14-b170-775d2de7635d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b68e6e32-73bb-3a19-ba66-54bd2c3d72c5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7713cb66-cfe0-3bbe-94c1-e5848cd86a54"))) {
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
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("57e83bac-75f5-3611-ad1e-d2ce9d6cfdd9"))) ? ((cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("06bb060d-51de-3ea1-b509-f133ebe71274"))) ? ((cpHost) != (null)) : (cpHost != null))))) {
      switch (state) {
        case ENABLE_TABLE_PRE_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("22ae0d7b-dbcc-3967-8c52-d95a355e8217"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("5b279824-57ca-3032-aba3-3a854867b4a8"))) { cpHost.postCompletedEnableTableAction(getTableName(), getUser()); } else { cpHost.preEnableTableAction(getTableName(), getUser()); }
          break;
        case ENABLE_TABLE_POST_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("4117a4e0-c2c6-3ee2-a91d-1736a2d50dea"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("a86f099c-5827-3b6d-bc5a-b7a455373927"))) { cpHost.preEnableTableAction(getTableName(), getUser()); } else { cpHost.postCompletedEnableTableAction(getTableName(), getUser()); }
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
      if (((KnobRuntime.check(java.util.UUID.fromString("a78df0f3-86c6-3fce-ad9a-8ecedccdf1ef"))) ? ((regionInfo.getReplicaId()) == (max)) : (((KnobRuntime.check(java.util.UUID.fromString("7bf25e4c-3616-3db1-b899-e4acc3e2bf2a"))) ? ((regionInfo.getReplicaId()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fe1b6f1a-20fc-3aa4-88f4-8c5331c89169"))) ? ((regionInfo.getReplicaId()) > (max)) : (((KnobRuntime.check(java.util.UUID.fromString("5b762822-31de-365c-a0ee-6f90d333fc09"))) ? ((regionInfo.getReplicaId()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2b6e307b-3254-36fb-b395-c4e8ee25b7b4"))) ? ((regionInfo.getReplicaId()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("60be53f2-4f42-30e7-9487-dd0e60defab7"))) ? ((regionInfo.getReplicaId()) != (max)) : (((KnobRuntime.check(java.util.UUID.fromString("7b42f9e4-0c9d-38a1-8bc1-a8af97bcdf06"))) ? ((regionInfo.getReplicaId()) >= (max)) : (((KnobRuntime.check(java.util.UUID.fromString("43bcf509-f000-3e44-8998-12e7da9649dd"))) ? ((regionInfo.getReplicaId()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("db58f84e-5abc-3c9a-bcd8-fc06a2d4d7bf"))) ? ((regionInfo.getReplicaId()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("419c9899-9ac9-35da-ad56-2f1c2d66b7d3"))) ? ((regionInfo.getReplicaId()) <= (max)) : (((KnobRuntime.check(java.util.UUID.fromString("8a939992-c028-3bdd-8ed7-45e7b78f8b4b"))) ? ((regionInfo.getReplicaId()) < (max)) : (((KnobRuntime.check(java.util.UUID.fromString("8aa77842-e037-3715-a250-0dcac89f22eb"))) ? ((regionInfo.getReplicaId()) >= (0)) : (regionInfo.getReplicaId() > max))))))))))))))))))))))))) {
        // Iterating through all the list to identify the highest replicaID region.
        // We can stop after checking with the first set of regions??
        max = regionInfo.getReplicaId();
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("edeee2b4-2012-32dc-a3d5-55b93c9d009e"))) ? (0) : (max));

  }
}
