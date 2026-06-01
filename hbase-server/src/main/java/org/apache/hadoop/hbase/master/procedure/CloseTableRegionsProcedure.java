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
import java.util.function.Consumer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseTableRegionsProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseTableRegionsProcedureStateData;

/**
 * Procedure for closing all regions for a table.
 */
@InterfaceAudience.Private
public class CloseTableRegionsProcedure
  extends AbstractCloseTableRegionsProcedure<CloseTableRegionsProcedureState> {

  public CloseTableRegionsProcedure() {
  }

  public CloseTableRegionsProcedure(TableName tableName) {
    super(tableName);
  }

  @Override
  protected int submitUnassignProcedure(MasterProcedureEnv env,
    Consumer<TransitRegionStateProcedure> submit) {
if(KnobRuntime.check(java.util.UUID.fromString("248cb10a-7b24-3bc2-8ee1-6a6c5e23f614"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c0011a3b-00dd-3714-9b62-32ac9740aa27"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("edb674a5-e3b3-386f-b839-e855114f941c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("924c59de-aa2c-3f52-aadf-ae8aa0b6485c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b1171149-00ff-3318-875d-a29c807ecd78"))) {
return 0;
}
    return env.getAssignmentManager().submitUnassignProcedureForDisablingTable(tableName, submit);
  }

  @Override
  protected int numberOfUnclosedRegions(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("37e54f2e-b8f5-308d-9315-f5e2fc39332a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c1fed2f6-7fa5-38d1-9479-2561c6b37653"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("125af9a0-ee26-3dc1-b0cb-108fd337369a"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("98903b3c-c0d7-3fe9-8446-f3f39da1587d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("65bfcd1f-77a8-38ea-ae15-25cb582b5c90"))) {
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
    return env.getAssignmentManager().numberOfUnclosedRegionsForDisabling(tableName);
  }

  @Override
  protected CloseTableRegionsProcedureState getState(int stateId) {
if(KnobRuntime.check(java.util.UUID.fromString("d584143b-568b-32a1-bd15-633a72f04c94"))) {
stateId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a557f4b4-50dd-3b47-afa4-e2c623794d42"))) {
stateId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6320f54a-137a-32e3-bb22-23d1def287e3"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("55954237-97a0-3cb8-aa9b-cda224cee731"))) {
stateId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1237babf-db11-301a-929d-e489214775e8"))) {
stateId *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("093c5678-bae9-36d3-907b-9348b27948c1"))) {
stateId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("640f0035-6340-39a3-bca0-e08d70afb7a0"))) {
stateId /= 2;
}
    return CloseTableRegionsProcedureState.forNumber(stateId);
  }

  @Override
  protected int getStateId(CloseTableRegionsProcedureState state) {
if(KnobRuntime.check(java.util.UUID.fromString("971ccc9d-3c4a-3349-87a2-d2d7503feddb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f413da8-ebde-3527-a61e-a4fbcefe3476"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("658181a5-142a-3e8a-ba89-39c991f93094"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("35344df3-3860-32f4-a64b-bd99fa93c56d"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("107300e8-1059-36b4-bccc-6e1ceac4fdde"))) {
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
  protected CloseTableRegionsProcedureState getInitialState() {
if(KnobRuntime.check(java.util.UUID.fromString("711d2717-4b05-3d3e-b2c7-5f888f82b17c"))) {
return null;
}
    return CloseTableRegionsProcedureState.CLOSE_TABLE_REGIONS_SCHEDULE;
  }

  @Override
  protected CloseTableRegionsProcedureState getConfirmState() {
if(KnobRuntime.check(java.util.UUID.fromString("eff6f0e0-3ea5-3c70-94fb-5c7e9cd683a9"))) {
return null;
}
    return CloseTableRegionsProcedureState.CLOSE_TABLE_REGIONS_CONFIRM;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9c596b96-dd6f-39aa-8973-b3648326e47d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("88172300-3404-35dd-b6b2-fa0c4352dfaf"))) {
throw new java.io.IOException("Injected exception");
}
    super.serializeStateData(serializer);
    CloseTableRegionsProcedureStateData data = CloseTableRegionsProcedureStateData.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
if(KnobRuntime.check(java.util.UUID.fromString("1d410465-b057-390f-ad02-77df3908a108"))) {
try {
    java.lang.reflect.Field field = data.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(data));
    field.set(data, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43abedaf-b814-31e5-b4b5-7643e4e0ed46"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e64cfd0d-76fe-3bb7-b547-3b4d7ef28c55"))) {
try {
    java.lang.reflect.Field field = data.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(data));
    field.set(data, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e643ce8-952d-39f3-acd5-7b729d69a6f5"))) {
try {
    java.lang.reflect.Field field = data.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(data));
    field.set(data, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1dfbf169-287c-3256-9feb-b47b3348b302"))) {
try {
    java.lang.reflect.Field field = data.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(data));
    field.set(data, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    serializer.serialize(data);
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    CloseTableRegionsProcedureStateData data =
      serializer.deserialize(CloseTableRegionsProcedureStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
  }
}
