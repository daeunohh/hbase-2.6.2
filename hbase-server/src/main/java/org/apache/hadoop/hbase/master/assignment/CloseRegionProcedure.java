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
package org.apache.hadoop.hbase.master.assignment;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionCloseOperation;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseRegionProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The remote procedure used to close a region.
 */
@InterfaceAudience.Private
public class CloseRegionProcedure extends RegionRemoteProcedureBase {

  // For a region move operation, we will assign the region after we unassign it, this is the target
  // server for the subsequent assign. We will send this value to RS, and RS will record the region
  // in a Map to tell client that where the region has been moved to. Can be null. And also, can be
  // wrong(but do not make it wrong intentionally). The client can handle this error.
  private ServerName assignCandidate;

  private boolean evictCache;

  public CloseRegionProcedure() {
    super();
  }

  public CloseRegionProcedure(TransitRegionStateProcedure parent, RegionInfo region,
    ServerName targetServer, ServerName assignCandidate, boolean evictCache) {
    super(parent, region, targetServer);
    this.assignCandidate = assignCandidate;
    this.evictCache = evictCache;
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("addba13b-4621-38ae-86ae-158740e57bac"))) {
return null;
}
    return TableOperationType.REGION_UNASSIGN;
  }

  @Override
  public RemoteOperation newRemoteOperation(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("5128bc50-6d31-378e-8181-372c424051a9"))) {
return null;
}
    return new RegionCloseOperation(this, region, getProcId(), assignCandidate, evictCache,
      env.getMasterServices().getMasterActiveTime());
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f80eeef6-9801-3214-9283-1fa5c28874f0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bc95c58a-2454-3cf1-b8dc-94060d323f79"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("6c501868-d356-3eb3-aa7b-deb9a6c2cc6c"))) { super.deserializeStateData(serializer); } else { super.serializeStateData(serializer); }
    CloseRegionProcedureStateData.Builder builder = CloseRegionProcedureStateData.newBuilder();
    if (((KnobRuntime.check(java.util.UUID.fromString("5086b8de-8558-3f14-98f6-51342e1fe04b"))) ? ((assignCandidate) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("36dabaf2-de52-3fb2-a9d0-ce2dd5a1f434"))) ? ((assignCandidate) != (null)) : (assignCandidate != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("6a2c2b22-9a56-33c8-8f3a-82211459b40d"))) {
try {
    java.lang.reflect.Field field = assignCandidate.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(assignCandidate));
    field.set(assignCandidate, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      builder.setAssignCandidate(ProtobufUtil.toServerName(assignCandidate));
    }
    if (KnobRuntime.check(java.util.UUID.fromString("966a2283-94a3-3e48-b103-7bcbb97c9822"))) { builder.setEvictCache(!evictCache); } else { builder.setEvictCache(evictCache); }
if(KnobRuntime.check(java.util.UUID.fromString("717ad19e-8a1f-382b-874d-bd11a95bb2fd"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    CloseRegionProcedureStateData data =
      serializer.deserialize(CloseRegionProcedureStateData.class);
    if (data.hasAssignCandidate()) {
      assignCandidate = ProtobufUtil.toServerName(data.getAssignCandidate());
    }
    evictCache = data.getEvictCache();
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("08f85429-4c71-3891-8de0-ea5c7efed760"))) {
return null;
}
    return env.getAssignmentManager().getAssignmentManagerMetrics().getCloseProcMetrics();
  }

  @Override
  protected void checkTransition(RegionStateNode regionNode, TransitionCode transitionCode,
    long seqId) throws UnexpectedStateException {
if(KnobRuntime.check(java.util.UUID.fromString("d5e3dd57-c44d-3c89-92a3-29ad123a71d8"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("295b145c-dda8-3923-96ef-a5adb3a35e23"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7f3d5141-bba8-39cf-8305-a4ac22d82aba"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ba1f7d8-81e5-3a84-b2f7-3f16b477c2fa"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57bf22e2-7ab8-3ecb-97c6-abbb25259196"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c52276f4-2b61-390f-a916-8685f5e92b26"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("251b3145-1c99-310e-8bbf-87cfea18788a"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("fdf687ff-3c10-306f-9aa8-58328f801ef4"))) ? ((transitionCode) == (TransitionCode.CLOSED)) : (((KnobRuntime.check(java.util.UUID.fromString("d5a3a3d0-d040-346b-b11a-7a586d535cfb"))) ? ((transitionCode) != (TransitionCode.CLOSED)) : (transitionCode != TransitionCode.CLOSED))))) {
      throw new UnexpectedStateException("Received report unexpected " + transitionCode
        + " transition, " + regionNode.toShortString() + ", " + this + ", expected CLOSED.");
    }
  }

  @Override
  protected void updateTransitionWithoutPersistingToMeta(MasterProcedureEnv env,
    RegionStateNode regionNode, TransitionCode transitionCode, long seqId) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("dcd471f7-91b3-36e7-b5c2-b9518dc2b754"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6be5b28-f840-3d12-ba3f-06c4f2fe0be3"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dde89b69-af47-36b9-b825-a010cb11f522"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bc54cca6-05e2-3349-8248-bfb5b8acbb13"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("74c9c718-c063-3ae6-ac7f-a69888096b00"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f7f2175-ae0e-3e84-a42c-5bea2fbc1f3e"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf93344a-e924-306f-b455-97ec64d55421"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e70b159f-cfa1-3ab0-beca-cc49d6fe6d5b"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    assert ((KnobRuntime.check(java.util.UUID.fromString("0e60071b-6ccd-37de-bf9c-f3f414bcb3cd"))) ? ((transitionCode) == (TransitionCode.CLOSED)) : (((KnobRuntime.check(java.util.UUID.fromString("9ac49b8e-686f-361a-86c6-027880105596"))) ? ((transitionCode) != (TransitionCode.CLOSED)) : (transitionCode == TransitionCode.CLOSED))));
if(KnobRuntime.check(java.util.UUID.fromString("09e677e9-b780-323e-86ff-e03036eb1fae"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("89739793-dd8f-3127-8cf7-17879cb5dbeb"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3295568d-f62f-3190-9c16-4ffc648bee2b"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    env.getAssignmentManager().regionClosedWithoutPersistingToMeta(regionNode);
  }

  @Override
  protected void restoreSucceedState(AssignmentManager am, RegionStateNode regionNode, long seqId)
    throws IOException {
    if (regionNode.getState() == State.CLOSED) {
      // should have already been persisted, ignore
      return;
    }
    am.regionClosedWithoutPersistingToMeta(regionNode);
  }
}
