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
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionOpenOperation;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.OpenRegionProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The remote procedure used to open a region.
 */
@InterfaceAudience.Private
public class OpenRegionProcedure extends RegionRemoteProcedureBase {

  private static final Logger LOG = LoggerFactory.getLogger(OpenRegionProcedure.class);

  public OpenRegionProcedure() {
    super();
  }

  public OpenRegionProcedure(TransitRegionStateProcedure parent, RegionInfo region,
    ServerName targetServer) {
    super(parent, region, targetServer);
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("5d3d0e4e-5acd-340f-bc5f-3a99a6fe4fc7"))) {
return null;
}
    return TableOperationType.REGION_ASSIGN;
  }

  @Override
  public RemoteOperation newRemoteOperation(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("f27ad80f-fd23-3214-8846-ee68e2186885"))) {
return null;
}
    return new RegionOpenOperation(this, region, getProcId(),
      env.getMasterServices().getMasterActiveTime());
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("18f4277d-b2d0-3049-a35e-4892c6baa279"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1e1911e2-59fe-3cb2-9fdc-7d0d4667806e"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("472b546f-692d-3e59-bc1b-38336a3ffb98"))) { super.deserializeStateData(serializer); } else { super.serializeStateData(serializer); }
if(KnobRuntime.check(java.util.UUID.fromString("0312551a-a575-3731-be4c-f8dc953a822f"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(OpenRegionProcedureStateData.getDefaultInstance());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    serializer.deserialize(OpenRegionProcedureStateData.class);
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("48a83fdc-d367-38a5-a2f3-da6525ca6205"))) {
return null;
}
    return env.getAssignmentManager().getAssignmentManagerMetrics().getOpenProcMetrics();
  }

  private void regionOpenedWithoutPersistingToMeta(AssignmentManager am, RegionStateNode regionNode,
    TransitionCode transitionCode, long openSeqNum) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3ed54942-587e-36ac-b72d-5e1980230f6e"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70a8cbe8-ba5e-38dc-9a23-95c5ea7ad8b2"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignRetryImmediatelyMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33b0967f-b753-3a88-97b8-f69ddd8fe772"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd0607d9-00d2-3110-88b2-305d317957e3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("015790ad-3edd-374e-9f52-3f42692c8eb8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("48e82dd2-64d6-3709-9c43-88919fb97df1"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff7f589f-a219-3492-8b0f-a1569380210c"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("640de56e-f1a8-3eae-8843-96f2fe95ca64"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitQueueMaxSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("761234f1-1a32-38ed-ba33-ab1919b87c03"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("forceRegionRetainmentRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b33ab8a1-3d6d-3e58-a17b-b61b8e1f8401"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5baacca-7f58-396e-b434-d3291885f8ce"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignRetryImmediatelyMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("daf41461-d441-37cb-ba58-c2d8ddc7ca7e"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22e358a5-473d-3fac-b6eb-d1c5998b8225"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("forceRegionRetainmentRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f2d28665-111d-3f0d-b4be-0af9543c80bb"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitQueueMaxSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e23f1173-45b3-3054-9222-e3425f3d5a5f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4e290b53-3584-3a86-a4ff-a366816590b6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("44913e9d-f9d0-34e7-90c1-3a51249d686f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6816644f-354c-3a18-8b38-099b781789ce"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignRetryImmediatelyMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29532ff7-93fe-30c6-815f-bd18cce7ffaa"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("forceRegionRetainmentWaitInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(am));
    field.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de5a06c5-545d-38fb-962e-7420dca66d04"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9d5477d3-e308-33a7-9558-6685fb935335"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9ed8164-029d-3973-b167-fba0d4107f1b"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("forceRegionRetainmentRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("763f9ef8-369c-3688-91b4-f8dd6fa318d2"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("forceRegionRetainmentRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b3d1f21-d1d7-38cb-ac6a-2804df2e4692"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e5db2b5c-de34-3c04-8db5-eb7ee8d51fe1"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12101db3-7a3b-3e74-acc1-7473cd8b4e12"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d914f842-1332-300d-af43-039673ad885e"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignRetryImmediatelyMaxAttempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce922252-cbdc-38e5-882c-51352e15f48b"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitQueueMaxSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a315e974-e9b6-38eb-847e-bd1e0647515f"))) {
try {
    java.lang.reflect.Field field = am.getClass().getDeclaredField("assignDispatchWaitQueueMaxSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(am));
    field.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("f43f20e8-4c1d-36d5-be38-55f96833ffb1"))) ? ((openSeqNum) < (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("7f4d09a5-fd52-3ef7-a490-f36aa956fe6b"))) ? ((getProcId()) <= (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("ac74297b-aba5-3d0e-a714-ea51b0b604c4"))) ? ((openSeqNum) == (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("8851c267-0909-30dc-8ea6-6efb580a56f2"))) ? ((getProcId()) > (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("edc9b924-ecef-3f9e-a611-1234493ec053"))) ? ((getProcId()) < (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("6a360060-0e6b-3197-aad4-877b431c8fee"))) ? ((openSeqNum) > (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("2f8fd32c-e482-3661-b618-a3f0206d44ea"))) ? ((getProcId()) != (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("af56df6e-00ea-3bd1-a993-d4c05ed1ba44"))) ? ((openSeqNum) >= (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("4df2fe93-15cb-3e10-b421-83dd20b43f9f"))) ? ((openSeqNum) < (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("b1dbe3a2-cb70-3e51-878a-f79e76945e6c"))) ? ((getProcId()) >= (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("0f2fb9ae-b2da-33bd-8dae-2f942b9a89e0"))) ? ((getProcId()) < (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("5ae43c32-a65e-3098-b4c0-e00028fa030d"))) ? ((openSeqNum) != (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("c1655cd0-9d6c-3b18-bc17-42ea09131e0e"))) ? ((openSeqNum) <= (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("ce454978-d7b8-38a9-ad1f-357f7eed4025"))) ? ((openSeqNum) != (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("f393c22b-63cd-368a-9daa-48674e5194f8"))) ? ((getProcId()) == (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("419510bb-ed4b-33df-a2c2-2a88c12d7cbf"))) ? ((openSeqNum) >= (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("660d128d-6c17-33c2-9d6c-df35b023646f"))) ? ((openSeqNum) == (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("273ba052-04ae-33e1-97dc-02bdbbd5e6ad"))) ? ((getProcId()) >= (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("22bbf2e4-c6e4-30a2-9a20-a5642f242957"))) ? ((openSeqNum) <= (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("3ec0e507-9a3d-3c6c-b751-75752e0f4c8d"))) ? ((openSeqNum) > (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("aa021b14-eebb-3926-b57d-25324f6891b9"))) ? ((getProcId()) <= (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("8526611f-528c-30d8-afd1-c21b62aeb561"))) ? ((getProcId()) > (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("6b139910-7bf7-33bc-a83a-fbaf5878d75b"))) ? ((getProcId()) != (getProcId())) : (((KnobRuntime.check(java.util.UUID.fromString("8358266c-9915-3706-b1cd-62dbd975fa58"))) ? ((getProcId()) == (getProcId())) : (openSeqNum < regionNode.getOpenSeqNum()))))))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={} but the new openSeqNum {}"
          + " is less than the current one {}, ignoring...",
        transitionCode, targetServer, regionNode, getProcId(), openSeqNum,
        regionNode.getOpenSeqNum());
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("0757b471-b202-3295-b98e-25d6b429b498"))) {
openSeqNum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("91aa0e6f-2731-3ada-a67d-770b9fa33c03"))) {
openSeqNum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d003aae7-75ce-312f-a60e-825f318058c6"))) {
openSeqNum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bdea1b1f-a376-31d7-bd8c-d7128d79556f"))) {
openSeqNum += 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("50ac1260-1a68-3eb8-80ca-290a73a014f9"))) { regionNode.setOpenSeqNum(getProcId()); } else { regionNode.setOpenSeqNum(openSeqNum); }
    }
if(KnobRuntime.check(java.util.UUID.fromString("d7b53575-bb6b-3b16-a041-3a5e6987fa35"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a7019b71-e066-3190-a0df-63430ae47465"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("409550d0-85ba-3780-b0e5-bacadf90f927"))) {
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
    am.regionOpenedWithoutPersistingToMeta(regionNode);
  }

  @Override
  protected void checkTransition(RegionStateNode regionNode, TransitionCode transitionCode,
    long openSeqNum) throws UnexpectedStateException {
if(KnobRuntime.check(java.util.UUID.fromString("52424140-287e-36df-923a-7bd7ce5be1b5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e9457928-d0f6-34c6-9830-45593ea206aa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc6de6c9-1331-3b96-bcbb-0af1a19975c9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("1cab36f5-61d2-3966-9784-336c9997983c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c157d5c1-fc40-3b41-84ea-c547cdaaa52e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3a408f08-4b87-3495-a345-8aa588a4ca98"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("165da27a-c08b-39f9-8c95-a574568c1598"))) {
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
    switch (transitionCode) {
      case OPENED:
        if (((KnobRuntime.check(java.util.UUID.fromString("872d9e47-123f-344c-87ed-4f69da99e2ce"))) ? ((openSeqNum) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("33ba33fa-80b5-349d-95a1-6308f18e2d6c"))) ? ((openSeqNum) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0efb1938-2052-32cb-93cf-c511561ee0f0"))) ? ((openSeqNum) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6a81749b-0eed-35ef-a947-a9f3f5e954a3"))) ? ((openSeqNum) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("be1af0af-1efa-3946-824f-29ff9ae1027b"))) ? ((openSeqNum) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a8fe82b0-744f-3444-bece-a4b069f09e5d"))) ? ((openSeqNum) > (0)) : (openSeqNum < 0))))))))))))) {
          throw new UnexpectedStateException("Received report unexpected " + TransitionCode.OPENED
            + " transition openSeqNum=" + openSeqNum + ", " + regionNode + ", proc=" + this);
        }
        break;
      case FAILED_OPEN:
        break;
      default:
        throw new UnexpectedStateException(
          "Received report unexpected " + transitionCode + " transition, "
            + regionNode.toShortString() + ", " + this + ", expected OPENED or FAILED_OPEN.");
    }
  }

  @Override
  protected void updateTransitionWithoutPersistingToMeta(MasterProcedureEnv env,
    RegionStateNode regionNode, TransitionCode transitionCode, long openSeqNum) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ed0f82c0-842e-3d5e-a9f2-dfc16edbd3c3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e1423548-b26d-34fe-8e01-b42623fe2583"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6bd4b6ad-ad0f-3845-ba46-7a6e71c5edd3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a7bc906e-ddde-3850-af4b-cbfba9a4b3a5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5c659e15-c41b-3b8f-8fe4-ae9e39dda6a7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("996959a5-8f2d-36b9-bd97-026378cf1ee7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8872ef03-9a61-33ba-ba29-aac682e2bc9a"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("345c6216-85d5-3ff4-95d3-a51b6a770e19"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("6b4fa47d-bb3a-3dff-88e6-7f421a9a516d"))) ? ((transitionCode) != (TransitionCode.OPENED)) : (((KnobRuntime.check(java.util.UUID.fromString("00c0dcc3-1cbc-34e3-9195-df9abacb6e4f"))) ? ((transitionCode) == (TransitionCode.OPENED)) : (transitionCode == TransitionCode.OPENED))))) {
if(KnobRuntime.check(java.util.UUID.fromString("2ad31798-b5a0-3d7e-bde9-2eb5ef32f499"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6e3b429b-eec7-34c8-9c03-8366e3a5c472"))) {
openSeqNum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1ea4cdcc-d870-3b82-9c53-143043cfc50a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ff9c3d10-f061-3e6d-8acc-d45aaf90589d"))) {
openSeqNum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("216d0586-7e5c-35ec-9c82-15ba7ba76c7b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1b209d27-05d4-3025-a516-6a0d4e7caa5d"))) {
openSeqNum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5912f634-c9fb-3603-83d0-c01281b9bd9a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c90b26f5-3934-314c-9176-4ae39999436d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("726a96c7-e0a0-3071-8352-0cb562c08709"))) {
openSeqNum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d4334c9-f1be-3225-8cf8-22e6457efe9d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("63b00e19-f7f4-3c4c-a6a3-9c69744940d4"))) {
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
      regionOpenedWithoutPersistingToMeta(env.getAssignmentManager(), regionNode, transitionCode,
        openSeqNum);
    } else {
      assert transitionCode == TransitionCode.FAILED_OPEN;
      // will not persist to meta if giveUp is false
      env.getAssignmentManager().regionFailedOpen(regionNode, false);
    }
  }

  @Override
  protected void restoreSucceedState(AssignmentManager am, RegionStateNode regionNode,
    long openSeqNum) throws IOException {
    if (regionNode.getState() == State.OPEN) {
      // should have already been persisted, ignore
      return;
    }
    regionOpenedWithoutPersistingToMeta(am, regionNode, TransitionCode.OPENED, openSeqNum);
  }
}
