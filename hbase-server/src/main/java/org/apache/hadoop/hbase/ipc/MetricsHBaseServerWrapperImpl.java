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
package org.apache.hadoop.hbase.ipc;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsHBaseServerWrapperImpl implements MetricsHBaseServerWrapper {

  private RpcServer server;

  MetricsHBaseServerWrapperImpl(RpcServer server) {
    this.server = server;
  }

  private boolean isServerStarted() {
if(KnobRuntime.check(java.util.UUID.fromString("f42e723f-998e-3d87-b7e2-a5b6fe59dcc9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("61e58845-90e6-35bd-a2e3-dc2386928e88"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("c6770df3-2639-3739-a620-f9fd0161c00f"))) ? ((this.server != null) && (this.server.isStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("bfbc43b2-143b-37d6-a847-28f32964bb56"))) ? ((this.server != null) && (isServerStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("4b75dbb9-b608-349e-9e5f-a35896277596"))) ? ((this.server) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("20219052-4a5a-3d33-8ec1-6e81bd47e1fd"))) ? (isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("d58c32fb-3300-30ce-bd5b-26287cb42224"))) ? (((this.server) != (null)) || (isServerStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("9b7f1df9-96eb-38f7-9ea7-4c7c9dba91ef"))) ? (((this.server) != (null)) && (this.server.isStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("ed36b017-19e8-3a64-8ebb-ec224c9fd962"))) ? (this.server.isStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("f6e066d3-3872-3139-9ecb-93d3f9525728"))) ? ((this.server != null) || (isServerStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("e28bc3f4-4e85-31e8-9452-3c563a9dee6f"))) ? (((this.server) != (null)) || (this.server.isStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("8ab2f9f2-8307-3119-a4ec-553e39aea2f4"))) ? (((this.server) == (null)) && (this.server.isStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("c7a798ff-2b44-36d1-ac7b-17f676ecfd5f"))) ? (((this.server) != (null)) && (isServerStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("75e6a4d4-696e-3f32-a0b9-87b82063c53f"))) ? ((this.server) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ac2e70db-474c-3052-8333-8df3a507096a"))) ? (((this.server) == (null)) || (this.server.isStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("eed780b4-d6f3-324b-bf17-41cd713fac06"))) ? (this.server != null) : (((KnobRuntime.check(java.util.UUID.fromString("b8d82d76-2b1b-3c1a-b347-232c4eb35288"))) ? (((this.server) == (null)) || (isServerStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("335c08fd-e827-3632-8251-55d83844937d"))) ? ((this.server != null) || (this.server.isStarted())) : (((KnobRuntime.check(java.util.UUID.fromString("7055eab1-1705-3318-bffe-c4b5a5419791"))) ? (((this.server) == (null)) && (isServerStarted())) : (this.server != null && this.server.isStarted()))))))))))))))))))))))))))))))))));
  }

  @Override
  public long getTotalQueueSize() {
if(KnobRuntime.check(java.util.UUID.fromString("d981c27b-6f09-32b6-96a2-4f30eb56919c"))) {
return 0;
}
    if (!isServerStarted()) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("75e1adc2-11dc-3f56-9b4f-f5ff1c067c03"))) ? (DirectMemoryUtils.getNettyDirectMemoryUsage()) : (server.callQueueSizeInBytes.sum()));
  }

  @Override
  public int getGeneralQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("b244cd76-e1cf-3d59-b91d-4ac709a0d84f"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("515f8209-7732-307b-80de-875514154e15"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("2bd27b57-66e8-3091-9ec6-b6cdc96ef941"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cdd5f7bc-fc25-39d3-82d1-cbaa651ba6cc"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("5f766eea-f0c7-3a29-bf22-5fdc3811200c"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0104f0b4-74b2-344b-a688-934c0ef85456"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6d842cf3-dba4-3112-af56-8f501cdad685"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("ce1f7e73-1748-37e0-80c6-dba13caf7092"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("eef82784-d158-3e80-9593-0df7d19a6b29"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6a82a747-f11a-339d-aaec-7bc0fee77654"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1985755d-36a4-358f-b5b8-f4d328eb2eff"))) ? (this.server.getScheduler() == null) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("7601bc52-5b00-3ff5-9b63-0da742b62a5c"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("f98a1a0f-cdc9-3bf4-a1c6-c8040d35f2fb"))) ? (server.getScheduler().getReplicationQueueLength()) : (server.getScheduler().getGeneralQueueLength()))));
  }

  @Override
  public int getReplicationQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("b2adfc51-823a-31a3-9410-2b36eb18d5c1"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("22cf4b0f-35e4-3501-8ef6-332a3bbd195c"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("104fb47c-943e-3c62-af48-43d09fa196f9"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("a961ab69-4ea4-3398-af8a-2079cdcdfad4"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("50b17ee8-2b2c-3278-a22a-a81a4e5775d6"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a1b8ee49-dda3-3a2f-a547-8cb69dca5c6c"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e5492887-03a0-3202-b9d5-ac721486340c"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("51dc1205-4ead-368e-baf5-2143681d9aef"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("aa69fba0-d85e-3fbe-916a-85a7b0830396"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d2638fc6-8093-34e3-a1cb-8f79bd855213"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("5d77132a-876d-3c5f-b7d0-b51a69f81e69"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("1b8c9186-fced-3e92-b6e3-b1db0ce1a1db"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("ff72ed9e-bd45-3f29-b407-2a8cbd139861"))) ? (server.getScheduler().getGeneralQueueLength()) : (server.getScheduler().getReplicationQueueLength()))));
  }

  @Override
  public int getBulkLoadQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("790a2e13-13bd-35df-ba99-74b1aca7f8b6"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("8a18ecac-c482-3579-bee9-db1afe894402"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8921a8b1-d5f4-3b05-8b3a-eb249ac7e413"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("22ebd690-ec21-3c9a-a4ef-fe53df96b762"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3f6b994d-8723-3c63-a0bd-a72d54d81890"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6a4fa5ca-22a3-3589-9235-60d71e4d1000"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("aa4130bc-8113-3012-a27f-22b8b5b926b4"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("590db64b-bc2e-326b-a41d-e338136822dc"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2ca7af76-7afe-3ffe-8489-9cbd8b30b376"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("fade3d68-c4b3-3cfe-b90c-2edcf1eaff34"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6904d596-c61f-31b0-8419-36e575a4a054"))) ? (this.server.getScheduler() == null) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("57d18b62-a1fb-3d1d-93da-e954d0b37a94"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("c34211ee-6f99-387e-808d-b5c51f47513e"))) ? (server.getScheduler().getReplicationQueueLength()) : (server.getScheduler().getBulkLoadQueueLength()))));
  }

  @Override
  public int getPriorityQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("e87557f7-32ee-37da-9e22-9e86cc982456"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("caaac3c0-377c-3c35-8660-eef3db5c69e2"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("549c524a-342e-3654-915a-da7d39219e1b"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f2486727-2701-3db0-9074-2504c8c21c81"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("aa021664-30c3-39a3-9908-566a6ca658ce"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a49420a5-89e1-3afb-902f-4fe70980985f"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("30e0b489-5b00-38ad-9bcd-b90321ada9af"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6cd45812-cd8c-3e4b-a69b-d68344e3253e"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("4c91eb5f-6cb2-308b-b9f6-b0fb660b4533"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ad1f714f-3fc1-3df2-889f-7904ff754c27"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("544ce782-c2b2-3736-a5a1-342963fc2848"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("afcf1c32-c96e-31ba-890b-904dbab36d2e"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("919ab3a0-e50b-32ce-a3b4-046a252d1d92"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("5d9bcc0e-d298-381f-9f7e-8c11728054c1"))) ? (server.getScheduler().getGeneralQueueLength()) : (server.getScheduler().getPriorityQueueLength()))))));
  }

  @Override
  public int getMetaPriorityQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("1202a743-f06e-341b-b0c3-51bec22030e6"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("1ed03d79-a701-3489-a6c1-701ca28def50"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c2bb7848-d393-3df7-a870-63e8d791899f"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bdcf3a02-82f6-3905-bbce-985dd7bd1fde"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("d38fc138-bf7a-3f40-a446-696d886a6bd9"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4c7a6755-6722-3e9a-a82c-52a842e26241"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("c9cbec72-04ff-3222-9e15-3df0cd1e0856"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("791e3ed3-c368-3f46-8e95-3b9269df9693"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e75f692b-2dd6-3f7c-aed3-afb9070d4f36"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("d769ab8a-8ad6-387f-82c6-9befe3fe49ba"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7e72bae7-d1c1-3bd2-af8e-2048a7076eec"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("36e349ff-238c-38c3-847a-09f57e7bb450"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("93418644-a318-3629-9b0d-09502b360f5e"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("a555ca6a-b9f3-33dd-a8e7-f6e249050fcd"))) ? (server.getScheduler().getGeneralQueueLength()) : (server.getScheduler().getMetaPriorityQueueLength()))))));
  }

  @Override
  public int getNumOpenConnections() {
if(KnobRuntime.check(java.util.UUID.fromString("8b866e75-f83d-3e02-bda9-0a2ced455362"))) {
return 0;
}
    if (!isServerStarted()) {
      return 0;
    }
    return server.getNumOpenConnections();
  }

  @Override
  public int getActiveRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("51f6953f-d3b5-300e-ae64-936ed3f1e376"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("5b62d62d-0e14-3c04-9980-9d6755fb152d"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("169c5a14-ec31-3e8c-bafc-adb965153ce2"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("744b47ec-05fe-3ea0-8408-aa5524589ad1"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b4022d46-fccd-3485-a0f1-f650a086ca16"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2cd54d2f-dcf2-37e6-a076-ce4018918153"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a3369052-f73f-3c92-ac67-ee099f18e7e0"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("48f275be-9fb2-36c4-9118-e1a510919e9f"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("918bc8ec-0539-33a9-b120-d2cf75640c79"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8c884732-eff9-35e6-8c9d-b3efd3a9345c"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("185697ca-7c4d-34d6-b8e3-a4baa3e2d86e"))) ? ((this.server.getScheduler()) != (null)) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("883f274c-c297-310b-bbdc-9aead153ece3"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("1dc03ac6-701e-32fd-913a-f140a2579699"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("d8f93435-c109-381a-b817-623471fb8510"))) ? (server.getScheduler().getGeneralQueueLength()) : (server.getScheduler().getActiveRpcHandlerCount()))))));
  }

  @Override
  public int getActiveGeneralRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("10692ff8-6782-328f-bef7-28260cff4e10"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("d48bfaa5-58d6-3287-897e-05a720b75955"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1ebd72b1-f516-3f47-b6ca-68a2e6cf866a"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("0297f4d3-9cf3-3af0-9506-33359fb86378"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8ff61dd5-abc0-35a8-9747-258ef5c9cdf7"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("784f885e-a8a2-30c0-ad3d-eb9e0d0b0230"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("0f40e033-faa4-3be9-9d2f-278a5108931d"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("12c7470e-9352-34f7-a44a-2b54bfb55f90"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2610bcbb-9bec-3c9a-936e-0e2f311cd391"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fef3f4aa-62a8-3a16-8285-b24e94e55081"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("99597619-5b5d-3a22-b085-e7f2f684ac32"))) ? (!isServerStarted()) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("a5e9daf0-9adb-3e6f-8c82-5943dadd6f08"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("8490a26b-e299-3078-9d46-05554410fc6c"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("0f65d135-6a42-33d2-9a30-67cf6cd139a4"))) ? (server.getScheduler().getReplicationQueueLength()) : (server.getScheduler().getActiveGeneralRpcHandlerCount()))))));
  }

  @Override
  public int getActivePriorityRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("cba6deec-d6b4-378b-af4c-977c8d6996b3"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e5667cb6-176f-3357-b220-20f0520dfc2c"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f6dbec63-72b5-3972-91da-77af7a0272cc"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("b16fa3a1-ebe3-3329-acbf-1aa64e21c4f7"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("10979523-742f-3d03-b655-81e80b44e399"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("b21e6f29-5df1-3947-9077-883e2decb18b"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("603b4f6f-3b8b-3b62-a621-833e0ba42e04"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("dee8a9f9-3313-30a5-a932-55c2109d4cc2"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("88475a1a-de85-3180-9f70-0b8a9fbd3cd4"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b9747ed6-fea6-345e-b578-fef0d3772d9f"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("58f6af77-212f-3f81-80b5-12ccff5bb5bf"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("49526937-64ea-3c57-95d8-89b65e768513"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("9267b12f-da46-3f8e-bd04-b3624ebd4080"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("ba2ef085-77ea-363d-bf0a-b952fffbef02"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (server.getScheduler().getActivePriorityRpcHandlerCount()))))));
  }

  @Override
  public int getActiveMetaPriorityRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveMetaPriorityRpcHandlerCount();
  }

  @Override
  public int getActiveReplicationRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("189de567-9901-30b3-9bbc-ed23e490edeb"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("0e72e2dc-599d-3598-b8b8-29b1948d3501"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e43b3484-7af3-396e-8224-2ca0382f31bf"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8c726364-bdae-3eb1-8be0-cc42c05ae74a"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("85aef854-b4a3-3a57-bce9-e0d4811b830a"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("21a638c0-b4d0-3369-8846-6740352b8d39"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b2b87531-a8bf-3696-9fef-7e3051037209"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("95967a0e-99af-3be2-927f-c6643c99af1e"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("28676035-3c01-3c3d-ae39-53d17d365d13"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2d10fe63-987f-3969-bd86-ad86fda02e95"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("cc776c4d-dc7c-3e20-a4b5-29a70395609c"))) ? (this.server.getScheduler() == null) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("5aad3357-8a8e-31fa-a5ac-74a3021e1718"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("b9fe7fc9-c806-360e-915f-7fb5e913047d"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("3c1319eb-8dad-31b3-9a55-e0f101ea3a7f"))) ? (server.getScheduler().getReplicationQueueLength()) : (server.getScheduler().getActiveReplicationRpcHandlerCount()))))));
  }

  @Override
  public int getActiveBulkLoadRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("4dc9aefe-8b38-3d19-a137-805156a3e8bb"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("3eb95ba0-5bf4-325d-a1d5-ceee68458cf7"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6b16e4ee-c3ab-388c-9618-feb9f229bc04"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("36ec16eb-94ae-3dc3-bd56-f4618fe28dcf"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d5b8eaef-8dab-3dfa-92f8-95c7be7e5320"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("d04f7ccf-15ed-33dd-959e-7ff4bc3cb620"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ada07d49-d5cf-3e90-a02d-b6c00352f60f"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("02d72cab-ae17-3383-a231-69ffcdda49e4"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("705d9582-6f34-350f-9695-a38411a867d0"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("f56ea6de-0655-3abd-86fd-afb71a8d43cc"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("aacfadd0-2cd1-3215-8cc8-61df27d53d46"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("34da7e7d-1ad8-397a-a9b5-43f84c41b237"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("ac66b81a-5072-3bff-b0fd-f9798ce24a40"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("eccfa125-5653-3cd5-ba2d-cf0ba53fd5b3"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (server.getScheduler().getActiveBulkLoadRpcHandlerCount()))))));
  }

  @Override
  public long getNumGeneralCallsDropped() {
if(KnobRuntime.check(java.util.UUID.fromString("52a2507e-6aa7-31f4-aacf-aac78c918dcb"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("3ea348f7-7418-38d8-b519-96536e7bc9c7"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1b7387da-a180-3ddc-989e-4d6e707a63fa"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("fe3e72a2-aa4a-373c-ab87-818ff3a1ae71"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("97626b37-968b-342a-ac48-c8ea7c7eb57b"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("591d7f94-e67b-3ea8-acfa-6fc31a9172da"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9ed82a31-b033-395f-822e-159f91c939f7"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7063d8db-90ce-366a-83c4-cac658c6b008"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("56ed1178-3520-3486-aaad-61281f63e45a"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("1921541a-800a-356a-aa8b-366f375f2a0f"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("347087ac-ece8-3475-ba50-3c0aebdcecb4"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("db0db6ea-e4d6-3534-a754-79e8048a0ae6"))) ? (DirectMemoryUtils.getNettyDirectMemoryUsage()) : (((KnobRuntime.check(java.util.UUID.fromString("2ca01862-1c9b-3585-bb31-e9eff2c44bc1"))) ? (server.getScheduler().getNumLifoModeSwitches()) : (server.getScheduler().getNumGeneralCallsDropped()))));
  }

  @Override
  public long getNumLifoModeSwitches() {
if(KnobRuntime.check(java.util.UUID.fromString("d3be826b-e351-30db-836e-de329cbadd65"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("49dafc69-bc0e-3491-9daa-b22a8a9a8272"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("db814dad-6fab-37ac-8ce5-e19ebd6f12f4"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("3f76aab3-80a2-3cad-bd96-ef5cdd16c927"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("8a924eb7-0103-35b3-b1ae-dc0bc98a3034"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b5c9a402-ea9f-3db3-a6c4-0f015f4f9a99"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("f62e5d01-2524-3979-a56b-775551b43ea5"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a2cf014f-ac67-3ae3-b806-76cd48636618"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1cdee4b3-7ae8-3305-b394-837487dfa9cd"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("721251cd-eadf-3464-8ae8-868b74306a89"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a763d9d5-8bbb-362f-81bb-a7c831a63758"))) ? (this.server.getScheduler() == null) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("d3c9779c-fd89-3959-926e-ee311eb08613"))) ? (DirectMemoryUtils.getNettyDirectMemoryUsage()) : (((KnobRuntime.check(java.util.UUID.fromString("81748d25-ba36-3461-9c5d-adb87e85ebd8"))) ? (server.getScheduler().getNumGeneralCallsDropped()) : (server.getScheduler().getNumLifoModeSwitches()))));
  }

  @Override
  public int getWriteQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("38adcc6f-3351-33d8-82a6-5b216dafcb50"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("37bf8323-62c0-3794-af78-89ae2d57d49d"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7b072e35-c265-3753-83cb-9a5b20cb321d"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("3a6211ff-69ff-3c45-8dd8-90c7ed25e6d9"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("bf60a1d3-f1d2-3f38-919c-308c73d52b27"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("dc960b78-0280-374a-8a51-368aa31e19f0"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("4ee0f805-72a6-3d70-a432-12ee598f7e59"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("32fcf63f-50ab-315b-8b1b-cdc93398a869"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f0437339-9d5a-34c3-8018-f587d1832247"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8e9245fd-1697-3b01-95ba-6648c75135a3"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a488bb89-0c14-3c12-b876-46cfbdfe718c"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("1de6e706-173d-303f-a733-c325d484d033"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("71986966-06bf-36a4-95c4-225ac2b48859"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("de640af5-3d0c-398e-b5b2-dbb1283f2d02"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (server.getScheduler().getWriteQueueLength()))))));
  }

  @Override
  public int getReadQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("b150526f-d240-3eaf-a053-cc46e825a432"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("1e814a2a-8746-39f0-85e7-a82c5e0b559e"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("2c01b0be-970f-338f-800d-f91cf6d7bd75"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e4666d7b-a974-351f-ae16-75c6b9699f7f"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("38ba58cd-ee72-33e2-9c36-643a20c34bff"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("51c3ad36-46af-33d4-b6ab-aa3df6525655"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("e1eab49a-daa6-3ad5-9632-e838363a426f"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("2c230dcb-3cf8-32c8-b46d-09b0497e4c12"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2c83a6f5-f941-3c26-9561-65674019bef8"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("02020186-af7f-31c0-b053-711ffec526b2"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("cb3610c1-b37e-3784-a086-e281ac5820a9"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("00eb70d8-b4b2-3ffc-b922-4ca8378ad0aa"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("f98995c0-b4de-3ce3-a5be-16a18387227e"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("4cad4575-4e87-3f1c-94b5-9951b0e87acc"))) ? (server.getScheduler().getReplicationQueueLength()) : (server.getScheduler().getReadQueueLength()))))));
  }

  @Override
  public int getScanQueueLength() {
if(KnobRuntime.check(java.util.UUID.fromString("7b72e3c0-437e-31b1-842e-d8c0585d9896"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("b1833070-ffc4-3dc8-b116-1bad509735d6"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("dab24b5b-5be4-39ab-bf28-77082910bc1c"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("8635b8c0-4422-3a4a-aa24-9b8a5948ec2c"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("08d0afad-fd56-3941-8de1-96154bb2a764"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cfed9b66-3f20-34be-992b-9c2be7fe4b44"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("914769ec-e072-380d-bc09-3fda4421c787"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("554c7fdc-e3ab-3c3a-80f0-b60b13c90a8d"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b365bbf5-9e50-3447-96cf-b6f053abe4d6"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d5257e19-a029-3cb9-9708-d9142b75928d"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f6959604-e79d-37a0-9b32-2feb44844f44"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("10760077-0cdb-3b9a-b0a6-64c6d635a285"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("e0d7ec91-7ad9-328b-8aea-69be0af89bd8"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("2c41189f-cf3a-31f3-982d-6193568996c4"))) ? (server.getScheduler().getGeneralQueueLength()) : (server.getScheduler().getScanQueueLength()))))));
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("aafaaec0-c9be-3492-9620-2587e31535c1"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("da914a91-325a-39d7-b0fd-b9bc8015f142"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6da8f61a-c48b-31b7-bf38-c6f4d4e23dc2"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ea58c946-3f56-358f-8ea2-8e099aae0518"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6e5475b9-bfa1-3218-a42b-0317ae88017d"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e1bd9247-37be-3ea1-a31c-0b1feb5ccc16"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c7fa1ca7-03e8-3c83-b1ea-96cb20a656e1"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8f9c9fe4-5721-3377-acb8-d1ab17982ded"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e6deb992-ae59-383d-892d-4a5d59e09184"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("26cccdb8-6130-397f-bd01-c65411c2b08c"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("85628124-ba5b-3ceb-90d6-fe7b3a2d365a"))) ? (this.server.getScheduler() == null) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("0fc306b1-5246-30af-bb1e-4f2159c8a165"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("c4ba8151-079b-3a72-9707-43a721a8bb58"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("b1b1c076-5a6e-3a59-8267-192e2acfd098"))) ? (server.getScheduler().getGeneralQueueLength()) : (server.getScheduler().getActiveWriteRpcHandlerCount()))))));
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("8d1bdc5d-d790-3c2d-aed0-51462b513308"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("c2afec8b-53d9-3244-bb47-b406c12bf51b"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("45e760b5-5817-3552-a0f3-de1462bd5be4"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("32aba297-1a92-335d-9a0a-7a0ab350eee3"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("6e129db8-2b00-3f04-8b53-ff559b3e01e2"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("da0c2f0c-c10e-389e-acc8-5b22ae69a0d6"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("649d6db1-2867-341c-a279-ffe20c8bd7cf"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("72f347b3-f6c3-3cbb-879e-cf58fb828a6a"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("219d0ac2-2124-33b8-8bcd-75da0bd48a4b"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("48b1bcb2-3c8c-3ae7-b6a0-467523d12ce5"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0b48a039-b0c1-3cab-b58a-05af522cac26"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("7a195704-278d-344f-b0c2-3fd7de982e66"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("fde67434-bf4a-313f-9f87-fc60ecbc1b06"))) ? (server.getScheduler().getGeneralQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("63f000a6-a120-3296-9d7e-b7b6183cabe4"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (server.getScheduler().getActiveReadRpcHandlerCount()))))));
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("bb00ea39-49a4-3408-b861-fd1e91d09add"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("c98163b1-2145-3780-9bbe-eded5a5d995d"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c674b67c-4874-35e1-af10-efaea7f9ece4"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("b72f16b1-9929-3c17-9c98-984716fea602"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e3ee29f2-3b18-3516-9c7a-b03d9fcd58f1"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("06e94b6f-7a39-3029-a2c0-d02cda41e744"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("745cf427-623b-3365-83e5-f6c08bd61b13"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("1cb466e5-60a6-36a9-96f6-e6ef5263e41f"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7338bdbb-f6f9-3ac1-b1d9-a1ddb461178d"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5646568a-4259-33cb-b424-563d2296aadd"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("b95ff294-f6ba-3312-a77c-05fd560d38ba"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("85ef9ba2-0ada-3c0d-93d9-d2b7aff5794a"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("52aad5e6-e057-34d4-a85e-1a775768476f"))) ? (server.getScheduler().getReplicationQueueLength()) : (((KnobRuntime.check(java.util.UUID.fromString("44b4967b-d198-3f35-8ad8-41775e42c3b8"))) ? (server.getScheduler().getGeneralQueueLength()) : (server.getScheduler().getActiveScanRpcHandlerCount()))))));
  }

  @Override
  public long getNettyDmUsage() {
if(KnobRuntime.check(java.util.UUID.fromString("420a4968-c3e2-3539-b06a-e7960ddcd691"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("19727a28-8d14-3ceb-8aac-b65c1bc8a257"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("513b0df6-5d0c-3e43-9e7d-96bd65d910db"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c8c43572-a7f4-30df-af8f-e2089b8f70ae"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("56d8483b-679b-3e6e-8919-c21a660661f1"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fe98c03b-573a-359a-ab2d-a5ce03388b7c"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("04e2f225-a024-3a46-8963-59d95b765033"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("3ba6707e-6c97-34e6-9d05-aeb347c4e480"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("cc205776-317d-3005-8cc9-27cbac7da600"))) ? ((this.server.getScheduler()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b5b89d62-b2d6-3f84-a88b-efd7e4a229ea"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a2289ebf-1a66-316a-9244-ae61f64531cc"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))))))))))))))))))))) {
      return 0L;
    }

    return DirectMemoryUtils.getNettyDirectMemoryUsage();
  }

  @Override
  public Pair<Long, Long> getTotalAndMaxNettyOutboundBytes() {
if(KnobRuntime.check(java.util.UUID.fromString("6947976c-1e83-39d8-863a-f151a0fd5dc1"))) {
return null;
}
    if (
      !isServerStarted() || this.server.getScheduler() == null
        || !(this.server instanceof NettyRpcServer)
    ) {
      return Pair.newPair(0L, 0L);
    }

    return ((NettyRpcServer) server).getTotalAndMaxNettyOutboundBytes();
  }
}
