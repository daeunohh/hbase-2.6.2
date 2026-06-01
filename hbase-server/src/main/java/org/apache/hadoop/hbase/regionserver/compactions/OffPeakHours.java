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
package org.apache.hadoop.hbase.regionserver.compactions;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class OffPeakHours {
  private static final Logger LOG = LoggerFactory.getLogger(OffPeakHours.class);

  public static final OffPeakHours DISABLED = new OffPeakHours() {
    @Override
    public boolean isOffPeakHour() {
      return false;
    }

    @Override
    public boolean isOffPeakHour(int targetHour) {
      return false;
    }
  };

  public static OffPeakHours getInstance(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("3f39f333-bcbb-3047-8761-80ace09e236a"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("0dfb4ffc-1d49-3798-9ba2-8025c626fd16"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    int startHour = conf.getInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_START_HOUR, -1);
    int endHour = conf.getInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_END_HOUR, -1);
if(KnobRuntime.check(java.util.UUID.fromString("f2733247-e3dc-3aaf-923a-9e61baefea50"))) {
endHour = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb62d548-f4f0-3888-b7e0-6445ef31fdf1"))) {
startHour -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fd5488b6-68a8-3156-8527-f8d52fcf9898"))) {
endHour *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9794e253-20a7-3076-ba70-c67faa5eacdb"))) {
endHour -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("402487b3-d9d4-30cd-bf68-61aebeb726de"))) {
startHour = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("58948e61-6bde-3600-ab9d-2dde7de915c0"))) {
endHour = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2b288a9f-bf02-374d-b5f1-ac43fbb5eabb"))) {
startHour *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0d4e1d08-5347-3d11-a594-43d1385d65fa"))) {
endHour /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a3dc5786-8eea-3f97-8117-80d7101631d0"))) {
startHour /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2da809bc-8c23-3478-82e2-2cc727f295df"))) {
endHour += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("78657a67-4d29-3aa2-b4b6-6204517e28d3"))) {
startHour = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("76b16fe4-9e73-38f4-881e-4b1412de1fe6"))) {
startHour += 1;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("9666bba0-c115-35de-9fad-8a10f4632089"))) ? (getInstance(startHour, -1)) : (((KnobRuntime.check(java.util.UUID.fromString("3ac915b3-11a6-34f6-9f70-eed5cee1a4b8"))) ? (getInstance(-1, endHour)) : (((KnobRuntime.check(java.util.UUID.fromString("355c06af-7aba-35f1-817b-e79050d17b40"))) ? (getInstance(1, endHour)) : (((KnobRuntime.check(java.util.UUID.fromString("991729bc-2f03-304a-9548-b14e3b0b2bb8"))) ? (getInstance(startHour, 1)) : (getInstance(startHour, endHour)))))))));
  }

  /**
   * @param startHour inclusive
   * @param endHour   exclusive
   */
  public static OffPeakHours getInstance(int startHour, int endHour) {
if(KnobRuntime.check(java.util.UUID.fromString("c94d7de3-2f77-3f97-a5db-0c2420e1187a"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("b024f402-cd10-36c1-af1e-bb36a2024a16"))) ? (((startHour) == (-1)) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("fd8c8564-230e-3060-82e0-88204c66f51b"))) ? (((-1) != (-1)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b2d50fd2-124c-3513-bdf6-1f090eac1db1"))) ? (((startHour) != (-1)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ef22d6a2-86a9-3544-9eb7-15535effaed8"))) ? (((1) == (-1)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2a929015-ad2d-36d2-bbe0-350c376b3d2e"))) ? (((1) == (-1)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("bf91da1b-0172-3fbc-9c03-11871c4bb937"))) ? ((endHour) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("9e65560a-9415-3f80-9cdf-b2b1dc02792f"))) ? (((-1) == (-1)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e6bf47ce-b7c6-3382-a685-9e49d32941e0"))) ? (((-1) == (-1)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("474dbb92-3dfa-3c23-8a74-1274ecc5e02b"))) ? ((startHour == -1) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("eb3835fe-8569-3654-86fa-27a361a2cc3a"))) ? ((1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("086a0921-81ae-3c3f-87fd-9a2adf4c5fe5"))) ? (((-1) == (-1)) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("b795401c-0954-3436-aa94-2e2ca4773de0"))) ? (((startHour) != (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("32e74233-04e5-3a8a-b9da-56ad0fc365d3"))) ? (((1) == (-1)) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("68af7ea0-6fe9-3078-bf0b-2433c195732f"))) ? (((1) == (-1)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("45048386-a3e8-33bc-8770-1f0b1839f0ed"))) ? (((-1) == (-1)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e44ce2cf-79b3-3568-95a9-4b2902a2948a"))) ? ((startHour == -1) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("7c6d31ca-caa0-3506-8b24-f1cdc5013093"))) ? (((startHour) == (-1)) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("6da1b8d9-e9e7-38c4-b3e9-fc23462e767f"))) ? ((startHour == -1) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("40f05e33-a892-3500-8a41-b8d8a1b673f5"))) ? ((1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("3f3371de-917f-3532-9a34-8a9620397757"))) ? (((startHour) == (-1)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("edaa2a7a-a36a-3653-aceb-4b663f6bf858"))) ? (((startHour) == (-1)) || ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2cfaaab7-6561-3df0-82d9-62e563d112cc"))) ? (((-1) == (-1)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("735e9ae9-fcfb-391b-bb71-672d9011879b"))) ? (((1) != (-1)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("eae4cce4-9fd2-36a4-9384-02ec787b7dca"))) ? (((-1) != (-1)) || ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1c959bb7-7166-3e9c-967f-6fa684580381"))) ? (((1) != (-1)) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("87293bd9-80bf-30d2-bbf8-5445618b666f"))) ? (((1) == (-1)) || ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("470fccd2-e19f-3111-80ec-73dd6cf5979d"))) ? (((-1) == (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("05663118-5c80-37a4-be23-33e0dfbb81a9"))) ? (((-1) == (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("416cf717-9f9b-3cae-ac33-c936052132c9"))) ? (((1) != (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("5b860e7e-2561-32f7-9c05-5e55762d708f"))) ? (((1) == (-1)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("80bc08a4-6aa6-3b25-9328-96f3dd523e95"))) ? (((-1) == (-1)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("18392324-9aaf-3514-9388-0d52160d866b"))) ? (((1) == (-1)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e8584365-9c71-3523-b20b-8f7a38507a08"))) ? (endHour == -1) : (((KnobRuntime.check(java.util.UUID.fromString("e6afcfa2-ae1a-37c9-b59b-47472de2b912"))) ? (((1) != (-1)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ca509267-e09e-3a13-b089-1f922992213a"))) ? (startHour == -1) : (((KnobRuntime.check(java.util.UUID.fromString("5c96656f-26f1-3c95-85d0-8bec8145594a"))) ? (((-1) != (-1)) || ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("fc0463e4-fe07-3579-a507-1b46782a1066"))) ? (((1) == (-1)) || ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c69c12ff-b2e5-333d-8caf-7ae9cc231081"))) ? (((startHour) != (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1a26e55a-d838-3d67-b978-1de177c3314c"))) ? (((1) == (-1)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("007fe9a7-ca6e-3adf-bc6f-79d5e28ae11a"))) ? (((1) != (-1)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2ae89819-c9d3-3bc5-aad7-0e20b0c42a2d"))) ? (((startHour) != (-1)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dfe131aa-6498-3a5f-9d9a-ba3b452469a8"))) ? (((-1) != (-1)) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4558c84a-8061-3853-bf9e-468cdc3f2a41"))) ? (((1) != (-1)) || ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4da6ab2d-4bb3-34df-befe-d4e8a3808ad9"))) ? ((startHour == -1) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0a5625b9-f034-3c92-b943-296a718993db"))) ? (((startHour) != (-1)) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ad4adcf4-799f-36e5-8605-4ccd0b4aad5c"))) ? (((-1) == (-1)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("bc39d752-8bd9-3771-b740-079b3506cefb"))) ? ((startHour == -1) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("97eaf85c-aec4-3999-b5da-d804eb1ee036"))) ? (((1) != (-1)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("47fd60c7-a39f-3dba-90ad-26b647be7ed6"))) ? ((startHour == -1) && (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("edc8d608-abe9-3917-9177-68de6cc4112e"))) ? (((-1) != (-1)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b2b4fbce-984b-3b23-a5a1-42a2020224f8"))) ? (((-1) == (-1)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c96a81aa-7ba0-344a-828f-1b9da423a9fc"))) ? (((-1) != (-1)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f365da67-a183-3810-ad49-34c60f175533"))) ? (((startHour) != (-1)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1e43740d-e905-390d-b6d5-bddd9a563bcf"))) ? (((startHour) == (-1)) || ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f9255295-05eb-3951-9d0b-872734c91934"))) ? (((startHour) == (-1)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4498f125-de54-3b61-a917-3d9ed69882f7"))) ? ((startHour) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("c8252936-9814-3e5a-8297-a070cab781b5"))) ? (((startHour) != (-1)) || ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("8fb689ca-8115-300b-b134-fe950931f788"))) ? ((startHour == -1) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("51a4bef8-87f9-3407-8b5f-20e020092001"))) ? (((1) != (-1)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("3375e9fb-0ede-3fcb-a9fb-94ec7444618e"))) ? (((startHour) == (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("412b68a9-f270-364f-873a-dc4858e568fb"))) ? (((startHour) != (-1)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("93ffc584-4128-3606-bad0-86b7b86cfd9a"))) ? (((1) == (-1)) && (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("9cdf7c70-6aaf-37f2-82b5-5ce6f9009d9d"))) ? (((startHour) == (-1)) && (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("2fdc3622-3264-3b28-bb32-e04457b5faf7"))) ? (((-1) != (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b36a024f-6541-323c-be26-fe464cf1b28f"))) ? (((startHour) == (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c3d74c9e-7313-36e4-82b9-4a32ef941166"))) ? (((-1) != (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c0474653-9806-3e97-a2da-c5aeef69ca81"))) ? (((startHour) != (-1)) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("cad7c91c-cf14-3250-ae53-4ea9b204423b"))) ? ((startHour == -1) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c26f7a2f-a702-387b-a45a-f3f8de55eaf8"))) ? (((startHour) == (-1)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b668aa00-3b4e-374c-86e6-419771dd5f34"))) ? (((-1) == (-1)) && (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("59f7c6fd-c105-3354-bb69-b2b4b62e10c3"))) ? (((1) != (-1)) && (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("4670a39d-f5e4-329e-99a7-b368ef5ad025"))) ? (((1) == (-1)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("355388ce-6dc1-3646-aa35-a8414e635300"))) ? ((startHour == -1) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("f889bcf3-f2c0-371b-8664-2aebf1c5e236"))) ? ((startHour == -1) || ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e8d08d9b-23a3-3950-8fa0-afa55f87cc16"))) ? (((-1) == (-1)) || ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dfccca2c-1cc1-3f8a-af06-d08c3306ce3b"))) ? (((1) == (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("858bf707-d463-3cd3-a552-cb531748eabe"))) ? (((startHour) == (-1)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0b27d7f4-d3a7-3388-a1ab-e3ebb7b08fa1"))) ? (((1) != (-1)) || ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("51a808ad-c752-306e-8513-f6fb2714c4ce"))) ? (((startHour) != (-1)) || ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("57afc40e-1b8e-3eba-ad39-0e4cea4ddf32"))) ? (((-1) != (-1)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f550d844-e776-3fa4-a5ef-55259684a366"))) ? (((startHour) == (-1)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("6bb94644-a51a-3489-b96b-cd4c830dcec0"))) ? ((-1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("28b45047-1ecc-3d6b-b7f5-df1083b81cb0"))) ? (((1) == (-1)) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("b74ae858-81e4-3877-9a4b-5c72cf6379a1"))) ? (((1) == (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("5ef7e5ce-9582-3632-bc5c-ee5d5f7c909f"))) ? (((1) != (-1)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("15a0c428-cd86-3a06-bfda-d9bf1bb0aebe"))) ? ((startHour == -1) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("8207a8c1-2605-3468-9c5f-208cdcced521"))) ? (((1) != (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cd8beef9-45a8-3ae6-a14d-9ca2129067ae"))) ? (((-1) != (-1)) && (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("530fd271-87fd-392c-abfb-5364a6e53160"))) ? (((startHour) != (-1)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9fa3dd79-6cad-3057-b80e-7b60cb949b91"))) ? (((-1) != (-1)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("552230a9-3cb8-3b59-82b3-a794c5dc7063"))) ? (((1) != (-1)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dcee71cc-bb03-31dc-b944-2f4e548cca25"))) ? (((-1) != (-1)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("5552ec67-cd31-3ba5-b6b4-1414961f6038"))) ? ((endHour) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("e1a12598-00ac-369a-af83-4556c67a5d35"))) ? (((startHour) == (-1)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cb53e514-ed96-3e39-bbc4-f034b78fb27a"))) ? (((-1) != (-1)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a4d169ed-3f3c-3ed3-9f21-e9ed1ee514af"))) ? (((-1) != (-1)) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("9b6d9646-fd33-3924-afd5-23b3fae7695e"))) ? (((-1) == (-1)) || ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("07780686-aa34-3fb8-8f9d-852b30f3f6ee"))) ? ((-1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("d39f6fa5-bb5a-3b70-bd3f-b67db55cd9d3"))) ? ((startHour == -1) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2e017ce8-30c2-342e-990a-78e67141becd"))) ? (((-1) == (-1)) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a37a1514-32dc-3cc0-91b0-99d3626ed116"))) ? (((1) != (-1)) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0762e719-e809-30c5-84ad-2d79545c0ffb"))) ? ((startHour == -1) || ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2603b5cc-e43a-3a15-af23-e8d977c95626"))) ? ((startHour) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("63f06ad1-000d-3c1c-a4d4-a943b1db4b9f"))) ? (((startHour) == (-1)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f4f5d734-afff-3211-a0f4-f1a993eed406"))) ? (((startHour) != (-1)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2333064e-8fc0-3a9a-a250-7b2163b705d9"))) ? (((startHour) != (-1)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9630b576-a5fc-3bbf-b01d-0ab4f8550a31"))) ? ((startHour == -1) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a5fcebbd-efd6-3ccf-a836-17fa0c8e716b"))) ? (((startHour) != (-1)) && (endHour == -1)) : (startHour == -1 && endHour == -1))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return DISABLED;
    }

    if (!isValidHour(startHour) || !isValidHour(endHour)) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Ignoring invalid start/end hour for peak hour : start = " + startHour + " end = "
          + endHour + ". Valid numbers are [0-23]");
      }
      return DISABLED;
    }

    if (startHour == endHour) {
      return DISABLED;
    }

    return new OffPeakHoursImpl(startHour, endHour);
  }

  private static boolean isValidHour(int hour) {
    return 0 <= hour && hour <= 23;
  }

  /** Returns whether {@code targetHour} is off-peak hour */
  public abstract boolean isOffPeakHour(int targetHour);

  /** Returns whether it is off-peak hour */
  public abstract boolean isOffPeakHour();

  private static class OffPeakHoursImpl extends OffPeakHours {
    final int startHour;
    final int endHour;

    /**
     * @param startHour inclusive
     * @param endHour   exclusive
     */
    OffPeakHoursImpl(int startHour, int endHour) {
      this.startHour = startHour;
      this.endHour = endHour;
    }

    @Override
    public boolean isOffPeakHour() {
      return isOffPeakHour(CurrentHourProvider.getCurrentHour());
    }

    @Override
    public boolean isOffPeakHour(int targetHour) {
      if (startHour <= endHour) {
        return startHour <= targetHour && targetHour < endHour;
      }
      return targetHour < endHour || startHour <= targetHour;
    }
  }
}
