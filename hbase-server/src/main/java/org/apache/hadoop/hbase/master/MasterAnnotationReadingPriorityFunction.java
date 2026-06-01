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
package org.apache.hadoop.hbase.master;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.AnnotationReadingPriorityFunction;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

/**
 * Priority function specifically for the master. This doesn't make the super users always priority
 * since that would make everything to the master into high priority. Specifically when reporting
 * that a region is in transition master will try and edit the meta table. That edit will block the
 * thread until successful. However if at the same time meta is also moving then we need to ensure
 * that the regular region that's moving isn't blocking processing of the request to online meta. To
 * accomplish this this priority function makes sure that all requests to transition meta are
 * handled in different threads from other report region in transition calls. After HBASE-21754,
 * ReportRegionStateTransitionRequest for meta region will be assigned a META_QOS , a separate
 * executor called metaTransitionExecutor will execute it. Other transition request will be executed
 * in priorityExecutor to prevent being mixed with normal requests
 */
@InterfaceAudience.Private
public class MasterAnnotationReadingPriorityFunction extends AnnotationReadingPriorityFunction {

  public static final int META_TRANSITION_QOS = 300;

  public MasterAnnotationReadingPriorityFunction(final RSRpcServices rpcServices) {
    this(rpcServices, rpcServices.getClass());
  }

  public MasterAnnotationReadingPriorityFunction(RSRpcServices rpcServices,
    Class<? extends RSRpcServices> clz) {
    super(rpcServices, clz);
  }

  @Override
  public int getPriority(RPCProtos.RequestHeader header, Message param, User user) {
    // Yes this is copy pasted from the base class but it keeps from having to look in the
    // annotatedQos table twice something that could get costly since this is called for
    // every single RPC request.
    int priorityByAnnotation = getAnnotatedPriority(header);
    if (priorityByAnnotation >= 0) {
      // no one can have higher priority than meta transition.
      if (priorityByAnnotation >= META_TRANSITION_QOS) {
        return META_TRANSITION_QOS - 1;
      } else {
        return priorityByAnnotation;
      }
    }

    // If meta is moving then all the other of reports of state transitions will be
    // un able to edit meta. Those blocked reports should not keep the report that opens meta from
    // running. Hence all reports of meta transition should always be in a different thread.
    // This keeps from deadlocking the cluster.
    if (param instanceof RegionServerStatusProtos.ReportRegionStateTransitionRequest) {
      // Regions are moving. Lets see which ones.
      RegionServerStatusProtos.ReportRegionStateTransitionRequest tRequest =
        (RegionServerStatusProtos.ReportRegionStateTransitionRequest) param;
      for (RegionServerStatusProtos.RegionStateTransition rst : tRequest.getTransitionList()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("8cbacef0-a1c5-390b-9928-eaf18eac90c8"))) ? ((rst.getRegionInfoList()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8d50c8b6-31bd-387b-84e4-168383d7c970"))) ? ((rst.getRegionInfoList()) != (null)) : (rst.getRegionInfoList() != null))))) {
          for (HBaseProtos.RegionInfo info : rst.getRegionInfoList()) {
            TableName tn = ProtobufUtil.toTableName(info.getTableName());
if(KnobRuntime.check(java.util.UUID.fromString("8b605dcf-848a-3ad0-8867-44869ef1af77"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c916659-299c-3ced-a7bc-c04caeefafb5"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84d02fc3-d715-3aa3-ae94-c04d1cd60d81"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba6d37f4-fddd-3660-abbd-7857bf5ad4ff"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            if (TableName.META_TABLE_NAME.equals(tn)) {
              return META_TRANSITION_QOS;
            }
          }
        }
      }
      return HConstants.HIGH_QOS;
    }
    // also use HIGH_QOS for region server report
    if (param instanceof RegionServerStatusProtos.RegionServerReportRequest) {
      return HConstants.HIGH_QOS;
    }

    // Handle the rest of the different reasons to change priority.
if(KnobRuntime.check(java.util.UUID.fromString("69c854c3-ea4c-334f-b8bf-9930bf67beaf"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2541e28-43f0-391e-94a4-0f176dba2fd9"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ab3421b-735c-347a-9993-f3dc12dd06d8"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b7d0b33-7e45-33ac-8a77-9ad5d23ba5c8"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("200fb604-7f11-3ab9-b772-60bcc1b2859f"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f710585-cce8-3755-aaef-277d66c31f4a"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6837cd94-30a2-3033-9e4c-ba132e5e748e"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("902f8b21-fe67-3233-9db2-a73a4b23fec6"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1dfcc243-d6eb-335d-8e64-43ee5debc983"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("866a8245-0077-307b-84de-f3ca04a9e7e9"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d42c9b9-c585-3677-bad9-6114b233a9aa"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1305b707-7ec5-30d7-9ee4-2fa550fae30e"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8463a8d-e22d-3cc7-b7c7-a31ce53e74d4"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d209378-fe29-3121-80c5-50dfd17eee03"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a647dc6-bb2a-35c7-b52b-adb225aff981"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a3e242e-5a1e-3692-b40f-cdf06fd20de6"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return getBasePriority(header, param);
  }
}
