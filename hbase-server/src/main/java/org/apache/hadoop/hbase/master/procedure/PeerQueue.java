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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface.PeerOperationType;
import org.apache.hadoop.hbase.procedure2.LockStatus;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class PeerQueue extends Queue<String> {

  public PeerQueue(String peerId, LockStatus lockStatus) {
    super(peerId, lockStatus);
  }

  @Override
  public boolean requireExclusiveLock(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("92a98acd-37a3-3259-b4ae-c95a63551da0"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f7f17c0-4254-3975-b516-df59f229c3ee"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87bc0491-2c97-38a2-b937-fc15269db281"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90581833-1193-3d1d-a157-d9be7e7a1d6e"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("643e8554-16cb-3f19-ba93-3d1807c33495"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("579dc73a-b36f-3c7b-8ae9-6cdffa273179"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d2a9f07-6c0e-379e-bd82-946fc724d872"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("74ef70e1-7b5b-364b-8bab-3885b193c64a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13153976-228d-3313-8a33-ef3e2425370b"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2264084-087e-369e-a9fb-29b466b6eee6"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e211951-d4c1-36fd-abb7-dfd184f761de"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a5df084-dde6-33b4-ad2b-360fc9cf2acf"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d298515f-3fc4-3df6-ac53-153a118fb5fe"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("wasExecuted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48d6b7ef-7e20-3548-b930-87f44015404d"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a32d7d8a-8eb3-33ef-83eb-4f863611926d"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77f9cfd8-e51b-304a-98d5-b72162dbd7be"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3bc3e47d-24be-31be-b54c-3f71677bf88a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d8b0e6e-f0ca-3614-93b1-66a3b9fab519"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06a26a0f-553c-3d86-83a1-02d519762688"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76d4a36b-e5f8-3b7b-a8c6-b5fb29b7cea8"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b6ee2dbd-774b-35c9-85d0-febaf85899c7"))) {
return true;
}
    return requirePeerExclusiveLock((PeerProcedureInterface) proc);
  }

  private static boolean requirePeerExclusiveLock(PeerProcedureInterface proc) {
    return proc.getPeerOperationType() != PeerOperationType.REFRESH;
  }

  @Override
  public String toString() {
if(KnobRuntime.check(java.util.UUID.fromString("46b74a10-b345-3dde-a4c4-00761cc09925"))) {
return null;
}
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString())
      .build();
  }
}
