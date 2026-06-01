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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.LockStatus;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class TableQueue extends Queue<TableName> {
  private final LockStatus namespaceLockStatus;

  public TableQueue(TableName tableName, int priority, LockStatus tableLock,
    LockStatus namespaceLockStatus) {
    super(tableName, priority, tableLock);
    this.namespaceLockStatus = namespaceLockStatus;
  }

  @Override
  public boolean isAvailable() {
if(KnobRuntime.check(java.util.UUID.fromString("357ec62b-394d-3a63-a461-43d2abd8df19"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("49f9f471-d536-35fa-83e2-bcae1e2b871a"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("b0c73844-5667-3832-a7f4-479f2902686d"))) ? ((!isEmpty()) && (!namespaceLockStatus.hasExclusiveLock())) : (((KnobRuntime.check(java.util.UUID.fromString("51d4f9eb-22b6-3eb7-9f6e-8e717836d50d"))) ? (!isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("3b52075f-56d5-38fc-9b4e-0486f05fc447"))) ? (!namespaceLockStatus.hasExclusiveLock()) : (((KnobRuntime.check(java.util.UUID.fromString("1ab0b448-e17a-38c4-8e1a-a96a846d5319"))) ? ((!isEmpty()) || (!namespaceLockStatus.hasExclusiveLock())) : (!isEmpty() && !namespaceLockStatus.hasExclusiveLock()))))))));
  }

  @Override
  public boolean requireExclusiveLock(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("3470d682-1eff-3252-9d28-6d6b22c28a91"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3c39e05-53cb-3344-933f-2f2a87983212"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("46c8f89a-fc9e-3ce1-b737-9c07f2d35818"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("95a9fefa-c843-38f3-b6b1-9157d356d3b3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0d0728eb-11a5-3552-96e8-e83ba3177572"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("04440944-253f-3266-b966-bff6aa0d6f58"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("da10ad23-a2b4-3049-b6a3-ece641c3a93d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f3af852-6963-3c83-9036-8f7a7fc20c7f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("74b63d72-df63-371c-b349-755e7b711492"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("43187380-3129-3c04-9008-bc870965023b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f8f20ba-18ec-3e9b-922b-59ac32ac8e05"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2fff40a8-30a1-39f8-bfba-4a3cb99edf55"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("452f7618-729f-3df8-9419-d52a22aab521"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c017db44-d3e9-336b-95d3-ff5b15d13408"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5437deb0-ef8d-39f5-a045-a21854e1f6a9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("e5adb096-9dad-33b5-8831-5c9d4a316a26"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7b356c59-bc55-3f35-ad0b-9a49ea18aa7f"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2b9649c0-b786-3e79-a0ca-5a324d8ad327"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("61cb01dc-21b8-33f5-9183-03aef5c0cda6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5add7dd0-1820-301c-813c-333e75f3709b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("198c92fb-83a0-3194-9e0c-01b3e4f32734"))) {
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
    return requireTableExclusiveLock((TableProcedureInterface) proc);
  }

  /**
   * @param proc must not be null
   */
  static boolean requireTableExclusiveLock(TableProcedureInterface proc) {
if(KnobRuntime.check(java.util.UUID.fromString("90204812-1fff-329b-9509-4f6bc8065373"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("7e77c170-efad-37cc-8157-874dc92efda7"))) {
return false;
}
    switch (proc.getTableOperationType()) {
      case CREATE:
      case DELETE:
      case DISABLE:
      case ENABLE:
        return true;
      case EDIT:
        // we allow concurrent edit on the NS table
        return !proc.getTableName().equals(TableName.NAMESPACE_TABLE_NAME);
      case READ:
      case FLUSH:
      case SNAPSHOT:
        return false;
      // region operations are using the shared-lock on the table
      // and then they will grab an xlock on the region.
      case REGION_SPLIT:
      case REGION_MERGE:
      case REGION_ASSIGN:
      case REGION_UNASSIGN:
      case REGION_EDIT:
      case REGION_GC:
      case MERGED_REGIONS_GC:
      case REGION_SNAPSHOT:
      case REGION_TRUNCATE:
        return false;
      default:
        break;
    }
    throw new UnsupportedOperationException("unexpected type " + proc.getTableOperationType());
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString())
      .append("namespaceLockStatus", namespaceLockStatus.describeLockStatus()).build();
  }
}
