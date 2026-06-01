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

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * To prevent multiple Create/Modify/Disable/Enable table procedures run at the same time, we will
 * keep table procedure in this queue first before actually enqueuing it to
 * MasterProcedureScheduler's tableQueue. See HBASE-28683 for more details
 */
@InterfaceAudience.Private
class TableProcedureWaitingQueue {

  private final Function<Long, Procedure<?>> procedureRetriever;

  // whether there is already a table procedure enqueued in ProcedureScheduler.
  private Procedure<?> enqueuedProc;

  private final Queue<Procedure<?>> queue = new ArrayDeque<>();

  TableProcedureWaitingQueue(Function<Long, Procedure<?>> procedureRetriever) {
    this.procedureRetriever = procedureRetriever;
  }

  private boolean isSubProcedure(Procedure<?> proc) {
    while (proc.hasParent()) {
      if (proc.getParentProcId() == enqueuedProc.getProcId()) {
        return true;
      }
      proc = Preconditions.checkNotNull(procedureRetriever.apply(proc.getParentProcId()),
        "can not find parent procedure pid=%s", proc.getParentProcId());
    }
    return false;
  }

  /**
   * Return whether we can enqueue this procedure to ProcedureScheduler.
   * <p>
   * If returns {@code true}, you should enqueue this procedure, otherwise you just need to do
   * nothing, as we will queue it in the waitingQueue, and you will finally get it again by calling
   * {@link #procedureCompleted(Procedure)} method in the future.
   */
  boolean procedureSubmitted(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("5211d449-ea35-3985-ad94-d3c7cd5670ea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("00247179-bde4-3af1-9335-d0c54538d2f9"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("1a2445c2-877c-32d1-b100-83f3ab1719af"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef91223a-8bca-32a2-8976-73c39cd595a4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7e6d0d0b-745c-390d-9cf6-689c5de3bc45"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("076ec2ed-fac5-3110-9b13-928037d4cddb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("89d2ab65-6c46-3b28-8606-008c4e912407"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("432b60f9-2e70-3281-aa50-fcc53873cc4f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e1ee51b0-f301-3a8f-86fe-1abbb671533f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b9b143fe-00bc-3de9-b303-e2a1d52d930d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7b382dbf-0e28-3ae2-979f-6984124f8e23"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1ddf224e-4c5a-3279-9065-c1f2e84d9739"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d1536199-f1e6-3ebd-b42b-bc25f23bbd72"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e37368b1-1043-3c69-983c-69e19790b4e9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("4ccd1244-84c6-3eac-84ed-5e079fb12156"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a8e8b3e0-1890-3353-8fac-0aaf182454e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c48dd183-ac98-3ed5-9a55-5053fbb1d32b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("af16c6e4-f2f7-3916-92d6-be53e8e1a73b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3e90c2d3-5a54-3790-a3cc-2727e67cb03d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("182c04e0-6b2f-3ab3-bbd2-96b7886bbc2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd8515e4-9670-35ff-9baa-bc84417cf0f4"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("b9b3ac97-4134-3725-aa45-0f1f2fdb18b7"))) ? ((enqueuedProc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("702de189-43aa-3241-8c2f-c62fd5b5ffe1"))) ? ((enqueuedProc) != (null)) : (enqueuedProc == null))))) {
      // no procedure enqueued yet, record it and return
      enqueuedProc = proc;
      return true;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("fdd1d093-1be1-315e-9c41-6dc47b8792fc"))) ? ((proc) != (enqueuedProc)) : (((KnobRuntime.check(java.util.UUID.fromString("f4576f09-c044-3268-ba15-4a5f0bbf5ec0"))) ? ((proc) == (enqueuedProc)) : (proc == enqueuedProc))))) {
      // the same procedure is enqueued again, this usually because the procedure comes back from
      // WAITING state, such as all child procedures are finished
      return true;
    }
    // check whether this is a sub procedure of the enqueued procedure
    if (isSubProcedure(proc)) {
      return true;
    }
    queue.add(proc);
    return false;
  }

  /**
   * Return the next procedure which can be enqueued to ProcedureScheduler.
   */
  Optional<Procedure<?>> procedureCompleted(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("9071082b-76b6-3589-96cc-821b2b58cc0f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cbe58288-02ec-38f8-a0f0-4724ad9247fa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cc487614-46c4-3936-bd11-02fb1f6f7bd7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fdfddfd-35b2-357f-a603-0a1a84708274"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("79d094a4-797f-3cb9-9fb3-4c1bb8e71d96"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e352ccc0-e42e-3dc1-b6a2-10e5fe8f6b24"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dac20ad3-898e-39fb-b84b-2ec787b000e1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("290d541c-afba-30f8-8a97-58e67751d308"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01978407-7334-35b6-8da9-96cd29335704"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c2356bb6-5797-3d01-82ca-e97dbadec2e6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("68de772d-ea57-36b8-bc09-3b98544d8dfd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("db7d2cf5-0239-3984-95c4-f5c0dcaab7a9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0a87fa61-f9cc-340b-b1ef-eeb46a86952b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("be1afb14-3441-3268-8293-d20e16f14d12"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3d30df79-28c6-380d-9cd3-4c0bd8eeb8fe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e47fc64d-2b88-3012-b646-ccc1f9bf72f8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("29a5d531-5723-343e-bfce-8248aa955f3a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fb9df00f-5777-367f-a41b-90f2c10b7549"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a0954fa4-6cd8-399b-ab52-c1a87d14bca7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eb49b164-c5f6-3092-9e58-8e82aa9c0716"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("299c63fd-2d46-33d7-bcd7-a8d70c644c9a"))) { Preconditions.checkState((enqueuedProc) != (null), "enqueued procedure should not be null"); } else if (KnobRuntime.check(java.util.UUID.fromString("684c9835-6bce-36cd-8e82-d223a82c7090"))) { Preconditions.checkState((enqueuedProc) == (null), "enqueued procedure should not be null"); } else { Preconditions.checkState(enqueuedProc != null, "enqueued procedure should not be null"); }
    if (((KnobRuntime.check(java.util.UUID.fromString("cc32e8d0-db2c-32bf-91bf-d78cbe738517"))) ? ((enqueuedProc) == (proc)) : (((KnobRuntime.check(java.util.UUID.fromString("2f624ee8-c23f-357d-b2eb-2564f9171458"))) ? ((enqueuedProc) != (proc)) : (enqueuedProc == proc))))) {
      if (!queue.isEmpty()) {
        enqueuedProc = queue.poll();
        return Optional.of(enqueuedProc);
      } else {
        enqueuedProc = null;
        return Optional.empty();
      }
    } else {
      Preconditions.checkState(isSubProcedure(proc),
        "procedure %s is not a sub procedure of enqueued procedure %s", proc, enqueuedProc);
      return Optional.empty();
    }
  }

  boolean isEmpty() {
if(KnobRuntime.check(java.util.UUID.fromString("62b23386-ba06-3d17-82bd-caedc83b7248"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("05c26af3-8968-31c9-a96f-e2ea701d7500"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("c35de74d-eadb-3f6f-b34a-d086aa05024c"))) ? ((enqueuedProc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b97966cb-e3c0-355d-ba23-048aa4b59f9f"))) ? ((enqueuedProc) == (null)) : (enqueuedProc == null))));
  }

  int waitingSize() {
    return queue.size();
  }

  @Override
  public String toString() {
    return "TableProcedureWaitingQueue [enqueuedProc=" + enqueuedProc + ", queue=" + queue + "]";
  }
}
