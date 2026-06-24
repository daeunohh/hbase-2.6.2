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
if(KnobRuntime.check(java.util.UUID.fromString("25a1c32a-31cc-3064-949f-d5c5b32118ae"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (enqueuedProc == null) {
      // no procedure enqueued yet, record it and return
      enqueuedProc = proc;
      return true;
    }
    if (proc == enqueuedProc) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fabfd314-67f6-34ea-87e6-2706111f08f5"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6a77d951-b029-3dc2-8425-ca7dba6a184a"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Preconditions.checkState(enqueuedProc != null, "enqueued procedure should not be null");
    if (((KnobRuntime.check(java.util.UUID.fromString("2f624ee8-c23f-357d-b2eb-2564f9171458"))) ? ((enqueuedProc) != (proc)) : (enqueuedProc == proc))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05c26af3-8968-31c9-a96f-e2ea701d7500"))) {
return false;
}
    return enqueuedProc == null;
  }

  int waitingSize() {
    return queue.size();
  }

  @Override
  public String toString() {
    return "TableProcedureWaitingQueue [enqueuedProc=" + enqueuedProc + ", queue=" + queue + "]";
  }
}
