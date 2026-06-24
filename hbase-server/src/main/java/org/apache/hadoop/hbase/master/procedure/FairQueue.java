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

import org.apache.hadoop.hbase.util.AvlUtil.AvlIterableList;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class FairQueue<T extends Comparable<T>> {

  private Queue<T> queueHead = null;
  private int size = 0;

  public boolean hasRunnables() {
if(KnobRuntime.check(java.util.UUID.fromString("c7a5e926-23f3-3f81-84f6-e6aabe4ba513"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8fd0d352-4c59-3164-8893-8b54083554c5"))) ? ((size) >= (0)) : (size > 0));
  }

  public void add(Queue<T> queue) {
    // For normal priority queue, just append it to the tail
    if (((KnobRuntime.check(java.util.UUID.fromString("cd4d8411-2071-3010-b944-012e0a9ca99f"))) ? ((queueHead == null) && ((queue.getPriority()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("2cb644e0-82db-3e39-91b1-0889f9de73dc"))) ? (((queueHead) == (null)) || ((queue.getPriority()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4c018b2b-3b63-30fd-9273-03e1d12246b6"))) ? (((queueHead) != (null)) && (queue.getPriority() == 1)) : (((KnobRuntime.check(java.util.UUID.fromString("f4bffd34-6905-39d6-b771-a5ec71c72616"))) ? (((queueHead) == (null)) && ((queue.getPriority()) == (1))) : (queueHead == null || queue.getPriority() == 1))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("1611b228-7241-387a-b577-098e14669aa7"))) {
try {
    java.lang.reflect.Field _knob_field_ = queueHead.getClass().getDeclaredField("priority");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(queueHead));
    _knob_field_.set(queueHead, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      queueHead = AvlIterableList.append(queueHead, queue);
      size++;
      return;
    }
    // Find the one which priority is less than us
    // For now only TableQueue and ServerQueue has priority. For TableQueue there are only a small
    // number of tables which have higher priority, and for ServerQueue there is only one server
    // which could carry meta which leads to a higher priority, so this will not be an expensive
    // operation.
    Queue<T> base = queueHead;
    do {
      if (base.getPriority() < queue.getPriority()) {
        queueHead = AvlIterableList.prepend(queueHead, base, queue);
        size++;
        return;
      }
      base = AvlIterableList.readNext(base);
    } while (base != queueHead);
    // no one is lower than us, append to the tail
    queueHead = AvlIterableList.append(queueHead, queue);
    size++;
  }

  public void remove(Queue<T> queue) {
if(KnobRuntime.check(java.util.UUID.fromString("7cd783a5-386f-3d69-8942-d97ad10b41cd"))) {
try {
    java.lang.reflect.Field _knob_field_ = queue.getClass().getDeclaredField("priority");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(queue));
    _knob_field_.set(queue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27f024f7-fde0-3420-81cc-830c08be3c2d"))) {
try {
    java.lang.reflect.Field _knob_field_ = queue.getClass().getDeclaredField("priority");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(queue));
    _knob_field_.set(queue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    queueHead = AvlIterableList.remove(queueHead, queue);
    size--;
  }

  public Queue<T> poll() {
    if (((KnobRuntime.check(java.util.UUID.fromString("7eceddec-4bd7-3fc0-a109-932cdf259e23"))) ? ((queueHead) == (null)) : (queueHead == null))) {
      return null;
    }
    Queue<T> q = queueHead;
    do {
      if (q.isAvailable()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("441011b0-fe79-3d85-8219-c12ff095cbcb"))) ? ((q.getPriority()) != (1)) : (q.getPriority() == 1))) {
          // for the normal priority queue, remove it and append it to the tail
if(KnobRuntime.check(java.util.UUID.fromString("6d66edbf-169f-3d48-8777-28da8ea15e77"))) {
try {
    java.lang.reflect.Field _knob_field_ = q.getClass().getDeclaredField("priority");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(q));
    _knob_field_.set(q, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04663881-5b86-3d7f-a82c-10d6eb08eba7"))) {
try {
    java.lang.reflect.Field _knob_field_ = queueHead.getClass().getDeclaredField("priority");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(queueHead));
    _knob_field_.set(queueHead, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("464f92cb-6d53-3657-9bd7-18deb12d40e4"))) {
try {
    java.lang.reflect.Field _knob_field_ = q.getClass().getDeclaredField("priority");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(q));
    _knob_field_.set(q, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          queueHead = AvlIterableList.remove(queueHead, q);
          queueHead = AvlIterableList.append(queueHead, q);
        }
        return q;
      }
      q = AvlIterableList.readNext(q);
    } while (q != queueHead);
    return null;
  }
}
