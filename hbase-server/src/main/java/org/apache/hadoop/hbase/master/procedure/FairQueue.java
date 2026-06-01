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
if(KnobRuntime.check(java.util.UUID.fromString("8e1b3026-0831-3477-b196-c88c85a17c48"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("174afb85-94cb-3aff-8cd2-24734a612bf9"))) ? ((size) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("68a35977-64c2-3489-aa0a-413b18fa0b01"))) ? ((size) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5918cc16-0414-3778-9749-2a1b2c99ee65"))) ? ((size) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8fd0d352-4c59-3164-8893-8b54083554c5"))) ? ((size) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("102585e8-cdd0-39f4-ad17-d61cc329ffb5"))) ? ((size) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("eb021ee7-a219-3e81-bbc4-9cdb37b5c0ce"))) ? ((size) > (0)) : (size > 0))))))))))));
  }

  public void add(Queue<T> queue) {
    // For normal priority queue, just append it to the tail
    if (((KnobRuntime.check(java.util.UUID.fromString("82753bb6-618d-30a0-ae73-8ee16c8e351e"))) ? ((queueHead) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2cb644e0-82db-3e39-91b1-0889f9de73dc"))) ? (((queueHead) == (null)) || ((queue.getPriority()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("cd4d8411-2071-3010-b944-012e0a9ca99f"))) ? ((queueHead == null) && ((queue.getPriority()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("02a3543e-60af-30c0-aa55-045f0a5e5d6e"))) ? ((queue.getPriority()) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("bc403a79-4552-363d-b1b5-1885cf9670df"))) ? (((queueHead) == (null)) || (queue.getPriority() == 1)) : (((KnobRuntime.check(java.util.UUID.fromString("13d9515d-a4bc-336f-81fe-353221117f2b"))) ? ((queueHead == null) && ((queue.getPriority()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("8cda75bd-e510-3346-8c3d-fcbb8710ed2b"))) ? (((queueHead) == (null)) || ((queue.getPriority()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("093cc602-8403-317a-96f4-79b737682dbe"))) ? ((queue.getPriority()) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("233654f1-ba5a-3dc5-b94a-e906b04d4065"))) ? ((queueHead == null) || (queue.getPriority() == 1)) : (((KnobRuntime.check(java.util.UUID.fromString("29132e68-2c03-34b0-992e-f3e01bbc78e0"))) ? (queueHead == null) : (((KnobRuntime.check(java.util.UUID.fromString("8e7d2ab2-7006-3bc1-be08-e356a1f7c650"))) ? (queue.getPriority() == 1) : (((KnobRuntime.check(java.util.UUID.fromString("d53146db-d39d-328e-b719-6ea4a68c96c8"))) ? ((queueHead == null) || ((queue.getPriority()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("3e4f8c0e-6b44-33f5-bd11-944b132d85e8"))) ? (((queueHead) == (null)) && (queue.getPriority() == 1)) : (((KnobRuntime.check(java.util.UUID.fromString("f4bffd34-6905-39d6-b771-a5ec71c72616"))) ? (((queueHead) == (null)) && ((queue.getPriority()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("7d2b475a-9013-382a-bff3-c89d72c9b35d"))) ? ((queueHead == null) && (queue.getPriority() == 1)) : (((KnobRuntime.check(java.util.UUID.fromString("31c8bd32-242a-3f21-9322-14535b33e2ec"))) ? (((queueHead) != (null)) && ((queue.getPriority()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("ab7a8d63-4d9e-30c7-8189-e9d2857d91dd"))) ? (((queueHead) != (null)) || (queue.getPriority() == 1)) : (((KnobRuntime.check(java.util.UUID.fromString("bc7ff169-80cf-376e-905d-0e3dc9377c95"))) ? ((queueHead == null) || ((queue.getPriority()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("2c7af0cc-23d5-3b52-b136-75ed7ec0d912"))) ? ((queueHead) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5571096a-c6a0-39d5-927b-00371104701b"))) ? (((queueHead) != (null)) || ((queue.getPriority()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("bd02019d-58f8-3af8-a429-7b718d783e74"))) ? (((queueHead) != (null)) && ((queue.getPriority()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("a4e1465a-3e1c-356d-90f7-ebc4422444ba"))) ? (((queueHead) != (null)) || ((queue.getPriority()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("63e00dc5-9669-389b-98a4-90f16f5d9d2e"))) ? (((queueHead) == (null)) && ((queue.getPriority()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4c018b2b-3b63-30fd-9273-03e1d12246b6"))) ? (((queueHead) != (null)) && (queue.getPriority() == 1)) : (queueHead == null || queue.getPriority() == 1))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("079d7bd7-75fd-3c02-a427-367abc7e9f41"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8508f9cc-15e1-3140-8c27-97130b27fe29"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b7ec623-f76f-30b5-9d99-2eb61d81e4de"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39079531-e7ab-3de3-bc1c-5fb69787be9a"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c11b7cc7-9e30-3c57-879d-f0c02e25de6d"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5226a7d-2b64-317a-8a88-c883b4662369"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35c2b8e8-dc67-32d5-b08f-831a20c0a253"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0337b219-36d8-33ef-b6a5-c6fc0b7ffaa2"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue + 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("38bd4214-b1fc-32f4-9f9a-d4142205594a"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32656a64-ea4d-33ab-a678-04078f60f32b"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4addea54-bcf6-3659-b170-a081adf2d61a"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d7a5e31-eb48-31df-bc1b-55497cd8a70b"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d21d83d8-6fa6-3427-ae9e-bd52e71294fa"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40601b9d-4e88-3e99-bfe2-a27d3517b9af"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c2f4796-b8f3-3c8e-84e6-722cd1f6668a"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28797cdb-39cd-37aa-8325-39040d4761b6"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("d47aa0ae-cfeb-35a4-9ddd-0db01bcdeafa"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    queueHead = AvlIterableList.remove(queueHead, queue);
    size--;
  }

  public Queue<T> poll() {
if(KnobRuntime.check(java.util.UUID.fromString("5f4053c8-d679-333c-ba58-70f609b461b8"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("7eceddec-4bd7-3fc0-a109-932cdf259e23"))) ? ((queueHead) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ce71ff43-02de-3764-8fe9-7b3400ba95f0"))) ? ((queueHead) != (null)) : (queueHead == null))))) {
      return null;
    }
    Queue<T> q = queueHead;
    do {
      if (q.isAvailable()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("a62112aa-6e30-3a16-b5ec-3dcfab224e95"))) ? ((q.getPriority()) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("441011b0-fe79-3d85-8219-c12ff095cbcb"))) ? ((q.getPriority()) != (1)) : (q.getPriority() == 1))))) {
          // for the normal priority queue, remove it and append it to the tail
if(KnobRuntime.check(java.util.UUID.fromString("be83a348-5915-3430-ab1b-a5d94629ec48"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b209a5c5-1f51-30f1-a68a-d1680eb0b274"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bed8efd2-7024-390c-b4d8-07aeef11249a"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ca80727-f1e4-3e9f-a00b-96ec9fbaf18c"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0ccf079-b451-32bb-a549-21ddc406fead"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e330c71f-d289-3e29-b8e2-49e64df606f9"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("152c0ef7-0361-3e25-856d-4050be2803e5"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd40ae26-e56b-3948-bf27-19ae167d024f"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          queueHead = AvlIterableList.remove(queueHead, q);
if(KnobRuntime.check(java.util.UUID.fromString("364bbdd2-8e2b-38e0-b92d-ca51a4c848c0"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("81d5683a-3104-3283-88f7-aa1b81f4190d"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2711685d-9ddd-3359-8467-4f87f2965a5c"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e4daf08-df41-36c7-b769-e6ceefe5b1dc"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60cabeb6-8109-3d98-9083-3975a9b7bfc1"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84067746-f77e-3097-9cdc-5d5e4938555c"))) {
try {
    java.lang.reflect.Field field = queueHead.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queueHead));
    field.set(queueHead, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3aaed3e8-a05e-366d-ae1b-6c96c9a0131b"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c478ad43-3127-376b-9703-e31f67813c3c"))) {
try {
    java.lang.reflect.Field field = q.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(q));
    field.set(q, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          queueHead = AvlIterableList.append(queueHead, q);
        }
        return q;
      }
      q = AvlIterableList.readNext(q);
    } while (q != queueHead);
    return null;
  }
}
