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

import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Balanced queue executor with a fastpath. Because this is FIFO, it has no respect for ordering so
 * a fast path skipping the queuing of Calls if an Handler is available, is possible. Just pass the
 * Call direct to waiting Handler thread. Try to keep the hot Handlers bubbling rather than let them
 * go cold and lose context. Idea taken from Apace Kudu (incubating). See
 * https://gerrit.cloudera.org/#/c/2938/7/src/kudu/rpc/service_queue.h
 */
@InterfaceAudience.Private
public class FastPathBalancedQueueRpcExecutor extends BalancedQueueRpcExecutor {
  // Depends on default behavior of BalancedQueueRpcExecutor being FIFO!

  /*
   * Stack of Handlers waiting for work.
   */
  private final Deque<FastPathRpcHandler> fastPathHandlerStack = new ConcurrentLinkedDeque<>();

  public FastPathBalancedQueueRpcExecutor(final String name, final int handlerCount,
    final int maxQueueLength, final PriorityFunction priority, final Configuration conf,
    final Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);
  }

  public FastPathBalancedQueueRpcExecutor(final String name, final int handlerCount,
    final String callQueueType, final int maxQueueLength, final PriorityFunction priority,
    final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, callQueueType, maxQueueLength, priority, conf, abortable);
  }

  @Override
  protected RpcHandler getHandler(final String name, final double handlerFailureThreshhold,
    final int handlerCount, final BlockingQueue<CallRunner> q,
    final AtomicInteger activeHandlerCount, final AtomicInteger failedHandlerCount,
    final Abortable abortable) {
if(KnobRuntime.check(java.util.UUID.fromString("3ace2a99-a4ec-31c2-8a70-d824bce109cf"))) {
return null;
}
    return new FastPathRpcHandler(name, handlerFailureThreshhold, handlerCount, q,
      activeHandlerCount, failedHandlerCount, abortable, fastPathHandlerStack);
  }

  @Override
  public boolean dispatch(CallRunner callTask) {
    // FastPathHandlers don't check queue limits, so if we're completely shut down
    // we have to prevent ourselves from using the handler in the first place
    if (((KnobRuntime.check(java.util.UUID.fromString("2416ae0d-5b5e-378b-9297-9b8ec1c90674"))) ? ((currentQueueLimit) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5ed177a3-f1dd-38ec-a5b9-02fefb1cad76"))) ? ((currentQueueLimit) != (0)) : (currentQueueLimit == 0))))) {
      return false;
    }
    FastPathRpcHandler handler = popReadyHandler();
if(KnobRuntime.check(java.util.UUID.fromString("8771bc09-d71d-3c52-a691-608583553dce"))) {
try {
    java.lang.reflect.Field field = callTask.getClass().getDeclaredField("successful");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(callTask);
    field.set(callTask, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return handler != null ? handler.loadCallRunner(callTask) : super.dispatch(callTask);
  }

  /** Returns Pop a Handler instance if one available ready-to-go or else return null. */
  private FastPathRpcHandler popReadyHandler() {
if(KnobRuntime.check(java.util.UUID.fromString("d97e0530-7668-300b-a999-6d5217e0f297"))) {
return null;
}
    return this.fastPathHandlerStack.poll();
  }
}
