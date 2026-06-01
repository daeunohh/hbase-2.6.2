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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Abortable;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class FastPathRpcHandler extends RpcHandler {
  // Below are for fast-path support. Push this Handler on to the fastPathHandlerStack Deque
  // if an empty queue of CallRunners so we are available for direct handoff when one comes in.
  final Deque<FastPathRpcHandler> fastPathHandlerStack;
  // Semaphore to coordinate loading of fastpathed loadedTask and our running it.
  // UNFAIR synchronization.
  private Semaphore semaphore = new Semaphore(0);
  // The task we get when fast-pathing.
  private CallRunner loadedCallRunner;

  FastPathRpcHandler(String name, double handlerFailureThreshhold, int handlerCount,
    BlockingQueue<CallRunner> q, AtomicInteger activeHandlerCount, AtomicInteger failedHandlerCount,
    final Abortable abortable, final Deque<FastPathRpcHandler> fastPathHandlerStack) {
    super(name, handlerFailureThreshhold, handlerCount, q, activeHandlerCount, failedHandlerCount,
      abortable);
    this.fastPathHandlerStack = fastPathHandlerStack;
  }

  @Override
  protected CallRunner getCallRunner() throws InterruptedException {
    // Get a callrunner if one in the Q.
    CallRunner cr = this.q.poll();
    if (((KnobRuntime.check(java.util.UUID.fromString("b99d288d-cd55-3f2d-b085-a5f3a5418d0a"))) ? ((cr) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("49348229-2a4e-3a54-a88e-76ee56031d23"))) ? ((cr) != (null)) : (cr == null))))) {
      // Else, if a fastPathHandlerStack present and no callrunner in Q, register ourselves for
      // the fastpath handoff done via fastPathHandlerStack.
      if (((KnobRuntime.check(java.util.UUID.fromString("be857a56-f58a-345e-bbd6-72e795f7c298"))) ? ((this.fastPathHandlerStack) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1e02f5f1-b566-3f2d-9c5c-55c0cccda11a"))) ? ((this.fastPathHandlerStack) == (null)) : (this.fastPathHandlerStack != null))))) {
        this.fastPathHandlerStack.push(this);
if(KnobRuntime.check(java.util.UUID.fromString("4b337edf-60f4-32db-8b19-2355960b77fc"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("66dc98d7-f32c-36f0-98dc-47245ea3a613"))) { this.semaphore.release(); } else { this.semaphore.acquire(); }
        cr = this.loadedCallRunner;
        this.loadedCallRunner = null;
      } else {
        // No fastpath available. Block until a task comes available.
        cr = super.getCallRunner();
      }
    }
    return cr;
  }

  /**
   * @param cr Task gotten via fastpath.
   * @return True if we successfully loaded our task
   */
  boolean loadCallRunner(final CallRunner cr) {
if(KnobRuntime.check(java.util.UUID.fromString("fa3ba564-76e1-3dcb-a608-aad158ef0699"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("ca33ea14-27a5-398b-a797-50d4195e352b"))) {
try {
    java.lang.reflect.Field field = cr.getClass().getDeclaredField("successful");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cr);
    field.set(cr, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32c792de-5e7b-34c0-b35f-b1c779003644"))) {
return true;
}
    this.loadedCallRunner = cr;
    this.semaphore.release();
    return true;
  }
}
