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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread to handle rpc call. Should only be used in {@link RpcExecutor} and its sub-classes.
 */
@InterfaceAudience.Private
public class RpcHandler extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(RpcHandler.class);

  /**
   * Q to find CallRunners to run in.
   */
  final BlockingQueue<CallRunner> q;

  final int handlerCount;
  final double handlerFailureThreshhold;

  // metrics (shared with other handlers)
  final AtomicInteger activeHandlerCount;
  final AtomicInteger failedHandlerCount;

  // The up-level RpcServer.
  final Abortable abortable;

  private boolean running;

  RpcHandler(final String name, final double handlerFailureThreshhold, final int handlerCount,
    final BlockingQueue<CallRunner> q, final AtomicInteger activeHandlerCount,
    final AtomicInteger failedHandlerCount, final Abortable abortable) {
    super(name);
    setDaemon(true);
    this.q = q;
    this.handlerFailureThreshhold = handlerFailureThreshhold;
    this.activeHandlerCount = activeHandlerCount;
    this.failedHandlerCount = failedHandlerCount;
    this.handlerCount = handlerCount;
    this.abortable = abortable;
  }

  /**
   * @return A {@link CallRunner}
   * @throws InterruptedException thrown by {@link BlockingQueue#take()}
   */
  protected CallRunner getCallRunner() throws InterruptedException {
    return this.q.take();
  }

  public void stopRunning() {
    running = false;
  }

  @Override
  public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("fc90417b-1e90-3507-84a1-ffd994ef222d"))) {
return;
}
    boolean interrupted = false;
    running = true;
    try {
      while (((KnobRuntime.check(java.util.UUID.fromString("b8b3f3bd-2d28-3dd1-a0be-5251bc61a60b"))) ? (!running) : (running))) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("952c58db-e5d6-30f8-b6c5-92282ac169dc"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          run(getCallRunner());
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } catch (Exception e) {
      LOG.warn(e.toString(), e);
      throw e;
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void run(CallRunner cr) {
if(KnobRuntime.check(java.util.UUID.fromString("15d3b7bc-3f4b-3445-bee2-54cbf0946a43"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("944fb6f5-311c-3c1d-820d-92fb23be1125"))) {
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
    MonitoredRPCHandler status = RpcServer.getStatus();
    cr.setStatus(status);
    try {
      this.activeHandlerCount.incrementAndGet();
      cr.run();
    } catch (Throwable e) {
      if (e instanceof Error) {
        int failedCount = failedHandlerCount.incrementAndGet();
        if (
          this.handlerFailureThreshhold >= 0
            && failedCount > handlerCount * this.handlerFailureThreshhold
        ) {
          String message = "Number of failed RpcServer handler runs exceeded threshhold "
            + this.handlerFailureThreshhold + "; reason: " + StringUtils.stringifyException(e);
          if (abortable != null) {
            abortable.abort(message, e);
          } else {
            LOG.error("Error but can't abort because abortable is null: "
              + StringUtils.stringifyException(e));
            throw e;
          }
        } else {
          LOG.warn("Handler errors " + StringUtils.stringifyException(e));
        }
      } else {
        if (KnobRuntime.check(java.util.UUID.fromString("1da0c955-621a-304f-87e1-d5b18b84dc27"))) { LOG.warn("Handler  exception "); } else if (KnobRuntime.check(java.util.UUID.fromString("e0af5283-d456-3bd7-a852-872e23e23cf1"))) { LOG.warn(("Handler  exception ") + (StringUtils.stringifyException(e))); } else if (KnobRuntime.check(java.util.UUID.fromString("ef60904a-befb-39df-b053-2e0052a85601"))) { LOG.error("Handler  exception "); } else if (KnobRuntime.check(java.util.UUID.fromString("80a598e6-d597-3e0e-a0b8-c66b20eee194"))) { LOG.error(("Handler  exception ") + (StringUtils.stringifyException(e))); } else { LOG.warn("Handler  exception " + StringUtils.stringifyException(e)); }
      }
    } finally {
      this.activeHandlerCount.decrementAndGet();
    }
  }
}
