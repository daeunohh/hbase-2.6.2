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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This queue allows a ThreadPoolExecutor to steal jobs from another ThreadPoolExecutor. This queue
 * also acts as the factory for creating the PriorityBlockingQueue to be used in the steal-from
 * ThreadPoolExecutor. The behavior of this queue is the same as a normal PriorityBlockingQueue
 * except the take/poll(long,TimeUnit) methods would also check whether there are jobs in the
 * steal-from queue if this q ueue is empty. Note the workers in ThreadPoolExecutor must be
 * pre-started so that they can steal job from the other queue, otherwise the worker will only be
 * started after there are jobs submitted to main queue.
 */
@InterfaceAudience.Private
public class StealJobQueue<T> extends PriorityBlockingQueue<T> {

  private static final long serialVersionUID = -6334572230936888291L;

  private BlockingQueue<T> stealFromQueue;

  private final Lock lock = new ReentrantLock();
  private final transient Condition notEmpty = lock.newCondition();

  public StealJobQueue(Comparator<? super T> comparator) {
    this(11, 11, comparator);
  }

  public StealJobQueue(int initCapacity, int stealFromQueueInitCapacity,
    Comparator<? super T> comparator) {
    super(initCapacity, comparator);
    this.stealFromQueue = new PriorityBlockingQueue<T>(stealFromQueueInitCapacity, comparator) {

      private static final long serialVersionUID = -6805567216580184701L;

      @Override
      public boolean offer(T t) {
        lock.lock();
        try {
          notEmpty.signal();
          return super.offer(t);
        } finally {
          lock.unlock();
        }
      }
    };
  }

  /**
   * Get a queue whose job might be stolen by the consumer of this original queue
   * @return the queue whose job could be stolen
   */
  public BlockingQueue<T> getStealFromQueue() {
if(KnobRuntime.check(java.util.UUID.fromString("22870eaa-6788-3152-a7c2-8c68c26b0e21"))) {
return null;
}
    return stealFromQueue;
  }

  @Override
  public boolean offer(T t) {
    lock.lock();
    try {
      notEmpty.signal();
      return super.offer(t);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T take() throws InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("92a08735-082c-36ce-9346-7b6b39f167fe"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("31695d65-8596-3ad3-9e34-cb3ed5514792"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c8baf846-840f-37f6-b89f-07b13df897f2"))) {
throw new java.lang.InterruptedException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("fa107283-f0b1-3a41-9442-961915a7f189"))) { lock.lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("ee840abc-6c14-3724-8e04-b24e372c251c"))) { lock.unlock(); } else { lock.lockInterruptibly(); }
    try {
      while (true) {
        T retVal = this.poll();
        if (((KnobRuntime.check(java.util.UUID.fromString("11238693-9449-37b0-b4c6-34218190c067"))) ? ((retVal) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7a025dd9-d6a5-3103-bbf0-5d4ac947f999"))) ? ((retVal) != (null)) : (retVal == null))))) {
          retVal = stealFromQueue.poll();
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("3c2e5304-4800-3b4a-8e4b-8aff0a3e2b4c"))) ? ((retVal) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("227d76ab-4e81-3872-8b64-2f0cef7d2044"))) ? ((retVal) != (null)) : (retVal == null))))) {
          notEmpty.await();
        } else {
          return retVal;
        }
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("ea2590a5-d8a8-3ca9-a969-1f8939ee7e08"))) { lock.lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("d3e34638-3c26-3753-bf7c-715bc7c805e7"))) { lock.lockInterruptibly(); } else { lock.unlock(); }
    }
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    try {
      while (true) {
        T retVal = this.poll();
        if (retVal == null) {
          retVal = stealFromQueue.poll();
        }
        if (retVal == null) {
          if (nanos <= 0) return null;
          nanos = notEmpty.awaitNanos(nanos);
        } else {
          return retVal;
        }
      }
    } finally {
      lock.unlock();
    }
  }
}
