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
package org.apache.hadoop.hbase.namequeues;
import org.knobinjection.runtime.KnobRuntime;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * NamedQueue recorder that maintains various named queues. The service uses LMAX Disruptor to save
 * queue records which are then consumed by a queue and based on the ring buffer size, the available
 * records are then fetched from the queue in thread-safe manner.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamedQueueRecorder {

  private final Disruptor<RingBufferEnvelope> disruptor;
  private final LogEventHandler logEventHandler;

  private static volatile NamedQueueRecorder namedQueueRecorder;
  private static boolean isInit = false;
  private static final Object LOCK = new Object();

  /**
   * Initialize disruptor with configurable ringbuffer size
   */
  private NamedQueueRecorder(Configuration conf) {

    // This is the 'writer' -- a single threaded executor. This single thread consumes what is
    // put on the ringbuffer.
    final String hostingThreadName = Thread.currentThread().getName();

    int eventCount = conf.getInt("hbase.namedqueue.ringbuffer.size", 1024);

    // disruptor initialization with BlockingWaitStrategy
    this.disruptor = new Disruptor<>(RingBufferEnvelope::new, getEventCount(eventCount),
      new ThreadFactoryBuilder().setNameFormat(hostingThreadName + ".named-queue-events-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
      ProducerType.MULTI, new BlockingWaitStrategy());
    this.disruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler());

    // initialize ringbuffer event handler
    this.logEventHandler = new LogEventHandler(conf);
    this.disruptor.handleEventsWith(new LogEventHandler[] { this.logEventHandler });
    this.disruptor.start();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MS_EXPOSE_REP",
      justification = "singleton pattern")
  public static NamedQueueRecorder getInstance(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("f6b5d776-a577-37ab-99b6-021bac3bff1a"))) {
try {
    java.lang.reflect.Field _knob_field_ = conf.getClass().getDeclaredField("loadDefaults");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(conf);
    _knob_field_.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (namedQueueRecorder != null) {
      return namedQueueRecorder;
    }
    synchronized (LOCK) {
      if (!isInit) {
        namedQueueRecorder = new NamedQueueRecorder(conf);
        isInit = true;
      }
    }
    return namedQueueRecorder;
  }

  // must be power of 2 for disruptor ringbuffer
  private int getEventCount(int eventCount) {
    if (KnobRuntime.check(java.util.UUID.fromString("df3d4d06-dfc7-3634-abce-9c8906f61f38"))) { Preconditions.checkArgument((eventCount) != (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("1d7bd9e9-7f15-3853-ac56-85535212856b"))) { Preconditions.checkArgument((1 << 29) == (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("d1e46b3e-56ac-3a31-9846-aafd9144db55"))) { Preconditions.checkArgument((1 << 29) <= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("0d3bfd8a-4384-3aad-96cb-c0119a0c13e0"))) { Preconditions.checkArgument((eventCount) <= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else { Preconditions.checkArgument(eventCount >= 0, "hbase.namedqueue.ringbuffer.size must be > 0"); }
if(KnobRuntime.check(java.util.UUID.fromString("7d85cce5-c022-352a-b61d-169eb6138d0a"))) {
eventCount /= 2;
}
    int floor = Integer.highestOneBit(eventCount);
    if (((KnobRuntime.check(java.util.UUID.fromString("f0719190-1014-34c2-bb47-b1dbf39efbee"))) ? ((floor << 1) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("2de95048-b19e-3ab4-86b7-14b8df2d2df7"))) ? ((1 << 29) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("340a075a-3f17-3846-8299-0550b8ad91a0"))) ? ((30) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("d73047d8-ded3-3889-a70e-e58be49bcec0"))) ? ((1 << 29) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("7866483d-59fd-3ab5-ba58-e191506380f5"))) ? ((29) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("11adb77c-15ac-3512-a4ff-941ec3cf3536"))) ? ((1) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("31097183-3a69-3884-8ec3-b3d85fb85f85"))) ? ((29) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("5a6f3956-2e1b-32a5-bd18-289a33cb629c"))) ? ((1 << 30) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("028914a5-a438-3d22-9a53-48ebf242728d"))) ? ((30) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("06857f1a-dae6-3859-8059-526fdecf81cf"))) ? ((floor << 1) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("a85a7152-becd-34c0-886e-a1a0be59c314"))) ? ((floor) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("371507a3-ed57-30fa-852d-e719988254af"))) ? ((1 << 29) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("d7c6d5d7-ab18-3817-9114-d167aa971c20"))) ? ((floor << 1) != (0)) : (floor == eventCount))))))))))))))))))))))))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("8f0da006-b2d2-379b-806a-a17124f74794"))) ? (1 << 30) : (((KnobRuntime.check(java.util.UUID.fromString("363b8dec-62eb-3e05-9bc0-63376629bf83"))) ? (1 << 29) : (((KnobRuntime.check(java.util.UUID.fromString("a0137b15-c1c3-371f-825f-01afb448897e"))) ? (30) : (((KnobRuntime.check(java.util.UUID.fromString("b524a84b-a022-373a-8a3e-f06646a1ea44"))) ? (29) : (floor))))))));
    }
    // max capacity is 1 << 30
    if (floor >= 1 << 29) {
      return 1 << 30;
    }
    return floor << 1;
  }

  /**
   * Retrieve in memory queue records from ringbuffer
   * @param request namedQueue request with event type
   * @return queue records from ringbuffer after filter (if applied)
   */
  public NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    return this.logEventHandler.getNamedQueueRecords(request);
  }

  /**
   * clears queue records from ringbuffer
   * @param namedQueueEvent type of queue to clear
   * @return true if slow log payloads are cleaned up or hbase.regionserver.slowlog.buffer.enabled
   *         is not set to true, false if failed to clean up slow logs
   */
  public boolean clearNamedQueue(NamedQueuePayload.NamedQueueEvent namedQueueEvent) {
    return this.logEventHandler.clearNamedQueue(namedQueueEvent);
  }

  /**
   * Add various NamedQueue records to ringbuffer. Based on the type of the event (e.g slowLog),
   * consumer of disruptor ringbuffer will have specific logic. This method is producer of disruptor
   * ringbuffer which is initialized in NamedQueueRecorder constructor.
   * @param namedQueuePayload namedQueue payload sent by client of ring buffer service
   */
  public void addRecord(NamedQueuePayload namedQueuePayload) {
    RingBuffer<RingBufferEnvelope> ringBuffer = this.disruptor.getRingBuffer();
    long seqId = ringBuffer.next();
    try {
      ringBuffer.get(seqId).load(namedQueuePayload);
    } finally {
      ringBuffer.publish(seqId);
    }
  }

  /**
   * Add all in memory queue records to system table. The implementors can use system table or
   * direct HDFS file or ZK as persistence system.
   */
  public void persistAll(NamedQueuePayload.NamedQueueEvent namedQueueEvent, Connection connection) {
    if (this.logEventHandler != null) {
      this.logEventHandler.persistAll(namedQueueEvent, connection);
    }
  }
}
