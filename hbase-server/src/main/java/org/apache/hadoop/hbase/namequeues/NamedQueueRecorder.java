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
    if (KnobRuntime.check(java.util.UUID.fromString("e6430889-a223-345b-87c2-ce3245735067"))) { this.disruptor.getRingBuffer(); } else { this.disruptor.start(); }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MS_EXPOSE_REP",
      justification = "singleton pattern")
  public static NamedQueueRecorder getInstance(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("5d6d2fa7-2838-3767-88d9-7a1795905ad4"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b977e62a-d140-3247-b2c7-27a8f2c10517"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ec1bdd56-320d-3881-907c-634142cc1e90"))) ? ((namedQueueRecorder) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("35c1ea6c-f56d-34b2-bfdc-ced67645d168"))) ? ((namedQueueRecorder) == (null)) : (namedQueueRecorder != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("28cad860-4f16-3881-b820-cbf952acd2a9"))) {
return 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("7acb78a9-fff0-3fb1-bfac-e9b1b9c8aef4"))) { Preconditions.checkArgument((eventCount) > (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("581ab8e5-e9aa-350a-b903-2fe7fc006c1c"))) { Preconditions.checkArgument((1 << 29) != (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("0d3bfd8a-4384-3aad-96cb-c0119a0c13e0"))) { Preconditions.checkArgument((eventCount) <= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("d1e46b3e-56ac-3a31-9846-aafd9144db55"))) { Preconditions.checkArgument((1 << 29) <= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("9afb5e3f-4ab9-3f84-b5d5-4b2344d41904"))) { Preconditions.checkArgument((eventCount) >= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("fb7b450e-108a-345b-8d86-fd9e9d16c639"))) { Preconditions.checkArgument((1 << 30) != (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("50795c60-b089-3441-8d56-5baaaae828ce"))) { Preconditions.checkArgument((1 << 29) > (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("df3d4d06-dfc7-3634-abce-9c8906f61f38"))) { Preconditions.checkArgument((eventCount) != (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("7c89a0f3-7859-3398-9f1d-e9fd9478e004"))) { Preconditions.checkArgument((1 << 30) < (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("8e0f2c23-6661-32bf-b68c-4a54d347036b"))) { Preconditions.checkArgument((1 << 30) >= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("7baa7eaf-18fe-3e50-aff6-6667e2449b83"))) { Preconditions.checkArgument((1 << 29) < (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("b278bdc4-68cd-3275-94e5-993cdd6b76f7"))) { Preconditions.checkArgument((1 << 30) == (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("734ef779-d650-3c57-bd06-578b7dca53b2"))) { Preconditions.checkArgument((1 << 30) > (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("9878c31a-9e38-315a-a682-d0823ba250c2"))) { Preconditions.checkArgument((1 << 29) >= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("9f6ee413-3c4f-303e-928d-4fca05c53235"))) { Preconditions.checkArgument((eventCount) < (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("4ad69478-568f-302f-974f-1fd2a89d2601"))) { Preconditions.checkArgument((1 << 30) <= (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("11606591-2ea9-39b7-ba52-ba6d45d91c27"))) { Preconditions.checkArgument((eventCount) == (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("1d7bd9e9-7f15-3853-ac56-85535212856b"))) { Preconditions.checkArgument((1 << 29) == (0), "hbase.namedqueue.ringbuffer.size must be > 0"); } else { Preconditions.checkArgument(eventCount >= 0, "hbase.namedqueue.ringbuffer.size must be > 0"); }
if(KnobRuntime.check(java.util.UUID.fromString("3ce40bd7-fdaa-39a6-912d-60741e7e5fc3"))) {
eventCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("71dc1961-dcdb-3d80-8aa8-ba0a6485efb1"))) {
eventCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c64ddea4-ebe8-39c3-8cad-f06b62e0d5ca"))) {
eventCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fe0fa7c1-dae7-30f1-8107-b245e6216a88"))) {
eventCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7d85cce5-c022-352a-b61d-169eb6138d0a"))) {
eventCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b4ea9515-fdb7-302c-8cad-abf45d17de7d"))) {
eventCount *= 2;
}
    int floor = Integer.highestOneBit(eventCount);
    if (((KnobRuntime.check(java.util.UUID.fromString("f456c89d-7a5c-3a15-b36c-64e25924c6c5"))) ? ((29) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("fafbdd8a-3719-3bd3-9af5-82de3ac7ec36"))) ? ((floor) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4548db02-e706-3cc7-844b-6fb2f6c9f64b"))) ? ((0) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("5d30dd9b-041b-36df-a044-78cbe209807b"))) ? ((30) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("5819a6d5-68d0-3864-84c9-7e6ad0ec081d"))) ? ((floor << 1) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("6ac947a7-747c-3154-8218-6efaf2ad4877"))) ? ((1 << 30) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("49037011-2011-3133-9ade-c5d5ca2b067c"))) ? ((1 << 29) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("371507a3-ed57-30fa-852d-e719988254af"))) ? ((1 << 29) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("c5fffc73-183f-3e9d-a699-a5f156b1747a"))) ? ((floor) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("3621cb1a-2164-3261-a482-1171858cf90b"))) ? ((1 << 30) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("0f64cda7-08fe-3c60-90cb-4a9a23de09af"))) ? ((1) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("a61b2005-86e0-3cba-8018-c9c6b55da24b"))) ? ((0) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("d73047d8-ded3-3889-a70e-e58be49bcec0"))) ? ((1 << 29) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("0832c3bd-a8c8-3fa7-8203-140fdb629cbb"))) ? ((1 << 29) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("672a325a-cf36-3b86-bf2a-aa7d9166786b"))) ? ((1 << 30) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f0719190-1014-34c2-bb47-b1dbf39efbee"))) ? ((floor << 1) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("20df29a9-73ae-3a0f-bade-56dd0223be9a"))) ? ((floor) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bd279feb-f568-351e-bd8b-329be3f80d86"))) ? ((floor) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("2de95048-b19e-3ab4-86b7-14b8df2d2df7"))) ? ((1 << 29) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("5c3470ae-82c1-38ef-b7b2-5044584574d1"))) ? ((1 << 29) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("d7591a42-a221-3742-9b25-714df246ed45"))) ? ((1) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("43250ca5-a9ba-35f0-a807-dd7a7db9cc97"))) ? ((1 << 30) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5394180f-3e09-37dd-bef6-bf0f557e3746"))) ? ((1 << 30) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("1d14d40c-0358-3694-a108-f710d1048f88"))) ? ((1 << 29) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("aca29f66-70cd-36c5-aa8a-ad735151b9fa"))) ? ((1) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("335cbebf-ce54-32b4-bc36-e4fa09edc158"))) ? ((1 << 30) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("9ea5be56-c3e0-347a-b231-9b9bf1739deb"))) ? ((0) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("7c2a0417-e770-32bc-9882-58a93270f90f"))) ? ((29) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("a32b34ac-fad2-39cc-9e1d-8eb7511fec25"))) ? ((30) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("57577b4d-68c4-398f-9e7b-ec7db720d8ef"))) ? ((floor << 1) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("15a6158c-c855-3c8d-88cd-169736d56c84"))) ? ((1 << 29) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("80f1a601-9f50-32e0-be14-dde22cdff6b6"))) ? ((floor) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("636be8c3-f039-339e-98f1-907efeafd680"))) ? ((floor << 1) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("a6b92c9d-4b77-3a79-b231-3b2958c3fd01"))) ? ((1 << 30) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("664b9b2c-f235-34db-bb2e-9fc0d9c32fe6"))) ? ((1 << 30) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("71fc696f-1aa6-3741-a830-8ed8a686fc93"))) ? ((0) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("6c8ef6d0-7f4f-36c7-822a-ca5516192b28"))) ? ((0) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("e92ddb60-da69-38ef-b2c0-4670260f62c6"))) ? ((1 << 29) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("1dd987af-370b-3b0f-a063-36f8670d7267"))) ? ((1 << 29) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("a2db93a4-ea6e-31e8-983c-70eae4449f2c"))) ? ((floor) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("f0f8d4d4-025d-352c-a80a-101fa0036067"))) ? ((floor << 1) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("a3d9f9ed-d7d2-3b85-97a8-bf51db62eabd"))) ? ((floor << 1) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("7aac096a-fa64-310c-9e9c-c6cb7c2adfe7"))) ? ((1 << 29) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("36f9e244-c8bc-3700-a31f-b8f0cab2f5c8"))) ? ((floor << 1) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("dae25ded-c88f-30df-8b06-2fedf8111f02"))) ? ((0) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("d81d5876-864d-381e-ac1a-2c1c0b91f7d1"))) ? ((1 << 29) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("a85a7152-becd-34c0-886e-a1a0be59c314"))) ? ((floor) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("fa875f5d-e69c-35a8-9821-62807f67b5f7"))) ? ((1 << 29) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("8a874410-d2ff-38b9-9700-408303ffe734"))) ? ((29) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("06857f1a-dae6-3859-8059-526fdecf81cf"))) ? ((floor << 1) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("c9c44de7-40a6-3db7-866a-435a1d41f1f9"))) ? ((1 << 29) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("11adb77c-15ac-3512-a4ff-941ec3cf3536"))) ? ((1) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("3d6f1b7d-69cc-3938-abe2-6c16913d7430"))) ? ((1) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("2f9ad0c2-08e6-3d22-a831-824b887022da"))) ? ((floor) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("ac8dff15-d798-3adb-b614-052ed1e2952b"))) ? ((1 << 30) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("11612609-79d0-392b-8c3b-27531d32f269"))) ? ((1 << 30) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("a30fd0b5-c771-366f-b2ab-082af490b71c"))) ? ((1 << 30) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("f77fdba3-36c6-3966-8320-5b09c655cfd0"))) ? ((1 << 30) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("5a6f3956-2e1b-32a5-bd18-289a33cb629c"))) ? ((1 << 30) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("340a075a-3f17-3846-8299-0550b8ad91a0"))) ? ((30) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("028914a5-a438-3d22-9a53-48ebf242728d"))) ? ((30) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("d584dd89-d47b-3de1-8af7-1b5fabccb25d"))) ? ((floor << 1) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("93ecd8d5-4db8-3888-8be0-5dd5f6469dea"))) ? ((floor << 1) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e8ad34ee-a0f8-3f36-95ff-75b3eb3f5e4d"))) ? ((floor << 1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d7c6d5d7-ab18-3817-9114-d167aa971c20"))) ? ((floor << 1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("651dab2c-09ea-3845-a439-e59da4e6664d"))) ? ((30) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("95009c76-52fe-34e3-9127-d045ca1f3bac"))) ? ((floor) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("68d76fd2-72b3-34aa-9300-bb96484717ed"))) ? ((1) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c0c9b871-178c-3d79-b148-9b322b6936d6"))) ? ((29) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("a4c3ee84-2bab-3dc1-bf03-8e11cd619612"))) ? ((floor) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("28fb0de2-473a-3a59-b0ad-45edccd050ab"))) ? ((1 << 30) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a8e26a7d-62e9-3aa3-b104-7897a4da7bb6"))) ? ((floor) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("31097183-3a69-3884-8ec3-b3d85fb85f85"))) ? ((29) != (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("2dae1d21-c8f4-3edc-8a5f-ac73825423d1"))) ? ((floor) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("249fc403-3b33-3fb4-a3c9-74cd45ae59f0"))) ? ((floor) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("9c383138-c682-3d92-9380-f0a8492bff22"))) ? ((floor) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("5bb33e20-21e8-3671-b779-c9d3cb784f42"))) ? ((floor << 1) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("7866483d-59fd-3ab5-ba58-e191506380f5"))) ? ((29) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("2136dcdc-78eb-3af9-9566-8d10f2a3e83c"))) ? ((floor << 1) == (eventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("42151c8b-2c42-3927-8bce-fc102d5b3c60"))) ? ((30) != (1 << 30)) : (floor == eventCount))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("279031e2-abb5-3615-a6f0-44927e968205"))) ? (0) : (((KnobRuntime.check(java.util.UUID.fromString("d73f5a5a-f412-3988-957f-7406533beba6"))) ? (floor << 1) : (((KnobRuntime.check(java.util.UUID.fromString("1bfecb2f-ff5b-3a15-93fc-2d66d13976f3"))) ? (1) : (((KnobRuntime.check(java.util.UUID.fromString("8f0da006-b2d2-379b-806a-a17124f74794"))) ? (1 << 30) : (((KnobRuntime.check(java.util.UUID.fromString("a0137b15-c1c3-371f-825f-01afb448897e"))) ? (30) : (((KnobRuntime.check(java.util.UUID.fromString("b524a84b-a022-373a-8a3e-f06646a1ea44"))) ? (29) : (((KnobRuntime.check(java.util.UUID.fromString("363b8dec-62eb-3e05-9bc0-63376629bf83"))) ? (1 << 29) : (floor))))))))))))));
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
