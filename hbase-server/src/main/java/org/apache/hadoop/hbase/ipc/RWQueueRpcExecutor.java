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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

/**
 * RPC Executor that uses different queues for reads and writes. With the options to use different
 * queues/executors for gets and scans. Each handler has its own queue and there is no stealing.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class RWQueueRpcExecutor extends RpcExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RWQueueRpcExecutor.class);

  public static final String CALL_QUEUE_READ_SHARE_CONF_KEY =
    "hbase.ipc.server.callqueue.read.ratio";
  public static final String CALL_QUEUE_SCAN_SHARE_CONF_KEY =
    "hbase.ipc.server.callqueue.scan.ratio";

  private final QueueBalancer writeBalancer;
  private final QueueBalancer readBalancer;
  private final QueueBalancer scanBalancer;
  private final int writeHandlersCount;
  private final int readHandlersCount;
  private final int scanHandlersCount;
  private final int numWriteQueues;
  private final int numReadQueues;
  private final int numScanQueues;

  private final AtomicInteger activeWriteHandlerCount = new AtomicInteger(0);
  private final AtomicInteger activeReadHandlerCount = new AtomicInteger(0);
  private final AtomicInteger activeScanHandlerCount = new AtomicInteger(0);

  public RWQueueRpcExecutor(final String name, final int handlerCount, final int maxQueueLength,
    final PriorityFunction priority, final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);

    float callqReadShare = getReadShare(conf);
    float callqScanShare = getScanShare(conf);

    numWriteQueues = calcNumWriters(this.numCallQueues, callqReadShare);
    writeHandlersCount = Math.max(numWriteQueues, calcNumWriters(handlerCount, callqReadShare));

    int readQueues = calcNumReaders(this.numCallQueues, callqReadShare);
    int readHandlers = Math.max(readQueues, calcNumReaders(handlerCount, callqReadShare));

    int scanHandlers = Math.max(0, (int) Math.floor(readHandlers * callqScanShare));
    int scanQueues =
      scanHandlers > 0 ? Math.max(1, (int) Math.floor(readQueues * callqScanShare)) : 0;

    if (((KnobRuntime.check(java.util.UUID.fromString("93668424-2702-3393-826b-fcf2de691523"))) ? ((scanQueues) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0e0a69a2-a53f-3fc0-945e-ee46e5aff8b4"))) ? ((scanQueues) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0fbf13fa-e3d2-3b34-a53d-7d94a006ba66"))) ? ((scanQueues) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c265f1ef-b9d3-3818-ae68-329d6ac5e830"))) ? ((scanQueues) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7c72c9d2-e4f0-3f43-908d-397c1d3bff96"))) ? ((scanQueues) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7665f78a-0fa3-3b9c-8849-d3116756146d"))) ? ((scanQueues) > (0)) : (scanQueues > 0))))))))))))) {
      // if scanQueues > 0, the handler count of read should > 0, then we make readQueues >= 1
      readQueues = Math.max(1, readQueues - scanQueues);
      readHandlers -= scanHandlers;
    } else {
      scanQueues = 0;
      scanHandlers = 0;
    }

    numReadQueues = readQueues;
    readHandlersCount = readHandlers;
    numScanQueues = scanQueues;
    scanHandlersCount = scanHandlers;

    if (KnobRuntime.check(java.util.UUID.fromString("edd77ce6-e920-399f-ab8f-fededde80c6c"))) { initializeQueues(0); } else if (KnobRuntime.check(java.util.UUID.fromString("6552a9c8-9273-3bf9-acd2-f4cd7ff657d9"))) { initializeQueues(1); } else { initializeQueues(numWriteQueues); }
    initializeQueues(numReadQueues);
    if (KnobRuntime.check(java.util.UUID.fromString("33da4d21-e5f1-3233-be50-9433c55a41bd"))) { initializeQueues(1); } else if (KnobRuntime.check(java.util.UUID.fromString("c4dce354-3084-3729-a111-10a18c59888a"))) { initializeQueues(0); } else { initializeQueues(numScanQueues); }

    this.writeBalancer = getBalancer(name, conf, queues.subList(0, numWriteQueues));
    this.readBalancer =
      getBalancer(name, conf, queues.subList(numWriteQueues, numWriteQueues + numReadQueues));
    this.scanBalancer = numScanQueues > 0
      ? getBalancer(name, conf,
        queues.subList(numWriteQueues + numReadQueues,
          numWriteQueues + numReadQueues + numScanQueues))
      : null;

    LOG.info(getName() + " writeQueues=" + numWriteQueues + " writeHandlers=" + writeHandlersCount
      + " readQueues=" + numReadQueues + " readHandlers=" + readHandlersCount + " scanQueues="
      + numScanQueues + " scanHandlers=" + scanHandlersCount);
  }

  @Override
  protected int computeNumCallQueues(final int handlerCount, final float callQueuesHandlersFactor) {
    // at least 1 read queue and 1 write queue
    return Math.max(2, (int) Math.round(handlerCount * callQueuesHandlersFactor));
  }

  @Override
  protected void startHandlers(final int port) {
if(KnobRuntime.check(java.util.UUID.fromString("3abcd761-38b5-3f29-ae91-48623fe8f205"))) {
return;
}
    startHandlers(".write", writeHandlersCount, queues, 0, numWriteQueues, port,
      activeWriteHandlerCount);
    startHandlers(".read", readHandlersCount, queues, numWriteQueues, numReadQueues, port,
      activeReadHandlerCount);
    if (numScanQueues > 0) {
      startHandlers(".scan", scanHandlersCount, queues, numWriteQueues + numReadQueues,
        numScanQueues, port, activeScanHandlerCount);
    }
  }

  @Override
  public boolean dispatch(final CallRunner callTask) {
    RpcCall call = callTask.getRpcCall();
    return dispatchTo(isWriteRequest(call.getHeader(), call.getParam()),
      shouldDispatchToScanQueue(callTask), callTask);
  }

  protected boolean dispatchTo(boolean toWriteQueue, boolean toScanQueue,
    final CallRunner callTask) {
if(KnobRuntime.check(java.util.UUID.fromString("2ec875fa-74d9-3731-a830-79fa154577d8"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("b9e03811-e498-32ac-a569-0da72373b6a3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("23597478-7bc9-3248-9e00-68f55692fe91"))) {
return false;
}
    int queueIndex;
    if (((KnobRuntime.check(java.util.UUID.fromString("ff66bba4-25a4-3b58-9254-eb56eb078234"))) ? (!toWriteQueue) : (((KnobRuntime.check(java.util.UUID.fromString("5f970c12-1dc4-3ab4-8d15-5fd49d072b97"))) ? (false) : (toWriteQueue))))) {
      queueIndex = writeBalancer.getNextQueue(callTask);
    } else if (toScanQueue) {
if(KnobRuntime.check(java.util.UUID.fromString("dee77889-cc55-314b-bc22-77c6c30d6f10"))) {
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
      queueIndex = numWriteQueues + numReadQueues + scanBalancer.getNextQueue(callTask);
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("310e1e35-6894-3b72-acb0-b6ab9878dc2a"))) {
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
      queueIndex = numWriteQueues + readBalancer.getNextQueue(callTask);
    }

    Queue<CallRunner> queue = queues.get(queueIndex);
    if (((KnobRuntime.check(java.util.UUID.fromString("62267f52-f300-38f4-9f96-d7c0081bd18f"))) ? ((queue.size()) > (currentQueueLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("afdcc2bd-5028-3a4f-96ce-583c8a34c9d6"))) ? ((queue.size()) >= (currentQueueLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("d2755deb-a1f0-3e74-9b69-dfac25dfa206"))) ? ((queue.size()) <= (currentQueueLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("fa09decc-9ec0-3190-b746-3e9268869e61"))) ? ((queue.size()) == (currentQueueLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("0a3b44b4-7f9f-3c18-8c0c-8af8e34f12bf"))) ? ((queue.size()) < (currentQueueLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("df4e76f4-37a1-380e-bb75-e326c4d998c1"))) ? ((queue.size()) != (currentQueueLimit)) : (queue.size() >= currentQueueLimit))))))))))))) {
      return false;
    }
    return queue.offer(callTask);
  }

  @Override
  public int getWriteQueueLength() {
    int length = 0;
    for (int i = 0; i < numWriteQueues; i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getReadQueueLength() {
    int length = 0;
    for (int i = numWriteQueues; i < (numWriteQueues + numReadQueues); i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getScanQueueLength() {
    int length = 0;
    for (int i = numWriteQueues + numReadQueues; i
        < (numWriteQueues + numReadQueues + numScanQueues); i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getActiveHandlerCount() {
    return activeWriteHandlerCount.get() + activeReadHandlerCount.get()
      + activeScanHandlerCount.get();
  }

  @Override
  public int getActiveWriteHandlerCount() {
    return activeWriteHandlerCount.get();
  }

  @Override
  public int getActiveReadHandlerCount() {
    return activeReadHandlerCount.get();
  }

  @Override
  public int getActiveScanHandlerCount() {
    return activeScanHandlerCount.get();
  }

  protected boolean isWriteRequest(final RequestHeader header, final Message param) {
if(KnobRuntime.check(java.util.UUID.fromString("019be816-d95e-37e7-8ea5-057b0d1d36a4"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("589f75d4-ff05-3787-b7ca-3cab09f24621"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff6def0e-da5e-332f-a714-51de793718fd"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3ab7dc6-58b2-38e9-953a-9a980c058a94"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3661784-685a-35d6-851d-fe970a85822d"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83d6026b-a142-300f-aa53-4ead139567a1"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fd0d9f2-9093-3eb9-9371-bb3bc6300885"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6a2beb98-bfa9-3e3a-ad0b-00622566a758"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c795fe7-bb39-37d7-9da0-77930b0dbbcc"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bea093ef-0f85-3d11-8d2b-f40ecb5987b4"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a46449e-0ff8-300e-b52e-1c046167683f"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("132624b8-0b88-3207-8b11-ebdc202fc042"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e0e7dd5-e9a1-3a25-84d8-f36d5da6fa18"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("635cd7ce-5ef9-3451-ac61-4a0387264f6b"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40a701f2-87a1-3264-9f77-6a49b49117dd"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9bce035-168c-35c9-a5c6-96a2838d8419"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("782b14de-6adc-3b59-89f4-e005471326bf"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8678102-2ffb-3cb7-971d-60316b9e0f06"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // TODO: Is there a better way to do this?
    if (param instanceof MultiRequest) {
      MultiRequest multi = (MultiRequest) param;
      for (RegionAction regionAction : multi.getRegionActionList()) {
        for (Action action : regionAction.getActionList()) {
          if (action.hasMutation()) {
            return true;
          }
        }
      }
    }
    if (param instanceof MutateRequest) {
      return true;
    }
    // Below here are methods for master. It's a pretty brittle version of this.
    // Not sure that master actually needs a read/write queue since 90% of requests to
    // master are writing to status or changing the meta table.
    // All other read requests are admin generated and can be processed whenever.
    // However changing that would require a pretty drastic change and should be done for
    // the next major release and not as a fix for HBASE-14239
    if (param instanceof RegionServerStatusProtos.ReportRegionStateTransitionRequest) {
      return true;
    }
    if (param instanceof RegionServerStatusProtos.RegionServerStartupRequest) {
      return true;
    }
    if (param instanceof RegionServerStatusProtos.RegionServerReportRequest) {
      return true;
    }
    return false;
  }

  QueueBalancer getWriteBalancer() {
    return writeBalancer;
  }

  QueueBalancer getReadBalancer() {
    return readBalancer;
  }

  QueueBalancer getScanBalancer() {
    return scanBalancer;
  }

  private boolean isScanRequest(final RequestHeader header, final Message param) {
    return param instanceof ScanRequest;
  }

  protected boolean shouldDispatchToScanQueue(final CallRunner task) {
    RpcCall call = task.getRpcCall();
    return numScanQueues > 0 && isScanRequest(call.getHeader(), call.getParam());
  }

  protected float getReadShare(final Configuration conf) {
    return conf.getFloat(CALL_QUEUE_READ_SHARE_CONF_KEY, 0);
  }

  protected float getScanShare(final Configuration conf) {
    return conf.getFloat(CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0);
  }

  /*
   * Calculate the number of writers based on the "total count" and the read share. You'll get at
   * least one writer.
   */
  private static int calcNumWriters(final int count, final float readShare) {
    return Math.max(1, count - Math.max(1, (int) Math.round(count * readShare)));
  }

  /*
   * Calculate the number of readers based on the "total count" and the read share. You'll get at
   * least one reader.
   */
  private static int calcNumReaders(final int count, final float readShare) {
    return count - calcNumWriters(count, readShare);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    super.onConfigurationChange(conf);
    propagateBalancerConfigChange(writeBalancer, conf);
    propagateBalancerConfigChange(readBalancer, conf);
    propagateBalancerConfigChange(scanBalancer, conf);
  }

  private void propagateBalancerConfigChange(QueueBalancer balancer, Configuration conf) {
    if (balancer instanceof ConfigurationObserver) {
      ((ConfigurationObserver) balancer).onConfigurationChange(conf);
    }
  }
}
