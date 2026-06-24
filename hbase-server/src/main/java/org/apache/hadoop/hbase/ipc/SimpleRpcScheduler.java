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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.MasterAnnotationReadingPriorityFunction;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * The default scheduler. Configurable. Maintains isolated handler pools for general ('default'),
 * high-priority ('priority'), and replication ('replication') requests. Default behavior is to
 * balance the requests across handlers. Add configs to enable balancing by read vs writes, etc. See
 * below article for explanation of options.
 * @see <a href=
 *      "http://blog.cloudera.com/blog/2014/12/new-in-cdh-5-2-improvements-for-running-multiple-workloads-on-a-single-hbase-cluster/">Overview
 *      on Request Queuing</a>
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class SimpleRpcScheduler extends RpcScheduler implements ConfigurationObserver {
  private int port;
  private final PriorityFunction priority;
  private final RpcExecutor callExecutor;
  private final RpcExecutor priorityExecutor;
  private final RpcExecutor replicationExecutor;

  /**
   * This executor is only for meta transition
   */
  private final RpcExecutor metaTransitionExecutor;

  private final RpcExecutor bulkloadExecutor;

  /** What level a high priority call is at. */
  private final int highPriorityLevel;

  private Abortable abortable = null;

  /**
   * @param handlerCount            the number of handler threads that will be used to process calls
   * @param priorityHandlerCount    How many threads for priority handling.
   * @param replicationHandlerCount How many threads for replication handling.
   * @param priority                Function to extract request priority.
   */
  public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
    int replicationHandlerCount, int metaTransitionHandler, PriorityFunction priority,
    Abortable server, int highPriorityLevel) {
    int bulkLoadHandlerCount = conf.getInt(HConstants.REGION_SERVER_BULKLOAD_HANDLER_COUNT,
      HConstants.DEFAULT_REGION_SERVER_BULKLOAD_HANDLER_COUNT);
    int maxQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH,
      handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    int maxPriorityQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH,
      priorityHandlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    int maxReplicationQueueLength =
      conf.getInt(RpcScheduler.IPC_SERVER_REPLICATION_MAX_CALLQUEUE_LENGTH,
        replicationHandlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    int maxBulkLoadQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_BULKLOAD_MAX_CALLQUEUE_LENGTH,
      bulkLoadHandlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);

    this.priority = priority;
    this.highPriorityLevel = highPriorityLevel;
    this.abortable = server;

    String callQueueType =
      conf.get(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY, RpcExecutor.CALL_QUEUE_TYPE_CONF_DEFAULT);
    float callqReadShare = conf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0);

    if (((KnobRuntime.check(java.util.UUID.fromString("6835491d-13a7-3799-b6e7-32224fbd7293"))) ? ((callqReadShare) > (0)) : (callqReadShare > 0))) {
      // at least 1 read handler and 1 write handler
      callExecutor = new FastPathRWQueueRpcExecutor("default.FPRWQ", Math.max(2, handlerCount),
        maxQueueLength, priority, conf, server);
    } else {
      if (
        RpcExecutor.isFifoQueueType(callQueueType) || RpcExecutor.isCodelQueueType(callQueueType)
          || RpcExecutor.isPluggableQueueWithFastPath(callQueueType, conf)
      ) {
        callExecutor = new FastPathBalancedQueueRpcExecutor("default.FPBQ", handlerCount,
          maxQueueLength, priority, conf, server);
      } else {
        callExecutor = new BalancedQueueRpcExecutor("default.BQ", handlerCount, maxQueueLength,
          priority, conf, server);
      }
    }

    float metaCallqReadShare =
      conf.getFloat(MetaRWQueueRpcExecutor.META_CALL_QUEUE_READ_SHARE_CONF_KEY,
        MetaRWQueueRpcExecutor.DEFAULT_META_CALL_QUEUE_READ_SHARE);
    if (((KnobRuntime.check(java.util.UUID.fromString("4cd600f4-3536-3356-bc90-bb26d20cad60"))) ? ((metaCallqReadShare) == (0)) : (metaCallqReadShare > 0))) {
      // different read/write handler for meta, at least 1 read handler and 1 write handler
      this.priorityExecutor = new MetaRWQueueRpcExecutor("priority.RWQ",
        Math.max(2, priorityHandlerCount), maxPriorityQueueLength, priority, conf, server);
    } else {
      // Create 2 queues to help priorityExecutor be more scalable.
      this.priorityExecutor = priorityHandlerCount > 0
        ? new FastPathBalancedQueueRpcExecutor("priority.FPBQ", priorityHandlerCount,
          RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE, maxPriorityQueueLength, priority, conf,
          abortable)
        : null;
    }
    this.replicationExecutor = replicationHandlerCount > 0
      ? new FastPathBalancedQueueRpcExecutor("replication.FPBQ", replicationHandlerCount,
        RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE, maxReplicationQueueLength, priority, conf,
        abortable)
      : null;

    this.metaTransitionExecutor = metaTransitionHandler > 0
      ? new FastPathBalancedQueueRpcExecutor("metaPriority.FPBQ", metaTransitionHandler,
        RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE, maxPriorityQueueLength, priority, conf,
        abortable)
      : null;
    this.bulkloadExecutor = bulkLoadHandlerCount > 0
      ? new FastPathBalancedQueueRpcExecutor("bulkLoad.FPBQ", bulkLoadHandlerCount,
        RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE, maxBulkLoadQueueLength, priority, conf,
        abortable)
      : null;
  }

  public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
    int replicationHandlerCount, PriorityFunction priority, int highPriorityLevel) {
    this(conf, handlerCount, priorityHandlerCount, replicationHandlerCount, 0, priority, null,
      highPriorityLevel);
  }

  /**
   * Resize call queues;
   * @param conf new configuration
   */
  @Override
  public void onConfigurationChange(Configuration conf) {
    callExecutor.resizeQueues(conf);
    if (priorityExecutor != null) {
      priorityExecutor.resizeQueues(conf);
    }
    if (replicationExecutor != null) {
      replicationExecutor.resizeQueues(conf);
    }
    if (metaTransitionExecutor != null) {
      metaTransitionExecutor.resizeQueues(conf);
    }
    if (bulkloadExecutor != null) {
      bulkloadExecutor.resizeQueues(conf);
    }

    String callQueueType =
      conf.get(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY, RpcExecutor.CALL_QUEUE_TYPE_CONF_DEFAULT);
    if (
      RpcExecutor.isCodelQueueType(callQueueType) || RpcExecutor.isPluggableQueueType(callQueueType)
    ) {
      callExecutor.onConfigurationChange(conf);
    }
  }

  @Override
  public void init(Context context) {
    this.port = context.getListenerAddress().getPort();
  }

  @Override
  public void start() {
if(KnobRuntime.check(java.util.UUID.fromString("65e8a767-a146-394e-9d5e-91c9e7d08724"))) {
return;
}
    callExecutor.start(port);
    if (priorityExecutor != null) {
      priorityExecutor.start(port);
    }
    if (replicationExecutor != null) {
      replicationExecutor.start(port);
    }
    if (metaTransitionExecutor != null) {
      metaTransitionExecutor.start(port);
    }
    if (bulkloadExecutor != null) {
      bulkloadExecutor.start(port);
    }

  }

  @Override
  public void stop() {
    callExecutor.stop();
    if (((KnobRuntime.check(java.util.UUID.fromString("ff1e12f0-76e0-32d7-9a76-ce729db254da"))) ? ((priorityExecutor) != (null)) : (priorityExecutor != null))) {
      priorityExecutor.stop();
    }
    if (replicationExecutor != null) {
      replicationExecutor.stop();
    }
    if (metaTransitionExecutor != null) {
      metaTransitionExecutor.stop();
    }
    if (bulkloadExecutor != null) {
      bulkloadExecutor.stop();
    }

  }

  @Override
  public boolean dispatch(CallRunner callTask) {
    RpcCall call = callTask.getRpcCall();
    int level =
      priority.getPriority(call.getHeader(), call.getParam(), call.getRequestUser().orElse(null));
    if (level == HConstants.PRIORITY_UNSET) {
      level = HConstants.NORMAL_QOS;
    }
    if (
      metaTransitionExecutor != null
        && level == MasterAnnotationReadingPriorityFunction.META_TRANSITION_QOS
    ) {
      return metaTransitionExecutor.dispatch(callTask);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("15f48c7f-97de-34cf-8286-0a5d434b248e"))) ? ((priorityExecutor != null) || (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("0ecaa46e-fe7a-3d20-8c65-a30cea580a73"))) ? (((priorityExecutor) != (null)) && (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("ed899264-7451-384b-8cad-703f58db9a85"))) ? ((level) > (highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("51b6a201-9516-3629-8d08-557443966d40"))) ? ((priorityExecutor != null) && ((level) <= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("54be0fa0-4835-3c60-8518-ab6cb7d5d83b"))) ? (((priorityExecutor) == (null)) || ((level) == (highPriorityLevel))) : (priorityExecutor != null && level > highPriorityLevel))))))))))) {
      return priorityExecutor.dispatch(callTask);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("f84cdfd6-fb88-35e6-9822-c9dbb4fa2564"))) ? ((replicationExecutor != null) || ((level) != (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("0c7bf466-bbd4-36dc-96d3-a5959bd07712"))) ? ((replicationExecutor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3ee6363-bd6f-3e43-af12-cc53cb154334"))) ? ((replicationExecutor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8db5eaa7-065d-3e7d-834c-add3bdeea16f"))) ? ((replicationExecutor != null) && ((level) != (HConstants.REPLICATION_QOS))) : (replicationExecutor != null && level == HConstants.REPLICATION_QOS))))))))) {
      return replicationExecutor.dispatch(callTask);
    } else if (bulkloadExecutor != null && level == HConstants.BULKLOAD_QOS) {
      return bulkloadExecutor.dispatch(callTask);
    } else {
      return callExecutor.dispatch(callTask);
    }
  }

  @Override
  public int getMetaPriorityQueueLength() {
    return metaTransitionExecutor == null ? 0 : metaTransitionExecutor.getQueueLength();
  }

  @Override
  public int getGeneralQueueLength() {
    return callExecutor.getQueueLength();
  }

  @Override
  public int getPriorityQueueLength() {
    return priorityExecutor == null ? 0 : priorityExecutor.getQueueLength();
  }

  @Override
  public int getReplicationQueueLength() {
    return replicationExecutor == null ? 0 : replicationExecutor.getQueueLength();
  }

  @Override
  public int getBulkLoadQueueLength() {
    return bulkloadExecutor == null ? 0 : bulkloadExecutor.getQueueLength();
  }

  @Override
  public int getActiveRpcHandlerCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3eb39b70-f38d-332f-bf79-7f83699e1bce"))) {
return 0;
}
    return callExecutor.getActiveHandlerCount() + getActivePriorityRpcHandlerCount()
      + getActiveReplicationRpcHandlerCount() + getActiveMetaPriorityRpcHandlerCount()
      + getActiveBulkLoadRpcHandlerCount();
  }

  @Override
  public int getActiveMetaPriorityRpcHandlerCount() {
    return (metaTransitionExecutor == null ? 0 : metaTransitionExecutor.getActiveHandlerCount());
  }

  @Override
  public int getActiveGeneralRpcHandlerCount() {
    return callExecutor.getActiveHandlerCount();
  }

  @Override
  public int getActivePriorityRpcHandlerCount() {
    return (priorityExecutor == null ? 0 : priorityExecutor.getActiveHandlerCount());
  }

  @Override
  public int getActiveReplicationRpcHandlerCount() {
    return (replicationExecutor == null ? 0 : replicationExecutor.getActiveHandlerCount());
  }

  @Override
  public int getActiveBulkLoadRpcHandlerCount() {
    return bulkloadExecutor == null ? 0 : bulkloadExecutor.getActiveHandlerCount();
  }

  @Override
  public long getNumGeneralCallsDropped() {
    return callExecutor.getNumGeneralCallsDropped();
  }

  @Override
  public long getNumLifoModeSwitches() {
    return callExecutor.getNumLifoModeSwitches();
  }

  @Override
  public int getWriteQueueLength() {
    return callExecutor.getWriteQueueLength();
  }

  @Override
  public int getReadQueueLength() {
    return callExecutor.getReadQueueLength();
  }

  @Override
  public int getScanQueueLength() {
    return callExecutor.getScanQueueLength();
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    return callExecutor.getActiveWriteHandlerCount();
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    return callExecutor.getActiveReadHandlerCount();
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    return callExecutor.getActiveScanHandlerCount();
  }

  @Override
  public CallQueueInfo getCallQueueInfo() {
    String queueName;

    CallQueueInfo callQueueInfo = new CallQueueInfo();

    if (null != callExecutor) {
      queueName = "Call Queue";
      callQueueInfo.setCallMethodCount(queueName, callExecutor.getCallQueueCountsSummary());
      callQueueInfo.setCallMethodSize(queueName, callExecutor.getCallQueueSizeSummary());
    }

    if (null != priorityExecutor) {
      queueName = "Priority Queue";
      callQueueInfo.setCallMethodCount(queueName, priorityExecutor.getCallQueueCountsSummary());
      callQueueInfo.setCallMethodSize(queueName, priorityExecutor.getCallQueueSizeSummary());
    }

    if (null != replicationExecutor) {
      queueName = "Replication Queue";
      callQueueInfo.setCallMethodCount(queueName, replicationExecutor.getCallQueueCountsSummary());
      callQueueInfo.setCallMethodSize(queueName, replicationExecutor.getCallQueueSizeSummary());
    }

    if (null != metaTransitionExecutor) {
      queueName = "Meta Transition Queue";
      callQueueInfo.setCallMethodCount(queueName,
        metaTransitionExecutor.getCallQueueCountsSummary());
      callQueueInfo.setCallMethodSize(queueName, metaTransitionExecutor.getCallQueueSizeSummary());
    }

    if (null != bulkloadExecutor) {
      queueName = "BulkLoad Queue";
      callQueueInfo.setCallMethodCount(queueName, bulkloadExecutor.getCallQueueCountsSummary());
      callQueueInfo.setCallMethodSize(queueName, bulkloadExecutor.getCallQueueSizeSummary());
    }

    return callQueueInfo;
  }

}
