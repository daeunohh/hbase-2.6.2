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

    if (((KnobRuntime.check(java.util.UUID.fromString("597b6f2e-01a1-33dd-9dc1-24a7be0143bb"))) ? ((callqReadShare) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c8fe5f62-96b1-3690-9b57-14502a610d7b"))) ? ((callqReadShare) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6835491d-13a7-3799-b6e7-32224fbd7293"))) ? ((callqReadShare) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("44293acc-de69-3bdf-a5aa-4312e7f307aa"))) ? ((callqReadShare) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("68d89107-5325-3872-bf22-3af55b966d66"))) ? ((callqReadShare) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8ad63b7f-595f-3dad-9979-60ba8836c842"))) ? ((callqReadShare) == (0)) : (callqReadShare > 0))))))))))))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("a6975148-83f2-37f9-8bf0-684d04374902"))) ? ((metaCallqReadShare) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d648dbdf-2825-323e-a1bf-a0aa2f002d1f"))) ? ((metaCallqReadShare) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b85febbb-6559-349b-8064-d22d2089366b"))) ? ((metaCallqReadShare) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("17f32fe0-0c4d-3969-a56c-eaeb3c64be60"))) ? ((metaCallqReadShare) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4cd600f4-3536-3356-bc90-bb26d20cad60"))) ? ((metaCallqReadShare) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d7f2b576-aac6-3d00-ae6a-eb029835fb2c"))) ? ((metaCallqReadShare) != (0)) : (metaCallqReadShare > 0))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("be3d75c0-dfbd-340a-8c31-27d8ed4b967f"))) {
return;
}
    this.port = context.getListenerAddress().getPort();
  }

  @Override
  public void start() {
if(KnobRuntime.check(java.util.UUID.fromString("65e8a767-a146-394e-9d5e-91c9e7d08724"))) {
return;
}
    callExecutor.start(port);
    if (((KnobRuntime.check(java.util.UUID.fromString("05f63ab1-0afb-39de-a95c-da68c70dc31a"))) ? ((priorityExecutor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f4d125ea-2b76-3049-b7b7-c8a5c518ba05"))) ? ((priorityExecutor) == (null)) : (priorityExecutor != null))))) {
      priorityExecutor.start(port);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("15eeca2f-89c8-39e6-b432-ca5ee2e85e06"))) ? ((replicationExecutor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3b9baacd-2cc3-3a2f-9951-713bb37ceede"))) ? ((replicationExecutor) != (null)) : (replicationExecutor != null))))) {
      replicationExecutor.start(port);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("7032d1ef-1866-3e74-a60b-afefde2a0c1b"))) ? ((metaTransitionExecutor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9b5647b1-0523-382e-ae55-1e3cc65ba61f"))) ? ((metaTransitionExecutor) != (null)) : (metaTransitionExecutor != null))))) {
      metaTransitionExecutor.start(port);
    }
    if (bulkloadExecutor != null) {
      bulkloadExecutor.start(port);
    }

  }

  @Override
  public void stop() {
if(KnobRuntime.check(java.util.UUID.fromString("4d7fb1eb-15d5-3acb-83db-f2617d796519"))) {
return;
}
    callExecutor.stop();
    if (((KnobRuntime.check(java.util.UUID.fromString("ff1e12f0-76e0-32d7-9a76-ce729db254da"))) ? ((priorityExecutor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("12001fa1-cce8-3e18-8c18-f2d1446cb2ab"))) ? ((priorityExecutor) == (null)) : (priorityExecutor != null))))) {
      priorityExecutor.stop();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("f62d695f-22da-3efb-b552-da48e750b949"))) ? ((replicationExecutor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8b57e361-ed89-3d47-9996-aab93ce8cb29"))) ? ((replicationExecutor) != (null)) : (replicationExecutor != null))))) {
      replicationExecutor.stop();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("3c03f76d-6f07-3f8f-8b89-3e415c6ae717"))) ? ((metaTransitionExecutor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4186c0fa-6366-3413-a868-b079a514cd72"))) ? ((metaTransitionExecutor) == (null)) : (metaTransitionExecutor != null))))) {
      metaTransitionExecutor.stop();
    }
    if (bulkloadExecutor != null) {
      bulkloadExecutor.stop();
    }

  }

  @Override
  public boolean dispatch(CallRunner callTask) {
if(KnobRuntime.check(java.util.UUID.fromString("7f6813a2-459a-3cb2-a9fe-97093de39a71"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("dfc3b044-f436-31ba-9993-c917e0091d8d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9a47b21d-b231-3524-9ba4-68f65a9bedc0"))) {
return false;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("7141bfa6-a245-3b57-95cb-2b6cdba0deb1"))) {
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
      return metaTransitionExecutor.dispatch(callTask);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("eac40889-8434-3c41-b6b3-f2218d224f93"))) ? (((priorityExecutor) != (null)) || ((level) <= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("13375dc4-6577-3d92-8d2f-65d8db43988e"))) ? (((priorityExecutor) != (null)) && ((level) != (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("ed899264-7451-384b-8cad-703f58db9a85"))) ? ((level) > (highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("277399a3-eff3-38b6-994e-dc5b7fccef60"))) ? ((priorityExecutor != null) || ((level) != (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("efe14875-1497-383f-b3f8-f0e08c027654"))) ? ((level) < (highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("2b3550d7-c11c-3fcc-a4f8-b6bbf774b309"))) ? (((priorityExecutor) == (null)) && ((level) <= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("35ef388e-bb03-32f6-8ca2-a728a6fb2911"))) ? (((priorityExecutor) != (null)) || ((level) > (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("8d59bb40-e1ab-32bd-ac4f-656fee43e283"))) ? (((priorityExecutor) == (null)) && ((level) < (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("dcbbe290-93e6-3bb2-a790-5e6a48fbe972"))) ? (((priorityExecutor) != (null)) || ((level) < (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("ccf1b35f-181b-3701-9b53-b52214f1fe09"))) ? (((priorityExecutor) == (null)) && ((level) > (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("848577ca-bf6e-3d63-9eb5-99e46cedf6ac"))) ? ((priorityExecutor != null) || ((level) == (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("7cb8ceac-f39c-3ffe-b8d7-1b4c38ff6d1a"))) ? ((level) <= (highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("30f659a9-f444-3161-b88a-3814ace2ee6f"))) ? (((priorityExecutor) == (null)) || ((level) < (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("b0abffbd-6698-312d-9c28-471ba5a30bd4"))) ? ((level) != (highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("60077b06-8dfd-34ee-9ae5-3ebbd20392ae"))) ? ((priorityExecutor != null) && ((level) < (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("e4f84d7f-27c6-3b89-97b2-d8b9973bb0af"))) ? ((priorityExecutor != null) && ((level) >= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("27c95bf6-c93a-32ef-8874-9161e125855e"))) ? (((priorityExecutor) != (null)) || (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("0ecaa46e-fe7a-3d20-8c65-a30cea580a73"))) ? (((priorityExecutor) != (null)) && (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("4549f0f6-c69e-3bba-84f2-9f93d18c9f63"))) ? (((priorityExecutor) != (null)) && ((level) == (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("0a1bfb1d-9d6a-3120-a7a3-8fe4c09d2cc4"))) ? ((priorityExecutor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6278489f-2d16-33cd-8ffd-faaba5307fac"))) ? (((priorityExecutor) == (null)) && (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("ec4febb7-76d1-3f0e-8424-ff0c3f54af3b"))) ? (((priorityExecutor) != (null)) || ((level) >= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("11206008-fad2-361d-bebe-7ea527511c4b"))) ? ((level) >= (highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("ede7a7d4-2c75-3552-8b7a-cafb7ded99c5"))) ? (level > highPriorityLevel) : (((KnobRuntime.check(java.util.UUID.fromString("89698161-f629-317a-888d-69882f38f984"))) ? (((priorityExecutor) != (null)) || ((level) == (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("9100aedd-1604-38ca-ae48-b20adb36fa5c"))) ? ((priorityExecutor != null) || ((level) > (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("4c396838-4179-35c0-b5b5-7b6b66fe1618"))) ? (((priorityExecutor) == (null)) && ((level) >= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("c5e6938f-a6fd-306f-b583-32a2af778981"))) ? ((priorityExecutor != null) && ((level) != (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("15f48c7f-97de-34cf-8286-0a5d434b248e"))) ? ((priorityExecutor != null) || (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("64c40d4f-068a-3f20-8189-d96cbef419b6"))) ? (((priorityExecutor) != (null)) && ((level) < (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("4fa75bbd-2d1a-37f3-aa88-ce04ffb58b02"))) ? (((priorityExecutor) == (null)) || ((level) <= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("554a1f23-0c98-3b8e-ac81-885bbbdc05f2"))) ? ((level) == (highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("62bf77a0-4a98-3811-875e-a21b2456b7ce"))) ? ((priorityExecutor != null) || ((level) < (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("da6a13e6-8ab0-3854-9dfe-979900eba5f5"))) ? ((priorityExecutor != null) || ((level) >= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("08ec4d47-b00c-34c1-b34a-765e1c2d2bcf"))) ? (((priorityExecutor) != (null)) || ((level) != (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("8222c715-6be2-3789-b9b6-6da3c61b771d"))) ? (((priorityExecutor) == (null)) || ((level) > (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("b912c16c-ad1e-3561-8f89-69c8f058f90f"))) ? ((priorityExecutor != null) && ((level) > (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("158f2210-8121-3108-bc1d-009626d68cb1"))) ? (((priorityExecutor) != (null)) && ((level) > (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("272f2697-77c3-37e5-983b-16ec820e29a6"))) ? (((priorityExecutor) == (null)) || (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("54be0fa0-4835-3c60-8518-ab6cb7d5d83b"))) ? (((priorityExecutor) == (null)) || ((level) == (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("51b6a201-9516-3629-8d08-557443966d40"))) ? ((priorityExecutor != null) && ((level) <= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("2a4b167f-d889-32bb-90d7-8752db60699c"))) ? ((priorityExecutor != null) || ((level) <= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("041df555-2f6f-35c8-8cab-e9be60f402ab"))) ? ((priorityExecutor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("636b0e16-9db1-3165-a401-6e2884429c5a"))) ? ((priorityExecutor != null) && (level > highPriorityLevel)) : (((KnobRuntime.check(java.util.UUID.fromString("4ebe3e87-65af-3dbe-946d-72e88cf78ee2"))) ? (((priorityExecutor) == (null)) || ((level) != (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("678ee02f-06d6-33f1-b747-1a5da3160b78"))) ? (((priorityExecutor) == (null)) && ((level) == (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("91a65740-999d-3ada-b660-2b68c8c61a64"))) ? (priorityExecutor != null) : (((KnobRuntime.check(java.util.UUID.fromString("77bca347-e8c1-3242-9296-194140b3dd61"))) ? (((priorityExecutor) != (null)) && ((level) <= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("67fcb486-97d4-3bfc-ac32-3998132d80bf"))) ? ((priorityExecutor != null) && ((level) == (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("eed0497e-39a6-3761-bcfe-59d3761a2553"))) ? (((priorityExecutor) != (null)) && ((level) >= (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("c4ccdd58-18f6-3c51-921d-18d18ce5a7f0"))) ? (((priorityExecutor) == (null)) && ((level) != (highPriorityLevel))) : (((KnobRuntime.check(java.util.UUID.fromString("fed7e3b7-30ce-3dc2-bf54-aedd11c6a99c"))) ? (((priorityExecutor) == (null)) || ((level) >= (highPriorityLevel))) : (priorityExecutor != null && level > highPriorityLevel))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("f03652f3-fc0a-31e4-9115-3ad403bb1071"))) {
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
      return priorityExecutor.dispatch(callTask);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("cc36b16c-b933-31f9-b2b2-b4e7f20a583a"))) ? (((replicationExecutor) != (null)) && ((level) == (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("62f46afc-e2fa-3925-bc6c-ef3f518dfcb7"))) ? (((replicationExecutor) == (null)) && ((level) != (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("ea53bf13-7fb9-36be-b82b-d7ba994deb18"))) ? ((replicationExecutor != null) && ((level) == (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("4763b994-013c-3a9a-aea3-7bb02d8003f5"))) ? ((replicationExecutor != null) && (level == HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("0c7bf466-bbd4-36dc-96d3-a5959bd07712"))) ? ((replicationExecutor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3ee6363-bd6f-3e43-af12-cc53cb154334"))) ? ((replicationExecutor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bf89d49a-d2f5-3430-8321-fa800f497ce7"))) ? (replicationExecutor != null) : (((KnobRuntime.check(java.util.UUID.fromString("b8f07f35-1e39-3ab1-acd2-17269da229ef"))) ? ((level) != (HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("7a70e835-6ca3-3949-984f-f4e0b858df56"))) ? (((replicationExecutor) != (null)) || ((level) == (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("541a20a0-2652-3a1f-ad1d-75bb0712e8fc"))) ? ((level) == (HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("8db5eaa7-065d-3e7d-834c-add3bdeea16f"))) ? ((replicationExecutor != null) && ((level) != (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("f96ed7eb-19b1-3788-8fba-8f42ef4a878e"))) ? (level == HConstants.REPLICATION_QOS) : (((KnobRuntime.check(java.util.UUID.fromString("094bc3b7-2e10-381d-b790-aaf3c6b067ca"))) ? ((replicationExecutor != null) || ((level) == (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("c8cc7f96-7ef0-38a8-9080-3414c97143a6"))) ? (((replicationExecutor) == (null)) || ((level) == (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("eaf01b03-293f-34d0-b90c-65e35002f3a2"))) ? (((replicationExecutor) != (null)) || (level == HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("e9414afc-1519-3a5b-8def-83108063c637"))) ? (((replicationExecutor) == (null)) && (level == HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("52b140aa-ec6e-369d-8c5f-caa6797f588a"))) ? (((replicationExecutor) == (null)) || ((level) != (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("6037875d-c104-3be9-bfa3-55be5c683b44"))) ? ((replicationExecutor != null) || (level == HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("c0ae6f41-222b-3fc9-8d8b-720a6d9b415f"))) ? (((replicationExecutor) == (null)) || (level == HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("343880d9-14f0-33d8-b4c2-5bb8d84bfd2c"))) ? (((replicationExecutor) == (null)) && ((level) == (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("afbc31eb-f395-3e0e-911f-017066b28c50"))) ? (((replicationExecutor) != (null)) && ((level) != (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("f84cdfd6-fb88-35e6-9822-c9dbb4fa2564"))) ? ((replicationExecutor != null) || ((level) != (HConstants.REPLICATION_QOS))) : (((KnobRuntime.check(java.util.UUID.fromString("0b208c71-06e8-3df6-9209-0b38aabee42f"))) ? (((replicationExecutor) != (null)) && (level == HConstants.REPLICATION_QOS)) : (((KnobRuntime.check(java.util.UUID.fromString("63bcaff0-8aec-34c5-97d1-728ff96ba9ea"))) ? (((replicationExecutor) != (null)) || ((level) != (HConstants.REPLICATION_QOS))) : (replicationExecutor != null && level == HConstants.REPLICATION_QOS))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("5b4ddbc5-e4e5-3637-8f77-e63810205d6f"))) {
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
