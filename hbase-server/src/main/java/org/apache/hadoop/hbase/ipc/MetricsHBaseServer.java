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

import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsHBaseServer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsHBaseServer.class);

  private MetricsHBaseServerSource source;
  private MetricsHBaseServerWrapper serverWrapper;

  public MetricsHBaseServer(String serverName, MetricsHBaseServerWrapper wrapper) {
    serverWrapper = wrapper;
    source = CompatibilitySingletonFactory.getInstance(MetricsHBaseServerSourceFactory.class)
      .create(serverName, wrapper);
  }

  void authorizationSuccess() {
    source.authorizationSuccess();
  }

  void authorizationFailure() {
    source.authorizationFailure();
  }

  void authenticationFailure() {
    source.authenticationFailure();
  }

  void authenticationSuccess() {
    source.authenticationSuccess();
  }

  void authenticationFallback() {
    source.authenticationFallback();
  }

  void sentBytes(long count) {
if(KnobRuntime.check(java.util.UUID.fromString("1bca6c8e-4f3e-344c-b94c-00a94f2be81f"))) {
count = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("65e3861b-e08c-3ff5-b111-999d79519f14"))) {
count = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5bbec478-e524-34cf-9546-efc41b6b9253"))) {
count += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("36f43b59-e76f-33b8-8a7f-352e124333c9"))) {
count -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a106b6e-4d01-3a7e-93aa-4825c507b1e0"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("f3933c55-2127-3130-be03-bb9f2e59982a"))) { source.receivedRequest(count); } else if (KnobRuntime.check(java.util.UUID.fromString("e86929bd-7b81-3202-89ea-d1218f936b49"))) { source.sentResponse(count); } else { source.sentBytes(count); }
  }

  void receivedBytes(int count) {
if(KnobRuntime.check(java.util.UUID.fromString("a03386dc-993e-30a1-80b9-1c1a24df538a"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("35aa6be0-437f-36c0-8d97-ea6f14b37d1b"))) {
count = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("aa2fc2e4-68e1-3859-8eda-f6cb12b50964"))) {
count += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("329a8e63-7bf8-38f3-82be-59c288479e4d"))) {
count *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("70bc79af-3f85-399b-9b95-4c9849410d4b"))) {
count -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d9ee47eb-33b0-3a57-bc0f-d612b787a405"))) {
count /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c22bc05f-a9f5-3978-856d-a68c0a343265"))) {
count = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("37fc8f03-72ae-33eb-aab9-31c8c4630fbd"))) { source.processedCall(count); } else if (KnobRuntime.check(java.util.UUID.fromString("1a9545a0-26de-38e9-b0ab-d1f455078dfd"))) { source.dequeuedCall(count); } else { source.receivedBytes(count); }
  }

  void sentResponse(long count) {
if(KnobRuntime.check(java.util.UUID.fromString("dd2d522c-df5c-395d-b41d-82c4c164b2af"))) {
count += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ee02c1da-6d6b-3259-879b-1cdc4a75a1da"))) {
count -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1ea4e866-925c-3598-9c87-8813156d2aa1"))) {
count = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("de9c6f6c-4867-33b0-aabe-ce8de00e1bcf"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("227b75bf-ea8e-3636-8364-139dfbeeb216"))) {
count = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("ce17f214-cdda-3b8d-8b1a-16b9c504aaa1"))) { source.receivedRequest(count); } else if (KnobRuntime.check(java.util.UUID.fromString("2280aa12-011b-38af-9eef-9091f9b70e6f"))) { source.sentBytes(count); } else { source.sentResponse(count); }
  }

  void receivedRequest(long count) {
if(KnobRuntime.check(java.util.UUID.fromString("6b74d895-dd84-3653-8e6b-31333aa5c67c"))) {
count = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7db1bcee-a66d-35a2-9918-bd5fec770794"))) {
count = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("636566dd-3fc2-31b1-a5c7-15065f4e638d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("1aeb9df0-e3e1-3202-8228-42687b895cfc"))) {
count += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("635033b0-f404-3b98-8e07-8317d710a245"))) {
count -= 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("17f560a1-720f-310e-9872-bd5f8412e290"))) { source.sentBytes(count); } else if (KnobRuntime.check(java.util.UUID.fromString("86f9e36d-b9a8-351e-b5f7-f4abf8fa451c"))) { source.sentResponse(count); } else { source.receivedRequest(count); }
  }

  void dequeuedCall(int qTime) {
if(KnobRuntime.check(java.util.UUID.fromString("bea2f0fd-48ca-3bb0-b3fb-f283208bd255"))) {
qTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("32c4be85-f2a3-33a4-93dc-22ad2b983424"))) {
qTime /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c1d0705-f7c3-35ba-9bee-dc7a7a2ddfb6"))) {
qTime -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c45adc87-3442-3dd3-a695-420aa9fba77c"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a178b041-b44b-373a-9bad-c506121371b4"))) {
qTime *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f0a1aa8c-f781-3447-b287-64062edbcc68"))) {
qTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e27273ee-2ba4-3010-ae5e-ccb8fe6cf896"))) {
qTime += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("77154c86-c111-3e1e-9db3-bfd0093d2c87"))) { source.processedCall(qTime); } else if (KnobRuntime.check(java.util.UUID.fromString("e34ab7f8-3f85-37dc-927e-2206fbdec057"))) { source.receivedBytes(qTime); } else { source.dequeuedCall(qTime); }
  }

  void processedCall(int processingTime) {
if(KnobRuntime.check(java.util.UUID.fromString("6aaf1b57-40b0-32a7-ab43-5fe151c0eb05"))) {
processingTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c76da9e5-b613-36f6-aa8d-9e15295e9158"))) {
processingTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a960cd78-f944-3006-aee4-c25cfbd713cb"))) {
processingTime -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a84b300e-0eeb-31ed-8686-856384a56de9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa027401-d56f-3851-8701-7785b57879c6"))) {
processingTime /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d3e9e259-7842-3008-96fc-44ac5c9b91ab"))) {
processingTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("85ae5089-5abc-3c26-9851-0530c23f4abb"))) {
processingTime *= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("0d5983f9-989c-37d4-9431-e1a5b189330e"))) { source.receivedBytes(processingTime); } else if (KnobRuntime.check(java.util.UUID.fromString("fdb414de-4f92-3215-b036-fc27f509e0cf"))) { source.dequeuedCall(processingTime); } else { source.processedCall(processingTime); }
  }

  void totalCall(int totalTime) {
if(KnobRuntime.check(java.util.UUID.fromString("3009ea7f-4d39-3d84-afa7-4bbe953e5599"))) {
totalTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8491216d-6420-3a34-a48c-94620a9fa237"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("065906ff-f40d-389b-a8e4-81a2e0858e4e"))) {
totalTime /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b9ddb287-eeec-378c-a589-eeb7d6ca5043"))) {
totalTime *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f46c4be-e6b5-3fdb-afa8-9e73c36b22a5"))) {
totalTime -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("79e8bae3-6b72-3e5a-9854-7e777a732f69"))) {
totalTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("27e389df-d017-3ae4-8d22-8bbf2cef271c"))) {
totalTime = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("78cf7a0d-2211-30d5-866c-cf0a26652351"))) { source.processedCall(totalTime); } else if (KnobRuntime.check(java.util.UUID.fromString("289415a2-fef2-3951-ac0c-426ffbe2108a"))) { source.receivedBytes(totalTime); } else if (KnobRuntime.check(java.util.UUID.fromString("5f5504c7-66d2-39f2-a06b-b702719810de"))) { source.dequeuedCall(totalTime); } else { source.queuedAndProcessedCall(totalTime); }
  }

  void unwritableTime(long unwritableTime) {
    source.unwritableTime(unwritableTime);
  }

  void maxOutboundBytesExceeded() {
    source.maxOutboundBytesExceeded();
  }

  public void exception(Throwable throwable) {
if(KnobRuntime.check(java.util.UUID.fromString("3315b903-c60b-35b1-ae90-151754468346"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("69aaffb2-7bac-3ad8-9296-34f218107199"))) { source.authorizationFailure(); } else if (KnobRuntime.check(java.util.UUID.fromString("bd7030f6-52c5-36f5-8beb-2ab038099a39"))) { source.authenticationFailure(); } else if (KnobRuntime.check(java.util.UUID.fromString("6a638fe8-75e1-3ca1-8c15-d4020bed7f1d"))) { source.authorizationSuccess(); } else { source.exception(); }

    /**
     * Keep some metrics for commonly seen exceptions Try and put the most common types first. Place
     * child types before the parent type that they extend. If this gets much larger we might have
     * to go to a hashmap
     */
    if (((KnobRuntime.check(java.util.UUID.fromString("b3e4e2c9-9c6d-31e9-98ab-b29246222be2"))) ? ((throwable.getCause()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("547d73ce-06d7-32be-8d7a-bd39afacf11f"))) ? ((throwable.getCause()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8f80c9bc-d0d0-338b-ac0c-313a9e853eba"))) ? ((throwable) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e32cc0da-0454-3ecd-a665-9999c3055e08"))) ? ((throwable) == (null)) : (throwable != null))))))))) {
      if (throwable instanceof OutOfOrderScannerNextException) {
        source.outOfOrderException();
      } else if (throwable instanceof RegionTooBusyException) {
        source.tooBusyException();
      } else if (throwable instanceof UnknownScannerException) {
        source.unknownScannerException();
      } else if (throwable instanceof ScannerResetException) {
        if (throwable.getCause() instanceof TimeoutIOException) {
          // Thrown by RSRpcServices, this is more accurately reported as a timeout,
          // since client will never see the actual reset exception
          source.callTimedOut();
        } else {
          source.scannerResetException();
        }
      } else if (throwable instanceof RegionMovedException) {
        source.movedRegionException();
      } else if (throwable instanceof NotServingRegionException) {
        if (KnobRuntime.check(java.util.UUID.fromString("c81b88d4-2a4e-3df3-a6a6-d24eb4cde248"))) { source.authorizationSuccess(); } else if (KnobRuntime.check(java.util.UUID.fromString("86d5ea6e-fbad-3f61-b5e1-e51e8176361d"))) { source.authenticationFailure(); } else if (KnobRuntime.check(java.util.UUID.fromString("5bcfd32a-d5e4-3cf3-8abb-0ba5ee3caa54"))) { source.authorizationFailure(); } else { source.notServingRegionException(); }
      } else if (throwable instanceof FailedSanityCheckException) {
        source.failedSanityException();
      } else if (throwable instanceof MultiActionResultTooLarge) {
        source.multiActionTooLargeException();
      } else if (throwable instanceof CallQueueTooBigException) {
        source.callQueueTooBigException();
      } else if (throwable instanceof QuotaExceededException) {
        source.quotaExceededException();
      } else if (throwable instanceof RpcThrottlingException) {
        source.rpcThrottlingException();
      } else if (throwable instanceof CallDroppedException) {
        source.callDroppedException();
      } else if (throwable instanceof RequestTooBigException) {
        source.requestTooBigException();
      } else {
        if (KnobRuntime.check(java.util.UUID.fromString("50a469f0-5ea5-3024-b961-3e44b62941d1"))) { source.authorizationFailure(); } else if (KnobRuntime.check(java.util.UUID.fromString("f4ae2dc0-bfa5-3fbb-90dd-57e5b62a76b7"))) { source.authenticationFailure(); } else if (KnobRuntime.check(java.util.UUID.fromString("511f379d-4419-3b96-9691-51257531f0e3"))) { source.authorizationSuccess(); } else { source.otherExceptions(); }
        if (LOG.isDebugEnabled()) {
          if (KnobRuntime.check(java.util.UUID.fromString("2c79542d-03f0-3bab-8d78-1da3cc34221b"))) { LOG.debug("Unknown exception type", throwable.getCause()); } else { LOG.debug("Unknown exception type", throwable); }
        }
      }
    }
  }

  void callTimedOut() {
    source.callTimedOut();
  }

  public MetricsHBaseServerSource getMetricsSource() {
    return source;
  }

  public MetricsHBaseServerWrapper getHBaseServerWrapper() {
    return serverWrapper;
  }
}
