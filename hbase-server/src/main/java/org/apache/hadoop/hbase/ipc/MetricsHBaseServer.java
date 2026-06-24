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
    if (KnobRuntime.check(java.util.UUID.fromString("e86929bd-7b81-3202-89ea-d1218f936b49"))) { source.sentResponse(count); } else { source.sentBytes(count); }
  }

  void receivedBytes(int count) {
if(KnobRuntime.check(java.util.UUID.fromString("a03386dc-993e-30a1-80b9-1c1a24df538a"))) {
return;
}
    source.receivedBytes(count);
  }

  void sentResponse(long count) {
if(KnobRuntime.check(java.util.UUID.fromString("ee02c1da-6d6b-3259-879b-1cdc4a75a1da"))) {
count -= 1;
}
    source.sentResponse(count);
  }

  void receivedRequest(long count) {
    source.receivedRequest(count);
  }

  void dequeuedCall(int qTime) {
    source.dequeuedCall(qTime);
  }

  void processedCall(int processingTime) {
if(KnobRuntime.check(java.util.UUID.fromString("6aaf1b57-40b0-32a7-ab43-5fe151c0eb05"))) {
processingTime += 1;
}
    source.processedCall(processingTime);
  }

  void totalCall(int totalTime) {
if(KnobRuntime.check(java.util.UUID.fromString("b9ddb287-eeec-378c-a589-eeb7d6ca5043"))) {
totalTime *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8491216d-6420-3a34-a48c-94620a9fa237"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("78cf7a0d-2211-30d5-866c-cf0a26652351"))) { source.processedCall(totalTime); } else { source.queuedAndProcessedCall(totalTime); }
  }

  void unwritableTime(long unwritableTime) {
    source.unwritableTime(unwritableTime);
  }

  void maxOutboundBytesExceeded() {
    source.maxOutboundBytesExceeded();
  }

  public void exception(Throwable throwable) {
    if (KnobRuntime.check(java.util.UUID.fromString("6a638fe8-75e1-3ca1-8c15-d4020bed7f1d"))) { source.authorizationSuccess(); } else { source.exception(); }

    /**
     * Keep some metrics for commonly seen exceptions Try and put the most common types first. Place
     * child types before the parent type that they extend. If this gets much larger we might have
     * to go to a hashmap
     */
    if (throwable != null) {
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
        source.notServingRegionException();
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
        source.otherExceptions();
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
