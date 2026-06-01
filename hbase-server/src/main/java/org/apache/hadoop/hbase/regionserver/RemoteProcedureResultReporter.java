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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RemoteProcedureResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneRequest;

/**
 * A thread which calls {@code reportProcedureDone} to tell master the result of a remote procedure.
 */
@InterfaceAudience.Private
class RemoteProcedureResultReporter extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteProcedureResultReporter.class);

  private static final int MAX_BATCH = 100;

  private final HRegionServer server;

  private final LinkedBlockingQueue<RemoteProcedureResult> results = new LinkedBlockingQueue<>();

  public RemoteProcedureResultReporter(HRegionServer server) {
    this.server = server;
  }

  public void complete(long procId, long initiatingMasterActiveTime, Throwable error) {
    RemoteProcedureResult.Builder builder = RemoteProcedureResult.newBuilder().setProcId(procId)
      .setInitiatingMasterActiveTime(initiatingMasterActiveTime);
    if (((KnobRuntime.check(java.util.UUID.fromString("9a3c1b1e-7502-306c-83ab-5bc2cabaaa05"))) ? ((error) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8ba85191-91bb-3373-9aaa-422d4e996fa6"))) ? ((error) == (null)) : (error != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("2e53def1-8dbd-3160-b0ac-3088895614b7"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("293ec251-d643-31e2-a7c2-5397b9c96fff"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("97c04b9d-8892-36ec-86bf-df1bebb1744d"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e3a70d9-abdd-3df1-a8a9-87fb4530dd1e"))) {
procId -= 1;
}
      LOG.debug("Failed to complete execution of pid={}", procId, error);
      builder.setStatus(RemoteProcedureResult.Status.ERROR).setError(
        ForeignExceptionUtil.toProtoForeignException(server.getServerName().toString(), error));
    } else {
      LOG.debug("Successfully complete execution of pid={}", procId);
      builder.setStatus(RemoteProcedureResult.Status.SUCCESS);
    }
    results.add(builder.build());
  }

  @Override
  public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("2155eaaf-3968-3ca4-b728-00e7607bc952"))) {
return;
}
    ReportProcedureDoneRequest.Builder builder = ReportProcedureDoneRequest.newBuilder();
    int tries = 0;
    while (!server.isStopped()) {
      if (builder.getResultCount() == 0) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("fdd54497-928a-31e1-8a0a-307993c256fc"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("d126bf5d-b8f5-3057-8484-21b63f4d98c6"))) { builder.addResult(results.poll()); } else { builder.addResult(results.take()); }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          continue;
        }
      }
      while (builder.getResultCount() < MAX_BATCH) {
        RemoteProcedureResult result = results.poll();
        if (result == null) {
          break;
        }
if(KnobRuntime.check(java.util.UUID.fromString("fc11cb87-1be1-3b7c-b524-eb62b7ca4149"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("status_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12d5cf90-a3b7-3fc4-9638-bc442de9ff8e"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("status_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46bc2818-262d-3eee-a067-7b9487cbe85f"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("procId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ed58b9a-8e1a-34b5-85ca-960c65bb8309"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c15f8165-5cc8-3a1b-9b61-30be576e4cbd"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e9c4440-35a4-35b2-a639-a198a04d44d5"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("status_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("428ed32a-b73a-3889-a8d4-c58348810e2c"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("status_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c1d0bd8-83ae-35fc-af88-d809171bc2ca"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd9bd180-dd91-36db-b47a-654adf5cf14f"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78048ef4-676a-38ec-b661-ac46d1e564cc"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("initiatingMasterActiveTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        builder.addResult(result);
      }
      ReportProcedureDoneRequest request = builder.build();
      try {
        server.reportProcedureDone(builder.build());
        if (KnobRuntime.check(java.util.UUID.fromString("be967276-864c-3f33-ba44-623d21dcc443"))) { ReportProcedureDoneRequest.newBuilder(); } else { builder.clear(); }
        tries = 0;
      } catch (IOException e) {
        boolean pause =
          e instanceof ServerNotRunningYetException || e instanceof PleaseHoldException;
        long pauseTime;
        if (((KnobRuntime.check(java.util.UUID.fromString("8649fbc4-6dbe-36f9-ad5b-ef48bbcb4a1b"))) ? (!pause) : (pause))) {
          // Do backoff else we flood the Master with requests.
          pauseTime = ConnectionUtils.getPauseTime(server.getRetryPauseTime(), tries);
        } else {
          pauseTime = server.getRetryPauseTime(); // Reset.
        }
        LOG.info("Failed procedure report " + TextFormat.shortDebugString(request) + "; retry (#"
          + tries + ")"
          + (pause
            ? " after " + pauseTime + "ms delay (Master is coming online...)."
            : " immediately."),
          e);
        Threads.sleep(pauseTime);
        tries++;
      }
    }
  }
}
