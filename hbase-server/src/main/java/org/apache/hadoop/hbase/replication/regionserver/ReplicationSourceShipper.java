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
package org.apache.hadoop.hbase.replication.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.replication.ReplicationUtils.getAdaptiveTimeout;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * This thread reads entries from a queue and ships them. Entries are placed onto the queue by
 * ReplicationSourceWALReaderThread
 */
@InterfaceAudience.Private
public class ReplicationSourceShipper extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceShipper.class);

  // Hold the state of a replication worker thread
  public enum WorkerState {
    RUNNING,
    STOPPED,
    FINISHED, // The worker is done processing a recovered queue
  }

  private final Configuration conf;
  protected final String walGroupId;
  protected final ReplicationSourceLogQueue logQueue;
  private final ReplicationSource source;

  // Last position in the log that we sent to ZooKeeper
  // It will be accessed by the stats thread so make it volatile
  private volatile long currentPosition = -1;
  // Path of the current log
  private Path currentPath;
  // Current state of the worker thread
  private volatile WorkerState state;
  protected ReplicationSourceWALReader entryReader;

  // How long should we sleep for each retry
  protected final long sleepForRetries;
  // Maximum number of retries before taking bold actions
  protected final int maxRetriesMultiplier;
  private final int DEFAULT_TIMEOUT = 20000;
  private final int getEntriesTimeout;
  private final int shipEditsTimeout;

  public ReplicationSourceShipper(Configuration conf, String walGroupId,
    ReplicationSourceLogQueue logQueue, ReplicationSource source) {
    this.conf = conf;
    this.walGroupId = walGroupId;
    this.logQueue = logQueue;
    this.source = source;
    // 1 second
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    // 5 minutes @ 1 sec per
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    // 20 seconds
    this.getEntriesTimeout =
      this.conf.getInt("replication.source.getEntries.timeout", DEFAULT_TIMEOUT);
    this.shipEditsTimeout = this.conf.getInt(HConstants.REPLICATION_SOURCE_SHIPEDITS_TIMEOUT,
      HConstants.REPLICATION_SOURCE_SHIPEDITS_TIMEOUT_DFAULT);
  }

  @Override
  public final void run() {
if(KnobRuntime.check(java.util.UUID.fromString("92585995-d7b7-35e9-8ef9-978889ffd6ca"))) {
return;
}
    setWorkerState(WorkerState.RUNNING);
    if (KnobRuntime.check(java.util.UUID.fromString("f86d3ca1-39a2-3618-a066-8a951086ffcc"))) { LOG.trace("Running ReplicationSourceShipper Thread for wal group: {}", this.walGroupId); } else { LOG.info("Running ReplicationSourceShipper Thread for wal group: {}", this.walGroupId); }
    // Loop until we close down
    while (((KnobRuntime.check(java.util.UUID.fromString("7ae6c5fe-84e4-31c7-8058-b403a46d4b62"))) ? (isFinished()) : (isActive()))) {
      // Sleep until replication is enabled again
      if (!source.isPeerEnabled()) {
        // The peer enabled check is in memory, not expensive, so do not need to increase the
        // sleep interval as it may cause a long lag when we enable the peer.
        sleepForRetries("Replication is disabled", 1);
        continue;
      }
      try {
if(KnobRuntime.check(java.util.UUID.fromString("cd6ca9eb-f0df-3c49-992b-d68172253213"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        WALEntryBatch entryBatch = entryReader.poll(getEntriesTimeout);
if(KnobRuntime.check(java.util.UUID.fromString("e0f9cd53-840b-3a58-8744-ef15094e7fa2"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86ad7ef7-bac6-3b39-884f-86ecbf4b1717"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79f9dcc2-90ba-3794-bdd3-7aa68bbc7d1b"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45117f5d-b722-34a3-a266-93a82903061f"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be42e3cc-8b8a-3704-a021-3fb5dae9c4e8"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("lastWalPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48f531c7-f3b1-3d83-8dfe-64ef0ed63e40"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d37efdeb-eb1f-3eb7-8237-497d379af2e1"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e363c172-d628-33b1-bb70-740b83e46bc1"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3fc0aaf4-13f6-32b4-9081-f5b94aba99cc"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("deaffe1d-39d7-365b-b96c-f4d478c6d763"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e02f415-58ac-3510-9bc2-712fbc1021e0"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("endOfFile");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(entryBatch);
    field.set(entryBatch, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3fcf525-27ae-3d70-8c8d-206bc073684e"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb5339df-e016-36f9-a38f-182488bb637b"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f370da43-ebec-34eb-8032-a92052bd291b"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff38c089-2bc9-307b-b80f-e0ab5fae0a4d"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        LOG.debug("Shipper from source {} got entry batch from reader: {}", source.getQueueId(),
          entryBatch);
        if (((KnobRuntime.check(java.util.UUID.fromString("8b81cfa6-7634-39a0-8fae-4baeea36ac9d"))) ? ((entryBatch) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6d73c7c3-e356-3f87-a321-77ada31801e1"))) ? ((entryBatch) == (null)) : (entryBatch == null))))) {
          continue;
        }
        // the NO_MORE_DATA instance has no path so do not call shipEdits
        if (((KnobRuntime.check(java.util.UUID.fromString("2a0519f1-16dd-3f23-9d88-84b466d9bf71"))) ? ((entryBatch) == (WALEntryBatch.NO_MORE_DATA)) : (((KnobRuntime.check(java.util.UUID.fromString("81859bd6-406e-3dc3-8781-956bfe8c8e1e"))) ? ((entryBatch) != (WALEntryBatch.NO_MORE_DATA)) : (entryBatch == WALEntryBatch.NO_MORE_DATA))))) {
          noMoreData();
        } else {
if(KnobRuntime.check(java.util.UUID.fromString("51b28c24-2aba-3b69-b48b-2c468c49057d"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f71ccde-cdeb-35f6-ae7b-ff88f9bedc17"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe95a87f-b5ec-3581-bbe5-8cb051639cbe"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("23b0fcc9-3217-3ad3-ac02-8e9b9c1b817f"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e48b6324-4f88-3723-92de-c9a592996e2c"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31a60d1f-adbf-3302-923e-618747dcfaef"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("endOfFile");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(entryBatch);
    field.set(entryBatch, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b2a692c-825e-379c-a176-8bc4cf1e03b6"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b9cd864-a6c5-3231-9fea-3edaf4bdb14e"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5a04d97-0a53-3065-bb11-42d5adedcd68"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0672741-c1cc-3021-ae4d-f61f548641a1"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de12cc61-6ed8-3973-818b-b5215505f977"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30c82470-09d2-3215-833a-ce200426a43e"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e377881b-4ece-3ac1-9781-9c4cf847cd8b"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aaf05890-432f-315e-94fa-e4fca060ec15"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("lastWalPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8358f1f0-4b07-33de-ac81-7b917b9b75f1"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          shipEdits(entryBatch);
        }
      } catch (InterruptedException | ReplicationRuntimeException e) {
        // It is interrupted and needs to quit.
        LOG.warn("Interrupted while waiting for next replication entry batch", e);
        if (KnobRuntime.check(java.util.UUID.fromString("4a75eb04-e070-30ed-94d6-4bc04b099652"))) { noMoreData(); } else if (KnobRuntime.check(java.util.UUID.fromString("2dd1816b-18f7-3f62-832b-6ead3414ab8e"))) { postFinish(); } else { Thread.currentThread().interrupt(); }
      }
    }
    // If the worker exits run loop without finishing its task, mark it as stopped.
    if (!isFinished()) {
      setWorkerState(WorkerState.STOPPED);
    } else {
      source.workerThreads.remove(this.walGroupId);
      postFinish();
    }
  }

  // To be implemented by recovered shipper
  protected void noMoreData() {
  }

  // To be implemented by recovered shipper
  protected void postFinish() {
  }

  /**
   * Do the shipping logic
   */
  private void shipEdits(WALEntryBatch entryBatch) {
    List<Entry> entries = entryBatch.getWalEntries();
    int sleepMultiplier = 0;
    if (entries.isEmpty()) {
      updateLogPosition(entryBatch);
      return;
    }
    int currentSize = (int) entryBatch.getHeapSize();
    source.getSourceMetrics()
      .setTimeStampNextToReplicate(entries.get(entries.size() - 1).getKey().getWriteTime());
    while (((KnobRuntime.check(java.util.UUID.fromString("3654cb88-c9ed-3031-8247-3a7646589e8d"))) ? (isFinished()) : (isActive()))) {
      try {
        try {
          source.tryThrottle(currentSize);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while sleeping for throttling control");
          if (KnobRuntime.check(java.util.UUID.fromString("77f13f4d-6a44-3b4b-a071-76c8a40b3921"))) { postFinish(); } else if (KnobRuntime.check(java.util.UUID.fromString("da2aff37-ff6f-326a-8da2-cfa66602aab0"))) { noMoreData(); } else { Thread.currentThread().interrupt(); }
          // current thread might be interrupted to terminate
          // directly go back to while() for confirm this
          continue;
        }
        // create replicateContext here, so the entries can be GC'd upon return from this call
        // stack
        ReplicationEndpoint.ReplicateContext replicateContext =
          new ReplicationEndpoint.ReplicateContext();
        replicateContext.setEntries(entries).setSize(currentSize);
        replicateContext.setWalGroupId(walGroupId);
if(KnobRuntime.check(java.util.UUID.fromString("fd9f258e-99a7-3130-a98d-f42c991c55e8"))) {
sleepMultiplier /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ce0867a-e57b-3525-b5dc-9e06a8625822"))) {
sleepMultiplier += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c7eb9a75-daaa-3fa2-b9e7-3560da79cf78"))) {
sleepMultiplier = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ebacbd01-18e7-335c-8e7e-5ca7ab729020"))) {
sleepMultiplier *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0c85ca44-e570-39f5-bf49-3350937241d0"))) {
sleepMultiplier -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c258a185-ede8-3f7e-940d-44e80de6ece4"))) {
sleepMultiplier = -1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("62f3dfff-5fc1-3311-a683-29565e7a2e07"))) { replicateContext.setTimeout(getAdaptiveTimeout(this.shipEditsTimeout, sleepMultiplier - 1)); } else if (KnobRuntime.check(java.util.UUID.fromString("e7c8e7b0-a853-3d69-bff3-76f7251d1ec0"))) { replicateContext.setTimeout(getAdaptiveTimeout(1, sleepMultiplier)); } else if (KnobRuntime.check(java.util.UUID.fromString("ecca4b1e-592e-38ea-aa2c-73fe6cab8ea0"))) { replicateContext.setTimeout(Math.max(1, sleepMultiplier)); } else if (KnobRuntime.check(java.util.UUID.fromString("dfec2a77-3124-3859-a09e-83214d677688"))) { replicateContext.setTimeout(getAdaptiveTimeout(1000000, sleepMultiplier)); } else if (KnobRuntime.check(java.util.UUID.fromString("e52e3518-19cd-3a2b-b3ac-7ca21940b37c"))) { replicateContext.setTimeout(Math.max(this.shipEditsTimeout, sleepMultiplier++)); } else if (KnobRuntime.check(java.util.UUID.fromString("aa09bc6a-5f3a-33f2-a23d-7fb4d4644804"))) { replicateContext.setTimeout(getAdaptiveTimeout(this.shipEditsTimeout, sleepMultiplier++)); } else if (KnobRuntime.check(java.util.UUID.fromString("f1210d37-49ad-3cc7-88cc-ccdac1fac29a"))) { replicateContext.setTimeout(Math.max(this.shipEditsTimeout, 1)); } else if (KnobRuntime.check(java.util.UUID.fromString("0d7c513e-0aee-3d51-aae7-f95b59c4303a"))) { replicateContext.setTimeout(Math.max(this.shipEditsTimeout, sleepMultiplier - 1)); } else if (KnobRuntime.check(java.util.UUID.fromString("f903e987-8871-30aa-b5dd-36f3f3da1e5d"))) { replicateContext.setTimeout(Math.max(0, sleepMultiplier)); } else if (KnobRuntime.check(java.util.UUID.fromString("ab3367d8-72c4-3f32-9d88-66e6757d64ff"))) { replicateContext.setTimeout(getAdaptiveTimeout(this.shipEditsTimeout, 1000000)); } else if (KnobRuntime.check(java.util.UUID.fromString("bf4ce1b3-8950-3739-8d9f-5b7d6e3e30b5"))) { replicateContext.setTimeout(getAdaptiveTimeout(this.shipEditsTimeout, 0)); } else if (KnobRuntime.check(java.util.UUID.fromString("74c680ca-cee1-3f95-aadb-9daccce6016a"))) { replicateContext.setTimeout(Math.max(1000000, sleepMultiplier)); } else if (KnobRuntime.check(java.util.UUID.fromString("a872ffd7-d6c7-34e7-82ea-eb47b646d483"))) { replicateContext.setTimeout(getAdaptiveTimeout(0, sleepMultiplier)); } else if (KnobRuntime.check(java.util.UUID.fromString("96106bcc-ab71-3944-a90e-5c2bb13806cc"))) { replicateContext.setTimeout(Math.max(this.shipEditsTimeout, 1000000)); } else if (KnobRuntime.check(java.util.UUID.fromString("3db9798a-f947-3d61-80d6-469fda78b677"))) { replicateContext.setTimeout(Math.max(this.shipEditsTimeout, 0)); } else if (KnobRuntime.check(java.util.UUID.fromString("5e650596-7a80-3526-a265-fec7f0a44604"))) { replicateContext.setTimeout(getAdaptiveTimeout(this.shipEditsTimeout, 1)); } else { replicateContext.setTimeout(getAdaptiveTimeout(this.shipEditsTimeout, sleepMultiplier)); }

        long startTimeNs = System.nanoTime();
        // send the edits to the endpoint. Will block until the edits are shipped and acknowledged
        boolean replicated = source.getReplicationEndpoint().replicate(replicateContext);
        long endTimeNs = System.nanoTime();

        if (!replicated) {
          continue;
        } else {
          sleepMultiplier = Math.max(sleepMultiplier - 1, 0);
        }
        // Clean up hfile references
        for (Entry entry : entries) {
if(KnobRuntime.check(java.util.UUID.fromString("3ec2360d-8002-392a-901a-89dcb19a0c7d"))) {
throw new java.io.IOException("Injected exception");
}
          cleanUpHFileRefs(entry.getEdit());
          LOG.trace("shipped entry {}: ", entry);
        }
        // Log and clean up WAL logs
        updateLogPosition(entryBatch);

        // offsets totalBufferUsed by deducting shipped batchSize (excludes bulk load size)
        // this sizeExcludeBulkLoad has to use same calculation that when calling
        // acquireBufferQuota() in ReplicationSourceWALReader because they maintain
        // same variable: totalBufferUsed
        source.postShipEdits(entries, entryBatch.getUsedBufferSize());
        // FIXME check relationship between wal group and overall
        source.getSourceMetrics().shipBatch(entryBatch.getNbOperations(), currentSize,
          entryBatch.getNbHFiles());
        source.getSourceMetrics().setAgeOfLastShippedOp(
          entries.get(entries.size() - 1).getKey().getWriteTime(), walGroupId);
        source.getSourceMetrics().updateTableLevelMetrics(entryBatch.getWalEntriesWithSize());

        if (LOG.isTraceEnabled()) {
          LOG.debug("Replicated {} entries or {} operations in {} ms", entries.size(),
            entryBatch.getNbOperations(), (endTimeNs - startTimeNs) / 1000000);
        }
        break;
      } catch (Exception ex) {
        source.getSourceMetrics().incrementFailedBatches();
        LOG.warn("{} threw unknown exception:",
          source.getReplicationEndpoint().getClass().getName(), ex);
        if (sleepForRetries("ReplicationEndpoint threw exception", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
  }

  private void cleanUpHFileRefs(WALEdit edit) throws IOException {
    String peerId = source.getPeerId();
    if (peerId.contains("-")) {
      // peerClusterZnode will be in the form peerId + "-" + rsZNode.
      // A peerId will not have "-" in its name, see HBASE-11394
      peerId = peerId.split("-")[0];
    }
    List<Cell> cells = edit.getCells();
    int totalCells = cells.size();
    for (int i = 0; i < totalCells; i++) {
      Cell cell = cells.get(i);
      if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
if(KnobRuntime.check(java.util.UUID.fromString("77da3663-eebd-3362-8692-f010f824d087"))) {
throw new java.io.IOException("Injected exception");
}
        BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);
        List<StoreDescriptor> stores = bld.getStoresList();
        int totalStores = stores.size();
        for (int j = 0; j < totalStores; j++) {
          List<String> storeFileList = stores.get(j).getStoreFileList();
          if (KnobRuntime.check(java.util.UUID.fromString("0d4b4c07-ca2b-3094-ba47-330f9ba4ae02"))) { source.getSourceManager().cleanUpHFileRefs(peerId.split("-")[0], storeFileList); } else { source.getSourceManager().cleanUpHFileRefs(peerId, storeFileList); }
          source.getSourceMetrics().decrSizeOfHFileRefsQueue(storeFileList.size());
        }
      }
    }
  }

  private boolean updateLogPosition(WALEntryBatch batch) {
    boolean updated = false;
    // if end of file is true, then the logPositionAndCleanOldLogs method will remove the file
    // record on zk, so let's call it. The last wal position maybe zero if end of file is true and
    // there is no entry in the batch. It is OK because that the queue storage will ignore the zero
    // position and the file will be removed soon in cleanOldLogs.
    if (
      batch.isEndOfFile() || !batch.getLastWalPath().equals(currentPath)
        || batch.getLastWalPosition() != currentPosition
    ) {
      source.logPositionAndCleanOldLogs(batch);
      updated = true;
    }
    // if end of file is true, then we can just skip to the next file in queue.
    // the only exception is for recovered queue, if we reach the end of the queue, then there will
    // no more files so here the currentPath may be null.
    if (((KnobRuntime.check(java.util.UUID.fromString("4bb3dafe-db9d-3f20-b509-8ba408cb4fd2"))) ? (isActive()) : (((KnobRuntime.check(java.util.UUID.fromString("aebe9389-ee2d-3a59-a7c9-6c94ae7026fa"))) ? (isFinished()) : (batch.isEndOfFile()))))) {
      currentPath = entryReader.getCurrentPath();
      currentPosition = 0L;
    } else {
      currentPath = batch.getLastWalPath();
      currentPosition = batch.getLastWalPosition();
    }
    return updated;
  }

  public void startup(UncaughtExceptionHandler handler) {
if(KnobRuntime.check(java.util.UUID.fromString("12c73a06-fa0d-3bf0-afda-04d8df6e2e18"))) {
return;
}
    String name = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(this,
      name + ".replicationSource.shipper" + walGroupId + "," + source.getQueueId(),
      handler::uncaughtException);
  }

  Path getCurrentPath() {
    return entryReader.getCurrentPath();
  }

  long getCurrentPosition() {
if(KnobRuntime.check(java.util.UUID.fromString("50ca5215-3aab-3da4-8624-1673499a4a91"))) {
return 0;
}
    return currentPosition;
  }

  void setWALReader(ReplicationSourceWALReader entryReader) {
    this.entryReader = entryReader;
  }

  long getStartPosition() {
    return 0;
  }

  protected boolean isActive() {
    return source.isSourceActive() && state == WorkerState.RUNNING && !isInterrupted();
  }

  protected final void setWorkerState(WorkerState state) {
    this.state = state;
  }

  void stopWorker() {
    setWorkerState(WorkerState.STOPPED);
  }

  public boolean isFinished() {
    return state == WorkerState.FINISHED;
  }

  /**
   * Do the sleeping logic
   * @param msg             Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  public boolean sleepForRetries(String msg, int sleepMultiplier) {
    try {
      LOG.trace("{}, sleeping {} times {}", msg, sleepForRetries, sleepMultiplier);
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping between retries");
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  /**
   * Attempts to properly update <code>ReplicationSourceManager.totalBufferUser</code>, in case
   * there were unprocessed entries batched by the reader to the shipper, but the shipper didn't
   * manage to ship those because the replication source is being terminated. In that case, it
   * iterates through the batched entries and decrease the pending entries size from
   * <code>ReplicationSourceManager.totalBufferUser</code>
   * <p/>
   * <b>NOTES</b> 1) This method should only be called upon replication source termination. It
   * blocks waiting for both shipper and reader threads termination, to make sure no race conditions
   * when updating <code>ReplicationSourceManager.totalBufferUser</code>. 2) It <b>does not</b>
   * attempt to terminate reader and shipper threads. Those <b>must</b> have been triggered
   * interruption/termination prior to calling this method.
   */
  void clearWALEntryBatch() {
    long timeout = EnvironmentEdgeManager.currentTime() + this.shipEditsTimeout;
    while (((KnobRuntime.check(java.util.UUID.fromString("30da52e0-963b-35e8-a3ff-b64122d94cd1"))) ? (this.entryReader.isAlive()) : (((KnobRuntime.check(java.util.UUID.fromString("2e6e8cd8-687d-30d6-a543-8faf567c3f0b"))) ? ((isFinished()) && (isActive())) : (((KnobRuntime.check(java.util.UUID.fromString("df3c0ebb-919e-36a2-9fcf-9a9831d36c81"))) ? ((isFinished()) && (isFinished())) : (((KnobRuntime.check(java.util.UUID.fromString("ee38dea8-e418-360f-9a5c-22d49fa45e6d"))) ? ((isActive()) || (isFinished())) : (((KnobRuntime.check(java.util.UUID.fromString("7e2a6752-230c-31c0-982c-982373fffb17"))) ? ((isActive()) && (isActive())) : (((KnobRuntime.check(java.util.UUID.fromString("cf3cd6b7-999f-3eda-b0d3-7f1b9603b0da"))) ? (this.isAlive()) : (((KnobRuntime.check(java.util.UUID.fromString("1bceee32-c298-328a-925e-a9e1e1ec9bb6"))) ? ((this.isAlive()) || (this.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("d21e5a83-9ba7-3d98-a102-a40b4195228e"))) ? ((isActive()) && (this.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("555c7a80-e8a3-3ffb-9cfe-f371a3dcc1e8"))) ? ((isActive()) && (isFinished())) : (((KnobRuntime.check(java.util.UUID.fromString("47247875-2ab3-36b0-854f-97e9646d1444"))) ? ((this.isAlive()) && (isFinished())) : (((KnobRuntime.check(java.util.UUID.fromString("85e5baf4-8315-3c66-9f2e-81df5c8a197a"))) ? (isFinished()) : (((KnobRuntime.check(java.util.UUID.fromString("e52b0d65-8054-3ed4-ad96-97c68c980237"))) ? ((isFinished()) || (isActive())) : (((KnobRuntime.check(java.util.UUID.fromString("abb83b98-8f89-38b8-975e-47d82524aa37"))) ? ((isActive()) || (this.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("a7d0fb42-d268-3263-bd79-0641be4b5090"))) ? (isActive()) : (((KnobRuntime.check(java.util.UUID.fromString("cdcadb75-95b3-31e6-a82b-690ceb58f6fe"))) ? ((isActive()) || (isActive())) : (((KnobRuntime.check(java.util.UUID.fromString("bd3cfc05-7840-34d7-b9a3-44e654f2d985"))) ? ((this.isAlive()) && (isActive())) : (((KnobRuntime.check(java.util.UUID.fromString("a8861bd3-e0a4-3aeb-8998-db91a393acc3"))) ? ((this.isAlive()) && (this.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("b4a7e33c-70bf-3db4-a45b-ae981ea56c96"))) ? ((this.isAlive()) || (isActive())) : (((KnobRuntime.check(java.util.UUID.fromString("e969da7b-f0f7-3ec5-984c-92253ad536b0"))) ? ((isFinished()) || (this.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("7495eb48-ea7e-3cbb-94a6-8585a3d4efc8"))) ? ((this.isAlive()) || (isFinished())) : (((KnobRuntime.check(java.util.UUID.fromString("a2c3a38f-375b-3b35-8d99-4b948c90e5cd"))) ? ((isFinished()) || (isFinished())) : (((KnobRuntime.check(java.util.UUID.fromString("3c29202c-09aa-3ae6-bae9-c1d2121ffa71"))) ? ((isFinished()) && (this.entryReader.isAlive())) : (this.isAlive() || this.entryReader.isAlive()))))))))))))))))))))))))))))))))))))))))))))) {
      try {
        if (((KnobRuntime.check(java.util.UUID.fromString("daa9d114-476c-3726-b1d9-827652782696"))) ? ((System.nanoTime()) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("a1638593-8591-379a-abde-655e1fd55c5d"))) ? ((EnvironmentEdgeManager.currentTime()) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("ec97e7c7-e7b4-37e0-97df-4d21458aa702"))) ? ((EnvironmentEdgeManager.currentTime()) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("1743e46e-15be-3ec5-a486-20a31416a4c2"))) ? ((EnvironmentEdgeManager.currentTime()) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("8c06d321-e47b-34ea-8e5f-791b15651eb8"))) ? ((EnvironmentEdgeManager.currentTime()) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("4080ad13-f61f-318e-bc61-8a0d1180c508"))) ? ((EnvironmentEdgeManager.currentTime()) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("30872a00-eec6-3834-b014-4a4f6b5fc6a3"))) ? ((System.nanoTime()) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("a8a60887-9be6-30ad-9a97-b10038c54861"))) ? ((System.nanoTime()) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("dbbdd129-402f-3e72-a7c9-cf9832ba7c3a"))) ? ((System.nanoTime()) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("b1f111cc-e310-3a24-ba81-f31a6d1ce91c"))) ? ((EnvironmentEdgeManager.currentTime()) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("e52a0d98-4408-3a4f-b867-784d22124115"))) ? ((System.nanoTime()) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("ac11a422-e798-3824-81ed-695d10b0dde3"))) ? ((System.nanoTime()) >= (timeout)) : (EnvironmentEdgeManager.currentTime() >= timeout))))))))))))))))))))))))) {
          LOG.warn(
            "Shipper clearWALEntryBatch method timed out whilst waiting reader/shipper "
              + "thread to stop. Not cleaning buffer usage. Shipper alive: {}; Reader alive: {}",
            this.source.getPeerId(), this.isAlive(), this.entryReader.isAlive());
          return;
        } else {
          // Wait both shipper and reader threads to stop
if(KnobRuntime.check(java.util.UUID.fromString("eb0df612-da25-3dc5-b51d-7577bea490e5"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          Thread.sleep(this.sleepForRetries);
        }
      } catch (InterruptedException e) {
        LOG.warn("{} Interrupted while waiting {} to stop on clearWALEntryBatch. "
          + "Not cleaning buffer usage: {}", this.source.getPeerId(), this.getName(), e);
        return;
      }
    }
    long totalReleasedBytes = 0;
    while (true) {
      WALEntryBatch batch = entryReader.entryBatchQueue.poll();
      if (batch == null) {
        break;
      }
      totalReleasedBytes += source.getSourceManager().releaseWALEntryBatchBufferQuota(batch);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Decrementing totalBufferUsed by {}B while stopping Replication WAL Readers.",
        totalReleasedBytes);
    }
  }
}
