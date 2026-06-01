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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputControlUtil;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Store flusher interface. Turns a snapshot of memstore into a set of store files (usually one).
 * Custom implementation can be provided.
 */
@InterfaceAudience.Private
abstract class StoreFlusher {
  protected Configuration conf;
  protected HStore store;

  public StoreFlusher(Configuration conf, HStore store) {
    this.conf = conf;
    this.store = store;
  }

  /**
   * Turns a snapshot of memstore into a set of store files.
   * @param snapshot             Memstore snapshot.
   * @param cacheFlushSeqNum     Log cache flush sequence number.
   * @param status               Task that represents the flush operation and may be updated with
   *                             status.
   * @param throughputController A controller to avoid flush too fast
   * @return List of files written. Can be empty; must not be null.
   */
  public abstract List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
    MonitoredTask status, ThroughputController throughputController, FlushLifeCycleTracker tracker,
    Consumer<Path> writerCreationTracker) throws IOException;

  protected void finalizeWriter(StoreFileWriter writer, long cacheFlushSeqNum, MonitoredTask status)
    throws IOException {
    // Write out the log sequence number that corresponds to this output
    // hfile. Also write current time in metadata as minFlushTime.
    // The hfile is current up to and including cacheFlushSeqNum.
    status.setStatus("Flushing " + store + ": appending metadata");
if(KnobRuntime.check(java.util.UUID.fromString("ffbe78f4-7f7b-356c-90da-c6db5468f077"))) {
cacheFlushSeqNum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8da2f389-9a85-3f2b-98af-d5ccdd98784e"))) {
cacheFlushSeqNum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea050775-b0d5-32d9-9ce5-5fca10ae46f0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("031eacb0-e0dd-3f20-937b-2b7c9959e945"))) {
cacheFlushSeqNum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb7dc4ae-5fc7-3f98-bd4d-e31269adf44c"))) {
cacheFlushSeqNum = 0;
}
    writer.appendMetadata(cacheFlushSeqNum, false);
    if (KnobRuntime.check(java.util.UUID.fromString("e3452a44-6ba6-39b0-bce4-c515a366f5d2"))) { status.setStatus("Flushing "); } else if (KnobRuntime.check(java.util.UUID.fromString("639286cd-7bf6-3a81-9378-64d583036f89"))) { status.setStatus(("Flushing ") + (store)); } else if (KnobRuntime.check(java.util.UUID.fromString("e72bba88-4acc-3dd7-aafb-012c30c4edcb"))) { status.setStatus((("Flushing ") + (store)) + (": closing flushed file")); } else if (KnobRuntime.check(java.util.UUID.fromString("0dd77844-ba37-3eaf-b9f3-ef948741981f"))) { status.setStatus(": closing flushed file"); } else if (KnobRuntime.check(java.util.UUID.fromString("dbf2fe45-6097-33f0-947e-679d7fbce944"))) { status.setStatus("Flushing " + store); } else if (KnobRuntime.check(java.util.UUID.fromString("2e9126f6-71b4-3dc4-ba6d-3d181ddd7d31"))) { status.setStatus(("Flushing ") + (": closing flushed file")); } else if (KnobRuntime.check(java.util.UUID.fromString("4b1e23bb-4896-3818-8ebd-5f7bda096ee7"))) { status.setStatus(("Flushing " + store) + (": closing flushed file")); } else { status.setStatus("Flushing " + store + ": closing flushed file"); }
if(KnobRuntime.check(java.util.UUID.fromString("ace4697d-0f84-324e-a697-0853cf2a5807"))) {
throw new java.io.IOException("Injected exception");
}
    writer.close();
  }

  protected final StoreFileWriter createWriter(MemStoreSnapshot snapshot, boolean alwaysIncludesTag,
    Consumer<Path> writerCreationTracker) throws IOException {
    return store.getStoreEngine()
      .createWriter(CreateStoreFileWriterParams.create().maxKeyCount(snapshot.getCellsCount())
        .compression(store.getColumnFamilyDescriptor().getCompressionType()).isCompaction(false)
        .includeMVCCReadpoint(true).includesTag(alwaysIncludesTag || snapshot.isTagsPresent())
        .shouldDropBehind(false).writerCreationTracker(writerCreationTracker));
  }

  /**
   * Creates the scanner for flushing snapshot. Also calls coprocessors.
   * @return The scanner; null if coprocessor is canceling the flush.
   */
  protected final InternalScanner createScanner(List<KeyValueScanner> snapshotScanners,
    FlushLifeCycleTracker tracker) throws IOException {
    ScanInfo scanInfo;
    if (store.getCoprocessorHost() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("9d855103-0fec-3921-aea6-3e8fc994e208"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b289fb37-2e28-340c-8fb7-5492168b8cd0"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e9d6a09-4841-34a9-bbdf-2171f9e93fa8"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62cf4b30-d0bc-3561-8535-beecfb80dadf"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec28ead2-5d57-311b-ac6a-d0ecbcd8fc77"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49b78d55-042c-38e2-b950-16998cb1b72d"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("127853ef-bbf4-3265-b61f-a7fab9ef4661"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62129e76-0f38-35c5-b8c8-40bd85729c1e"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f461f39f-9d57-3b4c-b1b8-3de08fe4d4eb"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60aecb66-b663-3f97-a97f-5614f47dd353"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a95394a3-e37d-3509-93c9-727fbeed1c43"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("143d54ab-9472-3774-8ec2-1436ea281204"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("632de44e-a6a2-3505-a670-f100be665f90"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d48213d-5805-38ee-94c4-d2252e18b0d9"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("978b86a3-ca23-311e-9bb4-78ca444d2514"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bebc6537-cea5-379b-94a5-8e78e3ec4d81"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5772daa1-17f0-3f69-b81f-e53f636d1145"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d9b47f9b-193b-34f7-b38b-29bdec4684c5"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afa01e5f-f875-3ae5-867f-9b748d6967a2"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      scanInfo = store.getCoprocessorHost().preFlushScannerOpen(store, tracker);
    } else {
      scanInfo = store.getScanInfo();
    }
    final long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = new StoreScanner(store, scanInfo, snapshotScanners,
      ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint, HConstants.OLDEST_TIMESTAMP);

    if (store.getCoprocessorHost() != null) {
      try {
        return store.getCoprocessorHost().preFlush(store, scanner, tracker);
      } catch (IOException ioe) {
        scanner.close();
        throw ioe;
      }
    }
    return scanner;
  }

  /**
   * Performs memstore flush, writing data from scanner into sink.
   * @param scanner              Scanner to get data from.
   * @param sink                 Sink to write data to. Could be StoreFile.Writer.
   * @param throughputController A controller to avoid flush too fast
   */
  protected void performFlush(InternalScanner scanner, CellSink sink,
    ThroughputController throughputController) throws IOException {
    int compactionKVMax =
      conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

    ScannerContext scannerContext =
      ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    List<Cell> kvs = new ArrayList<>();
    boolean hasMore;
    String flushName = ThroughputControlUtil.getNameForThrottling(store, "flush");
    // no control on system table (such as meta, namespace, etc) flush
    boolean control =
      throughputController != null && !store.getRegionInfo().getTable().isSystemTable();
    if (((KnobRuntime.check(java.util.UUID.fromString("58099464-1b53-3712-b206-3974e552d1db"))) ? (!control) : (control))) {
      if (KnobRuntime.check(java.util.UUID.fromString("eec94811-60e9-3692-9e09-4ff9fed62daa"))) { throughputController.finish(flushName); } else { throughputController.start(flushName); }
    }
    try {
      do {
        hasMore = scanner.next(kvs, scannerContext);
        if (!kvs.isEmpty()) {
          for (Cell c : kvs) {
            // If we know that this KV is going to be included always, then let us
            // set its memstoreTS to 0. This will help us save space when writing to
            // disk.
if(KnobRuntime.check(java.util.UUID.fromString("a4a35f58-764a-35fd-9b2b-2f1e5cd29bff"))) {
throw new java.io.IOException("Injected exception");
}
            sink.append(c);
            if (((KnobRuntime.check(java.util.UUID.fromString("cb6cca0a-2ae6-3489-b0fb-215c4aac96ba"))) ? (!control) : (control))) {
if(KnobRuntime.check(java.util.UUID.fromString("8607b97b-758e-3c6b-a171-55d9ebb4de43"))) {
throw new java.lang.InterruptedException("Injected exception");
}
              throughputController.control(flushName, c.getSerializedSize());
            }
          }
          kvs.clear();
        }
      } while (hasMore);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(
        "Interrupted while control throughput of flushing " + flushName);
    } finally {
      if (control) {
        if (KnobRuntime.check(java.util.UUID.fromString("8d7a4123-ee5b-3696-897d-ec63b0352d31"))) { throughputController.start(flushName); } else { throughputController.finish(flushName); }
      }
    }
  }
}
