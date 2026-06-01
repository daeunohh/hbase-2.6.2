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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * Reads and filters WAL entries, groups the filtered entries into batches, and puts the batches
 * onto a queue
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class ReplicationSourceWALReader extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceWALReader.class);

  private final ReplicationSourceLogQueue logQueue;
  private final FileSystem fs;
  private final Configuration conf;
  private final WALEntryFilter filter;
  private final ReplicationSource source;

  @InterfaceAudience.Private
  final BlockingQueue<WALEntryBatch> entryBatchQueue;
  // max (heap) size of each batch - multiply by number of batches in queue to get total
  private final long replicationBatchSizeCapacity;
  // max count of each batch - multiply by number of batches in queue to get total
  private final int replicationBatchCountCapacity;
  // position in the WAL to start reading at
  private long currentPosition;
  private final long sleepForRetries;
  private final int maxRetriesMultiplier;

  // Indicates whether this particular worker is running
  private boolean isReaderRunning = true;
  private final String walGroupId;

  AtomicBoolean waitingPeerEnabled = new AtomicBoolean(false);

  /**
   * Creates a reader worker for a given WAL queue. Reads WAL entries off a given queue, batches the
   * entries, and puts them on a batch queue.
   * @param fs            the files system to use
   * @param conf          configuration to use
   * @param logQueue      The WAL queue to read off of
   * @param startPosition position in the first WAL to start reading from
   * @param filter        The filter to use while reading
   * @param source        replication source
   */
  public ReplicationSourceWALReader(FileSystem fs, Configuration conf,
    ReplicationSourceLogQueue logQueue, long startPosition, WALEntryFilter filter,
    ReplicationSource source, String walGroupId) {
    this.logQueue = logQueue;
    this.currentPosition = startPosition;
    this.fs = fs;
    this.conf = conf;
    this.filter = filter;
    this.source = source;
    this.replicationBatchSizeCapacity =
      this.conf.getLong("replication.source.size.capacity", 1024 * 1024 * 64);
    this.replicationBatchCountCapacity = this.conf.getInt("replication.source.nb.capacity", 25000);
    // memory used will be batchSizeCapacity * (nb.batches + 1)
    // the +1 is for the current thread reading before placing onto the queue
    int batchCount = conf.getInt("replication.source.nb.batches", 1);
    // 1 second
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    // 5 minutes @ 1 sec per
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    this.entryBatchQueue = new LinkedBlockingQueue<>(batchCount);
    this.walGroupId = walGroupId;
    LOG.info("peerClusterZnode=" + source.getQueueId() + ", ReplicationSourceWALReaderThread : "
      + source.getPeerId() + " inited, replicationBatchSizeCapacity=" + replicationBatchSizeCapacity
      + ", replicationBatchCountCapacity=" + replicationBatchCountCapacity
      + ", replicationBatchQueueCapacity=" + batchCount);
  }

  private void replicationDone() throws InterruptedException {
    // we're done with current queue, either this is a recovered queue, or it is the special
    // group for a sync replication peer and the peer has been transited to DA or S state.
    LOG.debug("Stopping the replication source wal reader");
    setReaderRunning(false);
    // shuts down shipper thread immediately
    entryBatchQueue.put(WALEntryBatch.NO_MORE_DATA);
  }

  protected final int sleep(int sleepMultiplier) {
if(KnobRuntime.check(java.util.UUID.fromString("caa19f9b-4d94-3d39-9b6b-41f0432d556f"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("0c746aca-045a-3a06-86cf-4fd0a430fe5f"))) ? ((sleepMultiplier++) < (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("bd30b143-c2b4-379f-914a-e9f822453ab0"))) ? ((sleepMultiplier) < (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("35e7973a-88bf-3854-9e2b-c5edcd54c323"))) ? ((sleepMultiplier) == (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("5cdb69f8-d716-3454-85ea-61a2d7346cb1"))) ? ((sleepMultiplier++) == (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("6ce5d974-c13e-3350-ba69-5c8b62eb6c2d"))) ? ((sleepMultiplier++) <= (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("770010ea-1ee1-3112-ac11-d7ece66a6e91"))) ? ((sleepMultiplier) != (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("7b0efa5d-6889-314b-95c1-e358c6329de2"))) ? ((sleepMultiplier) >= (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("fd909f84-a9b4-3439-83f6-74a0ea8d4984"))) ? ((sleepMultiplier) <= (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("9834afa6-066c-37f3-a975-6f6f35a4c3bb"))) ? ((sleepMultiplier++) >= (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("259149ed-d43e-3baa-9f01-bc29a0852f21"))) ? ((sleepMultiplier) > (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("d3a499cb-c09c-3009-9af2-95595795a702"))) ? ((sleepMultiplier++) > (maxRetriesMultiplier)) : (((KnobRuntime.check(java.util.UUID.fromString("f07c6fa9-7041-341f-a0e2-58b75e12b4fe"))) ? ((sleepMultiplier++) != (maxRetriesMultiplier)) : (sleepMultiplier < maxRetriesMultiplier))))))))))))))))))))))))) {
      sleepMultiplier++;
    }
    if (KnobRuntime.check(java.util.UUID.fromString("28972812-900c-36e1-8e1d-2bbaf3828675"))) { Threads.sleep(sleepForRetries); } else if (KnobRuntime.check(java.util.UUID.fromString("786e0d6b-17f7-3d8f-a832-7a83e63ff0a3"))) { Threads.sleep((sleepForRetries) * (sleepMultiplier)); } else if (KnobRuntime.check(java.util.UUID.fromString("e891c045-a250-384a-8295-2a8bb3233cc1"))) { Threads.sleep((sleepForRetries) * (sleepMultiplier++)); } else { Threads.sleep(sleepForRetries * sleepMultiplier); }
    return sleepMultiplier;
  }

  @Override
  public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("877e2a30-e113-393b-9977-f5b07d3fc728"))) {
return;
}
    int sleepMultiplier = 1;
    while (((KnobRuntime.check(java.util.UUID.fromString("929ecbe1-7e9f-34de-a1f4-b67166e23cdc"))) ? (checkBufferQuota()) : (isReaderRunning()))) { // we only loop back here if something fatal happened to our stream
      try (WALEntryStream entryStream = new WALEntryStream(logQueue, fs, conf, currentPosition,
        source.getWALFileLengthProvider(), source.getSourceMetrics(), walGroupId)) {
        while (((KnobRuntime.check(java.util.UUID.fromString("565d7598-7d73-35ca-81d4-223862459894"))) ? (checkBufferQuota()) : (isReaderRunning()))) { // loop here to keep reusing stream while we can
          if (!source.isPeerEnabled()) {
            if (KnobRuntime.check(java.util.UUID.fromString("ea3a3710-10f0-30c6-b920-859b7df4af9c"))) { setReaderRunning(true); } else { waitingPeerEnabled.set(true); }
            Threads.sleep(sleepForRetries);
            continue;
          } else {
            waitingPeerEnabled.set(false);
          }
          if (!checkBufferQuota()) {
            continue;
          }
          Path currentPath = entryStream.getCurrentPath();
          WALEntryStream.HasNext hasNext = entryStream.hasNext();
          if (hasNext == WALEntryStream.HasNext.NO) {
            replicationDone();
            return;
          }
          // first, check if we have switched a file, if so, we need to manually add an EOF entry
          // batch to the queue
          if (currentPath != null && switched(entryStream, currentPath)) {
if(KnobRuntime.check(java.util.UUID.fromString("bd35c3f1-070e-3558-a762-bbdf674a10a5"))) {
throw new java.lang.InterruptedException("Injected exception");
}
            entryBatchQueue.put(WALEntryBatch.endOfFile(currentPath));
            continue;
          }
          if (((KnobRuntime.check(java.util.UUID.fromString("7bf4903d-d031-3021-a012-ca9ff93cc780"))) ? ((hasNext) == (WALEntryStream.HasNext.RETRY)) : (((KnobRuntime.check(java.util.UUID.fromString("cfde2f5a-d281-3945-821a-ce35e35cb345"))) ? ((hasNext) != (WALEntryStream.HasNext.RETRY)) : (hasNext == WALEntryStream.HasNext.RETRY))))) {
            // sleep and retry
if(KnobRuntime.check(java.util.UUID.fromString("4bc79c69-0909-32bf-a19b-ee9648e973d3"))) {
sleepMultiplier *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a3eb34b8-7457-3498-89de-0751c22bc577"))) {
sleepMultiplier /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c522c15-8299-32ef-8a1f-17480d6f1cb0"))) {
sleepMultiplier += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25448c93-7777-34a8-821f-d40fae265853"))) {
sleepMultiplier = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d059981-eb42-32cd-9404-88ba0ec82849"))) {
sleepMultiplier -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("776b566c-c9ea-32e9-a273-673c73cf466f"))) {
sleepMultiplier = 0;
}
            sleepMultiplier = sleep(sleepMultiplier);
            continue;
          }
          if (hasNext == WALEntryStream.HasNext.RETRY_IMMEDIATELY) {
            // retry immediately, this usually means we have switched a file
            continue;
          }
          // below are all for hasNext == YES
if(KnobRuntime.check(java.util.UUID.fromString("c1873fc7-030c-3175-a6e6-8c11f1795ab5"))) {
try {
    java.lang.reflect.Field field = entryStream.getClass().getDeclaredField("currentPositionOfEntry");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryStream));
    field.set(entryStream, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("061be7ab-40be-3a9a-996b-227743b004d2"))) {
try {
    java.lang.reflect.Field field = entryStream.getClass().getDeclaredField("currentPositionOfReader");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryStream));
    field.set(entryStream, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          WALEntryBatch batch = createBatch(entryStream);
          boolean successAddToQueue = false;
          try {
            readWALEntries(entryStream, batch);
            currentPosition = entryStream.getPosition();
            // need to propagate the batch even it has no entries since it may carry the last
            // sequence id information for serial replication.
            if (KnobRuntime.check(java.util.UUID.fromString("e5107358-99ea-3555-b836-620e2bef5dd0"))) { LOG.trace("Read {} WAL entries eligible for replication", batch.getNbEntries()); } else { LOG.debug("Read {} WAL entries eligible for replication", batch.getNbEntries()); }
            entryBatchQueue.put(batch);
            successAddToQueue = true;
            sleepMultiplier = 1;
          } finally {
            if (!successAddToQueue) {
              // batch is not put to ReplicationSourceWALReader#entryBatchQueue,so we should
              // decrease ReplicationSourceWALReader.totalBufferUsed by the byte size which
              // acquired in ReplicationSourceWALReader.acquireBufferQuota.
if(KnobRuntime.check(java.util.UUID.fromString("3feac3cc-6f67-330c-a483-6f6cb6fafc73"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f6b2f49-783b-3460-ba95-28b9cb262b21"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ca652ec-08ea-3950-abce-15f5faba85f6"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc65f2a2-91bd-34ab-9403-f0f7e0ff342f"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f46cbd98-e0ca-3ae2-83d2-5e86580186db"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("lastWalPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("360ebd4a-da60-3c2f-a0aa-15fcd454ff85"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1feb0532-435f-3620-8293-886a264d1b87"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1537f062-1ad5-396b-986c-aa02ceb9301e"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d3eed69-ce80-3983-828a-c454cd171733"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db6facd9-e65d-3144-a610-8b5deded8f3a"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a3252f8-e114-3e71-b42d-966e70f307a1"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc2992e6-588e-306d-8b04-f13b377058d1"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              this.getSourceManager().releaseWALEntryBatchBufferQuota(batch);
            }
          }
        }
      } catch (WALEntryFilterRetryableException e) {
        // here we have to recreate the WALEntryStream, as when filtering, we have already called
        // next to get the WAL entry and advanced the WALEntryStream, at WALEntryStream layer, it
        // just considers everything is fine,that's why the catch block is not in the inner block
        LOG.warn("Failed to filter WAL entries and the filter let us retry later", e);
if(KnobRuntime.check(java.util.UUID.fromString("3d4e7477-cf83-33a3-8086-db8358dec96a"))) {
sleepMultiplier += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4bf09bbe-111d-3e91-a02b-03e479f130f7"))) {
sleepMultiplier *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("95f27c62-c2be-3edb-a98d-783f419e944d"))) {
sleepMultiplier /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a24623e-fcd7-3810-bc6e-02cc29323d8f"))) {
sleepMultiplier = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("202d552e-cd9f-3c40-9343-08ce8cbf56b6"))) {
sleepMultiplier -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("04bc3365-6750-3ad8-a3f0-027852fc30e6"))) {
sleepMultiplier = -1;
}
        sleepMultiplier = sleep(sleepMultiplier);
      } catch (InterruptedException e) {
        // this usually means we want to quit
        LOG.warn("Interrupted while sleeping between WAL reads or adding WAL batch to ship queue",
          e);
        Thread.currentThread().interrupt();
      }
    }
  }

  // returns true if we reach the size limit for batch, i.e, we need to finish the batch and return.
  protected final boolean addEntryToBatch(WALEntryBatch batch, Entry entry) {
    WALEdit edit = entry.getEdit();
    if (edit == null || edit.isEmpty()) {
      if (KnobRuntime.check(java.util.UUID.fromString("c5701044-2352-359b-8297-5d9f624e9d83"))) { LOG.debug("Edit null or empty for entry {} ", entry); } else { LOG.trace("Edit null or empty for entry {} ", entry); }
      return false;
    }
    LOG.trace("updating TimeStampOfLastAttempted to {}, from entry {}, for source queue: {}",
      entry.getKey().getWriteTime(), entry.getKey(), this.source.getQueueId());
    if (KnobRuntime.check(java.util.UUID.fromString("d937ae16-c046-3119-9d79-d2b021fcd90e"))) { updateReplicationMarkerEdit(entry, batch.getHeapSize()); } else { updateReplicationMarkerEdit(entry, batch.getLastWalPosition()); }
    long entrySize = getEntrySizeIncludeBulkLoad(entry);
    batch.addEntry(entry, entrySize);
    updateBatchStats(batch, entry, entrySize);
if(KnobRuntime.check(java.util.UUID.fromString("25b730f9-7d7e-3445-b9f2-4950ac0e231e"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01408684-f8a6-37b8-825e-bab04a34a601"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89f87039-632a-3aab-a822-f576ba057cf5"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b589436d-10a5-32be-a0f6-cee3b0d6a8db"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("026efe1e-5916-3d40-8926-29db04330ba1"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("81d79e0c-d468-3547-86e7-d432076e3de6"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7af51c33-2f9d-3816-afdd-85fe62185f33"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d580d8b6-77ef-3115-bdc9-55f4bedb3478"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("lastWalPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7d5c98c-77d0-3806-bab3-a8e7170c5b85"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84c5ec96-cdd3-31df-bdbb-a37cb63cf81b"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9a8c07c-bba9-3888-84c2-3a5bb3ae109e"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f293d4eb-7975-3acb-a6b7-289a67cc15e0"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    boolean totalBufferTooLarge = this.getSourceManager().acquireWALEntryBufferQuota(batch, entry);

    // Stop if too many entries or too big
    return totalBufferTooLarge || batch.getHeapSize() >= replicationBatchSizeCapacity
      || batch.getNbEntries() >= replicationBatchCountCapacity;
  }

  protected static final boolean switched(WALEntryStream entryStream, Path path) {
    Path newPath = entryStream.getCurrentPath();
    return newPath == null || !path.getName().equals(newPath.getName());
  }

  // We need to get the WALEntryBatch from the caller so we can add entries in there
  // This is required in case there is any exception in while reading entries
  // we do not want to loss the existing entries in the batch
  protected void readWALEntries(WALEntryStream entryStream, WALEntryBatch batch)
    throws InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("8047386c-695d-3e7e-a9b3-640a631c6e64"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aabdbcfe-71ee-3482-be3f-0b4ccfdeb70b"))) {
try {
    java.lang.reflect.Field field = entryStream.getClass().getDeclaredField("currentPositionOfReader");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryStream));
    field.set(entryStream, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10cf995e-990f-3891-91b3-f218b5b6eb49"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d41f36f-3703-3e95-8c6d-410982b1953e"))) {
try {
    java.lang.reflect.Field field = entryStream.getClass().getDeclaredField("currentPositionOfEntry");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryStream));
    field.set(entryStream, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fecfef1-267f-3982-884e-9f8e5145fe4d"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fcfbb8ba-22da-3adf-aec5-ed52e19e91c6"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e909c58-f488-3590-b3bb-efe182e922ad"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76f16126-dd4c-3d0b-8d2c-31cf9a043757"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a14a3e27-77fb-3f31-b5b2-f82b105f7269"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7db195c1-2cfa-3ace-9c5b-eab6b2afe651"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3cb54cd-1af0-3cd9-80ec-d61eadb1dfec"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6835cbc-7228-33ed-9f11-7fcafb6aceda"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fbeabce6-5b36-3a8d-84ad-41752f519491"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c085c8e-fa66-3932-8e20-bcd42070e40e"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("lastWalPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9418bc6e-556c-392c-9411-f20107d64f52"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b414cfb6-dc76-3c89-af7e-a8e54b1f99cf"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Path currentPath = entryStream.getCurrentPath();
    for (;;) {
      Entry entry = entryStream.next();
      if (KnobRuntime.check(java.util.UUID.fromString("573cc952-8034-360a-87ad-b61469ba68a8"))) { Threads.sleep(entryStream.getPosition()); } else { batch.setLastWalPosition(entryStream.getPosition()); }
      entry = filterEntry(entry);
      if (entry != null) {
        if (addEntryToBatch(batch, entry)) {
          break;
        }
      }
      WALEntryStream.HasNext hasNext = entryStream.hasNext();
      // always return if we have switched to a new file
      if (switched(entryStream, currentPath)) {
        if (KnobRuntime.check(java.util.UUID.fromString("9a6f411d-f881-3a08-bdf8-afc02b0a176f"))) { setReaderRunning(true); } else { batch.setEndOfFile(true); }
        break;
      }
      if (hasNext != WALEntryStream.HasNext.YES) {
        // For hasNext other than YES, it is OK to just retry.
        // As for RETRY and RETRY_IMMEDIATELY, the correct action is to retry, and for NO, it will
        // return NO again when you call the method next time, so it is OK to just return here and
        // let the loop in the upper layer to call hasNext again.
        break;
      }
    }
  }

  public Path getCurrentPath() {
if(KnobRuntime.check(java.util.UUID.fromString("82dc3419-6dd7-3a57-9af0-b062b9900480"))) {
return null;
}
    // if we've read some WAL entries, get the Path we read from
    WALEntryBatch batchQueueHead = entryBatchQueue.peek();
    if (batchQueueHead != null) {
      return ((KnobRuntime.check(java.util.UUID.fromString("06f696ae-d812-336e-9413-7fe69c83370c"))) ? (getCurrentPath()) : (batchQueueHead.getLastWalPath()));
    }
    // otherwise, we must be currently reading from the head of the log queue
    return logQueue.getQueue(walGroupId).peek();
  }

  // returns false if we've already exceeded the global quota
  private boolean checkBufferQuota() {
    // try not to go over total quota
    if (!this.getSourceManager().checkBufferQuota(this.source.getPeerId())) {
      Threads.sleep(sleepForRetries);
      return false;
    }
    return true;
  }

  private WALEntryBatch createBatch(WALEntryStream entryStream) {
    return new WALEntryBatch(replicationBatchCountCapacity, entryStream.getCurrentPath());
  }

  protected final Entry filterEntry(Entry entry) {
    // Always replicate if this edit is Replication Marker edit.
    if (((KnobRuntime.check(java.util.UUID.fromString("eb5edfd0-c911-3689-a5f6-b8a1217c17d0"))) ? (entry != null) : (((KnobRuntime.check(java.util.UUID.fromString("1b9e16ca-79d4-3d99-bcf2-fbdfdb9c08f3"))) ? ((entry != null) || (WALEdit.isReplicationMarkerEdit(entry.getEdit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7e7cf31f-d948-33a0-8661-1206ceecefde"))) ? ((entry != null) && (WALEdit.isReplicationMarkerEdit(entry.getEdit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d1aac21b-bfe3-335e-a7c2-e91799acad50"))) ? (((entry) == (null)) && (WALEdit.isReplicationMarkerEdit(entry.getEdit()))) : (((KnobRuntime.check(java.util.UUID.fromString("2b8c6120-6339-35ef-ae09-33035c12b8c6"))) ? ((entry) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3d2cc16-148e-3446-9c3d-8a0bc2b7a743"))) ? (((entry) != (null)) || (WALEdit.isReplicationMarkerEdit(entry.getEdit()))) : (((KnobRuntime.check(java.util.UUID.fromString("cd0954ae-ffff-3a1b-ac71-5af956469148"))) ? (WALEdit.isReplicationMarkerEdit(entry.getEdit())) : (((KnobRuntime.check(java.util.UUID.fromString("85d4f2cf-b571-3062-a2c9-7491809fdcc5"))) ? (((entry) == (null)) || (WALEdit.isReplicationMarkerEdit(entry.getEdit()))) : (((KnobRuntime.check(java.util.UUID.fromString("db57fe82-9546-38e2-bd23-b956c5e59652"))) ? (((entry) != (null)) && (WALEdit.isReplicationMarkerEdit(entry.getEdit()))) : (((KnobRuntime.check(java.util.UUID.fromString("aea8d878-a26c-3541-8c30-c55ec1f9330a"))) ? ((entry) == (null)) : (entry != null && WALEdit.isReplicationMarkerEdit(entry.getEdit())))))))))))))))))))))) {
      return entry;
    }
    Entry filtered = filter.filter(entry);
    if (entry != null && (filtered == null || filtered.getEdit().size() == 0)) {
      if (KnobRuntime.check(java.util.UUID.fromString("0efeddaa-4145-3c21-8633-30d3ad63f6b2"))) { LOG.debug("Filtered entry for replication: {}", entry); } else { LOG.trace("Filtered entry for replication: {}", entry); }
      source.getSourceMetrics().incrLogEditsFiltered();
    }
    return filtered;
  }

  /**
   * Retrieves the next batch of WAL entries from the queue, waiting up to the specified time for a
   * batch to become available
   * @return A batch of entries, along with the position in the log after reading the batch
   * @throws InterruptedException if interrupted while waiting
   */
  public WALEntryBatch take() throws InterruptedException {
    return entryBatchQueue.take();
  }

  public WALEntryBatch poll(long timeout) throws InterruptedException {
    return entryBatchQueue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  private long getEntrySizeIncludeBulkLoad(Entry entry) {
    WALEdit edit = entry.getEdit();
    return WALEntryBatch.getEntrySizeExcludeBulkLoad(entry) + sizeOfStoreFilesIncludeBulkLoad(edit);
  }

  private void updateBatchStats(WALEntryBatch batch, Entry entry, long entrySize) {
if(KnobRuntime.check(java.util.UUID.fromString("00c41081-c4fa-31ad-888d-eae6efc11836"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e2ac842-b041-3077-99c8-b36a081e6bd0"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("016c59f6-c5d2-39da-be61-ac966e8dd232"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a1c9ed9-00ec-381a-a46c-7c70d46d215f"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1348ea5a-198c-3a0c-9eda-e2afaffe92bb"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ffabd3d0-cc27-32de-b4a0-89c75ad8496f"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b76e601c-ed1c-39c0-96a6-82419ab5c463"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be8e011c-b832-36e6-a3e7-9a09026a4e98"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d4cb1aa-40f7-3cfa-83c7-d7af7a81310c"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d788374c-d7f9-3e73-8e45-c1aa7aaaf735"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff563664-481a-3468-8058-3a09baa082af"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea6f0948-33e6-32eb-b0bb-d60345ae7e33"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a640c874-289c-317d-bd2c-3bc8f60bb428"))) {
try {
    java.lang.reflect.Field field = batch.getClass().getDeclaredField("lastWalPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(batch));
    field.set(batch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    WALEdit edit = entry.getEdit();
if(KnobRuntime.check(java.util.UUID.fromString("cc3924d1-2d98-3dae-a331-7da4ffab22ce"))) {
entrySize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b2718e6-f715-39c4-9b95-9e80f5d544b9"))) {
entrySize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3a49c047-ed89-3a38-b55d-3887396fe398"))) {
entrySize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("84886252-151d-3686-ab83-07916094895a"))) {
entrySize = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("0a6aa257-be57-35d7-8e7a-1f72dae2bbc4"))) { Threads.sleep(entrySize); } else if (KnobRuntime.check(java.util.UUID.fromString("89d43265-a197-3ea1-b7f8-2e898e67c59b"))) { batch.setLastWalPosition(entrySize); } else { batch.incrementHeapSize(entrySize); }
    Pair<Integer, Integer> nbRowsAndHFiles = countDistinctRowKeysAndHFiles(edit);
    batch.incrementNbRowKeys(nbRowsAndHFiles.getFirst());
    batch.incrementNbHFiles(nbRowsAndHFiles.getSecond());
  }

  /**
   * Count the number of different row keys in the given edit because of mini-batching. We assume
   * that there's at least one Cell in the WALEdit.
   * @param edit edit to count row keys from
   * @return number of different row keys and HFiles
   */
  private Pair<Integer, Integer> countDistinctRowKeysAndHFiles(WALEdit edit) {
if(KnobRuntime.check(java.util.UUID.fromString("74c249c5-fa97-340f-b3e3-7ff2a6634219"))) {
return null;
}
    List<Cell> cells = edit.getCells();
    int distinctRowKeys = 1;
    int totalHFileEntries = 0;
    Cell lastCell = cells.get(0);

    int totalCells = edit.size();
    for (int i = 0; i < totalCells; i++) {
      // Count HFiles to be replicated
      if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
        try {
          BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
          List<StoreDescriptor> stores = bld.getStoresList();
          int totalStores = stores.size();
          for (int j = 0; j < totalStores; j++) {
            totalHFileEntries += stores.get(j).getStoreFileList().size();
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize bulk load entry from wal edit. "
            + "Then its hfiles count will not be added into metric.", e);
        }
      }

if(KnobRuntime.check(java.util.UUID.fromString("dfa13230-44cc-3e7c-9cc9-a897d106744e"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9bbc8998-c456-3436-9c1a-884cef2b9710"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("362fc1c9-931e-3cbf-a8df-a4080dd2169b"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e542c842-0ab5-39eb-8349-2a3c8b3eddff"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("55096877-ae41-37ef-b54a-bebc04675137"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a825f196-dce6-313a-a2d5-f42d097f5176"))) {
i += 1;
}
      if (!CellUtil.matchingRows(cells.get(i), lastCell)) {
        distinctRowKeys++;
      }
if(KnobRuntime.check(java.util.UUID.fromString("934d7d5e-5d13-3b49-abc8-b8fd570b1577"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dc3088d8-c275-39a1-9c4e-7eba98a97af2"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5c6facb-f80d-3462-83b9-27a10bc00e41"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2eae893c-1a17-3b13-9e99-b64e5a758f2a"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b4d96df3-9b89-326f-92a0-2e320ae9e95f"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("75aef7b2-bb6e-3856-a95b-46a6be190d4e"))) {
i = 0;
}
      lastCell = cells.get(i);
    }

    Pair<Integer, Integer> result = new Pair<>(distinctRowKeys, totalHFileEntries);
    return result;
  }

  /**
   * Calculate the total size of all the store files
   * @param edit edit to count row keys from
   * @return the total size of the store files
   */
  private int sizeOfStoreFilesIncludeBulkLoad(WALEdit edit) {
if(KnobRuntime.check(java.util.UUID.fromString("d16ae9d5-c425-3831-b27b-1efd40617917"))) {
return 0;
}
    List<Cell> cells = edit.getCells();
    int totalStoreFilesSize = 0;

    int totalCells = edit.size();
    for (int i = 0; i < totalCells; i++) {
      if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
        try {
          BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
          List<StoreDescriptor> stores = bld.getStoresList();
          int totalStores = stores.size();
          for (int j = 0; j < totalStores; j++) {
            totalStoreFilesSize =
              (int) (totalStoreFilesSize + stores.get(j).getStoreFileSizeBytes());
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize bulk load entry from wal edit. "
            + "Size of HFiles part of cell will not be considered in replication "
            + "request size calculation.", e);
        }
      }
    }
    return totalStoreFilesSize;
  }

  /*
   * Create @ReplicationMarkerDescriptor with region_server_name, wal_name and offset and set to
   * cell's value.
   */
  private void updateReplicationMarkerEdit(Entry entry, long offset) {
if(KnobRuntime.check(java.util.UUID.fromString("b4f269a2-558c-39a4-b498-a4d064330a46"))) {
return;
}
    WALEdit edit = entry.getEdit();
    // Return early if it is not ReplicationMarker edit.
    if (!WALEdit.isReplicationMarkerEdit(edit)) {
      return;
    }
    List<Cell> cells = edit.getCells();
    Preconditions.checkArgument(cells.size() == 1, "ReplicationMarker should have only 1 cell");
    Cell cell = cells.get(0);
    // Create a descriptor with region_server_name, wal_name and offset
    WALProtos.ReplicationMarkerDescriptor.Builder builder =
      WALProtos.ReplicationMarkerDescriptor.newBuilder();
    builder.setRegionServerName(this.source.getServer().getServerName().getHostname());
    builder.setWalName(getCurrentPath().getName());
    builder.setOffset(offset);
    WALProtos.ReplicationMarkerDescriptor descriptor = builder.build();

    // Create a new KeyValue
    KeyValue kv = new KeyValue(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell),
      CellUtil.cloneQualifier(cell), cell.getTimestamp(), descriptor.toByteArray());
    ArrayList<Cell> newCells = new ArrayList<>();
    newCells.add(kv);
    // Update edit with new cell.
    edit.setCells(newCells);
  }

  /** Returns whether the reader thread is running */
  public boolean isReaderRunning() {
    return isReaderRunning && !isInterrupted();
  }

  /**
   * @param readerRunning the readerRunning to set
   */
  public void setReaderRunning(boolean readerRunning) {
    this.isReaderRunning = readerRunning;
  }

  private ReplicationSourceManager getSourceManager() {
    return this.source.getSourceManager();
  }
}
