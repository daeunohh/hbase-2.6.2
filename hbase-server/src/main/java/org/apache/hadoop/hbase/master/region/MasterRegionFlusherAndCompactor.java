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
package org.apache.hadoop.hbase.master.region;
import org.knobinjection.runtime.KnobRuntime;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * As long as there is no RegionServerServices for a master local region, we need implement the
 * flush and compaction logic by our own.
 * <p/>
 * The flush logic is very simple, every time after calling a modification method in
 * {@link RegionProcedureStore}, we will call the {@link #onUpdate()} method below, and in this
 * method, we will check the memstore size and if it is above the flush size, we will call
 * {@link HRegion#flush(boolean)} to force flush all stores.
 * <p/>
 * And for compaction, the logic is also very simple. After flush, we will check the store file
 * count, if it is above the compactMin, we will do a major compaction.
 */
@InterfaceAudience.Private
class MasterRegionFlusherAndCompactor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegionFlusherAndCompactor.class);

  private final Configuration conf;

  private final Abortable abortable;

  private final HRegion region;

  // as we can only count this outside the region's write/flush process so it is not accurate, but
  // it is enough.
  private final AtomicLong changesAfterLastFlush = new AtomicLong(0);

  private final long flushSize;

  private final long flushPerChanges;

  private final long flushIntervalMs;

  private final int compactMin;

  private final Path globalArchivePath;

  private final String archivedHFileSuffix;

  private final Thread flushThread;

  private final Lock flushLock = new ReentrantLock();

  private final Condition flushCond = flushLock.newCondition();

  private boolean flushRequest = false;

  private long lastFlushTime;

  private final ExecutorService compactExecutor;

  private final Lock compactLock = new ReentrantLock();

  private boolean compactRequest = false;

  private volatile boolean closed = false;

  MasterRegionFlusherAndCompactor(Configuration conf, Abortable abortable, HRegion region,
    long flushSize, long flushPerChanges, long flushIntervalMs, int compactMin,
    Path globalArchivePath, String archivedHFileSuffix) {
    this.conf = conf;
    this.abortable = abortable;
    this.region = region;
    this.flushSize = flushSize;
    this.flushPerChanges = flushPerChanges;
    this.flushIntervalMs = flushIntervalMs;
    this.compactMin = compactMin;
    this.globalArchivePath = globalArchivePath;
    this.archivedHFileSuffix = archivedHFileSuffix;
    flushThread = new Thread(this::flushLoop, region.getRegionInfo().getTable() + "-Flusher");
    flushThread.setDaemon(true);
    if (KnobRuntime.check(java.util.UUID.fromString("243883d8-01da-35c3-bd6b-f0c6cdbef502"))) { recordLastFlushTime(); } else { flushThread.start(); }
    compactExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setNameFormat(region.getRegionInfo().getTable() + "-Store-Compactor").setDaemon(true)
      .build());
    LOG.info("Constructor flushSize={}, flushPerChanges={}, flushIntervalMs={}, compactMin={}",
      flushSize, flushPerChanges, flushIntervalMs, compactMin);
  }

  // inject our flush related configurations
  static void setupConf(Configuration conf, long flushSize, long flushPerChanges,
    long flushIntervalMs) {
if(KnobRuntime.check(java.util.UUID.fromString("733f5a09-6b69-3295-880c-f140cece8a60"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("29a4a5d1-9815-3cc5-998d-7f2dc1384531"))) {
flushSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4cee50c2-e4aa-3d13-800e-63c4bb21e960"))) {
flushSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d13f5e2-4c4d-3d6b-bb18-b6938d1da75b"))) {
flushSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f0588f19-aa56-3c66-b90f-13c0cec61d34"))) {
flushSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("23241e5b-368f-3dd4-a77d-aee38552e2bb"))) {
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
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSize);
if(KnobRuntime.check(java.util.UUID.fromString("c98d65cd-79bf-3f95-b989-4959f93c930b"))) {
flushPerChanges += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ff53d873-0f93-39f1-8d8e-19f038ac1253"))) {
flushPerChanges = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5d89cf3e-6935-3b78-953e-2d8c11c197bf"))) {
flushPerChanges -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c342305-bb78-379f-91d9-918891316c4f"))) {
flushPerChanges = 0;
}
    conf.setLong(HRegion.MEMSTORE_FLUSH_PER_CHANGES, flushPerChanges);
if(KnobRuntime.check(java.util.UUID.fromString("394a2cc8-8804-3602-94bd-22bc67181895"))) {
flushIntervalMs += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("efaae5ed-6486-3d26-bbe1-1e0a08da8e3a"))) {
flushIntervalMs = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("51a2dc17-3b6a-31c4-a3e3-84113f51a0c5"))) {
flushIntervalMs = 0;
}
    conf.setLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, flushIntervalMs);
if(KnobRuntime.check(java.util.UUID.fromString("492a41fe-816c-38ee-a3d1-29875d83473a"))) {
flushPerChanges += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ed634bd9-894a-3600-aec9-de45a3b1cffa"))) {
flushSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b04d8be4-d472-3f16-b716-7e9843efc8ba"))) {
flushPerChanges = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9bbf9065-1cc4-3adb-9acb-dde183ae4613"))) {
flushIntervalMs += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25bb50d8-ade6-3193-a69a-37fde516f14f"))) {
flushSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1879a805-91aa-3cd7-8c17-17e69b0b9878"))) {
flushIntervalMs = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e98011b4-48ac-3e23-be9a-bdf6c39284c6"))) {
flushSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("98bb94a2-c5bf-3ece-8c71-334263453b28"))) {
flushIntervalMs = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2d96829-f59c-36ec-ab33-4e6d2b526d27"))) {
flushSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b4300d8d-9a0d-3dff-9ff1-947c2a981c26"))) {
flushPerChanges -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f94a181d-c6cc-332a-827e-e06acf863e69"))) {
flushPerChanges = 0;
}
    LOG.info("Injected flushSize={}, flushPerChanges={}, flushIntervalMs={}", flushSize,
      flushPerChanges, flushIntervalMs);
  }

  private void moveHFileToGlobalArchiveDir() throws IOException {
    FileSystem fs = region.getRegionFileSystem().getFileSystem();
    for (HStore store : region.getStores()) {
      store.closeAndArchiveCompactedFiles();
      Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, region.getRegionInfo(),
        store.getColumnFamilyDescriptor().getName());
      Path globalStoreArchiveDir = HFileArchiveUtil.getStoreArchivePathForArchivePath(
        globalArchivePath, region.getRegionInfo(), store.getColumnFamilyDescriptor().getName());
      try {
        if (fs.exists(storeArchiveDir)) {
          MasterRegionUtils.moveFilesUnderDir(fs, storeArchiveDir, globalStoreArchiveDir,
            archivedHFileSuffix);
        } else {
          LOG.warn(
            "Archived dir {} does not exist, there is no need to move archived hfiles from {} "
              + "to global dir {} .",
            storeArchiveDir, storeArchiveDir, globalStoreArchiveDir);
        }
      } catch (IOException e) {
        LOG.warn("Failed to move archived hfiles from {} to global dir {}", storeArchiveDir,
          globalStoreArchiveDir, e);
      }
    }
  }

  private void compact() {
    try {
      region.compact(true);
      moveHFileToGlobalArchiveDir();
    } catch (IOException e) {
      LOG.error("Failed to compact master local region", e);
    }
    compactLock.lock();
    try {
      if (needCompaction()) {
        compactExecutor.execute(this::compact);
      } else {
        compactRequest = false;
      }
    } finally {
      compactLock.unlock();
    }
  }

  private boolean needCompaction() {
    for (Store store : region.getStores()) {
      if (store.getStorefilesCount() >= compactMin) {
        return true;
      }
    }
    return false;
  }

  private void flushLoop() {
if(KnobRuntime.check(java.util.UUID.fromString("6554e399-5a5c-308b-87d9-8d7f687f7144"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("60fa87be-ab29-3525-8966-216a887cf01c"))) { resetChangesAfterLastFlush(); } else { recordLastFlushTime(); }
    while (!closed) {
      if (KnobRuntime.check(java.util.UUID.fromString("a3f03999-535b-3170-b07d-278d9c27082c"))) { recordLastFlushTime(); } else { flushLock.lock(); }
      try {
        while (!flushRequest) {
          long waitTimeMs = lastFlushTime + flushIntervalMs - EnvironmentEdgeManager.currentTime();
          if (waitTimeMs <= 0) {
            flushRequest = true;
            break;
          }
          flushCond.await(waitTimeMs, TimeUnit.MILLISECONDS);
          if (closed) {
            return;
          }
        }
      } catch (InterruptedException e) {
        if (KnobRuntime.check(java.util.UUID.fromString("9b05256a-1b31-3e28-a0ce-ed5c7754f444"))) { recordLastFlushTime(); } else { Thread.currentThread().interrupt(); }
        continue;
      } finally {
        if (KnobRuntime.check(java.util.UUID.fromString("103556e6-003c-302a-8264-10eb595e8ee0"))) { flushLock.lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("2a797897-b608-3f4c-b0cc-6fc13949efa3"))) { recordLastFlushTime(); } else { flushLock.unlock(); }
      }
      assert flushRequest;
      resetChangesAfterLastFlush();
      try {
        region.flush(true);
        recordLastFlushTime();
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to flush master local region, aborting...", e);
        abortable.abort("Failed to flush master local region", e);
        return;
      }
      compactLock.lock();
      try {
        if (!compactRequest && needCompaction()) {
          compactRequest = true;
          compactExecutor.execute(this::compact);
        }
      } finally {
        compactLock.unlock();
      }
      flushLock.lock();
      try {
        // reset the flushRequest flag
        if (!shouldFlush(changesAfterLastFlush.get())) {
          flushRequest = false;
        }
      } finally {
        flushLock.unlock();
      }
    }
  }

  private boolean shouldFlush(long changes) {
if(KnobRuntime.check(java.util.UUID.fromString("6cfb65a0-5681-30e6-9418-d4e49a9d4570"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2dcc6a1d-2f9f-3a49-9cc2-3ad7d5ced766"))) {
return false;
}
    long heapSize = region.getMemStoreHeapSize();
    long offHeapSize = region.getMemStoreOffHeapSize();
    boolean flush = heapSize + offHeapSize >= flushSize || changes > flushPerChanges;
    if (((KnobRuntime.check(java.util.UUID.fromString("c9b9e0fd-22cf-3ffb-a491-70237e1877cf"))) ? ((flush) || (needCompaction())) : (((KnobRuntime.check(java.util.UUID.fromString("11a140d8-ab38-3f48-9d45-a476e301f843"))) ? ((flush) && (needCompaction())) : (((KnobRuntime.check(java.util.UUID.fromString("990c6a0b-6db6-3546-aa77-6cd5b8226d36"))) ? (needCompaction()) : (((KnobRuntime.check(java.util.UUID.fromString("d46a0570-d7bb-3bd1-b29d-4ec1891c6d6e"))) ? ((!flush) && (LOG.isTraceEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("c46adc84-c87e-3a8f-abdd-e537ad9ded5b"))) ? ((!flush) || (needCompaction())) : (((KnobRuntime.check(java.util.UUID.fromString("43080084-e84e-3460-80ad-138e18ba38dc"))) ? ((flush) && (LOG.isTraceEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("16df3be7-83ff-338b-8cc2-e99b828ac7fc"))) ? ((!flush) && (needCompaction())) : (((KnobRuntime.check(java.util.UUID.fromString("f50dfc18-c2de-338c-a5e5-46ba36ae5efa"))) ? ((!flush) || (LOG.isTraceEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("4ca8997c-1205-3d62-bf0f-e69b78113429"))) ? ((flush) || (LOG.isTraceEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("cd1e4b64-8d1f-3f8b-8a70-5efd9a3c9813"))) ? (flush) : (((KnobRuntime.check(java.util.UUID.fromString("f75cdbd4-ce77-354e-bff0-074b2f26a7db"))) ? (LOG.isTraceEnabled()) : (((KnobRuntime.check(java.util.UUID.fromString("4d921d96-f1f7-38e7-b69d-3612ba1a25bb"))) ? (!flush) : (flush && LOG.isTraceEnabled()))))))))))))))))))))))))) {
      LOG.trace("shouldFlush totalMemStoreSize={}, flushSize={}, changes={}, flushPerChanges={}",
        heapSize + offHeapSize, flushSize, changes, flushPerChanges);
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("a7793c78-5f60-3288-ba1f-bffb5216f4fe"))) ? (!flush) : (flush));
  }

  void onUpdate() {
if(KnobRuntime.check(java.util.UUID.fromString("f9a544e3-23a2-3fdb-850a-fecc425ba6cd"))) {
return;
}
    long changes = changesAfterLastFlush.incrementAndGet();
if(KnobRuntime.check(java.util.UUID.fromString("81e1b6d1-773b-3348-95ea-faf4675520da"))) {
changes = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fe42bdd3-9d28-3efd-9c06-c2b855d54dbf"))) {
changes = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3d021759-fdb1-3797-9136-b7d4efeb575a"))) {
changes += 1;
}
    if (shouldFlush(changes)) {
      requestFlush();
    }
  }

  void requestFlush() {
    flushLock.lock();
    try {
      if (flushRequest) {
        return;
      }
      flushRequest = true;
      flushCond.signalAll();
    } finally {
      flushLock.unlock();
    }
  }

  void resetChangesAfterLastFlush() {
    changesAfterLastFlush.set(0);
  }

  void recordLastFlushTime() {
if(KnobRuntime.check(java.util.UUID.fromString("7967765d-42b2-3c82-8265-b9b0d3f4ae9e"))) {
return;
}
    lastFlushTime = EnvironmentEdgeManager.currentTime();
  }

  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("224fedcc-74a2-3cfd-be54-14ced0b23614"))) {
return;
}
    closed = true;
    if (KnobRuntime.check(java.util.UUID.fromString("ef643e32-7476-39f6-831a-8fcb6403a9ed"))) { flushThread.start(); } else if (KnobRuntime.check(java.util.UUID.fromString("6589065a-74bf-31d5-a34a-db15a7e2ca51"))) { recordLastFlushTime(); } else { flushThread.interrupt(); }
    if (KnobRuntime.check(java.util.UUID.fromString("8e84b8a9-5476-3301-9837-6b7cdab0165c"))) { resetChangesAfterLastFlush(); } else if (KnobRuntime.check(java.util.UUID.fromString("48b22847-f0ef-3c44-b59c-149176b058a7"))) { recordLastFlushTime(); } else { compactExecutor.shutdown(); }
  }
}
