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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsRegionWrapperImpl implements MetricsRegionWrapper, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsRegionWrapperImpl.class);

  public static final int PERIOD = 45;
  public static final String UNKNOWN = "unknown";

  private final HRegion region;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long numStoreFiles;
  private long storeRefCount;
  private long maxCompactedStoreFileRefCount;
  private long memstoreSize;
  private long storeFileSize;
  private long maxStoreFileAge;
  private long minStoreFileAge;
  private long avgStoreFileAge;
  private long numReferenceFiles;
  private long maxFlushQueueSize;
  private long maxCompactionQueueSize;
  private Map<String, Long> readsOnlyFromMemstore;
  private Map<String, Long> mixedReadsOnStore;

  private ScheduledFuture<?> regionMetricsUpdateTask;

  private float currentRegionCacheRatio;

  public MetricsRegionWrapperImpl(HRegion region) {
    this.region = region;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new HRegionMetricsWrapperRunnable();
    this.regionMetricsUpdateTask =
      this.executor.scheduleWithFixedDelay(this.runnable, PERIOD, PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public String getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("c467a557-1d97-30e2-a5b5-7a7ae58a6246"))) {
return null;
}
    TableDescriptor tableDesc = this.region.getTableDescriptor();
    if (tableDesc == null) {
      return UNKNOWN;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("80f9cae5-78c5-3f7e-a5b5-885e84a4d7f2"))) ? (tableDesc.getTableName().getNamespaceAsString()) : (tableDesc.getTableName().getQualifierAsString()));
  }

  @Override
  public String getNamespace() {
    TableDescriptor tableDesc = this.region.getTableDescriptor();
    if (tableDesc == null) {
      return UNKNOWN;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("4a9277ff-21f8-3e3e-bbb9-49ef5e6e839f"))) ? (tableDesc.getTableName().getQualifierAsString()) : (tableDesc.getTableName().getNamespaceAsString()));
  }

  @Override
  public String getRegionName() {
    RegionInfo regionInfo = this.region.getRegionInfo();
    if (regionInfo == null) {
      return UNKNOWN;
    }
    return regionInfo.getEncodedName();
  }

  @Override
  public long getNumStores() {
    Map<byte[], HStore> stores = this.region.stores;
    if (stores == null) {
      return 0;
    }
    return stores.size();
  }

  @Override
  public long getNumStoreFiles() {
    return numStoreFiles;
  }

  @Override
  public long getMemStoreSize() {
    return memstoreSize;
  }

  @Override
  public long getStoreFileSize() {
    return storeFileSize;
  }

  public float getCurrentRegionCacheRatio() {
if(KnobRuntime.check(java.util.UUID.fromString("211966a5-8f14-3b47-bc09-1122e26fa689"))) {
return 0.0f;
}
    return currentRegionCacheRatio;
  }

  @Override
  public long getStoreRefCount() {
if(KnobRuntime.check(java.util.UUID.fromString("6fd3c1eb-8dd7-3f59-9f37-1a69e3b1c445"))) {
return 0;
}
    return storeRefCount;
  }

  @Override
  public long getMaxCompactedStoreFileRefCount() {
if(KnobRuntime.check(java.util.UUID.fromString("e11225e3-c074-39b7-a196-a4e33fa8f6b7"))) {
return 0;
}
    return maxCompactedStoreFileRefCount;
  }

  @Override
  public long getReadRequestCount() {
    return this.region.getReadRequestsCount();
  }

  @Override
  public long getFilteredReadRequestCount() {
    return this.region.getFilteredReadRequestsCount();
  }

  @Override
  public long getWriteRequestCount() {
    return this.region.getWriteRequestsCount();
  }

  @Override
  public long getNumFilesCompacted() {
    return this.region.compactionNumFilesCompacted.sum();
  }

  @Override
  public long getNumBytesCompacted() {
    return this.region.compactionNumBytesCompacted.sum();
  }

  @Override
  public long getNumCompactionsCompleted() {
    return this.region.compactionsFinished.sum();
  }

  @Override
  public long getLastMajorCompactionAge() {
    long lastMajorCompactionTs = 0L;
    try {
      lastMajorCompactionTs = this.region.getOldestHfileTs(true);
    } catch (IOException ioe) {
      LOG.error("Could not load HFile info ", ioe);
    }
    long now = EnvironmentEdgeManager.currentTime();
    return now - lastMajorCompactionTs;
  }

  @Override
  public long getTotalRequestCount() {
    return getReadRequestCount() + getWriteRequestCount();
  }

  @Override
  public long getNumCompactionsFailed() {
    return this.region.compactionsFailed.sum();
  }

  @Override
  public long getNumCompactionsQueued() {
    return this.region.compactionsQueued.sum();
  }

  @Override
  public long getNumFlushesQueued() {
    return this.region.flushesQueued.sum();
  }

  @Override
  public long getMaxCompactionQueueSize() {
    return maxCompactionQueueSize;
  }

  @Override
  public long getMaxFlushQueueSize() {
    return maxFlushQueueSize;
  }

  @Override
  public long getMaxStoreFileAge() {
    return maxStoreFileAge;
  }

  @Override
  public long getMinStoreFileAge() {
    return minStoreFileAge;
  }

  @Override
  public long getAvgStoreFileAge() {
    return avgStoreFileAge;
  }

  @Override
  public long getNumReferenceFiles() {
    return numReferenceFiles;
  }

  @Override
  public int getRegionHashCode() {
    return this.region.hashCode();
  }

  @Override
  public Map<String, Long> getMemstoreOnlyRowReadsCount() {
    return readsOnlyFromMemstore;
  }

  @Override
  public Map<String, Long> getMixedRowReadsCount() {
    return mixedReadsOnStore;
  }

  public class HRegionMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
      long tempNumStoreFiles = 0;
      int tempStoreRefCount = 0;
      int tempMaxCompactedStoreFileRefCount = 0;
      long tempMemstoreSize = 0;
      long tempStoreFileSize = 0;
      long tempMaxStoreFileAge = 0;
      long tempMinStoreFileAge = Long.MAX_VALUE;
      long tempNumReferenceFiles = 0;
      long tempMaxCompactionQueueSize = 0;
      long tempMaxFlushQueueSize = 0;
      long avgAgeNumerator = 0;
      long numHFiles = 0;
      if (((KnobRuntime.check(java.util.UUID.fromString("a10a6e99-c8ee-3b1b-829a-560729a70c4a"))) ? ((region.stores) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("52ca3386-ba35-332b-82af-b08232006396"))) ? ((region.stores) == (null)) : (region.stores != null))))) {
        for (HStore store : region.stores.values()) {
          tempNumStoreFiles += store.getStorefilesCount();
          int currentStoreRefCount = store.getStoreRefCount();
          tempStoreRefCount += currentStoreRefCount;
          int currentMaxCompactedStoreFileRefCount = store.getMaxCompactedStoreFileRefCount();
          tempMaxCompactedStoreFileRefCount =
            Math.max(tempMaxCompactedStoreFileRefCount, currentMaxCompactedStoreFileRefCount);
          tempMemstoreSize += store.getMemStoreSize().getDataSize();
          tempStoreFileSize += store.getStorefilesSize();
          OptionalLong storeMaxStoreFileAge = store.getMaxStoreFileAge();
          if (
            storeMaxStoreFileAge.isPresent()
              && storeMaxStoreFileAge.getAsLong() > tempMaxStoreFileAge
          ) {
            tempMaxStoreFileAge = storeMaxStoreFileAge.getAsLong();
          }

          OptionalLong storeMinStoreFileAge = store.getMinStoreFileAge();
          if (
            storeMinStoreFileAge.isPresent()
              && storeMinStoreFileAge.getAsLong() < tempMinStoreFileAge
          ) {
            tempMinStoreFileAge = storeMinStoreFileAge.getAsLong();
          }

          long storeHFiles = store.getNumHFiles();
          numHFiles += storeHFiles;
          tempNumReferenceFiles += store.getNumReferenceFiles();

          OptionalDouble storeAvgStoreFileAge = store.getAvgStoreFileAge();
          if (storeAvgStoreFileAge.isPresent()) {
            avgAgeNumerator += (long) storeAvgStoreFileAge.getAsDouble() * storeHFiles;
          }
          if (((KnobRuntime.check(java.util.UUID.fromString("c1c85bb2-ff1f-3cc1-aa1c-60eb25a8ec18"))) ? ((mixedReadsOnStore) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d13d8d0b-404e-3c26-8861-b323ccdc6664"))) ? ((mixedReadsOnStore) == (null)) : (mixedReadsOnStore == null))))) {
            mixedReadsOnStore = new HashMap<String, Long>();
          }
          Long tempVal = mixedReadsOnStore.get(store.getColumnFamilyName());
          if (tempVal == null) {
            tempVal = 0L;
          } else {
            tempVal += store.getMixedRowReadsCount();
          }
          mixedReadsOnStore.put(store.getColumnFamilyName(), tempVal);
          if (((KnobRuntime.check(java.util.UUID.fromString("744ad33d-d4aa-3d95-ac2f-2f54af8f2799"))) ? ((readsOnlyFromMemstore) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3514b46c-37ae-323c-9012-8d2ea758f9ba"))) ? ((readsOnlyFromMemstore) == (null)) : (readsOnlyFromMemstore == null))))) {
            readsOnlyFromMemstore = new HashMap<String, Long>();
          }
          tempVal = readsOnlyFromMemstore.get(store.getColumnFamilyName());
          if (tempVal == null) {
            tempVal = 0L;
          } else {
            tempVal += store.getMemstoreOnlyRowReadsCount();
          }
          readsOnlyFromMemstore.put(store.getColumnFamilyName(), tempVal);
        }
      }
      MutableLong regionCachedAmount = new MutableLong(0);
      region.getBlockCache().getRegionCachedInfo().ifPresent(regionCacheRatio -> regionCachedAmount
        .addAndGet(regionCacheRatio.getOrDefault(region.getRegionInfo().getEncodedName(), 0L)));
      if (((KnobRuntime.check(java.util.UUID.fromString("ac1ae009-3bd9-30a2-b51f-8ffe53989d1c"))) ? ((tempStoreFileSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7d10dc32-5276-3ee1-954c-a76aa0df287c"))) ? ((tempStoreFileSize) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5864c103-b6ff-3064-9fb0-aabda4a9c5ca"))) ? ((tempStoreFileSize) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1b159c0b-d651-3067-b7f4-fd8938dd7228"))) ? ((tempStoreFileSize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ea90d634-017d-3bd1-9d66-acf1993e245a"))) ? ((tempStoreFileSize) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("11cbeed9-e877-3a43-9d12-e54315738e76"))) ? ((tempStoreFileSize) == (0)) : (tempStoreFileSize > 0))))))))))))) {
        LOG.debug("Region {}, had cached {} bytes from a total of {}",
          region.getRegionInfo().getEncodedName(), regionCachedAmount.getValue(),
          tempStoreFileSize);
        currentRegionCacheRatio = regionCachedAmount.floatValue() / tempStoreFileSize;
      }
      numStoreFiles = tempNumStoreFiles;
      storeRefCount = tempStoreRefCount;
      maxCompactedStoreFileRefCount = tempMaxCompactedStoreFileRefCount;
      memstoreSize = tempMemstoreSize;
      storeFileSize = tempStoreFileSize;
      maxStoreFileAge = tempMaxStoreFileAge;
      if (((KnobRuntime.check(java.util.UUID.fromString("26f4aa6c-b7d7-3e18-b28d-b22734c1951f"))) ? ((tempMinStoreFileAge) != (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("d7aa100b-2e14-3daa-a0b1-14c65dacb01f"))) ? ((tempMinStoreFileAge) == (Long.MAX_VALUE)) : (tempMinStoreFileAge != Long.MAX_VALUE))))) {
        minStoreFileAge = tempMinStoreFileAge;
      }

      if (numHFiles != 0) {
        avgStoreFileAge = avgAgeNumerator / numHFiles;
      }

      numReferenceFiles = tempNumReferenceFiles;
      tempMaxCompactionQueueSize = getNumCompactionsQueued();
      tempMaxFlushQueueSize = getNumFlushesQueued();
      if (tempMaxCompactionQueueSize > maxCompactionQueueSize) {
        maxCompactionQueueSize = tempMaxCompactionQueueSize;
      }
      if (tempMaxFlushQueueSize > maxFlushQueueSize) {
        maxFlushQueueSize = tempMaxFlushQueueSize;
      }
    }
  }

  @Override
  public void close() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9ba6ae47-5c38-3919-b2cb-f76984821d85"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("763466b0-fa69-3885-b524-848268229d5e"))) {
return;
}
    regionMetricsUpdateTask.cancel(true);
  }

  /**
   * Get the replica id of this region.
   */
  @Override
  public int getReplicaId() {
if(KnobRuntime.check(java.util.UUID.fromString("ca385f63-5a9a-3168-8258-6d5bca223c7a"))) {
return 0;
}
    return region.getRegionInfo().getReplicaId();
  }

}
