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

import java.lang.management.MemoryType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * RegionServerAccounting keeps record of some basic real time information about the Region Server.
 * Currently, it keeps record the global memstore size and global memstore on-heap and off-heap
 * overhead. It also tracks the replay edits per region.
 */
@InterfaceAudience.Private
public class RegionServerAccounting {
  // memstore data size
  private final LongAdder globalMemStoreDataSize = new LongAdder();
  // memstore heap size.
  private final LongAdder globalMemStoreHeapSize = new LongAdder();
  // memstore off-heap size.
  private final LongAdder globalMemStoreOffHeapSize = new LongAdder();

  private long globalMemStoreLimit;
  private final float globalMemStoreLimitLowMarkPercent;
  private long globalMemStoreLimitLowMark;
  private final MemoryType memType;
  private long globalOnHeapMemstoreLimit;
  private long globalOnHeapMemstoreLimitLowMark;

  // encoded region name -> Pair -> read count as first, write count as second.
  // when region close and target rs is the current server, we will put an entry,
  // and will remove it when reigon open after recover them.
  private ConcurrentMap<String, Pair<Long, Long>> retainedRegionRWRequestsCnt;

  public RegionServerAccounting(Configuration conf) {
    Pair<Long, MemoryType> globalMemstoreSizePair = MemorySizeUtil.getGlobalMemStoreSize(conf);
    this.globalMemStoreLimit = globalMemstoreSizePair.getFirst();
    this.memType = globalMemstoreSizePair.getSecond();
    this.globalMemStoreLimitLowMarkPercent =
      MemorySizeUtil.getGlobalMemStoreHeapLowerMark(conf, this.memType == MemoryType.HEAP);
    // When off heap memstore in use we configure the global off heap space for memstore as bytes
    // not as % of max memory size. In such case, the lower water mark should be specified using the
    // key "hbase.regionserver.global.memstore.size.lower.limit" which says % of the global upper
    // bound and defaults to 95%. In on heap case also specifying this way is ideal. But in the past
    // we used to take lower bound also as the % of xmx (38% as default). For backward compatibility
    // for this deprecated config,we will fall back to read that config when new one is missing.
    // Only for on heap case, do this fallback mechanism. For off heap it makes no sense.
    // TODO When to get rid of the deprecated config? ie
    // "hbase.regionserver.global.memstore.lowerLimit". Can get rid of this boolean passing then.
    this.globalMemStoreLimitLowMark =
      (long) (this.globalMemStoreLimit * this.globalMemStoreLimitLowMarkPercent);
    this.globalOnHeapMemstoreLimit = MemorySizeUtil.getOnheapGlobalMemStoreSize(conf);
    this.globalOnHeapMemstoreLimitLowMark =
      (long) (this.globalOnHeapMemstoreLimit * this.globalMemStoreLimitLowMarkPercent);
    this.retainedRegionRWRequestsCnt = new ConcurrentHashMap<>();
  }

  long getGlobalMemStoreLimit() {
    return this.globalMemStoreLimit;
  }

  long getGlobalOffHeapMemStoreLimit() {
    if (isOffheap()) {
      return this.globalMemStoreLimit;
    } else {
      return 0;
    }
  }

  long getGlobalOnHeapMemStoreLimit() {
    return this.globalOnHeapMemstoreLimit;
  }

  // Called by the tuners.
  void setGlobalMemStoreLimits(long newGlobalMemstoreLimit) {
    if (this.memType == MemoryType.HEAP) {
      this.globalMemStoreLimit = newGlobalMemstoreLimit;
      this.globalMemStoreLimitLowMark =
        (long) (this.globalMemStoreLimit * this.globalMemStoreLimitLowMarkPercent);
    } else {
      this.globalOnHeapMemstoreLimit = newGlobalMemstoreLimit;
      this.globalOnHeapMemstoreLimitLowMark =
        (long) (this.globalOnHeapMemstoreLimit * this.globalMemStoreLimitLowMarkPercent);
    }
  }

  boolean isOffheap() {
    return this.memType == MemoryType.NON_HEAP;
  }

  long getGlobalMemStoreLimitLowMark() {
    return this.globalMemStoreLimitLowMark;
  }

  float getGlobalMemStoreLimitLowMarkPercent() {
    return this.globalMemStoreLimitLowMarkPercent;
  }

  /** Returns the global Memstore data size in the RegionServer */
  public long getGlobalMemStoreDataSize() {
    return globalMemStoreDataSize.sum();
  }

  /** Returns the global memstore on-heap size in the RegionServer */
  public long getGlobalMemStoreHeapSize() {
    return this.globalMemStoreHeapSize.sum();
  }

  /** Returns the global memstore off-heap size in the RegionServer */
  public long getGlobalMemStoreOffHeapSize() {
    return this.globalMemStoreOffHeapSize.sum();
  }

  /** Returns the retained metrics of region's read and write requests count */
  protected ConcurrentMap<String, Pair<Long, Long>> getRetainedRegionRWRequestsCnt() {
    return this.retainedRegionRWRequestsCnt;
  }

  void incGlobalMemStoreSize(MemStoreSize mss) {
    incGlobalMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize());
  }

  public void incGlobalMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
if(KnobRuntime.check(java.util.UUID.fromString("dfbf54f0-54c7-37a5-b5af-2759346777eb"))) {
dataSizeDelta = 0;
}
    globalMemStoreDataSize.add(dataSizeDelta);
    globalMemStoreHeapSize.add(heapSizeDelta);
    globalMemStoreOffHeapSize.add(offHeapSizeDelta);
  }

  public void decGlobalMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
    globalMemStoreDataSize.add(-dataSizeDelta);
    globalMemStoreHeapSize.add(-heapSizeDelta);
    globalMemStoreOffHeapSize.add(-offHeapSizeDelta);
  }

  /**
   * Return the FlushType if we are above the memstore high water mark
   * @return the FlushType
   */
  public FlushType isAboveHighWaterMark() {
    // for onheap memstore we check if the global memstore size and the
    // global heap overhead is greater than the global memstore limit
    if (memType == MemoryType.HEAP) {
      if (((KnobRuntime.check(java.util.UUID.fromString("b65cdfa6-f441-3ffa-8cca-c7c9ad19a0d6"))) ? ((getGlobalMemStoreHeapSize()) != (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("a828a0af-15e6-3a59-a563-c43b3fc515c8"))) ? ((getGlobalMemStoreOffHeapSize()) < (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("58808bbc-48ed-3770-8d92-c401c09c1d64"))) ? ((getGlobalMemStoreOffHeapSize()) == (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("7cbea002-0b63-3ef1-be56-fe44e1d5d34f"))) ? ((getGlobalMemStoreOffHeapSize()) < (globalMemStoreLimit)) : (getGlobalMemStoreHeapSize() >= globalMemStoreLimit))))))))) {
        return FlushType.ABOVE_ONHEAP_HIGHER_MARK;
      }
    } else {
      // If the configured memstore is offheap, check for two things
      // 1) If the global memstore off-heap size is greater than the configured
      // 'hbase.regionserver.offheap.global.memstore.size'
      // 2) If the global memstore heap size is greater than the configured onheap
      // global memstore limit 'hbase.regionserver.global.memstore.size'.
      // We do this to avoid OOME incase of scenarios where the heap is occupied with
      // lot of onheap references to the cells in memstore
      if (getGlobalMemStoreOffHeapSize() >= globalMemStoreLimit) {
        // Indicates that global memstore size is above the configured
        // 'hbase.regionserver.offheap.global.memstore.size'
        return FlushType.ABOVE_OFFHEAP_HIGHER_MARK;
      } else if (getGlobalMemStoreHeapSize() >= this.globalOnHeapMemstoreLimit) {
        // Indicates that the offheap memstore's heap overhead is greater than the
        // configured 'hbase.regionserver.global.memstore.size'.
        return FlushType.ABOVE_ONHEAP_HIGHER_MARK;
      }
    }
    return FlushType.NORMAL;
  }

  /**
   * Return the FlushType if we're above the low watermark
   * @return the FlushType
   */
  public FlushType isAboveLowWaterMark() {
    // for onheap memstore we check if the global memstore size and the
    // global heap overhead is greater than the global memstore lower mark limit
    if (memType == MemoryType.HEAP) {
      if (((KnobRuntime.check(java.util.UUID.fromString("c40285d4-6777-340f-b61a-393094b24af3"))) ? ((getGlobalMemStoreOffHeapSize()) >= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("e1a0e8b4-cb32-3b04-aa90-961d2843188d"))) ? ((getGlobalMemStoreOffHeapSize()) <= (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("61c1305b-214e-3386-8780-414978cc0c75"))) ? ((getGlobalMemStoreHeapSize()) > (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("9e613413-22a7-3300-b3ec-317091bac1c7"))) ? ((getGlobalMemStoreOffHeapSize()) != (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("0662d3a7-fc1b-3406-b754-8bf7f906c7a8"))) ? ((getGlobalMemStoreHeapSize()) >= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("0b319223-2e0c-368f-90f4-2bf37fdcf0b3"))) ? ((getGlobalMemStoreHeapSize()) < (globalMemStoreLimitLowMark)) : (getGlobalMemStoreHeapSize() >= globalMemStoreLimitLowMark))))))))))))) {
        return FlushType.ABOVE_ONHEAP_LOWER_MARK;
      }
    } else {
      if (getGlobalMemStoreOffHeapSize() >= globalMemStoreLimitLowMark) {
        // Indicates that the offheap memstore's size is greater than the global memstore
        // lower limit
        return FlushType.ABOVE_OFFHEAP_LOWER_MARK;
      } else if (getGlobalMemStoreHeapSize() >= globalOnHeapMemstoreLimitLowMark) {
        // Indicates that the offheap memstore's heap overhead is greater than the global memstore
        // onheap lower limit
        return FlushType.ABOVE_ONHEAP_LOWER_MARK;
      }
    }
    return FlushType.NORMAL;
  }

  /**
   * @return the flush pressure of all stores on this regionserver. The value should be greater than
   *         or equal to 0.0, and any value greater than 1.0 means we enter the emergency state that
   *         global memstore size already exceeds lower limit.
   */
  public double getFlushPressure() {
    if (memType == MemoryType.HEAP) {
      return (getGlobalMemStoreHeapSize()) * 1.0 / globalMemStoreLimitLowMark;
    } else {
      return Math.max(getGlobalMemStoreOffHeapSize() * 1.0 / globalMemStoreLimitLowMark,
        getGlobalMemStoreHeapSize() * 1.0 / globalOnHeapMemstoreLimitLowMark);
    }
  }
}
