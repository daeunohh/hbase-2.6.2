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
if(KnobRuntime.check(java.util.UUID.fromString("580e2e79-5e6f-3a85-9d67-f25c39a822d6"))) {
return 0;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("134286ed-116b-3a1f-a1de-53803ce75fc7"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cbc7bec-b317-3665-96ff-f4b4d3054327"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("791304e8-0535-3b5c-be2c-b12761345d79"))) ? ((this.memType) != (MemoryType.NON_HEAP)) : (((KnobRuntime.check(java.util.UUID.fromString("4510311a-abac-31d4-9da9-ec2abd08a871"))) ? ((this.memType) == (MemoryType.NON_HEAP)) : (this.memType == MemoryType.NON_HEAP))));
  }

  long getGlobalMemStoreLimitLowMark() {
if(KnobRuntime.check(java.util.UUID.fromString("5b924c62-f5cd-3938-922e-94e74498c0fe"))) {
return 0;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("fda068a3-b0d3-361c-908c-71dc797e65c9"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("c343aaa0-87d0-34bb-b7f6-5ef71eb3fa87"))) ? (getGlobalMemStoreHeapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("441aaec5-b3b0-39dd-b651-d6aca843b616"))) ? (getGlobalMemStoreOffHeapSize()) : (this.globalMemStoreHeapSize.sum()))));
  }

  /** Returns the global memstore off-heap size in the RegionServer */
  public long getGlobalMemStoreOffHeapSize() {
    return this.globalMemStoreOffHeapSize.sum();
  }

  /** Returns the retained metrics of region's read and write requests count */
  protected ConcurrentMap<String, Pair<Long, Long>> getRetainedRegionRWRequestsCnt() {
if(KnobRuntime.check(java.util.UUID.fromString("da36e155-8d60-34c9-968f-a9175fe8171c"))) {
return null;
}
    return this.retainedRegionRWRequestsCnt;
  }

  void incGlobalMemStoreSize(MemStoreSize mss) {
    incGlobalMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize());
  }

  public void incGlobalMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
if(KnobRuntime.check(java.util.UUID.fromString("add214d3-65ae-3b08-aac1-1010bd9fa9fc"))) {
dataSizeDelta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("52b6ba95-0e19-3c66-adfa-476bf27d1922"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("dfbf54f0-54c7-37a5-b5af-2759346777eb"))) {
dataSizeDelta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("90eb789c-4089-39da-8c9d-b6b82cde4b91"))) {
dataSizeDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("99e73794-94fb-3189-87f0-841087bc2a5e"))) {
dataSizeDelta = -1;
}
    globalMemStoreDataSize.add(dataSizeDelta);
if(KnobRuntime.check(java.util.UUID.fromString("bfd3825e-e0c5-3249-9534-413c801d4d46"))) {
heapSizeDelta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a8f53ae-93ba-3b31-a69e-3ff98db5fa0a"))) {
heapSizeDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fdb00f95-e1e0-3ecf-bb9b-abb5006020d1"))) {
heapSizeDelta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("affab6b5-4771-364b-aec3-ab474cecd933"))) {
heapSizeDelta -= 1;
}
    globalMemStoreHeapSize.add(heapSizeDelta);
if(KnobRuntime.check(java.util.UUID.fromString("ad1c8e2d-fbc4-3bc7-85ec-9742cec4f430"))) {
offHeapSizeDelta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab44621b-db7c-3d5b-981a-601d689ded3a"))) {
offHeapSizeDelta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("44b52be9-d37f-31ee-9ff7-adbd9df698ab"))) {
offHeapSizeDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fceab6f9-1c64-370a-9815-c368bb0da8ee"))) {
offHeapSizeDelta = -1;
}
    globalMemStoreOffHeapSize.add(offHeapSizeDelta);
  }

  public void decGlobalMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
if(KnobRuntime.check(java.util.UUID.fromString("8ccbd299-e842-33f8-a1fe-384e47e436f6"))) {
return;
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("2f5b8e39-d71c-30a1-95d8-1bd7fdb45d1e"))) ? ((memType) == (MemoryType.HEAP)) : (((KnobRuntime.check(java.util.UUID.fromString("382a8b94-b05a-3054-b806-25522db59517"))) ? ((memType) != (MemoryType.HEAP)) : (memType == MemoryType.HEAP))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("051d3662-dbbb-3dc7-a31d-4b8897916c9f"))) ? ((getGlobalMemStoreOffHeapSize()) <= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("74f0e4e7-19b6-377e-989a-7c1bf33d275c"))) ? ((getGlobalMemStoreOffHeapSize()) != (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("40aad306-9021-38b0-9370-8b4b2afc4cb2"))) ? ((getGlobalMemStoreHeapSize()) < (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("6051ab37-0478-34d8-84b1-ab201b21a542"))) ? ((getGlobalMemStoreHeapSize()) == (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("c76acd32-8876-3264-bf5a-5714e5b6c2f3"))) ? ((getGlobalMemStoreHeapSize()) >= (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("6aad9d45-aca3-3409-9cf6-167e46110937"))) ? ((getGlobalMemStoreHeapSize()) != (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("2915ccd6-a6c4-3b5b-bdac-7b32ec46f52d"))) ? ((getGlobalMemStoreOffHeapSize()) != (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("06840a57-10cb-3c02-b163-77af86849dcd"))) ? ((getGlobalMemStoreHeapSize()) == (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("82746fd7-9601-34bb-b465-62f43f92b7fa"))) ? ((getGlobalMemStoreHeapSize()) < (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("7e156eac-dfcd-30c4-9f5f-2f50e53c77a9"))) ? ((getGlobalMemStoreOffHeapSize()) >= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("fb5d4f6c-b750-35ce-b85e-595c097c1e91"))) ? ((getGlobalMemStoreOffHeapSize()) <= (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("920b0449-8229-3cf7-86f0-71fae58f1019"))) ? ((getGlobalMemStoreHeapSize()) >= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("12532f82-c2c5-37f8-81bc-6c353b35a3e6"))) ? ((getGlobalMemStoreHeapSize()) <= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("83e452bc-d538-3d6c-97bd-07084384aeac"))) ? ((getGlobalMemStoreHeapSize()) > (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("b1cac98e-f54d-3a5b-ac19-c4590fb21e61"))) ? ((getGlobalMemStoreHeapSize()) > (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("58808bbc-48ed-3770-8d92-c401c09c1d64"))) ? ((getGlobalMemStoreOffHeapSize()) == (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("9c164b15-7e48-3f9d-aaa3-91a1ef534ecf"))) ? ((getGlobalMemStoreOffHeapSize()) >= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("369193a0-6acc-3bde-b84d-fe67bdda7e5f"))) ? ((getGlobalMemStoreOffHeapSize()) >= (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("7d29bf20-ce4a-320e-bc67-f4bc511432bb"))) ? ((getGlobalMemStoreOffHeapSize()) > (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("caf0be1e-4d5a-39c0-8fa8-44b4dfe03425"))) ? ((getGlobalMemStoreOffHeapSize()) > (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("b65cdfa6-f441-3ffa-8cca-c7c9ad19a0d6"))) ? ((getGlobalMemStoreHeapSize()) != (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("d1d67f5f-d046-335e-9825-d94f8ae357ce"))) ? ((getGlobalMemStoreOffHeapSize()) != (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("52e42fa0-afed-3160-b10d-8103a49f3ef2"))) ? ((getGlobalMemStoreHeapSize()) >= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("4e136dd6-3da6-3fe2-b771-2c98f2a96f1c"))) ? ((getGlobalMemStoreOffHeapSize()) < (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("fb87b3e7-1fd8-31ce-9f82-eddb78c63f1d"))) ? ((getGlobalMemStoreHeapSize()) != (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("a828a0af-15e6-3a59-a563-c43b3fc515c8"))) ? ((getGlobalMemStoreOffHeapSize()) < (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("dbc7b32f-fbe1-37cb-a574-a41d14592050"))) ? ((getGlobalMemStoreHeapSize()) > (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("0400766a-a992-39fa-8832-786a459eb7e6"))) ? ((getGlobalMemStoreOffHeapSize()) <= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("7d9d49ed-a3a0-3338-b23d-ef25f26c452b"))) ? ((getGlobalMemStoreOffHeapSize()) > (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("95d711e1-5de6-3f6a-9537-24bd375e58f9"))) ? ((getGlobalMemStoreHeapSize()) <= (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("727ab1c2-dcc5-3714-b06c-9dd8e282d7eb"))) ? ((getGlobalMemStoreHeapSize()) <= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("fae27cf1-dc00-3a8d-83e1-2aa63131533b"))) ? ((getGlobalMemStoreHeapSize()) < (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("7cbea002-0b63-3ef1-be56-fe44e1d5d34f"))) ? ((getGlobalMemStoreOffHeapSize()) < (globalMemStoreLimit)) : (((KnobRuntime.check(java.util.UUID.fromString("b06b3a32-fcc5-3139-ad7c-bb65d788ab4f"))) ? ((getGlobalMemStoreOffHeapSize()) == (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("32a6ee2b-79f6-3e5f-8b5d-44c7cd62e3d2"))) ? ((getGlobalMemStoreOffHeapSize()) == (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("588f3838-bd4a-321a-93b2-f6c96496e43e"))) ? ((getGlobalMemStoreHeapSize()) == (getGlobalMemStoreOffHeapSize())) : (getGlobalMemStoreHeapSize() >= globalMemStoreLimit))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("c525c7fb-be2b-347f-a163-5b97b3d0a803"))) ? ((memType) == (MemoryType.HEAP)) : (((KnobRuntime.check(java.util.UUID.fromString("c52a1e58-b73c-3965-bcb8-bd30f8b793fe"))) ? ((memType) != (MemoryType.HEAP)) : (memType == MemoryType.HEAP))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("1abd51b7-36c9-3d77-8209-3a6fb0341721"))) ? ((getGlobalMemStoreHeapSize()) != (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("316ffbbc-db4b-341c-af50-4444fd7cb20f"))) ? ((getGlobalMemStoreHeapSize()) > (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("3c42b6c4-5408-35a8-a7d1-63db494ae86d"))) ? ((getGlobalMemStoreOffHeapSize()) > (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("fbe66765-42de-3839-8ba0-fea7aadf93f8"))) ? ((getGlobalMemStoreOffHeapSize()) < (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("c709348e-8ce8-38cc-bbd3-8bf8bc94e79c"))) ? ((getGlobalMemStoreOffHeapSize()) < (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("4b9c6560-5705-3adb-b01e-e7424bfd66e4"))) ? ((getGlobalMemStoreOffHeapSize()) != (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("ff4dbec1-846d-3323-89cf-00cd51b0aa4e"))) ? ((getGlobalMemStoreOffHeapSize()) > (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("9f8649cb-5131-3aff-abe1-80828bb8413c"))) ? ((getGlobalMemStoreOffHeapSize()) == (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("07d912c7-2d05-3a93-8ffc-b19b3aefec14"))) ? ((getGlobalMemStoreHeapSize()) == (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("51307b6a-9de6-3f2d-9111-e6a36208f9e7"))) ? ((getGlobalMemStoreHeapSize()) < (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("cbc2aab0-b7c9-3354-9479-7eb7ea0ac1d7"))) ? ((getGlobalMemStoreOffHeapSize()) > (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("38d5530e-bf25-3e05-97a9-d45ba0866337"))) ? ((getGlobalMemStoreHeapSize()) >= (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("97352719-fef7-3d08-aab3-1ef81342cddb"))) ? ((getGlobalMemStoreOffHeapSize()) <= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("fffa05b5-7ad9-31bd-963f-bb4fc212cfd3"))) ? ((getGlobalMemStoreHeapSize()) != (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("0b319223-2e0c-368f-90f4-2bf37fdcf0b3"))) ? ((getGlobalMemStoreHeapSize()) < (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("9e613413-22a7-3300-b3ec-317091bac1c7"))) ? ((getGlobalMemStoreOffHeapSize()) != (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("f44cc351-edaa-35e3-af0a-4ec1f2dfb3bc"))) ? ((getGlobalMemStoreHeapSize()) <= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("1b42a77b-944a-3b0a-b921-8f49f4d7e6d6"))) ? ((getGlobalMemStoreOffHeapSize()) >= (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("0662d3a7-fc1b-3406-b754-8bf7f906c7a8"))) ? ((getGlobalMemStoreHeapSize()) >= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("c40285d4-6777-340f-b61a-393094b24af3"))) ? ((getGlobalMemStoreOffHeapSize()) >= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("6c4aed51-a8a5-3c89-83f5-70f03844419c"))) ? ((getGlobalMemStoreHeapSize()) < (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("59ebd764-1833-3f30-bfb9-e1e5185963b6"))) ? ((getGlobalMemStoreOffHeapSize()) < (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("a70156f6-6732-3e61-81e2-ce23dc48ef63"))) ? ((getGlobalMemStoreHeapSize()) > (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("da0e5a05-a76a-39ba-989e-65196b4ece75"))) ? ((getGlobalMemStoreHeapSize()) <= (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("7df13569-5856-385e-b3a7-9c9b3d9d6887"))) ? ((getGlobalMemStoreHeapSize()) == (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("bd859567-f29d-394c-8f83-7fdb83a57e61"))) ? ((getGlobalMemStoreOffHeapSize()) != (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("e1a0e8b4-cb32-3b04-aa90-961d2843188d"))) ? ((getGlobalMemStoreOffHeapSize()) <= (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("45e2dce6-edcb-35cb-b18d-cdefaa6e3833"))) ? ((getGlobalMemStoreOffHeapSize()) >= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("40e3d1d4-8ace-3bef-9e3c-3c06d9e288e2"))) ? ((getGlobalMemStoreHeapSize()) != (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("311e9180-8d88-3ab2-817c-c312bfbe39d0"))) ? ((getGlobalMemStoreHeapSize()) >= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("2defe8c1-ea41-3c87-9867-3cde43447cf1"))) ? ((getGlobalMemStoreHeapSize()) <= (getGlobalMemStoreOffHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("e8f5afe2-4baa-34dc-987c-f18c64afa7f1"))) ? ((getGlobalMemStoreOffHeapSize()) == (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("ada4279d-0f74-3843-b62e-da817a6f702a"))) ? ((getGlobalMemStoreHeapSize()) == (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("61c1305b-214e-3386-8780-414978cc0c75"))) ? ((getGlobalMemStoreHeapSize()) > (globalMemStoreLimitLowMark)) : (((KnobRuntime.check(java.util.UUID.fromString("ae5956a2-7211-3f73-9d13-6e9f5c611f0a"))) ? ((getGlobalMemStoreOffHeapSize()) <= (getGlobalMemStoreHeapSize())) : (((KnobRuntime.check(java.util.UUID.fromString("c8e3cf6f-d014-30d2-9425-f5e737c0677d"))) ? ((getGlobalMemStoreOffHeapSize()) == (getGlobalMemStoreHeapSize())) : (getGlobalMemStoreHeapSize() >= globalMemStoreLimitLowMark))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
