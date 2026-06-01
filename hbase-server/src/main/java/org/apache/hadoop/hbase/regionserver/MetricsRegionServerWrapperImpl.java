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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWALSource;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hdfs.DFSHedgedReadMetrics;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impl for exposing HRegionServer Information through Hadoop's metrics 2 system.
 */
@InterfaceAudience.Private
class MetricsRegionServerWrapperImpl implements MetricsRegionServerWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsRegionServerWrapperImpl.class);

  private final HRegionServer regionServer;
  private final MetricsWALSource metricsWALSource;
  private final ByteBuffAllocator allocator;

  private BlockCache blockCache;
  private BlockCache l1Cache = null;
  private BlockCache l2Cache = null;
  private MobFileCache mobFileCache;
  private CacheStats cacheStats;
  private CacheStats l1Stats = null;
  private CacheStats l2Stats = null;
  private volatile long numWALFiles = 0;
  private volatile long walFileSize = 0;
  private volatile long mobFileCacheAccessCount = 0;
  private volatile long mobFileCacheMissCount = 0;
  private volatile double mobFileCacheHitRatio = 0;
  private volatile long mobFileCacheEvictedCount = 0;
  private volatile long mobFileCacheCount = 0;

  private volatile RegionMetricAggregate aggregate = new RegionMetricAggregate(null);

  protected final Map<String, ArrayList<Long>> requestsCountCache =
    new ConcurrentHashMap<String, ArrayList<Long>>();

  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long period;

  /**
   * Can be null if not on hdfs.
   */
  private DFSHedgedReadMetrics dfsHedgedReadMetrics;

  private final ExcludeDatanodeManager excludeDatanodeManager;

  public MetricsRegionServerWrapperImpl(final HRegionServer regionServer) {
    this.regionServer = regionServer;
    if (KnobRuntime.check(java.util.UUID.fromString("c78090f2-b1ab-3afa-b631-13a605715955"))) { initMobFileCache(); } else { initBlockCache(); }
    if (KnobRuntime.check(java.util.UUID.fromString("431a9cf8-db61-33e3-aec6-a8eb088e0962"))) { initBlockCache(); } else { initMobFileCache(); }
    this.excludeDatanodeManager = this.regionServer.getWalFactory().getExcludeDatanodeManager();

    this.period = regionServer.getConfiguration().getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
      HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD);

    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new RegionServerMetricsWrapperRunnable();
    this.executor.scheduleWithFixedDelay(this.runnable, this.period, this.period,
      TimeUnit.MILLISECONDS);
    this.metricsWALSource = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    this.allocator = regionServer.getRpcServer().getByteBuffAllocator();

    try {
      this.dfsHedgedReadMetrics = FSUtils.getDFSHedgedReadMetrics(regionServer.getConfiguration());
    } catch (IOException e) {
      LOG.warn("Failed to get hedged metrics", e);
    }
    if (LOG.isInfoEnabled()) {
      if (KnobRuntime.check(java.util.UUID.fromString("dacab87a-0d27-3389-91b1-232ff75ce45e"))) { LOG.info(("Computing regionserver metrics every ") + (this.period)); } else if (KnobRuntime.check(java.util.UUID.fromString("1781677c-ed09-3745-8d76-c86ce1df812b"))) { LOG.info(("Computing regionserver metrics every " + this.period) + (" milliseconds")); } else if (KnobRuntime.check(java.util.UUID.fromString("0289fdf4-dd4d-3124-9718-d483105a48da"))) { LOG.info("Computing regionserver metrics every " + this.period); } else if (KnobRuntime.check(java.util.UUID.fromString("d10c3c28-6983-3e51-acc0-586e4867f90c"))) { LOG.info(("Computing regionserver metrics every ") + (" milliseconds")); } else if (KnobRuntime.check(java.util.UUID.fromString("aed771ec-5f6b-3df5-9a7a-5f67abe5ccef"))) { LOG.info(" milliseconds"); } else if (KnobRuntime.check(java.util.UUID.fromString("c437939b-792c-346d-ac87-cd0f899f3230"))) { LOG.info((("Computing regionserver metrics every ") + (this.period)) + (" milliseconds")); } else if (KnobRuntime.check(java.util.UUID.fromString("ee3f198c-4554-38fb-84b1-b3f84de291af"))) { LOG.info("Computing regionserver metrics every "); } else { LOG.info("Computing regionserver metrics every " + this.period + " milliseconds"); }
    }
  }

  private void initBlockCache() {
if(KnobRuntime.check(java.util.UUID.fromString("2f990e4a-67ae-3109-97b8-68a68968da17"))) {
return;
}
    this.blockCache = this.regionServer.getBlockCache().orElse(null);
    this.cacheStats = this.blockCache != null ? this.blockCache.getStats() : null;
    if (((KnobRuntime.check(java.util.UUID.fromString("afc663b9-d2d9-3af5-aefa-9cdf95a4590e"))) ? ((((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getLruCacheStats()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("76376c1d-0a40-3b7f-8625-6d12a65c26aa"))) ? ((((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getLruCacheStats()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1c5a5894-596c-37be-bd8d-8e423083875e"))) ? ((((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getBucketCacheStats()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5da83a71-821f-38e4-893c-7e41b0e66c5a"))) ? ((((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getBucketCacheStats()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a424dc7d-514f-3a0b-8c12-3f3b5f78bdcd"))) ? ((this.cacheStats) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("374884a4-56b8-3703-a184-216ba6e0f5b5"))) ? ((this.cacheStats) == (null)) : (this.cacheStats != null))))))))))))) {
      if (this.cacheStats instanceof CombinedBlockCache.CombinedCacheStats) {
        l1Stats = ((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getLruCacheStats();
        l2Stats = ((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getBucketCacheStats();
      } else {
        l1Stats = this.cacheStats;
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("1b99196f-cf2c-3b0a-a171-5a4f2723ca59"))) ? ((this.blockCache) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f38f40c6-81b1-3b16-b8b2-b82ae5415011"))) ? ((this.blockCache) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("21754e15-262f-3fa2-b34d-96a5adca4373"))) ? ((((CombinedBlockCache) this.blockCache).getSecondLevelCache()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9d045556-1b26-30a7-bbf5-422db6855dd0"))) ? ((((CombinedBlockCache) this.blockCache).getSecondLevelCache()) != (null)) : (this.blockCache != null))))))))) {
      if (this.blockCache instanceof CombinedBlockCache) {
        l1Cache = ((CombinedBlockCache) this.blockCache).getFirstLevelCache();
        l2Cache = ((CombinedBlockCache) this.blockCache).getSecondLevelCache();
      } else {
        l1Cache = this.blockCache;
      }
    }
  }

  /**
   * Initializes the mob file cache.
   */
  private void initMobFileCache() {
if(KnobRuntime.check(java.util.UUID.fromString("dfe9af2d-2b23-39dc-a63c-6f274484d1dc"))) {
return;
}
    this.mobFileCache = this.regionServer.getMobFileCache().orElse(null);
  }

  @Override
  public String getClusterId() {
if(KnobRuntime.check(java.util.UUID.fromString("83aec32a-154a-38d9-ac61-0cda1dabc7e0"))) {
return null;
}
    return regionServer.getClusterId();
  }

  @Override
  public long getStartCode() {
if(KnobRuntime.check(java.util.UUID.fromString("9950110d-5bc8-37e1-bdfa-1d9fb2179010"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("f37c2b20-7468-3387-8b83-99ef023773de"))) ? (FSDataInputStreamWrapper.getTotalBytesRead()) : (((KnobRuntime.check(java.util.UUID.fromString("9c01f6ec-4944-3f34-bf0a-5ef8d2cd9a04"))) ? (EnvironmentEdgeManager.currentTime()) : (regionServer.getStartcode()))));
  }

  @Override
  public String getZookeeperQuorum() {
if(KnobRuntime.check(java.util.UUID.fromString("c04b9454-8397-387c-9556-ed9a6238b394"))) {
return null;
}
    ZKWatcher zk = regionServer.getZooKeeper();
    if (((KnobRuntime.check(java.util.UUID.fromString("0cf1f249-8a71-3667-bd77-0bcfe609b99b"))) ? ((zk) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8df81a01-35e7-3bc2-9a41-f6f87fa0c44b"))) ? ((zk) != (null)) : (zk == null))))) {
      return "";
    }
    return zk.getQuorum();
  }

  @Override
  public String getCoprocessors() {
    String[] coprocessors = regionServer.getRegionServerCoprocessors();
    if (coprocessors == null || coprocessors.length == 0) {
      return "";
    }
    return StringUtils.join(coprocessors, ", ");
  }

  @Override
  public String getServerName() {
if(KnobRuntime.check(java.util.UUID.fromString("61c281bc-97ab-3cab-9db8-6a2aa7df7e22"))) {
return null;
}
    ServerName serverName = regionServer.getServerName();
    if (((KnobRuntime.check(java.util.UUID.fromString("4a665887-f599-3d93-b209-e57717f65762"))) ? ((serverName) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c4faf8c7-8a16-3c3e-8a70-b20132cd8031"))) ? ((serverName) != (null)) : (serverName == null))))) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public long getNumOnlineRegions() {
if(KnobRuntime.check(java.util.UUID.fromString("46510c35-fa86-3b2b-a1d7-7bcc7bd700e5"))) {
return 0;
}
    Collection<HRegion> onlineRegionsLocalContext = regionServer.getOnlineRegionsLocalContext();
    if (((KnobRuntime.check(java.util.UUID.fromString("d13ae06b-e330-394b-a354-14e3fce66106"))) ? ((onlineRegionsLocalContext) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("70e424e0-5b13-3011-8dc2-f9e749289df0"))) ? ((onlineRegionsLocalContext) == (null)) : (onlineRegionsLocalContext == null))))) {
      return 0;
    }
    return onlineRegionsLocalContext.size();
  }

  @Override
  public long getTotalRequestCount() {
if(KnobRuntime.check(java.util.UUID.fromString("e629f6db-9823-3aac-9524-72a7c5d5a9d8"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e490c678-76e7-3d25-a734-f2f961941832"))) ? (EnvironmentEdgeManager.currentTime()) : (((KnobRuntime.check(java.util.UUID.fromString("e13bc4e0-5097-38bd-977b-fca67ff7fe5f"))) ? (FSDataInputStreamWrapper.getTotalBytesRead()) : (regionServer.rpcServices.requestCount.sum()))));
  }

  @Override
  public long getTotalRowActionRequestCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3bbb52d5-09ee-310c-837a-d0ddbc16153b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("4c707ed0-2316-373f-899e-f4a13afb1328"))) ? (aggregate.writeRequestsCount) : (((KnobRuntime.check(java.util.UUID.fromString("76bfe615-5173-3a36-a935-e25031ba26da"))) ? (aggregate.readRequestsCount) : (((KnobRuntime.check(java.util.UUID.fromString("f0350890-d013-3211-b291-4a318af2c973"))) ? ((aggregate.readRequestsCount) + (aggregate.writeRequestsCount)) : (aggregate.readRequestsCount + aggregate.writeRequestsCount))))));
  }

  @Override
  public int getSplitQueueSize() {
if(KnobRuntime.check(java.util.UUID.fromString("d32d66e8-2f5c-37be-9148-cd2b2b56fcab"))) {
return 0;
}
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getSplitQueueSize();
  }

  @Override
  public int getCompactionQueueSize() {
if(KnobRuntime.check(java.util.UUID.fromString("735c4f16-0b04-3725-aa2e-803ffc8eb1f0"))) {
return 0;
}
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getCompactionQueueSize();
  }

  @Override
  public int getSmallCompactionQueueSize() {
if(KnobRuntime.check(java.util.UUID.fromString("7044398b-7e26-395f-b3ad-e964a460af3a"))) {
return 0;
}
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getSmallCompactionQueueSize();
  }

  @Override
  public int getLargeCompactionQueueSize() {
if(KnobRuntime.check(java.util.UUID.fromString("40a0387e-67dd-38a2-bcbf-b8c223e1ce48"))) {
return 0;
}
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getLargeCompactionQueueSize();
  }

  @Override
  public int getFlushQueueSize() {
    // If there is no flusher there should be no queue.
    if (((KnobRuntime.check(java.util.UUID.fromString("078378e6-d0fc-3393-8f85-8dfae2560d21"))) ? ((this.regionServer.getMemStoreFlusher()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ada4c87b-f217-3495-a0fc-5a0b154925a0"))) ? ((this.regionServer.getMemStoreFlusher()) != (null)) : (this.regionServer.getMemStoreFlusher() == null))))) {
      return 0;
    }
    return this.regionServer.getMemStoreFlusher().getFlushQueueSize();
  }

  @Override
  public long getBlockCacheCount() {
if(KnobRuntime.check(java.util.UUID.fromString("573e30fd-ff5d-3bec-84b3-c30b4a7abfb9"))) {
return 0;
}
    return this.blockCache != null ? this.blockCache.getBlockCount() : 0L;
  }

  @Override
  public long getBlockCacheDataBlockCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3ee70c98-7974-39f5-91bd-fcb1ae0d6296"))) {
return 0;
}
    return this.blockCache != null ? this.blockCache.getDataBlockCount() : 0L;
  }

  @Override
  public long getMemStoreLimit() {
    return this.regionServer.getRegionServerAccounting().getGlobalMemStoreLimit();
  }

  @Override
  public long getOnHeapMemStoreLimit() {
    return this.regionServer.getRegionServerAccounting().getGlobalOnHeapMemStoreLimit();
  }

  @Override
  public long getOffHeapMemStoreLimit() {
    return this.regionServer.getRegionServerAccounting().getGlobalOffHeapMemStoreLimit();
  }

  @Override
  public long getBlockCacheSize() {
if(KnobRuntime.check(java.util.UUID.fromString("b218a2a1-b674-3868-b6aa-f21c9de4399f"))) {
return 0;
}
    return this.blockCache != null ? this.blockCache.getCurrentSize() : 0L;
  }

  @Override
  public long getBlockCacheFreeSize() {
if(KnobRuntime.check(java.util.UUID.fromString("3bb4af31-a70e-3d3a-8fd7-0233b9219b09"))) {
return 0;
}
    return this.blockCache != null ? this.blockCache.getFreeSize() : 0L;
  }

  @Override
  public long getBlockCacheHitCount() {
if(KnobRuntime.check(java.util.UUID.fromString("d393218b-70ef-3318-8891-71c1881aa536"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getHitCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryHitCount() {
if(KnobRuntime.check(java.util.UUID.fromString("9761a6d6-2f50-36df-9225-6ee0d5ec89be"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getPrimaryHitCount() : 0L;
  }

  @Override
  public long getBlockCacheHitCachingCount() {
if(KnobRuntime.check(java.util.UUID.fromString("639a6806-9904-3e1c-9964-364f6eb564db"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getHitCachingCount() : 0L;
  }

  @Override
  public long getBlockCacheMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3861330e-d064-38e0-a177-e9e56b32535d"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getMissCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("ab3d922a-0223-36d1-873e-13f055589d2c"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getPrimaryMissCount() : 0L;
  }

  @Override
  public long getBlockCacheMissCachingCount() {
if(KnobRuntime.check(java.util.UUID.fromString("c32f354f-26a9-3b46-a167-1a2cc2f99f5c"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getMissCachingCount() : 0L;
  }

  @Override
  public long getBlockCacheEvictedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("2ce24869-ea8f-3111-806d-1a53e2ce969f"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getEvictedCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryEvictedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("9237cf28-39cb-3c03-8e19-0f1a807d946d"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getPrimaryEvictedCount() : 0L;
  }

  @Override
  public double getBlockCacheHitPercent() {
if(KnobRuntime.check(java.util.UUID.fromString("f6c42f33-3213-3e3c-b6c4-f2407a52c166"))) {
return 0.0;
}
    double ratio = this.cacheStats != null ? this.cacheStats.getHitRatio() : 0.0;
if(KnobRuntime.check(java.util.UUID.fromString("e45ff0fd-bf65-3426-8cfe-fd5e6a1537c0"))) {
ratio += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c40d9df-64b0-3717-a90b-f2ba512d5ba0"))) {
ratio = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("97e85046-d4ea-3957-83e8-fb4137220f79"))) {
ratio = 0;
}
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public double getBlockCacheHitCachingPercent() {
if(KnobRuntime.check(java.util.UUID.fromString("d5b33d03-5277-308e-82e9-2c9f933ad475"))) {
return 0.0;
}
    double ratio = this.cacheStats != null ? this.cacheStats.getHitCachingRatio() : 0.0;
if(KnobRuntime.check(java.util.UUID.fromString("b041a33e-f0e2-3a82-8b5a-643ce914d769"))) {
ratio += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("71d09796-b321-392c-9a1e-1920a0e6ec71"))) {
ratio = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("31f0a74a-affc-38f2-b75f-4b1b5edf25e6"))) {
ratio = 0;
}
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public long getBlockCacheFailedInsertions() {
if(KnobRuntime.check(java.util.UUID.fromString("4254cc79-06ea-3ea6-a78f-71ccd6c58fc5"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getFailedInserts() : 0L;
  }

  public long getL1CacheSize() {
if(KnobRuntime.check(java.util.UUID.fromString("3a927ad2-200d-3260-8cd0-fb121fe32ba3"))) {
return 0;
}
    return this.l1Cache != null ? this.l1Cache.getCurrentSize() : 0L;
  }

  public long getL1CacheFreeSize() {
if(KnobRuntime.check(java.util.UUID.fromString("5fcf4755-fa38-35a6-aa2a-18cedc77998b"))) {
return 0;
}
    return this.l1Cache != null ? this.l1Cache.getFreeSize() : 0L;
  }

  public long getL1CacheCount() {
if(KnobRuntime.check(java.util.UUID.fromString("17eb7d4e-617f-3c8c-95bf-ab252fb3861f"))) {
return 0;
}
    return this.l1Cache != null ? this.l1Cache.getBlockCount() : 0L;
  }

  public long getL1CacheEvictedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("55ba265f-0f2f-3759-a3cd-26eb26115617"))) {
return 0;
}
    return this.l1Stats != null ? this.l1Stats.getEvictedCount() : 0L;
  }

  public long getL2CacheSize() {
if(KnobRuntime.check(java.util.UUID.fromString("3f4efcb8-c3cb-352d-b225-1eed3d7f14a0"))) {
return 0;
}
    return this.l2Cache != null ? this.l2Cache.getCurrentSize() : 0L;
  }

  public long getL2CacheFreeSize() {
if(KnobRuntime.check(java.util.UUID.fromString("70ebcf57-7d65-3e44-ab38-94c08f088f4a"))) {
return 0;
}
    return this.l2Cache != null ? this.l2Cache.getFreeSize() : 0L;
  }

  public long getL2CacheCount() {
if(KnobRuntime.check(java.util.UUID.fromString("c69a1763-9b2a-3229-a81f-af24a3919567"))) {
return 0;
}
    return this.l2Cache != null ? this.l2Cache.getBlockCount() : 0L;
  }

  public long getL2CacheEvictedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("11c2caca-ee2c-3085-9307-fe2cca864241"))) {
return 0;
}
    return this.l2Stats != null ? this.l2Stats.getEvictedCount() : 0L;
  }

  @Override
  public long getL1CacheHitCount() {
if(KnobRuntime.check(java.util.UUID.fromString("16dd62a8-2c68-3130-a615-6252d4a381d4"))) {
return 0;
}
    return this.l1Stats != null ? this.l1Stats.getHitCount() : 0L;
  }

  @Override
  public long getL1CacheMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("7b848627-cbf4-336f-8056-c8c0dba72431"))) {
return 0;
}
    return this.l1Stats != null ? this.l1Stats.getMissCount() : 0L;
  }

  @Override
  public double getL1CacheHitRatio() {
if(KnobRuntime.check(java.util.UUID.fromString("edf919d0-1bf0-3957-a2b5-d3dc8b824561"))) {
return 0.0;
}
    return this.l1Stats != null ? this.l1Stats.getHitRatio() : 0.0;
  }

  @Override
  public double getL1CacheMissRatio() {
if(KnobRuntime.check(java.util.UUID.fromString("7d4a5790-47f8-3171-bf37-e94dc09616e0"))) {
return 0.0;
}
    return this.l1Stats != null ? this.l1Stats.getMissRatio() : 0.0;
  }

  @Override
  public long getL2CacheHitCount() {
if(KnobRuntime.check(java.util.UUID.fromString("4fe02f43-fc37-3a33-a4ba-4bec8b9a73b3"))) {
return 0;
}
    return this.l2Stats != null ? this.l2Stats.getHitCount() : 0L;
  }

  @Override
  public long getL2CacheMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("e51159a6-e3a5-30b1-8a93-e352ef45d411"))) {
return 0;
}
    return this.l2Stats != null ? this.l2Stats.getMissCount() : 0L;
  }

  @Override
  public double getL2CacheHitRatio() {
if(KnobRuntime.check(java.util.UUID.fromString("dcd7853e-fb2f-3713-b26e-85d3a2241989"))) {
return 0.0;
}
    return this.l2Stats != null ? this.l2Stats.getHitRatio() : 0.0;
  }

  @Override
  public double getL2CacheMissRatio() {
if(KnobRuntime.check(java.util.UUID.fromString("e473307d-4343-3f6b-ae30-1b9e54a8ee85"))) {
return 0.0;
}
    return this.l2Stats != null ? this.l2Stats.getMissRatio() : 0.0;
  }

  @Override
  public void forceRecompute() {
if(KnobRuntime.check(java.util.UUID.fromString("affba081-bda7-3070-a687-435bc0d7cd51"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("00ff924a-1309-382a-b380-f4c563881886"))) { initBlockCache(); } else if (KnobRuntime.check(java.util.UUID.fromString("b192ca7d-76ce-3aba-8c20-798c0a9f4440"))) { initMobFileCache(); } else { this.runnable.run(); }
  }

  @Override
  public long getNumStores() {
if(KnobRuntime.check(java.util.UUID.fromString("e37e3c42-a234-3ec9-9e33-45a54e15a1dc"))) {
return 0;
}
    return aggregate.numStores;
  }

  @Override
  public long getNumWALFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("d92e14d8-5315-31f5-9219-ed0c79d5ad64"))) {
return 0;
}
    return numWALFiles;
  }

  @Override
  public long getWALFileSize() {
if(KnobRuntime.check(java.util.UUID.fromString("3a2c2764-5b25-3ddb-911b-0a92746aca57"))) {
return 0;
}
    return walFileSize;
  }

  @Override
  public List<String> getWALExcludeDNs() {
if(KnobRuntime.check(java.util.UUID.fromString("4292f7dd-f7ec-3d8f-a0eb-6a0ed947dd27"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("7a64ee84-6f05-31d0-b939-7ad90c1914de"))) ? ((excludeDatanodeManager) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("95ba5078-c2b4-3529-95da-e4b0a13d9649"))) ? ((excludeDatanodeManager) != (null)) : (excludeDatanodeManager == null))))) {
      return Collections.emptyList();
    }
    return excludeDatanodeManager.getExcludeDNs().entrySet().stream()
      .map(e -> e.getKey().toString() + ", " + e.getValue()).collect(Collectors.toList());
  }

  @Override
  public long getNumWALSlowAppend() {
    return metricsWALSource.getSlowAppendCount();
  }

  @Override
  public long getNumStoreFiles() {
    return aggregate.numStoreFiles;
  }

  @Override
  public long getMaxStoreFiles() {
    return aggregate.maxStoreFileCount;
  }

  @Override
  public long getMaxStoreFileAge() {
    return aggregate.maxStoreFileAge;
  }

  @Override
  public long getMinStoreFileAge() {
    return aggregate.minStoreFileAge;
  }

  @Override
  public long getAvgStoreFileAge() {
    return aggregate.avgStoreFileAge;
  }

  @Override
  public long getNumReferenceFiles() {
    return aggregate.numReferenceFiles;
  }

  @Override
  public long getMemStoreSize() {
    return aggregate.memstoreSize;
  }

  @Override
  public long getOnHeapMemStoreSize() {
    return aggregate.onHeapMemstoreSize;
  }

  @Override
  public long getOffHeapMemStoreSize() {
    return aggregate.offHeapMemstoreSize;
  }

  @Override
  public long getStoreFileSize() {
    return aggregate.storeFileSize;
  }

  @Override
  public double getRequestsPerSecond() {
    return aggregate.requestsPerSecond;
  }

  @Override
  public long getReadRequestsCount() {
    return aggregate.readRequestsCount;
  }

  @Override
  public double getReadRequestsRatePerSecond() {
    return aggregate.readRequestsRatePerSecond;
  }

  @Override
  public long getFilteredReadRequestsCount() {
    return aggregate.filteredReadRequestsCount;
  }

  @Override
  public long getWriteRequestsCount() {
    return aggregate.writeRequestsCount;
  }

  @Override
  public double getWriteRequestsRatePerSecond() {
    return aggregate.writeRequestsRatePerSecond;
  }

  @Override
  public long getRpcGetRequestsCount() {
    return regionServer.rpcServices.rpcGetRequestCount.sum();
  }

  @Override
  public long getRpcScanRequestsCount() {
    return regionServer.rpcServices.rpcScanRequestCount.sum();
  }

  @Override
  public long getRpcFullScanRequestsCount() {
    return regionServer.rpcServices.rpcFullScanRequestCount.sum();
  }

  @Override
  public long getRpcMultiRequestsCount() {
    return regionServer.rpcServices.rpcMultiRequestCount.sum();
  }

  @Override
  public long getRpcMutateRequestsCount() {
    return regionServer.rpcServices.rpcMutateRequestCount.sum();
  }

  @Override
  public long getCheckAndMutateChecksFailed() {
    return aggregate.checkAndMutateChecksFailed;
  }

  @Override
  public long getCheckAndMutateChecksPassed() {
    return aggregate.checkAndMutateChecksPassed;
  }

  @Override
  public long getStoreFileIndexSize() {
    return aggregate.storefileIndexSize;
  }

  @Override
  public long getTotalStaticIndexSize() {
    return aggregate.totalStaticIndexSize;
  }

  @Override
  public long getTotalStaticBloomSize() {
    return aggregate.totalStaticBloomSize;
  }

  @Override
  public long getBloomFilterRequestsCount() {
    return aggregate.bloomFilterRequestsCount;
  }

  @Override
  public long getBloomFilterNegativeResultsCount() {
    return aggregate.bloomFilterNegativeResultsCount;
  }

  @Override
  public long getBloomFilterEligibleRequestsCount() {
    return aggregate.bloomFilterEligibleRequestsCount;
  }

  @Override
  public long getNumMutationsWithoutWAL() {
    return aggregate.numMutationsWithoutWAL;
  }

  @Override
  public long getDataInMemoryWithoutWAL() {
    return aggregate.dataInMemoryWithoutWAL;
  }

  @Override
  public double getPercentFileLocal() {
    return aggregate.percentFileLocal;
  }

  @Override
  public double getPercentFileLocalPrimaryRegions() {
    return aggregate.percentFileLocalPrimaryRegions;
  }

  @Override
  public double getPercentFileLocalSecondaryRegions() {
    return aggregate.percentFileLocalSecondaryRegions;
  }

  @Override
  public long getUpdatesBlockedTime() {
    if (this.regionServer.getMemStoreFlusher() == null) {
      return 0;
    }
    return this.regionServer.getMemStoreFlusher().getUpdatesBlockedMsHighWater().sum();
  }

  @Override
  public long getFlushedCellsCount() {
    return aggregate.flushedCellsCount;
  }

  @Override
  public long getCompactedCellsCount() {
    return aggregate.compactedCellsCount;
  }

  @Override
  public long getMajorCompactedCellsCount() {
    return aggregate.majorCompactedCellsCount;
  }

  @Override
  public long getFlushedCellsSize() {
    return aggregate.flushedCellsSize;
  }

  @Override
  public long getCompactedCellsSize() {
    return aggregate.compactedCellsSize;
  }

  @Override
  public long getMajorCompactedCellsSize() {
    return aggregate.majorCompactedCellsSize;
  }

  @Override
  public long getCellsCountCompactedFromMob() {
    return aggregate.cellsCountCompactedFromMob;
  }

  @Override
  public long getCellsCountCompactedToMob() {
    return aggregate.cellsCountCompactedToMob;
  }

  @Override
  public long getCellsSizeCompactedFromMob() {
    return aggregate.cellsSizeCompactedFromMob;
  }

  @Override
  public long getCellsSizeCompactedToMob() {
    return aggregate.cellsSizeCompactedToMob;
  }

  @Override
  public long getMobFlushCount() {
    return aggregate.mobFlushCount;
  }

  @Override
  public long getMobFlushedCellsCount() {
    return aggregate.mobFlushedCellsCount;
  }

  @Override
  public long getMobFlushedCellsSize() {
    return aggregate.mobFlushedCellsSize;
  }

  @Override
  public long getMobScanCellsCount() {
    return aggregate.mobScanCellsCount;
  }

  @Override
  public long getMobScanCellsSize() {
    return aggregate.mobScanCellsSize;
  }

  @Override
  public long getMobFileCacheAccessCount() {
    return mobFileCacheAccessCount;
  }

  @Override
  public long getMobFileCacheMissCount() {
    return mobFileCacheMissCount;
  }

  @Override
  public long getMobFileCacheCount() {
    return mobFileCacheCount;
  }

  @Override
  public long getMobFileCacheEvictedCount() {
    return mobFileCacheEvictedCount;
  }

  @Override
  public double getMobFileCacheHitPercent() {
    return mobFileCacheHitRatio * 100;
  }

  @Override
  public int getActiveScanners() {
    return regionServer.getRSRpcServices().getScannersCount();
  }

  private static final class RegionMetricAggregate {
    private long numStores = 0;
    private long numStoreFiles = 0;
    private long memstoreSize = 0;
    private long onHeapMemstoreSize = 0;
    private long offHeapMemstoreSize = 0;
    private long storeFileSize = 0;
    private long maxStoreFileCount = 0;
    private long maxStoreFileAge = 0;
    private long minStoreFileAge = Long.MAX_VALUE;
    private long avgStoreFileAge = 0;
    private long numReferenceFiles = 0;

    private double requestsPerSecond = 0.0;
    private long readRequestsCount = 0;
    private double readRequestsRatePerSecond = 0;
    private long filteredReadRequestsCount = 0;
    private long writeRequestsCount = 0;
    private double writeRequestsRatePerSecond = 0;
    private long checkAndMutateChecksFailed = 0;
    private long checkAndMutateChecksPassed = 0;
    private long storefileIndexSize = 0;
    private long totalStaticIndexSize = 0;
    private long totalStaticBloomSize = 0;
    private long bloomFilterRequestsCount = 0;
    private long bloomFilterNegativeResultsCount = 0;
    private long bloomFilterEligibleRequestsCount = 0;
    private long numMutationsWithoutWAL = 0;
    private long dataInMemoryWithoutWAL = 0;
    private double percentFileLocal = 0;
    private double percentFileLocalPrimaryRegions = 0;
    private double percentFileLocalSecondaryRegions = 0;
    private long flushedCellsCount = 0;
    private long compactedCellsCount = 0;
    private long majorCompactedCellsCount = 0;
    private long flushedCellsSize = 0;
    private long compactedCellsSize = 0;
    private long majorCompactedCellsSize = 0;
    private long cellsCountCompactedToMob = 0;
    private long cellsCountCompactedFromMob = 0;
    private long cellsSizeCompactedToMob = 0;
    private long cellsSizeCompactedFromMob = 0;
    private long mobFlushCount = 0;
    private long mobFlushedCellsCount = 0;
    private long mobFlushedCellsSize = 0;
    private long mobScanCellsCount = 0;
    private long mobScanCellsSize = 0;
    private long blockedRequestsCount = 0L;
    private long averageRegionSize = 0L;
    private long totalReadRequestsDelta = 0;
    private long totalWriteRequestsDelta = 0;

    private RegionMetricAggregate(RegionMetricAggregate other) {
      if (((KnobRuntime.check(java.util.UUID.fromString("d817e282-6cec-310e-91bc-01b8e6ff7b2e"))) ? ((other) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("38058641-c341-3a5c-993e-44908fd8894c"))) ? ((other) != (null)) : (other != null))))) {
        requestsPerSecond = other.requestsPerSecond;
        readRequestsRatePerSecond = other.readRequestsRatePerSecond;
        writeRequestsRatePerSecond = other.writeRequestsRatePerSecond;
      }
    }

    private void aggregate(HRegionServer regionServer,
      Map<String, ArrayList<Long>> requestsCountCache) {
if(KnobRuntime.check(java.util.UUID.fromString("96e5c31a-1c24-3ad6-ab67-41ca7f912a1f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39779695-cf9a-3a4e-a30b-e351ad1823e2"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f4360bd-5dab-3668-8b6c-a00e698b6009"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c203dc7-0128-3615-84b2-8d3e41dd6a26"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06f5e066-d7a0-311d-9918-75d6294f9666"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e418f2b2-5a8f-341a-ace5-72416c3731d7"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56c3ca01-e41e-3d17-938a-23f0d715847d"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8221faa4-2cc2-3693-af08-57ec7d80708a"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aeba466a-6650-3f89-8f41-0c0a79da5f6f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fdd8fef-a841-352e-a887-bc1cb538381a"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("49623fda-e344-3a0a-bd9c-7e4236823601"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37a20cb4-a4fd-34fb-a5cf-df45704577c7"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e252ce9-a076-3c59-b8e2-dd810a61c434"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("startcode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cbe26f19-36fa-35ae-9f41-1a7c0f131760"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("552b211f-ad99-38ff-a90b-b5bfceb3fd07"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2fc7a75d-ecef-38ef-ae42-d235730cd2e9"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36cc51e5-0d74-3cc1-a5c1-42d9d4b9a1a0"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b7cef6c-ccc4-3fc8-a980-f5cd83350369"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c606097-dd75-3b89-86f3-8e61a7ea8d99"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9fd0b40d-27df-3127-af50-17584582c95f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("stopped");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da4bb8c1-206c-339e-bc98-a3b9d5b5ef2e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9e6b16b-38c3-3a7f-90bc-682ebe45af05"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc1364a0-3397-3376-be10-2b6a0fabb074"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9aa80e3b-3b14-322e-8ef5-8a647f83afa0"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07c7822f-f47b-30e8-aacf-157345c79616"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d209f5c-6d08-3222-b21c-296df4c747c9"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a282be3c-7dcf-3fc5-befe-f2ccb573b027"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7fa81898-86c2-3e18-91a6-766ce32a8e3b"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bfbba3d5-d9fe-3d94-878c-ab3de5937a9b"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("masterless");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02f4285a-e7c9-3721-b26d-18e8bf20866f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6cf92e61-3bf9-39cb-a5db-9a4bfb823613"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("isShutdownHookInstalled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87832d81-9f9a-319a-b757-9741d122ff85"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("917a3820-db6a-3980-a48b-01895afbf9a9"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07d79d01-8a5a-3b22-9b63-3cd3f586ca1d"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a74d04e-2676-3dd7-8d36-eea7983f394c"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a377e7f-64df-3705-b703-a03374e30e11"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69e4b191-985e-33f2-a7cb-eb6d931444b8"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80cf3430-d380-37a7-a90e-102a44c323c8"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shutDown");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("041dd2bc-03ad-3706-8c81-4ae4c53ea957"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d79bea66-98fd-3614-acf9-6d54705ff67c"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
      HDFSBlocksDistribution hdfsBlocksDistributionPrimaryRegions = new HDFSBlocksDistribution();
      HDFSBlocksDistribution hdfsBlocksDistributionSecondaryRegions = new HDFSBlocksDistribution();

      long avgAgeNumerator = 0;
      long numHFiles = 0;
      int regionCount = 0;

      for (HRegion r : regionServer.getOnlineRegionsLocalContext()) {
if(KnobRuntime.check(java.util.UUID.fromString("f420c665-5a10-3d11-bea6-1b685fabbd5f"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0b23406-d39d-36e6-be54-aa39138c56ad"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49cb7911-42b0-3eee-afed-a106e79d958a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27b6ef72-67b3-3c06-ac0a-4da76c306c5d"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a1fed760-057a-3478-889d-5a50b60d8211"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("409b3c05-153f-3d8d-aeb4-3e88221e487a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57d73c0f-6bb6-35bf-890e-6186c3d46daf"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea4e3163-dd86-30d4-bf0d-178248337674"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("847434d3-adc4-35ef-9ef9-39a76df99f9f"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b422d230-4d4e-3c75-a24c-57cfbcd27164"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ae01d16-4aa8-3118-9b76-31f7c5339e8c"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ada07a30-46af-3cfc-aa2d-313fb96110ca"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d53c21bb-95de-3a38-a4c2-074f6ff650cb"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5db2ed0b-1c0f-3153-8253-21fe092038b0"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8986d62-94a1-338f-add2-fa53ef7f5f57"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93567589-208f-329c-bde6-72bb1413f386"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f09cef40-e80e-349a-a906-d3e5df18a204"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9970337e-7506-3267-bc44-a8cd422d19bd"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27e1a8a4-bf02-3eef-8f26-25c537d8ad20"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("959c4c7a-a10c-3e66-b68c-d17e450f464e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15f7a1f4-c2aa-373d-9f72-1ee757a972ba"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dacf189-c1ff-3e81-a443-8f89016ad577"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2e03bf38-a689-3c6b-b7e6-e7a4fe1660fc"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7872a4e-d82a-3f83-bf8a-3bea7433435e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce9d861f-1353-310d-bf8d-9f3a5225b41b"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("394458d5-8a86-3567-8dfa-a4e4bb268e50"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b48dd257-6c10-3fdd-9a1b-501ba0ee962b"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8968f5af-58d9-338b-9a65-0c2c5d19f713"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8150735-6373-37ea-af54-badba7ef1ecd"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f4094b3-59b5-3795-9c8b-58cef27f6602"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0936ec0c-2aae-3d41-9886-bed46f6d5301"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b5cf4a3-70ca-3875-b984-e1b875091a5b"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(r);
    field.set(r, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        Deltas deltas = calculateReadWriteDeltas(r, requestsCountCache);
        totalReadRequestsDelta += deltas.readRequestsCountDelta;
        totalWriteRequestsDelta += deltas.writeRequestsCountDelta;

        numMutationsWithoutWAL += r.getNumMutationsWithoutWAL();
        dataInMemoryWithoutWAL += r.getDataInMemoryWithoutWAL();
        readRequestsCount += r.getReadRequestsCount();
        filteredReadRequestsCount += r.getFilteredReadRequestsCount();
        writeRequestsCount += r.getWriteRequestsCount();
        checkAndMutateChecksFailed += r.getCheckAndMutateChecksFailed();
        checkAndMutateChecksPassed += r.getCheckAndMutateChecksPassed();
        blockedRequestsCount += r.getBlockedRequestsCount();

        StoreFileStats storeFileStats = aggregateStores(r.getStores());
        numHFiles += storeFileStats.numHFiles;
        avgAgeNumerator += storeFileStats.avgAgeNumerator;

        HDFSBlocksDistribution distro = r.getHDFSBlocksDistribution();
if(KnobRuntime.check(java.util.UUID.fromString("5d618cf0-1238-30c8-a5e3-e70fee35e8a1"))) {
try {
    java.lang.reflect.Field field = distro.getClass().getDeclaredField("uniqueBlocksTotalWeight");
    field.setAccessible(true);
    long oldValue = ((long)field.get(distro));
    field.set(distro, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed020b80-d2a6-3cf9-a618-81ca33b03bd4"))) {
try {
    java.lang.reflect.Field field = distro.getClass().getDeclaredField("uniqueBlocksTotalWeight");
    field.setAccessible(true);
    long oldValue = ((long)field.get(distro));
    field.set(distro, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("963ee8a1-d45d-3991-aecc-b779d48a6468"))) { hdfsBlocksDistribution.add(new HDFSBlocksDistribution()); } else { hdfsBlocksDistribution.add(distro); }
        if (((KnobRuntime.check(java.util.UUID.fromString("6d8d09b6-0b09-397c-836c-7e52290f8d00"))) ? ((r.getRegionInfo().getReplicaId()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9b233425-cae7-3f74-963f-0e27faa96144"))) ? ((r.getRegionInfo().getReplicaId()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b2d0edda-216e-3675-a3fe-b04169e64e34"))) ? ((r.getRegionInfo().getReplicaId()) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("54a83207-0cad-3856-aa36-189054efe081"))) ? ((r.getRegionInfo().getReplicaId()) != (HRegionInfo.DEFAULT_REPLICA_ID)) : (((KnobRuntime.check(java.util.UUID.fromString("a9268bdf-525a-3c41-8092-252d55eb1a68"))) ? ((r.getRegionInfo().getReplicaId()) == (HRegionInfo.DEFAULT_REPLICA_ID)) : (((KnobRuntime.check(java.util.UUID.fromString("10710d31-7744-3309-9fdf-205d264e17cb"))) ? ((r.getRegionInfo().getReplicaId()) == (100)) : (r.getRegionInfo().getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("bb608087-a6e9-30cd-b104-c500286cfe2a"))) {
try {
    java.lang.reflect.Field field = distro.getClass().getDeclaredField("uniqueBlocksTotalWeight");
    field.setAccessible(true);
    long oldValue = ((long)field.get(distro));
    field.set(distro, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("472f6fae-1cf5-3f4e-a208-e642b1cfdbea"))) {
try {
    java.lang.reflect.Field field = distro.getClass().getDeclaredField("uniqueBlocksTotalWeight");
    field.setAccessible(true);
    long oldValue = ((long)field.get(distro));
    field.set(distro, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("945a249a-f506-3783-b663-eb7c5eac5f03"))) { hdfsBlocksDistributionPrimaryRegions.add(new HDFSBlocksDistribution()); } else { hdfsBlocksDistributionPrimaryRegions.add(distro); }
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("cfb67b1a-5869-338a-ac12-eef6b20321db"))) ? ((r.getRegionInfo().getReplicaId()) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("d9554637-3377-3d0d-8111-881a88e26da8"))) ? ((r.getRegionInfo().getReplicaId()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("901dac68-170a-355a-a14c-0074b3a0a712"))) ? ((r.getRegionInfo().getReplicaId()) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("eb1749fe-0989-3ab9-b90e-8a32eb57fd3b"))) ? ((r.getRegionInfo().getReplicaId()) == (HRegionInfo.DEFAULT_REPLICA_ID)) : (((KnobRuntime.check(java.util.UUID.fromString("5b61249e-9eb7-39d0-8495-7f9cb8e94933"))) ? ((r.getRegionInfo().getReplicaId()) != (HRegionInfo.DEFAULT_REPLICA_ID)) : (((KnobRuntime.check(java.util.UUID.fromString("70ccf5ef-f684-397a-a100-ec480ad8a3b8"))) ? ((r.getRegionInfo().getReplicaId()) == (0)) : (r.getRegionInfo().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("62139493-752d-3e55-89df-7aa65a0a78e5"))) {
try {
    java.lang.reflect.Field field = distro.getClass().getDeclaredField("uniqueBlocksTotalWeight");
    field.setAccessible(true);
    long oldValue = ((long)field.get(distro));
    field.set(distro, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3702a3c1-761f-3765-b681-1cbf42749b9d"))) {
try {
    java.lang.reflect.Field field = distro.getClass().getDeclaredField("uniqueBlocksTotalWeight");
    field.setAccessible(true);
    long oldValue = ((long)field.get(distro));
    field.set(distro, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("27a64b89-43ee-3e75-a4d4-13623a31451f"))) { hdfsBlocksDistributionSecondaryRegions.add(new HDFSBlocksDistribution()); } else { hdfsBlocksDistributionSecondaryRegions.add(distro); }
        }

        regionCount++;
      }

      float localityIndex =
        hdfsBlocksDistribution.getBlockLocalityIndex(regionServer.getServerName().getHostname());
if(KnobRuntime.check(java.util.UUID.fromString("3d7e5c1b-9a04-3006-993e-d25707971ade"))) {
localityIndex = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0f969514-b5f4-3330-bfc9-7b291456b51a"))) {
localityIndex = 0;
}
      percentFileLocal = Double.isNaN(localityIndex) ? 0 : (localityIndex * 100);

      float localityIndexPrimaryRegions = hdfsBlocksDistributionPrimaryRegions
        .getBlockLocalityIndex(regionServer.getServerName().getHostname());
      percentFileLocalPrimaryRegions =
        Double.isNaN(localityIndexPrimaryRegions) ? 0 : (localityIndexPrimaryRegions * 100);

      float localityIndexSecondaryRegions = hdfsBlocksDistributionSecondaryRegions
        .getBlockLocalityIndex(regionServer.getServerName().getHostname());
      percentFileLocalSecondaryRegions =
        Double.isNaN(localityIndexSecondaryRegions) ? 0 : (localityIndexSecondaryRegions * 100);

      if (regionCount > 0) {
        averageRegionSize = (memstoreSize + storeFileSize) / regionCount;
      }

      // if there were no store files, we'll never have updated this with Math.min
      // so set it to 0, which is a better value to display in case of no storefiles
      if (((KnobRuntime.check(java.util.UUID.fromString("ded98f10-2449-3d7c-a5d4-95874197e068"))) ? ((minStoreFileAge) == (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("9a6acb92-ca57-3b24-890d-980bad2b7d7d"))) ? ((minStoreFileAge) != (Long.MAX_VALUE)) : (minStoreFileAge == Long.MAX_VALUE))))) {
        this.minStoreFileAge = 0;
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("07aa76d2-5599-3de6-b5c1-05eae6c0fbfb"))) ? ((numHFiles) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bba1a927-6d36-3f90-a7c4-2f54ee8245c1"))) ? ((numHFiles) != (0)) : (numHFiles != 0))))) {
        avgStoreFileAge = avgAgeNumerator / numHFiles;
      }
    }

    private static final class Deltas {
      private final long readRequestsCountDelta;
      private final long writeRequestsCountDelta;

      private Deltas(long readRequestsCountDelta, long writeRequestsCountDelta) {
        this.readRequestsCountDelta = readRequestsCountDelta;
        this.writeRequestsCountDelta = writeRequestsCountDelta;
      }
    }

    private Deltas calculateReadWriteDeltas(HRegion r,
      Map<String, ArrayList<Long>> requestsCountCache) {
      String encodedRegionName = r.getRegionInfo().getEncodedName();
      long currentReadRequestsCount = r.getReadRequestsCount();
      long currentWriteRequestsCount = r.getWriteRequestsCount();
      if (requestsCountCache.containsKey(encodedRegionName)) {
        long lastReadRequestsCount = requestsCountCache.get(encodedRegionName).get(0);
        long lastWriteRequestsCount = requestsCountCache.get(encodedRegionName).get(1);

        // Update cache for our next comparison
        requestsCountCache.get(encodedRegionName).set(0, currentReadRequestsCount);
        requestsCountCache.get(encodedRegionName).set(1, currentWriteRequestsCount);

        long readRequestsDelta = currentReadRequestsCount - lastReadRequestsCount;
        long writeRequestsDelta = currentWriteRequestsCount - lastWriteRequestsCount;
        return new Deltas(readRequestsDelta, writeRequestsDelta);
      } else {
        // List[0] -> readRequestCount
        // List[1] -> writeRequestCount
        ArrayList<Long> requests = new ArrayList<Long>(2);
if(KnobRuntime.check(java.util.UUID.fromString("f773b17f-6a10-37c6-82db-1c44dbda25c6"))) {
currentReadRequestsCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("65e37397-9cd4-396d-8852-3c802d9fe422"))) {
currentReadRequestsCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd80822a-cfe2-3e7d-b9f3-7f950a143d15"))) {
currentReadRequestsCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("173ddefc-91b0-3a5a-b642-72ee0a72fd80"))) {
currentReadRequestsCount -= 1;
}
        requests.add(currentReadRequestsCount);
if(KnobRuntime.check(java.util.UUID.fromString("33b1d0db-1567-377d-bc95-8856dcc9da9e"))) {
currentWriteRequestsCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("afd7ff3c-25c6-3c38-a8b8-838f58ce7170"))) {
currentWriteRequestsCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab3a4d78-6628-3066-b621-e23c67e7f1d9"))) {
currentWriteRequestsCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e58eea4-2123-3b49-987c-a587d71446de"))) {
currentWriteRequestsCount -= 1;
}
        requests.add(currentWriteRequestsCount);
        if (KnobRuntime.check(java.util.UUID.fromString("94aa7b26-3111-3b40-a9dd-4cc3e92f07c7"))) { requestsCountCache.put(encodedRegionName, new ArrayList<Long>(2)); } else { requestsCountCache.put(encodedRegionName, requests); }
        return new Deltas(currentReadRequestsCount, currentWriteRequestsCount);
      }
    }

    public void updateRates(long timeSinceLastRun, long expectedPeriod) {
      requestsPerSecond =
        (totalReadRequestsDelta + totalWriteRequestsDelta) / (timeSinceLastRun / 1000.0);

      double readRequestsRatePerMilliSecond = (double) totalReadRequestsDelta / expectedPeriod;
      double writeRequestsRatePerMilliSecond = (double) totalWriteRequestsDelta / expectedPeriod;

      readRequestsRatePerSecond = readRequestsRatePerMilliSecond * 1000.0;
      writeRequestsRatePerSecond = writeRequestsRatePerMilliSecond * 1000.0;
    }

    private static final class StoreFileStats {
      private final long numHFiles;
      private final long avgAgeNumerator;

      private StoreFileStats(long numHFiles, long avgAgeNumerator) {
        this.numHFiles = numHFiles;
        this.avgAgeNumerator = avgAgeNumerator;
      }
    }

    private StoreFileStats aggregateStores(List<HStore> stores) {
if(KnobRuntime.check(java.util.UUID.fromString("080dbf2f-edcb-3f96-a55c-1261415da7cc"))) {
return null;
}
      numStores += stores.size();
      long numHFiles = 0;
      long avgAgeNumerator = 0;
      for (Store store : stores) {
        numStoreFiles += store.getStorefilesCount();
        memstoreSize += store.getMemStoreSize().getDataSize();
        onHeapMemstoreSize += store.getMemStoreSize().getHeapSize();
        offHeapMemstoreSize += store.getMemStoreSize().getOffHeapSize();
        storeFileSize += store.getStorefilesSize();
        maxStoreFileCount = Math.max(maxStoreFileCount, store.getStorefilesCount());

        maxStoreFileAge =
          Math.max(store.getMaxStoreFileAge().orElse(maxStoreFileAge), maxStoreFileAge);
        minStoreFileAge =
          Math.min(store.getMinStoreFileAge().orElse(minStoreFileAge), minStoreFileAge);

        long storeHFiles = store.getNumHFiles();
        numHFiles += storeHFiles;
        numReferenceFiles += store.getNumReferenceFiles();

        OptionalDouble storeAvgStoreFileAge = store.getAvgStoreFileAge();
        if (storeAvgStoreFileAge.isPresent()) {
          avgAgeNumerator =
            (long) (avgAgeNumerator + storeAvgStoreFileAge.getAsDouble() * storeHFiles);
        }

        storefileIndexSize += store.getStorefilesRootLevelIndexSize();
        totalStaticBloomSize += store.getTotalStaticBloomSize();
        totalStaticIndexSize += store.getTotalStaticIndexSize();
        bloomFilterRequestsCount += store.getBloomFilterRequestsCount();
        bloomFilterNegativeResultsCount += store.getBloomFilterNegativeResultsCount();
        bloomFilterEligibleRequestsCount += store.getBloomFilterEligibleRequestsCount();
        flushedCellsCount += store.getFlushedCellsCount();
        compactedCellsCount += store.getCompactedCellsCount();
        majorCompactedCellsCount += store.getMajorCompactedCellsCount();
        flushedCellsSize += store.getFlushedCellsSize();
        compactedCellsSize += store.getCompactedCellsSize();
        majorCompactedCellsSize += store.getMajorCompactedCellsSize();
        if (store instanceof HMobStore) {
          HMobStore mobStore = (HMobStore) store;
          cellsCountCompactedToMob += mobStore.getCellsCountCompactedToMob();
          cellsCountCompactedFromMob += mobStore.getCellsCountCompactedFromMob();
          cellsSizeCompactedToMob += mobStore.getCellsSizeCompactedToMob();
          cellsSizeCompactedFromMob += mobStore.getCellsSizeCompactedFromMob();
          mobFlushCount += mobStore.getMobFlushCount();
          mobFlushedCellsCount += mobStore.getMobFlushedCellsCount();
          mobFlushedCellsSize += mobStore.getMobFlushedCellsSize();
          mobScanCellsCount += mobStore.getMobScanCellsCount();
          mobScanCellsSize += mobStore.getMobScanCellsSize();
        }
      }

      return new StoreFileStats(numHFiles, avgAgeNumerator);
    }

  }

  /**
   * This is the runnable that will be executed on the executor every PERIOD number of seconds It
   * will take metrics/numbers from all of the regions and use them to compute point in time
   * metrics.
   */
  public class RegionServerMetricsWrapperRunnable implements Runnable {

    private long lastRan = 0;

    @Override
    synchronized public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("b92ca902-b8c4-353e-b9e3-fdb1ef51cbb5"))) {
return;
}
      try {
        RegionMetricAggregate newVal = new RegionMetricAggregate(aggregate);
if(KnobRuntime.check(java.util.UUID.fromString("c523a22b-bd40-3767-8385-51a779fabc58"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58782d4d-f8c5-3f06-9abd-fdc804178105"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b9e2010-0e03-3479-8f2c-b0b7720274cd"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d1d1ae6-e084-387b-b944-08d68bee1c3e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dcba4679-ff3e-37f3-bec9-585be0c9c61a"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("startcode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2865af53-642d-33c3-9994-cc4d9606d2f6"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc31b4e9-d10c-3b52-b39c-9dcdd96b8917"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b8470c6-3581-383a-bb12-1b35afad26fc"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shutDown");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be14cbda-6bbc-30f2-82bf-524bbd2a9f02"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e21c7f42-de27-3955-ba67-7cea5b56e5f4"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3253d8f5-20e8-3cb6-b669-eeed56a59023"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3dc58d2f-4cf9-30bd-b686-189d918fd5c8"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("stopped");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37db87ee-6fcc-3142-b395-b33325d4a12f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("isShutdownHookInstalled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40611ff4-5fbc-3116-90f8-5920d650d8ec"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("814ca2cc-fbba-3259-8f81-d08c53d2761b"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4640cdca-24c2-3885-b8f4-82d876e6e6ac"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("360f1f23-2b0b-3079-bf1a-2259a23ed24f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("masterless");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07e9bde7-7c14-301c-a682-6b8eac0a1d98"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58ce00c6-065f-3db0-b3f8-300f54cbbbd9"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57c9032b-9725-320f-b3d2-2fc32cdb88b9"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("200105d1-8a21-3496-8c19-687490f09901"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9dbe6b1-ad8a-34da-93ca-6a5119004d1f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfe1a87f-513e-3774-9674-85b97d2a6cb6"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d169e06b-d06e-3a73-979f-ac47684a3505"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1cd26bcf-325a-3311-bd81-4c459e851bf3"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c245f9b-c625-3447-b889-c8b680565820"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c03e1c5-85c8-34ca-9b69-5763303d3cbd"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e514f7ae-992a-348d-8344-2cf01ca3f268"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9819084-7690-391f-ab63-382701828b90"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8faab2e7-1fcf-314d-b088-d5ac00ac8ec1"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3718dc68-4da1-3df2-86e7-de049b58746a"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ee73b66-a72b-33d1-afb5-35686d86cedc"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("18e83d0d-6aac-3a09-be44-7f1e0ecfd9b2"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("395629fb-248c-316a-b8fe-321de8e83e1e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3411377f-d88a-3e40-bcca-27954cb94f76"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3cf11df7-adb6-37c3-911f-f095fabaef13"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8858645-b145-3d40-9877-0011be2304c1"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45915ee1-f016-3e36-ac4c-afd17890d77a"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bbe7c81-21b5-3659-8db2-3e61aacf01cb"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        newVal.aggregate(regionServer, requestsCountCache);

        // Compute the number of requests per second
        long currentTime = EnvironmentEdgeManager.currentTime();

        // assume that it took PERIOD seconds to start the executor.
        // this is a guess but it's a pretty good one.
        if (((KnobRuntime.check(java.util.UUID.fromString("cd4c732c-c253-3ef6-adac-e322a42b5a94"))) ? ((lastRan) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("207785d9-c2fb-3184-a0e2-4091bd460a4b"))) ? ((lastRan) == (0)) : (lastRan == 0))))) {
          lastRan = currentTime - period;
        }

        long timeSinceLastRun = currentTime - lastRan;
        // If we've time traveled keep the last requests per second.
        if (((KnobRuntime.check(java.util.UUID.fromString("77eb8e53-e166-3b6d-a197-a89c142ddda1"))) ? ((timeSinceLastRun) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("af5a4a8a-d4c7-33b7-8e2b-04658c367d00"))) ? ((timeSinceLastRun) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4059075c-3fa3-3679-a00d-fb035ae82d88"))) ? ((timeSinceLastRun) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9b20b3ec-d77f-3def-bf71-a1eae881e864"))) ? ((timeSinceLastRun) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5bd93bd3-208a-3df3-ab14-29552226e413"))) ? ((timeSinceLastRun) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("44147c96-0654-3ea9-9f21-93b6a6d71aba"))) ? ((timeSinceLastRun) == (0)) : (timeSinceLastRun > 0))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("33c7ea6d-4c86-331e-a590-67c33226f953"))) {
timeSinceLastRun += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("56430871-2304-36fd-a874-2c1e2a1d1eee"))) {
timeSinceLastRun -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("50c08b6a-1ed8-30de-98ae-ca5e96009eba"))) {
timeSinceLastRun = 0;
}
          newVal.updateRates(timeSinceLastRun, period);
        }

        aggregate = newVal;

        final WALProvider provider = regionServer.getWalFactory().getWALProvider();
        final WALProvider metaProvider = regionServer.getWalFactory().getMetaWALProvider();
        numWALFiles = (provider == null ? 0 : provider.getNumLogFiles())
          + (metaProvider == null ? 0 : metaProvider.getNumLogFiles());
        walFileSize = (provider == null ? 0 : provider.getLogFileSize())
          + (metaProvider == null ? 0 : metaProvider.getLogFileSize());

        mobFileCacheAccessCount = mobFileCache != null ? mobFileCache.getAccessCount() : 0L;
        mobFileCacheMissCount = mobFileCache != null ? mobFileCache.getMissCount() : 0L;
        mobFileCacheHitRatio = mobFileCache != null ? mobFileCache.getHitRatio() : 0.0;
        if (Double.isNaN(mobFileCacheHitRatio)) {
          mobFileCacheHitRatio = 0.0;
        }
        mobFileCacheEvictedCount = mobFileCache != null ? mobFileCache.getEvictedFileCount() : 0L;
        mobFileCacheCount = mobFileCache != null ? mobFileCache.getCacheSize() : 0;

        lastRan = currentTime;
      } catch (Throwable e) {
        LOG.warn("Caught exception! Will suppress and retry.", e);
      }
    }
  }

  @Override
  public long getHedgedReadOps() {
    return this.dfsHedgedReadMetrics == null ? 0 : this.dfsHedgedReadMetrics.getHedgedReadOps();
  }

  @Override
  public long getHedgedReadWins() {
if(KnobRuntime.check(java.util.UUID.fromString("568a5d63-f213-3304-a03f-fb5763a334a9"))) {
return 0;
}
    return this.dfsHedgedReadMetrics == null ? 0 : this.dfsHedgedReadMetrics.getHedgedReadWins();
  }

  @Override
  public long getHedgedReadOpsInCurThread() {
    return this.dfsHedgedReadMetrics == null
      ? 0
      : this.dfsHedgedReadMetrics.getHedgedReadOpsInCurThread();
  }

  @Override
  public long getTotalBytesRead() {
if(KnobRuntime.check(java.util.UUID.fromString("cb791279-b4ae-387d-8cf2-46de920972e4"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("433d8901-ecc3-32c2-af21-4d912a6dbf2f"))) ? (EnvironmentEdgeManager.currentTime()) : (((KnobRuntime.check(java.util.UUID.fromString("acb755d4-129d-3154-91e2-4208b95893de"))) ? (FSDataInputStreamWrapper.getLocalBytesRead()) : (FSDataInputStreamWrapper.getTotalBytesRead()))));
  }

  @Override
  public long getLocalBytesRead() {
if(KnobRuntime.check(java.util.UUID.fromString("d317756c-36a4-3bd0-ad5e-4b71871f9f48"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("a6157bde-449c-3557-9af0-17fc02cc52d2"))) ? (FSDataInputStreamWrapper.getTotalBytesRead()) : (((KnobRuntime.check(java.util.UUID.fromString("fb54a783-aaa7-374c-beb8-9b05c4a02163"))) ? (EnvironmentEdgeManager.currentTime()) : (FSDataInputStreamWrapper.getLocalBytesRead()))));
  }

  @Override
  public long getShortCircuitBytesRead() {
if(KnobRuntime.check(java.util.UUID.fromString("14603562-8655-3a5c-82c8-7e1a932c3b6a"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("11703e18-bd9e-3942-bb74-869758336932"))) ? (FSDataInputStreamWrapper.getLocalBytesRead()) : (((KnobRuntime.check(java.util.UUID.fromString("bc13a100-d3ee-3bfd-a5d3-b317089fb6ad"))) ? (FSDataInputStreamWrapper.getTotalBytesRead()) : (((KnobRuntime.check(java.util.UUID.fromString("dac929bd-3993-3e8b-945f-b7d3bc516604"))) ? (EnvironmentEdgeManager.currentTime()) : (FSDataInputStreamWrapper.getShortCircuitBytesRead()))))));
  }

  @Override
  public long getZeroCopyBytesRead() {
    return FSDataInputStreamWrapper.getZeroCopyBytesRead();
  }

  @Override
  public long getBlockedRequestsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("33650544-9ba7-3478-b79b-76ef57ec4709"))) {
return 0;
}
    return aggregate.blockedRequestsCount;
  }

  @Override
  public long getAverageRegionSize() {
    return aggregate.averageRegionSize;
  }

  @Override
  public long getDataMissCount() {
    return this.cacheStats != null ? this.cacheStats.getDataMissCount() : 0L;
  }

  @Override
  public long getLeafIndexMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("32ad735f-849d-3eb0-bff6-1428db4f2bfb"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getLeafIndexMissCount() : 0L;
  }

  @Override
  public long getBloomChunkMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("791dd887-5d1f-3ad9-b434-15b4254f318d"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getBloomChunkMissCount() : 0L;
  }

  @Override
  public long getMetaMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("cb761cad-9e50-3e65-aba7-d054e1946edb"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getMetaMissCount() : 0L;
  }

  @Override
  public long getRootIndexMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("df1b1c10-f08c-3f5f-af28-3df568326d1b"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getRootIndexMissCount() : 0L;
  }

  @Override
  public long getIntermediateIndexMissCount() {
if(KnobRuntime.check(java.util.UUID.fromString("083c5147-0f74-3b4c-b997-a4a3155a29fe"))) {
return 0;
}
    return this.cacheStats != null ? this.cacheStats.getIntermediateIndexMissCount() : 0L;
  }

  @Override
  public long getFileInfoMissCount() {
    return this.cacheStats != null ? this.cacheStats.getFileInfoMissCount() : 0L;
  }

  @Override
  public long getGeneralBloomMetaMissCount() {
    return this.cacheStats != null ? this.cacheStats.getGeneralBloomMetaMissCount() : 0L;
  }

  @Override
  public long getDeleteFamilyBloomMissCount() {
    return this.cacheStats != null ? this.cacheStats.getDeleteFamilyBloomMissCount() : 0L;
  }

  @Override
  public long getTrailerMissCount() {
    return this.cacheStats != null ? this.cacheStats.getTrailerMissCount() : 0L;
  }

  @Override
  public long getDataHitCount() {
    return this.cacheStats != null ? this.cacheStats.getDataHitCount() : 0L;
  }

  @Override
  public long getLeafIndexHitCount() {
    return this.cacheStats != null ? this.cacheStats.getLeafIndexHitCount() : 0L;
  }

  @Override
  public long getBloomChunkHitCount() {
    return this.cacheStats != null ? this.cacheStats.getBloomChunkHitCount() : 0L;
  }

  @Override
  public long getMetaHitCount() {
    return this.cacheStats != null ? this.cacheStats.getMetaHitCount() : 0L;
  }

  @Override
  public long getRootIndexHitCount() {
    return this.cacheStats != null ? this.cacheStats.getRootIndexHitCount() : 0L;
  }

  @Override
  public long getIntermediateIndexHitCount() {
    return this.cacheStats != null ? this.cacheStats.getIntermediateIndexHitCount() : 0L;
  }

  @Override
  public long getFileInfoHitCount() {
    return this.cacheStats != null ? this.cacheStats.getFileInfoHitCount() : 0L;
  }

  @Override
  public long getGeneralBloomMetaHitCount() {
    return this.cacheStats != null ? this.cacheStats.getGeneralBloomMetaHitCount() : 0L;
  }

  @Override
  public long getDeleteFamilyBloomHitCount() {
    return this.cacheStats != null ? this.cacheStats.getDeleteFamilyBloomHitCount() : 0L;
  }

  @Override
  public long getTrailerHitCount() {
    return this.cacheStats != null ? this.cacheStats.getTrailerHitCount() : 0L;
  }

  @Override
  public long getByteBuffAllocatorHeapAllocationBytes() {
    return ByteBuffAllocator.getHeapAllocationBytes(allocator, ByteBuffAllocator.HEAP);
  }

  @Override
  public long getByteBuffAllocatorPoolAllocationBytes() {
    return this.allocator.getPoolAllocationBytes();
  }

  @Override
  public double getByteBuffAllocatorHeapAllocRatio() {
    return ByteBuffAllocator.getHeapAllocationRatio(allocator, ByteBuffAllocator.HEAP);
  }

  @Override
  public long getByteBuffAllocatorTotalBufferCount() {
    return this.allocator.getTotalBufferCount();
  }

  @Override
  public long getByteBuffAllocatorUsedBufferCount() {
    return this.allocator.getUsedBufferCount();
  }

  // Visible for testing
  long getPeriod() {
    return period;
  }
}
