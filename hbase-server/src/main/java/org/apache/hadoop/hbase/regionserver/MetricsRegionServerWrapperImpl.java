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
    initBlockCache();
    initMobFileCache();
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
      if (KnobRuntime.check(java.util.UUID.fromString("0289fdf4-dd4d-3124-9718-d483105a48da"), "regionserver", this.regionServer)) { LOG.info("Computing regionserver metrics every " + this.period); } else { LOG.info("Computing regionserver metrics every " + this.period + " milliseconds"); }
    }
  }

  private void initBlockCache() {
    this.blockCache = this.regionServer.getBlockCache().orElse(null);
    this.cacheStats = this.blockCache != null ? this.blockCache.getStats() : null;
    if (((KnobRuntime.check(java.util.UUID.fromString("1c5a5894-596c-37be-bd8d-8e423083875e"), "regionserver", this.regionServer)) ? ((((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getBucketCacheStats()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5da83a71-821f-38e4-893c-7e41b0e66c5a"), "regionserver", this.regionServer)) ? ((((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getBucketCacheStats()) != (null)) : (this.cacheStats != null))))) {
      if (this.cacheStats instanceof CombinedBlockCache.CombinedCacheStats) {
        l1Stats = ((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getLruCacheStats();
        l2Stats = ((CombinedBlockCache.CombinedCacheStats) this.cacheStats).getBucketCacheStats();
      } else {
        l1Stats = this.cacheStats;
      }
    }
    if (this.blockCache != null) {
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
    this.mobFileCache = this.regionServer.getMobFileCache().orElse(null);
  }

  @Override
  public String getClusterId() {
    return regionServer.getClusterId();
  }

  @Override
  public long getStartCode() {
    return regionServer.getStartcode();
  }

  @Override
  public String getZookeeperQuorum() {
    ZKWatcher zk = regionServer.getZooKeeper();
    if (zk == null) {
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
    ServerName serverName = regionServer.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public long getNumOnlineRegions() {
    Collection<HRegion> onlineRegionsLocalContext = regionServer.getOnlineRegionsLocalContext();
    if (((KnobRuntime.check(java.util.UUID.fromString("d13ae06b-e330-394b-a354-14e3fce66106"), "regionserver", this.regionServer)) ? ((onlineRegionsLocalContext) != (null)) : (onlineRegionsLocalContext == null))) {
      return 0;
    }
    return onlineRegionsLocalContext.size();
  }

  @Override
  public long getTotalRequestCount() {
    return regionServer.rpcServices.requestCount.sum();
  }

  @Override
  public long getTotalRowActionRequestCount() {
    return aggregate.readRequestsCount + aggregate.writeRequestsCount;
  }

  @Override
  public int getSplitQueueSize() {
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getSplitQueueSize();
  }

  @Override
  public int getCompactionQueueSize() {
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getCompactionQueueSize();
  }

  @Override
  public int getSmallCompactionQueueSize() {
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getSmallCompactionQueueSize();
  }

  @Override
  public int getLargeCompactionQueueSize() {
    final CompactSplit compactSplit = regionServer.getCompactSplitThread();
    return compactSplit == null ? 0 : compactSplit.getLargeCompactionQueueSize();
  }

  @Override
  public int getFlushQueueSize() {
    // If there is no flusher there should be no queue.
    if (((KnobRuntime.check(java.util.UUID.fromString("078378e6-d0fc-3393-8f85-8dfae2560d21"), "regionserver", this.regionServer)) ? ((this.regionServer.getMemStoreFlusher()) == (null)) : (this.regionServer.getMemStoreFlusher() == null))) {
      return 0;
    }
    return this.regionServer.getMemStoreFlusher().getFlushQueueSize();
  }

  @Override
  public long getBlockCacheCount() {
    return this.blockCache != null ? this.blockCache.getBlockCount() : 0L;
  }

  @Override
  public long getBlockCacheDataBlockCount() {
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
    return this.blockCache != null ? this.blockCache.getCurrentSize() : 0L;
  }

  @Override
  public long getBlockCacheFreeSize() {
    return this.blockCache != null ? this.blockCache.getFreeSize() : 0L;
  }

  @Override
  public long getBlockCacheHitCount() {
    return this.cacheStats != null ? this.cacheStats.getHitCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryHitCount() {
    return this.cacheStats != null ? this.cacheStats.getPrimaryHitCount() : 0L;
  }

  @Override
  public long getBlockCacheHitCachingCount() {
    return this.cacheStats != null ? this.cacheStats.getHitCachingCount() : 0L;
  }

  @Override
  public long getBlockCacheMissCount() {
    return this.cacheStats != null ? this.cacheStats.getMissCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryMissCount() {
    return this.cacheStats != null ? this.cacheStats.getPrimaryMissCount() : 0L;
  }

  @Override
  public long getBlockCacheMissCachingCount() {
    return this.cacheStats != null ? this.cacheStats.getMissCachingCount() : 0L;
  }

  @Override
  public long getBlockCacheEvictedCount() {
    return this.cacheStats != null ? this.cacheStats.getEvictedCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryEvictedCount() {
    return this.cacheStats != null ? this.cacheStats.getPrimaryEvictedCount() : 0L;
  }

  @Override
  public double getBlockCacheHitPercent() {
    double ratio = this.cacheStats != null ? this.cacheStats.getHitRatio() : 0.0;
if(KnobRuntime.check(java.util.UUID.fromString("6c40d9df-64b0-3717-a90b-f2ba512d5ba0"), "regionserver", this.regionServer)) {
ratio = -1;
}
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public double getBlockCacheHitCachingPercent() {
    double ratio = this.cacheStats != null ? this.cacheStats.getHitCachingRatio() : 0.0;
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public long getBlockCacheFailedInsertions() {
    return this.cacheStats != null ? this.cacheStats.getFailedInserts() : 0L;
  }

  public long getL1CacheSize() {
    return this.l1Cache != null ? this.l1Cache.getCurrentSize() : 0L;
  }

  public long getL1CacheFreeSize() {
    return this.l1Cache != null ? this.l1Cache.getFreeSize() : 0L;
  }

  public long getL1CacheCount() {
if(KnobRuntime.check(java.util.UUID.fromString("17eb7d4e-617f-3c8c-95bf-ab252fb3861f"), "regionserver", this.regionServer)) {
return 0;
}
    return this.l1Cache != null ? this.l1Cache.getBlockCount() : 0L;
  }

  public long getL1CacheEvictedCount() {
    return this.l1Stats != null ? this.l1Stats.getEvictedCount() : 0L;
  }

  public long getL2CacheSize() {
    return this.l2Cache != null ? this.l2Cache.getCurrentSize() : 0L;
  }

  public long getL2CacheFreeSize() {
    return this.l2Cache != null ? this.l2Cache.getFreeSize() : 0L;
  }

  public long getL2CacheCount() {
    return this.l2Cache != null ? this.l2Cache.getBlockCount() : 0L;
  }

  public long getL2CacheEvictedCount() {
    return this.l2Stats != null ? this.l2Stats.getEvictedCount() : 0L;
  }

  @Override
  public long getL1CacheHitCount() {
    return this.l1Stats != null ? this.l1Stats.getHitCount() : 0L;
  }

  @Override
  public long getL1CacheMissCount() {
    return this.l1Stats != null ? this.l1Stats.getMissCount() : 0L;
  }

  @Override
  public double getL1CacheHitRatio() {
    return this.l1Stats != null ? this.l1Stats.getHitRatio() : 0.0;
  }

  @Override
  public double getL1CacheMissRatio() {
if(KnobRuntime.check(java.util.UUID.fromString("7d4a5790-47f8-3171-bf37-e94dc09616e0"), "regionserver", this.regionServer)) {
return 0.0;
}
    return this.l1Stats != null ? this.l1Stats.getMissRatio() : 0.0;
  }

  @Override
  public long getL2CacheHitCount() {
    return this.l2Stats != null ? this.l2Stats.getHitCount() : 0L;
  }

  @Override
  public long getL2CacheMissCount() {
    return this.l2Stats != null ? this.l2Stats.getMissCount() : 0L;
  }

  @Override
  public double getL2CacheHitRatio() {
    return this.l2Stats != null ? this.l2Stats.getHitRatio() : 0.0;
  }

  @Override
  public double getL2CacheMissRatio() {
    return this.l2Stats != null ? this.l2Stats.getMissRatio() : 0.0;
  }

  @Override
  public void forceRecompute() {
    this.runnable.run();
  }

  @Override
  public long getNumStores() {
    return aggregate.numStores;
  }

  @Override
  public long getNumWALFiles() {
    return numWALFiles;
  }

  @Override
  public long getWALFileSize() {
    return walFileSize;
  }

  @Override
  public List<String> getWALExcludeDNs() {
    if (excludeDatanodeManager == null) {
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
      if (((KnobRuntime.check(java.util.UUID.fromString("d817e282-6cec-310e-91bc-01b8e6ff7b2e"))) ? ((other) == (null)) : (other != null))) {
        requestsPerSecond = other.requestsPerSecond;
        readRequestsRatePerSecond = other.readRequestsRatePerSecond;
        writeRequestsRatePerSecond = other.writeRequestsRatePerSecond;
      }
    }

    private void aggregate(HRegionServer regionServer,
      Map<String, ArrayList<Long>> requestsCountCache) {
if(KnobRuntime.check(java.util.UUID.fromString("4f354bd1-c88b-35a7-8d4f-a4c3820c95bc"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("unitMB");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("96e85e5a-4cdf-34e2-936e-bc74b09ea4b3"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("retryPauseTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01966174-53c0-3ddc-911c-df1797f95eda"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49a73c14-1bb7-39ce-9bf7-48e1b81d6787"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("unitMB");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3485c1c6-115c-3c73-a93e-b74a2609c3c7"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef77e355-9995-3587-a2b1-d2ecae24ed88"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("msgInterval");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e749f086-2cdd-3971-bf3a-366628a98195"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue * 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("6608652e-91c8-3ea6-bc50-f1d57cd74176"))) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(r));
    _knob_field_.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0456790-a897-3c1b-aa9f-21c4e407173d"))) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(r));
    _knob_field_.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20473ba1-13f0-3b55-9ca7-a680fe824af2"))) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(r));
    _knob_field_.set(r, oldValue + 1);
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
        hdfsBlocksDistribution.add(distro);
        if (((KnobRuntime.check(java.util.UUID.fromString("a9268bdf-525a-3c41-8092-252d55eb1a68"))) ? ((r.getRegionInfo().getReplicaId()) == (HRegionInfo.DEFAULT_REPLICA_ID)) : (r.getRegionInfo().getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID))) {
          hdfsBlocksDistributionPrimaryRegions.add(distro);
        }
        if (r.getRegionInfo().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
          hdfsBlocksDistributionSecondaryRegions.add(distro);
        }

        regionCount++;
      }

      float localityIndex =
        hdfsBlocksDistribution.getBlockLocalityIndex(regionServer.getServerName().getHostname());
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
      if (minStoreFileAge == Long.MAX_VALUE) {
        this.minStoreFileAge = 0;
      }

      if (numHFiles != 0) {
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
        requests.add(currentReadRequestsCount);
        requests.add(currentWriteRequestsCount);
        requestsCountCache.put(encodedRegionName, requests);
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
      try {
        RegionMetricAggregate newVal = new RegionMetricAggregate(aggregate);
if(KnobRuntime.check(java.util.UUID.fromString("20ff378a-f2ee-3d29-bb06-36134723a311"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("operationTimeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("542a8403-d414-331b-aa80-c2680b29ce6a"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("operationTimeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("09d69573-cc0f-3493-82d2-2436fd9d9c38"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue / 2);
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
        if (lastRan == 0) {
          lastRan = currentTime - period;
        }

        long timeSinceLastRun = currentTime - lastRan;
        // If we've time traveled keep the last requests per second.
        if (((KnobRuntime.check(java.util.UUID.fromString("77eb8e53-e166-3b6d-a197-a89c142ddda1"))) ? ((timeSinceLastRun) <= (0)) : (timeSinceLastRun > 0))) {
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
    return FSDataInputStreamWrapper.getTotalBytesRead();
  }

  @Override
  public long getLocalBytesRead() {
    return FSDataInputStreamWrapper.getLocalBytesRead();
  }

  @Override
  public long getShortCircuitBytesRead() {
    return FSDataInputStreamWrapper.getShortCircuitBytesRead();
  }

  @Override
  public long getZeroCopyBytesRead() {
    return FSDataInputStreamWrapper.getZeroCopyBytesRead();
  }

  @Override
  public long getBlockedRequestsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("33650544-9ba7-3478-b79b-76ef57ec4709"), "regionserver", this.regionServer)) {
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
    return this.cacheStats != null ? this.cacheStats.getLeafIndexMissCount() : 0L;
  }

  @Override
  public long getBloomChunkMissCount() {
    return this.cacheStats != null ? this.cacheStats.getBloomChunkMissCount() : 0L;
  }

  @Override
  public long getMetaMissCount() {
    return this.cacheStats != null ? this.cacheStats.getMetaMissCount() : 0L;
  }

  @Override
  public long getRootIndexMissCount() {
    return this.cacheStats != null ? this.cacheStats.getRootIndexMissCount() : 0L;
  }

  @Override
  public long getIntermediateIndexMissCount() {
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
