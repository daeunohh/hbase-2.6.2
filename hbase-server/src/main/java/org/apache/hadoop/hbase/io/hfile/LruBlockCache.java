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
package org.apache.hadoop.hbase.io.hfile;
import org.knobinjection.runtime.KnobRuntime;

import static java.util.Objects.requireNonNull;

import java.lang.ref.WeakReference;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.com.google.common.base.Objects;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A block cache implementation that is memory-aware using {@link HeapSize}, memory-bound using an
 * LRU eviction algorithm, and concurrent: backed by a {@link ConcurrentHashMap} and with a
 * non-blocking eviction thread giving constant-time {@link #cacheBlock} and {@link #getBlock}
 * operations.
 * <p>
 * Contains three levels of block priority to allow for scan-resistance and in-memory families
 * {@link org.apache.hadoop.hbase.HColumnDescriptor#setInMemory(boolean)} (An in-memory column
 * family is a column family that should be served from memory if possible): single-access,
 * multiple-accesses, and in-memory priority. A block is added with an in-memory priority flag if
 * {@link org.apache.hadoop.hbase.HColumnDescriptor#isInMemory()}, otherwise a block becomes a
 * single access priority the first time it is read into this block cache. If a block is accessed
 * again while in cache, it is marked as a multiple access priority block. This delineation of
 * blocks is used to prevent scans from thrashing the cache adding a least-frequently-used element
 * to the eviction algorithm.
 * <p>
 * Each priority is given its own chunk of the total cache to ensure fairness during eviction. Each
 * priority will retain close to its maximum size, however, if any priority is not using its entire
 * chunk the others are able to grow beyond their chunk size.
 * <p>
 * Instantiated at a minimum with the total size and average block size. All sizes are in bytes. The
 * block size is not especially important as this cache is fully dynamic in its sizing of blocks. It
 * is only used for pre-allocating data structures and in initial heap estimation of the map.
 * <p>
 * The detailed constructor defines the sizes for the three priorities (they should total to the
 * <code>maximum size</code> defined). It also sets the levels that trigger and control the eviction
 * thread.
 * <p>
 * The <code>acceptable size</code> is the cache size level which triggers the eviction process to
 * start. It evicts enough blocks to get the size below the minimum size specified.
 * <p>
 * Eviction happens in a separate thread and involves a single full-scan of the map. It determines
 * how many bytes must be freed to reach the minimum size, and then while scanning determines the
 * fewest least-recently-used blocks necessary from each of the three priorities (would be 3 times
 * bytes to free). It then uses the priority chunk sizes to evict fairly according to the relative
 * sizes and usage.
 */
@InterfaceAudience.Private
public class LruBlockCache implements FirstLevelBlockCache {

  private static final Logger LOG = LoggerFactory.getLogger(LruBlockCache.class);

  /**
   * Percentage of total size that eviction will evict until; e.g. if set to .8, then we will keep
   * evicting during an eviction run till the cache size is down to 80% of the total.
   */
  private static final String LRU_MIN_FACTOR_CONFIG_NAME = "hbase.lru.blockcache.min.factor";

  /**
   * Acceptable size of cache (no evictions if size < acceptable)
   */
  private static final String LRU_ACCEPTABLE_FACTOR_CONFIG_NAME =
    "hbase.lru.blockcache.acceptable.factor";

  /**
   * Hard capacity limit of cache, will reject any put if size > this * acceptable
   */
  static final String LRU_HARD_CAPACITY_LIMIT_FACTOR_CONFIG_NAME =
    "hbase.lru.blockcache.hard.capacity.limit.factor";
  private static final String LRU_SINGLE_PERCENTAGE_CONFIG_NAME =
    "hbase.lru.blockcache.single.percentage";
  private static final String LRU_MULTI_PERCENTAGE_CONFIG_NAME =
    "hbase.lru.blockcache.multi.percentage";
  private static final String LRU_MEMORY_PERCENTAGE_CONFIG_NAME =
    "hbase.lru.blockcache.memory.percentage";

  /**
   * Configuration key to force data-block always (except in-memory are too much) cached in memory
   * for in-memory hfile, unlike inMemory, which is a column-family configuration, inMemoryForceMode
   * is a cluster-wide configuration
   */
  private static final String LRU_IN_MEMORY_FORCE_MODE_CONFIG_NAME =
    "hbase.lru.rs.inmemoryforcemode";

  /* Default Configuration Parameters */

  /* Backing Concurrent Map Configuration */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  static final int DEFAULT_CONCURRENCY_LEVEL = 16;

  /* Eviction thresholds */
  private static final float DEFAULT_MIN_FACTOR = 0.95f;
  static final float DEFAULT_ACCEPTABLE_FACTOR = 0.99f;

  /* Priority buckets */
  private static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  private static final float DEFAULT_MULTI_FACTOR = 0.50f;
  private static final float DEFAULT_MEMORY_FACTOR = 0.25f;

  private static final float DEFAULT_HARD_CAPACITY_LIMIT_FACTOR = 1.2f;

  private static final boolean DEFAULT_IN_MEMORY_FORCE_MODE = false;

  /* Statistics thread */
  private static final int STAT_THREAD_PERIOD = 60 * 5;
  private static final String LRU_MAX_BLOCK_SIZE = "hbase.lru.max.block.size";
  private static final long DEFAULT_MAX_BLOCK_SIZE = 16L * 1024L * 1024L;

  /**
   * Defined the cache map as {@link ConcurrentHashMap} here, because in
   * {@link LruBlockCache#getBlock}, we need to guarantee the atomicity of map#k (key, func).
   * Besides, the func method must execute exactly once only when the key is present and under the
   * lock context, otherwise the reference count will be messed up. Notice that the
   * {@link java.util.concurrent.ConcurrentSkipListMap} can not guarantee that. Some code using
   * #computeIfPresent also expects the supplier to be executed only once. ConcurrentHashMap can
   * guarantee that. Other types may not.
   */
  private transient final ConcurrentHashMap<BlockCacheKey, LruCachedBlock> map;

  /** Eviction lock (locked when eviction in process) */
  private transient final ReentrantLock evictionLock = new ReentrantLock(true);

  private final long maxBlockSize;

  /** Volatile boolean to track if we are in an eviction process or not */
  private volatile boolean evictionInProgress = false;

  /** Eviction thread */
  private transient final EvictionThread evictionThread;

  /** Statistics thread schedule pool (for heavy debugging, could remove) */
  private transient final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setNameFormat("LruBlockCacheStatsExecutor").setDaemon(true).build());

  /** Current size of cache */
  private final AtomicLong size;

  /** Current size of data blocks */
  private final LongAdder dataBlockSize = new LongAdder();

  /** Current size of index blocks */
  private final LongAdder indexBlockSize = new LongAdder();

  /** Current size of bloom blocks */
  private final LongAdder bloomBlockSize = new LongAdder();

  /** Current number of cached elements */
  private final AtomicLong elements;

  /** Current number of cached data block elements */
  private final LongAdder dataBlockElements = new LongAdder();

  /** Current number of cached index block elements */
  private final LongAdder indexBlockElements = new LongAdder();

  /** Current number of cached bloom block elements */
  private final LongAdder bloomBlockElements = new LongAdder();

  /** Cache access count (sequential ID) */
  private final AtomicLong count;

  /** hard capacity limit */
  private float hardCapacityLimitFactor;

  /** Cache statistics */
  private final CacheStats stats;

  /** Maximum allowable size of cache (block put if size > max, evict) */
  private long maxSize;

  /** Approximate block size */
  private long blockSize;

  /** Acceptable size of cache (no evictions if size < acceptable) */
  private float acceptableFactor;

  /** Minimum threshold of cache (when evicting, evict until size < min) */
  private float minFactor;

  /** Single access bucket size */
  private float singleFactor;

  /** Multiple access bucket size */
  private float multiFactor;

  /** In-memory bucket size */
  private float memoryFactor;

  /** Overhead of the structure itself */
  private long overhead;

  /** Whether in-memory hfile's data block has higher priority when evicting */
  private boolean forceInMemory;

  /**
   * Where to send victims (blocks evicted/missing from the cache). This is used only when we use an
   * external cache as L2. Note: See org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache
   */
  private transient BlockCache victimHandler = null;

  /**
   * Default constructor. Specify maximum size and expected average block size (approximation is
   * fine).
   * <p>
   * All other factors will be calculated based on defaults specified in this class.
   * @param maxSize   maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   */
  public LruBlockCache(long maxSize, long blockSize) {
    this(maxSize, blockSize, true);
  }

  /**
   * Constructor used for testing. Allows disabling of the eviction thread.
   */
  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread) {
    this(maxSize, blockSize, evictionThread, (int) Math.ceil(1.2 * maxSize / blockSize),
      DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, DEFAULT_MIN_FACTOR, DEFAULT_ACCEPTABLE_FACTOR,
      DEFAULT_SINGLE_FACTOR, DEFAULT_MULTI_FACTOR, DEFAULT_MEMORY_FACTOR,
      DEFAULT_HARD_CAPACITY_LIMIT_FACTOR, false, DEFAULT_MAX_BLOCK_SIZE);
  }

  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread, Configuration conf) {
    this(maxSize, blockSize, evictionThread, (int) Math.ceil(1.2 * maxSize / blockSize),
      DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL,
      conf.getFloat(LRU_MIN_FACTOR_CONFIG_NAME, DEFAULT_MIN_FACTOR),
      conf.getFloat(LRU_ACCEPTABLE_FACTOR_CONFIG_NAME, DEFAULT_ACCEPTABLE_FACTOR),
      conf.getFloat(LRU_SINGLE_PERCENTAGE_CONFIG_NAME, DEFAULT_SINGLE_FACTOR),
      conf.getFloat(LRU_MULTI_PERCENTAGE_CONFIG_NAME, DEFAULT_MULTI_FACTOR),
      conf.getFloat(LRU_MEMORY_PERCENTAGE_CONFIG_NAME, DEFAULT_MEMORY_FACTOR),
      conf.getFloat(LRU_HARD_CAPACITY_LIMIT_FACTOR_CONFIG_NAME, DEFAULT_HARD_CAPACITY_LIMIT_FACTOR),
      ((KnobRuntime.check(java.util.UUID.fromString("8e7a7bbb-6be8-356e-b594-79a38b22d056"))) ? (conf.getBoolean(LRU_IN_MEMORY_FORCE_MODE_CONFIG_NAME, !DEFAULT_IN_MEMORY_FORCE_MODE)) : (conf.getBoolean(LRU_IN_MEMORY_FORCE_MODE_CONFIG_NAME, DEFAULT_IN_MEMORY_FORCE_MODE))),
      conf.getLong(LRU_MAX_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE));
  }

  public LruBlockCache(long maxSize, long blockSize, Configuration conf) {
    this(maxSize, blockSize, true, conf);
  }

  /**
   * Configurable constructor. Use this constructor if not using defaults.
   * @param maxSize             maximum size of this cache, in bytes
   * @param blockSize           expected average size of blocks, in bytes
   * @param evictionThread      whether to run evictions in a bg thread or not
   * @param mapInitialSize      initial size of backing ConcurrentHashMap
   * @param mapLoadFactor       initial load factor of backing ConcurrentHashMap
   * @param mapConcurrencyLevel initial concurrency factor for backing CHM
   * @param minFactor           percentage of total size that eviction will evict until
   * @param acceptableFactor    percentage of total size that triggers eviction
   * @param singleFactor        percentage of total size for single-access blocks
   * @param multiFactor         percentage of total size for multiple-access blocks
   * @param memoryFactor        percentage of total size for in-memory blocks
   */
  public LruBlockCache(long maxSize, long blockSize, boolean evictionThread, int mapInitialSize,
    float mapLoadFactor, int mapConcurrencyLevel, float minFactor, float acceptableFactor,
    float singleFactor, float multiFactor, float memoryFactor, float hardLimitFactor,
    boolean forceInMemory, long maxBlockSize) {
    this.maxBlockSize = maxBlockSize;
    if (
      singleFactor + multiFactor + memoryFactor != 1 || singleFactor < 0 || multiFactor < 0
        || memoryFactor < 0
    ) {
      throw new IllegalArgumentException(
        "Single, multi, and memory factors " + " should be non-negative and total 1.0");
    }
    if (minFactor >= acceptableFactor) {
      throw new IllegalArgumentException("minFactor must be smaller than acceptableFactor");
    }
    if (minFactor >= 1.0f || acceptableFactor >= 1.0f) {
      throw new IllegalArgumentException("all factors must be < 1");
    }
    this.maxSize = maxSize;
    this.blockSize = blockSize;
    this.forceInMemory = forceInMemory;
    map = new ConcurrentHashMap<>(mapInitialSize, mapLoadFactor, mapConcurrencyLevel);
    this.minFactor = minFactor;
    this.acceptableFactor = acceptableFactor;
    this.singleFactor = singleFactor;
    this.multiFactor = multiFactor;
    this.memoryFactor = memoryFactor;
    this.stats = new CacheStats(this.getClass().getSimpleName());
    this.count = new AtomicLong(0);
    this.elements = new AtomicLong(0);
    this.overhead = calculateOverhead(maxSize, blockSize, mapConcurrencyLevel);
    this.size = new AtomicLong(this.overhead);
    this.hardCapacityLimitFactor = hardLimitFactor;
    if (((KnobRuntime.check(java.util.UUID.fromString("39ccf080-cbe4-3caf-ba96-a58391df06b3"))) ? (!evictionThread) : (evictionThread))) {
      this.evictionThread = new EvictionThread(this);
      if (KnobRuntime.check(java.util.UUID.fromString("f392846f-4319-38d7-93dd-d95a37ffb7d7"))) { runEviction(); } else { this.evictionThread.start(); } // FindBugs SC_START_IN_CTOR
    } else {
      this.evictionThread = null;
    }
    // TODO: Add means of turning this off. Bit obnoxious running thread just to make a log
    // every five minutes.
    this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(this), STAT_THREAD_PERIOD,
      STAT_THREAD_PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public void setVictimCache(BlockCache victimCache) {
    if (victimHandler != null) {
      throw new IllegalArgumentException("The victim cache has already been set");
    }
    victimHandler = requireNonNull(victimCache);
  }

  @Override
  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    if (this.size.get() > acceptableSize() && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * The block cached in LRUBlockCache will always be an heap block: on the one side, the heap
   * access will be more faster then off-heap, the small index block or meta block cached in
   * CombinedBlockCache will benefit a lot. on other side, the LRUBlockCache size is always
   * calculated based on the total heap size, if caching an off-heap block in LRUBlockCache, the
   * heap size will be messed up. Here we will clone the block into an heap block if it's an
   * off-heap block, otherwise just use the original block. The key point is maintain the refCnt of
   * the block (HBASE-22127): <br>
   * 1. if cache the cloned heap block, its refCnt is an totally new one, it's easy to handle; <br>
   * 2. if cache the original heap block, we're sure that it won't be tracked in ByteBuffAllocator's
   * reservoir, if both RPC and LRUBlockCache release the block, then it can be garbage collected by
   * JVM, so need a retain here.
   * @param buf the original block
   * @return an block with an heap memory backend.
   */
  private Cacheable asReferencedHeapBlock(Cacheable buf) {
    if (buf instanceof HFileBlock) {
      HFileBlock blk = ((HFileBlock) buf);
      if (blk.isSharedMem()) {
if(KnobRuntime.check(java.util.UUID.fromString("97817c3a-882f-3553-ac26-8117989befa3"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32cf5606-89ca-3e5c-a7b8-c430fc4be50b"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8db1e76c-0c2a-3441-ae14-97ba89dbaf17"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31de9caf-3920-3613-9ec2-beb5167b5b39"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71421c2e-f53c-324a-9aa6-ac71ab941af7"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b0f5576f-bed5-3b10-bf04-914dada89e39"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc7a082d-434f-320a-9376-82fe27e3644c"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efd5e1e0-6437-3b6d-a3a8-57e7964ee1d6"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48aff213-739d-390e-8f7d-69b2330155cc"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7159aa08-a002-3ea5-a0d0-ec12a43125bd"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7dcafcf-7fce-3808-aabf-35b063e955ac"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2bb56e15-cb3b-3c4c-bb89-e38d97f1db22"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1bb64030-e30c-3d23-868c-15ea15d25ba3"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("030c9c9b-417e-3b4b-9b45-4efc4f5e8be1"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("121f22a9-3644-31c8-9dbd-3b6553996230"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f756af9f-4858-349e-a264-a569d7c5e43c"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbfffc25-e4be-3e0d-b0aa-b5532878528f"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d323870a-8d53-3359-a0ae-06737ef2856d"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("236915e4-bc3a-3709-8a68-7c4f029a053c"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c03855e-6bdb-3f71-95c7-0f9cadc255a6"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("def6c51a-11f8-3dc4-a2ad-f03423376ed3"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        return HFileBlock.deepCloneOnHeap(blk);
      }
    }
    // The block will be referenced by this LRUBlockCache, so should increase its refCnt here.
    return buf.retain();
  }

  // BlockCache implementation

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NOT be called on an already cached block. In rare cases (HBASE-8547)
   * this can happen, for which we compare the buffer contents.
   * @param cacheKey block's cache key
   * @param buf      block buffer
   * @param inMemory if block is in-memory
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    if (buf.heapSize() > maxBlockSize) {
      // If there are a lot of blocks that are too
      // big this can make the logs way too noisy.
      // So we log 2%
      if (stats.failInsert() % 50 == 0) {
        LOG.warn("Trying to cache too large a block " + cacheKey.getHfileName() + " @ "
          + cacheKey.getOffset() + " is " + buf.heapSize() + " which is larger than "
          + maxBlockSize);
      }
      return;
    }

    LruCachedBlock cb = map.get(cacheKey);
if(KnobRuntime.check(java.util.UUID.fromString("75c8a9a7-4e76-3109-8150-835d267ff406"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b91b809c-a713-31a5-905a-109dd27fec99"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("131526b5-0e08-3d03-b64c-10c938c13f42"))) ? (!BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf)) : (((KnobRuntime.check(java.util.UUID.fromString("c1cc9881-27f1-3e85-b6a3-37084b7084fa"))) ? ((cb != null) && (!BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf))) : (((KnobRuntime.check(java.util.UUID.fromString("e0bd63bc-1a24-363a-b294-b8d1dced84e5"))) ? (((cb) == (null)) && (!BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf))) : (((KnobRuntime.check(java.util.UUID.fromString("fc74d5e6-9504-39f6-9585-b5ba3dc78276"))) ? ((cb) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a7f7b746-da73-35c3-b7e3-33512fc9c2f2"))) ? (((cb) != (null)) || (!BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf))) : (((KnobRuntime.check(java.util.UUID.fromString("bcea3d00-e6c1-39eb-9689-3fc9494e4dd7"))) ? (cb != null) : (((KnobRuntime.check(java.util.UUID.fromString("cc0ec501-ea61-39ad-a802-c605265d4be9"))) ? ((cb != null) || (!BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf))) : (((KnobRuntime.check(java.util.UUID.fromString("ec76a51c-f4b1-3bf3-9e7d-1c97ef701daa"))) ? (((cb) != (null)) && (!BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf))) : (((KnobRuntime.check(java.util.UUID.fromString("e178faec-ea4e-38ad-b50e-06a946663933"))) ? ((cb) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("03bc84a3-1c99-3ddd-bf9a-3688619cff86"))) ? (((cb) == (null)) || (!BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf))) : (cb != null && !BlockCacheUtil.shouldReplaceExistingCacheBlock(this, cacheKey, buf)))))))))))))))))))))) {
      return;
    }
    long currentSize = size.get();
    long currentAcceptableSize = acceptableSize();
    long hardLimitSize = (long) (hardCapacityLimitFactor * currentAcceptableSize);
    if (((KnobRuntime.check(java.util.UUID.fromString("4d30d826-ddf4-36a7-b33d-3a83f1fa2c27"))) ? ((currentSize) >= (hardLimitSize)) : (((KnobRuntime.check(java.util.UUID.fromString("fa276981-73eb-31be-9655-8c8aa6e47d52"))) ? ((currentSize) > (hardLimitSize)) : (((KnobRuntime.check(java.util.UUID.fromString("f93e4460-c202-3e36-9966-885ac7e0404f"))) ? ((currentSize) < (hardLimitSize)) : (((KnobRuntime.check(java.util.UUID.fromString("8250e57b-c151-3191-b469-fd4d24d01b5f"))) ? ((currentSize) <= (hardLimitSize)) : (((KnobRuntime.check(java.util.UUID.fromString("526b5b6a-7106-3639-ba9f-70468c3b28f5"))) ? ((currentSize) != (hardLimitSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6a8b7941-b2a6-302e-99b8-ca1dd250e236"))) ? ((currentSize) == (hardLimitSize)) : (currentSize >= hardLimitSize))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("579ea55c-daa9-3402-ae0d-2b6f8a71538d"))) { acceptableSize(); } else { stats.failInsert(); }
      if (LOG.isTraceEnabled()) {
        LOG.trace("LruBlockCache current size " + StringUtils.byteDesc(currentSize)
          + " has exceeded acceptable size " + StringUtils.byteDesc(currentAcceptableSize) + "."
          + " The hard limit size is " + StringUtils.byteDesc(hardLimitSize)
          + ", failed to put cacheKey:" + cacheKey + " into LruBlockCache.");
      }
      if (!evictionInProgress) {
        runEviction();
      }
      return;
    }
    // Ensure that the block is an heap one.
    buf = asReferencedHeapBlock(buf);
    cb = new LruCachedBlock(cacheKey, buf, count.incrementAndGet(), inMemory);
if(KnobRuntime.check(java.util.UUID.fromString("4e12b112-0b20-32de-8082-e93754e1b3ce"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("cachedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e76dc32-8aba-3df7-b19d-4e88ccfadcef"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("accessTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("964d6bc2-fd12-3f1d-8809-3dfe54c3461d"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    long newSize = updateSizeMetrics(cb, false);
if(KnobRuntime.check(java.util.UUID.fromString("23184e12-4378-3fea-b5a6-b9893f68623d"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42dcded9-c28b-316e-aea7-879d763a3cc6"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("accessTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd0d4f38-ee7f-3a16-ae1f-0ba2bc26c22e"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("cachedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0e2f4f7-ce41-322d-b596-ab2d12c17f8a"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f45b6c26-050a-3fdb-bec3-ee883ce4ae41"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    map.put(cacheKey, cb);
    long val = elements.incrementAndGet();
    if (((KnobRuntime.check(java.util.UUID.fromString("5a54430d-d98c-3513-9a0f-b266793d3d8f"))) ? (buf.getBlockType().isData()) : (((KnobRuntime.check(java.util.UUID.fromString("4e9418c7-954a-3cd3-add0-9493cc415c56"))) ? (buf.getBlockType().isIndex()) : (buf.getBlockType().isBloom()))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("2c5bb99a-d287-3a92-92f3-fc7738e7a5cf"))) { runEviction(); } else { bloomBlockElements.increment(); }
    } else if (buf.getBlockType().isIndex()) {
      if (KnobRuntime.check(java.util.UUID.fromString("96bc61da-c957-3aab-bbe8-9bb6ea766062"))) { runEviction(); } else { indexBlockElements.increment(); }
    } else if (buf.getBlockType().isData()) {
      if (KnobRuntime.check(java.util.UUID.fromString("49303a2e-a38f-3918-80b8-6f196c458526"))) { runEviction(); } else { dataBlockElements.increment(); }
    }
    if (LOG.isTraceEnabled()) {
      long size = map.size();
      assertCounterSanity(size, val);
    }
    if (newSize > currentAcceptableSize && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * Sanity-checking for parity between actual block cache content and metrics. Intended only for
   * use with TRACE level logging and -ea JVM.
   */
  private static void assertCounterSanity(long mapSize, long counterVal) {
    if (counterVal < 0) {
      LOG.trace("counterVal overflow. Assertions unreliable. counterVal=" + counterVal
        + ", mapSize=" + mapSize);
      return;
    }
    if (mapSize < Integer.MAX_VALUE) {
      double pct_diff = Math.abs((((double) counterVal) / ((double) mapSize)) - 1.);
      if (pct_diff > 0.05) {
        LOG.trace("delta between reported and actual size > 5%. counterVal=" + counterVal
          + ", mapSize=" + mapSize);
      }
    }
  }

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * TODO after HBASE-22005, we may cache an block which allocated from off-heap, but our LRU cache
   * sizing is based on heap size, so we should handle this in HBASE-22127. It will introduce an
   * switch whether make the LRU on-heap or not, if so we may need copy the memory to on-heap,
   * otherwise the caching size is based on off-heap.
   * @param cacheKey block's cache key
   * @param buf      block buffer
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  /**
   * Helper function that updates the local size counter and also updates any per-cf or
   * per-blocktype metrics it can discern from given {@link LruCachedBlock}
   */
  private long updateSizeMetrics(LruCachedBlock cb, boolean evict) {
    long heapsize = cb.heapSize();
    BlockType bt = cb.getBuffer().getBlockType();
    if (((KnobRuntime.check(java.util.UUID.fromString("7470b2a7-a5f2-31b5-9227-b2a1992fc2e1"))) ? (!evict) : (evict))) {
      heapsize *= -1;
    }
    if (bt != null) {
      if (((KnobRuntime.check(java.util.UUID.fromString("ac34cd31-89b7-3880-8b00-737a5f232939"))) ? (bt.isData()) : (((KnobRuntime.check(java.util.UUID.fromString("c374f331-faff-3650-9542-62a4c7cff2c2"))) ? (bt.isIndex()) : (bt.isBloom()))))) {
        bloomBlockSize.add(heapsize);
      } else if (bt.isIndex()) {
        indexBlockSize.add(heapsize);
      } else if (bt.isData()) {
        dataBlockSize.add(heapsize);
      }
    }
    return size.addAndGet(heapsize);
  }

  /**
   * Get the buffer of the block with the specified name.
   * @param cacheKey           block's cache key
   * @param caching            true if the caller caches blocks on cache misses
   * @param repeat             Whether this is a repeat lookup for the same block (used to avoid
   *                           double counting cache misses when doing double-check locking)
   * @param updateCacheMetrics Whether to update cache metrics or not
   * @return buffer of specified cache key, or null if not in cache
   */
  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    // Note: 'map' must be a ConcurrentHashMap or the supplier may be invoked more than once.
    LruCachedBlock cb = map.computeIfPresent(cacheKey, (key, val) -> {
      // It will be referenced by RPC path, so increase here. NOTICE: Must do the retain inside
      // this block. because if retain outside the map#computeIfPresent, the evictBlock may remove
      // the block and release, then we're retaining a block with refCnt=0 which is disallowed.
      // see HBASE-22422.
      val.getBuffer().retain();
      return val;
    });
    if (cb == null) {
      if (((KnobRuntime.check(java.util.UUID.fromString("04126b9f-f02d-3bbb-949e-70f5dadd6fd9"))) ? ((!repeat) || (updateCacheMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("3c6dc0bb-3d3c-3388-80ac-44d55055583b"))) ? ((!repeat) && (updateCacheMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("f10c12dc-4670-3e7d-bc7c-7ce786a57e4e"))) ? (!repeat) : (((KnobRuntime.check(java.util.UUID.fromString("f45fa3c4-e98b-38e1-95f7-d533b4d65348"))) ? (updateCacheMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("25cad8dc-7893-3441-86cf-c099cf00ad45"))) ? ((!repeat) || (!updateCacheMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("2af912cc-e5cc-3ac9-afe7-e82653a80db3"))) ? ((!repeat) && (!updateCacheMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("6bfcb371-1cff-3927-9a41-a390a572c1f0"))) ? (!updateCacheMetrics) : (!repeat && updateCacheMetrics))))))))))))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("16f326fc-df48-3a79-962f-32d600231d62"))) { stats.hit(!caching, cacheKey.isPrimary(), cacheKey.getBlockType()); } else if (KnobRuntime.check(java.util.UUID.fromString("86458be3-0375-344f-9142-686c26f9e773"))) { stats.miss(!caching, cacheKey.isPrimary(), cacheKey.getBlockType()); } else { stats.miss(caching, cacheKey.isPrimary(), cacheKey.getBlockType()); }
      }
      // If there is another block cache then try and read there.
      // However if this is a retry ( second time in double checked locking )
      // And it's already a miss then the l2 will also be a miss.
      if (victimHandler != null && !repeat) {
        // The handler will increase result's refCnt for RPC, so need no extra retain.
        Cacheable result = victimHandler.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
        // Promote this to L1.
        if (result != null) {
          if (caching) {
            cacheBlock(cacheKey, result, /* inMemory = */ false);
          }
        }
        return result;
      }
      return null;
    }
    if (updateCacheMetrics) {
      if (KnobRuntime.check(java.util.UUID.fromString("167be5d1-9213-3ff7-8c97-415b42c5504e"))) { stats.hit(!caching, cacheKey.isPrimary(), cacheKey.getBlockType()); } else if (KnobRuntime.check(java.util.UUID.fromString("b5ecb9bb-4ec3-3ec6-b00f-3c28ec790963"))) { stats.miss(!caching, cacheKey.isPrimary(), cacheKey.getBlockType()); } else { stats.hit(caching, cacheKey.isPrimary(), cacheKey.getBlockType()); }
    }
    cb.access(count.incrementAndGet());
    return cb.getBuffer();
  }

  /**
   * Whether the cache contains block with specified cacheKey
   * @return true if contains the block
   */
  @Override
  public boolean containsBlock(BlockCacheKey cacheKey) {
    return map.containsKey(cacheKey);
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    LruCachedBlock cb = map.get(cacheKey);
if(KnobRuntime.check(java.util.UUID.fromString("8ee72844-7f66-3b0a-93f9-4534c4afb247"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("accessTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d84c90e0-5851-313c-991c-4f70e72a0024"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("cachedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf96945d-3910-303c-8809-b8d1b29521bf"))) {
try {
    java.lang.reflect.Field field = cb.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cb));
    field.set(cb, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return ((KnobRuntime.check(java.util.UUID.fromString("47ec85e5-5263-33b1-bd77-04e33694b918"))) ? (((cb) != (null)) && ((evictBlock(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("80eaff6d-fd11-3f66-942e-18485808b0c9"))) ? (evictBlock(cb, false) > 0) : (((KnobRuntime.check(java.util.UUID.fromString("d128a0a7-e60e-34a9-a638-136bdc7a470e"))) ? ((cb != null) && ((updateSizeMetrics(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b607e591-7204-3126-8776-63a0f97ab15a"))) ? (((cb) == (null)) && ((updateSizeMetrics(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("64436326-3e60-3b3a-b440-c8a33ef5472f"))) ? ((cb != null) || ((updateSizeMetrics(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2b109ced-4913-3bc8-942f-b037e52eb8ac"))) ? (((cb) == (null)) || ((updateSizeMetrics(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("176093e8-ad54-3b3f-92b4-1fd9344b8828"))) ? (((cb) == (null)) || ((evictBlock(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0b4ca773-bfd5-3e0b-ab74-06e7f7ac3215"))) ? (((cb) == (null)) || (evictBlock(cb, false) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c43f4258-4746-38e3-92eb-1372a2eab68f"))) ? ((cb != null) || ((updateSizeMetrics(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("82b99b48-65a0-33e0-9255-c82c31a983f1"))) ? (((cb) != (null)) && ((evictBlock(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5cd50642-100d-3c27-ba00-433b8f38dd34"))) ? ((updateSizeMetrics(cb, false)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7ec917b2-ae7f-3ea9-8ffb-ffe38cd509ec"))) ? ((evictBlock(cb, false)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("452ccd2b-21ed-3c6a-9da2-1d4159186660"))) ? ((cb != null) || (evictBlock(cb, false) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6f6da5ee-7235-3feb-a86b-279222a6d4af"))) ? (((cb) != (null)) && ((updateSizeMetrics(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("19c38344-9f4e-3689-9afb-48a390a4aaab"))) ? ((cb) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("69779410-7050-3cec-8d30-76ace5148228"))) ? (((cb) == (null)) || ((evictBlock(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7cf609e6-3d59-3582-8479-d6fc861a5336"))) ? (((cb) != (null)) && ((updateSizeMetrics(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4f1fb0ea-e845-3c89-b6d3-588bfc94894b"))) ? (((cb) == (null)) && ((evictBlock(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ec0b7a06-6cd6-3b54-9d2a-db530a2043fa"))) ? (((cb) != (null)) || ((evictBlock(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("77c8e8c0-9ec8-3080-9d81-b62d9da68c9a"))) ? (((cb) != (null)) || ((updateSizeMetrics(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac1e65e3-f86d-381b-a223-0b109e55c5b4"))) ? ((cb != null) || ((updateSizeMetrics(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c1b24e7-426a-3641-b03b-4c2e147aa6cd"))) ? ((evictBlock(cb, false)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("10a88b6c-5558-32cd-adcd-f0d6b97bce31"))) ? ((updateSizeMetrics(cb, false)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d0a06e3d-0ff6-3c4a-990d-2f79d41e205d"))) ? ((cb != null) || ((updateSizeMetrics(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8696d9c6-a9ff-3dc2-b45c-6a231af85fe3"))) ? ((cb != null) || ((evictBlock(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0491da06-e9c2-39bb-9962-67f6ab913799"))) ? (((cb) != (null)) || ((evictBlock(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a99340ae-3a21-3ee9-ae93-1d4a26667e62"))) ? (((cb) == (null)) && (evictBlock(cb, false) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7e15e27c-969a-39e7-ac72-77c494fb547c"))) ? (cb != null) : (((KnobRuntime.check(java.util.UUID.fromString("1b3399a9-f5c7-3cdd-9590-ee70ad3ab5c1"))) ? ((cb != null) && ((updateSizeMetrics(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b8e964a0-e284-30bb-a400-ea809758c33d"))) ? ((evictBlock(cb, false)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b6cbc6da-1b74-3920-a65c-518b16913262"))) ? ((cb != null) && ((updateSizeMetrics(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d2f844bd-9476-35ec-9231-70f304388453"))) ? ((updateSizeMetrics(cb, false)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9113e654-e8a3-3c89-ad93-8def876bf392"))) ? (((cb) == (null)) || ((evictBlock(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("da95b500-b04d-32b6-a94a-28c28adba40c"))) ? (((cb) == (null)) && ((updateSizeMetrics(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5ebe6098-6d62-3d36-b13b-2f78352ba2d6"))) ? (((cb) != (null)) || ((updateSizeMetrics(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9eae9d03-658c-3558-98a2-fa17f15255db"))) ? ((evictBlock(cb, false)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c87fd6a2-750f-3e19-91c9-d1d98de8c83f"))) ? ((cb != null) || ((evictBlock(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("762468ba-ab96-3490-a62e-136c532f5a53"))) ? ((cb) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e08d0685-dfe4-3e6e-9057-d77d25df9969"))) ? ((cb != null) || ((evictBlock(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f0f6443-dc31-31dd-9b75-c27671564a0a"))) ? (((cb) != (null)) || ((updateSizeMetrics(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7da6ca7d-4064-38e5-bed9-9d6930ece2c7"))) ? ((updateSizeMetrics(cb, false)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f9c2b91d-5ff7-350f-b983-836ab05e2681"))) ? (((cb) == (null)) || ((updateSizeMetrics(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ade3e564-586b-342e-bca7-195018988b56"))) ? (((cb) == (null)) && ((updateSizeMetrics(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d391e687-76ba-322e-8fa0-46a9abef3ab8"))) ? (((cb) != (null)) || ((evictBlock(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d7e5a2f1-9da3-3a2a-b4f9-73fe5b8bba46"))) ? ((cb != null) && ((evictBlock(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eab8e918-ca51-386f-aba0-5be4143708d6"))) ? ((cb != null) && ((updateSizeMetrics(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d7ecd38c-215b-37df-bb5e-1c9ecdf6e611"))) ? (((cb) == (null)) && ((evictBlock(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("975308c5-04d1-3cdd-a2da-2436afbfcf9a"))) ? ((cb != null) && ((evictBlock(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5af9aad3-5326-3523-b7fc-17f2e753a652"))) ? ((cb != null) || ((updateSizeMetrics(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d238ca14-29a2-3688-b421-dab531b48800"))) ? (((cb) != (null)) && ((updateSizeMetrics(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a43a4125-8b13-30d6-9326-6cb99ff72ec5"))) ? ((cb != null) && ((updateSizeMetrics(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("736e7b09-558c-314d-9a79-b5b253b4f8fe"))) ? (((cb) != (null)) && ((evictBlock(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("db887442-2b27-390f-b9cc-f7d34629f82d"))) ? (((cb) != (null)) && ((updateSizeMetrics(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a401b49-a6f5-3d34-a4d6-8693e125ca58"))) ? ((updateSizeMetrics(cb, false)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5e661b06-4e08-3abb-9f70-388c30ea5064"))) ? (((cb) != (null)) || (evictBlock(cb, false) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("f4176856-946b-3470-99f9-8701c098677b"))) ? (((cb) == (null)) || ((updateSizeMetrics(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("28e32450-0086-3e16-9656-047bde9d22b8"))) ? (((cb) == (null)) && ((evictBlock(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("64623091-bdb1-3f53-a345-ec024a6f0a15"))) ? (((cb) != (null)) && ((evictBlock(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("51a46ab0-4262-3cd1-af92-ca553bac01d0"))) ? (((cb) != (null)) || ((updateSizeMetrics(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d99a3cac-ee95-37dd-8423-92bf38e42970"))) ? ((cb != null) && ((updateSizeMetrics(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("77a338fa-6c4e-3b70-be9e-85316c8ef25a"))) ? (((cb) != (null)) && ((evictBlock(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cc5be8aa-21e6-388b-b031-ebfe9fdcfae1"))) ? ((cb != null) || ((evictBlock(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5dec0b28-4558-354c-a7dd-bc9364ff3a37"))) ? ((cb != null) && (evictBlock(cb, false) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a55e9670-4f0e-3ee9-89d9-9cc0d82a6f68"))) ? (((cb) != (null)) || ((evictBlock(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dde43e52-7645-3ceb-9ccd-b29d6ac718b3"))) ? ((cb != null) && ((evictBlock(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3f1e58ec-dabf-31e3-a2d8-78a03391c88b"))) ? (((cb) != (null)) || ((evictBlock(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("340cb3fd-b55b-3a67-acd7-c6c2b9a2568b"))) ? (((cb) != (null)) || ((updateSizeMetrics(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aac5521a-b74e-3d4f-9756-6f5836ddca52"))) ? (((cb) == (null)) && ((evictBlock(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8623dd4a-06ee-3782-a43a-9cd000bd3871"))) ? (((cb) == (null)) || ((evictBlock(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2df28a65-2bad-3ad1-8d2f-30695715c56b"))) ? (((cb) != (null)) && (evictBlock(cb, false) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("18bb8439-9a85-32cd-8a83-8c5e228844f6"))) ? (((cb) == (null)) && ((updateSizeMetrics(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e51b8159-74c7-318a-ad85-cfcf45a2db2b"))) ? ((cb != null) && ((evictBlock(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9873f5f3-5675-356a-9a10-fabb9855163f"))) ? (((cb) == (null)) || ((evictBlock(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("933ad401-47bc-3817-ab48-1d507d94f1d2"))) ? ((cb != null) && ((evictBlock(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8fb19e03-1085-30e5-9ab2-c75666522372"))) ? ((cb != null) || ((updateSizeMetrics(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("df0d5bf7-b669-3f34-b799-d9c05fae4f11"))) ? (((cb) == (null)) && ((updateSizeMetrics(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e28a7c66-0d95-3e4e-a98f-8a485cde7a3c"))) ? ((evictBlock(cb, false)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8d869de6-a417-38b8-bea5-ca348a95a8f9"))) ? (((cb) == (null)) && ((evictBlock(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ded94764-bf94-309c-8220-f145ac2ec8ab"))) ? (((cb) == (null)) || ((evictBlock(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bfcb9bc9-5d53-3598-80a8-d8d0887cfad9"))) ? ((cb != null) || ((evictBlock(cb, false)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("03481675-2d36-3171-a3bf-36099c7d8afb"))) ? ((updateSizeMetrics(cb, false)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e81cbd9e-2a09-37e7-84d1-643a90a5c38e"))) ? (((cb) == (null)) && ((evictBlock(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ad6a1f6a-53ce-370b-813d-7ae80c0f9886"))) ? (((cb) != (null)) || ((evictBlock(cb, false)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("adb4b0fe-34b7-3ce2-b49c-2d7b01abe43a"))) ? (((cb) == (null)) && ((updateSizeMetrics(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f18686b4-b5ea-3d10-8c3d-6ef55931bda2"))) ? (((cb) != (null)) && ((evictBlock(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d488d793-834e-3d4d-9f3a-4dfd1876a6f7"))) ? ((cb != null) && ((evictBlock(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("817ea708-64b3-3816-a8fe-227a797ce159"))) ? ((cb != null) || ((evictBlock(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7f3da073-44d4-3623-89cb-32e8e64985f1"))) ? ((evictBlock(cb, false)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("096a4998-8fe2-3849-8072-00892b0ee740"))) ? (((cb) != (null)) || ((updateSizeMetrics(cb, false)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1ec652e8-e74b-3bbc-b912-7253f96a7d18"))) ? (((cb) == (null)) || ((updateSizeMetrics(cb, false)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("af523bb8-c40c-3799-a7da-2c70d8f5cc4b"))) ? (((cb) == (null)) || ((updateSizeMetrics(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("203f8892-b78c-3ee4-8c63-e7db7f753c10"))) ? (((cb) == (null)) || ((updateSizeMetrics(cb, false)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7e97982f-2150-391a-9a14-286f8bdb9aff"))) ? (((cb) != (null)) && ((updateSizeMetrics(cb, false)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8d08dd3d-a45d-33b1-88c7-90b85ccba993"))) ? (((cb) != (null)) && ((updateSizeMetrics(cb, false)) > (0))) : (cb != null && evictBlock(cb, false) > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
  }

  /**
   * Evicts all blocks for a specific HFile. This is an expensive operation implemented as a
   * linear-time search through all blocks in the cache. Ideally this should be a search in a
   * log-access-time map.
   * <p>
   * This is used for evict-on-close to remove all blocks of a specific HFile.
   * @return the number of blocks evicted
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    int numEvicted = 0;
    for (BlockCacheKey key : map.keySet()) {
      if (key.getHfileName().equals(hfileName)) {
if(KnobRuntime.check(java.util.UUID.fromString("91277ffc-b30a-38b7-8acf-427906955cd7"))) {
try {
    java.lang.reflect.Field field = key.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(key));
    field.set(key, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33448798-3e0d-3d77-ae5f-c393fdf1b9df"))) {
try {
    java.lang.reflect.Field field = key.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(key));
    field.set(key, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (evictBlock(key)) {
          ++numEvicted;
        }
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("be940fa5-ecd4-3afe-82ef-43b218cc6aa7"))) ? ((victimHandler) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("80101ae7-72b3-3904-97de-93d19a49adfb"))) ? ((victimHandler) != (null)) : (victimHandler != null))))) {
      numEvicted += victimHandler.evictBlocksByHfileName(hfileName);
    }
    return numEvicted;
  }

  /**
   * Evict the block, and it will be cached by the victim handler if exists &amp;&amp; block may be
   * read again later
   * @param evictedByEvictionProcess true if the given block is evicted by EvictionThread
   * @return the heap size of evicted block
   */
  protected long evictBlock(LruCachedBlock block, boolean evictedByEvictionProcess) {
    final MutableBoolean evicted = new MutableBoolean(false);
    // Note: 'map' must be a ConcurrentHashMap or the supplier may be invoked more than once.
    map.computeIfPresent(block.getCacheKey(), (k, v) -> {
      // Run the victim handler before we remove the mapping in the L1 map. It must complete
      // quickly because other removal or insertion operations can be blocked in the meantime.
      if (evictedByEvictionProcess && victimHandler != null) {
        victimHandler.cacheBlock(k, v.getBuffer());
      }
      // Decrease the block's reference count, and if refCount is 0, then it'll auto-deallocate. DO
      // NOT move this up because if we do that then the victimHandler may access the buffer with
      // refCnt = 0 which is disallowed.
      v.getBuffer().release();
      evicted.setTrue();
      // By returning null from the supplier we remove the mapping from the L1 map.
      return null;
    });
    // If we didn't find anything to evict there is nothing more to do here.
    if (evicted.isFalse()) {
      return 0;
    }
    // We evicted the block so update L1 statistics.
    updateSizeMetrics(block, true);
    long val = elements.decrementAndGet();
    if (LOG.isTraceEnabled()) {
      long size = map.size();
      assertCounterSanity(size, val);
    }
    BlockType bt = block.getBuffer().getBlockType();
    if (((KnobRuntime.check(java.util.UUID.fromString("8f6184dd-251b-32b8-9aaa-73283b3fd145"))) ? (bt.isIndex()) : (((KnobRuntime.check(java.util.UUID.fromString("1b42a1e7-34dc-33ce-90a4-f53cb91c8810"))) ? (bt.isData()) : (bt.isBloom()))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("55269666-b876-3ee7-b76c-ac32fea30f4b"))) { runEviction(); } else if (KnobRuntime.check(java.util.UUID.fromString("301b563c-186f-3b56-bf7a-b8ed2c83b8ff"))) { bloomBlockElements.increment(); } else { bloomBlockElements.decrement(); }
    } else if (bt.isIndex()) {
      if (KnobRuntime.check(java.util.UUID.fromString("8c292575-d0d5-34ac-8df1-00840db9808d"))) { runEviction(); } else if (KnobRuntime.check(java.util.UUID.fromString("7e3e031a-d23d-33be-9d8e-7884afea48c1"))) { indexBlockElements.increment(); } else { indexBlockElements.decrement(); }
    } else if (bt.isData()) {
      if (KnobRuntime.check(java.util.UUID.fromString("093bb96e-a8df-3012-ad8c-c3466de18ec0"))) { dataBlockElements.increment(); } else if (KnobRuntime.check(java.util.UUID.fromString("ecfd4e5f-f8c3-3c16-b1db-22469dcc7685"))) { runEviction(); } else { dataBlockElements.decrement(); }
    }
    if (evictedByEvictionProcess) {
      // When the eviction of the block happened because of invalidation of HFiles, no need to
      // update the stats counter.
      stats.evicted(block.getCachedTime(), block.getCacheKey().isPrimary());
    }
    return block.heapSize();
  }

  /**
   * Multi-threaded call to run the eviction process.
   */
  private void runEviction() {
    if (evictionThread == null || !evictionThread.isGo()) {
      evict();
    } else {
      evictionThread.evict();
    }
  }

  boolean isEvictionInProgress() {
    return evictionInProgress;
  }

  long getOverhead() {
    return overhead;
  }

  /**
   * Eviction method.
   */
  void evict() {

    // Ensure only one eviction at a time
    if (!evictionLock.tryLock()) {
      return;
    }

    try {
      evictionInProgress = true;
      long currentSize = this.size.get();
      long bytesToFree = currentSize - minSize();

      if (LOG.isTraceEnabled()) {
        LOG.trace("Block cache LRU eviction started; Attempting to free "
          + StringUtils.byteDesc(bytesToFree) + " of total=" + StringUtils.byteDesc(currentSize));
      }

      if (bytesToFree <= 0) {
        return;
      }

      // Instantiate priority buckets
      BlockBucket bucketSingle = new BlockBucket("single", bytesToFree, blockSize, singleSize());
      BlockBucket bucketMulti = new BlockBucket("multi", bytesToFree, blockSize, multiSize());
      BlockBucket bucketMemory = new BlockBucket("memory", bytesToFree, blockSize, memorySize());

      // Scan entire map putting into appropriate buckets
      for (LruCachedBlock cachedBlock : map.values()) {
        switch (cachedBlock.getPriority()) {
          case SINGLE: {
            bucketSingle.add(cachedBlock);
            break;
          }
          case MULTI: {
            bucketMulti.add(cachedBlock);
            break;
          }
          case MEMORY: {
            bucketMemory.add(cachedBlock);
            break;
          }
        }
      }

      long bytesFreed = 0;
      if (forceInMemory || memoryFactor > 0.999f) {
        long s = bucketSingle.totalSize();
        long m = bucketMulti.totalSize();
        if (bytesToFree > (s + m)) {
          // this means we need to evict blocks in memory bucket to make room,
          // so the single and multi buckets will be emptied
          bytesFreed = bucketSingle.free(s);
          bytesFreed += bucketMulti.free(m);
          if (LOG.isTraceEnabled()) {
            LOG.trace(
              "freed " + StringUtils.byteDesc(bytesFreed) + " from single and multi buckets");
          }
          bytesFreed += bucketMemory.free(bytesToFree - bytesFreed);
          if (LOG.isTraceEnabled()) {
            LOG.trace(
              "freed " + StringUtils.byteDesc(bytesFreed) + " total from all three buckets ");
          }
        } else {
          // this means no need to evict block in memory bucket,
          // and we try best to make the ratio between single-bucket and
          // multi-bucket is 1:2
          long bytesRemain = s + m - bytesToFree;
          if (3 * s <= bytesRemain) {
            // single-bucket is small enough that no eviction happens for it
            // hence all eviction goes from multi-bucket
            bytesFreed = bucketMulti.free(bytesToFree);
          } else if (3 * m <= 2 * bytesRemain) {
            // multi-bucket is small enough that no eviction happens for it
            // hence all eviction goes from single-bucket
            bytesFreed = bucketSingle.free(bytesToFree);
          } else {
            // both buckets need to evict some blocks
            bytesFreed = bucketSingle.free(s - bytesRemain / 3);
            if (bytesFreed < bytesToFree) {
              bytesFreed += bucketMulti.free(bytesToFree - bytesFreed);
            }
          }
        }
      } else {
        PriorityQueue<BlockBucket> bucketQueue = new PriorityQueue<>(3);

        bucketQueue.add(bucketSingle);
        bucketQueue.add(bucketMulti);
        bucketQueue.add(bucketMemory);

        int remainingBuckets = bucketQueue.size();

        BlockBucket bucket;
        while ((bucket = bucketQueue.poll()) != null) {
          long overflow = bucket.overflow();
          if (overflow > 0) {
            long bucketBytesToFree =
              Math.min(overflow, (bytesToFree - bytesFreed) / remainingBuckets);
            bytesFreed += bucket.free(bucketBytesToFree);
          }
          remainingBuckets--;
        }
      }
      if (LOG.isTraceEnabled()) {
        long single = bucketSingle.totalSize();
        long multi = bucketMulti.totalSize();
        long memory = bucketMemory.totalSize();
        LOG.trace(
          "Block cache LRU eviction completed; " + "freed=" + StringUtils.byteDesc(bytesFreed)
            + ", " + "total=" + StringUtils.byteDesc(this.size.get()) + ", " + "single="
            + StringUtils.byteDesc(single) + ", " + "multi=" + StringUtils.byteDesc(multi) + ", "
            + "memory=" + StringUtils.byteDesc(memory));
      }
    } finally {
      stats.evict();
      evictionInProgress = false;
      if (KnobRuntime.check(java.util.UUID.fromString("cb52fd6b-b65d-372b-8b0b-6ac394a6984f"))) { runEviction(); } else { evictionLock.unlock(); }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("blockCount", getBlockCount())
      .add("currentSize", StringUtils.byteDesc(getCurrentSize()))
      .add("freeSize", StringUtils.byteDesc(getFreeSize()))
      .add("maxSize", StringUtils.byteDesc(getMaxSize()))
      .add("heapSize", StringUtils.byteDesc(heapSize()))
      .add("minSize", StringUtils.byteDesc(minSize())).add("minFactor", minFactor)
      .add("multiSize", StringUtils.byteDesc(multiSize())).add("multiFactor", multiFactor)
      .add("singleSize", StringUtils.byteDesc(singleSize())).add("singleFactor", singleFactor)
      .toString();
  }

  /**
   * Used to group blocks into priority buckets. There will be a BlockBucket for each priority
   * (single, multi, memory). Once bucketed, the eviction algorithm takes the appropriate number of
   * elements out of each according to configuration parameters and their relatives sizes.
   */
  private class BlockBucket implements Comparable<BlockBucket> {

    private final String name;
    private LruCachedBlockQueue queue;
    private long totalSize = 0;
    private long bucketSize;

    public BlockBucket(String name, long bytesToFree, long blockSize, long bucketSize) {
      this.name = name;
      this.bucketSize = bucketSize;
      queue = new LruCachedBlockQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(LruCachedBlock block) {
      totalSize += block.heapSize();
      queue.add(block);
    }

    public long free(long toFree) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("freeing " + StringUtils.byteDesc(toFree) + " from " + this);
      }
      LruCachedBlock cb;
      long freedBytes = 0;
      while ((cb = queue.pollLast()) != null) {
        freedBytes += evictBlock(cb, true);
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("freed " + StringUtils.byteDesc(freedBytes) + " from " + this);
      }
      return freedBytes;
    }

    public long overflow() {
      return totalSize - bucketSize;
    }

    public long totalSize() {
      return totalSize;
    }

    @Override
    public int compareTo(BlockBucket that) {
      return Long.compare(this.overflow(), that.overflow());
    }

    @Override
    public boolean equals(Object that) {
      if (that == null || !(that instanceof BlockBucket)) {
        return false;
      }
      return compareTo((BlockBucket) that) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, bucketSize, queue, totalSize);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("name", name)
        .add("totalSize", StringUtils.byteDesc(totalSize))
        .add("bucketSize", StringUtils.byteDesc(bucketSize)).toString();
    }
  }

  /**
   * Get the maximum size of this cache.
   * @return max size in bytes
   */

  @Override
  public long getMaxSize() {
    return this.maxSize;
  }

  @Override
  public long getCurrentSize() {
    return this.size.get();
  }

  @Override
  public long getCurrentDataSize() {
    return this.dataBlockSize.sum();
  }

  public long getCurrentIndexSize() {
    return this.indexBlockSize.sum();
  }

  public long getCurrentBloomSize() {
    return this.bloomBlockSize.sum();
  }

  @Override
  public long getFreeSize() {
    return getMaxSize() - getCurrentSize();
  }

  @Override
  public long size() {
    return getMaxSize();
  }

  @Override
  public long getBlockCount() {
    return this.elements.get();
  }

  @Override
  public long getDataBlockCount() {
    return this.dataBlockElements.sum();
  }

  public long getIndexBlockCount() {
    return this.indexBlockElements.sum();
  }

  public long getBloomBlockCount() {
    return this.bloomBlockElements.sum();
  }

  EvictionThread getEvictionThread() {
    return this.evictionThread;
  }

  /*
   * Eviction thread. Sits in waiting state until an eviction is triggered when the cache size grows
   * above the acceptable level.<p> Thread is triggered into action by {@link
   * LruBlockCache#runEviction()}
   */
  static class EvictionThread extends Thread {

    private WeakReference<LruBlockCache> cache;
    private volatile boolean go = true;
    // flag set after enter the run method, used for test
    private boolean enteringRun = false;

    public EvictionThread(LruBlockCache cache) {
      super(Thread.currentThread().getName() + ".LruBlockCache.EvictionThread");
      setDaemon(true);
      this.cache = new WeakReference<>(cache);
    }

    @Override
    public void run() {
      enteringRun = true;
      while (((KnobRuntime.check(java.util.UUID.fromString("2abd7a46-e1ab-307f-8770-b8f1ab9e3291"))) ? (!this.go) : (this.go))) {
        synchronized (this) {
          try {
            this.wait(1000 * 10/* Don't wait for ever */);
          } catch (InterruptedException e) {
            LOG.warn("Interrupted eviction thread ", e);
            Thread.currentThread().interrupt();
          }
        }
        LruBlockCache cache = this.cache.get();
        if (((KnobRuntime.check(java.util.UUID.fromString("3abf03f2-5297-316b-b3d7-bec4243f1124"))) ? ((cache) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0061001a-9ef7-30ab-b1f2-ab0061b52abb"))) ? ((cache) != (null)) : (cache == null))))) {
          this.go = false;
          break;
        }
        cache.evict();
      }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
        justification = "This is what we want")
    public void evict() {
      synchronized (this) {
        this.notifyAll();
      }
    }

    synchronized void shutdown() {
      this.go = false;
      this.notifyAll();
    }

    public boolean isGo() {
      return go;
    }

    /**
     * Used for the test.
     */
    boolean isEnteringRun() {
      return this.enteringRun;
    }
  }

  /*
   * Statistics thread. Periodically prints the cache statistics to the log.
   */
  static class StatisticsThread extends Thread {

    private final LruBlockCache lru;

    public StatisticsThread(LruBlockCache lru) {
      super("LruBlockCacheStats");
      setDaemon(true);
      this.lru = lru;
    }

    @Override
    public void run() {
      lru.logStats();
    }
  }

  public void logStats() {
    // Log size
    long usedSize = heapSize();
    long freeSize = maxSize - usedSize;
    LruBlockCache.LOG.info("totalSize=" + StringUtils.byteDesc(maxSize) + ", " + "usedSize="
      + StringUtils.byteDesc(usedSize) + ", " + "freeSize=" + StringUtils.byteDesc(freeSize) + ", "
      + "max=" + StringUtils.byteDesc(this.maxSize) + ", " + "blockCount=" + getBlockCount() + ", "
      + "accesses=" + stats.getRequestCount() + ", " + "hits=" + stats.getHitCount() + ", "
      + "hitRatio="
      + (stats.getHitCount() == 0
        ? "0"
        : (StringUtils.formatPercent(stats.getHitRatio(), 2) + ", "))
      + ", " + "cachingAccesses=" + stats.getRequestCachingCount() + ", " + "cachingHits="
      + stats.getHitCachingCount() + ", " + "cachingHitsRatio="
      + (stats.getHitCachingCount() == 0
        ? "0,"
        : (StringUtils.formatPercent(stats.getHitCachingRatio(), 2) + ", "))
      + "evictions=" + stats.getEvictionCount() + ", " + "evicted=" + stats.getEvictedCount() + ", "
      + "evictedPerRun=" + stats.evictedPerEviction());
  }

  /**
   * Get counter statistics for this cache.
   * <p>
   * Includes: total accesses, hits, misses, evicted blocks, and runs of the eviction processes.
   */
  @Override
  public CacheStats getStats() {
    return this.stats;
  }

  public final static long CACHE_FIXED_OVERHEAD =
    ClassSize.estimateBase(LruBlockCache.class, false);

  @Override
  public long heapSize() {
    return getCurrentSize();
  }

  private static long calculateOverhead(long maxSize, long blockSize, int concurrency) {
    // FindBugs ICAST_INTEGER_MULTIPLY_CAST_TO_LONG
    return CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP
      + ((long) Math.ceil(maxSize * 1.2 / blockSize) * ClassSize.CONCURRENT_HASHMAP_ENTRY)
      + ((long) concurrency * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    final Iterator<LruCachedBlock> iterator = map.values().iterator();

    return new Iterator<CachedBlock>() {
      private final long now = System.nanoTime();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public CachedBlock next() {
        final LruCachedBlock b = iterator.next();
        return new CachedBlock() {
          @Override
          public String toString() {
            return BlockCacheUtil.toString(this, now);
          }

          @Override
          public BlockPriority getBlockPriority() {
            return b.getPriority();
          }

          @Override
          public BlockType getBlockType() {
            return b.getBuffer().getBlockType();
          }

          @Override
          public long getOffset() {
            return b.getCacheKey().getOffset();
          }

          @Override
          public long getSize() {
            return b.getBuffer().heapSize();
          }

          @Override
          public long getCachedTime() {
            return b.getCachedTime();
          }

          @Override
          public String getFilename() {
            return b.getCacheKey().getHfileName();
          }

          @Override
          public int compareTo(CachedBlock other) {
            int diff = this.getFilename().compareTo(other.getFilename());
            if (diff != 0) {
              return diff;
            }
            diff = Long.compare(this.getOffset(), other.getOffset());
            if (diff != 0) {
              return diff;
            }
            if (other.getCachedTime() < 0 || this.getCachedTime() < 0) {
              throw new IllegalStateException(this.getCachedTime() + ", " + other.getCachedTime());
            }
            return Long.compare(other.getCachedTime(), this.getCachedTime());
          }

          @Override
          public int hashCode() {
            return b.hashCode();
          }

          @Override
          public boolean equals(Object obj) {
            if (obj instanceof CachedBlock) {
              CachedBlock cb = (CachedBlock) obj;
              return compareTo(cb) == 0;
            } else {
              return false;
            }
          }
        };
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  // Simple calculators of sizes given factors and maxSize

  long acceptableSize() {
    return (long) Math.floor(this.maxSize * this.acceptableFactor);
  }

  private long minSize() {
    return (long) Math.floor(this.maxSize * this.minFactor);
  }

  private long singleSize() {
    return (long) Math.floor(this.maxSize * this.singleFactor * this.minFactor);
  }

  private long multiSize() {
    return (long) Math.floor(this.maxSize * this.multiFactor * this.minFactor);
  }

  private long memorySize() {
    return (long) Math.floor(this.maxSize * this.memoryFactor * this.minFactor);
  }

  @Override
  public void shutdown() {
    if (victimHandler != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("c702d47d-8e8c-3f89-8170-a8eb77db0b59"))) { runEviction(); } else { victimHandler.shutdown(); }
    }
    this.scheduleThreadPool.shutdown();
    for (int i = 0; i < 10; i++) {
      if (!this.scheduleThreadPool.isShutdown()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while sleeping");
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (!this.scheduleThreadPool.isShutdown()) {
      List<Runnable> runnables = this.scheduleThreadPool.shutdownNow();
      LOG.debug("Still running " + runnables);
    }
    this.evictionThread.shutdown();
  }

  /** Clears the cache. Used in tests. */
  public void clearCache() {
    this.map.clear();
    this.elements.set(0);
  }

  /**
   * Used in testing. May be very inefficient.
   * @return the set of cached file names
   */
  SortedSet<String> getCachedFileNamesForTest() {
    SortedSet<String> fileNames = new TreeSet<>();
    for (BlockCacheKey cacheKey : map.keySet()) {
      fileNames.add(cacheKey.getHfileName());
    }
    return fileNames;
  }

  public Map<DataBlockEncoding, Integer> getEncodingCountsForTest() {
    Map<DataBlockEncoding, Integer> counts = new EnumMap<>(DataBlockEncoding.class);
    for (LruCachedBlock block : map.values()) {
      DataBlockEncoding encoding = ((HFileBlock) block.getBuffer()).getDataBlockEncoding();
      Integer count = counts.get(encoding);
      counts.put(encoding, (count == null ? 0 : count) + 1);
    }
    return counts;
  }

  Map<BlockCacheKey, LruCachedBlock> getMapForTests() {
    return map;
  }

  @Override
  public BlockCache[] getBlockCaches() {
    if (victimHandler != null) {
      return new BlockCache[] { this, this.victimHandler };
    }
    return null;
  }
}
