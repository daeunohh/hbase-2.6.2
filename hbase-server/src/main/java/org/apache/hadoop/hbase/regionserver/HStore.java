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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.InnerStoreCellComparator;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.FailedArchiveException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.ReadOnlyConfiguration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.InvalidHFileException;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.IterableUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;

/**
 * A Store holds a column family in a Region. Its a memstore and a set of zero or more StoreFiles,
 * which stretch backwards over time.
 * <p>
 * There's no reason to consider append-logging at this level; all logging and locking is handled at
 * the HRegion level. Store just provides services to manage sets of StoreFiles. One of the most
 * important of those services is compaction services where files are aggregated once they pass a
 * configurable threshold.
 * <p>
 * Locking and transactions are handled at a higher level. This API should not be called directly
 * but by an HRegion manager.
 */
@InterfaceAudience.Private
public class HStore
  implements Store, HeapSize, StoreConfigInformation, PropagatingConfigurationObserver {
  public static final String MEMSTORE_CLASS_NAME = "hbase.regionserver.memstore.class";
  public static final String COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY =
    "hbase.server.compactchecker.interval.multiplier";
  public static final String BLOCKING_STOREFILES_KEY = "hbase.hstore.blockingStoreFiles";
  public static final String BLOCK_STORAGE_POLICY_KEY = "hbase.hstore.block.storage.policy";
  // "NONE" is not a valid storage policy and means we defer the policy to HDFS
  public static final String DEFAULT_BLOCK_STORAGE_POLICY = "NONE";
  public static final int DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER = 1000;
  public static final int DEFAULT_BLOCKING_STOREFILE_COUNT = 16;

  // HBASE-24428 : Update compaction priority for recently split daughter regions
  // so as to prioritize their compaction.
  // Any compaction candidate with higher priority than compaction of newly split daugher regions
  // should have priority value < (Integer.MIN_VALUE + 1000)
  private static final int SPLIT_REGION_COMPACTION_PRIORITY = Integer.MIN_VALUE + 1000;

  private static final Logger LOG = LoggerFactory.getLogger(HStore.class);

  protected final MemStore memstore;
  // This stores directory in the filesystem.
  private final HRegion region;
  protected Configuration conf;
  private long lastCompactSize = 0;
  volatile boolean forceMajor = false;
  private AtomicLong storeSize = new AtomicLong();
  private AtomicLong totalUncompressedBytes = new AtomicLong();
  private LongAdder memstoreOnlyRowReadsCount = new LongAdder();
  // rows that has cells from both memstore and files (or only files)
  private LongAdder mixedRowReadsCount = new LongAdder();

  /**
   * Lock specific to archiving compacted store files. This avoids races around the combination of
   * retrieving the list of compacted files and moving them to the archive directory. Since this is
   * usually a background process (other than on close), we don't want to handle this with the store
   * write lock, which would block readers and degrade performance. Locked by: -
   * CompactedHFilesDispatchHandler via closeAndArchiveCompactedFiles() - close()
   */
  final ReentrantLock archiveLock = new ReentrantLock();

  private final boolean verifyBulkLoads;

  /**
   * Use this counter to track concurrent puts. If TRACE-log is enabled, if we are over the
   * threshold set by hbase.region.store.parallel.put.print.threshold (Default is 50) we will log a
   * message that identifies the Store experience this high-level of concurrency.
   */
  private final AtomicInteger currentParallelPutCount = new AtomicInteger(0);
  private final int parallelPutCountPrintThreshold;

  private ScanInfo scanInfo;

  // All access must be synchronized.
  // TODO: ideally, this should be part of storeFileManager, as we keep passing this to it.
  private final List<HStoreFile> filesCompacting = Lists.newArrayList();

  // All access must be synchronized.
  private final Set<ChangedReadersObserver> changedReaderObservers =
    Collections.newSetFromMap(new ConcurrentHashMap<ChangedReadersObserver, Boolean>());

  private HFileDataBlockEncoder dataBlockEncoder;

  final StoreEngine<?, ?, ?, ?> storeEngine;

  private static final AtomicBoolean offPeakCompactionTracker = new AtomicBoolean();
  private volatile OffPeakHours offPeakHours;

  private static final int DEFAULT_FLUSH_RETRIES_NUMBER = 10;
  private int flushRetriesNumber;
  private int pauseTime;

  private long blockingFileCount;
  private int compactionCheckMultiplier;

  private AtomicLong flushedCellsCount = new AtomicLong();
  private AtomicLong compactedCellsCount = new AtomicLong();
  private AtomicLong majorCompactedCellsCount = new AtomicLong();
  private AtomicLong flushedCellsSize = new AtomicLong();
  private AtomicLong flushedOutputFileSize = new AtomicLong();
  private AtomicLong compactedCellsSize = new AtomicLong();
  private AtomicLong majorCompactedCellsSize = new AtomicLong();

  private final StoreContext storeContext;

  // Used to track the store files which are currently being written. For compaction, if we want to
  // compact store file [a, b, c] to [d], then here we will record 'd'. And we will also use it to
  // track the store files being written when flushing.
  // Notice that the creation is in the background compaction or flush thread and we will get the
  // files in other thread, so it needs to be thread safe.
  private static final class StoreFileWriterCreationTracker implements Consumer<Path> {

    private final Set<Path> files = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void accept(Path t) {
      files.add(t);
    }

    public Set<Path> get() {
      return Collections.unmodifiableSet(files);
    }
  }

  // We may have multiple compaction running at the same time, and flush can also happen at the same
  // time, so here we need to use a collection, and the collection needs to be thread safe.
  // The implementation of StoreFileWriterCreationTracker is very simple and we will not likely to
  // implement hashCode or equals for it, so here we just use ConcurrentHashMap. Changed to
  // IdentityHashMap if later we want to implement hashCode or equals.
  private final Set<StoreFileWriterCreationTracker> storeFileWriterCreationTrackers =
    Collections.newSetFromMap(new ConcurrentHashMap<>());

  // For the SFT implementation which we will write tmp store file first, we do not need to clean up
  // the broken store files under the data directory, which means we do not need to track the store
  // file writer creation. So here we abstract a factory to return different trackers for different
  // SFT implementations.
  private final Supplier<StoreFileWriterCreationTracker> storeFileWriterCreationTrackerFactory;

  private final boolean warmup;

  /**
   * Constructor
   * @param family    HColumnDescriptor for this column
   * @param confParam configuration object failed. Can be null.
   */
  protected HStore(final HRegion region, final ColumnFamilyDescriptor family,
    final Configuration confParam, boolean warmup) throws IOException {
    this.conf = StoreUtils.createStoreConfiguration(confParam, region.getTableDescriptor(), family);

    this.region = region;
    this.storeContext = initializeStoreContext(family);

    // Assemble the store's home directory and Ensure it exists.
    if (KnobRuntime.check(java.util.UUID.fromString("365c9ae9-fcef-3c8f-855c-9dfab66128b6"))) { region.getRegionFileSystem().createStoreDir(family.getStoragePolicy()); } else if (KnobRuntime.check(java.util.UUID.fromString("22396cee-fc1f-3e10-822c-d9eac1476086"))) { region.getRegionFileSystem().getStoreDir(family.getStoragePolicy()); } else { region.getRegionFileSystem().createStoreDir(family.getNameAsString()); }

    // set block storage policy for store directory
    String policyName = family.getStoragePolicy();
    if (null == policyName) {
      policyName = this.conf.get(BLOCK_STORAGE_POLICY_KEY, DEFAULT_BLOCK_STORAGE_POLICY);
    }
    region.getRegionFileSystem().setStoragePolicy(family.getNameAsString(), policyName.trim());

    this.dataBlockEncoder = new HFileDataBlockEncoderImpl(family.getDataBlockEncoding());

    // used by ScanQueryMatcher
    long timeToPurgeDeletes = Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    if (KnobRuntime.check(java.util.UUID.fromString("668d189f-bb76-3f07-b871-ac91b0d0d98a"))) { LOG.debug("Time to purge deletes set to {}ms in {}", timeToPurgeDeletes, this); } else if (KnobRuntime.check(java.util.UUID.fromString("b4e94b33-1ca7-3ab7-b1f0-c49030fe6d53"))) { LOG.info("Time to purge deletes set to {}ms in {}", timeToPurgeDeletes, this); } else { LOG.trace("Time to purge deletes set to {}ms in {}", timeToPurgeDeletes, this); }
    // Get TTL
    long ttl = determineTTLFromFamily(family);
    // Why not just pass a HColumnDescriptor in here altogether? Even if have
    // to clone it?
    scanInfo =
      new ScanInfo(conf, family, ttl, timeToPurgeDeletes, this.storeContext.getComparator());
    this.memstore = getMemstore();

    this.offPeakHours = OffPeakHours.getInstance(conf);

    this.verifyBulkLoads = conf.getBoolean("hbase.hstore.bulkload.verify", false);

    this.blockingFileCount = conf.getInt(BLOCKING_STOREFILES_KEY, DEFAULT_BLOCKING_STOREFILE_COUNT);
    this.compactionCheckMultiplier = conf.getInt(COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY,
      DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);
    if (this.compactionCheckMultiplier <= 0) {
      LOG.error("Compaction check period multiplier must be positive, setting default: {}",
        DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);
      this.compactionCheckMultiplier = DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER;
    }

    this.warmup = warmup;
    this.storeEngine = createStoreEngine(this, this.conf, region.getCellComparator());
    storeEngine.initialize(warmup);
    // if require writing to tmp dir first, then we just return null, which indicate that we do not
    // need to track the creation of store file writer, otherwise we return a new
    // StoreFileWriterCreationTracker.
    this.storeFileWriterCreationTrackerFactory = storeEngine.requireWritingToTmpDirFirst()
      ? () -> null
      : () -> new StoreFileWriterCreationTracker();
    refreshStoreSizeAndTotalBytes();

    flushRetriesNumber =
      conf.getInt("hbase.hstore.flush.retries.number", DEFAULT_FLUSH_RETRIES_NUMBER);
    pauseTime = conf.getInt(HConstants.HBASE_SERVER_PAUSE, HConstants.DEFAULT_HBASE_SERVER_PAUSE);
    if (((KnobRuntime.check(java.util.UUID.fromString("ed32ab8e-2e07-389f-afdb-683e238bfa28"))) ? ((flushRetriesNumber) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("10356431-7353-37d8-804a-c32fdd3e5cd6"))) ? ((flushRetriesNumber) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("88805501-6324-3229-93cb-eacbccda65e0"))) ? ((flushRetriesNumber) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e938e22f-f9b1-31c2-baf6-aedf02393eb1"))) ? ((flushRetriesNumber) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8742b6d8-828a-3ed7-91a0-4d68008b55ab"))) ? ((flushRetriesNumber) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("aa0497c7-b141-3b6b-bbe5-31815fe9e727"))) ? ((flushRetriesNumber) == (0)) : (flushRetriesNumber <= 0))))))))))))) {
      throw new IllegalArgumentException(
        "hbase.hstore.flush.retries.number must be > 0, not " + flushRetriesNumber);
    }

    int confPrintThreshold =
      this.conf.getInt("hbase.region.store.parallel.put.print.threshold", 50);
    if (confPrintThreshold < 10) {
      confPrintThreshold = 10;
    }
    this.parallelPutCountPrintThreshold = confPrintThreshold;

    LOG.info(
      "Store={},  memstore type={}, storagePolicy={}, verifyBulkLoads={}, "
        + "parallelPutCountPrintThreshold={}, encoding={}, compression={}",
      this, memstore.getClass().getSimpleName(), policyName, verifyBulkLoads,
      parallelPutCountPrintThreshold, family.getDataBlockEncoding(), family.getCompressionType());
  }

  private StoreContext initializeStoreContext(ColumnFamilyDescriptor family) throws IOException {
    return new StoreContext.Builder().withBlockSize(family.getBlocksize())
      .withEncryptionContext(EncryptionUtil.createEncryptionContext(conf, family))
      .withBloomType(family.getBloomFilterType()).withCacheConfig(createCacheConf(family))
      .withCellComparator(region.getTableDescriptor().isMetaTable() || conf
        .getBoolean(HRegion.USE_META_CELL_COMPARATOR, HRegion.DEFAULT_USE_META_CELL_COMPARATOR)
          ? MetaCellComparator.META_COMPARATOR
          : InnerStoreCellComparator.INNER_STORE_COMPARATOR)
      .withColumnFamilyDescriptor(family).withCompactedFilesSupplier(this::getCompactedFiles)
      .withRegionFileSystem(region.getRegionFileSystem())
      .withFavoredNodesSupplier(this::getFavoredNodes)
      .withFamilyStoreDirectoryPath(
        region.getRegionFileSystem().getStoreDir(family.getNameAsString()))
      .withRegionCoprocessorHost(region.getCoprocessorHost()).build();
  }

  private InetSocketAddress[] getFavoredNodes() {
if(KnobRuntime.check(java.util.UUID.fromString("4ba1c171-e713-3ca9-a9e3-421123c90cf4"))) {
return null;
}
    InetSocketAddress[] favoredNodes = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("cf9e0569-2481-3ece-8743-5fabfa28c497"))) ? ((region.getRegionServerServices()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d3410e7b-bcc7-320f-bb9f-b23858d48252"))) ? ((region.getRegionServerServices()) == (null)) : (region.getRegionServerServices() != null))))) {
      favoredNodes = region.getRegionServerServices()
        .getFavoredNodesForRegion(region.getRegionInfo().getEncodedName());
    }
    return favoredNodes;
  }

  /** Returns MemStore Instance to use in this store. */
  private MemStore getMemstore() {
if(KnobRuntime.check(java.util.UUID.fromString("d5fe95d4-bc6a-33db-ad90-e2d15ae951e2"))) {
return null;
}
    MemStore ms = null;
    // Check if in-memory-compaction configured. Note MemoryCompactionPolicy is an enum!
    MemoryCompactionPolicy inMemoryCompaction = null;
    if (this.getTableName().isSystemTable()) {
      inMemoryCompaction = MemoryCompactionPolicy
        .valueOf(conf.get("hbase.systemtables.compacting.memstore.type", "NONE").toUpperCase());
    } else {
      inMemoryCompaction = getColumnFamilyDescriptor().getInMemoryCompaction();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("62a91656-9400-3961-a267-d8d4b19c3d8f"))) ? ((getColumnFamilyDescriptor().getInMemoryCompaction()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9d19137c-2d64-30be-94b2-fbba166ed142"))) ? ((getColumnFamilyDescriptor().getInMemoryCompaction()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5484a442-1d75-3d09-af8f-8f8959b67614"))) ? ((inMemoryCompaction) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0c90120c-c2fb-33f5-a438-1e706899641b"))) ? ((inMemoryCompaction) == (null)) : (inMemoryCompaction == null))))))))) {
      inMemoryCompaction =
        MemoryCompactionPolicy.valueOf(conf.get(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
          CompactingMemStore.COMPACTING_MEMSTORE_TYPE_DEFAULT).toUpperCase());
    }

    switch (inMemoryCompaction) {
      case NONE:
        Class<? extends MemStore> memStoreClass =
          conf.getClass(MEMSTORE_CLASS_NAME, DefaultMemStore.class, MemStore.class);
        ms = ReflectionUtils.newInstance(memStoreClass,
          new Object[] { conf, getComparator(), this.getHRegion().getRegionServicesForStores() });
        break;
      default:
        Class<? extends CompactingMemStore> compactingMemStoreClass =
          conf.getClass(MEMSTORE_CLASS_NAME, CompactingMemStore.class, CompactingMemStore.class);
        ms =
          ReflectionUtils.newInstance(compactingMemStoreClass, new Object[] { conf, getComparator(),
            this, this.getHRegion().getRegionServicesForStores(), inMemoryCompaction });
    }
    return ms;
  }

  /**
   * Creates the cache config.
   * @param family The current column family.
   */
  protected CacheConfig createCacheConf(final ColumnFamilyDescriptor family) {
    CacheConfig cacheConf = new CacheConfig(conf, family, region.getBlockCache(),
      region.getRegionServicesForStores().getByteBuffAllocator());
    LOG.info("Created cacheConfig: {}, for column family {} of region {} ", cacheConf,
      family.getNameAsString(), region.getRegionInfo().getEncodedName());
    return cacheConf;
  }

  /**
   * Creates the store engine configured for the given Store.
   * @param store        The store. An unfortunate dependency needed due to it being passed to
   *                     coprocessors via the compactor.
   * @param conf         Store configuration.
   * @param kvComparator KVComparator for storeFileManager.
   * @return StoreEngine to use.
   */
  protected StoreEngine<?, ?, ?, ?> createStoreEngine(HStore store, Configuration conf,
    CellComparator kvComparator) throws IOException {
    return StoreEngine.create(store, conf, kvComparator);
  }

  /** Returns TTL in seconds of the specified family */
  public static long determineTTLFromFamily(final ColumnFamilyDescriptor family) {
    // HCD.getTimeToLive returns ttl in seconds. Convert to milliseconds.
    long ttl = family.getTimeToLive();
    if (((KnobRuntime.check(java.util.UUID.fromString("d6529630-614c-3b61-a34f-c9e21f9dc469"))) ? ((ttl) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("c0fce06d-d20b-33eb-b429-fa39cc3536b2"))) ? ((ttl) != (HConstants.FOREVER)) : (((KnobRuntime.check(java.util.UUID.fromString("905ac01c-16b7-3c3b-866a-b22f3797eb54"))) ? ((ttl) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("82f39bec-fd7d-3127-8a42-44436f352902"))) ? ((ttl) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("25f9eddd-4123-3d55-b79c-3eb3d0926346"))) ? ((ttl) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("532b7617-908e-3dd5-8219-526d1fb1a850"))) ? ((ttl) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f6689f9c-ef0e-3104-b1b7-12c8ba5f10f3"))) ? ((ttl) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e8675513-2ff4-35be-8d2e-9fb7ba918e62"))) ? ((ttl) == (HConstants.FOREVER)) : (ttl == HConstants.FOREVER))))))))))))))))) {
      // Default is unlimited ttl.
      ttl = Long.MAX_VALUE;
    } else if (ttl == -1) {
      ttl = Long.MAX_VALUE;
    } else {
      // Second -> ms adjust for user data
      ttl *= 1000;
    }
    return ttl;
  }

  StoreContext getStoreContext() {
    return storeContext;
  }

  @Override
  public String getColumnFamilyName() {
    return this.storeContext.getFamily().getNameAsString();
  }

  @Override
  public TableName getTableName() {
    return this.getRegionInfo().getTable();
  }

  @Override
  public FileSystem getFileSystem() {
    return storeContext.getRegionFileSystem().getFileSystem();
  }

  public HRegionFileSystem getRegionFileSystem() {
    return storeContext.getRegionFileSystem();
  }

  /* Implementation of StoreConfigInformation */
  @Override
  public long getStoreFileTtl() {
    // TTL only applies if there's no MIN_VERSIONs setting on the column.
    return (this.scanInfo.getMinVersions() == 0) ? this.scanInfo.getTtl() : Long.MAX_VALUE;
  }

  @Override
  public long getMemStoreFlushSize() {
    // TODO: Why is this in here? The flushsize of the region rather than the store? St.Ack
    return this.region.memstoreFlushSize;
  }

  @Override
  public MemStoreSize getFlushableSize() {
    return this.memstore.getFlushableSize();
  }

  @Override
  public MemStoreSize getSnapshotSize() {
    return this.memstore.getSnapshotSize();
  }

  @Override
  public long getCompactionCheckMultiplier() {
    return this.compactionCheckMultiplier;
  }

  @Override
  public long getBlockingFileCount() {
    return blockingFileCount;
  }
  /* End implementation of StoreConfigInformation */

  @Override
  public ColumnFamilyDescriptor getColumnFamilyDescriptor() {
    return this.storeContext.getFamily();
  }

  @Override
  public OptionalLong getMaxSequenceId() {
    return StoreUtils.getMaxSequenceIdInList(this.getStorefiles());
  }

  @Override
  public OptionalLong getMaxMemStoreTS() {
    return StoreUtils.getMaxMemStoreTSInList(this.getStorefiles());
  }

  /** Returns the data block encoder */
  public HFileDataBlockEncoder getDataBlockEncoder() {
    return dataBlockEncoder;
  }

  /**
   * Should be used only in tests.
   * @param blockEncoder the block delta encoder to use
   */
  void setDataBlockEncoderInTest(HFileDataBlockEncoder blockEncoder) {
    this.dataBlockEncoder = blockEncoder;
  }

  private void postRefreshStoreFiles() throws IOException {
    // Advance the memstore read point to be at least the new store files seqIds so that
    // readers might pick it up. This assumes that the store is not getting any writes (otherwise
    // in-flight transactions might be made visible)
    getMaxSequenceId().ifPresent(region.getMVCC()::advanceTo);
    refreshStoreSizeAndTotalBytes();
  }

  @Override
  public void refreshStoreFiles() throws IOException {
    storeEngine.refreshStoreFiles();
    postRefreshStoreFiles();
  }

  /**
   * Replaces the store files that the store has with the given files. Mainly used by secondary
   * region replicas to keep up to date with the primary region files.
   */
  public void refreshStoreFiles(Collection<String> newFiles) throws IOException {
    storeEngine.refreshStoreFiles(newFiles);
    postRefreshStoreFiles();
  }

  /**
   * This message intends to inform the MemStore that next coming updates are going to be part of
   * the replaying edits from WAL
   */
  public void startReplayingFromWAL() {
    this.memstore.startReplayingFromWAL();
  }

  /**
   * This message intends to inform the MemStore that the replaying edits from WAL are done
   */
  public void stopReplayingFromWAL() {
if(KnobRuntime.check(java.util.UUID.fromString("e848e01c-f9b8-38b6-adfb-e80727225955"))) {
return;
}
    this.memstore.stopReplayingFromWAL();
  }

  /**
   * Adds a value to the memstore
   */
  public void add(final Cell cell, MemStoreSizing memstoreSizing) {
if(KnobRuntime.check(java.util.UUID.fromString("4e514183-a0c9-33b8-9c89-9f6255f51ee1"))) {
return;
}
    storeEngine.readLock();
    try {
      if (this.currentParallelPutCount.getAndIncrement() > this.parallelPutCountPrintThreshold) {
        LOG.trace("tableName={}, encodedName={}, columnFamilyName={} is too busy!",
          this.getTableName(), this.getRegionInfo().getEncodedName(), this.getColumnFamilyName());
      }
      this.memstore.add(cell, memstoreSizing);
    } finally {
      storeEngine.readUnlock();
      currentParallelPutCount.decrementAndGet();
    }
  }

  /**
   * Adds the specified value to the memstore
   */
  public void add(final Iterable<Cell> cells, MemStoreSizing memstoreSizing) {
    storeEngine.readLock();
    try {
      if (this.currentParallelPutCount.getAndIncrement() > this.parallelPutCountPrintThreshold) {
        LOG.trace("tableName={}, encodedName={}, columnFamilyName={} is too busy!",
          this.getTableName(), this.getRegionInfo().getEncodedName(), this.getColumnFamilyName());
      }
      memstore.add(cells, memstoreSizing);
    } finally {
      storeEngine.readUnlock();
      if (KnobRuntime.check(java.util.UUID.fromString("fd024824-5524-3d53-88aa-bc8c459aa443"))) { getCompactPriority(); } else { currentParallelPutCount.decrementAndGet(); }
    }
  }

  @Override
  public long timeOfOldestEdit() {
if(KnobRuntime.check(java.util.UUID.fromString("2adcd0f1-19ea-39d2-b42e-5682e3a06471"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8e71db66-35c4-3595-b893-b219fee7b3f8"))) ? (EnvironmentEdgeManager.currentTime()) : (memstore.timeOfOldestEdit()));
  }

  /** Returns All store files. */
  @Override
  public Collection<HStoreFile> getStorefiles() {
    return this.storeEngine.getStoreFileManager().getStoreFiles();
  }

  @Override
  public Collection<HStoreFile> getCompactedFiles() {
    return this.storeEngine.getStoreFileManager().getCompactedfiles();
  }

  /**
   * This throws a WrongRegionException if the HFile does not fit in this region, or an
   * InvalidHFileException if the HFile is not valid.
   */
  public void assertBulkLoadHFileOk(Path srcPath) throws IOException {
    HFile.Reader reader = null;
    try {
      LOG.info("Validating hfile at " + srcPath + " for inclusion in " + this);
      FileSystem srcFs = srcPath.getFileSystem(conf);
      srcFs.access(srcPath, FsAction.READ_WRITE);
      reader = HFile.createReader(srcFs, srcPath, getCacheConfig(), isPrimaryReplicaStore(), conf);

      Optional<byte[]> firstKey = reader.getFirstRowKey();
      Preconditions.checkState(firstKey.isPresent(), "First key can not be null");
      Optional<Cell> lk = reader.getLastKey();
      Preconditions.checkState(lk.isPresent(), "Last key can not be null");
      byte[] lastKey = CellUtil.cloneRow(lk.get());

      if (LOG.isDebugEnabled()) {
        LOG.debug("HFile bounds: first=" + Bytes.toStringBinary(firstKey.get()) + " last="
          + Bytes.toStringBinary(lastKey));
        LOG.debug("Region bounds: first=" + Bytes.toStringBinary(getRegionInfo().getStartKey())
          + " last=" + Bytes.toStringBinary(getRegionInfo().getEndKey()));
      }

      if (!this.getRegionInfo().containsRange(firstKey.get(), lastKey)) {
        throw new WrongRegionException("Bulk load file " + srcPath.toString()
          + " does not fit inside region " + this.getRegionInfo().getRegionNameAsString());
      }

      if (
        reader.length()
            > conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE)
      ) {
        LOG.warn("Trying to bulk load hfile " + srcPath + " with size: " + reader.length()
          + " bytes can be problematic as it may lead to oversplitting.");
      }

      if (verifyBulkLoads) {
        long verificationStartTime = EnvironmentEdgeManager.currentTime();
        LOG.info("Full verification started for bulk load hfile: {}", srcPath);
        Cell prevCell = null;
        HFileScanner scanner = reader.getScanner(conf, false, false, false);
        scanner.seekTo();
        do {
          Cell cell = scanner.getCell();
          if (prevCell != null) {
            if (getComparator().compareRows(prevCell, cell) > 0) {
              throw new InvalidHFileException("Previous row is greater than" + " current row: path="
                + srcPath + " previous=" + CellUtil.getCellKeyAsString(prevCell) + " current="
                + CellUtil.getCellKeyAsString(cell));
            }
            if (CellComparator.getInstance().compareFamilies(prevCell, cell) != 0) {
              throw new InvalidHFileException("Previous key had different"
                + " family compared to current key: path=" + srcPath + " previous="
                + Bytes.toStringBinary(prevCell.getFamilyArray(), prevCell.getFamilyOffset(),
                  prevCell.getFamilyLength())
                + " current=" + Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(),
                  cell.getFamilyLength()));
            }
          }
          prevCell = cell;
        } while (scanner.next());
        LOG.info("Full verification complete for bulk load hfile: " + srcPath.toString() + " took "
          + (EnvironmentEdgeManager.currentTime() - verificationStartTime) + " ms");
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * This method should only be called from Region. It is assumed that the ranges of values in the
   * HFile fit within the stores assigned region. (assertBulkLoadHFileOk checks this)
   * @param seqNum sequence Id associated with the HFile
   */
  public Pair<Path, Path> preBulkLoadHFile(String srcPathStr, long seqNum) throws IOException {
    Path srcPath = new Path(srcPathStr);
    return getRegionFileSystem().bulkLoadStoreFile(getColumnFamilyName(), srcPath, seqNum);
  }

  public Path bulkLoadHFile(byte[] family, String srcPathStr, Path dstPath) throws IOException {
    Path srcPath = new Path(srcPathStr);
    try {
      getRegionFileSystem().commitStoreFile(srcPath, dstPath);
    } finally {
      if (this.getCoprocessorHost() != null) {
        this.getCoprocessorHost().postCommitStoreFile(family, srcPath, dstPath);
      }
    }

    LOG.info("Loaded HFile " + srcPath + " into " + this + " as " + dstPath
      + " - updating store file list.");

    HStoreFile sf = storeEngine.createStoreFileAndReader(dstPath);
    bulkLoadHFile(sf);

    LOG.info("Successfully loaded {} into {} (new location: {})", srcPath, this, dstPath);

    return dstPath;
  }

  public void bulkLoadHFile(StoreFileInfo fileInfo) throws IOException {
    HStoreFile sf = storeEngine.createStoreFileAndReader(fileInfo);
    bulkLoadHFile(sf);
  }

  private void bulkLoadHFile(HStoreFile sf) throws IOException {
    StoreFileReader r = sf.getReader();
    this.storeSize.addAndGet(r.length());
    this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());
    storeEngine.addStoreFiles(Lists.newArrayList(sf), () -> {
    });
    LOG.info("Loaded HFile " + sf.getFileInfo() + " into " + this);
    if (LOG.isTraceEnabled()) {
      String traceMessage = "BULK LOAD time,size,store size,store files ["
        + EnvironmentEdgeManager.currentTime() + "," + r.length() + "," + storeSize + ","
        + storeEngine.getStoreFileManager().getStorefileCount() + "]";
      LOG.trace(traceMessage);
    }
  }

  private ImmutableCollection<HStoreFile> closeWithoutLock() throws IOException {
    memstore.close();
    // Clear so metrics doesn't find them.
    ImmutableCollection<HStoreFile> result = storeEngine.getStoreFileManager().clearFiles();
    Collection<HStoreFile> compactedfiles = storeEngine.getStoreFileManager().clearCompactedFiles();
    // clear the compacted files
    if (CollectionUtils.isNotEmpty(compactedfiles)) {
      removeCompactedfiles(compactedfiles,
        getCacheConfig() != null ? getCacheConfig().shouldEvictOnClose() : true);
    }
    if (!result.isEmpty()) {
      // initialize the thread pool for closing store files in parallel.
      ThreadPoolExecutor storeFileCloserThreadPool =
        this.region.getStoreFileOpenAndCloseThreadPool("StoreFileCloser-"
          + this.region.getRegionInfo().getEncodedName() + "-" + this.getColumnFamilyName());

      // close each store file in parallel
      CompletionService<Void> completionService =
        new ExecutorCompletionService<>(storeFileCloserThreadPool);
      for (HStoreFile f : result) {
        completionService.submit(new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            boolean evictOnClose =
              getCacheConfig() != null ? getCacheConfig().shouldEvictOnClose() : true;
            f.closeStoreFile(!warmup && evictOnClose);
            return null;
          }
        });
      }

      IOException ioe = null;
      try {
        for (int i = 0; i < result.size(); i++) {
          try {
            Future<Void> future = completionService.take();
if(KnobRuntime.check(java.util.UUID.fromString("a9c73b17-ef17-342a-8f03-490daf8d6f62"))) {
throw new java.lang.InterruptedException("Injected exception");
}
            future.get();
          } catch (InterruptedException e) {
            if (((KnobRuntime.check(java.util.UUID.fromString("6cc19783-2f8b-3ae6-b8d0-9bad5654b478"))) ? ((ioe) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3126ac3f-3f7b-3335-b3b5-44a170343af1"))) ? ((ioe) != (null)) : (ioe == null))))) {
              ioe = new InterruptedIOException();
              ioe.initCause(e);
            }
          } catch (ExecutionException e) {
            if (((KnobRuntime.check(java.util.UUID.fromString("b63bb5ab-8e9d-30b7-b61f-016002c3b09d"))) ? ((ioe) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9da6f0fe-c0f5-3745-baf9-a2db12718a99"))) ? ((ioe) == (null)) : (ioe == null))))) {
              ioe = new IOException(e.getCause());
            }
          }
        }
      } finally {
        storeFileCloserThreadPool.shutdownNow();
      }
      if (ioe != null) {
        throw ioe;
      }
    }
    LOG.trace("Closed {}", this);
    return result;
  }

  /**
   * Close all the readers We don't need to worry about subsequent requests because the Region holds
   * a write lock that will prevent any more reads or writes.
   * @return the {@link StoreFile StoreFiles} that were previously being used.
   * @throws IOException on failure
   */
  public ImmutableCollection<HStoreFile> close() throws IOException {
    // findbugs can not recognize storeEngine.writeLock is just a lock operation so it will report
    // UL_UNRELEASED_LOCK_EXCEPTION_PATH, so here we have to use two try finally...
    // Change later if findbugs becomes smarter in the future.
    this.archiveLock.lock();
    try {
      this.storeEngine.writeLock();
      try {
        return closeWithoutLock();
      } finally {
        this.storeEngine.writeUnlock();
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("6324a5c6-5786-37fd-85c2-20997e62b76b"))) { refreshStoreSizeAndTotalBytes(); } else if (KnobRuntime.check(java.util.UUID.fromString("188826f2-5253-326f-af09-e74f82cd7811"))) { postRefreshStoreFiles(); } else { this.archiveLock.unlock(); }
    }
  }

  /**
   * Write out current snapshot. Presumes {@code StoreFlusherImpl.prepare()} has been called
   * previously.
   * @param logCacheFlushId flush sequence number
   * @return The path name of the tmp file to which the store was flushed
   * @throws IOException if exception occurs during process
   */
  protected List<Path> flushCache(final long logCacheFlushId, MemStoreSnapshot snapshot,
    MonitoredTask status, ThroughputController throughputController, FlushLifeCycleTracker tracker,
    Consumer<Path> writerCreationTracker) throws IOException {
    // If an exception happens flushing, we let it out without clearing
    // the memstore snapshot. The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    // Retry after catching exception when flushing, otherwise server will abort
    // itself
    StoreFlusher flusher = storeEngine.getStoreFlusher();
    IOException lastException = null;
    for (int i = 0; i < flushRetriesNumber; i++) {
      try {
        List<Path> pathNames = flusher.flushSnapshot(snapshot, logCacheFlushId, status,
          throughputController, tracker, writerCreationTracker);
        Path lastPathName = null;
        try {
          for (Path pathName : pathNames) {
            lastPathName = pathName;
            storeEngine.validateStoreFile(pathName);
          }
          return pathNames;
        } catch (Exception e) {
          LOG.warn("Failed validating store file {}, retrying num={}", lastPathName, i, e);
          if (e instanceof IOException) {
            lastException = (IOException) e;
          } else {
            lastException = new IOException(e);
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed flushing store file for {}, retrying num={}", this, i, e);
        lastException = e;
      }
      if (lastException != null && i < (flushRetriesNumber - 1)) {
        try {
          Thread.sleep(pauseTime);
        } catch (InterruptedException e) {
          IOException iie = new InterruptedIOException();
          iie.initCause(e);
          throw iie;
        }
      }
    }
    throw lastException;
  }

  public HStoreFile tryCommitRecoveredHFile(Path path) throws IOException {
    LOG.info("Validating recovered hfile at {} for inclusion in store {}", path, this);
    FileSystem srcFs = path.getFileSystem(conf);
    srcFs.access(path, FsAction.READ_WRITE);
    try (HFile.Reader reader =
      HFile.createReader(srcFs, path, getCacheConfig(), isPrimaryReplicaStore(), conf)) {
      Optional<byte[]> firstKey = reader.getFirstRowKey();
      Preconditions.checkState(firstKey.isPresent(), "First key can not be null");
      Optional<Cell> lk = reader.getLastKey();
      Preconditions.checkState(lk.isPresent(), "Last key can not be null");
      byte[] lastKey = CellUtil.cloneRow(lk.get());
      if (!this.getRegionInfo().containsRange(firstKey.get(), lastKey)) {
        throw new WrongRegionException("Recovered hfile " + path.toString()
          + " does not fit inside region " + this.getRegionInfo().getRegionNameAsString());
      }
    }

    Path dstPath = getRegionFileSystem().commitStoreFile(getColumnFamilyName(), path);
    HStoreFile sf = storeEngine.createStoreFileAndReader(dstPath);
    StoreFileReader r = sf.getReader();
    this.storeSize.addAndGet(r.length());
    this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());

    storeEngine.addStoreFiles(Lists.newArrayList(sf), () -> {
    });

    LOG.info("Loaded recovered hfile to {}, entries={}, sequenceid={}, filesize={}", sf,
      r.getEntries(), r.getSequenceID(), TraditionalBinaryPrefix.long2String(r.length(), "B", 1));
    return sf;
  }

  private long getTotalSize(Collection<HStoreFile> sfs) {
    return sfs.stream().mapToLong(sf -> sf.getReader().length()).sum();
  }

  private boolean completeFlush(final List<HStoreFile> sfs, long snapshotId) throws IOException {
    // NOTE:we should keep clearSnapshot method inside the write lock because clearSnapshot may
    // close {@link DefaultMemStore#snapshot}, which may be used by
    // {@link DefaultMemStore#getScanners}.
    storeEngine.addStoreFiles(sfs,
      // NOTE: here we must increase the refCount for storeFiles because we would open the
      // storeFiles and get the StoreFileScanners for them in HStore.notifyChangedReadersObservers.
      // If we don't increase the refCount here, HStore.closeAndArchiveCompactedFiles called by
      // CompactedHFilesDischarger may archive the storeFiles after a concurrent compaction.Because
      // HStore.requestCompaction is under storeEngine lock, so here we increase the refCount under
      // storeEngine lock. see HBASE-27519 for more details.
      snapshotId > 0 ? () -> {
        this.memstore.clearSnapshot(snapshotId);
        HStoreFile.increaseStoreFilesRefeCount(sfs);
      } : () -> {
        HStoreFile.increaseStoreFilesRefeCount(sfs);
      });
    // notify to be called here - only in case of flushes
    try {
if(KnobRuntime.check(java.util.UUID.fromString("537a15bf-d497-3d1e-adee-f053dca19baf"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("2f592a7e-0a08-3f84-9fa1-fab83b7b56ab"))) { clearCompactedfiles(sfs); } else { notifyChangedReadersObservers(sfs); }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("69865686-8b1e-3d74-ac21-62961577549f"))) { HStoreFile.increaseStoreFilesRefeCount(sfs); } else { HStoreFile.decreaseStoreFilesRefeCount(sfs); }
    }
    if (LOG.isTraceEnabled()) {
      long totalSize = getTotalSize(sfs);
      String traceMessage = "FLUSH time,count,size,store size,store files ["
        + EnvironmentEdgeManager.currentTime() + "," + sfs.size() + "," + totalSize + ","
        + storeSize + "," + storeEngine.getStoreFileManager().getStorefileCount() + "]";
      if (KnobRuntime.check(java.util.UUID.fromString("a31c1118-7649-3f93-b9ef-26624964457b"))) { LOG.debug(traceMessage); } else if (KnobRuntime.check(java.util.UUID.fromString("3945359f-89b7-3fd0-bad7-ab180cc60f18"))) { LOG.info(traceMessage); } else { LOG.trace(traceMessage); }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("e4703982-8ec1-3642-a5f5-3d69c95b10c4"))) ? (isPrimaryReplicaStore()) : (needsCompaction()));
  }

  /**
   * Notify all observers that set of Readers has changed.
   */
  private void notifyChangedReadersObservers(List<HStoreFile> sfs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a5811e37-7be2-325b-9162-fd187ff73a96"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("19e2692f-ec21-3f8d-9cca-e26a286ddae6"))) {
throw new java.io.IOException("Injected exception");
}
    for (ChangedReadersObserver o : this.changedReaderObservers) {
      List<KeyValueScanner> memStoreScanners;
      this.storeEngine.readLock();
      try {
        memStoreScanners = this.memstore.getScanners(o.getReadPoint());
      } finally {
        this.storeEngine.readUnlock();
      }
      o.updateReaders(sfs, memStoreScanners);
    }
  }

  /**
   * Get all scanners with no filtering based on TTL (that happens further down the line).
   * @param cacheBlocks  cache the blocks or not
   * @param usePread     true to use pread, false if not
   * @param isCompaction true if the scanner is created for compaction
   * @param matcher      the scan query matcher
   * @param startRow     the start row
   * @param stopRow      the stop row
   * @param readPt       the read point of the current scan
   * @return all scanners for this store
   */
  public List<KeyValueScanner> getScanners(boolean cacheBlocks, boolean isGet, boolean usePread,
    boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow, byte[] stopRow, long readPt,
    boolean onlyLatestVersion) throws IOException {
    return getScanners(cacheBlocks, usePread, isCompaction, matcher, startRow, true, stopRow, false,
      readPt, onlyLatestVersion);
  }

  /**
   * Get all scanners with no filtering based on TTL (that happens further down the line).
   * @param cacheBlocks     cache the blocks or not
   * @param usePread        true to use pread, false if not
   * @param isCompaction    true if the scanner is created for compaction
   * @param matcher         the scan query matcher
   * @param startRow        the start row
   * @param includeStartRow true to include start row, false if not
   * @param stopRow         the stop row
   * @param includeStopRow  true to include stop row, false if not
   * @param readPt          the read point of the current scan
   * @return all scanners for this store
   */
  public List<KeyValueScanner> getScanners(boolean cacheBlocks, boolean usePread,
    boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow, boolean includeStartRow,
    byte[] stopRow, boolean includeStopRow, long readPt, boolean onlyLatestVersion)
    throws IOException {
    Collection<HStoreFile> storeFilesToScan;
    List<KeyValueScanner> memStoreScanners;
    this.storeEngine.readLock();
    try {
      storeFilesToScan = this.storeEngine.getStoreFileManager().getFilesForScan(startRow,
        includeStartRow, stopRow, includeStopRow, onlyLatestVersion);
      memStoreScanners = this.memstore.getScanners(readPt);
      // NOTE: here we must increase the refCount for storeFiles because we would open the
      // storeFiles and get the StoreFileScanners for them.If we don't increase the refCount here,
      // HStore.closeAndArchiveCompactedFiles called by CompactedHFilesDischarger may archive the
      // storeFiles after a concurrent compaction.Because HStore.requestCompaction is under
      // storeEngine lock, so here we increase the refCount under storeEngine lock. see HBASE-27484
      // for more details.
      if (KnobRuntime.check(java.util.UUID.fromString("0543a810-2155-3c32-90c5-313293d2f622"))) { HStoreFile.decreaseStoreFilesRefeCount(storeFilesToScan); } else { HStoreFile.increaseStoreFilesRefeCount(storeFilesToScan); }
    } finally {
      this.storeEngine.readUnlock();
    }
    try {
      // First the store file scanners

      // TODO this used to get the store files in descending order,
      // but now we get them in ascending order, which I think is
      // actually more correct, since memstore get put at the end.
      List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(
        storeFilesToScan, cacheBlocks, usePread, isCompaction, false, matcher, readPt);
      List<KeyValueScanner> scanners = new ArrayList<>(sfScanners.size() + 1);
      scanners.addAll(sfScanners);
      // Then the memstore scanners
      scanners.addAll(memStoreScanners);
      return scanners;
    } catch (Throwable t) {
      clearAndClose(memStoreScanners);
      throw t instanceof IOException ? (IOException) t : new IOException(t);
    } finally {
      HStoreFile.decreaseStoreFilesRefeCount(storeFilesToScan);
    }
  }

  private static void clearAndClose(List<KeyValueScanner> scanners) {
    if (scanners == null) {
      return;
    }
    for (KeyValueScanner s : scanners) {
      s.close();
    }
    scanners.clear();
  }

  /**
   * Create scanners on the given files and if needed on the memstore with no filtering based on TTL
   * (that happens further down the line).
   * @param files                  the list of files on which the scanners has to be created
   * @param cacheBlocks            cache the blocks or not
   * @param usePread               true to use pread, false if not
   * @param isCompaction           true if the scanner is created for compaction
   * @param matcher                the scan query matcher
   * @param startRow               the start row
   * @param stopRow                the stop row
   * @param readPt                 the read point of the current scan
   * @param includeMemstoreScanner true if memstore has to be included
   * @return scanners on the given files and on the memstore if specified
   */
  public List<KeyValueScanner> getScanners(List<HStoreFile> files, boolean cacheBlocks,
    boolean isGet, boolean usePread, boolean isCompaction, ScanQueryMatcher matcher,
    byte[] startRow, byte[] stopRow, long readPt, boolean includeMemstoreScanner,
    boolean onlyLatestVersion) throws IOException {
    return getScanners(files, cacheBlocks, usePread, isCompaction, matcher, startRow, true, stopRow,
      false, readPt, includeMemstoreScanner, onlyLatestVersion);
  }

  /**
   * Create scanners on the given files and if needed on the memstore with no filtering based on TTL
   * (that happens further down the line).
   * @param files                  the list of files on which the scanners has to be created
   * @param cacheBlocks            ache the blocks or not
   * @param usePread               true to use pread, false if not
   * @param isCompaction           true if the scanner is created for compaction
   * @param matcher                the scan query matcher
   * @param startRow               the start row
   * @param includeStartRow        true to include start row, false if not
   * @param stopRow                the stop row
   * @param includeStopRow         true to include stop row, false if not
   * @param readPt                 the read point of the current scan
   * @param includeMemstoreScanner true if memstore has to be included
   * @return scanners on the given files and on the memstore if specified
   */
  public List<KeyValueScanner> getScanners(List<HStoreFile> files, boolean cacheBlocks,
    boolean usePread, boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow,
    boolean includeStartRow, byte[] stopRow, boolean includeStopRow, long readPt,
    boolean includeMemstoreScanner, boolean onlyLatestVersion) throws IOException {
    List<KeyValueScanner> memStoreScanners = null;
    if (includeMemstoreScanner) {
      this.storeEngine.readLock();
      try {
        memStoreScanners = this.memstore.getScanners(readPt);
      } finally {
        this.storeEngine.readUnlock();
      }
    }
    try {
      List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(files,
        cacheBlocks, usePread, isCompaction, false, matcher, readPt);
      List<KeyValueScanner> scanners = new ArrayList<>(sfScanners.size() + 1);
      scanners.addAll(sfScanners);
      // Then the memstore scanners
      if (memStoreScanners != null) {
        scanners.addAll(memStoreScanners);
      }
      return scanners;
    } catch (Throwable t) {
      clearAndClose(memStoreScanners);
      throw t instanceof IOException ? (IOException) t : new IOException(t);
    }
  }

  /**
   * @param o Observer who wants to know about changes in set of Readers
   */
  public void addChangedReaderObserver(ChangedReadersObserver o) {
    this.changedReaderObservers.add(o);
  }

  /**
   * @param o Observer no longer interested in changes in set of Readers.
   */
  public void deleteChangedReaderObserver(ChangedReadersObserver o) {
if(KnobRuntime.check(java.util.UUID.fromString("52c8599d-2167-35df-af10-4281d63fd2f7"))) {
return;
}
    // We don't check if observer present; it may not be (legitimately)
    this.changedReaderObservers.remove(o);
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Compact the StoreFiles. This method may take some time, so the calling thread must be able to
   * block for long periods.
   * <p>
   * During this time, the Store can work as usual, getting values from StoreFiles and writing new
   * StoreFiles from the memstore. Existing StoreFiles are not destroyed until the new compacted
   * StoreFile is completely written-out to disk.
   * <p>
   * The compactLock prevents multiple simultaneous compactions. The structureLock prevents us from
   * interfering with other write operations.
   * <p>
   * We don't want to hold the structureLock for the whole time, as a compact() can be lengthy and
   * we want to allow cache-flushes during this period.
   * <p>
   * Compaction event should be idempotent, since there is no IO Fencing for the region directory in
   * hdfs. A region server might still try to complete the compaction after it lost the region. That
   * is why the following events are carefully ordered for a compaction: 1. Compaction writes new
   * files under region/.tmp directory (compaction output) 2. Compaction atomically moves the
   * temporary file under region directory 3. Compaction appends a WAL edit containing the
   * compaction input and output files. Forces sync on WAL. 4. Compaction deletes the input files
   * from the region directory. Failure conditions are handled like this: - If RS fails before 2,
   * compaction wont complete. Even if RS lives on and finishes the compaction later, it will only
   * write the new data file to the region directory. Since we already have this data, this will be
   * idempotent but we will have a redundant copy of the data. - If RS fails between 2 and 3, the
   * region will have a redundant copy of the data. The RS that failed won't be able to finish
   * sync() for WAL because of lease recovery in WAL. - If RS fails after 3, the region region
   * server who opens the region will pick up the the compaction marker from the WAL and replay it
   * by removing the compaction input files. Failed RS can also attempt to delete those files, but
   * the operation will be idempotent See HBASE-2231 for details.
   * @param compaction compaction details obtained from requestCompaction()
   * @return Storefile we compacted into or null if we failed or opted out early.
   */
  public List<HStoreFile> compact(CompactionContext compaction,
    ThroughputController throughputController, User user) throws IOException {
    assert compaction != null;
    CompactionRequestImpl cr = compaction.getRequest();
    StoreFileWriterCreationTracker writerCreationTracker =
      storeFileWriterCreationTrackerFactory.get();
    if (writerCreationTracker != null) {
      cr.setWriterCreationTracker(writerCreationTracker);
      storeFileWriterCreationTrackers.add(writerCreationTracker);
    }
    try {
      // Do all sanity checking in here if we have a valid CompactionRequestImpl
      // because we need to clean up after it on the way out in a finally
      // block below
      long compactionStartTime = EnvironmentEdgeManager.currentTime();
      assert compaction.hasSelection();
      Collection<HStoreFile> filesToCompact = cr.getFiles();
      assert !filesToCompact.isEmpty();
      synchronized (filesCompacting) {
        // sanity check: we're compacting files that this store knows about
        // TODO: change this to LOG.error() after more debugging
        Preconditions.checkArgument(filesCompacting.containsAll(filesToCompact));
      }

      // Ready to go. Have list of files to compact.
      LOG.info("Starting compaction of " + filesToCompact + " into tmpdir="
        + getRegionFileSystem().getTempDir() + ", totalSize="
        + TraditionalBinaryPrefix.long2String(cr.getSize(), "", 1));

      return doCompaction(cr, filesToCompact, user, compactionStartTime,
        compaction.compact(throughputController, user));
    } finally {
      finishCompactionRequest(cr);
    }
  }

  protected List<HStoreFile> doCompaction(CompactionRequestImpl cr,
    Collection<HStoreFile> filesToCompact, User user, long compactionStartTime, List<Path> newFiles)
    throws IOException {
    // Do the steps necessary to complete the compaction.
    setStoragePolicyFromFileName(newFiles);
    List<HStoreFile> sfs = storeEngine.commitStoreFiles(newFiles, true);
    if (this.getCoprocessorHost() != null) {
      for (HStoreFile sf : sfs) {
        getCoprocessorHost().postCompact(this, sf, cr.getTracker(), cr, user);
      }
    }
    replaceStoreFiles(filesToCompact, sfs, true);

    long outputBytes = getTotalSize(sfs);

    // At this point the store will use new files for all new scanners.
    refreshStoreSizeAndTotalBytes(); // update store size.

    long now = EnvironmentEdgeManager.currentTime();
    if (
      region.getRegionServerServices() != null
        && region.getRegionServerServices().getMetrics() != null
    ) {
      region.getRegionServerServices().getMetrics().updateCompaction(
        region.getTableDescriptor().getTableName().getNameAsString(), cr.isMajor(),
        now - compactionStartTime, cr.getFiles().size(), newFiles.size(), cr.getSize(),
        outputBytes);

    }

    logCompactionEndMessage(cr, sfs, now, compactionStartTime);
    return sfs;
  }

  // Set correct storage policy from the file name of DTCP.
  // Rename file will not change the storage policy.
  private void setStoragePolicyFromFileName(List<Path> newFiles) throws IOException {
    String prefix = HConstants.STORAGE_POLICY_PREFIX;
    for (Path newFile : newFiles) {
      if (newFile.getParent().getName().startsWith(prefix)) {
        CommonFSUtils.setStoragePolicy(getRegionFileSystem().getFileSystem(), newFile,
          newFile.getParent().getName().substring(prefix.length()));
      }
    }
  }

  /**
   * Writes the compaction WAL record.
   * @param filesCompacted Files compacted (input).
   * @param newFiles       Files from compaction.
   */
  private void writeCompactionWalRecord(Collection<HStoreFile> filesCompacted,
    Collection<HStoreFile> newFiles) throws IOException {
    if (region.getWAL() == null) {
      return;
    }
    List<Path> inputPaths =
      filesCompacted.stream().map(HStoreFile::getPath).collect(Collectors.toList());
    List<Path> outputPaths =
      newFiles.stream().map(HStoreFile::getPath).collect(Collectors.toList());
    RegionInfo info = this.region.getRegionInfo();
    CompactionDescriptor compactionDescriptor = ProtobufUtil.toCompactionDescriptor(info,
      getColumnFamilyDescriptor().getName(), inputPaths, outputPaths,
      getRegionFileSystem().getStoreDir(getColumnFamilyDescriptor().getNameAsString()));
    // Fix reaching into Region to get the maxWaitForSeqId.
    // Does this method belong in Region altogether given it is making so many references up there?
    // Could be Region#writeCompactionMarker(compactionDescriptor);
    WALUtil.writeCompactionMarker(this.region.getWAL(), this.region.getReplicationScope(),
      this.region.getRegionInfo(), compactionDescriptor, this.region.getMVCC());
  }

  @RestrictedApi(explanation = "Should only be called in TestHStore", link = "",
      allowedOnPath = ".*/(HStore|TestHStore).java")
  void replaceStoreFiles(Collection<HStoreFile> compactedFiles, Collection<HStoreFile> result,
    boolean writeCompactionMarker) throws IOException {
    storeEngine.replaceStoreFiles(compactedFiles, result, () -> {
      if (writeCompactionMarker) {
        writeCompactionWalRecord(compactedFiles, result);
      }
    }, () -> {
      synchronized (filesCompacting) {
        filesCompacting.removeAll(compactedFiles);
      }
    });
    // These may be null when the RS is shutting down. The space quota Chores will fix the Region
    // sizes later so it's not super-critical if we miss these.
    RegionServerServices rsServices = region.getRegionServerServices();
    if (rsServices != null && rsServices.getRegionServerSpaceQuotaManager() != null) {
      updateSpaceQuotaAfterFileReplacement(
        rsServices.getRegionServerSpaceQuotaManager().getRegionSizeStore(), getRegionInfo(),
        compactedFiles, result);
    }
  }

  /**
   * Updates the space quota usage for this region, removing the size for files compacted away and
   * adding in the size for new files.
   * @param sizeStore  The object tracking changes in region size for space quotas.
   * @param regionInfo The identifier for the region whose size is being updated.
   * @param oldFiles   Files removed from this store's region.
   * @param newFiles   Files added to this store's region.
   */
  void updateSpaceQuotaAfterFileReplacement(RegionSizeStore sizeStore, RegionInfo regionInfo,
    Collection<HStoreFile> oldFiles, Collection<HStoreFile> newFiles) {
    long delta = 0;
    if (oldFiles != null) {
      for (HStoreFile compactedFile : oldFiles) {
        if (compactedFile.isHFile()) {
          delta -= compactedFile.getReader().length();
        }
      }
    }
    if (newFiles != null) {
      for (HStoreFile newFile : newFiles) {
        if (newFile.isHFile()) {
          delta += newFile.getReader().length();
        }
      }
    }
    sizeStore.incrementRegionSize(regionInfo, delta);
  }

  /**
   * Log a very elaborate compaction completion message.
   * @param cr                  Request.
   * @param sfs                 Resulting files.
   * @param compactionStartTime Start time.
   */
  private void logCompactionEndMessage(CompactionRequestImpl cr, List<HStoreFile> sfs, long now,
    long compactionStartTime) {
    StringBuilder message = new StringBuilder("Completed" + (cr.isMajor() ? " major" : "")
      + " compaction of " + cr.getFiles().size() + (cr.isAllFiles() ? " (all)" : "")
      + " file(s) in " + this + " of " + this.getRegionInfo().getShortNameToLog() + " into ");
    if (sfs.isEmpty()) {
      message.append("none, ");
    } else {
      for (HStoreFile sf : sfs) {
        message.append(sf.getPath().getName());
        message.append("(size=");
        message.append(TraditionalBinaryPrefix.long2String(sf.getReader().length(), "", 1));
        message.append("), ");
      }
    }
    message.append("total size for store is ")
      .append(StringUtils.TraditionalBinaryPrefix.long2String(storeSize.get(), "", 1))
      .append(". This selection was in queue for ")
      .append(StringUtils.formatTimeDiff(compactionStartTime, cr.getSelectionTime()))
      .append(", and took ").append(StringUtils.formatTimeDiff(now, compactionStartTime))
      .append(" to execute.");
    LOG.info(message.toString());
    if (LOG.isTraceEnabled()) {
      int fileCount = storeEngine.getStoreFileManager().getStorefileCount();
      long resultSize = getTotalSize(sfs);
      String traceMessage = "COMPACTION start,end,size out,files in,files out,store size,"
        + "store files [" + compactionStartTime + "," + now + "," + resultSize + ","
        + cr.getFiles().size() + "," + sfs.size() + "," + storeSize + "," + fileCount + "]";
      LOG.trace(traceMessage);
    }
  }

  /**
   * Call to complete a compaction. Its for the case where we find in the WAL a compaction that was
   * not finished. We could find one recovering a WAL after a regionserver crash. See HBASE-2231.
   */
  public void replayCompactionMarker(CompactionDescriptor compaction, boolean pickCompactionFiles,
    boolean removeFiles) throws IOException {
    LOG.debug("Completing compaction from the WAL marker");
    List<String> compactionInputs = compaction.getCompactionInputList();
    List<String> compactionOutputs = Lists.newArrayList(compaction.getCompactionOutputList());

    // The Compaction Marker is written after the compaction is completed,
    // and the files moved into the region/family folder.
    //
    // If we crash after the entry is written, we may not have removed the
    // input files, but the output file is present.
    // (The unremoved input files will be removed by this function)
    //
    // If we scan the directory and the file is not present, it can mean that:
    // - The file was manually removed by the user
    // - The file was removed as consequence of subsequent compaction
    // so, we can't do anything with the "compaction output list" because those
    // files have already been loaded when opening the region (by virtue of
    // being in the store's folder) or they may be missing due to a compaction.

    String familyName = this.getColumnFamilyName();
    Set<String> inputFiles = new HashSet<>();
    for (String compactionInput : compactionInputs) {
      Path inputPath = getRegionFileSystem().getStoreFilePath(familyName, compactionInput);
      inputFiles.add(inputPath.getName());
    }

    // some of the input files might already be deleted
    List<HStoreFile> inputStoreFiles = new ArrayList<>(compactionInputs.size());
    for (HStoreFile sf : this.getStorefiles()) {
      if (inputFiles.contains(sf.getPath().getName())) {
        inputStoreFiles.add(sf);
      }
    }

    // check whether we need to pick up the new files
    List<HStoreFile> outputStoreFiles = new ArrayList<>(compactionOutputs.size());

    if (pickCompactionFiles) {
      for (HStoreFile sf : this.getStorefiles()) {
        compactionOutputs.remove(sf.getPath().getName());
      }
      for (String compactionOutput : compactionOutputs) {
        StoreFileInfo storeFileInfo =
          getRegionFileSystem().getStoreFileInfo(getColumnFamilyName(), compactionOutput);
        HStoreFile storeFile = storeEngine.createStoreFileAndReader(storeFileInfo);
        outputStoreFiles.add(storeFile);
      }
    }

    if (!inputStoreFiles.isEmpty() || !outputStoreFiles.isEmpty()) {
      LOG.info("Replaying compaction marker, replacing input files: " + inputStoreFiles
        + " with output files : " + outputStoreFiles);
      this.replaceStoreFiles(inputStoreFiles, outputStoreFiles, false);
      this.refreshStoreSizeAndTotalBytes();
    }
  }

  @Override
  public boolean hasReferences() {
    // Grab the read lock here, because we need to ensure that: only when the atomic
    // replaceStoreFiles(..) finished, we can get all the complete store file list.
    this.storeEngine.readLock();
    try {
      // Merge the current store files with compacted files here due to HBASE-20940.
      Collection<HStoreFile> allStoreFiles = new ArrayList<>(getStorefiles());
      allStoreFiles.addAll(getCompactedFiles());
      return StoreUtils.hasReferences(allStoreFiles);
    } finally {
      this.storeEngine.readUnlock();
    }
  }

  /**
   * getter for CompactionProgress object
   * @return CompactionProgress object; can be null
   */
  public CompactionProgress getCompactionProgress() {
    return this.storeEngine.getCompactor().getProgress();
  }

  @Override
  public boolean shouldPerformMajorCompaction() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9239d9ae-a2c6-3e02-b035-074ee952305d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d76b23b6-ad96-3ade-b0fb-0801782c9198"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3555e972-e3a5-389d-a1e6-2891524e81b8"))) {
return true;
}
    for (HStoreFile sf : ((KnobRuntime.check(java.util.UUID.fromString("8051a42c-430e-3bdd-81db-7e4f072ab6a1"))) ? (this.storeEngine.getStoreFileManager().getCompactedfiles()) : (((KnobRuntime.check(java.util.UUID.fromString("774a11a3-e37a-330c-ae0e-43b428d991a7"))) ? (getStorefiles()) : (this.storeEngine.getStoreFileManager().getStoreFiles()))))) {
      // TODO: what are these reader checks all over the place?
      if (sf.getReader() == null) {
if(KnobRuntime.check(java.util.UUID.fromString("f8429389-f407-3066-9824-3f7ef2ed1693"))) {
try {
    java.lang.reflect.Field field = sf.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sf));
    field.set(sf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80d5d13a-5f4b-3564-98a0-3d8cfae1930b"))) {
try {
    java.lang.reflect.Field field = sf.getClass().getDeclaredField("maxMemstoreTS");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sf));
    field.set(sf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("eeccdf66-1693-322f-8cc0-1ca2b2dfa4df"))) { LOG.error("StoreFile {} has null Reader", sf); } else if (KnobRuntime.check(java.util.UUID.fromString("9ee5f607-2e00-322a-9168-fa542e321cf3"))) { LOG.trace("StoreFile {} has null Reader", sf); } else if (KnobRuntime.check(java.util.UUID.fromString("6cf7af3e-90aa-3a62-aeb8-b0b0ff1a7f9c"))) { LOG.info("StoreFile {} has null Reader", sf); } else { LOG.debug("StoreFile {} has null Reader", sf); }
        return false;
      }
    }
    return storeEngine.getCompactionPolicy()
      .shouldPerformMajorCompaction(this.storeEngine.getStoreFileManager().getStoreFiles());
  }

  public Optional<CompactionContext> requestCompaction() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8e524afe-4354-3af2-bfc3-ce0ae061b528"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3a86225a-22b5-3374-8fbd-071f388fe016"))) {
return null;
}
    return requestCompaction(NO_PRIORITY, CompactionLifeCycleTracker.DUMMY, null);
  }

  public Optional<CompactionContext> requestCompaction(int priority,
    CompactionLifeCycleTracker tracker, User user) throws IOException {
    // don't even select for compaction if writes are disabled
    if (!this.areWritesEnabled()) {
      return Optional.empty();
    }
    // Before we do compaction, try to get rid of unneeded files to simplify things.
    removeUnneededFiles();

    final CompactionContext compaction = storeEngine.createCompaction();
    CompactionRequestImpl request = null;
    this.storeEngine.readLock();
    try {
      synchronized (filesCompacting) {
        // First, see if coprocessor would want to override selection.
        if (this.getCoprocessorHost() != null) {
          final List<HStoreFile> candidatesForCoproc = compaction.preSelect(this.filesCompacting);
          boolean override =
            getCoprocessorHost().preCompactSelection(this, candidatesForCoproc, tracker, user);
          if (override) {
            // Coprocessor is overriding normal file selection.
            compaction.forceSelect(new CompactionRequestImpl(candidatesForCoproc));
          }
        }

        // Normal case - coprocessor is not overriding file selection.
        if (!compaction.hasSelection()) {
          boolean isUserCompaction = priority == Store.PRIORITY_USER;
          boolean mayUseOffPeak =
            offPeakHours.isOffPeakHour() && offPeakCompactionTracker.compareAndSet(false, true);
          try {
            compaction.select(this.filesCompacting, isUserCompaction, mayUseOffPeak,
              forceMajor && filesCompacting.isEmpty());
          } catch (IOException e) {
            if (mayUseOffPeak) {
              offPeakCompactionTracker.set(false);
            }
            throw e;
          }
          assert compaction.hasSelection();
          if (mayUseOffPeak && !compaction.getRequest().isOffPeak()) {
            // Compaction policy doesn't want to take advantage of off-peak.
            offPeakCompactionTracker.set(false);
          }
        }
        if (this.getCoprocessorHost() != null) {
          this.getCoprocessorHost().postCompactSelection(this,
            ImmutableList.copyOf(compaction.getRequest().getFiles()), tracker,
            compaction.getRequest(), user);
        }
        // Finally, we have the resulting files list. Check if we have any files at all.
        request = compaction.getRequest();
        Collection<HStoreFile> selectedFiles = request.getFiles();
        if (selectedFiles.isEmpty()) {
          return Optional.empty();
        }

        addToCompactingFiles(selectedFiles);

        // If we're enqueuing a major, clear the force flag.
        this.forceMajor = this.forceMajor && !request.isMajor();

        // Set common request properties.
        // Set priority, either override value supplied by caller or from store.
        final int compactionPriority =
          (priority != Store.NO_PRIORITY) ? priority : getCompactPriority();
        request.setPriority(compactionPriority);

        if (request.isAfterSplit()) {
          // If the store belongs to recently splitted daughter regions, better we consider
          // them with the higher priority in the compaction queue.
          // Override priority if it is lower (higher int value) than
          // SPLIT_REGION_COMPACTION_PRIORITY
          final int splitHousekeepingPriority =
            Math.min(compactionPriority, SPLIT_REGION_COMPACTION_PRIORITY);
          request.setPriority(splitHousekeepingPriority);
          LOG.info(
            "Keeping/Overriding Compaction request priority to {} for CF {} since it"
              + " belongs to recently split daughter region {}",
            splitHousekeepingPriority, this.getColumnFamilyName(),
            getRegionInfo().getRegionNameAsString());
        }
        request.setDescription(getRegionInfo().getRegionNameAsString(), getColumnFamilyName());
        request.setTracker(tracker);
      }
    } finally {
      this.storeEngine.readUnlock();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(this + " is initiating " + (request.isMajor() ? "major" : "minor") + " compaction"
        + (request.isAllFiles() ? " (all files)" : ""));
    }
    this.region.reportCompactionRequestStart(request.isMajor());
    return Optional.of(compaction);
  }

  /** Adds the files to compacting files. filesCompacting must be locked. */
  private void addToCompactingFiles(Collection<HStoreFile> filesToAdd) {
    if (CollectionUtils.isEmpty(filesToAdd)) {
      return;
    }
    // Check that we do not try to compact the same StoreFile twice.
    if (!Collections.disjoint(filesCompacting, filesToAdd)) {
      Preconditions.checkArgument(false, "%s overlaps with %s", filesToAdd, filesCompacting);
    }
    filesCompacting.addAll(filesToAdd);
    Collections.sort(filesCompacting, storeEngine.getStoreFileManager().getStoreFileComparator());
  }

  private void removeUnneededFiles() throws IOException {
    if (!conf.getBoolean("hbase.store.delete.expired.storefile", true)) {
      return;
    }
    if (getColumnFamilyDescriptor().getMinVersions() > 0) {
      LOG.debug("Skipping expired store file removal due to min version of {} being {}", this,
        getColumnFamilyDescriptor().getMinVersions());
      return;
    }
    this.storeEngine.readLock();
    Collection<HStoreFile> delSfs = null;
    try {
      synchronized (filesCompacting) {
        long cfTtl = getStoreFileTtl();
        if (cfTtl != Long.MAX_VALUE) {
          delSfs = storeEngine.getStoreFileManager()
            .getUnneededFiles(EnvironmentEdgeManager.currentTime() - cfTtl, filesCompacting);
          addToCompactingFiles(delSfs);
        }
      }
    } finally {
      this.storeEngine.readUnlock();
    }

    if (CollectionUtils.isEmpty(delSfs)) {
      return;
    }

    Collection<HStoreFile> newFiles = Collections.emptyList(); // No new files.
    replaceStoreFiles(delSfs, newFiles, true);
    refreshStoreSizeAndTotalBytes();
    LOG.info("Completed removal of " + delSfs.size() + " unnecessary (expired) file(s) in " + this
      + "; total size is " + TraditionalBinaryPrefix.long2String(storeSize.get(), "", 1));
  }

  public void cancelRequestedCompaction(CompactionContext compaction) {
    finishCompactionRequest(compaction.getRequest());
  }

  protected void finishCompactionRequest(CompactionRequestImpl cr) {
    this.region.reportCompactionRequestEnd(cr.isMajor(), cr.getFiles().size(), cr.getSize());
    if (cr.isOffPeak()) {
      offPeakCompactionTracker.set(false);
      cr.setOffPeak(false);
    }
    synchronized (filesCompacting) {
      filesCompacting.removeAll(cr.getFiles());
    }
    // The tracker could be null, for example, we do not need to track the creation of store file
    // writer due to different implementation of SFT, or the compaction is canceled.
    if (cr.getWriterCreationTracker() != null) {
      storeFileWriterCreationTrackers.remove(cr.getWriterCreationTracker());
    }
  }

  /**
   * Update counts.
   */
  protected void refreshStoreSizeAndTotalBytes() throws IOException {
    this.storeSize.set(0L);
    this.totalUncompressedBytes.set(0L);
    for (HStoreFile hsf : this.storeEngine.getStoreFileManager().getStoreFiles()) {
      StoreFileReader r = hsf.getReader();
      if (r == null) {
        LOG.debug("StoreFile {} has a null Reader", hsf);
        continue;
      }
      this.storeSize.addAndGet(r.length());
      this.totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());
    }
  }

  /*
   * @param wantedVersions How many versions were asked for.
   * @return wantedVersions or this families' {@link HConstants#VERSIONS}.
   */
  int versionsToReturn(final int wantedVersions) {
if(KnobRuntime.check(java.util.UUID.fromString("24384d20-8a57-32ba-9760-990c51ea2f34"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("b9f01d46-8f1e-3f29-a87b-66ce84fcf0a3"))) ? ((wantedVersions) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ee9e7f6e-082b-3390-937d-4f7a22f6ab8e"))) ? ((getColumnFamilyDescriptor().getMaxVersions()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5976cabc-451b-3b66-9244-1f6a9e834838"))) ? ((getColumnFamilyDescriptor().getMaxVersions()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5d464816-626d-32b5-87db-06df9450f975"))) ? ((wantedVersions) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("147ada1b-786b-3818-8127-0d30c19ba3fa"))) ? ((getColumnFamilyDescriptor().getMaxVersions()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("df101d34-6cc1-3460-b311-c88089fcabc9"))) ? ((wantedVersions) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d3ff47d7-9ef8-3b4f-94e8-5411ca996fe9"))) ? ((getColumnFamilyDescriptor().getMaxVersions()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("90e73dc0-d71e-3795-a2aa-193bc4be43d4"))) ? ((wantedVersions) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("81005b88-6bbe-3e11-9462-599e8cf7cf4e"))) ? ((wantedVersions) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bba388a0-1fef-35bb-9515-b097bdc1403d"))) ? ((getColumnFamilyDescriptor().getMaxVersions()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("28c1f415-2989-3a83-aea1-b26e5d2f03f5"))) ? ((wantedVersions) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("46f12720-d282-383f-947c-0515623a2934"))) ? ((getColumnFamilyDescriptor().getMaxVersions()) != (0)) : (wantedVersions <= 0))))))))))))))))))))))))) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    // Make sure we do not return more than maximum versions for this store.
    int maxVersions = getColumnFamilyDescriptor().getMaxVersions();
    return wantedVersions > maxVersions ? maxVersions : wantedVersions;
  }

  @Override
  public boolean canSplit() {
if(KnobRuntime.check(java.util.UUID.fromString("5fb7a056-d824-3ae0-b4b3-1945b23e9cd9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("60396fcc-6390-3a58-a434-f8f813c49610"))) {
return true;
}
    // Not split-able if we find a reference store file present in the store.
    boolean result = !hasReferences();
    if (!result) {
      LOG.trace("Not splittable; has references: {}", this);
    }
    return result;
  }

  /**
   * Determines if Store should be split.
   */
  public Optional<byte[]> getSplitPoint() {
    this.storeEngine.readLock();
    try {
      // Should already be enforced by the split policy!
      assert !this.getRegionInfo().isMetaRegion();
      // Not split-able if we find a reference store file present in the store.
      if (hasReferences()) {
        if (KnobRuntime.check(java.util.UUID.fromString("ed756b46-5d86-3fb3-8473-a3f747d369f7"))) { LOG.info("Not splittable; has references: {}", this); } else if (KnobRuntime.check(java.util.UUID.fromString("283929df-b7a8-306a-ad8f-18487c908203"))) { LOG.error("Not splittable; has references: {}", this); } else { LOG.trace("Not splittable; has references: {}", this); }
        return Optional.empty();
      }
      return this.storeEngine.getStoreFileManager().getSplitPoint();
    } catch (IOException e) {
      LOG.warn("Failed getting store size for {}", this, e);
    } finally {
      this.storeEngine.readUnlock();
    }
    return Optional.empty();
  }

  @Override
  public long getLastCompactSize() {
    return this.lastCompactSize;
  }

  @Override
  public long getSize() {
    return storeSize.get();
  }

  public void triggerMajorCompaction() {
    this.forceMajor = true;
  }

  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return a scanner for both the memstore and the HStore files. Assumes we are not in a
   * compaction.
   * @param scan       Scan to apply when scanning the stores
   * @param targetCols columns to scan
   * @return a scanner over the current key values
   * @throws IOException on failure
   */
  public KeyValueScanner getScanner(Scan scan, final NavigableSet<byte[]> targetCols, long readPt)
    throws IOException {
    storeEngine.readLock();
    try {
      ScanInfo scanInfo;
      if (this.getCoprocessorHost() != null) {
        scanInfo = this.getCoprocessorHost().preStoreScannerOpen(this, scan);
      } else {
        scanInfo = getScanInfo();
      }
      return createScanner(scan, scanInfo, targetCols, readPt);
    } finally {
      storeEngine.readUnlock();
    }
  }

  // HMobStore will override this method to return its own implementation.
  protected KeyValueScanner createScanner(Scan scan, ScanInfo scanInfo,
    NavigableSet<byte[]> targetCols, long readPt) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("58815420-bf0d-3fab-bf2b-288830b0d13f"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79f45fca-94dc-332f-8957-7da6ac17204c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36e755ac-29d6-3205-a92a-a969a3a05cd0"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6b8f50a-e4c6-3484-b27a-a25f8d95d530"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1396a442-f6aa-3910-905b-9156fd930021"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e4e98f7b-dea8-37b0-8123-4fc7ff1b9a01"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac794eb8-cb5b-340e-848b-8f7c85f50a4d"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("mvccReadPoint");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3410748d-8a70-3f00-80aa-2ddbe9126303"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("096554eb-3613-3ebf-b6f8-f4bf52e64ef5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90032efa-25e0-3b95-b7d3-35079e519ece"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4daf6ee8-99e0-3e4f-8f39-cc21e1e49e35"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89f52d8b-ad3e-3379-a1ed-d36cb7360d5a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc1f4e33-e931-35e9-9bde-18a3e5164723"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8749adef-02fe-3dab-a37c-4f676da81250"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ef0d513-bbc3-3f3b-9908-ef13fdf454f8"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f98f3df0-d9fc-3f5d-9154-4067dff6d609"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("cellsPerTimeoutCheck");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5918b19-6854-3a27-a66d-e16e0628021b"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("tableMaxRowSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5549a5a-ea49-3d9f-98cd-f2bd2264b901"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("parallelSeekEnabled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scanInfo);
    field.set(scanInfo, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8bf18399-a0b2-3d00-9f46-ea07893fd0a4"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scan);
    field.set(scan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e1024ced-81e1-3296-8b77-998c8f1e9479"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f0e001a-9ea7-3f2c-a143-6c2c872049e8"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28678f01-e0de-3759-90fa-a071f04e0c94"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("86372202-1936-35bc-8258-805de876aa2b"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31a0d09d-cbc0-3089-8368-28441beeb23f"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("preadMaxBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("780cf236-da6a-3d4a-8f44-560d1662cc4a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20e42970-d86c-3153-ba7e-74470ad3123b"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("timeToPurgeDeletes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("510c4251-0f30-393d-bd88-b82b43205e86"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("821cc2d6-8ffc-3115-9cea-4ffd4224df1f"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10324683-cd25-3490-a3bf-1a908d96fa1a"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f20d435-615f-3ed3-93cb-29c8c9f84365"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa25cc6f-99ce-3c02-8f84-4ba7d73de21f"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("newVersionBehavior");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scanInfo);
    field.set(scanInfo, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e4abac7-e928-34a9-85f2-afe2a2234de6"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("743eeec7-742d-3c7e-9f63-f027f39672e1"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a26dce5-d786-3063-82a2-cf1dfe524f31"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a75954dc-7529-3c94-b25b-ec6abf84bfbd"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("245dd569-5563-3fa3-be7c-10b9a156c9be"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53d8fb9f-4844-343e-bc7c-b45710f1c2ec"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d85214c8-f66c-346b-bfcf-9e76f9e934f1"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b957c694-e807-3fb2-9488-b26fd8e96bda"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5212668b-776d-3779-9d5a-5e2d88b8ce8d"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d611e12b-e14e-3ba1-93c1-d59a11c3248e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("398f783c-ad45-3535-8a5e-8b30ef2052ad"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("81dcd849-3bc3-3499-9852-c101b8d9c69b"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return scan.isReversed()
      ? new ReversedStoreScanner(this, scanInfo, scan, targetCols, readPt)
      : new StoreScanner(this, scanInfo, scan, targetCols, readPt);
  }

  /**
   * Recreates the scanners on the current list of active store file scanners
   * @param currentFileScanners    the current set of active store file scanners
   * @param cacheBlocks            cache the blocks or not
   * @param usePread               use pread or not
   * @param isCompaction           is the scanner for compaction
   * @param matcher                the scan query matcher
   * @param startRow               the scan's start row
   * @param includeStartRow        should the scan include the start row
   * @param stopRow                the scan's stop row
   * @param includeStopRow         should the scan include the stop row
   * @param readPt                 the read point of the current scane
   * @param includeMemstoreScanner whether the current scanner should include memstorescanner
   * @return list of scanners recreated on the current Scanners
   */
  public List<KeyValueScanner> recreateScanners(List<KeyValueScanner> currentFileScanners,
    boolean cacheBlocks, boolean usePread, boolean isCompaction, ScanQueryMatcher matcher,
    byte[] startRow, boolean includeStartRow, byte[] stopRow, boolean includeStopRow, long readPt,
    boolean includeMemstoreScanner) throws IOException {
    this.storeEngine.readLock();
    try {
      Map<String, HStoreFile> name2File =
        new HashMap<>(getStorefilesCount() + getCompactedFilesCount());
      for (HStoreFile file : getStorefiles()) {
        name2File.put(file.getFileInfo().getActiveFileName(), file);
      }
      Collection<HStoreFile> compactedFiles = getCompactedFiles();
      for (HStoreFile file : IterableUtils.emptyIfNull(compactedFiles)) {
        name2File.put(file.getFileInfo().getActiveFileName(), file);
      }
      List<HStoreFile> filesToReopen = new ArrayList<>();
      for (KeyValueScanner kvs : currentFileScanners) {
        assert kvs.isFileScanner();
        if (kvs.peek() == null) {
          continue;
        }
        filesToReopen.add(name2File.get(kvs.getFilePath().getName()));
      }
      if (filesToReopen.isEmpty()) {
        return null;
      }
      return getScanners(filesToReopen, cacheBlocks, false, false, matcher, startRow,
        includeStartRow, stopRow, includeStopRow, readPt, false, false);
    } finally {
      this.storeEngine.readUnlock();
    }
  }

  @Override
  public String toString() {
    return this.getRegionInfo().getShortNameToLog() + "/" + this.getColumnFamilyName();
  }

  @Override
  public int getStorefilesCount() {
    return this.storeEngine.getStoreFileManager().getStorefileCount();
  }

  @Override
  public int getCompactedFilesCount() {
    return this.storeEngine.getStoreFileManager().getCompactedFilesCount();
  }

  private LongStream getStoreFileAgeStream() {
    return this.storeEngine.getStoreFileManager().getStoreFiles().stream().filter(sf -> {
      if (sf.getReader() == null) {
        LOG.debug("StoreFile {} has a null Reader", sf);
        return false;
      } else {
        return true;
      }
    }).filter(HStoreFile::isHFile).mapToLong(sf -> sf.getFileInfo().getCreatedTimestamp())
      .map(t -> EnvironmentEdgeManager.currentTime() - t);
  }

  @Override
  public OptionalLong getMaxStoreFileAge() {
    return getStoreFileAgeStream().max();
  }

  @Override
  public OptionalLong getMinStoreFileAge() {
if(KnobRuntime.check(java.util.UUID.fromString("5e03af2e-8a22-3997-92d8-02bf37921210"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("19eff9a0-af57-3df2-8fa9-5a10b092ac1e"))) ? (getMaxSequenceId()) : (((KnobRuntime.check(java.util.UUID.fromString("38f378c5-3999-3e91-bf7f-56b5cfeea1a4"))) ? (getStoreFileAgeStream().max()) : (getStoreFileAgeStream().min()))));
  }

  @Override
  public OptionalDouble getAvgStoreFileAge() {
    return getStoreFileAgeStream().average();
  }

  @Override
  public long getNumReferenceFiles() {
    return this.storeEngine.getStoreFileManager().getStoreFiles().stream()
      .filter(HStoreFile::isReference).count();
  }

  @Override
  public long getNumHFiles() {
    return this.storeEngine.getStoreFileManager().getStoreFiles().stream()
      .filter(HStoreFile::isHFile).count();
  }

  @Override
  public long getStoreSizeUncompressed() {
if(KnobRuntime.check(java.util.UUID.fromString("257346d8-caf9-3aee-8e95-cd0e40e08a85"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("15f7ad46-1e46-3672-97ca-f385bfd881a4"))) ? (EnvironmentEdgeManager.currentTime()) : (this.totalUncompressedBytes.get()));
  }

  @Override
  public long getStorefilesSize() {
if(KnobRuntime.check(java.util.UUID.fromString("84566402-645b-3c4e-ac31-5f07a086f2ac"))) {
return 0;
}
    // Include all StoreFiles
    return StoreUtils.getStorefilesSize(this.storeEngine.getStoreFileManager().getStoreFiles(),
      sf -> true);
  }

  @Override
  public long getHFilesSize() {
if(KnobRuntime.check(java.util.UUID.fromString("bc54617a-6f42-3de7-b7f7-61bfec89c38c"))) {
return 0;
}
    // Include only StoreFiles which are HFiles
    return StoreUtils.getStorefilesSize(this.storeEngine.getStoreFileManager().getStoreFiles(),
      HStoreFile::isHFile);
  }

  private long getStorefilesFieldSize(ToLongFunction<StoreFileReader> f) {
    return this.storeEngine.getStoreFileManager().getStoreFiles().stream()
      .mapToLong(file -> StoreUtils.getStorefileFieldSize(file, f)).sum();
  }

  @Override
  public long getStorefilesRootLevelIndexSize() {
if(KnobRuntime.check(java.util.UUID.fromString("859fbfee-fd27-301b-aac7-ae6a5f8b2202"))) {
return 0;
}
    return getStorefilesFieldSize(StoreFileReader::indexSize);
  }

  @Override
  public long getTotalStaticIndexSize() {
    return getStorefilesFieldSize(StoreFileReader::getUncompressedDataIndexSize);
  }

  @Override
  public long getTotalStaticBloomSize() {
    return getStorefilesFieldSize(StoreFileReader::getTotalBloomSize);
  }

  @Override
  public MemStoreSize getMemStoreSize() {
    return this.memstore.size();
  }

  @Override
  public int getCompactPriority() {
    int priority = this.storeEngine.getStoreFileManager().getStoreCompactionPriority();
    if (priority == PRIORITY_USER) {
      if (KnobRuntime.check(java.util.UUID.fromString("bdf32f4a-36a2-3eda-a456-9836aa33a53e"))) { LOG.debug("Compaction priority is USER despite there being no user compaction"); } else if (KnobRuntime.check(java.util.UUID.fromString("dce56d53-4121-37e2-bc03-668a19a46f9a"))) { LOG.info("Compaction priority is USER despite there being no user compaction"); } else { LOG.warn("Compaction priority is USER despite there being no user compaction"); }
    }
    return priority;
  }

  public boolean throttleCompaction(long compactionSize) {
    return storeEngine.getCompactionPolicy().throttleCompaction(compactionSize);
  }

  public HRegion getHRegion() {
if(KnobRuntime.check(java.util.UUID.fromString("a0562024-3bce-3721-86ff-18ec5a4d4d36"))) {
return null;
}
    return this.region;
  }

  public RegionCoprocessorHost getCoprocessorHost() {
    return this.region.getCoprocessorHost();
  }

  @Override
  public RegionInfo getRegionInfo() {
    return getRegionFileSystem().getRegionInfo();
  }

  @Override
  public boolean areWritesEnabled() {
    return this.region.areWritesEnabled();
  }

  @Override
  public long getSmallestReadPoint() {
    return this.region.getSmallestReadPoint();
  }

  /**
   * Adds or replaces the specified KeyValues.
   * <p>
   * For each KeyValue specified, if a cell with the same row, family, and qualifier exists in
   * MemStore, it will be replaced. Otherwise, it will just be inserted to MemStore.
   * <p>
   * This operation is atomic on each KeyValue (row/family/qualifier) but not necessarily atomic
   * across all of them.
   * @param readpoint readpoint below which we can safely remove duplicate KVs
   */
  public void upsert(Iterable<Cell> cells, long readpoint, MemStoreSizing memstoreSizing) {
    this.storeEngine.readLock();
    try {
      this.memstore.upsert(cells, readpoint, memstoreSizing);
    } finally {
      this.storeEngine.readUnlock();
    }
  }

  public StoreFlushContext createFlushContext(long cacheFlushId, FlushLifeCycleTracker tracker) {
    return new StoreFlusherImpl(cacheFlushId, tracker);
  }

  private final class StoreFlusherImpl implements StoreFlushContext {

    private final FlushLifeCycleTracker tracker;
    private final StoreFileWriterCreationTracker writerCreationTracker;
    private final long cacheFlushSeqNum;
    private MemStoreSnapshot snapshot;
    private List<Path> tempFiles;
    private List<Path> committedFiles;
    private long cacheFlushCount;
    private long cacheFlushSize;
    private long outputFileSize;

    private StoreFlusherImpl(long cacheFlushSeqNum, FlushLifeCycleTracker tracker) {
      this.cacheFlushSeqNum = cacheFlushSeqNum;
      this.tracker = tracker;
      this.writerCreationTracker = storeFileWriterCreationTrackerFactory.get();
    }

    /**
     * This is not thread safe. The caller should have a lock on the region or the store. If
     * necessary, the lock can be added with the patch provided in HBASE-10087
     */
    @Override
    public MemStoreSize prepare() {
      // passing the current sequence number of the wal - to allow bookkeeping in the memstore
      this.snapshot = memstore.snapshot();
      this.cacheFlushCount = snapshot.getCellsCount();
      this.cacheFlushSize = snapshot.getDataSize();
      committedFiles = new ArrayList<>(1);
      return snapshot.getMemStoreSize();
    }

    @Override
    public void flushCache(MonitoredTask status) throws IOException {
      RegionServerServices rsService = region.getRegionServerServices();
      ThroughputController throughputController =
        rsService == null ? null : rsService.getFlushThroughputController();
      // it could be null if we do not need to track the creation of store file writer due to
      // different SFT implementation.
      if (writerCreationTracker != null) {
        HStore.this.storeFileWriterCreationTrackers.add(writerCreationTracker);
      }
if(KnobRuntime.check(java.util.UUID.fromString("559a070b-f855-3f2f-9915-781363a74d75"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("id");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("590658f9-b070-3d6e-9e1e-f06ceb25aa0f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("tagsPresent");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshot);
    field.set(snapshot, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e43498c7-4e33-3b1d-8bd1-b4971d8b9bd2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d653efd4-c489-3a6a-9695-a1b873de7d6b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f4c3b34-e6c8-3559-8476-e4d32dd52714"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46c0e302-446b-3315-bf5a-c1c8ea9c0383"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("03a0a7d4-6e03-305b-954d-9c3265c0880a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      tempFiles = HStore.this.flushCache(cacheFlushSeqNum, snapshot, status, throughputController,
        tracker, writerCreationTracker);
    }

    @Override
    public boolean commit(MonitoredTask status) throws IOException {
      try {
        if (CollectionUtils.isEmpty(this.tempFiles)) {
          return false;
        }
        status.setStatus("Flushing " + this + ": reopening flushed file");
if(KnobRuntime.check(java.util.UUID.fromString("dba0bc35-3c79-3fdb-801d-041e331ff741"))) {
throw new java.io.IOException("Injected exception");
}
        List<HStoreFile> storeFiles = storeEngine.commitStoreFiles(tempFiles, false);
        for (HStoreFile sf : storeFiles) {
          StoreFileReader r = sf.getReader();
          if (LOG.isInfoEnabled()) {
            LOG.info("Added {}, entries={}, sequenceid={}, filesize={}", sf, r.getEntries(),
              cacheFlushSeqNum, TraditionalBinaryPrefix.long2String(r.length(), "", 1));
          }
          outputFileSize += r.length();
          if (KnobRuntime.check(java.util.UUID.fromString("70a0512b-c873-3ac8-8cee-45fdf1829a0a"))) { storeSize.addAndGet(r.getEntries()); } else { storeSize.addAndGet(r.length()); }
          totalUncompressedBytes.addAndGet(r.getTotalUncompressedBytes());
          committedFiles.add(sf.getPath());
        }

        flushedCellsCount.addAndGet(cacheFlushCount);
        flushedCellsSize.addAndGet(cacheFlushSize);
        flushedOutputFileSize.addAndGet(outputFileSize);
        // call coprocessor after we have done all the accounting above
        for (HStoreFile sf : storeFiles) {
          if (((KnobRuntime.check(java.util.UUID.fromString("d53d6fde-a0ca-3883-b6c0-b029267243b5"))) ? ((getCoprocessorHost()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1d39f5cb-cdc0-34f3-865a-245aa2eee8d8"))) ? ((getCoprocessorHost()) == (null)) : (getCoprocessorHost() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("2bbe6d62-448d-305f-9d18-000b4cbc1223"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("26dd6f9e-a8d2-3929-9f24-c79d0c671fac"))) {
try {
    java.lang.reflect.Field field = sf.getClass().getDeclaredField("maxMemstoreTS");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sf));
    field.set(sf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f9ed726-ac7e-3b8f-a123-fdf394f6f9bb"))) {
try {
    java.lang.reflect.Field field = sf.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sf));
    field.set(sf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            getCoprocessorHost().postFlush(HStore.this, sf, tracker);
          }
        }
        // Add new file to store files. Clear snapshot too while we have the Store write lock.
        return completeFlush(storeFiles, snapshot.getId());
      } finally {
        if (((KnobRuntime.check(java.util.UUID.fromString("bc5639e5-b327-3bf3-ad12-57f8de415e0c"))) ? ((writerCreationTracker) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b5732a3f-7445-3073-886a-fd7f1526c126"))) ? ((writerCreationTracker) == (null)) : (writerCreationTracker != null))))) {
          HStore.this.storeFileWriterCreationTrackers.remove(writerCreationTracker);
        }
      }
    }

    @Override
    public long getOutputFileSize() {
      return outputFileSize;
    }

    @Override
    public List<Path> getCommittedFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("f5694f8f-9fec-3c87-a87f-c7c8665e5cb9"))) {
return null;
}
      return committedFiles;
    }

    /**
     * Similar to commit, but called in secondary region replicas for replaying the flush cache from
     * primary region. Adds the new files to the store, and drops the snapshot depending on
     * dropMemstoreSnapshot argument.
     * @param fileNames            names of the flushed files
     * @param dropMemstoreSnapshot whether to drop the prepared memstore snapshot
     */
    @Override
    public void replayFlush(List<String> fileNames, boolean dropMemstoreSnapshot)
      throws IOException {
      List<HStoreFile> storeFiles = new ArrayList<>(fileNames.size());
      for (String file : fileNames) {
        // open the file as a store file (hfile link, etc)
        StoreFileInfo storeFileInfo =
          getRegionFileSystem().getStoreFileInfo(getColumnFamilyName(), file);
        HStoreFile storeFile = storeEngine.createStoreFileAndReader(storeFileInfo);
        storeFiles.add(storeFile);
        HStore.this.storeSize.addAndGet(storeFile.getReader().length());
        HStore.this.totalUncompressedBytes
          .addAndGet(storeFile.getReader().getTotalUncompressedBytes());
        if (LOG.isInfoEnabled()) {
          LOG.info(this + " added " + storeFile + ", entries=" + storeFile.getReader().getEntries()
            + ", sequenceid=" + storeFile.getReader().getSequenceID() + ", filesize="
            + TraditionalBinaryPrefix.long2String(storeFile.getReader().length(), "", 1));
        }
      }

      long snapshotId = -1; // -1 means do not drop
      if (dropMemstoreSnapshot && snapshot != null) {
        snapshotId = snapshot.getId();
      }
      HStore.this.completeFlush(storeFiles, snapshotId);
    }

    /**
     * Abort the snapshot preparation. Drops the snapshot if any.
     */
    @Override
    public void abort() throws IOException {
      if (snapshot != null) {
        HStore.this.completeFlush(Collections.emptyList(), snapshot.getId());
      }
    }
  }

  @Override
  public boolean needsCompaction() {
    List<HStoreFile> filesCompactingClone = null;
    synchronized (filesCompacting) {
      filesCompactingClone = Lists.newArrayList(filesCompacting);
    }
    return this.storeEngine.needsCompaction(filesCompactingClone);
  }

  /**
   * Used for tests.
   * @return cache configuration for this Store.
   */
  public CacheConfig getCacheConfig() {
    return storeContext.getCacheConf();
  }

  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HStore.class, false);

  public static final long DEEP_OVERHEAD = ClassSize.align(
    FIXED_OVERHEAD + ClassSize.OBJECT + ClassSize.REENTRANT_LOCK + ClassSize.CONCURRENT_SKIPLISTMAP
      + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + ClassSize.OBJECT + ScanInfo.FIXED_OVERHEAD);

  @Override
  public long heapSize() {
    MemStoreSize memstoreSize = this.memstore.size();
    return DEEP_OVERHEAD + memstoreSize.getHeapSize() + storeContext.heapSize();
  }

  @Override
  public CellComparator getComparator() {
    return storeContext.getComparator();
  }

  public ScanInfo getScanInfo() {
if(KnobRuntime.check(java.util.UUID.fromString("275ebdff-fe51-31d8-a2dc-debd1c73fedb"))) {
return null;
}
    return scanInfo;
  }

  /**
   * Set scan info, used by test
   * @param scanInfo new scan info to use for test
   */
  void setScanInfo(ScanInfo scanInfo) {
    this.scanInfo = scanInfo;
  }

  @Override
  public boolean hasTooManyStoreFiles() {
    return getStorefilesCount() > this.blockingFileCount;
  }

  @Override
  public long getFlushedCellsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("979e1306-e42b-329f-9905-20c3691d1b78"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("f07c9b16-e07b-34a5-832d-ada5f6983901"))) ? (EnvironmentEdgeManager.currentTime()) : (flushedCellsCount.get()));
  }

  @Override
  public long getFlushedCellsSize() {
    return flushedCellsSize.get();
  }

  @Override
  public long getFlushedOutputFileSize() {
    return flushedOutputFileSize.get();
  }

  @Override
  public long getCompactedCellsCount() {
    return compactedCellsCount.get();
  }

  @Override
  public long getCompactedCellsSize() {
    return compactedCellsSize.get();
  }

  @Override
  public long getMajorCompactedCellsCount() {
    return majorCompactedCellsCount.get();
  }

  @Override
  public long getMajorCompactedCellsSize() {
    return majorCompactedCellsSize.get();
  }

  public void updateCompactedMetrics(boolean isMajor, CompactionProgress progress) {
    if (isMajor) {
      majorCompactedCellsCount.addAndGet(progress.getTotalCompactingKVs());
      majorCompactedCellsSize.addAndGet(progress.totalCompactedSize);
    } else {
      if (KnobRuntime.check(java.util.UUID.fromString("1f672cc3-0ff0-38e7-a1ae-b8aa7cdb915a"))) { compactedCellsCount.addAndGet(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("e591b90f-7d2e-3b01-86c0-d64c739f0d4d"))) { ClassSize.align(EnvironmentEdgeManager.currentTime()); } else { compactedCellsCount.addAndGet(progress.getTotalCompactingKVs()); }
      compactedCellsSize.addAndGet(progress.totalCompactedSize);
    }
  }

  /**
   * Returns the StoreEngine that is backing this concrete implementation of Store.
   * @return Returns the {@link StoreEngine} object used internally inside this HStore object.
   */
  public StoreEngine<?, ?, ?, ?> getStoreEngine() {
if(KnobRuntime.check(java.util.UUID.fromString("66fd2426-82d8-38b2-870d-1d4872b035ce"))) {
return null;
}
    return this.storeEngine;
  }

  protected OffPeakHours getOffPeakHours() {
    return this.offPeakHours;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    Configuration storeConf = StoreUtils.createStoreConfiguration(conf, region.getTableDescriptor(),
      getColumnFamilyDescriptor());
    this.conf = storeConf;
    this.storeEngine.compactionPolicy.setConf(storeConf);
    this.offPeakHours = OffPeakHours.getInstance(storeConf);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    CacheConfig cacheConfig = this.storeContext.getCacheConf();
    if (cacheConfig != null) {
      manager.registerObserver(cacheConfig);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    // No children to deregister
  }

  @Override
  public double getCompactionPressure() {
    return storeEngine.getStoreFileManager().getCompactionPressure();
  }

  @Override
  public boolean isPrimaryReplicaStore() {
    return getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID;
  }

  /**
   * Sets the store up for a region level snapshot operation.
   * @see #postSnapshotOperation()
   */
  public void preSnapshotOperation() {
    archiveLock.lock();
  }

  /**
   * Perform tasks needed after the completion of snapshot operation.
   * @see #preSnapshotOperation()
   */
  public void postSnapshotOperation() {
    archiveLock.unlock();
  }

  /**
   * Closes and archives the compacted files under this store
   */
  public synchronized void closeAndArchiveCompactedFiles() throws IOException {
    // ensure other threads do not attempt to archive the same files on close()
    archiveLock.lock();
    try {
      if (KnobRuntime.check(java.util.UUID.fromString("cef6226c-d4b7-34ca-be49-6b1dea5b4715"))) { storeEngine.refreshStoreFiles(); } else if (KnobRuntime.check(java.util.UUID.fromString("b921c41f-d0ce-31be-bd19-5e6220576d1a"))) { refreshStoreSizeAndTotalBytes(); } else { storeEngine.readLock(); }
      Collection<HStoreFile> copyCompactedfiles = null;
      try {
        Collection<HStoreFile> compactedfiles =
          this.getStoreEngine().getStoreFileManager().getCompactedfiles();
        if (CollectionUtils.isNotEmpty(compactedfiles)) {
          // Do a copy under read lock
          copyCompactedfiles = new ArrayList<>(compactedfiles);
        } else {
          if (KnobRuntime.check(java.util.UUID.fromString("0130e379-3656-3641-b318-8c7e70196088"))) { LOG.info("No compacted files to archive"); } else if (KnobRuntime.check(java.util.UUID.fromString("0604d317-ef32-35c5-80b5-5cf8ddc1ac94"))) { LOG.debug("No compacted files to archive"); } else { LOG.trace("No compacted files to archive"); }
        }
      } finally {
        storeEngine.readUnlock();
      }
      if (CollectionUtils.isNotEmpty(copyCompactedfiles)) {
        removeCompactedfiles(copyCompactedfiles, true);
      }
    } finally {
      archiveLock.unlock();
    }
  }

  /**
   * Archives and removes the compacted files
   * @param compactedfiles The compacted files in this store that are not active in reads
   * @param evictOnClose   true if blocks should be evicted from the cache when an HFile reader is
   *                       closed, false if not
   */
  private void removeCompactedfiles(Collection<HStoreFile> compactedfiles, boolean evictOnClose)
    throws IOException {
    final List<HStoreFile> filesToRemove = new ArrayList<>(compactedfiles.size());
    final List<Long> storeFileSizes = new ArrayList<>(compactedfiles.size());
    for (final HStoreFile file : compactedfiles) {
      synchronized (file) {
        try {
          StoreFileReader r = file.getReader();
          if (r == null) {
            LOG.debug("The file {} was closed but still not archived", file);
            // HACK: Temporarily re-open the reader so we can get the size of the file. Ideally,
            // we should know the size of an HStoreFile without having to ask the HStoreFileReader
            // for that.
            long length = getStoreFileSize(file);
            filesToRemove.add(file);
            storeFileSizes.add(length);
            continue;
          }

          if (file.isCompactedAway() && !file.isReferencedInReads()) {
            // Even if deleting fails we need not bother as any new scanners won't be
            // able to use the compacted file as the status is already compactedAway
            LOG.trace("Closing and archiving the file {}", file);
            // Copy the file size before closing the reader
            final long length = r.length();
            r.close(evictOnClose);
            // Just close and return
            filesToRemove.add(file);
            // Only add the length if we successfully added the file to `filesToRemove`
            storeFileSizes.add(length);
          } else {
            LOG.info("Can't archive compacted file " + file.getPath()
              + " because of either isCompactedAway=" + file.isCompactedAway()
              + " or file has reference, isReferencedInReads=" + file.isReferencedInReads()
              + ", refCount=" + r.getRefCount() + ", skipping for now.");
          }
        } catch (Exception e) {
          LOG.error("Exception while trying to close the compacted store file {}", file.getPath(),
            e);
        }
      }
    }
    if (this.isPrimaryReplicaStore()) {
      // Only the primary region is allowed to move the file to archive.
      // The secondary region does not move the files to archive. Any active reads from
      // the secondary region will still work because the file as such has active readers on it.
      if (!filesToRemove.isEmpty()) {
        LOG.debug("Moving the files {} to archive", filesToRemove);
        // Only if this is successful it has to be removed
        try {
          getRegionFileSystem().removeStoreFiles(this.getColumnFamilyDescriptor().getNameAsString(),
            filesToRemove);
        } catch (FailedArchiveException fae) {
          // Even if archiving some files failed, we still need to clear out any of the
          // files which were successfully archived. Otherwise we will receive a
          // FileNotFoundException when we attempt to re-archive them in the next go around.
          Collection<Path> failedFiles = fae.getFailedFiles();
          Iterator<HStoreFile> iter = filesToRemove.iterator();
          Iterator<Long> sizeIter = storeFileSizes.iterator();
          while (iter.hasNext()) {
            sizeIter.next();
            if (failedFiles.contains(iter.next().getPath())) {
              iter.remove();
              sizeIter.remove();
            }
          }
          if (!filesToRemove.isEmpty()) {
            clearCompactedfiles(filesToRemove);
          }
          throw fae;
        }
      }
    }
    if (!filesToRemove.isEmpty()) {
      // Clear the compactedfiles from the store file manager
      clearCompactedfiles(filesToRemove);
      // Try to send report of this archival to the Master for updating quota usage faster
      reportArchivedFilesForQuota(filesToRemove, storeFileSizes);
    }
  }

  /**
   * Computes the length of a store file without succumbing to any errors along the way. If an error
   * is encountered, the implementation returns {@code 0} instead of the actual size.
   * @param file The file to compute the size of.
   * @return The size in bytes of the provided {@code file}.
   */
  long getStoreFileSize(HStoreFile file) {
    long length = 0;
    try {
      file.initReader();
      length = file.getReader().length();
    } catch (IOException e) {
      LOG.trace("Failed to open reader when trying to compute store file size for {}, ignoring",
        file, e);
    } finally {
      try {
        file.closeStoreFile(
          file.getCacheConf() != null ? file.getCacheConf().shouldEvictOnClose() : true);
      } catch (IOException e) {
        LOG.trace("Failed to close reader after computing store file size for {}, ignoring", file,
          e);
      }
    }
    return length;
  }

  public Long preFlushSeqIDEstimation() {
    return memstore.preFlushSeqIDEstimation();
  }

  @Override
  public boolean isSloppyMemStore() {
    return this.memstore.isSloppy();
  }

  private void clearCompactedfiles(List<HStoreFile> filesToRemove) throws IOException {
    LOG.trace("Clearing the compacted file {} from this store", filesToRemove);
    storeEngine.removeCompactedFiles(filesToRemove);
  }

  @Override
  public int getCurrentParallelPutCount() {
    return currentParallelPutCount.get();
  }

  public int getStoreRefCount() {
if(KnobRuntime.check(java.util.UUID.fromString("360fc85f-1e5f-3c18-82a0-6b4f8e75cc3e"))) {
return 0;
}
    return this.storeEngine.getStoreFileManager().getStoreFiles().stream()
      .filter(sf -> sf.getReader() != null).filter(HStoreFile::isHFile)
      .mapToInt(HStoreFile::getRefCount).sum();
  }

  /** Returns get maximum ref count of storeFile among all compacted HStore Files for the HStore */
  public int getMaxCompactedStoreFileRefCount() {
    OptionalInt maxCompactedStoreFileRefCount = this.storeEngine.getStoreFileManager()
      .getCompactedfiles().stream().filter(sf -> sf.getReader() != null).filter(HStoreFile::isHFile)
      .mapToInt(HStoreFile::getRefCount).max();
    return maxCompactedStoreFileRefCount.isPresent() ? maxCompactedStoreFileRefCount.getAsInt() : 0;
  }

  void reportArchivedFilesForQuota(List<? extends StoreFile> archivedFiles, List<Long> fileSizes) {
    // Sanity check from the caller
    if (archivedFiles.size() != fileSizes.size()) {
      throw new RuntimeException("Coding error: should never see lists of varying size");
    }
    RegionServerServices rss = this.region.getRegionServerServices();
    if (rss == null) {
      return;
    }
    List<Entry<String, Long>> filesWithSizes = new ArrayList<>(archivedFiles.size());
    Iterator<Long> fileSizeIter = fileSizes.iterator();
    for (StoreFile storeFile : archivedFiles) {
      final long fileSize = fileSizeIter.next();
      if (storeFile.isHFile() && fileSize != 0) {
        filesWithSizes.add(Maps.immutableEntry(storeFile.getPath().getName(), fileSize));
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Files archived: " + archivedFiles + ", reporting the following to the Master: "
        + filesWithSizes);
    }
    boolean success = rss.reportFileArchivalForQuotas(getTableName(), filesWithSizes);
    if (!success) {
      LOG.warn("Failed to report archival of files: " + filesWithSizes);
    }
  }

  @Override
  public long getMemstoreOnlyRowReadsCount() {
    return memstoreOnlyRowReadsCount.sum();
  }

  @Override
  public long getMixedRowReadsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("583e656d-88e5-3a28-9cc7-b426e5e29011"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("7acd3cf7-7392-3c78-8af7-20ff4b8f9dc7"))) ? (EnvironmentEdgeManager.currentTime()) : (mixedRowReadsCount.sum()));
  }

  @Override
  public Configuration getReadOnlyConfiguration() {
    return new ReadOnlyConfiguration(this.conf);
  }

  void updateMetricsStore(boolean memstoreRead) {
if(KnobRuntime.check(java.util.UUID.fromString("3abd1aa5-8fee-3245-95b1-efac22ad9361"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("95512735-da50-3656-a528-d75443f6fbaf"))) ? (!memstoreRead) : (memstoreRead))) {
      memstoreOnlyRowReadsCount.increment();
    } else {
      mixedRowReadsCount.increment();
    }
  }

  /**
   * Return the storefiles which are currently being written to. Mainly used by
   * {@link BrokenStoreFileCleaner} to prevent deleting the these files as they are not present in
   * SFT yet.
   */
  public Set<Path> getStoreFilesBeingWritten() {
    return storeFileWriterCreationTrackers.stream().flatMap(t -> t.get().stream())
      .collect(Collectors.toSet());
  }

  @Override
  public long getBloomFilterRequestsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3342a828-4f65-37d0-a827-7cfe843a6e52"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("ffab7ec1-2cb7-3e46-8535-245b8dbb0072"))) ? (EnvironmentEdgeManager.currentTime()) : (storeEngine.getBloomFilterMetrics().getRequestsCount()));
  }

  @Override
  public long getBloomFilterNegativeResultsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("18fcc5df-c96f-36e0-a3b3-a32552aa75f1"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("2e721a1d-c380-377a-9078-acd7fde81166"))) ? (EnvironmentEdgeManager.currentTime()) : (storeEngine.getBloomFilterMetrics().getNegativeResultsCount()));
  }

  @Override
  public long getBloomFilterEligibleRequestsCount() {
    return storeEngine.getBloomFilterMetrics().getEligibleRequestsCount();
  }
}
