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

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_LOCAL;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAJOR_COMPACTION_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.REGION_NAMES_KEY;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.ROW_LOCK_READ_LOCK_KEY;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import com.google.errorprone.annotations.RestrictedApi;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.opentelemetry.api.trace.Span;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.RandomAccess;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ReadOnlyConfiguration;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.metrics.MetricsTableRequests;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.StoreHotnessProtector;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALSyncTimeoutIOException;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationObserver;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.wal.WALSplitUtil.MutationReplay;
import org.apache.hadoop.hbase.wal.WALStreamReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.StoreFlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * Regions store data for a certain region of a table. It stores all columns for each row. A given
 * table consists of one or more Regions.
 * <p>
 * An Region is defined by its table and its key extent.
 * <p>
 * Locking at the Region level serves only one purpose: preventing the region from being closed (and
 * consequently split) while other operations are ongoing. Each row level operation obtains both a
 * row lock and a region read lock for the duration of the operation. While a scanner is being
 * constructed, getScanner holds a read lock. If the scanner is successfully constructed, it holds a
 * read lock until it is closed. A close takes out a write lock and consequently will block for
 * ongoing operations and will block new operations from starting while the close is in progress.
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Private
public class HRegion implements HeapSize, PropagatingConfigurationObserver, Region {
  private static final Logger LOG = LoggerFactory.getLogger(HRegion.class);

  public static final String LOAD_CFS_ON_DEMAND_CONFIG_KEY =
    "hbase.hregion.scan.loadColumnFamiliesOnDemand";

  public static final String HBASE_MAX_CELL_SIZE_KEY = "hbase.server.keyvalue.maxsize";
  public static final int DEFAULT_MAX_CELL_SIZE = 10485760;

  public static final String HBASE_REGIONSERVER_MINIBATCH_SIZE =
    "hbase.regionserver.minibatch.size";
  public static final int DEFAULT_HBASE_REGIONSERVER_MINIBATCH_SIZE = 20000;

  public static final String WAL_HSYNC_CONF_KEY = "hbase.wal.hsync";
  public static final boolean DEFAULT_WAL_HSYNC = false;

  /** Parameter name for compaction after bulkload */
  public static final String COMPACTION_AFTER_BULKLOAD_ENABLE =
    "hbase.compaction.after.bulkload.enable";

  /** Config for allow split when file count greater than the configured blocking file count */
  public static final String SPLIT_IGNORE_BLOCKING_ENABLED_KEY =
    "hbase.hregion.split.ignore.blocking.enabled";

  public static final String REGION_STORAGE_POLICY_KEY = "hbase.hregion.block.storage.policy";
  public static final String DEFAULT_REGION_STORAGE_POLICY = "NONE";

  /**
   * This is for for using HRegion as a local storage, where we may put the recovered edits in a
   * special place. Once this is set, we will only replay the recovered edits under this directory
   * and ignore the original replay directory configs.
   */
  public static final String SPECIAL_RECOVERED_EDITS_DIR =
    "hbase.hregion.special.recovered.edits.dir";

  /**
   * Mainly used for master local region, where we will replay the WAL file directly without
   * splitting, so it is possible to have WAL files which are not closed cleanly, in this way,
   * hitting EOF is expected so should not consider it as a critical problem.
   */
  public static final String RECOVERED_EDITS_IGNORE_EOF =
    "hbase.hregion.recovered.edits.ignore.eof";

  /**
   * Whether to use {@link MetaCellComparator} even if we are not meta region. Used when creating
   * master local region.
   */
  public static final String USE_META_CELL_COMPARATOR = "hbase.region.use.meta.cell.comparator";

  public static final boolean DEFAULT_USE_META_CELL_COMPARATOR = false;

  final AtomicBoolean closed = new AtomicBoolean(false);

  /*
   * Closing can take some time; use the closing flag if there is stuff we don't want to do while in
   * closing state; e.g. like offer this region up to the master as a region to close if the
   * carrying regionserver is overloaded. Once set, it is never cleared.
   */
  final AtomicBoolean closing = new AtomicBoolean(false);

  /**
   * The max sequence id of flushed data on this region. There is no edit in memory that is less
   * that this sequence id.
   */
  private volatile long maxFlushedSeqId = HConstants.NO_SEQNUM;

  /**
   * Record the sequence id of last flush operation. Can be in advance of {@link #maxFlushedSeqId}
   * when flushing a single column family. In this case, {@link #maxFlushedSeqId} will be older than
   * the oldest edit in memory.
   */
  private volatile long lastFlushOpSeqId = HConstants.NO_SEQNUM;

  /**
   * The sequence id of the last replayed open region event from the primary region. This is used to
   * skip entries before this due to the possibility of replay edits coming out of order from
   * replication.
   */
  protected volatile long lastReplayedOpenRegionSeqId = -1L;
  protected volatile long lastReplayedCompactionSeqId = -1L;

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  // map from a locked row to the context for that lock including:
  // - CountDownLatch for threads waiting on that row
  // - the thread that owns the lock (allow reentrancy)
  // - reference count of (reentrant) locks held by the thread
  // - the row itself
  private final ConcurrentHashMap<HashedBytes, RowLockContext> lockedRows =
    new ConcurrentHashMap<>();

  protected final Map<byte[], HStore> stores =
    new ConcurrentSkipListMap<>(Bytes.BYTES_RAWCOMPARATOR);

  // TODO: account for each registered handler in HeapSize computation
  private Map<String, com.google.protobuf.Service> coprocessorServiceHandlers = Maps.newHashMap();

  // Track data size in all memstores
  private final MemStoreSizing memStoreSizing = new ThreadSafeMemStoreSizing();
  RegionServicesForStores regionServicesForStores;

  // Debug possible data loss due to WAL off
  final LongAdder numMutationsWithoutWAL = new LongAdder();
  final LongAdder dataInMemoryWithoutWAL = new LongAdder();

  // Debug why CAS operations are taking a while.
  final LongAdder checkAndMutateChecksPassed = new LongAdder();
  final LongAdder checkAndMutateChecksFailed = new LongAdder();

  // Number of requests
  // Count rows for scan
  final LongAdder readRequestsCount = new LongAdder();
  final LongAdder filteredReadRequestsCount = new LongAdder();
  // Count rows for multi row mutations
  final LongAdder writeRequestsCount = new LongAdder();

  // Number of requests blocked by memstore size.
  private final LongAdder blockedRequestsCount = new LongAdder();

  // Compaction LongAdders
  final LongAdder compactionsFinished = new LongAdder();
  final LongAdder compactionsFailed = new LongAdder();
  final LongAdder compactionNumFilesCompacted = new LongAdder();
  final LongAdder compactionNumBytesCompacted = new LongAdder();
  final LongAdder compactionsQueued = new LongAdder();
  final LongAdder flushesQueued = new LongAdder();

  private BlockCache blockCache;
  private MobFileCache mobFileCache;
  private final WAL wal;
  private final HRegionFileSystem fs;
  protected final Configuration conf;
  private final Configuration baseConf;
  private final int rowLockWaitDuration;
  static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;

  private Path regionWalDir;
  private FileSystem walFS;

  // set to true if the region is restored from snapshot
  private boolean isRestoredRegion = false;

  public void setRestoredRegion(boolean restoredRegion) {
    isRestoredRegion = restoredRegion;
  }

  public MetricsTableRequests getMetricsTableRequests() {
    return metricsTableRequests;
  }

  // Handle table latency metrics
  private MetricsTableRequests metricsTableRequests;

  // The internal wait duration to acquire a lock before read/update
  // from the region. It is not per row. The purpose of this wait time
  // is to avoid waiting a long time while the region is busy, so that
  // we can release the IPC handler soon enough to improve the
  // availability of the region server. It can be adjusted by
  // tuning configuration "hbase.busy.wait.duration".
  final long busyWaitDuration;
  static final long DEFAULT_BUSY_WAIT_DURATION = HConstants.DEFAULT_HBASE_RPC_TIMEOUT;

  // If updating multiple rows in one call, wait longer,
  // i.e. waiting for busyWaitDuration * # of rows. However,
  // we can limit the max multiplier.
  final int maxBusyWaitMultiplier;

  // Max busy wait duration. There is no point to wait longer than the RPC
  // purge timeout, when a RPC call will be terminated by the RPC engine.
  final long maxBusyWaitDuration;

  // Max cell size. If nonzero, the maximum allowed size for any given cell
  // in bytes
  final long maxCellSize;

  // Number of mutations for minibatch processing.
  private final int miniBatchSize;

  // negative number indicates infinite timeout
  static final long DEFAULT_ROW_PROCESSOR_TIMEOUT = 60 * 1000L;
  final ExecutorService rowProcessorExecutor = Executors.newCachedThreadPool();

  final ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;
  final ReadPointCalculationLock smallestReadPointCalcLock;

  /**
   * The sequence ID that was enLongAddered when this region was opened.
   */
  private long openSeqNum = HConstants.NO_SEQNUM;

  /**
   * The default setting for whether to enable on-demand CF loading for scan requests to this
   * region. Requests can override it.
   */
  private boolean isLoadingCfsOnDemandDefault = false;

  private final AtomicInteger majorInProgress = new AtomicInteger(0);
  private final AtomicInteger minorInProgress = new AtomicInteger(0);

  //
  // Context: During replay we want to ensure that we do not lose any data. So, we
  // have to be conservative in how we replay wals. For each store, we calculate
  // the maxSeqId up to which the store was flushed. And, skip the edits which
  // are equal to or lower than maxSeqId for each store.
  // The following map is populated when opening the region
  Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /** Saved state from replaying prepare flush cache */
  private PrepareFlushResult prepareFlushResult = null;

  private volatile ConfigurationManager configurationManager;

  // Used for testing.
  private volatile Long timeoutForWriteLock = null;

  private final CellComparator cellComparator;

  private final int minBlockSizeBytes;

  /**
   * @return The smallest mvcc readPoint across all the scanners in this region. Writes older than
   *         this readPoint, are included in every read operation.
   */
  public long getSmallestReadPoint() {
    // We need to ensure that while we are calculating the smallestReadPoint
    // no new RegionScanners can grab a readPoint that we are unaware of.
    smallestReadPointCalcLock.lock(ReadPointCalculationLock.LockType.CALCULATION_LOCK);
    try {
      long minimumReadPoint = mvcc.getReadPoint();
      for (Long readPoint : this.scannerReadPoints.values()) {
        minimumReadPoint = Math.min(minimumReadPoint, readPoint);
      }
      return minimumReadPoint;
    } finally {
      smallestReadPointCalcLock.unlock(ReadPointCalculationLock.LockType.CALCULATION_LOCK);
    }
  }

  /*
   * Data structure of write state flags used coordinating flushes, compactions and closes.
   */
  static class WriteState {
    // Set while a memstore flush is happening.
    volatile boolean flushing = false;
    // Set when a flush has been requested.
    volatile boolean flushRequested = false;
    // Number of compactions running.
    AtomicInteger compacting = new AtomicInteger(0);
    // Gets set in close. If set, cannot compact or flush again.
    volatile boolean writesEnabled = true;
    // Set if region is read-only
    volatile boolean readOnly = false;
    // whether the reads are enabled. This is different than readOnly, because readOnly is
    // static in the lifetime of the region, while readsEnabled is dynamic
    volatile boolean readsEnabled = true;

    /**
     * Set flags that make this region read-only.
     * @param onOff flip value for region r/o setting
     */
    synchronized void setReadOnly(final boolean onOff) {
      this.writesEnabled = !onOff;
      this.readOnly = onOff;
    }

    boolean isReadOnly() {
if(KnobRuntime.check(java.util.UUID.fromString("b6ccbe19-b5ad-3b97-81de-1b1a543c767d"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6d33759-17e2-3f4f-a0f9-7ed1e4e94aaa"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("dc3db928-fe32-384c-b2ce-9e1ac8352411"))) ? (!this.readOnly) : (this.readOnly));
    }

    boolean isFlushRequested() {
if(KnobRuntime.check(java.util.UUID.fromString("8fd51796-ab35-3651-9ed9-916a4e38d9f2"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("eeba4e5d-217f-3e1b-9dc9-64fcd11bc43d"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("1c6b8613-b8c2-3d3a-b50a-c6c42f00c68d"))) ? (!this.flushRequested) : (this.flushRequested));
    }

    void setReadsEnabled(boolean readsEnabled) {
      this.readsEnabled = readsEnabled;
    }

    static final long HEAP_SIZE = ClassSize.align(ClassSize.OBJECT + 5 * Bytes.SIZEOF_BOOLEAN);
  }

  /**
   * Objects from this class are created when flushing to describe all the different states that
   * that method ends up in. The Result enum describes those states. The sequence id should only be
   * specified if the flush was successful, and the failure message should only be specified if it
   * didn't flush.
   */
  public static class FlushResultImpl implements FlushResult {
    final Result result;
    final String failureReason;
    final long flushSequenceId;
    final boolean wroteFlushWalMarker;

    /**
     * Convenience constructor to use when the flush is successful, the failure message is set to
     * null.
     * @param result          Expecting FLUSHED_NO_COMPACTION_NEEDED or FLUSHED_COMPACTION_NEEDED.
     * @param flushSequenceId Generated sequence id that comes right after the edits in the
     *                        memstores.
     */
    FlushResultImpl(Result result, long flushSequenceId) {
      this(result, flushSequenceId, null, false);
      assert result == Result.FLUSHED_NO_COMPACTION_NEEDED
        || result == Result.FLUSHED_COMPACTION_NEEDED;
    }

    /**
     * Convenience constructor to use when we cannot flush.
     * @param result        Expecting CANNOT_FLUSH_MEMSTORE_EMPTY or CANNOT_FLUSH.
     * @param failureReason Reason why we couldn't flush.
     */
    FlushResultImpl(Result result, String failureReason, boolean wroteFlushMarker) {
      this(result, -1, failureReason, wroteFlushMarker);
      assert result == Result.CANNOT_FLUSH_MEMSTORE_EMPTY || result == Result.CANNOT_FLUSH;
    }

    /**
     * Constructor with all the parameters.
     * @param result          Any of the Result.
     * @param flushSequenceId Generated sequence id if the memstores were flushed else -1.
     * @param failureReason   Reason why we couldn't flush, or null.
     */
    FlushResultImpl(Result result, long flushSequenceId, String failureReason,
      boolean wroteFlushMarker) {
      this.result = result;
      this.flushSequenceId = flushSequenceId;
      this.failureReason = failureReason;
      this.wroteFlushWalMarker = wroteFlushMarker;
    }

    /**
     * Convenience method, the equivalent of checking if result is FLUSHED_NO_COMPACTION_NEEDED or
     * FLUSHED_NO_COMPACTION_NEEDED.
     * @return true if the memstores were flushed, else false.
     */
    @Override
    public boolean isFlushSucceeded() {
      return result == Result.FLUSHED_NO_COMPACTION_NEEDED
        || result == Result.FLUSHED_COMPACTION_NEEDED;
    }

    /**
     * Convenience method, the equivalent of checking if result is FLUSHED_COMPACTION_NEEDED.
     * @return True if the flush requested a compaction, else false (doesn't even mean it flushed).
     */
    @Override
    public boolean isCompactionNeeded() {
if(KnobRuntime.check(java.util.UUID.fromString("ee651877-d1e5-3882-b44e-e730999ab454"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("90d48e1c-b71f-3bf7-a1ae-e1cb6962b5df"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("80d2b313-a8b2-3934-89a9-aabac599f916"))) ? ((result) != (Result.FLUSHED_COMPACTION_NEEDED)) : (((KnobRuntime.check(java.util.UUID.fromString("5e54645a-d242-36c4-93ba-d203700333aa"))) ? ((result) == (Result.FLUSHED_COMPACTION_NEEDED)) : (result == Result.FLUSHED_COMPACTION_NEEDED))));
    }

    @Override
    public String toString() {
      return new StringBuilder().append("flush result:").append(result).append(", ")
        .append("failureReason:").append(failureReason).append(",").append("flush seq id")
        .append(flushSequenceId).toString();
    }

    @Override
    public Result getResult() {
      return result;
    }
  }

  /** A result object from prepare flush cache stage */
  static class PrepareFlushResult {
    final FlushResultImpl result; // indicating a failure result from prepare
    final TreeMap<byte[], StoreFlushContext> storeFlushCtxs;
    final TreeMap<byte[], List<Path>> committedFiles;
    final TreeMap<byte[], MemStoreSize> storeFlushableSize;
    final long startTime;
    final long flushOpSeqId;
    final long flushedSeqId;
    final MemStoreSizing totalFlushableSize;

    /** Constructs an early exit case */
    PrepareFlushResult(FlushResultImpl result, long flushSeqId) {
      this(result, null, null, null, Math.max(0, flushSeqId), 0, 0, MemStoreSizing.DUD);
    }

    /** Constructs a successful prepare flush result */
    PrepareFlushResult(TreeMap<byte[], StoreFlushContext> storeFlushCtxs,
      TreeMap<byte[], List<Path>> committedFiles, TreeMap<byte[], MemStoreSize> storeFlushableSize,
      long startTime, long flushSeqId, long flushedSeqId, MemStoreSizing totalFlushableSize) {
      this(null, storeFlushCtxs, committedFiles, storeFlushableSize, startTime, flushSeqId,
        flushedSeqId, totalFlushableSize);
    }

    private PrepareFlushResult(FlushResultImpl result,
      TreeMap<byte[], StoreFlushContext> storeFlushCtxs, TreeMap<byte[], List<Path>> committedFiles,
      TreeMap<byte[], MemStoreSize> storeFlushableSize, long startTime, long flushSeqId,
      long flushedSeqId, MemStoreSizing totalFlushableSize) {
      this.result = result;
      this.storeFlushCtxs = storeFlushCtxs;
      this.committedFiles = committedFiles;
      this.storeFlushableSize = storeFlushableSize;
      this.startTime = startTime;
      this.flushOpSeqId = flushSeqId;
      this.flushedSeqId = flushedSeqId;
      this.totalFlushableSize = totalFlushableSize;
    }

    public FlushResult getResult() {
if(KnobRuntime.check(java.util.UUID.fromString("abd723b4-0f92-32b7-a9af-a5278cc8c3e7"))) {
return null;
}
      return this.result;
    }
  }

  /**
   * A class that tracks exceptions that have been observed in one batch. Not thread safe.
   */
  static class ObservedExceptionsInBatch {
    private boolean wrongRegion = false;
    private boolean failedSanityCheck = false;
    private boolean wrongFamily = false;

    /** Returns If a {@link WrongRegionException} has been observed. */
    boolean hasSeenWrongRegion() {
if(KnobRuntime.check(java.util.UUID.fromString("7dbba451-755f-3705-a223-c64992049c31"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("46bf7bca-1fc6-3654-81ca-62746d4c5669"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("dddd3aa9-ed5f-3702-8e34-507a11c3eb55"))) ? (!wrongRegion) : (wrongRegion));
    }

    /**
     * Records that a {@link WrongRegionException} has been observed.
     */
    void sawWrongRegion() {
      wrongRegion = true;
    }

    /** Returns If a {@link FailedSanityCheckException} has been observed. */
    boolean hasSeenFailedSanityCheck() {
if(KnobRuntime.check(java.util.UUID.fromString("c8f624b0-31d8-3562-8d04-1911a743888e"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("fcd9548c-c218-3925-9eef-28df21f57973"))) {
return true;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("0e942372-a55b-3080-ae1a-681fe566a372"))) ? (!failedSanityCheck) : (failedSanityCheck));
    }

    /**
     * Records that a {@link FailedSanityCheckException} has been observed.
     */
    void sawFailedSanityCheck() {
      failedSanityCheck = true;
    }

    /** Returns If a {@link NoSuchColumnFamilyException} has been observed. */
    boolean hasSeenNoSuchFamily() {
      return wrongFamily;
    }

    /**
     * Records that a {@link NoSuchColumnFamilyException} has been observed.
     */
    void sawNoSuchFamily() {
      wrongFamily = true;
    }
  }

  final WriteState writestate = new WriteState();

  long memstoreFlushSize;
  final long timestampSlop;
  final long rowProcessorTimeout;

  // Last flush time for each Store. Useful when we are flushing for each column
  private final ConcurrentMap<HStore, Long> lastStoreFlushTimeMap = new ConcurrentHashMap<>();

  protected RegionServerServices rsServices;
  private RegionServerAccounting rsAccounting;
  private long flushCheckInterval;
  // flushPerChanges is to prevent too many changes in memstore
  private long flushPerChanges;
  private long blockingMemStoreSize;
  // Used to guard closes
  final ReentrantReadWriteLock lock;
  // Used to track interruptible holders of the region lock. Currently that is only RPC handler
  // threads. Boolean value in map determines if lock holder can be interrupted, normally true,
  // but may be false when thread is transiting a critical section.
  final ConcurrentHashMap<Thread, Boolean> regionLockHolders;

  // Stop updates lock
  private final ReentrantReadWriteLock updatesLock = new ReentrantReadWriteLock();

  private final MultiVersionConcurrencyControl mvcc;

  // Coprocessor host
  private volatile RegionCoprocessorHost coprocessorHost;

  private TableDescriptor htableDescriptor = null;
  private RegionSplitPolicy splitPolicy;
  private RegionSplitRestriction splitRestriction;
  private FlushPolicy flushPolicy;

  private final MetricsRegion metricsRegion;
  private final MetricsRegionWrapperImpl metricsRegionWrapper;
  private final Durability regionDurability;
  private final boolean regionStatsEnabled;
  // Stores the replication scope of the various column families of the table
  // that has non-default scope
  private final NavigableMap<byte[], Integer> replicationScope =
    new TreeMap<>(Bytes.BYTES_COMPARATOR);

  private final StoreHotnessProtector storeHotnessProtector;

  /**
   * HRegion constructor. This constructor should only be used for testing and extensions. Instances
   * of HRegion should be instantiated with the {@link HRegion#createHRegion} or
   * {@link HRegion#openHRegion} method.
   * @param tableDir   qualified path of directory where region should be located, usually the table
   *                   directory.
   * @param wal        The WAL is the outbound log for any updates to the HRegion The wal file is a
   *                   logfile from the previous execution that's custom-computed for this HRegion.
   *                   The HRegionServer computes and sorts the appropriate wal info for this
   *                   HRegion. If there is a previous wal file (implying that the HRegion has been
   *                   written-to before), then read it from the supplied path.
   * @param fs         is the filesystem.
   * @param confParam  is global configuration settings.
   * @param regionInfo - RegionInfo that describes the region is new), then read them from the
   *                   supplied path.
   * @param htd        the table descriptor
   * @param rsServices reference to {@link RegionServerServices} or null
   * @deprecated Use other constructors.
   */
  @Deprecated
  public HRegion(final Path tableDir, final WAL wal, final FileSystem fs,
    final Configuration confParam, final RegionInfo regionInfo, final TableDescriptor htd,
    final RegionServerServices rsServices) {
    this(new HRegionFileSystem(confParam, fs, tableDir, regionInfo), wal, confParam, htd,
      rsServices);
  }

  /**
   * HRegion constructor. This constructor should only be used for testing and extensions. Instances
   * of HRegion should be instantiated with the {@link HRegion#createHRegion} or
   * {@link HRegion#openHRegion} method.
   * @param fs         is the filesystem.
   * @param wal        The WAL is the outbound log for any updates to the HRegion The wal file is a
   *                   logfile from the previous execution that's custom-computed for this HRegion.
   *                   The HRegionServer computes and sorts the appropriate wal info for this
   *                   HRegion. If there is a previous wal file (implying that the HRegion has been
   *                   written-to before), then read it from the supplied path.
   * @param confParam  is global configuration settings.
   * @param htd        the table descriptor
   * @param rsServices reference to {@link RegionServerServices} or null
   */
  public HRegion(final HRegionFileSystem fs, final WAL wal, final Configuration confParam,
    final TableDescriptor htd, final RegionServerServices rsServices) {
    if (htd == null) {
      throw new IllegalArgumentException("Need table descriptor");
    }

    if (confParam instanceof CompoundConfiguration) {
      throw new IllegalArgumentException("Need original base configuration");
    }

    this.wal = wal;
    this.fs = fs;
    this.mvcc = new MultiVersionConcurrencyControl(getRegionInfo().getShortNameToLog());

    // 'conf' renamed to 'confParam' b/c we use this.conf in the constructor
    this.baseConf = confParam;
    this.conf = new CompoundConfiguration().add(confParam).addBytesMap(htd.getValues());
    this.cellComparator = htd.isMetaTable()
      || conf.getBoolean(USE_META_CELL_COMPARATOR, DEFAULT_USE_META_CELL_COMPARATOR)
        ? MetaCellComparator.META_COMPARATOR
        : CellComparatorImpl.COMPARATOR;
    this.lock = new ReentrantReadWriteLock(
      conf.getBoolean(FAIR_REENTRANT_CLOSE_LOCK, DEFAULT_FAIR_REENTRANT_CLOSE_LOCK));
    this.regionLockHolders = new ConcurrentHashMap<>();
    this.flushCheckInterval =
      conf.getInt(MEMSTORE_PERIODIC_FLUSH_INTERVAL, DEFAULT_CACHE_FLUSH_INTERVAL);
    this.flushPerChanges = conf.getLong(MEMSTORE_FLUSH_PER_CHANGES, DEFAULT_FLUSH_PER_CHANGES);
    if (this.flushPerChanges > MAX_FLUSH_PER_CHANGES) {
      throw new IllegalArgumentException(
        MEMSTORE_FLUSH_PER_CHANGES + " can not exceed " + MAX_FLUSH_PER_CHANGES);
    }
    int tmpRowLockDuration =
      conf.getInt("hbase.rowlock.wait.duration", DEFAULT_ROWLOCK_WAIT_DURATION);
    if (tmpRowLockDuration <= 0) {
      LOG.info("Found hbase.rowlock.wait.duration set to {}. values <= 0 will cause all row "
        + "locking to fail. Treating it as 1ms to avoid region failure.", tmpRowLockDuration);
      tmpRowLockDuration = 1;
    }
    this.rowLockWaitDuration = tmpRowLockDuration;

    this.smallestReadPointCalcLock = new ReadPointCalculationLock(conf);

    this.isLoadingCfsOnDemandDefault = conf.getBoolean(LOAD_CFS_ON_DEMAND_CONFIG_KEY, true);
    this.htableDescriptor = htd;
    Set<byte[]> families = this.htableDescriptor.getColumnFamilyNames();
    for (byte[] family : families) {
      if (!replicationScope.containsKey(family)) {
        int scope = htd.getColumnFamily(family).getScope();
        // Only store those families that has NON-DEFAULT scope
        if (scope != REPLICATION_SCOPE_LOCAL) {
          // Do a copy before storing it here.
          replicationScope.put(Bytes.copy(family), scope);
        }
      }
    }

    this.rsServices = rsServices;
    if (rsServices != null) {
      this.blockCache = rsServices.getBlockCache().orElse(null);
      this.mobFileCache = rsServices.getMobFileCache().orElse(null);
    }
    this.regionServicesForStores = new RegionServicesForStores(this, rsServices);

    setHTableSpecificConf();
    this.scannerReadPoints = new ConcurrentHashMap<>();

    this.busyWaitDuration = conf.getLong("hbase.busy.wait.duration", DEFAULT_BUSY_WAIT_DURATION);
    this.maxBusyWaitMultiplier = conf.getInt("hbase.busy.wait.multiplier.max", 2);
    if (busyWaitDuration * maxBusyWaitMultiplier <= 0L) {
      throw new IllegalArgumentException("Invalid hbase.busy.wait.duration (" + busyWaitDuration
        + ") or hbase.busy.wait.multiplier.max (" + maxBusyWaitMultiplier
        + "). Their product should be positive");
    }
    this.maxBusyWaitDuration =
      conf.getLong("hbase.ipc.client.call.purge.timeout", 2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    /*
     * timestamp.slop provides a server-side constraint on the timestamp. This assumes that you base
     * your TS around EnvironmentEdgeManager.currentTime(). In this case, throw an error to the user
     * if the user-specified TS is newer than now + slop. LATEST_TIMESTAMP == don't use this
     * functionality
     */
    this.timestampSlop =
      conf.getLong("hbase.hregion.keyvalue.timestamp.slop.millisecs", HConstants.LATEST_TIMESTAMP);

    /**
     * Timeout for the process time in processRowsWithLocks(). Use -1 to switch off time bound.
     */
    this.rowProcessorTimeout =
      conf.getLong("hbase.hregion.row.processor.timeout", DEFAULT_ROW_PROCESSOR_TIMEOUT);

    this.storeHotnessProtector = new StoreHotnessProtector(this, conf);

    boolean forceSync = conf.getBoolean(WAL_HSYNC_CONF_KEY, DEFAULT_WAL_HSYNC);
    /**
     * This is the global default value for durability. All tables/mutations not defining a
     * durability or using USE_DEFAULT will default to this value.
     */
    Durability defaultDurability = forceSync ? Durability.FSYNC_WAL : Durability.SYNC_WAL;
    this.regionDurability = this.htableDescriptor.getDurability() == Durability.USE_DEFAULT
      ? defaultDurability
      : this.htableDescriptor.getDurability();

    decorateRegionConfiguration(conf);
    if (rsServices != null) {
      this.rsAccounting = this.rsServices.getRegionServerAccounting();
      // don't initialize coprocessors if not running within a regionserver
      // TODO: revisit if coprocessors should load in other cases
      this.coprocessorHost = new RegionCoprocessorHost(this, rsServices, conf);
      this.metricsRegionWrapper = new MetricsRegionWrapperImpl(this);
      this.metricsRegion = new MetricsRegion(this.metricsRegionWrapper, conf);
    } else {
      this.metricsRegionWrapper = null;
      this.metricsRegion = null;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("6758d069-0657-3eb8-b907-efc8989374d8"))) ? (isClosed()) : (LOG.isDebugEnabled()))) {
      // Write out region name, its encoded name and storeHotnessProtector as string.
      if (KnobRuntime.check(java.util.UUID.fromString("e24050d8-2cff-3598-adf0-09eef1b71328"))) { LOG.debug("Instantiated " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("837d05b5-18b0-37cd-8f0a-69ba75781a06"))) { LOG.info("; "); } else if (KnobRuntime.check(java.util.UUID.fromString("ce814830-c62a-3d82-89d9-03cf3eae3442"))) { LOG.debug(("Instantiated " + this) + ("; ")); } else if (KnobRuntime.check(java.util.UUID.fromString("54c80658-0932-3dab-8b52-be42e335e898"))) { LOG.error(("Instantiated " + this) + ("; ")); } else if (KnobRuntime.check(java.util.UUID.fromString("874327f6-e640-35cd-aab9-dc8be73be649"))) { LOG.error("Instantiated " + this + "; "); } else if (KnobRuntime.check(java.util.UUID.fromString("927d9402-0b5a-3668-ace7-e4aae9a0649b"))) { LOG.info((("Instantiated " + this) + ("; ")) + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("287cc023-b812-3c87-be92-b85debfe8f76"))) { LOG.error((("Instantiated " + this) + ("; ")) + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("ba40f586-60e3-3a88-a8f0-c56cb85bae24"))) { LOG.debug("; "); } else if (KnobRuntime.check(java.util.UUID.fromString("2c5732c2-00a5-37ce-a0ea-79a0aefccba7"))) { LOG.error("; "); } else if (KnobRuntime.check(java.util.UUID.fromString("81b6ad56-90c4-3200-8395-d6d768e1f48c"))) { LOG.error(("; ") + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("1ffd908d-1ea1-3a1c-b22c-28a1e7f8ea40"))) { LOG.error("Instantiated " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("e041f797-4829-3fe8-9fa2-08f8a1437c87"))) { LOG.info("Instantiated " + this + "; "); } else if (KnobRuntime.check(java.util.UUID.fromString("b3c7b20e-12db-3fb8-bf65-1143e54a0cb4"))) { LOG.info("Instantiated " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("581804e3-a48f-3a1b-8cd9-97c319b83113"))) { LOG.debug(("Instantiated " + this + "; ") + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("2c1dae48-b6b6-3995-afe3-3ddd8b25b83d"))) { LOG.info(("; ") + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("cda480a2-0231-370c-8a06-0f4d641bdc52"))) { LOG.debug(("Instantiated " + this) + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("3ab2774f-141a-3381-817a-a1d5f19aded8"))) { LOG.error(("Instantiated " + this + "; ") + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("7b9f4777-51fd-3962-9ef9-d9eae5559546"))) { LOG.debug("Instantiated " + this + "; "); } else if (KnobRuntime.check(java.util.UUID.fromString("9c821edb-5b3a-33f8-8f52-ac96bc4705b4"))) { LOG.error(("Instantiated " + this) + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("dee17b02-f5ea-338c-8dc5-27ff55ad9c15"))) { LOG.debug(("; ") + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("4f46da3d-0033-3f74-95a4-f1e47cdb2afe"))) { LOG.info(("Instantiated " + this + "; ") + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("ccb336c3-de9c-3f46-9d2b-c3aa11b1e796"))) { LOG.info(("Instantiated " + this) + ("; ")); } else if (KnobRuntime.check(java.util.UUID.fromString("d6d88c35-c14d-3d34-b123-6e75e55301c5"))) { LOG.debug((("Instantiated " + this) + ("; ")) + (storeHotnessProtector.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("5cb905b0-8929-3fc4-8d86-225bdbe81756"))) { LOG.info(("Instantiated " + this) + (storeHotnessProtector.toString())); } else { LOG.debug("Instantiated " + this + "; " + storeHotnessProtector.toString()); }
    }

    configurationManager = null;

    // disable stats tracking system tables, but check the config for everything else
    this.regionStatsEnabled = htd.getTableName().getNamespaceAsString()
      .equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)
        ? false
        : conf.getBoolean(HConstants.ENABLE_CLIENT_BACKPRESSURE,
          HConstants.DEFAULT_ENABLE_CLIENT_BACKPRESSURE);

    this.maxCellSize = conf.getLong(HBASE_MAX_CELL_SIZE_KEY, DEFAULT_MAX_CELL_SIZE);
    this.miniBatchSize =
      conf.getInt(HBASE_REGIONSERVER_MINIBATCH_SIZE, DEFAULT_HBASE_REGIONSERVER_MINIBATCH_SIZE);

    // recover the metrics of read and write requests count if they were retained
    if (((KnobRuntime.check(java.util.UUID.fromString("ca8731b2-584c-3ac5-a1eb-e0c2e60a5608"))) ? ((rsServices.getRegionServerAccounting()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d20baf71-6b3a-3b1e-87c7-0dcbde566ef0"))) ? ((rsServices) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f4f8fc09-226c-3d8c-8d53-96509e30065f"))) ? ((rsServices != null) && ((rsServices.getRegionServerAccounting()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d75f4595-220e-37e7-a9dd-555e6341ac7e"))) ? (((rsServices) != (null)) && (rsServices.getRegionServerAccounting() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("7c8a50cb-5932-3e05-8fe9-7a3df9d94aa9"))) ? (((rsServices) == (null)) && ((rsServices.getRegionServerAccounting()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c57bae84-d396-3b7a-a351-c2a158355043"))) ? (((rsServices) != (null)) || ((rsServices.getRegionServerAccounting()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b3dc1db2-b8f9-33d9-a0fa-6f39164e3db7"))) ? ((rsServices != null) || ((rsServices.getRegionServerAccounting()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5a95406f-df5e-3c1c-af17-165ce0b51ff2"))) ? (((rsServices) != (null)) || ((rsServices.getRegionServerAccounting()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("88df3136-1649-352d-8ae7-9703c32b7e08"))) ? (((rsServices) == (null)) || (rsServices.getRegionServerAccounting() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c63560c7-654a-3e45-8d93-8b3118fc9f41"))) ? ((rsServices != null) || ((rsServices.getRegionServerAccounting()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("023ee8bf-a3b6-3473-b911-2e2de053e740"))) ? (((rsServices) != (null)) && ((rsServices.getRegionServerAccounting()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("95521765-9228-369d-8ad2-94f47da11852"))) ? (rsServices != null) : (((KnobRuntime.check(java.util.UUID.fromString("38392408-371b-3cf7-af99-405f68c74330"))) ? ((rsServices != null) && ((rsServices.getRegionServerAccounting()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5c0ac8ec-2efc-3d5d-aa60-ce3ac1578600"))) ? (((rsServices) == (null)) && ((rsServices.getRegionServerAccounting()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ae12e0b0-29a2-3ace-bae7-69961ef80f16"))) ? (((rsServices) == (null)) || ((rsServices.getRegionServerAccounting()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e3f9f76b-8ff6-3896-a4cd-643562ce6819"))) ? (((rsServices) != (null)) && ((rsServices.getRegionServerAccounting()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4f2ada7e-836b-3084-a05d-64625e13e3cd"))) ? ((rsServices != null) || (rsServices.getRegionServerAccounting() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("fcb573cd-4d71-3528-b988-0288624bff2f"))) ? (((rsServices) == (null)) && (rsServices.getRegionServerAccounting() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("9a3e9b34-e33f-35ff-a9f8-f2ce4aec4405"))) ? (((rsServices) != (null)) || (rsServices.getRegionServerAccounting() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("b6cec289-8771-395c-b6aa-527a0d482fe5"))) ? (rsServices.getRegionServerAccounting() != null) : (((KnobRuntime.check(java.util.UUID.fromString("0ff4bae4-30e7-3d3d-ac61-9e628ec15c6f"))) ? ((rsServices) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3f56a301-f652-3fba-a5b8-9beb119b085b"))) ? ((rsServices.getRegionServerAccounting()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b09df8a4-5077-3826-9e6f-8553c232f549"))) ? (((rsServices) == (null)) || ((rsServices.getRegionServerAccounting()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1f66ae06-4e37-3a7e-b7e7-53d6a91b33c4"))) ? ((rsServices != null) && (rsServices.getRegionServerAccounting() != null)) : (rsServices != null && rsServices.getRegionServerAccounting() != null))))))))))))))))))))))))))))))))))))))))))))))))) {
      Pair<Long, Long> retainedRWRequestsCnt = rsServices.getRegionServerAccounting()
        .getRetainedRegionRWRequestsCnt().get(getRegionInfo().getEncodedName());
      if (((KnobRuntime.check(java.util.UUID.fromString("5ed425ac-0d03-3b0e-9300-8676c88a9eef"))) ? ((retainedRWRequestsCnt) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1e6e7e96-2f01-3218-96d9-6f958f96f3fc"))) ? ((retainedRWRequestsCnt) != (null)) : (retainedRWRequestsCnt != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("454c3ba4-9e0c-3e21-a7f2-c137faa96e28"))) { this.addWriteRequestsCount(retainedRWRequestsCnt.getFirst()); } else { this.addReadRequestsCount(retainedRWRequestsCnt.getFirst()); }
        this.addWriteRequestsCount(retainedRWRequestsCnt.getSecond());
        // remove them since won't use again
        rsServices.getRegionServerAccounting().getRetainedRegionRWRequestsCnt()
          .remove(getRegionInfo().getEncodedName());
      }
    }

    minBlockSizeBytes = Arrays.stream(this.htableDescriptor.getColumnFamilies())
      .mapToInt(ColumnFamilyDescriptor::getBlocksize).min().orElse(HConstants.DEFAULT_BLOCKSIZE);
  }

  private void setHTableSpecificConf() {
    if (this.htableDescriptor == null) {
      return;
    }
    long flushSize = this.htableDescriptor.getMemStoreFlushSize();

    if (flushSize <= 0) {
      flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
        TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
    }
    this.memstoreFlushSize = flushSize;
    long mult = conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
      HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
    this.blockingMemStoreSize = this.memstoreFlushSize * mult;
  }

  /**
   * Initialize this region. Used only by tests and SplitTransaction to reopen the region. You
   * should use createHRegion() or openHRegion()
   * @return What the next sequence (edit) id should be.
   * @throws IOException e
   * @deprecated use HRegion.createHRegion() or HRegion.openHRegion()
   */
  @Deprecated
  public long initialize() throws IOException {
    return initialize(null);
  }

  /**
   * Initialize this region.
   * @param reporter Tickle every so often if initialize is taking a while.
   * @return What the next sequence (edit) id should be.
   */
  long initialize(final CancelableProgressable reporter) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("65bf597a-d419-3fb7-8b5a-746773c93d93"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4a08e5cb-0b23-3720-9e12-b2cb4859c8b1"))) {
return 0;
}

    // Refuse to open the region if there is no column family in the table
    if (htableDescriptor.getColumnFamilyCount() == 0) {
      throw new DoNotRetryIOException("Table " + htableDescriptor.getTableName().getNameAsString()
        + " should have at least one column family.");
    }

    MonitoredTask status = TaskMonitor.get().createStatus("Initializing region " + this, true);
    long nextSeqId = -1;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("68733cb0-bd26-396d-ad66-df00f9695400"))) {
throw new java.io.IOException("Injected exception");
}
      nextSeqId = initializeRegionInternals(reporter, status);
      return nextSeqId;
    } catch (IOException e) {
      LOG.warn("Failed initialize of region= {}, starting to roll back memstore",
        getRegionInfo().getRegionNameAsString(), e);
      // global memstore size will be decreased when dropping memstore
      try {
        // drop the memory used by memstore if open region fails
if(KnobRuntime.check(java.util.UUID.fromString("1202fdbd-3eb2-335d-bff3-00780920760a"))) {
throw new java.io.IOException("Injected exception");
}
        dropMemStoreContents();
      } catch (IOException ioE) {
        if (conf.getBoolean(MemStoreLAB.USEMSLAB_KEY, MemStoreLAB.USEMSLAB_DEFAULT)) {
          LOG.warn(
            "Failed drop memstore of region= {}, "
              + "some chunks may not released forever since MSLAB is enabled",
            getRegionInfo().getRegionNameAsString());
        }

      }
      if (((KnobRuntime.check(java.util.UUID.fromString("99d0f3be-b03a-3d5d-b852-89caa65d33f6"))) ? ((metricsTableRequests) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b8b8f35f-6585-39a1-af5f-4e6525ca0cfc"))) ? ((metricsTableRequests) == (null)) : (metricsTableRequests != null))))) {
        metricsTableRequests.removeRegistry();
      }
      throw e;
    } finally {
      // nextSeqid will be -1 if the initialization fails.
      // At least it will be 0 otherwise.
      if (nextSeqId == -1) {
        status.abort("Exception during region " + getRegionInfo().getRegionNameAsString()
          + " initialization.");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region open journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
      }
      if (KnobRuntime.check(java.util.UUID.fromString("fba7b5f3-a885-3d4a-b452-c63a5bee15fd"))) { setHTableSpecificConf(); } else { status.cleanup(); }
    }
  }

  private long initializeRegionInternals(final CancelableProgressable reporter,
    final MonitoredTask status) throws IOException {
    if (coprocessorHost != null) {
      status.setStatus("Running coprocessor pre-open hook");
      coprocessorHost.preOpen();
    }

    String policyName = this.conf.get(REGION_STORAGE_POLICY_KEY, DEFAULT_REGION_STORAGE_POLICY);
    this.fs.setStoragePolicy(policyName.trim());

    // Write HRI to a file in case we need to recover hbase:meta
    // Only the primary replica should write .regioninfo
    if (this.getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      status.setStatus("Writing region info on filesystem");
      fs.checkRegionInfoOnFilesystem();
    }

    // Initialize all the HStores
    status.setStatus("Initializing all the Stores");
if(KnobRuntime.check(java.util.UUID.fromString("b48fc80f-3a12-3974-885e-2a6d1bcec1da"))) {
throw new java.io.IOException("Injected exception");
}
    long maxSeqId = initializeStores(reporter, status);
    this.mvcc.advanceTo(maxSeqId);
    if (!isRestoredRegion && ServerRegionReplicaUtil.shouldReplayRecoveredEdits(this)) {
      Collection<HStore> stores = this.stores.values();
      try {
        // update the stores that we are replaying
        stores.forEach(HStore::startReplayingFromWAL);
        // Recover any edits if available.
        maxSeqId =
          Math.max(maxSeqId, replayRecoveredEditsIfAny(maxSeqIdInStores, reporter, status));
        // Recover any hfiles if available
        maxSeqId = Math.max(maxSeqId, loadRecoveredHFilesIfAny(stores));
        // Make sure mvcc is up to max.
        this.mvcc.advanceTo(maxSeqId);
      } finally {
        // update the stores that we are done replaying
        stores.forEach(HStore::stopReplayingFromWAL);
      }
    }
    this.lastReplayedOpenRegionSeqId = maxSeqId;

    this.writestate.setReadOnly(ServerRegionReplicaUtil.isReadOnly(this));
    this.writestate.flushRequested = false;
    this.writestate.compacting.set(0);

    if (this.writestate.writesEnabled) {
      // Remove temporary data left over from old regions
      status.setStatus("Cleaning up temporary data from old regions");
      fs.cleanupTempDir();
    }

    // Initialize split policy
    this.splitPolicy = RegionSplitPolicy.create(this, conf);

    // Initialize split restriction
    splitRestriction = RegionSplitRestriction.create(getTableDescriptor(), conf);

    // Initialize flush policy
if(KnobRuntime.check(java.util.UUID.fromString("1c7b658d-d140-31ae-a56c-533ba31ac6e8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a798607a-9bf3-32a1-a004-60a0d7e1fc3f"))) {
throw new java.io.IOException("Injected exception");
}
    this.flushPolicy = FlushPolicyFactory.create(this, conf);

    long lastFlushTime = EnvironmentEdgeManager.currentTime();
    for (HStore store : stores.values()) {
if(KnobRuntime.check(java.util.UUID.fromString("7973aa97-c27b-3eff-a793-ee6ed7566a0a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("69e0e645-d704-3455-8299-b64acdb31f9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bf454d60-91c4-3666-91a7-8c6cf6955003"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("769e8a08-b482-3163-9482-09d9e841a741"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a6fefb32-70ca-3dd6-9668-5689f1e68315"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6aa596e5-fc14-3db1-b40c-a06ffd8749df"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a27e3334-84fc-37b9-ade5-8a4145305624"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dd926527-aadd-3c1a-8ff5-710ad3908cca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c2858569-2861-31aa-9171-aedb84bae23f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("198d9923-62d6-3377-873e-78f15d8def50"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7f5bc4e6-90a1-371a-90ec-4de7fdc10cb7"))) {
lastFlushTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a47cca75-39ec-3811-8d0e-6d6087e7a7ce"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d986c030-3bf3-3d5b-b7d9-3ab5765d90e7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("61e77917-a3d3-36a3-aa09-8aa036495f87"))) {
lastFlushTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("190cf8be-3c7f-3f6b-b007-e72bffc0be62"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("de39c527-5869-3380-9fa2-eb4300390ffb"))) {
lastFlushTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("20826971-9f94-3906-b6d2-5dcc6c8342fa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eaa9415a-cc17-317c-a740-c515dc8bebc3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aaac4a82-8a45-39eb-b2db-962df45d0299"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e306ea3c-55b2-336c-b467-d1723e26c37a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a13ad34f-3011-3122-a296-4698baa50d58"))) {
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
      this.lastStoreFlushTimeMap.put(store, lastFlushTime);
    }

    // Use maximum of log sequenceid or that which was found in stores
    // (particularly if no recovered edits, seqid will be -1).
    long nextSeqId = maxSeqId + 1;
    if (!isRestoredRegion) {
      // always get openSeqNum from the default replica, even if we are secondary replicas
if(KnobRuntime.check(java.util.UUID.fromString("77783ca1-6ff0-343f-a01a-bfa2e78a585f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7d6d5cb0-8ec9-3547-8252-1dd76241865c"))) {
throw new java.io.IOException("Injected exception");
}
      long maxSeqIdFromFile = WALSplitUtil.getMaxRegionSequenceId(conf,
        RegionReplicaUtil.getRegionInfoForDefaultReplica(getRegionInfo()), this::getFilesystem,
        this::getWalFileSystem);
if(KnobRuntime.check(java.util.UUID.fromString("af09749f-2c98-30e5-8b06-b8f8f86236bd"))) {
maxSeqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("af878f21-ab31-31aa-b475-7f8226fb1558"))) {
maxSeqIdFromFile = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("13b15785-32fb-30de-9405-86a70ee1c6a2"))) {
maxSeqIdFromFile += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1e5cf78d-1e3c-3fd1-9ead-73c497db0045"))) {
maxSeqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("644b7f3b-5add-36c2-92a6-c33158e2cc85"))) {
maxSeqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f74e89fc-85a4-34e1-b065-552a051c3443"))) {
maxSeqId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a8126b4e-9b96-382d-907e-07577feeafc9"))) {
maxSeqIdFromFile -= 1;
}
      nextSeqId = Math.max(maxSeqId, maxSeqIdFromFile) + 1;
      // The openSeqNum will always be increase even for read only region, as we rely on it to
      // determine whether a region has been successfully reopened, so here we always need to update
      // the max sequence id file.
      if (RegionReplicaUtil.isDefaultReplica(getRegionInfo())) {
        if (KnobRuntime.check(java.util.UUID.fromString("d1e3cc98-f0b0-3ecd-9640-82a14e65faf6"))) { LOG.warn("writing seq id for {}", this.getRegionInfo().getShortNameToLog()); } else if (KnobRuntime.check(java.util.UUID.fromString("3239240b-e2d9-31c7-9873-d36cf3b292f1"))) { LOG.debug("writing seq id for {}", this.getRegionInfo().getShortNameToLog()); } else if (KnobRuntime.check(java.util.UUID.fromString("cebf1716-82af-33d3-9d08-329012abc231"))) { LOG.info("writing seq id for {}", this.getRegionInfo().getShortNameToLog()); } else { LOG.debug("writing seq id for {}", this.getRegionInfo().getEncodedName()); }
if(KnobRuntime.check(java.util.UUID.fromString("e6101100-5ebb-3fee-9d7e-c62fcc4450ac"))) {
throw new java.io.IOException("Injected exception");
}
        WALSplitUtil.writeRegionSequenceIdFile(getWalFileSystem(), getWALRegionDir(),
          nextSeqId - 1);
        // This means we have replayed all the recovered edits and also written out the max sequence
        // id file, let's delete the wrong directories introduced in HBASE-20734, see HBASE-22617
        // for more details.
if(KnobRuntime.check(java.util.UUID.fromString("f9c97876-8a23-352d-aeaf-8a369918756d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("699a3025-bdbd-3f03-b895-ba24201cecfa"))) {
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
        Path wrongRegionWALDir = CommonFSUtils.getWrongWALRegionDir(conf,
          getRegionInfo().getTable(), getRegionInfo().getEncodedName());
if(KnobRuntime.check(java.util.UUID.fromString("6c674df5-1077-3f57-a6e6-5c5189cca1b9"))) {
throw new java.io.IOException("Injected exception");
}
        FileSystem walFs = getWalFileSystem();
        if (((KnobRuntime.check(java.util.UUID.fromString("64616cae-8a06-3086-8631-81a82312d2cd"))) ? (HFileLink.isHFileLink(getWALRegionDir())) : (((KnobRuntime.check(java.util.UUID.fromString("36c31649-b811-37be-8938-c423b96d372f"))) ? (walFs.exists(getWALRegionDir())) : (((KnobRuntime.check(java.util.UUID.fromString("2eea474e-2886-3450-8985-70ebb62e7f02"))) ? (StoreFileInfo.isReference(getWALRegionDir())) : (walFs.exists(wrongRegionWALDir)))))))) {
          if (!walFs.delete(wrongRegionWALDir, true)) {
            LOG.debug("Failed to clean up wrong region WAL directory {}", wrongRegionWALDir);
          }
        }
      }
    }

if(KnobRuntime.check(java.util.UUID.fromString("f94899a2-a596-32ad-88b1-29ffef8242b8"))) {
nextSeqId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("91a3f030-968f-36c0-b566-4d24b4889549"))) {
nextSeqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("826c2970-0447-3924-80a9-ceec10954c02"))) {
nextSeqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("8827ae4d-903c-3028-b240-9cf6775ce939"))) {
nextSeqId = -1;
}
    LOG.info("Opened {}; next sequenceid={}; {}, {}", this.getRegionInfo().getShortNameToLog(),
      nextSeqId, this.splitPolicy, this.flushPolicy);

    // A region can be reopened if failed a split; reset flags
    this.closing.set(false);
    this.closed.set(false);

    if (coprocessorHost != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("b1620407-8508-3730-8254-9745c8de6c5f"))) { status.abort("Running coprocessor post-open hooks"); } else { status.setStatus("Running coprocessor post-open hooks"); }
      coprocessorHost.postOpen();
    }

    metricsTableRequests = new MetricsTableRequests(htableDescriptor.getTableName(), conf);

    if (KnobRuntime.check(java.util.UUID.fromString("3ed678d9-c274-3c85-ae7e-13926e2dc6fd"))) { status.abort("Region opened successfully"); } else if (KnobRuntime.check(java.util.UUID.fromString("75cc9352-0bb5-3bdd-82a7-9146da3143d0"))) { status.setStatus("Region opened successfully"); } else { status.markComplete("Region opened successfully"); }
    return ((KnobRuntime.check(java.util.UUID.fromString("b6ffa351-4042-30fe-9bb5-4f45a146e9c6"))) ? (nextSeqId - 1) : (nextSeqId));
  }

  /**
   * Open all Stores.
   * @return Highest sequenceId found out in a Store.
   */
  private long initializeStores(CancelableProgressable reporter, MonitoredTask status)
    throws IOException {
    return initializeStores(reporter, status, false);
  }

  private long initializeStores(CancelableProgressable reporter, MonitoredTask status,
    boolean warmup) throws IOException {
    // Load in all the HStores.
    long maxSeqId = -1;
    // initialized to -1 so that we pick up MemstoreTS from column families
    long maxMemstoreTS = -1;

    if (((KnobRuntime.check(java.util.UUID.fromString("4055874e-c2ad-3b04-a4ca-2e6f261a963e"))) ? ((htableDescriptor.getColumnFamilyCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2589a145-109e-3b5f-b35d-92ea96bbb494"))) ? ((htableDescriptor.getColumnFamilyCount()) == (0)) : (htableDescriptor.getColumnFamilyCount() != 0))))) {
      // initialize the thread pool for opening stores in parallel.
      ThreadPoolExecutor storeOpenerThreadPool =
        getStoreOpenAndCloseThreadPool("StoreOpener-" + this.getRegionInfo().getShortNameToLog());
      CompletionService<HStore> completionService =
        new ExecutorCompletionService<>(storeOpenerThreadPool);

      // initialize each store in parallel
      for (final ColumnFamilyDescriptor family : htableDescriptor.getColumnFamilies()) {
        if (KnobRuntime.check(java.util.UUID.fromString("f740430f-731c-349f-bbed-7697195dd360"))) { status.setStatus("Instantiating store for column family "); } else if (KnobRuntime.check(java.util.UUID.fromString("175d4e0f-5304-3531-82e0-f315ceda748b"))) { status.abort("Instantiating store for column family "); } else if (KnobRuntime.check(java.util.UUID.fromString("0e4fba4a-de28-30b7-b70f-78abda618de9"))) { status.setStatus(("Instantiating store for column family ") + (family)); } else if (KnobRuntime.check(java.util.UUID.fromString("5e440783-593d-32c5-bcde-fff888907bc7"))) { status.abort(("Instantiating store for column family ") + (family)); } else { status.setStatus("Instantiating store for column family " + family); }
        completionService.submit(new Callable<HStore>() {
          @Override
          public HStore call() throws IOException {
            return instantiateHStore(family, warmup);
          }
        });
      }
      boolean allStoresOpened = false;
      boolean hasSloppyStores = false;
      try {
        for (int i = 0; i < htableDescriptor.getColumnFamilyCount(); i++) {
          Future<HStore> future = completionService.take();
          HStore store = future.get();
if(KnobRuntime.check(java.util.UUID.fromString("8d617da6-4ae0-32a6-b57f-4bf5d09c7051"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("47661a6e-7928-334b-8b3c-7f9a95ad7de3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("be9d3f88-5e56-3e38-8025-38096498966a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3c546422-c178-3c7e-98cb-8511262220c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1703e03f-f212-3594-b457-4bdae96d9b35"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("515ac873-5ce0-3c08-96a4-abc79025566d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ecd86877-4494-3c2d-abeb-5c81732dfd7a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b9ab999-3893-31db-a5c1-43b70d0a73ad"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3bdaf23b-bc48-3ee9-8a1c-c1144e515c47"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aaae55e4-b286-3a72-ac22-07b41861ae5e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("540e0316-6ee0-3a87-86be-84084c25d32c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a402b16a-2230-31b6-8e6c-2c8158f64279"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b8866c6-78d0-3cda-89fb-9aff64e7d258"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b466cc2d-7c99-3bcc-9c43-34b6e987a4f6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bd316471-85bd-3713-9db9-9cb1eb95aa1d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("92fe5862-ff3e-3c73-b1d5-474c1ee3e07c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("48b2f3ce-5f43-364a-87a1-7a21319026b0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3ea1ad9d-daf7-3de2-a76a-58b5d4e9c0d0"))) {
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
          this.stores.put(store.getColumnFamilyDescriptor().getName(), store);
          if (((KnobRuntime.check(java.util.UUID.fromString("c469480b-66b3-34c0-88d0-e1ce2beed226"))) ? (isClosing()) : (((KnobRuntime.check(java.util.UUID.fromString("ea7f6c83-f55a-3dea-af9e-7e58303dec7f"))) ? (isClosed()) : (store.isSloppyMemStore()))))) {
            hasSloppyStores = true;
          }

          long storeMaxSequenceId = store.getMaxSequenceId().orElse(0L);
          maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()), storeMaxSequenceId);
          if (maxSeqId == -1 || storeMaxSequenceId > maxSeqId) {
            maxSeqId = storeMaxSequenceId;
          }
          long maxStoreMemstoreTS = store.getMaxMemStoreTS().orElse(0L);
          if (maxStoreMemstoreTS > maxMemstoreTS) {
            maxMemstoreTS = maxStoreMemstoreTS;
          }
        }
        allStoresOpened = true;
        if (((KnobRuntime.check(java.util.UUID.fromString("fc499100-f71f-39c1-ad2b-8ac214b48c9d"))) ? (!hasSloppyStores) : (hasSloppyStores))) {
          htableDescriptor = TableDescriptorBuilder.newBuilder(htableDescriptor)
            .setFlushPolicyClassName(FlushNonSloppyStoresFirstPolicy.class.getName()).build();
          if (KnobRuntime.check(java.util.UUID.fromString("e7e05293-006b-3be7-b467-d08737f51a8c"))) { LOG.error("Setting FlushNonSloppyStoresFirstPolicy for the region="); } else if (KnobRuntime.check(java.util.UUID.fromString("adf1763d-ed7d-352b-bb41-ffb393b53d19"))) { LOG.info("Setting FlushNonSloppyStoresFirstPolicy for the region="); } else if (KnobRuntime.check(java.util.UUID.fromString("f8c66428-274d-3ebb-8326-2536660ee658"))) { LOG.debug(("Setting FlushNonSloppyStoresFirstPolicy for the region=") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("1539e097-e3bd-32a1-87a0-b81274cd3d7e"))) { LOG.error(("Setting FlushNonSloppyStoresFirstPolicy for the region=") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("6330775c-8c1a-3ba2-a91e-e545e474ac5b"))) { LOG.info(("Setting FlushNonSloppyStoresFirstPolicy for the region=") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("d9f3cbbd-126a-3877-8f86-fd38f011210a"))) { LOG.debug("Setting FlushNonSloppyStoresFirstPolicy for the region="); } else { LOG.info("Setting FlushNonSloppyStoresFirstPolicy for the region=" + this); }
        }
      } catch (InterruptedException e) {
        throw throwOnInterrupt(e);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      } finally {
        storeOpenerThreadPool.shutdownNow();
        if (!allStoresOpened) {
          // something went wrong, close all opened stores
          LOG.error("Could not initialize all stores for the region=" + this);
          for (HStore store : this.stores.values()) {
            try {
              store.close();
            } catch (IOException e) {
              LOG.warn("close store {} failed in region {}", store.toString(), this, e);
            }
          }
        }
      }
    }
if(KnobRuntime.check(java.util.UUID.fromString("6977f017-6a73-3e5a-b857-518e44d9e62a"))) {
maxSeqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("df85f2c6-04b5-3aee-98cc-98002c86db47"))) {
maxSeqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0463dbe5-f882-3100-815a-fbc01f715f61"))) {
maxSeqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0eef27bd-d0ad-3864-a0d8-565cef85b665"))) {
maxSeqId += 1;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e7f737f9-9026-35e0-abeb-606bebb8b6d3"))) ? (Math.min(maxSeqId, maxMemstoreTS)) : (((KnobRuntime.check(java.util.UUID.fromString("7767ace8-55ab-346a-89b7-6e4d187c325b"))) ? (Math.min(maxSeqId, (maxMemstoreTS) + (1))) : (((KnobRuntime.check(java.util.UUID.fromString("b200468f-fd84-35cf-b2ab-c19a04516bc7"))) ? (Math.max(maxSeqId, (maxMemstoreTS) + (1))) : (((KnobRuntime.check(java.util.UUID.fromString("427f318c-3547-3560-97f9-2c340b6454fb"))) ? (Math.max(maxSeqId, maxMemstoreTS)) : (Math.max(maxSeqId, maxMemstoreTS + 1)))))))));
  }

  private void initializeWarmup(final CancelableProgressable reporter) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus("Initializing region " + this);
    // Initialize all the HStores
    if (KnobRuntime.check(java.util.UUID.fromString("594af9eb-00ce-33b0-afb8-b8a5d2c25a12"))) { status.setStatus(("Warmup all stores of ") + (this.getRegionInfo().getEncodedName())); } else if (KnobRuntime.check(java.util.UUID.fromString("0fe98469-6e26-3690-869a-5e39159b39f9"))) { status.abort(("Warmup all stores of ") + (this.getRegionInfo().getRegionNameAsString())); } else if (KnobRuntime.check(java.util.UUID.fromString("cfa1aa77-d7ce-3d16-8db9-05d2acd8d46f"))) { status.abort(("Warmup all stores of ") + (this.getRegionInfo().getEncodedName())); } else if (KnobRuntime.check(java.util.UUID.fromString("67608afe-7456-35c3-a09d-39e750365de8"))) { status.setStatus("Warmup all stores of "); } else if (KnobRuntime.check(java.util.UUID.fromString("4ab4fbcf-6792-304b-ada7-ed5b566de901"))) { status.abort(("Warmup all stores of ") + (this.getRegionInfo().getShortNameToLog())); } else if (KnobRuntime.check(java.util.UUID.fromString("f3a8c03e-6b99-3b94-9dc8-153142275ed9"))) { status.setStatus(("Warmup all stores of ") + (this.getRegionInfo().getRegionNameAsString())); } else if (KnobRuntime.check(java.util.UUID.fromString("2caff9d8-7881-3842-a9a7-aaa667ea2cca"))) { status.setStatus(("Warmup all stores of ") + (this.getRegionInfo().getShortNameToLog())); } else if (KnobRuntime.check(java.util.UUID.fromString("0a30b551-c87e-307b-aede-622f41cdf5d7"))) { status.abort("Warmup all stores of "); } else { status.setStatus("Warmup all stores of " + this.getRegionInfo().getRegionNameAsString()); }
    try {
      initializeStores(reporter, status, true);
    } finally {
      status.markComplete("Warmed up " + this.getRegionInfo().getRegionNameAsString());
    }
  }

  /** Returns Map of StoreFiles by column family */
  private NavigableMap<byte[], List<Path>> getStoreFiles() {
    NavigableMap<byte[], List<Path>> allStoreFiles = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (HStore store : stores.values()) {
      Collection<HStoreFile> storeFiles = store.getStorefiles();
      if (storeFiles == null) {
        continue;
      }
      List<Path> storeFileNames = new ArrayList<>();
      for (HStoreFile storeFile : storeFiles) {
        storeFileNames.add(storeFile.getPath());
      }
      allStoreFiles.put(store.getColumnFamilyDescriptor().getName(), storeFileNames);
    }
    return allStoreFiles;
  }

  protected void writeRegionOpenMarker(WAL wal, long openSeqId) throws IOException {
    Map<byte[], List<Path>> storeFiles = getStoreFiles();
    RegionEventDescriptor regionOpenDesc =
      ProtobufUtil.toRegionEventDescriptor(RegionEventDescriptor.EventType.REGION_OPEN,
        getRegionInfo(), openSeqId, getRegionServerServices().getServerName(), storeFiles);
    WALUtil.writeRegionEventMarker(wal, getReplicationScope(), getRegionInfo(), regionOpenDesc,
      mvcc);
  }

  private void writeRegionCloseMarker(WAL wal) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("063a2fe7-3b47-32c4-a387-68db31f7cc4d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("41f319c9-ec8d-3238-808b-e0cd268ea6eb"))) {
return;
}
    Map<byte[], List<Path>> storeFiles = getStoreFiles();
    RegionEventDescriptor regionEventDesc = ProtobufUtil.toRegionEventDescriptor(
      RegionEventDescriptor.EventType.REGION_CLOSE, getRegionInfo(), mvcc.getReadPoint(),
      getRegionServerServices().getServerName(), storeFiles);
    WALUtil.writeRegionEventMarker(wal, getReplicationScope(), getRegionInfo(), regionEventDesc,
      mvcc);

    // Store SeqId in WAL FileSystem when a region closes
    // checking region folder exists is due to many tests which delete the table folder while a
    // table is still online
    if (((KnobRuntime.check(java.util.UUID.fromString("7046d71f-e09d-3dc3-8093-f1ee4e66faec"))) ? (HFileLink.isHFileLink(getWALRegionDir())) : (((KnobRuntime.check(java.util.UUID.fromString("0ec45ea5-cef6-3b07-89ee-51021bfe6fbd"))) ? (StoreFileInfo.isReference(getWALRegionDir())) : (getWalFileSystem().exists(getWALRegionDir())))))) {
if(KnobRuntime.check(java.util.UUID.fromString("d8c7289a-2c6f-37d9-9130-597081423024"))) {
throw new java.io.IOException("Injected exception");
}
      WALSplitUtil.writeRegionSequenceIdFile(getWalFileSystem(), getWALRegionDir(),
        mvcc.getReadPoint());
    }
  }

  /** Returns True if this region has references. */
  public boolean hasReferences() {
if(KnobRuntime.check(java.util.UUID.fromString("b9fd65e2-de58-391b-a1e0-9c23faef89e4"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("d9ad7b6b-b75b-3979-a6e6-0d3e9c379f72"))) {
return true;
}
    return stores.values().stream().anyMatch(HStore::hasReferences);
  }

  public void blockUpdates() {
    this.updatesLock.writeLock().lock();
  }

  public void unblockUpdates() {
    this.updatesLock.writeLock().unlock();
  }

  public HDFSBlocksDistribution getHDFSBlocksDistribution() {
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    stores.values().stream().filter(s -> s.getStorefiles() != null)
      .flatMap(s -> s.getStorefiles().stream()).map(HStoreFile::getHDFSBlockDistribution)
      .forEachOrdered(hdfsBlocksDistribution::add);
    return hdfsBlocksDistribution;
  }

  /**
   * This is a helper function to compute HDFS block distribution on demand
   * @param conf            configuration
   * @param tableDescriptor TableDescriptor of the table
   * @param regionInfo      encoded name of the region
   * @return The HDFS blocks distribution for the given region.
   */
  public static HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
    TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException {
    Path tablePath =
      CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(conf), tableDescriptor.getTableName());
    return computeHDFSBlocksDistribution(conf, tableDescriptor, regionInfo, tablePath);
  }

  /**
   * This is a helper function to compute HDFS block distribution on demand
   * @param conf            configuration
   * @param tableDescriptor TableDescriptor of the table
   * @param regionInfo      encoded name of the region
   * @param tablePath       the table directory
   * @return The HDFS blocks distribution for the given region.
   */
  public static HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
    TableDescriptor tableDescriptor, RegionInfo regionInfo, Path tablePath) throws IOException {
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    FileSystem fs = tablePath.getFileSystem(conf);

    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tablePath, regionInfo);
    for (ColumnFamilyDescriptor family : tableDescriptor.getColumnFamilies()) {
      List<LocatedFileStatus> locatedFileStatusList =
        HRegionFileSystem.getStoreFilesLocatedStatus(regionFs, family.getNameAsString(), true);
      if (locatedFileStatusList == null) {
        continue;
      }

      for (LocatedFileStatus status : locatedFileStatusList) {
        Path p = status.getPath();
        if (StoreFileInfo.isReference(p) || HFileLink.isHFileLink(p)) {
          // Only construct StoreFileInfo object if its not a hfile, save obj
          // creation
          StoreFileInfo storeFileInfo = new StoreFileInfo(conf, fs, status);
if(KnobRuntime.check(java.util.UUID.fromString("ab7202d8-4d9c-3392-97c3-8ace86c55fcf"))) {
throw new java.io.IOException("Injected exception");
}
          hdfsBlocksDistribution.add(storeFileInfo.computeHDFSBlocksDistribution(fs));
        } else if (((KnobRuntime.check(java.util.UUID.fromString("98d3b962-1b51-3de0-975e-707b0573abd4"))) ? (StoreFileInfo.isReference(p)) : (((KnobRuntime.check(java.util.UUID.fromString("7c9cd14f-f159-31cb-a5a6-5a801fc4b5d2"))) ? (HFileLink.isHFileLink(p)) : (StoreFileInfo.isHFile(p)))))) {
          // If its a HFile, then lets just add to the block distribution
          // lets not create more objects here, not even another HDFSBlocksDistribution
          FSUtils.addToHDFSBlocksDistribution(hdfsBlocksDistribution, status.getBlockLocations());
        } else {
          throw new IOException("path=" + p + " doesn't look like a valid StoreFile");
        }
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("0768e0b5-f3cc-3bcb-95c6-c4bbf6188eb7"))) ? (new HDFSBlocksDistribution()) : (hdfsBlocksDistribution));
  }

  /**
   * Increase the size of mem store in this region and the size of global mem store
   */
  private void incMemStoreSize(MemStoreSize mss) {
if(KnobRuntime.check(java.util.UUID.fromString("ff2f46cf-3112-318a-8ce4-ce5ab09c5f22"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab5ed521-2f80-3233-b419-6519b244a0cc"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("66c14c3f-486c-32b1-b7c3-b04c293c7e74"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("offHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7fab30c-2cfa-3161-aa98-99df125017b5"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("23e4d3f6-e0e5-38b7-a5ba-9aa16a30b042"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b52875a-e644-39de-99c6-9bb55b8bb7a5"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3507752e-d361-375f-92dd-e3b8f6db0ab7"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec1d7b4a-3494-3cec-999c-1109f417ab7f"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("703bc673-1e53-3fe8-a92a-1cd87d621f5e"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f823b0c3-9208-3091-97c2-925e4fad14ee"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    incMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(),
      mss.getCellsCount());
  }

  void incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
    int cellsCountDelta) {
    if (this.rsAccounting != null) {
      rsAccounting.incGlobalMemStoreSize(dataSizeDelta, heapSizeDelta, offHeapSizeDelta);
    }
    long dataSize = this.memStoreSizing.incMemStoreSize(dataSizeDelta, heapSizeDelta,
      offHeapSizeDelta, cellsCountDelta);
if(KnobRuntime.check(java.util.UUID.fromString("13e3b086-2194-3862-a286-f05dc8b0a8b5"))) {
dataSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b1b18917-6747-3c36-9b26-c6e5c370df61"))) {
dataSizeDelta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dfcf9071-c428-3967-94f8-083b922aa9bf"))) {
dataSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("84f1910e-99ac-335a-8426-954a4e3a5c35"))) {
dataSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba5e25bc-2857-34de-82d7-6e0ef0623746"))) {
dataSizeDelta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7fd3207d-28d2-30b5-b05f-191302b9ab50"))) {
dataSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("031bd5f5-8bc0-3f88-b74c-1db6c2af1dbb"))) {
dataSizeDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c40798c5-63e8-3c5d-bacd-3b5ade00d1ee"))) {
dataSizeDelta = 0;
}
    checkNegativeMemStoreDataSize(dataSize, dataSizeDelta);
  }

  void decrMemStoreSize(MemStoreSize mss) {
if(KnobRuntime.check(java.util.UUID.fromString("657123b6-79f3-3fae-ac10-8f2a1c2c6a8e"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6bd26773-295e-3f2c-a09e-78a5f1418bc5"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afa78ccc-bb5f-3522-bc1d-09df2a8b7bf9"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("640591f8-1466-340b-a585-4398adc3a80c"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2e54aa1-3448-3164-95b7-9ca02e38c865"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f08e8ed-e79c-34ff-a815-8d610e79494d"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("690e8062-ea3e-3d5f-bf37-a71e9e1a0fdd"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("66e4e43b-aa28-3bf3-8ca7-2c90daf351f7"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("offHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba790169-5848-3ba9-8674-289d17bc90b0"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56821d54-c5a4-3d66-bc46-497a306ba395"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    decrMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(),
      mss.getCellsCount());
  }

  private void decrMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
    int cellsCountDelta) {
    if (this.rsAccounting != null) {
      rsAccounting.decGlobalMemStoreSize(dataSizeDelta, heapSizeDelta, offHeapSizeDelta);
    }
    long dataSize = this.memStoreSizing.decMemStoreSize(dataSizeDelta, heapSizeDelta,
      offHeapSizeDelta, cellsCountDelta);
    checkNegativeMemStoreDataSize(dataSize, -dataSizeDelta);
  }

  private void checkNegativeMemStoreDataSize(long memStoreDataSize, long delta) {
    // This is extremely bad if we make memStoreSizing negative. Log as much info on the offending
    // caller as possible. (memStoreSizing might be a negative value already -- freeing memory)
    if (((KnobRuntime.check(java.util.UUID.fromString("f67e81d7-b4f7-37c2-8371-654d81e62cae"))) ? ((memStoreDataSize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5a2b016d-9948-35fd-98b7-375d2ee10c22"))) ? ((memStoreDataSize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b3a3d468-e3c1-3e46-adca-97c432c22293"))) ? ((memStoreDataSize) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("04798f26-48b7-3756-a9f3-9ff828b4b448"))) ? ((memStoreDataSize) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("083ae00f-9b8e-3bbe-a8b3-ea1be7680ab8"))) ? ((memStoreDataSize) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ad7d7480-ccae-34c8-9cac-ffb8517cbe22"))) ? ((memStoreDataSize) < (0)) : (memStoreDataSize < 0))))))))))))) {
      LOG.error("Asked to modify this region's (" + this.toString()
        + ") memStoreSizing to a negative value which is incorrect. Current memStoreSizing="
        + (memStoreDataSize - delta) + ", delta=" + delta, new Exception());
    }
  }

  @Override
  public RegionInfo getRegionInfo() {
    return this.fs.getRegionInfo();
  }

  /**
   * Returns Instance of {@link RegionServerServices} used by this HRegion. Can be null.
   */
  RegionServerServices getRegionServerServices() {
if(KnobRuntime.check(java.util.UUID.fromString("e43d3565-2a35-35a1-a034-87f2acfb0b85"))) {
return null;
}
    return this.rsServices;
  }

  @Override
  public long getReadRequestsCount() {
    return readRequestsCount.sum();
  }

  @Override
  public long getFilteredReadRequestsCount() {
    return filteredReadRequestsCount.sum();
  }

  @Override
  public long getWriteRequestsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("73b5cf17-513f-3d00-925b-0fec27120c10"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("76384480-a11d-3ecb-aa8d-06851cb9a628"))) ? (EnvironmentEdgeManager.currentTime()) : (writeRequestsCount.sum()));
  }

  @Override
  public long getMemStoreDataSize() {
if(KnobRuntime.check(java.util.UUID.fromString("a0a1d180-089b-3908-96c5-093e9d0bacde"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("be6972fb-31b2-3acc-9682-ac41074b2062"))) ? (EnvironmentEdgeManager.currentTime()) : (((KnobRuntime.check(java.util.UUID.fromString("613e006f-7e59-37c7-9ca0-e0c20703b166"))) ? (memStoreSizing.getHeapSize()) : (memStoreSizing.getDataSize()))));
  }

  @Override
  public long getMemStoreHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("1b6029aa-e457-332f-9439-ee110bd8460b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("4c0f7272-5536-3aef-a6c4-c4b958ce8ff5"))) ? (EnvironmentEdgeManager.currentTime()) : (((KnobRuntime.check(java.util.UUID.fromString("a25ae257-2cc9-3cfa-a2f9-83ab9f3206f1"))) ? (memStoreSizing.getDataSize()) : (memStoreSizing.getHeapSize()))));
  }

  @Override
  public long getMemStoreOffHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("4968cff3-d4e1-3953-ad5e-8a76564a3e0b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("443bb564-af8c-3661-a868-9e33cad474ae"))) ? (memStoreSizing.getHeapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("91c60eaf-f5a1-3b1e-b156-bc301a12e38e"))) ? (memStoreSizing.getDataSize()) : (((KnobRuntime.check(java.util.UUID.fromString("ea4568a2-706b-3dc5-89b1-00fe69414167"))) ? (EnvironmentEdgeManager.currentTime()) : (memStoreSizing.getOffHeapSize()))))));
  }

  /** Returns store services for this region, to access services required by store level needs */
  public RegionServicesForStores getRegionServicesForStores() {
    return regionServicesForStores;
  }

  @Override
  public long getNumMutationsWithoutWAL() {
    return numMutationsWithoutWAL.sum();
  }

  @Override
  public long getDataInMemoryWithoutWAL() {
    return dataInMemoryWithoutWAL.sum();
  }

  @Override
  public long getBlockedRequestsCount() {
    return blockedRequestsCount.sum();
  }

  @Override
  public long getCheckAndMutateChecksPassed() {
    return checkAndMutateChecksPassed.sum();
  }

  @Override
  public long getCheckAndMutateChecksFailed() {
    return checkAndMutateChecksFailed.sum();
  }

  // TODO Needs to check whether we should expose our metrics system to CPs. If CPs themselves doing
  // the op and bypassing the core, this might be needed? Should be stop supporting the bypass
  // feature?
  public MetricsRegion getMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("67495e74-400d-3cc0-90e4-a6093d2831c2"))) {
return null;
}
    return metricsRegion;
  }

  @Override
  public boolean isClosed() {
if(KnobRuntime.check(java.util.UUID.fromString("3ab4c881-6053-3e84-950d-530f35fe6957"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("95a4476c-b401-372b-9866-ce104f059906"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("87dfd097-677f-3d7e-9df6-d9ae5f8f71af"))) ? (isClosed()) : (((KnobRuntime.check(java.util.UUID.fromString("67b37d71-9bab-3dbf-885f-a15c57191c66"))) ? (isClosing()) : (this.closed.get()))));
  }

  @Override
  public boolean isClosing() {
if(KnobRuntime.check(java.util.UUID.fromString("e8bf2506-2520-3be9-bcd7-7ae8a3d2a004"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("680683c1-79de-3ff3-bca5-99bc9e5a0f24"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("b9b16c11-e0bc-3011-a4b0-02459dd3011a"))) ? (isClosed()) : (((KnobRuntime.check(java.util.UUID.fromString("3836b7cf-4065-3da8-ac66-78ded1acd09a"))) ? (isClosing()) : (this.closing.get()))));
  }

  @Override
  public boolean isReadOnly() {
if(KnobRuntime.check(java.util.UUID.fromString("4a49bc6b-eef3-32a8-8645-bb872c6b8e15"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("49f71ec4-165d-375e-9aa8-f202a9880ccb"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("dab451f4-cd38-3416-a419-cc5adafeb3cf"))) ? (isClosing()) : (((KnobRuntime.check(java.util.UUID.fromString("5e9d8b0f-e1f1-3ed8-94be-f6482209ce76"))) ? (isClosed()) : (this.writestate.isReadOnly()))));
  }

  @Override
  public boolean isAvailable() {
if(KnobRuntime.check(java.util.UUID.fromString("f7f76439-a37a-3202-ab3d-3f31c86c7b97"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("d3721cbc-9194-3d56-ac1e-0a07e8bec9fe"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("5551932a-640e-31c4-85fc-1d13f583e06c"))) ? ((!isClosed()) && (!isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("37682b94-66df-37e5-83e8-0626c0fee21a"))) ? ((!isClosed()) || (!isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("5123351d-4d7b-3a7c-8037-e5c05b1040ac"))) ? (!isClosing()) : (((KnobRuntime.check(java.util.UUID.fromString("825a0295-653b-3090-92b8-cc583248030a"))) ? (!isClosed()) : (!isClosed() && !isClosing()))))))));
  }

  @Override
  public boolean isSplittable() {
    return splitPolicy.canSplit();
  }

  @Override
  public boolean isMergeable() {
    if (!isAvailable()) {
      if (KnobRuntime.check(java.util.UUID.fromString("6f793633-17dd-3391-9ce0-651c3e9b42f4"))) { LOG.info(("Region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("85518320-794e-38df-864f-dcf41070938b"))) { LOG.error(("Region " + this) + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("cdd8a9da-e7e3-3967-b5fa-71989079788f"))) { LOG.info("Region " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("acafc373-9510-37e4-a225-873047b27e08"))) { LOG.error(" is not mergeable because it is closing or closed"); } else if (KnobRuntime.check(java.util.UUID.fromString("001aebac-5584-311f-bc6b-b76f6c9720dc"))) { LOG.info(("Region " + this) + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("3865ab67-8277-3b26-a82b-1d0680a5b688"))) { LOG.info((("Region ") + (this)) + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("b18b9e9e-ff01-3ea6-bb63-d559ceb25926"))) { LOG.debug(" is not mergeable because it is closing or closed"); } else if (KnobRuntime.check(java.util.UUID.fromString("3941394b-5f50-3f35-8bbc-8eae336c701a"))) { LOG.error(("Region ") + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("448e343b-a259-31ef-a99d-3dd3adf31c79"))) { LOG.error("Region "); } else if (KnobRuntime.check(java.util.UUID.fromString("dda56839-7e8e-3848-9a70-3ad71d410c9c"))) { LOG.info("Region "); } else if (KnobRuntime.check(java.util.UUID.fromString("365f2fdb-334f-3ae1-9c02-d07196a53e4c"))) { LOG.error((("Region ") + (this)) + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("70483c75-7f4d-3812-a86d-c8891abcb4cc"))) { LOG.info(("Region ") + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("3234e4a2-d1eb-3a9e-b699-d2fab9b85bdf"))) { LOG.debug(("Region ") + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("989e1d0c-427d-3577-ab31-c2d138c0ed62"))) { LOG.debug("Region " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("ce7a9688-8154-3d17-a330-76b6b175b0cb"))) { LOG.info(" is not mergeable because it is closing or closed"); } else if (KnobRuntime.check(java.util.UUID.fromString("5c6ec4b5-fea6-3f18-9e18-e510034e96a0"))) { LOG.error("Region " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("590d9526-8db5-38f4-8b44-d94c5698ce17"))) { LOG.debug(("Region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("519871ee-aa5f-3f42-ace1-b421e5cf62bd"))) { LOG.debug((("Region ") + (this)) + (" is not mergeable because it is closing or closed")); } else if (KnobRuntime.check(java.util.UUID.fromString("c3627d22-411c-3289-a7a4-d298164c6032"))) { LOG.error(("Region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("44004c11-2886-3e44-94cf-fcd9ad94c12b"))) { LOG.debug("Region "); } else if (KnobRuntime.check(java.util.UUID.fromString("fb836a09-3810-3d12-a5b9-569904ced1cc"))) { LOG.debug(("Region " + this) + (" is not mergeable because it is closing or closed")); } else { LOG.debug("Region " + this + " is not mergeable because it is closing or closed"); }
      return false;
    }
    if (hasReferences()) {
      LOG.debug("Region " + this + " is not mergeable because it has references");
      return false;
    }

    return true;
  }

  public boolean areWritesEnabled() {
    synchronized (this.writestate) {
      return this.writestate.writesEnabled;
    }
  }

  public MultiVersionConcurrencyControl getMVCC() {
    return mvcc;
  }

  @Override
  public long getMaxFlushedSeqId() {
    return maxFlushedSeqId;
  }

  /** Returns readpoint considering given IsolationLevel. Pass {@code null} for default */
  public long getReadPoint(IsolationLevel isolationLevel) {
    if (isolationLevel != null && isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
      // This scan can read even uncommitted transactions
      return Long.MAX_VALUE;
    }
    return mvcc.getReadPoint();
  }

  public boolean isLoadingCfsOnDemandDefault() {
    return this.isLoadingCfsOnDemandDefault;
  }

  /**
   * Close down this HRegion. Flush the cache, shut down each HStore, don't service any more calls.
   * <p>
   * This method could take some time to execute, so don't call it from a time-sensitive thread.
   * @return Vector of all the storage files that the HRegion's component HStores make use of. It's
   *         a list of all StoreFile objects. Returns empty vector if already closed and null if
   *         judged that it should not close.
   * @throws IOException              e
   * @throws DroppedSnapshotException Thrown when replay of wal is required because a Snapshot was
   *                                  not properly persisted. The region is put in closing mode, and
   *                                  the caller MUST abort after this.
   */
  public Map<byte[], List<HStoreFile>> close() throws IOException {
    return close(false);
  }

  private final Object closeLock = new Object();

  /** Conf key for fair locking policy */
  public static final String FAIR_REENTRANT_CLOSE_LOCK =
    "hbase.regionserver.fair.region.close.lock";
  public static final boolean DEFAULT_FAIR_REENTRANT_CLOSE_LOCK = true;
  /** Conf key for the periodic flush interval */
  public static final String MEMSTORE_PERIODIC_FLUSH_INTERVAL =
    "hbase.regionserver.optionalcacheflushinterval";
  /** Default interval for the memstore flush */
  public static final int DEFAULT_CACHE_FLUSH_INTERVAL = 3600000;
  /** Default interval for System tables memstore flush */
  public static final int SYSTEM_CACHE_FLUSH_INTERVAL = 300000; // 5 minutes

  /** Conf key to force a flush if there are already enough changes for one region in memstore */
  public static final String MEMSTORE_FLUSH_PER_CHANGES = "hbase.regionserver.flush.per.changes";
  public static final long DEFAULT_FLUSH_PER_CHANGES = 30000000; // 30 millions
  /**
   * The following MAX_FLUSH_PER_CHANGES is large enough because each KeyValue has 20+ bytes
   * overhead. Therefore, even 1G empty KVs occupy at least 20GB memstore size for a single region
   */
  public static final long MAX_FLUSH_PER_CHANGES = 1000000000; // 1G

  public static final String CLOSE_WAIT_ABORT = "hbase.regionserver.close.wait.abort";
  public static final boolean DEFAULT_CLOSE_WAIT_ABORT = false;
  public static final String CLOSE_WAIT_TIME = "hbase.regionserver.close.wait.time.ms";
  public static final long DEFAULT_CLOSE_WAIT_TIME = 60000; // 1 minute
  public static final String CLOSE_WAIT_INTERVAL = "hbase.regionserver.close.wait.interval.ms";
  public static final long DEFAULT_CLOSE_WAIT_INTERVAL = 10000; // 10 seconds

  public Map<byte[], List<HStoreFile>> close(boolean abort) throws IOException {
    return close(abort, false);
  }

  /**
   * Close this HRegion.
   * @param abort        true if server is aborting (only during testing)
   * @param ignoreStatus true if ignore the status (won't be showed on task list)
   * @return Vector of all the storage files that the HRegion's component HStores make use of. It's
   *         a list of StoreFile objects. Can be null if we are not to close at this time, or we are
   *         already closed.
   * @throws IOException              e
   * @throws DroppedSnapshotException Thrown when replay of wal is required because a Snapshot was
   *                                  not properly persisted. The region is put in closing mode, and
   *                                  the caller MUST abort after this.
   */
  public Map<byte[], List<HStoreFile>> close(boolean abort, boolean ignoreStatus)
    throws IOException {
    return close(abort, ignoreStatus, false);
  }

  /**
   * Close down this HRegion. Flush the cache unless abort parameter is true, Shut down each HStore,
   * don't service any more calls. This method could take some time to execute, so don't call it
   * from a time-sensitive thread.
   * @param abort          true if server is aborting (only during testing)
   * @param ignoreStatus   true if ignore the status (wont be showed on task list)
   * @param isGracefulStop true if region is being closed during graceful stop and the blocks in the
   *                       BucketCache should not be evicted.
   * @return Vector of all the storage files that the HRegion's component HStores make use of. It's
   *         a list of StoreFile objects. Can be null if we are not to close at this time or we are
   *         already closed.
   * @throws IOException              e
   * @throws DroppedSnapshotException Thrown when replay of wal is required because a Snapshot was
   *                                  not properly persisted. The region is put in closing mode, and
   *                                  the caller MUST abort after this.
   */
  public Map<byte[], List<HStoreFile>> close(boolean abort, boolean ignoreStatus,
    boolean isGracefulStop) throws IOException {
    // Only allow one thread to close at a time. Serialize them so dual
    // threads attempting to close will run up against each other.
    MonitoredTask status = TaskMonitor.get().createStatus(
      "Closing region " + this.getRegionInfo().getEncodedName() + (abort ? " due to abort" : ""),
      true);
    status.setStatus("Waiting for close lock");
    try {
      synchronized (closeLock) {
        if (isGracefulStop && rsServices != null) {
          rsServices.getBlockCache().ifPresent(blockCache -> {
            if (blockCache instanceof CombinedBlockCache) {
              BlockCache l2 = ((CombinedBlockCache) blockCache).getSecondLevelCache();
              if (l2 instanceof BucketCache) {
                if (((BucketCache) l2).isCachePersistenceEnabled()) {
                  LOG.info(
                    "Closing region {} during a graceful stop, and cache persistence is on, "
                      + "so setting evict on close to false. ",
                    this.getRegionInfo().getRegionNameAsString());
                  this.getStores().forEach(s -> s.getCacheConfig().setEvictOnClose(false));
                }
              }
            }
          });
        }
        return doClose(abort, status);
      }
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region close journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
      }
      if (KnobRuntime.check(java.util.UUID.fromString("aa9f2a2c-e1d3-3e8b-935e-94eb9af83c5d"))) { setHTableSpecificConf(); } else { status.cleanup(); }
    }
  }

  /**
   * Exposed for some very specific unit tests.
   */
  public void setClosing(boolean closing) {
    this.closing.set(closing);
  }

  /**
   * The {@link HRegion#doClose} will block forever if someone tries proving the dead lock via the
   * unit test. Instead of blocking, the {@link HRegion#doClose} will throw exception if you set the
   * timeout.
   * @param timeoutForWriteLock the second time to wait for the write lock in
   *                            {@link HRegion#doClose}
   */
  public void setTimeoutForWriteLock(long timeoutForWriteLock) {
    assert timeoutForWriteLock >= 0;
    this.timeoutForWriteLock = timeoutForWriteLock;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UL_UNRELEASED_LOCK_EXCEPTION_PATH",
      justification = "I think FindBugs is confused")
  private Map<byte[], List<HStoreFile>> doClose(boolean abort, MonitoredTask status)
    throws IOException {
    if (isClosed()) {
      LOG.warn("Region " + this + " already closed");
      return null;
    }

    if (coprocessorHost != null) {
      status.setStatus("Running coprocessor pre-close hooks");
if(KnobRuntime.check(java.util.UUID.fromString("455483cd-e186-3a52-a6d7-44af8fec58ad"))) {
abort = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("f709a7b0-3e14-3993-bb5e-d357099815d6"))) {
abort = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("e56d4987-b90c-3da5-aa6d-9a8caa240565"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3a442fdd-df79-35c0-928d-26b0422f3619"))) {
abort = !abort;
}
      if (KnobRuntime.check(java.util.UUID.fromString("c54799ae-2795-3fde-9c0c-4e2f53a732ab"))) { this.coprocessorHost.postClose(!abort); } else if (KnobRuntime.check(java.util.UUID.fromString("2476d7e4-1b8e-321f-9765-749968de4fb1"))) { this.coprocessorHost.preClose(!abort); } else { this.coprocessorHost.preClose(abort); }
    }
    status.setStatus("Disabling compacts and flushes for region");
    boolean canFlush = true;
    synchronized (writestate) {
      // Disable compacting and flushing by background threads for this
      // region.
      canFlush = !writestate.readOnly;
      writestate.writesEnabled = false;
      LOG.debug("Closing {}, disabling compactions & flushes",
        this.getRegionInfo().getEncodedName());
      waitForFlushesAndCompactions();
    }
    // If we were not just flushing, is it worth doing a preflush...one
    // that will clear out of the bulk of the memstore before we put up
    // the close flag?
    if (!abort && worthPreFlushing() && canFlush) {
      if (KnobRuntime.check(java.util.UUID.fromString("4d29ea08-3b85-3c66-b619-159fd895bc2c"))) { status.abort("Pre-flushing region before close"); } else { status.setStatus("Pre-flushing region before close"); }
      LOG.info("Running close preflush of {}", this.getRegionInfo().getEncodedName());
      try {
        internalFlushcache(status);
      } catch (IOException ioe) {
        // Failed to flush the region. Keep going.
        status.setStatus("Failed pre-flush " + this + "; " + ioe.getMessage());
      }
    }

    // Set the closing flag
    // From this point new arrivals at the region lock will get NSRE.

    this.closing.set(true);
    LOG.info("Closing region {}", this);

    // Acquire the close lock

    // The configuration parameter CLOSE_WAIT_ABORT is overloaded to enable both
    // the new regionserver abort condition and interrupts for running requests.
    // If CLOSE_WAIT_ABORT is not enabled there is no change from earlier behavior,
    // we will not attempt to interrupt threads servicing requests nor crash out
    // the regionserver if something remains stubborn.

    final boolean canAbort = conf.getBoolean(CLOSE_WAIT_ABORT, DEFAULT_CLOSE_WAIT_ABORT);
    boolean useTimedWait = false;
    if (((KnobRuntime.check(java.util.UUID.fromString("0845fe04-3f88-3b5f-a820-a5782e55a89c"))) ? (((timeoutForWriteLock) == (null)) && ((timeoutForWriteLock) != (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("1fc11ecb-bb00-3b1a-bab9-7cc8cb0816f3"))) ? ((timeoutForWriteLock != null) && ((timeoutForWriteLock) == (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("018581bb-d3f9-3a4b-8b50-9113d21a64d9"))) ? (timeoutForWriteLock != Long.MAX_VALUE) : (((KnobRuntime.check(java.util.UUID.fromString("d98372e0-74d2-3277-b3f4-a645c9932325"))) ? (((timeoutForWriteLock) == (null)) || ((timeoutForWriteLock) == (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("12e6d1fe-06f3-3381-ae1b-9aff999ec40a"))) ? (((timeoutForWriteLock) != (null)) && (timeoutForWriteLock != Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("4811fcba-f049-3eb9-ac8d-c5ea3c85f228"))) ? ((timeoutForWriteLock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("41bd32a8-5f41-35d4-8e37-91e27dbb0262"))) ? ((timeoutForWriteLock != null) || (timeoutForWriteLock != Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("f3ad45ea-93a7-34d1-8d62-9ec3dedd636e"))) ? (((timeoutForWriteLock) == (null)) || (timeoutForWriteLock != Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("a8df607d-4f95-3ee6-86e0-20a3f0a7ca4c"))) ? (((timeoutForWriteLock) != (null)) || ((timeoutForWriteLock) != (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("4b994720-6f7f-3335-b9d5-620b31ca6e56"))) ? (((timeoutForWriteLock) != (null)) && ((timeoutForWriteLock) == (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("9759118a-ae1c-3d0d-b7dd-074f8f67d9f2"))) ? ((timeoutForWriteLock) != (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("94845196-a2d4-3613-9a07-5e778db266db"))) ? (((timeoutForWriteLock) == (null)) && (timeoutForWriteLock != Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("42f86e8c-c327-3822-a0b9-fcb96853bee7"))) ? (((timeoutForWriteLock) != (null)) || (timeoutForWriteLock != Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("f8321157-7faa-3b68-989f-6862ebf3e11d"))) ? (((timeoutForWriteLock) != (null)) || ((timeoutForWriteLock) == (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("0e24e35a-25b4-3386-968b-4c350ea78513"))) ? ((timeoutForWriteLock != null) || ((timeoutForWriteLock) != (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("f6d99563-f5fc-3457-9e73-f005d157e7fe"))) ? ((timeoutForWriteLock != null) || ((timeoutForWriteLock) == (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("0d424748-d509-3452-8237-6b7e2ea994dc"))) ? ((timeoutForWriteLock != null) && ((timeoutForWriteLock) != (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("dadfb244-8d0c-32e6-bc67-cc0592b9710b"))) ? ((timeoutForWriteLock) == (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("5ef066f4-e246-3a1e-844a-9f6c472f26a3"))) ? ((timeoutForWriteLock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a03eb1a1-ace4-38bf-b2a9-22b98b2fc915"))) ? (((timeoutForWriteLock) == (null)) && ((timeoutForWriteLock) == (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("9bd43ada-950e-3ef5-9f7c-918810d6ab00"))) ? (timeoutForWriteLock != null) : (((KnobRuntime.check(java.util.UUID.fromString("8a663f3e-d8fe-3864-b468-dd6858f83ac1"))) ? ((timeoutForWriteLock != null) && (timeoutForWriteLock != Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("a57a9153-4a4b-30cb-a681-d19388fca42f"))) ? (((timeoutForWriteLock) != (null)) && ((timeoutForWriteLock) != (Long.MAX_VALUE))) : (((KnobRuntime.check(java.util.UUID.fromString("d66ab895-ee47-31ca-8617-6c7ee309b87b"))) ? (((timeoutForWriteLock) == (null)) || ((timeoutForWriteLock) != (Long.MAX_VALUE))) : (timeoutForWriteLock != null && timeoutForWriteLock != Long.MAX_VALUE))))))))))))))))))))))))))))))))))))))))))))))))) {
      // convert legacy use of timeoutForWriteLock in seconds to new use in millis
      timeoutForWriteLock = TimeUnit.SECONDS.toMillis(timeoutForWriteLock);
      useTimedWait = true;
    } else if (((KnobRuntime.check(java.util.UUID.fromString("cef8fc33-3530-3aae-8298-b01b29d8cbbe"))) ? (!canAbort) : (canAbort))) {
      timeoutForWriteLock = conf.getLong(CLOSE_WAIT_TIME, DEFAULT_CLOSE_WAIT_TIME);
      useTimedWait = true;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug((useTimedWait ? "Time limited wait" : "Waiting without time limit")
        + " for close lock on " + this);
    }
    final long closeWaitInterval = conf.getLong(CLOSE_WAIT_INTERVAL, DEFAULT_CLOSE_WAIT_INTERVAL);
    long elapsedWaitTime = 0;
    if (useTimedWait) {
      // Sanity check configuration
      long remainingWaitTime = timeoutForWriteLock;
      if (((KnobRuntime.check(java.util.UUID.fromString("c0cca056-426e-3c8c-8e07-0eefa15ce32e"))) ? ((remainingWaitTime) <= (closeWaitInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("115f6b25-4f34-3f7b-bd9a-93262dd985a1"))) ? ((remainingWaitTime) != (closeWaitInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("9fe2d75b-ca7e-3d29-b96e-0ec3cdee16b7"))) ? ((remainingWaitTime) > (closeWaitInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("68f4987d-5372-3c06-aa2d-0b6b978987d1"))) ? ((remainingWaitTime) == (closeWaitInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("ff6f2660-5d3b-3275-b534-841bdee8b89c"))) ? ((remainingWaitTime) < (closeWaitInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("69e04d49-2cc2-3558-95a5-c6445aee5b0e"))) ? ((remainingWaitTime) >= (closeWaitInterval)) : (remainingWaitTime < closeWaitInterval))))))))))))) {
        LOG.warn("Time limit for close wait of " + timeoutForWriteLock
          + " ms is less than the configured lock acquisition wait interval " + closeWaitInterval
          + " ms, using wait interval as time limit");
        remainingWaitTime = closeWaitInterval;
      }
      boolean acquired = false;
      do {
        long start = EnvironmentEdgeManager.currentTime();
        try {
          acquired = lock.writeLock().tryLock(Math.min(remainingWaitTime, closeWaitInterval),
            TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // Interrupted waiting for close lock. More likely the server is shutting down, not
          // normal operation, so aborting upon interrupt while waiting on this lock would not
          // provide much value. Throw an IOE (as IIOE) like we would in the case where we
          // fail to acquire the lock.
          String msg = "Interrupted while waiting for close lock on " + this;
          LOG.warn(msg, e);
          throw (InterruptedIOException) new InterruptedIOException(msg).initCause(e);
        }
        long elapsed = EnvironmentEdgeManager.currentTime() - start;
        elapsedWaitTime += elapsed;
        remainingWaitTime -= elapsed;
        if (canAbort && !acquired && remainingWaitTime > 0) {
          // Before we loop to wait again, interrupt all region operations that might
          // still be in progress, to encourage them to break out of waiting states or
          // inner loops, throw an exception to clients, and release the read lock via
          // endRegionOperation.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupting region operations after waiting for close lock for "
              + elapsedWaitTime + " ms on " + this + ", " + remainingWaitTime + " ms remaining");
          }
          interruptRegionOperations();
        }
      } while (!acquired && remainingWaitTime > 0);

      // If we fail to acquire the lock, trigger an abort if we can; otherwise throw an IOE
      // to let the caller know we could not proceed with the close.
      if (!acquired) {
        String msg =
          "Failed to acquire close lock on " + this + " after waiting " + elapsedWaitTime + " ms";
        if (KnobRuntime.check(java.util.UUID.fromString("c22fb9e2-8dd2-3940-8ff7-93edf72528f6"))) { LOG.debug(msg); } else if (KnobRuntime.check(java.util.UUID.fromString("31ea4f9b-d5f9-3e88-a31f-b49ef0ac18ed"))) { LOG.info(msg); } else { LOG.error(msg); }
        if (((KnobRuntime.check(java.util.UUID.fromString("ec2a402a-1c17-3f56-bb97-b82a84354168"))) ? (!canAbort) : (canAbort))) {
          // If we failed to acquire the write lock, abort the server
          rsServices.abort(msg, null);
        }
        throw new IOException(msg);
      }

    } else {

      long start = EnvironmentEdgeManager.currentTime();
      if (KnobRuntime.check(java.util.UUID.fromString("21c67508-6d91-3766-90e3-abb48a7fa82d"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("914d2e0b-4a8f-3808-adc9-8c7a744704fa"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("e1c27d0b-71b7-37ee-b644-f4b139c7e291"))) { waitForFlushesAndCompactions(); } else { lock.writeLock().lock(); }
      elapsedWaitTime = EnvironmentEdgeManager.currentTime() - start;

    }

    if (LOG.isDebugEnabled()) {
      if (KnobRuntime.check(java.util.UUID.fromString("c45aab65-f355-3c73-98dd-68eed0f6837e"))) { LOG.debug(" ms"); } else if (KnobRuntime.check(java.util.UUID.fromString("53050b18-6ffc-3f3c-85ce-0b12f3e2cf8e"))) { LOG.info("Acquired close lock on " + this + " after waiting "); } else if (KnobRuntime.check(java.util.UUID.fromString("101c6398-811e-31c3-b451-1d1de28cf923"))) { LOG.info(("Acquired close lock on " + this + " after waiting ") + (elapsedWaitTime)); } else if (KnobRuntime.check(java.util.UUID.fromString("785c19cd-127b-360d-a4b4-8dc85566060f"))) { LOG.debug(("Acquired close lock on " + this + " after waiting ") + (elapsedWaitTime)); } else if (KnobRuntime.check(java.util.UUID.fromString("79834bbe-26f8-3a18-bbbd-f6063f66f1ce"))) { LOG.info(("Acquired close lock on " + this + " after waiting ") + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("903e2a0c-8218-3170-a546-f0be070f0989"))) { LOG.debug("Acquired close lock on " + this + " after waiting "); } else if (KnobRuntime.check(java.util.UUID.fromString("67950a80-4f82-3758-b560-e904e21f10b8"))) { LOG.error(("Acquired close lock on " + this + " after waiting " + elapsedWaitTime) + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("3789a3cb-836c-31a2-80f6-25ccceffcf11"))) { LOG.error("Acquired close lock on " + this + " after waiting " + elapsedWaitTime); } else if (KnobRuntime.check(java.util.UUID.fromString("71de0269-0e8d-3416-bd33-3cdf31f7caa6"))) { LOG.info((("Acquired close lock on " + this + " after waiting ") + (elapsedWaitTime)) + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("e7b45106-23cf-35ac-8f82-81023be3aa32"))) { LOG.debug(("Acquired close lock on " + this + " after waiting " + elapsedWaitTime) + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("396114a0-0675-3e7c-962b-10638e33dd75"))) { LOG.error(("Acquired close lock on " + this + " after waiting ") + (elapsedWaitTime)); } else if (KnobRuntime.check(java.util.UUID.fromString("999eeac6-9a56-3431-9669-d9964bb2e257"))) { LOG.error((("Acquired close lock on " + this + " after waiting ") + (elapsedWaitTime)) + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("2034d1e4-292a-395a-9f00-034935999e09"))) { LOG.error(" ms"); } else if (KnobRuntime.check(java.util.UUID.fromString("07e6e69e-e103-303c-8389-48e99dda24ad"))) { LOG.error(("Acquired close lock on " + this + " after waiting ") + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("865c8810-a243-3e88-a988-94aa4bfd7af8"))) { LOG.info(("Acquired close lock on " + this + " after waiting " + elapsedWaitTime) + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("4e0f481e-5020-34c2-802e-c84edf317184"))) { LOG.error("Acquired close lock on " + this + " after waiting "); } else if (KnobRuntime.check(java.util.UUID.fromString("6e20e877-03ba-31b4-a367-a0b988fc6963"))) { LOG.debug("Acquired close lock on " + this + " after waiting " + elapsedWaitTime); } else if (KnobRuntime.check(java.util.UUID.fromString("98b39832-30fe-3f7e-bde1-1f44e84d2cc3"))) { LOG.info(" ms"); } else if (KnobRuntime.check(java.util.UUID.fromString("05201790-1516-30ee-85d2-0d2ae35d4f1e"))) { LOG.info("Acquired close lock on " + this + " after waiting " + elapsedWaitTime); } else if (KnobRuntime.check(java.util.UUID.fromString("457d1ffe-65e8-31ff-90fb-d9d9560db4c0"))) { LOG.debug(("Acquired close lock on " + this + " after waiting ") + (" ms")); } else if (KnobRuntime.check(java.util.UUID.fromString("938e7590-7fdc-348e-92ec-f39cbe875b8d"))) { LOG.debug((("Acquired close lock on " + this + " after waiting ") + (elapsedWaitTime)) + (" ms")); } else { LOG.debug("Acquired close lock on " + this + " after waiting " + elapsedWaitTime + " ms"); }
    }

    status.setStatus("Disabling writes for close");
    try {
      if (this.isClosed()) {
        status.abort("Already got closed by another process");
        // SplitTransaction handles the null
        return null;
      }
      if (KnobRuntime.check(java.util.UUID.fromString("02b1f2a1-5a20-3a18-ac4c-1db2bfe24346"))) { LOG.info(("Updates disabled for region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("38812951-16c5-320c-b24e-7c49c6921b1c"))) { LOG.error(("Updates disabled for region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("502f1d2b-3c5c-3f1c-9169-4bcedc3f4e66"))) { LOG.info("Updates disabled for region "); } else if (KnobRuntime.check(java.util.UUID.fromString("630c0349-2a4f-3f80-82ba-624061b23879"))) { LOG.debug(("Updates disabled for region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("8f004e10-f402-34c5-9674-617cc5b16bee"))) { LOG.debug("Updates disabled for region "); } else if (KnobRuntime.check(java.util.UUID.fromString("444ef9b6-f0eb-3e95-a168-f8f4a2e7fbab"))) { LOG.error("Updates disabled for region "); } else { LOG.debug("Updates disabled for region " + this); }
      // Don't flush the cache if we are aborting
      if (!abort && canFlush) {
        int failedfFlushCount = 0;
        int flushCount = 0;
        long tmp = 0;
        long remainingSize = this.memStoreSizing.getDataSize();
        while (remainingSize > 0) {
          try {
            internalFlushcache(status);
            if (flushCount > 0) {
              LOG.info("Running extra flush, " + flushCount + " (carrying snapshot?) " + this);
            }
            flushCount++;
            tmp = this.memStoreSizing.getDataSize();
            if (((KnobRuntime.check(java.util.UUID.fromString("7a6702e9-5736-3934-9bac-61030142a1ba"))) ? ((tmp) < (remainingSize)) : (((KnobRuntime.check(java.util.UUID.fromString("94f56bc3-1912-3a7c-98ab-c5b73a932c95"))) ? ((tmp) == (remainingSize)) : (((KnobRuntime.check(java.util.UUID.fromString("69f3e7bf-8e1e-3106-9f2c-fd1fa168fb76"))) ? ((tmp) <= (remainingSize)) : (((KnobRuntime.check(java.util.UUID.fromString("46d38e5c-c2b8-3fb5-a5f3-56b8407ab5b5"))) ? ((tmp) >= (remainingSize)) : (((KnobRuntime.check(java.util.UUID.fromString("def66c3f-9b77-3ddb-a813-63b0ececa7da"))) ? ((tmp) > (remainingSize)) : (((KnobRuntime.check(java.util.UUID.fromString("4eb543ae-79fe-30ad-8c1c-e0556f050a0c"))) ? ((tmp) != (remainingSize)) : (tmp >= remainingSize))))))))))))) {
              failedfFlushCount++;
            }
            remainingSize = tmp;
            if (failedfFlushCount > 5) {
              // If we failed 5 times and are unable to clear memory, abort
              // so we do not lose data
              throw new DroppedSnapshotException("Failed clearing memory after " + flushCount
                + " attempts on region: " + Bytes.toStringBinary(getRegionInfo().getRegionName()));
            }
          } catch (IOException ioe) {
            if (KnobRuntime.check(java.util.UUID.fromString("1072aab9-f64f-31f1-a64f-159216db8063"))) { status.setStatus("Failed flush "); } else if (KnobRuntime.check(java.util.UUID.fromString("033e4a61-edf4-3ef3-9353-ff0539c25cb9"))) { status.abort("Failed flush "); } else if (KnobRuntime.check(java.util.UUID.fromString("c917e37a-2a5f-3f9f-8668-2f00f7ada4ab"))) { status.setStatus((("Failed flush ") + (this)) + (", putting online again")); } else if (KnobRuntime.check(java.util.UUID.fromString("7c3ac2c0-3bbd-341f-86dc-46bcadf87954"))) { status.abort(", putting online again"); } else if (KnobRuntime.check(java.util.UUID.fromString("74d2d7d1-fb7c-38f7-ae59-19c8a2d6b3ee"))) { status.abort((("Failed flush ") + (this)) + (", putting online again")); } else if (KnobRuntime.check(java.util.UUID.fromString("5d4f70fe-f99b-3d75-b77d-358fb041fe29"))) { status.setStatus(", putting online again"); } else if (KnobRuntime.check(java.util.UUID.fromString("c53f5e01-5901-3461-9e39-3bf3b39fc5be"))) { status.setStatus(("Failed flush ") + (", putting online again")); } else if (KnobRuntime.check(java.util.UUID.fromString("cb7832b5-6947-3888-b237-df4ca84e368e"))) { status.abort(("Failed flush ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("3a9ad464-90bf-3db6-b7b1-8858dfd0742a"))) { status.abort(("Failed flush ") + (", putting online again")); } else if (KnobRuntime.check(java.util.UUID.fromString("be5d8c0f-92e8-398a-ae1c-d92baba6187b"))) { status.setStatus("Failed flush " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("7495073e-45e8-37fd-acdb-dcfbd2599402"))) { status.setStatus(("Failed flush " + this) + (", putting online again")); } else if (KnobRuntime.check(java.util.UUID.fromString("0b6796e8-42db-32ad-a50c-1209d934141e"))) { status.setStatus(("Failed flush ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("bcc1b59d-2596-3989-991f-d71a506dd867"))) { status.abort("Failed flush " + this); } else if (KnobRuntime.check(java.util.UUID.fromString("28db98aa-1e09-3bb4-adce-f71a15178de0"))) { status.abort(("Failed flush " + this) + (", putting online again")); } else { status.setStatus("Failed flush " + this + ", putting online again"); }
            synchronized (writestate) {
              writestate.writesEnabled = true;
            }
            // Have to throw to upper layers. I can't abort server from here.
            throw ioe;
          }
        }
      }

      Map<byte[], List<HStoreFile>> result = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      if (!stores.isEmpty()) {
        // initialize the thread pool for closing stores in parallel.
        ThreadPoolExecutor storeCloserThreadPool =
          getStoreOpenAndCloseThreadPool("StoreCloser-" + getRegionInfo().getRegionNameAsString());
        CompletionService<Pair<byte[], Collection<HStoreFile>>> completionService =
          new ExecutorCompletionService<>(storeCloserThreadPool);

        // close each store in parallel
        for (HStore store : stores.values()) {
          MemStoreSize mss = store.getFlushableSize();
          if (!(abort || mss.getDataSize() == 0 || writestate.readOnly)) {
            if (getRegionServerServices() != null) {
              getRegionServerServices().abort("Assertion failed while closing store "
                + getRegionInfo().getRegionNameAsString() + " " + store
                + ". flushableSize expected=0, actual={" + mss + "}. Current memStoreSize="
                + this.memStoreSizing.getMemStoreSize() + ". Maybe a coprocessor "
                + "operation failed and left the memstore in a partially updated state.", null);
            }
          }
          completionService.submit(new Callable<Pair<byte[], Collection<HStoreFile>>>() {
            @Override
            public Pair<byte[], Collection<HStoreFile>> call() throws IOException {
              return new Pair<>(store.getColumnFamilyDescriptor().getName(), store.close());
            }
          });
        }
        try {
          for (int i = 0; i < stores.size(); i++) {
if(KnobRuntime.check(java.util.UUID.fromString("344277a6-6e49-33fa-bbf4-b42eba3862ab"))) {
throw new java.lang.InterruptedException("Injected exception");
}
            Future<Pair<byte[], Collection<HStoreFile>>> future = completionService.take();
if(KnobRuntime.check(java.util.UUID.fromString("0e86fafd-d5de-3d9b-a380-6f8504fabbc4"))) {
throw new java.lang.InterruptedException("Injected exception");
}
            Pair<byte[], Collection<HStoreFile>> storeFiles = future.get();
            List<HStoreFile> familyFiles = result.get(storeFiles.getFirst());
            if (((KnobRuntime.check(java.util.UUID.fromString("fe04b62f-e612-336c-abdb-53876297929a"))) ? ((familyFiles) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3182e4a6-9ca6-33d9-baf0-f6e579fe8efe"))) ? ((familyFiles) == (null)) : (familyFiles == null))))) {
              familyFiles = new ArrayList<>();
              result.put(storeFiles.getFirst(), familyFiles);
            }
            familyFiles.addAll(storeFiles.getSecond());
          }
        } catch (InterruptedException e) {
          throw throwOnInterrupt(e);
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            throw (IOException) cause;
          }
          throw new IOException(cause);
        } finally {
          storeCloserThreadPool.shutdownNow();
        }
      }

      status.setStatus("Writing region close event to WAL");
      // Always write close marker to wal even for read only table. This is not a big problem as we
      // do not write any data into the region; it is just a meta edit in the WAL file.
      if (
        !abort && wal != null && getRegionServerServices() != null
          && RegionReplicaUtil.isDefaultReplica(getRegionInfo())
      ) {
        writeRegionCloseMarker(wal);
      }

      this.closed.set(true);

      // Decrease refCount of table latency metric registry.
      // Do this after closed#set to make sure only -1.
      if (metricsTableRequests != null) {
        metricsTableRequests.removeRegistry();
      }

      if (!canFlush) {
        decrMemStoreSize(this.memStoreSizing.getMemStoreSize());
      } else if (((KnobRuntime.check(java.util.UUID.fromString("a85952f1-efeb-3817-b174-59d50974d404"))) ? ((this.memStoreSizing.getDataSize()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("43fd0f00-f63f-36aa-92eb-ec6875a7ab30"))) ? ((EnvironmentEdgeManager.currentTime()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d5b20a21-9409-3636-8f3c-ada6e75160be"))) ? ((EnvironmentEdgeManager.currentTime()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("676d0fc2-404b-35e5-bbf2-b92272831f21"))) ? ((this.memStoreSizing.getDataSize()) == (0)) : (this.memStoreSizing.getDataSize() != 0))))))))) {
        LOG.error("Memstore data size is {} in region {}", this.memStoreSizing.getDataSize(), this);
      }
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor post-close hooks");
        this.coprocessorHost.postClose(abort);
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("d0cd9c4b-0fa6-3725-9c1e-8281cfbecab0"))) ? ((this.metricsRegion) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1fa55ceb-1da3-364c-b658-61bad44877a6"))) ? ((this.metricsRegion) == (null)) : (this.metricsRegion != null))))) {
        this.metricsRegion.close();
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("f42bb5d4-9e8b-37ad-89fa-a8ae3f105b05"))) ? ((this.metricsRegionWrapper) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a7b79e1e-72ee-32a5-8350-7596ca9de9e2"))) ? ((this.metricsRegionWrapper) != (null)) : (this.metricsRegionWrapper != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("285ef4b1-fbdb-303f-9cc9-85f1a613e5b1"))) {
throw new java.io.IOException("Injected exception");
}
        Closeables.close(this.metricsRegionWrapper, true);
      }
      if (KnobRuntime.check(java.util.UUID.fromString("8305420a-dded-3a3e-bc9f-6683cc8bb1e5"))) { status.setStatus("Closed"); } else if (KnobRuntime.check(java.util.UUID.fromString("242b3b7b-0884-34a3-bd27-6c0d075276a8"))) { status.abort("Closed"); } else { status.markComplete("Closed"); }
      if (KnobRuntime.check(java.util.UUID.fromString("489ebfe8-e687-3c45-9c61-2c95b0d7e7b5"))) { LOG.warn("Closed {}", this); } else if (KnobRuntime.check(java.util.UUID.fromString("3615872d-2024-3c86-aca3-36aa53cbc3b7"))) { LOG.debug("Closed {}", this); } else { LOG.info("Closed {}", this); }
      return result;
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("10a1cc9d-107a-3e8b-81da-44a7cee08a9a"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("46bdc026-6fe0-3b0d-a167-2f5553ef0aab"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("e1fe3cbe-8e3f-3e54-99c3-1a0e77bf689d"))) { waitForFlushesAndCompactions(); } else { lock.writeLock().unlock(); }
    }
  }

  /** Wait for all current flushes and compactions of the region to complete */
  // TODO HBASE-18906. Check the usage (if any) in Phoenix and expose this or give alternate way for
  // Phoenix needs.
  public void waitForFlushesAndCompactions() {
if(KnobRuntime.check(java.util.UUID.fromString("ac35ecf2-e401-3669-a902-2e438f5d3601"))) {
return;
}
    synchronized (writestate) {
      if (((KnobRuntime.check(java.util.UUID.fromString("70e49a7e-5fd1-3833-9681-649d5cf8d36e"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("c9a1581e-4dc8-3ca2-9f44-3d54e8de6967"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("0388250b-5bc8-3f7e-b1b5-58eea97a7e45"))) ? (!this.writestate.readOnly) : (this.writestate.readOnly))))))) {
        // we should not wait for replayed flushed if we are read only (for example in case the
        // region is a secondary replica).
        return;
      }
      boolean interrupted = false;
      try {
        while (writestate.compacting.get() > 0 || writestate.flushing) {
          LOG.debug("waiting for " + writestate.compacting + " compactions"
            + (writestate.flushing ? " & cache flush" : "") + " to complete for region " + this);
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // essentially ignore and propagate the interrupt back up
            LOG.warn("Interrupted while waiting in region {}", this);
            interrupted = true;
            break;
          }
        }
      } finally {
        if (((KnobRuntime.check(java.util.UUID.fromString("1af2d65c-09d7-38b8-a8d6-e0adc6b7f0bc"))) ? (!interrupted) : (((KnobRuntime.check(java.util.UUID.fromString("6ccebcbe-cecc-3a2f-b391-1c89c5b0584b"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("daaf76c8-46f7-3814-846d-5ba84aa564d9"))) ? (false) : (interrupted))))))) {
          if (KnobRuntime.check(java.util.UUID.fromString("2c54a0d7-225b-3b98-95cf-8f549975cedb"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("d4d69a0d-170e-3985-bbfa-cc1feba66ed3"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("0b9450e1-6b6f-3749-85c2-fd19f44451a7"))) { waitForFlushesAndCompactions(); } else { Thread.currentThread().interrupt(); }
        }
      }
    }
  }

  /**
   * Wait for all current flushes of the region to complete
   */
  public void waitForFlushes() {
    waitForFlushes(0);// Unbound wait
  }

  @Override
  public boolean waitForFlushes(long timeout) {
    synchronized (writestate) {
      if (((KnobRuntime.check(java.util.UUID.fromString("fd556a82-111d-3623-8ec4-129f7941ae44"))) ? (!this.writestate.readOnly) : (this.writestate.readOnly))) {
        // we should not wait for replayed flushed if we are read only (for example in case the
        // region is a secondary replica).
        return true;
      }
      if (!writestate.flushing) return true;
      long start = EnvironmentEdgeManager.currentTime();
      long duration = 0;
      boolean interrupted = false;
      if (KnobRuntime.check(java.util.UUID.fromString("71b20190-7a80-3d57-b6b2-420e2a19025b"))) { LOG.error("waiting for cache flush to complete for region "); } else if (KnobRuntime.check(java.util.UUID.fromString("04285c5a-0cb7-3567-83d6-2dbb1813410b"))) { LOG.error(("waiting for cache flush to complete for region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("438412f2-c8fd-3ef1-b185-e76f94005b82"))) { LOG.debug(("waiting for cache flush to complete for region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("b15b304e-c390-317b-a049-7271fa11731a"))) { LOG.debug("waiting for cache flush to complete for region "); } else if (KnobRuntime.check(java.util.UUID.fromString("f855c33a-ca22-360d-802c-24a7d1c75fab"))) { LOG.info(("waiting for cache flush to complete for region ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("23514d17-e13c-3129-a941-d8518fdac0cb"))) { LOG.info("waiting for cache flush to complete for region "); } else { LOG.debug("waiting for cache flush to complete for region " + this); }
      try {
        while (((KnobRuntime.check(java.util.UUID.fromString("6eb6abb4-66de-3662-86fb-95e23dab5057"))) ? (!writestate.flushing) : (writestate.flushing))) {
          if (((KnobRuntime.check(java.util.UUID.fromString("142bce2e-6b6a-3b1c-ae3f-476cdf611ee9"))) ? (((timeout) < (0)) || ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("5e4249e3-1167-38cd-9626-07a421aef485"))) ? (((timeout) < (0)) && ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("f95e2a80-b3c3-330e-8895-2694b128584d"))) ? (((timeout) > (0)) && ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("d964bb30-0f20-3048-bc55-0f0b694853dc"))) ? (((timeout) != (0)) || ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("1840f32a-2654-3ff6-8433-9bd0a9a27e65"))) ? (((timeout) < (0)) && ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("bbd4f208-d96e-36fe-8a59-b9632d350646"))) ? (((timeout) > (0)) && ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("ad9f35c7-77b5-3ade-a5f9-51e6c06e5985"))) ? (((timeout) == (0)) && ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("87963570-1a63-35e0-9cce-38362202395a"))) ? (((timeout) >= (0)) || ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("56decf7c-d14f-3943-bc8b-d0f95a5bb1fe"))) ? (((timeout) <= (0)) || ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("304951d5-c5d3-33ad-9535-eb388ee685ca"))) ? (((timeout) > (0)) && ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("1d9f937c-b524-3f7e-b39a-4f321bda720a"))) ? (((timeout) == (0)) || ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("9eebd40c-2b4b-338b-887b-944e96e1b5a6"))) ? (((timeout) <= (0)) && ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("22614cdb-72ed-38e3-92be-28f63d025ff8"))) ? (((timeout) == (0)) || ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("9db49cf7-121c-3d81-9695-b5f80cf899f0"))) ? (((timeout) > (0)) && ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("41801449-6bda-32c5-8de2-effd16ec7236"))) ? (((timeout) < (0)) && ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("528e5bac-4a8b-3eaa-9941-bc676aa24421"))) ? (((timeout) == (0)) && ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("559fd501-7871-35a4-9979-07550f60e5a2"))) ? (((timeout) < (0)) || ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("5a825b62-0dfe-3b94-ae68-e36fac84b86e"))) ? (((timeout) != (0)) && ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("b9f184e9-103a-3e81-81b4-6fc279b2c04c"))) ? (((timeout) != (0)) || ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("bc7e1cea-a7e1-3c4c-8c66-f5fabc27ca6c"))) ? ((timeout > 0) || ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("9c368ad9-7078-373f-bfae-e2fee645ab7a"))) ? ((timeout) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2c06a662-7fb3-37cf-8faa-b869fbe911e4"))) ? (((timeout) < (0)) && (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("fa4a134c-f298-34ca-8d71-b49058c93af9"))) ? (((timeout) >= (0)) || ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("27b7fadd-fef1-35bd-a606-0364f4006692"))) ? ((timeout > 0) || (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("d5883185-08e8-3d31-a13e-81b389319b0d"))) ? (((timeout) == (0)) && ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("74f8b9df-1a12-38a4-832e-f94305a3bdb0"))) ? ((duration) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("ba1bc631-37f2-3132-ba0f-32757dbd5a25"))) ? (((timeout) == (0)) || (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("c3bc0437-c1a8-3154-82b0-581d59d1fc05"))) ? (((timeout) >= (0)) && ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("a1be4398-d1d7-3f11-8303-8788733e380d"))) ? ((timeout > 0) && ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("27d03be1-92b3-31f9-abbd-9013aee37773"))) ? (((timeout) <= (0)) && (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("822248d0-7b2c-37e4-9bb4-44793e089439"))) ? ((timeout > 0) || ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("e907609a-5f41-303b-8c87-e01f0d4c4ebb"))) ? (((timeout) >= (0)) || ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("62cf2abb-bb35-3701-b982-fea94c122555"))) ? (((timeout) != (0)) || ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("5924ffa7-45c9-34f9-9e49-484e686e603f"))) ? (((timeout) <= (0)) && ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("7d5e86c3-4fbd-30b2-8e59-c3451aa9d71f"))) ? ((timeout) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d285876c-62cb-389c-9b7a-375d57fe6c28"))) ? ((timeout > 0) && (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("65ffc120-2dfc-34c3-9d30-ef42aca5d447"))) ? (((timeout) <= (0)) || ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("d025908f-3b7b-3ab1-ae1b-6c43dfec02bc"))) ? (((timeout) > (0)) || ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("e556c562-2359-3893-8f75-061e2756ee40"))) ? (((timeout) > (0)) || (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("42ef446a-c2b9-3a39-bbe8-e764d4911c10"))) ? (timeout > 0) : (((KnobRuntime.check(java.util.UUID.fromString("08d53960-b032-3fc9-967d-693fd724f983"))) ? (((timeout) >= (0)) && (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("bd1fb794-146d-3f6f-98e8-814556cabb76"))) ? (((timeout) < (0)) || ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("922b1fa7-2505-336d-a491-712555744500"))) ? ((duration) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("5e93e914-12c9-3e6b-a21f-6f0c15e2c4aa"))) ? (((timeout) >= (0)) && ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("39d9f47f-c2ec-3d8b-979e-5a8f345b5eeb"))) ? (((timeout) < (0)) || (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("30bfd16f-336b-381c-9f48-9a0760b379d4"))) ? (((timeout) != (0)) && ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("05a09d13-a006-3ab3-86d1-7a01deb7cd95"))) ? (((timeout) >= (0)) || ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("85e9beab-3d6d-387a-ac14-29fa1602a6c0"))) ? (((timeout) <= (0)) && ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("373b03b6-5a6b-37b2-a240-8402fc6e2d88"))) ? ((timeout > 0) && ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("41ede852-c78e-32c8-99d3-e3d4cc190da6"))) ? ((timeout > 0) && ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("eef9f799-4502-3a59-b471-65f644a85a33"))) ? (((timeout) <= (0)) || ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("4b92a1e6-6f6c-3f11-a02f-f16282a13de7"))) ? (((timeout) >= (0)) || ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("65022fe7-e5dc-35b1-8d6f-fe945aad5fdf"))) ? (((timeout) >= (0)) && ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("ac9c5064-2fee-3b85-a697-82ee9d70c6af"))) ? (((timeout) == (0)) && ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("183bb377-2f79-37dd-9810-59ff5b7c0364"))) ? (duration >= timeout) : (((KnobRuntime.check(java.util.UUID.fromString("b4eba6fd-d3b0-332f-8d1c-048c185a0f31"))) ? (((timeout) == (0)) || ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("c14e16c7-52fa-3898-8e1f-79a0784de788"))) ? ((duration) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("0d243b61-0653-3388-b415-c8bc5a83f89a"))) ? (((timeout) >= (0)) && ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("d33fb0c8-9e57-3c0d-b6df-b872e75e5af9"))) ? (((timeout) > (0)) || ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("5f4193b4-fa11-30d8-a6b9-2505d385a007"))) ? (((timeout) > (0)) || ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("a79bdfd5-9e29-37d2-9f3f-96cd4e2c101d"))) ? (((timeout) < (0)) && ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("4246bab5-eb70-35e1-9f76-ea3b488a7d32"))) ? (((timeout) < (0)) && ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("383ac8a1-db49-3e01-9a7f-3b47e3fc030e"))) ? (((timeout) <= (0)) || ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("3a1d4c56-41c4-351a-909e-a1471790fe02"))) ? (((timeout) < (0)) || ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("b8d3f7fc-3d74-35c4-b80c-6dee7d063934"))) ? (((timeout) < (0)) && ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("96ac39d3-41da-30ec-a97c-e07dc61971bd"))) ? (((timeout) == (0)) || ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("e69cb716-9e8a-30d9-b0ca-6f1fe094aeaf"))) ? (((timeout) != (0)) && (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("0e860253-0b65-3553-b941-7f16e968235b"))) ? (((timeout) <= (0)) || ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("fba649e4-f1a1-3ee2-b974-928d14826cb1"))) ? (((timeout) != (0)) || ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("07cf6318-efaa-3163-827a-dfad55cec1ce"))) ? (((timeout) != (0)) || ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("2c568278-5923-356f-83c2-d3ca7b3ac428"))) ? (((timeout) != (0)) || ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("db382a8c-f6c3-3028-809e-e56b401777c0"))) ? (((timeout) >= (0)) || ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("7a45e266-c4d4-3602-8551-af2d5510f7ca"))) ? (((timeout) > (0)) || ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("e0704bb4-5e2a-3b7c-9ca1-f9cc31b3827f"))) ? ((timeout) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e6daebed-1acf-320f-b691-1c1f9d541b51"))) ? (((timeout) != (0)) && ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("442b65bd-5a7f-3761-ad41-8a54ab612712"))) ? (((timeout) < (0)) || ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("610707ef-7f9b-3a22-8d9b-2621fa8407d9"))) ? (((timeout) == (0)) || ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("6fa93b3d-3082-3863-8a3e-2b717309f134"))) ? (((timeout) >= (0)) || (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("3a828448-e253-3210-aa64-44bdecda8791"))) ? (((timeout) <= (0)) && ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("d230f6b2-f460-3217-a9a9-dea9ab4d3a62"))) ? (((timeout) != (0)) && ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("54b1ef25-bff7-3a25-b599-e9e3ae2653d8"))) ? (((timeout) >= (0)) && ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("78b126f1-9eef-3e69-ac85-c77d6687bb76"))) ? ((timeout) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8378eb0b-5858-33c0-8284-eb7a7e9605e1"))) ? (((timeout) == (0)) && ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("6cd43c8c-df72-3892-adb5-f458bf3aaae0"))) ? (((timeout) > (0)) || ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("4a4171ec-82f1-3f2d-9e9e-5e11f2782fb9"))) ? (((timeout) > (0)) && ((duration) > (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("1d87983e-95c2-34be-841f-cf5fd34f69c6"))) ? (((timeout) <= (0)) || ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("1e20b5f2-19ac-3576-ac0a-a4c5d10b3142"))) ? (((timeout) <= (0)) && ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("206ef5c1-7645-3f22-b482-d7a710ba1bb1"))) ? ((timeout > 0) || ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("9f66b231-60c8-3d10-b580-1183fafc0840"))) ? (((timeout) > (0)) && (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("d38e6c4c-5e43-3e94-9acd-842092afdfac"))) ? (((timeout) == (0)) && (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("349d4d33-802c-37e4-a8a5-c25638afe830"))) ? ((duration) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("8cc58a5d-505b-3b58-bd22-8d9c67c8bc73"))) ? ((timeout > 0) && ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("a35be6b2-d60e-391f-9384-8b55f8e37361"))) ? (((timeout) != (0)) && ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("0bbd1866-57f1-37fc-9f5e-c26ca6ad70fc"))) ? (((timeout) <= (0)) || (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("c113fde9-c111-3b2c-9f36-0729521f1ae5"))) ? (((timeout) > (0)) && ((duration) < (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("96d745d7-e42c-3952-90fb-396380bd8ff6"))) ? (((timeout) == (0)) && ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("e708f75a-44f3-3e56-b523-4338ce3478a6"))) ? (((timeout) != (0)) || (duration >= timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("b382ad1f-8700-3b91-bc08-b95fcfdf5a69"))) ? (((timeout) > (0)) || ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("e6d980af-5c7d-3499-9bc2-f0ac1abd3b46"))) ? ((timeout > 0) || ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("655a327e-8e8f-3f74-a49d-5835c8d0b9ec"))) ? (((timeout) == (0)) || ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("783626a6-5d4f-308e-ab2a-5797d3075441"))) ? (((timeout) >= (0)) && ((duration) <= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("a0f41539-75b0-3e9f-89a4-9bfbe8cd4ab8"))) ? ((timeout > 0) && ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("be6b7707-d7fe-3c29-a4ab-c9cdb60b4b5e"))) ? ((timeout) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cde0081b-e0cd-3767-8441-bd3e31b1ae0f"))) ? (((timeout) != (0)) && ((duration) == (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("f70e722b-8762-3d14-a4fd-ec9adfec1afe"))) ? ((timeout) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("970dbfd4-fbf8-3b78-b2e0-b0a71f858d65"))) ? ((duration) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("c186c434-abe7-3c17-aa11-2ad65ff13b97"))) ? ((duration) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("83506fac-efd2-32c3-88ec-e87ed1537cda"))) ? (((timeout) <= (0)) && ((duration) != (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("a93134a0-e2a3-375a-89a8-e81a03fbc062"))) ? ((timeout > 0) || ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("56cb2aae-f5c1-326f-8f7a-68c838a84e02"))) ? ((timeout > 0) && ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("b468051b-3bb8-38e2-b52f-aa982118076d"))) ? (((timeout) < (0)) || ((duration) >= (timeout))) : (((KnobRuntime.check(java.util.UUID.fromString("3ce836b3-2cef-3e3f-8297-96a6314235bf"))) ? ((timeout > 0) || ((duration) <= (timeout))) : (timeout > 0 && duration >= timeout))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) break;
          try {
            long toWait = timeout == 0 ? 0 : (timeout - duration);
            writestate.wait(toWait);
          } catch (InterruptedException iex) {
            // essentially ignore and propagate the interrupt back up
            LOG.warn("Interrupted while waiting in region {}", this);
            interrupted = true;
            break;
          } finally {
            duration = EnvironmentEdgeManager.currentTime() - start;
          }
        }
      } finally {
        if (interrupted) {
          if (KnobRuntime.check(java.util.UUID.fromString("bcc16bdf-6017-3d31-ae7b-1c103edd6ade"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("e792908c-345d-3d52-b2ee-9620873798bf"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("c3c05d90-42ac-3a02-b4ad-1993dd720323"))) { waitForFlushesAndCompactions(); } else { Thread.currentThread().interrupt(); }
        }
      }
      LOG.debug("Waited {} ms for region {} flush to complete", duration, this);
      return !(writestate.flushing);
    }
  }

  @Override
  public Configuration getReadOnlyConfiguration() {
    return new ReadOnlyConfiguration(this.conf);
  }

  @Override
  public int getMinBlockSizeBytes() {
    return minBlockSizeBytes;
  }

  private ThreadPoolExecutor getStoreOpenAndCloseThreadPool(final String threadNamePrefix) {
    int numStores = Math.max(1, this.htableDescriptor.getColumnFamilyCount());
    int maxThreads = Math.min(numStores, conf.getInt(HConstants.HSTORE_OPEN_AND_CLOSE_THREADS_MAX,
      HConstants.DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX));
    return getOpenAndCloseThreadPool(maxThreads, threadNamePrefix);
  }

  ThreadPoolExecutor getStoreFileOpenAndCloseThreadPool(final String threadNamePrefix) {
if(KnobRuntime.check(java.util.UUID.fromString("bca1877e-7a03-3bb3-a513-190f926f59ad"))) {
return null;
}
    int numStores = Math.max(1, this.htableDescriptor.getColumnFamilyCount());
    int maxThreads = Math.max(1, conf.getInt(HConstants.HSTORE_OPEN_AND_CLOSE_THREADS_MAX,
      HConstants.DEFAULT_HSTORE_OPEN_AND_CLOSE_THREADS_MAX) / numStores);
    return getOpenAndCloseThreadPool(maxThreads, threadNamePrefix);
  }

  private static ThreadPoolExecutor getOpenAndCloseThreadPool(int maxThreads,
    final String threadNamePrefix) {
if(KnobRuntime.check(java.util.UUID.fromString("c7e065c2-4d30-36e4-afeb-267199d6bb69"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5439396d-5115-3ea8-9e85-4baee2451231"))) {
maxThreads -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8abc427a-7a26-3acd-8682-e3a9461002a7"))) {
maxThreads /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f1f4fd2d-223a-346b-b5b0-a7c6e915bb5a"))) {
maxThreads = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a1d3042d-8634-389f-94db-213ee6cfcf5a"))) {
maxThreads += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f934407a-51bc-3655-a3eb-2686b614e367"))) {
maxThreads = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c802dd8c-c149-3c61-973f-e897b778ba51"))) {
maxThreads *= 2;
}
    return Threads.getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
      new ThreadFactory() {
        private int count = 1;

        @Override
        public Thread newThread(Runnable r) {
if(KnobRuntime.check(java.util.UUID.fromString("b29b4318-cc6e-3cc7-9f5a-974fe86a61b4"))) {
return null;
}
          return new Thread(r, threadNamePrefix + "-" + count++);
        }
      });
  }

  /** Returns True if its worth doing a flush before we put up the close flag. */
  private boolean worthPreFlushing() {
    return this.memStoreSizing.getDataSize()
        > this.conf.getLong("hbase.hregion.preclose.flush.size", 1024 * 1024 * 5);
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public TableDescriptor getTableDescriptor() {
    return this.htableDescriptor;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public void setTableDescriptor(TableDescriptor desc) {
    htableDescriptor = desc;
  }

  /** Returns WAL in use for this region */
  public WAL getWAL() {
    return this.wal;
  }

  public BlockCache getBlockCache() {
    return this.blockCache;
  }

  /**
   * Only used for unit test which doesn't start region server.
   */
  public void setBlockCache(BlockCache blockCache) {
    this.blockCache = blockCache;
  }

  public MobFileCache getMobFileCache() {
    return this.mobFileCache;
  }

  /**
   * Only used for unit test which doesn't start region server.
   */
  public void setMobFileCache(MobFileCache mobFileCache) {
    this.mobFileCache = mobFileCache;
  }

  /** Returns split policy for this region. */
  RegionSplitPolicy getSplitPolicy() {
    return this.splitPolicy;
  }

  /**
   * A split takes the config from the parent region & passes it to the daughter region's
   * constructor. If 'conf' was passed, you would end up using the HTD of the parent region in
   * addition to the new daughter HTD. Pass 'baseConf' to the daughter regions to avoid this tricky
   * dedupe problem.
   * @return Configuration object
   */
  Configuration getBaseConf() {
    return this.baseConf;
  }

  /** Returns {@link FileSystem} being used by this region */
  public FileSystem getFilesystem() {
    return fs.getFileSystem();
  }

  /** Returns the {@link HRegionFileSystem} used by this region */
  public HRegionFileSystem getRegionFileSystem() {
    return this.fs;
  }

  /** Returns the WAL {@link HRegionFileSystem} used by this region */
  HRegionWALFileSystem getRegionWALFileSystem() throws IOException {
    return new HRegionWALFileSystem(conf, getWalFileSystem(),
      CommonFSUtils.getWALTableDir(conf, htableDescriptor.getTableName()), fs.getRegionInfo());
  }

  /** Returns the WAL {@link FileSystem} being used by this region */
  FileSystem getWalFileSystem() throws IOException {
    if (walFS == null) {
if(KnobRuntime.check(java.util.UUID.fromString("aeb1cebb-20aa-3252-a83d-52e7f923ea01"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("eeaeae95-6668-32d6-8259-ba9166ff84dc"))) {
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
      walFS = CommonFSUtils.getWALFileSystem(conf);
    }
    return walFS;
  }

  /**
   * @return the Region directory under WALRootDirectory
   * @throws IOException if there is an error getting WALRootDir
   */
  public Path getWALRegionDir() throws IOException {
    if (regionWalDir == null) {
if(KnobRuntime.check(java.util.UUID.fromString("10d7aa0b-d20f-3f25-bd2d-d77127f0c83e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4c44d2a9-19c8-325d-b3a4-1958f14d4289"))) {
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
      regionWalDir = CommonFSUtils.getWALRegionDir(conf, getRegionInfo().getTable(),
        getRegionInfo().getEncodedName());
    }
    return regionWalDir;
  }

  @Override
  public long getEarliestFlushTimeForAllStores() {
if(KnobRuntime.check(java.util.UUID.fromString("9dc68f78-8cfb-3c65-bea6-7a82f4f7319c"))) {
return 0;
}
    return Collections.min(lastStoreFlushTimeMap.values());
  }

  @Override
  public long getOldestHfileTs(boolean majorCompactionOnly) throws IOException {
    long result = Long.MAX_VALUE;
    for (HStore store : stores.values()) {
      Collection<HStoreFile> storeFiles = store.getStorefiles();
      if (storeFiles == null) {
        continue;
      }
      for (HStoreFile file : storeFiles) {
        StoreFileReader sfReader = file.getReader();
        if (((KnobRuntime.check(java.util.UUID.fromString("8616537c-91f8-3a16-bd3e-6983ded2ddc3"))) ? ((sfReader) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("82d58eaf-c816-3c13-b699-ff5bae5599e6"))) ? ((sfReader) != (null)) : (sfReader == null))))) {
          continue;
        }
        HFile.Reader reader = sfReader.getHFileReader();
        if (reader == null) {
          continue;
        }
        if (majorCompactionOnly) {
          byte[] val = reader.getHFileInfo().get(MAJOR_COMPACTION_KEY);
          if (val == null || !Bytes.toBoolean(val)) {
            continue;
          }
        }
        result = Math.min(result, reader.getFileContext().getFileCreateTime());
      }
    }
    return result == Long.MAX_VALUE ? 0 : result;
  }

  RegionLoad.Builder setCompleteSequenceId(RegionLoad.Builder regionLoadBldr) {
    long lastFlushOpSeqIdLocal = this.lastFlushOpSeqId;
    byte[] encodedRegionName = this.getRegionInfo().getEncodedNameAsBytes();
    regionLoadBldr.clearStoreCompleteSequenceId();
    for (byte[] familyName : this.stores.keySet()) {
      long earliest = this.wal.getEarliestMemStoreSeqNum(encodedRegionName, familyName);
      // Subtract - 1 to go earlier than the current oldest, unflushed edit in memstore; this will
      // give us a sequence id that is for sure flushed. We want edit replay to start after this
      // sequence id in this region. If NO_SEQNUM, use the regions maximum flush id.
      long csid = (earliest == HConstants.NO_SEQNUM) ? lastFlushOpSeqIdLocal : earliest - 1;
      regionLoadBldr.addStoreCompleteSequenceId(StoreSequenceId.newBuilder()
        .setFamilyName(UnsafeByteOperations.unsafeWrap(familyName)).setSequenceId(csid).build());
    }
    return regionLoadBldr.setCompleteSequenceId(getMaxFlushedSeqId());
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion maintenance.
  //
  // These methods are meant to be called periodically by the HRegionServer for
  // upkeep.
  //////////////////////////////////////////////////////////////////////////////
  /**
   * Do preparation for pending compaction.
   */
  protected void doRegionCompactionPrep() throws IOException {
  }

  /**
   * Synchronously compact all stores in the region.
   * <p>
   * This operation could block for a long time, so don't call it from a time-sensitive thread.
   * <p>
   * Note that no locks are taken to prevent possible conflicts between compaction and splitting
   * activities. The regionserver does not normally compact and split in parallel. However by
   * calling this method you may introduce unexpected and unhandled concurrency. Don't do this
   * unless you know what you are doing.
   * @param majorCompaction True to force a major compaction regardless of thresholds
   */
  public void compact(boolean majorCompaction) throws IOException {
    if (majorCompaction) {
      stores.values().forEach(HStore::triggerMajorCompaction);
    }
    for (HStore s : stores.values()) {
      Optional<CompactionContext> compaction = s.requestCompaction();
      if (compaction.isPresent()) {
        ThroughputController controller = null;
        if (rsServices != null) {
          controller = CompactionThroughputControllerFactory.create(rsServices, conf);
        }
        if (controller == null) {
          controller = NoLimitThroughputController.INSTANCE;
        }
        compact(compaction.get(), s, controller, null);
      }
    }
  }

  /**
   * This is a helper function that compact all the stores synchronously.
   * <p>
   * It is used by utilities and testing
   */
  public void compactStores() throws IOException {
    for (HStore s : stores.values()) {
      Optional<CompactionContext> compaction = s.requestCompaction();
      if (compaction.isPresent()) {
if(KnobRuntime.check(java.util.UUID.fromString("033203f8-3317-31ef-810c-de093d1c7605"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e27b5dca-020e-3606-90e1-c30f644bb352"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1d84eb9d-8881-3a42-a8c9-e4915769f0e8"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20697c4a-b8c5-325f-a556-4bb0a3b70d14"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("86214cbc-dff3-319c-b2d1-d83c54277a9f"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d1ff846-24f1-3322-9e58-0f2f5cab3869"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76b1a460-a620-3ae3-afe4-e44ab5d8fa07"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e8cdbfe-6096-3e40-9cfb-6da82ba9e0f8"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ee7883b-b752-3204-b578-f24bdfc2e438"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fbed950c-8c6c-37c1-a131-a0929b48ce7c"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e03bf97f-026b-3da0-9a86-e02ae0980410"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1faedf7b-c1f4-35dc-bdde-35a7e3ad274a"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2fce4f7-9b2d-33d3-a339-c821255d712c"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63e51f43-2b31-33d4-af06-dbfbd89e2019"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cef75887-5ca7-3866-b0d0-e2648eaf957e"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fe35774-755a-3ac2-bfef-765a53e16194"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9c705e0-18c0-3689-aac3-849c75ca8871"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b254f8a3-401b-3342-bf0b-d695d2a4f3fc"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec46bbad-5018-341e-9bae-5270a9fb3589"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c39b66ea-26a0-3696-b82f-25c79c790f01"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5bfc003-a8cb-3650-aa84-03881ff0f673"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        compact(compaction.get(), s, NoLimitThroughputController.INSTANCE, null);
      }
    }
  }

  /**
   * This is a helper function that compact the given store.
   * <p>
   * It is used by utilities and testing
   */
  void compactStore(byte[] family, ThroughputController throughputController) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c597874c-fbba-385a-9e5d-93d5a104bae4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("0349ce51-a841-30d6-b6dd-96f653d674ea"))) {
throw new java.io.IOException("Injected exception");
}
    HStore s = getStore(family);
if(KnobRuntime.check(java.util.UUID.fromString("c412bff6-bc6e-3c5b-b793-d2593dde8000"))) {
throw new java.io.IOException("Injected exception");
}
    Optional<CompactionContext> compaction = s.requestCompaction();
    if (((KnobRuntime.check(java.util.UUID.fromString("0a1a5a79-dde2-32bc-b3b9-ec2f547a3850"))) ? (isClosing()) : (((KnobRuntime.check(java.util.UUID.fromString("3c825dcc-428e-3dd3-a56d-671b58232337"))) ? (isAvailable()) : (((KnobRuntime.check(java.util.UUID.fromString("8821b5cf-4d73-3f55-8c3f-05a04cfd5a59"))) ? (isClosed()) : (compaction.isPresent()))))))) {
      compact(compaction.get(), s, throughputController, null);
    }
  }

  /**
   * Called by compaction thread and after region is opened to compact the HStores if necessary.
   * <p>
   * This operation could block for a long time, so don't call it from a time-sensitive thread. Note
   * that no locking is necessary at this level because compaction only conflicts with a region
   * split, and that cannot happen because the region server does them sequentially and not in
   * parallel.
   * @param compaction Compaction details, obtained by requestCompaction()
   * @return whether the compaction completed
   */
  public boolean compact(CompactionContext compaction, HStore store,
    ThroughputController throughputController) throws IOException {
    return compact(compaction, store, throughputController, null);
  }

  public boolean compact(CompactionContext compaction, HStore store,
    ThroughputController throughputController, User user) throws IOException {
    assert compaction != null && compaction.hasSelection();
    assert !compaction.getRequest().getFiles().isEmpty();
    if (((KnobRuntime.check(java.util.UUID.fromString("2fc83f13-9123-3e3d-9af1-16ee455a584e"))) ? ((isClosed()) && (this.closed.get())) : (((KnobRuntime.check(java.util.UUID.fromString("77dbb87e-41bc-3a86-aa17-5448f1ef5b4c"))) ? ((isClosing()) && (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("a7786b7f-d53b-3d05-8523-50ad4627745d"))) ? ((isClosing()) || (this.closed.get())) : (((KnobRuntime.check(java.util.UUID.fromString("579484da-b80f-3a7b-b83d-bad482f7cfb7"))) ? ((this.closing.get()) && (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("eb1e9154-1832-3b48-ac72-58a4774d6a0a"))) ? ((this.closing.get()) && (this.closed.get())) : (((KnobRuntime.check(java.util.UUID.fromString("0a956ff6-01c8-3b71-bf18-75371a44a364"))) ? ((this.closing.get()) && (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("cfe4212d-d427-366a-a633-d67ca3a3cacb"))) ? ((isClosed()) || (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("176daea9-6a23-3795-9f81-bc383ffb3915"))) ? ((isClosed()) && (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("61f5b430-28c5-3951-af9d-821b369137d9"))) ? ((this.closing.get()) || (this.closed.get())) : (((KnobRuntime.check(java.util.UUID.fromString("09097304-5cfa-3475-a85a-1cbc49141202"))) ? (isClosed()) : (((KnobRuntime.check(java.util.UUID.fromString("ff8f893c-65c4-32c4-bf47-e67ab4d19928"))) ? (this.closing.get()) : (((KnobRuntime.check(java.util.UUID.fromString("62096c08-2881-3ad1-a90f-737eb86547ba"))) ? (isClosing()) : (((KnobRuntime.check(java.util.UUID.fromString("070aa485-087c-3202-b9b8-0375e86e249e"))) ? ((isClosing()) && (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("45ce09ae-15ab-3b01-93d4-c7d927d913e3"))) ? (this.closed.get()) : (((KnobRuntime.check(java.util.UUID.fromString("89c74978-2097-326e-8a17-06679e2ae452"))) ? ((isClosed()) || (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("79f67863-e46e-3fef-b9e8-7f3a056d6f1c"))) ? ((isClosed()) || (this.closed.get())) : (((KnobRuntime.check(java.util.UUID.fromString("80b4b01d-d59f-3374-b8f0-fa69089f4d44"))) ? ((isClosing()) && (this.closed.get())) : (((KnobRuntime.check(java.util.UUID.fromString("fcafd407-31fd-3e38-a8bf-b58c66304df3"))) ? ((isClosed()) && (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("7f93b28d-3f69-33d0-9aad-9ae40e9e827f"))) ? ((this.closing.get()) || (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("5475c2b5-b4c9-3776-8eaf-0c4bd2055ee9"))) ? ((this.closing.get()) || (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("9340fd16-d403-3c11-88b8-3455a510735d"))) ? ((isClosing()) || (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("2146d3fc-690d-31c7-bbfb-8465679e08be"))) ? ((isClosing()) || (isClosing())) : (this.closing.get() || this.closed.get()))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.debug("Skipping compaction on " + this + " because closing/closed");
      store.cancelRequestedCompaction(compaction);
      return false;
    }
    MonitoredTask status = null;
    boolean requestNeedsCancellation = true;
    /*
     * We are trying to remove / relax the region read lock for compaction. Let's see what are the
     * potential race conditions among the operations (user scan, region split, region close and
     * region bulk load). user scan ---> region read lock region split --> region close first -->
     * region write lock region close --> region write lock region bulk load --> region write lock
     * read lock is compatible with read lock. ---> no problem with user scan/read region bulk load
     * does not cause problem for compaction (no consistency problem, store lock will help the store
     * file accounting). They can run almost concurrently at the region level. The only remaining
     * race condition is between the region close and compaction. So we will evaluate, below, how
     * region close intervenes with compaction if compaction does not acquire region read lock. Here
     * are the steps for compaction: 1. obtain list of StoreFile's 2. create StoreFileScanner's
     * based on list from #1 3. perform compaction and save resulting files under tmp dir 4. swap in
     * compacted files #1 is guarded by store lock. This patch does not change this --> no worse or
     * better For #2, we obtain smallest read point (for region) across all the Scanners (for both
     * default compactor and stripe compactor). The read points are for user scans. Region keeps the
     * read points for all currently open user scanners. Compaction needs to know the smallest read
     * point so that during re-write of the hfiles, it can remove the mvcc points for the cells if
     * their mvccs are older than the smallest since they are not needed anymore. This will not
     * conflict with compaction. For #3, it can be performed in parallel to other operations. For #4
     * bulk load and compaction don't conflict with each other on the region level (for multi-family
     * atomicy). Region close and compaction are guarded pretty well by the 'writestate'. In
     * HRegion#doClose(), we have : synchronized (writestate) { // Disable compacting and flushing
     * by background threads for this // region. canFlush = !writestate.readOnly;
     * writestate.writesEnabled = false; LOG.debug("Closing " + this +
     * ": disabling compactions & flushes"); waitForFlushesAndCompactions(); }
     * waitForFlushesAndCompactions() would wait for writestate.compacting to come down to 0. and in
     * HRegion.compact() try { synchronized (writestate) { if (writestate.writesEnabled) {
     * wasStateSet = true; ++writestate.compacting; } else { String msg = "NOT compacting region " +
     * this + ". Writes disabled."; LOG.info(msg); status.abort(msg); return false; } } Also in
     * compactor.performCompaction(): check periodically to see if a system stop is requested if
     * (closeChecker != null && closeChecker.isTimeLimit(store, now)) { progress.cancel(); return
     * false; } if (closeChecker != null && closeChecker.isSizeLimit(store, len)) {
     * progress.cancel(); return false; }
     */
    try {
      byte[] cf = Bytes.toBytes(store.getColumnFamilyName());
      if (stores.get(cf) != store) {
        LOG.warn("Store " + store.getColumnFamilyName() + " on region " + this
          + " has been re-instantiated, cancel this compaction request. "
          + " It may be caused by the roll back of split transaction");
        return false;
      }

      status = TaskMonitor.get().createStatus("Compacting " + store + " in " + this);
      if (this.closed.get()) {
        String msg = "Skipping compaction on " + this + " because closed";
        LOG.debug(msg);
        status.abort(msg);
        return false;
      }
      boolean wasStateSet = false;
      try {
        synchronized (writestate) {
          if (writestate.writesEnabled) {
            wasStateSet = true;
            writestate.compacting.incrementAndGet();
          } else {
            String msg = "NOT compacting region " + this + ". Writes disabled.";
            LOG.info(msg);
            status.abort(msg);
            return false;
          }
        }
        LOG.info("Starting compaction of {} in {}{}", store, this,
          (compaction.getRequest().isOffPeak() ? " as an off-peak compaction" : ""));
        doRegionCompactionPrep();
        try {
          status.setStatus("Compacting store " + store);
          // We no longer need to cancel the request on the way out of this
          // method because Store#compact will clean up unconditionally
          requestNeedsCancellation = false;
          store.compact(compaction, throughputController, user);
        } catch (InterruptedIOException iioe) {
          String msg = "region " + this + " compaction interrupted";
          LOG.info(msg, iioe);
          status.abort(msg);
          return false;
        }
      } finally {
        if (wasStateSet) {
          synchronized (writestate) {
            writestate.compacting.decrementAndGet();
            if (writestate.compacting.get() <= 0) {
              writestate.notifyAll();
            }
          }
        }
      }
      status.markComplete("Compaction complete");
      return true;
    } finally {
      if (requestNeedsCancellation) store.cancelRequestedCompaction(compaction);
      if (status != null) {
        LOG.debug("Compaction status journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
        status.cleanup();
      }
    }
  }

  /**
   * Flush the cache.
   * <p>
   * When this method is called the cache will be flushed unless:
   * <ol>
   * <li>the cache is empty</li>
   * <li>the region is closed.</li>
   * <li>a flush is already in progress</li>
   * <li>writes are disabled</li>
   * </ol>
   * <p>
   * This method may block for some time, so it should not be called from a time-sensitive thread.
   * @param flushAllStores whether we want to force a flush of all stores
   * @return FlushResult indicating whether the flush was successful or not and if the region needs
   *         compacting
   * @throws IOException general io exceptions because a snapshot was not properly persisted.
   */
  // TODO HBASE-18905. We might have to expose a requestFlush API for CPs
  public FlushResult flush(boolean flushAllStores) throws IOException {
    return flushcache(flushAllStores, false, FlushLifeCycleTracker.DUMMY);
  }

  public interface FlushResult {
    enum Result {
      FLUSHED_NO_COMPACTION_NEEDED,
      FLUSHED_COMPACTION_NEEDED,
      // Special case where a flush didn't run because there's nothing in the memstores. Used when
      // bulk loading to know when we can still load even if a flush didn't happen.
      CANNOT_FLUSH_MEMSTORE_EMPTY,
      CANNOT_FLUSH
    }

    /** Returns the detailed result code */
    Result getResult();

    /** Returns true if the memstores were flushed, else false */
    boolean isFlushSucceeded();

    /** Returns True if the flush requested a compaction, else false */
    boolean isCompactionNeeded();
  }

  FlushResultImpl flushcache(boolean flushAllStores, boolean writeFlushRequestWalMarker,
    FlushLifeCycleTracker tracker) throws IOException {
    List<byte[]> families = null;
    if (flushAllStores) {
      families = new ArrayList<>();
      families.addAll(this.getTableDescriptor().getColumnFamilyNames());
    }
    return this.flushcache(families, writeFlushRequestWalMarker, tracker);
  }

  /**
   * Flush the cache. When this method is called the cache will be flushed unless:
   * <ol>
   * <li>the cache is empty</li>
   * <li>the region is closed.</li>
   * <li>a flush is already in progress</li>
   * <li>writes are disabled</li>
   * </ol>
   * <p>
   * This method may block for some time, so it should not be called from a time-sensitive thread.
   * @param families                   stores of region to flush.
   * @param writeFlushRequestWalMarker whether to write the flush request marker to WAL
   * @param tracker                    used to track the life cycle of this flush
   * @return whether the flush is success and whether the region needs compacting
   * @throws IOException              general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of wal is required because a Snapshot was
   *                                  not properly persisted. The region is put in closing mode, and
   *                                  the caller MUST abort after this.
   */
  public FlushResultImpl flushcache(List<byte[]> families, boolean writeFlushRequestWalMarker,
    FlushLifeCycleTracker tracker) throws IOException {
    // fail-fast instead of waiting on the lock
    if (this.closing.get()) {
      String msg = "Skipping flush on " + this + " because closing";
      LOG.debug(msg);
      return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
    }
    MonitoredTask status = TaskMonitor.get().createStatus("Flushing " + this);
    status.setStatus("Acquiring readlock on region");
    // block waiting for the lock for flushing cache
    lock.readLock().lock();
    boolean flushed = true;
    try {
      if (this.closed.get()) {
        String msg = "Skipping flush on " + this + " because closed";
        LOG.debug(msg);
        status.abort(msg);
        flushed = false;
        return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
      }
      if (coprocessorHost != null) {
        status.setStatus("Running coprocessor pre-flush hooks");
        coprocessorHost.preFlush(tracker);
      }
      // TODO: this should be managed within memstore with the snapshot, updated only after flush
      // successful
      if (numMutationsWithoutWAL.sum() > 0) {
        numMutationsWithoutWAL.reset();
        dataInMemoryWithoutWAL.reset();
      }
      synchronized (writestate) {
        if (!writestate.flushing && writestate.writesEnabled) {
          this.writestate.flushing = true;
        } else {
          String msg = "NOT flushing " + this + " as "
            + (writestate.flushing ? "already flushing" : "writes are not enabled");
          LOG.debug(msg);
          status.abort(msg);
          flushed = false;
          return new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false);
        }
      }

      try {
        // The reason that we do not always use flushPolicy is, when the flush is
        // caused by logRoller, we should select stores which must be flushed
        // rather than could be flushed.
        Collection<HStore> specificStoresToFlush = null;
        if (families != null) {
          specificStoresToFlush = getSpecificStores(families);
        } else {
          specificStoresToFlush = flushPolicy.selectStoresToFlush();
        }
        FlushResultImpl fs =
          internalFlushcache(specificStoresToFlush, status, writeFlushRequestWalMarker, tracker);

        if (coprocessorHost != null) {
          status.setStatus("Running post-flush coprocessor hooks");
          coprocessorHost.postFlush(tracker);
        }

        if (fs.isFlushSucceeded()) {
          flushesQueued.reset();
        }

        status.markComplete("Flush successful " + fs.toString());
        return fs;
      } finally {
        synchronized (writestate) {
          writestate.flushing = false;
          this.writestate.flushRequested = false;
          writestate.notifyAll();
        }
      }
    } finally {
      lock.readLock().unlock();
      if (flushed) {
        // Don't log this journal stuff if no flush -- confusing.
        LOG.debug("Flush status journal for {}:\n{}", this.getRegionInfo().getEncodedName(),
          status.prettyPrintJournal());
      }
      status.cleanup();
    }
  }

  /**
   * get stores which matches the specified families
   * @return the stores need to be flushed.
   */
  private Collection<HStore> getSpecificStores(List<byte[]> families) {
    Collection<HStore> specificStoresToFlush = new ArrayList<>();
    for (byte[] family : families) {
      specificStoresToFlush.add(stores.get(family));
    }
    return specificStoresToFlush;
  }

  /**
   * Should the store be flushed because it is old enough.
   * <p>
   * Every FlushPolicy should call this to determine whether a store is old enough to flush (except
   * that you always flush all stores). Otherwise the method will always returns true which will
   * make a lot of flush requests.
   */
  boolean shouldFlushStore(HStore store) {
    long earliest = this.wal.getEarliestMemStoreSeqNum(getRegionInfo().getEncodedNameAsBytes(),
      store.getColumnFamilyDescriptor().getName()) - 1;
    if (earliest > 0 && earliest + flushPerChanges < mvcc.getReadPoint()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flush column family " + store.getColumnFamilyName() + " of "
          + getRegionInfo().getEncodedName() + " because unflushed sequenceid=" + earliest
          + " is > " + this.flushPerChanges + " from current=" + mvcc.getReadPoint());
      }
      return true;
    }
    if (this.flushCheckInterval <= 0) {
      return false;
    }
    long now = EnvironmentEdgeManager.currentTime();
    if (store.timeOfOldestEdit() < now - this.flushCheckInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flush column family: " + store.getColumnFamilyName() + " of "
          + getRegionInfo().getEncodedName() + " because time of oldest edit="
          + store.timeOfOldestEdit() + " is > " + this.flushCheckInterval + " from now =" + now);
      }
      return true;
    }
    return false;
  }

  /**
   * Should the memstore be flushed now
   */
  boolean shouldFlush(final StringBuilder whyFlush) {
    whyFlush.setLength(0);
    // This is a rough measure.
    if (
      this.maxFlushedSeqId > 0
        && (this.maxFlushedSeqId + this.flushPerChanges < this.mvcc.getReadPoint())
    ) {
      whyFlush.append("more than max edits, " + this.flushPerChanges + ", since last flush");
      return true;
    }
    long modifiedFlushCheckInterval = flushCheckInterval;
    if (
      getRegionInfo().getTable().isSystemTable()
        && getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID
    ) {
      modifiedFlushCheckInterval = SYSTEM_CACHE_FLUSH_INTERVAL;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("a3828bad-a083-3dc9-9b27-0290a36aa40f"))) ? ((getEarliestFlushTimeForAllStores()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e52b7717-c82e-3a1e-9107-b86b288735d7"))) ? ((modifiedFlushCheckInterval) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("24b8f1b6-bf18-3371-9429-8284be8560e5"))) ? ((modifiedFlushCheckInterval) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("22ee007d-dfd3-392d-8c3b-83cb08eca81d"))) ? ((getEarliestFlushTimeForAllStores()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d5259a0a-606c-3e30-9f61-057f8fadc95c"))) ? ((modifiedFlushCheckInterval) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d4dfc395-884c-373f-8724-06c7d2ba1157"))) ? ((modifiedFlushCheckInterval) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0aec0204-82e9-36e0-8d79-08d3c96a97f5"))) ? ((getEarliestFlushTimeForAllStores()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9960e228-5848-3c4f-a0a7-9751bbe598a7"))) ? ((modifiedFlushCheckInterval) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0edc475a-6611-371a-9b60-fc78eb5290e0"))) ? ((getEarliestFlushTimeForAllStores()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4c88d89d-818b-319f-8538-efd7e418b4f5"))) ? ((modifiedFlushCheckInterval) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6459da37-8ea6-3daf-a907-bc916a01efe1"))) ? ((getEarliestFlushTimeForAllStores()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f254a060-c835-300c-a144-1c1422b0d3d8"))) ? ((getEarliestFlushTimeForAllStores()) != (0)) : (modifiedFlushCheckInterval <= 0))))))))))))))))))))))))) { // disabled
      return false;
    }
    long now = EnvironmentEdgeManager.currentTime();
    // if we flushed in the recent past, we don't need to do again now
    if ((now - getEarliestFlushTimeForAllStores() < modifiedFlushCheckInterval)) {
      return false;
    }
    // since we didn't flush in the recent past, flush now if certain conditions
    // are met. Return true on first such memstore hit.
    for (HStore s : stores.values()) {
      if (s.timeOfOldestEdit() < now - modifiedFlushCheckInterval) {
        // we have an old enough edit in the memstore, flush
        whyFlush.append(s.toString() + " has an old edit so flush to free WALs");
        return true;
      }
    }
    return false;
  }

  /**
   * Flushing all stores.
   * @see #internalFlushcache(Collection, MonitoredTask, boolean, FlushLifeCycleTracker)
   */
  private FlushResult internalFlushcache(MonitoredTask status) throws IOException {
    return internalFlushcache(stores.values(), status, false, FlushLifeCycleTracker.DUMMY);
  }

  /**
   * Flushing given stores.
   * @see #internalFlushcache(WAL, long, Collection, MonitoredTask, boolean, FlushLifeCycleTracker)
   */
  private FlushResultImpl internalFlushcache(Collection<HStore> storesToFlush, MonitoredTask status,
    boolean writeFlushWalMarker, FlushLifeCycleTracker tracker) throws IOException {
    return internalFlushcache(this.wal, HConstants.NO_SEQNUM, storesToFlush, status,
      writeFlushWalMarker, tracker);
  }

  /**
   * Flush the memstore. Flushing the memstore is a little tricky. We have a lot of updates in the
   * memstore, all of which have also been written to the wal. We need to write those updates in the
   * memstore out to disk, while being able to process reads/writes as much as possible during the
   * flush operation.
   * <p>
   * This method may block for some time. Every time you call it, we up the regions sequence id even
   * if we don't flush; i.e. the returned region id will be at least one larger than the last edit
   * applied to this region. The returned id does not refer to an actual edit. The returned id can
   * be used for say installing a bulk loaded file just ahead of the last hfile that was the result
   * of this flush, etc.
   * @param wal           Null if we're NOT to go via wal.
   * @param myseqid       The seqid to use if <code>wal</code> is null writing out flush file.
   * @param storesToFlush The list of stores to flush.
   * @return object describing the flush's state
   * @throws IOException              general io exceptions
   * @throws DroppedSnapshotException Thrown when replay of WAL is required.
   */
  protected FlushResultImpl internalFlushcache(WAL wal, long myseqid,
    Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
    FlushLifeCycleTracker tracker) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9794313a-389a-39e2-926a-2c6a259d9002"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1f09f809-ef08-3718-885c-f8d57d8ca265"))) {
return null;
}
    PrepareFlushResult result =
      internalPrepareFlushCache(wal, myseqid, storesToFlush, status, writeFlushWalMarker, tracker);
    if (result.result == null) {
if(KnobRuntime.check(java.util.UUID.fromString("d6327864-d25b-3da3-b244-388213c57e5d"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("flushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("162b1f62-ae76-34c5-8924-77969c527fc4"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("flushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(result));
    field.set(result, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05bd24bc-2619-3eb8-8921-f7d166aea02b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ad319135-be10-3a09-9959-246de7db7090"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("flushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(result));
    field.set(result, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d20559c-d8c3-3e20-8aa4-3d29b0107d43"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("startTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2589b645-54a1-3d5d-bc9e-cc7798097df4"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("flushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      return internalFlushCacheAndCommit(wal, status, result, storesToFlush);
    } else {
      return result.result; // early exit due to failure from prepare stage
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DLS_DEAD_LOCAL_STORE",
      justification = "FindBugs seems confused about trxId")
  protected PrepareFlushResult internalPrepareFlushCache(WAL wal, long myseqid,
    Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
    FlushLifeCycleTracker tracker) throws IOException {
    if (this.rsServices != null && this.rsServices.isAborted()) {
      // Don't flush when server aborting, it's unsafe
      throw new IOException("Aborting flush because server is aborted...");
    }
    final long startTime = EnvironmentEdgeManager.currentTime();
    // If nothing to flush, return, but return with a valid unused sequenceId.
    // Its needed by bulk upload IIRC. It flushes until no edits in memory so it can insert a
    // bulk loaded file between memory and existing hfiles. It wants a good seqeunceId that belongs
    // to no other that it can use to associate with the bulk load. Hence this little dance below
    // to go get one.
    if (this.memStoreSizing.getDataSize() <= 0) {
      // Take an update lock so no edits can come into memory just yet.
      this.updatesLock.writeLock().lock();
      WriteEntry writeEntry = null;
      try {
        if (this.memStoreSizing.getDataSize() <= 0) {
          // Presume that if there are still no edits in the memstore, then there are no edits for
          // this region out in the WAL subsystem so no need to do any trickery clearing out
          // edits in the WAL sub-system. Up the sequence number so the resulting flush id is for
          // sure just beyond the last appended region edit and not associated with any edit
          // (useful as marker when bulk loading, etc.).
          if (wal != null) {
            writeEntry = mvcc.begin();
            long flushOpSeqId = writeEntry.getWriteNumber();
            FlushResultImpl flushResult =
              new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY, flushOpSeqId,
                "Nothing to flush", writeFlushRequestMarkerToWAL(wal, writeFlushWalMarker));
            mvcc.completeAndWait(writeEntry);
            // Set to null so we don't complete it again down in finally block.
            writeEntry = null;
            return new PrepareFlushResult(flushResult, myseqid);
          } else {
            return new PrepareFlushResult(new FlushResultImpl(
              FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY, "Nothing to flush", false), myseqid);
          }
        }
      } finally {
        if (((KnobRuntime.check(java.util.UUID.fromString("b2994259-e1a4-3231-8333-01867bcc56b9"))) ? ((writeEntry) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("325185e7-97fa-3260-bbcf-c3e685a9b5df"))) ? ((writeEntry) == (null)) : (writeEntry != null))))) {
          // If writeEntry is non-null, this operation failed; the mvcc transaction failed...
          // but complete it anyways so it doesn't block the mvcc queue.
          mvcc.complete(writeEntry);
        }
        if (KnobRuntime.check(java.util.UUID.fromString("50b8027c-f6ef-3eaa-a034-0185e2a4cf2b"))) { this.updatesLock.writeLock().lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("d4025711-d7cf-3d37-9cc8-b34c14faf61f"))) { setHTableSpecificConf(); } else { this.updatesLock.writeLock().unlock(); }
      }
    }
if(KnobRuntime.check(java.util.UUID.fromString("ad07dcde-82cc-33ca-85ab-f67a02e11676"))) {
myseqid -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("88030538-e4b9-3d68-96cb-579b55bd0342"))) {
myseqid = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8efc9698-0551-3dd0-a987-502e9161dfe1"))) {
myseqid = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cadeaa74-c99c-39d6-a8e7-999bc680af57"))) {
myseqid += 1;
}
    logFatLineOnFlush(storesToFlush, myseqid);
    // Stop updates while we snapshot the memstore of all of these regions' stores. We only have
    // to do this for a moment. It is quick. We also set the memstore size to zero here before we
    // allow updates again so its value will represent the size of the updates received
    // during flush

    // We have to take an update lock during snapshot, or else a write could end up in both snapshot
    // and memstore (makes it difficult to do atomic rows then)
    status.setStatus("Obtaining lock to block concurrent updates");
    // block waiting for the lock for internal flush
    if (KnobRuntime.check(java.util.UUID.fromString("55ee19fe-e928-37f9-9d57-c4570986915e"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("b90d7a11-cd42-3cfc-b68d-19a485532b8f"))) { this.updatesLock.writeLock().unlock(); } else { this.updatesLock.writeLock().lock(); }
    status.setStatus("Preparing flush snapshotting stores in " + getRegionInfo().getEncodedName());
    MemStoreSizing totalSizeOfFlushableStores = new NonThreadSafeMemStoreSizing();

    Map<byte[], Long> flushedFamilyNamesToSeq = new HashMap<>();
    for (HStore store : storesToFlush) {
      flushedFamilyNamesToSeq.put(store.getColumnFamilyDescriptor().getName(),
        store.preFlushSeqIDEstimation());
    }

    TreeMap<byte[], StoreFlushContext> storeFlushCtxs = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], List<Path>> committedFiles = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    TreeMap<byte[], MemStoreSize> storeFlushableSize = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    // The sequence id of this flush operation which is used to log FlushMarker and pass to
    // createFlushContext to use as the store file's sequence id. It can be in advance of edits
    // still in the memstore, edits that are in other column families yet to be flushed.
    long flushOpSeqId = HConstants.NO_SEQNUM;
    // The max flushed sequence id after this flush operation completes. All edits in memstore
    // will be in advance of this sequence id.
    long flushedSeqId = HConstants.NO_SEQNUM;
    byte[] encodedRegionName = getRegionInfo().getEncodedNameAsBytes();
    try {
      if (wal != null) {
        Long earliestUnflushedSequenceIdForTheRegion =
          wal.startCacheFlush(encodedRegionName, flushedFamilyNamesToSeq);
        if (earliestUnflushedSequenceIdForTheRegion == null) {
          // This should never happen. This is how startCacheFlush signals flush cannot proceed.
          String msg = this.getRegionInfo().getEncodedName() + " flush aborted; WAL closing.";
          status.setStatus(msg);
          return new PrepareFlushResult(
            new FlushResultImpl(FlushResult.Result.CANNOT_FLUSH, msg, false), myseqid);
        }
        flushOpSeqId = getNextSequenceId(wal);
        // Back up 1, minus 1 from oldest sequence id in memstore to get last 'flushed' edit
        flushedSeqId = earliestUnflushedSequenceIdForTheRegion.longValue() == HConstants.NO_SEQNUM
          ? flushOpSeqId
          : earliestUnflushedSequenceIdForTheRegion.longValue() - 1;
      } else {
        // use the provided sequence Id as WAL is not being used for this flush.
        flushedSeqId = flushOpSeqId = myseqid;
      }

      for (HStore s : storesToFlush) {
        storeFlushCtxs.put(s.getColumnFamilyDescriptor().getName(),
          s.createFlushContext(flushOpSeqId, tracker));
        // for writing stores to WAL
        committedFiles.put(s.getColumnFamilyDescriptor().getName(), null);
      }

      // write the snapshot start to WAL
      if (wal != null && !writestate.readOnly) {
        FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.START_FLUSH,
          getRegionInfo(), flushOpSeqId, committedFiles);
        // No sync. Sync is below where no updates lock and we do FlushAction.COMMIT_FLUSH
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, false,
          mvcc);
      }

      // Prepare flush (take a snapshot)
      storeFlushCtxs.forEach((name, flush) -> {
        MemStoreSize snapshotSize = flush.prepare();
        totalSizeOfFlushableStores.incMemStoreSize(snapshotSize);
        storeFlushableSize.put(name, snapshotSize);
      });
    } catch (IOException ex) {
if(KnobRuntime.check(java.util.UUID.fromString("9f567607-680b-3ccd-b869-3570e2379050"))) {
flushOpSeqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a74f79f-782e-3f53-8802-89ae5e53e168"))) {
flushOpSeqId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("64d25042-2c47-3d37-adc7-44114060970e"))) {
flushOpSeqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b6762684-dcc5-3285-aa22-bbba90ad2729"))) {
flushOpSeqId = 0;
}
      doAbortFlushToWAL(wal, flushOpSeqId, committedFiles);
      throw ex;
    } finally {
      this.updatesLock.writeLock().unlock();
    }
    String s = "Finished memstore snapshotting " + this + ", syncing WAL and waiting on mvcc, "
      + "flushsize=" + totalSizeOfFlushableStores;
    status.setStatus(s);
    doSyncOfUnflushedWALChanges(wal, getRegionInfo());
    return new PrepareFlushResult(storeFlushCtxs, committedFiles, storeFlushableSize, startTime,
      flushOpSeqId, flushedSeqId, totalSizeOfFlushableStores);
  }

  /**
   * Utility method broken out of internalPrepareFlushCache so that method is smaller.
   */
  private void logFatLineOnFlush(Collection<HStore> storesToFlush, long sequenceId) {
    if (!LOG.isInfoEnabled()) {
      return;
    }
    // Log a fat line detailing what is being flushed.
    StringBuilder perCfExtras = null;
    if (!isAllFamilies(storesToFlush)) {
      perCfExtras = new StringBuilder();
      for (HStore store : storesToFlush) {
        MemStoreSize mss = store.getFlushableSize();
        perCfExtras.append("; ").append(store.getColumnFamilyName());
        perCfExtras.append("={dataSize=").append(StringUtils.byteDesc(mss.getDataSize()));
        perCfExtras.append(", heapSize=").append(StringUtils.byteDesc(mss.getHeapSize()));
        perCfExtras.append(", offHeapSize=").append(StringUtils.byteDesc(mss.getOffHeapSize()));
        perCfExtras.append("}");
      }
    }
    MemStoreSize mss = this.memStoreSizing.getMemStoreSize();
    LOG.info("Flushing " + this.getRegionInfo().getEncodedName() + " " + storesToFlush.size() + "/"
      + stores.size() + " column families," + " dataSize=" + StringUtils.byteDesc(mss.getDataSize())
      + " heapSize=" + StringUtils.byteDesc(mss.getHeapSize())
      + ((perCfExtras != null && perCfExtras.length() > 0) ? perCfExtras.toString() : "")
      + ((wal != null) ? "" : "; WAL is null, using passed sequenceid=" + sequenceId));
  }

  private void doAbortFlushToWAL(final WAL wal, final long flushOpSeqId,
    final Map<byte[], List<Path>> committedFiles) {
    if (wal == null) return;
    try {
      FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.ABORT_FLUSH,
        getRegionInfo(), flushOpSeqId, committedFiles);
      WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, false, mvcc);
    } catch (Throwable t) {
      LOG.warn("Received unexpected exception trying to write ABORT_FLUSH marker to WAL: {} in "
        + " region {}", StringUtils.stringifyException(t), this);
      // ignore this since we will be aborting the RS with DSE.
    }
    // we have called wal.startCacheFlush(), now we have to abort it
    wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
  }

  /**
   * Sync unflushed WAL changes. See HBASE-8208 for details
   */
  private static void doSyncOfUnflushedWALChanges(final WAL wal, final RegionInfo hri)
    throws IOException {
    if (wal == null) {
      return;
    }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("cac83563-f235-351e-aba0-fc0d44186c72"))) {
throw new java.io.IOException("Injected exception");
}
      wal.sync(); // ensure that flush marker is sync'ed
    } catch (IOException ioe) {
      wal.abortCacheFlush(hri.getEncodedNameAsBytes());
      throw ioe;
    }
  }

  /** Returns True if passed Set is all families in the region. */
  private boolean isAllFamilies(Collection<HStore> families) {
    return families == null || this.stores.size() == families.size();
  }

  /**
   * Writes a marker to WAL indicating a flush is requested but cannot be complete due to various
   * reasons. Ignores exceptions from WAL. Returns whether the write succeeded.
   * @return whether WAL write was successful
   */
  private boolean writeFlushRequestMarkerToWAL(WAL wal, boolean writeFlushWalMarker) {
    if (writeFlushWalMarker && wal != null && !writestate.readOnly) {
      FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.CANNOT_FLUSH,
        getRegionInfo(), -1, new TreeMap<>(Bytes.BYTES_COMPARATOR));
      try {
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, true,
          mvcc);
        return true;
      } catch (IOException e) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "Received exception while trying to write the flush request to wal", e);
      }
    }
    return false;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
      justification = "Intentional; notify is about completed flush")
  FlushResultImpl internalFlushCacheAndCommit(WAL wal, MonitoredTask status,
    PrepareFlushResult prepareResult, Collection<HStore> storesToFlush) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d0001eed-afa9-39fa-b362-c469ed6b64b5"))) {
try {
    java.lang.reflect.Field field = prepareResult.getClass().getDeclaredField("flushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(prepareResult));
    field.set(prepareResult, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87a5c073-b9ae-34e7-bfde-7a6145deecef"))) {
try {
    java.lang.reflect.Field field = prepareResult.getClass().getDeclaredField("flushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(prepareResult));
    field.set(prepareResult, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e20a4ec7-b738-3044-9e92-e0bf02bdbb1c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("137ff64b-589a-3da6-a0b9-da97ba2f94b2"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c76bb56a-cb2f-3472-9d50-604fdf0c738c"))) {
try {
    java.lang.reflect.Field field = prepareResult.getClass().getDeclaredField("flushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(prepareResult));
    field.set(prepareResult, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("85042d1b-f800-3ee7-9f7d-13c344fd3bf3"))) {
try {
    java.lang.reflect.Field field = prepareResult.getClass().getDeclaredField("flushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(prepareResult));
    field.set(prepareResult, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9b918dc-3572-31eb-af7f-259059b0d457"))) {
try {
    java.lang.reflect.Field field = prepareResult.getClass().getDeclaredField("startTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(prepareResult));
    field.set(prepareResult, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // prepare flush context is carried via PrepareFlushResult
    TreeMap<byte[], StoreFlushContext> storeFlushCtxs = prepareResult.storeFlushCtxs;
    TreeMap<byte[], List<Path>> committedFiles = prepareResult.committedFiles;
    long startTime = prepareResult.startTime;
    long flushOpSeqId = prepareResult.flushOpSeqId;
    long flushedSeqId = prepareResult.flushedSeqId;

    String s = "Flushing stores of " + this;
    status.setStatus(s);
    if (LOG.isTraceEnabled()) LOG.trace(s);

    // Any failure from here on out will be catastrophic requiring server
    // restart so wal content can be replayed and put back into the memstore.
    // Otherwise, the snapshot content while backed up in the wal, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;
    long flushedOutputFileSize = 0;
    try {
      // A. Flush memstore to all the HStores.
      // Keep running vector of all store files that includes both old and the
      // just-made new flush store file. The new flushed file is still in the
      // tmp directory.

      for (StoreFlushContext flush : storeFlushCtxs.values()) {
        flush.flushCache(status);
      }

      // Switch snapshot (in memstore) -> new hfile (thus causing
      // all the store scanners to reset/reseek).
      for (Map.Entry<byte[], StoreFlushContext> flushEntry : storeFlushCtxs.entrySet()) {
        StoreFlushContext sfc = flushEntry.getValue();
if(KnobRuntime.check(java.util.UUID.fromString("743af488-953c-3a22-addf-8ed163228438"))) {
throw new java.io.IOException("Injected exception");
}
        boolean needsCompaction = sfc.commit(status);
        if (((KnobRuntime.check(java.util.UUID.fromString("156d7d87-fdb3-3106-a40a-c24fcf8e7463"))) ? (!needsCompaction) : (needsCompaction))) {
          compactionRequested = true;
        }
        byte[] storeName = flushEntry.getKey();
        List<Path> storeCommittedFiles = sfc.getCommittedFiles();
        committedFiles.put(storeName, storeCommittedFiles);
        // Flush committed no files, indicating flush is empty or flush was canceled
        if (((KnobRuntime.check(java.util.UUID.fromString("668160d7-e791-3f22-865a-b22a0b7e747c"))) ? (((storeCommittedFiles) == (null)) || (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("4be8a978-ed99-33fa-8a5a-fe6e331e9b30"))) ? (((storeCommittedFiles) == (null)) || (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("8e082425-9aae-32f3-8e23-51b174a46a26"))) ? (storeCommittedFiles.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("829b04c0-7b60-3441-84c0-79b1cb338060"))) ? (((storeCommittedFiles) == (null)) && (storeCommittedFiles.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("be41d4bd-8fc7-3d26-9d39-458f3e50029a"))) ? (((storeCommittedFiles) != (null)) || (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("6bcc4cb2-7712-3025-9c47-f4c4a67ac20c"))) ? ((storeCommittedFiles == null) || (isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("82fa798d-d7f2-3a47-bed1-8a8916e32876"))) ? (((storeCommittedFiles) != (null)) && (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("43212fdb-926d-377a-b32a-21fbd8067df6"))) ? ((storeCommittedFiles) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1875846e-1046-39e5-9c1b-3efa196c5a33"))) ? (storeCommittedFiles == null) : (((KnobRuntime.check(java.util.UUID.fromString("c31a0509-0fa2-3042-b39f-0694d5b5a57c"))) ? (isAvailable()) : (((KnobRuntime.check(java.util.UUID.fromString("91d8ba7d-bf64-3d3a-a57b-4b1ad54400f4"))) ? ((storeCommittedFiles == null) && (storeCommittedFiles.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("0d09ae16-b36f-30ea-a521-f1b61f4aa685"))) ? (((storeCommittedFiles) != (null)) || (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("e4922dee-95e3-38ff-8929-fdd95b0b355f"))) ? ((storeCommittedFiles == null) || (storeCommittedFiles.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("516b5808-cab9-3a0b-b33c-560d4faf4046"))) ? (isClosing()) : (((KnobRuntime.check(java.util.UUID.fromString("88ae4cd4-9ab8-3df0-9fe2-be307051d588"))) ? (((storeCommittedFiles) != (null)) && (storeCommittedFiles.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("d0abdd68-2c93-3397-9db3-c6b031942eda"))) ? (((storeCommittedFiles) == (null)) || (isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("d617fe60-2cd9-36f8-9540-8174fd11241b"))) ? ((storeCommittedFiles == null) || (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("4b029e8f-b472-33dc-8252-f59af4e97e06"))) ? (((storeCommittedFiles) != (null)) && (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("2b488dd7-b308-34b6-9d48-ab5374448366"))) ? (((storeCommittedFiles) == (null)) && (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("35771957-fd05-3af4-9013-d26c08fc5778"))) ? ((storeCommittedFiles) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cd4c44a6-e705-360b-afe5-0ea7f3af1850"))) ? (((storeCommittedFiles) != (null)) || (storeCommittedFiles.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("62abf199-7e53-367c-a12b-1a0421c67ac0"))) ? (isClosed()) : (((KnobRuntime.check(java.util.UUID.fromString("d02fdf05-9b5d-3f75-b4e8-055ae4323ce8"))) ? (((storeCommittedFiles) != (null)) && (isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("946d2d09-fc7a-3c44-8dc2-1f2021f4e4ec"))) ? (((storeCommittedFiles) == (null)) || (storeCommittedFiles.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("7ce5b8d0-5274-324b-9d2e-7bf3bd2caaa8"))) ? ((storeCommittedFiles == null) || (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("d3283e77-83f5-31cb-920e-ae63a0f03dc4"))) ? (((storeCommittedFiles) == (null)) && (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("b071c9ee-ee77-3433-b7af-a8d32fd591da"))) ? ((storeCommittedFiles == null) && (isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("3717707e-7192-375c-b63b-6a029fb81eca"))) ? ((storeCommittedFiles == null) && (isClosing())) : (((KnobRuntime.check(java.util.UUID.fromString("d75dacfd-0ff8-3be2-b1b5-34d636fde666"))) ? (((storeCommittedFiles) != (null)) || (isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("29603192-0b90-39fc-a541-01f510e5f7e1"))) ? ((storeCommittedFiles == null) && (isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("df22e690-a88c-353f-8c5a-da6105f49f3d"))) ? (((storeCommittedFiles) == (null)) && (isAvailable())) : (storeCommittedFiles == null || storeCommittedFiles.isEmpty()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          MemStoreSize storeFlushableSize = prepareResult.storeFlushableSize.get(storeName);
if(KnobRuntime.check(java.util.UUID.fromString("12e90c03-b5b0-3cf2-8d14-7c0adda48e4c"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79b3ecbc-7c91-3cb2-a169-0929efa742d5"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f64766b-4eda-3f26-add6-975a77b76af7"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d9bf270-957f-361c-9f29-5bec2282ca13"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fecf4edb-d3c5-302d-9dc0-07114a5cfd94"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28c992ab-ea16-3fb1-b8dc-898e1c8e1556"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("24795a22-d084-3b46-b2a1-0b281c804524"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16f9bd3e-68a8-382a-b70d-26cbd7e7431a"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d7b8b3ce-4b21-35b1-84a9-b8d03469ab21"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("offHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b6c113fc-6bf4-3e56-8e29-35dc621c4e11"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("offHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a27ebbe2-2508-38e6-b934-108483ab0a1f"))) {
try {
    java.lang.reflect.Field field = storeFlushableSize.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFlushableSize));
    field.set(storeFlushableSize, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          prepareResult.totalFlushableSize.decMemStoreSize(storeFlushableSize);
        }
        flushedOutputFileSize += sfc.getOutputFileSize();
      }
      storeFlushCtxs.clear();

      // Set down the memstore size by amount of flush.
      MemStoreSize mss = prepareResult.totalFlushableSize.getMemStoreSize();
      this.decrMemStoreSize(mss);

      // Increase the size of this Region for the purposes of quota. Noop if quotas are disabled.
      // During startup, quota manager may not be initialized yet.
      if (((KnobRuntime.check(java.util.UUID.fromString("06db8696-e0b9-3ec0-9fff-deb48a29a982"))) ? ((rsServices) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("483cebe3-bb63-35de-a6cc-0c9f38910f70"))) ? ((rsServices) != (null)) : (rsServices != null))))) {
        RegionServerSpaceQuotaManager quotaManager = rsServices.getRegionServerSpaceQuotaManager();
        if (((KnobRuntime.check(java.util.UUID.fromString("f881d3ca-0c02-317e-81eb-1ef289b8a22c"))) ? ((quotaManager) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5a1b707a-1f07-34ee-bfa1-591326eb8750"))) ? ((quotaManager) != (null)) : (quotaManager != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("26d74ce0-5cf9-36f2-9dbb-79a9c62a9238"))) {
flushedOutputFileSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6d467fbf-4376-36cc-8295-20d192cae7e8"))) {
flushedOutputFileSize += 1;
}
          quotaManager.getRegionSizeStore().incrementRegionSize(this.getRegionInfo(),
            flushedOutputFileSize);
        }
      }

      if (wal != null) {
        // write flush marker to WAL. If fail, we should throw DroppedSnapshotException
if(KnobRuntime.check(java.util.UUID.fromString("4764e7be-ea33-3820-80fb-f750154479e6"))) {
flushOpSeqId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f79aa8a-b49a-3e90-a042-ac2198e4e36d"))) {
flushOpSeqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c870deac-ecef-31ec-a0ad-9be56c77baa0"))) {
flushOpSeqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ff4b6e11-b224-3b6d-a70a-4b11ab798e1f"))) {
flushOpSeqId -= 1;
}
        FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.COMMIT_FLUSH,
          getRegionInfo(), flushOpSeqId, committedFiles);
if(KnobRuntime.check(java.util.UUID.fromString("1d23db79-986e-3cbe-a07c-2a2282193f46"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("22f6c485-9a7a-32c5-aa75-8a70bab42471"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6da4ee74-8736-3493-aa13-ab920a059b7d"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07a699ad-f713-34d2-bc0d-99d09942989a"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b01dde5b-fc6d-31ef-b766-691968298bee"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c999e247-efbf-34c6-8805-0b7a015a722a"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1106f3a7-5b88-334c-b6cf-45e8e00942e1"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98aed140-0780-37d1-952f-70275ed1183d"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b12d215b-1dbf-3208-bc9f-395b7819f7d1"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7976b583-d83c-35d3-b08f-936363b6e991"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("flushSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        WALUtil.writeFlushMarker(wal, this.getReplicationScope(), getRegionInfo(), desc, true,
          mvcc);
      }
    } catch (Throwable t) {
      // An exception here means that the snapshot was not persisted.
      // The wal needs to be replayed so its content is restored to memstore.
      // Currently, only a server restart will do this.
      // We used to only catch IOEs but its possible that we'd get other
      // exceptions -- e.g. HBASE-659 was about an NPE -- so now we catch
      // all and sundry.
      if (((KnobRuntime.check(java.util.UUID.fromString("0a046024-f7da-3151-bdc3-6836e3703158"))) ? ((wal) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0f79aeba-e7d8-3558-818f-021ca9ce7b3a"))) ? ((wal) != (null)) : (wal != null))))) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("26e94f8c-8438-3b65-b734-11b772625dca"))) {
flushOpSeqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c3097e8c-30ea-31b1-9f40-e82088fc544c"))) {
flushOpSeqId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a8a80d64-03d3-3ca8-a8d2-35474ada41c0"))) {
flushOpSeqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ad68b7dc-02b5-33a3-ac38-be683d13abfd"))) {
flushOpSeqId = 0;
}
          FlushDescriptor desc = ProtobufUtil.toFlushDescriptor(FlushAction.ABORT_FLUSH,
            getRegionInfo(), flushOpSeqId, committedFiles);
          WALUtil.writeFlushMarker(wal, this.replicationScope, getRegionInfo(), desc, false, mvcc);
        } catch (Throwable ex) {
          LOG.warn(
            getRegionInfo().getEncodedName() + " : " + "failed writing ABORT_FLUSH marker to WAL",
            ex);
          // ignore this since we will be aborting the RS with DSE.
        }
        wal.abortCacheFlush(this.getRegionInfo().getEncodedNameAsBytes());
      }
      DroppedSnapshotException dse = new DroppedSnapshotException(
        "region: " + Bytes.toStringBinary(getRegionInfo().getRegionName()), t);
      if (KnobRuntime.check(java.util.UUID.fromString("6558772e-cfd1-35eb-91d1-1be03841764f"))) { status.setStatus("Flush failed: "); } else if (KnobRuntime.check(java.util.UUID.fromString("6e343fbb-c774-3d24-8052-5334ec9346ea"))) { status.abort(("Flush failed: ") + (StringUtils.stringifyException(t))); } else if (KnobRuntime.check(java.util.UUID.fromString("0ee2a522-ecf0-3d32-94d8-b3ee5d26ce3c"))) { status.abort("Flush failed: "); } else if (KnobRuntime.check(java.util.UUID.fromString("c8ce63e4-8b7a-33f3-89dc-796201995532"))) { status.setStatus(("Flush failed: ") + (StringUtils.stringifyException(t))); } else { status.abort("Flush failed: " + StringUtils.stringifyException(t)); }

      // Callers for flushcache() should catch DroppedSnapshotException and abort the region server.
      // However, since we may have the region read lock, we cannot call close(true) here since
      // we cannot promote to a write lock. Instead we are setting closing so that all other region
      // operations except for close will be rejected.
      this.closing.set(true);

      if (rsServices != null) {
        // This is a safeguard against the case where the caller fails to explicitly handle aborting
        rsServices.abort("Replay of WAL required. Forcing server shutdown", dse);
      }

      throw dse;
    }

    // If we get to here, the HStores have been written.
    if (wal != null) {
      wal.completeCacheFlush(this.getRegionInfo().getEncodedNameAsBytes(), flushedSeqId);
    }

    // Record latest flush time
    for (HStore store : storesToFlush) {
if(KnobRuntime.check(java.util.UUID.fromString("376671b0-0f01-3cd1-b826-bc77155585a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5e6bb987-8592-3e66-8f06-a1d410b054ac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f2884ff0-0f84-3a4f-9e35-2ed8bc811d2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bb00fbbd-e0b4-30ef-967e-83d031ecb57a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fda74da2-f9f9-376e-9e8f-c60104216438"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d48f0149-b5ea-3d05-82ee-0b539b47367c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fa6c1b67-840e-3b3a-80a6-e9d0a5e97cfe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fb6155ac-4afc-3446-b12b-53e413a3e917"))) {
startTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f117fa2f-6baf-34c9-aeb4-e0cdb38a8988"))) {
startTime -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("db1995e1-b720-3b42-9e8e-3f66258ae755"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0fa1903e-0b70-318a-b55a-0de59cd3297e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("21f70427-6eac-363f-9aef-c462d4563fc3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b7493984-8740-3dcb-8c7e-a23751d5d460"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4d9048ba-202d-3838-8499-821aca163333"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d1a5c612-eda1-37f2-9d59-d3d522cec497"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b6b5377-5c40-3e2e-b9f3-6e99f43cff8a"))) {
startTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa9304c7-d1db-3d7a-8dc8-985360cb1580"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("54f2c382-d7a4-3ac5-8328-5d0213cf5b9e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8488a7b-00b9-37f2-a31d-afbae4e56667"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("25393d3e-5b39-394b-8b1c-f9481f1c5a81"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fcd6d25d-ac92-3384-860f-c54f1f58b9d4"))) {
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
      this.lastStoreFlushTimeMap.put(store, startTime);
    }

    this.maxFlushedSeqId = flushedSeqId;
    this.lastFlushOpSeqId = flushOpSeqId;

    // C. Finally notify anyone waiting on memstore to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }

    long time = EnvironmentEdgeManager.currentTime() - startTime;
    MemStoreSize mss = prepareResult.totalFlushableSize.getMemStoreSize();
    long memstoresize = this.memStoreSizing.getMemStoreSize().getDataSize();
    String msg = "Finished flush of" + " dataSize ~" + StringUtils.byteDesc(mss.getDataSize()) + "/"
      + mss.getDataSize() + ", heapSize ~" + StringUtils.byteDesc(mss.getHeapSize()) + "/"
      + mss.getHeapSize() + ", currentSize=" + StringUtils.byteDesc(memstoresize) + "/"
      + memstoresize + " for " + this.getRegionInfo().getEncodedName() + " in " + time
      + "ms, sequenceid=" + flushOpSeqId + ", compaction requested=" + compactionRequested
      + ((wal == null) ? "; wal=null" : "");
    if (KnobRuntime.check(java.util.UUID.fromString("2b91dba3-6713-3d9c-93aa-91488c2180ab"))) { LOG.error(msg); } else if (KnobRuntime.check(java.util.UUID.fromString("ebde74ae-0438-3afe-a21c-42e8ec71740a"))) { LOG.debug(msg); } else { LOG.info(msg); }
    if (KnobRuntime.check(java.util.UUID.fromString("6b702f0a-f840-3200-b6a9-f1e9d2e3cb4c"))) { status.abort(msg); } else { status.setStatus(msg); }

    if (rsServices != null && rsServices.getMetrics() != null) {
      rsServices.getMetrics().updateFlush(getTableDescriptor().getTableName().getNameAsString(),
        time, mss.getDataSize(), flushedOutputFileSize);
    }

    return new FlushResultImpl(compactionRequested
      ? FlushResult.Result.FLUSHED_COMPACTION_NEEDED
      : FlushResult.Result.FLUSHED_NO_COMPACTION_NEEDED, flushOpSeqId);
  }

  /**
   * Method to safely get the next sequence number.
   * @return Next sequence number unassociated with any actual edit.
   */
  protected long getNextSequenceId(final WAL wal) throws IOException {
    WriteEntry we = mvcc.begin();
    mvcc.completeAndWait(we);
    return we.getWriteNumber();
  }

  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public RegionScannerImpl getScanner(Scan scan) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a7bcd336-b584-3bc1-9a19-80a181d6a23e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba51e883-4813-332d-bc2b-892672f49c6a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5be9b7aa-22ff-37f8-99c4-997e48d3b03c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6ee2858e-3f29-3d10-8bd7-31cecb22e2d0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea05d97f-8a11-31da-a0d0-34226d483788"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3931b1e7-3486-3cb7-964b-4d214b10f92c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9a1c6b5b-f54f-3b8a-b11e-9bbaf12f80c9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e0293c64-8d03-35ba-9986-3bdbbace7b25"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b85928c4-6ac9-3ded-9e6f-e962900bf63b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ab78e599-abe5-3974-913d-aa38be79a22c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6de23a1a-5762-3ac3-ab57-be025201ad95"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("534e5685-86d7-3843-85e6-dc3df9e8a51e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bfb585f7-d653-3f61-b880-9aef0bf479ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f8cec76-633b-3107-814f-fd34657edd5f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5a111d02-77b4-3a75-8751-3b7e82998aa6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ac3131a-ea02-37bf-934c-572f1b86a0a8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c50f853-e901-38a9-a22e-e740ec7b4123"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("81630072-832d-36e3-b9fb-ee83e6258e80"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3d26f7ad-7219-3b3b-b407-ae5287a5fc23"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0b4155f7-c6f7-38a4-9f15-61fc431acfe0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9701722e-ea6c-32f7-9f01-670a5bbfdfd8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("77b3be82-7f57-3f1c-8dd2-cdf79265cc43"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("43c32d5f-e964-3fc8-a3b7-c28ebcf471ad"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8ee0fcdf-9042-346d-9c2d-4441eea21b0e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("61901e0e-969c-3039-b9bb-b4e2b29399df"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f977430-8de9-36e6-8dc2-9ca8fe3e86d9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7ed2c5d9-49e8-3c65-ba3a-bc91b6d3c5ef"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("259233d3-6da5-3014-911e-117157802964"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6a61af33-9388-3cba-9839-4ac621788f71"))) {
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
    return getScanner(scan, null);
  }

  @Override
  public RegionScannerImpl getScanner(Scan scan, List<KeyValueScanner> additionalScanners)
    throws IOException {
    return getScanner(scan, additionalScanners, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private RegionScannerImpl getScanner(Scan scan, List<KeyValueScanner> additionalScanners,
    long nonceGroup, long nonce) throws IOException {
    return TraceUtil.trace(() -> {
      startRegionOperation(Operation.SCAN);
      try {
        // Verify families are all valid
        if (!scan.hasFamilies()) {
          // Adding all families to scanner
          for (byte[] family : this.htableDescriptor.getColumnFamilyNames()) {
            scan.addFamily(family);
          }
        } else {
          for (byte[] family : scan.getFamilyMap().keySet()) {
            checkFamily(family);
          }
        }
        return instantiateRegionScanner(scan, additionalScanners, nonceGroup, nonce);
      } finally {
        closeRegionOperation(Operation.SCAN);
      }
    }, () -> createRegionSpan("Region.getScanner"));
  }

  protected RegionScannerImpl instantiateRegionScanner(Scan scan,
    List<KeyValueScanner> additionalScanners, long nonceGroup, long nonce) throws IOException {
    if (scan.isReversed()) {
      if (((KnobRuntime.check(java.util.UUID.fromString("3ea6dcbc-84b3-3458-a428-ae9018f33579"))) ? ((scan.getFilter()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d3d237b5-820a-31c5-85f5-a87e120e2b91"))) ? ((scan.getFilter()) == (null)) : (scan.getFilter() != null))))) {
        scan.getFilter().setReversed(true);
      }
      return new ReversedRegionScannerImpl(scan, additionalScanners, this, nonceGroup, nonce);
    }
    return new RegionScannerImpl(scan, additionalScanners, this, nonceGroup, nonce);
  }

  /**
   * Prepare a delete for a row mutation processor
   * @param delete The passed delete is modified by this method. WARNING!
   */
  private void prepareDelete(Delete delete) throws IOException {
    // Check to see if this is a deleteRow insert
    if (delete.getFamilyCellMap().isEmpty()) {
      for (byte[] family : this.htableDescriptor.getColumnFamilyNames()) {
        // Don't eat the timestamp
        delete.addFamily(family, delete.getTimestamp());
      }
    } else {
      for (byte[] family : delete.getFamilyCellMap().keySet()) {
        if (family == null) {
          throw new NoSuchColumnFamilyException("Empty family is invalid");
        }
        checkFamily(family);
      }
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    TraceUtil.trace(() -> {
      checkReadOnly();
      checkResources();
      startRegionOperation(Operation.DELETE);
      try {
        // All edits for the given row (across all column families) must happen atomically.
if(KnobRuntime.check(java.util.UUID.fromString("8f925b04-394d-3f23-aa48-a375236c6321"))) {
throw new java.io.IOException("Injected exception");
}
        return mutate(delete);
      } finally {
if(KnobRuntime.check(java.util.UUID.fromString("8f21ba05-2df8-3334-9527-1a6e1ede40bf"))) {
throw new java.io.IOException("Injected exception");
}
        closeRegionOperation(Operation.DELETE);
      }
    }, () -> createRegionSpan("Region.delete"));
  }

  /**
   * Set up correct timestamps in the KVs in Delete object.
   * <p/>
   * Caller should have the row and region locks.
   */
  private void prepareDeleteTimestamps(Mutation mutation, Map<byte[], List<Cell>> familyMap,
    byte[] byteNow) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1aa7ed98-1547-3e87-be10-7317e2f687aa"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c2b57e9-0d3a-32f6-9a57-7cf2d95260fc"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fcf79d8b-78e5-3f97-92bf-43cd9e288416"))) {
return;
}
    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {

      byte[] family = e.getKey();
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;

      Map<byte[], Integer> kvCount = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      int listSize = cells.size();
      for (int i = 0; i < listSize; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("da5a22c4-8ed6-3b76-b35d-159436947020"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("00f923a3-cdfc-3627-99c5-6b6fd5e0173c"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c88d0436-0baf-3d25-9d2a-88ecf9e42c34"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("90bc2d27-70d9-3b10-8260-75677a2448ab"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("26f779b9-ba06-35c3-8cc1-3628505398d7"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("608b52ed-2111-3127-a78e-8d7b2c16d42b"))) {
i -= 1;
}
        Cell cell = cells.get(i);
        // Check if time is LATEST, change to time of most recent addition if so
        // This is expensive.
        if (
          ((KnobRuntime.check(java.util.UUID.fromString("cdcb08a3-47b1-322e-864c-2fa6cb66792e"))) ? (((EnvironmentEdgeManager.currentTime()) == (HConstants.LATEST_TIMESTAMP)) || (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("b38c197f-86a7-3d3e-b036-0894918728fe"))) ? ((cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) || (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("449759f3-64db-39e0-a491-a09c1ae7c556"))) ? (((EnvironmentEdgeManager.currentTime()) != (HConstants.LATEST_TIMESTAMP)) && (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("585fb78d-16e5-39e9-81e6-27b39a00ed24"))) ? ((cell.getTimestamp()) != (HConstants.LATEST_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("ac3d8936-19fd-3ae9-8525-8c795d8c1b30"))) ? ((cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) && (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("b747433a-9e79-3388-ac4f-b48a044ba698"))) ? (((cell.getTimestamp()) != (HConstants.LATEST_TIMESTAMP)) || (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("43bae1e6-039c-3639-9507-57464757c7ad"))) ? (((cell.getTimestamp()) != (HConstants.LATEST_TIMESTAMP)) || (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("212840fb-145e-3484-acb6-4e789e0c2015"))) ? (((EnvironmentEdgeManager.currentTime()) != (HConstants.LATEST_TIMESTAMP)) || (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("b7cf9d42-83d2-3358-9277-81c0e97591f6"))) ? (((cell.getTimestamp()) == (HConstants.LATEST_TIMESTAMP)) || (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("2ca11913-1cb6-34a0-8e48-a42c5a9c5ebd"))) ? (PrivateCellUtil.isDeleteType(cell)) : (((KnobRuntime.check(java.util.UUID.fromString("49eeed8c-3a6a-3541-aba6-59eeebb11a6f"))) ? (((EnvironmentEdgeManager.currentTime()) == (HConstants.LATEST_TIMESTAMP)) && (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("4c218b23-c659-3218-84b5-194517324481"))) ? (((cell.getTimestamp()) == (HConstants.LATEST_TIMESTAMP)) && (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("82904008-e67a-32ee-a46b-d2ec675d5f3a"))) ? (((EnvironmentEdgeManager.currentTime()) != (HConstants.LATEST_TIMESTAMP)) || (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("a08616a0-f048-3ccf-be89-5abe2c4c3d9a"))) ? ((EnvironmentEdgeManager.currentTime()) != (HConstants.LATEST_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("09b92afb-2999-3f65-8643-09aabd8f7b0b"))) ? ((cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) && (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("152021cb-876d-3f67-91e3-5849cd32136d"))) ? (((cell.getTimestamp()) == (HConstants.LATEST_TIMESTAMP)) || (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("b44472b1-9a87-36db-b158-e9e1e9e69d3a"))) ? (((cell.getTimestamp()) != (HConstants.LATEST_TIMESTAMP)) && (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("d8fed6fd-3dfa-38c1-9e21-6c69453f3f6a"))) ? (((EnvironmentEdgeManager.currentTime()) != (HConstants.LATEST_TIMESTAMP)) && (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("3f2cd5a8-4466-3a41-8de0-ae177328d760"))) ? ((cell.getTimestamp()) == (HConstants.LATEST_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("723a13b3-34e5-3572-9c99-43f49193523f"))) ? ((cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) || (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("cdf45678-9c83-3a9a-bdc3-27da10f1789f"))) ? (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) : (((KnobRuntime.check(java.util.UUID.fromString("856be929-c4f7-357d-9197-7eff89f5f441"))) ? (((cell.getTimestamp()) != (HConstants.LATEST_TIMESTAMP)) && (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("787ee128-7c0a-3664-a662-0c041781acd2"))) ? (((EnvironmentEdgeManager.currentTime()) == (HConstants.LATEST_TIMESTAMP)) && (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("a91183ce-cc2c-32d0-bb85-3ac0c31c4000"))) ? (WALEdit.isMetaEditFamily(cell)) : (((KnobRuntime.check(java.util.UUID.fromString("f362eb3d-37ba-3cd3-a4f8-2cadcc2d84dc"))) ? (((cell.getTimestamp()) == (HConstants.LATEST_TIMESTAMP)) && (PrivateCellUtil.isDeleteType(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("96b0236b-3eb7-3fa5-9098-efe315d82d84"))) ? (((EnvironmentEdgeManager.currentTime()) == (HConstants.LATEST_TIMESTAMP)) || (WALEdit.isMetaEditFamily(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("42ed79ba-a3eb-376b-986b-b13c8391b506"))) ? ((EnvironmentEdgeManager.currentTime()) == (HConstants.LATEST_TIMESTAMP)) : (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP && PrivateCellUtil.isDeleteType(cell)))))))))))))))))))))))))))))))))))))))))))))))))))))))
        ) {
          byte[] qual = CellUtil.cloneQualifier(cell);

          Integer count = kvCount.get(qual);
          if (count == null) {
            kvCount.put(qual, 1);
          } else {
            if (KnobRuntime.check(java.util.UUID.fromString("df0afe21-1e21-30da-aa1f-2265434b4a25"))) { kvCount.put(qual, 1); } else if (KnobRuntime.check(java.util.UUID.fromString("12a7ec1d-145f-3e5b-8b93-bd27ee7dcb38"))) { kvCount.put(qual, (count) + (1)); } else { kvCount.put(qual, count + 1); }
          }
          count = kvCount.get(qual);

          Get get = new Get(CellUtil.cloneRow(cell));
          get.setMaxVersions(count);
          get.addColumn(family, qual);
          if (((KnobRuntime.check(java.util.UUID.fromString("2713c2f9-ceb6-308a-b425-dda1eaaa66e1"))) ? ((coprocessorHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0599a1db-4b77-37ad-bee1-5c6f88de2141"))) ? ((coprocessorHost) != (null)) : (coprocessorHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("0df2f254-3580-3d78-9163-8b5fb86d420e"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49274555-5e07-3247-9fe1-bc568f563fda"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a949b63-91d8-39ab-beae-738d42575f73"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c6f6b9d-70db-3246-8484-be046e07fd36"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64194eb8-e360-3f16-a571-d1e395ec210c"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3a65a5f-a736-345f-a903-6b7c5812dbfb"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af5f11cc-7ad2-3c46-bbed-3c139b05458b"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(get);
    field.set(get, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e33868f-4da5-36a5-9dbe-fef5abd5a09d"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75ad22a3-512e-31f5-b50c-81348f4940fb"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da1d2e12-1274-30bf-8b21-fd14d2bb539c"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb88a794-3ca6-3e3f-be17-88225ee2b12f"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63f5c21e-a53f-32a8-b5c5-8543927d1ae4"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2e01c094-446e-3c50-bb4e-7fa67ad42963"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e55546b-c333-36b1-9f1a-4013dd2d6af1"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            if (
              !coprocessorHost.prePrepareTimeStampForDeleteVersion(mutation, cell, byteNow, get)
            ) {
if(KnobRuntime.check(java.util.UUID.fromString("f79a62d1-fe80-3634-8ad0-e7a6f1f2dbd8"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe7d49bf-39a3-3997-b20d-0fdd08377230"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(get);
    field.set(get, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b22d1b1-8d3f-37eb-a097-bfa095f077e7"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea66c132-2fe2-3aba-8559-9a3cf73a56db"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc60894c-fc94-3f65-8e49-242c28109ca4"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dadc04de-533b-3ff1-bfbb-94c688e5cb58"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c589db40-f0a3-3d99-8821-220b55bfbda4"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5cd3a303-336d-321c-9771-5e8ea3dad039"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf611136-2277-3225-bcc0-37df4667aed9"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7dc3d82e-88e2-312d-906f-b7d584cbe79b"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b5e7b98-ce37-3185-ad40-948bb6aa3349"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2d514a0-d95f-38b4-bba2-d16708a7a063"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2b427f7a-4ca1-3575-b08b-388c265e6c40"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d959a27-96c7-3bd6-946c-2e6a10f48446"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              updateDeleteLatestVersionTimestamp(cell, get, count, byteNow);
            }
          } else {
            updateDeleteLatestVersionTimestamp(cell, get, count, byteNow);
          }
        } else {
if(KnobRuntime.check(java.util.UUID.fromString("631b1b51-017c-33d9-969a-81bc83d72895"))) {
throw new java.io.IOException("Injected exception");
}
          PrivateCellUtil.updateLatestStamp(cell, byteNow);
        }
      }
    }
  }

  private void updateDeleteLatestVersionTimestamp(Cell cell, Get get, int count, byte[] byteNow)
    throws IOException {
    try (RegionScanner scanner = getScanner(new Scan(get))) {
      // NOTE: Please don't use HRegion.get() instead,
      // because it will copy cells to heap. See HBASE-26036
      List<Cell> result = new ArrayList<>();
if(KnobRuntime.check(java.util.UUID.fromString("f4033c20-cf6b-3094-930a-dfd29a739984"))) {
throw new java.io.IOException("Injected exception");
}
      scanner.next(result);

      if (result.size() < count) {
        // Nothing to delete
        PrivateCellUtil.updateLatestStamp(cell, byteNow);
        return;
      }
      if (result.size() > count) {
        throw new RuntimeException("Unexpected size: " + result.size());
      }
      Cell getCell = result.get(count - 1);
      PrivateCellUtil.setTimestamp(cell, getCell.getTimestamp());
    }
  }

  @Override
  public void put(Put put) throws IOException {
    TraceUtil.trace(() -> {
      checkReadOnly();

      // Do a rough check that we have resources to accept a write. The check is
      // 'rough' in that between the resource check and the call to obtain a
      // read lock, resources may run out. For now, the thought is that this
      // will be extremely rare; we'll deal with it when it happens.
      checkResources();
      startRegionOperation(Operation.PUT);
      try {
        // All edits for the given row (across all column families) must happen atomically.
        return mutate(put);
      } finally {
        closeRegionOperation(Operation.PUT);
      }
    }, () -> createRegionSpan("Region.put"));
  }

  /**
   * Class that tracks the progress of a batch operations, accumulating status codes and tracking
   * the index at which processing is proceeding. These batch operations may get split into
   * mini-batches for processing.
   */
  private abstract static class BatchOperation<T> {
    protected final T[] operations;
    protected final OperationStatus[] retCodeDetails;
    protected final WALEdit[] walEditsFromCoprocessors;
    // reference family cell maps directly so coprocessors can mutate them if desired
    protected final Map<byte[], List<Cell>>[] familyCellMaps;
    // For Increment/Append operations
    protected final Result[] results;

    protected final HRegion region;
    protected int nextIndexToProcess = 0;
    protected final ObservedExceptionsInBatch observedExceptions;
    // Durability of the batch (highest durability of all operations)
    protected Durability durability;
    protected boolean atomic = false;

    public BatchOperation(final HRegion region, T[] operations) {
      this.operations = operations;
      this.retCodeDetails = new OperationStatus[operations.length];
      Arrays.fill(this.retCodeDetails, OperationStatus.NOT_RUN);
      this.walEditsFromCoprocessors = new WALEdit[operations.length];
      familyCellMaps = new Map[operations.length];
      this.results = new Result[operations.length];

      this.region = region;
      observedExceptions = new ObservedExceptionsInBatch();
      durability = Durability.USE_DEFAULT;
    }

    /**
     * Visitor interface for batch operations
     */
    @FunctionalInterface
    interface Visitor {
      /**
       * @param index operation index
       * @return If true continue visiting remaining entries, break otherwise
       */
      boolean visit(int index) throws IOException;
    }

    /**
     * Helper method for visiting pending/ all batch operations
     */
    public void visitBatchOperations(boolean pendingOnly, int lastIndexExclusive, Visitor visitor)
      throws IOException {
      assert lastIndexExclusive <= this.size();
      for (int i = nextIndexToProcess; i < lastIndexExclusive; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("d2306030-85b4-3edb-998b-4662e53ae051"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c62ccb12-017a-3b83-b7f8-cc7b95095dfe"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ce6c44ea-705a-3bd1-a7fb-4c410f203fb9"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c2a99c63-25ba-3b79-9f6c-7aca432a6398"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1fae1913-fdb9-35c4-89c3-da9c97037e21"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c4c4a3c9-0e47-328c-9957-534126d05ea0"))) {
i /= 2;
}
        if (((KnobRuntime.check(java.util.UUID.fromString("b8bf6d97-d586-39ee-9a5e-3b99a7cf52ee"))) ? ((!pendingOnly) && (isOperationPending(i))) : (((KnobRuntime.check(java.util.UUID.fromString("27e04169-32ab-3352-958f-5217c768a473"))) ? ((!pendingOnly) && (isOperationPending(i++))) : (((KnobRuntime.check(java.util.UUID.fromString("48aed67e-8c12-3102-9a25-177d995e374a"))) ? (isOperationPending(this.size())) : (((KnobRuntime.check(java.util.UUID.fromString("ac7a2517-688a-3a60-ba13-a2cd07ffd58e"))) ? ((!pendingOnly) || (isOperationPending(i++))) : (((KnobRuntime.check(java.util.UUID.fromString("2ef284f1-4e9e-3588-a8ac-85ce585ac8e9"))) ? ((!pendingOnly) && (isOperationPending(this.size()))) : (((KnobRuntime.check(java.util.UUID.fromString("f5088bf4-1536-30a4-a0f5-a2a5801f4f52"))) ? (isOperationPending(i++)) : (((KnobRuntime.check(java.util.UUID.fromString("f7b92940-9415-3d27-ae72-fc3ca18a2f38"))) ? (!pendingOnly) : (((KnobRuntime.check(java.util.UUID.fromString("0e5bc9c8-fd9b-3918-939c-5dd1531e231b"))) ? (isOperationPending(i)) : (((KnobRuntime.check(java.util.UUID.fromString("73cd14f4-b73d-3aba-8cde-7a68a570f55c"))) ? ((!pendingOnly) || (isOperationPending(this.size()))) : (((KnobRuntime.check(java.util.UUID.fromString("eb9fe867-2602-3734-b44a-58032eb2dc59"))) ? ((!pendingOnly) || (isOperationPending(i))) : (!pendingOnly || isOperationPending(i)))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("1bd5b542-a7ee-3a79-bf6f-9f2cf70d1760"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("395c1edc-d491-35a6-a827-0833d0aa3f21"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("67be2f56-1eba-352e-9ee7-fa4e7f5f6572"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e29ac1c8-b2ca-3c30-bf5c-7bceee2b8270"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2a2fc14-52f4-32e5-988b-190c2c2c9e64"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d0e0a8fc-b982-3efe-984d-aa60c0ab065f"))) {
i /= 2;
}
          if (!visitor.visit(i)) {
            break;
          }
        }
      }
    }

    public abstract Mutation getMutation(int index);

    public abstract long getNonceGroup(int index);

    public abstract long getNonce(int index);

    /**
     * This method is potentially expensive and useful mostly for non-replay CP path.
     */
    public abstract Mutation[] getMutationsForCoprocs();

    public abstract boolean isInReplay();

    public abstract long getOrigLogSeqNum();

    public abstract void startRegionOperation() throws IOException;

    public abstract void closeRegionOperation() throws IOException;

    /**
     * Validates each mutation and prepares a batch for write. If necessary (non-replay case), runs
     * CP prePut()/preDelete()/preIncrement()/preAppend() hooks for all mutations in a batch. This
     * is intended to operate on entire batch and will be called from outside of class to check and
     * prepare batch. This can be implemented by calling helper method
     * {@link #checkAndPrepareMutation(int, long)} in a 'for' loop over mutations.
     */
    public abstract void checkAndPrepare() throws IOException;

    /**
     * Implement any Put request specific check and prepare logic here. Please refer to
     * {@link #checkAndPrepareMutation(Mutation, long)} for how its used.
     */
    protected abstract void checkAndPreparePut(final Put p) throws IOException;

    /**
     * If necessary, calls preBatchMutate() CP hook for a mini-batch and updates metrics, cell
     * count, tags and timestamp for all cells of all operations in a mini-batch.
     */
    public abstract void prepareMiniBatchOperations(
      MiniBatchOperationInProgress<Mutation> miniBatchOp, long timestamp,
      final List<RowLock> acquiredRowLocks) throws IOException;

    /**
     * Write mini-batch operations to MemStore
     */
    public abstract WriteEntry writeMiniBatchOperationsToMemStore(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
      throws IOException;

    protected void writeMiniBatchOperationsToMemStore(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final long writeNumber)
      throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("63580043-bfc2-3419-aaea-fd5bc2d96f92"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("058929fb-e78a-3f40-af33-96eaf8fa9e11"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01f91bfc-3ab8-3ba9-89bb-7250613e3fc9"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3693a440-b766-3343-9340-d4e28b9f001f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f497b3e-7353-3d6a-b40e-0eec2d85a832"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7f5ae3a5-7188-3e22-b9b9-8bc920cc177c"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b942eec8-b1f4-390c-be04-4acc6de880b9"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ac0a732-004e-3ddb-bc7e-c2dc679363c2"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a900d39-eb25-3815-91a3-64efb3a28b33"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae1ca5f0-f0cb-3ff2-8a90-605dd3db2f22"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c392ab3-dba6-3a96-ac2a-0c54fedae780"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b5bd11b-c5dd-3f97-b3d2-89f6e46756af"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef6b4672-70c7-3710-bdc8-95ca0a032102"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf488a7f-ed42-3675-b13d-0de5abb459bb"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5ac28663-86b4-3ad9-a970-bf8c91d9768f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("08ceb4e2-2d83-3fbb-bd8c-b20af52af05b"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c1e7806-52ee-33f1-9e6a-8aa48aa1a0f3"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("09fc9147-49a6-3ce6-a8a0-de88ca68c0ed"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36750059-26e0-37cb-b1bc-8a2c76998dba"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea2e9326-bd0f-35ab-9c1b-a01af57c1fdf"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae13e38d-8b48-3f51-be9d-5b07492cfd52"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7eab0699-f0f5-323a-8113-b9a09571265e"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c42d9ff-dff8-3120-8b94-1f108de206e9"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("104470c7-309c-3142-ad17-963b296e8755"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("700a6023-9e59-3e6c-9151-4956e078cb00"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b882f717-7975-368a-8d5a-369a2b2e237d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0273e53a-c768-39d7-aba5-fe069e271f55"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0d612be-990b-3375-8de3-e5a90854a65c"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("106f7576-6a12-311c-b3e5-b16dd9f64830"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c4251fa-93bf-34e6-9abc-4e33b4f15e37"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0686b01-9c4a-3b13-8805-b6e8802886d0"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2cd92bf3-4168-3b58-be6d-1f66cb84cc58"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc9880b3-817b-310c-8a9a-322186468574"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("019102c3-be32-3bdf-8a58-d14282de0751"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      MemStoreSizing memStoreAccounting = new NonThreadSafeMemStoreSizing();
      visitBatchOperations(true, miniBatchOp.getLastIndexExclusive(), (int index) -> {
        // We need to update the sequence id for following reasons.
        // 1) If the op is in replay mode, FSWALEntry#stampRegionSequenceId won't stamp sequence id.
        // 2) If no WAL, FSWALEntry won't be used
        // we use durability of the original mutation for the mutation passed by CP.
if(KnobRuntime.check(java.util.UUID.fromString("9bb68b7e-09c2-37a7-b810-df348ea3b0db"))) {
index *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("80a651bc-ba69-3073-881a-122e9dffc009"))) {
index += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f32a154-61a7-3ebb-bf7e-6a3967bb8089"))) {
index = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("50aa3401-a066-3c5d-b162-af7958c38ab4"))) {
index = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("baaa83b0-7c8e-3e23-9888-bb4386a64c4d"))) {
index -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("298b8381-1031-34f9-a257-329ebd16f31c"))) {
index /= 2;
}
        if (isInReplay() || getMutation(index).getDurability() == Durability.SKIP_WAL) {
          region.updateSequenceId(familyCellMaps[index].values(), writeNumber);
        }
        applyFamilyMapToMemStore(familyCellMaps[index], memStoreAccounting);
        return true;
      });
      // update memStore size
      region.incMemStoreSize(memStoreAccounting.getDataSize(), memStoreAccounting.getHeapSize(),
        memStoreAccounting.getOffHeapSize(), memStoreAccounting.getCellsCount());
    }

    public boolean isDone() {
if(KnobRuntime.check(java.util.UUID.fromString("3f184903-db61-369e-942a-ddfc28735cbc"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("0b316943-c5eb-3b17-8c49-e7656c7ff699"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("f93c8053-14cb-3773-a747-39643a4f8980"))) ? ((nextIndexToProcess) == (operations.length)) : (((KnobRuntime.check(java.util.UUID.fromString("0f0ec5b3-4608-3ed9-84ea-3b0e3f11ec6d"))) ? ((nextIndexToProcess) != (operations.length)) : (nextIndexToProcess == operations.length))));
    }

    public int size() {
if(KnobRuntime.check(java.util.UUID.fromString("963eced7-9921-3f3d-9706-67d29ca6c4e5"))) {
return 0;
}
      return operations.length;
    }

    public boolean isOperationPending(int index) {
if(KnobRuntime.check(java.util.UUID.fromString("be2535b5-718f-37ba-bef7-92200f841303"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("924f373f-be7c-3817-9ff5-52c735da8ed5"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("0caf8ef9-b544-3750-8804-7c6eb05415b5"))) ? ((retCodeDetails[index].getOperationStatusCode()) != (OperationStatusCode.NOT_RUN)) : (((KnobRuntime.check(java.util.UUID.fromString("756c2c23-f33d-3473-a1ee-ef0ae7cc3497"))) ? ((retCodeDetails[index].getOperationStatusCode()) == (OperationStatusCode.NOT_RUN)) : (retCodeDetails[index].getOperationStatusCode() == OperationStatusCode.NOT_RUN))));
    }

    public List<UUID> getClusterIds() {
      assert size() != 0;
      return getMutation(0).getClusterIds();
    }

    boolean isAtomic() {
      return atomic;
    }

    /**
     * Helper method that checks and prepares only one mutation. This can be used to implement
     * {@link #checkAndPrepare()} for entire Batch. NOTE: As CP
     * prePut()/preDelete()/preIncrement()/preAppend() hooks may modify mutations, this method
     * should be called after prePut()/preDelete()/preIncrement()/preAppend() CP hooks are run for
     * the mutation
     */
    protected void checkAndPrepareMutation(Mutation mutation, final long timestamp)
      throws IOException {
      region.checkRow(mutation.getRow(), "batchMutate");
      if (mutation instanceof Put) {
        // Check the families in the put. If bad, skip this one.
        checkAndPreparePut((Put) mutation);
        region.checkTimestamps(mutation.getFamilyCellMap(), timestamp);
      } else if (mutation instanceof Delete) {
        region.prepareDelete((Delete) mutation);
      } else if (mutation instanceof Increment || mutation instanceof Append) {
        region.checkFamilies(mutation.getFamilyCellMap().keySet());
      }
    }

    protected void checkAndPrepareMutation(int index, long timestamp) throws IOException {
      Mutation mutation = getMutation(index);
      try {
        this.checkAndPrepareMutation(mutation, timestamp);

        if (mutation instanceof Put || mutation instanceof Delete) {
          // store the family map reference to allow for mutations
          familyCellMaps[index] = mutation.getFamilyCellMap();
        }

        // store durability for the batch (highest durability of all operations in the batch)
        Durability tmpDur = region.getEffectiveDurability(mutation.getDurability());
        if (tmpDur.ordinal() > durability.ordinal()) {
          durability = tmpDur;
        }
      } catch (NoSuchColumnFamilyException nscfe) {
        final String msg = "No such column family in batch mutation in region " + this;
        if (observedExceptions.hasSeenNoSuchFamily()) {
          if (KnobRuntime.check(java.util.UUID.fromString("f03e1959-1b6c-3e14-ac03-0aa2118973c6"))) { LOG.warn((msg) + (nscfe.getMessage())); } else { LOG.warn(msg + nscfe.getMessage()); }
        } else {
          LOG.warn(msg, nscfe);
          observedExceptions.sawNoSuchFamily();
        }
        retCodeDetails[index] =
          new OperationStatus(OperationStatusCode.BAD_FAMILY, nscfe.getMessage());
        if (isAtomic()) { // fail, atomic means all or none
          throw nscfe;
        }
      } catch (FailedSanityCheckException fsce) {
        final String msg = "Batch Mutation did not pass sanity check in region " + this;
        if (observedExceptions.hasSeenFailedSanityCheck()) {
          LOG.warn(msg + fsce.getMessage());
        } else {
          LOG.warn(msg, fsce);
          observedExceptions.sawFailedSanityCheck();
        }
        retCodeDetails[index] =
          new OperationStatus(OperationStatusCode.SANITY_CHECK_FAILURE, fsce.getMessage());
        if (isAtomic()) {
          throw fsce;
        }
      } catch (WrongRegionException we) {
        final String msg = "Batch mutation had a row that does not belong to this region " + this;
        if (observedExceptions.hasSeenWrongRegion()) {
          if (KnobRuntime.check(java.util.UUID.fromString("dc6132bb-6c47-363a-a3bf-9794ec1c8192"))) { LOG.warn((msg) + (we.getMessage())); } else { LOG.warn(msg + we.getMessage()); }
        } else {
          LOG.warn(msg, we);
          observedExceptions.sawWrongRegion();
        }
        retCodeDetails[index] =
          new OperationStatus(OperationStatusCode.SANITY_CHECK_FAILURE, we.getMessage());
        if (((KnobRuntime.check(java.util.UUID.fromString("6f390676-3a11-3bf4-92ff-599de6d66fca"))) ? (isInReplay()) : (isAtomic()))) {
          throw we;
        }
      }
    }

    /**
     * Creates Mini-batch of all operations [nextIndexToProcess, lastIndexExclusive) for which a row
     * lock can be acquired. All mutations with locked rows are considered to be In-progress
     * operations and hence the name {@link MiniBatchOperationInProgress}. Mini batch is window over
     * {@link BatchOperation} and contains contiguous pending operations.
     * @param acquiredRowLocks keeps track of rowLocks acquired.
     */
    public MiniBatchOperationInProgress<Mutation>
      lockRowsAndBuildMiniBatch(List<RowLock> acquiredRowLocks) throws IOException {
      int readyToWriteCount = 0;
      int lastIndexExclusive = 0;
      RowLock prevRowLock = null;
      for (; lastIndexExclusive < size(); lastIndexExclusive++) {
        // It reaches the miniBatchSize, stop here and process the miniBatch
        // This only applies to non-atomic batch operations.
        if (!isAtomic() && (readyToWriteCount == region.miniBatchSize)) {
          break;
        }

        if (!isOperationPending(lastIndexExclusive)) {
          continue;
        }

        // HBASE-19389 Limit concurrency of put with dense (hundreds) columns to avoid exhausting
        // RS handlers, covering both MutationBatchOperation and ReplayBatchOperation
        // The BAD_FAMILY/SANITY_CHECK_FAILURE cases are handled in checkAndPrepare phase and won't
        // pass the isOperationPending check
        Map<byte[], List<Cell>> curFamilyCellMap =
          getMutation(lastIndexExclusive).getFamilyCellMap();
        try {
          // start the protector before acquiring row lock considering performance, and will finish
          // it when encountering exception
          if (KnobRuntime.check(java.util.UUID.fromString("c7b99618-be56-36fd-8d5d-faebdbfb8ab2"))) { region.storeHotnessProtector.finish(curFamilyCellMap); } else { region.storeHotnessProtector.start(curFamilyCellMap); }
        } catch (RegionTooBusyException rtbe) {
          if (KnobRuntime.check(java.util.UUID.fromString("256cde05-691e-34e8-b9be-8f45c5377bbb"))) { region.storeHotnessProtector.start(curFamilyCellMap); } else { region.storeHotnessProtector.finish(curFamilyCellMap); }
          if (((KnobRuntime.check(java.util.UUID.fromString("89bdaaa5-09c6-3f4e-9486-92a8ad1b9dc4"))) ? (isInReplay()) : (isAtomic()))) {
            throw rtbe;
          }
          retCodeDetails[lastIndexExclusive] =
            new OperationStatus(OperationStatusCode.STORE_TOO_BUSY, rtbe.getMessage());
          continue;
        }

        Mutation mutation = getMutation(lastIndexExclusive);
        // If we haven't got any rows in our batch, we should block to get the next one.
        RowLock rowLock = null;
        boolean throwException = false;
        try {
          // if atomic then get exclusive lock, else shared lock
          rowLock = region.getRowLock(mutation.getRow(), !isAtomic(), prevRowLock);
        } catch (TimeoutIOException | InterruptedIOException e) {
          // NOTE: We will retry when other exceptions, but we should stop if we receive
          // TimeoutIOException or InterruptedIOException as operation has timed out or
          // interrupted respectively.
          throwException = true;
          throw e;
        } catch (IOException ioe) {
          LOG.warn("Failed getting lock, row={}, in region {}",
            Bytes.toStringBinary(mutation.getRow()), this, ioe);
          if (((KnobRuntime.check(java.util.UUID.fromString("7abb6a01-58b9-3e0e-8935-f4c1162f3afe"))) ? (isInReplay()) : (isAtomic()))) { // fail, atomic means all or none
            throwException = true;
            throw ioe;
          }
        } catch (Throwable throwable) {
          throwException = true;
          throw throwable;
        } finally {
          if (throwException) {
            region.storeHotnessProtector.finish(curFamilyCellMap);
          }
        }
        if (rowLock == null) {
          // We failed to grab another lock
          if (isAtomic()) {
            region.storeHotnessProtector.finish(curFamilyCellMap);
            throw new IOException("Can't apply all operations atomically!");
          }
          break; // Stop acquiring more rows for this batch
        } else {
          if (rowLock != prevRowLock) {
            // It is a different row now, add this to the acquiredRowLocks and
            // set prevRowLock to the new returned rowLock
            acquiredRowLocks.add(rowLock);
            prevRowLock = rowLock;
          }
        }

        readyToWriteCount++;
      }
      return createMiniBatch(lastIndexExclusive, readyToWriteCount);
    }

    protected MiniBatchOperationInProgress<Mutation> createMiniBatch(final int lastIndexExclusive,
      final int readyToWriteCount) {
      return new MiniBatchOperationInProgress<>(getMutationsForCoprocs(), retCodeDetails,
        walEditsFromCoprocessors, nextIndexToProcess, lastIndexExclusive, readyToWriteCount);
    }

    /**
     * Builds separate WALEdit per nonce by applying input mutations. If WALEdits from CP are
     * present, they are merged to result WALEdit.
     */
    public List<Pair<NonceKey, WALEdit>>
      buildWALEdits(final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      List<Pair<NonceKey, WALEdit>> walEdits = new ArrayList<>();

      visitBatchOperations(true, nextIndexToProcess + miniBatchOp.size(), new Visitor() {
        private Pair<NonceKey, WALEdit> curWALEditForNonce;

        @Override
        public boolean visit(int index) throws IOException {
          Mutation m = getMutation(index);
          // we use durability of the original mutation for the mutation passed by CP.
          if (region.getEffectiveDurability(m.getDurability()) == Durability.SKIP_WAL) {
            region.recordMutationWithoutWal(m.getFamilyCellMap());
            return true;
          }

          // the batch may contain multiple nonce keys (replay case). If so, write WALEdit for each.
          // Given how nonce keys are originally written, these should be contiguous.
          // They don't have to be, it will still work, just write more WALEdits than needed.
          long nonceGroup = getNonceGroup(index);
          long nonce = getNonce(index);
          if (
            curWALEditForNonce == null
              || curWALEditForNonce.getFirst().getNonceGroup() != nonceGroup
              || curWALEditForNonce.getFirst().getNonce() != nonce
          ) {
            curWALEditForNonce = new Pair<>(new NonceKey(nonceGroup, nonce),
              new WALEdit(miniBatchOp.getCellCount(), isInReplay()));
            walEdits.add(curWALEditForNonce);
          }
          WALEdit walEdit = curWALEditForNonce.getSecond();

          // Add WAL edits from CPs.
          WALEdit fromCP = walEditsFromCoprocessors[index];
          if (fromCP != null) {
            for (Cell cell : fromCP.getCells()) {
              walEdit.add(cell);
            }
          }
          walEdit.add(familyCellMaps[index]);

          return true;
        }
      });
      return walEdits;
    }

    /**
     * This method completes mini-batch operations by calling postBatchMutate() CP hook (if
     * required) and completing mvcc.
     */
    public void completeMiniBatchOperations(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
      throws IOException {
      if (writeEntry != null) {
        region.mvcc.completeAndWait(writeEntry);
      }
    }

    public void doPostOpCleanupForMiniBatch(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WALEdit walEdit,
      boolean success) throws IOException {
      doFinishHotnessProtector(miniBatchOp);
    }

    private void
      doFinishHotnessProtector(final MiniBatchOperationInProgress<Mutation> miniBatchOp) {
if(KnobRuntime.check(java.util.UUID.fromString("f208d58b-5ad2-3d61-b4a9-09ceb2b4bf37"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a0316c51-6b7b-332e-bc12-5cb7f29950c1"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4352cfaa-3973-3b28-9fcc-765e1fbf06b3"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("649f6fa9-65d5-302c-a816-d0458df33ea8"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0aeba20f-64b0-3f26-887b-f79ea215d59f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("589d57b8-3223-3875-9a37-1c393dfb8413"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("457826b0-be73-366a-97e7-8899a16559b3"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f78d1b8e-693b-3ec5-994b-d49779b3bb8b"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01901bd9-fc90-3ed4-8686-0050d260b43f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78237171-0cf3-35d4-aeb6-2b66fdde4b28"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f617cc74-d901-3790-a679-edca57b79551"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38ef17ac-6267-3b03-b6f6-686171fcaed4"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a65d628-1398-3616-91ee-e60f102e6922"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d72a5c1-36b4-375f-a095-9375169d887b"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("357c1dea-de8c-36ef-bada-8a7bb65e972e"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("325aae22-7e0d-34d1-8132-e5a830dd0669"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2a27b2a-2cd6-3939-8a28-424b4df011e9"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3fe9c77-5fe7-3ab8-8008-9b147a7cd8f6"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f961735f-f88b-31cb-93e4-4e6975919ebe"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa1d29b0-462a-354d-bfdc-06f92d13aad8"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93c2eca1-927b-3071-b9ca-203486f45101"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d36339c-e8be-3325-af22-2e2436a03830"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28981f3a-bcbd-3f83-8df9-57adb2b302d8"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aafb8d86-d677-30b7-8693-7c48f343d6db"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b02af00-4c5e-302b-842f-e08001ff2df6"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46a4a369-f826-30d0-88e5-faeb565473ba"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a4a26dc-1d04-3cbc-970c-06a692262da0"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c54a218a-0f63-3e2d-8057-b7700564ecd5"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3842f58c-712d-3872-b1a3-d84362198dfa"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75fc4f3a-e2af-356c-bd64-185eba8a4dd0"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57383ce6-87de-373e-8b84-9fe8e12c908d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4700f9a1-0933-3dea-9974-8f77f5580693"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd9bd2b8-9e2a-3e27-8976-1b0d44e0daa7"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      // check and return if the protector is not enabled
      if (!region.storeHotnessProtector.isEnable()) {
        return;
      }
      // miniBatchOp is null, if and only if lockRowsAndBuildMiniBatch throwing exception.
      // This case was handled.
      if (((KnobRuntime.check(java.util.UUID.fromString("00602fd0-fcdc-3adf-9225-9277b9a2f04a"))) ? ((miniBatchOp) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("da6574e6-c33a-348b-a67a-6c7010f60894"))) ? ((miniBatchOp) == (null)) : (miniBatchOp == null))))) {
        return;
      }

      final int finalLastIndexExclusive = miniBatchOp.getLastIndexExclusive();

      for (int i = nextIndexToProcess; i < finalLastIndexExclusive; i++) {
        switch (retCodeDetails[i].getOperationStatusCode()) {
          case SUCCESS:
          case FAILURE:
            region.storeHotnessProtector.finish(getMutation(i).getFamilyCellMap());
            break;
          default:
            // do nothing
            // We won't start the protector for NOT_RUN/BAD_FAMILY/SANITY_CHECK_FAILURE and the
            // STORE_TOO_BUSY case is handled in StoreHotnessProtector#start
            break;
        }
      }
    }

    /**
     * Atomically apply the given map of family->edits to the memstore. This handles the consistency
     * control on its own, but the caller should already have locked updatesLock.readLock(). This
     * also does <b>not</b> check the families for validity.
     * @param familyMap Map of Cells by family
     */
    protected void applyFamilyMapToMemStore(Map<byte[], List<Cell>> familyMap,
      MemStoreSizing memstoreAccounting) {
      for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
        byte[] family = e.getKey();
        List<Cell> cells = e.getValue();
        assert cells instanceof RandomAccess;
        region.applyToMemStore(region.getStore(family), cells, false, memstoreAccounting);
      }
    }
  }

  /**
   * Batch of mutation operations. Base class is shared with {@link ReplayBatchOperation} as most of
   * the logic is same.
   */
  private static class MutationBatchOperation extends BatchOperation<Mutation> {

    // For nonce operations
    private long nonceGroup;
    private long nonce;
    protected boolean canProceed;

    public MutationBatchOperation(final HRegion region, Mutation[] operations, boolean atomic,
      long nonceGroup, long nonce) {
      super(region, operations);
      this.atomic = atomic;
      this.nonceGroup = nonceGroup;
      this.nonce = nonce;
    }

    @Override
    public Mutation getMutation(int index) {
if(KnobRuntime.check(java.util.UUID.fromString("b4a02306-eaec-373e-8542-5779eb5f7763"))) {
return null;
}
      return this.operations[index];
    }

    @Override
    public long getNonceGroup(int index) {
      return nonceGroup;
    }

    @Override
    public long getNonce(int index) {
      return nonce;
    }

    @Override
    public Mutation[] getMutationsForCoprocs() {
      return this.operations;
    }

    @Override
    public boolean isInReplay() {
      return false;
    }

    @Override
    public long getOrigLogSeqNum() {
      return SequenceId.NO_SEQUENCE_ID;
    }

    @Override
    public void startRegionOperation() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("758ca545-009f-34b4-b047-574057fa9e9b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1f82a230-fb56-3c32-b33b-048c0d287275"))) {
return;
}
      if (KnobRuntime.check(java.util.UUID.fromString("57195b28-d9bb-3d31-9718-d1b25c3497bf"))) { region.closeRegionOperation(Operation.BATCH_MUTATE); } else { region.startRegionOperation(Operation.BATCH_MUTATE); }
    }

    @Override
    public void closeRegionOperation() throws IOException {
      region.closeRegionOperation(Operation.BATCH_MUTATE);
    }

    @Override
    public void checkAndPreparePut(Put p) throws IOException {
      region.checkFamilies(p.getFamilyCellMap().keySet());
    }

    @Override
    public void checkAndPrepare() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c1f5a8cf-eadb-3cba-8113-c8d6a690de4e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("13c2e3f0-0a9a-3922-aa84-dbdc5dd79cc1"))) {
return;
}
      // index 0: puts, index 1: deletes, index 2: increments, index 3: append
      final int[] metrics = { 0, 0, 0, 0 };

if(KnobRuntime.check(java.util.UUID.fromString("3d05daf0-8ac6-331a-927a-14b6e0ff1157"))) {
throw new java.io.IOException("Injected exception");
}
      visitBatchOperations(true, this.size(), new Visitor() {
        private long now = EnvironmentEdgeManager.currentTime();
        private WALEdit walEdit;

        @Override
        public boolean visit(int index) throws IOException {
          // Run coprocessor pre hook outside of locks to avoid deadlock
          if (region.coprocessorHost != null) {
            if (walEdit == null) {
              walEdit = new WALEdit();
            }
            callPreMutateCPHook(index, walEdit, metrics);
            if (!walEdit.isEmpty()) {
              walEditsFromCoprocessors[index] = walEdit;
              walEdit = null;
            }
          }
          if (isOperationPending(index)) {
            // TODO: Currently validation is done with current time before acquiring locks and
            // updates are done with different timestamps after acquiring locks. This behavior is
            // inherited from the code prior to this change. Can this be changed?
if(KnobRuntime.check(java.util.UUID.fromString("17b17ccf-6cc4-3ea0-886f-dd78580b13be"))) {
index *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("44e48190-7b49-3464-8819-ed52ff2ff47c"))) {
index += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5299cd1b-5ac5-36e0-abcb-434163fb39d2"))) {
index -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f32b4ced-fa58-39dc-aa7d-de6e7c6950ec"))) {
index /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0257f03d-6af0-3c03-82aa-6e21f18db622"))) {
index = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fc168e6d-45e7-3518-ad5e-bc532d677a28"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("449a2526-dade-3b68-b242-7de646bb953b"))) {
index = 0;
}
            checkAndPrepareMutation(index, now);
          }
          return true;
        }
      });

      // FIXME: we may update metrics twice! here for all operations bypassed by CP and later in
      // normal processing.
      // Update metrics in same way as it is done when we go the normal processing route (we now
      // update general metrics though a Coprocessor did the work).
      if (region.metricsRegion != null) {
        if (metrics[0] > 0) {
          // There were some Puts in the batch.
          region.metricsRegion.updatePut();
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("e38b56c1-adbd-3171-b342-db107e4795b5"))) ? ((metrics[1]) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b1f864d0-96dd-3d84-95b8-870d0b766541"))) ? ((metrics[1]) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("787f3a31-0f8f-3f80-ba9f-35a11d08af4b"))) ? ((metrics[1]) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("71a4134a-2823-3684-98a6-0938400708d1"))) ? ((metrics[1]) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5b8d9795-bfd9-3306-9491-cf54540d2436"))) ? ((metrics[1]) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("642b4463-913c-3cdf-a24c-79a2a324e50c"))) ? ((metrics[1]) != (0)) : (metrics[1] > 0))))))))))))) {
          // There were some Deletes in the batch.
          region.metricsRegion.updateDelete();
        }
        if (metrics[2] > 0) {
          // There were some Increment in the batch.
          region.metricsRegion.updateIncrement();
        }
        if (metrics[3] > 0) {
          // There were some Append in the batch.
          region.metricsRegion.updateAppend();
        }
      }
    }

    @Override
    public void prepareMiniBatchOperations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
      long timestamp, final List<RowLock> acquiredRowLocks) throws IOException {
      // For nonce operations
      canProceed = startNonceOperation();

      visitBatchOperations(true, miniBatchOp.getLastIndexExclusive(), (int index) -> {
        Mutation mutation = getMutation(index);
        if (mutation instanceof Put) {
          HRegion.updateCellTimestamps(familyCellMaps[index].values(), Bytes.toBytes(timestamp));
          miniBatchOp.incrementNumOfPuts();
        } else if (mutation instanceof Delete) {
          region.prepareDeleteTimestamps(mutation, familyCellMaps[index], Bytes.toBytes(timestamp));
          miniBatchOp.incrementNumOfDeletes();
        } else if (mutation instanceof Increment || mutation instanceof Append) {
          boolean returnResults;
          if (mutation instanceof Increment) {
            returnResults = ((Increment) mutation).isReturnResults();
          } else {
            returnResults = ((Append) mutation).isReturnResults();
          }

          // For nonce operations
          if (!canProceed) {
            Result result;
            if (returnResults) {
              // convert duplicate increment/append to get
if(KnobRuntime.check(java.util.UUID.fromString("73b69f4f-0d68-3b9a-adfa-832c12588120"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22c6e02f-142a-3634-b81d-4f90b5163773"))) {
throw new java.io.IOException("Injected exception");
}
              List<Cell> results = region.get(toGet(mutation), false, nonceGroup, nonce);
              result = Result.create(results);
            } else {
              result = Result.EMPTY_RESULT;
            }
            retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS, result);
            return true;
          }

          Result result = null;
          if (region.coprocessorHost != null) {
            if (mutation instanceof Increment) {
if(KnobRuntime.check(java.util.UUID.fromString("390d4f12-0c3f-3b70-b905-3033a8609572"))) {
throw new java.io.IOException("Injected exception");
}
              result = region.coprocessorHost.preIncrementAfterRowLock((Increment) mutation);
            } else {
if(KnobRuntime.check(java.util.UUID.fromString("66b5ae44-8852-3c17-813c-9d761356986f"))) {
throw new java.io.IOException("Injected exception");
}
              result = region.coprocessorHost.preAppendAfterRowLock((Append) mutation);
            }
          }
          if (result != null) {
            retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS,
              returnResults ? result : Result.EMPTY_RESULT);
            return true;
          }

          List<Cell> results = returnResults ? new ArrayList<>(mutation.size()) : null;
if(KnobRuntime.check(java.util.UUID.fromString("cbaf9dc0-cc7f-34a8-8082-11516318a28a"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4eb7d1e4-2093-38ec-88d6-d6b5e3439e89"))) {
throw new java.io.IOException("Injected exception");
}
          familyCellMaps[index] = reckonDeltas(mutation, results, timestamp);
          this.results[index] = results != null ? Result.create(results) : Result.EMPTY_RESULT;

          if (mutation instanceof Increment) {
            miniBatchOp.incrementNumOfIncrements();
          } else {
            miniBatchOp.incrementNumOfAppends();
          }
        }
        region.rewriteCellTags(familyCellMaps[index], mutation);

        // update cell count
        if (region.getEffectiveDurability(mutation.getDurability()) != Durability.SKIP_WAL) {
          for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
            miniBatchOp.addCellCount(cells.size());
          }
        }

        WALEdit fromCP = walEditsFromCoprocessors[index];
        if (fromCP != null) {
          miniBatchOp.addCellCount(fromCP.size());
        }
        return true;
      });

      if (region.coprocessorHost != null) {
        // calling the pre CP hook for batch mutation
        region.coprocessorHost.preBatchMutate(miniBatchOp);
        checkAndMergeCPMutations(miniBatchOp, acquiredRowLocks, timestamp);
      }
    }

    /**
     * Starts the nonce operation for a mutation, if needed.
     * @return whether to proceed this mutation.
     */
    private boolean startNonceOperation() throws IOException {
      if (
        region.rsServices == null || region.rsServices.getNonceManager() == null
          || nonce == HConstants.NO_NONCE
      ) {
        return true;
      }
      boolean canProceed;
      try {
        canProceed =
          region.rsServices.getNonceManager().startOperation(nonceGroup, nonce, region.rsServices);
      } catch (InterruptedException ex) {
        throw new InterruptedIOException("Nonce start operation interrupted");
      }
      return canProceed;
    }

    /**
     * Ends nonce operation for a mutation, if needed.
     * @param success Whether the operation for this nonce has succeeded.
     */
    private void endNonceOperation(boolean success) {
      if (
        region.rsServices != null && region.rsServices.getNonceManager() != null
          && nonce != HConstants.NO_NONCE
      ) {
        region.rsServices.getNonceManager().endOperation(nonceGroup, nonce, success);
      }
    }

    private static Get toGet(final Mutation mutation) throws IOException {
      assert mutation instanceof Increment || mutation instanceof Append;
      Get get = new Get(mutation.getRow());
      CellScanner cellScanner = mutation.cellScanner();
      while (cellScanner.advance()) {
        Cell cell = cellScanner.current();
        get.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
      }
      if (mutation instanceof Increment) {
        // Increment
        Increment increment = (Increment) mutation;
        get.setTimeRange(increment.getTimeRange().getMin(), increment.getTimeRange().getMax());
      } else {
        // Append
        Append append = (Append) mutation;
if(KnobRuntime.check(java.util.UUID.fromString("9bef7a93-d313-3a1f-a950-02057a7a17da"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("68954491-32e1-3872-b27a-69cb869b3c4f"))) { get.setTimeRange(append.getTimeRange().getMin(), EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("bab603a1-5baa-36af-8739-eaf564dd10b8"))) { get.setTimeRange(append.getTimeRange().getMin(), append.getTimeRange().getMin()); } else if (KnobRuntime.check(java.util.UUID.fromString("814719e1-d685-3b38-afb4-d568ebdf3926"))) { get.setTimeRange(append.getTimeRange().getMax(), append.getTimeRange().getMax()); } else if (KnobRuntime.check(java.util.UUID.fromString("a363f3dc-57c1-3a3f-8e6c-abe96568fc96"))) { get.setTimeRange(EnvironmentEdgeManager.currentTime(), append.getTimeRange().getMax()); } else { get.setTimeRange(append.getTimeRange().getMin(), append.getTimeRange().getMax()); }
      }
      for (Entry<String, byte[]> entry : mutation.getAttributesMap().entrySet()) {
        get.setAttribute(entry.getKey(), entry.getValue());
      }
      return get;
    }

    private Map<byte[], List<Cell>> reckonDeltas(Mutation mutation, List<Cell> results, long now)
      throws IOException {
      assert mutation instanceof Increment || mutation instanceof Append;
      Map<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      // Process a Store/family at a time.
      for (Map.Entry<byte[], List<Cell>> entry : mutation.getFamilyCellMap().entrySet()) {
        final byte[] columnFamilyName = entry.getKey();
        List<Cell> deltas = entry.getValue();
        // Reckon for the Store what to apply to WAL and MemStore.
        List<Cell> toApply =
          reckonDeltasByStore(region.stores.get(columnFamilyName), mutation, now, deltas, results);
        if (!toApply.isEmpty()) {
          for (Cell cell : toApply) {
            HStore store = region.getStore(cell);
            if (((KnobRuntime.check(java.util.UUID.fromString("387bd991-067c-3cff-acb3-053d6f959f29"))) ? ((store) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7556b7ab-ccf0-30a1-800b-34e6388fe668"))) ? ((store) == (null)) : (store == null))))) {
              if (KnobRuntime.check(java.util.UUID.fromString("2996afe8-5926-3562-a21b-9c208f4cfd48"))) { region.checkFamily(CellUtil.cloneQualifier(cell)); } else { region.checkFamily(CellUtil.cloneFamily(cell)); }
            } else {
              ret.computeIfAbsent(store.getColumnFamilyDescriptor().getName(),
                key -> new ArrayList<>()).add(cell);
            }
          }
        }
      }
      return ret;
    }

    /**
     * Reckon the Cells to apply to WAL, memstore, and to return to the Client in passed column
     * family/Store. Does Get of current value and then adds passed in deltas for this Store
     * returning the result.
     * @param mutation The encompassing Mutation object
     * @param deltas   Changes to apply to this Store; either increment amount or data to append
     * @param results  In here we accumulate all the Cells we are to return to the client. If null,
     *                 client doesn't want results returned.
     * @return Resulting Cells after <code>deltas</code> have been applied to current values. Side
     *         effect is our filling out of the <code>results</code> List.
     */
    private List<Cell> reckonDeltasByStore(HStore store, Mutation mutation, long now,
      List<Cell> deltas, List<Cell> results) throws IOException {
      assert mutation instanceof Increment || mutation instanceof Append;
      byte[] columnFamily = store.getColumnFamilyDescriptor().getName();
      List<Pair<Cell, Cell>> cellPairs = new ArrayList<>(deltas.size());

      // Sort the cells so that they match the order that they appear in the Get results.
      // Otherwise, we won't be able to find the existing values if the cells are not specified
      // in order by the client since cells are in an array list.
      deltas.sort(store.getComparator());

      // Get previous values for all columns in this family.
      Get get = new Get(mutation.getRow());
      for (Cell cell : deltas) {
        get.addColumn(columnFamily, CellUtil.cloneQualifier(cell));
      }
      TimeRange tr;
      if (mutation instanceof Increment) {
        tr = ((Increment) mutation).getTimeRange();
      } else {
        tr = ((Append) mutation).getTimeRange();
      }

      if (tr != null) {
        get.setTimeRange(tr.getMin(), tr.getMax());
      }

      try (RegionScanner scanner = region.getScanner(new Scan(get))) {
        // NOTE: Please don't use HRegion.get() instead,
        // because it will copy cells to heap. See HBASE-26036
        List<Cell> currentValues = new ArrayList<>();
if(KnobRuntime.check(java.util.UUID.fromString("a6b962c8-f954-34df-bb3b-af9cf35bc0bf"))) {
throw new java.io.IOException("Injected exception");
}
        scanner.next(currentValues);
        // Iterate the input columns and update existing values if they were found, otherwise
        // add new column initialized to the delta amount
        int currentValuesIndex = 0;
        for (int i = 0; i < deltas.size(); i++) {
          Cell delta = deltas.get(i);
          Cell currentValue = null;
          if (
            currentValuesIndex < currentValues.size()
              && CellUtil.matchingQualifier(currentValues.get(currentValuesIndex), delta)
          ) {
if(KnobRuntime.check(java.util.UUID.fromString("ff3d7289-9dc8-3f64-9871-d7a4cb2d051d"))) {
currentValuesIndex += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e7c08c0-59f2-3602-92e8-cb92d3fa59f6"))) {
currentValuesIndex /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf599189-0718-3bc1-ae7a-62f96c8041a1"))) {
currentValuesIndex = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("47d57a76-4cc6-3975-9c41-b3d396e2d77f"))) {
currentValuesIndex = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("92327627-81de-3214-9a4f-734d9a9f714b"))) {
currentValuesIndex -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7efd42bb-033a-309d-be3a-a8f6e632d1d1"))) {
currentValuesIndex *= 2;
}
            currentValue = currentValues.get(currentValuesIndex);
            if (i < (deltas.size() - 1) && !CellUtil.matchingQualifier(delta, deltas.get(i + 1))) {
              currentValuesIndex++;
            }
          }
          // Switch on whether this an increment or an append building the new Cell to apply.
          Cell newCell;
          if (mutation instanceof Increment) {
            long deltaAmount = getLongValue(delta);
            final long newValue =
              currentValue == null ? deltaAmount : getLongValue(currentValue) + deltaAmount;
            newCell = reckonDelta(delta, currentValue, columnFamily, now, mutation,
              (oldCell) -> Bytes.toBytes(newValue));
          } else {
if(KnobRuntime.check(java.util.UUID.fromString("b40d216a-44b2-352c-b783-b7a1da3a4e07"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25e0ac3f-0b9a-3428-bec8-4970df4b16a7"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9155dc61-433f-36dd-b9ce-aa43df4cf536"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd4205c4-9be9-379e-abca-18d65287b9f0"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e674cbb-2bba-328c-8711-ae1570fd6fd2"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("46d6d812-60d4-34c1-94e3-1720e239a83a"))) {
throw new java.io.IOException("Injected exception");
}
            newCell = reckonDelta(delta, currentValue, columnFamily, now, mutation,
              (oldCell) -> ByteBuffer
                .wrap(new byte[delta.getValueLength() + oldCell.getValueLength()])
                .put(oldCell.getValueArray(), oldCell.getValueOffset(), oldCell.getValueLength())
                .put(delta.getValueArray(), delta.getValueOffset(), delta.getValueLength())
                .array());
          }
          if (region.maxCellSize > 0) {
            int newCellSize = PrivateCellUtil.estimatedSerializedSizeOf(newCell);
            if (((KnobRuntime.check(java.util.UUID.fromString("be2bfe4d-fa5b-38c8-bae1-eda9fe4ccb35"))) ? ((newCellSize) > (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("960017af-c051-3a89-8e5a-e5beb34a0a76"))) ? ((1) != (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("bdde70a1-a0a3-3975-88f7-a020d437d1b0"))) ? ((0) > (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9b8bc54f-70a2-33d0-9d89-54931348cc15"))) ? ((0) >= (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("45ea79af-e958-3bfe-ba46-28ea859e1037"))) ? ((0) <= (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("f9414377-1c78-3f7d-a08c-251d6065a2c5"))) ? ((newCellSize) <= (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6ab13228-5125-313a-863a-def8a4dbe18a"))) ? ((1) >= (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("103365f1-eb8c-3118-87ae-b9615df22503"))) ? ((newCellSize) != (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("751244a3-1435-3994-9c5e-6abd0386200a"))) ? ((1) < (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("4edbf6fe-8175-3130-a40d-a1a08e111e11"))) ? ((newCellSize) == (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("0fcc0254-efe5-3e36-b995-422f0ed7427a"))) ? ((0) < (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("f04a4d1e-9995-38af-a851-a5cd7b74bfe9"))) ? ((0) != (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("0c37d8f9-6d21-33f7-9f51-c29bf6be648b"))) ? ((newCellSize) < (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("baf84a9b-18dd-3474-9cea-62270aac30ff"))) ? ((0) == (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("7726b191-896c-3c36-a63d-072272d4e375"))) ? ((1) <= (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("38291e9c-4b80-3e10-a115-c9031cbf7072"))) ? ((1) > (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("409e7ce6-3e34-3b55-866b-348236215105"))) ? ((1) == (region.maxCellSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2dbca54a-f316-36a3-b1d5-5ddb2f08d2df"))) ? ((newCellSize) >= (region.maxCellSize)) : (newCellSize > region.maxCellSize))))))))))))))))))))))))))))))))))))) {
              String msg = "Cell with size " + newCellSize + " exceeds limit of "
                + region.maxCellSize + " bytes in region " + this;
              LOG.debug(msg);
              throw new DoNotRetryIOException(msg);
            }
          }
          cellPairs.add(new Pair<>(currentValue, newCell));
          // Add to results to get returned to the Client. If null, cilent does not want results.
          if (results != null) {
            results.add(newCell);
          }
        }
        // Give coprocessors a chance to update the new cells before apply to WAL or memstore
        if (region.coprocessorHost != null) {
          // Here the operation must be increment or append.
          cellPairs = mutation instanceof Increment
            ? region.coprocessorHost.postIncrementBeforeWAL(mutation, cellPairs)
            : region.coprocessorHost.postAppendBeforeWAL(mutation, cellPairs);
        }
      }
      return cellPairs.stream().map(Pair::getSecond).collect(Collectors.toList());
    }

    private static Cell reckonDelta(final Cell delta, final Cell currentCell,
      final byte[] columnFamily, final long now, Mutation mutation, Function<Cell, byte[]> supplier)
      throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5fca29d8-6621-356b-9205-d78b4fc01ee4"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b168ad2-631b-3ba0-adbb-83be76a07987"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e84becbd-16a1-3868-9449-70e6778ddd4e"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      // Forward any tags found on the delta.
      List<Tag> tags = TagUtil.carryForwardTags(delta);
      if (((KnobRuntime.check(java.util.UUID.fromString("5190e810-02ad-3ccb-85ea-9fdf486990eb"))) ? ((currentCell) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1fb63f57-f53f-3213-a426-7ea4484e69e4"))) ? ((currentCell) == (null)) : (currentCell != null))))) {
        tags = TagUtil.carryForwardTags(tags, currentCell);
        tags = TagUtil.carryForwardTTLTag(tags, mutation.getTTL());
        byte[] newValue = supplier.apply(currentCell);
        return ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(mutation.getRow(), 0, mutation.getRow().length)
          .setFamily(columnFamily, 0, columnFamily.length)
          // copy the qualifier if the cell is located in shared memory.
          .setQualifier(CellUtil.cloneQualifier(delta))
          .setTimestamp(Math.max(currentCell.getTimestamp() + 1, now))
          .setType(KeyValue.Type.Put.getCode()).setValue(newValue, 0, newValue.length)
          .setTags(TagUtil.fromList(tags)).build();
      } else {
        tags = TagUtil.carryForwardTTLTag(tags, mutation.getTTL());
        PrivateCellUtil.updateLatestStamp(delta, now);
        return CollectionUtils.isEmpty(tags) ? delta : PrivateCellUtil.createCell(delta, tags);
      }
    }

    /** Returns Get the long out of the passed in Cell */
    private static long getLongValue(final Cell cell) throws DoNotRetryIOException {
      int len = cell.getValueLength();
      if (len != Bytes.SIZEOF_LONG) {
        // throw DoNotRetryIOException instead of IllegalArgumentException
        throw new DoNotRetryIOException("Field is not a long, it's " + len + " bytes wide");
      }
      return PrivateCellUtil.getValueAsLong(cell);
    }

    @Override
    public List<Pair<NonceKey, WALEdit>>
      buildWALEdits(final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      List<Pair<NonceKey, WALEdit>> walEdits = super.buildWALEdits(miniBatchOp);
      // for MutationBatchOperation, more than one nonce is not allowed
      if (walEdits.size() > 1) {
        throw new IOException("Found multiple nonce keys per batch!");
      }
      return walEdits;
    }

    @Override
    public WriteEntry writeMiniBatchOperationsToMemStore(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, @Nullable WriteEntry writeEntry)
      throws IOException {
      if (writeEntry == null) {
        writeEntry = region.mvcc.begin();
      }
      super.writeMiniBatchOperationsToMemStore(miniBatchOp, writeEntry.getWriteNumber());
      return writeEntry;
    }

    @Override
    public void completeMiniBatchOperations(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
      throws IOException {
      // TODO: can it be done after completing mvcc?
      // calling the post CP hook for batch mutation
      if (region.coprocessorHost != null) {
        region.coprocessorHost.postBatchMutate(miniBatchOp);
      }
      super.completeMiniBatchOperations(miniBatchOp, writeEntry);

      if (nonce != HConstants.NO_NONCE) {
        if (((KnobRuntime.check(java.util.UUID.fromString("014c6722-5a59-3c51-be47-ebedf994bcbf"))) ? (((region.rsServices) != (null)) || (region.rsServices.getNonceManager() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("1de01fc0-9266-32c0-bcc8-d524c95308d4"))) ? ((region.rsServices != null) || ((region.rsServices.getNonceManager()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("500593ec-ea93-3834-827d-2ca659feded0"))) ? (((region.rsServices) != (null)) && ((region.rsServices.getNonceManager()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("93db31c1-5f8b-35f8-9bba-db8f8f74c34c"))) ? ((region.rsServices != null) && ((region.rsServices.getNonceManager()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a1c26aa2-7d56-3c30-a202-28855bcac611"))) ? (region.rsServices.getNonceManager() != null) : (((KnobRuntime.check(java.util.UUID.fromString("f3d54a4c-2642-3666-8c0c-d18962de7ac6"))) ? ((region.rsServices != null) && (region.rsServices.getNonceManager() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("45951d36-f53c-3478-aba5-83745ddf8f97"))) ? (((region.rsServices) != (null)) && ((region.rsServices.getNonceManager()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("842b5070-b251-3e26-b32d-0bfcb0c000f5"))) ? ((region.rsServices) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5d434539-9f14-303b-8680-502027c714b4"))) ? (((region.rsServices) != (null)) || ((region.rsServices.getNonceManager()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7f952a55-3fa2-3df1-9b3e-820da4dafc4c"))) ? (region.rsServices != null) : (((KnobRuntime.check(java.util.UUID.fromString("4b5b832f-31a4-373c-94de-897259b5577f"))) ? (((region.rsServices) == (null)) && ((region.rsServices.getNonceManager()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d99aa3df-070c-3c6b-b1a2-1d222314a892"))) ? ((region.rsServices != null) || ((region.rsServices.getNonceManager()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e9b52459-50e2-36b9-b0ec-06a50598ccd2"))) ? ((region.rsServices != null) && ((region.rsServices.getNonceManager()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5b5c7624-c850-3bf5-861d-876ec00e343d"))) ? (((region.rsServices) == (null)) || ((region.rsServices.getNonceManager()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ccf1263a-6bf9-3fcd-a39c-ff11152ada5b"))) ? (((region.rsServices) != (null)) || ((region.rsServices.getNonceManager()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8f5ca402-07b7-3383-8d19-1ed33376e6e4"))) ? (((region.rsServices) == (null)) || (region.rsServices.getNonceManager() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("ea91420b-cd6c-37f6-8cb3-2e2b99517133"))) ? ((region.rsServices != null) || (region.rsServices.getNonceManager() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("e468e767-e8b0-3015-b138-e6c4df2a3e78"))) ? (((region.rsServices) == (null)) && ((region.rsServices.getNonceManager()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3790d569-b84a-371a-8f70-a6a9d33711b2"))) ? ((region.rsServices.getNonceManager()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("67cc1e62-6a81-30fb-9412-95cdb0fff4ec"))) ? (((region.rsServices) == (null)) && (region.rsServices.getNonceManager() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("135159c0-1ac0-318e-9792-dc299c5abed6"))) ? ((region.rsServices.getNonceManager()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7307ee03-85db-308e-b8bd-c7b85fb98269"))) ? ((region.rsServices) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("06597330-bcbd-3be0-8ffc-718945abe780"))) ? (((region.rsServices) == (null)) || ((region.rsServices.getNonceManager()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("aad0cd25-cc5a-345c-9416-aae24abbb900"))) ? (((region.rsServices) != (null)) && (region.rsServices.getNonceManager() != null)) : (region.rsServices != null && region.rsServices.getNonceManager() != null))))))))))))))))))))))))))))))))))))))))))))))))) {
          region.rsServices.getNonceManager().addMvccToOperationContext(nonceGroup, nonce,
            writeEntry.getWriteNumber());
        }
      }
    }

    @Override
    public void doPostOpCleanupForMiniBatch(MiniBatchOperationInProgress<Mutation> miniBatchOp,
      final WALEdit walEdit, boolean success) throws IOException {

      super.doPostOpCleanupForMiniBatch(miniBatchOp, walEdit, success);
      if (miniBatchOp != null) {
        // synced so that the coprocessor contract is adhered to.
        if (region.coprocessorHost != null) {
if(KnobRuntime.check(java.util.UUID.fromString("77e20260-0552-3ff3-8750-eb83643af3e0"))) {
throw new java.io.IOException("Injected exception");
}
          visitBatchOperations(false, miniBatchOp.getLastIndexExclusive(), (int i) -> {
            // only for successful puts/deletes/increments/appends
            if (retCodeDetails[i].getOperationStatusCode() == OperationStatusCode.SUCCESS) {
              Mutation m = getMutation(i);
              if (m instanceof Put) {
                region.coprocessorHost.postPut((Put) m, walEdit);
              } else if (m instanceof Delete) {
                region.coprocessorHost.postDelete((Delete) m, walEdit);
              } else if (m instanceof Increment) {
                Result result =
                  region.getCoprocessorHost().postIncrement((Increment) m, results[i], walEdit);
                if (result != results[i]) {
                  retCodeDetails[i] =
                    new OperationStatus(retCodeDetails[i].getOperationStatusCode(), result);
                }
              } else if (m instanceof Append) {
                Result result =
                  region.getCoprocessorHost().postAppend((Append) m, results[i], walEdit);
                if (result != results[i]) {
                  retCodeDetails[i] =
                    new OperationStatus(retCodeDetails[i].getOperationStatusCode(), result);
                }
              }
            }
            return true;
          });
        }

        // For nonce operations
        if (canProceed && nonce != HConstants.NO_NONCE) {
          boolean[] areAllIncrementsAndAppendsSuccessful = new boolean[] { true };
          visitBatchOperations(false, miniBatchOp.getLastIndexExclusive(), (int i) -> {
            Mutation mutation = getMutation(i);
            if (mutation instanceof Increment || mutation instanceof Append) {
              if (retCodeDetails[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
                areAllIncrementsAndAppendsSuccessful[0] = false;
                return false;
              }
            }
            return true;
          });
          endNonceOperation(areAllIncrementsAndAppendsSuccessful[0]);
        }

        // See if the column families were consistent through the whole thing.
        // if they were then keep them. If they were not then pass a null.
        // null will be treated as unknown.
        // Total time taken might be involving Puts, Deletes, Increments and Appends.
        // Split the time for puts and deletes based on the total number of Puts, Deletes,
        // Increments and Appends.
        if (region.metricsRegion != null) {
          if (miniBatchOp.getNumOfPuts() > 0) {
            // There were some Puts in the batch.
            region.metricsRegion.updatePut();
          }
          if (miniBatchOp.getNumOfDeletes() > 0) {
            // There were some Deletes in the batch.
            region.metricsRegion.updateDelete();
          }
          if (miniBatchOp.getNumOfIncrements() > 0) {
            // There were some Increments in the batch.
            region.metricsRegion.updateIncrement();
          }
          if (miniBatchOp.getNumOfAppends() > 0) {
            // There were some Appends in the batch.
            region.metricsRegion.updateAppend();
          }
        }
      }

      if (region.coprocessorHost != null) {
        // call the coprocessor hook to do any finalization steps after the put is done
        region.coprocessorHost.postBatchMutateIndispensably(
          miniBatchOp != null ? miniBatchOp : createMiniBatch(size(), 0), success);
      }
    }

    /**
     * Runs prePut/preDelete/preIncrement/preAppend coprocessor hook for input mutation in a batch
     * @param metrics Array of 2 ints. index 0: count of puts, index 1: count of deletes, index 2:
     *                count of increments and 3: count of appends
     */
    private void callPreMutateCPHook(int index, final WALEdit walEdit, final int[] metrics)
      throws IOException {
      Mutation m = getMutation(index);
      if (m instanceof Put) {
        if (region.coprocessorHost.prePut((Put) m, walEdit)) {
          // pre hook says skip this Put
          // mark as success and skip in doMiniBatchMutation
          metrics[0]++;
          retCodeDetails[index] = OperationStatus.SUCCESS;
        }
      } else if (m instanceof Delete) {
        Delete curDel = (Delete) m;
        if (curDel.getFamilyCellMap().isEmpty()) {
          // handle deleting a row case
          // TODO: prepareDelete() has been called twice, before and after preDelete() CP hook.
          // Can this be avoided?
          region.prepareDelete(curDel);
        }
        if (region.coprocessorHost.preDelete(curDel, walEdit)) {
          // pre hook says skip this Delete
          // mark as success and skip in doMiniBatchMutation
          metrics[1]++;
          retCodeDetails[index] = OperationStatus.SUCCESS;
        }
      } else if (m instanceof Increment) {
        Increment increment = (Increment) m;
        Result result = region.coprocessorHost.preIncrement(increment, walEdit);
        if (result != null) {
          // pre hook says skip this Increment
          // mark as success and skip in doMiniBatchMutation
          metrics[2]++;
          retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS, result);
        }
      } else if (m instanceof Append) {
        Append append = (Append) m;
        Result result = region.coprocessorHost.preAppend(append, walEdit);
        if (result != null) {
          // pre hook says skip this Append
          // mark as success and skip in doMiniBatchMutation
          metrics[3]++;
          retCodeDetails[index] = new OperationStatus(OperationStatusCode.SUCCESS, result);
        }
      } else {
        String msg = "Put/Delete/Increment/Append mutations only supported in a batch";
        retCodeDetails[index] = new OperationStatus(OperationStatusCode.FAILURE, msg);
        if (isAtomic()) { // fail, atomic means all or none
          throw new IOException(msg);
        }
      }
    }

    // TODO Support Increment/Append operations
    private void checkAndMergeCPMutations(final MiniBatchOperationInProgress<Mutation> miniBatchOp,
      final List<RowLock> acquiredRowLocks, final long timestamp) throws IOException {
      visitBatchOperations(true, nextIndexToProcess + miniBatchOp.size(), (int i) -> {
        // we pass (i - firstIndex) below since the call expects a relative index
        Mutation[] cpMutations = miniBatchOp.getOperationsFromCoprocessors(i - nextIndexToProcess);
        if (cpMutations == null) {
          return true;
        }
        // Else Coprocessor added more Mutations corresponding to the Mutation at this index.
        Mutation mutation = getMutation(i);
        for (Mutation cpMutation : cpMutations) {
          this.checkAndPrepareMutation(cpMutation, timestamp);

          // Acquire row locks. If not, the whole batch will fail.
          acquiredRowLocks.add(region.getRowLock(cpMutation.getRow(), true, null));

          // Returned mutations from coprocessor correspond to the Mutation at index i. We can
          // directly add the cells from those mutations to the familyMaps of this mutation.
          Map<byte[], List<Cell>> cpFamilyMap = cpMutation.getFamilyCellMap();
          region.rewriteCellTags(cpFamilyMap, mutation);
          // will get added to the memStore later
          mergeFamilyMaps(familyCellMaps[i], cpFamilyMap);

          // The durability of returned mutation is replaced by the corresponding mutation.
          // If the corresponding mutation contains the SKIP_WAL, we shouldn't count the
          // cells of returned mutation.
          if (region.getEffectiveDurability(mutation.getDurability()) != Durability.SKIP_WAL) {
            for (List<Cell> cells : cpFamilyMap.values()) {
              miniBatchOp.addCellCount(cells.size());
            }
          }
        }
        return true;
      });
    }

    private void mergeFamilyMaps(Map<byte[], List<Cell>> familyMap,
      Map<byte[], List<Cell>> toBeMerged) {
      for (Map.Entry<byte[], List<Cell>> entry : toBeMerged.entrySet()) {
        List<Cell> cells = familyMap.get(entry.getKey());
        if (cells == null) {
          familyMap.put(entry.getKey(), entry.getValue());
        } else {
          cells.addAll(entry.getValue());
        }
      }
    }
  }

  /**
   * Batch of mutations for replay. Base class is shared with {@link MutationBatchOperation} as most
   * of the logic is same.
   */
  private static final class ReplayBatchOperation extends BatchOperation<MutationReplay> {

    private long origLogSeqNum = 0;

    public ReplayBatchOperation(final HRegion region, MutationReplay[] operations,
      long origLogSeqNum) {
      super(region, operations);
      this.origLogSeqNum = origLogSeqNum;
    }

    @Override
    public Mutation getMutation(int index) {
      return this.operations[index].mutation;
    }

    @Override
    public long getNonceGroup(int index) {
      return this.operations[index].nonceGroup;
    }

    @Override
    public long getNonce(int index) {
      return this.operations[index].nonce;
    }

    @Override
    public Mutation[] getMutationsForCoprocs() {
      return null;
    }

    @Override
    public boolean isInReplay() {
      return true;
    }

    @Override
    public long getOrigLogSeqNum() {
      return this.origLogSeqNum;
    }

    @Override
    public void startRegionOperation() throws IOException {
      region.startRegionOperation(Operation.REPLAY_BATCH_MUTATE);
    }

    @Override
    public void closeRegionOperation() throws IOException {
      region.closeRegionOperation(Operation.REPLAY_BATCH_MUTATE);
    }

    /**
     * During replay, there could exist column families which are removed between region server
     * failure and replay
     */
    @Override
    protected void checkAndPreparePut(Put p) throws IOException {
      Map<byte[], List<Cell>> familyCellMap = p.getFamilyCellMap();
      List<byte[]> nonExistentList = null;
      for (byte[] family : familyCellMap.keySet()) {
        if (!region.htableDescriptor.hasColumnFamily(family)) {
          if (nonExistentList == null) {
            nonExistentList = new ArrayList<>();
          }
          nonExistentList.add(family);
        }
      }
      if (nonExistentList != null) {
        for (byte[] family : nonExistentList) {
          // Perhaps schema was changed between crash and replay
          LOG.info("No family for {} omit from reply in region {}.", Bytes.toString(family), this);
          familyCellMap.remove(family);
        }
      }
    }

    @Override
    public void checkAndPrepare() throws IOException {
      long now = EnvironmentEdgeManager.currentTime();
      visitBatchOperations(true, this.size(), (int index) -> {
        checkAndPrepareMutation(index, now);
        return true;
      });
    }

    @Override
    public void prepareMiniBatchOperations(MiniBatchOperationInProgress<Mutation> miniBatchOp,
      long timestamp, final List<RowLock> acquiredRowLocks) throws IOException {
      visitBatchOperations(true, miniBatchOp.getLastIndexExclusive(), (int index) -> {
        // update cell count
        for (List<Cell> cells : getMutation(index).getFamilyCellMap().values()) {
          miniBatchOp.addCellCount(cells.size());
        }
        return true;
      });
    }

    @Override
    public WriteEntry writeMiniBatchOperationsToMemStore(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
      throws IOException {
      super.writeMiniBatchOperationsToMemStore(miniBatchOp, getOrigLogSeqNum());
      return writeEntry;
    }

    @Override
    public void completeMiniBatchOperations(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final WriteEntry writeEntry)
      throws IOException {
      super.completeMiniBatchOperations(miniBatchOp, writeEntry);
      if (KnobRuntime.check(java.util.UUID.fromString("8ce2db5e-df20-3f45-ad79-a59c7315198a"))) { region.mvcc.advanceTo(EnvironmentEdgeManager.currentTime()); } else { region.mvcc.advanceTo(getOrigLogSeqNum()); }
    }
  }

  public OperationStatus[] batchMutate(Mutation[] mutations, boolean atomic, long nonceGroup,
    long nonce) throws IOException {
    // As it stands, this is used for 3 things
    // * batchMutate with single mutation - put/delete/increment/append, separate or from
    // checkAndMutate.
    // * coprocessor calls (see ex. BulkDeleteEndpoint).
    // So nonces are not really ever used by HBase. They could be by coprocs, and checkAnd...
    return batchMutate(new MutationBatchOperation(this, mutations, atomic, nonceGroup, nonce));
  }

  @Override
  public OperationStatus[] batchMutate(Mutation[] mutations) throws IOException {
    // If the mutations has any Increment/Append operations, we need to do batchMutate atomically
    boolean atomic =
      Arrays.stream(mutations).anyMatch(m -> m instanceof Increment || m instanceof Append);
    return batchMutate(mutations, atomic);
  }

  OperationStatus[] batchMutate(Mutation[] mutations, boolean atomic) throws IOException {
    return TraceUtil.trace(
      () -> batchMutate(mutations, atomic, HConstants.NO_NONCE, HConstants.NO_NONCE),
      () -> createRegionSpan("Region.batchMutate"));
  }

  public OperationStatus[] batchReplay(MutationReplay[] mutations, long replaySeqId)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3c38e2ce-f53a-337e-ad94-920487d6d82e"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("852a95d9-332c-39a2-a8fd-b097ff1cfd7a"))) {
throw new java.io.IOException("Injected exception");
}
    if (
      !RegionReplicaUtil.isDefaultReplica(getRegionInfo())
        && replaySeqId < lastReplayedOpenRegionSeqId
    ) {
      // if it is a secondary replica we should ignore these entries silently
      // since they are coming out of order
      if (LOG.isTraceEnabled()) {
        LOG.trace(getRegionInfo().getEncodedName() + " : " + "Skipping " + mutations.length
          + " mutations with replaySeqId=" + replaySeqId
          + " which is < than lastReplayedOpenRegionSeqId=" + lastReplayedOpenRegionSeqId);
        for (MutationReplay mut : mutations) {
          LOG.trace(getRegionInfo().getEncodedName() + " : Skipping : " + mut.mutation);
        }
      }

      OperationStatus[] statuses = new OperationStatus[mutations.length];
      for (int i = 0; i < statuses.length; i++) {
        statuses[i] = OperationStatus.SUCCESS;
      }
      return statuses;
    }
if(KnobRuntime.check(java.util.UUID.fromString("db78f13e-8e7f-30c8-8e50-f982e3386fa6"))) {
throw new java.io.IOException("Injected exception");
}
    return batchMutate(new ReplayBatchOperation(this, mutations, replaySeqId));
  }

  /**
   * Perform a batch of mutations.
   * <p/>
   * Operations in a batch are stored with highest durability specified of for all operations in a
   * batch, except for {@link Durability#SKIP_WAL}.
   * <p/>
   * This function is called from {@link #batchReplay(WALSplitUtil.MutationReplay[], long)} with
   * {@link ReplayBatchOperation} instance and {@link #batchMutate(Mutation[])} with
   * {@link MutationBatchOperation} instance as an argument. As the processing of replay batch and
   * mutation batch is very similar, lot of code is shared by providing generic methods in base
   * class {@link BatchOperation}. The logic for this method and
   * {@link #doMiniBatchMutate(BatchOperation)} is implemented using methods in base class which are
   * overridden by derived classes to implement special behavior.
   * @param batchOp contains the list of mutations
   * @return an array of OperationStatus which internally contains the OperationStatusCode and the
   *         exceptionMessage if any.
   * @throws IOException if an IO problem is encountered
   */
  private OperationStatus[] batchMutate(BatchOperation<?> batchOp) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0aff752e-ed54-3f4d-9111-87585f34f42a"))) {
try {
    java.lang.reflect.Field field = batchOp.getClass().getDeclaredField("nextIndexToProcess");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batchOp));
    field.set(batchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c999231-1533-3bd3-be54-da4995a58993"))) {
try {
    java.lang.reflect.Field field = batchOp.getClass().getDeclaredField("nextIndexToProcess");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batchOp));
    field.set(batchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c667ffcd-1ea1-3dee-a711-238d47ed9287"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b24a0bbd-4286-3a8e-a13b-f195397be90b"))) {
try {
    java.lang.reflect.Field field = batchOp.getClass().getDeclaredField("nextIndexToProcess");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batchOp));
    field.set(batchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("087df18b-4e89-3e3a-b257-589e2b45765b"))) {
try {
    java.lang.reflect.Field field = batchOp.getClass().getDeclaredField("atomic");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(batchOp);
    field.set(batchOp, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("406d5d8e-e770-37a5-9954-bc41ed996831"))) {
try {
    java.lang.reflect.Field field = batchOp.getClass().getDeclaredField("nextIndexToProcess");
    field.setAccessible(true);
    int oldValue = ((int)field.get(batchOp));
    field.set(batchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d8c5d9c-9648-3912-afa6-85af2230a4eb"))) {
return null;
}
    boolean initialized = false;
if(KnobRuntime.check(java.util.UUID.fromString("d29360c7-1c00-3142-9c2b-7f17809c299a"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("a34e069e-277c-3b44-980e-c73617e1ebd7"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("1ee3c13a-355b-3dd1-829e-1d28106a87ff"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("982ece8b-454b-3098-ac4e-87055d0b78f6"))) { setHTableSpecificConf(); } else { batchOp.startRegionOperation(); }
    try {
      while (!batchOp.isDone()) {
        if (!batchOp.isInReplay()) {
          checkReadOnly();
        }
        checkResources();

        if (!initialized) {
          this.writeRequestsCount.add(batchOp.size());
          // validate and prepare batch for write, for MutationBatchOperation it also calls CP
          // prePut()/preDelete()/preIncrement()/preAppend() hooks
          batchOp.checkAndPrepare();
          initialized = true;
        }
        doMiniBatchMutate(batchOp);
        requestFlushIfNeeded();
      }
    } finally {
      if (rsServices != null && rsServices.getMetrics() != null) {
        rsServices.getMetrics().updateWriteQueryMeter(this, batchOp.size());
      }
if(KnobRuntime.check(java.util.UUID.fromString("810ed0dc-c89d-32f1-8b78-e9a2c0cc811a"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("22a9080b-3992-310b-aa0c-892f72991598"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("f81a7907-4038-3ee8-ac2d-f3f19a833938"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("d58b3b1f-3222-391c-bb48-85fa3f0428db"))) { waitForFlushesAndCompactions(); } else { batchOp.closeRegionOperation(); }
    }
    return batchOp.retCodeDetails;
  }

  /**
   * Called to do a piece of the batch that came in to {@link #batchMutate(Mutation[])} In here we
   * also handle replay of edits on region recover. Also gets change in size brought about by
   * applying {@code batchOp}.
   */
  private void doMiniBatchMutate(BatchOperation<?> batchOp) throws IOException {
    boolean success = false;
    WALEdit walEdit = null;
    WriteEntry writeEntry = null;
    boolean locked = false;
    // We try to set up a batch in the range [batchOp.nextIndexToProcess,lastIndexExclusive)
    MiniBatchOperationInProgress<Mutation> miniBatchOp = null;
    /** Keep track of the locks we hold so we can release them in finally clause */
    List<RowLock> acquiredRowLocks = Lists.newArrayListWithCapacity(batchOp.size());

    // Check for thread interrupt status in case we have been signaled from
    // #interruptRegionOperation.
if(KnobRuntime.check(java.util.UUID.fromString("f1f3b418-a123-3170-93da-e9d0bce9cad3"))) {
throw new java.io.InterruptedIOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("0cfcc765-80fa-3025-922b-32650add8413"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("bb3ae2f1-c23d-323a-b499-151931e5d92d"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("9eddc904-bf6a-31ad-8ea7-232903c90600"))) { setHTableSpecificConf(); } else { checkInterrupt(); }

    try {
      // STEP 1. Try to acquire as many locks as we can and build mini-batch of operations with
      // locked rows
      miniBatchOp = batchOp.lockRowsAndBuildMiniBatch(acquiredRowLocks);

      // We've now grabbed as many mutations off the list as we can
      // Ensure we acquire at least one.
      if (miniBatchOp.getReadyToWriteCount() <= 0) {
        // Nothing to put/delete/increment/append -- an exception in the above such as
        // NoSuchColumnFamily?
        return;
      }

      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation. Do it before we take the lock and disable interrupts for
      // the WAL append.
      checkInterrupt();

      lock(this.updatesLock.readLock(), miniBatchOp.getReadyToWriteCount());
      locked = true;

      // From this point until memstore update this operation should not be interrupted.
      if (KnobRuntime.check(java.util.UUID.fromString("edfbc011-565a-3de5-acba-aed50252391b"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("63d8a823-7794-3268-8212-90e8f4313277"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("ef37c2eb-6c70-3539-b439-17042dec8f3b"))) { waitForFlushesAndCompactions(); } else { disableInterrupts(); }

      // STEP 2. Update mini batch of all operations in progress with LATEST_TIMESTAMP timestamp
      // We should record the timestamp only after we have acquired the rowLock,
      // otherwise, newer puts/deletes/increment/append are not guaranteed to have a newer
      // timestamp

      long now = EnvironmentEdgeManager.currentTime();
      batchOp.prepareMiniBatchOperations(miniBatchOp, now, acquiredRowLocks);

      // STEP 3. Build WAL edit

      List<Pair<NonceKey, WALEdit>> walEdits = batchOp.buildWALEdits(miniBatchOp);

      // STEP 4. Append the WALEdits to WAL and sync.

      for (Iterator<Pair<NonceKey, WALEdit>> it = walEdits.iterator(); it.hasNext();) {
        Pair<NonceKey, WALEdit> nonceKeyWALEditPair = it.next();
        walEdit = nonceKeyWALEditPair.getSecond();
        NonceKey nonceKey = nonceKeyWALEditPair.getFirst();

        if (walEdit != null && !walEdit.isEmpty()) {
          writeEntry = doWALAppend(walEdit, batchOp.durability, batchOp.getClusterIds(), now,
            nonceKey.getNonceGroup(), nonceKey.getNonce(), batchOp.getOrigLogSeqNum());
        }

        // Complete mvcc for all but last writeEntry (for replay case)
        if (it.hasNext() && writeEntry != null) {
          mvcc.complete(writeEntry);
          writeEntry = null;
        }
      }

      // STEP 5. Write back to memStore
      // NOTE: writeEntry can be null here
      writeEntry = batchOp.writeMiniBatchOperationsToMemStore(miniBatchOp, writeEntry);

      // STEP 6. Complete MiniBatchOperations: If required calls postBatchMutate() CP hook and
      // complete mvcc for last writeEntry
      batchOp.completeMiniBatchOperations(miniBatchOp, writeEntry);
      writeEntry = null;
      success = true;
    } finally {
      // Call complete rather than completeAndWait because we probably had error if walKey != null
      if (writeEntry != null) mvcc.complete(writeEntry);

      if (locked) {
        this.updatesLock.readLock().unlock();
      }
      releaseRowLocks(acquiredRowLocks);

      enableInterrupts();

      final int finalLastIndexExclusive =
        miniBatchOp != null ? miniBatchOp.getLastIndexExclusive() : batchOp.size();
      final boolean finalSuccess = success;
if(KnobRuntime.check(java.util.UUID.fromString("42a158dc-a62f-368f-8614-9e848befa07e"))) {
throw new java.io.IOException("Injected exception");
}
      batchOp.visitBatchOperations(true, finalLastIndexExclusive, (int i) -> {
        Mutation mutation = batchOp.getMutation(i);
        if (mutation instanceof Increment || mutation instanceof Append) {
          if (finalSuccess) {
            batchOp.retCodeDetails[i] =
              new OperationStatus(OperationStatusCode.SUCCESS, batchOp.results[i]);
          } else {
            batchOp.retCodeDetails[i] = OperationStatus.FAILURE;
          }
        } else {
          batchOp.retCodeDetails[i] =
            finalSuccess ? OperationStatus.SUCCESS : OperationStatus.FAILURE;
        }
        return true;
      });

      batchOp.doPostOpCleanupForMiniBatch(miniBatchOp, walEdit, finalSuccess);

      batchOp.nextIndexToProcess = finalLastIndexExclusive;
    }
  }

  /**
   * Returns effective durability from the passed durability and the table descriptor.
   */
  private Durability getEffectiveDurability(Durability d) {
    return d == Durability.USE_DEFAULT ? this.regionDurability : d;
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
    ByteArrayComparable comparator, TimeRange timeRange, Mutation mutation) throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      CheckAndMutate.Builder builder = CheckAndMutate.newBuilder(row)
        .ifMatches(family, qualifier, op, comparator.getValue()).timeRange(timeRange);
      if (mutation instanceof Put) {
        checkAndMutate = builder.build((Put) mutation);
      } else if (mutation instanceof Delete) {
        checkAndMutate = builder.build((Delete) mutation);
      } else {
        throw new DoNotRetryIOException(
          "Unsupported mutate type: " + mutation.getClass().getSimpleName().toUpperCase());
      }
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndMutate(byte[] row, Filter filter, TimeRange timeRange, Mutation mutation)
    throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      CheckAndMutate.Builder builder =
        CheckAndMutate.newBuilder(row).ifMatches(filter).timeRange(timeRange);
      if (mutation instanceof Put) {
        checkAndMutate = builder.build((Put) mutation);
      } else if (mutation instanceof Delete) {
        checkAndMutate = builder.build((Delete) mutation);
      } else {
        throw new DoNotRetryIOException(
          "Unsupported mutate type: " + mutation.getClass().getSimpleName().toUpperCase());
      }
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndRowMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
    ByteArrayComparable comparator, TimeRange timeRange, RowMutations rm) throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      checkAndMutate = CheckAndMutate.newBuilder(row)
        .ifMatches(family, qualifier, op, comparator.getValue()).timeRange(timeRange).build(rm);
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  @Deprecated
  public boolean checkAndRowMutate(byte[] row, Filter filter, TimeRange timeRange, RowMutations rm)
    throws IOException {
    CheckAndMutate checkAndMutate;
    try {
      checkAndMutate =
        CheckAndMutate.newBuilder(row).ifMatches(filter).timeRange(timeRange).build(rm);
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException(e.getMessage());
    }
    return checkAndMutate(checkAndMutate).isSuccess();
  }

  @Override
  public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
    return checkAndMutate(checkAndMutate, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate, long nonceGroup,
    long nonce) throws IOException {
    return TraceUtil.trace(() -> checkAndMutateInternal(checkAndMutate, nonceGroup, nonce),
      () -> createRegionSpan("Region.checkAndMutate"));
  }

  private CheckAndMutateResult checkAndMutateInternal(CheckAndMutate checkAndMutate,
    long nonceGroup, long nonce) throws IOException {
    byte[] row = checkAndMutate.getRow();
    Filter filter = null;
    byte[] family = null;
    byte[] qualifier = null;
    CompareOperator op = null;
    ByteArrayComparable comparator = null;
    if (checkAndMutate.hasFilter()) {
      filter = checkAndMutate.getFilter();
    } else {
      family = checkAndMutate.getFamily();
      qualifier = checkAndMutate.getQualifier();
      op = checkAndMutate.getCompareOp();
      comparator = new BinaryComparator(checkAndMutate.getValue());
    }
    TimeRange timeRange = checkAndMutate.getTimeRange();

    Mutation mutation = null;
    RowMutations rowMutations = null;
    if (checkAndMutate.getAction() instanceof Mutation) {
      mutation = (Mutation) checkAndMutate.getAction();
    } else {
      rowMutations = (RowMutations) checkAndMutate.getAction();
    }

    if (mutation != null) {
      checkMutationType(mutation);
      checkRow(mutation, row);
    } else {
      checkRow(rowMutations, row);
    }
    checkReadOnly();
    // TODO, add check for value length also move this check to the client
    checkResources();
    startRegionOperation();
    try {
      Get get = new Get(row);
      if (family != null) {
        checkFamily(family);
        get.addColumn(family, qualifier);
      }
      if (filter != null) {
        get.setFilter(filter);
      }
      if (timeRange != null) {
        get.setTimeRange(timeRange.getMin(), timeRange.getMax());
      }
      // Lock row - note that doBatchMutate will relock this row if called
      checkRow(row, "doCheckAndRowMutate");
      RowLock rowLock = getRowLock(get.getRow(), false, null);
      try {
        if (this.getCoprocessorHost() != null) {
          CheckAndMutateResult result =
            getCoprocessorHost().preCheckAndMutateAfterRowLock(checkAndMutate);
          if (result != null) {
            return result;
          }
        }

        // NOTE: We used to wait here until mvcc caught up: mvcc.await();
        // Supposition is that now all changes are done under row locks, then when we go to read,
        // we'll get the latest on this row.
        boolean matches = false;
        long cellTs = 0;
        try (RegionScanner scanner = getScanner(new Scan(get))) {
          // NOTE: Please don't use HRegion.get() instead,
          // because it will copy cells to heap. See HBASE-26036
          List<Cell> result = new ArrayList<>(1);
          scanner.next(result);
          if (filter != null) {
            if (!result.isEmpty()) {
              matches = true;
              cellTs = result.get(0).getTimestamp();
            }
          } else {
            boolean valueIsNull =
              comparator.getValue() == null || comparator.getValue().length == 0;
            if (result.isEmpty() && valueIsNull) {
              matches = op != CompareOperator.NOT_EQUAL;
            } else if (result.size() > 0 && valueIsNull) {
              matches = (result.get(0).getValueLength() == 0) == (op != CompareOperator.NOT_EQUAL);
              cellTs = result.get(0).getTimestamp();
            } else if (result.size() == 1) {
              Cell kv = result.get(0);
              cellTs = kv.getTimestamp();
              int compareResult = PrivateCellUtil.compareValue(kv, comparator);
              matches = matches(op, compareResult);
            }
          }
        }

        // If matches, perform the mutation or the rowMutations
        if (matches) {
          // We have acquired the row lock already. If the system clock is NOT monotonically
          // non-decreasing (see HBASE-14070) we should make sure that the mutation has a
          // larger timestamp than what was observed via Get. doBatchMutate already does this, but
          // there is no way to pass the cellTs. See HBASE-14054.
          long now = EnvironmentEdgeManager.currentTime();
          long ts = Math.max(now, cellTs); // ensure write is not eclipsed
          byte[] byteTs = Bytes.toBytes(ts);
          if (mutation != null) {
            if (mutation instanceof Put) {
              updateCellTimestamps(mutation.getFamilyCellMap().values(), byteTs);
            }
            // And else 'delete' is not needed since it already does a second get, and sets the
            // timestamp from get (see prepareDeleteTimestamps).
          } else {
            for (Mutation m : rowMutations.getMutations()) {
              if (m instanceof Put) {
                updateCellTimestamps(m.getFamilyCellMap().values(), byteTs);
              }
            }
            // And else 'delete' is not needed since it already does a second get, and sets the
            // timestamp from get (see prepareDeleteTimestamps).
          }
          // All edits for the given row (across all column families) must happen atomically.
          Result r;
          if (mutation != null) {
            r = mutate(mutation, true, nonceGroup, nonce).getResult();
          } else {
            r = mutateRow(rowMutations, nonceGroup, nonce);
          }
          this.checkAndMutateChecksPassed.increment();
          return new CheckAndMutateResult(true, r);
        }
        this.checkAndMutateChecksFailed.increment();
        return new CheckAndMutateResult(false, null);
      } finally {
        rowLock.release();
      }
    } finally {
      closeRegionOperation();
    }
  }

  private void checkMutationType(final Mutation mutation) throws DoNotRetryIOException {
    if (
      !(mutation instanceof Put) && !(mutation instanceof Delete)
        && !(mutation instanceof Increment) && !(mutation instanceof Append)
    ) {
      throw new org.apache.hadoop.hbase.DoNotRetryIOException(
        "Action must be Put or Delete or Increment or Delete");
    }
  }

  private void checkRow(final Row action, final byte[] row) throws DoNotRetryIOException {
if(KnobRuntime.check(java.util.UUID.fromString("4eddf2a9-45a2-3121-b299-a51085663467"))) {
return;
}
    if (!Bytes.equals(row, action.getRow())) {
      throw new org.apache.hadoop.hbase.DoNotRetryIOException("Action's getRow must match");
    }
  }

  private boolean matches(final CompareOperator op, final int compareResult) {
if(KnobRuntime.check(java.util.UUID.fromString("6a046ba6-d085-363b-8195-0189bc741fb4"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("3d18377c-9ccb-32bc-8549-8250a212d475"))) {
return false;
}
    boolean matches = false;
    switch (op) {
      case LESS:
        matches = compareResult < 0;
        break;
      case LESS_OR_EQUAL:
        matches = compareResult <= 0;
        break;
      case EQUAL:
        matches = compareResult == 0;
        break;
      case NOT_EQUAL:
        matches = compareResult != 0;
        break;
      case GREATER_OR_EQUAL:
        matches = compareResult >= 0;
        break;
      case GREATER:
        matches = compareResult > 0;
        break;
      default:
        throw new RuntimeException("Unknown Compare op " + op.name());
    }
    return matches;
  }

  private OperationStatus mutate(Mutation mutation) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("18dc74d6-8fe6-39e2-bfca-54d72c726be9"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0a431677-5700-3ce6-b83c-3f21c6774292"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("9807e651-075a-3a97-93b6-5d19bb52e286"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return mutate(mutation, false);
  }

  private OperationStatus mutate(Mutation mutation, boolean atomic) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ab68aa7f-1d21-3d06-a377-a5ca120068ff"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c66979bb-4a40-3439-b769-7b2f8fbf5173"))) {
atomic = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("9ea94531-34af-366e-8940-52664836381b"))) {
atomic = !atomic;
}
if(KnobRuntime.check(java.util.UUID.fromString("820326dd-b4c7-30cf-bb26-ad2e2a53b104"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("36ad9e8e-76fa-36e9-98c7-3acefe8453cb"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cdfd728e-ee78-37a7-ae06-3378d9ed6519"))) {
atomic = false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("3f27ba75-286d-3228-bed3-93ddcc89bb07"))) ? (mutate(mutation, !atomic, HConstants.NO_NONCE, HConstants.NO_NONCE)) : (mutate(mutation, atomic, HConstants.NO_NONCE, HConstants.NO_NONCE)));
  }

  private OperationStatus mutate(Mutation mutation, boolean atomic, long nonceGroup, long nonce)
    throws IOException {
    OperationStatus[] status =
      this.batchMutate(new Mutation[] { mutation }, atomic, nonceGroup, nonce);
    if (status[0].getOperationStatusCode().equals(OperationStatusCode.SANITY_CHECK_FAILURE)) {
      throw new FailedSanityCheckException(status[0].getExceptionMsg());
    } else if (status[0].getOperationStatusCode().equals(OperationStatusCode.BAD_FAMILY)) {
      throw new NoSuchColumnFamilyException(status[0].getExceptionMsg());
    } else if (status[0].getOperationStatusCode().equals(OperationStatusCode.STORE_TOO_BUSY)) {
      throw new RegionTooBusyException(status[0].getExceptionMsg());
    }
    return status[0];
  }

  /**
   * Complete taking the snapshot on the region. Writes the region info and adds references to the
   * working snapshot directory. TODO for api consistency, consider adding another version with no
   * {@link ForeignExceptionSnare} arg. (In the future other cancellable HRegion methods could
   * eventually add a {@link ForeignExceptionSnare}, or we could do something fancier).
   * @param desc     snapshot description object
   * @param exnSnare ForeignExceptionSnare that captures external exceptions in case we need to bail
   *                 out. This is allowed to be null and will just be ignored in that case.
   * @throws IOException if there is an external or internal error causing the snapshot to fail
   */
  public void addRegionToSnapshot(SnapshotDescription desc, ForeignExceptionSnare exnSnare)
    throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir, conf);

    SnapshotManifest manifest =
      SnapshotManifest.create(conf, getFilesystem(), snapshotDir, desc, exnSnare);
    manifest.addRegion(this);
  }

  private void updateSequenceId(final Iterable<List<Cell>> cellItr, final long sequenceId)
    throws IOException {
    for (List<Cell> cells : cellItr) {
      if (((KnobRuntime.check(java.util.UUID.fromString("e4f40cbd-e358-37ab-9c6f-b5291394d34f"))) ? ((cells) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("dd60a022-4e33-3ea6-8960-fed295723e8f"))) ? ((cells) == (null)) : (cells == null))))) return;
      for (Cell cell : cells) {
        PrivateCellUtil.setSequenceId(cell, sequenceId);
      }
    }
  }

  /**
   * Replace any cell timestamps set to {@link org.apache.hadoop.hbase.HConstants#LATEST_TIMESTAMP}
   * provided current timestamp.
   */
  private static void updateCellTimestamps(final Iterable<List<Cell>> cellItr, final byte[] now)
    throws IOException {
    for (List<Cell> cells : cellItr) {
      if (((KnobRuntime.check(java.util.UUID.fromString("e84c0708-7d48-3137-8c4a-5b554ab3aa60"))) ? ((cells) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ddde7221-4580-3e69-960b-c5602e28f75e"))) ? ((cells) == (null)) : (cells == null))))) continue;
      // Optimization: 'foreach' loop is not used. See:
      // HBASE-12023 HRegion.applyFamilyMapToMemstore creates too many iterator objects
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i = 0; i < listSize; i++) {
        PrivateCellUtil.updateLatestStamp(cells.get(i), now);
      }
    }
  }

  /**
   * Possibly rewrite incoming cell tags.
   */
  private void rewriteCellTags(Map<byte[], List<Cell>> familyMap, final Mutation m) {
    // Check if we have any work to do and early out otherwise
    // Update these checks as more logic is added here
    if (((KnobRuntime.check(java.util.UUID.fromString("d4f8d230-1de2-3842-bb15-fcf1bcc8d002"))) ? ((EnvironmentEdgeManager.currentTime()) == (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("2bebd8d4-cacc-32df-8497-8a45885c5d2d"))) ? ((m.getTTL()) != (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("2be193f7-1c2e-38f1-891a-1954731f5c44"))) ? ((m.getTTL()) == (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("f7fabceb-92c4-39ef-8d7d-0b61c430c3b6"))) ? ((EnvironmentEdgeManager.currentTime()) != (Long.MAX_VALUE)) : (m.getTTL() == Long.MAX_VALUE))))))))) {
      return;
    }

    // From this point we know we have some work to do
    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
      List<Cell> cells = e.getValue();
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i = 0; i < listSize; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("fa8c4969-7f85-31e6-bb2e-3c228c4882d2"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5d0b9e06-a7a7-321a-ac6b-09f759bcc99d"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3ca327fc-53bc-3d57-9df8-0074ea3c30a4"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2346824-beb8-3ee3-a04b-85ed4a120a77"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4c7db28-4a68-376b-a179-8c4a0ccd1746"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3dc255bc-58ad-384b-928e-09970c721438"))) {
i /= 2;
}
        Cell cell = cells.get(i);
        List<Tag> newTags = TagUtil.carryForwardTags(null, cell);
        newTags = TagUtil.carryForwardTTLTag(newTags, m.getTTL());
        // Rewrite the cell with the updated set of tags
if(KnobRuntime.check(java.util.UUID.fromString("3876bb2e-15f3-3d67-9476-05d632f5502d"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("020a6951-a07d-3ce2-9714-dc1ee2e039ce"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("15a5e131-6d45-370f-8d1f-4f09d7890f8c"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6615b667-2506-3490-b406-3853f2eed4d4"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3163bbe5-1174-37e4-b2b7-14b3261ea5e8"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ef8a972b-7d7d-35d7-a4b3-0d622608fc62"))) {
i = -1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("8d72d1a2-5feb-36b8-a9fc-e65e5d56de57"))) { cells.set(i++, PrivateCellUtil.createCell(cell, newTags)); } else if (KnobRuntime.check(java.util.UUID.fromString("b0848dd6-5950-373c-9f08-e26c86c84f0c"))) { cells.set(0, PrivateCellUtil.createCell(cell, newTags)); } else { cells.set(i, PrivateCellUtil.createCell(cell, newTags)); }
      }
    }
  }

  /**
   * Check if resources to support an update.
   * <p/>
   * We throw RegionTooBusyException if above memstore limit and expect client to retry using some
   * kind of backoff
   */
  private void checkResources() throws RegionTooBusyException {
    // If catalog region, do not impose resource constraints or block updates.
    if (this.getRegionInfo().isMetaRegion()) {
      return;
    }

    MemStoreSize mss = this.memStoreSizing.getMemStoreSize();
    if (((KnobRuntime.check(java.util.UUID.fromString("f1d41189-199b-30c5-b803-f1a4eba66055"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getHeapSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6e1409c0-e33f-36d2-a4d1-f16e90d8e9f0"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getDataSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("39f03a28-ecb2-33a3-ab48-92aab7107965"))) ? ((EnvironmentEdgeManager.currentTime()) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("5a313205-ef9a-36d6-a495-11222cdaa6af"))) ? (((mss.getHeapSize()) + (mss.getDataSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("61c3ef2f-08d6-3fb3-8aaf-0a6663bb671f"))) ? (((mss.getDataSize()) + (mss.getHeapSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("b1476ac9-9c36-3c8d-a66a-00ea48adcc44"))) ? ((mss.getOffHeapSize()) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("db0d480a-00e7-3768-a59d-b20f8585b493"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getOffHeapSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("8c3e2d2d-525f-3fa5-be47-862114fc286a"))) ? (((mss.getHeapSize()) + (EnvironmentEdgeManager.currentTime())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("21a0660f-7db8-3d0f-a5ce-5c311ca75698"))) ? ((mss.getHeapSize()) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("7ecc95c9-d0da-327d-ae32-bb3c61e3e407"))) ? ((mss.getDataSize()) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("b98929f1-dd03-35e9-8a5d-68ff38c7cc0c"))) ? ((mss.getHeapSize()) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("a6788fdb-f4eb-393b-9e8e-7c00d0a8d3e6"))) ? ((EnvironmentEdgeManager.currentTime()) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("1baf01db-f5af-3d10-b2c2-5ff1d4d118ff"))) ? (((mss.getDataSize()) + (EnvironmentEdgeManager.currentTime())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("eaa921b1-2c7e-349c-9c83-54c5beee8a9e"))) ? (((mss.getHeapSize()) + (mss.getOffHeapSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("afdd7f2b-8f62-3ef4-bb83-96edc43c7257"))) ? ((mss.getDataSize()) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6e376ce6-201a-3221-9847-ea85729cbb27"))) ? (((mss.getDataSize()) + (mss.getOffHeapSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("7d012355-6d59-3704-9752-6044c374e42f"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getDataSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("d0943a14-c6b6-347c-8d37-c626d87edc1d"))) ? (((mss.getHeapSize()) + (mss.getDataSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("133fe4d2-e2a3-3f0b-8661-9b0e884d6432"))) ? (((mss.getHeapSize()) + (EnvironmentEdgeManager.currentTime())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("837a7cb8-73f8-3f1a-87b6-f29fb94030ca"))) ? (((mss.getDataSize()) + (mss.getHeapSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("7fa2e8ec-964a-3f28-aacb-f5b087f6f21f"))) ? ((mss.getOffHeapSize()) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("66c74e74-c089-3316-a98a-84b23c39dc61"))) ? ((mss.getDataSize()) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9f0fa8f2-4739-3d88-b56d-217b781065b3"))) ? (((mss.getDataSize()) + (mss.getDataSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("d01ee687-2187-3893-8f04-a01e919fcb91"))) ? ((mss.getDataSize()) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("7200d9ab-d7d8-3fc7-a510-e708d8319817"))) ? (((mss.getDataSize()) + (EnvironmentEdgeManager.currentTime())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("c9bed4fb-4fdf-3368-a7c5-f7231a3767aa"))) ? (((mss.getHeapSize()) + (EnvironmentEdgeManager.currentTime())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("a80350fc-b61c-34aa-bdb9-14bb50000e2f"))) ? (((mss.getDataSize()) + (mss.getDataSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2435bab1-4f97-359c-8bbf-242952940881"))) ? ((mss.getHeapSize() + mss.getOffHeapSize()) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("b45bb679-67f1-3f1f-924e-9d436fc8af7a"))) ? (((mss.getDataSize()) + (mss.getOffHeapSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("d772778d-e131-302d-9008-09cf43cb404a"))) ? (((mss.getHeapSize()) + (mss.getHeapSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("a2e67877-17fb-302e-a958-b991f2721481"))) ? ((EnvironmentEdgeManager.currentTime()) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9c6a2e9c-4b7c-3e73-8f06-bd3934b3f487"))) ? ((mss.getHeapSize() + mss.getOffHeapSize()) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("3585c36e-a9db-3092-a159-30011bbb281b"))) ? (((mss.getHeapSize()) + (mss.getOffHeapSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("e146857a-f437-3139-bfa0-8a4f726dfdaa"))) ? ((mss.getOffHeapSize()) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("baa8976a-e865-328f-bdd4-6b94bb8f65d1"))) ? ((mss.getHeapSize() + mss.getOffHeapSize()) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6e28e590-8e2a-3cf6-b9ee-9d75634932ce"))) ? (((mss.getDataSize()) + (mss.getHeapSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("91b6e876-61e9-3cc1-bd41-5b29d7da7912"))) ? ((EnvironmentEdgeManager.currentTime()) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("7cf0cd51-9989-3a8d-9e37-82ba6c6ff59f"))) ? (((mss.getHeapSize()) + (mss.getDataSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("52a2da4d-931d-3e28-b5f4-ff8767ed1bdb"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getDataSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("30b1dcc4-6421-3964-96fe-6bc0cda9c50e"))) ? (((mss.getHeapSize()) + (mss.getDataSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2ba530c2-f112-3c4b-ad9e-10205d7c7048"))) ? (((mss.getHeapSize()) + (mss.getDataSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9cde8730-0586-31e6-a812-24cbe8ec29c0"))) ? (((mss.getDataSize()) + (EnvironmentEdgeManager.currentTime())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("a4154342-9e39-3ef0-be4a-3a284b193692"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getHeapSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("4c8e38ce-2c92-3505-a063-926d85019578"))) ? (((mss.getHeapSize()) + (mss.getOffHeapSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("52114a59-f180-3aa7-ab79-2b9fa7818bd2"))) ? (((mss.getHeapSize()) + (mss.getDataSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("05607d9f-8237-31ab-b760-8bf3490a9794"))) ? (((mss.getHeapSize()) + (mss.getHeapSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("fd53d563-74a1-3e6c-b3fd-9b49e2ebce3d"))) ? (((mss.getDataSize()) + (mss.getDataSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("698e47ef-714e-327b-8596-5ca365cc9fa5"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getHeapSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("a90f7894-bdd8-3634-baca-6a27661dea91"))) ? ((mss.getDataSize()) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2b51892c-b52a-354c-9bf0-f780bf0e8bc7"))) ? (((mss.getDataSize()) + (mss.getHeapSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("308f829b-a90e-338e-a678-6140bf4136df"))) ? (((mss.getDataSize()) + (mss.getDataSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("8a14acbf-7672-3fa2-b1b2-c29b52569c65"))) ? ((mss.getOffHeapSize()) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2b5ff8c1-e428-37a4-b12d-63437d887207"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getHeapSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2d234588-7fb9-351c-99fb-22881ab4d27d"))) ? ((mss.getHeapSize() + mss.getOffHeapSize()) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("423b6ade-345e-3e83-bd8f-bbd51a898aae"))) ? ((mss.getDataSize()) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("cc8216be-5190-30ec-8283-4f9ab288d137"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getDataSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("f7c67970-c3ea-31ab-8726-b91789aa2e52"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getHeapSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2886db6f-e209-3da7-8b60-4b270bed3e5a"))) ? ((mss.getOffHeapSize()) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("99ba2c04-c9a8-3ceb-b85a-5e5d1661e4b2"))) ? (((EnvironmentEdgeManager.currentTime()) + (EnvironmentEdgeManager.currentTime())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("13be1312-1603-35e4-a6c0-415d31d80dc3"))) ? ((mss.getHeapSize()) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9d0b140a-e1af-3173-8eee-93a2e87aca99"))) ? (((mss.getHeapSize()) + (mss.getHeapSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("4a7b45fb-b555-301a-9c59-171ab62e1467"))) ? (((mss.getHeapSize()) + (mss.getOffHeapSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("41729dae-0a3f-3ba0-9e49-122e55b30b43"))) ? ((EnvironmentEdgeManager.currentTime()) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("b0c1b70a-4280-357a-b2b8-90ba857c4420"))) ? (((EnvironmentEdgeManager.currentTime()) + (EnvironmentEdgeManager.currentTime())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("3a768b7c-df7c-3587-8791-364a32fc6dfd"))) ? ((mss.getHeapSize()) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("5cc65fd4-3b8b-3e35-8d8f-886e5eaad9dc"))) ? (((EnvironmentEdgeManager.currentTime()) + (EnvironmentEdgeManager.currentTime())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("e57ce3c3-4fca-32aa-95fb-002ab3f7e057"))) ? (((mss.getHeapSize()) + (mss.getHeapSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("3ba4a6c0-6ee3-3ed1-bdb5-33b041c0c239"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getOffHeapSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9089e7f0-111a-3bfa-88b9-982036bfdd9b"))) ? (((mss.getDataSize()) + (mss.getDataSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("4791b502-26b8-33b4-b931-172c606a7909"))) ? (((mss.getDataSize()) + (EnvironmentEdgeManager.currentTime())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("3053b36b-a795-30a8-85bd-ebe625c41711"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getDataSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("40a0e99b-c7c7-373c-ad82-ac8cea3964ff"))) ? (((mss.getDataSize()) + (mss.getHeapSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("d2f28e51-2c79-3761-b7c6-d1c3580f9495"))) ? (((mss.getHeapSize()) + (mss.getHeapSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("d6f29bb4-0989-3d70-9f2a-6ed9405d7ed9"))) ? (((mss.getDataSize()) + (mss.getOffHeapSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("f652ed06-3f6e-38ff-a388-99cc844b4213"))) ? ((mss.getHeapSize()) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("0bcfdeff-8602-35ee-919a-ffc0a43a7c16"))) ? (((mss.getHeapSize()) + (mss.getHeapSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9c880a62-bd0d-37ef-92e6-21c623364774"))) ? (((mss.getDataSize()) + (mss.getDataSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9802c66d-f8a0-316b-9a8a-c6496a164672"))) ? (((mss.getDataSize()) + (mss.getOffHeapSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("eceba45b-6b86-3a82-96a4-740fc334b82b"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getHeapSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("2f8eafcb-cda5-31ca-9210-277206dcc7a3"))) ? (((mss.getDataSize()) + (mss.getOffHeapSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("1756c5fc-e27f-3b33-bda4-b601c5a7be74"))) ? (((mss.getHeapSize()) + (mss.getOffHeapSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("781746eb-1f4b-37e2-b9f8-095b107c568b"))) ? (((mss.getDataSize()) + (mss.getOffHeapSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("8e45c567-8792-3104-b49d-4394167fca59"))) ? (((mss.getHeapSize()) + (mss.getOffHeapSize())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("42668b4d-4f42-3fe7-a015-f3c1d4379041"))) ? (((mss.getHeapSize()) + (EnvironmentEdgeManager.currentTime())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("17d289c9-c424-3229-aaae-2b52ba50ff58"))) ? ((mss.getHeapSize() + mss.getOffHeapSize()) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("a6adc586-215d-31ac-9928-1a390c544df7"))) ? ((EnvironmentEdgeManager.currentTime()) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("4925453b-02eb-3ae1-bfec-2602b732123f"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getDataSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("e89030a5-6465-3ac6-a481-b219fb04b10d"))) ? (((mss.getDataSize()) + (EnvironmentEdgeManager.currentTime())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6a4481a6-10e2-3265-a3e9-1c075a36a17a"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getOffHeapSize())) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6e648e1e-cfd8-3a2a-96f3-8924824a0c59"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getOffHeapSize())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("b4c10cad-4e59-35f1-8da9-3d4baf997bed"))) ? ((mss.getOffHeapSize()) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("1a087bf0-ba17-39d2-ad1d-da1ad91a0aff"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getOffHeapSize())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("cc2bf3cc-4643-3d31-bfc3-c10b088441ab"))) ? (((EnvironmentEdgeManager.currentTime()) + (EnvironmentEdgeManager.currentTime())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("62579199-2e36-361f-bc44-c378d4eee9fb"))) ? (((EnvironmentEdgeManager.currentTime()) + (EnvironmentEdgeManager.currentTime())) != (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("ecc7c6e9-e521-3307-a842-917a2b136d8e"))) ? (((EnvironmentEdgeManager.currentTime()) + (EnvironmentEdgeManager.currentTime())) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("8d37129b-03ba-3616-ac8b-210d2678b5df"))) ? (((EnvironmentEdgeManager.currentTime()) + (mss.getOffHeapSize())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("ebb23c97-2510-3939-abb5-32df4840e944"))) ? ((mss.getHeapSize()) == (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("af93777e-0b89-3352-b7a6-1db1ee40749a"))) ? (((mss.getHeapSize()) + (EnvironmentEdgeManager.currentTime())) >= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("90c5a470-e61c-394e-a038-2cebd585812d"))) ? ((mss.getHeapSize() + mss.getOffHeapSize()) <= (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("3e7cf1d5-58d8-3972-aef7-d805429805e9"))) ? (((mss.getDataSize()) + (mss.getHeapSize())) < (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("3c92c895-2b2a-349c-980e-7a2dbf9433f4"))) ? (((mss.getDataSize()) + (EnvironmentEdgeManager.currentTime())) > (this.blockingMemStoreSize)) : (((KnobRuntime.check(java.util.UUID.fromString("0b54f740-9d4d-3254-a911-618cbbcd4282"))) ? (((mss.getHeapSize()) + (EnvironmentEdgeManager.currentTime())) > (this.blockingMemStoreSize)) : (mss.getHeapSize() + mss.getOffHeapSize() > this.blockingMemStoreSize))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      blockedRequestsCount.increment();
      requestFlush();
      // Don't print current limit because it will vary too much. The message is used as a key
      // over in RetriesExhaustedWithDetailsException processing.
      final String regionName =
        this.getRegionInfo() == null ? "unknown" : this.getRegionInfo().getEncodedName();
      final String serverName = this.getRegionServerServices() == null
        ? "unknown"
        : (this.getRegionServerServices().getServerName() == null
          ? "unknown"
          : this.getRegionServerServices().getServerName().toString());
      RegionTooBusyException rtbe = new RegionTooBusyException("Over memstore limit="
        + org.apache.hadoop.hbase.procedure2.util.StringUtils.humanSize(this.blockingMemStoreSize)
        + ", regionName=" + regionName + ", server=" + serverName);
      LOG.warn("Region is too busy due to exceeding memstore size limit.", rtbe);
      throw rtbe;
    }
  }

  /**
   * @throws IOException Throws exception if region is in read-only mode.
   */
  private void checkReadOnly() throws IOException {
    if (isReadOnly()) {
      throw new DoNotRetryIOException("region is read only");
    }
  }

  private void checkReadsEnabled() throws IOException {
    if (!this.writestate.readsEnabled) {
      throw new IOException(getRegionInfo().getEncodedName()
        + ": The region's reads are disabled. Cannot serve the request");
    }
  }

  public void setReadsEnabled(boolean readsEnabled) {
    if (readsEnabled && !this.writestate.readsEnabled) {
      LOG.info("Enabling reads for {}", getRegionInfo().getEncodedName());
    }
    this.writestate.setReadsEnabled(readsEnabled);
  }

  /**
   * @param delta If we are doing delta changes -- e.g. increment/append -- then this flag will be
   *              set; when set we will run operations that make sense in the increment/append
   *              scenario but that do not make sense otherwise.
   * @see #applyToMemStore(HStore, Cell, MemStoreSizing)
   */
  private void applyToMemStore(HStore store, List<Cell> cells, boolean delta,
    MemStoreSizing memstoreAccounting) {
    // Any change in how we update Store/MemStore needs to also be done in other applyToMemStore!!!!
    boolean upsert = delta && store.getColumnFamilyDescriptor().getMaxVersions() == 1;
    if (upsert) {
      store.upsert(cells, getSmallestReadPoint(), memstoreAccounting);
    } else {
      store.add(cells, memstoreAccounting);
    }
  }

  /**
   * @see #applyToMemStore(HStore, List, boolean, MemStoreSizing)
   */
  private void applyToMemStore(HStore store, Cell cell, MemStoreSizing memstoreAccounting)
    throws IOException {
    // Any change in how we update Store/MemStore needs to also be done in other applyToMemStore!!!!
    if (store == null) {
      checkFamily(CellUtil.cloneFamily(cell));
      // Unreachable because checkFamily will throw exception
    }
    store.add(cell, memstoreAccounting);
  }

  /**
   * Check the collection of families for validity.
   */
  public void checkFamilies(Collection<byte[]> families) throws NoSuchColumnFamilyException {
    for (byte[] family : families) {
      checkFamily(family);
    }
  }

  /**
   * Check the collection of families for valid timestamps
   * @param now current timestamp
   */
  public void checkTimestamps(final Map<byte[], List<Cell>> familyMap, long now)
    throws FailedSanityCheckException {
    if (timestampSlop == HConstants.LATEST_TIMESTAMP) {
      return;
    }
    long maxTs = now + timestampSlop;
    for (List<Cell> kvs : familyMap.values()) {
      // Optimization: 'foreach' loop is not used. See:
      // HBASE-12023 HRegion.applyFamilyMapToMemstore creates too many iterator objects
      assert kvs instanceof RandomAccess;
      int listSize = kvs.size();
      for (int i = 0; i < listSize; i++) {
        Cell cell = kvs.get(i);
        // see if the user-side TS is out of range. latest = server-side
        long ts = cell.getTimestamp();
        if (ts != HConstants.LATEST_TIMESTAMP && ts > maxTs) {
          throw new FailedSanityCheckException(
            "Timestamp for KV out of range " + cell + " (too.new=" + timestampSlop + ")");
        }
      }
    }
  }

  /*
   * @return True if size is over the flush threshold
   */
  private boolean isFlushSize(MemStoreSize size) {
    return size.getHeapSize() + size.getOffHeapSize() > getMemStoreFlushSize();
  }

  private void deleteRecoveredEdits(FileSystem fs, Iterable<Path> files) throws IOException {
    for (Path file : files) {
      if (!fs.delete(file, false)) {
        LOG.error("Failed delete of {}", file);
      } else {
        LOG.debug("Deleted recovered.edits file={}", file);
      }
    }
  }

  /**
   * Read the edits put under this region by wal splitting process. Put the recovered edits back up
   * into this region.
   * <p>
   * We can ignore any wal message that has a sequence ID that's equal to or lower than minSeqId.
   * (Because we know such messages are already reflected in the HFiles.)
   * <p>
   * While this is running we are putting pressure on memory yet we are outside of our usual
   * accounting because we are not yet an onlined region (this stuff is being run as part of Region
   * initialization). This means that if we're up against global memory limits, we'll not be flagged
   * to flush because we are not online. We can't be flushed by usual mechanisms anyways; we're not
   * yet online so our relative sequenceids are not yet aligned with WAL sequenceids -- not till we
   * come up online, post processing of split edits.
   * <p>
   * But to help relieve memory pressure, at least manage our own heap size flushing if are in
   * excess of per-region limits. Flushing, though, we have to be careful and avoid using the
   * regionserver/wal sequenceid. Its running on a different line to whats going on in here in this
   * region context so if we crashed replaying these edits, but in the midst had a flush that used
   * the regionserver wal with a sequenceid in excess of whats going on in here in this region and
   * with its split editlogs, then we could miss edits the next time we go to recover. So, we have
   * to flush inline, using seqids that make sense in a this single region context only -- until we
   * online.
   * @param maxSeqIdInStores Any edit found in split editlogs needs to be in excess of the maxSeqId
   *                         for the store to be applied, else its skipped.
   * @return the sequence id of the last edit added to this region out of the recovered edits log or
   *         <code>minSeqId</code> if nothing added from editlogs.
   */
  long replayRecoveredEditsIfAny(Map<byte[], Long> maxSeqIdInStores,
    final CancelableProgressable reporter, final MonitoredTask status) throws IOException {
    long minSeqIdForTheRegion = -1;
    for (Long maxSeqIdInStore : maxSeqIdInStores.values()) {
      if (maxSeqIdInStore < minSeqIdForTheRegion || minSeqIdForTheRegion == -1) {
        minSeqIdForTheRegion = maxSeqIdInStore;
      }
    }
    long seqId = minSeqIdForTheRegion;
    String specialRecoveredEditsDirStr = conf.get(SPECIAL_RECOVERED_EDITS_DIR);
    if (org.apache.commons.lang3.StringUtils.isBlank(specialRecoveredEditsDirStr)) {
      FileSystem walFS = getWalFileSystem();
      FileSystem rootFS = getFilesystem();
      Path wrongRegionWALDir = CommonFSUtils.getWrongWALRegionDir(conf, getRegionInfo().getTable(),
        getRegionInfo().getEncodedName());
      Path regionWALDir = getWALRegionDir();
      Path regionDir =
        FSUtils.getRegionDirFromRootDir(CommonFSUtils.getRootDir(conf), getRegionInfo());

      // We made a mistake in HBASE-20734 so we need to do this dirty hack...
      NavigableSet<Path> filesUnderWrongRegionWALDir =
        WALSplitUtil.getSplitEditFilesSorted(walFS, wrongRegionWALDir);
      seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, walFS,
        filesUnderWrongRegionWALDir, reporter, regionDir));
      // This is to ensure backwards compatability with HBASE-20723 where recovered edits can appear
      // under the root dir even if walDir is set.
      NavigableSet<Path> filesUnderRootDir = Collections.emptyNavigableSet();
      if (!regionWALDir.equals(regionDir)) {
        filesUnderRootDir = WALSplitUtil.getSplitEditFilesSorted(rootFS, regionDir);
        seqId = Math.max(seqId, replayRecoveredEditsForPaths(minSeqIdForTheRegion, rootFS,
          filesUnderRootDir, reporter, regionDir));
      }

      NavigableSet<Path> files = WALSplitUtil.getSplitEditFilesSorted(walFS, regionWALDir);
      seqId = Math.max(seqId,
        replayRecoveredEditsForPaths(minSeqIdForTheRegion, walFS, files, reporter, regionWALDir));
      if (seqId > minSeqIdForTheRegion) {
        // Then we added some edits to memory. Flush and cleanup split edit files.
        internalFlushcache(null, seqId, stores.values(), status, false,
          FlushLifeCycleTracker.DUMMY);
      }
      // Now delete the content of recovered edits. We're done w/ them.
      if (files.size() > 0 && this.conf.getBoolean("hbase.region.archive.recovered.edits", false)) {
        // For debugging data loss issues!
        // If this flag is set, make use of the hfile archiving by making recovered.edits a fake
        // column family. Have to fake out file type too by casting our recovered.edits as
        // storefiles
        String fakeFamilyName = WALSplitUtil.getRegionDirRecoveredEditsDir(regionWALDir).getName();
        Set<HStoreFile> fakeStoreFiles = new HashSet<>(files.size());
        for (Path file : files) {
          fakeStoreFiles.add(new HStoreFile(walFS, file, this.conf, null, null, true));
        }
        getRegionWALFileSystem().archiveRecoveredEdits(fakeFamilyName, fakeStoreFiles);
      } else {
        deleteRecoveredEdits(walFS, Iterables.concat(files, filesUnderWrongRegionWALDir));
        deleteRecoveredEdits(rootFS, filesUnderRootDir);
      }
    } else {
      Path recoveredEditsDir = new Path(specialRecoveredEditsDirStr);
      FileSystem fs = recoveredEditsDir.getFileSystem(conf);
      FileStatus[] files = fs.listStatus(recoveredEditsDir);
      LOG.debug("Found {} recovered edits file(s) under {}", files == null ? 0 : files.length,
        recoveredEditsDir);
      if (files != null) {
        for (FileStatus file : files) {
          // it is safe to trust the zero-length in this case because we've been through rename and
          // lease recovery in the above.
          if (isZeroLengthThenDelete(fs, file, file.getPath())) {
            continue;
          }
          seqId =
            Math.max(seqId, replayRecoveredEdits(file.getPath(), maxSeqIdInStores, reporter, fs));
        }
      }
      if (seqId > minSeqIdForTheRegion) {
        // Then we added some edits to memory. Flush and cleanup split edit files.
        internalFlushcache(null, seqId, stores.values(), status, false,
          FlushLifeCycleTracker.DUMMY);
      }
      deleteRecoveredEdits(fs,
        Stream.of(files).map(FileStatus::getPath).collect(Collectors.toList()));
    }

    return seqId;
  }

  private long replayRecoveredEditsForPaths(long minSeqIdForTheRegion, FileSystem fs,
    final NavigableSet<Path> files, final CancelableProgressable reporter, final Path regionDir)
    throws IOException {
    long seqid = minSeqIdForTheRegion;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + (files == null ? 0 : files.size()) + " recovered edits file(s) under "
        + regionDir);
    }

    if (files == null || files.isEmpty()) {
      return minSeqIdForTheRegion;
    }

    for (Path edits : files) {
      if (edits == null || !fs.exists(edits)) {
        LOG.warn("Null or non-existent edits file: " + edits);
        continue;
      }
      if (isZeroLengthThenDelete(fs, fs.getFileStatus(edits), edits)) {
        continue;
      }

      long maxSeqId;
      String fileName = edits.getName();
      maxSeqId = Math.abs(Long.parseLong(fileName));
      if (maxSeqId <= minSeqIdForTheRegion) {
        if (LOG.isDebugEnabled()) {
          String msg = "Maximum sequenceid for this wal is " + maxSeqId
            + " and minimum sequenceid for the region " + this + "  is " + minSeqIdForTheRegion
            + ", skipped the whole file, path=" + edits;
          LOG.debug(msg);
        }
        continue;
      }

      try {
        // replay the edits. Replay can return -1 if everything is skipped, only update
        // if seqId is greater
        seqid = Math.max(seqid, replayRecoveredEdits(edits, maxSeqIdInStores, reporter, fs));
      } catch (IOException e) {
        handleException(fs, edits, e);
      }
    }
    return seqid;
  }

  private void handleException(FileSystem fs, Path edits, IOException e) throws IOException {
    boolean skipErrors = conf.getBoolean(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS,
      conf.getBoolean("hbase.skip.errors", HConstants.DEFAULT_HREGION_EDITS_REPLAY_SKIP_ERRORS));
    if (((KnobRuntime.check(java.util.UUID.fromString("fa024a3f-87ac-3a33-b189-0958e3a7107d"))) ? ((conf.get("hbase.skip.errors")) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("df5a2281-7e0a-3248-b83a-a5f9c83461ad"))) ? ((conf.get("hbase.skip.errors")) == (null)) : (conf.get("hbase.skip.errors") != null))))) {
      LOG.warn("The property 'hbase.skip.errors' has been deprecated. Please use "
        + HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS + " instead.");
    }
    if (skipErrors) {
if(KnobRuntime.check(java.util.UUID.fromString("f3a8d165-d3db-3341-a952-c03fab7a5e15"))) {
throw new java.io.IOException("Injected exception");
}
      Path p = WALSplitUtil.moveAsideBadEditsFile(fs, edits);
      LOG.error(HConstants.HREGION_EDITS_REPLAY_SKIP_ERRORS + "=true so continuing. Renamed "
        + edits + " as " + p, e);
    } else {
      throw e;
    }
  }

  /**
   * @param edits            File of recovered edits.
   * @param maxSeqIdInStores Maximum sequenceid found in each store. Edits in wal must be larger
   *                         than this to be replayed for each store.
   * @return the sequence id of the last edit added to this region out of the recovered edits log or
   *         <code>minSeqId</code> if nothing added from editlogs.
   */
  private long replayRecoveredEdits(final Path edits, Map<byte[], Long> maxSeqIdInStores,
    final CancelableProgressable reporter, FileSystem fs) throws IOException {
    String msg = "Replaying edits from " + edits;
    if (KnobRuntime.check(java.util.UUID.fromString("aefde71e-3f8b-3272-be5f-1efccdf0c990"))) { LOG.error(getRegionInfo().getEncodedName() + " : "); } else if (KnobRuntime.check(java.util.UUID.fromString("96e7b12e-6c01-3deb-83da-436a3aae758c"))) { LOG.error(getRegionInfo().getEncodedName() + " : " + "Found decreasing SeqId. PreId="); } else if (KnobRuntime.check(java.util.UUID.fromString("29e73096-ffb2-3178-9404-a3329b900a94"))) { LOG.debug(getRegionInfo().getEncodedName() + " : " + "Found decreasing SeqId. PreId="); } else if (KnobRuntime.check(java.util.UUID.fromString("2901fca8-698b-3374-b272-c4b23563532d"))) { LOG.debug(getRegionInfo().getEncodedName() + " : "); } else if (KnobRuntime.check(java.util.UUID.fromString("28ff52a4-a6b6-3838-9f76-cce16be45e64"))) { LOG.info(getRegionInfo().getEncodedName() + " : " + "Found decreasing SeqId. PreId="); } else if (KnobRuntime.check(java.util.UUID.fromString("ec68123f-612e-3b52-bdfc-c1221718f21c"))) { LOG.info(getRegionInfo().getEncodedName() + " : "); } else if (KnobRuntime.check(java.util.UUID.fromString("499c4f06-9d78-39a4-b5f4-1367b702882b"))) { LOG.info(getRegionInfo().getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("d698dff9-fc17-3f7a-850c-3874990a5f42"))) { LOG.error(getRegionInfo().getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("970357cc-81a4-388c-b00a-3763d23cc60d"))) { LOG.debug(getRegionInfo().getEncodedName()); } else { LOG.info(msg); }
    MonitoredTask status = TaskMonitor.get().createStatus(msg);

    if (KnobRuntime.check(java.util.UUID.fromString("c53833da-6a03-3d70-bd0e-8eafe983dbfc"))) { status.abort("Opening recovered edits"); } else { status.setStatus("Opening recovered edits"); }
if(KnobRuntime.check(java.util.UUID.fromString("896cd207-5351-340d-bc6b-5aa3780ce478"))) {
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
    try (WALStreamReader reader = WALFactory.createStreamReader(fs, edits, conf)) {
      long currentEditSeqId = -1;
      long currentReplaySeqId = -1;
      long firstSeqIdInLog = -1;
      long skippedEdits = 0;
      long editsCount = 0;
      long intervalEdits = 0;
      WAL.Entry entry;
      HStore store = null;
      boolean reported_once = false;
      ServerNonceManager ng = this.rsServices == null ? null : this.rsServices.getNonceManager();

      try {
        // How many edits seen before we check elapsed time
        int interval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
        // How often to send a progress report (default 1/2 master timeout)
        int period = this.conf.getInt("hbase.hstore.report.period", 300000);
        long lastReport = EnvironmentEdgeManager.currentTime();

        if (coprocessorHost != null) {
          coprocessorHost.preReplayWALs(this.getRegionInfo(), edits);
        }

if(KnobRuntime.check(java.util.UUID.fromString("a0ee4a01-37b2-3d50-bb26-11fcf9829d29"))) {
throw new java.io.IOException("Injected exception");
}
        while (((KnobRuntime.check(java.util.UUID.fromString("ebd7c066-0982-3636-970d-eef0760b4a3e"))) ? (((entry = reader.next())) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5f9a641f-6b94-3fe8-837a-8ccf16dd9ae4"))) ? (((entry = reader.next())) == (null)) : ((entry = reader.next()) != null))))) {
          WALKey key = entry.getKey();
          WALEdit val = entry.getEdit();

          if (ng != null) { // some test, or nonces disabled
            if (KnobRuntime.check(java.util.UUID.fromString("f9e5ceb7-f848-31d1-96f1-1969da13f6d2"))) { ng.reportOperationFromWal(EnvironmentEdgeManager.currentTime(), key.getNonce(), key.getWriteTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("aaf6bf59-764d-31f5-bde0-8b5db4c414b4"))) { ng.reportOperationFromWal(key.getNonceGroup(), EnvironmentEdgeManager.currentTime(), key.getWriteTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("2a99ffca-345c-3655-aae5-2f86c02085ba"))) { ng.reportOperationFromWal(key.getNonceGroup(), key.getNonce(), EnvironmentEdgeManager.currentTime()); } else { ng.reportOperationFromWal(key.getNonceGroup(), key.getNonce(), key.getWriteTime()); }
          }

          if (((KnobRuntime.check(java.util.UUID.fromString("6fa943c2-b274-3152-9061-ca9f1fd06873"))) ? ((reporter) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("478b20d0-6daf-32de-90fe-85aba27700b0"))) ? ((reporter) != (null)) : (reporter != null))))) {
            intervalEdits += val.size();
            if (((KnobRuntime.check(java.util.UUID.fromString("89337e1e-27c2-317e-8025-394bb8084d7d"))) ? ((intervalEdits) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bf966ef9-3218-30af-b9bd-977daa8667f1"))) ? ((intervalEdits) != (2000)) : (((KnobRuntime.check(java.util.UUID.fromString("b304da6b-5135-3acf-bf7a-b032326bbf93"))) ? ((intervalEdits) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("550ed427-81b4-325b-a657-8a37e2a6eb86"))) ? ((intervalEdits) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1d982c3a-c738-3b8b-8975-77f6c360fddc"))) ? ((intervalEdits) <= (2000)) : (((KnobRuntime.check(java.util.UUID.fromString("ca0801e5-1d9a-3536-ac1e-9e642009abc2"))) ? ((intervalEdits) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e44aada7-8d6f-33be-af3a-7052b8f20917"))) ? ((intervalEdits) > (300000)) : (((KnobRuntime.check(java.util.UUID.fromString("cfe14164-317e-3c07-9482-d59a28d2c719"))) ? ((intervalEdits) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("8e97c83b-bd78-3443-92df-60f5f832586f"))) ? ((intervalEdits) == (2000)) : (((KnobRuntime.check(java.util.UUID.fromString("4541e40e-339f-3bc0-a142-db99fc66326c"))) ? ((intervalEdits) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("83e9af58-cc81-3fc3-b552-35a9d41ea52f"))) ? ((intervalEdits) <= (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("28b760a5-bd7d-38d1-8fbe-a59ec603f462"))) ? ((intervalEdits) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("9352d87b-843c-318b-ae11-7dc95c844ad5"))) ? ((intervalEdits) < (2000)) : (((KnobRuntime.check(java.util.UUID.fromString("40003d18-3bc8-3219-8b0f-5cc059c49654"))) ? ((intervalEdits) < (300000)) : (((KnobRuntime.check(java.util.UUID.fromString("f0126097-5b54-3ae7-ba11-4cfb7ff135b1"))) ? ((intervalEdits) > (interval)) : (((KnobRuntime.check(java.util.UUID.fromString("b45d6a94-4983-3e46-84de-48ca07c4c4ac"))) ? ((intervalEdits) < (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("10a80b52-ef18-3067-9d9c-c398d99f029b"))) ? ((intervalEdits) >= (300000)) : (((KnobRuntime.check(java.util.UUID.fromString("811aeeba-bd29-3100-9747-3ed93b1ad377"))) ? ((intervalEdits) != (interval)) : (((KnobRuntime.check(java.util.UUID.fromString("748b5f56-f3d1-3569-b2c1-3c0c43758786"))) ? ((intervalEdits) >= (interval)) : (((KnobRuntime.check(java.util.UUID.fromString("3f0732c5-1c3f-3bb8-80d0-d852bb5ac867"))) ? ((intervalEdits) == (interval)) : (((KnobRuntime.check(java.util.UUID.fromString("b8c3b4d6-90bb-3db6-9bc0-7a885f563ec1"))) ? ((intervalEdits) == (300000)) : (((KnobRuntime.check(java.util.UUID.fromString("ec488a13-e39a-32c2-aaa9-276e0f771428"))) ? ((intervalEdits) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("e6eb36cd-7028-3461-b59b-639869273067"))) ? ((intervalEdits) <= (300000)) : (((KnobRuntime.check(java.util.UUID.fromString("bfa826de-e235-36b9-b332-3b6b7db70919"))) ? ((intervalEdits) < (interval)) : (((KnobRuntime.check(java.util.UUID.fromString("4fe3a128-bbeb-32b2-82ea-4e21257d8229"))) ? ((intervalEdits) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("99ac2358-fab1-3542-9997-bbe7c1344e38"))) ? ((intervalEdits) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("39fb5d8e-7641-36c0-b00b-132eefba2802"))) ? ((intervalEdits) <= (interval)) : (((KnobRuntime.check(java.util.UUID.fromString("9dfb3acd-a13d-3c42-9dd2-2fca54fda4da"))) ? ((intervalEdits) != (300000)) : (((KnobRuntime.check(java.util.UUID.fromString("4a7180e4-b341-336c-801f-1eddc0c23f65"))) ? ((intervalEdits) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("1cf5754d-521b-3daa-a961-e0e5bfee3a16"))) ? ((intervalEdits) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("50e1d8a4-d1ce-3602-bd3d-ad43332b1562"))) ? ((intervalEdits) > (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("03c20ccf-6f02-33f0-9538-0db82a7add64"))) ? ((intervalEdits) >= (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("6442c0af-c0cd-3df6-bf4f-5c67956e2360"))) ? ((intervalEdits) > (2000)) : (((KnobRuntime.check(java.util.UUID.fromString("c4a42104-6c9f-3ee3-b778-a58b6041b50c"))) ? ((intervalEdits) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ad06c77a-3b5b-382b-9a1e-e07e9ef5ca1a"))) ? ((intervalEdits) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8ef0b892-ad60-3a10-8a62-123bb19b50be"))) ? ((intervalEdits) >= (2000)) : (intervalEdits >= interval))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
              // Number of edits interval reached
              intervalEdits = 0;
              long cur = EnvironmentEdgeManager.currentTime();
              if (lastReport + period <= cur) {
                status.setStatus(
                  "Replaying edits..." + " skipped=" + skippedEdits + " edits=" + editsCount);
                // Timeout reached
                if (!reporter.progress()) {
                  msg = "Progressable reporter failed, stopping replay for region " + this;
                  if (KnobRuntime.check(java.util.UUID.fromString("9fad6cbf-b99e-3169-be33-779c24e220f6"))) { LOG.debug(getRegionInfo().getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("758c90c3-b691-3d6c-b30b-fa563bfcbbda"))) { LOG.info(getRegionInfo().getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("f300301e-33d0-3b57-bb34-04a915a0fc94"))) { LOG.error(getRegionInfo().getEncodedName() + " : " + "Found decreasing SeqId. PreId="); } else if (KnobRuntime.check(java.util.UUID.fromString("f5f03166-88d4-3f4c-b709-d4164e65c4ae"))) { LOG.info(getRegionInfo().getEncodedName() + " : "); } else if (KnobRuntime.check(java.util.UUID.fromString("90da6cf1-5674-3a98-8bb6-90c16f67ddec"))) { LOG.debug(getRegionInfo().getEncodedName() + " : "); } else if (KnobRuntime.check(java.util.UUID.fromString("09968471-7800-316f-bd5c-226da127c502"))) { LOG.debug(getRegionInfo().getEncodedName() + " : " + "Found decreasing SeqId. PreId="); } else if (KnobRuntime.check(java.util.UUID.fromString("c50547f2-3ac1-33f2-bbb1-a31f4d84f54b"))) { LOG.error(getRegionInfo().getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("f97ab000-2cc9-3973-b30d-bd4b6405db87"))) { LOG.info(getRegionInfo().getEncodedName() + " : " + "Found decreasing SeqId. PreId="); } else if (KnobRuntime.check(java.util.UUID.fromString("a3fa9cbb-c64e-3cd9-9498-71e649636b0e"))) { LOG.error(getRegionInfo().getEncodedName() + " : "); } else { LOG.warn(msg); }
                  status.abort(msg);
                  throw new IOException(msg);
                }
                reported_once = true;
                lastReport = cur;
              }
            }
          }

          if (firstSeqIdInLog == -1) {
            firstSeqIdInLog = key.getSequenceId();
          }
          if (currentEditSeqId > key.getSequenceId()) {
            // when this condition is true, it means we have a serious defect because we need to
            // maintain increasing SeqId for WAL edits per region
            LOG.error(getRegionInfo().getEncodedName() + " : " + "Found decreasing SeqId. PreId="
              + currentEditSeqId + " key=" + key + "; edit=" + val);
          } else {
            currentEditSeqId = key.getSequenceId();
          }
          currentReplaySeqId =
            (key.getOrigLogSeqNum() > 0) ? key.getOrigLogSeqNum() : currentEditSeqId;

          // Start coprocessor replay here. The coprocessor is for each WALEdit
          // instead of a KeyValue.
          if (((KnobRuntime.check(java.util.UUID.fromString("5acb1524-944d-3994-9db8-e1d6bbe627bb"))) ? ((coprocessorHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9e0699d6-a1fb-38be-880d-1a025dde7edf"))) ? ((coprocessorHost) == (null)) : (coprocessorHost != null))))) {
            if (KnobRuntime.check(java.util.UUID.fromString("17df3c0b-9f05-3acd-bedf-2927678475cd"))) { status.abort("Running pre-WAL-restore hook in coprocessors"); } else { status.setStatus("Running pre-WAL-restore hook in coprocessors"); }
            if (coprocessorHost.preWALRestore(this.getRegionInfo(), key, val)) {
              // if bypass this wal entry, ignore it ...
if(KnobRuntime.check(java.util.UUID.fromString("a519f267-10fb-3d81-82fa-3d6dbf55eef0"))) {
throw new java.io.IOException("Injected exception");
}
              continue;
            }
          }
          boolean checkRowWithinBoundary = false;
          // Check this edit is for this region.
          if (
            !Bytes.equals(key.getEncodedRegionName(), this.getRegionInfo().getEncodedNameAsBytes())
          ) {
            checkRowWithinBoundary = true;
          }

          boolean flush = false;
          MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
          for (Cell cell : val.getCells()) {
            // Check this edit is for me. Also, guard against writing the special
            // METACOLUMN info such as HBASE::CACHEFLUSH entries
            if (WALEdit.isMetaEditFamily(cell)) {
              // if region names don't match, skipp replaying compaction marker
              if (!checkRowWithinBoundary) {
                // this is a special edit, we should handle it
                CompactionDescriptor compaction = WALEdit.getCompaction(cell);
                if (compaction != null) {
                  // replay the compaction
                  replayWALCompactionMarker(compaction, false, true, Long.MAX_VALUE);
                }
              }
              skippedEdits++;
              continue;
            }
            // Figure which store the edit is meant for.
            if (
              store == null
                || !CellUtil.matchingFamily(cell, store.getColumnFamilyDescriptor().getName())
            ) {
              store = getStore(cell);
            }
            if (store == null) {
              // This should never happen. Perhaps schema was changed between
              // crash and redeploy?
              LOG.warn("No family for cell {} in region {}", cell, this);
              skippedEdits++;
              continue;
            }
            if (
              checkRowWithinBoundary && !rowIsInRange(this.getRegionInfo(), cell.getRowArray(),
                cell.getRowOffset(), cell.getRowLength())
            ) {
              LOG.warn("Row of {} is not within region boundary for region {}", cell, this);
              skippedEdits++;
              continue;
            }
            // Now, figure if we should skip this edit.
            if (
              key.getSequenceId()
                  <= maxSeqIdInStores.get(store.getColumnFamilyDescriptor().getName())
            ) {
              skippedEdits++;
              continue;
            }
            PrivateCellUtil.setSequenceId(cell, currentReplaySeqId);

            restoreEdit(store, cell, memStoreSizing);
            editsCount++;
          }
          MemStoreSize mss = memStoreSizing.getMemStoreSize();
if(KnobRuntime.check(java.util.UUID.fromString("132d982b-500d-31be-831a-1f576a2b97ed"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e841f665-139b-3adf-bbf1-ef327842dd06"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e62dcda-0c92-3d18-aae8-a53907e380d7"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ccc12af3-17ed-3dec-b84c-9a6d4933c153"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ffecc892-3753-30ae-a219-67f2bd5f0afc"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("902d0225-c1e1-30de-9d17-0e55dbe6df6d"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("baffa6cb-3bba-33ab-a1e0-59afb7007e33"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4590f8f3-dfdc-39c3-a34e-4e84ecb4f5c3"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8aa9b5f4-b492-361f-b105-b9aaf373d0d8"))) {
try {
    java.lang.reflect.Field field = mss.getClass().getDeclaredField("offHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mss));
    field.set(mss, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("0684e431-82ad-3774-9e0c-2413c1d9fe54"))) { decrMemStoreSize(mss); } else { incMemStoreSize(mss); }
          flush = isFlushSize(this.memStoreSizing.getMemStoreSize());
          if (flush) {
            internalFlushcache(null, currentEditSeqId, stores.values(), status, false,
              FlushLifeCycleTracker.DUMMY);
          }

          if (((KnobRuntime.check(java.util.UUID.fromString("ac265e07-18c8-33a8-bd6d-31dfd2a4404d"))) ? ((coprocessorHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1a36dfb7-5ef0-3c12-a1d8-e283e9377dcc"))) ? ((coprocessorHost) != (null)) : (coprocessorHost != null))))) {
            coprocessorHost.postWALRestore(this.getRegionInfo(), key, val);
          }
        }

        if (coprocessorHost != null) {
if(KnobRuntime.check(java.util.UUID.fromString("fe34d61a-4d08-3609-9625-adb092c8b81b"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("c0a5ff2a-5ffc-3848-a050-04e3023e3a4b"))) { coprocessorHost.postReplayWALs(getRegionInfo(), edits); } else if (KnobRuntime.check(java.util.UUID.fromString("dead8358-bdee-3781-bda6-ef17c1541f40"))) { coprocessorHost.preReplayWALs(getRegionInfo(), edits); } else { coprocessorHost.postReplayWALs(this.getRegionInfo(), edits); }
        }
      } catch (EOFException eof) {
        if (!conf.getBoolean(RECOVERED_EDITS_IGNORE_EOF, false)) {
if(KnobRuntime.check(java.util.UUID.fromString("a6cfc7bf-cdf9-35dd-9247-4bd81b09887b"))) {
throw new java.io.IOException("Injected exception");
}
          Path p = WALSplitUtil.moveAsideBadEditsFile(walFS, edits);
          msg = "EnLongAddered EOF. Most likely due to Master failure during "
            + "wal splitting, so we have this data in another edit. Continuing, but renaming "
            + edits + " as " + p + " for region " + this;
          LOG.warn(msg, eof);
          status.abort(msg);
        } else {
          LOG.warn("EOF while replaying recover edits and config '{}' is true so "
            + "we will ignore it and continue", RECOVERED_EDITS_IGNORE_EOF, eof);
        }
      } catch (IOException ioe) {
        // If the IOE resulted from bad file format,
        // then this problem is idempotent and retrying won't help
        if (ioe.getCause() instanceof ParseException) {
          Path p = WALSplitUtil.moveAsideBadEditsFile(walFS, edits);
          msg =
            "File corruption enLongAddered!  " + "Continuing, but renaming " + edits + " as " + p;
          LOG.warn(msg, ioe);
          status.setStatus(msg);
        } else {
          status.abort(StringUtils.stringifyException(ioe));
          // other IO errors may be transient (bad network connection,
          // checksum exception on one datanode, etc). throw & retry
          throw ioe;
        }
      }
      if (reporter != null && !reported_once) {
        reporter.progress();
      }
      msg = "Applied " + editsCount + ", skipped " + skippedEdits + ", firstSequenceIdInLog="
        + firstSeqIdInLog + ", maxSequenceIdInLog=" + currentEditSeqId + ", path=" + edits;
      status.markComplete(msg);
      LOG.debug(msg);
      return currentEditSeqId;
    } finally {
      status.cleanup();
    }
  }

  /**
   * Call to complete a compaction. Its for the case where we find in the WAL a compaction that was
   * not finished. We could find one recovering a WAL after a regionserver crash. See HBASE-2331.
   */
  void replayWALCompactionMarker(CompactionDescriptor compaction, boolean pickCompactionFiles,
    boolean removeFiles, long replaySeqId) throws IOException {
    try {
      checkTargetRegion(compaction.getEncodedRegionName().toByteArray(),
        "Compaction marker from WAL ", compaction);
    } catch (WrongRegionException wre) {
      if (RegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
        // skip the compaction marker since it is not for this region
        return;
      }
      throw wre;
    }

    synchronized (writestate) {
      if (replaySeqId < lastReplayedOpenRegionSeqId) {
        LOG.warn(getRegionInfo().getEncodedName() + " : " + "Skipping replaying compaction event :"
          + TextFormat.shortDebugString(compaction) + " because its sequence id " + replaySeqId
          + " is smaller than this regions " + "lastReplayedOpenRegionSeqId of "
          + lastReplayedOpenRegionSeqId);
        return;
      }
      if (replaySeqId < lastReplayedCompactionSeqId) {
        LOG.warn(getRegionInfo().getEncodedName() + " : " + "Skipping replaying compaction event :"
          + TextFormat.shortDebugString(compaction) + " because its sequence id " + replaySeqId
          + " is smaller than this regions " + "lastReplayedCompactionSeqId of "
          + lastReplayedCompactionSeqId);
        return;
      } else {
        lastReplayedCompactionSeqId = replaySeqId;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(getRegionInfo().getEncodedName() + " : " + "Replaying compaction marker "
          + TextFormat.shortDebugString(compaction) + " with seqId=" + replaySeqId
          + " and lastReplayedOpenRegionSeqId=" + lastReplayedOpenRegionSeqId);
      }

if(KnobRuntime.check(java.util.UUID.fromString("885d9c11-7ddb-3e00-8f53-80f56e6cd78f"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("5b1b9409-8e19-3112-b743-14d4cb6a4b1d"))) { closeRegionOperation(Operation.REPLAY_EVENT); } else { startRegionOperation(Operation.REPLAY_EVENT); }
      try {
        HStore store = this.getStore(compaction.getFamilyName().toByteArray());
        if (((KnobRuntime.check(java.util.UUID.fromString("0b93c5d8-cf03-325c-b72d-8ea9a130e12d"))) ? ((store) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("12c788c4-90cd-3335-ac1c-c4ff859f3658"))) ? ((store) != (null)) : (store == null))))) {
          LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Found Compaction WAL edit for deleted family:"
            + Bytes.toString(compaction.getFamilyName().toByteArray()));
          return;
        }
if(KnobRuntime.check(java.util.UUID.fromString("d4cdb0ec-2bb1-3181-9e8e-77410a7dd555"))) {
try {
    java.lang.reflect.Field field = compaction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compaction));
    field.set(compaction, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb4a4a47-9e0d-323d-a5d2-56621ac8af6d"))) {
try {
    java.lang.reflect.Field field = compaction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compaction));
    field.set(compaction, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("954144d6-c149-3616-a73f-5e24e07d7bca"))) {
pickCompactionFiles = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c8d2643-c8e2-324a-88dc-15e172a95eda"))) {
pickCompactionFiles = !pickCompactionFiles;
}
if(KnobRuntime.check(java.util.UUID.fromString("174381da-eb3c-3239-a0d9-9e3813611918"))) {
removeFiles = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("4470126f-d2a8-3085-bce6-42776b913266"))) {
removeFiles = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("19a85706-036b-3ca1-ac1b-aacfaab7929a"))) {
pickCompactionFiles = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f0e7062-e9f0-3e60-b6a2-251a19141716"))) {
removeFiles = !removeFiles;
}
if(KnobRuntime.check(java.util.UUID.fromString("a6ae707d-245e-3acb-9d9e-7932b4364c1a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("20e93f19-ad48-3b33-a8f5-b2e0fb0a756b"))) {
try {
    java.lang.reflect.Field field = compaction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compaction));
    field.set(compaction, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba6c95b2-d314-3659-a26d-ba529475fdf0"))) {
try {
    java.lang.reflect.Field field = compaction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compaction));
    field.set(compaction, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("e66d993e-6ba3-3300-a341-c5fbde3662ae"))) { store.replayCompactionMarker(compaction, !pickCompactionFiles, removeFiles); } else if (KnobRuntime.check(java.util.UUID.fromString("ee8476a9-9d65-326d-b45e-d560b27cb891"))) { store.replayCompactionMarker(compaction, pickCompactionFiles, !removeFiles); } else { store.replayCompactionMarker(compaction, pickCompactionFiles, removeFiles); }
        if (KnobRuntime.check(java.util.UUID.fromString("d4947e14-bc6a-333c-85a6-4eb65b4d5d3c"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("e9a18f98-3bff-31d3-9fb9-8c9281edeeeb"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("fcfa6895-1a0a-3f8e-a736-e5f29f3837fd"))) { waitForFlushesAndCompactions(); } else { logRegionFiles(); }
      } catch (FileNotFoundException ex) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "At least one of the store files in compaction: "
          + TextFormat.shortDebugString(compaction)
          + " doesn't exist any more. Skip loading the file(s)", ex);
      } finally {
if(KnobRuntime.check(java.util.UUID.fromString("bf288040-138d-36a4-bfd1-8af59a13abad"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("7612cb1c-1f0a-3a2f-9056-f52a4f1e98ae"))) { startRegionOperation(Operation.REPLAY_EVENT); } else { closeRegionOperation(Operation.REPLAY_EVENT); }
      }
    }
  }

  void replayWALFlushMarker(FlushDescriptor flush, long replaySeqId) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ee069912-33d9-38b6-959b-e0ff3853a2f9"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67e8a055-7562-381e-8097-a2e5f7650f7a"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("956d85ea-149f-3a3d-8e25-43568ef577a8"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6adccc97-a115-3f46-bf85-0b8499a150a6"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("flushSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flush));
    field.set(flush, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb3c7558-a96e-317c-a965-387ecf4b70c3"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a75707f9-344d-30be-8703-cb76829fab53"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cee3303-3c1e-341d-87cb-d0a49f395001"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("24fbe0b5-2d5b-3c40-bc55-3ebafb90cfb0"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11e3624f-d50a-3dff-a4db-a618c3a2033a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("39a64952-3595-3cef-90bc-9a6f90afc998"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b70d7e10-a656-3faa-8765-7286bce39d99"))) {
try {
    java.lang.reflect.Field field = flush.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flush));
    field.set(flush, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    checkTargetRegion(flush.getEncodedRegionName().toByteArray(), "Flush marker from WAL ", flush);

    if (ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
      return; // if primary nothing to do
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("fed04953-4d04-3f5d-8fcb-b1ce2bd2d0dc"))) ? (isClosed()) : (LOG.isDebugEnabled()))) {
      LOG.debug(getRegionInfo().getEncodedName() + " : " + "Replaying flush marker "
        + TextFormat.shortDebugString(flush));
    }

    startRegionOperation(Operation.REPLAY_EVENT); // use region close lock to guard against close
    try {
      FlushAction action = flush.getAction();
      switch (action) {
        case START_FLUSH:
          replayWALFlushStartMarker(flush);
          break;
        case COMMIT_FLUSH:
          replayWALFlushCommitMarker(flush);
          break;
        case ABORT_FLUSH:
          replayWALFlushAbortMarker(flush);
          break;
        case CANNOT_FLUSH:
          replayWALFlushCannotFlushMarker(flush, replaySeqId);
          break;
        default:
          LOG.warn(getRegionInfo().getEncodedName() + " : "
            + "Received a flush event with unknown action, ignoring. "
            + TextFormat.shortDebugString(flush));
          break;
      }

      logRegionFiles();
    } finally {
      closeRegionOperation(Operation.REPLAY_EVENT);
    }
  }

  /**
   * Replay the flush marker from primary region by creating a corresponding snapshot of the store
   * memstores, only if the memstores do not have a higher seqId from an earlier wal edit (because
   * the events may be coming out of order).
   */
  PrepareFlushResult replayWALFlushStartMarker(FlushDescriptor flush) throws IOException {
    long flushSeqId = flush.getFlushSequenceNumber();

    HashSet<HStore> storesToFlush = new HashSet<>();
    for (StoreFlushDescriptor storeFlush : flush.getStoreFlushesList()) {
      byte[] family = storeFlush.getFamilyName().toByteArray();
      HStore store = getStore(family);
      if (store == null) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "Received a flush start marker from primary, but the family is not found. Ignoring"
          + " StoreFlushDescriptor:" + TextFormat.shortDebugString(storeFlush));
        continue;
      }
      storesToFlush.add(store);
    }

    MonitoredTask status = TaskMonitor.get().createStatus("Preparing flush " + this);

    // we will use writestate as a coarse-grain lock for all the replay events
    // (flush, compaction, region open etc)
    synchronized (writestate) {
      try {
        if (((KnobRuntime.check(java.util.UUID.fromString("e6aa9bb7-9d14-3d0d-aff4-d6951c492aa6"))) ? ((flush.getFlushSequenceNumber()) <= (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("630841a2-cd94-3b6c-9d82-191270bf4383"))) ? ((EnvironmentEdgeManager.currentTime()) >= (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("388efece-740a-3eda-9c1b-6267ac755f6c"))) ? ((flush.getFlushSequenceNumber()) > (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("7668fe53-daf0-3408-8291-cb4fa1a96a73"))) ? ((flush.getFlushSequenceNumber()) != (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("b1904a28-45f9-303a-827c-ca5270b1f47a"))) ? ((flush.getFlushSequenceNumber()) >= (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("2fbcd1c5-2e09-37da-9162-ef7c8f89953d"))) ? ((flush.getFlushSequenceNumber()) == (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("3009eb40-6b92-3c0e-acf1-e20e7621011d"))) ? ((EnvironmentEdgeManager.currentTime()) > (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("e7af5f55-219b-3e6e-9f65-cbc8392769a3"))) ? ((EnvironmentEdgeManager.currentTime()) != (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("8e907ce3-0256-3f70-b4e2-0564ce14903d"))) ? ((flush.getFlushSequenceNumber()) < (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("815836eb-a3bb-3eee-9125-873f0c5155cf"))) ? ((EnvironmentEdgeManager.currentTime()) == (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("ec5e9c94-2442-39ce-9046-7a0c2f360ca7"))) ? ((EnvironmentEdgeManager.currentTime()) < (lastReplayedOpenRegionSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("f31d5f5d-4d74-3dd3-a320-5d12071a24fc"))) ? ((EnvironmentEdgeManager.currentTime()) <= (lastReplayedOpenRegionSeqId)) : (flush.getFlushSequenceNumber() < lastReplayedOpenRegionSeqId))))))))))))))))))))))))) {
          LOG.warn(getRegionInfo().getEncodedName() + " : " + "Skipping replaying flush event :"
            + TextFormat.shortDebugString(flush)
            + " because its sequence id is smaller than this regions lastReplayedOpenRegionSeqId "
            + " of " + lastReplayedOpenRegionSeqId);
          return null;
        }
        if (numMutationsWithoutWAL.sum() > 0) {
          numMutationsWithoutWAL.reset();
          dataInMemoryWithoutWAL.reset();
        }

        if (!writestate.flushing) {
          // we do not have an active snapshot and corresponding this.prepareResult. This means
          // we can just snapshot our memstores and continue as normal.

          // invoke prepareFlushCache. Send null as wal since we do not want the flush events in wal
          PrepareFlushResult prepareResult = internalPrepareFlushCache(null, flushSeqId,
            storesToFlush, status, false, FlushLifeCycleTracker.DUMMY);
          if (prepareResult.result == null) {
            // save the PrepareFlushResult so that we can use it later from commit flush
            this.writestate.flushing = true;
            this.prepareFlushResult = prepareResult;
            status.markComplete("Flush prepare successful");
            if (LOG.isDebugEnabled()) {
              LOG.debug(getRegionInfo().getEncodedName() + " : " + " Prepared flush with seqId:"
                + flush.getFlushSequenceNumber());
            }
          } else {
            // special case empty memstore. We will still save the flush result in this case, since
            // our memstore ie empty, but the primary is still flushing
            if (
              prepareResult.getResult().getResult()
                  == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY
            ) {
              this.writestate.flushing = true;
              this.prepareFlushResult = prepareResult;
              if (LOG.isDebugEnabled()) {
                LOG.debug(getRegionInfo().getEncodedName() + " : "
                  + " Prepared empty flush with seqId:" + flush.getFlushSequenceNumber());
              }
            }
            status.abort("Flush prepare failed with " + prepareResult.result);
            // nothing much to do. prepare flush failed because of some reason.
          }
          return prepareResult;
        } else {
          // we already have an active snapshot.
          if (flush.getFlushSequenceNumber() == this.prepareFlushResult.flushOpSeqId) {
            // They define the same flush. Log and continue.
            LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a flush prepare marker with the same seqId: "
              + +flush.getFlushSequenceNumber() + " before clearing the previous one with seqId: "
              + prepareFlushResult.flushOpSeqId + ". Ignoring");
            // ignore
          } else if (flush.getFlushSequenceNumber() < this.prepareFlushResult.flushOpSeqId) {
            // We received a flush with a smaller seqNum than what we have prepared. We can only
            // ignore this prepare flush request.
            LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a flush prepare marker with a smaller seqId: "
              + +flush.getFlushSequenceNumber() + " before clearing the previous one with seqId: "
              + prepareFlushResult.flushOpSeqId + ". Ignoring");
            // ignore
          } else {
            // We received a flush with a larger seqNum than what we have prepared
            LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a flush prepare marker with a larger seqId: "
              + +flush.getFlushSequenceNumber() + " before clearing the previous one with seqId: "
              + prepareFlushResult.flushOpSeqId + ". Ignoring");
            // We do not have multiple active snapshots in the memstore or a way to merge current
            // memstore snapshot with the contents and resnapshot for now. We cannot take
            // another snapshot and drop the previous one because that will cause temporary
            // data loss in the secondary. So we ignore this for now, deferring the resolution
            // to happen when we see the corresponding flush commit marker. If we have a memstore
            // snapshot with x, and later received another prepare snapshot with y (where x < y),
            // when we see flush commit for y, we will drop snapshot for x, and can also drop all
            // the memstore edits if everything in memstore is < y. This is the usual case for
            // RS crash + recovery where we might see consequtive prepare flush wal markers.
            // Otherwise, this will cause more memory to be used in secondary replica until a
            // further prapare + commit flush is seen and replayed.
          }
        }
      } finally {
        status.cleanup();
        writestate.notifyAll();
      }
    }
    return null;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
      justification = "Intentional; post memstore flush")
  void replayWALFlushCommitMarker(FlushDescriptor flush) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus("Committing flush " + this);

    // check whether we have the memstore snapshot with the corresponding seqId. Replay to
    // secondary region replicas are in order, except for when the region moves or then the
    // region server crashes. In those cases, we may receive replay requests out of order from
    // the original seqIds.
    synchronized (writestate) {
      try {
        if (flush.getFlushSequenceNumber() < lastReplayedOpenRegionSeqId) {
          LOG.warn(getRegionInfo().getEncodedName() + " : " + "Skipping replaying flush event :"
            + TextFormat.shortDebugString(flush)
            + " because its sequence id is smaller than this regions lastReplayedOpenRegionSeqId "
            + " of " + lastReplayedOpenRegionSeqId);
          return;
        }

        if (writestate.flushing) {
          PrepareFlushResult prepareFlushResult = this.prepareFlushResult;
          if (flush.getFlushSequenceNumber() == prepareFlushResult.flushOpSeqId) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(getRegionInfo().getEncodedName() + " : "
                + "Received a flush commit marker with seqId:" + flush.getFlushSequenceNumber()
                + " and a previous prepared snapshot was found");
            }
            // This is the regular case where we received commit flush after prepare flush
            // corresponding to the same seqId.
            replayFlushInStores(flush, prepareFlushResult, true);

            // Set down the memstore size by amount of flush.
            this.decrMemStoreSize(prepareFlushResult.totalFlushableSize.getMemStoreSize());
            this.prepareFlushResult = null;
            writestate.flushing = false;
          } else if (flush.getFlushSequenceNumber() < prepareFlushResult.flushOpSeqId) {
            // This should not happen normally. However, lets be safe and guard against these cases
            // we received a flush commit with a smaller seqId than what we have prepared
            // we will pick the flush file up from this commit (if we have not seen it), but we
            // will not drop the memstore
            LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a flush commit marker with smaller seqId: "
              + flush.getFlushSequenceNumber() + " than what we have prepared with seqId: "
              + prepareFlushResult.flushOpSeqId + ". Picking up new file, but not dropping"
              + "  prepared memstore snapshot");
            replayFlushInStores(flush, prepareFlushResult, false);

            // snapshot is not dropped, so memstore sizes should not be decremented
            // we still have the prepared snapshot, flushing should still be true
          } else {
            // This should not happen normally. However, lets be safe and guard against these cases
            // we received a flush commit with a larger seqId than what we have prepared
            // we will pick the flush file for this. We will also obtain the updates lock and
            // look for contents of the memstore to see whether we have edits after this seqId.
            // If not, we will drop all the memstore edits and the snapshot as well.
            LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a flush commit marker with larger seqId: "
              + flush.getFlushSequenceNumber() + " than what we have prepared with seqId: "
              + prepareFlushResult.flushOpSeqId + ". Picking up new file and dropping prepared"
              + " memstore snapshot");

            replayFlushInStores(flush, prepareFlushResult, true);

            // Set down the memstore size by amount of flush.
            this.decrMemStoreSize(prepareFlushResult.totalFlushableSize.getMemStoreSize());

            // Inspect the memstore contents to see whether the memstore contains only edits
            // with seqId smaller than the flush seqId. If so, we can discard those edits.
            dropMemStoreContentsForSeqId(flush.getFlushSequenceNumber(), null);

            this.prepareFlushResult = null;
            writestate.flushing = false;
          }
          // If we were waiting for observing a flush or region opening event for not showing
          // partial data after a secondary region crash, we can allow reads now. We can only make
          // sure that we are not showing partial data (for example skipping some previous edits)
          // until we observe a full flush start and flush commit. So if we were not able to find
          // a previous flush we will not enable reads now.
          this.setReadsEnabled(true);
        } else {
          LOG.warn(
            getRegionInfo().getEncodedName() + " : " + "Received a flush commit marker with seqId:"
              + flush.getFlushSequenceNumber() + ", but no previous prepared snapshot was found");
          // There is no corresponding prepare snapshot from before.
          // We will pick up the new flushed file
          replayFlushInStores(flush, null, false);

          // Inspect the memstore contents to see whether the memstore contains only edits
          // with seqId smaller than the flush seqId. If so, we can discard those edits.
          dropMemStoreContentsForSeqId(flush.getFlushSequenceNumber(), null);
        }

        status.markComplete("Flush commit successful");

        // Update the last flushed sequence id for region.
        this.maxFlushedSeqId = flush.getFlushSequenceNumber();

        // advance the mvcc read point so that the new flushed file is visible.
        mvcc.advanceTo(flush.getFlushSequenceNumber());

      } catch (FileNotFoundException ex) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "At least one of the store files in flush: " + TextFormat.shortDebugString(flush)
          + " doesn't exist any more. Skip loading the file(s)", ex);
      } finally {
        status.cleanup();
        writestate.notifyAll();
      }
    }

    // C. Finally notify anyone waiting on memstore to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }
  }

  /**
   * Replays the given flush descriptor by opening the flush files in stores and dropping the
   * memstore snapshots if requested.
   */
  private void replayFlushInStores(FlushDescriptor flush, PrepareFlushResult prepareFlushResult,
    boolean dropMemstoreSnapshot) throws IOException {
    for (StoreFlushDescriptor storeFlush : flush.getStoreFlushesList()) {
      byte[] family = storeFlush.getFamilyName().toByteArray();
      HStore store = getStore(family);
      if (store == null) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "Received a flush commit marker from primary, but the family is not found."
          + "Ignoring StoreFlushDescriptor:" + storeFlush);
        continue;
      }
      List<String> flushFiles = storeFlush.getFlushOutputList();
      StoreFlushContext ctx = null;
      long startTime = EnvironmentEdgeManager.currentTime();
      if (prepareFlushResult == null || prepareFlushResult.storeFlushCtxs == null) {
        ctx = store.createFlushContext(flush.getFlushSequenceNumber(), FlushLifeCycleTracker.DUMMY);
      } else {
        ctx = prepareFlushResult.storeFlushCtxs.get(family);
        startTime = prepareFlushResult.startTime;
      }

      if (ctx == null) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "Unexpected: flush commit marker received from store " + Bytes.toString(family)
          + " but no associated flush context. Ignoring");
        continue;
      }

      ctx.replayFlush(flushFiles, dropMemstoreSnapshot); // replay the flush

      // Record latest flush time
      this.lastStoreFlushTimeMap.put(store, startTime);
    }
  }

  private long loadRecoveredHFilesIfAny(Collection<HStore> stores) throws IOException {
    Path regionDir = fs.getRegionDir();
    long maxSeqId = -1;
    for (HStore store : stores) {
      String familyName = store.getColumnFamilyName();
      FileStatus[] files =
        WALSplitUtil.getRecoveredHFiles(fs.getFileSystem(), regionDir, familyName);
      if (files != null && files.length != 0) {
        for (FileStatus file : files) {
          Path filePath = file.getPath();
          // If file length is zero then delete it
          if (isZeroLengthThenDelete(fs.getFileSystem(), file, filePath)) {
            continue;
          }
          try {
            HStoreFile storefile = store.tryCommitRecoveredHFile(file.getPath());
            maxSeqId = Math.max(maxSeqId, storefile.getReader().getSequenceID());
          } catch (IOException e) {
            handleException(fs.getFileSystem(), filePath, e);
            continue;
          }
        }
        if (this.rsServices != null && store.needsCompaction()) {
          this.rsServices.getCompactionRequestor().requestCompaction(this, store,
            "load recovered hfiles request compaction", Store.PRIORITY_USER + 1,
            CompactionLifeCycleTracker.DUMMY, null);
        }
      }
    }
    return maxSeqId;
  }

  /**
   * Be careful, this method will drop all data in the memstore of this region. Currently, this
   * method is used to drop memstore to prevent memory leak when replaying recovered.edits while
   * opening region.
   */
  private MemStoreSize dropMemStoreContents() throws IOException {
    MemStoreSizing totalFreedSize = new NonThreadSafeMemStoreSizing();
    this.updatesLock.writeLock().lock();
    try {
      for (HStore s : stores.values()) {
        MemStoreSize memStoreSize = doDropStoreMemStoreContentsForSeqId(s, HConstants.NO_SEQNUM);
        LOG.info("Drop memstore for Store " + s.getColumnFamilyName() + " in region "
          + this.getRegionInfo().getRegionNameAsString() + " , dropped memstoresize: ["
          + memStoreSize + " }");
        totalFreedSize.incMemStoreSize(memStoreSize);
      }
      return totalFreedSize.getMemStoreSize();
    } finally {
      this.updatesLock.writeLock().unlock();
    }
  }

  /**
   * Drops the memstore contents after replaying a flush descriptor or region open event replay if
   * the memstore edits have seqNums smaller than the given seq id
   */
  private MemStoreSize dropMemStoreContentsForSeqId(long seqId, HStore store) throws IOException {
    MemStoreSizing totalFreedSize = new NonThreadSafeMemStoreSizing();
    this.updatesLock.writeLock().lock();
    try {

      long currentSeqId = mvcc.getReadPoint();
      if (seqId >= currentSeqId) {
        // then we can drop the memstore contents since everything is below this seqId
        LOG.info(getRegionInfo().getEncodedName() + " : "
          + "Dropping memstore contents as well since replayed flush seqId: " + seqId
          + " is greater than current seqId:" + currentSeqId);

        // Prepare flush (take a snapshot) and then abort (drop the snapshot)
        if (store == null) {
          for (HStore s : stores.values()) {
            totalFreedSize.incMemStoreSize(doDropStoreMemStoreContentsForSeqId(s, currentSeqId));
          }
        } else {
          totalFreedSize.incMemStoreSize(doDropStoreMemStoreContentsForSeqId(store, currentSeqId));
        }
      } else {
        LOG.info(getRegionInfo().getEncodedName() + " : "
          + "Not dropping memstore contents since replayed flush seqId: " + seqId
          + " is smaller than current seqId:" + currentSeqId);
      }
    } finally {
      this.updatesLock.writeLock().unlock();
    }
    return totalFreedSize.getMemStoreSize();
  }

  private MemStoreSize doDropStoreMemStoreContentsForSeqId(HStore s, long currentSeqId)
    throws IOException {
    MemStoreSize flushableSize = s.getFlushableSize();
    this.decrMemStoreSize(flushableSize);
    StoreFlushContext ctx = s.createFlushContext(currentSeqId, FlushLifeCycleTracker.DUMMY);
    ctx.prepare();
    ctx.abort();
    return flushableSize;
  }

  private void replayWALFlushAbortMarker(FlushDescriptor flush) {
    // nothing to do for now. A flush abort will cause a RS abort which means that the region
    // will be opened somewhere else later. We will see the region open event soon, and replaying
    // that will drop the snapshot
  }

  private void replayWALFlushCannotFlushMarker(FlushDescriptor flush, long replaySeqId) {
    synchronized (writestate) {
      if (this.lastReplayedOpenRegionSeqId > replaySeqId) {
        LOG.warn(getRegionInfo().getEncodedName() + " : " + "Skipping replaying flush event :"
          + TextFormat.shortDebugString(flush) + " because its sequence id " + replaySeqId
          + " is smaller than this regions " + "lastReplayedOpenRegionSeqId of "
          + lastReplayedOpenRegionSeqId);
        return;
      }

      // If we were waiting for observing a flush or region opening event for not showing partial
      // data after a secondary region crash, we can allow reads now. This event means that the
      // primary was not able to flush because memstore is empty when we requested flush. By the
      // time we observe this, we are guaranteed to have up to date seqId with our previous
      // assignment.
      this.setReadsEnabled(true);
    }
  }

  PrepareFlushResult getPrepareFlushResult() {
    return prepareFlushResult;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
      justification = "Intentional; cleared the memstore")
  void replayWALRegionEventMarker(RegionEventDescriptor regionEvent) throws IOException {
    checkTargetRegion(regionEvent.getEncodedRegionName().toByteArray(),
      "RegionEvent marker from WAL ", regionEvent);

    startRegionOperation(Operation.REPLAY_EVENT);
    try {
      if (ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
        return; // if primary nothing to do
      }

      if (regionEvent.getEventType() == EventType.REGION_CLOSE) {
        // nothing to do on REGION_CLOSE for now.
        return;
      }
      if (regionEvent.getEventType() != EventType.REGION_OPEN) {
        LOG.warn(getRegionInfo().getEncodedName() + " : "
          + "Unknown region event received, ignoring :" + TextFormat.shortDebugString(regionEvent));
        return;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(getRegionInfo().getEncodedName() + " : " + "Replaying region open event marker "
          + TextFormat.shortDebugString(regionEvent));
      }

      // we will use writestate as a coarse-grain lock for all the replay events
      synchronized (writestate) {
        // Replication can deliver events out of order when primary region moves or the region
        // server crashes, since there is no coordination between replication of different wal files
        // belonging to different region servers. We have to safe guard against this case by using
        // region open event's seqid. Since this is the first event that the region puts (after
        // possibly flushing recovered.edits), after seeing this event, we can ignore every edit
        // smaller than this seqId
        if (this.lastReplayedOpenRegionSeqId <= regionEvent.getLogSequenceNumber()) {
          this.lastReplayedOpenRegionSeqId = regionEvent.getLogSequenceNumber();
        } else {
          LOG.warn(getRegionInfo().getEncodedName() + " : " + "Skipping replaying region event :"
            + TextFormat.shortDebugString(regionEvent)
            + " because its sequence id is smaller than this regions lastReplayedOpenRegionSeqId "
            + " of " + lastReplayedOpenRegionSeqId);
          return;
        }

        // region open lists all the files that the region has at the time of the opening. Just pick
        // all the files and drop prepared flushes and empty memstores
        for (StoreDescriptor storeDescriptor : regionEvent.getStoresList()) {
          // stores of primary may be different now
          byte[] family = storeDescriptor.getFamilyName().toByteArray();
          HStore store = getStore(family);
          if (store == null) {
            LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a region open marker from primary, but the family is not found. "
              + "Ignoring. StoreDescriptor:" + storeDescriptor);
            continue;
          }

          long storeSeqId = store.getMaxSequenceId().orElse(0L);
          List<String> storeFiles = storeDescriptor.getStoreFileList();
          try {
            store.refreshStoreFiles(storeFiles); // replace the files with the new ones
          } catch (FileNotFoundException ex) {
            LOG.warn(getRegionInfo().getEncodedName() + " : " + "At least one of the store files: "
              + storeFiles + " doesn't exist any more. Skip loading the file(s)", ex);
            continue;
          }
          if (store.getMaxSequenceId().orElse(0L) != storeSeqId) {
            // Record latest flush time if we picked up new files
            lastStoreFlushTimeMap.put(store, EnvironmentEdgeManager.currentTime());
          }

          if (writestate.flushing) {
            // only drop memstore snapshots if they are smaller than last flush for the store
            if (this.prepareFlushResult.flushOpSeqId <= regionEvent.getLogSequenceNumber()) {
              StoreFlushContext ctx = this.prepareFlushResult.storeFlushCtxs == null
                ? null
                : this.prepareFlushResult.storeFlushCtxs.get(family);
              if (ctx != null) {
                MemStoreSize mss = store.getFlushableSize();
                ctx.abort();
                this.decrMemStoreSize(mss);
                this.prepareFlushResult.storeFlushCtxs.remove(family);
              }
            }
          }

          // Drop the memstore contents if they are now smaller than the latest seen flushed file
          dropMemStoreContentsForSeqId(regionEvent.getLogSequenceNumber(), store);
          if (storeSeqId > this.maxFlushedSeqId) {
            this.maxFlushedSeqId = storeSeqId;
          }
        }

        // if all stores ended up dropping their snapshots, we can safely drop the
        // prepareFlushResult
        dropPrepareFlushIfPossible();

        // advance the mvcc read point so that the new flushed file is visible.
        mvcc.await();

        // If we were waiting for observing a flush or region opening event for not showing partial
        // data after a secondary region crash, we can allow reads now.
        this.setReadsEnabled(true);

        // C. Finally notify anyone waiting on memstore to clear:
        // e.g. checkResources().
        synchronized (this) {
          notifyAll(); // FindBugs NN_NAKED_NOTIFY
        }
      }
      logRegionFiles();
    } finally {
      closeRegionOperation(Operation.REPLAY_EVENT);
    }
  }

  void replayWALBulkLoadEventMarker(WALProtos.BulkLoadDescriptor bulkLoadEvent) throws IOException {
    checkTargetRegion(bulkLoadEvent.getEncodedRegionName().toByteArray(),
      "BulkLoad marker from WAL ", bulkLoadEvent);

    if (ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
      return; // if primary nothing to do
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(getRegionInfo().getEncodedName() + " : " + "Replaying bulkload event marker "
        + TextFormat.shortDebugString(bulkLoadEvent));
    }
    // check if multiple families involved
    boolean multipleFamilies = false;
    byte[] family = null;
    for (StoreDescriptor storeDescriptor : bulkLoadEvent.getStoresList()) {
      byte[] fam = storeDescriptor.getFamilyName().toByteArray();
      if (family == null) {
        family = fam;
      } else if (!Bytes.equals(family, fam)) {
        multipleFamilies = true;
        break;
      }
    }

    startBulkRegionOperation(multipleFamilies);
    try {
      // we will use writestate as a coarse-grain lock for all the replay events
      synchronized (writestate) {
        // Replication can deliver events out of order when primary region moves or the region
        // server crashes, since there is no coordination between replication of different wal files
        // belonging to different region servers. We have to safe guard against this case by using
        // region open event's seqid. Since this is the first event that the region puts (after
        // possibly flushing recovered.edits), after seeing this event, we can ignore every edit
        // smaller than this seqId
        if (
          bulkLoadEvent.getBulkloadSeqNum() >= 0
            && this.lastReplayedOpenRegionSeqId >= bulkLoadEvent.getBulkloadSeqNum()
        ) {
          LOG.warn(getRegionInfo().getEncodedName() + " : " + "Skipping replaying bulkload event :"
            + TextFormat.shortDebugString(bulkLoadEvent)
            + " because its sequence id is smaller than this region's lastReplayedOpenRegionSeqId"
            + " =" + lastReplayedOpenRegionSeqId);

          return;
        }

        for (StoreDescriptor storeDescriptor : bulkLoadEvent.getStoresList()) {
          // stores of primary may be different now
          family = storeDescriptor.getFamilyName().toByteArray();
          HStore store = getStore(family);
          if (store == null) {
            LOG.warn(getRegionInfo().getEncodedName() + " : "
              + "Received a bulk load marker from primary, but the family is not found. "
              + "Ignoring. StoreDescriptor:" + storeDescriptor);
            continue;
          }

          List<String> storeFiles = storeDescriptor.getStoreFileList();
          for (String storeFile : storeFiles) {
            StoreFileInfo storeFileInfo = null;
            try {
              storeFileInfo = fs.getStoreFileInfo(Bytes.toString(family), storeFile);
              store.bulkLoadHFile(storeFileInfo);
            } catch (FileNotFoundException ex) {
              LOG.warn(getRegionInfo().getEncodedName() + " : "
                + ((storeFileInfo != null)
                  ? storeFileInfo.toString()
                  : (new Path(Bytes.toString(family), storeFile)).toString())
                + " doesn't exist any more. Skip loading the file");
            }
          }
        }
      }
      if (bulkLoadEvent.getBulkloadSeqNum() > 0) {
        mvcc.advanceTo(bulkLoadEvent.getBulkloadSeqNum());
      }
    } finally {
      closeBulkRegionOperation();
    }
  }

  /**
   * If all stores ended up dropping their snapshots, we can safely drop the prepareFlushResult
   */
  private void dropPrepareFlushIfPossible() {
    if (writestate.flushing) {
      boolean canDrop = true;
      if (prepareFlushResult.storeFlushCtxs != null) {
        for (Entry<byte[], StoreFlushContext> entry : prepareFlushResult.storeFlushCtxs
          .entrySet()) {
          HStore store = getStore(entry.getKey());
          if (store == null) {
            continue;
          }
          if (store.getSnapshotSize().getDataSize() > 0) {
            canDrop = false;
            break;
          }
        }
      }

      // this means that all the stores in the region has finished flushing, but the WAL marker
      // may not have been written or we did not receive it yet.
      if (canDrop) {
        writestate.flushing = false;
        this.prepareFlushResult = null;
      }
    }
  }

  @Override
  public boolean refreshStoreFiles() throws IOException {
    return refreshStoreFiles(false);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY",
      justification = "Notify is about post replay. Intentional")
  protected boolean refreshStoreFiles(boolean force) throws IOException {
    if (!force && ServerRegionReplicaUtil.isDefaultReplica(this.getRegionInfo())) {
      return false; // if primary nothing to do
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(getRegionInfo().getEncodedName() + " : "
        + "Refreshing store files to see whether we can free up memstore");
    }

    long totalFreedDataSize = 0;

    long smallestSeqIdInStores = Long.MAX_VALUE;

    startRegionOperation(); // obtain region close lock
    try {
      Map<HStore, Long> map = new HashMap<>();
      synchronized (writestate) {
        for (HStore store : stores.values()) {
          // TODO: some stores might see new data from flush, while others do not which
          // MIGHT break atomic edits across column families.
          long maxSeqIdBefore = store.getMaxSequenceId().orElse(0L);

          // refresh the store files. This is similar to observing a region open wal marker.
          store.refreshStoreFiles();

          long storeSeqId = store.getMaxSequenceId().orElse(0L);
          if (storeSeqId < smallestSeqIdInStores) {
            smallestSeqIdInStores = storeSeqId;
          }

          // see whether we can drop the memstore or the snapshot
          if (storeSeqId > maxSeqIdBefore) {
            if (writestate.flushing) {
              // only drop memstore snapshots if they are smaller than last flush for the store
              if (this.prepareFlushResult.flushOpSeqId <= storeSeqId) {
                StoreFlushContext ctx = this.prepareFlushResult.storeFlushCtxs == null
                  ? null
                  : this.prepareFlushResult.storeFlushCtxs
                    .get(store.getColumnFamilyDescriptor().getName());
                if (ctx != null) {
                  MemStoreSize mss = store.getFlushableSize();
                  ctx.abort();
                  this.decrMemStoreSize(mss);
                  this.prepareFlushResult.storeFlushCtxs
                    .remove(store.getColumnFamilyDescriptor().getName());
                  totalFreedDataSize += mss.getDataSize();
                }
              }
            }

            map.put(store, storeSeqId);
          }
        }

        // if all stores ended up dropping their snapshots, we can safely drop the
        // prepareFlushResult
        if (KnobRuntime.check(java.util.UUID.fromString("2931e41d-ba93-3b17-8b94-b2d3f66472fc"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("92c055fc-d254-3255-912c-22ff31e265aa"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("b81a3c0b-778b-3057-ba49-3c1d434dba83"))) { interruptRegionOperations(); } else { dropPrepareFlushIfPossible(); }

        // advance the mvcc read point so that the new flushed files are visible.
        // either greater than flush seq number or they were already picked up via flush.
        for (HStore s : stores.values()) {
          if (KnobRuntime.check(java.util.UUID.fromString("9b326168-a5a5-31a4-a26d-32d909c4c0b2"))) { mvcc.advanceTo(Math.abs(0L)); } else { mvcc.advanceTo(s.getMaxMemStoreTS().orElse(0L)); }
        }

        // smallestSeqIdInStores is the seqId that we have a corresponding hfile for. We can safely
        // skip all edits that are to be replayed in the future with that has a smaller seqId
        // than this. We are updating lastReplayedOpenRegionSeqId so that we can skip all edits
        // that we have picked the flush files for
        if (this.lastReplayedOpenRegionSeqId < smallestSeqIdInStores) {
          this.lastReplayedOpenRegionSeqId = smallestSeqIdInStores;
        }
      }
      if (!map.isEmpty()) {
        for (Map.Entry<HStore, Long> entry : map.entrySet()) {
          // Drop the memstore contents if they are now smaller than the latest seen flushed file
          totalFreedDataSize +=
            dropMemStoreContentsForSeqId(entry.getValue(), entry.getKey()).getDataSize();
        }
      }
      // C. Finally notify anyone waiting on memstore to clear:
      // e.g. checkResources().
      synchronized (this) {
        notifyAll(); // FindBugs NN_NAKED_NOTIFY
      }
      return ((KnobRuntime.check(java.util.UUID.fromString("ff96199e-115d-3ff6-8d84-fef755cdbd77"))) ? ((totalFreedDataSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("66903d4a-f25a-3dfd-b926-567ed1c0eaca"))) ? ((totalFreedDataSize) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d69bbd37-a06e-3958-acdc-a36e685e9421"))) ? ((totalFreedDataSize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cfff79db-a84a-3277-90fe-bf9d140b61a7"))) ? ((totalFreedDataSize) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a76fc556-41b2-3363-8000-19edaa94c586"))) ? ((totalFreedDataSize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2a8051f9-57bc-38fb-8218-e03179f06e4d"))) ? ((totalFreedDataSize) >= (0)) : (totalFreedDataSize > 0))))))))))));
    } finally {
      closeRegionOperation();
    }
  }

  private void logRegionFiles() {
    if (LOG.isTraceEnabled()) {
      LOG.trace(getRegionInfo().getEncodedName() + " : Store files for region: ");
      stores.values().stream().filter(s -> s.getStorefiles() != null)
        .flatMap(s -> s.getStorefiles().stream())
        .forEachOrdered(sf -> LOG.trace(getRegionInfo().getEncodedName() + " : " + sf));
    }
  }

  /**
   * Checks whether the given regionName is either equal to our region, or that the regionName is
   * the primary region to our corresponding range for the secondary replica.
   */
  private void checkTargetRegion(byte[] encodedRegionName, String exceptionMsg, Object payload)
    throws WrongRegionException {
    if (Bytes.equals(this.getRegionInfo().getEncodedNameAsBytes(), encodedRegionName)) {
      return;
    }

    if (
      !RegionReplicaUtil.isDefaultReplica(this.getRegionInfo())
        && Bytes.equals(encodedRegionName, this.fs.getRegionInfoForFS().getEncodedNameAsBytes())
    ) {
      return;
    }

    throw new WrongRegionException(
      exceptionMsg + payload + " targetted for region " + Bytes.toStringBinary(encodedRegionName)
        + " does not match this region: " + this.getRegionInfo());
  }

  /**
   * Used by tests
   * @param s    Store to add edit too.
   * @param cell Cell to add.
   */
  protected void restoreEdit(HStore s, Cell cell, MemStoreSizing memstoreAccounting) {
    s.add(cell, memstoreAccounting);
  }

  /**
   * make sure have been through lease recovery before get file status, so the file length can be
   * trusted.
   * @param p File to check.
   * @return True if file was zero-length (and if so, we'll delete it in here).
   */
  private static boolean isZeroLengthThenDelete(final FileSystem fs, final FileStatus stat,
    final Path p) throws IOException {
    if (stat.getLen() > 0) {
      return false;
    }
    LOG.warn("File " + p + " is zero-length, deleting.");
    fs.delete(p, false);
    return true;
  }

  protected HStore instantiateHStore(final ColumnFamilyDescriptor family, boolean warmup)
    throws IOException {
    if (family.isMobEnabled()) {
      if (HFile.getFormatVersion(this.conf) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
        throw new IOException("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS
          + " is required for MOB feature. Consider setting " + HFile.FORMAT_VERSION_KEY
          + " accordingly.");
      }
      return new HMobStore(this, family, this.conf, warmup);
    }
    return new HStore(this, family, this.conf, warmup);
  }

  @Override
  public HStore getStore(byte[] column) {
    return this.stores.get(column);
  }

  /**
   * Return HStore instance. Does not do any copy: as the number of store is limited, we iterate on
   * the list.
   */
  private HStore getStore(Cell cell) {
    return stores.entrySet().stream().filter(e -> CellUtil.matchingFamily(cell, e.getKey()))
      .map(e -> e.getValue()).findFirst().orElse(null);
  }

  @Override
  public List<HStore> getStores() {
    return new ArrayList<>(stores.values());
  }

  @Override
  public List<String> getStoreFileList(byte[][] columns) throws IllegalArgumentException {
    List<String> storeFileNames = new ArrayList<>();
    synchronized (closeLock) {
      for (byte[] column : columns) {
        HStore store = this.stores.get(column);
        if (store == null) {
          throw new IllegalArgumentException(
            "No column family : " + new String(column, StandardCharsets.UTF_8) + " available");
        }
        Collection<HStoreFile> storeFiles = store.getStorefiles();
        if (storeFiles == null) {
          continue;
        }
        for (HStoreFile storeFile : storeFiles) {
          storeFileNames.add(storeFile.getPath().toString());
        }

        logRegionFiles();
      }
    }
    return storeFileNames;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Support code
  //////////////////////////////////////////////////////////////////////////////

  /** Make sure this is a valid row for the HRegion */
  void checkRow(byte[] row, String op) throws IOException {
    if (!rowIsInRange(getRegionInfo(), row)) {
      throw new WrongRegionException("Requested row out of range for " + op + " on HRegion " + this
        + ", startKey='" + Bytes.toStringBinary(getRegionInfo().getStartKey()) + "', getEndKey()='"
        + Bytes.toStringBinary(getRegionInfo().getEndKey()) + "', row='" + Bytes.toStringBinary(row)
        + "'");
    }
  }

  /**
   * Get an exclusive ( write lock ) lock on a given row.
   * @param row Which row to lock.
   * @return A locked RowLock. The lock is exclusive and already aqquired.
   */
  public RowLock getRowLock(byte[] row) throws IOException {
    return getRowLock(row, false);
  }

  @Override
  public RowLock getRowLock(byte[] row, boolean readLock) throws IOException {
    checkRow(row, "row lock");
    return getRowLock(row, readLock, null);
  }

  Span createRegionSpan(String name) {
    return TraceUtil.createSpan(name).setAttribute(REGION_NAMES_KEY,
      Collections.singletonList(getRegionInfo().getRegionNameAsString()));
  }

  // will be override in tests
  protected RowLock getRowLockInternal(byte[] row, boolean readLock, RowLock prevRowLock)
    throws IOException {
    // create an object to use a a key in the row lock map
    HashedBytes rowKey = new HashedBytes(row);

    RowLockContext rowLockContext = null;
    RowLockImpl result = null;

    boolean success = false;
    try {
      // Keep trying until we have a lock or error out.
      // TODO: do we need to add a time component here?
      while (result == null) {
        rowLockContext = computeIfAbsent(lockedRows, rowKey, () -> new RowLockContext(rowKey));
        // Now try an get the lock.
        // This can fail as
        if (readLock) {
          // For read lock, if the caller has locked the same row previously, it will not try
          // to acquire the same read lock. It simply returns the previous row lock.
          RowLockImpl prevRowLockImpl = (RowLockImpl) prevRowLock;
          if (
            (prevRowLockImpl != null)
              && (prevRowLockImpl.getLock() == rowLockContext.readWriteLock.readLock())
          ) {
            success = true;
            return prevRowLock;
          }
          result = rowLockContext.newReadLock();
        } else {
          result = rowLockContext.newWriteLock();
        }
      }

      int timeout = rowLockWaitDuration;
      boolean reachDeadlineFirst = false;
      Optional<RpcCall> call = RpcServer.getCurrentCall();
      if (call.isPresent()) {
        long deadline = call.get().getDeadline();
        if (deadline < Long.MAX_VALUE) {
          int timeToDeadline = (int) (deadline - EnvironmentEdgeManager.currentTime());
          if (timeToDeadline <= this.rowLockWaitDuration) {
            reachDeadlineFirst = true;
            timeout = timeToDeadline;
          }
        }
      }

      if (timeout <= 0 || !result.getLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
        String message = "Timed out waiting for lock for row: " + rowKey + " in region "
          + getRegionInfo().getEncodedName();
        if (reachDeadlineFirst) {
          throw new TimeoutIOException(message);
        } else {
          // If timeToDeadline is larger than rowLockWaitDuration, we can not drop the request.
          throw new IOException(message);
        }
      }
      rowLockContext.setThreadName(Thread.currentThread().getName());
      success = true;
      return result;
    } catch (InterruptedException ie) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Thread interrupted waiting for lock on row: {}, in region {}", rowKey,
          getRegionInfo().getRegionNameAsString());
      }
      throw throwOnInterrupt(ie);
    } catch (Error error) {
      // The maximum lock count for read lock is 64K (hardcoded), when this maximum count
      // is reached, it will throw out an Error. This Error needs to be caught so it can
      // go ahead to process the minibatch with lock acquired.
      LOG.warn("Error to get row lock for {}, in region {}, cause: {}", Bytes.toStringBinary(row),
        getRegionInfo().getRegionNameAsString(), error);
      IOException ioe = new IOException(error);
      throw ioe;
    } finally {
      // Clean up the counts just in case this was the thing keeping the context alive.
      if (!success && rowLockContext != null) {
        rowLockContext.cleanUp();
      }
    }
  }

  private RowLock getRowLock(byte[] row, boolean readLock, final RowLock prevRowLock)
    throws IOException {
    return TraceUtil.trace(() -> getRowLockInternal(row, readLock, prevRowLock),
      () -> createRegionSpan("Region.getRowLock").setAttribute(ROW_LOCK_READ_LOCK_KEY, readLock));
  }

  private void releaseRowLocks(List<RowLock> rowLocks) {
    if (rowLocks != null) {
      for (RowLock rowLock : rowLocks) {
        rowLock.release();
      }
      rowLocks.clear();
    }
  }

  public int getReadLockCount() {
    return lock.getReadLockCount();
  }

  public ConcurrentHashMap<HashedBytes, RowLockContext> getLockedRows() {
    return lockedRows;
  }

  class RowLockContext {
    private final HashedBytes row;
    final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    final AtomicBoolean usable = new AtomicBoolean(true);
    final AtomicInteger count = new AtomicInteger(0);
    final Object lock = new Object();
    private String threadName;

    RowLockContext(HashedBytes row) {
      this.row = row;
    }

    RowLockImpl newWriteLock() {
      Lock l = readWriteLock.writeLock();
      return getRowLock(l);
    }

    RowLockImpl newReadLock() {
      Lock l = readWriteLock.readLock();
      return getRowLock(l);
    }

    private RowLockImpl getRowLock(Lock l) {
      count.incrementAndGet();
      synchronized (lock) {
        if (usable.get()) {
          return new RowLockImpl(this, l);
        } else {
          return null;
        }
      }
    }

    void cleanUp() {
      long c = count.decrementAndGet();
      if (c <= 0) {
        synchronized (lock) {
          if (count.get() <= 0 && usable.get()) { // Don't attempt to remove row if already removed
            usable.set(false);
            RowLockContext removed = lockedRows.remove(row);
            assert removed == this : "we should never remove a different context";
          }
        }
      }
    }

    public void setThreadName(String threadName) {
      this.threadName = threadName;
    }

    @Override
    public String toString() {
      return "RowLockContext{" + "row=" + row + ", readWriteLock=" + readWriteLock + ", count="
        + count + ", threadName=" + threadName + '}';
    }
  }

  /**
   * Class used to represent a lock on a row.
   */
  public static class RowLockImpl implements RowLock {
    private final RowLockContext context;
    private final Lock lock;

    public RowLockImpl(RowLockContext context, Lock lock) {
      this.context = context;
      this.lock = lock;
    }

    public Lock getLock() {
      return lock;
    }

    public RowLockContext getContext() {
      return context;
    }

    @Override
    public void release() {
      lock.unlock();
      context.cleanUp();
    }

    @Override
    public String toString() {
      return "RowLockImpl{" + "context=" + context + ", lock=" + lock + '}';
    }
  }

  /**
   * Determines whether multiple column families are present Precondition: familyPaths is not null
   * @param familyPaths List of (column family, hfilePath)
   */
  private static boolean hasMultipleColumnFamilies(Collection<Pair<byte[], String>> familyPaths) {
    boolean multipleFamilies = false;
    byte[] family = null;
    for (Pair<byte[], String> pair : familyPaths) {
      byte[] fam = pair.getFirst();
      if (family == null) {
        family = fam;
      } else if (!Bytes.equals(family, fam)) {
        multipleFamilies = true;
        break;
      }
    }
    return multipleFamilies;
  }

  /**
   * Attempts to atomically load a group of hfiles. This is critical for loading rows with multiple
   * column families atomically.
   * @param familyPaths      List of Pair&lt;byte[] column family, String hfilePath&gt;
   * @param bulkLoadListener Internal hooks enabling massaging/preparation of a file about to be
   *                         bulk loaded
   * @return Map from family to List of store file paths if successful, null if failed recoverably
   * @throws IOException if failed unrecoverably.
   */
  public Map<byte[], List<Path>> bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths,
    boolean assignSeqId, BulkLoadListener bulkLoadListener) throws IOException {
    return bulkLoadHFiles(familyPaths, assignSeqId, bulkLoadListener, false, null, true);
  }

  /**
   * Listener class to enable callers of bulkLoadHFile() to perform any necessary pre/post
   * processing of a given bulkload call
   */
  public interface BulkLoadListener {
    /**
     * Called before an HFile is actually loaded
     * @param family  family being loaded to
     * @param srcPath path of HFile
     * @return final path to be used for actual loading
     */
    String prepareBulkLoad(byte[] family, String srcPath, boolean copyFile, String customStaging)
      throws IOException;

    /**
     * Called after a successful HFile load
     * @param family  family being loaded to
     * @param srcPath path of HFile
     */
    void doneBulkLoad(byte[] family, String srcPath) throws IOException;

    /**
     * Called after a failed HFile load
     * @param family  family being loaded to
     * @param srcPath path of HFile
     */
    void failedBulkLoad(byte[] family, String srcPath) throws IOException;
  }

  /**
   * Attempts to atomically load a group of hfiles. This is critical for loading rows with multiple
   * column families atomically.
   * @param familyPaths      List of Pair&lt;byte[] column family, String hfilePath&gt;
   * @param bulkLoadListener Internal hooks enabling massaging/preparation of a file about to be
   *                         bulk loaded
   * @param copyFile         always copy hfiles if true
   * @param clusterIds       ids from clusters that had already handled the given bulkload event.
   * @return Map from family to List of store file paths if successful, null if failed recoverably
   * @throws IOException if failed unrecoverably.
   */
  public Map<byte[], List<Path>> bulkLoadHFiles(Collection<Pair<byte[], String>> familyPaths,
    boolean assignSeqId, BulkLoadListener bulkLoadListener, boolean copyFile,
    List<String> clusterIds, boolean replicate) throws IOException {
    long seqId = -1;
    Map<byte[], List<Path>> storeFiles = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    Map<String, Long> storeFilesSizes = new HashMap<>();
    Preconditions.checkNotNull(familyPaths);
    // we need writeLock for multi-family bulk load
    startBulkRegionOperation(hasMultipleColumnFamilies(familyPaths));
    boolean isSuccessful = false;
    try {
      this.writeRequestsCount.increment();

      // There possibly was a split that happened between when the split keys
      // were gathered and before the HRegion's write lock was taken. We need
      // to validate the HFile region before attempting to bulk load all of them
      IOException ioException = null;
      List<Pair<byte[], String>> failures = new ArrayList<>();
      for (Pair<byte[], String> p : familyPaths) {
        byte[] familyName = p.getFirst();
        String path = p.getSecond();

        HStore store = getStore(familyName);
        if (store == null) {
          ioException = new org.apache.hadoop.hbase.DoNotRetryIOException(
            "No such column family " + Bytes.toStringBinary(familyName));
        } else {
          try {
            store.assertBulkLoadHFileOk(new Path(path));
          } catch (WrongRegionException wre) {
            // recoverable (file doesn't fit in region)
            failures.add(p);
          } catch (IOException ioe) {
            // unrecoverable (hdfs problem)
            ioException = ioe;
          }
        }

        // validation failed because of some sort of IO problem.
        if (ioException != null) {
          LOG.error("There was IO error when checking if the bulk load is ok in region {}.", this,
            ioException);
          throw ioException;
        }
      }
      // validation failed, bail out before doing anything permanent.
      if (failures.size() != 0) {
        StringBuilder list = new StringBuilder();
        for (Pair<byte[], String> p : failures) {
          list.append("\n").append(Bytes.toString(p.getFirst())).append(" : ")
            .append(p.getSecond());
        }
        // problem when validating
        LOG.warn("There was a recoverable bulk load failure likely due to a split. These (family,"
          + " HFile) pairs were not loaded: {}, in region {}", list.toString(), this);
        return null;
      }

      // We need to assign a sequential ID that's in between two memstores in order to preserve
      // the guarantee that all the edits lower than the highest sequential ID from all the
      // HFiles are flushed on disk. See HBASE-10958. The sequence id returned when we flush is
      // guaranteed to be one beyond the file made when we flushed (or if nothing to flush, it is
      // a sequence id that we can be sure is beyond the last hfile written).
      if (assignSeqId) {
        FlushResult fs = flushcache(true, false, FlushLifeCycleTracker.DUMMY);
        if (fs.isFlushSucceeded()) {
          seqId = ((FlushResultImpl) fs).flushSequenceId;
        } else if (fs.getResult() == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY) {
          seqId = ((FlushResultImpl) fs).flushSequenceId;
        } else if (fs.getResult() == FlushResult.Result.CANNOT_FLUSH) {
          // CANNOT_FLUSH may mean that a flush is already on-going
          // we need to wait for that flush to complete
          waitForFlushes();
        } else {
          throw new IOException("Could not bulk load with an assigned sequential ID because the "
            + "flush didn't run. Reason for not flushing: " + ((FlushResultImpl) fs).failureReason);
        }
      }

      Map<byte[], List<Pair<Path, Path>>> familyWithFinalPath =
        new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (Pair<byte[], String> p : familyPaths) {
        byte[] familyName = p.getFirst();
        String path = p.getSecond();
        HStore store = getStore(familyName);
        if (!familyWithFinalPath.containsKey(familyName)) {
          familyWithFinalPath.put(familyName, new ArrayList<>());
        }
        List<Pair<Path, Path>> lst = familyWithFinalPath.get(familyName);
        String finalPath = path;
        try {
          boolean reqTmp = store.storeEngine.requireWritingToTmpDirFirst();
          if (bulkLoadListener != null) {
            finalPath = bulkLoadListener.prepareBulkLoad(familyName, path, copyFile,
              reqTmp ? null : fs.getRegionDir().toString());
          }
          Pair<Path, Path> pair = null;
          if (reqTmp || !StoreFileInfo.isHFile(finalPath)) {
            pair = store.preBulkLoadHFile(finalPath, seqId);
          } else {
            Path livePath = new Path(finalPath);
            pair = new Pair<>(livePath, livePath);
          }
          lst.add(pair);
        } catch (IOException ioe) {
          // A failure here can cause an atomicity violation that we currently
          // cannot recover from since it is likely a failed HDFS operation.

          LOG.error("There was a partial failure due to IO when attempting to" + " load "
            + Bytes.toString(p.getFirst()) + " : " + p.getSecond(), ioe);
          if (bulkLoadListener != null) {
            try {
              bulkLoadListener.failedBulkLoad(familyName, finalPath);
            } catch (Exception ex) {
              LOG.error("Error while calling failedBulkLoad for family "
                + Bytes.toString(familyName) + " with path " + path, ex);
            }
          }
          throw ioe;
        }
      }

      if (this.getCoprocessorHost() != null) {
        for (Map.Entry<byte[], List<Pair<Path, Path>>> entry : familyWithFinalPath.entrySet()) {
          this.getCoprocessorHost().preCommitStoreFile(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<byte[], List<Pair<Path, Path>>> entry : familyWithFinalPath.entrySet()) {
        byte[] familyName = entry.getKey();
        for (Pair<Path, Path> p : entry.getValue()) {
          String path = p.getFirst().toString();
          Path commitedStoreFile = p.getSecond();
          HStore store = getStore(familyName);
          try {
            store.bulkLoadHFile(familyName, path, commitedStoreFile);
            // Note the size of the store file
            try {
              FileSystem fs = commitedStoreFile.getFileSystem(baseConf);
              storeFilesSizes.put(commitedStoreFile.getName(),
                fs.getFileStatus(commitedStoreFile).getLen());
            } catch (IOException e) {
              LOG.warn("Failed to find the size of hfile " + commitedStoreFile, e);
              storeFilesSizes.put(commitedStoreFile.getName(), 0L);
            }

            if (storeFiles.containsKey(familyName)) {
              storeFiles.get(familyName).add(commitedStoreFile);
            } else {
              List<Path> storeFileNames = new ArrayList<>();
              storeFileNames.add(commitedStoreFile);
              storeFiles.put(familyName, storeFileNames);
            }
            if (bulkLoadListener != null) {
              bulkLoadListener.doneBulkLoad(familyName, path);
            }
          } catch (IOException ioe) {
            // A failure here can cause an atomicity violation that we currently
            // cannot recover from since it is likely a failed HDFS operation.

            // TODO Need a better story for reverting partial failures due to HDFS.
            LOG.error("There was a partial failure due to IO when attempting to" + " load "
              + Bytes.toString(familyName) + " : " + p.getSecond(), ioe);
            if (bulkLoadListener != null) {
              try {
                bulkLoadListener.failedBulkLoad(familyName, path);
              } catch (Exception ex) {
                LOG.error("Error while calling failedBulkLoad for family "
                  + Bytes.toString(familyName) + " with path " + path, ex);
              }
            }
            throw ioe;
          }
        }
      }

      isSuccessful = true;
      if (conf.getBoolean(COMPACTION_AFTER_BULKLOAD_ENABLE, true)) {
        // request compaction
        familyWithFinalPath.keySet().forEach(family -> {
          HStore store = getStore(family);
          try {
            if (this.rsServices != null && store.needsCompaction()) {
              this.rsServices.getCompactionRequestor().requestSystemCompaction(this, store,
                "bulkload hfiles request compaction", true);
              LOG.info("Request compaction for region {} family {} after bulk load",
                this.getRegionInfo().getEncodedName(), store.getColumnFamilyName());
            }
          } catch (IOException e) {
            LOG.error("bulkload hfiles request compaction error ", e);
          }
        });
      }
    } finally {
      if (wal != null && !storeFiles.isEmpty()) {
        // Write a bulk load event for hfiles that are loaded
        try {
          WALProtos.BulkLoadDescriptor loadDescriptor =
            ProtobufUtil.toBulkLoadDescriptor(this.getRegionInfo().getTable(),
              UnsafeByteOperations.unsafeWrap(this.getRegionInfo().getEncodedNameAsBytes()),
              storeFiles, storeFilesSizes, seqId, clusterIds, replicate);
          WALUtil.writeBulkLoadMarkerAndSync(this.wal, this.getReplicationScope(), getRegionInfo(),
            loadDescriptor, mvcc);
        } catch (IOException ioe) {
          if (this.rsServices != null) {
            // Have to abort region server because some hfiles has been loaded but we can't write
            // the event into WAL
            isSuccessful = false;
            this.rsServices.abort("Failed to write bulk load event into WAL.", ioe);
          }
        }
      }

      closeBulkRegionOperation();
    }
    return isSuccessful ? storeFiles : null;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof HRegion && Bytes.equals(getRegionInfo().getRegionName(),
      ((HRegion) o).getRegionInfo().getRegionName());
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(getRegionInfo().getRegionName());
  }

  @Override
  public String toString() {
    return getRegionInfo().getRegionNameAsString();
  }

  // Utility methods
  /**
   * A utility method to create new instances of HRegion based on the {@link HConstants#REGION_IMPL}
   * configuration property.
   * @param tableDir   qualified path of directory where region should be located, usually the table
   *                   directory.
   * @param wal        The WAL is the outbound log for any updates to the HRegion The wal file is a
   *                   logfile from the previous execution that's custom-computed for this HRegion.
   *                   The HRegionServer computes and sorts the appropriate wal info for this
   *                   HRegion. If there is a previous file (implying that the HRegion has been
   *                   written-to before), then read it from the supplied path.
   * @param fs         is the filesystem.
   * @param conf       is global configuration settings.
   * @param regionInfo - RegionInfo that describes the region is new), then read them from the
   *                   supplied path.
   * @param htd        the table descriptor
   * @return the new instance
   */
  public static HRegion newHRegion(Path tableDir, WAL wal, FileSystem fs, Configuration conf,
    RegionInfo regionInfo, final TableDescriptor htd, RegionServerServices rsServices) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HRegion> regionClass =
        (Class<? extends HRegion>) conf.getClass(HConstants.REGION_IMPL, HRegion.class);

      Constructor<? extends HRegion> c =
        regionClass.getConstructor(Path.class, WAL.class, FileSystem.class, Configuration.class,
          RegionInfo.class, TableDescriptor.class, RegionServerServices.class);

      return c.newInstance(tableDir, wal, fs, conf, regionInfo, htd, rsServices);
    } catch (Throwable e) {
      // todo: what should I throw here?
      throw new IllegalStateException("Could not instantiate a region instance.", e);
    }
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * @param info       Info for region to create.
   * @param rootDir    Root directory for HBase instance
   * @param wal        shared WAL
   * @param initialize - true to initialize the region
   * @return new HRegion
   */
  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
    final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal,
    final boolean initialize) throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, wal, initialize, null);
  }

  /**
   * Convenience method creating new HRegions. Used by createTable.
   * @param info          Info for region to create.
   * @param rootDir       Root directory for HBase instance
   * @param wal           shared WAL
   * @param initialize    - true to initialize the region
   * @param rsRpcServices An interface we can request flushes against.
   * @return new HRegion
   */
  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
    final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal,
    final boolean initialize, RegionServerServices rsRpcServices) throws IOException {
    LOG.info("creating " + info + ", tableDescriptor="
      + (hTableDescriptor == null ? "null" : hTableDescriptor) + ", regionDir=" + rootDir);
    createRegionDir(conf, info, rootDir);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
    HRegion region =
      HRegion.newHRegion(tableDir, wal, fs, conf, info, hTableDescriptor, rsRpcServices);
    if (initialize) {
      region.initialize(null);
    }
    return region;
  }

  /**
   * Create a region under the given table directory.
   */
  public static HRegion createHRegion(Configuration conf, RegionInfo regionInfo, FileSystem fs,
    Path tableDir, TableDescriptor tableDesc) throws IOException {
    LOG.info("Creating {}, tableDescriptor={}, under table dir {}", regionInfo, tableDesc,
      tableDir);
    HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, regionInfo);
    HRegion region = HRegion.newHRegion(tableDir, null, fs, conf, regionInfo, tableDesc, null);
    return region;
  }

  /**
   * Create the region directory in the filesystem.
   */
  public static HRegionFileSystem createRegionDir(Configuration configuration, RegionInfo ri,
    Path rootDir) throws IOException {
    FileSystem fs = rootDir.getFileSystem(configuration);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, ri.getTable());
    // If directory already exists, will log warning and keep going. Will try to create
    // .regioninfo. If one exists, will overwrite.
    return HRegionFileSystem.createRegionOnFileSystem(configuration, fs, tableDir, ri);
  }

  public static HRegion createHRegion(final RegionInfo info, final Path rootDir,
    final Configuration conf, final TableDescriptor hTableDescriptor, final WAL wal)
    throws IOException {
    return createHRegion(info, rootDir, conf, hTableDescriptor, wal, true);
  }

  /**
   * Open a Region.
   * @param info Info for region to be opened.
   * @param wal  WAL for region to use. This method will call WAL#setSequenceNumber(long) passing
   *             the result of the call to HRegion#getMinSequenceId() to ensure the wal id is
   *             properly kept up. HRegionStore does this every time it opens a new region.
   * @return new HRegion
   */
  public static HRegion openHRegion(final RegionInfo info, final TableDescriptor htd, final WAL wal,
    final Configuration conf) throws IOException {
    return openHRegion(info, htd, wal, conf, null, null);
  }

  /**
   * Open a Region.
   * @param info       Info for region to be opened
   * @param htd        the table descriptor
   * @param wal        WAL for region to use. This method will call WAL#setSequenceNumber(long)
   *                   passing the result of the call to HRegion#getMinSequenceId() to ensure the
   *                   wal id is properly kept up. HRegionStore does this every time it opens a new
   *                   region.
   * @param conf       The Configuration object to use.
   * @param rsServices An interface we can request flushes against.
   * @param reporter   An interface we can report progress against.
   * @return new HRegion
   */
  public static HRegion openHRegion(final RegionInfo info, final TableDescriptor htd, final WAL wal,
    final Configuration conf, final RegionServerServices rsServices,
    final CancelableProgressable reporter) throws IOException {
    return openHRegion(CommonFSUtils.getRootDir(conf), info, htd, wal, conf, rsServices, reporter);
  }

  /**
   * Open a Region.
   * @param rootDir Root directory for HBase instance
   * @param info    Info for region to be opened.
   * @param htd     the table descriptor
   * @param wal     WAL for region to use. This method will call WAL#setSequenceNumber(long) passing
   *                the result of the call to HRegion#getMinSequenceId() to ensure the wal id is
   *                properly kept up. HRegionStore does this every time it opens a new region.
   * @param conf    The Configuration object to use.
   * @return new HRegion
   */
  public static HRegion openHRegion(Path rootDir, final RegionInfo info, final TableDescriptor htd,
    final WAL wal, final Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("21fb7c8b-83a8-3569-88fd-3f35189a866b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d70f22e2-98c3-3a89-8b54-396c0ac0eec4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("37239cdb-384b-3226-af85-5ebeed1295c2"))) {
return null;
}
    return openHRegion(rootDir, info, htd, wal, conf, null, null);
  }

  /**
   * Open a Region.
   * @param rootDir    Root directory for HBase instance
   * @param info       Info for region to be opened.
   * @param htd        the table descriptor
   * @param wal        WAL for region to use. This method will call WAL#setSequenceNumber(long)
   *                   passing the result of the call to HRegion#getMinSequenceId() to ensure the
   *                   wal id is properly kept up. HRegionStore does this every time it opens a new
   *                   region.
   * @param conf       The Configuration object to use.
   * @param rsServices An interface we can request flushes against.
   * @param reporter   An interface we can report progress against.
   * @return new HRegion
   */
  public static HRegion openHRegion(final Path rootDir, final RegionInfo info,
    final TableDescriptor htd, final WAL wal, final Configuration conf,
    final RegionServerServices rsServices, final CancelableProgressable reporter)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("014281a7-636d-341a-991a-7c024e8ca620"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6b536196-3a6a-3883-88b0-7e84297a3b1e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1b1ed049-e3f7-3bf3-9fc8-4a5e494dd5b4"))) {
return null;
}
    FileSystem fs = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("e2187ba9-db7e-3fb2-b6ae-745acd3eba4d"))) ? ((rsServices) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d92ea003-d6f2-3682-ac15-44a78d061ddf"))) ? ((rsServices) == (null)) : (rsServices != null))))) {
      fs = rsServices.getFileSystem();
    }
    if (fs == null) {
      fs = rootDir.getFileSystem(conf);
    }
    return openHRegion(conf, fs, rootDir, info, htd, wal, rsServices, reporter);
  }

  /**
   * Open a Region.
   * @param conf    The Configuration object to use.
   * @param fs      Filesystem to use
   * @param rootDir Root directory for HBase instance
   * @param info    Info for region to be opened.
   * @param htd     the table descriptor
   * @param wal     WAL for region to use. This method will call WAL#setSequenceNumber(long) passing
   *                the result of the call to HRegion#getMinSequenceId() to ensure the wal id is
   *                properly kept up. HRegionStore does this every time it opens a new region.
   * @return new HRegion
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
    final Path rootDir, final RegionInfo info, final TableDescriptor htd, final WAL wal)
    throws IOException {
    return openHRegion(conf, fs, rootDir, info, htd, wal, null, null);
  }

  /**
   * Open a Region.
   * @param conf       The Configuration object to use.
   * @param fs         Filesystem to use
   * @param rootDir    Root directory for HBase instance
   * @param info       Info for region to be opened.
   * @param htd        the table descriptor
   * @param wal        WAL for region to use. This method will call WAL#setSequenceNumber(long)
   *                   passing the result of the call to HRegion#getMinSequenceId() to ensure the
   *                   wal id is properly kept up. HRegionStore does this every time it opens a new
   *                   region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter   An interface we can report progress against.
   * @return new HRegion
   */
  public static HRegion openHRegion(final Configuration conf, final FileSystem fs,
    final Path rootDir, final RegionInfo info, final TableDescriptor htd, final WAL wal,
    final RegionServerServices rsServices, final CancelableProgressable reporter)
    throws IOException {
    Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
if(KnobRuntime.check(java.util.UUID.fromString("f0657433-b9e7-3168-b8bb-92b989fd468d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5cd217c0-9121-35ad-b9a8-c4a92b8d2a42"))) {
throw new java.io.IOException("Injected exception");
}
    return ((KnobRuntime.check(java.util.UUID.fromString("0228ce99-af5e-3390-add9-70b831e070a5"))) ? (openHRegion(conf, fs, tableDir, info, htd, wal, rsServices, reporter)) : (openHRegionFromTableDir(conf, fs, tableDir, info, htd, wal, rsServices, reporter)));
  }

  /**
   * Open a Region.
   * @param conf       The Configuration object to use.
   * @param fs         Filesystem to use
   * @param info       Info for region to be opened.
   * @param htd        the table descriptor
   * @param wal        WAL for region to use. This method will call WAL#setSequenceNumber(long)
   *                   passing the result of the call to HRegion#getMinSequenceId() to ensure the
   *                   wal id is properly kept up. HRegionStore does this every time it opens a new
   *                   region.
   * @param rsServices An interface we can request flushes against.
   * @param reporter   An interface we can report progress against.
   * @return new HRegion
   */
  public static HRegion openHRegionFromTableDir(final Configuration conf, final FileSystem fs,
    final Path tableDir, final RegionInfo info, final TableDescriptor htd, final WAL wal,
    final RegionServerServices rsServices, final CancelableProgressable reporter)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ca42ed34-4675-372f-aed6-f16fadb6505a"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("3752fa77-a79c-3a75-b56b-afb936981a0b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fda59ce7-5e02-39ce-852b-9b5d56d59e26"))) {
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
    Objects.requireNonNull(info, "RegionInfo cannot be null");
    LOG.debug("Opening region: {}", info);
    HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, rsServices);
if(KnobRuntime.check(java.util.UUID.fromString("2b5f2166-9c10-3faa-9c57-4f7fbeb24fad"))) {
throw new java.io.IOException("Injected exception");
}
    return r.openHRegion(reporter);
  }

  public NavigableMap<byte[], Integer> getReplicationScope() {
    return this.replicationScope;
  }

  /**
   * Useful when reopening a closed region (normally for unit tests)
   * @param other    original object
   * @param reporter An interface we can report progress against.
   * @return new HRegion
   */
  public static HRegion openHRegion(final HRegion other, final CancelableProgressable reporter)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("52688348-682d-3fd0-9891-e0de8eb886c1"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b3a7713-604b-3496-aa9b-37ac1afcd1c5"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a279eea-dfec-3059-a343-05308d850e5b"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("94089d14-e81b-383f-9001-612981e7dd25"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("739abaca-3914-3965-914f-59cc46361eae"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65cfdb8f-3ee5-38ca-be90-cbccbc890acb"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c2629df-81ec-3f74-b80b-9556ba26a73b"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc6b3a3c-8025-30e4-b90c-70081502647b"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13baee79-5d0b-3ecd-8dcb-2180fa92f9e9"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3fdea0a5-9bf0-32c5-89ab-34a265efb7c6"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec889ed1-26a8-35fb-9d42-948756ec4229"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d2bc1a3f-64bf-3cdc-93f3-ca8673bdddb8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("277526d0-3fd1-32fe-a896-b5392f9f601a"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38417e8c-64fb-3277-8a33-eab1a2ae0633"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c917b81e-929c-3abc-918e-5a4cfb236869"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f98f8ab-2173-3a70-b7c8-a047ab615fcb"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72cfcbfc-adea-3633-b245-461641eb031e"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b7d2ea2-2ea1-363f-b29b-0da6bd43d624"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a8dc89a0-55a4-32f9-afd3-383da48d9a2c"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb75aa7a-417d-3f6d-baec-3108f81b5782"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("283a880a-dd95-362e-83a7-200c1c419b15"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8df998f-e482-387b-ba17-ff1bc420a355"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e90ff5cd-e9dd-37fb-a3a4-855df1ca6db7"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0c79d42-fc2d-3550-8693-8fe727a5a00b"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32099d4a-9c36-38af-ba7f-88ab47cb8394"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("993ce06e-408e-3ef0-957a-805e9ae9f4e1"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c1ee6f6-d628-304d-856f-b2543daf44a8"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("74b686c6-0b02-37b3-b3ac-d7a0377c4c59"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f1503766-b1e7-3402-9d2c-2f8d66127348"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71a90b82-5da5-309a-99a4-2abd21718f8d"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("81f3e5bc-caaf-361d-8704-ad033bebb236"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4500b79-ee3c-3957-a72a-c51225b00858"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8760ebaf-ac12-3451-b640-1d990f55c5a2"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37339eec-9a3f-3101-bfb4-956aae82cef9"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(other));
    field.set(other, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07406f72-1c68-3c84-a6d9-ab37c0df6229"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e82985cc-e42f-326b-9a6b-186292a9f24d"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(other);
    field.set(other, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8f1fed3-0f15-3592-8a69-8b3a350a9b40"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1d176909-846d-3a94-9d22-38d66b1a5b80"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3dec82e1-047b-3e97-9ff4-6833d65582c3"))) {
try {
    java.lang.reflect.Field field = other.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(other));
    field.set(other, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    HRegionFileSystem regionFs = other.getRegionFileSystem();
    HRegion r = newHRegion(regionFs.getTableDir(), other.getWAL(), regionFs.getFileSystem(),
      other.baseConf, other.getRegionInfo(), other.getTableDescriptor(), null);
if(KnobRuntime.check(java.util.UUID.fromString("ff69a233-692e-34d8-a6c2-658626c92349"))) {
throw new java.io.IOException("Injected exception");
}
    return r.openHRegion(reporter);
  }

  public static Region openHRegion(final Region other, final CancelableProgressable reporter)
    throws IOException {
    return openHRegion((HRegion) other, reporter);
  }

  /**
   * Open HRegion.
   * <p/>
   * Calls initialize and sets sequenceId.
   * @return Returns <code>this</code>
   */
  private HRegion openHRegion(final CancelableProgressable reporter) throws IOException {
    try {
      CompoundConfiguration cConfig =
        new CompoundConfiguration().add(conf).addBytesMap(htableDescriptor.getValues());
      // Refuse to open the region if we are missing local compression support
      TableDescriptorChecker.checkCompression(cConfig, htableDescriptor);
      // Refuse to open the region if encryption configuration is incorrect or
      // codec support is missing
      if (KnobRuntime.check(java.util.UUID.fromString("b62581ae-d995-37bf-bf75-210f8de07ec8"))) { LOG.error("checking encryption for "); } else if (KnobRuntime.check(java.util.UUID.fromString("44978036-fda9-32d4-b288-d9b815a6d115"))) { LOG.debug("checking encryption for "); } else if (KnobRuntime.check(java.util.UUID.fromString("1640dca2-8f1a-35b4-9a70-3fa8514f792d"))) { LOG.error(("checking encryption for ") + (this.getRegionInfo().getShortNameToLog())); } else if (KnobRuntime.check(java.util.UUID.fromString("ad8dc501-79d8-389d-b415-14213ae79ce7"))) { LOG.debug(("checking encryption for ") + (this.getRegionInfo().getShortNameToLog())); } else if (KnobRuntime.check(java.util.UUID.fromString("1702c84f-b9f8-3293-bf22-187d7edbbac1"))) { LOG.info("checking encryption for "); } else if (KnobRuntime.check(java.util.UUID.fromString("fdcbd399-10ee-314f-ac8f-30545787fb2c"))) { LOG.info(("checking encryption for ") + (this.getRegionInfo().getEncodedName())); } else if (KnobRuntime.check(java.util.UUID.fromString("cd7977d4-e492-386e-bdfd-2210239cfe8d"))) { LOG.info(("checking encryption for ") + (this.getRegionInfo().getShortNameToLog())); } else if (KnobRuntime.check(java.util.UUID.fromString("40c00eec-1a3e-3220-b479-e07b6ccb44a9"))) { LOG.debug(("checking encryption for ") + (this.getRegionInfo().getEncodedName())); } else if (KnobRuntime.check(java.util.UUID.fromString("40c8a01b-d668-3cd1-88d1-f18e629616be"))) { LOG.error(("checking encryption for ") + (this.getRegionInfo().getEncodedName())); } else { LOG.debug("checking encryption for " + this.getRegionInfo().getEncodedName()); }
      TableDescriptorChecker.checkEncryption(cConfig, htableDescriptor);
      // Refuse to open the region if a required class cannot be loaded
      LOG.debug("checking classloading for " + this.getRegionInfo().getEncodedName());
      TableDescriptorChecker.checkClassLoading(cConfig, htableDescriptor);
      this.openSeqNum = initialize(reporter);
      this.mvcc.advanceTo(openSeqNum);
      // The openSeqNum must be increased every time when a region is assigned, as we rely on it to
      // determine whether a region has been successfully reopened. So here we always write open
      // marker, even if the table is read only.
      if (
        wal != null && getRegionServerServices() != null
          && RegionReplicaUtil.isDefaultReplica(getRegionInfo())
      ) {
        writeRegionOpenMarker(wal, openSeqNum);
      }
    } catch (Throwable t) {
      // By coprocessor path wrong region will open failed,
      // MetricsRegionWrapperImpl is already init and not close,
      // add region close when open failed
      try {
        // It is not required to write sequence id file when region open is failed.
        // Passing true to skip the sequence id file write.
        this.close(true);
      } catch (Throwable e) {
        LOG.warn("Open region: {} failed. Try close region but got exception ",
          this.getRegionInfo(), e);
      }
      throw t;
    }
    return this;
  }

  /**
   * Open a Region on a read-only file-system (like hdfs snapshots)
   * @param conf The Configuration object to use.
   * @param fs   Filesystem to use
   * @param info Info for region to be opened.
   * @param htd  the table descriptor
   * @return new HRegion
   */
  public static HRegion openReadOnlyFileSystemHRegion(final Configuration conf, final FileSystem fs,
    final Path tableDir, RegionInfo info, final TableDescriptor htd) throws IOException {
    if (info == null) {
      throw new NullPointerException("Passed region info is null");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening region (readOnly filesystem): " + info);
    }
    if (info.getReplicaId() <= 0) {
      info = RegionInfoBuilder.newBuilder(info).setReplicaId(1).build();
    }
    HRegion r = HRegion.newHRegion(tableDir, null, fs, conf, info, htd, null);
    r.writestate.setReadOnly(true);
    return r.openHRegion(null);
  }

  public static HRegion warmupHRegion(final RegionInfo info, final TableDescriptor htd,
    final WAL wal, final Configuration conf, final RegionServerServices rsServices,
    final CancelableProgressable reporter) throws IOException {

    Objects.requireNonNull(info, "RegionInfo cannot be null");
    LOG.debug("Warmup {}", info);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, info.getTable());
    FileSystem fs = null;
    if (rsServices != null) {
      fs = rsServices.getFileSystem();
    }
    if (fs == null) {
      fs = rootDir.getFileSystem(conf);
    }
    HRegion r = HRegion.newHRegion(tableDir, wal, fs, conf, info, htd, null);
    r.initializeWarmup(reporter);
    r.close();
    return r;
  }

  /**
   * Computes the Path of the HRegion
   * @param tabledir qualified path for table
   * @param name     ENCODED region name
   * @return Path of HRegion directory
   * @deprecated For tests only; to be removed.
   */
  @Deprecated
  public static Path getRegionDir(final Path tabledir, final String name) {
    return new Path(tabledir, name);
  }

  /**
   * Determines if the specified row is within the row range specified by the specified RegionInfo
   * @param info RegionInfo that specifies the row range
   * @param row  row to be checked
   * @return true if the row is within the range specified by the RegionInfo
   */
  public static boolean rowIsInRange(RegionInfo info, final byte[] row) {
    return ((info.getStartKey().length == 0) || (Bytes.compareTo(info.getStartKey(), row) <= 0))
      && ((info.getEndKey().length == 0) || (Bytes.compareTo(info.getEndKey(), row) > 0));
  }

  public static boolean rowIsInRange(RegionInfo info, final byte[] row, final int offset,
    final short length) {
    return ((info.getStartKey().length == 0)
      || (Bytes.compareTo(info.getStartKey(), 0, info.getStartKey().length, row, offset, length)
          <= 0))
      && ((info.getEndKey().length == 0)
        || (Bytes.compareTo(info.getEndKey(), 0, info.getEndKey().length, row, offset, length)
            > 0));
  }

  @Override
  public Result get(final Get get) throws IOException {
    prepareGet(get);
    List<Cell> results = get(get, true);
    boolean stale = this.getRegionInfo().getReplicaId() != 0;
    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  void prepareGet(final Get get) throws IOException {
    checkRow(get.getRow(), "Get");
    // Verify families are all valid
    if (get.hasFamilies()) {
      for (byte[] family : get.familySet()) {
        checkFamily(family);
      }
    } else { // Adding all families to scanner
      for (byte[] family : this.htableDescriptor.getColumnFamilyNames()) {
        get.addFamily(family);
      }
    }
  }

  @Override
  public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
    return get(get, withCoprocessor, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  private List<Cell> get(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
    throws IOException {
    return TraceUtil.trace(() -> getInternal(get, withCoprocessor, nonceGroup, nonce),
      () -> createRegionSpan("Region.get"));
  }

  private List<Cell> getInternal(Get get, boolean withCoprocessor, long nonceGroup, long nonce)
    throws IOException {
    List<Cell> results = new ArrayList<>();
    long before = EnvironmentEdgeManager.currentTime();

    // pre-get CP hook
    if (withCoprocessor && (coprocessorHost != null)) {
      if (coprocessorHost.preGet(get, results)) {
        metricsUpdateForGet(results, before);
        return results;
      }
    }
    Scan scan = new Scan(get);
    if (scan.getLoadColumnFamiliesOnDemandValue() == null) {
      scan.setLoadColumnFamiliesOnDemand(isLoadingCfsOnDemandDefault());
    }
    try (RegionScanner scanner = getScanner(scan, null, nonceGroup, nonce)) {
      List<Cell> tmp = new ArrayList<>();
      scanner.next(tmp);
      // Copy EC to heap, then close the scanner.
      // This can be an EXPENSIVE call. It may make an extra copy from offheap to onheap buffers.
      // See more details in HBASE-26036.
      for (Cell cell : tmp) {
        results.add(CellUtil.cloneIfNecessary(cell));
      }
    }

    // post-get CP hook
    if (withCoprocessor && (coprocessorHost != null)) {
      coprocessorHost.postGet(get, results);
    }

    metricsUpdateForGet(results, before);

    return results;
  }

  void metricsUpdateForGet(List<Cell> results, long before) {
    if (this.metricsRegion != null) {
      this.metricsRegion.updateGet(EnvironmentEdgeManager.currentTime() - before);
    }
    if (rsServices != null && this.rsServices.getMetrics() != null) {
      rsServices.getMetrics().updateReadQueryMeter(this, 1);
    }
  }

  @Override
  public Result mutateRow(RowMutations rm) throws IOException {
    return mutateRow(rm, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public Result mutateRow(RowMutations rm, long nonceGroup, long nonce) throws IOException {
    final List<Mutation> m = rm.getMutations();
    OperationStatus[] statuses = batchMutate(m.toArray(new Mutation[0]), true, nonceGroup, nonce);

    List<Result> results = new ArrayList<>();
    for (OperationStatus status : statuses) {
      if (status.getResult() != null) {
        results.add(status.getResult());
      }
    }

    if (results.isEmpty()) {
      return null;
    }

    // Merge the results of the Increment/Append operations
    List<Cell> cells = new ArrayList<>();
    for (Result result : results) {
      if (result.rawCells() != null) {
        cells.addAll(Arrays.asList(result.rawCells()));
      }
    }
    return Result.create(cells);
  }

  /**
   * Perform atomic (all or none) mutations within the region.
   * @param mutations  The list of mutations to perform. <code>mutations</code> can contain
   *                   operations for multiple rows. Caller has to ensure that all rows are
   *                   contained in this region.
   * @param rowsToLock Rows to lock
   * @param nonceGroup Optional nonce group of the operation (client Id)
   * @param nonce      Optional nonce of the operation (unique random id to ensure "more
   *                   idempotence") If multiple rows are locked care should be taken that
   *                   <code>rowsToLock</code> is sorted in order to avoid deadlocks.
   */
  @Override
  public void mutateRowsWithLocks(Collection<Mutation> mutations, Collection<byte[]> rowsToLock,
    long nonceGroup, long nonce) throws IOException {
    batchMutate(new MutationBatchOperation(this, mutations.toArray(new Mutation[mutations.size()]),
      true, nonceGroup, nonce) {
      @Override
      public MiniBatchOperationInProgress<Mutation>
        lockRowsAndBuildMiniBatch(List<RowLock> acquiredRowLocks) throws IOException {
        RowLock prevRowLock = null;
        for (byte[] row : rowsToLock) {
          try {
            RowLock rowLock = region.getRowLock(row, false, prevRowLock); // write lock
            if (rowLock != prevRowLock) {
              acquiredRowLocks.add(rowLock);
              prevRowLock = rowLock;
            }
          } catch (IOException ioe) {
            LOG.warn("Failed getting lock, row={}, in region {}", Bytes.toStringBinary(row), this,
              ioe);
            throw ioe;
          }
        }
        return createMiniBatch(size(), size());
      }
    });
  }

  /** Returns statistics about the current load of the region */
  public ClientProtos.RegionLoadStats getLoadStatistics() {
    if (!regionStatsEnabled) {
      return null;
    }
    ClientProtos.RegionLoadStats.Builder stats = ClientProtos.RegionLoadStats.newBuilder();
    stats.setMemStoreLoad((int) (Math.min(100,
      (this.memStoreSizing.getMemStoreSize().getHeapSize() * 100) / this.memstoreFlushSize)));
    if (rsServices.getHeapMemoryManager() != null) {
      // the HeapMemoryManager uses -0.0 to signal a problem asking the JVM,
      // so we could just do the calculation below and we'll get a 0.
      // treating it as a special case analogous to no HMM instead so that it can be
      // programatically treated different from using <1% of heap.
      final float occupancy = rsServices.getHeapMemoryManager().getHeapOccupancyPercent();
      if (occupancy != HeapMemoryManager.HEAP_OCCUPANCY_ERROR_VALUE) {
        stats.setHeapOccupancy((int) (occupancy * 100));
      }
    }
    stats.setCompactionPressure((int) (rsServices.getCompactionPressure() * 100 > 100
      ? 100
      : rsServices.getCompactionPressure() * 100));
    return stats.build();
  }

  @Override
  public void processRowsWithLocks(RowProcessor<?, ?> processor) throws IOException {
    processRowsWithLocks(processor, rowProcessorTimeout, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  @Override
  public void processRowsWithLocks(RowProcessor<?, ?> processor, long nonceGroup, long nonce)
    throws IOException {
    processRowsWithLocks(processor, rowProcessorTimeout, nonceGroup, nonce);
  }

  @Override
  public void processRowsWithLocks(RowProcessor<?, ?> processor, long timeout, long nonceGroup,
    long nonce) throws IOException {
    for (byte[] row : processor.getRowsToLock()) {
      checkRow(row, "processRowsWithLocks");
    }
    if (!processor.readOnly()) {
      checkReadOnly();
    }
    checkResources();
    startRegionOperation();
    WALEdit walEdit = new WALEdit();

    // STEP 1. Run pre-process hook
    preProcess(processor, walEdit);
    // Short circuit the read only case
    if (processor.readOnly()) {
      try {
        long now = EnvironmentEdgeManager.currentTime();
        doProcessRowWithTimeout(processor, now, this, null, null, timeout);
        processor.postProcess(this, walEdit, true);
      } finally {
        closeRegionOperation();
      }
      return;
    }

    boolean locked = false;
    List<RowLock> acquiredRowLocks = null;
    List<Mutation> mutations = new ArrayList<>();
    Collection<byte[]> rowsToLock = processor.getRowsToLock();
    // This is assigned by mvcc either explicity in the below or in the guts of the WAL append
    // when it assigns the edit a sequencedid (A.K.A the mvcc write number).
    WriteEntry writeEntry = null;
    MemStoreSizing memstoreAccounting = new NonThreadSafeMemStoreSizing();

    // Check for thread interrupt status in case we have been signaled from
    // #interruptRegionOperation.
    checkInterrupt();

    try {
      boolean success = false;
      try {
        // STEP 2. Acquire the row lock(s)
        acquiredRowLocks = new ArrayList<>(rowsToLock.size());
        RowLock prevRowLock = null;
        for (byte[] row : rowsToLock) {
          // Attempt to lock all involved rows, throw if any lock times out
          // use a writer lock for mixed reads and writes
          RowLock rowLock = getRowLockInternal(row, false, prevRowLock);
          if (rowLock != prevRowLock) {
            acquiredRowLocks.add(rowLock);
            prevRowLock = rowLock;
          }
        }

        // Check for thread interrupt status in case we have been signaled from
        // #interruptRegionOperation. Do it before we take the lock and disable interrupts for
        // the WAL append.
        checkInterrupt();

        // STEP 3. Region lock
        lock(this.updatesLock.readLock(), acquiredRowLocks.isEmpty() ? 1 : acquiredRowLocks.size());
        locked = true;

        // From this point until memstore update this operation should not be interrupted.
        disableInterrupts();

        long now = EnvironmentEdgeManager.currentTime();
        // STEP 4. Let the processor scan the rows, generate mutations and add waledits
        doProcessRowWithTimeout(processor, now, this, mutations, walEdit, timeout);
        if (!mutations.isEmpty()) {
          writeRequestsCount.add(mutations.size());
          // STEP 5. Call the preBatchMutate hook
          processor.preBatchMutate(this, walEdit);

          // STEP 6. Append and sync if walEdit has data to write out.
          if (!walEdit.isEmpty()) {
            writeEntry = doWALAppend(walEdit, getEffectiveDurability(processor.useDurability()),
              processor.getClusterIds(), now, nonceGroup, nonce);
          } else {
            // We are here if WAL is being skipped.
            writeEntry = this.mvcc.begin();
          }

          // STEP 7. Apply to memstore
          long sequenceId = writeEntry.getWriteNumber();
          for (Mutation m : mutations) {
            // Handle any tag based cell features.
            // TODO: Do we need to call rewriteCellTags down in applyToMemStore()? Why not before
            // so tags go into WAL?
            rewriteCellTags(m.getFamilyCellMap(), m);
            for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance();) {
              Cell cell = cellScanner.current();
              if (walEdit.isEmpty()) {
                // If walEdit is empty, we put nothing in WAL. WAL stamps Cells with sequence id.
                // If no WAL, need to stamp it here.
                PrivateCellUtil.setSequenceId(cell, sequenceId);
              }
              applyToMemStore(getStore(cell), cell, memstoreAccounting);
            }
          }

          // STEP 8. call postBatchMutate hook
          processor.postBatchMutate(this);

          // STEP 9. Complete mvcc.
if(KnobRuntime.check(java.util.UUID.fromString("7c2aed5a-6cc4-3bd3-a1d3-e997da03db1e"))) {
try {
    java.lang.reflect.Field field = writeEntry.getClass().getDeclaredField("writeNumber");
    field.setAccessible(true);
    long oldValue = ((long)field.get(writeEntry));
    field.set(writeEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17f2332c-abd6-3444-bf3d-c3166c95b0fc"))) {
try {
    java.lang.reflect.Field field = writeEntry.getClass().getDeclaredField("completed");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(writeEntry);
    field.set(writeEntry, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          mvcc.completeAndWait(writeEntry);
          writeEntry = null;

          // STEP 10. Release region lock
          if (locked) {
            this.updatesLock.readLock().unlock();
            locked = false;
          }

          // STEP 11. Release row lock(s)
          releaseRowLocks(acquiredRowLocks);

          if (rsServices != null && rsServices.getMetrics() != null) {
            rsServices.getMetrics().updateWriteQueryMeter(this, mutations.size());
          }
        }
        success = true;
      } finally {
        // Call complete rather than completeAndWait because we probably had error if walKey != null
        if (writeEntry != null) mvcc.complete(writeEntry);
        if (locked) {
          this.updatesLock.readLock().unlock();
        }
        // release locks if some were acquired but another timed out
        releaseRowLocks(acquiredRowLocks);

        enableInterrupts();
      }

      // 12. Run post-process hook
      processor.postProcess(this, walEdit, success);
    } finally {
      closeRegionOperation();
      if (!mutations.isEmpty()) {
        this.incMemStoreSize(memstoreAccounting.getMemStoreSize());
        requestFlushIfNeeded();
      }
    }
  }

  private void preProcess(final RowProcessor<?, ?> processor, final WALEdit walEdit)
    throws IOException {
    try {
      processor.preProcess(this, walEdit);
    } catch (IOException e) {
      closeRegionOperation();
      throw e;
    }
  }

  private void doProcessRowWithTimeout(final RowProcessor<?, ?> processor, final long now,
    final HRegion region, final List<Mutation> mutations, final WALEdit walEdit, final long timeout)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f58956cb-4c69-3724-bf54-71c11822b9cf"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("250dd558-16d8-378d-b3a0-db65d0f75273"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f591f1f1-7ebd-36ca-a563-1f82898dce6e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("737fdd37-7039-3346-99d9-d24860c79ed5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fbb2036-9a49-3d5f-aee8-d2874bbbd75c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d1c98df-38f6-3a96-aa30-51fac935cf1c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc284b81-5886-3d1c-8d3e-4d8c0b8c2e4d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f222fcc-ba80-3728-9e96-f78c138b8d59"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6a6f8e1c-dbc1-3973-90f1-2dd698f98868"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e67d1c23-1e60-32c8-8f17-dad12d3b5346"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8ad041f-1864-3a3a-8791-cbd4b8f22823"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d076dae-d759-302d-ac2a-95fb2ce444da"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf2f9818-c1e5-3e0a-b5cd-86104e54f0f3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8a5599e-5808-371b-a93e-e7c24601e5cb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a4258ef-6fe5-3cd0-a800-40f61256fdbb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b927778-9a0b-3953-9039-07efe4c697e0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8321a4de-3741-3f72-88d0-96af6b947bfa"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("468bd0a5-c7c9-387e-8a25-61721d5fad2d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57688b53-31d2-3078-9bfc-2a7550e200b6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db32edf2-f855-3c0c-94a6-413205397ca6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2e5d48a3-e137-39f4-a8bf-cc2126a94e6b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4be9fcb5-96f0-34d7-b976-30865036e944"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53ddcaae-db63-3eab-b3a6-b57b55b878b3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd014854-2744-3815-92a5-d18cd654dd63"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3ab7046-72ee-3bec-879b-0646adb72edd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4184b790-73b3-3587-9f1b-5ceba3d5bb75"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62c55165-dd22-33d1-84a9-e2d436317528"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff7a22d4-8c4c-38bd-a6a3-94b5ef1d29f8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("434ab723-bb02-302c-9502-ff1fa62a8bb6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("182176a9-50b4-3fc6-9704-87edc35c1830"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e0ac9eb-5dcc-3804-9022-d936d0445c6e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf42af32-0ab3-3842-a683-15a8a853bc58"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1079bd7d-fa84-399d-be3d-1acf4bef33f4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6d23049d-421b-30ba-b908-49075c847999"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d13edd9-778e-3072-b6ec-e56b6393a8bb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61335851-21cd-353f-9925-e0d224e68459"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63c7ee1d-295b-3149-a69a-629cbcb46c40"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55495296-15d0-31dc-9215-bc072b30dc0e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05832dee-fe6d-3dd0-9498-f34d81435aa2"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // Short circuit the no time bound case.
    if (((KnobRuntime.check(java.util.UUID.fromString("4be9062e-09e0-33a9-a9f7-2e6d7e661e41"))) ? ((timeout) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a97cf84f-bafc-3bb5-a491-1d056d7aaa6b"))) ? ((timeout) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("90fff8e1-8ee3-3000-baa1-c7afcb600886"))) ? ((timeout) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("efbac417-658a-37cc-b760-8dd59e176e5e"))) ? ((timeout) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5e19537f-3ae9-354c-94de-206472a4a776"))) ? ((timeout) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7e96be43-8a45-3d50-969d-1349fa031969"))) ? ((timeout) == (0)) : (timeout < 0))))))))))))) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("139c8e16-111d-36bd-8755-509b45b45793"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("19da30f9-db22-38be-b4ad-5065c1ec41db"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50502aa0-fd9e-3b5f-ab78-8cf624986c73"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f546036a-6190-3a67-8561-0f3d695115db"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ddc9f757-1064-3c64-b11c-2ffc7dcbecf1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b5c9379-dd55-3885-9b17-25ffbb645dcc"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb5ce70a-67fc-3ffb-8636-201d8030566a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04d4a042-772c-3740-a110-295c5a6a8086"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2043b9c-c37e-328e-a651-0503be5c001a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("368640b5-b0fb-3276-8ae8-afe04168a859"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d31a14fe-1ea0-3220-8fd4-87c88d6dbd71"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("027c37df-8191-3894-b8d2-f5f0dcd9fcd8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9739d8e3-7355-3f7d-beab-b72b4458f5a0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b12af43f-615a-3768-9caf-092a9b6ef0af"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("607d6d11-5d28-37dd-bbc1-2b0047e1cef3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3dea8c79-9cf4-3007-9d31-e2f1dbcca000"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13837021-d40b-34c9-aaad-e74fb901cd1d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a033306c-6129-3832-8f10-3fa9ad74baf1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("870121a6-aa68-3f50-bb5f-28a1f8be5512"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dbc3fa73-48b9-3b60-b654-f97d89a23c0a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("070650ab-ee84-325b-a637-e9c9787033a3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bc9d74a0-6fbe-390d-a3fc-b44ab4983af3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59aca7cf-7e45-3b14-9283-248ad9cdc6b7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fd1eb89-90b9-3806-b90d-6722bb91a281"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("444abfbb-d088-351b-b40c-a523c1637f06"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9922eaa8-82e1-30a7-ad17-6340eff00602"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9cea8024-8beb-3107-81bc-a2c28f822baa"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86da4407-af3c-3a70-a5d6-a12e5fc2d4f7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c748a8cd-ee0f-33fa-8f86-28a8e354e5ea"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d9552a2-b257-3d33-bda0-ff788dfb265a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("948b443e-6aab-3e45-8df4-91b21fe29eea"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75d8d75e-fcd8-33ea-b39a-a2eefd4e6956"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a92efb22-aeb5-3dd8-bc7a-02098bd7adb6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87b43d02-74f3-3218-8527-4bd89a9ac055"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("579d4279-c594-36b0-b3f9-8c547376c687"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("087bdd9a-ccf5-3ab3-8ce0-e92f4cc85ab3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb4affde-f03b-3299-96c1-37162c73c227"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e77cd24e-6877-32c3-8e8e-a40eb8d17f83"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        processor.process(now, region, mutations, walEdit);
      } catch (IOException e) {
        String row = processor.getRowsToLock().isEmpty()
          ? ""
          : " on row(s):" + Bytes.toStringBinary(processor.getRowsToLock().iterator().next())
            + "...";
        LOG.warn("RowProcessor: {}, in region {}, throws Exception {}",
          processor.getClass().getName(), getRegionInfo().getRegionNameAsString(), row, e);
        throw e;
      }
      return;
    }

    // Case with time bound
    FutureTask<Void> task = new FutureTask<>(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2a73a270-e1fa-3866-bd8f-55129b9b6470"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("dffa3d25-7766-3df4-b5cf-a91b40a26c47"))) {
throw new java.io.IOException("Injected exception");
}
        try {
if(KnobRuntime.check(java.util.UUID.fromString("914c92a2-2cb6-3949-a605-b83a39d235da"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efc3d50c-f590-38c3-a70a-198d648d2bb4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1388b0a7-d61c-3066-a3be-1f414b1be4e8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f81b4d2-5c1a-3465-9790-576181a6e278"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68b7c9cd-09ff-3649-ba9f-ad1c8a2c5c2c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d64bec3e-1284-346e-85e0-74056ff12ae5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4782fab-cf71-3624-a43e-181c4fc0f694"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb4402d8-4db4-3647-b89b-c0c14c967e01"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe7a0c56-8a0f-3f3c-b251-de5b792bd8f7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b43f8e1e-8218-3805-9c6c-ef148085a023"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e155ec30-6950-3176-981a-1a10f1848e15"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebf82bbe-cfb2-3259-a221-2568cb589654"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("646f9853-9b97-3712-829a-238de848c3cd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("136e25e8-372c-3697-b90b-d7253390c26b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60e1615f-851b-371e-9d51-695b246d7a00"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0c136e7-bdde-3876-add0-bd78c5f43f7d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fda25b5b-8b0c-300b-a4a2-1319c1a14125"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f14029a-1084-3a8f-a436-6a7cf5725ff1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3096a250-09c3-3378-beba-fabee6d32430"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e621e4af-6f9a-309c-8d9e-7ab905c064e6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9bcae100-0514-3320-941a-6fa16b9c2076"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bdd73ca5-9a1e-3baf-851e-93c71fd6185f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("baea660c-a99f-38aa-bb18-2e083532de68"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afda1c16-68f9-3cbb-9eda-41086f0d7aa8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c2582ac-39da-3407-987f-7ea62b7bc381"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a7bb134-6d6c-3a35-b458-d5b16ebaed89"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54eb9638-3be0-32af-9abc-786c5d7bde7d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c2713e7-837f-358c-8945-b2f0e5acb12e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ded7968f-6672-342d-88ea-ac4d7f868c44"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("031e3155-685f-3aa9-bf29-b5569b2a85ce"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a1dfc2f4-a070-3bae-ac8f-7ad0d9061639"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a334e06-d57e-3c22-823a-c8f5948b35f7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce91e47c-5ac4-38d2-be63-718397e651ab"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4faa1bf8-623a-33f9-9017-9df297190b3c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70679d14-89ec-355a-9684-f5a72d5bce6c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("951b771a-fa3f-35bf-8885-bf84084a8533"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc65dc46-9a0f-3bf1-8b6d-e560573b78d0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93000863-8155-358c-8a89-207bcaf105dc"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          processor.process(now, region, mutations, walEdit);
          return null;
        } catch (IOException e) {
          String row = processor.getRowsToLock().isEmpty()
            ? ""
            : " on row(s):" + Bytes.toStringBinary(processor.getRowsToLock().iterator().next())
              + "...";
          LOG.warn("RowProcessor: {}, in region {}, throws Exception {}",
            processor.getClass().getName(), getRegionInfo().getRegionNameAsString(), row, e);
          throw e;
        }
      }
    });
    rowProcessorExecutor.execute(task);
    try {
      task.get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      throw throwOnInterrupt(ie);
    } catch (TimeoutException te) {
      String row = processor.getRowsToLock().isEmpty()
        ? ""
        : " on row(s):" + Bytes.toStringBinary(processor.getRowsToLock().iterator().next()) + "...";
      LOG.error("RowProcessor timeout: {} ms, in region {}, {}", timeout,
        getRegionInfo().getRegionNameAsString(), row);
      throw new IOException(te);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result append(Append append) throws IOException {
    return append(append, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public Result append(Append append, long nonceGroup, long nonce) throws IOException {
    return TraceUtil.trace(() -> {
      checkReadOnly();
      checkResources();
      startRegionOperation(Operation.APPEND);
      try {
        // All edits for the given row (across all column families) must happen atomically.
        return mutate(append, true, nonceGroup, nonce).getResult();
      } finally {
        closeRegionOperation(Operation.APPEND);
      }
    }, () -> createRegionSpan("Region.append"));
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return increment(increment, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public Result increment(Increment increment, long nonceGroup, long nonce) throws IOException {
    return TraceUtil.trace(() -> {
      checkReadOnly();
      checkResources();
      startRegionOperation(Operation.INCREMENT);
      try {
        // All edits for the given row (across all column families) must happen atomically.
        return mutate(increment, true, nonceGroup, nonce).getResult();
      } finally {
        closeRegionOperation(Operation.INCREMENT);
      }
    }, () -> createRegionSpan("Region.increment"));
  }

  private WriteEntry doWALAppend(WALEdit walEdit, Durability durability, List<UUID> clusterIds,
    long now, long nonceGroup, long nonce) throws IOException {
    return doWALAppend(walEdit, durability, clusterIds, now, nonceGroup, nonce,
      SequenceId.NO_SEQUENCE_ID);
  }

  /** Returns writeEntry associated with this append */
  private WriteEntry doWALAppend(WALEdit walEdit, Durability durability, List<UUID> clusterIds,
    long now, long nonceGroup, long nonce, long origLogSeqNum) throws IOException {
    Preconditions.checkArgument(walEdit != null && !walEdit.isEmpty(), "WALEdit is null or empty!");
    Preconditions.checkArgument(!walEdit.isReplay() || origLogSeqNum != SequenceId.NO_SEQUENCE_ID,
      "Invalid replay sequence Id for replay WALEdit!");
    // Using default cluster id, as this can only happen in the originating cluster.
    // A slave cluster receives the final value (not the delta) as a Put. We use HLogKey
    // here instead of WALKeyImpl directly to support legacy coprocessors.
    WALKeyImpl walKey = walEdit.isReplay()
      ? new WALKeyImpl(this.getRegionInfo().getEncodedNameAsBytes(),
        this.htableDescriptor.getTableName(), SequenceId.NO_SEQUENCE_ID, now, clusterIds,
        nonceGroup, nonce, mvcc)
      : new WALKeyImpl(this.getRegionInfo().getEncodedNameAsBytes(),
        this.htableDescriptor.getTableName(), SequenceId.NO_SEQUENCE_ID, now, clusterIds,
        nonceGroup, nonce, mvcc, this.getReplicationScope());
    if (walEdit.isReplay()) {
      walKey.setOrigLogSeqNum(origLogSeqNum);
    }
    // don't call the coproc hook for writes to the WAL caused by
    // system lifecycle events like flushes or compactions
    if (this.coprocessorHost != null && !walEdit.isMetaEdit()) {
      this.coprocessorHost.preWALAppend(walKey, walEdit);
    }
    WriteEntry writeEntry = null;
    try {
      long txid = this.wal.appendData(this.getRegionInfo(), walKey, walEdit);
      // Call sync on our edit.
      if (txid != 0) {
        sync(txid, durability);
      }
      writeEntry = walKey.getWriteEntry();
    } catch (IOException ioe) {
      if (walKey != null && walKey.getWriteEntry() != null) {
        mvcc.complete(walKey.getWriteEntry());
      }

      /**
       * If {@link WAL#sync} get a timeout exception, the only correct way is to abort the region
       * server, as the design of {@link WAL#sync}, is to succeed or die, there is no 'failure'. It
       * is usually not a big deal is because we set a very large default value(5 minutes) for
       * {@link AbstractFSWAL#WAL_SYNC_TIMEOUT_MS}, usually the WAL system will abort the region
       * server if it can not finish the sync within 5 minutes.
       */
      if (ioe instanceof WALSyncTimeoutIOException) {
        if (rsServices != null) {
          rsServices.abort("WAL sync timeout,forcing server shutdown", ioe);
        }
      }
      throw ioe;
    }
    return writeEntry;
  }

  //
  // New HBASE-880 Helpers
  //
  void checkFamily(final byte[] family) throws NoSuchColumnFamilyException {
    if (!this.htableDescriptor.hasColumnFamily(family)) {
      throw new NoSuchColumnFamilyException("Column family " + Bytes.toString(family)
        + " does not exist in region " + this + " in table " + this.htableDescriptor);
    }
  }

  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HRegion.class, false);

  // woefully out of date - currently missing:
  // 1 x HashMap - coprocessorServiceHandlers
  // 6 x LongAdder - numMutationsWithoutWAL, dataInMemoryWithoutWAL,
  // checkAndMutateChecksPassed, checkAndMutateChecksFailed, readRequestsCount,
  // writeRequestsCount
  // 1 x HRegion$WriteState - writestate
  // 1 x RegionCoprocessorHost - coprocessorHost
  // 1 x RegionSplitPolicy - splitPolicy
  // 1 x MetricsRegion - metricsRegion
  // 1 x MetricsRegionWrapperImpl - metricsRegionWrapper
  // 1 x ReadPointCalculationLock - smallestReadPointCalcLock
  public static final long DEEP_OVERHEAD = FIXED_OVERHEAD + ClassSize.OBJECT + // closeLock
    (2 * ClassSize.ATOMIC_BOOLEAN) + // closed, closing
    (3 * ClassSize.ATOMIC_LONG) + // numPutsWithoutWAL, dataInMemoryWithoutWAL,
                                  // compactionsFailed
    (3 * ClassSize.CONCURRENT_HASHMAP) + // lockedRows, scannerReadPoints, regionLockHolders
    WriteState.HEAP_SIZE + // writestate
    ClassSize.CONCURRENT_SKIPLISTMAP + ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + // stores
    (2 * ClassSize.REENTRANT_LOCK) + // lock, updatesLock
    MultiVersionConcurrencyControl.FIXED_SIZE // mvcc
    + 2 * ClassSize.TREEMAP // maxSeqIdInStores, replicationScopes
    + 2 * ClassSize.ATOMIC_INTEGER // majorInProgress, minorInProgress
    + ClassSize.STORE_SERVICES // store services
    + StoreHotnessProtector.FIXED_SIZE;

  @Override
  public long heapSize() {
    // this does not take into account row locks, recent flushes, mvcc entries, and more
    return DEEP_OVERHEAD + stores.values().stream().mapToLong(HStore::heapSize).sum();
  }

  /**
   * Registers a new protocol buffer {@link Service} subclass as a coprocessor endpoint to be
   * available for handling Region#execService(com.google.protobuf.RpcController,
   * org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall) calls.
   * <p>
   * Only a single instance may be registered per region for a given {@link Service} subclass (the
   * instances are keyed on {@link com.google.protobuf.Descriptors.ServiceDescriptor#getFullName()}.
   * After the first registration, subsequent calls with the same service name will fail with a
   * return value of {@code false}.
   * </p>
   * @param instance the {@code Service} subclass instance to expose as a coprocessor endpoint
   * @return {@code true} if the registration was successful, {@code false} otherwise
   */
  public boolean registerService(com.google.protobuf.Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor service {} already registered, rejecting request from {} in region {}",
        serviceName, instance, this);
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered coprocessor service: region="
        + Bytes.toStringBinary(getRegionInfo().getRegionName()) + " service=" + serviceName);
    }
    return true;
  }

  /**
   * Executes a single protocol buffer coprocessor endpoint {@link Service} method using the
   * registered protocol handlers. {@link Service} implementations must be registered via the
   * {@link #registerService(com.google.protobuf.Service)} method before they are available.
   * @param controller an {@code RpcContoller} implementation to pass to the invoked service
   * @param call       a {@code CoprocessorServiceCall} instance identifying the service, method,
   *                   and parameters for the method invocation
   * @return a protocol buffer {@code Message} instance containing the method's result
   * @throws IOException if no registered service handler is found or an error occurs during the
   *                     invocation
   * @see #registerService(com.google.protobuf.Service)
   */
  public com.google.protobuf.Message execService(com.google.protobuf.RpcController controller,
    CoprocessorServiceCall call) throws IOException {
    String serviceName = call.getServiceName();
    com.google.protobuf.Service service = coprocessorServiceHandlers.get(serviceName);
    if (service == null) {
      throw new UnknownProtocolException(null, "No registered coprocessor service found for "
        + serviceName + " in region " + Bytes.toStringBinary(getRegionInfo().getRegionName()));
    }
    com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();

    String methodName = call.getMethodName();
    com.google.protobuf.Descriptors.MethodDescriptor methodDesc =
      CoprocessorRpcUtils.getMethodDescriptor(methodName, serviceDesc);

    com.google.protobuf.Message.Builder builder =
      service.getRequestPrototype(methodDesc).newBuilderForType();

    org.apache.hadoop.hbase.protobuf.ProtobufUtil.mergeFrom(builder,
      call.getRequest().toByteArray());
    com.google.protobuf.Message request =
      CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());

    if (coprocessorHost != null) {
      request = coprocessorHost.preEndpointInvocation(service, methodName, request);
    }

    final com.google.protobuf.Message.Builder responseBuilder =
      service.getResponsePrototype(methodDesc).newBuilderForType();
if(KnobRuntime.check(java.util.UUID.fromString("5473a3b5-b188-358d-8977-b4a5569a103a"))) {
try {
    java.lang.reflect.Field field = methodDesc.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(methodDesc));
    field.set(methodDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35269266-b183-3154-a183-c5470adc6472"))) {
try {
    java.lang.reflect.Field field = methodDesc.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(methodDesc));
    field.set(methodDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6fd36a17-036b-361c-9aca-810e95ac000b"))) {
try {
    java.lang.reflect.Field field = methodDesc.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(methodDesc));
    field.set(methodDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f0149cd-551a-3f70-bd38-52a42bcb59fc"))) {
try {
    java.lang.reflect.Field field = methodDesc.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(methodDesc));
    field.set(methodDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    service.callMethod(methodDesc, controller, request,
      new com.google.protobuf.RpcCallback<com.google.protobuf.Message>() {
        @Override
        public void run(com.google.protobuf.Message message) {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
        }
      });

    if (((KnobRuntime.check(java.util.UUID.fromString("67e4d17c-7430-35ec-91d4-24c35a465694"))) ? ((coprocessorHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e8c56ac0-8385-35f8-ad01-97ff032b0e5b"))) ? ((coprocessorHost) != (null)) : (coprocessorHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("ce7678a5-6e43-3d4d-9ee4-71b4b6978ca3"))) {
throw new java.io.IOException("Injected exception");
}
      coprocessorHost.postEndpointInvocation(service, methodName, request, responseBuilder);
    }
    IOException exception =
      org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.getControllerException(controller);
    if (exception != null) {
      throw exception;
    }

    return responseBuilder.build();
  }

  public Optional<byte[]> checkSplit() {
    return checkSplit(false);
  }

  /**
   * Return the split point. An empty result indicates the region isn't splittable.
   */
  public Optional<byte[]> checkSplit(boolean force) {
    // Can't split META
    if (
      this.getRegionInfo().isMetaRegion()
        || TableName.NAMESPACE_TABLE_NAME.equals(this.getRegionInfo().getTable())
    ) {
      return Optional.empty();
    }

    // Can't split a region that is closing.
    if (this.isClosing()) {
      return Optional.empty();
    }

    if (!force && !splitPolicy.shouldSplit()) {
      return Optional.empty();
    }

    byte[] ret = splitPolicy.getSplitPoint();
    if (ret != null && ret.length > 0) {
      ret = splitRestriction.getRestrictedSplitPoint(ret);
    }

    if (ret != null) {
      try {
        checkRow(ret, "calculated split");
      } catch (IOException e) {
        LOG.error("Ignoring invalid split for region {}", this, e);
        return Optional.empty();
      }
      return Optional.of(ret);
    } else {
      return Optional.empty();
    }
  }

  /** Returns The priority that this region should have in the compaction queue */
  public int getCompactPriority() {
    if (conf.getBoolean(SPLIT_IGNORE_BLOCKING_ENABLED_KEY, false) && checkSplit().isPresent()) {
      // if a region should split, split it before compact
      return Store.PRIORITY_USER;
    }
    return stores.values().stream().mapToInt(HStore::getCompactPriority).min()
      .orElse(Store.NO_PRIORITY);
  }

  /** Returns the coprocessor host */
  public RegionCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /** @param coprocessorHost the new coprocessor host */
  public void setCoprocessorHost(final RegionCoprocessorHost coprocessorHost) {
    this.coprocessorHost = coprocessorHost;
  }

  @Override
  public void startRegionOperation() throws IOException {
    startRegionOperation(Operation.ANY);
  }

  @Override
  public void startRegionOperation(Operation op) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("82bf39d9-c972-3625-9140-622ef1728685"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("9c95093d-6213-35fa-a21c-8bf9d693721a"))) {
throw new java.io.IOException("Injected exception");
}
    boolean isInterruptableOp = false;
    switch (op) {
      case GET: // interruptible read operations
      case SCAN:
        isInterruptableOp = true;
        checkReadsEnabled();
        break;
      case INCREMENT: // interruptible write operations
      case APPEND:
      case PUT:
      case DELETE:
      case BATCH_MUTATE:
      case CHECK_AND_MUTATE:
        isInterruptableOp = true;
        break;
      default: // all others
        break;
    }
    if (
      op == Operation.MERGE_REGION || op == Operation.SPLIT_REGION || op == Operation.COMPACT_REGION
        || op == Operation.COMPACT_SWITCH
    ) {
      // split, merge or compact region doesn't need to check the closing/closed state or lock the
      // region
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("3ed8b915-c758-323b-916e-15ce8342f074"))) ? (isClosed()) : (((KnobRuntime.check(java.util.UUID.fromString("6012ddfb-8802-35db-b4df-1cbc0eb62413"))) ? (isClosing()) : (this.closing.get()))))) {
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closing");
    }
if(KnobRuntime.check(java.util.UUID.fromString("8d4513e3-393b-3a75-aec5-ece084a11d5c"))) {
throw new java.io.IOException("Injected exception");
}
    lock(lock.readLock());
    // Update regionLockHolders ONLY for any startRegionOperation call that is invoked from
    // an RPC handler
    Thread thisThread = Thread.currentThread();
    if (((KnobRuntime.check(java.util.UUID.fromString("b38ac8c7-4b07-3b23-a065-c7fd44207b91"))) ? (!isInterruptableOp) : (isInterruptableOp))) {
      regionLockHolders.put(thisThread, true);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("183a15bb-63d6-30db-83f7-6d430399f45f"))) ? (isClosed()) : (((KnobRuntime.check(java.util.UUID.fromString("679eabee-d9d4-3938-9f3b-2c88e49b5111"))) ? (isClosing()) : (this.closed.get()))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("740f5b63-86aa-3723-8286-12896b645a27"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("b7c46df9-7b77-333d-a2ce-00c7b6e5271b"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("a97fae55-e460-3f0e-9740-a627f878b7b9"))) { waitForFlushesAndCompactions(); } else { lock.readLock().unlock(); }
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closed");
    }
    // The unit for snapshot is a region. So, all stores for this region must be
    // prepared for snapshot operation before proceeding.
    if (((KnobRuntime.check(java.util.UUID.fromString("4a0fd790-a46d-3564-b199-7bdde99462a9"))) ? ((op) != (Operation.SNAPSHOT)) : (((KnobRuntime.check(java.util.UUID.fromString("5b2d8a8b-8202-3edc-8a5f-3d7fd7189383"))) ? ((op) == (Operation.SNAPSHOT)) : (op == Operation.SNAPSHOT))))) {
      stores.values().forEach(HStore::preSnapshotOperation);
    }
    try {
      if (coprocessorHost != null) {
        coprocessorHost.postStartRegionOperation(op);
      }
    } catch (Exception e) {
      if (isInterruptableOp) {
        // would be harmless to remove what we didn't add but we know by 'isInterruptableOp'
        // if we added this thread to regionLockHolders
        regionLockHolders.remove(thisThread);
      }
      lock.readLock().unlock();
      throw new IOException(e);
    }
  }

  @Override
  public void closeRegionOperation() throws IOException {
    if (KnobRuntime.check(java.util.UUID.fromString("88901628-8863-3fbd-b34e-7396fde5dcff"))) { startRegionOperation(Operation.ANY); } else { closeRegionOperation(Operation.ANY); }
  }

  @Override
  public void closeRegionOperation(Operation operation) throws IOException {
    if (operation == Operation.SNAPSHOT) {
      stores.values().forEach(HStore::postSnapshotOperation);
    }
    Thread thisThread = Thread.currentThread();
    regionLockHolders.remove(thisThread);
    if (KnobRuntime.check(java.util.UUID.fromString("fccb9fd6-1ab2-370c-ae17-2238b28598a0"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("2c8d9c8e-c525-3262-a197-fd01f444b1a7"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("6c081703-b61a-3c9f-a811-ad81f22f103e"))) { interruptRegionOperations(); } else { lock.readLock().unlock(); }
    if (((KnobRuntime.check(java.util.UUID.fromString("768fdbed-7b2e-3014-b362-8fc22c115ddc"))) ? ((coprocessorHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("453e065f-4d32-3339-8c72-4bb856ef6c41"))) ? ((coprocessorHost) != (null)) : (coprocessorHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("6c4108f5-ab4b-3767-b377-1ff4b8f68e91"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("7ad36b76-8cdd-386d-bda9-492a048f98c7"))) { closeRegionOperation(operation); } else if (KnobRuntime.check(java.util.UUID.fromString("ac9ddaca-3558-39c2-9936-c7feb52e4cae"))) { startRegionOperation(operation); } else { coprocessorHost.postCloseRegionOperation(operation); }
    }
  }

  /**
   * This method needs to be called before any public call that reads or modifies stores in bulk. It
   * has to be called just before a try. #closeBulkRegionOperation needs to be called in the try's
   * finally block Acquires a writelock and checks if the region is closing or closed.
   * @throws NotServingRegionException when the region is closing or closed
   * @throws RegionTooBusyException    if failed to get the lock in time
   * @throws InterruptedIOException    if interrupted while waiting for a lock
   */
  private void startBulkRegionOperation(boolean writeLockNeeded) throws IOException {
    if (this.closing.get()) {
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closing");
    }
if(KnobRuntime.check(java.util.UUID.fromString("3753204c-f54d-3fd2-bf33-2db84c27a56d"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ccaebfea-9767-34ff-9bfe-1070256e7b39"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("e4768641-8e81-3be0-955c-b0b7911ced48"))) ? (!writeLockNeeded) : (writeLockNeeded))))) lock(lock.writeLock());
    else lock(lock.readLock());
    if (this.closed.get()) {
      if (writeLockNeeded) lock.writeLock().unlock();
      else if (KnobRuntime.check(java.util.UUID.fromString("c3b9ea9a-6d7a-376e-82d8-dc2f919510bf"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("034bc882-afcb-3057-aa95-8a7ce6572846"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("70092ab6-aa26-3a22-9829-b36992fee156"))) { setHTableSpecificConf(); } else { lock.readLock().unlock(); }
      throw new NotServingRegionException(getRegionInfo().getRegionNameAsString() + " is closed");
    }
    regionLockHolders.put(Thread.currentThread(), true);
  }

  /**
   * Closes the lock. This needs to be called in the finally block corresponding to the try block of
   * #startRegionOperation
   */
  private void closeBulkRegionOperation() {
    regionLockHolders.remove(Thread.currentThread());
    if (lock.writeLock().isHeldByCurrentThread()) lock.writeLock().unlock();
    else lock.readLock().unlock();
  }

  /**
   * Update LongAdders for number of puts without wal and the size of possible data loss. These
   * information are exposed by the region server metrics.
   */
  private void recordMutationWithoutWal(final Map<byte[], List<Cell>> familyMap) {
    numMutationsWithoutWAL.increment();
    if (numMutationsWithoutWAL.sum() <= 1) {
      LOG.info("writing data to region " + this
        + " with WAL disabled. Data may be lost in the event of a crash.");
    }

    long mutationSize = 0;
    for (List<Cell> cells : familyMap.values()) {
      // Optimization: 'foreach' loop is not used. See:
      // HBASE-12023 HRegion.applyFamilyMapToMemstore creates too many iterator objects
      assert cells instanceof RandomAccess;
      int listSize = cells.size();
      for (int i = 0; i < listSize; i++) {
        Cell cell = cells.get(i);
        mutationSize += cell.getSerializedSize();
      }
    }

    dataInMemoryWithoutWAL.add(mutationSize);
  }

  private void lock(final Lock lock) throws IOException {
    lock(lock, 1);
  }

  /**
   * Try to acquire a lock. Throw RegionTooBusyException if failed to get the lock in time. Throw
   * InterruptedIOException if interrupted while waiting for the lock.
   */
  private void lock(final Lock lock, final int multiplier) throws IOException {
    try {
      final long waitTime = Math.min(maxBusyWaitDuration,
        busyWaitDuration * Math.min(multiplier, maxBusyWaitMultiplier));
      if (!lock.tryLock(waitTime, TimeUnit.MILLISECONDS)) {
        // Don't print millis. Message is used as a key over in
        // RetriesExhaustedWithDetailsException processing.
        final String regionName =
          this.getRegionInfo() == null ? "unknown" : this.getRegionInfo().getRegionNameAsString();
        final String serverName = this.getRegionServerServices() == null
          ? "unknown"
          : (this.getRegionServerServices().getServerName() == null
            ? "unknown"
            : this.getRegionServerServices().getServerName().toString());
        RegionTooBusyException rtbe = new RegionTooBusyException(
          "Failed to obtain lock; regionName=" + regionName + ", server=" + serverName);
        LOG.warn("Region is too busy to allow lock acquisition.", rtbe);
        throw rtbe;
      }
    } catch (InterruptedException ie) {
      if (((KnobRuntime.check(java.util.UUID.fromString("ff28c0e0-5294-3967-9405-46214b2c6315"))) ? (isClosed()) : (LOG.isDebugEnabled()))) {
        if (KnobRuntime.check(java.util.UUID.fromString("704a6337-49f8-3fec-b383-1794092146d6"))) { LOG.warn("Interrupted while waiting for a lock in region {}", this); } else if (KnobRuntime.check(java.util.UUID.fromString("37541928-03ab-3434-99fb-769b5a70237b"))) { LOG.info("Interrupted while waiting for a lock in region {}", this); } else { LOG.debug("Interrupted while waiting for a lock in region {}", this); }
      }
      throw throwOnInterrupt(ie);
    }
  }

  /**
   * Calls sync with the given transaction ID
   * @param txid should sync up to which transaction
   * @throws IOException If anything goes wrong with DFS
   */
  private void sync(long txid, Durability durability) throws IOException {
    if (this.getRegionInfo().isMetaRegion()) {
      this.wal.sync(txid);
    } else {
      switch (durability) {
        case USE_DEFAULT:
          // do what table defaults to
          if (shouldSyncWAL()) {
            this.wal.sync(txid);
          }
          break;
        case SKIP_WAL:
          // nothing do to
          break;
        case ASYNC_WAL:
          // nothing do to
          break;
        case SYNC_WAL:
          this.wal.sync(txid, false);
          break;
        case FSYNC_WAL:
          this.wal.sync(txid, true);
          break;
        default:
          throw new RuntimeException("Unknown durability " + durability);
      }
    }
  }

  /**
   * Check whether we should sync the wal from the table's durability settings
   */
  private boolean shouldSyncWAL() {
    return regionDurability.ordinal() > Durability.ASYNC_WAL.ordinal();
  }

  /** Returns the latest sequence number that was read from storage when this region was opened */
  public long getOpenSeqNum() {
    return this.openSeqNum;
  }

  @Override
  public Map<byte[], Long> getMaxStoreSeqId() {
    return this.maxSeqIdInStores;
  }

  public long getOldestSeqIdOfStore(byte[] familyName) {
    return wal.getEarliestMemStoreSeqNum(getRegionInfo().getEncodedNameAsBytes(), familyName);
  }

  @Override
  public CompactionState getCompactionState() {
    boolean hasMajor = majorInProgress.get() > 0, hasMinor = minorInProgress.get() > 0;
    return (hasMajor
      ? (hasMinor ? CompactionState.MAJOR_AND_MINOR : CompactionState.MAJOR)
      : (hasMinor ? CompactionState.MINOR : CompactionState.NONE));
  }

  public void reportCompactionRequestStart(boolean isMajor) {
if(KnobRuntime.check(java.util.UUID.fromString("4235892d-eb98-322d-b8b9-79247b159395"))) {
return;
}
    (isMajor ? majorInProgress : minorInProgress).incrementAndGet();
  }

  public void reportCompactionRequestEnd(boolean isMajor, int numFiles, long filesSizeCompacted) {
if(KnobRuntime.check(java.util.UUID.fromString("524b39fd-3bfd-3025-a001-6be7eb89f3da"))) {
return;
}
    int newValue = (isMajor ? majorInProgress : minorInProgress).decrementAndGet();

    // metrics
    if (KnobRuntime.check(java.util.UUID.fromString("99951197-46a8-375a-8863-983e7861b5ef"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("9389bc57-ee07-34c1-8be1-112363d3af48"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("ba872e8f-8b4d-35ab-ba2d-6e9f47934a76"))) { waitForFlushesAndCompactions(); } else { compactionsFinished.increment(); }
    compactionNumFilesCompacted.add(numFiles);
    compactionNumBytesCompacted.add(filesSizeCompacted);

    assert newValue >= 0;
  }

  public void reportCompactionRequestFailure() {
    compactionsFailed.increment();
  }

  public void incrementCompactionsQueuedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("92c81305-fab6-30be-964c-7e4e1e7b4a51"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("848e1d0e-1449-3747-881f-1262c466e82a"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("55c7c82f-501a-3f93-9616-30b90576ee85"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("2477c530-5ba0-3a45-8e11-804d751c69b3"))) { setHTableSpecificConf(); } else { compactionsQueued.increment(); }
  }

  public void decrementCompactionsQueuedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("6527bf58-f821-3788-8606-9ef16b65a2af"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("1d557554-8764-3135-9bb2-2cb1cd855aff"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("483fb644-c225-32a4-9755-7583ca7a0493"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("7ac52593-d734-3320-b0c8-0dbd16605681"))) { setHTableSpecificConf(); } else { compactionsQueued.decrement(); }
  }

  public void incrementFlushesQueuedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("7548057e-6656-3cd3-90c3-1967033ae056"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("b336a88d-0ca3-3c79-9bd4-f3cf7c1fa96d"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("2b38ef8b-fe6f-3990-bfb8-f65b368c9e6a"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("11b6a190-442a-3ade-8cf3-0d23ba90275c"))) { setHTableSpecificConf(); } else { flushesQueued.increment(); }
  }

  protected void decrementFlushesQueuedCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3bf5f3eb-1d07-32b8-a171-1a4d7e1ddc03"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("5be2a53a-8ebc-3bba-83c7-0e89c68185ef"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("9c8bc974-ed00-348a-b883-9bf13f05f6b9"))) { interruptRegionOperations(); } else if (KnobRuntime.check(java.util.UUID.fromString("42fa4836-2024-34d8-9a12-479bf2240845"))) { waitForFlushesAndCompactions(); } else { flushesQueued.decrement(); }
  }

  /**
   * If a handler thread is eligible for interrupt, make it ineligible. Should be paired with
   * {{@link #enableInterrupts()}.
   */
  void disableInterrupts() {
    regionLockHolders.computeIfPresent(Thread.currentThread(), (t, b) -> false);
  }

  /**
   * If a handler thread was made ineligible for interrupt via {{@link #disableInterrupts()}, make
   * it eligible again. No-op if interrupts are already enabled.
   */
  void enableInterrupts() {
    regionLockHolders.computeIfPresent(Thread.currentThread(), (t, b) -> true);
  }

  /**
   * Interrupt any region options that have acquired the region lock via
   * {@link #startRegionOperation(org.apache.hadoop.hbase.regionserver.Region.Operation)}, or
   * {@link #startBulkRegionOperation(boolean)}.
   */
  private void interruptRegionOperations() {
if(KnobRuntime.check(java.util.UUID.fromString("34d9fb2d-3c42-3c7f-bc8f-b94b09141ca0"))) {
return;
}
    for (Map.Entry<Thread, Boolean> entry : regionLockHolders.entrySet()) {
      // An entry in this map will have a boolean value indicating if it is currently
      // eligible for interrupt; if so, we should interrupt it.
      if (entry.getValue().booleanValue()) {
        entry.getKey().interrupt();
      }
    }
  }

  /**
   * Check thread interrupt status and throw an exception if interrupted.
   * @throws NotServingRegionException if region is closing
   * @throws InterruptedIOException    if interrupted but region is not closing
   */
  // Package scope for tests
  void checkInterrupt() throws NotServingRegionException, InterruptedIOException {
if(KnobRuntime.check(java.util.UUID.fromString("ecb4bc44-205a-3964-8d54-1f1f4296053e"))) {
throw new InterruptedIOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6ec2cd12-5dbd-3a3d-ac72-c40964bae416"))) {
return;
}
    if (Thread.interrupted()) {
      if (((KnobRuntime.check(java.util.UUID.fromString("317530b7-04a3-3cbc-97a0-be78038d507d"))) ? (isClosing()) : (((KnobRuntime.check(java.util.UUID.fromString("29994c71-fcb6-31fb-bfe9-ca91aa18699f"))) ? (isClosed()) : (this.closing.get()))))) {
        throw new NotServingRegionException(
          getRegionInfo().getRegionNameAsString() + " is closing");
      }
      throw new InterruptedIOException();
    }
  }

  /**
   * Throw the correct exception upon interrupt
   * @param t cause
   */
  // Package scope for tests
  IOException throwOnInterrupt(Throwable t) {
    if (this.closing.get()) {
      return (NotServingRegionException) new NotServingRegionException(
        getRegionInfo().getRegionNameAsString() + " is closing").initCause(t);
    }
    return (InterruptedIOException) new InterruptedIOException().initCause(t);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onConfigurationChange(Configuration conf) {
    this.storeHotnessProtector.update(conf);
    // update coprocessorHost if the configuration has changed.
    if (
      CoprocessorConfigurationUtil.checkConfigurationChange(getReadOnlyConfiguration(), conf,
        CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY)
    ) {
      LOG.info("Update the system coprocessors because the configuration has changed");
      decorateRegionConfiguration(conf);
      this.coprocessorHost = new RegionCoprocessorHost(this, rsServices, conf);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    configurationManager = manager;
    stores.values().forEach(manager::registerObserver);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    stores.values().forEach(configurationManager::deregisterObserver);
  }

  @Override
  public CellComparator getCellComparator() {
    return cellComparator;
  }

  public long getMemStoreFlushSize() {
    return this.memstoreFlushSize;
  }

  //// method for debugging tests
  void throwException(String title, String regionName) {
    StringBuilder buf = new StringBuilder();
    buf.append(title + ", ");
    buf.append(getRegionInfo().toString());
    buf.append(getRegionInfo().isMetaRegion() ? " meta region " : " ");
    buf.append("stores: ");
    for (HStore s : stores.values()) {
      buf.append(s.getColumnFamilyDescriptor().getNameAsString());
      buf.append(" size: ");
      buf.append(s.getMemStoreSize().getDataSize());
      buf.append(" ");
    }
    buf.append("end-of-stores");
    buf.append(", memstore size ");
    buf.append(getMemStoreDataSize());
    if (getRegionInfo().getRegionNameAsString().startsWith(regionName)) {
      throw new RuntimeException(buf.toString());
    }
  }

  @Override
  public void requestCompaction(String why, int priority, boolean major,
    CompactionLifeCycleTracker tracker) throws IOException {
    if (major) {
      stores.values().forEach(HStore::triggerMajorCompaction);
    }
if(KnobRuntime.check(java.util.UUID.fromString("0aa388c0-8812-38cd-8885-09c5c78fa7f9"))) {
priority += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("33cd16ec-50a2-3833-8776-4872861a5fc6"))) {
priority -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("326eb875-abd9-3a0c-a805-51da44f4e160"))) {
priority = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("765ad833-dd77-3ad7-81fc-62bc890a56bf"))) {
priority /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1e51b21a-3d7c-37c7-9066-a75d459924f6"))) {
priority = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a9fc32a7-3617-3907-ade4-f1f0e4efd60b"))) {
priority *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9abd0ac8-a432-3a0f-8112-89fd735dccdf"))) {
throw new java.io.IOException("Injected exception");
}
    rsServices.getCompactionRequestor().requestCompaction(this, why, priority, tracker,
      RpcServer.getRequestUser().orElse(null));
  }

  @Override
  public void requestCompaction(byte[] family, String why, int priority, boolean major,
    CompactionLifeCycleTracker tracker) throws IOException {
    HStore store = stores.get(family);
    if (store == null) {
      throw new NoSuchColumnFamilyException("column family " + Bytes.toString(family)
        + " does not exist in region " + getRegionInfo().getRegionNameAsString());
    }
    if (major) {
      store.triggerMajorCompaction();
    }
    rsServices.getCompactionRequestor().requestCompaction(this, store, why, priority, tracker,
      RpcServer.getRequestUser().orElse(null));
  }

  private void requestFlushIfNeeded() throws RegionTooBusyException {
if(KnobRuntime.check(java.util.UUID.fromString("d338cb8d-43c0-3fe7-8241-f59e29e5b35f"))) {
return;
}
    if (isFlushSize(this.memStoreSizing.getMemStoreSize())) {
      if (KnobRuntime.check(java.util.UUID.fromString("ea917584-b4d5-3623-80e9-1075911b2e61"))) { waitForFlushesAndCompactions(); } else if (KnobRuntime.check(java.util.UUID.fromString("70218bd4-bfb6-31c2-8b2b-eb26cc187868"))) { setHTableSpecificConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("1981be42-e08b-3733-b19f-96fe09ee1faf"))) { interruptRegionOperations(); } else { requestFlush(); }
    }
  }

  private void requestFlush() {
if(KnobRuntime.check(java.util.UUID.fromString("353e5a43-f4e8-3c07-8b7c-61134ddc3b66"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("31191964-6615-383f-951a-95714c574aa0"))) ? ((this.rsServices) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("27f8b3ec-3dc5-394b-9201-cf4fb4627752"))) ? ((this.rsServices) == (null)) : (this.rsServices == null))))) {
      return;
    }
    requestFlush0(FlushLifeCycleTracker.DUMMY);
  }

  private void requestFlush0(FlushLifeCycleTracker tracker) {
    boolean shouldFlush = false;
    synchronized (writestate) {
      if (!this.writestate.isFlushRequested()) {
        shouldFlush = true;
        writestate.flushRequested = true;
      }
    }
    if (shouldFlush) {
      // Make request outside of synchronize block; HBASE-818.
      this.rsServices.getFlushRequester().requestFlush(this, tracker);
      if (((KnobRuntime.check(java.util.UUID.fromString("a8251699-1160-391e-bcbb-1f772ba3857f"))) ? (isClosed()) : (LOG.isDebugEnabled()))) {
        LOG.debug("Flush requested on " + this.getRegionInfo().getEncodedName());
      }
    } else {
      if (KnobRuntime.check(java.util.UUID.fromString("24c14153-bac8-3877-9443-86ab97591110"))) { tracker.notExecuted(("Flush already requested on ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("c8b0c95f-c418-3bfd-a8be-6e4341957475"))) { tracker.notExecuted("Flush already requested on "); } else { tracker.notExecuted("Flush already requested on " + this); }
    }
  }

  @Override
  public void requestFlush(FlushLifeCycleTracker tracker) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6d12dfdb-0360-3c6c-ab81-266775fab850"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d01b6ed-37b4-3be1-a61d-2ddf550619df"))) {
throw new java.io.IOException("Injected exception");
}
    requestFlush0(tracker);
  }

  /**
   * This method modifies the region's configuration in order to inject replication-related features
   * @param conf region configurations
   */
  private static void decorateRegionConfiguration(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("cc2b5041-b169-3639-980e-65d4840a82e3"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("134199b0-1db2-3e25-8d1f-ed2e15301015"))) {
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
    if (ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
      String plugins = conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
      String replicationCoprocessorClass = ReplicationObserver.class.getCanonicalName();
      if (!plugins.contains(replicationCoprocessorClass)) {
        conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
          (plugins.equals("") ? "" : (plugins + ",")) + replicationCoprocessorClass);
      }
    }
  }

  public void addReadRequestsCount(long readRequestsCount) {
    this.readRequestsCount.add(readRequestsCount);
  }

  public void addWriteRequestsCount(long writeRequestsCount) {
    this.writeRequestsCount.add(writeRequestsCount);
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  boolean isReadsEnabled() {
    return this.writestate.readsEnabled;
  }
}
