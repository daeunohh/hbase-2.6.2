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
package org.apache.hadoop.hbase.regionserver.wal;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.ERROR;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.LOW_REPLICATION;
import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.SLOW_SYNC;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.WAL_IMPL;
import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import com.lmax.disruptor.RingBuffer;
import io.opentelemetry.api.trace.Span;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.MemoryType;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALProvider.WriterBase;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Implementation of {@link WAL} to go against {@link FileSystem}; i.e. keep WALs in HDFS. Only one
 * WAL is ever being written at a time. When a WAL hits a configured maximum size, it is rolled.
 * This is done internal to the implementation.
 * <p>
 * As data is flushed from the MemStore to other on-disk structures (files sorted by key, hfiles), a
 * WAL becomes obsolete. We can let go of all the log edits/entries for a given HRegion-sequence id.
 * A bunch of work in the below is done keeping account of these region sequence ids -- what is
 * flushed out to hfiles, and what is yet in WAL and in memory only.
 * <p>
 * It is only practical to delete entire files. Thus, we delete an entire on-disk file
 * <code>F</code> when all of the edits in <code>F</code> have a log-sequence-id that's older
 * (smaller) than the most-recent flush.
 * <p>
 * To read an WAL, call {@link WALFactory#createStreamReader(FileSystem, Path)} for one way read,
 * call {@link WALFactory#createTailingReader(FileSystem, Path, Configuration, long)} for
 * replication where we may want to tail the active WAL file.
 * <h2>Failure Semantic</h2> If an exception on append or sync, roll the WAL because the current WAL
 * is now a lame duck; any more appends or syncs will fail also with the same original exception. If
 * we have made successful appends to the WAL and we then are unable to sync them, our current
 * semantic is to return error to the client that the appends failed but also to abort the current
 * context, usually the hosting server. We need to replay the WALs. <br>
 * TODO: Change this semantic. A roll of WAL may be sufficient as long as we have flagged client
 * that the append failed. <br>
 * TODO: replication may pick up these last edits though they have been marked as failed append
 * (Need to keep our own file lengths, not rely on HDFS).
 */
@InterfaceAudience.Private
public abstract class AbstractFSWAL<W extends WriterBase> implements WAL {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWAL.class);

  protected static final String SLOW_SYNC_TIME_MS = "hbase.regionserver.wal.slowsync.ms";
  protected static final int DEFAULT_SLOW_SYNC_TIME_MS = 100; // in ms
  protected static final String ROLL_ON_SYNC_TIME_MS = "hbase.regionserver.wal.roll.on.sync.ms";
  protected static final int DEFAULT_ROLL_ON_SYNC_TIME_MS = 10000; // in ms
  protected static final String SLOW_SYNC_ROLL_THRESHOLD =
    "hbase.regionserver.wal.slowsync.roll.threshold";
  protected static final int DEFAULT_SLOW_SYNC_ROLL_THRESHOLD = 100; // 100 slow sync warnings
  protected static final String SLOW_SYNC_ROLL_INTERVAL_MS =
    "hbase.regionserver.wal.slowsync.roll.interval.ms";
  protected static final int DEFAULT_SLOW_SYNC_ROLL_INTERVAL_MS = 60 * 1000; // in ms, 1 minute

  public static final String WAL_SYNC_TIMEOUT_MS = "hbase.regionserver.wal.sync.timeout";
  protected static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000; // in ms, 5min

  public static final String WAL_ROLL_MULTIPLIER = "hbase.regionserver.logroll.multiplier";

  public static final String MAX_LOGS = "hbase.regionserver.maxlogs";

  public static final String RING_BUFFER_SLOT_COUNT =
    "hbase.regionserver.wal.disruptor.event.count";

  public static final String WAL_SHUTDOWN_WAIT_TIMEOUT_MS = "hbase.wal.shutdown.wait.timeout.ms";
  public static final int DEFAULT_WAL_SHUTDOWN_WAIT_TIMEOUT_MS = 15 * 1000;

  public static final String WAL_AVOID_LOCAL_WRITES_KEY =
    "hbase.regionserver.wal.avoid-local-writes";
  public static final boolean WAL_AVOID_LOCAL_WRITES_DEFAULT = false;

  /**
   * file system instance
   */
  protected final FileSystem fs;

  /**
   * WAL directory, where all WAL files would be placed.
   */
  protected final Path walDir;

  /**
   * dir path where old logs are kept.
   */
  protected final Path walArchiveDir;

  /**
   * Matches just those wal files that belong to this wal instance.
   */
  protected final PathFilter ourFiles;

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  protected final String walFilePrefix;

  /**
   * Suffix included on generated wal file names
   */
  protected final String walFileSuffix;

  /**
   * Prefix used when checking for wal membership.
   */
  protected final String prefixPathStr;

  protected final WALCoprocessorHost coprocessorHost;

  /**
   * conf object
   */
  protected final Configuration conf;

  protected final Abortable abortable;

  /** Listeners that are called on WAL events. */
  protected final List<WALActionsListener> listeners = new CopyOnWriteArrayList<>();

  /** Tracks the logs in the process of being closed. */
  protected final Map<String, W> inflightWALClosures = new ConcurrentHashMap<>();

  /**
   * Class that does accounting of sequenceids in WAL subsystem. Holds oldest outstanding sequence
   * id as yet not flushed as well as the most recent edit sequence id appended to the WAL. Has
   * facility for answering questions such as "Is it safe to GC a WAL?".
   */
  protected final SequenceIdAccounting sequenceIdAccounting = new SequenceIdAccounting();

  protected final long slowSyncNs, rollOnSyncNs;
  protected final int slowSyncRollThreshold;
  protected final int slowSyncCheckInterval;
  protected final AtomicInteger slowSyncCount = new AtomicInteger();

  private final long walSyncTimeoutNs;

  // If > than this size, roll the log.
  protected final long logrollsize;

  /**
   * Block size to use writing files.
   */
  protected final long blocksize;

  /*
   * If more than this many logs, force flush of oldest region to the oldest edit goes to disk. If
   * too many and we crash, then will take forever replaying. Keep the number of logs tidy.
   */
  protected final int maxLogs;

  protected final boolean useHsync;

  /**
   * This lock makes sure only one log roll runs at a time. Should not be taken while any other lock
   * is held. We don't just use synchronized because that results in bogus and tedious findbugs
   * warning when it thinks synchronized controls writer thread safety. It is held when we are
   * actually rolling the log. It is checked when we are looking to see if we should roll the log or
   * not.
   */
  protected final ReentrantLock rollWriterLock = new ReentrantLock(true);

  // The timestamp (in ms) when the log file was created.
  protected final AtomicLong filenum = new AtomicLong(-1);

  // Number of transactions in the current Wal.
  protected final AtomicInteger numEntries = new AtomicInteger(0);

  /**
   * The highest known outstanding unsync'd WALEdit transaction id. Usually, we use a queue to pass
   * WALEdit to background consumer thread, and the transaction id is the sequence number of the
   * corresponding entry in queue.
   */
  protected volatile long highestUnsyncedTxid = -1;

  /**
   * Updated to the transaction id of the last successful sync call. This can be less than
   * {@link #highestUnsyncedTxid} for case where we have an append where a sync has not yet come in
   * for it.
   */
  protected final AtomicLong highestSyncedTxid = new AtomicLong(0);

  /**
   * The total size of wal
   */
  protected final AtomicLong totalLogSize = new AtomicLong(0);
  /**
   * Current log file.
   */
  volatile W writer;

  // Last time to check low replication on hlog's pipeline
  private volatile long lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();

  // Last time we asked to roll the log due to a slow sync
  private volatile long lastTimeCheckSlowSync = EnvironmentEdgeManager.currentTime();

  protected volatile boolean closed = false;

  protected final AtomicBoolean shutdown = new AtomicBoolean(false);

  protected final long walShutdownTimeout;

  /**
   * WAL Comparator; it compares the timestamp (log filenum), present in the log file name. Throws
   * an IllegalArgumentException if used to compare paths from different wals.
   */
  final Comparator<Path> LOG_NAME_COMPARATOR =
    (o1, o2) -> Long.compare(getFileNumFromFileName(o1), getFileNumFromFileName(o2));

  private static final class WALProps {

    /**
     * Map the encoded region name to the highest sequence id.
     * <p/>
     * Contains all the regions it has an entry for.
     */
    private final Map<byte[], Long> encodedName2HighestSequenceId;

    /**
     * The log file size. Notice that the size may not be accurate if we do asynchronous close in
     * subclasses.
     */
    private final long logSize;

    /**
     * If we do asynchronous close in subclasses, it is possible that when adding WALProps to the
     * rolled map, the file is not closed yet, so in cleanOldLogs we should not archive this file,
     * for safety.
     */
    private volatile boolean closed = false;

    WALProps(Map<byte[], Long> encodedName2HighestSequenceId, long logSize) {
      this.encodedName2HighestSequenceId = encodedName2HighestSequenceId;
      this.logSize = logSize;
    }
  }

  /**
   * Map of WAL log file to properties. The map is sorted by the log file creation timestamp
   * (contained in the log file name).
   */
  protected final ConcurrentNavigableMap<Path, WALProps> walFile2Props =
    new ConcurrentSkipListMap<>(LOG_NAME_COMPARATOR);

  /**
   * A cache of sync futures reused by threads.
   */
  protected final SyncFutureCache syncFutureCache;

  /**
   * The class name of the runtime implementation, used as prefix for logging/tracing.
   * <p>
   * Performance testing shows getClass().getSimpleName() might be a bottleneck so we store it here,
   * refer to HBASE-17676 for more details
   * </p>
   */
  protected final String implClassName;

  protected final AtomicBoolean rollRequested = new AtomicBoolean(false);

  protected final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  // Run in caller if we get reject execution exception, to avoid aborting region server when we get
  // reject execution exception. Usually this should not happen but let's make it more robust.
  private final ExecutorService logArchiveExecutor =
    new ThreadPoolExecutor(1, 1, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WAL-Archive-%d").build(),
      new ThreadPoolExecutor.CallerRunsPolicy());

  private final int archiveRetries;

  public long getFilenum() {
    return this.filenum.get();
  }

  /**
   * A log file has a creation timestamp (in ms) in its file name ({@link #filenum}. This helper
   * method returns the creation timestamp from a given log file. It extracts the timestamp assuming
   * the filename is created with the {@link #computeFilename(long filenum)} method.
   * @return timestamp, as in the log file name.
   */
  protected long getFileNumFromFileName(Path fileName) {
    checkNotNull(fileName, "file name can't be null");
    if (!ourFiles.accept(fileName)) {
      throw new IllegalArgumentException(
        "The log file " + fileName + " doesn't belong to this WAL. (" + toString() + ")");
    }
    final String fileNameString = fileName.toString();
    String chompedPath = fileNameString.substring(prefixPathStr.length(),
      (fileNameString.length() - walFileSuffix.length()));
    return Long.parseLong(chompedPath);
  }

  private int calculateMaxLogFiles(Configuration conf, long logRollSize) {
    checkArgument(logRollSize > 0,
      "The log roll size cannot be zero or negative when calculating max log files, "
        + "current value is " + logRollSize);
    Pair<Long, MemoryType> globalMemstoreSize = MemorySizeUtil.getGlobalMemStoreSize(conf);
    return (int) ((globalMemstoreSize.getFirst() * 2) / logRollSize);
  }

  // must be power of 2
  protected final int getPreallocatedEventCount() {
    // Preallocate objects to use on the ring buffer. The way that appends and syncs work, we will
    // be stuck and make no progress if the buffer is filled with appends only and there is no
    // sync. If no sync, then the handlers will be outstanding just waiting on sync completion
    // before they return.
    int preallocatedEventCount = this.conf.getInt(RING_BUFFER_SLOT_COUNT, 1024 * 16);
    if (KnobRuntime.check(java.util.UUID.fromString("67193a99-c09e-3363-a7f2-47d42d283b9d"))) { checkArgument((1 << 30) > (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("36209f33-fbdd-38de-83c1-983aba87e19e"))) { checkArgument((preallocatedEventCount) <= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("7e8435f9-7176-3db7-92c5-0de4db655cf5"))) { checkArgument((1 << 30) <= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else { checkArgument(preallocatedEventCount >= 0, RING_BUFFER_SLOT_COUNT + " must > 0"); }
    int floor = Integer.highestOneBit(preallocatedEventCount);
    if (((KnobRuntime.check(java.util.UUID.fromString("0b710c93-12a4-3d75-a714-d47381dea307"))) ? ((1 << 30) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("1acca245-7337-343a-8eed-527849b6bef0"))) ? ((30) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("4eaf22b8-0e60-354f-b64a-514451efc7a2"))) ? ((1 << 30) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("1551980b-5402-3db0-bf13-d898d8a3519d"))) ? ((29) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("c5366072-5ea7-38a1-9544-75111ffc4191"))) ? ((1024 * 16) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("fc32577c-2fc5-382c-bb8b-1d05abdcc24a"))) ? ((floor << 1) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("c25bd089-7260-35f3-a12d-e4b617954e53"))) ? ((29) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("e3f9f268-64de-39b4-9311-1c15cfa82817"))) ? ((floor) != (16)) : (((KnobRuntime.check(java.util.UUID.fromString("3dac05b1-6c34-3224-b765-9673a7cc01d8"))) ? ((1 << 30) != (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("7d0e6c5b-6af0-365f-a744-d3969b5e4ce1"))) ? ((16) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("3226c480-9977-3044-b0e0-352eac46b57a"))) ? ((1 << 30) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("7507512f-9328-3c74-a74a-19d0d08d7f01"))) ? ((30) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c11725c4-9995-3dbc-a15e-65c0de777648"))) ? ((1) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("08180d9f-01be-3a71-a5f1-d8ce2b4c1e77"))) ? ((1 << 29) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("82079d76-86c6-38a2-8240-bfe244fd1210"))) ? ((16) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("b5d7d05f-fe81-33ca-9a79-3780a69fc76e"))) ? ((30) != (1 << 30)) : (floor == preallocatedEventCount))))))))))))))))))))))))))))))))) {
      return floor;
    }
    // max capacity is 1 << 30
    if (floor >= 1 << 29) {
      return 1 << 30;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("bb10d19b-a27a-3c8a-81a4-cf00abde14fd"))) ? ((16) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("946120cf-5c65-3687-ae70-09837c536f13"))) ? (1 << 30) : (((KnobRuntime.check(java.util.UUID.fromString("d4f0d3d7-55b8-32cc-8dfa-1419a48859f9"))) ? ((29) << (1)) : (floor << 1))))));
  }

  protected AbstractFSWAL(final FileSystem fs, final Path rootDir, final String logDir,
    final String archiveDir, final Configuration conf, final List<WALActionsListener> listeners,
    final boolean failIfWALExists, final String prefix, final String suffix)
    throws FailedLogCloseException, IOException {
    this(fs, null, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
  }

  protected AbstractFSWAL(final FileSystem fs, final Abortable abortable, final Path rootDir,
    final String logDir, final String archiveDir, final Configuration conf,
    final List<WALActionsListener> listeners, final boolean failIfWALExists, final String prefix,
    final String suffix) throws FailedLogCloseException, IOException {
    this.fs = fs;
    this.walDir = new Path(rootDir, logDir);
    this.walArchiveDir = new Path(rootDir, archiveDir);
    this.conf = conf;
    this.abortable = abortable;

    if (!fs.exists(walDir) && !fs.mkdirs(walDir)) {
      throw new IOException("Unable to mkdir " + walDir);
    }

    if (!fs.exists(this.walArchiveDir)) {
      if (!fs.mkdirs(this.walArchiveDir)) {
        throw new IOException("Unable to mkdir " + this.walArchiveDir);
      }
    }

    // If prefix is null||empty then just name it wal
    this.walFilePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER
        + "' but instead was '" + suffix + "'");
    }
    // Now that it exists, set the storage policy for the entire directory of wal files related to
    // this FSHLog instance
    String storagePolicy =
      conf.get(HConstants.WAL_STORAGE_POLICY, HConstants.DEFAULT_WAL_STORAGE_POLICY);
    CommonFSUtils.setStoragePolicy(fs, this.walDir, storagePolicy);
    this.walFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");
    this.prefixPathStr = new Path(walDir, walFilePrefix + WAL_FILE_NAME_DELIMITER).toString();

    this.ourFiles = new PathFilter() {
      @Override
      public boolean accept(final Path fileName) {
        // The path should start with dir/<prefix> and end with our suffix
        final String fileNameString = fileName.toString();
        if (!fileNameString.startsWith(prefixPathStr)) {
          return false;
        }
        if (walFileSuffix.isEmpty()) {
          // in the case of the null suffix, we need to ensure the filename ends with a timestamp.
          return org.apache.commons.lang3.StringUtils
            .isNumeric(fileNameString.substring(prefixPathStr.length()));
        } else if (!fileNameString.endsWith(walFileSuffix)) {
          return false;
        }
        return true;
      }
    };

    if (failIfWALExists) {
      final FileStatus[] walFiles = CommonFSUtils.listStatus(fs, walDir, ourFiles);
      if (null != walFiles && 0 != walFiles.length) {
        throw new IOException("Target WAL already exists within directory " + walDir);
      }
    }

    // Register listeners. TODO: Should this exist anymore? We have CPs?
    if (listeners != null) {
      for (WALActionsListener i : listeners) {
        registerWALActionsListener(i);
      }
    }
    this.coprocessorHost = new WALCoprocessorHost(this, conf);

    // Schedule a WAL roll when the WAL is 50% of the HDFS block size. Scheduling at 50% of block
    // size should make it so WAL rolls before we get to the end-of-block (Block transitions cost
    // some latency). In hbase-1 we did this differently. We scheduled a roll when we hit 95% of
    // the block size but experience from the field has it that this was not enough time for the
    // roll to happen before end-of-block. So the new accounting makes WALs of about the same
    // size as those made in hbase-1 (to prevent surprise), we now have default block size as
    // 2 times the DFS default: i.e. 2 * DFS default block size rolling at 50% full will generally
    // make similar size logs to 1 * DFS default block size rolling at 95% full. See HBASE-19148.
    this.blocksize = WALUtil.getWALBlockSize(this.conf, this.fs, this.walDir);
    float multiplier = conf.getFloat(WAL_ROLL_MULTIPLIER, 0.5f);
    this.logrollsize = (long) (this.blocksize * multiplier);
    this.maxLogs = conf.getInt(MAX_LOGS, Math.max(32, calculateMaxLogFiles(conf, logrollsize)));

    LOG.info("WAL configuration: blocksize=" + StringUtils.byteDesc(blocksize) + ", rollsize="
      + StringUtils.byteDesc(this.logrollsize) + ", prefix=" + this.walFilePrefix + ", suffix="
      + walFileSuffix + ", logDir=" + this.walDir + ", archiveDir=" + this.walArchiveDir
      + ", maxLogs=" + this.maxLogs);
    this.slowSyncNs = TimeUnit.MILLISECONDS.toNanos(conf.getInt(SLOW_SYNC_TIME_MS,
      conf.getInt("hbase.regionserver.hlog.slowsync.ms", DEFAULT_SLOW_SYNC_TIME_MS)));
    this.rollOnSyncNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getInt(ROLL_ON_SYNC_TIME_MS, DEFAULT_ROLL_ON_SYNC_TIME_MS));
    this.slowSyncRollThreshold =
      conf.getInt(SLOW_SYNC_ROLL_THRESHOLD, DEFAULT_SLOW_SYNC_ROLL_THRESHOLD);
    this.slowSyncCheckInterval =
      conf.getInt(SLOW_SYNC_ROLL_INTERVAL_MS, DEFAULT_SLOW_SYNC_ROLL_INTERVAL_MS);
    this.walSyncTimeoutNs = TimeUnit.MILLISECONDS.toNanos(conf.getLong(WAL_SYNC_TIMEOUT_MS,
      conf.getLong("hbase.regionserver.hlog.sync.timeout", DEFAULT_WAL_SYNC_TIMEOUT_MS)));
    this.syncFutureCache = new SyncFutureCache(conf);
    this.implClassName = getClass().getSimpleName();
    this.useHsync = conf.getBoolean(HRegion.WAL_HSYNC_CONF_KEY, HRegion.DEFAULT_WAL_HSYNC);
    archiveRetries = this.conf.getInt("hbase.regionserver.walroll.archive.retries", 0);
    this.walShutdownTimeout =
      conf.getLong(WAL_SHUTDOWN_WAIT_TIMEOUT_MS, DEFAULT_WAL_SHUTDOWN_WAIT_TIMEOUT_MS);
  }

  /**
   * Used to initialize the WAL. Usually just call rollWriter to create the first log writer.
   */
  public void init() throws IOException {
    rollWriter();
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, families);
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Map<byte[], Long> familyToSeq) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, familyToSeq);
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName, long maxFlushedSeqId) {
    this.sequenceIdAccounting.completeCacheFlush(encodedRegionName, maxFlushedSeqId);
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName) {
    // Used by tests. Deprecated as too subtle for general usage.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    // This method is used by tests and for figuring if we should flush or not because our
    // sequenceids are too old. It is also used reporting the master our oldest sequenceid for use
    // figuring what edits can be skipped during log recovery. getEarliestMemStoreSequenceId
    // from this.sequenceIdAccounting is looking first in flushingOldestStoreSequenceIds, the
    // currently flushing sequence ids, and if anything found there, it is returning these. This is
    // the right thing to do for the reporting oldest sequenceids to master; we won't skip edits if
    // we crash during the flush. For figuring what to flush, we might get requeued if our sequence
    // id is old even though we are currently flushing. This may mean we do too much flushing.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName, familyName);
  }

  @Override
  public Map<byte[], List<byte[]>> rollWriter() throws FailedLogCloseException, IOException {
if(KnobRuntime.check(java.util.UUID.fromString("478ce151-a24b-35f3-af2b-c0b81d31dc56"))) {
throw new java.io.IOException("Injected exception");
}
    return rollWriter(false);
  }

  @Override
  public final void sync() throws IOException {
    sync(useHsync);
  }

  @Override
  public final void sync(long txid) throws IOException {
    sync(txid, useHsync);
  }

  @Override
  public final void sync(boolean forceSync) throws IOException {
    TraceUtil.trace(() -> doSync(forceSync), () -> createSpan("WAL.sync"));
  }

  @Override
  public final void sync(long txid, boolean forceSync) throws IOException {
    TraceUtil.trace(() -> doSync(txid, forceSync), () -> createSpan("WAL.sync"));
  }

  protected abstract void doSync(boolean forceSync) throws IOException;

  protected abstract void doSync(long txid, boolean forceSync) throws IOException;

  /**
   * This is a convenience method that computes a new filename with a given file-number.
   * @param filenum to use
   */
  protected Path computeFilename(final long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("WAL file number can't be < 0");
    }
    String child = walFilePrefix + WAL_FILE_NAME_DELIMITER + filenum + walFileSuffix;
    return new Path(walDir, child);
  }

  /**
   * This is a convenience method that computes a new filename with a given using the current WAL
   * file-number
   */
  public Path getCurrentFileName() {
    return computeFilename(this.filenum.get());
  }

  /**
   * retrieve the next path to use for writing. Increments the internal filenum.
   */
  private Path getNewPath() throws IOException {
    this.filenum.set(Math.max(getFilenum() + 1, EnvironmentEdgeManager.currentTime()));
    Path newPath = getCurrentFileName();
    return ((KnobRuntime.check(java.util.UUID.fromString("a45ea3dc-a6bc-33dd-be1c-c7b644785205"))) ? (((org.apache.hadoop.fs.Path)(getCurrentFileName()))) : (newPath));
  }

  public Path getOldPath() {
    long currentFilenum = this.filenum.get();
    Path oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename will take care of meta wal filename
      oldPath = computeFilename(currentFilenum);
    } // I presume if currentFilenum is <= 0, this is first file and null for oldPath if fine?
    return oldPath;
  }

  /**
   * Tell listeners about pre log roll.
   */
  private void tellListenersAboutPreLogRoll(final Path oldPath, final Path newPath)
    throws IOException {
    coprocessorHost.preWALRoll(oldPath, newPath);

    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogRoll(oldPath, newPath);
      }
    }
  }

  /**
   * Tell listeners about post log roll.
   */
  private void tellListenersAboutPostLogRoll(final Path oldPath, final Path newPath)
    throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogRoll(oldPath, newPath);
      }
    }

    coprocessorHost.postWALRoll(oldPath, newPath);
  }

  // public only until class moves to o.a.h.h.wal
  /** Returns the number of rolled log files */
  public int getNumRolledLogFiles() {
    return walFile2Props.size();
  }

  // public only until class moves to o.a.h.h.wal
  /** Returns the number of log files in use */
  public int getNumLogFiles() {
    // +1 for current use log
    return getNumRolledLogFiles() + 1;
  }

  /**
   * If the number of un-archived WAL files ('live' WALs) is greater than maximum allowed, check the
   * first (oldest) WAL, and return those regions which should be flushed so that it can be
   * let-go/'archived'.
   * @return stores of regions (encodedRegionNames) to flush in order to archive the oldest WAL file
   */
  Map<byte[], List<byte[]>> findRegionsToForceFlush() throws IOException {
    Map<byte[], List<byte[]>> regions = null;
    int logCount = getNumRolledLogFiles();
    if (((KnobRuntime.check(java.util.UUID.fromString("fcbdd3e8-8457-3285-85f7-76972cfaeb1b"))) ? (((logCount) >= (this.maxLogs)) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("950b25a0-ddb8-3f19-bf62-3bdd00f343ac"))) ? (((0) <= (this.maxLogs)) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5261ec26-8c4a-3737-9c94-580b3bc83732"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8e00471b-8ba6-388b-b9df-fe31795279ce"))) ? ((logCount > this.maxLogs) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("98289d2f-3784-37ca-ba79-d19d1c15d3cc"))) ? (((logCount) != (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4ac60a9a-3ee6-3b85-8dde-54452289ba8d"))) ? (((getNumRolledLogFiles()) != (0)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b42f1875-3a7d-33c5-a64b-ff69a18e1eb2"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3f2941f9-d46c-3a7d-91fc-ca02b4819608"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b9ca684c-0510-3553-9da6-3dc2cecf4396"))) ? (((logCount) == (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("229fe113-d870-32d5-8b49-d6d48b4a5338"))) ? (((getNumRolledLogFiles()) >= (0)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de926878-5199-3de2-bb4f-c2d1f2e5098d"))) ? (((logCount) >= (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("82dd0e32-5e45-3cf7-ab23-d387efd22aaf"))) ? (((getNumRolledLogFiles()) <= (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8a646b84-1f71-3ba8-9469-82e00befd0f2"))) ? (((getNumRolledLogFiles()) <= (0)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("023b82a9-8245-3da6-9d21-570e8211a8b9"))) ? (((0) >= (this.maxLogs)) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("55eafeb6-ceea-3208-af64-91e76d66854a"))) ? (((0) == (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("148b4d98-8630-3a9c-8990-9cee44a87d5f"))) ? (((getNumRolledLogFiles()) > (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8c51ada0-ae57-39f0-90de-dcd1cb91aef2"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1a59deac-a7a3-3bd8-9cc1-e551bc64075d"))) ? (((logCount) != (this.maxLogs)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("22034b36-6fc3-35e4-a394-8df1b3894555"))) ? (((0) != (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3ffab184-9643-332f-af26-4fa727aeb4e1"))) ? (((logCount) == (getNumRolledLogFiles())) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fea5126f-3f05-3949-988e-ee03a6fed920"))) ? (((0) >= (this.maxLogs)) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("77670291-c0cb-3548-b2ad-d8ed62a01937"))) ? (((logCount) < (getNumRolledLogFiles())) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5e070b0c-12b1-35fc-aa21-9fc35771f526"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("791c7cc2-e1e8-3a82-b48b-e55b0689eda2"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("df0e5965-5dfb-3e93-ae20-f0c01c292421"))) ? (((getNumRolledLogFiles()) < (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b5efe3b8-c5a6-3451-9001-f51c148aa270"))) ? (((0) == (this.maxLogs)) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("59d98236-094b-383b-a61e-c44501eb3417"))) ? (((logCount) != (0)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d0771771-ae37-38d8-b42c-7d40db443eb3"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("24a9682f-355f-31c8-aae6-add9c22d05ba"))) ? (((logCount) == (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b7c39418-3ce6-3cd5-a36d-f61cfa217b94"))) ? (((0) == (this.maxLogs)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("75e20ad6-b54c-3bc1-ae43-f9d984158b76"))) ? (((logCount) >= (getNumRolledLogFiles())) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c0acc7ec-811d-3f1b-8745-d47b54040988"))) ? (((logCount) <= (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ba7257e6-1197-3243-87f3-2c78fedff89b"))) ? (((logCount) >= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a50e499d-b5c0-3d39-a8ee-cdcbd3df2c44"))) ? (((0) != (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8682f557-1024-3a05-8f4d-1681b35b11fe"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("369dbaa1-45d7-34d3-857e-1707d168d8d1"))) ? (((logCount) >= (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("314a106f-56d2-3c28-a14d-835c205883d6"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e6e4e9e8-29b0-3a72-9e5e-4408e6a61cb4"))) ? (((getNumRolledLogFiles()) > (this.maxLogs)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cd94f34b-d92e-3676-bc77-301213b1fa42"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("91f5c179-70eb-39bf-a50b-694f479f253a"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e535473c-17ae-32c3-8bda-63e717d67394"))) ? ((getNumRolledLogFiles()) <= (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("24d7ac2b-15ef-3450-8599-d0d6569e9719"))) ? (((0) != (this.maxLogs)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e64f5e8-b54d-3d9f-91d0-5365ed15065a"))) ? (((logCount) >= (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("645c6687-3bea-3374-aee9-ce469de9504b"))) ? (((0) != (getNumRolledLogFiles())) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fa5335d3-1ee1-32fa-ae3f-bb95d0e87d33"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52b5b217-fc8a-3044-b7e1-b0a3a7db546a"))) ? (((logCount) <= (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("332d5a97-4a7c-3cbe-a655-669ce1860873"))) ? (((logCount) < (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1ce6fc9b-ea73-33b6-b0da-0340e8e8ba1f"))) ? (((logCount) == (0)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("84242085-b970-3cdc-ad66-4d7c683ed0a8"))) ? (((logCount) == (this.maxLogs)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("12ce4f5b-45ed-3e5d-864d-b987f0b4cd88"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("875b4bdf-2022-3f8f-8b3b-ca422ab20ab5"))) ? (((logCount) == (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c44bb19c-d945-3ddd-9b8b-f2f94fd87c66"))) ? (((0) <= (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5b80cafa-c868-30cd-b26e-04c8adea10d2"))) ? (((getNumRolledLogFiles()) >= (0)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7c51c848-8c01-37a2-8b0a-26e9e40a4d28"))) ? (((logCount) != (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6e586974-ce7d-38b2-a453-c982fb7956e0"))) ? (((getNumRolledLogFiles()) != (0)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3b312c05-1b6b-3e1f-8ad6-b2215ecd91c4"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e6917ba-768d-325b-85f8-d48cccd2d3c7"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de84413e-25e8-337f-942a-eb271ca63d58"))) ? (((getNumRolledLogFiles()) >= (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3d266b49-7927-3e03-a06a-e0b19cf04691"))) ? (((logCount) < (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a26c09d6-0637-31e7-953d-833c57fd5ae7"))) ? (((logCount) > (this.maxLogs)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a5cdc07-847d-3816-8527-d6d516f1f7e1"))) ? (((logCount) >= (this.maxLogs)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("80c93d30-09d4-32d2-86f6-c8ebbd694a6b"))) ? ((logCount) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4c1233da-a5a7-34c3-b927-b029a4bceabc"))) ? (((logCount) == (0)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("65e23265-78af-3782-8bc8-4107c7169a62"))) ? (((logCount) != (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("096fa89f-e730-3017-a9c0-edeb0392c003"))) ? (((0) != (getNumRolledLogFiles())) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a237f796-7f5e-3777-bd75-2603dc43fd52"))) ? (((0) > (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5b516c53-95d5-3c79-80e3-989e145ce73e"))) ? (((logCount) != (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc94a490-14dd-30a6-96ff-2c5cc145a427"))) ? (((0) > (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("06849747-df6c-361f-bf82-f884e4e3258a"))) ? (((logCount) > (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("283cccf9-1bef-394f-86c7-89e3e2205c28"))) ? (((logCount) == (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("39088e2d-8281-33b7-9141-e765f9d0a19a"))) ? ((getNumRolledLogFiles()) == (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("48be8bb8-889d-3a6c-b4db-5db47bcc348c"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1b622be9-f52d-37ba-ae2f-39906284f76a"))) ? (((0) <= (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8edf8b7-6870-393f-aeea-657814e85a6c"))) ? (((0) == (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("db0b39a3-b56a-3cb2-aae2-e3f47193ee11"))) ? ((logCount > this.maxLogs) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5313f358-caa0-328b-aa49-36a0b1661a51"))) ? (((logCount) >= (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fca3bb6e-15c0-333b-a382-160e82bfce02"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9acefb84-8224-37df-94c7-1a2361009c7c"))) ? (((0) <= (this.maxLogs)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("769abbaa-b7df-316b-86ae-4fdecbe1235e"))) ? (((getNumRolledLogFiles()) <= (0)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c3fa35d8-9b47-34ae-b015-c0a797834e93"))) ? (((0) > (getNumRolledLogFiles())) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fe227f1e-2b98-3b40-9ec9-14fd5927b7db"))) ? (((getNumRolledLogFiles()) >= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a1d1c5a6-119c-3d16-a090-fde0c5b8256f"))) ? (((logCount) == (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5c5a288a-1cb6-3502-a6b6-10fb3a6f3d8d"))) ? (((0) == (getNumRolledLogFiles())) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("389b1add-a561-3c05-bd5d-67295b40b910"))) ? (((0) >= (this.maxLogs)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("220be629-e895-3aa7-9c9b-31c87b40d5a6"))) ? (((getNumRolledLogFiles()) != (0)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f79fdadf-255d-33c6-a852-ad085a9a10c1"))) ? (((logCount) < (getNumRolledLogFiles())) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7db0116b-9e9b-35c5-9f5f-7adafab7f3f0"))) ? (((0) >= (getNumRolledLogFiles())) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("999d5edc-c3e8-3311-a8a8-c36696788412"))) ? (((0) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("22917367-a6ef-335d-84cb-50e7ddeacc71"))) ? ((0) < (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("b01b5692-e56a-312f-9c49-70afbb96e41d"))) ? (((getNumRolledLogFiles()) >= (0)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("51bf591f-22d2-31a3-913f-7dcc9acc1f3e"))) ? (((logCount) > (0)) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9f043c4a-49e5-3cf3-b59a-a3dab789a3ea"))) ? (((logCount) > (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b4506aed-4b42-3fb1-9205-1454b72c07a1"))) ? ((0) > (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("0665a2b7-cbcc-3ffc-b93c-e8e2a25582ff"))) ? (((0) == (this.maxLogs)) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d401de96-a6bf-388f-ab1e-b76072387c44"))) ? (((logCount) >= (0)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("494a275a-1e82-34f3-99f9-2177453f17d9"))) ? (((getNumRolledLogFiles()) >= (0)) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f0af00a-6735-3a6d-9b60-b955fc2a5043"))) ? (((logCount) < (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c9429d66-cd8f-3f5d-b1aa-9618f4602b0c"))) ? (((getNumRolledLogFiles()) < (this.maxLogs)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f14eb14e-0b07-3f49-8086-83ef05feb428"))) ? (((0) <= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4efe4f9f-baf6-3ade-9717-4f1005956fd4"))) ? (((0) == (getNumRolledLogFiles())) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b18e0c8e-06ad-33b7-9984-33563da94df0"))) ? (((0) == (this.maxLogs)) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc429885-63bf-39f6-ad24-5cafa72545a1"))) ? ((0) == (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("df950568-702d-3215-adc5-9d170fae66dc"))) ? (((logCount) != (this.maxLogs)) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cdceec30-aed2-3f2c-8993-1ae6c22661ce"))) ? (((0) > (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("61af1277-8144-3a6d-be1d-473f169e9a7f"))) ? (((0) >= (this.maxLogs)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("69e81306-b367-3e3b-a4fc-3ec7a823855a"))) ? (((logCount) >= (this.maxLogs)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("809645c0-3f27-3bdf-b36b-2db194e501e0"))) ? (((getNumRolledLogFiles()) == (0)) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a75bb9b-b1f9-3c04-9842-62340f3ce497"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ed44fc2a-0168-3f6f-bbd8-1de0613fa049"))) ? (((0) >= (this.maxLogs)) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8e2d2bfb-6692-327a-8e78-ec7fe9ab2632"))) ? (((logCount) >= (0)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8cc7d241-9473-3c69-81b9-f4278ce3ace7"))) ? (((getNumRolledLogFiles()) == (0)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("58bc7672-9183-3250-8c8a-78a6904ce3a8"))) ? (((logCount) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("572d5d89-cba1-3677-9cd8-6346a4623909"))) ? (((logCount) >= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("888ff705-8571-3725-8e09-188b0896586c"))) ? (((0) <= (getNumRolledLogFiles())) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a003fd3-ecc3-3224-95e1-606a0fea9ab5"))) ? (((logCount) != (0)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("83e568e0-1344-3745-9ade-67f1c6a3090a"))) ? (((getNumRolledLogFiles()) < (0)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("553d7261-3a8a-3214-af7f-10b1aeed0722"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fdbe95c5-cd7c-3e61-bd4c-622efaf346bd"))) ? (((getNumRolledLogFiles()) > (0)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("81a2a876-8cc1-39a2-8809-e0e33d7fb8d1"))) ? (((getNumRolledLogFiles()) < (0)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("4ae27129-d014-38cc-a539-04d344fddf7e"))) ? (((0) != (this.maxLogs)) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d48d7e3f-6752-32e6-96d7-1a9a2ea03a82"))) ? (((logCount) == (0)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a5a75e39-1c65-30fd-9410-2fe83e7dad59"))) ? (((0) < (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("94b5724c-77f8-375e-ad13-02b8c4925574"))) ? (((getNumRolledLogFiles()) > (0)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6d334c5d-2502-3919-9452-bb5f0e473aa3"))) ? (((0) < (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d79a740-bd42-3c1c-bdfe-e60254879502"))) ? (((getNumRolledLogFiles()) <= (0)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("05b9e22f-d741-37ee-8b2e-2e1aec76fe11"))) ? (((logCount) != (this.maxLogs)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("02b21288-c9b1-3ef4-ba17-f33cee2b03e7"))) ? (((logCount) != (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("085614c7-3a22-3797-bb1d-d11327eb3ac9"))) ? (((getNumRolledLogFiles()) >= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9f73f500-6829-38fb-8f56-0e061abc8595"))) ? (((0) >= (getNumRolledLogFiles())) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e2e09862-2057-39c8-b006-1ee10f1198b7"))) ? ((0) >= (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("b4bc2e40-8e13-3372-8977-d6054b31d7df"))) ? (((0) <= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3347a8c4-3c9c-33de-9d73-f6440ffff033"))) ? (((logCount) <= (this.maxLogs)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fe44a266-8fd7-33a7-8e81-5e84ca4c0f65"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("563c8a1e-d35e-37b5-9ca9-1bd1198ec265"))) ? (((getNumRolledLogFiles()) <= (0)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("12bb4e07-30fd-376c-8bd0-e357aefc5395"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("211c82e1-c8b7-3904-944a-8e50610c240c"))) ? (((logCount) < (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7a4bd966-edee-3024-af10-664de297d9af"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5b6f44b0-8e6d-3dcc-bc4d-bafac1a4d002"))) ? (((logCount) == (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a9ebffd-342c-3020-8c6c-f397bd9cf12f"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c026a7dd-5132-3e21-b123-6c12700df94d"))) ? (((0) != (this.maxLogs)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c1aa45c3-74e2-3094-8bc3-c2ca2d85c49a"))) ? (((getNumRolledLogFiles()) >= (getNumRolledLogFiles())) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("479b6b7e-d6c0-31a0-9b17-e602728ddb13"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7d0a64eb-6df1-310c-8585-8c1eec51c4c7"))) ? ((logCount) >= (this.maxLogs)) : (logCount > this.maxLogs && logCount > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      Map.Entry<Path, WALProps> firstWALEntry = this.walFile2Props.firstEntry();
      regions =
        this.sequenceIdAccounting.findLower(firstWALEntry.getValue().encodedName2HighestSequenceId);
    }
    if (regions != null) {
      List<String> listForPrint = new ArrayList<>();
      for (Map.Entry<byte[], List<byte[]>> r : regions.entrySet()) {
        StringBuilder families = new StringBuilder();
        for (int i = 0; i < r.getValue().size(); i++) {
          if (i > 0) {
            families.append(",");
          }
          families.append(Bytes.toString(r.getValue().get(i)));
        }
        listForPrint.add(Bytes.toStringBinary(r.getKey()) + "[" + families.toString() + "]");
      }
      LOG.info("Too many WALs; count=" + logCount + ", max=" + this.maxLogs
        + "; forcing (partial) flush of " + regions.size() + " region(s): "
        + StringUtils.join(",", listForPrint));
    }
    return regions;
  }

  /**
   * Mark this WAL file as closed and call cleanOldLogs to see if we can archive this file.
   */
  protected final void markClosedAndClean(Path path) {
    WALProps props = walFile2Props.get(path);
    // typically this should not be null, but if there is no big issue if it is already null, so
    // let's make the code more robust
    if (props != null) {
      props.closed = true;
      cleanOldLogs();
    }
  }

  /**
   * Archive old logs. A WAL is eligible for archiving if all its WALEdits have been flushed.
   * <p/>
   * Use synchronized because we may call this method in different threads, normally when replacing
   * writer, and since now close writer may be asynchronous, we will also call this method in the
   * closeExecutor, right after we actually close a WAL writer.
   */
  private synchronized void cleanOldLogs() {
    List<Pair<Path, Long>> logsToArchive = null;
    // For each log file, look at its Map of regions to the highest sequence id; if all sequence ids
    // are older than what is currently in memory, the WAL can be GC'd.
    for (Map.Entry<Path, WALProps> e : this.walFile2Props.entrySet()) {
      if (!e.getValue().closed) {
        LOG.debug("{} is not closed yet, will try archiving it next time", e.getKey());
        continue;
      }
      Path log = e.getKey();
      Map<byte[], Long> sequenceNums = e.getValue().encodedName2HighestSequenceId;
      if (this.sequenceIdAccounting.areAllLower(sequenceNums)) {
        if (logsToArchive == null) {
          logsToArchive = new ArrayList<>();
        }
        logsToArchive.add(Pair.newPair(log, e.getValue().logSize));
        if (LOG.isTraceEnabled()) {
          LOG.trace("WAL file ready for archiving " + log);
        }
      }
    }

    if (logsToArchive != null) {
      final List<Pair<Path, Long>> localLogsToArchive = logsToArchive;
      // make it async
      for (Pair<Path, Long> log : localLogsToArchive) {
        logArchiveExecutor.execute(() -> {
          archive(log);
        });
        this.walFile2Props.remove(log.getFirst());
      }
    }
  }

  protected void archive(final Pair<Path, Long> log) {
    totalLogSize.addAndGet(-log.getSecond());
    int retry = 1;
    while (true) {
      try {
        archiveLogFile(log.getFirst());
        // successful
        break;
      } catch (Throwable e) {
        if (retry > archiveRetries) {
          LOG.error("Failed log archiving for the log {},", log.getFirst(), e);
          if (this.abortable != null) {
            this.abortable.abort("Failed log archiving", e);
            break;
          }
        } else {
if(KnobRuntime.check(java.util.UUID.fromString("51b74fc2-80e7-3935-bead-81f26f9213ac"))) {
retry /= 2;
}
          if (KnobRuntime.check(java.util.UUID.fromString("a7990a17-1e2b-30eb-a338-96b0e7f20e0f"))) { LOG.info("Log archiving failed for the log {} - attempt {}", log.getFirst(), 1, e); } else { LOG.error("Log archiving failed for the log {} - attempt {}", log.getFirst(), retry, e); }
        }
        retry++;
      }
    }
  }

  /*
   * only public so WALSplitter can use.
   * @return archived location of a WAL file with the given path p
   */
  public static Path getWALArchivePath(Path archiveDir, Path p) {
    return new Path(archiveDir, p.getName());
  }

  protected void archiveLogFile(final Path p) throws IOException {
    Path newPath = getWALArchivePath(this.walArchiveDir, p);
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    LOG.info("Archiving " + p + " to " + newPath);
    if (!CommonFSUtils.renameAndSetModifyTime(this.fs, p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogArchive(p, newPath);
      }
    }
  }

  protected final void logRollAndSetupWalProps(Path oldPath, Path newPath, long oldFileLen) {
    int oldNumEntries = this.numEntries.getAndSet(0);
    String newPathString = newPath != null ? CommonFSUtils.getPath(newPath) : null;
    if (oldPath != null) {
      this.walFile2Props.put(oldPath,
        new WALProps(this.sequenceIdAccounting.resetHighest(), oldFileLen));
      this.totalLogSize.addAndGet(oldFileLen);
      LOG.info("Rolled WAL {} with entries={}, filesize={}; new WAL {}",
        CommonFSUtils.getPath(oldPath), oldNumEntries, StringUtils.byteDesc(oldFileLen),
        newPathString);
    } else {
      LOG.info("New WAL {}", newPathString);
    }
  }

  private Span createSpan(String name) {
    return TraceUtil.createSpan(name).setAttribute(WAL_IMPL, implClassName);
  }

  /**
   * Cleans up current writer closing it and then puts in place the passed in {@code nextWriter}.
   * <p/>
   * <ul>
   * <li>In the case of creating a new WAL, oldPath will be null.</li>
   * <li>In the case of rolling over from one file to the next, none of the parameters will be null.
   * </li>
   * <li>In the case of closing out this FSHLog with no further use newPath and nextWriter will be
   * null.</li>
   * </ul>
   * @param oldPath    may be null
   * @param newPath    may be null
   * @param nextWriter may be null
   * @return the passed in <code>newPath</code>
   * @throws IOException if there is a problem flushing or closing the underlying FS
   */
  Path replaceWriter(Path oldPath, Path newPath, W nextWriter) throws IOException {
    return TraceUtil.trace(() -> {
      doReplaceWriter(oldPath, newPath, nextWriter);
      return newPath;
    }, () -> createSpan("WAL.replaceWriter"));
  }

  protected final void blockOnSync(SyncFuture syncFuture) throws IOException {
    // Now we have published the ringbuffer, halt the current thread until we get an answer back.
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("8a006c2e-4dc9-3a27-9bb2-aac77aaab16e"))) ? ((syncFuture) == (null)) : (syncFuture != null))) {
        if (closed) {
          throw new IOException("WAL has been closed");
        } else {
          syncFuture.get(walSyncTimeoutNs);
        }
      }
    } catch (TimeoutIOException tioe) {
      throw new WALSyncTimeoutIOException(tioe);
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted", ie);
      throw convertInterruptedExceptionToIOException(ie);
    } catch (ExecutionException e) {
      throw ensureIOException(e.getCause());
    }
  }

  private static IOException ensureIOException(final Throwable t) {
    return (t instanceof IOException) ? (IOException) t : new IOException(t);
  }

  private IOException convertInterruptedExceptionToIOException(final InterruptedException ie) {
    Thread.currentThread().interrupt();
    IOException ioe = new InterruptedIOException();
    ioe.initCause(ie);
    return ioe;
  }

  private Map<byte[], List<byte[]>> rollWriterInternal(boolean force) throws IOException {
    rollWriterLock.lock();
    try {
      // Return if nothing to flush.
      if (!force && this.writer != null && this.numEntries.get() <= 0) {
        return null;
      }
      Map<byte[], List<byte[]>> regionsToFlush = null;
      if (this.closed) {
        LOG.debug("WAL closed. Skipping rolling of writer");
        return regionsToFlush;
      }
      try {
        Path oldPath = getOldPath();
        Path newPath = getNewPath();
        // Any exception from here on is catastrophic, non-recoverable, so we currently abort.
        W nextWriter = this.createWriterInstance(newPath);
        tellListenersAboutPreLogRoll(oldPath, newPath);
        // NewPath could be equal to oldPath if replaceWriter fails.
        newPath = replaceWriter(oldPath, newPath, nextWriter);
        tellListenersAboutPostLogRoll(oldPath, newPath);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Create new " + implClassName + " writer with pipeline: "
            + FanOutOneBlockAsyncDFSOutputHelper
              .getDataNodeInfo(Arrays.stream(getPipeline()).collect(Collectors.toList())));
        }
        // We got a new writer, so reset the slow sync count
        lastTimeCheckSlowSync = EnvironmentEdgeManager.currentTime();
        slowSyncCount.set(0);
        // Can we delete any of the old log files?
        if (getNumRolledLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = findRegionsToForceFlush();
        }
      } catch (CommonFSUtils.StreamLacksCapabilityException exception) {
        // If the underlying FileSystem can't do what we ask, treat as IO failure, so
        // we'll abort.
        throw new IOException(
          "Underlying FileSystem can't meet stream requirements. See RS log " + "for details.",
          exception);
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  @Override
  public Map<byte[], List<byte[]>> rollWriter(boolean force) throws IOException {
    return TraceUtil.trace(() -> rollWriterInternal(force), () -> createSpan("WAL.rollWriter"));
  }

  // public only until class moves to o.a.h.h.wal
  /** Returns the size of log files in use */
  public long getLogFileSize() {
    return this.totalLogSize.get();
  }

  // public only until class moves to o.a.h.h.wal
  public void requestLogRoll() {
if(KnobRuntime.check(java.util.UUID.fromString("85a06bb8-2b7d-33a2-b4de-d4d8069c34a6"))) {
return;
}
    requestLogRoll(ERROR);
  }

  /**
   * Get the backing files associated with this WAL.
   * @return may be null if there are no files.
   */
  FileStatus[] getFiles() throws IOException {
    return CommonFSUtils.listStatus(fs, walDir, ourFiles);
  }

  @Override
  public void shutdown() throws IOException {
    if (!shutdown.compareAndSet(false, true)) {
      return;
    }
    closed = true;
    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logCloseRequested();
      }
    }

    ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WAL-Shutdown-%d").build());

    Future<Void> future = shutdownExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        if (rollWriterLock.tryLock(walShutdownTimeout, TimeUnit.SECONDS)) {
          try {
            doShutdown();
            if (syncFutureCache != null) {
              syncFutureCache.clear();
            }
          } finally {
            rollWriterLock.unlock();
          }
        } else {
          throw new IOException("Waiting for rollWriterLock timeout");
        }
        return null;
      }
    });
    shutdownExecutor.shutdown();

    try {
      future.get(walShutdownTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for shutdown WAL");
    } catch (TimeoutException e) {
      throw new TimeoutIOException("We have waited " + walShutdownTimeout + "ms, but"
        + " the shutdown of WAL doesn't complete! Please check the status of underlying "
        + "filesystem or increase the wait time by the config \"" + WAL_SHUTDOWN_WAIT_TIMEOUT_MS
        + "\"", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    } finally {
      // in shutdown, we may call cleanOldLogs so shutdown this executor in the end.
      // In sync replication implementation, we may shut down a WAL without shutting down the whole
      // region server, if we shut down this executor earlier we may get reject execution exception
      logArchiveExecutor.shutdown();
    }
    // we also need to wait logArchive to finish if we want to a graceful shutdown as we may still
    // have some pending archiving tasks not finished yet, and in close we may archive all the
    // remaining WAL files, there could be race if we do not wait for the background archive task
    // finish
    try {
      if (!logArchiveExecutor.awaitTermination(walShutdownTimeout, TimeUnit.MILLISECONDS)) {
        throw new TimeoutIOException("We have waited " + walShutdownTimeout + "ms, but"
          + " the shutdown of WAL doesn't complete! Please check the status of underlying "
          + "filesystem or increase the wait time by the config \"" + WAL_SHUTDOWN_WAIT_TIMEOUT_MS
          + "\"");
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted when waiting for shutdown WAL");
    }
  }

  @Override
  public void close() throws IOException {
    shutdown();
    final FileStatus[] files = getFiles();
    if (null != files && 0 != files.length) {
      for (FileStatus file : files) {
        Path p = getWALArchivePath(this.walArchiveDir, file.getPath());
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            if (KnobRuntime.check(java.util.UUID.fromString("a0be0099-638c-3508-afc6-29b6d99e6982"))) { i.preLogRoll(getOldPath(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("6d3be2f5-b242-3422-8663-4a109e5baeec"))) { i.preLogRoll(getCurrentFileName(), p); } else { i.preLogArchive(file.getPath(), p); }
          }
        }

        if (!CommonFSUtils.renameAndSetModifyTime(fs, file.getPath(), p)) {
          throw new IOException("Unable to rename " + file.getPath() + " to " + p);
        }
        // Tell our listeners that a log was archived.
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogArchive(file.getPath(), p);
          }
        }
      }
      LOG.debug(
        "Moved " + files.length + " WAL file(s) to " + CommonFSUtils.getPath(this.walArchiveDir));
    }
    LOG.info("Closed WAL: " + toString());
  }

  /** Returns number of WALs currently in the process of closing. */
  public int getInflightWALCloseCount() {
    return inflightWALClosures.size();
  }

  /**
   * updates the sequence number of a specific store. depending on the flag: replaces current seq
   * number if the given seq id is bigger, or even if it is lower than existing one
   */
  @Override
  public void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceid,
    boolean onlyIfGreater) {
    sequenceIdAccounting.updateStore(encodedRegionName, familyName, sequenceid, onlyIfGreater);
  }

  protected final SyncFuture getSyncFuture(long sequence, boolean forceSync) {
    return syncFutureCache.getIfPresentOrNew().reset(sequence, forceSync);
  }

  protected boolean isLogRollRequested() {
    return rollRequested.get();
  }

  protected final void requestLogRoll(final WALActionsListener.RollRequestReason reason) {
    // If we have already requested a roll, don't do it again
    // And only set rollRequested to true when there is a registered listener
    if (!this.listeners.isEmpty() && rollRequested.compareAndSet(false, true)) {
      for (WALActionsListener i : this.listeners) {
        i.logRollRequested(reason);
      }
    }
  }

  long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedTxid.get();
    long highestUnsynced = this.highestUnsyncedTxid;
    return highestSynced >= highestUnsynced ? 0 : highestUnsynced - highestSynced;
  }

  boolean isUnflushedEntries() {
    return ((KnobRuntime.check(java.util.UUID.fromString("66ff4c06-6c8c-39ad-85d4-8d817b00e54e"))) ? ((EnvironmentEdgeManager.currentTime()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a8aecf27-eaea-3b2e-8549-dd9b5f6cc738"))) ? ((getUnflushedEntriesCount()) == (0)) : (getUnflushedEntriesCount() > 0))));
  }

  /**
   * Exposed for testing only. Use to tricks like halt the ring buffer appending.
   */
  protected void atHeadOfRingBufferEventHandlerAppend() {
    // Noop
  }

  protected final boolean appendEntry(W writer, FSWALEntry entry) throws IOException {
    // TODO: WORK ON MAKING THIS APPEND FASTER. DOING WAY TOO MUCH WORK WITH CPs, PBing, etc.
    atHeadOfRingBufferEventHandlerAppend();
    long start = EnvironmentEdgeManager.currentTime();
    byte[] encodedRegionName = entry.getKey().getEncodedRegionName();
    long regionSequenceId = entry.getKey().getSequenceId();

    // Edits are empty, there is nothing to append. Maybe empty when we are looking for a
    // region sequence id only, a region edit/sequence id that is not associated with an actual
    // edit. It has to go through all the rigmarole to be sure we have the right ordering.
    if (entry.getEdit().isEmpty()) {
      return false;
    }

    // Coprocessor hook.
    coprocessorHost.preWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
    if (!listeners.isEmpty()) {
      for (WALActionsListener i : listeners) {
        i.visitLogEntryBeforeWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
      }
    }
    doAppend(writer, entry);
    assert highestUnsyncedTxid < entry.getTxid();
    highestUnsyncedTxid = entry.getTxid();
    if (entry.isCloseRegion()) {
      // let's clean all the records of this region
      sequenceIdAccounting.onRegionClose(encodedRegionName);
    } else {
      sequenceIdAccounting.update(encodedRegionName, entry.getFamilyNames(), regionSequenceId,
        entry.isInMemStore());
    }
    coprocessorHost.postWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
    // Update metrics.
    postAppend(entry, EnvironmentEdgeManager.currentTime() - start);
    numEntries.incrementAndGet();
    return true;
  }

  private long postAppend(final Entry e, final long elapsedTime) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("dd5dcddd-99e4-3f33-b8b5-580aa780f6c9"))) {
throw new java.io.IOException("Injected exception");
}
    long len = 0;
    if (!listeners.isEmpty()) {
      for (Cell cell : e.getEdit().getCells()) {
        len += PrivateCellUtil.estimatedSerializedSizeOf(cell);
      }
      for (WALActionsListener listener : listeners) {
        listener.postAppend(len, elapsedTime, e.getKey(), e.getEdit());
      }
    }
    return len;
  }

  protected final void postSync(final long timeInNanos, final int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg = new StringBuilder().append("Slow sync cost: ")
        .append(TimeUnit.NANOSECONDS.toMillis(timeInNanos)).append(" ms, current pipeline: ")
        .append(Arrays.toString(getPipeline())).toString();
      LOG.info(msg);
      // A single sync took too long.
      // Elsewhere in checkSlowSync, called from checkLogRoll, we will look at cumulative
      // effects. Here we have a single data point that indicates we should take immediate
      // action, so do so.
      if (timeInNanos > this.rollOnSyncNs) {
        LOG.warn("Requesting log roll because we exceeded slow sync threshold; time="
          + TimeUnit.NANOSECONDS.toMillis(timeInNanos) + " ms, threshold="
          + TimeUnit.NANOSECONDS.toMillis(rollOnSyncNs) + " ms, current pipeline: "
          + Arrays.toString(getPipeline()));
        requestLogRoll(SLOW_SYNC);
      }
      slowSyncCount.incrementAndGet(); // it's fine to unconditionally increment this
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.postSync(timeInNanos, handlerSyncs);
      }
    }
  }

  protected final long stampSequenceIdAndPublishToRingBuffer(RegionInfo hri, WALKeyImpl key,
    WALEdit edits, boolean inMemstore, RingBuffer<RingBufferTruck> ringBuffer) throws IOException {
    if (this.closed) {
      throw new IOException(
        "Cannot append; log is closed, regionName = " + hri.getRegionNameAsString());
    }
    MutableLong txidHolder = new MutableLong();
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin(() -> {
      txidHolder.setValue(ringBuffer.next());
    });
    long txid = txidHolder.longValue();
    ServerCall<?> rpcCall = RpcServer.getCurrentCall().filter(c -> c instanceof ServerCall)
      .filter(c -> c.getCellScanner() != null).map(c -> (ServerCall) c).orElse(null);
    try {
      FSWALEntry entry = new FSWALEntry(txid, key, edits, hri, inMemstore, rpcCall);
      entry.stampRegionSequenceId(we);
      ringBuffer.get(txid).load(entry);
    } finally {
      ringBuffer.publish(txid);
    }
    return txid;
  }

  @Override
  public String toString() {
    return implClassName + " " + walFilePrefix + ":" + walFileSuffix + "(num " + filenum + ")";
  }

  /**
   * if the given {@code path} is being written currently, then return its length.
   * <p>
   * This is used by replication to prevent replicating unacked log entries. See
   * https://issues.apache.org/jira/browse/HBASE-14004 for more details.
   */
  @Override
  public OptionalLong getLogFileSizeIfBeingWritten(Path path) {
    rollWriterLock.lock();
    try {
      Path currentPath = getOldPath();
      if (path.equals(currentPath)) {
        // Currently active path.
        W writer = this.writer;
        return writer != null ? OptionalLong.of(writer.getSyncedLength()) : OptionalLong.empty();
      } else {
        W temp = inflightWALClosures.get(path.getName());
        if (temp != null) {
          // In the process of being closed, trailer bytes may or may not be flushed.
          // Ensuring that we read all the bytes in a file is critical for correctness of tailing
          // use cases like replication, see HBASE-25924/HBASE-25932.
          return OptionalLong.of(temp.getSyncedLength());
        }
        // Log rolled successfully.
        return OptionalLong.empty();
      }
    } finally {
      rollWriterLock.unlock();
    }
  }

  @Override
  public long appendData(RegionInfo info, WALKeyImpl key, WALEdit edits) throws IOException {
    return TraceUtil.trace(() -> append(info, key, edits, true),
      () -> createSpan("WAL.appendData"));
  }

  @Override
  public long appendMarker(RegionInfo info, WALKeyImpl key, WALEdit edits) throws IOException {
    return TraceUtil.trace(() -> append(info, key, edits, false),
      () -> createSpan("WAL.appendMarker"));
  }

  /**
   * Append a set of edits to the WAL.
   * <p/>
   * The WAL is not flushed/sync'd after this transaction completes BUT on return this edit must
   * have its region edit/sequence id assigned else it messes up our unification of mvcc and
   * sequenceid. On return <code>key</code> will have the region edit/sequence id filled in.
   * <p/>
   * NOTE: This appends, at a time that is usually after this call returns, starts a mvcc
   * transaction by calling 'begin' wherein which we assign this update a sequenceid. At assignment
   * time, we stamp all the passed in Cells inside WALEdit with their sequenceId. You must
   * 'complete' the transaction this mvcc transaction by calling
   * MultiVersionConcurrencyControl#complete(...) or a variant otherwise mvcc will get stuck. Do it
   * in the finally of a try/finally block within which this appends lives and any subsequent
   * operations like sync or update of memstore, etc. Get the WriteEntry to pass mvcc out of the
   * passed in WALKey <code>walKey</code> parameter. Be warned that the WriteEntry is not
   * immediately available on return from this method. It WILL be available subsequent to a sync of
   * this append; otherwise, you will just have to wait on the WriteEntry to get filled in.
   * @param info       the regioninfo associated with append
   * @param key        Modified by this call; we add to it this edits region edit/sequence id.
   * @param edits      Edits to append. MAY CONTAIN NO EDITS for case where we want to get an edit
   *                   sequence id that is after all currently appended edits.
   * @param inMemstore Always true except for case where we are writing a region event meta marker
   *                   edit, for example, a compaction completion record into the WAL or noting a
   *                   Region Open event. In these cases the entry is just so we can finish an
   *                   unfinished compaction after a crash when the new Server reads the WAL on
   *                   recovery, etc. These transition event 'Markers' do not go via the memstore.
   *                   When memstore is false, we presume a Marker event edit.
   * @return Returns a 'transaction id' and <code>key</code> will have the region edit/sequence id
   *         in it.
   */
  protected abstract long append(RegionInfo info, WALKeyImpl key, WALEdit edits, boolean inMemstore)
    throws IOException;

  protected abstract void doAppend(W writer, FSWALEntry entry) throws IOException;

  protected abstract W createWriterInstance(Path path)
    throws IOException, CommonFSUtils.StreamLacksCapabilityException;

  /**
   * Notice that you need to clear the {@link #rollRequested} flag in this method, as the new writer
   * will begin to work before returning from this method. If we clear the flag after returning from
   * this call, we may miss a roll request. The implementation class should choose a proper place to
   * clear the {@link #rollRequested} flag, so we do not miss a roll request, typically before you
   * start writing to the new writer.
   */
  protected abstract void doReplaceWriter(Path oldPath, Path newPath, W nextWriter)
    throws IOException;

  protected abstract void doShutdown() throws IOException;

  protected abstract boolean doCheckLogLowReplication();

  /** Returns true if we exceeded the slow sync roll threshold over the last check interval */
  protected boolean doCheckSlowSync() {
    boolean result = false;
    long now = EnvironmentEdgeManager.currentTime();
    long elapsedTime = now - lastTimeCheckSlowSync;
    if (((KnobRuntime.check(java.util.UUID.fromString("72fedea8-85a8-3342-8238-b4586fa7bd59"))) ? ((elapsedTime) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8385d129-a24c-3ca6-ab83-2fecfc0087d5"))) ? ((elapsedTime) == ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("a568b4bb-a90e-399e-8891-ea3e44ac5b9d"))) ? ((elapsedTime) == (2)) : (elapsedTime >= slowSyncCheckInterval))))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("f45efb0d-df88-37f9-9258-7fa79968bcd7"))) ? ((getNumRolledLogFiles()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d7796772-5db5-3a50-90fc-2c11cda17380"))) ? ((slowSyncCount.get()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("775b1cc5-da00-3312-9b36-92f05d3b612d"))) ? ((getNumRolledLogFiles()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a891c717-79b4-31b2-be36-78f2b4146e60"))) ? ((getNumRolledLogFiles()) != (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("2c54ca35-b8dd-3283-be6a-12f04dd83b04"))) ? ((slowSyncCount.get()) > (slowSyncRollThreshold)) : (slowSyncCount.get() >= slowSyncRollThreshold))))))))))) {
        if (elapsedTime >= (2 * slowSyncCheckInterval)) {
          // If two or more slowSyncCheckInterval have elapsed this is a corner case
          // where a train of slow syncs almost triggered us but then there was a long
          // interval from then until the one more that pushed us over. If so, we
          // should do nothing and let the count reset.
          if (LOG.isDebugEnabled()) {
            LOG.debug("checkSlowSync triggered but we decided to ignore it; " + "count="
              + slowSyncCount.get() + ", threshold=" + slowSyncRollThreshold + ", elapsedTime="
              + elapsedTime + " ms, slowSyncCheckInterval=" + slowSyncCheckInterval + " ms");
          }
          // Fall through to count reset below
        } else {
          LOG.warn("Requesting log roll because we exceeded slow sync threshold; count="
            + slowSyncCount.get() + ", threshold=" + slowSyncRollThreshold + ", current pipeline: "
            + Arrays.toString(getPipeline()));
          result = true;
        }
      }
      lastTimeCheckSlowSync = now;
      slowSyncCount.set(0);
    }
    return result;
  }

  public void checkLogLowReplication(long checkInterval) {
    long now = EnvironmentEdgeManager.currentTime();
    if (now - lastTimeCheckLowReplication < checkInterval) {
      return;
    }
    // Will return immediately if we are in the middle of a WAL log roll currently.
    if (!rollWriterLock.tryLock()) {
      return;
    }
    try {
      lastTimeCheckLowReplication = now;
      if (doCheckLogLowReplication()) {
        requestLogRoll(LOW_REPLICATION);
      }
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * This method gets the pipeline for the current WAL.
   */
  abstract DatanodeInfo[] getPipeline();

  /**
   * This method gets the datanode replication count for the current WAL.
   */
  abstract int getLogReplication();

  private static void split(final Configuration conf, final Path p) throws IOException {
    FileSystem fs = CommonFSUtils.getWALFileSystem(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.getFileStatus(p).isDirectory()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = CommonFSUtils.getWALRootDir(conf);
    Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (
      conf.getBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR,
        AbstractFSWALProvider.DEFAULT_SEPARATE_OLDLOGDIR)
    ) {
      archiveDir = new Path(archiveDir, p.getName());
    }
    WALSplitter.split(baseDir, p, archiveDir, fs, conf, WALFactory.getInstance(conf));
  }

  W getWriter() {
    return this.writer;
  }

  private static void usage() {
    System.err.println("Usage: AbstractFSWAL <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: "
      + "AbstractFSWAL --dump hdfs://example.com:9000/hbase/WALs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println(
      "         For example: AbstractFSWAL --split hdfs://example.com:9000/hbase/WALs/DIR");
  }

  /**
   * Pass one or more log file names, and it will either dump out a text version on
   * <code>stdout</code> or split the specified log files.
   */
  public static void main(String[] args) throws IOException {
    if (((KnobRuntime.check(java.util.UUID.fromString("c1e21de9-b135-3c97-88e3-2705fa28917a"))) ? ((-1) >= (2)) : (args.length < 2))) {
      usage();
      System.exit(-1);
    }
    // either dump using the WALPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      WALPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--perf") == 0) {
      LOG.error(HBaseMarkers.FATAL, "Please use the WALPerformanceEvaluation tool instead. i.e.:");
      LOG.error(HBaseMarkers.FATAL,
        "\thbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation --iterations " + args[1]);
      System.exit(-1);
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          Path logPath = new Path(args[i]);
          CommonFSUtils.setFsDefault(conf, logPath);
          split(conf, logPath);
        } catch (IOException t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }
}
