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
if(KnobRuntime.check(java.util.UUID.fromString("a1578986-def0-3dea-b301-88e42da83cd9"))) {
return 0;
}
    // Preallocate objects to use on the ring buffer. The way that appends and syncs work, we will
    // be stuck and make no progress if the buffer is filled with appends only and there is no
    // sync. If no sync, then the handlers will be outstanding just waiting on sync completion
    // before they return.
    int preallocatedEventCount = this.conf.getInt(RING_BUFFER_SLOT_COUNT, 1024 * 16);
    if (KnobRuntime.check(java.util.UUID.fromString("baf1b5f2-2539-3ab4-a3f5-5ff785d2d2d4"))) { checkArgument((1 << 29) > (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("56595645-01cf-3c7c-97f9-f95c4529c8bd"))) { checkArgument((1 << 30) >= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("d1f09320-e8f3-326e-a18d-2f93f5311619"))) { checkArgument((1024 * 16) != (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("21892132-2c37-3b81-b69e-cf2fa0755e91"))) { checkArgument((1 << 29) <= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("4c8f0986-519c-3059-948b-43d97e548e28"))) { checkArgument((1024 * 16) < (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("88115666-8892-38dd-81df-771279cedcb3"))) { checkArgument((preallocatedEventCount) > (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("cb95e81a-a47a-3d9f-8365-affe938ae591"))) { checkArgument((1 << 29) >= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("26a64d99-c28b-3de3-9522-e2b073a517b4"))) { checkArgument((1024 * 16) > (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("245e4e29-9ff4-36bf-9444-bc5dc0117ce4"))) { checkArgument(preallocatedEventCount >= 0, (RING_BUFFER_SLOT_COUNT + " must > 0") + (" must > 0")); } else if (KnobRuntime.check(java.util.UUID.fromString("1563a715-5aa0-368b-87bc-08150b0781d1"))) { checkArgument((1 << 29) != (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("3267d3c5-ca86-31fe-8c74-1fda74511e51"))) { checkArgument((preallocatedEventCount) >= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("212c505c-07eb-3212-9089-d15a449be1d3"))) { checkArgument(preallocatedEventCount >= 0, (RING_BUFFER_SLOT_COUNT) + (" must > 0")); } else if (KnobRuntime.check(java.util.UUID.fromString("d3793439-36f7-3695-a996-f23e7994d23b"))) { checkArgument((1 << 30) != (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("7e8435f9-7176-3db7-92c5-0de4db655cf5"))) { checkArgument((1 << 30) <= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("701091e8-c3df-36c1-8776-9bf47d54c464"))) { checkArgument((1 << 30) < (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("d8ce6977-e49e-3754-ae2f-3032a580740b"))) { checkArgument((preallocatedEventCount) == (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("6de75443-0594-390f-b3c0-486a15a0c17d"))) { checkArgument((1024 * 16) >= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("a7c9119a-63dd-3b39-aad7-d1f2b71874c2"))) { checkArgument((1024 * 16) <= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("f27974f3-274d-35cd-8150-4d77ae25b690"))) { checkArgument((preallocatedEventCount) < (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("ec379d44-3d3e-3f94-80fd-dfe6419c785a"))) { checkArgument((1024 * 16) == (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("427c1672-9669-3e17-9d23-08b76b07debc"))) { checkArgument((1 << 29) == (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("addefda1-982b-3968-b62c-9b52f153928f"))) { checkArgument((preallocatedEventCount) != (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("0461748b-0a11-3f76-80ac-c966815ec935"))) { checkArgument((1 << 30) == (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("67193a99-c09e-3363-a7f2-47d42d283b9d"))) { checkArgument((1 << 30) > (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("62143c6f-b9f8-39db-9dbc-496a08808ff6"))) { checkArgument((1 << 29) < (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else if (KnobRuntime.check(java.util.UUID.fromString("36209f33-fbdd-38de-83c1-983aba87e19e"))) { checkArgument((preallocatedEventCount) <= (0), RING_BUFFER_SLOT_COUNT + " must > 0"); } else { checkArgument(preallocatedEventCount >= 0, RING_BUFFER_SLOT_COUNT + " must > 0"); }
    int floor = Integer.highestOneBit(preallocatedEventCount);
    if (((KnobRuntime.check(java.util.UUID.fromString("145d34b7-8737-3a05-938a-aa3b5b07d695"))) ? ((0) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("f55e1758-6b4f-35f1-9f00-db203d1fe99b"))) ? ((1) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("f5476d19-3e20-3fb1-a061-171bf72d42b9"))) ? ((1) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("88ec44a7-0366-39b4-9033-870f4c676aea"))) ? ((1 << 29) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("81b2cdf0-d4a7-3b32-826f-f43ad44fa9eb"))) ? ((1 << 30) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("2f7e6062-2f58-3b1b-8edd-3f074412e560"))) ? ((floor << 1) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("36f5c956-6b77-3b5c-9daa-5d3c61e78701"))) ? ((1 << 29) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("ea39114a-a3ac-39a1-ad2f-4c9672a2008e"))) ? ((1024) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("864d2a28-5a7a-348c-b476-5619c461a94d"))) ? ((1 << 29) == (16)) : (((KnobRuntime.check(java.util.UUID.fromString("687c490e-5825-31fb-af02-39bcf3c22f82"))) ? ((1 << 30) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("34f6869a-8f3f-3d53-86d2-8ebddb026f7e"))) ? ((1 << 30) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("51a0936b-49d7-3221-8e10-1d1e25917a82"))) ? ((1 << 29) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("e8b92563-ff6d-37dd-9cde-086fb79b5f15"))) ? ((floor << 1) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("a2f32bef-5315-3153-9005-5b33e86d3d25"))) ? ((1 << 29) != (16)) : (((KnobRuntime.check(java.util.UUID.fromString("70b55df0-1f87-3a47-afe1-2e62e14273ed"))) ? ((29) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("c2a1d957-d211-3212-afd2-8cd9e0508d4b"))) ? ((1) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("529efa1d-1a70-34e5-ad34-dcf5155120b9"))) ? ((floor) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("df2c6535-40f3-3248-91b9-8de176804f46"))) ? ((floor) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("dc46a274-e90e-3ac5-addd-8a24c96dee17"))) ? ((16) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("f1a4498c-0aa1-340f-b9b8-c4dcdb826eef"))) ? ((floor << 1) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("04cb0c39-eaed-347c-b96f-3dc1da837654"))) ? ((1 << 30) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("efdcdbeb-a6af-35c7-b525-70addf17aac9"))) ? ((floor) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e7dfdc8d-189c-3133-95da-1be7b994c314"))) ? ((1 << 30) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("b51674af-9f50-3b85-aedf-d722da93387e"))) ? ((floor) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("247a95d8-c970-341f-b841-59dd11a2b707"))) ? ((1 << 29) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("036f6d08-1a0f-3ae4-be54-f10cf96fac50"))) ? ((floor) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("3bdb947f-9fd9-3425-b29e-df70015cc88d"))) ? ((1 << 29) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8b3036ae-5edd-3c60-828a-370485bef31e"))) ? ((0) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("abc7fb0d-4cab-31c8-a818-fcd92fadbd43"))) ? ((floor) != (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("b5d7d05f-fe81-33ca-9a79-3780a69fc76e"))) ? ((30) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("bf3209d5-b4cf-3847-ae9f-ca86004acb4b"))) ? ((1 << 30) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("234bb969-1e8e-3e37-816c-7e0ff5f82928"))) ? ((1024 * 16) == (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("b80a6c04-1490-306c-aeac-af6079abc93f"))) ? ((29) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("6df51c36-c8d6-39f8-835d-7115ddaa623a"))) ? ((floor << 1) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("3663caa6-ad11-3520-858e-eb7dd59da50a"))) ? ((16) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("3dac05b1-6c34-3224-b765-9673a7cc01d8"))) ? ((1 << 30) != (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("79e91dda-d775-332d-97ab-a1c5db241828"))) ? ((floor << 1) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e231ef9f-7e89-382c-b1f2-b4d8fe69916c"))) ? ((floor) == (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("ef7730a5-738a-381e-b342-3398ded45acc"))) ? ((29) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("3226c480-9977-3044-b0e0-352eac46b57a"))) ? ((1 << 30) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e0782402-7a07-3968-b7be-e05817f20651"))) ? ((floor) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("7fae0a10-9454-36d3-be55-95ebbc33c05f"))) ? ((1024 * 16) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("ff14c69b-2869-3e3d-833e-03e4a8ad0f74"))) ? ((floor) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("3ab07849-188c-3ccc-9c21-12c7d43395de"))) ? ((floor << 1) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("3ee9c66e-d274-377d-8496-40665da12f69"))) ? ((1 << 29) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("0ab55f4e-915e-3ffd-a850-c7a5be1d0a27"))) ? ((floor) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("d3e36927-4e15-397d-a502-5a1d6255f759"))) ? ((1024 * 16) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("e3f9f268-64de-39b4-9311-1c15cfa82817"))) ? ((floor) != (16)) : (((KnobRuntime.check(java.util.UUID.fromString("0b710c93-12a4-3d75-a714-d47381dea307"))) ? ((1 << 30) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("d1c2ec10-50d0-3340-8c8d-67e6a6dfda9f"))) ? ((1024 * 16) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("13eccda1-b5db-3178-8363-e48ddb47486a"))) ? ((1024 * 16) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("08180d9f-01be-3a71-a5f1-d8ce2b4c1e77"))) ? ((1 << 29) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("503c4e30-6c5d-353c-957f-37c886d83813"))) ? ((floor << 1) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("fe97cd46-dd0b-325d-a520-f10db653ab49"))) ? ((0) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("703073ea-f178-34e3-b650-1aa0d99fad9b"))) ? ((floor << 1) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("7d0e6c5b-6af0-365f-a744-d3969b5e4ce1"))) ? ((16) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("c4288de5-c1d8-3541-95d7-567baae4b651"))) ? ((1 << 30) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("0d503a55-07d0-3ee2-ac39-8dc87bb107fb"))) ? ((1024 * 16) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("da7565a9-163e-328f-882d-0b91f2d67867"))) ? ((16) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("1acca245-7337-343a-8eed-527849b6bef0"))) ? ((30) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("e5524e44-b6bf-3243-a431-c00b1bbedd7c"))) ? ((floor << 1) != (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("9cc0d415-b844-3981-8d7d-e93763e23ac4"))) ? ((0) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("7cee22ee-511c-31f1-a48e-84317bda8a5a"))) ? ((floor << 1) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c5366072-5ea7-38a1-9544-75111ffc4191"))) ? ((1024 * 16) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("c6f2bafc-4d43-3ca3-b4da-aa001959e365"))) ? ((1024) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("2e70b020-2ce1-3d14-83a8-250dbbeb9a8a"))) ? ((1 << 29) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f8269195-1caa-3ccf-9534-2dc96e288e8a"))) ? ((floor) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("0c82a346-ef2d-3033-a027-db2fd9ade632"))) ? ((1024) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("0150e224-0e2a-324b-817e-b61cb9ea7ac8"))) ? ((1 << 29) == (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("63fd1f29-f22d-33f7-9dcc-122773826741"))) ? ((1 << 29) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("ff1919ca-83be-340d-908f-81a1543863a4"))) ? ((0) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("913d54d0-fbe6-33b8-b349-b20728b1b57a"))) ? ((1024 * 16) != (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("ccf40c9f-b45a-314c-84dd-1805e979e1b0"))) ? ((29) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("cb3877f4-a2d7-3bd1-8177-adb1549e39a2"))) ? ((30) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("5c754b0d-4109-3279-8cc6-aef0dab26262"))) ? ((30) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("550fb49b-e923-34d8-8391-aae7dd3b2096"))) ? ((1024) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("490776a1-00c1-3194-bf76-af654e2a7fa8"))) ? ((1 << 30) == (16)) : (((KnobRuntime.check(java.util.UUID.fromString("6ca8cb26-f2e8-3cb4-adc0-584ff28a9e01"))) ? ((1024 * 16) != (16)) : (((KnobRuntime.check(java.util.UUID.fromString("130b894d-7fd0-3291-b34d-8b7439580c3a"))) ? ((30) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("dcc02dfe-124f-313c-a550-5e80d9c5c09e"))) ? ((1024 * 16) == (16)) : (((KnobRuntime.check(java.util.UUID.fromString("a7ea1283-55a6-3e58-9507-182287f1dfe4"))) ? ((floor) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("fc32577c-2fc5-382c-bb8b-1d05abdcc24a"))) ? ((floor << 1) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("6dd5d5c1-3394-3a08-8d24-3eea030ff36f"))) ? ((1024 * 16) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("e42007b1-d23a-3303-8b15-8da810a48d72"))) ? ((1024) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("532e7956-e766-3e84-828a-eb4e5428c670"))) ? ((1) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("d2afe00d-a7d3-3cb5-b13a-7b8a8299ed7b"))) ? ((floor) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("335be5a2-1814-31b3-a5e9-fe2ad8af728a"))) ? ((1) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("913db3ab-4684-3338-8857-c875790dff78"))) ? ((1 << 30) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("0959077c-ab34-3c52-84d2-b0e7b8db70ac"))) ? ((floor << 1) != (16)) : (((KnobRuntime.check(java.util.UUID.fromString("1c3dc1da-2c8b-3044-8e8b-9cefb426d3fa"))) ? ((29) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("5994aa3e-9de8-34b8-bfde-c12400a41821"))) ? ((floor) == (16)) : (((KnobRuntime.check(java.util.UUID.fromString("eddc34a7-be21-3403-a3d1-6f9b396f4df7"))) ? ((floor << 1) == (16)) : (((KnobRuntime.check(java.util.UUID.fromString("4e60abbe-fa63-360a-9f80-18f24b9d7416"))) ? ((floor << 1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6fb9ac01-d475-3bbd-b708-7d92d93e49c0"))) ? ((floor << 1) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("c25bd089-7260-35f3-a12d-e4b617954e53"))) ? ((29) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("091bf8f9-b426-3b18-b10e-8967557f7182"))) ? ((1 << 29) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("87241dc1-7610-3f26-ab20-4176edcfb7ed"))) ? ((floor) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("a771d7e7-9c49-38c4-9c33-7c14e9c3e1dc"))) ? ((floor << 1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dfb9bd94-a4f2-3705-9f07-02495bad798c"))) ? ((floor) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("a30cd292-3d83-31bc-ac95-0510bf07f724"))) ? ((1 << 30) != (16)) : (((KnobRuntime.check(java.util.UUID.fromString("d42384f1-c7c4-3bed-879d-b2eccb8c9a4a"))) ? ((1 << 29) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("b395de38-ac36-33a0-8dfa-536c4a4b6b69"))) ? ((1 << 29) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("683c2d22-093d-3010-87cc-aff3fd3f1498"))) ? ((16) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("1551980b-5402-3db0-bf13-d898d8a3519d"))) ? ((29) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("6b11bcbd-2bd7-3137-906b-8b5dcb9f3982"))) ? ((1024 * 16) != (29)) : (((KnobRuntime.check(java.util.UUID.fromString("3b95586a-3bc7-3774-87e6-1cc45e04b17b"))) ? ((1024 * 16) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("4eaf22b8-0e60-354f-b64a-514451efc7a2"))) ? ((1 << 30) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("16b21d78-c403-3236-963f-d22a605a1b68"))) ? ((1 << 30) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("7507512f-9328-3c74-a74a-19d0d08d7f01"))) ? ((30) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c55fb3ae-ce06-3e88-a57f-794c4e66e2fa"))) ? ((1 << 30) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("dd1bbb05-6802-3951-bbf6-db820071e12c"))) ? ((1) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("f6b842b6-42a2-3864-a004-62c00cb22a0a"))) ? ((1024 * 16) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("932ef5f7-7f4d-33e7-927e-2c8c34b74616"))) ? ((1 << 29) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("24036273-ea2d-3ff6-9e84-a0d9da00f243"))) ? ((0) == (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("0c64f208-39d5-34f9-bd03-7f022ef85550"))) ? ((floor << 1) == (30)) : (((KnobRuntime.check(java.util.UUID.fromString("82079d76-86c6-38a2-8240-bfe244fd1210"))) ? ((16) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("4526e802-77f7-3e9c-86eb-cb6ea0c1c212"))) ? ((floor << 1) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("4ae796c5-90a8-3cc7-8396-1a0e477cf846"))) ? ((1024 * 16) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6b0a6fe9-5b42-33f8-a632-ccb7cfc541f4"))) ? ((1024 * 16) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("af30d8c3-dbf3-30e0-85b7-d267e6c2f2b1"))) ? ((1 << 30) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("f8334232-9ad6-37fd-aa0c-aab8588170e7"))) ? ((1 << 29) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("49ea8bec-ca7a-3351-870b-dec6987d5460"))) ? ((1 << 29) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("4379cd2f-2310-3800-a63c-9e5d747ed455"))) ? ((1 << 29) != (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("d8e6b8fd-facc-3eb7-a0d1-b8ab160f41bb"))) ? ((1) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("863041d6-8665-3a60-b60e-e7c7a7d51b62"))) ? ((1024 * 16) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c93a306c-f61c-3c37-8e21-61caf123e7ea"))) ? ((0) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("89bce1f0-5c2b-3fe3-a6e5-ed7f1963dae0"))) ? ((1 << 30) == (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("af137601-8173-31a1-8064-bcc2c5f0437b"))) ? ((1024 * 16) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("c78f9dc8-dc97-30d7-84a4-fbdf61575c76"))) ? ((30) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("2e7efabb-def0-3764-8902-73cbaedd2eec"))) ? ((30) == (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("791d6cf8-89b6-3758-8804-fcf87e170fb5"))) ? ((1 << 30) != (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("876f8053-ee08-3d88-b002-4c6fe6196a80"))) ? ((0) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("65cc06f1-23b2-3208-9b5e-469797a17522"))) ? ((floor) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("f1bb0c31-b99f-32c3-a354-538b786d046c"))) ? ((1024) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("8452503b-8e68-348c-9927-70a354b94917"))) ? ((floor) != (30)) : (((KnobRuntime.check(java.util.UUID.fromString("f93bfaa7-6ae1-3874-a867-1f82b576b102"))) ? ((16) == (1024 * 16)) : (((KnobRuntime.check(java.util.UUID.fromString("7dac8db1-1185-3395-8565-373e99e4e85a"))) ? ((1024 * 16) != (1 << 29)) : (((KnobRuntime.check(java.util.UUID.fromString("5fd1a22b-4409-38d4-b464-8c861da514b5"))) ? ((16) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("38e95e6b-48f3-325f-9dc4-7cea52ad0b9e"))) ? ((29) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("3ef17253-0f58-3fce-8fa2-f24209534ea2"))) ? ((floor << 1) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("6b3c54a9-c3e3-3701-b30a-ffa98440c354"))) ? ((1 << 29) != (preallocatedEventCount)) : (((KnobRuntime.check(java.util.UUID.fromString("20be99fe-96a4-3cb8-b7e3-39019cdf1926"))) ? ((floor) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("f27bfec8-9b92-3c50-9d69-ccbcff3d1c5e"))) ? ((1024) == (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("c11725c4-9995-3dbc-a15e-65c0de777648"))) ? ((1) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("cb961dd3-caf2-31a8-a943-2361226b40e4"))) ? ((1024) != (1 << 30)) : (((KnobRuntime.check(java.util.UUID.fromString("8c98739f-b0b2-3317-baf1-d291df5f5fdc"))) ? ((floor << 1) == (1024)) : (((KnobRuntime.check(java.util.UUID.fromString("1e7a62cd-9223-30a7-9662-4be898d83acb"))) ? ((1024 * 16) == (29)) : (((KnobRuntime.check(java.util.UUID.fromString("0e61b786-e034-323d-a7a5-7a4c111beab5"))) ? ((1 << 30) != (0)) : (floor == preallocatedEventCount))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return floor;
    }
    // max capacity is 1 << 30
    if (floor >= 1 << 29) {
      return 1 << 30;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("023f20c9-7c2b-3d66-a8c0-6d15d04283f4"))) ? ((1) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("0d3b29e1-fc17-386a-8d92-ff5f6c81d3d1"))) ? (1) : (((KnobRuntime.check(java.util.UUID.fromString("c8e183fd-a7c1-3c52-bf3a-6b5399748a30"))) ? (1024 * 16) : (((KnobRuntime.check(java.util.UUID.fromString("479722cc-b2a0-39ce-8aff-fef133ac36d5"))) ? ((1024 * 16) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("bb10d19b-a27a-3c8a-81a4-cf00abde14fd"))) ? ((16) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2e34c3a4-9314-39bd-9868-9ecb032c700c"))) ? (floor) : (((KnobRuntime.check(java.util.UUID.fromString("a2c14ff0-88ea-3b8e-a013-cedb37f5f875"))) ? ((floor << 1) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d4f0d3d7-55b8-32cc-8dfa-1419a48859f9"))) ? ((29) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("ad79bd85-db4a-3f06-8414-015a4cb2695f"))) ? (0) : (((KnobRuntime.check(java.util.UUID.fromString("946120cf-5c65-3687-ae70-09837c536f13"))) ? (1 << 30) : (((KnobRuntime.check(java.util.UUID.fromString("26dfe98f-24ec-39f5-a876-59607067a2b5"))) ? (30) : (((KnobRuntime.check(java.util.UUID.fromString("8cbca671-3aae-38dd-a61c-32d704145346"))) ? ((1024) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("712ce0b4-409e-38fb-b384-15fc26599330"))) ? ((1 << 29) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e704aa72-99c4-33d0-9c41-f88c3395a090"))) ? ((0) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("93c7c02f-fa00-3a09-8ae4-501d8497094e"))) ? ((30) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2450caf8-c3d7-3a0b-ac26-2026107e1322"))) ? ((1 << 30) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f3652835-6017-3187-a204-73776240d135"))) ? ((floor) << (1)) : (((KnobRuntime.check(java.util.UUID.fromString("594d0a36-0a08-3e40-ab1d-4fcc67c2bae0"))) ? (1024) : (((KnobRuntime.check(java.util.UUID.fromString("c4ee81f5-896b-34a2-8dda-054d9a7bb74e"))) ? (29) : (((KnobRuntime.check(java.util.UUID.fromString("5caf4a9f-0d25-3397-9f3e-5ce134aee5ff"))) ? (1 << 29) : (((KnobRuntime.check(java.util.UUID.fromString("66579993-e963-315d-bc6f-fd2402860602"))) ? (16) : (floor << 1))))))))))))))))))))))))))))))))))))))))));
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
if(KnobRuntime.check(java.util.UUID.fromString("0f851405-28b4-3e6e-8b38-5f9bd124f51b"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("579093cc-e9eb-39ef-b3c2-ba0f272afc1a"))) {
return false;
}
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

    if (((KnobRuntime.check(java.util.UUID.fromString("8b195148-cf5d-3079-8243-34e1a37fe309"))) ? (!failIfWALExists) : (failIfWALExists))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e7289ee1-654b-31d7-897c-a2b9e6a08ea7"))) {
return;
}
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
if(KnobRuntime.check(java.util.UUID.fromString("8ededd8c-16bc-38b3-b40b-57a9ee72b0d3"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c51ab7bc-97a1-3044-a5f0-ff696cf4dc2e"))) {
return false;
}
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
if(KnobRuntime.check(java.util.UUID.fromString("d3519c2f-255e-39e1-b975-12d02ea935e2"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("d835ea93-2156-34bb-9b6d-600a9cf3dcdd"))) {
return;
}
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName) {
    // Used by tests. Deprecated as too subtle for general usage.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
if(KnobRuntime.check(java.util.UUID.fromString("0ece25ab-00cd-39ad-9358-e7f5ca7c9b28"))) {
return 0;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("6a119aab-b619-35d7-a2d2-b2b5ed2c06d0"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("0cf2e9a2-1ac5-3110-81d7-fa76f4891308"))) ? (rollWriterInternal(false)) : (rollWriter(false)));
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
if(KnobRuntime.check(java.util.UUID.fromString("a56d0d74-86d1-332d-b641-8a429f113a29"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("996eda49-fd5e-36a8-a317-34243a9dae6b"))) ? ((filenum) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("229b022d-b6f2-3884-9ce2-8625f25c190f"))) ? ((filenum) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3e896568-ec58-346d-80dd-48f009eafed5"))) ? ((filenum) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("20ba9c67-5c36-366a-9b41-1e9dedc55654"))) ? ((filenum) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("60a4d854-820c-36cc-b435-a1736bc38bfa"))) ? ((filenum) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("22a79041-fe0c-3d06-9a0e-6dab76d4293d"))) ? ((filenum) <= (0)) : (filenum < 0))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f0be5e2c-3c21-3fdc-b82a-10484c39af51"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("4e23abd5-e54a-3f6e-9e8a-2c2615bac8e8"))) ? (computeFilename(EnvironmentEdgeManager.currentTime())) : (computeFilename(this.filenum.get())));
  }

  /**
   * retrieve the next path to use for writing. Increments the internal filenum.
   */
  private Path getNewPath() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a39e2206-ec46-3597-b01f-bcd2c8c049e3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("51fca4dc-211d-321f-8777-6e5a4f9e33c4"))) {
return null;
}
    if (KnobRuntime.check(java.util.UUID.fromString("71166664-cd01-30f5-a226-9b79d7ca699e"))) { this.filenum.set(Math.max((getFilenum()) + (1), EnvironmentEdgeManager.currentTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("afe14b5c-adcb-37ca-a046-3f89c9536218"))) { this.filenum.set(Math.max(getFilenum(), EnvironmentEdgeManager.currentTime())); } else { this.filenum.set(Math.max(getFilenum() + 1, EnvironmentEdgeManager.currentTime())); }
    Path newPath = getCurrentFileName();
    return ((KnobRuntime.check(java.util.UUID.fromString("a45ea3dc-a6bc-33dd-be1c-c7b644785205"))) ? (getCurrentFileName()) : (newPath));
  }

  public Path getOldPath() {
if(KnobRuntime.check(java.util.UUID.fromString("c3bb34b0-898a-384b-ae82-5b0b66017dee"))) {
return null;
}
    long currentFilenum = this.filenum.get();
    Path oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename will take care of meta wal filename
if(KnobRuntime.check(java.util.UUID.fromString("7df07cf6-2768-3f8f-b6b5-3b07fd71cce9"))) {
currentFilenum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("185b670d-b271-3e48-8e8b-5aff55e77725"))) {
currentFilenum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f8fc310b-3c59-3366-b006-a517f1c754cb"))) {
currentFilenum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("29b84658-99eb-356a-968b-880c1c6a038b"))) {
currentFilenum += 1;
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("83993376-0873-3402-a58b-88645f6eb1d2"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f238a655-2402-3434-ad94-5a45b9930af4"))) ? (((getNumRolledLogFiles()) >= (this.maxLogs)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("11fab4a6-66fc-3999-9cf9-1dc741b94763"))) ? (((logCount) != (0)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2e035294-6d06-3175-a4fc-6ebefd7d0c22"))) ? (((logCount) > (this.maxLogs)) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0c9785aa-6e80-30cf-a968-8d9d4bd36d6a"))) ? (((0) > (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d599c5af-4a84-32a1-a4ee-3e7f13b76df1"))) ? (((0) < (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3451461c-daf0-3c69-b36c-117da09d96c4"))) ? (((0) == (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("12c71e8d-a83a-3d8a-b3b6-5674a4d41915"))) ? (((logCount) == (0)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c188ba0-3c74-3134-96c1-01f20843175f"))) ? (((0) != (this.maxLogs)) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("08dab4e9-f8b5-38cd-8791-2f37db862f6d"))) ? (((logCount) != (0)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0cacb092-1862-37af-80f9-12a572a15df6"))) ? (((getNumRolledLogFiles()) == (0)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b820875f-521c-33bd-833c-a8bdc119cb19"))) ? (((getNumRolledLogFiles()) == (0)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("698f31d4-63da-34e0-836a-bbd59064aa1a"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ff62665-6bad-3a68-bf48-b38398bccd77"))) ? (((getNumRolledLogFiles()) == (0)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("572d5d89-cba1-3677-9cd8-6346a4623909"))) ? (((logCount) >= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c0ece87d-5aed-3710-8714-22343765c0fc"))) ? (((0) < (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4db5c99a-7b1f-3d5a-b28a-fa018e6091ae"))) ? (((logCount) == (0)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("019e6d2d-0cb3-329a-a459-e8185a7cc285"))) ? ((logCount > this.maxLogs) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("953cd294-7961-37c5-9ac4-372fae4b65a1"))) ? (((0) < (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9e5a215f-16a3-31f4-a713-4d47ad594ace"))) ? (((0) <= (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ccf4403d-8c65-331d-ba63-1796c1115398"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8e2d2bfb-6692-327a-8e78-ec7fe9ab2632"))) ? (((logCount) >= (0)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("049ffc10-bf35-357b-8a98-00a2894b563a"))) ? (((logCount) >= (this.maxLogs)) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("efc67a90-8b92-3ecb-b042-46547b96f490"))) ? (((logCount) != (this.maxLogs)) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ab1b1273-67ed-3662-8af2-f5022203af2f"))) ? (((logCount) < (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5e070b0c-12b1-35fc-aa21-9fc35771f526"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a237f796-7f5e-3777-bd75-2603dc43fd52"))) ? (((0) > (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f2d72fc-55b8-3670-9b50-d8ba3bac256f"))) ? (((logCount) > (0)) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d2cad78b-45ea-300b-be2e-f1ebf0e3fee9"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0a25e1f2-33b3-33fb-8e6e-ccff1c7c2403"))) ? (((0) < (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c2d08190-7269-3ed1-b207-5d8fe4162465"))) ? (((0) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0f4c4861-8048-372a-a16d-51db1f7c0b7c"))) ? (((logCount) == (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c51ada0-ae57-39f0-90de-dcd1cb91aef2"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("f4a4450e-4ea3-3b91-9c41-c68eccba9008"))) ? (((0) != (this.maxLogs)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("11a9151b-fea1-3f9a-bbf8-2899aaa92962"))) ? ((getNumRolledLogFiles()) >= (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("00b2f9ea-fed4-3d80-8e33-4b458d35eb57"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9f9d7c43-750b-3404-bed2-a10e63542ade"))) ? (((getNumRolledLogFiles()) < (0)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("859db860-23e2-323e-9d7e-a0d438a36fe7"))) ? (((logCount) == (0)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("47f59c67-a518-3c90-97da-a8cc9759fb97"))) ? (((0) >= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc429885-63bf-39f6-ad24-5cafa72545a1"))) ? ((0) == (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("4333de15-3cf3-3729-994c-a76f84c27d57"))) ? (((getNumRolledLogFiles()) > (0)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6fb38dbc-d139-37bd-82d0-2d3bd36dcdc8"))) ? (((logCount) == (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bf1afa0b-accf-3104-8d98-b6c2cdee1ca8"))) ? (((0) <= (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d7fda7c-67af-3fcd-91f6-7741a3a4e1cc"))) ? (((getNumRolledLogFiles()) <= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e33143de-6e27-3dfc-828f-14dcd8602532"))) ? (((logCount) == (this.maxLogs)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4408f014-b14a-3646-bb21-4d1d870c3a00"))) ? (((0) >= (this.maxLogs)) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("af3c017f-8428-3ed8-93c4-316855e44041"))) ? (((getNumRolledLogFiles()) > (0)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("314a106f-56d2-3c28-a14d-835c205883d6"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a1d1c5a6-119c-3d16-a090-fde0c5b8256f"))) ? (((logCount) == (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("15cd46ce-0f84-36a9-b70b-e7f1f1bca256"))) ? (((getNumRolledLogFiles()) >= (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7621c062-8b7f-3605-8c3c-d8dc9285f032"))) ? (((logCount) == (this.maxLogs)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c1238000-2834-3003-8691-4856e7fe32bd"))) ? (((0) >= (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("73996fff-4e71-3237-8be9-204e41bc4d9f"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("58267342-9b53-32bc-a55a-0a87e869843d"))) ? (((0) <= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aefc6fc3-43a2-3953-b5c5-31abf486e1f9"))) ? (((logCount) >= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("56ec8cb2-287f-3081-9031-4aa0d6a528db"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("26b8d784-5246-3cd1-b22d-bcddff914033"))) ? (((logCount) == (0)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("646b9d09-720e-3842-89b6-10e890736621"))) ? (((getNumRolledLogFiles()) == (0)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9459f433-8a06-30f1-9c5f-267e005e2a00"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a5cdc07-847d-3816-8527-d6d516f1f7e1"))) ? (((logCount) >= (this.maxLogs)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bbed3d57-8ec7-3a8e-8016-3466d7d2fdea"))) ? (((getNumRolledLogFiles()) != (0)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ba72a68d-2393-3166-a4f2-f9804df495e7"))) ? (((0) > (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4b73bd83-5aaf-39d4-aeed-322dc49c0ce7"))) ? (((logCount) <= (getNumRolledLogFiles())) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3d4da55-bbc8-3870-b50a-10e1272d8b2c"))) ? (((logCount) == (getNumRolledLogFiles())) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8cc7d241-9473-3c69-81b9-f4278ce3ace7"))) ? (((getNumRolledLogFiles()) == (0)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("643eeea5-0834-3f03-a8d1-05eb5a1fcca6"))) ? (((logCount) != (this.maxLogs)) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9e41b4fc-66ef-3007-9473-a4276899ea24"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e535473c-17ae-32c3-8bda-63e717d67394"))) ? ((getNumRolledLogFiles()) <= (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("951d01cd-ebf1-37f5-b214-c6813fa75eef"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bb77db3e-4bb3-3482-8a26-e78d50a7960f"))) ? (((logCount) == (this.maxLogs)) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6fe6b267-9ae2-33d3-ad6c-33da15b4184a"))) ? (((getNumRolledLogFiles()) < (0)) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("afb7c23e-8d73-345e-a910-35b9d516ec82"))) ? (logCount > this.maxLogs) : (((KnobRuntime.check(java.util.UUID.fromString("7c51c848-8c01-37a2-8b0a-26e9e40a4d28"))) ? (((logCount) != (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3816be02-bccc-3a57-9e4f-0a33bd300fec"))) ? (((getNumRolledLogFiles()) >= (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("53a42dbb-646d-3845-89d6-6c92273a60e6"))) ? (((getNumRolledLogFiles()) == (0)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d7cc54b1-1bf6-31e2-a870-7338eac05504"))) ? (((0) > (this.maxLogs)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("554cc528-e388-38d0-ad8d-467f616e558f"))) ? (((getNumRolledLogFiles()) >= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("46f1e108-b84c-3b8f-8cab-cf19c8567fab"))) ? (((0) <= (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a35c3564-2f39-3637-a839-ed1da0e1be9a"))) ? (((0) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e82ff1b2-3fa2-36bb-a0a3-ab2853013b75"))) ? ((logCount > this.maxLogs) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1c1114d4-e54a-32ce-9175-f1d6403b4ada"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca4c3dcf-e77f-3ced-8125-d0136685b3a3"))) ? ((logCount) > (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("c70bc3f9-5d41-38ae-8b86-c5a5b45d5a0b"))) ? (((logCount) > (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ea8f7ae9-0ed2-38b7-a3d4-7d1de2f1fbde"))) ? (((logCount) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0f004c6f-8242-36fd-b664-3fb3b1611915"))) ? (((getNumRolledLogFiles()) < (0)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4fc176f4-1bba-33b8-a99d-86205f93f018"))) ? (((logCount) > (this.maxLogs)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d48d7e3f-6752-32e6-96d7-1a9a2ea03a82"))) ? (((logCount) == (0)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c2a8fcb-658c-3ee4-b03b-3d09094cc91b"))) ? ((logCount) >= (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("1b83cd88-bd46-3f3b-8138-323a0c0bf2b2"))) ? (((logCount) == (0)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bde94f1f-9c9f-3424-98ca-d2a66ebddbf6"))) ? (((logCount) != (getNumRolledLogFiles())) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02e84e43-2e8e-321a-8e07-a81ca9e55242"))) ? (((logCount) <= (0)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b3ddcc48-6a61-3383-8a5f-6971685c81e5"))) ? (((0) != (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("35df20ef-434c-32c4-b16b-0c74be874b78"))) ? ((logCount > this.maxLogs) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("566354c9-e325-3964-9971-50da10b5771c"))) ? (((getNumRolledLogFiles()) < (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91cdc011-da73-338c-8b39-b9a005e04b93"))) ? (((logCount) >= (0)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ffca4530-c519-374d-8700-17b305fdd072"))) ? (((getNumRolledLogFiles()) > (0)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("250782ff-f300-36be-96d9-ade64e9d357a"))) ? (((getNumRolledLogFiles()) >= (this.maxLogs)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d0a3fe9-c93f-397d-85c6-ce4f782938cb"))) ? (((logCount) != (this.maxLogs)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4690a85f-2384-35d2-af47-d65fe10bafc3"))) ? (((getNumRolledLogFiles()) >= (getNumRolledLogFiles())) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ce2ff66d-8b03-32f6-b7c5-dd9598a0c54b"))) ? (((logCount) >= (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("34417260-354f-3751-88f6-14a0f9f30aee"))) ? (((logCount) != (this.maxLogs)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7775378f-c2f7-325b-bf12-e11a58f1dee3"))) ? (((logCount) != (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bc95efef-7c1a-3be3-a105-d34a96c96c8f"))) ? (((0) < (this.maxLogs)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39088e2d-8281-33b7-9141-e765f9d0a19a"))) ? ((getNumRolledLogFiles()) == (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("5c603b4c-912a-3803-8fc4-9e2135852c77"))) ? (((logCount) < (getNumRolledLogFiles())) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("24e43c92-86f5-36bb-abea-38e0228e3658"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fb82e0c1-db56-34ef-b516-3bd283d2678d"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2b0cdc6-8e25-394e-9ff7-6edef15c9b5f"))) ? (((logCount) != (0)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b7c39418-3ce6-3cd5-a36d-f61cfa217b94"))) ? (((0) == (this.maxLogs)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1384020e-7381-3272-a57a-6436e9aa440a"))) ? (((getNumRolledLogFiles()) <= (0)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1ddab2fd-99c7-3908-bcf4-a358690fff87"))) ? (((0) == (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("809645c0-3f27-3bdf-b36b-2db194e501e0"))) ? (((getNumRolledLogFiles()) == (0)) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("73f8652b-bb8a-36ce-9bb5-10b289fefcf1"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6dd1cc4c-06c4-34e2-af4d-be611821f62a"))) ? (((logCount) >= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e877508a-5704-3cbd-8ba8-6fec664b2564"))) ? ((logCount) == (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("5fa4e439-ee6b-38da-a7c0-36a5ef815df0"))) ? (((logCount) < (getNumRolledLogFiles())) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2ee9e955-dcdb-3157-a4d2-5f3cb0c481a4"))) ? (((0) < (this.maxLogs)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("97c7d36b-5263-3672-991c-b9ea25932ba2"))) ? (((logCount) >= (getNumRolledLogFiles())) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0307d779-c576-3c47-8828-36e3e1f7ee93"))) ? (((logCount) != (this.maxLogs)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1fbbac40-5306-36f0-9a84-fc8be38f14ed"))) ? (((0) == (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cdceec30-aed2-3f2c-8993-1ae6c22661ce"))) ? (((0) > (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c3f1f14-912f-3d90-b321-c764abeb2fe2"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7f037206-7d78-3476-9eed-377ec2c218c5"))) ? (((getNumRolledLogFiles()) > (0)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a05d0a1b-549c-36f8-8970-ec7dce056d6e"))) ? (((logCount) > (this.maxLogs)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7b39a01a-c19a-3033-92a3-bc494139552e"))) ? (((logCount) < (0)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fe5dc3b8-7c28-38b9-bf8d-a6cc0abdcf2a"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1ecb8b37-f3d5-396f-bab4-4ecb8ef5e9fc"))) ? (((logCount) > (this.maxLogs)) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ad1964e0-0a00-3791-a5b6-43e4ebe15355"))) ? (((logCount) > (0)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5bd3f284-2c49-3d20-a222-fb37d9cb7d59"))) ? (((logCount) >= (0)) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ecaf5a02-8597-3499-a558-925357f02ebc"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a6e53ea6-9b94-3b61-b046-06508e187d21"))) ? (((getNumRolledLogFiles()) >= (getNumRolledLogFiles())) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3c781fba-c743-3626-a9cf-c4de919b5cb5"))) ? (((getNumRolledLogFiles()) >= (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("937fec89-2ef9-38a9-b2ce-ef6b96d675f4"))) ? (((getNumRolledLogFiles()) > (this.maxLogs)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("846d14ea-4ab7-3db3-ab9f-51dd43fdf50e"))) ? (((getNumRolledLogFiles()) < (this.maxLogs)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4e25d837-8a7f-3be6-a643-f58d6cf0b953"))) ? ((getNumRolledLogFiles()) < (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("583ebc49-ae57-3eae-98f2-53735af3fe7e"))) ? (((logCount) <= (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("441264c8-312d-3082-880e-773455ffc2cd"))) ? (((logCount) > (this.maxLogs)) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9cee4bee-9426-3cda-9b0b-a76412c09c2e"))) ? (((logCount) == (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8a5daaf3-06e7-389d-a1ee-1652b84ae15c"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("30f4b51a-e4d0-3f1a-88d4-58d54ef9d259"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38335f77-ddb3-30fe-9bef-002428746349"))) ? (((logCount) < (this.maxLogs)) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9160a548-c829-363b-906f-8b4a59ac49cc"))) ? (((getNumRolledLogFiles()) != (0)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf8e4237-712b-3a00-8f61-101515d223fd"))) ? ((getNumRolledLogFiles()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f688b29f-2503-3f41-b56e-274738098fd0"))) ? (((getNumRolledLogFiles()) != (0)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("18d01f7a-d3fc-3e04-8146-dd03bdafbe4c"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("df950568-702d-3215-adc5-9d170fae66dc"))) ? (((logCount) != (this.maxLogs)) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("df20247d-f5b3-3834-9911-568e2e9938d8"))) ? (((getNumRolledLogFiles()) <= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6ff3dcc0-9a74-3b2d-a6a6-73494cef9b0e"))) ? (((getNumRolledLogFiles()) <= (0)) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("81613655-a6ac-350f-929e-4cda35e834c8"))) ? (((0) <= (this.maxLogs)) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e19f648d-365d-3318-ae25-5fc70e0e47e0"))) ? ((logCount) <= (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("2a003fd3-ecc3-3224-95e1-606a0fea9ab5"))) ? (((logCount) != (0)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("61af1277-8144-3a6d-be1d-473f169e9a7f"))) ? (((0) >= (this.maxLogs)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4363fbca-8f18-3137-bc85-a1ab435840f2"))) ? (((getNumRolledLogFiles()) < (0)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7cf6e7a2-5da7-313f-888e-9e0a312bcb1f"))) ? (((logCount) == (this.maxLogs)) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ced68529-5b2c-3d63-8c2d-7e61310b59c5"))) ? (((getNumRolledLogFiles()) >= (this.maxLogs)) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b1a223b3-4fbc-3c8e-891c-36397ea6f66b"))) ? (((logCount) >= (this.maxLogs)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4b3fd21d-5032-32cc-b5c4-4bbec99b5f5e"))) ? ((logCount > this.maxLogs) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f9e8a3c7-374e-3b97-a80a-b8647a9c1037"))) ? (((logCount) != (getNumRolledLogFiles())) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("94b5724c-77f8-375e-ad13-02b8c4925574"))) ? (((getNumRolledLogFiles()) > (0)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3347a8c4-3c9c-33de-9d73-f6440ffff033"))) ? (((logCount) <= (this.maxLogs)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("54fc1fd0-c8df-33d0-8261-79822263889a"))) ? (((logCount) > (getNumRolledLogFiles())) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02b59e6c-463e-37ed-867b-e545ca73fda3"))) ? (((logCount) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("840c8a00-47d0-31f8-a0f1-669025ded74f"))) ? (((getNumRolledLogFiles()) < (0)) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ab9064da-59ed-336e-a38e-cd4129dc5010"))) ? (((logCount) <= (0)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("195f9c29-3f7b-3a09-ac67-5c509a6f8c25"))) ? (((logCount) >= (0)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d1ef1dcb-e549-3990-99ce-66ff4972e088"))) ? (((0) >= (this.maxLogs)) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9f043c4a-49e5-3cf3-b59a-a3dab789a3ea"))) ? (((logCount) > (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("100601d6-4499-3302-a93a-c6cb946fff0d"))) ? (((logCount) != (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9b72a5c2-49ac-3d52-a7f2-78f88391d732"))) ? ((0) != (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("b777a163-14ba-306c-abf7-c888e342789c"))) ? (((getNumRolledLogFiles()) > (0)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0f4b441d-0920-3e88-8700-20381c238e2e"))) ? (((logCount) == (0)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b866ccfd-f7ad-39f5-8bc1-3fc6e40b88b1"))) ? (((logCount) < (0)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("45deb223-47ca-3af3-8720-29ea0e79363b"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7e8005d5-d9ea-3803-9922-eab740144ee8"))) ? (((logCount) >= (0)) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("82dd0e32-5e45-3cf7-ab23-d387efd22aaf"))) ? (((getNumRolledLogFiles()) <= (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("64d727a8-39fc-3535-912d-b5c63b34b37b"))) ? (((0) != (getNumRolledLogFiles())) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a7050ab2-c80b-3713-b023-d409aee10013"))) ? (((logCount) != (0)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("934e49e3-3c40-3c54-88d3-6238c4e7279d"))) ? (((0) > (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fd983a58-70d8-3537-89e4-8fadf1fadd40"))) ? (((getNumRolledLogFiles()) <= (0)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("01addd38-d620-3561-9d0f-4badcba22474"))) ? (((getNumRolledLogFiles()) <= (getNumRolledLogFiles())) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("5fa4c236-09bf-3d01-949f-95da61a8d315"))) ? (((logCount) <= (this.maxLogs)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7c018f79-ead5-356c-b66a-d7f3fe75a25a"))) ? (((logCount) < (getNumRolledLogFiles())) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91af4e98-201c-3575-a97f-86c4664fc7c6"))) ? (((logCount) != (getNumRolledLogFiles())) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1816589a-889e-38e4-a807-f84c6aedbc0d"))) ? (((logCount) > (this.maxLogs)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dc2b3e0e-1418-3320-b169-9612a8f5246e"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c180fbf6-8d34-370e-9e0c-4d3132eaa63d"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1208f998-57a0-301e-afc1-6943bec93a6b"))) ? (((getNumRolledLogFiles()) > (0)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("bb26e13f-a292-3a54-91e2-b48155b2a997"))) ? (((0) >= (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7f3298e8-1389-3c6b-8267-1e77d6e977d6"))) ? (((logCount) < (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("796a2de7-b27d-31d1-90dc-fe9bc06e0815"))) ? (((logCount) < (this.maxLogs)) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6ae8ae15-4de0-3b39-aa8f-a8bfd08ff6ac"))) ? (((logCount) > (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("222e4913-58a5-32b9-a8c3-2284bcbe47fc"))) ? (((logCount) != (0)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1bd3471f-c6b0-3afa-a145-e29b5a99c2d4"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39ece3f2-9965-3266-b570-3604f28541fe"))) ? (((logCount) != (0)) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d79a740-bd42-3c1c-bdfe-e60254879502"))) ? (((getNumRolledLogFiles()) <= (0)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a127030f-d869-3633-80e1-8bfbdec14e65"))) ? (((0) != (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0fedb7ba-a3a4-3ea4-8db6-122ca415ab71"))) ? (((getNumRolledLogFiles()) <= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fcc66191-b471-3a46-8929-ac3a1412a7f2"))) ? (((getNumRolledLogFiles()) != (0)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d4f272ad-5225-3a1c-887b-2b114ba8871e"))) ? (((0) <= (this.maxLogs)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f3a557b5-63e6-3d27-a9d6-709f8cdd0db9"))) ? (((logCount) > (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6f726145-ae9c-3201-9d0d-a8e316d35aa1"))) ? (((logCount) >= (0)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bde4fb26-2388-3426-918f-91eb8b5a67bf"))) ? (((logCount) >= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("21161ad0-9bec-3780-a27c-2ac0979665a5"))) ? (((logCount) >= (0)) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e9aa516c-50c8-30ee-93e7-8286620a1407"))) ? (((logCount) < (0)) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("21e3c0a2-6446-3a1b-8ef8-c82bf7c87bfc"))) ? (((logCount) > (0)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4e21c44a-9844-30fe-8dc3-ebdfea79fd17"))) ? (((logCount) <= (0)) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0c1e2303-df14-3b3a-b305-534dc288251e"))) ? (((logCount) > (0)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("577d89a7-385b-36fc-876e-41a19d1f9d87"))) ? (((getNumRolledLogFiles()) <= (getNumRolledLogFiles())) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3daa144-66fb-3499-8656-89f324342d39"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a5a75e39-1c65-30fd-9410-2fe83e7dad59"))) ? (((0) < (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fe3dbd25-ecc6-31a2-b3f6-e71551b4383d"))) ? (((logCount) >= (this.maxLogs)) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("adb0a240-ea9f-381b-b4e8-cae131e50744"))) ? (((getNumRolledLogFiles()) == (this.maxLogs)) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aa0f9b0c-71bf-3df2-b55c-46d620590c3f"))) ? ((0) >= (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("d401de96-a6bf-388f-ab1e-b76072387c44"))) ? (((logCount) >= (0)) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3e813ce5-b8fc-3f68-aa15-ef4e7f52c934"))) ? (((0) > (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca6ced31-851a-3704-92ac-8517a10981eb"))) ? (((logCount) == (getNumRolledLogFiles())) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("129c84dd-e4e5-3793-99e5-53a00cc0f600"))) ? ((logCount > this.maxLogs) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("106183ba-7682-309c-810e-7858480f027f"))) ? ((0) <= (this.maxLogs)) : (((KnobRuntime.check(java.util.UUID.fromString("2b8a3da6-18e4-396e-bcae-2138a8753c35"))) ? (((getNumRolledLogFiles()) <= (0)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bfa815ae-eb7f-349e-acad-f0f16744f750"))) ? (((logCount) < (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("574547c7-efc6-3d37-b4b8-8b0ab3b082c4"))) ? (((getNumRolledLogFiles()) == (0)) || ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c8c84020-1aa6-3505-83ea-b356d22e8b6e"))) ? (((logCount) > (getNumRolledLogFiles())) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2e3f1d43-c7b2-382a-8220-423a72df9fc2"))) ? (((logCount) > (getNumRolledLogFiles())) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("01b07a88-6a83-3761-b5ab-5e5f50f52863"))) ? (((0) <= (getNumRolledLogFiles())) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a866bb6e-de26-30af-a5c8-310838234ffc"))) ? (((logCount) > (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52b5b217-fc8a-3044-b7e1-b0a3a7db546a"))) ? (((logCount) <= (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a50e499d-b5c0-3d39-a8ee-cdcbd3df2c44"))) ? (((0) != (this.maxLogs)) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("57c81ed5-b9ec-38b7-b653-5819a5d09fb8"))) ? (((logCount) > (0)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de926878-5199-3de2-bb4f-c2d1f2e5098d"))) ? (((logCount) >= (getNumRolledLogFiles())) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("84eb9c9f-f279-3274-8530-7070b308e08d"))) ? (((0) <= (this.maxLogs)) && ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d85832f4-b660-3f51-9d04-3dd9179798d0"))) ? (((0) <= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fad37f60-9af1-3155-aae6-5ea1945c58cf"))) ? (((logCount) >= (this.maxLogs)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ea03d1a0-2d39-323e-a80f-f3eb88d63538"))) ? (((0) != (this.maxLogs)) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e545355f-1220-321b-af4d-d63973df4820"))) ? (((logCount) != (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bfdb05bc-8f4d-3451-8f5b-96225ef1ba1f"))) ? (((0) < (this.maxLogs)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ede9ef90-5c17-3bfb-85b6-3ee5bd8ebf7d"))) ? (((logCount) < (0)) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f0af00a-6735-3a6d-9b60-b955fc2a5043"))) ? (((logCount) < (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("51bf591f-22d2-31a3-913f-7dcc9acc1f3e"))) ? (((logCount) > (0)) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7b5219af-3142-3f51-9b7d-1c11c0ddb24b"))) ? (((logCount) >= (0)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d1c97ad-cee5-36f5-96e7-11ac2cb35573"))) ? (((getNumRolledLogFiles()) < (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4c289ee2-7972-3732-9d01-3ec137f2d374"))) ? (((getNumRolledLogFiles()) >= (0)) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a402f751-8113-337e-a515-7cdc99355c74"))) ? (((0) > (getNumRolledLogFiles())) && ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("72443733-da97-3d78-a5e0-2a0e0201d325"))) ? (((logCount) > (0)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d03916c4-e618-3e61-ab11-165691e894eb"))) ? (((0) <= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("07e9e39c-14d2-3253-bba6-b8943f61901c"))) ? (((0) >= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4974aa47-356e-37ac-bdeb-55ec50d53f5b"))) ? (((0) <= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("99b7f4ca-0b1f-3097-af69-c7def46dc464"))) ? (((0) > (this.maxLogs)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8708bb1e-1e75-36af-9c40-4fa55cc92243"))) ? (((logCount) >= (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("78a6de22-3ac9-38c1-8c68-808a1f8438c6"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6eb0f6f1-3ce7-370d-85cf-3b61d1eb9ab1"))) ? (((getNumRolledLogFiles()) > (this.maxLogs)) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2fd22d1-b412-3432-814b-ddcc8f4e3661"))) ? (((0) >= (this.maxLogs)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("85a83b50-84bd-3259-a118-be67f1019430"))) ? (((0) < (getNumRolledLogFiles())) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("130e16a8-c904-3a75-bc80-9d17b9332356"))) ? (((getNumRolledLogFiles()) != (0)) && ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c30e38f0-b353-3cc1-8867-ed21452b6653"))) ? (((logCount) == (0)) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c10ce3ba-3868-3ac2-b329-636d70b4a0b1"))) ? (((getNumRolledLogFiles()) != (0)) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6a4a3d25-865e-3db9-a919-0bc43bbad51f"))) ? ((0) <= (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("7e2e0f62-6bc4-3c62-8ef1-09434d3daeaa"))) ? (((logCount) > (0)) && ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("46f4f4f3-5e19-3ab9-93f6-fa69df811a27"))) ? (((0) < (getNumRolledLogFiles())) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("23a4b11f-3704-3e4a-a338-e1939bd1c12b"))) ? ((logCount > this.maxLogs) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("12ce4f5b-45ed-3e5d-864d-b987f0b4cd88"))) ? (((getNumRolledLogFiles()) <= (this.maxLogs)) && ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f1b48bfa-cca3-3590-b4a4-9a97739b2e92"))) ? (((0) == (this.maxLogs)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2151258d-ed70-3736-9bbf-ebe55e4a0a27"))) ? (((getNumRolledLogFiles()) == (getNumRolledLogFiles())) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9c60dac4-8f04-3495-84bd-c1a40cbf2f96"))) ? (((logCount) == (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("22ab1170-4468-3bc0-a851-a734629d663f"))) ? (((logCount) == (this.maxLogs)) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1426c77e-f9f9-3447-8cf8-cff5576f3a85"))) ? (((0) != (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91600663-9940-3b49-aede-a1e9690942f4"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc790773-9686-3793-937d-e09a9ecc30b4"))) ? (((logCount) > (getNumRolledLogFiles())) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4f7d6ffc-8aa5-39c7-8e84-d206e199a3dc"))) ? ((0) != (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("125c1a11-d39a-3c6b-ab96-e9e0e9850d96"))) ? (((logCount) <= (0)) && ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9c468f9d-1881-316b-8de2-a08d1396ecb9"))) ? (((logCount) == (getNumRolledLogFiles())) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf120855-07b4-39df-b298-b5f683bd4dbe"))) ? (((logCount) != (0)) || ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b10ebf95-1720-34c2-982a-b85a250f1581"))) ? (((0) < (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e7c99ad3-e88d-3fc8-8b04-ee2e539500d9"))) ? (((0) <= (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f01ac400-10fb-329f-9db3-e8ea2e0c852a"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("beac79cc-da56-3b13-9c93-643ddf847e70"))) ? (((logCount) >= (this.maxLogs)) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("876d305a-4081-324b-8d08-2b0feedb4e50"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e80a226b-b3d3-3b8b-9319-81d434ef746a"))) ? (((logCount) < (0)) && (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("096fa89f-e730-3017-a9c0-edeb0392c003"))) ? (((0) != (getNumRolledLogFiles())) || ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38e3d1cb-6e14-37ed-a010-ec9c571122e3"))) ? (((logCount) >= (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d377e81f-4537-3aa6-8a64-7bd0dc3acd36"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) || ((logCount) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0905cb50-973e-3608-8502-255b751209a3"))) ? (((logCount) > (0)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7cf9eb2c-50df-3232-9ed5-ed178bbbb4f6"))) ? ((logCount) < (getNumRolledLogFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("90901033-e713-3441-91de-614cdc15e8c0"))) ? (((getNumRolledLogFiles()) >= (this.maxLogs)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4c1233da-a5a7-34c3-b927-b029a4bceabc"))) ? (((logCount) == (0)) && ((getNumRolledLogFiles()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("89156718-93cb-3375-ae27-7fdfbe0f5978"))) ? (((getNumRolledLogFiles()) != (getNumRolledLogFiles())) && ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("808989b1-3fa8-3157-8853-f731fc6016ec"))) ? (((getNumRolledLogFiles()) > (getNumRolledLogFiles())) || ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("69e81306-b367-3e3b-a4fc-3ec7a823855a"))) ? (((logCount) >= (this.maxLogs)) || ((logCount) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b33bdb7c-bc3a-3fc7-8be8-64411a30d541"))) ? (((0) > (this.maxLogs)) || ((getNumRolledLogFiles()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("875b4bdf-2022-3f8f-8b3b-ca422ab20ab5"))) ? (((logCount) == (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("496b7f1c-290f-3e13-8c17-21899d55b7d3"))) ? ((logCount > this.maxLogs) || ((logCount) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8abdabb-080d-3451-b065-ed1369751ee0"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) && ((logCount) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("903846b2-0e51-3bdd-ab77-35992ae77cc7"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) && ((getNumRolledLogFiles()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f4a19e06-c0fb-3e93-8db4-5ac84443c2f9"))) ? (logCount > 0) : (((KnobRuntime.check(java.util.UUID.fromString("888ff705-8571-3725-8e09-188b0896586c"))) ? (((0) <= (getNumRolledLogFiles())) || ((logCount) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f1b85a16-c27e-3407-8efe-5fb8f091fe86"))) ? (((getNumRolledLogFiles()) >= (0)) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("169a4f87-f285-355f-a79e-572ff0e1ac17"))) ? (((getNumRolledLogFiles()) < (getNumRolledLogFiles())) || ((getNumRolledLogFiles()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d48e9476-7ad6-328e-934a-4550910a6342"))) ? (((getNumRolledLogFiles()) != (this.maxLogs)) || ((getNumRolledLogFiles()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("148b4d98-8630-3a9c-8990-9cee44a87d5f"))) ? (((getNumRolledLogFiles()) > (this.maxLogs)) || (logCount > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("0836589c-fd79-3065-bcb0-a536340e2f41"))) ? (((logCount) > (getNumRolledLogFiles())) && ((logCount) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4814057e-af52-3916-8d8d-d012c34717d4"))) ? (((0) > (this.maxLogs)) && ((getNumRolledLogFiles()) > (0))) : (logCount > this.maxLogs && logCount > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      Map.Entry<Path, WALProps> firstWALEntry = this.walFile2Props.firstEntry();
      regions =
        this.sequenceIdAccounting.findLower(firstWALEntry.getValue().encodedName2HighestSequenceId);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("7402945a-8c2c-318a-8f75-5087ea49110b"))) ? ((regions) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("165ebf7d-2346-38ff-9114-332efd3e10f5"))) ? ((regions) == (null)) : (regions != null))))) {
      List<String> listForPrint = new ArrayList<>();
      for (Map.Entry<byte[], List<byte[]>> r : regions.entrySet()) {
        StringBuilder families = new StringBuilder();
        for (int i = 0; i < r.getValue().size(); i++) {
          if (((KnobRuntime.check(java.util.UUID.fromString("116e3021-15aa-3ed0-8b14-faedff802a22"))) ? ((getNumRolledLogFiles()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("56887cf5-6235-3eda-99eb-c90913aa2393"))) ? ((i) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c9c209d2-0e29-33b0-813e-c46132b0f65e"))) ? ((i++) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ddd6261d-880d-36c4-bcf5-5e3722e31250"))) ? ((i) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("75d2b3ae-a201-3916-93f4-dbbfbb9debeb"))) ? ((getNumRolledLogFiles()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f972875f-db43-33dd-aa04-bcc9159dc667"))) ? ((i++) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2af6e765-8d7c-3e3b-97cc-c59db3a36e5b"))) ? ((i++) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ba866a5c-9ac5-3c40-aa0a-1ae2f490178b"))) ? ((i) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6bf64f34-ab8d-37d2-ae8f-737c58a79f23"))) ? ((i++) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8de93225-b56f-36fd-a4f8-44ef696608a6"))) ? ((i) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d444ddd0-81ea-3166-8a1f-c8cf447b5fde"))) ? ((i++) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("af9120e1-2da6-3ddf-97d4-28f919436916"))) ? ((getNumRolledLogFiles()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b07459e6-ec0c-38bb-8510-d0ff35d7f902"))) ? ((i) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ece28425-7c5d-33b8-bd49-dfcbaed81536"))) ? ((i) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("26f8e332-f485-39d0-bd61-627bc72bc743"))) ? ((getNumRolledLogFiles()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("911eb545-0c59-36ca-abfe-d37316535dad"))) ? ((getNumRolledLogFiles()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("aaf53630-b5e9-31e8-a14f-edfeec36e201"))) ? ((getNumRolledLogFiles()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("94e9033d-cb44-30cd-8431-b22048961eca"))) ? ((i++) <= (0)) : (i > 0))))))))))))))))))))))))))))))))))))) {
            families.append(",");
          }
          families.append(Bytes.toString(r.getValue().get(i)));
        }
        if (KnobRuntime.check(java.util.UUID.fromString("b29e436c-504b-3355-aa1f-4c6b636cfa86"))) { listForPrint.add((Bytes.toStringBinary(r.getKey()) + "[" + families.toString()) + ("]")); } else if (KnobRuntime.check(java.util.UUID.fromString("50c7a8d7-d037-3a7a-9c0f-5d67fb3f0175"))) { listForPrint.add(((Bytes.toStringBinary(r.getKey()) + "[") + (families.toString())) + ("]")); } else { listForPrint.add(Bytes.toStringBinary(r.getKey()) + "[" + families.toString() + "]"); }
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
        if (((KnobRuntime.check(java.util.UUID.fromString("7ad3d49f-34d8-356a-8d9e-c79bc59fbf90"))) ? (LOG.isDebugEnabled()) : (LOG.isTraceEnabled()))) {
          LOG.trace("WAL file ready for archiving " + log);
        }
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("854db35f-b0c2-3240-9033-a16fe974580c"))) ? ((logsToArchive) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3985885-28e4-374f-9061-1738eb2ba12e"))) ? ((logsToArchive) == (null)) : (logsToArchive != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("198cde6b-eacf-36e0-b7e8-791e913503e6"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("c995d54e-6413-302e-b6eb-5cbe4c02eed7"))) {
retry += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("013d8eb5-a34b-384e-a7af-47f08a2c01aa"))) {
retry = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b148ac28-b7e8-3a43-b46b-9ee3def875cb"))) {
retry = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5233645d-0b2b-3095-bb76-4998dbda6952"))) {
retry -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a93f426f-4bf9-382a-aea0-ec2bee58f30e"))) {
retry *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("51b74fc2-80e7-3935-bead-81f26f9213ac"))) {
retry /= 2;
}
          if (KnobRuntime.check(java.util.UUID.fromString("d98c76d5-8b10-38bb-a6d3-034429722d69"))) { LOG.error("Log archiving failed for the log {} - attempt {}", log.getFirst(), 1, e); } else if (KnobRuntime.check(java.util.UUID.fromString("55b0779d-5aa1-3997-ad64-e005886c8af3"))) { LOG.error("Log archiving failed for the log {} - attempt {}", log.getFirst(), retry++, e); } else if (KnobRuntime.check(java.util.UUID.fromString("a7990a17-1e2b-30eb-a338-96b0e7f20e0f"))) { LOG.info("Log archiving failed for the log {} - attempt {}", log.getFirst(), 1, e); } else if (KnobRuntime.check(java.util.UUID.fromString("48e7a0e6-d259-3b6e-96ae-744d6b3fcd47"))) { LOG.info("Log archiving failed for the log {} - attempt {}", log.getFirst(), retry++, e); } else { LOG.error("Log archiving failed for the log {} - attempt {}", log.getFirst(), retry, e); }
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
      if (((KnobRuntime.check(java.util.UUID.fromString("2e7e2a79-346e-3314-b311-7f64fe55cd6a"))) ? ((syncFuture) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8a006c2e-4dc9-3a27-9bb2-aac77aaab16e"))) ? ((syncFuture) == (null)) : (syncFuture != null))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("14c4498e-6c8b-3c8e-8224-f29202446504"))) ? (!closed) : (closed))) {
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
        return ((KnobRuntime.check(java.util.UUID.fromString("6dde4e66-03ca-3a92-8f64-1e12d9003211"))) ? (findRegionsToForceFlush()) : (regionsToFlush));
      }
      try {
        Path oldPath = getOldPath();
        Path newPath = getNewPath();
        // Any exception from here on is catastrophic, non-recoverable, so we currently abort.
if(KnobRuntime.check(java.util.UUID.fromString("019d85d6-cb18-3ba9-8ca5-5ca715ef263d"))) {
throw new java.io.IOException("Injected exception");
}
        W nextWriter = this.createWriterInstance(newPath);
if(KnobRuntime.check(java.util.UUID.fromString("58e6b6ce-b437-3a82-b5fb-9d7834c5b770"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("03663a1a-142f-3c65-b458-7e82f6063bb9"))) { tellListenersAboutPreLogRoll(getOldPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("03abd8f8-c1c9-3f24-ae11-a6de6dfe7bbc"))) { tellListenersAboutPostLogRoll(oldPath, getNewPath()); } else if (KnobRuntime.check(java.util.UUID.fromString("9c260349-192f-32dc-9d7c-2a779fb6e789"))) { tellListenersAboutPostLogRoll(getOldPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("9b30987d-16bb-3ca8-98f4-5e9ea269e838"))) { tellListenersAboutPostLogRoll(getNewPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("4f49d987-a710-396e-b9d5-8aec2f634df1"))) { tellListenersAboutPreLogRoll(getNewPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("fff3bdd6-8f6b-33e1-8f8c-678ccd2ee4ce"))) { tellListenersAboutPreLogRoll(oldPath, getNewPath()); } else if (KnobRuntime.check(java.util.UUID.fromString("9abb58fa-ae1a-3a7b-bfba-b206b6e6a665"))) { tellListenersAboutPreLogRoll(oldPath, getOldPath()); } else if (KnobRuntime.check(java.util.UUID.fromString("77bf653c-3a6c-3328-b808-914b3bbfcacf"))) { tellListenersAboutPostLogRoll(oldPath, getOldPath()); } else { tellListenersAboutPreLogRoll(oldPath, newPath); }
        // NewPath could be equal to oldPath if replaceWriter fails.
if(KnobRuntime.check(java.util.UUID.fromString("599400ce-a433-3acd-9b50-ad4a738c9207"))) {
throw new java.io.IOException("Injected exception");
}
        newPath = replaceWriter(oldPath, newPath, nextWriter);
if(KnobRuntime.check(java.util.UUID.fromString("b21d16f6-f90c-36e6-8de1-3e7ab2ad3e60"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("8d4118a8-a08b-3081-871b-9b48e27e63f7"))) { tellListenersAboutPreLogRoll(getNewPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("3f92ec2b-7c80-3c1a-b726-21f8f4cd3002"))) { tellListenersAboutPostLogRoll(getOldPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("be7b1543-c1a5-3256-ba7e-7ec9994b1e9b"))) { tellListenersAboutPostLogRoll(oldPath, getNewPath()); } else if (KnobRuntime.check(java.util.UUID.fromString("f594b19e-0d5b-3312-9751-0716bdd654f0"))) { tellListenersAboutPostLogRoll(getNewPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("49ce01ca-561c-3978-9815-17fee2f652bf"))) { tellListenersAboutPreLogRoll(oldPath, getNewPath()); } else if (KnobRuntime.check(java.util.UUID.fromString("bb7b1253-5ce1-3b6f-b550-7599b27b9adc"))) { tellListenersAboutPreLogRoll(oldPath, getOldPath()); } else if (KnobRuntime.check(java.util.UUID.fromString("fb053e11-24fd-3f24-b9c2-4ab0162c7283"))) { tellListenersAboutPreLogRoll(getOldPath(), newPath); } else if (KnobRuntime.check(java.util.UUID.fromString("e985fa41-e0a7-3323-803d-ae90a7a9ef4c"))) { tellListenersAboutPostLogRoll(oldPath, getOldPath()); } else { tellListenersAboutPostLogRoll(oldPath, newPath); }
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
          if (KnobRuntime.check(java.util.UUID.fromString("9e012076-96b2-3a42-9d23-571e2a8bf611"))) { doShutdown(); } else { cleanOldLogs(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("5f14238c-98be-395c-9584-424d8449fa05"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("2c01c9f0-f6a0-3532-8a2b-d83038b384cb"))) {
throw new java.io.IOException("Injected exception");
}
    return CommonFSUtils.listStatus(fs, walDir, ourFiles);
  }

  @Override
  public void shutdown() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8ecfbb80-ab0b-3f4a-ae90-be9f14d2f50a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8b03faca-a974-3658-8e34-dc2e6f90ab54"))) {
return;
}
    if (!shutdown.compareAndSet(false, true)) {
      return;
    }
    closed = true;
    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        if (KnobRuntime.check(java.util.UUID.fromString("ab9ce834-4ab4-373c-9b57-c70e5a160d2d"))) { cleanOldLogs(); } else { i.logCloseRequested(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("5cb63169-c321-390b-9b60-f1560a6e9bce"))) {
throw new java.io.IOException("Injected exception");
}
            if (KnobRuntime.check(java.util.UUID.fromString("a0be0099-638c-3508-afc6-29b6d99e6982"))) { i.preLogRoll(getOldPath(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("e5d5d424-7538-381d-9bca-1e1a1980b4d4"))) { i.postLogRoll(getOldPath(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("3919e545-8a1e-35c1-bd6d-a801800ebef3"))) { i.preLogRoll(getNewPath(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("6d3be2f5-b242-3422-8663-4a109e5baeec"))) { i.preLogRoll(getCurrentFileName(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("6213aaf3-dee2-3e4d-9a9d-80aeae3ee475"))) { i.postLogRoll(getNewPath(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("37f2baee-d7a6-3225-b447-eb2f23ce4367"))) { i.preLogArchive(getCurrentFileName(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("38db5a45-03f8-3fcf-88f9-f338522024ba"))) { i.preLogArchive(getOldPath(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("3bf15f2a-5c08-3aca-9b27-486a02c880a8"))) { i.preLogArchive(getNewPath(), p); } else if (KnobRuntime.check(java.util.UUID.fromString("fdc276f8-18b5-31d6-bc1f-e2afe9f7a429"))) { i.postLogRoll(getCurrentFileName(), p); } else { i.preLogArchive(file.getPath(), p); }
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
if(KnobRuntime.check(java.util.UUID.fromString("9813312d-4dac-3cf2-b2af-8bcf0da78e18"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("4741fecc-fc11-3de0-8dd0-e5632c2a65c3"))) {
onlyIfGreater = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("43ba451f-88a5-35a8-9d79-e8eb24f9525b"))) {
onlyIfGreater = false;
}
    if (KnobRuntime.check(java.util.UUID.fromString("d1043ddd-d5d4-3372-b6c2-ce18cc92fe1f"))) { sequenceIdAccounting.updateStore(encodedRegionName, familyName, sequenceid, !onlyIfGreater); } else { sequenceIdAccounting.updateStore(encodedRegionName, familyName, sequenceid, onlyIfGreater); }
  }

  protected final SyncFuture getSyncFuture(long sequence, boolean forceSync) {
    return syncFutureCache.getIfPresentOrNew().reset(sequence, forceSync);
  }

  protected boolean isLogRollRequested() {
if(KnobRuntime.check(java.util.UUID.fromString("fb4654a0-c59d-3bc7-89ef-1126bef9c640"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("1e9d083f-d98b-3658-b250-afd7dce00256"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("622404cd-3b02-3281-ad11-7d6fbf8e6d65"))) ? (doCheckLogLowReplication()) : (rollRequested.get()));
  }

  protected final void requestLogRoll(final WALActionsListener.RollRequestReason reason) {
    // If we have already requested a roll, don't do it again
    // And only set rollRequested to true when there is a registered listener
    if (!this.listeners.isEmpty() && rollRequested.compareAndSet(false, true)) {
      for (WALActionsListener i : this.listeners) {
        if (KnobRuntime.check(java.util.UUID.fromString("7841a920-d30c-3c9f-86f5-d019249c8d09"))) { requestLogRoll(reason); } else { i.logRollRequested(reason); }
      }
    }
  }

  long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedTxid.get();
    long highestUnsynced = this.highestUnsyncedTxid;
    return highestSynced >= highestUnsynced ? 0 : highestUnsynced - highestSynced;
  }

  boolean isUnflushedEntries() {
if(KnobRuntime.check(java.util.UUID.fromString("52367ced-8177-391c-8d3e-a8701f9297c0"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("f84c6dfc-f155-38d2-b0cb-baf595c9032d"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e853ff5a-689a-358b-b830-022b1856edcb"))) ? ((EnvironmentEdgeManager.currentTime()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("336f2732-caf8-37c7-bf15-11da19c08440"))) ? ((getFilenum()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0d42ab57-5d1b-3d38-b0d3-cf15a61ad3f1"))) ? ((getUnflushedEntriesCount()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("25f403dc-958d-3e37-bab9-8aaf914434a6"))) ? ((EnvironmentEdgeManager.currentTime()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c26ca4bb-0357-3dc5-baab-5caeda36d173"))) ? ((EnvironmentEdgeManager.currentTime()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dcd44487-7500-3a26-ab81-5a201a0986ca"))) ? ((getUnflushedEntriesCount()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cee21078-cd47-3f4c-afdd-a4255603f016"))) ? ((EnvironmentEdgeManager.currentTime()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("60d26f94-0ae4-302a-9c94-b823bddf4e29"))) ? ((getFilenum()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("19a9b2d6-6a40-359e-b78d-6173a67fea11"))) ? ((getFilenum()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fd8da5c0-a2be-3608-9bae-daa30350adfb"))) ? ((getUnflushedEntriesCount()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4245642a-bd44-3377-a3c4-4b9e3bb1c15d"))) ? ((getUnflushedEntriesCount()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("34257838-3acd-398e-91a7-ff6244b83583"))) ? ((getUnflushedEntriesCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b276d4b8-7e83-31f4-9657-63fea9e2191e"))) ? ((EnvironmentEdgeManager.currentTime()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("da3e49ef-0fd6-3dea-bd0f-84618ad7dc85"))) ? ((getFilenum()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("66ff4c06-6c8c-39ad-85d4-8d817b00e54e"))) ? ((EnvironmentEdgeManager.currentTime()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a34f71f7-86f9-3479-9452-32a1d73885b1"))) ? ((getFilenum()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a8aecf27-eaea-3b2e-8549-dd9b5f6cc738"))) ? ((getUnflushedEntriesCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7108e310-df65-335a-9431-2250b7f1837c"))) ? ((getFilenum()) != (0)) : (getUnflushedEntriesCount() > 0))))))))))))))))))))))))))))))))))));
  }

  /**
   * Exposed for testing only. Use to tricks like halt the ring buffer appending.
   */
  protected void atHeadOfRingBufferEventHandlerAppend() {
    // Noop
  }

  protected final boolean appendEntry(W writer, FSWALEntry entry) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b8c4de19-7b8b-3e09-9ee3-71c506934165"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a6973296-71e8-3b94-98eb-f58a5bd13976"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9c3f2358-eb6e-3d88-81e6-92758995333a"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("inMemstore");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(entry);
    field.set(entry, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50d38c0c-ee61-3a3b-bfd9-c1ee6d57e8a5"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("a9c227dc-63a7-3ecc-8f48-edcbdc4b60f1"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("txid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entry));
    field.set(entry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
if(KnobRuntime.check(java.util.UUID.fromString("21e6f484-8aab-3d24-82d2-1d72e306b75f"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("53f9864a-55b1-3bf8-8207-2ea68602bc0a"))) { coprocessorHost.preWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit()); } else { coprocessorHost.postWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit()); }
    // Update metrics.
    postAppend(entry, EnvironmentEdgeManager.currentTime() - start);
    if (KnobRuntime.check(java.util.UUID.fromString("bd4a4da0-bd57-317a-a788-e8f8a9cdb2cf"))) { getNumRolledLogFiles(); } else { numEntries.incrementAndGet(); }
    return true;
  }

  private long postAppend(final Entry e, final long elapsedTime) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("41421b65-f260-3014-96c7-c591c5634850"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd5dcddd-99e4-3f33-b8b5-580aa780f6c9"))) {
throw new java.io.IOException("Injected exception");
}
    long len = 0;
    if (!listeners.isEmpty()) {
      for (Cell cell : e.getEdit().getCells()) {
        len += PrivateCellUtil.estimatedSerializedSizeOf(cell);
      }
      for (WALActionsListener listener : listeners) {
if(KnobRuntime.check(java.util.UUID.fromString("a188240f-f44b-3b3e-bfab-6a08607e8e15"))) {
len += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d63ab9b5-a5a1-38b4-b477-7f4abf1ed660"))) {
len = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1e9e29ac-be95-339b-8ee2-87807284db72"))) {
throw new java.io.IOException("Injected exception");
}
        listener.postAppend(len, elapsedTime, e.getKey(), e.getEdit());
      }
    }
    return len;
  }

  protected final void postSync(final long timeInNanos, final int handlerSyncs) {
if(KnobRuntime.check(java.util.UUID.fromString("b08fc5d8-5be3-3006-9abc-94bbc07dbf7c"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("0cce18bb-496c-3bdf-bfef-312a2b74a2a7"))) ? ((timeInNanos) == (this.slowSyncNs)) : (((KnobRuntime.check(java.util.UUID.fromString("050eeb9f-55a2-3f99-afbf-4c1d2c3525b2"))) ? ((timeInNanos) >= (this.slowSyncNs)) : (((KnobRuntime.check(java.util.UUID.fromString("83b68ea4-6697-3e89-8814-eea60cfa0da3"))) ? ((timeInNanos) > (this.slowSyncNs)) : (((KnobRuntime.check(java.util.UUID.fromString("c6eb5c1c-9363-36b3-9367-e0507339c7b6"))) ? ((timeInNanos) != (this.slowSyncNs)) : (((KnobRuntime.check(java.util.UUID.fromString("0d4982b5-fc7f-3154-8305-035dca1bda86"))) ? ((timeInNanos) < (this.slowSyncNs)) : (((KnobRuntime.check(java.util.UUID.fromString("e93a15cf-5b8e-3124-80ac-f2ef06a01607"))) ? ((timeInNanos) <= (this.slowSyncNs)) : (timeInNanos > this.slowSyncNs))))))))))))) {
      String msg = new StringBuilder().append("Slow sync cost: ")
        .append(TimeUnit.NANOSECONDS.toMillis(timeInNanos)).append(" ms, current pipeline: ")
        .append(Arrays.toString(getPipeline())).toString();
      if (KnobRuntime.check(java.util.UUID.fromString("d2afa81a-a666-3620-b0fa-430d7d7fa8e5"))) { LOG.trace(msg); } else { LOG.info(msg); }
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
if(KnobRuntime.check(java.util.UUID.fromString("661ce092-60f1-364b-9541-6a1741df6bf1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f59b1f6a-362c-394d-a8e4-53d29b524e11"))) {
try {
    java.lang.reflect.Field field = we.getClass().getDeclaredField("writeNumber");
    field.setAccessible(true);
    long oldValue = ((long)field.get(we));
    field.set(we, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c84b1af0-5956-3f43-b667-ae5ff6de8b5d"))) {
try {
    java.lang.reflect.Field field = we.getClass().getDeclaredField("completed");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(we);
    field.set(we, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
if(KnobRuntime.check(java.util.UUID.fromString("f11c1ae3-61cb-3122-bc68-0140447e392f"))) {
return null;
}
    if (KnobRuntime.check(java.util.UUID.fromString("29235061-0747-3ceb-be87-b9b69f9c0b99"))) { cleanOldLogs(); } else { rollWriterLock.lock(); }
    try {
      Path currentPath = getOldPath();
      if (path.equals(currentPath)) {
        // Currently active path.
        W writer = this.writer;
        return writer != null ? OptionalLong.of(writer.getSyncedLength()) : OptionalLong.empty();
      } else {
        W temp = inflightWALClosures.get(path.getName());
        if (((KnobRuntime.check(java.util.UUID.fromString("cc193adf-cf74-3bd7-82a7-d1eba2e557ba"))) ? ((temp) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("52dc4554-5cad-3bcf-a717-321c96e0b7c6"))) ? ((temp) == (null)) : (temp != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a8926342-b8fd-32be-964e-adc479d6ebbe"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("27ad3b9f-c19b-367d-9cdc-1aa7b587763c"))) {
return false;
}
    boolean result = false;
    long now = EnvironmentEdgeManager.currentTime();
    long elapsedTime = now - lastTimeCheckSlowSync;
    if (((KnobRuntime.check(java.util.UUID.fromString("19a55bb6-5df9-3ae7-9524-0366d727f012"))) ? ((elapsedTime) != (slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("4c8fdc38-924f-3377-89c4-59f84cd54afc"))) ? ((elapsedTime) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a568b4bb-a90e-399e-8891-ea3e44ac5b9d"))) ? ((elapsedTime) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("ff470938-c8dd-3980-a087-bf9f9b85c008"))) ? ((elapsedTime) >= (slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("1b9e59c3-c5d9-390e-b6da-e8f33e2e9feb"))) ? ((elapsedTime) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5285e776-9da0-34f6-ba29-0ef5b722af0c"))) ? ((elapsedTime) <= (slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("2527ace8-932c-3a7a-85e4-159ef64f1eb7"))) ? ((elapsedTime) == (2 * slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("77ede3e7-8d7a-3dfc-8de7-dbf3bba4eff6"))) ? ((elapsedTime) >= (2 * slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("0697a85f-7555-3c9e-9bda-20c3ec3b2275"))) ? ((elapsedTime) >= ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("53cef985-c91b-3d2f-829a-438be211e643"))) ? ((elapsedTime) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("f96feb8f-c489-3c5f-a848-d75b22e4dc1a"))) ? ((elapsedTime) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4eb3021a-418c-38e7-9619-7e5583a67af8"))) ? ((elapsedTime) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("30d140ae-2245-338e-9f52-cf383f5280bd"))) ? ((elapsedTime) != ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("660be8f9-d56c-3910-8f4e-8b346b14307c"))) ? ((elapsedTime) <= ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("674c87ec-39cc-37bf-aecf-28361ca2afc7"))) ? ((elapsedTime) != (2 * slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("72fedea8-85a8-3342-8238-b4586fa7bd59"))) ? ((elapsedTime) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7c956db4-4a56-3fe0-aee6-79b0cd066335"))) ? ((elapsedTime) > (slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("0d3042d6-3309-34e1-a332-46919183b79f"))) ? ((elapsedTime) <= (2 * slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("433e3074-5f81-3528-b406-0580c746a8b4"))) ? ((elapsedTime) > ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("5d717979-0721-3569-9bbc-04d47d2b3685"))) ? ((elapsedTime) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("a9079015-9092-3490-842d-ed477f92071f"))) ? ((elapsedTime) < (slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("54af9d6a-e06b-3b67-956c-a718ce81f69b"))) ? ((elapsedTime) < (2 * slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("5f2a9c70-68c1-3f1e-9dcf-930116a72ef5"))) ? ((elapsedTime) == (slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("8385d129-a24c-3ca6-ab83-2fecfc0087d5"))) ? ((elapsedTime) == ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("2c9c40f0-c139-3ffd-b2fa-580820091b15"))) ? ((elapsedTime) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6c4ad1ab-8698-3736-8a3a-29f89de1b0d5"))) ? ((elapsedTime) > (2 * slowSyncCheckInterval)) : (((KnobRuntime.check(java.util.UUID.fromString("c95ea257-fa35-37f5-abd4-342e5acefc97"))) ? ((elapsedTime) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("38a5c11d-03da-392d-8f1f-3bf3d789e9ab"))) ? ((elapsedTime) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("6d81a74a-a387-3c2e-8343-8c6afe2af818"))) ? ((elapsedTime) < ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("36d63a82-5f0e-3460-880e-9c66ef686dc7"))) ? ((elapsedTime) == (0)) : (elapsedTime >= slowSyncCheckInterval))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("d96b19d9-5bca-3f8a-ad12-0db5d5fb6d2a"))) ? ((slowSyncCount.get()) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("b109f065-a460-3ce5-86cd-fb74280c0fff"))) ? ((slowSyncCount.get()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9a3bffd4-cc62-3a55-bad6-0ebe1f3bf6f4"))) ? ((slowSyncCount.get()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("775b1cc5-da00-3312-9b36-92f05d3b612d"))) ? ((getNumRolledLogFiles()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6876c202-2651-3758-bab5-11ca376c0686"))) ? ((slowSyncCount.get()) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("f59ff988-c0ad-348d-903a-9bb33988451c"))) ? ((getNumRolledLogFiles()) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("7f5481d4-b3d8-3c2f-acdd-d1217bf1b24d"))) ? ((getNumRolledLogFiles()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bc468dfa-db85-3a1a-9e19-c5236b5c6bf6"))) ? ((getNumRolledLogFiles()) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("7fa42bd0-d3b7-3d7d-ba83-0ec1b9606c6a"))) ? ((getNumRolledLogFiles()) == (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("0acfc024-5493-311e-a0ef-81345d54b408"))) ? ((getNumRolledLogFiles()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("40f56c21-f8ff-34bc-81b7-15261cf99a75"))) ? ((slowSyncCount.get()) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("527ea4c7-43f1-3f15-8409-1767a24c0c4d"))) ? ((slowSyncCount.get()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dcdeb109-0e59-32c1-a3f9-df829f6021d2"))) ? ((getNumRolledLogFiles()) <= (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("139a4e9b-cba1-3a69-af7a-ad0fb3f891c9"))) ? ((getNumRolledLogFiles()) > (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("2447cec5-8f78-3460-ad28-e43d6ada6a24"))) ? ((slowSyncCount.get()) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("e7d77b1d-0dd1-37f1-a6d5-f9634abb1ab0"))) ? ((slowSyncCount.get()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f45efb0d-df88-37f9-9258-7fa79968bcd7"))) ? ((getNumRolledLogFiles()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("929912f3-3bda-3808-974f-ebc0ff1c7d1c"))) ? ((getNumRolledLogFiles()) < (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("314e91cb-2303-318a-9cea-09c0181ef9ac"))) ? ((getNumRolledLogFiles()) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("2f2bab3a-cdfb-3926-b6a1-a231cff15031"))) ? ((slowSyncCount.get()) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("06873f0c-0fb9-3a2e-b432-84136fb06178"))) ? ((getNumRolledLogFiles()) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("d7796772-5db5-3a50-90fc-2c11cda17380"))) ? ((slowSyncCount.get()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ecfb7a36-48f9-38e1-8f36-6918855ec943"))) ? ((slowSyncCount.get()) >= (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("2c54ca35-b8dd-3283-be6a-12f04dd83b04"))) ? ((slowSyncCount.get()) > (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("720da9a2-e6b4-352d-823e-8da6fcc626e3"))) ? ((getNumRolledLogFiles()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d8c81dcc-3c6b-3a35-9537-16bc134199cf"))) ? ((slowSyncCount.get()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f058d43f-b2b4-3ff3-98be-9e7cc9967969"))) ? ((getNumRolledLogFiles()) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("a891c717-79b4-31b2-be36-78f2b4146e60"))) ? ((getNumRolledLogFiles()) != (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("260ff32b-73d7-39cc-8b32-edae342c4b00"))) ? ((getNumRolledLogFiles()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f9cf325e-6112-35ab-a221-7178a634989c"))) ? ((getNumRolledLogFiles()) >= (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("643a2238-5246-35b2-922e-91539006b441"))) ? ((slowSyncCount.get()) < (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("c0c6c38e-030f-3bae-8dd0-0ff5686a0bf0"))) ? ((slowSyncCount.get()) != (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("bda0ad0a-9ecc-3f82-a435-b5b053c20ead"))) ? ((slowSyncCount.get()) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("9a648402-3c37-3172-9234-9d801e306913"))) ? ((slowSyncCount.get()) == (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("379bdeb9-1e63-32cd-aa85-47972ff39b17"))) ? ((slowSyncCount.get()) <= (slowSyncRollThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("2f5fa789-3cd3-3bf3-b909-f1ce89428026"))) ? ((getNumRolledLogFiles()) < (2)) : (slowSyncCount.get() >= slowSyncRollThreshold))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("d9a55c67-7c80-3576-b4a3-a1a61ef49bfc"))) ? ((elapsedTime) >= ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("47a2448b-b25f-3459-a3f9-da37a83034cf"))) ? ((elapsedTime) != ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("75b8a1d4-481b-336f-9b81-d9ea5c79a06e"))) ? ((elapsedTime) > ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("9d93a43a-f434-3bca-9e88-d850251d1959"))) ? ((elapsedTime) <= ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("de9950be-5867-3476-ad0b-e771db678b5b"))) ? ((elapsedTime) < ((2 * slowSyncCheckInterval))) : (((KnobRuntime.check(java.util.UUID.fromString("b2b1dfd3-aba1-317e-b3c9-067842e5143f"))) ? ((elapsedTime) == ((2 * slowSyncCheckInterval))) : (elapsedTime >= (2 * slowSyncCheckInterval)))))))))))))) {
          // If two or more slowSyncCheckInterval have elapsed this is a corner case
          // where a train of slow syncs almost triggered us but then there was a long
          // interval from then until the one more that pushed us over. If so, we
          // should do nothing and let the count reset.
          if (((KnobRuntime.check(java.util.UUID.fromString("093fd7b5-5cca-3e8b-a21c-85d2f81f80e1"))) ? (LOG.isTraceEnabled()) : (LOG.isDebugEnabled()))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("ec69ddaf-4e3e-3eed-bf71-10097e48298f"))) { System.exit(0); } else { slowSyncCount.set(0); }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("7c4a61de-d598-3444-939c-c65a6a2e48d4"))) ? (!result) : (((KnobRuntime.check(java.util.UUID.fromString("42506ff4-4397-32bc-8f08-cfa2b9fc65f2"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("f7c992c3-cc4d-37d0-8d94-53521aef30dd"))) ? (true) : (result))))));
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

if(KnobRuntime.check(java.util.UUID.fromString("b804b598-4de7-33c1-958c-e6c6316a3dc0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ffe2af95-b36c-3f21-b11e-eec5de4023f2"))) {
throw new java.io.IOException("Injected exception");
}
    final Path baseDir = CommonFSUtils.getWALRootDir(conf);
    Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (
      conf.getBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR,
        AbstractFSWALProvider.DEFAULT_SEPARATE_OLDLOGDIR)
    ) {
      archiveDir = new Path(archiveDir, p.getName());
    }
if(KnobRuntime.check(java.util.UUID.fromString("44cf946e-faae-3b67-b384-64e62a06bb42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cee22f0f-00a6-39ec-9da0-c198374c8fbd"))) {
throw new java.io.IOException("Injected exception");
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
if(KnobRuntime.check(java.util.UUID.fromString("af210183-c950-33c9-b41d-042edb1778b7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bab0c08c-0ffa-3c49-a702-10945e67bc12"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("20afebb7-73fc-3772-a768-524766423d2b"))) ? ((args.length) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("db616576-acbb-325d-82f8-fe6f6c2116d9"))) ? ((-1) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("0b143ccc-5b24-32a4-843d-f9fb6596dd45"))) ? ((args.length) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("c1e21de9-b135-3c97-88e3-2705fa28917a"))) ? ((-1) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("a847000f-e3ba-3964-8540-933f86b99f7c"))) ? ((args.length) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("8e2103d8-e857-3e55-8bd9-b825a2ad5f1c"))) ? ((args.length) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("383b2078-5285-3bd1-8d53-dca2ff0a42d9"))) ? ((-1) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("b6a77850-a776-3139-8135-7cea00b34288"))) ? ((args.length) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("4d0146d4-e745-3413-83e2-693d540bde64"))) ? ((-1) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("8257f103-c736-3a47-8815-d23cb2957a63"))) ? ((-1) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("fb9ac80c-8e1b-3dd0-b462-2945810d934c"))) ? ((-1) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("6841cf12-c4d1-3685-82a8-2f7ed701bc2c"))) ? ((args.length) <= (2)) : (args.length < 2))))))))))))))))))))))))) {
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
