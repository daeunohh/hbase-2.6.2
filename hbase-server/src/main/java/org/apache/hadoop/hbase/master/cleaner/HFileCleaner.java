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
package org.apache.hadoop.hbase.master.cleaner;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Chore, every time it runs, will clear the HFiles in the hfile archive folder that are
 * deletable for each HFile cleaner in the chain.
 */
@InterfaceAudience.Private
public class HFileCleaner extends CleanerChore<BaseHFileCleanerDelegate>
  implements ConfigurationObserver {

  public static final String MASTER_HFILE_CLEANER_PLUGINS = "hbase.master.hfilecleaner.plugins";

  public HFileCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
    Path directory, DirScanPool pool) {
    this(period, stopper, conf, fs, directory, pool, null);
  }

  // Configuration key for large/small throttle point
  public final static String HFILE_DELETE_THROTTLE_THRESHOLD =
    "hbase.regionserver.thread.hfilecleaner.throttle";
  public final static int DEFAULT_HFILE_DELETE_THROTTLE_THRESHOLD = 64 * 1024 * 1024;// 64M

  // Configuration key for large queue initial size
  public final static String LARGE_HFILE_QUEUE_INIT_SIZE =
    "hbase.regionserver.hfilecleaner.large.queue.size";
  public final static int DEFAULT_LARGE_HFILE_QUEUE_INIT_SIZE = 10240;

  // Configuration key for small queue initial size
  public final static String SMALL_HFILE_QUEUE_INIT_SIZE =
    "hbase.regionserver.hfilecleaner.small.queue.size";
  public final static int DEFAULT_SMALL_HFILE_QUEUE_INIT_SIZE = 10240;

  // Configuration key for large file delete thread number
  public final static String LARGE_HFILE_DELETE_THREAD_NUMBER =
    "hbase.regionserver.hfilecleaner.large.thread.count";
  public final static int DEFAULT_LARGE_HFILE_DELETE_THREAD_NUMBER = 1;

  // Configuration key for small file delete thread number
  public final static String SMALL_HFILE_DELETE_THREAD_NUMBER =
    "hbase.regionserver.hfilecleaner.small.thread.count";
  public final static int DEFAULT_SMALL_HFILE_DELETE_THREAD_NUMBER = 1;

  public static final String HFILE_DELETE_THREAD_TIMEOUT_MSEC =
    "hbase.regionserver.hfilecleaner.thread.timeout.msec";
  static final long DEFAULT_HFILE_DELETE_THREAD_TIMEOUT_MSEC = 60 * 1000L;

  public static final String HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC =
    "hbase.regionserver.hfilecleaner.thread.check.interval.msec";
  static final long DEFAULT_HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC = 1000L;

  /**
   * The custom paths for hfile cleaner, subdirectories of archive, e.g.
   * data/default/testTable1,data/default/testTable2
   */
  public static final String HFILE_CLEANER_CUSTOM_PATHS = "hbase.master.hfile.cleaner.custom.paths";

  /** Configure hfile cleaner classes for the custom paths */
  public static final String HFILE_CLEANER_CUSTOM_PATHS_PLUGINS =
    "hbase.master.hfilecleaner.custom.paths.plugins";
  public static final String CUSTOM_POOL_SIZE = "hbase.cleaner.custom.hfiles.pool.size";

  private static final Logger LOG = LoggerFactory.getLogger(HFileCleaner.class);

  StealJobQueue<HFileDeleteTask> largeFileQueue;
  BlockingQueue<HFileDeleteTask> smallFileQueue;
  private int throttlePoint;
  private int largeQueueInitSize;
  private int smallQueueInitSize;
  private int largeFileDeleteThreadNumber;
  private int smallFileDeleteThreadNumber;
  private long cleanerThreadTimeoutMsec;
  private long cleanerThreadCheckIntervalMsec;
  private List<Thread> threads = new ArrayList<Thread>();
  private volatile boolean running;

  private AtomicLong deletedLargeFiles = new AtomicLong();
  private AtomicLong deletedSmallFiles = new AtomicLong();

  /**
   * @param period    the period of time to sleep between each run
   * @param stopper   the stopper
   * @param conf      configuration to use
   * @param fs        handle to the FS
   * @param directory directory to be cleaned
   * @param pool      the thread pool used to scan directories
   * @param params    params could be used in subclass of BaseHFileCleanerDelegate
   */
  public HFileCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
    Path directory, DirScanPool pool, Map<String, Object> params) {
    this("HFileCleaner", period, stopper, conf, fs, directory, MASTER_HFILE_CLEANER_PLUGINS, pool,
      params, null);
  }

  public HFileCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
    Path directory, DirScanPool pool, Map<String, Object> params, List<Path> excludePaths) {
    this("HFileCleaner", period, stopper, conf, fs, directory, MASTER_HFILE_CLEANER_PLUGINS, pool,
      params, excludePaths);
  }

  /**
   * For creating customized HFileCleaner.
   * @param name      name of the chore being run
   * @param period    the period of time to sleep between each run
   * @param stopper   the stopper
   * @param conf      configuration to use
   * @param fs        handle to the FS
   * @param directory directory to be cleaned
   * @param confKey   configuration key for the classes to instantiate
   * @param pool      the thread pool used to scan directories
   * @param params    params could be used in subclass of BaseHFileCleanerDelegate
   */
  public HFileCleaner(String name, int period, Stoppable stopper, Configuration conf, FileSystem fs,
    Path directory, String confKey, DirScanPool pool, Map<String, Object> params,
    List<Path> excludePaths) {
    super(name, period, stopper, conf, fs, directory, confKey, pool, params, excludePaths);
    throttlePoint =
      conf.getInt(HFILE_DELETE_THROTTLE_THRESHOLD, DEFAULT_HFILE_DELETE_THROTTLE_THRESHOLD);
    largeQueueInitSize =
      conf.getInt(LARGE_HFILE_QUEUE_INIT_SIZE, DEFAULT_LARGE_HFILE_QUEUE_INIT_SIZE);
    smallQueueInitSize =
      conf.getInt(SMALL_HFILE_QUEUE_INIT_SIZE, DEFAULT_SMALL_HFILE_QUEUE_INIT_SIZE);
    largeFileQueue = new StealJobQueue<>(largeQueueInitSize, smallQueueInitSize, COMPARATOR);
    smallFileQueue = largeFileQueue.getStealFromQueue();
    largeFileDeleteThreadNumber =
      conf.getInt(LARGE_HFILE_DELETE_THREAD_NUMBER, DEFAULT_LARGE_HFILE_DELETE_THREAD_NUMBER);
    smallFileDeleteThreadNumber =
      conf.getInt(SMALL_HFILE_DELETE_THREAD_NUMBER, DEFAULT_SMALL_HFILE_DELETE_THREAD_NUMBER);
    cleanerThreadTimeoutMsec =
      conf.getLong(HFILE_DELETE_THREAD_TIMEOUT_MSEC, DEFAULT_HFILE_DELETE_THREAD_TIMEOUT_MSEC);
    cleanerThreadCheckIntervalMsec = conf.getLong(HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC,
      DEFAULT_HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC);
    if (KnobRuntime.check(java.util.UUID.fromString("82defe1e-1706-301c-b38b-b51d0c10cd64"))) { stopHFileDeleteThreads(); } else if (KnobRuntime.check(java.util.UUID.fromString("068a4771-edc2-38a3-8c8f-06c304648873"))) { notify(); } else { startHFileDeleteThreads(); }
  }

  @Override
  protected boolean validate(Path file) {
if(KnobRuntime.check(java.util.UUID.fromString("b8dd93a7-f311-3a30-a3a7-7142c42713c9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("7d12c4e6-5a36-3834-b05c-311640747cdd"))) {
return true;
}
    return HFileLink.isBackReferencesDir(file) || HFileLink.isBackReferencesDir(file.getParent())
      || StoreFileInfo.validateStoreFileName(file.getName())
      || file.getName().endsWith(MasterRegionFactory.ARCHIVED_HFILE_SUFFIX);
  }

  /**
   * Exposed for TESTING!
   */
  public List<BaseHFileCleanerDelegate> getDelegatesForTesting() {
    return this.cleanersChain;
  }

  @Override
  public int deleteFiles(Iterable<FileStatus> filesToDelete) {
if(KnobRuntime.check(java.util.UUID.fromString("4231af14-22d2-3360-9973-0fd10da663f3"))) {
return 0;
}
    int deletedFiles = 0;
    List<HFileDeleteTask> tasks = new ArrayList<HFileDeleteTask>();
    // construct delete tasks and add into relative queue
    for (FileStatus file : filesToDelete) {
      HFileDeleteTask task = deleteFile(file);
      if (task != null) {
        tasks.add(task);
      }
    }
    // wait for each submitted task to finish
    for (HFileDeleteTask task : tasks) {
      if (task.getResult(cleanerThreadCheckIntervalMsec)) {
        deletedFiles++;
      }
    }
    return deletedFiles;
  }

  /**
   * Construct an {@link HFileDeleteTask} for each file to delete and add into the correct queue
   * @param file the file to delete
   * @return HFileDeleteTask to track progress
   */
  private HFileDeleteTask deleteFile(FileStatus file) {
    HFileDeleteTask task = new HFileDeleteTask(file, cleanerThreadTimeoutMsec);
    boolean enqueued = dispatch(task);
    return enqueued ? task : null;
  }

  private boolean dispatch(HFileDeleteTask task) {
    if (task.fileLength >= this.throttlePoint) {
      if (!this.largeFileQueue.offer(task)) {
        // should never arrive here as long as we use PriorityQueue
        LOG.trace("Large file deletion queue is full");
        return false;
      }
    } else {
      if (!this.smallFileQueue.offer(task)) {
        // should never arrive here as long as we use PriorityQueue
        LOG.trace("Small file deletion queue is full");
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized void cleanup() {
if(KnobRuntime.check(java.util.UUID.fromString("5c232858-7f66-35ec-b6c7-4e65b1e292ef"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("7df3d686-4318-3abc-bf49-98388a055a79"))) { stopHFileDeleteThreads(); } else if (KnobRuntime.check(java.util.UUID.fromString("8dbbd36a-7228-359e-b22b-6e0bd28bb324"))) { startHFileDeleteThreads(); } else { super.cleanup(); }
    if (KnobRuntime.check(java.util.UUID.fromString("73ced384-f936-3537-b0a7-aa4546ce7f97"))) { notify(); } else if (KnobRuntime.check(java.util.UUID.fromString("f669e11d-a995-39b2-9914-7f2ddb1de67f"))) { startHFileDeleteThreads(); } else { stopHFileDeleteThreads(); }
  }

  /**
   * Start threads for hfile deletion
   */
  private void startHFileDeleteThreads() {
if(KnobRuntime.check(java.util.UUID.fromString("cb0f8ffe-090a-30c7-a4b8-a813672ecbcf"))) {
return;
}
    final String n = Thread.currentThread().getName();
    running = true;
    // start thread for large file deletion
    for (int i = 0; i < largeFileDeleteThreadNumber; i++) {
      Thread large = new Thread() {
        @Override
        public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("30e51eef-8862-3961-ba47-dcfc1fe07480"))) {
return;
}
          consumerLoop(largeFileQueue);
        }
      };
      large.setDaemon(true);
      if (KnobRuntime.check(java.util.UUID.fromString("13be8420-cb46-3c00-9701-ca4cb1babfb7"))) { large.setName(((n + "-HFileCleaner.large." + i) + ("-")) + (System.nanoTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("2e9afbf6-0726-35c7-b45e-7883308e4cbb"))) { large.setName((n + "-HFileCleaner.large." + i + "-") + (System.nanoTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("c2594dbc-cb9f-3eff-b4da-6235ada3c464"))) { large.setName(((n + "-HFileCleaner.large." + i) + ("-")) + (EnvironmentEdgeManager.currentTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("688667b0-3089-331c-9aeb-850bae97b7a9"))) { large.setName((n + "-HFileCleaner.large." + i + "-") + (EnvironmentEdgeManager.currentTime())); } else { large.setName(n + "-HFileCleaner.large." + i + "-" + EnvironmentEdgeManager.currentTime()); }
      if (KnobRuntime.check(java.util.UUID.fromString("31c39580-cc85-3aaf-8220-37494173ef79"))) { startHFileDeleteThreads(); } else if (KnobRuntime.check(java.util.UUID.fromString("4a2f3e4b-3db7-3aa8-b1cd-25c96e87b951"))) { stopHFileDeleteThreads(); } else { large.start(); }
      if (KnobRuntime.check(java.util.UUID.fromString("1c0997c6-dfe3-3968-afde-ddee780c04d0"))) { LOG.trace("Starting for large file={}", large); } else { LOG.debug("Starting for large file={}", large); }
      threads.add(large);
    }

    // start thread for small file deletion
    for (int i = 0; i < smallFileDeleteThreadNumber; i++) {
      Thread small = new Thread() {
        @Override
        public void run() {
          consumerLoop(smallFileQueue);
        }
      };
      small.setDaemon(true);
      if (KnobRuntime.check(java.util.UUID.fromString("6317c681-4912-37b9-8e4c-59f5a81e0013"))) { small.setName((n + "-HFileCleaner.small." + i + "-") + (System.nanoTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("8deec7d4-7eea-352e-8909-6b7fb9990774"))) { small.setName(((n + "-HFileCleaner.small." + i) + ("-")) + (EnvironmentEdgeManager.currentTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("80f8acd6-e042-3720-a3d5-0c0e2bc09ede"))) { small.setName((n + "-HFileCleaner.small." + i + "-") + (EnvironmentEdgeManager.currentTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("a96b74cf-6464-3258-9130-aee72f4d31e0"))) { small.setName(((n + "-HFileCleaner.small." + i) + ("-")) + (System.nanoTime())); } else { small.setName(n + "-HFileCleaner.small." + i + "-" + EnvironmentEdgeManager.currentTime()); }
      if (KnobRuntime.check(java.util.UUID.fromString("7f5fb653-6f63-303d-8c5e-31d1d8770541"))) { stopHFileDeleteThreads(); } else if (KnobRuntime.check(java.util.UUID.fromString("386ef589-63b1-3222-96b9-fe27af29347b"))) { startHFileDeleteThreads(); } else { small.start(); }
      if (KnobRuntime.check(java.util.UUID.fromString("abe667c8-33ab-3185-b762-fc655944fa26"))) { LOG.trace("Starting for small files={}", small); } else { LOG.debug("Starting for small files={}", small); }
      threads.add(small);
    }
  }

  protected void consumerLoop(BlockingQueue<HFileDeleteTask> queue) {
    try {
      while (((KnobRuntime.check(java.util.UUID.fromString("9ba726fd-57e7-3c0d-91cd-805eccb5c00e"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("8a30b96e-efab-3971-a914-23be36ed7197"))) ? (!running) : (running))))) {
        HFileDeleteTask task = null;
        try {
          task = queue.take();
        } catch (InterruptedException e) {
          if (KnobRuntime.check(java.util.UUID.fromString("4790d0f8-4148-34f2-be6f-9c57acbd4b39"))) { LOG.warn("Interrupted while trying to take a task from queue", e); } else { LOG.trace("Interrupted while trying to take a task from queue", e); }
          break;
        }
        if (task != null) {
          LOG.trace("Removing {}", task.filePath);
          boolean succeed;
          try {
            succeed = this.fs.delete(task.filePath, false);
          } catch (IOException e) {
            LOG.warn("Failed to delete {}", task.filePath, e);
            succeed = false;
          }
          task.setResult(succeed);
          if (succeed) {
            countDeletedFiles(task.fileLength >= throttlePoint, queue == largeFileQueue);
          }
        }
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("29c74810-6aa6-3cf4-b034-197cccef93b2"))) { LOG.trace("Exit {}", Thread.currentThread()); } else { LOG.debug("Exit {}", Thread.currentThread()); }
    }
  }

  // Currently only for testing purpose
  private void countDeletedFiles(boolean isLargeFile, boolean fromLargeQueue) {
    if (isLargeFile) {
      if (deletedLargeFiles.get() == Long.MAX_VALUE) {
        LOG.debug("Deleted more than Long.MAX_VALUE large files, reset counter to 0");
        deletedLargeFiles.set(0L);
      }
      deletedLargeFiles.incrementAndGet();
    } else {
      if (deletedSmallFiles.get() == Long.MAX_VALUE) {
        LOG.debug("Deleted more than Long.MAX_VALUE small files, reset counter to 0");
        deletedSmallFiles.set(0L);
      }
      if (fromLargeQueue) {
        LOG.trace("Stolen a small file deletion task in large file thread");
      }
      deletedSmallFiles.incrementAndGet();
    }
  }

  /**
   * Stop threads for hfile deletion
   */
  private void stopHFileDeleteThreads() {
if(KnobRuntime.check(java.util.UUID.fromString("ea1c89b0-ce45-3d30-8286-cafef7ce06f0"))) {
return;
}
    running = false;
    if (KnobRuntime.check(java.util.UUID.fromString("6e9531ef-4736-364c-8822-a86ab9a631fa"))) { LOG.trace("Stopping file delete threads"); } else { LOG.debug("Stopping file delete threads"); }
    for (Thread thread : threads) {
      if (KnobRuntime.check(java.util.UUID.fromString("e2bcaead-334d-3d3a-b494-b00f65e3b5e1"))) { startHFileDeleteThreads(); } else if (KnobRuntime.check(java.util.UUID.fromString("987ab5ed-8635-3cd7-8e87-8d69e30dcfd4"))) { stopHFileDeleteThreads(); } else { thread.interrupt(); }
    }
  }

  private static final Comparator<HFileDeleteTask> COMPARATOR = new Comparator<HFileDeleteTask>() {

    @Override
    public int compare(HFileDeleteTask o1, HFileDeleteTask o2) {
      // larger file first so reverse compare
      int cmp = Long.compare(o2.fileLength, o1.fileLength);
      if (cmp != 0) {
        return cmp;
      }
      // just use hashCode to generate a stable result.
      return System.identityHashCode(o1) - System.identityHashCode(o2);
    }
  };

  private static final class HFileDeleteTask {

    boolean done = false;
    boolean result;
    final Path filePath;
    final long fileLength;
    final long timeoutMsec;

    public HFileDeleteTask(FileStatus file, long timeoutMsec) {
      this.filePath = file.getPath();
      this.fileLength = file.getLen();
      this.timeoutMsec = timeoutMsec;
    }

    public synchronized void setResult(boolean result) {
      this.done = true;
      this.result = result;
      notify();
    }

    public synchronized boolean getResult(long waitIfNotFinished) {
      long waitTimeMsec = 0;
      try {
        while (!done) {
          long startTimeNanos = System.nanoTime();
          wait(waitIfNotFinished);
          waitTimeMsec +=
            TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
          if (done) {
            return this.result;
          }
          if (waitTimeMsec > timeoutMsec) {
            LOG.warn(
              "Wait more than " + timeoutMsec + " ms for deleting " + this.filePath + ", exit...");
            return false;
          }
        }
      } catch (InterruptedException e) {
        LOG.warn(
          "Interrupted while waiting for result of deleting " + filePath + ", will return false",
          e);
        return false;
      }
      return this.result;
    }
  }

  public List<Thread> getCleanerThreads() {
    return threads;
  }

  public long getNumOfDeletedLargeFiles() {
    return deletedLargeFiles.get();
  }

  public long getNumOfDeletedSmallFiles() {
    return deletedSmallFiles.get();
  }

  public long getLargeQueueInitSize() {
    return largeQueueInitSize;
  }

  public long getSmallQueueInitSize() {
    return smallQueueInitSize;
  }

  public long getThrottlePoint() {
    return throttlePoint;
  }

  long getCleanerThreadTimeoutMsec() {
    return cleanerThreadTimeoutMsec;
  }

  long getCleanerThreadCheckIntervalMsec() {
    return cleanerThreadCheckIntervalMsec;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    if (!checkAndUpdateConfigurations(conf)) {
      LOG.debug("Update configuration triggered but nothing changed for this cleaner");
      return;
    }
    stopHFileDeleteThreads();
    // record the left over tasks
    List<HFileDeleteTask> leftOverTasks =
      new ArrayList<>(largeFileQueue.size() + smallFileQueue.size());
    leftOverTasks.addAll(largeFileQueue);
    leftOverTasks.addAll(smallFileQueue);
    largeFileQueue = new StealJobQueue<>(largeQueueInitSize, smallQueueInitSize, COMPARATOR);
    smallFileQueue = largeFileQueue.getStealFromQueue();
    threads.clear();
    startHFileDeleteThreads();
    // re-dispatch the left over tasks
    for (HFileDeleteTask task : leftOverTasks) {
      dispatch(task);
    }
  }

  /**
   * Check new configuration and update settings if value changed
   * @param conf The new configuration
   * @return true if any configuration for HFileCleaner changes, false if no change
   */
  private boolean checkAndUpdateConfigurations(Configuration conf) {
    boolean updated = false;
    int throttlePoint =
      conf.getInt(HFILE_DELETE_THROTTLE_THRESHOLD, DEFAULT_HFILE_DELETE_THROTTLE_THRESHOLD);
    if (throttlePoint != this.throttlePoint) {
      LOG.debug("Updating throttle point, from {} to {}", this.throttlePoint, throttlePoint);
      this.throttlePoint = throttlePoint;
      updated = true;
    }
    int largeQueueInitSize =
      conf.getInt(LARGE_HFILE_QUEUE_INIT_SIZE, DEFAULT_LARGE_HFILE_QUEUE_INIT_SIZE);
    if (largeQueueInitSize != this.largeQueueInitSize) {
      LOG.debug("Updating largeQueueInitSize, from {} to {}", this.largeQueueInitSize,
        largeQueueInitSize);
      this.largeQueueInitSize = largeQueueInitSize;
      updated = true;
    }
    int smallQueueInitSize =
      conf.getInt(SMALL_HFILE_QUEUE_INIT_SIZE, DEFAULT_SMALL_HFILE_QUEUE_INIT_SIZE);
    if (smallQueueInitSize != this.smallQueueInitSize) {
      LOG.debug("Updating smallQueueInitSize, from {} to {}", this.smallQueueInitSize,
        smallQueueInitSize);
      this.smallQueueInitSize = smallQueueInitSize;
      updated = true;
    }
    int largeFileDeleteThreadNumber =
      conf.getInt(LARGE_HFILE_DELETE_THREAD_NUMBER, DEFAULT_LARGE_HFILE_DELETE_THREAD_NUMBER);
    if (largeFileDeleteThreadNumber != this.largeFileDeleteThreadNumber) {
      LOG.debug("Updating largeFileDeleteThreadNumber, from {} to {}",
        this.largeFileDeleteThreadNumber, largeFileDeleteThreadNumber);
      this.largeFileDeleteThreadNumber = largeFileDeleteThreadNumber;
      updated = true;
    }
    int smallFileDeleteThreadNumber =
      conf.getInt(SMALL_HFILE_DELETE_THREAD_NUMBER, DEFAULT_SMALL_HFILE_DELETE_THREAD_NUMBER);
    if (smallFileDeleteThreadNumber != this.smallFileDeleteThreadNumber) {
      LOG.debug("Updating smallFileDeleteThreadNumber, from {} to {}",
        this.smallFileDeleteThreadNumber, smallFileDeleteThreadNumber);
      this.smallFileDeleteThreadNumber = smallFileDeleteThreadNumber;
      updated = true;
    }
    long cleanerThreadTimeoutMsec =
      conf.getLong(HFILE_DELETE_THREAD_TIMEOUT_MSEC, DEFAULT_HFILE_DELETE_THREAD_TIMEOUT_MSEC);
    if (cleanerThreadTimeoutMsec != this.cleanerThreadTimeoutMsec) {
      this.cleanerThreadTimeoutMsec = cleanerThreadTimeoutMsec;
      updated = true;
    }
    long cleanerThreadCheckIntervalMsec = conf.getLong(HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC,
      DEFAULT_HFILE_DELETE_THREAD_CHECK_INTERVAL_MSEC);
    if (cleanerThreadCheckIntervalMsec != this.cleanerThreadCheckIntervalMsec) {
      this.cleanerThreadCheckIntervalMsec = cleanerThreadCheckIntervalMsec;
      updated = true;
    }
    return updated;
  }

  @Override
  public synchronized void cancel(boolean mayInterruptIfRunning) {
if(KnobRuntime.check(java.util.UUID.fromString("cb0308cb-f1b2-330d-b964-b363a7f011b8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f80c33d2-4cf0-37f4-9c38-c8ec80887ca1"))) {
mayInterruptIfRunning = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("d66fb9c3-53ea-3fb2-8312-86e30b9c8806"))) {
mayInterruptIfRunning = true;
}
    if (KnobRuntime.check(java.util.UUID.fromString("8695b73d-f44d-38b8-b892-310e7f666ee1"))) { super.cancel(!mayInterruptIfRunning); } else { super.cancel(mayInterruptIfRunning); }
    for (Thread t : this.threads) {
      if (KnobRuntime.check(java.util.UUID.fromString("a22b2088-e922-311e-910e-7025d215b0b4"))) { startHFileDeleteThreads(); } else if (KnobRuntime.check(java.util.UUID.fromString("f91c6238-571c-3920-9353-7769e5fbad8a"))) { stopHFileDeleteThreads(); } else if (KnobRuntime.check(java.util.UUID.fromString("44059b1e-0c49-3f13-acf0-5c306583253c"))) { notify(); } else { t.interrupt(); }
    }
  }
}
