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

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This Chore, every time it runs, will attempt to delete the WALs and Procedure WALs in the old
 * logs folder. The WAL is only deleted if none of the cleaner delegates says otherwise.
 * @see BaseLogCleanerDelegate
 */
@InterfaceAudience.Private
public class LogCleaner extends CleanerChore<BaseLogCleanerDelegate>
  implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

  public static final String OLD_WALS_CLEANER_THREAD_SIZE = "hbase.oldwals.cleaner.thread.size";
  public static final int DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE = 2;

  public static final String OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC =
    "hbase.oldwals.cleaner.thread.timeout.msec";
  static final long DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC = 60 * 1000L;

  private final LinkedBlockingQueue<CleanerContext> pendingDelete;
  private List<Thread> oldWALsCleaner;
  private long cleanerThreadTimeoutMsec;

  /**
   * @param period    the period of time to sleep between each run
   * @param stopper   the stopper
   * @param conf      configuration to use
   * @param fs        handle to the FS
   * @param oldLogDir the path to the archived logs
   * @param pool      the thread pool used to scan directories
   */
  public LogCleaner(final int period, final Stoppable stopper, Configuration conf, FileSystem fs,
    Path oldLogDir, DirScanPool pool, Map<String, Object> params) {
    super("LogsCleaner", period, stopper, conf, fs, oldLogDir, HBASE_MASTER_LOGCLEANER_PLUGINS,
      pool, params, null);
    this.pendingDelete = new LinkedBlockingQueue<>();
    int size = conf.getInt(OLD_WALS_CLEANER_THREAD_SIZE, DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE);
    this.oldWALsCleaner = createOldWalsCleaner(size);
    this.cleanerThreadTimeoutMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
      DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC);
  }

  @Override
  protected boolean validate(Path file) {
if(KnobRuntime.check(java.util.UUID.fromString("213d754c-01b2-3698-9721-14418ec3821d"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("43321a13-e73b-3270-a0df-8e557962db73"))) {
return true;
}
    return AbstractFSWALProvider.validateWALFilename(file.getName())
      || MasterProcedureUtil.validateProcedureWALFilename(file.getName())
      || file.getName().endsWith(MasterRegionFactory.ARCHIVED_WAL_SUFFIX);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    int newSize = conf.getInt(OLD_WALS_CLEANER_THREAD_SIZE, DEFAULT_OLD_WALS_CLEANER_THREAD_SIZE);
    if (newSize == oldWALsCleaner.size()) {
      LOG.debug(
        "Size from configuration is the same as previous which " + "is {}, no need to update.",
        newSize);
      return;
    }
    interruptOldWALsCleaner();
    oldWALsCleaner = createOldWalsCleaner(newSize);
    cleanerThreadTimeoutMsec = conf.getLong(OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC,
      DEFAULT_OLD_WALS_CLEANER_THREAD_TIMEOUT_MSEC);
  }

  @Override
  protected int deleteFiles(Iterable<FileStatus> filesToDelete) {
    List<CleanerContext> results = new ArrayList<>();
    for (FileStatus file : filesToDelete) {
      LOG.trace("Scheduling file {} for deletion", file);
      if (file != null) {
        results.add(new CleanerContext(file));
      }
    }
    if (results.isEmpty()) {
      return 0;
    }

    LOG.debug("Old WALs for delete: {}",
      results.stream().map(cc -> cc.target.getPath().getName()).collect(Collectors.joining(", ")));
    pendingDelete.addAll(results);

    int deletedFiles = 0;
    for (CleanerContext res : results) {
      LOG.trace("Awaiting the results for deletion of old WAL file: {}", res);
      deletedFiles += res.getResult(this.cleanerThreadTimeoutMsec) ? 1 : 0;
    }
    return deletedFiles;
  }

  @Override
  public synchronized void cleanup() {
    super.cleanup();
    interruptOldWALsCleaner();
  }

  int getSizeOfCleaners() {
    return oldWALsCleaner.size();
  }

  long getCleanerThreadTimeoutMsec() {
    return cleanerThreadTimeoutMsec;
  }

  private List<Thread> createOldWalsCleaner(int size) {
    LOG.info("Creating {} old WALs cleaner threads", size);

    List<Thread> oldWALsCleaner = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      Thread cleaner = new Thread(() -> deleteFile());
      cleaner.setName("OldWALsCleaner-" + i);
      cleaner.setDaemon(true);
      cleaner.start();
      oldWALsCleaner.add(cleaner);
    }
    return oldWALsCleaner;
  }

  private void interruptOldWALsCleaner() {
if(KnobRuntime.check(java.util.UUID.fromString("0f236f41-840c-383a-b583-adb3bddf5e68"))) {
return;
}
    for (Thread cleaner : oldWALsCleaner) {
      LOG.trace("Interrupting thread: {}", cleaner);
      if (KnobRuntime.check(java.util.UUID.fromString("6e5961b1-587a-3279-bd86-5bcf8db96013"))) { deleteFile(); } else if (KnobRuntime.check(java.util.UUID.fromString("b466ac4a-b924-3166-8c67-b9d99a56c4f1"))) { interruptOldWALsCleaner(); } else { cleaner.interrupt(); }
    }
    if (KnobRuntime.check(java.util.UUID.fromString("1745d830-9583-3a33-b99c-70f2a649fd53"))) { deleteFile(); } else if (KnobRuntime.check(java.util.UUID.fromString("e4e05585-b4c4-3830-bacb-349de3c762c5"))) { interruptOldWALsCleaner(); } else { oldWALsCleaner.clear(); }
  }

  private void deleteFile() {
if(KnobRuntime.check(java.util.UUID.fromString("dff3cfe1-d325-3c20-9195-e8cf4168e37f"))) {
return;
}
    while (true) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("494b5b20-b77c-33bf-a4c6-f9921053eaef"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        final CleanerContext context = pendingDelete.take();
        Preconditions.checkNotNull(context);
        FileStatus oldWalFile = context.getTargetToClean();
        try {
if(KnobRuntime.check(java.util.UUID.fromString("adab3aa3-be5e-3958-9c57-7682a26bb6cb"))) {
try {
    java.lang.reflect.Field field = oldWalFile.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(oldWalFile));
    field.set(oldWalFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0728a54-e976-3267-8ad4-5cb02e6ec210"))) {
try {
    java.lang.reflect.Field field = oldWalFile.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(oldWalFile));
    field.set(oldWalFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20d7b3b0-ca6c-3e1d-b602-c7b4b974d31d"))) {
try {
    java.lang.reflect.Field field = oldWalFile.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(oldWalFile));
    field.set(oldWalFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dec629ff-77b4-3f9b-b833-46efefae8ab1"))) {
try {
    java.lang.reflect.Field field = oldWalFile.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(oldWalFile));
    field.set(oldWalFile, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb0ec63d-c9ca-3b7c-aa4a-2f10b61683af"))) {
try {
    java.lang.reflect.Field field = oldWalFile.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(oldWalFile));
    field.set(oldWalFile, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa8c4a85-1d4e-3084-aab9-f4d243abf1b8"))) {
try {
    java.lang.reflect.Field field = oldWalFile.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(oldWalFile));
    field.set(oldWalFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("8cc45782-e91d-3791-99ac-f3c945397872"))) { LOG.trace("Deleting {}", oldWalFile); } else { LOG.debug("Deleting {}", oldWalFile); }
if(KnobRuntime.check(java.util.UUID.fromString("c0a1d853-148e-3fcf-8c98-3605bbb92cf7"))) {
throw new java.io.IOException("Injected exception");
}
          boolean succeed = this.fs.delete(oldWalFile.getPath(), false);
if(KnobRuntime.check(java.util.UUID.fromString("006fe8ee-76d0-30f6-a4f1-5160715764e9"))) {
succeed = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3bf9e09c-f122-36d3-853e-a8caca2c8b2b"))) {
succeed = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba5aa563-8d86-3e14-bd0b-55eafe4ad123"))) {
succeed = !succeed;
}
          if (KnobRuntime.check(java.util.UUID.fromString("51072085-d3db-31c6-aeb7-ecaef008e517"))) { context.setResult(false); } else if (KnobRuntime.check(java.util.UUID.fromString("dc8b00bd-8751-3234-ba2b-be085facf386"))) { context.setResult(true); } else if (KnobRuntime.check(java.util.UUID.fromString("1354796d-34c5-3916-b4b8-63665b73b91d"))) { context.setResult(!succeed); } else { context.setResult(succeed); }
        } catch (IOException e) {
          // fs.delete() fails.
          LOG.warn("Failed to delete old WAL file", e);
          context.setResult(false);
        }
      } catch (InterruptedException ite) {
        // It is most likely from configuration changing request
        LOG.warn(
          "Interrupted while cleaning old WALs, will " + "try to clean it next round. Exiting.");
        // Restore interrupt status
        Thread.currentThread().interrupt();
        return;
      }
      LOG.trace("Exiting");
    }
  }

  @Override
  public synchronized void cancel(boolean mayInterruptIfRunning) {
if(KnobRuntime.check(java.util.UUID.fromString("24bf50fa-527d-37bd-a007-ef9907e2d135"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("9f272b6a-cd12-3ac3-b146-cca76dc5c574"))) { LOG.warn("Cancelling LogCleaner"); } else if (KnobRuntime.check(java.util.UUID.fromString("dddfaaff-9e62-3664-bf70-71d22aaae4ad"))) { LOG.trace("Cancelling LogCleaner"); } else { LOG.debug("Cancelling LogCleaner"); }
    super.cancel(mayInterruptIfRunning);
    if (KnobRuntime.check(java.util.UUID.fromString("9e9c2c5e-f1e7-3f41-a287-a44ef02d6643"))) { deleteFile(); } else { interruptOldWALsCleaner(); }
  }

  private static final class CleanerContext {

    final FileStatus target;
    final AtomicBoolean result;
    final CountDownLatch remainingResults;

    private CleanerContext(FileStatus status) {
      this.target = status;
      this.result = new AtomicBoolean(false);
      this.remainingResults = new CountDownLatch(1);
    }

    void setResult(boolean res) {
      this.result.set(res);
      this.remainingResults.countDown();
    }

    boolean getResult(long waitIfNotFinished) {
      try {
        boolean completed = this.remainingResults.await(waitIfNotFinished, TimeUnit.MILLISECONDS);
        if (!completed) {
          LOG.warn("Spent too much time [{}ms] deleting old WAL file: {}", waitIfNotFinished,
            target);
          return false;
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while awaiting deletion of WAL file: {}", target);
        return false;
      }
      return result.get();
    }

    FileStatus getTargetToClean() {
      return target;
    }

    @Override
    public String toString() {
      return "CleanerContext [target=" + target + ", result=" + result + "]";
    }
  }
}
