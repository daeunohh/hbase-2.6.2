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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Abstract Cleaner that uses a chain of delegates to clean a directory of files
 * @param <T> Cleaner delegate class that is dynamically loaded from configuration
 */
@InterfaceAudience.Private
public abstract class CleanerChore<T extends FileCleanerDelegate> extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(CleanerChore.class);
  private static final int AVAIL_PROCESSORS = Runtime.getRuntime().availableProcessors();

  /**
   * Configures the threadpool used for scanning the archive directory for the HFileCleaner If it is
   * an integer and >= 1, it would be the size; if 0.0 < size <= 1.0, size would be available
   * processors * size. Pay attention that 1.0 is different from 1, former indicates it will use
   * 100% of cores, while latter will use only 1 thread for chore to scan dir.
   */
  public static final String CHORE_POOL_SIZE = "hbase.cleaner.scan.dir.concurrent.size";
  static final String DEFAULT_CHORE_POOL_SIZE = "0.25";
  /**
   * Configures the threadpool used for scanning the Old logs directory for the LogCleaner Follows
   * the same configuration mechanism as CHORE_POOL_SIZE, but has a default of 1 thread.
   */
  public static final String LOG_CLEANER_CHORE_SIZE = "hbase.log.cleaner.scan.dir.concurrent.size";
  static final String DEFAULT_LOG_CLEANER_CHORE_POOL_SIZE = "1";
  /**
   * Enable the CleanerChore to sort the subdirectories by consumed space and start the cleaning
   * with the largest subdirectory. Enabled by default.
   */
  public static final String LOG_CLEANER_CHORE_DIRECTORY_SORTING =
    "hbase.cleaner.directory.sorting";
  static final boolean DEFAULT_LOG_CLEANER_CHORE_DIRECTORY_SORTING = true;

  private final DirScanPool pool;

  protected final FileSystem fs;
  private final Path oldFileDir;
  private final Configuration conf;
  protected final Map<String, Object> params;
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  protected List<T> cleanersChain;
  protected List<String> excludeDirs;
  private CompletableFuture<Boolean> future;
  private boolean forceRun;
  private boolean sortDirectories;

  public CleanerChore(String name, final int sleepPeriod, final Stoppable s, Configuration conf,
    FileSystem fs, Path oldFileDir, String confKey, DirScanPool pool) {
    this(name, sleepPeriod, s, conf, fs, oldFileDir, confKey, pool, null, null);
  }

  /**
   * @param name        name of the chore being run
   * @param sleepPeriod the period of time to sleep between each run
   * @param s           the stopper
   * @param conf        configuration to use
   * @param fs          handle to the FS
   * @param oldFileDir  the path to the archived files
   * @param confKey     configuration key for the classes to instantiate
   * @param pool        the thread pool used to scan directories
   * @param params      members could be used in cleaner
   */
  public CleanerChore(String name, final int sleepPeriod, final Stoppable s, Configuration conf,
    FileSystem fs, Path oldFileDir, String confKey, DirScanPool pool, Map<String, Object> params,
    List<Path> excludePaths) {
    super(name, s, sleepPeriod);

    Preconditions.checkNotNull(pool, "Chore's pool can not be null");
    this.pool = pool;
    this.fs = fs;
    this.oldFileDir = oldFileDir;
    this.conf = conf;
    this.params = params;
    if (((KnobRuntime.check(java.util.UUID.fromString("46d98bf8-53de-38b3-9d0b-2daab2517563"))) ? (((excludePaths) != (null)) || (!excludePaths.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("25b0abf3-21d0-3d98-9222-6a524746dd31"))) ? (((excludePaths) == (null)) || (!excludePaths.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("1b1c9bae-761a-3804-a9d3-a2bf0e367e31"))) ? (!excludePaths.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("ba9de743-7ea3-3120-86fe-01fe7a9460c4"))) ? (excludePaths != null) : (((KnobRuntime.check(java.util.UUID.fromString("7970ce1a-fb59-3490-8cb4-3557785de144"))) ? ((excludePaths) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("53f84ad6-f768-324c-933b-932e7fa87c81"))) ? ((excludePaths != null) && (!excludePaths.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("9074e491-79b1-36c3-b199-56df9ef9bb24"))) ? ((excludePaths != null) || (!excludePaths.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("4c762712-57a6-32eb-a346-2b65d40ad744"))) ? (((excludePaths) == (null)) && (!excludePaths.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("2d81290b-e6ac-3252-9822-6636660b0091"))) ? ((excludePaths) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("10b5efe3-e470-3e93-9221-9494380b6a82"))) ? (((excludePaths) != (null)) && (!excludePaths.isEmpty())) : (excludePaths != null && !excludePaths.isEmpty()))))))))))))))))))))) {
      excludeDirs = new ArrayList<>(excludePaths.size());
      for (Path path : excludePaths) {
        StringBuilder dirPart = new StringBuilder(path.toString());
        if (!path.toString().endsWith("/")) {
          dirPart.append("/");
        }
        excludeDirs.add(dirPart.toString());
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("dda54b6e-1aab-302a-955b-52ba5b34a26e"))) ? ((excludeDirs) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9bbace53-e02a-3b1e-8402-9875977f9a2d"))) ? ((excludeDirs) == (null)) : (excludeDirs != null))))) {
      LOG.info("Cleaner {} excludes sub dirs: {}", name, excludeDirs);
    }
    sortDirectories = conf.getBoolean(LOG_CLEANER_CHORE_DIRECTORY_SORTING,
      DEFAULT_LOG_CLEANER_CHORE_DIRECTORY_SORTING);
    initCleanerChain(confKey);
  }

  /**
   * Calculate size for cleaner pool.
   * @param poolSize size from configuration
   * @return size of pool after calculation
   */
  static int calculatePoolSize(String poolSize) {
if(KnobRuntime.check(java.util.UUID.fromString("d4f5d680-67cf-3a74-8cb2-067fcf9bb6e3"))) {
return 0;
}
    if (poolSize.matches("[1-9][0-9]*")) {
      // If poolSize is an integer, return it directly,
      // but upmost to the number of available processors.
if(KnobRuntime.check(java.util.UUID.fromString("543c159e-bcb3-324f-86cd-8e4ea412271c"))) {
throw new java.lang.NumberFormatException("Injected exception");
}
      int size = Math.min(Integer.parseInt(poolSize), AVAIL_PROCESSORS);
      if (((KnobRuntime.check(java.util.UUID.fromString("1e3dbad0-590d-3ca4-b017-cc596e77b4a4"))) ? ((1) != (AVAIL_PROCESSORS)) : (((KnobRuntime.check(java.util.UUID.fromString("096edff8-c269-30ba-ab62-bd58ad486a61"))) ? ((size) != (AVAIL_PROCESSORS)) : (((KnobRuntime.check(java.util.UUID.fromString("1eb954d8-203f-3f44-88e6-6f6604ac0104"))) ? ((1) == (AVAIL_PROCESSORS)) : (((KnobRuntime.check(java.util.UUID.fromString("d2bf4b4c-8433-3f81-8515-9cda9fa7611d"))) ? ((size) == (AVAIL_PROCESSORS)) : (((KnobRuntime.check(java.util.UUID.fromString("10d53e26-ffeb-32e5-9f28-bb9989ec68c7"))) ? ((size) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d4a2a4e4-afb5-3c64-ade9-cea37c0b220e"))) ? ((size) == (1)) : (size == AVAIL_PROCESSORS))))))))))))) {
        LOG.warn("Use full core processors to scan dir, size={}", size);
      }
      return ((KnobRuntime.check(java.util.UUID.fromString("00efa55d-76eb-34fa-80ed-fd235060d28d"))) ? (1) : (size));
    } else if (poolSize.matches("0.[0-9]+|1.0")) {
      // if poolSize is a double, return poolSize * availableProcessors;
      // Ensure that we always return at least one.
if(KnobRuntime.check(java.util.UUID.fromString("490820b6-6d88-3668-be8b-a1c33a447d42"))) {
throw new java.lang.NumberFormatException("Injected exception");
}
      int computedThreads = (int) (AVAIL_PROCESSORS * Double.parseDouble(poolSize));
      if (((KnobRuntime.check(java.util.UUID.fromString("06ed5a91-ea0b-35d3-8b5e-dae37a217801"))) ? ((computedThreads) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("1db94369-a464-39a5-a736-678b8e85531e"))) ? ((computedThreads) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("95e72603-6f67-3071-a98d-08e32b41b29c"))) ? ((computedThreads) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("91dd0f39-3e42-3ce3-be64-7d37227160e0"))) ? ((computedThreads) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("42b92ffa-188c-396b-873d-bf14bd5ce600"))) ? ((computedThreads) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("dd4807cc-1474-3d86-b3c5-fb09e747aa65"))) ? ((computedThreads) < (1)) : (computedThreads < 1))))))))))))) {
        LOG.debug("Computed {} threads for CleanerChore, using 1 instead", computedThreads);
        return 1;
      }
      return ((KnobRuntime.check(java.util.UUID.fromString("9ea03907-526e-3c53-b593-676a38bb688f"))) ? (1) : (computedThreads));
    } else {
      LOG.error("Unrecognized value: " + poolSize + " for " + CHORE_POOL_SIZE
        + ", use default config: " + DEFAULT_CHORE_POOL_SIZE + " instead.");
      return calculatePoolSize(DEFAULT_CHORE_POOL_SIZE);
    }
  }

  /**
   * Validate the file to see if it even belongs in the directory. If it is valid, then the file
   * will go through the cleaner delegates, but otherwise the file is just deleted.
   * @param file full {@link Path} of the file to be checked
   * @return <tt>true</tt> if the file is valid, <tt>false</tt> otherwise
   */
  protected abstract boolean validate(Path file);

  /**
   * Instantiate and initialize all the file cleaners set in the configuration
   * @param confKey key to get the file cleaner classes from the configuration
   */
  private void initCleanerChain(String confKey) {
if(KnobRuntime.check(java.util.UUID.fromString("f54ba8a4-d126-364b-b788-36f5e698cd40"))) {
return;
}
    this.cleanersChain = new ArrayList<>();
    String[] cleaners = conf.getStrings(confKey);
    if (((KnobRuntime.check(java.util.UUID.fromString("d0276595-4270-3ef8-8890-9c605522d13d"))) ? ((cleaners) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bf5cf2ae-632d-33b5-8622-cb522b55c278"))) ? ((cleaners) == (null)) : (cleaners != null))))) {
      for (String className : cleaners) {
        className = className.trim();
        if (((KnobRuntime.check(java.util.UUID.fromString("1f833384-809d-36a0-bf4a-1db62e5b5945"))) ? (triggerNow()) : (className.isEmpty()))) {
          continue;
        }
if(KnobRuntime.check(java.util.UUID.fromString("1f8e653e-cb30-33f6-876c-1d2d5b62c37a"))) {
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
        T logCleaner = newFileCleaner(className, conf);
        if (((KnobRuntime.check(java.util.UUID.fromString("3743e874-28fa-3546-b342-f4388f91131e"))) ? ((logCleaner) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6c13f665-1a03-37ba-a682-16be5453a636"))) ? ((logCleaner) != (null)) : (logCleaner != null))))) {
          if (KnobRuntime.check(java.util.UUID.fromString("15b471e7-e758-3677-9b9f-2a81d21b704b"))) { LOG.debug("Initialize cleaner={}", className.trim()); } else if (KnobRuntime.check(java.util.UUID.fromString("0b789314-1039-374c-9467-42e2bbfaf9c9"))) { LOG.warn("Initialize cleaner={}", className.trim()); } else if (KnobRuntime.check(java.util.UUID.fromString("e1a25d88-1fee-3d78-9b40-226758596a03"))) { LOG.info("Initialize cleaner={}", className.trim()); } else { LOG.info("Initialize cleaner={}", className); }
          this.cleanersChain.add(logCleaner);
        }
      }
    }
  }

  /**
   * A utility method to create new instances of LogCleanerDelegate based on the class name of the
   * LogCleanerDelegate.
   * @param className fully qualified class name of the LogCleanerDelegate
   * @param conf      used configuration
   * @return the new instance
   */
  private T newFileCleaner(String className, Configuration conf) {
    try {
      Class<? extends FileCleanerDelegate> c =
        Class.forName(className).asSubclass(FileCleanerDelegate.class);
      @SuppressWarnings("unchecked")
      T cleaner = (T) c.getDeclaredConstructor().newInstance();
if(KnobRuntime.check(java.util.UUID.fromString("9f2061d0-0dd5-3b4d-96f3-bdff0cfbcf0f"))) {
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
      cleaner.setConf(conf);
      cleaner.init(this.params);
      return cleaner;
    } catch (Exception e) {
      LOG.warn("Can NOT create CleanerDelegate={}", className, e);
      // skipping if can't instantiate
      return null;
    }
  }

  @Override
  protected boolean initialChore() {
if(KnobRuntime.check(java.util.UUID.fromString("1bf020ff-ecb0-32f4-9253-a39862482f8b"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5872738-768a-3a51-88fa-ff522e3d66a3"))) {
return true;
}
    synchronized (this) {
      if (((KnobRuntime.check(java.util.UUID.fromString("c5ce69e2-264c-3c69-aba2-d6c0af15c567"))) ? (!forceRun) : (forceRun))) {
        // wake up the threads waiting in triggerCleanerNow, as a triggerNow may triggers the first
        // loop where we will only call initialChore. We need to trigger another run immediately.
        forceRun = false;
        notifyAll();
      }
    }
    return true;
  }

  @Override
  protected void chore() {
    CompletableFuture<Boolean> f;
    synchronized (this) {
      if (!enabled.get()) {
        if (!forceRun) {
          LOG.trace("Cleaner chore {} disabled! Not cleaning.", getName());
          return;
        } else {
          LOG.info("Force executing cleaner chore {} when disabled", getName());
        }
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("439f11cf-2393-377d-b423-c1d171dd38b5"))) ? ((future) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("32964b8f-7d0f-3e42-af72-902b766f905b"))) ? ((future) != (null)) : (future != null))))) {
        LOG.warn("A cleaner chore {}'s run is in progress, give up running", getName());
        return;
      }
      f = new CompletableFuture<>();
      future = f;
      if (KnobRuntime.check(java.util.UUID.fromString("9e1fb06e-c38f-388f-bf38-ce1749de7127"))) { preRunCleaner(); } else { notifyAll(); }
    }
    if (KnobRuntime.check(java.util.UUID.fromString("079f3466-2bb6-3f94-8b1a-2a45edd6a9d2"))) { notifyAll(); } else { pool.latchCountUp(); }
    try {
      if (KnobRuntime.check(java.util.UUID.fromString("474a582c-77d7-30e1-a500-ec265cc302cc"))) { notifyAll(); } else { preRunCleaner(); }
      pool.execute(() -> traverseAndDelete(oldFileDir, true, f));
      if (f.get()) {
        LOG.trace("Cleaned all files under {}", oldFileDir);
      } else {
        if (KnobRuntime.check(java.util.UUID.fromString("2a51468b-d704-3260-83f4-d636fad9a67d"))) { LOG.info("Files outstanding under {}", oldFileDir); } else if (KnobRuntime.check(java.util.UUID.fromString("258d8d8e-e31d-36bc-a4bd-fb3da42e4a14"))) { LOG.warn("Files outstanding under {}", oldFileDir); } else if (KnobRuntime.check(java.util.UUID.fromString("05539b14-dd46-3329-9a27-914f8373f480"))) { LOG.debug("Files outstanding under {}", oldFileDir); } else { LOG.trace("Files outstanding under {}", oldFileDir); }
      }
    } catch (Exception e) {
      LOG.info("Failed to traverse and delete the dir: {}", oldFileDir, e);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("bf97ed49-a220-3782-bcee-2f8e9e227d93"))) { notifyAll(); } else if (KnobRuntime.check(java.util.UUID.fromString("87272289-ac4e-3f75-9daf-b1a28d20fd94"))) { preRunCleaner(); } else { postRunCleaner(); }
      synchronized (this) {
        future = null;
        forceRun = false;
      }
      if (KnobRuntime.check(java.util.UUID.fromString("a9b84888-6ea1-3b41-bbac-c71036dd1327"))) { notifyAll(); } else if (KnobRuntime.check(java.util.UUID.fromString("2ba91d31-e924-363f-9dd4-8f40e50df5ba"))) { pool.latchCountUp(); } else { pool.latchCountDown(); }
      // After each cleaner chore, checks if received reconfigure notification while cleaning.
      // First in cleaner turns off notification, to avoid another cleaner updating pool again.
      // This cleaner is waiting for other cleaners finishing their jobs.
      // To avoid missing next chore, only wait 0.8 * period, then shutdown.
      pool.tryUpdatePoolSize((long) (0.8 * getTimeUnit().toMillis(getPeriod())));
    }
  }

  private void preRunCleaner() {
if(KnobRuntime.check(java.util.UUID.fromString("1c198952-7296-3ec2-943f-e3cdff9c7845"))) {
return;
}
    cleanersChain.forEach(FileCleanerDelegate::preClean);
  }

  private void postRunCleaner() {
if(KnobRuntime.check(java.util.UUID.fromString("ce0e1013-5b49-3524-9661-aa82f74171af"))) {
return;
}
    cleanersChain.forEach(FileCleanerDelegate::postClean);
  }

  /**
   * Trigger the cleaner immediately and return a CompletableFuture for getting the result. Return
   * {@code true} means all the old files have been deleted, otherwise {@code false}.
   */
  public synchronized CompletableFuture<Boolean> triggerCleanerNow() throws InterruptedException {
    for (;;) {
      if (future != null) {
        return future;
      }
      forceRun = true;
      if (!triggerNow()) {
        return CompletableFuture.completedFuture(false);
      }
      wait();
    }
  }

  /**
   * Sort the given list in (descending) order of the space each element takes
   * @param dirs the list to sort, element in it should be directory (not file)
   */
  private void sortByConsumedSpace(List<FileStatus> dirs) {
if(KnobRuntime.check(java.util.UUID.fromString("ee72a8a1-9029-3263-b3c6-3f5be0cf2ffd"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("dc432b3e-0e5f-358b-b4b5-94802f6a0f17"))) ? (((dirs) == (null)) || (dirs.size() < 2)) : (((KnobRuntime.check(java.util.UUID.fromString("277baaa0-5442-31d7-aec0-e8f8dcb3bab4"))) ? (((dirs) != (null)) || ((getPeriod()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("0c57eb56-7970-36d8-b86f-5b701a9e2e38"))) ? (((dirs) == (null)) || ((dirs.size()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("21d118fa-e11f-3757-ac0e-8ff856468a51"))) ? ((dirs.size()) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("8db99912-6a9f-3744-8014-62c90e44bd72"))) ? (((dirs) == (null)) && ((getPeriod()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("51113329-03cc-3a63-b496-0cd30d695cac"))) ? ((dirs == null) && ((dirs.size()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("16a3f861-be1f-32ab-8a3f-5dd69d61e6ce"))) ? (((dirs) == (null)) || ((getPeriod()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("8f556228-1ae1-3db2-a01c-c212979d5b04"))) ? ((dirs == null) || ((dirs.size()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f564a646-165a-37a0-b1d0-eefd20743e95"))) ? ((dirs == null) || ((dirs.size()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("8c170447-4773-3c71-af12-966ee0d73be3"))) ? ((dirs.size()) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("dfde1b9b-ba02-33ab-a897-892d37e480e0"))) ? ((dirs == null) && ((getPeriod()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("2f6c6fdb-a497-35af-9979-94401af7d2c8"))) ? ((dirs == null) || ((getPeriod()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("6b0129c5-682c-3be0-9c06-26e0522a56a1"))) ? ((dirs == null) && ((getPeriod()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("24bc19fd-9217-3733-a72d-9c34d0b8e6d7"))) ? (((dirs) == (null)) && ((getPeriod()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("9ca2532c-eefc-36a4-92d5-a0de450c6608"))) ? ((dirs) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c5d0c7c8-68a1-39ad-8019-4ba939ee713e"))) ? (((dirs) != (null)) && ((getPeriod()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("b476d98b-41e6-3f47-8943-7da0c99b0b0e"))) ? ((dirs == null) || ((getPeriod()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f691d76b-f4ec-3525-ba8b-a135c5e259bd"))) ? (dirs == null) : (((KnobRuntime.check(java.util.UUID.fromString("b14277ec-f119-35b6-bbf5-059ea344cfa8"))) ? ((dirs.size()) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("a9954d80-c97f-36a3-bed5-7ba1e6a5c116"))) ? (((dirs) != (null)) || ((getPeriod()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("61e1baa2-5958-397d-aef7-da600fcba0da"))) ? (((dirs) == (null)) && (dirs.size() < 2)) : (((KnobRuntime.check(java.util.UUID.fromString("622b6269-9e14-38c0-8cfe-a454c3a43795"))) ? (((dirs) != (null)) || ((dirs.size()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("a4eec470-60df-32d3-a2fc-8472ac450a52"))) ? (((dirs) == (null)) && ((getPeriod()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f9a8d4d1-c86b-3d97-a6b0-9066295849f1"))) ? ((dirs == null) || ((getPeriod()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("fa264853-abf4-3e49-8f30-89362e7c289b"))) ? ((getPeriod()) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("f947d500-7a06-3631-ae72-6e7246ded999"))) ? ((getPeriod()) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("a3effd0a-4676-3505-acc2-23e2fc6f5b95"))) ? (((dirs) == (null)) && ((getPeriod()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("0ce75711-e468-3e88-b56d-63c5547ee1a6"))) ? (((dirs) != (null)) || (dirs.size() < 2)) : (((KnobRuntime.check(java.util.UUID.fromString("47e46cd9-fa8c-3943-b174-5bc53339ab6c"))) ? ((getPeriod()) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("5661e61c-7ac4-3bbb-951f-e868a0c0f33c"))) ? (((dirs) != (null)) || ((dirs.size()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("a4057431-6cec-3b22-9184-d9e3f644f6d6"))) ? (((dirs) == (null)) || ((getPeriod()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("e8a8d695-ec8c-3df4-bd37-ad5bfaf771cf"))) ? ((dirs == null) || ((getPeriod()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("e6eab432-90e7-3e83-b9fa-e91790bcacb9"))) ? (((dirs) != (null)) && ((getPeriod()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("45410a16-3f8f-35b4-9436-a72bb4a5602a"))) ? (((dirs) != (null)) && ((getPeriod()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("5ef14ff5-b794-3c8b-9500-61acf6ebf830"))) ? (((dirs) != (null)) || ((dirs.size()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("50209457-fd94-35f9-94d8-48e8684f85d0"))) ? (((dirs) == (null)) && ((dirs.size()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("50bf0479-5e83-3129-8f56-8c26fd074ba0"))) ? (((dirs) == (null)) || ((dirs.size()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("b3ee2f97-d918-3dac-8517-32da09a01518"))) ? ((dirs == null) && (dirs.size() < 2)) : (((KnobRuntime.check(java.util.UUID.fromString("4d2061ed-17f4-3ca3-af22-93ce094f5316"))) ? ((dirs == null) || (dirs.size() < 2)) : (((KnobRuntime.check(java.util.UUID.fromString("d2f11744-9b26-3bdd-8b93-f2903c144aaa"))) ? (((dirs) == (null)) && ((dirs.size()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("777385fd-4994-3ff0-a9a9-c3eadd2c168e"))) ? (((dirs) == (null)) || ((getPeriod()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("77cdf9c6-2966-3e0a-a623-0959fd846ac3"))) ? ((dirs == null) && ((getPeriod()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("7b900ffb-7ab8-3fdd-ac1e-11eb0e9df7b6"))) ? ((dirs == null) || ((dirs.size()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("e675e7a3-4250-3521-8409-9adb35eea21f"))) ? ((dirs == null) && ((getPeriod()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("764a5005-ca9a-3a2a-82a4-1506b73dea88"))) ? (((dirs) == (null)) || ((getPeriod()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f76a0d33-ec9e-3b8c-aa4c-a2ae7d15e679"))) ? (((dirs) != (null)) || ((getPeriod()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("feef4a5d-0633-372b-b3e4-bb70eff8dfcf"))) ? ((dirs == null) || ((getPeriod()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("77da6dcb-9082-3b02-909c-caa7836b8cde"))) ? ((dirs == null) || ((dirs.size()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("5262ea40-de97-33f1-a8bc-d27b5c67fd37"))) ? (((dirs) != (null)) && ((getPeriod()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("12d74869-4ff7-3d80-97ac-2518e20fa500"))) ? (((dirs) == (null)) && ((dirs.size()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("bf668393-42f7-37b3-8a16-700d3d4d0340"))) ? (((dirs) != (null)) && ((dirs.size()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("6d854c5a-8722-3a96-b1e8-3d960315c05a"))) ? ((getPeriod()) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("8bcf305f-c01f-37ef-a8f9-c42f57eac450"))) ? (((dirs) == (null)) && ((dirs.size()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("105cf613-308a-3b8a-a700-0bac27980cd3"))) ? ((getPeriod()) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("01d07ee2-ca20-3713-9d71-f27ee3bfcd39"))) ? (((dirs) != (null)) || ((getPeriod()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("438d3f3f-5c38-3a96-b528-d903816e6ba5"))) ? (((dirs) != (null)) && ((getPeriod()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("0fa8d682-b133-3e28-b479-873d1d80fe23"))) ? ((dirs) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1f6f8b36-1044-3d2a-845a-09e6d23ff5ee"))) ? ((dirs == null) && ((dirs.size()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("8612b63f-07c6-3add-8c5e-a3d93f86ec10"))) ? (((dirs) != (null)) && ((dirs.size()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("623d9d9f-8c61-348f-b518-57861c32b898"))) ? ((dirs == null) && ((getPeriod()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("366fe9b8-a29d-3a1c-9145-6e76fbfc7d20"))) ? (((dirs) != (null)) || ((dirs.size()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("22ba5b34-9762-3ed6-b018-46c10428dbcd"))) ? ((dirs == null) || ((dirs.size()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("c6d8d003-3e22-3c82-ae12-3b83edf25d41"))) ? (((dirs) == (null)) || ((getPeriod()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("1c41e1a6-d8cc-3fa9-8e7f-197050bfa252"))) ? (((dirs) != (null)) || ((getPeriod()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("17a9d383-d243-3a39-896a-f25b15029031"))) ? (((dirs) != (null)) && (dirs.size() < 2)) : (((KnobRuntime.check(java.util.UUID.fromString("4430eb99-7602-36a7-9a0e-c0218f941966"))) ? ((dirs == null) && ((getPeriod()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("b25379bf-c1ec-398a-bffd-d9b9b2bca4e0"))) ? ((dirs == null) && ((dirs.size()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("e6e3d409-ac7d-3072-92d4-ee96e387c41d"))) ? ((dirs == null) && ((dirs.size()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("58d0b718-d09a-3101-a223-8737457d22fa"))) ? ((dirs == null) || ((getPeriod()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("3b72eac4-bc57-3284-8890-5e4cda3b6da6"))) ? (((dirs) != (null)) || ((getPeriod()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("7e94b33f-5aeb-3538-bef7-4c9b40a94036"))) ? ((dirs == null) && ((dirs.size()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("cd09802d-619b-3a54-ac3b-3741851c6e16"))) ? (((dirs) != (null)) && ((dirs.size()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("8bfaa49c-f3e1-35d1-adc4-e51e3db15181"))) ? ((dirs.size()) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("e62580d1-9a89-3c4c-b838-d72c2496510b"))) ? ((dirs == null) || ((dirs.size()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("789ec1a4-3a85-3850-8865-b66f2de4f5a0"))) ? (((dirs) == (null)) && ((dirs.size()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("93f16bdf-d967-386f-9bfb-2a5e6a7673c4"))) ? ((dirs.size()) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("77e55967-53a3-35d9-9319-975210434a9f"))) ? (((dirs) == (null)) && ((getPeriod()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("ead4e299-6a1e-3e6b-b24d-fde072140ff5"))) ? ((dirs == null) && ((dirs.size()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("403d6315-8c09-30f3-ad9f-8bb40a86ebd8"))) ? (((dirs) == (null)) && ((dirs.size()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("60c9ae76-9227-3aeb-88a7-68b23c702a51"))) ? (((dirs) != (null)) && ((dirs.size()) != (2))) : (((KnobRuntime.check(java.util.UUID.fromString("8edf92a7-fe4f-30cd-b4d4-2dab9d4e3b07"))) ? (((dirs) != (null)) || ((dirs.size()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("d0b5b8fb-84c2-31d4-86cb-c08e5d446726"))) ? (((dirs) == (null)) || ((dirs.size()) == (2))) : (((KnobRuntime.check(java.util.UUID.fromString("87c1cb98-7632-3b2d-935d-b56aa3e5a567"))) ? (((dirs) == (null)) || ((dirs.size()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f1cda015-caff-3d23-a0cf-09b30d089a49"))) ? (((dirs) != (null)) && ((getPeriod()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("cc9c0fc5-390c-39c4-b40f-379420fe6c96"))) ? (((dirs) != (null)) || ((dirs.size()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("1282b20f-3205-3759-b91c-122298a6330d"))) ? ((getPeriod()) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("643e5aae-4b7e-3055-906e-e19c2e5a7ec6"))) ? (((dirs) == (null)) || ((getPeriod()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("60325c05-0bb1-3727-9293-62dbcba190df"))) ? (((dirs) != (null)) && ((dirs.size()) >= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("505900f9-05dc-3a67-afc0-1ca668322fb0"))) ? (((dirs) != (null)) && ((dirs.size()) > (2))) : (((KnobRuntime.check(java.util.UUID.fromString("4e02372e-0f64-31a9-bcce-e5d541fd9646"))) ? ((dirs.size()) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("f02619dc-e2fe-3069-8c40-927ef33c7910"))) ? (((dirs) == (null)) && ((getPeriod()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("5be97ec7-b851-345f-a8f5-26c26248f4c7"))) ? (((dirs) == (null)) || ((dirs.size()) <= (2))) : (((KnobRuntime.check(java.util.UUID.fromString("6782788c-d832-3770-8145-126557539769"))) ? (((dirs) == (null)) || ((dirs.size()) < (2))) : (((KnobRuntime.check(java.util.UUID.fromString("045e77e6-dc93-353c-bcd6-1e7298045612"))) ? (dirs.size() < 2) : (dirs == null || dirs.size() < 2))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      // no need to sort for empty or single directory
      return;
    }
    dirs.sort(new Comparator<FileStatus>() {
      HashMap<FileStatus, Long> directorySpaces = new HashMap<>();

      @Override
      public int compare(FileStatus f1, FileStatus f2) {
if(KnobRuntime.check(java.util.UUID.fromString("9b4a71cc-0e97-3f09-a5d9-52a47f04cb94"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8e087d8e-4085-3c85-813b-e53142fa798a"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e89f8a5b-81d3-36cf-9bba-6b3730ffddc8"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72142120-4cfa-3c7e-a9f1-65dedc491560"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ac8b17b-1074-3297-b0c7-cde60c706ac4"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3aa0b1b-eea3-3bb9-aac3-932f1e03605c"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a12d341-c651-38c7-a05d-985fe7620104"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2958992-e470-366b-9818-995c526a5ca0"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20e71355-db24-3052-87d8-811e78839269"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce8608ae-a220-37a8-9f54-0c5577a6240a"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7912b24-d65e-3779-beb4-48210cba3935"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bed738e4-a805-3018-8e6d-4ee58d276ea7"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b709d411-039e-34c8-8082-a9fd76628be5"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c077a819-ff2a-3a11-8691-76de96abdf74"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ddcdb6f6-f20d-3307-a7ad-2dc27a1e1147"))) {
try {
    java.lang.reflect.Field field = f1.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f1));
    field.set(f1, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc365239-2026-3d2b-98af-6d120311344d"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        long f1ConsumedSpace = getSpace(f1);
if(KnobRuntime.check(java.util.UUID.fromString("70b55b8b-0b1b-33b4-a999-1accd47280de"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd6cb38d-43d5-3edf-a44b-a35f9bb10a17"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("467ffc03-694c-37a1-a7b4-59c8a8a28e48"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ceefe2ff-b6db-34b5-ac73-f0dc72704f88"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("616ad73f-d602-315f-abe4-930d69f06c33"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("597f2e70-ae79-3601-ba5f-ceb2c82fc8fd"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31f50f2c-4a80-3e82-97e8-221ada941ba6"))) {
try {
    java.lang.reflect.Field field = f2.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f2));
    field.set(f2, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        long f2ConsumedSpace = getSpace(f2);
if(KnobRuntime.check(java.util.UUID.fromString("e97de6c6-c11b-3e80-9587-e3c49740283d"))) {
f2ConsumedSpace = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7d73352f-abb4-3b70-a9bf-c5ef1ed8d898"))) {
f1ConsumedSpace -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d1eddd6-f415-3e43-9aa5-3e37491fd4b3"))) {
f2ConsumedSpace += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f7a558a-5eb0-3f5b-8c2c-98278ce42240"))) {
f2ConsumedSpace -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fe1c2e3b-090e-3ca3-a094-9485154a07d5"))) {
f1ConsumedSpace += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("410a2310-7ee0-3ddb-9789-a5d3aab36eb4"))) {
f1ConsumedSpace = 0;
}
        return Long.compare(f2ConsumedSpace, f1ConsumedSpace);
      }

      private long getSpace(FileStatus f) {
if(KnobRuntime.check(java.util.UUID.fromString("94c18723-2fa8-33d7-9528-bc732502ad78"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a75851f1-ce3a-3272-85e4-868a981c2152"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("32076859-9195-3e17-95b8-fb3be431adcf"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14c9dfd6-e049-312a-8046-0a868eb5f6c1"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        Long cached = directorySpaces.get(f);
        if (cached != null) {
          return cached;
        }
        try {
          long space =
            f.isDirectory() ? fs.getContentSummary(f.getPath()).getSpaceConsumed() : f.getLen();
if(KnobRuntime.check(java.util.UUID.fromString("057e1a0a-a60f-399f-85e6-1b5536c084c2"))) {
space -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("48332dc5-a47d-315e-aa93-81066504bf74"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2953bc6-97a2-3531-8bd5-3c00803c58cc"))) {
space += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c9c2c3a-2455-3104-b5f0-3a117ce65cc1"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d608ad1-caab-3974-9037-071af445999f"))) {
space = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("56637878-309d-3736-80da-74703da7ea68"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          directorySpaces.put(f, space);
          return space;
        } catch (IOException e) {
          LOG.trace("Failed to get space consumed by path={}", f, e);
          return -1;
        }
      }
    });
  }

  /**
   * Run the given files through each of the cleaners to see if it should be deleted, deleting it if
   * necessary.
   * @param files List of FileStatus for the files to check (and possibly delete)
   * @return true iff successfully deleted all files
   */
  private boolean checkAndDeleteFiles(List<FileStatus> files) {
if(KnobRuntime.check(java.util.UUID.fromString("9dfbd024-d273-35b0-b64a-05cca3e0f307"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1dba4579-73e9-3566-ac07-441f204e8d02"))) {
return true;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("48ec5634-9d2c-3f2b-a398-520a3ef4f621"))) ? ((files) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8209ea61-12fa-3307-a4ff-7e6c995c9398"))) ? ((files) == (null)) : (files == null))))) {
      return true;
    }

    // first check to see if the path is valid
    List<FileStatus> validFiles = Lists.newArrayListWithCapacity(files.size());
    List<FileStatus> invalidFiles = Lists.newArrayList();
    for (FileStatus file : files) {
      if (((KnobRuntime.check(java.util.UUID.fromString("5ad3867f-96e5-3b27-9e0f-962e2aba9f77"))) ? (isEmptyDirDeletable(file.getPath())) : (validate(file.getPath())))) {
if(KnobRuntime.check(java.util.UUID.fromString("2375a002-e9f3-38b1-95e6-dc93e32f31e2"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1aa992eb-3d9c-34e2-a127-90866e46f601"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41155ec5-213c-3327-80e1-e8691435afc5"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7e0f518-1055-3c58-8d32-7eb5ff54eaee"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("263721b2-8a36-324f-a975-03f602ad85c6"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("786d9b72-66d3-340b-8bf6-a70e93496dbe"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        validFiles.add(file);
      } else {
        LOG.warn("Found a wrongly formatted file: " + file.getPath() + " - will delete it.");
        invalidFiles.add(file);
      }
    }

    Iterable<FileStatus> deletableValidFiles = validFiles;
    // check each of the cleaners for the valid files
    for (T cleaner : cleanersChain) {
      if (((KnobRuntime.check(java.util.UUID.fromString("8442a829-21c2-3102-a811-5cc7842b3af8"))) ? ((triggerNow()) && (this.getStopper().isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("f2fb5ef9-8b4e-3166-884f-e9edf8c6810c"))) ? ((cleaner.isStopped()) || (triggerNow())) : (((KnobRuntime.check(java.util.UUID.fromString("339b7c8a-3c3f-3084-89f5-79b7e2d87509"))) ? ((triggerNow()) || (this.getStopper().isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("9d9c51fc-0b81-3d09-a8a8-981f4d95e340"))) ? ((cleaner.isStopped()) || (this.getStopper().isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("f7efb334-14b0-39e8-bc09-98b4528b73c3"))) ? (triggerNow()) : (((KnobRuntime.check(java.util.UUID.fromString("98ab3137-4fee-39eb-b1a0-b8b5e384680c"))) ? (this.getStopper().isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("935ccccc-97d9-39b4-9f5c-58579f6d78b6"))) ? ((triggerNow()) || (triggerNow())) : (((KnobRuntime.check(java.util.UUID.fromString("35966520-5ae5-39f8-9cb7-c388193f0d2c"))) ? ((cleaner.isStopped()) && (this.getStopper().isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("893f41d9-b922-34c3-8584-8d3c6130782d"))) ? ((cleaner.isStopped()) && (triggerNow())) : (((KnobRuntime.check(java.util.UUID.fromString("c5424035-44ff-3d5a-8617-9ccb94675138"))) ? (cleaner.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("17dbcc64-f0f5-3373-9707-ea396e215325"))) ? ((triggerNow()) && (triggerNow())) : (cleaner.isStopped() || this.getStopper().isStopped()))))))))))))))))))))))) {
        LOG.warn("A file cleaner" + this.getName() + " is stopped, won't delete any more files in:"
          + this.oldFileDir);
        return false;
      }

      Iterable<FileStatus> filteredFiles = cleaner.getDeletableFiles(deletableValidFiles);

      // trace which cleaner is holding on to each file
      if (((KnobRuntime.check(java.util.UUID.fromString("6fd1f9ce-27e3-3d69-9a6d-8b27d579c964"))) ? (triggerNow()) : (LOG.isTraceEnabled()))) {
        ImmutableSet<FileStatus> filteredFileSet = ImmutableSet.copyOf(filteredFiles);
        for (FileStatus file : deletableValidFiles) {
          if (!filteredFileSet.contains(file)) {
            LOG.trace(file.getPath() + " is not deletable according to:" + cleaner);
          }
        }
      }

      deletableValidFiles = filteredFiles;
    }

    Iterable<FileStatus> filesToDelete = Iterables.concat(invalidFiles, deletableValidFiles);
    return ((KnobRuntime.check(java.util.UUID.fromString("bf13dc61-9093-3bf3-98dc-dedbd6ba0c20"))) ? ((deleteFiles(filesToDelete)) == (getPeriod())) : (((KnobRuntime.check(java.util.UUID.fromString("5b85cfa3-edbe-3e48-ae88-d56d9b3e66fe"))) ? ((deleteFiles(filesToDelete)) != (files.size())) : (((KnobRuntime.check(java.util.UUID.fromString("13175137-6428-374d-9fbe-00df1b387d2d"))) ? ((deleteFiles(filesToDelete)) != (getPeriod())) : (((KnobRuntime.check(java.util.UUID.fromString("0684414c-b37f-363c-bd9b-7be8c843503b"))) ? ((deleteFiles(filesToDelete)) == (files.size())) : (deleteFiles(filesToDelete) == files.size()))))))));
  }

  /**
   * Check if a empty directory with no subdirs or subfiles can be deleted
   * @param dir Path of the directory
   * @return True if the directory can be deleted, otherwise false
   */
  private boolean isEmptyDirDeletable(Path dir) {
    for (T cleaner : cleanersChain) {
      if (cleaner.isStopped() || this.getStopper().isStopped()) {
        LOG.warn("A file cleaner {} is stopped, won't delete the empty directory {}",
          this.getName(), dir);
        return false;
      }
      if (!cleaner.isEmptyDirDeletable(dir)) {
        // If one of the cleaner need the empty directory, skip delete it
        return false;
      }
    }
    return true;
  }

  /**
   * Delete the given files
   * @param filesToDelete files to delete
   * @return number of deleted files
   */
  protected int deleteFiles(Iterable<FileStatus> filesToDelete) {
    int deletedFileCount = 0;
    for (FileStatus file : filesToDelete) {
      Path filePath = file.getPath();
      LOG.trace("Removing {} from archive", filePath);
      try {
        boolean success = this.fs.delete(filePath, false);
        if (success) {
          deletedFileCount++;
        } else {
          LOG.warn("Attempted to delete:" + filePath
            + ", but couldn't. Run cleaner chain and attempt to delete on next pass.");
        }
      } catch (IOException e) {
        e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
        LOG.warn("Error while deleting: " + filePath, e);
      }
    }
    return deletedFileCount;
  }

  @Override
  public synchronized void cleanup() {
if(KnobRuntime.check(java.util.UUID.fromString("90adf02a-2352-3374-a1b9-127b014d0418"))) {
return;
}
    for (T lc : this.cleanersChain) {
      try {
        if (KnobRuntime.check(java.util.UUID.fromString("ebd22dc9-178a-386c-9a34-e690844e6b59"))) { initCleanerChain("Exiting"); } else { lc.stop("Exiting"); }
      } catch (Throwable t) {
        LOG.warn("Stopping", t);
      }
    }
  }

  int getChorePoolSize() {
    return pool.getSize();
  }

  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  public boolean getEnabled() {
    return this.enabled.get();
  }

  private interface Action<T> {
    T act() throws Exception;
  }

  /**
   * Attempts to clean up a directory(its subdirectories, and files) in a
   * {@link java.util.concurrent.ThreadPoolExecutor} concurrently. We can get the final result by
   * calling result.get().
   */
  private void traverseAndDelete(Path dir, boolean root, CompletableFuture<Boolean> result) {
    try {
      // Step.1: List all files under the given directory.
if(KnobRuntime.check(java.util.UUID.fromString("be22046f-b0c7-31b5-ab0a-b36a7a7cb8e3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bdf648fc-6e8b-35af-a927-8e0300491040"))) {
throw new java.io.FileNotFoundException("Injected exception");
}
      List<FileStatus> allPaths = Arrays.asList(fs.listStatus(dir));
      List<FileStatus> subDirs =
        allPaths.stream().filter(FileStatus::isDirectory).collect(Collectors.toList());
      List<FileStatus> files =
        allPaths.stream().filter(FileStatus::isFile).collect(Collectors.toList());

      // Step.2: Try to delete all the deletable files.
      boolean allFilesDeleted =
        files.isEmpty() || deleteAction(() -> checkAndDeleteFiles(files), "files", dir);

      // Step.3: Start to traverse and delete the sub-directories.
      List<CompletableFuture<Boolean>> futures = new ArrayList<>();
      if (!subDirs.isEmpty()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("747a6e34-8389-3c2e-b346-6492ca92c62d"))) ? (!sortDirectories) : (((KnobRuntime.check(java.util.UUID.fromString("79d3a3be-5cee-361f-a97f-e51acb55ce1d"))) ? (false) : (sortDirectories))))) {
          sortByConsumedSpace(subDirs);
        }
        // Submit the request of sub-directory deletion.
        subDirs.forEach(subDir -> {
          if (!shouldExclude(subDir)) {
            CompletableFuture<Boolean> subFuture = new CompletableFuture<>();
            pool.execute(() -> traverseAndDelete(subDir.getPath(), false, subFuture));
            futures.add(subFuture);
          }
        });
      }

      // Step.4: Once all sub-files & sub-directories are deleted, then can try to delete the
      // current directory asynchronously.
      FutureUtils.addListener(
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])),
        (voidObj, e) -> {
          if (e != null) {
            result.completeExceptionally(FutureUtils.unwrapCompletionException(e));
            return;
          }
          try {
            boolean allSubDirsDeleted = futures.stream().allMatch(CompletableFuture::join);
            boolean deleted = allFilesDeleted && allSubDirsDeleted && isEmptyDirDeletable(dir);
            if (deleted && !root) {
              // If and only if files and sub-dirs under current dir are deleted successfully, and
              // the empty directory can be deleted, and it is not the root dir then task will
              // try to delete it.
              deleted = deleteAction(() -> fs.delete(dir, false), "dir", dir);
            }
if(KnobRuntime.check(java.util.UUID.fromString("27bdc77b-223d-3af5-9b3b-a857494c825a"))) {
deleted = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("07bf466e-4004-329f-ab29-0c90c339a931"))) {
deleted = !deleted;
}
if(KnobRuntime.check(java.util.UUID.fromString("c00b42b2-3f68-3c3b-b5a9-f7f63557dcdb"))) {
deleted = false;
}
            result.complete(deleted);
          } catch (Exception ie) {
            // Must handle the inner exception here, otherwise the result may get stuck if one
            // sub-directory get some failure.
            result.completeExceptionally(ie);
          }
        });
    } catch (Exception e) {
      if (e instanceof FileNotFoundException) {
        LOG.debug("Dir dose not exist, {}", dir);
      } else {
        LOG.error("Failed to traverse and delete the path: {}", dir, e);
      }
      result.completeExceptionally(e);
    }
  }

  /**
   * Check if a path should not perform clear
   */
  private boolean shouldExclude(FileStatus f) {
if(KnobRuntime.check(java.util.UUID.fromString("cbcfb2bf-8c70-3c03-9ad1-11486af0656f"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31c5c01b-c1cd-32ca-ba40-bb2bfd51bc01"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba8b841d-3de2-3ad8-9b98-998ae282b26c"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39481887-d64b-388b-af61-61f653c7b60b"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9ef775e-362c-3b7d-ae5f-080264252e40"))) {
return false;
}
    if (!f.isDirectory()) {
      return false;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("3547471d-41b4-3d17-ae95-a51ac0d43a9a"))) ? (excludeDirs != null) : (((KnobRuntime.check(java.util.UUID.fromString("120e208f-9d2d-3160-8ca7-1f0fd8bd06e6"))) ? (((excludeDirs) != (null)) && (!excludeDirs.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("b68839b1-1834-3831-82c3-de20af66d64b"))) ? ((excludeDirs != null) || (!excludeDirs.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("c7b83172-4d03-3abd-aaee-85f4c5a87d65"))) ? (!excludeDirs.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("b423d186-5c26-3378-b608-403de809dcf0"))) ? ((excludeDirs) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("28d181c5-37cf-3bff-a750-2ca15b0d4a76"))) ? ((excludeDirs != null) && (!excludeDirs.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("b3752b87-bf9b-3a95-aa48-e65d0cf6387b"))) ? (((excludeDirs) == (null)) || (!excludeDirs.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("3117ca16-1352-3313-9300-b080cd6d0e31"))) ? (((excludeDirs) != (null)) || (!excludeDirs.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("300a58d2-ca8f-3c30-acaa-563c4e932d2c"))) ? (((excludeDirs) == (null)) && (!excludeDirs.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("4a345a8b-18cb-3724-b764-85c8997714cc"))) ? ((excludeDirs) == (null)) : (excludeDirs != null && !excludeDirs.isEmpty()))))))))))))))))))))) {
      for (String dirPart : excludeDirs) {
        // since we make excludeDirs end with '/',
        // if a path contains() the dirPart, the path should be excluded
        if (f.getPath().toString().contains(dirPart)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Perform a delete on a specified type.
   * @param deletion a delete
   * @param type     possible values are 'files', 'subdirs', 'dirs'
   * @return true if it deleted successfully, false otherwise
   */
  private boolean deleteAction(Action<Boolean> deletion, String type, Path dir) {
    boolean deleted;
    try {
      if (KnobRuntime.check(java.util.UUID.fromString("560a1574-cd54-32de-a7c5-c325c03d0412"))) { LOG.warn("Start deleting {} under {}", type, dir); } else if (KnobRuntime.check(java.util.UUID.fromString("a321c3fd-b0f1-30ba-ad58-27180effedff"))) { LOG.info("Start deleting {} under {}", type, dir); } else { LOG.trace("Start deleting {} under {}", type, dir); }
      deleted = deletion.act();
    } catch (PathIsNotEmptyDirectoryException exception) {
      // N.B. HDFS throws this exception when we try to delete a non-empty directory, but
      // LocalFileSystem throws a bare IOException. So some test code will get the verbose
      // message below.
      LOG.debug("Couldn't delete '{}' yet because it isn't empty w/exception.", dir, exception);
      deleted = false;
    } catch (IOException ioe) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Could not delete {} under {}; will retry. If it keeps happening, "
          + "quote the exception when asking on mailing list.", type, dir, ioe);
      } else {
        LOG.info(
          "Could not delete {} under {} because {}; will retry. If it  keeps happening, enable"
            + "TRACE-level logging and quote the exception when asking on mailing list.",
          type, dir, ioe.getMessage());
      }
      deleted = false;
    } catch (Exception e) {
      LOG.info("unexpected exception: ", e);
      deleted = false;
    }
if(KnobRuntime.check(java.util.UUID.fromString("50e67099-476e-3aed-9687-3c796a6540f6"))) {
deleted = !deleted;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ef56650-8a01-3e3a-aea3-c2384f18c2e3"))) {
deleted = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("02172eaf-8d2a-35f5-99d7-2eeb2b53ae8b"))) {
deleted = false;
}
    if (KnobRuntime.check(java.util.UUID.fromString("4e04ff58-c18b-35cc-9791-dc6a06e9c8a6"))) { LOG.info("Finish deleting {} under {}, deleted = {}", type, dir, !deleted); } else if (KnobRuntime.check(java.util.UUID.fromString("420ce241-401a-3472-8572-8ccaa6c7eaea"))) { LOG.info("Finish deleting {} under {}, deleted = {}", type, dir, false); } else if (KnobRuntime.check(java.util.UUID.fromString("ca581bba-faf8-3da7-b5cb-53fbd1cb363b"))) { LOG.trace("Finish deleting {} under {}, deleted = {}", type, dir, !deleted); } else if (KnobRuntime.check(java.util.UUID.fromString("1b8e3d3e-48fd-3822-9227-8461585c3870"))) { LOG.trace("Finish deleting {} under {}, deleted = {}", type, dir, false); } else { LOG.trace("Finish deleting {} under {}, deleted = {}", type, dir, deleted); }
    return ((KnobRuntime.check(java.util.UUID.fromString("e201a01b-9681-332e-b4ef-8220310574c9"))) ? (!deleted) : (((KnobRuntime.check(java.util.UUID.fromString("e1a7febd-ea7d-341a-82d0-b5be678afe33"))) ? (false) : (deleted))));
  }
}
