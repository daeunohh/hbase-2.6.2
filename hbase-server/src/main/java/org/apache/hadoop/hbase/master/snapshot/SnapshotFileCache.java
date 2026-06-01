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
package org.apache.hadoop.hbase.master.snapshot;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Intelligently keep track of all the files for all the snapshots.
 * <p>
 * A cache of files is kept to avoid querying the {@link FileSystem} frequently. If there is a cache
 * miss the directory modification time is used to ensure that we don't rescan directories that we
 * already have in cache. We only check the modification times of the snapshot directories
 * (/hbase/.snapshot/[snapshot_name]) to determine if the files need to be loaded into the cache.
 * <p>
 * New snapshots will be added to the cache and deleted snapshots will be removed when we refresh
 * the cache. If the files underneath a snapshot directory are changed, but not the snapshot itself,
 * we will ignore updates to that snapshot's files.
 * <p>
 * This is sufficient because each snapshot has its own directory and is added via an atomic rename
 * <i>once</i>, when the snapshot is created. We don't need to worry about the data in the snapshot
 * being run.
 * <p>
 * Further, the cache is periodically refreshed ensure that files in snapshots that were deleted are
 * also removed from the cache.
 * <p>
 * A {@link SnapshotFileCache.SnapshotFileInspector} must be passed when creating <tt>this</tt> to
 * allow extraction of files under /hbase/.snapshot/[snapshot name] directory, for each snapshot.
 * This allows you to only cache files under, for instance, all the logs in the .logs directory or
 * all the files under all the regions.
 * <p>
 * <tt>this</tt> also considers all running snapshots (those under /hbase/.snapshot/.tmp) as valid
 * snapshots and will attempt to cache files from those snapshots as well.
 * <p>
 * Queries about a given file are thread-safe with respect to multiple queries and cache refreshes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SnapshotFileCache implements Stoppable {
  interface SnapshotFileInspector {
    /**
     * Returns a collection of file names needed by the snapshot.
     * @param fs          {@link FileSystem} where snapshot mainifest files are stored
     * @param snapshotDir {@link Path} to the snapshot directory to scan.
     * @return the collection of file names needed by the snapshot.
     */
    Collection<String> filesUnderSnapshot(final FileSystem fs, final Path snapshotDir)
      throws IOException;
  }

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotFileCache.class);
  private volatile boolean stop = false;
  private final FileSystem fs, workingFs;
  private final SnapshotFileInspector fileInspector;
  private final Path snapshotDir, workingSnapshotDir;
  private volatile ImmutableSet<String> cache = ImmutableSet.of();
  /**
   * This is a helper map of information about the snapshot directories so we don't need to rescan
   * them if they haven't changed since the last time we looked.
   */
  private ImmutableMap<String, SnapshotDirectoryInfo> snapshots = ImmutableMap.of();
  private final Timer refreshTimer;

  private static final int LOCK_TIMEOUT_MS = 30000;

  /**
   * Create a snapshot file cache for all snapshots under the specified [root]/.snapshot on the
   * filesystem.
   * <p>
   * Immediately loads the file cache.
   * @param conf                 to extract the configured {@link FileSystem} where the snapshots
   *                             are stored and hbase root directory
   * @param cacheRefreshPeriod   frequency (ms) with which the cache should be refreshed
   * @param cacheRefreshDelay    amount of time to wait for the cache to be refreshed
   * @param refreshThreadName    name of the cache refresh thread
   * @param inspectSnapshotFiles Filter to apply to each snapshot to extract the files.
   * @throws IOException if the {@link FileSystem} or root directory cannot be loaded
   */
  public SnapshotFileCache(Configuration conf, long cacheRefreshPeriod, long cacheRefreshDelay,
    String refreshThreadName, SnapshotFileInspector inspectSnapshotFiles) throws IOException {
    this(CommonFSUtils.getCurrentFileSystem(conf), CommonFSUtils.getRootDir(conf),
      SnapshotDescriptionUtils.getWorkingSnapshotDir(CommonFSUtils.getRootDir(conf), conf)
        .getFileSystem(conf),
      SnapshotDescriptionUtils.getWorkingSnapshotDir(CommonFSUtils.getRootDir(conf), conf),
      cacheRefreshPeriod, cacheRefreshDelay, refreshThreadName, inspectSnapshotFiles);
  }

  /**
   * Create a snapshot file cache for all snapshots under the specified [root]/.snapshot on the
   * filesystem
   * @param fs                   {@link FileSystem} where the snapshots are stored
   * @param rootDir              hbase root directory
   * @param workingFs            {@link FileSystem} where ongoing snapshot mainifest files are
   *                             stored
   * @param workingDir           Location to store ongoing snapshot manifest files
   * @param cacheRefreshPeriod   period (ms) with which the cache should be refreshed
   * @param cacheRefreshDelay    amount of time to wait for the cache to be refreshed
   * @param refreshThreadName    name of the cache refresh thread
   * @param inspectSnapshotFiles Filter to apply to each snapshot to extract the files.
   */
  public SnapshotFileCache(FileSystem fs, Path rootDir, FileSystem workingFs, Path workingDir,
    long cacheRefreshPeriod, long cacheRefreshDelay, String refreshThreadName,
    SnapshotFileInspector inspectSnapshotFiles) {
    this.fs = fs;
    this.workingFs = workingFs;
    this.workingSnapshotDir = workingDir;
    this.fileInspector = inspectSnapshotFiles;
    this.snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    // periodically refresh the file cache to make sure we aren't superfluously saving files.
    this.refreshTimer = new Timer(refreshThreadName, true);
    this.refreshTimer.scheduleAtFixedRate(new RefreshCacheTask(), cacheRefreshDelay,
      cacheRefreshPeriod);
  }

  /**
   * Trigger a cache refresh, even if its before the next cache refresh. Does not affect pending
   * cache refreshes.
   * <p/>
   * Blocks until the cache is refreshed.
   * <p/>
   * Exposed for TESTING.
   */
  public synchronized void triggerCacheRefreshForTesting() {
    try {
      refreshCache();
    } catch (IOException e) {
      LOG.warn("Failed to refresh snapshot hfile cache!", e);
    }
    LOG.debug("Current cache:" + cache);
  }

  /**
   * Check to see if any of the passed file names is contained in any of the snapshots. First checks
   * an in-memory cache of the files to keep. If its not in the cache, then the cache is refreshed
   * and the cache checked again for that file. This ensures that we never return files that exist.
   * <p>
   * Note this may lead to periodic false positives for the file being referenced. Periodically, the
   * cache is refreshed even if there are no requests to ensure that the false negatives get removed
   * eventually. For instance, suppose you have a file in the snapshot and it gets loaded into the
   * cache. Then at some point later that snapshot is deleted. If the cache has not been refreshed
   * at that point, cache will still think the file system contains that file and return
   * <tt>true</tt>, even if it is no longer present (false positive). However, if the file never was
   * on the filesystem, we will never find it and always return <tt>false</tt>.
   * @param files file to check
   * @return <tt>unReferencedFiles</tt> the collection of files that do not have snapshot references
   * @throws IOException if there is an unexpected error reaching the filesystem.
   */
  public Iterable<FileStatus> getUnreferencedFiles(List<FileStatus> files,
    final SnapshotManager snapshotManager) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e14768c9-f032-3e9e-8d6e-02266b990216"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b613e91f-4117-3781-ae90-0f8254ff18e8"))) {
try {
    java.lang.reflect.Field field = snapshotManager.getClass().getDeclaredField("stopped");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshotManager);
    field.set(snapshotManager, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b50215a-3cd7-36ff-8aac-6adf2728a83f"))) {
try {
    java.lang.reflect.Field field = snapshotManager.getClass().getDeclaredField("isSnapshotSupported");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshotManager);
    field.set(snapshotManager, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a99e8cc7-e40f-3ea2-ba4c-21b2e97c95e4"))) {
return null;
}
    List<FileStatus> unReferencedFiles = Lists.newArrayList();
    List<String> snapshotsInProgress = null;
    boolean refreshed = false;
    Lock lock = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("178affc2-340f-30bf-91c1-3942d65f9af9"))) ? ((snapshotManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("46dc3c6a-38a7-3921-a1c9-daf8cb98ef4f"))) ? ((snapshotManager) == (null)) : (snapshotManager != null))))) {
      lock = snapshotManager.getTakingSnapshotLock().writeLock();
    }
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("9e3f7506-5bc0-32da-b001-0f892124e913"))) ? ((lock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d03aa46f-464c-369d-ab2a-5e105b20f181"))) ? (((lock) == (null)) && (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS))) : (((KnobRuntime.check(java.util.UUID.fromString("835d3557-1805-31a3-a1a5-1c0341707be7"))) ? (((lock) == (null)) || (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS))) : (((KnobRuntime.check(java.util.UUID.fromString("51ce5b95-6d5a-3f93-a5d2-69d2c81571eb"))) ? (lock == null) : (((KnobRuntime.check(java.util.UUID.fromString("520ccf62-879c-318b-a1ab-f79915ad1c28"))) ? ((lock == null) && (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS))) : (((KnobRuntime.check(java.util.UUID.fromString("fb71f76a-bbc7-3959-a994-9b962667a9dc"))) ? (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) : (((KnobRuntime.check(java.util.UUID.fromString("af5025a7-4737-3201-be48-ba2798a3a118"))) ? ((lock == null) || (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS))) : (((KnobRuntime.check(java.util.UUID.fromString("07eec070-0764-38cd-af44-04f46c77cf9e"))) ? (((lock) != (null)) || (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS))) : (((KnobRuntime.check(java.util.UUID.fromString("b2c6b16b-d15f-3ac7-829e-f7e0a1007b46"))) ? (((lock) != (null)) && (lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS))) : (((KnobRuntime.check(java.util.UUID.fromString("7e57c740-c936-36a0-8a0d-1c6932a2cde5"))) ? ((lock) == (null)) : (lock == null || lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)))))))))))))))))))))) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("d326d3b7-ab87-329b-b843-2120e68a254c"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (((KnobRuntime.check(java.util.UUID.fromString("db0baf12-0326-3719-b50c-dd0ea1d234fd"))) ? (snapshotManager != null) : (((KnobRuntime.check(java.util.UUID.fromString("88b4d764-0d52-38a5-972e-a226c683bf56"))) ? ((snapshotManager) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bdb5261b-d2eb-33fc-9ba2-ed0101c384b8"))) ? (((snapshotManager) == (null)) || (snapshotManager.isTakingAnySnapshot())) : (((KnobRuntime.check(java.util.UUID.fromString("8f0c90d4-31ae-3704-b0d2-4907ecd51587"))) ? (((snapshotManager) != (null)) || (snapshotManager.isTakingAnySnapshot())) : (((KnobRuntime.check(java.util.UUID.fromString("cc82694e-5ced-368b-b613-c8739a6265cc"))) ? (((snapshotManager) == (null)) && (snapshotManager.isTakingAnySnapshot())) : (((KnobRuntime.check(java.util.UUID.fromString("bfa98741-ee03-3cf1-b66a-df2aaf45fd4b"))) ? ((snapshotManager != null) && (snapshotManager.isTakingAnySnapshot())) : (((KnobRuntime.check(java.util.UUID.fromString("ab2d9b3b-7b0d-3e7b-94c1-99e901f27cbc"))) ? (snapshotManager.isTakingAnySnapshot()) : (((KnobRuntime.check(java.util.UUID.fromString("ed17a920-fd89-3faf-a9fb-84918e0b6e60"))) ? ((snapshotManager != null) || (snapshotManager.isTakingAnySnapshot())) : (((KnobRuntime.check(java.util.UUID.fromString("10804c33-922d-3830-b666-d2f773bd059f"))) ? ((snapshotManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6c872149-ef4b-3bbe-8e24-559f2f2b8b3d"))) ? (((snapshotManager) != (null)) && (snapshotManager.isTakingAnySnapshot())) : (snapshotManager != null && snapshotManager.isTakingAnySnapshot()))))))))))))))))))))) {
            LOG.warn("Not checking unreferenced files since snapshot is running, it will "
              + "skip to clean the HFiles this time");
            return unReferencedFiles;
          }
          ImmutableSet<String> currentCache = cache;
          for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            if (((KnobRuntime.check(java.util.UUID.fromString("a039073a-1698-3bb5-bb14-ca1dccc0d8c9"))) ? (!currentCache.contains(fileName)) : (((KnobRuntime.check(java.util.UUID.fromString("a122796e-8897-3d33-92e1-94737c870dc9"))) ? (!refreshed) : (((KnobRuntime.check(java.util.UUID.fromString("8e3f422e-8361-3d95-8157-555106943296"))) ? ((!refreshed) && (!currentCache.contains(fileName))) : (((KnobRuntime.check(java.util.UUID.fromString("d183ac2b-5ff9-3931-bf0c-5023279277e6"))) ? ((!refreshed) || (!currentCache.contains(fileName))) : (!refreshed && !currentCache.contains(fileName)))))))))) {
              synchronized (this) {
if(KnobRuntime.check(java.util.UUID.fromString("c187d520-0282-3e90-92bb-a8769513983f"))) {
throw new java.io.IOException("Injected exception");
}
                refreshCache();
                currentCache = cache;
                refreshed = true;
              }
            }
            if (currentCache.contains(fileName)) {
              continue;
            }
            if (((KnobRuntime.check(java.util.UUID.fromString("0b0224cf-5161-3687-ad3f-f84e64023319"))) ? ((snapshotsInProgress) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f1752c65-6d65-364c-bc8f-500d5117015d"))) ? ((getSnapshotsInProgress()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("df011c3b-5090-3db4-9e68-bcd327a0e2e6"))) ? ((snapshotsInProgress) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("312f7288-b6a4-313f-badd-1feef90a3bd2"))) ? ((getSnapshotsInProgress()) == (null)) : (snapshotsInProgress == null))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("58c1b34c-bbe3-37e4-9852-a7d8af8c6135"))) {
throw new java.io.IOException("Injected exception");
}
              snapshotsInProgress = getSnapshotsInProgress();
            }
            if (snapshotsInProgress.contains(fileName)) {
              continue;
            }
if(KnobRuntime.check(java.util.UUID.fromString("a0ae7a5f-56a2-33ba-9739-6c7a15c96f95"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6eb8df26-8f52-3f81-8b02-9f75cd9684d4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("30dbb541-b013-33bf-ac09-566ebf5c1e8a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cbc3a995-2c87-3c18-8866-f544d24ac389"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b2ac7b53-f5e8-323c-9150-87060839fb3d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba2664e3-1e93-3092-ab78-f9296a65a20c"))) {
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
            unReferencedFiles.add(file);
          }
        } finally {
          if (((KnobRuntime.check(java.util.UUID.fromString("d300298b-e783-37b8-924f-7dbb4925141f"))) ? ((lock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a0c34db6-d8b4-343c-9322-56e76a3d9869"))) ? ((lock) == (null)) : (lock != null))))) {
            if (KnobRuntime.check(java.util.UUID.fromString("cf18133d-75b0-3f65-876e-6240eab89098"))) { refreshCache(); } else { lock.unlock(); }
          }
        }
      } else {
        LOG.warn("Failed to acquire write lock on taking snapshot after waiting {}ms",
          LOCK_TIMEOUT_MS);
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while acquiring write lock on taking snapshot");
      Thread.currentThread().interrupt(); // restore the interrupt flag
    }
    return unReferencedFiles;
  }

  private void refreshCache() throws IOException {
    // just list the snapshot directory directly, do not check the modification time for the root
    // snapshot directory, as some file system implementations do not modify the parent directory's
    // modTime when there are new sub items, for example, S3.
if(KnobRuntime.check(java.util.UUID.fromString("2ce9cd52-b01b-353a-8e1e-a5a02615c3c7"))) {
throw new java.io.IOException("Injected exception");
}
    FileStatus[] snapshotDirs = CommonFSUtils.listStatus(fs, snapshotDir,
      p -> !p.getName().equals(SnapshotDescriptionUtils.SNAPSHOT_TMP_DIR_NAME));

    // clear the cache, as in the below code, either we will also clear the snapshots, or we will
    // refill the file name cache again.
    if (ArrayUtils.isEmpty(snapshotDirs)) {
      // remove all the remembered snapshots because we don't have any left
      if (LOG.isDebugEnabled() && this.snapshots.size() > 0) {
        LOG.debug("No snapshots on-disk, clear cache");
      }
      this.snapshots = ImmutableMap.of();
      this.cache = ImmutableSet.of();
      return;
    }

    ImmutableSet.Builder<String> cacheBuilder = ImmutableSet.builder();
    ImmutableMap.Builder<String, SnapshotDirectoryInfo> snapshotsBuilder = ImmutableMap.builder();
    // iterate over all the cached snapshots and see if we need to update some, it is not an
    // expensive operation if we do not reload the manifest of snapshots.
    for (FileStatus snapshotDir : snapshotDirs) {
      String name = snapshotDir.getPath().getName();
      SnapshotDirectoryInfo files = snapshots.get(name);
      // if we don't know about the snapshot or its been modified, we need to update the
      // files the latter could occur where I create a snapshot, then delete it, and then make a
      // new snapshot with the same name. We will need to update the cache the information from
      // that new snapshot, even though it has the same name as the files referenced have
      // probably changed.
      if (((KnobRuntime.check(java.util.UUID.fromString("72fbbcba-15ea-3b98-b9d4-a396a1b13b5c"))) ? (files.hasBeenModified(snapshotDir.getModificationTime())) : (((KnobRuntime.check(java.util.UUID.fromString("66328cc7-48a4-3be9-9bd2-326699581d5d"))) ? ((files == null) && (files.hasBeenModified(snapshotDir.getModificationTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("b4bc61cf-497e-36ed-aa83-ed735bb7f1c6"))) ? ((files == null) || (files.hasBeenModified(snapshotDir.getModificationTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("93ad87c0-58d6-35bd-89d7-3dad26463c41"))) ? (files == null) : (((KnobRuntime.check(java.util.UUID.fromString("c7734018-d07b-391b-bc66-96dd4907ed15"))) ? (((files) == (null)) && (files.hasBeenModified(snapshotDir.getModificationTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("87e6b376-3943-36c1-9c13-7cda059c22a9"))) ? (((files) != (null)) && (files.hasBeenModified(snapshotDir.getModificationTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("e381b62a-cbef-3f27-9c3e-0ad390c9441b"))) ? (((files) != (null)) || (files.hasBeenModified(snapshotDir.getModificationTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("847c8ec9-95a4-3c0d-a04a-098c56ce921c"))) ? (((files) == (null)) || (files.hasBeenModified(snapshotDir.getModificationTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("6ccd09c7-d405-399c-b125-1ee579d4cb35"))) ? ((files) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("92182df2-05d5-3b18-99fd-4137b1511ce4"))) ? ((files) != (null)) : (files == null || files.hasBeenModified(snapshotDir.getModificationTime())))))))))))))))))))))) {
        Collection<String> storedFiles =
          fileInspector.filesUnderSnapshot(fs, snapshotDir.getPath());
        files = new SnapshotDirectoryInfo(snapshotDir.getModificationTime(), storedFiles);
      }
      // add all the files to cache
      cacheBuilder.addAll(files.getFiles());
if(KnobRuntime.check(java.util.UUID.fromString("cb923f68-54b0-3f03-a567-72d696fcc565"))) {
try {
    java.lang.reflect.Field field = files.getClass().getDeclaredField("lastModified");
    field.setAccessible(true);
    long oldValue = ((long)field.get(files));
    field.set(files, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("212c36c2-62c9-39cd-aaa2-98ee8f6abbc8"))) {
try {
    java.lang.reflect.Field field = files.getClass().getDeclaredField("lastModified");
    field.setAccessible(true);
    long oldValue = ((long)field.get(files));
    field.set(files, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      snapshotsBuilder.put(name, files);
    }
    // set the snapshots we are tracking
    this.snapshots = snapshotsBuilder.build();
    this.cache = cacheBuilder.build();
  }

  List<String> getSnapshotsInProgress() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e082d66e-afe9-3514-853a-e800991864f4"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("dc577279-11a4-37eb-b63d-785ef87d0570"))) {
throw new java.io.IOException("Injected exception");
}
    List<String> snapshotInProgress = Lists.newArrayList();
    // only add those files to the cache, but not to the known snapshots

    FileStatus[] snapshotsInProgress =
      CommonFSUtils.listStatus(this.workingFs, this.workingSnapshotDir);

    if (!ArrayUtils.isEmpty(snapshotsInProgress)) {
      for (FileStatus snapshot : snapshotsInProgress) {
        try {
          snapshotInProgress
            .addAll(fileInspector.filesUnderSnapshot(workingFs, snapshot.getPath()));
        } catch (CorruptedSnapshotException cse) {
          LOG.info("Corrupted in-progress snapshot file exception, ignored.", cse);
        }
      }
    }
    return snapshotInProgress;
  }

  /**
   * Simple helper task that just periodically attempts to refresh the cache
   */
  public class RefreshCacheTask extends TimerTask {
    @Override
    public void run() {
      synchronized (SnapshotFileCache.this) {
        try {
          SnapshotFileCache.this.refreshCache();
        } catch (IOException e) {
          LOG.warn("Failed to refresh snapshot hfile cache!", e);
          // clear all the cached entries if we meet an error
          cache = ImmutableSet.of();
          snapshots = ImmutableMap.of();
        }
      }
    }
  }

  @Override
  public void stop(String why) {
if(KnobRuntime.check(java.util.UUID.fromString("e376f966-03fd-38a8-8b15-3dca678a160f"))) {
return;
}
    if (!this.stop) {
      this.stop = true;
      this.refreshTimer.cancel();
    }
  }

  @Override
  public boolean isStopped() {
if(KnobRuntime.check(java.util.UUID.fromString("c99acdab-0d74-34e7-91e6-22b592638dc2"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("01b036e4-3eb9-30c6-8c89-fe356d1ee2f3"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e6c9bab8-ada6-33dc-a8b4-762910b7f108"))) ? (!this.stop) : (this.stop));
  }

  /**
   * Information about a snapshot directory
   */
  private static class SnapshotDirectoryInfo {
    long lastModified;
    Collection<String> files;

    public SnapshotDirectoryInfo(long mtime, Collection<String> files) {
      this.lastModified = mtime;
      this.files = files;
    }

    /** Returns the hfiles in the snapshot when <tt>this</tt> was made. */
    public Collection<String> getFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("189c37f8-6391-3617-85ea-41563d123f1d"))) {
return null;
}
      return this.files;
    }

    /**
     * Check if the snapshot directory has been modified
     * @param mtime current modification time of the directory
     * @return <tt>true</tt> if it the modification time of the directory is newer time when we
     *         created <tt>this</tt>
     */
    public boolean hasBeenModified(long mtime) {
if(KnobRuntime.check(java.util.UUID.fromString("b88d93c3-9599-3a6d-b810-d970eb418cbb"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("72b07a7a-5daf-3114-9c6e-9322c0596e2e"))) {
return true;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("70f7307b-81e4-335d-aca5-2c47363c25a0"))) ? ((this.lastModified) == (mtime)) : (((KnobRuntime.check(java.util.UUID.fromString("7561454f-20b8-30f0-9446-0fb23e5a633b"))) ? ((this.lastModified) <= (mtime)) : (((KnobRuntime.check(java.util.UUID.fromString("60a6dfdc-3f33-3070-a7ae-f5dce525ba50"))) ? ((this.lastModified) < (mtime)) : (((KnobRuntime.check(java.util.UUID.fromString("2f4f5e8d-6727-3b6c-9ccf-7b486ceb67ad"))) ? ((this.lastModified) >= (mtime)) : (((KnobRuntime.check(java.util.UUID.fromString("44c9989f-fb2d-3428-a656-0e158b19b78d"))) ? ((this.lastModified) > (mtime)) : (((KnobRuntime.check(java.util.UUID.fromString("d02d4d3e-1d97-3109-8ae7-114fbfb083d3"))) ? ((this.lastModified) != (mtime)) : (this.lastModified < mtime))))))))))));
    }
  }
}
