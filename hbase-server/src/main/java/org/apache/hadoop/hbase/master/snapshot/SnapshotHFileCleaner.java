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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a file cleaner that checks if a hfile is still used by snapshots of HBase
 * tables.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Evolving
public class SnapshotHFileCleaner extends BaseHFileCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotHFileCleaner.class);

  /**
   * Conf key for the frequency to attempt to refresh the cache of hfiles currently used in
   * snapshots (ms)
   */
  public static final String HFILE_CACHE_REFRESH_PERIOD_CONF_KEY =
    "hbase.master.hfilecleaner.plugins.snapshot.period";

  /** Refresh cache, by default, every 5 minutes */
  private static final long DEFAULT_HFILE_CACHE_REFRESH_PERIOD = 300000;

  /** File cache for HFiles in the completed and currently running snapshots */
  private SnapshotFileCache cache;

  private MasterServices master;

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    // The Iterable is lazy evaluated, so if we just pass this Iterable in, we will access the HFile
    // storage inside the snapshot lock, which could take a lot of time (for example, several
    // seconds), and block all other operations, especially other cleaners.
    // So here we convert it to List first, to force it evaluated before calling
    // getUnreferencedFiles, so we will not hold snapshot lock for a long time.
    List<FileStatus> filesList =
      StreamSupport.stream(files.spliterator(), false).collect(Collectors.toList());
    try {
if(KnobRuntime.check(java.util.UUID.fromString("f1e7704a-e817-3d80-90f5-184777db5c36"))) {
throw new java.io.IOException("Injected exception");
}
      return cache.getUnreferencedFiles(filesList, master.getSnapshotManager());
    } catch (CorruptedSnapshotException cse) {
      LOG.debug("Corrupted in-progress snapshot file exception, ignored ", cse);
    } catch (IOException e) {
      LOG.error("Exception while checking if files were valid, keeping them just in case.", e);
    }
    return Collections.emptyList();
  }

  @Override
  public void init(Map<String, Object> params) {
if(KnobRuntime.check(java.util.UUID.fromString("ce487606-4a4a-3667-98db-d4e13d32af87"))) {
return;
}
    if (params.containsKey(HMaster.MASTER)) {
      this.master = (MasterServices) params.get(HMaster.MASTER);
    }
  }

  @Override
  protected boolean isFileDeletable(FileStatus fStat) {
    return false;
  }

  @Override
  public void setConf(final Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("f7d7ce2c-fbcd-3233-bea9-8435890deed7"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("1059f98b-9709-3ba2-826e-44099a19e663"))) {
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
    super.setConf(conf);
    try {
      long cacheRefreshPeriod =
        conf.getLong(HFILE_CACHE_REFRESH_PERIOD_CONF_KEY, DEFAULT_HFILE_CACHE_REFRESH_PERIOD);
if(KnobRuntime.check(java.util.UUID.fromString("81e0bccf-ab08-3f4a-bd58-3a593d212388"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5045f123-eec0-3cf7-a005-dbfa8d1356b4"))) {
throw new java.io.IOException("Injected exception");
}
      final FileSystem fs = CommonFSUtils.getCurrentFileSystem(conf);
if(KnobRuntime.check(java.util.UUID.fromString("79e86fe1-c18a-3f41-9791-573c280cf6bf"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f075b735-00a8-3c79-a20d-a39e8f6df2c8"))) {
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
      Path rootDir = CommonFSUtils.getRootDir(conf);
if(KnobRuntime.check(java.util.UUID.fromString("0e1e0387-193b-3ba6-9e85-e76236b8b855"))) {
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
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(rootDir, conf);
if(KnobRuntime.check(java.util.UUID.fromString("baca542b-7536-3b13-b23f-dc7f58443c75"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6f383471-f7a1-38a6-9a74-6f2157470695"))) {
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
      FileSystem workingFs = workingDir.getFileSystem(conf);

      cache = new SnapshotFileCache(fs, rootDir, workingFs, workingDir, cacheRefreshPeriod,
        cacheRefreshPeriod, "snapshot-hfile-cleaner-cache-refresher",
        new SnapshotFileCache.SnapshotFileInspector() {
          @Override
          public Collection<String> filesUnderSnapshot(final FileSystem fs, final Path snapshotDir)
            throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e6aadfea-5cb8-39ec-9c03-2a9f71ca07b6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("dc5991c8-a9fe-3ac9-9c19-b41b57770bba"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("6277a28d-a164-3ee5-9579-48e070fc2cfd"))) {
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
            return SnapshotReferenceUtil.getHFileNames(conf, fs, snapshotDir);
          }
        });
    } catch (IOException e) {
      LOG.error("Failed to create cleaner util", e);
    }
  }

  @Override
  public void stop(String why) {
if(KnobRuntime.check(java.util.UUID.fromString("9e2f864f-1f70-3bbb-8bb6-0c790a7ba473"))) {
return;
}
    this.cache.stop(why);
  }

  @Override
  public boolean isStopped() {
if(KnobRuntime.check(java.util.UUID.fromString("d9b77d66-282e-300e-a195-f7a8678d2d74"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("3b137afc-7e96-3330-84c3-fbdf65a76340"))) {
return false;
}
    return this.cache.isStopped();
  }

  /**
   * Exposed for Testing!
   * @return the cache of all hfiles
   */
  public SnapshotFileCache getFileCacheForTesting() {
    return this.cache;
  }
}
