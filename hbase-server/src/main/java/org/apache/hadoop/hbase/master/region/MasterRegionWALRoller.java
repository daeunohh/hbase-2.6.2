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
package org.apache.hadoop.hbase.master.region;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.HConstants.HREGION_OLDLOGDIR_NAME;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.AbstractWALRoller;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * As long as there is no RegionServerServices for a master local region, we need implement log
 * roller logic by our own.
 * <p/>
 * We can reuse most of the code for normal wal roller, the only difference is that there is only
 * one region, so in {@link #scheduleFlush(String, List)} method we can just schedule flush for the
 * master local region.
 */
@InterfaceAudience.Private
public final class MasterRegionWALRoller extends AbstractWALRoller<Abortable> {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegionWALRoller.class);

  private volatile MasterRegionFlusherAndCompactor flusherAndCompactor;

  private final FileSystem fs;

  private final Path walArchiveDir;

  private final Path globalWALArchiveDir;

  private final String archivedWALSuffix;

  private MasterRegionWALRoller(String name, Configuration conf, Abortable abortable, FileSystem fs,
    Path walRootDir, Path globalWALRootDir, String archivedWALSuffix) {
    super(name, conf, abortable);
    this.fs = fs;
    this.walArchiveDir = new Path(walRootDir, HREGION_OLDLOGDIR_NAME);
    this.globalWALArchiveDir = new Path(globalWALRootDir, HREGION_OLDLOGDIR_NAME);
    this.archivedWALSuffix = archivedWALSuffix;
  }

  @Override
  protected void afterWALArchive(Path oldPath, Path newPath) {
    // move the archived WAL files to the global archive path
    // here we do not use the newPath directly, so that even if we fail to move some of the
    // newPaths, we are still safe because every time we will get all the files under the archive
    // directory.
    try {
      MasterRegionUtils.moveFilesUnderDir(fs, walArchiveDir, globalWALArchiveDir,
        archivedWALSuffix);
    } catch (IOException e) {
      LOG.warn("Failed to move archived wals from {} to global dir {}", walArchiveDir,
        globalWALArchiveDir, e);
    }
  }

  @Override
  protected void scheduleFlush(String encodedRegionName, List<byte[]> families) {
    MasterRegionFlusherAndCompactor flusher = this.flusherAndCompactor;
    if (flusher != null) {
      flusher.requestFlush();
    }
  }

  void setFlusherAndCompactor(MasterRegionFlusherAndCompactor flusherAndCompactor) {
if(KnobRuntime.check(java.util.UUID.fromString("fad0b4cc-9f49-3a0a-a935-6f42edf1b454"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba7d8fc6-b233-3a40-a9a8-511703e89121"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba4b51e2-aee5-3523-9bb5-c456fc432d88"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46d2f18c-df58-30d4-9603-fe03443d8ed4"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("closed");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(flusherAndCompactor);
    field.set(flusherAndCompactor, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20cc73c5-c19f-3515-955e-bdd4b398f69e"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushIntervalMs");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b32a3635-bfe8-3878-9e1b-5cd57a60def1"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactRequest");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(flusherAndCompactor);
    field.set(flusherAndCompactor, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a754bd9b-19ce-3229-815d-ba0d5cfaaa00"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f4ebb17-530e-387a-9a73-fcd7ebaa0501"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushRequest");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(flusherAndCompactor);
    field.set(flusherAndCompactor, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb2bac73-0e77-3b38-88a4-9b3a661bf4f0"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03b09370-72c7-3661-8b14-7aa5a64573d3"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("194fde49-db2c-32ec-a3a0-7c4bf0bdbafb"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c573d172-90ca-3198-be0c-4cb916462f4d"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("lastFlushTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.flusherAndCompactor = flusherAndCompactor;
  }

  static MasterRegionWALRoller create(String name, Configuration conf, Abortable abortable,
    FileSystem fs, Path walRootDir, Path globalWALRootDir, String archivedWALSuffix,
    long rollPeriodMs, long flushSize) {
    // we can not run with wal disabled, so force set it to true.
    conf.setBoolean(WALFactory.WAL_ENABLED, true);
    // we do not need this feature, so force disable it.
    conf.setBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR, false);
if(KnobRuntime.check(java.util.UUID.fromString("7f63400f-9ec7-368a-8e45-c276d59c66d8"))) {
rollPeriodMs = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cdc41ca7-6008-3baa-8bf8-1fb637136d44"))) {
rollPeriodMs += 1;
}
    conf.setLong(WAL_ROLL_PERIOD_KEY, rollPeriodMs);
    // make the roll size the same with the flush size, as we only have one region here
    if (KnobRuntime.check(java.util.UUID.fromString("bea64240-6240-3724-82c4-cc39af851ac1"))) { conf.setLong(WALUtil.WAL_BLOCK_SIZE, (flushSize) * (2)); } else if (KnobRuntime.check(java.util.UUID.fromString("d7bbf2fe-0d39-36c7-903d-c78d46018849"))) { conf.setLong(WALUtil.WAL_BLOCK_SIZE, (flushSize * 2) * (2)); } else if (KnobRuntime.check(java.util.UUID.fromString("3aeac9c8-d557-36c0-afcb-296af74c409c"))) { conf.setLong(WALUtil.WAL_BLOCK_SIZE, flushSize); } else { conf.setLong(WALUtil.WAL_BLOCK_SIZE, flushSize * 2); }
    conf.setFloat(AbstractFSWAL.WAL_ROLL_MULTIPLIER, 0.5f);
    return new MasterRegionWALRoller(name, conf, abortable, fs, walRootDir, globalWALRootDir,
      archivedWALSuffix);
  }

}
