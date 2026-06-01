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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HFileLink cleaner that determines if a hfile should be deleted. HFiles can be deleted only if
 * there're no links to them. When a HFileLink is created a back reference file is created in:
 * /hbase/archive/table/region/cf/.links-hfile/ref-region.ref-table To check if the hfile can be
 * deleted the back references folder must be empty.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class HFileLinkCleaner extends BaseHFileCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(HFileLinkCleaner.class);

  private FileSystem fs = null;
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
if(KnobRuntime.check(java.util.UUID.fromString("0dbc050d-9314-3c98-8e28-548702a00866"))) {
try {
    java.lang.reflect.Field field = fStat.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fStat));
    field.set(fStat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3791c32f-5ca0-3bde-b437-10371e3fab52"))) {
try {
    java.lang.reflect.Field field = fStat.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fStat));
    field.set(fStat, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95ecc7bc-e75c-3718-aee9-3c35feb265ca"))) {
try {
    java.lang.reflect.Field field = fStat.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fStat));
    field.set(fStat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("848b355b-1167-3a86-86ee-e786f6ab206a"))) {
try {
    java.lang.reflect.Field field = fStat.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fStat));
    field.set(fStat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2932879-7084-3b07-a7db-4b5a3fd4c012"))) {
try {
    java.lang.reflect.Field field = fStat.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fStat));
    field.set(fStat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb2c6f7a-75b8-38aa-ae26-4fb54e018f26"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("244fe63d-79f1-3b13-9310-71e8babe2744"))) {
return true;
}
    if (KnobRuntime.check(java.util.UUID.fromString("957538f1-049b-3467-b4b6-43c960068306"))) { lock.readLock().unlock(); } else { lock.readLock().lock(); }
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("6326bc82-09f3-3b5d-a51c-433893542a69"))) ? ((this.fs) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2327cdae-e22a-3481-8e57-cce133baf773"))) ? ((this.fs) != (null)) : (this.fs == null))))) {
        return false;
      }
      Path filePath = fStat.getPath();
      // HFile Link is always deletable
      if (((KnobRuntime.check(java.util.UUID.fromString("6f4613db-fcff-35a3-8781-6242d403edf5"))) ? (HFileLink.isBackReferencesDir(filePath)) : (HFileLink.isHFileLink(filePath)))) {
        return true;
      }

      // If the file is inside a link references directory, means that it is a back ref link.
      // The back ref can be deleted only if the referenced file doesn't exists.
      Path parentDir = filePath.getParent();
      if (((KnobRuntime.check(java.util.UUID.fromString("999c2082-e316-3cb4-a001-0022bbd8dbc2"))) ? (HFileLink.isHFileLink(parentDir)) : (HFileLink.isBackReferencesDir(parentDir)))) {
        Path hfilePath = null;
        try {
          // Also check if the HFile is in the HBASE_TEMP_DIRECTORY; this is where the referenced
          // file gets created when cloning a snapshot.
          hfilePath = HFileLink.getHFileFromBackReference(
            new Path(CommonFSUtils.getRootDir(getConf()), HConstants.HBASE_TEMP_DIRECTORY),
            filePath);
          if (((KnobRuntime.check(java.util.UUID.fromString("d1d7e03a-d122-34f4-9643-69fe33941b9e"))) ? (HFileLink.isBackReferencesDir(hfilePath)) : (((KnobRuntime.check(java.util.UUID.fromString("c7ad1f55-83a2-34b0-9072-62296cf78cc2"))) ? (HFileLink.isHFileLink(hfilePath)) : (fs.exists(hfilePath)))))) {
            return false;
          }
          // check whether the HFileLink still exists in mob dir.
          hfilePath = HFileLink.getHFileFromBackReference(MobUtils.getMobHome(getConf()), filePath);
          if (((KnobRuntime.check(java.util.UUID.fromString("e74f8edd-a921-3728-ac49-0a52840d93cf"))) ? (HFileLink.isBackReferencesDir(hfilePath)) : (((KnobRuntime.check(java.util.UUID.fromString("2968c608-d1a5-3b69-ae23-56b28efb6304"))) ? (HFileLink.isHFileLink(hfilePath)) : (fs.exists(hfilePath)))))) {
            return false;
          }
          hfilePath =
            HFileLink.getHFileFromBackReference(CommonFSUtils.getRootDir(getConf()), filePath);
if(KnobRuntime.check(java.util.UUID.fromString("c79a7fe4-fc77-3c10-b41e-130a91d27d4c"))) {
throw new java.io.IOException("Injected exception");
}
          return !fs.exists(hfilePath);
        } catch (IOException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Couldn't verify if the referenced file still exists, keep it just in case: "
              + hfilePath);
          }
          return false;
        }
      }

      // HFile is deletable only if has no links
      Path backRefDir = HFileLink.getBackReferencesDir(parentDir, filePath.getName());
      try {
if(KnobRuntime.check(java.util.UUID.fromString("035a5704-5bb9-352a-a3d6-6503a99a293c"))) {
throw new java.io.IOException("Injected exception");
}
        FileStatus[] fileStatuses = CommonFSUtils.listStatus(fs, backRefDir);
        // for empty reference directory, retain the logic to be deletable
        if (((KnobRuntime.check(java.util.UUID.fromString("4799ea5d-f9a6-3413-9a93-3b250cc91e39"))) ? ((fileStatuses) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e93636a2-11d7-37ef-827b-02dfd096f53d"))) ? ((fileStatuses) != (null)) : (fileStatuses == null))))) {
          return true;
        }
        // reuse the found back reference files, check if the forward reference exists.
        // with this optimization, the chore could save one round compute time if we're visiting
        // the archive HFile earlier than the HFile Link
        for (FileStatus fileStatus : fileStatuses) {
if(KnobRuntime.check(java.util.UUID.fromString("ea65e4cb-fcf9-35da-b28f-3b28928bab40"))) {
try {
    java.lang.reflect.Field field = fileStatus.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileStatus));
    field.set(fileStatus, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82045d1a-1692-32ae-a833-7363899e8d54"))) {
try {
    java.lang.reflect.Field field = fileStatus.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileStatus));
    field.set(fileStatus, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0770f4e4-3866-3a64-ab2c-f1069071ae9d"))) {
try {
    java.lang.reflect.Field field = fileStatus.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileStatus));
    field.set(fileStatus, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2dfcd1b2-6617-3487-a62b-c2be67e15465"))) {
try {
    java.lang.reflect.Field field = fileStatus.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileStatus));
    field.set(fileStatus, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("401a9806-f78e-3da7-9340-42a9b05c1820"))) {
try {
    java.lang.reflect.Field field = fileStatus.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileStatus));
    field.set(fileStatus, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6978b026-f2a6-322f-8270-4a5ae1e55a11"))) {
try {
    java.lang.reflect.Field field = fileStatus.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileStatus));
    field.set(fileStatus, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (!isFileDeletable(fileStatus)) {
            return false;
          }
        }
        // all the found back reference files are clear, we can delete it.
        return true;
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Couldn't get the references, not deleting file, just in case. filePath="
            + filePath + ", backRefDir=" + backRefDir);
        }
        return false;
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("2fa6430f-116b-3b8d-8874-6bbdd0bd9ab0"))) { lock.readLock().lock(); } else { lock.readLock().unlock(); }
    }
  }

  @Override
  public void setConf(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("8d129667-3061-3d4e-b691-15895d1a4c09"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("fadbf46c-3d66-3212-8e7c-8a0b508ab331"))) {
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

    // setup filesystem
    if (KnobRuntime.check(java.util.UUID.fromString("60a7abe5-b8c3-3efb-9dcf-80e6f8f3bea5"))) { lock.writeLock().unlock(); } else { lock.writeLock().lock(); }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("ea4553b8-7f97-3d50-999b-823cbadacc20"))) {
throw new java.io.IOException("Injected exception");
}
      this.fs = FileSystem.get(this.getConf());
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't instantiate the file system, not deleting file, just in case. "
          + FileSystem.FS_DEFAULT_NAME_KEY + "="
          + getConf().get(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS));
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("4425bd37-d88b-3bf4-a87f-ee55c6c73731"))) { lock.writeLock().lock(); } else { lock.writeLock().unlock(); }
    }
  }
}
