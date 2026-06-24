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
if(KnobRuntime.check(java.util.UUID.fromString("244fe63d-79f1-3b13-9310-71e8babe2744"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("55596e77-c542-3f35-8e88-3b90bdf41944"))) {
try {
    java.lang.reflect.Field _knob_field_ = fStat.getClass().getDeclaredField("blocksize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(fStat));
    _knob_field_.set(fStat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    lock.readLock().lock();
    try {
      if (this.fs == null) {
        return false;
      }
      Path filePath = fStat.getPath();
      // HFile Link is always deletable
      if (HFileLink.isHFileLink(filePath)) {
        return true;
      }

      // If the file is inside a link references directory, means that it is a back ref link.
      // The back ref can be deleted only if the referenced file doesn't exists.
      Path parentDir = filePath.getParent();
      if (HFileLink.isBackReferencesDir(parentDir)) {
        Path hfilePath = null;
        try {
          // Also check if the HFile is in the HBASE_TEMP_DIRECTORY; this is where the referenced
          // file gets created when cloning a snapshot.
          hfilePath = HFileLink.getHFileFromBackReference(
            new Path(CommonFSUtils.getRootDir(getConf()), HConstants.HBASE_TEMP_DIRECTORY),
            filePath);
          if (fs.exists(hfilePath)) {
            return false;
          }
          // check whether the HFileLink still exists in mob dir.
          hfilePath = HFileLink.getHFileFromBackReference(MobUtils.getMobHome(getConf()), filePath);
          if (fs.exists(hfilePath)) {
            return false;
          }
          hfilePath =
            HFileLink.getHFileFromBackReference(CommonFSUtils.getRootDir(getConf()), filePath);
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
        FileStatus[] fileStatuses = CommonFSUtils.listStatus(fs, backRefDir);
        // for empty reference directory, retain the logic to be deletable
        if (fileStatuses == null) {
          return true;
        }
        // reuse the found back reference files, check if the forward reference exists.
        // with this optimization, the chore could save one round compute time if we're visiting
        // the archive HFile earlier than the HFile Link
        for (FileStatus fileStatus : fileStatuses) {
if(KnobRuntime.check(java.util.UUID.fromString("e3969682-81e3-3006-a536-00515eafe70a"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileStatus.getClass().getDeclaredField("access_time");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(fileStatus));
    _knob_field_.set(fileStatus, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c905566d-1f31-37be-a865-0eada55f8de1"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileStatus.getClass().getDeclaredField("access_time");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(fileStatus));
    _knob_field_.set(fileStatus, oldValue - 1);
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
      lock.readLock().unlock();
    }
  }

  @Override
  public void setConf(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("84aed757-548c-39e2-b0e1-ef6d9b90fc9b"))) {
try {
    java.lang.reflect.Field _knob_field_ = conf.getClass().getDeclaredField("loadDefaults");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(conf);
    _knob_field_.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    super.setConf(conf);

    // setup filesystem
    if (KnobRuntime.check(java.util.UUID.fromString("60a7abe5-b8c3-3efb-9dcf-80e6f8f3bea5"))) { lock.writeLock().unlock(); } else { lock.writeLock().lock(); }
    try {
      this.fs = FileSystem.get(this.getConf());
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Couldn't instantiate the file system, not deleting file, just in case. "
          + FileSystem.FS_DEFAULT_NAME_KEY + "="
          + getConf().get(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS));
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
