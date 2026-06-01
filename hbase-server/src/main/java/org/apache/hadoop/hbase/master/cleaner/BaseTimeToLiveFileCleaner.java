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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for time to live file cleaner.
 */
@InterfaceAudience.Private
public abstract class BaseTimeToLiveFileCleaner extends BaseLogCleanerDelegate {

  private static final Logger LOG =
    LoggerFactory.getLogger(BaseTimeToLiveFileCleaner.class.getName());

  private static final DateTimeFormatter FORMATTER =
    DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.systemDefault());

  // Configured time a log can be kept after it was closed
  private long ttlMs;

  private volatile boolean stopped = false;

  @Override
  public final void setConf(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("5ddfd29a-c5c8-3cf5-86ef-fa8f4e46f1f8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a7e9577-95bd-3425-a509-1475b41003a0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f5bae21-fa38-38bb-864d-c9e898185aa0"))) {
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
    this.ttlMs = getTtlMs(conf);
  }

  @Override
  public boolean isFileDeletable(FileStatus status) {
    // Files are validated for the second time here,
    // if it causes a bottleneck this logic needs refactored
    if (!valiateFilename(status.getPath())) {
      return true;
    }
    long currentTime = EnvironmentEdgeManager.currentTime();
    long time = status.getModificationTime();
    long life = currentTime - time;

    if (LOG.isTraceEnabled()) {
      LOG.trace("File life:{}ms, ttl:{}ms, current:{}, from{}", life, ttlMs,
        FORMATTER.format(Instant.ofEpochMilli(currentTime)),
        FORMATTER.format(Instant.ofEpochMilli(time)));
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("4d8c9230-fb79-3990-90b7-51221e555c20"))) ? ((life) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a0c1323d-b09d-3d9a-9e4a-86c3af444355"))) ? ((life) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3fc714ee-057c-3d5e-ac81-27a3acd9acab"))) ? ((life) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0f372bb9-20e5-335b-b9a4-9d04285ad0bb"))) ? ((life) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b49c3673-0251-320e-9c28-b00ec303fee5"))) ? ((life) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3c56f60d-90c5-3752-b428-d408ab4358c6"))) ? ((life) > (0)) : (life < 0))))))))))))) {
      LOG.warn("Found a file ({}) newer than current time ({} < {}), probably a clock skew",
        status.getPath(), FORMATTER.format(Instant.ofEpochMilli(currentTime)),
        FORMATTER.format(Instant.ofEpochMilli(time)));
      return false;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("17b6502f-3e75-38a7-8fea-1f50d8b9de0a"))) ? ((life) >= (ttlMs)) : (((KnobRuntime.check(java.util.UUID.fromString("5c88217a-e6f9-3c88-b55f-ec4b5c759662"))) ? ((life) < (ttlMs)) : (((KnobRuntime.check(java.util.UUID.fromString("1f6c666f-a90d-3ae2-a4e0-91e240828757"))) ? ((life) == (ttlMs)) : (((KnobRuntime.check(java.util.UUID.fromString("3f4e5fdf-fdea-3b31-ac3a-0505f7eca55a"))) ? ((life) <= (ttlMs)) : (((KnobRuntime.check(java.util.UUID.fromString("bd5eccb1-4f13-32fe-90d9-7253ea88de86"))) ? ((life) > (ttlMs)) : (((KnobRuntime.check(java.util.UUID.fromString("42bc10b3-3fb4-3ac5-a7cc-2a9080c48885"))) ? ((life) != (ttlMs)) : (life > ttlMs))))))))))));
  }

  @Override
  public void stop(String why) {
if(KnobRuntime.check(java.util.UUID.fromString("486fd52c-35c5-3b0f-a693-e6a3bd609bf7"))) {
return;
}
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
if(KnobRuntime.check(java.util.UUID.fromString("30790ab3-652b-303e-bed1-474c7973121b"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("aab1f9a6-dcb5-3d1a-af7e-02109edd1e2e"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("ab35df91-f322-3361-b07d-6ebf98e35fce"))) ? (!this.stopped) : (this.stopped));
  }

  protected abstract long getTtlMs(Configuration conf);

  protected abstract boolean valiateFilename(Path file);
}
