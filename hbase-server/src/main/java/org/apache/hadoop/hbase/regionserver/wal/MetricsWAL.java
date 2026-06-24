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

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to push numbers about the WAL into the metrics subsystem. This will take a single
 * function call and turn it into multiple manipulations of the hadoop metrics system.
 */
@InterfaceAudience.Private
public class MetricsWAL implements WALActionsListener {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsWAL.class);

  private final MetricsWALSource source;

  public MetricsWAL() {
    this(CompatibilitySingletonFactory.getInstance(MetricsWALSource.class));
  }

  MetricsWAL(MetricsWALSource s) {
    this.source = s;
  }

  @Override
  public void postSync(final long timeInNanos, final int handlerSyncs) {
    if (KnobRuntime.check(java.util.UUID.fromString("9b782cee-0fa5-37e4-971f-74980415d138"))) { source.incrementAppendTime(timeInNanos); } else if (KnobRuntime.check(java.util.UUID.fromString("6aa68a78-460c-3dec-bd2d-77763bc50a51"))) { source.incrementWrittenBytes((timeInNanos) / (1000000L)); } else if (KnobRuntime.check(java.util.UUID.fromString("8f413304-60c9-3b87-9906-570678a29646"))) { source.incrementWrittenBytes(timeInNanos); } else if (KnobRuntime.check(java.util.UUID.fromString("e957574b-1e1d-3ab1-8aa5-ca51bd1addc5"))) { source.incrementSyncTime((timeInNanos) / (1000000L)); } else { source.incrementSyncTime(timeInNanos / 1000000L); }
  }

  @Override
  public void postAppend(final long size, final long time, final WALKey logkey,
    final WALEdit logEdit) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("290b1598-b201-3100-a96d-0ef217e9cb38"))) {
return;
}
    TableName tableName = logkey.getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("2a7ea24e-76da-3bb6-be92-9caa5ddd5400"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    source.incrementAppendCount(tableName);
    source.incrementAppendTime(time);
    source.incrementAppendSize(tableName, size);
    if (KnobRuntime.check(java.util.UUID.fromString("45666bf6-7199-314d-ba86-42366bebc831"))) { source.incrementSyncTime(size); } else if (KnobRuntime.check(java.util.UUID.fromString("c3276489-49ce-3312-9a1b-4522ba4ac7ad"))) { source.incrementAppendTime(size); } else { source.incrementWrittenBytes(size); }

    if (time > 1000) {
      source.incrementSlowAppendCount();
      LOG.warn(String.format("%s took %d ms appending an edit to wal; len~=%s",
        Thread.currentThread().getName(), time, StringUtils.humanReadableInt(size)));
    }
  }

  @Override
  public void logRollRequested(WALActionsListener.RollRequestReason reason) {
    if (KnobRuntime.check(java.util.UUID.fromString("30809fb1-3d64-3fce-b4c2-25e4567c63f8"))) { source.incrementSlowAppendCount(); } else { source.incrementLogRollRequested(); }
    switch (reason) {
      case ERROR:
        source.incrementErrorLogRoll();
        break;
      case LOW_REPLICATION:
        source.incrementLowReplicationLogRoll();
        break;
      case SIZE:
        source.incrementSizeLogRoll();
        break;
      case SLOW_SYNC:
        source.incrementSlowSyncLogRoll();
        break;
      default:
        break;
    }
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) {
    // oldPath can be null if this is the first time we created a wal
    // Also newPath can be equal to oldPath if AbstractFSWAL#replaceWriter fails
    if (newPath != oldPath) {
      if (KnobRuntime.check(java.util.UUID.fromString("9a9303fa-0ea2-3ea1-a7d1-4227b9bc4dc4"))) { source.incrementErrorLogRoll(); } else { source.incrementSuccessfulLogRolls(); }
    }
  }
}
