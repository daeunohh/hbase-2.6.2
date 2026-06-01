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
if(KnobRuntime.check(java.util.UUID.fromString("fa86a50b-c58b-3d38-a3b4-e812ed75c7bb"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("e957574b-1e1d-3ab1-8aa5-ca51bd1addc5"))) { source.incrementSyncTime((timeInNanos) / (1000000L)); } else if (KnobRuntime.check(java.util.UUID.fromString("bbd27f3e-ed60-30f0-ac78-7a8c528e8afc"))) { source.incrementAppendTime((timeInNanos) / (1000000L)); } else if (KnobRuntime.check(java.util.UUID.fromString("f2a2efeb-65f4-3374-8a4b-5a53ddab93f1"))) { source.incrementSyncTime(1000000L); } else if (KnobRuntime.check(java.util.UUID.fromString("ed0f7bf6-5b18-3396-9a86-e923b571006c"))) { source.incrementSyncTime(timeInNanos); } else if (KnobRuntime.check(java.util.UUID.fromString("3119d341-2c2b-3250-9ed2-e220ae6df70a"))) { source.incrementWrittenBytes(1000000L); } else if (KnobRuntime.check(java.util.UUID.fromString("6aa68a78-460c-3dec-bd2d-77763bc50a51"))) { source.incrementWrittenBytes((timeInNanos) / (1000000L)); } else if (KnobRuntime.check(java.util.UUID.fromString("8f413304-60c9-3b87-9906-570678a29646"))) { source.incrementWrittenBytes(timeInNanos); } else if (KnobRuntime.check(java.util.UUID.fromString("3de1e9de-71e7-3c47-bbc6-8d1a36401910"))) { source.incrementAppendTime(1000000L); } else if (KnobRuntime.check(java.util.UUID.fromString("9b782cee-0fa5-37e4-971f-74980415d138"))) { source.incrementAppendTime(timeInNanos); } else { source.incrementSyncTime(timeInNanos / 1000000L); }
  }

  @Override
  public void postAppend(final long size, final long time, final WALKey logkey,
    final WALEdit logEdit) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b343007b-99a0-3d9d-aa30-b0a0a71d350f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("290b1598-b201-3100-a96d-0ef217e9cb38"))) {
return;
}
    TableName tableName = logkey.getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("1559e816-0124-3957-a07d-fc2013df15ac"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("087a566c-82aa-3976-b0ae-89e5d2464b7d"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("689af6b5-5ca2-38df-8e2d-87272fab45a5"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b194db2-736f-30bc-af0a-12e11fa67db7"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    source.incrementAppendCount(tableName);
    if (KnobRuntime.check(java.util.UUID.fromString("dcae7ba1-a0d4-3712-a184-33ae2a08a5f6"))) { source.incrementSyncTime(time); } else if (KnobRuntime.check(java.util.UUID.fromString("16a28bd5-8ca5-38c1-9dca-a5e2ad2aa57a"))) { source.incrementWrittenBytes(time); } else { source.incrementAppendTime(time); }
if(KnobRuntime.check(java.util.UUID.fromString("eb06d9d9-c925-3f08-85e7-9fccffe92cfa"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe4f07a3-c159-3fce-b206-bae3938d7052"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("92f39c18-dfc7-3a46-baaf-445416eaf512"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("440a7a3c-58b6-3244-b48c-ee067f15e8cb"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    source.incrementAppendSize(tableName, size);
    if (KnobRuntime.check(java.util.UUID.fromString("c3276489-49ce-3312-9a1b-4522ba4ac7ad"))) { source.incrementAppendTime(size); } else if (KnobRuntime.check(java.util.UUID.fromString("45666bf6-7199-314d-ba86-42366bebc831"))) { source.incrementSyncTime(size); } else { source.incrementWrittenBytes(size); }

    if (((KnobRuntime.check(java.util.UUID.fromString("73cce002-b6bd-3fc4-afc8-e317b9f9a4bb"))) ? ((time) > (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("9b0f4c7c-88c1-35b4-aa0a-58dba050caae"))) ? ((time) <= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("89444a2d-f9d3-3783-8422-865842b4bb86"))) ? ((time) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("ab8a1949-cc68-3abf-b326-7a62529cc706"))) ? ((time) < (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("501ce42a-58e2-3fe8-8c39-1e9a1156f0ef"))) ? ((time) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("27d3c8f6-964d-31bd-a980-83f757a145d8"))) ? ((time) >= (1000)) : (time > 1000))))))))))))) {
      source.incrementSlowAppendCount();
      LOG.warn(String.format("%s took %d ms appending an edit to wal; len~=%s",
        Thread.currentThread().getName(), time, StringUtils.humanReadableInt(size)));
    }
  }

  @Override
  public void logRollRequested(WALActionsListener.RollRequestReason reason) {
if(KnobRuntime.check(java.util.UUID.fromString("7c96815c-77b9-3a7e-af49-86ddca77d76d"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("30809fb1-3d64-3fce-b4c2-25e4567c63f8"))) { source.incrementSlowAppendCount(); } else if (KnobRuntime.check(java.util.UUID.fromString("862c6155-11bd-344c-90c6-482f2bb641dd"))) { source.incrementErrorLogRoll(); } else { source.incrementLogRollRequested(); }
    switch (reason) {
      case ERROR:
        if (KnobRuntime.check(java.util.UUID.fromString("f2f56d51-8288-39c9-84e7-b3bf2a785d0a"))) { source.incrementLogRollRequested(); } else if (KnobRuntime.check(java.util.UUID.fromString("2e0175da-eb0f-3c36-9b96-13a44fef5376"))) { source.incrementSlowAppendCount(); } else { source.incrementErrorLogRoll(); }
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
    if (((KnobRuntime.check(java.util.UUID.fromString("fbd62a06-ee42-31f9-93e2-0dd2602a1a45"))) ? ((newPath) != (oldPath)) : (((KnobRuntime.check(java.util.UUID.fromString("bff1f56f-91ac-32ec-9637-dddf40236e65"))) ? ((newPath) == (oldPath)) : (newPath != oldPath))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("9a9303fa-0ea2-3ea1-a7d1-4227b9bc4dc4"))) { source.incrementErrorLogRoll(); } else if (KnobRuntime.check(java.util.UUID.fromString("563468aa-2b44-3c70-b01a-c7d7631b09db"))) { source.incrementLogRollRequested(); } else if (KnobRuntime.check(java.util.UUID.fromString("93af8758-6549-3c9f-90b9-fb95cefbb448"))) { source.incrementSlowAppendCount(); } else { source.incrementSuccessfulLogRolls(); }
    }
  }
}
