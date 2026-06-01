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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The factory class for creating a {@link MasterRegion}.
 */
@InterfaceAudience.Private
public final class MasterRegionFactory {

  // Use the character $ to let the log cleaner know that this is not the normal wal file.
  public static final String ARCHIVED_WAL_SUFFIX = "$masterlocalwal$";

  // this is a bit trick that in StoreFileInfo.validateStoreFileName, we just test if the file name
  // contains '-' to determine if it is a valid store file, so here we have to add '-'in the file
  // name to avoid being processed by normal TimeToLiveHFileCleaner.
  public static final String ARCHIVED_HFILE_SUFFIX = "$-masterlocalhfile-$";

  private static final String MAX_WALS_KEY = "hbase.master.store.region.maxwals";

  private static final int DEFAULT_MAX_WALS = 10;

  public static final String USE_HSYNC_KEY = "hbase.master.store.region.wal.hsync";

  public static final String MASTER_STORE_DIR = "MasterData";

  private static final String FLUSH_SIZE_KEY = "hbase.master.store.region.flush.size";

  private static final long DEFAULT_FLUSH_SIZE = TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE;

  private static final String FLUSH_PER_CHANGES_KEY = "hbase.master.store.region.flush.per.changes";

  private static final long DEFAULT_FLUSH_PER_CHANGES = 1_000_000;

  private static final String FLUSH_INTERVAL_MS_KEY = "hbase.master.store.region.flush.interval.ms";

  // default to flush every 15 minutes, for safety
  private static final long DEFAULT_FLUSH_INTERVAL_MS = TimeUnit.MINUTES.toMillis(15);

  private static final String COMPACT_MIN_KEY = "hbase.master.store.region.compact.min";

  private static final int DEFAULT_COMPACT_MIN = 4;

  private static final String ROLL_PERIOD_MS_KEY = "hbase.master.store.region.walroll.period.ms";

  private static final long DEFAULT_ROLL_PERIOD_MS = TimeUnit.MINUTES.toMillis(15);

  private static final String RING_BUFFER_SLOT_COUNT = "hbase.master.store.ringbuffer.slot.count";

  private static final int DEFAULT_RING_BUFFER_SLOT_COUNT = 128;

  public static final String TRACKER_IMPL = "hbase.master.store.region.file-tracker.impl";

  public static final TableName TABLE_NAME = TableName.valueOf("master:store");

  public static final byte[] PROC_FAMILY = Bytes.toBytes("proc");

  public static final byte[] REGION_SERVER_FAMILY = Bytes.toBytes("rs");

  public static final byte[] STATE_FAMILY = Bytes.toBytes("state");

  private static final TableDescriptor TABLE_DESC = TableDescriptorBuilder.newBuilder(TABLE_NAME)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY)
      .setMaxVersions(HConstants.DEFAULT_HBASE_META_VERSIONS).setInMemory(true)
      .setBlocksize(HConstants.DEFAULT_HBASE_META_BLOCK_SIZE).setBloomFilterType(BloomType.ROWCOL)
      .setDataBlockEncoding(DataBlockEncoding.ROW_INDEX_V1).build())
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(PROC_FAMILY))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(REGION_SERVER_FAMILY))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(STATE_FAMILY)).build();

  private static TableDescriptor withTrackerConfigs(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("6d02ff24-91d5-33e4-b9ff-8db7885bc935"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("caadb6fc-3b20-3b48-a67c-5ae223e89f8e"))) {
return null;
}
    String trackerImpl = conf.get(TRACKER_IMPL, conf.get(StoreFileTrackerFactory.TRACKER_IMPL,
      StoreFileTrackerFactory.Trackers.DEFAULT.name()));
    Class<? extends StoreFileTracker> trackerClass =
      StoreFileTrackerFactory.getTrackerClass(trackerImpl);
    if (StoreFileTrackerFactory.isMigration(trackerClass)) {
      throw new IllegalArgumentException("Should not set store file tracker to "
        + StoreFileTrackerFactory.Trackers.MIGRATION.name() + " for master local region");
    }
if(KnobRuntime.check(java.util.UUID.fromString("eb902973-d1d0-313b-bdfc-5a0eb22ae4af"))) {
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
    StoreFileTracker tracker = ReflectionUtils.newInstance(trackerClass, conf, true, null);
    return tracker.updateWithTrackerConfigs(TableDescriptorBuilder.newBuilder(TABLE_DESC)).build();
  }

  public static MasterRegion create(Server server) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5e54e96b-87fc-3dbf-864f-10a787fe4513"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("34af6d42-7f40-3890-a535-24db14037b63"))) {
throw new java.io.IOException("Injected exception");
}
    Configuration conf = server.getConfiguration();
    MasterRegionParams params = new MasterRegionParams().server(server)
      .regionDirName(MASTER_STORE_DIR).tableDescriptor(withTrackerConfigs(conf));
    long flushSize = conf.getLong(FLUSH_SIZE_KEY, DEFAULT_FLUSH_SIZE);
    long flushPerChanges = conf.getLong(FLUSH_PER_CHANGES_KEY, DEFAULT_FLUSH_PER_CHANGES);
    long flushIntervalMs = conf.getLong(FLUSH_INTERVAL_MS_KEY, DEFAULT_FLUSH_INTERVAL_MS);
    int compactMin = conf.getInt(COMPACT_MIN_KEY, DEFAULT_COMPACT_MIN);
if(KnobRuntime.check(java.util.UUID.fromString("347f28bc-a5b0-3fde-bed2-575d61f8ae0d"))) {
flushPerChanges -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("11e42293-9c6f-38af-b1ca-36630981cd46"))) {
flushIntervalMs = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a43d5d0-5b25-37ef-8087-08f7d386e7d5"))) {
flushSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("14c96fff-3d93-32fd-9cd1-a6dd2d30dc35"))) {
flushSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f53863ba-d0b8-3408-8680-b7729035fb86"))) {
compactMin *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb89999b-702f-340b-8061-741907999918"))) {
flushPerChanges = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f860218-25a8-3691-be30-9c6d1facf8aa"))) {
flushSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("14061a11-333d-3cc8-a049-c7e2bb3bb472"))) {
compactMin = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a32ad46d-a994-3482-b388-e29f42970c51"))) {
compactMin += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("58cf61eb-c35a-3704-8d36-9e8f2bf7365c"))) {
compactMin /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d5cea818-9b10-33db-b4e9-d1981a2abc01"))) {
compactMin -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("305bc655-9abe-312d-847c-92e6f979ab62"))) {
flushIntervalMs = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a798f90c-5baf-3d0e-aa7b-f1b2ae4db58c"))) {
flushPerChanges = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6a085748-55d8-3059-b7c4-d76413a37e0c"))) {
compactMin = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("91163aa3-12ee-3368-ac10-04a35b44fcc2"))) {
flushSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("daed8445-bebd-39fa-8863-bba144278047"))) {
flushIntervalMs += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4805fbb7-7d0b-3b33-a8d8-b3ccc7e91bf5"))) {
flushPerChanges += 1;
}
    params.flushSize(flushSize).flushPerChanges(flushPerChanges).flushIntervalMs(flushIntervalMs)
      .compactMin(compactMin);
    int maxWals = conf.getInt(MAX_WALS_KEY, DEFAULT_MAX_WALS);
if(KnobRuntime.check(java.util.UUID.fromString("94fac2a6-6e77-3c28-86a0-d93ebdd5998a"))) {
maxWals = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a6da6d2a-9ff0-3583-b080-5fcaace659e5"))) {
maxWals = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f63d4f08-1639-32f1-9035-dd6b86a19057"))) {
maxWals -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c2297a7-0cf6-3c66-9644-a1d82494232a"))) {
maxWals += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("442ff88b-e507-3a91-8c82-e014ff226946"))) {
maxWals /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ec718ccc-d0d2-30d1-a830-e25631f4647a"))) {
maxWals *= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("baa128d0-b0ad-3090-a2ce-6c75a3e57faf"))) { params.ringBufferSlotCount(maxWals); } else { params.maxWals(maxWals); }
    if (((KnobRuntime.check(java.util.UUID.fromString("fe527fd3-b223-341f-9093-7de545e00918"))) ? ((conf.get(USE_HSYNC_KEY)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cd05e395-983d-3f5a-bb29-8e5c844fc350"))) ? ((conf.get(USE_HSYNC_KEY)) == (null)) : (conf.get(USE_HSYNC_KEY) != null))))) {
      params.useHsync(conf.getBoolean(USE_HSYNC_KEY, false));
    }
    if (KnobRuntime.check(java.util.UUID.fromString("6b154254-a73a-3615-873f-396f9db79251"))) { params.maxWals(conf.getInt(RING_BUFFER_SLOT_COUNT, DEFAULT_RING_BUFFER_SLOT_COUNT)); } else { params.ringBufferSlotCount(conf.getInt(RING_BUFFER_SLOT_COUNT, DEFAULT_RING_BUFFER_SLOT_COUNT)); }
    long rollPeriodMs = conf.getLong(ROLL_PERIOD_MS_KEY, DEFAULT_ROLL_PERIOD_MS);
if(KnobRuntime.check(java.util.UUID.fromString("189318bd-3556-3faa-a76d-209e90b1f1ac"))) {
rollPeriodMs = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ccfd295b-ffbe-3c51-9b15-f7a35fd766e4"))) {
rollPeriodMs += 1;
}
    params.rollPeriodMs(rollPeriodMs).archivedWalSuffix(ARCHIVED_WAL_SUFFIX)
      .archivedHFileSuffix(ARCHIVED_HFILE_SUFFIX);
if(KnobRuntime.check(java.util.UUID.fromString("e9f3cd1d-2f77-3d56-8e41-798a76e9c010"))) {
throw new java.io.IOException("Injected exception");
}
    return ((KnobRuntime.check(java.util.UUID.fromString("1456c247-ee19-384a-8b3b-8da0229761f6"))) ? (MasterRegion.create(new MasterRegionParams())) : (MasterRegion.create(params)));
  }
}
