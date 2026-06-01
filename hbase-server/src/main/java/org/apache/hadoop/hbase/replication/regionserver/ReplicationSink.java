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
package org.apache.hadoop.hbase.replication.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.OFFSET_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_INFO_FAMILY;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.REPLICATION_SINK_TRACKER_TABLE_NAME;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.RS_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.TIMESTAMP_COLUMN;
import static org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator.WAL_NAME_COLUMN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * <p>
 * This class is responsible for replicating the edits coming from another cluster.
 * </p>
 * <p>
 * This replication process is currently waiting for the edits to be applied before the method can
 * return. This means that the replication of edits is synchronized (after reading from WALs in
 * ReplicationSource) and that a single region server cannot receive edits from two sources at the
 * same time
 * </p>
 * <p>
 * This class uses the native HBase client in order to replicate entries.
 * </p>
 * TODO make this class more like ReplicationSource wrt log handling
 */
@InterfaceAudience.Private
public class ReplicationSink {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSink.class);
  private final Configuration conf;
  // Volatile because of note in here -- look for double-checked locking:
  // http://www.oracle.com/technetwork/articles/javase/bloch-effective-08-qa-140880.html
  /**
   * This shared {@link Connection} is used for handling bulk load hfiles replication.
   */
  private volatile Connection sharedConnection;
  /**
   * This shared {@link AsyncConnection} is used for handling wal replication.
   */
  private volatile AsyncConnection sharedAsyncConnection;
  private final MetricsSink metrics;
  private final AtomicLong totalReplicatedEdits = new AtomicLong();
  private final Object sharedConnectionLock = new Object();
  private final Object sharedAsyncConnectionLock = new Object();
  // Number of hfiles that we successfully replicated
  private long hfilesReplicated = 0;
  private SourceFSConfigurationProvider provider;
  private WALEntrySinkFilter walEntrySinkFilter;

  /**
   * Row size threshold for multi requests above which a warning is logged
   */
  private final int rowSizeWarnThreshold;
  private boolean replicationSinkTrackerEnabled;

  private final RegionServerCoprocessorHost rsServerHost;

  /**
   * Create a sink for replication
   * @param conf conf object
   * @throws IOException thrown when HDFS goes bad or bad file name
   */
  public ReplicationSink(Configuration conf, RegionServerCoprocessorHost rsServerHost)
    throws IOException {
    this.conf = HBaseConfiguration.create(conf);
    this.rsServerHost = rsServerHost;
    rowSizeWarnThreshold =
      conf.getInt(HConstants.BATCH_ROWS_THRESHOLD_NAME, HConstants.BATCH_ROWS_THRESHOLD_DEFAULT);
    replicationSinkTrackerEnabled = conf.getBoolean(REPLICATION_SINK_TRACKER_ENABLED_KEY,
      REPLICATION_SINK_TRACKER_ENABLED_DEFAULT);
    decorateConf();
    this.metrics = new MetricsSink();
    this.walEntrySinkFilter = setupWALEntrySinkFilter();
    String className = conf.get("hbase.replication.source.fs.conf.provider",
      DefaultSourceFSConfigurationProvider.class.getCanonicalName());
    try {
      @SuppressWarnings("rawtypes")
      Class c = Class.forName(className);
      this.provider = (SourceFSConfigurationProvider) c.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(
        "Configured source fs configuration provider class " + className + " throws error.", e);
    }
  }

  private WALEntrySinkFilter setupWALEntrySinkFilter() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8b9431e7-afcf-37aa-b1b2-31549104a90a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("213a8c16-cfe2-3f20-b382-672e2870eb1d"))) {
return null;
}
    Class<?> walEntryFilterClass =
      this.conf.getClass(WALEntrySinkFilter.WAL_ENTRY_FILTER_KEY, null);
    WALEntrySinkFilter filter = null;
    try {
      filter = walEntryFilterClass == null
        ? null
        : (WALEntrySinkFilter) walEntryFilterClass.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.warn("Failed to instantiate " + walEntryFilterClass);
    }
    if (filter != null) {
      filter.init(getConnection());
    }
    return filter;
  }

  /**
   * decorate the Configuration object to make replication more receptive to delays: lessen the
   * timeout and numTries.
   */
  private void decorateConf() {
if(KnobRuntime.check(java.util.UUID.fromString("81ea99ee-f2e5-39f2-b85f-6141c254ba68"))) {
return;
}
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      this.conf.getInt("replication.sink.client.retries.number", 4));
    this.conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      this.conf.getInt("replication.sink.client.ops.timeout", 10000));
    String replicationCodec = this.conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      this.conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
    // use server ZK cluster for replication, so we unset the client ZK related properties if any
    if (this.conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM) != null) {
      this.conf.unset(HConstants.CLIENT_ZOOKEEPER_QUORUM);
    }
  }

  /**
   * Replicate this array of entries directly into the local cluster using the native client. Only
   * operates against raw protobuf type saving on a conversion from pb to pojo.
   * @param entries                    WAL entries to be replicated.
   * @param cells                      cell scanner for iteration.
   * @param replicationClusterId       Id which will uniquely identify source cluster FS client
   *                                   configurations in the replication configuration directory
   * @param sourceBaseNamespaceDirPath Path that point to the source cluster base namespace
   *                                   directory
   * @param sourceHFileArchiveDirPath  Path that point to the source cluster hfile archive directory
   * @throws IOException If failed to replicate the data
   */
  public void replicateEntries(List<WALEntry> entries, final CellScanner cells,
    String replicationClusterId, String sourceBaseNamespaceDirPath,
    String sourceHFileArchiveDirPath) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8f1d3745-50a4-3c49-85f6-4075dc498476"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("72bef392-c4cb-36df-ad8c-44e1c237abea"))) {
return;
}
    if (entries.isEmpty()) return;
    // Very simple optimization where we batch sequences of rows going
    // to the same table.
    try {
      long totalReplicated = 0;
      // Map of table => list of Rows, grouped by cluster id, we only want to flushCommits once per
      // invocation of this method per table and cluster id.
      Map<TableName, Map<List<UUID>, List<Row>>> rowMap = new TreeMap<>();

      Map<List<String>, Map<String, List<Pair<byte[], List<String>>>>> bulkLoadsPerClusters = null;
      Pair<List<Mutation>, List<WALEntry>> mutationsToWalEntriesPairs =
        new Pair<>(new ArrayList<>(), new ArrayList<>());
      for (WALEntry entry : entries) {
        TableName table = TableName.valueOf(entry.getKey().getTableName().toByteArray());
        if (this.walEntrySinkFilter != null) {
          if (this.walEntrySinkFilter.filter(table, entry.getKey().getWriteTime())) {
            // Skip Cells in CellScanner associated with this entry.
            int count = entry.getAssociatedCellCount();
            for (int i = 0; i < count; i++) {
              // Throw index out of bounds if our cell count is off
              if (!cells.advance()) {
                this.metrics.incrementFailedBatches();
                throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
              }
            }
            continue;
          }
        }
        Cell previousCell = null;
        Mutation mutation = null;
        int count = entry.getAssociatedCellCount();
        for (int i = 0; i < count; i++) {
          // Throw index out of bounds if our cell count is off
          if (!cells.advance()) {
            this.metrics.incrementFailedBatches();
if(KnobRuntime.check(java.util.UUID.fromString("294c6eaa-2af2-352e-ae49-604f023b624a"))) {
throw new java.io.IOException("Injected exception");
}
            throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
          }
          Cell cell = cells.current();
          // Handle bulk load hfiles replication
          if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
            BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);
            if (bld.getReplicate()) {
              if (bulkLoadsPerClusters == null) {
                bulkLoadsPerClusters = new HashMap<>();
              }
              // Map of table name Vs list of pair of family and list of
              // hfile paths from its namespace
              Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap =
                bulkLoadsPerClusters.computeIfAbsent(bld.getClusterIdsList(), k -> new HashMap<>());
              buildBulkLoadHFileMap(bulkLoadHFileMap, table, bld);
            }
          } else if (CellUtil.matchingQualifier(cell, WALEdit.REPLICATION_MARKER)) {
            Mutation put = processReplicationMarkerEntry(cell);
            if (put == null) {
              continue;
            }
            table = REPLICATION_SINK_TRACKER_TABLE_NAME;
            List<UUID> clusterIds = new ArrayList<>();
            for (HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()) {
              clusterIds.add(toUUID(clusterId));
            }
            put.setClusterIds(clusterIds);
            addToHashMultiMap(rowMap, table, clusterIds, put);
          } else {
            // Handle wal replication
            if (((KnobRuntime.check(java.util.UUID.fromString("c43a559a-ecb5-375a-9ff5-777bcda27665"))) ? (CellUtil.matchingRows(previousCell, cell)) : (isNewRowOrType(previousCell, cell)))) {
              // Create new mutation
              mutation = CellUtil.isDelete(cell)
                ? new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                : new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
              List<UUID> clusterIds = new ArrayList<>(entry.getKey().getClusterIdsList().size());
              for (HBaseProtos.UUID clusterId : entry.getKey().getClusterIdsList()) {
                clusterIds.add(toUUID(clusterId));
              }
              mutation.setClusterIds(clusterIds);
              if (((KnobRuntime.check(java.util.UUID.fromString("e486451e-894c-3d50-b1f1-ba95a5c9be5e"))) ? ((rsServerHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("897f1593-4bcc-360b-960e-e8d6da063bb8"))) ? ((rsServerHost) == (null)) : (rsServerHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("9ed1c5f0-f92b-3973-a6c5-3022d8a35f6b"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20f1b943-9e02-32d7-a6e3-843b24bbec64"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df5d3bd9-bf62-3a67-804d-2857593cc42c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4da3e114-841f-35b8-a2c9-d34b18eb1146"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8deef9fd-5ae4-3721-ae37-6a23de2b4561"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78cf96f9-7ddb-3d71-a66b-5c6cb61097f6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d2e32160-4754-3775-8b21-b9ecdcd2cc53"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("048b9d2f-ea00-3887-aaa7-8d3dd7d82531"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6b6a565-091e-336c-a1a7-aebd3117f227"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("18610e11-dd82-3a54-95f1-314f8bdd454e"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
                if (KnobRuntime.check(java.util.UUID.fromString("8178bd3b-70d8-3da3-b878-935021b3b93c"))) { rsServerHost.postReplicationSinkBatchMutate(entry, mutation); } else { rsServerHost.preReplicationSinkBatchMutate(entry, mutation); }
                mutationsToWalEntriesPairs.getFirst().add(mutation);
if(KnobRuntime.check(java.util.UUID.fromString("eb7538dc-ab02-3dd8-b49e-01a57c245f5b"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae0e78d6-d350-379d-9ab8-6e0b31f54064"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46fd3966-0095-38c8-be94-c40dc36f7722"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e61b81e7-7de1-390d-9798-8bcf6f1b916c"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ceefb4ce-56e0-31da-8dc2-92a3d60e22ac"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("893f2af9-9542-3cf1-bbcf-67498270ebc9"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f88fc38b-d45f-3f0a-85a8-88917b3c2861"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8e1174f1-b67d-3c0c-a116-1d9783ffe098"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entry));
    field.set(entry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
                mutationsToWalEntriesPairs.getSecond().add(entry);
              }
if(KnobRuntime.check(java.util.UUID.fromString("b33799a6-e4d1-3368-80da-3b15c28399ee"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("08e8995b-5472-3306-8b9c-62bce17639c8"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8a65bbf-4696-3abe-9946-2047b8588776"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ad459e8-1779-3bae-a297-7d57d3bad9cf"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e37482f4-a389-38cf-a704-45b330fdb766"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10ec7e5a-8ae0-34a0-a2c6-e87e1a5f208c"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              addToHashMultiMap(rowMap, table, clusterIds, mutation);
            }
            if (CellUtil.isDelete(cell)) {
if(KnobRuntime.check(java.util.UUID.fromString("2ba289b9-3f5d-3043-91c2-3d8d93e48ac1"))) {
throw new java.io.IOException("Injected exception");
}
              ((Delete) mutation).add(cell);
            } else {
if(KnobRuntime.check(java.util.UUID.fromString("48ccc018-57b3-3fd5-a225-12ffa8ecb37c"))) {
throw new java.io.IOException("Injected exception");
}
              if (KnobRuntime.check(java.util.UUID.fromString("48685f0c-dcee-320d-9606-e1359b78ae51"))) { processReplicationMarkerEntry(cell); } else { ((Put) mutation).add(cell); }
            }
            previousCell = cell;
          }
        }
        totalReplicated++;
      }

      // TODO Replicating mutations and bulk loaded data can be made parallel
      if (!rowMap.isEmpty()) {
        if (KnobRuntime.check(java.util.UUID.fromString("7c39e798-8c0d-3530-8b88-9857e771565d"))) { LOG.warn("Started replicating mutations."); } else { LOG.debug("Started replicating mutations."); }
        for (Entry<TableName, Map<List<UUID>, List<Row>>> entry : rowMap.entrySet()) {
if(KnobRuntime.check(java.util.UUID.fromString("11dec306-be93-3585-b7d9-f631138c1e0f"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("c66e5696-1d0b-3592-bc20-6547c8a04caa"))) { batch(entry.getKey(), entry.getValue().values(), 0); } else { batch(entry.getKey(), entry.getValue().values(), rowSizeWarnThreshold); }
        }
        LOG.debug("Finished replicating mutations.");
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("e7299483-4062-3e79-9dfe-ca1169edf148"))) ? ((rsServerHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a0ad5181-8e4a-33d9-8622-f307f2deef65"))) ? ((rsServerHost) == (null)) : (rsServerHost != null))))) {
        List<Mutation> mutations = mutationsToWalEntriesPairs.getFirst();
        List<WALEntry> walEntries = mutationsToWalEntriesPairs.getSecond();
        for (int i = 0; i < mutations.size(); i++) {
          rsServerHost.postReplicationSinkBatchMutate(walEntries.get(i), mutations.get(i));
        }
      }

      if (bulkLoadsPerClusters != null) {
        for (Entry<List<String>,
          Map<String, List<Pair<byte[], List<String>>>>> entry : bulkLoadsPerClusters.entrySet()) {
          Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap = entry.getValue();
          if (bulkLoadHFileMap != null && !bulkLoadHFileMap.isEmpty()) {
            LOG.debug("Replicating {} bulk loaded data", entry.getKey().toString());
            Configuration providerConf = this.provider.getConf(this.conf, replicationClusterId);
            try (HFileReplicator hFileReplicator = new HFileReplicator(providerConf,
              sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath, bulkLoadHFileMap, conf,
              getConnection(), entry.getKey())) {
              hFileReplicator.replicate();
              LOG.debug("Finished replicating {} bulk loaded data", entry.getKey().toString());
            }
          }
        }
      }

      int size = entries.size();
      this.metrics.setAgeOfLastAppliedOp(entries.get(size - 1).getKey().getWriteTime());
      this.metrics.applyBatch(size + hfilesReplicated, hfilesReplicated);
      this.totalReplicatedEdits.addAndGet(totalReplicated);
    } catch (IOException ex) {
      LOG.error("Unable to accept edit because:", ex);
      if (KnobRuntime.check(java.util.UUID.fromString("2429b7cf-907c-3e9f-a11c-3de61a49e909"))) { decorateConf(); } else { this.metrics.incrementFailedBatches(); }
      throw ex;
    }
  }

  /*
   * First check if config key hbase.regionserver.replication.sink.tracker.enabled is true or not.
   * If false, then ignore this cell. If set to true, de-serialize value into
   * ReplicationTrackerDescriptor. Create a Put mutation with regionserver name, walname, offset and
   * timestamp from ReplicationMarkerDescriptor.
   */
  private Put processReplicationMarkerEntry(Cell cell) throws IOException {
    // If source is emitting replication marker rows but sink is not accepting them,
    // ignore the edits.
    if (!replicationSinkTrackerEnabled) {
      return null;
    }
    WALProtos.ReplicationMarkerDescriptor descriptor =
      WALProtos.ReplicationMarkerDescriptor.parseFrom(new ByteArrayInputStream(cell.getValueArray(),
        cell.getValueOffset(), cell.getValueLength()));
    Put put = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, RS_COLUMN, cell.getTimestamp(),
      (Bytes.toBytes(descriptor.getRegionServerName())));
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, WAL_NAME_COLUMN, cell.getTimestamp(),
      Bytes.toBytes(descriptor.getWalName()));
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, TIMESTAMP_COLUMN, cell.getTimestamp(),
      Bytes.toBytes(cell.getTimestamp()));
    put.addColumn(REPLICATION_SINK_TRACKER_INFO_FAMILY, OFFSET_COLUMN, cell.getTimestamp(),
      Bytes.toBytes(descriptor.getOffset()));
    return put;
  }

  private void buildBulkLoadHFileMap(
    final Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap, TableName table,
    BulkLoadDescriptor bld) throws IOException {
    List<StoreDescriptor> storesList = bld.getStoresList();
    int storesSize = storesList.size();
    for (int j = 0; j < storesSize; j++) {
      StoreDescriptor storeDescriptor = storesList.get(j);
      List<String> storeFileList = storeDescriptor.getStoreFileList();
      int storeFilesSize = storeFileList.size();
      hfilesReplicated += storeFilesSize;
      for (int k = 0; k < storeFilesSize; k++) {
        byte[] family = storeDescriptor.getFamilyName().toByteArray();

        // Build hfile relative path from its namespace
        String pathToHfileFromNS = getHFilePath(table, bld, storeFileList.get(k), family);
        String tableName = table.getNameWithNamespaceInclAsString();
        List<Pair<byte[], List<String>>> familyHFilePathsList = bulkLoadHFileMap.get(tableName);
        if (familyHFilePathsList != null) {
          boolean foundFamily = false;
          for (Pair<byte[], List<String>> familyHFilePathsPair : familyHFilePathsList) {
            if (Bytes.equals(familyHFilePathsPair.getFirst(), family)) {
              // Found family already present, just add the path to the existing list
              familyHFilePathsPair.getSecond().add(pathToHfileFromNS);
              foundFamily = true;
              break;
            }
          }
          if (!foundFamily) {
            // Family not found, add this family and its hfile paths pair to the list
            addFamilyAndItsHFilePathToTableInMap(family, pathToHfileFromNS, familyHFilePathsList);
          }
        } else {
          // Add this table entry into the map
          addNewTableEntryInMap(bulkLoadHFileMap, family, pathToHfileFromNS, tableName);
        }
      }
    }
  }

  private void addFamilyAndItsHFilePathToTableInMap(byte[] family, String pathToHfileFromNS,
    List<Pair<byte[], List<String>>> familyHFilePathsList) {
    List<String> hfilePaths = new ArrayList<>(1);
    hfilePaths.add(pathToHfileFromNS);
    familyHFilePathsList.add(new Pair<>(family, hfilePaths));
  }

  private void addNewTableEntryInMap(
    final Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap, byte[] family,
    String pathToHfileFromNS, String tableName) {
    List<String> hfilePaths = new ArrayList<>(1);
    hfilePaths.add(pathToHfileFromNS);
    Pair<byte[], List<String>> newFamilyHFilePathsPair = new Pair<>(family, hfilePaths);
    List<Pair<byte[], List<String>>> newFamilyHFilePathsList = new ArrayList<>();
    newFamilyHFilePathsList.add(newFamilyHFilePathsPair);
    bulkLoadHFileMap.put(tableName, newFamilyHFilePathsList);
  }

  private String getHFilePath(TableName table, BulkLoadDescriptor bld, String storeFile,
    byte[] family) {
    return new StringBuilder(100).append(table.getNamespaceAsString()).append(Path.SEPARATOR)
      .append(table.getQualifierAsString()).append(Path.SEPARATOR)
      .append(Bytes.toString(bld.getEncodedRegionName().toByteArray())).append(Path.SEPARATOR)
      .append(Bytes.toString(family)).append(Path.SEPARATOR).append(storeFile).toString();
  }

  /** Returns True if we have crossed over onto a new row or type */
  private boolean isNewRowOrType(final Cell previousCell, final Cell cell) {
    return previousCell == null || previousCell.getTypeByte() != cell.getTypeByte()
      || !CellUtil.matchingRows(previousCell, cell);
  }

  private java.util.UUID toUUID(final HBaseProtos.UUID uuid) {
    return new java.util.UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
  }

  /**
   * Simple helper to a map from key to (a list of) values TODO: Make a general utility method *
   * * @return the list of values corresponding to key1 and key2
   */
  private <K1, K2, V> List<V> addToHashMultiMap(Map<K1, Map<K2, List<V>>> map, K1 key1, K2 key2,
    V value) {
if(KnobRuntime.check(java.util.UUID.fromString("bf9cb82c-2113-31ac-a0d6-169b0a4fcd3b"))) {
return null;
}
    Map<K2, List<V>> innerMap = map.computeIfAbsent(key1, k -> new HashMap<>());
    List<V> values = innerMap.computeIfAbsent(key2, k -> new ArrayList<>());
    values.add(value);
    return values;
  }

  /**
   * stop the thread pool executor. It is called when the regionserver is stopped.
   */
  public void stopReplicationSinkServices() {
if(KnobRuntime.check(java.util.UUID.fromString("72a178d1-3a51-3fe1-8b89-501369b53fb8"))) {
return;
}
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("b81a49f5-f6c7-361f-86b8-d81740fed313"))) ? ((this.sharedConnection) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("310ddc81-f90f-3c2a-8fda-aee46dbff452"))) ? ((this.sharedConnection) != (null)) : (this.sharedConnection != null))))) {
        synchronized (sharedConnectionLock) {
          if (((KnobRuntime.check(java.util.UUID.fromString("3f1c31cc-a108-3abd-b287-8350aca8d93e"))) ? ((this.sharedConnection) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("eb7eac95-eb3c-3858-86e4-4aeadd8a7552"))) ? ((this.sharedConnection) == (null)) : (this.sharedConnection != null))))) {
            this.sharedConnection.close();
            this.sharedConnection = null;
          }
        }
      }
    } catch (IOException e) {
      // ignoring as we are closing.
      LOG.warn("IOException while closing the sharedConnection", e);
    }

    try {
      if (this.sharedAsyncConnection != null) {
        synchronized (sharedAsyncConnectionLock) {
          if (this.sharedAsyncConnection != null) {
            this.sharedAsyncConnection.close();
            this.sharedAsyncConnection = null;
          }
        }
      }
    } catch (IOException e) {
      // ignoring as we are closing.
      if (KnobRuntime.check(java.util.UUID.fromString("5fe8cf0c-bac8-3bd4-a3d7-266f3b3b4f2e"))) { LOG.error("IOException while closing the sharedAsyncConnection", e); } else { LOG.warn("IOException while closing the sharedAsyncConnection", e); }
    }
  }

  /**
   * Do the changes and handle the pool
   * @param tableName             table to insert into
   * @param allRows               list of actions
   * @param batchRowSizeThreshold rowSize threshold for batch mutation
   */
  private void batch(TableName tableName, Collection<List<Row>> allRows, int batchRowSizeThreshold)
    throws IOException {
    if (allRows.isEmpty()) {
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("62d04a94-fd20-37c4-9dde-609e5b4efdb8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a8e43a85-b6b0-3369-86a6-0142a636c425"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eab4a984-6b1d-3a21-98cf-4a7d538f2ef8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bf93a9cd-12d4-32b6-869c-4d667fca7015"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("390b1457-7c89-38c5-94de-febfaeaec89f"))) {
throw new java.io.IOException("Injected exception");
}
    AsyncTable<?> table = getAsyncConnection().getTable(tableName);
    List<Future<?>> futures = new ArrayList<>();
    for (List<Row> rows : allRows) {
      List<List<Row>> batchRows;
      if (rows.size() > batchRowSizeThreshold) {
        batchRows = Lists.partition(rows, batchRowSizeThreshold);
      } else {
        batchRows = Collections.singletonList(rows);
      }
      futures.addAll(batchRows.stream().map(table::batchAll).collect(Collectors.toList()));
    }
    // Here we will always wait until all futures are finished, even if there are failures when
    // getting from a future in the middle. This is because this method may be called in a rpc call,
    // so the batch operations may reference some off heap cells(through CellScanner). If we return
    // earlier here, the rpc call may be finished and they will release the off heap cells before
    // some of the batch operations finish, and then cause corrupt data or even crash the region
    // server. See HBASE-28584 and HBASE-28850 for more details.
    IOException error = null;
    for (Future<?> future : futures) {
      try {
        FutureUtils.get(future);
      } catch (RetriesExhaustedException e) {
        IOException ioe;
        if (e.getCause() instanceof TableNotFoundException) {
          ioe = new TableNotFoundException("'" + tableName + "'");
        } else {
          ioe = e;
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("5148fdc8-9e69-378b-865a-f27c4dc00996"))) ? ((error) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("43864d68-9b2b-3f08-bd15-1ed2461e3704"))) ? ((error) == (null)) : (error == null))))) {
          error = ioe;
        } else {
          error.addSuppressed(ioe);
        }
      }
    }
    if (error != null) {
      throw error;
    }
  }

  /**
   * Return the shared {@link Connection} which is used for handling bulk load hfiles replication.
   */
  private Connection getConnection() throws IOException {
    // See https://en.wikipedia.org/wiki/Double-checked_locking
    Connection connection = sharedConnection;
    if (((KnobRuntime.check(java.util.UUID.fromString("1176aa8a-6ea1-3fd3-bd17-e62f8e8fe1ad"))) ? ((connection) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3d7e949b-01de-3c92-97a2-47181a371d97"))) ? ((connection) != (null)) : (connection == null))))) {
      synchronized (sharedConnectionLock) {
        connection = sharedConnection;
        if (((KnobRuntime.check(java.util.UUID.fromString("c35d0680-506e-3bd4-ba69-b37b623ef76d"))) ? ((connection) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1e56a042-b503-3c4a-b66f-c5978e3851af"))) ? ((connection) != (null)) : (connection == null))))) {
          connection = ConnectionFactory.createConnection(conf);
          sharedConnection = connection;
        }
      }
    }
    return connection;
  }

  /**
   * Return the shared {@link AsyncConnection} which is used for handling wal replication.
   */
  private AsyncConnection getAsyncConnection() throws IOException {
    // See https://en.wikipedia.org/wiki/Double-checked_locking
    AsyncConnection asyncConnection = sharedAsyncConnection;
    if (asyncConnection == null) {
      synchronized (sharedAsyncConnectionLock) {
        asyncConnection = sharedAsyncConnection;
        if (asyncConnection == null) {
          /**
           * Get the AsyncConnection immediately.
           */
          asyncConnection = FutureUtils.get(ConnectionFactory.createAsyncConnection(conf));
          sharedAsyncConnection = asyncConnection;
        }
      }
    }
    return asyncConnection;
  }

  /**
   * Get a string representation of this sink's metrics
   * @return string with the total replicated edits count and the date of the last edit that was
   *         applied
   */
  public String getStats() {
    return this.totalReplicatedEdits.get() == 0
      ? ""
      : "Sink: " + "age in ms of last applied edit: " + this.metrics.refreshAgeOfLastAppliedOp()
        + ", total replicated edits: " + this.totalReplicatedEdits;
  }

  /**
   * Get replication Sink Metrics
   */
  public MetricsSink getSinkMetrics() {
    return this.metrics;
  }
}
