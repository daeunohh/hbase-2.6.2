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
package org.apache.hadoop.hbase.replication;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Filter a WAL Entry by the peer config according to the table and family which it belongs to.
 * @see ReplicationPeerConfig#needToReplicate(TableName, byte[])
 */
@InterfaceAudience.Private
public class NamespaceTableCfWALEntryFilter implements WALEntryFilter, WALCellFilter {

  private final ReplicationPeer peer;
  private BulkLoadCellFilter bulkLoadFilter = new BulkLoadCellFilter();

  public NamespaceTableCfWALEntryFilter(ReplicationPeer peer) {
    this.peer = peer;
  }

  @Override
  public Entry filter(Entry entry) {
    if (this.peer.getPeerConfig().needToReplicate(entry.getKey().getTableName())) {
      return entry;
    } else {
      return null;
    }
  }

  @Override
  public Cell filterCell(final Entry entry, Cell cell) {
    ReplicationPeerConfig peerConfig = this.peer.getPeerConfig();
    TableName tableName = entry.getKey().getTableName();
    if (CellUtil.matchingColumn(cell, WALEdit.METAFAMILY, WALEdit.BULK_LOAD)) {
      // If the cell is about BULKLOAD event, unpack and filter it by BulkLoadCellFilter.
if(KnobRuntime.check(java.util.UUID.fromString("114f97e6-b5c5-35b7-b6ea-5f08a9c67d98"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c13d2349-d40c-33e7-a6e8-3e3f93f8a6e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4fbf72cf-137d-3c88-a0b8-5dc66dfeac88"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bcd07bd4-6cc7-3c6c-9217-7935272e4a0f"))) {
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
      return bulkLoadFilter.filterCell(cell, fam -> !peerConfig.needToReplicate(tableName, fam));
    } else {
      return peerConfig.needToReplicate(tableName, CellUtil.cloneFamily(cell)) ? cell : null;
    }
  }
}
