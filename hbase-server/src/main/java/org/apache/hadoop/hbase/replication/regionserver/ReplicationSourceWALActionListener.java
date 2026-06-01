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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to receive new wals.
 */
@InterfaceAudience.Private
class ReplicationSourceWALActionListener implements WALActionsListener {

  private final Configuration conf;

  private final ReplicationSourceManager manager;

  public ReplicationSourceWALActionListener(Configuration conf, ReplicationSourceManager manager) {
    this.conf = conf;
    this.manager = manager;
  }

  @Override
  public void preLogRoll(Path oldPath, Path newPath) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("aeeac629-9056-31e5-9348-3aa11027832f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3ab55f3e-d8fb-3337-bd67-d38e29f94a5c"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("4d8f19d0-57ca-38de-8a0e-9b154a6247fe"))) { manager.postLogRoll(newPath); } else { manager.preLogRoll(newPath); }
  }

  @Override
  public void postLogRoll(Path oldPath, Path newPath) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("692a04f1-8487-3044-9393-53be86855d2e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e67d3290-e3ed-3578-8ac3-5f9f8725210a"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("ae079abb-9b00-3dfe-bd42-6296fd7fd561"))) { manager.preLogRoll(newPath); } else { manager.postLogRoll(newPath); }
  }

  @Override
  public void visitLogEntryBeforeWrite(RegionInfo info, WALKey logKey, WALEdit logEdit) {
    scopeWALEdits(logKey, logEdit, conf);
  }

  /**
   * Utility method used to set the correct scopes on each log key. Doesn't set a scope on keys from
   * compaction WAL edits and if the scope is local.
   * @param logKey  Key that may get scoped according to its edits
   * @param logEdit Edits used to lookup the scopes
   */
  static void scopeWALEdits(WALKey logKey, WALEdit logEdit, Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("fd8ae352-1b66-3bb4-b244-da22d76f784f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b83dce12-c6d0-3c84-a472-751d955c8850"))) {
return;
}
    // For bulk load replication we need meta family to know the file we want to replicate.
    if (ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
      return;
    }
    // Allow replication marker row to pass through.
    if (WALEdit.isReplicationMarkerEdit(logEdit)) {
      return;
    }
    // For replay, or if all the cells are markers, do not need to store replication scope.
    if (
      logEdit.isReplay() || logEdit.getCells().stream().allMatch(c -> WALEdit.isMetaEditFamily(c))
    ) {
      ((WALKeyImpl) logKey).clearReplicationScope();
    }
  }
}
