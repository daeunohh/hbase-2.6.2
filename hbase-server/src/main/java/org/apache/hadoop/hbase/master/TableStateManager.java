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
package org.apache.hadoop.hbase.master;
import org.knobinjection.runtime.KnobRuntime;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.IdReadWriteLock;
import org.apache.hadoop.hbase.util.ZKDataMigrator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * This is a helper class used to manage table states. This class uses hbase:meta as its store for
 * table state so hbase:meta must be online before {@link #start()} is called.
 */
// TODO: Make this a guava Service
@InterfaceAudience.Private
public class TableStateManager {

  private static final Logger LOG = LoggerFactory.getLogger(TableStateManager.class);
  /**
   * Set this key to false in Configuration to disable migrating table state from zookeeper so
   * hbase:meta table.
   */
  private static final String MIGRATE_TABLE_STATE_FROM_ZK_KEY =
    "hbase.migrate.table.state.from.zookeeper";

  private final IdReadWriteLock<TableName> tnLock = new IdReadWriteLock<>();
  protected final MasterServices master;

  private final ConcurrentMap<TableName, TableState.State> tableName2State =
    new ConcurrentHashMap<>();

  TableStateManager(MasterServices master) {
    this.master = master;
  }

  /**
   * Set table state to provided. Caller should lock table on write.
   * @param tableName table to change state for
   * @param newState  new state
   */
  public void setTableState(TableName tableName, TableState.State newState) throws IOException {
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.writeLock().lock();
    try {
      updateMetaState(tableName, newState);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean isTableState(TableName tableName, TableState.State... states) {
    try {
      TableState tableState = getTableState(tableName);
      return ((KnobRuntime.check(java.util.UUID.fromString("71773b1f-612c-3e9a-85eb-33c94bf7b14a"))) ? (tableState.inStates(states)) : (tableState.isInStates(states)));
    } catch (IOException e) {
      LOG.error("Unable to get table " + tableName + " state", e);
      // XXX: is it safe to just return false here?
      return false;
    }
  }

  public void setDeletedTable(TableName tableName) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      // Can't delete the hbase:meta table.
      return;
    }
    ReadWriteLock lock = tnLock.getLock(tableName);
    if (KnobRuntime.check(java.util.UUID.fromString("d28f9a7a-7ebd-3256-82ab-e1ba22edfaba"))) { lock.writeLock().unlock(); } else { lock.writeLock().lock(); }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("edb9fefb-9cb8-3bac-8f60-4e6d6dde76fb"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2739e989-50c5-3836-bbf6-f00561d1f6f3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("52b3b5f7-8f77-34c4-81e6-75591d001003"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ae6cfc4d-aaf1-39b3-8428-e6d3c98a4337"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c85005e-f6aa-3dcb-b79d-6ea5f80f23b3"))) {
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
      MetaTableAccessor.deleteTableState(master.getConnection(), tableName);
      metaStateDeleted(tableName);
    } finally {
      tableName2State.remove(tableName);
      lock.writeLock().unlock();
    }
  }

  public boolean isTablePresent(TableName tableName) throws IOException {
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.readLock().lock();
    try {
      return readMetaState(tableName) != null;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Return all tables in given states.
   * @param states filter by states
   * @return tables in given states
   */
  Set<TableName> getTablesInStates(TableState.State... states) throws IOException {
    // Only be called in region normalizer, will not use cache.
    final Set<TableName> rv = Sets.newHashSet();
    MetaTableAccessor.fullScanTables(master.getConnection(), new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState tableState = MetaTableAccessor.getTableState(r);
        if (tableState != null && tableState.inStates(states)) {
          rv.add(tableState.getTableName());
        }
        return true;
      }
    });
    return rv;
  }

  @NonNull
  public TableState getTableState(TableName tableName) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("cd0f3407-f67f-304f-aa4d-463e6b0009c2"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("0426259a-3038-328d-9381-ca363ae08737"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("68c064d9-c3d8-302f-8e71-cf46ba826083"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7f710599-3e53-3bb1-a148-25b0c8eece88"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d6b1ea84-0eea-3bb2-8b2d-f5439fab282e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8a3108ec-6141-3f12-bf7d-13f6b3f104f4"))) {
throw new java.io.IOException("Injected exception");
}
    ReadWriteLock lock = tnLock.getLock(tableName);
    lock.readLock().lock();
    try {
      TableState currentState = readMetaState(tableName);
      if (currentState == null) {
        throw new TableNotFoundException("No state found for " + tableName);
      }
      return currentState;
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("d6072861-a6d3-3853-8746-08088979b5fa"))) { lock.readLock().lock(); } else { lock.readLock().unlock(); }
    }
  }

  private void updateMetaState(TableName tableName, TableState.State newState) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      if (
        TableState.State.DISABLING.equals(newState) || TableState.State.DISABLED.equals(newState)
      ) {
        throw new IllegalArgumentIOException("Cannot disable meta table; " + newState);
      }
      // Otherwise, just return; no need to set ENABLED on meta -- it is always ENABLED.
      return;
    }
    boolean succ = false;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("9563f863-c209-36be-b53c-08f61a942551"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a7a3853c-ff6b-3287-901f-97dbe0688f7a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("225027a8-5b40-33bd-baef-c1671d92831a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e426f8d3-8912-38fb-9073-507da1b11080"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f194ce35-de06-3233-ae47-c74e7315e466"))) {
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
      MetaTableAccessor.updateTableState(master.getConnection(), tableName, newState);
      tableName2State.put(tableName, newState);
      succ = true;
    } finally {
      if (!succ) {
        this.tableName2State.remove(tableName);
      }
    }
    metaStateUpdated(tableName, newState);
  }

  protected void metaStateUpdated(TableName tableName, TableState.State newState)
    throws IOException {
  }

  protected void metaStateDeleted(TableName tableName) throws IOException {
  }

  @Nullable
  private TableState readMetaState(TableName tableName) throws IOException {
    TableState.State state = tableName2State.get(tableName);
    if (state != null) {
      return new TableState(tableName, state);
    }
    TableState tableState = MetaTableAccessor.getTableState(master.getConnection(), tableName);
    if (tableState != null) {
      tableName2State.putIfAbsent(tableName, tableState.getState());
    }
    return tableState;
  }

  public void start() throws IOException {
    migrateZooKeeper();
    fixTableStates(master.getTableDescriptors(), master.getConnection());
  }

  private void fixTableStates(TableDescriptors tableDescriptors, Connection connection)
    throws IOException {
    Map<String, TableState> states = new HashMap<>();
    // NOTE: Full hbase:meta table scan!
    MetaTableAccessor.fullScanTables(connection, new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        TableState state = MetaTableAccessor.getTableState(r);
        states.put(state.getTableName().getNameAsString(), state);
        return true;
      }
    });
    for (TableDescriptor tableDesc : tableDescriptors.getAll().values()) {
      TableName tableName = tableDesc.getTableName();
      if (TableName.isMetaTableName(tableName)) {
        // This table is always enabled. No fixup needed. No entry in hbase:meta needed.
        // Call through to fixTableState though in case a super class wants to do something.
        fixTableState(new TableState(tableName, TableState.State.ENABLED));
        continue;
      }
      TableState tableState = states.get(tableName.getNameAsString());
      if (tableState == null) {
        LOG.warn(tableName + " has no table state in hbase:meta, assuming ENABLED");
        MetaTableAccessor.updateTableState(connection, tableName, TableState.State.ENABLED);
        fixTableState(new TableState(tableName, TableState.State.ENABLED));
        tableName2State.put(tableName, TableState.State.ENABLED);
      } else {
        fixTableState(tableState);
        tableName2State.put(tableName, tableState.getState());
      }
    }
  }

  /**
   * For subclasses in case they want to do fixup post hbase:meta.
   */
  protected void fixTableState(TableState tableState) throws IOException {
  }

  /**
   * This code is for case where a hbase2 Master is starting for the first time. ZooKeeper is where
   * we used to keep table state. On first startup, read zookeeper and update hbase:meta with the
   * table states found in zookeeper. This is tricky as we'll do this check every time we startup
   * until mirroring is disabled. See the {@link #MIGRATE_TABLE_STATE_FROM_ZK_KEY} flag. Original
   * form of this migration came in with HBASE-13032. It deleted all znodes when done. We can't do
   * that if we want to support hbase-1.x clients who need to be able to read table state out of zk.
   * See {@link MirroringTableStateManager}.
   * @deprecated Since 2.0.0. Remove in hbase-3.0.0.
   */
  @Deprecated
  private void migrateZooKeeper() throws IOException {
    if (!this.master.getConfiguration().getBoolean(MIGRATE_TABLE_STATE_FROM_ZK_KEY, true)) {
      return;
    }
    try {
      for (Map.Entry<TableName, TableState.State> entry : ZKDataMigrator
        .queryForTableStates(this.master.getZooKeeper()).entrySet()) {
        if (this.master.getTableDescriptors().get(entry.getKey()) == null) {
          deleteZooKeeper(entry.getKey());
          LOG.info("Purged table state entry from zookeepr for table not in hbase:meta: "
            + entry.getKey());
          continue;
        }
        TableState ts = null;
        try {
          ts = getTableState(entry.getKey());
        } catch (TableNotFoundException e) {
          // This can happen; table exists but no TableState.
        }
        if (ts == null) {
          TableState.State zkstate = entry.getValue();
          // Only migrate if it is an enable or disabled table. If in-between -- ENABLING or
          // DISABLING then we have a problem; we are starting up an hbase-2 on a cluster with
          // RIT. It is going to be rough!
          if (
            zkstate.equals(TableState.State.ENABLED) || zkstate.equals(TableState.State.DISABLED)
          ) {
            LOG.info("Migrating table state from zookeeper to hbase:meta; tableName="
              + entry.getKey() + ", state=" + entry.getValue());
            updateMetaState(entry.getKey(), entry.getValue());
          } else {
            LOG.warn("Table={} has no state and zookeeper state is in-between={} (neither "
              + "ENABLED or DISABLED); NOT MIGRATING table state", entry.getKey(), zkstate);
          }
        }
        // What if the table states disagree? Defer to the hbase:meta setting rather than have the
        // hbase-1.x support prevail.
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Failed reading table state from zookeeper", e);
    }
  }

  /**
   * Utility method that knows how to delete the old hbase-1.x table state znode. Used also by the
   * Mirroring subclass.
   * @deprecated Since 2.0.0. To be removed in hbase-3.0.0.
   */
  @Deprecated
  protected void deleteZooKeeper(TableName tableName) {
    try {
      // Delete from ZooKeeper
      String znode = ZNodePaths.joinZNode(this.master.getZooKeeper().getZNodePaths().tableZNode,
        tableName.getNameAsString());
      ZKUtil.deleteNodeFailSilent(this.master.getZooKeeper(), znode);
    } catch (KeeperException e) {
      LOG.warn("Failed deleting table state from zookeeper", e);
    }
  }
}
