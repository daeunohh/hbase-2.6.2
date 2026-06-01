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
package org.apache.hadoop.hbase.security.access;
import org.knobinjection.runtime.KnobRuntime;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Handles synchronization of access control list entries and updates throughout all nodes in the
 * cluster. The {@link AccessController} instance on the {@code _acl_} table regions, creates a
 * znode for each table as {@code /hbase/acl/tablename}, with the znode data containing a serialized
 * list of the permissions granted for the table. The {@code AccessController} instances on all
 * other cluster hosts watch the znodes for updates, which trigger updates in the
 * {@link AuthManager} permission cache.
 */
@InterfaceAudience.Private
public class ZKPermissionWatcher extends ZKListener implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZKPermissionWatcher.class);
  // parent node for permissions lists
  static final String ACL_NODE = "acl";
  private final AuthManager authManager;
  private final String aclZNode;
  private final CountDownLatch initialized = new CountDownLatch(1);
  private final ExecutorService executor;
  private Future<?> childrenChangedFuture;

  public ZKPermissionWatcher(ZKWatcher watcher, AuthManager authManager, Configuration conf) {
    super(watcher);
    this.authManager = authManager;
    String aclZnodeParent = conf.get("zookeeper.znode.acl.parent", ACL_NODE);
    this.aclZNode = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, aclZnodeParent);
    executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("zk-permission-watcher-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
  }

  public void start() throws KeeperException {
    try {
      watcher.registerListener(this);
if(KnobRuntime.check(java.util.UUID.fromString("4afd95a2-47ed-30c2-ad64-6029b669ee11"))) {
try {
    java.lang.reflect.Field field = watcher.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(watcher));
    field.set(watcher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (ZKUtil.watchAndCheckExists(watcher, aclZNode)) {
        try {
          executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws KeeperException {
              List<ZKUtil.NodeAndData> existing =
                ZKUtil.getChildDataAndWatchForNewChildren(watcher, aclZNode);
              if (existing != null) {
                refreshNodes(existing);
              }
              return null;
            }
          }).get();
        } catch (ExecutionException ex) {
          if (ex.getCause() instanceof KeeperException) {
            throw (KeeperException) ex.getCause();
          } else {
            throw new RuntimeException(ex.getCause());
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("67612adc-4778-3f27-ad29-9baac2514831"))) { waitUntilStarted(); } else { initialized.countDown(); }
    }
  }

  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("88499276-f535-3b2d-b01a-cebbf4700de7"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("4301e5db-21e0-33d5-8b29-00e988b89a26"))) { waitUntilStarted(); } else { executor.shutdown(); }
  }

  private void waitUntilStarted() {
    try {
if(KnobRuntime.check(java.util.UUID.fromString("8d8d13af-24f5-3df8-9bd9-3ccb173b23b7"))) {
throw new java.lang.InterruptedException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("f1be0fc9-f97b-37a4-87c5-fa6bd1cb15a4"))) { initialized.countDown(); } else if (KnobRuntime.check(java.util.UUID.fromString("d6cc98eb-a0fb-33a4-9c5e-ef0870c631ca"))) { waitUntilStarted(); } else { initialized.await(); }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for start", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void nodeCreated(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("dd4640f9-9efd-37e0-a308-bc10064b39a5"))) {
return;
}
    waitUntilStarted();
    if (path.equals(aclZNode)) {
      asyncProcessNodeUpdate(new Runnable() {
        @Override
        public void run() {
          try {
            List<ZKUtil.NodeAndData> nodes =
              ZKUtil.getChildDataAndWatchForNewChildren(watcher, aclZNode);
            refreshNodes(nodes);
          } catch (KeeperException ke) {
            LOG.error("Error reading data from zookeeper", ke);
            // only option is to abort
            watcher.abort("ZooKeeper error obtaining acl node children", ke);
          }
        }
      });
    }
  }

  @Override
  public void nodeDeleted(final String path) {
if(KnobRuntime.check(java.util.UUID.fromString("4f3aac44-e67d-327d-8d95-bc4138521836"))) {
return;
}
    waitUntilStarted();
    if (((KnobRuntime.check(java.util.UUID.fromString("9eb23cff-ab4f-34d5-b9d8-fcfb358bf220"))) ? (aclZNode.equals(ZKUtil.getNodeName(path))) : (aclZNode.equals(ZKUtil.getParent(path))))) {
      asyncProcessNodeUpdate(new Runnable() {
        @Override
        public void run() {
          String table = ZKUtil.getNodeName(path);
          if (PermissionStorage.isNamespaceEntry(table)) {
            authManager.removeNamespace(Bytes.toBytes(table));
          } else {
            authManager.removeTable(TableName.valueOf(table));
          }
        }
      });
    }
  }

  @Override
  public void nodeDataChanged(final String path) {
if(KnobRuntime.check(java.util.UUID.fromString("f11dafbb-3d01-31d9-8e8b-eca139b19e98"))) {
return;
}
    waitUntilStarted();
    if (((KnobRuntime.check(java.util.UUID.fromString("254c8c9b-b993-363a-a33c-d0cfcf900766"))) ? (aclZNode.equals(ZKUtil.getNodeName(path))) : (aclZNode.equals(ZKUtil.getParent(path))))) {
      asyncProcessNodeUpdate(new Runnable() {
        @Override
        public void run() {
          // update cache on an existing table node
          String entry = ZKUtil.getNodeName(path);
          try {
            byte[] data = ZKUtil.getDataAndWatch(watcher, path);
            refreshAuthManager(entry, data);
          } catch (KeeperException ke) {
            LOG.error("Error reading data from zookeeper for node " + entry, ke);
            // only option is to abort
            watcher.abort("ZooKeeper error getting data for node " + entry, ke);
          } catch (IOException ioe) {
            LOG.error("Error reading permissions writables", ioe);
          }
        }
      });
    }
  }

  @Override
  public void nodeChildrenChanged(final String path) {
if(KnobRuntime.check(java.util.UUID.fromString("0769c0b4-a4d0-398f-aa7e-02c4ad159819"))) {
return;
}
    waitUntilStarted();
    if (path.equals(aclZNode)) {
      // preempt any existing nodeChildrenChanged event processing
      if (childrenChangedFuture != null && !childrenChangedFuture.isDone()) {
        boolean cancelled = childrenChangedFuture.cancel(true);
        if (!cancelled) {
          // task may have finished between our check and attempted cancel, this is fine.
          if (!childrenChangedFuture.isDone()) {
            LOG.warn("Could not cancel processing node children changed event, "
              + "please file a JIRA and attach logs if possible.");
          }
        }
      }
      childrenChangedFuture = asyncProcessNodeUpdate(() -> {
        try {
          final List<ZKUtil.NodeAndData> nodeList =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, aclZNode, false);
          refreshNodes(nodeList);
        } catch (KeeperException ke) {
          String msg = "ZooKeeper error while reading node children data for path " + path;
          LOG.error(msg, ke);
          watcher.abort(msg, ke);
        }
      });
    }
  }

  private Future<?> asyncProcessNodeUpdate(Runnable runnable) {
    if (!executor.isShutdown()) {
      try {
        return executor.submit(runnable);
      } catch (RejectedExecutionException e) {
        if (executor.isShutdown()) {
          LOG.warn("aclZNode changed after ZKPermissionWatcher was shutdown");
        } else {
          throw e;
        }
      }
    }
    return null; // No task launched so there will be nothing to cancel later
  }

  private void refreshNodes(List<ZKUtil.NodeAndData> nodes) {
    for (ZKUtil.NodeAndData n : nodes) {
      if (Thread.interrupted()) {
        // Use Thread.interrupted so that we clear interrupt status
        break;
      }
      if (n.isEmpty()) continue;
      String path = n.getNode();
      String entry = (ZKUtil.getNodeName(path));
      try {
        refreshAuthManager(entry, n.getData());
      } catch (IOException ioe) {
        LOG.error("Failed parsing permissions for table '" + entry + "' from zk", ioe);
      }
    }
  }

  private void refreshAuthManager(String entry, byte[] nodeData) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating permissions cache from {} with data {}", entry,
        Bytes.toStringBinary(nodeData));
    }
    if (PermissionStorage.isNamespaceEntry(entry)) {
      authManager.refreshNamespaceCacheFromWritable(PermissionStorage.fromNamespaceEntry(entry),
        nodeData);
    } else {
      authManager.refreshTableCacheFromWritable(TableName.valueOf(entry), nodeData);
    }
  }

  /***
   * Write a table's access controls to the permissions mirror in zookeeper
   */
  public void writeToZookeeper(byte[] entry, byte[] permsData) {
    String entryName = Bytes.toString(entry);
    String zkNode = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, ACL_NODE);
    zkNode = ZNodePaths.joinZNode(zkNode, entryName);

    try {
      ZKUtil.createWithParents(watcher, zkNode);
      ZKUtil.updateExistingNodeData(watcher, zkNode, permsData, -1);
    } catch (KeeperException e) {
      LOG.error("Failed updating permissions for entry '" + entryName + "'", e);
      watcher.abort("Failed writing node " + zkNode + " to zookeeper", e);
    }
  }

  /***
   * Delete the acl notify node of table
   */
  public void deleteTableACLNode(final TableName tableName) {
    String zkNode = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, ACL_NODE);
    zkNode = ZNodePaths.joinZNode(zkNode, tableName.getNameAsString());

    try {
      ZKUtil.deleteNode(watcher, zkNode);
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("No acl notify node of table '" + tableName + "'");
    } catch (KeeperException e) {
      LOG.error("Failed deleting acl node of table '" + tableName + "'", e);
      watcher.abort("Failed deleting node " + zkNode, e);
    }
  }

  /***
   * Delete the acl notify node of namespace
   */
  public void deleteNamespaceACLNode(final String namespace) {
    String zkNode = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, ACL_NODE);
    zkNode = ZNodePaths.joinZNode(zkNode, PermissionStorage.NAMESPACE_PREFIX + namespace);

    try {
      ZKUtil.deleteNode(watcher, zkNode);
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("No acl notify node of namespace '" + namespace + "'");
    } catch (KeeperException e) {
      LOG.error("Failed deleting acl node of namespace '" + namespace + "'", e);
      watcher.abort("Failed deleting node " + zkNode, e);
    }
  }
}
