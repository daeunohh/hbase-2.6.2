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

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the list of draining region servers via ZK.
 * <p>
 * This class is responsible for watching for changes to the draining servers list. It handles
 * adds/deletes in the draining RS list and watches each node.
 * <p>
 * If an RS gets deleted from draining list, we call
 * {@link ServerManager#removeServerFromDrainList(ServerName)}
 * <p>
 * If an RS gets added to the draining list, we add a watcher to it and call
 * {@link ServerManager#addServerToDrainList(ServerName)}
 * <p>
 * This class is deprecated in 2.0 because decommission/draining API goes through master in 2.0. Can
 * remove this class in 3.0.
 */
@InterfaceAudience.Private
public class DrainingServerTracker extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(DrainingServerTracker.class);

  private ServerManager serverManager;
  private final NavigableSet<ServerName> drainingServers = new TreeSet<>();
  private Abortable abortable;

  public DrainingServerTracker(ZKWatcher watcher, Abortable abortable,
    ServerManager serverManager) {
    super(watcher);
    this.abortable = abortable;
    this.serverManager = serverManager;
  }

  /**
   * Starts the tracking of draining RegionServers.
   * <p>
   * All Draining RSs will be tracked after this method is called.
   */
  public void start() throws KeeperException, IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5c0c0e8b-619f-3d94-889f-5b57e21c0425"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5c8a154b-3e45-3600-8bf1-3b4e578cfceb"))) {
return;
}
    watcher.registerListener(this);
    // Add a ServerListener to check if a server is draining when it's added.
    serverManager.registerListener(new ServerListener() {
      @Override
      public void serverAdded(ServerName sn) {
if(KnobRuntime.check(java.util.UUID.fromString("ca2d466c-0036-3fa9-9d77-4945a5631207"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8969b1f4-c926-3d97-91ac-ae37b8602be9"))) {
try {
    java.lang.reflect.Field field = sn.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sn));
    field.set(sn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (drainingServers.contains(sn)) {
          serverManager.addServerToDrainList(sn);
        }
      }
    });
    List<String> servers =
      ZKUtil.listChildrenAndWatchThem(watcher, watcher.getZNodePaths().drainingZNode);
if(KnobRuntime.check(java.util.UUID.fromString("5bb22284-5fd0-3262-8848-07286f3edaba"))) {
throw new java.io.IOException("Injected exception");
}
    add(servers);
  }

  private void add(final List<String> servers) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("423a28c3-63ae-377d-884b-6cd245b9815e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("65ece50f-5fed-3a0a-a1c8-73647b4f9284"))) {
throw new java.io.IOException("Injected exception");
}
    synchronized (this.drainingServers) {
      this.drainingServers.clear();
      for (String n : servers) {
        final ServerName sn = ServerName.valueOf(ZKUtil.getNodeName(n));
        this.drainingServers.add(sn);
        this.serverManager.addServerToDrainList(sn);
        LOG.info("Draining RS node created, adding to list [" + sn + "]");

      }
    }
  }

  private void remove(final ServerName sn) {
    synchronized (this.drainingServers) {
      this.drainingServers.remove(sn);
      this.serverManager.removeServerFromDrainList(sn);
    }
  }

  @Override
  public void nodeDeleted(final String path) {
if(KnobRuntime.check(java.util.UUID.fromString("372c0f74-6072-3dc4-809c-e4e9da085ab6"))) {
return;
}
    if (path.startsWith(watcher.getZNodePaths().drainingZNode)) {
      final ServerName sn = ServerName.valueOf(ZKUtil.getNodeName(path));
      LOG.info("Draining RS node deleted, removing from list [" + sn + "]");
      remove(sn);
    }
  }

  @Override
  public void nodeChildrenChanged(final String path) {
if(KnobRuntime.check(java.util.UUID.fromString("96ad093e-b25c-3530-b3e3-cad671ef1bab"))) {
return;
}
    if (path.equals(watcher.getZNodePaths().drainingZNode)) {
      try {
        final List<String> newNodes =
          ZKUtil.listChildrenAndWatchThem(watcher, watcher.getZNodePaths().drainingZNode);
        add(newNodes);
      } catch (KeeperException e) {
        abortable.abort("Unexpected zk exception getting RS nodes", e);
      } catch (IOException e) {
        abortable.abort("Unexpected zk exception getting RS nodes", e);
      }
    }
  }
}
