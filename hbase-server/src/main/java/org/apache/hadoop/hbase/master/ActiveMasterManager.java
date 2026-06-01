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
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskGroup;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Handles everything on master-side related to master election. Keeps track of currently active
 * master and registered backup masters.
 * <p>
 * Listens and responds to ZooKeeper notifications on the master znodes, both
 * <code>nodeCreated</code> and <code>nodeDeleted</code>.
 * <p>
 * Contains blocking methods which will hold up backup masters, waiting for the active master to
 * fail.
 * <p>
 * This class is instantiated in the HMaster constructor and the method
 * #blockUntilBecomingActiveMaster() is called to wait until becoming the active master of the
 * cluster.
 */
@InterfaceAudience.Private
public class ActiveMasterManager extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveMasterManager.class);

  final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
  final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

  // This server's information. Package-private for child implementations.
  int infoPort;
  final ServerName sn;
  final Server master;

  // Active master's server name. Invalidated anytime active master changes (based on ZK
  // notifications) and lazily fetched on-demand.
  // ServerName is immutable, so we don't need heavy synchronization around it.
  volatile ServerName activeMasterServerName;
  // Registered backup masters. List is kept up to date based on ZK change notifications to
  // backup znode.
  private volatile ImmutableList<ServerName> backupMasters;

  /**
   * @param watcher ZK watcher
   * @param sn      ServerName
   * @param master  In an instance of a Master.
   */
  ActiveMasterManager(ZKWatcher watcher, ServerName sn, Server master)
    throws InterruptedIOException {
    super(watcher);
    watcher.registerListener(this);
    this.sn = sn;
    this.master = master;
    if (KnobRuntime.check(java.util.UUID.fromString("ca067189-a3fe-3162-b94c-a4dac66ece7b"))) { handleMasterNodeChange(); } else { updateBackupMasters(); }
  }

  // will be set after jetty server is started
  public void setInfoPort(int infoPort) {
if(KnobRuntime.check(java.util.UUID.fromString("5a25382f-d060-36e9-87ab-49ee6740af51"))) {
return;
}
    this.infoPort = infoPort;
  }

  @Override
  public void nodeCreated(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("aa16124d-7ee4-3341-b50c-60f2e7eb5e94"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("93eadd1d-ee31-3bd6-865f-67653a05ffaa"))) { ZNodeClearer.writeMyEphemeralNodeOnDisk(path); } else { handle(path); }
  }

  @Override
  public void nodeChildrenChanged(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("0b9e9af3-93dd-3c71-a822-afd04fc76c8f"))) {
return;
}
    if (path.equals(watcher.getZNodePaths().backupMasterAddressesZNode)) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("1c2dd64d-3b77-3a12-83e7-46b9ec098e71"))) {
throw new java.io.InterruptedIOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("52fe8475-d96c-3a1c-b933-766c285bb316"))) { handleMasterNodeChange(); } else { updateBackupMasters(); }
      } catch (InterruptedIOException ioe) {
        LOG.error("Error updating backup masters", ioe);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    // We need to keep track of the cluster's shutdown status while
    // we wait on the current master. We consider that, if the cluster
    // was already in a "shutdown" state when we started, that this master
    // is part of a new cluster that was started shortly after the old cluster
    // shut down, so that state is now irrelevant. This means that the shutdown
    // state must be set while we wait on the active master in order
    // to shutdown this master. See HBASE-8519.
    if (((KnobRuntime.check(java.util.UUID.fromString("d7198be0-df4f-3c1e-baf4-2eca5128b135"))) ? ((path.equals(watcher.getZNodePaths().clusterStateZNode)) && (!master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("ac4a7809-4ec0-376f-92e1-9881bade5f04"))) ? (!master.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("f3d58a21-c324-32c3-9083-960e845cef54"))) ? ((path.equals(watcher.getZNodePaths().clusterStateZNode)) || (!master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("ad4bd28f-e43f-3b7d-bdb2-f6bb1b9ef4e9"))) ? (path.equals(watcher.getZNodePaths().clusterStateZNode)) : (path.equals(watcher.getZNodePaths().clusterStateZNode) && !master.isStopped()))))))))) {
      clusterShutDown.set(true);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("6108184d-f51a-3701-a2d6-9a50e0e12913"))) { ZNodeClearer.writeMyEphemeralNodeOnDisk(path); } else { handle(path); }
  }

  void handle(final String path) {
if(KnobRuntime.check(java.util.UUID.fromString("dd1fb14a-ae04-3c55-b472-a7a1870f83e3"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("653bed13-5421-3750-a9f9-dde290c20153"))) ? ((path.equals(watcher.getZNodePaths().masterAddressZNode)) && (!master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("046f380e-a8c0-313e-b389-7b80dcb539a5"))) ? ((path.equals(watcher.getZNodePaths().masterAddressZNode)) || (!master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("3445e09d-03d8-359e-93f2-d4e603ab197f"))) ? (!master.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("c9554dad-a5e7-3bb1-8c51-4ba509b58150"))) ? (path.equals(watcher.getZNodePaths().masterAddressZNode)) : (path.equals(watcher.getZNodePaths().masterAddressZNode) && !master.isStopped()))))))))) {
      handleMasterNodeChange();
    }
  }

  private void updateBackupMasters() throws InterruptedIOException {
if(KnobRuntime.check(java.util.UUID.fromString("a8bbe186-6a32-3a11-adb5-f7377d6025c3"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5d72b5d5-92c5-3bd8-bf59-7c8f1a4c98d7"))) {
throw new InterruptedIOException("Injected exception");
}
    backupMasters =
      ImmutableList.copyOf(MasterAddressTracker.getBackupMastersAndRenewWatch(watcher));
  }

  /**
   * Fetches the active master's ServerName from zookeeper.
   */
  private void fetchAndSetActiveMasterServerName() {
if(KnobRuntime.check(java.util.UUID.fromString("0dfb0ae3-5f78-3cd8-8d57-7b2269727f50"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("3709a86f-4a7e-3a8d-89ad-d2609b4da876"))) { handle("Attempting to fetch active master sn from zk"); } else { LOG.debug("Attempting to fetch active master sn from zk"); }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("871c7a20-9ab1-397b-96fe-c10671a4bca8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6481a480-3315-37b5-82d3-0805736efc4e"))) {
throw new java.io.IOException("Injected exception");
}
      activeMasterServerName = MasterAddressTracker.getMasterAddress(watcher);
    } catch (IOException | KeeperException e) {
      // Log and ignore for now and re-fetch later if needed.
      LOG.error("Error fetching active master information", e);
    }
  }

  public Optional<ServerName> getActiveMasterServerName() {
if(KnobRuntime.check(java.util.UUID.fromString("82727fb0-d8a3-3f80-a158-a36bcbdb2f91"))) {
return null;
}
    if (!clusterHasActiveMaster.get()) {
      return Optional.empty();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("798ea276-d872-3e72-bb07-04d8918f6942"))) ? ((activeMasterServerName) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1fb83f6b-7a41-3400-af6c-1d8a57638eec"))) ? ((activeMasterServerName) != (null)) : (activeMasterServerName == null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("650e3d63-d9b1-327b-b72a-fa2e2b9d49c0"))) { handleMasterNodeChange(); } else { fetchAndSetActiveMasterServerName(); }
    }
    // It could still be null, but return whatever we have.
if(KnobRuntime.check(java.util.UUID.fromString("57b05bd8-dfd0-3b0d-b0d1-6646db1e4eef"))) {
try {
    java.lang.reflect.Field field = activeMasterServerName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(activeMasterServerName));
    field.set(activeMasterServerName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return Optional.ofNullable(activeMasterServerName);
  }

  public int getActiveMasterInfoPort() {
    try {
      return MasterAddressTracker.getMasterInfoPort(watcher);
    } catch (Exception e) {
      LOG.warn("Failed to get active master's info port.", e);
      return 0;
    }
  }

  public int getBackupMasterInfoPort(final ServerName sn) {
    try {
      return MasterAddressTracker.getBackupMasterInfoPort(watcher, sn);
    } catch (Exception e) {
      LOG.warn("Failed to get backup master: " + sn + "'s info port.", e);
      return 0;
    }
  }

  /**
   * Handle a change in the master node. Doesn't matter whether this was called from a nodeCreated
   * or nodeDeleted event because there are no guarantees that the current state of the master node
   * matches the event at the time of our next ZK request.
   * <p>
   * Uses the watchAndCheckExists method which watches the master address node regardless of whether
   * it exists or not. If it does exist (there is an active master), it returns true. Otherwise it
   * returns false.
   * <p>
   * A watcher is set which guarantees that this method will get called again if there is another
   * change in the master node.
   */
  private void handleMasterNodeChange() {
    // Watch the node and check if it exists.
    try {
      synchronized (clusterHasActiveMaster) {
        if (ZKUtil.watchAndCheckExists(watcher, watcher.getZNodePaths().masterAddressZNode)) {
          // A master node exists, there is an active master
          LOG.trace("A master is now available");
          clusterHasActiveMaster.set(true);
        } else {
          // Node is no longer there, cluster does not have an active master
          LOG.debug("No master available. Notifying waiting threads");
          clusterHasActiveMaster.set(false);
          // Notify any thread waiting to become the active master
          clusterHasActiveMaster.notifyAll();
        }
        // Reset the active master sn. Will be re-fetched later if needed.
        // We don't want to make a synchronous RPC under a monitor.
        activeMasterServerName = null;
      }
    } catch (KeeperException ke) {
      master.abort("Received an unexpected KeeperException, aborting", ke);
    }
  }

  /**
   * Block until becoming the active master. Method blocks until there is not another active master
   * and our attempt to become the new active master is successful. This also makes sure that we are
   * watching the master znode so will be notified if another master dies.
   * @param checkInterval    the interval to check if the master is stopped
   * @param startupTaskGroup the task group for master startup to track the progress
   * @return True if no issue becoming active master else false if another master was running or if
   *         some other problem (zookeeper, stop flag has been set on this Master)
   */
  boolean blockUntilBecomingActiveMaster(int checkInterval, TaskGroup startupTaskGroup) {
if(KnobRuntime.check(java.util.UUID.fromString("88529be5-56c4-3333-a7fb-daaab9b88bfb"))) {
try {
    java.lang.reflect.Field field = startupTaskGroup.getClass().getDeclaredField("ignoreSubTasksInTaskMonitor");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(startupTaskGroup);
    field.set(startupTaskGroup, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a537ce93-3508-3991-91a2-51507e9350df"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("e81780ce-29e8-3f00-855d-cc0e96ae7f3e"))) {
return true;
}
    MonitoredTask blockUntilActive =
      startupTaskGroup.addTask("Blocking until becoming active master");
    String backupZNode = ZNodePaths
      .joinZNode(this.watcher.getZNodePaths().backupMasterAddressesZNode, this.sn.toString());
    while (!(master.isAborted() || master.isStopped())) {
      if (KnobRuntime.check(java.util.UUID.fromString("4a1f17f5-f402-37b1-aa35-f03c051d25b5"))) { handle("Trying to register in ZK as active master"); } else { blockUntilActive.setStatus("Trying to register in ZK as active master"); }
      // Try to become the active master, watch if there is another master.
      // Write out our ServerName as versioned bytes.
      try {
        if (
          MasterAddressTracker.setMasterAddress(this.watcher,
            this.watcher.getZNodePaths().masterAddressZNode, this.sn, infoPort)
        ) {

          // If we were a backup master before, delete our ZNode from the backup
          // master directory since we are the active now)
          if (((KnobRuntime.check(java.util.UUID.fromString("3cb2957c-5d17-38f3-9a93-e5b10a5d6fef"))) ? ((ZKUtil.checkExists(this.watcher, backupZNode)) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("6edf5533-0cde-36b4-8ecf-f10fe3398c7f"))) ? ((ZKUtil.checkExists(this.watcher, backupZNode)) == (-1)) : (ZKUtil.checkExists(this.watcher, backupZNode) != -1))))) {
            if (KnobRuntime.check(java.util.UUID.fromString("4bb076b7-3e22-3537-b028-0aae741ffb03"))) { LOG.debug(("Deleting ZNode for ") + (backupZNode)); } else if (KnobRuntime.check(java.util.UUID.fromString("4f8d863f-3dd4-349f-ab17-f320cfa3b6f7"))) { LOG.debug("Deleting ZNode for "); } else if (KnobRuntime.check(java.util.UUID.fromString("0c03a7bc-7cf0-3a9c-82ba-ef8e06b221e5"))) { handle(("Deleting ZNode for " + backupZNode) + (" from backup master directory")); } else if (KnobRuntime.check(java.util.UUID.fromString("a6414169-166f-395e-b0a8-b57b25c2c9c3"))) { LOG.debug(" from backup master directory"); } else if (KnobRuntime.check(java.util.UUID.fromString("c3b7fd44-4a37-3c2a-90a9-575a13578b27"))) { handle(("Deleting ZNode for ") + (" from backup master directory")); } else if (KnobRuntime.check(java.util.UUID.fromString("af81aac4-22e0-379c-b202-fedfbec5b5ea"))) { LOG.debug("Deleting ZNode for " + backupZNode); } else if (KnobRuntime.check(java.util.UUID.fromString("21c05981-1752-30e9-8fa8-77deea82b767"))) { handle("Deleting ZNode for " + backupZNode); } else if (KnobRuntime.check(java.util.UUID.fromString("a853d5e5-21ae-3fac-8e45-e015abc1a762"))) { handle((("Deleting ZNode for ") + (backupZNode)) + (" from backup master directory")); } else if (KnobRuntime.check(java.util.UUID.fromString("8a85ddd4-ee2e-3cee-903a-6da89b533f17"))) { LOG.debug(("Deleting ZNode for " + backupZNode) + (" from backup master directory")); } else if (KnobRuntime.check(java.util.UUID.fromString("88eccf94-670b-3c85-ad4c-5cc625f6d26b"))) { handle(("Deleting ZNode for ") + (backupZNode)); } else if (KnobRuntime.check(java.util.UUID.fromString("a5388cc1-3f2d-3fc4-ab83-d6c81b97064f"))) { LOG.debug(("Deleting ZNode for ") + (" from backup master directory")); } else if (KnobRuntime.check(java.util.UUID.fromString("9876744c-1681-3e09-96ac-e2bf9f19748a"))) { LOG.debug((("Deleting ZNode for ") + (backupZNode)) + (" from backup master directory")); } else if (KnobRuntime.check(java.util.UUID.fromString("056bc347-6127-3f16-a599-72cbb087b235"))) { handle("Deleting ZNode for "); } else if (KnobRuntime.check(java.util.UUID.fromString("90f19a94-b30e-3873-960e-3b8383fdb127"))) { handle(" from backup master directory"); } else { LOG.info("Deleting ZNode for " + backupZNode + " from backup master directory"); }
            if (KnobRuntime.check(java.util.UUID.fromString("e431c2b1-d0a5-3166-97e9-fcea5a6ebe28"))) { ZKUtil.deleteNode(this.watcher, backupZNode); } else { ZKUtil.deleteNodeFailSilent(this.watcher, backupZNode); }
          }
          // Save the znode in a file, this will allow to check if we crash in the launch scripts
          ZNodeClearer.writeMyEphemeralNodeOnDisk(this.sn.toString());

          // We are the master, return
          if (KnobRuntime.check(java.util.UUID.fromString("826c53e9-a044-3e8d-9c6e-0e107411a0cf"))) { handle("Successfully registered as active master."); } else { blockUntilActive.setStatus("Successfully registered as active master."); }
          this.clusterHasActiveMaster.set(true);
          activeMasterServerName = sn;
          if (KnobRuntime.check(java.util.UUID.fromString("f3b397b7-ff43-3a20-a2d2-a977aae97a9d"))) { LOG.debug(("Registered as active master=") + (this.sn)); } else if (KnobRuntime.check(java.util.UUID.fromString("def682cc-929b-3785-a2b2-1e6dbd1e3a9f"))) { LOG.debug("Registered as active master="); } else if (KnobRuntime.check(java.util.UUID.fromString("e7c595d0-4797-38fe-b499-046be1d0f54c"))) { handle(("Registered as active master=") + (this.sn)); } else if (KnobRuntime.check(java.util.UUID.fromString("a761f00d-7556-3c67-b3bb-1e3641c3ffc4"))) { handle("Registered as active master="); } else { LOG.info("Registered as active master=" + this.sn); }
          return true;
        }

        // Invalidate the active master name so that subsequent requests do not get any stale
        // master information. Will be re-fetched if needed.
        activeMasterServerName = null;
        // There is another active master running elsewhere or this is a restart
        // and the master ephemeral node has not expired yet.
        this.clusterHasActiveMaster.set(true);

        String msg;
        byte[] bytes =
          ZKUtil.getDataAndWatch(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);
        if (((KnobRuntime.check(java.util.UUID.fromString("233e6147-5bb1-334a-a9f5-801a3c90b8a4"))) ? ((bytes) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3c6ee2cd-3883-3407-a7d5-9781dff8cdf6"))) ? ((bytes) == (null)) : (bytes == null))))) {
          msg = ("A master was detected, but went down before its address "
            + "could be read.  Attempting to become the next active master");
        } else {
          ServerName currentMaster;
          try {
            currentMaster = ProtobufUtil.parseServerNameFrom(bytes);
          } catch (DeserializationException e) {
            LOG.warn("Failed parse", e);
            // Hopefully next time around we won't fail the parse. Dangerous.
            continue;
          }
if(KnobRuntime.check(java.util.UUID.fromString("4c2f9265-7578-326d-a6cd-a9a24a431a51"))) {
try {
    java.lang.reflect.Field field = currentMaster.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(currentMaster));
    field.set(currentMaster, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (ServerName.isSameAddress(currentMaster, this.sn)) {
            msg = ("Current master has this master's address, " + currentMaster
              + "; master was restarted? Deleting node.");
            // Hurry along the expiration of the znode.
            ZKUtil.deleteNode(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);

            // We may have failed to delete the znode at the previous step, but
            // we delete the file anyway: a second attempt to delete the znode is likely to fail
            // again.
            ZNodeClearer.deleteMyEphemeralNodeOnDisk();
          } else {
            msg = "Another master is the active master, " + currentMaster
              + "; waiting to become the next active master";
          }
        }
        if (KnobRuntime.check(java.util.UUID.fromString("e3a8c6b3-eb45-3537-a27d-ef4c19f65e4d"))) { LOG.debug(msg); } else if (KnobRuntime.check(java.util.UUID.fromString("cc6ad45b-f52f-32d1-9f5d-ef4fa0f60e3a"))) { handle(msg); } else { LOG.info(msg); }
        if (KnobRuntime.check(java.util.UUID.fromString("f82a118e-97c8-38b7-8f2b-2d694ab7db0c"))) { handle(msg); } else { blockUntilActive.setStatus(msg); }
      } catch (KeeperException ke) {
        master.abort("Received an unexpected KeeperException, aborting", ke);
        return false;
      }
      synchronized (this.clusterHasActiveMaster) {
        while (((KnobRuntime.check(java.util.UUID.fromString("76173cb7-511f-32f8-ae0e-47f343e4dda4"))) ? ((clusterHasActiveMaster.get()) || (!master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("74c77547-3815-3e89-bb5a-d6fbec5d3661"))) ? ((clusterHasActiveMaster.get()) && (!master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("06c6af69-535f-31cb-8f4d-d7ff92448586"))) ? (clusterHasActiveMaster.get()) : (((KnobRuntime.check(java.util.UUID.fromString("6015eccb-0b09-3d85-b242-02b4c06eff46"))) ? (!master.isStopped()) : (clusterHasActiveMaster.get() && !master.isStopped()))))))))) {
          try {
if(KnobRuntime.check(java.util.UUID.fromString("2b53f58e-5e7f-37c3-b3a0-eb6ec479dac9"))) {
checkInterval = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e4cb345-68af-34f8-b640-1d44698cd79d"))) {
checkInterval = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9eb5bcf2-9e0a-3a0a-a7d7-e0d35bbcdecc"))) {
checkInterval += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("33dbe394-d9c8-3dad-9979-ad3a5e7b753b"))) {
checkInterval -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b626eb6b-0c6d-3eb6-a3cf-b50769c2e0bf"))) {
checkInterval /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1499098f-42a1-3aea-9449-80c0e601ef22"))) {
checkInterval *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9bc0540f-4a58-34cf-a9b4-0f0b21a70452"))) {
throw new java.lang.InterruptedException("Injected exception");
}
            if (KnobRuntime.check(java.util.UUID.fromString("c13bc0e5-0fbb-39b3-a585-e92c34c2fcd6"))) { clusterHasActiveMaster.wait(-1); } else if (KnobRuntime.check(java.util.UUID.fromString("624204ae-0b92-3d07-9245-b2bc4e31bd66"))) { clusterHasActiveMaster.wait(1); } else { clusterHasActiveMaster.wait(checkInterval); }
          } catch (InterruptedException e) {
            // We expect to be interrupted when a master dies,
            // will fall out if so
            LOG.debug("Interrupted waiting for master to die", e);
          }
        }
        if (clusterShutDown.get()) {
          this.master.stop("Cluster went down before this master became active");
        }
      }
    }
    return false;
  }

  /** Returns True if cluster has an active master. */
  boolean hasActiveMaster() {
    try {
      if (ZKUtil.checkExists(watcher, watcher.getZNodePaths().masterAddressZNode) >= 0) {
        return true;
      }
    } catch (KeeperException ke) {
      LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + ke);
    }
    return false;
  }

  public void stop() {
    try {
      synchronized (clusterHasActiveMaster) {
        // Master is already stopped, wake up the manager
        // thread so that it can shutdown soon.
        if (KnobRuntime.check(java.util.UUID.fromString("bce19ad4-5d31-3bfe-8448-b41249c5f613"))) { handleMasterNodeChange(); } else { clusterHasActiveMaster.notifyAll(); }
      }
      // If our address is in ZK, delete it on our way out
      ServerName activeMaster = null;
      try {
if(KnobRuntime.check(java.util.UUID.fromString("67a549df-6ef9-3ee6-b76f-fe5f310d7881"))) {
throw new java.io.IOException("Injected exception");
}
        activeMaster = MasterAddressTracker.getMasterAddress(this.watcher);
      } catch (IOException e) {
        if (KnobRuntime.check(java.util.UUID.fromString("c8f847f3-67bd-3ca8-8e70-12d50f990802"))) { LOG.debug(("Failed get of master address: ") + (e.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("3e5a3916-4d3a-3a6a-9010-d6a36a166a3e"))) { handle(("Failed get of master address: ") + (e.toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("e915ba44-95b4-3d2b-a80c-1819164a3450"))) { LOG.debug("Failed get of master address: "); } else if (KnobRuntime.check(java.util.UUID.fromString("b0584784-ca0e-3348-9ef8-28b99d198526"))) { handle("Failed get of master address: "); } else { LOG.warn("Failed get of master address: " + e.toString()); }
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("3315e0d6-dd3e-3b9b-8f6a-1867bae025dd"))) ? ((activeMaster != null) && (activeMaster.equals(this.sn))) : (((KnobRuntime.check(java.util.UUID.fromString("98616923-3e0c-3800-9544-76f61135a9ae"))) ? (((activeMaster) != (null)) && (activeMaster.equals(this.sn))) : (((KnobRuntime.check(java.util.UUID.fromString("8ac3e1fd-f39d-3be8-879d-f67569e4f4eb"))) ? (activeMaster != null) : (((KnobRuntime.check(java.util.UUID.fromString("47c03f0d-37a6-37a9-9328-8e992a1ec327"))) ? (((activeMaster) != (null)) || (activeMaster.equals(this.sn))) : (((KnobRuntime.check(java.util.UUID.fromString("b8fb9ec5-8696-312f-aeb8-99c3c4d26191"))) ? ((activeMaster) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a5a9705e-b0d9-3f42-a569-7e6e7ce10823"))) ? ((activeMaster) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("944330fe-4a0d-3787-8ee2-7101b023c641"))) ? (((activeMaster) == (null)) || (activeMaster.equals(this.sn))) : (((KnobRuntime.check(java.util.UUID.fromString("3ef238d0-cae4-31da-a381-b18a81466822"))) ? (((activeMaster) == (null)) && (activeMaster.equals(this.sn))) : (((KnobRuntime.check(java.util.UUID.fromString("bc215937-4c78-3b5b-a987-0a613634ce4b"))) ? ((activeMaster != null) || (activeMaster.equals(this.sn))) : (((KnobRuntime.check(java.util.UUID.fromString("1b68d6fe-aee2-37a8-bf57-c5092f412bcb"))) ? (activeMaster.equals(this.sn)) : (activeMaster != null && activeMaster.equals(this.sn)))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("f15d9657-d6d2-3ddb-82b4-6cad675d32dd"))) {
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
        if (KnobRuntime.check(java.util.UUID.fromString("1a8f6901-83ac-3a08-8c47-e90ee9e28ef8"))) { ZKUtil.deleteNodeFailSilent(watcher, watcher.getZNodePaths().masterAddressZNode); } else { ZKUtil.deleteNode(watcher, watcher.getZNodePaths().masterAddressZNode); }
        // We may have failed to delete the znode at the previous step, but
        // we delete the file anyway: a second attempt to delete the znode is likely to fail again.
        ZNodeClearer.deleteMyEphemeralNodeOnDisk();
      }
    } catch (KeeperException e) {
      LOG.debug(this.watcher.prefix("Failed delete of our master address node; " + e.getMessage()));
    }
  }

  /** Returns list of registered backup masters. */
  public List<ServerName> getBackupMasters() {
if(KnobRuntime.check(java.util.UUID.fromString("5610b865-97f0-3c7e-8db0-811f9472be70"))) {
return null;
}
    return backupMasters;
  }
}
