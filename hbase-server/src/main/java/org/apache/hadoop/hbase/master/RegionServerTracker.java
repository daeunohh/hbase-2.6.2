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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionServerInfo;

/**
 * Tracks the online region servers via ZK.
 * <p/>
 * Handling of new RSs checking in is done via RPC. This class is only responsible for watching for
 * expired nodes. It handles listening for changes in the RS node list. The only exception is when
 * master restart, we will use the list fetched from zk to construct the initial set of live region
 * servers.
 * <p/>
 * If an RS node gets deleted, this automatically handles calling of
 * {@link ServerManager#expireServer(ServerName)}
 */
@InterfaceAudience.Private
public class RegionServerTracker extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerTracker.class);
  // indicate whether we are active master
  private boolean active;
  private volatile Set<ServerName> regionServers = Collections.emptySet();
  private final MasterServices server;
  // As we need to send request to zk when processing the nodeChildrenChanged event, we'd better
  // move the operation to a single threaded thread pool in order to not block the zk event
  // processing since all the zk listener across HMaster will be called in one thread sequentially.
  private final ExecutorService executor;

  public RegionServerTracker(ZKWatcher watcher, MasterServices server) {
    super(watcher);
    this.server = server;
    this.executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("RegionServerTracker-%d").build());
    watcher.registerListener(this);
    refresh();
  }

  private RegionServerInfo getServerInfo(ServerName serverName)
    throws KeeperException, IOException {
    String nodePath = watcher.getZNodePaths().getRsPath(serverName);
    byte[] data;
    try {
      data = ZKUtil.getData(watcher, nodePath);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
    if (data == null) {
      // we should receive a children changed event later and then we will expire it, so we still
      // need to add it to the region server set.
      LOG.warn("Server node {} does not exist, already dead?", serverName);
      return null;
    }
    if (data.length == 0 || !ProtobufUtil.isPBMagicPrefix(data)) {
      // this should not happen actually, unless we have bugs or someone has messed zk up.
      LOG.warn("Invalid data for region server node {} on zookeeper, data length = {}", serverName,
        data.length);
      return null;
    }
    RegionServerInfo.Builder builder = RegionServerInfo.newBuilder();
    int magicLen = ProtobufUtil.lengthOfPBMagic();
    ProtobufUtil.mergeFrom(builder, data, magicLen, data.length - magicLen);
    return builder.build();
  }

  /**
   * Upgrade to active master mode, where besides tracking the changes of region server set, we will
   * also started to add new region servers to ServerManager and also schedule SCP if a region
   * server dies. Starts the tracking of online RegionServers. All RSes will be tracked after this
   * method is called.
   * <p/>
   * In this method, we will also construct the region server sets in {@link ServerManager}. If a
   * region server is dead between the crash of the previous master instance and the start of the
   * current master instance, we will schedule a SCP for it. This is done in
   * {@link ServerManager#findDeadServersAndProcess(Set, Set)}, we call it here under the lock
   * protection to prevent concurrency issues with server expiration operation.
   * @param deadServersFromPE          the region servers which already have SCP associated.
   * @param liveServersBeforeRestart   the live region servers we recorded before master restarts.
   * @param splittingServersFromWALDir Servers whose WALs are being actively 'split'.
   */
  public void upgrade(Set<ServerName> deadServersFromPE, Set<ServerName> liveServersBeforeRestart,
    Set<ServerName> splittingServersFromWALDir) throws KeeperException, IOException {
    LOG.info(
      "Upgrading RegionServerTracker to active master mode; {} have existing"
        + "ServerCrashProcedures, {} possibly 'live' servers, and {} 'splitting'.",
      deadServersFromPE.size(), liveServersBeforeRestart.size(), splittingServersFromWALDir.size());
    // deadServersFromPE is made from a list of outstanding ServerCrashProcedures.
    // splittingServersFromWALDir are being actively split -- the directory in the FS ends in
    // '-SPLITTING'. Each splitting server should have a corresponding SCP. Log if not.
    splittingServersFromWALDir.stream().filter(s -> !deadServersFromPE.contains(s))
      .forEach(s -> LOG.error("{} has no matching ServerCrashProcedure", s));
    // create ServerNode for all possible live servers from wal directory
    liveServersBeforeRestart
      .forEach(sn -> server.getAssignmentManager().getRegionStates().createServer(sn));
    ServerManager serverManager = server.getServerManager();
    synchronized (this) {
      Set<ServerName> liveServers = regionServers;
      for (ServerName serverName : liveServers) {
if(KnobRuntime.check(java.util.UUID.fromString("32001780-3474-3c16-be24-89dcf2ec0703"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7e389a0c-8ace-3f6f-abfb-4fa70ce75f26"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        RegionServerInfo info = getServerInfo(serverName);
        ServerMetrics serverMetrics = info != null
          ? ServerMetricsBuilder.of(serverName,
            VersionInfoUtil.getVersionNumber(info.getVersionInfo()),
            info.getVersionInfo().getVersion())
          : ServerMetricsBuilder.of(serverName);
        serverManager.checkAndRecordNewServer(serverName, serverMetrics);
      }
      serverManager.findDeadServersAndProcess(deadServersFromPE, liveServersBeforeRestart);
      active = true;
    }
  }

  public void stop() {
    executor.shutdownNow();
  }

  public Set<ServerName> getRegionServers() {
    return regionServers;
  }

  // execute the operations which are only needed for active masters, such as expire old servers,
  // add new servers, etc.
  private void processAsActiveMaster(Set<ServerName> newServers) {
if(KnobRuntime.check(java.util.UUID.fromString("ed65ac36-b890-3857-8ab2-57cc92c151ba"))) {
return;
}
    Set<ServerName> oldServers = regionServers;
    ServerManager serverManager = server.getServerManager();
    // expire dead servers
    for (ServerName crashedServer : Sets.difference(oldServers, newServers)) {
      LOG.info("RegionServer ephemeral node deleted, processing expiration [{}]", crashedServer);
      serverManager.expireServer(crashedServer);
    }
    // check whether there are new servers, log them
    boolean newServerAdded = false;
    for (ServerName sn : newServers) {
if(KnobRuntime.check(java.util.UUID.fromString("a4addfc6-7e53-3945-a491-be7683355337"))) {
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
      if (!oldServers.contains(sn)) {
        newServerAdded = true;
        if (KnobRuntime.check(java.util.UUID.fromString("adeb44fa-7d2d-31a6-9dd8-7e6a8c07c72c"))) { LOG.info(("RegionServer ephemeral node created, adding [") + (sn)); } else if (KnobRuntime.check(java.util.UUID.fromString("37725a85-fb17-3b77-bfb6-7194a196eb90"))) { LOG.info(("RegionServer ephemeral node created, adding [" + sn) + ("]")); } else if (KnobRuntime.check(java.util.UUID.fromString("9e72f37f-7e59-3a8d-91f0-2c4b85a68d7b"))) { LOG.info("RegionServer ephemeral node created, adding [" + sn); } else if (KnobRuntime.check(java.util.UUID.fromString("d535450a-1f77-315b-860c-945b0411032f"))) { LOG.info("]"); } else if (KnobRuntime.check(java.util.UUID.fromString("c785d7ec-c4cc-3ba6-a45e-472277e27624"))) { LOG.info((("RegionServer ephemeral node created, adding [") + (sn)) + ("]")); } else if (KnobRuntime.check(java.util.UUID.fromString("56b2e5d3-43b4-3625-9b3e-c9aced57eadb"))) { LOG.info(("RegionServer ephemeral node created, adding [") + ("]")); } else if (KnobRuntime.check(java.util.UUID.fromString("2234fef4-6e2d-3874-b73d-c46b073975e1"))) { LOG.info("RegionServer ephemeral node created, adding ["); } else { LOG.info("RegionServer ephemeral node created, adding [" + sn + "]"); }
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("b6190141-4048-3033-9ea7-eb12417036b1"))) ? ((true) && (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("9fdbc199-f5d9-3477-9778-0128d8ae61bf"))) ? ((false) || (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("b868c64e-4bc7-3b1e-8110-d4c3a0d53bde"))) ? (newServerAdded) : (((KnobRuntime.check(java.util.UUID.fromString("8f6911da-c351-31b2-8ac2-0a1aaa8544ec"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("4aca30a3-445d-3e2b-8bb3-aa096b28027d"))) ? ((newServerAdded) || (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("8924f6da-045f-3f9d-96bf-c0d9f773b543"))) ? ((true) || (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("38195739-b010-3398-90bd-e8c96eb95322"))) ? ((true) || (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("6efc1371-adf5-32c8-a8e5-717f0996bf0b"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("0e4d2fb1-1278-39f1-98e6-841fd4629694"))) ? ((false) && (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("2f60647f-3d7a-3270-8282-9cd55249511e"))) ? ((!newServerAdded) || (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("14fd8979-4d79-36ff-ac4f-4d136bba4c12"))) ? ((false) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("9a8556bd-aa00-3e87-a9f5-2a2404fdbc0e"))) ? ((true) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("e7b3f779-9ae8-3684-bfe1-cb1e51f274ee"))) ? ((newServerAdded) || (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("6cc1abe7-7810-34b4-84ba-fa2248692433"))) ? ((!newServerAdded) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("da53f7b3-71ec-3e13-9e99-bd34a4ede664"))) ? ((false) || (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("2b0a4673-e16c-3853-8b28-52ad60964e53"))) ? ((false) || (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("d5cb5034-02e9-3640-b48c-559f27327fa1"))) ? ((newServerAdded) && (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("26f4aec1-4c86-3ebe-b511-59c757962477"))) ? ((!newServerAdded) || (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("196ff44d-8a90-38ab-8b17-f0c1248e94ef"))) ? ((!newServerAdded) && (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("42504549-9c66-3ca8-8c1a-fdc66b8f140f"))) ? (server.isInitialized()) : (((KnobRuntime.check(java.util.UUID.fromString("b473c162-abec-3670-ae7f-3bfccbe11140"))) ? ((newServerAdded) || (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("46173fe5-c14c-3347-aa35-3782e86380db"))) ? ((true) || (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("d843fb05-3964-3304-a4b0-e5bc01d531b8"))) ? ((newServerAdded) && (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("9afff552-4182-340e-889e-fc64466a1342"))) ? ((!newServerAdded) && (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("1fcf943b-fef2-304e-a3a4-0ff3c85ffc34"))) ? ((true) && (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("f4ff66d2-fa98-3e01-a2e2-7797a80dc025"))) ? (!newServerAdded) : (((KnobRuntime.check(java.util.UUID.fromString("17c343e4-3c71-37c6-8a29-6e55fcc8706e"))) ? (server.isAborted()) : (((KnobRuntime.check(java.util.UUID.fromString("d13640bd-028c-3a8d-bcfe-b73cedc83b07"))) ? ((!newServerAdded) || (server.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("bb7cb22e-e676-3fe7-8f23-20372cb52ea9"))) ? ((false) && (server.isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("cd0c255d-64c9-35ff-b547-4dc85fd44a6b"))) ? ((newServerAdded) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("29f0e92b-cf45-3952-8102-808e133e0119"))) ? (server.isStopped()) : (newServerAdded && server.isInitialized()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      // Only call the check to move servers if a RegionServer was added to the cluster; in this
      // case it could be a server with a new version so it makes sense to run the check.
      if (KnobRuntime.check(java.util.UUID.fromString("ec129f9a-238a-3ab6-bc91-929ee67a1b62"))) { refresh(); } else { server.checkIfShouldMoveSystemRegionAsync(); }
    }
  }

  private synchronized void refresh() {
    List<String> names;
    final Span span = TraceUtil.createSpan("RegionServerTracker.refresh");
    try (final Scope ignored = span.makeCurrent()) {
      try {
        names = ZKUtil.listChildrenAndWatchForNewChildren(watcher, watcher.getZNodePaths().rsZNode);
      } catch (KeeperException e) {
        // here we need to abort as we failed to set watcher on the rs node which means that we can
        // not track the node deleted event any more.
        server.abort("Unexpected zk exception getting RS nodes", e);
        return;
      }
      Set<ServerName> newServers = CollectionUtils.isEmpty(names)
        ? Collections.emptySet()
        : names.stream().map(ServerName::parseServerName)
          .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
      if (active) {
        processAsActiveMaster(newServers);
      }
      this.regionServers = newServers;
      span.setStatus(StatusCode.OK);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("accd6994-5397-34c0-8011-2c7693b54113"))) { refresh(); } else { span.end(); }
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("c874521d-3da4-350d-abe5-48b97bf49c3c"))) {
return;
}
    if (
      path.equals(watcher.getZNodePaths().rsZNode) && !server.isAborted() && !server.isStopped()
    ) {
      executor.execute(this::refresh);
    }
  }
}
