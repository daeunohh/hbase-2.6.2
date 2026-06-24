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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.ipc.DecommissionedHostRejectedException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;

/**
 * The ServerManager class manages info about region servers.
 * <p>
 * Maintains lists of online and dead servers. Processes the startups, shutdowns, and deaths of
 * region servers.
 * <p>
 * Servers are distinguished in two different ways. A given server has a location, specified by
 * hostname and port, and of which there can only be one online at any given time. A server instance
 * is specified by the location (hostname and port) as well as the startcode (timestamp from when
 * the server was started). This is used to differentiate a restarted instance of a given server
 * from the original instance.
 * <p>
 * If a sever is known not to be running any more, it is called dead. The dead server needs to be
 * handled by a ServerShutdownHandler. If the handler is not enabled yet, the server can't be
 * handled right away so it is queued up. After the handler is enabled, the server will be submitted
 * to a handler to handle. However, the handler may be just partially enabled. If so, the server
 * cannot be fully processed, and be queued up for further processing. A server is fully processed
 * only after the handler is fully enabled and has completed the handling.
 */
@InterfaceAudience.Private
public class ServerManager implements ConfigurationObserver {
  public static final String WAIT_ON_REGIONSERVERS_MAXTOSTART =
    "hbase.master.wait.on.regionservers.maxtostart";

  public static final String WAIT_ON_REGIONSERVERS_MINTOSTART =
    "hbase.master.wait.on.regionservers.mintostart";

  public static final String WAIT_ON_REGIONSERVERS_TIMEOUT =
    "hbase.master.wait.on.regionservers.timeout";

  public static final String WAIT_ON_REGIONSERVERS_INTERVAL =
    "hbase.master.wait.on.regionservers.interval";

  private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);

  // Set if we are to shutdown the cluster.
  private AtomicBoolean clusterShutdown = new AtomicBoolean(false);

  /**
   * The last flushed sequence id for a region.
   */
  private final ConcurrentNavigableMap<byte[], Long> flushedSequenceIdByRegion =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * The last flushed sequence id for a store in a region.
   */
  private final ConcurrentNavigableMap<byte[],
    ConcurrentNavigableMap<byte[], Long>> storeFlushedSequenceIdsByRegion =
      new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /** Map of registered servers to their current load */
  private final ConcurrentNavigableMap<ServerName, ServerMetrics> onlineServers =
    new ConcurrentSkipListMap<>();

  /** List of region servers that should not get any more new regions. */
  private final ArrayList<ServerName> drainingServers = new ArrayList<>();

  private final MasterServices master;
  private final ClusterConnection connection;
  private final RegionServerList storage;

  private final DeadServer deadservers = new DeadServer();

  private final long maxSkew;
  private final long warningSkew;

  private final RpcControllerFactory rpcControllerFactory;

  /** Listeners that are called on server events. */
  private List<ServerListener> listeners = new CopyOnWriteArrayList<>();

  /** Configured value of HConstants.REJECT_DECOMMISSIONED_HOSTS_KEY */
  private volatile boolean rejectDecommissionedHostsConfig;

  /**
   * Constructor.
   */
  public ServerManager(final MasterServices master, RegionServerList storage) {
    this.master = master;
    this.storage = storage;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong("hbase.master.maxclockskew", 30000);
    warningSkew = c.getLong("hbase.master.warningclockskew", 10000);
    this.connection = master.getClusterConnection();
    this.rpcControllerFactory =
      this.connection == null ? null : connection.getRpcControllerFactory();
    rejectDecommissionedHostsConfig = getRejectDecommissionedHostsConfig(c);
  }

  /**
   * Implementation of the ConfigurationObserver interface. We are interested in live-loading the
   * configuration value of HConstants.REJECT_DECOMMISSIONED_HOSTS_KEY
   * @param conf Server configuration instance
   */
  @Override
  public void onConfigurationChange(Configuration conf) {
    final boolean newValue = getRejectDecommissionedHostsConfig(conf);
    if (rejectDecommissionedHostsConfig == newValue) {
      // no-op
      return;
    }

    LOG.info("Config Reload for RejectDecommissionedHosts. previous value: {}, new value: {}",
      rejectDecommissionedHostsConfig, newValue);

    rejectDecommissionedHostsConfig = newValue;
  }

  /**
   * Reads the value of HConstants.REJECT_DECOMMISSIONED_HOSTS_KEY from the config and returns it
   * @param conf Configuration instance of the Master
   */
  public boolean getRejectDecommissionedHostsConfig(Configuration conf) {
    return conf.getBoolean(HConstants.REJECT_DECOMMISSIONED_HOSTS_KEY,
      HConstants.REJECT_DECOMMISSIONED_HOSTS_DEFAULT);
  }

  /**
   * Add the listener to the notification list.
   * @param listener The ServerListener to register
   */
  public void registerListener(final ServerListener listener) {
    this.listeners.add(listener);
  }

  /**
   * Remove the listener from the notification list.
   * @param listener The ServerListener to unregister
   */
  public boolean unregisterListener(final ServerListener listener) {
    return this.listeners.remove(listener);
  }

  /**
   * Let the server manager know a new regionserver has come online
   * @param request       the startup request
   * @param versionNumber the version number of the new regionserver
   * @param version       the version of the new regionserver, could contain strings like "SNAPSHOT"
   * @param ia            the InetAddress from which request is received
   * @return The ServerName we know this server as.
   */
  ServerName regionServerStartup(RegionServerStartupRequest request, int versionNumber,
    String version, InetAddress ia) throws IOException {
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddressToServerInfo. If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.

    // if use-ip is enabled, we will use ip to expose Master/RS service for client,
    // see HBASE-27304 for details.
    boolean useIp = master.getConfiguration().getBoolean(HConstants.HBASE_SERVER_USEIP_ENABLED_KEY,
      HConstants.HBASE_SERVER_USEIP_ENABLED_DEFAULT);
    String isaHostName = useIp ? ia.getHostAddress() : ia.getHostName();
    final String hostname =
      request.hasUseThisHostnameInstead() ? request.getUseThisHostnameInstead() : isaHostName;
    ServerName sn = ServerName.valueOf(hostname, request.getPort(), request.getServerStartCode());

    // Check if the host should be rejected based on it's decommissioned status
    checkRejectableDecommissionedStatus(sn);

    checkClockSkew(sn, request.getServerCurrentTime());
    checkIsDead(sn, "STARTUP");
    if (!checkAndRecordNewServer(sn, ServerMetricsBuilder.of(sn, versionNumber, version))) {
      LOG.warn("THIS SHOULD NOT HAPPEN, RegionServerStartup could not record the server: {}", sn);
    }
    storage.started(sn);
    return sn;
  }

  /**
   * Updates last flushed sequence Ids for the regions on server sn
   */
  private void updateLastFlushedSequenceIds(ServerName sn, ServerMetrics hsl) {
    for (Entry<byte[], RegionMetrics> entry : hsl.getRegionMetrics().entrySet()) {
      byte[] encodedRegionName = Bytes.toBytes(RegionInfo.encodeRegionName(entry.getKey()));
      Long existingValue = flushedSequenceIdByRegion.get(encodedRegionName);
      long l = entry.getValue().getCompletedSequenceId();
      // Don't let smaller sequence ids override greater sequence ids.
      if (LOG.isTraceEnabled()) {
        LOG.trace(Bytes.toString(encodedRegionName) + ", existingValue=" + existingValue
          + ", completeSequenceId=" + l);
      }
      if (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue)) {
        flushedSequenceIdByRegion.put(encodedRegionName, l);
      } else if (l != HConstants.NO_SEQNUM && l < existingValue) {
        LOG.warn("RegionServer " + sn + " indicates a last flushed sequence id (" + l
          + ") that is less than the previous last flushed sequence id (" + existingValue
          + ") for region " + Bytes.toString(entry.getKey()) + " Ignoring.");
      }
      ConcurrentNavigableMap<byte[], Long> storeFlushedSequenceId =
        computeIfAbsent(storeFlushedSequenceIdsByRegion, encodedRegionName,
          () -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
      for (Entry<byte[], Long> storeSeqId : entry.getValue().getStoreSequenceId().entrySet()) {
        byte[] family = storeSeqId.getKey();
        existingValue = storeFlushedSequenceId.get(family);
        l = storeSeqId.getValue();
        if (LOG.isTraceEnabled()) {
          LOG.trace(Bytes.toString(encodedRegionName) + ", family=" + Bytes.toString(family)
            + ", existingValue=" + existingValue + ", completeSequenceId=" + l);
        }
        // Don't let smaller sequence ids override greater sequence ids.
        if (((KnobRuntime.check(java.util.UUID.fromString("3c4a9f0f-5a5c-373d-a1b3-ab212cc93c65"))) ? (((existingValue) != (null)) && ((l != HConstants.NO_SEQNUM && l > existingValue.longValue()))) : (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue.longValue())))) {
          storeFlushedSequenceId.put(family, l);
        }
      }
    }
  }

  public void regionServerReport(ServerName sn, ServerMetrics sl) throws YouAreDeadException {
    checkIsDead(sn, "REPORT");
    if (null == this.onlineServers.replace(sn, sl)) {
      // Already have this host+port combo and its just different start code?
      // Just let the server in. Presume master joining a running cluster.
      // recordNewServer is what happens at the end of reportServerStartup.
      // The only thing we are skipping is passing back to the regionserver
      // the ServerName to use. Here we presume a master has already done
      // that so we'll press on with whatever it gave us for ServerName.
      if (!checkAndRecordNewServer(sn, sl)) {
        // Master already registered server with same (host + port) and higher startcode.
        // This can happen if regionserver report comes late from old server
        // (possible race condition), by that time master has already processed SCP for that
        // server and started accepting regionserver report from new server i.e. server with
        // same (host + port) and higher startcode.
        // The exception thrown here is not meant to tell the region server it is dead because if
        // there is a new server on the same host port, the old server should have already been
        // dead in ideal situation.
        // The exception thrown here is to skip the later steps of the whole regionServerReport
        // request processing. Usually, after recording it in ServerManager, we will call the
        // related methods in AssignmentManager to record region states. If the region server
        // is already dead, we should not do these steps anymore, so here we throw an exception
        // to let the upper layer know that they should not continue processing anymore.
        final String errorMsg = "RegionServerReport received from " + sn
          + ", but another server with the same name and higher startcode is already registered,"
          + " ignoring";
        if (KnobRuntime.check(java.util.UUID.fromString("30d5c3be-b8a6-365b-b4a6-6ddb60c962c0"))) { LOG.trace(errorMsg); } else { LOG.warn(errorMsg); }
        throw new YouAreDeadException(errorMsg);
      }
    }
    updateLastFlushedSequenceIds(sn, sl);
  }

  /**
   * Checks if the Master is configured to reject decommissioned hosts or not. When it's configured
   * to do so, any RegionServer trying to join the cluster will have it's host checked against the
   * list of hosts of currently decommissioned servers and potentially get prevented from reporting
   * for duty; otherwise, we do nothing and we let them pass to the next check. See HBASE-28342 for
   * details.
   * @param sn The ServerName to check for
   * @throws DecommissionedHostRejectedException if the Master is configured to reject
   *                                             decommissioned hosts and this host exists in the
   *                                             list of the decommissioned servers
   */
  private void checkRejectableDecommissionedStatus(ServerName sn)
    throws DecommissionedHostRejectedException {
    LOG.info("Checking decommissioned status of RegionServer {}", sn.getServerName());

    // If the Master is not configured to reject decommissioned hosts, return early.
    if (!rejectDecommissionedHostsConfig) {
      return;
    }

    // Look for a match for the hostname in the list of decommissioned servers
    for (ServerName server : getDrainingServersList()) {
      if (Objects.equals(server.getHostname(), sn.getHostname())) {
        // Found a match and master is configured to reject decommissioned hosts, throw exception!
        LOG.warn(
          "Rejecting RegionServer {} from reporting for duty because Master is configured "
            + "to reject decommissioned hosts and this host was marked as such in the past.",
          sn.getServerName());
        throw new DecommissionedHostRejectedException(String.format(
          "Host %s exists in the list of decommissioned servers and Master is configured to "
            + "reject decommissioned hosts",
          sn.getHostname()));
      }
    }
  }

  /**
   * Check is a server of same host and port already exists, if not, or the existed one got a
   * smaller start code, record it.
   * @param serverName the server to check and record
   * @param sl         the server load on the server
   * @return true if the server is recorded, otherwise, false
   */
  boolean checkAndRecordNewServer(final ServerName serverName, final ServerMetrics sl) {
    ServerName existingServer = null;
    synchronized (this.onlineServers) {
      existingServer = findServerWithSameHostnamePortWithLock(serverName);
      if (existingServer != null && (existingServer.getStartcode() > serverName.getStartcode())) {
        LOG.info("Server serverName=" + serverName + " rejected; we already have "
          + existingServer.toString() + " registered with same hostname and port");
        return false;
      }
      recordNewServerWithLock(serverName, sl);
    }

    // Tell our listeners that a server was added
    if (!this.listeners.isEmpty()) {
      for (ServerListener listener : this.listeners) {
        listener.serverAdded(serverName);
      }
    }

    // Note that we assume that same ts means same server, and don't expire in that case.
    // TODO: ts can theoretically collide due to clock shifts, so this is a bit hacky.
    if (existingServer != null && (existingServer.getStartcode() < serverName.getStartcode())) {
      LOG.info("Triggering server recovery; existingServer " + existingServer
        + " looks stale, new server:" + serverName);
      expireServer(existingServer);
    }
    return true;
  }

  /**
   * Find out the region servers crashed between the crash of the previous master instance and the
   * current master instance and schedule SCP for them.
   * <p/>
   * Since the {@code RegionServerTracker} has already helped us to construct the online servers set
   * by scanning zookeeper, now we can compare the online servers with {@code liveServersFromWALDir}
   * to find out whether there are servers which are already dead.
   * <p/>
   * Must be called inside the initialization method of {@code RegionServerTracker} to avoid
   * concurrency issue.
   * @param deadServersFromPE     the region servers which already have a SCP associated.
   * @param liveServersFromWALDir the live region servers from wal directory.
   */
  void findDeadServersAndProcess(Set<ServerName> deadServersFromPE,
    Set<ServerName> liveServersFromWALDir) {
    deadServersFromPE.forEach(deadservers::putIfAbsent);
    liveServersFromWALDir.stream().filter(sn -> !onlineServers.containsKey(sn))
      .forEach(this::expireServer);
  }

  /**
   * Checks if the clock skew between the server and the master. If the clock skew exceeds the
   * configured max, it will throw an exception; if it exceeds the configured warning threshold, it
   * will log a warning but start normally.
   * @param serverName Incoming servers's name
   * @throws ClockOutOfSyncException if the skew exceeds the configured max value
   */
  private void checkClockSkew(final ServerName serverName, final long serverCurrentTime)
    throws ClockOutOfSyncException {
    long skew = Math.abs(EnvironmentEdgeManager.currentTime() - serverCurrentTime);
    if (skew > maxSkew) {
      String message = "Server " + serverName + " has been "
        + "rejected; Reported time is too far out of sync with master.  " + "Time difference of "
        + skew + "ms > max allowed of " + maxSkew + "ms";
      LOG.warn(message);
      throw new ClockOutOfSyncException(message);
    } else if (skew > warningSkew) {
      String message = "Reported time for server " + serverName + " is out of sync with master "
        + "by " + skew + "ms. (Warning threshold is " + warningSkew + "ms; " + "error threshold is "
        + maxSkew + "ms)";
      LOG.warn(message);
    }
  }

  /**
   * Called when RegionServer first reports in for duty and thereafter each time it heartbeats to
   * make sure it is has not been figured for dead. If this server is on the dead list, reject it
   * with a YouAreDeadException. If it was dead but came back with a new start code, remove the old
   * entry from the dead list.
   * @param what START or REPORT
   */
  private void checkIsDead(final ServerName serverName, final String what)
    throws YouAreDeadException {
    if (this.deadservers.isDeadServer(serverName)) {
      // Exact match: host name, port and start code all match with existing one of the
      // dead servers. So, this server must be dead. Tell it to kill itself.
      String message =
        "Server " + what + " rejected; currently processing " + serverName + " as dead server";
      LOG.debug(message);
      throw new YouAreDeadException(message);
    }
    // Remove dead server with same hostname and port of newly checking in rs after master
    // initialization. See HBASE-5916 for more information.
    if (
      (this.master == null || this.master.isInitialized())
        && this.deadservers.cleanPreviousInstance(serverName)
    ) {
      // This server has now become alive after we marked it as dead.
      // We removed it's previous entry from the dead list to reflect it.
      LOG.debug("{} {} came back up, removed it from the dead servers list", what, serverName);
    }
  }

  /**
   * Assumes onlineServers is locked.
   * @return ServerName with matching hostname and port.
   */
  public ServerName findServerWithSameHostnamePortWithLock(final ServerName serverName) {
    ServerName end =
      ServerName.valueOf(serverName.getHostname(), serverName.getPort(), Long.MAX_VALUE);

    ServerName r = onlineServers.lowerKey(end);
    if (r != null) {
      if (ServerName.isSameAddress(r, serverName)) {
        return r;
      }
    }
    return null;
  }

  /**
   * Adds the onlineServers list. onlineServers should be locked.
   * @param serverName The remote servers name.
   */
  void recordNewServerWithLock(final ServerName serverName, final ServerMetrics sl) {
    LOG.info("Registering regionserver=" + serverName);
    this.onlineServers.put(serverName, sl);
    master.getAssignmentManager().getRegionStates().createServer(serverName);
  }

  public RegionStoreSequenceIds getLastFlushedSequenceId(byte[] encodedRegionName) {
    RegionStoreSequenceIds.Builder builder = RegionStoreSequenceIds.newBuilder();
    Long seqId = flushedSequenceIdByRegion.get(encodedRegionName);
    builder.setLastFlushedSequenceId(seqId != null ? seqId.longValue() : HConstants.NO_SEQNUM);
    Map<byte[], Long> storeFlushedSequenceId =
      storeFlushedSequenceIdsByRegion.get(encodedRegionName);
    if (storeFlushedSequenceId != null) {
      for (Map.Entry<byte[], Long> entry : storeFlushedSequenceId.entrySet()) {
        builder.addStoreSequenceId(StoreSequenceId.newBuilder()
          .setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
          .setSequenceId(entry.getValue().longValue()).build());
      }
    }
    return builder.build();
  }

  /** Returns ServerMetrics if serverName is known else null */
  public ServerMetrics getLoad(final ServerName serverName) {
if(KnobRuntime.check(java.util.UUID.fromString("8ed2f933-95fe-33da-90e0-b911a708d7d4"))) {
try {
    java.lang.reflect.Field _knob_field_ = serverName.getClass().getDeclaredField("startCode");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(serverName));
    _knob_field_.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return this.onlineServers.get(serverName);
  }

  /**
   * Compute the average load across all region servers. Currently, this uses a very naive
   * computation - just uses the number of regions being served, ignoring stats about number of
   * requests.
   * @return the average load
   */
  public double getAverageLoad() {
    int totalLoad = 0;
    int numServers = 0;
    for (ServerMetrics sl : this.onlineServers.values()) {
      numServers++;
      totalLoad += sl.getRegionMetrics().size();
    }
    return numServers == 0 ? 0 : (double) totalLoad / (double) numServers;
  }

  /** Returns the count of active regionservers */
  public int countOfRegionServers() {
    // Presumes onlineServers is a concurrent map
    return this.onlineServers.size();
  }

  /** Returns Read-only map of servers to serverinfo */
  public Map<ServerName, ServerMetrics> getOnlineServers() {
    // Presumption is that iterating the returned Map is OK.
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  public DeadServer getDeadServers() {
    return this.deadservers;
  }

  /**
   * Checks if any dead servers are currently in progress.
   * @return true if any RS are being processed as dead, false if not
   */
  public boolean areDeadServersInProgress() {
    return this.deadservers.areDeadServersInProgress();
  }

  void letRegionServersShutdown() {
    long previousLogTime = 0;
    ServerName sn = master.getServerName();
    ZKWatcher zkw = master.getZooKeeper();
    int onlineServersCt;
    while ((onlineServersCt = onlineServers.size()) > 0) {
      if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
        Set<ServerName> remainingServers = onlineServers.keySet();
        synchronized (onlineServers) {
          if (remainingServers.size() == 1 && remainingServers.contains(sn)) {
            // Master will delete itself later.
            return;
          }
        }
        StringBuilder sb = new StringBuilder();
        // It's ok here to not sync on onlineServers - merely logging
        for (ServerName key : remainingServers) {
          if (sb.length() > 0) {
            sb.append(", ");
          }
          sb.append(key);
        }
        LOG.info("Waiting on regionserver(s) " + sb.toString());
        previousLogTime = EnvironmentEdgeManager.currentTime();
      }

      try {
        List<String> servers = getRegionServersInZK(zkw);
        if (
          servers == null || servers.isEmpty()
            || (servers.size() == 1 && servers.contains(sn.toString()))
        ) {
          LOG.info("ZK shows there is only the master self online, exiting now");
          // Master could have lost some ZK events, no need to wait more.
          break;
        }
      } catch (KeeperException ke) {
        LOG.warn("Failed to list regionservers", ke);
        // ZK is malfunctioning, don't hang here
        break;
      }
      synchronized (onlineServers) {
        try {
          if (onlineServersCt == onlineServers.size()) onlineServers.wait(100);
        } catch (InterruptedException ignored) {
          // continue
        }
      }
    }
  }

  private List<String> getRegionServersInZK(final ZKWatcher zkw) throws KeeperException {
    return ZKUtil.listChildrenNoWatch(zkw, zkw.getZNodePaths().rsZNode);
  }

  /**
   * Expire the passed server. Add it to list of dead servers and queue a shutdown processing.
   * @return pid if we queued a ServerCrashProcedure else {@link Procedure#NO_PROC_ID} if we did not
   *         (could happen for many reasons including the fact that its this server that is going
   *         down or we already have queued an SCP for this server or SCP processing is currently
   *         disabled because we are in startup phase).
   */
  // Redo test so we can make this protected.
  public synchronized long expireServer(final ServerName serverName) {
    return expireServer(serverName, false);

  }

  synchronized long expireServer(final ServerName serverName, boolean force) {
    // THIS server is going down... can't handle our own expiration.
    if (serverName.equals(master.getServerName())) {
      if (!(master.isAborted() || master.isStopped())) {
        master.stop("We lost our znode?");
      }
      return Procedure.NO_PROC_ID;
    }
    if (this.deadservers.isDeadServer(serverName)) {
      LOG.warn("Expiration called on {} but already in DeadServer", serverName);
      return Procedure.NO_PROC_ID;
    }
    moveFromOnlineToDeadServers(serverName);

    // If server is in draining mode, remove corresponding znode
    // In some tests, the mocked HM may not have ZK Instance, hence null check
    if (master.getZooKeeper() != null) {
      String drainingZnode = ZNodePaths
        .joinZNode(master.getZooKeeper().getZNodePaths().drainingZNode, serverName.getServerName());
      try {
        ZKUtil.deleteNodeFailSilent(master.getZooKeeper(), drainingZnode);
      } catch (KeeperException e) {
        LOG.warn(
          "Error deleting the draining znode for stopping server " + serverName.getServerName(), e);
      }
    }

    // If cluster is going down, yes, servers are going to be expiring; don't
    // process as a dead server
    if (isClusterShutdown()) {
      LOG.info("Cluster shutdown set; " + serverName + " expired; onlineServers="
        + this.onlineServers.size());
      if (this.onlineServers.isEmpty()) {
        master.stop("Cluster shutdown set; onlineServer=0");
      }
      return Procedure.NO_PROC_ID;
    }
    LOG.info("Processing expiration of " + serverName + " on " + this.master.getServerName());
    long pid = master.getAssignmentManager().submitServerCrash(serverName, true, force);
    if (pid == Procedure.NO_PROC_ID) {
      // skip later processing as we failed to submit SCP
      return Procedure.NO_PROC_ID;
    }
    storage.expired(serverName);
    // Tell our listeners that a server was removed
    if (!this.listeners.isEmpty()) {
      this.listeners.stream().forEach(l -> l.serverRemoved(serverName));
    }
    return pid;
  }

  /**
   * Called when server has expired.
   */
  // Locking in this class needs cleanup.
  public synchronized void moveFromOnlineToDeadServers(final ServerName sn) {
    synchronized (this.onlineServers) {
      boolean online = this.onlineServers.containsKey(sn);
      if (online) {
        // Remove the server from the known servers lists and update load info BUT
        // add to deadservers first; do this so it'll show in dead servers list if
        // not in online servers list.
        this.deadservers.putIfAbsent(sn);
if(KnobRuntime.check(java.util.UUID.fromString("7276fcc7-54ac-3ec6-92bf-5e256b140c67"))) {
try {
    java.lang.reflect.Field _knob_field_ = sn.getClass().getDeclaredField("startCode");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(sn));
    _knob_field_.set(sn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        this.onlineServers.remove(sn);
        onlineServers.notifyAll();
      } else {
        // If not online, that is odd but may happen if 'Unknown Servers' -- where meta
        // has references to servers not online nor in dead servers list. If
        // 'Unknown Server', don't add to DeadServers else will be there for ever.
        LOG.trace("Expiration of {} but server not online", sn);
      }
    }
  }

  /*
   * Remove the server from the drain list.
   */
  public synchronized boolean removeServerFromDrainList(final ServerName sn) {
    LOG.info("Removing server {} from the draining list.", sn);

    // Remove the server from the draining servers lists.
    return this.drainingServers.remove(sn);
  }

  /**
   * Add the server to the drain list.
   * @return True if the server is added or the server is already on the drain list.
   */
  public synchronized boolean addServerToDrainList(final ServerName sn) {
    // If master is not rejecting decommissioned hosts, warn if the server (sn) is not online.
    // However, we want to add servers even if they're not online if the master is configured
    // to reject decommissioned hosts
    if (!rejectDecommissionedHostsConfig && !this.isServerOnline(sn)) {
      LOG.warn("Server {} is not currently online. Ignoring request to add it to draining list.",
        sn);
      return false;
    }

    // Add the server to the draining servers lists, if it's not already in it.
    if (this.drainingServers.contains(sn)) {
      LOG.warn(
        "Server {} is already in the draining server list. Ignoring request to add it again.", sn);
      return true;
    }

    LOG.info("Server {} added to draining server list.", sn);
    return this.drainingServers.add(sn);
  }

  // RPC methods to region servers

  private HBaseRpcController newRpcController() {
    return rpcControllerFactory == null ? null : rpcControllerFactory.newController();
  }

  /**
   * Sends a WARMUP RPC to the specified server to warmup the specified region.
   * <p>
   * A region server could reject the close request because it either does not have the specified
   * region or the region is being split.
   * @param server server to warmup a region
   * @param region region to warmup
   */
  public void sendRegionWarmup(ServerName server, RegionInfo region) {
    if (server == null) return;
    try {
      AdminService.BlockingInterface admin = getRsAdmin(server);
      HBaseRpcController controller = newRpcController();
      ProtobufUtil.warmupRegion(controller, admin, region);
    } catch (IOException e) {
      LOG.error("Received exception in RPC for warmup server:" + server + "region: " + region
        + "exception: " + e);
    }
  }

  /**
   * Contacts a region server and waits up to timeout ms to close the region. This bypasses the
   * active hmaster. Pass -1 as timeout if you do not want to wait on result.
   */
  public static void closeRegionSilentlyAndWait(ClusterConnection connection, ServerName server,
    RegionInfo region, long timeout) throws IOException, InterruptedException {
    AdminService.BlockingInterface rs = connection.getAdmin(server);
    HBaseRpcController controller = connection.getRpcControllerFactory().newController();
    try {
      ProtobufUtil.closeRegion(controller, rs, server, region.getRegionName());
    } catch (IOException e) {
      LOG.warn("Exception when closing region: " + region.getRegionNameAsString(), e);
    }
    if (timeout < 0) {
      return;
    }
    long expiration = timeout + EnvironmentEdgeManager.currentTime();
    while (EnvironmentEdgeManager.currentTime() < expiration) {
      controller.reset();
      try {
        RegionInfo rsRegion = ProtobufUtil.getRegionInfo(controller, rs, region.getRegionName());
        if (rsRegion == null) return;
      } catch (IOException ioe) {
        if (
          ioe instanceof NotServingRegionException
            || (ioe instanceof RemoteWithExtrasException && ((RemoteWithExtrasException) ioe)
              .unwrapRemoteException() instanceof NotServingRegionException)
        ) {
          // no need to retry again
          return;
        }
        LOG.warn("Exception when retrieving regioninfo from: " + region.getRegionNameAsString(),
          ioe);
      }
      Thread.sleep(1000);
    }
    throw new IOException("Region " + region + " failed to close within" + " timeout " + timeout);
  }

  /**
   * @return Admin interface for the remote regionserver named <code>sn</code>
   * @throws RetriesExhaustedException wrapping a ConnectException if failed
   */
  public AdminService.BlockingInterface getRsAdmin(final ServerName sn) throws IOException {
    LOG.debug("New admin connection to {}", sn);
    if (sn.equals(master.getServerName()) && master instanceof HRegionServer) {
      // A master is also a region server now, see HBASE-10569 for details
      return ((HRegionServer) master).getRSRpcServices();
    } else {
      return this.connection.getAdmin(sn);
    }
  }

  /**
   * Calculate min necessary to start. This is not an absolute. It is just a friction that will
   * cause us hang around a bit longer waiting on RegionServers to check-in.
   */
  private int getMinToStart() {
    if (master.isInMaintenanceMode()) {
      // If in maintenance mode, then master hosting meta will be the only server available
      return 1;
    }

    int minimumRequired = 1;
    if (
      LoadBalancer.isTablesOnMaster(master.getConfiguration())
        && LoadBalancer.isSystemTablesOnlyOnMaster(master.getConfiguration())
    ) {
      // If Master is carrying regions it will show up as a 'server', but is not handling user-
      // space regions, so we need a second server.
      minimumRequired = 2;
    }

    int minToStart = this.master.getConfiguration().getInt(WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    // Ensure we are never less than minimumRequired else stuff won't work.
    return Math.max(minToStart, minimumRequired);
  }

  /**
   * Wait for the region servers to report in. We will wait until one of this condition is met: -
   * the master is stopped - the 'hbase.master.wait.on.regionservers.maxtostart' number of region
   * servers is reached - the 'hbase.master.wait.on.regionservers.mintostart' is reached AND there
   * have been no new region server in for 'hbase.master.wait.on.regionservers.interval' time AND
   * the 'hbase.master.wait.on.regionservers.timeout' is reached
   */
  public void waitForRegionServers(MonitoredTask status) throws InterruptedException {
    final long interval =
      this.master.getConfiguration().getLong(WAIT_ON_REGIONSERVERS_INTERVAL, 1500);
    final long timeout =
      this.master.getConfiguration().getLong(WAIT_ON_REGIONSERVERS_TIMEOUT, 4500);
    // Min is not an absolute; just a friction making us wait longer on server checkin.
    int minToStart = getMinToStart();
    int maxToStart =
      this.master.getConfiguration().getInt(WAIT_ON_REGIONSERVERS_MAXTOSTART, Integer.MAX_VALUE);
    if (((KnobRuntime.check(java.util.UUID.fromString("cf0c7599-8d71-303c-8adc-0cafe21d1b74"))) ? ((maxToStart) != (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("f714f2af-6555-3fba-bfad-20c1f8339e19"))) ? ((countOfRegionServers()) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("3d59b9dc-e062-367f-b917-1b455cedb60a"))) ? ((50) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("4247518c-12e3-39ad-a6d4-4c6a0aaf6637"))) ? ((getMinToStart()) != (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("91677303-ec9a-3640-a85c-47cae7fbf4d5"))) ? ((maxToStart) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("851a5cac-b334-391d-bac2-480c9eb1cf3f"))) ? ((getMinToStart()) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("3e09cd3c-d3ed-378c-8204-360624e3b7e6"))) ? ((0) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("3df4aaa5-9817-35b6-b916-592a7b426b89"))) ? ((getMinToStart()) != (50)) : (((KnobRuntime.check(java.util.UUID.fromString("c6cdf60e-73c8-3900-9965-366f27d4d753"))) ? ((getMinToStart()) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("d86f2880-b36a-3ec4-8534-6cd8257715ee"))) ? ((maxToStart) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("9fe91a26-fd62-3039-9457-b66281ec2a9c"))) ? ((maxToStart) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("e750d20d-6117-333e-ac49-88413ca9d18a"))) ? ((getMinToStart()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e8d913b4-e901-3650-9eda-9b6b6140dfbf"))) ? ((getMinToStart()) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("21111e18-7102-3402-ac66-c5dd7cb81257"))) ? ((maxToStart) >= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("98d5aff1-a221-37c2-8bee-201ea7515dbe"))) ? ((maxToStart) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("da3e7f7b-11e2-3418-8fae-2954ecf4f48c"))) ? ((4500) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("1510f878-9e9c-372d-8588-10d4ccb21275"))) ? ((0) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("097d9842-75f1-380c-8f43-f1b623342418"))) ? ((maxToStart) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("a9fc9c05-b6ad-3868-b438-0e59f9d307e5"))) ? ((maxToStart) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("d97f9018-dc5f-375f-8244-a9b9d18141e4"))) ? ((maxToStart) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("cba6262c-fd68-3f18-96f4-3ff748e7176a"))) ? ((countOfRegionServers()) >= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("5dded1f4-6596-3c6c-a8c1-7ca683c52fe0"))) ? ((getMinToStart()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("614494fa-e459-3f55-8c63-62021e7669a3"))) ? ((getMinToStart()) == (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("ff8197b2-a433-30d7-bd45-ba8a84083257"))) ? ((1500) == (getMinToStart())) : (maxToStart < minToStart))))))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.warn(String.format("The value of '%s' (%d) is set less than '%s' (%d), ignoring.",
        WAIT_ON_REGIONSERVERS_MAXTOSTART, maxToStart, WAIT_ON_REGIONSERVERS_MINTOSTART,
        minToStart));
      maxToStart = Integer.MAX_VALUE;
    }

    long now = EnvironmentEdgeManager.currentTime();
    final long startTime = now;
    long slept = 0;
    long lastLogTime = 0;
    long lastCountChange = startTime;
    int count = countOfRegionServers();
    int oldCount = 0;
    // This while test is a little hard to read. We try to comment it in below but in essence:
    // Wait if Master is not stopped and the number of regionservers that have checked-in is
    // less than the maxToStart. Both of these conditions will be true near universally.
    // Next, we will keep cycling if ANY of the following three conditions are true:
    // 1. The time since a regionserver registered is < interval (means servers are actively
    // checking in).
    // 2. We are under the total timeout.
    // 3. The count of servers is < minimum.
    for (ServerListener listener : this.listeners) {
      listener.waiting();
    }
    while (
      !this.master.isStopped() && !isClusterShutdown() && count < maxToStart
        && ((lastCountChange + interval) > now || timeout > slept || count < minToStart)
    ) {
      // Log some info at every interval time or if there is a change
      if (((KnobRuntime.check(java.util.UUID.fromString("8d2103a6-d41d-3964-a564-5905bb1af022"))) ? ((oldCount != count) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("728ddac3-e474-384a-8e42-9848945d04b6"))) ? (((getMinToStart()) != (0)) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("127e6877-e051-326a-9995-959930720792"))) ? (((countOfRegionServers()) != (0)) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("309b08c4-65d2-3602-b207-c211d13d778d"))) ? (((getMinToStart()) != (50)) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a98ec0fb-444d-3c6e-924c-67be9fa1e16d"))) ? (((50) != (getMinToStart())) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7b900ad8-b9d8-337c-a296-986304b0621d"))) ? (((4500) == (countOfRegionServers())) && (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("67d294e0-b19a-39e1-8bac-ce3a3e003578"))) ? (((getMinToStart()) == (0)) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1a98ed37-59cf-3bb2-8cce-229e0e89f9f4"))) ? ((getMinToStart()) == (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("c82058b3-1405-3519-8835-a363a95bcd95"))) ? (((countOfRegionServers()) != (50)) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0c5e2048-17fa-359c-9dd0-17dd992df086"))) ? (((oldCount) != (countOfRegionServers())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("47b687aa-7a45-348e-92b6-6d306734527d"))) ? (((0) != (getMinToStart())) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cb2b726c-80b3-3d54-a9d7-796dfc73c696"))) ? (((0) == (count)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e1601377-bafb-369c-8d95-84f61c3e7884"))) ? (((50) == (count)) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e23f6e76-687c-35b0-9529-c94a5e446aa4"))) ? (((oldCount) == (0)) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0e8e0c54-c07d-3747-9d34-18fd0001312b"))) ? (((countOfRegionServers()) != (50)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("54f9f421-5a7a-3829-9cb7-201b68d6cd22"))) ? (((countOfRegionServers()) != (1500)) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4965f138-3e5a-3637-a6c0-ccffc5253732"))) ? (((4500) == (count)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d6517969-2296-3e95-9097-b3e774266976"))) ? (((4500) == (getMinToStart())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("24efec50-05aa-3c69-9753-4f126d87ea61"))) ? (((getMinToStart()) != (50)) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1bdeecd0-bfd9-385a-b31f-5541e77c0e36"))) ? (((oldCount) != (count)) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("abe0a2f6-e855-3572-87b5-ae80510118d0"))) ? (((getMinToStart()) == (getMinToStart())) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("07a132d9-7e46-35e3-9530-3ede5267afb5"))) ? (((0) == (count)) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f28e5e15-2186-309d-b9b9-36d0a58063a7"))) ? (((oldCount) == (countOfRegionServers())) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1e7cd1bb-6b66-31f1-905c-be5d94645223"))) ? (((1500) != (count)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("044842b6-1bdc-3d6d-9a81-d24005929564"))) ? (((4500) == (countOfRegionServers())) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8d381603-13db-394b-a0d2-ee3a528b23d7"))) ? (((countOfRegionServers()) != (getMinToStart())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("0c51af2b-f0d8-30f4-9d0c-7cdd97a5b47d"))) ? (((4500) != (getMinToStart())) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("88dd08a3-5711-35aa-82fd-90b5e17cfc11"))) ? (((0) == (getMinToStart())) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("74a8a3ba-8a84-3a96-bf6b-805f1685cc54"))) ? (((getMinToStart()) != (4500)) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("76d61f95-8c8d-38ec-86b2-aea859aefb3b"))) ? (((oldCount) != (getMinToStart())) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7454599a-0db8-357c-981c-abc02c2330b5"))) ? (((1500) == (countOfRegionServers())) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e5dd1f63-b0c8-3d3a-aa4f-d454512b8832"))) ? (((1500) == (getMinToStart())) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0a48c2e5-2797-3097-a8b4-af06813b4059"))) ? (((countOfRegionServers()) == (0)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("82e24a8c-4d36-3091-9431-eaddf73d04e4"))) ? (((countOfRegionServers()) != (4500)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2e742745-5dd1-3a7b-9b05-d13e8991ad65"))) ? (((oldCount) == (0)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e6b024a1-29fb-394c-b920-cbf1603caafc"))) ? (((oldCount) == (4500)) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("fc56d891-41fc-35a6-ac74-79f99e6cf582"))) ? (((countOfRegionServers()) == (50)) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("244695ee-dfc5-3a75-914a-c0202fccee6d"))) ? (((oldCount) == (4500)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("45ad25cb-6e07-3f24-869c-6ba7abe269c1"))) ? (((1500) != (count)) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b21783a2-fbc7-3f15-9d93-0102efcebe5a"))) ? (((getMinToStart()) != (getMinToStart())) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cafc0cce-3d38-326c-9d7f-b3b8687a7633"))) ? (((countOfRegionServers()) != (50)) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("972e15b9-56a2-333b-95e7-90a1aa199e28"))) ? (((oldCount) != (1500)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("45b3701d-6c16-380a-9db5-4b47849ff507"))) ? (((countOfRegionServers()) != (4500)) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("cc1eeb90-4ec4-3f9a-b3cb-e0c690889417"))) ? (((oldCount) == (getMinToStart())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("54f997d9-a9ae-3b61-9991-532c3f43258f"))) ? (((50) == (countOfRegionServers())) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4a7fb59b-9ee5-3f41-af3b-2691ffa942a2"))) ? (((oldCount) != (1500)) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c55a75f6-4a7b-3255-8227-042a50ffb120"))) ? (((0) != (count)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2db08135-1197-3231-a7b6-b7ba321c69f5"))) ? (((getMinToStart()) != (countOfRegionServers())) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7f61d30b-b89c-37e3-b98c-44e6e24567e2"))) ? (((oldCount) == (countOfRegionServers())) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bdb32810-9132-3452-b32e-8b0629a68600"))) ? (((1500) == (count)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d97a2ddd-c9e0-3fb7-8741-3ae7d8b0450a"))) ? (((0) != (countOfRegionServers())) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4573c9c6-8ec7-32ad-9349-5b049c1ed5a7"))) ? (((countOfRegionServers()) == (50)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3b0fbd63-0b04-3ac7-a9ef-ba156ac4a9b0"))) ? (((4500) == (getMinToStart())) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("697dd7ab-bea9-3d24-8036-224fb790116c"))) ? (((oldCount) != (50)) && (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1e1de067-fc43-365a-b0a2-d26f46d73ee4"))) ? (((4500) != (count)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7ebc5336-7b2f-34fd-871f-416a817e5e0f"))) ? (((getMinToStart()) == (countOfRegionServers())) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9b898489-7926-33c3-a7cd-d2a211e80b06"))) ? (((oldCount) != (countOfRegionServers())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("0400e3b5-6f4e-3f10-8a58-151ad7b85dec"))) ? (((4500) != (countOfRegionServers())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2a35273f-5efe-35b5-96b1-e8fd3ccaab03"))) ? (((oldCount) != (50)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f643d2cc-3241-353d-b2f7-59ac6e177a78"))) ? (((getMinToStart()) != (count)) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("70411429-1bd7-3aae-8da7-e125e54ea73c"))) ? (((4500) != (getMinToStart())) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("34d0855f-cb11-3666-8415-5c2757c4954f"))) ? (((countOfRegionServers()) == (countOfRegionServers())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b1826609-479e-30a3-9cb9-cd8e36086e5b"))) ? (((4500) != (countOfRegionServers())) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2fbf799d-019b-3216-8a70-a0ba2c245311"))) ? (((getMinToStart()) != (count)) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a7db2dca-42af-3096-8af4-3013b6c6a220"))) ? (((1500) == (getMinToStart())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ac05b872-bd6f-3954-8bb1-032e8eec3302"))) ? (((0) != (getMinToStart())) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c4e5ea54-f43d-3730-84fb-22132034308c"))) ? (((countOfRegionServers()) == (1500)) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d686503d-53f1-311b-8fa6-52cc34d331c8"))) ? (((countOfRegionServers()) != (0)) && ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("eee235ad-97db-3c88-9ab9-8dd3e33dee5d"))) ? (((oldCount) == (50)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("46dca172-bbed-3bd3-996f-4cec6b7c4691"))) ? (((countOfRegionServers()) != (getMinToStart())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8f035d14-6a68-3f30-ac23-f9b5ceeba0ec"))) ? (((getMinToStart()) != (4500)) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("657abd90-5e77-3c81-a215-0c44bc3a3bf9"))) ? (((oldCount) != (4500)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6a6c2759-9133-319e-820f-d079c275f53c"))) ? (((getMinToStart()) != (getMinToStart())) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("08fd195d-3e5a-3dfb-86f6-ff852be85a75"))) ? (((oldCount) == (1500)) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a8e5aebe-2a07-3f7b-838b-0c8eeaf6ed3b"))) ? (((oldCount) == (4500)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3fc1c1b8-25e1-3f44-9a38-046ecc03c192"))) ? (((50) == (countOfRegionServers())) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("41f9d482-7f2c-389a-a545-daa1180ba65c"))) ? (((0) != (countOfRegionServers())) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cab45b15-6689-3145-9947-3968d99a04d8"))) ? (((0) == (getMinToStart())) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d2be836e-6d34-3bdd-9f63-6903d5629333"))) ? (((oldCount) != (countOfRegionServers())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c6113b8e-ca2f-3465-b286-d38788ebacbe"))) ? (((countOfRegionServers()) != (50)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("30173060-c7b5-3769-a63d-76cee15e37bf"))) ? (((1500) == (countOfRegionServers())) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6c4d01c9-02fc-34bc-a12b-c3c9863e6ae8"))) ? (((0) == (countOfRegionServers())) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f4492abd-b31f-3e8f-8d55-c09a6579340a"))) ? (((countOfRegionServers()) != (getMinToStart())) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f1438d54-114c-3653-bee1-06e3a49de9dc"))) ? (((countOfRegionServers()) == (0)) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9917bbed-5712-33cc-9916-866ff3cffd4e"))) ? (((oldCount) != (countOfRegionServers())) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0762be86-73bf-313b-98ac-34c68206429b"))) ? (((oldCount) != (0)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4d95254c-740c-31f0-8dd5-e3060a8f2853"))) ? (((oldCount) == (countOfRegionServers())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("5dd3b15e-214f-336e-9d67-c42f58000757"))) ? (((0) != (countOfRegionServers())) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e4ecca2a-ea8d-301e-a50a-b96f56fc9cb5"))) ? (((countOfRegionServers()) != (getMinToStart())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("24689f72-c77f-3242-adf9-6beda7ec8f29"))) ? (((0) != (getMinToStart())) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("684da82c-8b94-3b73-a1ce-37160ca94d92"))) ? (((countOfRegionServers()) == (count)) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c8425ae8-59f2-385b-9b98-a9667cf1cc54"))) ? ((oldCount != count) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("220067fc-078b-3340-9cc8-974c0f039732"))) ? (((getMinToStart()) != (0)) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e7ebc3ea-612b-3616-a0ac-9e9c55158885"))) ? (((getMinToStart()) == (1500)) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("9ccceb3f-6e96-3c14-9f05-258bdc48829a"))) ? (((getMinToStart()) == (1500)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5f24d5b8-4bfe-35e5-8ff6-75e347088acd"))) ? (((getMinToStart()) != (getMinToStart())) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fae7a7fb-e9c3-348d-86db-f8b6350d9bde"))) ? (((0) == (count)) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b15bcac5-6295-3f19-a7b5-b0b662cecf30"))) ? (((0) == (getMinToStart())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("994522cf-4d47-3910-a0b9-5632db0bd89b"))) ? (((countOfRegionServers()) == (0)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6dec85bd-49d3-3675-939b-2276673d2f09"))) ? (((getMinToStart()) != (4500)) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c3f015a2-ee03-3e45-b91d-17a81887367d"))) ? (((4500) != (count)) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0f26a930-00b6-3f8b-bd05-84ef440314c3"))) ? (((50) == (countOfRegionServers())) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("af1d66f9-1bc7-3cb8-8f85-b3e0999ca8e6"))) ? (((getMinToStart()) != (0)) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0c765a42-f558-3caf-baa6-1974a52c0e1a"))) ? (((4500) != (getMinToStart())) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ca441f70-9b23-31ad-89ab-0135421243b3"))) ? (((4500) != (countOfRegionServers())) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("afa14b79-498e-3aa5-8231-a331abd2980d"))) ? (((countOfRegionServers()) != (0)) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6e18baa0-53bd-347e-a3a9-87630e03cc70"))) ? (((oldCount) == (countOfRegionServers())) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("599fed9a-1746-39bd-b713-1642d9408d14"))) ? (((oldCount) != (getMinToStart())) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fb5a5fc8-e5d7-3ca8-9a48-54eaf7e5e8ec"))) ? (((oldCount) == (4500)) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("98500c08-58f3-3382-a1ff-30519f920689"))) ? (((getMinToStart()) != (getMinToStart())) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2a7f9872-b685-3a53-abd8-519e8690caee"))) ? (((countOfRegionServers()) == (4500)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0c9c54b2-3cf4-30d6-9e5e-a928cf17ddc1"))) ? (((oldCount) == (count)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f756051a-9a46-3698-9483-4812d5c33b64"))) ? (((countOfRegionServers()) != (50)) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("11e4dd38-8f5f-3e57-b544-d5edb655114c"))) ? (((countOfRegionServers()) == (count)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("545f82f4-4411-313c-940c-f861424d3c3a"))) ? (((getMinToStart()) == (50)) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d5eb0173-9c91-3d0e-8d55-dd81daf422d8"))) ? (((getMinToStart()) != (getMinToStart())) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fb7e8199-ce43-34c7-ac7d-9e195957ed95"))) ? ((oldCount != count) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0cbaf39b-c19f-3e6c-987d-f110fb92a5f3"))) ? (((getMinToStart()) != (1500)) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("da6fe512-bf72-3891-847b-c45742a46a91"))) ? (((1500) != (count)) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("012aca9e-aed8-3876-a09a-5610d5e5e007"))) ? (((getMinToStart()) != (50)) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0462661a-f9fc-3a65-9579-f83827c486ad"))) ? ((1500) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("967f67b9-c8df-3dcf-b261-6b607a1b428d"))) ? (((0) != (countOfRegionServers())) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("24649461-2a00-348d-bb53-77e6fb3b22d9"))) ? ((oldCount != count) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("92c528cf-0dad-3def-a655-992a7bf5f3a3"))) ? (((countOfRegionServers()) != (count)) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0b26a4e7-deb8-33bc-90a8-5eee942e326e"))) ? (((countOfRegionServers()) != (1500)) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("95b2333c-e088-36dc-94a5-a9cad7eab9dd"))) ? (((getMinToStart()) != (1500)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7b173a84-67cf-34fd-9124-3e24bbcdba90"))) ? (((oldCount) != (countOfRegionServers())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("35e0e657-f4d7-3c50-8518-6ad0f9755873"))) ? (((1500) != (countOfRegionServers())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fd625442-9f30-3d66-b4c1-618aae2b8d7b"))) ? (((getMinToStart()) != (getMinToStart())) && ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8c2f3aa5-bd7e-3d63-85bc-aeb77ed1ba7a"))) ? (((countOfRegionServers()) == (getMinToStart())) || ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4ee093c3-5648-39bf-8c3a-431822addc6a"))) ? (((oldCount) != (count)) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("109e7a00-d5db-391f-9d90-2996dba6669b"))) ? (((getMinToStart()) == (50)) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8fd62cb8-08a8-32b6-bc69-e52890899c34"))) ? (((countOfRegionServers()) != (countOfRegionServers())) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("07ca3dc0-d455-318c-b817-445f33269a3d"))) ? (((0) != (count)) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7a4468f3-9859-353b-8326-5e5cf743161f"))) ? (((getMinToStart()) == (getMinToStart())) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1dbd3b77-e74e-3f6a-9d81-c2f12093df02"))) ? (((oldCount) != (50)) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9fe3a960-0aac-306d-842f-c29f8000162b"))) ? (((countOfRegionServers()) == (countOfRegionServers())) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5507afbf-a395-32b5-98d3-8dac5c43430b"))) ? (((getMinToStart()) != (50)) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d0b8021f-8479-3cce-8bc0-da59c60e02b2"))) ? (((0) == (count)) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c5ac49a2-4357-37ca-b242-d77368b7f1f9"))) ? (((getMinToStart()) == (countOfRegionServers())) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("00330f9e-0a71-326d-8536-7bfc535c4fd7"))) ? (((getMinToStart()) != (0)) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bd63e0cf-00ac-36a6-bd44-c8e43550a845"))) ? ((4500) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("167f4be8-e317-3334-a06f-30234f1f4840"))) ? (((oldCount) == (getMinToStart())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b3451aae-cfd7-3099-b381-ba7e780c6d3b"))) ? (((countOfRegionServers()) == (count)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3a1524ae-f05f-3d2b-a6b2-d38d248d2401"))) ? (((1500) != (getMinToStart())) && ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("27a5966f-b31a-3040-8366-763b70cee501"))) ? (((oldCount) == (1500)) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("91d5e561-7f77-3bf5-8008-ddd68f88edfd"))) ? (((0) != (countOfRegionServers())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("ac7369db-960b-3bc6-8202-1aacd4bc2ace"))) ? (((oldCount) == (50)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("095228db-a10a-33bb-ad69-5e4188e80645"))) ? (((0) == (countOfRegionServers())) || ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d37a5920-feda-32dc-a379-f92ce51f613a"))) ? (((50) == (countOfRegionServers())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("79fb62be-40ec-3cfe-b314-88e6966be3ee"))) ? (((countOfRegionServers()) == (countOfRegionServers())) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d045eed4-f520-3bc3-84d9-efd7ed94499e"))) ? (((0) == (countOfRegionServers())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("aa9a22a1-8747-34a3-876d-4fcfdaa7ce08"))) ? (((1500) == (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("feabe850-85b1-31c3-bc12-215d6bf91ea0"))) ? (((50) == (getMinToStart())) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fa0c4461-9637-3b9b-a4b5-d357e86e4a15"))) ? (((50) == (count)) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6cfddb7b-63e6-3a5b-a5c1-3d6f556f4b10"))) ? (((countOfRegionServers()) == (50)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e21fc522-0a4b-3f65-93c8-7089a516d7c1"))) ? (((1500) != (getMinToStart())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("15bd7e75-9a69-34fb-b895-4104e8acfe4e"))) ? ((oldCount) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("d732edf2-1090-37de-bfe4-38f7162d49a6"))) ? (((countOfRegionServers()) == (count)) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8db297b3-5836-3ec2-9fd0-54f4f0ccc6d5"))) ? (((50) != (count)) && ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("553c9b01-0200-388d-9153-c6ece5787856"))) ? (((1500) == (count)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7be6f829-c400-3dff-a178-5b0cf2bd64e9"))) ? (((1500) == (countOfRegionServers())) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b11d1dd8-e52a-3540-a1a6-ce2d6a63b105"))) ? (((getMinToStart()) == (4500)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3364c9c0-9de6-328d-9843-c99d5d440421"))) ? (((1500) != (getMinToStart())) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a050a76f-3189-3568-9bf3-0eb9d7e0180a"))) ? (((getMinToStart()) == (getMinToStart())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("952647cd-90f7-3827-8406-05015c8d4c85"))) ? (((getMinToStart()) == (countOfRegionServers())) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1c9842a6-41fe-3785-89a4-400211054af6"))) ? (((0) != (count)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ffeceb38-c3bc-3893-bf90-bc93c2b5314a"))) ? (((oldCount) != (countOfRegionServers())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b19279e6-8e25-3c7f-8161-13bd6bfb395d"))) ? (((countOfRegionServers()) == (countOfRegionServers())) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("50b7f460-83b9-392e-a055-46d496c7b632"))) ? (((1500) != (countOfRegionServers())) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("045a89cb-5ebe-3e73-9b0f-9acb94e9080c"))) ? (((countOfRegionServers()) == (countOfRegionServers())) && ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9604a887-9b03-39d1-a47b-dacd00622b24"))) ? (((4500) == (countOfRegionServers())) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7893bc8b-64a8-34c2-b5c6-1e262e8e6aea"))) ? (((4500) != (getMinToStart())) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4382a61b-feaa-3b64-9776-9818cbba9e30"))) ? (((0) != (count)) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2c633a70-2817-3da3-bb1b-9daa4faf71da"))) ? (((oldCount) != (countOfRegionServers())) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1744ee61-fe57-3b20-ae5e-38e492b2748a"))) ? (((0) == (countOfRegionServers())) && (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ba6c5932-86ea-37bf-9e36-e3337e7061aa"))) ? (((countOfRegionServers()) == (0)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a3cf0831-4337-3223-ad87-3f4c61e96687"))) ? (((0) == (count)) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b00740bf-2b1d-30ca-afe0-531edd553ce2"))) ? (((4500) == (countOfRegionServers())) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("65c089ce-2acc-3029-8344-bfcb9dec91de"))) ? (((getMinToStart()) == (count)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("806cc1f1-a5a6-3789-829b-69327d913cfc"))) ? (((getMinToStart()) != (4500)) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9c218627-2298-3ad4-ba3a-0bc1b164f866"))) ? (((countOfRegionServers()) != (1500)) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2290d062-16e4-324d-85d6-44bbdfa92866"))) ? ((oldCount) == (count)) : (((KnobRuntime.check(java.util.UUID.fromString("a4b6eefd-ed36-320b-bb74-64ffbe95607f"))) ? (((oldCount) == (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("eeed31aa-2d3e-38af-8d9a-829e9f19a786"))) ? (((countOfRegionServers()) == (getMinToStart())) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("655ebcf6-0f1f-3601-80b6-ed32410aeb6b"))) ? (((countOfRegionServers()) == (1500)) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5aad9290-418d-3e26-a9ec-1d221bb6fe01"))) ? (((getMinToStart()) != (count)) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4e46ae2f-0a04-3b89-9f49-0e133f848aba"))) ? (((oldCount) != (0)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2585dc0e-f72d-38e4-bda2-1d9269022bc2"))) ? (((getMinToStart()) == (countOfRegionServers())) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("aab78ca3-3f83-3c9a-aa59-c7697aa00a44"))) ? (((oldCount) == (4500)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("27aa6179-bf0f-3812-a20c-201d3a4dc91a"))) ? (((0) != (getMinToStart())) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("213a3eae-7b84-3dd7-932b-a629987d9185"))) ? (((0) != (countOfRegionServers())) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5d6d340c-7ab5-34b6-85f6-283f8da22835"))) ? (((oldCount) == (getMinToStart())) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4656c6ed-4159-3feb-a0af-363babc7f3d2"))) ? (((countOfRegionServers()) != (50)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1fe97ea7-9b74-36fe-ba16-65967ff69de2"))) ? (((50) != (countOfRegionServers())) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b1ee82a9-7b7e-3c67-9ca1-64d6bf0329bf"))) ? (((oldCount) == (countOfRegionServers())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fb8fc94e-e1ed-340e-8fee-041cf1fc3527"))) ? (((getMinToStart()) == (1500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("dfdd1d8a-477b-3369-a776-9d3402c4820c"))) ? (((getMinToStart()) != (getMinToStart())) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("561e352c-7b51-324c-a6ac-a243f2b2e88f"))) ? (((oldCount) != (getMinToStart())) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4bddbfb1-945a-3b18-acec-f83dad8320dd"))) ? (((getMinToStart()) != (4500)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("efef5d37-8562-39ad-855b-2b338c983c68"))) ? (((oldCount) == (0)) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("f43912e3-5541-36e6-8eeb-bdfb8d14ebf2"))) ? (((getMinToStart()) != (4500)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d0e8306c-6183-31ee-8352-8d1aae8ac3c4"))) ? (((getMinToStart()) == (getMinToStart())) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6b0b7300-ee3d-3556-91d9-98c4fc664648"))) ? (((getMinToStart()) == (0)) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0a5c0564-45de-3145-b1a3-360cf566c58a"))) ? (((countOfRegionServers()) == (countOfRegionServers())) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6a8a3665-4b0b-360c-95ec-3327bc243fcd"))) ? (((50) != (count)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3e33e293-2bb0-3254-898b-e4456820c85f"))) ? (((getMinToStart()) == (countOfRegionServers())) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7ae20d49-47ac-3437-8575-158ffe2da09e"))) ? (((oldCount) == (countOfRegionServers())) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("45512898-5153-3287-a6e7-02f33ed4efd9"))) ? (((getMinToStart()) == (4500)) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4d6ef9c6-804f-3376-a3e9-d11d6daa6810"))) ? (((50) == (getMinToStart())) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6b90cab5-f878-3e87-bea1-821315bdccc4"))) ? (((getMinToStart()) == (getMinToStart())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b40488c2-6c2d-3679-a07e-57a34f8f95b7"))) ? (((getMinToStart()) != (getMinToStart())) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("637b64ba-e4ed-3ab4-a126-5c9c5f75289f"))) ? (((1500) == (getMinToStart())) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b7b044e5-301a-3cf2-a1ab-1917db96965c"))) ? (((getMinToStart()) != (countOfRegionServers())) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9b0ea57a-361f-3e8c-ad4f-2d46f9e30dc3"))) ? (((getMinToStart()) == (countOfRegionServers())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("f3bed5de-82d5-3593-9440-5359d7cc1073"))) ? (((countOfRegionServers()) == (count)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e3e37c6d-1bba-3cbc-8c40-f700cd91a137"))) ? (((oldCount) == (50)) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("281170d6-0045-3506-9202-eec30f377161"))) ? (((getMinToStart()) != (countOfRegionServers())) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c071ca53-d4d4-36a5-9d8f-b95afe17e54f"))) ? (((getMinToStart()) == (countOfRegionServers())) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7dde4f97-7d31-3b13-aaf2-b46b73791d80"))) ? ((oldCount != count) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1ddc2029-775c-31c8-a497-25075947bfd7"))) ? (((4500) == (count)) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("febc8335-1801-3ae2-a924-f18ecd2f292d"))) ? (((oldCount) != (getMinToStart())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("358592ee-f19b-3d89-b418-9eccaac7a37c"))) ? (((countOfRegionServers()) == (50)) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e64d2533-aa9e-3903-9a9a-f99642228836"))) ? (((countOfRegionServers()) == (countOfRegionServers())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("87b83cf0-b4d4-33ed-9a54-d6f5c2b171a0"))) ? (((oldCount) == (1500)) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fe9e4921-6e2f-300d-9641-f44bc05d03b6"))) ? (((oldCount) == (0)) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("75f70ad3-aad5-345f-8515-f1fe57d667d6"))) ? (((getMinToStart()) != (getMinToStart())) && ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("07fbc5c5-b62c-30ea-9dbe-a2cd6eeabc64"))) ? (((1500) != (countOfRegionServers())) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b3082193-82f2-3072-add4-bf227cb43e64"))) ? (((getMinToStart()) == (50)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("14244087-aa1a-3820-994e-7f68bbde195c"))) ? (((1500) != (count)) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7cf7902d-4de6-3c97-9b6a-67040ce21ffa"))) ? (((4500) != (countOfRegionServers())) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cf89c33f-19f6-314c-81d1-94ebd116e618"))) ? (((oldCount) != (getMinToStart())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("bcec6c49-d5e4-397c-a312-a088c2ee1a4b"))) ? (((countOfRegionServers()) != (countOfRegionServers())) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c8e043ca-6f72-332f-86a3-b9895187468b"))) ? (((getMinToStart()) == (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("6de555c7-0f29-3032-ac01-80bb3387f2b7"))) ? ((oldCount != count) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6c8d40c0-0217-34f2-8c19-3919d7dda68e"))) ? (((countOfRegionServers()) != (4500)) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("96775492-051a-3efc-8e75-41cf291cc348"))) ? (((getMinToStart()) == (count)) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f582f6dc-1e1c-3aa9-b357-c09837dff2a5"))) ? (((countOfRegionServers()) == (50)) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("898ed8fd-62ce-35f8-8555-742e7c67ee8f"))) ? (((oldCount) == (1500)) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("25ca3ce5-2679-3a08-8886-b0a4605aff0b"))) ? (((50) != (getMinToStart())) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d6769d60-d662-3a8a-92aa-2ce026d57941"))) ? (((getMinToStart()) != (1500)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("65d7dc1d-f329-32fb-b962-2c37d93e17df"))) ? (((countOfRegionServers()) == (4500)) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b85c20f0-925b-3590-ba14-b91ec297ed10"))) ? (((oldCount) != (getMinToStart())) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("520145e8-a7fd-32bc-a7c0-9e5b859bdc8a"))) ? (((0) == (count)) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a475b0fb-7e29-37eb-a39a-2b0b896bedc9"))) ? (((1500) != (count)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b078641e-6114-3a13-8106-e099ca58a96b"))) ? (((getMinToStart()) != (countOfRegionServers())) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b17e1c93-08e2-34b2-8d0f-0680afe4c7e5"))) ? (((oldCount) == (countOfRegionServers())) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d56d4df5-48b1-314e-8ac2-4ac2ea11f6a6"))) ? (((countOfRegionServers()) == (countOfRegionServers())) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f3d477ff-b091-3848-838d-a5db935eeb35"))) ? (((oldCount) == (getMinToStart())) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cb0aa295-e5b2-3dad-a57e-633caf57bb11"))) ? (((countOfRegionServers()) == (1500)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("aa898fdd-f2a4-3cdb-8b29-357f36650283"))) ? (((50) == (count)) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6fafbb4e-22fc-31c9-a70d-09b7ca4bd958"))) ? (((1500) != (countOfRegionServers())) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b976ff38-42df-3c66-85de-b085a4ba65d0"))) ? (((countOfRegionServers()) != (getMinToStart())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cdb4b347-66c9-376d-a13d-d0dbed59d096"))) ? (((countOfRegionServers()) == (1500)) || ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d991f453-7a0d-3714-ab39-e5cba6becc04"))) ? (((oldCount) != (getMinToStart())) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("927e589e-4446-3497-9972-8c9891ef77d1"))) ? (((countOfRegionServers()) != (0)) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f2c7ed17-fc4e-350b-a736-d5d5710303ce"))) ? (((oldCount) == (countOfRegionServers())) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d20a86da-74da-38e4-a015-2caa33f3155f"))) ? (((getMinToStart()) == (count)) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fa352383-7dfa-3bf8-a20d-2ca938c426ca"))) ? (((0) == (count)) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("64f391d4-d5f6-30d4-ac3b-9a6052d968fe"))) ? ((oldCount != count) && ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("eba550d2-0bbf-3585-978d-1324b6511c61"))) ? (((1500) == (countOfRegionServers())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e77fd89e-7dc4-387c-965f-4f6768dff397"))) ? (((getMinToStart()) == (count)) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c70bcaf1-fffa-31a9-bb66-63ea2d956344"))) ? (((0) == (getMinToStart())) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("85d438f1-d341-3391-9f70-3466f9ff0365"))) ? (((4500) == (count)) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7e986ab6-074b-35ed-852e-73bd9a8658a8"))) ? (((1500) == (getMinToStart())) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("aa34b7fd-2daf-3d33-96cb-afdf7210d9ac"))) ? (((getMinToStart()) == (1500)) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("56300757-0bcb-3c8c-ba5c-62b7bebade00"))) ? (((getMinToStart()) == (countOfRegionServers())) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2cfa0136-b6a0-380e-bf3f-d43be9c89d93"))) ? (((oldCount) == (4500)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("880590dc-e766-36cb-9a0d-4303524111ed"))) ? (((oldCount) == (50)) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("00760bad-25f5-3e89-8da6-c70355b10ea0"))) ? (((countOfRegionServers()) == (0)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("25d57a76-f1be-3eaf-8fce-2a5c32a70d64"))) ? (((50) != (countOfRegionServers())) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("212d7627-9ce8-3b22-b6a8-e16b3435861d"))) ? (((getMinToStart()) != (0)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e8356b5b-8d47-32e1-a0ab-fe36ef136143"))) ? (((countOfRegionServers()) == (count)) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4705b9fc-ebbe-3dd9-85c8-f07cb8c3fc1e"))) ? (((getMinToStart()) != (50)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5d34d947-7f1f-3c68-a295-cabee19b9082"))) ? (((oldCount) == (50)) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("30fe5bc3-71a2-3278-b450-6e473d138322"))) ? (((oldCount) != (0)) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d6002e6e-b62f-3be2-9c1d-28756c60447b"))) ? (((1500) == (getMinToStart())) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("82744e71-4921-3c5b-8bf0-1f9a2c9594e5"))) ? (((1500) == (getMinToStart())) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("dfe4c3a9-982a-3354-b507-e393b0fde273"))) ? (((1500) == (count)) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b93af258-3b02-37b0-af3a-3bf4cff238e4"))) ? (((oldCount) == (4500)) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7091654b-fc80-3816-8175-e075940a5c49"))) ? (((getMinToStart()) != (getMinToStart())) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("40987fad-9181-34e9-923a-8bf2de6e7c2a"))) ? (((countOfRegionServers()) == (50)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("149eab74-663e-36be-aa0f-3365e2917ce6"))) ? (((countOfRegionServers()) == (0)) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e4dab9c9-e087-311d-b6d2-21957f0331cd"))) ? (((50) != (countOfRegionServers())) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("47e57e2e-a6c9-368b-93a8-6f2f16f7650f"))) ? (((countOfRegionServers()) == (50)) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("745f9cad-61e4-31f7-8a53-e0650012a73f"))) ? (((oldCount) == (4500)) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("94e628ae-a0d1-37f0-82ec-945c7221d866"))) ? (((countOfRegionServers()) != (countOfRegionServers())) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b311268d-ce36-34fa-9b0f-a43305438d3f"))) ? (((getMinToStart()) != (50)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("90be9405-16d4-34db-b684-aa6c535daedc"))) ? (((getMinToStart()) != (getMinToStart())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("013766ff-66c4-35cc-aa41-12ef4bf8b013"))) ? (((4500) == (count)) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e14f34cf-eaaf-3a14-abce-f7cc05d38ac0"))) ? (((countOfRegionServers()) == (50)) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("66cd859c-fd0d-390d-901a-5a491cad6793"))) ? (((getMinToStart()) == (countOfRegionServers())) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("68f65681-f794-3bea-bc05-8bff5c1d84b9"))) ? (((4500) == (countOfRegionServers())) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("78e1366e-b747-35f0-be0d-035e05cfcc4f"))) ? (((getMinToStart()) != (50)) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("881b4ee8-9939-37e1-acb8-0d434237c407"))) ? (((getMinToStart()) != (count)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("31491566-93ad-37d9-b43e-fc9e160c304d"))) ? (((1500) != (count)) || ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f15a4f81-2572-3b8d-95c4-38cf5d6c5bc5"))) ? (((4500) != (getMinToStart())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("dce09aaf-5a69-3a40-9e3d-a88ad22487bf"))) ? (((countOfRegionServers()) == (getMinToStart())) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ef837e05-fd4d-38ec-8e29-a882429a7d0b"))) ? (((getMinToStart()) == (getMinToStart())) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3e475f0c-4024-3c7c-b509-8aa0fd6c88fd"))) ? (((oldCount) != (count)) && ((lastLogTime + interval) != (now))) : (oldCount != count || lastLogTime + interval < now))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        lastLogTime = now;
        String msg =
          "Waiting on regionserver count=" + count + "; waited=" + slept + "ms, expecting min="
            + minToStart + " server(s), max=" + getStrForMax(maxToStart) + " server(s), "
            + "timeout=" + timeout + "ms, lastChange=" + (now - lastCountChange) + "ms";
        LOG.info(msg);
        status.setStatus(msg);
      }

      // We sleep for some time
      final long sleepTime = 50;
      Thread.sleep(sleepTime);
      now = EnvironmentEdgeManager.currentTime();
      slept = now - startTime;

      oldCount = count;
      count = countOfRegionServers();
      if (count != oldCount) {
        lastCountChange = now;
      }
    }
    // Did we exit the loop because cluster is going down?
    if (isClusterShutdown()) {
      this.master.stop("Cluster shutdown");
    }
    LOG.info("Finished waiting on RegionServer count=" + count + "; waited=" + slept + "ms,"
      + " expected min=" + minToStart + " server(s), max=" + getStrForMax(maxToStart)
      + " server(s)," + " master is " + (this.master.isStopped() ? "stopped." : "running"));
  }

  private String getStrForMax(final int max) {
if(KnobRuntime.check(java.util.UUID.fromString("a585fa03-9f8f-3047-b2b6-21ac26f23860"))) {
return null;
}
    return max == Integer.MAX_VALUE ? "NO_LIMIT" : Integer.toString(max);
  }

  /** Returns A copy of the internal list of online servers. */
  public List<ServerName> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    // TODO: FIX. THIS IS POPULAR CALL.
    return new ArrayList<>(this.onlineServers.keySet());
  }

  /**
   * @param keys                 The target server name
   * @param idleServerPredicator Evaluates the server on the given load
   * @return A copy of the internal list of online servers matched by the predicator
   */
  public List<ServerName> getOnlineServersListWithPredicator(List<ServerName> keys,
    Predicate<ServerMetrics> idleServerPredicator) {
    List<ServerName> names = new ArrayList<>();
    if (keys != null && idleServerPredicator != null) {
      keys.forEach(name -> {
        ServerMetrics load = onlineServers.get(name);
        if (load != null) {
          if (idleServerPredicator.test(load)) {
            names.add(name);
          }
        }
      });
    }
    return names;
  }

  /** Returns A copy of the internal list of draining servers. */
  public List<ServerName> getDrainingServersList() {
    return new ArrayList<>(this.drainingServers);
  }

  public boolean isServerOnline(ServerName serverName) {
    return serverName != null && onlineServers.containsKey(serverName);
  }

  public enum ServerLiveState {
    LIVE,
    DEAD,
    UNKNOWN
  }

  /** Returns whether the server is online, dead, or unknown. */
  public synchronized ServerLiveState isServerKnownAndOnline(ServerName serverName) {
    return onlineServers.containsKey(serverName)
      ? ServerLiveState.LIVE
      : (deadservers.isDeadServer(serverName) ? ServerLiveState.DEAD : ServerLiveState.UNKNOWN);
  }

  /**
   * Check if a server is known to be dead. A server can be online, or known to be dead, or unknown
   * to this manager (i.e, not online, not known to be dead either; it is simply not tracked by the
   * master any more, for example, a very old previous instance).
   */
  public synchronized boolean isServerDead(ServerName serverName) {
    return serverName == null || deadservers.isDeadServer(serverName);
  }

  /**
   * Check if a server is unknown. A server can be online, or known to be dead, or unknown to this
   * manager (i.e, not online, not known to be dead either; it is simply not tracked by the master
   * any more, for example, a very old previous instance).
   */
  public boolean isServerUnknown(ServerName serverName) {
    return serverName == null
      || (!onlineServers.containsKey(serverName) && !deadservers.isDeadServer(serverName));
  }

  public void shutdownCluster() {
    String statusStr = "Cluster shutdown requested of master=" + this.master.getServerName();
    LOG.info(statusStr);
    this.clusterShutdown.set(true);
    if (onlineServers.isEmpty()) {
      // we do not synchronize here so this may cause a double stop, but not a big deal
      master.stop("OnlineServer=0 right after cluster shutdown set");
    }
  }

  public boolean isClusterShutdown() {
    return this.clusterShutdown.get();
  }

  /**
   * Stop the ServerManager.
   */
  public void stop() {
    // Nothing to do.
  }

  /**
   * Creates a list of possible destinations for a region. It contains the online servers, but not
   * the draining or dying servers.
   * @param serversToExclude can be null if there is no server to exclude
   */
  public List<ServerName> createDestinationServersList(final List<ServerName> serversToExclude) {
    Set<ServerName> destServers = new HashSet<>();
    onlineServers.forEach((sn, sm) -> {
      if (sm.getLastReportTimestamp() > 0) {
        // This means we have already called regionServerReport at leaset once, then let's include
        // this server for region assignment. This is an optimization to avoid assigning regions to
        // an uninitialized server. See HBASE-25032 for more details.
        destServers.add(sn);
      }
    });

    if (serversToExclude != null) {
      destServers.removeAll(serversToExclude);
    }

    // Loop through the draining server list and remove them from the server list
    final List<ServerName> drainingServersCopy = getDrainingServersList();
    destServers.removeAll(drainingServersCopy);

    return new ArrayList<>(destServers);
  }

  /**
   * Calls {@link #createDestinationServersList} without server to exclude.
   */
  public List<ServerName> createDestinationServersList() {
    return createDestinationServersList(null);
  }

  /**
   * To clear any dead server with same host name and port of any online server
   */
  void clearDeadServersWithSameHostNameAndPortOfOnlineServer() {
    for (ServerName serverName : getOnlineServersList()) {
      deadservers.cleanAllPreviousInstances(serverName);
    }
  }

  /**
   * Called by delete table and similar to notify the ServerManager that a region was removed.
   */
  public void removeRegion(final RegionInfo regionInfo) {
    final byte[] encodedName = regionInfo.getEncodedNameAsBytes();
    storeFlushedSequenceIdsByRegion.remove(encodedName);
    flushedSequenceIdByRegion.remove(encodedName);
  }

  public boolean isRegionInServerManagerStates(final RegionInfo hri) {
    final byte[] encodedName = hri.getEncodedNameAsBytes();
    return (storeFlushedSequenceIdsByRegion.containsKey(encodedName)
      || flushedSequenceIdByRegion.containsKey(encodedName));
  }

  /**
   * Called by delete table and similar to notify the ServerManager that a region was removed.
   */
  public void removeRegions(final List<RegionInfo> regions) {
    for (RegionInfo hri : regions) {
      removeRegion(hri);
    }
  }

  /**
   * May return 0 when server is not online.
   */
  public int getVersionNumber(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getVersionNumber() : 0;
  }

  /**
   * May return "0.0.0" when server is not online
   */
  public String getVersion(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getVersion() : "0.0.0";
  }

  public int getInfoPort(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getInfoServerPort() : 0;
  }
}
