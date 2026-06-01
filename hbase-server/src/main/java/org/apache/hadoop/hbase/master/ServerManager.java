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
if(KnobRuntime.check(java.util.UUID.fromString("60975b52-514c-3a9a-b616-f5f08bb9fcc8"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("74da4077-7895-3e75-92e5-7b40d1a641ae"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c67ec73c-46f0-3b01-af86-3edd5ab0731c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("11c99482-51a1-35a0-b684-e73563cb6a9d"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("2570dcfd-508f-3e8d-91be-7de83ae4da7d"))) { moveFromOnlineToDeadServers(sn); } else { checkRejectableDecommissionedStatus(sn); }

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
        if (((KnobRuntime.check(java.util.UUID.fromString("dcd4fe45-17dd-3475-958c-da48062dd883"))) ? ((l != HConstants.NO_SEQNUM && l > existingValue.longValue())) : (((KnobRuntime.check(java.util.UUID.fromString("646a2a4f-335e-3014-946f-ee1871c7e9ef"))) ? (((existingValue) == (null)) || ((l != HConstants.NO_SEQNUM && l > existingValue.longValue()))) : (((KnobRuntime.check(java.util.UUID.fromString("81c457a0-f432-3b85-9100-0586f11ba07e"))) ? ((existingValue) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b8f0fdde-409b-3a8b-9206-c55dfbc20f21"))) ? (((existingValue) == (null)) && ((l != HConstants.NO_SEQNUM && l > existingValue.longValue()))) : (((KnobRuntime.check(java.util.UUID.fromString("97592678-bd22-3133-abfc-4929b7d87110"))) ? ((existingValue == null) && ((l != HConstants.NO_SEQNUM && l > existingValue.longValue()))) : (((KnobRuntime.check(java.util.UUID.fromString("1f0e6fd0-e513-383c-95e8-9c9282dcc8a5"))) ? ((existingValue == null) || ((l != HConstants.NO_SEQNUM && l > existingValue.longValue()))) : (((KnobRuntime.check(java.util.UUID.fromString("6dcb09d1-d71d-34e6-99a3-38d8c09c660c"))) ? (((existingValue) != (null)) || ((l != HConstants.NO_SEQNUM && l > existingValue.longValue()))) : (((KnobRuntime.check(java.util.UUID.fromString("e5ee1a03-993e-34f0-a6d4-f09e5d3a7e25"))) ? ((existingValue) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d0e25d09-9c69-3145-bd86-81463e5a32d7"))) ? (existingValue == null) : (((KnobRuntime.check(java.util.UUID.fromString("3c4a9f0f-5a5c-373d-a1b3-ab212cc93c65"))) ? (((existingValue) != (null)) && ((l != HConstants.NO_SEQNUM && l > existingValue.longValue()))) : (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue.longValue())))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36ef34c5-2817-39d8-9bd4-b9a4e1ef50e0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc76a849-ae77-369d-b254-55f820a38449"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("8e8fa2d2-5cc5-3083-b7a2-bca7632c5fd2"))) { updateLastFlushedSequenceIds(findServerWithSameHostnamePortWithLock(serverName), sl); } else if (KnobRuntime.check(java.util.UUID.fromString("81d010de-f784-3fbf-b3b4-77be44513b0c"))) { recordNewServerWithLock(findServerWithSameHostnamePortWithLock(serverName), sl); } else { recordNewServerWithLock(serverName, sl); }
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
    } else if (((KnobRuntime.check(java.util.UUID.fromString("78fecfe7-1f8a-38fb-b8de-4f4d3936d8dd"))) ? ((skew) < (warningSkew)) : (((KnobRuntime.check(java.util.UUID.fromString("b6e05da7-9bb3-3cd9-99cc-dacb15647b15"))) ? ((skew) > (warningSkew)) : (((KnobRuntime.check(java.util.UUID.fromString("83927da6-82da-3cf0-93f5-604528d533ce"))) ? ((skew) == (warningSkew)) : (((KnobRuntime.check(java.util.UUID.fromString("a9897587-e6e8-311b-8c7f-8267814802ae"))) ? ((skew) >= (warningSkew)) : (((KnobRuntime.check(java.util.UUID.fromString("732ac201-2f02-3c09-b4d5-c7151f0e2850"))) ? ((skew) <= (warningSkew)) : (((KnobRuntime.check(java.util.UUID.fromString("58d0e7b1-b55f-3a29-a2d3-fb7ec12ba7f5"))) ? ((skew) != (warningSkew)) : (skew > warningSkew))))))))))))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("561afdcb-7eb4-3558-a11a-d30e3747ddb2"))) { LOG.trace(message); } else if (KnobRuntime.check(java.util.UUID.fromString("294589ca-de7d-3ffc-9920-8c457422ccd9"))) { LOG.warn(message); } else { LOG.debug(message); }
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
if(KnobRuntime.check(java.util.UUID.fromString("9a3d1b71-4069-30fb-acdb-9c00a96a1eed"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("b0d55696-9c2c-38d4-84d9-03b0068f4c09"))) { LOG.info("{} {} came back up, removed it from the dead servers list", what, serverName); } else { LOG.debug("{} {} came back up, removed it from the dead servers list", what, serverName); }
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
    if (((KnobRuntime.check(java.util.UUID.fromString("49a8eb95-dcb5-3421-a5af-ebf67dc53621"))) ? ((r) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5ec360bd-e4fb-3ce6-b5cd-227d9911a7f0"))) ? ((r) != (null)) : (r != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b43ac98c-a3e9-3e72-b94f-b7bcc9df0ed8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("1b65c141-44b7-339d-8edd-d8139d429167"))) {
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
    return ((KnobRuntime.check(java.util.UUID.fromString("7db3ed95-7675-3331-b42f-827fff2e5903"))) ? (this.onlineServers.remove(serverName)) : (this.onlineServers.get(serverName)));
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
if(KnobRuntime.check(java.util.UUID.fromString("601ec60c-011e-3a43-8ad0-e2e4fd1f1207"))) {
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
      boolean online = this.onlineServers.containsKey(sn);
      if (online) {
        // Remove the server from the known servers lists and update load info BUT
        // add to deadservers first; do this so it'll show in dead servers list if
        // not in online servers list.
if(KnobRuntime.check(java.util.UUID.fromString("c4a5f51c-973b-3020-a2e1-c63fa6593524"))) {
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
        if (KnobRuntime.check(java.util.UUID.fromString("3d6244f3-c893-35de-852d-2e190b44e899"))) { moveFromOnlineToDeadServers(sn); } else { this.deadservers.putIfAbsent(sn); }
if(KnobRuntime.check(java.util.UUID.fromString("b4e651f0-a1f0-3a81-8bd2-f8dd7207ae11"))) {
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
        if (KnobRuntime.check(java.util.UUID.fromString("ce739f2e-aa33-3d2b-81fa-2a14ab1ca491"))) { this.onlineServers.get(sn); } else { this.onlineServers.remove(sn); }
        onlineServers.notifyAll();
      } else {
        // If not online, that is odd but may happen if 'Unknown Servers' -- where meta
        // has references to servers not online nor in dead servers list. If
        // 'Unknown Server', don't add to DeadServers else will be there for ever.
if(KnobRuntime.check(java.util.UUID.fromString("f267f0f6-e4cd-3f7e-9da5-ef65dbfbe197"))) {
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
        if (KnobRuntime.check(java.util.UUID.fromString("3b27a225-df86-3768-9b45-f1e6ce8b90b1"))) { LOG.info("Expiration of {} but server not online", sn); } else if (KnobRuntime.check(java.util.UUID.fromString("16b9b7e3-9f28-30b8-b4e1-008f4a5a8192"))) { LOG.warn("Expiration of {} but server not online", sn); } else { LOG.trace("Expiration of {} but server not online", sn); }
      }
    }
  }

  /*
   * Remove the server from the drain list.
   */
  public synchronized boolean removeServerFromDrainList(final ServerName sn) {
if(KnobRuntime.check(java.util.UUID.fromString("4107ab8e-7408-3d25-8269-7aef3820c0d5"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("41a61a63-a580-313b-a6e9-e39f733d0c48"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9f27f77d-b817-3c2b-af92-6334a7accaf0"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("ac3f0c93-5d40-3788-895d-5714c96b24a9"))) { LOG.warn("Removing server {} from the draining list.", sn); } else { LOG.info("Removing server {} from the draining list.", sn); }

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

if(KnobRuntime.check(java.util.UUID.fromString("e227b0f2-c75c-3e42-81ca-caa4da36f0de"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("25603118-f2ea-3ef4-af94-2a875f246d00"))) { LOG.warn("Server {} added to draining server list.", sn); } else { LOG.info("Server {} added to draining server list.", sn); }
if(KnobRuntime.check(java.util.UUID.fromString("0a3dfe76-0dc6-3210-a405-4c86bc6a9894"))) {
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
    return this.drainingServers.add(sn);
  }

  // RPC methods to region servers

  private HBaseRpcController newRpcController() {
if(KnobRuntime.check(java.util.UUID.fromString("e38356af-9598-3c26-8488-4038c49d9508"))) {
return null;
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("50c12825-2f8b-3d72-ad60-a049c90802e7"))) ? ((1500) < (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("36791e6e-e922-3af2-a415-3aa84881f17e"))) ? ((1500) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("d36f8b64-53b9-3ea4-b12c-933028e50cf6"))) ? ((getMinToStart()) > (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("fa01d48d-c249-3dbd-9002-e742e9bfb7af"))) ? ((maxToStart) == (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("dd40dd63-21e8-316c-a8ec-7de0fae52a67"))) ? ((maxToStart) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("33314f48-73ca-316f-9e71-5931b485f2f5"))) ? ((0) < (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("9f320dda-0521-3e91-a3bc-1ab3804e7c2c"))) ? ((maxToStart) > (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("214217a1-f2be-3fb3-8ef9-4da1aa19285e"))) ? ((getMinToStart()) > (50)) : (((KnobRuntime.check(java.util.UUID.fromString("cd202dcb-2d15-315a-b9ed-aed2b01570dd"))) ? ((50) > (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("f1b87496-8249-35ff-8c30-99da2131bf2d"))) ? ((50) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("97d5b50c-89b3-3602-baa7-329294eb9b59"))) ? ((getMinToStart()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6b89e828-6106-3a2f-8f8a-917e410edbfc"))) ? ((1500) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("8a9a14ac-b755-3ffb-9423-9892e2bbbf5a"))) ? ((countOfRegionServers()) != (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("ea175156-f74b-31c6-b826-e8b8d218c86d"))) ? ((0) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("dbaa65c2-abb0-3a62-a66b-bf59b7f37313"))) ? ((countOfRegionServers()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f1d7d9e2-4ff9-353b-b774-4bd168e74bf4"))) ? ((countOfRegionServers()) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("9b492ce6-116b-3158-8d6e-68df689a054c"))) ? ((1500) >= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("236ef99c-6fe7-31b9-b51f-a3ae4de8e41f"))) ? ((maxToStart) > (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("b48bfa12-9648-36c8-a731-e0964034308a"))) ? ((maxToStart) < (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("d97f9018-dc5f-375f-8244-a9b9d18141e4"))) ? ((maxToStart) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("610d27ef-1072-3c5c-b608-efe4211be1e3"))) ? ((maxToStart) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0a1613ed-3b60-3ed0-96e2-7c1fb4f5ac04"))) ? ((maxToStart) > (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("8c686051-0e34-3743-becd-f1a213dea096"))) ? ((countOfRegionServers()) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("980cec77-1839-3611-a040-306205c1cb57"))) ? ((50) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("803fcb02-0224-382c-9ee3-0a4cbef971a4"))) ? ((countOfRegionServers()) == (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("c050a16a-8695-3109-894a-602e236624d9"))) ? ((countOfRegionServers()) < (50)) : (((KnobRuntime.check(java.util.UUID.fromString("19c49cbc-fb18-31c4-a706-54a91acf3bc3"))) ? ((maxToStart) >= (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("f0524026-9142-3075-ad86-4799f8ce12a0"))) ? ((1500) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("018b3a4f-830d-3a13-8ecc-c4d402ed29bc"))) ? ((4500) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("efefc7c0-dc4f-3ebe-808e-aa3d13fb5897"))) ? ((countOfRegionServers()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e6fba5ca-4ca3-32eb-b966-74dfe68a458f"))) ? ((countOfRegionServers()) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("272a945a-4f92-3061-b46b-d19d9b894168"))) ? ((countOfRegionServers()) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("03929146-ddbe-36a7-b152-05dd195ac26b"))) ? ((maxToStart) > (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("af2bb89b-1814-37c3-8e50-f31268034b1e"))) ? ((getMinToStart()) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("a9fc9c05-b6ad-3868-b438-0e59f9d307e5"))) ? ((maxToStart) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("b9163269-23e3-3737-8121-abf5c75d6fa6"))) ? ((getMinToStart()) != (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("656ec84c-ecc8-3359-9c23-ed28164f4fca"))) ? ((maxToStart) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("13ffe62e-c7e7-3f78-955e-b42d639da434"))) ? ((getMinToStart()) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("9fe91a26-fd62-3039-9457-b66281ec2a9c"))) ? ((maxToStart) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("8c289055-ab53-3292-a20b-1bc91210b57d"))) ? ((countOfRegionServers()) != (50)) : (((KnobRuntime.check(java.util.UUID.fromString("3d59b9dc-e062-367f-b917-1b455cedb60a"))) ? ((50) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("b78e224e-b8d0-30c1-bccf-da54bc2a6eb0"))) ? ((50) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("c7b0aad5-df84-30a5-a862-722626e77b49"))) ? ((50) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("ceaf726d-8850-3a01-9dcf-015d2e58c88a"))) ? ((4500) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("c3cd45a5-195d-35d2-9672-da168bcb4d87"))) ? ((getMinToStart()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3df4aaa5-9817-35b6-b916-592a7b426b89"))) ? ((getMinToStart()) != (50)) : (((KnobRuntime.check(java.util.UUID.fromString("34e7a160-0b3d-339b-abcd-0b04cacaabc6"))) ? ((maxToStart) <= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("97f2d868-5b81-39b0-a16e-f427109061cc"))) ? ((maxToStart) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5a71d849-6b68-3996-b49c-cb5afb907571"))) ? ((50) < (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("3b5484a8-c662-3805-a495-7d2de361c03e"))) ? ((4500) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("c27aa53b-8f01-3d4c-82c1-669e4d693923"))) ? ((getMinToStart()) == (50)) : (((KnobRuntime.check(java.util.UUID.fromString("91677303-ec9a-3640-a85c-47cae7fbf4d5"))) ? ((maxToStart) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("afaa2b42-3edf-33c9-98c8-62f534c4e41d"))) ? ((getMinToStart()) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("95cefb41-e76b-3537-bae9-01f77b3ad6b4"))) ? ((countOfRegionServers()) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("0731dbbd-eee5-35f9-99cd-7a596574aca5"))) ? ((countOfRegionServers()) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("77187e34-25de-32b8-b340-560845f41297"))) ? ((getMinToStart()) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("c3b845c4-57f1-3bed-9ac6-5ad796dbba44"))) ? ((1500) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("95d22438-8ee7-3b3e-bb0c-d44f3f2defc5"))) ? ((4500) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("0f8398c9-a7e7-3641-9c10-fef7e417871a"))) ? ((0) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("614494fa-e459-3f55-8c63-62021e7669a3"))) ? ((getMinToStart()) == (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("04bcade6-862a-399f-bf87-67fc128d1ea9"))) ? ((countOfRegionServers()) < (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("bf8ae336-9355-3cb6-aaf6-cb75d5fe2dff"))) ? ((countOfRegionServers()) <= (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("1510f878-9e9c-372d-8588-10d4ccb21275"))) ? ((0) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("482f25eb-e317-3266-abbb-b6c6ad9c7760"))) ? ((countOfRegionServers()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1b8c90e4-f308-3db7-ac4f-4a66cabec3a5"))) ? ((0) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("e8d913b4-e901-3650-9eda-9b6b6140dfbf"))) ? ((getMinToStart()) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("50bf1297-4a8b-3be6-b838-6f78a96aeec2"))) ? ((countOfRegionServers()) > (50)) : (((KnobRuntime.check(java.util.UUID.fromString("2c22f84b-2f03-307e-a129-9958e2abc950"))) ? ((getMinToStart()) <= (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("a71a3fd9-9fe8-37d7-b3d3-126437f44cee"))) ? ((50) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("205de86d-79cf-3156-8e08-7c728b700a09"))) ? ((1500) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("21111e18-7102-3402-ac66-c5dd7cb81257"))) ? ((maxToStart) >= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("e6482cba-e27d-3aa4-908f-17e71d9f9895"))) ? ((getMinToStart()) > (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("99f98b74-ebac-320c-95bc-87ba3052c7c5"))) ? ((countOfRegionServers()) >= (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("9794bf07-f846-315e-a864-70fefebfe27e"))) ? ((countOfRegionServers()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2c5ebb46-ee10-382a-9575-354c75a95544"))) ? ((maxToStart) < (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("3d6e3347-6d78-3e19-916b-db20cd7d9e61"))) ? ((getMinToStart()) >= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("cf0c7599-8d71-303c-8adc-0cafe21d1b74"))) ? ((maxToStart) != (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("c21484de-0561-3a00-86c1-5faba834d3f1"))) ? ((getMinToStart()) > (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("3e1ec9ed-40ec-3255-a54d-2a08a3e1e25b"))) ? ((getMinToStart()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("376821a5-43c0-3ef7-8e24-dc2d01c63cd7"))) ? ((50) > (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("637c4d8e-ad67-336c-ad03-9f4226583a3a"))) ? ((50) >= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("e4f34ad4-6580-350e-b794-9595b7dd8b41"))) ? ((maxToStart) < (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("7e08f9ee-afe0-3f69-ab67-6f256cb525bc"))) ? ((50) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("671be1ee-b7ad-36ca-9435-e9b8429dc5bf"))) ? ((0) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("c07161f7-9fce-3ae7-93e0-a5dfacf1fcf1"))) ? ((countOfRegionServers()) < (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("d81edb73-1fe9-30cb-b190-fd1698714509"))) ? ((countOfRegionServers()) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("d1df31e2-30ce-310f-97b6-a9d5e9bbaacd"))) ? ((getMinToStart()) < (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("fbff779f-bc0d-351d-80d7-75b15c6de5b2"))) ? ((maxToStart) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("ea72ac93-993b-3b74-9884-1fddcfd4b035"))) ? ((maxToStart) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("2462f667-6d6e-3a0f-9083-ac308619104d"))) ? ((50) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("98d5aff1-a221-37c2-8bee-201ea7515dbe"))) ? ((maxToStart) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("e2f5192c-e96b-3a1a-a5b7-2606054b0719"))) ? ((getMinToStart()) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("f6e72a0d-ec4e-3f8a-bafe-e7a8b1b64423"))) ? ((getMinToStart()) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("2c2f7cbe-779d-3db7-8bc8-1e0667b96c62"))) ? ((countOfRegionServers()) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("9dc0861f-a18b-3612-be19-1f947dd812f3"))) ? ((50) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("c6cdf60e-73c8-3900-9965-366f27d4d753"))) ? ((getMinToStart()) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("f8b8270a-b97d-3bbc-acbe-ea413c55f05a"))) ? ((50) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("be2d9c4e-33ab-3050-befc-a946a42b6a2a"))) ? ((countOfRegionServers()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0d19ec15-cf0f-37ec-8986-798b2c0b962b"))) ? ((maxToStart) < (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("dc42213b-757d-3c8d-adf4-cb1939ff1dd2"))) ? ((maxToStart) >= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("4e104859-f00c-30b3-97c9-f28ae629bed8"))) ? ((1500) > (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("1f2d01c3-42d9-39a8-80af-e835900b382d"))) ? ((4500) > (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("e97ec30d-d06c-318a-a16e-ba9548f4e0d1"))) ? ((maxToStart) >= (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("30d027e5-54dd-3384-a104-9a9fe9e8cc3e"))) ? ((countOfRegionServers()) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("6668efce-26e3-3ad5-add9-8d4b6824c848"))) ? ((countOfRegionServers()) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("1703f11a-fbc6-3776-8074-e4e818b97920"))) ? ((50) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("7c6ae758-e134-370f-9ae7-ef1177202ab5"))) ? ((4500) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("709adfed-29d9-33e7-b399-5cf91425c84f"))) ? ((4500) >= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("2e84bc5d-77ba-3776-aa8e-83492dfbaca7"))) ? ((maxToStart) != (50)) : (((KnobRuntime.check(java.util.UUID.fromString("19edcd07-9874-3c73-b1ad-43b76cf2cf13"))) ? ((getMinToStart()) < (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("b3ce178f-fe65-3446-a2d3-c7f30a5f39e4"))) ? ((countOfRegionServers()) > (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("223a3090-a5f0-3c37-91be-c584101591a5"))) ? ((0) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("f00a1ec9-baf7-3006-8430-7aad7e6b730f"))) ? ((50) < (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("6df5e21e-ead7-30fe-b4ef-66adeec9cc33"))) ? ((1500) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("1e43ea92-1e4f-341f-aa90-92ac91f0d1bd"))) ? ((0) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("2d4a8d92-cff5-3cd4-a9ef-306232d9da05"))) ? ((4500) < (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("cf356a96-d420-33ef-ad52-37047deed7a6"))) ? ((countOfRegionServers()) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("48ad9a37-9727-3a24-9e3e-a0954ab2547c"))) ? ((getMinToStart()) <= (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("019afafa-4718-330e-a53a-9ffc4bd004fd"))) ? ((countOfRegionServers()) < (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("8a4b0d19-fd2c-3e95-997c-a7dbb1446300"))) ? ((50) != (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("b01452cc-83bc-323f-8a69-eb34cd9e6c9d"))) ? ((4500) < (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("e7e8253b-5769-338b-a761-3deda1af3f4e"))) ? ((countOfRegionServers()) > (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("d86f2880-b36a-3ec4-8534-6cd8257715ee"))) ? ((maxToStart) >= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("3e09cd3c-d3ed-378c-8204-360624e3b7e6"))) ? ((0) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("b807aad3-bf23-360b-b25a-6944f4858648"))) ? ((0) > (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("ff8197b2-a433-30d7-bd45-ba8a84083257"))) ? ((1500) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("d5aa376f-a170-3fdd-a8f0-56e435b7c87c"))) ? ((getMinToStart()) < (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("c4bdc330-3513-3f59-a363-e3059be78804"))) ? ((maxToStart) <= (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("01c62527-5a69-3720-9c35-829c2f87a9c6"))) ? ((4500) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("f4389ab0-f0f7-38b8-b994-1b3fb870067c"))) ? ((getMinToStart()) >= (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("ba149b54-d01a-37f7-b2f3-e933861b79cb"))) ? ((getMinToStart()) >= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("6a03f08d-df42-3caf-be7d-bdef106ec791"))) ? ((countOfRegionServers()) > (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("2f0f409f-8fd5-3d4e-84a5-b8a04e0626cb"))) ? ((4500) > (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("f714f2af-6555-3fba-bfad-20c1f8339e19"))) ? ((countOfRegionServers()) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("6db9472a-8b41-3e1b-b12c-5fdcd32fa4b0"))) ? ((maxToStart) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("851a5cac-b334-391d-bac2-480c9eb1cf3f"))) ? ((getMinToStart()) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("a2146eb7-9e3e-3133-9750-ecafc63492a2"))) ? ((getMinToStart()) >= (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("e750d20d-6117-333e-ac49-88413ca9d18a"))) ? ((getMinToStart()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5422372d-b998-3c90-baea-ea70cf8528df"))) ? ((countOfRegionServers()) <= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("0d5d3ca3-647a-35f0-a2e9-3b679a3eed9e"))) ? ((countOfRegionServers()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d022888b-b0f1-37ef-949c-9624a56de751"))) ? ((maxToStart) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("11283841-3bda-31b0-b981-98b815b81b29"))) ? ((4500) <= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("1397acd7-e1fb-33f9-b50f-17ddfe7e7349"))) ? ((getMinToStart()) > (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("ef1c43f1-2557-3c47-8f81-93c233487c71"))) ? ((1500) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("c8a35145-5127-359c-970f-e006dce05142"))) ? ((countOfRegionServers()) >= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("cb4ad39e-3f67-3f98-ae32-ecf37d7b6356"))) ? ((getMinToStart()) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("213ff787-1f5f-36f0-a1be-ea3d6691c235"))) ? ((1500) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("113f9d88-f718-3132-9cd6-44bf8362ec45"))) ? ((maxToStart) <= (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("1290bf2e-06c8-3cb0-a027-9b989db4a8e8"))) ? ((4500) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("4451e309-9551-3815-aa5f-01aa6d4651cb"))) ? ((getMinToStart()) < (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("80f57b1f-bb21-3fa9-9830-b5bd793a8d65"))) ? ((1500) < (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("c5a93bf6-035f-38dd-9688-c12cdb644449"))) ? ((4500) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("eca8c33a-714e-3b4d-b000-a26c19769cba"))) ? ((1500) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("5172893e-237d-3254-b568-63cbd2783900"))) ? ((0) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("7a2a5c13-2ef6-3b19-93bd-63c24bfefe88"))) ? ((1500) <= (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("ea704635-44d2-3353-a81a-dd431fa5a308"))) ? ((countOfRegionServers()) <= (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("3335e1db-35de-3da3-bd53-0c8622770f96"))) ? ((0) < (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("219c00cb-e834-35a4-9378-56092904b110"))) ? ((0) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("d9da32c9-b0a6-3368-90dc-f8f670cb4f61"))) ? ((1500) != (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("097d9842-75f1-380c-8f43-f1b623342418"))) ? ((maxToStart) <= (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("4842a492-42b3-3eb4-b1e0-eaa3982fa715"))) ? ((getMinToStart()) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("d14c5fba-7988-3c65-ab84-a53dbb6c579b"))) ? ((0) > (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("cba6262c-fd68-3f18-96f4-3ff748e7176a"))) ? ((countOfRegionServers()) >= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("f3a084f4-fe45-38a5-a794-3bb251ea2188"))) ? ((maxToStart) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b54383fd-4428-3934-bab7-1c9c1b29996a"))) ? ((getMinToStart()) >= (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("ef14f0a2-7ec2-3a07-8ff4-9bbc429deb75"))) ? ((countOfRegionServers()) >= (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("4b32c404-9e9f-3eb0-a052-198f9828bbb7"))) ? ((getMinToStart()) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("e8248d81-4b84-3d62-bfd9-5deb90e2415a"))) ? ((countOfRegionServers()) > (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("414d2d4e-c0ad-3afd-b26c-80190f97877c"))) ? ((0) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("6e314865-6d8c-39a3-9635-c079f2794a8f"))) ? ((maxToStart) > (50)) : (((KnobRuntime.check(java.util.UUID.fromString("12b5c165-51f7-3e6c-a722-2c0762a4a453"))) ? ((maxToStart) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f882880a-786a-3aa2-a1c1-a9801d72b0e2"))) ? ((maxToStart) == (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("bb46db10-7211-3c31-84eb-9c09347af79d"))) ? ((50) < (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("5dded1f4-6596-3c6c-a8c1-7ca683c52fe0"))) ? ((getMinToStart()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d27c957d-9e7c-39e0-a3fe-8b030dbe581c"))) ? ((0) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("ce1b51e8-01e8-31eb-9f37-7f994db0e222"))) ? ((countOfRegionServers()) < (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("0f539cfe-7673-3a91-b4c6-7fd833f95728"))) ? ((countOfRegionServers()) == (50)) : (((KnobRuntime.check(java.util.UUID.fromString("3808f7d2-2ac1-35c8-b292-4e94ab7e86b4"))) ? ((4500) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("5a2dcf9e-3440-370a-81d4-7679c2ce9148"))) ? ((1500) != (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("ee83857e-9fe4-370d-a651-6fc2aae4f620"))) ? ((countOfRegionServers()) != (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("cddd4294-f616-351f-a634-042aea0ffd1f"))) ? ((4500) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("608af97f-ec7b-388a-8152-15d52ea2c582"))) ? ((maxToStart) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("53130ed7-8bfc-3bd5-891e-157f17373bbc"))) ? ((getMinToStart()) < (50)) : (((KnobRuntime.check(java.util.UUID.fromString("4e484092-cfe5-37c8-b767-3f85c71f968c"))) ? ((maxToStart) < (50)) : (((KnobRuntime.check(java.util.UUID.fromString("38d30839-b91b-312f-8ce2-dac82834873b"))) ? ((1500) > (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("c9199565-9707-35bb-b59a-9d1bf445a428"))) ? ((countOfRegionServers()) == (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("8f8c7162-7174-3157-9eed-949fcd69ea08"))) ? ((getMinToStart()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("da3e7f7b-11e2-3418-8fae-2954ecf4f48c"))) ? ((4500) == (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("34b5c871-a946-3320-8753-ca55c02c1482"))) ? ((getMinToStart()) <= (50)) : (((KnobRuntime.check(java.util.UUID.fromString("ea97abfe-d700-3fc3-9448-d3a89b2873be"))) ? ((0) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("bed479c2-7fe6-3555-8ac0-01c913eb9e9c"))) ? ((countOfRegionServers()) > (minToStart)) : (((KnobRuntime.check(java.util.UUID.fromString("f06ddb14-365e-3dfc-b9f9-9e9d67ebac65"))) ? ((getMinToStart()) == (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("2f6b3fda-18f9-38f4-93e9-ebbd99fce67c"))) ? ((maxToStart) != (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("b99c0b85-130b-3d16-b1be-91d3e293c15d"))) ? ((4500) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("4247518c-12e3-39ad-a6d4-4c6a0aaf6637"))) ? ((getMinToStart()) != (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("125f4c24-5d59-3721-91d1-29580e94962b"))) ? ((maxToStart) == (countOfRegionServers())) : (((KnobRuntime.check(java.util.UUID.fromString("195703e0-651c-3f5c-b312-8fe377bf9d36"))) ? ((maxToStart) == (50)) : (((KnobRuntime.check(java.util.UUID.fromString("c7e6c023-11b4-32ef-bb37-c2629f37256b"))) ? ((0) >= (countOfRegionServers())) : (maxToStart < minToStart))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
      if (((KnobRuntime.check(java.util.UUID.fromString("bcab6723-4a5d-3ccb-a8f5-e333f7a49570"))) ? (((oldCount) != (0)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a04139c8-0369-3800-8be9-b7d9bc2d6d0f"))) ? (((0) != (getMinToStart())) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("586563f4-9de6-3361-8036-aae2bef3ad07"))) ? (((getMinToStart()) == (countOfRegionServers())) || ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9db2ae46-9c53-3a56-b7d0-5678b243e132"))) ? (((1500) == (countOfRegionServers())) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5a766a7b-e7de-3c12-8f00-1c72488ae995"))) ? (((1500) != (getMinToStart())) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a504b908-5d8f-3580-99b2-c464c0aeb914"))) ? ((50) == (count)) : (((KnobRuntime.check(java.util.UUID.fromString("8aaf1070-e72f-3d02-bb52-2291191e8f6e"))) ? (((oldCount) == (getMinToStart())) && ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3861942b-1d69-3450-9913-56cd7fa73edd"))) ? (((50) != (count)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("37e427db-0143-3496-98fc-e6ae074a071c"))) ? (((countOfRegionServers()) != (countOfRegionServers())) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ac8f7f87-0cd0-361d-99a1-8b69843f0802"))) ? (((getMinToStart()) == (0)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a7466ad1-cd0c-33a2-a1a7-f56003dc08d8"))) ? (((4500) == (getMinToStart())) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bc9f2dff-3b9e-3898-afb3-bbb178d5d8f3"))) ? (((getMinToStart()) != (4500)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4841f330-ad30-35f1-a95e-cfac858b0757"))) ? (((countOfRegionServers()) == (0)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("736b4027-8684-3f93-bc97-163f96440c36"))) ? (((1500) == (getMinToStart())) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("88ef03c5-5d7d-3b12-944b-99d9ddbb0c0d"))) ? (((0) != (getMinToStart())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c3fd0492-c621-3d2e-abcb-30feeda2c03f"))) ? (((oldCount) == (getMinToStart())) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("23525ce7-3a06-307a-949e-1946a42e99e1"))) ? (((oldCount) != (50)) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ae9c5163-7612-3748-a948-d01b2a3babb2"))) ? (((oldCount) != (4500)) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6b0b7300-ee3d-3556-91d9-98c4fc664648"))) ? (((getMinToStart()) == (0)) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f44aa095-7df7-3e72-8819-adf7a94f95c3"))) ? (((4500) == (count)) || ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("75484a6a-fb4a-3610-b15b-92ce38854c3a"))) ? (((0) != (getMinToStart())) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("95fded44-5663-36c4-bcaa-fef3210361f0"))) ? (((oldCount) == (countOfRegionServers())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("27d84fc7-672b-30f7-a469-baf78174f187"))) ? (((getMinToStart()) != (1500)) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("95c22e44-1f67-3403-90ca-5a2b3c163ee1"))) ? (((oldCount) == (0)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("597a31bd-b6d1-3074-abc4-ecf04006f0d9"))) ? (((countOfRegionServers()) != (4500)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6ee92c4f-2e42-36a6-a6a7-f6c58bffbd5c"))) ? (((oldCount) == (count)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("147617b2-1b5c-38d2-9211-fe3a9cf1a2a1"))) ? (((getMinToStart()) != (getMinToStart())) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("63f69838-3f61-3168-8bfa-28e34aa43b16"))) ? (((getMinToStart()) != (countOfRegionServers())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("b659df62-680f-3c2c-bf44-bfcfb0b9e4a7"))) ? (((1500) != (countOfRegionServers())) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("970cdb7c-7272-30ac-a3a6-054438a0537a"))) ? (((4500) != (countOfRegionServers())) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f7ac7d7c-de18-3426-aa07-1533f29ec03b"))) ? (((countOfRegionServers()) != (getMinToStart())) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5bb70472-e44a-35fa-ba87-bdf36b5556b5"))) ? (((oldCount) != (count)) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2bce75a1-3fe7-3609-b8ac-bbb7e94a7f58"))) ? (((50) != (countOfRegionServers())) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e21fc522-0a4b-3f65-93c8-7089a516d7c1"))) ? (((1500) != (getMinToStart())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8c9f6cc5-6e12-372f-9b7a-063c2043bbf8"))) ? (((getMinToStart()) == (1500)) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("993a7d1d-5c61-334c-9898-1089c7363ccd"))) ? (((countOfRegionServers()) != (count)) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6ed6dd20-70cd-3738-90c4-81f5bd2b2de3"))) ? (((countOfRegionServers()) != (countOfRegionServers())) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7932342d-0c7d-3795-8570-22eff03e4fa1"))) ? (((4500) != (count)) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8d41ec1f-19dd-3b12-a7a7-b2dec9e0aafb"))) ? (((1500) != (countOfRegionServers())) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8ef7ef3c-4dae-3ba2-b560-54cc6d9e9451"))) ? (((0) != (getMinToStart())) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bdf50953-80fa-3b1e-8ba2-d8757781a585"))) ? (((0) == (getMinToStart())) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("df4f905d-6520-3ad6-8ddf-a3825fcd3664"))) ? (((oldCount) != (1500)) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("667f04ed-01b7-3c4f-b353-fa026e890dbf"))) ? (((countOfRegionServers()) == (4500)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8b92b24e-aca7-3f09-ad36-ede6886ab8bf"))) ? (((1500) == (countOfRegionServers())) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0c9c0ab6-5072-3fab-98da-7327511847f4"))) ? (((countOfRegionServers()) != (getMinToStart())) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7757353f-5a93-3e4b-97de-523e0f2b8d51"))) ? (((getMinToStart()) != (getMinToStart())) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("29876cce-36bb-3324-8f6e-52497ebdf368"))) ? (((50) == (getMinToStart())) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cc1eeb90-4ec4-3f9a-b3cb-e0c690889417"))) ? (((oldCount) == (getMinToStart())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("353577b1-daf0-3679-b295-8f3d0ffdd3c2"))) ? (((getMinToStart()) != (1500)) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("37097809-e47a-3cf7-a428-9ec6063a5bfe"))) ? (((getMinToStart()) != (countOfRegionServers())) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a182b65c-59af-331b-8dfa-cccd3bc34459"))) ? (((oldCount) != (1500)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("41fae2a1-8b66-3d50-a4ae-3b6384dc0996"))) ? (((50) == (count)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("df006bbd-b04f-34cb-90d1-34c95a7e0e1a"))) ? (((getMinToStart()) == (4500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("59f52935-3f5a-3580-9673-27ab82f7adb1"))) ? (((countOfRegionServers()) == (1500)) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8f506bbc-f05f-30e6-a74c-8f6a0f35d10f"))) ? (((countOfRegionServers()) != (50)) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bf706d19-3dbc-3ef0-872f-d9c49e05b919"))) ? (((getMinToStart()) == (getMinToStart())) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("06822d49-91f1-3683-bdd6-ca9e9060e6b4"))) ? (((countOfRegionServers()) != (count)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5dbec93e-2220-38ea-95d4-1aa6c9eb9a70"))) ? (((50) != (count)) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("1c45b1fa-1da1-3544-97f6-777b8c3b08dc"))) ? (((getMinToStart()) != (countOfRegionServers())) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e8ef3ddc-d36e-3026-aced-4bd7de6d94da"))) ? (((4500) != (getMinToStart())) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("37a8ab9c-bfde-3c6b-8bcd-d649bcc42e10"))) ? (((4500) != (getMinToStart())) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7c0f7537-f4c9-3fba-a944-de36980f87e8"))) ? (((50) == (count)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("121ad69e-8b07-3d8d-94b2-60f45952c24c"))) ? (((countOfRegionServers()) != (50)) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bd63e0cf-00ac-36a6-bd44-c8e43550a845"))) ? ((4500) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("dd1a0e73-004f-360f-a6a0-89a49eb33ebc"))) ? (((0) == (countOfRegionServers())) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("dac54d45-da19-37e5-8997-3281c52e76e2"))) ? (((countOfRegionServers()) == (0)) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("51566af7-2237-37fc-8c99-2e8f49a3b4c6"))) ? (((1500) == (getMinToStart())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3959f148-ba1c-3bd1-b24f-9284ad4e3b6c"))) ? (((0) != (count)) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5e0be33d-6e34-3ff2-8add-ccacc620d1b2"))) ? (((oldCount) != (countOfRegionServers())) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("86ca5814-9d31-3fe8-8d78-e3d60944c4b9"))) ? (((50) == (countOfRegionServers())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6150c033-ed2f-3765-a333-5cbc435b8d4d"))) ? (((getMinToStart()) == (getMinToStart())) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fe5f19a8-b408-3f0e-99fe-bb6afbcf4ae1"))) ? (((50) == (countOfRegionServers())) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f8904a41-2cd3-3d61-8dfe-4094c1b90f96"))) ? (((getMinToStart()) != (count)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("aa65180a-39c6-3e7e-9959-1e8b59266c60"))) ? (((getMinToStart()) == (4500)) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e2b5ccb8-8ba8-3bfc-a3b2-73cace99674d"))) ? ((oldCount != count) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d6517969-2296-3e95-9097-b3e774266976"))) ? (((4500) == (getMinToStart())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b71726b4-d266-3694-8ca5-bb46003f14bf"))) ? (((countOfRegionServers()) == (getMinToStart())) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c2cbcfa7-79c3-3c59-8616-3fac1def5cdd"))) ? (((countOfRegionServers()) != (4500)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3f311436-fe9f-3488-aae0-6efa37b3b805"))) ? (((getMinToStart()) != (1500)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5713527c-229e-3882-a5ca-bb8e64373f32"))) ? (((0) != (count)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cb0aa295-e5b2-3dad-a57e-633caf57bb11"))) ? (((countOfRegionServers()) == (1500)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cc3fa6df-0e74-3d32-9694-00ea6511b172"))) ? (((50) != (getMinToStart())) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a6600b01-3e6b-335b-aff4-439aa13bc822"))) ? (((0) == (countOfRegionServers())) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("193d4b63-a23f-34ea-bdde-ba71c01a8566"))) ? (((0) != (count)) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cab45b15-6689-3145-9947-3968d99a04d8"))) ? (((0) == (getMinToStart())) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("52c62c49-7302-38a3-b558-5d6e84dc2073"))) ? (((getMinToStart()) == (1500)) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e57b5055-5460-311d-b436-f11c596af0a4"))) ? (((oldCount) != (50)) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8d318df0-6807-3654-9195-88363beb1fda"))) ? (((oldCount) != (1500)) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6ff3f58b-e636-3791-b1d8-626892cab728"))) ? (((countOfRegionServers()) != (countOfRegionServers())) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4126ff45-cdce-339b-b90d-7eae15260e5d"))) ? (((countOfRegionServers()) == (50)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3d3b65b8-fe88-3bec-b469-b2e824be5722"))) ? (((50) == (countOfRegionServers())) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a1596e2f-328b-34e6-8256-dbee870015f1"))) ? (((0) == (countOfRegionServers())) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cdbd8305-8553-336b-9ee2-6a6a0db216a9"))) ? (((getMinToStart()) != (count)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2d60a676-09a1-336f-9e76-1500b4123a0f"))) ? ((getMinToStart()) != (1500)) : (((KnobRuntime.check(java.util.UUID.fromString("c036bae8-88a5-368f-8c51-9b11b01e189c"))) ? (((oldCount) == (getMinToStart())) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ab82dde0-dd7a-3349-9c75-eea0358ecca0"))) ? (((getMinToStart()) != (0)) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b311268d-ce36-34fa-9b0f-a43305438d3f"))) ? (((getMinToStart()) != (50)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0d465a29-806e-3d98-aa26-f364dcc8792f"))) ? (((1500) != (getMinToStart())) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f575338a-51bc-3c37-b598-829e3303d1fb"))) ? (((4500) == (countOfRegionServers())) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e0f34654-9bab-35e6-9bb9-5db2ad989e1e"))) ? (((50) == (count)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5c99591a-c3d6-3ffa-96f0-e4bb3410bef6"))) ? (((getMinToStart()) != (1500)) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d7d71e5b-d430-3f2c-9399-1a6959997ab1"))) ? (((50) == (count)) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5dde5680-987e-3a2d-a27d-0771bd984f37"))) ? (((oldCount) == (countOfRegionServers())) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ecc45763-94b3-3fe6-aa2c-40eda960284c"))) ? (((getMinToStart()) != (50)) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c6b62126-7c3e-368d-973d-5aa1dec2d245"))) ? ((oldCount != count) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7ae20d49-47ac-3437-8575-158ffe2da09e"))) ? (((oldCount) == (countOfRegionServers())) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("da49226f-d7bd-3146-940e-38c310e71dc9"))) ? ((countOfRegionServers()) != (4500)) : (((KnobRuntime.check(java.util.UUID.fromString("b15fb5b6-bc42-3a8f-be9a-d6ae18851754"))) ? (((countOfRegionServers()) == (getMinToStart())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ae838dd8-3760-3779-ac64-1fb36eced9c1"))) ? (((4500) != (countOfRegionServers())) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("aa43787f-f2d2-390b-8d92-bc70015b9b33"))) ? (((oldCount) != (0)) || ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6c4c7477-8826-351a-8e11-d826d96fcb28"))) ? ((lastLogTime) < (now)) : (((KnobRuntime.check(java.util.UUID.fromString("4469a48d-58a5-3964-80e2-b420b232f33f"))) ? (((oldCount) != (50)) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("501a4ada-5f18-3a59-bb49-6ec150752dc8"))) ? (((countOfRegionServers()) != (getMinToStart())) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bf199f02-e6ad-32d5-85e7-e41bc24c08ad"))) ? (((oldCount) == (50)) && (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("39839981-4a05-3197-91a6-6eaf4c2d0606"))) ? (((oldCount) == (50)) && ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e328a8db-b07a-3abe-8887-e8d7d807970c"))) ? (((50) == (countOfRegionServers())) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f065980d-b736-3c5c-836f-da9e59c83f5e"))) ? (((countOfRegionServers()) == (countOfRegionServers())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("60e99b02-c319-385e-b8c2-47e8bf9ec68a"))) ? (((1500) != (count)) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6e713211-0d8e-33ba-bee4-a9bd58f09137"))) ? (((oldCount) != (1500)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a98ec0fb-444d-3c6e-924c-67be9fa1e16d"))) ? (((50) != (getMinToStart())) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ee9a96f3-a88f-33b3-8339-f9acdadcf626"))) ? (((4500) == (getMinToStart())) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8a3aa361-92a8-3da2-827e-d7155de28f11"))) ? (((1500) != (count)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7c66e25e-e4de-3aac-9cb1-de089b578833"))) ? (((countOfRegionServers()) == (getMinToStart())) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("af271a8b-faae-3c59-b11b-3cadeebfcc52"))) ? (((countOfRegionServers()) != (0)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e4521448-a431-37c8-9d49-c68198a4fb71"))) ? (((0) == (getMinToStart())) || ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f2a8c023-3250-35dc-a350-ec00a06a5207"))) ? (((getMinToStart()) != (0)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6550c931-ee37-3a82-8771-634bebb200a7"))) ? (((oldCount) == (count)) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d1ee10bd-c6b0-34b8-b838-1a77575dbb92"))) ? (((oldCount) != (count)) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4f10a9a6-85f6-31e6-8162-e7006eb69900"))) ? (((countOfRegionServers()) == (4500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("199de33a-6a24-3114-be12-9ba0bfa5bfaa"))) ? (((4500) == (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("d800182c-3327-3369-bba3-fb9946c9b62e"))) ? (((getMinToStart()) == (1500)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("dbf3b812-e6a8-3431-829e-d2ab5920c8b0"))) ? (((getMinToStart()) == (count)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5aa2f3f0-276f-3ae0-b1e3-5be38abc3a0a"))) ? (((0) == (getMinToStart())) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("342e6d45-364a-3b96-8022-1ddd457a31d0"))) ? (((4500) == (countOfRegionServers())) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("631305bc-a578-364f-8ff3-a9441baa0294"))) ? (((getMinToStart()) == (0)) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6310e4ac-6bd1-3f05-9fa8-d4b480692b0e"))) ? (((countOfRegionServers()) != (count)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9e639943-022c-3878-9ab2-6868395c902b"))) ? (((getMinToStart()) != (0)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f3bed5de-82d5-3593-9440-5359d7cc1073"))) ? (((countOfRegionServers()) == (count)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d87664de-9ba5-365d-b316-e3bd506fab82"))) ? (((countOfRegionServers()) != (getMinToStart())) || ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("daedcf1e-9732-39c1-8122-379dde13149a"))) ? (((countOfRegionServers()) == (1500)) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a9472595-dcd0-3a52-94aa-232c0c4dab0c"))) ? (((countOfRegionServers()) != (4500)) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("72045509-9eb5-3578-814f-78fbdd984887"))) ? (((oldCount) == (0)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("98001f84-4b30-37d4-81a5-cb597ff12b18"))) ? (((oldCount) == (count)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3b535c5a-e760-3853-a114-0baef13d5bd7"))) ? (((getMinToStart()) != (getMinToStart())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("d91ce00c-3d52-3195-a245-be7cdfa08164"))) ? (((countOfRegionServers()) != (getMinToStart())) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cf73c271-13ba-322f-92b3-16511476b435"))) ? (((getMinToStart()) == (1500)) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("348b2c5d-18e3-341f-b2ea-d70b6e5c1d2a"))) ? ((getMinToStart()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ce253fbd-48fd-3f91-bef6-e41c8054edf4"))) ? (((getMinToStart()) != (countOfRegionServers())) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b7b72a26-dc73-3f81-993a-bbf59c005cc7"))) ? (((50) == (getMinToStart())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("627ef25b-cccc-34b0-a879-aea99bbc1c98"))) ? (((oldCount) == (getMinToStart())) || ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("28d363ea-89d4-3848-a295-ff9879529e26"))) ? (((getMinToStart()) == (4500)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b19279e6-8e25-3c7f-8161-13bd6bfb395d"))) ? (((countOfRegionServers()) == (countOfRegionServers())) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("07332e68-45ae-3b9f-9a5c-51b541f30672"))) ? (((50) != (count)) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0f267c35-bce1-3c3b-be90-09232f329491"))) ? (((getMinToStart()) == (1500)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("65c089ce-2acc-3029-8344-bfcb9dec91de"))) ? (((getMinToStart()) == (count)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("be803b8f-6d5e-32c0-817c-86afbdaf9f70"))) ? (((oldCount) != (4500)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7398050d-6274-3855-9c79-9c903ceb7a0b"))) ? (((countOfRegionServers()) != (count)) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8af6f9e1-8b40-36f0-93a3-d451f1089297"))) ? (((0) == (getMinToStart())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fa3e47f9-caa7-31f0-9a07-0ad8c6a8edf0"))) ? (((oldCount) != (countOfRegionServers())) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c2f26cfe-8963-3933-9cdd-250e36a79cda"))) ? ((oldCount != count) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("deaa84b3-6404-3142-8e33-09cf500ae90d"))) ? (((4500) != (countOfRegionServers())) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("db0f7fa8-cfef-3e7a-9dab-d9dd88ba0235"))) ? (((4500) != (getMinToStart())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a7a70ee9-9053-3c6e-8e9c-ae3fbfc82016"))) ? (((countOfRegionServers()) == (getMinToStart())) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("466c2a22-33f4-373d-90aa-809cc93f2d88"))) ? (((countOfRegionServers()) == (getMinToStart())) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("83b571c2-97c8-36ea-9bef-808b7bf8ca60"))) ? (((50) == (getMinToStart())) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c23dd305-65f8-3f1d-8f54-049d12f1763f"))) ? (((0) != (count)) || ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1205a78e-c5d3-3ded-8795-43a049182d69"))) ? (((getMinToStart()) != (1500)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c0d1fbd8-d786-37a5-bb81-922cca6e527d"))) ? (((countOfRegionServers()) != (50)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("185bba68-303b-372d-be1a-d86e8dadbe84"))) ? (((countOfRegionServers()) != (count)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("378fc6c4-36fa-3eaa-b75c-bd6d1e665c83"))) ? (((oldCount) != (1500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c31a0114-c63b-34de-8a22-4bd114a704d5"))) ? (((oldCount) == (count)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("002f7a3f-9023-334d-890b-cb3503daaa9d"))) ? (((0) == (countOfRegionServers())) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5d43c0e5-9090-3054-83bd-0e29836a5cdd"))) ? (((getMinToStart()) != (50)) || ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fabca0a9-972d-34c1-a473-4d9d50e9f013"))) ? (((1500) == (count)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("5d1ab7fb-f0a6-31d2-869d-745107147560"))) ? (((countOfRegionServers()) == (countOfRegionServers())) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7d8423f4-5c57-34e1-896c-9b8dda2a07df"))) ? (((4500) == (count)) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8f81de17-4f29-313d-93d5-b7e3ea6c40bd"))) ? (((50) != (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("110e7a23-fbe5-32cd-9f85-13f6d47e6ea5"))) ? (((4500) != (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("9425f29d-9210-3d52-b76f-13219637940f"))) ? (((getMinToStart()) != (50)) || ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("54f9f421-5a7a-3829-9cb7-201b68d6cd22"))) ? (((countOfRegionServers()) != (1500)) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ba8ba898-986f-3f7f-8bdc-23dcecd448af"))) ? (((countOfRegionServers()) == (1500)) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f0501a04-f91b-33c8-a0c1-fbd9dad7bd17"))) ? (((countOfRegionServers()) == (getMinToStart())) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0243fede-c708-32ef-9e47-e79646987597"))) ? (((oldCount) == (0)) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1dc1e0f0-86a0-3f4a-8673-3073873b45cf"))) ? (((getMinToStart()) == (getMinToStart())) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cfa84c1f-67b1-3b77-b20a-514f9a822cf8"))) ? (((countOfRegionServers()) == (getMinToStart())) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("00760bad-25f5-3e89-8da6-c70355b10ea0"))) ? (((countOfRegionServers()) == (0)) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("10fb9961-6b6d-3360-abfe-68076bcfa4ff"))) ? (((countOfRegionServers()) != (4500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2d54d943-d0cf-38c4-ae24-fb1a8c472687"))) ? (((getMinToStart()) != (count)) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6de555c7-0f29-3032-ac01-80bb3387f2b7"))) ? ((oldCount != count) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c10d7d46-cd23-3b52-9689-1a36dc8ca088"))) ? (((50) == (count)) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6c24049b-a2ce-3621-a56a-97b8fd48986c"))) ? (((getMinToStart()) != (1500)) || ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2ee9a06d-64a6-34c7-bfa9-ffd7bab46438"))) ? (((50) != (getMinToStart())) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("39d9da35-3d7c-3ff0-ae9c-627f764b0e79"))) ? ((50) != (count)) : (((KnobRuntime.check(java.util.UUID.fromString("b5938b9b-feff-3c92-bdf3-b42c4f9431a1"))) ? (((oldCount) == (4500)) || ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cedd48a3-b3d5-3b94-8fd4-b447b1d7243d"))) ? (((countOfRegionServers()) != (countOfRegionServers())) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7ad4cd59-b810-31a8-97ce-7d48f3af324e"))) ? (((getMinToStart()) == (count)) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f9e226c2-121e-3834-8b47-d5d669644a27"))) ? (((oldCount) != (count)) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("17cbe904-e9e2-3ebd-9bde-fbed90528be8"))) ? (((1500) != (countOfRegionServers())) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cb488529-735d-377a-a866-113f8480d97a"))) ? (((oldCount) != (50)) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c685fb80-c47d-34cb-be0a-559b90cc96aa"))) ? (((oldCount) != (4500)) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7e888410-1e23-33cb-be50-79815791a810"))) ? (((getMinToStart()) == (4500)) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("eba550d2-0bbf-3585-978d-1324b6511c61"))) ? (((1500) == (countOfRegionServers())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("27e694ab-3f97-33b6-a189-fdf9abe0584d"))) ? (((4500) == (getMinToStart())) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3898f5ac-920c-3401-b1a9-e3a6e2c230f2"))) ? (((getMinToStart()) == (countOfRegionServers())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("8b38034c-25d0-383b-8a37-cf7d7a6b38f3"))) ? ((getMinToStart()) == (getMinToStart())) : (((KnobRuntime.check(java.util.UUID.fromString("23f1cd13-d918-337f-a3dc-8fb70453341e"))) ? (((countOfRegionServers()) != (countOfRegionServers())) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("ee290711-bc36-39ca-abd1-8febd7bc4cc0"))) ? (((50) == (count)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2acf16c5-de01-3e6e-b372-ac21ab0b6dbc"))) ? (((1500) == (countOfRegionServers())) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("acb06d1a-13ba-35a1-aea9-eda7307a84f4"))) ? (((1500) == (count)) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ffeceb38-c3bc-3893-bf90-bc93c2b5314a"))) ? (((oldCount) != (countOfRegionServers())) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4a6d0e30-fc14-3b3e-8701-68de0c797ea1"))) ? (((getMinToStart()) != (50)) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("4441fe9d-548b-35e0-bc8f-453ec39fce05"))) ? (((50) != (countOfRegionServers())) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("aaf3ba3d-fe94-3b53-ab08-927b2029d3f9"))) ? (((getMinToStart()) == (50)) || (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9c8a7f0b-b628-3a16-bf62-049d8856649b"))) ? (((getMinToStart()) == (50)) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("10dc5448-b93e-32b7-bf20-42c378fa79ab"))) ? (((getMinToStart()) != (4500)) || ((lastLogTime) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b5f9f22f-8b55-3e7c-a63e-9469b45c7e62"))) ? (((0) == (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("9a2c2b6c-a19b-370b-88ab-e23035b09cae"))) ? (((oldCount) != (1500)) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("a961d401-2969-3225-86fc-8d478cf27539"))) ? (((getMinToStart()) != (0)) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f5a639a7-79cb-3098-a100-072be77f7fb3"))) ? (((50) != (getMinToStart())) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("34865bb0-daab-341e-b8e3-68cb3b04e6de"))) ? (((getMinToStart()) != (1500)) || ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b90c6139-2504-3cfe-8ad1-6ea3d72f7d42"))) ? (((1500) == (count)) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("07802c74-ffcf-3be3-aea6-b03d9bc851ac"))) ? (((oldCount) == (count)) || ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2ffe8fc2-1c8b-39de-94c9-38b9726f4579"))) ? (((countOfRegionServers()) != (4500)) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cf064fd5-0656-3b24-a6b2-d20f972e8037"))) ? (((getMinToStart()) == (getMinToStart())) && ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("df17f36b-6d07-39c1-9956-52771397f6b3"))) ? (((50) != (getMinToStart())) && (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e344c4bd-6615-3b74-a974-50cb63eba8b5"))) ? (((getMinToStart()) == (getMinToStart())) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9542bd31-ddb8-359a-9cbf-92cca8aac5ea"))) ? (((0) == (countOfRegionServers())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("637b64ba-e4ed-3ab4-a126-5c9c5f75289f"))) ? (((1500) == (getMinToStart())) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f005082b-ade4-3033-8df3-3203c4230930"))) ? (((oldCount) != (count)) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d8e1b479-5644-3586-b91f-26f0ba17130c"))) ? (((countOfRegionServers()) != (4500)) && ((lastLogTime) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7cbe2e14-573b-3791-87d3-c27573858319"))) ? (((countOfRegionServers()) != (getMinToStart())) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("be3770fc-2e87-3a80-bb1c-686dfba482ae"))) ? (((50) != (count)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("0aa2dbf2-0daf-3f29-a425-9b59b6d27dd2"))) ? (((countOfRegionServers()) != (50)) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("1c94d0ff-23ed-342d-b6e7-7bb9a1d42d3c"))) ? (((50) == (count)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4e6a07c7-0d46-33e6-8545-454d9ae797e3"))) ? (((50) == (getMinToStart())) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7c2ff82d-b715-3aaa-8146-6bc84b2c7ae5"))) ? (((50) == (countOfRegionServers())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6167e009-85c2-3a7f-a905-39d06a1396a3"))) ? (((oldCount) == (getMinToStart())) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ccf4e301-35ad-3f96-8b66-b777d4d6904d"))) ? (((getMinToStart()) != (4500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e8356b5b-8d47-32e1-a0ab-fe36ef136143"))) ? (((countOfRegionServers()) == (count)) && ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("2d07d7b7-62d1-3cc5-9c7c-eac79987cab5"))) ? (((4500) != (getMinToStart())) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("27aa6179-bf0f-3812-a20c-201d3a4dc91a"))) ? (((0) != (getMinToStart())) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("50eee0a9-1ba7-38a2-8f1a-1129919bf10a"))) ? (((getMinToStart()) != (50)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("45f78b1d-73d2-3bcf-92ea-355ebfa0d1a0"))) ? (((50) == (getMinToStart())) || ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f77ddb40-b329-3d07-904b-3b5b4a515f7a"))) ? (((oldCount) == (getMinToStart())) || (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("83b1c08a-07b2-38a3-b1af-18ed1c001ad6"))) ? (((4500) != (getMinToStart())) && ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e42e69df-a693-326e-bbd4-c2f4f49a37c0"))) ? (((0) != (countOfRegionServers())) && ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("413f4903-30dd-3733-b191-6e8dd6c69410"))) ? (((0) == (countOfRegionServers())) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("85a8876d-969a-3109-b161-99bcc109717d"))) ? (((getMinToStart()) == (countOfRegionServers())) && ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("eed5f0ba-873f-3d50-a363-df534d970bd4"))) ? (((getMinToStart()) != (50)) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f15a4f81-2572-3b8d-95c4-38cf5d6c5bc5"))) ? (((4500) != (getMinToStart())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8439a894-dc37-3910-b235-f377cc13bbc7"))) ? (((countOfRegionServers()) == (4500)) && (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("57a4193d-b3d8-3fbc-9924-0502ce51e373"))) ? (((oldCount) != (50)) || ((lastLogTime + interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fb806780-8a7d-3655-b830-7a41da868aa5"))) ? (((oldCount) != (4500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("7cf7902d-4de6-3c97-9b6a-67040ce21ffa"))) ? (((4500) != (countOfRegionServers())) || ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("40987fad-9181-34e9-923a-8bf2de6e7c2a"))) ? (((countOfRegionServers()) == (50)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4375d332-7178-3997-b7b2-ed270bb5280b"))) ? (((getMinToStart()) == (count)) || (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("533e6082-5bcd-3214-9c2a-ce677e2f450f"))) ? (((0) != (getMinToStart())) && ((lastLogTime) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c2dfa023-ce77-3387-9508-76c205ed96c2"))) ? (((oldCount) == (1500)) && (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("92ad317d-ca2d-3958-bcec-a62d9e48c1e5"))) ? (((oldCount) != (countOfRegionServers())) && (((lastLogTime) + (interval)) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3cedb007-69fc-36e9-820d-4813bd63c2a1"))) ? (((4500) != (getMinToStart())) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("50034ff0-eb0b-3f9c-b7db-bcfd8c0edfd2"))) ? (((50) == (getMinToStart())) || (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("db279fee-8729-305b-835e-5a06e40f68c6"))) ? (((countOfRegionServers()) == (50)) || ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("95101a72-fed2-342d-8ca5-002a47df9009"))) ? (((countOfRegionServers()) != (0)) && (lastLogTime + interval < now)) : (((KnobRuntime.check(java.util.UUID.fromString("2500efb9-045a-37a6-b997-627dbb86f513"))) ? (((oldCount) != (50)) || (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("14e5d775-e7b0-3f9c-8b6c-bf526bff603a"))) ? (((1500) == (count)) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6a920872-46d6-39b4-8ddc-3ec858ca6865"))) ? (((countOfRegionServers()) == (getMinToStart())) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ddee272f-46a6-31c7-bd60-875cfd256c75"))) ? (((countOfRegionServers()) == (50)) && ((lastLogTime + interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("58d2b87e-5e8c-3fb8-9e4a-98635d52c516"))) ? (((1500) == (count)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d4e5b08b-153a-3991-84a9-67b28f2ea869"))) ? (((countOfRegionServers()) != (0)) && ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("38a4c21f-c056-3e85-ac6f-77b921426e63"))) ? (((4500) != (getMinToStart())) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9fe5d4e4-a125-3a77-8a81-fdb6232fdd96"))) ? (((getMinToStart()) == (1500)) && ((lastLogTime + interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6cfddb7b-63e6-3a5b-a5c1-3d6f556f4b10"))) ? (((countOfRegionServers()) == (50)) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("586e85c2-dcac-3b8c-8818-5a1d2ec1b08a"))) ? (((oldCount) == (1500)) && ((interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("43dfced2-ec5d-3f0f-9ae8-c0d35264417c"))) ? (((oldCount) != (countOfRegionServers())) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("beae0306-e323-3999-ace3-e86742ab0801"))) ? (((countOfRegionServers()) != (1500)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("b7a59578-a53a-3810-b234-2cdb128c9b68"))) ? (((50) != (count)) && ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("cc429495-f4f6-37f5-b6d8-e079a49a2f4c"))) ? (((getMinToStart()) == (count)) && ((interval) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("9dcfb32d-a35a-31b0-b8c2-17efc5f155e1"))) ? (((0) == (getMinToStart())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("91e3301e-70e6-3502-bd74-2f5c04a6a00b"))) ? (((oldCount) != (1500)) || ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("3540c97e-7b28-3bd6-8afd-f8cf0bf09179"))) ? (((getMinToStart()) != (1500)) || ((lastLogTime + interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("6d2dc752-db78-3af5-9e23-7e311b4cf7c8"))) ? (((1500) == (count)) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e6ed8b21-621d-3cf8-84ee-97c62c6d8e8e"))) ? (((countOfRegionServers()) == (50)) && ((interval) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("090fb170-0577-3034-b97b-2e3f09c2e164"))) ? (((countOfRegionServers()) == (4500)) || ((lastLogTime) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ee0a9c8e-4e8c-3ae8-8a2c-6d73bdb8b009"))) ? (((50) == (countOfRegionServers())) && ((lastLogTime + interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("45822415-b72e-3a9b-a179-844ffeac1a26"))) ? (((getMinToStart()) != (50)) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("181704e6-7fba-3081-9cc7-a0a9850a7f17"))) ? (((oldCount) != (4500)) && ((interval) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("e8528b3f-d886-39b2-9e42-cbf6899ec98f"))) ? (((0) == (count)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("bb09b046-f9a9-3042-b350-48dabd6d7b95"))) ? (((oldCount) == (count)) && ((lastLogTime) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("d89e0a28-1db4-3d6b-8b49-7356b25dbac1"))) ? (((countOfRegionServers()) != (1500)) || (((lastLogTime) + (interval)) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("dc479046-e210-3510-aad1-7d274e3592cd"))) ? (((1500) == (count)) && (((lastLogTime) + (interval)) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fc310c4c-86e0-346c-9d77-67e6d11f0a55"))) ? (((1500) != (countOfRegionServers())) && (((lastLogTime) + (interval)) >= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("505fc044-3092-3ef3-ae8a-3dd71c9ee2a1"))) ? (((countOfRegionServers()) == (countOfRegionServers())) || ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("f9be4409-0c35-37ad-a6d6-74a4ff4d5ad4"))) ? (((oldCount) != (0)) || ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("fc664291-736e-33f8-b584-4d34e33dfd6c"))) ? (((countOfRegionServers()) != (1500)) && (((lastLogTime) + (interval)) <= (now))) : (((KnobRuntime.check(java.util.UUID.fromString("07b37f5d-93b4-3740-9ff6-20a1afb94994"))) ? (((4500) == (count)) && ((interval) < (now))) : (((KnobRuntime.check(java.util.UUID.fromString("ae0ecf25-0b4e-3fc0-8975-c13cb551b9c5"))) ? (((0) == (getMinToStart())) || ((interval) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8510bb1a-da1c-3528-adb0-b032d0674f36"))) ? (((getMinToStart()) != (4500)) || ((lastLogTime) != (now))) : (((KnobRuntime.check(java.util.UUID.fromString("8c9564bd-9f03-35db-94c9-6f849a1b73f1"))) ? (((countOfRegionServers()) != (countOfRegionServers())) && ((lastLogTime + interval) > (now))) : (((KnobRuntime.check(java.util.UUID.fromString("c70bcaf1-fffa-31a9-bb66-63ea2d956344"))) ? (((0) == (getMinToStart())) || (((lastLogTime) + (interval)) == (now))) : (((KnobRuntime.check(java.util.UUID.fromString("4b7494e4-3952-324c-a32a-53f3d516762a"))) ? (((oldCount) == (0)) || ((lastLogTime) <= (now))) : (oldCount != count || lastLogTime + interval < now))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fa16dae2-8f99-3bfb-be1e-0cc359d82a11"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("da2df126-49c0-3c64-b9ae-9b03b686ce5c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("80c1404b-048e-3afd-a1d8-81355d6cdeb1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("982850ee-65ba-3550-8150-8a3d88818a08"))) {
return;
}
    String statusStr = "Cluster shutdown requested of master=" + this.master.getServerName();
    if (KnobRuntime.check(java.util.UUID.fromString("ffab7deb-7bfd-3704-a3e4-d977c178efda"))) { LOG.warn(statusStr); } else if (KnobRuntime.check(java.util.UUID.fromString("31f5f2ce-4434-31d2-925b-78feb2240213"))) { LOG.trace(statusStr); } else { LOG.info(statusStr); }
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

    if (((KnobRuntime.check(java.util.UUID.fromString("c47e2faa-e307-3934-ac29-17b75deccc0f"))) ? ((serversToExclude) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("dc271d18-16e7-3811-9440-761cd29679b5"))) ? ((getDrainingServersList()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("95f95334-1a17-3e2b-99f3-676fa7f03f4f"))) ? ((serversToExclude) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fa7a8f53-98fb-32a1-918e-4505479315b0"))) ? ((getDrainingServersList()) == (null)) : (serversToExclude != null))))))))) {
      destServers.removeAll(serversToExclude);
    }

    // Loop through the draining server list and remove them from the server list
    final List<ServerName> drainingServersCopy = getDrainingServersList();
    if (KnobRuntime.check(java.util.UUID.fromString("28a81311-5070-3143-8d24-386939520103"))) { destServers.removeAll(getDrainingServersList()); } else { destServers.removeAll(drainingServersCopy); }

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
    if (KnobRuntime.check(java.util.UUID.fromString("20f0218e-f9e6-38c6-bcd4-a20d8d652b07"))) { storeFlushedSequenceIdsByRegion.get(encodedName); } else { storeFlushedSequenceIdsByRegion.remove(encodedName); }
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
if(KnobRuntime.check(java.util.UUID.fromString("0622f528-b4a2-3b10-b151-686f8c34bcd2"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("089aff53-e4ce-3162-9846-bdb9eca39630"))) {
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
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getInfoServerPort() : 0;
  }
}
