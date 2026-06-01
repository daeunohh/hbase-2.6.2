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
package org.apache.hadoop.hbase.master.replication;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Manages and performs all replication admin operations.
 * <p>
 * Used to add/remove a replication peer.
 * <p>
 * Implement {@link ConfigurationObserver} mainly for recreating {@link ReplicationPeerStorage}, for
 * supporting migrating across different replication peer storages without restarting master.
 */
@InterfaceAudience.Private
public class ReplicationPeerManager implements ConfigurationObserver {

  private volatile ReplicationPeerStorage peerStorage;

  private final ReplicationQueueStorage queueStorage;

  private final ConcurrentMap<String, ReplicationPeerDescription> peers;

  private final String clusterId;

  private volatile Configuration conf;

  // for dynamic recreating ReplicationPeerStorage.
  private final FileSystem fs;

  private final ZKWatcher zk;

  ReplicationPeerManager(FileSystem fs, ZKWatcher zk, ReplicationPeerStorage peerStorage,
    ReplicationQueueStorage queueStorage, ConcurrentMap<String, ReplicationPeerDescription> peers,
    Configuration conf, String clusterId) {
    this.fs = fs;
    this.zk = zk;
    this.peerStorage = peerStorage;
    this.queueStorage = queueStorage;
    this.peers = peers;
    this.conf = conf;
    this.clusterId = clusterId;
  }

  private void checkQueuesDeleted(String peerId)
    throws ReplicationException, DoNotRetryIOException {
    for (ServerName replicator : queueStorage.getListOfReplicators()) {
      List<String> queueIds = queueStorage.getAllQueues(replicator);
      for (String queueId : queueIds) {
        ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
        if (queueInfo.getPeerId().equals(peerId)) {
          throw new DoNotRetryIOException("undeleted queue for peerId: " + peerId + ", replicator: "
            + replicator + ", queueId: " + queueId);
        }
      }
    }
    if (queueStorage.getAllPeersFromHFileRefsQueue().contains(peerId)) {
      throw new DoNotRetryIOException("Undeleted queue for peer " + peerId + " in hfile-refs");
    }
  }

  void preAddPeer(String peerId, ReplicationPeerConfig peerConfig)
    throws DoNotRetryIOException, ReplicationException {
    if (peerId.contains("-")) {
      throw new DoNotRetryIOException("Found invalid peer name: " + peerId);
    }
    checkPeerConfig(peerConfig);
    if (peers.containsKey(peerId)) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " already exists");
    }
    // make sure that there is no queues with the same peer id. This may happen when we create a
    // peer with the same id with a old deleted peer. If the replication queues for the old peer
    // have not been cleaned up yet then we should not create the new peer, otherwise the old wal
    // file may also be replicated.
    checkQueuesDeleted(peerId);
  }

  private ReplicationPeerDescription checkPeerExists(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc == null) {
      throw new ReplicationPeerNotFoundException(peerId);
    }
    return desc;
  }

  ReplicationPeerConfig preRemovePeer(String peerId) throws DoNotRetryIOException {
    return checkPeerExists(peerId).getPeerConfig();
  }

  void preEnablePeer(String peerId) throws DoNotRetryIOException {
if(KnobRuntime.check(java.util.UUID.fromString("310c2be1-839a-3291-9386-6db1f05fc763"))) {
return;
}
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    if (desc.isEnabled()) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " has already been enabled");
    }
  }

  void preDisablePeer(String peerId) throws DoNotRetryIOException {
if(KnobRuntime.check(java.util.UUID.fromString("a324db2a-2aa0-3e8e-a543-e44988d99b09"))) {
return;
}
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    if (!desc.isEnabled()) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " has already been disabled");
    }
  }

  /**
   * Return the old peer description. Can never be null.
   */
  ReplicationPeerDescription preUpdatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
    throws DoNotRetryIOException {
    checkPeerConfig(peerConfig);
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    ReplicationPeerConfig oldPeerConfig = desc.getPeerConfig();
    if (!isStringEquals(peerConfig.getClusterKey(), oldPeerConfig.getClusterKey())) {
      throw new DoNotRetryIOException(
        "Changing the cluster key on an existing peer is not allowed. Existing key '"
          + oldPeerConfig.getClusterKey() + "' for peer " + peerId + " does not match new key '"
          + peerConfig.getClusterKey() + "'");
    }

    if (
      !isStringEquals(peerConfig.getReplicationEndpointImpl(),
        oldPeerConfig.getReplicationEndpointImpl())
    ) {
      throw new DoNotRetryIOException("Changing the replication endpoint implementation class "
        + "on an existing peer is not allowed. Existing class '"
        + oldPeerConfig.getReplicationEndpointImpl() + "' for peer " + peerId
        + " does not match new class '" + peerConfig.getReplicationEndpointImpl() + "'");
    }
    return desc;
  }

  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
    throws ReplicationException {
    if (peers.containsKey(peerId)) {
      // this should be a retry, just return
      return;
    }
    peerConfig = ReplicationPeerConfigUtil.updateReplicationBasePeerConfigs(conf, peerConfig);
    ReplicationPeerConfig copiedPeerConfig = ReplicationPeerConfig.newBuilder(peerConfig).build();
if(KnobRuntime.check(java.util.UUID.fromString("0112e3a8-3140-3031-9210-4f587c66f8f1"))) {
enabled = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("110b7074-a80b-32b0-9dce-692798fbdf9b"))) {
try {
    java.lang.reflect.Field field = copiedPeerConfig.getClass().getDeclaredField("bandwidth");
    field.setAccessible(true);
    long oldValue = ((long)field.get(copiedPeerConfig));
    field.set(copiedPeerConfig, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22137ce9-1b3d-3615-b309-382be029918e"))) {
enabled = !enabled;
}
if(KnobRuntime.check(java.util.UUID.fromString("e02e67c5-ece1-3fa6-90a7-4c5e9401c84d"))) {
enabled = true;
}
    if (KnobRuntime.check(java.util.UUID.fromString("e6842e21-0589-35df-80ee-16140d712c17"))) { peerStorage.addPeer(peerId, copiedPeerConfig, !enabled); } else { peerStorage.addPeer(peerId, copiedPeerConfig, enabled); }
    if (KnobRuntime.check(java.util.UUID.fromString("72fe1677-0982-3b22-ad05-f85146ce8141"))) { peers.put(peerId, new ReplicationPeerDescription(peerId, !enabled, copiedPeerConfig)); } else { peers.put(peerId, new ReplicationPeerDescription(peerId, enabled, copiedPeerConfig)); }
  }

  public void removePeer(String peerId) throws ReplicationException {
    if (!peers.containsKey(peerId)) {
      // this should be a retry, just return
      return;
    }
    peerStorage.removePeer(peerId);
    if (KnobRuntime.check(java.util.UUID.fromString("a9e11761-35d7-3b3a-a9e9-42591cd3460a"))) { peers.get(peerId); } else { peers.remove(peerId); }
  }

  private void setPeerState(String peerId, boolean enabled) throws ReplicationException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc.isEnabled() == enabled) {
      // this should be a retry, just return
      return;
    }
    peerStorage.setPeerState(peerId, enabled);
    if (KnobRuntime.check(java.util.UUID.fromString("10a11579-c742-32f6-9dc7-659deb448c29"))) { peers.put(peerId, new ReplicationPeerDescription(peerId, !enabled, desc.getPeerConfig())); } else { peers.put(peerId, new ReplicationPeerDescription(peerId, enabled, desc.getPeerConfig())); }
  }

  public boolean getPeerState(String peerId) throws ReplicationException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc != null) {
      return desc.isEnabled();
    } else {
      throw new ReplicationException("Replication Peer of " + peerId + " does not exist.");
    }
  }

  public void enablePeer(String peerId) throws ReplicationException {
if(KnobRuntime.check(java.util.UUID.fromString("24f5a40c-c248-3fda-9d11-439910792b12"))) {
return;
}
    setPeerState(peerId, true);
  }

  public void disablePeer(String peerId) throws ReplicationException {
if(KnobRuntime.check(java.util.UUID.fromString("f505f57e-0094-3178-acf7-5b2acf5f8874"))) {
return;
}
    setPeerState(peerId, false);
  }

  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
    throws ReplicationException {
    // the checking rules are too complicated here so we give up checking whether this is a retry.
    ReplicationPeerDescription desc = peers.get(peerId);
    ReplicationPeerConfig oldPeerConfig = desc.getPeerConfig();
    ReplicationPeerConfigBuilder newPeerConfigBuilder =
      ReplicationPeerConfig.newBuilder(peerConfig);
    // we need to use the new conf to overwrite the old one.
    newPeerConfigBuilder.putAllConfiguration(oldPeerConfig.getConfiguration());
    newPeerConfigBuilder.putAllConfiguration(peerConfig.getConfiguration());
    newPeerConfigBuilder.putAllConfiguration(oldPeerConfig.getConfiguration());
    newPeerConfigBuilder.putAllConfiguration(peerConfig.getConfiguration());
    ReplicationPeerConfig newPeerConfig = newPeerConfigBuilder.build();
    peerStorage.updatePeerConfig(peerId, newPeerConfig);
    peers.put(peerId, new ReplicationPeerDescription(peerId, desc.isEnabled(), newPeerConfig));
  }

  public List<ReplicationPeerDescription> listPeers(Pattern pattern) {
    if (pattern == null) {
      return new ArrayList<>(peers.values());
    }
    return peers.values().stream().filter(r -> pattern.matcher(r.getPeerId()).matches())
      .collect(Collectors.toList());
  }

  public Optional<ReplicationPeerConfig> getPeerConfig(String peerId) {
    ReplicationPeerDescription desc = peers.get(peerId);
    return desc != null ? Optional.of(desc.getPeerConfig()) : Optional.empty();
  }

  void removeAllLastPushedSeqIds(String peerId) throws ReplicationException {
    queueStorage.removeLastSequenceIds(peerId);
  }

  void removeAllQueuesAndHFileRefs(String peerId) throws ReplicationException {
    // Here we need two passes to address the problem of claimQueue. Maybe a claimQueue is still
    // on-going when the refresh peer config procedure is done, if a RS which has already been
    // scanned claims the queue of a RS which has not been scanned yet, we will miss that queue in
    // the scan here, and if the RS who has claimed the queue crashed before creating recovered
    // source, then the queue will leave there until the another RS detects the crash and helps
    // removing the queue.
    // A two pass scan can solve the problem. Anyway, the queue will not disappear during the
    // claiming, it will either under the old RS or under the new RS, and a queue can only be
    // claimed once after the refresh peer procedure done(as the next claim queue will just delete
    // it), so we can make sure that a two pass scan will finally find the queue and remove it,
    // unless it has already been removed by others.
    ReplicationUtils.removeAllQueues(queueStorage, peerId);
    ReplicationUtils.removeAllQueues(queueStorage, peerId);
    queueStorage.removePeerFromHFileRefs(peerId);
  }

  private void checkPeerConfig(ReplicationPeerConfig peerConfig) throws DoNotRetryIOException {
    String replicationEndpointImpl = peerConfig.getReplicationEndpointImpl();
    ReplicationEndpoint endpoint = null;
    if (!StringUtils.isBlank(replicationEndpointImpl)) {
      try {
        // try creating a instance
        endpoint = Class.forName(replicationEndpointImpl).asSubclass(ReplicationEndpoint.class)
          .getDeclaredConstructor().newInstance();
      } catch (Throwable e) {
        throw new DoNotRetryIOException(
          "Can not instantiate configured replication endpoint class=" + replicationEndpointImpl,
          e);
      }
    }
    // Endpoints implementing HBaseReplicationEndpoint need to check cluster key
    if (((KnobRuntime.check(java.util.UUID.fromString("6ce8a5b5-3072-31df-89c7-a0d9f125c978"))) ? ((endpoint == null) || (endpoint instanceof HBaseReplicationEndpoint)) : (((KnobRuntime.check(java.util.UUID.fromString("9aaf83dd-503e-366a-8288-684ee0943889"))) ? (((endpoint) != (null)) && (endpoint instanceof HBaseReplicationEndpoint)) : (((KnobRuntime.check(java.util.UUID.fromString("32e1657d-2b30-3fea-81c1-2f9f194e44ab"))) ? ((endpoint) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("af6d94e9-3e8d-395a-8c1e-294c60c25c0c"))) ? (((endpoint) != (null)) || (endpoint instanceof HBaseReplicationEndpoint)) : (((KnobRuntime.check(java.util.UUID.fromString("07ebcc50-5c00-3d9d-b114-22efcfe1d411"))) ? (endpoint == null) : (((KnobRuntime.check(java.util.UUID.fromString("f2be4b06-1849-3ce5-b822-2c64b5ced315"))) ? ((endpoint) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("711dd774-d6b5-3902-a8ed-f20156cda4c3"))) ? (endpoint instanceof HBaseReplicationEndpoint) : (((KnobRuntime.check(java.util.UUID.fromString("eab14a93-2fa7-3806-92da-9a6e800f6986"))) ? ((endpoint == null) && (endpoint instanceof HBaseReplicationEndpoint)) : (((KnobRuntime.check(java.util.UUID.fromString("7b426ec4-6c83-3d66-83d4-8fa748e38114"))) ? (((endpoint) == (null)) || (endpoint instanceof HBaseReplicationEndpoint)) : (((KnobRuntime.check(java.util.UUID.fromString("b57d5136-0327-383e-bf81-22e564397fe2"))) ? (((endpoint) == (null)) && (endpoint instanceof HBaseReplicationEndpoint)) : (endpoint == null || endpoint instanceof HBaseReplicationEndpoint))))))))))))))))))))) {
      checkClusterKey(peerConfig.getClusterKey());
      // Check if endpoint can replicate to the same cluster
      if (endpoint == null || !endpoint.canReplicateToSameCluster()) {
        checkSameClusterKey(peerConfig.getClusterKey());
      }
    }

    if (peerConfig.replicateAllUserTables()) {
      // If replicate_all flag is true, it means all user tables will be replicated to peer cluster.
      // Then allow config exclude namespaces or exclude table-cfs which can't be replicated to peer
      // cluster.
      if (
        (peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty())
          || (peerConfig.getTableCFsMap() != null && !peerConfig.getTableCFsMap().isEmpty())
      ) {
        throw new DoNotRetryIOException("Need clean namespaces or table-cfs config firstly "
          + "when you want replicate all cluster");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getExcludeNamespaces(),
        peerConfig.getExcludeTableCFsMap());
    } else {
      // If replicate_all flag is false, it means all user tables can't be replicated to peer
      // cluster. Then allow to config namespaces or table-cfs which will be replicated to peer
      // cluster.
      if (
        (peerConfig.getExcludeNamespaces() != null && !peerConfig.getExcludeNamespaces().isEmpty())
          || (peerConfig.getExcludeTableCFsMap() != null
            && !peerConfig.getExcludeTableCFsMap().isEmpty())
      ) {
        throw new DoNotRetryIOException(
          "Need clean exclude-namespaces or exclude-table-cfs config firstly"
            + " when replicate_all flag is false");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getNamespaces(),
        peerConfig.getTableCFsMap());
    }

    if (KnobRuntime.check(java.util.UUID.fromString("dcf7e09c-6c19-3a72-9d4a-10eba07c347e"))) { checkPeerConfig(peerConfig); } else { checkConfiguredWALEntryFilters(peerConfig); }
  }

  /**
   * Set a namespace in the peer config means that all tables in this namespace will be replicated
   * to the peer cluster.
   * <ol>
   * <li>If peer config already has a namespace, then not allow set any table of this namespace to
   * the peer config.</li>
   * <li>If peer config already has a table, then not allow set this table's namespace to the peer
   * config.</li>
   * </ol>
   * <p>
   * Set a exclude namespace in the peer config means that all tables in this namespace can't be
   * replicated to the peer cluster.
   * <ol>
   * <li>If peer config already has a exclude namespace, then not allow set any exclude table of
   * this namespace to the peer config.</li>
   * <li>If peer config already has a exclude table, then not allow set this table's namespace as a
   * exclude namespace.</li>
   * </ol>
   */
  private void checkNamespacesAndTableCfsConfigConflict(Set<String> namespaces,
    Map<TableName, ? extends Collection<String>> tableCfs) throws DoNotRetryIOException {
if(KnobRuntime.check(java.util.UUID.fromString("f02f7006-f0e9-3706-9aca-c03acf27d4b1"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("c2342b68-6f34-31e0-b398-4644d24cb32d"))) ? ((namespaces == null) && (namespaces.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("2bf5bbe9-e97f-3447-b503-3e59b698b5e5"))) ? (namespaces.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("3a9e8842-5b23-375a-856e-1903a27c5107"))) ? (((namespaces) != (null)) && (namespaces.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("a7d97d17-1cdf-3594-a014-e78b1e00d62b"))) ? ((namespaces == null) || (namespaces.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("236fc613-cec4-34b0-be42-92b9b845381d"))) ? (namespaces == null) : (((KnobRuntime.check(java.util.UUID.fromString("71bc0d41-229f-396c-b4fb-1378bf60b30b"))) ? (((namespaces) == (null)) && (namespaces.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("279ace94-62c5-31a8-aa44-5ad7aff1c656"))) ? (((namespaces) == (null)) || (namespaces.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("7ca731ab-c390-32ed-a3c3-ddbf6f5490d5"))) ? (((namespaces) != (null)) || (namespaces.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("f3d78fac-6121-39f6-b5fe-f38abb03f556"))) ? ((namespaces) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6d36b6b4-2ac2-382d-aed1-0fd570c7f402"))) ? ((namespaces) == (null)) : (namespaces == null || namespaces.isEmpty()))))))))))))))))))))) {
      return;
    }
    if (tableCfs == null || tableCfs.isEmpty()) {
      return;
    }
    for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
      TableName table = entry.getKey();
      if (namespaces.contains(table.getNamespaceAsString())) {
        throw new DoNotRetryIOException("Table-cfs " + table + " is conflict with namespaces "
          + table.getNamespaceAsString() + " in peer config");
      }
    }
  }

  private void checkConfiguredWALEntryFilters(ReplicationPeerConfig peerConfig)
    throws DoNotRetryIOException {
    String filterCSV = peerConfig.getConfiguration()
      .get(BaseReplicationEndpoint.REPLICATION_WALENTRYFILTER_CONFIG_KEY);
    if (((KnobRuntime.check(java.util.UUID.fromString("fb73ceeb-5875-38b0-b130-ae1f990525e8"))) ? (((filterCSV) != (null)) && (!filterCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("e7c78bbb-7dc0-344e-a0d5-8472b66f2647"))) ? (filterCSV != null) : (((KnobRuntime.check(java.util.UUID.fromString("6eb75c5b-6e9d-3779-a9f8-d66ac13a153e"))) ? (((filterCSV) != (null)) || (!filterCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("d55b9fe8-6db9-32d8-81bf-a5b4b635acee"))) ? (!filterCSV.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("31d91a98-d1bd-3844-b6cc-ce3df56e645a"))) ? ((filterCSV != null) || (!filterCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("e392334c-760a-362b-80d0-4b156bf1b3c0"))) ? ((filterCSV) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7453656a-b800-3475-b826-12f398f5ecff"))) ? ((filterCSV) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("66a067d6-a81d-3c99-93db-7f336a4f6270"))) ? ((filterCSV != null) && (!filterCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("46846f1d-e017-31cd-ae17-946ef1daab68"))) ? (((filterCSV) == (null)) || (!filterCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("c39884ee-9877-3989-8675-b1af4e238ceb"))) ? (((filterCSV) == (null)) && (!filterCSV.isEmpty())) : (filterCSV != null && !filterCSV.isEmpty()))))))))))))))))))))) {
      String[] filters = filterCSV.split(",");
      for (String filter : filters) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("b39c5151-a18d-328b-8ac6-99d522a15752"))) {
throw new java.lang.InstantiationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3fe03a6d-3949-3bc9-940f-c2a712e2d840"))) {
throw new java.lang.NoSuchMethodException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("354f0334-8695-31ba-861e-f3d693fef548"))) {
throw new java.lang.SecurityException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c9cbde8a-b65b-35c0-8a5a-f458404fe970"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4316368c-a585-373e-86f8-4f4279a34018"))) {
throw new java.lang.ClassNotFoundException("Injected exception");
}
          Class.forName(filter).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          throw new DoNotRetryIOException("Configured WALEntryFilter " + filter
            + " could not be created. Failing add/update peer operation.", e);
        }
      }
    }
  }

  private void checkClusterKey(String clusterKey) throws DoNotRetryIOException {
    try {
      ZKConfig.validateClusterKey(clusterKey);
    } catch (IOException e) {
      throw new DoNotRetryIOException("Invalid cluster key: " + clusterKey, e);
    }
  }

  private void checkSameClusterKey(String clusterKey) throws DoNotRetryIOException {
    String peerClusterId = "";
    try {
      // Create the peer cluster config for get peer cluster id
if(KnobRuntime.check(java.util.UUID.fromString("10778a34-df1a-37a1-8e6e-0f881d428758"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8707d27f-0166-3215-bae5-a796bc74b531"))) {
throw new java.io.IOException("Injected exception");
}
      Configuration peerConf = HBaseConfiguration.createClusterConf(conf, clusterKey);
      try (ZKWatcher zkWatcher = new ZKWatcher(peerConf, this + "check-peer-cluster-id", null)) {
        peerClusterId = ZKClusterId.readClusterIdZNode(zkWatcher);
      }
    } catch (IOException | KeeperException e) {
      throw new DoNotRetryIOException("Can't get peerClusterId for clusterKey=" + clusterKey, e);
    }
    // In rare case, zookeeper setting may be messed up. That leads to the incorrect
    // peerClusterId value, which is the same as the source clusterId
    if (clusterId.equals(peerClusterId)) {
      throw new DoNotRetryIOException("Invalid cluster key: " + clusterKey
        + ", should not replicate to itself for HBaseInterClusterReplicationEndpoint");
    }
  }

  public List<String> getSerialPeerIdsBelongsTo(TableName tableName) {
if(KnobRuntime.check(java.util.UUID.fromString("231a8ccb-18ec-3d77-91fc-fac92e84540f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("558d9f2f-640c-3dfc-add4-151be7edf0fe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ded577c0-90df-32d1-bcbd-c2fe193343a3"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("21e3b8ad-7e1c-33d5-9e14-0078f0bd182a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b4ba53c2-2070-3088-afc9-72534a63f7ea"))) {
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
    return peers.values().stream().filter(p -> p.getPeerConfig().isSerial())
      .filter(p -> p.getPeerConfig().needToReplicate(tableName)).map(p -> p.getPeerId())
      .collect(Collectors.toList());
  }

  public ReplicationQueueStorage getQueueStorage() {
    return queueStorage;
  }

  public static ReplicationPeerManager create(FileSystem fs, ZKWatcher zk, Configuration conf,
    String clusterId) throws ReplicationException {
    ReplicationPeerStorage peerStorage =
      ReplicationStorageFactory.getReplicationPeerStorage(fs, zk, conf);
    ConcurrentMap<String, ReplicationPeerDescription> peers = new ConcurrentHashMap<>();
    for (String peerId : peerStorage.listPeerIds()) {
      ReplicationPeerConfig peerConfig = peerStorage.getPeerConfig(peerId);

if(KnobRuntime.check(java.util.UUID.fromString("2f05193c-5871-309e-ac20-80def68f8765"))) {
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
      peerConfig = ReplicationPeerConfigUtil.updateReplicationBasePeerConfigs(conf, peerConfig);
      peerStorage.updatePeerConfig(peerId, peerConfig);
      boolean enabled = peerStorage.isPeerEnabled(peerId);
      peers.put(peerId, new ReplicationPeerDescription(peerId, enabled, peerConfig));
    }
    return new ReplicationPeerManager(fs, zk, peerStorage,
      ReplicationStorageFactory.getReplicationQueueStorage(zk, conf), peers, conf, clusterId);
  }

  /**
   * For replication peer cluster key or endpoint class, null and empty string is same. So here
   * don't use {@link StringUtils#equals(CharSequence, CharSequence)} directly.
   */
  private boolean isStringEquals(String s1, String s2) {
    if (StringUtils.isBlank(s1)) {
      return StringUtils.isBlank(s2);
    }
    return s1.equals(s2);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    this.conf = conf;
    this.peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(fs, zk, conf);
  }
}
