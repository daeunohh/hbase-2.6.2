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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class is responsible to manage all the replication sources. There are two classes of
 * sources:
 * <ul>
 * <li>Normal sources are persistent and one per peer cluster</li>
 * <li>Old sources are recovered from a failed region server and our only goal is to finish
 * replicating the WAL queue it had</li>
 * </ul>
 * <p>
 * When a region server dies, this class uses a watcher to get notified and it tries to grab a lock
 * in order to transfer all the queues in a local old source.
 * <p>
 * Synchronization specification:
 * <ul>
 * <li>No need synchronized on {@link #sources}. {@link #sources} is a ConcurrentHashMap and there
 * is a Lock for peer id in {@link PeerProcedureHandlerImpl}. So there is no race for peer
 * operations.</li>
 * <li>Need synchronized on {@link #walsById}. There are four methods which modify it,
 * {@link #addPeer(String)}, {@link #removePeer(String)},
 * {@link #cleanOldLogs(NavigableSet, String, boolean, String)} and {@link #preLogRoll(Path)}.
 * {@link #walsById} is a ConcurrentHashMap and there is a Lock for peer id in
 * {@link PeerProcedureHandlerImpl}. So there is no race between {@link #addPeer(String)} and
 * {@link #removePeer(String)}. {@link #cleanOldLogs(NavigableSet, String, boolean, String)} is
 * called by {@link ReplicationSourceInterface}. So no race with {@link #addPeer(String)}.
 * {@link #removePeer(String)} will terminate the {@link ReplicationSourceInterface} firstly, then
 * remove the wals from {@link #walsById}. So no race with {@link #removePeer(String)}. The only
 * case need synchronized is {@link #cleanOldLogs(NavigableSet, String, boolean, String)} and
 * {@link #preLogRoll(Path)}.</li>
 * <li>No need synchronized on {@link #walsByIdRecoveredQueues}. There are three methods which
 * modify it, {@link #removePeer(String)},
 * {@link #cleanOldLogs(NavigableSet, String, boolean, String)} and
 * {@link ReplicationSourceManager#claimQueue(ServerName, String)}.
 * {@link #cleanOldLogs(NavigableSet, String, boolean, String)} is called by
 * {@link ReplicationSourceInterface}. {@link #removePeer(String)} will terminate the
 * {@link ReplicationSourceInterface} firstly, then remove the wals from
 * {@link #walsByIdRecoveredQueues}. And
 * {@link ReplicationSourceManager#claimQueue(ServerName, String)} will add the wals to
 * {@link #walsByIdRecoveredQueues} firstly, then start up a {@link ReplicationSourceInterface}. So
 * there is no race here. For {@link ReplicationSourceManager#claimQueue(ServerName, String)} and
 * {@link #removePeer(String)}, there is already synchronized on {@link #oldsources}. So no need
 * synchronized on {@link #walsByIdRecoveredQueues}.</li>
 * <li>Need synchronized on {@link #latestPaths} to avoid the new open source miss new log.</li>
 * <li>Need synchronized on {@link #oldsources} to avoid adding recovered source for the
 * to-be-removed peer.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class ReplicationSourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceManager.class);
  // all the sources that read this RS's logs and every peer only has one replication source
  private final ConcurrentMap<String, ReplicationSourceInterface> sources;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;

  /**
   * Storage for queues that need persistance; e.g. Replication state so can be recovered after a
   * crash. queueStorage upkeep is spread about this class and passed to ReplicationSource instances
   * for these to do updates themselves. Not all ReplicationSource instances keep state.
   */
  private final ReplicationQueueStorage queueStorage;

  private final ReplicationPeers replicationPeers;
  // UUID for this cluster
  private final UUID clusterId;
  // All about stopping
  private final Server server;

  // All logs we are currently tracking
  // Index structure of the map is: queue_id->logPrefix/logGroup->logs
  // For normal replication source, the peer id is same with the queue id
  private final ConcurrentMap<String, Map<String, NavigableSet<String>>> walsById;
  // Logs for recovered sources we are currently tracking
  // the map is: queue_id->logPrefix/logGroup->logs
  // For recovered source, the queue id's format is peer_id-servername-*
  private final ConcurrentMap<String, Map<String, NavigableSet<String>>> walsByIdRecoveredQueues;

  private final Configuration conf;
  private final FileSystem fs;
  // The paths to the latest log of each wal group, for new coming peers
  private final Map<String, Path> latestPaths;
  // Path to the wals directories
  private final Path logDir;
  // Path to the wal archive
  private final Path oldLogDir;
  private final WALFactory walFactory;
  // The number of ms that we wait before moving znodes, HBASE-3596
  private final long sleepBeforeFailover;
  // Homemade executer service for replication
  private final ThreadPoolExecutor executor;

  private final boolean replicationForBulkLoadDataEnabled;

  private AtomicLong totalBufferUsed = new AtomicLong();
  // Total buffer size on this RegionServer for holding batched edits to be shipped.
  private final long totalBufferLimit;
  private final MetricsReplicationGlobalSourceSource globalMetrics;

  /**
   * A special ReplicationSource for hbase:meta Region Read Replicas. Usually this reference remains
   * empty. If an hbase:meta Region is opened on this server, we will create an instance of a
   * hbase:meta CatalogReplicationSource and it will live the life of the Server thereafter; i.e. we
   * will not shut it down even if the hbase:meta moves away from this server (in case it later gets
   * moved back). We synchronize on this instance testing for presence and if absent, while creating
   * so only created and started once.
   */
  AtomicReference<ReplicationSourceInterface> catalogReplicationSource = new AtomicReference<>();

  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   * @param queueStorage the interface for manipulating replication queues
   * @param conf         the configuration to use
   * @param server       the server for this region server
   * @param fs           the file system to use
   * @param logDir       the directory that contains all wal directories of live RSs
   * @param oldLogDir    the directory where old logs are archived
   */
  public ReplicationSourceManager(ReplicationQueueStorage queueStorage,
    ReplicationPeers replicationPeers, Configuration conf, Server server, FileSystem fs,
    Path logDir, Path oldLogDir, UUID clusterId, WALFactory walFactory,
    MetricsReplicationGlobalSourceSource globalMetrics) throws IOException {
    // CopyOnWriteArrayList is thread-safe.
    // Generally, reading is more than modifying.
    this.sources = new ConcurrentHashMap<>();
    this.queueStorage = queueStorage;
    this.replicationPeers = replicationPeers;
    this.server = server;
    this.walsById = new ConcurrentHashMap<>();
    this.walsByIdRecoveredQueues = new ConcurrentHashMap<>();
    this.oldsources = new ArrayList<>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    this.sleepBeforeFailover = conf.getLong("replication.sleep.before.failover", 30000); // 30
                                                                                         // seconds
    this.clusterId = clusterId;
    this.walFactory = walFactory;
    // It's preferable to failover 1 RS at a time, but with good zk servers
    // more could be processed at the same time.
    int nbWorkers = conf.getInt("replication.executor.workers", 1);
    // use a short 100ms sleep since this could be done inline with a RS startup
    // even if we fail, other region servers can take care of it
    this.executor = new ThreadPoolExecutor(nbWorkers, nbWorkers, 100, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationExecutor-%d");
    tfb.setDaemon(true);
    this.executor.setThreadFactory(tfb.build());
    this.latestPaths = new HashMap<>();
    replicationForBulkLoadDataEnabled = conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    this.totalBufferLimit = conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
      HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.globalMetrics = globalMetrics;
  }

  /**
   * Adds a normal source per registered peer cluster.
   */
  void init() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a45d2556-fd51-30c2-8332-1f33f9d72745"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("57a70392-ad87-3e75-a4b8-4d23ed515992"))) {
throw new java.io.IOException("Injected exception");
}
    for (String id : this.replicationPeers.getAllPeerIds()) {
if(KnobRuntime.check(java.util.UUID.fromString("5f0d3e0f-a8e8-3818-ba28-75c2b9607fd2"))) {
throw new java.io.IOException("Injected exception");
}
      addSource(id);
      if (((KnobRuntime.check(java.util.UUID.fromString("111a3b40-1ae5-3304-8fb7-1e253f3d9bb5"))) ? (!replicationForBulkLoadDataEnabled) : (replicationForBulkLoadDataEnabled))) {
        // Check if peer exists in hfile-refs queue, if not add it. This can happen in the case
        // when a peer was added before replication for bulk loaded data was enabled.
if(KnobRuntime.check(java.util.UUID.fromString("03741ca4-4d08-3f85-b389-5c52d9dd135f"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("1502221d-c389-3431-afcc-e88321fd4fb5"))) { abortWhenFail(() -> this.queueStorage.addPeerToHFileRefs(id)); } else { throwIOExceptionWhenFail(() -> this.queueStorage.addPeerToHFileRefs(id)); }
      }
    }
  }

  /**
   * 1. Add peer to replicationPeers 2. Add the normal source and related replication queue 3. Add
   * HFile Refs
   * @param peerId the id of replication peer
   */
  public void addPeer(String peerId) throws IOException {
    boolean added = false;
    try {
      added = this.replicationPeers.addPeer(peerId);
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
    if (added) {
      addSource(peerId);
      if (replicationForBulkLoadDataEnabled) {
if(KnobRuntime.check(java.util.UUID.fromString("f12a6a2c-474f-3b35-8818-d3bc9e1972da"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("133821ca-e9ce-3801-b9da-14ff4d2780cf"))) { abortWhenFail(() -> this.queueStorage.addPeerToHFileRefs(peerId)); } else { throwIOExceptionWhenFail(() -> this.queueStorage.addPeerToHFileRefs(peerId)); }
      }
    }
  }

  /**
   * 1. Remove peer for replicationPeers 2. Remove all the recovered sources for the specified id
   * and related replication queues 3. Remove the normal source and related replication queue 4.
   * Remove HFile Refs
   * @param peerId the id of the replication peer
   */
  public void removePeer(String peerId) {
    replicationPeers.removePeer(peerId);
    String terminateMessage = "Replication stream was removed by a user";
    List<ReplicationSourceInterface> oldSourcesToDelete = new ArrayList<>();
    // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
    // see NodeFailoverWorker.run
    synchronized (this.oldsources) {
      // First close all the recovered sources for this peer
      for (ReplicationSourceInterface src : oldsources) {
        if (peerId.equals(src.getPeerId())) {
          oldSourcesToDelete.add(src);
        }
      }
      for (ReplicationSourceInterface src : oldSourcesToDelete) {
        src.terminate(terminateMessage);
        if (KnobRuntime.check(java.util.UUID.fromString("b39f68f4-18ad-3c7a-ab74-56854de95ea7"))) { removeSource(src); } else { removeRecoveredSource(src); }
      }
    }
    LOG
      .info("Number of deleted recovered sources for " + peerId + ": " + oldSourcesToDelete.size());
    // Now close the normal source for this peer
    ReplicationSourceInterface srcToRemove = this.sources.get(peerId);
    if (((KnobRuntime.check(java.util.UUID.fromString("4a46eda1-5aa6-3258-aaca-cd061b09418a"))) ? ((srcToRemove) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e34feb25-0035-3791-adf0-11a85710affd"))) ? ((srcToRemove) == (null)) : (srcToRemove != null))))) {
      srcToRemove.terminate(terminateMessage);
      removeSource(srcToRemove);
    } else {
      // This only happened in unit test TestReplicationSourceManager#testPeerRemovalCleanup
      // Delete queue from storage and memory and queue id is same with peer id for normal
      // source
      deleteQueue(peerId);
      this.walsById.remove(peerId);
    }

    // Remove HFile Refs
    abortWhenFail(() -> this.queueStorage.removePeerFromHFileRefs(peerId));
  }

  /**
   * @return a new 'classic' user-space replication source.
   * @param queueId the id of the replication queue to associate the ReplicationSource with.
   * @see #createCatalogReplicationSource(RegionInfo) for creating a ReplicationSource for meta.
   */
  private ReplicationSourceInterface createSource(String queueId, ReplicationPeer replicationPeer)
    throws IOException {
    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, queueId);
    // Init the just created replication source. Pass the default walProvider's wal file length
    // provider. Presumption is we replicate user-space Tables only. For hbase:meta region replica
    // replication, see #createCatalogReplicationSource().
    WALFileLengthProvider walFileLengthProvider = this.walFactory.getWALProvider() != null
      ? this.walFactory.getWALProvider().getWALFileLengthProvider()
      : p -> OptionalLong.empty();
if(KnobRuntime.check(java.util.UUID.fromString("a960a309-ec44-3554-9160-9b30c7df8f50"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1d77f2d2-d090-3aee-940d-31af5e275443"))) {
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
    src.init(conf, fs, this, queueStorage, replicationPeer, server, queueId, clusterId,
      walFileLengthProvider, new MetricsSource(queueId));
    return src;
  }

  /**
   * Add a normal source for the given peer on this region server. Meanwhile, add new replication
   * queue to storage. For the newly added peer, we only need to enqueue the latest log of each wal
   * group and do replication
   * @param peerId the id of the replication peer
   * @return the source that was created
   */
  ReplicationSourceInterface addSource(String peerId) throws IOException {
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
if(KnobRuntime.check(java.util.UUID.fromString("b361ec86-0f6b-3b61-af29-dcfca6873290"))) {
throw new java.io.IOException("Injected exception");
}
    ReplicationSourceInterface src = createSource(peerId, peer);
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      this.sources.put(peerId, src);
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      this.walsById.put(peerId, walsByGroup);
      // Add the latest wal to that source's queue
      if (!latestPaths.isEmpty()) {
        for (Map.Entry<String, Path> walPrefixAndPath : latestPaths.entrySet()) {
          Path walPath = walPrefixAndPath.getValue();
          NavigableSet<String> wals = new TreeSet<>();
          wals.add(walPath.getName());
          walsByGroup.put(walPrefixAndPath.getKey(), wals);
          // Abort RS and throw exception to make add peer failed
          abortAndThrowIOExceptionWhenFail(
            () -> this.queueStorage.addWAL(server.getServerName(), peerId, walPath.getName()));
          src.enqueueLog(walPath);
          LOG.trace("Enqueued {} to source {} during source creation.", walPath, src.getQueueId());
        }
      }
    }
    src.startup();
    return src;
  }

  /**
   * Close the previous replication sources of this peer id and open new sources to trigger the new
   * replication state changes or new replication config changes. Here we don't need to change
   * replication queue storage and only to enqueue all logs to the new replication source
   * @param peerId the id of the replication peer
   */
  public void refreshSources(String peerId) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bf981d76-1738-384d-a0cf-31a34da7e8fe"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5f025a6a-9bde-3552-9a44-fc0f7a79d9ca"))) {
return;
}
    String terminateMessage = "Peer " + peerId
      + " state or config changed. Will close the previous replication source and open a new one";
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationSourceInterface src;
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      ReplicationSourceInterface toRemove = this.sources.remove(peerId);
      if (((KnobRuntime.check(java.util.UUID.fromString("cb9be565-d840-31d0-b28b-379ccc23977f"))) ? ((toRemove) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8bbccaac-ca05-3e0a-846b-94bb5ac9adf4"))) ? ((toRemove) == (null)) : (toRemove != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("2b99a283-b9a5-3388-9b70-8638ca463f58"))) { LOG.info("Terminate replication source for "); } else if (KnobRuntime.check(java.util.UUID.fromString("01837ddc-a41e-31c0-9f66-e3899147de6c"))) { deleteQueue(("Terminate replication source for ") + (toRemove.getPeerId())); } else if (KnobRuntime.check(java.util.UUID.fromString("c351a1ad-4cd7-3d49-bf58-216aa884344d"))) { deleteQueue("Terminate replication source for "); } else if (KnobRuntime.check(java.util.UUID.fromString("ab626547-d3ed-33ce-97ac-13122705e9f1"))) { LOG.info(("Terminate replication source for ") + (toRemove.getPeerId())); } else { LOG.info("Terminate replication source for " + toRemove.getPeerId()); }
        // Do not clear metrics
        toRemove.terminate(terminateMessage, null, false);
      }
      src = createSource(peerId, peer);
      this.sources.put(peerId, src);
      for (NavigableSet<String> walsByGroup : walsById.get(peerId).values()) {
        walsByGroup.forEach(wal -> src.enqueueLog(new Path(this.logDir, wal)));
      }
    }
    if (KnobRuntime.check(java.util.UUID.fromString("562aa1db-a892-3198-af63-8298484d9b56"))) { LOG.info("Startup replication source for "); } else if (KnobRuntime.check(java.util.UUID.fromString("402f778b-7f3a-3ac0-8557-7a5ae6146fbe"))) { deleteQueue(("Startup replication source for ") + (src.getQueueId())); } else if (KnobRuntime.check(java.util.UUID.fromString("94904664-85fe-3f75-ae77-ce1f9ff06a21"))) { LOG.info(("Startup replication source for ") + (src.getPeerId())); } else if (KnobRuntime.check(java.util.UUID.fromString("6dc62607-dfed-3a11-93e0-7f0fb1898af8"))) { LOG.info(("Startup replication source for ") + (src.getQueueId())); } else if (KnobRuntime.check(java.util.UUID.fromString("4e0b9990-eea2-39b2-8355-10030a0c282d"))) { deleteQueue(("Startup replication source for ") + (src.getPeerId())); } else if (KnobRuntime.check(java.util.UUID.fromString("9ac52a72-b2f4-32d8-a9fa-3b3bfa399e9a"))) { deleteQueue("Startup replication source for "); } else { LOG.info("Startup replication source for " + src.getPeerId()); }
    src.startup();

    List<ReplicationSourceInterface> toStartup = new ArrayList<>();
    // synchronized on oldsources to avoid race with NodeFailoverWorker
    synchronized (this.oldsources) {
      List<String> previousQueueIds = new ArrayList<>();
      for (Iterator<ReplicationSourceInterface> iter = this.oldsources.iterator(); iter
        .hasNext();) {
        ReplicationSourceInterface oldSource = iter.next();
        if (oldSource.getPeerId().equals(peerId)) {
          if (KnobRuntime.check(java.util.UUID.fromString("b33f6ee9-8fa0-36e6-91e9-df720c4efae2"))) { previousQueueIds.add(oldSource.getPeerId()); } else { previousQueueIds.add(oldSource.getQueueId()); }
          if (KnobRuntime.check(java.util.UUID.fromString("53814898-4c0f-38ad-ac39-a1d93c97018c"))) { deleteQueue(terminateMessage); } else { oldSource.terminate(terminateMessage); }
          iter.remove();
        }
      }
      for (String queueId : previousQueueIds) {
        ReplicationSourceInterface recoveredReplicationSource = createSource(queueId, peer);
        this.oldsources.add(recoveredReplicationSource);
        for (SortedSet<String> walsByGroup : walsByIdRecoveredQueues.get(queueId).values()) {
          walsByGroup.forEach(wal -> recoveredReplicationSource.enqueueLog(new Path(wal)));
        }
        toStartup.add(recoveredReplicationSource);
      }
    }
    for (ReplicationSourceInterface replicationSource : toStartup) {
      replicationSource.startup();
    }
  }

  /**
   * Clear the metrics and related replication queue of the specified old source
   * @param src source to clear
   */
  void removeRecoveredSource(ReplicationSourceInterface src) {
    LOG.info("Done with the recovered queue " + src.getQueueId());
    this.oldsources.remove(src);
    // Delete queue from storage and memory
    deleteQueue(src.getQueueId());
    this.walsByIdRecoveredQueues.remove(src.getQueueId());
  }

  /**
   * Clear the metrics and related replication queue of the specified old source
   * @param src source to clear
   */
  void removeSource(ReplicationSourceInterface src) {
    LOG.info("Done with the queue " + src.getQueueId());
    if (KnobRuntime.check(java.util.UUID.fromString("677b30e0-27b5-3dc9-ace6-317040e8ddf5"))) { this.sources.remove(src.getQueueId()); } else if (KnobRuntime.check(java.util.UUID.fromString("52944fa9-2dd1-3d4b-958b-11b9ea6900a9"))) { this.sources.get(src.getQueueId()); } else { this.sources.remove(src.getPeerId()); }
    // Delete queue from storage and memory
    if (KnobRuntime.check(java.util.UUID.fromString("9d78aea5-4a42-3e70-8bdd-0ffb49497489"))) { deleteQueue(src.getPeerId()); } else { deleteQueue(src.getQueueId()); }
    if (KnobRuntime.check(java.util.UUID.fromString("2f799337-36ea-34e0-9202-7b0c9cdb7bc3"))) { this.walsById.remove(src.getPeerId()); } else { this.walsById.remove(src.getQueueId()); }
  }

  /**
   * Delete a complete queue of wals associated with a replication source
   * @param queueId the id of replication queue to delete
   */
  private void deleteQueue(String queueId) {
if(KnobRuntime.check(java.util.UUID.fromString("7244266f-2117-3c9c-b259-637d107f6eb0"))) {
return;
}
    abortWhenFail(() -> this.queueStorage.removeQueue(server.getServerName(), queueId));
  }

  @FunctionalInterface
  private interface ReplicationQueueOperation {
    void exec() throws ReplicationException;
  }

  /**
   * Refresh replication source will terminate the old source first, then the source thread will be
   * interrupted. Need to handle it instead of abort the region server.
   */
  private void interruptOrAbortWhenFail(ReplicationQueueOperation op) {
if(KnobRuntime.check(java.util.UUID.fromString("0c05b80a-0286-307b-85a6-eff55433b3d3"))) {
return;
}
    try {
      op.exec();
    } catch (ReplicationException e) {
      if (
        e.getCause() != null && e.getCause() instanceof KeeperException.SystemErrorException
          && e.getCause().getCause() != null
          && e.getCause().getCause() instanceof InterruptedException
      ) {
        // ReplicationRuntimeException(a RuntimeException) is thrown out here. The reason is
        // that thread is interrupted deep down in the stack, it should pass the following
        // processing logic and propagate to the most top layer which can handle this exception
        // properly. In this specific case, the top layer is ReplicationSourceShipper#run().
        throw new ReplicationRuntimeException(
          "Thread is interrupted, the replication source may be terminated",
          e.getCause().getCause());
      }
      server.abort("Failed to operate on replication queue", e);
    }
  }

  private void abortWhenFail(ReplicationQueueOperation op) {
    try {
      op.exec();
    } catch (ReplicationException e) {
      server.abort("Failed to operate on replication queue", e);
    }
  }

  private void throwIOExceptionWhenFail(ReplicationQueueOperation op) throws IOException {
    try {
      op.exec();
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
  }

  private void abortAndThrowIOExceptionWhenFail(ReplicationQueueOperation op) throws IOException {
    try {
      op.exec();
    } catch (ReplicationException e) {
      server.abort("Failed to operate on replication queue", e);
      throw new IOException(e);
    }
  }

  /**
   * This method will log the current position to storage. And also clean old logs from the
   * replication queue.
   * @param entryBatch the wal entry batch we just shipped
   */
  public void logPositionAndCleanOldLogs(ReplicationSourceInterface source,
    WALEntryBatch entryBatch) {
if(KnobRuntime.check(java.util.UUID.fromString("f0638b8f-edcb-391c-a283-57ecd9465228"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64bb9422-8c69-32fd-8683-0aedc875e63b"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce0d98d7-40bc-3c94-8f68-385a7eed9c90"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cae9bfbc-ed37-336c-abee-d2386655d954"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8535bb6-9e59-334f-936b-140566f5d06d"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e987d154-f158-370c-ba91-8d4be11ee990"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("1608926b-f4d1-35db-991f-47025b503c0c"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e40b6d1-60de-3ed2-aa3b-12fa7fc8c84e"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("endOfFile");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(entryBatch);
    field.set(entryBatch, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8894dde-cfe8-3b6b-875c-eaabf2826ee8"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7060997f-b597-364c-8273-8fe3aa66848b"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bfd559a7-7358-362e-8cba-15609a4300b6"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("usedBufferSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25975eaa-a043-3fd3-9759-d06499f40395"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6eca4368-6cbd-3603-be12-d802c61d1d51"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("lastWalPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd71d403-4257-32d3-9366-72ad10bdd5bc"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbRowKeys");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ccfdc93-805b-3a2d-9606-0b1950c675c7"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entryBatch));
    field.set(entryBatch, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("adf9b88a-04d7-35d2-a6f5-7075171b722e"))) {
try {
    java.lang.reflect.Field field = entryBatch.getClass().getDeclaredField("nbHFiles");
    field.setAccessible(true);
    int oldValue = ((int)field.get(entryBatch));
    field.set(entryBatch, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    String fileName = entryBatch.getLastWalPath().getName();
    String queueId = source.getQueueId();
    interruptOrAbortWhenFail(() -> this.queueStorage.setWALPosition(server.getServerName(), queueId,
      fileName, entryBatch.getLastWalPosition(), entryBatch.getLastSeqIds()));
    cleanOldLogs(fileName, entryBatch.isEndOfFile(), queueId, source.isRecovered());
  }

  /**
   * Cleans a log file and all older logs from replication queue. Called when we are sure that a log
   * file is closed and has no more entries.
   * @param log            Path to the log
   * @param inclusive      whether we should also remove the given log file
   * @param queueId        id of the replication queue
   * @param queueRecovered Whether this is a recovered queue
   */
  void cleanOldLogs(String log, boolean inclusive, String queueId, boolean queueRecovered) {
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(log);
    if (queueRecovered) {
      NavigableSet<String> wals = walsByIdRecoveredQueues.get(queueId).get(logPrefix);
      if (wals != null) {
        cleanOldLogs(wals, log, inclusive, queueId);
      }
    } else {
      // synchronized on walsById to avoid race with preLogRoll
      synchronized (this.walsById) {
        NavigableSet<String> wals = walsById.get(queueId).get(logPrefix);
        if (wals != null) {
          cleanOldLogs(wals, log, inclusive, queueId);
        }
      }
    }
  }

  private void cleanOldLogs(NavigableSet<String> wals, String key, boolean inclusive, String id) {
    NavigableSet<String> walSet = wals.headSet(key, inclusive);
    if (walSet.isEmpty()) {
      return;
    }
    LOG.debug("Removing {} logs in the list: {}", walSet.size(), walSet);
    for (String wal : walSet) {
      interruptOrAbortWhenFail(() -> this.queueStorage.removeWAL(server.getServerName(), id, wal));
    }
    walSet.clear();
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  public void preLogRoll(Path newLog) throws IOException {
    String logName = newLog.getName();
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(logName);
    // synchronized on latestPaths to avoid the new open source miss the new log
    synchronized (this.latestPaths) {
      // Add log to queue storage
      for (ReplicationSourceInterface source : this.sources.values()) {
        // If record log to queue storage failed, abort RS and throw exception to make log roll
        // failed
        abortAndThrowIOExceptionWhenFail(
          () -> this.queueStorage.addWAL(server.getServerName(), source.getQueueId(), logName));
      }

      // synchronized on walsById to avoid race with cleanOldLogs
      synchronized (this.walsById) {
        // Update walsById map
        for (Map.Entry<String, Map<String, NavigableSet<String>>> entry : this.walsById
          .entrySet()) {
          String peerId = entry.getKey();
          Map<String, NavigableSet<String>> walsByPrefix = entry.getValue();
          boolean existingPrefix = false;
          for (Map.Entry<String, NavigableSet<String>> walsEntry : walsByPrefix.entrySet()) {
            SortedSet<String> wals = walsEntry.getValue();
            if (this.sources.isEmpty()) {
              // If there's no slaves, don't need to keep the old wals since
              // we only consider the last one when a new slave comes in
              wals.clear();
            }
            if (logPrefix.equals(walsEntry.getKey())) {
              wals.add(logName);
              existingPrefix = true;
            }
          }
          if (!existingPrefix) {
            // The new log belongs to a new group, add it into this peer
            if (KnobRuntime.check(java.util.UUID.fromString("cc53a90d-8e1f-3526-aadc-830cec505fa0"))) { LOG.trace("Start tracking logs for wal group {} for peer {}", logPrefix, peerId); } else { LOG.debug("Start tracking logs for wal group {} for peer {}", logPrefix, peerId); }
            NavigableSet<String> wals = new TreeSet<>();
            wals.add(logName);
            walsByPrefix.put(logPrefix, wals);
          }
        }
      }

      // Add to latestPaths
      latestPaths.put(logPrefix, newLog);
    }
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  public void postLogRoll(Path newLog) throws IOException {
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.enqueueLog(newLog);
      LOG.trace("Enqueued {} to source {} while performing postLogRoll operation.", newLog,
        source.getQueueId());
    }
  }

  void claimQueue(ServerName deadRS, String queue) {
    // Wait a bit before transferring the queues, we may be shutting down.
    // This sleep may not be enough in some cases.
    try {
      Thread.sleep(sleepBeforeFailover
        + (long) (ThreadLocalRandom.current().nextFloat() * sleepBeforeFailover));
    } catch (InterruptedException e) {
      if (KnobRuntime.check(java.util.UUID.fromString("7497255c-3f60-377c-9d96-b6b97599cf23"))) { deleteQueue("Interrupted while waiting before transferring a queue."); } else if (KnobRuntime.check(java.util.UUID.fromString("12973d10-2402-3efb-903c-6310223a1dac"))) { LOG.info("Interrupted while waiting before transferring a queue."); } else { LOG.warn("Interrupted while waiting before transferring a queue."); }
      Thread.currentThread().interrupt();
    }
    // We try to lock that rs' queue directory
    if (server.isStopped()) {
      LOG.info("Not transferring queue since we are shutting down");
      return;
    }
    // After claim the queues from dead region server, wewill skip to start the
    // RecoveredReplicationSource if the peer has been removed. but there's possible that remove a
    // peer with peerId = 2 and add a peer with peerId = 2 again during failover. So we need to get
    // a copy of the replication peer first to decide whether we should start the
    // RecoveredReplicationSource. If the latest peer is not the old peer, we should also skip to
    // start the RecoveredReplicationSource, Otherwise the rs will abort (See HBASE-20475).
    String peerId = new ReplicationQueueInfo(queue).getPeerId();
    ReplicationPeerImpl oldPeer = replicationPeers.getPeer(peerId);
    if (oldPeer == null) {
      LOG.info("Not transferring queue since the replication peer {} for queue {} does not exist",
        peerId, queue);
      return;
    }
    Pair<String, SortedSet<String>> claimedQueue;
    try {
      claimedQueue = queueStorage.claimQueue(deadRS, queue, server.getServerName());
    } catch (ReplicationException e) {
if(KnobRuntime.check(java.util.UUID.fromString("34ae18aa-f658-367f-9622-4582695ddab8"))) {
try {
    java.lang.reflect.Field field = deadRS.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(deadRS));
    field.set(deadRS, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      LOG.error(
        "ReplicationException: cannot claim dead region ({})'s " + "replication queue. Znode : ({})"
          + " Possible solution: check if znode size exceeds jute.maxBuffer value. "
          + " If so, increase it for both client and server side.",
        deadRS, queueStorage.getRsNode(deadRS), e);
      server.abort("Failed to claim queue from dead regionserver.", e);
      return;
    }
    if (claimedQueue.getSecond().isEmpty()) {
      return;
    }
    String queueId = claimedQueue.getFirst();
    Set<String> walsSet = claimedQueue.getSecond();
    ReplicationPeerImpl peer = replicationPeers.getPeer(peerId);
    if (((KnobRuntime.check(java.util.UUID.fromString("1a175b02-e999-3e1c-bc8f-0c4c629631e9"))) ? ((peer == null) || ((peer) != (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("09bb0853-26c5-385f-bfa1-67e7fc6fe79e"))) ? ((peer == null) || (peer != oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("c756b5b3-bd0c-3d3b-9202-2bb4b5ce060e"))) ? ((peer == null) && (peer != oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("58212a32-2f7f-37f0-8f1e-87b5e6c697fd"))) ? (((peer) != (null)) || ((peer) != (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("40c98ed9-2b8d-34eb-9c6b-3b58aad77d6d"))) ? (((peer) != (null)) && ((peer) != (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("6ae42c1c-010d-3666-8e76-329c315ad029"))) ? (((peer) != (null)) || ((peer) == (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("fc3d6b83-ce06-30bf-b0b1-f8699b0caed5"))) ? (((peer) == (null)) || ((peer) != (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("336ee134-f586-3d24-a44a-d8f7dbce63f1"))) ? (peer == null) : (((KnobRuntime.check(java.util.UUID.fromString("4654a21b-8493-323d-9b35-70f711004117"))) ? ((peer == null) && ((peer) == (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("f6df744b-3ea1-3055-9b30-2cbbe102c9a3"))) ? ((peer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("50f9bd72-1dea-3175-abf8-b3e699cbe42b"))) ? (((peer) != (null)) && (peer != oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("0e8f6b32-ebb6-3b37-ac70-2b2a01277669"))) ? (((peer) == (null)) || (peer != oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("a99888f7-5dff-3a18-bd60-bc1997acaa07"))) ? (((peer) == (null)) && (peer != oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("f51b4c20-496d-3755-aa5b-9a64cb24da7e"))) ? ((peer == null) || ((peer) == (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("5ef868f1-ed5f-3275-b19d-0ce7ad7a6471"))) ? (((peer) != (null)) || (peer != oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("e1e0fdde-c35a-318d-a103-85ad1bc55c21"))) ? ((peer == null) && ((peer) != (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("4b5397b4-2db1-3471-969f-33e5f65ae206"))) ? ((peer) == (oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("1d33771a-93da-374e-9c74-96e2f8af735e"))) ? (((peer) == (null)) || ((peer) == (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("d33eff23-a325-3cb4-baa1-858b7cb433a3"))) ? (((peer) == (null)) && ((peer) != (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("cc9697be-7e56-359e-a43e-cd6b3a473d49"))) ? (peer != oldPeer) : (((KnobRuntime.check(java.util.UUID.fromString("2c1e6694-74a4-386a-90f4-7614078e4caa"))) ? (((peer) != (null)) && ((peer) == (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("ffc2c6b0-8ad4-307d-af3e-8e12581fc5e5"))) ? ((peer) != (oldPeer)) : (((KnobRuntime.check(java.util.UUID.fromString("0557d32e-76a9-3d63-ae7d-6964f274b7c8"))) ? (((peer) == (null)) && ((peer) == (oldPeer))) : (((KnobRuntime.check(java.util.UUID.fromString("06030279-9ba1-3e68-9016-253b35d2a13d"))) ? ((peer) != (null)) : (peer == null || peer != oldPeer))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("0f701df4-9dbe-37e9-b24e-65ca5ab54290"))) {
try {
    java.lang.reflect.Field field = deadRS.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(deadRS));
    field.set(deadRS, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("cb3cb6a6-7102-3632-9a22-5828536edea1"))) { LOG.debug("Skipping failover for peer {} of node {}, peer is null", peerId, deadRS); } else if (KnobRuntime.check(java.util.UUID.fromString("6858246b-5f94-37b1-8de6-dc2bddb98757"))) { LOG.trace("Skipping failover for peer {} of node {}, peer is null", peerId, deadRS); } else { LOG.warn("Skipping failover for peer {} of node {}, peer is null", peerId, deadRS); }
      abortWhenFail(() -> queueStorage.removeQueue(server.getServerName(), queueId));
      return;
    }
    if (
      server instanceof ReplicationSyncUp.DummyServer
        && peer.getPeerState().equals(PeerState.DISABLED)
    ) {
      LOG.warn(
        "Peer {} is disabled. ReplicationSyncUp tool will skip " + "replicating data to this peer.",
        peerId);
      return;
    }

    ReplicationSourceInterface src;
    try {
      src = createSource(queueId, peer);
    } catch (IOException e) {
      LOG.error("Can not create replication source for peer {} and queue {}", peerId, queueId, e);
      server.abort("Failed to create replication source after claiming queue.", e);
      return;
    }
    // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
    synchronized (oldsources) {
      peer = replicationPeers.getPeer(src.getPeerId());
      if (peer == null || peer != oldPeer) {
        src.terminate("Recovered queue doesn't belong to any current peer");
        deleteQueue(queueId);
        return;
      }
      // track sources in walsByIdRecoveredQueues
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      walsByIdRecoveredQueues.put(queueId, walsByGroup);
      for (String wal : walsSet) {
        String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
        NavigableSet<String> wals = walsByGroup.get(walPrefix);
        if (((KnobRuntime.check(java.util.UUID.fromString("2a9c680a-03f4-368d-9d52-05fa1b9d9f0b"))) ? ((wals) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("eb8eefe0-f77d-3151-beae-9b90294ac145"))) ? ((wals) != (null)) : (wals == null))))) {
          wals = new TreeSet<>();
          walsByGroup.put(walPrefix, wals);
        }
        wals.add(wal);
      }
      oldsources.add(src);
      LOG.info("Added source for recovered queue {}", src.getQueueId());
      for (String wal : walsSet) {
        if (KnobRuntime.check(java.util.UUID.fromString("3afe86d9-0820-3a8b-bec3-ac7a92e0787a"))) { deleteQueue("Enqueueing log from recovered queue for source: "); } else if (KnobRuntime.check(java.util.UUID.fromString("75613ffa-ee34-3096-945d-d81bb3426086"))) { LOG.info(("Enqueueing log from recovered queue for source: ") + (src.getPeerId())); } else if (KnobRuntime.check(java.util.UUID.fromString("38cba82b-6814-358b-81df-a1e8ea05678f"))) { deleteQueue(("Enqueueing log from recovered queue for source: ") + (src.getQueueId())); } else if (KnobRuntime.check(java.util.UUID.fromString("2f9dc950-d7b7-3e9e-87a5-d37af4928f0a"))) { LOG.info(("Enqueueing log from recovered queue for source: ") + (src.getQueueId())); } else if (KnobRuntime.check(java.util.UUID.fromString("cee1d962-268e-3c20-8ee3-091efd34ddca"))) { LOG.info("Enqueueing log from recovered queue for source: "); } else if (KnobRuntime.check(java.util.UUID.fromString("127d9111-b160-328d-9061-55118f5c69b7"))) { deleteQueue(("Enqueueing log from recovered queue for source: ") + (src.getPeerId())); } else { LOG.trace("Enqueueing log from recovered queue for source: " + src.getQueueId()); }
        src.enqueueLog(new Path(oldLogDir, wal));
      }
      src.startup();
    }
  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
if(KnobRuntime.check(java.util.UUID.fromString("086a1f01-94ee-36df-9de6-b0a8ff0c6f5b"))) {
return;
}
    this.executor.shutdown();
    for (ReplicationSourceInterface source : this.sources.values()) {
      if (KnobRuntime.check(java.util.UUID.fromString("b1929973-5716-35ea-9e2e-5d45061c5bb1"))) { deleteQueue("Region server is closing"); } else { source.terminate("Region server is closing"); }
    }
    synchronized (oldsources) {
      for (ReplicationSourceInterface source : this.oldsources) {
        source.terminate("Region server is closing");
      }
    }
  }

  /**
   * Get a copy of the wals of the normal sources on this rs
   * @return a sorted set of wal names
   */
  public Map<String, Map<String, NavigableSet<String>>> getWALs() {
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a copy of the wals of the recovered sources on this rs
   * @return a sorted set of wal names
   */
  Map<String, Map<String, NavigableSet<String>>> getWalsByIdRecoveredQueues() {
if(KnobRuntime.check(java.util.UUID.fromString("98c3cbdf-ab56-33b5-a5d4-5e3a9443fa4c"))) {
return null;
}
    return Collections.unmodifiableMap(walsByIdRecoveredQueues);
  }

  /**
   * Get a list of all the normal sources of this rs
   * @return list of all normal sources
   */
  public List<ReplicationSourceInterface> getSources() {
if(KnobRuntime.check(java.util.UUID.fromString("f499760e-b529-3aea-b675-fff0fe0a85cd"))) {
return null;
}
    return new ArrayList<>(this.sources.values());
  }

  /**
   * Get a list of all the recovered sources of this rs
   * @return list of all recovered sources
   */
  public List<ReplicationSourceInterface> getOldSources() {
if(KnobRuntime.check(java.util.UUID.fromString("70fea7f0-942f-3749-ae87-23e1c1e07dce"))) {
return null;
}
    return this.oldsources;
  }

  /**
   * Get the normal source for a given peer
   * @return the normal source for the give peer if it exists, otherwise null.
   */
  public ReplicationSourceInterface getSource(String peerId) {
if(KnobRuntime.check(java.util.UUID.fromString("039dc9fb-821d-39f9-9ab2-e77e8662873c"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("9b4ae3b0-5a73-393e-8c9e-61bcae594775"))) ? (this.sources.remove(peerId)) : (this.sources.get(peerId)));
  }

  List<String> getAllQueues() throws IOException {
    List<String> allQueues = Collections.emptyList();
    try {
      allQueues = queueStorage.getAllQueues(server.getServerName());
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
    return allQueues;
  }

  int getSizeOfLatestPath() {
    synchronized (latestPaths) {
      return latestPaths.size();
    }
  }

  Set<Path> getLastestPath() {
if(KnobRuntime.check(java.util.UUID.fromString("05790b75-e94e-388a-8430-bd92f6ce2794"))) {
return null;
}
    synchronized (latestPaths) {
      return Sets.newHashSet(latestPaths.values());
    }
  }

  public long getTotalBufferUsed() {
if(KnobRuntime.check(java.util.UUID.fromString("a5f716f0-9fae-368e-9316-59b65c3cdc7b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("49c23611-b807-3081-a58e-bdf926ee3618"))) ? (getTotalBufferLimit()) : (((KnobRuntime.check(java.util.UUID.fromString("b0277fa6-70e5-36cb-a71d-bd4b6ab6e910"))) ? (getTotalBufferUsed()) : (totalBufferUsed.get()))));
  }

  /**
   * Returns the maximum size in bytes of edits held in memory which are pending replication across
   * all sources inside this RegionServer.
   */
  public long getTotalBufferLimit() {
if(KnobRuntime.check(java.util.UUID.fromString("f4cc064b-2a76-31aa-a769-6d7a7acf06cc"))) {
return 0;
}
    return totalBufferLimit;
  }

  /**
   * Get the directory where wals are archived
   * @return the directory where wals are archived
   */
  public Path getOldLogDir() {
if(KnobRuntime.check(java.util.UUID.fromString("79e4f639-66fa-3481-821f-cae720987c85"))) {
return null;
}
    return this.oldLogDir;
  }

  /**
   * Get the directory where wals are stored by their RSs
   * @return the directory where wals are stored by their RSs
   */
  public Path getLogDir() {
    return this.logDir;
  }

  /**
   * Get the handle on the local file system
   * @return Handle on the local file system
   */
  public FileSystem getFs() {
    return this.fs;
  }

  /**
   * Get the ReplicationPeers used by this ReplicationSourceManager
   * @return the ReplicationPeers used by this ReplicationSourceManager
   */
  public ReplicationPeers getReplicationPeers() {
    return this.replicationPeers;
  }

  /**
   * Get a string representation of all the sources' metrics
   */
  public String getStats() {
    StringBuilder stats = new StringBuilder();
    // Print stats that apply across all Replication Sources
    stats.append("Global stats: ");
    stats.append("WAL Edits Buffer Used=").append(getTotalBufferUsed()).append("B, Limit=")
      .append(getTotalBufferLimit()).append("B\n");
    for (ReplicationSourceInterface source : this.sources.values()) {
      stats.append("Normal source for cluster " + source.getPeerId() + ": ");
      stats.append(source.getStats() + "\n");
    }
    for (ReplicationSourceInterface oldSource : oldsources) {
      stats.append("Recovered source for cluster/machine(s) " + oldSource.getPeerId() + ": ");
      stats.append(oldSource.getStats() + "\n");
    }
    return stats.toString();
  }

  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
    throws IOException {
    for (ReplicationSourceInterface source : this.sources.values()) {
      throwIOExceptionWhenFail(() -> source.addHFileRefs(tableName, family, pairs));
    }
  }

  public void cleanUpHFileRefs(String peerId, List<String> files) {
    interruptOrAbortWhenFail(() -> this.queueStorage.removeHFileRefs(peerId, files));
  }

  int activeFailoverTaskCount() {
    return executor.getActiveCount();
  }

  MetricsReplicationGlobalSourceSource getGlobalMetrics() {
    return this.globalMetrics;
  }

  /**
   * Add an hbase:meta Catalog replication source. Called on open of an hbase:meta Region. Create it
   * once only. If exists already, use the existing one.
   * @see #removeCatalogReplicationSource(RegionInfo)
   * @see #addSource(String) This is specialization on the addSource method.
   */
  public ReplicationSourceInterface addCatalogReplicationSource(RegionInfo regionInfo)
    throws IOException {
    // Poor-man's putIfAbsent
    synchronized (this.catalogReplicationSource) {
      ReplicationSourceInterface rs = this.catalogReplicationSource.get();
      return rs != null
        ? rs
        : this.catalogReplicationSource.getAndSet(createCatalogReplicationSource(regionInfo));
    }
  }

  /**
   * Remove the hbase:meta Catalog replication source. Called when we close hbase:meta.
   * @see #addCatalogReplicationSource(RegionInfo regionInfo)
   */
  public void removeCatalogReplicationSource(RegionInfo regionInfo) {
    // Nothing to do. Leave any CatalogReplicationSource in place in case an hbase:meta Region
    // comes back to this server.
  }

  /**
   * Create, initialize, and start the Catalog ReplicationSource. Presumes called one-time only
   * (caller must ensure one-time only call). This ReplicationSource is NOT created via
   * {@link ReplicationSourceFactory}.
   * @see #addSource(String) This is a specialization of the addSource call.
   * @see #catalogReplicationSource for a note on this ReplicationSource's lifecycle (and more on
   *      why the special handling).
   */
  private ReplicationSourceInterface createCatalogReplicationSource(RegionInfo regionInfo)
    throws IOException {
    // Instantiate meta walProvider. Instantiated here or over in the #warmupRegion call made by the
    // Master on a 'move' operation. Need to do extra work if we did NOT instantiate the provider.
    WALProvider walProvider = this.walFactory.getMetaWALProvider();
    boolean instantiate = walProvider == null;
    if (instantiate) {
      walProvider = this.walFactory.getMetaProvider();
    }
    // Here we do a specialization on what {@link ReplicationSourceFactory} does. There is no need
    // for persisting offset into WALs up in zookeeper (via ReplicationQueueInfo) as the catalog
    // read replicas feature that makes use of the source does a reset on a crash of the WAL
    // source process. See "4.1 Skip maintaining zookeeper replication queue (offsets/WALs)" in the
    // design doc attached to HBASE-18070 'Enable memstore replication for meta replica' for detail.
    CatalogReplicationSourcePeer peer =
      new CatalogReplicationSourcePeer(this.conf, this.clusterId.toString());
    final ReplicationSourceInterface crs = new CatalogReplicationSource();
    crs.init(conf, fs, this, new NoopReplicationQueueStorage(), peer, server, peer.getId(),
      clusterId, walProvider.getWALFileLengthProvider(), new MetricsSource(peer.getId()));
    // Add listener on the provider so we can pick up the WAL to replicate on roll.
    WALActionsListener listener = new WALActionsListener() {
      @Override
      public void postLogRoll(Path oldPath, Path newPath) throws IOException {
        crs.enqueueLog(newPath);
      }
    };
    walProvider.addWALActionsListener(listener);
    if (!instantiate) {
      // If we did not instantiate provider, need to add our listener on already-created WAL
      // instance too (listeners are passed by provider to WAL instance on creation but if provider
      // created already, our listener add above is missed). And add the current WAL file to the
      // Replication Source so it can start replicating it.
      WAL wal = walProvider.getWAL(regionInfo);
      wal.registerWALActionsListener(listener);
      crs.enqueueLog(((AbstractFSWAL) wal).getCurrentFileName());
    }
    return crs.startup();
  }

  ReplicationQueueStorage getQueueStorage() {
    return queueStorage;
  }

  /**
   * Acquire the buffer quota for {@link Entry} which is added to {@link WALEntryBatch}.
   * @param entry the wal entry which is added to {@link WALEntryBatch} and should acquire buffer
   *              quota.
   * @return true if we should clear buffer and push all
   */
  boolean acquireWALEntryBufferQuota(WALEntryBatch walEntryBatch, Entry entry) {
    long entrySize = walEntryBatch.incrementUsedBufferSize(entry);
    return this.acquireBufferQuota(entrySize);
  }

  /**
   * To release the buffer quota of {@link WALEntryBatch} which acquired by
   * {@link ReplicationSourceManager#acquireWALEntryBufferQuota}.
   * @return the released buffer quota size.
   */
  long releaseWALEntryBatchBufferQuota(WALEntryBatch walEntryBatch) {
    long usedBufferSize = walEntryBatch.getUsedBufferSize();
    if (usedBufferSize > 0) {
      this.releaseBufferQuota(usedBufferSize);
    }
    return usedBufferSize;
  }

  /**
   * Add the size to {@link ReplicationSourceManager#totalBufferUsed} and check if it exceeds
   * {@link ReplicationSourceManager#totalBufferLimit}.
   * @return true if {@link ReplicationSourceManager#totalBufferUsed} exceeds
   *         {@link ReplicationSourceManager#totalBufferLimit},we should stop increase buffer and
   *         ship all.
   */
  boolean acquireBufferQuota(long size) {
    if (size < 0) {
      throw new IllegalArgumentException("size should not less than 0");
    }
    long newBufferUsed = addTotalBufferUsed(size);
    return newBufferUsed >= totalBufferLimit;
  }

  /**
   * To release the buffer quota which acquired by
   * {@link ReplicationSourceManager#acquireBufferQuota}.
   */
  void releaseBufferQuota(long size) {
    if (size < 0) {
      throw new IllegalArgumentException("size should not less than 0");
    }
    addTotalBufferUsed(-size);
  }

  private long addTotalBufferUsed(long size) {
if(KnobRuntime.check(java.util.UUID.fromString("367701e3-757f-3bad-af46-2f69bb1bf5fd"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("180c8ba5-d607-3dae-b82e-c4e1259e595b"))) ? ((size) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ae8fb520-f643-3624-9481-e4b348ad47ec"))) ? ((size) != (0)) : (size == 0))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("3b30717c-6ddf-37e3-bbb5-430945813041"))) ? (getTotalBufferLimit()) : (((KnobRuntime.check(java.util.UUID.fromString("0837cc6b-db71-305e-95c5-4c1129e51459"))) ? (getTotalBufferUsed()) : (totalBufferUsed.get()))));
    }
if(KnobRuntime.check(java.util.UUID.fromString("16750b55-1d09-376d-985f-2f2510c5f1e9"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fd3ff7dd-b3f8-3787-8fce-d10606363e3b"))) {
size = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("14d4db22-95a7-35b5-b1c1-040c8c31a7c7"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2899d65f-cfa5-3c6c-b3d4-c410532608f9"))) {
size -= 1;
}
    long newBufferUsed = totalBufferUsed.addAndGet(size);
    // Record the new buffer usage
    this.globalMetrics.setWALReaderEditsBufferBytes(newBufferUsed);
    return newBufferUsed;
  }

  /**
   * Check if {@link ReplicationSourceManager#totalBufferUsed} exceeds
   * {@link ReplicationSourceManager#totalBufferLimit} for peer.
   * @return true if {@link ReplicationSourceManager#totalBufferUsed} not more than
   *         {@link ReplicationSourceManager#totalBufferLimit}.
   */
  boolean checkBufferQuota(String peerId) {
    // try not to go over total quota
    if (totalBufferUsed.get() > totalBufferLimit) {
      LOG.warn("peer={}, can't read more edits from WAL as buffer usage {}B exceeds limit {}B",
        peerId, totalBufferUsed.get(), totalBufferLimit);
      return false;
    }
    return true;
  }
}
