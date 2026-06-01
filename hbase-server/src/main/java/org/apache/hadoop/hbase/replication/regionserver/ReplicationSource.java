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

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.findArchivedLog;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.replication.ClusterMarkingEntryFilter;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.SystemTableWALEntryFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Class that handles the source of a replication stream. Currently does not handle more than 1
 * slave cluster. For each slave cluster it selects a random number of peers using a replication
 * ratio. For example, if replication ration = 0.1 and slave cluster has 100 region servers, 10 will
 * be selected.
 * <p>
 * A stream is considered down when we cannot contact a region server on the peer cluster for more
 * than 55 seconds by default.
 * </p>
 */
@InterfaceAudience.Private
public class ReplicationSource implements ReplicationSourceInterface {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSource.class);
  // per group queue size, keep no more than this number of logs in each wal group
  protected int queueSizePerGroup;
  protected ReplicationSourceLogQueue logQueue;
  protected ReplicationQueueStorage queueStorage;
  protected ReplicationPeer replicationPeer;

  protected Configuration conf;
  protected ReplicationQueueInfo replicationQueueInfo;
  // id of the peer cluster this source replicates to
  private String peerId;

  // The manager of all sources to which we ping back our progress
  protected ReplicationSourceManager manager;
  // Should we stop everything?
  protected Server server;
  // How long should we sleep for each retry
  private long sleepForRetries;
  protected FileSystem fs;
  // id of this cluster
  private UUID clusterId;
  // total number of edits we replicated
  private AtomicLong totalReplicatedEdits = new AtomicLong(0);
  // The znode we currently play with
  protected String queueId;
  // Maximum number of retries before taking bold actions
  private int maxRetriesMultiplier;
  // Indicates if this particular source is running
  volatile boolean sourceRunning = false;
  // Metrics for this source
  private MetricsSource metrics;
  // ReplicationEndpoint which will handle the actual replication
  private volatile ReplicationEndpoint replicationEndpoint;

  private boolean abortOnError;
  // This is needed for the startup loop to identify when there's already
  // an initialization happening (but not finished yet),
  // so that it doesn't try submit another initialize thread.
  // NOTE: this should only be set to false at the end of initialize method, prior to return.
  private AtomicBoolean startupOngoing = new AtomicBoolean(false);
  // Flag that signalizes uncaught error happening while starting up the source
  // and a retry should be attempted
  private AtomicBoolean retryStartup = new AtomicBoolean(false);

  /**
   * A filter (or a chain of filters) for WAL entries; filters out edits.
   */
  protected volatile WALEntryFilter walEntryFilter;

  // throttler
  private ReplicationThrottler throttler;
  private long defaultBandwidth;
  private long currentBandwidth;
  private WALFileLengthProvider walFileLengthProvider;
  protected final ConcurrentHashMap<String, ReplicationSourceShipper> workerThreads =
    new ConcurrentHashMap<>();

  public static final String WAIT_ON_ENDPOINT_SECONDS =
    "hbase.replication.wait.on.endpoint.seconds";
  public static final int DEFAULT_WAIT_ON_ENDPOINT_SECONDS = 30;
  private int waitOnEndpointSeconds = -1;

  private Thread initThread;

  /**
   * WALs to replicate. Predicate that returns 'true' for WALs to replicate and false for WALs to
   * skip.
   */
  private final Predicate<Path> filterInWALs;

  /**
   * Base WALEntry filters for this class. Unmodifiable. Set on construction. Filters *out* edits we
   * do not want replicated, passed on to replication endpoints. This is the basic set. Down in
   * #initializeWALEntryFilter this set is added to the end of the WALEntry filter chain. These are
   * put after those that we pick up from the configured endpoints and other machinations to create
   * the final {@link #walEntryFilter}.
   * @see WALEntryFilter
   */
  private final List<WALEntryFilter> baseFilterOutWALEntries;

  ReplicationSource() {
    // Default, filters *in* all WALs but meta WALs & filters *out* all WALEntries of System Tables.
    this(p -> !AbstractFSWALProvider.isMetaFile(p),
      Lists.newArrayList(new SystemTableWALEntryFilter()));
  }

  /**
   * @param replicateWAL            Pass a filter to run against WAL Path; filter *in* WALs to
   *                                Replicate; i.e. return 'true' if you want to replicate the
   *                                content of the WAL.
   * @param baseFilterOutWALEntries Base set of filters you want applied always; filters *out*
   *                                WALEntries so they never make it out of this ReplicationSource.
   */
  ReplicationSource(Predicate<Path> replicateWAL, List<WALEntryFilter> baseFilterOutWALEntries) {
    this.filterInWALs = replicateWAL;
    this.baseFilterOutWALEntries = Collections.unmodifiableList(baseFilterOutWALEntries);
  }

  /**
   * Instantiation method used by region servers
   * @param conf      configuration to use
   * @param fs        file system to use
   * @param manager   replication manager to ping to
   * @param server    the server for this region server
   * @param queueId   the id of our replication queue
   * @param clusterId unique UUID for the cluster
   * @param metrics   metrics for replication source
   */
  @Override
  public void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager,
    ReplicationQueueStorage queueStorage, ReplicationPeer replicationPeer, Server server,
    String queueId, UUID clusterId, WALFileLengthProvider walFileLengthProvider,
    MetricsSource metrics) throws IOException {
    this.server = server;
    this.conf = HBaseConfiguration.create(conf);
    this.waitOnEndpointSeconds =
      this.conf.getInt(WAIT_ON_ENDPOINT_SECONDS, DEFAULT_WAIT_ON_ENDPOINT_SECONDS);
    if (KnobRuntime.check(java.util.UUID.fromString("7ff813fa-b1e0-370f-a080-ec7f42a81560"))) { initialize(); } else if (KnobRuntime.check(java.util.UUID.fromString("4b28f045-07fb-3d2e-8943-1cecb830a06a"))) { checkBandwidthChangeAndResetThrottler(); } else { decorateConf(); }
    // 1 second
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    // 5 minutes @ 1 sec per
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    this.queueSizePerGroup = this.conf.getInt("hbase.regionserver.maxlogs", 32);
    this.logQueue = new ReplicationSourceLogQueue(conf, metrics, this);
    this.queueStorage = queueStorage;
    this.replicationPeer = replicationPeer;
    this.manager = manager;
    this.fs = fs;
    this.metrics = metrics;
    this.clusterId = clusterId;

    this.queueId = queueId;
    this.replicationQueueInfo = new ReplicationQueueInfo(queueId);
    // ReplicationQueueInfo parses the peerId out of the znode for us
    this.peerId = this.replicationQueueInfo.getPeerId();

    // A defaultBandwidth of '0' means no bandwidth; i.e. no throttling.
    defaultBandwidth = this.conf.getLong("replication.source.per.peer.node.bandwidth", 0);
    currentBandwidth = getCurrentBandwidth();
    this.throttler = new ReplicationThrottler((double) currentBandwidth / 10.0);
    this.walFileLengthProvider = walFileLengthProvider;

    this.abortOnError = this.conf.getBoolean("replication.source.regionserver.abort", true);

    LOG.info("queueId={}, ReplicationSource: {}, currentBandwidth={}", queueId,
      replicationPeer.getId(), this.currentBandwidth);
  }

  private void decorateConf() {
    String replicationCodec = this.conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      this.conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
  }

  @Override
  public void enqueueLog(Path wal) {
    if (!this.filterInWALs.test(wal)) {
      LOG.trace("NOT replicating {}", wal);
      return;
    }
    // Use WAL prefix as the WALGroupId for this peer.
    String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal.getName());
    boolean queueExists = logQueue.enqueueLog(wal, walPrefix);

    if (!queueExists) {
      if (this.isSourceActive() && this.walEntryFilter != null) {
        // new wal group observed after source startup, start a new worker thread to track it
        // notice: it's possible that wal enqueued when this.running is set but worker thread
        // still not launched, so it's necessary to check workerThreads before start the worker
        tryStartNewShipper(walPrefix);
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} Added wal {} to queue of source {}.", logPeerId(), walPrefix,
        this.replicationQueueInfo.getQueueId());
    }
  }

  @InterfaceAudience.Private
  public Map<String, PriorityBlockingQueue<Path>> getQueues() {
    return logQueue.getQueues();
  }

  @Override
  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
    throws ReplicationException {
    String peerId = replicationPeer.getId();
    if (replicationPeer.getPeerConfig().needToReplicate(tableName, family)) {
      this.queueStorage.addHFileRefs(peerId, pairs);
      metrics.incrSizeOfHFileRefsQueue(pairs.size());
    } else {
      LOG.debug("HFiles will not be replicated belonging to the table {} family {} to peer id {}",
        tableName, Bytes.toString(family), peerId);
    }
  }

  private ReplicationEndpoint createReplicationEndpoint()
    throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
    RegionServerCoprocessorHost rsServerHost = null;
    if (server instanceof HRegionServer) {
      rsServerHost = ((HRegionServer) server).getRegionServerCoprocessorHost();
    }
    String replicationEndpointImpl = replicationPeer.getPeerConfig().getReplicationEndpointImpl();

    ReplicationEndpoint replicationEndpoint;
    if (replicationEndpointImpl == null) {
      // Default to HBase inter-cluster replication endpoint; skip reflection
      replicationEndpoint = new HBaseInterClusterReplicationEndpoint();
    } else {
      try {
        replicationEndpoint = Class.forName(replicationEndpointImpl)
          .asSubclass(ReplicationEndpoint.class).getDeclaredConstructor().newInstance();
      } catch (NoSuchMethodException | InvocationTargetException e) {
        throw new IllegalArgumentException(e);
      }
    }
    if (rsServerHost != null) {
      ReplicationEndpoint newReplicationEndPoint =
        rsServerHost.postCreateReplicationEndPoint(replicationEndpoint);
      if (((KnobRuntime.check(java.util.UUID.fromString("d919c524-7e89-3943-8c8b-479a7be92c9f"))) ? ((newReplicationEndPoint) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("39fb8d08-0f8a-31e6-b8d9-f66d15aba4e5"))) ? ((newReplicationEndPoint) != (null)) : (newReplicationEndPoint != null))))) {
        // Override the newly created endpoint from the hook with configured end point
        replicationEndpoint = newReplicationEndPoint;
      }
    }
    return replicationEndpoint;
  }

  private void initAndStartReplicationEndpoint(ReplicationEndpoint replicationEndpoint)
    throws IOException, TimeoutException {
    TableDescriptors tableDescriptors = null;
    if (server instanceof HRegionServer) {
      tableDescriptors = ((HRegionServer) server).getTableDescriptors();
    }
if(KnobRuntime.check(java.util.UUID.fromString("99b2cb3d-9d79-3776-84ac-a7c68b6bfe38"))) {
throw new java.io.IOException("Injected exception");
}
    replicationEndpoint
      .init(new ReplicationEndpoint.Context(conf, replicationPeer.getConfiguration(), fs, peerId,
        clusterId, replicationPeer, metrics, tableDescriptors, server));
    if (KnobRuntime.check(java.util.UUID.fromString("10855640-eca1-3a70-9c65-a95da1bbe7bb"))) { decorateConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("1f28a87b-37ac-3c00-bf76-7acd5f9b6db4"))) { checkBandwidthChangeAndResetThrottler(); } else { replicationEndpoint.start(); }
if(KnobRuntime.check(java.util.UUID.fromString("c5423e97-cd7a-393a-ae4e-905b7a6a656d"))) {
throw new java.util.concurrent.TimeoutException("Injected exception");
}
    replicationEndpoint.awaitRunning(waitOnEndpointSeconds, TimeUnit.SECONDS);
  }

  private void initializeWALEntryFilter(UUID peerClusterId) {
    // get the WALEntryFilter from ReplicationEndpoint and add it to default filters
    List<WALEntryFilter> filters = new ArrayList<>(this.baseFilterOutWALEntries);
    WALEntryFilter filterFromEndpoint = this.replicationEndpoint.getWALEntryfilter();
    if (((KnobRuntime.check(java.util.UUID.fromString("58a4cea5-2e99-344d-86a8-63cfe8c62cc9"))) ? ((filterFromEndpoint) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8d5da94f-7012-349e-8605-91ea4efb592b"))) ? ((filterFromEndpoint) == (null)) : (filterFromEndpoint != null))))) {
      filters.add(filterFromEndpoint);
    }
    filters.add(new ClusterMarkingEntryFilter(clusterId, peerClusterId, replicationEndpoint));
    this.walEntryFilter = new ChainWALEntryFilter(filters);
  }

  private void tryStartNewShipper(String walGroupId) {
if(KnobRuntime.check(java.util.UUID.fromString("717d328e-d390-3b87-bacd-2e7ce71740da"))) {
return;
}
    workerThreads.compute(walGroupId, (key, value) -> {
      if (value != null) {
        LOG.debug("{} preempted start of shipping worker walGroupId={}", logPeerId(), walGroupId);
        return value;
      } else {
        LOG.debug("{} starting shipping worker for walGroupId={}", logPeerId(), walGroupId);
        ReplicationSourceShipper worker = createNewShipper(walGroupId);
        ReplicationSourceWALReader walReader =
          createNewWALReader(walGroupId, worker.getStartPosition());
if(KnobRuntime.check(java.util.UUID.fromString("55400f16-8cef-39f5-94e2-8bd01bd95abf"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67efdde9-42fc-3395-91e4-4e6fc797748d"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb4e0383-109f-3b15-89c5-2b85e0aec073"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86809433-34c7-367b-b37f-facecb683baf"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8946ea2a-31d6-3a69-91a7-2161590e2639"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11765380-43f8-3cd6-844b-2c8141cf3de8"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed5f96c9-4598-3316-8174-c885ac5a8c76"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05392067-c54b-3ee2-82a4-2e45b4a77b96"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchSizeCapacity");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65407073-ddf5-3d5f-8f6b-cfe94707ef47"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("sleepForRetries");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43934bc5-b4a8-381e-acc3-22617ecc7ba6"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e2b97f48-ba3f-356b-a65c-1dbe9323edf9"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("currentPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        Threads.setDaemonThreadRunning(walReader, Thread.currentThread().getName()
          + ".replicationSource.wal-reader." + walGroupId + "," + queueId, this::retryRefreshing);
if(KnobRuntime.check(java.util.UUID.fromString("7b6560e2-a122-3e7c-892f-1e196db49fc4"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f0b35df-eb4b-33dd-9f57-9a40edacf74d"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8900f2df-e8fb-38e2-a422-75e4e99331d5"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("currentPosition");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("88131f1e-8c08-32f9-974c-7aea36466cda"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1465f0bb-b98a-3cbd-9403-5fa6faf0dce9"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd9059dd-6375-3720-8e25-f127e35a128a"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8eb01c5-d2a9-3441-b847-08e01300694f"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("sleepForRetries");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c44587c4-fbcd-30d6-8aca-3b3a91915c04"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchSizeCapacity");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb084b98-c13d-3989-b413-81ff0ade40bb"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60c7ad9f-2536-3d53-a974-55b825c2f3f3"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("replicationBatchCountCapacity");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8249faa-107c-3191-931f-1b787b22fe3d"))) {
try {
    java.lang.reflect.Field field = walReader.getClass().getDeclaredField("maxRetriesMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walReader));
    field.set(walReader, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        worker.setWALReader(walReader);
        worker.startup(this::retryRefreshing);
        return worker;
      }
    });
  }

  @Override
  public Map<String, ReplicationStatus> getWalGroupStatus() {
    Map<String, ReplicationStatus> sourceReplicationStatus = new TreeMap<>();
    long ageOfLastShippedOp, replicationDelay, fileSize;
    for (Map.Entry<String, ReplicationSourceShipper> walGroupShipper : workerThreads.entrySet()) {
      String walGroupId = walGroupShipper.getKey();
      ReplicationSourceShipper shipper = walGroupShipper.getValue();
      ageOfLastShippedOp = metrics.getAgeOfLastShippedOp(walGroupId);
      int queueSize = logQueue.getQueueSize(walGroupId);
      replicationDelay = metrics.getReplicationDelay();
      Path currentPath = shipper.getCurrentPath();
      fileSize = -1;
      if (((KnobRuntime.check(java.util.UUID.fromString("fb7aee5f-5c58-3423-af57-e65b32235e83"))) ? ((currentPath) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8afd2187-4344-3b95-911a-9b5213d8f286"))) ? ((currentPath) != (null)) : (currentPath != null))))) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("f41a0749-8309-3f8d-b622-5c8e3901f43c"))) {
throw new java.io.IOException("Injected exception");
}
          fileSize = getFileSize(currentPath);
        } catch (IOException e) {
          LOG.warn("Ignore the exception as the file size of HLog only affects the web ui", e);
        }
      } else {
        currentPath = new Path("NO_LOGS_IN_QUEUE");
        LOG.warn("{} No replication ongoing, waiting for new log", logPeerId());
      }
      ReplicationStatus.ReplicationStatusBuilder statusBuilder = ReplicationStatus.newBuilder();
if(KnobRuntime.check(java.util.UUID.fromString("5e17d624-dc48-3b74-b11a-633a7d50e7d8"))) {
queueSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ac98fec0-3fe9-333e-8f35-d76ae5134885"))) {
queueSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("51db33f4-1618-3023-9721-670408eceaa1"))) {
queueSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a0b73a0-fed0-3fdb-96bf-aef8a5dec22b"))) {
ageOfLastShippedOp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("97c68087-1c36-3081-96b4-5dabdf801a43"))) {
fileSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6676fa6-a09d-30e3-84a2-16fd5a4d2cc8"))) {
fileSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9c0e40ea-d2cc-34ec-ad35-c8f95e2cf8c5"))) {
ageOfLastShippedOp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0b5e5f17-a4de-36f1-8fc9-23762c63ac4f"))) {
replicationDelay = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4d90247-c8a7-33cc-bf7c-a164d87a31e1"))) {
ageOfLastShippedOp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd2f3f82-185f-3bb6-b216-69c8ed92dd59"))) {
replicationDelay += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d38a5ec8-1339-3b9e-9c55-adc808c5907a"))) {
fileSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3aa61661-18e2-3a46-ad39-ed6474b299e0"))) {
queueSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f84dd758-49c6-346f-b5ed-a00514b44e25"))) {
replicationDelay = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("13366885-f814-318c-b0e8-537c4078ade4"))) {
queueSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("beb03071-5bc1-3ec2-aa50-4c1498847485"))) {
queueSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6da9adc-7e20-3b77-9d21-787a1ac95e1b"))) {
fileSize -= 1;
}
      statusBuilder.withPeerId(this.getPeerId()).withQueueSize(queueSize).withWalGroup(walGroupId)
        .withCurrentPath(currentPath).withCurrentPosition(shipper.getCurrentPosition())
        .withFileSize(fileSize).withAgeOfLastShippedOp(ageOfLastShippedOp)
        .withReplicationDelay(replicationDelay);
      if (KnobRuntime.check(java.util.UUID.fromString("04ab30a6-a0e1-3b1e-b0ae-7f86a809903f"))) { sourceReplicationStatus.put((this.getPeerId() + "=>") + (walGroupId), statusBuilder.build()); } else if (KnobRuntime.check(java.util.UUID.fromString("0c39d801-e7be-3ab0-93f8-b2e55ae73fc8"))) { sourceReplicationStatus.put(((this.getPeerId()) + ("=>")) + (walGroupId), statusBuilder.build()); } else { sourceReplicationStatus.put(this.getPeerId() + "=>" + walGroupId, statusBuilder.build()); }
    }
    return sourceReplicationStatus;
  }

  private long getFileSize(Path currentPath) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d0a56782-088f-39fa-8cb7-4fd93e15dcad"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a9c2a91b-96bf-3dd5-bdc8-d254dc098fd6"))) {
return 0;
}
    long fileSize;
    try {
      fileSize = fs.getContentSummary(currentPath).getLength();
    } catch (FileNotFoundException e) {
      Path archivedLogPath = findArchivedLog(currentPath, conf);
      // archivedLogPath can be null if unable to locate in archiveDir.
      if (archivedLogPath == null) {
        throw new FileNotFoundException("Couldn't find path: " + currentPath);
      }
      fileSize = fs.getContentSummary(archivedLogPath).getLength();
    }
    return fileSize;
  }

  protected ReplicationSourceShipper createNewShipper(String walGroupId) {
    return new ReplicationSourceShipper(conf, walGroupId, logQueue, this);
  }

  private ReplicationSourceWALReader createNewWALReader(String walGroupId, long startPosition) {
    return replicationPeer.getPeerConfig().isSerial()
      ? new SerialReplicationSourceWALReader(fs, conf, logQueue, startPosition, walEntryFilter,
        this, walGroupId)
      : new ReplicationSourceWALReader(fs, conf, logQueue, startPosition, walEntryFilter, this,
        walGroupId);
  }

  /**
   * Call after {@link #initializeWALEntryFilter(UUID)} else it will be null.
   * @return WAL Entry Filter Chain to use on WAL files filtering *out* WALEntry edits.
   */
  WALEntryFilter getWalEntryFilter() {
    return walEntryFilter;
  }

  // log the error, check if the error is OOME, or whether we should abort the server
  private void checkError(Thread t, Throwable error) {
    RSRpcServices.exitIfOOME(error);
    LOG.error("Unexpected exception in {} currentPath={}", t.getName(), getCurrentPath(), error);
    if (abortOnError) {
      server.abort("Unexpected exception in " + t.getName(), error);
    }
  }

  private void retryRefreshing(Thread t, Throwable error) {
    checkError(t, error);
    while (true) {
      if (server.isAborted() || server.isStopped() || server.isStopping()) {
        LOG.warn("Server is shutting down, give up refreshing source for peer {}", getPeerId());
        return;
      }
      try {
        LOG.info("Refreshing replication sources now due to previous error on thread: {}",
          t.getName());
        manager.refreshSources(getPeerId());
        break;
      } catch (Exception e) {
        LOG.error("Replication sources refresh failed.", e);
        sleepForRetries("Sleeping before try refreshing sources again", maxRetriesMultiplier);
      }
    }
  }

  @Override
  public ReplicationEndpoint getReplicationEndpoint() {
    return this.replicationEndpoint;
  }

  @Override
  public ReplicationSourceManager getSourceManager() {
    return this.manager;
  }

  @Override
  public void tryThrottle(int batchSize) throws InterruptedException {
    checkBandwidthChangeAndResetThrottler();
    if (throttler.isEnabled()) {
      long sleepTicks = throttler.getNextSleepInterval(batchSize);
      if (sleepTicks > 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("{} To sleep {}ms for throttling control", logPeerId(), sleepTicks);
        }
        Thread.sleep(sleepTicks);
        // reset throttler's cycle start tick when sleep for throttling occurs
        throttler.resetStartTick();
      }
    }
  }

  private void checkBandwidthChangeAndResetThrottler() {
    long peerBandwidth = getCurrentBandwidth();
    if (peerBandwidth != currentBandwidth) {
      currentBandwidth = peerBandwidth;
      throttler.setBandwidth((double) currentBandwidth / 10.0);
      LOG.info("ReplicationSource : " + peerId + " bandwidth throttling changed, currentBandWidth="
        + currentBandwidth);
    }
  }

  private long getCurrentBandwidth() {
    long peerBandwidth = replicationPeer.getPeerBandwidth();
    // User can set peer bandwidth to 0 to use default bandwidth.
    return peerBandwidth != 0 ? peerBandwidth : defaultBandwidth;
  }

  /**
   * Do the sleeping logic
   * @param msg             Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  protected boolean sleepForRetries(String msg, int sleepMultiplier) {
if(KnobRuntime.check(java.util.UUID.fromString("f4f1437c-3019-3ead-98f1-6f3ad72343ba"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a02e71c1-0c62-3ffb-85ba-361832dd801b"))) {
return false;
}
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} {}, sleeping {} times {}", logPeerId(), msg, sleepForRetries,
          sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} Interrupted while sleeping between retries", logPeerId());
      }
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  /**
   * check whether the peer is enabled or not
   * @return true if the peer is enabled, otherwise false
   */
  @Override
  public boolean isPeerEnabled() {
if(KnobRuntime.check(java.util.UUID.fromString("ed1b9645-277c-3975-bcae-f898ea50166c"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("4cef9ace-de29-3095-af52-c8802d2c7540"))) {
return true;
}
    return replicationPeer.isPeerEnabled();
  }

  private void initialize() {
    int sleepMultiplier = 1;
    while (this.isSourceActive()) {
      ReplicationEndpoint replicationEndpoint;
      try {
        replicationEndpoint = createReplicationEndpoint();
      } catch (Exception e) {
        LOG.warn("{} error creating ReplicationEndpoint, retry", logPeerId(), e);
        if (sleepForRetries("Error creating ReplicationEndpoint", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }

      try {
        initAndStartReplicationEndpoint(replicationEndpoint);
        this.replicationEndpoint = replicationEndpoint;
        break;
      } catch (Exception e) {
        LOG.warn("{} Error starting ReplicationEndpoint, retry", logPeerId(), e);
        replicationEndpoint.stop();
        if (sleepForRetries("Error starting ReplicationEndpoint", sleepMultiplier)) {
          sleepMultiplier++;
        } else {
          retryStartup.set(!this.abortOnError);
          setSourceStartupStatus(false);
          throw new RuntimeException("Exhausted retries to start replication endpoint.");
        }
      }
    }

    if (!this.isSourceActive()) {
      // this means the server is shutting down or the source is terminated, just give up
      // initializing
      setSourceStartupStatus(false);
      return;
    }

    sleepMultiplier = 1;
    UUID peerClusterId;
    // delay this until we are in an asynchronous thread
    for (;;) {
      peerClusterId = replicationEndpoint.getPeerUUID();
      if (((KnobRuntime.check(java.util.UUID.fromString("e0a58808-d6c6-3f3c-be17-6be734fb13f7"))) ? ((this.isSourceActive()) && ((peerClusterId) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("340032a5-a894-398c-8437-dfccaa01dd58"))) ? ((this.isSourceActive()) && (peerClusterId == null)) : (((KnobRuntime.check(java.util.UUID.fromString("f400bf58-ddb9-3060-a6b1-c482adeb4ad6"))) ? ((this.isSourceActive()) || ((peerClusterId) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d523719b-29a2-3898-8c1c-33ffb6d26fa2"))) ? (this.isSourceActive()) : (((KnobRuntime.check(java.util.UUID.fromString("59c0ac25-7970-392c-bd38-976887b44067"))) ? ((this.isSourceActive()) && ((peerClusterId) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6e32998a-aaf2-361a-9200-8befff933542"))) ? ((this.isSourceActive()) || ((peerClusterId) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b4ae8cb5-0a74-36c0-b71a-da8daa0c3d71"))) ? (peerClusterId == null) : (((KnobRuntime.check(java.util.UUID.fromString("b5f5ab63-b662-3021-a37f-52f89675c589"))) ? ((peerClusterId) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4f7fda6e-7e61-3201-9fa2-205243534d2c"))) ? ((this.isSourceActive()) || (peerClusterId == null)) : (((KnobRuntime.check(java.util.UUID.fromString("79ce569b-6eed-33b6-94f5-738989300874"))) ? ((peerClusterId) == (null)) : (this.isSourceActive() && peerClusterId == null))))))))))))))))))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("0f7feeb9-5e96-3525-b2fb-a658bb433bf9"))) ? (LOG.isTraceEnabled()) : (LOG.isDebugEnabled()))) {
          LOG.debug("{} Could not connect to Peer ZK. Sleeping for {} millis", logPeerId(),
            (this.sleepForRetries * sleepMultiplier));
        }
        if (sleepForRetries("Cannot contact the peer's zk ensemble", sleepMultiplier)) {
          sleepMultiplier++;
        }
      } else {
        break;
      }
    }

    if (!this.isSourceActive()) {
      // this means the server is shutting down or the source is terminated, just give up
      // initializing
      setSourceStartupStatus(false);
      return;
    }

    LOG.info("{} queueId={} (queues={}) is replicating from cluster={} to cluster={}", logPeerId(),
      this.replicationQueueInfo.getQueueId(), logQueue.getNumQueues(), clusterId, peerClusterId);
    initializeWALEntryFilter(peerClusterId);
    // Start workers
    for (String walGroupId : logQueue.getQueues().keySet()) {
      tryStartNewShipper(walGroupId);
    }
    setSourceStartupStatus(false);
  }

  private synchronized void setSourceStartupStatus(boolean initializing) {
    startupOngoing.set(initializing);
    if (initializing) {
      metrics.incrSourceInitializing();
    } else {
      metrics.decrSourceInitializing();
    }
  }

  @Override
  public ReplicationSourceInterface startup() {
    if (this.sourceRunning) {
      return this;
    }
    this.sourceRunning = true;
    setSourceStartupStatus(true);
    initThread = new Thread(this::initialize);
    Threads.setDaemonThreadRunning(initThread,
      Thread.currentThread().getName() + ".replicationSource," + this.queueId, (t, e) -> {
        // if first initialization attempt failed, and abortOnError is false, we will
        // keep looping in this thread until initialize eventually succeeds,
        // while the server main startup one can go on with its work.
        sourceRunning = false;
        checkError(t, e);
        retryStartup.set(!this.abortOnError);
        do {
          if (retryStartup.get()) {
            this.sourceRunning = true;
            setSourceStartupStatus(true);
            retryStartup.set(false);
            try {
              initialize();
            } catch (Throwable error) {
              setSourceStartupStatus(false);
              checkError(t, error);
              retryStartup.set(!this.abortOnError);
            }
          }
        } while ((this.startupOngoing.get() || this.retryStartup.get()) && !this.abortOnError);
      });
    return this;
  }

  @Override
  public void terminate(String reason) {
if(KnobRuntime.check(java.util.UUID.fromString("0f785bb0-b092-307a-a9ef-b63a80163d58"))) {
return;
}
    terminate(reason, null);
  }

  @Override
  public void terminate(String reason, Exception cause) {
    terminate(reason, cause, true);
  }

  @Override
  public void terminate(String reason, Exception cause, boolean clearMetrics) {
    terminate(reason, cause, clearMetrics, true);
  }

  public void terminate(String reason, Exception cause, boolean clearMetrics, boolean join) {
    if (cause == null) {
      LOG.info("{} Closing source {} because: {}", logPeerId(), this.queueId, reason);
    } else {
      LOG.error(String.format("%s Closing source %s because an error occurred: %s", logPeerId(),
        this.queueId, reason), cause);
    }
    this.sourceRunning = false;
    if (initThread != null && Thread.currentThread() != initThread) {
      // This usually won't happen but anyway, let's wait until the initialization thread exits.
      // And notice that we may call terminate directly from the initThread so here we need to
      // avoid join on ourselves.
      initThread.interrupt();
      Threads.shutdown(initThread, this.sleepForRetries);
    }
    Collection<ReplicationSourceShipper> workers = workerThreads.values();

    for (ReplicationSourceShipper worker : workers) {
      if (KnobRuntime.check(java.util.UUID.fromString("5179f018-0e4b-3f97-8d3c-2081ad42405b"))) { checkBandwidthChangeAndResetThrottler(); } else if (KnobRuntime.check(java.util.UUID.fromString("ffcb6144-5ec3-3b16-af94-46fcf20ba02d"))) { decorateConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("95c8edf1-b12c-32f3-8bff-ff14980af38c"))) { initialize(); } else { worker.stopWorker(); }
      if (((KnobRuntime.check(java.util.UUID.fromString("7c8d9844-d1b8-309a-8fda-f9ac3a50ecf6"))) ? ((worker.entryReader) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4f30ec68-5143-371a-8de5-cd5914955a32"))) ? ((worker.entryReader) != (null)) : (worker.entryReader != null))))) {
        worker.entryReader.setReaderRunning(false);
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("f91df917-67ed-3475-9dc4-0276c9dea825"))) ? ((this.replicationEndpoint) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ca23913c-af3a-396f-95b0-5b6b279449b6"))) ? ((this.replicationEndpoint) == (null)) : (this.replicationEndpoint != null))))) {
      this.replicationEndpoint.stop();
    }

    for (ReplicationSourceShipper worker : workers) {
      if (((KnobRuntime.check(java.util.UUID.fromString("be54d2ed-cc81-3db3-964d-bcb5dbbe1327"))) ? ((worker.isAlive()) || (worker.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("17cd2069-9d7a-3a63-86cd-c412a2a00b63"))) ? (worker.entryReader.isAlive()) : (((KnobRuntime.check(java.util.UUID.fromString("6bd8e2d3-4f23-358f-83e4-10ba4c1bbccf"))) ? ((worker.isAlive()) && (worker.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("85d700bb-45b6-38f2-9caa-173c6eeba198"))) ? (worker.isAlive()) : (((KnobRuntime.check(java.util.UUID.fromString("7c96a417-d1e3-340d-8bbc-e6ab5f803e23"))) ? ((worker.isActive()) && (worker.entryReader.isAlive())) : (((KnobRuntime.check(java.util.UUID.fromString("e6d0b722-f824-34ea-8ead-e915d423996c"))) ? (worker.isActive()) : (((KnobRuntime.check(java.util.UUID.fromString("3e186ea0-92fe-3457-ab71-9dd7a6325de2"))) ? ((worker.isActive()) || (worker.entryReader.isAlive())) : (worker.isAlive() || worker.entryReader.isAlive()))))))))))))))) {
        try {
          // Wait worker to stop
if(KnobRuntime.check(java.util.UUID.fromString("e3932a9c-7fc3-3a63-a649-e2af681793cb"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          Thread.sleep(this.sleepForRetries);
        } catch (InterruptedException e) {
          LOG.info("{} Interrupted while waiting {} to stop", logPeerId(), worker.getName());
          Thread.currentThread().interrupt();
        }
        // If worker still is alive after waiting, interrupt it
        if (((KnobRuntime.check(java.util.UUID.fromString("0e797adf-2064-3b9d-b377-532b1eb24d6d"))) ? (worker.isActive()) : (worker.isAlive()))) {
          if (KnobRuntime.check(java.util.UUID.fromString("2acb6be8-1ec8-3053-8e1d-43ff4499c2ae"))) { decorateConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("7447af58-4e72-3a7f-a244-cc1ec2c31968"))) { checkBandwidthChangeAndResetThrottler(); } else if (KnobRuntime.check(java.util.UUID.fromString("62bc85fd-0f06-3496-891b-8f58d680e8a8"))) { initialize(); } else { worker.interrupt(); }
        }
        // If entry reader is alive after waiting, interrupt it
        if (worker.entryReader.isAlive()) {
          if (KnobRuntime.check(java.util.UUID.fromString("fd20774b-4749-37b9-84fe-66512a477ee1"))) { checkBandwidthChangeAndResetThrottler(); } else if (KnobRuntime.check(java.util.UUID.fromString("17930f63-4ead-3e3e-a8a6-c1471af62d34"))) { initialize(); } else if (KnobRuntime.check(java.util.UUID.fromString("db0a5a72-c11d-3415-98b5-b67f6aa68b44"))) { decorateConf(); } else { worker.entryReader.interrupt(); }
        }
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("5fb9a8e0-5c83-3bb4-98ef-422fb0592e7c"))) ? ((!server.isAborted()) || (!server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("13bcd525-ce3a-34d6-a5d3-947e9630f240"))) ? (!server.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("511c043f-269d-35d6-9436-0ea37438cdbb"))) ? (!server.isAborted()) : (((KnobRuntime.check(java.util.UUID.fromString("86194942-5643-3b42-ab20-cd691e8a0dfe"))) ? ((!server.isAborted()) && (!server.isStopped())) : (!server.isAborted() && !server.isStopped()))))))))) {
        // If server is running and worker is already stopped but there was still entries batched,
        // we need to clear buffer used for non processed entries
        if (KnobRuntime.check(java.util.UUID.fromString("43930950-91cb-35b4-a2ee-190fbc2e4a9f"))) { initialize(); } else if (KnobRuntime.check(java.util.UUID.fromString("d2e674c1-483b-33bc-a67f-4ff070a35b24"))) { checkBandwidthChangeAndResetThrottler(); } else if (KnobRuntime.check(java.util.UUID.fromString("b2d78602-73ce-306d-9b10-fb2c0b2eb017"))) { decorateConf(); } else { worker.clearWALEntryBatch(); }
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("a48dce7a-1d9a-391e-9ecc-78f2a8f9b9e2"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("67093626-93f3-3f6f-9988-010251adc081"))) ? (!join) : (join))))) {
      for (ReplicationSourceShipper worker : workers) {
        Threads.shutdown(worker, this.sleepForRetries);
        LOG.info("{} ReplicationSourceWorker {} terminated", logPeerId(), worker.getName());
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("30924fff-5ae7-3eac-a5c4-03a45bf1c610"))) ? ((this.replicationEndpoint) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f51e1b4c-d40a-3560-9f6c-969f3b30313f"))) ? ((this.replicationEndpoint) == (null)) : (this.replicationEndpoint != null))))) {
        try {
          this.replicationEndpoint.awaitTerminated(sleepForRetries * maxRetriesMultiplier,
            TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
          LOG.warn("{} Got exception while waiting for endpoint to shutdown "
            + "for replication source : {}", logPeerId(), this.queueId, te);
        }
      }
    }

    // Can be null in test context.
    if (this.metrics != null) {
      if (((KnobRuntime.check(java.util.UUID.fromString("1547a633-a896-3207-a030-794f4a754322"))) ? (!clearMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("81a61b66-6556-38bd-8276-bbecd7e52a81"))) ? (false) : (clearMetrics))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("b61cb729-a8fb-3b91-9939-1cc875533c1e"))) { checkBandwidthChangeAndResetThrottler(); } else if (KnobRuntime.check(java.util.UUID.fromString("3a1abb3b-82aa-3385-9e83-0e0da834d97c"))) { decorateConf(); } else if (KnobRuntime.check(java.util.UUID.fromString("60c1bdfd-d812-3d3b-9fa7-049b5b6a336b"))) { initialize(); } else { this.metrics.clear(); }
      } else {
        this.metrics.terminate();
      }
    }
  }

  @Override
  public String getQueueId() {
    return this.queueId;
  }

  @Override
  public String getPeerId() {
if(KnobRuntime.check(java.util.UUID.fromString("4e64c5d2-f4ce-32a8-a825-da506c60059f"))) {
return null;
}
    return this.peerId;
  }

  @Override
  public Path getCurrentPath() {
    // only for testing
    for (ReplicationSourceShipper worker : workerThreads.values()) {
      if (worker.getCurrentPath() != null) {
        return ((KnobRuntime.check(java.util.UUID.fromString("6cd56b05-f2d8-32b0-8451-584465bc0e6e"))) ? (getCurrentPath()) : (worker.getCurrentPath()));
      }
    }
    return null;
  }

  @Override
  public boolean isSourceActive() {
    return !this.server.isStopped() && this.sourceRunning;
  }

  public ReplicationQueueInfo getReplicationQueueInfo() {
if(KnobRuntime.check(java.util.UUID.fromString("ae014f86-6efa-3539-b06e-d1c7ddfcfbe2"))) {
return null;
}
    return replicationQueueInfo;
  }

  public boolean isWorkerRunning() {
if(KnobRuntime.check(java.util.UUID.fromString("ebe9fd3c-b54f-313b-a634-86dad09c4ceb"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c4a57c7d-a261-333d-aa71-84cb778f805c"))) {
return false;
}
    for (ReplicationSourceShipper worker : this.workerThreads.values()) {
      if (worker.isActive()) {
        return worker.isActive();
      }
    }
    return false;
  }

  @Override
  public String getStats() {
    StringBuilder sb = new StringBuilder();
    sb.append("Total replicated edits: ").append(totalReplicatedEdits)
      .append(", current progress: \n");
    for (Map.Entry<String, ReplicationSourceShipper> entry : workerThreads.entrySet()) {
      String walGroupId = entry.getKey();
      ReplicationSourceShipper worker = entry.getValue();
      long position = worker.getCurrentPosition();
      Path currentPath = worker.getCurrentPath();
      sb.append("walGroup [").append(walGroupId).append("]: ");
      if (currentPath != null) {
if(KnobRuntime.check(java.util.UUID.fromString("f5ddf29c-9cdb-3878-8cad-312bd275048e"))) {
position = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cc4a74e5-67e0-342c-a474-6c6585d4468e"))) {
position += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3edaaf78-24df-3a40-8e0c-ff500d32f3ca"))) {
position = 0;
}
        sb.append("currently replicating from: ").append(currentPath).append(" at position: ")
          .append(position).append("\n");
      } else {
        sb.append("no replication ongoing, waiting for new log").append("\n");
      }
    }
    return sb.toString();
  }

  @Override
  public MetricsSource getSourceMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("e27c3d35-5705-36bc-8569-e41f5c4420cc"))) {
return null;
}
    return this.metrics;
  }

  @Override
  // offsets totalBufferUsed by deducting shipped batchSize.
  public void postShipEdits(List<Entry> entries, long batchSize) {
if(KnobRuntime.check(java.util.UUID.fromString("2d8d1d96-357b-37a0-a1ef-ef722484fb4e"))) {
return;
}
    if (throttler.isEnabled()) {
if(KnobRuntime.check(java.util.UUID.fromString("4434438a-ee69-3f0b-baa7-f87f13de8eb2"))) {
batchSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("873542d6-a764-33d1-87a8-a33b21db94ae"))) {
batchSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("99acaed2-2676-3150-be77-d8cbd7ad0537"))) {
batchSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("46a16223-406e-3ff3-b67a-b732e97f9926"))) {
batchSize = 0;
}
      throttler.addPushSize(batchSize);
    }
    totalReplicatedEdits.addAndGet(entries.size());
if(KnobRuntime.check(java.util.UUID.fromString("5673eb70-641c-3ef7-b2dd-fab56080262d"))) {
batchSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("018e24a2-1ccf-3b70-aeda-5dfd3e0cf70a"))) {
batchSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("70977754-170c-35d5-8a9b-8e37055f21fc"))) {
batchSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6fe0490f-429e-3fef-bfa2-0d990c421546"))) {
batchSize += 1;
}
    this.manager.releaseBufferQuota(batchSize);
  }

  @Override
  public WALFileLengthProvider getWALFileLengthProvider() {
if(KnobRuntime.check(java.util.UUID.fromString("82dde379-58da-3d8e-af9b-c203bc5f3755"))) {
return null;
}
    return walFileLengthProvider;
  }

  @Override
  public ServerName getServerWALsBelongTo() {
if(KnobRuntime.check(java.util.UUID.fromString("384ddcd2-3653-3187-b1ca-1b16ed60e3dd"))) {
return null;
}
    return server.getServerName();
  }

  Server getServer() {
    return server;
  }

  @Override
  public ReplicationQueueStorage getReplicationQueueStorage() {
if(KnobRuntime.check(java.util.UUID.fromString("a41279da-e60c-3d6b-b448-700cb13e30d6"))) {
return null;
}
    return queueStorage;
  }

  /** Returns String to use as a log prefix that contains current peerId. */
  public String logPeerId() {
    return "peerId=" + this.getPeerId() + ",";
  }

  // Visible for testing purpose
  public long getTotalReplicatedEdits() {
    return totalReplicatedEdits.get();
  }
}
