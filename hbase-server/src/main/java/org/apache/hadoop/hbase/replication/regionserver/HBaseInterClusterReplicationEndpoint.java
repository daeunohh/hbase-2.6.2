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
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSinkManager.SinkPeer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} implementation for replicating
 * to another HBase cluster. For the slave cluster it selects a random number of peers using a
 * replication ratio. For example, if replication ration = 0.1 and slave cluster has 100 region
 * servers, 10 will be selected.
 * <p>
 * A stream is considered down when we cannot contact a region server on the peer cluster for more
 * than 55 seconds by default.
 * </p>
 */
@InterfaceAudience.Private
public class HBaseInterClusterReplicationEndpoint extends HBaseReplicationEndpoint {
  private static final Logger LOG =
    LoggerFactory.getLogger(HBaseInterClusterReplicationEndpoint.class);

  private static final long DEFAULT_MAX_TERMINATION_WAIT_MULTIPLIER = 2;

  /** Drop edits for tables that been deleted from the replication source and target */
  public static final String REPLICATION_DROP_ON_DELETED_TABLE_KEY =
    "hbase.replication.drop.on.deleted.table";
  /** Drop edits for CFs that been deleted from the replication source and target */
  public static final String REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY =
    "hbase.replication.drop.on.deleted.columnfamily";

  private ClusterConnection conn;
  private Configuration localConf;
  private Configuration conf;
  // How long should we sleep for each retry
  private long sleepForRetries;
  // Maximum number of retries before taking bold actions
  private int maxRetriesMultiplier;
  // Socket timeouts require even bolder actions since we don't want to DDOS
  private int socketTimeoutMultiplier;
  // Amount of time for shutdown to wait for all tasks to complete
  private long maxTerminationWait;
  // Size limit for replication RPCs, in bytes
  private int replicationRpcLimit;
  // Metrics for this source
  private MetricsSource metrics;
  // Handles connecting to peer region servers
  private ReplicationSinkManager replicationSinkMgr;
  private boolean peersSelected = false;
  private String replicationClusterId = "";
  private ThreadPoolExecutor exec;
  private int maxThreads;
  private Path baseNamespaceDir;
  private Path hfileArchiveDir;
  private boolean replicationBulkLoadDataEnabled;
  private Abortable abortable;
  private boolean dropOnDeletedTables;
  private boolean dropOnDeletedColumnFamilies;
  private boolean isSerial = false;
  // Initialising as 0 to guarantee at least one logging message
  private long lastSinkFetchTime = 0;

  /*
   * Some implementations of HBaseInterClusterReplicationEndpoint may require instantiating
   * different Connection implementations, or initialize it in a different way, so defining
   * createConnection as protected for possible overridings.
   */
  protected Connection createConnection(Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a056fa48-3d16-3a2e-81cb-563897a11edc"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2cca33a5-fd41-3538-95ec-5ff34d8255fb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("98cf6929-8a14-37ef-a9d0-7f39d0321229"))) {
return null;
}
    return ConnectionFactory.createConnection(conf);
  }

  /*
   * Some implementations of HBaseInterClusterReplicationEndpoint may require instantiating
   * different ReplicationSinkManager implementations, or initialize it in a different way, so
   * defining createReplicationSinkManager as protected for possible overridings.
   */
  protected ReplicationSinkManager createReplicationSinkManager(Connection conn) {
    return new ReplicationSinkManager((ClusterConnection) conn, this.ctx.getPeerId(), this,
      this.conf);
  }

  @Override
  public void init(Context context) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5b7b6839-0a12-3094-87df-1495333c5b2d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("4eb87912-9a49-3105-9c9c-d3b65eb83fd5"))) {
throw new java.io.IOException("Injected exception");
}
    super.init(context);
    this.conf = HBaseConfiguration.create(ctx.getConfiguration());
    this.localConf = HBaseConfiguration.create(ctx.getLocalConfiguration());
    decorateConf();
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    this.socketTimeoutMultiplier =
      this.conf.getInt("replication.source.socketTimeoutMultiplier", maxRetriesMultiplier);
    // A Replicator job is bound by the RPC timeout. We will wait this long for all Replicator
    // tasks to terminate when doStop() is called.
    long maxTerminationWaitMultiplier = this.conf.getLong(
      "replication.source.maxterminationmultiplier", DEFAULT_MAX_TERMINATION_WAIT_MULTIPLIER);
    this.maxTerminationWait = maxTerminationWaitMultiplier
      * this.conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    // TODO: This connection is replication specific or we should make it particular to
    // replication and make replication specific settings such as compression or codec to use
    // passing Cells.
    Connection connection = createConnection(this.conf);
    // Since createConnection method may be overridden by extending classes, we need to make sure
    // it's indeed returning a ClusterConnection instance.
    Preconditions.checkState(connection instanceof ClusterConnection);
    this.conn = (ClusterConnection) connection;
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    this.metrics = context.getMetrics();
    // ReplicationQueueInfo parses the peerId out of the znode for us
    this.replicationSinkMgr = createReplicationSinkManager(conn);
    // per sink thread pool
    this.maxThreads = this.conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
      HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);
    this.exec = Threads.getBoundedCachedThreadPool(maxThreads, 60, TimeUnit.SECONDS,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SinkThread-%d").build());
    this.abortable = ctx.getAbortable();
    // Set the size limit for replication RPCs to 95% of the max request size.
    // We could do with less slop if we have an accurate estimate of encoded size. Being
    // conservative for now.
    this.replicationRpcLimit =
      (int) (0.95 * conf.getLong(RpcServer.MAX_REQUEST_SIZE, RpcServer.DEFAULT_MAX_REQUEST_SIZE));
    this.dropOnDeletedTables = this.conf.getBoolean(REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
    this.dropOnDeletedColumnFamilies =
      this.conf.getBoolean(REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY, false);

    this.replicationBulkLoadDataEnabled = conf.getBoolean(
      HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    if (((KnobRuntime.check(java.util.UUID.fromString("7c95ce5f-a08f-35dd-bd7b-bcf362cdc430"))) ? (!this.replicationBulkLoadDataEnabled) : (this.replicationBulkLoadDataEnabled))) {
      replicationClusterId = this.conf.get(HConstants.REPLICATION_CLUSTER_ID);
    }
    // Construct base namespace directory and hfile archive directory path
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path baseNSDir = new Path(HConstants.BASE_NAMESPACE_DIR);
    baseNamespaceDir = new Path(rootDir, baseNSDir);
    hfileArchiveDir = new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNSDir));
    isSerial = context.getPeerConfig().isSerial();
  }

  private void decorateConf() {
if(KnobRuntime.check(java.util.UUID.fromString("fae591d8-314f-30f5-93f8-0006183bef38"))) {
return;
}
    String replicationCodec = this.conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      this.conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
  }

  private void connectToPeers() {
    getRegionServers();

    int sleepMultiplier = 1;

    // Connect to peer cluster first, unless we have to stop
    while (this.isRunning() && replicationSinkMgr.getNumSinks() == 0) {
      replicationSinkMgr.chooseSinks();
      if (this.isRunning() && replicationSinkMgr.getNumSinks() == 0) {
        if (sleepForRetries("Waiting for peers", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
  }

  /**
   * Do the sleeping logic
   * @param msg             Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  private boolean sleepForRetries(String msg, int sleepMultiplier) {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} {}, sleeping {} times {}", logPeerId(), msg, sleepForRetries,
          sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} {} Interrupted while sleeping between retries", msg, logPeerId());
      }
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  private int getEstimatedEntrySize(Entry e) {
    long size = e.getKey().estimatedSerializedSizeOf() + e.getEdit().estimatedSerializedSizeOf();
    return (int) size;
  }

  private List<List<Entry>> createParallelBatches(final List<Entry> entries) {
    int numSinks = Math.max(replicationSinkMgr.getNumSinks(), 1);
    int n = Math.min(Math.min(this.maxThreads, entries.size() / 100 + 1), numSinks);
    List<List<Entry>> entryLists =
      Stream.generate(ArrayList<Entry>::new).limit(n).collect(Collectors.toList());
    int[] sizes = new int[n];
    for (Entry e : entries) {
      int index = Math.abs(Bytes.hashCode(e.getKey().getEncodedRegionName()) % n);
      int entrySize = getEstimatedEntrySize(e);
      // If this batch has at least one entry and is over sized, move it to the tail of list and
      // initialize the entryLists[index] to be a empty list.
      if (sizes[index] > 0 && sizes[index] + entrySize > replicationRpcLimit) {
        entryLists.add(entryLists.get(index));
        entryLists.set(index, new ArrayList<>());
        sizes[index] = 0;
      }
      entryLists.get(index).add(e);
      sizes[index] += entrySize;
    }
    return entryLists;
  }

  private List<List<Entry>> createSerialBatches(final List<Entry> entries) {
    Map<byte[], List<Entry>> regionEntries = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry e : entries) {
      regionEntries.computeIfAbsent(e.getKey().getEncodedRegionName(), key -> new ArrayList<>())
        .add(e);
    }
    return new ArrayList<>(regionEntries.values());
  }

  /**
   * Divide the entries into multiple batches, so that we can replicate each batch in a thread pool
   * concurrently. Note that, for serial replication, we need to make sure that entries from the
   * same region to be replicated serially, so entries from the same region consist of a batch, and
   * we will divide a batch into several batches by replicationRpcLimit in method
   * serialReplicateRegionEntries()
   */
  private List<List<Entry>> createBatches(final List<Entry> entries) {
    if (isSerial) {
      return createSerialBatches(entries);
    } else {
      return createParallelBatches(entries);
    }
  }

  /**
   * Check if there's an {@link TableNotFoundException} in the caused by stacktrace.
   */
  public static boolean isTableNotFoundException(Throwable io) {
    if (io instanceof RemoteException) {
      io = ((RemoteException) io).unwrapRemoteException();
    }
    if (io != null && io.getMessage().contains("TableNotFoundException")) {
      return true;
    }
    for (; io != null; io = io.getCause()) {
      if (io instanceof TableNotFoundException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if there's an {@link NoSuchColumnFamilyException} in the caused by stacktrace.
   */
  public static boolean isNoSuchColumnFamilyException(Throwable io) {
    if (io instanceof RemoteException) {
      io = ((RemoteException) io).unwrapRemoteException();
    }
    if (io != null && io.getMessage().contains("NoSuchColumnFamilyException")) {
      return true;
    }
    for (; io != null; io = io.getCause()) {
      if (io instanceof NoSuchColumnFamilyException) {
        return true;
      }
    }
    return false;
  }

  List<List<Entry>> filterNotExistTableEdits(final List<List<Entry>> oldEntryList) {
    List<List<Entry>> entryList = new ArrayList<>();
    Map<TableName, Boolean> existMap = new HashMap<>();
    try (Connection localConn = ConnectionFactory.createConnection(ctx.getLocalConfiguration());
      Admin localAdmin = localConn.getAdmin()) {
      for (List<Entry> oldEntries : oldEntryList) {
        List<Entry> entries = new ArrayList<>();
        for (Entry e : oldEntries) {
          TableName tableName = e.getKey().getTableName();
          boolean exist = true;
          if (existMap.containsKey(tableName)) {
            exist = existMap.get(tableName);
          } else {
            try {
              exist = localAdmin.tableExists(tableName);
              existMap.put(tableName, exist);
            } catch (IOException iox) {
              LOG.warn("Exception checking for local table " + tableName, iox);
              // we can't drop edits without full assurance, so we assume table exists.
              exist = true;
            }
          }
          if (exist) {
            entries.add(e);
          } else {
            // Would potentially be better to retry in one of the outer loops
            // and add a table filter there; but that would break the encapsulation,
            // so we're doing the filtering here.
            LOG.warn("Missing table detected at sink, local table also does not exist, "
              + "filtering edits for table '{}'", tableName);
          }
        }
        if (!entries.isEmpty()) {
          entryList.add(entries);
        }
      }
    } catch (IOException iox) {
      LOG.warn("Exception when creating connection to check local table", iox);
      return oldEntryList;
    }
    return entryList;
  }

  List<List<Entry>> filterNotExistColumnFamilyEdits(final List<List<Entry>> oldEntryList) {
if(KnobRuntime.check(java.util.UUID.fromString("15c34bc0-5ba9-323b-af17-3dab4188b635"))) {
return null;
}
    List<List<Entry>> entryList = new ArrayList<>();
    Map<TableName, Set<String>> existColumnFamilyMap = new HashMap<>();
    try (Connection localConn = ConnectionFactory.createConnection(ctx.getLocalConfiguration());
      Admin localAdmin = localConn.getAdmin()) {
      for (List<Entry> oldEntries : oldEntryList) {
        List<Entry> entries = new ArrayList<>();
        for (Entry e : oldEntries) {
          TableName tableName = e.getKey().getTableName();
          if (!existColumnFamilyMap.containsKey(tableName)) {
            try {
              Set<String> cfs = localAdmin.getDescriptor(tableName).getColumnFamilyNames().stream()
                .map(Bytes::toString).collect(Collectors.toSet());
if(KnobRuntime.check(java.util.UUID.fromString("6489d0ba-020e-32d4-9a3c-fbeb7c33e0f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e704d36f-a0fd-3696-9ef3-67026cab2330"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0688b2d7-d80d-331a-acc4-bda85dfe66ab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6b53799c-3c48-35cb-85b4-9039488eada2"))) {
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
              existColumnFamilyMap.put(tableName, cfs);
            } catch (Exception ex) {
              LOG.warn("Exception getting cf names for local table {}", tableName, ex);
              // if catch any exception, we are not sure about table's description,
              // so replicate raw entry
              entries.add(e);
              continue;
            }
          }

          Set<String> existColumnFamilies = existColumnFamilyMap.get(tableName);
          Set<String> missingCFs = new HashSet<>();
          WALEdit walEdit = new WALEdit();
          walEdit.getCells().addAll(e.getEdit().getCells());
          WALUtil.filterCells(walEdit, cell -> {
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            if (existColumnFamilies.contains(cf)) {
              return cell;
            } else {
              missingCFs.add(cf);
              return null;
            }
          });
          if (!walEdit.isEmpty()) {
            Entry newEntry = new Entry(e.getKey(), walEdit);
            entries.add(newEntry);
          }

          if (!missingCFs.isEmpty()) {
            // Would potentially be better to retry in one of the outer loops
            // and add a table filter there; but that would break the encapsulation,
            // so we're doing the filtering here.
            LOG.warn(
              "Missing column family detected at sink, local column family also does not exist,"
                + " filtering edits for table '{}',column family '{}'",
              tableName, missingCFs);
          }
        }
        if (!entries.isEmpty()) {
          entryList.add(entries);
        }
      }
    } catch (IOException iox) {
      LOG.warn("Exception when creating connection to check local table", iox);
      return oldEntryList;
    }
    return entryList;
  }

  private void reconnectToPeerCluster() {
    ClusterConnection connection = null;
    try {
      connection = (ClusterConnection) ConnectionFactory.createConnection(this.conf);
    } catch (IOException ioe) {
      LOG.warn("{} Failed to create connection for peer cluster", logPeerId(), ioe);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("8f8ba80d-460b-3117-9ca2-eb2cd689841d"))) ? ((connection) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ed005618-9328-3e90-a16f-1f3a1e0bbfc4"))) ? ((connection) != (null)) : (connection != null))))) {
      this.conn = connection;
    }
  }

  private long parallelReplicate(CompletionService<Integer> pool, ReplicateContext replicateContext,
    List<List<Entry>> batches) throws IOException {
    int futures = 0;
    for (int i = 0; i < batches.size(); i++) {
if(KnobRuntime.check(java.util.UUID.fromString("2cc1c699-aeca-38dc-8f17-522b4a534f21"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ed990724-aced-3932-b649-b8d0bd17710d"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("747bda71-a93e-392e-bf40-32f83d085a17"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0105d2e2-b8ae-388c-8546-8fdcff085795"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d90a8e34-c491-3b45-8fd4-db3d9eb0210d"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2320fd86-bbae-3709-b823-76c8ecc950c2"))) {
i *= 2;
}
      List<Entry> entries = batches.get(i);
      if (!entries.isEmpty()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("c99f958a-deec-3196-b674-ef8db2fa225b"))) ? (LOG.isDebugEnabled()) : (LOG.isTraceEnabled()))) {
          LOG.trace("{} Submitting {} entries of total size {}", logPeerId(), entries.size(),
            replicateContext.getSize());
        }
        // RuntimeExceptions encountered here bubble up and are handled in ReplicationSource
        pool.submit(createReplicator(entries, i, replicateContext.getTimeout()));
        futures++;
      }
    }

    IOException iox = null;
    long lastWriteTime = 0;
    for (int i = 0; i < futures; i++) {
      try {
        // wait for all futures, remove successful parts
        // (only the remaining parts will be retried)
        Future<Integer> f = pool.take();
        int index = f.get();
if(KnobRuntime.check(java.util.UUID.fromString("9f0f15cc-7a9a-3c68-a9df-8a975161750c"))) {
index = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("97dae675-23b5-3a22-b0e5-7ca828d62858"))) {
index = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dbee0e4e-412d-3967-837d-736fd0ed53f8"))) {
index /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a2474aa-6bdf-3da7-9e53-201040c2cd9a"))) {
index += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4bd0a14a-e217-3da1-b452-c5e2a15e8e7c"))) {
index -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("47ee5a0e-bd57-362f-baf2-02ef30fe424a"))) {
index *= 2;
}
        List<Entry> batch = batches.get(index);
        batches.set(index, Collections.emptyList()); // remove successful batch
        // Find the most recent write time in the batch
        long writeTime = batch.get(batch.size() - 1).getKey().getWriteTime();
        if (writeTime > lastWriteTime) {
          lastWriteTime = writeTime;
        }
      } catch (InterruptedException ie) {
        iox = new IOException(ie);
      } catch (ExecutionException ee) {
        iox = ee.getCause() instanceof IOException
          ? (IOException) ee.getCause()
          : new IOException(ee.getCause());
      }
    }
    if (iox != null) {
      // if we had any exceptions, try again
      throw iox;
    }
    return lastWriteTime;
  }

  /**
   * Do the shipping logic
   */
  @Override
  public boolean replicate(ReplicateContext replicateContext) {
if(KnobRuntime.check(java.util.UUID.fromString("5f772324-269f-3909-8eb8-4fa0d899ff6a"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b00ae0c8-d99b-37ef-99d4-94b7d2ae4355"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad3dea73-237b-3228-bed2-a52c346aff1c"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1db42da1-916b-3f53-bfd5-36e386c74351"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40be9c50-cdd5-37be-858c-caf6c003f1be"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f34e4213-48e6-3614-9b8e-09ef74e8985b"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("6bb24108-e1f0-3514-8026-688ce805feca"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51f849bc-8a7c-397c-9bed-7c1ff128d4c7"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7e9624f-b818-3c60-8819-97e17509be70"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("639e0b97-24bf-34b1-a971-bd1f97d44fa1"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    CompletionService<Integer> pool = new ExecutorCompletionService<>(this.exec);
    int sleepMultiplier = 1;
    int initialTimeout = replicateContext.getTimeout();

    if (!peersSelected && this.isRunning()) {
      connectToPeers();
      peersSelected = true;
    }

    int numSinks = replicationSinkMgr.getNumSinks();
    if (numSinks == 0) {
      if (
        ((KnobRuntime.check(java.util.UUID.fromString("9ed60872-0554-391a-86f3-d709e5d5356e"))) ? (((EnvironmentEdgeManager.currentTime() - lastSinkFetchTime)) > ((maxRetriesMultiplier * 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("d161b78d-baa9-3cd8-b748-e7d96640dabc"))) ? (((EnvironmentEdgeManager.currentTime() - lastSinkFetchTime)) < ((maxRetriesMultiplier * 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("dcf7ee0e-5760-3779-855c-681819637f41"))) ? (((EnvironmentEdgeManager.currentTime() - lastSinkFetchTime)) >= ((maxRetriesMultiplier * 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("6fc04053-fb4e-3b31-a7fd-bd4dd4796c72"))) ? (((EnvironmentEdgeManager.currentTime() - lastSinkFetchTime)) <= ((maxRetriesMultiplier * 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("11733992-8a59-330b-ac4c-5feab80aca09"))) ? (((EnvironmentEdgeManager.currentTime() - lastSinkFetchTime)) == ((maxRetriesMultiplier * 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("3f0b924a-b853-3ad5-b747-292f6de6503f"))) ? (((EnvironmentEdgeManager.currentTime() - lastSinkFetchTime)) != ((maxRetriesMultiplier * 1000))) : ((EnvironmentEdgeManager.currentTime() - lastSinkFetchTime) >= (maxRetriesMultiplier * 1000)))))))))))))
      ) {
        LOG.warn("No replication sinks found, returning without replicating. "
          + "The source should retry with the same set of edits. Not logging this again for "
          + "the next {} seconds.", maxRetriesMultiplier);
        lastSinkFetchTime = EnvironmentEdgeManager.currentTime();
      }
      sleepForRetries("No sinks available at peer", sleepMultiplier);
      return false;
    }

    List<List<Entry>> batches = createBatches(replicateContext.getEntries());
    while (this.isRunning() && !exec.isShutdown()) {
      if (!isPeerEnabled()) {
if(KnobRuntime.check(java.util.UUID.fromString("29ab5712-4725-3897-b578-f1bc0a49c362"))) {
sleepMultiplier = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("28df062d-d7a9-3f44-8351-f0d06b936fcb"))) {
sleepMultiplier /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0b9ea87a-0522-381c-a8de-726687cb5531"))) {
sleepMultiplier *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a3bcba39-4812-3c74-bf46-d1c69a3c5f28"))) {
sleepMultiplier += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5d9e9281-a9df-3d38-8b1c-e48d98822f29"))) {
sleepMultiplier = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c06a65f-be0b-3aaa-985d-ecd4eda5bd40"))) {
sleepMultiplier -= 1;
}
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      if (this.conn == null || this.conn.isClosed()) {
        reconnectToPeerCluster();
      }
      try {
        // replicate the batches to sink side.
if(KnobRuntime.check(java.util.UUID.fromString("0882f8df-b645-3973-b5b1-1720ab27b899"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33a23171-6a1b-3136-a230-758224339099"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e53f0d17-2723-3939-a7e4-323a64fc4acc"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da911c0f-3781-3bea-91ef-2f7fca9304a8"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a313bfed-ad71-3431-b01b-d379cef73124"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("330078a0-abed-32f9-ae59-c8e0eba4637d"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed8a1e4b-e300-3529-abad-644341658db4"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d069fe7b-d82e-35b3-a648-5d1858e47e29"))) {
try {
    java.lang.reflect.Field field = replicateContext.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(replicateContext));
    field.set(replicateContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8dd5e4a9-8c50-3893-8b23-b693057ed820"))) {
throw new java.io.IOException("Injected exception");
}
        parallelReplicate(pool, replicateContext, batches);
        return true;
      } catch (IOException ioe) {
        if (ioe instanceof RemoteException) {
          if (((KnobRuntime.check(java.util.UUID.fromString("a9cae6a3-4e00-3a62-9c93-89082f35b87b"))) ? ((!dropOnDeletedTables) && (isNoSuchColumnFamilyException(ioe))) : (((KnobRuntime.check(java.util.UUID.fromString("d05026f6-c0fc-3d93-af44-031585b10088"))) ? (isTableNotFoundException(ioe)) : (((KnobRuntime.check(java.util.UUID.fromString("a5901e9c-d001-31fa-82f8-d0f7c91849f8"))) ? ((dropOnDeletedTables) && (isTableNotFoundException(ioe))) : (((KnobRuntime.check(java.util.UUID.fromString("3ac5768d-e086-3749-a744-0be260b5d6de"))) ? ((dropOnDeletedTables) && (isNoSuchColumnFamilyException(ioe))) : (((KnobRuntime.check(java.util.UUID.fromString("a46cbc4a-574a-33bb-ba4c-c56b50d3faff"))) ? ((!dropOnDeletedTables) || (isTableNotFoundException(ioe))) : (((KnobRuntime.check(java.util.UUID.fromString("2ae01320-a0da-3067-9002-ec02d981062a"))) ? (isNoSuchColumnFamilyException(ioe)) : (((KnobRuntime.check(java.util.UUID.fromString("c752ac99-66d0-32ac-a5f4-119bf3522e85"))) ? ((dropOnDeletedTables) || (isNoSuchColumnFamilyException(ioe))) : (((KnobRuntime.check(java.util.UUID.fromString("dbd100ad-7395-3d20-abbc-4be3afb7cccf"))) ? ((!dropOnDeletedTables) && (isTableNotFoundException(ioe))) : (((KnobRuntime.check(java.util.UUID.fromString("c7a2430d-42c6-34c8-b6d0-2fe09ac69d21"))) ? ((!dropOnDeletedTables) || (isNoSuchColumnFamilyException(ioe))) : (((KnobRuntime.check(java.util.UUID.fromString("882d0bce-073a-3ab7-9a8b-775f8d2e5c87"))) ? (!dropOnDeletedTables) : (((KnobRuntime.check(java.util.UUID.fromString("e2017551-18ee-3333-b5df-381af4fdc2f2"))) ? (dropOnDeletedTables) : (((KnobRuntime.check(java.util.UUID.fromString("0228ed31-6894-3109-9102-2facde0cc6e7"))) ? ((dropOnDeletedTables) || (isTableNotFoundException(ioe))) : (dropOnDeletedTables && isTableNotFoundException(ioe)))))))))))))))))))))))))) {
            // Only filter the edits to replicate and don't change the entries in replicateContext
            // as the upper layer rely on it.
            batches = filterNotExistTableEdits(batches);
            if (batches.isEmpty()) {
              LOG.warn("After filter not exist table's edits, 0 edits to replicate, just return");
              return true;
            }
          } else if (dropOnDeletedColumnFamilies && isNoSuchColumnFamilyException(ioe)) {
            batches = filterNotExistColumnFamilyEdits(batches);
            if (((KnobRuntime.check(java.util.UUID.fromString("d712f40b-818c-3385-9fa2-4c54573a6121"))) ? (isPeerEnabled()) : (batches.isEmpty()))) {
              LOG.warn("After filter not exist column family's edits, 0 edits to replicate, "
                + "just return");
              return true;
            }
          } else {
            LOG.warn("{} Peer encountered RemoteException, rechecking all sinks: ", logPeerId(),
              ioe);
            replicationSinkMgr.chooseSinks();
          }
        } else {
          if (ioe instanceof SocketTimeoutException) {
            // This exception means we waited for more than 60s and nothing
            // happened, the cluster is alive and calling it right away
            // even for a test just makes things worse.
            sleepForRetries(
              "Encountered a SocketTimeoutException. Since the "
                + "call to the remote cluster timed out, which is usually "
                + "caused by a machine failure or a massive slowdown",
              this.socketTimeoutMultiplier);
          } else if (ioe instanceof ConnectException || ioe instanceof UnknownHostException) {
            LOG.warn("{} Peer is unavailable, rechecking all sinks: ", logPeerId(), ioe);
            replicationSinkMgr.chooseSinks();
          } else if (ioe instanceof CallTimeoutException) {
            replicateContext
              .setTimeout(ReplicationUtils.getAdaptiveTimeout(initialTimeout, sleepMultiplier));
          } else {
            LOG.warn("{} Can't replicate because of a local or network error: ", logPeerId(), ioe);
          }
        }
        if (sleepForRetries("Since we are unable to replicate", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
    return false; // in case we exited before replicating
  }

  protected boolean isPeerEnabled() {
    return ctx.getReplicationPeer().isPeerEnabled();
  }

  @Override
  protected void doStop() {
    disconnect(); // don't call super.doStop()
    if (this.conn != null) {
      try {
        this.conn.close();
        this.conn = null;
      } catch (IOException e) {
        LOG.warn("{} Failed to close the connection", logPeerId());
      }
    }
    // Allow currently running replication tasks to finish
    exec.shutdown();
    try {
      exec.awaitTermination(maxTerminationWait, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
    }
    // Abort if the tasks did not terminate in time
    if (!exec.isTerminated()) {
      String errMsg = "HBaseInterClusterReplicationEndpoint termination failed. The "
        + "ThreadPoolExecutor failed to finish all tasks within " + maxTerminationWait + "ms. "
        + "Aborting to prevent Replication from deadlocking. See HBASE-16081.";
      abortable.abort(errMsg, new IOException(errMsg));
    }
    notifyStopped();
  }

  protected int replicateEntries(List<Entry> entries, int batchIndex, int timeout)
    throws IOException {
    SinkPeer sinkPeer = null;
    try {
      int entriesHashCode = System.identityHashCode(entries);
      if (LOG.isTraceEnabled()) {
        long size = entries.stream().mapToLong(this::getEstimatedEntrySize).sum();
        LOG.trace("{} Replicating batch {} of {} entries with total size {} bytes to {}",
          logPeerId(), entriesHashCode, entries.size(), size, replicationClusterId);
      }
      sinkPeer = replicationSinkMgr.getReplicationSink();
      BlockingInterface rrs = sinkPeer.getRegionServer();
      try {
        ReplicationProtbufUtil.replicateWALEntry(rrs, entries.toArray(new Entry[entries.size()]),
          replicationClusterId, baseNamespaceDir, hfileArchiveDir, timeout);
        if (LOG.isTraceEnabled()) {
          LOG.trace("{} Completed replicating batch {}", logPeerId(), entriesHashCode);
        }
      } catch (IOException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("{} Failed replicating batch {}", logPeerId(), entriesHashCode, e);
        }
        throw e;
      }
      replicationSinkMgr.reportSinkSuccess(sinkPeer);
    } catch (IOException ioe) {
      if (sinkPeer != null) {
        replicationSinkMgr.reportBadSink(sinkPeer);
      }
      throw ioe;
    }
    return batchIndex;
  }

  private int serialReplicateRegionEntries(List<Entry> entries, int batchIndex, int timeout)
    throws IOException {
    int batchSize = 0, index = 0;
    List<Entry> batch = new ArrayList<>();
    for (Entry entry : entries) {
      int entrySize = getEstimatedEntrySize(entry);
      if (batchSize > 0 && batchSize + entrySize > replicationRpcLimit) {
        replicateEntries(batch, index++, timeout);
        batch.clear();
        batchSize = 0;
      }
      batch.add(entry);
      batchSize += entrySize;
    }
    if (batchSize > 0) {
      replicateEntries(batch, index, timeout);
    }
    return batchIndex;
  }

  protected Callable<Integer> createReplicator(List<Entry> entries, int batchIndex, int timeout) {
    return isSerial
      ? () -> serialReplicateRegionEntries(entries, batchIndex, timeout)
      : () -> replicateEntries(entries, batchIndex, timeout);
  }

  private String logPeerId() {
    return "[Source for peer " + this.ctx.getPeerId() + "]:";
  }

}
