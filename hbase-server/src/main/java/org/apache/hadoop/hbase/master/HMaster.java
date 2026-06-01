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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.master.cleaner.HFileCleaner.CUSTOM_POOL_SIZE;
import static org.apache.hadoop.hbase.util.DNS.MASTER_HOSTNAME_KEY;

import com.google.errorprone.annotations.RestrictedApi;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.PleaseRestartMasterException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ServerTask;
import org.apache.hadoop.hbase.ServerTaskBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.MasterStoppedException;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.favored.FavoredNodesPromoter;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.MasterRpcServices.BalanceSwitchMode;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStateStore;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.master.balancer.BalancerChore;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerStateStore;
import org.apache.hadoop.hbase.master.balancer.MaintenanceLoadBalancer;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.ReplicationBarrierCleaner;
import org.apache.hadoop.hbase.master.cleaner.SnapshotCleanerChore;
import org.apache.hadoop.hbase.master.hbck.HbckChore;
import org.apache.hadoop.hbase.master.http.MasterDumpServlet;
import org.apache.hadoop.hbase.master.http.MasterRedirectServlet;
import org.apache.hadoop.hbase.master.http.MasterStatusServlet;
import org.apache.hadoop.hbase.master.http.api_v1.ResourceConfigFactory;
import org.apache.hadoop.hbase.master.http.hbck.HbckConfigFactory;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.migrate.RollingUpgradeChore;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerManager;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerStateStore;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.EnableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.FlushTableProcedure;
import org.apache.hadoop.hbase.master.procedure.InitMetaProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil.NonceProcedureRunnable;
import org.apache.hadoop.hbase.master.procedure.ModifyTableProcedure;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.procedure.TruncateRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.TruncateTableProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.master.replication.AddPeerProcedure;
import org.apache.hadoop.hbase.master.replication.DisablePeerProcedure;
import org.apache.hadoop.hbase.master.replication.EnablePeerProcedure;
import org.apache.hadoop.hbase.master.replication.ModifyPeerProcedure;
import org.apache.hadoop.hbase.master.replication.RemovePeerProcedure;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerModificationStateStore;
import org.apache.hadoop.hbase.master.replication.UpdatePeerConfigProcedure;
import org.apache.hadoop.hbase.master.slowlog.SlowLogMasterService;
import org.apache.hadoop.hbase.master.snapshot.SnapshotCleanupStateStore;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator;
import org.apache.hadoop.hbase.master.zksyncer.MasterAddressSyncer;
import org.apache.hadoop.hbase.master.zksyncer.MetaLocationSyncer;
import org.apache.hadoop.hbase.mob.MobFileCleanerChore;
import org.apache.hadoop.hbase.mob.MobFileCompactionChore;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskGroup;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.flush.MasterFlushTableProcedureManager;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureStoreListener;
import org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.MasterQuotasObserver;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.SnapshotQuotaObserverChore;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotNotifier;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotNotifierFactory;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.storefiletracker.ModifyColumnFamilyStoreFileTrackerProcedure;
import org.apache.hadoop.hbase.regionserver.storefiletracker.ModifyTableStoreFileTrackerProcedure;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationPeerConfigUpgrader;
import org.apache.hadoop.hbase.replication.master.ReplicationSinkTrackerTableCreator;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.webapp.WebAppContext;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ResourceConfig;
import org.apache.hbase.thirdparty.org.glassfish.jersey.servlet.ServletContainer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * HMaster is the "master server" for HBase. An HBase cluster has one active master. If many masters
 * are started, all compete. Whichever wins goes on to run the cluster. All others park themselves
 * in their constructor until master or cluster shutdown or until the active master loses its lease
 * in zookeeper. Thereafter, all running master jostle to take over master role.
 * <p/>
 * The Master can be asked shutdown the cluster. See {@link #shutdown()}. In this case it will tell
 * all regionservers to go down and then wait on them all reporting in that they are down. This
 * master will then shut itself down.
 * <p/>
 * You can also shutdown just this master. Call {@link #stopMaster()}.
 * @see org.apache.zookeeper.Watcher
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings("deprecation")
public class HMaster extends HRegionServer implements MasterServices {

  private static final Logger LOG = LoggerFactory.getLogger(HMaster.class);

  // MASTER is name of the webapp and the attribute name used stuffing this
  // instance into a web context !! AND OTHER PLACES !!
  public static final String MASTER = "master";

  // Manager and zk listener for master election
  private final ActiveMasterManager activeMasterManager;
  // Region server tracker
  private final RegionServerTracker regionServerTracker;
  // Draining region server tracker
  private DrainingServerTracker drainingServerTracker;
  // Tracker for load balancer state
  LoadBalancerStateStore loadBalancerStateStore;
  // Tracker for meta location, if any client ZK quorum specified
  private MetaLocationSyncer metaLocationSyncer;
  // Tracker for active master location, if any client ZK quorum specified
  @InterfaceAudience.Private
  MasterAddressSyncer masterAddressSyncer;
  // Tracker for auto snapshot cleanup state
  SnapshotCleanupStateStore snapshotCleanupStateStore;

  // Tracker for split and merge state
  private SplitOrMergeStateStore splitOrMergeStateStore;

  private ClusterSchemaService clusterSchemaService;

  public static final String HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS =
    "hbase.master.wait.on.service.seconds";
  public static final int DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS = 5 * 60;

  public static final String HBASE_MASTER_CLEANER_INTERVAL = "hbase.master.cleaner.interval";

  public static final int DEFAULT_HBASE_MASTER_CLEANER_INTERVAL = 600 * 1000;

  // Metrics for the HMaster
  final MetricsMaster metricsMaster;
  // file system manager for the master FS operations
  private MasterFileSystem fileSystemManager;
  private MasterWalManager walManager;

  // manager to manage procedure-based WAL splitting, can be null if current
  // is zk-based WAL splitting. SplitWALManager will replace SplitLogManager
  // and MasterWalManager, which means zk-based WAL splitting code will be
  // useless after we switch to the procedure-based one. our eventual goal
  // is to remove all the zk-based WAL splitting code.
  private SplitWALManager splitWALManager;

  // server manager to deal with region server info
  private volatile ServerManager serverManager;

  // manager of assignment nodes in zookeeper
  private AssignmentManager assignmentManager;

  // manager of replication
  private ReplicationPeerManager replicationPeerManager;

  // buffer for "fatal error" notices from region servers
  // in the cluster. This is only used for assisting
  // operations/debugging.
  MemoryBoundedLogMessageBuffer rsFatals;

  // flag set after we become the active master (used for testing)
  private volatile boolean activeMaster = false;

  // flag set after we complete initialization once active
  private final ProcedureEvent<?> initialized = new ProcedureEvent<>("master initialized");

  // flag set after master services are started,
  // initialization may have not completed yet.
  volatile boolean serviceStarted = false;

  // Maximum time we should run balancer for
  private final int maxBalancingTime;
  // Maximum percent of regions in transition when balancing
  private final double maxRitPercent;

  private final LockManager lockManager = new LockManager(this);

  private LoadBalancer balancer;
  private BalancerChore balancerChore;
  private static boolean disableBalancerChoreForTest = false;
  private RegionNormalizerManager regionNormalizerManager;
  private ClusterStatusChore clusterStatusChore;
  private ClusterStatusPublisher clusterStatusPublisherChore = null;
  private SnapshotCleanerChore snapshotCleanerChore = null;

  private HbckChore hbckChore;
  CatalogJanitor catalogJanitorChore;
  // Threadpool for scanning the Old logs directory, used by the LogCleaner
  private DirScanPool logCleanerPool;
  private LogCleaner logCleaner;
  // HFile cleaners for the custom hfile archive paths and the default archive path
  // The archive path cleaner is the first element
  private List<HFileCleaner> hfileCleaners = new ArrayList<>();
  // The hfile cleaner paths, including custom paths and the default archive path
  private List<Path> hfileCleanerPaths = new ArrayList<>();
  // The shared hfile cleaner pool for the custom archive paths
  private DirScanPool sharedHFileCleanerPool;
  // The exclusive hfile cleaner pool for scanning the archive directory
  private DirScanPool exclusiveHFileCleanerPool;
  private ReplicationBarrierCleaner replicationBarrierCleaner;
  private MobFileCleanerChore mobFileCleanerChore;
  private MobFileCompactionChore mobFileCompactionChore;
  private RollingUpgradeChore rollingUpgradeChore;
  // used to synchronize the mobCompactionStates
  private final IdLock mobCompactionLock = new IdLock();
  // save the information of mob compactions in tables.
  // the key is table name, the value is the number of compactions in that table.
  private Map<TableName, AtomicInteger> mobCompactionStates = Maps.newConcurrentMap();

  volatile MasterCoprocessorHost cpHost;

  private final boolean preLoadTableDescriptors;

  // Time stamps for when a hmaster became active
  private long masterActiveTime;

  // Time stamp for when HMaster finishes becoming Active Master
  private long masterFinishedInitializationTime;

  Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  // monitor for snapshot of hbase tables
  SnapshotManager snapshotManager;
  // monitor for distributed procedures
  private MasterProcedureManagerHost mpmHost;

  private RegionsRecoveryChore regionsRecoveryChore = null;

  private RegionsRecoveryConfigManager regionsRecoveryConfigManager = null;
  // it is assigned after 'initialized' guard set to true, so should be volatile
  private volatile MasterQuotaManager quotaManager;
  private SpaceQuotaSnapshotNotifier spaceQuotaSnapshotNotifier;
  private QuotaObserverChore quotaObserverChore;
  private SnapshotQuotaObserverChore snapshotQuotaChore;
  private OldWALsDirSizeChore oldWALsDirSizeChore;

  private ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private ProcedureStore procedureStore;

  // the master local storage to store procedure data, meta region locations, etc.
  private MasterRegion masterRegion;

  private RegionServerList rsListStorage;

  // handle table states
  private TableStateManager tableStateManager;

  /* Handle favored nodes information */
  private FavoredNodesManager favoredNodesManager;

  /** jetty server for master to redirect requests to regionserver infoServer */
  private Server masterJettyServer;

  // Determine if we should do normal startup or minimal "single-user" mode with no region
  // servers and no user tables. Useful for repair and recovery of hbase:meta
  private final boolean maintenanceMode;
  static final String MAINTENANCE_MODE = "hbase.master.maintenance_mode";

  // Cached clusterId on stand by masters to serve clusterID requests from clients.
  private final CachedClusterId cachedClusterId;

  public static final String WARMUP_BEFORE_MOVE = "hbase.master.warmup.before.move";
  private static final boolean DEFAULT_WARMUP_BEFORE_MOVE = true;

  /**
   * Use RSProcedureDispatcher instance to initiate master to rs remote procedure execution. Use
   * this config to extend RSProcedureDispatcher (mainly for testing purpose).
   */
  public static final String HBASE_MASTER_RSPROC_DISPATCHER_CLASS =
    "hbase.master.rsproc.dispatcher.class";
  private static final String DEFAULT_HBASE_MASTER_RSPROC_DISPATCHER_CLASS =
    RSProcedureDispatcher.class.getName();

  private TaskGroup startupTaskGroup;

  /**
   * Store whether we allow replication peer modification operations.
   */
  private ReplicationPeerModificationStateStore replicationPeerModificationStateStore;

  /**
   * Initializes the HMaster. The steps are as follows:
   * <p>
   * <ol>
   * <li>Initialize the local HRegionServer
   * <li>Start the ActiveMasterManager.
   * </ol>
   * <p>
   * Remaining steps of initialization occur in {@link #finishActiveMasterInitialization()} after
   * the master becomes the active one.
   */
  public HMaster(final Configuration conf) throws IOException {
    super(conf);
    final Span span = TraceUtil.createSpan("HMaster.cxtor");
    try (Scope ignored = span.makeCurrent()) {
      if (conf.getBoolean(MAINTENANCE_MODE, false)) {
        LOG.info("Detected {}=true via configuration.", MAINTENANCE_MODE);
        maintenanceMode = true;
      } else if (Boolean.getBoolean(MAINTENANCE_MODE)) {
        LOG.info("Detected {}=true via environment variables.", MAINTENANCE_MODE);
        maintenanceMode = true;
      } else {
        maintenanceMode = false;
      }
      this.rsFatals = new MemoryBoundedLogMessageBuffer(
        conf.getLong("hbase.master.buffer.for.rs.fatals", 1 * 1024 * 1024));
      LOG.info("hbase.rootdir={}, hbase.cluster.distributed={}", getDataRootDir(),
        this.conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, false));

      // Disable usage of meta replicas in the master
      this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);

      decorateMasterConfiguration(this.conf);

      // Hack! Maps DFSClient => Master for logs. HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapreduce.task.attempt.id") == null) {
        this.conf.set("mapreduce.task.attempt.id", "hb_m_" + this.serverName.toString());
      }

      this.metricsMaster = new MetricsMaster(new MetricsMasterWrapperImpl(this));

      // preload table descriptor at startup
      this.preLoadTableDescriptors = conf.getBoolean("hbase.master.preload.tabledescriptors", true);

      this.maxBalancingTime = getMaxBalancingTime();
      this.maxRitPercent = conf.getDouble(HConstants.HBASE_MASTER_BALANCER_MAX_RIT_PERCENT,
        HConstants.DEFAULT_HBASE_MASTER_BALANCER_MAX_RIT_PERCENT);

      // Do we publish the status?
      boolean shouldPublish =
        conf.getBoolean(HConstants.STATUS_PUBLISHED, HConstants.STATUS_PUBLISHED_DEFAULT);
      Class<? extends ClusterStatusPublisher.Publisher> publisherClass =
        conf.getClass(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS,
          ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS,
          ClusterStatusPublisher.Publisher.class);

      if (((KnobRuntime.check(java.util.UUID.fromString("a5701f6e-c07d-391d-905e-db5dc668fd18"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("463bba95-1989-3cbf-9358-70d9fac772b3"))) ? (!shouldPublish) : (((KnobRuntime.check(java.util.UUID.fromString("f0625a95-8d1a-31dc-933b-fc3d165c24e0"))) ? (true) : (shouldPublish))))))) {
        if (publisherClass == null) {
          LOG.warn(HConstants.STATUS_PUBLISHED + " is true, but "
            + ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS
            + " is not set - not publishing status");
        } else {
          clusterStatusPublisherChore = new ClusterStatusPublisher(this, conf, publisherClass);
          if (KnobRuntime.check(java.util.UUID.fromString("946e6e6b-6bd9-367e-8d6e-48eaa2129e5e"))) { LOG.info("Created {}", this.clusterStatusPublisherChore); } else { LOG.debug("Created {}", this.clusterStatusPublisherChore); }
          getChoreService().scheduleChore(clusterStatusPublisherChore);
        }
      }

      this.activeMasterManager = createActiveMasterManager(zooKeeper, serverName, this);

      cachedClusterId = new CachedClusterId(this, conf);

      this.regionServerTracker = new RegionServerTracker(zooKeeper, this);
      span.setStatus(StatusCode.OK);
    } catch (Throwable t) {
      // Make sure we log the exception. HMaster is often started via reflection and the
      // cause of failed startup is lost.
      TraceUtil.setError(span, t);
      LOG.error("Failed construction of Master", t);
      throw t;
    } finally {
      span.end();
    }
  }

  /**
   * Protected to have custom implementations in tests override the default ActiveMaster
   * implementation.
   */
  protected ActiveMasterManager createActiveMasterManager(ZKWatcher zk, ServerName sn,
    org.apache.hadoop.hbase.Server server) throws InterruptedIOException {
if(KnobRuntime.check(java.util.UUID.fromString("09442861-7756-3279-b4bd-1b13c2f886ed"))) {
try {
    java.lang.reflect.Field field = zk.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(zk));
    field.set(zk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a73d80df-4733-35f4-87f7-076487288cd7"))) {
throw new InterruptedIOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f936f820-d133-36d0-bee4-8b454a31c229"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6e474d3c-2ac0-312d-baf1-0fc6d1be41f5"))) {
return null;
}
    return new ActiveMasterManager(zk, sn, server);
  }

  @Override
  protected String getUseThisHostnameInstead(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("b3e4b080-6ed0-3c72-9b6f-f00ae15200d7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9cc3122d-248b-32fd-b60a-dff390fc6048"))) {
return null;
}
    return conf.get(MASTER_HOSTNAME_KEY);
  }

  private void registerConfigurationObservers() {
    configurationManager.registerObserver(this.rpcServices);
    configurationManager.registerObserver(this);
  }

  @Override
  protected void preUpdateConfiguration() throws IOException {
    if (cpHost != null) {
if(KnobRuntime.check(java.util.UUID.fromString("51d762b4-cd50-3e9b-8897-03239bdc6da1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("73ba3107-23f2-3246-aab1-60487cb63897"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("c735ff9b-db59-3e03-8a54-7533599daab5"))) { decorateMasterConfiguration(conf); } else if (KnobRuntime.check(java.util.UUID.fromString("7dbc5bee-d930-306a-bf3b-0e3c07acd877"))) { cpHost.postUpdateConfiguration(conf); } else { cpHost.preUpdateConfiguration(conf); }
    }
  }

  @Override
  protected void postUpdateConfiguration() throws IOException {
    if (cpHost != null) {
      cpHost.postUpdateConfiguration(conf);
    }
  }

  // Main run loop. Calls through to the regionserver run loop AFTER becoming active Master; will
  // block in here until then.
  @Override
  public void run() {
    try {
      registerConfigurationObservers();
      Threads.setDaemonThreadRunning(new Thread(() -> TraceUtil.trace(() -> {
        try {
          int infoPort = putUpJettyServer();
          startActiveMasterManager(infoPort);
        } catch (Throwable t) {
          // Make sure we log the exception.
          String error = "Failed to become Active Master";
          LOG.error(error, t);
          // Abort should have been called already.
          if (!isAborted()) {
            abort(error, t);
          }
        }
      }, "HMaster.becomeActiveMaster")), getName() + ":becomeActiveMaster");
      // Fall in here even if we have been aborted. Need to run the shutdown services and
      // the super run call will do this for us.
      if (KnobRuntime.check(java.util.UUID.fromString("fec0d231-8644-3999-b7e2-5b6efc2d9c5d"))) { super.configureInfoServer(); } else if (KnobRuntime.check(java.util.UUID.fromString("c4ef8d3d-4306-3582-84b7-79b23dbc3c39"))) { registerConfigurationObservers(); } else { super.run(); }
    } finally {
      final Span span = TraceUtil.createSpan("HMaster exiting main loop");
      try (Scope ignored = span.makeCurrent()) {
        if (this.clusterSchemaService != null) {
          // If on way out, then we are no longer active master.
          this.clusterSchemaService.stopAsync();
          try {
if(KnobRuntime.check(java.util.UUID.fromString("5c781f0a-fa9d-3d21-8d6e-14ca5d577ed9"))) {
throw new java.util.concurrent.TimeoutException("Injected exception");
}
            this.clusterSchemaService
              .awaitTerminated(getConfiguration().getInt(HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS,
                DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS), TimeUnit.SECONDS);
          } catch (TimeoutException te) {
            if (KnobRuntime.check(java.util.UUID.fromString("7207af78-a7b7-3b98-8318-bf54a936c54d"))) { LOG.error("Failed shutdown of clusterSchemaService", te); } else if (KnobRuntime.check(java.util.UUID.fromString("8a116dfc-d193-3e6a-8dd2-43feb04fde7b"))) { abort("Failed shutdown of clusterSchemaService", te); } else { LOG.warn("Failed shutdown of clusterSchemaService", te); }
          }
        }
        this.activeMaster = false;
        span.setStatus(StatusCode.OK);
      } finally {
        span.end();
      }
    }
  }

  // return the actual infoPort, -1 means disable info server.
  private int putUpJettyServer() throws IOException {
    if (!conf.getBoolean("hbase.master.infoserver.redirect", true)) {
      return -1;
    }
    final int infoPort =
      conf.getInt("hbase.master.info.port.orig", HConstants.DEFAULT_MASTER_INFOPORT);
    // -1 is for disabling info server, so no redirecting
    if (((KnobRuntime.check(java.util.UUID.fromString("a322cecf-28ee-3bf3-b876-13cafb247d9d"))) ? ((infoPort < 0) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8885e16f-6bdc-3cb1-afe2-fe2964488bf9"))) ? (((-1) > (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("0bcc00c4-5b95-3375-83df-30dd07ca3f06"))) ? (((-1) == (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("42ce8ebf-3cc9-30ec-ae97-3f4572047be6"))) ? (((-1) >= (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("fa1a5ef9-da34-3ffb-b24b-c22853b76e48"))) ? (((-1) != (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("c209fc5a-0481-3606-9cfe-fd38818a60f6"))) ? (((-1) > (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("67d24986-3feb-3839-88aa-5d699a9b4094"))) ? (((-1) < (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a5b70e88-2579-3771-9412-9359f78dcafa"))) ? (((-1) >= (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("33b0331f-f5bd-3cfc-b8e0-f1b1afe642bf"))) ? (((-1) < (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("c822cc43-0c8e-3073-b1d8-0808db1d541b"))) ? (((infoPort) != (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("dbbd2308-6187-3311-a6dd-c68d9bc8c396"))) ? (((infoPort) < (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("cd6c3d4b-7bfd-33f6-b147-5602ca2c8127"))) ? ((infoPort) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("64adb9ec-15a6-37c3-8e43-3f7191edc2e6"))) ? (((-1) > (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0f894aba-0f9a-30f4-adef-14a69ecef759"))) ? (((infoPort) < (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f635a29f-b989-3536-9e38-16525399635e"))) ? ((infoPort) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("de6d6823-fb94-337a-ab0f-e06a080db793"))) ? (((infoPort) == (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("94974774-1fce-3558-b3e2-858bac5e53c2"))) ? ((infoPort < 0) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("5b44fa61-d4f0-3d49-9747-4d00a65de2ba"))) ? (((infoPort) > (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6f903ff5-431c-3ad4-8880-58b0edcf91f3"))) ? (((infoPort) != (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1c06236b-f533-3cd2-a34c-a1ec00dc9ed1"))) ? (((infoPort) < (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("718a7fb0-eed5-3ac9-85d9-a1fe8a6daf6b"))) ? (((-1) != (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("93e4d072-07c4-3556-8f43-d3d270dd2bcb"))) ? (((infoPort) >= (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("0375f11d-5477-36b1-9ff4-ed05f1e629cc"))) ? (((-1) == (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0b1c89a5-1ee7-3693-87e0-6be31d4a9418"))) ? (((-1) < (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7b24b395-a7d9-375e-9de7-ca1c0e95dc34"))) ? (infoServer == null) : (((KnobRuntime.check(java.util.UUID.fromString("f9409024-6088-3101-9945-18f072348fb7"))) ? (((infoPort) == (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a18d0e9e-d659-3606-bf53-7f11d3b60e23"))) ? ((-1) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("40c5141b-fe7e-3a92-ac83-38570061dd89"))) ? (((infoPort) <= (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("9980f283-43af-32ff-b511-861dcc2ca455"))) ? (((-1) != (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("c2172953-3337-38b4-8435-4b26869a2518"))) ? (((-1) == (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("336c2f95-76a7-300f-9d24-66fab01c0906"))) ? ((infoPort) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6dc7d7fb-701b-3593-9b0d-7c3baad65d28"))) ? ((infoPort < 0) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6b0d6e66-7550-393b-9043-14b1e8090135"))) ? (((infoPort) == (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e8fda93f-51e5-3aea-9f78-5a26f169ea1f"))) ? (((-1) == (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("75ecfff4-54dc-360b-84a5-63ac19837f0f"))) ? (((-1) >= (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e7701d2f-4472-32b1-a897-37f3a865e2ec"))) ? (((infoPort) <= (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("201d6e60-d049-3d55-8051-9526c6a8b208"))) ? ((-1) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e75dd7e2-12cc-3333-b77e-eed4c476f96e"))) ? ((-1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bcf77eff-4e48-3ed8-9b7b-8d8e122773ff"))) ? (((-1) >= (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2a186fea-ea56-3e2a-a961-f9ed3a14666b"))) ? (infoPort < 0) : (((KnobRuntime.check(java.util.UUID.fromString("7e701ed0-4da6-3d35-9f15-d173d009be36"))) ? (((-1) <= (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("520c8689-05ff-3739-9604-7bb53297dad0"))) ? (((-1) >= (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("7ff8191c-67a3-3503-91f7-efada2fd8e7e"))) ? (((infoPort) <= (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("777c81a7-1164-39b2-a916-f648ba828027"))) ? (((infoPort) != (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3c717709-c2e8-3348-83f5-a45eac656b4d"))) ? (((infoPort) > (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a691ca56-281e-3e41-82fa-154a9f708020"))) ? ((infoPort) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a312d2bd-82ec-36f7-b64e-6c05592477d1"))) ? (((-1) <= (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("925e822e-5175-3111-8791-5e2ecce55609"))) ? (((-1) > (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("573a7d10-f2dd-39b0-8f36-cf71db01d54e"))) ? (((infoPort) > (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("a8d6ca30-4f21-3b5e-818e-1747b141fd1e"))) ? (((-1) != (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("31a54323-f5ad-3dec-a040-3492bdab6744"))) ? (((infoPort) != (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("17640d4b-5111-31c8-b148-7dfe70c17521"))) ? (((-1) > (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9cdf8200-85c2-3854-af62-acd8b75dfca4"))) ? (((-1) < (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("42c9e3b1-6dda-3e04-b4c5-01b83b411d22"))) ? (((infoPort) <= (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f165981f-0fdd-3f81-9017-6eb8b7936b93"))) ? (((infoPort) < (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("464bf082-2fdd-3b21-9944-cc9b5cd3fb4e"))) ? ((infoServer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d66994ed-ed1e-3fbc-b712-e9f3e737462e"))) ? (((-1) <= (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c0d588a9-d43b-354a-897a-ea4e88171a73"))) ? (((infoPort) == (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4ae4abc9-00f7-36cd-af56-6bdbbe0d9983"))) ? (((-1) <= (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bcef8ee5-6230-3899-8d34-b93197e4dead"))) ? (((infoPort) != (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("92ca7287-c109-304b-bcc2-dafa4cea899d"))) ? (((-1) == (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("69d390c4-6f42-3ecb-9b2e-bb719b08ef20"))) ? (((infoPort) == (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("99e75e65-295a-32f4-8a3d-fe87cce45a7f"))) ? ((infoPort) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("22e32dae-0f7a-38ba-8289-382f6abeaf10"))) ? (((infoPort) >= (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3190cf04-894e-3d05-a8bf-ae150ae54393"))) ? (((infoPort) >= (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0a0052cb-2e5c-35e5-8da3-8b733ec35b01"))) ? ((infoPort < 0) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c25172de-e30e-3132-92dc-6e2f34e30dc2"))) ? (((infoPort) < (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c8414373-cdc0-3724-8379-a915f0a32dfc"))) ? (((infoPort) >= (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("7de3d536-2b9b-378a-9168-78a145409328"))) ? (((infoPort) > (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bb794890-ac70-3163-9fc4-3c858963ed1a"))) ? (((-1) != (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("38526bca-0dfd-302b-8918-8ad4027fa94a"))) ? (((infoPort) > (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("86cbc010-7126-3239-ab59-db790c823e27"))) ? ((-1) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("17a0f79e-c19a-30d0-8886-11197fdacc68"))) ? (((infoPort) <= (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4c3b5556-77fc-3dd9-b7a6-b5dc5143e0e3"))) ? ((-1) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f1fb8e0d-fac1-3c0f-8b6d-820c8ce6fa88"))) ? (((-1) <= (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1ec45c41-86c6-3616-a846-60b699b53a82"))) ? (((infoPort) != (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bf834e25-0bdc-30db-b2ae-cf2fe4ce86c4"))) ? ((infoPort < 0) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("fd0a2c5c-23cd-3ac1-bf68-429a7d436e91"))) ? (((-1) < (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ee1822bb-6d59-3b41-a803-028d25ff503d"))) ? ((-1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("83771f39-8e51-31b8-8bd2-e8544293d703"))) ? (((-1) == (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("dc7615a3-4fa6-3d20-a3e9-283744d011c3"))) ? (((infoPort) > (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("cd106db1-14b9-3ef0-bb94-e10eff72d84c"))) ? ((infoPort) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ba721e33-5a53-3a34-aa5e-2c6af7ebe5d2"))) ? (((-1) > (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("02bf5bc9-815e-3934-bda7-147c9744f6c5"))) ? ((infoServer) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("39847d83-1d81-374e-b96b-d33a9cd7b8b6"))) ? (((infoPort) <= (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("59c8eeb1-94e4-3b17-acce-54bb2fb1e786"))) ? (((infoPort) >= (0)) && ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("304e1b6a-c670-3de2-9c56-7b355681385b"))) ? (((infoPort) == (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("71b08c85-d260-396a-8fa5-99c1410ddb65"))) ? (((infoPort) < (0)) || (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("22a38151-6100-3138-8cc5-fc2fa989787f"))) ? ((infoPort < 0) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b42a4793-6686-3dcb-bc00-118d300e77ea"))) ? (((-1) >= (0)) && (infoServer == null)) : (((KnobRuntime.check(java.util.UUID.fromString("905a7523-f074-3055-9fba-8136d0512a9f"))) ? (((infoPort) >= (0)) || ((infoServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("af8decfc-ce49-3210-8c2e-9b5306848576"))) ? (((-1) != (0)) || ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("52b2dc9c-56c8-3dab-87a4-732c3e6a19dc"))) ? (((-1) < (0)) && ((infoServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2de6798e-6d83-3bc5-9871-7c563f131b22"))) ? (((-1) <= (0)) || (infoServer == null)) : (infoPort < 0 || infoServer == null))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return -1;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("ea8fa7d9-7350-3788-9c12-de7717f146c1"))) ? ((1) == (getMaxBalancingTime())) : (((KnobRuntime.check(java.util.UUID.fromString("e0566f46-8aef-3dbd-9370-59c795e15c91"))) ? ((1) == (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("0efd3709-b035-3480-9bef-f70c23345f39"))) ? ((0) != (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("0195e444-e5e5-3237-8731-237f36bb60c2"))) ? ((infoPort) == (getMaxBalancingTime())) : (((KnobRuntime.check(java.util.UUID.fromString("f40d00b6-520f-37a0-981d-4d11c2f61446"))) ? ((1) != (getMaxBalancingTime())) : (((KnobRuntime.check(java.util.UUID.fromString("61255a76-be73-3da1-993d-ba21d9b18f82"))) ? ((0) == (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("6f3ad153-3c4a-3e00-b0f6-fdc9168b507b"))) ? ((-1) != (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("9e4b25d7-0afb-3ac7-b7b9-6da65d1a10f1"))) ? ((-1) != (getMaxBalancingTime())) : (((KnobRuntime.check(java.util.UUID.fromString("b1b78374-c895-3b7d-9f08-297347234007"))) ? ((infoPort) != (getMaxBalancingTime())) : (((KnobRuntime.check(java.util.UUID.fromString("de150c1d-e7f2-3bd7-ba66-7cf60060f474"))) ? ((0) != (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("46d57e8f-33a9-3bd5-97d1-1f34f64dd95f"))) ? ((0) == (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("ed1997fc-22b2-3f35-9c68-a823e82b808c"))) ? ((infoPort) == (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("99eeab9b-d258-388b-bc1a-fad61dce1435"))) ? ((-1) != (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("38abc1ab-f99c-324e-97dd-e7a6eadb458b"))) ? ((1) == (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("9ed63ac1-9a5f-36cd-b936-c7122c681aa5"))) ? ((-1) == (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("2c08774c-981b-3ce3-bd94-729923a68569"))) ? ((infoPort) == (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("69fe791f-2a31-3cdc-a574-901da86a2e77"))) ? ((0) == (getMaxBalancingTime())) : (((KnobRuntime.check(java.util.UUID.fromString("ed9f724d-bcac-386c-8cca-2430cf665097"))) ? ((infoPort) != (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("90e5d528-fac0-33d7-8fcf-8430578cd011"))) ? ((1) != (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("935008bc-9489-3ba3-adc6-9136e27be0b0"))) ? ((-1) == (getMaxBalancingTime())) : (((KnobRuntime.check(java.util.UUID.fromString("cab43a1e-243f-38bc-8e7c-e986bcd8db8a"))) ? ((infoPort) != (infoServer.getPort())) : (((KnobRuntime.check(java.util.UUID.fromString("ee2ea673-7074-3565-bdac-ebb660734b62"))) ? ((-1) == (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("f5d4f328-62e2-3d3c-9beb-83c3eca1186b"))) ? ((1) != (putUpJettyServer())) : (((KnobRuntime.check(java.util.UUID.fromString("ff36ca72-ba62-38ac-a07f-98aaf684eddf"))) ? ((0) != (getMaxBalancingTime())) : (infoPort == infoServer.getPort()))))))))))))))))))))))))))))))))))))))))))))))))) {
      // server is already running
      return ((KnobRuntime.check(java.util.UUID.fromString("811055e5-b681-3af8-acab-942c92faab42"))) ? (0) : (((KnobRuntime.check(java.util.UUID.fromString("37896505-d328-374e-b0d1-61ccdbee01b4"))) ? (-1) : (((KnobRuntime.check(java.util.UUID.fromString("9ab50e65-01f1-340a-b2ca-ad0360586bc3"))) ? (1) : (infoPort))))));
    }
    final String addr = conf.get("hbase.master.info.bindAddress", "0.0.0.0");
    if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
      String msg = "Failed to start redirecting jetty server. Address " + addr
        + " does not belong to this host. Correct configuration parameter: "
        + "hbase.master.info.bindAddress";
      if (KnobRuntime.check(java.util.UUID.fromString("e70b20a1-fedb-3d21-b5ba-9475b2aefe76"))) { LOG.info(msg); } else if (KnobRuntime.check(java.util.UUID.fromString("1f4d8047-a562-3e6c-916c-5245b3c85d44"))) { LOG.warn(msg); } else { LOG.error(msg); }
      throw new IOException(msg);
    }

    // TODO I'm pretty sure we could just add another binding to the InfoServer run by
    // the RegionServer and have it run the RedirectServlet instead of standing up
    // a second entire stack here.
    masterJettyServer = new Server();
    final ServerConnector connector = new ServerConnector(masterJettyServer);
    if (KnobRuntime.check(java.util.UUID.fromString("b570ad07-a5bd-3a17-8373-e17ef11d79c6"))) { stop(addr); } else { connector.setHost(addr); }
    connector.setPort(infoPort);
if(KnobRuntime.check(java.util.UUID.fromString("1fa527cf-1f62-3968-9c87-aa2248ba2658"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_localPort");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("88cd2824-bc8b-31fe-9075-299e59d38295"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedSendBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f944b28d-704d-36b2-aad6-6fde2f3af50d"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedSendBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7acad03-7b5b-3cad-bbf4-0d306376a144"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptQueueSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("353cdfe7-2b14-3723-a7ab-11199f60ba71"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_localPort");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42857ce7-464c-335b-8c8f-8e98c2db6859"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedReceiveBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77e370b7-f0d9-3403-9216-2d0e3786d19a"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptQueueSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5cdc86d0-2959-334e-8830-d91c8778c44a"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedReceiveBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36feb2ad-2e4b-30fa-beff-1a03b5ee8019"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedReceiveBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33ea617d-9a74-3d3f-9015-59a235a97a47"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptQueueSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6f6c189-d670-3861-8274-2c6aeaf6c444"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptQueueSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("81f0e769-744f-3788-ab7d-87da4a50f29d"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedReceiveBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33a4c537-cdb5-345e-a361-a3d4e7fb0e45"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_localPort");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f9fdabf-3751-3aee-a713-e89774f74f9c"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedSendBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a119193-d75e-3a8e-abb1-f6545da34e7c"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_localPort");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fba9d3cf-99ae-352c-804c-d3b62640132e"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_inheritChannel");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(connector);
    field.set(connector, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1889eb5a-667a-3d61-8cce-c2f1aca42a87"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_acceptedSendBufferSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connector));
    field.set(connector, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9ace038-6aa1-39bf-8976-11e66a42c549"))) {
try {
    java.lang.reflect.Field field = connector.getClass().getDeclaredField("_reuseAddress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(connector);
    field.set(connector, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    masterJettyServer.addConnector(connector);
    masterJettyServer.setStopAtShutdown(true);
    masterJettyServer.setHandler(HttpServer.buildGzipHandler(masterJettyServer.getHandler()));

    final String redirectHostname =
      StringUtils.isBlank(useThisHostnameInstead) ? null : useThisHostnameInstead;

    final MasterRedirectServlet redirect = new MasterRedirectServlet(infoServer, redirectHostname);
    final WebAppContext context =
      new WebAppContext(null, "/", null, null, null, null, WebAppContext.NO_SESSIONS);
    context.addServlet(new ServletHolder(redirect), "/*");
    if (KnobRuntime.check(java.util.UUID.fromString("12303a9d-fd74-3917-9f61-f5ece501efb1"))) { context.setServer(new Server()); } else { context.setServer(masterJettyServer); }

    try {
      masterJettyServer.start();
    } catch (Exception e) {
      throw new IOException("Failed to start redirecting jetty server", e);
    }
    return connector.getLocalPort();
  }

  /**
   * For compatibility, if failed with regionserver credentials, try the master one
   */
  @Override
  protected void login(UserProvider user, String host) throws IOException {
    try {
      super.login(user, host);
    } catch (IOException ie) {
      user.login(SecurityConstants.MASTER_KRB_KEYTAB_FILE, SecurityConstants.MASTER_KRB_PRINCIPAL,
        host);
    }
  }

  /**
   * If configured to put regions on active master, wait till a backup master becomes active.
   * Otherwise, loop till the server is stopped or aborted.
   */
  @Override
  protected void waitForMasterActive() {
    if (maintenanceMode) {
      return;
    }
    boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(conf);
    while (!(tablesOnMaster && activeMaster) && !isStopped() && !isAborted()) {
      sleeper.sleep();
    }
  }

  @InterfaceAudience.Private
  public MasterRpcServices getMasterRpcServices() {
    return (MasterRpcServices) rpcServices;
  }

  public boolean balanceSwitch(final boolean b) throws IOException {
    return getMasterRpcServices().switchBalancer(b, BalanceSwitchMode.ASYNC);
  }

  @Override
  protected String getProcessName() {
if(KnobRuntime.check(java.util.UUID.fromString("ab0b826c-bcb9-3b16-a287-00459a3ba588"))) {
return null;
}
    return MASTER;
  }

  @Override
  protected boolean canCreateBaseZNode() {
if(KnobRuntime.check(java.util.UUID.fromString("10095aaf-d1e9-36c4-9cad-e9a190ffce54"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("09f9d6b0-c5f3-3aea-8c43-aaea4209621c"))) {
return false;
}
    return true;
  }

  @Override
  protected boolean canUpdateTableDescriptor() {
    return true;
  }

  @Override
  protected boolean cacheTableDescriptor() {
    return true;
  }

  @Override
  protected RSRpcServices createRpcServices() throws IOException {
    return new MasterRpcServices(this);
  }

  @Override
  protected void configureInfoServer() {
    infoServer.addUnprivilegedServlet("master-status", "/master-status", MasterStatusServlet.class);
    infoServer.addUnprivilegedServlet("api_v1", "/api/v1/*", buildApiV1Servlet());
    infoServer.addUnprivilegedServlet("hbck", "/hbck/*", buildHbckServlet());

    infoServer.setAttribute(MASTER, this);
    if (LoadBalancer.isTablesOnMaster(conf)) {
      super.configureInfoServer();
    }
  }

  private ServletHolder buildApiV1Servlet() {
    final ResourceConfig config = ResourceConfigFactory.createResourceConfig(conf, this);
    return new ServletHolder(new ServletContainer(config));
  }

  private ServletHolder buildHbckServlet() {
    final ResourceConfig config = HbckConfigFactory.createResourceConfig(conf, this);
    return new ServletHolder(new ServletContainer(config));
  }

  @Override
  protected Class<? extends HttpServlet> getDumpServlet() {
    return MasterDumpServlet.class;
  }

  @Override
  public MetricsMaster getMasterMetrics() {
    return metricsMaster;
  }

  /**
   * Initialize all ZK based system trackers. But do not include {@link RegionServerTracker}, it
   * should have already been initialized along with {@link ServerManager}.
   */
  private void initializeZKBasedSystemTrackers()
    throws IOException, KeeperException, ReplicationException, DeserializationException {
    if (maintenanceMode) {
      // in maintenance mode, always use MaintenanceLoadBalancer.
      conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, MaintenanceLoadBalancer.class,
        LoadBalancer.class);
    }
    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.loadBalancerStateStore = new LoadBalancerStateStore(masterRegion, zooKeeper);

    this.regionNormalizerManager =
      RegionNormalizerFactory.createNormalizerManager(conf, masterRegion, zooKeeper, this);
    this.configurationManager.registerObserver(regionNormalizerManager);
    this.regionNormalizerManager.start();

    this.splitOrMergeStateStore = new SplitOrMergeStateStore(masterRegion, zooKeeper, conf);

    this.replicationPeerManager =
      ReplicationPeerManager.create(fileSystemManager.getFileSystem(), zooKeeper, conf, clusterId);
    this.configurationManager.registerObserver(replicationPeerManager);
    this.replicationPeerModificationStateStore =
      new ReplicationPeerModificationStateStore(masterRegion);

    this.drainingServerTracker = new DrainingServerTracker(zooKeeper, this, this.serverManager);
    this.drainingServerTracker.start();

    this.snapshotCleanupStateStore = new SnapshotCleanupStateStore(masterRegion, zooKeeper);

    String clientQuorumServers = conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM);
    boolean clientZkObserverMode = conf.getBoolean(HConstants.CLIENT_ZOOKEEPER_OBSERVER_MODE,
      HConstants.DEFAULT_CLIENT_ZOOKEEPER_OBSERVER_MODE);
    if (((KnobRuntime.check(java.util.UUID.fromString("e9645743-219d-30c4-a8d2-bc6294b80d88"))) ? ((getProcessName()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4bc8bc4d-f94a-3616-8ba2-717a4a9932c1"))) ? ((clientQuorumServers != null) && (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("5d553ac3-886d-3108-8143-37d752db6122"))) ? ((getProcessName() + ":") == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5b41d3b0-4ab0-35cb-8f05-e60820c08212"))) ? (((getProcessName()) != (null)) && (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("1616c0e3-9db4-3ee5-929e-78a38695ac77"))) ? ((getProcessName() + ":") != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a1147dcd-5ece-3abb-893f-304b4fed8621"))) ? ((clientQuorumServers != null) || (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("4a4998a8-5f11-3221-9947-e506fcb2ddac"))) ? ((clientQuorumServers) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e4ce312a-bec5-3a00-ad1a-1c7bab8efa47"))) ? (((getProcessName() + ":") != (null)) && (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("e543cf9e-a6ad-3931-8feb-cb4dffcbe315"))) ? (((clientQuorumServers) != (null)) || (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("19a33bb2-601e-38e4-a61f-3f656618ade9"))) ? (((getProcessName()) == (null)) && (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("3affffba-1f03-303e-9f27-9ef3de0d188e"))) ? ((clientQuorumServers) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7a92927e-75a2-3b8d-b586-149295cac6c7"))) ? (((clientQuorumServers) != (null)) && (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("ff7e592b-de7a-3be4-ad6f-26c7df82f517"))) ? ((getProcessName()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ac120ccf-eff9-3545-b440-0564a6823081"))) ? (!clientZkObserverMode) : (((KnobRuntime.check(java.util.UUID.fromString("655e364f-7181-3c21-bd5b-800a281992aa"))) ? (((getProcessName() + ":") == (null)) || (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("c4724f1d-1650-33ee-95f5-934be7be0bb8"))) ? (((getProcessName() + ":") == (null)) && (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("13f5583e-647d-394a-b8ef-127301e43d42"))) ? (((clientQuorumServers) == (null)) && (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("701be7d0-7587-3716-9aee-0dc38bd0c7a4"))) ? (((clientQuorumServers) == (null)) || (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("7dd69c04-0417-36a2-819a-dab26cbd39b1"))) ? (((getProcessName()) != (null)) || (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("4ce7081d-f1fa-3d3e-b2e3-84e2900200de"))) ? (clientQuorumServers != null) : (((KnobRuntime.check(java.util.UUID.fromString("7eae51a5-a441-3d5c-b39f-df4c73435fe7"))) ? (((getProcessName() + ":") != (null)) || (!clientZkObserverMode)) : (((KnobRuntime.check(java.util.UUID.fromString("0ef7950d-70eb-32a9-9fb7-6a4d7889d866"))) ? (((getProcessName()) == (null)) || (!clientZkObserverMode)) : (clientQuorumServers != null && !clientZkObserverMode))))))))))))))))))))))))))))))))))))))))))))) {
      // we need to take care of the ZK information synchronization
      // if given client ZK are not observer nodes
      ZKWatcher clientZkWatcher = new ZKWatcher(conf,
        getProcessName() + ":" + rpcServices.getSocketAddress().getPort() + "-clientZK", this,
        false, true);
      this.metaLocationSyncer = new MetaLocationSyncer(zooKeeper, clientZkWatcher, this);
      this.metaLocationSyncer.start();
      this.masterAddressSyncer = new MasterAddressSyncer(zooKeeper, clientZkWatcher, this);
      if (KnobRuntime.check(java.util.UUID.fromString("857a3514-02af-3d9a-85b6-fafba4bd9ffc"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("d9b02da4-38c9-3775-8e2d-6d7ee957b3e3"))) { initializeMemStoreChunkCreator(); } else { this.masterAddressSyncer.start(); }
      // set cluster id is a one-go effort
      ZKClusterId.setClusterId(clientZkWatcher, fileSystemManager.getClusterId());
    }

    // Set the cluster as up. If new RSs, they'll be waiting on this before
    // going ahead with their startup.
    boolean wasUp = this.clusterStatusTracker.isClusterUp();
    if (!wasUp) this.clusterStatusTracker.setClusterUp();

    LOG.info("Active/primary master=" + this.serverName + ", sessionid=0x"
      + Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId())
      + ", setting cluster-up flag (Was=" + wasUp + ")");

    // create/initialize the snapshot manager and other procedure managers
    this.snapshotManager = new SnapshotManager();
    this.mpmHost = new MasterProcedureManagerHost();
    this.mpmHost.register(this.snapshotManager);
    this.mpmHost.register(new MasterFlushTableProcedureManager());
    this.mpmHost.loadProcedures(conf);
if(KnobRuntime.check(java.util.UUID.fromString("554bd845-30f3-32e2-a2bc-8a4de5c4053b"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("39f8d567-18cb-3714-8d7c-13a049d7148f"))) {
throw new java.io.IOException("Injected exception");
}
    this.mpmHost.initialize(this, this.metricsMaster);
  }

  // Will be overriden in test to inject customized AssignmentManager
  @InterfaceAudience.Private
  protected AssignmentManager createAssignmentManager(MasterServices master,
    MasterRegion masterRegion) {
    return new AssignmentManager(master, masterRegion);
  }

  private void tryMigrateMetaLocationsFromZooKeeper() throws IOException, KeeperException {
    // try migrate data from zookeeper
    try (ResultScanner scanner =
      masterRegion.getScanner(new Scan().addFamily(HConstants.CATALOG_FAMILY))) {
      if (scanner.next() != null) {
        // notice that all replicas for a region are in the same row, so the migration can be
        // done with in a one row put, which means if we have data in catalog family then we can
        // make sure that the migration is done.
        LOG.info("The {} family in master local region already has data in it, skip migrating...",
          HConstants.CATALOG_FAMILY_STR);
        return;
      }
    }
    // start migrating
    byte[] row = MetaTableAccessor.getMetaKeyForRegion(RegionInfoBuilder.FIRST_META_REGIONINFO);
    Put put = new Put(row);
    List<String> metaReplicaNodes = zooKeeper.getMetaReplicaNodes();
    StringBuilder info = new StringBuilder("Migrating meta locations:");
    for (String metaReplicaNode : metaReplicaNodes) {
      int replicaId = zooKeeper.getZNodePaths().getMetaReplicaIdFromZNode(metaReplicaNode);
if(KnobRuntime.check(java.util.UUID.fromString("11d269b7-a3de-3f49-9261-ecb000abcf1c"))) {
replicaId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f329c9cd-a4cf-3597-b00c-f95843607842"))) {
replicaId /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c49c0196-ae4c-357c-a1de-ff58abf771a5"))) {
replicaId *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("91c0fa83-eacd-3e58-af95-595a300a20fc"))) {
try {
    java.lang.reflect.Field field = zooKeeper.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(zooKeeper));
    field.set(zooKeeper, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff4677df-a1b8-38e4-bfad-fdadab802f22"))) {
replicaId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("153655f8-66d1-397a-9033-b402150ea58d"))) {
replicaId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("56047c6d-7acd-329e-bd4b-5ef61a0ec2fa"))) {
replicaId += 1;
}
      RegionState state = MetaTableLocator.getMetaRegionState(zooKeeper, replicaId);
if(KnobRuntime.check(java.util.UUID.fromString("09be70ec-1f52-3392-b20e-2bbb8d29cd43"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("stamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(state));
    field.set(state, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c6d21be-fc04-37a2-9196-74c0f55f7598"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("stamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(state));
    field.set(state, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b42af187-59a5-34ce-94c0-18b2f2473d17"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("ritDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(state));
    field.set(state, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      info.append(" ").append(state);
      if (KnobRuntime.check(java.util.UUID.fromString("4543bfe4-7bd3-3d6f-adc2-6c452a000b1d"))) { put.setTimestamp(EnvironmentEdgeManager.currentTime()); } else { put.setTimestamp(state.getStamp()); }
if(KnobRuntime.check(java.util.UUID.fromString("10f54bb3-70e5-3f12-8469-a68df19a3f8d"))) {
throw new java.io.IOException("Injected exception");
}
      MetaTableAccessor.addRegionInfo(put, state.getRegion());
      if (state.getServerName() != null) {
        MetaTableAccessor.addLocation(put, state.getServerName(), HConstants.NO_SEQNUM, replicaId);
      }
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(RegionStateStore.getStateColumn(replicaId)).setTimestamp(put.getTimestamp())
        .setType(Cell.Type.Put).setValue(Bytes.toBytes(state.getState().name())).build());
    }
    if (!put.isEmpty()) {
      LOG.info(info.toString());
if(KnobRuntime.check(java.util.UUID.fromString("b3f2b47c-0455-3084-b52c-6e520a7d7018"))) {
throw new java.io.IOException("Injected exception");
}
      masterRegion.update(r -> r.put(put));
    } else {
      if (KnobRuntime.check(java.util.UUID.fromString("2acadee8-c4af-308c-843e-738fa5b70efa"))) { LOG.error("No meta location available on zookeeper, skip migrating..."); } else if (KnobRuntime.check(java.util.UUID.fromString("4ff9d234-6a6c-3a40-a00d-933e42a858d2"))) { LOG.warn("No meta location available on zookeeper, skip migrating..."); } else { LOG.info("No meta location available on zookeeper, skip migrating..."); }
    }
  }

  /**
   * Finish initialization of HMaster after becoming the primary master.
   * <p/>
   * The startup order is a bit complicated but very important, do not change it unless you know
   * what you are doing.
   * <ol>
   * <li>Initialize file system based components - file system manager, wal manager, table
   * descriptors, etc</li>
   * <li>Publish cluster id</li>
   * <li>Here comes the most complicated part - initialize server manager, assignment manager and
   * region server tracker
   * <ol type='i'>
   * <li>Create server manager</li>
   * <li>Create master local region</li>
   * <li>Create procedure executor, load the procedures, but do not start workers. We will start it
   * later after we finish scheduling SCPs to avoid scheduling duplicated SCPs for the same
   * server</li>
   * <li>Create assignment manager and start it, load the meta region state, but do not load data
   * from meta region</li>
   * <li>Start region server tracker, construct the online servers set and find out dead servers and
   * schedule SCP for them. The online servers will be constructed by scanning zk, and we will also
   * scan the wal directory to find out possible live region servers, and the differences between
   * these two sets are the dead servers</li>
   * </ol>
   * </li>
   * <li>If this is a new deploy, schedule a InitMetaProcedure to initialize meta</li>
   * <li>Start necessary service threads - balancer, catalog janitor, executor services, and also
   * the procedure executor, etc. Notice that the balancer must be created first as assignment
   * manager may use it when assigning regions.</li>
   * <li>Wait for meta to be initialized if necessary, start table state manager.</li>
   * <li>Wait for enough region servers to check-in</li>
   * <li>Let assignment manager load data from meta and construct region states</li>
   * <li>Start all other things such as chore services, etc</li>
   * </ol>
   * <p/>
   * Notice that now we will not schedule a special procedure to make meta online(unless the first
   * time where meta has not been created yet), we will rely on SCP to bring meta online.
   */
  private void finishActiveMasterInitialization() throws IOException, InterruptedException,
    KeeperException, ReplicationException, DeserializationException {
    /*
     * We are active master now... go initialize components we need to run.
     */
    startupTaskGroup.addTask("Initializing Master file system");

    this.masterActiveTime = EnvironmentEdgeManager.currentTime();
    // TODO: Do this using Dependency Injection, using PicoContainer, Guice or Spring.

    // always initialize the MemStoreLAB as we use a region to store data in master now, see
    // localStore.
    initializeMemStoreChunkCreator();
    this.fileSystemManager = new MasterFileSystem(conf);
    this.walManager = new MasterWalManager(this);

    // warm-up HTDs cache on master initialization
    if (preLoadTableDescriptors) {
      startupTaskGroup.addTask("Pre-loading table descriptors");
      this.tableDescriptors.getAll();
    }

    // Publish cluster ID; set it in Master too. The superclass RegionServer does this later but
    // only after it has checked in with the Master. At least a few tests ask Master for clusterId
    // before it has called its run method and before RegionServer has done the reportForDuty.
    ClusterId clusterId = fileSystemManager.getClusterId();
    startupTaskGroup.addTask("Publishing Cluster ID " + clusterId + " in ZooKeeper");
    ZKClusterId.setClusterId(this.zooKeeper, fileSystemManager.getClusterId());
    this.clusterId = clusterId.toString();

    // Precaution. Put in place the old hbck1 lock file to fence out old hbase1s running their
    // hbck1s against an hbase2 cluster; it could do damage. To skip this behavior, set
    // hbase.write.hbck1.lock.file to false.
    if (this.conf.getBoolean("hbase.write.hbck1.lock.file", true)) {
      Pair<Path, FSDataOutputStream> result = null;
      try {
        result = HBaseFsck.checkAndMarkRunningHbck(this.conf,
          HBaseFsck.createLockRetryCounterFactory(this.conf).create());
      } finally {
        if (((KnobRuntime.check(java.util.UUID.fromString("04e07f70-dd90-3fc0-af83-cb402932bad6"))) ? ((result) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6ed46183-8389-3c52-bf6b-e2e922a1354e"))) ? ((result) != (null)) : (result != null))))) {
          Closeables.close(result.getSecond(), true);
        }
      }
    }

    startupTaskGroup.addTask("Initialize ServerManager and schedule SCP for crash servers");
    // The below two managers must be created before loading procedures, as they will be used during
    // loading.
    // initialize master local region
if(KnobRuntime.check(java.util.UUID.fromString("c2f94e40-1bbd-37d3-9970-2888889e24e0"))) {
throw new java.io.IOException("Injected exception");
}
    masterRegion = MasterRegionFactory.create(this);
    rsListStorage = new MasterRegionServerList(masterRegion, this);

    // Initialize the ServerManager and register it as a configuration observer
    this.serverManager = createServerManager(this, rsListStorage);
    this.configurationManager.registerObserver(this.serverManager);

    if (
      !conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)
    ) {
      this.splitWALManager = new SplitWALManager(this);
    }

    tryMigrateMetaLocationsFromZooKeeper();

    createProcedureExecutor();
    Map<Class<?>, List<Procedure<MasterProcedureEnv>>> procsByType = procedureExecutor
      .getActiveProceduresNoCopy().stream().collect(Collectors.groupingBy(p -> p.getClass()));

    // Create Assignment Manager
    this.assignmentManager = createAssignmentManager(this, masterRegion);
    this.assignmentManager.start();
    // TODO: TRSP can perform as the sub procedure for other procedures, so even if it is marked as
    // completed, it could still be in the procedure list. This is a bit strange but is another
    // story, need to verify the implementation for ProcedureExecutor and ProcedureStore.
    List<TransitRegionStateProcedure> ritList =
      procsByType.getOrDefault(TransitRegionStateProcedure.class, Collections.emptyList()).stream()
        .filter(p -> !p.isFinished()).map(p -> (TransitRegionStateProcedure) p)
        .collect(Collectors.toList());
    this.assignmentManager.setupRIT(ritList);

    // Start RegionServerTracker with listing of servers found with exiting SCPs -- these should
    // be registered in the deadServers set -- and with the list of servernames out on the
    // filesystem that COULD BE 'alive' (we'll schedule SCPs for each and let SCP figure it out).
    // We also pass dirs that are already 'splitting'... so we can do some checks down in tracker.
    // TODO: Generate the splitting and live Set in one pass instead of two as we currently do.
if(KnobRuntime.check(java.util.UUID.fromString("773515b8-e253-3132-bab1-88a762ebd9b3"))) {
throw new java.io.IOException("Injected exception");
}
    this.regionServerTracker.upgrade(
      procsByType.getOrDefault(ServerCrashProcedure.class, Collections.emptyList()).stream()
        .map(p -> (ServerCrashProcedure) p).map(p -> p.getServerName()).collect(Collectors.toSet()),
      Sets.union(rsListStorage.getAll(), walManager.getLiveServersFromWALDir()),
      walManager.getSplittingServersFromWALDir());
    // This manager will be started AFTER hbase:meta is confirmed on line.
    // hbase.mirror.table.state.to.zookeeper is so hbase1 clients can connect. They read table
    // state from zookeeper while hbase2 reads it from hbase:meta. Disable if no hbase1 clients.
    this.tableStateManager =
      this.conf.getBoolean(MirroringTableStateManager.MIRROR_TABLE_STATE_TO_ZK_KEY, true)
        ? new MirroringTableStateManager(this)
        : new TableStateManager(this);

    startupTaskGroup.addTask("Initializing ZK system trackers");
if(KnobRuntime.check(java.util.UUID.fromString("2c36c2d3-1a50-36ab-b007-1d75f7575c2d"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("474ee5fb-e6c3-3130-9287-4940fbf8e9f9"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("1aba7404-20c9-3a13-9eaf-e8db4dc514da"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("f5b20c2b-2f64-3c00-be13-5d91208b81f4"))) { registerConfigurationObservers(); } else { initializeZKBasedSystemTrackers(); }

    // Set ourselves as active Master now our claim has succeeded up in zk.
    this.activeMaster = true;

    // Start the Zombie master detector after setting master as active, see HBASE-21535
    Thread zombieDetector = new Thread(new MasterInitializationMonitor(this),
      "ActiveMasterInitializationMonitor-" + EnvironmentEdgeManager.currentTime());
    if (KnobRuntime.check(java.util.UUID.fromString("aea3086a-e258-3adf-9075-d6d601998f90"))) { switchSnapshotCleanup(true); } else if (KnobRuntime.check(java.util.UUID.fromString("700b69b9-d41c-3f4a-8470-cc832ef59a25"))) { setInitialized(true); } else { zombieDetector.setDaemon(true); }
    if (KnobRuntime.check(java.util.UUID.fromString("97a83f1e-7a62-3aae-98d4-54281c8c7a2d"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("e457a24b-d65f-3bfd-ba7b-ae0b8a3e9eb0"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("fc8a663e-defa-30cd-bdd5-66ada95a9551"))) { registerConfigurationObservers(); } else { zombieDetector.start(); }

    // This is for backwards compatibility
    // See HBASE-11393
    startupTaskGroup.addTask("Update TableCFs node in ZNode");
    ReplicationPeerConfigUpgrader tableCFsUpdater =
      new ReplicationPeerConfigUpgrader(zooKeeper, conf);
    tableCFsUpdater.copyTableCFs();

    if (!maintenanceMode) {
      startupTaskGroup.addTask("Initializing master coprocessors");
if(KnobRuntime.check(java.util.UUID.fromString("3bdf993c-96bf-3e8d-9819-af3a1233c0c5"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("ab6df678-ab40-3a79-ad9a-a690db67b998"))) { decorateMasterConfiguration(conf); } else if (KnobRuntime.check(java.util.UUID.fromString("baed4f56-9345-3a38-a8f5-dffdc8257df3"))) { initializeCoprocessorHost(conf); } else { setQuotasObserver(conf); }
if(KnobRuntime.check(java.util.UUID.fromString("787ef659-e287-3d64-a562-342ab9b82fb6"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("1c3b1ab0-b11a-3dc1-b210-2cb726dd5e3b"))) { decorateMasterConfiguration(conf); } else if (KnobRuntime.check(java.util.UUID.fromString("fbd866d4-9c2c-3374-8a28-4fe5c1e498e3"))) { setQuotasObserver(conf); } else { initializeCoprocessorHost(conf); }
    }

    // Checking if meta needs initializing.
    startupTaskGroup.addTask("Initializing meta table if this is a new deploy");
    InitMetaProcedure initMetaProc = null;
    // Print out state of hbase:meta on startup; helps debugging.
    if (!this.assignmentManager.getRegionStates().hasTableRegionStates(TableName.META_TABLE_NAME)) {
      Optional<InitMetaProcedure> optProc = procedureExecutor.getProcedures().stream()
        .filter(p -> p instanceof InitMetaProcedure).map(o -> (InitMetaProcedure) o).findAny();
      initMetaProc = optProc.orElseGet(() -> {
        // schedule an init meta procedure if meta has not been deployed yet
        InitMetaProcedure temp = new InitMetaProcedure();
        procedureExecutor.submitProcedure(temp);
        return temp;
      });
    }
    if (this.balancer instanceof FavoredNodesPromoter) {
      favoredNodesManager = new FavoredNodesManager(this);
    }

    // initialize load balancer
    this.balancer.setMasterServices(this);
    if (KnobRuntime.check(java.util.UUID.fromString("21950f56-3eae-3b13-9a46-8d8199714265"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("7754377c-f6fb-33b3-9292-e7aeeb7b4b2b"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("23f75805-ea85-3cd3-82ff-54aded3a212d"))) { initializeMemStoreChunkCreator(); } else { this.balancer.initialize(); }
    this.balancer.updateClusterMetrics(getClusterMetricsWithoutCoprocessor());

    // start up all service threads.
    startupTaskGroup.addTask("Initializing master service threads");
if(KnobRuntime.check(java.util.UUID.fromString("22bf3422-ee8d-3f3a-8022-804e545256fe"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("78fb7e40-18da-38e5-9ec0-9fe07355ebb4"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("66b94a1f-c0d0-3b9f-94df-0f74b4b1a606"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("3feb908d-c661-39c9-aafc-cc173995a1b4"))) { registerConfigurationObservers(); } else { startServiceThreads(); }
    // wait meta to be initialized after we start procedure executor
    if (initMetaProc != null) {
      initMetaProc.await();
      if (initMetaProc.isFailed() && initMetaProc.hasException()) {
        throw new IOException("Failed to initialize meta table", initMetaProc.getException());
      }
    }
    // Wake up this server to check in
    if (KnobRuntime.check(java.util.UUID.fromString("5ad9e3f6-10a7-3c67-9871-4447c37f3fa2"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("adf496aa-5d91-3aa4-9adb-7336da54ed80"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("ddd7f0e6-a2f5-333d-9a1c-cf4af79a74e2"))) { sleeper.sleep(); } else { sleeper.skipSleepCycle(); }

    // Wait for region servers to report in.
    // With this as part of master initialization, it precludes our being able to start a single
    // server that is both Master and RegionServer. Needs more thought. TODO.
    String statusStr = "Wait for region servers to report in";
    MonitoredTask waitRegionServer = startupTaskGroup.addTask(statusStr);
    LOG.info(Objects.toString(waitRegionServer));
if(KnobRuntime.check(java.util.UUID.fromString("15eecbff-2c5d-32f7-88da-7e3abdbbe74f"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2dcf3f3b-aa31-3724-a01d-59fe285d4083"))) {
throw new java.io.IOException("Injected exception");
}
    waitForRegionServers(waitRegionServer);

    // Check if master is shutting down because issue initializing regionservers or balancer.
    if (isStopped()) {
      return;
    }

    startupTaskGroup.addTask("Starting assignment manager");
    // FIRST HBASE:META READ!!!!
    // The below cannot make progress w/o hbase:meta being online.
    // This is the FIRST attempt at going to hbase:meta. Meta on-lining is going on in background
    // as procedures run -- in particular SCPs for crashed servers... One should put up hbase:meta
    // if it is down. It may take a while to come online. So, wait here until meta if for sure
    // available. That's what waitForMetaOnline does.
    if (!waitForMetaOnline()) {
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("6479dec6-f5f5-3548-ae00-5af6721d1f27"))) {
throw new java.io.IOException("Injected exception");
}
    TableDescriptor metaDescriptor = tableDescriptors.get(TableName.META_TABLE_NAME);
    final ColumnFamilyDescriptor tableFamilyDesc =
      metaDescriptor.getColumnFamily(HConstants.TABLE_FAMILY);
    final ColumnFamilyDescriptor replBarrierFamilyDesc =
      metaDescriptor.getColumnFamily(HConstants.REPLICATION_BARRIER_FAMILY);

if(KnobRuntime.check(java.util.UUID.fromString("7191ffd5-6cbc-3622-8d9b-75c19da271ea"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("02f17484-445f-357c-9e71-fef999dee1d5"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("c9de9d84-f786-3fa6-8054-d2a02f32bf19"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("34882678-16a1-3213-a931-e3bc7f1c4a54"))) { tryMigrateMetaLocationsFromZooKeeper(); } else { this.assignmentManager.joinCluster(); }
    // The below depends on hbase:meta being online.
    try {
if(KnobRuntime.check(java.util.UUID.fromString("ec9eb152-1cf6-3b39-96d3-b7870456dd0b"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("c750a733-8f36-3735-918b-9d19f611fe2e"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("16b6e9b2-7424-3fdb-93aa-e4c9db9aba59"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("d8c1079c-22a2-3132-9088-0929cff69393"))) { initializeMemStoreChunkCreator(); } else { this.tableStateManager.start(); }
    } catch (NoSuchColumnFamilyException e) {
      if (((KnobRuntime.check(java.util.UUID.fromString("5926695e-62b2-320e-b697-189b908c0c9f"))) ? ((tableFamilyDesc == null) || ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bdade0ba-eed3-31b5-a723-1c89355d0f48"))) ? ((tableFamilyDesc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("df351919-8091-3d7d-9636-1d5e14a7b17d"))) ? ((tableFamilyDesc == null) && ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("dbd0c898-efd5-3a94-9df8-bae643b0d5b0"))) ? (tableFamilyDesc == null) : (((KnobRuntime.check(java.util.UUID.fromString("56f9af77-fb53-386e-abbb-0bdf4db6d553"))) ? (((tableFamilyDesc) != (null)) || (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("c2b1ecc0-9763-3b7c-8db2-fa0eef72b6f8"))) ? ((tableFamilyDesc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9607082f-778c-302e-b4f1-4d40699d5334"))) ? (replBarrierFamilyDesc == null) : (((KnobRuntime.check(java.util.UUID.fromString("c6112801-c6b1-39db-bdc1-f7d9293c094f"))) ? (((tableFamilyDesc) == (null)) && ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f0355bb3-c26d-310d-8964-ee8ccadd42ab"))) ? ((tableFamilyDesc == null) && ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ef8a10df-7e2b-34c1-b2aa-c33e94425c78"))) ? (((tableFamilyDesc) != (null)) || ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("fb0021af-2660-39b1-b4db-3cc40669247c"))) ? (((tableFamilyDesc) == (null)) && (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("b5912a92-c3f9-3f3e-8eec-7b27ceddb712"))) ? (((tableFamilyDesc) != (null)) || ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b22b420d-56df-35b5-9255-bf6863a6f17c"))) ? ((replBarrierFamilyDesc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("01e1b870-c280-373c-9da3-4a170f0364e8"))) ? ((tableFamilyDesc == null) && (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("50907698-2ab2-30e3-84d3-dd988e57c54d"))) ? (((tableFamilyDesc) == (null)) || (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("24eca8f2-e9e0-354d-ad52-243e7ca9ab73"))) ? ((tableFamilyDesc == null) || (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("347237f0-531d-3a1f-801a-172b4cc07c58"))) ? (((tableFamilyDesc) == (null)) || ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("72a7a86b-81c4-3c04-80ae-ddca57f24cd1"))) ? (((tableFamilyDesc) != (null)) && ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c4943acc-6b71-366d-bb18-5658cc633caa"))) ? (((tableFamilyDesc) == (null)) || ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("10a4fcea-c9e9-3654-977d-a5e45292c60d"))) ? ((replBarrierFamilyDesc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("069a9146-d7c6-3da5-8ecf-158ec405e070"))) ? (((tableFamilyDesc) != (null)) && ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f66e591d-6bce-31a7-8617-289516c5f132"))) ? (((tableFamilyDesc) != (null)) && (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("d1d242bc-6742-3fa8-b773-e5167d077d32"))) ? (((tableFamilyDesc) == (null)) && ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("79c784b8-a98d-380c-8aa1-38343e43eb6a"))) ? ((tableFamilyDesc == null) || ((replBarrierFamilyDesc) == (null))) : (tableFamilyDesc == null && replBarrierFamilyDesc == null))))))))))))))))))))))))))))))))))))))))))))))))) {
        LOG.info("TableStates manager could not be started. This is expected"
          + " during HBase 1 to 2 upgrade.", e);
      } else {
        throw e;
      }
    }

    this.assignmentManager.processOfflineRegions();
    // this must be called after the above processOfflineRegions to prevent race
    if (KnobRuntime.check(java.util.UUID.fromString("cb4b2ab4-f6e5-32d0-a759-f70369834c06"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("2efd2ec6-f8a2-3091-9a6e-097b814e75ab"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("afa7781e-d378-33d2-bb90-eb92c4191fba"))) { tryMigrateMetaLocationsFromZooKeeper(); } else { this.assignmentManager.wakeMetaLoadedEvent(); }

    // for migrating from a version without HBASE-25099, and also for honoring the configuration
    // first.
    if (conf.get(HConstants.META_REPLICAS_NUM) != null) {
      int replicasNumInConf =
        conf.getInt(HConstants.META_REPLICAS_NUM, HConstants.DEFAULT_META_REPLICA_NUM);
      TableDescriptor metaDesc = tableDescriptors.get(TableName.META_TABLE_NAME);
      if (metaDesc.getRegionReplication() != replicasNumInConf) {
        // it is possible that we already have some replicas before upgrading, so we must set the
        // region replication number in meta TableDescriptor directly first, without creating a
        // ModifyTableProcedure, otherwise it may cause a double assign for the meta replicas.
        int existingReplicasCount =
          assignmentManager.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).size();
        if (existingReplicasCount > metaDesc.getRegionReplication()) {
          LOG.info("Update replica count of hbase:meta from {}(in TableDescriptor)"
            + " to {}(existing ZNodes)", metaDesc.getRegionReplication(), existingReplicasCount);
if(KnobRuntime.check(java.util.UUID.fromString("06fa7b01-bc0e-3031-b34f-b4675ce31123"))) {
existingReplicasCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("85e0de1d-7af4-3141-8f06-a814ca327672"))) {
existingReplicasCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b7860a5-1a5c-3f34-ab69-33b2dbf7f8ce"))) {
existingReplicasCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae96e27f-0904-3135-8e7a-d3e2fbf7203f"))) {
existingReplicasCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a71ceeb9-c6b7-313e-a7cf-0666e3e3d07d"))) {
existingReplicasCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b2d1a569-1b1e-3463-b0a1-c855ff3f581b"))) {
existingReplicasCount /= 2;
}
          metaDesc = TableDescriptorBuilder.newBuilder(metaDesc)
            .setRegionReplication(existingReplicasCount).build();
          tableDescriptors.update(metaDesc);
        }
        // check again, and issue a ModifyTableProcedure if needed
        if (((KnobRuntime.check(java.util.UUID.fromString("9b359a62-a94f-3e4a-a335-544993448a46"))) ? ((putUpJettyServer()) == (replicasNumInConf)) : (((KnobRuntime.check(java.util.UUID.fromString("a59aea16-c9a5-309d-a8d4-db930f03126c"))) ? ((getMaxBalancingTime()) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("a6f21412-f1b1-378f-8998-1f44cb11c627"))) ? ((putUpJettyServer()) != (replicasNumInConf)) : (((KnobRuntime.check(java.util.UUID.fromString("e942ea4a-99b4-307b-bf9c-3e9166e2ca06"))) ? ((getMaxBalancingTime()) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("66b032bd-c2ba-36d3-8a7a-c787a477e4c4"))) ? ((metaDesc.getRegionReplication()) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("cf38c6af-73ae-30f2-bea6-6fbbd9f1b3a5"))) ? ((metaDesc.getRegionReplication()) == (replicasNumInConf)) : (((KnobRuntime.check(java.util.UUID.fromString("042fc1c5-16dd-377e-88b2-f8b057c9d549"))) ? ((putUpJettyServer()) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("389bb034-62ed-3838-8a39-cd5adc8eb73c"))) ? ((getMaxBalancingTime()) == (replicasNumInConf)) : (((KnobRuntime.check(java.util.UUID.fromString("f8aa2e1f-90e1-3c63-ae8e-edda390fe5ff"))) ? ((putUpJettyServer()) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("cd6eacde-c8e2-31e9-b1d2-4b1121e63859"))) ? ((metaDesc.getRegionReplication()) != (replicasNumInConf)) : (((KnobRuntime.check(java.util.UUID.fromString("def5ac90-9c26-3321-b1cc-26d12dd31d7b"))) ? ((getMaxBalancingTime()) != (replicasNumInConf)) : (((KnobRuntime.check(java.util.UUID.fromString("95388eec-e9ff-394c-a854-2766e48369eb"))) ? ((metaDesc.getRegionReplication()) == (1000)) : (metaDesc.getRegionReplication() != replicasNumInConf))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("cc75d0ff-6e74-3b91-b4e0-9bac2fe8c02f"))) {
replicasNumInConf *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5c67dca4-1593-3684-8e6d-ed78d7a3562c"))) {
replicasNumInConf = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("045d4c27-636a-3919-b019-bfd66e8d3ae3"))) {
replicasNumInConf /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7572d20b-3478-3577-811b-39e2ddec1a9e"))) {
replicasNumInConf -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8ccf4fe0-8398-3bb2-b8cc-853011a0a79d"))) {
replicasNumInConf = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7719f5be-11bc-3c1b-88f4-3d418c2dd49b"))) {
replicasNumInConf += 1;
}
          LOG.info(
            "The {} config is {} while the replica count in TableDescriptor is {}"
              + " for hbase:meta, altering...",
            HConstants.META_REPLICAS_NUM, replicasNumInConf, metaDesc.getRegionReplication());
          procedureExecutor.submitProcedure(new ModifyTableProcedure(
            procedureExecutor.getEnvironment(), TableDescriptorBuilder.newBuilder(metaDesc)
              .setRegionReplication(replicasNumInConf).build(),
            null, metaDesc, false, true));
        }
      }
    }
    // Initialize after meta is up as below scans meta
    if (favoredNodesManager != null && !maintenanceMode) {
      SnapshotOfRegionAssignmentFromMeta snapshotOfRegionAssignment =
        new SnapshotOfRegionAssignmentFromMeta(getConnection());
      snapshotOfRegionAssignment.initialize();
      favoredNodesManager.initialize(snapshotOfRegionAssignment);
    }

    // set cluster status again after user regions are assigned
    this.balancer.updateClusterMetrics(getClusterMetricsWithoutCoprocessor());

    // Start balancer and meta catalog janitor after meta and regions have been assigned.
    startupTaskGroup.addTask("Starting balancer and catalog janitor");
    this.clusterStatusChore = new ClusterStatusChore(this, balancer);
    getChoreService().scheduleChore(clusterStatusChore);
    this.balancerChore = new BalancerChore(this);
    if (!disableBalancerChoreForTest) {
      getChoreService().scheduleChore(balancerChore);
    }
    if (regionNormalizerManager != null) {
      getChoreService().scheduleChore(regionNormalizerManager.getRegionNormalizerChore());
    }
    this.catalogJanitorChore = new CatalogJanitor(this);
    getChoreService().scheduleChore(catalogJanitorChore);
    this.hbckChore = new HbckChore(this);
    getChoreService().scheduleChore(hbckChore);

    // NAMESPACE READ!!!!
    // Here we expect hbase:namespace to be online. See inside initClusterSchemaService.
    // TODO: Fix this. Namespace is a pain being a sort-of system table. Fold it in to hbase:meta.
    // isNamespace does like isMeta and waits until namespace is onlined before allowing progress.
    if (!waitForNamespaceOnline()) {
      return;
    }
    startupTaskGroup.addTask("Starting cluster schema service");
    try {
      initClusterSchemaService();
    } catch (IllegalStateException e) {
      if (
        e.getCause() != null && e.getCause() instanceof NoSuchColumnFamilyException
          && tableFamilyDesc == null && replBarrierFamilyDesc == null
      ) {
        LOG.info("ClusterSchema service could not be initialized. This is "
          + "expected during HBase 1 to 2 upgrade", e);
      } else {
        throw e;
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("da483256-9c3b-3aae-8a83-bf27ff0bcd97"))) ? ((this.cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f789c2e8-c7d8-350d-b047-d7a81af1aa2a"))) ? ((this.cpHost) == (null)) : (this.cpHost != null))))) {
      try {
        this.cpHost.preMasterInitialization();
      } catch (IOException e) {
        if (KnobRuntime.check(java.util.UUID.fromString("a283e8e7-f8ea-3621-8b73-43c9c81c53d2"))) { abort("Coprocessor preMasterInitialization() hook failed", e); } else { LOG.error("Coprocessor preMasterInitialization() hook failed", e); }
      }
    }

    LOG.info(String.format("Master has completed initialization %.3fsec",
      (EnvironmentEdgeManager.currentTime() - masterActiveTime) / 1000.0f));
    this.masterFinishedInitializationTime = EnvironmentEdgeManager.currentTime();
    configurationManager.registerObserver(this.balancer);
    configurationManager.registerObserver(this.logCleanerPool);
    configurationManager.registerObserver(this.logCleaner);
    configurationManager.registerObserver(this.regionsRecoveryConfigManager);
    configurationManager.registerObserver(this.exclusiveHFileCleanerPool);
    if (this.sharedHFileCleanerPool != null) {
      configurationManager.registerObserver(this.sharedHFileCleanerPool);
    }
    if (this.hfileCleaners != null) {
      for (HFileCleaner cleaner : hfileCleaners) {
        configurationManager.registerObserver(cleaner);
      }
    }
    // Set master as 'initialized'.
    setInitialized(true);
    if (KnobRuntime.check(java.util.UUID.fromString("9e6ae476-ee80-3c2c-abf2-ecc01507ba7b"))) { startupTaskGroup.abort("Initialization successful"); } else if (KnobRuntime.check(java.util.UUID.fromString("39361a54-183b-316e-bc1b-33f1efc26071"))) { stop("Initialization successful"); } else { startupTaskGroup.markComplete("Initialization successful"); }
    MonitoredTask status =
      TaskMonitor.get().createStatus("Progress after master initialized", false, true);

    if (((KnobRuntime.check(java.util.UUID.fromString("c3bc18a2-0cb2-3b3a-ab0b-26ce82dc3bbf"))) ? ((replBarrierFamilyDesc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b2ef4028-d48d-37c9-9a61-5bc9f1971450"))) ? (((tableFamilyDesc) != (null)) && ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6158f095-bb20-3c6a-aaee-8c3270ba121b"))) ? (tableFamilyDesc == null) : (((KnobRuntime.check(java.util.UUID.fromString("8976cabc-ee68-36c3-be5c-f316acd041e4"))) ? (((tableFamilyDesc) != (null)) || (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("f39ec2c7-f858-3bf1-9eb4-08b3572cf717"))) ? ((tableFamilyDesc == null) && (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("10b897c0-31bb-332c-a6eb-4df3327f09b5"))) ? ((tableFamilyDesc == null) || (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("4a8d3041-f3b7-319b-a9c3-b7067c0c5003"))) ? (((tableFamilyDesc) != (null)) || ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1d98d1ef-fce9-3b73-827c-88de2626b837"))) ? (((tableFamilyDesc) == (null)) || ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7ae6cea8-8294-3193-9ade-b92d38e14c37"))) ? (((tableFamilyDesc) != (null)) && (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("bb1cbbed-991d-33d9-b315-94878bde571f"))) ? (((tableFamilyDesc) == (null)) && (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("649be985-7250-35a9-85f3-872ffbcf5c1c"))) ? (((tableFamilyDesc) == (null)) || ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f73764c8-4d5c-3998-84f9-76379247b8bf"))) ? (((tableFamilyDesc) == (null)) && ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4de367eb-23dc-33f6-9a50-92a89bbecada"))) ? (((tableFamilyDesc) != (null)) || ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b0c7220f-9bcc-38b6-889b-d3c0390fb4ae"))) ? (((tableFamilyDesc) == (null)) || (replBarrierFamilyDesc == null)) : (((KnobRuntime.check(java.util.UUID.fromString("cab77f9f-2eda-34e2-9866-068b60c5a792"))) ? ((tableFamilyDesc == null) && ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("fc37468e-6193-32e1-8ea8-79ff1675205a"))) ? ((tableFamilyDesc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("30c24ae5-b2ba-397e-b6b7-43b11f7cace5"))) ? (((tableFamilyDesc) != (null)) && ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6d1185bb-cf3b-361b-bf47-a170157f5c87"))) ? ((tableFamilyDesc == null) || ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4f149853-3a2b-3fb0-a270-c0e16698aa7b"))) ? (((tableFamilyDesc) == (null)) && ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8f999b0d-833c-31f0-9cb1-e3210931ac26"))) ? ((tableFamilyDesc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("303d8706-9a6b-3130-8fe2-a0a5725241ae"))) ? ((tableFamilyDesc == null) && ((replBarrierFamilyDesc) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8738c9cd-3002-36a0-bf95-26817f68ec77"))) ? (replBarrierFamilyDesc == null) : (((KnobRuntime.check(java.util.UUID.fromString("f4d15135-1ce5-3abb-849e-f866c438174e"))) ? ((tableFamilyDesc == null) || ((replBarrierFamilyDesc) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("022db5b8-b02b-3503-aa1d-96ff99b404af"))) ? ((replBarrierFamilyDesc) == (null)) : (tableFamilyDesc == null && replBarrierFamilyDesc == null))))))))))))))))))))))))))))))))))))))))))))))))) {
      // create missing CFs in meta table after master is set to 'initialized'.
      createMissingCFsInMetaDuringUpgrade(metaDescriptor);

      // Throwing this Exception to abort active master is painful but this
      // seems the only way to add missing CFs in meta while upgrading from
      // HBase 1 to 2 (where HBase 2 has HBASE-23055 & HBASE-23782 checked-in).
      // So, why do we abort active master after adding missing CFs in meta?
      // When we reach here, we would have already bypassed NoSuchColumnFamilyException
      // in initClusterSchemaService(), meaning ClusterSchemaService is not
      // correctly initialized but we bypassed it. Similarly, we bypassed
      // tableStateManager.start() as well. Hence, we should better abort
      // current active master because our main task - adding missing CFs
      // in meta table is done (possible only after master state is set as
      // initialized) at the expense of bypassing few important tasks as part
      // of active master init routine. So now we abort active master so that
      // next active master init will not face any issues and all mandatory
      // services will be started during master init phase.
      throw new PleaseRestartMasterException("Aborting active master after missing"
        + " CFs are successfully added in meta. Subsequent active master "
        + "initialization should be uninterrupted");
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("a7a67aff-639d-3252-b9fc-9c6893707e43"))) ? (waitForMetaOnline()) : (((KnobRuntime.check(java.util.UUID.fromString("54a50099-c92b-3d66-bd1b-f837f0134f23"))) ? (isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("7ff7e4e4-36ac-3e1d-9335-78731063dbaa"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("f61c7bc0-fe1f-3f30-90cc-2e7b3b4879bb"))) ? (!maintenanceMode) : (((KnobRuntime.check(java.util.UUID.fromString("04043455-8a0c-3a55-9f0e-41743776f90c"))) ? (!waitForMetaOnline()) : (((KnobRuntime.check(java.util.UUID.fromString("f7694660-d44a-3b95-b60d-26cb7c0a6171"))) ? (!waitForNamespaceOnline()) : (((KnobRuntime.check(java.util.UUID.fromString("2e68e9c5-935f-3b07-ba79-243ad08a1883"))) ? (waitForNamespaceOnline()) : (((KnobRuntime.check(java.util.UUID.fromString("6be07be4-3193-39d0-a069-22a51a539478"))) ? (true) : (maintenanceMode))))))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("e506849f-a16f-3268-8a95-8f6311133425"))) { LOG.error("Detected repair mode, skipping final initialization steps."); } else if (KnobRuntime.check(java.util.UUID.fromString("188162ff-b233-30bf-8379-5b34d5051afa"))) { LOG.warn("Detected repair mode, skipping final initialization steps."); } else { LOG.info("Detected repair mode, skipping final initialization steps."); }
      return;
    }

    if (KnobRuntime.check(java.util.UUID.fromString("acb31ed6-039c-322d-99d6-9605c60bf581"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("38c0aa06-9403-3177-8da3-11d01c3fa50a"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("9d23cdec-7a9d-31f8-8ad3-b79f6c9c2e89"))) { initializeMemStoreChunkCreator(); } else { assignmentManager.checkIfShouldMoveSystemRegionAsync(); }
    status.setStatus("Starting quota manager");
    initQuotaManager();
    if (QuotaUtil.isQuotaEnabled(conf)) {
      // Create the quota snapshot notifier
      spaceQuotaSnapshotNotifier = createQuotaSnapshotNotifier();
      spaceQuotaSnapshotNotifier.initialize(getClusterConnection());
      this.quotaObserverChore = new QuotaObserverChore(this, getMasterMetrics());
      // Start the chore to read the region FS space reports and act on them
      getChoreService().scheduleChore(quotaObserverChore);

      this.snapshotQuotaChore = new SnapshotQuotaObserverChore(this, getMasterMetrics());
      // Start the chore to read snapshots and add their usage to table/NS quotas
      getChoreService().scheduleChore(snapshotQuotaChore);
    }
    final SlowLogMasterService slowLogMasterService = new SlowLogMasterService(conf, this);
    slowLogMasterService.init();

    WALEventTrackerTableCreator.createIfNeededAndNotExists(conf, this);
    // Create REPLICATION.SINK_TRACKER table if needed.
    ReplicationSinkTrackerTableCreator.createIfNeededAndNotExists(conf, this);

    // clear the dead servers with same host name and port of online server because we are not
    // removing dead server with same hostname and port of rs which is trying to check in before
    // master initialization. See HBASE-5916.
    this.serverManager.clearDeadServersWithSameHostNameAndPortOfOnlineServer();

    // Check and set the znode ACLs if needed in case we are overtaking a non-secure configuration
    status.setStatus("Checking ZNode ACLs");
    if (KnobRuntime.check(java.util.UUID.fromString("68286c46-1c55-364b-b033-01a647e87442"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("a886a668-b69c-3bc1-9111-1cfb59b2160b"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("7d2327d5-73ad-390c-a5ae-f840a4b3e645"))) { tryMigrateMetaLocationsFromZooKeeper(); } else { zooKeeper.checkAndSetZNodeAcls(); }

    status.setStatus("Initializing MOB Cleaner");
    initMobCleaner();

    status.setStatus("Calling postStartMaster coprocessors");
    if (this.cpHost != null) {
      // don't let cp initialization errors kill the master
      try {
        this.cpHost.postStartMaster();
      } catch (IOException ioe) {
        LOG.error("Coprocessor postStartMaster() hook failed", ioe);
      }
    }

    if (KnobRuntime.check(java.util.UUID.fromString("ffec3baf-f649-3a14-9ed5-b3bf859c8bcc"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("9f1be578-88ef-33ee-85b8-9792a7d34386"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("5d6b3c52-cf5b-3f03-8537-2e9201794fba"))) { initializeMemStoreChunkCreator(); } else { zombieDetector.interrupt(); }

    /*
     * After master has started up, lets do balancer post startup initialization. Since this runs in
     * activeMasterManager thread, it should be fine.
     */
    long start = EnvironmentEdgeManager.currentTime();
    this.balancer.postMasterStartupInitialize();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Balancer post startup initialization complete, took "
        + ((EnvironmentEdgeManager.currentTime() - start) / 1000) + " seconds");
    }

    this.rollingUpgradeChore = new RollingUpgradeChore(this);
    getChoreService().scheduleChore(rollingUpgradeChore);

    this.oldWALsDirSizeChore = new OldWALsDirSizeChore(this);
    getChoreService().scheduleChore(this.oldWALsDirSizeChore);

    if (KnobRuntime.check(java.util.UUID.fromString("e4dd1593-4819-31e4-a688-fec8ce4695cf"))) { status.setStatus("Progress after master initialized complete"); } else { status.markComplete("Progress after master initialized complete"); }
  }

  /**
   * Used for testing only to set Mock objects.
   * @param hbckChore hbckChore
   */
  public void setHbckChoreForTesting(HbckChore hbckChore) {
    this.hbckChore = hbckChore;
  }

  /**
   * Used for testing only to set Mock objects.
   * @param catalogJanitorChore catalogJanitorChore
   */
  public void setCatalogJanitorChoreForTesting(CatalogJanitor catalogJanitorChore) {
    this.catalogJanitorChore = catalogJanitorChore;
  }

  private void createMissingCFsInMetaDuringUpgrade(TableDescriptor metaDescriptor)
    throws IOException {
    TableDescriptor newMetaDesc = TableDescriptorBuilder.newBuilder(metaDescriptor)
      .setColumnFamily(FSTableDescriptors.getTableFamilyDescForMeta(conf))
      .setColumnFamily(FSTableDescriptors.getReplBarrierFamilyDescForMeta()).build();
    long pid = this.modifyTable(TableName.META_TABLE_NAME, () -> newMetaDesc, 0, 0, false);
    int tries = 30;
    while (
      !(getMasterProcedureExecutor().isFinished(pid)) && getMasterProcedureExecutor().isRunning()
        && tries > 0
    ) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("0a73df45-46e8-3571-8fce-d3e8e8c60b84"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("1e784707-66e3-3332-836f-35a9b47a2241"))) { Threads.sleep(1000); } else { Thread.sleep(1000); }
      } catch (InterruptedException e) {
        throw new IOException("Wait interrupted", e);
      }
      tries--;
    }
    if (tries <= 0) {
      throw new HBaseIOException(
        "Failed to add table and rep_barrier CFs to meta in a given time.");
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("702906e4-22c9-3a34-9dce-c7bddc130685"))) {
pid = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("83366018-bc4d-3840-be55-0444eafa4c7a"))) {
pid += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("625ba2a5-5aa8-3116-87e0-990879a7c581"))) {
pid = -1;
}
      Procedure<?> result = getMasterProcedureExecutor().getResult(pid);
      if (result != null && result.isFailed()) {
        throw new IOException("Failed to add table and rep_barrier CFs to meta. "
          + MasterProcedureUtil.unwrapRemoteIOException(result));
      }
    }
  }

  /**
   * Check hbase:meta is up and ready for reading. For use during Master startup only.
   * @return True if meta is UP and online and startup can progress. Otherwise, meta is not online
   *         and we will hold here until operator intervention.
   */
  @InterfaceAudience.Private
  public boolean waitForMetaOnline() {
    return isRegionOnline(RegionInfoBuilder.FIRST_META_REGIONINFO);
  }

  /**
   * @return True if region is online and scannable else false if an error or shutdown (Otherwise we
   *         just block in here holding up all forward-progess).
   */
  private boolean isRegionOnline(RegionInfo ri) {
    RetryCounter rc = null;
    while (!isStopped()) {
      RegionState rs = this.assignmentManager.getRegionStates().getRegionState(ri);
      if (rs != null && rs.isOpened()) {
        if (this.getServerManager().isServerOnline(rs.getServerName())) {
          return true;
        }
      }
      // Region is not OPEN.
      Optional<Procedure<MasterProcedureEnv>> optProc = this.procedureExecutor.getProcedures()
        .stream().filter(p -> p instanceof ServerCrashProcedure).findAny();
      // TODO: Add a page to refguide on how to do repair. Have this log message point to it.
      // Page will talk about loss of edits, how to schedule at least the meta WAL recovery, and
      // then how to assign including how to break region lock if one held.
      LOG.warn(
        "{} is NOT online; state={}; ServerCrashProcedures={}. Master startup cannot "
          + "progress, in holding-pattern until region onlined.",
        ri.getRegionNameAsString(), rs, optProc.isPresent());
      // Check once-a-minute.
      if (rc == null) {
        rc = new RetryCounterFactory(Integer.MAX_VALUE, 1000, 60_000).create();
      }
      Threads.sleep(rc.getBackoffTimeAndIncrementAttempts());
    }
    return false;
  }

  /**
   * Check hbase:namespace table is assigned. If not, startup will hang looking for the ns table
   * (TODO: Fix this! NS should not hold-up startup).
   * @return True if namespace table is up/online.
   */
  @InterfaceAudience.Private
  public boolean waitForNamespaceOnline() {
    List<RegionInfo> ris =
      this.assignmentManager.getRegionStates().getRegionsOfTable(TableName.NAMESPACE_TABLE_NAME);
    if (ris.isEmpty()) {
      // If empty, means we've not assigned the namespace table yet... Just return true so startup
      // continues and the namespace table gets created.
      return true;
    }
    // Else there are namespace regions up in meta. Ensure they are assigned before we go on.
    for (RegionInfo ri : ris) {
      if (!isRegionOnline(ri)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Adds the {@code MasterQuotasObserver} to the list of configured Master observers to
   * automatically remove quotas for a table when that table is deleted.
   */
  @InterfaceAudience.Private
  public void updateConfigurationForQuotasObserver(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("f970bf09-46ab-31d1-b270-763a2cf6fbae"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("328d6124-bd4e-3fff-8859-a791cfd04103"))) {
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
    // We're configured to not delete quotas on table deletion, so we don't need to add the obs.
    if (
      !conf.getBoolean(MasterQuotasObserver.REMOVE_QUOTA_ON_TABLE_DELETE,
        MasterQuotasObserver.REMOVE_QUOTA_ON_TABLE_DELETE_DEFAULT)
    ) {
      return;
    }
    String[] masterCoprocs = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    final int length = null == masterCoprocs ? 0 : masterCoprocs.length;
    String[] updatedCoprocs = new String[length + 1];
    if (((KnobRuntime.check(java.util.UUID.fromString("fe74c038-ba32-3665-9e3c-a9da265e68b0"))) ? ((length) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("82f2a963-5b4b-331c-ac25-7fa94b6f9856"))) ? ((length) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("423a9d72-09c9-301f-b820-f177253c0dba"))) ? ((length) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e822b3ed-424d-3461-b497-e4372864ae5c"))) ? ((length) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("67a17bed-b97a-3541-8fb9-c7dba179861b"))) ? ((length) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b7493815-5a31-3952-af12-0a0993fce934"))) ? ((length) >= (0)) : (length > 0))))))))))))) {
      System.arraycopy(masterCoprocs, 0, updatedCoprocs, 0, masterCoprocs.length);
    }
    updatedCoprocs[length] = MasterQuotasObserver.class.getName();
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, updatedCoprocs);
  }

  private void initMobCleaner() {
    this.mobFileCleanerChore = new MobFileCleanerChore(this);
    getChoreService().scheduleChore(mobFileCleanerChore);
    this.mobFileCompactionChore = new MobFileCompactionChore(this);
    getChoreService().scheduleChore(mobFileCompactionChore);
  }

  /**
   * <p>
   * Create a {@link ServerManager} instance.
   * </p>
   * <p>
   * Will be overridden in tests.
   * </p>
   */
  @InterfaceAudience.Private
  protected ServerManager createServerManager(MasterServices master, RegionServerList storage)
    throws IOException {
    // We put this out here in a method so can do a Mockito.spy and stub it out
    // w/ a mocked up ServerManager.
    setupClusterConnection();
    return new ServerManager(master, storage);
  }

  private void waitForRegionServers(final MonitoredTask status)
    throws IOException, InterruptedException {
    this.serverManager.waitForRegionServers(status);
  }

  // Will be overridden in tests
  @InterfaceAudience.Private
  protected void initClusterSchemaService() throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("44e8cd90-4bb2-3582-8327-64e6b4b121f5"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7869841f-d6ba-36f3-8fdb-b067d6f7533b"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("576028ec-bcd0-3afa-a1d2-0dcf3a4df921"))) {
throw new java.io.IOException("Injected exception");
}
    this.clusterSchemaService = new ClusterSchemaServiceImpl(this);
    this.clusterSchemaService.startAsync();
    try {
      this.clusterSchemaService
        .awaitRunning(getConfiguration().getInt(HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS,
          DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS), TimeUnit.SECONDS);
    } catch (TimeoutException toe) {
      throw new IOException("Timedout starting ClusterSchemaService", toe);
    }
  }

  private void initQuotaManager() throws IOException {
    MasterQuotaManager quotaManager = new MasterQuotaManager(this);
    quotaManager.start();
    this.quotaManager = quotaManager;
  }

  private SpaceQuotaSnapshotNotifier createQuotaSnapshotNotifier() {
    SpaceQuotaSnapshotNotifier notifier =
      SpaceQuotaSnapshotNotifierFactory.getInstance().create(getConfiguration());
    return notifier;
  }

  public boolean isCatalogJanitorEnabled() {
    return catalogJanitorChore != null ? catalogJanitorChore.getEnabled() : false;
  }

  boolean isCleanerChoreEnabled() {
    boolean hfileCleanerFlag = true, logCleanerFlag = true;

    if (getHFileCleaner() != null) {
      hfileCleanerFlag = getHFileCleaner().getEnabled();
    }

    if (logCleaner != null) {
      logCleanerFlag = logCleaner.getEnabled();
    }

    return (hfileCleanerFlag && logCleanerFlag);
  }

  @Override
  public ServerManager getServerManager() {
if(KnobRuntime.check(java.util.UUID.fromString("a1205338-a689-309f-ac46-a57e449fb2ab"))) {
return null;
}
    return this.serverManager;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return this.fileSystemManager;
  }

  @Override
  public MasterWalManager getMasterWalManager() {
    return this.walManager;
  }

  @Override
  public SplitWALManager getSplitWALManager() {
    return splitWALManager;
  }

  @Override
  public TableStateManager getTableStateManager() {
    return tableStateManager;
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception then they just die
   * with a logged message. This should be fine because in general, we do not expect the master to
   * get such unhandled exceptions as OOMEs; it should be lightly loaded. See what HRegionServer
   * does if need to install an unexpected exception handler.
   */
  private void startServiceThreads() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("329a20b9-eaec-3359-96e5-a6c90440e5da"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c6219fc3-5376-3e0a-849c-dabc8fcd17e2"))) {
return;
}
    // Start the executor service pools
    final int masterOpenRegionPoolSize = conf.getInt(HConstants.MASTER_OPEN_REGION_THREADS,
      HConstants.MASTER_OPEN_REGION_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.MASTER_OPEN_REGION).setCorePoolSize(masterOpenRegionPoolSize));
    final int masterCloseRegionPoolSize = conf.getInt(HConstants.MASTER_CLOSE_REGION_THREADS,
      HConstants.MASTER_CLOSE_REGION_THREADS_DEFAULT);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.MASTER_CLOSE_REGION)
        .setCorePoolSize(masterCloseRegionPoolSize));
    final int masterServerOpThreads = conf.getInt(HConstants.MASTER_SERVER_OPERATIONS_THREADS,
      HConstants.MASTER_SERVER_OPERATIONS_THREADS_DEFAULT);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.MASTER_SERVER_OPERATIONS)
        .setCorePoolSize(masterServerOpThreads));
    final int masterServerMetaOpsThreads =
      conf.getInt(HConstants.MASTER_META_SERVER_OPERATIONS_THREADS,
        HConstants.MASTER_META_SERVER_OPERATIONS_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.MASTER_META_SERVER_OPERATIONS)
      .setCorePoolSize(masterServerMetaOpsThreads));
    final int masterLogReplayThreads = conf.getInt(HConstants.MASTER_LOG_REPLAY_OPS_THREADS,
      HConstants.MASTER_LOG_REPLAY_OPS_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.M_LOG_REPLAY_OPS).setCorePoolSize(masterLogReplayThreads));
    final int masterSnapshotThreads = conf.getInt(SnapshotManager.SNAPSHOT_POOL_THREADS_KEY,
      SnapshotManager.SNAPSHOT_POOL_THREADS_DEFAULT);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.MASTER_SNAPSHOT_OPERATIONS)
        .setCorePoolSize(masterSnapshotThreads).setAllowCoreThreadTimeout(true));
    final int masterMergeDispatchThreads = conf.getInt(HConstants.MASTER_MERGE_DISPATCH_THREADS,
      HConstants.MASTER_MERGE_DISPATCH_THREADS_DEFAULT);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.MASTER_MERGE_OPERATIONS)
        .setCorePoolSize(masterMergeDispatchThreads).setAllowCoreThreadTimeout(true));

    // We depend on there being only one instance of this executor running
    // at a time. To do concurrency, would need fencing of enable/disable of
    // tables.
    // Any time changing this maxThreads to > 1, pls see the comment at
    // AccessController#postCompletedCreateTableAction
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.MASTER_TABLE_OPERATIONS).setCorePoolSize(1));
    startProcedureExecutor();

    // Create log cleaner thread pool
    logCleanerPool = DirScanPool.getLogCleanerScanPool(conf);
    Map<String, Object> params = new HashMap<>();
    params.put(MASTER, this);
    // Start log cleaner thread
    int cleanerInterval =
      conf.getInt(HBASE_MASTER_CLEANER_INTERVAL, DEFAULT_HBASE_MASTER_CLEANER_INTERVAL);
    this.logCleaner =
      new LogCleaner(cleanerInterval, this, conf, getMasterWalManager().getFileSystem(),
        getMasterWalManager().getOldLogDir(), logCleanerPool, params);
    getChoreService().scheduleChore(logCleaner);

    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);

    // Create custom archive hfile cleaners
    String[] paths = conf.getStrings(HFileCleaner.HFILE_CLEANER_CUSTOM_PATHS);
    // todo: handle the overlap issues for the custom paths

    if (paths != null && paths.length > 0) {
      if (conf.getStrings(HFileCleaner.HFILE_CLEANER_CUSTOM_PATHS_PLUGINS) == null) {
        Set<String> cleanerClasses = new HashSet<>();
        String[] cleaners = conf.getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
        if (cleaners != null) {
          Collections.addAll(cleanerClasses, cleaners);
        }
        conf.setStrings(HFileCleaner.HFILE_CLEANER_CUSTOM_PATHS_PLUGINS,
          cleanerClasses.toArray(new String[cleanerClasses.size()]));
        LOG.info("Archive custom cleaner paths: {}, plugins: {}", Arrays.asList(paths),
          cleanerClasses);
      }
      // share the hfile cleaner pool in custom paths
      sharedHFileCleanerPool = DirScanPool.getHFileCleanerScanPool(conf.get(CUSTOM_POOL_SIZE, "6"));
      for (int i = 0; i < paths.length; i++) {
        Path path = new Path(paths[i].trim());
        HFileCleaner cleaner =
          new HFileCleaner("ArchiveCustomHFileCleaner-" + path.getName(), cleanerInterval, this,
            conf, getMasterFileSystem().getFileSystem(), new Path(archiveDir, path),
            HFileCleaner.HFILE_CLEANER_CUSTOM_PATHS_PLUGINS, sharedHFileCleanerPool, params, null);
if(KnobRuntime.check(java.util.UUID.fromString("b682fd40-6137-3c7c-916f-4d51013229cf"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cba43f4b-4bb9-365a-a769-547b3a8fbb5b"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7d8db09-43d1-38a4-a302-79a18f9573e9"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("cleanerThreadTimeoutMsec");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cleaner));
    field.set(cleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("acbf9861-475c-3608-9933-cf70389b9bbe"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("cleanerThreadCheckIntervalMsec");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cleaner));
    field.set(cleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb689fc4-8fc8-3c30-ac46-d4b86944cb05"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("73a3062c-0d9f-3cd0-ac85-bb1123b9cc8f"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06efb0ec-a576-3c1a-83e8-bd807bb78282"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de5e4a8a-322e-306c-8f0f-582280b84f00"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5d67df2-ce13-38f2-aed7-48a6c77a3269"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3244571f-3d4a-3a66-85cb-a59a532b0bd6"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("cleanerThreadTimeoutMsec");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cleaner));
    field.set(cleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2c9c1f4-5a9f-3526-95a9-e4355434be25"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46e0d2da-7416-34c3-b373-b0dcce1c6dd9"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8774a7d8-1527-3521-b2fe-535ec8c52281"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c205f476-1416-3967-83a1-c322b530b5bf"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a78cf94a-0c84-3492-b426-b4bfbc55ac03"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("cleanerThreadCheckIntervalMsec");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cleaner));
    field.set(cleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a811fae6-88b7-321a-a089-34812ccd087e"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25e0f2c2-97aa-39c1-8928-378865e310e0"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ca87187-cfd9-3547-a22b-9171ac3b1dd0"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7682d0b0-9c6c-3f1e-8a25-61aa3316f84f"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("running");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cleaner);
    field.set(cleaner, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c55fe66e-d4ad-3ac9-bfb5-067bb06a635f"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77221aab-2b4f-3cf0-aa6d-eea3f8310bf1"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54b6c9a8-9aa1-3823-b255-c69725d77781"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11da7b47-08cb-3667-87e9-00a1989d95c3"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6263da0-3cd3-36dd-b74f-110488310af2"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a07386e-e2cc-3982-9ad3-3de679ed3373"))) {
try {
    java.lang.reflect.Field field = cleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cleaner));
    field.set(cleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        hfileCleaners.add(cleaner);
        hfileCleanerPaths.add(path);
      }
    }

    // Create the whole archive dir cleaner thread pool
    exclusiveHFileCleanerPool = DirScanPool.getHFileCleanerScanPool(conf);
    hfileCleaners.add(0,
      new HFileCleaner(cleanerInterval, this, conf, getMasterFileSystem().getFileSystem(),
        archiveDir, exclusiveHFileCleanerPool, params, hfileCleanerPaths));
    if (KnobRuntime.check(java.util.UUID.fromString("711f1259-813f-3102-933d-4ef8c249a7ce"))) { hfileCleanerPaths.add(0, getMasterWalManager().getOldLogDir()); } else { hfileCleanerPaths.add(0, archiveDir); }
    // Schedule all the hfile cleaners
    for (HFileCleaner hFileCleaner : hfileCleaners) {
if(KnobRuntime.check(java.util.UUID.fromString("152782a8-d3e6-3322-9d04-9028a7148d08"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dcf46b53-e3ee-339c-b2d3-eb9dc968eca1"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59b9e181-0821-3598-9de2-b7be0b2203ef"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0cb7e0b8-66a9-3300-b229-5110c743560f"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b0be381-03f6-3c4f-8d13-566671d4b227"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eec69db5-225a-3f2d-9cf0-e1a38f3d955d"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("cleanerThreadCheckIntervalMsec");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c1cad03-fa77-3db4-989f-383e7a55e6ea"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c38291e-e433-31c3-9512-3215f79daf4f"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e8a78c5-cf39-35a2-8d4b-e051b748473c"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3bb62892-5643-3503-8662-67347fb415e9"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d77bc9f2-86c7-34be-a8b1-c0adc640114a"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("running");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(hFileCleaner);
    field.set(hFileCleaner, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b980ebe7-fa95-3c8f-801a-9f2ce393d268"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9997caa-cb8d-3ef7-9de6-e580c49a9403"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("cleanerThreadTimeoutMsec");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c9e594d-4a2e-3e84-85f2-9f48b13700ed"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc8927f8-9219-3d77-9f1e-ff6687cab6c7"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("463df6f8-c099-34f4-bf73-3a5910e6824a"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30ebdffe-3545-386c-98c0-aa0904b8a54a"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad2fcf8e-b572-3a98-888c-4d9aa490456c"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59bb9447-9d8b-321b-9157-053e68d584d4"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("throttlePoint");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5bde742-12f9-32e6-88ca-dfd0a6e73204"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e229f33b-01c9-33f3-9cb1-d782193f77b4"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e980ce0-b1c1-35b3-b1e2-48dab9a8ff85"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("smallQueueInitSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65e3622c-7c30-3e6e-85d7-a94ca9e7813f"))) {
try {
    java.lang.reflect.Field field = hFileCleaner.getClass().getDeclaredField("largeFileDeleteThreadNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileCleaner));
    field.set(hFileCleaner, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      getChoreService().scheduleChore(hFileCleaner);
    }

    // Regions Reopen based on very high storeFileRefCount is considered enabled
    // only if hbase.regions.recovery.store.file.ref.count has value > 0
    final int maxStoreFileRefCount = conf.getInt(HConstants.STORE_FILE_REF_COUNT_THRESHOLD,
      HConstants.DEFAULT_STORE_FILE_REF_COUNT_THRESHOLD);
    if (((KnobRuntime.check(java.util.UUID.fromString("55a4d9ee-7319-3e98-9e4f-dfa991a6362a"))) ? ((maxStoreFileRefCount) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b545fbef-82eb-3622-91f4-2243035d78dc"))) ? ((maxStoreFileRefCount) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8d8edc41-b6fb-35fb-a444-918a20d4729a"))) ? ((maxStoreFileRefCount) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fa10c8cc-c7b3-3506-9e42-b3cc0407847f"))) ? ((maxStoreFileRefCount) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("06325852-99cf-3fd3-adb6-ec10c909806a"))) ? ((maxStoreFileRefCount) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2d05d7c8-89db-3860-81f8-1f6ad786e057"))) ? ((maxStoreFileRefCount) < (0)) : (maxStoreFileRefCount > 0))))))))))))) {
      this.regionsRecoveryChore = new RegionsRecoveryChore(this, conf, this);
      getChoreService().scheduleChore(this.regionsRecoveryChore);
    } else {
      LOG.info(
        "Reopening regions with very high storeFileRefCount is disabled. "
          + "Provide threshold value > 0 for {} to enable it.",
        HConstants.STORE_FILE_REF_COUNT_THRESHOLD);
    }

    this.regionsRecoveryConfigManager = new RegionsRecoveryConfigManager(this);

    replicationBarrierCleaner =
      new ReplicationBarrierCleaner(conf, this, getConnection(), replicationPeerManager);
    getChoreService().scheduleChore(replicationBarrierCleaner);

    final boolean isSnapshotChoreEnabled = this.snapshotCleanupStateStore.get();
    this.snapshotCleanerChore = new SnapshotCleanerChore(this, conf, getSnapshotManager());
    if (((KnobRuntime.check(java.util.UUID.fromString("71dd404d-8069-3c91-9643-b1a6233ba7d2"))) ? (!isSnapshotChoreEnabled) : (((KnobRuntime.check(java.util.UUID.fromString("b3013d07-055d-30de-8bd7-040a34027ba6"))) ? (true) : (isSnapshotChoreEnabled))))) {
      getChoreService().scheduleChore(this.snapshotCleanerChore);
    } else {
      if (((KnobRuntime.check(java.util.UUID.fromString("da74cf5b-eb88-3ebb-8399-23ee073bf23a"))) ? (isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("3b22e717-43fc-3d8d-a6dc-f439aef13ec4"))) ? (isAborted()) : (LOG.isTraceEnabled()))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("9b06cad8-16c1-38b2-b0b3-b8e8c6201857"))) { LOG.error("Snapshot Cleaner Chore is disabled. Not starting up the chore.."); } else if (KnobRuntime.check(java.util.UUID.fromString("8c50b142-2cd4-3e58-b51a-999c1f23544f"))) { LOG.info("Snapshot Cleaner Chore is disabled. Not starting up the chore.."); } else if (KnobRuntime.check(java.util.UUID.fromString("3e4cba0e-081c-3faf-abc9-73433a18885f"))) { LOG.warn("Snapshot Cleaner Chore is disabled. Not starting up the chore.."); } else { LOG.trace("Snapshot Cleaner Chore is disabled. Not starting up the chore.."); }
      }
    }
    serviceStarted = true;
    if (((KnobRuntime.check(java.util.UUID.fromString("31155674-c0a9-3762-8469-56154d9a3082"))) ? (isAborted()) : (((KnobRuntime.check(java.util.UUID.fromString("8fcdcfae-7dad-3470-9576-50f158788ddd"))) ? (isStopped()) : (LOG.isTraceEnabled()))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("794e13c8-1d94-3750-8fcc-f2461ece8a3d"))) { LOG.info("Started service threads"); } else if (KnobRuntime.check(java.util.UUID.fromString("9a6cb436-317c-33a7-9c36-50117f27dd02"))) { LOG.warn("Started service threads"); } else if (KnobRuntime.check(java.util.UUID.fromString("6e649801-51ec-3f42-8f03-3f55fe62c785"))) { LOG.error("Started service threads"); } else { LOG.trace("Started service threads"); }
    }
  }

  @Override
  protected void stopServiceThreads() {
    if (masterJettyServer != null) {
      LOG.info("Stopping master jetty server");
      try {
        masterJettyServer.stop();
      } catch (Exception e) {
        if (KnobRuntime.check(java.util.UUID.fromString("eed94c98-234b-365c-8a8e-5507bd76c683"))) { abort("Failed to stop master jetty server", e); } else { LOG.error("Failed to stop master jetty server", e); }
      }
    }
    stopChores();

    super.stopServiceThreads();
    if (exclusiveHFileCleanerPool != null) {
      exclusiveHFileCleanerPool.shutdownNow();
      exclusiveHFileCleanerPool = null;
    }
    if (logCleanerPool != null) {
      logCleanerPool.shutdownNow();
      logCleanerPool = null;
    }
    if (sharedHFileCleanerPool != null) {
      sharedHFileCleanerPool.shutdownNow();
      sharedHFileCleanerPool = null;
    }

    LOG.debug("Stopping service threads");

    // stop procedure executor prior to other services such as server manager and assignment
    // manager, as these services are important for some running procedures. See HBASE-24117 for
    // example.
    stopProcedureExecutor();

    if (((KnobRuntime.check(java.util.UUID.fromString("7a10d9af-b821-3f0c-b972-0752a98b64a5"))) ? ((regionNormalizerManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("65160816-506c-3c89-ab18-e64c98aeb929"))) ? ((regionNormalizerManager) == (null)) : (regionNormalizerManager != null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("36af6f03-d5c5-3fb5-854a-ef6c4014bbd7"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("8ec04dd1-df95-3d3b-ba23-c24ddffa386d"))) { registerConfigurationObservers(); } else { regionNormalizerManager.stop(); }
    }
    if (this.quotaManager != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("54ec0abe-5a86-3faa-99e1-f55983b918f0"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("ae4d234d-d581-3465-b029-6be6700a11aa"))) { initializeMemStoreChunkCreator(); } else { this.quotaManager.stop(); }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("f554db38-b524-3c65-bc7f-c430dbc19761"))) ? ((this.activeMasterManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9b117c28-25a3-3428-9128-960e8ea29f24"))) ? ((this.activeMasterManager) == (null)) : (this.activeMasterManager != null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("1d5e8d6c-7361-3c19-9464-8638071615f1"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("c366deac-08d6-3918-b22e-f5af93d99d50"))) { initializeMemStoreChunkCreator(); } else { this.activeMasterManager.stop(); }
    }
    if (this.serverManager != null) {
      this.serverManager.stop();
    }
    if (this.assignmentManager != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("249c3337-5695-30be-99ba-6425de769340"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("e36522c6-01d6-3560-8de0-bc9f66601cbd"))) { initializeMemStoreChunkCreator(); } else { this.assignmentManager.stop(); }
    }

    if (masterRegion != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("867d70ac-a59c-3a30-8ff4-00dab5dea76a"))) { masterRegion.close(isStopped()); } else if (KnobRuntime.check(java.util.UUID.fromString("0c95b380-bf8f-3b00-938d-d744cc5200c8"))) { setInitialized(isStopped()); } else { masterRegion.close(isAborted()); }
    }
    if (this.walManager != null) {
      this.walManager.stop();
    }
    if (this.fileSystemManager != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("a1a587af-384c-3611-9ca5-833d3acb0b08"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("82c8ec9f-f1bc-39a4-93df-02601772353f"))) { initializeMemStoreChunkCreator(); } else { this.fileSystemManager.stop(); }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("e89e1758-28c3-346d-ba96-73ce2370bc4f"))) ? ((this.mpmHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1e6353aa-a800-3dcf-91a9-238cf98798b0"))) ? ((this.mpmHost) != (null)) : (this.mpmHost != null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("319a8d88-dcac-3e5e-bd00-5f5b60a13798"))) { stop("server shutting down."); } else { this.mpmHost.stop("server shutting down."); }
    }
    if (this.regionServerTracker != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("446cea4f-482b-34e4-9f38-e46da462a2b0"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("f762d458-7204-36f3-8a19-127ec803c920"))) { registerConfigurationObservers(); } else { this.regionServerTracker.stop(); }
    }
  }

  private void createProcedureExecutor() throws IOException {
    final String procedureDispatcherClassName =
      conf.get(HBASE_MASTER_RSPROC_DISPATCHER_CLASS, DEFAULT_HBASE_MASTER_RSPROC_DISPATCHER_CLASS);
    final RSProcedureDispatcher procedureDispatcher = ReflectionUtils.instantiateWithCustomCtor(
      procedureDispatcherClassName, new Class[] { MasterServices.class }, new Object[] { this });
    final MasterProcedureEnv procEnv = new MasterProcedureEnv(this, procedureDispatcher);
    procedureStore = new RegionProcedureStore(this, masterRegion,
      new MasterProcedureEnv.FsUtilsLeaseRecovery(this));
    procedureStore.registerListener(new ProcedureStoreListener() {

      @Override
      public void abortProcess() {
if(KnobRuntime.check(java.util.UUID.fromString("0d8128c2-0c7b-3a1f-80ae-22c79bd36e9d"))) {
return;
}
        abort("The Procedure Store lost the lease", null);
      }
    });
    MasterProcedureScheduler procedureScheduler = procEnv.getProcedureScheduler();
    procedureExecutor = new ProcedureExecutor<>(conf, procEnv, procedureStore, procedureScheduler);
    configurationManager.registerObserver(procEnv);

    int cpus = Runtime.getRuntime().availableProcessors();
    final int numThreads = conf.getInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, Math.max(
      (cpus > 0 ? cpus / 4 : 0), MasterProcedureConstants.DEFAULT_MIN_MASTER_PROCEDURE_THREADS));
    final boolean abortOnCorruption =
      conf.getBoolean(MasterProcedureConstants.EXECUTOR_ABORT_ON_CORRUPTION,
        MasterProcedureConstants.DEFAULT_EXECUTOR_ABORT_ON_CORRUPTION);
    procedureStore.start(numThreads);
    // Just initialize it but do not start the workers, we will start the workers later by calling
    // startProcedureExecutor. See the javadoc for finishActiveMasterInitialization for more
    // details.
    procedureExecutor.init(numThreads, abortOnCorruption);
    if (!procEnv.getRemoteDispatcher().start()) {
      throw new HBaseIOException("Failed start of remote dispatcher");
    }
  }

  // will be override in UT
  protected void startProcedureExecutor() throws IOException {
    procedureExecutor.startWorkers();
  }

  /**
   * Turn on/off Snapshot Cleanup Chore
   * @param on indicates whether Snapshot Cleanup Chore is to be run
   */
  void switchSnapshotCleanup(final boolean on, final boolean synchronous) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("87ac7d81-3291-38ca-bba5-f8bd054cc9d9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("86f5e728-6378-3199-a82d-17953dc31423"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e4294749-2edb-3754-aa69-27862bf04114"))) ? (!synchronous) : (synchronous))) {
      synchronized (this.snapshotCleanerChore) {
if(KnobRuntime.check(java.util.UUID.fromString("0c2d38fe-496a-3db9-9de5-050a44d043c7"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("aad54e3a-7ca7-3629-9031-aa90996ab575"))) { setInitialized(!on); } else if (KnobRuntime.check(java.util.UUID.fromString("be5b0dfd-bfc4-3502-b091-51eabab74bae"))) { switchSnapshotCleanup(!on); } else { switchSnapshotCleanup(on); }
      }
    } else {
      switchSnapshotCleanup(on);
    }
  }

  private void switchSnapshotCleanup(final boolean on) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8da5d84b-5079-32aa-84a9-5befc4dc4ef6"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5632f1dd-2cbb-3bc7-8ccd-f0f2e1afbc9e"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("ef0e5548-ca27-3261-80d3-6ce65d263b35"))) { snapshotCleanupStateStore.set(!on); } else { snapshotCleanupStateStore.set(on); }
    if (on) {
      getChoreService().scheduleChore(this.snapshotCleanerChore);
    } else {
      this.snapshotCleanerChore.cancel();
    }
  }

  private void stopProcedureExecutor() {
if(KnobRuntime.check(java.util.UUID.fromString("f4275fe3-7fb6-382c-ae8e-f09973d2106f"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("43b773f1-6985-3bc0-a359-0d0e0f3732e0"))) ? ((procedureExecutor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f070d21f-81e3-3a27-8fb8-c0c9a9e7eb7d"))) ? ((procedureExecutor) != (null)) : (procedureExecutor != null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("dc761b8b-86bf-37dc-b2a5-053fe436f8cf"))) { configurationManager.registerObserver(procedureExecutor.getEnvironment()); } else { configurationManager.deregisterObserver(procedureExecutor.getEnvironment()); }
      procedureExecutor.getEnvironment().getRemoteDispatcher().stop();
      if (KnobRuntime.check(java.util.UUID.fromString("1ced9a1f-6597-3030-8af3-343d1f6a9df8"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("9132bc54-227a-3f67-bb6c-208f2ff476a6"))) { registerConfigurationObservers(); } else { procedureExecutor.stop(); }
      if (KnobRuntime.check(java.util.UUID.fromString("fb72f8d3-53b8-336e-aaea-c63e8628931c"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("30d2d345-4f2e-3097-a5b6-d1e0df93fa32"))) { registerConfigurationObservers(); } else { procedureExecutor.join(); }
      procedureExecutor = null;
    }

    if (procedureStore != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("81dfabe6-c31c-3e94-82d6-a0d7249be080"))) { setInitialized(isStopped()); } else { procedureStore.stop(isAborted()); }
      procedureStore = null;
    }
  }

  private void stopChores() {
if(KnobRuntime.check(java.util.UUID.fromString("7d5f9558-d38d-3c7c-8e99-21c9aeb0f78d"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("4f7db23c-7e68-3f4f-8fc9-0abfd5d0eae5"))) ? ((getChoreService()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3e1971ca-ceab-30d5-9b81-14984033c226"))) ? ((getChoreService()) != (null)) : (getChoreService() != null))))) {
      shutdownChore(mobFileCleanerChore);
if(KnobRuntime.check(java.util.UUID.fromString("e3f9af0f-9557-3cb5-9275-42f43eb2d79d"))) {
try {
    java.lang.reflect.Field field = mobFileCompactionChore.getClass().getDeclaredField("regionBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mobFileCompactionChore));
    field.set(mobFileCompactionChore, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71ffff4d-ad48-32f3-bd2d-58a56fdf91cc"))) {
try {
    java.lang.reflect.Field field = mobFileCompactionChore.getClass().getDeclaredField("regionBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mobFileCompactionChore));
    field.set(mobFileCompactionChore, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d287811-2dcb-37be-9626-4b4925d0a3f3"))) {
try {
    java.lang.reflect.Field field = mobFileCompactionChore.getClass().getDeclaredField("regionBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mobFileCompactionChore));
    field.set(mobFileCompactionChore, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7369bcba-5513-3bec-b806-2f91b75e51be"))) {
try {
    java.lang.reflect.Field field = mobFileCompactionChore.getClass().getDeclaredField("regionBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mobFileCompactionChore));
    field.set(mobFileCompactionChore, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      shutdownChore(mobFileCompactionChore);
      shutdownChore(balancerChore);
      if (((KnobRuntime.check(java.util.UUID.fromString("b66cec86-a425-3181-aad9-7ffdadab015f"))) ? ((regionNormalizerManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b961bc96-7562-3b13-abfc-da89d83e0017"))) ? ((regionNormalizerManager) == (null)) : (regionNormalizerManager != null))))) {
        shutdownChore(regionNormalizerManager.getRegionNormalizerChore());
      }
      shutdownChore(clusterStatusChore);
      shutdownChore(catalogJanitorChore);
      shutdownChore(clusterStatusPublisherChore);
      shutdownChore(snapshotQuotaChore);
      shutdownChore(logCleaner);
      if (hfileCleaners != null) {
        for (ScheduledChore chore : hfileCleaners) {
          if (KnobRuntime.check(java.util.UUID.fromString("7b4369be-50a9-3915-8c27-2dee9cdcc0ff"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("0ce8b70e-d7fa-327a-9d33-b52df23bd841"))) { initializeMemStoreChunkCreator(); } else { chore.shutdown(); }
        }
        hfileCleaners = null;
      }
      shutdownChore(replicationBarrierCleaner);
      shutdownChore(snapshotCleanerChore);
      shutdownChore(hbckChore);
if(KnobRuntime.check(java.util.UUID.fromString("d463ea84-50a7-33c4-855e-08ce9b33e6f8"))) {
try {
    java.lang.reflect.Field field = regionsRecoveryChore.getClass().getDeclaredField("storeFileRefCountThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionsRecoveryChore));
    field.set(regionsRecoveryChore, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c368f0e4-7d61-30c0-ab4d-c97113d63bdc"))) {
try {
    java.lang.reflect.Field field = regionsRecoveryChore.getClass().getDeclaredField("storeFileRefCountThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionsRecoveryChore));
    field.set(regionsRecoveryChore, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a2f9d29-7a4a-3bf5-9388-3a9cfdc8f411"))) {
try {
    java.lang.reflect.Field field = regionsRecoveryChore.getClass().getDeclaredField("storeFileRefCountThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionsRecoveryChore));
    field.set(regionsRecoveryChore, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b74df77-f430-3966-9e2b-9d6690868502"))) {
try {
    java.lang.reflect.Field field = regionsRecoveryChore.getClass().getDeclaredField("storeFileRefCountThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionsRecoveryChore));
    field.set(regionsRecoveryChore, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      shutdownChore(regionsRecoveryChore);
      shutdownChore(rollingUpgradeChore);
      shutdownChore(oldWALsDirSizeChore);
    }
  }

  /** Returns Get remote side's InetAddress */
  InetAddress getRemoteInetAddress(final int port, final long serverStartCode)
    throws UnknownHostException {
    // Do it out here in its own little method so can fake an address when
    // mocking up in tests.
    InetAddress ia = RpcServer.getRemoteIp();

    // The call could be from the local regionserver,
    // in which case, there is no remote address.
    if (((KnobRuntime.check(java.util.UUID.fromString("2804e433-cb02-3811-bc62-a43c53b1871a"))) ? (((ia) == (null)) || (serverStartCode == startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("26c7645a-f8b7-3d9e-a8d0-77cf856f2c64"))) ? ((ia == null) || ((serverStartCode) != (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("14ccb49e-d746-3d9d-ae82-9f4d6e7ccb43"))) ? (((ia) != (null)) && ((serverStartCode) == (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("057067d9-fa5e-3d3e-9dfe-0bdcc449b6be"))) ? ((serverStartCode) != (startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("d0075700-a6af-308c-ac83-09e2328cd912"))) ? ((ia) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cd494a84-ee00-3b77-88fc-c530a8dba65a"))) ? (ia == null) : (((KnobRuntime.check(java.util.UUID.fromString("7c90fb14-e577-36d2-89b6-adf1746676f2"))) ? (((ia) == (null)) && (serverStartCode == startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("ceb5a2df-6210-3cef-be8a-5e0f4b974cb1"))) ? ((ia == null) && ((serverStartCode) == (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("2508fa60-f69d-3372-8a71-20ca2c0d67f1"))) ? (((ia) != (null)) || (serverStartCode == startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("6631f6b0-1cfc-3a9f-a696-4078a061eae3"))) ? ((ia == null) && (serverStartCode == startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("742b1f5e-dd0c-39b2-81fc-397fbde8549a"))) ? (((ia) != (null)) && ((serverStartCode) != (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("c35e9e4e-a13d-3e6e-83b7-497b44d8c83e"))) ? (((ia) == (null)) && ((serverStartCode) != (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("21509310-9ef8-307b-9c55-f52b02a7bd90"))) ? (((ia) != (null)) || ((serverStartCode) == (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("105f506f-33e9-3133-8414-006c2b59f055"))) ? ((ia == null) || ((serverStartCode) == (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("dd5f54a3-efe4-368b-9412-bf67180157d5"))) ? (((ia) != (null)) && (serverStartCode == startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("94935938-e272-37e5-a58a-03b81396537d"))) ? (((ia) == (null)) || ((serverStartCode) == (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("c1ee8348-1900-30de-8aaf-049fabb81bc4"))) ? (serverStartCode == startcode) : (((KnobRuntime.check(java.util.UUID.fromString("ce96f829-a8f4-393a-b1b7-c1ead7ef18a8"))) ? ((serverStartCode) == (startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("4f601107-711d-3217-a5ed-71ea0f0d33dc"))) ? (((ia) != (null)) || ((serverStartCode) != (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("9154195f-f33f-3fd3-a8bf-269dabd69922"))) ? ((ia == null) && ((serverStartCode) != (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("04686306-e88c-385a-b27f-9fbc49122d22"))) ? ((ia == null) || (serverStartCode == startcode)) : (((KnobRuntime.check(java.util.UUID.fromString("34e99f63-9362-317e-9af5-e1607d1e2fa8"))) ? ((ia) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6aeb879a-6005-325f-aa65-3f7f9f05dbca"))) ? (((ia) == (null)) || ((serverStartCode) != (startcode))) : (((KnobRuntime.check(java.util.UUID.fromString("c5a429da-c7ac-34b3-9524-af12b1d2d063"))) ? (((ia) == (null)) && ((serverStartCode) == (startcode))) : (ia == null && serverStartCode == startcode))))))))))))))))))))))))))))))))))))))))))))))))) {
      InetSocketAddress isa = rpcServices.getSocketAddress();
      if (isa != null && isa.getPort() == port) {
        ia = isa.getAddress();
      }
    }
    return ia;
  }

  /** Returns Maximum time we should run balancer for */
  private int getMaxBalancingTime() {
    // if max balancing time isn't set, defaulting it to period time
    int maxBalancingTime =
      getConfiguration().getInt(HConstants.HBASE_BALANCER_MAX_BALANCING, getConfiguration()
        .getInt(HConstants.HBASE_BALANCER_PERIOD, HConstants.DEFAULT_HBASE_BALANCER_PERIOD));
    return maxBalancingTime;
  }

  /** Returns Maximum number of regions in transition */
  private int getMaxRegionsInTransition() {
    int numRegions = this.assignmentManager.getRegionStates().getRegionAssignments().size();
    return Math.max((int) Math.floor(numRegions * this.maxRitPercent), 1);
  }

  /**
   * It first sleep to the next balance plan start time. Meanwhile, throttling by the max number
   * regions in transition to protect availability.
   * @param nextBalanceStartTime   The next balance plan start time
   * @param maxRegionsInTransition max number of regions in transition
   * @param cutoffTime             when to exit balancer
   */
  private void balanceThrottling(long nextBalanceStartTime, int maxRegionsInTransition,
    long cutoffTime) {
    boolean interrupted = false;

    // Sleep to next balance plan start time
    // But if there are zero regions in transition, it can skip sleep to speed up.
    while (
      !interrupted && EnvironmentEdgeManager.currentTime() < nextBalanceStartTime
        && this.assignmentManager.getRegionStates().hasRegionsInTransition()
    ) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("ecddde7d-04f4-36a7-9e96-e8c6481b1380"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("abcc1ae0-828a-3645-8cea-2f7987a62034"))) { Threads.sleep(100); } else { Thread.sleep(100); }
      } catch (InterruptedException ie) {
        interrupted = true;
      }
    }

    // Throttling by max number regions in transition
    while (
      !interrupted && maxRegionsInTransition > 0
        && this.assignmentManager.getRegionStates().getRegionsInTransitionCount()
            >= maxRegionsInTransition
        && EnvironmentEdgeManager.currentTime() <= cutoffTime
    ) {
      try {
        // sleep if the number of regions in transition exceeds the limit
if(KnobRuntime.check(java.util.UUID.fromString("86e74a5d-5430-3f49-b9fe-b9e7f32b368d"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("c3ddad31-d24b-3de9-8617-06771c5f2578"))) { Threads.sleep(100); } else { Thread.sleep(100); }
      } catch (InterruptedException ie) {
        interrupted = true;
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("a33296e6-d115-3caa-85ef-d029fd6bd903"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("b3c77226-640f-365a-bf54-19ed3326e075"))) ? (!interrupted) : (((KnobRuntime.check(java.util.UUID.fromString("557ee90c-6461-351c-b3b1-d3c70a7bcdb6"))) ? (true) : (interrupted))))))) Thread.currentThread().interrupt();
  }

  public BalanceResponse balance() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4e3690d2-9cb1-3e72-ab63-8d31fbe3197e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("46ed54ba-7abf-307c-98c0-b2a664cb0c3e"))) {
return null;
}
    return balance(BalanceRequest.defaultInstance());
  }

  /**
   * Trigger a normal balance, see {@link HMaster#balance()} . If the balance is not executed this
   * time, the metrics related to the balance will be updated. When balance is running, related
   * metrics will be updated at the same time. But if some checking logic failed and cause the
   * balancer exit early, we lost the chance to update balancer metrics. This will lead to user
   * missing the latest balancer info.
   */
  public BalanceResponse balanceOrUpdateMetrics() throws IOException {
    synchronized (this.balancer) {
if(KnobRuntime.check(java.util.UUID.fromString("00664b13-dc5d-39ba-8796-a1c0e49d0338"))) {
throw new java.io.IOException("Injected exception");
}
      BalanceResponse response = balance();
      if (!response.isBalancerRan()) {
        Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
          this.assignmentManager.getRegionStates().getAssignmentsForBalancer(this.tableStateManager,
            this.serverManager.getOnlineServersList());
        for (Map<ServerName, List<RegionInfo>> serverMap : assignments.values()) {
          serverMap.keySet().removeAll(this.serverManager.getDrainingServersList());
        }
        this.balancer.updateBalancerLoadInfo(assignments);
      }
      return ((KnobRuntime.check(java.util.UUID.fromString("d4f70303-9200-3a01-8249-c5734318bcfd"))) ? (balance()) : (response));
    }
  }

  /**
   * Checks master state before initiating action over region topology.
   * @param action the name of the action under consideration, for logging.
   * @return {@code true} when the caller should exit early, {@code false} otherwise.
   */
  @Override
  public boolean skipRegionManagementAction(final String action) {
    // Note: this method could be `default` on MasterServices if but for logging.
    if (!isInitialized()) {
      LOG.debug("Master has not been initialized, don't run {}.", action);
      return true;
    }
    if (this.getServerManager().isClusterShutdown()) {
      LOG.info("Cluster is shutting down, don't run {}.", action);
      return true;
    }
    if (isInMaintenanceMode()) {
      LOG.info("Master is in maintenance mode, don't run {}.", action);
      return true;
    }
    return false;
  }

  public BalanceResponse balance(BalanceRequest request) throws IOException {
    checkInitialized();

    BalanceResponse.Builder responseBuilder = BalanceResponse.newBuilder();

    if (loadBalancerStateStore == null || !(loadBalancerStateStore.get() || request.isDryRun())) {
      return responseBuilder.build();
    }

    if (skipRegionManagementAction("balancer")) {
      return responseBuilder.build();
    }

    synchronized (this.balancer) {
      // Only allow one balance run at at time.
      if (this.assignmentManager.hasRegionsInTransition()) {
        List<RegionStateNode> regionsInTransition = assignmentManager.getRegionsInTransition();
        // if hbase:meta region is in transition, result of assignment cannot be recorded
        // ignore the force flag in that case
        boolean metaInTransition = assignmentManager.isMetaRegionInTransition();
        List<RegionStateNode> toPrint = regionsInTransition;
        int max = 5;
        boolean truncated = false;
        if (regionsInTransition.size() > max) {
          toPrint = regionsInTransition.subList(0, max);
          truncated = true;
        }

        if (!request.isIgnoreRegionsInTransition() || metaInTransition) {
          LOG.info("Not running balancer (ignoreRIT=false" + ", metaRIT=" + metaInTransition
            + ") because " + regionsInTransition.size() + " region(s) in transition: " + toPrint
            + (truncated ? "(truncated list)" : ""));
          return responseBuilder.build();
        }
      }
      if (this.serverManager.areDeadServersInProgress()) {
        LOG.info("Not running balancer because processing dead regionserver(s): "
          + this.serverManager.getDeadServers());
        return responseBuilder.build();
      }

      if (this.cpHost != null) {
        try {
          if (this.cpHost.preBalance(request)) {
            LOG.debug("Coprocessor bypassing balancer request");
            return responseBuilder.build();
          }
        } catch (IOException ioe) {
          LOG.error("Error invoking master coprocessor preBalance()", ioe);
          return responseBuilder.build();
        }
      }

      Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
        this.assignmentManager.getRegionStates().getAssignmentsForBalancer(tableStateManager,
          this.serverManager.getOnlineServersList());
      for (Map<ServerName, List<RegionInfo>> serverMap : assignments.values()) {
        serverMap.keySet().removeAll(this.serverManager.getDrainingServersList());
      }

      // Give the balancer the current cluster state.
      this.balancer.updateClusterMetrics(getClusterMetricsWithoutCoprocessor());

      List<RegionPlan> plans = this.balancer.balanceCluster(assignments);

      responseBuilder.setBalancerRan(true).setMovesCalculated(plans == null ? 0 : plans.size());

      if (skipRegionManagementAction("balancer")) {
        // make one last check that the cluster isn't shutting down before proceeding.
        return responseBuilder.build();
      }

      // For dry run we don't actually want to execute the moves, but we do want
      // to execute the coprocessor below
      List<RegionPlan> sucRPs =
        request.isDryRun() ? Collections.emptyList() : executeRegionPlansWithThrottling(plans);

      if (this.cpHost != null) {
        try {
          this.cpHost.postBalance(request, sucRPs);
        } catch (IOException ioe) {
          // balancing already succeeded so don't change the result
          LOG.error("Error invoking master coprocessor postBalance()", ioe);
        }
      }

      responseBuilder.setMovesExecuted(sucRPs.size());
    }

    // If LoadBalancer did not generate any plans, it means the cluster is already balanced.
    // Return true indicating a success.
    return responseBuilder.build();
  }

  /**
   * Execute region plans with throttling
   * @param plans to execute
   * @return succeeded plans
   */
  public List<RegionPlan> executeRegionPlansWithThrottling(List<RegionPlan> plans) {
    List<RegionPlan> successRegionPlans = new ArrayList<>();
    int maxRegionsInTransition = getMaxRegionsInTransition();
    long balanceStartTime = EnvironmentEdgeManager.currentTime();
    long cutoffTime = balanceStartTime + this.maxBalancingTime;
    int rpCount = 0; // number of RegionPlans balanced so far
    if (plans != null && !plans.isEmpty()) {
      int balanceInterval = this.maxBalancingTime / plans.size();
      LOG.info(
        "Balancer plans size is " + plans.size() + ", the balance interval is " + balanceInterval
          + " ms, and the max number regions in transition is " + maxRegionsInTransition);

      for (RegionPlan plan : plans) {
        LOG.info("balance " + plan);
        // TODO: bulk assign
        try {
          this.assignmentManager.balance(plan);
        } catch (HBaseIOException hioe) {
          // should ignore failed plans here, avoiding the whole balance plans be aborted
          // later calls of balance() can fetch up the failed and skipped plans
          LOG.warn("Failed balance plan {}, skipping...", plan, hioe);
        }
        // rpCount records balance plans processed, does not care if a plan succeeds
        rpCount++;
        successRegionPlans.add(plan);

        if (this.maxBalancingTime > 0) {
          balanceThrottling(balanceStartTime + rpCount * balanceInterval, maxRegionsInTransition,
            cutoffTime);
        }

        // if performing next balance exceeds cutoff time, exit the loop
        if (
          this.maxBalancingTime > 0 && rpCount < plans.size()
            && EnvironmentEdgeManager.currentTime() > cutoffTime
        ) {
          // TODO: After balance, there should not be a cutoff time (keeping it as
          // a security net for now)
          LOG.debug(
            "No more balancing till next balance run; maxBalanceTime=" + this.maxBalancingTime);
          break;
        }
      }
    }
    LOG.debug("Balancer is going into sleep until next period in {}ms", getConfiguration()
      .getInt(HConstants.HBASE_BALANCER_PERIOD, HConstants.DEFAULT_HBASE_BALANCER_PERIOD));
    return successRegionPlans;
  }

  @Override
  public RegionNormalizerManager getRegionNormalizerManager() {
    return regionNormalizerManager;
  }

  @Override
  public boolean normalizeRegions(final NormalizeTableFilterParams ntfp,
    final boolean isHighPriority) throws IOException {
    if (regionNormalizerManager == null || !regionNormalizerManager.isNormalizerOn()) {
      LOG.debug("Region normalization is disabled, don't run region normalizer.");
      return false;
    }
    if (skipRegionManagementAction("region normalizer")) {
      return false;
    }
    if (assignmentManager.hasRegionsInTransition()) {
      return false;
    }

    final Set<TableName> matchingTables = getTableDescriptors(new LinkedList<>(),
      ntfp.getNamespace(), ntfp.getRegex(), ntfp.getTableNames(), false).stream()
        .map(TableDescriptor::getTableName).collect(Collectors.toSet());
    final Set<TableName> allEnabledTables =
      tableStateManager.getTablesInStates(TableState.State.ENABLED);
    final List<TableName> targetTables =
      new ArrayList<>(Sets.intersection(matchingTables, allEnabledTables));
    Collections.shuffle(targetTables);
    return regionNormalizerManager.normalizeRegions(targetTables, isHighPriority);
  }

  /** Returns Client info for use as prefix on an audit log string; who did an action */
  @Override
  public String getClientIdAuditPrefix() {
    return "Client=" + RpcServer.getRequestUserName().orElse(null) + "/"
      + RpcServer.getRemoteAddress().orElse(null);
  }

  /**
   * Switch for the background CatalogJanitor thread. Used for testing. The thread will continue to
   * run. It will just be a noop if disabled.
   * @param b If false, the catalog janitor won't do anything.
   */
  public void setCatalogJanitorEnabled(final boolean b) {
    this.catalogJanitorChore.setEnabled(b);
  }

  @Override
  public long mergeRegions(final RegionInfo[] regionsToMerge, final boolean forcible, final long ng,
    final long nonce) throws IOException {
    checkInitialized();

    final String regionNamesToLog = RegionInfo.getShortNameToLog(regionsToMerge);

    if (!isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
      LOG.warn("Merge switch is off! skip merge of " + regionNamesToLog);
      throw new DoNotRetryIOException(
        "Merge of " + regionNamesToLog + " failed because merge switch is off");
    }

    if (!getTableDescriptors().get(regionsToMerge[0].getTable()).isMergeEnabled()) {
      LOG.warn("Merge is disabled for the table! Skipping merge of {}", regionNamesToLog);
      throw new DoNotRetryIOException(
        "Merge of " + regionNamesToLog + " failed as region merge is disabled for the table");
    }

    return MasterProcedureUtil.submitProcedure(new NonceProcedureRunnable(this, ng, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preMergeRegions(regionsToMerge);
        String aid = getClientIdAuditPrefix();
        LOG.info("{} merge regions {}", aid, regionNamesToLog);
        submitProcedure(new MergeTableRegionsProcedure(procedureExecutor.getEnvironment(),
          regionsToMerge, forcible));
        getMaster().getMasterCoprocessorHost().postMergeRegions(regionsToMerge);
      }

      @Override
      protected String getDescription() {
        return "MergeTableProcedure";
      }
    });
  }

  @Override
  public long splitRegion(final RegionInfo regionInfo, final byte[] splitRow, final long nonceGroup,
    final long nonce) throws IOException {
    checkInitialized();

    if (!isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
      LOG.warn("Split switch is off! skip split of " + regionInfo);
      throw new DoNotRetryIOException(
        "Split region " + regionInfo.getRegionNameAsString() + " failed due to split switch off");
    }

    if (!getTableDescriptors().get(regionInfo.getTable()).isSplitEnabled()) {
      LOG.warn("Split is disabled for the table! Skipping split of {}", regionInfo);
      throw new DoNotRetryIOException("Split region " + regionInfo.getRegionNameAsString()
        + " failed as region split is disabled for the table");
    }

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preSplitRegion(regionInfo.getTable(), splitRow);
          LOG.info(getClientIdAuditPrefix() + " split " + regionInfo.getRegionNameAsString());

          // Execute the operation asynchronously
          submitProcedure(getAssignmentManager().createSplitProcedure(regionInfo, splitRow));
        }

        @Override
        protected String getDescription() {
          return "SplitTableProcedure";
        }
      });
  }

  // Public so can be accessed by tests. Blocks until move is done.
  // Replace with an async implementation from which you can get
  // a success/failure result.
  @InterfaceAudience.Private
  public void move(final byte[] encodedRegionName, byte[] destServerName) throws HBaseIOException {
    RegionState regionState =
      assignmentManager.getRegionStates().getRegionState(Bytes.toString(encodedRegionName));

    RegionInfo hri;
    if (regionState != null) {
      hri = regionState.getRegion();
    } else {
      throw new UnknownRegionException(Bytes.toStringBinary(encodedRegionName));
    }

    ServerName dest;
    List<ServerName> exclude = hri.getTable().isSystemTable()
      ? assignmentManager.getExcludedServersForSystemTable()
      : new ArrayList<>(1);
    if (
      destServerName != null && exclude.contains(ServerName.valueOf(Bytes.toString(destServerName)))
    ) {
      LOG.info(Bytes.toString(encodedRegionName) + " can not move to "
        + Bytes.toString(destServerName) + " because the server is in exclude list");
      destServerName = null;
    }
    if (destServerName == null || destServerName.length == 0) {
      LOG.info("Passed destination servername is null/empty so " + "choosing a server at random");
      exclude.add(regionState.getServerName());
      final List<ServerName> destServers = this.serverManager.createDestinationServersList(exclude);
      dest = balancer.randomAssignment(hri, destServers);
      if (dest == null) {
        LOG.debug("Unable to determine a plan to assign " + hri);
        return;
      }
    } else {
      ServerName candidate = ServerName.valueOf(Bytes.toString(destServerName));
      dest = balancer.randomAssignment(hri, Lists.newArrayList(candidate));
      if (dest == null) {
        LOG.debug("Unable to determine a plan to assign " + hri);
        return;
      }
      // TODO: What is this? I don't get it.
      if (
        dest.equals(serverName) && balancer instanceof BaseLoadBalancer
          && !((BaseLoadBalancer) balancer).shouldBeOnMaster(hri)
      ) {
        // To avoid unnecessary region moving later by balancer. Don't put user
        // regions on master.
        LOG.debug("Skipping move of region " + hri.getRegionNameAsString()
          + " to avoid unnecessary region moving later by load balancer,"
          + " because it should not be on master");
        return;
      }
    }

    if (dest.equals(regionState.getServerName())) {
      LOG.debug("Skipping move of region " + hri.getRegionNameAsString()
        + " because region already assigned to the same server " + dest + ".");
      return;
    }

    // Now we can do the move
    RegionPlan rp = new RegionPlan(hri, regionState.getServerName(), dest);
    assert rp.getDestination() != null : rp.toString() + " " + dest;

    try {
      checkInitialized();
      if (((KnobRuntime.check(java.util.UUID.fromString("8575bcc5-4771-32ca-9b11-44792072ee52"))) ? ((this.cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8f1936ca-a6e8-3ecb-9034-349458a0ff2e"))) ? ((this.cpHost) != (null)) : (this.cpHost != null))))) {
        this.cpHost.preMove(hri, rp.getSource(), rp.getDestination());
      }

      TransitRegionStateProcedure proc =
        this.assignmentManager.createMoveRegionProcedure(rp.getRegionInfo(), rp.getDestination());
      if (((KnobRuntime.check(java.util.UUID.fromString("c5fa94c0-2ffd-393e-9b5d-cfab4f6070a7"))) ? (conf.getBoolean(WARMUP_BEFORE_MOVE, !DEFAULT_WARMUP_BEFORE_MOVE)) : (conf.getBoolean(WARMUP_BEFORE_MOVE, DEFAULT_WARMUP_BEFORE_MOVE)))) {
        // Warmup the region on the destination before initiating the move. this call
        // is synchronous and takes some time. doing it before the source region gets
        // closed
        LOG.info(getClientIdAuditPrefix() + " move " + rp + ", warming up region on "
          + rp.getDestination());
        serverManager.sendRegionWarmup(rp.getDestination(), hri);
      }
      if (KnobRuntime.check(java.util.UUID.fromString("573ddbdb-7f38-31cb-962b-7ebcbe345bf7"))) { LOG.warn(((getClientIdAuditPrefix() + " move ") + (rp)) + (", running balancer")); } else if (KnobRuntime.check(java.util.UUID.fromString("b8175a24-e178-349c-86de-5de1ec33cf91"))) { LOG.error((getClientIdAuditPrefix() + " move " + rp) + (", running balancer")); } else if (KnobRuntime.check(java.util.UUID.fromString("2e84bc26-0bd0-3ede-b6e7-258c401ece3d"))) { LOG.error(((getClientIdAuditPrefix() + " move ") + (rp)) + (", running balancer")); } else if (KnobRuntime.check(java.util.UUID.fromString("55235d3f-bb65-340e-a0f1-72fe8795b53f"))) { LOG.warn((getClientIdAuditPrefix() + " move " + rp) + (", running balancer")); } else if (KnobRuntime.check(java.util.UUID.fromString("7f32f73d-a561-3810-a03e-7d4a1fc32f06"))) { LOG.info(((getClientIdAuditPrefix() + " move ") + (rp)) + (", running balancer")); } else if (KnobRuntime.check(java.util.UUID.fromString("3b66d49e-1d47-32bf-b618-3b065f2dfcf2"))) { LOG.info((getClientIdAuditPrefix() + " move " + rp) + (", running balancer")); } else { LOG.info(getClientIdAuditPrefix() + " move " + rp + ", running balancer"); }
if(KnobRuntime.check(java.util.UUID.fromString("a5bb68db-642b-3a94-8b63-b80065aa1883"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("evictCache");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("907e6b5f-8532-3c0a-99d1-5664aecc0abe"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("forceRetainmentTotalWait");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d99b8a0f-0e4c-3a6c-ac85-c10a24a38b08"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("isSplit");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7d10557-4c44-375b-84b9-ea49b8c5749a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("forceNewPlan");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      Future<byte[]> future = ProcedureSyncWait.submitProcedure(this.procedureExecutor, proc);
      try {
        // Is this going to work? Will we throw exception on error?
        // TODO: CompletableFuture rather than this stunted Future.
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new HBaseIOException(e);
      }
      if (this.cpHost != null) {
        this.cpHost.postMove(hri, rp.getSource(), rp.getDestination());
      }
    } catch (IOException ioe) {
      if (ioe instanceof HBaseIOException) {
        throw (HBaseIOException) ioe;
      }
      throw new HBaseIOException(ioe);
    }
  }

  @Override
  public long createTable(final TableDescriptor tableDescriptor, final byte[][] splitKeys,
    final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    TableDescriptor desc = getMasterCoprocessorHost().preCreateTableRegionsInfos(tableDescriptor);
    if (desc == null) {
      throw new IOException("Creation for " + tableDescriptor + " is canceled by CP");
    }
    String namespace = desc.getTableName().getNamespaceAsString();
    this.clusterSchemaService.getNamespace(namespace);

    RegionInfo[] newRegions = ModifyRegionUtils.createRegionInfos(desc, splitKeys);
    TableDescriptorChecker.sanityCheck(conf, desc);

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preCreateTable(desc, newRegions);

          LOG.info(getClientIdAuditPrefix() + " create " + desc);

          // TODO: We can handle/merge duplicate requests, and differentiate the case of
          // TableExistsException by saying if the schema is the same or not.
          //
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
          submitProcedure(
            new CreateTableProcedure(procedureExecutor.getEnvironment(), desc, newRegions, latch));
          latch.await();

          getMaster().getMasterCoprocessorHost().postCreateTable(desc, newRegions);
        }

        @Override
        protected String getDescription() {
          return "CreateTableProcedure";
        }
      });
  }

  @Override
  public long createSystemTable(final TableDescriptor tableDescriptor) throws IOException {
    if (isStopped()) {
      throw new MasterNotRunningException();
    }

    TableName tableName = tableDescriptor.getTableName();
    if (!(tableName.isSystemTable())) {
      throw new IllegalArgumentException(
        "Only system table creation can use this createSystemTable API");
    }

    RegionInfo[] newRegions = ModifyRegionUtils.createRegionInfos(tableDescriptor, null);

    LOG.info(getClientIdAuditPrefix() + " create " + tableDescriptor);

    // This special create table is called locally to master. Therefore, no RPC means no need
    // to use nonce to detect duplicated RPC call.
    long procId = this.procedureExecutor.submitProcedure(
      new CreateTableProcedure(procedureExecutor.getEnvironment(), tableDescriptor, newRegions));

    return procId;
  }

  private void startActiveMasterManager(int infoPort) throws KeeperException {
    String backupZNode = ZNodePaths.joinZNode(zooKeeper.getZNodePaths().backupMasterAddressesZNode,
      serverName.toString());
    /*
     * Add a ZNode for ourselves in the backup master directory since we may not become the active
     * master. If so, we want the actual active master to know we are backup masters, so that it
     * won't assign regions to us if so configured. If we become the active master later,
     * ActiveMasterManager will delete this node explicitly. If we crash before then, ZooKeeper will
     * delete this node for us since it is ephemeral.
     */
    LOG.info("Adding backup master ZNode " + backupZNode);
    if (!MasterAddressTracker.setMasterAddress(zooKeeper, backupZNode, serverName, infoPort)) {
      LOG.warn("Failed create of " + backupZNode + " by " + serverName);
    }
    this.activeMasterManager.setInfoPort(infoPort);
    int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
    // If we're a backup master, stall until a primary to write this address
    if (conf.getBoolean(HConstants.MASTER_TYPE_BACKUP, HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
      LOG.debug("HMaster started in backup mode. Stalling until master znode is written.");
      // This will only be a minute or so while the cluster starts up,
      // so don't worry about setting watches on the parent znode
      while (!activeMasterManager.hasActiveMaster()) {
        LOG.debug("Waiting for master address and cluster state znode to be written.");
        Threads.sleep(timeout);
      }
    }

    // Here for the master startup process, we use TaskGroup to monitor the whole progress.
    // The UI is similar to how Hadoop designed the startup page for the NameNode.
    // See HBASE-21521 for more details.
    // We do not cleanup the startupTaskGroup, let the startup progress information
    // be permanent in the MEM.
    startupTaskGroup = TaskMonitor.createTaskGroup(true, "Master startup");
    try {
      if (activeMasterManager.blockUntilBecomingActiveMaster(timeout, startupTaskGroup)) {
if(KnobRuntime.check(java.util.UUID.fromString("a3d6e8ac-ce00-317f-b1c8-e6c97c8762d6"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("65556cad-e1cf-3c9a-9261-3f50067a748f"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("1a588f2c-b2d2-3254-83c2-f14c8096930f"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("0efa7506-b3d9-31d8-ab5c-0b33cc62f352"))) { tryMigrateMetaLocationsFromZooKeeper(); } else if (KnobRuntime.check(java.util.UUID.fromString("2c1d8af9-a60e-3ca3-9885-b25a82fc2da6"))) { initializeMemStoreChunkCreator(); } else { finishActiveMasterInitialization(); }
      }
    } catch (Throwable t) {
      startupTaskGroup.abort("Failed to become active master due to:" + t.getMessage());
      LOG.error(HBaseMarkers.FATAL, "Failed to become active master", t);
      // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
      if (
        t instanceof NoClassDefFoundError
          && t.getMessage().contains("org/apache/hadoop/hdfs/protocol/HdfsConstants$SafeModeAction")
      ) {
        // improved error message for this special case
        abort("HBase is having a problem with its Hadoop jars.  You may need to recompile "
          + "HBase against Hadoop version " + org.apache.hadoop.util.VersionInfo.getVersion()
          + " or change your hadoop jars to start properly", t);
      } else {
        abort("Unhandled exception. Starting shutdown.", t);
      }
    }
  }

  private static boolean isCatalogTable(final TableName tableName) {
    return tableName.equals(TableName.META_TABLE_NAME);
  }

  @Override
  public long deleteTable(final TableName tableName, final long nonceGroup, final long nonce)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1508dc10-fd61-3a9a-9a6e-8116a0b2c5cf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eeb59d6c-1a2b-3b5e-8596-7fd78f22a2db"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ea314ac3-ff64-3fc8-a6f0-e3fc126c75d4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3bfcaf21-2cfb-3ba4-a53a-7c3f6bee4dc4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0dc6b7b6-088f-3179-ac85-fd53c4bf64d3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f94bf58-f6d6-3b34-b0c7-1ec57e6d9a85"))) {
return 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("bb11de71-6691-3aa9-9cfa-1a8247d1bf01"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("9a58d45a-7bcb-31f2-97f2-67d7b3f01997"))) { initializeMemStoreChunkCreator(); } else { checkInitialized(); }

if(KnobRuntime.check(java.util.UUID.fromString("edf4db0d-ce45-3c90-bf6e-9ce9bfc7db8a"))) {
throw new java.io.IOException("Injected exception");
}
    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preDeleteTable(tableName);

          LOG.info(getClientIdAuditPrefix() + " delete " + tableName);

          // TODO: We can handle/merge duplicate request
          //
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
          submitProcedure(
            new DeleteTableProcedure(procedureExecutor.getEnvironment(), tableName, latch));
          latch.await();

if(KnobRuntime.check(java.util.UUID.fromString("04406349-80d3-39fc-9ecd-800f1bca25f5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05866534-7932-3ead-9ea2-6e2d14d368fa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("856e30eb-635d-3543-942c-4adf4ff88d79"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc8c0293-a8bd-3e5c-b3ec-bab9654de813"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0b2883a4-88bb-388f-94b8-dac426fcd801"))) {
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
          getMaster().getMasterCoprocessorHost().postDeleteTable(tableName);
        }

        @Override
        protected String getDescription() {
          return "DeleteTableProcedure";
        }
      });
  }

  @Override
  public long truncateTable(final TableName tableName, final boolean preserveSplits,
    final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8c9aba4e-0f60-3990-90f0-1b09a6a817f8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9af44274-a028-332f-bef0-befcc6d0c47f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2e22c240-93b0-31b4-99b8-f31c3a2c90eb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("50e39d95-5383-3dee-b175-9d1360a41d04"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1acb5c2c-f8b4-335c-a676-eda53aeac5b3"))) {
throw new java.io.IOException("Injected exception");
}
          getMaster().getMasterCoprocessorHost().preTruncateTable(tableName);

          LOG.info(getClientIdAuditPrefix() + " truncate " + tableName);
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch(2, 0);
          submitProcedure(new TruncateTableProcedure(procedureExecutor.getEnvironment(), tableName,
            preserveSplits, latch));
          latch.await();

          getMaster().getMasterCoprocessorHost().postTruncateTable(tableName);
        }

        @Override
        protected String getDescription() {
          return "TruncateTableProcedure";
        }
      });
  }

  @Override
  public long truncateRegion(final RegionInfo regionInfo, final long nonceGroup, final long nonce)
    throws IOException {
    checkInitialized();

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preTruncateRegion(regionInfo);

          LOG.info(
            getClientIdAuditPrefix() + " truncate region " + regionInfo.getRegionNameAsString());

          // Execute the operation asynchronously
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch(2, 0);
          submitProcedure(
            new TruncateRegionProcedure(procedureExecutor.getEnvironment(), regionInfo, latch));
          latch.await();

if(KnobRuntime.check(java.util.UUID.fromString("aa8450df-e6e1-3863-a7ba-a504c8983fcf"))) {
throw new java.io.IOException("Injected exception");
}
          getMaster().getMasterCoprocessorHost().postTruncateRegion(regionInfo);
        }

        @Override
        protected String getDescription() {
          return "TruncateRegionProcedure";
        }
      });
  }

  @Override
  public long addColumn(final TableName tableName, final ColumnFamilyDescriptor column,
    final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    checkTableExists(tableName);

    return modifyTable(tableName, new TableDescriptorGetter() {

      @Override
      public TableDescriptor get() throws IOException {
        TableDescriptor old = getTableDescriptors().get(tableName);
        if (old.hasColumnFamily(column.getName())) {
          throw new InvalidFamilyOperationException("Column family '" + column.getNameAsString()
            + "' in table '" + tableName + "' already exists so cannot be added");
        }

        return TableDescriptorBuilder.newBuilder(old).setColumnFamily(column).build();
      }
    }, nonceGroup, nonce, true);
  }

  /**
   * Implement to return TableDescriptor after pre-checks
   */
  protected interface TableDescriptorGetter {
    TableDescriptor get() throws IOException;
  }

  @Override
  public long modifyColumn(final TableName tableName, final ColumnFamilyDescriptor descriptor,
    final long nonceGroup, final long nonce) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e4d82693-65b0-3954-b74a-b3a1a5dac271"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1fe7fe81-b935-37c7-a452-dd8314427271"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8d048e8d-5c68-30c9-8eec-b80780b3204d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("033bbc45-0fda-3bd6-aa1e-71c5cf5ba886"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cffc02d4-b233-349c-bd82-54a9723a5de1"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fbbdc64e-91c9-3300-937d-1e764f2959de"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("2a19bd5c-1f85-30bb-a416-6e9617317197"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("954bda70-6344-34d1-a7b9-9c82c83640af"))) { registerConfigurationObservers(); } else { checkInitialized(); }
if(KnobRuntime.check(java.util.UUID.fromString("03860f38-feb6-3686-bb40-1f3151e29ff9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ee9fd6e5-16a9-3749-920b-6bbe7b1c335b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("35c7c926-9742-3f20-816f-f353c2b22d74"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("00630b31-b521-32dd-8721-05b4cb5e8ed8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("72bbba5f-d475-3005-b000-6db46aa6eee6"))) {
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
    checkTableExists(tableName);
if(KnobRuntime.check(java.util.UUID.fromString("e5631773-00da-3f1d-83ca-757ce45ebcc5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("de8cc286-0eb7-3581-898a-04508d74df24"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("52fb3cfc-8c04-3676-86c1-cd1965255055"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f813f78a-5176-3fab-925f-79b86a7e1558"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6123c01f-50b6-3ab1-8199-fea19506d3c7"))) {
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
    return modifyTable(tableName, new TableDescriptorGetter() {

      @Override
      public TableDescriptor get() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("70a512b1-4889-39c2-b977-839d11dd8f3b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f71a6f14-628a-310d-b6af-6707c0c4147c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("06275e64-cf2e-3be7-8e8f-a085d1a2f2da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2c7d8d34-82c2-3da2-b5f0-b68d8224103f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("11458b7a-3fe5-3d9a-a744-83f59b72aa9b"))) {
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
        TableDescriptor old = getTableDescriptors().get(tableName);
        if (!old.hasColumnFamily(descriptor.getName())) {
          throw new InvalidFamilyOperationException("Family '" + descriptor.getNameAsString()
            + "' does not exist, so it cannot be modified");
        }

        return TableDescriptorBuilder.newBuilder(old).modifyColumnFamily(descriptor).build();
      }
    }, nonceGroup, nonce, true);
  }

  @Override
  public long modifyColumnStoreFileTracker(TableName tableName, byte[] family, String dstSFT,
    long nonceGroup, long nonce) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("eb35c52a-eb2f-30fe-8ff3-d1f482ddce32"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ca5fb2e9-11ae-3bf1-9fe9-07d43f37ab24"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a8418e54-237d-308e-8e66-397ae2b5f30f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c5d5db57-683a-3934-a7e5-f73a00ef15b7"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2dd5e416-0543-3ee4-a3e1-d5b93f5cf0dc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d3307b35-e5cc-3830-bfbd-b71577d4c0b7"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("e4b8d2df-1a35-3af4-99bb-d5d47257b2e9"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("e28eba6b-1f22-3b64-8401-fa4d0bd148ea"))) { initializeMemStoreChunkCreator(); } else { checkInitialized(); }
if(KnobRuntime.check(java.util.UUID.fromString("3a39cae8-5bbb-3ced-87de-72b5df827b8b"))) {
throw new java.io.IOException("Injected exception");
}
    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {

        @Override
        protected void run() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("efb96bdb-0801-38f3-98c7-8aecc099094f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("04282507-8afe-3105-818a-023e354648e0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("937323b5-3642-3cea-8c68-4423f8eb1d7a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6bd17063-513a-3bb0-8e4f-a4a8ceca1f68"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("78ca7039-3700-3a28-8f75-ac9573f35b9b"))) {
throw new java.io.IOException("Injected exception");
}
          String sft = getMaster().getMasterCoprocessorHost()
            .preModifyColumnFamilyStoreFileTracker(tableName, family, dstSFT);
if(KnobRuntime.check(java.util.UUID.fromString("c920ed3a-e1df-34d7-81a3-bd20de8dbb8e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b79c7b08-7cd3-3035-b2ff-d740f3711139"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5aee630-2631-33da-a162-ba10490ef293"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d3dfa494-0576-326c-95ac-6c6b94388d4a"))) {
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
          LOG.info("{} modify column {} store file tracker of table {} to {}",
            getClientIdAuditPrefix(), Bytes.toStringBinary(family), tableName, sft);
          submitProcedure(new ModifyColumnFamilyStoreFileTrackerProcedure(
            procedureExecutor.getEnvironment(), tableName, family, sft));
if(KnobRuntime.check(java.util.UUID.fromString("410f2a3c-0a98-31cf-ad4e-03677c274cd0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5401d52a-5f45-301d-a6cd-be7b42b727e4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("35af65fc-97b5-3e72-ad13-288d9d589779"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("35b03f7b-eaa7-3477-8720-b8d992cc30ee"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4763e91b-4a3e-3c01-a910-8b6a12a8264a"))) {
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
          getMaster().getMasterCoprocessorHost().postModifyColumnFamilyStoreFileTracker(tableName,
            family, dstSFT);
        }

        @Override
        protected String getDescription() {
          return "ModifyColumnFamilyStoreFileTrackerProcedure";
        }
      });
  }

  @Override
  public long deleteColumn(final TableName tableName, final byte[] columnName,
    final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    checkTableExists(tableName);

    return modifyTable(tableName, new TableDescriptorGetter() {

      @Override
      public TableDescriptor get() throws IOException {
        TableDescriptor old = getTableDescriptors().get(tableName);

        if (!old.hasColumnFamily(columnName)) {
          throw new InvalidFamilyOperationException(
            "Family '" + Bytes.toString(columnName) + "' does not exist, so it cannot be deleted");
        }
        if (old.getColumnFamilyCount() == 1) {
          throw new InvalidFamilyOperationException("Family '" + Bytes.toString(columnName)
            + "' is the only column family in the table, so it cannot be deleted");
        }
        return TableDescriptorBuilder.newBuilder(old).removeColumnFamily(columnName).build();
      }
    }, nonceGroup, nonce, true);
  }

  @Override
  public long enableTable(final TableName tableName, final long nonceGroup, final long nonce)
    throws IOException {
    checkInitialized();

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preEnableTable(tableName);

          // Normally, it would make sense for this authorization check to exist inside
          // AccessController, but because the authorization check is done based on internal state
          // (rather than explicit permissions) we'll do the check here instead of in the
          // coprocessor.
          MasterQuotaManager quotaManager = getMasterQuotaManager();
          if (quotaManager != null) {
            if (quotaManager.isQuotaInitialized()) {
              // skip checking quotas for system tables, see:
              // https://issues.apache.org/jira/browse/HBASE-28183
              if (!tableName.isSystemTable()) {
                SpaceQuotaSnapshot currSnapshotOfTable =
                  QuotaTableUtil.getCurrentSnapshotFromQuotaTable(getConnection(), tableName);
                if (currSnapshotOfTable != null) {
                  SpaceQuotaStatus quotaStatus = currSnapshotOfTable.getQuotaStatus();
                  if (
                    quotaStatus.isInViolation()
                      && SpaceViolationPolicy.DISABLE == quotaStatus.getPolicy().orElse(null)
                  ) {
                    throw new AccessDeniedException("Enabling the table '" + tableName
                      + "' is disallowed due to a violated space quota.");
                  }
                }
              }
            } else if (LOG.isTraceEnabled()) {
              LOG
                .trace("Unable to check for space quotas as the MasterQuotaManager is not enabled");
            }
          }

          LOG.info(getClientIdAuditPrefix() + " enable " + tableName);

          // Execute the operation asynchronously - client will check the progress of the operation
          // In case the request is from a <1.1 client before returning,
          // we want to make sure that the table is prepared to be
          // enabled (the table is locked and the table state is set).
          // Note: if the procedure throws exception, we will catch it and rethrow.
          final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createLatch();
          submitProcedure(
            new EnableTableProcedure(procedureExecutor.getEnvironment(), tableName, prepareLatch));
          prepareLatch.await();

          getMaster().getMasterCoprocessorHost().postEnableTable(tableName);
        }

        @Override
        protected String getDescription() {
          return "EnableTableProcedure";
        }
      });
  }

  @Override
  public long disableTable(final TableName tableName, final long nonceGroup, final long nonce)
    throws IOException {
    checkInitialized();

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preDisableTable(tableName);

          LOG.info(getClientIdAuditPrefix() + " disable " + tableName);

          // Execute the operation asynchronously - client will check the progress of the operation
          // In case the request is from a <1.1 client before returning,
          // we want to make sure that the table is prepared to be
          // enabled (the table is locked and the table state is set).
          // Note: if the procedure throws exception, we will catch it and rethrow.
          //
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createBlockingLatch();
          submitProcedure(new DisableTableProcedure(procedureExecutor.getEnvironment(), tableName,
            false, prepareLatch));
          prepareLatch.await();

          getMaster().getMasterCoprocessorHost().postDisableTable(tableName);
        }

        @Override
        protected String getDescription() {
          return "DisableTableProcedure";
        }
      });
  }

  private long modifyTable(final TableName tableName,
    final TableDescriptorGetter newDescriptorGetter, final long nonceGroup, final long nonce,
    final boolean shouldCheckDescriptor) throws IOException {
    return modifyTable(tableName, newDescriptorGetter, nonceGroup, nonce, shouldCheckDescriptor,
      true);
  }

  private long modifyTable(final TableName tableName,
    final TableDescriptorGetter newDescriptorGetter, final long nonceGroup, final long nonce,
    final boolean shouldCheckDescriptor, final boolean reopenRegions) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("641539c9-d58b-378e-b319-b710827096d0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7f57fe56-2af4-3498-b892-f9355b51ee44"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b709336b-d66b-3bcf-a941-edf5343fc7e9"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("21e41f84-ac18-30de-8f0d-9e987c81daa0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("34cf0361-ecd8-30ec-bad7-e7e37392ca69"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3abfa8a4-b804-3d34-adaf-41df7bdf5498"))) {
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
    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("dad62271-5e88-30b9-929b-4665c73b9731"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2835b491-312b-3bde-8d6f-6acf094adb11"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2fbd8c32-e019-348d-913f-0837669fcedc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2e21198b-66f3-3b2f-a2b0-680f3f26808d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f7fce03-043c-3511-bb74-9bc126e78380"))) {
throw new java.io.IOException("Injected exception");
}
          TableDescriptor oldDescriptor = getMaster().getTableDescriptors().get(tableName);
          TableDescriptor newDescriptor = getMaster().getMasterCoprocessorHost()
            .preModifyTable(tableName, oldDescriptor, newDescriptorGetter.get());
          TableDescriptorChecker.sanityCheck(conf, newDescriptor);
          LOG.info("{} modify table {} from {} to {}", getClientIdAuditPrefix(), tableName,
            oldDescriptor, newDescriptor);

          // Execute the operation synchronously - wait for the operation completes before
          // continuing.
          //
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
          submitProcedure(new ModifyTableProcedure(procedureExecutor.getEnvironment(),
            newDescriptor, latch, oldDescriptor, shouldCheckDescriptor, reopenRegions));
          latch.await();

          getMaster().getMasterCoprocessorHost().postModifyTable(tableName, oldDescriptor,
            newDescriptor);
        }

        @Override
        protected String getDescription() {
          return "ModifyTableProcedure";
        }
      });

  }

  @Override
  public long modifyTable(final TableName tableName, final TableDescriptor newDescriptor,
    final long nonceGroup, final long nonce, final boolean reopenRegions) throws IOException {
    checkInitialized();
    return modifyTable(tableName, new TableDescriptorGetter() {
      @Override
      public TableDescriptor get() throws IOException {
        return newDescriptor;
      }
    }, nonceGroup, nonce, false, reopenRegions);

  }

  @Override
  public long modifyTableStoreFileTracker(TableName tableName, String dstSFT, long nonceGroup,
    long nonce) throws IOException {
    checkInitialized();
    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {

        @Override
        protected void run() throws IOException {
          String sft = getMaster().getMasterCoprocessorHost()
            .preModifyTableStoreFileTracker(tableName, dstSFT);
          LOG.info("{} modify table store file tracker of table {} to {}", getClientIdAuditPrefix(),
            tableName, sft);
          submitProcedure(new ModifyTableStoreFileTrackerProcedure(
            procedureExecutor.getEnvironment(), tableName, sft));
          getMaster().getMasterCoprocessorHost().postModifyTableStoreFileTracker(tableName, sft);
        }

        @Override
        protected String getDescription() {
          return "ModifyTableStoreFileTrackerProcedure";
        }
      });
  }

  public long restoreSnapshot(final SnapshotDescription snapshotDesc, final long nonceGroup,
    final long nonce, final boolean restoreAcl, final String customSFT) throws IOException {
    checkInitialized();
    getSnapshotManager().checkSnapshotSupport();

    // Ensure namespace exists. Will throw exception if non-known NS.
    final TableName dstTable = TableName.valueOf(snapshotDesc.getTable());
if(KnobRuntime.check(java.util.UUID.fromString("bfe2e2f3-607d-3f40-93b2-5176f2a93b86"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("461065f7-960d-334d-91d0-e81de4ad10a5"))) { getNamespace(getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("63da9c21-0eea-33fd-8ff7-4fac8a1d3f9f"))) { getClusterSchema().getNamespace(getClientIdAuditPrefix()); } else if (KnobRuntime.check(java.util.UUID.fromString("a0ff614f-8a15-301c-9866-edce029fee7b"))) { getNamespace(getClientIdAuditPrefix()); } else if (KnobRuntime.check(java.util.UUID.fromString("1d4ff4ac-d5d7-3771-93f3-0498a90236ca"))) { getClusterSchema().getNamespace(getProcessName()); } else if (KnobRuntime.check(java.util.UUID.fromString("c5f1d656-b7ed-3ad2-bd43-39aec88c2a73"))) { getNamespace(getProcessName()); } else if (KnobRuntime.check(java.util.UUID.fromString("698c9d9f-54e8-3790-911a-bbc93f7d5ff0"))) { getClusterSchema().getNamespace(getName()); } else { getClusterSchema().getNamespace(dstTable.getNamespaceAsString()); }

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9d16f52d-1cda-3d53-a404-77c55d4f9325"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c96066f3-32e5-3551-b0e2-d9ef0c2c3aaa"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75f265c1-c971-3d34-8a1a-b6b167ac96ad"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7c86f3d-16dd-3235-a73c-0e7ee424021e"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e4a2c1f-a968-3eb2-833e-a1c5e0828d0b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8554e503-740b-3b91-a1e5-ae8dabcf6513"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec08f0f8-a0fb-3736-b603-ae320d455caa"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("670d66ff-aa31-3309-875c-7cfd6ad0fed5"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1af67049-ed75-32b4-b220-d38d68b89663"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7fee0fee-6a01-3679-bf4f-8ef19cd4ff3d"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ceae10c5-688b-39fa-bd22-024fe8bb7522"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a761c96-3df8-392c-a0ac-9f8551f3854c"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0dd37990-1c5e-36b5-8cd7-6bf1599658ab"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93c528fb-5c68-36d8-884f-268353d39251"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4af4d9ab-b19a-3885-b0b9-799b7317f10f"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68604428-ffa1-36c4-a3ba-2ac2bced38d0"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ee0a6d71-c0d4-3745-ab7f-210e99dcdf7d"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          setProcId(getSnapshotManager().restoreOrCloneSnapshot(snapshotDesc, getNonceKey(),
            restoreAcl, customSFT));
        }

        @Override
        protected String getDescription() {
          return "RestoreSnapshotProcedure";
        }
      });
  }

  private void checkTableExists(final TableName tableName)
    throws IOException, TableNotFoundException {
    if (!tableDescriptors.exists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
  }

  @Override
  public void checkTableModifiable(final TableName tableName)
    throws IOException, TableNotFoundException, TableNotDisabledException {
    if (isCatalogTable(tableName)) {
      throw new IOException("Can't modify catalog tables");
    }
if(KnobRuntime.check(java.util.UUID.fromString("8771cda8-8662-3fc2-8cf4-3f2efd20d7b0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5472f9df-fbc8-3883-a272-d9a96759d1ab"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bf0fcc4e-d62a-3780-86f8-6a4b8d01346d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ecccb9bd-4529-384b-a468-87a1dd17eaf3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e91f2ab5-958a-3898-8b4d-446efd3ddf54"))) {
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
    checkTableExists(tableName);
if(KnobRuntime.check(java.util.UUID.fromString("60e61a28-c928-334c-91b7-9d19e9beb6a0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3eba8461-4f1e-352c-b8f2-2e6d0819b279"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ef35dd43-22df-36be-bacb-a304ac44167e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f481a067-f2db-315a-acb1-4d9d3d7382cd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("452c1a3e-55b3-3799-b0c1-43b930c737ca"))) {
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
    TableState ts = getTableStateManager().getTableState(tableName);
    if (!ts.isDisabled()) {
      throw new TableNotDisabledException("Not DISABLED; " + ts);
    }
  }

  public ClusterMetrics getClusterMetricsWithoutCoprocessor() throws InterruptedIOException {
    return getClusterMetricsWithoutCoprocessor(EnumSet.allOf(Option.class));
  }

  public ClusterMetrics getClusterMetricsWithoutCoprocessor(EnumSet<Option> options)
    throws InterruptedIOException {
    ClusterMetricsBuilder builder = ClusterMetricsBuilder.newBuilder();
    // given that hbase1 can't submit the request with Option,
    // we return all information to client if the list of Option is empty.
    if (((KnobRuntime.check(java.util.UUID.fromString("a0e1f1e9-35a9-386d-a7be-01796fb15721"))) ? (isAborted()) : (((KnobRuntime.check(java.util.UUID.fromString("0015fa34-2ca0-325a-9cea-18506ec108c7"))) ? (isStopped()) : (options.isEmpty()))))) {
      options = EnumSet.allOf(Option.class);
    }

    // TASKS and/or LIVE_SERVERS will populate this map, which will be given to the builder if
    // not null after option processing completes.
    Map<ServerName, ServerMetrics> serverMetricsMap = null;

    for (Option opt : options) {
      switch (opt) {
        case HBASE_VERSION:
          builder.setHBaseVersion(VersionInfo.getVersion());
          break;
        case CLUSTER_ID:
          builder.setClusterId(getClusterId());
          break;
        case MASTER:
          builder.setMasterName(getServerName());
          break;
        case BACKUP_MASTERS:
          builder.setBackerMasterNames(getBackupMasters());
          break;
        case TASKS: {
          // Master tasks
          builder.setMasterTasks(TaskMonitor.get().getTasks().stream()
            .map(task -> ServerTaskBuilder.newBuilder().setDescription(task.getDescription())
              .setStatus(task.getStatus())
              .setState(ServerTask.State.valueOf(task.getState().name()))
              .setStartTime(task.getStartTime()).setCompletionTime(task.getCompletionTimestamp())
              .build())
            .collect(Collectors.toList()));
          // TASKS is also synonymous with LIVE_SERVERS for now because task information for
          // regionservers is carried in ServerLoad.
          // Add entries to serverMetricsMap for all live servers, if we haven't already done so
          if (serverMetricsMap == null) {
            serverMetricsMap = getOnlineServers();
          }
          break;
        }
        case LIVE_SERVERS: {
          // Add entries to serverMetricsMap for all live servers, if we haven't already done so
          if (serverMetricsMap == null) {
            serverMetricsMap = getOnlineServers();
          }
          break;
        }
        case DEAD_SERVERS: {
          if (serverManager != null) {
            builder.setDeadServerNames(
              new ArrayList<>(serverManager.getDeadServers().copyServerNames()));
          }
          break;
        }
        case UNKNOWN_SERVERS: {
          if (serverManager != null) {
            builder.setUnknownServerNames(getUnknownServers());
          }
          break;
        }
        case MASTER_COPROCESSORS: {
          if (((KnobRuntime.check(java.util.UUID.fromString("4ce893f8-a515-3abc-9cb1-d0229ca6ad2a"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("eae21b3d-010b-30fd-981e-3502716321fc"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
            builder.setMasterCoprocessorNames(Arrays.asList(getMasterCoprocessors()));
          }
          break;
        }
        case REGIONS_IN_TRANSITION: {
          if (assignmentManager != null) {
            builder.setRegionsInTransition(
              assignmentManager.getRegionStates().getRegionsStateInTransition());
          }
          break;
        }
        case BALANCER_ON: {
          if (loadBalancerStateStore != null) {
            builder.setBalancerOn(loadBalancerStateStore.get());
          }
          break;
        }
        case MASTER_INFO_PORT: {
          if (((KnobRuntime.check(java.util.UUID.fromString("6a4401c5-5a30-3350-8a8c-562d2925417d"))) ? ((infoServer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a8de8473-65cc-3e42-8653-b33b5734028d"))) ? ((infoServer) != (null)) : (infoServer != null))))) {
            builder.setMasterInfoPort(infoServer.getPort());
          }
          break;
        }
        case SERVERS_NAME: {
          if (serverManager != null) {
            builder.setServerNames(serverManager.getOnlineServersList());
          }
          break;
        }
        case TABLE_TO_REGIONS_COUNT: {
          if (isActiveMaster() && isInitialized() && assignmentManager != null) {
            try {
              Map<TableName, RegionStatesCount> tableRegionStatesCountMap = new HashMap<>();
              Map<String, TableDescriptor> tableDescriptorMap = getTableDescriptors().getAll();
              for (TableDescriptor tableDescriptor : tableDescriptorMap.values()) {
                TableName tableName = tableDescriptor.getTableName();
                RegionStatesCount regionStatesCount =
                  assignmentManager.getRegionStatesCount(tableName);
                tableRegionStatesCountMap.put(tableName, regionStatesCount);
              }
              builder.setTableRegionStatesCount(tableRegionStatesCountMap);
            } catch (IOException e) {
              LOG.error("Error while populating TABLE_TO_REGIONS_COUNT for Cluster Metrics..", e);
            }
          }
          break;
        }
      }
    }

    if (serverMetricsMap != null) {
      builder.setLiveServerMetrics(serverMetricsMap);
    }

    return ((KnobRuntime.check(java.util.UUID.fromString("abd2e2fc-95e1-39b1-b819-bae323d88e2b"))) ? (getClusterMetricsWithoutCoprocessor()) : (builder.build()));
  }

  private List<ServerName> getUnknownServers() {
    if (serverManager != null) {
      final Set<ServerName> serverNames = getAssignmentManager().getRegionStates().getRegionStates()
        .stream().map(RegionState::getServerName).collect(Collectors.toSet());
      final List<ServerName> unknownServerNames = serverNames.stream()
        .filter(sn -> sn != null && serverManager.isServerUnknown(sn)).collect(Collectors.toList());
      return unknownServerNames;
    }
    return null;
  }

  private Map<ServerName, ServerMetrics> getOnlineServers() {
if(KnobRuntime.check(java.util.UUID.fromString("5ec69c44-f302-33aa-b575-72a9738467ae"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("47d8aaaa-b37d-3a14-8a82-b435ab4604a9"))) ? ((serverManager) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("52ff97cc-d87b-387d-9a58-0907d13ea6fb"))) ? ((serverManager) != (null)) : (serverManager != null))))) {
      final Map<ServerName, ServerMetrics> map = new HashMap<>();
      serverManager.getOnlineServers().entrySet().forEach(e -> map.put(e.getKey(), e.getValue()));
      return map;
    }
    return null;
  }

  /** Returns cluster status */
  public ClusterMetrics getClusterMetrics() throws IOException {
    return getClusterMetrics(EnumSet.allOf(Option.class));
  }

  public ClusterMetrics getClusterMetrics(EnumSet<Option> options) throws IOException {
    if (cpHost != null) {
      cpHost.preGetClusterMetrics();
    }
    ClusterMetrics status = getClusterMetricsWithoutCoprocessor(options);
    if (cpHost != null) {
      cpHost.postGetClusterMetrics(status);
    }
    return status;
  }

  @Override
  public Optional<ServerName> getActiveMaster() {
if(KnobRuntime.check(java.util.UUID.fromString("d5911bfa-3003-3698-867f-75861462fe31"))) {
return null;
}
    return activeMasterManager.getActiveMasterServerName();
  }

  @Override
  public List<ServerName> getBackupMasters() {
    return activeMasterManager.getBackupMasters();
  }

  /** Returns info port of active master or 0 if any exception occurs. */
  public int getActiveMasterInfoPort() {
    return activeMasterManager.getActiveMasterInfoPort();
  }

  /**
   * @param sn is ServerName of the backup master
   * @return info port of backup master or 0 if any exception occurs.
   */
  public int getBackupMasterInfoPort(final ServerName sn) {
    return activeMasterManager.getBackupMasterInfoPort(sn);
  }

  @Override
  public Iterator<ServerName> getBootstrapNodes() {
    return regionServerTracker.getRegionServers().iterator();
  }

  /**
   * The set of loaded coprocessors is stored in a static set. Since it's statically allocated, it
   * does not require that HMaster's cpHost be initialized prior to accessing it.
   * @return a String representation of the set of names of the loaded coprocessors.
   */
  public static String getLoadedCoprocessors() {
if(KnobRuntime.check(java.util.UUID.fromString("d7bafc2e-5d63-3031-a499-01f6beff382b"))) {
return null;
}
    return CoprocessorHost.getLoadedCoprocessors().toString();
  }

  /** Returns timestamp in millis when HMaster was started. */
  public long getMasterStartTime() {
    return startcode;
  }

  /** Returns timestamp in millis when HMaster became the active master. */
  @Override
  public long getMasterActiveTime() {
    return masterActiveTime;
  }

  /** Returns timestamp in millis when HMaster finished becoming the active master */
  public long getMasterFinishedInitializationTime() {
if(KnobRuntime.check(java.util.UUID.fromString("1e517f09-6635-32c7-97e4-ad14ac2d3c20"))) {
return 0;
}
    return masterFinishedInitializationTime;
  }

  public int getNumWALFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("5bdf0d13-f170-3924-b511-ffc45143b871"))) {
return 0;
}
    return 0;
  }

  public ProcedureStore getProcedureStore() {
    return procedureStore;
  }

  public int getRegionServerInfoPort(final ServerName sn) {
    int port = this.serverManager.getInfoPort(sn);
    return port == 0
      ? conf.getInt(HConstants.REGIONSERVER_INFO_PORT, HConstants.DEFAULT_REGIONSERVER_INFOPORT)
      : port;
  }

  @Override
  public String getRegionServerVersion(ServerName sn) {
if(KnobRuntime.check(java.util.UUID.fromString("de701a32-7539-3cc1-831d-ac4492005c5e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2b395671-026f-37cd-909f-cd6cd18a8f39"))) {
return null;
}
    // Will return "0.0.0" if the server is not online to prevent move system region to unknown
    // version RS.
if(KnobRuntime.check(java.util.UUID.fromString("83d61c09-7187-3390-995f-30f9be0e5e62"))) {
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
    return this.serverManager.getVersion(sn);
  }

  @Override
  public void checkIfShouldMoveSystemRegionAsync() {
    assignmentManager.checkIfShouldMoveSystemRegionAsync();
  }

  /** Returns array of coprocessor SimpleNames. */
  public String[] getMasterCoprocessors() {
if(KnobRuntime.check(java.util.UUID.fromString("cfb3d50c-f1c4-3ea0-8f43-85abf9aa5e30"))) {
return null;
}
    Set<String> masterCoprocessors = getMasterCoprocessorHost().getCoprocessors();
    return masterCoprocessors.toArray(new String[masterCoprocessors.size()]);
  }

  @Override
  public void abort(String reason, Throwable cause) {
    if (!setAbortRequested() || isStopped()) {
      LOG.debug("Abort called but aborted={}, stopped={}", isAborted(), isStopped());
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("1f2b6fba-f58e-33c7-a9d5-bf94c1e9f3cc"))) ? ((cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d48811f6-f8e4-3d04-bd4a-a0b25a955e74"))) ? ((cpHost) != (null)) : (cpHost != null))))) {
      // HBASE-4014: dump a list of loaded coprocessors.
      LOG.error(HBaseMarkers.FATAL,
        "Master server abort: loaded coprocessors are: " + getLoadedCoprocessors());
    }
    String msg = "***** ABORTING master " + this + ": " + reason + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }

    try {
if(KnobRuntime.check(java.util.UUID.fromString("2bea8aae-a5f0-3bcf-852a-6460ef37c1ff"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("16ebf0b3-4ef5-3706-9979-44813573239c"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("b145fff4-2f54-3ee2-bf14-a875d420b71a"))) { initializeMemStoreChunkCreator(); } else { stopMaster(); }
    } catch (IOException e) {
      LOG.error("Exception occurred while stopping master", e);
    }
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public MasterCoprocessorHost getMasterCoprocessorHost() {
    return cpHost;
  }

  @Override
  public MasterQuotaManager getMasterQuotaManager() {
    return quotaManager;
  }

  @Override
  public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return procedureExecutor;
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return this.assignmentManager;
  }

  @Override
  public CatalogJanitor getCatalogJanitor() {
    return this.catalogJanitorChore;
  }

  public MemoryBoundedLogMessageBuffer getRegionServerFatalLogBuffer() {
    return rsFatals;
  }

  public TaskGroup getStartupProgress() {
    return startupTaskGroup;
  }

  /**
   * Shutdown the cluster. Master runs a coordinated stop of all RegionServers and then itself.
   */
  public void shutdown() throws IOException {
    TraceUtil.trace(() -> {
      if (cpHost != null) {
        cpHost.preShutdown();
      }

      // Tell the servermanager cluster shutdown has been called. This makes it so when Master is
      // last running server, it'll stop itself. Next, we broadcast the cluster shutdown by setting
      // the cluster status as down. RegionServers will notice this change in state and will start
      // shutting themselves down. When last has exited, Master can go down.
      if (this.serverManager != null) {
        this.serverManager.shutdownCluster();
      }
      if (this.clusterStatusTracker != null) {
        try {
          this.clusterStatusTracker.setClusterDown();
        } catch (KeeperException e) {
          LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
        }
      }
      // Stop the procedure executor. Will stop any ongoing assign, unassign, server crash etc.,
      // processing so we can go down.
      if (this.procedureExecutor != null) {
        this.procedureExecutor.stop();
      }
      // Shutdown our cluster connection. This will kill any hosted RPCs that might be going on;
      // this is what we want especially if the Master is in startup phase doing call outs to
      // hbase:meta, etc. when cluster is down. Without ths connection close, we'd have to wait on
      // the rpc to timeout.
      if (this.clusterConnection != null) {
        this.clusterConnection.close();
      }
    }, "HMaster.shutdown");
  }

  public void stopMaster() throws IOException {
    if (cpHost != null) {
      cpHost.preStopMaster();
    }
    stop("Stopped by " + Thread.currentThread().getName());
  }

  @Override
  public void stop(String msg) {
    if (!isStopped()) {
      super.stop(msg);
      if (this.activeMasterManager != null) {
        this.activeMasterManager.stop();
      }
    }
  }

  @InterfaceAudience.Private
  protected void checkServiceStarted() throws ServerNotRunningYetException {
    if (!serviceStarted) {
      throw new ServerNotRunningYetException("Server is not running yet");
    }
  }

  void checkInitialized() throws PleaseHoldException, ServerNotRunningYetException,
    MasterNotRunningException, MasterStoppedException {
if(KnobRuntime.check(java.util.UUID.fromString("5e51fa09-8656-38f0-84e5-daed2fb658f5"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("559896a4-2995-3ddf-93fc-b89f23668e71"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("edfc4242-71ea-36a7-8ee0-7ff6a8d7c279"))) { registerConfigurationObservers(); } else { checkServiceStarted(); }
    if (!isInitialized()) {
      throw new PleaseHoldException("Master is initializing");
    }
    if (isStopped()) {
      throw new MasterStoppedException();
    }
  }

  /**
   * Report whether this master is currently the active master or not. If not active master, we are
   * parked on ZK waiting to become active. This method is used for testing.
   * @return true if active master, false if not.
   */
  @Override
  public boolean isActiveMaster() {
    return activeMaster;
  }

  /**
   * Report whether this master has completed with its initialization and is ready. If ready, the
   * master is also the active master. A standby master is never ready. This method is used for
   * testing.
   * @return true if master is ready to go, false if not.
   */
  @Override
  public boolean isInitialized() {
    return initialized.isReady();
  }

  /**
   * Report whether this master is started This method is used for testing.
   * @return true if master is ready to go, false if not.
   */

  @Override
  public boolean isOnline() {
if(KnobRuntime.check(java.util.UUID.fromString("ba94a83d-2433-30e8-b7dc-c49c2adfc876"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("477a47ca-40fd-3a7f-b862-c68ab20fedde"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("a606b72f-e94b-3a9a-beac-b42fe44f503f"))) ? (!serviceStarted) : (serviceStarted));
  }

  /**
   * Report whether this master is in maintenance mode.
   * @return true if master is in maintenanceMode
   */
  @Override
  public boolean isInMaintenanceMode() {
    return maintenanceMode;
  }

  @InterfaceAudience.Private
  public void setInitialized(boolean isInitialized) {
    procedureExecutor.getEnvironment().setEventReady(initialized, isInitialized);
  }

  /**
   * Mainly used in procedure related tests, where we will restart ProcedureExecutor and
   * AssignmentManager, but we do not want to restart master(to speed up the test), so we need to
   * disable rpc for a while otherwise some critical rpc requests such as
   * reportRegionStateTransition could fail and cause region server to abort.
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public void setServiceStarted(boolean started) {
if(KnobRuntime.check(java.util.UUID.fromString("2fdc6036-1c02-3e0e-a937-e1f011736297"))) {
return;
}
    this.serviceStarted = started;
  }

  @Override
  public ProcedureEvent<?> getInitializedEvent() {
    return initialized;
  }

  /**
   * Compute the average load across all region servers. Currently, this uses a very naive
   * computation - just uses the number of regions being served, ignoring stats about number of
   * requests.
   * @return the average load
   */
  public double getAverageLoad() {
if(KnobRuntime.check(java.util.UUID.fromString("7de39a1b-0aa9-3d5a-8a12-8e83dc01edef"))) {
return 0.0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("4f7f9462-1796-3d74-95c9-3689b7c42b0c"))) ? ((this.assignmentManager) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2a161ad5-a875-3cc9-b421-92cc1d0bcc91"))) ? ((this.assignmentManager) != (null)) : (this.assignmentManager == null))))) {
      return 0;
    }

    RegionStates regionStates = this.assignmentManager.getRegionStates();
    if (((KnobRuntime.check(java.util.UUID.fromString("a9a5021b-ce10-3220-9da1-a313c8b84130"))) ? ((regionStates) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3765702-c0ea-381b-bf6c-40bb17a4abed"))) ? ((regionStates) != (null)) : (regionStates == null))))) {
      return 0;
    }
    return regionStates.getAverageLoad();
  }

  @Override
  public boolean registerService(Service instance) {
if(KnobRuntime.check(java.util.UUID.fromString("9b18fdba-dee0-3696-92b9-236faa8b621d"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c7d9eb0a-950e-3731-b64e-221dd1a01cc3"))) {
return false;
}
    /*
     * No stacking of instances is allowed for a single service name
     */
    Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor service " + serviceName
        + " already registered, rejecting request from " + instance);
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered master coprocessor service: service=" + serviceName);
    }
    return true;
  }

  /**
   * Utility for constructing an instance of the passed HMaster class.
   * @return HMaster instance.
   */
  public static HMaster constructMaster(Class<? extends HMaster> masterClass,
    final Configuration conf) {
    try {
      Constructor<? extends HMaster> c = masterClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      Throwable error = e;
      if (
        e instanceof InvocationTargetException
          && ((InvocationTargetException) e).getTargetException() != null
      ) {
        error = ((InvocationTargetException) e).getTargetException();
      }
      throw new RuntimeException("Failed construction of Master: " + masterClass.toString() + ". ",
        error);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.master.HMasterCommandLine
   */
  public static void main(String[] args) {
    LOG.info("STARTING service " + HMaster.class.getSimpleName());
    VersionInfo.logVersion();
    new HMasterCommandLine(HMaster.class).doMain(args);
  }

  public HFileCleaner getHFileCleaner() {
    return this.hfileCleaners.get(0);
  }

  public List<HFileCleaner> getHFileCleaners() {
    return this.hfileCleaners;
  }

  public LogCleaner getLogCleaner() {
    return this.logCleaner;
  }

  /** Returns the underlying snapshot manager */
  @Override
  public SnapshotManager getSnapshotManager() {
    return this.snapshotManager;
  }

  /** Returns the underlying MasterProcedureManagerHost */
  @Override
  public MasterProcedureManagerHost getMasterProcedureManagerHost() {
    return mpmHost;
  }

  @Override
  public ClusterSchema getClusterSchema() {
    return this.clusterSchemaService;
  }

  /**
   * Create a new Namespace.
   * @param namespaceDescriptor descriptor for new Namespace
   * @param nonceGroup          Identifier for the source of the request, a client or process.
   * @param nonce               A unique identifier for this operation from the client or process
   *                            identified by <code>nonceGroup</code> (the source must ensure each
   *                            operation gets a unique id).
   * @return procedure id
   */
  long createNamespace(final NamespaceDescriptor namespaceDescriptor, final long nonceGroup,
    final long nonce) throws IOException {
    checkInitialized();

    TableName.isLegalNamespaceName(Bytes.toBytes(namespaceDescriptor.getName()));

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preCreateNamespace(namespaceDescriptor);
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
          LOG.info(getClientIdAuditPrefix() + " creating " + namespaceDescriptor);
          // Execute the operation synchronously - wait for the operation to complete before
          // continuing.
          setProcId(getClusterSchema().createNamespace(namespaceDescriptor, getNonceKey(), latch));
          latch.await();
          getMaster().getMasterCoprocessorHost().postCreateNamespace(namespaceDescriptor);
        }

        @Override
        protected String getDescription() {
          return "CreateNamespaceProcedure";
        }
      });
  }

  /**
   * Modify an existing Namespace.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce      A unique identifier for this operation from the client or process identified
   *                   by <code>nonceGroup</code> (the source must ensure each operation gets a
   *                   unique id).
   * @return procedure id
   */
  long modifyNamespace(final NamespaceDescriptor newNsDescriptor, final long nonceGroup,
    final long nonce) throws IOException {
    checkInitialized();

    TableName.isLegalNamespaceName(Bytes.toBytes(newNsDescriptor.getName()));

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          NamespaceDescriptor oldNsDescriptor = getNamespace(newNsDescriptor.getName());
          getMaster().getMasterCoprocessorHost().preModifyNamespace(oldNsDescriptor,
            newNsDescriptor);
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
          LOG.info(getClientIdAuditPrefix() + " modify " + newNsDescriptor);
          // Execute the operation synchronously - wait for the operation to complete before
          // continuing.
          setProcId(getClusterSchema().modifyNamespace(newNsDescriptor, getNonceKey(), latch));
          latch.await();
if(KnobRuntime.check(java.util.UUID.fromString("e348a49f-7d0b-3150-965f-d91d3049b38f"))) {
throw new java.io.IOException("Injected exception");
}
          getMaster().getMasterCoprocessorHost().postModifyNamespace(oldNsDescriptor,
            newNsDescriptor);
        }

        @Override
        protected String getDescription() {
          return "ModifyNamespaceProcedure";
        }
      });
  }

  /**
   * Delete an existing Namespace. Only empty Namespaces (no tables) can be removed.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce      A unique identifier for this operation from the client or process identified
   *                   by <code>nonceGroup</code> (the source must ensure each operation gets a
   *                   unique id).
   * @return procedure id
   */
  long deleteNamespace(final String name, final long nonceGroup, final long nonce)
    throws IOException {
    checkInitialized();

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preDeleteNamespace(name);
          LOG.info(getClientIdAuditPrefix() + " delete " + name);
          // Execute the operation synchronously - wait for the operation to complete before
          // continuing.
          //
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
          setProcId(submitProcedure(
            new DeleteNamespaceProcedure(procedureExecutor.getEnvironment(), name, latch)));
          latch.await();
          // Will not be invoked in the face of Exception thrown by the Procedure's execution
if(KnobRuntime.check(java.util.UUID.fromString("ed827189-da4a-32e4-aa78-0513b81d6cfa"))) {
throw new java.io.IOException("Injected exception");
}
          getMaster().getMasterCoprocessorHost().postDeleteNamespace(name);
        }

        @Override
        protected String getDescription() {
          return "DeleteNamespaceProcedure";
        }
      });
  }

  /**
   * Get a Namespace
   * @param name Name of the Namespace
   * @return Namespace descriptor for <code>name</code>
   */
  NamespaceDescriptor getNamespace(String name) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("036fca30-4b78-3d37-a8e2-69c749699af8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("0abc6eb6-3a4b-3341-99c5-87a5e5815ec1"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("f32dd950-d879-304d-ba3f-aff4ebd34357"))) { registerConfigurationObservers(); } else if (KnobRuntime.check(java.util.UUID.fromString("8b6861fa-b7bc-3af5-8552-85649237e970"))) { initializeMemStoreChunkCreator(); } else { checkInitialized(); }
if(KnobRuntime.check(java.util.UUID.fromString("7e1dfe5b-13b5-3e14-97e2-0412782d7c2c"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("c7af42dd-2829-3050-82e8-ce68f19ec788"))) ? ((this.cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e6867a30-0e49-39e9-bdf8-7aed1d41951a"))) ? ((this.cpHost) == (null)) : (this.cpHost != null))))) this.cpHost.preGetNamespaceDescriptor(name);
if(KnobRuntime.check(java.util.UUID.fromString("75dbd952-d979-34c6-9851-046acabf8c2b"))) {
throw new java.io.IOException("Injected exception");
}
    NamespaceDescriptor nsd = this.clusterSchemaService.getNamespace(name);
    if (this.cpHost != null) this.cpHost.postGetNamespaceDescriptor(nsd);
    return nsd;
  }

  /**
   * Get all Namespaces
   * @return All Namespace descriptors
   */
  List<NamespaceDescriptor> getNamespaces() throws IOException {
    checkInitialized();
    final List<NamespaceDescriptor> nsds = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preListNamespaceDescriptors(nsds);
    }
    nsds.addAll(this.clusterSchemaService.getNamespaces());
    if (this.cpHost != null) {
      this.cpHost.postListNamespaceDescriptors(nsds);
    }
    return nsds;
  }

  /**
   * List namespace names
   * @return All namespace names
   */
  public List<String> listNamespaces() throws IOException {
    checkInitialized();
    List<String> namespaces = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preListNamespaces(namespaces);
    }
    for (NamespaceDescriptor namespace : clusterSchemaService.getNamespaces()) {
      namespaces.add(namespace.getName());
    }
    if (cpHost != null) {
      cpHost.postListNamespaces(namespaces);
    }
    return namespaces;
  }

  @Override
  public List<TableName> listTableNamesByNamespace(String name) throws IOException {
    checkInitialized();
    return listTableNames(name, null, true);
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
    checkInitialized();
    return listTableDescriptors(name, null, null, true);
  }

  @Override
  public boolean abortProcedure(final long procId, final boolean mayInterruptIfRunning)
    throws IOException {
    if (cpHost != null) {
      cpHost.preAbortProcedure(this.procedureExecutor, procId);
    }

    final boolean result = this.procedureExecutor.abort(procId, mayInterruptIfRunning);

    if (cpHost != null) {
      cpHost.postAbortProcedure();
    }

    return result;
  }

  @Override
  public List<Procedure<?>> getProcedures() throws IOException {
    if (cpHost != null) {
      cpHost.preGetProcedures();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    List<Procedure<?>> procList = (List) this.procedureExecutor.getProcedures();

    if (cpHost != null) {
      cpHost.postGetProcedures(procList);
    }

    return procList;
  }

  @Override
  public List<LockedResource> getLocks() throws IOException {
    if (cpHost != null) {
      cpHost.preGetLocks();
    }

    MasterProcedureScheduler procedureScheduler =
      procedureExecutor.getEnvironment().getProcedureScheduler();

    final List<LockedResource> lockedResources = procedureScheduler.getLocks();

    if (cpHost != null) {
      cpHost.postGetLocks(lockedResources);
    }

    return lockedResources;
  }

  /**
   * Returns the list of table descriptors that match the specified request
   * @param namespace        the namespace to query, or null if querying for all
   * @param regex            The regular expression to match against, or null if querying for all
   * @param tableNameList    the list of table names, or null if querying for all
   * @param includeSysTables False to match only against userspace tables
   * @return the list of table descriptors
   */
  public List<TableDescriptor> listTableDescriptors(final String namespace, final String regex,
    final List<TableName> tableNameList, final boolean includeSysTables) throws IOException {
    List<TableDescriptor> htds = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preGetTableDescriptors(tableNameList, htds, regex);
    }
    htds = getTableDescriptors(htds, namespace, regex, tableNameList, includeSysTables);
    if (cpHost != null) {
      cpHost.postGetTableDescriptors(tableNameList, htds, regex);
    }
    return htds;
  }

  /**
   * Returns the list of table names that match the specified request
   * @param regex            The regular expression to match against, or null if querying for all
   * @param namespace        the namespace to query, or null if querying for all
   * @param includeSysTables False to match only against userspace tables
   * @return the list of table names
   */
  public List<TableName> listTableNames(final String namespace, final String regex,
    final boolean includeSysTables) throws IOException {
    List<TableDescriptor> htds = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preGetTableNames(htds, regex);
    }
    htds = getTableDescriptors(htds, namespace, regex, null, includeSysTables);
    if (cpHost != null) {
      cpHost.postGetTableNames(htds, regex);
    }
    List<TableName> result = new ArrayList<>(htds.size());
    for (TableDescriptor htd : htds)
      result.add(htd.getTableName());
    return result;
  }

  /**
   * Return a list of table table descriptors after applying any provided filter parameters. Note
   * that the user-facing description of this filter logic is presented on the class-level javadoc
   * of {@link NormalizeTableFilterParams}.
   */
  private List<TableDescriptor> getTableDescriptors(final List<TableDescriptor> htds,
    final String namespace, final String regex, final List<TableName> tableNameList,
    final boolean includeSysTables) throws IOException {
    if (tableNameList == null || tableNameList.isEmpty()) {
      // request for all TableDescriptors
      Collection<TableDescriptor> allHtds;
      if (namespace != null && namespace.length() > 0) {
        // Do a check on the namespace existence. Will fail if does not exist.
        this.clusterSchemaService.getNamespace(namespace);
        allHtds = tableDescriptors.getByNamespace(namespace).values();
      } else {
        allHtds = tableDescriptors.getAll().values();
      }
      for (TableDescriptor desc : allHtds) {
        if (
          tableStateManager.isTablePresent(desc.getTableName())
            && (includeSysTables || !desc.getTableName().isSystemTable())
        ) {
          htds.add(desc);
        }
      }
    } else {
      for (TableName s : tableNameList) {
        if (tableStateManager.isTablePresent(s)) {
          TableDescriptor desc = tableDescriptors.get(s);
if(KnobRuntime.check(java.util.UUID.fromString("8747e09f-609a-3703-9a4e-2c1b596ca4a0"))) {
throw new java.io.IOException("Injected exception");
}
          if (((KnobRuntime.check(java.util.UUID.fromString("aac8ac8f-6356-33a7-86ae-71a8989e2fcd"))) ? ((desc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ee4cfaa5-4b0a-3bbe-a5cb-334f8f7ff2e3"))) ? ((desc) == (null)) : (desc != null))))) {
            htds.add(desc);
          }
        }
      }
    }

    // Retains only those matched by regular expression.
    if (regex != null) filterTablesByRegex(htds, Pattern.compile(regex));
    return htds;
  }

  /**
   * Removes the table descriptors that don't match the pattern.
   * @param descriptors list of table descriptors to filter
   * @param pattern     the regex to use
   */
  private static void filterTablesByRegex(final Collection<TableDescriptor> descriptors,
    final Pattern pattern) {
    final String defaultNS = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
    Iterator<TableDescriptor> itr = descriptors.iterator();
    while (itr.hasNext()) {
      TableDescriptor htd = itr.next();
      String tableName = htd.getTableName().getNameAsString();
      boolean matched = pattern.matcher(tableName).matches();
      if (!matched && htd.getTableName().getNamespaceAsString().equals(defaultNS)) {
        matched = pattern.matcher(defaultNS + TableName.NAMESPACE_DELIM + tableName).matches();
      }
      if (!matched) {
        itr.remove();
      }
    }
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName table) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a4f7bc81-867a-335d-82d0-79bf6b04a85a"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89e5f2bd-4fdb-3f6d-9ecc-8e78999f1374"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67ee9fdb-4ea5-3c1e-90f2-6501df772482"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e14f4d6b-0613-3bc6-aa8b-000798791ef5"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("248cbfd8-389f-35a2-a91c-fb2372592c9c"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7dd3d251-108e-3ed4-b813-9532016ffa29"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7322803-24c0-3c43-bfb4-7e361d5d1d17"))) {
throw new java.io.IOException("Injected exception");
}
    return getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLastMajorCompactionTimestamp(table);
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLastMajorCompactionTimestamp(regionName);
  }

  /**
   * Gets the mob file compaction state for a specific table. Whether all the mob files are selected
   * is known during the compaction execution, but the statistic is done just before compaction
   * starts, it is hard to know the compaction type at that time, so the rough statistics are chosen
   * for the mob file compaction. Only two compaction states are available,
   * CompactionState.MAJOR_AND_MINOR and CompactionState.NONE.
   * @param tableName The current table name.
   * @return If a given table is in mob file compaction now.
   */
  public GetRegionInfoResponse.CompactionState getMobCompactionState(TableName tableName) {
    AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
    if (compactionsCount != null && compactionsCount.get() != 0) {
      return GetRegionInfoResponse.CompactionState.MAJOR_AND_MINOR;
    }
    return GetRegionInfoResponse.CompactionState.NONE;
  }

  public void reportMobCompactionStart(TableName tableName) throws IOException {
    IdLock.Entry lockEntry = null;
    try {
      lockEntry = mobCompactionLock.getLockEntry(tableName.hashCode());
      AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
      if (compactionsCount == null) {
        compactionsCount = new AtomicInteger(0);
        mobCompactionStates.put(tableName, compactionsCount);
      }
      compactionsCount.incrementAndGet();
    } finally {
      if (lockEntry != null) {
        mobCompactionLock.releaseLockEntry(lockEntry);
      }
    }
  }

  public void reportMobCompactionEnd(TableName tableName) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ac943628-8506-3de8-86d1-4c3103284082"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5ed838e8-f8ca-3d2b-949b-da48c778ed61"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a6e7b670-85c5-3ca7-a20d-b1b6368aafc5"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("1b810cde-5864-3bc3-82b7-35c518a9c93d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7881df4a-8b96-384c-b702-6f086fae3b1e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3c7b8276-cfed-3a7d-bd16-e561e2018e61"))) {
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
    IdLock.Entry lockEntry = null;
    try {
      lockEntry = mobCompactionLock.getLockEntry(tableName.hashCode());
if(KnobRuntime.check(java.util.UUID.fromString("5b23659f-4f43-3b15-ac27-4c1a7eeaab75"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("effa86ce-17fb-3c8b-9144-485da96530ac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("718e9c21-9ce9-3b22-802b-dd16fb0c3314"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("98fec03c-cc48-35a6-8c5e-458c38423b7d"))) {
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
      AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
      if (((KnobRuntime.check(java.util.UUID.fromString("193f69b4-822a-33d6-961a-49deda2972ad"))) ? ((compactionsCount) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("caae3f66-22d6-3bdd-a8ed-2d6966c63447"))) ? ((compactionsCount) != (null)) : (compactionsCount != null))))) {
        int count = compactionsCount.decrementAndGet();
        // remove the entry if the count is 0.
        if (((KnobRuntime.check(java.util.UUID.fromString("d1fe2dd5-ce61-3a83-a911-ca787c610650"))) ? ((count) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("310d3e40-d801-31d1-92fc-7d69b63a0a67"))) ? ((count) != (0)) : (count == 0))))) {
          mobCompactionStates.remove(tableName);
        }
      }
    } finally {
      if (((KnobRuntime.check(java.util.UUID.fromString("709fb81a-ebf0-3cca-b3de-e8ba7c9a8541"))) ? ((lockEntry) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a83af613-eb94-3e57-8ad5-c2a235aa6aad"))) ? ((lockEntry) != (null)) : (lockEntry != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("bbe1d18f-8138-3898-af46-7221cb710a79"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5b11628-7391-3561-a299-055d21ada493"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("990b2e2b-5a8a-359e-ba7b-e8644f81fead"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("id");
    field.setAccessible(true);
    long oldValue = ((long)field.get(lockEntry));
    field.set(lockEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efaf30d2-e82a-3703-b0e7-bd8e3446191c"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("acac134b-6ba0-362f-b463-8a1b024efd7b"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("620cea95-3884-32d7-8f98-3c48baeab0a5"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(lockEntry);
    field.set(lockEntry, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        mobCompactionLock.releaseLockEntry(lockEntry);
      }
    }
  }

  /**
   * Queries the state of the {@link LoadBalancerStateStore}. If the balancer is not initialized,
   * false is returned.
   * @return The state of the load balancer, or false if the load balancer isn't defined.
   */
  public boolean isBalancerOn() {
    return !isInMaintenanceMode() && loadBalancerStateStore != null && loadBalancerStateStore.get();
  }

  /**
   * Queries the state of the {@link RegionNormalizerStateStore}. If it's not initialized, false is
   * returned.
   */
  public boolean isNormalizerOn() {
    return !isInMaintenanceMode() && getRegionNormalizerManager().isNormalizerOn();
  }

  /**
   * Queries the state of the {@link SplitOrMergeStateStore}. If it is not initialized, false is
   * returned. If switchType is illegal, false will return.
   * @param switchType see {@link org.apache.hadoop.hbase.client.MasterSwitchType}
   * @return The state of the switch
   */
  @Override
  public boolean isSplitOrMergeEnabled(MasterSwitchType switchType) {
    return !isInMaintenanceMode() && splitOrMergeStateStore != null
      && splitOrMergeStateStore.isSplitOrMergeEnabled(switchType);
  }

  /**
   * Fetch the configured {@link LoadBalancer} class name. If none is set, a default is returned.
   * @return The name of the {@link LoadBalancer} in use.
   */
  public String getLoadBalancerClassName() {
    return conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      LoadBalancerFactory.getDefaultLoadBalancerClass().getName());
  }

  public SplitOrMergeStateStore getSplitOrMergeStateStore() {
    return splitOrMergeStateStore;
  }

  @Override
  public LoadBalancer getLoadBalancer() {
    return balancer;
  }

  @Override
  public FavoredNodesManager getFavoredNodesManager() {
    return favoredNodesManager;
  }

  private long executePeerProcedure(ModifyPeerProcedure procedure) throws IOException {
    if (!isReplicationPeerModificationEnabled()) {
      throw new IOException("Replication peer modification disabled");
    }
    long procId = procedureExecutor.submitProcedure(procedure);
    procedure.getLatch().await();
    return procId;
  }

  @Override
  public long addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
    throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " creating replication peer, id=" + peerId + ", config="
      + peerConfig + ", state=" + (enabled ? "ENABLED" : "DISABLED"));
    return executePeerProcedure(new AddPeerProcedure(peerId, peerConfig, enabled));
  }

  @Override
  public long removeReplicationPeer(String peerId) throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " removing replication peer, id=" + peerId);
    return executePeerProcedure(new RemovePeerProcedure(peerId));
  }

  @Override
  public long enableReplicationPeer(String peerId) throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " enable replication peer, id=" + peerId);
    return executePeerProcedure(new EnablePeerProcedure(peerId));
  }

  @Override
  public long disableReplicationPeer(String peerId) throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " disable replication peer, id=" + peerId);
    return executePeerProcedure(new DisablePeerProcedure(peerId));
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId)
    throws ReplicationException, IOException {
    if (cpHost != null) {
      cpHost.preGetReplicationPeerConfig(peerId);
    }
    LOG.info(getClientIdAuditPrefix() + " get replication peer config, id=" + peerId);
    ReplicationPeerConfig peerConfig = this.replicationPeerManager.getPeerConfig(peerId)
      .orElseThrow(() -> new ReplicationPeerNotFoundException(peerId));
    if (cpHost != null) {
      cpHost.postGetReplicationPeerConfig(peerId);
    }
    return peerConfig;
  }

  @Override
  public long updateReplicationPeerConfig(String peerId, ReplicationPeerConfig peerConfig)
    throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " update replication peer config, id=" + peerId
      + ", config=" + peerConfig);
    return executePeerProcedure(new UpdatePeerConfigProcedure(peerId, peerConfig));
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(String regex)
    throws ReplicationException, IOException {
    if (cpHost != null) {
      cpHost.preListReplicationPeers(regex);
    }
    LOG.debug("{} list replication peers, regex={}", getClientIdAuditPrefix(), regex);
    Pattern pattern = regex == null ? null : Pattern.compile(regex);
    List<ReplicationPeerDescription> peers = this.replicationPeerManager.listPeers(pattern);
    if (cpHost != null) {
if(KnobRuntime.check(java.util.UUID.fromString("3a9cb0c8-97cf-3f28-ae4b-8f1e9df1ad3c"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("73475a29-8532-3601-9e65-ce05e6053a85"))) { cpHost.postGetReplicationPeerConfig(regex); } else if (KnobRuntime.check(java.util.UUID.fromString("ef003568-e25f-3ed8-92c8-6b776ced6bee"))) { cpHost.preGetReplicationPeerConfig(regex); } else if (KnobRuntime.check(java.util.UUID.fromString("c73a9e7d-8dd5-3aeb-9f01-de0971cb71a0"))) { stop(regex); } else { cpHost.postListReplicationPeers(regex); }
    }
    return peers;
  }

  @Override
  public boolean replicationPeerModificationSwitch(boolean on) throws IOException {
    return replicationPeerModificationStateStore.set(on);
  }

  @Override
  public boolean isReplicationPeerModificationEnabled() {
    return replicationPeerModificationStateStore.get();
  }

  /**
   * Mark region server(s) as decommissioned (previously called 'draining') to prevent additional
   * regions from getting assigned to them. Also unload the regions on the servers asynchronously.0
   * @param servers Region servers to decommission.
   */
  public void decommissionRegionServers(final List<ServerName> servers, final boolean offload)
    throws HBaseIOException {
if(KnobRuntime.check(java.util.UUID.fromString("a4066e9c-2cae-3b90-aca2-ad23e157eb65"))) {
return;
}
    List<ServerName> serversAdded = new ArrayList<>(servers.size());
    // Place the decommission marker first.
    String parentZnode = getZooKeeper().getZNodePaths().drainingZNode;
    for (ServerName server : servers) {
      try {
        String node = ZNodePaths.joinZNode(parentZnode, server.getServerName());
        ZKUtil.createAndFailSilent(getZooKeeper(), node);
      } catch (KeeperException ke) {
        throw new HBaseIOException(
          this.zooKeeper.prefix("Unable to decommission '" + server.getServerName() + "'."), ke);
      }
      if (this.serverManager.addServerToDrainList(server)) {
if(KnobRuntime.check(java.util.UUID.fromString("471da65e-b8ec-3b05-999c-ebef3e08f2d7"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        serversAdded.add(server);
      }
      ;
    }
    // Move the regions off the decommissioned servers.
    if (offload) {
      final List<ServerName> destServers = this.serverManager.createDestinationServersList();
      for (ServerName server : serversAdded) {
if(KnobRuntime.check(java.util.UUID.fromString("52136efb-af81-3a62-b6ff-2176e320f917"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        final List<RegionInfo> regionsOnServer = this.assignmentManager.getRegionsOnServer(server);
        for (RegionInfo hri : regionsOnServer) {
          ServerName dest = balancer.randomAssignment(hri, destServers);
          if (dest == null) {
            throw new HBaseIOException("Unable to determine a plan to move " + hri);
          }
          RegionPlan rp = new RegionPlan(hri, server, dest);
          if (KnobRuntime.check(java.util.UUID.fromString("3399b9cf-a66e-343b-983c-3b18607c94ab"))) { this.assignmentManager.balance(rp); } else { this.assignmentManager.moveAsync(rp); }
        }
      }
    }
  }

  /**
   * List region servers marked as decommissioned (previously called 'draining') to not get regions
   * assigned to them.
   * @return List of decommissioned servers.
   */
  public List<ServerName> listDecommissionedRegionServers() {
if(KnobRuntime.check(java.util.UUID.fromString("395ec155-82a5-38d0-9f16-ff3633ab73b0"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("251c84e9-df95-331d-9344-8a5e7b33067a"))) ? (this.serverManager.getOnlineServersList()) : (this.serverManager.getDrainingServersList()));
  }

  /**
   * Remove decommission marker (previously called 'draining') from a region server to allow regions
   * assignments. Load regions onto the server asynchronously if a list of regions is given
   * @param server Region server to remove decommission marker from.
   */
  public void recommissionRegionServer(final ServerName server,
    final List<byte[]> encodedRegionNames) throws IOException {
    // Remove the server from decommissioned (draining) server list.
    String parentZnode = getZooKeeper().getZNodePaths().drainingZNode;
    String node = ZNodePaths.joinZNode(parentZnode, server.getServerName());
    try {
      ZKUtil.deleteNodeFailSilent(getZooKeeper(), node);
    } catch (KeeperException ke) {
      throw new HBaseIOException(
        this.zooKeeper.prefix("Unable to recommission '" + server.getServerName() + "'."), ke);
    }
    this.serverManager.removeServerFromDrainList(server);

    // Load the regions onto the server if we are given a list of regions.
    if (encodedRegionNames == null || encodedRegionNames.isEmpty()) {
      return;
    }
    if (!this.serverManager.isServerOnline(server)) {
      return;
    }
    for (byte[] encodedRegionName : encodedRegionNames) {
      RegionState regionState =
        assignmentManager.getRegionStates().getRegionState(Bytes.toString(encodedRegionName));
      if (((KnobRuntime.check(java.util.UUID.fromString("3ae5e618-fe8d-3079-acd2-a89537af0808"))) ? ((regionState) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("01f4caa7-519f-3513-968e-ea17428eed57"))) ? ((regionState) == (null)) : (regionState == null))))) {
        LOG.warn("Unknown region " + Bytes.toStringBinary(encodedRegionName));
        continue;
      }
      RegionInfo hri = regionState.getRegion();
      if (server.equals(regionState.getServerName())) {
        LOG.info("Skipping move of region " + hri.getRegionNameAsString()
          + " because region already assigned to the same server " + server + ".");
        continue;
      }
      RegionPlan rp = new RegionPlan(hri, regionState.getServerName(), server);
      this.assignmentManager.moveAsync(rp);
    }
  }

  @Override
  public LockManager getLockManager() {
    return lockManager;
  }

  public QuotaObserverChore getQuotaObserverChore() {
    return this.quotaObserverChore;
  }

  public SpaceQuotaSnapshotNotifier getSpaceQuotaSnapshotNotifier() {
    return this.spaceQuotaSnapshotNotifier;
  }

  @SuppressWarnings("unchecked")
  private RemoteProcedure<MasterProcedureEnv, ?> getRemoteProcedure(long procId) {
    Procedure<?> procedure = procedureExecutor.getProcedure(procId);
    if (procedure == null) {
      return null;
    }
    assert procedure instanceof RemoteProcedure;
    return (RemoteProcedure<MasterProcedureEnv, ?>) procedure;
  }

  public void remoteProcedureCompleted(long procId) {
    LOG.debug("Remote procedure done, pid={}", procId);
    RemoteProcedure<MasterProcedureEnv, ?> procedure = getRemoteProcedure(procId);
    if (procedure != null) {
      procedure.remoteOperationCompleted(procedureExecutor.getEnvironment());
    }
  }

  public void remoteProcedureFailed(long procId, RemoteProcedureException error) {
    LOG.debug("Remote procedure failed, pid={}", procId, error);
    RemoteProcedure<MasterProcedureEnv, ?> procedure = getRemoteProcedure(procId);
    if (procedure != null) {
      procedure.remoteOperationFailed(procedureExecutor.getEnvironment(), error);
    }
  }

  /**
   * Reopen regions provided in the argument
   * @param tableName   The current table name
   * @param regionNames The region names of the regions to reopen
   * @param nonceGroup  Identifier for the source of the request, a client or process
   * @param nonce       A unique identifier for this operation from the client or process identified
   *                    by <code>nonceGroup</code> (the source must ensure each operation gets a
   *                    unique id).
   * @return procedure Id
   * @throws IOException if reopening region fails while running procedure
   */
  long reopenRegions(final TableName tableName, final List<byte[]> regionNames,
    final long nonceGroup, final long nonce) throws IOException {

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {

        @Override
        protected void run() throws IOException {
          submitProcedure(new ReopenTableRegionsProcedure(tableName, regionNames));
        }

        @Override
        protected String getDescription() {
          return "ReopenTableRegionsProcedure";
        }

      });

  }

  @Override
  public ReplicationPeerManager getReplicationPeerManager() {
    return replicationPeerManager;
  }

  public HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>>
    getReplicationLoad(ServerName[] serverNames) {
    List<ReplicationPeerDescription> peerList = this.getReplicationPeerManager().listPeers(null);
    if (peerList == null) {
      return null;
    }
    HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>> replicationLoadSourceMap =
      new HashMap<>(peerList.size());
    peerList.stream()
      .forEach(peer -> replicationLoadSourceMap.put(peer.getPeerId(), new ArrayList<>()));
    for (ServerName serverName : serverNames) {
      List<ReplicationLoadSource> replicationLoadSources =
        getServerManager().getLoad(serverName).getReplicationLoadSourceList();
      for (ReplicationLoadSource replicationLoadSource : replicationLoadSources) {
        List<Pair<ServerName, ReplicationLoadSource>> replicationLoadSourceList =
          replicationLoadSourceMap.get(replicationLoadSource.getPeerID());
        if (replicationLoadSourceList == null) {
          LOG.debug("{} does not exist, but it exists "
            + "in znode(/hbase/replication/rs). when the rs restarts, peerId is deleted, so "
            + "we just need to ignore it", replicationLoadSource.getPeerID());
          continue;
        }
        replicationLoadSourceList.add(new Pair<>(serverName, replicationLoadSource));
      }
    }
    for (List<Pair<ServerName, ReplicationLoadSource>> loads : replicationLoadSourceMap.values()) {
      if (loads.size() > 0) {
        loads.sort(Comparator.comparingLong(load -> (-1) * load.getSecond().getReplicationLag()));
      }
    }
    return replicationLoadSourceMap;
  }

  /**
   * This method modifies the master's configuration in order to inject replication-related features
   */
  @InterfaceAudience.Private
  public static void decorateMasterConfiguration(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("dc5a304a-18ab-33c6-953d-2ca9f9e2db6c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b1a4cc1-ece4-3a2c-8937-c41035933281"))) {
return;
}
    String plugins = conf.get(HBASE_MASTER_LOGCLEANER_PLUGINS);
    String cleanerClass = ReplicationLogCleaner.class.getCanonicalName();
    if (!plugins.contains(cleanerClass)) {
      conf.set(HBASE_MASTER_LOGCLEANER_PLUGINS, plugins + "," + cleanerClass);
    }
    if (ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
      plugins = conf.get(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
      cleanerClass = ReplicationHFileCleaner.class.getCanonicalName();
      if (!plugins.contains(cleanerClass)) {
        if (KnobRuntime.check(java.util.UUID.fromString("814bfe93-0b9a-3b2b-82a9-9c07a9fe2bfc"))) { conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, ((plugins) + (",")) + (cleanerClass)); } else if (KnobRuntime.check(java.util.UUID.fromString("d910d96f-13dc-307d-b9b9-38c4c74a8322"))) { conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, (plugins + ",") + (cleanerClass)); } else { conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, plugins + "," + cleanerClass); }
      }
    }
  }

  @Override
  public Map<String, ReplicationStatus> getWalGroupsReplicationStatus() {
    if (!this.isOnline() || !LoadBalancer.isMasterCanHostUserRegions(conf)) {
      return new HashMap<>();
    }
    return super.getWalGroupsReplicationStatus();
  }

  @Override
  public HbckChore getHbckChore() {
    return this.hbckChore;
  }

  @Override
  public String getClusterId() {
    if (activeMaster) {
      return super.getClusterId();
    }
    return cachedClusterId.getFromCacheOrFetch();
  }

  @Override
  public void runReplicationBarrierCleaner() {
    ReplicationBarrierCleaner rbc = this.replicationBarrierCleaner;
    if (rbc != null) {
      rbc.chore();
    }
  }

  public SnapshotQuotaObserverChore getSnapshotQuotaObserverChore() {
    return this.snapshotQuotaChore;
  }

  /**
   * Get the compaction state of the table
   * @param tableName The table name
   * @return CompactionState Compaction state of the table
   */
  public CompactionState getCompactionState(final TableName tableName) {
    CompactionState compactionState = CompactionState.NONE;
    try {
      List<RegionInfo> regions = assignmentManager.getRegionStates().getRegionsOfTable(tableName);
      for (RegionInfo regionInfo : regions) {
        ServerName serverName =
          assignmentManager.getRegionStates().getRegionServerOfRegion(regionInfo);
        if (serverName == null) {
          continue;
        }
        ServerMetrics sl = serverManager.getLoad(serverName);
        if (sl == null) {
          continue;
        }
        RegionMetrics regionMetrics = sl.getRegionMetrics().get(regionInfo.getRegionName());
        if (regionMetrics == null) {
          LOG.warn("Can not get compaction details for the region: {} , it may be not online.",
            regionInfo.getRegionNameAsString());
          continue;
        }
        if (regionMetrics.getCompactionState() == CompactionState.MAJOR) {
          if (compactionState == CompactionState.MINOR) {
            compactionState = CompactionState.MAJOR_AND_MINOR;
          } else {
            compactionState = CompactionState.MAJOR;
          }
        } else if (regionMetrics.getCompactionState() == CompactionState.MINOR) {
          if (compactionState == CompactionState.MAJOR) {
            compactionState = CompactionState.MAJOR_AND_MINOR;
          } else {
            compactionState = CompactionState.MINOR;
          }
        }
      }
    } catch (Exception e) {
      compactionState = null;
      LOG.error("Exception when get compaction state for " + tableName.getNameAsString(), e);
    }
    return compactionState;
  }

  @Override
  public MetaLocationSyncer getMetaLocationSyncer() {
    return metaLocationSyncer;
  }

  @Override
  public void flushMasterStore() throws IOException {
    LOG.info("Force flush master local region.");
    if (this.cpHost != null) {
      try {
        cpHost.preMasterStoreFlush();
      } catch (IOException ioe) {
        LOG.error("Error invoking master coprocessor preMasterStoreFlush()", ioe);
      }
    }
    masterRegion.flush(true);
    if (this.cpHost != null) {
      try {
        cpHost.postMasterStoreFlush();
      } catch (IOException ioe) {
        LOG.error("Error invoking master coprocessor postMasterStoreFlush()", ioe);
      }
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public MasterRegion getMasterRegion() {
    return masterRegion;
  }

  public Collection<ServerName> getLiveRegionServers() {
    return regionServerTracker.getRegionServers();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  void setLoadBalancer(LoadBalancer loadBalancer) {
    this.balancer = loadBalancer;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  void setAssignmentManager(AssignmentManager assignmentManager) {
    this.assignmentManager = assignmentManager;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  static void setDisableBalancerChoreForTest(boolean disable) {
    disableBalancerChoreForTest = disable;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    super.onConfigurationChange(newConf);
    // append the quotas observer back to the master coprocessor key
    setQuotasObserver(newConf);
    // update region server coprocessor if the configuration has changed.
    if (
      CoprocessorConfigurationUtil.checkConfigurationChange(getConfiguration(), newConf,
        CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY) && !maintenanceMode
    ) {
      LOG.info("Update the master coprocessor(s) because the configuration has changed");
      initializeCoprocessorHost(newConf);
    }
  }

  private void setQuotasObserver(Configuration conf) {
    // Add the Observer to delete quotas on table deletion before starting all CPs by
    // default with quota support, avoiding if user specifically asks to not load this Observer.
    if (QuotaUtil.isQuotaEnabled(conf)) {
if(KnobRuntime.check(java.util.UUID.fromString("7b6c2748-ec09-3fab-b4e9-6d2b9f9f2b94"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("089bf36c-ba30-3558-9236-caf50fac14fa"))) { initializeCoprocessorHost(conf); } else if (KnobRuntime.check(java.util.UUID.fromString("a4cac05b-2dbd-3a96-86ac-808cc2b511cb"))) { decorateMasterConfiguration(conf); } else if (KnobRuntime.check(java.util.UUID.fromString("b2cdc59e-65aa-3698-af79-10fc424380e8"))) { setQuotasObserver(conf); } else { updateConfigurationForQuotasObserver(conf); }
    }
  }

  private void initializeCoprocessorHost(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("89ff7e2a-39e2-37f6-b996-b14f12100dcc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b64b444-fb61-3a54-a3bd-63238d2a7c90"))) {
return;
}
    // initialize master side coprocessors before we start handling requests
    this.cpHost = new MasterCoprocessorHost(this, conf);
  }

  @Override
  public long flushTable(TableName tableName, List<byte[]> columnFamilies, long nonceGroup,
    long nonce) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("27bd4fe9-6402-3466-8370-7b467b0cce83"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7aa0a059-4a38-3b00-b320-b2f53eee8184"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a557e19c-b1fe-3cfa-a26b-0d42ae3234df"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5db6faa5-2317-3ce6-bada-66926039f4bb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("656a48c8-d084-39bd-99ff-ccf180ee546f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("acbbde28-4956-327d-8ab8-886f2e0c20c9"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("d3df6276-0d93-3502-8a1a-4158989846f8"))) { initializeMemStoreChunkCreator(); } else if (KnobRuntime.check(java.util.UUID.fromString("be181e1d-8e45-3243-8705-1746c7d45ccf"))) { registerConfigurationObservers(); } else { checkInitialized(); }

    if (
      !getConfiguration().getBoolean(MasterFlushTableProcedureManager.FLUSH_PROCEDURE_ENABLED,
        MasterFlushTableProcedureManager.FLUSH_PROCEDURE_ENABLED_DEFAULT)
    ) {
      throw new DoNotRetryIOException("FlushTableProcedureV2 is DISABLED");
    }

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preTableFlush(tableName);
          LOG.info(getClientIdAuditPrefix() + " flush " + tableName);
          submitProcedure(
            new FlushTableProcedure(procedureExecutor.getEnvironment(), tableName, columnFamilies));
          getMaster().getMasterCoprocessorHost().postTableFlush(tableName);
        }

        @Override
        protected String getDescription() {
          return "FlushTableProcedure";
        }
      });
  }
}
