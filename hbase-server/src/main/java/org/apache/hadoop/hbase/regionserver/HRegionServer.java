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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.ChoreService.CHORE_SERVICE_INITIAL_POOL_SIZE;
import static org.apache.hadoop.hbase.ChoreService.DEFAULT_CHORE_SERVICE_INITIAL_POOL_SIZE;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_SLOW_LOG_SYS_TABLE_CHORE_DURATION;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.namequeues.NamedQueueServiceChore.NAMED_QUEUE_CHORE_DURATION_DEFAULT;
import static org.apache.hadoop.hbase.namequeues.NamedQueueServiceChore.NAMED_QUEUE_CHORE_DURATION_KEY;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_CHORE_DURATION_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_CHORE_DURATION_KEY;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.util.DNS.UNSAFE_RS_HOSTNAME_KEY;

import com.google.errorprone.annotations.RestrictedApi;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.servlet.http.HttpServlet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.FailedCloseWALAfterInitializedErrorException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.MetaRegionLocationCache;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionServerRegistry;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.ServerConnectionUtils;
import org.apache.hadoop.hbase.client.locking.EntityLock;
import org.apache.hadoop.hbase.client.locking.LockServiceClient;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.DecommissionedHostRejectedException;
import org.apache.hadoop.hbase.ipc.NettyRpcClientConfigHelper;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.mob.RSMobFileCleanerChore;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.NamedQueueServiceChore;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.quotas.FileSystemUtilizationChore;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionSize;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.RSProcedureHandler;
import org.apache.hadoop.hbase.regionserver.handler.RegionReplicaFlushHandler;
import org.apache.hadoop.hbase.regionserver.http.RSDumpServlet;
import org.apache.hadoop.hbase.regionserver.http.RSStatusServlet;
import org.apache.hadoop.hbase.regionserver.throttle.FlushThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEventTrackerListener;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.NettyAsyncFSWALConfigHelper;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAuthentication;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.net.InetAddresses;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.UserLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUseReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with the HMaster. There
 * are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings({ "deprecation" })
public class HRegionServer extends Thread
  implements RegionServerServices, LastSequenceId, ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionServer.class);

  int unitMB = 1024 * 1024;
  int unitKB = 1024;

  /**
   * For testing only! Set to true to skip notifying region assignment to master .
   */
  @InterfaceAudience.Private
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MS_SHOULD_BE_FINAL")
  public static boolean TEST_SKIP_REPORTING_TRANSITION = false;

  /**
   * A map from RegionName to current action in progress. Boolean value indicates: true - if open
   * region action in progress false - if close region action in progress
   */
  private final ConcurrentMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * Used to cache the open/close region procedures which already submitted. See
   * {@link #submitRegionProcedure(long)}.
   */
  private final ConcurrentMap<Long, Long> submittedRegionProcedures = new ConcurrentHashMap<>();
  /**
   * Used to cache the open/close region procedures which already executed. See
   * {@link #submitRegionProcedure(long)}.
   */
  private final Cache<Long, Long> executedRegionProcedures =
    CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build();

  /**
   * Used to cache the moved-out regions
   */
  private final Cache<String, MovedRegionInfo> movedRegionInfoCache = CacheBuilder.newBuilder()
    .expireAfterWrite(movedRegionCacheExpiredTime(), TimeUnit.MILLISECONDS).build();

  private MemStoreFlusher cacheFlusher;

  private HeapMemoryManager hMemManager;

  /**
   * Cluster connection to be shared by services. Initialized at server startup and closed when
   * server shuts down. Clients must never close it explicitly. Clients hosted by this Server should
   * make use of this clusterConnection rather than create their own; if they create their own,
   * there is no way for the hosting server to shutdown ongoing client RPCs.
   */
  protected ClusterConnection clusterConnection;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  private ReplicationSourceService replicationSourceHandler;
  private ReplicationSinkService replicationSinkHandler;

  // Compactions
  private CompactSplit compactSplitThread;

  /**
   * Map of regions currently being served by this region server. Key is the encoded region name.
   * All access should be synchronized.
   */
  private final Map<String, HRegion> onlineRegions = new ConcurrentHashMap<>();
  /**
   * Lock for gating access to {@link #onlineRegions}. TODO: If this map is gated by a lock, does it
   * need to be a ConcurrentHashMap?
   */
  private final ReentrantReadWriteLock onlineRegionsLock = new ReentrantReadWriteLock();

  /**
   * Map of encoded region names to the DataNode locations they should be hosted on We store the
   * value as Address since InetSocketAddress is required by the HDFS API (create() that takes
   * favored nodes as hints for placing file blocks). We could have used ServerName here as the
   * value class, but we'd need to convert it to InetSocketAddress at some point before the HDFS API
   * call, and it seems a bit weird to store ServerName since ServerName refers to RegionServers and
   * here we really mean DataNode locations. We don't store it as InetSocketAddress here because the
   * conversion on demand from Address to InetSocketAddress will guarantee the resolution results
   * will be fresh when we need it.
   */
  private final Map<String, Address[]> regionFavoredNodesMap = new ConcurrentHashMap<>();

  private LeaseManager leaseManager;

  // Instance of the hbase executor executorService.
  protected ExecutorService executorService;

  private volatile boolean dataFsOk;
  private HFileSystem dataFs;
  private HFileSystem walFs;

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  private volatile boolean stopped = false;
  // Only for testing
  private boolean isShutdownHookInstalled = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  private AtomicBoolean abortRequested;
  static final String ABORT_TIMEOUT = "hbase.regionserver.abort.timeout";
  // Default abort timeout is 1200 seconds for safe
  private static final long DEFAULT_ABORT_TIMEOUT = 1200000;
  // Will run this task when abort timeout
  static final String ABORT_TIMEOUT_TASK = "hbase.regionserver.abort.timeout.task";

  // A state before we go into stopped state. At this stage we're closing user
  // space regions.
  private boolean stopping = false;
  private volatile boolean killed = false;
  private volatile boolean shutDown = false;

  protected final Configuration conf;

  private Path dataRootDir;
  private Path walRootDir;

  private final int threadWakeFrequency;
  final int msgInterval;

  private static final String PERIOD_COMPACTION = "hbase.regionserver.compaction.check.period";
  private final int compactionCheckFrequency;
  private static final String PERIOD_FLUSH = "hbase.regionserver.flush.check.period";
  private final int flushCheckFrequency;

  // Stub to do region server status calls against the master.
  private volatile RegionServerStatusService.BlockingInterface rssStub;
  private volatile LockService.BlockingInterface lockStub;
  // RPC client. Used to make the stub above that does region server status checking.
  private RpcClient rpcClient;

  private RpcRetryingCallerFactory rpcRetryingCallerFactory;
  private RpcControllerFactory rpcControllerFactory;

  private UncaughtExceptionHandler uncaughtExceptionHandler;

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  protected InfoServer infoServer;
  private JvmPauseMonitor pauseMonitor;

  private RSSnapshotVerifier rsSnapshotVerifier;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  private MetricsRegionServer metricsRegionServer;
  MetricsRegionServerWrapperImpl metricsRegionServerImpl;

  /**
   * ChoreService used to schedule tasks that we want to run periodically
   */
  private ChoreService choreService;

  /**
   * Check for compactions requests.
   */
  private ScheduledChore compactionChecker;

  /**
   * Check for flushes
   */
  private ScheduledChore periodicFlusher;

  private volatile WALFactory walFactory;

  private LogRoller walRoller;

  // A thread which calls reportProcedureDone
  private RemoteProcedureResultReporter procedureResultReporter;

  // flag set after we're done setting up server threads
  final AtomicBoolean online = new AtomicBoolean(false);

  // zookeeper connection and watcher
  protected final ZKWatcher zooKeeper;

  // master address tracker
  private final MasterAddressTracker masterAddressTracker;

  /**
   * Cache for the meta region replica's locations. Also tracks their changes to avoid stale cache
   * entries. Used for serving ClientMetaService.
   */
  private final MetaRegionLocationCache metaRegionLocationCache;

  // Cluster Status Tracker
  protected final ClusterStatusTracker clusterStatusTracker;

  // Log Splitting Worker
  private SplitLogWorker splitLogWorker;

  // A sleeper that sleeps for msgInterval.
  protected final Sleeper sleeper;

  private final int operationTimeout;
  private final int shortOperationTimeout;

  // Time to pause if master says 'please hold'
  private final long retryPauseTime;

  private final RegionServerAccounting regionServerAccounting;

  private NamedQueueServiceChore namedQueueServiceChore = null;

  // Block cache
  private BlockCache blockCache;
  // The cache for mob files
  private MobFileCache mobFileCache;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /** The nonce manager chore. */
  private ScheduledChore nonceManagerChore;

  private Map<String, com.google.protobuf.Service> coprocessorServiceHandlers = Maps.newHashMap();

  /**
   * The server name the Master sees us as. Its made from the hostname the master passes us, port,
   * and server startcode. Gets set after registration against Master.
   */
  protected ServerName serverName;

  /**
   * hostname specified by hostname config
   */
  protected String useThisHostnameInstead;

  /**
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *             {@link HRegionServer#UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY} instead.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-24667">HBASE-24667</a>
   */
  @Deprecated
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  final static String RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY =
    "hbase.regionserver.hostname.disable.master.reversedns";

  /**
   * HBASE-18226: This config and hbase.unsafe.regionserver.hostname are mutually exclusive.
   * Exception will be thrown if both are used.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  final static String UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY =
    "hbase.unsafe.regionserver.hostname.disable.master.reversedns";

  /**
   * This servers startcode.
   */
  protected final long startcode;

  /**
   * Unique identifier for the cluster we are a part of.
   */
  protected String clusterId;

  // chore for refreshing store files for secondary regions
  private StorefileRefresherChore storefileRefresher;

  private volatile RegionServerCoprocessorHost rsHost;

  private RegionServerProcedureManagerHost rspmHost;

  private RegionServerRpcQuotaManager rsQuotaManager;
  private RegionServerSpaceQuotaManager rsSpaceQuotaManager;

  /**
   * Nonce manager. Nonces are used to make operations like increment and append idempotent in the
   * case where client doesn't receive the response from a successful operation and retries. We
   * track the successful ops for some time via a nonce sent by client and handle duplicate
   * operations (currently, by failing them; in future we might use MVCC to return result). Nonces
   * are also recovered from WAL during, recovery; however, the caveats (from HBASE-3787) are: - WAL
   * recovery is optimized, and under high load we won't read nearly nonce-timeout worth of past
   * records. If we don't read the records, we don't read and recover the nonces. Some WALs within
   * nonce-timeout at recovery may not even be present due to rolling/cleanup. - There's no WAL
   * recovery during normal region move, so nonces will not be transfered. We can have separate
   * additional "Nonce WAL". It will just contain bunch of numbers and won't be flushed on main path
   * - because WAL itself also contains nonces, if we only flush it before memstore flush, for a
   * given nonce we will either see it in the WAL (if it was never flushed to disk, it will be part
   * of recovery), or we'll see it as part of the nonce log (or both occasionally, which doesn't
   * matter). Nonce log file can be deleted after the latest nonce in it expired. It can also be
   * recovered during move.
   */
  final ServerNonceManager nonceManager;

  private UserProvider userProvider;

  protected final RSRpcServices rpcServices;

  private CoordinatedStateManager csm;

  /**
   * Configuration manager is used to register/deregister and notify the configuration observers
   * when the regionserver is notified that there was a change in the on disk configs.
   */
  protected final ConfigurationManager configurationManager;

  private BrokenStoreFileCleaner brokenStoreFileCleaner;

  private RSMobFileCleanerChore rsMobFileCleanerChore;

  @InterfaceAudience.Private
  CompactedHFilesDischarger compactedFileDischarger;

  private volatile ThroughputController flushThroughputController;

  private SecureBulkLoadManager secureBulkLoadManager;

  private FileSystemUtilizationChore fsUtilizationChore;

  private final NettyEventLoopGroupConfig eventLoopGroupConfig;

  /**
   * Provide online slow log responses from ringbuffer
   */
  private NamedQueueRecorder namedQueueRecorder = null;

  private BootstrapNodeManager bootstrapNodeManager;

  /**
   * True if this RegionServer is coming up in a cluster where there is no Master; means it needs to
   * just come up and make do without a Master to talk to: e.g. in test or HRegionServer is doing
   * other than its usual duties: e.g. as an hollowed-out host whose only purpose is as a
   * Replication-stream sink; see HBASE-18846 for more. TODO: can this replace
   * {@link #TEST_SKIP_REPORTING_TRANSITION} ?
   */
  private final boolean masterless;
  private static final String MASTERLESS_CONFIG_NAME = "hbase.masterless";

  /** regionserver codec list **/
  private static final String REGIONSERVER_CODEC = "hbase.regionserver.codecs";

  // A timer to shutdown the process if abort takes too long
  private Timer abortMonitor;

  /*
   * Chore that creates replication marker rows.
   */
  private ReplicationMarkerChore replicationMarkerChore;

  // A timer submit requests to the PrefetchExecutor
  private PrefetchExecutorNotifier prefetchExecutorNotifier;

  /**
   * Starts a HRegionServer at the default location.
   * <p/>
   * Don't start any services or managers in here in the Constructor. Defer till after we register
   * with the Master as much as possible. See {@link #startServices}.
   */
  public HRegionServer(final Configuration conf) throws IOException {
    super("RegionServer"); // thread name
    final Span span = TraceUtil.createSpan("HRegionServer.cxtor");
    try (Scope ignored = span.makeCurrent()) {
      this.startcode = EnvironmentEdgeManager.currentTime();
      this.conf = conf;
      this.dataFsOk = true;
      this.masterless = conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
      this.eventLoopGroupConfig = setupNetty(this.conf);
      MemorySizeUtil.checkForClusterFreeHeapMemoryLimit(this.conf);
      HFile.checkHFileVersion(this.conf);
      checkCodecs(this.conf);
      this.userProvider = UserProvider.instantiate(conf);
      FSUtils.setupShortCircuitRead(this.conf);

      // Disable usage of meta replicas in the regionserver
      this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);
      // Config'ed params
      this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
      this.compactionCheckFrequency = conf.getInt(PERIOD_COMPACTION, this.threadWakeFrequency);
      this.flushCheckFrequency = conf.getInt(PERIOD_FLUSH, this.threadWakeFrequency);
      this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

      this.sleeper = new Sleeper(this.msgInterval, this);

      boolean isNoncesEnabled = conf.getBoolean(HConstants.HBASE_RS_NONCES_ENABLED, true);
      this.nonceManager = isNoncesEnabled ? new ServerNonceManager(this.conf) : null;

      this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

      this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

      this.retryPauseTime = conf.getLong(HConstants.HBASE_RPC_SHORTOPERATION_RETRY_PAUSE_TIME,
        HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_RETRY_PAUSE_TIME);

      this.abortRequested = new AtomicBoolean(false);
      this.stopped = false;

      this.namedQueueRecorder = NamedQueueRecorder.getInstance(this.conf);
      rpcServices = createRpcServices();
      useThisHostnameInstead = getUseThisHostnameInstead(conf);

      // if use-ip is enabled, we will use ip to expose Master/RS service for client,
      // see HBASE-27304 for details.
      boolean useIp = conf.getBoolean(HConstants.HBASE_SERVER_USEIP_ENABLED_KEY,
        HConstants.HBASE_SERVER_USEIP_ENABLED_DEFAULT);
      String isaHostName =
        useIp ? rpcServices.isa.getAddress().getHostAddress() : rpcServices.isa.getHostName();
      String hostName =
        StringUtils.isBlank(useThisHostnameInstead) ? isaHostName : useThisHostnameInstead;
      serverName = ServerName.valueOf(hostName, this.rpcServices.isa.getPort(), this.startcode);

      rpcControllerFactory = RpcControllerFactory.instantiate(this.conf);
      rpcRetryingCallerFactory = RpcRetryingCallerFactory.instantiate(this.conf,
        clusterConnection == null ? null : clusterConnection.getConnectionMetrics());

      // login the zookeeper client principal (if using security)
      ZKAuthentication.loginClient(this.conf, HConstants.ZK_CLIENT_KEYTAB_FILE,
        HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, hostName);
      // login the server principal (if using secure Hadoop)
      login(userProvider, hostName);
      // init superusers and add the server principal (if using security)
      // or process owner as default super user.
      Superusers.initialize(conf);
      regionServerAccounting = new RegionServerAccounting(conf);

      boolean isMasterNotCarryTable =
        this instanceof HMaster && !LoadBalancer.isTablesOnMaster(conf);

      // no need to instantiate block cache and mob file cache when master not carry table
      if (!isMasterNotCarryTable) {
        blockCache = BlockCacheFactory.createBlockCache(conf);
        mobFileCache = new MobFileCache(conf);
      }

      rsSnapshotVerifier = new RSSnapshotVerifier(conf);

      uncaughtExceptionHandler =
        (t, e) -> abort("Uncaught exception in executorService thread " + t.getName(), e);

      initializeFileSystem();

      this.configurationManager = new ConfigurationManager();
      setupSignalHandlers();

      // Some unit tests don't need a cluster, so no zookeeper at all
      // Open connection to zookeeper and set primary watcher
      zooKeeper = new ZKWatcher(conf, getProcessName() + ":" + rpcServices.isa.getPort(), this,
        canCreateBaseZNode());
      // If no master in cluster, skip trying to track one or look for a cluster status.
      if (!this.masterless) {
        if (
          conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)
        ) {
          this.csm = new ZkCoordinatedStateManager(this);
        }

        masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
        masterAddressTracker.start();

        clusterStatusTracker = new ClusterStatusTracker(zooKeeper, this);
        clusterStatusTracker.start();
      } else {
        masterAddressTracker = null;
        clusterStatusTracker = null;
      }
      this.rpcServices.start(zooKeeper);
      this.metaRegionLocationCache = new MetaRegionLocationCache(zooKeeper);
      // This violates 'no starting stuff in Constructor' but Master depends on the below chore
      // and executor being created and takes a different startup route. Lots of overlap between HRS
      // and M (An M IS A HRS now). Need to refactor so less duplication between M and its super
      // Master expects Constructor to put up web servers. Ugh.
      // class HRS. TODO.
      int choreServiceInitialSize =
        conf.getInt(CHORE_SERVICE_INITIAL_POOL_SIZE, DEFAULT_CHORE_SERVICE_INITIAL_POOL_SIZE);
      this.choreService = new ChoreService(getName(), choreServiceInitialSize, true);
      this.executorService = new ExecutorService(getName());
      putUpWebUI();
      span.setStatus(StatusCode.OK);
    } catch (Throwable t) {
      // Make sure we log the exception. HRegionServer is often started via reflection and the
      // cause of failed startup is lost.
      TraceUtil.setError(span, t);
      LOG.error("Failed construction RegionServer", t);
      throw t;
    } finally {
      span.end();
    }
  }

  // HMaster should override this method to load the specific config for master
  protected String getUseThisHostnameInstead(Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("be712f74-e163-3034-ab93-ba680c5de13b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("acab5d90-91ee-32d8-b774-0116de5b971c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e4addc03-90de-3bd9-8f81-7816804b0993"))) {
return null;
}
    String hostname = conf.get(UNSAFE_RS_HOSTNAME_KEY);
    if (((KnobRuntime.check(java.util.UUID.fromString("9b00b177-1fe9-3757-b1a7-798bb86819b1"))) ? (conf.getBoolean(UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " and ", false)) : (conf.getBoolean(UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, false)))) {
      if (!StringUtils.isBlank(hostname)) {
        String msg = UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " and "
          + UNSAFE_RS_HOSTNAME_KEY + " are mutually exclusive. Do not set "
          + UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " to true while "
          + UNSAFE_RS_HOSTNAME_KEY + " is used";
        throw new IOException(msg);
      } else {
        return rpcServices.isa.getHostName();
      }
    } else {
      return hostname;
    }
  }

  private void setupSignalHandlers() {
    if (!SystemUtils.IS_OS_WINDOWS) {
      HBasePlatformDependent.handle("HUP", (number, name) -> {
        try {
          updateConfiguration();
        } catch (IOException e) {
          LOG.error("Problem while reloading configuration", e);
        }
      });
    }
  }

  private static NettyEventLoopGroupConfig setupNetty(Configuration conf) {
    // Initialize netty event loop group at start as we may use it for rpc server, rpc client & WAL.
    NettyEventLoopGroupConfig nelgc = new NettyEventLoopGroupConfig(conf, "RS-EventLoopGroup");
if(KnobRuntime.check(java.util.UUID.fromString("b4fca999-c83a-30c3-8c96-fcb811646c33"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("6e833e3f-5d01-3287-a816-fbfa55e6f3d9"))) { NettyAsyncFSWALConfigHelper.setEventLoopConfig(conf, nelgc.group(), nelgc.clientChannelClass()); } else { NettyRpcClientConfigHelper.setEventLoopConfig(conf, nelgc.group(), nelgc.clientChannelClass()); }
if(KnobRuntime.check(java.util.UUID.fromString("a22dd191-2a06-316b-a4f9-b946ec34313c"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("58431518-96ec-3ed6-928b-2d9af33b7158"))) { NettyRpcClientConfigHelper.setEventLoopConfig(conf, nelgc.group(), nelgc.clientChannelClass()); } else { NettyAsyncFSWALConfigHelper.setEventLoopConfig(conf, nelgc.group(), nelgc.clientChannelClass()); }
    return nelgc;
  }

  private void initializeFileSystem() throws IOException {
    // Get fs instance used by this RS. Do we use checksum verification in the hbase? If hbase
    // checksum verification enabled, then automatically switch off hdfs checksum verification.
    boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    String walDirUri = CommonFSUtils.getDirUri(this.conf,
      new Path(conf.get(CommonFSUtils.HBASE_WAL_DIR, conf.get(HConstants.HBASE_DIR))));
    // set WAL's uri
    if (walDirUri != null) {
      CommonFSUtils.setFsDefault(this.conf, walDirUri);
    }
    // init the WALFs
    this.walFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.walRootDir = CommonFSUtils.getWALRootDir(this.conf);
    // Set 'fs.defaultFS' to match the filesystem on hbase.rootdir else
    // underlying hadoop hdfs accessors will be going against wrong filesystem
    // (unless all is set to defaults).
    String rootDirUri =
      CommonFSUtils.getDirUri(this.conf, new Path(conf.get(HConstants.HBASE_DIR)));
    if (rootDirUri != null) {
      CommonFSUtils.setFsDefault(this.conf, rootDirUri);
    }
    // init the filesystem
    this.dataFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.dataRootDir = CommonFSUtils.getRootDir(this.conf);
    this.tableDescriptors = new FSTableDescriptors(this.dataFs, this.dataRootDir,
      !canUpdateTableDescriptor(), cacheTableDescriptor());
  }

  protected void login(UserProvider user, String host) throws IOException {
    user.login(SecurityConstants.REGIONSERVER_KRB_KEYTAB_FILE,
      SecurityConstants.REGIONSERVER_KRB_PRINCIPAL, host);
  }

  /**
   * Wait for an active Master. See override in Master superclass for how it is used.
   */
  protected void waitForMasterActive() {
  }

  protected String getProcessName() {
if(KnobRuntime.check(java.util.UUID.fromString("c2cbc3c6-348f-3d0f-9a10-a1a764c0d226"))) {
return null;
}
    return REGIONSERVER;
  }

  protected boolean canCreateBaseZNode() {
if(KnobRuntime.check(java.util.UUID.fromString("eb2f1904-db47-3fdd-8d66-3ec23174a661"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("dfb83b5d-591f-34fa-afae-ba7e6dd6934f"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("c4a9e53a-eabe-3142-b822-ff4fff50cea3"))) ? (!this.masterless) : (this.masterless));
  }

  protected boolean canUpdateTableDescriptor() {
if(KnobRuntime.check(java.util.UUID.fromString("454ffb90-db1c-3272-9c3f-7cebfcb9f65b"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("931a3008-bc1f-3276-8e21-cf939e98a696"))) {
return false;
}
    return false;
  }

  protected boolean cacheTableDescriptor() {
if(KnobRuntime.check(java.util.UUID.fromString("c0215757-e0b4-3b8b-88cb-eec94e884fd7"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("a817b3f2-f080-3c87-977b-4da69efc534e"))) {
return true;
}
    return false;
  }

  protected RSRpcServices createRpcServices() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ea67b0ba-cc37-38c5-abdd-67b5847a0c97"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8b64b84-28a6-36fe-819b-2cbb1faa20ef"))) {
throw new java.io.IOException("Injected exception");
}
    return new RSRpcServices(this);
  }

  protected void configureInfoServer() {
if(KnobRuntime.check(java.util.UUID.fromString("1fb23d8f-4a6f-3acd-ba06-95f3b046039a"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("f7aba14c-3c9e-32d9-b53f-e298faf8e32f"))) { infoServer.addPrivilegedServlet("rs-status", "/rs-status", RSStatusServlet.class); } else { infoServer.addUnprivilegedServlet("rs-status", "/rs-status", RSStatusServlet.class); }
    infoServer.setAttribute(REGIONSERVER, this);
  }

  protected Class<? extends HttpServlet> getDumpServlet() {
if(KnobRuntime.check(java.util.UUID.fromString("ed733012-bf66-3bda-bf67-bb5c29042da9"))) {
return null;
}
    return RSDumpServlet.class;
  }

  /**
   * Used by {@link RSDumpServlet} to generate debugging information.
   */
  public void dumpRowLocks(final PrintWriter out) {
    StringBuilder sb = new StringBuilder();
    for (HRegion region : getRegions()) {
      if (((KnobRuntime.check(java.util.UUID.fromString("d76a1042-0410-3287-85e1-979c51e7900e"))) ? ((getNumberOfOnlineRegions()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0d43493b-e2dc-3336-b6f6-d4d9d84c841f"))) ? ((movedRegionCacheExpiredTime()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("975e39a8-f689-33c8-b633-9e8d81fe1a49"))) ? ((getNumberOfOnlineRegions()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f8e2d092-8c00-3d4a-bc0c-8351e2a8af3d"))) ? ((getNumberOfOnlineRegions()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0ea0b210-18b8-36bb-91f4-89a45ec5db4e"))) ? ((region.getLockedRows().size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8ea47e76-da31-3e9c-aa45-231b61364d00"))) ? ((movedRegionCacheExpiredTime()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("21a0ea08-1cac-345f-b9dc-873007670a5c"))) ? ((region.getLockedRows().size()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e2525060-4190-3183-b239-322602f653a8"))) ? ((movedRegionCacheExpiredTime()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3144004a-61d9-3507-982e-14ba2d33d9c4"))) ? ((getNumberOfOnlineRegions()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("76195b8a-a962-3b71-9eea-26d6e3f964d4"))) ? ((getNumberOfOnlineRegions()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("419c7511-75b0-370d-915f-73cb42c68097"))) ? ((getNumberOfOnlineRegions()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4ccf8dd4-5beb-3f05-a0b7-9c31233209c5"))) ? ((movedRegionCacheExpiredTime()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("58f552c9-1e35-3f95-b77c-2df4d1e5d758"))) ? ((region.getLockedRows().size()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dd96c17a-6c5e-3fcb-afca-6f282abc8e37"))) ? ((movedRegionCacheExpiredTime()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c928fe8f-3c13-33d8-94c2-165a078359ac"))) ? ((region.getLockedRows().size()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("90ba2880-b460-3ea5-9aa6-318862b4ac2b"))) ? ((region.getLockedRows().size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("beee4a8a-e636-315f-9f3f-73d880cfe6d3"))) ? ((region.getLockedRows().size()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d893fa99-1d99-33a3-8f7e-1d20af3b3526"))) ? ((movedRegionCacheExpiredTime()) != (0)) : (region.getLockedRows().size() > 0))))))))))))))))))))))))))))))))))))) {
        for (HRegion.RowLockContext rowLockContext : region.getLockedRows().values()) {
          sb.setLength(0);
          sb.append(region.getTableDescriptor().getTableName()).append(",")
            .append(region.getRegionInfo().getEncodedName()).append(",");
          if (KnobRuntime.check(java.util.UUID.fromString("e4a31436-d675-32f0-93ab-effd6224c717"))) { sb.append(getProcessName()); } else if (KnobRuntime.check(java.util.UUID.fromString("945dc52d-f8bc-37a6-a221-9addaa7324e1"))) { sb.append(getName()); } else { sb.append(rowLockContext.toString()); }
          if (KnobRuntime.check(java.util.UUID.fromString("3c2af6be-b81a-3dc1-8611-9df4f1a0950c"))) { out.println(new StringBuilder()); } else { out.println(sb); }
        }
      }
    }
  }

  @Override
  public boolean registerService(com.google.protobuf.Service instance) {
    /*
     * No stacking of instances is allowed for a single executorService name
     */
    com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor executorService " + serviceName
        + " already registered, rejecting request from " + instance);
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Registered regionserver coprocessor executorService: executorService=" + serviceName);
    }
    return true;
  }

  protected ClusterConnection createClusterConnection() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c3125ace-c1bd-3452-b7c9-13b0136f8529"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("74d209d5-0d39-337a-a794-3ff7211a8936"))) {
return null;
}
    // Create a cluster connection that when appropriate, can short-circuit and go directly to the
    // local server if the request is to the local server bypassing RPC. Can be used for both local
    // and remote invocations.
if(KnobRuntime.check(java.util.UUID.fromString("60545e02-6d05-39fc-be32-f70ad453bf4b"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rpcTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e03ef0b6-6b67-310f-b329-edc041895362"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rowSizeWarnThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c66ab400-ecf8-3391-9f9f-f7c2576291f0"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("scannerLeaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("837f69d0-299d-3dc6-8c68-46c00ba53a1a"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rowSizeWarnThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58f2480e-48db-3617-8ccd-d725e32d9240"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f904b96-2fcb-3099-ba55-49030d3a4549"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rpcTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b82383f3-0674-3641-a7c5-600007411176"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("scannerLeaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a21a84bb-8c7d-33eb-9f8a-8c8c40637358"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("15cd57da-9137-319a-ad43-1b3102091afb"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rowSizeWarnThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3176c1a-b4dd-38f2-83d2-1438d235c090"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5b670447-4e05-3818-90f2-1f4a9c7ff79d"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rowSizeWarnThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("99855206-581f-3c05-991e-d130b398c822"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rpcTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02d90379-4d34-3c15-9fbe-117659c906d9"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("minimumScanTimeLimitDelta");
    field.setAccessible(true);
    long oldValue = ((long)field.get(rpcServices));
    field.set(rpcServices, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e2d2351-66da-390a-9505-57537670cdbc"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("rpcTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63053273-431c-366b-a210-c5685d220c3d"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("maxScannerResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(rpcServices));
    field.set(rpcServices, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9ddd2bd-b092-3dd5-9251-028fed7dfb8b"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("scannerLeaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2b73dab-d0e1-3135-bdf2-3720ab19611a"))) {
try {
    java.lang.reflect.Field field = rpcServices.getClass().getDeclaredField("scannerLeaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rpcServices));
    field.set(rpcServices, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return ServerConnectionUtils.createShortCircuitConnection(conf, userProvider.getCurrent(),
      serverName, rpcServices, rpcServices, new RegionServerRegistry(this));
  }

  /**
   * Run test on configured codecs to make sure supporting libs are in place.
   * @param c configuration object
   * @throws IOException if compression test fails for any regionserver codec
   */
  private static void checkCodecs(final Configuration c) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("efebacce-df2f-36b3-9757-70c05c507300"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f9ca8b9c-6e67-30ef-be36-d051d55b4c9e"))) {
return;
}
    // check to see if the codec list is available:
    String[] codecs = c.getStrings(REGIONSERVER_CODEC, (String[]) null);
    if (((KnobRuntime.check(java.util.UUID.fromString("8d8a8caf-f382-35fa-a485-cb65afbea7c6"))) ? ((codecs) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0fe85253-ddd6-393e-9f6d-ef6a7d3fc320"))) ? (((String[]) null) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("43993442-f46b-3bc8-b3e3-da55d2a739f9"))) ? (((String[]) null) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("04b396c3-a40a-3cd9-a26e-d53232afa077"))) ? ((codecs) == (null)) : (codecs == null))))))))) {
      return;
    }
    for (String codec : ((KnobRuntime.check(java.util.UUID.fromString("8b181a46-eddf-37f0-8cb8-5d35771f4f42"))) ? ((String[]) null) : (codecs))) {
      if (!CompressionTest.testCompression(codec)) {
        throw new IOException(
          "Compression codec " + codec + " not supported, aborting RS construction");
      }
    }
  }

  public String getClusterId() {
if(KnobRuntime.check(java.util.UUID.fromString("41d0a82a-cf57-3f73-b4c7-7e5953108ce0"))) {
return null;
}
    return this.clusterId;
  }

  /**
   * Setup our cluster connection if not already initialized.
   */
  protected synchronized void setupClusterConnection() throws IOException {
    if (clusterConnection == null) {
if(KnobRuntime.check(java.util.UUID.fromString("143c6335-c69a-3e5a-8323-f1283354c8fb"))) {
throw new java.io.IOException("Injected exception");
}
      clusterConnection = createClusterConnection();
    }
  }

  /**
   * All initialization needed before we go register with Master.<br>
   * Do bare minimum. Do bulk of initializations AFTER we've connected to the Master.<br>
   * In here we just put up the RpcServer, setup Connection, and ZooKeeper.
   */
  private void preRegistrationInitialization() {
    final Span span = TraceUtil.createSpan("HRegionServer.preRegistrationInitialization");
    try (Scope ignored = span.makeCurrent()) {
if(KnobRuntime.check(java.util.UUID.fromString("fa28c7b2-355e-3b5c-a825-e862cc1da3de"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6b730b95-d6fb-3751-b482-43b5cd093478"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("199af875-0d48-3c13-a63e-a99959b0c698"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("abfd063a-8961-3261-aabd-b513b8c54008"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("fde412d5-dde5-3467-95ec-646e755dd74e"))) { setupSignalHandlers(); } else { initializeZooKeeper(); }
      setupClusterConnection();
      if (!(this instanceof HMaster)) {
        bootstrapNodeManager = new BootstrapNodeManager(clusterConnection, masterAddressTracker);
      }
      // Setup RPC client for master communication
      this.rpcClient = RpcClientFactory.createClient(conf, clusterId,
        new InetSocketAddress(this.rpcServices.isa.getAddress(), 0),
        clusterConnection.getConnectionMetrics(), Collections.emptyMap());
      span.setStatus(StatusCode.OK);
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      TraceUtil.setError(span, t);
      if (KnobRuntime.check(java.util.UUID.fromString("2f89a6b8-a76c-3e8e-b298-a882287a5206"))) { setupSignalHandlers(); } else { this.rpcServices.stop(); }
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("15367fde-92ac-3415-9d4a-380c42460ca1"))) { setupSignalHandlers(); } else { span.end(); }
    }
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this cluster and then after
   * that, wait until cluster 'up' flag has been set. This is the order in which master does things.
   * <p>
   * Finally open long-living server short-circuit connection.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
      justification = "cluster Id znode read would give us correct response")
  private void initializeZooKeeper() throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("68020929-16cd-3c63-bc5e-d8e1d3a7adca"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("39f457e5-8cdd-3bf7-8d6b-76457c873b0c"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d4c5cf04-c17d-3ac0-ac95-4fe349b31e29"))) {
return;
}
    // Nothing to do in here if no Master in the mix.
    if (((KnobRuntime.check(java.util.UUID.fromString("f420753b-8eac-3347-af2f-7a692884f715"))) ? (!this.masterless) : (this.masterless))) {
      return;
    }

    // Create the master address tracker, register with zk, and start it. Then
    // block until a master is available. No point in starting up if no master
    // running.
    blockAndCheckIfStopped(this.masterAddressTracker);

    // Wait on cluster being up. Master will set this flag up in zookeeper
    // when ready.
    blockAndCheckIfStopped(this.clusterStatusTracker);

    // If we are HMaster then the cluster id should have already been set.
    if (((KnobRuntime.check(java.util.UUID.fromString("a6f717e9-665e-36ff-a77d-b219db85ef6c"))) ? ((clusterId) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("65962882-4cea-34fa-b917-1ce26e7fbd49"))) ? ((clusterId) != (null)) : (clusterId == null))))) {
      // Retrieve clusterId
      // Since cluster status is now up
      // ID should have already been set by HMaster
      try {
        clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
        if (((KnobRuntime.check(java.util.UUID.fromString("ae829592-c965-3d99-8399-41554dcae290"))) ? ((clusterId) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c98400c8-ca50-3caf-990b-00964aa65651"))) ? ((clusterId) != (null)) : (clusterId == null))))) {
          this.abort("Cluster ID has not been set");
        }
        if (KnobRuntime.check(java.util.UUID.fromString("58c9c197-c99f-32bd-9227-fdd06a7e1029"))) { LOG.debug("ClusterId : "); } else if (KnobRuntime.check(java.util.UUID.fromString("e7ef1969-e4ff-3416-b8e0-ee7e180b2ed0"))) { LOG.error(("ClusterId : ") + (clusterId)); } else if (KnobRuntime.check(java.util.UUID.fromString("80f318a1-73d2-3b34-969a-3bd96b35fda3"))) { LOG.debug(("ClusterId : ") + (clusterId)); } else if (KnobRuntime.check(java.util.UUID.fromString("9a146a0e-a46d-3385-86c9-c6aa7bac2b6c"))) { LOG.error("ClusterId : "); } else if (KnobRuntime.check(java.util.UUID.fromString("bf1b698a-35e5-38ce-aceb-fde5507e5fd1"))) { LOG.info(("ClusterId : ") + (clusterId)); } else if (KnobRuntime.check(java.util.UUID.fromString("eda9a9ea-279d-3238-acac-26288bf83c0d"))) { LOG.info("ClusterId : "); } else { LOG.info("ClusterId : " + clusterId); }
      } catch (KeeperException e) {
        if (KnobRuntime.check(java.util.UUID.fromString("51b95112-74ed-326f-99cf-dadd82a1347e"))) { abort("Failed to retrieve Cluster ID", e); } else { this.abort("Failed to retrieve Cluster ID", e); }
      }
    }

    if (KnobRuntime.check(java.util.UUID.fromString("8975aec4-ce35-3149-9f43-4666c4bb3a38"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("16eadcea-2a7d-3c31-a5a7-a5c34fa36616"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("e6e68b8f-e91c-3504-8232-6d36efb9daec"))) { initializeFileSystem(); } else { waitForMasterActive(); }
    if (((KnobRuntime.check(java.util.UUID.fromString("50205688-5f17-327d-b7e0-a3d2b4ff53c0"))) ? ((isStopped()) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("a553ded6-ba49-3a20-bdb0-585ce9c4ed37"))) ? ((canUpdateTableDescriptor()) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("c4d0b5e6-5613-374a-a7f0-2b9d931d42bc"))) ? ((cacheTableDescriptor()) || (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("4f2ad4e9-0e5c-3b27-b3a7-3cab676ab265"))) ? ((canCreateBaseZNode()) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("098c67b7-9884-36a3-8d48-ede2a8ed9fc1"))) ? ((cacheTableDescriptor()) && (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("d50a6272-65a0-3217-8d76-353a5e08fb63"))) ? (canCreateBaseZNode()) : (((KnobRuntime.check(java.util.UUID.fromString("145cb6f6-9bc3-3069-80db-bb68cc2e01a8"))) ? ((canUpdateTableDescriptor()) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("98893881-4b22-3b39-8c5b-caaecb53d260"))) ? ((canCreateBaseZNode()) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("d35a79a6-c0de-3d81-b2f9-34750885cfd6"))) ? ((canUpdateTableDescriptor()) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("4402bc7e-e6eb-39a1-97b9-d2f43f10b72c"))) ? ((cacheTableDescriptor()) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("3b7b8a51-2b21-320e-9ae2-a3d8a016b271"))) ? (isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("c5c92780-6bb5-3963-8331-f9cf2bc87239"))) ? ((isStopped()) && (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("23a9afc5-efce-3dc2-b927-e75288d3b4bc"))) ? ((canUpdateTableDescriptor()) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("ea000167-81ea-3baf-a806-8e08e88d0edb"))) ? ((canCreateBaseZNode()) && (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("bdf1ca1a-8228-355b-a888-d3db382a7269"))) ? ((isStopped()) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("be21f4a2-e7ad-3ccb-b125-bd7d33a54912"))) ? ((isStopped()) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("9a89723f-297d-30fc-95b5-c99ec5a72f80"))) ? ((cacheTableDescriptor()) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("0682c92a-0b8b-3ad0-a22f-c1ba845c257f"))) ? ((isStopped()) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("1234a227-a4b4-3231-9ff7-35c0d4ac9906"))) ? (isAborted()) : (((KnobRuntime.check(java.util.UUID.fromString("d3e9675e-7c14-374a-aeaf-41614792e0b1"))) ? ((canCreateBaseZNode()) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("a9f7f12b-81cc-39c4-92ff-93cd9c0984ad"))) ? ((isStopped()) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("eeb33d54-153c-3931-85e4-d4208434cb14"))) ? ((canUpdateTableDescriptor()) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("cf79b99f-a673-3ac6-8e91-3db124db74da"))) ? ((cacheTableDescriptor()) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("23b14e5c-91a4-3e3a-b923-619c8d4b9241"))) ? (canUpdateTableDescriptor()) : (((KnobRuntime.check(java.util.UUID.fromString("ca4d57db-9a02-3bac-873f-834f9c6c8a4a"))) ? ((canUpdateTableDescriptor()) || (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("61041dd6-c3ea-33d8-976a-77e5e376d98e"))) ? ((canCreateBaseZNode()) || (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("362bb77b-763c-3803-97a6-9c22e6eee15a"))) ? (cacheTableDescriptor()) : (((KnobRuntime.check(java.util.UUID.fromString("a0aa5c00-609f-3c4c-8d72-3facccf38d90"))) ? ((cacheTableDescriptor()) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("24bbe70b-e93a-38ae-a578-4393220243c5"))) ? ((isStopped()) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("f8415d6e-d884-30e3-b72c-d9edd698fcd7"))) ? ((cacheTableDescriptor()) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("85a86c61-9995-38b3-a168-ad5293496650"))) ? ((canCreateBaseZNode()) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("376e6bcc-ccb4-326c-8f9c-0c8e93561806"))) ? ((canCreateBaseZNode()) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("25401bb6-9904-3d87-a7d1-44b7d207383b"))) ? ((canCreateBaseZNode()) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("12cb543e-1d9d-3538-a888-396e28286a6f"))) ? ((isStopped()) || (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("6950ae86-95c4-3507-8cb2-3844d7a08349"))) ? ((canUpdateTableDescriptor()) && (isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("36669af6-1404-363f-8359-bac7b6a385a5"))) ? ((cacheTableDescriptor()) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("fc212a7c-24ba-3055-b7a5-9411b5df84cc"))) ? ((canUpdateTableDescriptor()) && (canCreateBaseZNode())) : (isStopped() || isAborted()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return; // No need for further initialization
    }

    // watch for snapshots and other procedures
    try {
      rspmHost = new RegionServerProcedureManagerHost();
      rspmHost.loadProcedures(conf);
      rspmHost.initialize(this);
    } catch (KeeperException e) {
      this.abort("Failed to reach coordination cluster when creating procedure handler.", e);
    }
  }

  /**
   * Utilty method to wait indefinitely on a znode availability while checking if the region server
   * is shut down
   * @param tracker znode tracker to use
   * @throws IOException          any IO exception, plus if the RS is stopped
   * @throws InterruptedException if the waiting thread is interrupted
   */
  private void blockAndCheckIfStopped(ZKNodeTracker tracker)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("6b316d44-3e3a-3655-8bef-25015cdefd68"))) {
try {
    java.lang.reflect.Field field = tracker.getClass().getDeclaredField("stopped");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(tracker);
    field.set(tracker, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c745806f-00a8-3e7e-b73e-0f2700224aaf"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a4b25ee-a289-3871-93f2-88bfd7b01168"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fc2e7cdb-cd83-313e-8c77-302b61aacda4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fcec87aa-5b99-3a33-a17f-b2db1a5adae9"))) {
throw new java.lang.InterruptedException("Injected exception");
}
    while (((KnobRuntime.check(java.util.UUID.fromString("e61235b0-f5ea-31c4-9bd2-7adad2b2f3c9"))) ? ((tracker.blockUntilAvailable(this.msgInterval, false)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fbecb7e1-6c8f-36e4-a97d-10a2a29a556e"))) ? ((tracker.blockUntilAvailable(this.msgInterval, false)) != (null)) : (tracker.blockUntilAvailable(this.msgInterval, false) == null))))) {
      if (this.stopped) {
        throw new IOException("Received the shutdown message while waiting.");
      }
    }
  }

  /** Returns True if the cluster is up. */
  @Override
  public boolean isClusterUp() {
    return this.masterless
      || (this.clusterStatusTracker != null && this.clusterStatusTracker.isClusterUp());
  }

  private void initializeReplicationMarkerChore() {
    boolean replicationMarkerEnabled =
      conf.getBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
    // If replication or replication marker is not enabled then return immediately.
    if (replicationMarkerEnabled) {
      int period = conf.getInt(REPLICATION_MARKER_CHORE_DURATION_KEY,
        REPLICATION_MARKER_CHORE_DURATION_DEFAULT);
      replicationMarkerChore = new ReplicationMarkerChore(this, this, period);
    }
  }

  /**
   * The HRegionServer sticks in this loop until closed.
   */
  @Override
  public void run() {
    if (isStopped()) {
      LOG.info("Skipping run; stopped");
      return;
    }
    try {
      if (KnobRuntime.check(java.util.UUID.fromString("234b7782-b8f7-3a8c-b6f2-96c1849f1af6"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("46c049f5-dc16-389c-9019-2fde2fdae9fc"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("e9f59f26-dde7-3242-8f3b-d09cf524590b"))) { putUpWebUI(); } else { installShutdownHook(); }
      // Do pre-registration initializations; zookeeper, lease threads, etc.
      preRegistrationInitialization();
    } catch (Throwable e) {
      abort("Fatal exception during initialization", e);
    }

    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("93bf3ca8-8619-3919-8348-e9cad9fc1578"))) ? ((!isStopped()) && (!isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("ec8af18e-3d52-3a6a-94f5-ee46b97cbc1c"))) ? ((!isStopped()) || (!isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("ab0a761a-0250-3c80-acb2-829add9a1a9e"))) ? (!isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("6361b396-c33f-3535-819c-fef48afceff5"))) ? (!isAborted()) : (!isStopped() && !isAborted()))))))))) {
        // Initialize the RegionServerCoprocessorHost now that our ephemeral
        // node was created, in case any coprocessors want to use ZooKeeper
        this.rsHost = new RegionServerCoprocessorHost(this, this.conf);

        // Try and register with the Master; tell it we are here. Break if server is stopped or
        // the clusterup flag is down or hdfs went wacky. Once registered successfully, go ahead and
        // start up all Services. Use RetryCounter to get backoff in case Master is struggling to
        // come up.
        LOG.debug("About to register with Master.");
        TraceUtil.trace(() -> {
          RetryCounterFactory rcf =
            new RetryCounterFactory(Integer.MAX_VALUE, this.sleeper.getPeriod(), 1000 * 60 * 5);
          RetryCounter rc = rcf.create();
          while (keepLooping()) {
            RegionServerStartupResponse w = reportForDuty();
            if (w == null) {
              long sleepTime = rc.getBackoffTimeAndIncrementAttempts();
              LOG.warn("reportForDuty failed; sleeping {} ms and then retrying.", sleepTime);
if(KnobRuntime.check(java.util.UUID.fromString("6b362359-2c8a-3249-aab3-337744089a93"))) {
sleepTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("42f45187-1a44-359c-ac21-4190f19fce90"))) {
sleepTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("554ad023-732d-32b4-8394-d08c0d0162ff"))) {
sleepTime -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1135572a-0b26-38e2-b4aa-23da51824d96"))) {
sleepTime = -1;
}
              this.sleeper.sleep(sleepTime);
            } else {
if(KnobRuntime.check(java.util.UUID.fromString("6b6845bc-c3b3-3d22-802d-031c10826016"))) {
throw new java.io.IOException("Injected exception");
}
              handleReportForDutyResponse(w);
              break;
            }
          }
        }, "HRegionServer.registerWithMaster");
      }

      if (!isStopped() && isHealthy()) {
        TraceUtil.trace(() -> {
          // start the snapshot handler and other procedure handlers,
          // since the server is ready to run
          if (this.rspmHost != null) {
            this.rspmHost.start();
          }
          // Start the Quota Manager
          if (this.rsQuotaManager != null) {
            rsQuotaManager.start(getRpcServer().getScheduler());
          }
          if (this.rsSpaceQuotaManager != null) {
            this.rsSpaceQuotaManager.start();
          }
        }, "HRegionServer.startup");
      }

      // We registered with the Master. Go into run mode.
      long lastMsg = EnvironmentEdgeManager.currentTime();
      long oldRequestCount = -1;
      // The main run loop.
      while (!isStopped() && isHealthy()) {
        if (!isClusterUp()) {
          if (onlineRegions.isEmpty()) {
            stop("Exiting; cluster shutdown set and not carrying any regions");
          } else if (!this.stopping) {
            this.stopping = true;
            LOG.info("Closing user regions");
            closeUserRegions(this.abortRequested.get());
          } else {
            boolean allUserRegionsOffline = areAllUserRegionsOffline();
            if (allUserRegionsOffline) {
              // Set stopped if no more write requests tp meta tables
              // since last time we went around the loop. Any open
              // meta regions will be closed on our way out.
              if (oldRequestCount == getWriteRequestCount()) {
                stop("Stopped; only catalog regions remaining online");
                break;
              }
              oldRequestCount = getWriteRequestCount();
            } else {
              // Make sure all regions have been closed -- some regions may
              // have not got it because we were splitting at the time of
              // the call to closeUserRegions.
              closeUserRegions(this.abortRequested.get());
            }
            LOG.debug("Waiting on " + getOnlineRegionsAsPrintableString());
          }
        }
        long now = EnvironmentEdgeManager.currentTime();
        if ((now - lastMsg) >= msgInterval) {
          tryRegionServerReport(lastMsg, now);
          lastMsg = EnvironmentEdgeManager.currentTime();
        }
        if (!isStopped() && !isAborted()) {
          this.sleeper.sleep();
        }
      } // for
    } catch (Throwable t) {
      if (!rpcServices.checkOOME(t)) {
        String prefix = t instanceof YouAreDeadException ? "" : "Unhandled: ";
        abort(prefix + t.getMessage(), t);
      }
    }

    final Span span = TraceUtil.createSpan("HRegionServer exiting main loop");
    try (Scope ignored = span.makeCurrent()) {
      if (this.leaseManager != null) {
        this.leaseManager.closeAfterLeasesExpire();
      }
      if (this.splitLogWorker != null) {
        splitLogWorker.stop();
      }
      if (this.infoServer != null) {
        LOG.info("Stopping infoServer");
        try {
          this.infoServer.stop();
        } catch (Exception e) {
          LOG.error("Failed to stop infoServer", e);
        }
      }
      // Send cache a shutdown.
      if (blockCache != null) {
        blockCache.shutdown();
      }
      if (mobFileCache != null) {
        mobFileCache.shutdown();
      }

      // Send interrupts to wake up threads if sleeping so they notice shutdown.
      // TODO: Should we check they are alive? If OOME could have exited already
      if (((KnobRuntime.check(java.util.UUID.fromString("2dcdcb18-02a0-30ca-b12f-2e632f778e2a"))) ? ((this.hMemManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bce228d7-d769-3560-a209-e7b4a3de8657"))) ? ((this.hMemManager) == (null)) : (this.hMemManager != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("06c64ac6-c314-3e39-bd0f-0c3e4a83d935"))) { setupSignalHandlers(); } else { this.hMemManager.stop(); }
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("3a1e58c8-2c38-30fe-aa01-f487568ae8a5"))) ? ((this.cacheFlusher) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9383d9ac-29b1-30b9-9034-2de6b85a7610"))) ? ((this.cacheFlusher) == (null)) : (this.cacheFlusher != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("fb8ebb35-d6a9-3578-9f41-796e076c4cfd"))) { setupSignalHandlers(); } else { this.cacheFlusher.interruptIfNecessary(); }
      }
      if (this.compactSplitThread != null) {
        if (KnobRuntime.check(java.util.UUID.fromString("59eac572-aabb-3322-b51b-1e30eb687951"))) { setupSignalHandlers(); } else { this.compactSplitThread.interruptIfNecessary(); }
      }

      // Stop the snapshot and other procedure handlers, forcefully killing all running tasks
      if (((KnobRuntime.check(java.util.UUID.fromString("0881a424-46ff-39cb-b8be-881464904b3a"))) ? ((rspmHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e2a915b3-b519-33d1-a6e2-18534f4298bb"))) ? ((rspmHost) == (null)) : (rspmHost != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("90385831-1d54-3d87-ba58-65a8ee4ef7ed"))) { rspmHost.stop(canUpdateTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("6ce79fb5-10cd-3e75-a410-6893c7969156"))) { rspmHost.stop((canCreateBaseZNode()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("3c863158-7034-3fbb-a72a-e12949a07e6e"))) { rspmHost.stop((cacheTableDescriptor()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("551f02dc-750a-37b7-9bb0-4c54588dc0fa"))) { closeUserRegions(this.abortRequested.get()); } else if (KnobRuntime.check(java.util.UUID.fromString("b3b99a7c-aeb5-3b1c-9e16-e97714a9822d"))) { closeUserRegions((cacheTableDescriptor()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("30d8ed71-0a85-3e92-9bfe-9743244e868f"))) { closeUserRegions((canUpdateTableDescriptor()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("c71ece25-700f-30fd-82ce-636fe7037983"))) { rspmHost.stop((cacheTableDescriptor()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("27a218ed-8763-3051-9181-614cdb2d453f"))) { closeUserRegions((this.abortRequested.get()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("3c921a3d-b502-3e62-bf1f-27b90e729a9d"))) { closeUserRegions((this.abortRequested.get()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("42306353-17b7-39c0-8bbc-f244608b8046"))) { rspmHost.stop((canUpdateTableDescriptor()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("683a8a8e-169e-3266-90a6-38628db0ebf6"))) { rspmHost.stop((cacheTableDescriptor()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("44f0b900-dfbc-3046-818a-8f922ec5c68a"))) { closeUserRegions(canCreateBaseZNode()); } else if (KnobRuntime.check(java.util.UUID.fromString("f6a7187c-2796-3dd3-b44f-346051e62044"))) { closeUserRegions((canUpdateTableDescriptor()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("5ec91c1a-d66b-301d-a252-e482e68a52fd"))) { rspmHost.stop(this.abortRequested.get()); } else if (KnobRuntime.check(java.util.UUID.fromString("7049d536-2b5b-3600-aa2d-d97e26689f14"))) { closeUserRegions(canUpdateTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("21f6fa94-dd3a-38a3-ab0d-014a73f2f2ef"))) { closeUserRegions((canUpdateTableDescriptor()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("d0dd3421-6c5c-3468-8b66-721e646bea60"))) { rspmHost.stop(!this.killed); } else if (KnobRuntime.check(java.util.UUID.fromString("14e29d4b-6d64-3c3b-ba81-4e2d296812be"))) { rspmHost.stop((canCreateBaseZNode()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("5df99603-1edf-3c45-87c8-91b00cef5228"))) { closeUserRegions((canCreateBaseZNode()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("7efad9bd-7841-3616-a60e-b0a85856b410"))) { rspmHost.stop(cacheTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("a6a6f2af-3435-395b-bcaf-d1cf527b0f25"))) { closeUserRegions((canCreateBaseZNode()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("f8e7149c-079f-3d6d-aab1-91402641a2a7"))) { closeUserRegions((this.abortRequested.get()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("d0cbee25-f4ad-3383-8155-76156975316a"))) { rspmHost.stop((this.abortRequested.get()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("aef49862-08d5-3b50-9548-685e87f65e8c"))) { rspmHost.stop((this.abortRequested.get()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("f64a09bb-7c25-318e-9352-bba2cb9ab1d0"))) { rspmHost.stop((canCreateBaseZNode()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("be525bd4-6266-30fa-a932-be615d46cb56"))) { rspmHost.stop(this.killed); } else if (KnobRuntime.check(java.util.UUID.fromString("341b2703-3f52-3588-af58-5a2b83fbd76c"))) { closeUserRegions((cacheTableDescriptor()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("46da8d15-5439-3d3a-8026-5b439070bb1a"))) { closeUserRegions(!this.killed); } else if (KnobRuntime.check(java.util.UUID.fromString("c326946e-5680-36f8-83df-70a8cbb43ee7"))) { rspmHost.stop((canUpdateTableDescriptor()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("08a43e23-3324-3bb4-84b3-99ece1445e02"))) { closeUserRegions((cacheTableDescriptor()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("1a139936-7767-3af5-a2e3-cc8642662bb0"))) { rspmHost.stop((canUpdateTableDescriptor()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("6742d61d-3044-306f-a6fb-69ae4d8a76ef"))) { closeUserRegions((canUpdateTableDescriptor()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("802d4fef-60a8-30d2-bf09-2028fc52a613"))) { rspmHost.stop(canCreateBaseZNode()); } else if (KnobRuntime.check(java.util.UUID.fromString("8cfbdb43-01b4-37a9-bb8b-fce11da0a27a"))) { rspmHost.stop((this.abortRequested.get()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("5dcbbd59-1399-393f-99ff-045d978dc173"))) { closeUserRegions((canCreateBaseZNode()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("67538c09-7b1c-376a-aa01-0b52ab2c644a"))) { rspmHost.stop((this.abortRequested.get()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("c50a2da0-5658-364a-a9e6-d228b3fbacfb"))) { closeUserRegions((this.abortRequested.get()) || (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("fd791590-6d7b-3eb5-ac8d-c54c11856232"))) { closeUserRegions(this.killed); } else if (KnobRuntime.check(java.util.UUID.fromString("751fc9e7-ea09-3efd-91e8-bd4a3bc4bc1d"))) { rspmHost.stop((cacheTableDescriptor()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("9565c31b-643a-3838-ace8-08f94c1763c2"))) { rspmHost.stop((canCreateBaseZNode()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("a19bdc2c-4eaa-36c5-b54a-dbea201d80ed"))) { closeUserRegions((canCreateBaseZNode()) && (this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("ea92ebb7-7d88-37dd-82fe-fcae84d2d513"))) { closeUserRegions((cacheTableDescriptor()) || (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("2d9a4f13-a050-319c-8561-bb4ef90898d5"))) { rspmHost.stop((canUpdateTableDescriptor()) && (!this.killed)); } else if (KnobRuntime.check(java.util.UUID.fromString("bef60db0-f128-351a-bc5f-dcfd8b895e23"))) { closeUserRegions(cacheTableDescriptor()); } else { rspmHost.stop(this.abortRequested.get() || this.killed); }
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("eec3c4bd-4110-3517-b9e7-afd087111531"))) ? (!this.killed) : (this.killed))) {
        // Just skip out w/o closing regions. Used when testing.
      } else if (abortRequested.get()) {
        if (this.dataFsOk) {
          if (KnobRuntime.check(java.util.UUID.fromString("a5693bc2-a121-34e6-a5d7-939378f53ed5"))) { closeUserRegions(cacheTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("c1785567-1307-36a3-b3f8-5c8e6eeaea57"))) { closeUserRegions(canCreateBaseZNode()); } else if (KnobRuntime.check(java.util.UUID.fromString("ea779dc5-2fe9-3856-86ca-e2ac314b39ec"))) { closeUserRegions(canUpdateTableDescriptor()); } else { closeUserRegions(abortRequested.get()); } // Don't leave any open file handles
        }
        if (KnobRuntime.check(java.util.UUID.fromString("ca6951b0-62bd-38b2-a51b-e01282e716ff"))) { LOG.info("aborting server "); } else if (KnobRuntime.check(java.util.UUID.fromString("2fcdf06b-6d26-318a-80a4-e70f6a9bca0a"))) { LOG.error("aborting server "); } else if (KnobRuntime.check(java.util.UUID.fromString("d46fb89f-a9c5-3f7d-abe4-40db3bc75836"))) { LOG.error(("aborting server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("f3256c18-1264-33f0-bbd9-75f8e4cc0545"))) { LOG.info(("aborting server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("e0eda44a-e6d5-33d8-8ea6-cae75ce09fd9"))) { LOG.debug(("aborting server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("966f5e4c-5b02-3cf2-909b-dcac520e34ec"))) { LOG.debug("aborting server "); } else { LOG.info("aborting server " + this.serverName); }
      } else {
        if (KnobRuntime.check(java.util.UUID.fromString("d51d3b80-ea62-3b5f-909a-b458067b0748"))) { closeUserRegions(cacheTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("fa824320-9f59-33d0-a84a-eee819f3c833"))) { closeUserRegions(canUpdateTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("37e5166e-66f0-3a53-ae22-ca66a508497f"))) { closeUserRegions(canCreateBaseZNode()); } else { closeUserRegions(abortRequested.get()); }
        if (KnobRuntime.check(java.util.UUID.fromString("062ccc17-baa1-361c-97af-5420dbeef393"))) { LOG.info("stopping server "); } else if (KnobRuntime.check(java.util.UUID.fromString("dfe864b0-84e8-3312-a09f-9c087b1569be"))) { LOG.info(("stopping server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("3acaf8b0-5783-3155-bf87-281ca956ba93"))) { LOG.error("stopping server "); } else if (KnobRuntime.check(java.util.UUID.fromString("ff4f4ef4-dce9-3fd5-a1e8-e017ea91dc82"))) { LOG.error(("stopping server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("66d0c1e8-7a13-33aa-b499-f2850a03b443"))) { LOG.debug(("stopping server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("b689e863-c8ea-3b99-9188-bf0385b0c1fc"))) { LOG.debug("stopping server "); } else { LOG.info("stopping server " + this.serverName); }
      }

      if (this.clusterConnection != null && !clusterConnection.isClosed()) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("36bc1ade-3cf6-3a4e-8357-01aaa8087921"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("00dfee36-a45a-32cd-82f1-fe6b5103fcf2"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("f7c51c2e-8af8-3477-a06c-dc37ef59cd49"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("68b9597c-5a2b-36ba-903e-0a5df6e8dd1c"))) { initializeFileSystem(); } else { this.clusterConnection.close(); }
        } catch (IOException e) {
          // Although the {@link Closeable} interface throws an {@link
          // IOException}, in reality, the implementation would never do that.
          if (KnobRuntime.check(java.util.UUID.fromString("ebc73e52-197f-3fa2-adf1-25b02145ff8d"))) { abort("Attempt to close server's short circuit ClusterConnection failed.", e); } else if (KnobRuntime.check(java.util.UUID.fromString("e2062ff4-c700-352b-842d-4e67a6fbea21"))) { LOG.error("Attempt to close server's short circuit ClusterConnection failed.", e); } else { LOG.warn("Attempt to close server's short circuit ClusterConnection failed.", e); }
        }
      }

      // Closing the compactSplit thread before closing meta regions
      if (!this.killed && containsMetaTableRegions()) {
        if (!abortRequested.get() || this.dataFsOk) {
          if (this.compactSplitThread != null) {
            if (KnobRuntime.check(java.util.UUID.fromString("4841efd6-bb41-3170-8f10-70cc717155a1"))) { setupSignalHandlers(); } else { this.compactSplitThread.join(); }
            this.compactSplitThread = null;
          }
          if (KnobRuntime.check(java.util.UUID.fromString("b4120159-8025-3c18-b753-d89c6b291f40"))) { closeUserRegions(canUpdateTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("7e714e8d-1c81-3162-921c-fc382a05d594"))) { closeUserRegions(cacheTableDescriptor()); } else if (KnobRuntime.check(java.util.UUID.fromString("b43a1eb0-d7d4-3992-8608-eb84570c1598"))) { closeUserRegions(canCreateBaseZNode()); } else { closeMetaTableRegions(abortRequested.get()); }
        }
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("ddf489ef-f1bd-39ea-ae3f-7c7a799622b9"))) ? ((!this.killed) && (!this.dataFsOk)) : (((KnobRuntime.check(java.util.UUID.fromString("72dacc90-6ae6-3acd-b4ce-105027c3733c"))) ? ((!this.killed) || (!this.dataFsOk)) : (((KnobRuntime.check(java.util.UUID.fromString("75e30593-ac42-340b-97e6-577a1b74543e"))) ? ((!this.killed) && (this.dataFsOk)) : (((KnobRuntime.check(java.util.UUID.fromString("ad9da656-61f4-3856-b971-51a00e160f71"))) ? (!this.killed) : (((KnobRuntime.check(java.util.UUID.fromString("dd26c99a-c53e-34c1-afee-529d3abf21e9"))) ? (!this.dataFsOk) : (((KnobRuntime.check(java.util.UUID.fromString("128a80c6-030e-3fd3-9a10-c80a207135fc"))) ? (this.dataFsOk) : (((KnobRuntime.check(java.util.UUID.fromString("5444bbe6-a8f0-310c-809c-3e3b0b65c321"))) ? ((!this.killed) || (this.dataFsOk)) : (!this.killed && this.dataFsOk))))))))))))))) {
        waitOnAllRegionsToClose(abortRequested.get());
        if (KnobRuntime.check(java.util.UUID.fromString("d99379a3-0143-3b5f-ad77-39f935fb1235"))) { LOG.error(("stopping server " + this.serverName) + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("57c19da7-852e-30dc-a98b-ed407ae03aed"))) { LOG.debug(("stopping server " + this.serverName) + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("ced9fc97-07fc-3807-8443-cd653f7f20c0"))) { LOG.error((("stopping server ") + (this.serverName)) + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("af039cf7-59f6-337d-8d52-b9d10d7fb403"))) { LOG.info(("stopping server ") + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("4d43921b-275f-3f07-9087-639688948f30"))) { LOG.info((("stopping server ") + (this.serverName)) + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("487066be-4aef-3fc6-8be7-11413d992385"))) { LOG.error("stopping server "); } else if (KnobRuntime.check(java.util.UUID.fromString("28c73dd5-728f-3a3b-bf0a-bc519f4505a2"))) { LOG.info(("stopping server " + this.serverName) + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("367bfbe0-023d-3d96-aadd-455abc422f92"))) { LOG.info("stopping server "); } else if (KnobRuntime.check(java.util.UUID.fromString("cc1b31ab-bcb6-303c-98c0-42b2690a652a"))) { LOG.error(("stopping server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("b70c6913-155d-3e07-8f79-c30e5818353c"))) { LOG.debug("stopping server " + this.serverName); } else if (KnobRuntime.check(java.util.UUID.fromString("1bf5c65a-fe1f-3e22-90d3-25be0d4b145a"))) { LOG.debug(("stopping server ") + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("9fd324b3-c46e-3cb4-8057-a98b0b485710"))) { LOG.debug("stopping server "); } else if (KnobRuntime.check(java.util.UUID.fromString("498530e4-6da9-3756-9d47-781b4aa58487"))) { LOG.debug("; all regions closed."); } else if (KnobRuntime.check(java.util.UUID.fromString("cd521b8c-06dc-38be-88fc-a6e092e27e4f"))) { LOG.info("stopping server " + this.serverName); } else if (KnobRuntime.check(java.util.UUID.fromString("fe4f3f73-6880-376f-8251-0955e8d65796"))) { LOG.info("; all regions closed."); } else if (KnobRuntime.check(java.util.UUID.fromString("8dc8dc45-6a1f-379e-a679-4265a504b51b"))) { LOG.error("stopping server " + this.serverName); } else if (KnobRuntime.check(java.util.UUID.fromString("667191e5-58e4-3739-a1d1-442f967be8b0"))) { LOG.error(("stopping server ") + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("ac618fc7-9b9e-35c4-afdb-3b14e7bb29b0"))) { LOG.error("; all regions closed."); } else if (KnobRuntime.check(java.util.UUID.fromString("33bee229-53f8-3ce4-855d-0c5f612b792e"))) { LOG.debug((("stopping server ") + (this.serverName)) + ("; all regions closed.")); } else if (KnobRuntime.check(java.util.UUID.fromString("eee9b8b5-cf3c-3f98-aebe-52c25874130c"))) { LOG.info(("stopping server ") + (this.serverName)); } else if (KnobRuntime.check(java.util.UUID.fromString("0a7636fc-c2f6-31ec-b0d9-3e64b39d1193"))) { LOG.debug(("stopping server ") + (this.serverName)); } else { LOG.info("stopping server " + this.serverName + "; all regions closed."); }
      }

      // Stop the quota manager
      if (rsQuotaManager != null) {
        rsQuotaManager.stop();
      }
      if (rsSpaceQuotaManager != null) {
        rsSpaceQuotaManager.stop();
        rsSpaceQuotaManager = null;
      }

      // flag may be changed when closing regions throws exception.
      if (this.dataFsOk) {
        shutdownWAL(!abortRequested.get());
      }

      // Make sure the proxy is down.
      if (this.rssStub != null) {
        this.rssStub = null;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("df5f4341-65c1-3ad9-bdf0-0b4249ea7de2"))) ? ((this.lockStub) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3e832e24-91ae-3677-ab33-126348454714"))) ? ((this.lockStub) == (null)) : (this.lockStub != null))))) {
        this.lockStub = null;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("5e4ed26c-26be-3bd2-826a-8452ed52e06b"))) ? ((this.rpcClient) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7f757496-63f5-3371-8c71-d765a1d3e7c6"))) ? ((this.rpcClient) != (null)) : (this.rpcClient != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("6de2f44f-11cd-3117-8577-6a980bf7d673"))) { setupSignalHandlers(); } else { this.rpcClient.close(); }
      }
      if (this.leaseManager != null) {
        this.leaseManager.close();
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("615f31f2-ff59-3f82-b217-78cd099ea79a"))) ? ((this.pauseMonitor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4449b475-8f92-3a52-80f3-064c298a1649"))) ? ((this.pauseMonitor) == (null)) : (this.pauseMonitor != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("7b9736f5-928f-31fd-b8ec-06c3d8028f04"))) { setupSignalHandlers(); } else { this.pauseMonitor.stop(); }
      }

      if (!killed) {
        stopServiceThreads();
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("02cf8e23-5128-3d97-baa1-e39f147178db"))) ? ((this.rpcServices) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("35d2bef7-9167-31ab-bb4d-49fb4da7f1dc"))) ? ((this.rpcServices) != (null)) : (this.rpcServices != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("f32b112c-14ca-3ab8-b0ae-b5fe75454c28"))) { setupSignalHandlers(); } else { this.rpcServices.stop(); }
      }

      try {
        if (KnobRuntime.check(java.util.UUID.fromString("96d304fa-b885-359f-a8ca-598bdb74cee8"))) { setupSignalHandlers(); } else { deleteMyEphemeralNode(); }
      } catch (KeeperException.NoNodeException nn) {
        // pass
      } catch (KeeperException e) {
        LOG.warn("Failed deleting my ephemeral node", e);
      }
      // We may have failed to delete the znode at the previous step, but
      // we delete the file anyway: a second attempt to delete the znode is likely to fail again.
      ZNodeClearer.deleteMyEphemeralNodeOnDisk();

      if (this.zooKeeper != null) {
        this.zooKeeper.close();
      }
      this.shutDown = true;
      LOG.info("Exiting; stopping=" + this.serverName + "; zookeeper connection closed.");
      span.setStatus(StatusCode.OK);
    } finally {
      span.end();
    }
  }

  /**
   * This method is called when HMaster and HRegionServer are started. Please see to HBASE-26977 for
   * details.
   */
  private void installShutdownHook() {
    ShutdownHook.install(conf, dataFs, this, Thread.currentThread());
    isShutdownHookInstalled = true;
  }

  /**
   * This method is used for testing.
   */
  public boolean isShutdownHookInstalled() {
    return isShutdownHookInstalled;
  }

  private boolean containsMetaTableRegions() {
    return onlineRegions.containsKey(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedName());
  }

  private boolean areAllUserRegionsOffline() {
    if (getNumberOfOnlineRegions() > 2) {
      return false;
    }
    boolean allUserRegionsOffline = true;
    for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
      if (!e.getValue().getRegionInfo().isMetaRegion()) {
        allUserRegionsOffline = false;
        break;
      }
    }
    return allUserRegionsOffline;
  }

  /** Returns Current write count for all online regions. */
  private long getWriteRequestCount() {
    long writeCount = 0;
    for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
      writeCount += e.getValue().getWriteRequestsCount();
    }
    return writeCount;
  }

  @InterfaceAudience.Private
  protected void tryRegionServerReport(long reportStartTime, long reportEndTime)
    throws IOException {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (((KnobRuntime.check(java.util.UUID.fromString("2a0e89ab-6e4b-314e-81f1-719ea187d6d3"))) ? ((rss) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a07e08ed-20ca-3078-a453-9aeb620064e3"))) ? ((rss) != (null)) : (rss == null))))) {
      // the current server could be stopping.
      return;
    }
    ClusterStatusProtos.ServerLoad sl = buildServerLoad(reportStartTime, reportEndTime);
    final Span span = TraceUtil.createSpan("HRegionServer.tryRegionServerReport");
    try (Scope ignored = span.makeCurrent()) {
      RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();
      request.setServer(ProtobufUtil.toServerName(this.serverName));
if(KnobRuntime.check(java.util.UUID.fromString("4a04a004-4baa-3203-8db8-5e06ab17d1cc"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("infoServerPort_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46e88092-fd16-3d62-8868-297d8ac7b0fa"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("reportStartTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f05679b5-92e8-36ec-b718-f02ba30a0527"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b55f6e75-7d2f-34af-86e0-c242cc5dca12"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("maxHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f7b5ab5-129d-311e-938d-5d798a256a2a"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("074ab754-0035-3d23-b0fa-7e6b04baa337"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("infoServerPort_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5cee386-8369-3234-99a1-32f970579da1"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("maxHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5249d5b-eb39-3a31-9522-dce181fe1193"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("infoServerPort_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b12b78f6-d3b0-3cb6-bd6d-7e47987b6d4c"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("maxHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27a243f1-05ad-3941-9124-0d9f213a591e"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("totalNumberOfRequests_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("960dd5e2-3631-3832-9f91-675ffcf17b65"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("usedHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ab846de-1e81-33db-b185-31a2623b2f24"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("usedHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("300b6055-89a4-3b43-9a0f-6e65095a1df7"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("usedHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7dc36a67-00b6-33fc-a467-077d30196d3f"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("usedHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3d8c252-5ce0-31fa-a4ce-83af15a0e181"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("maxHeapMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1529f17d-e87c-36d7-bb91-9bb1441ca68c"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("numberOfRequests_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38b9edf6-2a6b-33e7-96d2-879722ae2f99"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93e256c5-efa8-324a-bb9c-5d207965ebdf"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("reportEndTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sl));
    field.set(sl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("23263905-ebcb-3de5-ac84-7beb86c8f7bf"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("infoServerPort_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("492aeeb5-d5ab-32a7-80f7-552f52dd4320"))) {
try {
    java.lang.reflect.Field field = sl.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sl));
    field.set(sl, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      request.setLoad(sl);
      rss.regionServerReport(null, request.build());
      span.setStatus(StatusCode.OK);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        TraceUtil.setError(span, ioe);
        throw ioe;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      TraceUtil.setError(span, se);
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      createRegionServerStatusStub(true);
    } finally {
      span.end();
    }
  }

  /**
   * Reports the given map of Regions and their size on the filesystem to the active Master.
   * @param regionSizeStore The store containing region sizes
   * @return false if FileSystemUtilizationChore should pause reporting to master. true otherwise
   */
  public boolean reportRegionSizesForQuotas(RegionSizeStore regionSizeStore) {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null) {
      // the current server could be stopping.
      LOG.trace("Skipping Region size report to HMaster as stub is null");
      return true;
    }
    try {
      buildReportAndSend(rss, regionSizeStore);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof PleaseHoldException) {
        LOG.trace("Failed to report region sizes to Master because it is initializing."
          + " This will be retried.", ioe);
        // The Master is coming up. Will retry the report later. Avoid re-creating the stub.
        return true;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      createRegionServerStatusStub(true);
      if (ioe instanceof DoNotRetryIOException) {
        DoNotRetryIOException doNotRetryEx = (DoNotRetryIOException) ioe;
        if (doNotRetryEx.getCause() != null) {
          Throwable t = doNotRetryEx.getCause();
          if (t instanceof UnsupportedOperationException) {
            if (KnobRuntime.check(java.util.UUID.fromString("d85a91a2-4d1f-3127-86ec-1eccac0bd9ab"))) { LOG.error("master doesn't support ReportRegionSpaceUse, pause before retrying"); } else if (KnobRuntime.check(java.util.UUID.fromString("f0c3ae3f-c26a-33d1-a348-35645b2dcdcf"))) { LOG.info("master doesn't support ReportRegionSpaceUse, pause before retrying"); } else { LOG.debug("master doesn't support ReportRegionSpaceUse, pause before retrying"); }
            return false;
          }
        }
      }
      LOG.debug("Failed to report region sizes to Master. This will be retried.", ioe);
    }
    return true;
  }

  /**
   * Builds the region size report and sends it to the master. Upon successful sending of the
   * report, the region sizes that were sent are marked as sent.
   * @param rss             The stub to send to the Master
   * @param regionSizeStore The store containing region sizes
   */
  private void buildReportAndSend(RegionServerStatusService.BlockingInterface rss,
    RegionSizeStore regionSizeStore) throws ServiceException {
    RegionSpaceUseReportRequest request =
      buildRegionSpaceUseReportRequest(Objects.requireNonNull(regionSizeStore));
    rss.reportRegionSpaceUse(null, request);
    // Record the number of size reports sent
    if (metricsRegionServer != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("32b950fd-6168-35b8-bee6-2b36b61c8a9f"))) { Threads.sleep(getNumberOfOnlineRegions()); } else if (KnobRuntime.check(java.util.UUID.fromString("8ba6afad-45ff-30a1-904e-2f3828d7b508"))) { Threads.sleep(movedRegionCacheExpiredTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("bf247e0f-e6d0-321b-92d6-faa01a63fb59"))) { metricsRegionServer.incrementNumRegionSizeReportsSent(getNumberOfOnlineRegions()); } else if (KnobRuntime.check(java.util.UUID.fromString("60e2213a-43cc-341a-ab3f-8ccfc82ce939"))) { metricsRegionServer.incrementNumRegionSizeReportsSent(movedRegionCacheExpiredTime()); } else { metricsRegionServer.incrementNumRegionSizeReportsSent(regionSizeStore.size()); }
    }
  }

  /**
   * Builds a {@link RegionSpaceUseReportRequest} protobuf message from the region size map.
   * @param regionSizes The size in bytes of regions
   * @return The corresponding protocol buffer message.
   */
  RegionSpaceUseReportRequest buildRegionSpaceUseReportRequest(RegionSizeStore regionSizes) {
    RegionSpaceUseReportRequest.Builder request = RegionSpaceUseReportRequest.newBuilder();
    for (Entry<RegionInfo, RegionSize> entry : regionSizes) {
      if (KnobRuntime.check(java.util.UUID.fromString("ee75a052-6569-3608-8d25-7d17b382a95a"))) { request.addSpaceUse(convertRegionSize(entry.getKey(), EnvironmentEdgeManager.currentTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("1970ac8a-fe01-3315-9986-6f285c6b1e3a"))) { request.addSpaceUse(convertRegionSize(entry.getKey(), getWriteRequestCount())); } else { request.addSpaceUse(convertRegionSize(entry.getKey(), entry.getValue().getSize())); }
    }
    return request.build();
  }

  /**
   * Converts a pair of {@link RegionInfo} and {@code long} into a {@link RegionSpaceUse} protobuf
   * message.
   * @param regionInfo  The RegionInfo
   * @param sizeInBytes The size in bytes of the Region
   * @return The protocol buffer
   */
  RegionSpaceUse convertRegionSize(RegionInfo regionInfo, Long sizeInBytes) {
if(KnobRuntime.check(java.util.UUID.fromString("cd06ef8d-9aac-3fbd-bcee-141ab3f0263c"))) {
return null;
}
    return RegionSpaceUse.newBuilder()
      .setRegionInfo(ProtobufUtil.toRegionInfo(Objects.requireNonNull(regionInfo)))
      .setRegionSize(Objects.requireNonNull(sizeInBytes)).build();
  }

  private ClusterStatusProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("38e135af-3f54-34cf-9a84-89f530ab0dd8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e7f774fa-b323-3759-80cc-ecc9df12503c"))) {
return null;
}
    // We're getting the MetricsRegionServerWrapper here because the wrapper computes requests
    // per second, and other metrics As long as metrics are part of ServerLoad it's best to use
    // the wrapper to compute those numbers in one place.
    // In the long term most of these should be moved off of ServerLoad and the heart beat.
    // Instead they should be stored in an HBase table so that external visibility into HBase is
    // improved; Additionally the load balancer will be able to take advantage of a more complete
    // history.
    MetricsRegionServerWrapper regionServerWrapper = metricsRegionServer.getRegionServerWrapper();
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    long usedMemory = -1L;
    long maxMemory = -1L;
    final MemoryUsage usage = MemorySizeUtil.safeGetHeapMemoryUsage();
    if (usage != null) {
      usedMemory = usage.getUsed();
      maxMemory = usage.getMax();
    }

    ClusterStatusProtos.ServerLoad.Builder serverLoad = ClusterStatusProtos.ServerLoad.newBuilder();
    serverLoad.setNumberOfRequests((int) regionServerWrapper.getRequestsPerSecond());
    serverLoad.setTotalNumberOfRequests(regionServerWrapper.getTotalRequestCount());
    serverLoad.setUsedHeapMB((int) (usedMemory / 1024 / 1024));
    if (KnobRuntime.check(java.util.UUID.fromString("19ad096c-d4e7-3b47-b7d4-04cd6482e136"))) { serverLoad.setInfoServerPort((int) (maxMemory / 1024 / 1024)); } else if (KnobRuntime.check(java.util.UUID.fromString("d3539c68-48f9-3e02-9d0b-2081290ceb39"))) { serverLoad.setUsedHeapMB((int) (maxMemory / 1024 / 1024)); } else { serverLoad.setMaxHeapMB((int) (maxMemory / 1024 / 1024)); }
    Set<String> coprocessors = getWAL(null).getCoprocessorHost().getCoprocessors();
    Coprocessor.Builder coprocessorBuilder = Coprocessor.newBuilder();
    for (String coprocessor : coprocessors) {
      serverLoad.addCoprocessors(coprocessorBuilder.setName(coprocessor).build());
    }
    RegionLoad.Builder regionLoadBldr = RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();
    for (HRegion region : ((KnobRuntime.check(java.util.UUID.fromString("c2c05d86-fbca-3daf-b844-06957a9fa514"))) ? (getOnlineRegionsLocalContext()) : (regions))) {
      if (region.getCoprocessorHost() != null) {
        Set<String> regionCoprocessors = region.getCoprocessorHost().getCoprocessors();
        for (String regionCoprocessor : regionCoprocessors) {
          serverLoad.addCoprocessors(coprocessorBuilder.setName(regionCoprocessor).build());
        }
      }
if(KnobRuntime.check(java.util.UUID.fromString("4d051e05-5270-389f-a3ab-262095052c52"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79b92a3c-f34f-3264-9ef4-6fa2e86f1494"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbbbc1a4-b097-3267-a49e-c30b08534d14"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5146c837-0d0d-3894-9134-29a45e7034d4"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c20ecb7-4da4-3fd1-a145-b4e5de0d4f96"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("556545b5-70b1-3369-86c7-9007c28e3533"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8b1a09e-8fbe-3f94-983a-778f28d35212"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8926e91d-3ebf-3a13-9fd0-a809306f4e71"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("656c80ba-8c88-33bf-910f-ab07b364c978"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba9c3ae1-bb69-3b32-ae9c-4ae2d25dd727"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66902b8e-1f24-3ed1-b8da-782743f2e34b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea460706-b9e0-3540-a693-162f207bc993"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5944b2e4-c8c0-37df-8640-ada2ea3ad468"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b98de4e-5f34-33e1-866f-a7f77f5e5a94"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af10a8d0-358f-334e-92a5-73051ba78297"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9147c2d2-0af2-3654-a0af-2cd88e676858"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b790063c-1fdc-3efc-98d9-3b1e554eae38"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("lastMajorCompactionTs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45649b19-1bb9-3462-8cf0-1ee66a1c9796"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5b8a549-0a6d-36a1-bc0b-49f6698c6e7b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bb7530e5-106c-35d1-8344-4eae1d9af9d5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4dbb12ad-b0eb-34fa-8e78-25b2c4388bfa"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cad4d84d-7f99-3ba5-bb28-c01b780f4e1f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f663de29-dba0-3fff-9e4f-785d1405123e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1593dd44-0141-3b06-9f8e-b0b432c8f298"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53fd0555-73d8-3cab-ab31-57812a2f2685"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0ff0536-96ed-3285-af24-59f55bb8c501"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksTotalWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0b4de70-2f10-3e24-a514-c0c8393523df"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57809ef6-e260-3fec-aadb-74154274db86"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("893dbe63-811e-3e74-9e46-bd2378a8553c"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c1c79cc-f046-3d7e-bfab-77e25a6be924"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efe63671-73b1-3781-ade9-a8e169655196"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e20541b-094b-35a3-9f50-694a987525bb"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("959e7f4d-4cda-30c4-b824-d6b783dfef95"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("81530627-6f16-3854-9aab-5e5d0821a24c"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c3633f15-b5ca-3dae-9117-9fe95af3b71f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c033689c-914f-3746-8346-643330db90c0"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("currentCompactedKVs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3fb8f67-0553-342a-bbd6-d7178d4673a8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f06ba7da-04dc-3639-9cc2-24dce0d49a58"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f223ef0c-0aa6-391a-9342-7ed822ddb558"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c9f4ff2-1892-3d5c-8e20-2a5837b048c1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2bee4b44-82df-3290-89cc-8a311edd7921"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0be88678-3b3a-3711-af6b-fbc53749160b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a821a86-279c-3c7f-b4c6-71666a4f9c53"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d84f1759-9a16-3be0-8ec7-74c3080ae097"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86e4446e-b045-322a-a7a3-63c4b7be5b5e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3fe27865-94a2-31c4-af54-29d9b1e31a6b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c46567c7-7783-39ce-915c-91d031e4a361"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e22367d4-dc04-3b5f-8c07-b44e79590c63"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("94e72fd0-8835-398d-96f8-a27a3077263e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f88785a-208b-3e41-9939-64e0cb3a189e"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33e49701-cf8e-36a9-813c-0502be3275d9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5253d2e6-29a9-3c6e-a1b6-22b86ab2380b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ec24105-6a27-3e53-abdd-16939aaa6568"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f48b888b-a1ea-37e3-937b-cb0608715dc1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("readRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bc19a742-7806-37c9-b58c-ba89fc06ce68"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("writeRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3aabc90c-bb73-3b57-b4ec-b5e430456da2"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66af806f-b07f-3e8d-aa77-7535443a5725"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalCompactingKVs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac87b870-e09f-389a-a183-1c2717db3ba3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e1d191a-170a-344a-ba8c-fd9a9f7d0414"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41d00431-e65e-33d9-b7fd-80c64deb1e08"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksLocalWithSsdWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77daaef8-4cc1-3ceb-8f70-0b6120360d99"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fea35fd8-5343-307c-adfb-97792fb2be68"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30afde42-e698-3c89-9211-dae772a92710"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8b084a5-0626-3c81-848c-4642b6a2c2b2"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79e02552-dc9f-3955-bbab-fb51239dbcf7"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd4d1740-92a8-33dd-926a-a9db478f9e08"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e804c304-88d4-3357-b586-0b9811bc7a05"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25acd5ac-e36f-30ae-8c3f-71a4f1b62417"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35d8bcad-786f-3bcc-9f42-1ef5ddca9d84"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a551a485-4a8e-3bd4-95d1-b69efa2b8db7"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3998a92-c5f9-3d3c-9bdb-5facfe5a5fa1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba91c09e-d793-331f-8784-d02f754aa07f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67bc1009-8070-3c97-8489-6ca38ad33cc5"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d1d7d5f-bd33-30ce-998e-cbfd223000ba"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("983fa7db-4160-3d60-a13d-a95aaef21179"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36d0bb97-1154-301c-9916-2858f326971a"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a20c2ba5-0903-347b-90d9-c790c70e70d5"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("completeSequenceId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c55c655-58d0-3f1b-b45d-8c338a619933"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9fe1323-a009-3fdb-a7c6-d459dbf7dc8f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6c662d7-144e-387a-8e26-e5d85233e37d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("34f8b017-2ebc-3489-a85e-21e7913ca905"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b3841f2-92e3-3107-90c0-d9bd39367571"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d7eb021a-fbf1-33da-932b-83bf635295de"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f318520-5957-3a4f-baea-a78a9199ada0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("648ef0e5-5536-3d4f-bf00-333d871fba0c"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3bde10ea-5df8-3ecb-80da-1e89de1cd64f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksLocalWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e506714-e9c8-38fe-b4a6-9d5ca461c9a4"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64ea2378-9b98-3831-8ab5-91f3f93644b2"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ee2bc85c-af46-3bc7-a3c5-7831a359453c"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("881d63d9-e1b2-328f-a09a-802b3961eb34"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("filteredReadRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9def897c-0f70-391d-8072-248ae8af57e7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56eccf76-73ec-3901-a372-879186a304e6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42b613b7-1920-37c2-8ae3-b3d4e0fdfa75"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("571bf22d-5503-34b4-bbdf-865e983491ba"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9026168-7b3e-3346-b69a-e5c4877ae2d2"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6c7dc1f-ddda-37e4-983f-1b8fd47f6a3a"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0e83882-aea2-3ab6-9536-827a728a160d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d208959-c7df-3bd7-8e25-1264aa9ab386"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9794014-c302-30f7-a4c0-e7ea0cb47045"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec2e504a-661d-3e0d-b8cf-d077e814547e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28f30463-3f13-36d6-b361-a478d8be7687"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76906976-824e-3662-b260-b4b190114401"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4a25df4-c72f-3a51-9b15-0ac40fb9a513"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01d70e24-8722-36f3-b932-2ccba2de25fb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5f2be7b-435c-3f0a-a298-6095da336e66"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b7d8168-7954-3002-a13e-e83350649605"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6c2a2bc-ff58-3155-ad44-3bbddc4a930f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a73a3d9-c6c1-36af-88c7-422824955515"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileIndexSizeKB_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f1416e9-803e-3e71-b5e3-58872fbb28c0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      serverLoad.addRegionLoads(createRegionLoad(region, regionLoadBldr, regionSpecifier));
if(KnobRuntime.check(java.util.UUID.fromString("c32eaf46-c3b9-32c1-bb6b-ce806b4a8862"))) {
throw new java.io.IOException("Injected exception");
}
      for (String coprocessor : getWAL(region.getRegionInfo()).getCoprocessorHost()
        .getCoprocessors()) {
        serverLoad.addCoprocessors(coprocessorBuilder.setName(coprocessor).build());
      }
    }

    getBlockCache().ifPresent(cache -> {
      cache.getRegionCachedInfo().ifPresent(regionCachedInfo -> {
        regionCachedInfo.forEach((regionName, prefetchSize) -> {
          serverLoad.putRegionCachedInfo(regionName, roundSize(prefetchSize, unitMB));
        });
      });
    });

    serverLoad.setReportStartTime(reportStartTime);
    serverLoad.setReportEndTime(reportEndTime);
    if (this.infoServer != null) {
      serverLoad.setInfoServerPort(this.infoServer.getPort());
    } else {
      if (KnobRuntime.check(java.util.UUID.fromString("ab54472d-5860-3b6e-927f-c27df5477e2a"))) { serverLoad.setUsedHeapMB(-1); } else if (KnobRuntime.check(java.util.UUID.fromString("ee4a547e-fe2b-3233-8df4-f94e37c00a34"))) { serverLoad.setMaxHeapMB(-1); } else { serverLoad.setInfoServerPort(-1); }
    }
    MetricsUserAggregateSource userSource =
      metricsRegionServer.getMetricsUserAggregate().getSource();
    if (((KnobRuntime.check(java.util.UUID.fromString("ebbbbc35-1ce5-37de-8f2b-f1e2db0db2aa"))) ? ((userSource) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cfff4d1e-5c61-3965-bdd6-c4d70690f23d"))) ? ((userSource) == (null)) : (userSource != null))))) {
      Map<String, MetricsUserSource> userMetricMap = userSource.getUserSources();
      for (Entry<String, MetricsUserSource> entry : userMetricMap.entrySet()) {
        serverLoad.addUserLoads(createUserLoad(entry.getKey(), entry.getValue()));
      }
    }

    // for the replicationLoad purpose. Only need to get from one executorService
    // either source or sink will get the same info
    ReplicationSourceService rsources = getReplicationSourceService();
    if (rsources != null) {
      // always refresh first to get the latest value
      ReplicationLoad rLoad = rsources.refreshAndGetReplicationLoad();
      if (rLoad != null) {
        serverLoad.setReplLoadSink(rLoad.getReplicationLoadSink());
        for (ClusterStatusProtos.ReplicationLoadSource rLS : rLoad
          .getReplicationLoadSourceEntries()) {
          serverLoad.addReplLoadSource(rLS);
        }

      }
    }

    TaskMonitor.get().getTasks().forEach(task -> serverLoad.addTasks(ClusterStatusProtos.ServerTask
      .newBuilder().setDescription(task.getDescription())
      .setStatus(task.getStatus() != null ? task.getStatus() : "")
      .setState(ClusterStatusProtos.ServerTask.State.valueOf(task.getState().name()))
      .setStartTime(task.getStartTime()).setCompletionTime(task.getCompletionTimestamp()).build()));

    return serverLoad.build();
  }

  private String getOnlineRegionsAsPrintableString() {
if(KnobRuntime.check(java.util.UUID.fromString("573894b1-3e02-330f-a35c-57c086608017"))) {
return null;
}
    StringBuilder sb = new StringBuilder();
    for (Region r : this.onlineRegions.values()) {
      if (((KnobRuntime.check(java.util.UUID.fromString("db9213cd-db4f-3c74-b257-4ed157281c94"))) ? ((sb.length()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("938e5736-e6f5-3046-ae5d-e61591a8bbd1"))) ? ((getNumberOfOnlineRegions()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cdee31ae-e843-3c04-a680-0abcd17756cb"))) ? ((movedRegionCacheExpiredTime()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c66a2598-4bd5-33f6-a57a-2d4e7a3cd058"))) ? ((getNumberOfOnlineRegions()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("588e73fc-ad05-30d6-b96d-094e8d0a9b98"))) ? ((getNumberOfOnlineRegions()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("30f60bbf-5fc2-3f95-bdd1-641c13c0f56c"))) ? ((movedRegionCacheExpiredTime()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("36e51db1-526f-341c-aea1-5da8b7c0dc00"))) ? ((sb.length()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a332c146-c05c-32e2-9341-cde82c549d49"))) ? ((getNumberOfOnlineRegions()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ff944394-56a7-3161-a792-3ada5d884805"))) ? ((movedRegionCacheExpiredTime()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("685ff9df-a6a4-33da-9d44-2dfb7fd5710c"))) ? ((movedRegionCacheExpiredTime()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c45f522d-1a45-365a-a6b6-d278d395cfad"))) ? ((getNumberOfOnlineRegions()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8d5268c5-589e-37dc-b20b-3ae384d41c60"))) ? ((sb.length()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0c77ae7c-1fa7-3c30-bf92-252d0d010d16"))) ? ((movedRegionCacheExpiredTime()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("38a27d83-a01b-32b8-8f96-f345699fd69c"))) ? ((sb.length()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e1f95b74-dade-3830-aab8-9c267ff483fb"))) ? ((movedRegionCacheExpiredTime()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2c93272f-2e0a-351c-9c0f-17be25afde20"))) ? ((sb.length()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2ceb4956-60f8-3523-8539-6b8308ee870e"))) ? ((sb.length()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("97d6efc6-b5e5-3446-91a7-664ac7679c3b"))) ? ((getNumberOfOnlineRegions()) <= (0)) : (sb.length() > 0))))))))))))))))))))))))))))))))))))) {
        sb.append(", ");
      }
      if (KnobRuntime.check(java.util.UUID.fromString("0ad45877-3bdb-31fc-a23b-b4ec944ea2c6"))) { sb.append(getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("21457e58-f2e6-3aae-89c5-4dac197a8197"))) { sb.append(getProcessName()); } else { sb.append(r.getRegionInfo().getEncodedName()); }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("b38b6c2e-9f9d-362f-8672-88db619c1191"))) ? (getProcessName()) : (((KnobRuntime.check(java.util.UUID.fromString("60b0427d-f18d-373d-ab2c-8db26e6b711e"))) ? (getName()) : (sb.toString()))));
  }

  /**
   * Wait on regions close.
   */
  private void waitOnAllRegionsToClose(final boolean abort) {
if(KnobRuntime.check(java.util.UUID.fromString("6e7641ea-0fe6-38bf-9015-b8d9d4d506e0"))) {
return;
}
    // Wait till all regions are closed before going out.
    int lastCount = -1;
    long previousLogTime = 0;
    Set<String> closedRegions = new HashSet<>();
    boolean interrupted = false;
    try {
      while (!onlineRegions.isEmpty()) {
        int count = getNumberOfOnlineRegions();
        // Only print a message if the count of regions has changed.
        if (((KnobRuntime.check(java.util.UUID.fromString("56b6ca12-d889-3163-9960-ab989892ce12"))) ? ((getNumberOfOnlineRegions()) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("b99f07f1-fee2-3ae4-abcf-8317fab6d147"))) ? ((getNumberOfOnlineRegions()) == (200)) : (((KnobRuntime.check(java.util.UUID.fromString("0386ae5a-ae44-3842-9b48-65f6045cd3c4"))) ? ((0) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("f6cffb8e-fbb8-3934-9c8c-6413f84629ae"))) ? ((count) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("75517021-9423-3b8f-b676-d04bd18e951b"))) ? ((count) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("fb21de26-f76c-3dc5-b493-24b2c6a50d52"))) ? ((count) != (10)) : (((KnobRuntime.check(java.util.UUID.fromString("ba4c4de9-8905-365c-ac12-719b1711998c"))) ? ((count) == (200)) : (((KnobRuntime.check(java.util.UUID.fromString("18f81adc-9faa-354d-a05d-54121db9933d"))) ? ((getNumberOfOnlineRegions()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c04304aa-1896-3856-9f5b-c4c0ebc1e170"))) ? ((1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("bdcd7c10-4ec6-38f4-b236-4b7157b39cc1"))) ? ((getNumberOfOnlineRegions()) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("7c265abb-1ea5-3496-b28b-faff1069b025"))) ? ((-1) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("5a20d761-a39c-3f61-a953-be5cbaa52148"))) ? ((-1) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("b1406f61-51a3-3324-8859-062b490e16b3"))) ? ((count) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("e5271a92-ec8a-3bfb-8878-308f59bb637f"))) ? ((1000) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c527af09-c3d3-38cb-9444-65df2254afbc"))) ? ((count) == (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("e71a90b4-5269-3786-a8a1-2c011b0503d4"))) ? ((10) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("0f3063d0-58eb-3546-bee6-23827daeef09"))) ? ((-1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("23834ef2-b475-3c9a-800c-eda7fec50417"))) ? ((getNumberOfOnlineRegions()) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("962c9939-c88e-34d5-add2-1a53c564bb88"))) ? ((0) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("4c6c2613-a491-35d9-9359-c4603fcb634f"))) ? ((count) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("13c3b929-031f-3ba5-8d78-f0723dec7795"))) ? ((count) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("b01160c4-08e5-364e-81b8-4bf50fa99fc4"))) ? ((1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("ece87cc1-74f7-3548-9529-563fa23363da"))) ? ((getNumberOfOnlineRegions()) != (200)) : (((KnobRuntime.check(java.util.UUID.fromString("dd3ebe67-dfc0-3f69-9388-c0ed888f202e"))) ? ((getNumberOfOnlineRegions()) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("53f4bcf8-5dc2-3d52-bfe5-75ede680cecb"))) ? ((getNumberOfOnlineRegions()) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("66d7f93b-2422-3b60-9aeb-8eb435fed8ea"))) ? ((0) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("92339e72-05be-356e-b346-46a59082d3d2"))) ? ((getNumberOfOnlineRegions()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("82bbf009-c872-320e-8741-b8eb14f0f07c"))) ? ((-1) == (10)) : (((KnobRuntime.check(java.util.UUID.fromString("25d4573e-51bc-3b32-8dc2-64239ddbfa1d"))) ? ((-1) == (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("53528640-6957-329d-a9d4-355efafcedca"))) ? ((1) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("2d3113e0-1c93-3d13-b8a3-fbde9562219c"))) ? ((1000) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("104ec2ce-f9fa-3eaf-aafe-74ce07251ab7"))) ? ((200) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("b0df716d-024e-3cfc-9d09-170dd33637b9"))) ? ((-1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d95f80f0-1f23-3d3e-a722-9faa78507fb6"))) ? ((200) == (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("1a68b778-6ea5-3622-84a7-ad54f1610405"))) ? ((-1) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("cdaf7c36-4c2d-3336-81f6-6da49c2add25"))) ? ((count) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("a827e3db-2f92-33bf-bf88-ad2470a551bd"))) ? ((count) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("8270e6a4-be9f-3006-ab23-9ac057ae6e76"))) ? ((count) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("20e12262-a509-3d06-a003-f527585a408e"))) ? ((10) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("95973bd7-2d65-3fab-b240-f513e5f328e7"))) ? ((getNumberOfOnlineRegions()) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c938dd17-ab40-371b-83a2-bdb228dc9695"))) ? ((getNumberOfOnlineRegions()) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("495c7d0c-a294-31fa-9001-dc71a7060610"))) ? ((1000) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("fd8086e1-987d-36e3-9c6f-d197f1ce6052"))) ? ((-1) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("83d8adab-f4e6-3ec1-9dc6-79ea67bd31c7"))) ? ((1) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("a5c563ed-df26-3750-9a22-2880071628b5"))) ? ((1) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("c7279bae-9f34-3549-9771-0819bc124cfa"))) ? ((1000) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("b166c8c3-b30f-3e77-b8e2-1ede21d105f9"))) ? ((10) == (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("32a8302a-f56e-3fb2-90e9-471df3377abe"))) ? ((-1) != (10)) : (((KnobRuntime.check(java.util.UUID.fromString("bcb2fea5-8903-334f-a057-feb65150fb35"))) ? ((count) != (200)) : (((KnobRuntime.check(java.util.UUID.fromString("7368f8c3-39f6-3593-a57b-cb9be958d1d0"))) ? ((200) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("003f205c-4eb8-3245-ba22-74a7b4ec2f55"))) ? ((getNumberOfOnlineRegions()) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("32126e60-6fcf-3e48-8341-cc273f053e49"))) ? ((count) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("54ab5b3a-6cc0-34d5-8590-9f7c9c9bc8b3"))) ? ((getNumberOfOnlineRegions()) == (10)) : (((KnobRuntime.check(java.util.UUID.fromString("5ae97c87-5d27-3465-a031-6bf26388bbe3"))) ? ((200) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("1f3d2ec3-17ae-3061-872f-9e1a9929792c"))) ? ((count) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("57974926-b420-30bb-8813-90067c71c173"))) ? ((10) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("a19dc3bc-785f-36c8-80de-e033208356c3"))) ? ((200) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("84677269-4816-3152-8f36-27faa02d9a1f"))) ? ((-1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("be140a64-206e-3385-b74a-090f4bfe62b2"))) ? ((-1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("798cf445-7e1c-3606-9a91-92ffb3f4b681"))) ? ((1000) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("624f9b42-d3bb-3cc6-a90d-6015897c12d9"))) ? ((1000) == (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("09a235b8-539f-30a7-ba64-58da3b39874d"))) ? ((-1) == (200)) : (((KnobRuntime.check(java.util.UUID.fromString("a482bb06-9ef9-314b-a93b-a4ab57f359e6"))) ? ((10) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("870b1962-467c-3bf9-b97a-eafd34acd23e"))) ? ((200) != (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("d94af0d8-6b96-3b73-bfad-f4d56d434239"))) ? ((count) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("04d74793-3f2e-3f8e-bf7e-01e08d01737a"))) ? ((getNumberOfOnlineRegions()) == (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("f562f345-beaf-32d0-bcd3-66e1831fe0d3"))) ? ((10) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("3e2557c1-3ed7-3538-9a09-5ecf49259a0a"))) ? ((getNumberOfOnlineRegions()) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("129c467a-f6c3-3810-b612-1f0d812cb26c"))) ? ((0) == (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("ad404782-7492-3677-9d79-ad625cfdfbbc"))) ? ((-1) != (getNumberOfOnlineRegions())) : (((KnobRuntime.check(java.util.UUID.fromString("cde71fea-21aa-367c-b9c1-19b1afcb819f"))) ? ((0) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("a5dd51cc-c193-37bf-b884-7ea12781e760"))) ? ((0) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("5f926a62-527e-3436-9052-15c6369a065e"))) ? ((count) == (10)) : (((KnobRuntime.check(java.util.UUID.fromString("8a374943-2e50-3f98-bb9e-63ea9f5985e7"))) ? ((-1) == (lastCount)) : (((KnobRuntime.check(java.util.UUID.fromString("b59a223d-0f19-31c8-a409-4f6dd8c392ae"))) ? ((getNumberOfOnlineRegions()) != (10)) : (((KnobRuntime.check(java.util.UUID.fromString("806ab258-3492-3034-bfcb-7a66ef277241"))) ? ((-1) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("86635730-8d5f-3c32-824c-8a695ba87432"))) ? ((-1) != (200)) : (((KnobRuntime.check(java.util.UUID.fromString("7392cf79-dbd6-3a2e-8989-39c59c7816af"))) ? ((1) == (getNumberOfOnlineRegions())) : (count != lastCount))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          // Log every second at most
          if (((KnobRuntime.check(java.util.UUID.fromString("7c9d04c2-9544-317e-af43-5aeda18c55b5"))) ? ((EnvironmentEdgeManager.currentTime()) <= ((previousLogTime + 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("b685cdf8-aae4-3405-8bd8-a6bd57ffe8fd"))) ? ((EnvironmentEdgeManager.currentTime()) < ((previousLogTime + 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("c381dc17-bb13-3187-b40e-921da78034e9"))) ? ((EnvironmentEdgeManager.currentTime()) == ((previousLogTime + 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("f5719cef-0c7c-3618-9388-0d75349e2bdb"))) ? ((EnvironmentEdgeManager.currentTime()) > ((previousLogTime + 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("528fb49b-9883-3f7c-acda-1272593d36e9"))) ? ((EnvironmentEdgeManager.currentTime()) >= ((previousLogTime + 1000))) : (((KnobRuntime.check(java.util.UUID.fromString("e525c049-7459-369e-9108-ad9fd9c7da62"))) ? ((EnvironmentEdgeManager.currentTime()) != ((previousLogTime + 1000))) : (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)))))))))))))) {
            previousLogTime = EnvironmentEdgeManager.currentTime();
            lastCount = count;
            if (KnobRuntime.check(java.util.UUID.fromString("7a8855e2-3992-3684-92b0-466cf8cb371f"))) { LOG.info(("Waiting on ") + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("5d053e32-da7e-3922-9442-a4f666d8a4ea"))) { LOG.info(" regions to close"); } else if (KnobRuntime.check(java.util.UUID.fromString("e0bb179f-5b00-3b1c-8916-a50d9e6488da"))) { LOG.debug(("Waiting on " + count) + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("85577c77-f989-3638-b3cc-b83048addf03"))) { LOG.info("Waiting on " + count); } else if (KnobRuntime.check(java.util.UUID.fromString("3891079a-e071-3bdf-bfe4-3ea4dc8998ef"))) { LOG.info((("Waiting on ") + (count)) + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("505101d0-20b2-313a-bd69-31ca45859286"))) { LOG.info(("Waiting on " + count) + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("87cadc6d-5e66-33b5-9df4-3ebf5620c347"))) { LOG.error(" regions to close"); } else if (KnobRuntime.check(java.util.UUID.fromString("bf9c577d-d4e7-31dd-8b52-215a588dd80a"))) { LOG.error(("Waiting on ") + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("928d874d-6ce5-3a91-b480-fbc2d4cbee3f"))) { LOG.error((("Waiting on ") + (count)) + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("8d78fc11-6b74-35b7-a39b-61634ea856ad"))) { LOG.error(("Waiting on ") + (count)); } else if (KnobRuntime.check(java.util.UUID.fromString("3757c9cc-02b5-3436-a039-9a96ebf99474"))) { LOG.debug(" regions to close"); } else if (KnobRuntime.check(java.util.UUID.fromString("98f7c699-0488-3bc6-a877-ca2b5bfa6f36"))) { LOG.debug(("Waiting on ") + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("f73eec2d-550f-3bef-8442-0e26a98a544b"))) { LOG.debug((("Waiting on ") + (count)) + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("acb6a51a-3152-353e-be9e-288be65205f6"))) { LOG.info("Waiting on "); } else if (KnobRuntime.check(java.util.UUID.fromString("2670675a-9a4a-3d63-8add-4bcb18d2331c"))) { LOG.error("Waiting on " + count); } else if (KnobRuntime.check(java.util.UUID.fromString("29262895-e6ac-3127-8c16-e10f163d0dec"))) { LOG.error("Waiting on "); } else if (KnobRuntime.check(java.util.UUID.fromString("18fe4696-dec6-3ed6-aaef-aa4608bc91e4"))) { LOG.debug(("Waiting on ") + (count)); } else if (KnobRuntime.check(java.util.UUID.fromString("ae8253f3-2e22-33b6-a4cb-0d957632f071"))) { LOG.debug("Waiting on " + count); } else if (KnobRuntime.check(java.util.UUID.fromString("927abf28-3841-33cb-b62d-53725fc4bf70"))) { LOG.error(("Waiting on " + count) + (" regions to close")); } else if (KnobRuntime.check(java.util.UUID.fromString("40723589-9d0f-3f2b-a0d5-a93f621c0634"))) { LOG.info(("Waiting on ") + (count)); } else if (KnobRuntime.check(java.util.UUID.fromString("269e01d1-1208-3b3d-8bc8-79aa7a8e9cd1"))) { LOG.debug("Waiting on "); } else { LOG.info("Waiting on " + count + " regions to close"); }
            // Only print out regions still closing if a small number else will
            // swamp the log.
            if (((KnobRuntime.check(java.util.UUID.fromString("fb74d3dc-767d-33eb-8332-0eb47cf059e5"))) ? (((-1) < (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("e41939d1-ed26-3f06-9311-f5e189299ae1"))) ? (((getNumberOfOnlineRegions()) > (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("35b69e5f-1d8c-3b8e-b07a-3abb51bd232b"))) ? (((count) == (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("1a337c90-fa0f-3ac4-885e-185dfdb34647"))) ? (((getNumberOfOnlineRegions()) > (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("8adfd7f8-c107-358a-99e7-195a109ce951"))) ? (((count) > (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("74cf484e-03e1-37ba-97f4-8ee72eb2ed49"))) ? (((-1) >= (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("6fd3762b-58d0-3b05-a037-ad1568a6fbf9"))) ? (((getNumberOfOnlineRegions()) <= (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("51d34dc3-c754-3622-8d08-3700da57307f"))) ? (((-1) > (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("8a322827-af93-3481-ba49-7d770b720758"))) ? (((-1) <= (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("00ed4814-1bb6-3544-9b30-403cc0c253e3"))) ? (LOG.isDebugEnabled()) : (((KnobRuntime.check(java.util.UUID.fromString("016acd10-0868-3f3c-a30f-16b34405d5c6"))) ? (((-1) > (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("8b2db441-4d21-3896-bdbc-4ca6353a9ffd"))) ? ((-1) != (10)) : (((KnobRuntime.check(java.util.UUID.fromString("5174a4fb-fd76-3b5b-a80a-be5637923429"))) ? (((getNumberOfOnlineRegions()) > (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("f11c699f-69e0-3302-a275-035248a5aebf"))) ? (((getNumberOfOnlineRegions()) <= (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("790b0008-ab80-34ac-a082-dcf182efd3ee"))) ? (((getNumberOfOnlineRegions()) == (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("b60e45fa-bb65-3d15-8d59-e99edcb7fcd9"))) ? (((count) != (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("669a43e2-9c15-307e-ac2e-43f44c1858f7"))) ? (((getNumberOfOnlineRegions()) == (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("2ea434ef-62a0-3043-aeca-e5209927eaa2"))) ? (((-1) == (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("d90d47f1-0336-33f4-87f3-e50ab5908ce8"))) ? ((getNumberOfOnlineRegions()) != (10)) : (((KnobRuntime.check(java.util.UUID.fromString("f6ac8a37-96c0-324d-ac9d-a18e54b8bb96"))) ? (((getNumberOfOnlineRegions()) == (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("b830480c-3875-3eac-a7cc-584d3aa14692"))) ? (((count) <= (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("26b51cad-808d-39da-b8fc-6702a3ad548c"))) ? (((count) >= (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("51809973-5229-3a41-874a-586d645e725d"))) ? (((-1) == (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("22a0c4a7-dda4-3494-bfc9-3018ac9d2204"))) ? ((count < 10) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("c5e4c8bd-7606-3bef-91e8-04b3267d79ee"))) ? ((getNumberOfOnlineRegions()) == (10)) : (((KnobRuntime.check(java.util.UUID.fromString("7e5cc184-6752-38db-a1ac-dd1c8c835413"))) ? (((count) < (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("c6670fc7-cb01-326c-af7b-002731b1ab1e"))) ? (((count) == (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("a10ecfd8-c742-3479-961c-0ee942604c44"))) ? ((count < 10) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("aacf44be-7982-37f2-a9ea-0f70ffa9c04d"))) ? (((-1) != (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("8bdfdcdd-a37e-3f25-86d9-e9fc41d716c8"))) ? (((getNumberOfOnlineRegions()) < (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("421b0291-e8ff-3325-918c-d91dff854243"))) ? (((getNumberOfOnlineRegions()) != (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("50818543-4f5a-3785-8416-fc16cd8c107d"))) ? (((count) >= (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("7e7fbdc8-f0a9-3c18-997b-1e38d7aad735"))) ? (((-1) <= (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("853a2306-94f3-3d6f-b5ab-012f1f3330e1"))) ? ((count) <= (10)) : (((KnobRuntime.check(java.util.UUID.fromString("db9610fa-05a4-38c5-bfd9-a90225701e5e"))) ? (((getNumberOfOnlineRegions()) >= (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("e47dbddd-8e84-3d1a-8de2-0a7d34eb1fc1"))) ? (((count) != (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("84235a53-0a9a-372f-a801-04a09868a881"))) ? (((getNumberOfOnlineRegions()) < (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("fedd2a70-e452-3129-8308-15501a6fa0ff"))) ? ((count < 10) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("4f7b238f-5980-3efa-8605-bc7523835e5d"))) ? (((count) != (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("f88f3075-2075-30d3-a399-47b3d64f494c"))) ? (((-1) >= (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("32516aaa-2d22-3246-809c-29a5d7003f1b"))) ? (((-1) == (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("27fc0b36-1d7a-30cb-ab65-f7e5869167c2"))) ? ((getNumberOfOnlineRegions()) < (10)) : (((KnobRuntime.check(java.util.UUID.fromString("a828db12-0017-3e4a-8c27-e349c8b57fea"))) ? (((count) != (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("3ea47923-bf26-39c4-bc77-f25a5d388a2e"))) ? (((count) >= (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("60bb2b8f-e032-3d15-a01e-33d5dc8495af"))) ? (((count) != (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("4180cc65-6dce-30d3-bd6c-ab1485e87f03"))) ? (((getNumberOfOnlineRegions()) <= (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("ce7b90af-3a88-3715-abc4-1bff7f4c09e5"))) ? ((count) > (10)) : (((KnobRuntime.check(java.util.UUID.fromString("37c21e5c-f2f8-3c96-a251-247a4a404c91"))) ? (((-1) == (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("b10434f7-9537-34ef-b34b-58a609ec0978"))) ? (((count) > (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("75b48cb3-95ac-3435-8d3c-3678feee30e5"))) ? (((count) > (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("63b6af4f-125f-3a6e-9639-820122f6a09d"))) ? (((count) != (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("f79ac50a-98ad-3f0f-97b9-110b5941d34a"))) ? (((getNumberOfOnlineRegions()) < (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("285b1149-ccbe-3dc2-b355-7eb444510ec2"))) ? (cacheTableDescriptor()) : (((KnobRuntime.check(java.util.UUID.fromString("edb7f2d2-5575-3839-9a00-d3b63b18104b"))) ? (((count) >= (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("5fdc5b70-c9b8-37b0-8ccc-542df3ecbad8"))) ? (((-1) == (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("ad233c41-d151-3a74-8be5-d1c0859fa02a"))) ? (((-1) != (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("1f9f5228-9af7-3f42-a69b-55f58d0064c1"))) ? (((count) <= (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("3f44c5e7-aeeb-396f-b99f-d80754c58793"))) ? (((-1) > (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("9929d3bb-4b90-37e2-be0c-d3110898c82a"))) ? (((count) < (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("839014c1-fc9d-300c-bb03-9e5a680da7b5"))) ? (((getNumberOfOnlineRegions()) == (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("2032600e-6399-3b08-9a9a-bf9a706ef2ec"))) ? (((getNumberOfOnlineRegions()) == (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("0e553eaf-1cde-3c1a-b803-f1b0df35374f"))) ? (((getNumberOfOnlineRegions()) >= (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("4b615193-587a-3e8a-9e0f-eb84316f091c"))) ? ((-1) > (10)) : (((KnobRuntime.check(java.util.UUID.fromString("6751a7ad-223e-3b57-8b61-35924c92776d"))) ? (((getNumberOfOnlineRegions()) != (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("a09d6022-77fd-33ae-831f-bfd48edddca5"))) ? (count < 10) : (((KnobRuntime.check(java.util.UUID.fromString("823b0dc8-88ab-3a48-a3cb-e99f3a6731d8"))) ? (((getNumberOfOnlineRegions()) > (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("a064dfb3-9a69-3b26-ae11-49a063b57d51"))) ? (((getNumberOfOnlineRegions()) > (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("a5623121-0593-368f-9db9-0e0ca43140da"))) ? (((getNumberOfOnlineRegions()) < (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("772e6201-7566-3bf3-894a-680c09401d35"))) ? (((-1) < (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("bbb9cbd5-3594-3cf2-9c85-37b203929024"))) ? (((count) >= (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("1673de94-3c03-3aa1-8215-68513fbeb6c2"))) ? (((count) > (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("7dec9bce-9a5d-3e4b-8074-c547d8181446"))) ? (((count) < (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("ad0885d3-86ea-3baa-96a5-92279841602b"))) ? (((-1) <= (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("26e7f681-9a8c-367c-87b4-3734a99ece66"))) ? ((getNumberOfOnlineRegions()) >= (10)) : (((KnobRuntime.check(java.util.UUID.fromString("827bcca0-f7ab-358e-aebe-399cf1afc9ad"))) ? (((-1) != (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("e3c58a3c-ecca-36a7-957a-98dee9826d45"))) ? (((count) < (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("c951ef12-59d7-367f-a631-458f9a184f56"))) ? (((count) > (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("865aa17e-adc0-3bd2-90f7-dc673605de4d"))) ? (((getNumberOfOnlineRegions()) >= (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("4115af26-cc37-389e-968d-b0b544c35c47"))) ? ((count < 10) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("3b34aeb6-b725-33dc-91b9-1f37ce18c77b"))) ? (((getNumberOfOnlineRegions()) >= (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("776d5944-aaf5-3f66-bbea-21c0f1b3a958"))) ? (((count) >= (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("1aa12b95-ef82-3ca0-acd7-d7160a216488"))) ? (((getNumberOfOnlineRegions()) != (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("010e3dd7-51c5-3442-93c1-a92d83475222"))) ? (((count) < (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("6e76eea6-1198-350d-97a5-06fca6a3513d"))) ? ((count) == (10)) : (((KnobRuntime.check(java.util.UUID.fromString("4d8d71f4-79df-397a-b6c1-02b45e43e73a"))) ? (((getNumberOfOnlineRegions()) > (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("33f1a5ee-4899-362e-84a0-619ea9d65f9c"))) ? (((-1) >= (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("4f606a8f-06bf-3df1-b5e5-5a4118cf974d"))) ? (((getNumberOfOnlineRegions()) <= (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("75ad692e-c4e9-3263-a433-57717771c55c"))) ? (((count) == (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("f60eb42f-50dc-36c1-a46b-e64d4bbe9e8d"))) ? (((-1) < (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("a3fda73d-46ee-36c2-9b77-057909192c70"))) ? (((-1) > (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("f59074cd-08fc-3fd2-9d21-804d6442053e"))) ? (((-1) != (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("ea17bf54-8fda-31cd-b6e8-2e411db0efb3"))) ? (((getNumberOfOnlineRegions()) >= (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("ee478e04-9b27-316d-925b-2bda3ac94ed3"))) ? ((count < 10) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("b99ddcc6-a04c-3c2f-acfe-45b59912f4df"))) ? (((-1) < (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("842c89ed-cd0f-39fd-bc53-00ea711bc3e1"))) ? (((count) == (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("2f2ecbb6-4dd1-351b-adaa-67017bdbb957"))) ? (((getNumberOfOnlineRegions()) < (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("b045c56c-b4dc-35a4-aaf7-61885dfb25d2"))) ? (((-1) <= (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("9aaddca8-4266-3f88-a3fd-a86bc1e6b26c"))) ? (((count) >= (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("9583f0b0-8cd7-3a57-b158-fada995a8c62"))) ? (((count) <= (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("0e3df937-4041-3a42-9c46-2b6484115a20"))) ? ((-1) < (10)) : (((KnobRuntime.check(java.util.UUID.fromString("e702e70e-3f10-3dc2-90b0-494771626f44"))) ? ((getNumberOfOnlineRegions()) <= (10)) : (((KnobRuntime.check(java.util.UUID.fromString("d27248ac-4c5c-38c7-8060-4c7a8fe284e3"))) ? (((getNumberOfOnlineRegions()) < (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("8d876b78-60c0-387a-afc7-8b70726c05c6"))) ? (((-1) < (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("f3a5d526-8603-3010-89de-2bec7ba0b07e"))) ? (((-1) != (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("af9ca4de-bc3b-3525-8321-402ec9720df5"))) ? (((-1) >= (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("257122d4-ce3d-3c0c-9108-4d25222bed04"))) ? (((-1) >= (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("94e07949-d917-3a0f-bae8-6089d285a9fb"))) ? (((count) <= (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("287384ed-e541-3778-bb15-2111849e4d13"))) ? ((getNumberOfOnlineRegions()) > (10)) : (((KnobRuntime.check(java.util.UUID.fromString("8272093f-8bbf-3e8d-891d-76e49b41efcd"))) ? (((-1) <= (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("5ee144f7-32f0-3480-a974-5840943529a0"))) ? (((getNumberOfOnlineRegions()) != (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("58ec8474-0c1d-3ab1-8ba7-de1bcf6901b4"))) ? (((getNumberOfOnlineRegions()) < (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("22a3303f-fb9d-3ceb-a83a-0c94a5fe0a8a"))) ? (((count) <= (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("32eefeaa-5383-33ee-af24-fc95143e3999"))) ? (((getNumberOfOnlineRegions()) == (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("4e02556b-4da2-3aec-ad58-af7a1f850efc"))) ? (((count) == (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("cf07804f-c7ba-3148-9ab1-9b3424ecacc6"))) ? (canCreateBaseZNode()) : (((KnobRuntime.check(java.util.UUID.fromString("d131d23a-61f3-324d-99e6-10333f74addf"))) ? (((-1) >= (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("befe1bdb-0540-356a-9e8d-af533e345d90"))) ? ((-1) >= (10)) : (((KnobRuntime.check(java.util.UUID.fromString("d235d2c8-c46a-3e85-bbc0-95032e9dfb27"))) ? (((getNumberOfOnlineRegions()) <= (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("29b5edae-7143-3225-aa59-2f0014cd0e85"))) ? ((-1) == (10)) : (((KnobRuntime.check(java.util.UUID.fromString("b253810c-c1c1-3de9-9eec-81c63e3e5e42"))) ? (((getNumberOfOnlineRegions()) <= (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("f384ef38-9ba3-33f4-844a-9243b7b8d5ab"))) ? ((count) >= (10)) : (((KnobRuntime.check(java.util.UUID.fromString("464eb8d1-34cf-3ff6-8df1-bf69f711c205"))) ? (((count) > (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("beba3c76-be23-3b73-8d2e-2174305b8570"))) ? (((count) <= (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("7c958b1a-e166-3fa5-8b45-c6bc749382a8"))) ? (((getNumberOfOnlineRegions()) < (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("8c3862ac-a8af-3fae-8bf8-710550e8493b"))) ? (((-1) > (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("c4e7100a-2fd6-3a67-aab7-4b3580859877"))) ? ((count) != (10)) : (((KnobRuntime.check(java.util.UUID.fromString("bbc3b11e-ef78-3d51-af09-b1f0add7418b"))) ? (((getNumberOfOnlineRegions()) != (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("7ac17020-003f-3838-ad72-d2980bb6d909"))) ? (((count) < (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("cfcca29c-b3cd-3256-a173-fc889ef829a0"))) ? (((getNumberOfOnlineRegions()) != (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("e82a99f6-9cfc-34d1-8007-79e150fd2f13"))) ? (((-1) == (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("7eca3b0b-cad5-324e-a8fa-24cd0da3ff02"))) ? ((count < 10) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("017622a5-d2b2-3e9c-962c-eb1af605be70"))) ? (((count) == (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("a1d682ac-9b21-38dc-8a6f-b7f28fd83d90"))) ? (((count) != (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("0d216c5a-4731-3f21-b80c-aa43a6a1debf"))) ? (((getNumberOfOnlineRegions()) <= (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("c862a0f0-6d47-3956-b92c-3b1d117fa8cf"))) ? (((-1) == (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("4d029de8-f97c-3d93-8b81-60f96c28f54a"))) ? (((-1) > (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("2e9615ab-56d0-3ee4-b0c4-087b64d4d897"))) ? (((getNumberOfOnlineRegions()) != (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("afd37df7-baa9-3c1c-bdfc-8eed5064c81d"))) ? (((getNumberOfOnlineRegions()) >= (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("aec02c8b-60d1-3e25-aaad-737919423d00"))) ? (((getNumberOfOnlineRegions()) >= (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("ba293e00-e578-3ac8-9114-fefa5532fbfa"))) ? (((-1) < (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("161f0b8f-af14-3b24-876c-41dc5480ca88"))) ? (((getNumberOfOnlineRegions()) >= (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("5cc881e1-41d4-3367-9613-22e326633d94"))) ? (((-1) >= (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("19e24d98-5a28-3a19-9820-7aca046609c8"))) ? (((count) == (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("30dd28d3-5db4-324f-abb0-738af5604ba3"))) ? ((count) < (10)) : (((KnobRuntime.check(java.util.UUID.fromString("35df93cf-b9c0-3ac6-a8d9-5f4fb80785b0"))) ? (((count) != (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("8638cc3d-0c75-3250-82a9-faa348bb200f"))) ? (((count) <= (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("95da4a3c-e54f-3508-8872-6725d32c726c"))) ? (((-1) >= (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("7d21b0fe-c20a-3d17-b164-2182ee9bd01c"))) ? (((count) <= (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("e6f6465d-ae3f-38e7-9717-d50a9e7eec70"))) ? (((-1) != (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("a38ba281-2dba-3a80-a38f-83a8be050209"))) ? ((-1) <= (10)) : (((KnobRuntime.check(java.util.UUID.fromString("280a9381-6c6f-3b21-8026-8b1cbcdae0ae"))) ? (((count) < (10)) || (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("7b8b6afb-f6c4-3bab-874b-c2d167dfdd18"))) ? (((-1) < (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("df9543f8-040e-384f-b580-e62f8bfa456e"))) ? (((count) == (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("5034446a-da43-3275-828f-990563d6f03e"))) ? (((count) > (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("68b290e4-697d-3516-b7d9-493f94339fa9"))) ? (((getNumberOfOnlineRegions()) == (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("d72ed931-a6ad-3737-8f86-319a7ebef4c0"))) ? ((count < 10) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("b050d2ad-4cd9-3b55-bfd9-d29caabbb7e6"))) ? (((getNumberOfOnlineRegions()) == (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("de24591b-6504-3594-8dc8-56eb1b7488ce"))) ? (((-1) <= (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("031be2d2-1d38-356b-99ad-1e144d7a035f"))) ? (((-1) <= (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("9ad4e694-0485-3869-9495-ee96dcb25130"))) ? (((-1) > (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("e8f211d6-f740-3f4d-84d6-d3a43c6dc9d1"))) ? (((-1) == (10)) || (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("f714b310-f674-386a-a1c4-0864928593ea"))) ? (((getNumberOfOnlineRegions()) > (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("53878ea4-a2ec-3fa6-9597-55896be59b10"))) ? (((-1) <= (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("2f505a1a-7b18-3a58-98c2-9b70ad20e072"))) ? (((count) > (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("e7349e84-c532-3710-acb2-220a5f779982"))) ? (((getNumberOfOnlineRegions()) <= (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("d521e7cd-1f44-3b01-9fa5-058e123166b2"))) ? (((getNumberOfOnlineRegions()) > (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("51cdd98b-2442-3d31-b103-20c0268a3614"))) ? (((getNumberOfOnlineRegions()) != (10)) || (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("68331680-a76d-34fe-a068-99be94c02b07"))) ? (((-1) != (10)) && (canCreateBaseZNode())) : (((KnobRuntime.check(java.util.UUID.fromString("95cf3d76-bd46-3b33-ad1e-4c0de0b1034a"))) ? (((-1) != (10)) && (cacheTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("1608887e-b386-3abb-9052-1be9a4230ea4"))) ? ((count < 10) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("9ca55d5d-dde4-3e7a-bb65-0b05c66180f8"))) ? (canUpdateTableDescriptor()) : (((KnobRuntime.check(java.util.UUID.fromString("9d78c222-61a4-3022-99ed-7bafae876387"))) ? (((-1) > (10)) && (canUpdateTableDescriptor())) : (((KnobRuntime.check(java.util.UUID.fromString("1e89fbb3-e8bf-344e-8f8c-07820c20fce9"))) ? (((-1) < (10)) || (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("aef9250b-2780-30d7-a0b5-661946ce035e"))) ? (((count) >= (10)) && (LOG.isDebugEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("2c8ae0b2-52f8-3768-b37e-cf2802d20c0d"))) ? (((count) < (10)) && (canUpdateTableDescriptor())) : (count < 10 && LOG.isDebugEnabled()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
              if (KnobRuntime.check(java.util.UUID.fromString("0b257086-221e-374e-ab08-a93f4b369a0b"))) { LOG.info(("Online Regions=") + (this.onlineRegions)); } else if (KnobRuntime.check(java.util.UUID.fromString("509cac6b-e38d-395d-9eb6-cc31378095c0"))) { LOG.debug("Online Regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("92e508e4-6753-38ca-aeb2-2d8a15b5066a"))) { LOG.debug(("Online Regions=") + (this.onlineRegions)); } else if (KnobRuntime.check(java.util.UUID.fromString("bffff0d7-02ce-38cd-a702-20f669aa50f1"))) { LOG.info("Online Regions="); } else if (KnobRuntime.check(java.util.UUID.fromString("a644ffc7-4420-37fa-8dad-ded923e32144"))) { LOG.error(("Online Regions=") + (this.onlineRegions)); } else if (KnobRuntime.check(java.util.UUID.fromString("496d0bc3-6982-3fca-8f58-164cd4dbf28e"))) { LOG.error("Online Regions="); } else { LOG.debug("Online Regions=" + this.onlineRegions); }
            }
          }
        }
        // Ensure all user regions have been sent a close. Use this to
        // protect against the case where an open comes in after we start the
        // iterator of onlineRegions to close all user regions.
        for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
          RegionInfo hri = e.getValue().getRegionInfo();
          if (
            !this.regionsInTransitionInRS.containsKey(hri.getEncodedNameAsBytes())
              && !closedRegions.contains(hri.getEncodedName())
          ) {
            closedRegions.add(hri.getEncodedName());
            // Don't update zk with this close transition; pass false.
            if (KnobRuntime.check(java.util.UUID.fromString("990db4e8-c2ec-3c2c-823d-febd422cb904"))) { closeRegionIgnoreErrors(hri, !abort); } else if (KnobRuntime.check(java.util.UUID.fromString("4fe2e7b9-58fc-3cfc-964d-c28b016ce377"))) { closeRegionIgnoreErrors(hri, false); } else if (KnobRuntime.check(java.util.UUID.fromString("0663b18d-d644-3387-a6c6-1ae3848c4fa6"))) { closeRegionIgnoreErrors(hri, true); } else if (KnobRuntime.check(java.util.UUID.fromString("01cd0943-6ab9-3d5a-a5f8-e63ceadf6903"))) { closeRegionIgnoreErrors(hri, sleepInterrupted(200)); } else { closeRegionIgnoreErrors(hri, abort); }
          }
        }
        // No regions in RIT, we could stop waiting now.
        if (((KnobRuntime.check(java.util.UUID.fromString("1d5da2e4-e138-35d3-a8ac-70198138415d"))) ? (canCreateBaseZNode()) : (((KnobRuntime.check(java.util.UUID.fromString("54dc7a2e-ff2b-38a0-8c01-3e2d512ea166"))) ? (canUpdateTableDescriptor()) : (((KnobRuntime.check(java.util.UUID.fromString("712cb0b7-3267-3852-b0f3-6f58411dcc2e"))) ? (cacheTableDescriptor()) : (this.regionsInTransitionInRS.isEmpty()))))))) {
          if (!onlineRegions.isEmpty()) {
            LOG.info("We were exiting though online regions are not empty,"
              + " because some regions failed closing");
          }
          break;
        } else {
          LOG.debug("Waiting on {}", this.regionsInTransitionInRS.keySet().stream()
            .map(e -> Bytes.toString(e)).collect(Collectors.joining(", ")));
        }
        if (sleepInterrupted(200)) {
          interrupted = true;
        }
      }
    } finally {
      if (((KnobRuntime.check(java.util.UUID.fromString("4d900f69-4163-3449-9225-9cb0b9b2bc69"))) ? (sleepInterrupted(200)) : (((KnobRuntime.check(java.util.UUID.fromString("3cedfd8a-6d04-3991-9e57-c4d533925af3"))) ? (!interrupted) : (((KnobRuntime.check(java.util.UUID.fromString("717f92ed-09c8-3fec-899a-1775bfdf1d24"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("010719ac-d203-3110-916e-7e47c7914598"))) ? (true) : (interrupted))))))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("6d600525-03dd-3343-bdbc-43eabb16edcc"))) { setupSignalHandlers(); } else { Thread.currentThread().interrupt(); }
      }
    }
  }

  private static boolean sleepInterrupted(long millis) {
if(KnobRuntime.check(java.util.UUID.fromString("046caefd-c998-3488-b733-8ddee1321285"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("920c7aa0-881b-3ea9-a974-539b62e5b563"))) {
return true;
}
    boolean interrupted = false;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("adcaf0d8-2f0f-3df1-a228-70a806eda936"))) {
millis += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cf64dc7e-85f2-3e53-bb2c-98e2ea9d3745"))) {
millis = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b20a5a02-993a-32d5-b804-a3518ef1d7f9"))) {
throw new java.lang.InterruptedException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("bcb38339-61ff-3c31-a35a-3bd29eabf711"))) { Threads.sleep(millis); } else { Thread.sleep(millis); }
    } catch (InterruptedException e) {
      if (KnobRuntime.check(java.util.UUID.fromString("21fea96e-6a0c-31ce-8545-ece8cffdbe66"))) { LOG.error("Interrupted while sleeping"); } else if (KnobRuntime.check(java.util.UUID.fromString("4c4acb26-1e83-3078-8cf5-fdcee37ed83c"))) { LOG.debug("Interrupted while sleeping"); } else if (KnobRuntime.check(java.util.UUID.fromString("34b84579-9cc6-3244-9833-5a3afa2a01ec"))) { LOG.info("Interrupted while sleeping"); } else { LOG.warn("Interrupted while sleeping"); }
      interrupted = true;
    }
    return interrupted;
  }

  private void shutdownWAL(final boolean close) {
    if (this.walFactory != null) {
      try {
        if (close) {
          walFactory.close();
        } else {
          walFactory.shutdown();
        }
      } catch (Throwable e) {
        e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
        LOG.error("Shutdown / close of WAL failed: " + e);
        LOG.debug("Shutdown / close exception details:", e);
      }
    }
  }

  /**
   * get NamedQueue Provider to add different logs to ringbuffer
   */
  public NamedQueueRecorder getNamedQueueRecorder() {
    return this.namedQueueRecorder;
  }

  /*
   * Run init. Sets up wal and starts up all server threads.
   * @param c Extra configuration.
   */
  protected void handleReportForDutyResponse(final RegionServerStartupResponse c)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8daedfb4-45f2-3d94-bbe4-e97d9ca82da2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c46a960c-6c44-3012-8eb8-e9c57788dd25"))) {
return;
}
    try {
      boolean updateRootDir = false;
      for (NameStringPair e : c.getMapEntriesList()) {
        String key = e.getName();
        // The hostname the master sees us as.
        if (key.equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          String hostnameFromMasterPOV = e.getValue();
          this.serverName = ServerName.valueOf(hostnameFromMasterPOV,
            rpcServices.getSocketAddress().getPort(), this.startcode);
          String expectedHostName = rpcServices.getSocketAddress().getHostName();
          // if Master use-ip is enabled, RegionServer use-ip will be enabled by default even if it
          // is set to disable. so we will use the ip of the RegionServer to compare with the
          // hostname passed by the Master, see HBASE-27304 for details.
          if (
            StringUtils.isBlank(useThisHostnameInstead) && getActiveMaster().isPresent()
              && InetAddresses.isInetAddress(getActiveMaster().get().getHostname())
          ) {
            expectedHostName = rpcServices.getSocketAddress().getAddress().getHostAddress();
          }
          boolean isHostnameConsist = StringUtils.isBlank(useThisHostnameInstead)
            ? hostnameFromMasterPOV.equals(expectedHostName)
            : hostnameFromMasterPOV.equals(useThisHostnameInstead);
          if (!isHostnameConsist) {
            String msg = "Master passed us a different hostname to use; was="
              + (StringUtils.isBlank(useThisHostnameInstead)
                ? expectedHostName
                : this.useThisHostnameInstead)
              + ", but now=" + hostnameFromMasterPOV;
            LOG.error(msg);
            throw new IOException(msg);
          }
          continue;
        }

        String value = e.getValue();
        if (key.equals(HConstants.HBASE_DIR)) {
          if (value != null && !value.equals(conf.get(HConstants.HBASE_DIR))) {
            updateRootDir = true;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }
      // Set our ephemeral znode up in zookeeper now we have a name.
      createMyEphemeralNode();

      if (((KnobRuntime.check(java.util.UUID.fromString("6a07d950-5385-357b-82fa-791b16be1a1b"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("7abf8747-c527-3616-9707-ea356bf23180"))) ? (getConfiguration().getBoolean("hbase.regionserver.workers", true)) : (((KnobRuntime.check(java.util.UUID.fromString("1c72449b-353c-31d4-b7bc-f8de3dcd7c36"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("aa457547-ee15-3c07-94cd-785d3249648a"))) ? (getActiveMaster().isPresent()) : (((KnobRuntime.check(java.util.UUID.fromString("d53e82e8-e6cf-3f72-b5aa-2fc9a722954b"))) ? (!updateRootDir) : (updateRootDir))))))))))) {
        // initialize file system by the config fs.defaultFS and hbase.rootdir from master
if(KnobRuntime.check(java.util.UUID.fromString("30e6f7ba-0321-3502-b8f7-94999a1f0da2"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("31376c73-da00-355c-9a1b-2f0944d4591e"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("f2f5c108-cb72-3f0a-bcac-c8acdaecdd29"))) { putUpWebUI(); } else { initializeFileSystem(); }
      }

      // hack! Maps DFSClient => RegionServer for logs. HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapreduce.task.attempt.id") == null) {
        this.conf.set("mapreduce.task.attempt.id", "hb_rs_" + this.serverName.toString());
      }

      // Save it in a file, this will allow to see if we crash
      if (KnobRuntime.check(java.util.UUID.fromString("a5884e8e-83a9-3d23-893f-9e7e16d66a81"))) { ZNodeClearer.writeMyEphemeralNodeOnDisk(getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("df47c996-e63d-32e2-b286-59562a9bc159"))) { ZNodeClearer.writeMyEphemeralNodeOnDisk(getProcessName()); } else { ZNodeClearer.writeMyEphemeralNodeOnDisk(getMyEphemeralNodePath()); }

      // This call sets up an initialized replication and WAL. Later we start it up.
if(KnobRuntime.check(java.util.UUID.fromString("1d0817c8-66a8-351b-a812-45fad51d8046"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("1912b2ff-6ec3-3509-b41c-a4b94de3c7f4"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("3f566502-2923-3bdc-8832-19bc86a4c254"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("82a6350c-a8b8-3a2e-bb97-c76a3b348561"))) { initializeFileSystem(); } else { setupWALAndReplication(); }
      // Init in here rather than in constructor after thread name has been set
      final MetricsTable metricsTable =
        new MetricsTable(new MetricsTableWrapperAggregateImpl(this));
      this.metricsRegionServerImpl = new MetricsRegionServerWrapperImpl(this);
      this.metricsRegionServer =
        new MetricsRegionServer(metricsRegionServerImpl, conf, metricsTable);
      // Now that we have a metrics source, start the pause monitor
      this.pauseMonitor = new JvmPauseMonitor(conf, getMetrics().getMetricsSource());
      if (KnobRuntime.check(java.util.UUID.fromString("56ff06e1-e41e-3b1d-8e1d-e6df8a2509ab"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("40d6ce5e-165f-3b19-949a-54bd70df368d"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("b4192e3d-0ba1-3f0c-888b-fb0b81480247"))) { putUpWebUI(); } else { pauseMonitor.start(); }

      // There is a rare case where we do NOT want services to start. Check config.
      if (getConfiguration().getBoolean("hbase.regionserver.workers", true)) {
        startServices();
      }
      // In here we start up the replication Service. Above we initialized it. TODO. Reconcile.
      // or make sense of it.
      startReplicationService();

      // Set up ZK
      LOG.info(
        "Serving as " + this.serverName + ", RpcServer on " + rpcServices.isa + ", sessionid=0x"
          + Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));

      // Wake up anyone waiting for this server to online
      synchronized (online) {
        if (KnobRuntime.check(java.util.UUID.fromString("1a1300d8-3d09-3241-a8c5-9e78fcf32f10"))) { closeUserRegions(true); } else { online.set(true); }
        online.notifyAll();
      }
    } catch (Throwable e) {
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"), "Region server startup failed");
    } finally {
      sleeper.skipSleepCycle();
    }
  }

  protected void initializeMemStoreChunkCreator() {
    if (MemStoreLAB.isEnabled(conf)) {
      // MSLAB is enabled. So initialize MemStoreChunkPool
      // By this time, the MemstoreFlusher is already initialized. We can get the global limits from
      // it.
      Pair<Long, MemoryType> pair = MemorySizeUtil.getGlobalMemStoreSize(conf);
      long globalMemStoreSize = pair.getFirst();
      boolean offheap = this.regionServerAccounting.isOffheap();
      // When off heap memstore in use, take full area for chunk pool.
      float poolSizePercentage = offheap
        ? 1.0F
        : conf.getFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, MemStoreLAB.POOL_MAX_SIZE_DEFAULT);
      float initialCountPercentage = conf.getFloat(MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY,
        MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT);
      int chunkSize = conf.getInt(MemStoreLAB.CHUNK_SIZE_KEY, MemStoreLAB.CHUNK_SIZE_DEFAULT);
      float indexChunkSizePercent = conf.getFloat(MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_KEY,
        MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      // init the chunkCreator
      ChunkCreator.initialize(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
        initialCountPercentage, this.hMemManager, indexChunkSizePercent);
    }
  }

  private void startHeapMemoryManager() {
if(KnobRuntime.check(java.util.UUID.fromString("2ea89b85-6336-3c3a-9473-5a994f9dc058"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("a07f5730-3be8-3c48-b88b-32894d164dc6"))) ? ((this.blockCache) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("74b6b685-b381-3294-999a-5a69b33035a2"))) ? ((this.blockCache) != (null)) : (this.blockCache != null))))) {
      this.hMemManager =
        new HeapMemoryManager(this.blockCache, this.cacheFlusher, this, regionServerAccounting);
      this.hMemManager.start(getChoreService());
    }
  }

  private void createMyEphemeralNode() throws KeeperException {
    RegionServerInfo.Builder rsInfo = RegionServerInfo.newBuilder();
    rsInfo.setInfoPort(infoServer != null ? infoServer.getPort() : -1);
    rsInfo.setVersionInfo(ProtobufUtil.getVersionInfo());
    byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
    ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper, getMyEphemeralNodePath(), data);
  }

  private void deleteMyEphemeralNode() throws KeeperException {
    ZKUtil.deleteNode(this.zooKeeper, getMyEphemeralNodePath());
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return regionServerAccounting;
  }

  // Round the size with KB or MB.
  // A trick here is that if the sizeInBytes is less than sizeUnit, we will round the size to 1
  // instead of 0 if it is not 0, to avoid some schedulers think the region has no data. See
  // HBASE-26340 for more details on why this is important.
  private static int roundSize(long sizeInByte, int sizeUnit) {
if(KnobRuntime.check(java.util.UUID.fromString("faea4270-d47f-3b95-aeaa-5ff4da623091"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("2542e043-c457-30a8-9ee2-44ff2378c9e9"))) ? ((sizeInByte) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("04ee83fb-5f1f-3add-9967-e3a994ff814a"))) ? ((sizeInByte) == (0)) : (sizeInByte == 0))))) {
      return 0;
    } else if (sizeInByte < sizeUnit) {
      return 1;
    } else {
      return (int) Math.min(sizeInByte / sizeUnit, Integer.MAX_VALUE);
    }
  }

  private void computeIfPersistentBucketCache(Consumer<BucketCache> computation) {
    if (blockCache instanceof CombinedBlockCache) {
      BlockCache l2 = ((CombinedBlockCache) blockCache).getSecondLevelCache();
      if (l2 instanceof BucketCache && ((BucketCache) l2).isCachePersistent()) {
        computation.accept((BucketCache) l2);
      }
    }
  }

  /**
   * @param r               Region to get RegionLoad for.
   * @param regionLoadBldr  the RegionLoad.Builder, can be null
   * @param regionSpecifier the RegionSpecifier.Builder, can be null
   * @return RegionLoad instance.
   */
  RegionLoad createRegionLoad(final HRegion r, RegionLoad.Builder regionLoadBldr,
    RegionSpecifier.Builder regionSpecifier) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c6b265b6-6fa3-32b2-9510-6f50dfa6d863"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("readRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3a5b2b8-8c7d-3707-82d2-82371d84fa7f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5c845e1-bfb1-3cb5-961f-c1427258bc6f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b62a22c0-1127-3d70-b5fc-4455dc07f3b8"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2bd8a6d5-7a8a-30b4-8d01-83910ef79c63"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f94602d-85dd-3a8d-a571-b871e228078d"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e2aa797-50a5-3156-82c0-863908294a4f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b4e8e74-1cc7-348b-9f61-ac7a5e0dd92f"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5943bd6-8ca8-313e-9178-106cbea0a011"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38b53cd4-09e8-3362-975e-a97a4b2b72cb"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6bde269d-b6d7-3b4e-844d-023b0335090e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a275e9a0-ee3f-34bb-9e8a-8d3f605ce5f7"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6789d99d-0691-3aae-81b2-3d4d472801d1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04459db2-2a63-37aa-aab5-3aab0b48f33b"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c75dceb-5545-3951-aa15-f82a624d44a2"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4bdf8143-2c43-3e20-992d-ec55a55bbf5a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dc65cf8-1cea-3c6b-b030-4f7abf6d1cbc"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a1d3868-4cef-3a1e-9153-6a500937611e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3adac579-8ad6-3709-8a70-488153c276e5"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e4b7de2-0bf5-3a3b-a327-752f752efe9f"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3da2f416-3c25-3600-9a46-09340d0829db"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7c65d95-7fcd-36ac-865d-9a0d7ce6ae64"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1cd2d712-54f2-3a35-b3c7-ad617a8ee2f0"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfe510de-a5c9-3d6d-9732-0f9555a44fe9"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7eeaabd5-8f1d-3c2c-98f3-e5dd4805e865"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38f8bbd6-75ee-302d-aedc-025a9d8f43a5"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e342deb1-0c74-3924-9ac0-343d9556dfc8"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ecf836e4-47b8-3047-9fcf-fa76b0df8f98"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("00b8fd60-feb5-3eaa-83de-6143cee5d99d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("filteredReadRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bcc68aa2-5849-32e1-8fc7-4dd0dd439260"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7e06533-d095-32f9-9014-6c0b15a124e2"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7e66d13-f0c4-373b-bf9d-44c5870575db"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6a092c8-7596-3b3d-a57d-cb858b3339e4"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("lastMajorCompactionTs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca6387b9-65cb-3003-966e-4aed7cdbeb94"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aebd72e9-e005-3447-936b-763e4b433a1f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksLocalWithSsdWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e4a404ed-046f-33a6-841a-5267755ba448"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("239c47b9-c94b-306d-a8a9-2b46c52f6f30"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1d24114-dde3-38f5-aa11-57ad72c57df1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebf5eabc-8113-3401-ba99-65986f189ec3"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a19d9f5a-4f95-3fdb-874e-ee391d00b993"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65c47245-c201-3921-b3e5-478ed839eea5"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b83043ae-47cf-3ea4-9ebb-37363f1827b7"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c3f19d9-98d7-33f7-bfb2-d8e688d7f1ba"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b320590-82a8-3a91-a1fb-e61b92084db4"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db6669cf-f27e-3851-8ca4-a8e3fd82d23e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40a1950f-d9ad-3017-bd28-47404d965c65"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32e0c1a9-7336-3343-b8f5-3222e4a0ae04"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c441b5ef-a75a-30ba-9697-8b9f604b66b9"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("090dcd03-d338-398c-a4f0-5ab210f8db40"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b52990a9-5ef0-3d65-906a-817cd9b1975c"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a4280fd-4d6c-3604-b88c-db2859df6972"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15081db6-cf2c-3a00-9223-d5df9f7b1651"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e3bdfa4-87bd-3e44-8380-5d40c15cecdf"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("24599a6e-3176-3d1b-8055-f4c8b7bb5a0c"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("026faff1-88ba-31b0-88dc-add737cc6969"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("677b8a76-76c6-391e-9edf-52b1e17732df"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a36b8c00-2f19-31f2-9bea-c52cb64d133c"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileIndexSizeKB_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0245197-bc83-3b06-b870-206045aad5bf"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db370156-55e3-36ce-9418-7a7191b12cbf"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4325eea8-7661-3ffc-9536-03662b0e1e97"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eea48b95-15a9-3692-9be9-05f0f641f7c2"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc5fbe7d-5c53-33b6-8c65-003ea330bd2b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksTotalWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b462271-854b-3b3f-9d80-d5a941947669"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8550b3b7-00e4-3cad-b749-a914927187a0"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60cce325-d9dc-3a88-ae40-f1fc3ab421f8"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e53b23a-f60b-39d7-8bcd-70f8558fe291"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ae91d5d-c09a-3018-9b72-331947ff1640"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("218aecd6-b174-3aa4-bd8e-a7acd57dfbc1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("44797791-b61a-3ab1-9021-4ac071f92788"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebe5b5c8-5fd2-30ae-8a55-c9a5e43b16eb"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("839cc60f-9a3a-3a78-bcbd-2f077fd02d96"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ee310615-8544-302f-96e8-1e95d47ad608"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30dee661-2b36-3125-ad0a-369884220749"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("13c9b9c7-5a67-3807-bac0-1d2b65e03b8d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9bf53145-b42d-32a9-82ae-4235c685e4fe"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d87ed46a-df2e-387b-9959-e6a8f8462341"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d104f099-f801-3413-ac44-fb90e6fc19ae"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksLocalWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("abdc1975-c50d-3ef1-aed1-77f49e48b786"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dce9b439-6ace-3bd0-abb8-5473025f6880"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(r);
    field.set(r, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fce3d022-5a6e-3232-9b7d-e71e880aabb8"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20c52429-6e24-3846-81f0-b28baf02ac73"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c71eee4-4450-3edb-8b1f-5f62f15ec0ad"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f81d7200-1e91-3d28-ace1-2daa59d458f5"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39755882-7b02-3030-a9db-0a166523c0e5"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("445dfa18-0bec-3451-abf1-93edb026fe14"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7fcdf0c2-1582-3650-bdb3-ed0ee2d6e843"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7cf5d1ab-106a-3370-b974-5fe14f33296a"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("writeRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab30dc8e-5558-3622-9277-9ab38989ee06"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("47c1ce55-ae86-3d98-83f2-e2f30dbe7e03"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae2bc896-3cdc-3b13-b2f8-5d989bad5482"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56fd855e-8a0a-3370-a9dd-1121464c4e0b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("completeSequenceId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02702d06-c8d3-3a80-8f60-99e57f45e053"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("91f13b58-4c04-3b06-8839-9df83ae12ca7"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9a18333d-4b01-3d59-9e87-8141bb19aaf9"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("535e9e39-14f7-326c-a6a4-a62451201ac2"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("currentCompactedKVs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba9a5634-9995-3a11-a65f-2a239ee5b784"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b5f9b45-d0e1-3283-b6da-0039bbe9656d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed8ce190-b419-3240-b35f-af2cdafa24f3"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5445a47d-9960-3556-8fa3-6af7f170d69f"))) {
try {
    java.lang.reflect.Field field = regionSpecifier.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionSpecifier));
    field.set(regionSpecifier, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7db2e23-e347-37e7-919b-ecd3574bf382"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9805d43-e529-3016-9859-367dcd97f70f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c59460d9-7d80-30a1-90e8-9a14ae4012f5"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8cc24dd3-1135-38e5-a6c9-8724f7361d5e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14e396a9-6687-37f8-b5cf-67a4f8959005"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("651cdcbf-3131-3d54-ab60-1a9228cecc46"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalCompactingKVs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    byte[] name = r.getRegionInfo().getRegionName();
    String regionEncodedName = r.getRegionInfo().getEncodedName();
    int stores = 0;
    int storefiles = 0;
    int storeRefCount = 0;
    int maxCompactedStoreFileRefCount = 0;
    long storeUncompressedSize = 0L;
    long storefileSize = 0L;
    long storefileIndexSize = 0L;
    long rootLevelIndexSize = 0L;
    long totalStaticIndexSize = 0L;
    long totalStaticBloomSize = 0L;
    long totalCompactingKVs = 0L;
    long currentCompactedKVs = 0L;
    long totalRegionSize = 0L;
    List<HStore> storeList = r.getStores();
    stores += storeList.size();
    for (HStore store : storeList) {
      storefiles += store.getStorefilesCount();
      int currentStoreRefCount = store.getStoreRefCount();
      storeRefCount += currentStoreRefCount;
      int currentMaxCompactedStoreFileRefCount = store.getMaxCompactedStoreFileRefCount();
      maxCompactedStoreFileRefCount =
        Math.max(maxCompactedStoreFileRefCount, currentMaxCompactedStoreFileRefCount);
      storeUncompressedSize += store.getStoreSizeUncompressed();
      storefileSize += store.getStorefilesSize();
      totalRegionSize += store.getHFilesSize();
      // TODO: storefileIndexSizeKB is same with rootLevelIndexSizeKB?
      storefileIndexSize += store.getStorefilesRootLevelIndexSize();
      CompactionProgress progress = store.getCompactionProgress();
      if (progress != null) {
        totalCompactingKVs += progress.getTotalCompactingKVs();
        currentCompactedKVs += progress.currentCompactedKVs;
      }
      rootLevelIndexSize += store.getStorefilesRootLevelIndexSize();
      totalStaticIndexSize += store.getTotalStaticIndexSize();
      totalStaticBloomSize += store.getTotalStaticBloomSize();
    }

    int memstoreSizeMB = roundSize(r.getMemStoreDataSize(), unitMB);
if(KnobRuntime.check(java.util.UUID.fromString("f8ca29fc-3a17-3bf2-8909-f8b281a0aca4"))) {
storeUncompressedSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("505523c9-8f13-3d39-b7d1-97a2150ea2dd"))) {
storeUncompressedSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7e85eea7-4360-348a-bc20-046f132a4a40"))) {
storeUncompressedSize = -1;
}
    int storeUncompressedSizeMB = roundSize(storeUncompressedSize, unitMB);
if(KnobRuntime.check(java.util.UUID.fromString("5e82137c-942c-3f43-9075-bc46d2d7444e"))) {
storefileSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("611004c6-9dd0-398b-b74f-49b882021df5"))) {
storefileSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0fd324ae-3a38-370d-8f59-18dda82171eb"))) {
storefileSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("40c85f6b-cf69-32f6-978a-f1d210551712"))) {
storefileSize = 0;
}
    int storefileSizeMB = roundSize(storefileSize, unitMB);
if(KnobRuntime.check(java.util.UUID.fromString("79bf6a44-c8b2-3ad2-8602-f5bb4006e658"))) {
storefileIndexSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bcb860cd-2caf-307c-9017-227490ce8be2"))) {
storefileIndexSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf0b7ca7-5785-33fa-8a9b-4469aa6562f8"))) {
storefileIndexSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fdae5ed0-7b7b-31af-926c-4f320d561be7"))) {
storefileIndexSize = -1;
}
    int storefileIndexSizeKB = roundSize(storefileIndexSize, unitKB);
if(KnobRuntime.check(java.util.UUID.fromString("f72784ed-fcc3-30fd-a555-0f8b903b6f8d"))) {
rootLevelIndexSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("44f2ee7d-932c-315a-b74d-0b8d3a07b186"))) {
rootLevelIndexSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("99cb4b2f-91b4-3321-a7cb-d8e79fe9b8bb"))) {
rootLevelIndexSize = -1;
}
    int rootLevelIndexSizeKB = roundSize(rootLevelIndexSize, unitKB);
if(KnobRuntime.check(java.util.UUID.fromString("49846775-aaed-3086-a500-942aed0f0b49"))) {
totalStaticIndexSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("01f74d8c-6ea5-38f0-94ac-f48f9a760ee6"))) {
totalStaticIndexSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d11f26a0-7b01-377a-ae66-02c05236fb3d"))) {
totalStaticIndexSize = 0;
}
    int totalStaticIndexSizeKB = roundSize(totalStaticIndexSize, unitKB);
if(KnobRuntime.check(java.util.UUID.fromString("ac146e2f-098e-3343-99a6-26976ebd5813"))) {
totalStaticBloomSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("35e0dc27-88bb-3293-887c-5a67ebe89cd0"))) {
totalStaticBloomSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6d3b2519-0952-3167-aa27-5a76c6cda2ce"))) {
totalStaticBloomSize += 1;
}
    int totalStaticBloomSizeKB = roundSize(totalStaticBloomSize, unitKB);
if(KnobRuntime.check(java.util.UUID.fromString("e1d86b77-514f-32da-aa93-85fc29d044c2"))) {
totalRegionSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("49702b07-097a-344c-83dd-a080966be250"))) {
totalRegionSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e9664b6-9a3a-32fb-9061-cfcc308c186a"))) {
totalRegionSize = -1;
}
    int regionSizeMB = roundSize(totalRegionSize, unitMB);
    final MutableFloat currentRegionCachedRatio = new MutableFloat(0.0f);
    getBlockCache().ifPresent(bc -> {
      bc.getRegionCachedInfo().ifPresent(regionCachedInfo -> {
        if (regionCachedInfo.containsKey(regionEncodedName)) {
          currentRegionCachedRatio.setValue(regionSizeMB == 0
            ? 0.0f
            : (float) roundSize(regionCachedInfo.get(regionEncodedName), unitMB) / regionSizeMB);
        }
      });
    });

    HDFSBlocksDistribution hdfsBd = r.getHDFSBlocksDistribution();
    float dataLocality = hdfsBd.getBlockLocalityIndex(serverName.getHostname());
    float dataLocalityForSsd = hdfsBd.getBlockLocalityIndexForSsd(serverName.getHostname());
    long blocksTotalWeight = hdfsBd.getUniqueBlocksTotalWeight();
    long blocksLocalWeight = hdfsBd.getBlocksLocalWeight(serverName.getHostname());
    long blocksLocalWithSsdWeight = hdfsBd.getBlocksLocalWithSsdWeight(serverName.getHostname());
    if (((KnobRuntime.check(java.util.UUID.fromString("7053ea05-05f9-3cd2-a7a8-cad0acc1f91c"))) ? ((regionLoadBldr) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ea62cbfe-28de-3201-95a7-79772fe7be3d"))) ? ((regionLoadBldr) != (null)) : (regionLoadBldr == null))))) {
      regionLoadBldr = RegionLoad.newBuilder();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("72ed9bf3-8d5c-3dd8-ab35-d522e03f8335"))) ? ((regionSpecifier) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a514e4bf-af91-374b-bf9a-5bb071f57266"))) ? ((regionSpecifier) == (null)) : (regionSpecifier == null))))) {
      regionSpecifier = RegionSpecifier.newBuilder();
    }

    regionSpecifier.setType(RegionSpecifierType.REGION_NAME);
    regionSpecifier.setValue(UnsafeByteOperations.unsafeWrap(name));
if(KnobRuntime.check(java.util.UUID.fromString("6e52b726-becd-376b-8865-952582afd766"))) {
stores /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("56b94855-f209-39af-b78c-6db1cbd35185"))) {
dataLocality = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("79ec994d-b01e-3355-8ade-73896dae867c"))) {
dataLocality = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cdf6c61a-f373-32c1-80b6-1140ad2b8427"))) {
rootLevelIndexSizeKB -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1f8124a4-5380-3628-8c64-c5941b28b2b8"))) {
storefiles = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f9af61f4-38d6-3e92-80a0-6dd4e08bd5ad"))) {
storefileSizeMB += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1810b952-e8a8-31f9-ae56-d7a5554ab461"))) {
totalCompactingKVs = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d98fabf9-9d89-35b7-817e-63b16178c047"))) {
totalStaticBloomSizeKB = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c62f3609-7626-3f96-b340-d571f73dfe96"))) {
totalStaticBloomSizeKB = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("87abd743-a2cc-3f50-a7f7-9cd6c088f4d1"))) {
storeRefCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("bc4251df-91c1-382b-b71d-a6fd5e682352"))) {
totalStaticIndexSizeKB = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a6b305b5-00d4-3211-92ee-85e272a337a2"))) {
storefileIndexSizeKB *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("16572763-181e-3fdb-920b-147e06a3a606"))) {
totalStaticBloomSizeKB /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("68dd855d-22c8-383d-9787-101d77b0df19"))) {
maxCompactedStoreFileRefCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("26aebbf0-2b6f-3ea6-a190-da12cb12b37a"))) {
storefiles *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a4e895dd-62c5-3f3a-aadc-6e901a4d69fd"))) {
maxCompactedStoreFileRefCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("40b80c34-bcaa-3542-822b-b251ea1bd12e"))) {
stores = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9dab4da0-114b-3fd9-b083-c28a1accbdec"))) {
storeUncompressedSizeMB += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0340377e-33ce-3958-8bd6-53ac9ce737fc"))) {
totalStaticBloomSizeKB += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3956071d-8aaa-357a-b0c2-a9873912fb44"))) {
storefileIndexSizeKB = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a62047c6-404d-31d7-8d68-eb1dd619bd69"))) {
totalStaticIndexSizeKB /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("172d11f5-5263-3b41-b9d1-dc611d5e52dd"))) {
blocksLocalWithSsdWeight = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6d98119b-474f-3069-9a0e-e4cdd2fb4173"))) {
currentCompactedKVs = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7243cf01-29ee-3906-acca-941d4789ad07"))) {
stores += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0428a5cb-43ab-3f75-8cb5-d6e6e0d66c88"))) {
memstoreSizeMB *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9089113b-0412-329f-b51c-9aed8c6d283f"))) {
currentCompactedKVs += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a613ea7d-ece8-30b7-8d0f-9c7cc62ee303"))) {
currentCompactedKVs = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("47b75648-dfb5-3370-be95-5dda6b4db669"))) {
storefileSizeMB -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fed2cc3d-92ff-3f22-aa1f-590d9330e0f1"))) {
storeUncompressedSizeMB = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("86836809-b3fe-31b1-800c-80bbaaa281f7"))) {
dataLocalityForSsd = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e19e433c-f7e9-3683-8120-b74acba38343"))) {
memstoreSizeMB -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6bf83989-f8aa-3480-b6b0-3f059036c12c"))) {
memstoreSizeMB += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("21cab2e3-407f-3b5e-a87d-a677a99e80f3"))) {
storeUncompressedSizeMB *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c7a0a881-4b96-351f-8d59-7b9cb93b1bd3"))) {
memstoreSizeMB = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2dbdd4f1-9824-397b-90f6-2a20bd3f868b"))) {
totalStaticIndexSizeKB += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e14e17b-84c8-3db6-a662-78232889d806"))) {
blocksLocalWeight = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a2a1b6f8-29ed-35ad-8bac-024930fe1005"))) {
dataLocalityForSsd = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3a3a580b-d773-3c11-a462-022c742b442c"))) {
rootLevelIndexSizeKB *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0d5399d8-0f7d-3d3e-9953-49a2c14fe54c"))) {
maxCompactedStoreFileRefCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f04d1e42-2e85-3480-a6f6-c1501923c537"))) {
totalStaticIndexSizeKB = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("89bcb774-b6e1-36a0-b914-e00c2bd98010"))) {
totalCompactingKVs = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0d62be2b-4540-37dd-8839-1b7e52465a37"))) {
storefileSizeMB = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e22f42e6-0930-3450-9caf-c4db47064172"))) {
rootLevelIndexSizeKB += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e401829d-4e7b-3cbe-ac22-03b63836758c"))) {
maxCompactedStoreFileRefCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b3f8d48e-78eb-369f-aea7-553179879a1b"))) {
storeRefCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c68213bb-29a8-30fe-9910-e545668f8557"))) {
totalStaticBloomSizeKB *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2df238ac-6248-3276-82b5-4783865409bf"))) {
storefileIndexSizeKB -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a64fcc3-1388-3013-ab24-3c167da26f5d"))) {
storeUncompressedSizeMB = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fbe9b73b-2366-32b3-9e74-7f81087b56aa"))) {
storefileIndexSizeKB = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("304b30b5-989f-3c12-8dd1-3cdb1e40ba03"))) {
memstoreSizeMB = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("970da64a-19da-3b86-82c7-5bb8e55ca93c"))) {
storefiles -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fd442207-84a7-3610-9cc3-5d27788a7efb"))) {
storefiles = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0fa229b4-9079-39bc-9a8b-b59f0d7dee70"))) {
storeUncompressedSizeMB -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d52f382a-6783-3fde-ac9d-19d9bbb96d75"))) {
maxCompactedStoreFileRefCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6ddf72b6-acae-35b2-9722-5027ed6ce56a"))) {
blocksTotalWeight += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("488bda01-38d1-32e7-a1e0-e7a8e4b272ab"))) {
totalStaticIndexSizeKB -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7005f611-ffca-3819-8acc-6df99a80eeb3"))) {
rootLevelIndexSizeKB /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("655b77f1-89b1-3f5f-b634-26530c855a1e"))) {
storeRefCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9538cd2-f00a-3bb9-bfc2-1712a93bb028"))) {
storeRefCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b4a5c76-85ee-3ced-9686-b5de6715c268"))) {
rootLevelIndexSizeKB = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d705fcd4-bfeb-35ae-bc1a-6f22d3866470"))) {
memstoreSizeMB /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f21f2b0b-5457-3341-9816-3ed03b365644"))) {
blocksLocalWeight += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("793c030c-fdc2-3a2c-a941-6c522968a101"))) {
blocksTotalWeight = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1f738ca0-5b7f-3881-8ab6-bf5648940c2c"))) {
storefileSizeMB = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("19ebdb04-85f1-386c-b341-526aaf1565ad"))) {
maxCompactedStoreFileRefCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("db2069b8-8619-3fb7-b525-def299391ca4"))) {
storefileIndexSizeKB += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c1498d3-587f-3905-8d77-61e3384b44b7"))) {
totalStaticBloomSizeKB -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c5ec172-3542-397f-88cb-31f269e863b9"))) {
totalStaticIndexSizeKB *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("20871f37-8a4b-3a60-8701-a91b7d4ae4bb"))) {
totalCompactingKVs += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9b1e16f2-1c7c-3c9b-a54d-56ca0e8a35f4"))) {
storefileSizeMB /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("93e321dd-92f7-3a77-ac70-90eefde327da"))) {
storefiles /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2c74e629-1ced-3b82-a338-abd24595db32"))) {
rootLevelIndexSizeKB = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf0e20c3-04ed-3183-b7f3-c21be01de1ae"))) {
stores *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("13c616e3-4f42-3257-9155-3d0fc4708c03"))) {
storeRefCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2c0e8621-efc4-3d81-96ef-0dd3d0bb8357"))) {
blocksLocalWithSsdWeight += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("74da55fb-0e1e-3c75-833e-0b9a702f46f9"))) {
stores -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b5c1891-524c-34d9-875c-8dabd5bc862d"))) {
storefileSizeMB *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("eab645dd-5c11-3486-aaed-f83d68e86754"))) {
storeUncompressedSizeMB /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c7ce3b6-f553-365f-b065-71087a39f799"))) {
storeRefCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e123d34-8598-3340-8c90-7528d8d63eae"))) {
storefiles += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("efe04875-509c-324a-89c1-360d011c1ea1"))) {
stores = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("27b82d8b-e4bc-300d-be56-12502c22380b"))) {
storefileIndexSizeKB /= 2;
}
    regionLoadBldr.setRegionSpecifier(regionSpecifier.build()).setStores(stores)
      .setStorefiles(storefiles).setStoreRefCount(storeRefCount)
      .setMaxCompactedStoreFileRefCount(maxCompactedStoreFileRefCount)
      .setStoreUncompressedSizeMB(storeUncompressedSizeMB).setStorefileSizeMB(storefileSizeMB)
      .setMemStoreSizeMB(memstoreSizeMB).setStorefileIndexSizeKB(storefileIndexSizeKB)
      .setRootIndexSizeKB(rootLevelIndexSizeKB).setTotalStaticIndexSizeKB(totalStaticIndexSizeKB)
      .setTotalStaticBloomSizeKB(totalStaticBloomSizeKB)
      .setReadRequestsCount(r.getReadRequestsCount())
      .setFilteredReadRequestsCount(r.getFilteredReadRequestsCount())
      .setWriteRequestsCount(r.getWriteRequestsCount()).setTotalCompactingKVs(totalCompactingKVs)
      .setCurrentCompactedKVs(currentCompactedKVs).setDataLocality(dataLocality)
      .setDataLocalityForSsd(dataLocalityForSsd).setBlocksLocalWeight(blocksLocalWeight)
      .setBlocksLocalWithSsdWeight(blocksLocalWithSsdWeight).setBlocksTotalWeight(blocksTotalWeight)
      .setCompactionState(ProtobufUtil.createCompactionStateForRegionLoad(r.getCompactionState()))
      .setLastMajorCompactionTs(r.getOldestHfileTs(true)).setRegionSizeMB(regionSizeMB)
      .setCurrentRegionCachedRatio(currentRegionCachedRatio.floatValue());
if(KnobRuntime.check(java.util.UUID.fromString("8cd71592-0ad6-3a71-b223-7a0cd109fe01"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28c7e9eb-8744-3cb0-ab11-fa3226102323"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("678815cf-0e55-313c-b7c2-76627ded4ae9"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d695339-f4a3-358a-b753-8052aa026004"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20b00bb3-20cd-3974-9f6a-f789a80b7101"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbeb0d77-872f-3322-9c9d-b0d1e5ed97a1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksTotalWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2690db75-b879-36c6-a8af-22fe3afe7e67"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16da3753-4924-3b65-8b35-359d01f37ab3"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5494dbc0-ef99-37a9-9fd1-72976815b518"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7bf66908-9eec-345d-800f-37af51c7b577"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec3a562f-be64-3688-bfb0-e8417ea4a724"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksLocalWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2b0f73b-8230-3d62-a485-4f2c2c2a59f4"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c23f72e-b475-38da-aa75-3bb0883fc72d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d694e499-25ac-34bb-a2f0-c7f67303c744"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4076ad5b-cb29-3450-ac58-108f77e3d7ed"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14416944-28c2-3d99-8118-3b5274493a95"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d616e239-8780-394c-bfb8-aa3ea687949d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad002f55-01ee-3d2e-9f00-f48578b21551"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36baac6a-5f9e-37dc-964f-489d08ec5893"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("filteredReadRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6da75b2b-849c-3b25-8d0e-af22b0d508a3"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d02bd6b-97a1-30ef-8259-2f1c7dc565da"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c44026c1-438d-3f21-8515-7688f90d8685"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("blocksLocalWithSsdWeight_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff7b0b9e-ed47-3c37-8d5f-b3eb9f93fd6d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9ba31f7-7ce0-31e2-a794-f676e26e1fe3"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f4c5c04-f430-39d7-9b23-ddb747fe447e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("regionSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3b81989-811e-3e03-99c6-077f81a62d5e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileIndexSizeKB_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af3862be-7de2-30c0-b128-84441ed29ee9"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5726e868-f3fd-33f3-b397-12dee70b72e6"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af6e7e5e-e836-3c65-b6cf-ba736794b365"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c2ad975-71ef-3ea7-b325-4af6ae0b3cd8"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3cb6140-1574-38a3-9537-3548e7093c10"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8706a98a-59ee-3a3d-89e1-cb1589681e89"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e1ec366e-4e40-3a20-96d2-1578e5801b7b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d36ffad0-c426-3abf-8a3f-0433b9cef3a8"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d466ccb-10e1-3a5b-88e7-b58883ec614b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("compactionState_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84cf8942-f748-3f5b-8713-76c286506178"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("writeRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a73d758d-e786-36bf-9364-626cf409c73a"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefileSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ad2690e-3bb5-3167-b6ed-1c245bef33c3"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("completeSequenceId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c47b37ed-772c-313a-b4f7-465ad4b030f4"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c304bc9c-aa86-3842-82f3-003047904042"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("467f5e0f-5b54-3b41-bad1-5f2686b5d874"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1df166bf-ce63-35f8-bc1d-c85d59053c02"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("readRequestsCount_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45bd1551-2f5e-3bd0-879b-14b116318f2d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5b6c0b4-197a-3cc2-ade7-7b58b8cb5c19"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f2638dd-ef66-31dd-a73a-cabcf8117fa1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1aac944b-071d-3bfa-ade2-9ff44facd90e"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebb8a990-deb1-34a0-bad2-03e6326fa46a"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89a10fb1-5603-3662-8e74-08e21fb65d88"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("443bda31-5562-36ad-ad5c-4c67b48e2ac1"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0b9ea86-f301-31fe-b62b-2ed83d4a6fab"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("currentCompactedKVs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f49106d-c560-34b1-aca1-949019fe6bae"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalCompactingKVs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8cc03625-d13d-392a-857a-d459a0c969ec"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db432c34-5772-35df-8902-232653ee5694"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebf254cb-f8e3-3f8d-b821-d4824eb3c655"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5d54e12-f1ff-37f0-99bf-6610c5556b1f"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storefiles_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e727ae18-0369-362f-bc5f-558332a5f57b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("storeUncompressedSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("178f084f-3994-35c2-838c-caea0a7f812b"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("edf5ca27-bea2-37a5-b486-f3539b425845"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("stores_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c306be8-de42-3239-882a-d69c1102aa35"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("maxCompactedStoreFileRefCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12e72e75-ebb2-3c2a-b026-71fb12d79613"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("rootIndexSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec32bf22-38fb-37ef-a335-2a8dbc960bde"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("lastMajorCompactionTs_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("125f94c9-327c-3e8d-b2ae-97433b17cd2d"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("memStoreSizeMB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30fb0ecb-bd87-3d97-9c4a-6014159d17b6"))) {
try {
    java.lang.reflect.Field field = regionLoadBldr.getClass().getDeclaredField("totalStaticBloomSizeKB_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionLoadBldr));
    field.set(regionLoadBldr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    r.setCompleteSequenceId(regionLoadBldr);
    return regionLoadBldr.build();
  }

  private UserLoad createUserLoad(String user, MetricsUserSource userSource) {
if(KnobRuntime.check(java.util.UUID.fromString("5423c920-21ff-3f77-997e-90c1ab40249d"))) {
return null;
}
    UserLoad.Builder userLoadBldr = UserLoad.newBuilder();
    userLoadBldr.setUserName(user);
    userSource.getClientMetrics().values().stream()
      .map(clientMetrics -> ClusterStatusProtos.ClientMetrics.newBuilder()
        .setHostName(clientMetrics.getHostName())
        .setWriteRequestsCount(clientMetrics.getWriteRequestsCount())
        .setFilteredRequestsCount(clientMetrics.getFilteredReadRequests())
        .setReadRequestsCount(clientMetrics.getReadRequestsCount()).build())
      .forEach(userLoadBldr::addClientMetrics);
    return userLoadBldr.build();
  }

  public RegionLoad createRegionLoad(final String encodedRegionName) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c72497ce-f3cc-3403-9f3c-9aba07abc32a"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("3097ca78-8c09-3c98-979c-b0dc9257f207"))) {
throw new java.io.IOException("Injected exception");
}
    HRegion r = onlineRegions.get(encodedRegionName);
if(KnobRuntime.check(java.util.UUID.fromString("79a1c6f1-5d00-3a39-82a6-5688025d1835"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efeb6f04-f910-3c36-8068-fb01110d811a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77e99c83-07a2-3e0b-97fd-4369cc2b3ed9"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54ea82cd-3841-359f-a672-1825cbcd9c61"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef3f0b19-d1aa-3196-aaa7-411d7a968222"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02244175-852e-30c3-ad5a-e482644421c0"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("445fc1bf-e357-346c-9479-97a767a8f7b8"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ddbdbc07-6873-32a4-9a21-853fa8d38e64"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86551eea-7729-3697-b9df-969bd22da6af"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3871d67-e156-31a0-a265-dff868b773c2"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e1fb79ae-5153-337d-bda1-756f10f73f52"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a43ecbb-fea5-3fb2-8960-a4876d700dbb"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5073ff50-9d7c-3814-bf67-82fc74766181"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f25d47b3-960c-3f69-86b2-aba82e3ee985"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b62ce208-1832-37b0-9e97-8be49eff885f"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4be0a6ce-1c37-3d96-95fb-35823ff8b2c3"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c27fdb35-82ae-3b29-bb29-22ae79b7b9e1"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2537adc6-2132-3a3f-9510-496a9a82ad94"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f152fca4-b4e0-330b-b4c5-e1a77c8d909c"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9de08779-9532-3b2f-bd06-9c03f3a38ebf"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("baf79cd4-57fd-3a4d-ba34-515433b9c7bb"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87423df0-04a4-3b42-a5af-f88e9aa2167c"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76f8f0e5-1cfc-32ab-b7b6-af2180bb5aad"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("44b98758-910d-3ec7-a8ab-63fcf22897bf"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59e2c096-63b2-33a7-b04e-b3e88c562351"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7bd3aba2-12af-341a-8a55-aac17bfb50e0"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b60bdc63-070b-33cc-8c77-0e1aac97c74e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e24f3e6a-e7fd-3455-b9cc-8e462eb5816d"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15e523e0-664c-36a8-95e5-74fdbe1bc6a9"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fbc8fe26-cc4c-3f62-a6f7-6396825dba86"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(r);
    field.set(r, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e785f30f-0dcc-348f-9b87-9a4796fb44ba"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc76bd07-cd6b-39c4-a9c9-61cd91ea10d8"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db325395-b787-3523-a138-8316c24f4757"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return r != null ? createRegionLoad(r, null, null) : null;
  }

  /**
   * Inner class that runs on a long period checking if regions need compaction.
   */
  private static class CompactionChecker extends ScheduledChore {
    private final HRegionServer instance;
    private final int majorCompactPriority;
    private final static int DEFAULT_PRIORITY = Integer.MAX_VALUE;
    // Iteration is 1-based rather than 0-based so we don't check for compaction
    // immediately upon region server startup
    private long iteration = 1;

    CompactionChecker(final HRegionServer h, final int sleepTime, final Stoppable stopper) {
      super("CompactionChecker", stopper, sleepTime);
      this.instance = h;
      if (KnobRuntime.check(java.util.UUID.fromString("d0a7f719-cbc9-345c-91c7-b30addc10ebb"))) { LOG.info((this.getName() + " runs every ") + (Duration.ofMillis(sleepTime))); } else if (KnobRuntime.check(java.util.UUID.fromString("58217a20-d29a-3350-9469-53e9241b4e4b"))) { LOG.info(((this.getName()) + (" runs every ")) + (Duration.ofMillis(sleepTime))); } else { LOG.info(this.getName() + " runs every " + Duration.ofMillis(sleepTime)); }

      /*
       * MajorCompactPriority is configurable. If not set, the compaction will use default priority.
       */
      this.majorCompactPriority = this.instance.conf
        .getInt("hbase.regionserver.compactionChecker.majorCompactPriority", DEFAULT_PRIORITY);
    }

    @Override
    protected void chore() {
if(KnobRuntime.check(java.util.UUID.fromString("43d04d87-d916-35df-a483-de965213bee0"))) {
return;
}
      for (HRegion hr : this.instance.onlineRegions.values()) {
        // If region is read only or compaction is disabled at table level, there's no need to
        // iterate through region's stores
        if (((KnobRuntime.check(java.util.UUID.fromString("2dcff0e5-c0d7-3a8b-b04b-f8383af9d5a0"))) ? ((((hr) == (null)) && (hr.isReadOnly())) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("ce078dd2-1113-376e-a188-9856c663dd60"))) ? ((((hr) != (null)) || (hr.isReadOnly())) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("0dfa9b27-708a-38dd-a0cb-99a00a6b9b4e"))) ? ((hr.isReadOnly()) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("5dd5d82e-6332-38ac-ba50-cb016b82aa29"))) ? ((((hr) == (null)) && (hr.isReadOnly())) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("f9a518e1-dc1a-3ed0-97d0-71d9678f4a59"))) ? (((hr) != (null)) || (hr.isReadOnly())) : (((KnobRuntime.check(java.util.UUID.fromString("ceb46862-5a68-3561-bd5b-bd08d2a6d559"))) ? (((hr) == (null)) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("d2ffc734-073c-3801-825f-e3db6188615a"))) ? (((hr == null) || (hr.isReadOnly())) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("f734c386-0722-33d9-bf16-f4445814a935"))) ? (((hr) != (null)) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("f18b91c8-eeb5-3626-b386-0b62ca3639a7"))) ? (((hr == null) || (hr.isReadOnly())) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("d072aabf-d4e8-38fb-b800-3025f11ae0ea"))) ? ((((hr) == (null)) || (hr.isReadOnly())) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("a78188cb-bf39-3928-86f9-a659787d7672"))) ? ((hr == null) && (hr.isReadOnly())) : (((KnobRuntime.check(java.util.UUID.fromString("97f6162a-791e-3992-b02b-dd4636ce684e"))) ? ((hr == null) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("d16b3903-7c58-3d62-9e68-4cf1369fcc2c"))) ? (((hr) == (null)) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("c10fed54-1ceb-3a0b-ab7c-296a86511bac"))) ? (((hr) == (null)) && (hr.isReadOnly())) : (((KnobRuntime.check(java.util.UUID.fromString("93a089d7-a62d-396e-8f37-4d42e54306e6"))) ? ((hr == null) || (hr.isReadOnly())) : (((KnobRuntime.check(java.util.UUID.fromString("279822b6-e72d-321b-ae97-c91d67ee14a4"))) ? ((hr) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8ce5561d-e9c3-3a4b-bc45-8c496c7eee1c"))) ? (hr == null || hr.isReadOnly()) : (((KnobRuntime.check(java.util.UUID.fromString("b343ad60-e7f0-3d70-9b10-592781ecc633"))) ? (((hr) == (null)) || (hr.isReadOnly())) : (((KnobRuntime.check(java.util.UUID.fromString("54fecd5e-123d-36d6-8418-293358ae325d"))) ? ((((hr) != (null)) && (hr.isReadOnly())) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("a97f4013-08ac-31fd-97ad-542643c8f7d8"))) ? ((hr == null || hr.isReadOnly()) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("dac44fe3-1f5a-3fb2-af66-0f7c3439ce19"))) ? ((hr) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bf542a94-fa9f-3f51-961c-9d8a2f29981f"))) ? (((hr) != (null)) && (hr.isReadOnly())) : (((KnobRuntime.check(java.util.UUID.fromString("e891c9e0-7f07-35e3-91ed-82ffb98dcb98"))) ? (hr.isReadOnly()) : (((KnobRuntime.check(java.util.UUID.fromString("2965c839-8a95-3ece-8fff-9a8670e956f4"))) ? ((hr == null || hr.isReadOnly()) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("23f2540f-7324-3459-964d-a2e8b8d12809"))) ? ((((hr) == (null)) || (hr.isReadOnly())) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("51a765ab-d4e0-3fea-b5c1-682b0f4e37c9"))) ? (((hr == null) && (hr.isReadOnly())) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("8e65e6d2-8fe3-3011-8127-20b1d7b6441e"))) ? (((hr == null) && (hr.isReadOnly())) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("f328c9f9-c474-3c7a-974c-c64222909009"))) ? ((hr == null) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("4312e007-5f41-3c51-b2ef-e90ae206e360"))) ? ((hr.isReadOnly()) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("66967235-28ca-314f-9272-e67ba8534fd9"))) ? ((((hr) != (null)) && (hr.isReadOnly())) || (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("e1bb9ff5-93e7-3dda-a14b-dbf94754e41f"))) ? (hr == null) : (((KnobRuntime.check(java.util.UUID.fromString("454926b9-e95a-300c-ae0f-1d424789adf4"))) ? (((hr) != (null)) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("73ac1831-735a-348e-a2db-51c2c7a4adb2"))) ? ((((hr) != (null)) || (hr.isReadOnly())) && (!hr.getTableDescriptor().isCompactionEnabled())) : (((KnobRuntime.check(java.util.UUID.fromString("96820284-b88d-3a9d-a87d-7f6178cdf7e0"))) ? (!hr.getTableDescriptor().isCompactionEnabled()) : (hr == null || hr.isReadOnly() || !hr.getTableDescriptor().isCompactionEnabled()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          continue;
        }
        for (HStore s : hr.stores.values()) {
          try {
            long multiplier = s.getCompactionCheckMultiplier();
            assert ((KnobRuntime.check(java.util.UUID.fromString("bf92540d-8e92-3ca7-80e6-46e33c16e4d3"))) ? ((multiplier) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4eb839ec-3111-3e8e-89c1-2bb16a956d88"))) ? ((multiplier) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3fcb06c1-dd60-3379-b5b6-f5e5feb03679"))) ? ((multiplier) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fad2ee54-72de-38c4-90e1-07ec2ef0f04c"))) ? ((multiplier) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0b3d7254-cc20-3e1c-8dca-555cbd16aa21"))) ? ((multiplier) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d8269b19-4b13-3831-a138-e56b566c8828"))) ? ((multiplier) > (0)) : (multiplier > 0))))))))))));
            if (((KnobRuntime.check(java.util.UUID.fromString("ad746752-2f49-318d-b7ee-a9ac72049101"))) ? ((iteration % multiplier) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cf89a0be-3e54-30b8-a9dd-9dceadf41468"))) ? ((iteration) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d1741dfc-b653-3f2e-90f0-5f300e15762b"))) ? ((multiplier) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ca4189d6-0279-334b-b290-6de446c24caf"))) ? ((multiplier) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fe5f9c5e-9703-3db2-a612-c2ee7acfc2a2"))) ? (((iteration) % (multiplier)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("42971e7b-14c9-3d8f-9198-c047ddc3db5f"))) ? ((iteration % multiplier) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("129fce10-5084-3144-af62-e4eb8e0055e3"))) ? (((iteration) % (multiplier)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7e869a7c-4137-3540-befd-2a756b7c1091"))) ? ((iteration) == (0)) : (iteration % multiplier != 0))))))))))))))))) {
              continue;
            }
            if (((KnobRuntime.check(java.util.UUID.fromString("ba8576f3-5dc3-35be-9e07-449b40b0c976"))) ? (s.shouldPerformMajorCompaction()) : (s.needsCompaction()))) {
              // Queue a compaction. Will recognize if major is needed.
if(KnobRuntime.check(java.util.UUID.fromString("e421ccf5-d239-35fb-9f01-5ca904ed89b8"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6bb6fc1f-4ac1-3e29-9b35-0428d695bd34"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b1fafbf-0fe0-370c-99e0-8a0da06f240a"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7ba14a0-297f-39a3-a504-b6f9c1c88a4b"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb1d7ff1-7b24-37fa-a536-26c1175940fa"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb8317e7-96df-3c72-a63f-837d754e62a5"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b47ef980-9394-359d-b141-dc4821a04b23"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("703ce6ed-43cc-3505-882a-c5b5a303641c"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d8e8159-3b7c-3b94-ac2e-a81478c2330d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7b40fa1a-8aa9-3b41-b074-1dfcf167b5fd"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25675c29-2ce0-3d73-9946-ce7f30f7efb2"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f30ebca-b4a3-30fc-a425-0d4c22a18dc6"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f6cfb72-9f26-3d3b-9535-76d5ff978076"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b00091f1-6138-363c-8755-6e166628d807"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8868bbec-fd1a-3426-b9d0-732e0a6be04f"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa8005a9-9d76-39ce-9313-368105c7e463"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29485cbe-94cd-35fc-804b-7546ba51b739"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4704377c-7100-30be-b2e6-d77b5d20a6d4"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9dae45e0-90cc-3c01-a410-7fdf8cc492f4"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0dd07b31-58aa-335b-a6ee-de735c11e8f0"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d7b2032-4ac3-3639-af8e-c594180e8607"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b3941b1-9c67-3477-85fd-bac6909bef5f"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d298778c-dc0e-345f-be7a-a16c07bbe7bb"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(hr);
    field.set(hr, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ee761b3a-db17-3eb3-a9ca-a7fbdb891b29"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8dcdb6ff-e7a4-3be4-8e4b-237f546f409f"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9dfb5800-d5bc-3323-9278-4d4c3b55751a"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7aaadae6-5254-3e47-a20d-246359ab510a"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e14e1e9a-546c-3bfb-8929-cfd4ef7b0f89"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6a777e1c-2669-3f57-af32-313ad66e4d44"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aacd1829-e94c-3e57-a23f-5834e51e4b17"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ad9db7d-f4c7-30df-a24a-cc8905c6280a"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e33e5b1-9716-330b-a432-5f975e2186db"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e8befaf-d385-3faa-88b4-f9194d6f52f7"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bffc2744-ade8-333f-80aa-0498c77c6ce8"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02dbccdb-19af-3873-9163-e0bb4046ff1a"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7925f688-f613-3552-beff-82a03a406647"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("312df1f2-49b0-3253-9ce8-e36d315b72e1"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8f5568c-6071-3b9f-9cfd-146f07ba4394"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f88c6556-89f8-3f66-bfa6-a0a934c52478"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa5b143c-e39c-3be3-a095-9a71021cac05"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9cd86e6e-5113-3d44-b0b3-4d01faa9e0d9"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ecfc052e-6d2f-3ed8-9ca0-116252051a61"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06240eea-922f-3c3a-9afb-b989ea93e3fe"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aeb84981-beb3-35b4-a900-c2f7012f5d1e"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6ebe5ab-55eb-384b-b211-fbff7d786534"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84474e0b-16e6-3f45-891c-b7b9f7023465"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f2a1af9-50e1-3970-9eb0-033189cdc93a"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ff2d3ea-2fd4-3915-8aad-2e4dc16bdf9f"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bd63b3f0-fa0e-3436-baa0-40bcdf9fcda2"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2e82b5a8-be4c-388e-90fe-8a06ec58cc96"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(s));
    field.set(s, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a125bbb7-8d93-3bd8-8356-807ebf64b17e"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hr));
    field.set(hr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef2e7778-f211-34e5-969e-60085d421f97"))) {
try {
    java.lang.reflect.Field field = s.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(s));
    field.set(s, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b6b00b0-aaf0-361b-b9f4-42b64af7088b"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1f94ae2-f4a2-3d0c-bacc-cece5a2ac7ad"))) {
try {
    java.lang.reflect.Field field = hr.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hr));
    field.set(hr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              this.instance.compactSplitThread.requestSystemCompaction(hr, s,
                getName() + " requests compaction");
            } else if (s.shouldPerformMajorCompaction()) {
              s.triggerMajorCompaction();
              if (
                majorCompactPriority == DEFAULT_PRIORITY
                  || majorCompactPriority > hr.getCompactPriority()
              ) {
                this.instance.compactSplitThread.requestCompaction(hr, s,
                  getName() + " requests major compaction; use default priority", Store.NO_PRIORITY,
                  CompactionLifeCycleTracker.DUMMY, null);
              } else {
                this.instance.compactSplitThread.requestCompaction(hr, s,
                  getName() + " requests major compaction; use configured priority",
                  this.majorCompactPriority, CompactionLifeCycleTracker.DUMMY, null);
              }
            }
          } catch (IOException e) {
            if (KnobRuntime.check(java.util.UUID.fromString("04cd5eba-1e39-3946-bf5b-26127a6ecbcd"))) { LOG.warn("Failed major compaction check on ", e); } else if (KnobRuntime.check(java.util.UUID.fromString("14ddebdc-68a9-3778-8ca1-e5ce5f4dcfdb"))) { LOG.warn(("Failed major compaction check on ") + (hr), e); } else { LOG.warn("Failed major compaction check on " + hr, e); }
          }
        }
      }
      iteration = (iteration == Long.MAX_VALUE) ? 0 : (iteration + 1);
    }
  }

  private static class PeriodicMemStoreFlusher extends ScheduledChore {
    private final HRegionServer server;
    private final static int RANGE_OF_DELAY = 5 * 60; // 5 min in seconds
    private final static int MIN_DELAY_TIME = 0; // millisec
    private final long rangeOfDelayMs;

    PeriodicMemStoreFlusher(int cacheFlushInterval, final HRegionServer server) {
      super("MemstoreFlusherChore", server, cacheFlushInterval);
      this.server = server;

      final long configuredRangeOfDelay = server.getConfiguration()
        .getInt("hbase.regionserver.periodicmemstoreflusher.rangeofdelayseconds", RANGE_OF_DELAY);
      this.rangeOfDelayMs = TimeUnit.SECONDS.toMillis(configuredRangeOfDelay);
    }

    @Override
    protected void chore() {
      final StringBuilder whyFlush = new StringBuilder();
      for (HRegion r : this.server.onlineRegions.values()) {
        if (r == null) {
          continue;
        }
        if (r.shouldFlush(whyFlush)) {
          FlushRequester requester = server.getFlushRequester();
          if (((KnobRuntime.check(java.util.UUID.fromString("70596953-bf10-3608-9c22-45780ebd7afa"))) ? ((requester) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1084744e-ff45-32b2-9ebe-2181129b595c"))) ? ((requester) != (null)) : (requester != null))))) {
            long delay = ThreadLocalRandom.current().nextLong(rangeOfDelayMs) + MIN_DELAY_TIME;
            // Throttle the flushes by putting a delay. If we don't throttle, and there
            // is a balanced write-load on the regions in a table, we might end up
            // overwhelming the filesystem with too many flushes at once.
            if (requester.requestDelayedFlush(r, delay)) {
              LOG.info("{} requesting flush of {} because {} after random delay {} ms", getName(),
                r.getRegionInfo().getRegionNameAsString(), whyFlush.toString(), delay);
            }
          }
        }
      }
    }
  }

  /**
   * Report the status of the server. A server is online once all the startup is completed (setting
   * up filesystem, starting executorService threads, etc.). This method is designed mostly to be
   * useful in tests.
   * @return true if online, false if not.
   */
  public boolean isOnline() {
if(KnobRuntime.check(java.util.UUID.fromString("cb7fb2ad-e342-3bbe-901f-5cd11b7d4464"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1dc43938-3c1d-3528-b213-3dd0ba30d845"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("9a297384-bd59-302f-b66a-6ea89bdae565"))) ? (canCreateBaseZNode()) : (((KnobRuntime.check(java.util.UUID.fromString("92510178-787a-32c9-b634-360868ddff71"))) ? (cacheTableDescriptor()) : (((KnobRuntime.check(java.util.UUID.fromString("a50c44ca-168a-3f42-8478-cd6de55cf7d2"))) ? (canUpdateTableDescriptor()) : (online.get()))))));
  }

  /**
   * Setup WAL log and replication if enabled. Replication setup is done in here because it wants to
   * be hooked up to WAL.
   */
  private void setupWALAndReplication() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("53bbd97b-9909-3260-95e7-f9d7aebc3462"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("58927204-6fea-3c8a-9f07-04ed988f4679"))) {
throw new java.io.IOException("Injected exception");
}
    WALFactory factory = new WALFactory(conf, serverName.toString(), (Server) this);
    // TODO Replication make assumptions here based on the default filesystem impl
    Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    String logName = AbstractFSWALProvider.getWALDirectoryName(this.serverName.toString());

    Path logDir = new Path(walRootDir, logName);
    if (KnobRuntime.check(java.util.UUID.fromString("ba8446c3-9ae1-31a5-9112-a4b035929908"))) { LOG.warn("logDir={}", logDir); } else if (KnobRuntime.check(java.util.UUID.fromString("0cd5a88f-1b18-3691-8e7e-ef5b1eab6149"))) { LOG.info("logDir={}", logDir); } else { LOG.debug("logDir={}", logDir); }
    if (((KnobRuntime.check(java.util.UUID.fromString("c03875ce-5ffc-3e30-aae7-0027a8a1a207"))) ? (this.walFs.mkdirs(logDir)) : (this.walFs.exists(logDir)))) {
      throw new RegionServerRunningException(
        "Region server has already created directory at " + this.serverName.toString());
    }
    // Always create wal directory as now we need this when master restarts to find out the live
    // region servers.
    if (!this.walFs.mkdirs(logDir)) {
      throw new IOException("Can not create wal directory " + logDir);
    }
    // Instantiate replication if replication enabled. Pass it the log directories.
    createNewReplicationInstance(conf, this, this.walFs, logDir, oldLogDir, factory);

    WALActionsListener walEventListener = getWALEventTrackerListener(conf);
    if (walEventListener != null && factory.getWALProvider() != null) {
      factory.getWALProvider().addWALActionsListener(walEventListener);
    }
    this.walFactory = factory;
  }

  private WALActionsListener getWALEventTrackerListener(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("40ebfde2-80d6-38a7-a714-f9a518114d6a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5436a8bb-d3c0-337f-a816-254d126a44e4"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("66ee2c0e-4a06-3913-aff6-e6aa4effcdad"))) ? (conf.getBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, !WAL_EVENT_TRACKER_ENABLED_DEFAULT)) : (conf.getBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, WAL_EVENT_TRACKER_ENABLED_DEFAULT)))) {
      WALEventTrackerListener listener =
        new WALEventTrackerListener(conf, getNamedQueueRecorder(), getServerName());
      return listener;
    }
    return null;
  }

  /**
   * Start up replication source and sink handlers.
   */
  private void startReplicationService() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("cf92bfc7-9948-3884-b189-aba926152413"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("d977326c-5f4c-320a-8400-6b3ed8553639"))) {
throw new java.io.IOException("Injected exception");
}
    if (
      this.replicationSourceHandler == this.replicationSinkHandler
        && this.replicationSourceHandler != null
    ) {
      this.replicationSourceHandler.startReplicationService();
    } else {
      if (this.replicationSourceHandler != null) {
if(KnobRuntime.check(java.util.UUID.fromString("21540ca3-3402-368a-a940-9a31e7ba7270"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("90dfa014-5a1f-393a-8e06-eea68f4a492c"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("b98fea06-c972-3ed1-94b6-5cedd717d7ff"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("7b7b2ec7-0766-3622-8720-d88d440ea82d"))) { putUpWebUI(); } else { this.replicationSourceHandler.startReplicationService(); }
      }
      if (this.replicationSinkHandler != null) {
if(KnobRuntime.check(java.util.UUID.fromString("a66be90b-ffb7-3bdd-b08e-99ced9f09028"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("ad7313bc-24ea-338c-84d8-0c85dbc3aa25"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("afeb7f24-e7dc-3ad9-ae9d-b8edd0111c1e"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("eb53a1ad-9af3-39c0-bb76-0ee0c5cae299"))) { putUpWebUI(); } else { this.replicationSinkHandler.startReplicationService(); }
      }
    }
  }

  /** Returns Master address tracker instance. */
  public MasterAddressTracker getMasterAddressTracker() {
if(KnobRuntime.check(java.util.UUID.fromString("0cb6fcf3-40e6-3f2f-8450-9ff273575114"))) {
return null;
}
    return this.masterAddressTracker;
  }

  /**
   * Start maintenance Threads, Server, Worker and lease checker threads. Start all threads we need
   * to run. This is called after we've successfully registered with the Master. Install an
   * UncaughtExceptionHandler that calls abort of RegionServer if we get an unhandled exception. We
   * cannot set the handler on all threads. Server's internal Listener thread is off limits. For
   * Server, if an OOME, it waits a while then retries. Meantime, a flush or a compaction that tries
   * to run should trigger same critical condition and the shutdown will run. On its way out, this
   * server will shut down Server. Leases are sort of inbetween. It has an internal thread that
   * while it inherits from Chore, it keeps its own internal stop mechanism so needs to be stopped
   * by this hosting server. Worker logs the exception and exits.
   */
  private void startServices() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d89360ae-dee0-31ed-bbc8-12df9c1159e4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("caed5974-43ef-3a1d-85cd-508528a94707"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("89b8d2e0-be8a-397f-a2d1-50c8c527f2d8"))) ? (!isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("dc4a3da5-8015-3db8-b095-5677c58665cf"))) ? ((!isStopped()) || (!isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("e12f7abc-f07d-34d0-97c1-cc6f3c385b26"))) ? ((!isStopped()) && (!isAborted())) : (((KnobRuntime.check(java.util.UUID.fromString("d4ed02d7-792b-3a18-bb5a-08d6164b4e79"))) ? (!isAborted()) : (!isStopped() && !isAborted()))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("f32c38e4-38fa-3be0-aa9c-20a70638ab7c"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("1ac88f8d-68bc-3629-a968-7a64d2949276"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("9d29d7c2-74b4-3407-8570-9218ec8c2ce5"))) { setupSignalHandlers(); } else { initializeThreads(); }
    }
    this.secureBulkLoadManager = new SecureBulkLoadManager(this.conf, clusterConnection);
    this.secureBulkLoadManager.start();

    // Health checker thread.
    if (isHealthCheckerConfigured()) {
      int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
        HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }

    this.walRoller = new LogRoller(this);
    this.flushThroughputController = FlushThroughputControllerFactory.create(this, conf);
    this.procedureResultReporter = new RemoteProcedureResultReporter(this);

    // Create the CompactedFileDischarger chore executorService. This chore helps to
    // remove the compacted files that will no longer be used in reads.
    // Default is 2 mins. The default value for TTLCleaner is 5 mins so we set this to
    // 2 mins so that compacted files can be archived before the TTLCleaner runs
    int cleanerInterval = conf.getInt("hbase.hfile.compaction.discharger.interval", 2 * 60 * 1000);
    this.compactedFileDischarger = new CompactedHFilesDischarger(cleanerInterval, this, this);
    choreService.scheduleChore(compactedFileDischarger);

    // Start executor services
    final int openRegionThreads = conf.getInt("hbase.regionserver.executor.openregion.threads", 3);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_OPEN_REGION).setCorePoolSize(openRegionThreads));
    final int openMetaThreads = conf.getInt("hbase.regionserver.executor.openmeta.threads", 1);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_OPEN_META).setCorePoolSize(openMetaThreads));
    final int openPriorityRegionThreads =
      conf.getInt("hbase.regionserver.executor.openpriorityregion.threads", 3);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_OPEN_PRIORITY_REGION)
        .setCorePoolSize(openPriorityRegionThreads));
    final int closeRegionThreads =
      conf.getInt("hbase.regionserver.executor.closeregion.threads", 3);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_CLOSE_REGION).setCorePoolSize(closeRegionThreads));
    final int closeMetaThreads = conf.getInt("hbase.regionserver.executor.closemeta.threads", 1);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_CLOSE_META).setCorePoolSize(closeMetaThreads));
    if (((KnobRuntime.check(java.util.UUID.fromString("80a151d1-be5a-35ce-85b5-a697343cf47a"))) ? (conf.getBoolean(getName() + ".leaseChecker", false)) : (((KnobRuntime.check(java.util.UUID.fromString("b903fa7d-6b6c-37c1-a65f-cefc442fbe09"))) ? (conf.getBoolean(getName() + ".procedureResultReporter", false)) : (((KnobRuntime.check(java.util.UUID.fromString("e3cd8a08-2ca7-3996-8a8f-6e8aaa45b8b2"))) ? (conf.getBoolean(getName() + ".logRoller", false)) : (((KnobRuntime.check(java.util.UUID.fromString("f6a0bd3a-ddc0-3d56-a673-a83309e23d65"))) ? (conf.getBoolean(getName(), false)) : (conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false)))))))))) {
      final int storeScannerParallelSeekThreads =
        conf.getInt("hbase.storescanner.parallel.seek.threads", 10);
      executorService.startExecutorService(
        executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_PARALLEL_SEEK)
          .setCorePoolSize(storeScannerParallelSeekThreads).setAllowCoreThreadTimeout(true));
    }
    final int logReplayOpsThreads =
      conf.getInt(HBASE_SPLIT_WAL_MAX_SPLITTER, DEFAULT_HBASE_SPLIT_WAL_MAX_SPLITTER);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_LOG_REPLAY_OPS)
        .setCorePoolSize(logReplayOpsThreads).setAllowCoreThreadTimeout(true));
    // Start the threads for compacted files discharger
    final int compactionDischargerThreads =
      conf.getInt(CompactionConfiguration.HBASE_HFILE_COMPACTION_DISCHARGER_THREAD_COUNT, 10);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_COMPACTED_FILES_DISCHARGER)
      .setCorePoolSize(compactionDischargerThreads));
if(KnobRuntime.check(java.util.UUID.fromString("ee1e73b7-4fc0-332f-9bee-47bb17ba3e49"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("b8e4ea7c-a883-3720-9653-64d8d9c0be9f"))) ? (MemStoreLAB.isEnabled(getConfiguration())) : (((KnobRuntime.check(java.util.UUID.fromString("806593b3-1af0-3c15-adc2-d820fbb41721"))) ? (ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(getConfiguration())) : (((KnobRuntime.check(java.util.UUID.fromString("2ba3a722-6d6a-359a-9b93-9b20d144f613"))) ? (LoadBalancer.isTablesOnMaster(getConfiguration())) : (ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(conf)))))))) {
      final int regionReplicaFlushThreads =
        conf.getInt("hbase.regionserver.region.replica.flusher.threads",
          conf.getInt("hbase.regionserver.executor.openregion.threads", 3));
      executorService.startExecutorService(executorService.new ExecutorConfig()
        .setExecutorType(ExecutorType.RS_REGION_REPLICA_FLUSH_OPS)
        .setCorePoolSize(regionReplicaFlushThreads));
    }
    final int refreshPeerThreads =
      conf.getInt("hbase.regionserver.executor.refresh.peer.threads", 2);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_REFRESH_PEER).setCorePoolSize(refreshPeerThreads));

    final int switchRpcThrottleThreads =
      conf.getInt("hbase.regionserver.executor.switch.rpc.throttle.threads", 1);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_SWITCH_RPC_THROTTLE)
        .setCorePoolSize(switchRpcThrottleThreads));
    final int claimReplicationQueueThreads =
      conf.getInt("hbase.regionserver.executor.claim.replication.queue.threads", 1);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_CLAIM_REPLICATION_QUEUE)
        .setCorePoolSize(claimReplicationQueueThreads));
    final int rsSnapshotOperationThreads =
      conf.getInt("hbase.regionserver.executor.snapshot.operations.threads", 3);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_SNAPSHOT_OPERATIONS)
        .setCorePoolSize(rsSnapshotOperationThreads));
    final int rsFlushOperationThreads =
      conf.getInt("hbase.regionserver.executor.flush.operations.threads", 3);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_FLUSH_OPERATIONS).setCorePoolSize(rsFlushOperationThreads));

    Threads.setDaemonThreadRunning(this.walRoller, getName() + ".logRoller",
      uncaughtExceptionHandler);
    if (this.cacheFlusher != null) {
      this.cacheFlusher.start(uncaughtExceptionHandler);
    }
    Threads.setDaemonThreadRunning(this.procedureResultReporter,
      getName() + ".procedureResultReporter", uncaughtExceptionHandler);

    if (this.compactionChecker != null) {
if(KnobRuntime.check(java.util.UUID.fromString("8b527cfd-7ac5-3a00-b9fc-e202d0ab8a9d"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("timeOfThisRun");
    field.setAccessible(true);
    long oldValue = ((long)field.get(compactionChecker));
    field.set(compactionChecker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b0f141f0-dd87-3d5a-b40b-e5cf84153da2"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compactionChecker));
    field.set(compactionChecker, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25c7b515-07f9-313b-98eb-39776589a835"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("timeOfLastRun");
    field.setAccessible(true);
    long oldValue = ((long)field.get(compactionChecker));
    field.set(compactionChecker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0236b731-add1-36da-87e3-5c39454dc25a"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compactionChecker));
    field.set(compactionChecker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("262bbed2-4cb2-32ec-b62e-b2eab6d9a745"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("initialDelay");
    field.setAccessible(true);
    long oldValue = ((long)field.get(compactionChecker));
    field.set(compactionChecker, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5816edf8-64b3-349b-9c52-cd097dd1d496"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("initialDelay");
    field.setAccessible(true);
    long oldValue = ((long)field.get(compactionChecker));
    field.set(compactionChecker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7cff2533-98b0-3afa-892b-029e4fb067ca"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compactionChecker));
    field.set(compactionChecker, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c3221dfd-93ea-3892-b983-88dadf32fcbd"))) {
try {
    java.lang.reflect.Field field = compactionChecker.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(compactionChecker));
    field.set(compactionChecker, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      choreService.scheduleChore(compactionChecker);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("2861179b-be56-3f2d-83bd-55ed76d1a105"))) ? ((this.periodicFlusher) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a9a863db-c99d-32ca-8d96-303ab31d9853"))) ? ((this.periodicFlusher) == (null)) : (this.periodicFlusher != null))))) {
      choreService.scheduleChore(periodicFlusher);
    }
    if (this.healthCheckChore != null) {
      choreService.scheduleChore(healthCheckChore);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("612eb4c2-40a9-3792-bd38-cb4831246de2"))) ? ((this.nonceManagerChore) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9f00e10b-b86b-35e8-bea6-1eef5aa180f5"))) ? ((this.nonceManagerChore) != (null)) : (this.nonceManagerChore != null))))) {
      choreService.scheduleChore(nonceManagerChore);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("ae8ec553-e339-33b2-ac76-160a5929bb04"))) ? ((this.storefileRefresher) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("952c3416-0c21-39b6-9e86-51cb67a6cf53"))) ? ((this.storefileRefresher) != (null)) : (this.storefileRefresher != null))))) {
      choreService.scheduleChore(storefileRefresher);
    }
    if (this.fsUtilizationChore != null) {
      choreService.scheduleChore(fsUtilizationChore);
    }
    if (this.namedQueueServiceChore != null) {
      choreService.scheduleChore(namedQueueServiceChore);
    }
    if (this.brokenStoreFileCleaner != null) {
      choreService.scheduleChore(brokenStoreFileCleaner);
    }
    if (this.rsMobFileCleanerChore != null) {
      choreService.scheduleChore(rsMobFileCleanerChore);
    }
    if (replicationMarkerChore != null) {
      LOG.info("Starting replication marker chore");
      choreService.scheduleChore(replicationMarkerChore);
    }

    // Leases is not a Thread. Internally it runs a daemon thread. If it gets
    // an unhandled exception, it will just exit.
    Threads.setDaemonThreadRunning(this.leaseManager, getName() + ".leaseChecker",
      uncaughtExceptionHandler);

    // Create the log splitting worker and start it
    // set a smaller retries to fast fail otherwise splitlogworker could be blocked for
    // quite a while inside Connection layer. The worker won't be available for other
    // tasks even after current task is preempted after a split task times out.
    Configuration sinkConf = HBaseConfiguration.create(conf);
    sinkConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      conf.getInt("hbase.log.replay.retries.number", 8)); // 8 retries take about 23 seconds
    sinkConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
      conf.getInt("hbase.log.replay.rpc.timeout", 30000)); // default 30 seconds
    if (KnobRuntime.check(java.util.UUID.fromString("bf465438-f1a0-39c2-96dd-81874205a752"))) { sinkConf.setInt(getName(), 1); } else if (KnobRuntime.check(java.util.UUID.fromString("0f5e10ad-3ee9-348c-9227-2df52765c26d"))) { sinkConf.setInt(getName() + ".procedureResultReporter", 1); } else if (KnobRuntime.check(java.util.UUID.fromString("3a15d326-dabd-3268-b961-db2259472319"))) { sinkConf.setInt(getName() + ".logRoller", 1); } else if (KnobRuntime.check(java.util.UUID.fromString("ecc356a2-7613-3fa4-8824-16ab8c7ef306"))) { sinkConf.setInt(getName() + ".leaseChecker", 1); } else { sinkConf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1); }
    if (
      this.csm != null
        && conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)
    ) {
      // SplitLogWorker needs csm. If none, don't start this.
      this.splitLogWorker = new SplitLogWorker(sinkConf, this, this, walFactory);
      if (KnobRuntime.check(java.util.UUID.fromString("d01c962f-3737-36b6-9df6-cd198d71ba34"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("c718cf7d-fd48-399a-ae1b-8f4c95d840dc"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("388c1bf9-daa0-3dd5-ab3b-bcf23e2a92c3"))) { initializeFileSystem(); } else { splitLogWorker.start(); }
      if (KnobRuntime.check(java.util.UUID.fromString("499d30bb-24f6-34e8-a5e3-7ea27becbc24"))) { LOG.error("SplitLogWorker started"); } else if (KnobRuntime.check(java.util.UUID.fromString("0180d9b4-e9dd-34be-8a8c-6c0596a4f6a8"))) { LOG.info("SplitLogWorker started"); } else { LOG.debug("SplitLogWorker started"); }
    }

    // Memstore services.
    if (KnobRuntime.check(java.util.UUID.fromString("244c3a07-c128-3c8a-b226-c52d7083fba3"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("632f1398-0b4e-3c47-bd1e-4877a53e4cd2"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("abe0e532-45d9-393b-97b7-8438cc81e597"))) { initializeFileSystem(); } else { startHeapMemoryManager(); }
    // Call it after starting HeapMemoryManager.
    initializeMemStoreChunkCreator();
  }

  private void initializeThreads() {
if(KnobRuntime.check(java.util.UUID.fromString("037aee94-198e-3d38-b1ef-04a3030cc7da"))) {
return;
}
    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplit(this);

    // Prefetch Notifier
    this.prefetchExecutorNotifier = new PrefetchExecutorNotifier(conf);

    // Background thread to check for compactions; needed if region has not gotten updates
    // in a while. It will take care of not checking too frequently on store-by-store basis.
    this.compactionChecker = new CompactionChecker(this, this.compactionCheckFrequency, this);
    this.periodicFlusher = new PeriodicMemStoreFlusher(this.flushCheckFrequency, this);
    this.leaseManager = new LeaseManager(this.threadWakeFrequency);

    final boolean isSlowLogTableEnabled = conf.getBoolean(HConstants.SLOW_LOG_SYS_TABLE_ENABLED_KEY,
      HConstants.DEFAULT_SLOW_LOG_SYS_TABLE_ENABLED_KEY);
    final boolean walEventTrackerEnabled =
      conf.getBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, WAL_EVENT_TRACKER_ENABLED_DEFAULT);

    if (isSlowLogTableEnabled || walEventTrackerEnabled) {
      // default chore duration: 10 min
      // After <version number>, we will remove hbase.slowlog.systable.chore.duration conf property
      final int slowLogChoreDuration = conf.getInt(HConstants.SLOW_LOG_SYS_TABLE_CHORE_DURATION_KEY,
        DEFAULT_SLOW_LOG_SYS_TABLE_CHORE_DURATION);

      final int namedQueueChoreDuration =
        conf.getInt(NAMED_QUEUE_CHORE_DURATION_KEY, NAMED_QUEUE_CHORE_DURATION_DEFAULT);
      // Considering min of slowLogChoreDuration and namedQueueChoreDuration
      int choreDuration = Math.min(slowLogChoreDuration, namedQueueChoreDuration);

      namedQueueServiceChore = new NamedQueueServiceChore(this, choreDuration,
        this.namedQueueRecorder, this.getConnection());
    }

    if (this.nonceManager != null) {
      // Create the scheduled chore that cleans up nonces.
      nonceManagerChore = this.nonceManager.createCleanupScheduledChore(this);
    }

    // Setup the Quota Manager
    rsQuotaManager = new RegionServerRpcQuotaManager(this);
    rsSpaceQuotaManager = new RegionServerSpaceQuotaManager(this);

    if (QuotaUtil.isQuotaEnabled(conf)) {
      this.fsUtilizationChore = new FileSystemUtilizationChore(this);
    }

    boolean onlyMetaRefresh = false;
    int storefileRefreshPeriod =
      conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
        StorefileRefresherChore.DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD);
    if (storefileRefreshPeriod == 0) {
      storefileRefreshPeriod =
        conf.getInt(StorefileRefresherChore.REGIONSERVER_META_STOREFILE_REFRESH_PERIOD,
          StorefileRefresherChore.DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD);
      onlyMetaRefresh = true;
    }
    if (storefileRefreshPeriod > 0) {
      this.storefileRefresher =
        new StorefileRefresherChore(storefileRefreshPeriod, onlyMetaRefresh, this, this);
    }

    int brokenStoreFileCleanerPeriod =
      conf.getInt(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_PERIOD,
        BrokenStoreFileCleaner.DEFAULT_BROKEN_STOREFILE_CLEANER_PERIOD);
    int brokenStoreFileCleanerDelay =
      conf.getInt(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_DELAY,
        BrokenStoreFileCleaner.DEFAULT_BROKEN_STOREFILE_CLEANER_DELAY);
    double brokenStoreFileCleanerDelayJitter =
      conf.getDouble(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_DELAY_JITTER,
        BrokenStoreFileCleaner.DEFAULT_BROKEN_STOREFILE_CLEANER_DELAY_JITTER);
    double jitterRate =
      (ThreadLocalRandom.current().nextDouble() - 0.5D) * brokenStoreFileCleanerDelayJitter;
    long jitterValue = Math.round(brokenStoreFileCleanerDelay * jitterRate);
    this.brokenStoreFileCleaner =
      new BrokenStoreFileCleaner((int) (brokenStoreFileCleanerDelay + jitterValue),
        brokenStoreFileCleanerPeriod, this, conf, this);

    this.rsMobFileCleanerChore = new RSMobFileCleanerChore(this);

    registerConfigurationObservers();
    initializeReplicationMarkerChore();
  }

  private void registerConfigurationObservers() {
    // Register Replication if possible, as now we support recreating replication peer storage, for
    // migrating across different replication peer storages online
    if (replicationSourceHandler instanceof ConfigurationObserver) {
      configurationManager.registerObserver((ConfigurationObserver) replicationSourceHandler);
    }
    if (
      replicationSourceHandler != replicationSinkHandler
        && replicationSinkHandler instanceof ConfigurationObserver
    ) {
      configurationManager.registerObserver((ConfigurationObserver) replicationSinkHandler);
    }
    // Registering the compactSplitThread object with the ConfigurationManager.
    configurationManager.registerObserver(this.compactSplitThread);
    configurationManager.registerObserver(this.cacheFlusher);
    configurationManager.registerObserver(this.rpcServices);
    configurationManager.registerObserver(this.prefetchExecutorNotifier);
    configurationManager.registerObserver(this);
  }

  /**
   * Puts up the webui.
   */
  private void putUpWebUI() throws IOException {
    int port =
      this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT, HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");

    boolean isMaster = false;
    if (this instanceof HMaster) {
      port = conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT);
      addr = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
      isMaster = true;
    }
    // -1 is for disabling info server
    if (port < 0) {
      return;
    }

    if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
      String msg = "Failed to start http info server. Address " + addr
        + " does not belong to this host. Correct configuration parameter: "
        + (isMaster ? "hbase.master.info.bindAddress" : "hbase.regionserver.info.bindAddress");
      LOG.error(msg);
      throw new IOException(msg);
    }
    // check if auto port bind enabled
    boolean auto = this.conf.getBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO, false);
    while (true) {
      try {
        this.infoServer = new InfoServer(getProcessName(), addr, port, false, this.conf);
        infoServer.addPrivilegedServlet("dump", "/dump", getDumpServlet());
        configureInfoServer();
        this.infoServer.start();
        break;
      } catch (BindException e) {
        if (!auto) {
          // auto bind disabled throw BindException
          LOG.error("Failed binding http info server to port: " + port);
          throw e;
        }
        // auto bind enabled, try to use another port
        LOG.info("Failed binding http info server to port: " + port);
        port++;
        LOG.info("Retry starting http info server with port: " + port);
      }
    }
    port = this.infoServer.getPort();
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, port);
    int masterInfoPort =
      conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT);
    conf.setInt("hbase.master.info.port.orig", masterInfoPort);
    conf.setInt(HConstants.MASTER_INFO_PORT, port);
  }

  /*
   * Verify that server is healthy
   */
  private boolean isHealthy() {
    if (!dataFsOk) {
      // File system problem
      return false;
    }
    // Verify that all threads are alive
    boolean healthy = (this.leaseManager == null || this.leaseManager.isAlive())
      && (this.cacheFlusher == null || this.cacheFlusher.isAlive())
      && (this.walRoller == null || this.walRoller.isAlive())
      && (this.compactionChecker == null || this.compactionChecker.isScheduled())
      && (this.periodicFlusher == null || this.periodicFlusher.isScheduled());
    if (!healthy) {
      stop("One or more threads are no longer alive -- stop");
    }
    return healthy;
  }

  @Override
  public List<WAL> getWALs() {
    return walFactory.getWALs();
  }

  @Override
  public WAL getWAL(RegionInfo regionInfo) throws IOException {
    try {
      WAL wal = walFactory.getWAL(regionInfo);
      if (this.walRoller != null) {
        this.walRoller.addWAL(wal);
      }
      return wal;
    } catch (FailedCloseWALAfterInitializedErrorException ex) {
      // see HBASE-21751 for details
      abort("WAL can not clean up after init failed", ex);
      throw ex;
    }
  }

  public LogRoller getWalRoller() {
    return walRoller;
  }

  WALFactory getWalFactory() {
if(KnobRuntime.check(java.util.UUID.fromString("46b6d92e-d4f5-3bba-89a7-8e9b6e43287b"))) {
return null;
}
    return walFactory;
  }

  @Override
  public Connection getConnection() {
    return getClusterConnection();
  }

  @Override
  public ClusterConnection getClusterConnection() {
if(KnobRuntime.check(java.util.UUID.fromString("d7e49bc9-5525-3f4e-a9ff-baab6bde2762"))) {
return null;
}
    return this.clusterConnection;
  }

  @Override
  public void stop(final String msg) {
    stop(msg, false, RpcServer.getRequestUser().orElse(null));
  }

  /**
   * Stops the regionserver.
   * @param msg   Status message
   * @param force True if this is a regionserver abort
   * @param user  The user executing the stop request, or null if no user is associated
   */
  public void stop(final String msg, final boolean force, final User user) {
    if (!this.stopped) {
      LOG.info("***** STOPPING region server '" + this + "' *****");
      if (this.rsHost != null) {
        // when forced via abort don't allow CPs to override
        try {
          this.rsHost.preStop(msg, user);
        } catch (IOException ioe) {
          if (!force) {
            LOG.warn("The region server did not stop", ioe);
            return;
          }
          LOG.warn("Skipping coprocessor exception on preStop() due to forced shutdown", ioe);
        }
      }
      this.stopped = true;
      LOG.info("STOPPED: " + msg);
      // Wakes run() if it is sleeping
      sleeper.skipSleepCycle();
    }
  }

  public void waitForServerOnline() {
if(KnobRuntime.check(java.util.UUID.fromString("27d40586-676e-3188-aae7-315bce553c39"))) {
return;
}
    while (((KnobRuntime.check(java.util.UUID.fromString("70d5dbbd-6e5b-3579-8947-a74b75ecb73f"))) ? (!isOnline()) : (((KnobRuntime.check(java.util.UUID.fromString("727c1ed0-2e29-39cf-81fe-1da09ff566c6"))) ? (!isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("5c908a1d-c362-35bb-8ec5-656a12544d40"))) ? ((!isStopped()) || (!isOnline())) : (((KnobRuntime.check(java.util.UUID.fromString("11200f11-3d7a-31cd-a4d7-c90359ac8946"))) ? ((!isStopped()) && (!isOnline())) : (!isStopped() && !isOnline()))))))))) {
      synchronized (online) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("84c2b120-2b4f-33d1-8b84-61d55cbbddeb"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("e4604bf3-8018-338e-a0b6-948074c6daab"))) { Thread.sleep(msgInterval); } else if (KnobRuntime.check(java.util.UUID.fromString("68e78110-c7ba-31f3-be21-dcf87c3ae091"))) { Threads.sleep(msgInterval); } else { online.wait(msgInterval); }
        } catch (InterruptedException ie) {
          if (KnobRuntime.check(java.util.UUID.fromString("2bb87326-54d6-3485-9112-d78df1ba23b6"))) { setupSignalHandlers(); } else { Thread.currentThread().interrupt(); }
          break;
        }
      }
    }
  }

  @Override
  public void postOpenDeployTasks(final PostOpenDeployContext context) throws IOException {
    HRegion r = context.getRegion();
    long openProcId = context.getOpenProcId();
    long masterSystemTime = context.getMasterSystemTime();
    long initiatingMasterActiveTime = context.getInitiatingMasterActiveTime();
    rpcServices.checkOpen();
    LOG.info("Post open deploy tasks for {}, pid={}, masterSystemTime={}",
      r.getRegionInfo().getRegionNameAsString(), openProcId, masterSystemTime);
    // Do checks to see if we need to compact (references or too many files)
    for (HStore s : r.stores.values()) {
      if (s.hasReferences() || s.needsCompaction()) {
        this.compactSplitThread.requestSystemCompaction(r, s, "Opening Region");
      }
    }
    long openSeqNum = r.getOpenSeqNum();
    if (((KnobRuntime.check(java.util.UUID.fromString("711d2ca1-43c9-3ff6-a470-248f514b3e7c"))) ? ((openSeqNum) != (HConstants.NO_SEQNUM)) : (((KnobRuntime.check(java.util.UUID.fromString("96487a42-46ce-3dbe-812a-1ccc76bc6c8d"))) ? ((openSeqNum) == (HConstants.NO_SEQNUM)) : (openSeqNum == HConstants.NO_SEQNUM))))) {
      // If we opened a region, we should have read some sequence number from it.
      LOG.error(
        "No sequence number found when opening " + r.getRegionInfo().getRegionNameAsString());
      openSeqNum = 0;
    }

    // Notify master
    if (
      !reportRegionStateTransition(new RegionStateTransitionContext(TransitionCode.OPENED,
        openSeqNum, openProcId, masterSystemTime, r.getRegionInfo(), initiatingMasterActiveTime))
    ) {
      throw new IOException(
        "Failed to report opened region to master: " + r.getRegionInfo().getRegionNameAsString());
    }

    triggerFlushInPrimaryRegion(r);

    LOG.debug("Finished post open deploy task for " + r.getRegionInfo().getRegionNameAsString());
  }

  /**
   * Helper method for use in tests. Skip the region transition report when there's no master around
   * to receive it.
   */
  private boolean skipReportingTransition(final RegionStateTransitionContext context) {
if(KnobRuntime.check(java.util.UUID.fromString("0d226066-f322-3ae4-9474-e58e002bfbc1"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2c2a3a65-11c3-3e93-aaf5-304c03ab563e"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("masterSystemTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f96be035-2346-3630-bb5e-b923b0fd9d25"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("initiatingMasterActiveTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7756e46f-014a-3a62-846f-cbf074d08016"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d74ee9f-06f9-3b9a-8497-226a48caf07e"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b2bda1c-9f0a-3085-ba8d-d4f1efe168a3"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final TransitionCode code = context.getCode();
    final long openSeqNum = context.getOpenSeqNum();
    long masterSystemTime = context.getMasterSystemTime();
    final RegionInfo[] hris = context.getHris();

    if (code == TransitionCode.OPENED) {
      Preconditions.checkArgument(hris != null && hris.length == 1);
      if (hris[0].isMetaRegion()) {
        LOG.warn(
          "meta table location is stored in master local store, so we can not skip reporting");
        return false;
      } else {
        try {
          MetaTableAccessor.updateRegionLocation(clusterConnection, hris[0], serverName, openSeqNum,
            masterSystemTime);
        } catch (IOException e) {
          LOG.info("Failed to update meta", e);
          return false;
        }
      }
    }
    return true;
  }

  private ReportRegionStateTransitionRequest
    createReportRegionStateTransitionRequest(final RegionStateTransitionContext context) {
if(KnobRuntime.check(java.util.UUID.fromString("58abc6a5-ac8d-3b29-9073-40d60882a662"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("initiatingMasterActiveTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0ed2931-30d2-3554-a1e7-29a2b4e0d74f"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10123fdd-ee47-34a2-bcbb-66ee136cf3e8"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f655ff93-b382-3a17-9403-431d1449ec4c"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("masterSystemTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9a26175e-3fd9-389c-b37f-a4b97731b309"))) {
return null;
}
    final TransitionCode code = context.getCode();
    final long openSeqNum = context.getOpenSeqNum();
    final RegionInfo[] hris = context.getHris();
    final long[] procIds = context.getProcIds();

    ReportRegionStateTransitionRequest.Builder builder =
      ReportRegionStateTransitionRequest.newBuilder();
if(KnobRuntime.check(java.util.UUID.fromString("b1138d0a-7790-38fe-8aed-81eb2c7a731d"))) {
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
    builder.setServer(ProtobufUtil.toServerName(serverName));
    RegionStateTransition.Builder transition = builder.addTransitionBuilder();
if(KnobRuntime.check(java.util.UUID.fromString("4415ec14-7ee0-3182-b66b-10b96fcf32a5"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ddfac47-32b4-3910-9698-55eb3d6f8db2"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea7c4eea-e69b-3240-b69f-cfae594eb061"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36ead07a-819e-3184-99fd-63406f943c55"))) {
try {
    java.lang.reflect.Field field = code.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(code));
    field.set(code, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    transition.setTransitionCode(code);
    if (((KnobRuntime.check(java.util.UUID.fromString("2c9b083c-8af0-3671-8954-dd39fe7f25cc"))) ? ((code) == (TransitionCode.OPENED)) : (((KnobRuntime.check(java.util.UUID.fromString("b4619593-088e-33e0-b4af-3bf04233dfb3"))) ? ((openSeqNum) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("964a1e51-fe57-3424-9b50-792698683de3"))) ? ((openSeqNum) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a36fcea8-a8d2-3f72-b8bc-796929079eeb"))) ? (((code) == (TransitionCode.OPENED)) || ((openSeqNum) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("90937856-d859-347b-bf15-1f5068426aea"))) ? ((code == TransitionCode.OPENED) || ((openSeqNum) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("15d51892-4f5b-3753-b921-26304731d79b"))) ? (((code) != (TransitionCode.OPENED)) && (openSeqNum >= 0)) : (((KnobRuntime.check(java.util.UUID.fromString("cd339147-615c-35b3-a679-cbcfb034bd87"))) ? (((code) == (TransitionCode.OPENED)) || ((openSeqNum) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1a2e9e88-a91b-3c09-83cf-94b6e814377b"))) ? (((code) == (TransitionCode.OPENED)) && ((openSeqNum) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a4f6cbd0-5f14-31c2-a1fc-58f0232cba37"))) ? ((code == TransitionCode.OPENED) && ((openSeqNum) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("90694eb8-6c10-36ad-8ca8-857f84347c14"))) ? ((openSeqNum) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f7edb0d4-763c-377e-ab1d-6dbc11ba7033"))) ? (((code) == (TransitionCode.OPENED)) || (openSeqNum >= 0)) : (((KnobRuntime.check(java.util.UUID.fromString("34a61039-87c0-38ce-a050-1b0f9d18731a"))) ? ((code) != (TransitionCode.OPENED)) : (((KnobRuntime.check(java.util.UUID.fromString("4d64f21d-a435-3d49-a6fc-1ae6112a717d"))) ? (((code) == (TransitionCode.OPENED)) && ((openSeqNum) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7588965c-25e6-396f-a61c-87699f1cc984"))) ? ((code == TransitionCode.OPENED) && ((openSeqNum) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3d883fd3-43db-32cd-97c7-76ebe0d31836"))) ? (((code) == (TransitionCode.OPENED)) && ((openSeqNum) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0ceaaa27-73d7-3a4d-aa38-3dcf015c0717"))) ? ((code == TransitionCode.OPENED) || ((openSeqNum) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91a63881-2292-3f12-8c1f-a86d1a682643"))) ? ((code == TransitionCode.OPENED) && ((openSeqNum) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("77270d73-0d4e-3b13-bb8d-505cc1c09704"))) ? (((code) == (TransitionCode.OPENED)) && ((openSeqNum) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fb72d631-0f60-3676-b251-35b036e50309"))) ? (((code) != (TransitionCode.OPENED)) || ((openSeqNum) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e636e947-7623-3377-ac40-fa001b17f170"))) ? (code == TransitionCode.OPENED) : (((KnobRuntime.check(java.util.UUID.fromString("c8f92e19-0157-3b1a-b90d-5583d06e33a7"))) ? ((code == TransitionCode.OPENED) || (openSeqNum >= 0)) : (((KnobRuntime.check(java.util.UUID.fromString("39cf36e7-e2a2-3ac8-8cd8-f05fb8c34891"))) ? (((code) != (TransitionCode.OPENED)) || ((openSeqNum) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("80293910-c247-36ef-9d39-64c79e183dcd"))) ? (openSeqNum >= 0) : (((KnobRuntime.check(java.util.UUID.fromString("15b7d504-9d6a-3ea0-bf26-2c3e04aaf703"))) ? (((code) != (TransitionCode.OPENED)) && ((openSeqNum) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4118e21b-ef57-308c-b0e7-0a86d2457a0e"))) ? (((code) != (TransitionCode.OPENED)) || ((openSeqNum) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("298217df-f505-3272-b9b7-100ac9498eb0"))) ? (((code) == (TransitionCode.OPENED)) || ((openSeqNum) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca1a02b6-74bd-3a82-8a55-279e4b563984"))) ? (((code) != (TransitionCode.OPENED)) || ((openSeqNum) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("809bd8f0-2121-3340-bda7-98f0cc66a7c6"))) ? ((code == TransitionCode.OPENED) && ((openSeqNum) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5d9b19bc-4a0c-361b-81c4-a2c84047e34e"))) ? (((code) != (TransitionCode.OPENED)) || ((openSeqNum) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f50cca8c-64f1-3dff-8a0c-cb1358885b59"))) ? ((code == TransitionCode.OPENED) && ((openSeqNum) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b1672207-e4de-33dd-a866-22e69a8e51e9"))) ? ((code == TransitionCode.OPENED) || ((openSeqNum) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f6823817-73aa-394b-92db-de68f1a78e2e"))) ? (((code) != (TransitionCode.OPENED)) && ((openSeqNum) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3db63e61-0d99-3a0e-a6fa-b4bc922a1b8b"))) ? (((code) != (TransitionCode.OPENED)) && ((openSeqNum) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0a404f5b-f91a-3bd8-9a75-ee2c9dc8b99d"))) ? (((code) != (TransitionCode.OPENED)) && ((openSeqNum) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c0512a44-c527-37fa-8143-3e7a787fad6c"))) ? ((openSeqNum) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6904b3f1-107c-30c0-9a66-840e1e2a34b8"))) ? (((code) == (TransitionCode.OPENED)) || ((openSeqNum) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3d40320a-a1fb-3c09-b818-a3ca94493083"))) ? (((code) != (TransitionCode.OPENED)) || (openSeqNum >= 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a333bdb0-8f12-3df2-ba4b-9feee1b4fc4b"))) ? (((code) == (TransitionCode.OPENED)) && ((openSeqNum) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b6ad0b3c-815f-34ea-93d1-5071bf5a95b8"))) ? ((code == TransitionCode.OPENED) && (openSeqNum >= 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ac899ac5-5e49-3af9-ad97-5226b3820588"))) ? (((code) == (TransitionCode.OPENED)) || ((openSeqNum) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5676dc81-1e3b-32d1-9ddc-793a12a1bd46"))) ? ((openSeqNum) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7d0a8651-fe26-3cad-905a-e20c4468f9a4"))) ? ((code == TransitionCode.OPENED) || ((openSeqNum) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("357df5c3-0773-328e-a558-f9e5ca79551d"))) ? ((openSeqNum) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f2084328-5813-3106-9a47-57b5f41db65b"))) ? (((code) == (TransitionCode.OPENED)) && ((openSeqNum) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("37e09ec7-e384-389d-b657-0ee391ae3369"))) ? (((code) != (TransitionCode.OPENED)) && ((openSeqNum) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("199d93f4-8d50-3f96-8d91-361f51cd0de1"))) ? ((code == TransitionCode.OPENED) && ((openSeqNum) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d709e3de-7fae-30c8-bc90-2f3670aea7b8"))) ? ((code == TransitionCode.OPENED) || ((openSeqNum) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("43b8e066-9b9a-352d-9883-2e2a35453833"))) ? (((code) == (TransitionCode.OPENED)) && (openSeqNum >= 0)) : (((KnobRuntime.check(java.util.UUID.fromString("796222c4-bfcb-3c38-8420-043f55ae6eee"))) ? (((code) == (TransitionCode.OPENED)) || ((openSeqNum) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8deccdf4-b658-3bcb-bb35-a4994c9cffff"))) ? (((code) != (TransitionCode.OPENED)) || ((openSeqNum) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("638aa2a0-5f83-3f3c-9e73-9b3c7e7bb01f"))) ? ((code == TransitionCode.OPENED) || ((openSeqNum) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5c704612-9a5f-3eca-8127-5becf6799d88"))) ? (((code) != (TransitionCode.OPENED)) && ((openSeqNum) <= (0))) : (code == TransitionCode.OPENED && openSeqNum >= 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("bd3172e2-3204-34d2-b06c-6998047c1ce5"))) { transition.addProcId(openSeqNum); } else if (KnobRuntime.check(java.util.UUID.fromString("faddc49a-7112-3a7b-8081-65d7fec68b9c"))) { transition.setInitiatingMasterActiveTime(openSeqNum); } else { transition.setOpenSeqNum(openSeqNum); }
    }
    for (RegionInfo hri : hris) {
      transition.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
    }
    for (long procId : procIds) {
if(KnobRuntime.check(java.util.UUID.fromString("430984cc-6e40-30a1-b686-292cd8474b9c"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3a784c2a-4024-30d4-828e-6bc82e290b11"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f608a3b-1a6a-3243-9399-2ba89a49a386"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("27f21af2-dadf-3a82-bad8-8b6b43caac15"))) {
procId -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("4b195fc0-cb23-3e95-b5e6-ca0b62500b2d"))) { transition.setInitiatingMasterActiveTime(procId); } else if (KnobRuntime.check(java.util.UUID.fromString("7f075e10-0a90-338b-a077-0bbf4b833f21"))) { transition.setOpenSeqNum(procId); } else { transition.addProcId(procId); }
    }
    if (KnobRuntime.check(java.util.UUID.fromString("1ab62836-acc3-3884-975f-9e2cf2002761"))) { transition.addProcId(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("85902983-7f28-3b0a-8857-84118a7f7765"))) { transition.setInitiatingMasterActiveTime(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("73ffa2fc-fadb-3543-95f2-d8d0b2aaa6ad"))) { transition.setInitiatingMasterActiveTime(getWriteRequestCount()); } else if (KnobRuntime.check(java.util.UUID.fromString("d41f7486-3d65-33d4-8f35-9157cfff13d4"))) { transition.addProcId(getWriteRequestCount()); } else if (KnobRuntime.check(java.util.UUID.fromString("df47948c-4637-35a4-9451-a7e7dc57aa4c"))) { transition.setOpenSeqNum(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("78e8e1c1-0f6d-381c-9340-58aa8609560b"))) { transition.setOpenSeqNum(getWriteRequestCount()); } else { transition.setInitiatingMasterActiveTime(context.getInitiatingMasterActiveTime()); }

    return builder.build();
  }

  @Override
  public boolean reportRegionStateTransition(final RegionStateTransitionContext context) {
if(KnobRuntime.check(java.util.UUID.fromString("6335bc4f-b6d2-313c-91d7-973de97eb0aa"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("masterSystemTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e047387-ab3e-3c2c-8377-2aab8469ac9c"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7778245e-d8b5-3619-987d-bede91dd20cf"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ebacfbd7-0bf7-3d25-857c-bd80a6dcd427"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6456ca3b-cfce-3097-b212-85b114b5b6d1"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("initiatingMasterActiveTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3942d68c-41e9-325e-86f1-da1e26e08c51"))) {
return false;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("b9377f38-69e4-33a5-9035-68969eb4fbcd"))) ? (!TEST_SKIP_REPORTING_TRANSITION) : (TEST_SKIP_REPORTING_TRANSITION))) {
      return skipReportingTransition(context);
    }
    final ReportRegionStateTransitionRequest request =
      createReportRegionStateTransitionRequest(context);

    int tries = 0;
    long pauseTime = this.retryPauseTime;
    // Keep looping till we get an error. We want to send reports even though server is going down.
    // Only go down if clusterConnection is null. It is set to null almost as last thing as the
    // HRegionServer does down.
    while (((KnobRuntime.check(java.util.UUID.fromString("8f8c019f-0259-30d7-8cb4-cdd3b5e280c7"))) ? ((this.clusterConnection != null) && (!this.clusterConnection.isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("e075a19e-4495-3b1d-a2c4-1d545743d133"))) ? (((this.clusterConnection) != (null)) && (!this.clusterConnection.isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("8f4c180a-8112-3d65-a116-ecc2b611d923"))) ? ((this.clusterConnection) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("efc09edb-53d1-39c0-b0f0-9628320f45bb"))) ? (!this.clusterConnection.isClosed()) : (((KnobRuntime.check(java.util.UUID.fromString("6c2279e7-5328-335f-8d43-8d07cab7a539"))) ? (this.clusterConnection != null) : (((KnobRuntime.check(java.util.UUID.fromString("22ab3188-ed9e-3957-a58d-0276f462a246"))) ? (((this.clusterConnection) != (null)) || (!this.clusterConnection.isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("88ffdbbb-2785-36eb-8b70-0bc432f4cb67"))) ? (((this.clusterConnection) == (null)) && (!this.clusterConnection.isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("79dc820b-4903-3ca1-a476-bce9bb373640"))) ? ((this.clusterConnection != null) || (!this.clusterConnection.isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("17f6456f-9183-387f-aff9-69700e778fb9"))) ? (((this.clusterConnection) == (null)) || (!this.clusterConnection.isClosed())) : (((KnobRuntime.check(java.util.UUID.fromString("87e32874-877f-36e0-bc30-a8bc2b8f7fba"))) ? ((this.clusterConnection) == (null)) : (this.clusterConnection != null && !this.clusterConnection.isClosed()))))))))))))))))))))) {
      RegionServerStatusService.BlockingInterface rss = rssStub;
      try {
        if (rss == null) {
          createRegionServerStatusStub();
          continue;
        }
        ReportRegionStateTransitionResponse response =
          rss.reportRegionStateTransition(null, request);
        if (response.hasErrorMessage()) {
          LOG.info("TRANSITION FAILED " + request + ": " + response.getErrorMessage());
          break;
        }
        // Log if we had to retry else don't log unless TRACE. We want to
        // know if were successful after an attempt showed in logs as failed.
        if (tries > 0 || LOG.isTraceEnabled()) {
          LOG.info("TRANSITION REPORTED " + request);
        }
        // NOTE: Return mid-method!!!
        return true;
      } catch (ServiceException se) {
        IOException ioe = ProtobufUtil.getRemoteException(se);
        boolean pause = ioe instanceof ServerNotRunningYetException
          || ioe instanceof PleaseHoldException || ioe instanceof CallQueueTooBigException;
        if (pause) {
          // Do backoff else we flood the Master with requests.
          pauseTime = ConnectionUtils.getPauseTime(this.retryPauseTime, tries);
        } else {
          pauseTime = this.retryPauseTime; // Reset.
        }
        LOG.info("Failed report transition " + TextFormat.shortDebugString(request) + "; retry (#"
          + tries + ")"
          + (pause
            ? " after " + pauseTime + "ms delay (Master is coming online...)."
            : " immediately."),
          ioe);
        if (((KnobRuntime.check(java.util.UUID.fromString("9d45775b-09ba-35e0-9459-dc61bcb87ecf"))) ? (!pause) : (pause))) {
if(KnobRuntime.check(java.util.UUID.fromString("fb31d106-8280-355d-8ae0-84ece6c58040"))) {
pauseTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("482a9570-eac0-394d-9aba-af22854e10a5"))) {
pauseTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("504abae6-ca16-3a6e-8bbe-a29af2cfaf27"))) {
pauseTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a9a2ca47-be59-310a-94a5-ed55c50bbef0"))) {
pauseTime -= 1;
}
          Threads.sleep(pauseTime);
        }
        tries++;
        if (((KnobRuntime.check(java.util.UUID.fromString("2cb176a5-1e82-398f-afc0-90daf5a39f57"))) ? ((rssStub) != (rss)) : (((KnobRuntime.check(java.util.UUID.fromString("5e0c10da-1c66-3b8f-8f01-fee171cde02c"))) ? ((rssStub) == (rss)) : (rssStub == rss))))) {
          rssStub = null;
        }
      }
    }
    return false;
  }

  /**
   * Trigger a flush in the primary region replica if this region is a secondary replica. Does not
   * block this thread. See RegionReplicaFlushHandler for details.
   */
  private void triggerFlushInPrimaryRegion(final HRegion region) {
    if (ServerRegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
      return;
    }
    TableName tn = region.getTableDescriptor().getTableName();
    if (
      !ServerRegionReplicaUtil.isRegionReplicaReplicationEnabled(region.conf, tn)
        || !ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(region.conf) ||
        // If the memstore replication not setup, we do not have to wait for observing a flush event
        // from primary before starting to serve reads, because gaps from replication is not
        // applicable,this logic is from
        // TableDescriptorBuilder.ModifyableTableDescriptor.setRegionMemStoreReplication by
        // HBASE-13063
        !region.getTableDescriptor().hasRegionMemStoreReplication()
    ) {
      region.setReadsEnabled(true);
      return;
    }

    region.setReadsEnabled(false); // disable reads before marking the region as opened.
    // RegionReplicaFlushHandler might reset this.

    // Submit it to be handled by one of the handlers so that we do not block OpenRegionHandler
    if (this.executorService != null) {
      this.executorService.submit(new RegionReplicaFlushHandler(this, clusterConnection,
        rpcRetryingCallerFactory, rpcControllerFactory, operationTimeout, region));
    } else {
      LOG.info("Executor is null; not running flush of primary region replica for {}",
        region.getRegionInfo());
    }
  }

  @Override
  public RpcServerInterface getRpcServer() {
    return rpcServices.rpcServer;
  }

  @InterfaceAudience.Private
  public RSRpcServices getRSRpcServices() {
    return rpcServices;
  }

  /**
   * Cause the server to exit without closing the regions it is serving, the log it is using and
   * without notifying the master. Used unit testing and on catastrophic events such as HDFS is
   * yanked out from under hbase or we OOME. the reason we are aborting the exception that caused
   * the abort, or null
   */
  @Override
  public void abort(String reason, Throwable cause) {
    if (!setAbortRequested()) {
      // Abort already in progress, ignore the new request.
      LOG.debug("Abort already in progress. Ignoring the current request with reason: {}", reason);
      return;
    }
    String msg = "***** ABORTING region server " + this + ": " + reason + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }
    // HBASE-4014: show list of coprocessors that were loaded to help debug
    // regionserver crashes.Note that we're implicitly using
    // java.util.HashSet's toString() method to print the coprocessor names.
    LOG.error(HBaseMarkers.FATAL,
      "RegionServer abort: loaded coprocessors are: " + CoprocessorHost.getLoadedCoprocessors());
    // Try and dump metrics if abort -- might give clue as to how fatal came about....
    try {
      LOG.info("Dump of metrics as JSON on abort: " + DumpRegionServerMetrics.dumpMetrics());
    } catch (MalformedObjectNameException | IOException e) {
      LOG.warn("Failed dumping metrics", e);
    }

    // Do our best to report our abort to the master, but this may not work
    try {
      if (cause != null) {
        msg += "\nCause:\n" + Throwables.getStackTraceAsString(cause);
      }
      // Report to the master but only if we have already registered with the master.
      RegionServerStatusService.BlockingInterface rss = rssStub;
      if (rss != null && this.serverName != null) {
        ReportRSFatalErrorRequest.Builder builder = ReportRSFatalErrorRequest.newBuilder();
        builder.setServer(ProtobufUtil.toServerName(this.serverName));
        builder.setErrorMessage(msg);
        rss.reportRSFatalError(null, builder.build());
      }
    } catch (Throwable t) {
      LOG.warn("Unable to report fatal error to master", t);
    }

    scheduleAbortTimer();
    // shutdown should be run as the internal user
    stop(reason, true, null);
  }

  /**
   * Sets the abort state if not already set.
   * @return True if abortRequested set to True successfully, false if an abort is already in
   *         progress.
   */
  protected boolean setAbortRequested() {
if(KnobRuntime.check(java.util.UUID.fromString("6503ae55-84e3-3d04-8ff1-8d442ba01bfc"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("04dddad0-a782-3249-b2c0-77b1874ffc28"))) {
return true;
}
    return abortRequested.compareAndSet(false, true);
  }

  @Override
  public boolean isAborted() {
    return abortRequested.get();
  }

  /*
   * Simulate a kill -9 of this server. Exits w/o closing regions or cleaninup logs but it does
   * close socket in case want to bring up server on old hostname+port immediately.
   */
  @InterfaceAudience.Private
  protected void kill() {
if(KnobRuntime.check(java.util.UUID.fromString("0fa7ce5e-c970-36db-b3eb-d182c72bcf79"))) {
return;
}
    this.killed = true;
    abort("Simulated kill");
  }

  // Limits the time spent in the shutdown process.
  private void scheduleAbortTimer() {
    if (this.abortMonitor == null) {
      this.abortMonitor = new Timer("Abort regionserver monitor", true);
      TimerTask abortTimeoutTask = null;
      try {
        Constructor<? extends TimerTask> timerTaskCtor =
          Class.forName(conf.get(ABORT_TIMEOUT_TASK, SystemExitWhenAbortTimeout.class.getName()))
            .asSubclass(TimerTask.class).getDeclaredConstructor();
        timerTaskCtor.setAccessible(true);
        abortTimeoutTask = timerTaskCtor.newInstance();
      } catch (Exception e) {
        if (KnobRuntime.check(java.util.UUID.fromString("f7bbe35d-8e75-3ecb-9d59-432bdc7afaf5"))) { abort("Initialize abort timeout task failed", e); } else if (KnobRuntime.check(java.util.UUID.fromString("d50fa269-c54f-3cde-8e0e-3a55639af9ae"))) { LOG.error("Initialize abort timeout task failed", e); } else { LOG.warn("Initialize abort timeout task failed", e); }
      }
      if (abortTimeoutTask != null) {
        abortMonitor.schedule(abortTimeoutTask, conf.getLong(ABORT_TIMEOUT, DEFAULT_ABORT_TIMEOUT));
      }
    }
  }

  protected final void shutdownChore(ScheduledChore chore) {
    if (chore != null) {
      chore.shutdown();
    }
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops have already been
   * called.
   */
  protected void stopServiceThreads() {
    // clean up the scheduled chores
    if (this.choreService != null) {
      shutdownChore(nonceManagerChore);
      shutdownChore(compactionChecker);
      shutdownChore(compactedFileDischarger);
if(KnobRuntime.check(java.util.UUID.fromString("85cf3a35-dc61-3450-8ac7-89fcb1679d13"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e4016fb5-0a83-370e-be1d-d4d544d0077e"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e19a007-872d-3f34-94d9-e038dda08636"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("initialDelay");
    field.setAccessible(true);
    long oldValue = ((long)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f1aa2d0f-e0fd-3939-9d11-b18bdaf37be9"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("timeOfLastRun");
    field.setAccessible(true);
    long oldValue = ((long)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42fa0595-8db1-339f-b409-2efed24be151"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("timeOfThisRun");
    field.setAccessible(true);
    long oldValue = ((long)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("828cac42-eb32-3fb7-b496-de9a39dafd45"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("949cd7e2-0b0a-3667-8f50-19cb83731455"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("initialDelay");
    field.setAccessible(true);
    long oldValue = ((long)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2eb4c50b-0c47-38fe-8281-7615bdb0364a"))) {
try {
    java.lang.reflect.Field field = periodicFlusher.getClass().getDeclaredField("period");
    field.setAccessible(true);
    int oldValue = ((int)field.get(periodicFlusher));
    field.set(periodicFlusher, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      shutdownChore(periodicFlusher);
if(KnobRuntime.check(java.util.UUID.fromString("cb177f7f-2b6f-3919-868e-3e2ff019537a"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("numTimesUnhealthy");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad2bf91e-7f28-388f-8a91-0b3016617193"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("startWindow");
    field.setAccessible(true);
    long oldValue = ((long)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca8b23be-ff8e-334a-9535-fd6793e316ce"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("numTimesUnhealthy");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("780afc38-35b1-330e-8d68-0746916a4bb2"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("numTimesUnhealthy");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0f4b023-40c0-30bc-b973-92e6c0251819"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("failureWindow");
    field.setAccessible(true);
    long oldValue = ((long)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ccc7732e-c936-3905-bdcb-c1b5079b9cc3"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("threshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be5bdb43-fd64-37d4-8940-ad7b07b066b6"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("threshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b95275f4-5d1a-32d2-bf11-8d76e3f7a712"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("threshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54a0fc8b-3d79-3a02-b5f2-5854976a03b5"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("threshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aeddd844-7f39-3c15-836d-c4bee721c7b1"))) {
try {
    java.lang.reflect.Field field = healthCheckChore.getClass().getDeclaredField("numTimesUnhealthy");
    field.setAccessible(true);
    int oldValue = ((int)field.get(healthCheckChore));
    field.set(healthCheckChore, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      shutdownChore(healthCheckChore);
      shutdownChore(storefileRefresher);
      shutdownChore(fsUtilizationChore);
      shutdownChore(namedQueueServiceChore);
      shutdownChore(replicationMarkerChore);
      shutdownChore(rsMobFileCleanerChore);
      // cancel the remaining scheduled chores (in case we missed out any)
      // TODO: cancel will not cleanup the chores, so we need make sure we do not miss any
      choreService.shutdown();
    }
    if (bootstrapNodeManager != null) {
      bootstrapNodeManager.stop();
    }
    if (this.cacheFlusher != null) {
      this.cacheFlusher.shutdown();
    }
    if (this.walRoller != null) {
      this.walRoller.close();
    }
    if (this.compactSplitThread != null) {
      this.compactSplitThread.join();
    }
    if (this.executorService != null) {
      this.executorService.shutdown();
    }
    if (
      this.replicationSourceHandler != null
        && this.replicationSourceHandler == this.replicationSinkHandler
    ) {
      this.replicationSourceHandler.stopReplicationService();
    } else {
      if (this.replicationSourceHandler != null) {
        this.replicationSourceHandler.stopReplicationService();
      }
      if (this.replicationSinkHandler != null) {
        this.replicationSinkHandler.stopReplicationService();
      }
    }
  }

  /** Returns Return the object that implements the replication source executorService. */
  @InterfaceAudience.Private
  public ReplicationSourceService getReplicationSourceService() {
    return replicationSourceHandler;
  }

  /** Returns Return the object that implements the replication sink executorService. */
  ReplicationSinkService getReplicationSinkService() {
    return replicationSinkHandler;
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current rssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @return master + port, or null if server has been stopped
   */
  private synchronized ServerName createRegionServerStatusStub() {
    // Create RS stub without refreshing the master node from ZK, use cached data
    return createRegionServerStatusStub(false);
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current rssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @param refresh If true then master address will be read from ZK, otherwise use cached data
   * @return master + port, or null if server has been stopped
   */
  @InterfaceAudience.Private
  protected synchronized ServerName createRegionServerStatusStub(boolean refresh) {
if(KnobRuntime.check(java.util.UUID.fromString("f42613f5-a8a9-3147-ad34-0ea71e4a5072"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("d474a914-ab2d-3bb0-8d86-66c4325d4118"))) ? ((rssStub) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c1d6adf3-2203-3f70-b825-d5c14e54198e"))) ? ((rssStub) != (null)) : (rssStub != null))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("94d4a0dd-894a-3d5e-aad4-6b179843f772"))) ? (getServerName()) : (((KnobRuntime.check(java.util.UUID.fromString("725ebafa-a934-37f0-9c34-c9ae98cb367c"))) ? (createRegionServerStatusStub()) : (masterAddressTracker.getMasterAddress()))));
    }
    ServerName sn = null;
    long previousLogTime = 0;
    RegionServerStatusService.BlockingInterface intRssStub = null;
    LockService.BlockingInterface intLockStub = null;
    boolean interrupted = false;
    try {
      while (keepLooping()) {
        sn = this.masterAddressTracker.getMasterAddress(refresh);
        if (sn == null) {
          if (!keepLooping()) {
            // give up with no connection.
            LOG.debug("No master found and cluster is stopped; bailing out");
            return null;
          }
          if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
            LOG.debug("No master found; retry");
            previousLogTime = EnvironmentEdgeManager.currentTime();
          }
          refresh = true; // let's try pull it from ZK directly
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
          continue;
        }

        // If we are on the active master, use the shortcut
        if (this instanceof HMaster && sn.equals(getServerName())) {
          intRssStub = ((HMaster) this).getMasterRpcServices();
          intLockStub = ((HMaster) this).getMasterRpcServices();
          break;
        }
        try {
          BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(sn,
            userProvider.getCurrent(), shortOperationTimeout);
          intRssStub = RegionServerStatusService.newBlockingStub(channel);
          intLockStub = LockService.newBlockingStub(channel);
          break;
        } catch (IOException e) {
          if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
            e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
            if (e instanceof ServerNotRunningYetException) {
              LOG.info("Master isn't available yet, retrying");
            } else {
              LOG.warn("Unable to connect to master. Retrying. Error was:", e);
            }
            previousLogTime = EnvironmentEdgeManager.currentTime();
          }
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    this.rssStub = intRssStub;
    this.lockStub = intLockStub;
    return sn;
  }

  /**
   * @return True if we should break loop because cluster is going down or this server has been
   *         stopped or hdfs has gone bad.
   */
  private boolean keepLooping() {
    return !this.stopped && isClusterUp();
  }

  /*
   * Let the master know we're here Run initialization using parameters passed us by the master.
   * @return A Map of key/value configurations we got from the Master else null if we failed to
   * register.
   */
  private RegionServerStartupResponse reportForDuty() throws IOException {
    if (this.masterless) {
      return RegionServerStartupResponse.getDefaultInstance();
    }
    ServerName masterServerName = createRegionServerStatusStub(true);
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (masterServerName == null || rss == null) {
      return null;
    }
    RegionServerStartupResponse result = null;
    try {
      rpcServices.requestCount.reset();
      if (KnobRuntime.check(java.util.UUID.fromString("4892ae13-a50d-3127-8049-0e5633e9f9ce"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("ff0a5dbe-d018-3961-af1c-a4da56344275"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("6b96cfd5-a513-31c5-99e3-19bb1a757a9a"))) { putUpWebUI(); } else { rpcServices.rpcGetRequestCount.reset(); }
      rpcServices.rpcScanRequestCount.reset();
      if (KnobRuntime.check(java.util.UUID.fromString("7b13d218-60bf-39aa-9662-b0c55cf36875"))) { putUpWebUI(); } else if (KnobRuntime.check(java.util.UUID.fromString("038862a6-5ce7-34d5-b947-585a2f68a853"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("ff4c54e7-8c4c-357a-8c7b-e509ac204c7d"))) { setupSignalHandlers(); } else { rpcServices.rpcFullScanRequestCount.reset(); }
      rpcServices.rpcMultiRequestCount.reset();
      if (KnobRuntime.check(java.util.UUID.fromString("d82e46c0-8692-3224-8f64-7696fd9e58ea"))) { initializeFileSystem(); } else if (KnobRuntime.check(java.util.UUID.fromString("0b0218ef-99ca-3ff9-a31e-1cdfdb40af24"))) { setupSignalHandlers(); } else if (KnobRuntime.check(java.util.UUID.fromString("b2b8ef9b-2955-336a-8c17-cc9336798026"))) { putUpWebUI(); } else { rpcServices.rpcMutateRequestCount.reset(); }
      LOG.info("reportForDuty to master=" + masterServerName + " with isa=" + rpcServices.isa
        + ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTime();
      int port = rpcServices.isa.getPort();
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      if (!StringUtils.isBlank(useThisHostnameInstead)) {
        request.setUseThisHostnameInstead(useThisHostnameInstead);
      }
if(KnobRuntime.check(java.util.UUID.fromString("110725cb-cc9c-3f08-848b-30100bddf436"))) {
port += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ee531eeb-6be2-3573-b55b-4b6c24a120d0"))) {
port -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6bb6ae1d-5bdd-3ca5-b45d-f576f067e0a1"))) {
port *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("95d8db31-380c-3438-b60e-91a381e4c7ee"))) {
port = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("69c21fe1-1d2b-3c89-ad30-a579f95ebc53"))) {
port /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f42c2b1-f31a-3960-8b31-17d7bb17d032"))) {
port = -1;
}
      request.setPort(port);
      request.setServerStartCode(this.startcode);
if(KnobRuntime.check(java.util.UUID.fromString("cb03045b-98fd-3414-85e6-d1bbbc664d6c"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("78061903-579b-3e6b-aaf9-687ccebb6b54"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9a5fac0b-7c30-3dae-88fa-dbd3351fb867"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bbc16f5c-e377-39e6-9f24-419e5074ff50"))) {
now += 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("d99f9eb6-5d76-3896-b928-9073b6a6385d"))) { request.setServerStartCode(now); } else { request.setServerCurrentTime(now); }
      result = rss.regionServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.error(HBaseMarkers.FATAL, "Master rejected startup because clock is out of sync", ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else if (ioe instanceof DecommissionedHostRejectedException) {
        LOG.error(HBaseMarkers.FATAL,
          "Master rejected startup because the host is considered decommissioned", ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else if (ioe instanceof ServerNotRunningYetException) {
        LOG.debug("Master is not running yet");
      } else {
        LOG.warn("error telling master we are up", se);
      }
      rssStub = null;
    }
    return result;
  }

  @Override
  public RegionStoreSequenceIds getLastSequenceId(byte[] encodedRegionName) {
    try {
      GetLastFlushedSequenceIdRequest req =
        RequestConverter.buildGetLastFlushedSequenceIdRequest(encodedRegionName);
      RegionServerStatusService.BlockingInterface rss = rssStub;
      if (rss == null) { // Try to connect one more time
        createRegionServerStatusStub();
        rss = rssStub;
        if (rss == null) {
          // Still no luck, we tried
          LOG.warn("Unable to connect to the master to check " + "the last flushed sequence id");
          return RegionStoreSequenceIds.newBuilder().setLastFlushedSequenceId(HConstants.NO_SEQNUM)
            .build();
        }
      }
if(KnobRuntime.check(java.util.UUID.fromString("8a4aeb9a-fe6b-3839-a062-69c8104a62aa"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("276575d9-288e-3226-9d87-80ab47673b1c"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3402a31-8e3f-391d-883e-8b1b1887414a"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6db35cf-1aa6-3252-84c3-f326c6dd580e"))) {
try {
    java.lang.reflect.Field field = req.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(req));
    field.set(req, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      GetLastFlushedSequenceIdResponse resp = rss.getLastFlushedSequenceId(null, req);
      return RegionStoreSequenceIds.newBuilder()
        .setLastFlushedSequenceId(resp.getLastFlushedSequenceId())
        .addAllStoreSequenceId(resp.getStoreLastFlushedSequenceIdList()).build();
    } catch (ServiceException e) {
      LOG.warn("Unable to connect to the master to check the last flushed sequence id", e);
      return RegionStoreSequenceIds.newBuilder().setLastFlushedSequenceId(HConstants.NO_SEQNUM)
        .build();
    }
  }

  /**
   * Close meta region if we carry it
   * @param abort Whether we're running an abort.
   */
  private void closeMetaTableRegions(final boolean abort) {
    HRegion meta = null;
    if (KnobRuntime.check(java.util.UUID.fromString("bc4ceaa3-5cdc-3c96-a962-aff978de19c2"))) { setupSignalHandlers(); } else { this.onlineRegionsLock.writeLock().lock(); }
    try {
      for (Map.Entry<String, HRegion> e : onlineRegions.entrySet()) {
        RegionInfo hri = e.getValue().getRegionInfo();
        if (hri.isMetaRegion()) {
          meta = e.getValue();
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("feaeaf3d-b19f-3f98-99ec-ab1147db2308"))) ? ((meta) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9c6f556c-d3fa-3778-9a4b-9b3b251a1bd5"))) ? ((meta) != (null)) : (meta != null))))) {
          break;
        }
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("cf46ccc9-eafe-370a-b951-867354cfeb88"))) { setupSignalHandlers(); } else { this.onlineRegionsLock.writeLock().unlock(); }
    }
    if (meta != null) {
      closeRegionIgnoreErrors(meta.getRegionInfo(), abort);
    }
  }

  /**
   * Schedule closes on all user regions. Should be safe calling multiple times because it wont'
   * close regions that are already closed or that are closing.
   * @param abort Whether we're running an abort.
   */
  private void closeUserRegions(final boolean abort) {
if(KnobRuntime.check(java.util.UUID.fromString("dd9c6ddb-064d-3013-b70f-6c3f370d0dac"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("3e35b8fa-d944-322d-8faf-51e056e352e9"))) { setupSignalHandlers(); } else { this.onlineRegionsLock.writeLock().lock(); }
    try {
      for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        if (!r.getRegionInfo().isMetaRegion() && r.isAvailable()) {
          // Don't update zk with this close transition; pass false.
          closeRegionIgnoreErrors(r.getRegionInfo(), abort);
        }
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("c59ed80b-715c-3661-8e08-857cd0cd4bff"))) { setupSignalHandlers(); } else { this.onlineRegionsLock.writeLock().unlock(); }
    }
  }

  /** Returns the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  /** Returns true if a stop has been requested. */
  @Override
  public boolean isStopped() {
if(KnobRuntime.check(java.util.UUID.fromString("4d2aacbb-78a6-39c9-9554-044a890cfe56"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2529efd0-1607-34d7-9d0a-4ffff2bce773"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("d64db443-4bfb-36b1-8594-942f51879c84"))) ? (!this.stopped) : (this.stopped));
  }

  @Override
  public boolean isStopping() {
    return this.stopping;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  protected Map<String, HRegion> getOnlineRegions() {
if(KnobRuntime.check(java.util.UUID.fromString("7bf161d3-70c2-3299-8f6d-a3981e59296e"))) {
return null;
}
    return this.onlineRegions;
  }

  public int getNumberOfOnlineRegions() {
    return this.onlineRegions.size();
  }

  /**
   * For tests, web ui and metrics. This method will only work if HRegionServer is in the same JVM
   * as client; HRegion cannot be serialized to cross an rpc.
   */
  public Collection<HRegion> getOnlineRegionsLocalContext() {
    Collection<HRegion> regions = this.onlineRegions.values();
    return Collections.unmodifiableCollection(regions);
  }

  @Override
  public void addRegion(HRegion region) {
    this.onlineRegions.put(region.getRegionInfo().getEncodedName(), region);
    configurationManager.registerObserver(region);
  }

  /**
   * @return A new Map of online regions sorted by region off-heap size with the first entry being
   *         the biggest. If two regions are the same size, then the last one found wins; i.e. this
   *         method may NOT return all regions.
   */
  SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedByOffHeapSize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<>(Comparator.reverseOrder());
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      sortedRegions.put(region.getMemStoreOffHeapSize(), region);
    }
    return sortedRegions;
  }

  /**
   * @return A new Map of online regions sorted by region heap size with the first entry being the
   *         biggest. If two regions are the same size, then the last one found wins; i.e. this
   *         method may NOT return all regions.
   */
  SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedByOnHeapSize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<>(Comparator.reverseOrder());
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      sortedRegions.put(region.getMemStoreHeapSize(), region);
    }
    return sortedRegions;
  }

  /** Returns time stamp in millis of when this region server was started */
  public long getStartcode() {
    return this.startcode;
  }

  /** Returns reference to FlushRequester */
  @Override
  public FlushRequester getFlushRequester() {
    return this.cacheFlusher;
  }

  @Override
  public CompactionRequester getCompactionRequestor() {
    return this.compactSplitThread;
  }

  @Override
  public LeaseManager getLeaseManager() {
    return leaseManager;
  }

  /** Returns Return the rootDir. */
  protected Path getDataRootDir() {
if(KnobRuntime.check(java.util.UUID.fromString("9897ee3b-a938-3cb3-9f84-aa3c07c25a9f"))) {
return null;
}
    return dataRootDir;
  }

  @Override
  public FileSystem getFileSystem() {
if(KnobRuntime.check(java.util.UUID.fromString("103ee738-a509-3bea-97f6-afddecca7e14"))) {
return null;
}
    return dataFs;
  }

  /** Returns {@code true} when the data file system is available, {@code false} otherwise. */
  boolean isDataFileSystemOk() {
    return this.dataFsOk;
  }

  /** Returns Return the walRootDir. */
  public Path getWALRootDir() {
    return walRootDir;
  }

  /** Returns Return the walFs. */
  public FileSystem getWALFileSystem() {
    return walFs;
  }

  @Override
  public String toString() {
    return getServerName().toString();
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
if(KnobRuntime.check(java.util.UUID.fromString("e6b884f9-8cb8-3e31-87f3-3b3621154715"))) {
return null;
}
    return csm;
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  public RegionServerCoprocessorHost getRegionServerCoprocessorHost() {
    return this.rsHost;
  }

  @Override
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
if(KnobRuntime.check(java.util.UUID.fromString("f54771d1-bc14-32a1-bdc0-4825cffcf309"))) {
return null;
}
    return this.regionsInTransitionInRS;
  }

  @Override
  public ExecutorService getExecutorService() {
if(KnobRuntime.check(java.util.UUID.fromString("d9e8b3f2-33a1-3883-bb7f-03c55baed333"))) {
return null;
}
    return executorService;
  }

  @Override
  public ChoreService getChoreService() {
    return choreService;
  }

  @Override
  public RegionServerRpcQuotaManager getRegionServerRpcQuotaManager() {
    return rsQuotaManager;
  }

  //
  // Main program and support routines
  //
  /**
   * Load the replication executorService objects, if any
   */
  private static void createNewReplicationInstance(Configuration conf, HRegionServer server,
    FileSystem walFs, Path walDir, Path oldWALDir, WALFactory walFactory) throws IOException {
    if (
      (server instanceof HMaster)
        && (!LoadBalancer.isTablesOnMaster(conf) || LoadBalancer.isSystemTablesOnlyOnMaster(conf))
    ) {
      return;
    }
    // read in the name of the source replication class from the config file.
    String sourceClassname = conf.get(HConstants.REPLICATION_SOURCE_SERVICE_CLASSNAME,
      HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // read in the name of the sink replication class from the config file.
    String sinkClassname = conf.get(HConstants.REPLICATION_SINK_SERVICE_CLASSNAME,
      HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // If both the sink and the source class names are the same, then instantiate
    // only one object.
    if (sourceClassname.equals(sinkClassname)) {
      server.replicationSourceHandler = newReplicationInstance(sourceClassname,
        ReplicationSourceService.class, conf, server, walFs, walDir, oldWALDir, walFactory);
      server.replicationSinkHandler = (ReplicationSinkService) server.replicationSourceHandler;
    } else {
      server.replicationSourceHandler = newReplicationInstance(sourceClassname,
        ReplicationSourceService.class, conf, server, walFs, walDir, oldWALDir, walFactory);
      server.replicationSinkHandler = newReplicationInstance(sinkClassname,
        ReplicationSinkService.class, conf, server, walFs, walDir, oldWALDir, walFactory);
    }
  }

  private static <T extends ReplicationService> T newReplicationInstance(String classname,
    Class<T> xface, Configuration conf, HRegionServer server, FileSystem walFs, Path logDir,
    Path oldLogDir, WALFactory walFactory) throws IOException {
    final Class<? extends T> clazz;
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      clazz = Class.forName(classname, true, classLoader).asSubclass(xface);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Could not find class for " + classname);
    }
    T service = ReflectionUtils.newInstance(clazz, conf);
    service.initialize(server, walFs, logDir, oldLogDir, walFactory);
    return service;
  }

  public Map<String, ReplicationStatus> getWalGroupsReplicationStatus() {
    Map<String, ReplicationStatus> walGroupsReplicationStatus = new TreeMap<>();
    if (!this.isOnline()) {
      return walGroupsReplicationStatus;
    }
    List<ReplicationSourceInterface> allSources = new ArrayList<>();
    allSources.addAll(replicationSourceHandler.getReplicationManager().getSources());
    if (KnobRuntime.check(java.util.UUID.fromString("03210255-5737-3bfb-9001-4ef571a54da5"))) { allSources.addAll(replicationSourceHandler.getReplicationManager().getSources()); } else { allSources.addAll(replicationSourceHandler.getReplicationManager().getOldSources()); }
    for (ReplicationSourceInterface source : allSources) {
      walGroupsReplicationStatus.putAll(source.getWalGroupStatus());
    }
    return walGroupsReplicationStatus;
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   */
  static HRegionServer constructRegionServer(final Class<? extends HRegionServer> regionServerClass,
    final Configuration conf) {
    try {
      Constructor<? extends HRegionServer> c =
        regionServerClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException(
        "Failed construction of " + "Regionserver: " + regionServerClass.toString(), e);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine
   */
  public static void main(String[] args) {
    LOG.info("STARTING executorService " + HRegionServer.class.getSimpleName());
    VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass = (Class<? extends HRegionServer>) conf
      .getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);

    new HRegionServerCommandLine(regionServerClass).doMain(args);
  }

  /**
   * Gets the online regions of the specified table. This method looks at the in-memory
   * onlineRegions. It does not go to <code>hbase:meta</code>. Only returns <em>online</em> regions.
   * If a region on this table has been closed during a disable, etc., it will not be included in
   * the returned list. So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName table to limit the scope of the query
   * @return Online regions from <code>tableName</code>
   */
  @Override
  public List<HRegion> getRegions(TableName tableName) {
    List<HRegion> tableRegions = new ArrayList<>();
    synchronized (this.onlineRegions) {
      for (HRegion region : this.onlineRegions.values()) {
        RegionInfo regionInfo = region.getRegionInfo();
        if (regionInfo.getTable().equals(tableName)) {
          tableRegions.add(region);
        }
      }
    }
    return tableRegions;
  }

  @Override
  public List<HRegion> getRegions() {
    List<HRegion> allRegions;
    synchronized (this.onlineRegions) {
      // Return a clone copy of the onlineRegions
      allRegions = new ArrayList<>(onlineRegions.values());
    }
    return allRegions;
  }

  /**
   * Gets the online tables in this RS. This method looks at the in-memory onlineRegions.
   * @return all the online tables in this RS
   */
  public Set<TableName> getOnlineTables() {
    Set<TableName> tables = new HashSet<>();
    synchronized (this.onlineRegions) {
      for (Region region : this.onlineRegions.values()) {
        tables.add(region.getTableDescriptor().getTableName());
      }
    }
    return tables;
  }

  public String[] getRegionServerCoprocessors() {
    TreeSet<String> coprocessors = new TreeSet<>();
    try {
if(KnobRuntime.check(java.util.UUID.fromString("cc9d52df-76f8-3fca-a105-f031773eb818"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("6eb9e3c9-387d-3e6d-be7c-67b9ac80e8a8"))) { coprocessors.addAll(CoprocessorHost.getLoadedCoprocessors()); } else { coprocessors.addAll(getWAL(null).getCoprocessorHost().getCoprocessors()); }
    } catch (IOException exception) {
      LOG.warn("Exception attempting to fetch wal coprocessor information for the common wal; "
        + "skipping.");
      LOG.debug("Exception details for failure to fetch wal coprocessor information.", exception);
    }
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    for (HRegion region : regions) {
      coprocessors.addAll(region.getCoprocessorHost().getCoprocessors());
      try {
        coprocessors.addAll(getWAL(region.getRegionInfo()).getCoprocessorHost().getCoprocessors());
      } catch (IOException exception) {
        LOG.warn("Exception attempting to fetch wal coprocessor information for region " + region
          + "; skipping.");
        LOG.debug("Exception details for failure to fetch wal coprocessor information.", exception);
      }
    }
    coprocessors.addAll(rsHost.getCoprocessors());
    return coprocessors.toArray(new String[0]);
  }

  /**
   * Try to close the region, logs a warning on failure but continues.
   * @param region Region to close
   */
  private void closeRegionIgnoreErrors(RegionInfo region, final boolean abort) {
    try {
      if (!closeRegion(region.getEncodedName(), abort, null)) {
        LOG
          .warn("Failed to close " + region.getRegionNameAsString() + " - ignoring and continuing");
      }
    } catch (IOException e) {
      LOG.warn("Failed to close " + region.getRegionNameAsString() + " - ignoring and continuing",
        e);
    }
  }

  /**
   * Close asynchronously a region, can be called from the master or internally by the regionserver
   * when stopping. If called from the master, the region will update the status.
   * <p>
   * If an opening was in progress, this method will cancel it, but will not start a new close. The
   * coprocessors are not called in this case. A NotServingRegionException exception is thrown.
   * </p>
   * <p>
   * If a close was in progress, this new request will be ignored, and an exception thrown.
   * </p>
   * <p>
   * Provides additional flag to indicate if this region blocks should be evicted from the cache.
   * </p>
   * @param encodedName Region to close
   * @param abort       True if we are aborting
   * @param destination Where the Region is being moved too... maybe null if unknown.
   * @return True if closed a region.
   * @throws NotServingRegionException if the region is not online
   */
  protected boolean closeRegion(String encodedName, final boolean abort,
    final ServerName destination) throws NotServingRegionException {
    // Check for permissions to close.
    HRegion actualRegion = this.getRegion(encodedName);
    // Can be null if we're calling close on a region that's not online
    if ((actualRegion != null) && (actualRegion.getCoprocessorHost() != null)) {
      try {
        actualRegion.getCoprocessorHost().preClose(false);
      } catch (IOException exp) {
        if (KnobRuntime.check(java.util.UUID.fromString("1f99e28b-1ba2-3beb-b25c-bf130afce130"))) { LOG.error("Unable to close region: the coprocessor launched an error ", exp); } else if (KnobRuntime.check(java.util.UUID.fromString("066c53e3-a295-3d86-bf24-93f960d7ce09"))) { abort("Unable to close region: the coprocessor launched an error ", exp); } else { LOG.warn("Unable to close region: the coprocessor launched an error ", exp); }
        return false;
      }
    }

    // previous can come back 'null' if not in map.
    final Boolean previous =
      this.regionsInTransitionInRS.putIfAbsent(Bytes.toBytes(encodedName), Boolean.FALSE);

    if (Boolean.TRUE.equals(previous)) {
      LOG.info("Received CLOSE for the region:" + encodedName + " , which we are already "
        + "trying to OPEN. Cancelling OPENING.");
      if (!regionsInTransitionInRS.replace(Bytes.toBytes(encodedName), previous, Boolean.FALSE)) {
        // The replace failed. That should be an exceptional case, but theoretically it can happen.
        // We're going to try to do a standard close then.
        LOG.warn("The opening for region " + encodedName + " was done before we could cancel it."
          + " Doing a standard close now");
        return closeRegion(encodedName, abort, destination);
      }
      // Let's get the region from the online region list again
      actualRegion = this.getRegion(encodedName);
      if (actualRegion == null) { // If already online, we still need to close it.
        LOG.info("The opening previously in progress has been cancelled by a CLOSE request.");
        // The master deletes the znode when it receives this exception.
        throw new NotServingRegionException(
          "The region " + encodedName + " was opening but not yet served. Opening is cancelled.");
      }
    } else if (((KnobRuntime.check(java.util.UUID.fromString("b7ad7e41-e38c-3f37-bc31-ddec9fd8638d"))) ? ((previous) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("69fe1941-1b95-335b-9514-57bde72bc1ad"))) ? ((previous) == (null)) : (previous == null))))) {
      LOG.info("Received CLOSE for {}", encodedName);
    } else if (Boolean.FALSE.equals(previous)) {
      LOG.info("Received CLOSE for the region: " + encodedName
        + ", which we are already trying to CLOSE, but not completed yet");
      return true;
    }

    if (actualRegion == null) {
      LOG.debug("Received CLOSE for a region which is not online, and we're not opening.");
      this.regionsInTransitionInRS.remove(Bytes.toBytes(encodedName));
      // The master deletes the znode when it receives this exception.
      throw new NotServingRegionException(
        "The region " + encodedName + " is not online, and is not opening.");
    }

    CloseRegionHandler crh;
    final RegionInfo hri = actualRegion.getRegionInfo();
    if (hri.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, hri, abort);
    } else {
      crh = new CloseRegionHandler(this, this, hri, abort, destination);
    }
    this.executorService.submit(crh);
    return true;
  }

  /**
   * @return HRegion for the passed binary <code>regionName</code> or null if named region is not
   *         member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    String encodedRegionName = RegionInfo.encodeRegionName(regionName);
    return this.onlineRegions.get(encodedRegionName);
  }

  @Override
  public HRegion getRegion(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }

  @Override
  public boolean removeRegion(final HRegion r, ServerName destination) {
    HRegion toReturn = this.onlineRegions.remove(r.getRegionInfo().getEncodedName());
    metricsRegionServerImpl.requestsCountCache.remove(r.getRegionInfo().getEncodedName());
    if (destination != null) {
      long closeSeqNum = r.getMaxFlushedSeqId();
      if (closeSeqNum == HConstants.NO_SEQNUM) {
        // No edits in WAL for this region; get the sequence number when the region was opened.
        closeSeqNum = r.getOpenSeqNum();
        if (closeSeqNum == HConstants.NO_SEQNUM) {
          closeSeqNum = 0;
        }
      }
      boolean selfMove = ServerName.isSameAddress(destination, this.getServerName());
      addToMovedRegions(r.getRegionInfo().getEncodedName(), destination, closeSeqNum, selfMove);
      if (selfMove) {
        this.regionServerAccounting.getRetainedRegionRWRequestsCnt().put(
          r.getRegionInfo().getEncodedName(),
          new Pair<>(r.getReadRequestsCount(), r.getWriteRequestsCount()));
      }
    }
    this.regionFavoredNodesMap.remove(r.getRegionInfo().getEncodedName());
    return toReturn != null;
  }

  /**
   * Protected Utility method for safely obtaining an HRegion handle.
   * @param regionName Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   */
  protected HRegion getRegion(final byte[] regionName) throws NotServingRegionException {
    String encodedRegionName = RegionInfo.encodeRegionName(regionName);
    return getRegionByEncodedName(regionName, encodedRegionName);
  }

  public HRegion getRegionByEncodedName(String encodedRegionName) throws NotServingRegionException {
if(KnobRuntime.check(java.util.UUID.fromString("051b54ce-bbee-3c83-990d-fef29c35bcdf"))) {
return null;
}
    return getRegionByEncodedName(null, encodedRegionName);
  }

  private HRegion getRegionByEncodedName(byte[] regionName, String encodedRegionName)
    throws NotServingRegionException {
    HRegion region = this.onlineRegions.get(encodedRegionName);
    if (region == null) {
      MovedRegionInfo moveInfo = getMovedRegion(encodedRegionName);
      if (moveInfo != null) {
        throw new RegionMovedException(moveInfo.getServerName(), moveInfo.getSeqNum());
      }
      Boolean isOpening = this.regionsInTransitionInRS.get(Bytes.toBytes(encodedRegionName));
      String regionNameStr =
        regionName == null ? encodedRegionName : Bytes.toStringBinary(regionName);
      if (isOpening != null && isOpening) {
        throw new RegionOpeningException(
          "Region " + regionNameStr + " is opening on " + this.serverName);
      }
      throw new NotServingRegionException(
        "" + regionNameStr + " is not online on " + this.serverName);
    }
    return region;
  }

  /**
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to IOE if it isn't
   * already.
   * @param t   Throwable
   * @param msg Message to log in error. Can be null.
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t, final String msg) {
    // Don't log as error if NSRE; NSRE is 'normal' operation.
    if (t instanceof NotServingRegionException) {
      LOG.debug("NotServingRegionException; " + t.getMessage());
      return t;
    }
    Throwable e = t instanceof RemoteException ? ((RemoteException) t).unwrapRemoteException() : t;
    if (msg == null) {
      LOG.error("", e);
    } else {
      LOG.error(msg, e);
    }
    if (!rpcServices.checkOOME(t)) {
      checkFileSystem();
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("e7ee513c-c217-393b-8d81-b05575854bb5"))) ? (t instanceof RemoteException ? ((RemoteException) t).unwrapRemoteException() : t) : (t));
  }

  /**
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t, final String msg) {
if(KnobRuntime.check(java.util.UUID.fromString("97b74e9b-e824-3551-a217-eb6d444e7a9f"))) {
return null;
}
    return (t instanceof IOException ? (IOException) t
      : msg == null || msg.length() == 0 ? new IOException(t)
      : new IOException(msg, t));
  }

  /**
   * Checks to see if the file system is still accessible. If not, sets abortRequested and
   * stopRequested
   * @return false if file system is not available
   */
  boolean checkFileSystem() {
    if (this.dataFsOk && this.dataFs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.dataFs);
      } catch (IOException e) {
        abort("File System not available", e);
        this.dataFsOk = false;
      }
    }
    return this.dataFsOk;
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName,
    List<org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
    Address[] addr = new Address[favoredNodes.size()];
    // Refer to the comment on the declaration of regionFavoredNodesMap on why
    // it is a map of region name to Address[]
    for (int i = 0; i < favoredNodes.size(); i++) {
      addr[i] = Address.fromParts(favoredNodes.get(i).getHostName(), favoredNodes.get(i).getPort());
    }
    regionFavoredNodesMap.put(encodedRegionName, addr);
  }

  /**
   * Return the favored nodes for a region given its encoded name. Look at the comment around
   * {@link #regionFavoredNodesMap} on why we convert to InetSocketAddress[] here.
   * @param encodedRegionName the encoded region name.
   * @return array of favored locations
   */
  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return Address.toSocketAddress(regionFavoredNodesMap.get(encodedRegionName));
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return this.nonceManager;
  }

  private static class MovedRegionInfo {
    private final ServerName serverName;
    private final long seqNum;

    MovedRegionInfo(ServerName serverName, long closeSeqNum) {
      this.serverName = serverName;
      this.seqNum = closeSeqNum;
    }

    public ServerName getServerName() {
      return serverName;
    }

    public long getSeqNum() {
      return seqNum;
    }
  }

  /**
   * We need a timeout. If not there is a risk of giving a wrong information: this would double the
   * number of network calls instead of reducing them.
   */
  private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

  private void addToMovedRegions(String encodedName, ServerName destination, long closeSeqNum,
    boolean selfMove) {
    if (selfMove) {
      LOG.warn("Not adding moved region record: " + encodedName + " to self.");
      return;
    }
    LOG.info("Adding " + encodedName + " move to " + destination + " record at close sequenceid="
      + closeSeqNum);
    movedRegionInfoCache.put(encodedName, new MovedRegionInfo(destination, closeSeqNum));
  }

  void removeFromMovedRegions(String encodedName) {
    movedRegionInfoCache.invalidate(encodedName);
  }

  @InterfaceAudience.Private
  public MovedRegionInfo getMovedRegion(String encodedRegionName) {
    return movedRegionInfoCache.getIfPresent(encodedRegionName);
  }

  @InterfaceAudience.Private
  public int movedRegionCacheExpiredTime() {
    return TIMEOUT_REGION_MOVED;
  }

  private String getMyEphemeralNodePath() {
    return zooKeeper.getZNodePaths().getRsPath(serverName);
  }

  private boolean isHealthCheckerConfigured() {
    String healthScriptLocation = this.conf.get(HConstants.HEALTH_SCRIPT_LOC);
    return org.apache.commons.lang3.StringUtils.isNotBlank(healthScriptLocation);
  }

  /** Returns the underlying {@link CompactSplit} for the servers */
  public CompactSplit getCompactSplitThread() {
    return this.compactSplitThread;
  }

  CoprocessorServiceResponse execRegionServerService(
    @SuppressWarnings("UnusedParameters") final RpcController controller,
    final CoprocessorServiceRequest serviceRequest) throws ServiceException {
    try {
      ServerRpcController serviceController = new ServerRpcController();
      CoprocessorServiceCall call = serviceRequest.getCall();
      String serviceName = call.getServiceName();
      com.google.protobuf.Service service = coprocessorServiceHandlers.get(serviceName);
      if (service == null) {
        throw new UnknownProtocolException(null,
          "No registered coprocessor executorService found for " + serviceName);
      }
      com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc =
        service.getDescriptorForType();

      String methodName = call.getMethodName();
      com.google.protobuf.Descriptors.MethodDescriptor methodDesc =
        serviceDesc.findMethodByName(methodName);
      if (methodDesc == null) {
        throw new UnknownProtocolException(service.getClass(),
          "Unknown method " + methodName + " called on executorService " + serviceName);
      }

      com.google.protobuf.Message request =
        CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());
      final com.google.protobuf.Message.Builder responseBuilder =
        service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, serviceController, request, message -> {
        if (message != null) {
          responseBuilder.mergeFrom(message);
        }
      });
      IOException exception = CoprocessorRpcUtils.getControllerException(serviceController);
      if (exception != null) {
        throw exception;
      }
      return CoprocessorRpcUtils.getResponse(responseBuilder.build(), HConstants.EMPTY_BYTE_ARRAY);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * May be null if this is a master which not carry table.
   * @return The block cache instance used by the regionserver.
   */
  @Override
  public Optional<BlockCache> getBlockCache() {
    return Optional.ofNullable(this.blockCache);
  }

  /**
   * May be null if this is a master which not carry table.
   * @return The cache for mob files used by the regionserver.
   */
  @Override
  public Optional<MobFileCache> getMobFileCache() {
    return Optional.ofNullable(this.mobFileCache);
  }

  @Override
  public AccessChecker getAccessChecker() {
    return rpcServices.getAccessChecker();
  }

  @Override
  public ZKPermissionWatcher getZKPermissionWatcher() {
    return rpcServices.getZkPermissionWatcher();
  }

  /** Returns : Returns the ConfigurationManager object for testing purposes. */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public ConfigurationManager getConfigurationManager() {
    return configurationManager;
  }

  /** Returns Return table descriptors implementation. */
  @Override
  public TableDescriptors getTableDescriptors() {
    return this.tableDescriptors;
  }

  /**
   * Reload the configuration from disk.
   */
  void updateConfiguration() throws IOException {
    LOG.info("Reloading the configuration from disk.");
    // Reload the configuration from disk.
    preUpdateConfiguration();
    conf.reloadConfiguration();
    configurationManager.notifyAllObservers(conf);
    postUpdateConfiguration();
  }

  protected void preUpdateConfiguration() throws IOException {
    if (rsHost != null) {
      rsHost.preUpdateConfiguration(conf);
    }
  }

  protected void postUpdateConfiguration() throws IOException {
    if (rsHost != null) {
      rsHost.postUpdateConfiguration(conf);
    }
  }

  CacheEvictionStats clearRegionBlockCache(Region region) {
    long evictedBlocks = 0;

    for (Store store : region.getStores()) {
      for (StoreFile hFile : store.getStorefiles()) {
        evictedBlocks += blockCache.evictBlocksByHfileName(hFile.getPath().getName());
      }
    }

    return CacheEvictionStats.builder().withEvictedBlocks(evictedBlocks).build();
  }

  @Override
  public double getCompactionPressure() {
    double max = 0;
    for (Region region : onlineRegions.values()) {
      for (Store store : region.getStores()) {
        double normCount = store.getCompactionPressure();
        if (normCount > max) {
          max = normCount;
        }
      }
    }
    return max;
  }

  @Override
  public HeapMemoryManager getHeapMemoryManager() {
    return hMemManager;
  }

  public MemStoreFlusher getMemStoreFlusher() {
    return cacheFlusher;
  }

  /**
   * For testing
   * @return whether all wal roll request finished for this regionserver
   */
  @InterfaceAudience.Private
  public boolean walRollRequestFinished() {
    return this.walRoller.walRollFinished();
  }

  @Override
  public ThroughputController getFlushThroughputController() {
    return flushThroughputController;
  }

  @Override
  public double getFlushPressure() {
    if (getRegionServerAccounting() == null || cacheFlusher == null) {
      // return 0 during RS initialization
      return 0.0;
    }
    return getRegionServerAccounting().getFlushPressure();
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    ThroughputController old = this.flushThroughputController;
    if (old != null) {
      old.stop("configuration change");
    }
    this.flushThroughputController = FlushThroughputControllerFactory.create(this, newConf);
    try {
      Superusers.initialize(newConf);
    } catch (IOException e) {
      LOG.warn("Failed to initialize SuperUsers on reloading of the configuration");
    }

    // update region server coprocessor if the configuration has changed.
    if (
      CoprocessorConfigurationUtil.checkConfigurationChange(getConfiguration(), newConf,
        CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY)
    ) {
      LOG.info("Update region server coprocessors because the configuration has changed");
      this.rsHost = new RegionServerCoprocessorHost(this, newConf);
    }
  }

  @Override
  public MetricsRegionServer getMetrics() {
    return metricsRegionServer;
  }

  @Override
  public SecureBulkLoadManager getSecureBulkLoadManager() {
    return this.secureBulkLoadManager;
  }

  @Override
  public EntityLock regionLock(final List<RegionInfo> regionInfos, final String description,
    final Abortable abort) {
    return new LockServiceClient(conf, lockStub, clusterConnection.getNonceGenerator())
      .regionLock(regionInfos, description, abort);
  }

  @Override
  public void unassign(byte[] regionName) throws IOException {
    clusterConnection.getAdmin().unassign(regionName, false);
  }

  @Override
  public RegionServerSpaceQuotaManager getRegionServerSpaceQuotaManager() {
    return this.rsSpaceQuotaManager;
  }

  @Override
  public boolean reportFileArchivalForQuotas(TableName tableName,
    Collection<Entry<String, Long>> archivedFiles) {
    if (TEST_SKIP_REPORTING_TRANSITION) {
      return false;
    }
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null || rsSpaceQuotaManager == null) {
      // the current server could be stopping.
      LOG.trace("Skipping file archival reporting to HMaster as stub is null");
      return false;
    }
    try {
      RegionServerStatusProtos.FileArchiveNotificationRequest request =
        rsSpaceQuotaManager.buildFileArchiveRequest(tableName, archivedFiles);
      rss.reportFileArchival(null, request);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof PleaseHoldException) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Failed to report file archival(s) to Master because it is initializing."
            + " This will be retried.", ioe);
        }
        // The Master is coming up. Will retry the report later. Avoid re-creating the stub.
        return false;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      // re-create the stub if we failed to report the archival
      createRegionServerStatusStub(true);
      LOG.debug("Failed to report file archival(s) to Master. This will be retried.", ioe);
      return false;
    }
    return true;
  }

  public NettyEventLoopGroupConfig getEventLoopGroupConfig() {
    return eventLoopGroupConfig;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    User user = UserProvider.instantiate(conf).getCurrent();
    return ServerConnectionUtils.createShortCircuitConnection(conf, user, this.serverName,
      this.rpcServices, this.rpcServices, new RegionServerRegistry(this));
  }

  void executeProcedure(long procId, long initiatingMasterActiveTime,
    RSProcedureCallable callable) {
    executorService
      .submit(new RSProcedureHandler(this, procId, initiatingMasterActiveTime, callable));
  }

  public void remoteProcedureComplete(long procId, long initiatingMasterActiveTime,
    Throwable error) {
    procedureResultReporter.complete(procId, initiatingMasterActiveTime, error);
  }

  void reportProcedureDone(ReportProcedureDoneRequest request) throws IOException {
    RegionServerStatusService.BlockingInterface rss;
    // TODO: juggling class state with an instance variable, outside of a synchronized block :'(
    for (;;) {
      rss = rssStub;
      if (rss != null) {
        break;
      }
      createRegionServerStatusStub();
    }
    try {
      rss.reportProcedureDone(null, request);
    } catch (ServiceException se) {
      if (rssStub == rss) {
        rssStub = null;
      }
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Will ignore the open/close region procedures which already submitted or executed. When master
   * had unfinished open/close region procedure and restarted, new active master may send duplicate
   * open/close region request to regionserver. The open/close request is submitted to a thread pool
   * and execute. So first need a cache for submitted open/close region procedures. After the
   * open/close region request executed and report region transition succeed, cache it in executed
   * region procedures cache. See {@link #finishRegionProcedure(long)}. After report region
   * transition succeed, master will not send the open/close region request to regionserver again.
   * And we thought that the ongoing duplicate open/close region request should not be delayed more
   * than 600 seconds. So the executed region procedures cache will expire after 600 seconds. See
   * HBASE-22404 for more details.
   * @param procId the id of the open/close region procedure
   * @return true if the procedure can be submitted.
   */
  boolean submitRegionProcedure(long procId) {
    if (procId == -1) {
      return true;
    }
    // Ignore the region procedures which already submitted.
    Long previous = submittedRegionProcedures.putIfAbsent(procId, procId);
    if (previous != null) {
      LOG.warn("Received procedure pid={}, which already submitted, just ignore it", procId);
      return false;
    }
    // Ignore the region procedures which already executed.
    if (executedRegionProcedures.getIfPresent(procId) != null) {
      LOG.warn("Received procedure pid={}, which already executed, just ignore it", procId);
      return false;
    }
    return true;
  }

  /**
   * See {@link #submitRegionProcedure(long)}.
   * @param procId the id of the open/close region procedure
   */
  public void finishRegionProcedure(long procId) {
    executedRegionProcedures.put(procId, procId);
    submittedRegionProcedures.remove(procId);
  }

  public boolean isShutDown() {
    return shutDown;
  }

  /**
   * Force to terminate region server when abort timeout.
   */
  private static class SystemExitWhenAbortTimeout extends TimerTask {

    public SystemExitWhenAbortTimeout() {
    }

    @Override
    public void run() {
      LOG.warn("Aborting region server timed out, terminating forcibly"
        + " and does not wait for any running shutdown hooks or finalizers to finish their work."
        + " Thread dump to stdout.");
      Threads.printThreadInfo(System.out, "Zombie HRegionServer");
      Runtime.getRuntime().halt(1);
    }
  }

  @InterfaceAudience.Private
  public CompactedHFilesDischarger getCompactedHFilesDischarger() {
    return compactedFileDischarger;
  }

  /**
   * Return pause time configured in {@link HConstants#HBASE_RPC_SHORTOPERATION_RETRY_PAUSE_TIME}}
   * @return pause time
   */
  @InterfaceAudience.Private
  public long getRetryPauseTime() {
    return this.retryPauseTime;
  }

  public Optional<ServerName> getActiveMaster() {
    return Optional.ofNullable(masterAddressTracker.getMasterAddress());
  }

  public List<ServerName> getBackupMasters() {
    return masterAddressTracker.getBackupMasters();
  }

  public Iterator<ServerName> getBootstrapNodes() {
    return bootstrapNodeManager.getBootstrapNodes().iterator();
  }

  public MetaRegionLocationCache getMetaRegionLocationCache() {
    return this.metaRegionLocationCache;
  }

  @InterfaceAudience.Private
  public BrokenStoreFileCleaner getBrokenStoreFileCleaner() {
    return brokenStoreFileCleaner;
  }

  @InterfaceAudience.Private
  public RSMobFileCleanerChore getRSMobFileCleanerChore() {
    return rsMobFileCleanerChore;
  }

  RSSnapshotVerifier getRsSnapshotVerifier() {
    return rsSnapshotVerifier;
  }
}
