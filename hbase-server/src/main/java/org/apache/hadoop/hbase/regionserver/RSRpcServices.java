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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CacheEvictionStatsBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.QosPriority;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcCallback;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.namequeues.NamedQueuePayload;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.RpcLogDetails;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.quotas.ActivePolicyEnforcement;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.LeaseManager.Lease;
import org.apache.hadoop.hbase.regionserver.LeaseManager.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.handler.AssignRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenPriorityRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.UnassignRegionHandler;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.NoopAccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReservoirSample;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.wal.WALSplitUtil.MutationReplay;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponseRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetCachedFilesListRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetCachedFilesListResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RemoteProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponseRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.BootstrapNodeService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.GetAllBootstrapNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos.GetAllBootstrapNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRegionLoadStats;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameInt64Pair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MapReduceProtos.ScanMetrics;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse.TableQuotaSnapshot;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersResponseEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog.SlowLogPayload;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;

/**
 * Implements the regionserver RPC services.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class RSRpcServices implements HBaseRPCErrorHandler, AdminService.BlockingInterface,
  ClientService.BlockingInterface, ClientMetaService.BlockingInterface,
  BootstrapNodeService.BlockingInterface, PriorityFunction, ConfigurationObserver {
  protected static final Logger LOG = LoggerFactory.getLogger(RSRpcServices.class);

  /** RPC scheduler to use for the region server. */
  public static final String REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
    "hbase.region.server.rpc.scheduler.factory.class";

  /** RPC scheduler to use for the master. */
  public static final String MASTER_RPC_SCHEDULER_FACTORY_CLASS =
    "hbase.master.rpc.scheduler.factory.class";

  /**
   * Minimum allowable time limit delta (in milliseconds) that can be enforced during scans. This
   * configuration exists to prevent the scenario where a time limit is specified to be so
   * restrictive that the time limit is reached immediately (before any cells are scanned).
   */
  private static final String REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA =
    "hbase.region.server.rpc.minimum.scan.time.limit.delta";
  /**
   * Default value of {@link RSRpcServices#REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA}
   */
  static final long DEFAULT_REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA = 10;

  /**
   * Whether to reject rows with size > threshold defined by
   * {@link HConstants#BATCH_ROWS_THRESHOLD_NAME}
   */
  private static final String REJECT_BATCH_ROWS_OVER_THRESHOLD =
    "hbase.rpc.rows.size.threshold.reject";

  /**
   * Default value of config {@link RSRpcServices#REJECT_BATCH_ROWS_OVER_THRESHOLD}
   */
  private static final boolean DEFAULT_REJECT_BATCH_ROWS_OVER_THRESHOLD = false;

  public static final String CLIENT_BOOTSTRAP_NODE_LIMIT = "hbase.client.bootstrap.node.limit";

  public static final int DEFAULT_CLIENT_BOOTSTRAP_NODE_LIMIT = 10;

  // Request counter. (Includes requests that are not serviced by regions.)
  // Count only once for requests with multiple actions like multi/caching-scan/replayBatch
  final LongAdder requestCount = new LongAdder();

  // Request counter for rpc get
  final LongAdder rpcGetRequestCount = new LongAdder();

  // Request counter for rpc scan
  final LongAdder rpcScanRequestCount = new LongAdder();

  // Request counter for scans that might end up in full scans
  final LongAdder rpcFullScanRequestCount = new LongAdder();

  // Request counter for rpc multi
  final LongAdder rpcMultiRequestCount = new LongAdder();

  // Request counter for rpc mutate
  final LongAdder rpcMutateRequestCount = new LongAdder();

  // Server to handle client requests.
  final RpcServerInterface rpcServer;
  final InetSocketAddress isa;

  protected final HRegionServer regionServer;
  private volatile long maxScannerResultSize;

  // The reference to the priority extraction function
  private final PriorityFunction priority;

  private ScannerIdGenerator scannerIdGenerator;
  private final ConcurrentMap<String, RegionScannerHolder> scanners = new ConcurrentHashMap<>();
  // Hold the name and last sequence number of a closed scanner for a while. This is used
  // to keep compatible for old clients which may send next or close request to a region
  // scanner which has already been exhausted. The entries will be removed automatically
  // after scannerLeaseTimeoutPeriod.
  private final Cache<String, Long> closedScanners;
  /**
   * The lease timeout period for client scanners (milliseconds).
   */
  private final int scannerLeaseTimeoutPeriod;

  /**
   * The RPC timeout period (milliseconds)
   */
  private final int rpcTimeout;

  /**
   * The minimum allowable delta to use for the scan limit
   */
  private final long minimumScanTimeLimitDelta;

  /**
   * Row size threshold for multi requests above which a warning is logged
   */
  private volatile int rowSizeWarnThreshold;
  /*
   * Whether we should reject requests with very high no of rows i.e. beyond threshold defined by
   * rowSizeWarnThreshold
   */
  private volatile boolean rejectRowsWithSizeOverThreshold;

  final AtomicBoolean clearCompactionQueues = new AtomicBoolean(false);

  private AccessChecker accessChecker;
  private ZKPermissionWatcher zkPermissionWatcher;

  /**
   * Services launched in RSRpcServices. By default they are on but you can use the below booleans
   * to selectively enable/disable these services (Rare is the case where you would ever turn off
   * one or the other).
   */
  public static final String REGIONSERVER_ADMIN_SERVICE_CONFIG =
    "hbase.regionserver.admin.executorService";
  public static final String REGIONSERVER_CLIENT_SERVICE_CONFIG =
    "hbase.regionserver.client.executorService";
  public static final String REGIONSERVER_CLIENT_META_SERVICE_CONFIG =
    "hbase.regionserver.client.meta.executorService";
  public static final String REGIONSERVER_BOOTSTRAP_NODES_SERVICE_CONFIG =
    "hbase.regionserver.bootstrap.nodes.executorService";

  /**
   * An Rpc callback for closing a RegionScanner.
   */
  private static final class RegionScannerCloseCallBack implements RpcCallback {

    private final RegionScanner scanner;

    public RegionScannerCloseCallBack(RegionScanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public void run() throws IOException {
      this.scanner.close();
    }
  }

  /**
   * An Rpc callback for doing shipped() call on a RegionScanner.
   */
  private class RegionScannerShippedCallBack implements RpcCallback {
    private final String scannerName;
    private final Shipper shipper;
    private final Lease lease;

    public RegionScannerShippedCallBack(String scannerName, Shipper shipper, Lease lease) {
      this.scannerName = scannerName;
      this.shipper = shipper;
      this.lease = lease;
    }

    @Override
    public void run() throws IOException {
      this.shipper.shipped();
      // We're done. On way out re-add the above removed lease. The lease was temp removed for this
      // Rpc call and we are at end of the call now. Time to add it back.
      if (scanners.containsKey(scannerName)) {
        if (lease != null) {
          regionServer.getLeaseManager().addLease(lease);
        }
      }
    }
  }

  /**
   * An RpcCallBack that creates a list of scanners that needs to perform callBack operation on
   * completion of multiGets.
   */
  static class RegionScannersCloseCallBack implements RpcCallback {
    private final List<RegionScanner> scanners = new ArrayList<>();

    public void addScanner(RegionScanner scanner) {
if(KnobRuntime.check(java.util.UUID.fromString("b1e221ee-9b42-35ec-8c86-c56ff256ad53"))) {
return;
}
      this.scanners.add(scanner);
    }

    @Override
    public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("91bb632b-7680-37ee-87b9-6f4367a29895"))) {
return;
}
      for (RegionScanner scanner : scanners) {
        try {
          scanner.close();
        } catch (IOException e) {
          if (KnobRuntime.check(java.util.UUID.fromString("f8d92cf7-9848-34d3-b3cf-4de1ca9fe77c"))) { LOG.error("Exception while closing the scanner ", e); } else if (KnobRuntime.check(java.util.UUID.fromString("3557f17a-056d-3b67-a520-4db880fcc1d5"))) { LOG.error(("Exception while closing the scanner ") + (scanner), e); } else { LOG.error("Exception while closing the scanner " + scanner, e); }
        }
      }
    }
  }

  static class RegionScannerContext {
    final String scannerName;
    final RegionScannerHolder holder;
    final OperationQuota quota;

    RegionScannerContext(String scannerName, RegionScannerHolder holder, OperationQuota quota) {
      this.scannerName = scannerName;
      this.holder = holder;
      this.quota = quota;
    }
  }

  /**
   * Holder class which holds the RegionScanner, nextCallSeq and RpcCallbacks together.
   */
  static final class RegionScannerHolder {
    private final AtomicLong nextCallSeq = new AtomicLong(0);
    private final RegionScanner s;
    private final HRegion r;
    private final RpcCallback closeCallBack;
    private final RpcCallback shippedCallback;
    private byte[] rowOfLastPartialResult;
    private boolean needCursor;
    private boolean fullRegionScan;
    private final String clientIPAndPort;
    private final String userName;
    private volatile long maxBlockBytesScanned = 0;
    private volatile long prevBlockBytesScanned = 0;
    private volatile long prevBlockBytesScannedDifference = 0;

    RegionScannerHolder(RegionScanner s, HRegion r, RpcCallback closeCallBack,
      RpcCallback shippedCallback, boolean needCursor, boolean fullRegionScan,
      String clientIPAndPort, String userName) {
      this.s = s;
      this.r = r;
      this.closeCallBack = closeCallBack;
      this.shippedCallback = shippedCallback;
      this.needCursor = needCursor;
      this.fullRegionScan = fullRegionScan;
      this.clientIPAndPort = clientIPAndPort;
      this.userName = userName;
    }

    long getNextCallSeq() {
      return nextCallSeq.get();
    }

    boolean incNextCallSeq(long currentSeq) {
      // Use CAS to prevent multiple scan request running on the same scanner.
      return nextCallSeq.compareAndSet(currentSeq, currentSeq + 1);
    }

    long getMaxBlockBytesScanned() {
      return maxBlockBytesScanned;
    }

    long getPrevBlockBytesScannedDifference() {
      return prevBlockBytesScannedDifference;
    }

    void updateBlockBytesScanned(long blockBytesScanned) {
      prevBlockBytesScannedDifference = blockBytesScanned - prevBlockBytesScanned;
      prevBlockBytesScanned = blockBytesScanned;
      if (blockBytesScanned > maxBlockBytesScanned) {
        maxBlockBytesScanned = blockBytesScanned;
      }
    }

    // Should be called only when we need to print lease expired messages otherwise
    // cache the String once made.
    @Override
    public String toString() {
      return "clientIPAndPort=" + this.clientIPAndPort + ", userName=" + this.userName
        + ", regionInfo=" + this.r.getRegionInfo().getRegionNameAsString();
    }
  }

  /**
   * Instantiated as a scanner lease. If the lease times out, the scanner is closed
   */
  private class ScannerListener implements LeaseListener {
    private final String scannerName;

    ScannerListener(final String n) {
      this.scannerName = n;
    }

    @Override
    public void leaseExpired() {
      RegionScannerHolder rsh = scanners.remove(this.scannerName);
      if (rsh == null) {
        LOG.warn("Scanner lease {} expired but no outstanding scanner", this.scannerName);
        return;
      }
      LOG.info("Scanner lease {} expired {}", this.scannerName, rsh);
      regionServer.getMetrics().incrScannerLeaseExpired();
      RegionScanner s = rsh.s;
      HRegion region = null;
      try {
        region = regionServer.getRegion(s.getRegionInfo().getRegionName());
        if (region != null && region.getCoprocessorHost() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("cb564e57-4730-3b5e-bd64-5d59207f02d3"))) {
throw new java.io.IOException("Injected exception");
}
          region.getCoprocessorHost().preScannerClose(s);
        }
      } catch (IOException e) {
        LOG.error("Closing scanner {} {}", this.scannerName, rsh, e);
      } finally {
        try {
          s.close();
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().postScannerClose(s);
          }
        } catch (IOException e) {
          LOG.error("Closing scanner {} {}", this.scannerName, rsh, e);
        }
      }
    }
  }

  private static ResultOrException getResultOrException(final ClientProtos.Result r,
    final int index) {
if(KnobRuntime.check(java.util.UUID.fromString("172438de-a953-3352-a00f-a5f0a13f12f3"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c73a3577-c5e5-34aa-b52b-cbba43e4c419"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("exists_");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(r);
    field.set(r, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("925c4891-1f48-3a50-bccf-ac2c2fbcd29e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07b4a903-c778-3c5a-9d7d-2df6905d322b"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("193cb60d-dad0-3b72-abb1-bf63de949de8"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3516547c-a27b-30b7-a465-55568a2e7a7a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55f10b3c-bf0d-3996-a164-410a46df5e8a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e55d637-13f1-33af-a7dd-877f7988cdb3"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0ee0bab3-6273-3890-a984-144045261a48"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ffc6bc2c-a902-3b28-95da-88a57200ef10"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return getResultOrException(ResponseConverter.buildActionResult(r), index);
  }

  private static ResultOrException getResultOrException(final Exception e, final int index) {
if(KnobRuntime.check(java.util.UUID.fromString("b18e178f-a895-30a1-a200-9dc7078761f9"))) {
return null;
}
    return getResultOrException(ResponseConverter.buildActionResult(e), index);
  }

  private static ResultOrException getResultOrException(final ResultOrException.Builder builder,
    final int index) {
    return builder.setIndex(index).build();
  }

  /**
   * Checks for the following pre-checks in order:
   * <ol>
   * <li>RegionServer is running</li>
   * <li>If authorization is enabled, then RPC caller has ADMIN permissions</li>
   * </ol>
   * @param requestName name of rpc request. Used in reporting failures to provide context.
   * @throws ServiceException If any of the above listed pre-check fails.
   */
  private void rpcPreCheck(String requestName) throws ServiceException {
    try {
      checkOpen();
      requirePermission(requestName, Permission.Action.ADMIN);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  private boolean isClientCellBlockSupport(RpcCallContext context) {
    return context != null && context.isClientCellBlockSupported();
  }

  private void addResult(final MutateResponse.Builder builder, final Result result,
    final HBaseRpcController rpcc, boolean clientCellBlockSupported) {
    if (result == null) return;
    if (clientCellBlockSupported) {
      builder.setResult(ProtobufUtil.toResultNoData(result));
      rpcc.setCellScanner(result.cellScanner());
    } else {
      ClientProtos.Result pbr = ProtobufUtil.toResult(result);
      builder.setResult(pbr);
    }
  }

  private void addResults(ScanResponse.Builder builder, List<Result> results,
    HBaseRpcController controller, boolean isDefaultRegion, boolean clientCellBlockSupported) {
    builder.setStale(!isDefaultRegion);
    if (results.isEmpty()) {
      return;
    }
    if (clientCellBlockSupported) {
      for (Result res : results) {
        builder.addCellsPerResult(res.size());
        builder.addPartialFlagPerResult(res.mayHaveMoreCellsInRow());
      }
      controller.setCellScanner(CellUtil.createCellScanner(results));
    } else {
      for (Result res : results) {
        ClientProtos.Result pbr = ProtobufUtil.toResult(res);
        builder.addResults(pbr);
      }
    }
  }

  private CheckAndMutateResult checkAndMutate(HRegion region, List<ClientProtos.Action> actions,
    CellScanner cellScanner, Condition condition, long nonceGroup,
    ActivePolicyEnforcement spaceQuotaEnforcement) throws IOException {
    int countOfCompleteMutation = 0;
    try {
      if (!region.getRegionInfo().isMetaRegion()) {
        regionServer.getMemStoreFlusher().reclaimMemStoreMemory();
      }
      List<Mutation> mutations = new ArrayList<>();
      long nonce = HConstants.NO_NONCE;
      for (ClientProtos.Action action : actions) {
        if (action.hasGet()) {
          throw new DoNotRetryIOException(
            "Atomic put and/or delete only, not a Get=" + action.getGet());
        }
        MutationProto mutation = action.getMutation();
        MutationType type = mutation.getMutateType();
        switch (type) {
          case PUT:
            Put put = ProtobufUtil.toPut(mutation, cellScanner);
            ++countOfCompleteMutation;
            checkCellSizeLimit(region, put);
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(put);
            mutations.add(put);
            break;
          case DELETE:
            Delete del = ProtobufUtil.toDelete(mutation, cellScanner);
            ++countOfCompleteMutation;
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(del);
            mutations.add(del);
            break;
          case INCREMENT:
            Increment increment = ProtobufUtil.toIncrement(mutation, cellScanner);
            nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
            ++countOfCompleteMutation;
            checkCellSizeLimit(region, increment);
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(increment);
            mutations.add(increment);
            break;
          case APPEND:
            Append append = ProtobufUtil.toAppend(mutation, cellScanner);
            nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
            ++countOfCompleteMutation;
            checkCellSizeLimit(region, append);
            spaceQuotaEnforcement.getPolicyEnforcement(region).check(append);
            mutations.add(append);
            break;
          default:
            throw new DoNotRetryIOException("invalid mutation type : " + type);
        }
      }

      if (mutations.size() == 0) {
        return new CheckAndMutateResult(true, null);
      } else {
        CheckAndMutate checkAndMutate = ProtobufUtil.toCheckAndMutate(condition, mutations);
        CheckAndMutateResult result = null;
        if (region.getCoprocessorHost() != null) {
          result = region.getCoprocessorHost().preCheckAndMutate(checkAndMutate);
        }
        if (result == null) {
          result = region.checkAndMutate(checkAndMutate, nonceGroup, nonce);
          if (region.getCoprocessorHost() != null) {
            result = region.getCoprocessorHost().postCheckAndMutate(checkAndMutate, result);
          }
        }
        return result;
      }
    } finally {
      // Currently, the checkAndMutate isn't supported by batch so it won't mess up the cell scanner
      // even if the malformed cells are not skipped.
      for (int i = countOfCompleteMutation; i < actions.size(); ++i) {
        skipCellsForMutation(actions.get(i), cellScanner);
      }
    }
  }

  /**
   * Execute an append mutation.
   * @return result to return to client if default operation should be bypassed as indicated by
   *         RegionObserver, null otherwise
   */
  private Result append(final HRegion region, final OperationQuota quota,
    final MutationProto mutation, final CellScanner cellScanner, long nonceGroup,
    ActivePolicyEnforcement spaceQuota, RpcCallContext context) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    Append append = ProtobufUtil.toAppend(mutation, cellScanner);
    checkCellSizeLimit(region, append);
    spaceQuota.getPolicyEnforcement(region).check(append);
    quota.addMutation(append);
    long blockBytesScannedBefore = context != null ? context.getBlockBytesScanned() : 0;
    long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
    Result r = region.append(append, nonceGroup, nonce);
    if (regionServer.getMetrics() != null) {
      long blockBytesScanned =
        context != null ? context.getBlockBytesScanned() - blockBytesScannedBefore : 0;
if(KnobRuntime.check(java.util.UUID.fromString("ffc1e00f-9dbd-3882-9db5-3998610f5591"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a77d9a5e-89b6-31f3-9219-3b1b7f3ac180"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4de56e6b-ea0e-39d0-8d32-3c8f99a26c96"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aea55072-62b2-3371-83ab-ea840b9ef7e5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("50a15d88-52b3-3a5e-b46e-916be7f132c5"))) {
blockBytesScanned = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fb702732-f7a0-3a88-a8b6-809995db0014"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("40cbd0cc-889e-3791-b09e-f4e58a36880d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("059c5ad3-f8ca-3348-a688-99ad0b771a36"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("441b6217-a07f-3782-81d4-12d110255989"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("431b8d2f-dfe3-3a6a-bd51-4f2d206732e0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("da6f5f51-e545-3d3f-b7b9-1e237a512f2a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d9b39089-7d22-3b8f-bc97-fb0b9d0fd61b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c44d11d5-d832-3e65-9c66-dc3a7344dfe9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f4eeae0-c79c-38d2-bdfa-8bda500538ef"))) {
blockBytesScanned -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("90ceaa66-61c4-38ab-ba08-842a87f36c3f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bc08a186-3037-33b8-9865-062a0a0116ab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f953fb06-1d50-3d35-a883-e28a2e9e3cd9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("228f6e4b-70c7-3ce4-87e4-19633d0b8d15"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aafb5f4a-e660-39d3-8d91-b240cd740bfe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("617d4da1-12cc-3e3e-a670-22eea3fc4c55"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f4a250e-f8f2-3eda-a0ea-d3225ef8fee2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e52d5615-8018-3b56-80f3-27ea32d2a6b3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("89c5db7e-99a6-3c2a-8908-e785a96a03cb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("805a63d3-21f4-3655-9f4a-d07cd9713ae5"))) {
blockBytesScanned = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("84c4d1a3-9022-3c69-920d-9fed16471399"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d58cc3e1-f8dd-3386-b03b-2a71132977af"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fab1da10-eaa2-3ad7-9bac-d06a87b8a317"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b9c66621-6661-372b-b1b8-e9fb3ad12678"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("637e8cc7-6c59-3262-bf4b-de04ee67bd5d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f0521361-cf9e-30d7-b264-498eb6963ca5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea2b0a16-f48e-30ce-ac18-378ddfdac50a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a43484f6-8cbf-3800-9720-1386dfd5ba47"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5d61c57e-eac4-3f11-9be1-a52c17ac3bdb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9f8aacbe-9750-3057-8ec1-9a1d0b21fde8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("426f6a2b-f0be-39f2-887e-71bba65c5d1b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c802ff2f-58cd-394b-868c-b48bd79404b2"))) {
blockBytesScanned += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b232a7c6-673b-3d4a-8ba6-698f5a02112d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5a365053-2252-33cd-be74-4ed6ed2e4421"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("754c685d-5d55-3ea9-b025-0d21e7078bb0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4b0f67c5-9db3-36cf-a80c-1dad84ea5142"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b46fb0f-aca9-3c86-a394-5df3f5c08722"))) {
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
      regionServer.getMetrics().updateAppend(region, EnvironmentEdgeManager.currentTime() - before,
        blockBytesScanned);
    }
    return r == null ? Result.EMPTY_RESULT : r;
  }

  /**
   * Execute an increment mutation.
   */
  private Result increment(final HRegion region, final OperationQuota quota,
    final MutationProto mutation, final CellScanner cells, long nonceGroup,
    ActivePolicyEnforcement spaceQuota, RpcCallContext context) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("069cb3b2-ca99-34c5-953c-51e30d06e3b8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6baeeeb3-5b8a-34b0-baf9-d018a8717451"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("50054917-2fe2-3388-b446-479731734f43"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ae2cd3aa-f955-3d33-abcc-61919b1b3d2e"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c2354ea8-4d65-3157-856f-0ed6aee040bd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c3fe3af3-e656-3bd6-bee1-ccdb870b8959"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95a28ec5-7f76-3ecb-8680-ec2c5efadaf7"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a362a61a-3df9-31ca-ac6a-7d8996e51aeb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3743a7ad-3c27-3a72-bf2f-d19f89ce5f88"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b50e72e8-2379-34ce-96ed-916f5fcadc26"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b4822e13-3e1e-3957-a873-80c56dd831da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("94311c18-1dc6-3bfc-b435-61e81b141600"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3f324c6-7604-3e31-b305-5c79b943e5fd"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2713b69-4cca-3730-80ee-27db734d438f"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f3aa9d4-a6cf-3a06-b5e0-2220e5e1c551"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("29f8ba50-dae1-394f-beab-8f981faebae3"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e91afd28-6255-3126-9fa0-25c0e3d4f5f1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bacfa1ff-baa7-36ea-9c19-a847ff441de3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("904c6bac-5509-3797-aab1-c8c91c6ce792"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea56afea-b35e-3745-a6d2-4b20312b189b"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("384b3d6a-2898-3b93-985e-2c1921a824b4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5908011f-4b5b-375f-ad3b-5e85b48285a3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("94a53807-162a-38da-9b55-9055452357a3"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e79c4f35-5a48-3d25-9973-b67582b148f7"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ebd63a8-6cd9-31bd-b568-e9e95251d52b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a95979ba-2b05-3261-a7c2-d4e0c98047dd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8da6c06b-fc54-3a80-9f4c-222060038f29"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("99dc582d-e1bc-3a79-83e9-62ca18ec6133"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f233b324-b415-34e8-8734-f9936f27eb70"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0dacfa77-e213-3d13-b1e9-e54649076f13"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("58017582-69fa-332e-a2e9-dd5f761078b9"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("785c4d43-2295-3d7e-85fc-2bb8e7b735d1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc40a1b1-9579-3784-84a4-986a68de409a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4647dd44-9bd9-3162-a260-300be64a4b1f"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("112519cf-d32b-3f88-b7f3-f50246f9cd4c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7fb98d32-ad7d-3bec-9df5-74002ab1e177"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fe72e27-3b7e-3e63-9021-299327fcb598"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("81566049-24a8-38d9-a6d2-6042a6c29f18"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b3907ee-f69d-3d89-be8d-c25c3f8260b5"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8735ab44-ae6b-3629-82a6-8159944d1c09"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("54e5497c-06d8-3dc0-8c77-8e9d06d20e27"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0c486075-69a6-3fdb-8933-d1fd5c47fc5f"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6c27b43-0dbd-36b5-8cd4-bf495192dfb8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4d34566a-9f0d-3262-b345-bc9dce2f4161"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c5ff51a0-9f6b-3597-b6b8-7cc0ba2bce24"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("944e7db7-eb69-37c9-8459-3b8facc4872f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5156219-129b-3a70-9ccf-4f871ef6f71b"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78ed6351-87e7-3fec-949c-3c98a9150563"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f41d856-d888-368e-ab74-f4d3961239aa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f1fdb394-f6e2-3cd9-99e3-98d855d9eb77"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca1e3a37-cc10-3681-8db0-e82c4c507d36"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("90128c8e-ef09-37a2-8268-fa52d41a4abc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7e47fcea-bee1-3627-a77d-5855a2ba82de"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1176c49a-5501-39d6-bb62-9472b64307eb"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a4d5b6a-fc73-392d-ba2d-6bf049fb3d4e"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6514330a-96f6-376b-ae3c-86048f23d278"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("498fc82b-6553-37e0-8523-3ffbbbe9a71d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8b2d212a-482d-38c7-a7f4-9d2cd28367dc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c6cde762-99b4-3eca-8770-e9d372bc2f06"))) {
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
    long before = EnvironmentEdgeManager.currentTime();
if(KnobRuntime.check(java.util.UUID.fromString("ffe7e5fa-cf4d-37a0-a865-5c73aff6b18c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86d25d3c-5e80-35c6-abfa-5658fed98596"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b8eabda-2fe0-36e5-a0e7-6ac8487a3513"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a85528e-c30a-3852-aa6f-d1637c72208c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e29d1d1c-8748-3250-86ec-48ec1971068a"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11d0405f-845f-378a-b57d-65bef72f022e"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51ab25c4-ff37-354a-a853-06df5cecb299"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1be4c478-eae0-3f3e-9676-cadbc54f0c38"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b845746-a565-3bc0-9379-9e0ce53344d3"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d7bc17a-bbe6-3d0c-a000-20a624c8843d"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8363c5f3-1d20-3c86-8ead-7292d7ae3422"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca7b6f89-e0ac-3d3f-a8f9-4078812f837f"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfa8ede2-9234-3291-9d83-b560ba846213"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2cb12d3a-10c7-3d6b-9782-7bcb718ca487"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8492fac-dfed-3cb9-827a-d6799ab96590"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3cd18045-ab34-356b-af4f-eae224159405"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04778c13-04e4-3318-8f25-b320b2e930b7"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98e07fba-008f-399f-a50f-459a3cc71cbc"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77a37b28-c9a7-3aa8-9043-275102699be9"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b90bed5-ecb5-3498-869d-d50b1e469241"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b6f6aaa0-6048-393b-8293-6f09f8d54a7a"))) {
throw new java.io.IOException("Injected exception");
}
    Increment increment = ProtobufUtil.toIncrement(mutation, cells);
if(KnobRuntime.check(java.util.UUID.fromString("bb9d6d83-bc56-391a-b06e-9b5ced90b95f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("64d4a8c9-63d7-3a8b-958e-c60af8ac5dbe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("afd81b76-f067-3748-b92d-1f9e6eb980b4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f104f33f-2477-338c-9746-b4b1cc1a128b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5f83a6f1-ec5b-39a1-a338-2544942d324d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("484c385e-0775-3c92-9748-c49415afb61e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("82c1578c-9c1b-3462-999f-840a8861ded3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6cc8b3c9-0e52-3ec5-a46b-6ea0a2518dfc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6e1fef70-b44d-3cc1-a7dc-ba3f07a6df64"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a8d40612-829d-36a9-a8f3-a3ab09af0119"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8470e785-ca9a-3dfd-bae4-5978391eee3b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("14cc694f-df31-3391-b272-f2c3e81e66d4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("955fc165-3898-38e8-93a0-0b9cd035bfaf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ce53ccb0-d814-3c1a-9fcc-96c68a53bf40"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3dbe7d9b-5b69-33df-aa8e-81279deeb795"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6737ec38-bbdc-3ae1-a10a-c5417af5d78c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa9ca3cf-37fe-39e6-9126-e75589421c76"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e70639f2-6595-3800-88f3-01ca268e7de9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("06a4dbd7-bef8-365a-8bd1-413b3f2e111c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("949a9066-fb83-346d-85b8-7f021672145f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("754ffc6f-cd1a-322c-9047-8c8d61f96d15"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("66b795b0-9af2-3f05-9891-b0dfb93bbed2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("144c1c9e-ea4c-34c4-badd-d74db25965ba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("86901eed-5ba1-3099-9825-2704a9f5beba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d91d4ded-f988-3599-874e-0eb8246121fc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b2197776-7551-3dbc-8ab4-b6a817e19152"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("281371ae-980a-3ce8-a233-b8dae5078c17"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fff3ba7e-0116-306c-b366-51bd5415ba4e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cebc7ec4-b74a-31a2-9308-07d9325cf58e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fca1bb5f-ef71-3742-97e6-02cca0d251bc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("946fcf3e-2f43-30c8-9bae-b1da7654fd8e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aea03838-b58f-3f8a-a436-0295c3ed6dfa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d503f3e5-2ea5-34f7-b6fd-797a05d89f3f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("08f00f14-e250-3439-b3bd-73fa7305c8a2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a4b797b9-19b1-3490-beea-4384a2679b25"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ffd8c32d-beb2-3235-9e5d-5a9aa7c67113"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7fada8e8-0cc8-3c09-bbf6-c2ff39236136"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bdcdecd6-c4ac-3ad9-9898-f04a0b043cdb"))) {
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
    checkCellSizeLimit(region, increment);
    spaceQuota.getPolicyEnforcement(region).check(increment);
    quota.addMutation(increment);
    long blockBytesScannedBefore = context != null ? context.getBlockBytesScanned() : 0;
    long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
    Result r = region.increment(increment, nonceGroup, nonce);
    final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
    if (metricsRegionServer != null) {
      long blockBytesScanned =
        context != null ? context.getBlockBytesScanned() - blockBytesScannedBefore : 0;
      metricsRegionServer.updateIncrement(region, EnvironmentEdgeManager.currentTime() - before,
        blockBytesScanned);
    }
    return r == null ? Result.EMPTY_RESULT : r;
  }

  /**
   * Run through the regionMutation <code>rm</code> and per Mutation, do the work, and then when
   * done, add an instance of a {@link ResultOrException} that corresponds to each Mutation.
   * @param cellsToReturn Could be null. May be allocated in this method. This is what this method
   *                      returns as a 'result'.
   * @param closeCallBack the callback to be used with multigets
   * @param context       the current RpcCallContext
   * @return Return the <code>cellScanner</code> passed
   */
  private List<CellScannable> doNonAtomicRegionMutation(final HRegion region,
    final OperationQuota quota, final RegionAction actions, final CellScanner cellScanner,
    final RegionActionResult.Builder builder, List<CellScannable> cellsToReturn, long nonceGroup,
    final RegionScannersCloseCallBack closeCallBack, RpcCallContext context,
    ActivePolicyEnforcement spaceQuotaEnforcement) {
if(KnobRuntime.check(java.util.UUID.fromString("f203fe82-eeb1-3156-872b-53a2fda75a7a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6eea2fa0-dd6d-3270-99d7-e8240aa1f8a1"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d534226-090b-32c0-9392-993aa98eeafd"))) {
try {
    java.lang.reflect.Field field = actions.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(actions));
    field.set(actions, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f84433e-f0b6-3753-8bbc-29f7b408067f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("57160e1d-3bfa-34fe-83f8-0b58f78e31f5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5f337a1-7453-31ea-b8fe-6b2a1412f575"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3ed6d0dc-9089-37ed-8699-2c8e45be697d"))) {
try {
    java.lang.reflect.Field field = actions.getClass().getDeclaredField("atomic_");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(actions);
    field.set(actions, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef6158a3-072b-3768-acba-3749aafd476e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba5bdb65-703e-3c6f-a862-3d65ebaf2865"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f69a7457-31e0-39ba-90bd-68f20a1c90d2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("41a356c0-3676-3e88-9fd0-9950f47b1007"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79af8c40-8a7b-30a8-99d4-de1dcacaed80"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3ed4c5c-22be-3895-a05b-768ff3c60dac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("83b0b07d-c0de-3248-9a4d-d381b648d245"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5c452116-27fe-38bd-a81b-0d3600535b7e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2cd8a82f-021f-3a48-8f70-b22b51e3de96"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e409b905-f269-3176-9408-318a5cfea9c1"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("200d4980-4802-3c6b-a251-d5adfb5267dc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("30077362-be85-3817-921a-00f92cdff4e8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("46317883-4d8b-3733-8f88-cdb537439ae5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39b22561-dae9-367a-bf2e-0b0b89dee66b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eb7300df-b366-3797-9f82-8710cfed745a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("90da879f-3837-3d55-9ff4-ad980082832c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("87472cb9-1c0e-36a9-bf57-9433d3a911d9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("85171279-ab5f-36e8-87f4-a0b12699da43"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8bca5567-ae2d-3bf2-b051-cb145df2a74d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e3309299-eb22-39cf-84f6-de2f9281cbb7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a6f2389e-a29a-31bd-9c39-eb63cfa7f6b0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8d58599b-ad2a-384d-acb2-aa17047e89cf"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("369a2a47-12df-3c48-a88f-8354fa04f177"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ec73cde0-fb90-324d-ac40-5d7f30782999"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a55d0093-77ca-3960-b5de-5a4dd7ab2a18"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c19cacff-bb85-337f-aead-3b0f62007777"))) {
try {
    java.lang.reflect.Field field = actions.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(actions));
    field.set(actions, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a1fcbb84-e841-31a7-a31e-50e80cbb0501"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b1638fa-9b1f-344a-b4a2-2a6b4e65ab2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c9dd06bc-6723-38e5-af5e-65f7cc6d607c"))) {
try {
    java.lang.reflect.Field field = actions.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(actions));
    field.set(actions, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c22293c-158b-3da0-8d3d-d9eaf5fb543c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("446230f3-c707-3612-bf99-0d4f65ab607a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("21d8b602-b6d8-3a63-aa04-669232ddfbf4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("df4dc135-8af4-397f-b987-8cbb32c28dde"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6e71ac12-9308-326f-99c5-6e1b610dba7d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b171454e-27ec-3f03-bed2-f3c97f6db8dc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7dadc58f-3abc-3bab-b96f-f926b0c60d85"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6347fbf1-41b0-3fae-bcd6-32fd33d0fde7"))) {
try {
    java.lang.reflect.Field field = actions.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(actions));
    field.set(actions, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb0c217c-21c0-37b9-ad53-da1faa724d4b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("66a475a6-66ef-368f-9196-fcda8bd86fb7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("acf692d0-bc31-337c-9976-50c57aafa259"))) {
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
    // Gather up CONTIGUOUS Puts and Deletes in this mutations List. Idea is that rather than do
    // one at a time, we instead pass them in batch. Be aware that the corresponding
    // ResultOrException instance that matches each Put or Delete is then added down in the
    // doNonAtomicBatchOp call. We should be staying aligned though the Put and Delete are
    // deferred/batched
    List<ClientProtos.Action> mutations = null;
    long maxQuotaResultSize = Math.min(maxScannerResultSize, quota.getMaxResultSize());
    IOException sizeIOE = null;
    ClientProtos.ResultOrException.Builder resultOrExceptionBuilder =
      ResultOrException.newBuilder();
    boolean hasResultOrException = false;
    for (ClientProtos.Action action : actions.getActionList()) {
      hasResultOrException = false;
      resultOrExceptionBuilder.clear();
      try {
        Result r = null;
        long blockBytesScannedBefore = context != null ? context.getBlockBytesScanned() : 0;
        if (
          context != null && context.isRetryImmediatelySupported()
            && (context.getResponseCellSize() > maxQuotaResultSize
              || blockBytesScannedBefore + context.getResponseExceptionSize() > maxQuotaResultSize)
        ) {

          // We're storing the exception since the exception and reason string won't
          // change after the response size limit is reached.
          if (sizeIOE == null) {
            // We don't need the stack un-winding do don't throw the exception.
            // Throwing will kill the JVM's JIT.
            //
            // Instead just create the exception and then store it.
            sizeIOE = new MultiActionResultTooLarge("Max size exceeded" + " CellSize: "
              + context.getResponseCellSize() + " BlockSize: " + blockBytesScannedBefore);

            // Only report the exception once since there's only one request that
            // caused the exception. Otherwise this number will dominate the exceptions count.
            rpcServer.getMetrics().exception(sizeIOE);
          }

          // Now that there's an exception is known to be created
          // use it for the response.
          //
          // This will create a copy in the builder.
          NameBytesPair pair = ResponseConverter.buildException(sizeIOE);
          resultOrExceptionBuilder.setException(pair);
          context.incrementResponseExceptionSize(pair.getSerializedSize());
          resultOrExceptionBuilder.setIndex(action.getIndex());
          builder.addResultOrException(resultOrExceptionBuilder.build());
          skipCellsForMutation(action, cellScanner);
          continue;
        }
        if (action.hasGet()) {
          long before = EnvironmentEdgeManager.currentTime();
          ClientProtos.Get pbGet = action.getGet();
          // An asynchbase client, https://github.com/OpenTSDB/asynchbase, starts by trying to do
          // a get closest before. Throwing the UnknownProtocolException signals it that it needs
          // to switch and do hbase2 protocol (HBase servers do not tell clients what versions
          // they are; its a problem for non-native clients like asynchbase. HBASE-20225.
          if (pbGet.hasClosestRowBefore() && pbGet.getClosestRowBefore()) {
            throw new UnknownProtocolException("Is this a pre-hbase-1.0.0 or asynchbase client? "
              + "Client is invoking getClosestRowBefore removed in hbase-2.0.0 replaced by "
              + "reverse Scan.");
          }
          try {
if(KnobRuntime.check(java.util.UUID.fromString("d0908413-f617-31d2-b412-8d2d60064388"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("728a8a16-af04-3ca9-bab8-60aadae82e50"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5827bd4f-8092-3e93-8295-ce88512e8a84"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7686989-8cde-369d-96a5-a2238e4aaee8"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7483d2ec-5140-3d11-b8ca-38e47448ec26"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("89fbbb7b-af8a-3670-bc54-7320928e888e"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ecc14586-12aa-330e-94a4-e8e19c2c7eee"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fefbc816-7e48-3276-92f0-075761befd3c"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("baa83b1c-ab01-3191-b3ed-d4c72b2a6551"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d823fd2a-4097-3304-a9e8-fd03d8091781"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("257bc642-5ac6-35df-8a16-9c854abd831e"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70e11083-d97f-35ff-8c75-ec66fea74596"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07f20fe3-aa67-3d86-8f86-20ffad77d016"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e734e065-5024-31c0-a504-c89ca99103e3"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71cfecdb-79dd-3701-bb13-e246ebe7befe"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65663788-7255-339f-b416-dc8e167debb3"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("cacheBlocks_");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(pbGet);
    field.set(pbGet, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72a3a9ff-9206-3a1a-a405-696a06177d05"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e459613-1671-3249-a536-1e1be7e3e695"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e2e8cb2-2ee6-3aff-b299-fd55d1a7ce84"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c7939bd-5abb-3522-994c-9dec42f007a7"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3257298-f97b-3e2f-874c-c614cd34d85f"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("026454e0-42fa-3cb8-b7d8-1b6baf3e9b8a"))) {
try {
    java.lang.reflect.Field field = pbGet.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbGet));
    field.set(pbGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            Get get = ProtobufUtil.toGet(pbGet);
            if (((KnobRuntime.check(java.util.UUID.fromString("fdc37bc5-42e2-3fc6-b536-262b22cd8728"))) ? ((context) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("23bb6ec5-ba65-366b-9906-4d28490c7bfe"))) ? ((context) == (null)) : (context != null))))) {
              r = get(get, (region), closeCallBack, context);
            } else {
              r = region.get(get);
            }
          } finally {
            final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
            if (((KnobRuntime.check(java.util.UUID.fromString("8b77132a-ae00-3f05-9877-62d8e0f5806a"))) ? ((metricsRegionServer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b78f94fa-c4c6-3672-aba1-81d7da0465ac"))) ? ((metricsRegionServer) != (null)) : (metricsRegionServer != null))))) {
              long blockBytesScanned =
                context != null ? context.getBlockBytesScanned() - blockBytesScannedBefore : 0;
              metricsRegionServer.updateGet(region, EnvironmentEdgeManager.currentTime() - before,
                blockBytesScanned);
            }
          }
        } else if (action.hasServiceCall()) {
          hasResultOrException = true;
          com.google.protobuf.Message result = execServiceOnRegion(region, action.getServiceCall());
          ClientProtos.CoprocessorServiceResult.Builder serviceResultBuilder =
            ClientProtos.CoprocessorServiceResult.newBuilder();
          resultOrExceptionBuilder.setServiceResult(serviceResultBuilder
            .setValue(serviceResultBuilder.getValueBuilder().setName(result.getClass().getName())
              // TODO: Copy!!!
              .setValue(UnsafeByteOperations.unsafeWrap(result.toByteArray()))));
        } else if (action.hasMutation()) {
          MutationType type = action.getMutation().getMutateType();
          if (
            type != MutationType.PUT && type != MutationType.DELETE && mutations != null
              && !mutations.isEmpty()
          ) {
            // Flush out any Puts or Deletes already collected.
            doNonAtomicBatchOp(builder, region, quota, mutations, cellScanner,
              spaceQuotaEnforcement);
            mutations.clear();
          }
          switch (type) {
            case APPEND:
              r = append(region, quota, action.getMutation(), cellScanner, nonceGroup,
                spaceQuotaEnforcement, context);
              break;
            case INCREMENT:
              r = increment(region, quota, action.getMutation(), cellScanner, nonceGroup,
                spaceQuotaEnforcement, context);
              break;
            case PUT:
            case DELETE:
              // Collect the individual mutations and apply in a batch
              if (mutations == null) {
                mutations = new ArrayList<>(actions.getActionCount());
              }
if(KnobRuntime.check(java.util.UUID.fromString("54400764-1cb9-3794-9b2e-9acf98b27e5b"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("index_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a534891-9106-3219-b3b4-c4933b1a9bc0"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75199562-2720-340a-94aa-2553e4a919d3"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("index_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77f8e31a-1d04-3aff-aa2d-a9ff0c91997e"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("index_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f7e1dfc-9d63-373c-8ec4-7de1fdefc591"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("index_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("456f189e-42ef-3eb9-8c9b-534ea913c3ba"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e7c657c-983c-3ecd-96dd-72135f596e81"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d559bc6-4f73-30d0-911c-05261408a602"))) {
try {
    java.lang.reflect.Field field = action.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(action));
    field.set(action, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              mutations.add(action);
              break;
            default:
              throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
          }
        } else {
          throw new HBaseIOException("Unexpected Action type");
        }
        if (r != null) {
          ClientProtos.Result pbResult = null;
          if (isClientCellBlockSupport(context)) {
if(KnobRuntime.check(java.util.UUID.fromString("80d7a5a5-0e2c-307d-9472-0fb18ac65ce8"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a433bc2a-5242-3023-8197-79e95004b479"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c103e83c-af33-34cb-9128-fd5d086c340f"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3dbea7a-ee38-3ab5-a4f5-e5efaa5af1f2"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            pbResult = ProtobufUtil.toResultNoData(r);
            // Hard to guess the size here. Just make a rough guess.
            if (cellsToReturn == null) {
              cellsToReturn = new ArrayList<>();
            }
            cellsToReturn.add(r);
          } else {
            pbResult = ProtobufUtil.toResult(r);
          }
          addSize(context, r);
          hasResultOrException = true;
          resultOrExceptionBuilder.setResult(pbResult);
        }
        // Could get to here and there was no result and no exception. Presumes we added
        // a Put or Delete to the collecting Mutations List for adding later. In this
        // case the corresponding ResultOrException instance for the Put or Delete will be added
        // down in the doNonAtomicBatchOp method call rather than up here.
      } catch (IOException ie) {
        rpcServer.getMetrics().exception(ie);
        hasResultOrException = true;
        NameBytesPair pair = ResponseConverter.buildException(ie);
        resultOrExceptionBuilder.setException(pair);
        context.incrementResponseExceptionSize(pair.getSerializedSize());
      }
      if (hasResultOrException) {
        // Propagate index.
        resultOrExceptionBuilder.setIndex(action.getIndex());
        builder.addResultOrException(resultOrExceptionBuilder.build());
      }
    }
    // Finish up any outstanding mutations
    if (!CollectionUtils.isEmpty(mutations)) {
      doNonAtomicBatchOp(builder, region, quota, mutations, cellScanner, spaceQuotaEnforcement);
    }
    return cellsToReturn;
  }

  private void checkCellSizeLimit(final HRegion r, final Mutation m) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ba861298-f070-3712-b5f0-0818d2f095f9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("682a0b8a-20d7-316e-aed4-01d9c5287548"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("64d52364-5a8a-3e21-82e1-c805fff84f42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c22d6d73-5f57-356e-89ec-695d1fea4b42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("76673da3-83b6-3aa2-a59f-1a871804ae9b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4a1ed7b0-7e95-359a-a36e-bb6afdbfe2fa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("23649a0c-f6d4-3600-a3ea-8d8b8cfebd30"))) {
try {
    java.lang.reflect.Field field = m.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(m));
    field.set(m, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("927b8da3-cc97-3ecd-8ee5-9c60154ab16c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fff79665-2d28-3219-8d41-849ce4422a9c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("13962c59-58b8-3361-97bc-a1581987304f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("43de91dc-49c0-3508-9ae8-02b655278f8d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("43a51df3-e786-39a1-9f1f-3de8c44da673"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1b076a7c-3c5d-398a-8ab3-5d8fb87740c8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7adeb7df-b9b5-3c31-bb08-cc1e58120bb6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3d43cf74-79c7-3249-a993-e65972542fa9"))) {
try {
    java.lang.reflect.Field field = m.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(m));
    field.set(m, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04e17c14-b537-3c15-8410-fd861c93a07b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c86e041c-9098-384f-aafb-5606afb42044"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("101b60e2-793c-361a-b1ab-6ab2246b7611"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f61fc06b-6619-369a-aa45-a45f771e9f99"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b57a89d5-6bc1-3888-aaaf-e9348afce668"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c95d1ba9-aba0-3af0-9d51-11bca30d04f3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f9078f4a-0250-36da-83dc-3339b23e9f31"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5791785c-38a8-3f89-9e16-990e8a5ad75b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a0c6ed09-8167-3625-888b-15ccba870f16"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bb65270e-2b0e-3540-bbc0-b19e5b41ca2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("12b4677e-6823-3f71-83fc-25eb3a691ecc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cbab6c36-ff4e-30a4-878b-dd5aa7933d00"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bce1ad1a-a7bc-339f-b709-facef0da58d0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0534ec2e-34fd-3625-bec3-d48151a2d3e0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("54d67271-b69a-3fbc-87ba-419b0c599c57"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c903d92-e184-3b9f-94cb-7e1385c1b141"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7fb60e91-4dfa-3d5d-b16b-6ebb459f4612"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c308e6ef-789f-3acd-ad8d-f580a4b1cfee"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e0b20e79-4436-35ad-adba-add033727ef2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d001498f-cc0d-32ff-8ff4-16dd8440b61d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc1b8e05-629a-37fd-b4ea-c80d9671c55f"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("71aa5a35-21c8-31dc-ad9a-f41fd39bdb03"))) ? ((r.maxCellSize) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c2c85dbe-a9cf-34af-abb6-a28d4d405023"))) ? ((r.maxCellSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b633c9ff-f8e9-31bd-bdfe-c95f2c92db79"))) ? ((r.maxCellSize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("38720eb3-16dc-3541-939c-db49ac7bb43e"))) ? ((r.maxCellSize) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("76bdff03-74f5-33b7-abeb-f208a5cdd3b8"))) ? ((r.maxCellSize) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1ef7b759-5438-30fa-9440-a7148276bc34"))) ? ((r.maxCellSize) != (0)) : (r.maxCellSize > 0))))))))))))) {
      CellScanner cells = m.cellScanner();
      while (cells.advance()) {
        int size = PrivateCellUtil.estimatedSerializedSizeOf(cells.current());
        if (size > r.maxCellSize) {
          String msg = "Cell[" + cells.current() + "] with size " + size + " exceeds limit of "
            + r.maxCellSize + " bytes";
          LOG.debug(msg);
          throw new DoNotRetryIOException(msg);
        }
      }
    }
  }

  private void doAtomicBatchOp(final RegionActionResult.Builder builder, final HRegion region,
    final OperationQuota quota, final List<ClientProtos.Action> mutations, final CellScanner cells,
    long nonceGroup, ActivePolicyEnforcement spaceQuotaEnforcement) throws IOException {
    // Just throw the exception. The exception will be caught and then added to region-level
    // exception for RegionAction. Leaving the null to action result is ok since the null
    // result is viewed as failure by hbase client. And the region-lever exception will be used
    // to replaced the null result. see AsyncRequestFutureImpl#receiveMultiAction and
    // AsyncBatchRpcRetryingCaller#onComplete for more details.
if(KnobRuntime.check(java.util.UUID.fromString("3f4ae32a-9e94-34cb-be15-9c8bb07a9506"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("77b9b7b9-96a0-3067-9572-da51277c008e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("72e89058-b27e-3196-b963-31a9ccef5990"))) {
nonceGroup = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1b797088-9852-337e-a030-188bbdaf8b58"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2ab6aaf5-b8a5-3f54-855e-af864ed3e7c2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b187e53a-7e7a-3eea-8be6-a0c5b1fbeeb0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("22474119-43aa-3a54-a776-34824d388c6d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cceb6132-33a3-39cb-93ad-a947bee92243"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06fa42c5-159e-3176-b58e-2580eaafb347"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d03b53d3-54ca-33cf-9073-54353dccca19"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("42886f5b-f416-30e6-8a8c-e40c29e4a179"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("673915ee-9f17-3e56-b2fb-bc0f50ef400d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c497bee9-b62e-3192-8dd3-cb5fcbd95171"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79fe8832-dd21-3553-a6b4-67abc541e7a7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef8e0c40-227e-3d70-9de8-270c7fc99c47"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("33d8fb50-f9d5-39ac-aedc-dbb3a6101303"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f1c49507-109e-3102-ab20-798011845f06"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1c96bdac-19a0-3e66-ac29-b20cbe951a15"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("db5ebdb0-6236-3a4c-b71e-6dba5ba0da04"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efba70d9-8555-3213-8c05-ef8d9465ceff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("797a6ee7-a037-35aa-ba28-c192b5a98e04"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68fbe1f5-3b1c-35f0-a2ce-c4795a244772"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01c0a03f-f311-33e1-ba85-03ac63214992"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("522def8a-9597-3764-846a-01e181f17990"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("40dd742f-4ede-3315-ae9f-43e6e44907fa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c6d09af3-241f-3571-b2eb-963c0ed4b977"))) {
nonceGroup += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf42e233-5d59-3ea3-b089-430c3ec3ee10"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("44498a98-abb1-343c-b77a-9656d6f60b3e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e6005586-cd37-37a7-b9b1-9520a965f3bb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3c51bc1e-b4b8-3818-9668-249ac7550187"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("80e6a8a3-dcb8-305c-b488-a831d5f09fc5"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("261677bf-6c68-3b47-a485-dde8f1866afd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01f42fd1-f517-366f-ab1a-81a045f8512a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d983a95f-a9c3-3896-9b3f-c50858f8598b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("90938a78-86c4-3ec5-b672-cddfa35d0aa4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6d8345b2-2bf1-3c3c-887c-2e62f2006073"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("23d46240-0111-38cf-8a28-ad234c37122d"))) {
nonceGroup = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a57aad1f-6675-30da-9fc1-fba9a312d3f3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ee42f0fc-d148-31a4-b889-890cbc19d496"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a0099bb7-e9f5-34bf-9cbf-b78ec8821df1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f99e77fb-247f-34af-b962-2d7f1317fead"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a7f6b4f0-7d53-33ef-a19f-b30bda5f7442"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9679c508-3093-337d-af7b-a86290f61346"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("722e4515-6968-3737-a439-0fa4adf3a245"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c7b325df-f096-38e6-97f6-76e11cc2f686"))) {
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
    doBatchOp(builder, region, quota, mutations, cells, nonceGroup, spaceQuotaEnforcement, true);
  }

  private void doNonAtomicBatchOp(final RegionActionResult.Builder builder, final HRegion region,
    final OperationQuota quota, final List<ClientProtos.Action> mutations, final CellScanner cells,
    ActivePolicyEnforcement spaceQuotaEnforcement) {
    try {
      doBatchOp(builder, region, quota, mutations, cells, HConstants.NO_NONCE,
        spaceQuotaEnforcement, false);
    } catch (IOException e) {
      // Set the exception for each action. The mutations in same RegionAction are group to
      // different batch and then be processed individually. Hence, we don't set the region-level
      // exception here for whole RegionAction.
      for (Action mutation : mutations) {
        builder.addResultOrException(getResultOrException(e, mutation.getIndex()));
      }
    }
  }

  /**
   * Execute a list of mutations.
   */
  private void doBatchOp(final RegionActionResult.Builder builder, final HRegion region,
    final OperationQuota quota, final List<ClientProtos.Action> mutations, final CellScanner cells,
    long nonceGroup, ActivePolicyEnforcement spaceQuotaEnforcement, boolean atomic)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("523ec2d5-e720-3576-8bbf-e5526f0d6953"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("11d4970b-bcad-318f-a3e1-ceb2272cf22d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("16682e63-77ba-3da6-9e3e-63fded274b4b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("357c3a2f-e1c8-3bf3-ab2b-2901b3bec18d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("186652ff-3895-38b7-975f-33c913413629"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("75b2c648-66ef-3b2f-8918-9e39a83f2df4"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e2a2b88e-9997-3f8b-8ef5-d28d19a728b8"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6751ce5d-9d2c-3387-9f4e-f1ae56197897"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4e8d9ebf-6e87-314b-a5b2-86350053c933"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("119abb66-f9b8-3984-9a4b-e7e184b831cc"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("525b3361-19c7-3bd5-917f-2a3df9350018"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("47feb3a1-1902-30d7-af8a-d41a24074c05"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2c7f07b2-0870-36ea-8f7c-b4bd2baebfc6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1e1206f1-cc6d-3111-8153-202640fb3dc7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9fb20ca9-2ea2-3581-ab58-827b0669f18d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a16e7d8a-09a6-369b-ab40-639310cd11b0"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec15278a-e0d1-345f-b46d-e9f031bfd27f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea76355e-7c67-3f86-8935-468f6df74528"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cea8db18-8988-32ca-897d-bc3ba10498b2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b76f8221-1650-3104-884a-08070105734a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39d495cb-2153-3c01-bc17-744ee16b25e8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b4c29ef-fbd6-30dc-938a-c36d685c2775"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d752d954-6b8f-360d-ab42-397a08078e71"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("81aeabd4-cfc1-3c39-9141-5a34acd62bf8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dcc6855c-55d6-39a4-99e6-2003f3698fae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8d4017f0-e2d3-39a7-815a-3e1445759f82"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3b32f65-100a-3720-b853-6d562e773fa4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c0630c6-1e1e-3276-a456-a740ecddc7c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("40fbae48-1513-3e18-b9e5-a671db99c817"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a27474af-b51b-3e9e-bbb8-30dfd1833376"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a2bb56fb-65ac-30e7-9e34-29b51bad1f80"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9165f9a4-118e-36c3-84e7-00da346dc230"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("600fb517-a4c1-3829-948d-65bf211031ba"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3bd69ed6-1d4e-3326-9360-c688407cc1a6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c576e019-6fbd-3b6b-84fe-08e940449d05"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7d83705a-d664-3977-95a6-a2bf0f59d3cf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7e228404-87a9-3eed-926b-5fc76c8396ba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("505c0405-4c6a-3d4a-a12c-ae7560d59554"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2ee7ac9c-71a2-3a33-a653-17ff37d6b83c"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("fae48aac-738b-3cb6-af8e-e3459d0d7f65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("972a7354-e081-3b97-bf48-c687da79dce8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("95e77909-8c8c-315e-b95b-53f48a29c523"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c1b5507a-fa8b-3609-a9b0-36723ffdde48"))) {
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
    Mutation[] mArray = new Mutation[mutations.size()];
    long before = EnvironmentEdgeManager.currentTime();
    boolean batchContainsPuts = false, batchContainsDelete = false;
    try {
      /**
       * HBASE-17924 mutationActionMap is a map to map the relation between mutations and actions
       * since mutation array may have been reoredered.In order to return the right result or
       * exception to the corresponding actions, We need to know which action is the mutation belong
       * to. We can't sort ClientProtos.Action array, since they are bonded to cellscanners.
       */
      Map<Mutation, ClientProtos.Action> mutationActionMap = new HashMap<>();
      int i = 0;
      long nonce = HConstants.NO_NONCE;
      for (ClientProtos.Action action : mutations) {
        if (action.hasGet()) {
          throw new DoNotRetryIOException(
            "Atomic put and/or delete only, not a Get=" + action.getGet());
        }
        MutationProto m = action.getMutation();
        Mutation mutation;
        switch (m.getMutateType()) {
          case PUT:
            mutation = ProtobufUtil.toPut(m, cells);
            batchContainsPuts = true;
            break;

          case DELETE:
            mutation = ProtobufUtil.toDelete(m, cells);
            batchContainsDelete = true;
            break;

          case INCREMENT:
            mutation = ProtobufUtil.toIncrement(m, cells);
            nonce = m.hasNonce() ? m.getNonce() : HConstants.NO_NONCE;
            break;

          case APPEND:
            mutation = ProtobufUtil.toAppend(m, cells);
            nonce = m.hasNonce() ? m.getNonce() : HConstants.NO_NONCE;
            break;

          default:
            throw new DoNotRetryIOException("Invalid mutation type : " + m.getMutateType());
        }
        mutationActionMap.put(mutation, action);
        mArray[i++] = mutation;
        checkCellSizeLimit(region, mutation);
        // Check if a space quota disallows this mutation
        spaceQuotaEnforcement.getPolicyEnforcement(region).check(mutation);
        quota.addMutation(mutation);
      }

      if (!region.getRegionInfo().isMetaRegion()) {
        regionServer.getMemStoreFlusher().reclaimMemStoreMemory();
      }

      // HBASE-17924
      // Sort to improve lock efficiency for non-atomic batch of operations. If atomic
      // order is preserved as its expected from the client
      if (!atomic) {
        Arrays.sort(mArray, (v1, v2) -> Row.COMPARATOR.compare(v1, v2));
      }

      OperationStatus[] codes = region.batchMutate(mArray, atomic, nonceGroup, nonce);

      // When atomic is true, it indicates that the mutateRow API or the batch API with
      // RowMutations is called. In this case, we need to merge the results of the
      // Increment/Append operations if the mutations include those operations, and set the merged
      // result to the first element of the ResultOrException list
      if (atomic) {
        List<ResultOrException> resultOrExceptions = new ArrayList<>();
        List<Result> results = new ArrayList<>();
        for (i = 0; i < codes.length; i++) {
          if (codes[i].getResult() != null) {
            results.add(codes[i].getResult());
          }
          if (((KnobRuntime.check(java.util.UUID.fromString("746a41b4-de26-3434-b73f-e891b71cf9f4"))) ? ((i) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d0062a81-816d-35d3-a9c6-3c80ea29d503"))) ? ((i) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1aea3de0-b868-361d-a558-c4afc06ed66d"))) ? ((i++) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c24c88ac-17fe-3b78-be15-360c355e3d57"))) ? ((i++) != (0)) : (i != 0))))))))) {
            resultOrExceptions
              .add(getResultOrException(ClientProtos.Result.getDefaultInstance(), i));
          }
        }

        if (results.isEmpty()) {
          builder.addResultOrException(
            getResultOrException(ClientProtos.Result.getDefaultInstance(), 0));
        } else {
          // Merge the results of the Increment/Append operations
          List<Cell> cellList = new ArrayList<>();
          for (Result result : results) {
            if (((KnobRuntime.check(java.util.UUID.fromString("e75fd57c-c423-3a43-a907-7a3f57dddd2f"))) ? ((result.rawCells()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("dacc57ab-f34d-37f1-a298-120ad819e4ca"))) ? ((result.rawCells()) == (null)) : (result.rawCells() != null))))) {
              cellList.addAll(Arrays.asList(result.rawCells()));
            }
          }
          Result result = Result.create(cellList);

          // Set the merged result of the Increment/Append operations to the first element of the
          // ResultOrException list
          builder.addResultOrException(getResultOrException(ProtobufUtil.toResult(result), 0));
        }

        builder.addAllResultOrException(resultOrExceptions);
        return;
      }

      for (i = 0; i < codes.length; i++) {
        Mutation currentMutation = mArray[i];
if(KnobRuntime.check(java.util.UUID.fromString("6b1e7c60-f3f0-399e-90e0-c71d08d8e756"))) {
try {
    java.lang.reflect.Field field = currentMutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(currentMutation));
    field.set(currentMutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        ClientProtos.Action currentAction = mutationActionMap.get(currentMutation);
        int index = currentAction.hasIndex() ? currentAction.getIndex() : i;
        Exception e;
        switch (codes[i].getOperationStatusCode()) {
          case BAD_FAMILY:
            e = new NoSuchColumnFamilyException(codes[i].getExceptionMsg());
if(KnobRuntime.check(java.util.UUID.fromString("f5411712-8645-3e2d-bdb5-a7984cfd58b7"))) {
index = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f0d9a18a-617f-369e-9178-37f32f783e83"))) {
index += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b246346d-5b98-38f8-b115-67b925cb5f52"))) {
index = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cee6179-e8fd-359b-9ff4-425ebeaa9d85"))) {
index /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f76a5b5-5a79-34ab-8731-a935182f839a"))) {
index *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a0df341b-eed5-3255-a6ce-9d3cfaa3a3b0"))) {
index -= 1;
}
            if (KnobRuntime.check(java.util.UUID.fromString("127750de-aa9b-3e91-8059-ed256153f56c"))) { builder.addResultOrException(getResultOrException(e, 0)); } else { builder.addResultOrException(getResultOrException(e, index)); }
            break;

          case SANITY_CHECK_FAILURE:
            e = new FailedSanityCheckException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          default:
            e = new DoNotRetryIOException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          case SUCCESS:
            ClientProtos.Result result = codes[i].getResult() == null
              ? ClientProtos.Result.getDefaultInstance()
              : ProtobufUtil.toResult(codes[i].getResult());
            builder.addResultOrException(getResultOrException(result, index));
            break;

          case STORE_TOO_BUSY:
            e = new RegionTooBusyException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;
        }
      }
    } finally {
      int processedMutationIndex = 0;
      for (Action mutation : mutations) {
        // The non-null mArray[i] means the cell scanner has been read.
        if (mArray[processedMutationIndex++] == null) {
          skipCellsForMutation(mutation, cells);
        }
      }
      updateMutationMetrics(region, before, batchContainsPuts, batchContainsDelete);
    }
  }

  private void updateMutationMetrics(HRegion region, long starttime, boolean batchContainsPuts,
    boolean batchContainsDelete) {
    final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      if (batchContainsPuts) {
        metricsRegionServer.updatePutBatch(region, after - starttime);
      }
      if (batchContainsDelete) {
        metricsRegionServer.updateDeleteBatch(region, after - starttime);
      }
    }
  }

  /**
   * Execute a list of Put/Delete mutations. The function returns OperationStatus instead of
   * constructing MultiResponse to save a possible loop if caller doesn't need MultiResponse.
   * @return an array of OperationStatus which internally contains the OperationStatusCode and the
   *         exceptionMessage if any
   */
  private OperationStatus[] doReplayBatchOp(final HRegion region,
    final List<MutationReplay> mutations, long replaySeqId) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    boolean batchContainsPuts = false, batchContainsDelete = false;
    try {
      for (Iterator<MutationReplay> it = mutations.iterator(); it.hasNext();) {
        MutationReplay m = it.next();

        if (m.getType() == MutationType.PUT) {
          batchContainsPuts = true;
        } else {
          batchContainsDelete = true;
        }

        NavigableMap<byte[], List<Cell>> map = m.mutation.getFamilyCellMap();
        List<Cell> metaCells = map.get(WALEdit.METAFAMILY);
        if (metaCells != null && !metaCells.isEmpty()) {
          for (Cell metaCell : metaCells) {
            CompactionDescriptor compactionDesc = WALEdit.getCompaction(metaCell);
            boolean isDefaultReplica = RegionReplicaUtil.isDefaultReplica(region.getRegionInfo());
            HRegion hRegion = region;
            if (compactionDesc != null) {
              // replay the compaction. Remove the files from stores only if we are the primary
              // region replica (thus own the files)
              hRegion.replayWALCompactionMarker(compactionDesc, !isDefaultReplica, isDefaultReplica,
                replaySeqId);
              continue;
            }
if(KnobRuntime.check(java.util.UUID.fromString("b23e5de4-7f98-3767-8e96-e32d3e339cef"))) {
throw new java.io.IOException("Injected exception");
}
            FlushDescriptor flushDesc = WALEdit.getFlushDescriptor(metaCell);
            if (((KnobRuntime.check(java.util.UUID.fromString("37821ce3-3a11-3733-900a-27b2a071b4df"))) ? ((flushDesc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ccba996b-035b-30b6-8d51-10a6ead31674"))) ? (!isDefaultReplica) : (((KnobRuntime.check(java.util.UUID.fromString("f86f7ecc-1683-37bd-8558-a07a06c7f337"))) ? (((flushDesc) != (null)) && (!isDefaultReplica)) : (((KnobRuntime.check(java.util.UUID.fromString("2b9d78fa-9fc3-348f-9ee5-907119ba0da6"))) ? ((flushDesc != null) || (!isDefaultReplica)) : (((KnobRuntime.check(java.util.UUID.fromString("74548971-1990-3aa1-b696-f0dcecd662e9"))) ? (((flushDesc) == (null)) && (!isDefaultReplica)) : (((KnobRuntime.check(java.util.UUID.fromString("2d773c3f-593e-35ef-a313-a788c3f3b5e1"))) ? (((flushDesc) == (null)) || (!isDefaultReplica)) : (((KnobRuntime.check(java.util.UUID.fromString("9db99446-4249-3e21-9a09-bb2632f4846c"))) ? ((flushDesc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("259f23b2-6f18-3a01-89db-33f4e2e6be0b"))) ? (((flushDesc) != (null)) || (!isDefaultReplica)) : (((KnobRuntime.check(java.util.UUID.fromString("9588bf90-038d-3652-a4e4-876bd08eef4d"))) ? (flushDesc != null) : (((KnobRuntime.check(java.util.UUID.fromString("c6ec1bc5-4d3b-3507-bcf9-e733eca7903a"))) ? ((flushDesc != null) && (!isDefaultReplica)) : (flushDesc != null && !isDefaultReplica))))))))))))))))))))) {
              hRegion.replayWALFlushMarker(flushDesc, replaySeqId);
              continue;
            }
if(KnobRuntime.check(java.util.UUID.fromString("01ac4f00-526c-3c10-8fa2-8879fe5ad239"))) {
throw new java.io.IOException("Injected exception");
}
            RegionEventDescriptor regionEvent = WALEdit.getRegionEventDescriptor(metaCell);
            if (regionEvent != null && !isDefaultReplica) {
if(KnobRuntime.check(java.util.UUID.fromString("679e4347-665b-335c-a18c-f85d5a2f1968"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f1efcaa5-9594-3ec2-b484-0b69925e2aff"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6530a52f-93a8-3666-bb1a-7755e16a27b4"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab468006-12a7-30f5-a72b-1dd8d5e99a15"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf7eee2c-fd6d-31e2-a8d9-ac36ee34701b"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7465378-5078-3dc4-bcae-ca16ae6747da"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce067c0b-4666-3dbf-98a7-d39bf449f887"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("logSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionEvent));
    field.set(regionEvent, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b7ea1f1-c6e6-382c-9ca9-54f86f052702"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa2d90f9-41a0-37b1-bf93-cb3c873019d9"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b97345fe-3915-346c-a822-a55cc84810d7"))) {
try {
    java.lang.reflect.Field field = regionEvent.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEvent));
    field.set(regionEvent, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              hRegion.replayWALRegionEventMarker(regionEvent);
              continue;
            }
if(KnobRuntime.check(java.util.UUID.fromString("909b7f73-d2aa-3683-a060-3ceec8a75144"))) {
throw new java.io.IOException("Injected exception");
}
            BulkLoadDescriptor bulkLoadEvent = WALEdit.getBulkLoadDescriptor(metaCell);
            if (bulkLoadEvent != null) {
              hRegion.replayWALBulkLoadEventMarker(bulkLoadEvent);
              continue;
            }
          }
          it.remove();
        }
      }
      if (KnobRuntime.check(java.util.UUID.fromString("6332f356-5c51-36eb-bdf6-5bd8cdf5c2be"))) { closeAllScanners(); } else if (KnobRuntime.check(java.util.UUID.fromString("8f5c966d-b21e-3fae-b6de-ebe4d6d0e0a2"))) { checkOpen(); } else { requestCount.increment(); }
      if (!region.getRegionInfo().isMetaRegion()) {
        regionServer.getMemStoreFlusher().reclaimMemStoreMemory();
      }
      return region.batchReplay(mutations.toArray(new MutationReplay[mutations.size()]),
        replaySeqId);
    } finally {
      updateMutationMetrics(region, before, batchContainsPuts, batchContainsDelete);
    }
  }

  private void closeAllScanners() {
    // Close any outstanding scanners. Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, RegionScannerHolder> e : scanners.entrySet()) {
      try {
        e.getValue().s.close();
      } catch (IOException ioe) {
        LOG.warn("Closing scanner " + e.getKey(), ioe);
      }
    }
  }

  // Directly invoked only for testing
  public RSRpcServices(final HRegionServer rs) throws IOException {
    final Configuration conf = rs.getConfiguration();
    regionServer = rs;
    final RpcSchedulerFactory rpcSchedulerFactory;
    try {
      rpcSchedulerFactory = getRpcSchedulerFactoryClass().asSubclass(RpcSchedulerFactory.class)
        .getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
      | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
    // Server to handle client requests.
    final InetSocketAddress initialIsa;
    final InetSocketAddress bindAddress;
    if (this instanceof MasterRpcServices) {
      String hostname = DNS.getHostname(conf, DNS.ServerType.MASTER);
      int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
      // Creation of a HSA will force a resolve.
      initialIsa = new InetSocketAddress(hostname, port);
      bindAddress = new InetSocketAddress(conf.get("hbase.master.ipc.address", hostname), port);
    } else {
      String hostname = DNS.getHostname(conf, DNS.ServerType.REGIONSERVER);
      int port = conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT);
      // Creation of a HSA will force a resolve.
      initialIsa = new InetSocketAddress(hostname, port);
      bindAddress =
        new InetSocketAddress(conf.get("hbase.regionserver.ipc.address", hostname), port);
    }
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    priority = createPriority();
    // Using Address means we don't get the IP too. Shorten it more even to just the host name
    // w/o the domain.
    final String name = rs.getProcessName() + "/"
      + Address.fromParts(initialIsa.getHostName(), initialIsa.getPort()).toStringWithoutDomain();
    // Set how many times to retry talking to another server over Connection.
    ConnectionUtils.setServerSideHConnectionRetriesConfig(conf, name, LOG);
    rpcServer = createRpcServer(rs, rpcSchedulerFactory, bindAddress, name);
    rpcServer.setRsRpcServices(this);
    if (!(rs instanceof HMaster)) {
      rpcServer.setNamedQueueRecorder(rs.getNamedQueueRecorder());
    }
    setReloadableGuardrails(conf);
    scannerLeaseTimeoutPeriod = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
    rpcTimeout =
      conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    minimumScanTimeLimitDelta = conf.getLong(REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA,
      DEFAULT_REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA);

    final InetSocketAddress address = rpcServer.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    // Set our address, however we need the final port that was given to rpcServer
    isa = new InetSocketAddress(initialIsa.getHostName(), address.getPort());
    rpcServer.setErrorHandler(this);
    rs.setName(name);

    closedScanners = CacheBuilder.newBuilder()
      .expireAfterAccess(scannerLeaseTimeoutPeriod, TimeUnit.MILLISECONDS).build();
  }

  protected RpcServerInterface createRpcServer(final Server server,
    final RpcSchedulerFactory rpcSchedulerFactory, final InetSocketAddress bindAddress,
    final String name) throws IOException {
    final Configuration conf = server.getConfiguration();
    boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    try {
      return RpcServerFactory.createRpcServer(server, name, getServices(), bindAddress, // use final
                                                                                        // bindAddress
                                                                                        // for this
                                                                                        // server.
        conf, rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(be.getMessage() + ". To switch ports use the '"
        + HConstants.REGIONSERVER_PORT + "' configuration property.",
        be.getCause() != null ? be.getCause() : be);
    }
  }

  protected Class<?> getRpcSchedulerFactoryClass() {
    final Configuration conf = regionServer.getConfiguration();
    return conf.getClass(REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SimpleRpcSchedulerFactory.class);
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    if (rpcServer instanceof ConfigurationObserver) {
      if (KnobRuntime.check(java.util.UUID.fromString("2c712215-c75f-38f6-a294-00d26f2f1665"))) { setReloadableGuardrails(newConf); } else { ((ConfigurationObserver) rpcServer).onConfigurationChange(newConf); }
      setReloadableGuardrails(newConf);
    }
  }

  protected PriorityFunction createPriority() {
    return new AnnotationReadingPriorityFunction(this);
  }

  protected void requirePermission(String request, Permission.Action perm) throws IOException {
    if (accessChecker != null) {
if(KnobRuntime.check(java.util.UUID.fromString("5113ff3e-9718-37eb-a329-76006057c96a"))) {
throw new java.io.IOException("Injected exception");
}
      accessChecker.requirePermission(RpcServer.getRequestUser().orElse(null), request, null, perm);
    }
  }

  public int getScannersCount() {
if(KnobRuntime.check(java.util.UUID.fromString("a1bb4f6a-f31a-3827-81e9-ba66f63a01d2"))) {
return 0;
}
    return scanners.size();
  }

  /** Returns The outstanding RegionScanner for <code>scannerId</code> or null if none found. */
  RegionScanner getScanner(long scannerId) {
    RegionScannerHolder rsh = checkQuotaAndGetRegionScannerContext(scannerId);
    return rsh == null ? null : rsh.s;
  }

  /** Returns The associated RegionScannerHolder for <code>scannerId</code> or null. */
  private RegionScannerHolder checkQuotaAndGetRegionScannerContext(long scannerId) {
    return scanners.get(toScannerName(scannerId));
  }

  public String getScanDetailsWithId(long scannerId) {
    RegionScanner scanner = getScanner(scannerId);
    if (scanner == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    builder.append("table: ").append(scanner.getRegionInfo().getTable().getNameAsString());
    builder.append(" region: ").append(scanner.getRegionInfo().getRegionNameAsString());
    if (KnobRuntime.check(java.util.UUID.fromString("835b9a03-d03f-3f0a-97ae-02313ed555f2"))) { builder.append(" operation_id: ").append(getRemoteClientIpAndPort()); } else if (KnobRuntime.check(java.util.UUID.fromString("7c5a9199-5727-3f16-aece-77f6f4dd12a3"))) { builder.append(" operation_id: ").append(getUserName()); } else { builder.append(" operation_id: ").append(scanner.getOperationId()); }
    return builder.toString();
  }

  public String getScanDetailsWithRequest(ScanRequest request) {
if(KnobRuntime.check(java.util.UUID.fromString("d4ba9adf-9be5-3c2d-9c5b-d04cbc1fa1ce"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("42a4ec4f-5c89-376f-80f1-0eb448787ffb"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd63d356-0af8-3a25-bfc2-7b10dee86cd5"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("81b2503a-b06e-32cf-8637-3c166584ef8e"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f5b8861-437a-3f78-ad42-013f79e22f92"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("009e7227-1560-3e37-8930-b55da75552c5"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("811286e3-17ee-3fbe-b3ed-f7b3346b4188"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9655e9ec-5eab-3deb-8fa8-3e7a0a16e9fc"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("nextCallSeq_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af7a6317-3128-34df-a4bb-9d03d5912a58"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39a04937-ba12-34af-b276-b5e83f49353d"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("nextCallSeq_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63b03de8-f29c-3a6d-970f-a0d575108f55"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("958bfdad-5cb5-3a4e-aa59-49e40779f1e6"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d091b670-202c-3665-baa6-b32fe6858c06"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("44b5a7ec-dc45-38f4-9ad5-cf8d8ffd54c1"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfbb337f-01e1-33d2-ada0-95115b5440d3"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e42f0f5-9554-31b0-854d-053624679702"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec1f4ad2-29d5-3208-beb1-f8f86c9b4bac"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    try {
      if (!request.hasRegion()) {
        return null;
      }
      Region region = getRegion(request.getRegion());
      StringBuilder builder = new StringBuilder();
      builder.append("table: ").append(region.getRegionInfo().getTable().getNameAsString());
      builder.append(" region: ").append(region.getRegionInfo().getRegionNameAsString());
      for (NameBytesPair pair : request.getScan().getAttributeList()) {
        if (OperationWithAttributes.ID_ATRIBUTE.equals(pair.getName())) {
          builder.append(" operation_id: ").append(Bytes.toString(pair.getValue().toByteArray()));
          break;
        }
      }
      return ((KnobRuntime.check(java.util.UUID.fromString("c78c5ed3-4332-3b15-8881-aa115abdcd01"))) ? (getRemoteClientIpAndPort()) : (builder.toString()));
    } catch (IOException ignored) {
      return null;
    }
  }

  /**
   * Get the vtime associated with the scanner. Currently the vtime is the number of "next" calls.
   */
  long getScannerVirtualTime(long scannerId) {
    RegionScannerHolder rsh = checkQuotaAndGetRegionScannerContext(scannerId);
    return rsh == null ? 0L : rsh.getNextCallSeq();
  }

  /**
   * Method to account for the size of retained cells.
   * @param context rpc call context
   * @param r       result to add size.
   * @return an object that represents the last referenced block from this response.
   */
  void addSize(RpcCallContext context, Result r) {
    if (context != null && r != null && !r.isEmpty()) {
      for (Cell c : r.rawCells()) {
        context.incrementResponseCellSize(PrivateCellUtil.estimatedSerializedSizeOf(c));
      }
    }
  }

  /** Returns Remote client's ip and port else null if can't be determined. */
  @RestrictedApi(explanation = "Should only be called in TestRSRpcServices and RSRpcServices",
      link = "", allowedOnPath = ".*(TestRSRpcServices|RSRpcServices).java")
  static String getRemoteClientIpAndPort() {
    RpcCall rpcCall = RpcServer.getCurrentCall().orElse(null);
    if (rpcCall == null) {
      return HConstants.EMPTY_STRING;
    }
    InetAddress address = rpcCall.getRemoteAddress();
    if (address == null) {
      return HConstants.EMPTY_STRING;
    }
    // Be careful here with InetAddress. Do InetAddress#getHostAddress. It will not do a name
    // resolution. Just use the IP. It is generally a smaller amount of info to keep around while
    // scanning than a hostname anyways.
    return Address.fromParts(address.getHostAddress(), rpcCall.getRemotePort()).toString();
  }

  /** Returns Remote client's username. */
  @RestrictedApi(explanation = "Should only be called in TestRSRpcServices and RSRpcServices",
      link = "", allowedOnPath = ".*(TestRSRpcServices|RSRpcServices).java")
  static String getUserName() {
    RpcCall rpcCall = RpcServer.getCurrentCall().orElse(null);
    if (rpcCall == null) {
      return HConstants.EMPTY_STRING;
    }
    return rpcCall.getRequestUserName().orElse(HConstants.EMPTY_STRING);
  }

  private RegionScannerHolder addScanner(String scannerName, RegionScanner s, Shipper shipper,
    HRegion r, boolean needCursor, boolean fullRegionScan) throws LeaseStillHeldException {
if(KnobRuntime.check(java.util.UUID.fromString("3d451803-ceac-3df7-b692-dd71170cd0ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("719c75d4-18a4-32e2-91c4-a0431186d935"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8919d4da-95e1-3754-b829-808b3ca0a55c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3951016e-d756-312d-a359-23e8cbc4c794"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a7dd7f78-07c9-35f2-904d-2d4cda692824"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ee1d9854-91eb-3e11-8d74-b15e3662b1d8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("34737ad7-c699-3e1c-b552-08914af25cb6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02713bdd-f259-3b26-8789-90a98f128fad"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a26afa01-8270-357c-9c01-7d4c4b92aade"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("600b780c-3c94-3805-94d6-5413dd95922d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f06ffabd-b049-3f2e-b59d-36c4e88e967c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3888e302-ecae-31bb-bd4a-e7c69eb5c2c0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("be15f524-0b9e-3c64-acaa-df99a1a12a10"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0cb24cff-dedc-38c5-bbd9-553a195836e6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("863d5b96-5519-3f86-ace0-9c8d61077c97"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2cff9930-b855-3803-a40b-77afd4282d81"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3dfd04f3-4d8d-360e-a377-952d4e4448d1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("91e8aa13-ecdc-323a-a3d9-4f5418090572"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6de258d2-ab06-34bc-8773-8c5e1817b5e6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fec4907d-b29f-371d-a7c1-8d02acbea31f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc132362-e89f-3b31-a825-47771f603e23"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2645c1b3-2d26-37d5-91be-6853823b0957"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ab4ecc3d-2faa-39e3-8e3a-5c7200436dae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("19cd9439-1663-306f-9c1f-802dafe8a321"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("818a972c-2c1d-3c97-951c-2b537b1e4ad2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ffd2d8ba-134b-3f41-9626-fde9282481c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0cf5ccb6-f052-35e4-8898-7c18233ef317"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b18866fb-c10b-3b5a-bcb9-7c38b4da6a26"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3db01f11-0348-3c98-b955-384dacbd7afd"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("1c6e4118-1fc5-3453-a4af-6fb8f790984b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f28c1f4-a1da-3cb8-b123-c132a45a17fc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f25a1b2c-9174-3996-935f-8b43d2451a95"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("04982a69-952f-3bbc-806a-e91d3a5f4f20"))) {
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
    Lease lease = regionServer.getLeaseManager().createLease(scannerName,
      this.scannerLeaseTimeoutPeriod, new ScannerListener(scannerName));
    RpcCallback shippedCallback = new RegionScannerShippedCallBack(scannerName, shipper, lease);
    RpcCallback closeCallback =
      s instanceof RpcCallback ? (RpcCallback) s : new RegionScannerCloseCallBack(s);
    RegionScannerHolder rsh = new RegionScannerHolder(s, r, closeCallback, shippedCallback,
      needCursor, fullRegionScan, getRemoteClientIpAndPort(), getUserName());
if(KnobRuntime.check(java.util.UUID.fromString("39d62aa3-3469-390e-b6f7-1d4c42606253"))) {
try {
    java.lang.reflect.Field field = rsh.getClass().getDeclaredField("needCursor");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(rsh);
    field.set(rsh, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4fd3776-975e-3c69-bf1c-6f570c961284"))) {
try {
    java.lang.reflect.Field field = rsh.getClass().getDeclaredField("prevBlockBytesScanned");
    field.setAccessible(true);
    long oldValue = ((long)field.get(rsh));
    field.set(rsh, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e31f00cc-c90d-3ceb-9c54-3ce0e801703e"))) {
try {
    java.lang.reflect.Field field = rsh.getClass().getDeclaredField("prevBlockBytesScannedDifference");
    field.setAccessible(true);
    long oldValue = ((long)field.get(rsh));
    field.set(rsh, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1d407528-7e8d-3b64-9964-9e4af6ebf10f"))) {
try {
    java.lang.reflect.Field field = rsh.getClass().getDeclaredField("maxBlockBytesScanned");
    field.setAccessible(true);
    long oldValue = ((long)field.get(rsh));
    field.set(rsh, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    RegionScannerHolder existing = scanners.putIfAbsent(scannerName, rsh);
    assert existing == null : "scannerId must be unique within regionserver's whole lifecycle! "
      + scannerName + ", " + existing;
    return rsh;
  }

  private boolean isFullRegionScan(Scan scan, HRegion region) {
    // If the scan start row equals or less than the start key of the region
    // and stop row greater than equals end key (if stop row present)
    // or if the stop row is empty
    // account this as a full region scan
    if (
      Bytes.compareTo(scan.getStartRow(), region.getRegionInfo().getStartKey()) <= 0
        && (Bytes.compareTo(scan.getStopRow(), region.getRegionInfo().getEndKey()) >= 0
          && !Bytes.equals(region.getRegionInfo().getEndKey(), HConstants.EMPTY_END_ROW)
          || Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))
    ) {
      return true;
    }
    return false;
  }

  /**
   * Find the HRegion based on a region specifier
   * @param regionSpecifier the region specifier
   * @return the corresponding region
   * @throws IOException if the specifier is not null, but failed to find the region
   */
  public HRegion getRegion(final RegionSpecifier regionSpecifier) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("532a1b1b-6e03-38b7-8113-bdbc3be1f285"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d7f97100-5904-35e1-9d8c-7458590232f3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ed15a5b4-5b02-3263-a4d8-8a79c5fe2ede"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0d69d0f5-a560-316b-93b4-d7f6a0e64282"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f1de555-23fc-3f52-b0b6-bbb1396b06d5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("26c988c0-a52f-3ed1-a9bb-089d627ae4f6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a2f8f839-5f0d-34eb-b0db-82ebdcb83484"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("71824ae5-5c1a-3d9f-b70c-20666a43bdd8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("df4859b9-8a92-3a10-9add-1b5d5767eb59"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e1cce5ba-9eb3-3731-9963-2bc6e5f371ce"))) {
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
    return regionServer.getRegion(regionSpecifier.getValue().toByteArray());
  }

  /**
   * Find the List of HRegions based on a list of region specifiers
   * @param regionSpecifiers the list of region specifiers
   * @return the corresponding list of regions
   * @throws IOException if any of the specifiers is not null, but failed to find the region
   */
  private List<HRegion> getRegions(final List<RegionSpecifier> regionSpecifiers,
    final CacheEvictionStatsBuilder stats) {
if(KnobRuntime.check(java.util.UUID.fromString("803ab02d-a700-34b9-a40e-00bf5ffa8e2c"))) {
try {
    java.lang.reflect.Field field = stats.getClass().getDeclaredField("maxCacheSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stats));
    field.set(stats, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d66cfc5b-aad6-3ce2-abdc-f4cd7c82c7e5"))) {
try {
    java.lang.reflect.Field field = stats.getClass().getDeclaredField("maxCacheSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stats));
    field.set(stats, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a8b17bf-7173-334f-b870-c071ad41b927"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("4767d0e2-714c-325c-9965-7194c9b3c745"))) {
try {
    java.lang.reflect.Field field = stats.getClass().getDeclaredField("evictedBlocks");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stats));
    field.set(stats, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    List<HRegion> regions = Lists.newArrayListWithCapacity(regionSpecifiers.size());
    for (RegionSpecifier regionSpecifier : regionSpecifiers) {
      try {
        regions.add(regionServer.getRegion(regionSpecifier.getValue().toByteArray()));
      } catch (NotServingRegionException e) {
        stats.addException(regionSpecifier.getValue().toByteArray(), e);
      }
    }
    return regions;
  }

  public PriorityFunction getPriority() {
    return priority;
  }

  public Configuration getConfiguration() {
    return regionServer.getConfiguration();
  }

  private RegionServerRpcQuotaManager getRpcQuotaManager() {
    return regionServer.getRegionServerRpcQuotaManager();
  }

  private RegionServerSpaceQuotaManager getSpaceQuotaManager() {
    return regionServer.getRegionServerSpaceQuotaManager();
  }

  void start(ZKWatcher zkWatcher) {
    if (AccessChecker.isAuthorizationSupported(getConfiguration())) {
      accessChecker = new AccessChecker(getConfiguration());
    } else {
      accessChecker = new NoopAccessChecker(getConfiguration());
    }
    zkPermissionWatcher =
      new ZKPermissionWatcher(zkWatcher, accessChecker.getAuthManager(), getConfiguration());
    try {
      zkPermissionWatcher.start();
    } catch (KeeperException e) {
      LOG.error("ZooKeeper permission watcher initialization failed", e);
    }
    this.scannerIdGenerator = new ScannerIdGenerator(this.regionServer.serverName);
    rpcServer.start();
  }

  void stop() {
    if (zkPermissionWatcher != null) {
      zkPermissionWatcher.close();
    }
    closeAllScanners();
    rpcServer.stop();
  }

  /**
   * Called to verify that this server is up and running.
   */
  // TODO : Rename this and HMaster#checkInitialized to isRunning() (or a better name).
  protected void checkOpen() throws IOException {
    if (regionServer.isAborted()) {
      throw new RegionServerAbortedException("Server " + regionServer.serverName + " aborting");
    }
    if (regionServer.isStopped()) {
      throw new RegionServerStoppedException("Server " + regionServer.serverName + " stopping");
    }
    if (!regionServer.isDataFileSystemOk()) {
      throw new RegionServerStoppedException("File system not available");
    }
    if (!regionServer.isOnline()) {
      throw new ServerNotRunningYetException(
        "Server " + regionServer.serverName + " is not running yet");
    }
  }

  /**
   * By default, put up an Admin and a Client Service. Set booleans
   * <code>hbase.regionserver.admin.executorService</code> and
   * <code>hbase.regionserver.client.executorService</code> if you want to enable/disable services.
   * Default is that both are enabled.
   * @return immutable list of blocking services and the security info classes that this server
   *         supports
   */
  protected List<BlockingServiceAndInterface> getServices() {
    boolean admin = getConfiguration().getBoolean(REGIONSERVER_ADMIN_SERVICE_CONFIG, true);
    boolean client = getConfiguration().getBoolean(REGIONSERVER_CLIENT_SERVICE_CONFIG, true);
    boolean clientMeta =
      getConfiguration().getBoolean(REGIONSERVER_CLIENT_META_SERVICE_CONFIG, true);
    boolean bootstrapNodes =
      getConfiguration().getBoolean(REGIONSERVER_BOOTSTRAP_NODES_SERVICE_CONFIG, true);
    List<BlockingServiceAndInterface> bssi = new ArrayList<>();
    if (client) {
      bssi.add(new BlockingServiceAndInterface(ClientService.newReflectiveBlockingService(this),
        ClientService.BlockingInterface.class));
    }
    if (admin) {
      bssi.add(new BlockingServiceAndInterface(AdminService.newReflectiveBlockingService(this),
        AdminService.BlockingInterface.class));
    }
    if (clientMeta) {
      bssi.add(new BlockingServiceAndInterface(ClientMetaService.newReflectiveBlockingService(this),
        ClientMetaService.BlockingInterface.class));
    }
    if (bootstrapNodes) {
      bssi.add(
        new BlockingServiceAndInterface(BootstrapNodeService.newReflectiveBlockingService(this),
          BootstrapNodeService.BlockingInterface.class));
    }
    return new ImmutableList.Builder<BlockingServiceAndInterface>().addAll(bssi).build();
  }

  public InetSocketAddress getSocketAddress() {
    return isa;
  }

  @Override
  public int getPriority(RequestHeader header, Message param, User user) {
    return priority.getPriority(header, param, user);
  }

  @Override
  public long getDeadline(RequestHeader header, Message param) {
    return priority.getDeadline(header, param);
  }

  /*
   * Check if an OOME and, if so, abort immediately to avoid creating more objects.
   * @return True if we OOME'd and are aborting.
   */
  @Override
  public boolean checkOOME(final Throwable e) {
    return exitIfOOME(e);
  }

  public static boolean exitIfOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (
        e instanceof OutOfMemoryError
          || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
          || (e.getMessage() != null && e.getMessage().contains("java.lang.OutOfMemoryError"))
      ) {
        stop = true;
        LOG.error(HBaseMarkers.FATAL, "Run out of memory; " + RSRpcServices.class.getSimpleName()
          + " will abort itself immediately", e);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
  }

  /**
   * Close a region on the region server.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public CloseRegionResponse closeRegion(final RpcController controller,
    final CloseRegionRequest request) throws ServiceException {
    final ServerName sn = (request.hasDestinationServer()
      ? ProtobufUtil.toServerName(request.getDestinationServer())
      : null);

    try {
      checkOpen();
      throwOnWrongStartCode(request);
      final String encodedRegionName = ProtobufUtil.getRegionEncodedName(request.getRegion());

      requestCount.increment();
      if (sn == null) {
        LOG.info("Close " + encodedRegionName + " without moving");
      } else {
        LOG.info("Close " + encodedRegionName + ", moving to " + sn);
      }
      boolean closed = regionServer.closeRegion(encodedRegionName, false, sn);
      CloseRegionResponse.Builder builder = CloseRegionResponse.newBuilder().setClosed(closed);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Compact a region on the region server.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public CompactRegionResponse compactRegion(final RpcController controller,
    final CompactRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      // Quota support is enabled, the requesting user is not system/super user
      // and a quota policy is enforced that disables compactions.
      if (
        QuotaUtil.isQuotaEnabled(getConfiguration())
          && !Superusers.isSuperUser(RpcServer.getRequestUser().orElse(null))
          && this.regionServer.getRegionServerSpaceQuotaManager()
            .areCompactionsDisabled(region.getTableDescriptor().getTableName())
      ) {
        throw new DoNotRetryIOException(
          "Compactions on this region are " + "disabled due to a space quota violation.");
      }
      region.startRegionOperation(Operation.COMPACT_REGION);
      LOG.info("Compacting " + region.getRegionInfo().getRegionNameAsString());
      boolean major = request.hasMajor() && request.getMajor();
      if (((KnobRuntime.check(java.util.UUID.fromString("5b6419a9-180f-32ba-94a3-daca156fab4b"))) ? (request.hasMajor()) : (((KnobRuntime.check(java.util.UUID.fromString("19e9b4ea-b134-3da8-8f13-03954ec276c4"))) ? (request.getMajor()) : (request.hasFamily()))))) {
        byte[] family = request.getFamily().toByteArray();
        String log = "User-triggered " + (major ? "major " : "") + "compaction for region "
          + region.getRegionInfo().getRegionNameAsString() + " and family "
          + Bytes.toString(family);
        LOG.trace(log);
        region.requestCompaction(family, log, Store.PRIORITY_USER, major,
          CompactionLifeCycleTracker.DUMMY);
      } else {
        String log = "User-triggered " + (major ? "major " : "") + "compaction for region "
          + region.getRegionInfo().getRegionNameAsString();
        LOG.trace(log);
if(KnobRuntime.check(java.util.UUID.fromString("21ef00fc-9dd2-3ca9-9ef2-490c89279469"))) {
major = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("01d8793b-93e9-3551-9f77-531365dd7f77"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b01355c5-6515-3788-aae1-e84f1bea816a"))) {
major = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("331730de-4a10-3a4e-a124-1bc9cf23660e"))) {
major = !major;
}
        if (KnobRuntime.check(java.util.UUID.fromString("0f6af157-f639-3620-8b9d-b2bfddeaf57c"))) { region.requestCompaction(log, Store.PRIORITY_USER, !major, CompactionLifeCycleTracker.DUMMY); } else { region.requestCompaction(log, Store.PRIORITY_USER, major, CompactionLifeCycleTracker.DUMMY); }
      }
      return CompactRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CompactionSwitchResponse compactionSwitch(RpcController controller,
    CompactionSwitchRequest request) throws ServiceException {
    rpcPreCheck("compactionSwitch");
    final CompactSplit compactSplitThread = regionServer.getCompactSplitThread();
    requestCount.increment();
    boolean prevState = compactSplitThread.isCompactionsEnabled();
    CompactionSwitchResponse response =
      CompactionSwitchResponse.newBuilder().setPrevState(prevState).build();
    if (prevState == request.getEnabled()) {
      // passed in requested state is same as current state. No action required
      return response;
    }
    compactSplitThread.switchCompaction(request.getEnabled());
    return response;
  }

  /**
   * Flush a region on the region server.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public FlushRegionResponse flushRegion(final RpcController controller,
    final FlushRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Flushing " + region.getRegionInfo().getRegionNameAsString());
      boolean shouldFlush = true;
      if (request.hasIfOlderThanTs()) {
        shouldFlush = region.getEarliestFlushTimeForAllStores() < request.getIfOlderThanTs();
      }
      FlushRegionResponse.Builder builder = FlushRegionResponse.newBuilder();
      if (shouldFlush) {
        boolean writeFlushWalMarker =
          request.hasWriteFlushWalMarker() ? request.getWriteFlushWalMarker() : false;
        // Go behind the curtain so we can manage writing of the flush WAL marker
        HRegion.FlushResultImpl flushResult = null;
        if (request.hasFamily()) {
          List<byte[]> families = new ArrayList();
          families.add(request.getFamily().toByteArray());
          TableDescriptor tableDescriptor = region.getTableDescriptor();
          List<String> noSuchFamilies =
            families.stream().filter(f -> !tableDescriptor.hasColumnFamily(f)).map(Bytes::toString)
              .collect(Collectors.toList());
          if (!noSuchFamilies.isEmpty()) {
            throw new NoSuchColumnFamilyException("Column families " + noSuchFamilies
              + " don't exist in table " + tableDescriptor.getTableName().getNameAsString());
          }
          flushResult =
            region.flushcache(families, writeFlushWalMarker, FlushLifeCycleTracker.DUMMY);
        } else {
          flushResult = region.flushcache(true, writeFlushWalMarker, FlushLifeCycleTracker.DUMMY);
        }
        boolean compactionNeeded = flushResult.isCompactionNeeded();
        if (compactionNeeded) {
          regionServer.getCompactSplitThread().requestSystemCompaction(region,
            "Compaction through user triggered flush");
        }
        builder.setFlushed(flushResult.isFlushSucceeded());
        builder.setWroteFlushWalMarker(flushResult.wroteFlushWalMarker);
      }
      builder.setLastFlushTime(region.getEarliestFlushTimeForAllStores());
      return builder.build();
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places. If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of wal
      // is required. Currently the only way to do this is a restart of
      // the server.
      regionServer.abort("Replay of WAL required. Forcing server shutdown", ex);
      throw new ServiceException(ex);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public GetOnlineRegionResponse getOnlineRegion(final RpcController controller,
    final GetOnlineRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      Map<String, HRegion> onlineRegions = regionServer.getOnlineRegions();
      List<RegionInfo> list = new ArrayList<>(onlineRegions.size());
      for (HRegion region : onlineRegions.values()) {
        list.add(region.getRegionInfo());
      }
      list.sort(RegionInfo.COMPARATOR);
      return ResponseConverter.buildGetOnlineRegionResponse(list);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  // Master implementation of this Admin Service differs given it is not
  // able to supply detail only known to RegionServer. See note on
  // MasterRpcServers#getRegionInfo.
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
    final GetRegionInfoRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      RegionInfo info = region.getRegionInfo();
      byte[] bestSplitRow;
      if (request.hasBestSplitRow() && request.getBestSplitRow()) {
        bestSplitRow = region.checkSplit(true).orElse(null);
        // when all table data are in memstore, bestSplitRow = null
        // try to flush region first
        if (bestSplitRow == null) {
if(KnobRuntime.check(java.util.UUID.fromString("50b1f2a7-8fdd-34e4-b058-c5fef6f56578"))) {
throw new java.io.IOException("Injected exception");
}
          region.flush(true);
          bestSplitRow = region.checkSplit(true).orElse(null);
        }
      } else {
        bestSplitRow = null;
      }
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(ProtobufUtil.toRegionInfo(info));
      if (((KnobRuntime.check(java.util.UUID.fromString("0768f5c2-5813-3aa2-a2db-9c8ea25606c4"))) ? ((request.getBestSplitRow()) || (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("242bd7cf-7dd6-35a8-a589-5b07dd06039f"))) ? ((request.hasCompactionState()) || (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("05c92bd7-e420-3141-b71d-40d985977c11"))) ? (request.hasBestSplitRow()) : (((KnobRuntime.check(java.util.UUID.fromString("1123b7eb-928c-350f-a573-8f7084578b0f"))) ? (request.hasCompactionState()) : (((KnobRuntime.check(java.util.UUID.fromString("f2f70fdc-4962-3c4b-b4d7-adccec33699c"))) ? ((request.hasBestSplitRow()) && (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("3836b285-1fab-3e1b-89c3-e644fcb83f0e"))) ? ((request.hasBestSplitRow()) && (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("7c84e1b8-e42b-354a-9d80-3a32ffe48347"))) ? ((request.hasBestSplitRow()) || (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("1df1d167-2490-34db-8cbf-b601085857d5"))) ? ((request.getBestSplitRow()) && (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("09702111-c2de-393f-925c-dec6207955c3"))) ? ((request.hasCompactionState()) && (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("98531f7c-768c-35da-881a-a4a7685c94bd"))) ? ((request.getBestSplitRow()) || (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("a6bf3eb6-1619-3941-8881-563bc0cc9db6"))) ? ((request.hasCompactionState()) && (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("9c0cb2c0-f87d-3061-ad9f-3ef6fbc5f882"))) ? (request.getCompactionState()) : (((KnobRuntime.check(java.util.UUID.fromString("517a610e-6a8a-3c05-9ab6-745c4eb484d5"))) ? ((request.hasCompactionState()) || (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("27a1c8e4-aff0-3396-8605-5e7532112777"))) ? (request.getBestSplitRow()) : (((KnobRuntime.check(java.util.UUID.fromString("f0663aba-535a-3c50-9334-b449a00d7e8f"))) ? ((request.hasBestSplitRow()) && (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("d15cac14-a684-302b-9030-363c8d33e16d"))) ? ((request.hasBestSplitRow()) && (request.getCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("4cc08ede-b75c-379a-8dfa-45e21ab3ed2d"))) ? ((request.hasBestSplitRow()) || (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("fa246f64-9c74-3f28-8a09-ad0478b71721"))) ? ((request.hasCompactionState()) || (request.getCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("65be6398-e6c4-31bc-bcbf-99722d54cbbf"))) ? ((request.getBestSplitRow()) && (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("2839f068-2d3f-3696-ab2c-6466b07ebc3f"))) ? ((request.hasBestSplitRow()) || (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("d3c410ab-5888-39a2-b642-054f01f03889"))) ? ((request.hasCompactionState()) && (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("4614288b-c6c9-3175-9ed3-2a856a5b650b"))) ? ((request.getBestSplitRow()) && (request.getCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("e0e6312c-bbca-30f4-bd13-545282c23c91"))) ? ((request.getBestSplitRow()) || (request.getCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("2a504000-7f45-3c63-8f76-120772f06b5b"))) ? ((request.hasCompactionState()) && (request.getCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("4a66006f-917d-3e5a-b8c9-745ad5264b69"))) ? ((request.hasBestSplitRow()) || (request.getCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("5f19aa5d-904d-3172-8e88-97092ce78139"))) ? ((request.getBestSplitRow()) && (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("d7e74cb1-9f33-3f2d-97de-45135043163e"))) ? ((request.getBestSplitRow()) || (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("52cb1eea-ee10-3293-8366-4a373267e10b"))) ? ((request.hasCompactionState()) || (request.hasCompactionState())) : (request.hasCompactionState() && request.getCompactionState()))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        builder.setCompactionState(ProtobufUtil.createCompactionState(region.getCompactionState()));
      }
      builder.setSplittable(region.isSplittable());
      builder.setMergeable(region.isMergeable());
      if (((KnobRuntime.check(java.util.UUID.fromString("59d0ebe1-fb86-3189-b5f1-3fd5753883f0"))) ? (((request.hasBestSplitRow()) || (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("75d60f8e-5028-3c3d-bf84-6ba5aa44438e"))) ? ((request.hasCompactionState()) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("587d0c1a-d54c-31b8-8010-b79864fa85f2"))) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("eeb2d044-be50-3ba0-b4bf-dd5d82c85eb1"))) ? (((request.hasCompactionState()) && (request.hasBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("1767e4dd-afa5-3b60-aa57-f624ba503b08"))) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7c714be5-9291-3a6d-80fa-c7aa9700f0f9"))) ? (((request.hasCompactionState()) || (request.hasBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ed6e54f4-27bd-30c0-a51c-460cb3bb8747"))) ? (((request.hasBestSplitRow()) || (request.hasBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("9b4c3892-740b-38f2-acad-44ee0c962fe4"))) ? ((request.getBestSplitRow()) || (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("1c861562-e43d-3466-bab6-f4b757537082"))) ? (((request.hasBestSplitRow()) && (request.hasBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("288b3501-53a2-32b0-8635-cbd5270abd1c"))) ? (((request.hasBestSplitRow()) && (request.getBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0032f32c-2dfb-39f4-8666-c95686ca7707"))) ? (((request.hasCompactionState()) || (request.getBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d6e6c0fb-396a-3e2d-893c-2f995a3dbbf3"))) ? (((request.getBestSplitRow()) && (request.getBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("e74ccf8b-4eb6-3a8f-8fb1-9b428ead3052"))) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c31aed50-4345-335f-a80f-3ab85dd75658"))) ? (((request.getBestSplitRow()) || (request.hasBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("1f3b99b3-c4d4-3563-a699-41f9a36728bd"))) ? (((request.getBestSplitRow()) && (request.getBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9e8e1baf-0aff-34b1-9ed4-fa400314cae4"))) ? (((request.hasCompactionState()) && (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("e9b2fd47-4307-37d8-9cfc-03ad42f282c5"))) ? (((request.hasBestSplitRow()) && (request.hasBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6b3fcdcd-35f3-3b6d-933c-e15c162847d5"))) ? (((request.getBestSplitRow()) && (request.getBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("e4886b6f-b11e-3581-ba6a-bc7dac183e8a"))) ? (((request.hasCompactionState()) || (request.hasBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d35935d9-7e47-3f14-acf5-9c2cef028439"))) ? (((request.hasCompactionState()) && (request.hasCompactionState())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("add6dab5-10d0-320b-8c23-6b4aefdc8619"))) ? (((request.hasBestSplitRow()) || (request.getBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6e3b9bba-6a20-33c2-9949-fe5296777d6d"))) ? (((request.hasCompactionState()) || (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c40e1cbe-86c5-35f2-a25c-f298cbad302c"))) ? (((request.hasCompactionState()) && (request.hasCompactionState())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c608594d-85ca-3ce1-8f77-df6592edcd13"))) ? (((request.getBestSplitRow()) && (request.getBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("dd8e36ed-c23f-374c-aa53-158113d1a348"))) ? ((request.hasCompactionState()) && (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("012aeb34-27be-3366-89e8-71a0c5b6ad82"))) ? (((request.hasCompactionState()) || (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("406b1dd1-0bb0-3973-b253-d8ead91e8692"))) ? (((request.getBestSplitRow()) || (request.getBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c52f77d2-986c-3dad-9a1b-aff778c90a4d"))) ? (((request.hasBestSplitRow()) || (request.getBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("306f5306-0168-356a-bb87-111ba86b3411"))) ? (((request.hasBestSplitRow()) && (request.hasCompactionState())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4a6b4808-e1b5-378f-9eb0-b79d5ac10dca"))) ? ((request.hasCompactionState()) && (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("27b9272a-5099-3e68-b8c7-5e14cf7ab1f6"))) ? (((request.hasCompactionState()) || (request.getBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f3a4a36a-0938-3842-b040-f19412d76d93"))) ? (((request.hasCompactionState()) || (request.hasCompactionState())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("8f6c73f3-2f91-3287-a06f-1bb79f564a4f"))) ? (((request.getBestSplitRow()) && (request.hasBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4e80e21c-453a-3628-a4cf-8eea799aee0a"))) ? ((request.hasCompactionState()) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2d900512-7d7c-341d-8a81-04bd51830a1e"))) ? (((request.hasBestSplitRow()) || (request.getBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("27907091-2703-3614-8f89-2a82a4734470"))) ? ((bestSplitRow) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5e77dac3-b0fb-3aed-b65c-39707a4b3399"))) ? ((request.getBestSplitRow()) && (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("2d824e1b-56df-3f14-b89b-d8e00fc80d5c"))) ? (((request.hasCompactionState()) && (request.getBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e522f97a-7d56-3bd8-aeb8-3005945efedf"))) ? (((request.hasBestSplitRow()) || (request.hasBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("6c1a9cc1-ca30-37cb-86a4-81b542c979c9"))) ? (((request.hasBestSplitRow()) && (request.hasBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("02fa5858-8e67-3d14-9304-d3cfb156924c"))) ? (((request.hasBestSplitRow()) && (request.getBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("352bf22a-bc6f-3d60-9b62-ca457b98b5d6"))) ? (((request.hasBestSplitRow()) && (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("fbf4a7ac-a838-3e63-839e-6d3191d7110f"))) ? (((request.hasBestSplitRow()) && (request.getBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0fc07843-d283-3df5-8f02-ec9305886601"))) ? ((request.getBestSplitRow()) || (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("504a0672-01e5-3727-b339-a6a928d32221"))) ? (((request.hasBestSplitRow()) || (request.hasBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e316d606-1e92-3290-a68d-d35f3f4df550"))) ? (((request.getBestSplitRow()) || (request.getBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1fde1b3a-a993-3d7e-a4c9-a7bb5c8ada2d"))) ? ((bestSplitRow) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a76e92d8-613f-3458-bbc7-29e9d7bca696"))) ? (((request.hasCompactionState()) || (request.getBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("387154bc-520f-37d9-890a-5beb644e46bd"))) ? ((request.hasBestSplitRow()) && (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("bb38dc1d-0bad-3c1b-93b7-6e9102972b0d"))) ? ((request.getBestSplitRow()) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4b1c9fcf-db12-3e44-97ed-d947bcb15852"))) ? (((request.hasCompactionState()) || (request.hasCompactionState())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2d8900dd-c727-38e4-9997-651a099c5541"))) ? (((request.hasBestSplitRow()) || (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("39623829-9097-3412-9031-27abdcecb825"))) ? ((request.hasBestSplitRow()) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("759d375b-6695-3a2a-a9f8-40c8432c72bb"))) ? (((request.getBestSplitRow()) && (request.getBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("37ab919e-8583-3766-b305-fdcec8148e34"))) ? ((request.getBestSplitRow()) && (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("c0b3d7f3-e15a-3006-b173-4e0190aa8857"))) ? (request.hasBestSplitRow() && request.getBestSplitRow()) : (((KnobRuntime.check(java.util.UUID.fromString("904bba20-e8bd-3ec8-8563-083a3b9919c0"))) ? (((request.hasBestSplitRow()) && (request.getBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c24c62e5-39f6-348b-87e3-bac88c227ef6"))) ? (((request.hasBestSplitRow()) && (request.hasBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("69105ec9-eb65-3e73-9538-eca71839df1c"))) ? ((request.getBestSplitRow()) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("b74416f6-7e8d-3f01-bc34-ccfc09e8d5da"))) ? (((request.getBestSplitRow()) || (request.hasBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0abff7ec-2c38-37c4-b5bc-e6f292d4ed60"))) ? ((request.hasBestSplitRow() && request.getBestSplitRow()) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("87c8cf59-39a8-3cf8-9a8b-ea4d0f42e054"))) ? (((request.getBestSplitRow()) || (request.getBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c822d514-1181-3d12-924f-2b2ff1f138dd"))) ? (request.hasCompactionState()) : (((KnobRuntime.check(java.util.UUID.fromString("306f21c3-fef3-3313-8bac-e94ad63626a4"))) ? (((request.hasCompactionState()) && (request.hasCompactionState())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("a0a58828-6d85-3e59-858f-8b97f14d07de"))) ? ((request.hasBestSplitRow()) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c20c9e3e-2527-318d-90e6-d90780ad4643"))) ? ((request.hasBestSplitRow()) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("81390626-304e-39ec-a052-c3016f44bbaf"))) ? (((request.hasBestSplitRow()) && (request.hasCompactionState())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e9b7982c-0c6e-3a5c-8737-7aa9e4864d02"))) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("9009f6ca-400d-37f4-9e62-e7c3982a6ea9"))) ? (((request.hasBestSplitRow()) || (request.hasCompactionState())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("dab7a786-0381-38f8-bae6-9600ba79ca52"))) ? (request.hasBestSplitRow()) : (((KnobRuntime.check(java.util.UUID.fromString("3409c562-a344-3c4c-8787-b2310a558211"))) ? (bestSplitRow != null) : (((KnobRuntime.check(java.util.UUID.fromString("60a4df4e-0cf4-3e6c-9174-db161189b491"))) ? (((request.getBestSplitRow()) || (request.getBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("5d9e96ac-6d8b-31af-a362-089f1695950b"))) ? (((request.getBestSplitRow()) && (request.hasBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f51ec109-0252-3686-a281-a571ac527695"))) ? (((request.hasBestSplitRow()) || (request.hasBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("db47d568-a7a6-3b80-9fd1-24567a079ab4"))) ? (((request.getBestSplitRow()) && (request.hasBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("f179bf02-6ff3-31d9-ac24-e8eae9f857d1"))) ? ((request.getBestSplitRow()) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c5109e21-d683-373f-b91d-8c72e5e67ec0"))) ? (((request.hasBestSplitRow()) || (request.hasCompactionState())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("affd148d-002b-3bff-b2d9-c54de585fed5"))) ? (((request.hasBestSplitRow()) && (request.hasCompactionState())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("567c9daa-9afe-3efa-9fed-f83ae58f6fe5"))) ? ((request.hasBestSplitRow()) && (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("21b421d9-681e-3d11-a0f3-b3841931f00f"))) ? (((request.hasCompactionState()) || (request.getBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("0ac91f5e-a416-3f22-a662-ad0d290167ac"))) ? (((request.hasCompactionState()) && (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("79b9ab1c-2d39-398f-8d8a-7380f4f5b867"))) ? (((request.hasBestSplitRow()) || (request.getBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("d6ae6afd-e645-32a6-9a2d-480d4eb95955"))) ? (((request.hasBestSplitRow()) && (request.getBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("b0fe883e-5bfe-38b1-90d4-e6b7a97b1b11"))) ? (((request.hasBestSplitRow()) || (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6fe78f6c-ff5a-3631-a493-3ab5302870e0"))) ? ((request.hasBestSplitRow()) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c685c37b-de0e-368e-9373-557d76434e84"))) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9e292787-b2c5-35c2-bcec-d30de020b5ef"))) ? ((request.getBestSplitRow()) && (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("847d8ea0-6912-3263-b27b-f965f527daef"))) ? ((request.hasBestSplitRow() && request.getBestSplitRow()) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4682df8c-8c97-34a8-b9c6-025fc40e55e9"))) ? (((request.hasBestSplitRow()) && (request.hasCompactionState())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a6d9f4b5-2a91-3d12-a4f1-d526badb4025"))) ? ((request.hasCompactionState()) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("5fe1b250-8ad7-3518-9b65-96c36ad260dd"))) ? (((request.hasCompactionState()) && (request.getBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("e74fa02d-5c9f-33dc-b9bf-3ed96899446d"))) ? (((request.getBestSplitRow()) || (request.hasBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ec6b5c1a-7bf7-3eb8-8e86-65d969de10dd"))) ? ((request.getBestSplitRow()) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("57c8b79f-88e6-3c98-9f2c-2b0b06ea64c1"))) ? ((request.hasBestSplitRow()) || (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("1535c53b-f047-332a-9afd-4f7b3465258c"))) ? ((request.hasBestSplitRow()) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3b0163ec-051e-3acd-a7b9-0295d8ebd3e8"))) ? (((request.hasBestSplitRow()) && (request.hasBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("21de8242-51d2-3b78-97ec-cd5743d20337"))) ? ((request.hasCompactionState()) || (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("2e389a81-a53d-3034-a508-fc040c268da4"))) ? (((request.hasCompactionState()) && (request.hasCompactionState())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("15f4f9cd-584a-383f-aa65-991095466b8b"))) ? (((request.hasCompactionState()) || (request.getBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f982c93e-fada-3f8b-831e-c79a718d4715"))) ? ((request.hasBestSplitRow() && request.getBestSplitRow()) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("88b9574e-62f1-3807-8720-da2a00e01c77"))) ? (((request.hasCompactionState()) || (request.hasBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("77a3b6f5-c596-309f-823f-30b39fadbb9a"))) ? (((request.hasCompactionState()) && (request.getBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4f57b545-6120-3ef7-9dfe-5915e6c95fb1"))) ? (((request.getBestSplitRow()) && (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("da4fd2ab-78e7-3934-bdfc-f0e4f9896619"))) ? (((request.hasBestSplitRow()) && (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("14ccb0d1-786a-3866-95de-fcd30ab90415"))) ? ((request.getBestSplitRow()) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("8442d207-98e4-3ee8-a78a-fca3f0e1dc17"))) ? ((request.hasCompactionState()) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3f0ce31a-cd81-39f2-8ed8-3e447bb11786"))) ? (((request.hasCompactionState()) && (request.hasBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("d8f5651d-9791-3d10-9435-48ea783d62f2"))) ? (((request.getBestSplitRow()) && (request.hasBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4dd2e671-7e4f-316c-87c9-c1e809a0dfaf"))) ? (((request.hasBestSplitRow()) || (request.getBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("5d8c2338-06a1-3fbf-b987-dd28f79f96ae"))) ? (((request.hasCompactionState()) && (request.hasBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6d80f778-8ce6-3655-8282-6a3420a8f5b3"))) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("747b16a5-0900-3ba2-8e4a-230ec47fcf35"))) ? ((request.hasBestSplitRow() && request.getBestSplitRow()) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("e20bb679-4f0d-3a6f-806f-f969e2635129"))) ? (((request.hasCompactionState()) && (request.getBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5c63f26a-591d-3a93-ab25-efa466886690"))) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("6cf3459e-43a3-3f08-8adc-77c73fb370d8"))) ? ((request.getBestSplitRow()) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4e0d5524-1b39-3fd5-86f9-d116016cf58b"))) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("af0e8079-6e4e-3243-9f99-b791113ddc8a"))) ? (((request.hasCompactionState()) && (request.getBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("69fa3a31-e10f-3405-ad4b-cf0f64d566c6"))) ? (((request.hasCompactionState()) || (request.hasCompactionState())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("dce3caa4-41ff-3977-8be5-0725127fe332"))) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("043aeac5-4d22-3e93-bf8c-6bb1e8882280"))) ? (((request.hasCompactionState()) || (request.hasBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("79ca4d7b-09a8-3bc2-bbb6-8a7ca3fcf492"))) ? (((request.hasCompactionState()) && (request.hasBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a320b9ab-2b14-3cb6-a339-f696c12d4a53"))) ? (((request.getBestSplitRow()) && (request.getBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8e567e82-07b9-305b-87e3-293d8584d39b"))) ? ((request.hasBestSplitRow()) && (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("97bb5897-fc3f-3245-9ee2-43ee2840ff42"))) ? (((request.getBestSplitRow()) || (request.getBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("cd6a0eef-584d-3ae4-b0bd-95f4e179443a"))) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("77c313f4-d423-375f-842d-7604060440c2"))) ? (((request.getBestSplitRow()) || (request.hasBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("674c5b9e-7040-3c8f-8d6b-694b61724d41"))) ? ((request.hasCompactionState()) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("6e71251a-a17d-327c-8650-19de33bc83ad"))) ? (((request.hasCompactionState()) && (request.hasBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8f770ee2-c979-3eae-a05b-a42cde1f2492"))) ? ((request.hasCompactionState()) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f6948d23-faff-34d4-84b9-b1fa5d4467de"))) ? ((request.hasBestSplitRow() && request.getBestSplitRow()) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d3cfebae-e5d2-320f-a00d-e048580b1cef"))) ? ((request.hasCompactionState()) && (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("6667a064-b0a2-3eaf-a46d-1baeeba0a599"))) ? ((request.hasBestSplitRow()) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("68987c74-fe69-3903-9dd7-4be55ab1c75c"))) ? (((request.hasCompactionState()) && (request.getBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1421469b-61ec-302d-b59e-23522d87ebb4"))) ? (((request.getBestSplitRow()) || (request.getBestSplitRow())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b68435bb-39ca-3ff8-a205-7629c97e3ac8"))) ? (((request.getBestSplitRow()) || (request.hasBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("fea5264a-8a4c-31af-a615-5009d8ad6d9f"))) ? (((request.getBestSplitRow()) && (request.hasBestSplitRow())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("bd574466-1001-36ab-a807-44508ec1af03"))) ? ((request.getBestSplitRow()) || (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("a903dfdf-358d-3d14-bae0-28a1dd8a3e0d"))) ? (((request.hasBestSplitRow()) && (request.getBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("835f852d-51a8-3c06-8059-4a6d03da3de2"))) ? (request.getBestSplitRow()) : (((KnobRuntime.check(java.util.UUID.fromString("8b269470-bdf6-31e0-894f-03805f2f7f6e"))) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1a624533-5ac2-3230-b3b5-7e61ffc9b6ee"))) ? ((request.hasBestSplitRow()) || (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("12c9574c-d966-38b2-ba43-d51eb5e7c17d"))) ? ((request.hasBestSplitRow() && request.getBestSplitRow()) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f2d0633f-937a-32c2-9cdc-4568baebf63e"))) ? ((request.hasCompactionState()) || (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("0534d21b-43d0-3396-85cc-b1d7cfe5d519"))) ? (((request.hasCompactionState()) && (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4f3418a1-c190-3228-b304-0ab63281de02"))) ? (((request.hasCompactionState()) || (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("0443248d-20fd-36e2-bca7-4374e2d5449b"))) ? (((request.hasBestSplitRow()) || (request.hasCompactionState())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ab6e8616-e0b9-3055-9b76-4951f39c20f0"))) ? (((request.hasCompactionState()) || (request.hasBestSplitRow())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("5ac246f6-d66e-30b8-a43d-a22c73468453"))) ? (((request.hasCompactionState()) || (request.hasCompactionState())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4b9279ea-77be-3c21-8bdf-af1a18bad59f"))) ? (((request.hasBestSplitRow()) && (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("96926605-c1d6-3269-8dfe-6dc89d147f78"))) ? ((request.hasCompactionState()) || (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("16e9bc79-ecb9-3188-bbc0-ea6f46271218"))) ? (((request.getBestSplitRow()) || (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e6b270fc-093b-34aa-8e57-4b371b38e63b"))) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3be678cc-e416-349a-9eee-1e767433c505"))) ? (((request.hasCompactionState()) || (request.getBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("536d62b3-6d17-3e3d-a827-4aac2f8ca5b1"))) ? (((request.hasBestSplitRow()) || (request.getBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bf7e477a-ad64-3b84-a02d-c69bfe19a8da"))) ? ((request.hasBestSplitRow()) || (request.hasCompactionState())) : (((KnobRuntime.check(java.util.UUID.fromString("c13bcd47-b371-3c6c-a014-f3a0a6271356"))) ? (((request.hasBestSplitRow()) || (request.hasCompactionState())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3ce4ec4-eac4-3423-96a9-7d9a6cb8e897"))) ? (((request.hasBestSplitRow()) || (request.hasBestSplitRow())) && ((bestSplitRow) != (null))) : (request.hasBestSplitRow() && request.getBestSplitRow() && bestSplitRow != null))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        builder.setBestSplitRow(UnsafeByteOperations.unsafeWrap(bestSplitRow));
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public GetRegionLoadResponse getRegionLoad(RpcController controller, GetRegionLoadRequest request)
    throws ServiceException {
if(KnobRuntime.check(java.util.UUID.fromString("30b6a432-cb07-3dc1-bed4-26a7bdf97045"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("003cda83-f4da-3291-a0a1-d0a10011917e"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16f5ff49-85f7-345b-990c-24e88cca3331"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b51d492-2df2-3aee-b494-ff9c3df68a11"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6e9e25e-4c25-3fb7-b9e1-3612ae884e47"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}

    List<HRegion> regions;
    if (request.hasTableName()) {
      TableName tableName = ProtobufUtil.toTableName(request.getTableName());
      regions = regionServer.getRegions(tableName);
    } else {
      regions = regionServer.getRegions();
    }
    List<RegionLoad> rLoads = new ArrayList<>(regions.size());
    RegionLoad.Builder regionLoadBuilder = ClusterStatusProtos.RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();

    try {
      for (HRegion region : regions) {
        rLoads.add(regionServer.createRegionLoad(region, regionLoadBuilder, regionSpecifier));
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    GetRegionLoadResponse.Builder builder = GetRegionLoadResponse.newBuilder();
    builder.addAllRegionLoads(rLoads);
    return builder.build();
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public ClearCompactionQueuesResponse clearCompactionQueues(RpcController controller,
    ClearCompactionQueuesRequest request) throws ServiceException {
    LOG.debug("Client=" + RpcServer.getRequestUserName().orElse(null) + "/"
      + RpcServer.getRemoteAddress().orElse(null) + " clear compactions queue");
    ClearCompactionQueuesResponse.Builder respBuilder = ClearCompactionQueuesResponse.newBuilder();
    requestCount.increment();
    if (clearCompactionQueues.compareAndSet(false, true)) {
      final CompactSplit compactSplitThread = regionServer.getCompactSplitThread();
      try {
        checkOpen();
        regionServer.getRegionServerCoprocessorHost().preClearCompactionQueues();
        for (String queueName : request.getQueueNameList()) {
          LOG.debug("clear " + queueName + " compaction queue");
          switch (queueName) {
            case "long":
              compactSplitThread.clearLongCompactionsQueue();
              break;
            case "short":
              compactSplitThread.clearShortCompactionsQueue();
              break;
            default:
              LOG.warn("Unknown queue name " + queueName);
              throw new IOException("Unknown queue name " + queueName);
          }
        }
        regionServer.getRegionServerCoprocessorHost().postClearCompactionQueues();
      } catch (IOException ie) {
        throw new ServiceException(ie);
      } finally {
        clearCompactionQueues.set(false);
      }
    } else {
      LOG.warn("Clear compactions queue is executing by other admin.");
    }
    return respBuilder.build();
  }

  /**
   * Get some information of the region server.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public GetServerInfoResponse getServerInfo(final RpcController controller,
    final GetServerInfoRequest request) throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    requestCount.increment();
    int infoPort = regionServer.infoServer != null ? regionServer.infoServer.getPort() : -1;
    return ResponseConverter.buildGetServerInfoResponse(regionServer.serverName, infoPort);
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public GetStoreFileResponse getStoreFile(final RpcController controller,
    final GetStoreFileRequest request) throws ServiceException {
    try {
      checkOpen();
      HRegion region = getRegion(request.getRegion());
      requestCount.increment();
      Set<byte[]> columnFamilies;
      if (request.getFamilyCount() == 0) {
        columnFamilies = region.getTableDescriptor().getColumnFamilyNames();
      } else {
        columnFamilies = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
        for (ByteString cf : request.getFamilyList()) {
          columnFamilies.add(cf.toByteArray());
        }
      }
      int nCF = columnFamilies.size();
      List<String> fileList = region.getStoreFileList(columnFamilies.toArray(new byte[nCF][]));
      GetStoreFileResponse.Builder builder = GetStoreFileResponse.newBuilder();
      builder.addAllStoreFile(fileList);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private void throwOnWrongStartCode(OpenRegionRequest request) throws ServiceException {
    if (!request.hasServerStartCode()) {
      LOG.warn("OpenRegionRequest for {} does not have a start code", request.getOpenInfoList());
      return;
    }
    throwOnWrongStartCode(request.getServerStartCode());
  }

  private void throwOnWrongStartCode(CloseRegionRequest request) throws ServiceException {
    if (!request.hasServerStartCode()) {
      LOG.warn("CloseRegionRequest for {} does not have a start code", request.getRegion());
      return;
    }
    throwOnWrongStartCode(request.getServerStartCode());
  }

  private void throwOnWrongStartCode(long serverStartCode) throws ServiceException {
    // check that we are the same server that this RPC is intended for.
    if (regionServer.serverName.getStartcode() != serverStartCode) {
      throw new ServiceException(new DoNotRetryIOException(
        "This RPC was intended for a " + "different server with startCode: " + serverStartCode
          + ", this server is: " + regionServer.serverName));
    }
  }

  private void throwOnWrongStartCode(ExecuteProceduresRequest req) throws ServiceException {
    if (req.getOpenRegionCount() > 0) {
      for (OpenRegionRequest openReq : req.getOpenRegionList()) {
        throwOnWrongStartCode(openReq);
      }
    }
    if (req.getCloseRegionCount() > 0) {
      for (CloseRegionRequest closeReq : req.getCloseRegionList()) {
        throwOnWrongStartCode(closeReq);
      }
    }
  }

  /**
   * Open asynchronously a region or a set of regions on the region server. The opening is
   * coordinated by ZooKeeper, and this method requires the znode to be created before being called.
   * As a consequence, this method should be called only from the master.
   * <p>
   * Different manages states for the region are:
   * </p>
   * <ul>
   * <li>region not opened: the region opening will start asynchronously.</li>
   * <li>a close is already in progress: this is considered as an error.</li>
   * <li>an open is already in progress: this new open request will be ignored. This is important
   * because the Master can do multiple requests if it crashes.</li>
   * <li>the region is already opened: this new open request will be ignored.</li>
   * </ul>
   * <p>
   * Bulk assign: If there are more than 1 region to open, it will be considered as a bulk assign.
   * For a single region opening, errors are sent through a ServiceException. For bulk assign,
   * errors are put in the response as FAILED_OPENING.
   * </p>
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public OpenRegionResponse openRegion(final RpcController controller,
    final OpenRegionRequest request) throws ServiceException {
    requestCount.increment();
    throwOnWrongStartCode(request);

    OpenRegionResponse.Builder builder = OpenRegionResponse.newBuilder();
    final int regionCount = request.getOpenInfoCount();
    final Map<TableName, TableDescriptor> htds = new HashMap<>(regionCount);
    final boolean isBulkAssign = regionCount > 1;
    try {
      checkOpen();
    } catch (IOException ie) {
      TableName tableName = null;
      if (regionCount == 1) {
        org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo ri =
          request.getOpenInfo(0).getRegion();
        if (ri != null) {
          tableName = ProtobufUtil.toTableName(ri.getTableName());
        }
      }
      if (!TableName.META_TABLE_NAME.equals(tableName)) {
        throw new ServiceException(ie);
      }
      // We are assigning meta, wait a little for regionserver to finish initialization.
      // Default to quarter of RPC timeout
      int timeout = regionServer.getConfiguration().getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT) >> 2;
      long endTime = EnvironmentEdgeManager.currentTime() + timeout;
      synchronized (regionServer.online) {
        try {
          while (
            EnvironmentEdgeManager.currentTime() <= endTime && !regionServer.isStopped()
              && !regionServer.isOnline()
          ) {
if(KnobRuntime.check(java.util.UUID.fromString("0cb61411-cd32-3a95-ac63-42f915dd5191"))) {
throw new java.lang.InterruptedException("Injected exception");
}
            if (KnobRuntime.check(java.util.UUID.fromString("b13af57c-5b07-3f34-9e9a-1f6efac7f1ec"))) { throwOnWrongStartCode(regionServer.msgInterval); } else { regionServer.online.wait(regionServer.msgInterval); }
          }
if(KnobRuntime.check(java.util.UUID.fromString("0a1d1dc6-d828-342a-b1e4-90c61b3aa9d1"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("8cce58a8-3cd9-3a0f-94e7-c25313e54f49"))) { closeAllScanners(); } else { checkOpen(); }
        } catch (InterruptedException t) {
          Thread.currentThread().interrupt();
          throw new ServiceException(t);
        } catch (IOException e) {
          throw new ServiceException(e);
        }
      }
    }

    long masterSystemTime = request.hasMasterSystemTime() ? request.getMasterSystemTime() : -1;

    for (RegionOpenInfo regionOpenInfo : request.getOpenInfoList()) {
      final RegionInfo region = ProtobufUtil.toRegionInfo(regionOpenInfo.getRegion());
      TableDescriptor htd;
      try {
        String encodedName = region.getEncodedName();
        byte[] encodedNameBytes = region.getEncodedNameAsBytes();
        final HRegion onlineRegion = regionServer.getRegion(encodedName);
        if (onlineRegion != null) {
          // The region is already online. This should not happen any more.
          String error = "Received OPEN for the region:" + region.getRegionNameAsString()
            + ", which is already online";
          LOG.warn(error);
          // regionServer.abort(error);
          // throw new IOException(error);
          builder.addOpeningState(RegionOpeningState.OPENED);
          continue;
        }
        LOG.info("Open " + region.getRegionNameAsString());

        final Boolean previous =
          regionServer.getRegionsInTransitionInRS().putIfAbsent(encodedNameBytes, Boolean.TRUE);

        if (Boolean.FALSE.equals(previous)) {
          if (regionServer.getRegion(encodedName) != null) {
            // There is a close in progress. This should not happen any more.
            String error = "Received OPEN for the region:" + region.getRegionNameAsString()
              + ", which we are already trying to CLOSE";
            regionServer.abort(error);
            throw new IOException(error);
          }
          regionServer.getRegionsInTransitionInRS().put(encodedNameBytes, Boolean.TRUE);
        }

        if (Boolean.TRUE.equals(previous)) {
          // An open is in progress. This is supported, but let's log this.
          LOG.info("Receiving OPEN for the region:" + region.getRegionNameAsString()
            + ", which we are already trying to OPEN"
            + " - ignoring this new request for this region.");
        }

        // We are opening this region. If it moves back and forth for whatever reason, we don't
        // want to keep returning the stale moved record while we are opening/if we close again.
        regionServer.removeFromMovedRegions(region.getEncodedName());

        if (previous == null || !previous.booleanValue()) {
          htd = htds.get(region.getTable());
          if (htd == null) {
            htd = regionServer.tableDescriptors.get(region.getTable());
            htds.put(region.getTable(), htd);
          }
          if (htd == null) {
            throw new IOException("Missing table descriptor for " + region.getEncodedName());
          }
          // If there is no action in progress, we can submit a specific handler.
          // Need to pass the expected version in the constructor.
          if (regionServer.executorService == null) {
            LOG.info("No executor executorService; skipping open request");
          } else {
            if (region.isMetaRegion()) {
              regionServer.executorService.submit(
                new OpenMetaHandler(regionServer, regionServer, region, htd, masterSystemTime));
            } else {
              if (regionOpenInfo.getFavoredNodesCount() > 0) {
                regionServer.updateRegionFavoredNodesMapping(region.getEncodedName(),
                  regionOpenInfo.getFavoredNodesList());
              }
              if (htd.getPriority() >= HConstants.ADMIN_QOS || region.getTable().isSystemTable()) {
                regionServer.executorService.submit(new OpenPriorityRegionHandler(regionServer,
                  regionServer, region, htd, masterSystemTime));
              } else {
                regionServer.executorService.submit(
                  new OpenRegionHandler(regionServer, regionServer, region, htd, masterSystemTime));
              }
            }
          }
        }

        builder.addOpeningState(RegionOpeningState.OPENED);
      } catch (IOException ie) {
        LOG.warn("Failed opening region " + region.getRegionNameAsString(), ie);
        if (isBulkAssign) {
          builder.addOpeningState(RegionOpeningState.FAILED_OPENING);
        } else {
          throw new ServiceException(ie);
        }
      }
    }
    return builder.build();
  }

  /**
   * Warmup a region on this server. This method should only be called by Master. It synchronously
   * opens the region and closes the region bringing the most important pages in cache.
   */
  @Override
  public WarmupRegionResponse warmupRegion(final RpcController controller,
    final WarmupRegionRequest request) throws ServiceException {
    final RegionInfo region = ProtobufUtil.toRegionInfo(request.getRegionInfo());
    WarmupRegionResponse response = WarmupRegionResponse.getDefaultInstance();
    try {
      checkOpen();
      String encodedName = region.getEncodedName();
      byte[] encodedNameBytes = region.getEncodedNameAsBytes();
      final HRegion onlineRegion = regionServer.getRegion(encodedName);
      if (onlineRegion != null) {
        LOG.info("{} is online; skipping warmup", region);
        return response;
      }
      TableDescriptor htd = regionServer.tableDescriptors.get(region.getTable());
      if (regionServer.getRegionsInTransitionInRS().containsKey(encodedNameBytes)) {
        LOG.info("{} is in transition; skipping warmup", region);
        return response;
      }
      LOG.info("Warmup {}", region.getRegionNameAsString());
      HRegion.warmupHRegion(region, htd, regionServer.getWAL(region),
        regionServer.getConfiguration(), regionServer, null);
    } catch (IOException ie) {
      LOG.error("Failed warmup of {}", region.getRegionNameAsString(), ie);
      throw new ServiceException(ie);
    }

    return response;
  }

  /**
   * Replay the given changes when distributedLogReplay WAL edits from a failed RS. The guarantee is
   * that the given mutations will be durable on the receiving RS if this method returns without any
   * exception.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.REPLAY_QOS)
  public ReplicateWALEntryResponse replay(final RpcController controller,
    final ReplicateWALEntryRequest request) throws ServiceException {
    long before = EnvironmentEdgeManager.currentTime();
    CellScanner cells = ((HBaseRpcController) controller).cellScanner();
    ((HBaseRpcController) controller).setCellScanner(null);
    try {
if(KnobRuntime.check(java.util.UUID.fromString("bd6b500c-7c51-3868-817d-ca1c9ad6586e"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("c6200d69-0ede-31b7-bfec-2e79e71a8d81"))) { closeAllScanners(); } else { checkOpen(); }
      List<WALEntry> entries = request.getEntryList();
      if (((KnobRuntime.check(java.util.UUID.fromString("28eff42d-b500-3235-b6bb-10f5fbe39cc7"))) ? (((entries) != (null)) || (entries.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("df8531cf-32ad-310d-a328-92e5346f2339"))) ? (((entries) == (null)) || (entries.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("722dbb23-2d26-33ee-aa06-5dc52375237b"))) ? (entries == null) : (((KnobRuntime.check(java.util.UUID.fromString("6b60f002-d1bf-3d30-abfb-64364539de18"))) ? (((entries) == (null)) && (entries.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("8d06c777-ac43-3f2c-b763-fba1b393b986"))) ? (((entries) != (null)) && (entries.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("1aee9472-d85a-3b30-9fea-03995cc35539"))) ? ((entries == null) && (entries.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("aa891d42-ea49-341a-966c-9a4eaeb9379b"))) ? ((entries == null) || (entries.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("bd0d52d2-a037-35ff-a404-d10c19333187"))) ? (entries.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("f10f2663-9e4d-365c-98d9-c65c6675786d"))) ? ((entries) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a873434a-72a5-39db-80ca-f07b5021671d"))) ? ((entries) == (null)) : (entries == null || entries.isEmpty()))))))))))))))))))))) {
        // empty input
        return ReplicateWALEntryResponse.newBuilder().build();
      }
      ByteString regionName = entries.get(0).getKey().getEncodedRegionName();
      HRegion region = regionServer.getRegionByEncodedName(regionName.toStringUtf8());
      RegionCoprocessorHost coprocessorHost =
        ServerRegionReplicaUtil.isDefaultReplica(region.getRegionInfo())
          ? region.getCoprocessorHost()
          : null; // do not invoke coprocessors if this is a secondary region replica
      List<Pair<WALKey, WALEdit>> walEntries = new ArrayList<>();

      // Skip adding the edits to WAL if this is a secondary region replica
      boolean isPrimary = RegionReplicaUtil.isDefaultReplica(region.getRegionInfo());
      Durability durability = isPrimary ? Durability.USE_DEFAULT : Durability.SKIP_WAL;

      for (WALEntry entry : entries) {
        if (!regionName.equals(entry.getKey().getEncodedRegionName())) {
          throw new NotServingRegionException("Replay request contains entries from multiple "
            + "regions. First region:" + regionName.toStringUtf8() + " , other region:"
            + entry.getKey().getEncodedRegionName());
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("31f8fa42-9443-3e24-84cd-e885d7a03898"))) ? ((regionServer.nonceManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("07686990-0d36-3057-bdc5-1a5bceff2ec7"))) ? (((regionServer.nonceManager) == (null)) || (!isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("c7625dc9-cdf3-32eb-9ba7-c152f5eaf772"))) ? (((regionServer.nonceManager) != (null)) && (!isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("f01e1364-6d33-3a81-8f3b-da609ccc0110"))) ? ((regionServer.nonceManager) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3e33e86a-0aef-3fcd-9a83-917722e8be01"))) ? (regionServer.nonceManager != null) : (((KnobRuntime.check(java.util.UUID.fromString("e9f0b02c-8414-3365-b784-98780d0b28d0"))) ? (((regionServer.nonceManager) != (null)) || (!isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("2caf4dbe-7dd7-30e0-84b3-e2e41352e9cd"))) ? (isPrimary) : (((KnobRuntime.check(java.util.UUID.fromString("05f929a5-8227-3b24-93ae-f2a24989ea0f"))) ? (((regionServer.nonceManager) == (null)) && (!isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("b0e8631a-87cf-307f-a8da-77331d92476a"))) ? (((regionServer.nonceManager) != (null)) && (isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("ad8a8880-8328-3590-885a-6e10e0f9c05f"))) ? ((regionServer.nonceManager != null) && (!isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("2c6bcd61-1f8e-387e-aeb1-fbee08458fed"))) ? (!isPrimary) : (((KnobRuntime.check(java.util.UUID.fromString("70f28062-efb3-3a5c-a3f7-ea5c2dcf77d3"))) ? (((regionServer.nonceManager) != (null)) || (isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("3be2834c-efd6-3ef9-8d53-2409236b3ce8"))) ? ((regionServer.nonceManager != null) && (isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("1c7c5835-c42f-333a-8306-c42f4743dd85"))) ? (((regionServer.nonceManager) == (null)) || (isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("1f3919d1-0edf-3d7b-8e6f-96568b04e84a"))) ? ((regionServer.nonceManager != null) || (isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("5c69c68a-bfce-3c22-9786-56e33ce0810a"))) ? (((regionServer.nonceManager) == (null)) && (isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("e173c8a3-7f36-37fa-867d-ff7070f31518"))) ? ((regionServer.nonceManager != null) || (!isPrimary)) : (regionServer.nonceManager != null && isPrimary))))))))))))))))))))))))))))))))))) {
          long nonceGroup =
            entry.getKey().hasNonceGroup() ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
if(KnobRuntime.check(java.util.UUID.fromString("4c60ec1b-cc4b-30c3-a514-f0d95ba2bb6e"))) {
nonceGroup = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("126c1f70-2953-3f6a-8ba4-cfaf26e77ead"))) {
nonceGroup = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("15b6156a-5ead-3123-a772-020298db528a"))) {
nonce = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b25b5b17-2e3c-30b6-82ab-48c2fd720f89"))) {
nonce = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("93f8d0f3-8819-3473-9cf5-9475e7694504"))) {
nonceGroup += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ad6ad120-3171-36c9-bf5f-ff7d9a0ead54"))) {
nonce += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("61855811-a525-3f7c-b8ec-ae0e74fe0c12"))) {
nonce -= 1;
}
          regionServer.nonceManager.reportOperationFromWal(nonceGroup, nonce,
            entry.getKey().getWriteTime());
        }
        Pair<WALKey, WALEdit> walEntry = (coprocessorHost == null) ? null : new Pair<>();
        List<MutationReplay> edits =
          WALSplitUtil.getMutationsFromWALEntry(entry, cells, walEntry, durability);
        if (coprocessorHost != null) {
          // Start coprocessor replay here. The coprocessor is for each WALEdit instead of a
          // KeyValue.
          if (
            coprocessorHost.preWALRestore(region.getRegionInfo(), walEntry.getFirst(),
              walEntry.getSecond())
          ) {
            // if bypass this log entry, ignore it ...
            continue;
          }
          walEntries.add(walEntry);
        }
        if (edits != null && !edits.isEmpty()) {
          // HBASE-17924
          // sort to improve lock efficiency
          Collections.sort(edits, (v1, v2) -> Row.COMPARATOR.compare(v1.mutation, v2.mutation));
          long replaySeqId = (entry.getKey().hasOrigSequenceNumber())
            ? entry.getKey().getOrigSequenceNumber()
            : entry.getKey().getLogSequenceNumber();
          OperationStatus[] result = doReplayBatchOp(region, edits, replaySeqId);
          // check if it's a partial success
          for (int i = 0; result != null && i < result.length; i++) {
            if (result[i] != OperationStatus.SUCCESS) {
              throw new IOException(result[i].getExceptionMsg());
            }
          }
        }
      }

      // sync wal at the end because ASYNC_WAL is used above
      WAL wal = region.getWAL();
      if (wal != null) {
        wal.sync();
      }

      if (coprocessorHost != null) {
        for (Pair<WALKey, WALEdit> entry : walEntries) {
          coprocessorHost.postWALRestore(region.getRegionInfo(), entry.getFirst(),
            entry.getSecond());
        }
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
      if (metricsRegionServer != null) {
        metricsRegionServer.updateReplay(EnvironmentEdgeManager.currentTime() - before);
      }
    }
  }

  /**
   * Replicate WAL entries on the region server.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.REPLICATION_QOS)
  public ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
    final ReplicateWALEntryRequest request) throws ServiceException {
    try {
      checkOpen();
      if (regionServer.getReplicationSinkService() != null) {
        requestCount.increment();
        List<WALEntry> entries = request.getEntryList();
        CellScanner cellScanner = ((HBaseRpcController) controller).cellScanner();
        ((HBaseRpcController) controller).setCellScanner(null);
        regionServer.getRegionServerCoprocessorHost().preReplicateLogEntries();
        regionServer.getReplicationSinkService().replicateLogEntries(entries, cellScanner,
          request.getReplicationClusterId(), request.getSourceBaseNamespaceDirPath(),
          request.getSourceHFileArchiveDirPath());
        regionServer.getRegionServerCoprocessorHost().postReplicateLogEntries();
        return ReplicateWALEntryResponse.newBuilder().build();
      } else {
        throw new ServiceException("Replication services are not initialized yet");
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Roll the WAL writer of the region server.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  public RollWALWriterResponse rollWALWriter(final RpcController controller,
    final RollWALWriterRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      regionServer.getRegionServerCoprocessorHost().preRollWALWriterRequest();
      regionServer.getWalRoller().requestRollAll();
      regionServer.getRegionServerCoprocessorHost().postRollWALWriterRequest();
      RollWALWriterResponse.Builder builder = RollWALWriterResponse.newBuilder();
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Stop the region server.
   * @param controller the RPC controller
   * @param request    the request
   */
  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public StopServerResponse stopServer(final RpcController controller,
    final StopServerRequest request) throws ServiceException {
    rpcPreCheck("stopServer");
    requestCount.increment();
    String reason = request.getReason();
    regionServer.stop(reason);
    return StopServerResponse.newBuilder().build();
  }

  @Override
  public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
    UpdateFavoredNodesRequest request) throws ServiceException {
    rpcPreCheck("updateFavoredNodes");
    List<UpdateFavoredNodesRequest.RegionUpdateInfo> openInfoList = request.getUpdateInfoList();
    UpdateFavoredNodesResponse.Builder respBuilder = UpdateFavoredNodesResponse.newBuilder();
    for (UpdateFavoredNodesRequest.RegionUpdateInfo regionUpdateInfo : openInfoList) {
      RegionInfo hri = ProtobufUtil.toRegionInfo(regionUpdateInfo.getRegion());
      if (regionUpdateInfo.getFavoredNodesCount() > 0) {
        regionServer.updateRegionFavoredNodesMapping(hri.getEncodedName(),
          regionUpdateInfo.getFavoredNodesList());
      }
    }
    respBuilder.setResponse(openInfoList.size());
    return respBuilder.build();
  }

  /**
   * Atomically bulk load several HFiles into an open region
   * @return true if successful, false is failed but recoverably (no action)
   * @throws ServiceException if failed unrecoverably
   */
  @Override
  public BulkLoadHFileResponse bulkLoadHFile(final RpcController controller,
    final BulkLoadHFileRequest request) throws ServiceException {
    long start = EnvironmentEdgeManager.currentTime();
    List<String> clusterIds = new ArrayList<>(request.getClusterIdsList());
    if (clusterIds.contains(this.regionServer.clusterId)) {
      return BulkLoadHFileResponse.newBuilder().setLoaded(true).build();
    } else {
      clusterIds.add(this.regionServer.clusterId);
    }
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      Map<byte[], List<Path>> map = null;
      final boolean spaceQuotaEnabled = QuotaUtil.isQuotaEnabled(getConfiguration());
      long sizeToBeLoaded = -1;

      // Check to see if this bulk load would exceed the space quota for this table
      if (spaceQuotaEnabled) {
        ActivePolicyEnforcement activeSpaceQuotas = getSpaceQuotaManager().getActiveEnforcements();
        SpaceViolationPolicyEnforcement enforcement =
          activeSpaceQuotas.getPolicyEnforcement(region);
        if (enforcement != null) {
          // Bulk loads must still be atomic. We must enact all or none.
          List<String> filePaths = new ArrayList<>(request.getFamilyPathCount());
          for (FamilyPath familyPath : request.getFamilyPathList()) {
            if (KnobRuntime.check(java.util.UUID.fromString("39c75cb8-d852-3131-ade4-c8f266010d98"))) { filePaths.add(getUserName()); } else if (KnobRuntime.check(java.util.UUID.fromString("63ce92f0-708d-3941-a851-3185b48e8f89"))) { filePaths.add(getRemoteClientIpAndPort()); } else { filePaths.add(familyPath.getPath()); }
          }
          // Check if the batch of files exceeds the current quota
if(KnobRuntime.check(java.util.UUID.fromString("40c43c8e-ae1b-312e-8e15-473639ece05e"))) {
throw new java.io.IOException("Injected exception");
}
          sizeToBeLoaded = enforcement.computeBulkLoadSize(getFileSystem(filePaths), filePaths);
        }
      }

      List<Pair<byte[], String>> familyPaths = new ArrayList<>(request.getFamilyPathCount());
      for (FamilyPath familyPath : request.getFamilyPathList()) {
        familyPaths.add(new Pair<>(familyPath.getFamily().toByteArray(), familyPath.getPath()));
      }
      if (!request.hasBulkToken()) {
        if (region.getCoprocessorHost() != null) {
          region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
        }
        try {
          map = region.bulkLoadHFiles(familyPaths, request.getAssignSeqNum(), null,
            request.getCopyFile(), clusterIds, request.getReplicate());
        } finally {
          if (region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().postBulkLoadHFile(familyPaths, map);
          }
        }
      } else {
        // secure bulk load
        map =
          regionServer.getSecureBulkLoadManager().secureBulkLoadHFiles(region, request, clusterIds);
      }
      BulkLoadHFileResponse.Builder builder = BulkLoadHFileResponse.newBuilder();
      builder.setLoaded(map != null);
      if (map != null) {
        // Treat any negative size as a flag to "ignore" updating the region size as that is
        // not possible to occur in real life (cannot bulk load a file with negative size)
        if (((KnobRuntime.check(java.util.UUID.fromString("980e553c-b2ca-3e2a-bd34-f4fabd5baaa3"))) ? ((spaceQuotaEnabled) || ((sizeToBeLoaded) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("15b11166-95d7-3f74-bac6-67c45b737eb7"))) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("481a8f50-2317-33d1-80de-f70cf8345484"))) ? ((spaceQuotaEnabled) || ((sizeToBeLoaded) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("54de5671-a351-30e1-8652-dc24fadf97bd"))) ? ((spaceQuotaEnabled) && ((sizeToBeLoaded) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ace5a367-ca38-3cb6-b5f5-45a89334e31a"))) ? ((sizeToBeLoaded) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("40fcd490-9964-3b58-8d73-66ac33550541"))) ? ((sizeToBeLoaded) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f95e4a28-fc3b-3434-92e9-c86b02a1bc50"))) ? ((spaceQuotaEnabled) && ((sizeToBeLoaded) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a19817c5-2c1c-344a-8a34-cd2965d3261a"))) ? ((spaceQuotaEnabled) && ((sizeToBeLoaded) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e566024c-a9e5-307b-a7e0-f239e5b813d9"))) ? ((spaceQuotaEnabled) && (sizeToBeLoaded > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("05e6a4ec-4eb3-3711-ace9-add6ae6126c9"))) ? ((!spaceQuotaEnabled) && ((sizeToBeLoaded) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("87017f47-c2e6-3097-8536-b9f55b10bf50"))) ? ((!spaceQuotaEnabled) && ((sizeToBeLoaded) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("70d35e2a-05de-3a0a-afb6-c973b106a7c2"))) ? ((spaceQuotaEnabled) || ((sizeToBeLoaded) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca30b1cf-018d-36e2-b238-eebe92d807b6"))) ? ((spaceQuotaEnabled) && ((sizeToBeLoaded) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8cd08401-75bc-3f22-a67a-90f9dd9ab528"))) ? ((!spaceQuotaEnabled) && ((sizeToBeLoaded) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6b0d1e9f-9328-3fc5-b819-266571550eb8"))) ? ((spaceQuotaEnabled) || ((sizeToBeLoaded) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ed7600e3-2f1d-3af4-b822-fba7486eb957"))) ? (!spaceQuotaEnabled) : (((KnobRuntime.check(java.util.UUID.fromString("808b8030-4afb-315c-a1f7-eb19eed2c0f4"))) ? ((!spaceQuotaEnabled) && ((sizeToBeLoaded) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a849c40c-5a14-3c07-a44e-66f22418a763"))) ? ((!spaceQuotaEnabled) || (sizeToBeLoaded > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8931ba71-17ed-35f8-aeca-decfdcd43523"))) ? (sizeToBeLoaded > 0) : (((KnobRuntime.check(java.util.UUID.fromString("ac8fe172-7c1c-3cab-ae41-f7fede2fb135"))) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("398453de-ba1a-32dd-8946-8ec23c2cde02"))) ? ((!spaceQuotaEnabled) && ((sizeToBeLoaded) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02eca3f8-42ce-38ee-ae0c-ea4abaddd1d6"))) ? ((spaceQuotaEnabled) || (sizeToBeLoaded > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6573fcc5-3ade-3aee-a836-3a596724f7f6"))) ? (spaceQuotaEnabled) : (((KnobRuntime.check(java.util.UUID.fromString("6d47016d-d419-32de-b0e1-e76a0844e3b6"))) ? ((sizeToBeLoaded) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e9369a41-2159-3079-8e83-08de1d8c4d1e"))) ? ((sizeToBeLoaded) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("332c3767-57ec-380e-af97-2cdd4e7136b9"))) ? ((spaceQuotaEnabled) && ((sizeToBeLoaded) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aff44ed4-fea7-37cc-ac90-774e7efeecce"))) ? ((spaceQuotaEnabled) && ((sizeToBeLoaded) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("26cf0aad-4690-3c73-898b-22fa2edb101b"))) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8a5b0eb8-1de8-391c-8d47-7a9846753214"))) ? ((spaceQuotaEnabled) || ((sizeToBeLoaded) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("edccae4b-8c41-3eaa-a6e9-545bd4ba7e13"))) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1b5970e3-815c-361a-915a-e8d71b4feea0"))) ? ((!spaceQuotaEnabled) && (sizeToBeLoaded > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("161d2150-d6b6-34b2-bf23-6e7a5dc9e0f1"))) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("581d0c6a-3193-3331-a183-72794fc96abb"))) ? ((sizeToBeLoaded) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9ad7bb3a-2dbf-3daf-a20f-5e34222a8cd5"))) ? ((spaceQuotaEnabled) || ((sizeToBeLoaded) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b6f043e0-76d8-33fd-8e98-6e38b960f356"))) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("13291602-0415-334c-9382-99bbbd1a45ad"))) ? ((!spaceQuotaEnabled) && ((sizeToBeLoaded) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("94664657-1a5b-3767-98e1-1c19ec9f139c"))) ? ((sizeToBeLoaded) < (0)) : (spaceQuotaEnabled && sizeToBeLoaded > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          if (((KnobRuntime.check(java.util.UUID.fromString("8202d14b-2995-3078-bd70-f8a5d98978ab"))) ? (LOG.isDebugEnabled()) : (LOG.isTraceEnabled()))) {
            LOG.trace("Incrementing space use of " + region.getRegionInfo() + " by "
              + sizeToBeLoaded + " bytes");
          }
          // Inform space quotas of the new files for this region
          getSpaceQuotaManager().getRegionSizeStore().incrementRegionSize(region.getRegionInfo(),
            sizeToBeLoaded);
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
      if (metricsRegionServer != null) {
        metricsRegionServer.updateBulkLoad(EnvironmentEdgeManager.currentTime() - start);
      }
    }
  }

  @Override
  public PrepareBulkLoadResponse prepareBulkLoad(RpcController controller,
    PrepareBulkLoadRequest request) throws ServiceException {
if(KnobRuntime.check(java.util.UUID.fromString("63395fbc-1018-385a-80f6-f8d2ad098250"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dda84c66-e5fa-329c-b8fa-f1ba402e52fd"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5191002e-2b97-3a0a-8767-e21a5070fb01"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49bdd6ee-171b-3324-88b9-17b9b3409444"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d76f3d4-c717-3d30-a727-197356c101a1"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    try {
if(KnobRuntime.check(java.util.UUID.fromString("f18d2e43-01c4-375f-a579-8f7f7ea56ccc"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("987a1cc8-c6ec-3605-87f3-20084bfffaa3"))) { closeAllScanners(); } else { checkOpen(); }
      if (KnobRuntime.check(java.util.UUID.fromString("59f46f20-6cf2-352e-97ac-2a3998809871"))) { closeAllScanners(); } else if (KnobRuntime.check(java.util.UUID.fromString("7543856c-db2f-3951-b609-73d1257d7f15"))) { checkOpen(); } else { requestCount.increment(); }

if(KnobRuntime.check(java.util.UUID.fromString("55bff940-a69b-3202-b66d-15e4fdcf486f"))) {
throw new java.io.IOException("Injected exception");
}
      HRegion region = getRegion(request.getRegion());

if(KnobRuntime.check(java.util.UUID.fromString("8fce1579-a0e2-3e96-8572-e8398fb07725"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5e0ce133-3eeb-3d0e-b46f-c3ae93cf0632"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9dadefcc-2ffb-389e-93fe-b6a2a8b65d6e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b086e7bd-df79-3684-9115-f7d63ddc680a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba4a183d-80d3-38f0-acfd-882a0845fb5a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("108edc2c-54dd-3770-adcc-4d48bd384f25"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4833a77-1b40-388a-bc88-23634b5e675f"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7662a57-9061-3592-9b38-812ed5b46d09"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("194af0c1-ffed-30c9-b265-9cba87ec25c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5baab0e8-5561-3439-b971-288436f59a09"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e6f10c8c-eb16-3abb-94f3-196137e4c530"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bd4ff8f6-add9-3096-befa-2136ad19b1e6"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2b35a696-72ed-3eff-86a7-941fe192b398"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef0a4780-47f4-3619-b559-d686beb9a68f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d1b8b8e4-8a7d-3ef9-9d0d-e8805ec4f1a0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("da225d0b-da29-3450-a503-a6eaedc7ffd9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("43412f4a-5492-3b43-b732-669e4ccf7342"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8cc68a23-135c-326f-8f11-0443313e0916"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b2ca9f9-88b7-3cbe-9125-02d8d255618c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1edef277-7733-387b-81af-a062d8318540"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a66a6e51-5a72-31bf-948a-e310327ff8c0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3451d054-31d7-3c0c-998d-7c77a8ed5c73"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2671924a-ac0d-3752-90ec-24233f233170"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02f0370c-c602-3e94-84fd-7f6e8233a51a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f506717c-b6da-364c-93bc-77875a339a15"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a9284d3d-36a6-3558-b123-3b709497d22d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bacf27cf-18ad-3827-947a-771a5d174a91"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("86dfbba2-1d7d-30c4-ba33-5dd9b39f6871"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2cb9ae0a-d059-3293-ae22-2cb412783443"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82c90799-15f1-3024-b793-f6c4097ca00b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("384d6604-ea0d-3ca8-a4c0-05a3311e7579"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b787733c-6bca-3dfa-94d2-3f447b9be4c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b03dabf4-3c3e-3690-bdb3-dd6975541d91"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("919182b6-149e-385e-8923-8aa9d3ec943f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ab654980-200d-3c30-b22f-9c9a778f6bcf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("994520a0-38d0-34ea-a246-9d7d2ff05daa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d6489cae-f66d-3cd5-8277-1f80f7635e38"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ce4a7eb-384b-3ccf-baf2-772380bed324"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9008bad0-6cfe-3e0c-954b-ebe23c2887bb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2c3d2002-9ac9-3e67-ac0d-e3f8f9c59b27"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8897b530-1ad1-3dba-9568-4e6fe4f4d3fe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4302cca0-4e6a-32c1-b25b-6d6c168fff4e"))) {
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
      String bulkToken = regionServer.getSecureBulkLoadManager().prepareBulkLoad(region, request);
      PrepareBulkLoadResponse.Builder builder = PrepareBulkLoadResponse.newBuilder();
      builder.setBulkToken(bulkToken);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CleanupBulkLoadResponse cleanupBulkLoad(RpcController controller,
    CleanupBulkLoadRequest request) throws ServiceException {
if(KnobRuntime.check(java.util.UUID.fromString("f869b287-b5ca-3bfa-80f5-449230cbacd5"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45ae7ffe-d00f-3621-b582-7be1ab72cf84"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("85bef33a-e551-3a5b-a9ce-0dd51a19a3ad"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3d6078c-075a-3807-b2cb-e3d170d0de59"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3ec684c-c178-3c41-afb1-d4067d61ecc2"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    try {
if(KnobRuntime.check(java.util.UUID.fromString("70a64beb-3fed-3846-abf6-2f83ddef3490"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("6977ea7b-1d79-32ce-a438-e6631bd8c569"))) { closeAllScanners(); } else { checkOpen(); }
      requestCount.increment();

if(KnobRuntime.check(java.util.UUID.fromString("befa9ee3-8bc7-3d22-8d00-248e083013c8"))) {
throw new java.io.IOException("Injected exception");
}
      HRegion region = getRegion(request.getRegion());

      regionServer.getSecureBulkLoadManager().cleanupBulkLoad(region, request);
      return CleanupBulkLoadResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CoprocessorServiceResponse execService(final RpcController controller,
    final CoprocessorServiceRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      com.google.protobuf.Message result = execServiceOnRegion(region, request.getCall());
      CoprocessorServiceResponse.Builder builder = CoprocessorServiceResponse.newBuilder();
      builder.setRegion(RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME,
        region.getRegionInfo().getRegionName()));
      // TODO: COPIES!!!!!!
      builder.setValue(builder.getValueBuilder().setName(result.getClass().getName()).setValue(
        org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(result.toByteArray())));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private FileSystem getFileSystem(List<String> filePaths) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8950e0de-8238-3318-9336-e2cf0a34d53d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("96107903-1ff0-3b87-b749-11237d01a317"))) {
throw new java.io.IOException("Injected exception");
}
    if (filePaths.isEmpty()) {
      // local hdfs
      return regionServer.getFileSystem();
    }
    // source hdfs
if(KnobRuntime.check(java.util.UUID.fromString("c390c03f-5752-3527-af1d-1dac773c6fe6"))) {
throw new java.io.IOException("Injected exception");
}
    return ((KnobRuntime.check(java.util.UUID.fromString("b7fa61c8-f2ca-354f-b5ce-ca39cd6900f0"))) ? (new Path(filePaths.get(0)).getFileSystem(getConfiguration())) : (new Path(filePaths.get(0)).getFileSystem(regionServer.getConfiguration())));
  }

  private com.google.protobuf.Message execServiceOnRegion(HRegion region,
    final ClientProtos.CoprocessorServiceCall serviceCall) throws IOException {
    // ignore the passed in controller (from the serialized call)
    ServerRpcController execController = new ServerRpcController();
    return region.execService(execController, serviceCall);
  }

  /**
   * Get data from a table.
   * @param controller the RPC controller
   * @param request    the get request
   */
  @Override
  public GetResponse get(final RpcController controller, final GetRequest request)
    throws ServiceException {
    long before = EnvironmentEdgeManager.currentTime();
    OperationQuota quota = null;
    HRegion region = null;
    RpcCallContext context = RpcServer.getCurrentCall().orElse(null);
    try {
      checkOpen();
      if (KnobRuntime.check(java.util.UUID.fromString("15146973-2d85-3ae3-b87e-0d67328cf85d"))) { closeAllScanners(); } else if (KnobRuntime.check(java.util.UUID.fromString("4ce8c66e-bd53-3079-8266-e1fe1bdc67c6"))) { checkOpen(); } else { requestCount.increment(); }
      if (KnobRuntime.check(java.util.UUID.fromString("a522f055-9c3f-3a26-aa5f-6fdcfa64d529"))) { closeAllScanners(); } else if (KnobRuntime.check(java.util.UUID.fromString("a5fc54bd-85f8-3c7f-9a47-e9a5d330014a"))) { checkOpen(); } else { rpcGetRequestCount.increment(); }
      region = getRegion(request.getRegion());

      GetResponse.Builder builder = GetResponse.newBuilder();
      ClientProtos.Get get = request.getGet();
      // An asynchbase client, https://github.com/OpenTSDB/asynchbase, starts by trying to do
      // a get closest before. Throwing the UnknownProtocolException signals it that it needs
      // to switch and do hbase2 protocol (HBase servers do not tell clients what versions
      // they are; its a problem for non-native clients like asynchbase. HBASE-20225.
      if (get.hasClosestRowBefore() && get.getClosestRowBefore()) {
        throw new UnknownProtocolException("Is this a pre-hbase-1.0.0 or asynchbase client? "
          + "Client is invoking getClosestRowBefore removed in hbase-2.0.0 replaced by "
          + "reverse Scan.");
      }
      Boolean existence = null;
      Result r = null;
if(KnobRuntime.check(java.util.UUID.fromString("2719d0d3-fb2b-3193-b974-57c9852f1f2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("863bdbf7-000a-39d7-b207-57aec4de3cbd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8728aae7-7ca2-3103-b05c-e5126decf351"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("767910fe-0d35-3da1-a5d2-7203230bff28"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("10e6c1b2-157c-3ae3-b45a-ac312f8046ea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e7683534-e2fc-3ce0-abf1-2c00d52a9ad6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("252606ea-3896-342c-9c6e-2387ff4aa8f9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7bd12d76-9eff-3fde-9fee-01584b1c078d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7befaa26-983a-3bcf-897a-088644b021a3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f6147140-9676-3356-bcf7-902131a198c7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36372f80-e466-3b84-b004-719f3d21d117"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ce56228-c591-32c4-92ec-0f5c468c81d7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b73747c2-a8e2-377e-a545-16e1d32d34e1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f034443a-0c03-3b2c-ba91-27f63d13aa6c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("51e793e5-461e-3071-8aa6-03e6a8eacbcf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0eb0022f-77ef-303d-992b-af2a8568e5bc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fe6afda1-36be-37aa-b7ac-f47127fc23d6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fcf5100a-fc68-3b56-8aef-06d202ea0273"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c60b35c7-07fe-32b0-be09-5e809a584f9a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e75b5327-6b93-3df0-88ad-e972a0d65e90"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b21a060b-90d2-3e97-8876-d5f7be1e71d0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bf02d17d-21aa-34da-8278-6d06bb1a0fb9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bd0a3f26-d71d-3188-9f9b-75028f36b313"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1b243f77-3e5a-3724-b762-a0b37241d960"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d45ac201-10d1-3778-b148-83975900a853"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bfb79b7d-b70a-3a22-9027-84fc01595bff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc92c3e8-6b4e-3b74-a3e0-2aea31472eaa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("db1481e4-673c-3304-8356-d6bfd919cd13"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3a3ca2f5-a49a-3f77-8cbb-5eea11aaffd1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5267ef6f-8a91-36b8-bafa-a043fff6005f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("150f7185-ce7a-34a4-978a-83e51f34add0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2dfacd5c-d3d7-399a-9583-f1287345310a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f911f00b-18f1-3f57-b8ea-0ee7412889c7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9f5d2f8a-3f43-3cef-addf-8b7a957f483b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c97d296c-4108-314c-adbe-8bee190778f3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d692eccb-b95a-3c8d-9f8b-1fed61303709"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6d1e32c4-d74d-3fa9-b884-d7fe520d9445"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9fd00632-c32d-3298-a4e6-01b75de7ead5"))) {
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
      quota = getRpcQuotaManager().checkBatchQuota(region, OperationQuota.OperationType.GET);

      Get clientGet = ProtobufUtil.toGet(get);
      if (((KnobRuntime.check(java.util.UUID.fromString("cce20e09-9f45-3ab4-a4ca-24bca66c2f2b"))) ? ((get.getClosestRowBefore()) || ((region.getCoprocessorHost()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2f7a22cc-314b-3858-854e-c48883931749"))) ? ((get.getClosestRowBefore()) && ((region.getCoprocessorHost()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5c19b166-86ea-31f9-beb5-c94dd677ca75"))) ? ((get.getClosestRowBefore()) && (region.getCoprocessorHost() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("dbf561b3-36ce-3667-a281-d9095642e2f3"))) ? ((get.getExistenceOnly()) && ((region.getCoprocessorHost()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e99ee88b-262f-3b27-af53-9a38e670fbdb"))) ? ((get.getExistenceOnly()) && (region.getCoprocessorHost() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("466761ef-a56b-35d6-9088-0b8229180751"))) ? ((get.getClosestRowBefore()) || (region.getCoprocessorHost() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("518218ab-e31e-3fc1-9dba-ca3f337e6e68"))) ? ((get.getExistenceOnly()) || (region.getCoprocessorHost() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("350ae1dc-c00f-34bf-a4cf-7b28aaab9d04"))) ? ((get.hasClosestRowBefore()) || ((region.getCoprocessorHost()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("89f5bf1f-ea65-3269-89c8-3812d3266acf"))) ? (get.hasClosestRowBefore()) : (((KnobRuntime.check(java.util.UUID.fromString("3f224a6f-88a8-3c1c-a9a8-766c98d54f65"))) ? ((get.hasClosestRowBefore()) || (region.getCoprocessorHost() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("bc0a9348-6356-3def-87ac-68cd5364887a"))) ? ((get.hasClosestRowBefore()) && ((region.getCoprocessorHost()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9451180e-1753-37b8-a9d9-61bb9a477015"))) ? ((get.getClosestRowBefore()) && ((region.getCoprocessorHost()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("79e8f49c-9704-3f3d-a769-59ba4f46dcdc"))) ? ((get.hasClosestRowBefore()) || ((region.getCoprocessorHost()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("21fa123f-5f35-3160-9e16-210270c2d379"))) ? ((region.getCoprocessorHost()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5abab7a0-6713-3b21-a0fb-72c94237c214"))) ? ((get.hasClosestRowBefore()) && (region.getCoprocessorHost() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("a1ffa29e-10a2-3c5b-bcf9-2be90100fa7c"))) ? ((get.getClosestRowBefore()) || ((region.getCoprocessorHost()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5037112b-faf7-3aa8-843b-65d87c384a03"))) ? (get.getExistenceOnly()) : (((KnobRuntime.check(java.util.UUID.fromString("182df910-8594-3853-a690-46057a707453"))) ? ((get.getExistenceOnly()) && ((region.getCoprocessorHost()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1183a765-02a9-3fec-a3c5-c78e9e189452"))) ? (get.getClosestRowBefore()) : (((KnobRuntime.check(java.util.UUID.fromString("781c9160-b8b4-3981-b9f3-a96af6def211"))) ? ((get.getExistenceOnly()) || ((region.getCoprocessorHost()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2951dcfa-1ae2-3749-9a6a-fd105c75b906"))) ? ((get.getExistenceOnly()) || ((region.getCoprocessorHost()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("53822f2b-b356-3403-88e2-956099b4bfc2"))) ? ((region.getCoprocessorHost()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b9db9bef-fce4-3c5b-bf79-e0b1b88b195f"))) ? (region.getCoprocessorHost() != null) : (((KnobRuntime.check(java.util.UUID.fromString("98a18794-b8aa-3598-8291-8c46fbe3da5a"))) ? ((get.hasClosestRowBefore()) && ((region.getCoprocessorHost()) != (null))) : (get.getExistenceOnly() && region.getCoprocessorHost() != null))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("9a7e3823-8fb0-3596-be7c-0dfdbd7bcb03"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0487a28-887c-3cb7-8bd0-3bae6c6bdfe1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("87e8fac2-994f-3962-a361-2e6598cde599"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(clientGet);
    field.set(clientGet, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11f4e011-6e1a-367e-9bd4-a53c743ab390"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25fe16d6-651b-3533-bcf9-7d0780c88814"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fdefb4df-6c99-3b77-83d8-fdcc783a8b2a"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dbe6ed4-b023-3570-9e45-077385baa073"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("901244e1-a827-3685-99c2-b15b24d5caed"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6406079-4a20-347b-b6a9-a343bb7e0d25"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c0988f9-5c10-3c95-beba-a4f0ed350248"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("47211975-95cf-3cfe-a670-e32035ac8da7"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b568bc7-32af-3974-8c8f-dcf83169b661"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e4e8b06f-5acb-37dd-8210-abc6c80a9ef3"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c5cdba8-c4d0-308d-b120-61cb8f15b105"))) {
try {
    java.lang.reflect.Field field = clientGet.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(clientGet));
    field.set(clientGet, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        existence = region.getCoprocessorHost().preExists(clientGet);
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("cc3ac0cc-368a-3bf0-9685-1fb1593d726d"))) ? ((existence) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a0bc6cc5-a708-3097-8462-90023a98df85"))) ? ((existence) == (null)) : (existence == null))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("6cc36d5b-fba1-30d2-ae69-c2cf34580f30"))) ? ((context) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("71bfef28-76c2-3006-9922-ed73ebceb505"))) ? ((context) == (null)) : (context != null))))) {
          r = get(clientGet, (region), null, context);
        } else {
          // for test purpose
          r = region.get(clientGet);
        }
        if (get.getExistenceOnly()) {
          boolean exists = r.getExists();
          if (region.getCoprocessorHost() != null) {
            exists = region.getCoprocessorHost().postExists(clientGet, exists);
          }
          existence = exists;
        }
      }
      if (existence != null) {
        ClientProtos.Result pbr =
          ProtobufUtil.toResult(existence, region.getRegionInfo().getReplicaId() != 0);
if(KnobRuntime.check(java.util.UUID.fromString("91243d95-1ef4-3f4b-9da7-ba0d46b6193e"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b506ab5d-da6b-31ef-83cd-d901a49e2264"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66ccf8f5-711d-34a9-8911-01ac1ff80d5d"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9199ab54-1b48-330d-92d2-783f67f331a5"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("381bc23e-43d1-3633-9b13-e446e6e68646"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42cf925b-8c89-37f4-8b39-0011e16db3c6"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b1b3396-1e32-3bce-9491-4dc9820ce5c6"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("925f4f13-8935-3805-861f-14d20eed7083"))) {
try {
    java.lang.reflect.Field field = pbr.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pbr));
    field.set(pbr, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        builder.setResult(pbr);
      } else if (((KnobRuntime.check(java.util.UUID.fromString("798f59f2-222c-306e-9cd5-43c1fd81ca01"))) ? ((r) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cfb5ab62-3be8-39c1-8aae-a5d6792b7fae"))) ? ((r) == (null)) : (r != null))))) {
        ClientProtos.Result pbr;
        if (
          isClientCellBlockSupport(context) && controller instanceof HBaseRpcController
            && VersionInfoUtil.hasMinimumVersion(context.getClientVersionInfo(), 1, 3)
        ) {
if(KnobRuntime.check(java.util.UUID.fromString("531fd3d5-e809-3b0a-a922-0f0f6d9327a6"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("092928ba-6699-3a80-950e-00f4cf1e4e43"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("97141c83-2006-385b-b8b7-a43d70cb7945"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f337e87f-7fbb-357a-86c7-faab2ff8b55d"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          pbr = ProtobufUtil.toResultNoData(r);
          ((HBaseRpcController) controller)
            .setCellScanner(CellUtil.createCellScanner(r.rawCells()));
          addSize(context, r);
        } else {
          pbr = ProtobufUtil.toResult(r);
        }
        builder.setResult(pbr);
      }
      // r.cells is null when an table.exists(get) call
      if (((KnobRuntime.check(java.util.UUID.fromString("fa856315-cf75-3fee-b23f-664724457352"))) ? ((r != null) || ((r.rawCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c7ef0de5-1699-37b8-8171-fca528a08bd9"))) ? (((r) != (null)) && ((r.rawCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0b058b37-faca-39af-8eba-ce5076c951ec"))) ? (((r) != (null)) || (r.rawCells() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("67070bf9-9c87-3889-8f7f-fd83931fc3f8"))) ? ((r != null) || (r.rawCells() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("0cc3f026-ac64-31c8-a6ef-85679c43aceb"))) ? ((r != null) && ((r.rawCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9db37c71-1d06-3473-a0d5-7768db3771e1"))) ? ((r) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b7ff10cd-94ef-39de-9d2e-12f2f7c1a922"))) ? (((r) == (null)) || (r.rawCells() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("1b2497e1-b03c-3f76-95ae-8272698de6ee"))) ? (((r) != (null)) || ((r.rawCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9948ac80-5ec1-3c2b-9497-f3d4371425d5"))) ? (((r) != (null)) && ((r.rawCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0bf1bd70-9673-3a5f-85f7-51bad6d15181"))) ? (((r) != (null)) || ((r.rawCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("12dd622c-9147-3a5a-8ace-cc95141a530f"))) ? (r.rawCells() != null) : (((KnobRuntime.check(java.util.UUID.fromString("36fa4a55-e294-3b3c-9898-b8ef9dd33d8d"))) ? ((r != null) && ((r.rawCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3f5cb133-26cf-3e73-9c60-c39d9055d733"))) ? (((r) != (null)) && (r.rawCells() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("a63bbd15-a92a-3d1c-b334-484ae3d1b18c"))) ? (((r) == (null)) && ((r.rawCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bd1b2344-a9a8-3ac5-84f6-d6f0aee04c2d"))) ? ((r.rawCells()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("547f554c-dfbf-33c3-b236-7ef188622045"))) ? ((r) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("418e3c2b-4048-365a-a262-53770a98ca71"))) ? (((r) == (null)) && (r.rawCells() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("ba94bc17-14cc-353d-9ef7-60bd48d934c0"))) ? ((r != null) && (r.rawCells() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("3bfb1c19-4c5b-34e9-aad0-423103d03b62"))) ? (((r) == (null)) && ((r.rawCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0d79eba3-e4af-3304-9b99-99062908fea1"))) ? ((r != null) || ((r.rawCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1c299fc6-a8b0-3f06-88f0-d839aaf59943"))) ? (((r) == (null)) || ((r.rawCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("091089bc-e6e6-3288-ad81-f7edc6b46cc8"))) ? ((r.rawCells()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1cbbe877-a7e5-354c-9e47-8416c6c313ca"))) ? (r != null) : (((KnobRuntime.check(java.util.UUID.fromString("dd9742d4-28f5-3b5a-8021-002e3e2af5f2"))) ? (((r) == (null)) || ((r.rawCells()) != (null))) : (r != null && r.rawCells() != null))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("da21460e-8605-307c-88e0-13016adfa180"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f05e17c4-3cea-307c-a0cd-5a42f4268f00"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e444b089-1813-3da7-9a5e-3fb5985d10c6"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe365ecd-069c-31e1-93c1-794a1bbab1b1"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        quota.addGetResult(r);
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
      if (metricsRegionServer != null && region != null) {
        long blockBytesScanned = context != null ? context.getBlockBytesScanned() : 0;
        metricsRegionServer.updateGet(region, EnvironmentEdgeManager.currentTime() - before,
          blockBytesScanned);
      }
      if (quota != null) {
        quota.close();
      }
    }
  }

  private Result get(Get get, HRegion region, RegionScannersCloseCallBack closeCallBack,
    RpcCallContext context) throws IOException {
    region.prepareGet(get);
    boolean stale = region.getRegionInfo().getReplicaId() != 0;

    // This method is almost the same as HRegion#get.
    List<Cell> results = new ArrayList<>();
    long before = EnvironmentEdgeManager.currentTime();
    // pre-get CP hook
    if (region.getCoprocessorHost() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("d907917e-bf62-3b5c-8f4a-924e1885a6ae"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63a827bc-488e-3ebf-a9e9-3e36f2e2de8a"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e72aca2-f299-3888-922b-b4f5cdcb4006"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("73047830-faeb-33ba-9ff2-8b23602364c1"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b23d6eaa-89a5-3af7-b903-7273dab6995d"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ccd31cb3-e42a-3fd8-a9a7-a22c0857e7f5"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7eb7c478-a167-3048-a6fd-7ef80303d82a"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0436795f-577a-3da7-a98b-a280a4e6ef99"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2a6f89b-ea7d-3ee3-b2ac-2994c9acab92"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(get);
    field.set(get, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("347e5a6d-6599-3233-9cd4-45fee308ce34"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e07c9a7b-e143-3a3e-8160-c818db9d7933"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("843bbe02-86ec-3f22-9581-ff052063a0a2"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfbff02f-5443-31a7-b4d1-b99f76259a00"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (region.getCoprocessorHost().preGet(get, results)) {
if(KnobRuntime.check(java.util.UUID.fromString("aab41a2c-4ae3-3bdd-8189-08c5771dc227"))) {
before -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a619a815-8f05-3124-806e-e7f198ebba59"))) {
before = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd3fe0ee-26c4-3a5c-950d-ac277f4a8830"))) {
before += 1;
}
        region.metricsUpdateForGet(results, before);
        return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null,
          stale);
      }
    }
    Scan scan = new Scan(get);
    if (((KnobRuntime.check(java.util.UUID.fromString("7781eda8-e13f-3c67-8991-8389416856b9"))) ? ((scan.getLoadColumnFamiliesOnDemandValue()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f8db2fdf-bc26-3353-9c9a-5190af2e7891"))) ? ((scan.getLoadColumnFamiliesOnDemandValue()) != (null)) : (scan.getLoadColumnFamiliesOnDemandValue() == null))))) {
      scan.setLoadColumnFamiliesOnDemand(region.isLoadingCfsOnDemandDefault());
    }
    RegionScannerImpl scanner = null;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("0896afbf-f713-302a-bcd3-68e8a001e86a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e506b43-6e9e-3e72-bea5-d5554aaf363a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c781fec-a0f9-30e7-8bb6-9b4507b8a6a0"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43debf25-a7dc-3a34-b040-fced58e70f8e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("628196e4-a66d-34fb-a983-6cc34c3f2d4e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5190c369-10bb-31b2-aefe-17c9f632d891"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27f3f257-ea74-3cb9-a6ae-95d0f7bd89c4"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b072f071-4d9c-3737-a99d-218c52a53d1b"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("942b018e-77ca-3f83-ad6b-484f15c9f8e2"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a24a3726-92c6-396c-a271-2f612399a916"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("799a03f1-8a0a-3fd5-9634-883a9ee4110b"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f7bf70e-fa5f-3ff8-9144-d466570583c6"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a68c1eeb-0ce9-3824-9bf2-8257de519669"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("mvccReadPoint");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7100a66-ee2d-3deb-ba0c-ffd4446bee6a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scan);
    field.set(scan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9cfc939-3365-3f86-825d-7d68d2b28250"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cbdb4e4a-db94-3e23-918f-92fa2754be14"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0bb78ee-e59c-393a-9733-79a20559421b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2a17d1a8-3be5-320d-8586-719fe5aa23f3"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d898d389-12f6-3b69-8303-61927f81cd4c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a8fb998b-8979-34c3-8c54-675ff509cba6"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9674eb72-a09f-39c0-9928-8ad7751b136b"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("112b2701-0ec1-30d5-a875-ce3fc7e4f071"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2942817f-505e-39e4-a9dc-d70502f77f50"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71be5e07-ff76-33f0-b127-f95c7251fbb2"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("94f96874-3551-3a13-af81-2a9c540767e2"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b55a5f22-3d2b-32cb-8ab2-aa2488eb3db3"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("26c0ab97-f11d-3713-950d-35ba10513bc4"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb884977-c886-3851-94f1-f16f7c65e6cd"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      scanner = region.getScanner(scan);
      scanner.next(results);
    } finally {
      if (scanner != null) {
        if (closeCallBack == null) {
          // If there is a context then the scanner can be added to the current
          // RpcCallContext. The rpc callback will take care of closing the
          // scanner, for eg in case
          // of get()
          context.setCallBack(scanner);
        } else {
          // The call is from multi() where the results from the get() are
          // aggregated and then send out to the
          // rpc. The rpccall back will close all such scanners created as part
          // of multi().
          closeCallBack.addScanner(scanner);
        }
      }
    }

    // post-get CP hook
    if (((KnobRuntime.check(java.util.UUID.fromString("9cde4643-c4db-385d-9b92-07c0f821ad69"))) ? ((region.getCoprocessorHost()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0e3d86af-b903-37bd-8aaf-8d84ab0e7a8c"))) ? ((region.getCoprocessorHost()) != (null)) : (region.getCoprocessorHost() != null))))) {
      region.getCoprocessorHost().postGet(get, results);
    }
    region.metricsUpdateForGet(results, before);

    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  private void checkBatchSizeAndLogLargeSize(MultiRequest request) throws ServiceException {
if(KnobRuntime.check(java.util.UUID.fromString("5a49f5d4-6945-344c-9d9c-363c74661687"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0acdc5b7-fcb0-3020-b87c-64b13e24aa29"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28b76def-3ab3-3274-b459-b28c04b7d55d"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de277835-a7cb-3550-80c1-3ce22964956b"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b248bd6-9126-3d05-934e-f6d6653a8a3a"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c7df99e-8827-34d6-bdfa-33b105a9d7a5"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("nonceGroup_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    int sum = 0;
    String firstRegionName = null;
    for (RegionAction regionAction : request.getRegionActionList()) {
      if (((KnobRuntime.check(java.util.UUID.fromString("dbf6ebda-4199-3e1e-9565-276081c5485c"))) ? ((sum) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fc6baf2d-580d-3818-be5a-4f55d4bad74c"))) ? ((sum) == (0)) : (sum == 0))))) {
        firstRegionName = Bytes.toStringBinary(regionAction.getRegion().getValue().toByteArray());
      }
      sum += regionAction.getActionCount();
    }
    if (sum > rowSizeWarnThreshold) {
      LOG.warn("Large batch operation detected (greater than " + rowSizeWarnThreshold
        + ") (HBASE-18023)." + " Requested Number of Rows: " + sum + " Client: "
        + RpcServer.getRequestUserName().orElse(null) + "/"
        + RpcServer.getRemoteAddress().orElse(null) + " first region in multi=" + firstRegionName);
      if (rejectRowsWithSizeOverThreshold) {
        throw new ServiceException(
          "Rejecting large batch operation for current batch with firstRegionName: "
            + firstRegionName + " , Requested Number of Rows: " + sum + " , Size Threshold: "
            + rowSizeWarnThreshold);
      }
    }
  }

  private void failRegionAction(MultiResponse.Builder responseBuilder,
    RegionActionResult.Builder regionActionResultBuilder, RegionAction regionAction,
    CellScanner cellScanner, Throwable error) {
    rpcServer.getMetrics().exception(error);
    regionActionResultBuilder.setException(ResponseConverter.buildException(error));
    responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
    // All Mutations in this RegionAction not executed as we can not see the Region online here
    // in this RS. Will be retried from Client. Skipping all the Cells in CellScanner
    // corresponding to these Mutations.
    if (cellScanner != null) {
      skipCellsForMutations(regionAction.getActionList(), cellScanner);
    }
  }

  /**
   * Execute multiple actions on a table: get, mutate, and/or execCoprocessor
   * @param rpcc    the RPC controller
   * @param request the multi request
   */
  @Override
  public MultiResponse multi(final RpcController rpcc, final MultiRequest request)
    throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }

    checkBatchSizeAndLogLargeSize(request);

    // rpc controller is how we bring in data via the back door; it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    HBaseRpcController controller = (HBaseRpcController) rpcc;
    CellScanner cellScanner = controller != null ? controller.cellScanner() : null;
    if (controller != null) {
      controller.setCellScanner(null);
    }

    long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;

    MultiResponse.Builder responseBuilder = MultiResponse.newBuilder();
    RegionActionResult.Builder regionActionResultBuilder = RegionActionResult.newBuilder();
    this.rpcMultiRequestCount.increment();
    this.requestCount.increment();
    ActivePolicyEnforcement spaceQuotaEnforcement = getSpaceQuotaManager().getActiveEnforcements();

    // We no longer use MultiRequest#condition. Instead, we use RegionAction#condition. The
    // following logic is for backward compatibility as old clients still use
    // MultiRequest#condition in case of checkAndMutate with RowMutations.
    if (request.hasCondition()) {
      if (request.getRegionActionList().isEmpty()) {
        // If the region action list is empty, do nothing.
        responseBuilder.setProcessed(true);
        return responseBuilder.build();
      }

      RegionAction regionAction = request.getRegionAction(0);

      // When request.hasCondition() is true, regionAction.getAtomic() should be always true. So
      // we can assume regionAction.getAtomic() is true here.
      assert ((KnobRuntime.check(java.util.UUID.fromString("30536761-6882-3681-bbaa-84fefeafa91c"))) ? (regionAction.hasCondition()) : (regionAction.getAtomic()));

      OperationQuota quota;
      HRegion region;
      RegionSpecifier regionSpecifier = regionAction.getRegion();

      try {
if(KnobRuntime.check(java.util.UUID.fromString("ae156903-014e-3361-97f0-5a8080d53a90"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("532396b7-c4ce-33ab-bf4b-11270c27580e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eea69445-2e27-3644-affe-67d54ff27df7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("205a0a93-cf96-34ad-9b3a-491e59947962"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0642e87a-cd0c-3d3e-a5d9-0fc08946d753"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("93a98ef6-9336-3a34-a345-a51daa66af45"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("91821444-772d-3a46-941f-8b1de313fad3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("450046bc-2c10-320a-9c55-1da14fa60c6a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d893e6f8-2718-3f65-8c98-ed66465f0e89"))) {
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
        region = getRegion(regionSpecifier);
        quota = getRpcQuotaManager().checkBatchQuota(region, regionAction.getActionList(),
          regionAction.hasCondition());
      } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("e67712aa-9ddb-3ad2-b45e-7feba98ac720"))) {
try {
    java.lang.reflect.Field field = regionAction.getClass().getDeclaredField("atomic_");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionAction);
    field.set(regionAction, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7de006e2-dddc-312c-8b09-4e651e90ec64"))) {
try {
    java.lang.reflect.Field field = regionAction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionAction));
    field.set(regionAction, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dad69f50-39f6-3da0-b9e1-8941bc0d2338"))) {
try {
    java.lang.reflect.Field field = responseBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(responseBuilder));
    field.set(responseBuilder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22997d0e-8266-3c64-ba17-1c439cf014d2"))) {
try {
    java.lang.reflect.Field field = regionActionResultBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionActionResultBuilder));
    field.set(regionActionResultBuilder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3a926a5-ca14-33cd-8303-4e7609b163f6"))) {
try {
    java.lang.reflect.Field field = responseBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(responseBuilder));
    field.set(responseBuilder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d215181e-e341-3538-8a91-6a3e3079bd53"))) {
try {
    java.lang.reflect.Field field = regionActionResultBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionActionResultBuilder));
    field.set(regionActionResultBuilder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1eb56e15-5ae8-3cce-bb83-cdb6642b96e3"))) {
try {
    java.lang.reflect.Field field = regionAction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionAction));
    field.set(regionAction, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bad462cc-70cc-3a19-8df7-6d400612edbf"))) {
try {
    java.lang.reflect.Field field = regionAction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionAction));
    field.set(regionAction, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef8f37e0-9f39-35a9-9a16-31269510dbcb"))) {
try {
    java.lang.reflect.Field field = responseBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(responseBuilder));
    field.set(responseBuilder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41d0da49-ac60-3687-9ec0-0dc83e3b7c46"))) {
try {
    java.lang.reflect.Field field = regionActionResultBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionActionResultBuilder));
    field.set(regionActionResultBuilder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28c4bcd0-bdb2-3d5c-b5ce-f9475d043dcb"))) {
try {
    java.lang.reflect.Field field = responseBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(responseBuilder));
    field.set(responseBuilder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6971622-b361-3ccb-85f1-31e664fbc2f0"))) {
try {
    java.lang.reflect.Field field = regionActionResultBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionActionResultBuilder));
    field.set(regionActionResultBuilder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9275ab1d-8b90-3fd4-93dd-3721dc3be343"))) {
try {
    java.lang.reflect.Field field = regionAction.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionAction));
    field.set(regionAction, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner, e);
        return responseBuilder.build();
      }

      try {
if(KnobRuntime.check(java.util.UUID.fromString("e0fd9162-0646-3828-9fbd-e7ebd4fbae7f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fb1da724-4b94-3b31-8b48-c9cc0de608a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5c50b957-6019-33b4-8a0a-83ce17c8fe67"))) {
nonceGroup = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9549f834-8503-38b8-82f3-0708efacb307"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8f040efa-6389-35dd-a116-b38e74258f11"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4c17e84-4030-3277-a507-db7da03339b1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1ad50aa2-d7d7-314d-96f3-d704ba9c9e7b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1e859394-b5df-32df-8fc0-97e872234941"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1355649e-54ae-3668-82f4-2570d531fd6b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("021e7d58-baab-3990-9d71-d86663510a40"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("db16ae35-8bd5-34b6-9040-486b8e63b2b7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("604d5aa9-e1f2-35b3-9bdc-2d085a2b63a4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("97db4572-6a69-33db-80e1-75a485b1313b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("abbcf37f-5280-3113-9766-413456b11142"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5b8ad587-04b6-3c39-98dc-9db7057e8924"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("41094f05-3aba-38f0-aa06-f59229000d66"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("63050c54-8a79-3210-9d26-abf8fb57e942"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eeda80e7-711f-3352-af4e-9fffe4916313"))) {
nonceGroup += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25d71f6b-24c5-3cb9-9269-ec9400bf033c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f56abc51-6dca-306d-ab1a-e50ed1db3e64"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c4649fcd-846a-3b94-b4c0-fe4f16403e1a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cf54641a-f63a-378e-b591-bcff06ff197c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("159b2e44-f605-3739-994f-2e4553a5fa08"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("abebedd7-c430-3978-b8f9-5d4d2da473ab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79db8483-466f-3231-b404-e913985978c2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8ad9ec65-9a7b-3250-980d-a5690e68d177"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a54d862f-3b96-3b1b-bd6a-5bcd91fd4241"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b6c08eb9-be45-317b-86e3-7a8b0fb9385f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b701592-ef42-350f-b336-596a0e6a247b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8287cb01-37ba-397d-8727-985beed4b6d9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05c8a5d2-0d2a-3f3e-a2a0-f45c3c463c9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("37a21f09-d26b-39f6-ab99-7b4fb84ca6ff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("af32978d-fadf-3cee-8fbc-5e6e8a9aa737"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("07c7f516-6fe5-3589-8983-f295149cbc11"))) {
nonceGroup = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("653e4ced-4c62-3b17-aaac-7934a0033cea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b09894da-6580-318b-8df3-38f9b6d97d2c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("96fdd825-aee6-3210-8179-b81646d8e2fc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("75f21068-4c27-3490-861a-e86fc18c6fb2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba721bf2-6416-3e9f-ac8d-5878017240fc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("63e23690-3217-3f10-8665-b94000c34f17"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3328595c-e04d-3804-b24c-49f4c2180da1"))) {
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
        CheckAndMutateResult result = checkAndMutate(region, regionAction.getActionList(),
          cellScanner, request.getCondition(), nonceGroup, spaceQuotaEnforcement);
        responseBuilder.setProcessed(result.isSuccess());
        ClientProtos.ResultOrException.Builder resultOrExceptionOrBuilder =
          ClientProtos.ResultOrException.newBuilder();
        for (int i = 0; i < regionAction.getActionCount(); i++) {
          // To unify the response format with doNonAtomicRegionMutation and read through
          // client's AsyncProcess we have to add an empty result instance per operation
          resultOrExceptionOrBuilder.clear();
          resultOrExceptionOrBuilder.setIndex(i);
          regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder.build());
        }
      } catch (IOException e) {
        rpcServer.getMetrics().exception(e);
        // As it's an atomic operation with a condition, we may expect it's a global failure.
        regionActionResultBuilder.setException(ResponseConverter.buildException(e));
      } finally {
        quota.close();
      }

      responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
      ClientProtos.RegionLoadStats regionLoadStats = region.getLoadStatistics();
      if (regionLoadStats != null) {
        responseBuilder.setRegionStatistics(MultiRegionLoadStats.newBuilder()
          .addRegion(regionSpecifier).addStat(regionLoadStats).build());
      }
      return responseBuilder.build();
    }

    // this will contain all the cells that we need to return. It's created later, if needed.
    List<CellScannable> cellsToReturn = null;
    RegionScannersCloseCallBack closeCallBack = null;
    RpcCallContext context = RpcServer.getCurrentCall().orElse(null);
    Map<RegionSpecifier, ClientProtos.RegionLoadStats> regionStats =
      new HashMap<>(request.getRegionActionCount());

    for (RegionAction regionAction : request.getRegionActionList()) {
      OperationQuota quota;
      HRegion region;
      RegionSpecifier regionSpecifier = regionAction.getRegion();
      regionActionResultBuilder.clear();

      try {
        region = getRegion(regionSpecifier);
        quota = getRpcQuotaManager().checkBatchQuota(region, regionAction.getActionList(),
          regionAction.hasCondition());
      } catch (IOException e) {
        failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner, e);
        continue; // For this region it's a failure.
      }

      try {
        if (regionAction.hasCondition()) {
          try {
            ClientProtos.ResultOrException.Builder resultOrExceptionOrBuilder =
              ClientProtos.ResultOrException.newBuilder();
            if (regionAction.getActionCount() == 1) {
              CheckAndMutateResult result =
                checkAndMutate(region, quota, regionAction.getAction(0).getMutation(), cellScanner,
                  regionAction.getCondition(), nonceGroup, spaceQuotaEnforcement, context);
              regionActionResultBuilder.setProcessed(result.isSuccess());
              resultOrExceptionOrBuilder.setIndex(0);
              if (result.getResult() != null) {
                resultOrExceptionOrBuilder.setResult(ProtobufUtil.toResult(result.getResult()));
              }
              regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder.build());
            } else {
              CheckAndMutateResult result = checkAndMutate(region, regionAction.getActionList(),
                cellScanner, regionAction.getCondition(), nonceGroup, spaceQuotaEnforcement);
              regionActionResultBuilder.setProcessed(result.isSuccess());
              for (int i = 0; i < regionAction.getActionCount(); i++) {
                if (i == 0 && result.getResult() != null) {
                  // Set the result of the Increment/Append operations to the first element of the
                  // ResultOrException list
                  resultOrExceptionOrBuilder.setIndex(i);
                  regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder
                    .setResult(ProtobufUtil.toResult(result.getResult())).build());
                  continue;
                }
                // To unify the response format with doNonAtomicRegionMutation and read through
                // client's AsyncProcess we have to add an empty result instance per operation
                if (KnobRuntime.check(java.util.UUID.fromString("01d60f28-c845-3832-a996-a37ad44b190d"))) { ClientProtos.ResultOrException.newBuilder(); } else if (KnobRuntime.check(java.util.UUID.fromString("2c20b5dd-4985-3661-a820-1c6c7576ce23"))) { ResultOrException.newBuilder(); } else { resultOrExceptionOrBuilder.clear(); }
                resultOrExceptionOrBuilder.setIndex(i);
                regionActionResultBuilder.addResultOrException(resultOrExceptionOrBuilder.build());
              }
            }
          } catch (IOException e) {
            rpcServer.getMetrics().exception(e);
            // As it's an atomic operation with a condition, we may expect it's a global failure.
            regionActionResultBuilder.setException(ResponseConverter.buildException(e));
          }
        } else if (regionAction.hasAtomic() && regionAction.getAtomic()) {
          try {
            doAtomicBatchOp(regionActionResultBuilder, region, quota, regionAction.getActionList(),
              cellScanner, nonceGroup, spaceQuotaEnforcement);
            regionActionResultBuilder.setProcessed(true);
            // We no longer use MultiResponse#processed. Instead, we use
            // RegionActionResult#processed. This is for backward compatibility for old clients.
            responseBuilder.setProcessed(true);
          } catch (IOException e) {
            rpcServer.getMetrics().exception(e);
            // As it's atomic, we may expect it's a global failure.
            regionActionResultBuilder.setException(ResponseConverter.buildException(e));
          }
        } else {
          // doNonAtomicRegionMutation manages the exception internally
          if (((KnobRuntime.check(java.util.UUID.fromString("8971f4fe-455a-39d8-8ffb-38b9346dd847"))) ? (((context) != (null)) || ((closeCallBack) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("08e377e0-1cce-3e7c-a017-8c08a6ea339f"))) ? (((context) == (null)) || ((closeCallBack) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2aebac58-e01b-33e3-a006-a6ac5acb0337"))) ? ((context != null) || (closeCallBack == null)) : (((KnobRuntime.check(java.util.UUID.fromString("570b3e96-4191-3943-890e-441145ed86aa"))) ? ((context != null) || ((closeCallBack) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bde69db4-7e1c-39bd-885a-14139bbf1f22"))) ? (((context) == (null)) || (closeCallBack == null)) : (((KnobRuntime.check(java.util.UUID.fromString("2bf2ee7b-e81c-3ce7-b2e8-f98fa37d25c8"))) ? (context != null) : (((KnobRuntime.check(java.util.UUID.fromString("f0a7df03-4f90-37a5-860c-f66757a7fa04"))) ? (((context) == (null)) || ((closeCallBack) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("33ea4e89-30e6-3cb3-aaeb-cf4cfee5f7a9"))) ? ((context != null) && ((closeCallBack) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("355de8d9-40d2-3436-98f3-805e1b260665"))) ? ((closeCallBack) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e8da0afe-41b0-31c7-9d39-f7635898e9f1"))) ? (((context) != (null)) && ((closeCallBack) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3d8f4e23-cd7f-354e-a5f8-25a2e7592678"))) ? (((context) != (null)) || (closeCallBack == null)) : (((KnobRuntime.check(java.util.UUID.fromString("04b1dbd1-428d-33af-a8df-01315174f478"))) ? (((context) == (null)) && ((closeCallBack) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("11250cdb-055c-3884-beaf-3b217790a7ff"))) ? (closeCallBack == null) : (((KnobRuntime.check(java.util.UUID.fromString("6c68429e-b135-3691-ade1-91da45330d7f"))) ? (((context) == (null)) && ((closeCallBack) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("47ccb228-4efd-3b82-8628-0b72df82a434"))) ? ((context != null) && ((closeCallBack) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("03364370-caca-3be3-92db-b9a3cd0bc414"))) ? (((context) == (null)) && (closeCallBack == null)) : (((KnobRuntime.check(java.util.UUID.fromString("747208d8-db4b-3c1e-bf2e-1d5345ff6b55"))) ? ((context != null) && (closeCallBack == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6dfb7aa9-b218-37e7-a149-59769f8dca50"))) ? (((context) != (null)) || ((closeCallBack) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("40b0bf3c-67e6-366b-af85-a0f9b4ae6089"))) ? ((closeCallBack) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("03601877-a231-3cc1-b166-e379cb04508b"))) ? ((context) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f5a37462-6aac-3c81-a231-c73107114a1f"))) ? (((context) != (null)) && (closeCallBack == null)) : (((KnobRuntime.check(java.util.UUID.fromString("4ee89f95-f1e7-3208-b860-7eb16b725330"))) ? (((context) != (null)) && ((closeCallBack) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6826d4bb-7d24-3036-83dd-8b4e705046b0"))) ? ((context != null) || ((closeCallBack) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8a222bd7-c8d1-3d55-8ab4-b19e0375a33d"))) ? ((context) == (null)) : (context != null && closeCallBack == null))))))))))))))))))))))))))))))))))))))))))))))))) {
            // An RpcCallBack that creates a list of scanners that needs to perform callBack
            // operation on completion of multiGets.
            // Set this only once
            closeCallBack = new RegionScannersCloseCallBack();
            context.setCallBack(closeCallBack);
          }
          cellsToReturn = doNonAtomicRegionMutation(region, quota, regionAction, cellScanner,
            regionActionResultBuilder, cellsToReturn, nonceGroup, closeCallBack, context,
            spaceQuotaEnforcement);
        }
      } finally {
        quota.close();
      }

      responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
      ClientProtos.RegionLoadStats regionLoadStats = region.getLoadStatistics();
      if (regionLoadStats != null) {
        regionStats.put(regionSpecifier, regionLoadStats);
      }
    }
    // Load the controller with the Cells to return.
    if (cellsToReturn != null && !cellsToReturn.isEmpty() && controller != null) {
      controller.setCellScanner(CellUtil.createCellScanner(cellsToReturn));
    }

    MultiRegionLoadStats.Builder builder = MultiRegionLoadStats.newBuilder();
    for (Entry<RegionSpecifier, ClientProtos.RegionLoadStats> stat : regionStats.entrySet()) {
      builder.addRegion(stat.getKey());
      builder.addStat(stat.getValue());
    }
    responseBuilder.setRegionStatistics(builder);
    return responseBuilder.build();
  }

  private void skipCellsForMutations(List<Action> actions, CellScanner cellScanner) {
if(KnobRuntime.check(java.util.UUID.fromString("7728191b-3636-3663-a23c-956e7fda0b82"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ab0d0a8c-f956-39d2-b4e8-b3f9075dd8a2"))) ? ((cellScanner) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("19e422fe-daca-3412-83e8-f5474a8e1ef9"))) ? ((cellScanner) == (null)) : (cellScanner == null))))) {
      return;
    }
    for (Action action : actions) {
      skipCellsForMutation(action, cellScanner);
    }
  }

  private void skipCellsForMutation(Action action, CellScanner cellScanner) {
    if (cellScanner == null) {
      return;
    }
    try {
      if (action.hasMutation()) {
        MutationProto m = action.getMutation();
        if (m.hasAssociatedCellCount()) {
          for (int i = 0; i < m.getAssociatedCellCount(); i++) {
            cellScanner.advance();
          }
        }
      }
    } catch (IOException e) {
      // No need to handle these Individual Muatation level issue. Any way this entire RegionAction
      // marked as failed as we could not see the Region here. At client side the top level
      // RegionAction exception will be considered first.
      if (KnobRuntime.check(java.util.UUID.fromString("463fa7da-1931-36eb-8a5c-60b40907140f"))) { LOG.warn("Error while skipping Cells in CellScanner for invalid Region Mutations", e); } else { LOG.error("Error while skipping Cells in CellScanner for invalid Region Mutations", e); }
    }
  }

  /**
   * Mutate data in a table.
   * @param rpcc    the RPC controller
   * @param request the mutate request
   */
  @Override
  public MutateResponse mutate(final RpcController rpcc, final MutateRequest request)
    throws ServiceException {
if(KnobRuntime.check(java.util.UUID.fromString("6f6ed4db-eb74-3ce5-9b51-4ad9aaefb6da"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12de31a8-f981-3926-894c-becec82f0a01"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("865020aa-0f4a-3011-9e27-530dfc2ed38e"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b75b87e6-9e38-3f69-92fc-3313c3e584e8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("2564eab2-3003-3692-ab40-de62625a69f6"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("nonceGroup_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3f63bed8-4efa-3980-a17c-3ab4f84e21fe"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // rpc controller is how we bring in data via the back door; it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    HBaseRpcController controller = (HBaseRpcController) rpcc;
    CellScanner cellScanner = controller != null ? controller.cellScanner() : null;
    OperationQuota quota = null;
    RpcCallContext context = RpcServer.getCurrentCall().orElse(null);
    // Clear scanner so we are not holding on to reference across call.
    if (((KnobRuntime.check(java.util.UUID.fromString("b0f06189-8d2c-3c8c-9a9f-e69d92fe5b94"))) ? ((controller) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("35101ca3-ab96-3420-9082-d2f681a359df"))) ? ((controller) == (null)) : (controller != null))))) {
      controller.setCellScanner(null);
    }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("3909c3bf-3635-3c6f-9713-db646e6780ae"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("0cd03552-f5ad-303c-872e-c182c41aec96"))) { closeAllScanners(); } else { checkOpen(); }
      requestCount.increment();
      rpcMutateRequestCount.increment();
      HRegion region = getRegion(request.getRegion());
      MutateResponse.Builder builder = MutateResponse.newBuilder();
      MutationProto mutation = request.getMutation();
      if (!region.getRegionInfo().isMetaRegion()) {
        regionServer.getMemStoreFlusher().reclaimMemStoreMemory();
      }
      long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;
      OperationQuota.OperationType operationType = QuotaUtil.getQuotaOperationType(request);
      quota = getRpcQuotaManager().checkBatchQuota(region, operationType);
      ActivePolicyEnforcement spaceQuotaEnforcement =
        getSpaceQuotaManager().getActiveEnforcements();

      if (((KnobRuntime.check(java.util.UUID.fromString("e9f81042-b7a4-3d5d-a1f6-652658e9b01b"))) ? (request.hasNonceGroup()) : (request.hasCondition()))) {
if(KnobRuntime.check(java.util.UUID.fromString("588361af-89b1-3de3-b62f-affc0d7cb8bd"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4355cc0a-1f94-3d43-973c-5da6e1aa2764"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5de9241d-0439-345a-a85d-4d3622af0a5c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02fc3292-067c-3f30-9248-4c3fa6e11b98"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e546209d-6f67-3d09-a1e3-9dd9111b44f2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c99de209-b42e-35f6-b4a7-394450645db6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5dd3ce12-cac1-3256-bcd9-fd8eb2ebaeae"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70e99a56-4373-3895-a479-0b7cfa643226"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2112d01c-21f8-32a8-99ae-dce343624b44"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("627e7e6e-ba49-3ee6-b763-ae6d68a2ce9a"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("083047da-57fa-34ad-bc4b-731592807055"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0bfcbb6e-395d-3700-8c2b-3c22c212776c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f8d07694-bfd3-3030-ba81-06f82377457a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("95d8633e-a245-365e-b584-c6a3ef363ba0"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9fbb9ed1-874e-3c9c-93f0-35ec03026e6b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c9c28d93-715e-3e70-ad2a-7451318acefa"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66db3cdb-93da-310a-b06f-0a993399ba39"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("735d476d-49fa-3bbb-a288-3b48ed550857"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("924d04e4-641c-3f7c-9669-f00733c03751"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e864c0e7-0a5b-34cc-8977-723f8a28a03c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ad6b273-9c2c-3d21-adf1-49cae0bb321c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2b4578e8-5eb9-3bb8-aaa7-014ada1c6b26"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7dd385f3-523b-3618-bacf-43ceb345df62"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5578e67-e1cc-36dd-8c64-fedc8e236ef0"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d019162a-852f-3b47-8b21-98b4fc20d7ca"))) {
nonceGroup = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b801df5e-11be-313b-a450-57a95fefdd08"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a2173690-d2e4-3a58-8305-b7f5ba45a6a1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6ff7f34e-d4ed-3f27-93b5-650ee75d6f0c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dec4980-0a31-300a-b08d-82c642420f07"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0b40600-cc30-346a-93b1-9c48482d3926"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fdb47773-8c79-3c1d-b173-a2f990f65d0b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5efe492e-bbf4-368d-9b11-0ae5056f159d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("819861ce-defd-34e1-b834-575e28d4378b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("92e1aec3-759a-3f69-8bbe-2c41e8e4ee94"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d2b3faf-d361-34e8-a566-930bbabde19c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b3ff4878-4083-3a3d-9582-24a4957cb232"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dfb352ba-bbc4-3e7a-9369-3f3389c2db51"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("87bac6c0-c135-325c-84ff-554cb00a6c9e"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1aece72e-29c0-3a0d-9a66-f40578905db3"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("867a54f6-a5ea-39cb-82af-86feb2c139d5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8e430c27-bd0e-3983-a777-55747a61bd95"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc10beeb-67f9-3c06-bc0e-7fc39430138b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b908c557-8a63-33a1-a775-d7aa495926ea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3ea008cb-a750-361f-a259-18f3a3617f9b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a215e578-8988-30df-aac6-0bbf37d12c26"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36ed1026-33c8-3be6-91ec-0907f3279e02"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05271f86-cc66-3cc7-8e19-65178df08e91"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b48d74f4-263d-3a20-a7aa-16cbfe6ff78c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1fc9f995-f8fb-30df-b64c-777f0285b026"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfaed9af-1e9a-39c7-9516-1494b10620e5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b8bcd4b8-50b6-3caf-b381-545fd1db14b9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("da1c9a38-5c6f-321d-acc9-2efa57ef11b2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fd326d7a-3868-3628-b7ca-f62a5bde2b65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6b932235-38c5-3ad6-a248-5a21d25b29b3"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a72edc88-61fa-3ba4-a299-33ab39813bd8"))) {
nonceGroup += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0bb2fb3f-c749-3239-bc25-279ae06523ae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7496bc33-4253-37cf-a3c9-52caa5f68544"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b01b5081-68d7-37ba-8e7f-717d63b35a7e"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b150d801-275c-3b20-ac8a-f567c6934f79"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2b6f2430-5ec8-3056-a714-6b42e518f328"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("43d4e0cc-3aee-3f0f-94c1-09657a47b2ef"))) {
nonceGroup = 0;
}
        CheckAndMutateResult result = checkAndMutate(region, quota, mutation, cellScanner,
          request.getCondition(), nonceGroup, spaceQuotaEnforcement, context);
        builder.setProcessed(result.isSuccess());
        boolean clientCellBlockSupported = isClientCellBlockSupport(context);
        addResult(builder, result.getResult(), controller, clientCellBlockSupported);
        if (clientCellBlockSupported) {
          addSize(context, result.getResult());
        }
      } else {
        Result r = null;
        Boolean processed = null;
        MutationType type = mutation.getMutateType();
        switch (type) {
          case APPEND:
            // TODO: this doesn't actually check anything.
            r = append(region, quota, mutation, cellScanner, nonceGroup, spaceQuotaEnforcement,
              context);
            break;
          case INCREMENT:
            // TODO: this doesn't actually check anything.
            r = increment(region, quota, mutation, cellScanner, nonceGroup, spaceQuotaEnforcement,
              context);
            break;
          case PUT:
            put(region, quota, mutation, cellScanner, spaceQuotaEnforcement);
            processed = Boolean.TRUE;
            break;
          case DELETE:
if(KnobRuntime.check(java.util.UUID.fromString("c9c3e2cc-130c-3a5a-be13-d15cff387c1b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e5a1237a-fd21-3156-a2a6-ab22db13be97"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("99f65180-092f-3289-897b-b2fcae70836b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("893ca847-2f83-3ef6-be17-34eea6ca8108"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c54c45a3-721f-3653-a39e-c7b7bb607eb9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("799038ba-42d6-3568-a383-ae716e73da21"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("819c4c61-e3e1-32d3-a887-366eb218dde7"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d76c741c-a6c6-3feb-b310-e5dad7f485d0"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("94930937-2085-3116-b33d-d4420f1455e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6defbb20-4089-37d5-a6f4-68c7ae4bb2c2"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15c9e765-307e-3483-ab53-0fd44b3b4f7f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5d200d28-656c-3613-81e3-14a626b44905"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4aec72bf-e442-35a3-b2a6-6c90aafb30f9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("55828547-7f9d-3489-8776-91479dd510e8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("da98cd0b-97cd-3718-aed6-9878f35773d8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("80a178ca-068d-3c5b-bef1-88b96f500201"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("63d948d5-5993-367a-be95-368fa1ab9c30"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("67d9af66-9010-3d35-8290-58f301c98bc0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7bd98c5f-f985-35a2-9d00-2b099ae6a26c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bdde3554-730a-3097-95b7-8e5ea048d589"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f543ebce-5d50-3a29-8228-25278f4b6788"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43772c9e-e2c6-37d4-b3ee-89f052f1b4a2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ff8493a9-f640-38e9-a295-39a99718ccd9"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("929827c3-be4b-3517-a46b-f352b35336c6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("35b15ce1-9a44-330f-bb48-77bd0d7ccebd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bc87326d-d362-38d9-b57a-2d9fb6ecdaba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9357aaf3-4cac-39e9-9b4b-b8aa28ec8a9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ad701ddd-f3d2-31e6-b376-a1b608e73662"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b69f1afc-a4ab-3da2-befb-e9566ec4acc1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c52090aa-9785-3fd3-a567-d80e4834cba3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("93b5fb03-f07b-3155-ac07-5437f9a41b12"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b6cfa361-f9ae-37b4-9e14-460a07684d5d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f950169a-227d-36eb-ae52-756444250b29"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a3000e4-396c-3691-8303-8e665cd8291b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2e1a5aa6-36c6-3eb3-8a87-b246b47b6bcd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36daa96f-82e2-3440-abdc-4899cdeb767e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4b8c8a71-228a-3a32-9f48-d245ed725ae4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("efc284bd-1b26-30a7-bdbf-a17d56458047"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a9f51481-b0f0-30cd-8eac-e56bdce40b3b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e5ea98ca-250d-3cd8-b90c-0bb7167cd70a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("603c202e-e69c-3a6b-9a3c-f652eb949ac3"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f925c231-6e2b-302d-9986-99ebd6eff377"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("973e7094-f589-304c-b72c-ad7d65502e1f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b95c6687-f0f0-3c63-a7fa-012fc383d2ab"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("614f1a24-024c-3479-b320-a4ace2515db7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("74618b3b-48b0-3376-b473-592e1d431c5f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ddd7c2a-3e6a-3702-b563-05d35fe94760"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07a64b0e-0f3f-3781-a6a6-9738e4af96ed"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1fec586-5723-3981-b14c-97564a587220"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fddcd99-68ee-366d-b59d-7d63763e7446"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a99c1b76-f78d-3de8-bd62-a2b49f91e8f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("73fa3558-b0d7-37b3-8ed9-494123361b47"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c1a29132-b4f0-3d0f-9941-16606d456058"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("67a78f74-e3af-336d-82ad-8452b4e5c355"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c1a3b063-d8b9-396d-b4d9-3f1cff7340ad"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68e67f3d-f196-36c7-ba61-bc7bade9d39f"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5babde85-4d0e-3986-95e3-709f5675fed4"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da0b30b3-a153-3af5-8093-7c56bbbd233e"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            if (KnobRuntime.check(java.util.UUID.fromString("2653b19f-304d-3ad5-a3ea-8f6d673f5bdc"))) { put(region, quota, mutation, cellScanner, spaceQuotaEnforcement); } else { delete(region, quota, mutation, cellScanner, spaceQuotaEnforcement); }
            processed = Boolean.TRUE;
            break;
          default:
            throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("e742d4cb-2dae-3b60-a971-e26d59ab5d2a"))) ? ((processed) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7c891934-4e42-3648-9234-23630f54cb77"))) ? ((processed) != (null)) : (processed != null))))) {
          builder.setProcessed(processed);
        }
        boolean clientCellBlockSupported = isClientCellBlockSupport(context);
        addResult(builder, r, controller, clientCellBlockSupported);
        if (clientCellBlockSupported) {
          addSize(context, r);
        }
      }
      return builder.build();
    } catch (IOException ie) {
      if (KnobRuntime.check(java.util.UUID.fromString("38fb7398-4869-3050-9b43-1d6a65279fdd"))) { regionServer.isStopped(); } else if (KnobRuntime.check(java.util.UUID.fromString("3f10add0-84cb-3322-8f9a-457f54d4ab25"))) { regionServer.isAborted(); } else if (KnobRuntime.check(java.util.UUID.fromString("2e398ec3-a296-3097-b500-28f6f93a3ee0"))) { regionServer.isDataFileSystemOk(); } else { regionServer.checkFileSystem(); }
      throw new ServiceException(ie);
    } finally {
      if (((KnobRuntime.check(java.util.UUID.fromString("af808acd-28b6-3ed1-a396-2793215a7cb0"))) ? ((quota) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4ce56126-3632-3cf8-81f4-e642b9696cda"))) ? ((quota) != (null)) : (quota != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("9de02209-408a-3e99-b02d-f57b06cfa2bb"))) { closeAllScanners(); } else { quota.close(); }
      }
    }
  }

  private void put(HRegion region, OperationQuota quota, MutationProto mutation,
    CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("50d3aa0a-f765-3135-9325-76a22fdaf7b1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0848c641-9342-3b8c-ac82-c8e39c73c473"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9fa8e44-0580-376a-b12c-40f253b39b03"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("82140611-127b-3b0b-b009-5832ae1a15ca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9c384634-c1f3-3eb3-9c2f-78188caca8e0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("132c7bf3-16e9-3d61-944c-8289848524fb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f48aec14-fe25-3af9-969d-bf735c23994c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7523c853-a1ca-364f-86d5-45dd25aaa5e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("821412a7-2349-328e-8108-07d6ddcdb098"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d77e6a7-8dbf-384c-b604-ed3705f07bf7"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9a02a62f-20ab-3689-9046-8d1073331769"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("859b91b6-0d0d-3350-807e-03bc2db40571"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5a53d49e-f16c-3949-ac65-ee733a018fc9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("75dd0c74-1a8d-34fd-bd05-0d2c6a956c09"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("594d87bc-af9a-3283-8701-e8e59a655e2d"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("373cef88-b6bb-393e-9eb2-5b1776ad3159"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c853f223-5876-356b-bbbc-33a9b97cbbec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("387d4c5d-fca1-3aec-a6c5-0eda9221b956"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a9f3e5db-d148-36da-9f51-fe04c8d0e1ae"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87796372-aea0-3221-a96a-75e7b2a738d2"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8546d96c-b201-3dfc-a9b9-4c490e8c564b"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("082fb983-3b82-39ab-a5a3-0fcb878cd645"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("481c500c-ea29-3a16-8c35-7fdc5bdeb6a7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8d86f77c-47af-3816-b9df-5796c0aa86b1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cbdcd9e8-d4a5-3bcc-80bd-133636cb6f31"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5fb26998-5df4-3df2-87d9-553e991b8457"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fe16b861-a2b4-3207-9506-9ad3469bd5ac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e6556a7f-c9f7-31f3-8821-7b642e1b7f02"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20a02fb1-f6d9-335a-b7da-60cb798967d2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0cb73270-084a-3270-ae11-5839b7ffa1dc"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a571853e-2ab2-3f5e-995a-697d573a9470"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c06a676-5a86-3ce8-bc3d-e18a9cd901dd"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("edf293ec-43b4-3122-b04e-3b97c8ae5c84"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9955b8a1-8091-3d18-8659-a6bb865b85c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d080948b-ca55-31bd-a576-8448cb3c92f2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d591fbf3-7b12-3c25-beb0-2dd78ad7ab1c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("946287f4-074e-3559-9877-7a8e3cdca063"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f1ea059-3c58-3461-9b81-c7e7cd35e722"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13745335-acec-37cf-9224-684ab834d8c3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ca815214-44b3-32f4-a97b-b7d064964bcf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("510cff15-c558-3242-9d6c-a150e99781e4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8245a2ab-bca7-31b8-ac13-3ce12c781b5f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ff42bbce-14d3-3f29-a0dc-9ed28d2c54b5"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49437036-fbd6-324d-bb13-61fc5e736690"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a5fe5acf-fa2f-3096-ac41-f800671513c5"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58eafb96-31c4-3bd3-82ba-3662896321fd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("693f60a7-f795-39e3-8c42-2c142be8f112"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3fda3f10-c9e6-333e-b764-746f680aa4f1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eb81444b-26bf-3809-96ee-8772877438c9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("22bcb7b0-33e2-3631-beab-ecd03baeaab7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e9f67bc0-b841-3293-ae3a-2ddb7a9b2147"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4bf114ab-ced8-3d4e-a65a-d730eda840ff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9a626380-3001-3dfa-bcdd-5a1bd6788ea1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a86ae1b0-bee5-3db0-92e6-2a4c4bfe4713"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86624c44-5a2f-373b-8933-8dc05abfc5d2"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("52e80b27-7a17-34ba-a78d-036a4176998b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d5a93205-1438-3c86-823f-66814b6bac19"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d1eb5c27-384a-3eb1-87ed-1338bbc4a61d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d38b67bf-b4dd-35db-a004-049f636e6122"))) {
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
    long before = EnvironmentEdgeManager.currentTime();
    Put put = ProtobufUtil.toPut(mutation, cellScanner);
    checkCellSizeLimit(region, put);
    spaceQuota.getPolicyEnforcement(region).check(put);
    quota.addMutation(put);
if(KnobRuntime.check(java.util.UUID.fromString("7e7a5515-3e9f-3df1-9e8d-ce03830a01fc"))) {
throw new java.io.IOException("Injected exception");
}
    region.put(put);

    MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
    if (((KnobRuntime.check(java.util.UUID.fromString("c0df4d24-6073-34e6-99d0-446f255a76a3"))) ? ((metricsRegionServer) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2422863a-eb5a-3dbe-93b5-8caeb1b95014"))) ? ((metricsRegionServer) == (null)) : (metricsRegionServer != null))))) {
      long after = EnvironmentEdgeManager.currentTime();
      metricsRegionServer.updatePut(region, after - before);
    }
  }

  private void delete(HRegion region, OperationQuota quota, MutationProto mutation,
    CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("08872eca-d40c-3a2d-aa8a-385da9fe007e"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("edf34dc1-c9bd-3186-a8b1-3d8bf8f90ca6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a75c631a-7012-3551-98e3-c1912a57d13a"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8cf37246-234b-34e2-b339-4f27459f30de"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2cee8c78-1f4d-3d0e-98ed-6856b6bacb39"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2b6e4297-12aa-3b04-af28-c42aac216b9c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("65dc1bbc-728e-3202-9715-b776c401e8e9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("77963c54-4e31-3f74-96ea-cb0197d37b4f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("37645369-4c89-388e-9c3c-8cb81e5562c0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("470f5df1-e241-3440-97b0-132a60db459f"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("002e50e0-9051-3734-bccd-e9cee8f0091c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a69b981-dae4-3be3-9a7a-84d8461f563e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f3f4d88-6c01-3f1d-b64b-2c29adc87943"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5173054-9786-313a-a614-bb4302909464"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("43890df1-242b-3b03-ac07-4c6431cf35ce"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("26886da2-1d6c-3e1d-9a43-257aea0f09b5"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e8b15bf-fa8e-38c2-b7c1-ac18727cca43"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67e65748-3324-360d-bcb0-a22d08cdf52b"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fae1fb9b-c8e2-3447-9e6b-ed8a8be2bac0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5fde2f66-e19e-3de3-861a-f2a62fbd0f63"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e3b635e9-b8ed-3e34-86d8-2baa939f31dd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("20559200-91a8-33ef-aa7c-f3d066bda35c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eaa7fefc-1bc2-3ad4-85df-20ba438c146d"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dcd39f3b-5854-3e13-8bbe-57572cf60707"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bb89e1ce-ba20-3fdc-8d39-accc774170b4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01138367-ab24-3754-b3c6-59b785c78ef4"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fabd1552-c8b2-318d-90e4-4b242dcc8787"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("timestamp_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10b32dc0-1d65-3abd-9ee2-8157a721ed4a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1bda94bd-02cb-368c-b052-2c157022cf6b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e0a90ce6-fb57-31f3-94cf-c6fdf15827a0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5f8d55ae-790d-389b-b7d3-999723c755c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("03e09222-db38-3d3b-b26a-68c0b18af571"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("82cefdf3-7059-3120-a6a0-1900b0367b25"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0bfaf7b-d0de-329b-9b8a-ff6739c6c2e5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8cff882-f75b-3092-a9c6-d9697618c336"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ba702a2-791c-32d5-9b6f-656f6d7c7611"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fd253e0-6a93-3609-89f1-b38d7b2d9209"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("67c0fd0b-437d-3923-8ace-360955044be2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("895227dd-1a03-3ccc-bec0-ec4cd87f0c55"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("53db6c93-4c74-3dce-bb5e-e92216936e1d"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0a89b01-109e-343c-81ed-2f67020dfb01"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39bad29d-1c11-33a1-b8b6-f055d60e79ab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("368aa6cc-7023-3b1e-ab90-f82c52abd0b4"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e443ac8-0c17-3779-b0a7-38d32b2503b2"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ad6319e-4fa2-3e70-9ffa-e224f73b5b8e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("09b07f2f-48ad-383f-bf79-78cbbd2935aa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dffa7980-33ff-3885-9520-60d58ecebd6c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f691b6e8-d283-3703-b843-634ea4241dd0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1098aef8-1243-3cb0-a402-50c36d847393"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("76435d0c-4592-394c-827f-0ac46640bb27"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f6e465c-dd86-3180-94a1-5968c276cdad"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("durability_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a04c47bd-cf75-3c97-9885-a19ef1837a52"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2c984bf5-4b35-327e-9252-e42bf84f1c68"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("e474ef68-a20a-3f48-bbc9-bbaa8d576737"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3c997ca7-e54c-393f-b527-96cf9b3c9471"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("mutateType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab2d1f4b-b4c8-3bec-aa97-38e8aef725f7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6eb2303f-0ba2-3120-96c7-cdaf781c41c8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e89cc24b-753c-3e32-bcfa-d9e10c2c255a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ae667b91-8136-34d7-a75d-fea9b1d73c61"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(mutation));
    field.set(mutation, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    long before = EnvironmentEdgeManager.currentTime();
    Delete delete = ProtobufUtil.toDelete(mutation, cellScanner);
    checkCellSizeLimit(region, delete);
    spaceQuota.getPolicyEnforcement(region).check(delete);
    quota.addMutation(delete);
    region.delete(delete);

    MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      metricsRegionServer.updateDelete(region, after - before);
    }
  }

  private CheckAndMutateResult checkAndMutate(HRegion region, OperationQuota quota,
    MutationProto mutation, CellScanner cellScanner, Condition condition, long nonceGroup,
    ActivePolicyEnforcement spaceQuota, RpcCallContext context) throws IOException {
    long before = EnvironmentEdgeManager.currentTime();
    long blockBytesScannedBefore = context != null ? context.getBlockBytesScanned() : 0;
    CheckAndMutate checkAndMutate = ProtobufUtil.toCheckAndMutate(condition, mutation, cellScanner);
    long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
    checkCellSizeLimit(region, (Mutation) checkAndMutate.getAction());
    spaceQuota.getPolicyEnforcement(region).check((Mutation) checkAndMutate.getAction());
    quota.addMutation((Mutation) checkAndMutate.getAction());

    CheckAndMutateResult result = null;
    if (region.getCoprocessorHost() != null) {
      result = region.getCoprocessorHost().preCheckAndMutate(checkAndMutate);
    }
    if (result == null) {
      result = region.checkAndMutate(checkAndMutate, nonceGroup, nonce);
      if (region.getCoprocessorHost() != null) {
        result = region.getCoprocessorHost().postCheckAndMutate(checkAndMutate, result);
      }
    }
    MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      long blockBytesScanned =
        context != null ? context.getBlockBytesScanned() - blockBytesScannedBefore : 0;
      metricsRegionServer.updateCheckAndMutate(region, after - before, blockBytesScanned);

      MutationType type = mutation.getMutateType();
      switch (type) {
        case PUT:
          metricsRegionServer.updateCheckAndPut(region, after - before);
          break;
        case DELETE:
          metricsRegionServer.updateCheckAndDelete(region, after - before);
          break;
        default:
          break;
      }
    }
    return result;
  }

  // This is used to keep compatible with the old client implementation. Consider remove it if we
  // decide to drop the support of the client that still sends close request to a region scanner
  // which has already been exhausted.
  @Deprecated
  private static final IOException SCANNER_ALREADY_CLOSED = new IOException() {

    private static final long serialVersionUID = -4305297078988180130L;

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  };

  private RegionScannerHolder getRegionScanner(ScanRequest request) throws IOException {
    String scannerName = toScannerName(request.getScannerId());
    RegionScannerHolder rsh = this.scanners.get(scannerName);
    if (rsh == null) {
      // just ignore the next or close request if scanner does not exists.
      Long lastCallSeq = closedScanners.getIfPresent(scannerName);
      if (lastCallSeq != null) {
        // Check the sequence number to catch if the last call was incorrectly retried.
        // The only allowed scenario is when the scanner is exhausted and one more scan
        // request arrives - in this case returning 0 rows is correct.
        if (request.hasNextCallSeq() && request.getNextCallSeq() != lastCallSeq + 1) {
          throw new OutOfOrderScannerNextException("Expected nextCallSeq for closed request: "
            + (lastCallSeq + 1) + " But the nextCallSeq got from client: "
            + request.getNextCallSeq() + "; request=" + TextFormat.shortDebugString(request));
        } else {
          throw SCANNER_ALREADY_CLOSED;
        }
      } else {
        LOG.warn("Client tried to access missing scanner " + scannerName);
        throw new UnknownScannerException(
          "Unknown scanner '" + scannerName + "'. This can happen due to any of the following "
            + "reasons: a) Scanner id given is wrong, b) Scanner lease expired because of "
            + "long wait between consecutive client checkins, c) Server may be closing down, "
            + "d) RegionServer restart during upgrade.\nIf the issue is due to reason (b), a "
            + "possible fix would be increasing the value of"
            + "'hbase.client.scanner.timeout.period' configuration.");
      }
    }
    RegionInfo hri = rsh.s.getRegionInfo();
    // Yes, should be the same instance
    if (regionServer.getOnlineRegion(hri.getRegionName()) != rsh.r) {
      String msg = "Region has changed on the scanner " + scannerName + ": regionName="
        + hri.getRegionNameAsString() + ", scannerRegionName=" + rsh.r;
      LOG.warn(msg + ", closing...");
      scanners.remove(scannerName);
      try {
        rsh.s.close();
      } catch (IOException e) {
        LOG.warn("Getting exception closing " + scannerName, e);
      } finally {
        try {
          regionServer.getLeaseManager().cancelLease(scannerName);
        } catch (LeaseException e) {
          LOG.warn("Getting exception closing " + scannerName, e);
        }
      }
      throw new NotServingRegionException(msg);
    }
    return rsh;
  }

  /**
   * @return Pair with scannerName key to use with this new Scanner and its RegionScannerHolder
   *         value.
   */
  private Pair<String, RegionScannerHolder> newRegionScanner(ScanRequest request, HRegion region,
    ScanResponse.Builder builder) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4398b265-0fe6-34f9-ac9e-486eb4ae762a"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("nextCallSeq_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55926a4c-3bb1-326f-ada6-12d4e5edf2f9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("98e18cd9-684c-3bcf-91ce-63bdeeaa49c3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2408eb19-55fb-3055-82ad-fb11558e1728"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba1002e8-b6ea-33c4-ad2b-f7f6b60ac938"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f650dc6d-1bf8-3a3e-837e-25be7e49755b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d969d7c9-b36e-33fa-8f3a-4dace8c42484"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f23d56b7-4d28-3fd3-a68b-6b060d6ec3ff"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4444bac-ff2f-33eb-a0ea-e34621b167a3"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df2f833b-cfef-315e-9af0-08f6c18a3246"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14622e6c-a316-3761-8a55-72f79b019e25"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53a1a391-e3b5-3842-a670-f1bf4ae91a36"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b4ea407-c663-378d-a713-63fc13b07077"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("55f17d9b-384f-3e90-b7cf-4563f5c27312"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("195f67e8-bd32-3e1e-aa15-19db17e360f2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7a05bd1a-6641-331a-b186-2e565e25782c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("387e4eb1-a8a5-3f53-9a53-eb7792906965"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("nextCallSeq_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a274991e-aed6-3c22-b4a5-400ab87141e0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa5eb1c0-aa1c-3b95-a7b3-9719f7de7633"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0c0f23f4-40f9-3890-ba00-f3980a922bd6"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("f71fabb1-9185-34e6-b666-d6b0bfe4c985"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93322754-1e5b-3cb2-aae5-abaaa1b9ba30"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3d92a9cb-b322-35ca-bf8d-b20763767b00"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02fadf91-a0a5-3a09-9414-e5d2ece63ed4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("11f93e6a-171a-3463-ab2d-a47a62a86fcd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("12631b04-ae85-3654-b163-ae23781a043e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("212b8de2-e81d-3c7a-9211-367ae265a614"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f8054356-64c8-3b43-a949-d8de11c6c025"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae32583d-3233-3560-9e47-7e1f25fae1b3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("412d36bf-f522-3ebd-9d2c-cc5b793aac16"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f108938b-59a7-38c1-9029-9c0539681203"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("52997c75-8dd4-3a88-b848-e20fa02e2140"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bb2bf91f-1545-3a86-a3cb-e7605c021632"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3e77ea2-7448-31d3-a176-54894c297339"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("21a98c5b-3801-3f6a-91a7-7323443e1eb1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e1158add-ce13-3165-a5d1-9243302cf893"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7040d8b2-b749-3ee9-920f-f8cedfb3f485"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("102bdcb1-1a91-33c9-bfa6-c50457d3f432"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfaac7f1-6178-3ac6-9f73-3d41d1785dde"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("123e9eee-6f69-3261-9449-7ced1b62914a"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5eb02a5-9e56-31a0-adb5-4d6bb11d8ce6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f0759005-b7c3-3202-b96a-b232679e2dc1"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9bdf525-8dce-30ba-9c76-3054194abbfc"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32bb4775-94d8-337e-924f-892348ed7699"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("mvccReadPoint_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f11165bc-8028-3885-801e-e5adfba31eb5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("17241e75-7d31-3b93-87be-5454d9415a21"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9ca30f15-bc2e-3a9a-b641-a32dac34a6c3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("025577aa-c474-3ecd-b500-8be7b99b3934"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ffa6a3a6-9341-3c54-9be1-592877dc0610"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0951991f-8cc1-35d3-a2f1-542dd054fbe3"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limitOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4fbb966-92c7-307c-b27b-5abed706a654"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("numberOfRows_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e1741b4a-5d4b-30fa-9ca1-6c7a41f85a9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b329800-2e78-36a0-8812-5204f9b3f3c4"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bd3e8625-01b6-38d0-bca7-fc50f3c4b3c2"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ef6e263-bd00-3aa5-8bba-e45609589e77"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d242311-8be6-34b4-b1a6-425f02b585dc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bded5dba-bcf2-3f97-a27c-52ffcc2950f2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9eb4ba62-1886-3b5f-94b9-d9665b615c31"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("554d84bf-e090-356b-a1e0-36475c5a10a5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("58ccfdf7-b177-3379-aff0-e897abd62efe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2d23a488-0746-3a42-9696-8137d34321ff"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f602c19-9bd8-3197-bbf1-39d663ed2630"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cb99c95c-d9f5-3446-989d-e2279e6f300c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f1a6d7b1-1b0a-327d-af21-e4c3d217f116"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c0fbaba-4c20-3294-a515-1dfb45c7363b"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a59bcf1-5415-3fcf-8707-7a31ced5ee69"))) {
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
    ClientProtos.Scan protoScan = request.getScan();
    boolean isLoadingCfsOnDemandSet = protoScan.hasLoadColumnFamiliesOnDemand();
if(KnobRuntime.check(java.util.UUID.fromString("8f342627-6ec9-3968-9470-c0b87037ab96"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("mvccReadPoint_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af1e6ad5-fe9d-3455-b761-a5a8927e6f8d"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77e62fb5-de83-33c3-907b-ab6faaa5737e"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5916e580-bdd2-37e5-b21a-7f664ae8e2dd"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("readType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32791b1e-f063-33e0-b0ea-fd7c5f9cfa59"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ee15cec-0af0-3c30-92f9-be94101ddd93"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a53ece27-a0d8-3d53-a589-acf1293e84aa"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a737c342-3110-39f8-a378-cf18a9691d4e"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("batchSize_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4206c9f1-aaa2-3b0a-bbcf-8b52e47f03fa"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62afbb90-6a9f-34c9-9905-431e1087e58b"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("caching_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86c5ede6-0d79-31e2-b65e-ebcb8fb783af"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("cacheBlocks_");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(protoScan);
    field.set(protoScan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66612627-c2a8-3219-aa22-33620b1a0cfb"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("maxResultSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da8d4772-9989-33e9-88b6-2e2ab796bd3b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bac8b711-4c1d-317e-887e-d6d719ebc9a3"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("103fedde-2890-3838-9759-78a8aa90c951"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5355d6e9-039d-3ed8-bfb5-d42d759abe5c"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54e3112c-f0d9-3d8a-8c85-c27960576ede"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("caching_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c282a8bc-fa56-3243-862a-a9832905babf"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8a09a2b-8179-3b29-98bf-61f04c13c170"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeOffset_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c1ce3f0-d9b3-306e-b26b-592f871a3790"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc5867df-7f80-3f12-8bd3-6bef13d9ed39"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("caching_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("919ecba3-a5c0-3a76-b30c-609b0834ff90"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59b0976c-ea24-38f1-ab95-f9001d54d494"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b4aab68-7cfe-3d73-8742-4066d9d90d77"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d7689a2a-fe19-3311-8cdd-433fa730472d"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("readType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8e2737d0-e0b4-3b3c-9cb4-cf14fade2153"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("batchSize_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b891c0b2-63e3-3a0c-9f80-6631abcf734e"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("readType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c761f31d-40e7-3ad8-a254-9cb1c2cb4e78"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("storeLimit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fc48540-c312-33d5-96ec-9f35e9120a61"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("92ba393e-9190-3fb7-a8a2-25a72f8aaa0c"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50cc1623-122c-37b4-ac24-2235f8db7e11"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("consistency_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("692a06e5-db9f-3fb9-9a5d-2bd6bf678b47"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("readType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e69312d-8dfd-3ec9-8d2b-ddd3b241c4d3"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("batchSize_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e2d96fa2-55e1-3fa0-ba58-5c64ad8bd94f"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("caching_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad35ae77-7a23-37b2-8662-2b9c00ebe796"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("batchSize_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("612b818e-1b1a-3259-9580-a76ec9fd130e"))) {
try {
    java.lang.reflect.Field field = protoScan.getClass().getDeclaredField("maxVersions_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(protoScan));
    field.set(protoScan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Scan scan = ProtobufUtil.toScan(protoScan);
    // if the request doesn't set this, get the default region setting.
    if (!isLoadingCfsOnDemandSet) {
      scan.setLoadColumnFamiliesOnDemand(region.isLoadingCfsOnDemandDefault());
    }

    if (!scan.hasFamilies()) {
      // Adding all families to scanner
      for (byte[] family : region.getTableDescriptor().getColumnFamilyNames()) {
        scan.addFamily(family);
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("9c999c2c-a46f-3d61-8d8e-f12094ca7df0"))) ? ((region.getCoprocessorHost()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1806a764-5540-3049-821a-2c8d5a45f564"))) ? ((region.getCoprocessorHost()) != (null)) : (region.getCoprocessorHost() != null))))) {
      // preScannerOpen is not allowed to return a RegionScanner. Only post hook can create a
      // wrapper for the core created RegionScanner
      region.getCoprocessorHost().preScannerOpen(scan);
    }
    RegionScannerImpl coreScanner = region.getScanner(scan);
    Shipper shipper = coreScanner;
    RegionScanner scanner = coreScanner;
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("c3b9de5d-5b04-3fe8-9f31-5ba3614179c3"))) ? ((region.getCoprocessorHost()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("201e0937-2c4a-3281-a216-13ae819eda91"))) ? ((region.getCoprocessorHost()) != (null)) : (region.getCoprocessorHost() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("30b81830-2c29-340a-b9a9-b246c99dda38"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c88c72d-f79b-3298-a4a6-ca0aaee45232"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0583b201-7f8c-3182-9189-89bbaeae2adc"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2507614-ac61-3302-b749-a05688877212"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bd8525d3-18ed-3d19-8c79-25c909a88580"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e350eba3-bb6a-3b87-9305-febc818906ae"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e9d80a61-8671-3836-a654-fcde75aece2f"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86ed88f4-29b9-3077-a5a0-c41b16dca76d"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scan);
    field.set(scan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69b00954-0a50-3cf8-889e-92db7e91826e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7f1b4d32-7316-3eeb-ae81-c81a2982a553"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("404dd4e2-75bb-3462-8041-ee09890e2ff5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fd69933-4710-3eee-98f5-ada6aab10882"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f928e5f-e167-3963-9e7a-aff356af1115"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3b5a9bb-2595-3ee7-a788-bb485871884f"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd991cf8-3dea-3969-81cb-5434041cfd37"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ec1073c-58ed-3051-90b7-7ba2e5f50fd9"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d174245-93ce-3def-b8ed-f97630cb6780"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8442fa2d-8a55-3ae5-a163-9aa8b878e770"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41bd3097-3219-3eef-85d2-2ade7b7a79c4"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b5750d7-f451-3486-8e8c-ff767ccbe11a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c90439a-a7b9-3698-afd6-49544cfd185c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38a08bb7-8256-34c9-b805-ae4c048b66a7"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ee64b1c-75f6-3899-9418-7a1d25942b5c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("mvccReadPoint");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0d41182-f8d7-3a69-bf13-2a024b3558cb"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29b0ba0b-03a9-34ec-85fe-d2b2969af012"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05c9a187-9192-35e9-a4ac-dceae0b9a420"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54ee923d-796e-3602-ae13-e925e53918a6"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25341add-d4c9-3cd1-9109-039ec2feaeb7"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        scanner = region.getCoprocessorHost().postScannerOpen(scan, scanner);
      }
    } catch (Exception e) {
      // Although region coprocessor is for advanced users and they should take care of the
      // implementation to not damage the HBase system, closing the scanner on exception here does
      // not have any bad side effect, so let's do it
if(KnobRuntime.check(java.util.UUID.fromString("516009af-a666-3471-b4d1-0ccfbf267899"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("3ff21828-d7b9-3de9-9b7e-3415440d8dad"))) { closeAllScanners(); } else if (KnobRuntime.check(java.util.UUID.fromString("58052e99-e68e-3212-aca7-ff16add97c57"))) { checkOpen(); } else { scanner.close(); }
      throw e;
    }
    long scannerId = scannerIdGenerator.generateNewScannerId();
    builder.setScannerId(scannerId);
    builder.setMvccReadPoint(scanner.getMvccReadPoint());
    builder.setTtl(scannerLeaseTimeoutPeriod);
    String scannerName = toScannerName(scannerId);

    boolean fullRegionScan =
      !region.getRegionInfo().getTable().isSystemTable() && isFullRegionScan(scan, region);

    return new Pair<String, RegionScannerHolder>(scannerName,
      addScanner(scannerName, scanner, shipper, region, scan.isNeedCursorResult(), fullRegionScan));
  }

  /**
   * The returned String is used as key doing look up of outstanding Scanners in this Servers'
   * this.scanners, the Map of outstanding scanners and their current state.
   * @param scannerId A scanner long id.
   * @return The long id as a String.
   */
  private static String toScannerName(long scannerId) {
    return Long.toString(scannerId);
  }

  private void checkScanNextCallSeq(ScanRequest request, RegionScannerHolder rsh)
    throws OutOfOrderScannerNextException {
    // if nextCallSeq does not match throw Exception straight away. This needs to be
    // performed even before checking of Lease.
    // See HBASE-5974
    if (request.hasNextCallSeq()) {
      long callSeq = request.getNextCallSeq();
      if (!rsh.incNextCallSeq(callSeq)) {
        throw new OutOfOrderScannerNextException(
          "Expected nextCallSeq: " + rsh.getNextCallSeq() + " But the nextCallSeq got from client: "
            + request.getNextCallSeq() + "; request=" + TextFormat.shortDebugString(request));
      }
    }
  }

  private void addScannerLeaseBack(LeaseManager.Lease lease) {
    try {
      regionServer.getLeaseManager().addLease(lease);
    } catch (LeaseStillHeldException e) {
      // should not happen as the scanner id is unique.
      throw new AssertionError(e);
    }
  }

  // visible for testing only
  long getTimeLimit(RpcCall rpcCall, HBaseRpcController controller,
    boolean allowHeartbeatMessages) {
if(KnobRuntime.check(java.util.UUID.fromString("010e2478-5030-3caf-ba71-468fb8b69625"))) {
return 0;
}
    // Set the time limit to be half of the more restrictive timeout value (one of the
    // timeout values must be positive). In the event that both values are positive, the
    // more restrictive of the two is used to calculate the limit.
    if (allowHeartbeatMessages) {
      long now = EnvironmentEdgeManager.currentTime();
      long remainingTimeout = getRemainingRpcTimeout(rpcCall, controller, now);
      if (scannerLeaseTimeoutPeriod > 0 || remainingTimeout > 0) {
        long timeLimitDelta;
        if (scannerLeaseTimeoutPeriod > 0 && remainingTimeout > 0) {
if(KnobRuntime.check(java.util.UUID.fromString("3bf0c530-b488-3a31-9153-96775ba15d1f"))) {
remainingTimeout += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0456d8d8-56c5-3658-a8cd-a16a4e7800a9"))) {
remainingTimeout -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8f343e8-8332-39e9-a16e-ff6ff0acff72"))) {
remainingTimeout = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("98631151-b2d6-3b59-a53a-390b2e034307"))) {
remainingTimeout = 0;
}
          timeLimitDelta = Math.min(scannerLeaseTimeoutPeriod, remainingTimeout);
        } else {
          timeLimitDelta =
            scannerLeaseTimeoutPeriod > 0 ? scannerLeaseTimeoutPeriod : remainingTimeout;
        }

        // Use half of whichever timeout value was more restrictive... But don't allow
        // the time limit to be less than the allowable minimum (could cause an
        // immediate timeout before scanning any data).
        timeLimitDelta = Math.max(timeLimitDelta / 2, minimumScanTimeLimitDelta);
        return now + timeLimitDelta;
      }
    }
    // Default value of timeLimit is negative to indicate no timeLimit should be
    // enforced.
    return -1L;
  }

  private long getRemainingRpcTimeout(RpcCall call, HBaseRpcController controller, long now) {
if(KnobRuntime.check(java.util.UUID.fromString("1b7c9e4d-874f-38a9-b304-3c68ee1d4d8c"))) {
return 0;
}
    long timeout;
    if (controller != null && controller.getCallTimeout() > 0) {
      timeout = controller.getCallTimeout();
    } else if (rpcTimeout > 0) {
      timeout = rpcTimeout;
    } else {
      return -1;
    }
    if (call != null) {
      timeout -= (now - call.getReceiveTime());
    }
    // getTimeLimit ignores values <= 0, but timeout may now be negative if queue time was high.
    // return minimum value here in that case so we count this in calculating the final delta.
    return Math.max(minimumScanTimeLimitDelta, timeout);
  }

  private void checkLimitOfRows(int numOfCompleteRows, int limitOfRows, boolean moreRows,
    ScannerContext scannerContext, ScanResponse.Builder builder) {
    if (numOfCompleteRows >= limitOfRows) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Done scanning, limit of rows reached, moreRows: " + moreRows
          + " scannerContext: " + scannerContext);
      }
      if (KnobRuntime.check(java.util.UUID.fromString("38932d3e-8a4c-3c22-a4dc-7041d74980b4"))) { builder.setStale(false); } else if (KnobRuntime.check(java.util.UUID.fromString("f57370bc-557a-35e9-8c70-ae96632c47ec"))) { builder.addPartialFlagPerResult(false); } else { builder.setMoreResults(false); }
    }
  }

  // return whether we have more results in region.
  private void scan(HBaseRpcController controller, ScanRequest request, RegionScannerHolder rsh,
    long maxQuotaResultSize, int maxResults, int limitOfRows, List<Result> results,
    ScanResponse.Builder builder, RpcCall rpcCall) throws IOException {
    HRegion region = rsh.r;
    RegionScanner scanner = rsh.s;
    long maxResultSize;
    if (((KnobRuntime.check(java.util.UUID.fromString("31750c85-99b6-3016-b3a4-bb21cd66513e"))) ? ((scanner.getMaxResultSize()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("51af83f3-58f7-399d-8a7d-e296eb9b8ff7"))) ? ((EnvironmentEdgeManager.currentTime()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a81c936a-c282-3660-9f15-4a0942aee67d"))) ? ((scanner.getMaxResultSize()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d3a537d0-3ae6-3497-81fa-61be2115adb8"))) ? ((EnvironmentEdgeManager.currentTime()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ca9456fe-c638-3684-8306-f69b6878e308"))) ? ((scanner.getMaxResultSize()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a5b6baa6-5797-3886-99d4-acb79804c488"))) ? ((scanner.getMaxResultSize()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0859ac27-fa74-3e4a-974d-962730809d80"))) ? ((EnvironmentEdgeManager.currentTime()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d4f1b6c2-ed32-353f-80e8-a8bec5c222d7"))) ? ((EnvironmentEdgeManager.currentTime()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5073942e-c6a8-3d87-b5f6-8dce3e77dd42"))) ? ((scanner.getMaxResultSize()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("96bc8ba3-a11d-3ab4-98f7-7535646309ee"))) ? ((EnvironmentEdgeManager.currentTime()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("24d5ccc0-5020-3527-abda-765654fd5c00"))) ? ((EnvironmentEdgeManager.currentTime()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("85ad45b4-05a0-32e3-b9c9-517513aa50f9"))) ? ((scanner.getMaxResultSize()) > (0)) : (scanner.getMaxResultSize() > 0))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("d807cefe-d2af-35ee-ad24-7b57f8f55336"))) {
maxQuotaResultSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cb48c7a-0a4a-33ab-9a3e-67db87600d50"))) {
maxQuotaResultSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d778618b-91ac-319d-a861-472162ee3ea0"))) {
maxQuotaResultSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("327d6225-d231-3a8a-aca3-f0b8f405a506"))) {
maxQuotaResultSize += 1;
}
      maxResultSize = Math.min(scanner.getMaxResultSize(), maxQuotaResultSize);
    } else {
      maxResultSize = maxQuotaResultSize;
    }
    // This is cells inside a row. Default size is 10 so if many versions or many cfs,
    // then we'll resize. Resizings show in profiler. Set it higher than 10. For now
    // arbitrary 32. TODO: keep record of general size of results being returned.
    ArrayList<Cell> values = new ArrayList<>(32);
if(KnobRuntime.check(java.util.UUID.fromString("2fd61734-a51d-319b-8f85-24812879a455"))) {
throw new java.io.IOException("Injected exception");
}
    region.startRegionOperation(Operation.SCAN);
    long before = EnvironmentEdgeManager.currentTime();
    // Used to check if we've matched the row limit set on the Scan
    int numOfCompleteRows = 0;
    // Count of times we call nextRaw; can be > numOfCompleteRows.
    int numOfNextRawCalls = 0;
    try {
      int numOfResults = 0;
      synchronized (scanner) {
        boolean stale = (region.getRegionInfo().getReplicaId() != 0);
        boolean clientHandlesPartials =
          request.hasClientHandlesPartials() && request.getClientHandlesPartials();
        boolean clientHandlesHeartbeats =
          request.hasClientHandlesHeartbeats() && request.getClientHandlesHeartbeats();

        // On the server side we must ensure that the correct ordering of partial results is
        // returned to the client to allow them to properly reconstruct the partial results.
        // If the coprocessor host is adding to the result list, we cannot guarantee the
        // correct ordering of partial results and so we prevent partial results from being
        // formed.
        boolean serverGuaranteesOrderOfPartials = results.isEmpty();
        boolean allowPartialResults = clientHandlesPartials && serverGuaranteesOrderOfPartials;
        boolean moreRows = false;

        // Heartbeat messages occur when the processing of the ScanRequest is exceeds a
        // certain time threshold on the server. When the time threshold is exceeded, the
        // server stops the scan and sends back whatever Results it has accumulated within
        // that time period (may be empty). Since heartbeat messages have the potential to
        // create partial Results (in the event that the timeout occurs in the middle of a
        // row), we must only generate heartbeat messages when the client can handle both
        // heartbeats AND partials
        boolean allowHeartbeatMessages = clientHandlesHeartbeats && allowPartialResults;

        long timeLimit = getTimeLimit(rpcCall, controller, allowHeartbeatMessages);

        final LimitScope sizeScope =
          allowPartialResults ? LimitScope.BETWEEN_CELLS : LimitScope.BETWEEN_ROWS;
        final LimitScope timeScope =
          allowHeartbeatMessages ? LimitScope.BETWEEN_CELLS : LimitScope.BETWEEN_ROWS;

        boolean trackMetrics = request.hasTrackScanMetrics() && request.getTrackScanMetrics();

        // Configure with limits for this RPC. Set keep progress true since size progress
        // towards size limit should be kept between calls to nextRaw
        ScannerContext.Builder contextBuilder = ScannerContext.newBuilder(true);
        // maxResultSize - either we can reach this much size for all cells(being read) data or sum
        // of heap size occupied by cells(being read). Cell data means its key and value parts.
        // maxQuotaResultSize - max results just from server side configuration and quotas, without
        // user's specified max. We use this for evaluating limits based on blocks (not cells).
        // We may have accumulated some results in coprocessor preScannerNext call. Subtract any
        // cell or block size from maximum here so we adhere to total limits of request.
        // Note: we track block size in StoreScanner. If the CP hook got cells from hbase, it will
        // have accumulated block bytes. If not, this will be 0 for block size.
        long maxCellSize = maxResultSize;
        long maxBlockSize = maxQuotaResultSize;
        if (((KnobRuntime.check(java.util.UUID.fromString("da6d24ac-6e4f-32ae-b3dd-01f7e068bfbe"))) ? ((rpcCall) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ae64d322-97a4-3bd7-8832-1b713cdfee37"))) ? ((rpcCall) != (null)) : (rpcCall != null))))) {
          maxBlockSize -= rpcCall.getBlockBytesScanned();
          maxCellSize -= rpcCall.getResponseCellSize();
        }

        contextBuilder.setSizeLimit(sizeScope, maxCellSize, maxCellSize, maxBlockSize);
        contextBuilder.setBatchLimit(scanner.getBatch());
        contextBuilder.setTimeLimit(timeScope, timeLimit);
        contextBuilder.setTrackMetrics(trackMetrics);
        ScannerContext scannerContext = contextBuilder.build();
        boolean limitReached = false;
        while (numOfResults < maxResults) {
          // Reset the batch progress to 0 before every call to RegionScanner#nextRaw. The
          // batch limit is a limit on the number of cells per Result. Thus, if progress is
          // being tracked (i.e. scannerContext.keepProgress() is true) then we need to
          // reset the batch progress between nextRaw invocations since we don't want the
          // batch progress from previous calls to affect future calls
          scannerContext.setBatchProgress(0);
          assert values.isEmpty();

          // Collect values to be returned here
          moreRows = scanner.nextRaw(values, scannerContext);
          if (rpcCall == null) {
            // When there is no RpcCallContext,copy EC to heap, then the scanner would close,
            // This can be an EXPENSIVE call. It may make an extra copy from offheap to onheap
            // buffers.See more details in HBASE-26036.
            CellUtil.cloneIfNecessary(values);
          }
          numOfNextRawCalls++;

          if (!values.isEmpty()) {
            if (((KnobRuntime.check(java.util.UUID.fromString("d862430c-9582-3c43-a739-24bfacee47ed"))) ? ((limitOfRows) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("421de0c4-fcf1-367e-966e-c31f0c619360"))) ? ((limitOfRows) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("199d6eac-6c0a-3c6c-811a-9f84a3d70671"))) ? ((limitOfRows) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dbf44876-4fb7-3b32-8deb-ff3f5ce6c11c"))) ? ((limitOfRows) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dbf0c786-0296-3151-8865-6970e11e8355"))) ? ((limitOfRows) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4726aff6-912d-3b82-8e9f-4c21909f3214"))) ? ((limitOfRows) <= (0)) : (limitOfRows > 0))))))))))))) {
              // First we need to check if the last result is partial and we have a row change. If
              // so then we need to increase the numOfCompleteRows.
              if (results.isEmpty()) {
                if (
                  rsh.rowOfLastPartialResult != null
                    && !CellUtil.matchingRows(values.get(0), rsh.rowOfLastPartialResult)
                ) {
                  numOfCompleteRows++;
                  checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext,
                    builder);
                }
              } else {
                Result lastResult = results.get(results.size() - 1);
                if (
                  lastResult.mayHaveMoreCellsInRow()
                    && !CellUtil.matchingRows(values.get(0), lastResult.getRow())
                ) {
                  numOfCompleteRows++;
if(KnobRuntime.check(java.util.UUID.fromString("02254b47-66ce-35a1-8acc-32b1ad41e55a"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb13484f-4958-3608-a648-fbb8ff8b7662"))) {
numOfCompleteRows *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("03aa3986-aada-3984-877a-42e19bd85868"))) {
numOfCompleteRows -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dfa13095-1d9f-3870-8a14-d14f01842a5c"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b5b1e88-96e8-36eb-8a36-ec4a72b56624"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13c81c2e-26cc-3ca9-abc7-1b58007a9cc2"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("038ef8c8-b78c-308c-98a2-13fb120a1a5a"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83d4566c-8461-3a5a-b575-b319e5dd0a73"))) {
limitOfRows /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3977f6e9-bc75-312c-8898-ae06f85390b2"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4cbcde1f-24db-3dcb-addb-5966232a966a"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61c62a04-8dab-3bb6-94d5-e5ca145c0c99"))) {
limitOfRows = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("79a58a47-33b3-3fdf-b926-27d8a4afee7a"))) {
numOfCompleteRows = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("73d0f2ab-d452-3034-b635-88daf3024fc9"))) {
numOfCompleteRows /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("25a51f4a-d83b-3618-bded-cdca0e7e9011"))) {
numOfCompleteRows = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a8c3a40-26fa-3784-8c80-34ffe37c30a8"))) {
moreRows = !moreRows;
}
if(KnobRuntime.check(java.util.UUID.fromString("87c1ac4b-b753-3c38-b7a3-bc086f8ee584"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3107e1bd-18ef-3b84-a8fc-59c8753fc85c"))) {
limitOfRows += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("be670aa4-4563-3d0d-8cec-fa60e9a26e31"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("089cf319-f029-37c4-b2ee-fa39c96a123e"))) {
limitOfRows -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8289e07-ddc6-3d1e-9651-19280e2d22df"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dce61384-1e2a-371a-b4b9-d686feb358db"))) {
moreRows = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("26ce7989-bf34-3c4b-b7c2-a81b73e93065"))) {
limitOfRows = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("268996ad-5d6c-30e1-8d25-62aac0a9bcd2"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b47574a-8160-36e1-86c5-fbbd9ee7eade"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("mvccReadPoint_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("891f5b51-0eb4-3cf4-8614-a53fa05b47d5"))) {
limitOfRows *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d6adf8b-c946-32de-9176-f067a529be2b"))) {
moreRows = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("66d81f93-7565-350b-9bb1-f64fc6aff88c"))) {
numOfCompleteRows += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8567ecc-030e-3622-a33f-44fc5a616ff6"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
                  checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext,
                    builder);
                }
              }
              if (((KnobRuntime.check(java.util.UUID.fromString("dc03d4ca-5500-3f92-be27-956e1f8cc309"))) ? (builder.hasMoreResults()) : (((KnobRuntime.check(java.util.UUID.fromString("697dfdb4-22ef-37cb-a299-6dd97d49b8f5"))) ? ((builder.hasMoreResults()) && (!builder.getMoreResults())) : (((KnobRuntime.check(java.util.UUID.fromString("40b0b3bf-df49-3b53-abef-b86707bfbdfa"))) ? (builder.getMoreResults()) : (((KnobRuntime.check(java.util.UUID.fromString("9b8c63c7-04e6-3c7d-9c14-2a4ee2bcb5b6"))) ? (!builder.getMoreResults()) : (((KnobRuntime.check(java.util.UUID.fromString("b2c51371-cabf-3f69-87db-1299d236f0ed"))) ? ((builder.getMoreResults()) && (!builder.getMoreResults())) : (((KnobRuntime.check(java.util.UUID.fromString("66e31e6d-500f-38c6-bf73-3438b194e3ca"))) ? ((builder.getMoreResults()) || (!builder.getMoreResults())) : (((KnobRuntime.check(java.util.UUID.fromString("2e67c4f0-523a-34f4-bb02-31dbb9e84ce1"))) ? ((builder.hasMoreResults()) || (!builder.getMoreResults())) : (builder.hasMoreResults() && !builder.getMoreResults()))))))))))))))) {
                break;
              }
            }
            boolean mayHaveMoreCellsInRow = scannerContext.mayHaveMoreCellsInRow();
            Result r = Result.create(values, null, stale, mayHaveMoreCellsInRow);
            results.add(r);
            numOfResults++;
            if (((KnobRuntime.check(java.util.UUID.fromString("54e76ced-805d-31ae-832e-11d12d0659bd"))) ? ((!mayHaveMoreCellsInRow) || ((limitOfRows) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9f0c656b-d30d-3913-bf13-e058f227c41f"))) ? ((!mayHaveMoreCellsInRow) || ((limitOfRows) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("acb37a08-e980-369f-981f-a0955982c950"))) ? ((limitOfRows) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("38104151-234a-34e9-b208-c1ab1a3a35d5"))) ? ((!mayHaveMoreCellsInRow) || ((limitOfRows) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e1c67bf5-d96a-33d9-a84f-ba5466e3bc5f"))) ? ((limitOfRows) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1f0482bf-3c0d-3c4b-ab38-b1e7828038db"))) ? ((!mayHaveMoreCellsInRow) || (limitOfRows > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1782fc5e-b03d-35e6-a293-86c3e402863f"))) ? ((limitOfRows) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7e0c8d28-0612-33c8-a14e-9a1c1dd75f8a"))) ? ((!mayHaveMoreCellsInRow) || ((limitOfRows) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("25c316f4-a6be-3a48-9ec0-141adf580e60"))) ? ((!mayHaveMoreCellsInRow) || ((limitOfRows) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("98db5cb1-b0cc-38ff-8f40-522f89a348d7"))) ? ((!mayHaveMoreCellsInRow) && ((limitOfRows) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6381ac04-ff24-3351-ba0e-7a9d43a0d039"))) ? ((!mayHaveMoreCellsInRow) && ((limitOfRows) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("16026641-8d6f-38e0-9cea-e0bcbb26b136"))) ? (!mayHaveMoreCellsInRow) : (((KnobRuntime.check(java.util.UUID.fromString("b2f7c22a-b89f-393e-9df5-2c96ada67502"))) ? ((!mayHaveMoreCellsInRow) && ((limitOfRows) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ef4e8303-e76c-3f91-8e7b-096bb8e1024d"))) ? ((limitOfRows) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dc78216a-c516-3246-8c96-34dbc987221e"))) ? ((!mayHaveMoreCellsInRow) && ((limitOfRows) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1cbe2ab8-8624-36a4-9a4f-f25b636063a8"))) ? ((!mayHaveMoreCellsInRow) && (limitOfRows > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1dd24283-ee03-3ad7-b43d-49d8815cf4bf"))) ? ((limitOfRows) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c65034f0-55f9-3332-9b9c-f63840a47d18"))) ? ((!mayHaveMoreCellsInRow) && ((limitOfRows) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("11bef568-7810-3609-aee4-fc42bcb52da2"))) ? ((!mayHaveMoreCellsInRow) || ((limitOfRows) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a51936e6-af61-3d24-a8de-69366ecd8a36"))) ? ((!mayHaveMoreCellsInRow) && ((limitOfRows) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7d9f180a-32e1-3ae5-8617-47185ba52212"))) ? ((limitOfRows) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bf7caf61-1f6b-37ba-b062-7468bcebb7dd"))) ? (limitOfRows > 0) : (!mayHaveMoreCellsInRow && limitOfRows > 0))))))))))))))))))))))))))))))))))))))))))))) {
              numOfCompleteRows++;
if(KnobRuntime.check(java.util.UUID.fromString("af031e08-ff41-31ce-a480-9906d4c1c1c0"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61cae337-ebe7-3462-8469-14b0b7c78c79"))) {
limitOfRows *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("de45a28f-8b37-3493-8299-64a5c3389764"))) {
numOfCompleteRows *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("05b141be-4694-3699-b5a8-f1dab02ecebe"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68546759-a580-393d-a61a-e6557583b9ef"))) {
moreRows = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("025add71-9002-3e68-830a-0944651aab9e"))) {
limitOfRows = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9170fc9-0c47-38d9-8fb7-7413b712b652"))) {
moreRows = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("1487d2ba-7e77-3210-90a7-75cdf3cd0763"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e545d34f-1db2-3c16-85b9-b96cbd227d92"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55529aab-7950-38d9-88e9-1505ecded5dc"))) {
moreRows = !moreRows;
}
if(KnobRuntime.check(java.util.UUID.fromString("81ce4eea-a0d2-348f-89ac-2bccbfedfc34"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("mvccReadPoint_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bd914f85-a657-36d7-8ea6-a37d6abec584"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f88e030f-b36f-3891-afd3-6ed3cf338fad"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("scannerId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d870b536-efe1-3e0b-a0ac-b9b6f2bf6b37"))) {
limitOfRows += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9b4d0f41-f2d9-3fbd-ac87-d02266eca2ca"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6d0e36e-8020-3c7c-91f3-3628754f4f85"))) {
numOfCompleteRows = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d82fd15c-92be-3d91-9150-7377f2b6513e"))) {
numOfCompleteRows = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6635a647-2d0e-3826-a5d6-7863731713eb"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30fdb344-203f-30b8-b29f-4706052d3106"))) {
limitOfRows /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f1ac522-2019-3536-941d-5c5d27767c8a"))) {
limitOfRows = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("023932d3-01a6-3f1d-8905-9953bdb563aa"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e37f032b-9d90-398b-8544-32abc5456980"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20ea792c-3fba-3ef0-b302-a69add4e7ee0"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6f96b1d-c48f-3545-b2e8-0a18a5538aed"))) {
numOfCompleteRows += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("345ecb04-cb32-3711-9c88-3404bc917782"))) {
numOfCompleteRows /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1042ca6b-d939-3340-a879-d9c9340c9519"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c16b88ef-22b8-3de6-973e-c7f2add0b8db"))) {
numOfCompleteRows -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("099a7280-035d-3ab2-810e-82b01ba3c3a5"))) {
limitOfRows -= 1;
}
              if (KnobRuntime.check(java.util.UUID.fromString("474b4984-8d91-3ea3-913f-9ce0a9adb568"))) { checkLimitOfRows(32, limitOfRows, moreRows, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("05235550-c99e-3b8b-a2f1-31b525632255"))) { checkLimitOfRows(numOfCompleteRows, limitOfRows, !moreRows, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("e94b442b-4c55-3020-a633-7f289b0ca311"))) { checkLimitOfRows(numOfCompleteRows++, limitOfRows, moreRows, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("aa74b8b7-7b0c-3f1b-86d8-f8e614e4e295"))) { checkLimitOfRows(numOfCompleteRows, limitOfRows, false, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("ef57d882-b5ef-382c-9d8a-cbca7d874de4"))) { checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext, builder.setHeartbeatMessage(true)); } else if (KnobRuntime.check(java.util.UUID.fromString("0d92f337-e2c6-3864-ad7f-db30025b25a5"))) { checkLimitOfRows(1, limitOfRows, moreRows, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("69c25753-bfd5-3cb5-b3c1-30d9815ef5e9"))) { checkLimitOfRows(numOfCompleteRows, 32, moreRows, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("bc7673e7-baca-3854-93ff-9c708bb3ac4d"))) { checkLimitOfRows(numOfCompleteRows, 1, moreRows, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("0e21045e-d9c0-3d3a-aba0-df09c87834a4"))) { checkLimitOfRows(numOfCompleteRows, 0, moreRows, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("f9e5037f-94e4-3cee-95df-ebd36ab03f54"))) { checkLimitOfRows(numOfCompleteRows, limitOfRows, true, scannerContext, builder); } else if (KnobRuntime.check(java.util.UUID.fromString("8dadd331-513b-35ef-b5e4-c6446f742451"))) { checkLimitOfRows(0, limitOfRows, moreRows, scannerContext, builder); } else { checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext, builder); }
              if (((KnobRuntime.check(java.util.UUID.fromString("c184b43a-5fde-3df3-9be3-523877139193"))) ? ((builder.getMoreResults()) || (!builder.getMoreResults())) : (((KnobRuntime.check(java.util.UUID.fromString("0d20b1f0-e621-3845-bf67-d257d967df53"))) ? (builder.getMoreResults()) : (((KnobRuntime.check(java.util.UUID.fromString("f7f48ed7-812d-3f7d-8621-c54c36f8dd94"))) ? (builder.hasMoreResults()) : (((KnobRuntime.check(java.util.UUID.fromString("4cc5ad6c-2848-357b-b879-6eca54dffcd6"))) ? ((builder.hasMoreResults()) || (!builder.getMoreResults())) : (((KnobRuntime.check(java.util.UUID.fromString("ccb5d121-65de-38e1-b93f-0c5e7b97ecdc"))) ? ((builder.hasMoreResults()) && (!builder.getMoreResults())) : (((KnobRuntime.check(java.util.UUID.fromString("8bb77e95-d9c4-3da8-8bc2-be76ddbfdd88"))) ? ((builder.getMoreResults()) && (!builder.getMoreResults())) : (((KnobRuntime.check(java.util.UUID.fromString("ec7714bb-7b06-3558-8e45-da8ff2a60032"))) ? (!builder.getMoreResults()) : (builder.hasMoreResults() && !builder.getMoreResults()))))))))))))))) {
                break;
              }
            }
          } else if (!moreRows && !results.isEmpty()) {
            // No more cells for the scan here, we need to ensure that the mayHaveMoreCellsInRow of
            // last result is false. Otherwise it's possible that: the first nextRaw returned
            // because BATCH_LIMIT_REACHED (BTW it happen to exhaust all cells of the scan),so the
            // last result's mayHaveMoreCellsInRow will be true. while the following nextRaw will
            // return with moreRows=false, which means moreResultsInRegion would be false, it will
            // be a contradictory state (HBASE-21206).
            int lastIdx = results.size() - 1;
            Result r = results.get(lastIdx);
            if (r.mayHaveMoreCellsInRow()) {
              results.set(lastIdx, Result.create(r.rawCells(), r.getExists(), r.isStale(), false));
            }
          }
          boolean sizeLimitReached = scannerContext.checkSizeLimit(LimitScope.BETWEEN_ROWS);
          boolean timeLimitReached = scannerContext.checkTimeLimit(LimitScope.BETWEEN_ROWS);
          boolean resultsLimitReached = numOfResults >= maxResults;
          limitReached = sizeLimitReached || timeLimitReached || resultsLimitReached;

          if (((KnobRuntime.check(java.util.UUID.fromString("c36a8e15-1e1e-3e92-a95d-5e0aafc3794c"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("0598e6cb-05eb-3b40-b443-ec1300641533"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("e13c6595-a873-3a28-bfb3-ffdea1e86dee"))) ? ((limitReached) && (!moreRows)) : (((KnobRuntime.check(java.util.UUID.fromString("55cc9e7a-cb33-3bb8-9887-dafd8644ccde"))) ? ((!limitReached) || (!moreRows)) : (((KnobRuntime.check(java.util.UUID.fromString("55f2043e-7b37-3ab5-a316-058be0f66af7"))) ? ((!limitReached) && (!moreRows)) : (((KnobRuntime.check(java.util.UUID.fromString("7b5c9b93-aa23-3476-95e9-5d636cf60816"))) ? ((true) && (!moreRows)) : (((KnobRuntime.check(java.util.UUID.fromString("05b7aef4-0f42-37de-8f70-985b15569a48"))) ? (!moreRows) : (((KnobRuntime.check(java.util.UUID.fromString("b880b3ee-9747-37f3-9bc8-d8ec9af28cd6"))) ? ((false) && (!moreRows)) : (((KnobRuntime.check(java.util.UUID.fromString("7e66c129-2f81-3e05-91ba-4a2a3b9752e4"))) ? ((limitReached) || (!moreRows)) : (((KnobRuntime.check(java.util.UUID.fromString("5978a186-ab6c-3f96-8d0a-398967d93640"))) ? (limitReached) : (((KnobRuntime.check(java.util.UUID.fromString("19740678-ed9d-37c8-842e-a6b095cc1c5b"))) ? (!limitReached) : (((KnobRuntime.check(java.util.UUID.fromString("5c830f9a-4cd7-3c7d-9afc-91ac85ae26a2"))) ? ((true) || (!moreRows)) : (((KnobRuntime.check(java.util.UUID.fromString("9382f578-400a-3c36-bfa4-4738d750920c"))) ? ((false) || (!moreRows)) : (limitReached || !moreRows))))))))))))))))))))))))))) {
            // With block size limit, we may exceed size limit without collecting any results.
            // In this case we want to send heartbeat and/or cursor. We don't want to send heartbeat
            // or cursor if results were collected, for example for cell size or heap size limits.
            boolean sizeLimitReachedWithoutResults = sizeLimitReached && results.isEmpty();
            // We only want to mark a ScanResponse as a heartbeat message in the event that
            // there are more values to be read server side. If there aren't more values,
            // marking it as a heartbeat is wasteful because the client will need to issue
            // another ScanRequest only to realize that they already have all the values
            if (moreRows && (timeLimitReached || sizeLimitReachedWithoutResults)) {
              // Heartbeat messages occur when the time limit has been reached, or size limit has
              // been reached before collecting any results. This can happen for heavily filtered
              // scans which scan over too many blocks.
              builder.setHeartbeatMessage(true);
              if (rsh.needCursor) {
                Cell cursorCell = scannerContext.getLastPeekedCell();
                if (cursorCell != null) {
                  builder.setCursor(ProtobufUtil.toCursor(cursorCell));
                }
              }
            }
            break;
          }
          values.clear();
        }
        if (rpcCall != null) {
          if (KnobRuntime.check(java.util.UUID.fromString("556ae5a9-8c3a-375a-932f-92b67d66a8c1"))) { rpcCall.incrementResponseCellSize(EnvironmentEdgeManager.currentTime()); } else { rpcCall.incrementResponseCellSize(scannerContext.getHeapSizeProgress()); }
        }
if(KnobRuntime.check(java.util.UUID.fromString("679f3c79-ca20-3c4c-ba97-9aba98c91bd3"))) {
moreRows = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9e6cbbb8-19c3-3abe-9208-92c1a0df7ea4"))) {
moreRows = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ec14f5d8-68d4-37c1-a1dd-ad4ce84b0ef6"))) {
moreRows = !moreRows;
}
        if (KnobRuntime.check(java.util.UUID.fromString("9f3050e6-6c42-3daa-8800-d7e4f1da4125"))) { builder.addPartialFlagPerResult(!moreRows); } else if (KnobRuntime.check(java.util.UUID.fromString("8705d587-6c27-376f-b668-a92b8b7f3ac6"))) { builder.setStale(!moreRows); } else if (KnobRuntime.check(java.util.UUID.fromString("adf833b0-c6bb-3035-a88c-cc6eb0781cd3"))) { builder.setStale(true); } else if (KnobRuntime.check(java.util.UUID.fromString("ce25d712-01cd-34e2-b27b-11fd7b2d4eb7"))) { builder.setMoreResults(false); } else if (KnobRuntime.check(java.util.UUID.fromString("8f2157b3-31e7-3ed9-b66a-6e25fc5ec4e2"))) { builder.setMoreResults(true); } else if (KnobRuntime.check(java.util.UUID.fromString("d8e85e8f-e995-3bd4-aa33-ecaa5cd03fe7"))) { builder.setMoreResults(!moreRows); } else if (KnobRuntime.check(java.util.UUID.fromString("1cc78140-750d-3a17-a262-631f30b03f5e"))) { builder.setStale(false); } else if (KnobRuntime.check(java.util.UUID.fromString("c5203e61-38f3-3390-9970-e5858641dd88"))) { builder.addPartialFlagPerResult(true); } else if (KnobRuntime.check(java.util.UUID.fromString("75358e7b-a504-3998-b63d-119fdc7deec2"))) { builder.addPartialFlagPerResult(false); } else { builder.setMoreResultsInRegion(moreRows); }
        // Check to see if the client requested that we track metrics server side. If the
        // client requested metrics, retrieve the metrics from the scanner context.
        if (((KnobRuntime.check(java.util.UUID.fromString("588b2e3c-bba0-3591-a3c7-b0f19d6d11a6"))) ? (!trackMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("ae1cf9f5-6b0c-37e7-a6a3-8813a96c3415"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("6b592027-6553-32e0-8fe3-63665bf167b8"))) ? (true) : (trackMetrics))))))) {
          // rather than increment yet another counter in StoreScanner, just set the value here
          // from block size progress before writing into the response
          scannerContext.getMetrics().countOfBlockBytesScanned
            .set(scannerContext.getBlockSizeProgress());
          if (rpcCall != null) {
            scannerContext.getMetrics().fsReadTime.set(rpcCall.getFsReadTime());
          }
          Map<String, Long> metrics = scannerContext.getMetrics().getMetricsMap();
          ScanMetrics.Builder metricBuilder = ScanMetrics.newBuilder();
          NameInt64Pair.Builder pairBuilder = NameInt64Pair.newBuilder();

          for (Entry<String, Long> entry : metrics.entrySet()) {
            pairBuilder.setName(entry.getKey());
            pairBuilder.setValue(entry.getValue());
            metricBuilder.addMetrics(pairBuilder.build());
          }

          builder.setScanMetrics(metricBuilder.build());
        }
      }
    } finally {
if(KnobRuntime.check(java.util.UUID.fromString("cb2a25e7-4c7d-3fe4-88d5-bf646d6e7052"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("74a6c2fe-d859-35ec-8bf2-cc8cce91de60"))) { closeAllScanners(); } else if (KnobRuntime.check(java.util.UUID.fromString("2eb65553-bdb6-3635-8707-3c1a1f6556dd"))) { checkOpen(); } else { region.closeRegionOperation(); }
      // Update serverside metrics, even on error.
      long end = EnvironmentEdgeManager.currentTime();
      long responseCellSize = 0;
      long blockBytesScanned = 0;
      if (rpcCall != null) {
        responseCellSize = rpcCall.getResponseCellSize();
        blockBytesScanned = rpcCall.getBlockBytesScanned();
if(KnobRuntime.check(java.util.UUID.fromString("853271c9-faf5-3397-bfc1-1fab6c631b23"))) {
blockBytesScanned -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("20a3f8fc-3750-381a-af80-799bce75a554"))) {
blockBytesScanned += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("51f4addc-69af-3442-aa5b-6a2c6ad4adaf"))) {
blockBytesScanned = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cab947ed-fad7-375e-9fa6-ce1a5539f105"))) {
blockBytesScanned = -1;
}
        rsh.updateBlockBytesScanned(blockBytesScanned);
      }
      region.getMetrics().updateScanTime(end - before);
      final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
      if (metricsRegionServer != null) {
        metricsRegionServer.updateScan(region, end - before, responseCellSize, blockBytesScanned);
        metricsRegionServer.updateReadQueryMeter(region, numOfNextRawCalls);
      }
    }
    // coprocessor postNext hook
    if (region.getCoprocessorHost() != null) {
      region.getCoprocessorHost().postScannerNext(scanner, results, maxResults, true);
    }
  }

  /**
   * Scan data in a table.
   * @param controller the RPC controller
   * @param request    the scan request
   */
  @Override
  public ScanResponse scan(final RpcController controller, final ScanRequest request)
    throws ServiceException {
    if (controller != null && !(controller instanceof HBaseRpcController)) {
      throw new UnsupportedOperationException(
        "We only do " + "HBaseRpcControllers! FIX IF A PROBLEM: " + controller);
    }
    if (!request.hasScannerId() && !request.hasScan()) {
      throw new ServiceException(
        new DoNotRetryIOException("Missing required input: scannerId or scan"));
    }
    try {
      checkOpen();
    } catch (IOException e) {
      if (request.hasScannerId()) {
        String scannerName = toScannerName(request.getScannerId());
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Server shutting down and client tried to access missing scanner " + scannerName);
        }
        final LeaseManager leaseManager = regionServer.getLeaseManager();
        if (leaseManager != null) {
          try {
            if (KnobRuntime.check(java.util.UUID.fromString("20717ccc-c31c-3630-8a44-fec3a3a59a2c"))) { rpcPreCheck(scannerName); } else { leaseManager.cancelLease(scannerName); }
          } catch (LeaseException le) {
            // No problem, ignore
            if (((KnobRuntime.check(java.util.UUID.fromString("78702729-6236-3203-8983-51154cc38801"))) ? (LOG.isDebugEnabled()) : (LOG.isTraceEnabled()))) {
              LOG.trace("Un-able to cancel lease of scanner. It could already be closed.");
            }
          }
        }
      }
      throw new ServiceException(e);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("dc98f340-cad4-3b92-aaa4-9b09797afa02"))) { closeAllScanners(); } else { requestCount.increment(); }
    if (KnobRuntime.check(java.util.UUID.fromString("7aa16c14-e37d-3486-88a2-b608340bfbb3"))) { closeAllScanners(); } else { rpcScanRequestCount.increment(); }
    RegionScannerContext rsx;
    ScanResponse.Builder builder = ScanResponse.newBuilder();
    try {
      rsx = checkQuotaAndGetRegionScannerContext(request, builder);
    } catch (IOException e) {
      if (((KnobRuntime.check(java.util.UUID.fromString("9d4db94a-b425-38cb-8a14-38388ede6709"))) ? ((e) == (SCANNER_ALREADY_CLOSED)) : (((KnobRuntime.check(java.util.UUID.fromString("c0645172-b85c-3832-9207-a11a98f936e1"))) ? ((e) != (SCANNER_ALREADY_CLOSED)) : (e == SCANNER_ALREADY_CLOSED))))) {
        // Now we will close scanner automatically if there are no more results for this region but
        // the old client will still send a close request to us. Just ignore it and return.
        return builder.build();
      }
      throw new ServiceException(e);
    }
    String scannerName = rsx.scannerName;
    RegionScannerHolder rsh = rsx.holder;
    OperationQuota quota = rsx.quota;
    if (rsh.fullRegionScan) {
      rpcFullScanRequestCount.increment();
    }
    HRegion region = rsh.r;
    LeaseManager.Lease lease;
    try {
      // Remove lease while its being processed in server; protects against case
      // where processing of request takes > lease expiration time. or null if none found.
      lease = regionServer.getLeaseManager().removeLease(scannerName);
    } catch (LeaseException e) {
      throw new ServiceException(e);
    }
    if (request.hasRenew() && request.getRenew()) {
      // add back and return
      addScannerLeaseBack(lease);
      try {
        checkScanNextCallSeq(request, rsh);
      } catch (OutOfOrderScannerNextException e) {
        throw new ServiceException(e);
      }
      return builder.build();
    }
    try {
      checkScanNextCallSeq(request, rsh);
    } catch (OutOfOrderScannerNextException e) {
      addScannerLeaseBack(lease);
      throw new ServiceException(e);
    }
    // Now we have increased the next call sequence. If we give client an error, the retry will
    // never success. So we'd better close the scanner and return a DoNotRetryIOException to client
    // and then client will try to open a new scanner.
    boolean closeScanner = request.hasCloseScanner() ? request.getCloseScanner() : false;
    int rows; // this is scan.getCaching
    if (((KnobRuntime.check(java.util.UUID.fromString("12607fa9-6584-3259-9915-e08b5e1dc6cd"))) ? (request.hasNextCallSeq()) : (((KnobRuntime.check(java.util.UUID.fromString("c6c54140-7a00-33d6-9637-4c60ac66cc18"))) ? (request.hasRegion()) : (request.hasNumberOfRows()))))) {
      rows = request.getNumberOfRows();
    } else {
      rows = closeScanner ? 0 : 1;
    }
    RpcCall rpcCall = RpcServer.getCurrentCall().orElse(null);
    // now let's do the real scan.
    long maxQuotaResultSize = Math.min(maxScannerResultSize, quota.getMaxResultSize());
    RegionScanner scanner = rsh.s;
    // this is the limit of rows for this scan, if we the number of rows reach this value, we will
    // close the scanner.
    int limitOfRows;
    if (((KnobRuntime.check(java.util.UUID.fromString("d7813441-29d3-3c95-9db5-c1eeca497e17"))) ? (request.hasRegion()) : (((KnobRuntime.check(java.util.UUID.fromString("0781e681-8a19-3d07-b35e-b2977ccaa670"))) ? (request.hasNextCallSeq()) : (request.hasLimitOfRows()))))) {
      limitOfRows = request.getLimitOfRows();
    } else {
      limitOfRows = -1;
    }
    boolean scannerClosed = false;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("aa046418-3c06-3780-832d-4f2000f6ecb0"))) {
rows += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e9f2dfd-53e8-34ed-b66b-e70837c77817"))) {
rows /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3903a533-1286-3fb6-8aaa-3241a1f1b13f"))) {
rows = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("20a6b15f-1bf5-3ad1-aa53-bdfac4441d68"))) {
rows *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("af0f9aee-3d16-3202-8533-068cb04f0d27"))) {
rows -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b94cf819-01d4-3de4-a636-75a7ab45cd21"))) {
rows = -1;
}
      List<Result> results = new ArrayList<>(Math.min(rows, 512));
      if (((KnobRuntime.check(java.util.UUID.fromString("b899b983-60cf-31d3-8aca-8028441c7c72"))) ? ((rows) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("eba1abca-2080-3c88-944d-c5f68dc8160c"))) ? ((rows) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ca3f2731-daee-3d1d-a809-8573fa03b1f3"))) ? ((rows) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a82513ed-799c-34e4-83e0-bbd4a12c6f2b"))) ? ((rows) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8510bbf5-8247-3e79-8266-f3d99368f23e"))) ? ((rows) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9707bcc8-ecf3-3d26-bf2e-d1efd62ff638"))) ? ((rows) == (0)) : (rows > 0))))))))))))) {
        boolean done = false;
        // Call coprocessor. Get region info from scanner.
        if (region.getCoprocessorHost() != null) {
          Boolean bypass = region.getCoprocessorHost().preScannerNext(scanner, results, rows);
          if (!results.isEmpty()) {
            for (Result r : results) {
              // add cell size from CP results so we can track response size and update limits
              // when calling scan below if !done. We'll also have tracked block size if the CP
              // got results from hbase, since StoreScanner tracks that for all calls automatically.
              addSize(rpcCall, r);
            }
          }
          if (((KnobRuntime.check(java.util.UUID.fromString("8a8abb2f-f784-3843-9d4b-13a4bae279dc"))) ? (bypass.booleanValue()) : (((KnobRuntime.check(java.util.UUID.fromString("1f89422f-a0be-368a-b11f-afa9579c30bf"))) ? (((bypass) == (null)) && (bypass.booleanValue())) : (((KnobRuntime.check(java.util.UUID.fromString("c3422110-ce3a-3835-8290-1aa3ef00fb29"))) ? (((bypass) == (null)) || (bypass.booleanValue())) : (((KnobRuntime.check(java.util.UUID.fromString("3a37079c-e60b-34a7-94e0-8d2aa1d30224"))) ? (((bypass) != (null)) && (bypass.booleanValue())) : (((KnobRuntime.check(java.util.UUID.fromString("6aab63a8-62e6-3ffb-90b9-f08fc0990426"))) ? ((bypass) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fde2b5db-6b95-37e6-87f0-fc2327c75720"))) ? ((bypass != null) && (bypass.booleanValue())) : (((KnobRuntime.check(java.util.UUID.fromString("d62c12eb-877f-3eee-b44f-5aa950d09454"))) ? ((bypass) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7adc0a7a-ea13-3e3b-9503-937b7ccbc3c1"))) ? (bypass != null) : (((KnobRuntime.check(java.util.UUID.fromString("412bcd72-3008-3bb6-9f40-b75cd5a869d0"))) ? (((bypass) != (null)) || (bypass.booleanValue())) : (((KnobRuntime.check(java.util.UUID.fromString("f7f6f025-3067-3c68-94cb-64da1c287be0"))) ? ((bypass != null) || (bypass.booleanValue())) : (bypass != null && bypass.booleanValue()))))))))))))))))))))) {
            done = true;
          }
        }
        if (!done) {
          scan((HBaseRpcController) controller, request, rsh, maxQuotaResultSize, rows, limitOfRows,
            results, builder, rpcCall);
        } else {
          builder.setMoreResultsInRegion(!results.isEmpty());
        }
      } else {
        // This is a open scanner call with numberOfRow = 0, so set more results in region to true.
        if (KnobRuntime.check(java.util.UUID.fromString("c5edb92a-f742-3e47-ab33-7d549b1b0c4f"))) { builder.setMoreResults(true); } else if (KnobRuntime.check(java.util.UUID.fromString("10e6fae2-0a8c-33ff-b1ed-5be9e5afde60"))) { builder.setStale(true); } else if (KnobRuntime.check(java.util.UUID.fromString("7ee8d2be-ac30-3bc6-843f-9ffd6859a3c8"))) { builder.addPartialFlagPerResult(true); } else { builder.setMoreResultsInRegion(true); }
      }

      quota.addScanResult(results);
      addResults(builder, results, (HBaseRpcController) controller,
        RegionReplicaUtil.isDefaultReplica(region.getRegionInfo()),
        isClientCellBlockSupport(rpcCall));
      if (scanner.isFilterDone() && results.isEmpty()) {
        // If the scanner's filter - if any - is done with the scan
        // only set moreResults to false if the results is empty. This is used to keep compatible
        // with the old scan implementation where we just ignore the returned results if moreResults
        // is false. Can remove the isEmpty check after we get rid of the old implementation.
        if (KnobRuntime.check(java.util.UUID.fromString("398bf6cf-b34f-365e-8ee2-6158498ad463"))) { builder.setStale(false); } else if (KnobRuntime.check(java.util.UUID.fromString("29e8f978-245a-3a96-b14f-c2dd769468be"))) { builder.addPartialFlagPerResult(false); } else { builder.setMoreResults(false); }
      }
      // Later we may close the scanner depending on this flag so here we need to make sure that we
      // have already set this flag.
      assert ((KnobRuntime.check(java.util.UUID.fromString("1c055d4a-69b9-33ba-ae86-8c3e93a57a65"))) ? (builder.getMoreResults()) : (((KnobRuntime.check(java.util.UUID.fromString("61e646c9-d720-32e4-9b50-0f9747fc4572"))) ? (builder.hasMoreResults()) : (builder.hasMoreResultsInRegion()))));
      // we only set moreResults to false in the above code, so set it to true if we haven't set it
      // yet.
      if (!builder.hasMoreResults()) {
        builder.setMoreResults(true);
      }
      if (builder.getMoreResults() && builder.getMoreResultsInRegion() && !results.isEmpty()) {
        // Record the last cell of the last result if it is a partial result
        // We need this to calculate the complete rows we have returned to client as the
        // mayHaveMoreCellsInRow is true does not mean that there will be extra cells for the
        // current row. We may filter out all the remaining cells for the current row and just
        // return the cells of the nextRow when calling RegionScanner.nextRaw. So here we need to
        // check for row change.
        Result lastResult = results.get(results.size() - 1);
        if (lastResult.mayHaveMoreCellsInRow()) {
          rsh.rowOfLastPartialResult = lastResult.getRow();
        } else {
          rsh.rowOfLastPartialResult = null;
        }
      }
      if (!builder.getMoreResults() || !builder.getMoreResultsInRegion() || closeScanner) {
        scannerClosed = true;
if(KnobRuntime.check(java.util.UUID.fromString("48fe1a88-70ff-3bf5-95d1-7f5412d177e4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5470e9c-f5c2-304d-93d2-0a16815619fd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5809aecc-5539-3a1e-8ddc-9740e8353e9d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("92647466-8b14-3d4e-b1fc-9a786a0e3de0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("be6ca22d-b292-3dd6-bfe5-5a4ba674c925"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d191d37e-a455-3938-b9c8-12d923dccaa9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("daeb2157-01e4-3f71-84c7-214261528087"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("dca3a8e4-bb0a-3da1-8042-0d719ad3b38c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f9e7727-8dd7-30f9-9045-47e972ffba5a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ddf21d63-3b85-3a72-881d-71eff556ab94"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("743d67fd-4daf-32b5-8e14-356c4d7b0bbb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4689ff2a-b27d-3c66-ab27-e3e5595981b4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("58ff22e0-e7be-349a-a2e9-b331155d8f68"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("430c1f82-25a5-3eda-832f-9ddd9f006860"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("71b48a3c-1a90-3294-9aa6-ec48f12d68e3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("54270ed5-6e3c-3f5e-8cf6-ed56a5bc79ac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("826f6363-c722-3400-90c3-db1a79f1584a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c3d80c5d-9870-3c33-a367-ea805022e975"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("220d98bf-02ab-3980-87e9-430d0a0110ea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02a0830b-4739-343f-8a1b-3011c029b96c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3cdf28a2-9a0f-364b-8959-203ed5f0dfea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4965fc5f-a0f5-3f00-bf8a-7e32e428368e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3895ff6-594e-33fc-9279-39d3d3c21531"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9281b840-b3b1-3591-9d0c-e6c4a80ff2a6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("816045a4-365a-3a7b-8b63-16bb37869ed6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("264a6e48-4566-3ce9-8b41-12347f704394"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cec2d574-4e46-3ee1-949c-7e297c770a89"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("264dd35d-e0f6-3269-83f0-f3c0af4c4b63"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("48e6ebf2-4260-3d8f-adb9-e4adc0d56931"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("69e01876-324f-3d7a-ad4c-dbb4b0a40d43"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a91037f7-890d-32a6-ac05-79be5df098ec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8d244c56-c253-3cdd-b7a6-ece7bea60ea4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9db938aa-c521-3113-8ff4-77d1b7d074ea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a6dac54d-3c8c-3dd2-9940-651f95edc6a3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8bab3ef2-1c45-3c48-af26-b88b1cfe8294"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cf166781-7cf6-3a5f-9f7d-242fe4c5df88"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ff397ac8-a36a-3ce5-8857-abff20d8a8b7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("20cf092a-c3b3-38c5-b0d1-8d73656d4402"))) {
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
        closeScanner(region, scanner, scannerName, rpcCall, false);
      }

      // There's no point returning to a timed out client. Throwing ensures scanner is closed
      if (((KnobRuntime.check(java.util.UUID.fromString("b2a590f2-6f85-380d-9902-480082f292c1"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("59d2fdb0-aa9f-3538-969e-4919aed96705"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("ae1ed96f-4e4e-34ea-aec8-f102b87b660c"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) <= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("dc5a9dfb-bcd5-3280-8612-c1774d3956b1"))) ? ((EnvironmentEdgeManager.currentTime()) == (rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("ee6190a8-a23d-3c6e-8b5d-130ad9f3c8e4"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) > (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("5c59e2c2-22e7-3b18-aeaa-1fabd6667b63"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) == (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("28e8f713-3c53-375f-bf06-b74cf0936995"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("e57154f3-9e9d-348f-a7bd-83391d1cd3e6"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("52af82ad-e4ee-3c01-a6ce-eaf52de35ace"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("266c9dfe-4f30-316f-a104-f58eae1dfe09"))) ? ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("1433d495-3506-392c-ac1a-de81a8dd0bcf"))) ? (EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline()) : (((KnobRuntime.check(java.util.UUID.fromString("1d9bdba9-b491-35da-b5d5-354714cf99b6"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("eac691cc-24a4-34fa-a9f4-188f5b27056b"))) ? (((rpcCall) != (null)) || (EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("4d50703e-979b-3747-86c4-1333f87ae0a9"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("b46e9fff-be7f-30a8-8dbc-8e7c89d4884f"))) ? (((rpcCall) == (null)) && (EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("ecdab485-a9c2-3f82-9c2f-08ad52a08830"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("5cba4514-9801-3c3a-b8e0-e85cc2b1f7bb"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("d9346bb9-2b95-3214-8887-1da435c1359b"))) ? ((rpcCall != null) && (EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("42c41014-0bcd-34fa-b381-aa5503ea5db9"))) ? (((rpcCall) == (null)) || (EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("7278fcb5-c4c3-3401-a2e6-e840bed2fe06"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("f0c19b52-49e1-3f3a-adbb-2b0f5fba98af"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) > (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("cabff2ba-2db0-3ddf-a353-71dce4e6a7df"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("efb29ee6-f198-3ccd-b1d6-0e4d61fe8c08"))) ? ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("5a3a4b95-37ca-3c96-8240-49b68eccaf40"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("3ce0e703-4bfa-3863-bb6d-e308b4a10f2d"))) ? (((rpcCall) != (null)) && (EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("8ae60138-89d8-301b-a788-8a9931bbf7d2"))) ? ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("fc1b9867-0175-3cc2-8b80-2aadc93975d0"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("8aa1c2af-09d8-3d87-a7a2-a579c6a0e889"))) ? ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("6750ce47-1bd9-3696-837a-2ce328407059"))) ? ((EnvironmentEdgeManager.currentTime()) >= (rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("d89670d4-4796-3cbc-9a96-2f0af1de6b29"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) == (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("333c1ddf-ec03-331d-9cd1-466087f84e4c"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) > (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("490e6d44-9cde-3c24-ba8f-b4f0958da8fa"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("4887e2cb-43b3-3cec-b318-cce71eb6ec90"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) == (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("6194f51c-6c0d-329d-9e57-c6663bed536e"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("0def65fa-e59a-301e-a4eb-4b86c7683f0c"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) >= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("7dc4464d-624c-3d74-a9ae-240027f8a541"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("490f7e6a-2e4a-3e68-b2f4-9f499a4d5048"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) <= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("05e90e3f-a2c3-3da0-9257-46a07e9eeed0"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) > (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("3082b1dd-601b-3942-9723-9143ff4bc744"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("8aa70833-8756-3892-ad45-79b31be4689e"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("f2a04952-4af8-316b-bf0c-5592250c9f23"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("1c620b8f-89f9-303e-8475-957e8a270d4e"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("66fbfd51-47b7-399e-b305-887293f40a5f"))) ? ((EnvironmentEdgeManager.currentTime()) > (rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("53f6428b-247d-3b62-bfc7-db1b84718ff6"))) ? ((rpcCall) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8641ed3a-424b-3c96-8fc0-d0464f314b40"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("542a4803-fd52-3b44-b708-6ae9e79700ec"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("195b9cf9-5ecc-39d2-8044-3d20a66b0824"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("30528662-7171-33ea-9f4b-6a30622884f3"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) >= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("e052a31c-ed0b-3bc0-867e-03c4f9e8287c"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) >= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("79b355df-78bb-3293-af6c-fb0d207f8a44"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("65066bfd-baf0-36db-beed-0eb4e5b5d96f"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) >= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("232b9df9-2986-3524-b9ba-c39d3292a259"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("16615921-9d36-329d-aafc-977664c6597f"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) <= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("007a50b2-bde7-33c5-a5e4-1426cc24fa5c"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("124f19c7-d612-3e0a-b774-1dba9077bbac"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) == (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("f4f5f1a1-0daf-35a8-80db-1757164b892b"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("fab013cf-8698-3bd5-b293-f1f500d2e126"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) > (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("8f528b8d-b6a7-3777-a18a-24b5973d85f8"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) <= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("1ae03412-8670-3597-bae5-992de29d80b9"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("d5b55f20-7ca9-3c58-8c74-deb6294833b4"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("ba292e16-f4b2-39fb-92ce-dcec3bd7f160"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("82ef5a0c-c7bb-385f-8db8-93d31c21c9c5"))) ? ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("1abcb675-373e-3905-8308-0a97ad41c8a3"))) ? (rpcCall != null) : (((KnobRuntime.check(java.util.UUID.fromString("b94ceb67-ffb7-361a-818f-f69379da675a"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("82e07a95-e847-301a-8839-ff1f8b681f98"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) == (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("e134d96d-72cd-3b56-bc5e-4a9b4f5a5d00"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("34f3500a-85db-35ac-8b0a-0eab365b0f98"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) > (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("c0050bd2-7939-3cc4-96da-d47664107b0b"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) <= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("d01bba0d-036b-3f2e-9f3a-2535e27b37eb"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) >= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("932091eb-eb67-353e-bae4-f40472a64d2a"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) <= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("e20549bc-3876-366d-8c00-5262d856c763"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("eed06b2d-9130-3a03-b494-6e4fa2a8edd4"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("499f3e29-8759-3d4d-affb-69f45f6246dd"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("3fce64ca-0aee-3bf1-8054-6ee9b0e12af1"))) ? ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("f23a3298-e1da-3610-877d-8131be2283e2"))) ? ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("03b166f2-8206-30a0-91cf-ec888227cd9f"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("f4f11e73-0e64-3e04-b593-055c81482c49"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) >= (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("54e79633-244f-3c40-8edd-4038042715b3"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("1f62020d-8279-3f86-8f11-c5f365afa059"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("c19d01cf-1372-3de9-9351-39a25b557e0b"))) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) == (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("2a314f37-fd1c-32e7-a11c-4290b0dc964f"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("54f032f0-a7e4-343d-b705-37cf6a3b28df"))) ? ((rpcCall != null) || (EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("a10857f0-4e2f-3f7b-8260-f938779cddee"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("2a2078fa-6250-39d9-8356-5c14bd02f5a5"))) ? ((rpcCall) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("63b7d9f3-e59f-32dc-86d5-c0c31853ebcd"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("44cea567-c2ef-3184-be61-e4e5a178a2cd"))) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("9b9f7a1f-58b0-3fca-83bb-20a74b9466f3"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("ce436850-585d-31bd-8984-1ad2455d9745"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("85dedacb-d50d-3e5b-9091-878eb31f700b"))) ? (((rpcCall) == (null)) && ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("b57ec143-cb17-3718-9989-26ae194e6b41"))) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("cbfa8b5e-0f7f-3bc6-9aa3-4d527d0c2227"))) ? ((EnvironmentEdgeManager.currentTime()) <= (rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("409eb3e5-f31c-3d12-a9db-9736d4f377b4"))) ? ((rpcCall != null) || ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("d62cd506-2851-3db7-a718-1a0ce177c0b6"))) ? ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("fbe0c00c-b7dd-3ff7-b2bc-aac043cb8565"))) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline()))) : (rpcCall != null && EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        throw new TimeoutIOException("Client deadline exceeded, cannot return results");
      }

      return builder.build();
    } catch (IOException e) {
      try {
        // scanner is closed here
        scannerClosed = true;
        // The scanner state might be left in a dirty state, so we will tell the Client to
        // fail this RPC and close the scanner while opening up another one from the start of
        // row that the client has last seen.
        closeScanner(region, scanner, scannerName, rpcCall, true);

        // If it is a DoNotRetryIOException already, throw as it is. Unfortunately, DNRIOE is
        // used in two different semantics.
        // (1) The first is to close the client scanner and bubble up the exception all the way
        // to the application. This is preferred when the exception is really un-recoverable
        // (like CorruptHFileException, etc). Plain DoNotRetryIOException also falls into this
        // bucket usually.
        // (2) Second semantics is to close the current region scanner only, but continue the
        // client scanner by overriding the exception. This is usually UnknownScannerException,
        // OutOfOrderScannerNextException, etc where the region scanner has to be closed, but the
        // application-level ClientScanner has to continue without bubbling up the exception to
        // the client. See ClientScanner code to see how it deals with these special exceptions.
        if (e instanceof DoNotRetryIOException) {
          throw e;
        }

        // If it is a FileNotFoundException, wrap as a
        // DoNotRetryIOException. This can avoid the retry in ClientScanner.
        if (e instanceof FileNotFoundException) {
          throw new DoNotRetryIOException(e);
        }

        // We closed the scanner already. Instead of throwing the IOException, and client
        // retrying with the same scannerId only to get USE on the next RPC, we directly throw
        // a special exception to save an RPC.
        if (VersionInfoUtil.hasMinimumVersion(rpcCall.getClientVersionInfo(), 1, 4)) {
          // 1.4.0+ clients know how to handle
          throw new ScannerResetException("Scanner is closed on the server-side", e);
        } else {
          // older clients do not know about SRE. Just throw USE, which they will handle
          throw new UnknownScannerException("Throwing UnknownScannerException to reset the client"
            + " scanner state for clients older than 1.3.", e);
        }
      } catch (IOException ioe) {
        throw new ServiceException(ioe);
      }
    } finally {
      if (!scannerClosed) {
        // Adding resets expiration time on lease.
        // the closeCallBack will be set in closeScanner so here we only care about shippedCallback
        if (rpcCall != null) {
          rpcCall.setCallBack(rsh.shippedCallback);
        } else {
          // If context is null,here we call rsh.shippedCallback directly to reuse the logic in
          // rsh.shippedCallback to release the internal resources in rsh,and lease is also added
          // back to regionserver's LeaseManager in rsh.shippedCallback.
          runShippedCallback(rsh);
        }
      }
      quota.close();
    }
  }

  private void runShippedCallback(RegionScannerHolder rsh) throws ServiceException {
    assert rsh.shippedCallback != null;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("70a56086-1dfe-31bf-b625-02666f2e5c83"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("d9237fd9-6069-3ad8-9e23-2ba2265bcb1b"))) { checkOpen(); } else if (KnobRuntime.check(java.util.UUID.fromString("5045fa3a-7d4e-3670-b12c-f6fcf0749a51"))) { closeAllScanners(); } else { rsh.shippedCallback.run(); }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  private void closeScanner(HRegion region, RegionScanner scanner, String scannerName,
    RpcCallContext context, boolean isError) throws IOException {
    if (region.getCoprocessorHost() != null) {
      if (region.getCoprocessorHost().preScannerClose(scanner)) {
        // bypass the actual close.
        return;
      }
    }
    RegionScannerHolder rsh = scanners.remove(scannerName);
    if (rsh != null) {
      if (context != null) {
        context.setCallBack(rsh.closeCallBack);
      } else {
        rsh.s.close();
      }
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postScannerClose(scanner);
      }
      if (!isError) {
        closedScanners.put(scannerName, rsh.getNextCallSeq());
      }
    }
  }

  @Override
  public CoprocessorServiceResponse execRegionServerService(RpcController controller,
    CoprocessorServiceRequest request) throws ServiceException {
if(KnobRuntime.check(java.util.UUID.fromString("b477984e-b20c-327b-95c7-312b1b74d752"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("882b6ccf-f127-39f4-8360-4cc984d3d5dc"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("fb51f48e-94f7-3873-88a2-f9a112b5fc55"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3f7cdc20-c3f1-3577-bd8d-7a44e572409d"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("128ae3ba-b3e3-31cf-b69f-9bfbeb18b1be"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    rpcPreCheck("execRegionServerService");
if(KnobRuntime.check(java.util.UUID.fromString("544d7333-1be7-3fab-bbda-2b19434ce7f1"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8534dc4-6bc1-3f31-accf-7a73c82a9b6c"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60950f7b-d9aa-3fb9-b5f8-7c5446db3baf"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("353e7bf4-aeec-3e47-91c7-8db61a3f56f8"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return regionServer.execRegionServerService(controller, request);
  }

  @Override
  public UpdateConfigurationResponse updateConfiguration(RpcController controller,
    UpdateConfigurationRequest request) throws ServiceException {
if(KnobRuntime.check(java.util.UUID.fromString("dff900f6-017a-3f36-aaf9-6743a3942647"))) {
return null;
}
    try {
if(KnobRuntime.check(java.util.UUID.fromString("1ec99e66-d6c0-3b23-9b60-d2c4c26b7b43"))) {
throw new java.io.IOException("Injected exception");
}
      requirePermission("updateConfiguration", Permission.Action.ADMIN);
      this.regionServer.updateConfiguration();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
    return UpdateConfigurationResponse.getDefaultInstance();
  }

  @Override
  public GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(RpcController controller,
    GetSpaceQuotaSnapshotsRequest request) throws ServiceException {
    try {
      final RegionServerSpaceQuotaManager manager = regionServer.getRegionServerSpaceQuotaManager();
      final GetSpaceQuotaSnapshotsResponse.Builder builder =
        GetSpaceQuotaSnapshotsResponse.newBuilder();
      if (manager != null) {
        final Map<TableName, SpaceQuotaSnapshot> snapshots = manager.copyQuotaSnapshots();
        for (Entry<TableName, SpaceQuotaSnapshot> snapshot : snapshots.entrySet()) {
          builder.addSnapshots(TableQuotaSnapshot.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(snapshot.getKey()))
            .setSnapshot(SpaceQuotaSnapshot.toProtoSnapshot(snapshot.getValue())).build());
        }
      }
      return builder.build();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller,
    ClearRegionBlockCacheRequest request) throws ServiceException {

    try {
      rpcPreCheck("clearRegionBlockCache");
      ClearRegionBlockCacheResponse.Builder builder = ClearRegionBlockCacheResponse.newBuilder();
      CacheEvictionStatsBuilder stats = CacheEvictionStats.builder();
      regionServer.getRegionServerCoprocessorHost().preClearRegionBlockCache();
      List<HRegion> regions = getRegions(request.getRegionList(), stats);
      for (HRegion region : regions) {
        try {
          stats = stats.append(this.regionServer.clearRegionBlockCache(region));
        } catch (Exception e) {
          stats.addException(region.getRegionInfo().getRegionName(), e);
        }
      }
      stats.withMaxCacheSize(regionServer.getBlockCache().map(BlockCache::getMaxSize).orElse(0L));
      regionServer.getRegionServerCoprocessorHost().postClearRegionBlockCache(stats.build());
      return builder.setStats(ProtobufUtil.toCacheEvictionStats(stats.build())).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private void executeOpenRegionProcedures(OpenRegionRequest request,
    Map<TableName, TableDescriptor> tdCache) {
    long masterSystemTime = request.hasMasterSystemTime() ? request.getMasterSystemTime() : -1;
    long initiatingMasterActiveTime =
      request.hasInitiatingMasterActiveTime() ? request.getInitiatingMasterActiveTime() : -1;
    for (RegionOpenInfo regionOpenInfo : request.getOpenInfoList()) {
      RegionInfo regionInfo = ProtobufUtil.toRegionInfo(regionOpenInfo.getRegion());
      TableName tableName = regionInfo.getTable();
if(KnobRuntime.check(java.util.UUID.fromString("b4d9856b-61d8-321b-bd48-432ff8344d73"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("63b4283d-8ea2-373b-9ba2-0432cbce0691"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f3240e9-5331-33ee-9bcc-b862a88f3c15"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3dfee668-65e3-3917-b7b5-8d157be97109"))) {
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
      TableDescriptor tableDesc = tdCache.get(tableName);
      if (tableDesc == null) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("c93082d2-6e7a-3879-a99c-65cdf0e95f10"))) {
throw new java.io.IOException("Injected exception");
}
          tableDesc = regionServer.getTableDescriptors().get(regionInfo.getTable());
        } catch (IOException e) {
          // Here we do not fail the whole method since we also need deal with other
          // procedures, and we can not ignore this one, so we still schedule a
          // AssignRegionHandler and it will report back to master if we still can not get the
          // TableDescriptor.
          LOG.warn("Failed to get TableDescriptor of {}, will try again in the handler",
            regionInfo.getTable(), e);
        }
        if (tableDesc != null) {
          tdCache.put(tableName, tableDesc);
        }
      }
      if (regionOpenInfo.getFavoredNodesCount() > 0) {
        regionServer.updateRegionFavoredNodesMapping(regionInfo.getEncodedName(),
          regionOpenInfo.getFavoredNodesList());
      }
      long procId = regionOpenInfo.getOpenProcId();
      if (regionServer.submitRegionProcedure(procId)) {
if(KnobRuntime.check(java.util.UUID.fromString("a4e58dbe-d904-3cb7-b5cb-78debe17ae8d"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c364cb9-22ee-3ac1-8c07-860ee0ca440b"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d2231b8-88fb-31cd-b973-aa5df4d8ed41"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfeaf336-0520-3d33-97de-09d98a33cb2b"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa40ecc4-043b-3fe2-927d-3ec449c3da14"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("751368e9-f0ce-3550-abcc-f1a8a2a4e4bc"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d77ce315-809b-368c-bc1f-89a58eb4dd4c"))) {
initiatingMasterActiveTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("69be6af0-1d44-3d6d-bf5a-d4f1a1a49ae7"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("240b6be9-ed7f-3903-ace0-753627970723"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("857ae6e7-8f3c-3c3f-b230-876dfd0ebcab"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3aa3286-c9e2-3cf8-a7cf-eb9810d98684"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9200a00-4ee9-3b01-8be6-3dcc42c38995"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32084646-8d88-37ae-83e5-65558c9dfa02"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4cf137d1-e8d2-31d9-be21-97863d7fa09e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e651a55-80ac-38c6-a3a5-50f3c9eea515"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d5ad15a-d183-3d28-825e-6296995534f7"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ae598e7-e1de-3ce9-8cf7-492ae3726278"))) {
masterSystemTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("56676de8-0cfd-3aea-8995-70ecbef82f57"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shutDown");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9ff1c391-fe23-3487-8421-43b73bbb7e99"))) {
initiatingMasterActiveTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ce59a519-03c2-3d74-8064-439046564011"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98d5bbc1-2b56-395a-91ea-9185135990c5"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("startcode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a823be0-4d29-3cd9-bac1-cd9cb45d1af0"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a9944bc-ef09-31ad-b72b-4c0c8f58af67"))) {
masterSystemTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4f8f390e-724e-320f-a351-994f6766db16"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd13db3d-746d-3410-8701-340d5c1b443d"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c7680b0-0abc-3cca-8b66-fa1a20deec60"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d33dff46-8927-3f63-85eb-a3e8b6d567de"))) {
masterSystemTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("277d08b3-6b58-3c29-a5f5-36021caf1eb0"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3f81d5e-4e6e-330c-a700-a1e4be4637ef"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45a921ea-4b78-34a9-8859-db6cfba0c391"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("stopped");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af59f4ac-42a9-3a69-9438-73e5fde0e05a"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ef36c77-b2ab-3684-8f0e-2e675bde9a13"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("84bb8efd-9834-3a1e-9ced-e22b78c5d2a3"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("91ed4f63-dbb3-3bbf-827c-b199e5b19e10"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38facdf9-f435-33ba-bc7a-951a14df1b1e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("masterless");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aea95bb8-644b-3adc-9555-867256d92d5f"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69b62aad-7517-3803-8321-c23226f2908e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("08ef3608-6242-33a5-931f-8854a84975dd"))) {
initiatingMasterActiveTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e1b47a2-07a6-3805-a8fb-ba15076ca1ce"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76a11314-36dd-386a-a094-9e91a6e54b04"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8dc7e206-c5df-3a72-b5da-9ab48a1b9466"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2fabe526-336b-3e25-91eb-d3e05e3e1d02"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afad8c0b-fbac-3200-80a0-cdf5d1b4a92c"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a666d247-8caa-3487-ae47-64cb27520bd1"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c4bdeee8-d5ca-3471-9a6f-cbc212482450"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f62305d4-2d1f-3b2d-ae5a-9d221879612a"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cde9d258-208d-3d76-a412-76f1f193feee"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9a890a00-57fc-3a55-a2a9-a7afb32099d9"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f50313d-60bb-301b-8f69-c7378849d417"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("isShutdownHookInstalled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        regionServer.getExecutorService().submit(AssignRegionHandler.create(regionServer,
          regionInfo, procId, tableDesc, masterSystemTime, initiatingMasterActiveTime));
      }
    }
  }

  private void executeCloseRegionProcedures(CloseRegionRequest request) {
    String encodedName;
    long initiatingMasterActiveTime =
      request.hasInitiatingMasterActiveTime() ? request.getInitiatingMasterActiveTime() : -1;
    try {
      encodedName = ProtobufUtil.getRegionEncodedName(request.getRegion());
    } catch (DoNotRetryIOException e) {
      throw new UncheckedIOException("Should not happen", e);
    }
    ServerName destination = request.hasDestinationServer()
      ? ProtobufUtil.toServerName(request.getDestinationServer())
      : null;
    long procId = request.getCloseProcId();
    boolean evictCache = request.getEvictCache();
if(KnobRuntime.check(java.util.UUID.fromString("02a75397-c733-3cba-abf3-df24c039fbcc"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f585cb7-04c4-3c48-9c16-9d2385ed4c44"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("29aac6ac-676e-327b-9f3e-d285944df2ec"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d8bfea5-61b3-3400-b98b-e43a59acdd6e"))) {
procId = -1;
}
    if (regionServer.submitRegionProcedure(procId)) {
if(KnobRuntime.check(java.util.UUID.fromString("266afcdd-6cf7-39f6-9980-4e93c6bb9055"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9552072-73f9-3208-bfc8-ed440e1daaf3"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("57dbac8b-b21b-328e-8c2a-2aac7783dbcc"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("049b083a-7de7-3b7b-8c92-7198bf7b1058"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2655f4de-4284-3f1d-b913-d9ccb5a90e91"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d715e3fb-5e16-3ef7-abc5-076921cbbe5e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f39e2f5-e136-3038-87f2-23ce3075c99c"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f28fb22-41f0-31cd-9da0-a349d7921bb4"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64b967ff-1fad-3025-bb3b-d49d1398baf1"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("106a58bb-6811-3d5b-afaf-523c02bf446d"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9167bff7-3ea1-3f99-8860-3c96080628d2"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9dee30aa-62d8-3005-863c-a3d8d0700b94"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c13ac53b-93b6-3faa-aa38-0e3d3b33e9fc"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ab85036-93ff-3be6-9960-00d98a23d355"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("34780336-29e3-315e-a050-125cd3e441d3"))) {
evictCache = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("0f61c67d-f912-3a71-82e4-f892882f6345"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("812c45fb-ba1f-39f6-94e6-fc209582d275"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e931a5bc-1434-3d1c-81d6-8f5cf5a18648"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dbacae13-57f7-3aca-8473-67c2d97f70e8"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("isShutdownHookInstalled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6036f3a9-c626-366f-b557-1c41b03dc281"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ace28de-cb5d-37e8-96c9-2f6fd72029c2"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3d005a5-3fc3-3f8a-b60a-1dc64831ae79"))) {
try {
    java.lang.reflect.Field field = destination.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(destination));
    field.set(destination, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c1e7c48-ac19-3e8f-b8cd-a7b596efda4c"))) {
initiatingMasterActiveTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea5e9f03-7fa0-3fa7-9039-f3bb0aaaf6c9"))) {
initiatingMasterActiveTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3b6add7c-3292-3de7-82c4-46cd1fbf0315"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("startcode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3743871f-9a01-317b-bc04-464ea3997e0d"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba5f3aaa-6661-348e-aec6-94b27b615da9"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd3dcb1a-4f26-3a8e-93ed-9876d50a24eb"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3676825-d3d9-36f3-b3c2-179b733b42d4"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf2b1d79-f5fb-3235-9481-66430ad770bd"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("88b6d42e-1674-3132-a6c0-c74eee05b91c"))) {
initiatingMasterActiveTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c3c5f25a-6cd0-30a8-938a-191efbf1253e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("91ebaf8d-8596-396d-a250-13036bf59901"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02a84afb-725e-35cf-aefb-84ad34bc5079"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1bf2fd44-014f-3162-9b82-cdc30dd86130"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa11a599-9ce9-3c11-8327-3b4aa695101b"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("800aa76f-9225-3341-8217-bc1ef75880d9"))) {
evictCache = !evictCache;
}
if(KnobRuntime.check(java.util.UUID.fromString("c39530a8-de4f-3297-aca8-d77df26eb56b"))) {
evictCache = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("21bf97de-e118-3b35-bf5c-ad6c4f0d3733"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cea95ba6-a56d-3cae-b2b0-5f6f53b0174d"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50be6e75-3230-31f2-a93f-579d2421b426"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3462774b-dc29-3e92-87a7-d57ce9265b63"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("630bccce-48d0-3df0-870c-71e3af94035a"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1024758e-e612-35ab-801e-1b01511a0f78"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2f82e87-1d5c-3ac7-9de4-d61b587b0a17"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("stopped");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25ae1890-d62e-37da-b649-25d26e2fd015"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("15014caa-e15e-3d97-8917-1b20056c653b"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fdc54f55-0ee1-3382-95ac-e5557e8dc5a0"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("masterless");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf28d7fa-56ff-3289-bc39-de6e1f68267e"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("shutDown");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionServer);
    field.set(regionServer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9557a00-bee6-3241-ad46-59237549a276"))) {
try {
    java.lang.reflect.Field field = regionServer.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionServer));
    field.set(regionServer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      regionServer.getExecutorService().submit(UnassignRegionHandler.create(regionServer,
        encodedName, procId, false, destination, evictCache, initiatingMasterActiveTime));
    }
  }

  private void executeProcedures(RemoteProcedureRequest request) {
if(KnobRuntime.check(java.util.UUID.fromString("c24f7a03-83df-3efa-a3f9-bdcea3ea96e8"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("procId_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c652a9c9-c038-3619-a2c6-beff9d46eae5"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eca65636-8a6d-34fc-b502-675facdb4340"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("134d0dd9-fd39-371a-8e5f-f3644ff6e21e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b540b9a1-da02-3abf-90fe-5a25a585e5c2"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba77c4c5-cf6d-3087-95d1-dda0648a7193"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("initiatingMasterActiveTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("adff0251-a70b-367a-b2a8-4241a09866a7"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    RSProcedureCallable callable;
    try {
      callable = Class.forName(request.getProcClass()).asSubclass(RSProcedureCallable.class)
        .getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.warn("Failed to instantiating remote procedure {}, pid={}", request.getProcClass(),
        request.getProcId(), e);
      regionServer.remoteProcedureComplete(request.getProcId(),
        request.getInitiatingMasterActiveTime(), e);
      return;
    }
    callable.init(request.getProcData().toByteArray(), regionServer);
    LOG.debug("Executing remote procedure {}, pid={}", callable.getClass(), request.getProcId());
    regionServer.executeProcedure(request.getProcId(), request.getInitiatingMasterActiveTime(),
      callable);
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public ExecuteProceduresResponse executeProcedures(RpcController controller,
    ExecuteProceduresRequest request) throws ServiceException {
    try {
      checkOpen();
      throwOnWrongStartCode(request);
      regionServer.getRegionServerCoprocessorHost().preExecuteProcedures();
      if (request.getOpenRegionCount() > 0) {
        // Avoid reading from the TableDescritor every time(usually it will read from the file
        // system)
        Map<TableName, TableDescriptor> tdCache = new HashMap<>();
        request.getOpenRegionList().forEach(req -> executeOpenRegionProcedures(req, tdCache));
      }
      if (request.getCloseRegionCount() > 0) {
        request.getCloseRegionList().forEach(this::executeCloseRegionProcedures);
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("db16a599-291e-37c5-a93a-5903752fb50c"))) ? ((request.getProcCount()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d45161a1-7a2a-3661-bf5c-9e03cad93ab0"))) ? ((request.getCloseRegionCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4c72b49a-2251-3bbc-a1bb-32468205bae5"))) ? ((request.getProcCount()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("89e33665-0217-3462-b789-b4971dc3cebf"))) ? ((request.getCloseRegionCount()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5610f94f-6dac-387f-9488-1e0d4f5ce96c"))) ? ((request.getOpenRegionCount()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7c89aa6e-7108-3aa9-905d-2449031b11c4"))) ? ((request.getOpenRegionCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("39ba3a50-5b2f-3de9-82da-88d936377417"))) ? ((request.getCloseRegionCount()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("df502af2-bd94-38b1-bbb2-07808ebe7974"))) ? ((request.getProcCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7a13d762-61b8-3c92-b1ec-778445f7d768"))) ? ((request.getProcCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1ea71098-546f-3ac3-a3bd-b5b220821895"))) ? ((request.getOpenRegionCount()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d53f65d0-3151-351d-b712-87a19f889f45"))) ? ((request.getCloseRegionCount()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("763005a2-e0d4-3452-b184-f29618b50f81"))) ? ((request.getProcCount()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("44b20eb0-533c-316e-bdd5-e01e4d0a129e"))) ? ((request.getCloseRegionCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("010eae44-736e-3d4e-ba04-4270267ab0e4"))) ? ((request.getOpenRegionCount()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e88813c1-f210-3f45-808e-d8c92287ba41"))) ? ((request.getCloseRegionCount()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ba534ead-8242-3b61-ab91-7b70445fafba"))) ? ((request.getProcCount()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1f9616b3-5afb-3336-b3fa-6bcced966cbf"))) ? ((request.getOpenRegionCount()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("87279b7b-dff4-3d6a-8abf-967df70c6a84"))) ? ((request.getOpenRegionCount()) == (0)) : (request.getProcCount() > 0))))))))))))))))))))))))))))))))))))) {
        request.getProcList().forEach(this::executeProcedures);
      }
if(KnobRuntime.check(java.util.UUID.fromString("6b292103-9786-3ece-97ce-0cc1ec01766a"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("55cd9bad-052c-3b57-b374-71b1bd6df2c3"))) { checkOpen(); } else if (KnobRuntime.check(java.util.UUID.fromString("39850b7a-c64b-3b89-99c6-29a165a625e7"))) { closeAllScanners(); } else { regionServer.getRegionServerCoprocessorHost().postExecuteProcedures(); }
      return ExecuteProceduresResponse.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public SlowLogResponses getSlowLogResponses(final RpcController controller,
    final SlowLogResponseRequest request) {
    final NamedQueueRecorder namedQueueRecorder = this.regionServer.getNamedQueueRecorder();
    final List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request, namedQueueRecorder);
    SlowLogResponses slowLogResponses =
      SlowLogResponses.newBuilder().addAllSlowLogPayloads(slowLogPayloads).build();
    return slowLogResponses;
  }

  private List<SlowLogPayload> getSlowLogPayloads(SlowLogResponseRequest request,
    NamedQueueRecorder namedQueueRecorder) {
    if (namedQueueRecorder == null) {
      return Collections.emptyList();
    }
    List<SlowLogPayload> slowLogPayloads;
    NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(RpcLogDetails.SLOW_LOG_EVENT);
if(KnobRuntime.check(java.util.UUID.fromString("1e3251e0-596f-3d57-843d-cea138bf2780"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67ed8c27-dcf1-3025-992f-b3dceff6d454"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("766d0c01-63ec-3cb9-b895-b38eff31c08d"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("logType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4cada50d-e37b-3408-a2cc-f2a0be31c6ee"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a918075a-a5a9-3d60-b3ec-0c6ea0ba038d"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("limit_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6171a033-4091-359f-8066-e5a882c8a9cf"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("logType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e33e9bf-3c44-307a-9399-69bd9ef4aaa2"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc67dc5e-c1ec-3c8f-808e-9034e0ec1274"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("logType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42c870d4-5ad1-3e91-92e6-2d563c7f8311"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("logType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dbc06b63-62b7-3e42-a3a0-c78ff23d27a7"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5581c712-0589-31ec-b696-3fa56de9978b"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("filterByOperator_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a7ba71d-76e1-3710-8ce0-efbd10b9f7f9"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("filterByOperator_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7eec6e5c-4283-3efc-8c77-075d7f9abb09"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87e15209-ec0a-3db1-b50d-012a560c68ca"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("filterByOperator_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a6e1e69-f65f-352d-8862-882d2778decf"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("filterByOperator_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c6c2140-6215-3519-b11a-9dc5c4872ab2"))) {
try {
    java.lang.reflect.Field field = request.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(request));
    field.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    namedQueueGetRequest.setSlowLogResponseRequest(request);
    NamedQueueGetResponse namedQueueGetResponse =
      namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
    slowLogPayloads = namedQueueGetResponse != null
      ? namedQueueGetResponse.getSlowLogPayloads()
      : Collections.emptyList();
    return slowLogPayloads;
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public SlowLogResponses getLargeLogResponses(final RpcController controller,
    final SlowLogResponseRequest request) {
    final NamedQueueRecorder namedQueueRecorder = this.regionServer.getNamedQueueRecorder();
    final List<SlowLogPayload> slowLogPayloads = getSlowLogPayloads(request, namedQueueRecorder);
    SlowLogResponses slowLogResponses =
      SlowLogResponses.newBuilder().addAllSlowLogPayloads(slowLogPayloads).build();
    return slowLogResponses;
  }

  @Override
  @QosPriority(priority = HConstants.ADMIN_QOS)
  public ClearSlowLogResponses clearSlowLogsResponses(final RpcController controller,
    final ClearSlowLogResponseRequest request) throws ServiceException {
    rpcPreCheck("clearSlowLogsResponses");
    final NamedQueueRecorder namedQueueRecorder = this.regionServer.getNamedQueueRecorder();
    boolean slowLogsCleaned = Optional.ofNullable(namedQueueRecorder)
      .map(
        queueRecorder -> queueRecorder.clearNamedQueue(NamedQueuePayload.NamedQueueEvent.SLOW_LOG))
      .orElse(false);
    ClearSlowLogResponses clearSlowLogResponses =
      ClearSlowLogResponses.newBuilder().setIsCleaned(slowLogsCleaned).build();
    return clearSlowLogResponses;
  }

  @Override
  public HBaseProtos.LogEntry getLogEntries(RpcController controller,
    HBaseProtos.LogRequest request) throws ServiceException {
    try {
      final String logClassName = request.getLogClassName();
      Class<?> logClass = Class.forName(logClassName).asSubclass(Message.class);
      Method method = logClass.getMethod("parseFrom", ByteString.class);
      if (logClassName.contains("SlowLogResponseRequest")) {
        SlowLogResponseRequest slowLogResponseRequest =
          (SlowLogResponseRequest) method.invoke(null, request.getLogMessage());
        final NamedQueueRecorder namedQueueRecorder = this.regionServer.getNamedQueueRecorder();
        final List<SlowLogPayload> slowLogPayloads =
          getSlowLogPayloads(slowLogResponseRequest, namedQueueRecorder);
        SlowLogResponses slowLogResponses =
          SlowLogResponses.newBuilder().addAllSlowLogPayloads(slowLogPayloads).build();
        return HBaseProtos.LogEntry.newBuilder()
          .setLogClassName(slowLogResponses.getClass().getName())
          .setLogMessage(slowLogResponses.toByteString()).build();
      }
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
      | InvocationTargetException e) {
      LOG.error("Error while retrieving log entries.", e);
      throw new ServiceException(e);
    }
    throw new ServiceException("Invalid request params");
  }

  @Override
  public GetCachedFilesListResponse getCachedFilesList(RpcController controller,
    GetCachedFilesListRequest request) throws ServiceException {
    GetCachedFilesListResponse.Builder responseBuilder = GetCachedFilesListResponse.newBuilder();
    List<String> fullyCachedFiles = new ArrayList<>();
    regionServer.getBlockCache().flatMap(BlockCache::getFullyCachedFiles).ifPresent(fcf -> {
      fullyCachedFiles.addAll(fcf.keySet());
    });
    return responseBuilder.addAllCachedFiles(fullyCachedFiles).build();
  }

  public RpcScheduler getRpcScheduler() {
if(KnobRuntime.check(java.util.UUID.fromString("7632fe7a-f293-336b-8acd-ddb72a2a375b"))) {
return null;
}
    return rpcServer.getScheduler();
  }

  protected AccessChecker getAccessChecker() {
    return accessChecker;
  }

  protected ZKPermissionWatcher getZkPermissionWatcher() {
    return zkPermissionWatcher;
  }

  @Override
  public GetClusterIdResponse getClusterId(RpcController controller, GetClusterIdRequest request)
    throws ServiceException {
    return GetClusterIdResponse.newBuilder().setClusterId(regionServer.getClusterId()).build();
  }

  @Override
  public GetActiveMasterResponse getActiveMaster(RpcController controller,
    GetActiveMasterRequest request) throws ServiceException {
    GetActiveMasterResponse.Builder builder = GetActiveMasterResponse.newBuilder();
    regionServer.getActiveMaster()
      .ifPresent(name -> builder.setServerName(ProtobufUtil.toServerName(name)));
    return builder.build();
  }

  @Override
  public GetMastersResponse getMasters(RpcController controller, GetMastersRequest request)
    throws ServiceException {
    GetMastersResponse.Builder builder = GetMastersResponse.newBuilder();
    regionServer.getActiveMaster()
      .ifPresent(activeMaster -> builder.addMasterServers(GetMastersResponseEntry.newBuilder()
        .setServerName(ProtobufUtil.toServerName(activeMaster)).setIsActive(true)));
    regionServer.getBackupMasters()
      .forEach(backupMaster -> builder.addMasterServers(GetMastersResponseEntry.newBuilder()
        .setServerName(ProtobufUtil.toServerName(backupMaster)).setIsActive(false)));
    return builder.build();
  }

  @Override
  public GetMetaRegionLocationsResponse getMetaRegionLocations(RpcController controller,
    GetMetaRegionLocationsRequest request) throws ServiceException {
    GetMetaRegionLocationsResponse.Builder builder = GetMetaRegionLocationsResponse.newBuilder();
    Optional<List<HRegionLocation>> metaLocations =
      regionServer.getMetaRegionLocationCache().getMetaRegionLocations();
    metaLocations.ifPresent(hRegionLocations -> hRegionLocations
      .forEach(location -> builder.addMetaLocations(ProtobufUtil.toRegionLocation(location))));
    return builder.build();
  }

  @Override
  public final GetBootstrapNodesResponse getBootstrapNodes(RpcController controller,
    GetBootstrapNodesRequest request) throws ServiceException {
    int maxNodeCount = regionServer.getConfiguration().getInt(CLIENT_BOOTSTRAP_NODE_LIMIT,
      DEFAULT_CLIENT_BOOTSTRAP_NODE_LIMIT);
    ReservoirSample<ServerName> sample = new ReservoirSample<>(maxNodeCount);
    sample.add(regionServer.getBootstrapNodes());

    GetBootstrapNodesResponse.Builder builder = GetBootstrapNodesResponse.newBuilder();
    sample.getSamplingResult().stream().map(ProtobufUtil::toServerName)
      .forEach(builder::addServerName);
    return builder.build();
  }

  @Override
  public GetAllBootstrapNodesResponse getAllBootstrapNodes(RpcController controller,
    GetAllBootstrapNodesRequest request) throws ServiceException {
    GetAllBootstrapNodesResponse.Builder builder = GetAllBootstrapNodesResponse.newBuilder();
    regionServer.getBootstrapNodes()
      .forEachRemaining(server -> builder.addNode(ProtobufUtil.toServerName(server)));
    return builder.build();
  }

  private void setReloadableGuardrails(Configuration conf) {
    rowSizeWarnThreshold =
      conf.getInt(HConstants.BATCH_ROWS_THRESHOLD_NAME, HConstants.BATCH_ROWS_THRESHOLD_DEFAULT);
    rejectRowsWithSizeOverThreshold =
      conf.getBoolean(REJECT_BATCH_ROWS_OVER_THRESHOLD, DEFAULT_REJECT_BATCH_ROWS_OVER_THRESHOLD);
    maxScannerResultSize = conf.getLong(HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
      HConstants.DEFAULT_HBASE_SERVER_SCANNER_MAX_RESULT_SIZE);
  }

  RegionScannerContext checkQuotaAndGetRegionScannerContext(ScanRequest request,
    ScanResponse.Builder builder) throws IOException {
    if (request.hasScannerId()) {
      // The downstream projects such as AsyncHBase in OpenTSDB need this value. See HBASE-18000
      // for more details.
      long scannerId = request.getScannerId();
      builder.setScannerId(scannerId);
      String scannerName = toScannerName(scannerId);
      RegionScannerHolder rsh = getRegionScanner(request);
      OperationQuota quota =
        getRpcQuotaManager().checkScanQuota(rsh.r, request, maxScannerResultSize,
          rsh.getMaxBlockBytesScanned(), rsh.getPrevBlockBytesScannedDifference());
      return new RegionScannerContext(scannerName, rsh, quota);
    }

    HRegion region = getRegion(request.getRegion());
    OperationQuota quota =
      getRpcQuotaManager().checkScanQuota(region, request, maxScannerResultSize, 0L, 0L);
    Pair<String, RegionScannerHolder> pair = newRegionScanner(request, region, builder);
    return new RegionScannerContext(pair.getFirst(), pair.getSecond(), quota);
  }
}
