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
          LOG.error("Exception while closing the scanner " + scanner, e);
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
if(KnobRuntime.check(java.util.UUID.fromString("1a3f9824-1ac2-3d51-b941-0e6ec6591854"))) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(r));
    _knob_field_.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return getResultOrException(ResponseConverter.buildActionResult(r), index);
  }

  private static ResultOrException getResultOrException(final Exception e, final int index) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fcc4cba-a0c2-3a00-b0a3-c419931e0d3c"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16ed84ee-1d04-368f-9a5c-0e9a0eea6ad6"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba66ac8d-0012-3372-a579-29ba3026a02b"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b46f7e2-1e45-3371-ad6b-57dcc80e6727"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("openSeqNum");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32bccc0b-6c31-30b0-8593-22abf2e24091"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10fff9bf-7be3-3c68-89fb-2a79bfc97b79"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("busyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("e14b5a08-94b4-39ea-ac6d-a6e93ba1681c"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastFlushOpSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3687ee5d-d4bb-3da9-8898-d56830e27a31"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("802277a0-eedd-31fd-83db-d0c9838089ba"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa61e1ca-397c-34ac-85eb-148c78e36c0f"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("blockingMemStoreSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    long before = EnvironmentEdgeManager.currentTime();
if(KnobRuntime.check(java.util.UUID.fromString("dc6de42b-4d6e-33cf-8bec-2a6ecc8052be"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = mutation.getClass().getDeclaredField("associatedCellCount_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(mutation));
    _knob_field_.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Increment increment = ProtobufUtil.toIncrement(mutation, cells);
if(KnobRuntime.check(java.util.UUID.fromString("4ed277e0-b406-3c2a-ac44-c6366eb40270"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushPerChanges");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("275e550a-bd38-3c80-a3fc-3d206ac27f5c"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("busyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("4c095abb-09a9-3993-a58c-89f40b9905a9"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxFlushedSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65baf8bb-5f25-33b2-a98e-79edc1bf3bf0"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = actions.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(actions));
    _knob_field_.set(actions, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90da879f-3837-3d55-9ff4-ad980082832c"), "regionserver", this.regionServer)) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ccae2c1a-363d-3528-8882-45fea47f2197"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("397fb214-c551-34ab-946e-470e12127759"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c9cdaa7-f000-3bfb-beb2-48c9b3f567e0"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("e302a5a6-9ef4-3f83-8a8d-fd2daea384fa"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = pbGet.getClass().getDeclaredField("storeOffset_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(pbGet));
    _knob_field_.set(pbGet, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            Get get = ProtobufUtil.toGet(pbGet);
            if (context != null) {
              r = get(get, (region), closeCallBack, context);
            } else {
              r = region.get(get);
            }
          } finally {
            final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
            if (metricsRegionServer != null) {
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
if(KnobRuntime.check(java.util.UUID.fromString("724061c2-c070-3085-a2dc-f539590ce7e3"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(r));
    _knob_field_.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e923aec3-ce69-35e8-8286-5889b9b1af06"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("openSeqNum");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(r));
    _knob_field_.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("76bdff03-74f5-33b7-abeb-f208a5cdd3b8"), "regionserver", this.regionServer)) ? ((r.maxCellSize) <= (0)) : (r.maxCellSize > 0))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c862b9ba-98a6-35bf-9f93-582479af1f90"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1d1df79d-f6a9-3a43-8594-412044875f5c"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("openSeqNum");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5afa105f-9bfb-36dd-ae0a-05ff1caac95d"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("4cedc29a-5b2d-34ed-9bae-7e713fed7a7d"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20780c20-d750-374a-9468-665785719770"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("190539d5-bd38-38c4-95c0-a7cde295c26f"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastFlushOpSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0423189-4919-3e46-913c-448d5c7ece85"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
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
          if (((KnobRuntime.check(java.util.UUID.fromString("d0062a81-816d-35d3-a9c6-3c80ea29d503"), "regionserver", this.regionServer)) ? ((i) != (0)) : (i != 0))) {
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
            if (result.rawCells() != null) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2d179a88-6b2b-3df9-a934-f2a1bec0f343"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = currentMutation.getClass().getDeclaredField("ts");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(currentMutation));
    _knob_field_.set(currentMutation, oldValue - 1);
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
            if (KnobRuntime.check(java.util.UUID.fromString("127750de-aa9b-3e91-8059-ed256153f56c"), "regionserver", this.regionServer)) { builder.addResultOrException(getResultOrException(e, 0)); } else { builder.addResultOrException(getResultOrException(e, index)); }
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
            FlushDescriptor flushDesc = WALEdit.getFlushDescriptor(metaCell);
            if (((KnobRuntime.check(java.util.UUID.fromString("9588bf90-038d-3652-a4e4-876bd08eef4d"), "regionserver", this.regionServer)) ? (flushDesc != null) : (((KnobRuntime.check(java.util.UUID.fromString("74548971-1990-3aa1-b696-f0dcecd662e9"), "regionserver", this.regionServer)) ? (((flushDesc) == (null)) && (!isDefaultReplica)) : (flushDesc != null && !isDefaultReplica))))) {
              hRegion.replayWALFlushMarker(flushDesc, replaySeqId);
              continue;
            }
            RegionEventDescriptor regionEvent = WALEdit.getRegionEventDescriptor(metaCell);
            if (regionEvent != null && !isDefaultReplica) {
              hRegion.replayWALRegionEventMarker(regionEvent);
              continue;
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
      requestCount.increment();
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
      if (KnobRuntime.check(java.util.UUID.fromString("2c712215-c75f-38f6-a294-00d26f2f1665"), "regionserver", this.regionServer)) { setReloadableGuardrails(newConf); } else { ((ConfigurationObserver) rpcServer).onConfigurationChange(newConf); }
      setReloadableGuardrails(newConf);
    }
  }

  protected PriorityFunction createPriority() {
    return new AnnotationReadingPriorityFunction(this);
  }

  protected void requirePermission(String request, Permission.Action perm) throws IOException {
    if (accessChecker != null) {
      accessChecker.requirePermission(RpcServer.getRequestUser().orElse(null), request, null, perm);
    }
  }

  public int getScannersCount() {
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
    builder.append(" operation_id: ").append(scanner.getOperationId());
    return builder.toString();
  }

  public String getScanDetailsWithRequest(ScanRequest request) {
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
      return builder.toString();
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
if(KnobRuntime.check(java.util.UUID.fromString("60e337df-a300-3167-97d0-861986b38b19"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(r));
    _knob_field_.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("571d2768-2f1b-3931-8a71-c8ddc2cbad85"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("maxBusyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(r));
    _knob_field_.set(r, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("f4d2ea0d-c04e-3099-a7a5-8c9983602590"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = stats.getClass().getDeclaredField("evictedBlocks");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(stats));
    _knob_field_.set(stats, oldValue - 1);
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
      if (((KnobRuntime.check(java.util.UUID.fromString("5b6419a9-180f-32ba-94a3-daca156fab4b"), "regionserver", this.regionServer)) ? (request.hasMajor()) : (request.hasFamily()))) {
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
        region.requestCompaction(log, Store.PRIORITY_USER, major, CompactionLifeCycleTracker.DUMMY);
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
if(KnobRuntime.check(java.util.UUID.fromString("50b1f2a7-8fdd-34e4-b058-c5fef6f56578"), "regionserver", this.regionServer)) {
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
      if (request.hasCompactionState() && request.getCompactionState()) {
        builder.setCompactionState(ProtobufUtil.createCompactionState(region.getCompactionState()));
      }
      builder.setSplittable(region.isSplittable());
      builder.setMergeable(region.isMergeable());
      if (((KnobRuntime.check(java.util.UUID.fromString("2d824e1b-56df-3f14-b89b-d8e00fc80d5c"), "regionserver", this.regionServer)) ? (((request.hasCompactionState()) && (request.getBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("da4fd2ab-78e7-3934-bdfc-f0e4f9896619"), "regionserver", this.regionServer)) ? (((request.hasBestSplitRow()) && (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8b269470-bdf6-31e0-894f-03805f2f7f6e"), "regionserver", this.regionServer)) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0ac91f5e-a416-3f22-a662-ad0d290167ac"), "regionserver", this.regionServer)) ? (((request.hasCompactionState()) && (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("57c8b79f-88e6-3c98-9f2c-2b0b06ea64c1"), "regionserver", this.regionServer)) ? ((request.hasBestSplitRow()) || (request.getBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("352bf22a-bc6f-3d60-9b62-ca457b98b5d6"), "regionserver", this.regionServer)) ? (((request.hasBestSplitRow()) && (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c685c37b-de0e-368e-9373-557d76434e84"), "regionserver", this.regionServer)) ? (((request.getBestSplitRow()) || (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("96926605-c1d6-3269-8dfe-6dc89d147f78"), "regionserver", this.regionServer)) ? ((request.hasCompactionState()) || (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("2e389a81-a53d-3034-a508-fc040c268da4"), "regionserver", this.regionServer)) ? (((request.hasCompactionState()) && (request.hasCompactionState())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e9b7982c-0c6e-3a5c-8737-7aa9e4864d02"), "regionserver", this.regionServer)) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) && (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("1767e4dd-afa5-3b60-aa57-f624ba503b08"), "regionserver", this.regionServer)) ? (((request.getBestSplitRow()) && (request.hasCompactionState())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("288b3501-53a2-32b0-8635-cbd5270abd1c"), "regionserver", this.regionServer)) ? (((request.hasBestSplitRow()) && (request.getBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c13bcd47-b371-3c6c-a014-f3a0a6271356"), "regionserver", this.regionServer)) ? (((request.hasBestSplitRow()) || (request.hasCompactionState())) || (bestSplitRow != null)) : (((KnobRuntime.check(java.util.UUID.fromString("d8f5651d-9791-3d10-9435-48ea783d62f2"), "regionserver", this.regionServer)) ? (((request.getBestSplitRow()) && (request.hasBestSplitRow())) && ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("77c313f4-d423-375f-842d-7604060440c2"), "regionserver", this.regionServer)) ? (((request.getBestSplitRow()) || (request.hasBestSplitRow())) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0534d21b-43d0-3396-85cc-b1d7cfe5d519"), "regionserver", this.regionServer)) ? (((request.hasCompactionState()) && (request.hasCompactionState())) && ((bestSplitRow) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a0a58828-6d85-3e59-858f-8b97f14d07de"), "regionserver", this.regionServer)) ? ((request.hasBestSplitRow()) || ((bestSplitRow) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("567c9daa-9afe-3efa-9fed-f83ae58f6fe5"), "regionserver", this.regionServer)) ? ((request.hasBestSplitRow()) && (request.hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("b0fe883e-5bfe-38b1-90d4-e6b7a97b1b11"), "regionserver", this.regionServer)) ? (((request.hasBestSplitRow()) || (request.hasBestSplitRow())) || ((bestSplitRow) != (null))) : (request.hasBestSplitRow() && request.getBestSplitRow() && bestSplitRow != null))))))))))))))))))))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("134adf01-4499-319b-b656-6c8b8b55a6be"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = request.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(request));
    _knob_field_.set(request, oldValue * 2);
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
            regionServer.online.wait(regionServer.msgInterval);
          }
          checkOpen();
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
      checkOpen();
      List<WALEntry> entries = request.getEntryList();
      if (((KnobRuntime.check(java.util.UUID.fromString("df8531cf-32ad-310d-a328-92e5346f2339"), "regionserver", this.regionServer)) ? (((entries) == (null)) || (entries.isEmpty())) : (entries == null || entries.isEmpty()))) {
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
        if (((KnobRuntime.check(java.util.UUID.fromString("c7625dc9-cdf3-32eb-9ba7-c152f5eaf772"), "regionserver", this.regionServer)) ? (((regionServer.nonceManager) != (null)) && (!isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("07686990-0d36-3057-bdc5-1a5bceff2ec7"), "regionserver", this.regionServer)) ? (((regionServer.nonceManager) == (null)) || (!isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("5c69c68a-bfce-3c22-9786-56e33ce0810a"), "regionserver", this.regionServer)) ? (((regionServer.nonceManager) == (null)) && (isPrimary)) : (((KnobRuntime.check(java.util.UUID.fromString("3e33e86a-0aef-3fcd-9a83-917722e8be01"), "regionserver", this.regionServer)) ? (regionServer.nonceManager != null) : (((KnobRuntime.check(java.util.UUID.fromString("ad8a8880-8328-3590-885a-6e10e0f9c05f"), "regionserver", this.regionServer)) ? ((regionServer.nonceManager != null) && (!isPrimary)) : (regionServer.nonceManager != null && isPrimary))))))))))) {
          long nonceGroup =
            entry.getKey().hasNonceGroup() ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
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
            if (KnobRuntime.check(java.util.UUID.fromString("63ce92f0-708d-3941-a851-3185b48e8f89"), "regionserver", this.regionServer)) { filePaths.add(getRemoteClientIpAndPort()); } else { filePaths.add(familyPath.getPath()); }
          }
          // Check if the batch of files exceeds the current quota
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
        if (((KnobRuntime.check(java.util.UUID.fromString("40fcd490-9964-3b58-8d73-66ac33550541"), "regionserver", this.regionServer)) ? ((sizeToBeLoaded) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("808b8030-4afb-315c-a1f7-eb19eed2c0f4"), "regionserver", this.regionServer)) ? ((!spaceQuotaEnabled) && ((sizeToBeLoaded) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("26cf0aad-4690-3c73-898b-22fa2edb101b"), "regionserver", this.regionServer)) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac8fe172-7c1c-3cab-ae41-f7fede2fb135"), "regionserver", this.regionServer)) ? ((!spaceQuotaEnabled) || ((sizeToBeLoaded) < (0))) : (spaceQuotaEnabled && sizeToBeLoaded > 0))))))))) {
          if (LOG.isTraceEnabled()) {
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
if(KnobRuntime.check(java.util.UUID.fromString("49bdd6ee-171b-3324-88b9-17b9b3409444"), "regionserver", this.regionServer)) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a3743653-07d5-3ebc-af0f-b39bd798c596"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = request.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(request));
    _knob_field_.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    try {
      checkOpen();
      if (KnobRuntime.check(java.util.UUID.fromString("7543856c-db2f-3951-b609-73d1257d7f15"), "regionserver", this.regionServer)) { checkOpen(); } else { requestCount.increment(); }

      HRegion region = getRegion(request.getRegion());

if(KnobRuntime.check(java.util.UUID.fromString("a24aedf9-eaff-33a6-9e35-f04fa72ce226"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = request.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(request));
    _knob_field_.set(request, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("fef6cc80-8b66-306c-8825-7c6fe75e15f6"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = request.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(request));
    _knob_field_.set(request, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    try {
      checkOpen();
      requestCount.increment();

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
    if (filePaths.isEmpty()) {
      // local hdfs
      return regionServer.getFileSystem();
    }
    // source hdfs
    return new Path(filePaths.get(0)).getFileSystem(regionServer.getConfiguration());
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
      requestCount.increment();
      rpcGetRequestCount.increment();
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
if(KnobRuntime.check(java.util.UUID.fromString("3757af00-ea63-33fd-a5bf-be6e00ed711e"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4d3cf3f-3b99-3473-b289-2f9cc2d5058f"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("busyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe0a4a9c-3c05-3e5f-89ea-831aae5a718a"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("701cd5e8-a1c6-3d51-bfee-d26c07a913d0"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9f18d2a-3449-3ed6-a3c8-41160134aacd"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4cb24bf2-493b-3b91-8d1b-f3fedfe85317"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f5d2f8a-3f43-3cef-addf-8b7a957f483b"), "regionserver", this.regionServer)) {
throw new java.io.IOException("Injected exception");
}
      quota = getRpcQuotaManager().checkBatchQuota(region, OperationQuota.OperationType.GET);

      Get clientGet = ProtobufUtil.toGet(get);
      if (((KnobRuntime.check(java.util.UUID.fromString("98a18794-b8aa-3598-8291-8c46fbe3da5a"), "regionserver", this.regionServer)) ? ((get.hasClosestRowBefore()) && ((region.getCoprocessorHost()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("781c9160-b8b4-3981-b9f3-a96af6def211"), "regionserver", this.regionServer)) ? ((get.getExistenceOnly()) || ((region.getCoprocessorHost()) != (null))) : (get.getExistenceOnly() && region.getCoprocessorHost() != null))))) {
        existence = region.getCoprocessorHost().preExists(clientGet);
      }
      if (existence == null) {
        if (context != null) {
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
        builder.setResult(pbr);
      } else if (r != null) {
        ClientProtos.Result pbr;
        if (
          isClientCellBlockSupport(context) && controller instanceof HBaseRpcController
            && VersionInfoUtil.hasMinimumVersion(context.getClientVersionInfo(), 1, 3)
        ) {
if(KnobRuntime.check(java.util.UUID.fromString("4dbcd719-265d-3c77-addb-aa046c71cd25"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("cellScannerIndex");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(r));
    _knob_field_.set(r, oldValue + 1);
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
      if (((KnobRuntime.check(java.util.UUID.fromString("547f554c-dfbf-33c3-b236-7ef188622045"), "regionserver", this.regionServer)) ? ((r) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fa856315-cf75-3fee-b23f-664724457352"), "regionserver", this.regionServer)) ? ((r != null) || ((r.rawCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0d79eba3-e4af-3304-9b99-99062908fea1"), "regionserver", this.regionServer)) ? ((r != null) || ((r.rawCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0bf1bd70-9673-3a5f-85f7-51bad6d15181"), "regionserver", this.regionServer)) ? (((r) != (null)) || ((r.rawCells()) != (null))) : (r != null && r.rawCells() != null))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("74a1ff51-e651-3898-aa8a-db814c6a518e"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = get.getClass().getDeclaredField("storeLimit");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(get));
    _knob_field_.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("330a235c-3084-3d7e-b83f-795d2bdf2f69"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = get.getClass().getDeclaredField("storeLimit");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(get));
    _knob_field_.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (region.getCoprocessorHost().preGet(get, results)) {
if(KnobRuntime.check(java.util.UUID.fromString("a619a815-8f05-3124-806e-e7f198ebba59"), "regionserver", this.regionServer)) {
before = 0;
}
        region.metricsUpdateForGet(results, before);
        return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null,
          stale);
      }
    }
    Scan scan = new Scan(get);
    if (((KnobRuntime.check(java.util.UUID.fromString("f8db2fdf-bc26-3353-9c9a-5190af2e7891"), "regionserver", this.regionServer)) ? ((scan.getLoadColumnFamiliesOnDemandValue()) != (null)) : (scan.getLoadColumnFamiliesOnDemandValue() == null))) {
      scan.setLoadColumnFamiliesOnDemand(region.isLoadingCfsOnDemandDefault());
    }
    RegionScannerImpl scanner = null;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("f39b1d57-242f-361c-9291-738e635349ff"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("storeOffset");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f6c9519-335a-32ba-9d4a-770985e656eb"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("caching");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf95126e-29db-3463-82e8-717139569169"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("batch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue * 2);
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
    if (region.getCoprocessorHost() != null) {
      region.getCoprocessorHost().postGet(get, results);
    }
    region.metricsUpdateForGet(results, before);

    return Result.create(results, get.isCheckExistenceOnly() ? !results.isEmpty() : null, stale);
  }

  private void checkBatchSizeAndLogLargeSize(MultiRequest request) throws ServiceException {
    int sum = 0;
    String firstRegionName = null;
    for (RegionAction regionAction : request.getRegionActionList()) {
      if (sum == 0) {
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
      assert regionAction.getAtomic();

      OperationQuota quota;
      HRegion region;
      RegionSpecifier regionSpecifier = regionAction.getRegion();

      try {
        region = getRegion(regionSpecifier);
        quota = getRpcQuotaManager().checkBatchQuota(region, regionAction.getActionList(),
          regionAction.hasCondition());
      } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("6c3cc589-d3d5-3f8c-9808-688abd2dbc71"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = responseBuilder.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(responseBuilder));
    _knob_field_.set(responseBuilder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0073381d-5468-3b88-97be-055e3c3ccd33"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = responseBuilder.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(responseBuilder));
    _knob_field_.set(responseBuilder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        failRegionAction(responseBuilder, regionActionResultBuilder, regionAction, cellScanner, e);
        return responseBuilder.build();
      }

      try {
if(KnobRuntime.check(java.util.UUID.fromString("eeda80e7-711f-3352-af4e-9fffe4916313"), "regionserver", this.regionServer)) {
nonceGroup += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("98d5d546-f93c-3299-96f8-aa88238137d9"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad38e814-4a9c-3f31-a48c-c0be9d032331"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9020ca4e-5196-310c-bd6b-60ed31c15350"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e38da70c-b165-3596-90ad-8a71dd17e4ed"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
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
                resultOrExceptionOrBuilder.clear();
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
          if (((KnobRuntime.check(java.util.UUID.fromString("570b3e96-4191-3943-890e-441145ed86aa"), "regionserver", this.regionServer)) ? ((context != null) || ((closeCallBack) != (null))) : (context != null && closeCallBack == null))) {
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
    if (cellScanner == null) {
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
      LOG.error("Error while skipping Cells in CellScanner for invalid Region Mutations", e);
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
if(KnobRuntime.check(java.util.UUID.fromString("b75b87e6-9e38-3f69-92fc-3313c3e584e8"), "regionserver", this.regionServer)) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4391620-ac25-36ee-887d-3fdb2a0a14be"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = request.getClass().getDeclaredField("nonceGroup_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(request));
    _knob_field_.set(request, oldValue - 1);
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
    if (controller != null) {
      controller.setCellScanner(null);
    }
    try {
      checkOpen();
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

      if (request.hasCondition()) {
if(KnobRuntime.check(java.util.UUID.fromString("25f26a09-d136-368d-bd82-17f31bd41f8a"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43d4e0cc-3aee-3f0f-94c1-09657a47b2ef"), "regionserver", this.regionServer)) {
nonceGroup = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2a2082e-488e-3dc5-8a2b-2d6250ab99e8"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba9ee3a0-0e20-3e76-a938-ed8daaaff2d0"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = mutation.getClass().getDeclaredField("mutateType_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(mutation));
    _knob_field_.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d4cba09-b0f9-39b8-95ad-a5e5d988c575"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d019162a-852f-3b47-8b21-98b4fc20d7ca"), "regionserver", this.regionServer)) {
nonceGroup = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b36222c6-3bc4-3c68-bc60-767b3677f82f"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
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
if(KnobRuntime.check(java.util.UUID.fromString("ce0f4b96-428a-3619-ba50-9066acc22f9e"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = mutation.getClass().getDeclaredField("mutateType_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(mutation));
    _knob_field_.set(mutation, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1aae8f92-fa19-3c52-aabe-0907cf6fcef3"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = mutation.getClass().getDeclaredField("nonce_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(mutation));
    _knob_field_.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f91dfbd-207c-3533-9e41-560fe5484038"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = mutation.getClass().getDeclaredField("mutateType_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(mutation));
    _knob_field_.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb11ab58-2b62-3938-8bd9-90cd621259b6"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            delete(region, quota, mutation, cellScanner, spaceQuotaEnforcement);
            processed = Boolean.TRUE;
            break;
          default:
            throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
        }
        if (processed != null) {
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
      regionServer.checkFileSystem();
      throw new ServiceException(ie);
    } finally {
      if (quota != null) {
        quota.close();
      }
    }
  }

  private void put(HRegion region, OperationQuota quota, MutationProto mutation,
    CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("082597c7-45b6-35a1-848c-cfd7edb0a382"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = mutation.getClass().getDeclaredField("mutateType_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(mutation));
    _knob_field_.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d1a7a6c-c6b9-3f5d-a112-9487ac6b1bc9"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab55da39-4870-3a36-9150-06416da2e15b"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86bd8571-3c39-3f4e-bc5f-1efa8dfbffe2"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b62facf-f760-3369-a465-5f7f0eaf9d45"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c8726ea-81fb-3b98-852f-f414ee736bf3"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("busyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1cf7d660-0af8-3800-bed0-e505ec80f445"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b65332d-36ff-3846-af99-a58f732ecff3"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = mutation.getClass().getDeclaredField("associatedCellCount_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(mutation));
    _knob_field_.set(mutation, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b82f8f38-7fe2-3657-af1d-b88e002fc204"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowProcessorTimeout");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
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
    region.put(put);

    MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
    if (metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      metricsRegionServer.updatePut(region, after - before);
    }
  }

  private void delete(HRegion region, OperationQuota quota, MutationProto mutation,
    CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("43ba4b60-1802-3583-9f4e-8d0f5a715a9f"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("070c4f21-9ea9-33bb-be41-e534ba573ca0"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ea73ee4-7e9e-3f8c-bb39-958c4ec8d97b"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62d34468-e5e1-3cd6-948e-a1adbb4cd4d0"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("openSeqNum");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61f9f5d6-d16a-3072-8fd0-570bb31a33e5"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8cc0d8e8-4046-38f3-ab0f-2198daae9494"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("207943a0-5a3e-38cd-aac5-45948f0bf4fb"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("4c43e9d5-d378-30ed-ab0c-e1243d6acbdc"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushPerChanges");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8cf9863-ec42-31c0-9faf-2dcfd068b775"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d544e6f-fe1e-30ac-9c88-f9640d257e6a"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = request.getClass().getDeclaredField("numberOfRows_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(request));
    _knob_field_.set(request, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4aeae381-c761-3bc6-a55e-a4cbdf4ef55c"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b00e549-05a8-3874-a8d4-b5d6c34ebc8d"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("openSeqNum");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9901309-1a10-3080-8bd9-e70d0e577d70"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    ClientProtos.Scan protoScan = request.getScan();
    boolean isLoadingCfsOnDemandSet = protoScan.hasLoadColumnFamiliesOnDemand();
if(KnobRuntime.check(java.util.UUID.fromString("8e89f2cb-7640-3b96-8465-5012fc510fed"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = protoScan.getClass().getDeclaredField("cacheBlocks_");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(protoScan);
    _knob_field_.set(protoScan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7242d8d-d5a2-3f7b-8a01-151848556801"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = protoScan.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(protoScan));
    _knob_field_.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0397f9e9-6444-3e34-8569-badd8ea3e9f5"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = protoScan.getClass().getDeclaredField("readType_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(protoScan));
    _knob_field_.set(protoScan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8a80a87-969a-37c9-a43d-2a70b5fcb172"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = protoScan.getClass().getDeclaredField("readType_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(protoScan));
    _knob_field_.set(protoScan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9ec2bd2e-0de9-3051-84c3-c17226e06aa4"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = protoScan.getClass().getDeclaredField("maxVersions_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(protoScan));
    _knob_field_.set(protoScan, oldValue / 2);
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
    if (((KnobRuntime.check(java.util.UUID.fromString("9c999c2c-a46f-3d61-8d8e-f12094ca7df0"), "regionserver", this.regionServer)) ? ((region.getCoprocessorHost()) == (null)) : (region.getCoprocessorHost() != null))) {
      // preScannerOpen is not allowed to return a RegionScanner. Only post hook can create a
      // wrapper for the core created RegionScanner
      region.getCoprocessorHost().preScannerOpen(scan);
    }
    RegionScannerImpl coreScanner = region.getScanner(scan);
    Shipper shipper = coreScanner;
    RegionScanner scanner = coreScanner;
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("c3b9de5d-5b04-3fe8-9f31-5ba3614179c3"), "regionserver", this.regionServer)) ? ((region.getCoprocessorHost()) == (null)) : (region.getCoprocessorHost() != null))) {
if(KnobRuntime.check(java.util.UUID.fromString("b2507614-ac61-3302-b749-a05688877212"), "regionserver", this.regionServer)) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("48915167-f114-375a-8438-268b156eebb7"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("mvccReadPoint");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf949858-226b-33e5-bf96-d5251f42b73f"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("storeLimit");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("987eecf8-c8a6-3980-bdbe-38ea532fc5bb"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("storeOffset");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue - 1);
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
      scanner.close();
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
    // Set the time limit to be half of the more restrictive timeout value (one of the
    // timeout values must be positive). In the event that both values are positive, the
    // more restrictive of the two is used to calculate the limit.
    if (allowHeartbeatMessages) {
      long now = EnvironmentEdgeManager.currentTime();
      long remainingTimeout = getRemainingRpcTimeout(rpcCall, controller, now);
      if (scannerLeaseTimeoutPeriod > 0 || remainingTimeout > 0) {
        long timeLimitDelta;
        if (scannerLeaseTimeoutPeriod > 0 && remainingTimeout > 0) {
if(KnobRuntime.check(java.util.UUID.fromString("98631151-b2d6-3b59-a53a-390b2e034307"), "regionserver", this.regionServer)) {
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
      builder.setMoreResults(false);
    }
  }

  // return whether we have more results in region.
  private void scan(HBaseRpcController controller, ScanRequest request, RegionScannerHolder rsh,
    long maxQuotaResultSize, int maxResults, int limitOfRows, List<Result> results,
    ScanResponse.Builder builder, RpcCall rpcCall) throws IOException {
    HRegion region = rsh.r;
    RegionScanner scanner = rsh.s;
    long maxResultSize;
    if (scanner.getMaxResultSize() > 0) {
      maxResultSize = Math.min(scanner.getMaxResultSize(), maxQuotaResultSize);
    } else {
      maxResultSize = maxQuotaResultSize;
    }
    // This is cells inside a row. Default size is 10 so if many versions or many cfs,
    // then we'll resize. Resizings show in profiler. Set it higher than 10. For now
    // arbitrary 32. TODO: keep record of general size of results being returned.
    ArrayList<Cell> values = new ArrayList<>(32);
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
        if (rpcCall != null) {
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
            if (limitOfRows > 0) {
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
if(KnobRuntime.check(java.util.UUID.fromString("106cbb89-cd41-33d4-ba0b-624366001fb7"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = builder.getClass().getDeclaredField("scannerId_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(builder));
    _knob_field_.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03aa3986-aada-3984-877a-42e19bd85868"), "regionserver", this.regionServer)) {
numOfCompleteRows -= 1;
}
                  checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext,
                    builder);
                }
              }
              if (builder.hasMoreResults() && !builder.getMoreResults()) {
                break;
              }
            }
            boolean mayHaveMoreCellsInRow = scannerContext.mayHaveMoreCellsInRow();
            Result r = Result.create(values, null, stale, mayHaveMoreCellsInRow);
            results.add(r);
            numOfResults++;
            if (((KnobRuntime.check(java.util.UUID.fromString("a51936e6-af61-3d24-a8de-69366ecd8a36"), "regionserver", this.regionServer)) ? ((!mayHaveMoreCellsInRow) && ((limitOfRows) >= (0))) : (!mayHaveMoreCellsInRow && limitOfRows > 0))) {
              numOfCompleteRows++;
if(KnobRuntime.check(java.util.UUID.fromString("794691ba-32a6-3c27-bc15-fbb3f7218a70"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = builder.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(builder));
    _knob_field_.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("099a7280-035d-3ab2-810e-82b01ba3c3a5"), "regionserver", this.regionServer)) {
limitOfRows -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f1ac522-2019-3536-941d-5c5d27767c8a"), "regionserver", this.regionServer)) {
limitOfRows = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("61cae337-ebe7-3462-8469-14b0b7c78c79"), "regionserver", this.regionServer)) {
limitOfRows *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9b7aaf8d-4def-332e-a8d7-9ec158415241"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = builder.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(builder));
    _knob_field_.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              if (KnobRuntime.check(java.util.UUID.fromString("ef57d882-b5ef-382c-9d8a-cbca7d874de4"), "regionserver", this.regionServer)) { checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext, builder.setHeartbeatMessage(true)); } else { checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext, builder); }
              if (builder.hasMoreResults() && !builder.getMoreResults()) {
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

          if (limitReached || !moreRows) {
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
          rpcCall.incrementResponseCellSize(scannerContext.getHeapSizeProgress());
        }
if(KnobRuntime.check(java.util.UUID.fromString("ec14f5d8-68d4-37c1-a1dd-ad4ce84b0ef6"), "regionserver", this.regionServer)) {
moreRows = !moreRows;
}
        if (KnobRuntime.check(java.util.UUID.fromString("d8e85e8f-e995-3bd4-aa33-ecaa5cd03fe7"), "regionserver", this.regionServer)) { builder.setMoreResults(!moreRows); } else { builder.setMoreResultsInRegion(moreRows); }
        // Check to see if the client requested that we track metrics server side. If the
        // client requested metrics, retrieve the metrics from the scanner context.
        if (trackMetrics) {
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
      region.closeRegionOperation();
      // Update serverside metrics, even on error.
      long end = EnvironmentEdgeManager.currentTime();
      long responseCellSize = 0;
      long blockBytesScanned = 0;
      if (rpcCall != null) {
        responseCellSize = rpcCall.getResponseCellSize();
        blockBytesScanned = rpcCall.getBlockBytesScanned();
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
            leaseManager.cancelLease(scannerName);
          } catch (LeaseException le) {
            // No problem, ignore
            if (LOG.isTraceEnabled()) {
              LOG.trace("Un-able to cancel lease of scanner. It could already be closed.");
            }
          }
        }
      }
      throw new ServiceException(e);
    }
    requestCount.increment();
    rpcScanRequestCount.increment();
    RegionScannerContext rsx;
    ScanResponse.Builder builder = ScanResponse.newBuilder();
    try {
      rsx = checkQuotaAndGetRegionScannerContext(request, builder);
    } catch (IOException e) {
      if (((KnobRuntime.check(java.util.UUID.fromString("9d4db94a-b425-38cb-8a14-38388ede6709"), "regionserver", this.regionServer)) ? ((e) == (SCANNER_ALREADY_CLOSED)) : (e == SCANNER_ALREADY_CLOSED))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("c6c54140-7a00-33d6-9637-4c60ac66cc18"), "regionserver", this.regionServer)) ? (request.hasRegion()) : (request.hasNumberOfRows()))) {
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
    if (request.hasLimitOfRows()) {
      limitOfRows = request.getLimitOfRows();
    } else {
      limitOfRows = -1;
    }
    boolean scannerClosed = false;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("3903a533-1286-3fb6-8aaa-3241a1f1b13f"), "regionserver", this.regionServer)) {
rows = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("20a6b15f-1bf5-3ad1-aa53-bdfac4441d68"), "regionserver", this.regionServer)) {
rows *= 2;
}
      List<Result> results = new ArrayList<>(Math.min(rows, 512));
      if (((KnobRuntime.check(java.util.UUID.fromString("ca3f2731-daee-3d1d-a809-8573fa03b1f3"), "regionserver", this.regionServer)) ? ((rows) > (0)) : (rows > 0))) {
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
          if (bypass != null && bypass.booleanValue()) {
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
        builder.setMoreResultsInRegion(true);
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
        builder.setMoreResults(false);
      }
      // Later we may close the scanner depending on this flag so here we need to make sure that we
      // have already set this flag.
      assert ((KnobRuntime.check(java.util.UUID.fromString("61e646c9-d720-32e4-9b50-0f9747fc4572"), "regionserver", this.regionServer)) ? (builder.hasMoreResults()) : (builder.hasMoreResultsInRegion()));
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
if(KnobRuntime.check(java.util.UUID.fromString("0a0f7762-5c4d-3edc-97b1-813b43d107de"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e56ea851-417e-3483-8bc9-25077c4583d2"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2eef0a8d-941b-3798-9116-ba9897392af5"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78e4272c-83d5-3ab4-b046-0f50b0ff4a95"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        closeScanner(region, scanner, scannerName, rpcCall, false);
      }

      // There's no point returning to a timed out client. Throwing ensures scanner is closed
      if (((KnobRuntime.check(java.util.UUID.fromString("ecdab485-a9c2-3f82-9c2f-08ad52a08830"), "regionserver", this.regionServer)) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) < (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("efb29ee6-f198-3ccd-b1d6-0e4d61fe8c08"), "regionserver", this.regionServer)) ? ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("03b166f2-8206-30a0-91cf-ec888227cd9f"), "regionserver", this.regionServer)) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("ba292e16-f4b2-39fb-92ce-dcec3bd7f160"), "regionserver", this.regionServer)) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("8641ed3a-424b-3c96-8fc0-d0464f314b40"), "regionserver", this.regionServer)) ? (((rpcCall) != (null)) && ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("ce436850-585d-31bd-8984-1ad2455d9745"), "regionserver", this.regionServer)) ? (((rpcCall) != (null)) || ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("fbe0c00c-b7dd-3ff7-b2bc-aac043cb8565"), "regionserver", this.regionServer)) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) != (rpcCall.getDeadline()))) : (((KnobRuntime.check(java.util.UUID.fromString("28e8f713-3c53-375f-bf06-b74cf0936995"), "regionserver", this.regionServer)) ? ((rpcCall != null) && ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime()))) : (((KnobRuntime.check(java.util.UUID.fromString("82ef5a0c-c7bb-385f-8db8-93d31c21c9c5"), "regionserver", this.regionServer)) ? ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("fc1b9867-0175-3cc2-8b80-2aadc93975d0"), "regionserver", this.regionServer)) ? (((rpcCall) == (null)) || ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime()))) : (rpcCall != null && EnvironmentEdgeManager.currentTime() > rpcCall.getDeadline()))))))))))))))))))))) {
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
      rsh.shippedCallback.run();
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
    rpcPreCheck("execRegionServerService");
    return regionServer.execRegionServerService(controller, request);
  }

  @Override
  public UpdateConfigurationResponse updateConfiguration(RpcController controller,
    UpdateConfigurationRequest request) throws ServiceException {
    try {
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
if(KnobRuntime.check(java.util.UUID.fromString("e51836a2-d29d-3735-a9ca-602b55ef95dd"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      TableDescriptor tableDesc = tdCache.get(tableName);
      if (tableDesc == null) {
        try {
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
if(KnobRuntime.check(java.util.UUID.fromString("4a3f9b89-2f53-377e-a442-48282f7c7915"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("unitKB");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("193fe954-10db-3da0-97ab-2a7a690fa328"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("flushCheckFrequency");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8748f957-1079-3f97-8e48-904eabd47ac2"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("retryPauseTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue + 1);
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
    if (regionServer.submitRegionProcedure(procId)) {
if(KnobRuntime.check(java.util.UUID.fromString("f555b1e6-747a-3337-815c-4b0b731b38a8"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("unitKB");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b149f996-0ebf-398b-88be-bf1637a1ec51"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("retryPauseTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf17db93-8313-37a4-897b-ed02d760d01c"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("msgInterval");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5640b413-ef5a-34e6-a4e7-7e89178bca70"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("threadWakeFrequency");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0159463-5efe-3497-88fa-5ff330f97e1a"), "regionserver", this.regionServer)) {
try {
    java.lang.reflect.Field _knob_field_ = regionServer.getClass().getDeclaredField("operationTimeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionServer));
    _knob_field_.set(regionServer, oldValue - 1);
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
      if (((KnobRuntime.check(java.util.UUID.fromString("df502af2-bd94-38b1-bbb2-07808ebe7974"), "regionserver", this.regionServer)) ? ((request.getProcCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4c72b49a-2251-3bbc-a1bb-32468205bae5"), "regionserver", this.regionServer)) ? ((request.getProcCount()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d45161a1-7a2a-3661-bf5c-9e03cad93ab0"), "regionserver", this.regionServer)) ? ((request.getCloseRegionCount()) == (0)) : (request.getProcCount() > 0))))))) {
        request.getProcList().forEach(this::executeProcedures);
      }
      regionServer.getRegionServerCoprocessorHost().postExecuteProcedures();
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
