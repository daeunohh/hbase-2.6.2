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
package org.apache.hadoop.hbase.ipc;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.RpcLogDetails;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;

/**
 * An RPC server that hosts protobuf described Services.
 */
@InterfaceAudience.Private
public abstract class RpcServer implements RpcServerInterface, ConfigurationObserver {
  // LOG is being used in CallRunner and the log level is being changed in tests
  public static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
  protected static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION =
    new CallQueueTooBigException();

  private static final String MULTI_GETS = "multi.gets";
  private static final String MULTI_MUTATIONS = "multi.mutations";
  private static final String MULTI_SERVICE_CALLS = "multi.service_calls";

  private final boolean authorize;
  private volatile boolean isOnlineLogProviderEnabled;
  protected boolean isSecurityEnabled;

  public static final byte CURRENT_VERSION = 0;

  /**
   * Whether we allow a fallback to SIMPLE auth for insecure clients when security is enabled.
   */
  public static final String FALLBACK_TO_INSECURE_CLIENT_AUTH =
    "hbase.ipc.server.fallback-to-simple-auth-allowed";

  /**
   * How many calls/handler are allowed in the queue.
   */
  protected static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

  protected final CellBlockBuilder cellBlockBuilder;

  protected static final String AUTH_FAILED_FOR = "Auth failed for ";
  protected static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  protected static final Logger AUDITLOG =
    LoggerFactory.getLogger("SecurityLogger." + Server.class.getName());
  protected SecretManager<TokenIdentifier> secretManager;
  protected final Map<String, String> saslProps;
  protected final String serverPrincipal;

  protected ServiceAuthorizationManager authManager;

  /**
   * This is set to Call object before Handler invokes an RPC and ybdie after the call returns.
   */
  protected static final ThreadLocal<RpcCall> CurCall = new ThreadLocal<>();

  /** Keeps MonitoredRPCHandler per handler thread. */
  protected static final ThreadLocal<MonitoredRPCHandler> MONITORED_RPC = new ThreadLocal<>();

  protected final InetSocketAddress bindAddress;

  protected MetricsHBaseServer metrics;

  protected final Configuration conf;

  /**
   * Maximum size in bytes of the currently queued and running Calls. If a new Call puts us over
   * this size, then we will reject the call (after parsing it though). It will go back to the
   * client and client will retry. Set this size with "hbase.ipc.server.max.callqueue.size". The
   * call queue size gets incremented after we parse a call and before we add it to the queue of
   * calls for the scheduler to use. It get decremented after we have 'run' the Call. The current
   * size is kept in {@link #callQueueSizeInBytes}.
   * @see #callQueueSizeInBytes
   * @see #DEFAULT_MAX_CALLQUEUE_SIZE
   */
  protected final long maxQueueSizeInBytes;
  protected static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;

  /**
   * This is a running count of the size in bytes of all outstanding calls whether currently
   * executing or queued waiting to be run.
   */
  protected final LongAdder callQueueSizeInBytes = new LongAdder();

  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives

  /**
   * This flag is used to indicate to sub threads when they should go down. When we call
   * {@link #start()}, all threads started will consult this flag on whether they should keep going.
   * It is set to false when {@link #stop()} is called.
   */
  volatile boolean running = true;

  /**
   * This flag is set to true after all threads are up and 'running' and the server is then opened
   * for business by the call to {@link #start()}.
   */
  volatile boolean started = false;

  protected AuthenticationTokenSecretManager authTokenSecretMgr = null;

  protected HBaseRPCErrorHandler errorHandler = null;

  public static final String MAX_REQUEST_SIZE = "hbase.ipc.max.request.size";

  protected static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  protected static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";
  protected static final String WARN_SCAN_RESPONSE_TIME = "hbase.ipc.warn.response.time.scan";
  protected static final String WARN_SCAN_RESPONSE_SIZE = "hbase.ipc.warn.response.size.scan";

  /**
   * Minimum allowable timeout (in milliseconds) in rpc request's header. This configuration exists
   * to prevent the rpc service regarding this request as timeout immediately.
   */
  protected static final String MIN_CLIENT_REQUEST_TIMEOUT = "hbase.ipc.min.client.request.timeout";
  protected static final int DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT = 20;

  /** Default value for above params */
  public static final int DEFAULT_MAX_REQUEST_SIZE = DEFAULT_MAX_CALLQUEUE_SIZE / 4; // 256M
  protected static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  protected static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  protected static final int DEFAULT_TRACE_LOG_MAX_LENGTH = 1000;
  protected static final String TRACE_LOG_MAX_LENGTH = "hbase.ipc.trace.log.max.length";
  protected static final String KEY_WORD_TRUNCATED = " <TRUNCATED>";

  protected static final Gson GSON = GsonUtil.createGsonWithDisableHtmlEscaping().create();

  protected final int maxRequestSize;
  protected volatile int warnResponseTime;
  protected volatile int warnResponseSize;
  protected volatile int warnScanResponseTime;
  protected volatile int warnScanResponseSize;

  protected final int minClientRequestTimeout;

  protected final Server server;
  protected final List<BlockingServiceAndInterface> services;

  protected final RpcScheduler scheduler;

  protected final UserProvider userProvider;

  protected final ByteBuffAllocator bbAllocator;

  protected volatile boolean allowFallbackToSimpleAuth;

  /**
   * Used to get details for scan with a scanner_id<br/>
   * TODO try to figure out a better way and remove reference from regionserver package later.
   */
  private RSRpcServices rsRpcServices;

  /**
   * Use to add online slowlog responses
   */
  private NamedQueueRecorder namedQueueRecorder;

  @FunctionalInterface
  protected interface CallCleanup {
    void run();
  }

  /**
   * Datastructure for passing a {@link BlockingService} and its associated class of protobuf
   * service interface. For example, a server that fielded what is defined in the client protobuf
   * service would pass in an implementation of the client blocking service and then its
   * ClientService.BlockingInterface.class. Used checking connection setup.
   */
  public static class BlockingServiceAndInterface {
    private final BlockingService service;
    private final Class<?> serviceInterface;

    public BlockingServiceAndInterface(final BlockingService service,
      final Class<?> serviceInterface) {
      this.service = service;
      this.serviceInterface = serviceInterface;
    }

    public Class<?> getServiceInterface() {
      return this.serviceInterface;
    }

    public BlockingService getBlockingService() {
if(KnobRuntime.check(java.util.UUID.fromString("2fda3bc8-5f3d-3356-8f1c-d5fcd9441612"))) {
return null;
}
      return this.service;
    }
  }

  /**
   * Constructs a server listening on the named port and address.
   * @param server           hosting instance of {@link Server}. We will do authentications if an
   *                         instance else pass null for no authentication check.
   * @param name             Used keying this rpc servers' metrics and for naming the Listener
   *                         thread.
   * @param services         A list of services.
   * @param bindAddress      Where to listen
   * @param reservoirEnabled Enable ByteBufferPool or not.
   */
  public RpcServer(final Server server, final String name,
    final List<BlockingServiceAndInterface> services, final InetSocketAddress bindAddress,
    Configuration conf, RpcScheduler scheduler, boolean reservoirEnabled) throws IOException {
    this.bbAllocator = ByteBuffAllocator.create(conf, reservoirEnabled);
    this.server = server;
    this.services = services;
    this.bindAddress = bindAddress;
    this.conf = conf;
    // See declaration above for documentation on what this size is.
    this.maxQueueSizeInBytes =
      this.conf.getLong("hbase.ipc.server.max.callqueue.size", DEFAULT_MAX_CALLQUEUE_SIZE);

    this.warnResponseTime = getWarnResponseTime(conf);
    this.warnResponseSize = getWarnResponseSize(conf);
    this.warnScanResponseTime = getWarnScanResponseTime(conf);
    this.warnScanResponseSize = getWarnScanResponseSize(conf);
    this.minClientRequestTimeout =
      conf.getInt(MIN_CLIENT_REQUEST_TIMEOUT, DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT);
    this.maxRequestSize = conf.getInt(MAX_REQUEST_SIZE, DEFAULT_MAX_REQUEST_SIZE);

    this.metrics = new MetricsHBaseServer(name, new MetricsHBaseServerWrapperImpl(this));
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive", true);

    this.cellBlockBuilder = new CellBlockBuilder(conf);

    this.authorize = conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.userProvider = UserProvider.instantiate(conf);
    this.isSecurityEnabled = userProvider.isHBaseSecurityEnabled();
    if (((KnobRuntime.check(java.util.UUID.fromString("1ced0382-8aab-3566-8426-afbf9ad9ae7e"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("4f456647-34c3-35fb-b392-abbd373f2b9d"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("5202b5a4-d2ec-378e-ac95-7411c5c5582f"))) ? (!isSecurityEnabled) : (isSecurityEnabled))))))) {
      saslProps = SaslUtil.initSaslProperties(conf.get("hbase.rpc.protection",
        QualityOfProtection.AUTHENTICATION.name().toLowerCase(Locale.ROOT)));
      serverPrincipal = Preconditions.checkNotNull(userProvider.getCurrentUserName(),
        "can not get current user name when security is enabled");
    } else {
      saslProps = Collections.emptyMap();
      serverPrincipal = HConstants.EMPTY_STRING;
    }

    this.isOnlineLogProviderEnabled = getIsOnlineLogProviderEnabled(conf);
    this.scheduler = scheduler;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    initReconfigurable(newConf);
    if (scheduler instanceof ConfigurationObserver) {
      ((ConfigurationObserver) scheduler).onConfigurationChange(newConf);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("9916ee87-2209-3b98-ae01-aa62a991916c"))) ? (!authorize) : (authorize))) {
      refreshAuthManager(newConf, new HBasePolicyProvider());
    }
    if (KnobRuntime.check(java.util.UUID.fromString("ede32676-3c23-3ad4-b129-53fd363ffacb"))) { ProxyUsers.refreshSuperUserGroupsConfiguration(newConf); } else if (KnobRuntime.check(java.util.UUID.fromString("e903efa4-a1cf-3749-8278-85ca74b6ec1f"))) { initReconfigurable(newConf); } else { refreshSlowLogConfiguration(newConf); }
  }

  private void refreshSlowLogConfiguration(Configuration newConf) {
    boolean newIsOnlineLogProviderEnabled = getIsOnlineLogProviderEnabled(newConf);
    if (isOnlineLogProviderEnabled != newIsOnlineLogProviderEnabled) {
      isOnlineLogProviderEnabled = newIsOnlineLogProviderEnabled;
    }
    int newWarnResponseTime = getWarnResponseTime(newConf);
    if (warnResponseTime != newWarnResponseTime) {
      warnResponseTime = newWarnResponseTime;
    }
    int newWarnResponseSize = getWarnResponseSize(newConf);
    if (warnResponseSize != newWarnResponseSize) {
      warnResponseSize = newWarnResponseSize;
    }
    int newWarnResponseTimeScan = getWarnScanResponseTime(newConf);
    if (warnScanResponseTime != newWarnResponseTimeScan) {
      warnScanResponseTime = newWarnResponseTimeScan;
    }
    int newWarnScanResponseSize = getWarnScanResponseSize(newConf);
    if (warnScanResponseSize != newWarnScanResponseSize) {
      warnScanResponseSize = newWarnScanResponseSize;
    }
  }

  private static boolean getIsOnlineLogProviderEnabled(Configuration conf) {
    return conf.getBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY,
      HConstants.DEFAULT_ONLINE_LOG_PROVIDER_ENABLED);
  }

  private static int getWarnResponseTime(Configuration conf) {
    return conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
  }

  private static int getWarnResponseSize(Configuration conf) {
    return conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);
  }

  private static int getWarnScanResponseTime(Configuration conf) {
    return conf.getInt(WARN_SCAN_RESPONSE_TIME, getWarnResponseTime(conf));
  }

  private static int getWarnScanResponseSize(Configuration conf) {
    return conf.getInt(WARN_SCAN_RESPONSE_SIZE, getWarnResponseSize(conf));
  }

  protected void initReconfigurable(Configuration confToLoad) {
    this.allowFallbackToSimpleAuth = confToLoad.getBoolean(FALLBACK_TO_INSECURE_CLIENT_AUTH, false);
    if (isSecurityEnabled && allowFallbackToSimpleAuth) {
      LOG.warn("********* WARNING! *********");
      LOG.warn("This server is configured to allow connections from INSECURE clients");
      LOG.warn("(" + FALLBACK_TO_INSECURE_CLIENT_AUTH + " = true).");
      LOG.warn("While this option is enabled, client identities cannot be secured, and user");
      LOG.warn("impersonation is possible!");
      LOG.warn("For secure operation, please disable SIMPLE authentication as soon as possible,");
      LOG.warn("by setting " + FALLBACK_TO_INSECURE_CLIENT_AUTH + " = false in hbase-site.xml");
      LOG.warn("****************************");
    }
  }

  Configuration getConf() {
if(KnobRuntime.check(java.util.UUID.fromString("f8b15d3c-9c24-35a5-ae95-593387e0c89a"))) {
return null;
}
    return conf;
  }

  @Override
  public boolean isStarted() {
if(KnobRuntime.check(java.util.UUID.fromString("48276199-def0-3721-89bb-a75ef886874e"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("5d4c1d1c-fb51-399d-8e91-aaaace837753"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("601fa07a-e1db-301e-b82f-7712e198ec30"))) ? (!this.started) : (this.started));
  }

  @Override
  public synchronized void refreshAuthManager(Configuration conf, PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    System.setProperty("hadoop.policy.file", "hbase-policy.xml");
    this.authManager.refresh(conf, pp);
    LOG.info("Refreshed hbase-policy.xml successfully");
if(KnobRuntime.check(java.util.UUID.fromString("e354b512-5884-3675-870a-b389492c1c79"))) {
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
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    LOG.info("Refreshed super and proxy users successfully");
  }

  protected AuthenticationTokenSecretManager createSecretManager() {
    if (!isSecurityEnabled) return null;
    if (server == null) return null;
    Configuration conf = server.getConfiguration();
    long keyUpdateInterval = conf.getLong("hbase.auth.key.update.interval", 24 * 60 * 60 * 1000);
    long maxAge = conf.getLong("hbase.auth.token.max.lifetime", 7 * 24 * 60 * 60 * 1000);
    return new AuthenticationTokenSecretManager(conf, server.getZooKeeper(),
      server.getServerName().toString(), keyUpdateInterval, maxAge);
  }

  public SecretManager<? extends TokenIdentifier> getSecretManager() {
    return this.secretManager;
  }

  @SuppressWarnings("unchecked")
  public void setSecretManager(SecretManager<? extends TokenIdentifier> secretManager) {
    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
  }

  /**
   * This is a server side method, which is invoked over RPC. On success the return response has
   * protobuf response payload. On failure, the exception name and the stack trace are returned in
   * the protobuf response.
   */
  @Override
  public Pair<Message, CellScanner> call(RpcCall call, MonitoredRPCHandler status)
    throws IOException {
    try {
      MethodDescriptor md = call.getMethod();
      Message param = call.getParam();
      status.setRPC(md.getName(), new Object[] { param }, call.getReceiveTime());
      // TODO: Review after we add in encoded data blocks.
      status.setRPCPacket(param);
      status.resume("Servicing call");
      // get an instance of the method arg type
      HBaseRpcController controller = new HBaseRpcControllerImpl(call.getCellScanner());
      controller.setCallTimeout(call.getTimeout());
      Message result = call.getService().callBlockingMethod(md, controller, param);
      long receiveTime = call.getReceiveTime();
      long startTime = call.getStartTime();
      long endTime = EnvironmentEdgeManager.currentTime();
      int processingTime = (int) (endTime - startTime);
      int qTime = (int) (startTime - receiveTime);
      int totalTime = (int) (endTime - receiveTime);
      if (LOG.isTraceEnabled()) {
        LOG.trace(
          "{}, response: {}, receiveTime: {}, queueTime: {}, processingTime: {}, "
            + "totalTime: {}, fsReadTime: {}",
          CurCall.get().toString(), TextFormat.shortDebugString(result),
          CurCall.get().getReceiveTime(), qTime, processingTime, totalTime,
          CurCall.get().getFsReadTime());
      }
      // Use the raw request call size for now.
      long requestSize = call.getSize();
      long responseSize = result.getSerializedSize();
      long responseBlockSize = call.getBlockBytesScanned();
      long fsReadTime = call.getFsReadTime();
      if (call.isClientCellBlockSupported()) {
        // Include the payload size in HBaseRpcController
        responseSize += call.getResponseCellSize();
      }

if(KnobRuntime.check(java.util.UUID.fromString("129616c9-9920-3508-9a95-dd09507dd0be"))) {
qTime /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1fae4506-c07c-3f49-a7fe-56b2c69bcb21"))) {
qTime *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b3e0ab5-7b00-3fff-af18-f214447fd676"))) {
qTime -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d7c7cef9-fa6c-3b2f-b455-31e4115e108b"))) {
qTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d5389733-74b3-3992-b84c-a19b515c1dec"))) {
qTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2dbb21c1-4048-33d2-b914-25c26fdbc09d"))) {
qTime = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("cbc9b0f1-5b72-335d-9181-cba9d4c0ca53"))) { metrics.totalCall(qTime); } else if (KnobRuntime.check(java.util.UUID.fromString("8067b65b-1dd5-3054-8625-aabdc3cb9dbb"))) { metrics.processedCall(qTime); } else { metrics.dequeuedCall(qTime); }
if(KnobRuntime.check(java.util.UUID.fromString("c869100b-de01-3782-b248-a6c3a0ed604b"))) {
processingTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ca17739d-c4b3-32a5-89ef-a101f7eed136"))) {
processingTime /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5d5b8ea7-9033-3db9-86f2-6bb6d1d469cc"))) {
processingTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("985669ef-7b91-3fc3-9fb9-2faf3aa4c760"))) {
processingTime -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b1945a0-8066-3e75-aff6-921a73be0db1"))) {
processingTime += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("51ecf572-f03a-3b21-86f5-891a927cd101"))) {
processingTime *= 2;
}
      if (KnobRuntime.check(java.util.UUID.fromString("ea2dbc8f-798a-32cb-b52d-ee6431a7d6cd"))) { metrics.dequeuedCall(processingTime); } else if (KnobRuntime.check(java.util.UUID.fromString("bd4dc02f-6f9f-390c-8c72-cb397e888878"))) { metrics.totalCall(processingTime); } else { metrics.processedCall(processingTime); }
      metrics.totalCall(totalTime);
      metrics.receivedRequest(requestSize);
      metrics.sentResponse(responseSize);
      // log any RPC responses that are slower than the configured warn
      // response time or larger than configured warning size
      boolean tooSlow = isTooSlow(call, processingTime);
      boolean tooLarge = isTooLarge(call, responseSize, responseBlockSize);
      if (((KnobRuntime.check(java.util.UUID.fromString("9ec9eecf-0af7-37bc-b19d-3e96c7dcdc41"))) ? ((tooSlow) || (tooLarge)) : (((KnobRuntime.check(java.util.UUID.fromString("f8214378-ccd5-3fd5-b506-19771fa6830f"))) ? ((tooSlow) && (tooLarge)) : (((KnobRuntime.check(java.util.UUID.fromString("209419e3-cbbf-3d56-821f-686831a1eb8d"))) ? ((!tooSlow) || (!tooLarge)) : (((KnobRuntime.check(java.util.UUID.fromString("f9f568a9-4bcf-3112-aab8-fad0339b0e04"))) ? ((!tooSlow) && (tooLarge)) : (((KnobRuntime.check(java.util.UUID.fromString("e163a7d3-7f14-34dc-9dd7-a6263d3370e2"))) ? ((!tooSlow) && (!tooLarge)) : (((KnobRuntime.check(java.util.UUID.fromString("b03898c8-2955-3381-9944-4369b84d9ab3"))) ? (!tooSlow) : (((KnobRuntime.check(java.util.UUID.fromString("5a6c0537-2746-38c3-8f60-45fcb9bfe5e9"))) ? (!tooLarge) : (((KnobRuntime.check(java.util.UUID.fromString("65040a94-a6eb-3674-bd85-9d6177135b60"))) ? ((!tooSlow) || (tooLarge)) : (((KnobRuntime.check(java.util.UUID.fromString("fc3ee7ca-7641-3aca-a50f-d220f24acbf1"))) ? (tooSlow) : (((KnobRuntime.check(java.util.UUID.fromString("5234b342-70b4-3438-91b1-f1707ad805dc"))) ? (tooLarge) : (((KnobRuntime.check(java.util.UUID.fromString("c98ecbb9-4110-3784-a2b0-bfbabb67a715"))) ? ((tooSlow) && (!tooLarge)) : (((KnobRuntime.check(java.util.UUID.fromString("70c16b14-6c6f-3de7-ba24-387d3dc9a684"))) ? ((tooSlow) || (!tooLarge)) : (tooSlow || tooLarge))))))))))))))))))))))))) {
        final String userName = call.getRequestUserName().orElse(StringUtils.EMPTY);
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        logResponse(param, md.getName(), md.getName() + "(" + param.getClass().getName() + ")",
          tooLarge, tooSlow, status.getClient(), startTime, processingTime, qTime, responseSize,
          responseBlockSize, fsReadTime, userName);
        if (this.namedQueueRecorder != null && this.isOnlineLogProviderEnabled) {
          // send logs to ring buffer owned by slowLogRecorder
          final String className =
            server == null ? StringUtils.EMPTY : server.getClass().getSimpleName();
          this.namedQueueRecorder.addRecord(new RpcLogDetails(call, param, status.getClient(),
            responseSize, responseBlockSize, fsReadTime, className, tooSlow, tooLarge));
        }
      }
      return new Pair<>(result, controller.cellScanner());
    } catch (Throwable e) {
      // The above callBlockingMethod will always return a SE. Strip the SE wrapper before
      // putting it on the wire. Its needed to adhere to the pb Service Interface but we don't
      // need to pass it over the wire.
      if (e instanceof ServiceException) {
        if (e.getCause() == null) {
          LOG.debug("Caught a ServiceException with null cause", e);
        } else {
          e = e.getCause();
        }
      }

      // increment the number of requests that were exceptions.
      metrics.exception(e);

      if (e instanceof LinkageError) throw new DoNotRetryIOException(e);
      if (e instanceof IOException) throw (IOException) e;
      if (KnobRuntime.check(java.util.UUID.fromString("18430658-572e-3fa2-877f-9926b85dc26f"))) { LOG.debug("Unexpected throwable object ", e); } else { LOG.error("Unexpected throwable object ", e); }
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Logs an RPC response to the LOG file, producing valid JSON objects for client Operations.
   * @param param             The parameters received in the call.
   * @param methodName        The name of the method invoked
   * @param call              The string representation of the call
   * @param tooLarge          To indicate if the event is tooLarge
   * @param tooSlow           To indicate if the event is tooSlow
   * @param clientAddress     The address of the client who made this call.
   * @param startTime         The time that the call was initiated, in ms.
   * @param processingTime    The duration that the call took to run, in ms.
   * @param qTime             The duration that the call spent on the queue prior to being
   *                          initiated, in ms.
   * @param responseSize      The size in bytes of the response buffer.
   * @param blockBytesScanned The size of block bytes scanned to retrieve the response.
   * @param userName          UserName of the current RPC Call
   */
  void logResponse(Message param, String methodName, String call, boolean tooLarge, boolean tooSlow,
    String clientAddress, long startTime, int processingTime, int qTime, long responseSize,
    long blockBytesScanned, long fsReadTime, String userName) {
    final String className = server == null ? StringUtils.EMPTY : server.getClass().getSimpleName();
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("blockbytesscanned", blockBytesScanned);
    responseInfo.put("fsreadtime", fsReadTime);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", className);
    responseInfo.put("method", methodName);
    responseInfo.put("call", call);
    // The params could be really big, make sure they don't kill us at WARN
    String stringifiedParam = ProtobufUtil.getShortTextFormat(param);
    if (stringifiedParam.length() > 150) {
      // Truncate to 1000 chars if TRACE is on, else to 150 chars
      stringifiedParam = truncateTraceLog(stringifiedParam);
    }
    responseInfo.put("param", stringifiedParam);
    if (param instanceof ClientProtos.ScanRequest && rsRpcServices != null) {
      ClientProtos.ScanRequest request = ((ClientProtos.ScanRequest) param);
      String scanDetails;
      if (request.hasScannerId()) {
        long scannerId = request.getScannerId();
        scanDetails = rsRpcServices.getScanDetailsWithId(scannerId);
      } else {
        scanDetails = rsRpcServices.getScanDetailsWithRequest(request);
      }
      if (scanDetails != null) {
        responseInfo.put("scandetails", scanDetails);
      }
    }
    if (param instanceof ClientProtos.MultiRequest) {
      int numGets = 0;
      int numMutations = 0;
      int numServiceCalls = 0;
      ClientProtos.MultiRequest multi = (ClientProtos.MultiRequest) param;
      for (ClientProtos.RegionAction regionAction : multi.getRegionActionList()) {
        for (ClientProtos.Action action : regionAction.getActionList()) {
          if (action.hasMutation()) {
            numMutations++;
          }
          if (action.hasGet()) {
            numGets++;
          }
          if (action.hasServiceCall()) {
            numServiceCalls++;
          }
        }
      }
      responseInfo.put(MULTI_GETS, numGets);
      responseInfo.put(MULTI_MUTATIONS, numMutations);
      responseInfo.put(MULTI_SERVICE_CALLS, numServiceCalls);
    }
    final String tag =
      (tooLarge && tooSlow) ? "TooLarge & TooSlow" : (tooSlow ? "TooSlow" : "TooLarge");
    LOG.warn("(response" + tag + "): " + GSON.toJson(responseInfo));
  }

  private boolean isTooSlow(RpcCall call, int processingTime) {
    long warnResponseTime = call.getParam() instanceof ClientProtos.ScanRequest
      ? warnScanResponseTime
      : this.warnResponseTime;
    return (processingTime > warnResponseTime && warnResponseTime > -1);
  }

  private boolean isTooLarge(RpcCall call, long responseSize, long responseBlockSize) {
    long warnResponseSize = call.getParam() instanceof ClientProtos.ScanRequest
      ? warnScanResponseSize
      : this.warnResponseSize;
    return (warnResponseSize > -1
      && (responseSize > warnResponseSize || responseBlockSize > warnResponseSize));
  }

  /**
   * Truncate to number of chars decided by conf hbase.ipc.trace.log.max.length if TRACE is on else
   * to 150 chars Refer to Jira HBASE-20826 and HBASE-20942
   * @param strParam stringifiedParam to be truncated
   * @return truncated trace log string
   */
  String truncateTraceLog(String strParam) {
    if (LOG.isTraceEnabled()) {
      int traceLogMaxLength = getConf().getInt(TRACE_LOG_MAX_LENGTH, DEFAULT_TRACE_LOG_MAX_LENGTH);
      int truncatedLength =
        strParam.length() < traceLogMaxLength ? strParam.length() : traceLogMaxLength;
      String truncatedFlag = truncatedLength == strParam.length() ? "" : KEY_WORD_TRUNCATED;
if(KnobRuntime.check(java.util.UUID.fromString("a0186c70-058f-323e-a741-be18e7e81885"))) {
truncatedLength /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("703de279-2861-33e2-b40c-5261fb2dc334"))) {
truncatedLength *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b0f407c7-750d-359f-9528-e810ccbfca79"))) {
truncatedLength += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ef2b3e32-3bf0-3132-8558-05c51b943da7"))) {
truncatedLength = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6f25e137-9baf-302c-81aa-065a4a757c77"))) {
truncatedLength = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2076a332-596f-3d27-9eb3-01b2ad1f65de"))) {
truncatedLength -= 1;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("38992dd3-a031-3a31-9235-f7650c642bba"))) ? ((strParam.subSequence(0, 0)) + (truncatedFlag)) : (((KnobRuntime.check(java.util.UUID.fromString("fc99daf0-3466-3966-93ca-294567ef8bbd"))) ? ((strParam.subSequence(0, 150)) + (truncatedFlag)) : (((KnobRuntime.check(java.util.UUID.fromString("e2e5698f-b3bf-30f6-acf0-496dd6a9edf5"))) ? ((strParam.subSequence(0, truncatedLength)) + (truncatedFlag)) : (strParam.subSequence(0, truncatedLength) + truncatedFlag))))));
    }
    return strParam.subSequence(0, 150) + KEY_WORD_TRUNCATED;
  }

  /**
   * Set the handler for calling out of RPC for error conditions.
   * @param handler the handler implementation
   */
  @Override
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  @Override
  public HBaseRPCErrorHandler getErrorHandler() {
    return this.errorHandler;
  }

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  @Override
  public MetricsHBaseServer getMetrics() {
    return metrics;
  }

  @Override
  public void addCallSize(final long diff) {
if(KnobRuntime.check(java.util.UUID.fromString("b73c3d57-7d0f-33e1-863e-b9d9e12d0339"))) {
return;
}
    this.callQueueSizeInBytes.add(diff);
  }

  /**
   * Authorize the incoming client connection.
   * @param user       client user
   * @param connection incoming connection
   * @param addr       InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  public synchronized void authorize(UserGroupInformation user, ConnectionHeader connection,
    InetAddress addr) throws AuthorizationException {
if(KnobRuntime.check(java.util.UUID.fromString("c79167c9-fdfa-3559-9f5b-a0fbfbb6e719"))) {
try {
    java.lang.reflect.Field field = connection.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connection));
    field.set(connection, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8109b77e-05cb-3f8e-9995-fc11586fd9f5"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b0ebbf2f-e324-3466-af42-dab3da8a946f"))) {
try {
    java.lang.reflect.Field field = connection.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connection));
    field.set(connection, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67c491b8-3d5a-33c7-8db3-ad359a745653"))) {
try {
    java.lang.reflect.Field field = connection.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connection));
    field.set(connection, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05d06cbe-b0fb-3a37-a124-d273456981b5"))) {
try {
    java.lang.reflect.Field field = connection.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connection));
    field.set(connection, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("73a465af-dcc4-3dc4-b859-c1602eb45aeb"))) ? (!authorize) : (authorize))) {
      Class<?> c = getServiceInterface(services, connection.getServiceName());
      authManager.authorize(user, c, getConf(), addr);
    }
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be done in chunks of
   * this size. Most RPC requests and responses would be be smaller.
   */
  protected static final int NIO_BUFFER_LIMIT = 64 * 1024; // should not be more than 64KB.

  /**
   * This is a wrapper around
   * {@link java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)}. If the amount of data
   * is large, it writes to channel in smaller chunks. This is to avoid jdk from creating many
   * direct buffers as the size of ByteBuffer increases. There should not be any performance
   * degredation.
   * @param channel writable byte channel to write on
   * @param buffer  buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
   */
  protected int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {

    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT)
      ? channel.read(buffer)
      : channelIO(channel, null, buffer);
    if (count > 0) {
      metrics.receivedBytes(count);
    }
    return count;
  }

  /**
   * Helper for {@link #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)}.
   * Only one of readCh or writeCh should be non-null.
   * @param readCh  read channel
   * @param writeCh write channel
   * @param buf     buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh, WritableByteChannel writeCh,
    ByteBuffer buf) throws IOException {

    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;

    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);

        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);
      }
    }

    int nBytes = initialRemaining - buf.remaining();
    return (nBytes > 0) ? nBytes : ret;
  }

  /**
   * Needed for features such as delayed calls. We need to be able to store the current call so that
   * we can complete it later or ask questions of what is supported by the current ongoing call.
   * @return An RpcCallContext backed by the currently ongoing call (gotten from a thread local)
   */
  public static Optional<RpcCall> getCurrentCall() {
    return Optional.ofNullable(CurCall.get());
  }

  public static boolean isInRpcCallContext() {
    return CurCall.get() != null;
  }

  /**
   * Used by {@link org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore}. For
   * master's rpc call, it may generate new procedure and mutate the region which store procedure.
   * There are some check about rpc when mutate region, such as rpc timeout check. So unset the rpc
   * call to avoid the rpc check.
   * @return the currently ongoing rpc call
   */
  public static Optional<RpcCall> unsetCurrentCall() {
    Optional<RpcCall> rpcCall = getCurrentCall();
    CurCall.set(null);
    return ((KnobRuntime.check(java.util.UUID.fromString("aac0f297-c727-3947-b728-97e9ed9a6005"))) ? (getCurrentCall()) : (rpcCall));
  }

  /**
   * Used by {@link org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore}. Set the
   * rpc call back after mutate region.
   */
  public static void setCurrentCall(RpcCall rpcCall) {
    CurCall.set(rpcCall);
  }

  /**
   * Returns the user credentials associated with the current RPC request or not present if no
   * credentials were provided.
   * @return A User
   */
  public static Optional<User> getRequestUser() {
    Optional<RpcCall> ctx = getCurrentCall();
    return ctx.isPresent() ? ctx.get().getRequestUser() : Optional.empty();
  }

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  abstract public int getNumOpenConnections();

  /**
   * Returns the username for any user associated with the current RPC request or not present if no
   * user is set.
   */
  public static Optional<String> getRequestUserName() {
    return getRequestUser().map(User::getShortName);
  }

  /**
   * Returns the address of the remote client associated with the current RPC request or not present
   * if no address is set.
   */
  public static Optional<InetAddress> getRemoteAddress() {
    return getCurrentCall().map(RpcCall::getRemoteAddress);
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services    Available service instances
   * @return Matching BlockingServiceAndInterface pair
   */
  protected static BlockingServiceAndInterface getServiceAndInterface(
    final List<BlockingServiceAndInterface> services, final String serviceName) {
    for (BlockingServiceAndInterface bs : services) {
      if (bs.getBlockingService().getDescriptorForType().getName().equals(serviceName)) {
        return bs;
      }
    }
    return null;
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services    Available services and their service interfaces.
   * @return Service interface class for <code>serviceName</code>
   */
  protected static Class<?> getServiceInterface(final List<BlockingServiceAndInterface> services,
    final String serviceName) {
    BlockingServiceAndInterface bsasi = getServiceAndInterface(services, serviceName);
    return bsasi == null ? null : bsasi.getServiceInterface();
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services    Available services and their service interfaces.
   * @return BlockingService that goes with the passed <code>serviceName</code>
   */
  protected static BlockingService getService(final List<BlockingServiceAndInterface> services,
    final String serviceName) {
    BlockingServiceAndInterface bsasi = getServiceAndInterface(services, serviceName);
    return bsasi == null ? null : bsasi.getBlockingService();
  }

  protected static MonitoredRPCHandler getStatus() {
    // It is ugly the way we park status up in RpcServer. Let it be for now. TODO.
    MonitoredRPCHandler status = RpcServer.MONITORED_RPC.get();
    if (status != null) {
      return status;
    }
    status = TaskMonitor.get().createRPCStatus(Thread.currentThread().getName());
    if (KnobRuntime.check(java.util.UUID.fromString("7eb6d045-3112-3c69-bd42-07abc52900d1"))) { status.resume("Waiting for a call"); } else { status.pause("Waiting for a call"); }
    RpcServer.MONITORED_RPC.set(status);
    return status;
  }

  /**
   * Returns the remote side ip address when invoked inside an RPC Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    RpcCall call = CurCall.get();
    if (call != null) {
      return call.getRemoteAddress();
    }
    return null;
  }

  @Override
  public RpcScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public ByteBuffAllocator getByteBuffAllocator() {
    return this.bbAllocator;
  }

  @Override
  public void setRsRpcServices(RSRpcServices rsRpcServices) {
    this.rsRpcServices = rsRpcServices;
  }

  @Override
  public void setNamedQueueRecorder(NamedQueueRecorder namedQueueRecorder) {
if(KnobRuntime.check(java.util.UUID.fromString("bf89e1e9-fea4-3711-9305-b40d19729859"))) {
return;
}
    this.namedQueueRecorder = namedQueueRecorder;
  }

  protected boolean needAuthorization() {
if(KnobRuntime.check(java.util.UUID.fromString("fbbe428d-275c-312c-ba80-a255d1ef19dd"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c609151-b5cc-3e29-b258-7160059f2cc0"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("31baa82f-8f51-309f-8718-83823fc49e42"))) ? (!authorize) : (authorize));
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public List<BlockingServiceAndInterface> getServices() {
    return services;
  }
}
