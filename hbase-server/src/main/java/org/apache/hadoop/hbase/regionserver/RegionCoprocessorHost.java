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

import com.google.protobuf.Message;
import com.google.protobuf.Service;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.RawCellBuilderFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SharedConnection;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.CoprocessorServiceBackwardCompatiblity;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.RpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.map.AbstractReferenceMap;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.map.ReferenceMap;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Implements the coprocessor environment and runtime support for coprocessors loaded within a
 * {@link Region}.
 */
@InterfaceAudience.Private
public class RegionCoprocessorHost
  extends CoprocessorHost<RegionCoprocessor, RegionCoprocessorEnvironment> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionCoprocessorHost.class);
  // The shared data map
  private static final ReferenceMap<String, ConcurrentMap<String, Object>> SHARED_DATA_MAP =
    new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
      AbstractReferenceMap.ReferenceStrength.WEAK);

  // optimization: no need to call postScannerFilterRow, if no coprocessor implements it
  private final boolean hasCustomPostScannerFilterRow;

  /*
   * Whether any configured CPs override postScannerFilterRow hook
   */
  public boolean hasCustomPostScannerFilterRow() {
    return hasCustomPostScannerFilterRow;
  }

  /**
   * Encapsulation of the environment of each coprocessor
   */
  private static class RegionEnvironment extends BaseEnvironment<RegionCoprocessor>
    implements RegionCoprocessorEnvironment {
    private Region region;
    ConcurrentMap<String, Object> sharedData;
    private final MetricRegistry metricRegistry;
    private final RegionServerServices services;
    private final RpcQuotaManager rpcQuotaManager;

    /**
     * Constructor
     * @param impl     the coprocessor instance
     * @param priority chaining priority
     */
    public RegionEnvironment(final RegionCoprocessor impl, final int priority, final int seq,
      final Configuration conf, final Region region, final RegionServerServices services,
      final ConcurrentMap<String, Object> sharedData) {
      super(impl, priority, seq, conf);
      this.region = region;
      this.sharedData = sharedData;
      this.services = services;
      this.metricRegistry =
        MetricsCoprocessor.createRegistryForRegionCoprocessor(impl.getClass().getName());
      // Some unit tests reach this line with services == null, and are okay with rpcQuotaManager
      // being null. Let these unit tests succeed. This should not happen in real usage.
      if (((KnobRuntime.check(java.util.UUID.fromString("b796a920-4ccd-3800-a038-4e05c7418446"))) ? ((services) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2eb0413a-1e50-3b0a-8164-a41118eaa164"))) ? ((services) != (null)) : (services != null))))) {
        this.rpcQuotaManager = services.getRegionServerRpcQuotaManager();
      } else {
        this.rpcQuotaManager = null;
      }
    }

    /** Returns the region */
    @Override
    public Region getRegion() {
      return region;
    }

    @Override
    public OnlineRegions getOnlineRegions() {
      return this.services;
    }

    @Override
    public Connection getConnection() {
      // Mocks may have services as null at test time.
      return services != null ? new SharedConnection(services.getConnection()) : null;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return services != null ? this.services.createConnection(conf) : null;
    }

    @Override
    public ServerName getServerName() {
      return services != null ? services.getServerName() : null;
    }

    @Override
    public void shutdown() {
if(KnobRuntime.check(java.util.UUID.fromString("80040340-a1f4-391f-8578-d5d54383abb0"))) {
return;
}
      super.shutdown();
      MetricsCoprocessor.removeRegistry(this.metricRegistry);
    }

    @Override
    public ConcurrentMap<String, Object> getSharedData() {
      return sharedData;
    }

    @Override
    public RegionInfo getRegionInfo() {
      return region.getRegionInfo();
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
      return metricRegistry;
    }

    @Override
    public RawCellBuilder getCellBuilder() {
      // We always do a DEEP_COPY only
      return RawCellBuilderFactory.create();
    }

    @Override
    public RpcQuotaManager getRpcQuotaManager() {
      return rpcQuotaManager;
    }

    @Override
    public OperationQuota checkScanQuota(Scan scan, long maxBlockBytesScanned,
      long prevBlockBytesScannedDifference) throws IOException, RpcThrottlingException {
      ClientProtos.ScanRequest scanRequest = RequestConverter
        .buildScanRequest(region.getRegionInfo().getRegionName(), scan, scan.getCaching(), false);
      long maxScannerResultSize =
        services.getConfiguration().getLong(HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
          HConstants.DEFAULT_HBASE_SERVER_SCANNER_MAX_RESULT_SIZE);
      return rpcQuotaManager.checkScanQuota(region, scanRequest, maxScannerResultSize,
        maxBlockBytesScanned, prevBlockBytesScannedDifference);
    }

    @Override
    public OperationQuota checkBatchQuota(Region region, OperationQuota.OperationType type)
      throws IOException, RpcThrottlingException {
      return rpcQuotaManager.checkBatchQuota(region, type);
    }

    @Override
    public OperationQuota checkBatchQuota(final Region region, int numWrites, int numReads)
      throws IOException, RpcThrottlingException {
      return rpcQuotaManager.checkBatchQuota(region, numWrites, numReads);
    }
  }

  /**
   * Special version of RegionEnvironment that exposes RegionServerServices for Core Coprocessors
   * only. Temporary hack until Core Coprocessors are integrated into Core.
   */
  private static class RegionEnvironmentForCoreCoprocessors extends RegionEnvironment
    implements HasRegionServerServices {
    private final RegionServerServices rsServices;

    public RegionEnvironmentForCoreCoprocessors(final RegionCoprocessor impl, final int priority,
      final int seq, final Configuration conf, final Region region,
      final RegionServerServices services, final ConcurrentMap<String, Object> sharedData) {
      super(impl, priority, seq, conf, region, services, sharedData);
      this.rsServices = services;
    }

    /**
     * @return An instance of RegionServerServices, an object NOT for general user-space Coprocessor
     *         consumption.
     */
    @Override
    public RegionServerServices getRegionServerServices() {
      return this.rsServices;
    }
  }

  static class TableCoprocessorAttribute {
    private Path path;
    private String className;
    private int priority;
    private Configuration conf;

    public TableCoprocessorAttribute(Path path, String className, int priority,
      Configuration conf) {
      this.path = path;
      this.className = className;
      this.priority = priority;
      this.conf = conf;
    }

    public Path getPath() {
      return path;
    }

    public String getClassName() {
      return className;
    }

    public int getPriority() {
      return priority;
    }

    public Configuration getConf() {
      return conf;
    }
  }

  /** The region server services */
  RegionServerServices rsServices;
  /** The region */
  HRegion region;

  /**
   * Constructor
   * @param region     the region
   * @param rsServices interface to available region server functionality
   * @param conf       the configuration
   */
  @SuppressWarnings("ReturnValueIgnored") // Checking method exists as CPU optimization
  public RegionCoprocessorHost(final HRegion region, final RegionServerServices rsServices,
    final Configuration conf) {
    super(rsServices);
    this.conf = conf;
    this.rsServices = rsServices;
    this.region = region;
    this.pathPrefix = Integer.toString(this.region.getRegionInfo().hashCode());

    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, REGION_COPROCESSOR_CONF_KEY);

    // load system default cp's for user tables from configuration.
    if (!region.getRegionInfo().getTable().isSystemTable()) {
      loadSystemCoprocessors(conf, USER_REGION_COPROCESSOR_CONF_KEY);
    }

    // load Coprocessor From HDFS
    loadTableCoprocessors(conf);

    // now check whether any coprocessor implements postScannerFilterRow
    boolean hasCustomPostScannerFilterRow = false;
    out: for (RegionCoprocessorEnvironment env : coprocEnvironments) {
      if (env.getInstance() instanceof RegionObserver) {
        Class<?> clazz = env.getInstance().getClass();
        for (;;) {
          if (clazz == Object.class) {
            // we dont need to look postScannerFilterRow into Object class
            break; // break the inner loop
          }
          try {
            clazz.getDeclaredMethod("postScannerFilterRow", ObserverContext.class,
              InternalScanner.class, Cell.class, boolean.class);
            // this coprocessor has a custom version of postScannerFilterRow
            hasCustomPostScannerFilterRow = true;
            break out;
          } catch (NoSuchMethodException ignore) {
          }
          // the deprecated signature still exists
          try {
            clazz.getDeclaredMethod("postScannerFilterRow", ObserverContext.class,
              InternalScanner.class, byte[].class, int.class, short.class, boolean.class);
            // this coprocessor has a custom version of postScannerFilterRow
            hasCustomPostScannerFilterRow = true;
            break out;
          } catch (NoSuchMethodException ignore) {
          }
          clazz = clazz.getSuperclass();
        }
      }
    }
    this.hasCustomPostScannerFilterRow = hasCustomPostScannerFilterRow;
  }

  static List<TableCoprocessorAttribute> getTableCoprocessorAttrsFromSchema(Configuration conf,
    TableDescriptor htd) {
if(KnobRuntime.check(java.util.UUID.fromString("7dc093b8-14eb-39fb-8887-f2c104ff5d11"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2b7e585d-2e7f-34fc-80af-2656bbf2fcf4"))) {
return null;
}
    return htd.getCoprocessorDescriptors().stream().map(cp -> {
      Path path = cp.getJarPath().map(p -> new Path(p)).orElse(null);
      Configuration ourConf;
      if (!cp.getProperties().isEmpty()) {
        // do an explicit deep copy of the passed configuration
        ourConf = new Configuration(false);
        HBaseConfiguration.merge(ourConf, conf);
        cp.getProperties().forEach((k, v) -> ourConf.set(k, v));
      } else {
        ourConf = conf;
      }
      return new TableCoprocessorAttribute(path, cp.getClassName(), cp.getPriority(), ourConf);
    }).collect(Collectors.toList());
  }

  /**
   * Sanity check the table coprocessor attributes of the supplied schema. Will throw an exception
   * if there is a problem.
   */
  public static void testTableCoprocessorAttrs(final Configuration conf, final TableDescriptor htd)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("53ee3652-8f44-3815-8e85-0854749c737d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("68b011ad-ef00-389e-8375-a36156d94c76"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a14232bf-2d09-3dd5-b9d4-630e17774ab6"))) {
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
    String pathPrefix = UUID.randomUUID().toString();
if(KnobRuntime.check(java.util.UUID.fromString("31f9a7aa-0921-34d7-91df-1308026608d9"))) {
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
    for (TableCoprocessorAttribute attr : getTableCoprocessorAttrsFromSchema(conf, htd)) {
      if (attr.getPriority() < 0) {
        throw new IOException(
          "Priority for coprocessor " + attr.getClassName() + " cannot be less than 0");
      }
      ClassLoader old = Thread.currentThread().getContextClassLoader();
      try {
        ClassLoader cl;
        if (attr.getPath() != null) {
          cl = CoprocessorClassLoader.getClassLoader(attr.getPath(),
            CoprocessorHost.class.getClassLoader(), pathPrefix, conf);
        } else {
          cl = CoprocessorHost.class.getClassLoader();
        }
        if (KnobRuntime.check(java.util.UUID.fromString("a08d0ee9-6598-3aa8-a8e2-31b199d93a71"))) { Thread.currentThread().setContextClassLoader(CoprocessorHost.class.getClassLoader()); } else { Thread.currentThread().setContextClassLoader(cl); }
        if (cl instanceof CoprocessorClassLoader) {
          String[] includedClassPrefixes = null;
          if (conf.get(HConstants.CP_HTD_ATTR_INCLUSION_KEY) != null) {
            String prefixes = attr.conf.get(HConstants.CP_HTD_ATTR_INCLUSION_KEY);
            includedClassPrefixes = prefixes.split(";");
          }
          ((CoprocessorClassLoader) cl).loadClass(attr.getClassName(), includedClassPrefixes);
        } else {
          cl.loadClass(attr.getClassName());
        }
      } catch (ClassNotFoundException e) {
        throw new IOException("Class " + attr.getClassName() + " cannot be loaded", e);
      } finally {
        Thread.currentThread().setContextClassLoader(old);
      }
    }
  }

  void loadTableCoprocessors(final Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("36050169-173a-36dc-b659-c4ddb2ea7618"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e95013a7-5cf4-33a3-bab0-2a71914b78b7"))) {
return;
}
    boolean coprocessorsEnabled =
      conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY, DEFAULT_COPROCESSORS_ENABLED);
    boolean tableCoprocessorsEnabled =
      conf.getBoolean(USER_COPROCESSORS_ENABLED_CONF_KEY, DEFAULT_USER_COPROCESSORS_ENABLED);
    if (!(coprocessorsEnabled && tableCoprocessorsEnabled)) {
      return;
    }

    // scan the table attributes for coprocessor load specifications
    // initialize the coprocessors
    List<RegionCoprocessorEnvironment> configured = new ArrayList<>();
if(KnobRuntime.check(java.util.UUID.fromString("6f679f4c-7d1d-3ca3-9e8a-89365093cf3b"))) {
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
    for (TableCoprocessorAttribute attr : getTableCoprocessorAttrsFromSchema(conf,
      region.getTableDescriptor())) {
      // Load encompasses classloading and coprocessor initialization
      try {
        RegionCoprocessorEnvironment env =
          load(attr.getPath(), attr.getClassName(), attr.getPriority(), attr.getConf());
        if (env == null) {
          continue;
        }
        configured.add(env);
        LOG.info("Loaded coprocessor " + attr.getClassName() + " from HTD of "
          + region.getTableDescriptor().getTableName().getNameAsString() + " successfully.");
      } catch (Throwable t) {
        // Coprocessor failed to load, do we abort on error?
        if (conf.getBoolean(ABORT_ON_ERROR_KEY, DEFAULT_ABORT_ON_ERROR)) {
          abortServer(attr.getClassName(), t);
        } else {
          LOG.error("Failed to load coprocessor " + attr.getClassName(), t);
        }
      }
    }
    // add together to coprocessor set for COW efficiency
    coprocEnvironments.addAll(configured);
  }

  @Override
  public RegionEnvironment createEnvironment(RegionCoprocessor instance, int priority, int seq,
    Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("9798218d-c9a5-3d91-9171-fe14ad1ceedb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a535759e-3586-32f6-9669-2af24261fab4"))) {
return null;
}
    // If coprocessor exposes any services, register them.
    for (Service service : instance.getServices()) {
      region.registerService(service);
    }
    ConcurrentMap<String, Object> classData;
    // make sure only one thread can add maps
    synchronized (SHARED_DATA_MAP) {
      // as long as at least one RegionEnvironment holds on to its classData it will
      // remain in this map
      classData = SHARED_DATA_MAP.computeIfAbsent(instance.getClass().getName(),
        k -> new ConcurrentHashMap<>());
    }
    // If a CoreCoprocessor, return a 'richer' environment, one laden with RegionServerServices.
    return instance.getClass().isAnnotationPresent(CoreCoprocessor.class)
      ? new RegionEnvironmentForCoreCoprocessors(instance, priority, seq, conf, region, rsServices,
        classData)
      : new RegionEnvironment(instance, priority, seq, conf, region, rsServices, classData);
  }

  @Override
  public RegionCoprocessor checkAndGetInstance(Class<?> implClass)
    throws InstantiationException, IllegalAccessException {
if(KnobRuntime.check(java.util.UUID.fromString("e0d52a34-1df1-39fd-a54d-923d56470444"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("06b1afd7-7a43-3b5a-8512-4875a6f934a6"))) {
throw new InstantiationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5007a238-234c-3479-ba6d-f1e85939209e"))) {
throw new IllegalAccessException("Injected exception");
}
    try {
      if (RegionCoprocessor.class.isAssignableFrom(implClass)) {
        return implClass.asSubclass(RegionCoprocessor.class).getDeclaredConstructor().newInstance();
      } else if (CoprocessorService.class.isAssignableFrom(implClass)) {
        // For backward compatibility with old CoprocessorService impl which don't extend
        // RegionCoprocessor.
        CoprocessorService cs;
        cs = implClass.asSubclass(CoprocessorService.class).getDeclaredConstructor().newInstance();
        return new CoprocessorServiceBackwardCompatiblity.RegionCoprocessorService(cs);
      } else {
        LOG.error("{} is not of type RegionCoprocessor. Check the configuration of {}",
          implClass.getName(), CoprocessorHost.REGION_COPROCESSOR_CONF_KEY);
        return null;
      }
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
    }
  }

  private ObserverGetter<RegionCoprocessor, RegionObserver> regionObserverGetter =
    RegionCoprocessor::getRegionObserver;

  private ObserverGetter<RegionCoprocessor, EndpointObserver> endpointObserverGetter =
    RegionCoprocessor::getEndpointObserver;

  abstract class RegionObserverOperationWithoutResult
    extends ObserverOperationWithoutResult<RegionObserver> {
    public RegionObserverOperationWithoutResult() {
      super(regionObserverGetter);
    }

    public RegionObserverOperationWithoutResult(User user) {
      super(regionObserverGetter, user);
    }

    public RegionObserverOperationWithoutResult(boolean bypassable) {
      super(regionObserverGetter, null, bypassable);
    }

    public RegionObserverOperationWithoutResult(User user, boolean bypassable) {
      super(regionObserverGetter, user, bypassable);
    }
  }

  abstract class BulkLoadObserverOperation
    extends ObserverOperationWithoutResult<BulkLoadObserver> {
    public BulkLoadObserverOperation(User user) {
      super(RegionCoprocessor::getBulkLoadObserver, user);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Observer operations
  //////////////////////////////////////////////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Observer operations
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Invoked before a region open.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void preOpen() throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("e7c668c1-9564-3284-8913-22966f884019"))) {
throw new java.io.IOException("Injected exception");
}
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preOpen(this);
      }
    });
  }

  /**
   * Invoked after a region open
   */
  public void postOpen() {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    try {
      execOperation(new RegionObserverOperationWithoutResult() {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.postOpen(this);
        }
      });
    } catch (IOException e) {
      LOG.warn(e.toString(), e);
    }
  }

  /**
   * Invoked before a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void preClose(final boolean abortRequested) throws IOException {
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preClose(this, abortRequested);
      }
    });
  }

  /**
   * Invoked after a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void postClose(final boolean abortRequested) {
    try {
      execOperation(new RegionObserverOperationWithoutResult() {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.postClose(this, abortRequested);
        }

        @Override
        public void postEnvCall() {
          shutdown(this.getEnvironment());
        }
      });
    } catch (IOException e) {
      LOG.warn(e.toString(), e);
    }
  }

  /**
   * Called prior to selecting the {@link HStoreFile}s for compaction from the list of currently
   * available candidates.
   * <p>
   * Supports Coprocessor 'bypass' -- 'bypass' is how this method indicates that it changed the
   * passed in <code>candidates</code>.
   * @param store      The store where compaction is being requested
   * @param candidates The currently available store files
   * @param tracker    used to track the life cycle of a compaction
   * @param user       the user
   */
  public boolean preCompactSelection(final HStore store, final List<HStoreFile> candidates,
    final CompactionLifeCycleTracker tracker, final User user) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(user, bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preCompactSelection(this, store, candidates, tracker);
      }
    });
  }

  /**
   * Called after the {@link HStoreFile}s to be compacted have been selected from the available
   * candidates.
   * @param store    The store where compaction is being requested
   * @param selected The store files selected to compact
   * @param tracker  used to track the life cycle of a compaction
   * @param request  the compaction request
   * @param user     the user
   */
  public void postCompactSelection(final HStore store, final List<HStoreFile> selected,
    final CompactionLifeCycleTracker tracker, final CompactionRequest request, final User user)
    throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult(user) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postCompactSelection(this, store, selected, tracker, request);
      }
    });
  }

  /**
   * Called prior to opening store scanner for compaction.
   */
  public ScanInfo preCompactScannerOpen(HStore store, ScanType scanType,
    CompactionLifeCycleTracker tracker, CompactionRequest request, User user) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return store.getScanInfo();
    }
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo());
    execOperation(new RegionObserverOperationWithoutResult(user) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preCompactScannerOpen(this, store, scanType, builder, tracker, request);
      }
    });
    return builder.build();
  }

  /**
   * Called prior to rewriting the store files selected for compaction
   * @param store    the store being compacted
   * @param scanner  the scanner used to read store data during compaction
   * @param scanType type of Scan
   * @param tracker  used to track the life cycle of a compaction
   * @param request  the compaction request
   * @param user     the user
   * @return Scanner to use (cannot be null!)
   */
  public InternalScanner preCompact(final HStore store, final InternalScanner scanner,
    final ScanType scanType, final CompactionLifeCycleTracker tracker,
    final CompactionRequest request, final User user) throws IOException {
    InternalScanner defaultResult = scanner;
    if (coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, InternalScanner>(
      regionObserverGetter, defaultResult, user) {
      @Override
      public InternalScanner call(RegionObserver observer) throws IOException {
        InternalScanner scanner =
          observer.preCompact(this, store, getResult(), scanType, tracker, request);
        if (scanner == null) {
          throw new CoprocessorException("Null Scanner return disallowed!");
        }
        return scanner;
      }
    });
  }

  /**
   * Called after the store compaction has completed.
   * @param store      the store being compacted
   * @param resultFile the new store file written during compaction
   * @param tracker    used to track the life cycle of a compaction
   * @param request    the compaction request
   * @param user       the user
   */
  public void postCompact(final HStore store, final HStoreFile resultFile,
    final CompactionLifeCycleTracker tracker, final CompactionRequest request, final User user)
    throws IOException {
    execOperation(
      coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult(user) {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.postCompact(this, store, resultFile, tracker, request);
        }
      });
  }

  /**
   * Invoked before create StoreScanner for flush.
   */
  public ScanInfo preFlushScannerOpen(HStore store, FlushLifeCycleTracker tracker)
    throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return store.getScanInfo();
    }
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo());
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preFlushScannerOpen(this, store, builder, tracker);
      }
    });
    return builder.build();
  }

  /**
   * Invoked before a memstore flush
   * @return Scanner to use (cannot be null!)
   */
  public InternalScanner preFlush(HStore store, InternalScanner scanner,
    FlushLifeCycleTracker tracker) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return scanner;
    }
if(KnobRuntime.check(java.util.UUID.fromString("b198723a-89b8-375e-9e04-67ae2f3ac242"))) {
throw new java.io.IOException("Injected exception");
}
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, InternalScanner>(
      regionObserverGetter, scanner) {
      @Override
      public InternalScanner call(RegionObserver observer) throws IOException {
        InternalScanner scanner = observer.preFlush(this, store, getResult(), tracker);
        if (scanner == null) {
          throw new CoprocessorException("Null Scanner return disallowed!");
        }
        return scanner;
      }
    });
  }

  /**
   * Invoked before a memstore flush
   */
  public void preFlush(FlushLifeCycleTracker tracker) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preFlush(this, tracker);
      }
    });
  }

  /**
   * Invoked after a memstore flush
   */
  public void postFlush(FlushLifeCycleTracker tracker) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postFlush(this, tracker);
      }
    });
  }

  /**
   * Invoked before in memory compaction.
   */
  public void preMemStoreCompaction(HStore store) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preMemStoreCompaction(this, store);
      }
    });
  }

  /**
   * Invoked before create StoreScanner for in memory compaction.
   */
  public ScanInfo preMemStoreCompactionCompactScannerOpen(HStore store) throws IOException {
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo());
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preMemStoreCompactionCompactScannerOpen(this, store, builder);
      }
    });
    return builder.build();
  }

  /**
   * Invoked before compacting memstore.
   */
  public InternalScanner preMemStoreCompactionCompact(HStore store, InternalScanner scanner)
    throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return scanner;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, InternalScanner>(
      regionObserverGetter, scanner) {
      @Override
      public InternalScanner call(RegionObserver observer) throws IOException {
        return observer.preMemStoreCompactionCompact(this, store, getResult());
      }
    });
  }

  /**
   * Invoked after in memory compaction.
   */
  public void postMemStoreCompaction(HStore store) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postMemStoreCompaction(this, store);
      }
    });
  }

  /**
   * Invoked after a memstore flush
   */
  public void postFlush(HStore store, HStoreFile storeFile, FlushLifeCycleTracker tracker)
    throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("a2cbe120-f210-321f-98b3-c53b976bb6b4"))) {
throw new java.io.IOException("Injected exception");
}
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postFlush(this, store, storeFile, tracker);
      }
    });
  }

  // RegionObserver support
  /**
   * Supports Coprocessor 'bypass'.
   * @param get     the Get request
   * @param results What to return if return is true/'bypass'.
   * @return true if default processing should be bypassed.
   * @exception IOException Exception
   */
  public boolean preGet(final Get get, final List<Cell> results) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preGetOp(this, get, results);
      }
    });
  }

  /**
   * @param get     the Get request
   * @param results the result set
   * @exception IOException Exception
   */
  public void postGet(final Get get, final List<Cell> results) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("d7a94fa8-d563-31dd-8f8d-ecbfb1bbbabd"))) {
throw new java.io.IOException("Injected exception");
}
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postGetOp(this, get, results);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param get the Get request
   * @return true or false to return to client if bypassing normal operation, or null otherwise
   * @exception IOException Exception
   */
  public Boolean preExists(final Get get) throws IOException {
    boolean bypassable = true;
    boolean defaultResult = false;
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Boolean>(
      regionObserverGetter, defaultResult, bypassable) {
      @Override
      public Boolean call(RegionObserver observer) throws IOException {
        return observer.preExists(this, get, getResult());
      }
    });
  }

  /**
   * @param get    the Get request
   * @param result the result returned by the region server
   * @return the result to return to the client
   * @exception IOException Exception
   */
  public boolean postExists(final Get get, boolean result) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, Boolean>(regionObserverGetter, result) {
        @Override
        public Boolean call(RegionObserver observer) throws IOException {
          return observer.postExists(this, get, getResult());
        }
      });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param put  The Put object
   * @param edit The WALEdit object.
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean prePut(final Put put, final WALEdit edit) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.prePut(this, put, edit);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param mutation - the current mutation
   * @param kv       - the current cell
   * @param byteNow  - current timestamp in bytes
   * @param get      - the get that could be used Note that the get only does not specify the family
   *                 and qualifier that should be used
   * @return true if default processing should be bypassed
   * @deprecated In hbase-2.0.0. Will be removed in hbase-3.0.0. Added explicitly for a single
   *             Coprocessor for its needs only. Will be removed.
   */
  @Deprecated
  public boolean prePrepareTimeStampForDeleteVersion(final Mutation mutation, final Cell kv,
    final byte[] byteNow, final Get get) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.prePrepareTimeStampForDeleteVersion(this, mutation, kv, byteNow, get);
      }
    });
  }

  /**
   * @param put  The Put object
   * @param edit The WALEdit object.
   * @exception IOException Exception
   */
  public void postPut(final Put put, final WALEdit edit) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("11802172-7d72-3894-a8d8-8d010709960b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("de8b1526-7789-326d-858c-c04919bd1138"))) {
return;
}
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postPut(this, put, edit);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param delete The Delete object
   * @param edit   The WALEdit object.
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preDelete(final Delete delete, final WALEdit edit) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preDelete(this, delete, edit);
      }
    });
  }

  /**
   * @param delete The Delete object
   * @param edit   The WALEdit object.
   * @exception IOException Exception
   */
  public void postDelete(final Delete delete, final WALEdit edit) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postDelete(this, delete, edit);
      }
    });
  }

  public void preBatchMutate(final MiniBatchOperationInProgress<Mutation> miniBatchOp)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("543b778d-c8c5-3270-8b3b-aba4f50ecf7a"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f751106a-d332-3832-8e56-378688785935"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a28ffa91-d569-3811-a7ed-d1bf109818b5"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0761b18-1eb5-3c31-9695-78d8cf70fbb6"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("223e5809-30a1-351d-8870-8f156514790f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb93936f-7585-3cd1-a936-87362ea134e4"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62055de2-20e8-3e0b-889a-bacf046a8d00"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b21acab-b995-35ca-9533-972bcc19129e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("14963138-e771-3d9e-a7c4-326d271218c1"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ee2fc3e2-266b-3cc7-b43c-a94e5c09626e"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d93a3807-67f2-33bd-9f61-06467765f89a"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d834b84-769d-36ac-bc8a-3d3445170243"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9863397f-e394-3c83-b6d0-43c5e1152bac"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9b1b289-e1e9-3c18-9a7c-88d0860bb92f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6fa6f430-44a9-3369-9e73-e44bd0c7b646"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b026fc4-2a21-3ea0-96b0-21c3895aecc9"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebacfbfc-6f5a-39dd-b390-fe050eebf78f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b2efb27-7c3a-3278-b3be-e52c247f339c"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62fa0e22-f24e-3b99-b938-c0a62a3fe1c5"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3542048b-5c58-385c-8239-fe2a2c39796d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d35db31-db49-34c0-a80c-caa8f8878faf"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b8485b6-5336-359c-b3f9-fd8258bd5a16"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1aad9961-df8f-35e1-9f64-b6c6108057ee"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae60264d-7426-3430-a291-e92dd215d0eb"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df2ab328-816e-3660-94dc-86cdb95c7f90"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1103a3d3-4284-377b-9a7a-266dd0d848a9"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87981d2e-46ca-3279-86fc-1117162b90e2"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("99c6fadc-e09b-3dd8-ad70-fcb342c034be"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8bcba736-9fce-38db-8d42-879c03369add"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ccb8d2c7-cc72-33af-9cf1-0e359f3b487b"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f25d9d41-0e77-3718-b9f0-bd4746e2f791"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4139cdc-9ea1-303d-908c-b451c961f3e5"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46617050-caaa-37f8-9757-7b40257fa5f3"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76d97926-a7cf-3bca-8ba1-3402882d3f39"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preBatchMutate(this, miniBatchOp);
      }
    });
  }

  public void postBatchMutate(final MiniBatchOperationInProgress<Mutation> miniBatchOp)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("baabff09-4ade-3fc6-b9e1-598e6379bc6c"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b4c053ea-6529-3c89-818b-9437f8943b30"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11777279-5d57-391d-9a36-5aca5756b15c"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de4c931c-2d08-3413-9143-0af8cbe50482"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("da4f15db-700e-380c-a647-3a1ddb8bf47d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb2b5853-2719-3959-8f74-2c960362a0d7"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8db4d5a7-5412-359a-a83c-7596d7a36072"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf99a1be-937a-3d2f-8638-3d59bc409f4a"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b42e982e-edd8-3dcc-b158-d9f23d9a71ee"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20a6c91d-eec1-3811-8682-2b97a66d9370"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77e5c5ea-0e12-3cf3-95a1-2628aa31a277"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("620d6e0e-1b7d-33be-b2e1-b9be6c1a8037"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7091e85a-4550-3372-a951-1fe69677407f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bedc451-8880-3d0a-b7f3-ee38b5f94950"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78a16c60-11c6-3165-b93a-e52fe7325696"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aedf427d-c5b9-3e0f-a7ab-4a818ceff507"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("11ab8e75-4d9f-348f-98e9-dd5e23a971d2"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd1eff74-856c-3160-8bd0-8470b1010b97"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ee844d4-e798-36c7-8075-a3444eed3f52"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ea6a87e-4745-3e60-93ca-eda80da46264"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d0d017d-8bb1-39b4-8306-b31a3b9faae5"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bcf95391-161d-37d2-b01f-03cb73308d62"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9959a995-7880-3c22-ae87-4b6bd1d64f70"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d30352ec-37b9-3fe3-ac99-869697bc6887"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d82184c5-17ff-3a39-8800-a38c42b951f7"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c4c8692-3ba6-3cb0-b753-c0e31a3b4830"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9d96274-5ddb-3f7e-b621-2055dc4cb8ab"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("041eadeb-dd03-3874-b950-9904185a4d62"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cbe44aed-2801-3370-af5e-33c50243566d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a88b6660-90de-30a8-92e8-2b0cbe39a8bd"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0bb1629b-c176-31c1-bd65-2a022bc5f2e2"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab9fc911-3f81-372c-855e-35554b4686b9"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37612a9b-ccc2-34f8-ab7f-63238de182e0"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8cb9f8d-fbe2-3bf3-a68b-1dbda4496585"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postBatchMutate(this, miniBatchOp);
      }
    });
  }

  public void postBatchMutateIndispensably(final MiniBatchOperationInProgress<Mutation> miniBatchOp,
    final boolean success) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1e9edcca-d0f8-3a51-baef-f794c4c1b965"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4beb0917-4c5f-31e1-b16b-9bf390ab192d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84bae510-0c7d-3096-97f6-fc47f07600fc"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58fdc501-19f6-3634-8c11-a0265731845d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5ecc5a90-e33a-3df3-b288-595a41080b4d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("980e5cba-1e14-350b-88ed-ef0a3eca9cca"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7dc4be5-6309-340c-8a97-aa0fab0961bf"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae88aebb-e011-3bf9-a12e-6bcc151495e7"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("315d8304-23c1-3720-93d3-63e772a87ecd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9223b387-7246-3a2a-b913-2348cabff49f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb39065a-74f2-331d-9448-8339714b7c8b"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31988748-84aa-3469-bf4e-b6c09d38598f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5fd4d5a-891b-35b0-99fb-076015d5c17b"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3d29d3e-a76a-39b9-9063-22794c5038e5"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("73525b60-893a-3a0b-b39b-6bc904aba51d"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54c1b42c-813c-325a-8908-0c0a63188b81"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("96e9bbe1-2afe-3569-bf20-4d242bd297ee"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c0ee6e5-1808-3bb3-a709-6760964f766f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef5ef644-9476-39a6-b63e-4b77312fc938"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("becd58a7-3a92-310b-9f3a-43ae2cc28bc2"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7619c794-2146-3948-bebd-6e447c0a2f8e"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2d35592-34ad-3542-abd8-d434d37f034c"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfDeletes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e8050fc-b8a5-38be-8408-f7ccfa2eeae7"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("087f9105-c23d-36ec-bf31-e87e525f21ca"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfIncrements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2bd9e6c-a1af-3d5d-ad2b-9a4e081a55d3"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31ffbe71-3998-3180-a083-9e2b01ee8669"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("678935b3-ac0d-3042-8d39-e743cde338ee"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("readyToWriteCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("961cc499-2b15-38d2-aaa2-8ca716ca6414"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("lastIndexExclusive");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eccf20a8-4933-362b-8426-38ec0b6bf848"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9bb6c67b-9578-3432-b95b-c3cedc87fbba"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("99904e95-bf2e-367d-adf4-2eb30bf00df6"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfAppends");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("546a68e9-f726-3048-8adf-1699d2c6681b"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("numOfPuts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c80b997-c1d5-35a2-975b-f14120a31e94"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("cellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("421cd46b-67d3-3390-9021-a49f82ce913f"))) {
try {
    java.lang.reflect.Field field = miniBatchOp.getClass().getDeclaredField("firstIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(miniBatchOp));
    field.set(miniBatchOp, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postBatchMutateIndispensably(this, miniBatchOp, success);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param checkAndMutate the CheckAndMutate object
   * @return true or false to return to client if default processing should be bypassed, or null
   *         otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult preCheckAndMutate(CheckAndMutate checkAndMutate) throws IOException {
    boolean bypassable = true;
    CheckAndMutateResult defaultResult = new CheckAndMutateResult(false, null);
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, CheckAndMutateResult>(regionObserverGetter,
        defaultResult, bypassable) {
        @Override
        public CheckAndMutateResult call(RegionObserver observer) throws IOException {
          return observer.preCheckAndMutate(this, checkAndMutate, getResult());
        }
      });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param checkAndMutate the CheckAndMutate object
   * @return true or false to return to client if default processing should be bypassed, or null
   *         otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult preCheckAndMutateAfterRowLock(CheckAndMutate checkAndMutate)
    throws IOException {
    boolean bypassable = true;
    CheckAndMutateResult defaultResult = new CheckAndMutateResult(false, null);
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, CheckAndMutateResult>(regionObserverGetter,
        defaultResult, bypassable) {
        @Override
        public CheckAndMutateResult call(RegionObserver observer) throws IOException {
          return observer.preCheckAndMutateAfterRowLock(this, checkAndMutate, getResult());
        }
      });
  }

  /**
   * @param checkAndMutate the CheckAndMutate object
   * @param result         the result returned by the checkAndMutate
   * @return true or false to return to client if default processing should be bypassed, or null
   *         otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult postCheckAndMutate(CheckAndMutate checkAndMutate,
    CheckAndMutateResult result) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, CheckAndMutateResult>(regionObserverGetter,
        result) {
        @Override
        public CheckAndMutateResult call(RegionObserver observer) throws IOException {
          return observer.postCheckAndMutate(this, checkAndMutate, getResult());
        }
      });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param append append object
   * @param edit   The WALEdit object.
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppend(final Append append, final WALEdit edit) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (this.coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Result>(
      regionObserverGetter, defaultResult, bypassable) {
      @Override
      public Result call(RegionObserver observer) throws IOException {
        return observer.preAppend(this, append, edit);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param append append object
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppendAfterRowLock(final Append append) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (this.coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Result>(
      regionObserverGetter, defaultResult, bypassable) {
      @Override
      public Result call(RegionObserver observer) throws IOException {
        return observer.preAppendAfterRowLock(this, append);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param increment increment object
   * @param edit      The WALEdit object.
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrement(final Increment increment, final WALEdit edit) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Result>(
      regionObserverGetter, defaultResult, bypassable) {
      @Override
      public Result call(RegionObserver observer) throws IOException {
        return observer.preIncrement(this, increment, edit);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param increment increment object
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrementAfterRowLock(final Increment increment) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Result>(
      regionObserverGetter, defaultResult, bypassable) {
      @Override
      public Result call(RegionObserver observer) throws IOException {
        return observer.preIncrementAfterRowLock(this, increment);
      }
    });
  }

  /**
   * @param append Append object
   * @param result the result returned by the append
   * @param edit   The WALEdit object.
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postAppend(final Append append, final Result result, final WALEdit edit)
    throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter, result) {
        @Override
        public Result call(RegionObserver observer) throws IOException {
          return observer.postAppend(this, append, result, edit);
        }
      });
  }

  /**
   * @param increment increment object
   * @param result    the result returned by postIncrement
   * @param edit      The WALEdit object.
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postIncrement(final Increment increment, Result result, final WALEdit edit)
    throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter, result) {
        @Override
        public Result call(RegionObserver observer) throws IOException {
          return observer.postIncrement(this, increment, getResult(), edit);
        }
      });
  }

  /**
   * @param scan the Scan specification
   * @exception IOException Exception
   */
  public void preScannerOpen(final Scan scan) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preScannerOpen(this, scan);
      }
    });
  }

  /**
   * @param scan the Scan specification
   * @param s    the scanner
   * @return the scanner instance to use
   * @exception IOException Exception
   */
  public RegionScanner postScannerOpen(final Scan scan, RegionScanner s) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5e891149-84f4-3ae7-be2b-22a7a8c4c172"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("44cd28af-53f8-352c-bb0e-9ee2eb13d309"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("90d925ea-ec9a-3428-a2d0-cfbe3b0c3e0b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eadab548-28a7-3a66-8aa2-eccc19ec4627"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f4a84e2a-9990-386d-aea6-15a1b447eb27"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("74c769a9-5166-3797-942e-10bfa1451a41"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cfd45dc0-dad8-3027-a309-511c2550a366"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("960e8788-68bc-33e8-8753-fd676ce40f63"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b7e5ecf3-17d8-322a-986d-a80793a9189e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("22990b9e-8eda-387d-89c1-e2035f0e277a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("84fae8e0-fa76-3a36-8b01-b78aeb8707ba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c4ccccf2-92fa-32db-904b-f07de4c2f749"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("28322a18-58b2-39a3-9ded-5aa816787b1e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05e11c43-ef63-3fcb-be61-fa9aa047d18d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("56caaf95-62dc-3a34-9952-c252887db52b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("77e1de6f-7462-3b9b-957a-cbc1cc322f65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("634c096e-a422-3669-9727-bfc1785859e5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("daa233b7-8e86-3dbc-97f0-6656dd6df115"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("221f8e73-0ba1-3b47-b75e-f03bba8f8ff8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9ac2c5c3-52f2-352c-a25b-e76baf21c11a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7804ef70-c00b-381b-906f-79e10aeb7add"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b8e062bc-dcff-3457-8743-321dbed67ac0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d7df0bee-ce5f-32d1-9fc5-cabe945f1b3d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b57ae108-b516-3b8c-9236-46921eea5ebc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3ed6bd01-1ca9-3b73-8691-d1827a21591f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f83b1766-7568-30ce-9fae-1e691bb44fde"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("17566614-307e-3922-90af-fbf5b42e9acd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("146316da-a30c-3d21-a71c-1ee9f519e91f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("85f9d264-59dd-393f-8291-f5a50071f4eb"))) {
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
    if (this.coprocEnvironments.isEmpty()) {
      return s;
    }
if(KnobRuntime.check(java.util.UUID.fromString("27734b42-137c-36b4-9590-64fbb65624ec"))) {
throw new java.io.IOException("Injected exception");
}
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, RegionScanner>(regionObserverGetter, s) {
        @Override
        public RegionScanner call(RegionObserver observer) throws IOException {
          return observer.postScannerOpen(this, scan, getResult());
        }
      });
  }

  /**
   * @param s       the scanner
   * @param results the result set returned by the region server
   * @param limit   the maximum number of results to return
   * @return 'has next' indication to client if bypassing default behavior, or null otherwise
   * @exception IOException Exception
   */
  public Boolean preScannerNext(final InternalScanner s, final List<Result> results,
    final int limit) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e2d0dcaf-39c8-3839-a65b-7f3563a70603"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e9d22c9c-c3cd-3ae7-8e18-ede76b69decf"))) {
return null;
}
    boolean bypassable = true;
    boolean defaultResult = false;
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
if(KnobRuntime.check(java.util.UUID.fromString("a9f01966-7f10-31fb-b1c1-60bfe90c166e"))) {
throw new java.io.IOException("Injected exception");
}
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Boolean>(
      regionObserverGetter, defaultResult, bypassable) {
      @Override
      public Boolean call(RegionObserver observer) throws IOException {
        return observer.preScannerNext(this, s, results, limit, getResult());
      }
    });
  }

  /**
   * @param s       the scanner
   * @param results the result set returned by the region server
   * @param limit   the maximum number of results to return
   * @return 'has more' indication to give to client
   * @exception IOException Exception
   */
  public boolean postScannerNext(final InternalScanner s, final List<Result> results,
    final int limit, boolean hasMore) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1435a17c-e086-32ff-8a50-045db4b575a4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0d65c182-b253-3dd0-aee1-ae4c6824f083"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea443638-e411-3f54-8c0a-c07089032f68"))) {
return false;
}
    if (this.coprocEnvironments.isEmpty()) {
      return hasMore;
    }
if(KnobRuntime.check(java.util.UUID.fromString("d50b1b9b-aa46-312e-895d-93a5db360a2c"))) {
throw new java.io.IOException("Injected exception");
}
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, Boolean>(regionObserverGetter, hasMore) {
        @Override
        public Boolean call(RegionObserver observer) throws IOException {
          return observer.postScannerNext(this, s, results, limit, getResult());
        }
      });
  }

  /**
   * This will be called by the scan flow when the current scanned row is being filtered out by the
   * filter.
   * @param s          the scanner
   * @param curRowCell The cell in the current row which got filtered out
   * @return whether more rows are available for the scanner or not
   */
  public boolean postScannerFilterRow(final InternalScanner s, final Cell curRowCell)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9b610880-d270-3ddd-9717-ee4d148665ad"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("4d1608ce-cbe3-3810-bc5b-5c2f532c4706"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("84c23b89-c0e1-3fe6-8085-2265602f5fc4"))) {
return false;
}
    // short circuit for performance
    boolean defaultResult = true;
    if (!hasCustomPostScannerFilterRow) {
      return defaultResult;
    }
    if (this.coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Boolean>(
      regionObserverGetter, defaultResult) {
      @Override
      public Boolean call(RegionObserver observer) throws IOException {
        return observer.postScannerFilterRow(this, s, curRowCell, getResult());
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param s the scanner
   * @return true if default behavior should be bypassed, false otherwise
   * @exception IOException Exception
   */
  // Should this be bypassable?
  public boolean preScannerClose(final InternalScanner s) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6fe05d8c-42aa-3e04-8f14-8bd1dbbd4800"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("65df66fe-4067-328b-b398-354c408070ae"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a4bb71e6-3223-32a2-8c03-f1ae5f6cc1ea"))) {
return false;
}
    return execOperation(
      coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult(true) {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.preScannerClose(this, s);
        }
      });
  }

  /**
   * @exception IOException Exception
   */
  public void postScannerClose(final InternalScanner s) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postScannerClose(this, s);
      }
    });
  }

  /**
   * Called before open store scanner for user scan.
   */
  public ScanInfo preStoreScannerOpen(HStore store, Scan scan) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("92cbe4e5-761b-3055-ae9d-3baf7d49c16c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("df738999-74ae-3534-967f-96447e9a9db9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("317acd7f-8720-3c07-b663-7b7cc0194d21"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b27d1c7-1a1f-3752-9835-6849f055eac9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b3380a8-81e7-300e-bc04-70d0ca8b20b9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8f91608-2d33-3071-a64f-14f2508319a5"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8c771a6-cf5b-3ec1-adb7-16b090bb4743"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac866e65-16c8-37de-8ee2-e9093779ca5d"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59ee11ae-45d9-341f-bde7-fd19dfc6a9ef"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8f72951-7e76-309d-b28a-5aea376bc425"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fab56b4c-e81e-30b6-93ec-47b8f7152306"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("41ea502f-d122-3875-b322-17a0df33de05"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ebcc8f02-ec1f-3960-bef1-086639617382"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ca2b45e5-d382-3627-9520-5ea93947c4aa"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("544d9d39-a9c9-32e3-a978-a784738ad810"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8474386e-ef3c-36fa-b7b0-0ba1f3b0c9f8"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bd2de97-ef2c-37b1-8182-10541ab4f653"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("493bc365-fef3-31b5-a1b6-98212e213544"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6a4b7a06-579b-3b14-84d2-63782e401867"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a6e685c8-bc16-334e-a371-d35a1d6958e8"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa5df947-28ac-34dd-b849-5b21a52a6b8a"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c87d524d-328d-344f-bf21-aa6ff4cdf792"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6a2eced4-21b3-318e-b836-4601cfb666ac"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5a82c05-ebc7-3379-abe5-af0b0ec21575"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f404aa98-4dc7-3dbe-a3ec-49b94963baef"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ecd774ea-39d4-3ff0-a67f-ee8da30096e8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a570bdc4-89c4-3533-abf7-579cd14b93e6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f8e63656-2656-3c05-8222-931d01d8094a"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("831e0972-bac5-3785-9939-ccdb85429903"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57ec5853-fd5c-308f-895e-dbf2550920b5"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3c6443a-0d62-31d3-a0e8-ffa78fb481d1"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0490c5a1-08a5-3f28-a55c-4899292cd70c"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6a960299-56f9-3c37-ac2a-ca214a224b2e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8eb0ec41-460e-38b6-836f-6aa65dd81aee"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2a7d57c2-f65a-3369-b914-d773e3f739ca"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3355c25e-1fd9-334c-991b-56561f29f118"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c2f2fd57-c61c-3b2c-beb6-e4e43cbf7b95"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("475b9989-144b-3905-b790-b1ffd11e973d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a16884be-bae4-343d-bb8c-93817394a7a1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f427d6a3-8d28-3875-88f5-c5be04b4dd96"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("60bcc121-6221-3110-8789-ceb3602af63d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("aa4872dd-f68a-3af5-bf77-c462df90bffb"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da9a0022-842b-39f9-a21a-dbc7e84d69a2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bab07c98-153b-3f0a-ac22-e0b84f8f3d7b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05af00be-30e6-30d8-9e3c-7c385a39395a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36de955b-56a4-384e-ada7-aa730329efb1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0f40b4f5-a5a0-3195-9a7a-7b102a5b6c4c"))) {
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
    if (coprocEnvironments.isEmpty()) return store.getScanInfo();
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo(), scan);
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preStoreScannerOpen(this, store, builder);
      }
    });
    return builder.build();
  }

  /**
   * @param info  the RegionInfo for this region
   * @param edits the file of recovered edits
   */
  public void preReplayWALs(final RegionInfo info, final Path edits) throws IOException {
    execOperation(
      coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult(true) {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.preReplayWALs(this, info, edits);
        }
      });
  }

  /**
   * @param info  the RegionInfo for this region
   * @param edits the file of recovered edits
   * @throws IOException Exception
   */
  public void postReplayWALs(final RegionInfo info, final Path edits) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postReplayWALs(this, info, edits);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @return true if default behavior should be bypassed, false otherwise
   * @deprecated Since hbase-2.0.0. No replacement. To be removed in hbase-3.0.0 and replaced with
   *             something that doesn't expose IntefaceAudience.Private classes.
   */
  @Deprecated
  public boolean preWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
    throws IOException {
    return execOperation(
      coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult(true) {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.preWALRestore(this, info, logKey, logEdit);
        }
      });
  }

  /**
   * @deprecated Since hbase-2.0.0. No replacement. To be removed in hbase-3.0.0 and replaced with
   *             something that doesn't expose IntefaceAudience.Private classes.
   */
  @Deprecated
  public void postWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
    throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postWALRestore(this, info, logKey, logEdit);
      }
    });
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   */
  public void preBulkLoadHFile(final List<Pair<byte[], String>> familyPaths) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preBulkLoadHFile(this, familyPaths);
      }
    });
  }

  public boolean preCommitStoreFile(final byte[] family, final List<Pair<Path, Path>> pairs)
    throws IOException {
    return execOperation(
      coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.preCommitStoreFile(this, family, pairs);
        }
      });
  }

  public void postCommitStoreFile(final byte[] family, Path srcPath, Path dstPath)
    throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postCommitStoreFile(this, family, srcPath, dstPath);
      }
    });
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   * @param map         Map of CF to List of file paths for the final loaded files
   */
  public void postBulkLoadHFile(final List<Pair<byte[], String>> familyPaths,
    Map<byte[], List<Path>> map) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("ad8df852-a74c-355b-a868-38285025fbd8"))) {
throw new java.io.IOException("Injected exception");
}
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postBulkLoadHFile(this, familyPaths, map);
      }
    });
  }

  public void postStartRegionOperation(final Operation op) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ec4e1a73-d6d4-3af3-b75d-b48d81292a10"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("92bb4110-597e-32d1-98df-ac49595c9658"))) {
throw new java.io.IOException("Injected exception");
}
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postStartRegionOperation(this, op);
      }
    });
  }

  public void postCloseRegionOperation(final Operation op) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postCloseRegionOperation(this, op);
      }
    });
  }

  /**
   * @param fs   fileystem to read from
   * @param p    path to the file
   * @param in   {@link FSDataInputStreamWrapper}
   * @param size Full size of the file
   * @param r    original reference file. This will be not null only when reading a split file.
   * @return a Reader instance to use instead of the base reader if overriding default behavior,
   *         null otherwise
   */
  public StoreFileReader preStoreFileReaderOpen(final FileSystem fs, final Path p,
    final FSDataInputStreamWrapper in, final long size, final CacheConfig cacheConf,
    final Reference r) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, StoreFileReader>(regionObserverGetter, null) {
        @Override
        public StoreFileReader call(RegionObserver observer) throws IOException {
          return observer.preStoreFileReaderOpen(this, fs, p, in, size, cacheConf, r, getResult());
        }
      });
  }

  /**
   * @param fs     fileystem to read from
   * @param p      path to the file
   * @param in     {@link FSDataInputStreamWrapper}
   * @param size   Full size of the file
   * @param r      original reference file. This will be not null only when reading a split file.
   * @param reader the base reader instance
   * @return The reader to use
   */
  public StoreFileReader postStoreFileReaderOpen(final FileSystem fs, final Path p,
    final FSDataInputStreamWrapper in, final long size, final CacheConfig cacheConf,
    final Reference r, final StoreFileReader reader) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return reader;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, StoreFileReader>(
      regionObserverGetter, reader) {
      @Override
      public StoreFileReader call(RegionObserver observer) throws IOException {
        return observer.postStoreFileReaderOpen(this, fs, p, in, size, cacheConf, r, getResult());
      }
    });
  }

  public List<Pair<Cell, Cell>> postIncrementBeforeWAL(final Mutation mutation,
    final List<Pair<Cell, Cell>> cellPairs) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return cellPairs;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, List<Pair<Cell, Cell>>>(regionObserverGetter,
        cellPairs) {
        @Override
        public List<Pair<Cell, Cell>> call(RegionObserver observer) throws IOException {
          return observer.postIncrementBeforeWAL(this, mutation, getResult());
        }
      });
  }

  public List<Pair<Cell, Cell>> postAppendBeforeWAL(final Mutation mutation,
    final List<Pair<Cell, Cell>> cellPairs) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return cellPairs;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, List<Pair<Cell, Cell>>>(regionObserverGetter,
        cellPairs) {
        @Override
        public List<Pair<Cell, Cell>> call(RegionObserver observer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a25a6218-87ae-3cf1-a376-0e386862c60c"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          return observer.postAppendBeforeWAL(this, mutation, getResult());
        }
      });
  }

  public void preWALAppend(WALKey key, WALEdit edit) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("36587bcd-c926-372b-a81a-995b8ae55e49"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f2a0f73c-d53b-31a8-ae56-0fa33d278aaf"))) {
return;
}
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preWALAppend(this, key, edit);
      }
    });
  }

  public Message preEndpointInvocation(final Service service, final String methodName,
    Message request) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return request;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<EndpointObserver, Message>(endpointObserverGetter, request) {
        @Override
        public Message call(EndpointObserver observer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("94ed0b0d-07a4-352c-9a35-6a7f7f51a519"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("b809d56b-4f3f-39c6-a89b-04fee0176210"))) {
throw new java.io.IOException("Injected exception");
}
          return observer.preEndpointInvocation(this, service, methodName, getResult());
        }
      });
  }

  public void postEndpointInvocation(final Service service, final String methodName,
    final Message request, final Message.Builder responseBuilder) throws IOException {
    execOperation(coprocEnvironments.isEmpty()
      ? null
      : new ObserverOperationWithoutResult<EndpointObserver>(endpointObserverGetter) {
        @Override
        public void call(EndpointObserver observer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("65f41dee-a386-3437-8f0e-d6411f75f0a1"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("173474d3-a306-366d-b929-ec802637963d"))) {
throw new java.io.IOException("Injected exception");
}
          observer.postEndpointInvocation(this, service, methodName, request, responseBuilder);
        }
      });
  }

  /**
   * @deprecated Since 2.0 with out any replacement and will be removed in 3.0
   */
  @Deprecated
  public DeleteTracker postInstantiateDeleteTracker(DeleteTracker result) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, DeleteTracker>(regionObserverGetter, result) {
        @Override
        public DeleteTracker call(RegionObserver observer) throws IOException {
          return observer.postInstantiateDeleteTracker(this, getResult());
        }
      });
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // BulkLoadObserver hooks
  /////////////////////////////////////////////////////////////////////////////////////////////////
  public void prePrepareBulkLoad(User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new BulkLoadObserverOperation(user) {
      @Override
      protected void call(BulkLoadObserver observer) throws IOException {
        observer.prePrepareBulkLoad(this);
      }
    });
  }

  public void preCleanupBulkLoad(User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new BulkLoadObserverOperation(user) {
      @Override
      protected void call(BulkLoadObserver observer) throws IOException {
        observer.preCleanupBulkLoad(this);
      }
    });
  }
}
