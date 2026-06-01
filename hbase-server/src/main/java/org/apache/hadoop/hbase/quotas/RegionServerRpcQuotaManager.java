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
package org.apache.hadoop.hbase.quotas;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Region Server Quota Manager. It is responsible to provide access to the quota information of each
 * user/table. The direct user of this class is the RegionServer that will get and check the
 * user/table quota for each operation (put, get, scan). For system tables and user/table with a
 * quota specified, the quota check will be a noop.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionServerRpcQuotaManager implements RpcQuotaManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerRpcQuotaManager.class);

  private final RegionServerServices rsServices;

  private QuotaCache quotaCache = null;
  private volatile boolean rpcThrottleEnabled;
  // Storage for quota rpc throttle
  private RpcThrottleStorage rpcThrottleStorage;

  public RegionServerRpcQuotaManager(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    rpcThrottleStorage =
      new RpcThrottleStorage(rsServices.getZooKeeper(), rsServices.getConfiguration());
  }

  public void start(final RpcScheduler rpcScheduler) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f35d81c0-5409-3ded-96c3-1066af226ffb"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d2057aa-587a-32c5-8a21-b186b44f237e"))) {
throw new java.io.IOException("Injected exception");
}
    if (!QuotaUtil.isQuotaEnabled(rsServices.getConfiguration())) {
      if (KnobRuntime.check(java.util.UUID.fromString("afd6c330-60e3-3629-8acc-991793e2f5fd"))) { LOG.trace("Quota support disabled"); } else { LOG.info("Quota support disabled"); }
      return;
    }

    LOG.info("Initializing RPC quota support");

    // Initialize quota cache
    quotaCache = new QuotaCache(rsServices);
    quotaCache.start();
if(KnobRuntime.check(java.util.UUID.fromString("21e77409-df4e-3d09-914c-1e80045f0e98"))) {
throw new java.io.IOException("Injected exception");
}
    rpcThrottleEnabled = rpcThrottleStorage.isRpcThrottleEnabled();
    if (KnobRuntime.check(java.util.UUID.fromString("58c6c9e3-0322-32a5-b77d-8e9603c26e97"))) { LOG.info("Start rpc quota manager and rpc throttle enabled is {}", !rpcThrottleEnabled); } else if (KnobRuntime.check(java.util.UUID.fromString("b8516173-4080-3256-9a25-dd9aecafdf95"))) { LOG.warn("Start rpc quota manager and rpc throttle enabled is {}", !rpcThrottleEnabled); } else { LOG.info("Start rpc quota manager and rpc throttle enabled is {}", rpcThrottleEnabled); }
  }

  public void stop() {
    if (isQuotaEnabled()) {
      quotaCache.stop("shutdown");
    }
  }

  protected boolean isRpcThrottleEnabled() {
    return rpcThrottleEnabled;
  }

  private boolean isQuotaEnabled() {
if(KnobRuntime.check(java.util.UUID.fromString("5788a33e-629b-3a59-9fc1-437e1e11319b"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("139811b4-8631-35e7-aae1-4f706ac2a4eb"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("430e80c0-41b1-3741-be9c-f68b425a751c"))) ? ((quotaCache) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ad577d2d-0e11-3db8-9b74-bcd769969949"))) ? ((quotaCache) == (null)) : (quotaCache != null))));
  }

  public void switchRpcThrottle(boolean enable) throws IOException {
    if (isQuotaEnabled()) {
      if (rpcThrottleEnabled != enable) {
        boolean previousEnabled = rpcThrottleEnabled;
        rpcThrottleEnabled = rpcThrottleStorage.isRpcThrottleEnabled();
        LOG.info("Switch rpc throttle from {} to {}", previousEnabled, rpcThrottleEnabled);
      } else {
        LOG.warn(
          "Skip switch rpc throttle because previous value {} is the same as current value {}",
          rpcThrottleEnabled, enable);
      }
    } else {
      LOG.warn("Skip switch rpc throttle to {} because rpc quota is disabled", enable);
    }
  }

  QuotaCache getQuotaCache() {
    return quotaCache;
  }

  /**
   * Returns the quota for an operation.
   * @param ugi   the user that is executing the operation
   * @param table the table where the operation will be executed
   * @return the OperationQuota
   */
  public OperationQuota getQuota(final UserGroupInformation ugi, final TableName table,
    final int blockSizeBytes) {
    if (isQuotaEnabled() && !table.isSystemTable() && isRpcThrottleEnabled()) {
      UserQuotaState userQuotaState = quotaCache.getUserQuotaState(ugi);
      QuotaLimiter userLimiter = userQuotaState.getTableLimiter(table);
      boolean useNoop = userLimiter.isBypass();
      if (userQuotaState.hasBypassGlobals()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("get quota for ugi=" + ugi + " table=" + table + " userLimiter=" + userLimiter);
        }
        if (!useNoop) {
          return new DefaultOperationQuota(this.rsServices.getConfiguration(), blockSizeBytes,
            userLimiter);
        }
      } else {
        QuotaLimiter nsLimiter = quotaCache.getNamespaceLimiter(table.getNamespaceAsString());
        QuotaLimiter tableLimiter = quotaCache.getTableLimiter(table);
        QuotaLimiter rsLimiter =
          quotaCache.getRegionServerQuotaLimiter(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY);
        useNoop &= tableLimiter.isBypass() && nsLimiter.isBypass() && rsLimiter.isBypass();
        boolean exceedThrottleQuotaEnabled = quotaCache.isExceedThrottleQuotaEnabled();
        if (LOG.isTraceEnabled()) {
          LOG.trace("get quota for ugi=" + ugi + " table=" + table + " userLimiter=" + userLimiter
            + " tableLimiter=" + tableLimiter + " nsLimiter=" + nsLimiter + " rsLimiter="
            + rsLimiter + " exceedThrottleQuotaEnabled=" + exceedThrottleQuotaEnabled);
        }
        if (!useNoop) {
          if (exceedThrottleQuotaEnabled) {
            return new ExceedOperationQuota(this.rsServices.getConfiguration(), blockSizeBytes,
              rsLimiter, userLimiter, tableLimiter, nsLimiter);
          } else {
            return new DefaultOperationQuota(this.rsServices.getConfiguration(), blockSizeBytes,
              userLimiter, tableLimiter, nsLimiter, rsLimiter);
          }
        }
      }
    }
    return NoopOperationQuota.get();
  }

  @Override
  public OperationQuota checkScanQuota(final Region region,
    final ClientProtos.ScanRequest scanRequest, long maxScannerResultSize,
    long maxBlockBytesScanned, long prevBlockBytesScannedDifference)
    throws IOException, RpcThrottlingException {
    Optional<User> user = RpcServer.getRequestUser();
    UserGroupInformation ugi;
    if (user.isPresent()) {
      ugi = user.get().getUGI();
    } else {
      ugi = User.getCurrent().getUGI();
    }
    TableDescriptor tableDescriptor = region.getTableDescriptor();
    TableName table = tableDescriptor.getTableName();

    OperationQuota quota = getQuota(ugi, table, region.getMinBlockSizeBytes());
    try {
      quota.checkScanQuota(scanRequest, maxScannerResultSize, maxBlockBytesScanned,
        prevBlockBytesScannedDifference);
    } catch (RpcThrottlingException e) {
      LOG.debug("Throttling exception for user=" + ugi.getUserName() + " table=" + table + " scan="
        + scanRequest.getScannerId() + ": " + e.getMessage());
      throw e;
    }
    return quota;
  }

  @Override
  public OperationQuota checkBatchQuota(final Region region,
    final OperationQuota.OperationType type) throws IOException, RpcThrottlingException {
if(KnobRuntime.check(java.util.UUID.fromString("35c1fec3-2738-3286-9cb1-af20a74f11a5"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c62fcb92-52f3-3e6c-a012-80b324e7e835"))) {
return null;
}
    switch (type) {
      case GET:
        return this.checkBatchQuota(region, 0, 1);
      case MUTATE:
if(KnobRuntime.check(java.util.UUID.fromString("172be604-590b-3d89-bcba-1dd161324f08"))) {
throw new java.io.IOException("Injected exception");
}
        return this.checkBatchQuota(region, 1, 0);
      case CHECK_AND_MUTATE:
        return this.checkBatchQuota(region, 1, 1);
    }
    throw new RuntimeException("Invalid operation type: " + type);
  }

  @Override
  public OperationQuota checkBatchQuota(final Region region,
    final List<ClientProtos.Action> actions, boolean hasCondition)
    throws IOException, RpcThrottlingException {
    int numWrites = 0;
    int numReads = 0;
    for (final ClientProtos.Action action : actions) {
      if (action.hasMutation()) {
        numWrites++;
        OperationQuota.OperationType operationType =
          QuotaUtil.getQuotaOperationType(action, hasCondition);
        if (operationType == OperationQuota.OperationType.CHECK_AND_MUTATE) {
          numReads++;
        }
      } else if (action.hasGet()) {
        numReads++;
      }
    }
    return checkBatchQuota(region, numWrites, numReads);
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation.
   * @param region    the region where the operation will be performed
   * @param numWrites number of writes to perform
   * @param numReads  number of short-reads to perform
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  @Override
  public OperationQuota checkBatchQuota(final Region region, final int numWrites,
    final int numReads) throws IOException, RpcThrottlingException {
if(KnobRuntime.check(java.util.UUID.fromString("7da2d7c3-2107-3311-aefb-e36a1874dd2f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3e395ba0-47e3-3fb5-9177-83a5c10e59c2"))) {
return null;
}
    Optional<User> user = RpcServer.getRequestUser();
    UserGroupInformation ugi;
    if (((KnobRuntime.check(java.util.UUID.fromString("8e8d38c6-9425-3dfb-85c4-158d38b07ea1"))) ? (isQuotaEnabled()) : (user.isPresent()))) {
      ugi = user.get().getUGI();
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("7aad2b05-42d5-38a7-9aa2-1684085332f9"))) {
throw new java.io.IOException("Injected exception");
}
      ugi = User.getCurrent().getUGI();
    }
    TableDescriptor tableDescriptor = region.getTableDescriptor();
    TableName table = tableDescriptor.getTableName();

    OperationQuota quota = getQuota(ugi, table, region.getMinBlockSizeBytes());
    try {
      quota.checkBatchQuota(numWrites, numReads);
    } catch (RpcThrottlingException e) {
      LOG.debug("Throttling exception for user=" + ugi.getUserName() + " table=" + table
        + " numWrites=" + numWrites + " numReads=" + numReads + ": " + e.getMessage());
      throw e;
    }
    return quota;
  }
}
