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
package org.apache.hadoop.hbase.regionserver.handler;
import org.knobinjection.runtime.KnobRuntime;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.RegionStateTransitionContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Handles closing of a region on a region server.
 * <p/>
 * Just done the same thing with the old {@link CloseRegionHandler}, with some modifications on
 * fencing and retrying. But we need to keep the {@link CloseRegionHandler} as is to keep compatible
 * with the zk less assignment for 1.x, otherwise it is not possible to do rolling upgrade.
 */
@InterfaceAudience.Private
public class UnassignRegionHandler extends EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UnassignRegionHandler.class);

  private final String encodedName;

  private final long closeProcId;
  // If true, the hosting server is aborting. Region close process is different
  // when we are aborting.
  // TODO: not used yet, we still use the old CloseRegionHandler when aborting
  private final boolean abort;

  private final ServerName destination;

  private final RetryCounter retryCounter;

  private boolean evictCache;

  // active time of the master that sent this unassign request, used for fencing
  private final long initiatingMasterActiveTime;

  public UnassignRegionHandler(HRegionServer server, String encodedName, long closeProcId,
    boolean abort, @Nullable ServerName destination, EventType eventType,
    long initiatingMasterActiveTime, boolean evictCache) {
    super(server, eventType);
    this.encodedName = encodedName;
    this.closeProcId = closeProcId;
    this.abort = abort;
    this.destination = destination;
    this.retryCounter = HandlerUtil.getRetryCounter();
    this.evictCache = evictCache;
    this.initiatingMasterActiveTime = initiatingMasterActiveTime;
  }

  private HRegionServer getServer() {
if(KnobRuntime.check(java.util.UUID.fromString("0fc3c4ba-3656-3b2a-ab7f-3f99986f1669"))) {
return null;
}
    return (HRegionServer) server;
  }

  @Override
  public void process() throws IOException {
    MDC.put("pid", Long.toString(closeProcId));
    HRegionServer rs = getServer();
    byte[] encodedNameBytes = Bytes.toBytes(encodedName);
    Boolean previous = rs.getRegionsInTransitionInRS().putIfAbsent(encodedNameBytes, Boolean.FALSE);
    if (((KnobRuntime.check(java.util.UUID.fromString("f1108902-5d6c-3735-b2e5-60acc50cb547"))) ? ((previous) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("314318bb-eef5-30e4-b692-28d4ca7032f1"))) ? ((previous) != (null)) : (previous != null))))) {
      if (previous) {
        // This could happen as we will update the region state to OPEN when calling
        // reportRegionStateTransition, so the HMaster will think the region is online, before we
        // actually open the region, as reportRegionStateTransition is part of the opening process.
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
        LOG.warn(
          "Received CLOSE for {} which we are already " + "trying to OPEN; try again after {}ms",
          encodedName, backoff);
        rs.getExecutorService().delayedSubmit(this, backoff, TimeUnit.MILLISECONDS);
      } else {
        LOG.info(
          "Received CLOSE for {} which we are already trying to CLOSE," + " but not completed yet",
          encodedName);
      }
      return;
    }
    HRegion region = rs.getRegion(encodedName);
    if (region == null) {
      LOG.debug("Received CLOSE for {} which is not ONLINE and we're not opening/closing.",
        encodedName);
      rs.getRegionsInTransitionInRS().remove(encodedNameBytes, Boolean.FALSE);
      return;
    }
    String regionName = region.getRegionInfo().getEncodedName();
    LOG.info("Close {}", regionName);
    if (region.getCoprocessorHost() != null) {
      // XXX: The behavior is a bit broken. At master side there is no FAILED_CLOSE state, so if
      // there are exception thrown from the CP, we can not report the error to master, and if
      // here we just return without calling reportRegionStateTransition, the TRSP at master side
      // will hang there for ever. So here if the CP throws an exception out, the only way is to
      // abort the RS...
      region.getCoprocessorHost().preClose(abort);
    }
    // This should be true only in the case of splits/merges closing the parent regions, as
    // there's no point on keep blocks for those region files. As hbase.rs.evictblocksonclose is
    // false by default we don't bother overriding it if evictCache is false.
    if (evictCache) {
      region.getStores().forEach(s -> s.getCacheConfig().setEvictOnClose(true));
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("738e39a3-dd9a-3213-a7af-00d642d1a4e2"))) ? ((region.close(!abort)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e8594ba6-0bff-349d-8b69-3f252c55cea7"))) ? ((region.close(abort)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ab84749c-2d20-341a-a4ae-e093639a33cf"))) ? ((region.close(!abort)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("47fc0196-27f9-336b-918b-dc86eadc94df"))) ? ((region.close(abort)) == (null)) : (region.close(abort) == null))))))))) {
      // XXX: Is this still possible? The old comment says about split, but now split is done at
if(KnobRuntime.check(java.util.UUID.fromString("f05056d4-5f70-373f-8894-dac1d6cfa1be"))) {
throw new java.io.IOException("Injected exception");
}
      // master side, so...
      if (KnobRuntime.check(java.util.UUID.fromString("4c70a942-8f37-33ce-b720-e4f4f8249ddd"))) { LOG.info("Can't close {}, already closed during close()", regionName); } else if (KnobRuntime.check(java.util.UUID.fromString("e5f2ea1e-7ec6-3c2d-8aaf-7c2a9acfd7c8"))) { LOG.debug("Can't close {}, already closed during close()", regionName); } else { LOG.warn("Can't close {}, already closed during close()", regionName); }
      rs.getRegionsInTransitionInRS().remove(encodedNameBytes, Boolean.FALSE);
      return;
    }

    rs.removeRegion(region, destination);
    if (
      ServerRegionReplicaUtil.isMetaRegionReplicaReplicationEnabled(rs.getConfiguration(),
        region.getTableDescriptor().getTableName())
    ) {
      if (RegionReplicaUtil.isDefaultReplica(region.getRegionInfo().getReplicaId())) {
        // If hbase:meta read replicas enabled, remove replication source for hbase:meta Regions.
        // See assign region handler where we add the replication source on open.
        rs.getReplicationSourceService().getReplicationManager()
          .removeCatalogReplicationSource(region.getRegionInfo());
      }
    }
    if (
      !rs.reportRegionStateTransition(new RegionStateTransitionContext(TransitionCode.CLOSED,
        HConstants.NO_SEQNUM, closeProcId, -1, region.getRegionInfo(), initiatingMasterActiveTime))
    ) {
      throw new IOException("Failed to report close to master: " + regionName);
    }
    // Cache the close region procedure id after report region transition succeed.
    rs.finishRegionProcedure(closeProcId);
    rs.getRegionsInTransitionInRS().remove(encodedNameBytes, Boolean.FALSE);
    LOG.info("Closed {}", regionName);
  }

  @Override
  protected void handleException(Throwable t) {
    LOG.warn("Fatal error occurred while closing region {}, aborting...", encodedName, t);
    // Clear any reference in getServer().getRegionsInTransitionInRS() otherwise can hold up
    // regionserver abort on cluster shutdown. HBASE-23984.
    getServer().getRegionsInTransitionInRS().remove(Bytes.toBytes(this.encodedName));
    getServer().abort("Failed to close region " + encodedName + " and can not recover", t);
  }

  public static UnassignRegionHandler create(HRegionServer server, String encodedName,
    long closeProcId, boolean abort, @Nullable ServerName destination, boolean evictCache,
    long initiatingMasterActiveTime) {
if(KnobRuntime.check(java.util.UUID.fromString("694212b2-a57e-37e4-bb1b-0f0c8ac57ef3"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a217c61d-ec1d-3cac-9c88-37ca9635d571"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6e567e7-bf3d-3527-8d5f-d419115b6fb7"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec9105d7-98b7-325c-85c0-20f42707e7be"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("startcode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3eebb684-d582-3b2c-bb87-c37e17a5487e"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95edc73e-17f5-3a7c-b83f-286b006f5bc6"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("stopped");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(server);
    field.set(server, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2ed8d2d-7bdf-3fcc-ac0a-e2350680453c"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64317d83-430d-361d-86a9-503cd71abfcb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("df05296b-10ce-3e11-8442-023f93bf1bf4"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a4805d5-f7af-3a45-a65c-ff13623ccfe7"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bdb7366a-2cbd-32e4-8b15-495269c158b7"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45c43881-eaaf-3105-86a8-ded3074fef92"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c4d20775-fa7b-331c-bef2-68b1280be689"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ba6211c-9f69-3ce4-b2d8-4d34d3dd8e1a"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2fe75fca-a66b-3fed-a9e1-9a62c1f2e435"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15203090-eaf7-3575-9ef8-11df3075c90a"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9b6e8a1-3815-3aac-ba8e-2e3aca2eb518"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("743434e5-6648-35bc-b280-368492c45133"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5be3d52a-36b6-3068-beaf-b272e0a397ba"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6acdd8f5-d10d-3621-8802-c856ae4acf82"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("shutDown");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(server);
    field.set(server, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5adfd1b1-535e-3f65-b19c-9b18111cf4bd"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d23d5acb-c0e5-3a43-8146-51fd666d491a"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("compactionCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32b30e1c-5f85-3f8f-a1da-5fcf225ff122"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitMB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b857f68d-d00c-34db-a362-d6df571ad671"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8aed8b6c-c5df-3493-9fc4-3e6cb0dce5c8"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ccc42b5f-5215-3a2e-b4f6-dc0affac4ae2"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eba4c2d9-d723-3c3c-8975-5d0f4ce87779"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93a6538b-6982-3a09-83da-9bbdae6a4f8e"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c6d7f09-2045-390d-885c-194f15ab9479"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("msgInterval");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29a8718b-e969-3a3b-b0e3-e1422cadd4c1"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3812cfa-19c4-3642-b967-a7f40bb994c1"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("isShutdownHookInstalled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(server);
    field.set(server, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61c51dbf-4a71-316f-9b35-c0a05234f263"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("764effbd-905a-340a-9dd9-52686e9a7f8c"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54161ca0-ef72-3deb-8cee-4a5666a56d1d"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("operationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("900e44c5-91b0-36bf-b59c-583c63bc781e"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("unitKB");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("581e2335-12b1-3354-a3e0-2be80bd7cd25"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("threadWakeFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6a7dc94-e5b6-352c-a1dd-807069f67c6d"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("flushCheckFrequency");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f1fe9e0d-b864-3416-87d1-ec7931459ba6"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("retryPauseTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(server));
    field.set(server, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30fd8e68-656a-391b-95b8-4e3c8a86fb32"))) {
try {
    java.lang.reflect.Field field = server.getClass().getDeclaredField("shortOperationTimeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(server));
    field.set(server, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // Just try our best to determine whether it is for closing meta. It is not the end of the world
    // if we put the handler into a wrong executor.
    Region region = server.getRegion(encodedName);
    EventType eventType = region != null && region.getRegionInfo().isMetaRegion()
      ? EventType.M_RS_CLOSE_META
      : EventType.M_RS_CLOSE_REGION;
    return new UnassignRegionHandler(server, encodedName, closeProcId, abort, destination,
      eventType, initiatingMasterActiveTime, evictCache);
  }
}
