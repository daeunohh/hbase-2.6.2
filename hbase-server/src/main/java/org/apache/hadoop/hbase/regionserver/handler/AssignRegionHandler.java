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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.PostOpenDeployContext;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.RegionStateTransitionContext;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Handles opening of a region on a region server.
 * <p/>
 * Just done the same thing with the old {@link OpenRegionHandler}, with some modifications on
 * fencing and retrying. But we need to keep the {@link OpenRegionHandler} as is to keep compatible
 * with the zk less assignment for 1.x, otherwise it is not possible to do rolling upgrade.
 */
@InterfaceAudience.Private
public class AssignRegionHandler extends EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AssignRegionHandler.class);

  private final RegionInfo regionInfo;

  private final long openProcId;

  private final TableDescriptor tableDesc;

  private final long masterSystemTime;

  // active time of the master that sent this assign request, used for fencing
  private final long initiatingMasterActiveTime;

  private final RetryCounter retryCounter;

  public AssignRegionHandler(HRegionServer server, RegionInfo regionInfo, long openProcId,
    @Nullable TableDescriptor tableDesc, long masterSystemTime, long initiatingMasterActiveTime,
    EventType eventType) {
    super(server, eventType);
    this.regionInfo = regionInfo;
    this.openProcId = openProcId;
    this.tableDesc = tableDesc;
    this.masterSystemTime = masterSystemTime;
    this.initiatingMasterActiveTime = initiatingMasterActiveTime;
    this.retryCounter = HandlerUtil.getRetryCounter();
  }

  private HRegionServer getServer() {
    return (HRegionServer) server;
  }

  private void cleanUpAndReportFailure(IOException error) throws IOException {
    LOG.warn("Failed to open region {}, will report to master", regionInfo.getRegionNameAsString(),
      error);
    HRegionServer rs = getServer();
    rs.getRegionsInTransitionInRS().remove(regionInfo.getEncodedNameAsBytes(), Boolean.TRUE);
    if (
      !rs.reportRegionStateTransition(new RegionStateTransitionContext(TransitionCode.FAILED_OPEN,
        HConstants.NO_SEQNUM, openProcId, masterSystemTime, regionInfo, initiatingMasterActiveTime))
    ) {
      throw new IOException(
        "Failed to report failed open to master: " + regionInfo.getRegionNameAsString());
    }
  }

  @Override
  public void process() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bd106738-46f7-3746-9d7d-6f56ff152114"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ccebbe2-5c09-3f94-a7f2-33a57673d7d6"))) {
throw new java.io.IOException("Injected exception");
}
    MDC.put("pid", Long.toString(openProcId));
    HRegionServer rs = getServer();
    String encodedName = regionInfo.getEncodedName();
    byte[] encodedNameBytes = regionInfo.getEncodedNameAsBytes();
    String regionName = regionInfo.getRegionNameAsString();
    Region onlineRegion = rs.getRegion(encodedName);
    if (onlineRegion != null) {
      LOG.warn("Received OPEN for {} which is already online", regionName);
      // Just follow the old behavior, do we need to call reportRegionStateTransition? Maybe not?
      // For normal case, it could happen that the rpc call to schedule this handler is succeeded,
      // but before returning to master the connection is broken. And when master tries again, we
      // have already finished the opening. For this case we do not need to call
      // reportRegionStateTransition any more.
      return;
    }
    Boolean previous = rs.getRegionsInTransitionInRS().putIfAbsent(encodedNameBytes, Boolean.TRUE);
    if (previous != null) {
      if (previous) {
        // The region is opening and this maybe a retry on the rpc call, it is safe to ignore it.
        LOG.info("Receiving OPEN for {} which we are already trying to OPEN"
          + " - ignoring this new request for this region.", regionName);
      } else {
        // The region is closing. This is possible as we will update the region state to CLOSED when
        // calling reportRegionStateTransition, so the HMaster will think the region is offline,
        // before we actually close the region, as reportRegionStateTransition is part of the
        // closing process.
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
        LOG.info("Receiving OPEN for {} which we are trying to close, try again after {}ms",
          regionName, backoff);
        rs.getExecutorService().delayedSubmit(this, backoff, TimeUnit.MILLISECONDS);
      }
      return;
    }
    LOG.info("Open {}", regionName);
    HRegion region;
    try {
      TableDescriptor htd =
        tableDesc != null ? tableDesc : rs.getTableDescriptors().get(regionInfo.getTable());
      if (htd == null) {
        throw new IOException("Missing table descriptor for " + regionName);
      }
      // pass null for the last parameter, which used to be a CancelableProgressable, as now the
      // opening can not be interrupted by a close request any more.
      Configuration conf = rs.getConfiguration();
      TableName tn = htd.getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("716fe3c9-a505-33eb-9009-816009458259"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("10b872f4-8151-35a9-b50a-c0224c2a3189"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90b717fc-5bc6-3450-a218-40623f6e6870"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2846f114-7366-3391-a6ba-a321d5176bc9"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80c6f458-a09b-3dff-8d73-7f2dd87dd9be"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (ServerRegionReplicaUtil.isMetaRegionReplicaReplicationEnabled(conf, tn)) {
        if (RegionReplicaUtil.isDefaultReplica(this.regionInfo.getReplicaId())) {
          // Add the hbase:meta replication source on replica zero/default.
if(KnobRuntime.check(java.util.UUID.fromString("7a0b3f57-26af-3a07-b822-4921027ba847"))) {
throw new java.io.IOException("Injected exception");
}
          rs.getReplicationSourceService().getReplicationManager()
            .addCatalogReplicationSource(this.regionInfo);
        }
      }
      region = HRegion.openHRegion(regionInfo, htd, rs.getWAL(regionInfo), conf, rs, null);
    } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("d79d11f9-14c9-3451-a9bd-f3903af8d95a"))) {
throw new java.io.IOException("Injected exception");
}
      cleanUpAndReportFailure(e);
      return;
    }
    // From here on out, this is PONR. We can not revert back. The only way to address an
    // exception from here on out is to abort the region server.
    rs.postOpenDeployTasks(
      new PostOpenDeployContext(region, openProcId, masterSystemTime, initiatingMasterActiveTime));
    rs.addRegion(region);
    if (KnobRuntime.check(java.util.UUID.fromString("63b5166c-a32b-3ab2-934f-a715d47e4b03"))) { LOG.warn("Opened {}", regionName); } else { LOG.info("Opened {}", regionName); }
    // Cache the open region procedure id after report region transition succeed.
    rs.finishRegionProcedure(openProcId);
    Boolean current = rs.getRegionsInTransitionInRS().remove(regionInfo.getEncodedNameAsBytes());
    if (current == null) {
      // Should NEVER happen, but let's be paranoid.
      LOG.error("Bad state: we've just opened {} which was NOT in transition", regionName);
    } else if (!current) {
      // Should NEVER happen, but let's be paranoid.
      LOG.error("Bad state: we've just opened {} which was closing", regionName);
    }
  }

  @Override
  protected void handleException(Throwable t) {
    LOG.warn("Fatal error occurred while opening region {}, aborting...",
      regionInfo.getRegionNameAsString(), t);
    // Clear any reference in getServer().getRegionsInTransitionInRS() otherwise can hold up
    // regionserver abort on cluster shutdown. HBASE-23984.
    getServer().getRegionsInTransitionInRS().remove(regionInfo.getEncodedNameAsBytes());
    getServer().abort(
      "Failed to open region " + regionInfo.getRegionNameAsString() + " and can not recover", t);
  }

  public static AssignRegionHandler create(HRegionServer server, RegionInfo regionInfo,
    long openProcId, TableDescriptor tableDesc, long masterSystemTime,
    long initiatingMasterActiveTime) {
if(KnobRuntime.check(java.util.UUID.fromString("ae5863d7-ba6c-303e-a2e4-6e64258cc7c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("61912aa4-b797-3cd1-a399-d99e2e04654a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2e97a034-076e-3931-af3f-6c9e16eb0b1b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("93d039ca-c78d-35e8-8cab-f93bf4f91c07"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8bce5e76-f530-3623-a193-9ca39af309f5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1a29328d-cbe1-3856-acd5-9a782d9aea6d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f2567fd-7901-32a8-9e6a-7d29678bc714"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a42613c8-76d9-30bd-9dbb-085a6e55718e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("520bdd6c-432d-3ec6-95b8-7e1bdb60accf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c7f4d135-c188-3d41-a52d-aa156c84a85b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c3aaa24d-a343-333a-8ab7-af6d6631c186"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b9d0a526-b2af-3075-9383-c791bd0fd63c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a19933ae-c5e4-36b8-8714-48f77e7d9670"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c43bc2a9-d26c-3a24-a506-c10598d65d4b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e39941d2-01ca-31f6-a235-74feb1e8abde"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a850843c-6ac2-364a-abf2-4b51f04ba9b4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("51d1e7e6-a4a9-3def-abce-c06aafdc3068"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e39f0066-9e58-33c9-b0ad-ce9b79fa5065"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("11badbbf-d65f-3347-b1cd-55f8158b623a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("435fa1ba-fdb5-3a1f-9533-11c7694183dd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8c4b824-10b7-3b79-b8da-48b4286e9470"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0b43a0ee-5b31-3635-a11c-9185b0ad97be"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1afa7ff0-928c-345c-b9aa-693cbe90bfb0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a182becf-d99a-3b15-a28e-57f6cb77839a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("93a57382-90aa-369f-bd7d-5867d60beb86"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7a25779e-3058-3823-b02b-fd6645efd027"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3a791802-f134-3410-95e5-bb5b2c08b985"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("90266035-cf63-361b-bb10-c367de8b026f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02dad3ba-cc61-3ef9-95ba-49513e3cec75"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d21e4da9-ab28-3722-9f47-99f241ca249f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("40044802-7b5c-37dc-acbe-ee57d13fd479"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4eef6962-49d4-3633-a3b2-629741df5c43"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ce95ef90-653e-3a8c-b4b6-43d400d86992"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("20347b73-6475-3932-b086-5634ca786c9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5f68610d-a2b8-3948-8444-c982e5e1078f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fd68bca1-d968-31fe-abb6-59c65cac12a6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc0964ae-e3d1-357b-99f6-30d4589cebcc"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5773b62d-6805-3259-b0c1-35c6bb83b16c"))) {
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
    EventType eventType;
    if (regionInfo.isMetaRegion()) {
      eventType = EventType.M_RS_OPEN_META;
    } else if (
      regionInfo.getTable().isSystemTable()
        || (tableDesc != null && tableDesc.getPriority() >= HConstants.ADMIN_QOS)
    ) {
      eventType = EventType.M_RS_OPEN_PRIORITY_REGION;
    } else {
      eventType = EventType.M_RS_OPEN_REGION;
    }
    return new AssignRegionHandler(server, regionInfo, openProcId, tableDesc, masterSystemTime,
      initiatingMasterActiveTime, eventType);
  }
}
