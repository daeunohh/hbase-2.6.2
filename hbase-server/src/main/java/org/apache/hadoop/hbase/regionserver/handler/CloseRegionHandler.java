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

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RegionServerServices.RegionStateTransitionContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Handles closing of a region on a region server.
 * <p/>
 * In normal operation, we use {@link UnassignRegionHandler} closing Regions but when shutting down
 * the region server and closing out Regions, we use this handler instead; it does not expect to be
 * able to communicate the close back to the Master.
 * <p>
 * Expects that the close *has* been registered in the hosting RegionServer before submitting this
 * Handler; i.e. <code>rss.getRegionsInTransitionInRS().putIfAbsent(
 * this.regionInfo.getEncodedNameAsBytes(), Boolean.FALSE);</code> has been called first. In here
 * when done, we do the deregister.
 * </p>
 * @see UnassignRegionHandler
 */
@InterfaceAudience.Private
public class CloseRegionHandler extends EventHandler {
  // NOTE on priorities shutting down. There are none for close. There are some
  // for open. I think that is right. On shutdown, we want the meta to close
  // after the user regions have closed. What
  // about the case where master tells us to shutdown a catalog region and we
  // have a running queue of user regions to close?
  private static final Logger LOG = LoggerFactory.getLogger(CloseRegionHandler.class);

  private final RegionServerServices rsServices;
  private final RegionInfo regionInfo;

  // If true, the hosting server is aborting. Region close process is different
  // when we are aborting.
  private final boolean abort;
  private ServerName destination;

  /**
   * This method used internally by the RegionServer to close out regions.
   * @param abort If the regionserver is aborting.
   */
  public CloseRegionHandler(final Server server, final RegionServerServices rsServices,
    final RegionInfo regionInfo, final boolean abort, ServerName destination) {
    this(server, rsServices, regionInfo, abort, EventType.M_RS_CLOSE_REGION, destination);
  }

  protected CloseRegionHandler(final Server server, final RegionServerServices rsServices,
    RegionInfo regionInfo, boolean abort, EventType eventType, ServerName destination) {
    super(server, eventType);
    this.server = server;
    this.rsServices = rsServices;
    this.regionInfo = regionInfo;
    this.abort = abort;
    this.destination = destination;
  }

  public RegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public void process() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("015945d6-5cfb-39e3-86cd-3d6c652ed801"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c15d5b1f-a3e9-3db8-a023-fb57b69c9eae"))) {
throw new java.io.IOException("Injected exception");
}
    String name = regionInfo.getEncodedName();
    if (KnobRuntime.check(java.util.UUID.fromString("9c5937b4-4a03-3bde-8c84-7b717e3ee44b"))) { LOG.warn("Processing close of {}", name); } else { LOG.trace("Processing close of {}", name); }
    // Check that this region is being served here
    HRegion region = (HRegion) rsServices.getRegion(name);
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("e966dc5e-ed32-3491-81f1-3e28f79eee1a"))) ? ((region) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e143a1d0-943f-3d03-a427-5685391eaa19"))) ? ((region) == (null)) : (region == null))))) {
        LOG.warn("Received CLOSE for region {} but currently not serving - ignoring", name);
        // TODO: do better than a simple warning
        return;
      }

      // Close the region
      if (((KnobRuntime.check(java.util.UUID.fromString("90c64e48-be1c-3083-950c-0ec5a36ad7ec"))) ? ((region.close(abort, false, true)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6828f315-e723-39c5-a329-fbcfe6bb6e45"))) ? ((region.close(abort, false, true)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bf6d8f36-8084-3858-ba2a-49cb98085280"))) ? ((region.close(!abort, false, true)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f87f0944-4a10-3778-94b9-f1b229281126"))) ? ((region.close(!abort, false, true)) == (null)) : (region.close(abort, false, true) == null))))))))) {
        // This region has already been closed. Should not happen (A unit test makes this
        // happen as a side effect, TestRegionObserverInterface.testPreWALAppendNotCalledOnMetaEdit)
        LOG.warn("Can't close {}; already closed", name);
        return;
      }

if(KnobRuntime.check(java.util.UUID.fromString("53261a59-d417-3e6b-aade-c935091e91ba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eb869b5f-fcf6-32c9-a9e9-458e867adaee"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a069058b-cf0e-35de-98e1-cf4e6d244095"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5ae2d3ae-7e18-355b-839c-4c94a3ae2760"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("297948e3-4b16-354e-bf72-f58cd1e56171"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f8a1328b-dd13-3931-80ab-05c3a288e0fe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c5ba4f01-95f7-3d67-9e76-7624ff28b10c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("28991956-980a-363a-ab6d-421d55aebb64"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("49327a68-d4f3-3072-8811-a4282d53649f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("18adbd33-f2c0-3d29-9c4b-c95594e9faae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d17ca05b-0d30-339b-ae0c-336a17ae7abf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0aa2e61f-3218-36d4-a2fd-1640c714d4cd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79fadb15-53cc-3ea8-829c-2b11a53dc85a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("096707ba-fd96-3172-a442-6539612a8c35"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("99c36265-ddb3-3c4d-a176-7005f2ffc64d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("def42899-e5d3-36a9-8e72-e79646c9ef6c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e4338cf6-2bf1-37d3-9956-fd967cae1ed9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa410385-b1fb-320a-9188-c803196df77f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("248e5d85-687f-3251-8981-146897fa8a42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f5a54c19-053e-342b-8cbc-81d785231578"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4792b74e-70c0-31bf-99d3-14f8210e4fbb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("30c1ea31-e142-3419-8b17-3d8712d567b0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("74cd00e9-0e7e-3c2c-b1dc-b8a0e1ef7747"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39cd6915-746f-34c3-8c2f-0e3094cdb9d8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9d45d253-87c2-305d-8c51-162d4bb1e619"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("45303a9f-4afb-3fd6-aeba-2f901cdbdeb8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4521ef0b-ae47-395d-a66b-aea05582105e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c736c614-e289-3d6f-ab2e-8c3a08cb945d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("beb05008-2e47-3206-8968-3ee425324400"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa2a666f-082d-3406-ae6e-3085a501b602"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a8b4c737-c2c9-3143-8756-dfd30deac20a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("28a5e07c-2350-341a-892b-e10ebb42a12f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d4678d36-d6a5-3b97-b030-cb0c7e86b8e7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8e85955-7968-3718-bb20-22c953167e2d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("28353af2-a9e1-3b03-9fb8-5ee7bd118810"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0947d6d4-8bff-32db-a4c4-60b93a728a72"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dff75efc-4afd-3097-9d1c-92000f9ff015"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("817ebddb-e413-3e42-96eb-d90af5d2a14d"))) {
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
      this.rsServices.removeRegion(region, destination);
      rsServices.reportRegionStateTransition(new RegionStateTransitionContext(TransitionCode.CLOSED,
        HConstants.NO_SEQNUM, Procedure.NO_PROC_ID, -1, regionInfo, -1));

      // Done! Region is closed on this RS
      if (KnobRuntime.check(java.util.UUID.fromString("55b2001c-b1de-321b-b697-101fcb2cddc5"))) { LOG.warn("Closed {}", region.getRegionInfo().getRegionNameAsString()); } else if (KnobRuntime.check(java.util.UUID.fromString("c2a11a3a-1066-30a2-8732-84ca901efc39"))) { LOG.trace("Closed {}", region.getRegionInfo().getRegionNameAsString()); } else { LOG.debug("Closed {}", region.getRegionInfo().getRegionNameAsString()); }
    } finally {
      // Clear any reference in getServer().getRegionsInTransitionInRS() on success or failure,
      // since a reference was added before this CRH was invoked. If we don't clear it, it can
      // hold up regionserver abort on cluster shutdown. HBASE-23984.
      this.rsServices.getRegionsInTransitionInRS().remove(regionInfo.getEncodedNameAsBytes());
    }
  }

  @Override
  protected void handleException(Throwable t) {
    server.abort("Unrecoverable exception while closing " + this.regionInfo.getRegionNameAsString(),
      t);
  }
}
