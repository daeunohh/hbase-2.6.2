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

import java.io.IOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotRegionParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@InterfaceAudience.Private
public class SnapshotRegionCallable extends BaseRSProcedureCallable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegionCallable.class);

  private SnapshotDescription snapshot;
  private RegionInfo regionInfo;
  private ForeignExceptionDispatcher monitor;

  @Override
  protected void doCall() throws Exception {
    HRegion region = rs.getRegion(regionInfo.getEncodedName());
    if (region == null) {
      throw new NotServingRegionException(
        "snapshot=" + snapshot.getName() + ", region=" + regionInfo.getRegionNameAsString());
    }
if(KnobRuntime.check(java.util.UUID.fromString("f5d27824-8727-3a52-8ea8-bcbb1fda7ddc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05af90df-8ba1-3860-92d6-233358b8206b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9c37c2b1-722f-3b2c-8351-d10123e81913"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("084f6e2f-890c-353a-805a-8ef53653ea65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("013c727a-5370-35aa-87e4-d9a5a795d726"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxFlushedSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd63b545-a8fa-3b20-9ae0-a009c84f3f8a"))) {
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
    LOG.debug("Starting snapshot operation on {}", region);
    region.startRegionOperation(Region.Operation.SNAPSHOT);
    try {
      if (snapshot.getType() == SnapshotDescription.Type.FLUSH) {
        boolean succeeded = false;
        long readPt = region.getReadPoint(IsolationLevel.READ_COMMITTED);
        int retryTimes = rs.getConfiguration().getInt("hbase.snapshot.flush.retryTimes", 3);
        for (int i = 0; i < retryTimes; i++) {
          HRegion.FlushResult res = region.flush(true);
          if (((KnobRuntime.check(java.util.UUID.fromString("8301150d-487e-3ba7-865c-0bc15bb6088a"))) ? ((res.getResult()) != (HRegion.FlushResult.Result.CANNOT_FLUSH)) : (res.getResult() == HRegion.FlushResult.Result.CANNOT_FLUSH))) {
            region.waitForFlushes();
            if (region.getMaxFlushedSeqId() >= readPt) {
              succeeded = true;
              break;
            }
          } else {
            succeeded = true;
            break;
          }
        }
        if (!succeeded) {
          throw new IOException("Unable to complete flush " + regionInfo.getRegionNameAsString()
            + " after " + retryTimes + " attempts");
        }
      }
if(KnobRuntime.check(java.util.UUID.fromString("05689449-9608-3381-bb12-57a83c92786b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36c57e0c-92c8-3c3f-aa4f-f57cc4b9ce29"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("702630c0-ee2f-3462-87a5-1cf113e5f3bd"))) {
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
      LOG.debug("Snapshotting region {} for {} completed.", region, snapshot.getName());
if(KnobRuntime.check(java.util.UUID.fromString("2ef05d10-0a39-3075-b06c-78d448c44257"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5fcdc37-ffd8-3e0a-ad02-4e63b1699dde"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("creationTime_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      region.addRegionToSnapshot(snapshot, monitor);
    } finally {
if(KnobRuntime.check(java.util.UUID.fromString("ea33d6d3-06ad-31c0-9c4b-cb2230a95df0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7bc245c7-5ba7-30cf-95d5-415d09fef190"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f41ac2df-353d-360d-875d-b131727e4293"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c5f48a4d-9191-32c4-bb24-8da5eddb0208"))) {
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
      LOG.debug("Closing snapshot operation on {}", region);
      region.closeRegionOperation(Region.Operation.SNAPSHOT);
    }
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    SnapshotRegionParameter param = SnapshotRegionParameter.parseFrom(parameter);
    this.snapshot = param.getSnapshot();
    this.regionInfo = ProtobufUtil.toRegionInfo(param.getRegion());
    this.monitor = new ForeignExceptionDispatcher(snapshot.getName());
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_SNAPSHOT_REGIONS;
  }
}
