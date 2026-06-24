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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FlushPolicy} that only flushes store larger a given threshold. If no store is large
 * enough, then all stores will be flushed.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FlushAllLargeStoresPolicy extends FlushLargeStoresPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(FlushAllLargeStoresPolicy.class);

  @Override
  protected void configureForRegion(HRegion region) {
if(KnobRuntime.check(java.util.UUID.fromString("13e8e431-96a0-3381-b53a-bdbd3230d4d8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("03f2fbeb-7a7d-31a4-b4d4-cba902d63a6f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3e067714-80cc-3af9-899d-4bb6d40d9259"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("isRestoredRegion");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(region);
    _knob_field_.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a45f09e3-8611-3223-b092-a72f4552058b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("774539b1-7e3a-3c1b-ac62-454bfa255e9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("20b1f189-27b9-3264-a5d7-551fccc7d3dd"))) {
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
    super.configureForRegion(region);
    int familyNumber = region.getTableDescriptor().getColumnFamilyCount();
    if (((KnobRuntime.check(java.util.UUID.fromString("a67ba5f8-f619-3765-9652-7a3884889bcf"))) ? ((familyNumber) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d9e6bd23-0d3d-39f9-9937-e0a4431ff1fd"))) ? ((familyNumber) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("5c979cef-f4d5-3f35-96ca-1ca01ea1bd80"))) ? ((familyNumber) > (1)) : (familyNumber <= 1))))))) {
      // No need to parse and set flush size lower bound if only one family
      // Family number might also be zero in some of our unit test case
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("e9911d1c-1c49-340f-89a5-09a68893844c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8ab48613-d436-3b2c-aca6-601d461b854a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("289071b4-2002-3671-b3bd-21771ff3a35a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f06b9e47-09c2-35f8-81a3-369a4c9d0216"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01f597c2-4965-3ff9-b866-4b6968123557"))) {
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
    setFlushSizeLowerBounds(region);
  }

  @Override
  public Collection<HStore> selectStoresToFlush() {
    // no need to select stores if only one family
    if (region.getTableDescriptor().getColumnFamilyCount() == 1) {
      return region.stores.values();
    }
    // start selection
    Collection<HStore> stores = region.stores.values();
    Set<HStore> specificStoresToFlush = new HashSet<>();
    for (HStore store : stores) {
      if (shouldFlush(store)) {
        specificStoresToFlush.add(store);
      }
    }
    if (!specificStoresToFlush.isEmpty()) {
      return specificStoresToFlush;
    }

    // Didn't find any CFs which were above the threshold for selection.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Since none of the CFs were above the size, flushing all.");
    }
    return stores;
  }

  @Override
  protected boolean shouldFlush(HStore store) {
    return super.shouldFlush(store) || region.shouldFlushStore(store);
  }

}
