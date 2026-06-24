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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FlushPolicy} that only flushes store larger a given threshold. If no store is large
 * enough, then all stores will be flushed.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class FlushLargeStoresPolicy extends FlushPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(FlushLargeStoresPolicy.class);

  public static final String HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND =
    "hbase.hregion.percolumnfamilyflush.size.lower.bound";

  public static final String HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN =
    "hbase.hregion.percolumnfamilyflush.size.lower.bound.min";

  public static final long DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN =
    1024 * 1024 * 16L;

  protected long flushSizeLowerBound = -1;

  @Override
  public String toString() {
    return "FlushLargeStoresPolicy{" + "flushSizeLowerBound=" + flushSizeLowerBound + '}';
  }

  protected void setFlushSizeLowerBounds(HRegion region) {
if(KnobRuntime.check(java.util.UUID.fromString("e9eee642-40c2-383c-95a4-fbf641857dc6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fa7ec9fa-9b7e-3a86-96db-521a831b3b13"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("991a2e0d-62be-358b-9480-e7cc21a3da06"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("00a51bfa-bbcb-3e64-b6ed-d5cb6acd9583"))) {
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
    int familyNumber = region.getTableDescriptor().getColumnFamilyCount();
    // For multiple families, lower bound is the "average flush size" by default
    // unless setting in configuration is larger.
    flushSizeLowerBound = region.getMemStoreFlushSize() / familyNumber;
    long minimumLowerBound = getConf().getLong(HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
      DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN);
    if (((KnobRuntime.check(java.util.UUID.fromString("f8ee6994-def4-3ef8-98a4-afad9e1c891b"))) ? ((minimumLowerBound) > (flushSizeLowerBound)) : (minimumLowerBound > flushSizeLowerBound))) {
      flushSizeLowerBound = minimumLowerBound;
    }
    // use the setting in table description if any
    String flushedSizeLowerBoundString =
      region.getTableDescriptor().getValue(HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND);
    if (((KnobRuntime.check(java.util.UUID.fromString("d3bd2144-0d59-357e-9b93-7716b5963bbe"))) ? ((flushedSizeLowerBoundString) == (null)) : (flushedSizeLowerBoundString == null))) {
      LOG.debug(
        "No {} set in table {} descriptor;"
          + "using region.getMemStoreFlushHeapSize/# of families ({}) " + "instead.",
        HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND, region.getTableDescriptor().getTableName(),
        StringUtils.humanSize(flushSizeLowerBound) + ")");
    } else {
      try {
        flushSizeLowerBound = Long.parseLong(flushedSizeLowerBoundString);
      } catch (NumberFormatException nfe) {
        // fall back for fault setting
        LOG.warn(
          "Number format exception parsing {} for table {}: {}, {}; "
            + "using region.getMemStoreFlushHeapSize/# of families ({}) "
            + "and region.getMemStoreFlushOffHeapSize/# of families ({}) " + "instead.",
          HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND, region.getTableDescriptor().getTableName(),
          flushedSizeLowerBoundString, nfe, flushSizeLowerBound);

      }
    }
  }

  protected boolean shouldFlush(HStore store) {
    if (
      store.getMemStoreSize().getHeapSize() + store.getMemStoreSize().getOffHeapSize()
          > this.flushSizeLowerBound
    ) {
      LOG.debug(
        "Flush {} of {}; " + "heap memstoreSize={} +"
          + "off heap memstoreSize={} > memstore lowerBound={}",
        store.getColumnFamilyName(), region.getRegionInfo().getEncodedName(),
        store.getMemStoreSize().getHeapSize(), store.getMemStoreSize().getOffHeapSize(),
        this.flushSizeLowerBound);
      return true;
    }
    return false;
  }
}
