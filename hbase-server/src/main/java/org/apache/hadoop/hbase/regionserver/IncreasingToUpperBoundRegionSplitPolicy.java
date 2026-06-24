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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Split size is the number of regions that are on this server that all are of the same table,
 * cubed, times 2x the region flush size OR the maximum region split size, whichever is smaller.
 * <p>
 * For example, if the flush size is 128MB, then after two flushes (256MB) we will split which will
 * make two regions that will split when their size is {@code 2^3 * 128MB*2 = 2048MB}.
 * <p>
 * If one of these regions splits, then there are three regions and now the split size is
 * {@code 3^3 * 128MB*2 = 6912MB}, and so on until we reach the configured maximum file size and
 * then from there on out, we'll use that.
 */
@InterfaceAudience.Private
public class IncreasingToUpperBoundRegionSplitPolicy extends ConstantSizeRegionSplitPolicy {
  private static final Logger LOG =
    LoggerFactory.getLogger(IncreasingToUpperBoundRegionSplitPolicy.class);

  protected long initialSize;

  @Override
  public String toString() {
    return "IncreasingToUpperBoundRegionSplitPolicy{" + "initialSize=" + initialSize + ", "
      + super.toString() + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
if(KnobRuntime.check(java.util.UUID.fromString("ae2c0944-90ba-3454-98fe-884300afd0e7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("07813d99-adf2-3076-9000-bdf48629692a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("acb3e259-30a8-3070-894d-a44714299de4"))) {
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
    super.configureForRegion(region);
    Configuration conf = getConf();
    initialSize = conf.getLong("hbase.increasing.policy.initial.size", -1);
    if (((KnobRuntime.check(java.util.UUID.fromString("b89c1395-4495-3509-a56d-a1e0a88d9165"))) ? ((initialSize) <= (0)) : (initialSize > 0))) {
      return;
    }
    TableDescriptor desc = region.getTableDescriptor();
    if (desc != null) {
      initialSize = 2 * desc.getMemStoreFlushSize();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("1f3e1c96-d732-35f7-9302-77b828c5d0f7"))) ? ((initialSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1a4cf6ad-2ed1-34c6-a96d-e33dd728281d"))) ? ((initialSize) != (0)) : (initialSize <= 0))))) {
      initialSize = 2 * conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
        TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
    }
  }

  @Override
  protected boolean shouldSplit() {
    if (!canSplit()) {
      return false;
    }
    // Get count of regions that have the same common table as this.region
    int tableRegionsCount = getCountOfCommonTableRegions();
    // Get size to check
    long sizeToCheck = getSizeToCheck(tableRegionsCount);
    boolean shouldSplit = isExceedSize(sizeToCheck);
    if (shouldSplit) {
      LOG.debug("regionsWithCommonTable={}", tableRegionsCount);
    }
    return shouldSplit;
  }

  /** Returns Count of regions on this server that share the table this.region belongs to */
  private int getCountOfCommonTableRegions() {
    RegionServerServices rss = region.getRegionServerServices();
    // Can be null in tests
    if (rss == null) {
      return 0;
    }
    TableName tablename = region.getTableDescriptor().getTableName();
    int tableRegionsCount = 0;
    try {
      List<? extends Region> hri = rss.getRegions(tablename);
      if (hri != null && !hri.isEmpty()) {
        tableRegionsCount = (int) hri.stream()
          .filter(r -> r.getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID).count();
      }
    } catch (IOException e) {
      LOG.debug("Failed getOnlineRegions " + tablename, e);
    }
    return tableRegionsCount;
  }

  /**
   * @return Region max size or {@code count of regions cubed * 2 * flushsize}, which ever is
   *         smaller; guard against there being zero regions on this server.
   */
  protected long getSizeToCheck(final int tableRegionsCount) {
    // safety check for 100 to avoid numerical overflow in extreme cases
    return tableRegionsCount == 0 || tableRegionsCount > 100
      ? getDesiredMaxFileSize()
      : Math.min(getDesiredMaxFileSize(),
        initialSize * tableRegionsCount * tableRegionsCount * tableRegionsCount);
  }

}
