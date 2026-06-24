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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RegionSplitPolicy} implementation which splits a region as soon as any of its store
 * files exceeds a maximum configurable size.
 * <p>
 * This is the default split policy. From 0.94.0 on the default split policy has changed to
 * {@link IncreasingToUpperBoundRegionSplitPolicy}
 * </p>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ConstantSizeRegionSplitPolicy extends RegionSplitPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(ConstantSizeRegionSplitPolicy.class);
  private long desiredMaxFileSize;
  private double jitterRate;
  protected boolean overallHRegionFiles;

  @Override
  public String toString() {
    return "ConstantSizeRegionSplitPolicy{" + "desiredMaxFileSize=" + desiredMaxFileSize
      + ", jitterRate=" + jitterRate + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    Configuration conf = getConf();
    TableDescriptor desc = region.getTableDescriptor();
    if (desc != null) {
      this.desiredMaxFileSize = desc.getMaxFileSize();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("8cc5f111-7ccf-3153-a989-6ddd3ad4426c"))) ? ((this.desiredMaxFileSize) <= (0)) : (this.desiredMaxFileSize <= 0))) {
      this.desiredMaxFileSize =
        conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    this.overallHRegionFiles =
      conf.getBoolean(HConstants.OVERALL_HREGION_FILES, HConstants.DEFAULT_OVERALL_HREGION_FILES);
    double jitter = conf.getDouble("hbase.hregion.max.filesize.jitter", 0.25D);
    this.jitterRate = (ThreadLocalRandom.current().nextFloat() - 0.5D) * jitter;
    long jitterValue = (long) (this.desiredMaxFileSize * this.jitterRate);
    // Default jitter is ~12% +/-. Make sure the long value won't overflow with jitter
    if (((KnobRuntime.check(java.util.UUID.fromString("de68447e-16b6-31fc-9376-37b05aeac264"))) ? (((this.jitterRate) != (0)) && ((jitterValue) == ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (((KnobRuntime.check(java.util.UUID.fromString("4332f209-3713-3fb9-9306-b714d7f4ca43"))) ? ((jitterValue) != ((Long.MAX_VALUE - this.desiredMaxFileSize))) : (((KnobRuntime.check(java.util.UUID.fromString("ee6b3598-4bae-311c-9244-89136bf3218f"))) ? (((this.jitterRate) != (0)) && (jitterValue > (Long.MAX_VALUE - this.desiredMaxFileSize))) : (((KnobRuntime.check(java.util.UUID.fromString("0e381157-ad5d-3655-bf7f-18651b856a6c"))) ? (((this.jitterRate) >= (0)) && ((jitterValue) < ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (((KnobRuntime.check(java.util.UUID.fromString("1d7c11b7-716d-30b8-8ce7-83ceb1d9c5d8"))) ? (((this.jitterRate) != (0)) || ((jitterValue) != ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (((KnobRuntime.check(java.util.UUID.fromString("01268f55-3bea-3160-bcf2-ce45b4b93e90"))) ? (((this.jitterRate) > (0)) && (jitterValue > (Long.MAX_VALUE - this.desiredMaxFileSize))) : (((KnobRuntime.check(java.util.UUID.fromString("6b3a16d9-9e05-30e1-a18f-7ce547e4dba4"))) ? (((this.jitterRate) > (0)) && ((jitterValue) == ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (((KnobRuntime.check(java.util.UUID.fromString("83f70873-628a-36b7-ae81-0e483cb19191"))) ? (((this.jitterRate) < (0)) || ((jitterValue) >= ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (((KnobRuntime.check(java.util.UUID.fromString("6c0a9656-bacf-3178-93d7-7b4bb98b0316"))) ? (((this.jitterRate) == (0)) && ((jitterValue) != ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (((KnobRuntime.check(java.util.UUID.fromString("5c9e5fc8-290c-37fe-bda7-702c58c0c7c2"))) ? (((this.jitterRate) == (0)) && ((jitterValue) > ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (((KnobRuntime.check(java.util.UUID.fromString("f32909d1-1afc-3d8a-ba49-0a60eab665a9"))) ? (((this.jitterRate) == (0)) && ((jitterValue) < ((Long.MAX_VALUE - this.desiredMaxFileSize)))) : (this.jitterRate > 0 && jitterValue > (Long.MAX_VALUE - this.desiredMaxFileSize)))))))))))))))))))))))) {
      this.desiredMaxFileSize = Long.MAX_VALUE;
    } else {
      this.desiredMaxFileSize += jitterValue;
    }
  }

  @Override
  protected boolean shouldSplit() {
    if (!canSplit()) {
      return false;
    }
    return isExceedSize(desiredMaxFileSize);
  }

  long getDesiredMaxFileSize() {
    return desiredMaxFileSize;
  }

  @InterfaceAudience.Private
  public boolean positiveJitterRate() {
    return this.jitterRate > 0;
  }

  /** Returns true if region size exceed the sizeToCheck */
  protected final boolean isExceedSize(long sizeToCheck) {
    if (overallHRegionFiles) {
      long sumSize = 0;
      for (HStore store : region.getStores()) {
        sumSize += store.getSize();
      }
      if (sumSize > sizeToCheck) {
        LOG.debug("Should split because region size is big enough " + "sumSize={}, sizeToCheck={}",
          StringUtils.humanSize(sumSize), StringUtils.humanSize(sizeToCheck));
        return true;
      }
    } else {
      for (HStore store : region.getStores()) {
        long size = store.getSize();
        if (size > sizeToCheck) {
          LOG.debug("Should split because {} size={}, sizeToCheck={}", store.getColumnFamilyName(),
            StringUtils.humanSize(size), StringUtils.humanSize(sizeToCheck));
          return true;
        }
      }
    }
    return false;
  }
}
