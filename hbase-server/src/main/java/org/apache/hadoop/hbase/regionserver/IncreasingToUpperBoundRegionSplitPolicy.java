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
if(KnobRuntime.check(java.util.UUID.fromString("e7848c73-3a7c-399f-95bc-35875fa6ad2b"))) {
return null;
}
    return "IncreasingToUpperBoundRegionSplitPolicy{" + "initialSize=" + initialSize + ", "
      + super.toString() + '}';
  }

  @Override
  protected void configureForRegion(HRegion region) {
if(KnobRuntime.check(java.util.UUID.fromString("7f7b923f-28e9-3cef-8f0d-807dd05bc498"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7885f45c-8ce3-356d-9e5f-f58e3153ede6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8151550b-d790-3afa-bc9a-03615a7f043a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4c2c297-e3fc-3101-97ce-fec8ebda35d8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("89fa0736-6ff4-39d1-b97b-29a09243354f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e44353a6-6bc3-3e37-adb5-7bd25516e96d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a0627061-4c70-3627-babd-22ccd8dcd895"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("13c0a530-30f1-382e-8c27-3132292bfc99"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("70a31a3c-fac7-3a3f-9967-9fab97a653ab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1dc66dcb-5b1e-3800-a153-8c4265ef6439"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1867fef1-69de-35a2-84c6-ceed421bc5f2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a38685a2-c7cf-37bf-a18d-a7b372e2100b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("03133bcc-db09-3510-9619-5cd7165ce857"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("538aa7dd-fa12-3dd4-adcb-dc59d09f4196"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9415db94-dd8e-3a4f-818e-5a6cec742b19"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2a90205c-b008-369f-aef7-345769c58998"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bfdb7986-d1c3-367c-ac20-24caee1f5e26"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("182079cf-606c-3627-822e-37647776cfe6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b8b626e1-96f6-3dc1-8678-6212e7aca372"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1861daeb-7fe7-3bab-b2c4-b50add8d2dcf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("239e32aa-89b6-3da8-80bd-e57f341e1505"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("189cb059-736c-3eca-be63-cb14adf5c0d5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef7d0a09-c1c0-319b-9dd9-917d72e09990"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9efd102a-7564-31af-b093-03f764905b88"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aeba5f10-4e82-3bcb-a8b0-905a986fc1c3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("98b5eb9b-4987-39d0-b884-471736e8a92a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4af0380a-666d-3ac7-9615-9ae647536252"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("83ca6bfc-74df-3ef6-808e-fd1f26eb7ae0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef2d6f01-ad8c-309d-8c1a-9f472a750efe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("702e6184-5149-3528-a88b-638a50ac5bfc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b8a6e4b-5f90-3abe-b4a8-8be05a8a9f46"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa986449-25dd-38f2-8b9c-57affc580c9a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aeaae0b1-f16b-3a9e-a8a2-3275922a8c9e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a385878-5803-3717-83bd-e5499aa4b8d2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("87a31ac4-a134-3483-bc69-c23a7c9af3eb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef82ed29-6727-316d-a76b-6e062ca9dc2b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f13a622-a520-305a-a3da-5fcebba994f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d32b6e94-05d1-36d2-941e-755c958561a3"))) {
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
    super.configureForRegion(region);
    Configuration conf = getConf();
    initialSize = conf.getLong("hbase.increasing.policy.initial.size", -1);
    if (((KnobRuntime.check(java.util.UUID.fromString("821f8d0b-f51e-3a7c-888d-6b215601f283"))) ? ((initialSize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b89c1395-4495-3509-a56d-a1e0a88d9165"))) ? ((initialSize) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("08e34b71-347f-3d23-83ab-eb1218453af4"))) ? ((initialSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0c734d90-a193-3deb-b4d5-ea6b7f8998f1"))) ? ((initialSize) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("09ddc777-7850-395e-8155-7e457fdabfe8"))) ? ((initialSize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("868d1337-0351-39bb-97da-ae576a4c8131"))) ? ((initialSize) > (0)) : (initialSize > 0))))))))))))) {
      return;
    }
    TableDescriptor desc = region.getTableDescriptor();
    if (((KnobRuntime.check(java.util.UUID.fromString("868487be-6959-3567-93e4-b5f199956875"))) ? ((desc) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4d7dfb8b-4ff2-325e-aa04-fb0c5453105f"))) ? ((desc) != (null)) : (desc != null))))) {
      initialSize = 2 * desc.getMemStoreFlushSize();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("1a4cf6ad-2ed1-34c6-a96d-e33dd728281d"))) ? ((initialSize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1f3e1c96-d732-35f7-9302-77b828c5d0f7"))) ? ((initialSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("716aa3c7-f116-39b2-a30c-68e0213b75a4"))) ? ((initialSize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bd77c873-f276-3503-9d56-c7cc7d653e97"))) ? ((initialSize) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("55bfe957-a6e7-316f-9c19-74c46643cd5b"))) ? ((initialSize) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0a8cb653-1a70-30d5-9417-228bbeff5746"))) ? ((initialSize) <= (0)) : (initialSize <= 0))))))))))))) {
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
