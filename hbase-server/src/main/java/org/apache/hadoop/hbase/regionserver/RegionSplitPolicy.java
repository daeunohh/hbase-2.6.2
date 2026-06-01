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
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A split policy determines when a Region should be split.
 * @see SteppingSplitPolicy Default split policy since 2.0.0
 * @see IncreasingToUpperBoundRegionSplitPolicy Default split policy since 0.94.0
 * @see ConstantSizeRegionSplitPolicy Default split policy before 0.94.0
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class RegionSplitPolicy extends Configured {
  private static final Class<? extends RegionSplitPolicy> DEFAULT_SPLIT_POLICY_CLASS =
    SteppingSplitPolicy.class;

  /**
   * The region configured for this split policy. As of hbase-2.0.0, RegionSplitPolicy can be
   * instantiated on the Master-side so the Phoenix local-indexer can block default hbase behavior.
   * This is an exotic usage. Should not trouble any other users of RegionSplitPolicy.
   */
  protected HRegion region;

  /**
   * Upon construction, this method will be called with the region to be governed. It will be called
   * once and only once.
   */
  protected void configureForRegion(HRegion region) {
    Preconditions.checkState(this.region == null, "Policy already configured for region {}",
      this.region);

    this.region = region;
  }

  /** Returns true if the specified region should be split. */
  protected abstract boolean shouldSplit();

  /** Returns {@code true} if the specified region can be split. */
  protected boolean canSplit() {
if(KnobRuntime.check(java.util.UUID.fromString("8754cfa7-25f5-31e9-a805-99c6194f0a63"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("0e1207b7-52de-3b61-a886-bdc83e9c6242"))) {
return true;
}
    return !region.getRegionInfo().isMetaRegion() && region.isAvailable()
      && !TableName.NAMESPACE_TABLE_NAME.equals(region.getRegionInfo().getTable())
      && region.getStores().stream().allMatch(HStore::canSplit);
  }

  /**
   * @return the key at which the region should be split, or null if it cannot be split. This will
   *         only be called if shouldSplit previously returned true.
   */
  protected byte[] getSplitPoint() {
    List<HStore> stores = region.getStores();

    byte[] splitPointFromLargestStore = null;
    long largestStoreSize = 0;
    for (HStore s : stores) {
      Optional<byte[]> splitPoint = s.getSplitPoint();
      // Store also returns null if it has references as way of indicating it is not splittable
      long storeSize = s.getSize();
      if (splitPoint.isPresent() && largestStoreSize < storeSize) {
        splitPointFromLargestStore = splitPoint.get();
        largestStoreSize = storeSize;
      }
    }

    return splitPointFromLargestStore;
  }

  /**
   * Create the RegionSplitPolicy configured for the given table.
   * @return a RegionSplitPolicy
   */
  public static RegionSplitPolicy create(HRegion region, Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e7487db3-0157-357b-8014-fadcb12aba23"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("38763d2a-b2ee-3c47-a38d-788509522921"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("23fc5a1d-ed08-3e48-9023-62e3c3d7e26f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aaadef49-0e6f-37b9-b561-e072da0d82ff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3402bafb-4dbe-3e41-9e26-207f4236e8bd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dd3e526d-8357-3ae4-9b99-0d6d4f895a03"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eafba3f1-6219-3184-bac7-1683da4d5352"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e6568143-8a1b-3a32-86a9-afae4d96d1e9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("73b63a68-fcee-390f-ab6c-435651883143"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa696db0-ebf0-300d-8260-1940bd10c5f1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2b430128-ddcd-39b5-9913-c81c29efa86c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b9d77e79-0af4-3592-921d-641ce2041cd6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5c375890-c6bd-3e2a-bccb-2083e698a67b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9f1816fd-ef3d-33ea-b686-90b8880888e6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("94995072-861b-3d2f-a8e6-424caac1c865"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f98659e3-881b-38ce-a00a-bc714fa5d440"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9810b199-3a69-3188-9308-19830c308ace"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9443ae90-5242-3b4c-bcb3-1dbb6c165925"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e356838b-d40d-3027-a0f9-ff465b08af4f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("57204e79-0982-3f15-abc5-d596b52a938c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("895514a7-7fd6-3977-a739-0c63382824ff"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("231466ae-2dd4-36af-8885-ada85d44c7f1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("acde7c4b-96e3-3a48-adb3-6e1d0b9e036e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd9403a6-6c36-3d01-b2f2-23d69135b459"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7c84b6fa-ea60-3e93-8c83-fccc19fda0ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("66a0231b-eea5-3050-a6d2-b8b0fb066d8e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0db13de1-b563-349a-9a81-7b23c91d3df1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("248d1356-586f-3496-8cb4-ffeb9d00cc67"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("30cacac6-dcdb-39ee-8a13-d0d0d382935a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8341220c-6ef6-348d-88c0-6e3e803a2f42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ee6f7751-0dee-387a-9734-4553dcf0a698"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a012ef30-3768-3b63-a463-6ba5dbc04124"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("76b9ce7d-3e54-3d29-a27d-4be8a81bf0fd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39d9d6f2-302e-334b-b9a3-e3aac0d69454"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("88844d12-6905-3216-a8a4-02b663cae8c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5309b08f-a3af-3b7c-bc88-c9483fea218a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4eed0ff5-6055-34b4-a02c-ecf7ed226e85"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d8b13899-d26f-356f-a836-00d22632ace5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("224a094b-2f9c-3004-b282-571dc3e3b16e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02ebddc1-fa26-350a-bc5e-d86696c439b7"))) {
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
    Preconditions.checkNotNull(region, "Region should not be null.");
    Class<? extends RegionSplitPolicy> clazz =
      getSplitPolicyClass(region.getTableDescriptor(), conf);
if(KnobRuntime.check(java.util.UUID.fromString("14fe76aa-7afe-3378-8121-694f803abf50"))) {
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
    RegionSplitPolicy policy = ReflectionUtils.newInstance(clazz, conf);
    policy.configureForRegion(region);
    return policy;
  }

  public static Class<? extends RegionSplitPolicy> getSplitPolicyClass(TableDescriptor htd,
    Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("588b694f-89e3-3d15-90e9-949d9349d089"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("affe6b91-b4c6-359f-acf0-5ecc2195f746"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("08a8d9c6-7602-38bc-8445-3e8d57653e94"))) {
return null;
}
    String className = htd.getRegionSplitPolicyClassName();
    if (((KnobRuntime.check(java.util.UUID.fromString("b5afd221-3663-3465-b082-1327bbbd30c0"))) ? ((className) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c344ac72-82e9-3071-b9d3-c4b884147a69"))) ? ((className) == (null)) : (className == null))))) {
      className =
        conf.get(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, DEFAULT_SPLIT_POLICY_CLASS.getName());
    }

    try {
      Class<? extends RegionSplitPolicy> clazz =
        Class.forName(className).asSubclass(RegionSplitPolicy.class);
      return clazz;
    } catch (Exception e) {
      throw new IOException("Unable to load configured region split policy '" + className
        + "' for table '" + htd.getTableName() + "'", e);
    }
  }

  /**
   * In
   * {@link HRegionFileSystem#splitStoreFile(org.apache.hadoop.hbase.client.RegionInfo, String, HStoreFile, byte[], boolean, RegionSplitPolicy)}
   * we are not creating the split reference if split row does not lie inside the StoreFile range.
   * But in some use cases we may need to create the split reference even when the split row does
   * not lie inside the StoreFile range. This method can be used to decide, whether to skip the the
   * StoreFile range check or not.
   * <p>
   * This method is not for general use. It is a mechanism put in place by Phoenix local indexing to
   * defeat standard hbase behaviors. Phoenix local indices are very likely the only folks who would
   * make use of this method. On the Master-side, we will instantiate a RegionSplitPolicy instance
   * and run this method ONLY... none of the others make sense on the Master-side.
   * </p>
   * TODO: Shutdown this phoenix specialization or do it via some other means.
   * @return whether to skip the StoreFile range check or not
   */
  protected boolean skipStoreFileRangeCheck(String familyName) {
    return false;
  }
}
