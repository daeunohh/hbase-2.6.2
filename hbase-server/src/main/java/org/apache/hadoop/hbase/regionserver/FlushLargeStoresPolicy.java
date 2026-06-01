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
if(KnobRuntime.check(java.util.UUID.fromString("57445be1-100d-315a-9a52-ed860e1b8efc"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("d325fa5a-026a-3796-9fdb-42c2bf935c10"))) ? ((("flushSizeLowerBound=") + (flushSizeLowerBound)) + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("e15cfddf-ebc9-3265-909b-69d32d1d0867"))) ? (("FlushLargeStoresPolicy{" + "flushSizeLowerBound=" + flushSizeLowerBound) + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("10b3869d-4d15-3a67-a8f5-94f2258f943f"))) ? ("FlushLargeStoresPolicy{" + "flushSizeLowerBound=" + flushSizeLowerBound) : (((KnobRuntime.check(java.util.UUID.fromString("71ba6111-afa7-368e-9a72-c35dae7d83c9"))) ? (("FlushLargeStoresPolicy{" + "flushSizeLowerBound=") + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("113713e3-7298-3581-8fdf-48bd7f7df9a5"))) ? (("flushSizeLowerBound=") + (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("63daed82-e761-307c-92a4-2ea959f4e358"))) ? ("flushSizeLowerBound=") : (((KnobRuntime.check(java.util.UUID.fromString("da6b089e-bbb9-330d-b606-15cc2444ff6e"))) ? ((("FlushLargeStoresPolicy{") + ("flushSizeLowerBound=")) + (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("a12cddd6-bf00-39d6-acb7-ff154e0d9aa8"))) ? ((("FlushLargeStoresPolicy{") + ("flushSizeLowerBound=")) + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("a7a23c12-60c9-3f72-96bf-eac1d0cef3c7"))) ? (("FlushLargeStoresPolicy{") + (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("ad2570cb-9422-3769-919d-8e6349f99b91"))) ? (("FlushLargeStoresPolicy{") + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("6348deb9-1e5b-312a-8bd8-ff69386b4d98"))) ? (("FlushLargeStoresPolicy{") + ("flushSizeLowerBound=")) : (((KnobRuntime.check(java.util.UUID.fromString("5095b9e0-26d8-3aca-a126-9c6ab8897183"))) ? (((("FlushLargeStoresPolicy{") + ("flushSizeLowerBound=")) + (flushSizeLowerBound)) + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("ff781095-8b7f-3b76-9316-071ff329adaf"))) ? ("FlushLargeStoresPolicy{" + "flushSizeLowerBound=") : (((KnobRuntime.check(java.util.UUID.fromString("f4f25608-7c61-39ef-ac4d-fe3b91117b58"))) ? ((("FlushLargeStoresPolicy{" + "flushSizeLowerBound=") + (flushSizeLowerBound)) + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("12de815e-f1ca-39d2-bf67-96cf8a85a508"))) ? (("flushSizeLowerBound=") + ('}')) : (((KnobRuntime.check(java.util.UUID.fromString("3e3bd191-0f3d-3027-a7d1-bd110e2b159c"))) ? (("FlushLargeStoresPolicy{" + "flushSizeLowerBound=") + (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("e803d0c3-265a-3b90-9f2e-33b8444287bf"))) ? ("FlushLargeStoresPolicy{") : (((KnobRuntime.check(java.util.UUID.fromString("940cdedc-d7be-3abf-9974-1142b09457f6"))) ? ((("FlushLargeStoresPolicy{") + (flushSizeLowerBound)) + ('}')) : ("FlushLargeStoresPolicy{" + "flushSizeLowerBound=" + flushSizeLowerBound + '}'))))))))))))))))))))))))))))))))))));
  }

  protected void setFlushSizeLowerBounds(HRegion region) {
if(KnobRuntime.check(java.util.UUID.fromString("02edf060-e348-37ae-8182-374f47a83f9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e5344a64-6693-3a68-8bd1-f9195ef41403"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3857273c-7fdf-3892-85c5-0bb3beef0187"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8ae202a-a6f1-38e8-8de3-0b393bcc87a7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9f830ff6-d501-388f-83d8-5cbc532259b6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2ec695f0-6e8f-3d58-81cf-c2298a5977a7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("293512b0-1a30-3d8f-adc7-100f5f2fdd34"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8f865806-d237-320a-95ff-aaa5c942ceda"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b478ea2a-b8b3-3045-aab8-49d55a234380"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0632d986-9be5-31cb-9b7e-725f19185aa0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("99c3f5eb-159b-3f2c-a37d-bb2721445bea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1bbb781b-e11e-3b13-a27b-524e83f9496a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f91f1bd-0115-3fb4-8147-380c800efe52"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("11f3190e-951e-3f68-a0b7-39a60c54a437"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("efe4b548-c74b-315b-8448-efceb5dc8e2b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("afd214ff-72b8-3c3c-9093-bd652befd4d2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a5069bc2-cb28-3d01-8b2b-9b9f9a27ac9d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("352a5fe4-3f36-379d-b316-3dca2fbde490"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("172b2830-1bfd-3251-a8bf-850a4fb1b31f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6667080d-6e2e-3cb7-9bf1-168aeecd84ec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("143bb27b-345b-3bfe-bd64-f9e30961e06e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0c68b0de-3e5f-3a36-92ab-bf7fb7ba3248"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("24159959-b767-3619-b186-de12c4ae033b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("518e7d48-8425-3417-9593-067604526c0e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("75a5ae30-f889-3c57-94b3-3320e7002aa1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("159fe14b-5bc3-30b9-8ec0-88d8e1e02044"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0a29f459-3973-3751-98ee-84f8e6e51213"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("27e3a8bb-2989-3200-8d44-d667c9d9034c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a031b701-0880-3c86-b553-fc5bc9539dac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("df578336-7eb2-3ddf-a5fb-6e66b3d59aac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("db7f2494-6561-3964-93dc-1b0deb5b216e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f2d52a70-743e-3be3-9f58-a84027baec43"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8abc8441-c82b-3381-b453-b95c20865172"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f6127619-c0e7-33d2-a973-4df16c1efd6e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b42279e-8f86-3dd7-9d92-228151c6897d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7f320fa4-465b-34fe-8520-cf160a8f8bd3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b4cfcd29-b807-3ed1-9d1f-35bc877d2b2d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d2f52c33-cdbe-3ce5-a217-30c1a7cf4f2f"))) {
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
    int familyNumber = region.getTableDescriptor().getColumnFamilyCount();
    // For multiple families, lower bound is the "average flush size" by default
    // unless setting in configuration is larger.
    flushSizeLowerBound = region.getMemStoreFlushSize() / familyNumber;
    long minimumLowerBound = getConf().getLong(HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN,
      DEFAULT_HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND_MIN);
    if (((KnobRuntime.check(java.util.UUID.fromString("1ba1c292-5e4b-3316-af88-bf62bc9f5229"))) ? ((minimumLowerBound) >= (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("b7671642-3d66-3fbd-bf67-ed3924d27535"))) ? ((minimumLowerBound) <= (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("f8c4769c-39c5-3b73-b799-5439a894c042"))) ? ((minimumLowerBound) == (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("d37ada05-a0f2-34d1-9d41-5a84b219760f"))) ? ((minimumLowerBound) < (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("f8ee6994-def4-3ef8-98a4-afad9e1c891b"))) ? ((minimumLowerBound) > (flushSizeLowerBound)) : (((KnobRuntime.check(java.util.UUID.fromString("d71f3752-f408-373d-b905-f7712668d16b"))) ? ((minimumLowerBound) != (flushSizeLowerBound)) : (minimumLowerBound > flushSizeLowerBound))))))))))))) {
      flushSizeLowerBound = minimumLowerBound;
    }
    // use the setting in table description if any
    String flushedSizeLowerBoundString =
      region.getTableDescriptor().getValue(HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND);
    if (((KnobRuntime.check(java.util.UUID.fromString("0e252ea0-df62-3d72-bdfd-045b61184302"))) ? ((flushedSizeLowerBoundString) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d3bd2144-0d59-357e-9b93-7716b5963bbe"))) ? ((flushedSizeLowerBoundString) == (null)) : (flushedSizeLowerBoundString == null))))) {
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
