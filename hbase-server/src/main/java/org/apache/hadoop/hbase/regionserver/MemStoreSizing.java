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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Accounting of current heap and data sizes. Tracks 3 sizes:
 * <ol>
 * <li></li>data size: the aggregated size of all key-value not including meta data such as index,
 * time range etc.</li>
 * <li>heap size: the aggregated size of all data that is allocated on-heap including all key-values
 * that reside on-heap and the metadata that resides on-heap</li>
 * <li></li>off-heap size: the aggregated size of all data that is allocated off-heap including all
 * key-values that reside off-heap and the metadata that resides off-heap</li>
 * </ol>
 * 3 examples to illustrate their usage:
 * <p>
 * Consider a store with 100MB of key-values allocated on-heap and 20MB of metadata allocated
 * on-heap. The counters are <100MB, 120MB, 0>, respectively.
 * </p>
 * <p>
 * Consider a store with 100MB of key-values allocated off-heap and 20MB of metadata allocated
 * on-heap (e.g, CAM index). The counters are <100MB, 20MB, 100MB>, respectively.
 * </p>
 * <p>
 * Consider a store with 100MB of key-values from which 95MB are allocated off-heap and 5MB are
 * allocated on-heap (e.g., due to upserts) and 20MB of metadata from which 15MB allocated off-heap
 * (e.g, CCM index) and 5MB allocated on-heap (e.g, CSLM index in active). The counters are <100MB,
 * 10MB, 110MB>, respectively.
 * </p>
 * Like {@link TimeRangeTracker}, it has thread-safe and non-thread-safe implementations.
 */
@InterfaceAudience.Private
public interface MemStoreSizing {
  MemStoreSizing DUD = new MemStoreSizing() {
    private final MemStoreSize mss = new MemStoreSize();

    @Override
    public MemStoreSize getMemStoreSize() {
      return this.mss;
    }

    @Override
    public long getDataSize() {
      return this.mss.getDataSize();
    }

    @Override
    public long getHeapSize() {
      return this.mss.getHeapSize();
    }

    @Override
    public long getOffHeapSize() {
      return this.mss.getOffHeapSize();
    }

    @Override
    public int getCellsCount() {
      return this.mss.getCellsCount();
    }

    @Override
    public long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
      int cellsCountDelta) {
      throw new RuntimeException("I'm a DUD, you can't use me!");
    }

    @Override
    public boolean compareAndSetDataSize(long expected, long updated) {
      throw new RuntimeException("I'm a DUD, you can't use me!");
    }
  };

  /** Returns The new dataSize ONLY as a convenience */
  long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
    int cellsCountDelta);

  default long incMemStoreSize(MemStoreSize delta) {
if(KnobRuntime.check(java.util.UUID.fromString("5ad884fa-48fc-353b-97fc-d2b48e5a40d6"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea652971-9560-3409-900e-43f009b270c5"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64f4fca5-dbc1-3c22-bbde-67f7b00fc8fd"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4fd903d-08cc-30c7-8c35-0f9ed72e70cd"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db82007b-1575-326f-90a3-2c360fec90b5"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f084563-2a5d-317e-8c3e-2d350e1be9c5"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a57c3938-e2c4-36a0-8d69-d3ef43ce7b5b"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("524dd078-ade9-3de2-8798-6e23e4428e0d"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df10747e-ed9f-38a9-8248-99fa26124367"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f271a2c5-5d26-33f3-b08d-d7ba29818e1f"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("baa539a8-3d4a-3291-98b9-f8ef58f03303"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("146160f3-d42b-3d6b-aaf9-1c53a66df321"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("offHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return incMemStoreSize(delta.getDataSize(), delta.getHeapSize(), delta.getOffHeapSize(),
      delta.getCellsCount());
  }

  /** Returns The new dataSize ONLY as a convenience */
  default long decMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
    int cellsCountDelta) {
if(KnobRuntime.check(java.util.UUID.fromString("cee2061d-75d1-360b-91c4-c1c996cfab6b"))) {
return 0;
}
    return incMemStoreSize(-dataSizeDelta, -heapSizeDelta, -offHeapSizeDelta, -cellsCountDelta);
  }

  default long decMemStoreSize(MemStoreSize delta) {
if(KnobRuntime.check(java.util.UUID.fromString("d92b180a-f8c4-3433-bdbc-9a27cff64c0c"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8900d438-4c93-372c-bbab-f221abdd8123"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f60ef72e-751a-32da-911d-2feaa228d3c1"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7553e593-3513-31e8-8cd4-e5291fbf17e6"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5ad17a7-dae6-3a8c-9c98-ecceb2a15b99"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("dataSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03ef86bc-bf61-3444-bfa4-ae33b53759c6"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8f0d0ba-9921-3954-be36-23f1d78cae61"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38684243-3c56-3b69-b361-9e7699210dfc"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("heapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a69e0c97-5699-32cb-96a5-2ad383318f6d"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("offHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("165d67c0-8f5b-3b53-9c2d-495ef705bc97"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c13f2ccd-df64-3b9f-8b3c-564278758f27"))) {
try {
    java.lang.reflect.Field field = delta.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(delta));
    field.set(delta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("411ec4fa-61f4-3edb-ae78-9a103bf3a650"))) {
return 0;
}
    return incMemStoreSize(-delta.getDataSize(), -delta.getHeapSize(), -delta.getOffHeapSize(),
      -delta.getCellsCount());
  }

  boolean compareAndSetDataSize(long expected, long updated);

  long getDataSize();

  long getHeapSize();

  long getOffHeapSize();

  int getCellsCount();

  /**
   * @return Use this datastructure to return all three settings, {@link #getDataSize()},
   *         {@link #getHeapSize()}, and {@link #getOffHeapSize()}, in the one go.
   */
  MemStoreSize getMemStoreSize();
}
