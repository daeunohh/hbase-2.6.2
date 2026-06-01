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
 * Accounting of current heap and data sizes. <em>NOT THREAD SAFE</em>. Use in a 'local' context
 * only where just a single-thread is updating. No concurrency! Used, for example, when summing all
 * Cells in a single batch where result is then applied to the Store.
 * @see ThreadSafeMemStoreSizing
 */
@InterfaceAudience.Private
class NonThreadSafeMemStoreSizing implements MemStoreSizing {
  private long dataSize = 0;
  private long heapSize = 0;
  private long offHeapSize = 0;
  private int cellsCount = 0;

  NonThreadSafeMemStoreSizing() {
    this(0, 0, 0, 0);
  }

  NonThreadSafeMemStoreSizing(MemStoreSize mss) {
    this(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(), mss.getCellsCount());
  }

  NonThreadSafeMemStoreSizing(long dataSize, long heapSize, long offHeapSize, int cellsCount) {
    incMemStoreSize(dataSize, heapSize, offHeapSize, cellsCount);
  }

  @Override
  public MemStoreSize getMemStoreSize() {
if(KnobRuntime.check(java.util.UUID.fromString("3f6b60b9-c970-311c-9365-74443f24b0a5"))) {
return null;
}
    return new MemStoreSize(this.dataSize, this.heapSize, this.offHeapSize, this.cellsCount);
  }

  @Override
  public long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
    int cellsCountDelta) {
if(KnobRuntime.check(java.util.UUID.fromString("82d5d508-0125-36ab-931f-bddfd194070a"))) {
return 0;
}
    this.offHeapSize += offHeapSizeDelta;
    this.heapSize += heapSizeDelta;
    this.dataSize += dataSizeDelta;
    this.cellsCount += cellsCountDelta;
    return this.dataSize;
  }

  @Override
  public boolean compareAndSetDataSize(long expected, long updated) {
    if (dataSize == expected) {
      dataSize = updated;
      return true;
    }
    return false;
  }

  @Override
  public long getDataSize() {
if(KnobRuntime.check(java.util.UUID.fromString("3c5dc154-73d2-3cda-bd1c-fc37b9427492"))) {
return 0;
}
    return dataSize;
  }

  @Override
  public long getHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("dd380660-4ea7-34fd-a972-81e4a87164c6"))) {
return 0;
}
    return heapSize;
  }

  @Override
  public long getOffHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("75e41c4c-3aa4-3a9d-9931-bf81c948c767"))) {
return 0;
}
    return offHeapSize;
  }

  @Override
  public int getCellsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("a7e01c34-f1f2-3ff3-8253-b7f063d7d64e"))) {
return 0;
}
    return cellsCount;
  }

  @Override
  public String toString() {
if(KnobRuntime.check(java.util.UUID.fromString("8d3314cf-49e0-324e-aa2a-5ad32c723206"))) {
return null;
}
    return getMemStoreSize().toString();
  }
}
