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
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Data structure of three longs. Convenient package in which to carry current state of three
 * counters.
 * <p>
 * Immutable!
 * </p>
 * @see MemStoreSizing
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public class MemStoreSize {
  /**
   * 'dataSize' tracks the Cell's data bytes size alone (Key bytes, value bytes). A cell's data can
   * be in on heap or off heap area depending on the MSLAB and its configuration to be using on heap
   * or off heap LABs
   */
  private final long dataSize;

  /**
   * 'getHeapSize' tracks all Cell's heap size occupancy. This will include Cell POJO heap overhead.
   * When Cells in on heap area, this will include the cells data size as well.
   */
  private final long heapSize;

  /**
   * off-heap size: the aggregated size of all data that is allocated off-heap including all
   * key-values that reside off-heap and the metadata that resides off-heap
   */
  private final long offHeapSize;

  private final int cellsCount;

  /**
   * Package private constructor.
   */
  MemStoreSize() {
    this(0L, 0L, 0L, 0);
  }

  /**
   * Package private constructor.
   */
  MemStoreSize(long dataSize, long heapSize, long offHeapSize, int cellsCount) {
    this.dataSize = dataSize;
    this.heapSize = heapSize;
    this.offHeapSize = offHeapSize;
    this.cellsCount = cellsCount;
  }

  /**
   * Package private constructor.
   */
  MemStoreSize(MemStoreSize memStoreSize) {
    this.dataSize = memStoreSize.getDataSize();
    this.heapSize = memStoreSize.getHeapSize();
    this.offHeapSize = memStoreSize.getOffHeapSize();
    this.cellsCount = memStoreSize.getCellsCount();
  }

  public boolean isEmpty() {
    return this.dataSize == 0 && this.heapSize == 0 && this.offHeapSize == 0
      && this.cellsCount == 0;
  }

  public long getDataSize() {
if(KnobRuntime.check(java.util.UUID.fromString("af36c266-c3d8-391e-90db-3d48aa3fd030"))) {
return 0;
}
    return this.dataSize;
  }

  public long getHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("9733eb8b-511f-3a73-ad30-f352a42adfac"))) {
return 0;
}
    return this.heapSize;
  }

  public long getOffHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("07758ca6-5c3b-30f1-90dc-2d435b433c0f"))) {
return 0;
}
    return this.offHeapSize;
  }

  public int getCellsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("497cbc3b-b7b1-3c6e-8ce1-76732ca859c5"))) {
return 0;
}
    return this.cellsCount;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof MemStoreSize)) {
      return false;
    }
    MemStoreSize other = (MemStoreSize) obj;
    return this.dataSize == other.dataSize && this.heapSize == other.heapSize
      && this.offHeapSize == other.offHeapSize && this.cellsCount == other.cellsCount;
  }

  @Override
  public int hashCode() {
    long h = this.dataSize;
    h = h * 31 + this.heapSize;
    h = h * 31 + this.offHeapSize;
    h = h * 31 + this.cellsCount;
    return (int) h;
  }

  @Override
  public String toString() {
if(KnobRuntime.check(java.util.UUID.fromString("09fe3843-e82b-3a05-8baf-19581a2e1a93"))) {
return null;
}
    return "dataSize=" + this.dataSize + ", getHeapSize=" + this.heapSize + ", getOffHeapSize="
      + this.offHeapSize + ", getCellsCount=" + this.cellsCount;
  }
}
