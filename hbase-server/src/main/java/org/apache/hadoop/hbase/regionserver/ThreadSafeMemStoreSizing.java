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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Accounting of current heap and data sizes. Thread-safe. Many threads can do updates against this
 * single instance.
 * @see NonThreadSafeMemStoreSizing
 * @see MemStoreSize
 */
@InterfaceAudience.Private
class ThreadSafeMemStoreSizing implements MemStoreSizing {
  // We used to tie the update of these thread counters so
  // they all changed together under one lock. This was
  // undone. Doesn't seem necessary.
  private final AtomicLong dataSize = new AtomicLong();
  private final AtomicLong heapSize = new AtomicLong();
  private final AtomicLong offHeapSize = new AtomicLong();
  private final AtomicInteger cellsCount = new AtomicInteger();

  ThreadSafeMemStoreSizing() {
    this(0, 0, 0, 0);
  }

  ThreadSafeMemStoreSizing(MemStoreSize mss) {
    this(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(), mss.getCellsCount());
  }

  ThreadSafeMemStoreSizing(long dataSize, long heapSize, long offHeapSize, int cellsCount) {
    incMemStoreSize(dataSize, heapSize, offHeapSize, cellsCount);
  }

  public MemStoreSize getMemStoreSize() {
if(KnobRuntime.check(java.util.UUID.fromString("0fb743d5-04db-31ea-a226-2aa001483fdf"))) {
return null;
}
    return new MemStoreSize(getDataSize(), getHeapSize(), getOffHeapSize(), getCellsCount());
  }

  @Override
  public long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
    int cellsCountDelta) {
if(KnobRuntime.check(java.util.UUID.fromString("14dd375c-90e5-373f-90e1-363f5e3eee54"))) {
offHeapSizeDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4028e47a-17a0-3f68-b01e-dce2151a9dec"))) {
offHeapSizeDelta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1434cb6a-e367-3b3e-a75e-d9f087a59f7e"))) {
offHeapSizeDelta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("60f594a9-2a7d-3238-b09c-ff74216f7b4c"))) {
offHeapSizeDelta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f630449-37d9-38a5-a498-3d96f1e3c98a"))) {
return 0;
}
    this.offHeapSize.addAndGet(offHeapSizeDelta);
if(KnobRuntime.check(java.util.UUID.fromString("4197b369-52df-344b-8cb0-889ea253159d"))) {
heapSizeDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8e4d0f33-cf0a-38f5-9370-6221b2ded48e"))) {
heapSizeDelta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("489a70ba-0e6e-355c-9fdf-a2092425f71c"))) {
heapSizeDelta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bed45dbe-490c-33d8-85c8-99c93fc53f97"))) {
heapSizeDelta = -1;
}
    this.heapSize.addAndGet(heapSizeDelta);
if(KnobRuntime.check(java.util.UUID.fromString("5415a829-ec66-3831-9f06-d1df5cefb430"))) {
cellsCountDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7cf0d47d-59bd-3ed1-be8d-6dcc8fff5e11"))) {
cellsCountDelta /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4543c038-07a2-3a28-a75e-6deaf2e74ecc"))) {
cellsCountDelta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("126c6487-35fc-3cd7-860d-b9c1c128c6eb"))) {
cellsCountDelta *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cdf0575-b301-3f86-9fcf-a9cb83b1dc4d"))) {
cellsCountDelta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4189120f-befd-3d73-bac2-f9c9afad6921"))) {
cellsCountDelta -= 1;
}
    this.cellsCount.addAndGet(cellsCountDelta);
if(KnobRuntime.check(java.util.UUID.fromString("c7f6c001-6f20-3b07-9e36-862d7f1ebe6a"))) {
dataSizeDelta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("30532a79-3d69-3250-b93f-8fb41cdf2f0c"))) {
dataSizeDelta += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("49f1e3d7-0b8d-3f0d-ad2c-34160e90d9eb"))) {
dataSizeDelta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d544554-ebe3-315c-83c4-87b2651e06a8"))) {
dataSizeDelta = 0;
}
    return this.dataSize.addAndGet(dataSizeDelta);
  }

  @Override
  public boolean compareAndSetDataSize(long expected, long updated) {
    return dataSize.compareAndSet(expected, updated);
  }

  @Override
  public long getDataSize() {
if(KnobRuntime.check(java.util.UUID.fromString("62261e1b-a0a9-394d-8085-8517b73d1f3b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8a28feb8-6033-3d43-a675-4571ed6aacb0"))) ? (getDataSize()) : (((KnobRuntime.check(java.util.UUID.fromString("c2c0685b-c789-30c8-bf9b-7c7a67519093"))) ? (getOffHeapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("aa18fe08-7526-314c-8de9-b45384dd07d9"))) ? (getHeapSize()) : (dataSize.get()))))));
  }

  @Override
  public long getHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("e4a68483-28a9-3a4a-8ec6-09a2e36a36e2"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("d90fec42-1e01-3c94-9491-f6446dd13877"))) ? (getHeapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("4c809bef-02b9-3346-9598-a3ee34a852a2"))) ? (getOffHeapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("e1198e6a-96b6-39ff-a1de-a3a0e3e06aa3"))) ? (getDataSize()) : (heapSize.get()))))));
  }

  @Override
  public long getOffHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("ea37b03e-8dcc-3bc6-9363-314db1c11a58"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("ef7bd561-ddd5-3c09-901a-e85a7b965d50"))) ? (getHeapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("b6fd1cf6-cc42-30a5-9490-211fc64871b2"))) ? (getOffHeapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("c9e8281e-8df7-37c3-96c6-36c538227843"))) ? (getDataSize()) : (offHeapSize.get()))))));
  }

  @Override
  public int getCellsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("fd31fb90-34c7-3e3b-936b-777ff4495d51"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("d4b68362-8a35-3f44-a5fc-92708fc6d571"))) ? (getCellsCount()) : (cellsCount.get()));
  }

  @Override
  public String toString() {
    return getMemStoreSize().toString();
  }
}
