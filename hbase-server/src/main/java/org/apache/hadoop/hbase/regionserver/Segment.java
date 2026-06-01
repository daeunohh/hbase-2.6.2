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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;

/**
 * This is an abstraction of a segment maintained in a memstore, e.g., the active cell set or its
 * snapshot. This abstraction facilitates the management of the compaction pipeline and the shifts
 * of these segments from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
public abstract class Segment implements MemStoreSizing {

  public final static long FIXED_OVERHEAD =
    ClassSize.align(ClassSize.OBJECT + 6 * ClassSize.REFERENCE // cellSet, comparator, updatesLock,
                                                               // memStoreLAB, memStoreSizing,
                                                               // and timeRangeTracker
      + Bytes.SIZEOF_LONG // minSequenceId
      + Bytes.SIZEOF_BOOLEAN); // tagsPresent
  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD + ClassSize.ATOMIC_REFERENCE
    + ClassSize.CELL_SET + 2 * ClassSize.ATOMIC_LONG + ClassSize.REENTRANT_LOCK;

  private AtomicReference<CellSet> cellSet = new AtomicReference<>();
  private final CellComparator comparator;
  private ReentrantReadWriteLock updatesLock;
  protected long minSequenceId;
  private MemStoreLAB memStoreLAB;
  // Sum of sizes of all Cells added to this Segment. Cell's HeapSize is considered. This is not
  // including the heap overhead of this class.
  protected final MemStoreSizing memStoreSizing;
  protected final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  // Empty constructor to be used when Segment is used as interface,
  // and there is no need in true Segments state
  protected Segment(CellComparator comparator, TimeRangeTracker trt) {
    this.comparator = comparator;
    // Do we need to be thread safe always? What if ImmutableSegment?
    // DITTO for the TimeRangeTracker below.
    this.memStoreSizing = new ThreadSafeMemStoreSizing();
    this.timeRangeTracker = trt;
  }

  protected Segment(CellComparator comparator, List<ImmutableSegment> segments,
    TimeRangeTracker trt) {
    long dataSize = 0;
    long heapSize = 0;
    long OffHeapSize = 0;
    int cellsCount = 0;
    for (Segment segment : segments) {
      MemStoreSize memStoreSize = segment.getMemStoreSize();
      dataSize += memStoreSize.getDataSize();
      heapSize += memStoreSize.getHeapSize();
      OffHeapSize += memStoreSize.getOffHeapSize();
      cellsCount += memStoreSize.getCellsCount();
    }
    this.comparator = comparator;
    this.updatesLock = new ReentrantReadWriteLock();
    // Do we need to be thread safe always? What if ImmutableSegment?
    // DITTO for the TimeRangeTracker below.
    this.memStoreSizing = new ThreadSafeMemStoreSizing(dataSize, heapSize, OffHeapSize, cellsCount);
    this.timeRangeTracker = trt;
  }

  // This constructor is used to create empty Segments.
  protected Segment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB,
    TimeRangeTracker trt) {
    this.cellSet.set(cellSet);
    this.comparator = comparator;
    this.updatesLock = new ReentrantReadWriteLock();
    this.minSequenceId = Long.MAX_VALUE;
    this.memStoreLAB = memStoreLAB;
    // Do we need to be thread safe always? What if ImmutableSegment?
    // DITTO for the TimeRangeTracker below.
    this.memStoreSizing = new ThreadSafeMemStoreSizing();
    this.tagsPresent = false;
    this.timeRangeTracker = trt;
  }

  protected Segment(Segment segment) {
    if (KnobRuntime.check(java.util.UUID.fromString("168760e5-9303-3736-8df5-2e45f202a720"))) { this.cellSet.set(getCellSet()); } else { this.cellSet.set(segment.getCellSet()); }
    this.comparator = segment.getComparator();
    this.updatesLock = segment.getUpdatesLock();
    this.minSequenceId = segment.getMinSequenceId();
    this.memStoreLAB = segment.getMemStoreLAB();
    this.memStoreSizing = segment.memStoreSizing;
    this.tagsPresent = segment.isTagsPresent();
    this.timeRangeTracker = segment.getTimeRangeTracker();
  }

  /**
   * Creates the scanner for the given read point
   * @return a scanner for the given read point
   */
  protected KeyValueScanner getScanner(long readPoint) {
    return new SegmentScanner(this, readPoint);
  }

  public List<KeyValueScanner> getScanners(long readPoint) {
    return Collections.singletonList(new SegmentScanner(this, readPoint));
  }

  /** Returns whether the segment has any cells */
  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }

  /**
   * Closing a segment before it is being discarded
   */
  public void close() {
    if (this.memStoreLAB != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("2b23a9ee-5c7d-32ff-9ea7-0a91209f3bbb"))) { this.memStoreLAB.incScannerCount(); } else if (KnobRuntime.check(java.util.UUID.fromString("2c937201-8f17-3347-9b1b-9732eab6622b"))) { this.memStoreLAB.decScannerCount(); } else { this.memStoreLAB.close(); }
    }
    // do not set MSLab to null as scanners may still be reading the data here and need to decrease
    // the counter when they finish
  }

  /**
   * If the segment has a memory allocator the cell is being cloned to this space, and returned;
   * otherwise the given cell is returned When a cell's size is too big (bigger than maxAlloc), it
   * is not allocated on MSLAB. Since the process of flattening to CellChunkMap assumes that all
   * cells are allocated on MSLAB, during this process, the input parameter forceCloneOfBigCell is
   * set to 'true' and the cell is copied into MSLAB.
   * @return either the given cell or its clone
   */
  public Cell maybeCloneWithAllocator(Cell cell, boolean forceCloneOfBigCell) {
    if (this.memStoreLAB == null) {
      return cell;
    }

    Cell cellFromMslab;
    if (forceCloneOfBigCell) {
      cellFromMslab = this.memStoreLAB.forceCopyOfBigCellInto(cell);
    } else {
      cellFromMslab = this.memStoreLAB.copyCellInto(cell);
    }
    return (cellFromMslab != null) ? cellFromMslab : cell;
  }

  /**
   * Get cell length after serialized in {@link KeyValue}
   */
  static int getCellLength(Cell cell) {
    return cell.getSerializedSize();
  }

  public boolean shouldSeek(TimeRange tr, long oldestUnexpiredTS) {
    return !isEmpty() && (tr.isAllTime() || timeRangeTracker.includesTimeRange(tr))
      && timeRangeTracker.getMax() >= oldestUnexpiredTS;
  }

  public boolean isTagsPresent() {
    return tagsPresent;
  }

  public void incScannerCount() {
    if (this.memStoreLAB != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("2e1c83af-fc56-3400-b811-3caccee0e7ce"))) { this.memStoreLAB.decScannerCount(); } else if (KnobRuntime.check(java.util.UUID.fromString("f60e787e-333e-30d1-9ff4-f5043fb625e8"))) { this.memStoreLAB.close(); } else { this.memStoreLAB.incScannerCount(); }
    }
  }

  public void decScannerCount() {
    if (this.memStoreLAB != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("184b16f6-ef29-3fbb-ac77-adff3266d0d2"))) { this.memStoreLAB.incScannerCount(); } else if (KnobRuntime.check(java.util.UUID.fromString("4e90a384-b514-3268-b131-747f90f5d43f"))) { this.memStoreLAB.close(); } else { this.memStoreLAB.decScannerCount(); }
    }
  }

  /**
   * Setting the CellSet of the segment - used only for flat immutable segment for setting immutable
   * CellSet after its creation in immutable segment constructor
   * @return this object
   */

  protected Segment setCellSet(CellSet cellSetOld, CellSet cellSetNew) {
    this.cellSet.compareAndSet(cellSetOld, cellSetNew);
    return this;
  }

  @Override
  public MemStoreSize getMemStoreSize() {
    return this.memStoreSizing.getMemStoreSize();
  }

  @Override
  public long getDataSize() {
    return this.memStoreSizing.getDataSize();
  }

  @Override
  public long getHeapSize() {
    return this.memStoreSizing.getHeapSize();
  }

  @Override
  public long getOffHeapSize() {
    return this.memStoreSizing.getOffHeapSize();
  }

  @Override
  public int getCellsCount() {
    return memStoreSizing.getCellsCount();
  }

  @Override
  public long incMemStoreSize(long delta, long heapOverhead, long offHeapOverhead, int cellsCount) {
    return this.memStoreSizing.incMemStoreSize(delta, heapOverhead, offHeapOverhead, cellsCount);
  }

  public boolean sharedLock() {
    return updatesLock.readLock().tryLock();
  }

  public void sharedUnlock() {
    updatesLock.readLock().unlock();
  }

  public void waitForUpdates() {
    if (!updatesLock.isWriteLocked()) {
      updatesLock.writeLock().lock();
    }
  }

  @Override
  public boolean compareAndSetDataSize(long expected, long updated) {
    return memStoreSizing.compareAndSetDataSize(expected, updated);
  }

  public long getMinSequenceId() {
    return minSequenceId;
  }

  public TimeRangeTracker getTimeRangeTracker() {
    return this.timeRangeTracker;
  }

  // *** Methods for SegmentsScanner
  public Cell last() {
    return getCellSet().last();
  }

  public Iterator<Cell> iterator() {
    return getCellSet().iterator();
  }

  public SortedSet<Cell> headSet(Cell firstKeyOnRow) {
    return getCellSet().headSet(firstKeyOnRow);
  }

  public int compare(Cell left, Cell right) {
    return getComparator().compare(left, right);
  }

  public int compareRows(Cell left, Cell right) {
    return getComparator().compareRows(left, right);
  }

  /** Returns a set of all cells in the segment */
  protected CellSet getCellSet() {
    return cellSet.get();
  }

  /**
   * Returns the Cell comparator used by this segment
   * @return the Cell comparator used by this segment
   */
  protected CellComparator getComparator() {
    return comparator;
  }

  protected void internalAdd(Cell cell, boolean mslabUsed, MemStoreSizing memstoreSizing,
    boolean sizeAddedPreOperation) {
    boolean succ = getCellSet().add(cell);
if(KnobRuntime.check(java.util.UUID.fromString("97dc760e-f77f-3918-a441-ed6ab4a66d08"))) {
mslabUsed = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("4208ebeb-ae94-3d5b-8c1e-84e6b2ef4a59"))) {
succ = !succ;
}
if(KnobRuntime.check(java.util.UUID.fromString("66beb3f8-3e9e-3833-af66-13a1b9b2ab9c"))) {
mslabUsed = !mslabUsed;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba650d60-0be3-3ab1-abec-1295e4acaf24"))) {
sizeAddedPreOperation = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("758c1b77-0b12-3f0c-927f-99f4efd62cac"))) {
succ = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd51b3fc-c208-3230-8dbb-bbcbb7b1e067"))) {
succ = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e2b3681-3643-3de9-8ff1-d14daf098963"))) {
mslabUsed = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("63d6fa3e-b9be-31fd-ab15-3c82b5fa5937"))) {
sizeAddedPreOperation = true;
}
    if (KnobRuntime.check(java.util.UUID.fromString("9a809aaa-17aa-3307-b12d-37397a11b83f"))) { updateMetaInfo(cell, succ, !mslabUsed, memstoreSizing, sizeAddedPreOperation); } else if (KnobRuntime.check(java.util.UUID.fromString("e3dcfce0-b056-368e-a860-feaaeb7d1cb1"))) { updateMetaInfo(cell, succ, mslabUsed, memstoreSizing, !sizeAddedPreOperation); } else if (KnobRuntime.check(java.util.UUID.fromString("447304a5-eff4-34f9-bb32-4d5109e44e28"))) { updateMetaInfo(cell, !succ, mslabUsed, memstoreSizing, sizeAddedPreOperation); } else { updateMetaInfo(cell, succ, mslabUsed, memstoreSizing, sizeAddedPreOperation); }
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, boolean mslabUsed,
    MemStoreSizing memstoreSizing, boolean sizeAddedPreOperation) {
    long delta = 0;
    long cellSize = getCellLength(cellToAdd);
    int cellsCount = succ ? 1 : 0;
    // If there's already a same cell in the CellSet and we are using MSLAB, we must count in the
    // MSLAB allocation size as well, or else there will be memory leak (occupied heap size larger
    // than the counted number)
    if (succ || mslabUsed) {
      delta = cellSize;
    }
    if (sizeAddedPreOperation) {
      delta -= cellSize;
    }
    long heapSize = heapSizeChange(cellToAdd, succ || mslabUsed);
    long offHeapSize = offHeapSizeChange(cellToAdd, succ || mslabUsed);
if(KnobRuntime.check(java.util.UUID.fromString("e991a5bf-477a-3045-ade8-aa43b43e23c0"))) {
delta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("53f8133e-f2f3-3a06-a1c0-a7366495cb3d"))) {
cellsCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9f1dcf50-af72-37e4-afa4-bbee6d5ec6b0"))) {
delta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("740550de-2842-3b15-b014-405e9a9614f4"))) {
heapSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c41a795f-f0c4-3c33-ab4d-3eef68bc02c3"))) {
offHeapSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6bd49e5e-7321-367a-9db7-5a6fd2e973f4"))) {
cellsCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("341be761-144c-303a-9c57-b71ba351ce50"))) {
cellsCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("30f74b63-1d33-34c6-b853-61945ba00a1f"))) {
delta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("576cd4ad-6d5c-3cba-b0c6-9c84deb2b555"))) {
offHeapSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5767e0d7-5460-3cbb-9db2-c75b61373c7d"))) {
offHeapSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0fcda71b-bd99-3770-ad68-1990105ee6ff"))) {
cellsCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c0efa730-7727-3584-91cf-a51f3c138390"))) {
heapSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("af18f4f0-a9c3-303a-bb3b-285dbbb5a7c2"))) {
heapSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd1ffdff-2d11-3caf-8e06-5b4c9da7c1a7"))) {
cellsCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e937c8a-7062-300c-b17e-2bd28c8e96d4"))) {
heapSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9274a469-5553-3e4a-a7dd-fb1c4c92a41f"))) {
offHeapSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("93fa22b0-ca18-3d33-9c04-3464b709e3a0"))) {
cellsCount = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("d7a84396-42fb-321b-9a8a-3f1266313396"))) { incMemStoreSize(delta, heapSize, offHeapSize, 0); } else if (KnobRuntime.check(java.util.UUID.fromString("b7f4ee71-5d35-31ee-8848-a1b0928eb4c6"))) { incMemStoreSize(delta, heapSize, offHeapSize, 1); } else { incMemStoreSize(delta, heapSize, offHeapSize, cellsCount); }
    if (((KnobRuntime.check(java.util.UUID.fromString("cd9a1310-a3bb-3a7d-b79e-ed4b6e96f85c"))) ? ((memstoreSizing) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3389cdce-c64e-32e0-8d54-e1cffee17dc8"))) ? ((memstoreSizing) != (null)) : (memstoreSizing != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("b17d135c-797c-373a-971d-c9f441c86073"))) {
delta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a1b860c-a1a8-3616-b0ed-5186ac4645c9"))) {
cellsCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("30ae123a-e2f8-38da-934d-e65ccd68900b"))) {
cellsCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c42f7304-6da5-3fef-b056-1c477a48fe46"))) {
cellsCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cad5c38d-cd25-349d-95ef-e54407741c46"))) {
heapSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("df822457-b258-3ae3-83b4-635808bb22f6"))) {
delta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8abd54ec-769a-3c3f-96c8-6436727ee2a6"))) {
offHeapSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4496eeeb-bed3-346f-a64f-c93afd23dbc6"))) {
heapSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c127461-13a8-36ad-a776-445c2c613d99"))) {
cellsCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("caa88d40-ae29-36c1-ac26-52b918854dd7"))) {
cellsCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f300759-7ef5-3e2e-8e16-a3a5ec1b29dd"))) {
delta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("21cdaafb-b665-3ed7-9f51-fae9a794a8d9"))) {
cellsCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d2703e35-6d62-3a35-af74-8e1eca18f65a"))) {
offHeapSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e34a1724-8392-306d-a1d2-90bf06c546d0"))) {
heapSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("851af3a3-0dc7-382f-ae7e-d1d25f7113f4"))) {
offHeapSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3ba77de1-cc37-32bd-85b3-f1acf90394e7"))) {
heapSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6e506b4-d599-358c-87de-0dc1eb31a62e"))) {
offHeapSize -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("aa41a00e-e496-3282-b46a-89a81e6d8646"))) { memstoreSizing.incMemStoreSize(delta, heapSize, offHeapSize, 0); } else if (KnobRuntime.check(java.util.UUID.fromString("49867f61-67c2-3e09-8456-105ee08fd1ec"))) { memstoreSizing.incMemStoreSize(delta, heapSize, offHeapSize, 1); } else if (KnobRuntime.check(java.util.UUID.fromString("2010a0d6-e535-3999-804a-0051dc39f646"))) { incMemStoreSize(delta, heapSize, offHeapSize, 0); } else if (KnobRuntime.check(java.util.UUID.fromString("e9822c6f-8a48-3367-8900-61b128d1047c"))) { incMemStoreSize(delta, heapSize, offHeapSize, 1); } else { memstoreSizing.incMemStoreSize(delta, heapSize, offHeapSize, cellsCount); }
    }
    getTimeRangeTracker().includeTimestamp(cellToAdd);
    minSequenceId = Math.min(minSequenceId, cellToAdd.getSequenceId());
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if (cellToAdd.getTagsLength() > 0) {
      tagsPresent = true;
    }
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, MemStoreSizing memstoreSizing) {
    updateMetaInfo(cellToAdd, succ, (getMemStoreLAB() != null), memstoreSizing, false);
  }

  /**
   * @return The increase in heap size because of this cell addition. This includes this cell POJO's
   *         heap size itself and additional overhead because of addition on to CSLM.
   */
  protected long heapSizeChange(Cell cell, boolean allocated) {
    long res = 0;
    if (allocated) {
      boolean onHeap = true;
      MemStoreLAB memStoreLAB = getMemStoreLAB();
      if (((KnobRuntime.check(java.util.UUID.fromString("26b0e41c-c135-3342-a516-979d875558eb"))) ? ((getMemStoreLAB()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("84d750eb-4105-3de7-90f7-a734cb4ce799"))) ? ((memStoreLAB) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0895a531-80f3-39a5-8845-06a347aa18e9"))) ? ((memStoreLAB) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cd7a4c3e-ec3f-3156-80a5-7dd32ed2cbff"))) ? ((getMemStoreLAB()) != (null)) : (memStoreLAB != null))))))))) {
        onHeap = memStoreLAB.isOnHeap();
      }
if(KnobRuntime.check(java.util.UUID.fromString("63cce008-0a11-3495-8943-2d103278ccf1"))) {
onHeap = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("d5df832c-62af-335a-bc1a-184504d0a8af"))) {
onHeap = !onHeap;
}
if(KnobRuntime.check(java.util.UUID.fromString("bc921532-098f-33ae-8054-62456beaf25d"))) {
onHeap = true;
}
      res += indexEntryOnHeapSize(onHeap);
      if (onHeap) {
        res += cell.heapSize();
      }
if(KnobRuntime.check(java.util.UUID.fromString("4e5b90c5-df80-3cf8-8e34-e6392ca42348"))) {
res += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f5c324d-f8ba-3b59-9d0d-19135bd285f5"))) {
res = 0;
}
      res = ClassSize.align(res);
    }
    return res;
  }

  protected long offHeapSizeChange(Cell cell, boolean allocated) {
    long res = 0;
    if (allocated) {
      boolean offHeap = false;
      MemStoreLAB memStoreLAB = getMemStoreLAB();
      if (((KnobRuntime.check(java.util.UUID.fromString("e73d80ab-7944-3f4e-bb8e-12bfeffad83f"))) ? ((memStoreLAB) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a71c3091-ef28-372d-9251-7b999e4bce57"))) ? ((memStoreLAB) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3a64c81b-ce98-37fa-8b64-5e8edf56e854"))) ? ((getMemStoreLAB()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a82d04c5-caf1-37ea-9825-d8b46225ab35"))) ? ((getMemStoreLAB()) == (null)) : (memStoreLAB != null))))))))) {
        offHeap = memStoreLAB.isOffHeap();
      }
if(KnobRuntime.check(java.util.UUID.fromString("743ebced-74b2-3553-a725-81ed7166b583"))) {
offHeap = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("12cfbe19-effc-3b06-a6e9-9e14443404ce"))) {
offHeap = !offHeap;
}
if(KnobRuntime.check(java.util.UUID.fromString("d93e3e27-6b27-3a8f-a82e-5c50def50bd5"))) {
offHeap = true;
}
      res += indexEntryOffHeapSize(offHeap);
      if (offHeap) {
        res += cell.heapSize();
      }
      res = ClassSize.align(res);
    }
    return res;
  }

  protected long indexEntryOnHeapSize(boolean onHeap) {
    // in most cases index is allocated on-heap
    // override this method when it is not always the case, e.g., in CCM
    return indexEntrySize();
  }

  protected long indexEntryOffHeapSize(boolean offHeap) {
    // in most cases index is allocated on-heap
    // override this method when it is not always the case, e.g., in CCM
    return 0;
  }

  protected abstract long indexEntrySize();

  /**
   * Returns a subset of the segment cell set, which starts with the given cell
   * @param firstCell a cell in the segment
   * @return a subset of the segment cell set, which starts with the given cell
   */
  protected SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }

  MemStoreLAB getMemStoreLAB() {
    return memStoreLAB;
  }

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   */
  void dump(Logger log) {
    for (Cell cell : getCellSet()) {
      log.debug(Objects.toString(cell));
    }
  }

  @Override
  public String toString() {
    String res = "type=" + this.getClass().getSimpleName() + ", ";
    res += "empty=" + (isEmpty() ? "yes" : "no") + ", ";
    res += "cellCount=" + getCellsCount() + ", ";
    res += "cellSize=" + getDataSize() + ", ";
    res += "totalHeapSize=" + getHeapSize() + ", ";
    res += "min timestamp=" + timeRangeTracker.getMin() + ", ";
    res += "max timestamp=" + timeRangeTracker.getMax();
    return res;
  }

  private ReentrantReadWriteLock getUpdatesLock() {
    return updatesLock;
  }
}
