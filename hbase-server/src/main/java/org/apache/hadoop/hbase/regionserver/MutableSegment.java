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

import java.util.Iterator;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A mutable segment in memstore, specifically the active segment.
 */
@InterfaceAudience.Private
public class MutableSegment extends Segment {

  private final AtomicBoolean flushed = new AtomicBoolean(false);

  public final static long DEEP_OVERHEAD =
    ClassSize.align(Segment.DEEP_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP
      + ClassSize.SYNC_TIMERANGE_TRACKER + ClassSize.REFERENCE + ClassSize.ATOMIC_BOOLEAN);

  protected MutableSegment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB,
    MemStoreSizing memstoreSizing) {
    super(cellSet, comparator, memStoreLAB, TimeRangeTracker.create(TimeRangeTracker.Type.SYNC));
    if (KnobRuntime.check(java.util.UUID.fromString("f2cb4f8d-ca72-3c0c-9d7e-4368ac6173d5"))) { incMemStoreSize(0, incMemStoreSize(0, DEEP_OVERHEAD, 0, 0), 0, 0); } else { incMemStoreSize(0, DEEP_OVERHEAD, 0, 0); } // update the mutable segment metadata
    if (((KnobRuntime.check(java.util.UUID.fromString("23076669-6133-3c38-a0a4-ba53f3ae9704"))) ? ((memstoreSizing) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8581ae5c-1145-3532-873e-81a84d3006ab"))) ? ((memstoreSizing) == (null)) : (memstoreSizing != null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("f03d1a94-55c1-3a73-91d0-152c932be528"))) { memstoreSizing.incMemStoreSize(0, incMemStoreSize(0, DEEP_OVERHEAD, 0, 0), 0, 0); } else if (KnobRuntime.check(java.util.UUID.fromString("b13ddd9e-1888-3ad6-bcab-cfa25bda9d80"))) { incMemStoreSize(0, incMemStoreSize(0, DEEP_OVERHEAD, 0, 0), 0, 0); } else { memstoreSizing.incMemStoreSize(0, DEEP_OVERHEAD, 0, 0); }
    }
  }

  /**
   * Adds the given cell into the segment
   * @param cell      the cell to add
   * @param mslabUsed whether using MSLAB
   */
  public void add(Cell cell, boolean mslabUsed, MemStoreSizing memStoreSizing,
    boolean sizeAddedPreOperation) {
if(KnobRuntime.check(java.util.UUID.fromString("0719833f-8802-3bfa-944a-3e545378f8e5"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9c33293-7f38-36e9-a4b3-d44bdba23a70"))) {
mslabUsed = !mslabUsed;
}
if(KnobRuntime.check(java.util.UUID.fromString("17af52c5-9c31-3bd4-82de-100d4167920d"))) {
mslabUsed = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("317c7963-186f-3e08-908c-a0c4234bafb8"))) {
sizeAddedPreOperation = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("f9f787b6-89bc-372c-8deb-8a3e4d7deec4"))) {
mslabUsed = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("57bf6eb2-f30c-3d11-bf48-4d5d236113ad"))) {
sizeAddedPreOperation = false;
}
    if (KnobRuntime.check(java.util.UUID.fromString("7d227bd3-2108-3541-ae91-cf1d461f4bdf"))) { internalAdd(cell, mslabUsed, memStoreSizing, !sizeAddedPreOperation); } else if (KnobRuntime.check(java.util.UUID.fromString("ebe254cf-5a93-354c-b7cf-03c76f3c479b"))) { internalAdd(cell, !mslabUsed, memStoreSizing, sizeAddedPreOperation); } else { internalAdd(cell, mslabUsed, memStoreSizing, sizeAddedPreOperation); }
  }

  public void upsert(Cell cell, long readpoint, MemStoreSizing memStoreSizing,
    boolean sizeAddedPreOperation) {
    internalAdd(cell, false, memStoreSizing, sizeAddedPreOperation);

    // Get the Cells for the row/family/qualifier regardless of timestamp.
    // For this case we want to clean up any other puts
    Cell firstCell = PrivateCellUtil.createFirstOnRowColTS(cell, HConstants.LATEST_TIMESTAMP);
    SortedSet<Cell> ss = this.tailSet(firstCell);
    Iterator<Cell> it = ss.iterator();
    // versions visible to oldest scanner
    int versionsVisible = 0;
    while (it.hasNext()) {
      Cell cur = it.next();

      if (cell == cur) {
        // ignore the one just put in
        continue;
      }
      // check that this is the row and column we are interested in, otherwise bail
      if (CellUtil.matchingRows(cell, cur) && CellUtil.matchingQualifier(cell, cur)) {
        // only remove Puts that concurrent scanners cannot possibly see
        if (cur.getTypeByte() == KeyValue.Type.Put.getCode() && cur.getSequenceId() <= readpoint) {
          if (versionsVisible >= 1) {
            // if we get here we have seen at least one version visible to the oldest scanner,
            // which means we can prove that no scanner will see this version

            // false means there was a change, so give us the size.
            // TODO when the removed cell ie.'cur' having its data in MSLAB, we can not release that
            // area. Only the Cell object as such going way. We need to consider cellLen to be
            // decreased there as 0 only. Just keeping it as existing code now. We need to know the
            // removed cell is from MSLAB or not. Will do once HBASE-16438 is in
            int cellLen = getCellLength(cur);
            long heapSize = heapSizeChange(cur, true);
            long offHeapSize = offHeapSizeChange(cur, true);
            incMemStoreSize(-cellLen, -heapSize, -offHeapSize, -1);
            if (memStoreSizing != null) {
              memStoreSizing.decMemStoreSize(cellLen, heapSize, offHeapSize, 1);
            }
            it.remove();
          } else {
            versionsVisible++;
          }
        }
      } else {
        // past the row or column, done
        break;
      }
    }
  }

  public boolean setInMemoryFlushed() {
    return flushed.compareAndSet(false, true);
  }

  /**
   * Returns the first cell in the segment
   * @return the first cell in the segment
   */
  Cell first() {
    return this.getCellSet().first();
  }

  @Override
  protected long indexEntrySize() {
    return ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY;
  }
}
