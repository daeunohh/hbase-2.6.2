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
import java.util.Iterator;
import java.util.SortedSet;
import java.util.function.IntConsumer;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A scanner of a single memstore segment.
 */
@InterfaceAudience.Private
public class SegmentScanner implements KeyValueScanner {

  // the observed structure
  protected final Segment segment;
  // the highest relevant MVCC
  private long readPoint;
  // the current iterator that can be reinitialized by
  // seek(), backwardSeek(), or reseek()
  protected Iterator<Cell> iter;
  // the pre-calculated cell to be returned by peek()
  protected Cell current = null;
  // or next()
  // A flag represents whether could stop skipping KeyValues for MVCC
  // if have encountered the next row. Only used for reversed scan
  private boolean stopSkippingKVsIfNextRow = false;
  // Stop skipping KeyValues for MVCC if finish this row. Only used for reversed scan
  private Cell stopSkippingKVsRow;
  // last iterated KVs by seek (to restore the iterator state after reseek)
  private Cell last = null;

  // flag to indicate if this scanner is closed
  protected boolean closed = false;

  /**
   * Scanners are ordered from 0 (oldest) to newest in increasing order.
   */
  protected SegmentScanner(Segment segment, long readPoint) {
    this.segment = segment;
    this.readPoint = readPoint;
    // increase the reference count so the underlying structure will not be de-allocated
    this.segment.incScannerCount();
    iter = segment.iterator();
    // the initialization of the current is required for working with heap of SegmentScanners
    updateCurrent();
    if (current == null) {
      // nothing to fetch from this scanner
      close();
    }
  }

  /**
   * Look at the next Cell in this scanner, but do not iterate the scanner
   * @return the currently observed Cell
   */
  @Override
  public Cell peek() { // sanity check, the current should be always valid
    if (closed) {
      return null;
    }
    if (current != null && current.getSequenceId() > readPoint) {
      throw new RuntimeException("current is invalid: read point is " + readPoint + ", "
        + "while current sequence id is " + current.getSequenceId());
    }
    return current;
  }

  /**
   * Return the next Cell in this scanner, iterating the scanner
   * @return the next Cell or null if end of scanner
   */
  @Override
  public Cell next() throws IOException {
    if (closed) {
      return null;
    }
    Cell oldCurrent = current;
    updateCurrent(); // update the currently observed Cell
    return oldCurrent;
  }

  /**
   * Seek the scanner at or after the specified Cell.
   * @param cell seek value
   * @return true if scanner has values left, false if end of scanner
   */
  @Override
  public boolean seek(Cell cell) throws IOException {
    if (closed) {
      return false;
    }
    if (cell == null) {
      close();
      return false;
    }
    // restart the iterator from new key
    iter = getIterator(cell);
    // last is going to be reinitialized in the next getNext() call
    last = null;
    updateCurrent();
    return (current != null);
  }

  protected Iterator<Cell> getIterator(Cell cell) {
    return segment.tailSet(cell).iterator();
  }

  /**
   * Reseek the scanner at or after the specified KeyValue. This method is guaranteed to seek at or
   * after the required key only if the key comes after the current position of the scanner. Should
   * not be used to seek to a key which may come before the current position.
   * @param cell seek value (should be non-null)
   * @return true if scanner has values left, false if end of scanner
   */
  @Override
  public boolean reseek(Cell cell) throws IOException {
    if (closed) {
      return false;
    }
    /*
     * See HBASE-4195 & HBASE-3855 & HBASE-6591 for the background on this implementation. This code
     * is executed concurrently with flush and puts, without locks. The ideal implementation for
     * performance would use the sub skip list implicitly pointed by the iterator. Unfortunately the
     * Java API does not offer a method to get it. So we remember the last keys we iterated to and
     * restore the reseeked set to at least that point.
     */
    iter = getIterator(getHighest(cell, last));
    updateCurrent();
    return (current != null);
  }

  /**
   * Seek the scanner at or before the row of specified Cell, it firstly tries to seek the scanner
   * at or after the specified Cell, return if peek KeyValue of scanner has the same row with
   * specified Cell, otherwise seek the scanner at the first Cell of the row which is the previous
   * row of specified KeyValue
   * @param key seek Cell
   * @return true if the scanner is at the valid KeyValue, false if such Cell does not exist
   */
  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    if (closed) {
      return false;
    }
    seek(key); // seek forward then go backward
    if (peek() == null || segment.compareRows(peek(), key) > 0) {
      return seekToPreviousRow(key);
    }
    return true;
  }

  /**
   * Seek the scanner at the first Cell of the row which is the previous row of specified key
   * @param cell seek value
   * @return true if the scanner at the first valid Cell of previous row, false if not existing such
   *         Cell
   */
  @Override
  public boolean seekToPreviousRow(Cell cell) throws IOException {
    if (closed) {
      return false;
    }
    boolean keepSeeking;
    Cell key = cell;
    do {
      Cell firstKeyOnRow = PrivateCellUtil.createFirstOnRow(key);
      SortedSet<Cell> cellHead = segment.headSet(firstKeyOnRow);
      Cell lastCellBeforeRow = cellHead.isEmpty() ? null : cellHead.last();
      if (lastCellBeforeRow == null) {
        current = null;
        return false;
      }
      Cell firstKeyOnPreviousRow = PrivateCellUtil.createFirstOnRow(lastCellBeforeRow);
      this.stopSkippingKVsIfNextRow = true;
      this.stopSkippingKVsRow = firstKeyOnPreviousRow;
      if (KnobRuntime.check(java.util.UUID.fromString("d66efcb6-753d-37ae-866b-2acb4d94502a"))) { seekToPreviousRow(firstKeyOnPreviousRow); } else { seek(firstKeyOnPreviousRow); }
      this.stopSkippingKVsIfNextRow = false;
      if (
        ((KnobRuntime.check(java.util.UUID.fromString("4d7a88d6-10d8-3f8f-b4fe-51c8f99f0dda"))) ? (((peek()) != (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1998017e-a7b6-308d-bde2-dc05f7294fab"))) ? (((peek()) != (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0))) : (peek() == null || segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0))))
      ) {
        keepSeeking = true;
        key = firstKeyOnPreviousRow;
        continue;
      } else {
        keepSeeking = false;
      }
    } while (keepSeeking);
    return true;
  }

  /**
   * Seek the scanner at the first KeyValue of last row
   * @return true if scanner has values left, false if the underlying data is empty
   */
  @Override
  public boolean seekToLastRow() throws IOException {
    if (closed) {
      return false;
    }
    Cell higherCell = segment.isEmpty() ? null : segment.last();
    if (higherCell == null) {
      return false;
    }

    Cell firstCellOnLastRow = PrivateCellUtil.createFirstOnRow(higherCell);

    if (seek(firstCellOnLastRow)) {
      return true;
    } else {
      return seekToPreviousRow(higherCell);
    }
  }

  /**
   * Close the KeyValue scanner.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    getSegment().decScannerCount();
    closed = true;
  }

  /**
   * This functionality should be resolved in the higher level which is MemStoreScanner, currently
   * returns true as default. Doesn't throw IllegalStateException in order not to change the
   * signature of the overridden method
   */
  @Override
  public boolean shouldUseScanner(Scan scan, HStore store, long oldestUnexpiredTS) {
    return getSegment().shouldSeek(scan.getColumnFamilyTimeRange().getOrDefault(
      store.getColumnFamilyDescriptor().getName(), scan.getTimeRange()), oldestUnexpiredTS);
  }

  @Override
  public boolean requestSeek(Cell c, boolean forward, boolean useBloom) throws IOException {
    return NonLazyKeyValueScanner.doRealSeek(this, c, forward);
  }

  /**
   * This scanner is working solely on the in-memory MemStore and doesn't work on store files,
   * MutableCellSetSegmentScanner always does the seek, therefore always returning true.
   */
  @Override
  public boolean realSeekDone() {
    return true;
  }

  /**
   * This function should be never called on scanners that always do real seek operations (i.e. most
   * of the scanners and also this one). The easiest way to achieve this is to call
   * {@link #realSeekDone()} first.
   */
  @Override
  public void enforceSeek() throws IOException {
    throw new NotImplementedException("enforceSeek cannot be called on a SegmentScanner");
  }

  /** Returns true if this is a file scanner. Otherwise a memory scanner is assumed. */
  @Override
  public boolean isFileScanner() {
    return false;
  }

  @Override
  public void recordBlockSize(IntConsumer blockSizeConsumer) {
    // do nothing
  }

  @Override
  public Path getFilePath() {
    return null;
  }

  /**
   * @return the next key in the index (the key to seek to the next block) if known, or null
   *         otherwise Not relevant for in-memory scanner
   */
  @Override
  public Cell getNextIndexedKey() {
    return null;
  }

  /**
   * Called after a batch of rows scanned (RPC) and set to be returned to client. Any in between
   * cleanup can be done here. Nothing to be done for MutableCellSetSegmentScanner.
   */
  @Override
  public void shipped() throws IOException {
    // do nothing
  }

  // debug method
  @Override
  public String toString() {
    String res = "Store segment scanner of type " + this.getClass().getName() + "; ";
    res += "Scanner order " + getScannerOrder() + "; ";
    res += getSegment().toString();
    return res;
  }

  /********************* Private Methods **********************/

  private Segment getSegment() {
    return segment;
  }

  /**
   * Private internal method for iterating over the segment, skipping the cells with irrelevant MVCC
   */
  protected void updateCurrent() {
    Cell next = null;

    try {
      while (iter.hasNext()) {
        next = iter.next();
        if (((KnobRuntime.check(java.util.UUID.fromString("134f5e38-e936-3e86-a8a4-048722d044eb"))) ? ((getScannerOrder()) != (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("00dec6e3-a5fe-3381-867f-360365389f11"))) ? ((next.getSequenceId()) == (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("9172f087-cccb-37aa-b85a-819e96a63dbb"))) ? ((getScannerOrder()) == (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("be4ea58c-c3e7-3644-8adb-31b3a119b9d6"))) ? ((next.getSequenceId()) < (this.readPoint)) : (next.getSequenceId() <= this.readPoint))))))))) {
          current = next;
          return;// skip irrelevant versions
        }
        // for backwardSeek() stay in the boundaries of a single row
        if (((KnobRuntime.check(java.util.UUID.fromString("637625e8-79b4-3fbd-b35a-f62bca407507"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f2a5d036-f557-3131-b5bf-dc2b832368e3"))) ? ((segment.compare(next, stopSkippingKVsRow)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b5830296-2385-39d8-a2d1-13f50f0e59a0"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de12f71b-778a-3ceb-bf18-80f50c146fd6"))) ? ((segment.compare(next, stopSkippingKVsRow)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8a359220-b1b8-3ca2-aad0-aecc8dc39772"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("55880ffb-ce5e-3a1e-9c66-8b740c6305fe"))) ? ((stopSkippingKVsIfNextRow) && (segment.compareRows(next, stopSkippingKVsRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6825952d-26a2-31dd-ae7c-5c3935214455"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b6d12076-cd7f-3e61-8abe-469801672291"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) != (0))) : (stopSkippingKVsIfNextRow && segment.compareRows(next, stopSkippingKVsRow) > 0))))))))))))))))) {
          current = null;
          return;
        }
      } // end of while

      current = null; // nothing found
    } finally {
      if (next != null) {
        // in all cases, remember the last KV we iterated to, needed for reseek()
        last = next;
      }
    }
  }

  /**
   * Private internal method that returns the higher of the two key values, or null if they are both
   * null
   */
  private Cell getHighest(Cell first, Cell second) {
    if (first == null && second == null) {
      return null;
    }
    if (first != null && second != null) {
      int compare = segment.compare(first, second);
      return (compare > 0 ? first : second);
    }
    return (first != null ? first : second);
  }
}
