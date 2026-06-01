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
    if (KnobRuntime.check(java.util.UUID.fromString("1fec8e37-8d6a-3107-9c5a-11e00412a6db"))) { close(); } else if (KnobRuntime.check(java.util.UUID.fromString("4a2b88c1-5173-3512-a88a-e8df78de8d23"))) { updateCurrent(); } else { this.segment.incScannerCount(); }
    iter = segment.iterator();
    // the initialization of the current is required for working with heap of SegmentScanners
    if (KnobRuntime.check(java.util.UUID.fromString("5379ecc8-a76e-326f-9cea-eb3c63737fc0"))) { close(); } else { updateCurrent(); }
    if (((KnobRuntime.check(java.util.UUID.fromString("278018b1-5768-35be-a307-76e18f5205bc"))) ? ((current) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9fa314fa-d01f-303b-80a3-0eea5757acdd"))) ? ((current) == (null)) : (current == null))))) {
      // nothing to fetch from this scanner
      if (KnobRuntime.check(java.util.UUID.fromString("d5b884ac-8936-35ed-b3af-8df489be24a9"))) { updateCurrent(); } else { close(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("fbf310d0-ce43-3c8d-aab8-50388d8f90c2"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("21f4ed97-e493-3311-8bc1-23f0232f2884"))) {
throw new java.io.IOException("Injected exception");
}
      return ((KnobRuntime.check(java.util.UUID.fromString("bd93d723-1122-37f2-94d9-c5f6228948f1"))) ? (seek(key)) : (seekToPreviousRow(key)));
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
      if (((KnobRuntime.check(java.util.UUID.fromString("4d34469d-c3b5-316f-b3eb-18804ba5656c"))) ? ((lastCellBeforeRow) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("56fd19d5-bc81-395a-8189-289f0d50fe5f"))) ? ((lastCellBeforeRow) != (null)) : (lastCellBeforeRow == null))))) {
        current = null;
        return false;
      }
      Cell firstKeyOnPreviousRow = PrivateCellUtil.createFirstOnRow(lastCellBeforeRow);
      this.stopSkippingKVsIfNextRow = true;
      this.stopSkippingKVsRow = firstKeyOnPreviousRow;
if(KnobRuntime.check(java.util.UUID.fromString("f3729137-10a6-3f99-b2d3-d79c4e24a6bc"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("d66efcb6-753d-37ae-866b-2acb4d94502a"))) { seekToPreviousRow(firstKeyOnPreviousRow); } else { seek(firstKeyOnPreviousRow); }
      this.stopSkippingKVsIfNextRow = false;
      if (
        ((KnobRuntime.check(java.util.UUID.fromString("c05353be-f99d-39aa-b6f3-055050d4d73f"))) ? ((peek() == null) || (segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8017a0b2-cc45-3449-841b-6f85db51f49a"))) ? (((peek()) == (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c777652-669b-343f-a62b-4657057a0339"))) ? (((peek()) != (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d7a88d6-10d8-3f8f-b4fe-51c8f99f0dda"))) ? (((peek()) != (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2eb7ce0f-3aae-3f46-829f-982fd3d6a825"))) ? (((peek()) == (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5d4f6b85-e24a-396d-a334-f8e8e2126e93"))) ? ((peek() == null) && (segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("27484bbf-74a1-3191-b79a-0ca330ee7052"))) ? (((peek()) != (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6ece6d7d-674f-3952-a70d-2aeabb534f46"))) ? (((peek()) != (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("95d7f716-de49-3444-91c0-b5326a40b514"))) ? (((peek()) != (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2d3af80d-af2c-3813-824c-91e4fffb5685"))) ? (((peek()) != (null)) || (segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2e89db8a-260f-3a42-a8df-9abb98cfa959"))) ? ((peek() == null) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3c2004d0-c553-32a5-873d-ff72e76dec60"))) ? ((peek() == null) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b81b9402-fb1a-31fb-88a4-e3117b787805"))) ? (((peek()) == (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b9762d51-8531-320f-9518-5323711c5950"))) ? ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b6046772-035a-3071-8534-0aea73857b61"))) ? (((peek()) == (null)) && (segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1998017e-a7b6-308d-bde2-dc05f7294fab"))) ? (((peek()) != (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("256f6ad2-cc39-3dd5-bf15-f5979b775218"))) ? ((peek() == null) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3e71b622-861d-39e3-bfff-718c798f0714"))) ? (((peek()) == (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0eb1f678-7ea2-31b7-88fd-5769a409775e"))) ? (((peek()) == (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c8e2c07-6a6c-35ea-897c-ac4c084c1113"))) ? (((peek()) != (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e91faeda-1b63-3b7c-99b0-66c944cff9bc"))) ? ((peek()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b977143b-34ea-31fb-bf25-38633f0bf871"))) ? ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b2ca2860-43fd-33a1-85ff-aee76b2f5bff"))) ? ((peek() == null) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2e1ab364-ce83-34c0-ad9a-ba3cdb3e4f8f"))) ? ((peek()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5182426a-3cb5-32b1-8497-3c75c9cdee6f"))) ? (((peek()) != (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a233f290-f3cf-3c2c-a51c-1d3fbef6f5da"))) ? ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fad12aa3-026e-3c82-bb20-5d3c4f6ea793"))) ? ((peek() == null) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("80bce404-4505-3ab6-b7a8-c61d0ed6cb62"))) ? (((peek()) != (null)) && (segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("60ddb0ec-868f-3927-835c-534ccaf1bbbd"))) ? ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0f890b7c-8800-3e2c-8703-a8c8ab8050b8"))) ? (((peek()) == (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("310ce287-8b1f-3fd8-8982-1296b4a95989"))) ? (((peek()) == (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b500b175-1456-3c68-ba96-0656b91ee0f0"))) ? ((peek() == null) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5c297646-32d3-3dc0-90ad-4feea7a1699b"))) ? (peek() == null) : (((KnobRuntime.check(java.util.UUID.fromString("c6091401-fffd-3a0f-ab7d-bc2a4cb0c66d"))) ? ((peek() == null) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ba0a47dc-8fa2-331c-860a-822b063611c8"))) ? ((peek() == null) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac8daa53-b0c9-3521-9a79-d02d01b1ce56"))) ? (((peek()) == (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("21df010b-1a7f-3bad-b7d0-6da11ab7df96"))) ? ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("09a882b3-c34d-32ef-8561-01c76e4506b8"))) ? (((peek()) == (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("11d48e8b-7471-305c-a8f1-eb5469d2c07c"))) ? ((peek() == null) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf666a1e-156a-383b-944a-b43df8984317"))) ? (((peek()) != (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eddcf6b2-3dda-33b1-9fc6-e3a492d8452b"))) ? (((peek()) == (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6da3581b-1d09-3a04-8aff-b6267a4ff06e"))) ? (((peek()) != (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7bc8ec54-fbe6-3d7c-b160-04dfaba2a5df"))) ? (((peek()) == (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d016afc-d73a-3444-82f5-56d0ffb3b6d8"))) ? ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ebb0a4e8-d9d2-387b-95b5-a72262a28620"))) ? (((peek()) == (null)) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5d30d1cc-6e8d-39ac-9fcc-4b14cd87f410"))) ? (segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0) : (((KnobRuntime.check(java.util.UUID.fromString("4caf1884-9235-330f-9cb8-c9e8c4b1ba3a"))) ? (((peek()) != (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("21dea9c1-dc1c-31f8-bb4f-53e410931141"))) ? ((peek() == null) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc74a663-5538-3e8b-b4a8-00deb385f771"))) ? (((peek()) != (null)) || ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("37c3a640-ca67-325a-9186-381cc7661b60"))) ? ((peek() == null) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("216b3d12-bea9-3916-81fe-c80843f06363"))) ? (((peek()) == (null)) || (segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("9aa12061-c5ef-3cc4-90e9-c608738b710b"))) ? ((peek() == null) && ((segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow)) <= (0))) : (peek() == null || segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
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
if(KnobRuntime.check(java.util.UUID.fromString("fab51af5-377f-3762-933b-7c71c2229184"))) {
throw new java.io.IOException("Injected exception");
}
      return ((KnobRuntime.check(java.util.UUID.fromString("944713cc-89f9-3073-9afc-ea6df6c69be5"))) ? (seek(higherCell)) : (seekToPreviousRow(higherCell)));
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
if(KnobRuntime.check(java.util.UUID.fromString("68c0c7ad-b1a1-3a59-87af-c80e6cb84bae"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("4b355a35-cef6-3276-b864-de480d08eefd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9442e574-d437-31b2-86c1-3dac0d6ca843"))) {
forward = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("3af3912b-09bc-3431-b229-ae0f63ac6f69"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("71800ce1-32ec-322f-9b72-931fff088957"))) {
forward = !forward;
}
if(KnobRuntime.check(java.util.UUID.fromString("47afcfdb-d136-3012-a3fe-62f1ac108ffd"))) {
forward = false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8ff12771-5e25-38cb-8b60-92622ef99ca7"))) ? (NonLazyKeyValueScanner.doRealSeek(this, c, !forward)) : (NonLazyKeyValueScanner.doRealSeek(this, c, forward)));
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
        if (((KnobRuntime.check(java.util.UUID.fromString("134f5e38-e936-3e86-a8a4-048722d044eb"))) ? ((getScannerOrder()) != (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("9a812feb-181d-394f-b44f-bdaed7035a33"))) ? ((next.getSequenceId()) <= (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("f36db415-b5a5-3713-9aba-c5c7b5a688b9"))) ? ((getScannerOrder()) < (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("12729046-724a-3c76-af71-7e03146cdb41"))) ? ((next.getSequenceId()) != (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("b8b87b58-18d4-3460-98eb-aecb8fc7ad4e"))) ? ((next.getSequenceId()) >= (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("40c6674f-c5a6-39b1-b92e-61ccc66507fe"))) ? ((getScannerOrder()) >= (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("b55efa1e-ddf6-3d86-b9ac-be4b1e002a85"))) ? ((next.getSequenceId()) > (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("e25a2424-9444-36cb-a1f3-18f15ba09fe0"))) ? ((getScannerOrder()) <= (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("9172f087-cccb-37aa-b85a-819e96a63dbb"))) ? ((getScannerOrder()) == (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("00dec6e3-a5fe-3381-867f-360365389f11"))) ? ((next.getSequenceId()) == (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("f010e5fe-f9a0-37d7-a2a5-af55fa3c9972"))) ? ((getScannerOrder()) > (this.readPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("be4ea58c-c3e7-3644-8adb-31b3a119b9d6"))) ? ((next.getSequenceId()) < (this.readPoint)) : (next.getSequenceId() <= this.readPoint))))))))))))))))))))))))) {
          current = next;
          return;// skip irrelevant versions
        }
        // for backwardSeek() stay in the boundaries of a single row
        if (((KnobRuntime.check(java.util.UUID.fromString("55880ffb-ce5e-3a1e-9c66-8b740c6305fe"))) ? ((stopSkippingKVsIfNextRow) && (segment.compareRows(next, stopSkippingKVsRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("02b65d36-9d99-3350-bcdc-a0dc4b2d15f2"))) ? ((segment.compare(next, stopSkippingKVsRow)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fb4a4f72-56cd-3448-aebc-a0af72ae96d8"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d319b130-708b-3ff7-9ea7-86fc133b378c"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0bb42673-1d0e-3b9d-bbd6-bcfe24c85015"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cb8d7084-ad69-3625-b70b-4a251ed70dc0"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f2a5d036-f557-3131-b5bf-dc2b832368e3"))) ? ((segment.compare(next, stopSkippingKVsRow)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6785491c-4948-317a-915b-29830818c329"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de12f71b-778a-3ceb-bf18-80f50c146fd6"))) ? ((segment.compare(next, stopSkippingKVsRow)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8a359220-b1b8-3ca2-aad0-aecc8dc39772"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d2b00faf-a784-3286-a5d7-9d1f50e3d2d5"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c8463163-c0ca-36c2-8520-c88ac1bb87cc"))) ? ((segment.compareRows(next, stopSkippingKVsRow)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c29fe888-95e4-3c0e-8c19-2f84a4508422"))) ? ((segment.compare(next, stopSkippingKVsRow)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b83ea413-67e6-34ac-b096-4e9559a738e2"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("50a61dff-a583-3f10-93b9-1dacc47fe2f9"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("41f89ba9-e76e-3000-8d12-c114b4b1dc52"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e1b89365-6e64-335d-8678-e5fd0e6f1b14"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d7e4325e-648a-38b0-940a-d96e875b52a3"))) ? (!stopSkippingKVsIfNextRow) : (((KnobRuntime.check(java.util.UUID.fromString("4f86ea75-4a99-3da9-bbef-09a3aae482bd"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("41bb1acb-49ec-373f-8b4e-54329f254f93"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3b3d86bf-192b-3c68-909f-c102e9a5e79a"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4157b294-f430-37f6-a52d-34c51158c13e"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("03915135-ccaf-35e4-9536-a38d0e525cac"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("42191dc5-a30b-3047-b0be-cf972531d7f9"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("611729f8-0ce5-3105-8f19-a8233212336a"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aaafc677-1797-3edc-9c50-0034b337fb0e"))) ? (segment.compareRows(next, stopSkippingKVsRow) > 0) : (((KnobRuntime.check(java.util.UUID.fromString("f0aae97e-953e-3aed-a058-84bce58a4f6d"))) ? (stopSkippingKVsIfNextRow) : (((KnobRuntime.check(java.util.UUID.fromString("9f613055-29d3-3bf6-b541-a4996403458b"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b6d12076-cd7f-3e61-8abe-469801672291"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91a84551-f8a4-3c67-8e86-fcb16054b625"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eb9ab3f2-f68d-32e8-bc8f-97996b4fe3c6"))) ? ((segment.compareRows(next, stopSkippingKVsRow)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b83e4911-7c37-39c7-878d-9275059297a9"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c8873877-2817-36a2-8f68-5a043602e6ad"))) ? ((segment.compareRows(next, stopSkippingKVsRow)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("80790963-c241-39bb-a36d-4e6529baf507"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b5830296-2385-39d8-a2d1-13f50f0e59a0"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("979867a0-9178-33aa-89f2-6386d6579fa1"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8babfa97-2222-3da1-a05a-11d09478a9b5"))) ? ((segment.compare(next, stopSkippingKVsRow)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("03bbadb9-100b-3efa-b2eb-bde0a046d5bd"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cd707d3d-d60f-3fda-a713-2703f54e34f6"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6825952d-26a2-31dd-ae7c-5c3935214455"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4140719f-1fea-3fdc-83c5-ca366a929302"))) ? ((segment.compareRows(next, stopSkippingKVsRow)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d9027eea-175c-35a6-8e00-5f80282a0142"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eb6369bc-be97-393a-b34c-1ef67d720fa2"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("939f105d-0ec7-3751-944d-1f5bc9ed9b53"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7cce3c26-a7a1-3033-b5a2-3fb3975c419e"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b64198f7-b43a-3494-863d-63c1cd0aca2b"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2d6e9c5e-1c12-3fb6-a0f2-2da565e99742"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f2a4713-6faf-391f-b6b0-cecc8b2dd58e"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("48cd6568-e617-381e-a7fb-556a0ca7b876"))) ? ((!stopSkippingKVsIfNextRow) && (segment.compareRows(next, stopSkippingKVsRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("f7d0d76e-239c-3e6f-8e63-7ef01d7c48d3"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("99ddf0f4-ce60-36d9-b1be-94b21fc753f1"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c33a7dc4-8969-321b-bc64-eb2c2867ca20"))) ? ((stopSkippingKVsIfNextRow) || (segment.compareRows(next, stopSkippingKVsRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("637625e8-79b4-3fbd-b35a-f62bca407507"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f826a789-a976-33c3-a925-fd684255a99c"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f5aba77f-174b-30e0-9d07-c95e45c9acd8"))) ? ((segment.compare(next, stopSkippingKVsRow)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("933f9f32-7dad-308c-a1e8-04cbbc660844"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a2d5f6b6-5c5d-3ce9-a5b7-ffea1b0ca0a2"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ddc8d18-2778-3b82-8341-717ee25ab479"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2041edee-2624-3598-9145-8ca2ea835195"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6e7bc5d7-fbb4-3c27-bb08-3f0b63ce3db5"))) ? ((!stopSkippingKVsIfNextRow) && ((segment.compare(next, stopSkippingKVsRow)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a59ce21d-6475-3662-84a6-6bd8c3a85bd2"))) ? ((segment.compareRows(next, stopSkippingKVsRow)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("33598f91-d44c-3043-b424-9100b5eec0b5"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("70717abc-3a1b-34ae-9892-32770d26ad1c"))) ? ((stopSkippingKVsIfNextRow) && ((segment.compareRows(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("98bfdb00-0598-34b8-850c-9bdfd99182e5"))) ? ((stopSkippingKVsIfNextRow) || ((segment.compareRows(next, stopSkippingKVsRow)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7dec9e98-3c84-3c4a-9b46-1d87c1d7d93b"))) ? ((!stopSkippingKVsIfNextRow) || ((segment.compare(next, stopSkippingKVsRow)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("18cae9f5-a17d-3673-b6e2-ab6181ad35a1"))) ? ((!stopSkippingKVsIfNextRow) || (segment.compareRows(next, stopSkippingKVsRow) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b5890d37-9a06-3c43-9a4d-159583660a51"))) ? ((segment.compareRows(next, stopSkippingKVsRow)) < (0)) : (stopSkippingKVsIfNextRow && segment.compareRows(next, stopSkippingKVsRow) > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
