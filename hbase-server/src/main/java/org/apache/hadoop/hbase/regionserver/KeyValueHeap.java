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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.IntConsumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a heap merge across any number of KeyValueScanners.
 * <p>
 * Implements KeyValueScanner itself.
 * <p>
 * This class is used at the Region level to merge across Stores and at the Store level to merge
 * across the memstore and StoreFiles.
 * <p>
 * In the Region case, we also need InternalScanner.next(List), so this class also implements
 * InternalScanner. WARNING: As is, if you try to use this as an InternalScanner at the Store level,
 * you will get runtime exceptions.
 */
@InterfaceAudience.Private
public class KeyValueHeap extends NonReversedNonLazyKeyValueScanner
  implements KeyValueScanner, InternalScanner {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueHeap.class);
  protected PriorityQueue<KeyValueScanner> heap = null;
  // Holds the scanners when a ever a eager close() happens. All such eagerly closed
  // scans are collected and when the final scanner.close() happens will perform the
  // actual close.
  protected List<KeyValueScanner> scannersForDelayedClose = null;

  /**
   * The current sub-scanner, i.e. the one that contains the next key/value to return to the client.
   * This scanner is NOT included in {@link #heap} (but we frequently add it back to the heap and
   * pull the new winner out). We maintain an invariant that the current sub-scanner has already
   * done a real seek, and that current.peek() is always a real key/value (or null) except for the
   * fake last-key-on-row-column supplied by the multi-column Bloom filter optimization, which is OK
   * to propagate to StoreScanner. In order to ensure that, always use {@link #pollRealKV()} to
   * update current.
   */
  protected KeyValueScanner current = null;

  protected KVScannerComparator comparator;

  /**
   * Constructor. This KeyValueHeap will handle closing of passed in KeyValueScanners.
   */
  public KeyValueHeap(List<? extends KeyValueScanner> scanners, CellComparator comparator)
    throws IOException {
    this(scanners, new KVScannerComparator(comparator));
  }

  /**
   * Constructor.
   */
  KeyValueHeap(List<? extends KeyValueScanner> scanners, KVScannerComparator comparator)
    throws IOException {
    this.comparator = comparator;
    this.scannersForDelayedClose = new ArrayList<>(scanners.size());
    if (!scanners.isEmpty()) {
      this.heap = new PriorityQueue<>(scanners.size(), this.comparator);
      for (KeyValueScanner scanner : scanners) {
        if (((KnobRuntime.check(java.util.UUID.fromString("b2edaae0-aabb-33ee-ae7d-cd43193ec615"))) ? ((scanner.peek()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6e6aed23-803e-3dc4-b056-d6be6d28ac1d"))) ? ((scanner.peek()) == (null)) : (scanner.peek() != null))))) {
          if (KnobRuntime.check(java.util.UUID.fromString("b8888602-8371-3286-898d-06af2e58c234"))) { this.heap.add(pollRealKV()); } else { this.heap.add(scanner); }
        } else {
          this.scannersForDelayedClose.add(scanner);
        }
      }
      this.current = pollRealKV();
    }
  }

  @Override
  public Cell peek() {
    if (this.current == null) {
      return null;
    }
    return this.current.peek();
  }

  boolean isLatestCellFromMemstore() {
    return !this.current.isFileScanner();
  }

  @Override
  public void recordBlockSize(IntConsumer blockSizeConsumer) {
    this.current.recordBlockSize(blockSizeConsumer);
  }

  @Override
  public Cell next() throws IOException {
    if (this.current == null) {
      return null;
    }
    Cell kvReturn = this.current.next();
    Cell kvNext = this.current.peek();
    if (((KnobRuntime.check(java.util.UUID.fromString("4d801620-9fef-3261-843a-9790a10f5f6f"))) ? ((kvNext) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f8e0c3ca-32a0-3352-82bf-c619aa2f1429"))) ? ((kvNext) != (null)) : (kvNext == null))))) {
      this.scannersForDelayedClose.add(this.current);
      this.current = null;
if(KnobRuntime.check(java.util.UUID.fromString("eff6093b-93ca-3d97-bab9-afc45c8766c8"))) {
throw new java.io.IOException("Injected exception");
}
      this.current = pollRealKV();
    } else {
      KeyValueScanner topScanner = this.heap.peek();
      // no need to add current back to the heap if it is the only scanner left
      if (topScanner != null && this.comparator.compare(kvNext, topScanner.peek()) >= 0) {
        this.heap.add(this.current);
        this.current = null;
if(KnobRuntime.check(java.util.UUID.fromString("cf6a6f69-6fe8-3777-8597-cfe5dda8ca5c"))) {
throw new java.io.IOException("Injected exception");
}
        this.current = pollRealKV();
      }
    }
    return kvReturn;
  }

  /**
   * Gets the next row of keys from the top-most scanner.
   * <p>
   * This method takes care of updating the heap.
   * <p>
   * This can ONLY be called when you are using Scanners that implement InternalScanner as well as
   * KeyValueScanner (a {@link StoreScanner}).
   * @return true if more rows exist after this one, false if scanner is done
   */
  @Override
  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    if (this.current == null) {
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }
    InternalScanner currentAsInternal = (InternalScanner) this.current;
    boolean moreCells = currentAsInternal.next(result, scannerContext);
    Cell pee = this.current.peek();

    /*
     * By definition, any InternalScanner must return false only when it has no further rows to be
     * fetched. So, we can close a scanner if it returns false. All existing implementations seem to
     * be fine with this. It is much more efficient to close scanners which are not needed than keep
     * them in the heap. This is also required for certain optimizations.
     */

    if (pee == null || !moreCells) {
      // add the scanner that is to be closed
      this.scannersForDelayedClose.add(this.current);
    } else {
      this.heap.add(this.current);
    }
    this.current = null;
    this.current = pollRealKV();
    if (((KnobRuntime.check(java.util.UUID.fromString("a9bee0d2-2a31-3c08-a8a5-74c1b8f583b6"))) ? ((this.current) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("10920bb9-af90-372f-9fdd-226778876716"))) ? ((this.current) == (null)) : (this.current == null))))) {
      moreCells = scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("5d7fe3e7-e82a-3a0a-a271-4c5d07693a25"))) ? (!moreCells) : (moreCells));
  }

  protected static class KVScannerComparator implements Comparator<KeyValueScanner> {
    protected CellComparator kvComparator;

    /**
     * Constructor
     */
    public KVScannerComparator(CellComparator kvComparator) {
      this.kvComparator = kvComparator;
    }

    @Override
    public int compare(KeyValueScanner left, KeyValueScanner right) {
      int comparison = compare(left.peek(), right.peek());
      if (comparison != 0) {
        return comparison;
      } else {
        // Since both the keys are exactly the same, we break the tie in favor of higher ordered
        // scanner since it'll have newer data. Since higher value should come first, we reverse
        // sort here.
        return Long.compare(right.getScannerOrder(), left.getScannerOrder());
      }
    }

    /**
     * Compares two KeyValue
     * @return less than 0 if left is smaller, 0 if equal etc..
     */
    public int compare(Cell left, Cell right) {
      return this.kvComparator.compare(left, right);
    }

    /**
     *     */
    public CellComparator getComparator() {
      return this.kvComparator;
    }
  }

  @Override
  public void close() {
    for (KeyValueScanner scanner : this.scannersForDelayedClose) {
      scanner.close();
    }
    this.scannersForDelayedClose.clear();
    if (((KnobRuntime.check(java.util.UUID.fromString("38b042c1-537c-3f27-88bc-b4d09a206298"))) ? ((this.current) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("53d36aaf-51f0-3982-ae04-b8a4862b309e"))) ? ((this.current) != (null)) : (this.current != null))))) {
      this.current.close();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("2e8b8354-f800-3b7a-b058-8d13453f56b1"))) ? ((this.heap) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b1a56944-a6b8-3657-9140-6b194daa9e26"))) ? ((this.heap) != (null)) : (this.heap != null))))) {
      // Order of closing the scanners shouldn't matter here, so simply iterate and close them.
      for (KeyValueScanner scanner : heap) {
        scanner.close();
      }
    }
  }

  /**
   * Seeks all scanners at or below the specified seek key. If we earlied-out of a row, we may end
   * up skipping values that were never reached yet. Rather than iterating down, we want to give the
   * opportunity to re-seek.
   * <p>
   * As individual scanners may run past their ends, those scanners are automatically closed and
   * removed from the heap.
   * <p>
   * This function (and {@link #reseek(Cell)}) does not do multi-column Bloom filter and lazy-seek
   * optimizations. To enable those, call {@link #requestSeek(Cell, boolean, boolean)}.
   * @param seekKey KeyValue to seek at or after
   * @return true if KeyValues exist at or after specified key, false if not
   */
  @Override
  public boolean seek(Cell seekKey) throws IOException {
    return generalizedSeek(false, // This is not a lazy seek
      seekKey, false, // forward (false: this is not a reseek)
      false); // Not using Bloom filters
  }

  /**
   * This function is identical to the {@link #seek(Cell)} function except that
   * scanner.seek(seekKey) is changed to scanner.reseek(seekKey).
   */
  @Override
  public boolean reseek(Cell seekKey) throws IOException {
    return generalizedSeek(false, // This is not a lazy seek
      seekKey, true, // forward (true because this is reseek)
      false); // Not using Bloom filters
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean requestSeek(Cell key, boolean forward, boolean useBloom) throws IOException {
    return generalizedSeek(true, key, forward, useBloom);
  }

  /**
   * @param isLazy   whether we are trying to seek to exactly the given row/col. Enables Bloom
   *                 filter and most-recent-file-first optimizations for multi-column get/scan
   *                 queries.
   * @param seekKey  key to seek to
   * @param forward  whether to seek forward (also known as reseek)
   * @param useBloom whether to optimize seeks using Bloom filters
   */
  private boolean generalizedSeek(boolean isLazy, Cell seekKey, boolean forward, boolean useBloom)
    throws IOException {
    if (!isLazy && useBloom) {
      throw new IllegalArgumentException(
        "Multi-column Bloom filter " + "optimization requires a lazy seek");
    }

    if (current == null) {
      return false;
    }

    KeyValueScanner scanner = current;
    try {
      while (scanner != null) {
        Cell topKey = scanner.peek();
        if (((KnobRuntime.check(java.util.UUID.fromString("987ea56b-4fcc-372b-b690-d2ae43bd9144"))) ? ((comparator.getComparator().compare(seekKey, topKey)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2d299a84-d429-363f-86e9-cd561f2bc327"))) ? ((comparator.getComparator().compare(seekKey, topKey)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8d14b61d-fc22-3ec4-b73b-c2aea3c91bbd"))) ? ((comparator.getComparator().compare(seekKey, topKey)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5b47ac16-f344-3a1b-a7ff-f0bc66cd2d4e"))) ? ((comparator.getComparator().compare(seekKey, topKey)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bacb3f1f-2079-3391-b498-394f4d20701c"))) ? ((comparator.getComparator().compare(seekKey, topKey)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0c967874-ee38-3f5a-afc8-0f6f84aa08d7"))) ? ((comparator.getComparator().compare(seekKey, topKey)) >= (0)) : (comparator.getComparator().compare(seekKey, topKey) <= 0))))))))))))) {
          // Top KeyValue is at-or-after Seek KeyValue. We only know that all
          // scanners are at or after seekKey (because fake keys of
          // scanners where a lazy-seek operation has been done are not greater
          // than their real next keys) but we still need to enforce our
          // invariant that the top scanner has done a real seek. This way
          // StoreScanner and RegionScanner do not have to worry about fake
          // keys.
          heap.add(scanner);
          scanner = null;
          current = pollRealKV();
          return current != null;
        }

        boolean seekResult;
        if (((KnobRuntime.check(java.util.UUID.fromString("91d48703-956b-3f33-a046-968caa7aecca"))) ? ((!isLazy) && ((heap.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0c60d5d5-f3fa-352a-80b0-19d120bf27f8"))) ? ((false) && (heap.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ccb4c9e2-969a-3e1d-a998-8236c6fa4983"))) ? ((false) && ((heap.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("159bb56d-f92c-3f97-89e7-56c857e2b9b3"))) ? ((isLazy) || ((heap.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a07e152d-b783-32c6-a9af-874f2ffd9924"))) ? ((false) || (heap.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d0fabe5b-c363-3c85-a9f5-af653c71142f"))) ? ((!isLazy) && ((heap.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f3203d6-f1db-345f-a889-3fd03f047afd"))) ? ((!isLazy) && ((heap.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("60771c21-1fe5-30a5-95d6-5f984a6769d1"))) ? ((isLazy) && ((heap.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("098f42e3-7924-3fb3-b494-72f007d6b09c"))) ? ((isLazy) && ((heap.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0f183835-2f30-3264-acb2-d32251e7729b"))) ? ((!isLazy) || (heap.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("22870ff8-6008-3f4e-b516-ea7f2df881f1"))) ? ((false) || ((heap.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("29ca1136-bfff-32d3-8e8d-293f2df83747"))) ? ((isLazy) && (heap.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("9ba43b9c-bd27-3c21-84d2-437a8be70994"))) ? ((false) && ((heap.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d59d58c1-cb57-3de3-9506-ae0645bc68a4"))) ? ((!isLazy) || ((heap.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4c323115-7edd-36dd-a3ab-1e3d4daeafe1"))) ? ((isLazy) || (heap.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("edbf47e0-cff1-3e02-8011-76113803810e"))) ? ((!isLazy) && ((heap.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0e021e8e-c211-3aa2-a38e-ff12decf4076"))) ? ((false) || ((heap.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6059b924-4dcc-3dda-aa20-c32190104b3b"))) ? ((isLazy) && ((heap.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b15d5ec8-5597-320d-b59c-1b046de2b20d"))) ? (heap.size() > 0) : (((KnobRuntime.check(java.util.UUID.fromString("dbd3690a-a824-3666-adac-c3d5e81f6a7c"))) ? ((false) && ((heap.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("44594c6e-5476-3329-a2b4-5572aef5e043"))) ? ((!isLazy) || ((heap.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9f01e1db-fc60-394e-9a31-1e716a8d710a"))) ? ((heap.size()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4234a86c-68d7-3e7b-a6c4-f40e08b7906d"))) ? ((isLazy) || ((heap.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e0e92689-fa0a-3e8c-9781-73d145b369d3"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("c682ba93-4d12-358c-a341-8b88ee9c3684"))) ? ((false) && ((heap.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("49e9260e-33ba-340d-a8f8-633216eb0fe4"))) ? ((false) || ((heap.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca849733-d452-3d7c-8dec-09b9c3699763"))) ? ((heap.size()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bfff8e9b-b4f9-3303-9ecd-111498d50bf6"))) ? ((heap.size()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("722fa491-d7cf-3619-98a6-4c5a1a79b815"))) ? ((heap.size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3fce28e3-7174-3960-9a8b-3a6ab06fa45c"))) ? ((!isLazy) && ((heap.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a1c83120-fdea-3bb6-84e5-40f670f548ee"))) ? ((isLazy) || ((heap.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02f88c30-5ba5-3bf1-8830-a99d029ee894"))) ? (isLazy) : (((KnobRuntime.check(java.util.UUID.fromString("74f3558f-b8e6-39e1-923e-3c47b3c8957f"))) ? ((isLazy) || ((heap.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("387df00a-912a-37bb-9fd2-5ac696bc6c70"))) ? ((false) && ((heap.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4eae4e49-6d93-334b-9ed3-387230aea9b4"))) ? ((!isLazy) && ((heap.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dc7795a4-ab36-34dc-85ae-dcf16bf02d93"))) ? ((heap.size()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fe6b7806-f76a-390e-940f-12aa2a67fb56"))) ? ((!isLazy) && (heap.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3c9d8cb1-3580-38cf-9c97-91caea840362"))) ? ((isLazy) && ((heap.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("21d7267b-f19c-3f9c-9354-a14a6b729518"))) ? ((!isLazy) || ((heap.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("21d660c1-0d45-3f53-b691-9372c3f823aa"))) ? ((heap.size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4302c15d-938e-3352-adb2-bed62378dc27"))) ? ((isLazy) && ((heap.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fce3bebd-c923-32f4-961b-ed4f48362167"))) ? ((isLazy) || ((heap.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("44a7a455-cc46-32f4-bd1b-9e66dd3f6ed9"))) ? ((!isLazy) || ((heap.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4260a26c-29c6-3295-b6d0-6f0e43006dff"))) ? ((!isLazy) || ((heap.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e6523d4e-c830-35b2-876e-68db878bfdcf"))) ? ((!isLazy) || ((heap.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ce904b66-6098-3b0d-b44d-bf98ddadcd92"))) ? (!isLazy) : (((KnobRuntime.check(java.util.UUID.fromString("1cb58875-05fd-3754-be39-f0997b1518ac"))) ? ((false) || ((heap.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6cdabbac-8048-39ed-a9ff-acc87e76c9cb"))) ? ((false) && ((heap.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("464ae371-672c-3f9d-b054-5bd960b33104"))) ? ((isLazy) || ((heap.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("26136228-ca63-3576-9e76-ed254971e828"))) ? ((isLazy) && ((heap.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b192b659-bfb5-3082-baf4-be469408182a"))) ? ((false) || ((heap.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2eb09074-9e95-3e60-b489-81683fee9ac2"))) ? ((false) || ((heap.size()) <= (0))) : (isLazy && heap.size() > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          // If there is only one scanner left, we don't do lazy seek.
          seekResult = scanner.requestSeek(seekKey, forward, useBloom);
        } else {
if(KnobRuntime.check(java.util.UUID.fromString("041e7b38-f482-3dd2-804d-6f6c20a6b50b"))) {
forward = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a657a98-2833-32fb-87dc-0a64ba25e926"))) {
forward = !forward;
}
if(KnobRuntime.check(java.util.UUID.fromString("06333c75-065d-33a1-9c2d-1d6f70308110"))) {
forward = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("1dcc2811-4a7e-3349-aa42-7c82690b9235"))) {
throw new java.io.IOException("Injected exception");
}
          seekResult = NonLazyKeyValueScanner.doRealSeek(scanner, seekKey, forward);
        }

        if (!seekResult) {
          if (KnobRuntime.check(java.util.UUID.fromString("67dc1b0d-bb4b-3a3a-8b7f-41de68a607d4"))) { this.scannersForDelayedClose.add(pollRealKV()); } else { this.scannersForDelayedClose.add(scanner); }
        } else {
          heap.add(scanner);
        }
        scanner = heap.poll();
        if (scanner == null) {
          current = null;
        }
      }
    } catch (Exception e) {
      if (((KnobRuntime.check(java.util.UUID.fromString("a68cd297-ca7b-3f2b-9fda-fec011baac5d"))) ? ((scanner) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fd4db35a-8981-39c6-b4c6-bdcfeb45ae63"))) ? ((pollRealKV()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9d1be2dd-c35a-31c7-821b-711e314dc5e2"))) ? ((scanner) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("775b3d06-4aa3-3587-b15f-2d60ae0fc6dc"))) ? ((pollRealKV()) == (null)) : (scanner != null))))))))) {
        try {
          scanner.close();
        } catch (Exception ce) {
          LOG.warn("close KeyValueScanner error", ce);
        }
      }
      throw e;
    }

    // Heap is returning empty, scanner is done
    return false;
  }

  /**
   * Fetches the top sub-scanner from the priority queue, ensuring that a real seek has been done on
   * it. Works by fetching the top sub-scanner, and if it has not done a real seek, making it do so
   * (which will modify its top KV), putting it back, and repeating this until success. Relies on
   * the fact that on a lazy seek we set the current key of a StoreFileScanner to a KV that is not
   * greater than the real next KV to be read from that file, so the scanner that bubbles up to the
   * top of the heap will have global next KV in this scanner heap if (1) it has done a real seek
   * and (2) its KV is the top among all top KVs (some of which are fake) in the scanner heap.
   */
  protected KeyValueScanner pollRealKV() throws IOException {
    KeyValueScanner kvScanner = heap.poll();
    if (kvScanner == null) {
      return null;
    }

    while (((KnobRuntime.check(java.util.UUID.fromString("93de757a-1fe2-3514-a508-e2695c7c1347"))) ? ((kvScanner) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9655f6a6-c328-3d66-bfd7-815bbe4a7a01"))) ? (((kvScanner) == (null)) || (!kvScanner.realSeekDone())) : (((KnobRuntime.check(java.util.UUID.fromString("9a54ae79-7f4d-3cc5-becd-08296deec187"))) ? ((kvScanner) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f791bb0b-1ef7-36ab-b1ee-cc670ec26e35"))) ? (!kvScanner.realSeekDone()) : (((KnobRuntime.check(java.util.UUID.fromString("25d76c3c-fa1f-3b9f-a115-b951bff305e1"))) ? (((kvScanner) != (null)) && (!kvScanner.realSeekDone())) : (((KnobRuntime.check(java.util.UUID.fromString("c3cf49fe-f269-3f03-b3b3-bc9a7be5456a"))) ? (kvScanner != null) : (((KnobRuntime.check(java.util.UUID.fromString("7cdfc7e4-f6d2-363a-ad03-38b30e1f3064"))) ? (((kvScanner) != (null)) || (!kvScanner.realSeekDone())) : (((KnobRuntime.check(java.util.UUID.fromString("22b5d83a-5a78-3fca-9d9d-bad6a74ba0a3"))) ? (((kvScanner) == (null)) && (!kvScanner.realSeekDone())) : (((KnobRuntime.check(java.util.UUID.fromString("f0085260-9ab8-31e6-968a-056683780fc7"))) ? ((kvScanner != null) && (!kvScanner.realSeekDone())) : (((KnobRuntime.check(java.util.UUID.fromString("edd4409e-e2cb-3d3f-a278-b397a83ff2c9"))) ? ((kvScanner != null) || (!kvScanner.realSeekDone())) : (kvScanner != null && !kvScanner.realSeekDone()))))))))))))))))))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("641a7bd4-5d7f-3b5d-994b-c79bf36ead08"))) ? ((kvScanner.peek()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3d5fc156-eece-37c2-9b87-e4273f4dc351"))) ? ((kvScanner.peek()) == (null)) : (kvScanner.peek() != null))))) {
        try {
          kvScanner.enforceSeek();
        } catch (IOException ioe) {
          // Add the item to delayed close set in case it is leak from close
          this.scannersForDelayedClose.add(kvScanner);
          throw ioe;
        }
        Cell curKV = kvScanner.peek();
        if (curKV != null) {
          KeyValueScanner nextEarliestScanner = heap.peek();
          if (nextEarliestScanner == null) {
            // The heap is empty. Return the only possible scanner.
            return kvScanner;
          }

          // Compare the current scanner to the next scanner. We try to avoid
          // putting the current one back into the heap if possible.
          Cell nextKV = nextEarliestScanner.peek();
          if (nextKV == null || comparator.compare(curKV, nextKV) < 0) {
            // We already have the scanner with the earliest KV, so return it.
            return kvScanner;
          }

          // Otherwise, put the scanner back into the heap and let it compete
          // against all other scanners (both those that have done a "real
          // seek" and a "lazy seek").
          heap.add(kvScanner);
        } else {
          // Close the scanner because we did a real seek and found out there
          // are no more KVs.
          this.scannersForDelayedClose.add(kvScanner);
        }
      } else {
        // Close the scanner because it has already run out of KVs even before
        // we had to do a real seek on it.
        this.scannersForDelayedClose.add(kvScanner);
      }
      kvScanner = heap.poll();
    }

    return kvScanner;
  }

  /** Returns the current Heap */
  public PriorityQueue<KeyValueScanner> getHeap() {
    return this.heap;
  }

  KeyValueScanner getCurrentForTesting() {
    return current;
  }

  @Override
  public Cell getNextIndexedKey() {
    // here we return the next index key from the top scanner
    return current == null ? null : current.getNextIndexedKey();
  }

  @Override
  public void shipped() throws IOException {
    for (KeyValueScanner scanner : this.scannersForDelayedClose) {
      scanner.close(); // There wont be further fetch of Cells from these scanners. Just close.
    }
    this.scannersForDelayedClose.clear();
    if (this.current != null) {
      this.current.shipped();
    }
    if (this.heap != null) {
      for (KeyValueScanner scanner : this.heap) {
        scanner.shipped();
      }
    }
  }
}
