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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * KeyValueScanner adaptor over the Reader. It also provides hooks into bloom filter things.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.PHOENIX)
@InterfaceStability.Evolving
public class StoreFileScanner implements KeyValueScanner {
  // the reader it comes from:
  private final StoreFileReader reader;
  private final HFileScanner hfs;
  private Cell cur = null;
  private boolean closed = false;

  private boolean realSeekDone;
  private boolean delayedReseek;
  private Cell delayedSeekKV;

  private final boolean enforceMVCC;
  private final boolean hasMVCCInfo;
  // A flag represents whether could stop skipping KeyValues for MVCC
  // if have encountered the next row. Only used for reversed scan
  private boolean stopSkippingKVsIfNextRow = false;
  // A Cell that represents the row before the most previously seeked to row in seekToPreviousRow
  private Cell previousRow = null;
  // Whether the underlying HFile is using a data block encoding that has lower cost for seeking to
  // a row from the beginning of a block (i.e. RIV1). If the data block encoding has a high cost for
  // seeks, then we can use a modified reverse scanning algorithm to reduce seeks from the beginning
  // of the block
  private final boolean isFastSeekingEncoding;

  private static LongAdder seekCount;

  private final boolean canOptimizeForNonNullColumn;

  private final long readPt;

  // Order of this scanner relative to other scanners when duplicate key-value is found.
  // Higher values means scanner has newer data.
  private final long scannerOrder;

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param useMVCC                     If true, scanner will filter out updates with MVCC larger
   *                                    than {@code readPt}.
   * @param readPt                      MVCC value to use to filter out the updates newer than this
   *                                    scanner.
   * @param hasMVCC                     Set to true if underlying store file reader has MVCC info.
   * @param scannerOrder                Order of the scanner relative to other scanners. See
   *                                    {@link KeyValueScanner#getScannerOrder()}.
   * @param canOptimizeForNonNullColumn {@code true} if we can make sure there is no null column,
   *                                    otherwise {@code false}. This is a hint for optimization.
   * @param isFastSeekingEncoding       {@code true} if the data block encoding can seek quickly
   *                                    from the beginning of a block (i.e. RIV1), otherwise
   *                                    {@code false}. This is a hint for optimization.
   */
  public StoreFileScanner(StoreFileReader reader, HFileScanner hfs, boolean useMVCC,
    boolean hasMVCC, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn,
    boolean isFastSeekingEncoding) {
    this.readPt = readPt;
    this.reader = reader;
    this.hfs = hfs;
    this.enforceMVCC = useMVCC;
    this.hasMVCCInfo = hasMVCC;
    this.scannerOrder = scannerOrder;
    this.canOptimizeForNonNullColumn = canOptimizeForNonNullColumn;
    this.isFastSeekingEncoding = isFastSeekingEncoding;
    this.reader.incrementRefCount();
  }

  /**
   * Return an array of scanners corresponding to the given set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(Collection<HStoreFile> files,
    boolean cacheBlocks, boolean usePread, boolean isCompaction, boolean useDropBehind, long readPt)
    throws IOException {
    return getScannersForStoreFiles(files, cacheBlocks, usePread, isCompaction, useDropBehind, null,
      readPt);
  }

  /**
   * Return an array of scanners corresponding to the given set of store files, And set the
   * ScanQueryMatcher for each store file scanner for further optimization
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(Collection<HStoreFile> files,
    boolean cacheBlocks, boolean usePread, boolean isCompaction, boolean canUseDrop,
    ScanQueryMatcher matcher, long readPt) throws IOException {
    if (files.isEmpty()) {
      return Collections.emptyList();
    }
    List<StoreFileScanner> scanners = new ArrayList<>(files.size());
    boolean canOptimizeForNonNullColumn = matcher != null ? !matcher.hasNullColumnInQuery() : false;
    PriorityQueue<HStoreFile> sortedFiles =
      new PriorityQueue<>(files.size(), StoreFileComparators.SEQ_ID);
    for (HStoreFile file : files) {
      // The sort function needs metadata so we need to open reader first before sorting the list.
      file.initReader();
      sortedFiles.add(file);
    }
    boolean succ = false;
    try {
      for (int i = 0, n = files.size(); i < n; i++) {
        HStoreFile sf = sortedFiles.remove();
        StoreFileScanner scanner;
        if (usePread) {
if(KnobRuntime.check(java.util.UUID.fromString("cb2438e6-9d7b-3737-a0df-01a11b400b6a"))) {
canOptimizeForNonNullColumn = !canOptimizeForNonNullColumn;
}
          scanner = sf.getPreadScanner(cacheBlocks, readPt, i, canOptimizeForNonNullColumn);
        } else {
          scanner = sf.getStreamScanner(canUseDrop, cacheBlocks, isCompaction, readPt, i,
            canOptimizeForNonNullColumn);
        }
        scanners.add(scanner);
      }
      succ = true;
    } finally {
      if (!succ) {
        for (StoreFileScanner scanner : scanners) {
          scanner.close();
        }
      }
    }
    return scanners;
  }

  /**
   * Get scanners for compaction. We will create a separated reader for each store file to avoid
   * contention with normal read request.
   */
  public static List<StoreFileScanner> getScannersForCompaction(Collection<HStoreFile> files,
    boolean canUseDropBehind, long readPt) throws IOException {
    List<StoreFileScanner> scanners = new ArrayList<>(files.size());
    List<HStoreFile> sortedFiles = new ArrayList<>(files);
    Collections.sort(sortedFiles, StoreFileComparators.SEQ_ID);
    boolean succ = false;
    try {
      for (int i = 0, n = sortedFiles.size(); i < n; i++) {
        scanners.add(
          sortedFiles.get(i).getStreamScanner(canUseDropBehind, false, true, readPt, i, false));
      }
      succ = true;
    } finally {
      if (!succ) {
        for (StoreFileScanner scanner : scanners) {
          scanner.close();
        }
      }
    }
    return scanners;
  }

  @Override
  public String toString() {
    return "StoreFileScanner[" + hfs.toString() + ", cur=" + cur + "]";
  }

  @Override
  public Cell peek() {
    return cur;
  }

  @Override
  public Cell next() throws IOException {
    Cell retKey = cur;

    try {
      // only seek if we aren't at the end. cur == null implies 'end'.
      if (cur != null) {
        hfs.next();
        setCurrentCell(hfs.getCell());
        if (hasMVCCInfo || this.reader.isBulkLoaded()) {
          skipKVsNewerThanReadpoint();
        }
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  @Override
  public boolean seek(Cell key) throws IOException {
    if (seekCount != null) seekCount.increment();

    try {
      try {
        if (!seekAtOrAfter(hfs, key)) {
          this.cur = null;
          return false;
        }

        setCurrentCell(hfs.getCell());

        if (!hasMVCCInfo && this.reader.isBulkLoaded()) {
          return skipKVsNewerThanReadpoint();
        } else {
          return !hasMVCCInfo ? true : skipKVsNewerThanReadpoint();
        }
      } finally {
        realSeekDone = true;
        previousRow = null;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not seek " + this + " to key " + key, ioe);
    }
  }

  @Override
  public boolean reseek(Cell key) throws IOException {
    if (seekCount != null) seekCount.increment();

    try {
      try {
        if (!reseekAtOrAfter(hfs, key)) {
          this.cur = null;
          return false;
        }
        setCurrentCell(hfs.getCell());

        if (!hasMVCCInfo && this.reader.isBulkLoaded()) {
          return skipKVsNewerThanReadpoint();
        } else {
          return !hasMVCCInfo ? true : skipKVsNewerThanReadpoint();
        }
      } finally {
        realSeekDone = true;
        previousRow = null;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not reseek " + this + " to key " + key, ioe);
    }
  }

  protected void setCurrentCell(Cell newVal) throws IOException {
    this.cur = newVal;
    if (this.cur != null && this.reader.isBulkLoaded() && !this.reader.isSkipResetSeqId()) {
      PrivateCellUtil.setSequenceId(cur, this.reader.getSequenceID());
    }
  }

  protected boolean skipKVsNewerThanReadpoint() throws IOException {
    // We want to ignore all key-values that are newer than our current
    // readPoint
    Cell startKV = cur;
    while (enforceMVCC && cur != null && (cur.getSequenceId() > readPt)) {
      boolean hasNext = hfs.next();
      setCurrentCell(hfs.getCell());
      if (
        ((KnobRuntime.check(java.util.UUID.fromString("dab447e4-221a-372e-bdd2-a2edc977e3b7"))) ? (((hasNext) || (false)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("aaaa9c8e-3780-393f-a36e-e3d232978508"))) ? (((hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("76d7b914-dafb-3a3b-b1b1-ae1bb40fca16"))) ? (((false) && (false)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86cff069-6361-36ff-9a3e-a0bd64c50381"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f0e91c24-3f6d-3802-942c-045ba4e5f764"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ce046dc2-432d-35dd-bef9-7ae3f1d96f0d"))) ? (((!hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8d59682-72bf-34e4-b832-b244e109baf8"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("35ba6e63-5761-3c1d-807a-58835dcdeef4"))) ? (((true) && (true)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("64dbc7ad-5a03-310a-8061-0614fabf064b"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("920bb305-edb4-30e4-9ac2-98cd97d8c847"))) ? (((hasNext) && (true)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0cff3771-5577-3a55-a029-487907c9643b"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c2fd2874-a141-3cc1-a3be-726c507cc7a5"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a5f5f8a5-74fd-3260-933d-c2b1f4ec423b"))) ? (((hasNext) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3da676ba-369d-357f-bda3-37961be40c9e"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e21e2eb6-f0fb-3d1d-8a2f-2766fbbfe053"))) ? (((!hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8149b8a0-02b2-3a77-a4f6-c0a421f35116"))) ? (((!hasNext) || (true)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ecc0e643-0d54-3e80-8478-24d5aea45fae"))) ? (((false) && (false)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d726ecc2-5881-3aa7-9e61-f1dc8b21bd28"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("932a519c-3d9a-3644-a060-530325a540dd"))) ? (((hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("efec0a55-83df-346c-8b90-957c94f7a9b0"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c682ca34-ea03-34dd-9824-50e4b3d33a96"))) ? (((!hasNext) && (true)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("50533351-edc6-3a37-bdd8-64b6b8014b88"))) ? (((false) || (false)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4a8a8200-7ad1-346a-a3c4-5d89c43ad294"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5f98ffd2-fd4e-3f42-ab4c-6391da42e758"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d58db59e-8da9-3105-9238-a28429250ff8"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b871eb23-290e-3876-8bb0-540b9660120b"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c4a4a2a9-c7a4-3f90-b67f-603ebc0315d4"))) ? ((hasNext) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e3965e04-7671-38bc-bdf9-3e8c763b6d70"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cfe43a53-e9d5-31cc-8db5-9c5b7decd538"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("59ac4303-5dfd-3499-b6c8-d635d567831c"))) ? ((!hasNext) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b8dfa698-bbad-3bb3-95c8-3d4dc92eecff"))) ? ((!hasNext) || (false)) : (((KnobRuntime.check(java.util.UUID.fromString("9b855911-c663-3963-86c5-9e7c52ce2f0b"))) ? ((true) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b12759a5-b39c-34cf-980b-865621576f8c"))) ? ((!this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6687d843-079b-33e6-bd96-4937d6eddceb"))) ? (((hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("34dfd232-3556-3a7a-b464-9588ceb934ed"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("12257f8c-6e78-3860-89f0-3c3149cba4d5"))) ? (((hasNext) || (true)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("224b3df8-32d1-32be-bc48-05b8e6b43b7e"))) ? ((false) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d0065ce3-8038-3123-8a24-cc35f74bc20e"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5e783464-e1c2-3998-83af-63a4f43b4c4e"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ad462be-f631-37c5-a987-192b8e99ac14"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("50eaa936-d172-3fc0-a4aa-c60c6e74153d"))) ? ((!this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("717b4dbd-8dc6-39f4-9162-85b8a34aefb4"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6ca9c2a1-3ea1-394c-8ceb-7c2b52cdc564"))) ? (((false) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e442112f-c589-3868-af10-994ffd038416"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d9a44836-eb6c-32b1-84bd-8ff3d6b7c1c7"))) ? (((true) && (true)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("662f2eed-0db1-31ce-8975-ded8044f72da"))) ? (((true) || (true)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a9ffcd9e-8180-3bd8-b493-9e259521e3fe"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39b90560-0e57-311c-a31a-2527ae7a2da5"))) ? (((false) && (false)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2f8e33ae-2796-3b11-b151-813979a87238"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eafc9a00-a864-36db-9ca8-e514a4fd93dd"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f3b761a-13f4-3350-9665-d4ee6f27907e"))) ? (((false) && (false)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d0dfb0b6-2698-31bd-ab25-58b5c47a1eec"))) ? (((true) && (true)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("66738f74-371c-30ce-9736-892f287f15f8"))) ? (((false) && (true)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0090bfb3-9c8b-31b2-8cea-db51d11ff80d"))) ? ((true) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7d03f333-9695-3436-977e-f569360958a4"))) ? (((false) || (true)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f8009f82-449f-3558-a614-0735043cd6b0"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cbb30df6-da5e-3cf0-baff-a4759a71509c"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ed525ebd-02da-3d4e-b3b7-57b93c494a7c"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a0790fc9-0285-3775-9d74-c365658e3820"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b728d23a-0f94-39a0-b0fc-286849320acb"))) ? (((true) || (false)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("58424b73-7094-33f3-9356-7d21f9e9d2e8"))) ? ((true) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8dba19df-b8df-3af3-bc41-a2b232223e59"))) ? ((!hasNext) && (!this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("028439f3-ea4d-3daa-b760-bed7c15c7c05"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3be53702-fdcb-3185-9371-590958853381"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("43685850-aff5-3986-b9da-3749444c20e9"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("64d36034-a974-37f3-a6f0-ba866b0d89e7"))) ? ((false) && ((getComparator().compareRows(cur, startKV)) == (0))) : (hasNext && this.stopSkippingKVsIfNextRow && getComparator().compareRows(cur, startKV) > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
      ) {
        return false;
      }
    }

    if (cur == null) {
      return false;
    }

    return true;
  }

  @Override
  public void close() {
    if (closed) return;
    cur = null;
    this.hfs.close();
    if (this.reader != null) {
      this.reader.readCompleted();
    }
    closed = true;
  }

  /** Returns false if not found or if k is after the end. */
  public static boolean seekAtOrAfter(HFileScanner s, Cell k) throws IOException {
    int result = s.seekTo(k);
    if (result < 0) {
      if (result == HConstants.INDEX_KEY_MAGIC) {
        // using faked key
        return true;
      }
      // Passed KV is smaller than first KV in file, work from start of file
      return s.seekTo();
    } else if (result > 0) {
      // Passed KV is larger than current KV in file, if there is a next
      // it is the "after", if not then this scanner is done.
      return s.next();
    }
    // Seeked to the exact key
    return true;
  }

  static boolean reseekAtOrAfter(HFileScanner s, Cell k) throws IOException {
    // This function is similar to seekAtOrAfter function
    int result = s.reseekTo(k);
    if (result <= 0) {
      if (result == HConstants.INDEX_KEY_MAGIC) {
        // using faked key
        return true;
      }
      // If up to now scanner is not seeked yet, this means passed KV is smaller
      // than first KV in file, and it is the first time we seek on this file.
      // So we also need to work from the start of file.
      if (!s.isSeeked()) {
        return s.seekTo();
      }
      return true;
    }
    // passed KV is larger than current KV in file, if there is a next
    // it is after, if not then this scanner is done.
    return s.next();
  }

  /**
   * @see KeyValueScanner#getScannerOrder()
   */
  @Override
  public long getScannerOrder() {
    return scannerOrder;
  }

  /**
   * Pretend we have done a seek but don't do it yet, if possible. The hope is that we find
   * requested columns in more recent files and won't have to seek in older files. Creates a fake
   * key/value with the given row/column and the highest (most recent) possible timestamp we might
   * get from this file. When users of such "lazy scanner" need to know the next KV precisely (e.g.
   * when this scanner is at the top of the heap), they run {@link #enforceSeek()}.
   * <p>
   * Note that this function does guarantee that the current KV of this scanner will be advanced to
   * at least the given KV. Because of this, it does have to do a real seek in cases when the seek
   * timestamp is older than the highest timestamp of the file, e.g. when we are trying to seek to
   * the next row/column and use OLDEST_TIMESTAMP in the seek key.
   */
  @Override
  public boolean requestSeek(Cell kv, boolean forward, boolean useBloom) throws IOException {
    if (kv.getFamilyLength() == 0) {
      useBloom = false;
    }

    boolean haveToSeek = true;
    if (useBloom) {
      // check ROWCOL Bloom filter first.
      if (reader.getBloomFilterType() == BloomType.ROWCOL) {
        haveToSeek = reader.passesGeneralRowColBloomFilter(kv);
      } else if (
        canOptimizeForNonNullColumn
          && ((PrivateCellUtil.isDeleteFamily(kv) || PrivateCellUtil.isDeleteFamilyVersion(kv)))
      ) {
        // if there is no such delete family kv in the store file,
        // then no need to seek.
        haveToSeek = reader.passesDeleteFamilyBloomFilter(kv.getRowArray(), kv.getRowOffset(),
          kv.getRowLength());
      }
    }

    delayedReseek = forward;
    delayedSeekKV = kv;

    if (haveToSeek) {
      // This row/column might be in this store file (or we did not use the
      // Bloom filter), so we still need to seek.
      realSeekDone = false;
      long maxTimestampInFile = reader.getMaxTimestamp();
      long seekTimestamp = kv.getTimestamp();
      if (seekTimestamp > maxTimestampInFile) {
        // Create a fake key that is not greater than the real next key.
        // (Lower timestamps correspond to higher KVs.)
        // To understand this better, consider that we are asked to seek to
        // a higher timestamp than the max timestamp in this file. We know that
        // the next point when we have to consider this file again is when we
        // pass the max timestamp of this file (with the same row/column).
        setCurrentCell(PrivateCellUtil.createFirstOnRowColTS(kv, maxTimestampInFile));
      } else {
        // This will be the case e.g. when we need to seek to the next
        // row/column, and we don't know exactly what they are, so we set the
        // seek key's timestamp to OLDEST_TIMESTAMP to skip the rest of this
        // row/column.
        enforceSeek();
      }
      return cur != null;
    }

    // Multi-column Bloom filter optimization.
    // Create a fake key/value, so that this scanner only bubbles up to the top
    // of the KeyValueHeap in StoreScanner after we scanned this row/column in
    // all other store files. The query matcher will then just skip this fake
    // key/value and the store scanner will progress to the next column. This
    // is obviously not a "real real" seek, but unlike the fake KV earlier in
    // this method, we want this to be propagated to ScanQueryMatcher.
    setCurrentCell(PrivateCellUtil.createLastOnRowCol(kv));

    realSeekDone = true;
    return true;
  }

  StoreFileReader getReader() {
    return reader;
  }

  CellComparator getComparator() {
    return reader.getComparator();
  }

  @Override
  public boolean realSeekDone() {
    return realSeekDone;
  }

  @Override
  public void enforceSeek() throws IOException {
    if (realSeekDone) return;

    if (delayedReseek) {
      reseek(delayedSeekKV);
    } else {
      seek(delayedSeekKV);
    }
  }

  @Override
  public boolean isFileScanner() {
    return true;
  }

  @Override
  public void recordBlockSize(IntConsumer blockSizeConsumer) {
    hfs.recordBlockSize(blockSizeConsumer);
  }

  @Override
  public Path getFilePath() {
    return reader.getHFileReader().getPath();
  }

  // Test methods
  static final long getSeekCount() {
    return seekCount.sum();
  }

  static final void instrument() {
    seekCount = new LongAdder();
  }

  @Override
  public boolean shouldUseScanner(Scan scan, HStore store, long oldestUnexpiredTS) {
    // if the file has no entries, no need to validate or create a scanner.
    byte[] cf = store.getColumnFamilyDescriptor().getName();
    TimeRange timeRange = scan.getColumnFamilyTimeRange().get(cf);
    if (timeRange == null) {
      timeRange = scan.getTimeRange();
    }
if(KnobRuntime.check(java.util.UUID.fromString("9f586d24-0b66-3c49-b142-c89b2f7a89f3"))) {
try {
    java.lang.reflect.Field _knob_field_ = timeRange.getClass().getDeclaredField("maxStamp");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(timeRange));
    _knob_field_.set(timeRange, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return reader.passesTimerangeFilter(timeRange, oldestUnexpiredTS)
      && reader.passesKeyRangeFilter(scan)
      && reader.passesBloomFilter(scan, scan.getFamilyMap().get(cf));
  }

  @Override
  public boolean seekToPreviousRow(Cell originalKey) throws IOException {
    try {
      if (isFastSeekingEncoding) {
        return seekToPreviousRowStateless(originalKey);
      } else if (previousRow == null || getComparator().compareRows(previousRow, originalKey) > 0) {
        return seekToPreviousRowWithoutHint(originalKey);
      } else {
        return seekToPreviousRowWithHint();
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not seekToPreviousRow " + this + " to key " + originalKey, ioe);
    } finally {
      this.realSeekDone = true;
    }
  }

  /**
   * This variant of the {@link StoreFileScanner#seekToPreviousRow(Cell)} method requires one seek
   * and one reseek. This method maintains state in {@link StoreFileScanner#previousRow} which only
   * makes sense in the context of a sequential row-by-row reverse scan.
   * {@link StoreFileScanner#previousRow} should be reset if that is not the case. The reasoning for
   * why this method is faster than {@link StoreFileScanner#seekToPreviousRowStateless(Cell)} is
   * that seeks are slower as they need to start from the beginning of the file, while reseeks go
   * forward from the current position.
   */
  private boolean seekToPreviousRowWithHint() throws IOException {
    do {
      // Using our existing seek hint, set our next seek hint
      Cell firstKeyOfPreviousRow = PrivateCellUtil.createFirstOnRow(previousRow);
      seekBeforeAndSaveKeyToPreviousRow(firstKeyOfPreviousRow);

      // Reseek back to our initial seek hint (i.e. what we think is the start of the
      // previous row)
      if (!reseekAtOrAfter(firstKeyOfPreviousRow)) {
        return false;
      }

      // If after skipping newer Kvs, we're still in our seek hint row, then we're finished
      if (isStillAtSeekTargetAfterSkippingNewerKvs(firstKeyOfPreviousRow)) {
        return true;
      }

      // If the previousRow seek hint is missing, that means that we're at row after the first row
      // in the storefile. Use the without-hint seek path to process the final row
      if (previousRow == null) {
        return seekToPreviousRowWithoutHint(firstKeyOfPreviousRow);
      }

      // Otherwise, use the previousRow seek hint to continue traversing backwards
    } while (true);
  }

  /**
   * This variant of the {@link StoreFileScanner#seekToPreviousRow(Cell)} method requires two seeks
   * and one reseek. The extra expense/seek is with the intent of speeding up subsequent calls by
   * using the {@link StoreFileScanner#seekToPreviousRowWithHint} which this method seeds the state
   * for by setting {@link StoreFileScanner#previousRow}
   */
  private boolean seekToPreviousRowWithoutHint(Cell originalKey) throws IOException {
    // Rewind to the cell before the beginning of this row
    Cell keyAtBeginningOfRow = PrivateCellUtil.createFirstOnRow(originalKey);
    if (!seekBefore(keyAtBeginningOfRow)) {
      return false;
    }

    // Rewind before this row and save what we find as a seek hint
    Cell firstKeyOfPreviousRow = PrivateCellUtil.createFirstOnRow(hfs.getCell());
    seekBeforeAndSaveKeyToPreviousRow(firstKeyOfPreviousRow);

    // Seek back to the start of the previous row
    if (!reseekAtOrAfter(firstKeyOfPreviousRow)) {
      return false;
    }

    // If after skipping newer Kvs, we're still in what we thought was the previous
    // row, then we can exit
    if (isStillAtSeekTargetAfterSkippingNewerKvs(firstKeyOfPreviousRow)) {
      return true;
    }

    // Skipping newer kvs resulted in skipping the entire row that we thought was the
    // previous row. If we've set a seek hint, then we can use that to go backwards
    // further
    if (previousRow != null) {
      return seekToPreviousRowWithHint();
    }

    // If we've made it here, then we weren't able to set a seek hint. This can happen
    // only if we're at the beginning of the storefile i.e. there is no row before this
    // one
    return false;
  }

  /**
   * This variant of the {@link StoreFileScanner#seekToPreviousRow(Cell)} method requires two seeks.
   * It should be used if the cost for seeking is lower i.e. when using a fast seeking data block
   * encoding like RIV1.
   */
  private boolean seekToPreviousRowStateless(Cell originalKey) throws IOException {
    Cell key = originalKey;
    do {
      Cell keyAtBeginningOfRow = PrivateCellUtil.createFirstOnRow(key);
      if (!seekBefore(keyAtBeginningOfRow)) {
        return false;
      }

      Cell firstKeyOfPreviousRow = PrivateCellUtil.createFirstOnRow(hfs.getCell());
      if (!seekAtOrAfter(firstKeyOfPreviousRow)) {
        return false;
      }

      if (isStillAtSeekTargetAfterSkippingNewerKvs(firstKeyOfPreviousRow)) {
        return true;
      }
      key = firstKeyOfPreviousRow;
    } while (true);
  }

  private boolean seekBefore(Cell seekKey) throws IOException {
    if (seekCount != null) {
      seekCount.increment();
    }
    if (!hfs.seekBefore(seekKey)) {
      this.cur = null;
      return false;
    }

    return true;
  }

  /**
   * Seeks before the seek target cell and saves the location to {@link #previousRow}. If there
   * doesn't exist a KV in this file before the seek target cell, reposition the scanner at the
   * beginning of the storefile (in preparation to a reseek at or after the seek key) and set the
   * {@link #previousRow} to null. If {@link #previousRow} is ever non-null and then transitions to
   * being null again via this method, that's because there doesn't exist a row before the seek
   * target in the storefile (i.e. we're at the beginning of the storefile)
   */
  private void seekBeforeAndSaveKeyToPreviousRow(Cell seekKey) throws IOException {
    if (seekCount != null) {
      seekCount.increment();
    }
    if (!hfs.seekBefore(seekKey)) {
      // Since the above seek failed, we need to position ourselves back at the start of the
      // block or else our reseek might fail. seekTo() cannot return false here as at least
      // one seekBefore will have returned true by the time we get here
      hfs.seekTo();
      this.previousRow = null;
    } else {
      this.previousRow = hfs.getCell();
    }
  }

  private boolean seekAtOrAfter(Cell seekKey) throws IOException {
    if (seekCount != null) {
      seekCount.increment();
    }
    if (!seekAtOrAfter(hfs, seekKey)) {
      this.cur = null;
      return false;
    }

    return true;
  }

  private boolean reseekAtOrAfter(Cell seekKey) throws IOException {
    if (seekCount != null) {
      seekCount.increment();
    }
    if (!reseekAtOrAfter(hfs, seekKey)) {
      this.cur = null;
      return false;
    }

    return true;
  }

  private boolean isStillAtSeekTargetAfterSkippingNewerKvs(Cell seekKey) throws IOException {
    setCurrentCell(hfs.getCell());
    return skipKvsNewerThanReadpointReversed() && getComparator().compareRows(cur, seekKey) <= 0;
  }

  private boolean skipKvsNewerThanReadpointReversed() throws IOException {
    this.stopSkippingKVsIfNextRow = true;
    boolean resultOfSkipKVs;
    try {
      resultOfSkipKVs = skipKVsNewerThanReadpoint();
    } finally {
      this.stopSkippingKVsIfNextRow = false;
    }

    return resultOfSkipKVs;
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    Optional<byte[]> lastRow = reader.getLastRowKey();
    if (!lastRow.isPresent()) {
      return false;
    }
    Cell seekKey = PrivateCellUtil.createFirstOnRow(lastRow.get());
    if (seek(seekKey)) {
      return true;
    } else {
      return seekToPreviousRow(seekKey);
    }
  }

  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    seek(key);
    if (cur == null || getComparator().compareRows(cur, key) > 0) {
      return seekToPreviousRow(key);
    }
    return true;
  }

  @Override
  public Cell getNextIndexedKey() {
    return hfs.getNextIndexedKey();
  }

  @Override
  public void shipped() throws IOException {
    this.hfs.shipped();
  }
}
