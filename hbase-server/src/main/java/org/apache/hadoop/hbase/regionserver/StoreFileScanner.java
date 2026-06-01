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
    if (KnobRuntime.check(java.util.UUID.fromString("6496715c-5228-3f34-bcef-077ee100be13"))) { this.reader.readCompleted(); } else { this.reader.incrementRefCount(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("561c3ec8-4735-386a-b120-0110a3a5e24e"))) {
throw new java.io.IOException("Injected exception");
}
      file.initReader();
if(KnobRuntime.check(java.util.UUID.fromString("a0be0cc6-5cc3-3359-a67b-3280e264a7f1"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("excludeFromMinorCompaction");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(file);
    field.set(file, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec783262-86bf-346a-9922-22cdca03e878"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("compactedAway");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(file);
    field.set(file, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7405ca26-20c2-3522-bb03-49ca5a41f2f8"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed86dfc9-a615-3b11-8677-55c73ea527fd"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("isHistorical");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(file);
    field.set(file, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4baa21c1-911a-35d5-9bea-e2a4423ebab3"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f2ed7e3d-a360-3f4b-a913-bf86a39d6800"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98626c8b-3995-32b0-90f4-23694a19c823"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("maxMemstoreTS");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      sortedFiles.add(file);
    }
    boolean succ = false;
    try {
      for (int i = 0, n = files.size(); i < n; i++) {
        HStoreFile sf = sortedFiles.remove();
        StoreFileScanner scanner;
        if (usePread) {
if(KnobRuntime.check(java.util.UUID.fromString("93def5b4-06ce-35af-aeb0-0dbf8f5ce8b2"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("68f087e1-ea63-38aa-9543-5fdb58560d3f"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3d77a908-c802-34e9-b4bc-8f8e5daebc9e"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb2438e6-9d7b-3737-a0df-01a11b400b6a"))) {
canOptimizeForNonNullColumn = !canOptimizeForNonNullColumn;
}
if(KnobRuntime.check(java.util.UUID.fromString("83fded4e-6614-3c9a-943d-7e81736c6acf"))) {
canOptimizeForNonNullColumn = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("f113506f-6d2f-3688-9919-02d7fa6afeed"))) {
readPt = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3eff3a42-8577-36b7-bef7-23048ed6238b"))) {
cacheBlocks = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9a33b130-fc3d-30f9-9fdd-3b56e2bf3936"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9518788-9f2c-3508-9688-0888e5e9673c"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("737f668f-d9c7-3d59-ae3a-e27cfb4bce11"))) {
readPt = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d898b370-974d-3e2f-a1bc-59ddfe1ac775"))) {
cacheBlocks = !cacheBlocks;
}
if(KnobRuntime.check(java.util.UUID.fromString("c80ee6c9-c6d9-3736-bf8f-5555e790e2c1"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0db46634-1eab-322b-99c2-bfe5097c3946"))) {
canOptimizeForNonNullColumn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf29d7e5-d08c-31e2-984d-fdd8aec117dd"))) {
cacheBlocks = true;
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
      if (((KnobRuntime.check(java.util.UUID.fromString("e597326b-af80-31ac-a998-440aa389bb61"))) ? ((cur) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d22f8b79-324f-350c-8ba3-c88e468dba05"))) ? ((cur) != (null)) : (cur != null))))) {
        hfs.next();
        setCurrentCell(hfs.getCell());
        if (hasMVCCInfo || this.reader.isBulkLoaded()) {
if(KnobRuntime.check(java.util.UUID.fromString("3f207491-037c-377c-99bd-7e888638e626"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("c972765a-575f-3658-bd85-392d2b3a5fa1"))) {
throw new java.io.IOException("Injected exception");
}
        setCurrentCell(hfs.getCell());

        if (((KnobRuntime.check(java.util.UUID.fromString("9731882b-66b8-3d6e-8337-f9144a9955ec"))) ? (this.reader.isBulkLoaded()) : (((KnobRuntime.check(java.util.UUID.fromString("6641896a-3550-316e-84a8-09fb12bc0993"))) ? (skipKVsNewerThanReadpoint()) : (((KnobRuntime.check(java.util.UUID.fromString("7dacdcfb-135e-3203-9565-580419ccaa03"))) ? ((!hasMVCCInfo) && (this.reader.isBulkLoaded())) : (((KnobRuntime.check(java.util.UUID.fromString("bf7a7442-1e16-3393-8da9-1db29af1ef44"))) ? ((!hasMVCCInfo) && (skipKVsNewerThanReadpoint())) : (((KnobRuntime.check(java.util.UUID.fromString("a832e616-e90f-34b9-a4af-bb45f0371b32"))) ? (!hasMVCCInfo) : (((KnobRuntime.check(java.util.UUID.fromString("c399fb74-8550-3f58-bb82-600f35289819"))) ? ((!hasMVCCInfo) || (this.reader.isBulkLoaded())) : (((KnobRuntime.check(java.util.UUID.fromString("60d9cfce-afcd-34df-8f16-13de1821432f"))) ? ((!hasMVCCInfo) || (skipKVsNewerThanReadpoint())) : (!hasMVCCInfo && this.reader.isBulkLoaded()))))))))))))))) {
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
        ((KnobRuntime.check(java.util.UUID.fromString("43685850-aff5-3986-b9da-3749444c20e9"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("458f5c26-be03-31e5-98f6-1cb275d40516"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("526dfc0e-0a2e-3195-a7de-a502b919f929"))) ? (((false) || (false)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d9ffe092-c60e-33a0-a736-13b8ac7f8870"))) ? (((false) && (true)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ea1cff36-a5dc-3f05-85d3-4141c20651bf"))) ? ((getComparator().compareRows(cur, startKV)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9840a551-6df1-3253-be94-349b35334bcc"))) ? ((this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7ddf5d87-0b82-35d8-8548-ec3380b68ece"))) ? (((true) || (true)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bf171f32-c471-34c1-9249-73f38a712910"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5f6a927d-228b-3584-aabf-57240524650e"))) ? ((false) && (!this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("619e3acd-3d89-3024-a25a-0414559ce6ef"))) ? ((false) || (false)) : (((KnobRuntime.check(java.util.UUID.fromString("ce70501e-0290-3eea-ac11-ebdafb0d16e0"))) ? ((!hasNext) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("76d7b914-dafb-3a3b-b1b1-ae1bb40fca16"))) ? (((false) && (false)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac9bb7ac-995f-3856-84c0-c93958af9af5"))) ? (((false) || (true)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("801f18b6-982f-3eea-b41e-11170c2c2d7f"))) ? (((false) && (true)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8a1a0a1-47e1-31ce-a799-9dc82dd24382"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("767bdc5f-dc09-3734-beb6-369577109f26"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("028439f3-ea4d-3daa-b760-bed7c15c7c05"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1864d591-6838-31d9-94c2-e3cd8d937e63"))) ? (((false) && (true)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4f9bf8c3-898a-3e67-8894-aae8e0ebb79e"))) ? (((true) && (false)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("80f89c4d-e363-3516-9220-da1b8c17938c"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b728d23a-0f94-39a0-b0fc-286849320acb"))) ? (((true) || (false)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("619aaddc-57fe-3039-ae93-fdd47fbbfe05"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e7626e24-d5fd-3fe5-bc78-c82285c66686"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("712ecafd-9103-3872-a6be-0cf57f0bb0a4"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("daef3a86-78ec-3069-acdb-cbb9bd2c045b"))) ? (((!hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2ccb189e-0b1e-38df-b5bc-72b29a2ca6d1"))) ? ((!this.stopSkippingKVsIfNextRow) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("eefd2b7e-4a04-3b28-8643-4d55f714deb1"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ef4dad38-e98c-3e92-ae27-7ef3d4b66620"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bb7c2302-bd99-3a0a-87ab-f3b6a7702f6f"))) ? (((hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("93ae8b9e-6677-3610-b0b5-9f867197c00a"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ed0d9c51-2af8-37fe-92c0-f10448ae9dff"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ecc0e643-0d54-3e80-8478-24d5aea45fae"))) ? (((false) && (false)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ad462be-f631-37c5-a987-192b8e99ac14"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("efeb5aca-29dd-3fb7-91ad-27f633d54a63"))) ? (((hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d9effe44-38ea-3071-a4ea-98c510695b93"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0df67958-7129-3635-bb99-2393d2faabe2"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a007c721-ae55-32c3-917e-ba207dc338a2"))) ? (((false) && (true)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("08a96bc5-5e05-30ea-b919-3f21dd4aa22d"))) ? ((this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bd9138d1-cff6-302d-9b7f-cea075cdb04d"))) ? (((hasNext) || (true)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dab447e4-221a-372e-bdd2-a2edc977e3b7"))) ? (((hasNext) || (false)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("55974c1a-7a31-382c-8502-f996d86fe172"))) ? (((!hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b214ae33-8581-3d46-91ed-333d832fba0c"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("30d08349-984d-302e-8649-0d80b0a5227d"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("065ad04c-39f2-3398-81d9-abdd87efe692"))) ? ((getComparator().compareRows(cur, startKV)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2136b850-1c31-3b9a-a969-0f20037d0ac9"))) ? (((true) && (false)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c2c3136-77d3-308d-bbcc-ef1f16b7256e"))) ? ((hasNext) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("4ab2e5d4-1b0c-3c42-80b2-42b5835aa8e4"))) ? (((true) && (true)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a0f03458-f483-3041-a8fc-07de74102970"))) ? (((!hasNext) || (true)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4584cddc-b0d8-375e-8543-abf9d0a235db"))) ? (((!hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("505073e6-8651-3420-b086-106579d2d702"))) ? (((true) || (true)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0d753c2b-0f20-347e-b5ef-19dd7c6ec9d4"))) ? (((!hasNext) || (true)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("877c31e7-176d-3bd3-91a8-3b242d04a21d"))) ? (((false) && (true)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7be72482-f300-3981-b2cc-f79f1deef0ec"))) ? (((hasNext) && (false)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a36345f7-31f9-3976-a825-bd3a75dc951f"))) ? (((!hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("598a3288-38fd-31c1-beeb-54ca1b3fc2b6"))) ? ((true) || (true)) : (((KnobRuntime.check(java.util.UUID.fromString("db0429c0-f0c6-3c65-b55a-15887badfc67"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86731011-d93b-3e12-b55e-ab8030c0dadc"))) ? (((!hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("430f67d6-5598-3528-9ccd-1e03840175df"))) ? (((hasNext) && (true)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("64d36034-a974-37f3-a6f0-ba866b0d89e7"))) ? ((false) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bc33b17c-02b9-368d-9de4-e136e8759b44"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("66c83661-e324-3ac9-9e0a-f971e374d57b"))) ? ((this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2c2f556a-3e14-3fb1-a08e-0399a1fe260d"))) ? ((!this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e2510bc5-f7a9-3174-8fbe-b28ba8e48b95"))) ? ((!hasNext) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a574b49f-8929-3798-b348-28791697391e"))) ? (((false) || (true)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3fd299a8-19c9-3b6a-ae4b-8221bd2ca58a"))) ? (this.stopSkippingKVsIfNextRow) : (((KnobRuntime.check(java.util.UUID.fromString("16758ee8-94c0-3735-b172-f1d62b664419"))) ? (((false) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("773cbf4e-ac95-3c8e-a78f-21514f1252e2"))) ? (((true) && (false)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b04b762c-860e-3aae-a696-40a6890a17cc"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9fcdaefe-9fd6-399c-a598-fd6c3fe7ca04"))) ? (((false) || (false)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d051a9b7-a0c3-3d95-8219-ddfb77e2eb75"))) ? ((this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a2fcbe78-c666-3d08-95b0-c10583847b53"))) ? (((true) && (false)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f76685ef-6d95-3cad-a828-64079cb874d1"))) ? ((!hasNext) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("46078000-592c-3a87-b49d-213dc4ebf538"))) ? (((!hasNext) && (false)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5f36819c-4598-3cdd-b374-3776ff8de875"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("36c62667-71ec-3a13-aff2-47314e619090"))) ? (((hasNext) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86f732bb-038b-3e53-a725-d1830b0db775"))) ? (((false) && (false)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ecbf1784-1455-3aa4-8380-39c281553ce2"))) ? (((hasNext) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8443cd07-52f7-3af6-a36a-db286685aa63"))) ? ((false) || (this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("4352c1fb-ac99-302a-92b4-86e4f2243a5a"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("44054880-dab9-35e3-8e12-7b5a49c0bdee"))) ? ((!this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e6b47004-ecec-30c0-90ba-f76675aa4b2d"))) ? (((!hasNext) && (false)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("44c94cda-4a2e-3d6b-ab60-aac1c6ba4da0"))) ? ((hasNext) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("29917065-b7db-30ab-b6f6-5101614f7f5e"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38b1b1a6-d0d5-3084-8974-eb54d738ef94"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2e041f66-d2f5-3dd3-ae65-5c5b6dd38efc"))) ? ((!hasNext) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1ecbf626-f8b6-31dc-a7a8-dfb8b49bf7e8"))) ? ((!this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("34f855e6-657c-371b-83c0-3b5d036009b0"))) ? (((true) || (true)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1522634d-8256-3944-bf8e-9c55e54521cb"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9e60806e-6aa6-3470-84c3-800e6c841fa4"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("01fcb887-16b7-3eeb-9170-6104e5c87e3b"))) ? ((hasNext) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("18ff87f8-94b2-3014-befc-0b40b5b0057c"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bd39cf8d-c547-3e3a-b2fb-824fc07e5670"))) ? (((hasNext) && (false)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a44c73fa-d565-3690-b529-50f1d58ab5dc"))) ? ((false) || (!this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("f25a911b-f214-3bda-b9aa-05834fb0965a"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("50533351-edc6-3a37-bdd8-64b6b8014b88"))) ? (((false) || (false)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9daa8f20-1081-3c02-a218-5641ceb8f195"))) ? ((false) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("44dabba8-8552-3cba-bd7d-f4852becc3bc"))) ? (((true) || (false)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a202ec6-0f95-38ee-9fcd-43dd33a82982"))) ? (((hasNext) && (true)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5ec949c4-9b4e-3058-8b55-abe3d8ffe653"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c64e451-4fda-37ef-b948-8753e8cccb55"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("70ef0f7f-feea-36f8-9684-947ee1b530d7"))) ? ((getComparator().compareRows(cur, startKV)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ef74ed5a-dee6-3e44-a558-34f7da2b6ae1"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bedff444-38cb-3b16-9c7b-ca37ce31ad74"))) ? (((!hasNext) && (false)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("209e06fa-1c72-3d58-9c82-5b3440d35340"))) ? (((true) && (false)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6f1755d0-ee57-347f-aeca-8436da592717"))) ? (((!hasNext) && (false)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("26be0b0d-b998-32da-960b-7391a330e7e8"))) ? (((true) && (false)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ade08a2-49d2-39b5-92db-d70149891776"))) ? (((hasNext) || (true)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a0790fc9-0285-3775-9d74-c365658e3820"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca1d98ff-9c70-3e4e-83a7-b05ba0a67430"))) ? ((!hasNext) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3bb92d92-0de7-3115-9b29-25695fe5f8b7"))) ? (((!hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0b711d1a-6510-3770-8264-c8a505423c69"))) ? (((false) || (true)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86ca8d2a-af1e-33e7-a66c-0825cb9cd154"))) ? (((hasNext) || (this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7dcc165b-3145-34ef-b161-b518ffab1f51"))) ? (!this.stopSkippingKVsIfNextRow) : (((KnobRuntime.check(java.util.UUID.fromString("e8bc306e-22a7-32f7-be1c-2c0dc890ecd3"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5b2cf079-41f3-39a1-9620-9d19e1c59faa"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1ada2ac2-98bf-3c16-8ae8-2ecbd6fa3891"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7cc22017-40b1-3b53-9f43-2c06afb0418b"))) ? (((!hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3b089e11-5d28-376f-b294-4f29c6e00235"))) ? (((true) || (false)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("44bf42e4-1576-3473-b8bc-38542da57664"))) ? (((false) || (false)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c0aff846-bcfe-3239-8ce8-92a66f877834"))) ? ((true) && (true)) : (((KnobRuntime.check(java.util.UUID.fromString("d32ddc7e-8922-393a-8245-93528dff0505"))) ? (((!hasNext) || (true)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("98fd4dcd-109c-36fb-a1d8-7cd7af78c974"))) ? ((!this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("53cedcbf-8640-3684-8bcd-252a028d77e1"))) ? (((false) || (this.stopSkippingKVsIfNextRow)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("aaaa9c8e-3780-393f-a36e-e3d232978508"))) ? (((hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b4d56c57-9d56-372c-926e-56c828b4c2c6"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a851e724-925a-3ed9-a336-a225abe65a9f"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b30d50b7-f71d-326b-a1ae-f76c23aab4ba"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b0fc15db-630d-37b4-a252-8174d04e4e94"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6ca9c2a1-3ea1-394c-8ceb-7c2b52cdc564"))) ? (((false) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("58058c69-3f5d-3fb7-8acf-a43291ff087c"))) ? (((hasNext) && (false)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("808842ce-bb47-33a9-aced-a6963e41e94d"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("224b3df8-32d1-32be-bc48-05b8e6b43b7e"))) ? ((false) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("96530672-4840-306e-a645-099c0d3cce3c"))) ? ((true) && (this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("f91e5f67-8568-30d7-8ead-71380769c8d9"))) ? (((hasNext) && (false)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("35baa4c4-43ba-3fc8-9a8e-c0325c742ae1"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("205ae533-2634-38b6-87d4-29811b3bdf2d"))) ? (((!hasNext) || (false)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("662f2eed-0db1-31ce-8975-ded8044f72da"))) ? (((true) || (true)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8bf2daab-9c38-3bda-9944-915f0ae86367"))) ? (((!hasNext) || (true)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52bd058b-a0cb-3238-a045-d4f827704a39"))) ? (((!hasNext) || (true)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("34dfd232-3556-3a7a-b464-9588ceb934ed"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("180ccca9-a9c0-3cd7-a7a5-f0e41c799ae9"))) ? (hasNext && this.stopSkippingKVsIfNextRow) : (((KnobRuntime.check(java.util.UUID.fromString("123441a6-0f01-3a8f-bf30-0365d3e17059"))) ? ((!this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e0f86474-674c-34b9-b67a-5152809f8f33"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f1743730-a048-3489-8fc8-ac923b7efb25"))) ? ((this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e11c709e-b264-3b27-9e5e-5ce1b87a3b64"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a495d751-4cfd-3d04-bac6-58c9c66da753"))) ? ((!hasNext) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c64d9f4d-7c25-37df-9f52-6d8ea15862ab"))) ? (((hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0090bfb3-9c8b-31b2-8cea-db51d11ff80d"))) ? ((true) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1fa2ba51-c56d-37a4-b54b-af73170b1b50"))) ? (((hasNext) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4c34d750-d01a-3392-8f7e-9ba259e80c4f"))) ? (((false) && (true)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3b1e4de2-c795-3f40-b373-1234b3b66642"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4e953f34-22e6-3704-adc8-be3a0dd84bac"))) ? (((!hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("34ce4798-7e39-3419-8d17-23956a2fb9df"))) ? (((false) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8e39bb0-748d-365d-895e-a8e56543fa4f"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e3b8fb08-3148-36e8-9fb4-15970eafdf59"))) ? (((!hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("717b4dbd-8dc6-39f4-9162-85b8a34aefb4"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("73f49a03-9700-3409-8c2c-7fd8c85ea395"))) ? (((!hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("efec0a55-83df-346c-8b90-957c94f7a9b0"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d2db21c3-c97c-3c07-a232-51aeba482dd6"))) ? (((!hasNext) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c8a726c5-81e1-3077-b409-050ca14e20db"))) ? ((true) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1f243d15-7273-3f3b-bd2a-b3958a15f095"))) ? (((true) || (true)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("24098a13-dba9-3b9d-99b6-8294def0eddb"))) ? (((true) && (true)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("657b9135-b27e-39a1-950a-8ac33e64201a"))) ? ((this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dab9ed3a-3605-3b7e-832d-73ef2341f7c6"))) ? (((hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c680e36a-3a03-32a1-875c-7bf40a9a084e"))) ? ((hasNext) && (false)) : (((KnobRuntime.check(java.util.UUID.fromString("e4f413d7-a574-3264-b313-eb1448cbdeaa"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4470d77a-ff61-35b7-a665-0718a622db39"))) ? (((hasNext) && (true)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d57e114a-2796-3e85-9db8-2a91e1bf935e"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("31c22c74-b053-3991-8545-4c5192c50140"))) ? (((false) || (true)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ace599d2-8a87-3862-8ae4-6f24a7545fc6"))) ? (((true) || (true)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("055dd151-d2cb-33bc-bf01-8a44de6c1d14"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("61c91ead-b96b-3687-889b-15cf66a7c442"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a94ca24-9fee-3017-8d04-0badf1868c51"))) ? (((hasNext) && (true)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f3b761a-13f4-3350-9665-d4ee6f27907e"))) ? (((false) && (false)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("778b9b2a-0641-387f-ac1d-3957f3eb5aa7"))) ? ((false) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c2fd2874-a141-3cc1-a3be-726c507cc7a5"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("272665d4-ccdc-3b12-8222-6ba51a1d01bf"))) ? (((true) && (true)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("4629b24d-35f9-36f2-9255-adfbff49a570"))) ? (((hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("18b81629-b72e-340b-b4fa-7bd00089137f"))) ? ((hasNext) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f0d8f877-d88b-39b3-a4d3-67db621497de"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f75da87e-fb04-3961-9451-31f068a29da9"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0637e60d-f931-364f-959c-58a8600a33c7"))) ? (((true) || (true)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("40ca8c07-c485-31d2-a909-9a72a120f091"))) ? ((false) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("672ed457-39f7-3fe6-904b-662ff0851465"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b1db1497-16d6-34ed-aac2-cdad1fdcebf5"))) ? ((getComparator().compareRows(cur, startKV)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("01985df3-f9d7-3a81-b9a5-3ea6f5738fad"))) ? ((this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d4c6f66f-9e3a-3232-ad3e-635427dc882b"))) ? (((!hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b532b5f7-adfe-359a-ba21-4e428340c054"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b6f05f26-7fca-35b2-8ac8-6090063ccd65"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f545f4ef-b971-301b-baf7-a7f6e95ae7b9"))) ? ((hasNext) && (this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("aa5a528a-c328-3cfc-b456-c0ac7b3c010f"))) ? (((hasNext) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b4454a05-25c9-3646-be6d-221fa2ca793c"))) ? (((true) && (true)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc083c8d-4d36-325f-9381-73f7850b7808"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e58cf33a-ea1e-31df-a981-c1d3a4e32435"))) ? (((!hasNext) && (true)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("82a1ea0f-b3af-3904-bef0-1c2aafb1b297"))) ? (((!hasNext) || (true)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cfe43a53-e9d5-31cc-8db5-9c5b7decd538"))) ? (((false) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c7fae9d2-d1ec-36ab-9752-f7e70d4375e1"))) ? (((!hasNext) || (true)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8e85b40c-acc4-3b66-b850-e46ea7f7a195"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4ef4b3ed-7d87-3c8d-9e47-d7fe32131d32"))) ? (((!hasNext) && (false)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("65382060-55f4-3d43-9798-a5a07f13d07e"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("768f3e93-2147-3149-8209-88e4970e72fd"))) ? (((false) || (false)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2f88707-342a-3b9d-baa9-cc4df69d25f9"))) ? (((!hasNext) || (true)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e3965e04-7671-38bc-bdf9-3e8c763b6d70"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7c36f7bb-e321-334a-8635-21ed41d94df0"))) ? (((hasNext) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac38a55e-5145-3ae9-a3eb-aebdb5a1a35b"))) ? ((getComparator().compareRows(cur, startKV)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("38218e2b-cbe1-3a34-8a3b-eaa14117efdd"))) ? (((true) || (true)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1c3faa4e-0969-3b68-aa76-b50c093a4326"))) ? ((this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("492dbc5b-a0ab-30a0-828d-22d601942228"))) ? (((true) && (false)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86cff069-6361-36ff-9a3e-a0bd64c50381"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ad3540f7-f4a6-3044-a898-2e0f4e40e53e"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5b51ef93-944b-32d4-b7bd-57bf2438ddc0"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("084774ea-48dc-3e8d-9fd7-e5f2b7ef179b"))) ? (((hasNext) && (true)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9b69f7a3-61ad-3a43-88e0-b516b3f507b7"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b871eb23-290e-3876-8bb0-540b9660120b"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("99529a4c-476e-30f9-be01-ca80846e70a1"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c2a59be-f200-3ba6-abe9-fc8e7c597329"))) ? (((!hasNext) && (!this.stopSkippingKVsIfNextRow)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("01d6c114-2458-351f-9ba9-3b721691378c"))) ? (((true) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("176eb81c-c693-30dc-9d94-7a86ce582d55"))) ? (((hasNext) && (false)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d4f84914-b222-3ee8-8e59-4ac200d84659"))) ? (((false) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7efce162-8d57-3740-aec9-141c03ada066"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7ae2a5fe-86f4-37c1-bcba-176597836047"))) ? ((false) && (this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("77ac4732-57ce-3bd8-b8f6-2c03f670f9e9"))) ? (((hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fb68fa8c-3f02-379b-af80-9d3e3709b5a2"))) ? (((hasNext) && (false)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2c8e7da5-6a82-33cd-bcc9-a5a759b0ef6e"))) ? (((!hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("165db489-c45f-3a42-9b8d-b99808cdc352"))) ? (((false) && (false)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8206d692-efff-37fc-8c91-6e9bd378346b"))) ? (((!hasNext) && (true)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4070dc63-d2d8-34f8-ac81-fd108f55c899"))) ? (((!hasNext) && (false)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2f594fb6-59e3-37a5-96a0-9978cb09cafe"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c303c42b-5209-3e62-91aa-538898ef74d4"))) ? (((hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39711683-1265-366e-b167-7b8058cc391b"))) ? ((hasNext) || (false)) : (((KnobRuntime.check(java.util.UUID.fromString("7b0a57a9-ff9c-3fad-b444-4c7dc5666471"))) ? ((!hasNext) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1a9d177d-b1c3-3881-8e41-2d8c28f45544"))) ? ((false) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5641bd6d-0d83-3e4f-9dc3-59147d508929"))) ? (((!hasNext) || (false)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6f6e4f20-a0fa-3da4-88f4-260d966ebc9b"))) ? ((true) || (this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("3a140489-ad48-35ce-bec3-e7ad3918456b"))) ? ((hasNext) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("699c105b-b7d3-3a0e-847e-634e7374b71d"))) ? ((true) && (!this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("70ee2c90-0d6d-3c99-8e42-944f9278504b"))) ? (((false) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e442112f-c589-3868-af10-994ffd038416"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bcaa2047-a789-3f82-a8d9-5387b178a998"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("38d14fed-0abb-3d72-aa0f-efb4d7a790ae"))) ? (getComparator().compareRows(cur, startKV) > 0) : (((KnobRuntime.check(java.util.UUID.fromString("7a218e26-83fc-3797-94cb-c8a31361c12f"))) ? (((false) || (true)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cc2f11a6-b117-3209-8440-7426c16e0830"))) ? ((this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9030675c-2a7f-34bb-85aa-a4d9a5a33aef"))) ? (((!hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("926b4b78-58b8-325d-9493-262082b186a1"))) ? ((!hasNext) || (this.stopSkippingKVsIfNextRow)) : (((KnobRuntime.check(java.util.UUID.fromString("2ce4cc21-0d4d-3f29-a1a7-014a34c85737"))) ? (((hasNext) || (true)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e1efce32-de24-361a-ab22-780cbd72ed64"))) ? (((false) || (true)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f54b16e9-cb46-33a0-affe-45e20167549e"))) ? ((this.stopSkippingKVsIfNextRow) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c4a4a2a9-c7a4-3f90-b67f-603ebc0315d4"))) ? ((hasNext) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("894af055-414d-3795-9810-74a4f90344fd"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0cba7f67-90a4-3031-a27d-0787fc5e172b"))) ? ((this.stopSkippingKVsIfNextRow) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1d1ba40f-5b21-3c94-a34c-804340bde290"))) ? ((hasNext) || (true)) : (((KnobRuntime.check(java.util.UUID.fromString("1a1e9b84-9f14-35c0-93cc-7eab1bfc2d3d"))) ? (((false) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("89df0258-7878-3d08-8f47-6f7aea7459dd"))) ? ((hasNext) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ffbf656e-7794-31b5-8cb5-16bb06e8c3df"))) ? (((true) && (false)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a9aca3c9-a597-3120-b567-3cdd42b98280"))) ? (((false) && (false)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39b90560-0e57-311c-a31a-2527ae7a2da5"))) ? (((false) && (false)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d8b8e80d-4efa-3a78-bfd2-6e73f82e8771"))) ? (((true) && (false)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2e98964b-1073-3ca7-a588-d007ec8b236e"))) ? (((true) && (true)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f2c48e08-c96c-34d1-832a-8eeca92dad22"))) ? (((!hasNext) && (true)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f33ff6d6-82ab-3adc-b63c-baea15d48fb4"))) ? (((false) || (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("76e13e67-1fd2-3873-8919-fd0a0f0793f0"))) ? (((true) || (this.stopSkippingKVsIfNextRow)) || (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1a0ddda0-84e2-3784-b0f2-02a10b90074f"))) ? ((this.stopSkippingKVsIfNextRow) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a67381a7-83b8-3e16-b304-d565172ad829"))) ? (((false) || (false)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9346458e-3e73-3b0b-bf8d-433637a142db"))) ? (((hasNext) && (false)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("84436bfa-76b7-3832-b25e-b2f0114da737"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0958b8e7-a60e-367c-88e9-a858f57dd41b"))) ? (((false) || (false)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bf79fa1e-098c-3d1b-916a-2f05c1f7a5b3"))) ? ((!this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5d846180-3021-33c0-9f9a-01ab78cf8c38"))) ? (((!hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aa1b755b-bfbf-34c8-8191-5e8cf385a3da"))) ? (((false) && (false)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("61faa69d-f434-3e27-9d71-1069f8d67bb7"))) ? (((!hasNext) && (false)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("499b0a07-0a86-3be5-9ca1-908ca978c712"))) ? (((false) && (false)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f69b0e00-f258-3780-9984-8b27dd520e98"))) ? (((hasNext) || (false)) && ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("317a518b-8622-391a-9408-26efccc63726"))) ? ((false) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("be2b880b-c698-34c0-a972-251f2aa27b44"))) ? (((false) && (true)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2fb3ba9f-d50c-321b-88ed-bc7e6d5a2f5f"))) ? (((false) || (true)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0cff3771-5577-3a55-a029-487907c9643b"))) ? ((hasNext && this.stopSkippingKVsIfNextRow) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e9b4a96b-2c56-32f7-a11c-b565b5b27bf3"))) ? ((true) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac14ce28-0d2d-3f1d-aa2e-7d923e51a229"))) ? (((!hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e816c853-9dae-3800-b188-2936899175e9"))) ? (((true) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fbe11255-08b9-3481-b57f-31c1166b640b"))) ? (((false) && (true)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2f8e33ae-2796-3b11-b151-813979a87238"))) ? (((!hasNext) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8149b8a0-02b2-3a77-a4f6-c0a421f35116"))) ? (((!hasNext) || (true)) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3c302dc8-8ff5-3e5d-9077-0e885202c728"))) ? (((hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4b391ead-2703-3f1d-8f9f-23ad6fa355c0"))) ? (((hasNext) && (false)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b8dfa698-bbad-3bb3-95c8-3d4dc92eecff"))) ? ((!hasNext) || (false)) : (((KnobRuntime.check(java.util.UUID.fromString("7e2c6920-4f77-3a1e-a9d4-ea2e8cfc76ab"))) ? (((true) && (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3be53702-fdcb-3185-9371-590958853381"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a81e77d-8d70-306a-8982-827773dfa648"))) ? (((hasNext) && (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6687d843-079b-33e6-bd96-4937d6eddceb"))) ? (((hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f5073d12-3626-3ed6-9f37-72514683f216"))) ? (((!hasNext) || (this.stopSkippingKVsIfNextRow)) && ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5e783464-e1c2-3998-83af-63a4f43b4c4e"))) ? (((hasNext) || (!this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b809ce2d-7461-3300-aeff-59eaf14c1af5"))) ? (((true) || (false)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d6a8e395-96e7-3b79-b379-42fd4539c5d2"))) ? (((hasNext) && (this.stopSkippingKVsIfNextRow)) && (getComparator().compareRows(cur, startKV) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3360f684-f30d-34cc-9fb5-6d45db8ceb66"))) ? (((false) && (false)) && ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("030233fc-d2e6-3ad2-bafa-dd12bad7a6b3"))) ? (((hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cff992b9-b8b9-38e8-84b7-6e1f64645fd0"))) ? (((!hasNext) || (false)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cfa72869-1c6e-3d1c-9e96-de9790afeb60"))) ? (((false) || (true)) || ((getComparator().compareRows(cur, startKV)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("abee6d77-be79-3e54-a5e9-8e165c84a230"))) ? ((false) && ((getComparator().compareRows(cur, startKV)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("faa88860-23af-37fd-999f-5e14b4678cb4"))) ? (((false) && (this.stopSkippingKVsIfNextRow)) || ((getComparator().compareRows(cur, startKV)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca80710e-acb0-30ad-ba8b-8932d68369a8"))) ? (((true) && (false)) && (getComparator().compareRows(cur, startKV) > 0)) : (hasNext && this.stopSkippingKVsIfNextRow && getComparator().compareRows(cur, startKV) > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
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
      if (KnobRuntime.check(java.util.UUID.fromString("df8731de-c7f2-308a-8d39-ae57ae88ce8c"))) { this.reader.incrementRefCount(); } else { this.reader.readCompleted(); }
    }
    closed = true;
  }

  /** Returns false if not found or if k is after the end. */
  public static boolean seekAtOrAfter(HFileScanner s, Cell k) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e70ba030-2726-3cf0-ad48-f04801f0061e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("56a69066-6b5f-3b86-ac6d-28764a3ddb18"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("65976791-efc0-3d0b-a20f-b5650c40d421"))) {
return true;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("16caba54-7dc2-3c01-be99-1f6ab8a2e205"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("f21dd370-9b87-3274-aa11-2c3f50e15d54"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("9978318a-f494-303e-8e78-cd9f0ab4bb4b"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("3ec8a35f-cdc4-326c-b903-d76d4b6c0c8c"))) ? (!realSeekDone) : (realSeekDone))) return;

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
if(KnobRuntime.check(java.util.UUID.fromString("0aa25574-96b7-38fb-a09f-627ebe7626c6"))) {
return;
}
    hfs.recordBlockSize(blockSizeConsumer);
  }

  @Override
  public Path getFilePath() {
if(KnobRuntime.check(java.util.UUID.fromString("bc686fda-a530-355b-8bdf-8ffabbc927ae"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("2bb2b410-8022-3060-9fbc-2d4989c45040"))) {
try {
    java.lang.reflect.Field field = timeRange.getClass().getDeclaredField("maxStamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(timeRange));
    field.set(timeRange, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9d6e865-4bfc-37b8-a005-c8fa3579d338"))) {
oldestUnexpiredTS = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1308bd3c-c7a2-33a9-aa27-5760dec1bb11"))) {
try {
    java.lang.reflect.Field field = timeRange.getClass().getDeclaredField("minStamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(timeRange));
    field.set(timeRange, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c4b48759-79a6-38ea-aa00-70f1ea5e1179"))) {
try {
    java.lang.reflect.Field field = timeRange.getClass().getDeclaredField("maxStamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(timeRange));
    field.set(timeRange, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c7095e6-e765-3920-a434-616b32e125b0"))) {
oldestUnexpiredTS = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4b0c2286-c37a-3a56-a504-fad56aa34cbf"))) {
try {
    java.lang.reflect.Field field = timeRange.getClass().getDeclaredField("minStamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(timeRange));
    field.set(timeRange, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04f73b09-6cee-3ca9-bbdd-cb0d9f21bf3d"))) {
try {
    java.lang.reflect.Field field = timeRange.getClass().getDeclaredField("minStamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(timeRange));
    field.set(timeRange, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("791781a5-5395-3d14-851e-dea174970c3f"))) {
try {
    java.lang.reflect.Field field = timeRange.getClass().getDeclaredField("maxStamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(timeRange));
    field.set(timeRange, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ad13524-f4cf-3634-950a-d57f27510602"))) {
oldestUnexpiredTS += 1;
}
    return reader.passesTimerangeFilter(timeRange, oldestUnexpiredTS)
      && reader.passesKeyRangeFilter(scan)
      && reader.passesBloomFilter(scan, scan.getFamilyMap().get(cf));
  }

  @Override
  public boolean seekToPreviousRow(Cell originalKey) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0ca970f9-8d25-38cd-9f2b-21464a6cd46b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("58c2788b-cfd8-3990-b830-34c142db3f44"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("abced88f-612f-3ef3-a7a5-560ed9553828"))) {
return true;
}
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
