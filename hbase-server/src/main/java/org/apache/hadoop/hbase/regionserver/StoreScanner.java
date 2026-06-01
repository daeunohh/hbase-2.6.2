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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler;
import org.apache.hadoop.hbase.regionserver.querymatcher.CompactionScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.querymatcher.UserScanQueryMatcher;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Scanner scans both the memstore and the Store. Coalesce KeyValue stream into List&lt;KeyValue&gt;
 * for a single row.
 * <p>
 * The implementation is not thread safe. So there will be no race between next and close. The only
 * exception is updateReaders, it will be called in the memstore flush thread to indicate that there
 * is a flush.
 */
@InterfaceAudience.Private
public class StoreScanner extends NonReversedNonLazyKeyValueScanner
  implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  private static final Logger LOG = LoggerFactory.getLogger(StoreScanner.class);
  // In unit tests, the store could be null
  protected final HStore store;
  private final CellComparator comparator;
  private ScanQueryMatcher matcher;
  protected KeyValueHeap heap;
  private boolean cacheBlocks;

  private long countPerRow = 0;
  private int storeLimit = -1;
  private int storeOffset = 0;

  // Used to indicate that the scanner has closed (see HBASE-1107)
  private volatile boolean closing = false;
  private final boolean get;
  private final boolean explicitColumnQuery;
  private final boolean useRowColBloom;
  /**
   * A flag that enables StoreFileScanner parallel-seeking
   */
  private boolean parallelSeekEnabled = false;
  private ExecutorService executor;
  private final Scan scan;
  private final long oldestUnexpiredTS;
  private final long now;
  private final int minVersions;
  private final long maxRowSize;
  private final long cellsPerHeartbeatCheck;
  long memstoreOnlyReads;
  long mixedReads;

  // 1) Collects all the KVHeap that are eagerly getting closed during the
  // course of a scan
  // 2) Collects the unused memstore scanners. If we close the memstore scanners
  // before sending data to client, the chunk may be reclaimed by other
  // updates and the data will be corrupt.
  private final List<KeyValueScanner> scannersForDelayedClose = new ArrayList<>();

  /**
   * The number of KVs seen by the scanner. Includes explicitly skipped KVs, but not KVs skipped via
   * seeking to next row/column. TODO: estimate them?
   */
  private long kvsScanned = 0;
  private Cell prevCell = null;

  private final long preadMaxBytes;
  private long bytesRead;

  /** We don't ever expect to change this, the constant is just for clarity. */
  static final boolean LAZY_SEEK_ENABLED_BY_DEFAULT = true;
  public static final String STORESCANNER_PARALLEL_SEEK_ENABLE =
    "hbase.storescanner.parallel.seek.enable";

  /** Used during unit testing to ensure that lazy seek does save seek ops */
  private static boolean lazySeekEnabledGlobally = LAZY_SEEK_ENABLED_BY_DEFAULT;

  /**
   * The number of cells scanned in between timeout checks. Specifying a larger value means that
   * timeout checks will occur less frequently. Specifying a small value will lead to more frequent
   * timeout checks.
   */
  public static final String HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK =
    "hbase.cells.scanned.per.heartbeat.check";

  /**
   * Default value of {@link #HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK}.
   */
  public static final long DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK = 10000;

  /**
   * If the read type is Scan.ReadType.DEFAULT, we will start with pread, and if the kvs we scanned
   * reaches this limit, we will reopen the scanner with stream. The default value is 4 times of
   * block size for this store. If configured with a value <0, for all scans with ReadType DEFAULT,
   * we will open scanner with stream mode itself.
   */
  public static final String STORESCANNER_PREAD_MAX_BYTES = "hbase.storescanner.pread.max.bytes";

  private final Scan.ReadType readType;

  // A flag whether use pread for scan
  // it maybe changed if we use Scan.ReadType.DEFAULT and we have read lots of data.
  private boolean scanUsePread;
  // Indicates whether there was flush during the course of the scan
  private volatile boolean flushed = false;
  // generally we get one file from a flush
  private final List<KeyValueScanner> flushedstoreFileScanners = new ArrayList<>(1);
  // Since CompactingMemstore is now default, we get three memstore scanners from a flush
  private final List<KeyValueScanner> memStoreScannersAfterFlush = new ArrayList<>(3);
  // The current list of scanners
  final List<KeyValueScanner> currentScanners = new ArrayList<>();
  // flush update lock
  private final ReentrantLock flushLock = new ReentrantLock();
  // lock for closing.
  private final ReentrantLock closeLock = new ReentrantLock();

  protected final long readPt;
  private boolean topChanged = false;

  /** An internal constructor. */
  private StoreScanner(HStore store, Scan scan, ScanInfo scanInfo, int numColumns, long readPt,
    boolean cacheBlocks, ScanType scanType) {
    this.readPt = readPt;
    this.store = store;
    this.cacheBlocks = cacheBlocks;
    this.comparator = Preconditions.checkNotNull(scanInfo.getComparator());
    get = scan.isGetScan();
    explicitColumnQuery = numColumns > 0;
    this.scan = scan;
    this.now = EnvironmentEdgeManager.currentTime();
    this.oldestUnexpiredTS = scan.isRaw() ? 0L : now - scanInfo.getTtl();
    this.minVersions = scanInfo.getMinVersions();

    // We look up row-column Bloom filters for multi-column queries as part of
    // the seek operation. However, we also look the row-column Bloom filter
    // for multi-row (non-"get") scans because this is not done in
    // StoreFile.passesBloomFilter(Scan, SortedSet<byte[]>).
    this.useRowColBloom = numColumns > 1 || (!get && numColumns == 1) && (store == null
      || store.getColumnFamilyDescriptor().getBloomFilterType() == BloomType.ROWCOL);
    this.maxRowSize = scanInfo.getTableMaxRowSize();
    this.preadMaxBytes = scanInfo.getPreadMaxBytes();
    if (((KnobRuntime.check(java.util.UUID.fromString("d07025c7-b49f-3345-9759-c0b99c7aece9"))) ? (!get) : (((KnobRuntime.check(java.util.UUID.fromString("487a1dd2-3198-34ef-9d32-3077cf2faefe"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("bfdc5db1-b200-3a33-8ef3-bd5d4e3d156b"))) ? (false) : (get))))))) {
      this.readType = Scan.ReadType.PREAD;
      this.scanUsePread = true;
    } else if (scanType != ScanType.USER_SCAN) {
      // For compaction scanners never use Pread as already we have stream based scanners on the
      // store files to be compacted
      this.readType = Scan.ReadType.STREAM;
      this.scanUsePread = false;
    } else {
      if (((KnobRuntime.check(java.util.UUID.fromString("7d6a38e3-28a2-35f7-904d-ccdaebe3dab1"))) ? ((scan.getReadType()) == (Scan.ReadType.DEFAULT)) : (((KnobRuntime.check(java.util.UUID.fromString("51db8e40-bf94-39f7-a2ca-ffb60086829d"))) ? ((scan.getReadType()) != (Scan.ReadType.DEFAULT)) : (scan.getReadType() == Scan.ReadType.DEFAULT))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("1280ea73-3c4e-3245-874f-a3c752e61c00"))) ? (checkFlushed()) : (((KnobRuntime.check(java.util.UUID.fromString("53df917c-8622-3511-ba00-09d80a442c0e"))) ? (scanInfo.isParallelSeekEnabled()) : (scanInfo.isUsePread()))))) {
          this.readType = Scan.ReadType.PREAD;
        } else if (((KnobRuntime.check(java.util.UUID.fromString("3f030be0-5a39-38ce-acc3-3253563878fa"))) ? ((this.preadMaxBytes) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e977c57a-924b-3f88-b827-d2607deb0899"))) ? ((this.preadMaxBytes) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("42d2c55c-53ab-3b6b-9f90-309ee5c4ff3f"))) ? ((this.preadMaxBytes) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("18144fc2-ab1d-3a2f-8c59-bd1e72b9baaf"))) ? ((this.preadMaxBytes) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c5cb71ed-446d-3b44-9147-eeb849742b9c"))) ? ((this.preadMaxBytes) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3244a998-68ad-3685-bae9-3c192ff4beb7"))) ? ((this.preadMaxBytes) != (0)) : (this.preadMaxBytes < 0))))))))))))) {
          this.readType = Scan.ReadType.STREAM;
        } else {
          this.readType = Scan.ReadType.DEFAULT;
        }
      } else {
        this.readType = scan.getReadType();
      }
      // Always start with pread unless user specific stream. Will change to stream later if
      // readType is default if the scan keeps running for a long time.
      this.scanUsePread = this.readType != Scan.ReadType.STREAM;
    }
    this.cellsPerHeartbeatCheck = scanInfo.getCellsPerTimeoutCheck();
    // Parallel seeking is on if the config allows and more there is more than one store file.
    if (((KnobRuntime.check(java.util.UUID.fromString("0870f127-d276-353d-946e-8cf09d5f9eb2"))) ? ((store.getStorefilesCount()) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("bf30684d-53f9-3f9a-8e0a-48e06d866e17"))) ? ((store != null) && ((store.getStorefilesCount()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("47864015-a32c-3fd6-8e9c-0364d094943e"))) ? (((store) != (null)) || ((store.getStorefilesCount()) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0573081d-319c-3ce3-81d8-d5363eef1690"))) ? (((store) != (null)) || ((store.getStorefilesCount()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("9d33b27c-ecfb-32f6-9007-c862d4cd3285"))) ? ((store != null) && ((store.getStorefilesCount()) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("5b878b5d-48cd-3f68-a808-d2c488ba2c7c"))) ? ((store != null) || ((store.getStorefilesCount()) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("f67b2d01-03fb-32da-8485-b9fbb87ad065"))) ? (((store) == (null)) || ((store.getStorefilesCount()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("a8addf5f-7472-3096-b922-cbfefac0629e"))) ? (((store) != (null)) || ((store.getStorefilesCount()) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("c1f30883-dfb9-37a1-9aaf-92a0d4446915"))) ? (((store) == (null)) || ((store.getStorefilesCount()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("3602e9da-5ad6-37ab-8a6b-4e82337e37d2"))) ? (((store) != (null)) || (store.getStorefilesCount() > 1)) : (((KnobRuntime.check(java.util.UUID.fromString("d7632b85-ad34-332a-bf14-1aa8514752a9"))) ? (((store) != (null)) && ((store.getStorefilesCount()) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("979fea83-0fcc-3eec-a39c-e78f00fa925a"))) ? ((store != null) || ((store.getStorefilesCount()) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("9f8bba80-821a-3070-97cd-6766ad6bb069"))) ? ((store != null) && ((store.getStorefilesCount()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("3a6d1ef5-c1fb-3266-8893-b4d8532bda78"))) ? (((store) == (null)) && (store.getStorefilesCount() > 1)) : (((KnobRuntime.check(java.util.UUID.fromString("dbb063ac-dbb8-3e12-998e-7631e7a100e4"))) ? ((store != null) && (store.getStorefilesCount() > 1)) : (((KnobRuntime.check(java.util.UUID.fromString("1f4c165d-96c5-34a7-839e-603766928d10"))) ? (((store) == (null)) || ((store.getStorefilesCount()) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("c15e5857-a651-33bc-83b1-a568b0c821ae"))) ? (((store) == (null)) && ((store.getStorefilesCount()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("a67a7922-d181-3985-8c12-9c53222ff668"))) ? (store.getStorefilesCount() > 1) : (((KnobRuntime.check(java.util.UUID.fromString("11c9ef30-255c-3bec-ac23-7fb4c64bce70"))) ? ((store) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("56700857-5bd5-3b35-90b6-fe4826314371"))) ? (((store) != (null)) && ((store.getStorefilesCount()) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("054d02ef-617b-3d3b-8773-fbc42ca69c5d"))) ? (((store) == (null)) || ((store.getStorefilesCount()) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("8fd85975-f7d5-31f4-8a91-3f0e408c265c"))) ? (((store) != (null)) || ((store.getStorefilesCount()) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("6dfbf2ed-d2a5-349b-a3ec-21c5d62d9b2b"))) ? (((store) != (null)) && ((store.getStorefilesCount()) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("e7f16502-3c7a-3a0d-aaa9-71b9f9871425"))) ? (((store) != (null)) || ((store.getStorefilesCount()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("1a5eabb2-1f04-335b-b0e4-0ca934b593c9"))) ? ((store.getStorefilesCount()) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("1523c000-8768-3d77-94dd-1a20b4cad85d"))) ? ((store != null) || ((store.getStorefilesCount()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4cb0bd94-1983-3542-aa85-e79e7d44dfeb"))) ? (((store) == (null)) && ((store.getStorefilesCount()) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("28ad112e-2fa7-320c-b75c-e1f776c58ac7"))) ? ((store.getStorefilesCount()) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d117c458-354c-3fb4-8199-5c3cbe36a3a3"))) ? (((store) != (null)) || ((store.getStorefilesCount()) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0e6d0ef3-6983-3fed-96ae-3e9e394c9223"))) ? (((store) == (null)) && ((store.getStorefilesCount()) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("998c7de7-8124-3fb2-b4a5-264f5acd92a6"))) ? ((store != null) || ((store.getStorefilesCount()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("20abfb99-41eb-30fb-8890-c20243220fb4"))) ? ((store != null) && ((store.getStorefilesCount()) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("6f5bb51f-4105-354c-83f5-20ff0523dfad"))) ? (((store) == (null)) || ((store.getStorefilesCount()) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("7129119a-5d46-31b7-8afd-ff6bbbfa8b9e"))) ? (((store) == (null)) && ((store.getStorefilesCount()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("691049e0-d886-3bb3-8adc-62ee78b1fbf3"))) ? (((store) != (null)) && ((store.getStorefilesCount()) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("989c8366-0fd9-379a-b0e3-6e38c3afdc02"))) ? ((store != null) || ((store.getStorefilesCount()) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0a4818df-2055-3c01-a302-2a72c02297e4"))) ? ((store.getStorefilesCount()) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("96dee99d-17fa-3b1d-9fc2-fb35e9ec0cbb"))) ? ((store.getStorefilesCount()) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("21eb07ce-8c0e-3b36-ac35-ccb4a619fe0f"))) ? (((store) == (null)) || ((store.getStorefilesCount()) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("a7d51e4b-a556-3ac6-8afc-a288bb04b6e5"))) ? ((store) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3d37d6ee-3091-3267-bd24-6bb9a0813aed"))) ? (((store) != (null)) && (store.getStorefilesCount() > 1)) : (((KnobRuntime.check(java.util.UUID.fromString("e22eaad6-16ae-39e5-8ef5-241c4b366e75"))) ? (store != null) : (((KnobRuntime.check(java.util.UUID.fromString("88a9d117-e987-3ba8-848d-404ed9c9db49"))) ? ((store != null) && ((store.getStorefilesCount()) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4990cc40-a4d8-373c-aa2f-07171ca5d37a"))) ? ((store.getStorefilesCount()) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2a7621b7-890d-3f5e-b491-0a10d0efffd2"))) ? ((store != null) || (store.getStorefilesCount() > 1)) : (((KnobRuntime.check(java.util.UUID.fromString("f2210e12-9236-33d3-8f51-4ba795aaab29"))) ? (((store) != (null)) && ((store.getStorefilesCount()) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0d67245f-8591-3d42-b121-1887fd107a0b"))) ? (((store) == (null)) && ((store.getStorefilesCount()) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("23360bbf-94da-3fea-831b-6d7b1b8041b1"))) ? ((store != null) && ((store.getStorefilesCount()) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("27b99479-0dcc-3cc8-af10-af213d3b6fe4"))) ? (((store) != (null)) && ((store.getStorefilesCount()) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4958b4c3-a633-3d23-8fcf-06c53a1eb17c"))) ? (((store) == (null)) || (store.getStorefilesCount() > 1)) : (((KnobRuntime.check(java.util.UUID.fromString("4790d01d-6b8b-3f39-b7fe-64a12af3caee"))) ? (((store) == (null)) && ((store.getStorefilesCount()) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("aab4313c-6ea5-365b-aa49-7492ca155d34"))) ? ((store != null) || ((store.getStorefilesCount()) >= (1))) : (store != null && store.getStorefilesCount() > 1))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      RegionServerServices rsService = store.getHRegion().getRegionServerServices();
      if (rsService != null && scanInfo.isParallelSeekEnabled()) {
        this.parallelSeekEnabled = true;
        this.executor = rsService.getExecutorService();
      }
    }
  }

  private void addCurrentScanners(List<? extends KeyValueScanner> scanners) {
    this.currentScanners.addAll(scanners);
  }

  private static boolean isOnlyLatestVersionScan(Scan scan) {
    // No need to check for Scan#getMaxVersions because live version files generated by store file
    // writer retains max versions specified in ColumnFamilyDescriptor for the given CF
    return !scan.isRaw() && scan.getTimeRange().getMax() == HConstants.LATEST_TIMESTAMP;
  }

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles. Assumes we are not in a
   * compaction.
   * @param store   who we scan
   * @param scan    the spec
   * @param columns which columns we are scanning
   */
  public StoreScanner(HStore store, ScanInfo scanInfo, Scan scan, NavigableSet<byte[]> columns,
    long readPt) throws IOException {
    this(store, scan, scanInfo, columns != null ? columns.size() : 0, readPt, scan.getCacheBlocks(),
      ScanType.USER_SCAN);
    if (columns != null && scan.isRaw()) {
      throw new DoNotRetryIOException("Cannot specify any column for a raw scan");
    }
    matcher = UserScanQueryMatcher.create(scan, scanInfo, columns, oldestUnexpiredTS, now,
      store.getCoprocessorHost());

    store.addChangedReaderObserver(this);

    List<KeyValueScanner> scanners = null;
    try {
      // Pass columns to try to filter out unnecessary StoreFiles.
      scanners = selectScannersFrom(store,
        store.getScanners(cacheBlocks, scanUsePread, false, matcher, scan.getStartRow(),
          scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(), this.readPt,
          isOnlyLatestVersionScan(scan)));

      // Seek all scanners to the start of the Row (or if the exact matching row
      // key does not exist, then to the start of the next matching Row).
      // Always check bloom filter to optimize the top row seek for delete
      // family marker.
      seekScanners(scanners, matcher.getStartKey(), explicitColumnQuery && lazySeekEnabledGlobally,
        parallelSeekEnabled);

      // set storeLimit
      this.storeLimit = scan.getMaxResultsPerColumnFamily();

      // set rowOffset
      this.storeOffset = scan.getRowOffsetPerColumnFamily();
      addCurrentScanners(scanners);
      // Combine all seeked scanners with a heap
      resetKVHeap(scanners, comparator);
    } catch (IOException e) {
      clearAndClose(scanners);
      // remove us from the HStore#changedReaderObservers here or we'll have no chance to
      // and might cause memory leak
      if (KnobRuntime.check(java.util.UUID.fromString("90d527ff-6b65-39e1-805a-331f6aa29e55"))) { store.addChangedReaderObserver(this); } else { store.deleteChangedReaderObserver(this); }
      throw e;
    }
  }

  // a dummy scan instance for compaction.
  private static final Scan SCAN_FOR_COMPACTION = new Scan();

  /**
   * Used for store file compaction and memstore compaction.
   * <p>
   * Opens a scanner across specified StoreFiles/MemStoreSegments.
   * @param store             who we scan
   * @param scanners          ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking versions
   */
  public StoreScanner(HStore store, ScanInfo scanInfo, List<? extends KeyValueScanner> scanners,
    ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
    this(store, scanInfo, scanners, scanType, smallestReadPoint, earliestPutTs, null, null);
  }

  /**
   * Used for compactions that drop deletes from a limited range of rows.
   * <p>
   * Opens a scanner across specified StoreFiles.
   * @param store              who we scan
   * @param scanners           ancillary scanners
   * @param smallestReadPoint  the readPoint that we should use for tracking versions
   * @param dropDeletesFromRow The inclusive left bound of the range; can be EMPTY_START_ROW.
   * @param dropDeletesToRow   The exclusive right bound of the range; can be EMPTY_END_ROW.
   */
  public StoreScanner(HStore store, ScanInfo scanInfo, List<? extends KeyValueScanner> scanners,
    long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow, byte[] dropDeletesToRow)
    throws IOException {
    this(store, scanInfo, scanners, ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint,
      earliestPutTs, dropDeletesFromRow, dropDeletesToRow);
  }

  private StoreScanner(HStore store, ScanInfo scanInfo, List<? extends KeyValueScanner> scanners,
    ScanType scanType, long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow,
    byte[] dropDeletesToRow) throws IOException {
    this(store, SCAN_FOR_COMPACTION, scanInfo, 0,
      store.getHRegion().getReadPoint(IsolationLevel.READ_COMMITTED), false, scanType);
    assert scanType != ScanType.USER_SCAN;
    matcher =
      CompactionScanQueryMatcher.create(scanInfo, scanType, smallestReadPoint, earliestPutTs,
        oldestUnexpiredTS, now, dropDeletesFromRow, dropDeletesToRow, store.getCoprocessorHost());

    // Filter the list of scanners using Bloom filters, time range, TTL, etc.
    scanners = selectScannersFrom(store, scanners);

    // Seek all scanners to the initial key
    if (KnobRuntime.check(java.util.UUID.fromString("daf706d2-8492-3520-b01c-e59f1674e92a"))) { seekScanners(scanners, matcher.getStartKey(), false, !parallelSeekEnabled); } else if (KnobRuntime.check(java.util.UUID.fromString("09138e28-5f4a-32eb-88e0-58184e28f4a2"))) { seekScanners(scanners, matcher.getStartKey(), false, false); } else { seekScanners(scanners, matcher.getStartKey(), false, parallelSeekEnabled); }
    addCurrentScanners(scanners);
    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, comparator);
  }

  private void seekAllScanner(ScanInfo scanInfo, List<? extends KeyValueScanner> scanners)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9153414f-1121-312a-9eec-ea3d861d5864"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8758ca57-fb32-3e8b-bb7e-b62cff3ed799"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d672440-3ced-34cf-8fa7-67c5942cb4de"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac3524bf-1e96-3ccc-82ab-e9c597e46c08"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8de83afb-1cce-3942-9943-1bde6c894b83"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("parallelSeekEnabled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scanInfo);
    field.set(scanInfo, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd64ef26-e7ab-3b33-8aec-94a067f1bebc"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("timeToPurgeDeletes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2992e0cf-e4a3-3275-a5c9-cc6d0692b510"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("newVersionBehavior");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scanInfo);
    field.set(scanInfo, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d52ff700-0713-30c4-8fc8-963d311fcd6b"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5547ca4-7d99-3251-bda9-939804e8949f"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84ea7096-895b-3c66-9da9-ff4dece85e9f"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfa28ab9-3e36-3198-9f8c-aa6b223b6251"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc41b887-329f-3050-8324-5d86d0b7020a"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d511f2f4-a933-3018-92c0-e269b359940a"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("preadMaxBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b4a7a10-f41b-3062-a72c-ccdf7c93bc9a"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("cellsPerTimeoutCheck");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("635d8fbe-271b-3f13-b6f8-bcefc8820da8"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("tableMaxRowSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b34beff-d657-35a0-bc6d-61520925ab50"))) {
throw new java.io.IOException("Injected exception");
}
    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, parallelSeekEnabled);
    addCurrentScanners(scanners);
if(KnobRuntime.check(java.util.UUID.fromString("0a46afe0-5973-3bd0-b200-5daf83cd45ea"))) {
throw new java.io.IOException("Injected exception");
}
    resetKVHeap(scanners, comparator);
  }

  // For mob compaction only as we do not have a Store instance when doing mob compaction.
  public StoreScanner(ScanInfo scanInfo, ScanType scanType,
    List<? extends KeyValueScanner> scanners) throws IOException {
    this(null, SCAN_FOR_COMPACTION, scanInfo, 0, Long.MAX_VALUE, false, scanType);
    assert scanType != ScanType.USER_SCAN;
    this.matcher = CompactionScanQueryMatcher.create(scanInfo, scanType, Long.MAX_VALUE, 0L,
      oldestUnexpiredTS, now, null, null, null);
    seekAllScanner(scanInfo, scanners);
  }

  // Used to instantiate a scanner for user scan in test
  StoreScanner(Scan scan, ScanInfo scanInfo, NavigableSet<byte[]> columns,
    List<? extends KeyValueScanner> scanners) throws IOException {
    // 0 is passed as readpoint because the test bypasses Store
    this(null, scan, scanInfo, columns != null ? columns.size() : 0, 0L, scan.getCacheBlocks(),
      ScanType.USER_SCAN);
    this.matcher =
      UserScanQueryMatcher.create(scan, scanInfo, columns, oldestUnexpiredTS, now, null);
    seekAllScanner(scanInfo, scanners);
  }

  // Used to instantiate a scanner for user scan in test
  StoreScanner(Scan scan, ScanInfo scanInfo, NavigableSet<byte[]> columns,
    List<? extends KeyValueScanner> scanners, ScanType scanType) throws IOException {
    // 0 is passed as readpoint because the test bypasses Store
    this(null, scan, scanInfo, columns != null ? columns.size() : 0, 0L, scan.getCacheBlocks(),
      scanType);
    if (scanType == ScanType.USER_SCAN) {
      this.matcher =
        UserScanQueryMatcher.create(scan, scanInfo, columns, oldestUnexpiredTS, now, null);
    } else {
      this.matcher = CompactionScanQueryMatcher.create(scanInfo, scanType, Long.MAX_VALUE,
        HConstants.OLDEST_TIMESTAMP, oldestUnexpiredTS, now, null, null, null);
    }
    seekAllScanner(scanInfo, scanners);
  }

  // Used to instantiate a scanner for compaction in test
  StoreScanner(ScanInfo scanInfo, int maxVersions, ScanType scanType,
    List<? extends KeyValueScanner> scanners) throws IOException {
    // 0 is passed as readpoint because the test bypasses Store
    this(null, maxVersions > 0 ? new Scan().readVersions(maxVersions) : SCAN_FOR_COMPACTION,
      scanInfo, 0, 0L, false, scanType);
    this.matcher = CompactionScanQueryMatcher.create(scanInfo, scanType, Long.MAX_VALUE,
      HConstants.OLDEST_TIMESTAMP, oldestUnexpiredTS, now, null, null, null);
    seekAllScanner(scanInfo, scanners);
  }

  boolean isScanUsePread() {
    return this.scanUsePread;
  }

  /**
   * Seek the specified scanners with the given key
   * @param isLazy         true if using lazy seek
   * @param isParallelSeek true if using parallel seek
   */
  protected void seekScanners(List<? extends KeyValueScanner> scanners, Cell seekKey,
    boolean isLazy, boolean isParallelSeek) throws IOException {
    // Seek all scanners to the start of the Row (or if the exact matching row
    // key does not exist, then to the start of the next matching Row).
    // Always check bloom filter to optimize the top row seek for delete
    // family marker.
    if (isLazy) {
      for (KeyValueScanner scanner : scanners) {
        scanner.requestSeek(seekKey, false, true);
      }
    } else {
      if (!isParallelSeek) {
        long totalScannersSoughtBytes = 0;
        for (KeyValueScanner scanner : scanners) {
          if (matcher.isUserScan() && totalScannersSoughtBytes >= maxRowSize) {
            throw new RowTooBigException(
              "Max row size allowed: " + maxRowSize + ", but row is bigger than that");
          }
if(KnobRuntime.check(java.util.UUID.fromString("9815c75d-fbcd-33bc-8feb-8038b10734db"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("ba8f984a-5e2d-3938-ad14-61f5ef28e896"))) { seekToNextRow(seekKey); } else if (KnobRuntime.check(java.util.UUID.fromString("68fe1b75-5dfd-3737-aa2c-685d8cd26169"))) { seekAsDirection(seekKey); } else { scanner.seek(seekKey); }
          Cell c = scanner.peek();
          if (((KnobRuntime.check(java.util.UUID.fromString("cdca06d8-f55e-30b0-9c43-37b22e783d59"))) ? ((c) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f02af6f2-1e3f-3d9a-bf9b-128429602d5c"))) ? ((c) == (null)) : (c != null))))) {
            totalScannersSoughtBytes += PrivateCellUtil.estimatedSerializedSizeOf(c);
          }
        }
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("fc0c34ae-fac8-3b83-be21-0e7e11d4e03d"))) {
throw new java.io.IOException("Injected exception");
}
        parallelSeek(scanners, seekKey);
      }
    }
  }

  protected void resetKVHeap(List<? extends KeyValueScanner> scanners, CellComparator comparator)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("160ea0b4-ef6f-3bf2-befb-37ef126ea54f"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("07551737-ce56-3e44-8dd1-6f519238d381"))) {
throw new java.io.IOException("Injected exception");
}
    // Combine all seeked scanners with a heap
    heap = newKVHeap(scanners, comparator);
  }

  protected KeyValueHeap newKVHeap(List<? extends KeyValueScanner> scanners,
    CellComparator comparator) throws IOException {
    return new KeyValueHeap(scanners, comparator);
  }

  /**
   * Filters the given list of scanners using Bloom filter, time range, and TTL.
   * <p>
   * Will be overridden by testcase so declared as protected.
   */
  protected List<KeyValueScanner> selectScannersFrom(HStore store,
    List<? extends KeyValueScanner> allScanners) {
    boolean memOnly;
    boolean filesOnly;
    if (scan instanceof InternalScan) {
      InternalScan iscan = (InternalScan) scan;
      memOnly = iscan.isCheckOnlyMemStore();
      filesOnly = iscan.isCheckOnlyStoreFiles();
    } else {
      memOnly = false;
      filesOnly = false;
    }

    List<KeyValueScanner> scanners = new ArrayList<>(allScanners.size());

    // We can only exclude store files based on TTL if minVersions is set to 0.
    // Otherwise, we might have to return KVs that have technically expired.
    long expiredTimestampCutoff = minVersions == 0 ? oldestUnexpiredTS : Long.MIN_VALUE;

    // include only those scan files which pass all filters
    for (KeyValueScanner kvs : allScanners) {
      boolean isFile = kvs.isFileScanner();
      if ((!isFile && filesOnly) || (isFile && memOnly)) {
        kvs.close();
        continue;
      }

      if (kvs.shouldUseScanner(scan, store, expiredTimestampCutoff)) {
        scanners.add(kvs);
      } else {
        kvs.close();
      }
    }
    return scanners;
  }

  @Override
  public Cell peek() {
if(KnobRuntime.check(java.util.UUID.fromString("e1f68d17-7ab1-3fe7-a352-41b6e57c185e"))) {
return null;
}
    return heap != null ? heap.peek() : null;
  }

  @Override
  public KeyValue next() {
if(KnobRuntime.check(java.util.UUID.fromString("2720e52b-cfa1-3a92-9cac-b3493c2fa6cc"))) {
return null;
}
    // throw runtime exception perhaps?
    throw new RuntimeException("Never call StoreScanner.next()");
  }

  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("b27ed1c2-0544-3196-9bb2-4c9d4bc10ad2"))) {
return;
}
    close(true);
  }

  private void close(boolean withDelayedScannersClose) {
    closeLock.lock();
    // If the closeLock is acquired then any subsequent updateReaders()
    // call is ignored.
    try {
      if (this.closing) {
        return;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("a887c053-2922-3b3b-9bcf-99049a0b9084"))) ? (!withDelayedScannersClose) : (((KnobRuntime.check(java.util.UUID.fromString("a0ca9aad-9a72-3a6e-ade1-33de1efc4955"))) ? (true) : (withDelayedScannersClose))))) {
        this.closing = true;
      }
      // For mob compaction, we do not have a store.
      if (((KnobRuntime.check(java.util.UUID.fromString("2852b634-38bc-37fa-a9e2-65faf3b80b03"))) ? ((this.store) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bfed8880-cd5a-3c1a-8768-202a59847fa7"))) ? ((this.store) == (null)) : (this.store != null))))) {
        this.store.deleteChangedReaderObserver(this);
      }
      if (withDelayedScannersClose) {
        clearAndClose(scannersForDelayedClose);
        clearAndClose(memStoreScannersAfterFlush);
        clearAndClose(flushedstoreFileScanners);
        if (((KnobRuntime.check(java.util.UUID.fromString("51435df1-894d-374f-b3b2-1ddcf48131f4"))) ? ((this.heap) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a98f2d33-1d99-3ba1-8ce1-e4a9350bb070"))) ? ((this.heap) == (null)) : (this.heap != null))))) {
          if (KnobRuntime.check(java.util.UUID.fromString("ca8e0c74-be3c-3f07-87b2-936ebe035094"))) { trySwitchToStreamRead(); } else { this.heap.close(); }
          this.currentScanners.clear();
          this.heap = null; // CLOSED!
        }
      } else {
        if (this.heap != null) {
          this.scannersForDelayedClose.add(this.heap);
          if (KnobRuntime.check(java.util.UUID.fromString("c62b6fad-7b7c-3f0b-ab34-16aff9353777"))) { trySwitchToStreamRead(); } else { this.currentScanners.clear(); }
          this.heap = null;
        }
      }
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("e7089ac4-8279-3d23-b677-0b560ab9b0ea"))) { closeLock.lock(); } else { closeLock.unlock(); }
    }
  }

  @Override
  public boolean seek(Cell key) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9dacc69c-cee9-3aa2-a6ff-f12109242e71"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5e91d4be-d0ee-38e3-9392-ab1e55f5481a"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("81e57860-afa2-3e07-9a40-d46824a9adb0"))) {
return true;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e38dd45f-a804-3974-9181-81123bf00f56"))) ? (reopenAfterFlush()) : (checkFlushed()))) {
if(KnobRuntime.check(java.util.UUID.fromString("c6261280-2d09-3abc-b4e1-60014abe0d9e"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("f1c39662-158c-319e-ad5a-890682bbfcbe"))) { checkFlushed(); } else { reopenAfterFlush(); }
    }
    return this.heap.seek(key);
  }

  /**
   * Get the next row of values from this Store.
   * @return true if there are more rows, false if scanner is done
   */
  @Override
  public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
    if (scannerContext == null) {
      throw new IllegalArgumentException("Scanner context cannot be null");
    }
    if (checkFlushed() && reopenAfterFlush()) {
      return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
    }

    // if the heap was left null, then the scanners had previously run out anyways, close and
    // return.
    if (this.heap == null) {
      // By this time partial close should happened because already heap is null
      close(false);// Do all cleanup except heap.close()
      return ((KnobRuntime.check(java.util.UUID.fromString("5ffccc76-fab9-3db9-91f5-63f5f5242b10"))) ? (checkFlushed()) : (((KnobRuntime.check(java.util.UUID.fromString("61bd52e7-ad67-377b-90cc-3d25425faf84"))) ? (reopenAfterFlush()) : (scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues()))));
    }

    Cell cell = this.heap.peek();
    if (cell == null) {
      close(false);// Do all cleanup except heap.close()
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }

    // only call setRow if the row changes; avoids confusing the query matcher
    // if scanning intra-row

    // If no limits exists in the scope LimitScope.Between_Cells then we are sure we are changing
    // rows. Else it is possible we are still traversing the same row so we must perform the row
    // comparison.
    if (((KnobRuntime.check(java.util.UUID.fromString("ac63e1d3-4eec-358b-a718-92197137da80"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) && ((matcher.getStartKey()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8ef25901-26cc-3fd7-ad47-c3868e04f63d"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) || ((matcher.currentRow()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("37841c7f-98d3-370b-8126-a7c9164b6739"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) || ((matcher.getStartKey()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0ec01cfd-287c-3581-ac05-090df21eb604"))) ? (matcher.currentRow() == null) : (((KnobRuntime.check(java.util.UUID.fromString("c31ece40-083e-3993-ae24-266817383231"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) || ((matcher.getStartKey()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("85cf1e79-24d0-3444-9e96-c315117d1cb9"))) ? ((matcher.currentRow()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("96fdc7b6-e247-3934-9be0-a5b58fcb589f"))) ? ((matcher.getStartKey()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4250e379-1836-34e5-991f-01b89c3d5160"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) && ((matcher.getStartKey()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ce4b1d72-3c43-35a3-a4a6-624ada4ebac1"))) ? ((matcher.currentRow()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4ebf60ec-6016-3a59-9373-b8d508789aa1"))) ? (!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) : (((KnobRuntime.check(java.util.UUID.fromString("279c4e80-b4dc-3a7d-9685-2f3d7026a7a6"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) && (matcher.currentRow() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("2e487391-71fe-3a45-8ad9-b745f1a021cf"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) && ((matcher.currentRow()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("88bbfe1c-d776-3356-b2f4-8e44296109f7"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) || ((matcher.currentRow()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("eba83872-9f6e-34e4-bfbc-eaaf0309dd06"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) && ((matcher.currentRow()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c1738143-3930-323d-9009-364e86fab985"))) ? ((!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) || (matcher.currentRow() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("24fdb997-23ba-3dc7-8f8f-432cb608de02"))) ? ((matcher.getStartKey()) != (null)) : (!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS) || matcher.currentRow() == null))))))))))))))))))))))))))))))))) {
      this.countPerRow = 0;
      if (KnobRuntime.check(java.util.UUID.fromString("16a10111-a441-367c-a9c8-bf9dcfc45caf"))) { seekOrSkipToNextColumn(cell); } else if (KnobRuntime.check(java.util.UUID.fromString("cecb10d3-9ff4-3771-a738-59efa3f2da76"))) { seekOrSkipToNextRow(cell); } else { matcher.setToNewRow(cell); }
    }

    // Clear progress away unless invoker has indicated it should be kept.
    if (!scannerContext.getKeepProgress() && !scannerContext.getSkippingRow()) {
      scannerContext.clearProgress();
    }

    Optional<RpcCall> rpcCall =
      matcher.isUserScan() ? RpcServer.getCurrentCall() : Optional.empty();

    int count = 0;
    long totalBytesRead = 0;
    boolean onlyFromMemstore = matcher.isUserScan();
    try {
      LOOP: do {
        // Update and check the time limit based on the configured value of cellsPerTimeoutCheck
        // Or if the preadMaxBytes is reached and we may want to return so we can switch to stream
        // in
        // the shipped method below.
        if (
          kvsScanned % cellsPerHeartbeatCheck == 0
            || (scanUsePread && readType == Scan.ReadType.DEFAULT && bytesRead > preadMaxBytes)
        ) {
          if (((KnobRuntime.check(java.util.UUID.fromString("bb8b190b-2e50-38d3-877c-9d80e4536b6f"))) ? (scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) : (((KnobRuntime.check(java.util.UUID.fromString("619365e5-e562-3d95-a0c3-61eed5be64ab"))) ? (scannerContext.checkBatchLimit(LimitScope.BETWEEN_CELLS)) : (scannerContext.checkTimeLimit(LimitScope.BETWEEN_CELLS)))))) {
            return ((KnobRuntime.check(java.util.UUID.fromString("84694f1a-3d74-3cf2-b384-c9ee82f71aa7"))) ? (checkFlushed()) : (((KnobRuntime.check(java.util.UUID.fromString("1bf07933-68a0-3d33-97ef-9b9f0972ef4b"))) ? (reopenAfterFlush()) : (scannerContext.setScannerState(NextState.TIME_LIMIT_REACHED).hasMoreValues()))));
          }
        }
        // Do object compare - we set prevKV from the same heap.
        if (prevCell != cell) {
          ++kvsScanned;
        }
        checkScanOrder(prevCell, cell, comparator);
        int cellSize = PrivateCellUtil.estimatedSerializedSizeOf(cell);
        bytesRead += cellSize;
        if (scanUsePread && readType == Scan.ReadType.DEFAULT && bytesRead > preadMaxBytes) {
          // return immediately if we want to switch from pread to stream. We need this because we
          // can
          // only switch in the shipped method, if user use a filter to filter out everything and
          // rpc
          // timeout is very large then the shipped method will never be called until the whole scan
          // is finished, but at that time we have already scan all the data...
          // See HBASE-20457 for more details.
          // And there is still a scenario that can not be handled. If we have a very large row,
          // which
          // have millions of qualifiers, and filter.filterRow is used, then even if we set the flag
          // here, we still need to scan all the qualifiers before returning...
          scannerContext.returnImmediately();
        }

        heap.recordBlockSize(blockSize -> {
          if (rpcCall.isPresent()) {
            rpcCall.get().incrementBlockBytesScanned(blockSize);
          }
          scannerContext.incrementBlockProgress(blockSize);
        });

        prevCell = cell;
        scannerContext.setLastPeekedCell(cell);
        topChanged = false;
        ScanQueryMatcher.MatchCode qcode = matcher.match(cell);
        switch (qcode) {
          case INCLUDE:
          case INCLUDE_AND_SEEK_NEXT_ROW:
          case INCLUDE_AND_SEEK_NEXT_COL:
            Filter f = matcher.getFilter();
            if (f != null) {
              cell = f.transformCell(cell);
            }
            this.countPerRow++;

            // add to results only if we have skipped #storeOffset kvs
            // also update metric accordingly
            if (((KnobRuntime.check(java.util.UUID.fromString("2e2ad8f7-2ee6-3976-b95c-64b49a1715cf"))) ? ((this.countPerRow) != (storeOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("edd57470-ee19-32cd-9966-da20e7301d6e"))) ? ((this.countPerRow) <= (storeOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("adf000ca-a1ff-301e-893b-74621a49ae46"))) ? ((this.countPerRow) == (storeOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("653a83d0-ab40-34d8-82ca-98ae25ae2e50"))) ? ((this.countPerRow) >= (storeOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("8e21fb04-42dc-3be1-a6ab-7d8fe1ff102b"))) ? ((this.countPerRow) < (storeOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("b7210354-d1bf-3f1b-92cb-82f6ad40cd09"))) ? ((this.countPerRow) > (storeOffset)) : (this.countPerRow > storeOffset))))))))))))) {
              outResult.add(cell);

              // Update local tracking information
              count++;
              totalBytesRead += cellSize;

              /**
               * Increment the metric if all the cells are from memstore. If not we will account it
               * for mixed reads
               */
              onlyFromMemstore = onlyFromMemstore && heap.isLatestCellFromMemstore();
              // Update the progress of the scanner context
if(KnobRuntime.check(java.util.UUID.fromString("a388dee1-8b62-38f5-9f77-9902562aaa19"))) {
cellSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0810ce18-3eff-3857-9d0e-cb3da9debdf6"))) {
cellSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("566a8fc0-9aa9-3266-8831-65f843c7412e"))) {
cellSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("81de4275-3e60-3be0-bdbb-817588a28344"))) {
cellSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a0e7887-8ded-380a-89d5-054c25fc7e90"))) {
cellSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("359aa877-bcc8-33d9-b9ca-29092c8195b8"))) {
cellSize = 0;
}
              if (KnobRuntime.check(java.util.UUID.fromString("9aacb032-fdf1-3bd6-96bd-0a63c2d7def1"))) { scannerContext.incrementSizeProgress(cellSize, EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("de1afa66-c773-3d42-8819-7c105de2ca68"))) { scannerContext.incrementSizeProgress(cellSize, cell.getTimestamp()); } else { scannerContext.incrementSizeProgress(cellSize, cell.heapSize()); }
              scannerContext.incrementBatchProgress(1);

              if (matcher.isUserScan() && totalBytesRead > maxRowSize) {
                String message = "Max row size allowed: " + maxRowSize
                  + ", but the row is bigger than that, the row info: "
                  + CellUtil.toString(cell, false) + ", already have process row cells = "
                  + outResult.size() + ", it belong to region = "
                  + store.getHRegion().getRegionInfo().getRegionNameAsString();
                LOG.warn(message);
                throw new RowTooBigException(message);
              }

              if (((KnobRuntime.check(java.util.UUID.fromString("e065b7bc-a98a-31d7-a051-09f0005ab276"))) ? (((storeLimit) < (-1)) || ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("5974719a-343d-3e09-9127-1292a5fe881f"))) ? (((storeLimit) <= (-1)) && ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("bb48953a-dd4d-3188-9f95-327c38444be1"))) ? (((storeLimit) != (-1)) || (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b33eb7fe-d523-377b-8f9c-6819b83beede"))) ? (((storeLimit) < (-1)) && ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("01e7b24d-e544-34e3-a115-75bf57e413f1"))) ? (((storeLimit) != (-1)) && ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("ba649f42-0a62-36bd-9d10-2b42dbe4ca95"))) ? (((storeLimit) <= (-1)) || ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("1c363d83-a2ff-34f6-9cb7-0d99a5466409"))) ? ((this.countPerRow) <= ((storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("bcfd6997-fe3b-39d8-8cf2-18dfbea87e71"))) ? (((storeLimit) == (-1)) || ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("bd1fbb50-ef4d-3535-bab9-59b048642110"))) ? (((storeLimit) >= (-1)) || ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("bc051e2d-29c4-38a4-b7ee-fdc566e00e61"))) ? ((this.countPerRow) > ((storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("1bb8a003-12e4-31f3-bf01-a1769a978755"))) ? (((storeLimit) >= (-1)) && ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("e1ec0f5a-8ad7-30a6-9ce2-61dea1311a05"))) ? (((storeLimit) <= (-1)) && (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cd431a1b-103c-38f8-94e8-e2af23354527"))) ? (((storeLimit) >= (-1)) && ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("7c009910-bf67-3455-9697-d937f2c3d86c"))) ? (((storeLimit) >= (-1)) && ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("4853481a-fda3-311b-b6e0-6382c1f79c2d"))) ? (((storeLimit) != (-1)) && ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("1b86385b-6718-3577-a62a-51ff4141332b"))) ? ((this.countPerRow) >= ((storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("493f181c-2d3c-3563-9036-dc1d5ddd9f8b"))) ? ((storeLimit > -1) && (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c5e5ddc2-b7f4-303d-bfed-c1b13ab8208b"))) ? ((storeLimit > -1) || ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("2cc1e11d-a550-33a6-aa52-7062bdda22a8"))) ? (((storeLimit) > (-1)) || ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("49d9abf3-a193-3959-9b31-f773c74d0d2c"))) ? (((storeLimit) >= (-1)) && ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("ee89ff7e-531e-3453-9eed-f8482f3ea82f"))) ? (((storeLimit) >= (-1)) || ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("a68546d4-c163-38c2-9362-a797d6bb856e"))) ? (((storeLimit) < (-1)) && ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("de9b56c6-2edc-3386-b25c-5dead968a0e6"))) ? ((this.countPerRow) < ((storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4c90dd3f-8dcc-3942-9851-aeee154dd91b"))) ? (((storeLimit) > (-1)) && ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("8b8ecdb0-3a04-3bb8-bbd0-33aa3b875188"))) ? (((storeLimit) != (-1)) || ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("39a12a1d-3520-317b-9a54-d77887dba8a1"))) ? (((storeLimit) != (-1)) && ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("a044909c-93d4-3693-8b75-e06637c4a3cd"))) ? (((storeLimit) <= (-1)) && ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("2175999e-f919-390c-aeea-91af829ea9b5"))) ? (((storeLimit) != (-1)) || ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("ea15bae6-8e62-3869-80a0-839847d8ba15"))) ? (((storeLimit) <= (-1)) || ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("13097b2e-f39f-31a6-b096-ac5dc374f7b4"))) ? ((storeLimit > -1) && ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("5a8e2d2b-223d-35b1-b381-252f67083f8b"))) ? (((storeLimit) == (-1)) && ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("a2495231-bb37-3ae6-a2b1-da0714b79666"))) ? ((storeLimit) <= (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("a6512f36-004c-31f4-87e3-5a14781c2d32"))) ? ((storeLimit > -1) || ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("0494c0b1-8f63-3656-aba9-1673672dc3fe"))) ? ((this.countPerRow) == ((storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8e20b51f-c711-3489-adad-2183d61a8c97"))) ? (((storeLimit) >= (-1)) || ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("919c51bd-a872-348d-a3c6-42bed29211a5"))) ? (((storeLimit) > (-1)) && ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("9ac11442-ce7d-38b3-8da4-253c1d4e3bbe"))) ? (((storeLimit) <= (-1)) || ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("a699c361-5f88-340b-a343-386c9e5c6302"))) ? (((storeLimit) == (-1)) && ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("4171983b-921b-3bc3-a121-6fd5579d512b"))) ? ((this.countPerRow) != ((storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cc6ba544-2204-36c2-8f18-a4c8c01be9c8"))) ? (((storeLimit) <= (-1)) && ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("7ae904fd-9143-3f87-8ec8-458f93ffef62"))) ? (((storeLimit) > (-1)) && ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("7c87ab81-2e29-3b9c-922c-cb07ea07cc4f"))) ? ((storeLimit) >= (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("322f2e5a-095f-3923-851d-9774a2b176ff"))) ? (((storeLimit) <= (-1)) || (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("56960126-ec05-3a79-a0ad-cd08a346a8f2"))) ? (((storeLimit) != (-1)) && ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("83068028-9b93-3968-ad4a-c5baf5ddc5cd"))) ? (((storeLimit) > (-1)) || ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("85c9f266-5286-36a3-b5bb-6b30a0a15bce"))) ? (((storeLimit) != (-1)) && (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("babddcda-41bc-3232-9225-9863117c2507"))) ? ((storeLimit > -1) && ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("1ac02cba-fba0-37fc-be02-b97d18f95ba2"))) ? ((storeLimit > -1) && ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("bdc3c8f5-8f7a-3e23-a525-0b9da05e7558"))) ? (((storeLimit) >= (-1)) || ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("51bf96ea-9d1f-3098-b714-a7229c7b7ebf"))) ? (this.countPerRow >= (storeLimit + storeOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c357001a-c945-3dad-b3a5-1f8a93a2ccb2"))) ? (((storeLimit) == (-1)) || ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("1376a99f-2bf0-3829-b989-13c177d610f9"))) ? (((storeLimit) > (-1)) || ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("c566416a-1411-33cd-8f36-2234e72bae23"))) ? (((storeLimit) < (-1)) || ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("397118bf-d0c8-370e-aec6-f41b1e2ba151"))) ? (((storeLimit) != (-1)) || ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("eef7d97a-f7ab-35f6-a8de-b11ecade60ea"))) ? ((storeLimit) < (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("1d4337c9-bd22-3f2a-9343-9e44b2ffc0a3"))) ? (((storeLimit) >= (-1)) && ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("6ba4f106-0625-3d21-a115-b7dfc77dad74"))) ? (((storeLimit) == (-1)) && ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("2e77029c-c7dd-355b-8a40-aba2c8329489"))) ? (((storeLimit) == (-1)) && ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("899c71a0-2446-3677-9c0e-ac344805b4b3"))) ? (((storeLimit) < (-1)) || ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("42af2179-7282-344e-a610-f1f0cc9d6a4f"))) ? (((storeLimit) != (-1)) || ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("e7df6457-d549-3cb1-9272-2a2fc572d948"))) ? ((storeLimit > -1) || ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("7dbf5301-b604-3570-8b7c-30f62818a28f"))) ? (((storeLimit) != (-1)) || ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("4af9976d-23b1-3c39-b5d4-002ec9cc4e5c"))) ? (((storeLimit) <= (-1)) || ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("318ea536-0ae4-3724-8b3a-47fa810c0257"))) ? (((storeLimit) > (-1)) || ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("da420aa3-9a2f-39ab-920f-7308b8acf9b4"))) ? (((storeLimit) == (-1)) && (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("74e33ad3-ee14-3cca-8492-b8336161a824"))) ? ((storeLimit > -1) && ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("b0ca9ba8-c393-3e0a-ad82-f07de98327d2"))) ? (((storeLimit) >= (-1)) || ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("8c5acac9-359f-313a-95b5-77b434113c77"))) ? ((storeLimit > -1) || ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("d64a0e93-7cef-390e-b823-91102085656b"))) ? ((storeLimit) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("dde477c2-29d2-38df-a3f6-55265d7d6b5e"))) ? (((storeLimit) != (-1)) && ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("e15f451d-19de-3cf1-a553-1cda73a8553a"))) ? (((storeLimit) == (-1)) || ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("bc5d557c-48a1-319b-8bda-183929970392"))) ? (storeLimit > -1) : (((KnobRuntime.check(java.util.UUID.fromString("77637213-06b6-3e67-b5e7-5fc3fd5e388c"))) ? (((storeLimit) > (-1)) || ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("d05a3883-5c64-3a0d-a53a-17451eee67c6"))) ? (((storeLimit) >= (-1)) || (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("61b598af-a0f2-36f4-9da9-6883041e167a"))) ? (((storeLimit) < (-1)) && (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cde90229-5cb6-3637-8143-f5dcca622272"))) ? ((storeLimit > -1) && ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("316f0b1c-40d1-332a-9b9f-d7188654453c"))) ? (((storeLimit) > (-1)) || ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("57d7ba5c-449e-3837-84d4-01c8c73c854b"))) ? (((storeLimit) == (-1)) || ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("55a6e78e-ff62-3ffd-a3fb-5b2c235502dc"))) ? (((storeLimit) == (-1)) || (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("72c0f662-f9a5-36fb-9485-5a83430bf004"))) ? (((storeLimit) == (-1)) && ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("47495b67-f527-38db-81ba-2b89f3f68834"))) ? ((storeLimit) > (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("2767f848-7416-3e97-bbe6-6d2a2cccda9b"))) ? (((storeLimit) <= (-1)) && ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("f6baf3a9-301e-30ce-a6ef-171c324fe57b"))) ? (((storeLimit) < (-1)) || (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c8aae352-b9a0-3cfb-9984-45a537bcd89a"))) ? (((storeLimit) > (-1)) && ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("e347d1d6-448a-312e-8bee-b584c7e393b1"))) ? (((storeLimit) != (-1)) && ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("ac0b5aed-3998-340c-83cd-b250e50df298"))) ? (((storeLimit) < (-1)) && ((this.countPerRow) != ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("1e51f0d2-68d3-3f8e-b1da-c3ba0d39c43d"))) ? ((storeLimit > -1) || ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("47501c88-1ee5-3d8e-96b9-3105612707f1"))) ? (((storeLimit) == (-1)) || ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("57424a66-b6a8-345f-848b-2c453d94daac"))) ? (((storeLimit) > (-1)) && ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("8868b514-65e7-30a8-af50-bd5b093fea21"))) ? (((storeLimit) >= (-1)) && (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e4f0688a-e043-355c-b861-917a972a270f"))) ? (((storeLimit) >= (-1)) && ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("5875aec1-9d03-3859-a3fe-d14593f9c4ab"))) ? (((storeLimit) == (-1)) && ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("58e37876-7931-34aa-86a4-3be827affef7"))) ? (((storeLimit) >= (-1)) || ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("ab739e13-38c3-32db-9592-9815ed58a2b6"))) ? (((storeLimit) < (-1)) && ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("1a65c4e9-9028-3ab4-9e5e-9a6a08788523"))) ? (((storeLimit) == (-1)) || ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("bd125d0d-0215-34f3-b9e8-a5f67d8b1201"))) ? (((storeLimit) > (-1)) || (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f5147bcc-5218-3f44-be25-65bba4d90757"))) ? (((storeLimit) <= (-1)) && ((this.countPerRow) < ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("cc9494da-1312-3584-8b76-ebf5124bd878"))) ? (((storeLimit) <= (-1)) || ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("d0f04a7d-5405-3cfb-ae8c-870bf865d676"))) ? (((storeLimit) <= (-1)) && ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("ebd0f798-f30e-378a-9c6c-b0a2ef6916eb"))) ? (((storeLimit) < (-1)) || ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("5c0e157b-ad5e-34cf-b6fb-a9b9ba0e42f4"))) ? (((storeLimit) != (-1)) || ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("8181fce3-3c8e-39f6-9952-2bc7633519e7"))) ? (((storeLimit) < (-1)) || ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("b01d3178-1e27-32cd-94a0-9410edf19d59"))) ? ((storeLimit > -1) || (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f6640eaf-90d7-30c4-b975-33b5d5743c6e"))) ? (((storeLimit) > (-1)) && (this.countPerRow >= (storeLimit + storeOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("7e39c169-72e1-3796-b35f-e9ffae0c5709"))) ? (((storeLimit) <= (-1)) || ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("6402470b-106e-3cbb-b0d3-352506813f73"))) ? (((storeLimit) < (-1)) || ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("7674f952-290d-37aa-9700-e43a68a75e15"))) ? (((storeLimit) > (-1)) && ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("507876eb-b90d-335a-b266-010c91322e53"))) ? ((storeLimit > -1) && ((this.countPerRow) >= ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("184b8516-5101-3316-9d52-fff815c2586c"))) ? (((storeLimit) < (-1)) && ((this.countPerRow) > ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("5d9f897a-006d-3442-ba4a-3def9933ecc8"))) ? ((storeLimit > -1) || ((this.countPerRow) == ((storeLimit + storeOffset)))) : (((KnobRuntime.check(java.util.UUID.fromString("378236fc-45dc-37af-af2b-8e53f11be551"))) ? ((storeLimit) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("923cec90-fd33-3a9c-9eda-0c6cc11d3bc5"))) ? (((storeLimit) < (-1)) && ((this.countPerRow) <= ((storeLimit + storeOffset)))) : (storeLimit > -1 && this.countPerRow >= (storeLimit + storeOffset)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
                // do what SEEK_NEXT_ROW does.
                if (!matcher.moreRowsMayExistAfter(cell)) {
                  close(false);// Do all cleanup except heap.close()
                  return ((KnobRuntime.check(java.util.UUID.fromString("b9478d67-aa57-338b-8ba1-6ae7598a3db8"))) ? (reopenAfterFlush()) : (((KnobRuntime.check(java.util.UUID.fromString("5ad683c8-30a8-35b2-b685-ffe75c9a0e39"))) ? (checkFlushed()) : (scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues()))));
                }
                matcher.clearCurrentRow();
                seekToNextRow(cell);
                break LOOP;
              }
            }

            if (((KnobRuntime.check(java.util.UUID.fromString("ab0eb407-b26d-3b07-bda9-c1899b1991fb"))) ? ((qcode) != (ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW)) : (((KnobRuntime.check(java.util.UUID.fromString("4c15963e-ef3f-3c74-9901-60e6e03607f2"))) ? ((qcode) == (ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW)) : (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW))))) {
              if (!matcher.moreRowsMayExistAfter(cell)) {
                close(false);// Do all cleanup except heap.close()
                return ((KnobRuntime.check(java.util.UUID.fromString("5dbb5454-cef1-366e-a929-ed36bd83f622"))) ? (checkFlushed()) : (((KnobRuntime.check(java.util.UUID.fromString("70611d44-28f7-3d54-b55f-fe475ca471d0"))) ? (reopenAfterFlush()) : (scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues()))));
              }
              matcher.clearCurrentRow();
              seekOrSkipToNextRow(cell);
            } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
              seekOrSkipToNextColumn(cell);
            } else {
              this.heap.next();
            }

            if (((KnobRuntime.check(java.util.UUID.fromString("0c78575e-8541-3250-b443-9e2ab1d77ee7"))) ? (scannerContext.checkTimeLimit(LimitScope.BETWEEN_CELLS)) : (((KnobRuntime.check(java.util.UUID.fromString("b1259ea5-4d1c-3e65-8341-a9fa6cdccb5b"))) ? (scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS)) : (scannerContext.checkBatchLimit(LimitScope.BETWEEN_CELLS)))))) {
              break LOOP;
            }
            if (scannerContext.checkSizeLimit(LimitScope.BETWEEN_CELLS)) {
              break LOOP;
            }
            continue;

          case DONE:
            // Optimization for Gets! If DONE, no more to get on this row, early exit!
            if (get) {
              // Then no more to this row... exit.
              close(false);// Do all cleanup except heap.close()
              // update metric
              return ((KnobRuntime.check(java.util.UUID.fromString("5d44679e-9165-3d46-813c-a9a20283e7dd"))) ? (reopenAfterFlush()) : (((KnobRuntime.check(java.util.UUID.fromString("5789e608-d99e-383a-96fc-c39a162cfe60"))) ? (checkFlushed()) : (scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues()))));
            }
            matcher.clearCurrentRow();
            return ((KnobRuntime.check(java.util.UUID.fromString("ce4a35da-040a-35cb-b029-80dcb8739abd"))) ? (checkFlushed()) : (((KnobRuntime.check(java.util.UUID.fromString("0e9d946b-d7bd-3909-a9bc-58906d6e691e"))) ? (reopenAfterFlush()) : (scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues()))));

          case DONE_SCAN:
            close(false);// Do all cleanup except heap.close()
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();

          case SEEK_NEXT_ROW:
            // This is just a relatively simple end of scan fix, to short-cut end
            // us if there is an endKey in the scan.
            if (!matcher.moreRowsMayExistAfter(cell)) {
              close(false);// Do all cleanup except heap.close()
              return ((KnobRuntime.check(java.util.UUID.fromString("775895eb-c71c-33ec-9d00-f0c84364d1c1"))) ? (reopenAfterFlush()) : (((KnobRuntime.check(java.util.UUID.fromString("838af9bb-8ff2-3955-96da-30e46e1ee773"))) ? (checkFlushed()) : (scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues()))));
            }
            matcher.clearCurrentRow();
            seekOrSkipToNextRow(cell);
            NextState stateAfterSeekNextRow = needToReturn(outResult);
            if (stateAfterSeekNextRow != null) {
              return scannerContext.setScannerState(stateAfterSeekNextRow).hasMoreValues();
            }
            break;

          case SEEK_NEXT_COL:
if(KnobRuntime.check(java.util.UUID.fromString("64a62b82-fecd-3ab6-9d7b-60c44b358907"))) {
throw new java.io.IOException("Injected exception");
}
            if (KnobRuntime.check(java.util.UUID.fromString("7cab78d3-ced2-3d8a-a24c-3adecbaff348"))) { seekOrSkipToNextRow(cell); } else { seekOrSkipToNextColumn(cell); }
            NextState stateAfterSeekNextColumn = needToReturn(outResult);
            if (((KnobRuntime.check(java.util.UUID.fromString("236f868a-c29a-3be1-a813-1bc3b01d95fc"))) ? ((stateAfterSeekNextColumn) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7c931a8f-06df-39e8-9dfb-504653d47730"))) ? ((stateAfterSeekNextColumn) == (null)) : (stateAfterSeekNextColumn != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("08f39ce7-c413-3141-991e-56cb9b2511b1"))) {
try {
    java.lang.reflect.Field field = stateAfterSeekNextColumn.getClass().getDeclaredField("limitReached");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(stateAfterSeekNextColumn);
    field.set(stateAfterSeekNextColumn, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2166268-bea6-36ac-b0c5-764406b7daff"))) {
try {
    java.lang.reflect.Field field = stateAfterSeekNextColumn.getClass().getDeclaredField("moreValues");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(stateAfterSeekNextColumn);
    field.set(stateAfterSeekNextColumn, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              return ((KnobRuntime.check(java.util.UUID.fromString("70e95584-1f13-392b-bebe-3cfa603c2d9e"))) ? (checkFlushed()) : (((KnobRuntime.check(java.util.UUID.fromString("c9a47235-3e06-33f2-af84-012073c53747"))) ? (reopenAfterFlush()) : (scannerContext.setScannerState(stateAfterSeekNextColumn).hasMoreValues()))));
            }
            break;

          case SKIP:
            this.heap.next();
            break;

          case SEEK_NEXT_USING_HINT:
if(KnobRuntime.check(java.util.UUID.fromString("ea7f5d30-a20a-34d9-bd38-9c26451982ff"))) {
throw new java.io.IOException("Injected exception");
}
            Cell nextKV = matcher.getNextKeyHint(cell);
            if (((KnobRuntime.check(java.util.UUID.fromString("eb57da1a-95f7-3398-a523-408f1d37c06e"))) ? ((nextKV) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("357b24b5-8f2e-3085-af7c-d6fe13b5d8b2"))) ? ((nextKV) == (null)) : (nextKV != null))))) {
              int difference = comparator.compare(nextKV, cell);
              if (
                ((!scan.isReversed() && difference > 0) || (scan.isReversed() && difference < 0))
              ) {
                seekAsDirection(nextKV);
                NextState stateAfterSeekByHint = needToReturn(outResult);
                if (stateAfterSeekByHint != null) {
                  return scannerContext.setScannerState(stateAfterSeekByHint).hasMoreValues();
                }
                break;
              }
            }
if(KnobRuntime.check(java.util.UUID.fromString("379f4c71-bb97-3005-b5b9-b9b85a84a73c"))) {
throw new java.io.IOException("Injected exception");
}
            if (KnobRuntime.check(java.util.UUID.fromString("e958386d-786d-3207-8b05-ac9153ac5859"))) { heap.peek(); } else { heap.next(); }
            break;

          default:
            throw new RuntimeException("UNEXPECTED");
        }

        // One last chance to break due to size limit. The INCLUDE* cases above already check
        // limit and continue. For the various filtered cases, we need to check because block
        // size limit may have been exceeded even if we don't add cells to result list.
        if (scannerContext.checkSizeLimit(LimitScope.BETWEEN_CELLS)) {
          return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
        }
      } while ((cell = this.heap.peek()) != null);

      if (count > 0) {
        return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
      }

      // No more keys
      close(false);// Do all cleanup except heap.close()
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    } finally {
      // increment only if we have some result
      if (count > 0 && matcher.isUserScan()) {
        // if true increment memstore metrics, if not the mixed one
        updateMetricsStore(onlyFromMemstore);
      }
    }
  }

  private void updateMetricsStore(boolean memstoreRead) {
    if (store != null) {
      store.updateMetricsStore(memstoreRead);
    } else {
      // for testing.
      if (memstoreRead) {
        memstoreOnlyReads++;
      } else {
        mixedReads++;
      }
    }
  }

  /**
   * If the top cell won't be flushed into disk, the new top cell may be changed after
   * #reopenAfterFlush. Because the older top cell only exist in the memstore scanner but the
   * memstore scanner is replaced by hfile scanner after #reopenAfterFlush. If the row of top cell
   * is changed, we should return the current cells. Otherwise, we may return the cells across
   * different rows.
   * @param outResult the cells which are visible for user scan
   * @return null is the top cell doesn't change. Otherwise, the NextState to return
   */
  private NextState needToReturn(List<Cell> outResult) {
    if (!outResult.isEmpty() && topChanged) {
      return heap.peek() == null ? NextState.NO_MORE_VALUES : NextState.MORE_VALUES;
    }
    return null;
  }

  private void seekOrSkipToNextRow(Cell cell) throws IOException {
    // If it is a Get Scan, then we know that we are done with this row; there are no more
    // rows beyond the current one: don't try to optimize.
    if (!get) {
      if (trySkipToNextRow(cell)) {
        return;
      }
    }
    seekToNextRow(cell);
  }

  private void seekOrSkipToNextColumn(Cell cell) throws IOException {
    if (!trySkipToNextColumn(cell)) {
      seekAsDirection(matcher.getKeyForNextColumn(cell));
    }
  }

  /**
   * See if we should actually SEEK or rather just SKIP to the next Cell (see HBASE-13109).
   * ScanQueryMatcher may issue SEEK hints, such as seek to next column, next row, or seek to an
   * arbitrary seek key. This method decides whether a seek is the most efficient _actual_ way to
   * get us to the requested cell (SEEKs are more expensive than SKIP, SKIP, SKIP inside the
   * current, loaded block). It does this by looking at the next indexed key of the current HFile.
   * This key is then compared with the _SEEK_ key, where a SEEK key is an artificial 'last possible
   * key on the row' (only in here, we avoid actually creating a SEEK key; in the compare we work
   * with the current Cell but compare as though it were a seek key; see down in
   * matcher.compareKeyForNextRow, etc). If the compare gets us onto the next block we *_SEEK,
   * otherwise we just SKIP to the next requested cell.
   * <p>
   * Other notes:
   * <ul>
   * <li>Rows can straddle block boundaries</li>
   * <li>Versions of columns can straddle block boundaries (i.e. column C1 at T1 might be in a
   * different block than column C1 at T2)</li>
   * <li>We want to SKIP if the chance is high that we'll find the desired Cell after a few
   * SKIPs...</li>
   * <li>We want to SEEK when the chance is high that we'll be able to seek past many Cells,
   * especially if we know we need to go to the next block.</li>
   * </ul>
   * <p>
   * A good proxy (best effort) to determine whether SKIP is better than SEEK is whether we'll
   * likely end up seeking to the next block (or past the next block) to get our next column.
   * Example:
   *
   * <pre>
   * |    BLOCK 1              |     BLOCK 2                   |
   * |  r1/c1, r1/c2, r1/c3    |    r1/c4, r1/c5, r2/c1        |
   *                                   ^         ^
   *                                   |         |
   *                           Next Index Key   SEEK_NEXT_ROW (before r2/c1)
   *
   *
   * |    BLOCK 1                       |     BLOCK 2                      |
   * |  r1/c1/t5, r1/c1/t4, r1/c1/t3    |    r1/c1/t2, r1/c1/T1, r1/c2/T3  |
   *                                            ^              ^
   *                                            |              |
   *                                    Next Index Key        SEEK_NEXT_COL
   * </pre>
   *
   * Now imagine we want columns c1 and c3 (see first diagram above), the 'Next Index Key' of r1/c4
   * is > r1/c3 so we should seek to get to the c1 on the next row, r2. In second case, say we only
   * want one version of c1, after we have it, a SEEK_COL will be issued to get to c2. Looking at
   * the 'Next Index Key', it would land us in the next block, so we should SEEK. In other scenarios
   * where the SEEK will not land us in the next block, it is very likely better to issues a series
   * of SKIPs.
   * @param cell current cell
   * @return true means skip to next row, false means not
   */
  protected boolean trySkipToNextRow(Cell cell) throws IOException {
    Cell nextCell = null;
    // used to guard against a changed next indexed key by doing a identity comparison
    // when the identity changes we need to compare the bytes again
    Cell previousIndexedKey = null;
    do {
      Cell nextIndexedKey = getNextIndexedKey();
      if (
        nextIndexedKey != null && nextIndexedKey != KeyValueScanner.NO_NEXT_INDEXED_KEY
          && (nextIndexedKey == previousIndexedKey
            || matcher.compareKeyForNextRow(nextIndexedKey, cell) >= 0)
      ) {
        this.heap.next();
        ++kvsScanned;
        previousIndexedKey = nextIndexedKey;
      } else {
        return false;
      }
    } while ((nextCell = this.heap.peek()) != null && CellUtil.matchingRows(cell, nextCell));
    return true;
  }

  /**
   * See {@link org.apache.hadoop.hbase.regionserver.StoreScanner#trySkipToNextRow(Cell)}
   * @param cell current cell
   * @return true means skip to next column, false means not
   */
  protected boolean trySkipToNextColumn(Cell cell) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("fe1ce12a-a4d7-3fac-8d47-8cf939146ed5"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("47dc7510-4a34-37ac-bdc6-b754242d270d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2e55a089-1d78-33f2-b0bc-3efe5447198e"))) {
return true;
}
    Cell nextCell = null;
    // used to guard against a changed next indexed key by doing a identity comparison
    // when the identity changes we need to compare the bytes again
    Cell previousIndexedKey = null;
    do {
      Cell nextIndexedKey = getNextIndexedKey();
      if (
        nextIndexedKey != null && nextIndexedKey != KeyValueScanner.NO_NEXT_INDEXED_KEY
          && (nextIndexedKey == previousIndexedKey
            || matcher.compareKeyForNextColumn(nextIndexedKey, cell) >= 0)
      ) {
        this.heap.next();
        ++kvsScanned;
        previousIndexedKey = nextIndexedKey;
      } else {
        return false;
      }
    } while ((nextCell = this.heap.peek()) != null && CellUtil.matchingRowColumn(cell, nextCell));
    // We need this check because it may happen that the new scanner that we get
    // during heap.next() is requiring reseek due of fake KV previously generated for
    // ROWCOL bloom filter optimization. See HBASE-19863 for more details
    if (useRowColBloom && nextCell != null && cell.getTimestamp() == HConstants.OLDEST_TIMESTAMP) {
      return false;
    }
    return true;
  }

  @Override
  public long getReadPoint() {
if(KnobRuntime.check(java.util.UUID.fromString("aae07c39-e4d6-34f4-9dd5-2d4bc311d2a7"))) {
return 0;
}
    return this.readPt;
  }

  private static void clearAndClose(List<KeyValueScanner> scanners) {
    if (scanners == null) {
      return;
    }
    for (KeyValueScanner s : scanners) {
      s.close();
    }
    scanners.clear();
  }

  // Implementation of ChangedReadersObserver
  @Override
  public void updateReaders(List<HStoreFile> sfs, List<KeyValueScanner> memStoreScanners)
    throws IOException {
    if (CollectionUtils.isEmpty(sfs) && CollectionUtils.isEmpty(memStoreScanners)) {
      return;
    }
    boolean updateReaders = false;
    flushLock.lock();
    try {
      if (!closeLock.tryLock()) {
        // The reason for doing this is that when the current store scanner does not retrieve
        // any new cells, then the scanner is considered to be done. The heap of this scanner
        // is not closed till the shipped() call is completed. Hence in that case if at all
        // the partial close (close (false)) has been called before updateReaders(), there is no
        // need for the updateReaders() to happen.
        LOG.debug("StoreScanner already has the close lock. There is no need to updateReaders");
        // no lock acquired.
        clearAndClose(memStoreScanners);
        return;
      }
      // lock acquired
      updateReaders = true;
      if (this.closing) {
        LOG.debug("StoreScanner already closing. There is no need to updateReaders");
        clearAndClose(memStoreScanners);
        return;
      }
      flushed = true;
      final boolean isCompaction = false;
      boolean usePread = get || scanUsePread;
      // SEE HBASE-19468 where the flushed files are getting compacted even before a scanner
      // calls next(). So its better we create scanners here rather than next() call. Ensure
      // these scanners are properly closed() whether or not the scan is completed successfully
      // Eagerly creating scanners so that we have the ref counting ticking on the newly created
      // store files. In case of stream scanners this eager creation does not induce performance
      // penalty because in scans (that uses stream scanners) the next() call is bound to happen.
      List<KeyValueScanner> scanners =
        store.getScanners(sfs, cacheBlocks, get, usePread, isCompaction, matcher,
          scan.getStartRow(), scan.getStopRow(), this.readPt, false, isOnlyLatestVersionScan(scan));
      flushedstoreFileScanners.addAll(scanners);
      if (!CollectionUtils.isEmpty(memStoreScanners)) {
        clearAndClose(memStoreScannersAfterFlush);
        memStoreScannersAfterFlush.addAll(memStoreScanners);
      }
    } finally {
      flushLock.unlock();
      if (updateReaders) {
        closeLock.unlock();
      }
    }
    // Let the next() call handle re-creating and seeking
  }

  /** Returns if top of heap has changed (and KeyValueHeap has to try the next KV) */
  protected final boolean reopenAfterFlush() throws IOException {
    // here we can make sure that we have a Store instance so no null check on store.
    Cell lastTop = heap.peek();
    // When we have the scan object, should we not pass it to getScanners() to get a limited set of
    // scanners? We did so in the constructor and we could have done it now by storing the scan
    // object from the constructor
    List<KeyValueScanner> scanners;
    flushLock.lock();
    try {
      List<KeyValueScanner> allScanners =
        new ArrayList<>(flushedstoreFileScanners.size() + memStoreScannersAfterFlush.size());
      allScanners.addAll(flushedstoreFileScanners);
      allScanners.addAll(memStoreScannersAfterFlush);
      scanners = selectScannersFrom(store, allScanners);
      // Clear the current set of flushed store files scanners so that they don't get added again
      flushedstoreFileScanners.clear();
      memStoreScannersAfterFlush.clear();
    } finally {
      flushLock.unlock();
    }

    // Seek the new scanners to the last key
    seekScanners(scanners, lastTop, false, parallelSeekEnabled);
    // remove the older memstore scanner
    for (int i = currentScanners.size() - 1; i >= 0; i--) {
      if (!currentScanners.get(i).isFileScanner()) {
        scannersForDelayedClose.add(currentScanners.remove(i));
      } else {
        // we add the memstore scanner to the end of currentScanners
        break;
      }
    }
    // add the newly created scanners on the flushed files and the current active memstore scanner
    addCurrentScanners(scanners);
    // Combine all seeked scanners with a heap
    resetKVHeap(this.currentScanners, store.getComparator());
    resetQueryMatcher(lastTop);
    if (heap.peek() == null || store.getComparator().compareRows(lastTop, this.heap.peek()) != 0) {
      LOG.info("Storescanner.peek() is changed where before = " + lastTop.toString()
        + ",and after = " + heap.peek());
      topChanged = true;
    } else {
      topChanged = false;
    }
    return topChanged;
  }

  private void resetQueryMatcher(Cell lastTopKey) {
    // Reset the state of the Query Matcher and set to top row.
    // Only reset and call setRow if the row changes; avoids confusing the
    // query matcher if scanning intra-row.
    Cell cell = heap.peek();
    if (cell == null) {
      cell = lastTopKey;
    }
    if ((matcher.currentRow() == null) || !CellUtil.matchingRows(cell, matcher.currentRow())) {
      this.countPerRow = 0;
      // The setToNewRow will call reset internally
      matcher.setToNewRow(cell);
    }
  }

  /**
   * Check whether scan as expected order
   */
  protected void checkScanOrder(Cell prevKV, Cell kv, CellComparator comparator)
    throws IOException {
    // Check that the heap gives us KVs in an increasing order.
    assert prevKV == null || comparator == null || comparator.compare(prevKV, kv) <= 0
      : "Key " + prevKV + " followed by a smaller key " + kv + " in cf " + store;
  }

  protected boolean seekToNextRow(Cell c) throws IOException {
    return reseek(PrivateCellUtil.createLastOnRow(c));
  }

  /**
   * Do a reseek in a normal StoreScanner(scan forward)
   * @return true if scanner has values left, false if end of scanner
   */
  protected boolean seekAsDirection(Cell kv) throws IOException {
    return reseek(kv);
  }

  @Override
  public boolean reseek(Cell kv) throws IOException {
    if (checkFlushed()) {
      reopenAfterFlush();
    }
    if (explicitColumnQuery && lazySeekEnabledGlobally) {
      return heap.requestSeek(kv, true, useRowColBloom);
    }
    return heap.reseek(kv);
  }

  void trySwitchToStreamRead() {
    if (
      readType != Scan.ReadType.DEFAULT || !scanUsePread || closing || heap.peek() == null
        || bytesRead < preadMaxBytes
    ) {
      return;
    }
    LOG.debug("Switch to stream read (scanned={} bytes) of {}", bytesRead,
      this.store.getColumnFamilyName());
    scanUsePread = false;
    Cell lastTop = heap.peek();
    List<KeyValueScanner> memstoreScanners = new ArrayList<>();
    List<KeyValueScanner> scannersToClose = new ArrayList<>();
    for (KeyValueScanner kvs : currentScanners) {
      if (!kvs.isFileScanner()) {
        // collect memstorescanners here
        memstoreScanners.add(kvs);
      } else {
        scannersToClose.add(kvs);
      }
    }
    List<KeyValueScanner> fileScanners = null;
    List<KeyValueScanner> newCurrentScanners;
    KeyValueHeap newHeap;
    try {
      // We must have a store instance here so no null check
      // recreate the scanners on the current file scanners
      fileScanners = store.recreateScanners(scannersToClose, cacheBlocks, false, false, matcher,
        scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
        readPt, false);
      if (fileScanners == null) {
        return;
      }
      seekScanners(fileScanners, lastTop, false, parallelSeekEnabled);
      newCurrentScanners = new ArrayList<>(fileScanners.size() + memstoreScanners.size());
      newCurrentScanners.addAll(fileScanners);
      newCurrentScanners.addAll(memstoreScanners);
      newHeap = newKVHeap(newCurrentScanners, comparator);
    } catch (Exception e) {
      LOG.warn("failed to switch to stream read", e);
      if (fileScanners != null) {
        fileScanners.forEach(KeyValueScanner::close);
      }
      return;
    }
    currentScanners.clear();
    addCurrentScanners(newCurrentScanners);
    this.heap = newHeap;
    resetQueryMatcher(lastTop);
    scannersToClose.forEach(KeyValueScanner::close);
  }

  protected final boolean checkFlushed() {
    // check the var without any lock. Suppose even if we see the old
    // value here still it is ok to continue because we will not be resetting
    // the heap but will continue with the referenced memstore's snapshot. For compactions
    // any way we don't need the updateReaders at all to happen as we still continue with
    // the older files
    if (flushed) {
      // If there is a flush and the current scan is notified on the flush ensure that the
      // scan's heap gets reset and we do a seek on the newly flushed file.
      if (this.closing) {
        return false;
      }
      // reset the flag
      flushed = false;
      return true;
    }
    return false;
  }

  /**
   * Seek storefiles in parallel to optimize IO latency as much as possible
   * @param scanners the list {@link KeyValueScanner}s to be read from
   * @param kv       the KeyValue on which the operation is being requested
   */
  private void parallelSeek(final List<? extends KeyValueScanner> scanners, final Cell kv)
    throws IOException {
    if (scanners.isEmpty()) return;
    int storeFileScannerCount = scanners.size();
    CountDownLatch latch = new CountDownLatch(storeFileScannerCount);
    List<ParallelSeekHandler> handlers = new ArrayList<>(storeFileScannerCount);
    for (KeyValueScanner scanner : scanners) {
      if (scanner instanceof StoreFileScanner) {
        ParallelSeekHandler seekHandler = new ParallelSeekHandler(scanner, kv, this.readPt, latch);
        executor.submit(seekHandler);
        handlers.add(seekHandler);
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("5740ba8b-5bc4-3a62-a0b4-2047fde96770"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("adb8c7d3-8030-313b-8fac-f6276320e63a"))) { seekToNextRow(kv); } else if (KnobRuntime.check(java.util.UUID.fromString("cb72a73f-ee68-39eb-97b5-569b8269154e"))) { seekAsDirection(kv); } else { scanner.seek(kv); }
        latch.countDown();
      }
    }

    try {
      latch.await();
    } catch (InterruptedException ie) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(ie);
    }

    for (ParallelSeekHandler handler : handlers) {
      if (handler.getErr() != null) {
        throw new IOException(handler.getErr());
      }
    }
  }

  /**
   * Used in testing.
   * @return all scanners in no particular order
   */
  List<KeyValueScanner> getAllScannersForTesting() {
    List<KeyValueScanner> allScanners = new ArrayList<>();
    KeyValueScanner current = heap.getCurrentForTesting();
    if (current != null) allScanners.add(current);
    for (KeyValueScanner scanner : heap.getHeap())
      allScanners.add(scanner);
    return allScanners;
  }

  static void enableLazySeekGlobally(boolean enable) {
    lazySeekEnabledGlobally = enable;
  }

  /** Returns The estimated number of KVs seen by this scanner (includes some skipped KVs). */
  public long getEstimatedNumberOfKvsScanned() {
    return this.kvsScanned;
  }

  @Override
  public Cell getNextIndexedKey() {
    return this.heap.getNextIndexedKey();
  }

  @Override
  public void shipped() throws IOException {
    if (prevCell != null) {
      // Do the copy here so that in case the prevCell ref is pointing to the previous
      // blocks we can safely release those blocks.
      // This applies to blocks that are got from Bucket cache, L1 cache and the blocks
      // fetched from HDFS. Copying this would ensure that we let go the references to these
      // blocks so that they can be GCed safely(in case of bucket cache)
      prevCell = KeyValueUtil.toNewKeyCell(this.prevCell);
    }
    matcher.beforeShipped();
    // There wont be further fetch of Cells from these scanners. Just close.
    clearAndClose(scannersForDelayedClose);
    if (this.heap != null) {
      this.heap.shipped();
      // When switching from pread to stream, we will open a new scanner for each store file, but
      // the old scanner may still track the HFileBlocks we have scanned but not sent back to client
      // yet. If we close the scanner immediately then the HFileBlocks may be messed up by others
      // before we serialize and send it back to client. The HFileBlocks will be released in shipped
      // method, so we here will also open new scanners and close old scanners in shipped method.
      // See HBASE-18055 for more details.
      trySwitchToStreamRead();
    }
  }
}
