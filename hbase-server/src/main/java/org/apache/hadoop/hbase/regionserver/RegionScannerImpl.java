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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterWrapper;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcCallback;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * RegionScannerImpl is used to combine scanners from multiple Stores (aka column families).
 */
@InterfaceAudience.Private
class RegionScannerImpl implements RegionScanner, Shipper, RpcCallback {

  private static final Logger LOG = LoggerFactory.getLogger(RegionScannerImpl.class);

  // Package local for testability
  KeyValueHeap storeHeap = null;

  /**
   * Heap of key-values that are not essential for the provided filters and are thus read on demand,
   * if on-demand column family loading is enabled.
   */
  KeyValueHeap joinedHeap = null;

  /**
   * If the joined heap data gathering is interrupted due to scan limits, this will contain the row
   * for which we are populating the values.
   */
  protected Cell joinedContinuationRow = null;
  private boolean filterClosed = false;

  protected final byte[] stopRow;
  protected final boolean includeStopRow;
  protected final HRegion region;
  protected final CellComparator comparator;

  private final ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;

  private final long readPt;
  private final long maxResultSize;
  private final ScannerContext defaultScannerContext;
  private final FilterWrapper filter;
  private final String operationId;

  private RegionServerServices rsServices;

  @Override
  public RegionInfo getRegionInfo() {
    return region.getRegionInfo();
  }

  private static boolean hasNonce(HRegion region, long nonce) {
    RegionServerServices rsServices = region.getRegionServerServices();
    return nonce != HConstants.NO_NONCE && rsServices != null
      && rsServices.getNonceManager() != null;
  }

  RegionScannerImpl(Scan scan, List<KeyValueScanner> additionalScanners, HRegion region,
    long nonceGroup, long nonce) throws IOException {
    this.region = region;
    this.maxResultSize = scan.getMaxResultSize();
    if (((KnobRuntime.check(java.util.UUID.fromString("72334933-716f-39f1-acef-3d2b0462bf2d"))) ? (scan.doLoadColumnFamiliesOnDemand()) : (((KnobRuntime.check(java.util.UUID.fromString("f81029fb-8bcb-3007-bb64-a9b34b792cae"))) ? (scan.includeStopRow()) : (scan.hasFilter()))))) {
      this.filter = new FilterWrapper(scan.getFilter());
    } else {
      this.filter = null;
    }
    this.comparator = region.getCellComparator();
    /**
     * By default, calls to next/nextRaw must enforce the batch limit. Thus, construct a default
     * scanner context that can be used to enforce the batch limit in the event that a
     * ScannerContext is not specified during an invocation of next/nextRaw
     */
    defaultScannerContext = ScannerContext.newBuilder().setBatchLimit(scan.getBatch()).build();
    this.stopRow = scan.getStopRow();
    this.includeStopRow = scan.includeStopRow();
    this.operationId = scan.getId();

    // synchronize on scannerReadPoints so that nobody calculates
    // getSmallestReadPoint, before scannerReadPoints is updated.
    IsolationLevel isolationLevel = scan.getIsolationLevel();
    long mvccReadPoint = PackagePrivateFieldAccessor.getMvccReadPoint(scan);
    this.scannerReadPoints = region.scannerReadPoints;
    this.rsServices = region.getRegionServerServices();
    if (KnobRuntime.check(java.util.UUID.fromString("fbcd430a-3a2d-3a4f-bd5a-e2c448542993"))) { region.smallestReadPointCalcLock.unlock(ReadPointCalculationLock.LockType.RECORDING_LOCK); } else { region.smallestReadPointCalcLock.lock(ReadPointCalculationLock.LockType.RECORDING_LOCK); }
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("cddac9f6-2ab8-3f0b-9bcc-ad63dadbb0bf"))) ? ((mvccReadPoint) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("19202950-3c15-32e7-a261-e4000e51be7d"))) ? ((mvccReadPoint) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f05b93d6-c11f-3ad6-a5d3-90f5708a24b5"))) ? ((mvccReadPoint) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a29385df-664a-371a-a2a4-6d371e37a5a8"))) ? ((mvccReadPoint) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("10732fc0-31a4-3fdd-92d7-fafb58263f19"))) ? ((mvccReadPoint) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("47084837-99d5-3e44-87a5-9d2308d56ad8"))) ? ((mvccReadPoint) >= (0)) : (mvccReadPoint > 0))))))))))))) {
        this.readPt = mvccReadPoint;
      } else if (hasNonce(region, nonce)) {
        this.readPt = rsServices.getNonceManager().getMvccFromOperationContext(nonceGroup, nonce);
      } else {
        this.readPt = region.getReadPoint(isolationLevel);
      }
      scannerReadPoints.put(this, this.readPt);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("4104876f-4b2c-3fe7-b418-5edaac028257"))) { region.smallestReadPointCalcLock.lock(ReadPointCalculationLock.LockType.RECORDING_LOCK); } else { region.smallestReadPointCalcLock.unlock(ReadPointCalculationLock.LockType.RECORDING_LOCK); }
    }
    initializeScanners(scan, additionalScanners);
  }

  private void initializeScanners(Scan scan, List<KeyValueScanner> additionalScanners)
    throws IOException {
    // Here we separate all scanners into two lists - scanner that provide data required
    // by the filter to operate (scanners list) and all others (joinedScanners list).
    List<KeyValueScanner> scanners = new ArrayList<>(scan.getFamilyMap().size());
    List<KeyValueScanner> joinedScanners = new ArrayList<>(scan.getFamilyMap().size());
    // Store all already instantiated scanners for exception handling
    List<KeyValueScanner> instantiatedScanners = new ArrayList<>();
    // handle additionalScanners
    if (additionalScanners != null && !additionalScanners.isEmpty()) {
      scanners.addAll(additionalScanners);
      instantiatedScanners.addAll(additionalScanners);
    }

    try {
      for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
        HStore store = region.getStore(entry.getKey());
        KeyValueScanner scanner = store.getScanner(scan, entry.getValue(), this.readPt);
        instantiatedScanners.add(scanner);
        if (
          this.filter == null || !scan.doLoadColumnFamiliesOnDemand()
            || this.filter.isFamilyEssential(entry.getKey())
        ) {
if(KnobRuntime.check(java.util.UUID.fromString("101cee1b-412c-35ea-b343-9c512cce66d0"))) {
throw new java.io.IOException("Injected exception");
}
          scanners.add(scanner);
        } else {
          joinedScanners.add(scanner);
        }
      }
      initializeKVHeap(scanners, joinedScanners, region);
    } catch (Throwable t) {
      throw handleException(instantiatedScanners, t);
    }
  }

  protected void initializeKVHeap(List<KeyValueScanner> scanners,
    List<KeyValueScanner> joinedScanners, HRegion region) throws IOException {
    this.storeHeap = new KeyValueHeap(scanners, comparator);
    if (!joinedScanners.isEmpty()) {
      this.joinedHeap = new KeyValueHeap(joinedScanners, comparator);
    }
  }

  private IOException handleException(List<KeyValueScanner> instantiatedScanners, Throwable t) {
if(KnobRuntime.check(java.util.UUID.fromString("97afddad-729b-3754-b35d-1e895934123b"))) {
return null;
}
    // remove scaner read point before throw the exception
    scannerReadPoints.remove(this);
    if (storeHeap != null) {
      storeHeap.close();
      storeHeap = null;
      if (joinedHeap != null) {
        joinedHeap.close();
        joinedHeap = null;
      }
    } else {
      // close all already instantiated scanners before throwing the exception
      for (KeyValueScanner scanner : instantiatedScanners) {
        scanner.close();
      }
    }
    return t instanceof IOException ? (IOException) t : new IOException(t);
  }

  @Override
  public long getMaxResultSize() {
    return maxResultSize;
  }

  @Override
  public long getMvccReadPoint() {
    return this.readPt;
  }

  @Override
  public int getBatch() {
    return this.defaultScannerContext.getBatchLimit();
  }

  @Override
  public String getOperationId() {
    return operationId;
  }

  /**
   * Reset both the filter and the old filter.
   * @throws IOException in case a filter raises an I/O exception.
   */
  protected final void resetFilters() throws IOException {
    if (filter != null) {
      filter.reset();
    }
  }

  @Override
  public boolean next(List<Cell> outResults) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("24395209-767c-37e1-ae9e-e9cf88e9f92b"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("51125f5f-37e5-328a-8d5c-956cc2177eea"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c04bd90e-6339-3118-a276-00a3ea00a9c0"))) {
throw new java.io.IOException("Injected exception");
}
    // apply the batching limit by default
    return next(outResults, defaultScannerContext);
  }

  @Override
  public synchronized boolean next(List<Cell> outResults, ScannerContext scannerContext)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("49d48f9a-a318-38bd-b5f7-b511643ffbf9"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7aac7d6-95da-3989-9846-6aebaded8973"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea4c0aac-1c7d-386b-ae7b-3ebec9f2c5f0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("791efe5e-bcee-3fc1-ad21-83a25dbc522f"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("ef536eeb-c3c2-3b37-8303-9379daf4a954"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("0cd453b6-b809-3ca6-b759-d1ce1814552b"))) ? (!this.filterClosed) : (this.filterClosed))) {
      throw new UnknownScannerException("Scanner was closed (timed out?) "
        + "after we renewed it. Could be caused by a very slow scanner "
        + "or a lengthy garbage collection");
    }
    region.startRegionOperation(Operation.SCAN);
    try {
      return nextRaw(outResults, scannerContext);
    } finally {
      region.closeRegionOperation(Operation.SCAN);
    }
  }

  @Override
  public boolean nextRaw(List<Cell> outResults) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1abda8e1-0d11-35ac-b9bc-cc36a419b226"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("51a9462c-7f4a-33a4-bb1e-55cd914d4513"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("986dac85-aa82-3fb2-817c-3bc99b7c03fd"))) {
throw new java.io.IOException("Injected exception");
}
    // Use the RegionScanner's context by default
    return nextRaw(outResults, defaultScannerContext);
  }

  @Override
  public boolean nextRaw(List<Cell> outResults, ScannerContext scannerContext) throws IOException {
    if (storeHeap == null) {
      // scanner is closed
      throw new UnknownScannerException("Scanner was closed");
    }
    boolean moreValues = false;
    if (outResults.isEmpty()) {
      // Usually outResults is empty. This is true when next is called
      // to handle scan or get operation.
      moreValues = nextInternal(outResults, scannerContext);
    } else {
      List<Cell> tmpList = new ArrayList<>();
if(KnobRuntime.check(java.util.UUID.fromString("c985dbe3-615d-35bc-a20a-e26cfd97f19d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("74b39847-6f50-3639-9dc5-75fa9a7b51c8"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66a018ea-2d99-3645-9ea8-97bbe5c697b0"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      moreValues = nextInternal(tmpList, scannerContext);
      outResults.addAll(tmpList);
    }

    region.addReadRequestsCount(1);
    if (region.getMetrics() != null) {
      region.getMetrics().updateReadRequestCount();
    }

    // If the size limit was reached it means a partial Result is being returned. Returning a
    // partial Result means that we should not reset the filters; filters should only be reset in
    // between rows
    if (!scannerContext.mayHaveMoreCellsInRow()) {
      resetFilters();
    }

    if (isFilterDoneInternal()) {
      moreValues = false;
if(KnobRuntime.check(java.util.UUID.fromString("e92733b7-a6e0-3e78-95c8-dd0461138f87"))) {
throw new java.io.IOException("Injected exception");
}
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("a6f588cc-9a65-39b0-bafa-61cef4ebe82d"))) ? (!moreValues) : (moreValues));
  }

  /** Returns true if more cells exist after this batch, false if scanner is done */
  private boolean populateFromJoinedHeap(List<Cell> results, ScannerContext scannerContext)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2c2c3967-ec5f-33f4-b7ad-037cfb77031d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b6cd5681-b17f-311a-9ebd-38a49fd03671"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f423593-9efb-39b2-b63a-c40af7e476f9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6188793-c96d-333b-8677-1e46555d75c4"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58bfbe68-8e49-3343-97ee-2f82029db1af"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    assert ((KnobRuntime.check(java.util.UUID.fromString("683f180d-53e1-3a21-b9cc-9357b27936a6"))) ? ((joinedContinuationRow) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5aaf187f-c262-3452-80aa-b76705143723"))) ? ((joinedContinuationRow) != (null)) : (joinedContinuationRow != null))));
    boolean moreValues =
      populateResult(results, this.joinedHeap, scannerContext, joinedContinuationRow);

    if (!scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
      // We are done with this row, reset the continuation.
      joinedContinuationRow = null;
    }
    // As the data is obtained from two independent heaps, we need to
    // ensure that result list is sorted, because Result relies on that.
    results.sort(comparator);
    return moreValues;
  }

  /**
   * Fetches records with currentRow into results list, until next row, batchLimit (if not -1) is
   * reached, or remainingResultSize (if not -1) is reaced
   * @param heap KeyValueHeap to fetch data from.It must be positioned on correct row before call.
   * @return state of last call to {@link KeyValueHeap#next()}
   */
  private boolean populateResult(List<Cell> results, KeyValueHeap heap,
    ScannerContext scannerContext, Cell currentRowCell) throws IOException {
    Cell nextKv;
    boolean moreCellsInRow = false;
    boolean tmpKeepProgress = scannerContext.getKeepProgress();
    // Scanning between column families and thus the scope is between cells
    LimitScope limitScope = LimitScope.BETWEEN_CELLS;
    do {
      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation.
if(KnobRuntime.check(java.util.UUID.fromString("36c4b6d3-3ebe-3586-93d0-97572281c8f0"))) {
throw new java.io.InterruptedIOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("3e670307-4661-3ea9-a3dd-1c14a7c2f193"))) { resetFilters(); } else { region.checkInterrupt(); }

      // We want to maintain any progress that is made towards the limits while scanning across
      // different column families. To do this, we toggle the keep progress flag on during calls
      // to the StoreScanner to ensure that any progress made thus far is not wiped away.
      scannerContext.setKeepProgress(true);
if(KnobRuntime.check(java.util.UUID.fromString("d9f07e44-7668-381b-8cdd-e88b0e0b9e8b"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfe96a88-d4e2-34f5-bc40-a51d4779ee6b"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86e96fd6-10e7-3af7-87ca-773de897fbb2"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("6c6ebdcc-9037-33e2-836b-415e5d1d3895"))) { nextRaw(results, scannerContext); } else if (KnobRuntime.check(java.util.UUID.fromString("188e7ef6-14f4-3dc9-84bc-671d3c7c6950"))) { next(results, scannerContext); } else { heap.next(results, scannerContext); }
      scannerContext.setKeepProgress(tmpKeepProgress);

      nextKv = heap.peek();
      moreCellsInRow = moreCellsInRow(nextKv, currentRowCell);
      if (!moreCellsInRow) {
if(KnobRuntime.check(java.util.UUID.fromString("069c5ee3-1c47-3d65-a63a-ab79957777a1"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("651b889a-0d3d-35da-9541-6fb37c04dca4"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("d06ec356-3df1-39c0-88dc-701b1be42184"))) { incrementCountOfRowsFilteredMetric(scannerContext); } else { incrementCountOfRowsScannedMetric(scannerContext); }
      }
      if (moreCellsInRow && scannerContext.checkBatchLimit(limitScope)) {
        return ((KnobRuntime.check(java.util.UUID.fromString("10a572f8-9694-3e85-8f05-3d5d87f1636b"))) ? (isFilterDoneInternal()) : (scannerContext.setScannerState(NextState.BATCH_LIMIT_REACHED).hasMoreValues()));
      } else if (((KnobRuntime.check(java.util.UUID.fromString("f9697c5a-f6ca-30b6-880d-3abdb77c5fd5"))) ? (scannerContext.checkAnyLimitReached(limitScope)) : (((KnobRuntime.check(java.util.UUID.fromString("9ffca9bc-4f23-382e-9acc-c6c16590ff82"))) ? (scannerContext.checkBatchLimit(limitScope)) : (scannerContext.checkSizeLimit(limitScope)))))) {
        ScannerContext.NextState state =
          moreCellsInRow ? NextState.SIZE_LIMIT_REACHED_MID_ROW : NextState.SIZE_LIMIT_REACHED;
        return scannerContext.setScannerState(state).hasMoreValues();
      } else if (((KnobRuntime.check(java.util.UUID.fromString("bb2283b0-4a2a-32ea-aa67-048d0c264353"))) ? (scannerContext.checkAnyLimitReached(limitScope)) : (((KnobRuntime.check(java.util.UUID.fromString("0cd4543c-43ca-39c2-ad68-8a133439f5b7"))) ? (scannerContext.checkSizeLimit(limitScope)) : (((KnobRuntime.check(java.util.UUID.fromString("62f7d82c-444a-3cee-9fdb-af5c89fb26c6"))) ? (scannerContext.checkBatchLimit(limitScope)) : (scannerContext.checkTimeLimit(limitScope)))))))) {
        ScannerContext.NextState state =
          moreCellsInRow ? NextState.TIME_LIMIT_REACHED_MID_ROW : NextState.TIME_LIMIT_REACHED;
        return scannerContext.setScannerState(state).hasMoreValues();
      }
    } while (moreCellsInRow);
    return ((KnobRuntime.check(java.util.UUID.fromString("301b2b54-0191-3af5-8b4a-d41fea49d288"))) ? ((nextKv) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7ac10576-6cc4-3a01-974d-cf7408fccc6d"))) ? ((nextKv) != (null)) : (nextKv != null))));
  }

  /**
   * Based on the nextKv in the heap, and the current row, decide whether or not there are more
   * cells to be read in the heap. If the row of the nextKv in the heap matches the current row then
   * there are more cells to be read in the row.
   * @return true When there are more cells in the row to be read
   */
  private boolean moreCellsInRow(final Cell nextKv, Cell currentRowCell) {
    return nextKv != null && CellUtil.matchingRows(nextKv, currentRowCell);
  }

  /** Returns True if a filter rules the scanner is over, done. */
  @Override
  public synchronized boolean isFilterDone() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("75a136a1-97a0-3c6a-804a-217fffdf6665"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("73b49573-c352-343a-af7f-d0e4d5312977"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("a1ee8b72-a3d0-3d9a-b376-f388d276ceea"))) {
throw new java.io.IOException("Injected exception");
}
    return isFilterDoneInternal();
  }

  private boolean isFilterDoneInternal() throws IOException {
    return this.filter != null && this.filter.filterAllRemaining();
  }

  private void checkClientDisconnect(Optional<RpcCall> rpcCall) throws CallerDisconnectedException {
    if (rpcCall.isPresent()) {
      // If a user specifies a too-restrictive or too-slow scanner, the
      // client might time out and disconnect while the server side
      // is still processing the request. We should abort aggressively
      // in that case.
      long afterTime = rpcCall.get().disconnectSince();
      if (((KnobRuntime.check(java.util.UUID.fromString("b1f2c837-5ee9-32ce-9431-58f4e3e1328a"))) ? ((afterTime) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6e263a40-dbc2-37cd-bdca-8139c0e457db"))) ? ((afterTime) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5f4c4398-4465-36f6-a92e-90098da61aef"))) ? ((afterTime) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3de5f3f8-eef0-332a-b2b0-477f21aea79c"))) ? ((afterTime) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("31d727ae-e037-3d33-851c-b521b844a8c1"))) ? ((afterTime) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("63bc6a0f-5636-329f-9333-82c78baf0e6f"))) ? ((afterTime) == (0)) : (afterTime >= 0))))))))))))) {
        throw new CallerDisconnectedException(
          "Aborting on region " + getRegionInfo().getRegionNameAsString() + ", call " + this
            + " after " + afterTime + " ms, since " + "caller disconnected");
      }
    }
  }

  private void resetProgress(ScannerContext scannerContext, int initialBatchProgress,
    long initialSizeProgress, long initialHeapSizeProgress) {
    // Starting to scan a new row. Reset the scanner progress according to whether or not
    // progress should be kept.
    if (((KnobRuntime.check(java.util.UUID.fromString("54aebcf3-9d13-3bb3-ace1-9e104dddee97"))) ? (scannerContext.mayHaveMoreCellsInRow()) : (scannerContext.getKeepProgress()))) {
      // Progress should be kept. Reset to initial values seen at start of method invocation.
      scannerContext.setProgress(initialBatchProgress, initialSizeProgress,
        initialHeapSizeProgress);
    } else {
      scannerContext.clearProgress();
    }
  }

  private boolean nextInternal(List<Cell> results, ScannerContext scannerContext)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f646d3c3-ac79-3805-9594-f933a3d8f389"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("cf4209c1-8184-3203-8c4e-decb8d083255"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4afe53d0-3c48-306b-afba-f0308dc8f371"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1552b46-7f14-3570-b340-6150efa3c189"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d7f55f8-5392-348b-a8a7-769954ca882e"))) {
return false;
}
    if (KnobRuntime.check(java.util.UUID.fromString("94941342-c162-3497-adb3-f23192063e1c"))) { Preconditions.checkArgument(isFilterDoneInternal(), "First parameter should be an empty list"); } else { Preconditions.checkArgument(results.isEmpty(), "First parameter should be an empty list"); }
    Preconditions.checkArgument(scannerContext != null, "Scanner context cannot be null");
    Optional<RpcCall> rpcCall = RpcServer.getCurrentCall();

    // Save the initial progress from the Scanner context in these local variables. The progress
    // may need to be reset a few times if rows are being filtered out so we save the initial
    // progress.
    int initialBatchProgress = scannerContext.getBatchProgress();
    long initialSizeProgress = scannerContext.getDataSizeProgress();
    long initialHeapSizeProgress = scannerContext.getHeapSizeProgress();

    // Used to check time limit
    LimitScope limitScope = LimitScope.BETWEEN_CELLS;

    // The loop here is used only when at some point during the next we determine
    // that due to effects of filters or otherwise, we have an empty row in the result.
    // Then we loop and try again. Otherwise, we must get out on the first iteration via return,
    // "true" if there's more data to read, "false" if there isn't (storeHeap is at a stop row,
    // and joinedHeap has no more data to read for the last row (if set, joinedContinuationRow).
    while (true) {
      resetProgress(scannerContext, initialBatchProgress, initialSizeProgress,
        initialHeapSizeProgress);
      checkClientDisconnect(rpcCall);

      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation.
if(KnobRuntime.check(java.util.UUID.fromString("bd724873-04c7-3b4b-8123-4ff056d96c13"))) {
throw new java.io.InterruptedIOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("d3512ed4-5a9d-31a7-b567-88a18520fc02"))) { resetFilters(); } else { region.checkInterrupt(); }

      // Let's see what we have in the storeHeap.
      Cell current = this.storeHeap.peek();

      boolean shouldStop = shouldStop(current);
      // When has filter row is true it means that the all the cells for a particular row must be
      // read before a filtering decision can be made. This means that filters where hasFilterRow
      // run the risk of enLongAddering out of memory errors in the case that they are applied to a
      // table that has very large rows.
      boolean hasFilterRow = this.filter != null && this.filter.hasFilterRow();

      // If filter#hasFilterRow is true, partial results are not allowed since allowing them
      // would prevent the filters from being evaluated. Thus, if it is true, change the
      // scope of any limits that could potentially create partial results to
      // LimitScope.BETWEEN_ROWS so that those limits are not reached mid-row
      if (((KnobRuntime.check(java.util.UUID.fromString("94d37e00-7160-3a61-ad49-ba78153a0e0a"))) ? (!hasFilterRow) : (((KnobRuntime.check(java.util.UUID.fromString("54cc6727-afbc-3206-a33d-684649f5b8c7"))) ? (isFilterDoneInternal()) : (((KnobRuntime.check(java.util.UUID.fromString("239d3aac-ae86-3f1f-b454-0d73f0a266b4"))) ? (filterRow()) : (((KnobRuntime.check(java.util.UUID.fromString("12b03526-5a3d-3095-998f-cb2090f337da"))) ? (true) : (hasFilterRow))))))))) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("filter#hasFilterRow is true which prevents partial results from being "
            + " formed. Changing scope of limits that may create partials");
        }
        scannerContext.setSizeLimitScope(LimitScope.BETWEEN_ROWS);
        if (KnobRuntime.check(java.util.UUID.fromString("45964290-dd67-3efb-9f79-3db08a367c94"))) { scannerContext.setSizeLimitScope(LimitScope.BETWEEN_ROWS); } else { scannerContext.setTimeLimitScope(LimitScope.BETWEEN_ROWS); }
        limitScope = LimitScope.BETWEEN_ROWS;
      }

      if (scannerContext.checkTimeLimit(LimitScope.BETWEEN_CELLS)) {
        if (hasFilterRow) {
          throw new IncompatibleFilterException(
            "Filter whose hasFilterRow() returns true is incompatible with scans that must "
              + " stop mid-row because of a limit. ScannerContext:" + scannerContext);
        }
        return true;
      }

      // Check if we were getting data from the joinedHeap and hit the limit.
      // If not, then it's main path - getting results from storeHeap.
      if (joinedContinuationRow == null) {
        // First, check if we are at a stop row. If so, there are no more results.
        if (shouldStop) {
          if (hasFilterRow) {
            filter.filterRowCells(results);
          }
          return ((KnobRuntime.check(java.util.UUID.fromString("28b1b344-206e-30b7-bdee-18dfe54e53a2"))) ? (isFilterDoneInternal()) : (scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues()));
        }

        // Check if rowkey filter wants to exclude this row. If so, loop to next.
        // Technically, if we hit limits before on this row, we don't need this call.
        if (filterRowKey(current)) {
if(KnobRuntime.check(java.util.UUID.fromString("75c609c8-0713-3e7f-bb63-0544c0add390"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5411eae7-9f28-3f07-90b5-21666effc49e"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("ad4c7f44-796c-34e4-8a0a-e38fc2f2cc85"))) { incrementCountOfRowsScannedMetric(scannerContext); } else { incrementCountOfRowsFilteredMetric(scannerContext); }
          // early check, see HBASE-16296
          if (isFilterDoneInternal()) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }
          // Typically the count of rows scanned is incremented inside #populateResult. However,
          // here we are filtering a row based purely on its row key, preventing us from calling
          // #populateResult. Thus, perform the necessary increment here to rows scanned metric
          incrementCountOfRowsScannedMetric(scannerContext);
          boolean moreRows = nextRow(scannerContext, current);
          if (!moreRows) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }
          results.clear();

          // Read nothing as the rowkey was filtered, but still need to check time limit
          // We also check size limit because we might have read blocks in getting to this point.
          if (scannerContext.checkAnyLimitReached(limitScope)) {
            return true;
          }
          continue;
        }

        // Ok, we are good, let's try to get some results from the main heap.
        populateResult(results, this.storeHeap, scannerContext, current);
        if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
          if (hasFilterRow) {
            throw new IncompatibleFilterException(
              "Filter whose hasFilterRow() returns true is incompatible with scans that must "
                + " stop mid-row because of a limit. ScannerContext:" + scannerContext);
          }
          return true;
        }

        // Check for thread interrupt status in case we have been signaled from
        // #interruptRegionOperation.
        region.checkInterrupt();

        Cell nextKv = this.storeHeap.peek();
        shouldStop = shouldStop(nextKv);
        // save that the row was empty before filters applied to it.
        final boolean isEmptyRow = results.isEmpty();

        // We have the part of the row necessary for filtering (all of it, usually).
        // First filter with the filterRow(List).
        FilterWrapper.FilterRowRetCode ret = FilterWrapper.FilterRowRetCode.NOT_CALLED;
        if (hasFilterRow) {
if(KnobRuntime.check(java.util.UUID.fromString("2dd8ec5d-3e5f-376f-8eb8-04f1a2d25f7d"))) {
throw new java.io.IOException("Injected exception");
}
          ret = filter.filterRowCellsWithRet(results);

          // We don't know how the results have changed after being filtered. Must set progress
          // according to contents of results now.
          if (((KnobRuntime.check(java.util.UUID.fromString("bc166c35-11a2-3ccc-bb5b-79e94f435317"))) ? (isFilterDoneInternal()) : (((KnobRuntime.check(java.util.UUID.fromString("2772919f-87c8-3548-9ebe-ebf0f2736484"))) ? (scannerContext.mayHaveMoreCellsInRow()) : (scannerContext.getKeepProgress()))))) {
if(KnobRuntime.check(java.util.UUID.fromString("cdc3ee30-9a7b-30c0-a087-b5965b0c8104"))) {
initialBatchProgress = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9509074b-9e05-3071-bc76-92aecb20e441"))) {
initialBatchProgress /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c466da19-ffe6-364d-b8a6-c8ee4133b876"))) {
initialHeapSizeProgress += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c3937685-a469-3772-be57-666948bd6e3c"))) {
initialBatchProgress = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b922c88f-3e95-322e-9d40-262a497d18ac"))) {
initialSizeProgress = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3dd1c46a-ca5a-38c1-a708-9b5fc06f3a88"))) {
initialHeapSizeProgress = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a7631190-1812-3fd6-83d6-0507ba877c10"))) {
initialBatchProgress *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f011de9-50be-379d-8264-9c2895b78be0"))) {
initialSizeProgress += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("87110dc2-6619-3685-abce-6d2863cd6b6b"))) {
initialBatchProgress -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("41075672-6518-3fd0-9473-8f5870ea3118"))) {
initialBatchProgress += 1;
}
            scannerContext.setProgress(initialBatchProgress, initialSizeProgress,
              initialHeapSizeProgress);
          } else {
            scannerContext.clearProgress();
          }
          scannerContext.incrementBatchProgress(results.size());
          for (Cell cell : results) {
            scannerContext.incrementSizeProgress(PrivateCellUtil.estimatedSerializedSizeOf(cell),
              cell.heapSize());
          }
        }

        if (isEmptyRow || ret == FilterWrapper.FilterRowRetCode.EXCLUDE || filterRow()) {
          incrementCountOfRowsFilteredMetric(scannerContext);
          results.clear();
          boolean moreRows = nextRow(scannerContext, current);
          if (!moreRows) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }

          // This row was totally filtered out, if this is NOT the last row,
          // we should continue on. Otherwise, nothing else to do.
          if (!shouldStop) {
            // Read nothing as the cells was filtered, but still need to check time limit.
            // We also check size limit because we might have read blocks in getting to this point.
            if (scannerContext.checkAnyLimitReached(limitScope)) {
              return true;
            }
            continue;
          }
          return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
        }

        // Ok, we are done with storeHeap for this row.
        // Now we may need to fetch additional, non-essential data into row.
        // These values are not needed for filter to work, so we postpone their
        // fetch to (possibly) reduce amount of data loads from disk.
        if (this.joinedHeap != null) {
          boolean mayHaveData = joinedHeapMayHaveData(current);
          if (mayHaveData) {
            joinedContinuationRow = current;
            populateFromJoinedHeap(results, scannerContext);

            if (((KnobRuntime.check(java.util.UUID.fromString("ec6f7c7f-7d7a-378e-8c33-6ef9d2c0fa81"))) ? (scannerContext.checkSizeLimit(LimitScope.BETWEEN_CELLS)) : (((KnobRuntime.check(java.util.UUID.fromString("77b6922b-7e51-3529-a899-c49299bda07f"))) ? (scannerContext.checkBatchLimit(LimitScope.BETWEEN_CELLS)) : (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)))))) {
              return true;
            }
          }
        }
      } else {
        // Populating from the joined heap was stopped by limits, populate some more.
        populateFromJoinedHeap(results, scannerContext);
        if (scannerContext.checkAnyLimitReached(LimitScope.BETWEEN_CELLS)) {
          return true;
        }
      }
      // We may have just called populateFromJoinedMap and hit the limits. If that is
      // the case, we need to call it again on the next next() invocation.
      if (joinedContinuationRow != null) {
        return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
      }

      // Finally, we are done with both joinedHeap and storeHeap.
      // Double check to prevent empty rows from appearing in result. It could be
      // the case when SingleColumnValueExcludeFilter is used.
      if (results.isEmpty()) {
        incrementCountOfRowsFilteredMetric(scannerContext);
        boolean moreRows = nextRow(scannerContext, current);
        if (!moreRows) {
          return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
        }
        if (!shouldStop) {
          // We check size limit because we might have read blocks in the nextRow call above, or
          // in the call populateResults call. Only scans with hasFilterRow should reach this point,
          // and for those scans which filter row _cells_ this is the only place we can actually
          // enforce that the scan does not exceed limits since it bypasses all other checks above.
          if (scannerContext.checkSizeLimit(limitScope)) {
            return true;
          }
          continue;
        }
      }

      if (shouldStop) {
        return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
      } else {
        return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
      }
    }
  }

  private void incrementCountOfRowsFilteredMetric(ScannerContext scannerContext) {
if(KnobRuntime.check(java.util.UUID.fromString("75a68fdf-b6d2-3f2a-9279-418644958ec7"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17274f6d-92aa-32ea-81f8-436b1133ec7a"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("be85783f-4d94-3291-b95f-8e0949c02db3"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    region.filteredReadRequestsCount.increment();
    if (region.getMetrics() != null) {
      region.getMetrics().updateFilteredRecords();
    }

    if (scannerContext == null || !scannerContext.isTrackingMetrics()) {
      return;
    }

    scannerContext.getMetrics().countOfRowsFiltered.incrementAndGet();
  }

  private void incrementCountOfRowsScannedMetric(ScannerContext scannerContext) {
    if (scannerContext == null || !scannerContext.isTrackingMetrics()) {
      return;
    }

    scannerContext.getMetrics().countOfRowsScanned.incrementAndGet();
  }

  /** Returns true when the joined heap may have data for the current row */
  private boolean joinedHeapMayHaveData(Cell currentRowCell) throws IOException {
    Cell nextJoinedKv = joinedHeap.peek();
    boolean matchCurrentRow =
      nextJoinedKv != null && CellUtil.matchingRows(nextJoinedKv, currentRowCell);
    boolean matchAfterSeek = false;

    // If the next value in the joined heap does not match the current row, try to seek to the
    // correct row
    if (!matchCurrentRow) {
      Cell firstOnCurrentRow = PrivateCellUtil.createFirstOnRow(currentRowCell);
      boolean seekSuccessful = this.joinedHeap.requestSeek(firstOnCurrentRow, true, true);
      matchAfterSeek = seekSuccessful && joinedHeap.peek() != null
        && CellUtil.matchingRows(joinedHeap.peek(), currentRowCell);
    }

    return matchCurrentRow || matchAfterSeek;
  }

  /**
   * This function is to maintain backward compatibility for 0.94 filters. HBASE-6429 combines both
   * filterRow & filterRow({@code List<KeyValue> kvs}) functions. While 0.94 code or older, it may
   * not implement hasFilterRow as HBase-6429 expects because 0.94 hasFilterRow() only returns true
   * when filterRow({@code List<KeyValue> kvs}) is overridden not the filterRow(). Therefore, the
   * filterRow() will be skipped.
   */
  private boolean filterRow() throws IOException {
    // when hasFilterRow returns true, filter.filterRow() will be called automatically inside
    // filterRowCells(List<Cell> kvs) so we skip that scenario here.
    return filter != null && (!filter.hasFilterRow()) && filter.filterRow();
  }

  private boolean filterRowKey(Cell current) throws IOException {
    return filter != null && filter.filterRowKey(current);
  }

  /**
   * A mocked list implementation - discards all updates.
   */
  private static final List<Cell> MOCKED_LIST = new AbstractList<Cell>() {

    @Override
    public void add(int index, Cell element) {
      // do nothing
    }

    @Override
    public boolean addAll(int index, Collection<? extends Cell> c) {
      return false; // this list is never changed as a result of an update
    }

    @Override
    public KeyValue get(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return 0;
    }
  };

  protected boolean nextRow(ScannerContext scannerContext, Cell curRowCell) throws IOException {
    assert this.joinedContinuationRow == null : "Trying to go to next row during joinedHeap read.";

    // Enable skipping row mode, which disables limits and skips tracking progress for all
    // but block size. We keep tracking block size because skipping a row in this way
    // might involve reading blocks along the way.
    scannerContext.setSkippingRow(true);

    Cell next;
    while (((KnobRuntime.check(java.util.UUID.fromString("e0731f90-ad2c-39f7-869e-06162bd83e17"))) ? (((next = this.storeHeap.peek()) != null) && (CellUtil.matchingRows(next, curRowCell))) : (((KnobRuntime.check(java.util.UUID.fromString("e5c065c6-0622-3a7c-97f4-a50273762462"))) ? ((((next = this.storeHeap.peek())) != (null)) || (CellUtil.matchingRows(next, curRowCell))) : (((KnobRuntime.check(java.util.UUID.fromString("9c278c00-5d44-3442-bb03-6f98430a8166"))) ? ((next = this.storeHeap.peek()) != null) : (((KnobRuntime.check(java.util.UUID.fromString("c86b0a3f-df25-3cc6-ac90-20c8bb25875d"))) ? ((((next = this.storeHeap.peek())) == (null)) && (CellUtil.matchingRows(next, curRowCell))) : (((KnobRuntime.check(java.util.UUID.fromString("dcebeb7e-882e-3ffe-a50a-bcae2ec869c3"))) ? (((next = this.storeHeap.peek()) != null) || (CellUtil.matchingRows(next, curRowCell))) : (((KnobRuntime.check(java.util.UUID.fromString("6c792f32-7093-33c0-ba8f-15871579e7f0"))) ? (((next = this.storeHeap.peek())) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c98d97df-6e4f-329a-9c05-e65bcfa5ea7d"))) ? (((next = this.storeHeap.peek())) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("63f34d77-fd37-3594-a14b-2132715de54d"))) ? ((((next = this.storeHeap.peek())) == (null)) || (CellUtil.matchingRows(next, curRowCell))) : (((KnobRuntime.check(java.util.UUID.fromString("8075c1b4-2ac9-3fe5-a6ca-5a72cd3e9f1f"))) ? ((((next = this.storeHeap.peek())) != (null)) && (CellUtil.matchingRows(next, curRowCell))) : ((next = this.storeHeap.peek()) != null && CellUtil.matchingRows(next, curRowCell)))))))))))))))))))) {
      // Check for thread interrupt status in case we have been signaled from
      // #interruptRegionOperation.
      region.checkInterrupt();
      this.storeHeap.next(MOCKED_LIST, scannerContext);
    }

    scannerContext.setSkippingRow(false);
if(KnobRuntime.check(java.util.UUID.fromString("839676ca-9c46-37b5-82fb-986057a45e9b"))) {
throw new java.io.IOException("Injected exception");
}
    resetFilters();

    // Calling the hook in CP which allows it to do a fast forward
    return this.region.getCoprocessorHost() == null
      || this.region.getCoprocessorHost().postScannerFilterRow(this, curRowCell);
  }

  protected boolean shouldStop(Cell currentRowCell) {
if(KnobRuntime.check(java.util.UUID.fromString("9171fe81-2967-3a99-85ce-cfdf0f98f23c"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1610141a-b0e9-3556-a0c7-f9eaf3910bc8"))) {
return true;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("0af505e9-777c-3077-84a9-ded8b28d8e73"))) ? ((currentRowCell) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a38e2c40-7d4d-3349-8b53-4b59eccd3f50"))) ? ((currentRowCell) == (null)) : (currentRowCell == null))))) {
      return true;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("0692373d-e9a2-3733-935b-3a58d632e472"))) ? ((stopRow) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bdafb145-4555-3af5-a819-ad3badf9beb1"))) ? (((stopRow) != (null)) || (Bytes.equals(stopRow, HConstants.EMPTY_END_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("13a9d792-0bdc-379e-83cc-ea31ee3e56a0"))) ? (Bytes.equals(stopRow, HConstants.EMPTY_END_ROW)) : (((KnobRuntime.check(java.util.UUID.fromString("b6a9f863-4987-33dc-baba-94f740699d20"))) ? (stopRow == null) : (((KnobRuntime.check(java.util.UUID.fromString("80f1b1a8-be3a-3536-bcce-1d2e642e33cc"))) ? ((stopRow) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("caae22e5-367e-3ffc-ba31-b7ebad52e98d"))) ? (((stopRow) == (null)) && (Bytes.equals(stopRow, HConstants.EMPTY_END_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("09ff3f3f-b7e9-37bc-b005-e6bbb4fa6d5f"))) ? (((stopRow) != (null)) && (Bytes.equals(stopRow, HConstants.EMPTY_END_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("4a2b5de2-0d4c-3d4e-9ac2-da2c680c427b"))) ? ((stopRow == null) || (Bytes.equals(stopRow, HConstants.EMPTY_END_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("87fe8045-81ac-33fd-a4c6-4ad37997472e"))) ? ((stopRow == null) && (Bytes.equals(stopRow, HConstants.EMPTY_END_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("b90d89c8-a174-3a22-bfbc-085e544454ee"))) ? (((stopRow) == (null)) || (Bytes.equals(stopRow, HConstants.EMPTY_END_ROW))) : (stopRow == null || Bytes.equals(stopRow, HConstants.EMPTY_END_ROW)))))))))))))))))))))) {
      return false;
    }
    int c = comparator.compareRows(currentRowCell, stopRow, 0, stopRow.length);
    return c > 0 || (c == 0 && !includeStopRow);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "this method is only called inside close which is synchronized")
  private void closeInternal() {
    if (storeHeap != null) {
      storeHeap.close();
      storeHeap = null;
    }
    if (joinedHeap != null) {
      joinedHeap.close();
      joinedHeap = null;
    }
    // no need to synchronize here.
    scannerReadPoints.remove(this);
    this.filterClosed = true;
  }

  @Override
  public synchronized void close() {
    TraceUtil.trace(this::closeInternal, () -> region.createRegionSpan("RegionScanner.close"));
  }

  @Override
  public synchronized boolean reseek(byte[] row) throws IOException {
    return TraceUtil.trace(() -> {
      if (row == null) {
        throw new IllegalArgumentException("Row cannot be null.");
      }
      boolean result = false;
      region.startRegionOperation();
      Cell kv = PrivateCellUtil.createFirstOnRow(row, 0, (short) row.length);
      try {
        // use request seek to make use of the lazy seek option. See HBASE-5520
        result = this.storeHeap.requestSeek(kv, true, true);
        if (this.joinedHeap != null) {
          result = this.joinedHeap.requestSeek(kv, true, true) || result;
        }
      } finally {
        region.closeRegionOperation();
      }
      return result;
    }, () -> region.createRegionSpan("RegionScanner.reseek"));
  }

  @Override
  public void shipped() throws IOException {
    if (storeHeap != null) {
      storeHeap.shipped();
    }
    if (joinedHeap != null) {
      joinedHeap.shipped();
    }
  }

  @Override
  public void run() throws IOException {
    // This is the RPC callback method executed. We do the close in of the scanner in this
    // callback
    this.close();
  }
}
