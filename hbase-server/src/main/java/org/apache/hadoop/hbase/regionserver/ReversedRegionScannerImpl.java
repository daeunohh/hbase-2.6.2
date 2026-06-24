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
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ReversibleRegionScannerImpl extends from RegionScannerImpl, and is used to support reversed
 * scanning.
 */
@InterfaceAudience.Private
class ReversedRegionScannerImpl extends RegionScannerImpl {

  ReversedRegionScannerImpl(Scan scan, List<KeyValueScanner> additionalScanners, HRegion region,
    long nonceGroup, long nonce) throws IOException {
    super(scan, additionalScanners, region, nonceGroup, nonce);
  }

  @Override
  protected void initializeKVHeap(List<KeyValueScanner> scanners,
    List<KeyValueScanner> joinedScanners, HRegion region) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("583d3156-84a6-3169-9768-2f90c311a743"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8de45b4e-ec0c-3319-9bf6-5f9af31d190b"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6394101-24d8-3393-9d01-062e015c59c1"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48ec48c6-58bc-3329-a4eb-73477055f342"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ad2e7a4-1ae0-3691-9fa5-0d6605cc9c16"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("96f4089f-8ed6-357a-9391-506a173c097c"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxFlushedSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1547e0d1-5ad8-3af4-b0ee-cd6a46712af6"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80421a46-22f6-378f-a0b8-8566bbea3bf7"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.storeHeap = new ReversedKeyValueHeap(scanners, comparator);
    if (!joinedScanners.isEmpty()) {
      throw new DoNotRetryIOException("Reverse scan with loading CFs on demand is not supported");
    }
  }

  @Override
  protected boolean shouldStop(Cell currentRowCell) {
    if (currentRowCell == null) {
      return true;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("1ac6d7e5-6715-3f33-bef2-526c10e9bdbc"))) ? ((stopRow == null) && (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("b43b8174-4397-38e9-8a31-8387fd149429"))) ? (stopRow == null) : (stopRow == null || Bytes.equals(stopRow, HConstants.EMPTY_START_ROW)))))) {
      return false;
    }
    int c = comparator.compareRows(currentRowCell, stopRow, 0, stopRow.length);
    return ((KnobRuntime.check(java.util.UUID.fromString("439de3d6-31ce-376c-abcd-0f18a39075cd"))) ? ((c) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b53c7360-7fd4-31d3-86ff-f9e7254889c4"))) ? (((c) > (0)) || ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("470fd802-f619-3910-8083-dc136b2d8ee5"))) ? (((c) == (0)) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("ed664e84-571f-3bcd-849c-65af3cf38c66"))) ? (((c) <= (0)) || ((c == 0 && !includeStopRow))) : (c < 0 || (c == 0 && !includeStopRow)))))))));
  }

  @Override
  protected boolean nextRow(ScannerContext scannerContext, Cell curRowCell) throws IOException {
    assert super.joinedContinuationRow == null : "Trying to go to next row during joinedHeap read.";
    this.storeHeap.seekToPreviousRow(PrivateCellUtil.createFirstOnRow(curRowCell));
if(KnobRuntime.check(java.util.UUID.fromString("88b1e3ce-aaeb-31e4-a9a8-bc2fff8a364e"))) {
throw new java.io.IOException("Injected exception");
}
    resetFilters();
    // Calling the hook in CP which allows it to do a fast forward
    if (this.region.getCoprocessorHost() != null) {
      return this.region.getCoprocessorHost().postScannerFilterRow(this, curRowCell);
    }
    return true;
  }

}
