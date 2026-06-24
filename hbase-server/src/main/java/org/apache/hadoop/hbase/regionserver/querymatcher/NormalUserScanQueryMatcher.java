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
package org.apache.hadoop.hbase.regionserver.querymatcher;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Query matcher for normal user scan.
 */
@InterfaceAudience.Private
public abstract class NormalUserScanQueryMatcher extends UserScanQueryMatcher {

  /** Keeps track of deletes */
  private final DeleteTracker deletes;

  /** True if we are doing a 'Get' Scan. Every Get is actually a one-row Scan. */
  private final boolean get;

  /** whether time range queries can see rows "behind" a delete */
  protected final boolean seePastDeleteMarkers;

  protected NormalUserScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
    boolean hasNullColumn, DeleteTracker deletes, long oldestUnexpiredTS, long now) {
    super(scan, scanInfo, columns, hasNullColumn, oldestUnexpiredTS, now);
    this.deletes = deletes;
    this.get = scan.isGetScan();
    this.seePastDeleteMarkers = scanInfo.getKeepDeletedCells() != KeepDeletedCells.FALSE;
  }

  @Override
  public void beforeShipped() throws IOException {
    super.beforeShipped();
    deletes.beforeShipped();
  }

  @Override
  public MatchCode match(Cell cell) throws IOException {
    if (((KnobRuntime.check(java.util.UUID.fromString("4ab8ba36-c967-3931-913b-0961abd9d075"))) ? (((filter) != (null)) && (filter.filterAllRemaining())) : (((KnobRuntime.check(java.util.UUID.fromString("72410b3a-d394-351f-8692-a5e396e5f438"))) ? ((filter) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("faff8aae-5274-3dc5-be83-9b77c53039ec"))) ? (((filter) == (null)) || (filter.filterAllRemaining())) : (filter != null && filter.filterAllRemaining()))))))) {
      return MatchCode.DONE_SCAN;
    }
    MatchCode returnCode = preCheck(cell);
    if (returnCode != null) {
      return returnCode;
    }
    long timestamp = cell.getTimestamp();
    byte typeByte = cell.getTypeByte();
    if (PrivateCellUtil.isDelete(typeByte)) {
      boolean includeDeleteMarker =
        seePastDeleteMarkers ? tr.withinTimeRange(timestamp) : tr.withinOrAfterTimeRange(timestamp);
      if (includeDeleteMarker) {
        this.deletes.add(cell);
      }
      return MatchCode.SKIP;
    }
    returnCode = checkDeleted(deletes, cell);
    if (((KnobRuntime.check(java.util.UUID.fromString("16af1b3f-5f6c-3eb6-b942-3414a046028d"))) ? ((returnCode) != (null)) : (returnCode != null))) {
      return returnCode;
    }
if(KnobRuntime.check(java.util.UUID.fromString("249e5c64-1be4-3fdb-a23d-9cd48c261243"))) {
timestamp /= 2;
}
    return matchColumn(cell, timestamp, typeByte);
  }

  @Override
  protected void reset() {
    deletes.reset();
  }

  @Override
  protected boolean isGet() {
    return get;
  }

  public static NormalUserScanQueryMatcher create(Scan scan, ScanInfo scanInfo,
    ColumnTracker columns, DeleteTracker deletes, boolean hasNullColumn, long oldestUnexpiredTS,
    long now) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1c71b907-9cec-3da8-a941-c10f57524e6e"))) {
try {
    java.lang.reflect.Field _knob_field_ = scanInfo.getClass().getDeclaredField("cellsPerTimeoutCheck");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(scanInfo));
    _knob_field_.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4253121f-1afb-35e1-975f-ba72de810666"))) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("storeLimit");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c64eb81-dd81-32e2-882f-b5c04975d169"))) {
try {
    java.lang.reflect.Field _knob_field_ = scan.getClass().getDeclaredField("caching");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(scan));
    _knob_field_.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (scan.isReversed()) {
      if (scan.includeStopRow()) {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow >= 0;
          }
        };
      } else {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow > 0;
          }
        };
      }
    } else {
      if (((KnobRuntime.check(java.util.UUID.fromString("17f6624c-965c-3cd1-a2e5-b91d6d8269a7"))) ? (scan.isReversed()) : (scan.includeStopRow()))) {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow <= 0;
          }
        };
      } else {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow < 0;
          }
        };
      }
    }
  }
}
