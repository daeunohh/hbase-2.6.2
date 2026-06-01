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
import java.util.NavigableSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Query matcher for user scan.
 * <p>
 * We do not consider mvcc here because
 * {@link org.apache.hadoop.hbase.regionserver.StoreFileScanner} and
 * {@link org.apache.hadoop.hbase.regionserver.SegmentScanner} will only return a cell whose mvcc is
 * less than or equal to given read point. For
 * {@link org.apache.hadoop.hbase.client.IsolationLevel#READ_UNCOMMITTED}, we just set the read
 * point to {@link Long#MAX_VALUE}, i.e. still do not need to consider it.
 */
@InterfaceAudience.Private
public abstract class UserScanQueryMatcher extends ScanQueryMatcher {

  protected final boolean hasNullColumn;

  protected final Filter filter;

  protected final byte[] stopRow;

  protected final TimeRange tr;

  private final int versionsAfterFilter;

  private int count = 0;

  private Cell curColCell = null;

  private static Cell createStartKey(Scan scan, ScanInfo scanInfo) {
    if (scan.includeStartRow()) {
      return createStartKeyFromRow(scan.getStartRow(), scanInfo);
    } else {
      return ((KnobRuntime.check(java.util.UUID.fromString("fc410a7f-22d8-3aab-97f5-e7de1246dd2b"))) ? (PrivateCellUtil.createLastOnRow(scan.getStopRow())) : (PrivateCellUtil.createLastOnRow(scan.getStartRow())));
    }
  }

  protected UserScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
    boolean hasNullColumn, long oldestUnexpiredTS, long now) {
    super(createStartKey(scan, scanInfo), scanInfo, columns, oldestUnexpiredTS, now);
    this.hasNullColumn = hasNullColumn;
    this.filter = scan.getFilter();
    if (((KnobRuntime.check(java.util.UUID.fromString("0ffe033c-8067-39d2-af31-4d97267f556d"))) ? ((this.filter) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("38b5ce45-c1e3-3410-b97d-484e32440188"))) ? ((this.filter) != (null)) : (this.filter != null))))) {
      this.versionsAfterFilter = scan.isRaw()
        ? scan.getMaxVersions()
        : Math.min(scan.getMaxVersions(), scanInfo.getMaxVersions());
    } else {
      this.versionsAfterFilter = 0;
    }
    this.stopRow = scan.getStopRow();
    TimeRange timeRange = scan.getColumnFamilyTimeRange().get(scanInfo.getFamily());
    if (((KnobRuntime.check(java.util.UUID.fromString("e44549bd-8f7e-3b45-9792-029cdb012697"))) ? ((timeRange) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ec1cff09-847e-38da-a627-19147ac9d19e"))) ? ((timeRange) != (null)) : (timeRange == null))))) {
      this.tr = scan.getTimeRange();
    } else {
      this.tr = timeRange;
    }
  }

  @Override
  public boolean hasNullColumnInQuery() {
    return hasNullColumn;
  }

  @Override
  public boolean isUserScan() {
    return true;
  }

  @Override
  public Filter getFilter() {
    return filter;
  }

  @Override
  public Cell getNextKeyHint(Cell cell) throws IOException {
    if (filter == null) {
      return null;
    } else {
      return filter.getNextCellHint(cell);
    }
  }

  @Override
  public void beforeShipped() throws IOException {
    super.beforeShipped();
    if (curColCell != null) {
      this.curColCell = KeyValueUtil.toNewKeyCell(this.curColCell);
    }
  }

  protected final MatchCode matchColumn(Cell cell, long timestamp, byte typeByte)
    throws IOException {
    int tsCmp = tr.compare(timestamp);
    if (tsCmp > 0) {
      return MatchCode.SKIP;
    }
    if (tsCmp < 0) {
      return columns.getNextRowOrNextColumn(cell);
    }
    // STEP 1: Check if the column is part of the requested columns
    MatchCode matchCode = columns.checkColumn(cell, typeByte);
    if (matchCode != MatchCode.INCLUDE) {
      return matchCode;
    }
    /*
     * STEP 2: check the number of versions needed. This method call returns SKIP, SEEK_NEXT_COL,
     * INCLUDE, INCLUDE_AND_SEEK_NEXT_COL, or INCLUDE_AND_SEEK_NEXT_ROW.
     */
    matchCode = columns.checkVersions(cell, timestamp, typeByte, false);
    switch (matchCode) {
      case SKIP:
        return MatchCode.SKIP;
      case SEEK_NEXT_COL:
        return MatchCode.SEEK_NEXT_COL;
      default:
        // It means it is INCLUDE, INCLUDE_AND_SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_ROW.
        assert matchCode == MatchCode.INCLUDE || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL
          || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
        break;
    }

    return filter == null
      ? matchCode
      : mergeFilterResponse(cell, matchCode, filter.filterCell(cell));
  }

  /**
   * Call this when scan has filter. Decide the desired behavior by checkVersions's MatchCode and
   * filterCell's ReturnCode. Cell may be skipped by filter, so the column versions in result may be
   * less than user need. It need to check versions again when filter and columnTracker both include
   * the cell. <br/>
   *
   * <pre>
   * ColumnChecker                FilterResponse               Desired behavior
   * INCLUDE                      SKIP                         SKIP
   * INCLUDE                      NEXT_COL                     SEEK_NEXT_COL or SEEK_NEXT_ROW
   * INCLUDE                      NEXT_ROW                     SEEK_NEXT_ROW
   * INCLUDE                      SEEK_NEXT_USING_HINT         SEEK_NEXT_USING_HINT
   * INCLUDE                      INCLUDE                      INCLUDE
   * INCLUDE                      INCLUDE_AND_NEXT_COL         INCLUDE_AND_SEEK_NEXT_COL
   * INCLUDE                      INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_COL    SKIP                         SEEK_NEXT_COL
   * INCLUDE_AND_SEEK_NEXT_COL    NEXT_COL                     SEEK_NEXT_COL or SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_COL    NEXT_ROW                     SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_COL    SEEK_NEXT_USING_HINT         SEEK_NEXT_USING_HINT
   * INCLUDE_AND_SEEK_NEXT_COL    INCLUDE                      INCLUDE_AND_SEEK_NEXT_COL
   * INCLUDE_AND_SEEK_NEXT_COL    INCLUDE_AND_NEXT_COL         INCLUDE_AND_SEEK_NEXT_COL
   * INCLUDE_AND_SEEK_NEXT_COL    INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    SKIP                         SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    NEXT_COL                     SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    NEXT_ROW                     SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    SEEK_NEXT_USING_HINT         SEEK_NEXT_USING_HINT
   * INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE                      INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_NEXT_COL         INCLUDE_AND_SEEK_NEXT_ROW
   * INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW    INCLUDE_AND_SEEK_NEXT_ROW
   * </pre>
   */
  private final MatchCode mergeFilterResponse(Cell cell, MatchCode matchCode,
    ReturnCode filterResponse) {
    switch (filterResponse) {
      case SKIP:
        if (matchCode == MatchCode.INCLUDE) {
          return MatchCode.SKIP;
        } else if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
          return MatchCode.SEEK_NEXT_COL;
        } else if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
          return MatchCode.SEEK_NEXT_ROW;
        }
        break;
      case NEXT_COL:
        if (matchCode == MatchCode.INCLUDE || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
          return columns.getNextRowOrNextColumn(cell);
        } else if (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
          return MatchCode.SEEK_NEXT_ROW;
        }
        break;
      case NEXT_ROW:
        return MatchCode.SEEK_NEXT_ROW;
      case SEEK_NEXT_USING_HINT:
        return MatchCode.SEEK_NEXT_USING_HINT;
      case INCLUDE:
        break;
      case INCLUDE_AND_NEXT_COL:
        if (matchCode == MatchCode.INCLUDE) {
          matchCode = MatchCode.INCLUDE_AND_SEEK_NEXT_COL;
        }
        break;
      case INCLUDE_AND_SEEK_NEXT_ROW:
        matchCode = MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
        break;
      default:
        throw new RuntimeException("UNEXPECTED");
    }

    // It means it is INCLUDE, INCLUDE_AND_SEEK_NEXT_COL or INCLUDE_AND_SEEK_NEXT_ROW.
    assert matchCode == MatchCode.INCLUDE || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL
      || matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;

    // We need to make sure that the number of cells returned will not exceed max version in scan
    // when the match code is INCLUDE* case.
    if (curColCell == null || !CellUtil.matchingRowColumn(cell, curColCell)) {
      count = 0;
      curColCell = cell;
    }
    count += 1;

    if (count > versionsAfterFilter) {
      // when the number of cells exceed max version in scan, we should return SEEK_NEXT_COL match
      // code, but if current code is INCLUDE_AND_SEEK_NEXT_ROW, we can optimize to choose the max
      // step between SEEK_NEXT_COL and INCLUDE_AND_SEEK_NEXT_ROW, which is SEEK_NEXT_ROW.
      if (((KnobRuntime.check(java.util.UUID.fromString("6c09fd9e-b35d-3053-9ce7-fd4917340a40"))) ? ((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_ROW)) : (((KnobRuntime.check(java.util.UUID.fromString("090dda56-6f89-3e89-9814-a0d81eca945d"))) ? ((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_ROW)) : (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW))))) {
        matchCode = MatchCode.SEEK_NEXT_ROW;
      } else {
        matchCode = MatchCode.SEEK_NEXT_COL;
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("2bec4e3d-885c-3c30-8069-7984cc1bbc9d"))) ? (((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) && ((matchCode) == (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("b59c85f1-5ae9-359a-8ebd-faa333c8679b"))) ? ((matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) || ((matchCode) != (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("5bc53c0b-d4b9-3882-94b7-2f8e33247870"))) ? (((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) && (matchCode == MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("1094f143-6129-3b99-9b06-be654bc68dcd"))) ? ((matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) && ((matchCode) == (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("e89f2c79-6f50-3e12-8f54-c3e1b8ac9e5e"))) ? (((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) || (matchCode == MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("cebb48a8-8fde-386f-8264-ee4079a73d68"))) ? ((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("5151eaf6-cc3e-3d7f-b69a-a8b6df11d5dc"))) ? (((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) || ((matchCode) != (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("19128034-b688-398e-8bc3-5661f74201d6"))) ? ((matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) || (matchCode == MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("b1e9ac06-d319-3a99-8786-318ee0e81027"))) ? ((matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) || ((matchCode) == (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("c85fde3c-cfcf-3cc7-8663-9fc2ea54f86b"))) ? (((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) || ((matchCode) == (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("efd43cdf-a437-3955-a256-ab963ef74b27"))) ? ((matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) && ((matchCode) != (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("99c23519-9fde-3b81-a020-2432db14e964"))) ? ((matchCode) != (MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("23ae93dc-acff-3c25-9a48-7eaffc2fba8c"))) ? (((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) || ((matchCode) != (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("28852f50-a318-33a4-aa80-7429f195b0b7"))) ? (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) : (((KnobRuntime.check(java.util.UUID.fromString("2fc763ce-91a9-3049-b468-2a38dc505c44"))) ? (((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) && ((matchCode) != (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("7468e361-c360-3637-91e9-d612ddb82670"))) ? (((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) || (matchCode == MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("ace3386b-0c7e-352f-843e-162734e9c495"))) ? ((matchCode) == (MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("fce6b2b1-ccd6-369d-b0dd-dd89862f08df"))) ? (((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) && ((matchCode) == (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("12e87eb5-94e6-376d-b58c-ff4038117a39"))) ? (((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) && ((matchCode) != (MatchCode.SEEK_NEXT_COL))) : (((KnobRuntime.check(java.util.UUID.fromString("ccf6a798-2afd-30df-8674-b46650c115f5"))) ? ((matchCode) == (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("e993ce2a-f28c-3e2a-82f9-bdaf05d11d9b"))) ? ((matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) && (matchCode == MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("3ba46a44-0fd4-3fa4-a588-3faaa0c42065"))) ? (matchCode == MatchCode.SEEK_NEXT_COL) : (((KnobRuntime.check(java.util.UUID.fromString("fb48b12e-c0cf-3911-8285-6e406c98dc26"))) ? (((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) && (matchCode == MatchCode.SEEK_NEXT_COL)) : (((KnobRuntime.check(java.util.UUID.fromString("032d64ff-edb5-3bd3-bf4f-ff0846a5b534"))) ? (((matchCode) != (MatchCode.INCLUDE_AND_SEEK_NEXT_COL)) || ((matchCode) == (MatchCode.SEEK_NEXT_COL))) : (matchCode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL || matchCode == MatchCode.SEEK_NEXT_COL))))))))))))))))))))))))))))))))))))))))))))))))) {
      // Update column tracker to next column, As we use the column hint from the tracker to seek
      // to next cell (HBASE-19749)
      columns.doneWithColumn(cell);
    }
    return matchCode;
  }

  protected abstract boolean isGet();

  protected abstract boolean moreRowsMayExistsAfter(int cmpToStopRow);

  @Override
  public boolean moreRowsMayExistAfter(Cell cell) {
    // If a 'get' Scan -- we are doing a Get (every Get is a single-row Scan in implementation) --
    // then we are looking at one row only, the one specified in the Get coordinate..so we know
    // for sure that there are no more rows on this Scan
    if (isGet()) {
      return false;
    }
    // If no stopRow, return that there may be more rows. The tests that follow depend on a
    // non-empty, non-default stopRow so this little test below short-circuits out doing the
    // following compares.
    if (this.stopRow == null || this.stopRow.length == 0) {
      return true;
    }
    return moreRowsMayExistsAfter(rowComparator.compareRows(cell, stopRow, 0, stopRow.length));
  }

  public static UserScanQueryMatcher create(Scan scan, ScanInfo scanInfo,
    NavigableSet<byte[]> columns, long oldestUnexpiredTS, long now,
    RegionCoprocessorHost regionCoprocessorHost) throws IOException {
    boolean hasNullColumn =
      !(columns != null && columns.size() != 0 && columns.first().length != 0);
    Pair<DeleteTracker, ColumnTracker> trackers =
      getTrackers(regionCoprocessorHost, columns, scanInfo, oldestUnexpiredTS, scan);
    DeleteTracker deleteTracker = trackers.getFirst();
    ColumnTracker columnTracker = trackers.getSecond();
    if (scan.isRaw()) {
      return RawScanQueryMatcher.create(scan, scanInfo, columnTracker, hasNullColumn,
        oldestUnexpiredTS, now);
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("0068bb4e-e4a8-33eb-9b18-3d78a6f88664"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b986c16-24fe-3e4b-8488-e06fc1e5b70e"))) {
oldestUnexpiredTS = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a47ddda-b896-3c57-807d-165c6a09f160"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c7fe023-45c2-34e8-9fad-7fba64d5608f"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cbab8729-bea4-37ea-822f-daaf56a17382"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66a40250-ac66-3d95-97bc-abd017100f41"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9212210a-3eeb-3752-8d13-ad19f5dc75f9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ecbc608a-51bc-396f-8291-589bd636feb1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("25a1b1ce-da0b-37b8-a9c9-03ce7f0c86e9"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16e575fd-23f4-3d87-b81b-048edc1d9256"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ac8b9f1-8161-3f5c-9a4b-bc619bf84d23"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c05e8603-ec19-3f69-a044-35033c60ee06"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("52ca58b4-da3d-36e4-9476-8172451243f4"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scan);
    field.set(scan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06b10a82-c83e-35e1-ba76-a683266d9092"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8a454948-6212-3b9e-8bd6-415634200b8c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("098c739e-c72f-362d-b9ec-b2feaef3b074"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("415bc39f-cb6e-378d-ae97-281aae1a0675"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2052178c-b53c-3a3f-81ba-2f12a11d99ad"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("931fbc47-605d-3646-9e5f-c6ab36734e34"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3fdd15f9-51ac-3fbd-87ca-3beee278bf49"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd76cdef-1e2d-39e7-9a85-1594a227626d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc86e8f0-941f-3347-9db3-fba720c8c9b3"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("699045f9-38b6-3d92-b36c-06e376e59c35"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("226d83c3-ed2d-3d1c-a0a8-abc41d6eaab4"))) {
hasNullColumn = !hasNullColumn;
}
if(KnobRuntime.check(java.util.UUID.fromString("eed75f57-875f-380a-9859-cb2f16b7f0d4"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f08e20c-8701-3c73-bfbe-3620b570a2c9"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22bf7d3a-fd0d-3f45-8dbd-17d719e20630"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dbbe50db-55cd-385e-a098-263392ffd47f"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("mvccReadPoint");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64f2b24b-5dd6-3d59-a01a-a7e744f922ea"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf80f55d-b7d3-3dda-96c8-444e9a47b9e9"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3851a098-1a66-3397-a33a-1ff77f9d679c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("729199e3-880e-37d0-9886-818c796ec385"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb61e347-95ff-332e-a604-1214693718b6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("43941e42-6b54-3a30-8007-8ba06e1d156a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb886a5e-71cb-3082-9796-ef28da7d4636"))) {
hasNullColumn = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c47a70ec-2e80-3847-b87f-1e401e68e33a"))) {
oldestUnexpiredTS += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2e2390a5-0dea-325a-b554-6a507bbd9b16"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f4c7599a-973a-31bf-94ac-f6c6ea910dd4"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("90ddc162-63b9-3eb8-a1b1-15169e39d2c6"))) {
oldestUnexpiredTS = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0d525d75-01e3-3011-9255-3b608c177460"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a8882dcb-395c-34aa-b49e-58c8abda4209"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b567571c-9233-3ecc-af52-910f5ea5341c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("89c5ee1f-ded0-3f28-8b29-b4571290a20a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ca8c1ca-9880-3fa8-89df-f711c0064b47"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2b89655-a86f-3972-a272-f22ae4d54214"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("822c7f13-d762-3cd9-99d8-5b606c7419da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e007aa7e-bee8-30a8-8bf9-07f17303356e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1fd92bde-99ae-3d2e-bf62-42a93cedeaf8"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc4f68bf-f3d9-3037-ace7-52ded8b8e0fc"))) {
hasNullColumn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("dc5c5425-f304-38af-84ac-d8485c0b34a3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8bbbb513-86b4-38a7-a12b-e16460f51a25"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("399e3393-6136-3144-b437-57eb696731d6"))) {
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
      return NormalUserScanQueryMatcher.create(scan, scanInfo, columnTracker, deleteTracker,
        hasNullColumn, oldestUnexpiredTS, now);
    }
  }
}
