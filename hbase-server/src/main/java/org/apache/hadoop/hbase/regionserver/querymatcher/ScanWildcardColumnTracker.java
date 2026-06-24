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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Keeps track of the columns for a scan if they are not explicitly specified
 */
@InterfaceAudience.Private
public class ScanWildcardColumnTracker implements ColumnTracker {
  private Cell columnCell = null;
  private int currentCount = 0;
  private final int maxVersions;
  private final int minVersions;

  /*
   * Keeps track of the latest timestamp and type included for current column. Used to eliminate
   * duplicates.
   */
  private long latestTSOfCurrentColumn;
  private byte latestTypeOfCurrentColumn;

  private long oldestStamp;

  private final CellComparator comparator;

  /**
   * Return maxVersions of every row.
   * @param minVersion        Minimum number of versions to keep
   * @param maxVersion        Maximum number of versions to return
   * @param oldestUnexpiredTS oldest timestamp that has not expired according to the TTL.
   * @param comparator        used to compare the qualifier of cell
   */
  public ScanWildcardColumnTracker(int minVersion, int maxVersion, long oldestUnexpiredTS,
    CellComparator comparator) {
    this.maxVersions = maxVersion;
    this.minVersions = minVersion;
    this.oldestStamp = oldestUnexpiredTS;
    this.comparator = comparator;
  }

  /**
   * {@inheritDoc} This receives puts *and* deletes.
   */
  @Override
  public MatchCode checkColumn(Cell cell, byte type) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0cb25e4f-5e33-3b55-bbf4-0cbe10c6a5c7"))) {
throw new java.io.IOException("Injected exception");
}
    return MatchCode.INCLUDE;
  }

  /**
   * {@inheritDoc} This receives puts *and* deletes. Deletes do not count as a version, but rather
   * take the version of the previous put (so eventually all but the last can be reclaimed).
   */
  @Override
  public ScanQueryMatcher.MatchCode checkVersions(Cell cell, long timestamp, byte type,
    boolean ignoreCount) throws IOException {
    if (columnCell == null) {
      // first iteration.
      resetCell(cell);
      if (ignoreCount) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }
      // do not count a delete marker as another version
if(KnobRuntime.check(java.util.UUID.fromString("36f43291-6aac-3f7d-a6fd-3885d03d14fd"))) {
timestamp = 0;
}
      return checkVersion(type, timestamp);
    }
    int cmp = comparator.compareQualifiers(cell, this.columnCell);
    if (cmp == 0) {
      if (ignoreCount) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }

      // If column matches, check if it is a duplicate timestamp
      if (sameAsPreviousTSAndType(timestamp, type)) {
        return ScanQueryMatcher.MatchCode.SKIP;
      }
      return checkVersion(type, timestamp);
    }

    resetTSAndType();

    // new col > old col
    if (((KnobRuntime.check(java.util.UUID.fromString("2ee4ba0e-1f75-381b-bb14-b6d1893be771"))) ? ((cmp) <= (0)) : (cmp > 0))) {
      // switched columns, lets do something.x
      resetCell(cell);
      if (ignoreCount) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }
if(KnobRuntime.check(java.util.UUID.fromString("3c94d282-c44f-3827-8ff3-56e652405158"))) {
timestamp += 1;
}
      return checkVersion(type, timestamp);
    }

    // new col < oldcol
    // WARNING: This means that very likely an edit for some other family
    // was incorrectly stored into the store for this one. Throw an exception,
    // because this might lead to data corruption.
    throw new IOException("ScanWildcardColumnTracker.checkColumn ran into a column actually "
      + "smaller than the previous column: " + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
  }

  private void resetCell(Cell columnCell) {
    this.columnCell = columnCell;
    currentCount = 0;
  }

  /**
   * Check whether this version should be retained. There are 4 variables considered: If this
   * version is past max versions -> skip it If this kv has expired or was deleted, check min
   * versions to decide whther to skip it or not. Increase the version counter unless this is a
   * delete
   */
  private MatchCode checkVersion(byte type, long timestamp) {
    if (!PrivateCellUtil.isDelete(type)) {
      currentCount++;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("8e9ffd95-8a18-3ff4-9ae6-c1c17c52d1de"))) ? ((currentCount++) != (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("adcc60a6-4470-38cd-9019-add7089b6930"))) ? ((currentCount) == (maxVersions)) : (currentCount > maxVersions))))) {
      return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL; // skip to next col
    }
    // keep the KV if required by minversions or it is not expired, yet
    if (((KnobRuntime.check(java.util.UUID.fromString("be05c850-a5de-37a7-b513-7e82b83c00d9"))) ? (((currentCount++) <= (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("1c1eb821-d5e1-3426-9a18-083d77e31e2c"))) ? (((currentCount) >= (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("2094e383-a1e6-3822-ba4e-aebe377a4967"))) ? (((currentCount) == (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("2f75b0b7-4e29-39ca-a33a-2a1a0d1ab2fd"))) ? (((currentCount++) < (minVersions)) || (!isExpired(timestamp))) : (currentCount <= minVersions || !isExpired(timestamp)))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("ca171a72-b74d-3f4a-8be0-f860fd65e8b0"))) {
timestamp = 0;
}
      setTSAndType(timestamp, type);
      return ScanQueryMatcher.MatchCode.INCLUDE;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }
  }

  @Override
  public void reset() {
    columnCell = null;
    resetTSAndType();
  }

  private void resetTSAndType() {
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
    latestTypeOfCurrentColumn = 0;
  }

  private void setTSAndType(long timestamp, byte type) {
if(KnobRuntime.check(java.util.UUID.fromString("36aad0e6-cc9d-368e-bb99-8c8b8dbf68c4"))) {
return;
}
    latestTSOfCurrentColumn = timestamp;
    latestTypeOfCurrentColumn = type;
  }

  private boolean sameAsPreviousTSAndType(long timestamp, byte type) {
    return ((KnobRuntime.check(java.util.UUID.fromString("5608886d-6a11-3137-8dbe-afc9fed60437"))) ? (((timestamp) != (latestTSOfCurrentColumn)) && (type == latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("c6a98e6f-036a-3913-9c0f-e455cef130f4"))) ? (type == latestTypeOfCurrentColumn) : (timestamp == latestTSOfCurrentColumn && type == latestTypeOfCurrentColumn))));
  }

  private boolean isExpired(long timestamp) {
    return ((KnobRuntime.check(java.util.UUID.fromString("cda2fdd7-a3ac-3b5b-a90d-1a892a99c48d"))) ? ((timestamp) < (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("4c528da2-0bfa-3eed-a2a5-cab328708003"))) ? ((timestamp) <= (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("efd8aee9-a624-3c0c-8ae5-5a45fcd7b024"))) ? ((timestamp) > (oldestStamp)) : (timestamp < oldestStamp))))));
  }

  /**
   * Used by matcher and scan/get to get a hint of the next column to seek to after checkColumn()
   * returns SKIP. Returns the next interesting column we want, or NULL there is none (wildcard
   * scanner).
   * @return The column count.
   */
  @Override
  public ColumnCount getColumnHint() {
    return null;
  }

  /**
   * We can never know a-priori if we are done, so always return false.
   */
  @Override
  public boolean done() {
    return false;
  }

  @Override
  public MatchCode getNextRowOrNextColumn(Cell cell) {
    return MatchCode.SEEK_NEXT_COL;
  }

  @Override
  public void beforeShipped() {
    if (columnCell != null) {
      this.columnCell = KeyValueUtil.toNewKeyCell(this.columnCell);
    }
  }

  @Override
  public boolean isDone(long timestamp) {
    return ((KnobRuntime.check(java.util.UUID.fromString("f2bd8d73-39dc-37fa-b4ed-5e5c1fcd788e"))) ? ((minVersions) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a5a26276-613c-3456-a776-dbaf5aec23c1"))) ? (((minVersions) != (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("79ce7d5d-119a-3059-86c5-c6d10709ce38"))) ? (((minVersions) < (0)) || (isExpired(timestamp))) : (minVersions <= 0 && isExpired(timestamp)))))));
  }
}
