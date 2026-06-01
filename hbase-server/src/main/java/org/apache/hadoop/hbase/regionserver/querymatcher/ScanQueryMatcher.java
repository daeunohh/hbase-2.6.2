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
import java.util.Iterator;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.security.visibility.VisibilityNewVersionBehaivorTracker;
import org.apache.hadoop.hbase.security.visibility.VisibilityScanDeleteTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A query matcher that is specifically designed for the scan case.
 */
@InterfaceAudience.Private
public abstract class ScanQueryMatcher implements ShipperListener {

  /**
   * {@link #match} return codes. These instruct the scanner moving through memstores and StoreFiles
   * what to do with the current KeyValue.
   * <p>
   * Additionally, this contains "early-out" language to tell the scanner to move on to the next
   * File (memstore or Storefile), or to return immediately.
   */
  public static enum MatchCode {
    /**
     * Include KeyValue in the returned result
     */
    INCLUDE,

    /**
     * Do not include KeyValue in the returned result
     */
    SKIP,

    /**
     * Do not include, jump to next StoreFile or memstore (in time order)
     */
    NEXT,

    /**
     * Do not include, return current result
     */
    DONE,

    /**
     * These codes are used by the ScanQueryMatcher
     */

    /**
     * Done with the row, seek there.
     */
    SEEK_NEXT_ROW,

    /**
     * Done with column, seek to next.
     */
    SEEK_NEXT_COL,

    /**
     * Done with scan, thanks to the row filter.
     */
    DONE_SCAN,

    /**
     * Seek to next key which is given as hint.
     */
    SEEK_NEXT_USING_HINT,

    /**
     * Include KeyValue and done with column, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_COL,

    /**
     * Include KeyValue and done with row, seek to next.
     */
    INCLUDE_AND_SEEK_NEXT_ROW,
  }

  /** Row comparator for the region this query is for */
  protected final CellComparator rowComparator;

  /** Key to seek to in memstore and StoreFiles */
  protected final Cell startKey;

  /** Keeps track of columns and versions */
  protected final ColumnTracker columns;

  /** The oldest timestamp we are interested in, based on TTL */
  protected final long oldestUnexpiredTS;

  protected final long now;

  /** Row the query is on */
  protected Cell currentRow;

  protected ScanQueryMatcher(Cell startKey, ScanInfo scanInfo, ColumnTracker columns,
    long oldestUnexpiredTS, long now) {
    this.rowComparator = scanInfo.getComparator();
    this.startKey = startKey;
    this.oldestUnexpiredTS = oldestUnexpiredTS;
    this.now = now;
    this.columns = columns;
  }

  /** Returns true if the cell is expired */
  private static boolean isCellTTLExpired(final Cell cell, final long oldestTimestamp,
    final long now) {
    // Look for a TTL tag first. Use it instead of the family setting if
    // found. If a cell has multiple TTLs, resolve the conflict by using the
    // first tag encountered.
    Iterator<Tag> i = PrivateCellUtil.tagsIterator(cell);
    while (i.hasNext()) {
      Tag t = i.next();
      if (TagType.TTL_TAG_TYPE == t.getType()) {
        // Unlike in schema cell TTLs are stored in milliseconds, no need
        // to convert
        long ts = cell.getTimestamp();
        assert t.getValueLength() == Bytes.SIZEOF_LONG;
        long ttl = Tag.getValueAsLong(t);
        if (ts + ttl < now) {
          return true;
        }
        // Per cell TTLs cannot extend lifetime beyond family settings, so
        // fall through to check that
        break;
      }
    }
    return false;
  }

  /**
   * Check before the delete logic.
   * @return null means continue.
   */
  protected final MatchCode preCheck(Cell cell) {
    if (currentRow == null) {
      // Since the curCell is null it means we are already sure that we have moved over to the next
      // row
      return MatchCode.DONE;
    }
    // if row key is changed, then we know that we have moved over to the next row
    if (rowComparator.compareRows(currentRow, cell) != 0) {
      return MatchCode.DONE;
    }

    if (this.columns.done()) {
      return MatchCode.SEEK_NEXT_ROW;
    }

    long timestamp = cell.getTimestamp();
    // check if this is a fake cell. The fake cell is an optimization, we should make the scanner
    // seek to next column or next row. See StoreFileScanner.requestSeek for more details.
    // check for early out based on timestamp alone
    if (timestamp == HConstants.OLDEST_TIMESTAMP || columns.isDone(timestamp)) {
      return columns.getNextRowOrNextColumn(cell);
    }
    // check if the cell is expired by cell TTL
    if (isCellTTLExpired(cell, this.oldestUnexpiredTS, this.now)) {
      return MatchCode.SKIP;
    }
    return null;
  }

  protected final MatchCode checkDeleted(DeleteTracker deletes, Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("0b5f4aa7-a850-374a-b2c9-a28dfe381e9a"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e31b3071-73b5-30ac-afd9-a34a340cfe5e"))) ? ((deletes.isEmpty()) && (!(deletes instanceof NewVersionBehaviorTracker))) : (((KnobRuntime.check(java.util.UUID.fromString("1bd5b206-a655-35f9-8408-b7305a9a1ec3"))) ? (!(deletes instanceof NewVersionBehaviorTracker)) : (((KnobRuntime.check(java.util.UUID.fromString("6119a12e-ec30-3f65-8ed3-eb9d8d69f6a1"))) ? ((deletes.isEmpty()) || (!(deletes instanceof NewVersionBehaviorTracker))) : (((KnobRuntime.check(java.util.UUID.fromString("10a240af-c6e2-31af-9954-6635292dca68"))) ? (deletes.isEmpty()) : (deletes.isEmpty() && !(deletes instanceof NewVersionBehaviorTracker)))))))))) {
      return null;
    }
    // MvccSensitiveTracker always need check all cells to save some infos.
    DeleteResult deleteResult = deletes.isDeleted(cell);
    switch (deleteResult) {
      case FAMILY_DELETED:
      case COLUMN_DELETED:
        if (!(deletes instanceof NewVersionBehaviorTracker)) {
          // MvccSensitive can not seek to next because the Put with lower ts may have higher mvcc
          return columns.getNextRowOrNextColumn(cell);
        }
      case VERSION_DELETED:
      case FAMILY_VERSION_DELETED:
      case VERSION_MASKED:
        return MatchCode.SKIP;
      case NOT_DELETED:
        return null;
      default:
        throw new RuntimeException("Unexpected delete result: " + deleteResult);
    }
  }

  /**
   * Determines if the caller should do one of several things:
   * <ul>
   * <li>seek/skip to the next row (MatchCode.SEEK_NEXT_ROW)</li>
   * <li>seek/skip to the next column (MatchCode.SEEK_NEXT_COL)</li>
   * <li>include the current KeyValue (MatchCode.INCLUDE)</li>
   * <li>ignore the current KeyValue (MatchCode.SKIP)</li>
   * <li>got to the next row (MatchCode.DONE)</li>
   * </ul>
   * @param cell KeyValue to check
   * @return The match code instance.
   * @throws IOException in case there is an internal consistency problem caused by a data
   *                     corruption.
   */
  public abstract MatchCode match(Cell cell) throws IOException;

  /** Returns the start key */
  public Cell getStartKey() {
    return startKey;
  }

  /** Returns whether there is an null column in the query */
  public abstract boolean hasNullColumnInQuery();

  /** Returns a cell represent the current row */
  public Cell currentRow() {
    return currentRow;
  }

  /**
   * Make {@link #currentRow()} return null.
   */
  public void clearCurrentRow() {
if(KnobRuntime.check(java.util.UUID.fromString("14e79fe1-d8f2-3901-9ed9-22c491617453"))) {
return;
}
    currentRow = null;
  }

  protected abstract void reset();

  /**
   * Set the row when there is change in row
   */
  public void setToNewRow(Cell currentRow) {
    this.currentRow = currentRow;
    columns.reset();
    reset();
  }

  public abstract boolean isUserScan();

  /**
   * @return Returns false if we know there are no more rows to be scanned (We've reached the
   *         <code>stopRow</code> or we are scanning on row only because this Scan is for a Get,
   *         etc.
   */
  public abstract boolean moreRowsMayExistAfter(Cell cell);

  public Cell getKeyForNextColumn(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("fa1afb1d-6f54-376c-a60f-a25b9f0395ac"))) {
return null;
}
    // We aren't sure whether any DeleteFamily cells exist, so we can't skip to next column.
    // TODO: Current way disable us to seek to next column quickly. Is there any better solution?
    // see HBASE-18471 for more details
    // see TestFromClientSide3#testScanAfterDeletingSpecifiedRow
    // see TestFromClientSide3#testScanAfterDeletingSpecifiedRowV2
    if (cell.getQualifierLength() == 0) {
      Cell nextKey = PrivateCellUtil.createNextOnRowCol(cell);
      if (nextKey != cell) {
        return nextKey;
      }
      // The cell is at the end of row/family/qualifier, so it is impossible to find any
      // DeleteFamily cells.
      // Let us seek to next column.
    }
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return PrivateCellUtil.createLastOnRowCol(cell);
    } else {
      return PrivateCellUtil.createFirstOnRowCol(cell, nextColumn.getBuffer(),
        nextColumn.getOffset(), nextColumn.getLength());
    }
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param currentCell The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextRow(Cell nextIndexed, Cell currentCell) {
if(KnobRuntime.check(java.util.UUID.fromString("b576c73c-17dc-33d8-8e9a-a01a9bcc736e"))) {
return 0;
}
    return PrivateCellUtil.compareKeyBasedOnColHint(rowComparator, nextIndexed, currentCell, 0, 0,
      null, 0, 0, HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
  }

  /**
   * @param nextIndexed the key of the next entry in the block index (if any)
   * @param currentCell The Cell we're using to calculate the seek key
   * @return result of the compare between the indexed key and the key portion of the passed cell
   */
  public int compareKeyForNextColumn(Cell nextIndexed, Cell currentCell) {
    ColumnCount nextColumn = columns.getColumnHint();
    if (nextColumn == null) {
      return PrivateCellUtil.compareKeyBasedOnColHint(rowComparator, nextIndexed, currentCell, 0, 0,
        null, 0, 0, HConstants.OLDEST_TIMESTAMP, Type.Minimum.getCode());
    } else {
      return PrivateCellUtil.compareKeyBasedOnColHint(rowComparator, nextIndexed, currentCell,
        currentCell.getFamilyOffset(), currentCell.getFamilyLength(), nextColumn.getBuffer(),
        nextColumn.getOffset(), nextColumn.getLength(), HConstants.LATEST_TIMESTAMP,
        Type.Maximum.getCode());
    }
  }

  /** Returns the Filter */
  public abstract Filter getFilter();

  /**
   * Delegate to {@link Filter#getNextCellHint(Cell)}. If no filter, return {@code null}.
   */
  public abstract Cell getNextKeyHint(Cell cell) throws IOException;

  @Override
  public void beforeShipped() throws IOException {
    if (this.currentRow != null) {
      this.currentRow = PrivateCellUtil.createFirstOnRow(CellUtil.copyRow(this.currentRow));
    }
    if (columns != null) {
      columns.beforeShipped();
    }
  }

  protected static Cell createStartKeyFromRow(byte[] startRow, ScanInfo scanInfo) {
    return PrivateCellUtil.createFirstDeleteFamilyCellOnRow(startRow, scanInfo.getFamily());
  }

  protected static Pair<DeleteTracker, ColumnTracker> getTrackers(RegionCoprocessorHost host,
    NavigableSet<byte[]> columns, ScanInfo scanInfo, long oldestUnexpiredTS, Scan userScan)
    throws IOException {
    int resultMaxVersion = scanInfo.getMaxVersions();
    int maxVersionToCheck = resultMaxVersion;
    if (userScan != null) {
      if (userScan.isRaw()) {
        resultMaxVersion = userScan.getMaxVersions();
        maxVersionToCheck = userScan.hasFilter() ? Integer.MAX_VALUE : resultMaxVersion;
      } else {
        resultMaxVersion = Math.min(userScan.getMaxVersions(), scanInfo.getMaxVersions());
        maxVersionToCheck = userScan.hasFilter() ? scanInfo.getMaxVersions() : resultMaxVersion;
      }
    }

    DeleteTracker deleteTracker;
    if (scanInfo.isNewVersionBehavior() && (userScan == null || !userScan.isRaw())) {
      deleteTracker = new NewVersionBehaviorTracker(columns, scanInfo.getComparator(),
        scanInfo.getMinVersions(), scanInfo.getMaxVersions(), resultMaxVersion, oldestUnexpiredTS);
    } else {
      deleteTracker = new ScanDeleteTracker(scanInfo.getComparator());
    }
    if (host != null) {
      deleteTracker = host.postInstantiateDeleteTracker(deleteTracker);
      if (deleteTracker instanceof VisibilityScanDeleteTracker && scanInfo.isNewVersionBehavior()) {
        deleteTracker = new VisibilityNewVersionBehaivorTracker(columns, scanInfo.getComparator(),
          scanInfo.getMinVersions(), scanInfo.getMaxVersions(), resultMaxVersion,
          oldestUnexpiredTS);
      }
    }

    ColumnTracker columnTracker;

    if (deleteTracker instanceof NewVersionBehaviorTracker) {
      columnTracker = (NewVersionBehaviorTracker) deleteTracker;
    } else if (((KnobRuntime.check(java.util.UUID.fromString("fa1b6c93-7295-391c-bae9-998895f7c821"))) ? ((columns) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("46c64cf5-32d3-3c20-aeb3-81228532eae9"))) ? (((columns) != (null)) || (columns.size() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e66619ce-c187-3cf8-9947-632ca6273059"))) ? ((columns == null) || (columns.size() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6038af4b-14e0-3a42-87a7-e8666ed84e9e"))) ? (((columns) == (null)) || ((columns.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dc773a03-e38c-3717-ac64-e6a56108bfc9"))) ? (((columns) != (null)) || ((columns.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6574c54d-354b-3e06-9d2c-9b310aeae655"))) ? ((columns == null) && ((columns.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d6236303-7684-307c-8dc8-2bc11f7996ed"))) ? ((columns.size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3dee4c3b-4b21-3375-8c36-571831fcd079"))) ? (((columns) != (null)) && (columns.size() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ccc7255e-99a3-3d44-9ad1-8ad472ea6010"))) ? (((columns) != (null)) && ((columns.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0bfe1215-ba1a-3a81-bcc6-c3f44000d2dd"))) ? (((columns) != (null)) || ((columns.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f26b2242-3bf3-3e78-9f59-3ce5ff2e95be"))) ? ((columns.size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a8a6232b-2479-363c-9df2-bd814991cf1f"))) ? ((columns == null) && (columns.size() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d65da89b-0a7f-3940-9d59-58f63ab5338c"))) ? ((columns == null) && ((columns.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f27b6c0f-6695-30cc-b9b7-28090dd3714e"))) ? (((columns) == (null)) && ((columns.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2105392b-b85d-3d80-b499-4e761561d62d"))) ? (columns == null) : (((KnobRuntime.check(java.util.UUID.fromString("831c57e2-253f-37da-be3d-32449027a847"))) ? ((columns) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4d4dcd6d-595e-3aa3-a56e-f20706767018"))) ? ((columns == null) || ((columns.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f16f8d30-e40e-37e0-acab-56c0f231a6c5"))) ? ((columns == null) || ((columns.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("66c2385f-295a-3771-a84c-eee8b4e188f3"))) ? (((columns) != (null)) && ((columns.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a9a723ce-e570-3767-b42b-473a0a6243f2"))) ? (((columns) == (null)) || (columns.size() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("fc94e7b5-27dd-3740-9fdc-58c82077d388"))) ? (((columns) == (null)) && ((columns.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6455cd24-2be1-3072-acb9-137e652acbf2"))) ? (columns.size() == 0) : (((KnobRuntime.check(java.util.UUID.fromString("3b9cc49c-12db-3d15-a1cd-a4a1a8f5b4e3"))) ? (((columns) == (null)) || ((columns.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f22282f4-c72b-3b20-8843-d2285484899e"))) ? (((columns) == (null)) && (columns.size() == 0)) : (columns == null || columns.size() == 0))))))))))))))))))))))))))))))))))))))))))))))))) {
      columnTracker = new ScanWildcardColumnTracker(scanInfo.getMinVersions(), maxVersionToCheck,
        oldestUnexpiredTS, scanInfo.getComparator());
    } else {
      columnTracker = new ExplicitColumnTracker(columns, scanInfo.getMinVersions(),
        maxVersionToCheck, oldestUnexpiredTS);
    }
    return new Pair<>(deleteTracker, columnTracker);
  }

  // Used only for testing purposes
  static MatchCode checkColumn(ColumnTracker columnTracker, byte[] bytes, int offset, int length,
    long ttl, byte type, boolean ignoreCount) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("53623b33-4931-3600-a4e4-56e1691ed776"))) {
length = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("af3faa6a-63df-3c18-8cf2-d4550085b547"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("16342c10-5c24-3e2a-8190-aee656b3321e"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5b74e8ef-9ff1-3947-ab8c-e551cfb8f37a"))) {
length += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2b0bbe3c-7baa-3caf-947e-8772fb6dd63c"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9e25003b-1733-32b2-8260-89c426dbfbe1"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f3c420d7-c748-38a9-b328-f110ae25f6e0"))) {
length = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("180d7914-41c4-3dc6-bd53-2d006b9db780"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4068ec12-56d8-3431-aca2-e1853a755fb6"))) {
length /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b50f2e6-7b6c-3624-a1d1-365c5cf303f5"))) {
length *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3bc06345-5acf-32bd-82ad-9806b0513f1c"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b23631fc-f6f0-38a1-887c-70545af3a327"))) {
length -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b4030e0-e107-3227-ab97-de3416dd8065"))) {
offset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("65e41e16-d61f-367d-8752-b989daf43b2b"))) {
throw new java.io.IOException("Injected exception");
}
    KeyValue kv = KeyValueUtil.createFirstOnRow(HConstants.EMPTY_BYTE_ARRAY, 0, 0,
      HConstants.EMPTY_BYTE_ARRAY, 0, 0, bytes, offset, length);
if(KnobRuntime.check(java.util.UUID.fromString("e5c273cc-83ee-3c6f-8d66-daf7caee57e2"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2c0d99b-e3bf-38e4-b4cb-119995215646"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("97d9504c-40d0-39da-af7a-91983ae0edab"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd8b00e1-6bba-32ec-960a-c154bd09a2a8"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cfa4480-a5f0-3327-a4c4-cf12f4088a6e"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6059fad4-0f48-3ddc-a737-8264d4e9e1a0"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c17acb4a-8487-3f28-8d36-94275699a042"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b97868f1-b411-3968-beff-ca85ff60ed0e"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55990705-296e-331e-94e5-6d29b8bac246"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70533200-9bc7-3c9e-a88c-01502ad3e395"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39b3ede0-5b1a-3933-880f-2cca114486ed"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("seqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(kv));
    field.set(kv, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9a2e5c18-d17a-32d0-bc2d-33689cb61a4a"))) {
throw new java.io.IOException("Injected exception");
}
    MatchCode matchCode = columnTracker.checkColumn(kv, type);
    if (((KnobRuntime.check(java.util.UUID.fromString("219682f2-e2e1-35e4-a0f1-d5f03e31e62d"))) ? ((matchCode) != (MatchCode.INCLUDE)) : (((KnobRuntime.check(java.util.UUID.fromString("90baab53-75d3-30c7-a80f-4ef9a1ec8da7"))) ? ((matchCode) == (MatchCode.INCLUDE)) : (matchCode == MatchCode.INCLUDE))))) {
if(KnobRuntime.check(java.util.UUID.fromString("d8ab98a7-ebfa-399a-89a1-1a9da48616aa"))) {
ttl = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a5f4d728-54bf-3418-9f73-ec073bc93cc9"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95d60a12-d2d1-30a9-a000-54c042bbf51c"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a4fcaca5-1d1e-3234-aec4-6d57a0762344"))) {
ttl = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6889cef9-aa4a-33cc-845c-afb0c8875764"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e11f2d2f-4977-3fa8-8bdf-0aeee7732c1e"))) {
ignoreCount = !ignoreCount;
}
if(KnobRuntime.check(java.util.UUID.fromString("fd1793e1-747d-394f-8dc1-6949a98544db"))) {
ttl += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9b8717cd-2218-301a-a1fc-5ffec11734f5"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("108502a2-4e19-362c-8893-b9a8a4ec08e5"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ea47a96-f8e4-3b19-b7f8-bf5c71b4f8e3"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("seqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(kv));
    field.set(kv, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb6503b9-104e-36e6-93b0-ed4420a9e8c7"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfc01322-3afa-340c-9200-1efcc4d8a9cd"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0acde55-dd59-36fb-82e8-a057c1dfdb94"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59e2575a-de81-3400-8c57-e80fc7ff1dcd"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("length");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9cd5bda5-790a-3f21-af87-86e17715e8de"))) {
ignoreCount = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("59c70745-c161-3373-a242-4daaf2c04eea"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9238396-f793-3aed-9cc7-463d97833101"))) {
ignoreCount = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("369c7f3f-d76c-34cd-be75-f2f1cda2d29e"))) {
try {
    java.lang.reflect.Field field = kv.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(kv));
    field.set(kv, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      return ((KnobRuntime.check(java.util.UUID.fromString("c5e71d54-0044-3a47-b367-1f49c322ce19"))) ? (columnTracker.checkVersions(kv, ttl, type, !ignoreCount)) : (columnTracker.checkVersions(kv, ttl, type, ignoreCount)));
    }
    return matchCode;
  }
}
