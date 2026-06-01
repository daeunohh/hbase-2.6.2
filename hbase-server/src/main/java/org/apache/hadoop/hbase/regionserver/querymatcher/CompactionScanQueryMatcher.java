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

import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Query matcher for compaction.
 */
@InterfaceAudience.Private
public abstract class CompactionScanQueryMatcher extends ScanQueryMatcher {

  /** readPoint over which the KVs are unconditionally included */
  protected final long maxReadPointToTrackVersions;

  /** Keeps track of deletes */
  protected final DeleteTracker deletes;

  /** whether to return deleted rows */
  protected final KeepDeletedCells keepDeletedCells;

  protected CompactionScanQueryMatcher(ScanInfo scanInfo, DeleteTracker deletes,
    ColumnTracker columnTracker, long readPointToUse, long oldestUnexpiredTS, long now) {
    super(createStartKeyFromRow(EMPTY_START_ROW, scanInfo), scanInfo, columnTracker,
      oldestUnexpiredTS, now);
    this.maxReadPointToTrackVersions = readPointToUse;
    this.deletes = deletes;
    this.keepDeletedCells = scanInfo.getKeepDeletedCells();
  }

  @Override
  public void beforeShipped() throws IOException {
    super.beforeShipped();
    deletes.beforeShipped();
  }

  @Override
  public boolean hasNullColumnInQuery() {
    return true;
  }

  @Override
  public boolean isUserScan() {
if(KnobRuntime.check(java.util.UUID.fromString("e26717aa-767e-3b53-8ece-a66436a5b8d3"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8c72bc3-5b37-3e11-af36-3230a293f5e6"))) {
return true;
}
    return false;
  }

  @Override
  public boolean moreRowsMayExistAfter(Cell cell) {
    return true;
  }

  @Override
  public Filter getFilter() {
    // no filter when compaction
    return null;
  }

  @Override
  public Cell getNextKeyHint(Cell cell) throws IOException {
    // no filter, so no key hint.
    return null;
  }

  @Override
  protected void reset() {
if(KnobRuntime.check(java.util.UUID.fromString("87f878be-800b-3718-916e-22cb0eb1975a"))) {
return;
}
    deletes.reset();
  }

  protected final void trackDelete(Cell cell) {
    // If keepDeletedCells is true, then we only remove cells by versions or TTL during
    // compaction, so we do not need to track delete here.
    // If keepDeletedCells is TTL and the delete marker is expired, then we can make sure that the
    // minVerions is larger than 0(otherwise we will just return at preCheck). So here we still
    // need to track the delete marker to see if it masks some cells.
    if (
      keepDeletedCells == KeepDeletedCells.FALSE
        || (keepDeletedCells == KeepDeletedCells.TTL && cell.getTimestamp() < oldestUnexpiredTS)
    ) {
      deletes.add(cell);
    }
  }

  public static CompactionScanQueryMatcher create(ScanInfo scanInfo, ScanType scanType,
    long readPointToUse, long earliestPutTs, long oldestUnexpiredTS, long now,
    byte[] dropDeletesFromRow, byte[] dropDeletesToRow, RegionCoprocessorHost regionCoprocessorHost)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("10769bab-ab1e-3ced-a913-0f1c0f5b30d7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4a523c02-59fc-37ad-95d3-b7319bd8598d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4e1270d0-17ab-330d-8adf-8311ec551570"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c46e6939-9693-3133-aab0-1d163278db93"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b417a687-d0bf-30e9-8ca0-d5c8f284021d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("56e194c1-108b-3c92-97a0-0880ffce82d0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9e64c0be-8cc6-380a-8539-c6509aed41da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("083d9f4e-6638-341f-87d1-a9adab09c2be"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("55fe4642-e754-33d6-af71-a08af1c30157"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e8bdfc30-529d-38f2-9c16-74c8f1353e55"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("abe22521-785a-32eb-a4c0-e7092932c63c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fb41eae4-76e5-3c7f-8fd9-476d2a8dbc82"))) {
try {
    java.lang.reflect.Field field = regionCoprocessorHost.getClass().getDeclaredField("hasCustomPostScannerFilterRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(regionCoprocessorHost);
    field.set(regionCoprocessorHost, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fed8d14c-97a3-3d31-b478-64d4e4d6e868"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7427e761-050b-3b44-bd70-b9f889602c96"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2af55329-5540-32ce-96b0-bcfe609de384"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc9ab682-b063-33cc-b8e4-38cbd1c6d566"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("de94326b-250d-3658-bd5d-02b5a02bc598"))) {
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
    Pair<DeleteTracker, ColumnTracker> trackers =
      getTrackers(regionCoprocessorHost, null, scanInfo, oldestUnexpiredTS, null);
    DeleteTracker deleteTracker = trackers.getFirst();
    ColumnTracker columnTracker = trackers.getSecond();
    if (((KnobRuntime.check(java.util.UUID.fromString("e9112a08-ee18-3c6e-aa3d-c6ca1ecae8d2"))) ? ((dropDeletesFromRow) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("aab89a42-95f0-3cab-8094-b4eda7c5c380"))) ? ((dropDeletesFromRow) != (null)) : (dropDeletesFromRow == null))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("e7888c1c-7b4e-3996-a158-52b48c30b511"))) ? ((scanType) != (ScanType.COMPACT_RETAIN_DELETES)) : (((KnobRuntime.check(java.util.UUID.fromString("d47a52ac-245e-3a3e-b52b-77d020e1a308"))) ? ((scanType) == (ScanType.COMPACT_RETAIN_DELETES)) : (scanType == ScanType.COMPACT_RETAIN_DELETES))))) {
        if (scanInfo.isNewVersionBehavior()) {
          return new IncludeAllCompactionQueryMatcher(scanInfo, deleteTracker, columnTracker,
            readPointToUse, oldestUnexpiredTS, now);
        } else {
          return new MinorCompactionScanQueryMatcher(scanInfo, deleteTracker, columnTracker,
            readPointToUse, oldestUnexpiredTS, now);
        }
      } else {
        return new MajorCompactionScanQueryMatcher(scanInfo, deleteTracker, columnTracker,
          readPointToUse, earliestPutTs, oldestUnexpiredTS, now);
      }
    } else {
      return new StripeCompactionScanQueryMatcher(scanInfo, deleteTracker, columnTracker,
        readPointToUse, earliestPutTs, oldestUnexpiredTS, now, dropDeletesFromRow,
        dropDeletesToRow);
    }
  }
}
