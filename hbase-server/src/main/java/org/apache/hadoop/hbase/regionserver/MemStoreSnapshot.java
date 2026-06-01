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

import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * {@link MemStoreSnapshot} is a Context Object to hold details of the snapshot taken on a MemStore.
 * Details include the snapshot's identifier, count of cells in it and total memory size occupied by
 * all the cells, timestamp information of all the cells and the snapshot immutableSegment.
 * <p>
 * NOTE:Every time when {@link MemStoreSnapshot#getScanners} is called, we create new
 * {@link SnapshotSegmentScanner}s on the {@link MemStoreSnapshot#snapshotImmutableSegment},and
 * {@link Segment#incScannerCount} is invoked in the {@link SnapshotSegmentScanner} ctor to increase
 * the reference count of {@link MemStoreLAB} which used by
 * {@link MemStoreSnapshot#snapshotImmutableSegment}, so after we finish using these scanners, we
 * must call their close method to invoke {@link Segment#decScannerCount}.
 */
@InterfaceAudience.Private
public class MemStoreSnapshot {
  private final long id;
  private final int cellsCount;
  private final MemStoreSize memStoreSize;
  private final TimeRangeTracker timeRangeTracker;
  private final boolean tagsPresent;
  private final ImmutableSegment snapshotImmutableSegment;

  public MemStoreSnapshot(long id, ImmutableSegment snapshot) {
    this.id = id;
    this.cellsCount = snapshot.getCellsCount();
    this.memStoreSize = snapshot.getMemStoreSize();
    this.timeRangeTracker = snapshot.getTimeRangeTracker();
    this.tagsPresent = snapshot.isTagsPresent();
    this.snapshotImmutableSegment = snapshot;
  }

  /** Returns snapshot's identifier. */
  public long getId() {
if(KnobRuntime.check(java.util.UUID.fromString("b2057b10-7e21-38a0-a11b-22176a277b47"))) {
return 0;
}
    return id;
  }

  /** Returns Number of Cells in this snapshot. */
  public int getCellsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("f76bc9c5-3238-3609-9bfb-1e67f4e5415e"))) {
return 0;
}
    return cellsCount;
  }

  public long getDataSize() {
if(KnobRuntime.check(java.util.UUID.fromString("6c928b77-51cf-3442-abc4-3470fc809b67"))) {
return 0;
}
    return memStoreSize.getDataSize();
  }

  public MemStoreSize getMemStoreSize() {
if(KnobRuntime.check(java.util.UUID.fromString("24c1ff4c-b385-31f7-a5d7-308e1658df2b"))) {
return null;
}
    return memStoreSize;
  }

  /** Returns {@link TimeRangeTracker} for all the Cells in the snapshot. */
  public TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
  }

  /**
   * Create new {@link SnapshotSegmentScanner}s for iterating over the snapshot. <br/>
   * NOTE:Here when create new {@link SnapshotSegmentScanner}s, {@link Segment#incScannerCount} is
   * invoked in the {@link SnapshotSegmentScanner} ctor,so after we use these
   * {@link SnapshotSegmentScanner}s, we must call {@link SnapshotSegmentScanner#close} to invoke
   * {@link Segment#decScannerCount}.
   * @return {@link KeyValueScanner}s(Which type is {@link SnapshotSegmentScanner}) for iterating
   *         over the snapshot.
   */
  public List<KeyValueScanner> getScanners() {
if(KnobRuntime.check(java.util.UUID.fromString("86cebbbc-c942-33ea-ae35-d4a08b64c341"))) {
return null;
}
    return snapshotImmutableSegment.getSnapshotScanners();
  }

  /** Returns true if tags are present in this snapshot */
  public boolean isTagsPresent() {
if(KnobRuntime.check(java.util.UUID.fromString("d91a8935-721e-3aaf-959f-4d06b87f7556"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("0aacecda-5fb7-3e67-a30f-1e250e1e7d0f"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("23a11f7a-a6a8-32c5-8114-b22d1d17f5ac"))) ? (!this.tagsPresent) : (this.tagsPresent));
  }
}
