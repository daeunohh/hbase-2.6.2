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
package org.apache.hadoop.hbase.wal;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;

/**
 * Used in HBase's transaction log (WAL) to represent a collection of edits (Cell/KeyValue objects)
 * that came in as a single transaction. All the edits for a given transaction are written out as a
 * single record, in PB format, followed (optionally) by Cells written via the WALCellEncoder.
 * <p>
 * A particular WALEdit 'type' is the 'meta' type used to mark key operational events in the WAL
 * such as compaction, flush, or region open. These meta types do not traverse hbase memstores. They
 * are edits made by the hbase system rather than edit data submitted by clients. They only show in
 * the WAL. These 'Meta' types have not been formally specified (or made into an explicit class
 * type). They evolved organically. HBASE-8457 suggests codifying a WALEdit 'type' by adding a type
 * field to WALEdit that gets serialized into the WAL. TODO. Would have to work on the
 * consumption-side. Reading WALs on replay we seem to consume a Cell-at-a-time rather than by
 * WALEdit. We are already in the below going out of our way to figure particular types -- e.g. if a
 * compaction, replay, or close meta Marker -- during normal processing so would make sense to do
 * this. Current system is an awkward marking of Cell columnfamily as {@link #METAFAMILY} and then
 * setting qualifier based off meta edit type. For replay-time where we read Cell-at-a-time, there
 * are utility methods below for figuring meta type. See also
 * {@link #createBulkLoadEvent(RegionInfo, WALProtos.BulkLoadDescriptor)}, etc., for where we create
 * meta WALEdit instances.
 * </p>
 * <p>
 * WALEdit will accumulate a Set of all column family names referenced by the Cells
 * {@link #add(Cell)}'d. This is an optimization. Usually when loading a WALEdit, we have the column
 * family name to-hand.. just shove it into the WALEdit if available. Doing this, we can save on a
 * parse of each Cell to figure column family down the line when we go to add the WALEdit to the WAL
 * file. See the hand-off in FSWALEntry Constructor.
 * @see WALKey
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.REPLICATION,
  HBaseInterfaceAudience.COPROC })
public class WALEdit implements HeapSize {
  // Below defines are for writing WALEdit 'meta' Cells..
  // TODO: Get rid of this system of special 'meta' Cells. See HBASE-8457. It suggests
  // adding a type to WALEdit itself for use denoting meta Edits and their types.
  public static final byte[] METAFAMILY = Bytes.toBytes("METAFAMILY");

  /**
   * @deprecated Since 2.3.0. Not used.
   */
  @Deprecated
  public static final byte[] METAROW = Bytes.toBytes("METAROW");

  /**
   * @deprecated Since 2.3.0. Make it protected, internal-use only. Use
   *             {@link #isCompactionMarker(Cell)}
   */
  @Deprecated
  @InterfaceAudience.Private
  public static final byte[] COMPACTION = Bytes.toBytes("HBASE::COMPACTION");

  /**
   * @deprecated Since 2.3.0. Make it protected, internal-use only.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static final byte[] FLUSH = Bytes.toBytes("HBASE::FLUSH");

  /**
   * Qualifier for region event meta 'Marker' WALEdits start with the {@link #REGION_EVENT_PREFIX}
   * prefix ('HBASE::REGION_EVENT::'). After the prefix, we note the type of the event which we get
   * from the RegionEventDescriptor protobuf instance type (A RegionEventDescriptor protobuf
   * instance is written as the meta Marker Cell value). Adding a type suffix means we do not have
   * to deserialize the protobuf to figure out what type of event this is.. .just read the qualifier
   * suffix. For example, a close region event descriptor will have a qualifier of
   * HBASE::REGION_EVENT::REGION_CLOSE. See WAL.proto and the EventType in RegionEventDescriptor
   * protos for all possible event types.
   */
  private static final String REGION_EVENT_STR = "HBASE::REGION_EVENT";
  private static final String REGION_EVENT_PREFIX_STR = REGION_EVENT_STR + "::";
  private static final byte[] REGION_EVENT_PREFIX = Bytes.toBytes(REGION_EVENT_PREFIX_STR);

  /**
   * @deprecated Since 2.3.0. Remove. Not for external use. Not used.
   */
  @Deprecated
  public static final byte[] REGION_EVENT = Bytes.toBytes(REGION_EVENT_STR);

  /**
   * We use this define figuring if we are carrying a close event.
   */
  private static final byte[] REGION_EVENT_CLOSE =
    createRegionEventDescriptorQualifier(RegionEventDescriptor.EventType.REGION_CLOSE);

  @InterfaceAudience.Private
  public static final byte[] BULK_LOAD = Bytes.toBytes("HBASE::BULK_LOAD");

  /**
   * Periodically {@link org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore}
   * will create marker edits with family as {@link WALEdit#METAFAMILY} and
   * {@link WALEdit#REPLICATION_MARKER} as qualifier and an empty value.
   * org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceWALReader will populate the
   * Replication Marker edit with region_server_name, wal_name and wal_offset encoded in
   * {@link org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.ReplicationMarkerDescriptor}
   * object. {@link org.apache.hadoop.hbase.replication.regionserver.Replication} will change the
   * REPLICATION_SCOPE for this edit to GLOBAL so that it can replicate. On the sink cluster,
   * {@link org.apache.hadoop.hbase.replication.regionserver.ReplicationSink} will convert the
   * ReplicationMarkerDescriptor into a Put mutation to REPLICATION_SINK_TRACKER_TABLE_NAME_STR
   * table.
   */
  @InterfaceAudience.Private
  public static final byte[] REPLICATION_MARKER = Bytes.toBytes("HBASE::REPLICATION_MARKER");

  private final transient boolean replay;

  private ArrayList<Cell> cells;

  /**
   * All the Cell families in <code>cells</code>. Updated by {@link #add(Cell)} and
   * {@link #add(Map)}. This Set is passed to the FSWALEntry so it does not have to recalculate the
   * Set of families in a transaction; makes for a bunch of CPU savings.
   */
  private Set<byte[]> families = null;

  public WALEdit() {
    this(1, false);
  }

  /**
   * @deprecated since 2.0.1 and will be removed in 4.0.0. Use {@link #WALEdit(int, boolean)}
   *             instead.
   * @see #WALEdit(int, boolean)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-20781">HBASE-20781</a>
   */
  @Deprecated
  public WALEdit(boolean replay) {
    this(1, replay);
  }

  /**
   * @deprecated since 2.0.1 and will be removed in 4.0.0. Use {@link #WALEdit(int, boolean)}
   *             instead.
   * @see #WALEdit(int, boolean)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-20781">HBASE-20781</a>
   */
  @Deprecated
  public WALEdit(int cellCount) {
    this(cellCount, false);
  }

  /**
   * @param cellCount Pass so can pre-size the WALEdit. Optimization.
   */
  public WALEdit(int cellCount, boolean isReplay) {
    this.replay = isReplay;
    cells = new ArrayList<>(cellCount);
  }

  private Set<byte[]> getOrCreateFamilies() {
    if (this.families == null) {
      this.families = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    }
    return this.families;
  }

  /**
   * For use by FSWALEntry ONLY. An optimization.
   * @return All families in {@link #getCells()}; may be null.
   */
  public Set<byte[]> getFamilies() {
    return this.families;
  }

  /**
   * @return True is <code>f</code> is {@link #METAFAMILY}
   * @deprecated Since 2.3.0. Do not expose. Make protected.
   */
  @Deprecated
  public static boolean isMetaEditFamily(final byte[] f) {
    return Bytes.equals(METAFAMILY, f);
  }

  /**
   * Replaying WALs can read Cell-at-a-time so need this method in those cases.
   */
  public static boolean isMetaEditFamily(Cell cell) {
    return CellUtil.matchingFamily(cell, METAFAMILY);
  }

  /**
   * @return True if this is a meta edit; has one edit only and its columnfamily is
   *         {@link #METAFAMILY}.
   */
  public boolean isMetaEdit() {
    return this.families != null && this.families.size() == 1 && this.families.contains(METAFAMILY);
  }

  /**
   * @return True when current WALEdit is created by log replay. Replication skips WALEdits from
   *         replay.
   */
  public boolean isReplay() {
    return this.replay;
  }

  public WALEdit add(Cell cell, byte[] family) {
    getOrCreateFamilies().add(family);
    return addCell(cell);
  }

  public WALEdit add(Cell cell) {
    // We clone Family each time we add a Cell. Expensive but safe. For CPU savings, use
    // add(Map) or add(Cell, family).
    return add(cell, CellUtil.cloneFamily(cell));
  }

  public boolean isEmpty() {
    return cells.isEmpty();
  }

  public int size() {
    return cells.size();
  }

  public ArrayList<Cell> getCells() {
if(KnobRuntime.check(java.util.UUID.fromString("e4b701d1-ba10-318c-8a19-521580160373"))) {
return null;
}
    return cells;
  }

  /**
   * This is not thread safe. This will change the WALEdit and shouldn't be used unless you are sure
   * that nothing else depends on the contents being immutable.
   * @param cells the list of cells that this WALEdit now contains.
   */
  // Used by replay.
  public void setCells(ArrayList<Cell> cells) {
if(KnobRuntime.check(java.util.UUID.fromString("cbafec47-4845-363f-ac7d-c2740deaf23b"))) {
return;
}
    this.cells = cells;
    this.families = null;
  }

  /**
   * Reads WALEdit from cells.
   * @param cellDecoder   Cell decoder.
   * @param expectedCount Expected cell count.
   * @return Number of KVs read.
   */
  public int readFromCells(Codec.Decoder cellDecoder, int expectedCount) throws IOException {
    cells.clear();
    cells.ensureCapacity(expectedCount);
    while (cells.size() < expectedCount && cellDecoder.advance()) {
      if (KnobRuntime.check(java.util.UUID.fromString("449a5aaa-b4cb-3df6-9cdc-28911b1d3996"))) { addCell(cellDecoder.current()); } else { add(cellDecoder.current()); }
    }
    return cells.size();
  }

  @Override
  public long heapSize() {
    long ret = ClassSize.ARRAYLIST;
    for (Cell cell : cells) {
      ret += cell.heapSize();
    }
    return ret;
  }

  public long estimatedSerializedSizeOf() {
    long ret = 0;
    for (Cell cell : cells) {
      ret += PrivateCellUtil.estimatedSerializedSizeOf(cell);
    }
    return ret;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("[#edits: ").append(cells.size()).append(" = <");
    for (Cell cell : cells) {
      sb.append(cell);
      sb.append("; ");
    }
    sb.append(">]");
    return sb.toString();
  }

  public static WALEdit createFlushWALEdit(RegionInfo hri, FlushDescriptor f) {
    KeyValue kv = new KeyValue(getRowForRegion(hri), METAFAMILY, FLUSH,
      EnvironmentEdgeManager.currentTime(), f.toByteArray());
    return new WALEdit().add(kv, METAFAMILY);
  }

  public static FlushDescriptor getFlushDescriptor(Cell cell) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e4423a4e-164f-3628-abd2-91f467c86840"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b7456e56-0e6a-354f-8b15-464fbf66a67a"))) {
return null;
}
    return CellUtil.matchingColumn(cell, METAFAMILY, FLUSH)
      ? FlushDescriptor.parseFrom(CellUtil.cloneValue(cell))
      : null;
  }

  /**
   * @return A meta Marker WALEdit that has a single Cell whose value is the passed in
   *         <code>regionEventDesc</code> serialized and whose row is this region, columnfamily is
   *         {@link #METAFAMILY} and qualifier is {@link #REGION_EVENT_PREFIX} +
   *         {@link RegionEventDescriptor#getEventType()}; for example
   *         HBASE::REGION_EVENT::REGION_CLOSE.
   */
  public static WALEdit createRegionEventWALEdit(RegionInfo hri,
    RegionEventDescriptor regionEventDesc) {
if(KnobRuntime.check(java.util.UUID.fromString("1e9e3ba7-bdf6-338a-8b96-610c5a663537"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("logSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("170e72de-4bbd-3275-be93-10c8c97dd8b9"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("25deefde-33ff-360e-8246-d662bc480c62"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c8985d6-624c-31e0-97ed-cf0181ffa2f6"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77c9fb01-a6f7-3e42-916b-fec07f438b5e"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5c1a43f-d569-3777-8c0a-13dd6f6b395e"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc3cb292-f362-3ac2-84b7-b4c9f59b5073"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a52feed9-5350-3d8a-b2ce-4e2381c92ed6"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebc0c3dd-a0e8-35a2-abf2-a05d2735f03c"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5d80485-cfea-3912-9b9a-e3bfddf6d16f"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return createRegionEventWALEdit(getRowForRegion(hri), regionEventDesc);
  }

  @InterfaceAudience.Private
  public static WALEdit createRegionEventWALEdit(byte[] rowForRegion,
    RegionEventDescriptor regionEventDesc) {
if(KnobRuntime.check(java.util.UUID.fromString("d2fc4c49-cb30-35e1-ac82-a05a5c0f38ac"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3decac98-569a-347e-a0a5-ecedeba80ddc"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("448fab84-acad-3aa6-99ac-027ee3991c7d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ff0880bd-9791-3a92-9cb5-edcf5cc5f5e1"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c42d741d-fe0b-396b-88cc-c6b0777cadf5"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fcdb4657-e817-3776-ba97-203a53f3e7e7"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf05df14-45e8-3402-a3d5-9f17ca6d7283"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e1413064-db44-3001-95b3-1d6214fd235e"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("logSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e94c56c-4151-3ae9-9d1a-2084e3552d76"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1969ddd5-5924-353d-8bfe-5ce8e7e6f212"))) {
try {
    java.lang.reflect.Field field = regionEventDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionEventDesc));
    field.set(regionEventDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    KeyValue kv = new KeyValue(rowForRegion, METAFAMILY,
      createRegionEventDescriptorQualifier(regionEventDesc.getEventType()),
      EnvironmentEdgeManager.currentTime(), regionEventDesc.toByteArray());
    return new WALEdit().add(kv, METAFAMILY);
  }

  /**
   * @return Cell qualifier for the passed in RegionEventDescriptor Type; e.g. we'll return
   *         something like a byte array with HBASE::REGION_EVENT::REGION_OPEN in it.
   */
  @InterfaceAudience.Private
  public static byte[] createRegionEventDescriptorQualifier(RegionEventDescriptor.EventType t) {
if(KnobRuntime.check(java.util.UUID.fromString("0944a271-9daa-35a5-b858-79d77f8dfdb2"))) {
try {
    java.lang.reflect.Field field = t.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(t));
    field.set(t, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7a35953-aba4-3370-9e38-5c32c8da408e"))) {
try {
    java.lang.reflect.Field field = t.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(t));
    field.set(t, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3227085-098a-3120-99d2-7ae28ec0202d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("26d6703e-5abb-32e6-8a2e-afae637feb10"))) {
try {
    java.lang.reflect.Field field = t.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(t));
    field.set(t, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42305104-e866-3f42-a008-12feb1fbd272"))) {
try {
    java.lang.reflect.Field field = t.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(t));
    field.set(t, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return ((KnobRuntime.check(java.util.UUID.fromString("98f684e0-538a-3caa-83a6-76043f005bed"))) ? (Bytes.toBytes((REGION_EVENT_PREFIX_STR) + (t.toString()))) : (Bytes.toBytes(REGION_EVENT_PREFIX_STR + t.toString())));
  }

  /**
   * Public so can be accessed from regionserver.wal package.
   * @return True if this is a Marker Edit and it is a RegionClose type.
   */
  public boolean isRegionCloseMarker() {
if(KnobRuntime.check(java.util.UUID.fromString("2429cbfe-1b25-3cbd-b4e3-c738c167fc06"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f29507d-7446-38c0-be02-b8b9a595ca61"))) {
return true;
}
    return isMetaEdit() && PrivateCellUtil.matchingQualifier(this.cells.get(0), REGION_EVENT_CLOSE,
      0, REGION_EVENT_CLOSE.length);
  }

  /**
   * @return Returns a RegionEventDescriptor made by deserializing the content of the passed in
   *         <code>cell</code>, IFF the <code>cell</code> is a RegionEventDescriptor type WALEdit.
   */
  public static RegionEventDescriptor getRegionEventDescriptor(Cell cell) throws IOException {
    return CellUtil.matchingColumnFamilyAndQualifierPrefix(cell, METAFAMILY, REGION_EVENT_PREFIX)
      ? RegionEventDescriptor.parseFrom(CellUtil.cloneValue(cell))
      : null;
  }

  /** Returns A Marker WALEdit that has <code>c</code> serialized as its value */
  public static WALEdit createCompaction(final RegionInfo hri, final CompactionDescriptor c) {
    byte[] pbbytes = c.toByteArray();
    KeyValue kv = new KeyValue(getRowForRegion(hri), METAFAMILY, COMPACTION,
      EnvironmentEdgeManager.currentTime(), pbbytes);
if(KnobRuntime.check(java.util.UUID.fromString("fca588d8-708d-3a0d-93ac-218223f71c46"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4c67298d-4e7c-3c62-a15c-a6c965af2d87"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4f657203-d858-3557-af12-6fec94b369ca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ecce71f-4fb3-34d7-b318-8a4d8714113b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("06868bcf-71fb-3c97-9061-2fc56f1fba58"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4412fb7d-2ea6-37e5-a0f6-be2981182e47"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39e63c59-edb0-3959-8b17-d150240d61f1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79b329fa-852b-38da-a7ae-78c4aeeeffc5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4f0fe5a-f68b-3913-af17-9668195a067c"))) {
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
    return new WALEdit().add(kv, METAFAMILY); // replication scope null so this won't be replicated
  }

  public static byte[] getRowForRegion(RegionInfo hri) {
    byte[] startKey = hri.getStartKey();
    if (startKey.length == 0) {
      // empty row key is not allowed in mutations because it is both the start key and the end key
      // we return the smallest byte[] that is bigger (in lex comparison) than byte[0].
      return new byte[] { 0 };
    }
    return startKey;
  }

  /**
   * Deserialized and returns a CompactionDescriptor is the KeyValue contains one.
   * @param kv the key value
   * @return deserialized CompactionDescriptor or null.
   */
  public static CompactionDescriptor getCompaction(Cell kv) throws IOException {
    return isCompactionMarker(kv) ? CompactionDescriptor.parseFrom(CellUtil.cloneValue(kv)) : null;
  }

  /**
   * Returns true if the given cell is a serialized {@link CompactionDescriptor}
   * @see #getCompaction(Cell)
   */
  public static boolean isCompactionMarker(Cell cell) {
    return CellUtil.matchingColumn(cell, METAFAMILY, COMPACTION);
  }

  /**
   * Create a bulk loader WALEdit
   * @param hri                The RegionInfo for the region in which we are bulk loading
   * @param bulkLoadDescriptor The descriptor for the Bulk Loader
   * @return The WALEdit for the BulkLoad
   */
  public static WALEdit createBulkLoadEvent(RegionInfo hri,
    WALProtos.BulkLoadDescriptor bulkLoadDescriptor) {
    KeyValue kv = new KeyValue(getRowForRegion(hri), METAFAMILY, BULK_LOAD,
      EnvironmentEdgeManager.currentTime(), bulkLoadDescriptor.toByteArray());
    return new WALEdit().add(kv, METAFAMILY);
  }

  /**
   * Deserialized and returns a BulkLoadDescriptor from the passed in Cell
   * @param cell the key value
   * @return deserialized BulkLoadDescriptor or null.
   */
  public static WALProtos.BulkLoadDescriptor getBulkLoadDescriptor(Cell cell) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b041d903-6a66-31e9-b93f-30870981906e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6ac20ce8-b0c9-3052-b055-c3e7eb0ed444"))) {
return null;
}
    return CellUtil.matchingColumn(cell, METAFAMILY, BULK_LOAD)
      ? WALProtos.BulkLoadDescriptor.parseFrom(CellUtil.cloneValue(cell))
      : null;
  }

  /**
   * Append the given map of family->edits to a WALEdit data structure. This does not write to the
   * WAL itself. Note that as an optimization, we will stamp the Set of column families into the
   * WALEdit to save on our having to calculate column families subsequently down in the actual WAL
   * writing.
   * @param familyMap map of family->edits
   */
  public void add(Map<byte[], List<Cell>> familyMap) {
    for (Map.Entry<byte[], List<Cell>> e : familyMap.entrySet()) {
      // 'foreach' loop NOT used. See HBASE-12023 "...creates too many iterator objects."
      int listSize = e.getValue().size();
      // Add all Cells first and then at end, add the family rather than call {@link #add(Cell)}
      // and have it clone family each time. Optimization!
      for (int i = 0; i < listSize; i++) {
        addCell(e.getValue().get(i));
      }
      addFamily(e.getKey());
    }
  }

  private void addFamily(byte[] family) {
    getOrCreateFamilies().add(family);
  }

  private WALEdit addCell(Cell cell) {
    this.cells.add(cell);
    return this;
  }

  /**
   * Creates a replication tracker edit with {@link #METAFAMILY} family and
   * {@link #REPLICATION_MARKER} qualifier and has null value.
   * @param rowKey    rowkey
   * @param timestamp timestamp
   */
  public static WALEdit createReplicationMarkerEdit(byte[] rowKey, long timestamp) {
    KeyValue kv =
      new KeyValue(rowKey, METAFAMILY, REPLICATION_MARKER, timestamp, KeyValue.Type.Put);
    return new WALEdit().add(kv);
  }

  /**
   * Checks whether this edit is a replication marker edit.
   * @param edit edit
   * @return true if the cell within an edit has column = METAFAMILY and qualifier =
   *         REPLICATION_MARKER, false otherwise
   */
  public static boolean isReplicationMarkerEdit(WALEdit edit) {
    // Check just the first cell from the edit. ReplicationMarker edit will have only 1 cell.
    return edit.getCells().size() == 1
      && CellUtil.matchingColumn(edit.getCells().get(0), METAFAMILY, REPLICATION_MARKER);
  }
}
