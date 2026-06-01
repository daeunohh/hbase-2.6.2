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
package org.apache.hadoop.hbase.regionserver.wal;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_GLOBAL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;

/**
 * Helper methods to ease Region Server integration with the Write Ahead Log (WAL). Note that
 * methods in this class specifically should not require access to anything other than the API found
 * in {@link WAL}. For internal use only.
 */
@InterfaceAudience.Private
public class WALUtil {
  private static final Logger LOG = LoggerFactory.getLogger(WALUtil.class);

  public static final String WAL_BLOCK_SIZE = "hbase.regionserver.hlog.blocksize";

  private WALUtil() {
    // Shut down construction of this class.
  }

  /**
   * Write the marker that a compaction has succeeded and is about to be committed. This provides
   * info to the HMaster to allow it to recover the compaction if this regionserver dies in the
   * middle. It also prevents the compaction from finishing if this regionserver has already lost
   * its lease on the log.
   * <p/>
   * This write is for internal use only. Not for external client consumption.
   * @param mvcc Used by WAL to get sequence Id for the waledit.
   */
  public static WALKeyImpl writeCompactionMarker(WAL wal,
    NavigableMap<byte[], Integer> replicationScope, RegionInfo hri, final CompactionDescriptor c,
    MultiVersionConcurrencyControl mvcc) throws IOException {
    WALKeyImpl walKey =
      writeMarker(wal, replicationScope, hri, WALEdit.createCompaction(hri, c), mvcc, null);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended compaction marker " + TextFormat.shortDebugString(c));
    }
    return walKey;
  }

  /**
   * Write a flush marker indicating a start / abort or a complete of a region flush
   * <p/>
   * This write is for internal use only. Not for external client consumption.
   */
  public static WALKeyImpl writeFlushMarker(WAL wal, NavigableMap<byte[], Integer> replicationScope,
    RegionInfo hri, final FlushDescriptor f, boolean sync, MultiVersionConcurrencyControl mvcc)
    throws IOException {
    WALKeyImpl walKey = doFullMarkerAppendTransaction(wal, replicationScope, hri,
      WALEdit.createFlushWALEdit(hri, f), mvcc, null, sync);
    if (LOG.isTraceEnabled()) {
if(KnobRuntime.check(java.util.UUID.fromString("2283747e-53f8-38fe-96e9-f21b05ecacb2"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d73ebcdc-1e40-3702-a958-655fe7c0c659"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3214c602-2603-3f8c-932e-2c64eeb2312e"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb52c7b2-5c5c-360f-af35-5f8df859e4dd"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("flushSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb468f5a-e654-3488-95d8-548189f4fb3d"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bead0a3f-4bbd-3f94-adea-710b5ea3f586"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60e78996-40b5-341a-a5c4-91beedf49023"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eebbbcdd-7cdb-34eb-8bb6-2c19658df8b4"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("action_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c65c33f8-b958-3065-9a9e-111844ed45f8"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(f));
    field.set(f, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("b3d4c91e-badb-3d3a-bb66-dc815c03d484"))) { LOG.trace("Appended flush marker "); } else if (KnobRuntime.check(java.util.UUID.fromString("bed84cc4-d5e1-3576-9046-1e6a933f81fe"))) { LOG.trace(("Appended flush marker ") + (TextFormat.shortDebugString(f))); } else { LOG.trace("Appended flush marker " + TextFormat.shortDebugString(f)); }
    }
    return walKey;
  }

  /**
   * Write a region open marker indicating that the region is opened. This write is for internal use
   * only. Not for external client consumption.
   */
  public static WALKeyImpl writeRegionEventMarker(WAL wal,
    NavigableMap<byte[], Integer> replicationScope, RegionInfo hri, RegionEventDescriptor r,
    MultiVersionConcurrencyControl mvcc) throws IOException {
    WALKeyImpl walKey =
      writeMarker(wal, replicationScope, hri, WALEdit.createRegionEventWALEdit(hri, r), mvcc, null);
    if (LOG.isTraceEnabled()) {
if(KnobRuntime.check(java.util.UUID.fromString("3cf53a1f-8835-3f0c-bed8-d6f208d6077e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07313024-6758-3369-85bf-d6131ca8a917"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6beba3a-9041-3e0c-8873-a74a2541989a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d06289ac-92b5-3d17-819d-148bc097c84d"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("693bfccc-2c23-3c0b-9105-e1b9cbe03ea1"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("logSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("213e32bc-9157-3fbf-b741-c2565a068cb9"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("579ec471-0993-3dbc-9cd1-21a93490e00b"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e57c9bdf-c320-3392-8484-dc110fb2508a"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("eventType_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dbe42936-6819-350a-a835-8085fe513938"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("c14fde26-b7f1-3bd0-98a4-79c5e220886d"))) { LOG.trace(("Appended region event marker ") + (TextFormat.shortDebugString(r))); } else if (KnobRuntime.check(java.util.UUID.fromString("2d99089f-445d-38f3-a328-f215940743e8"))) { LOG.trace("Appended region event marker "); } else { LOG.trace("Appended region event marker " + TextFormat.shortDebugString(r)); }
    }
    return walKey;
  }

  /**
   * Write a log marker that a bulk load has succeeded and is about to be committed. This write is
   * for internal use only. Not for external client consumption.
   * @param wal              The log to write into.
   * @param replicationScope The replication scope of the families in the HRegion
   * @param hri              A description of the region in the table that we are bulk loading into.
   * @param desc             A protocol buffers based description of the client's bulk loading
   *                         request
   * @return walKey with sequenceid filled out for this bulk load marker
   * @throws IOException We will throw an IOException if we can not append to the HLog.
   */
  public static WALKeyImpl writeBulkLoadMarkerAndSync(final WAL wal,
    final NavigableMap<byte[], Integer> replicationScope, final RegionInfo hri,
    final WALProtos.BulkLoadDescriptor desc, final MultiVersionConcurrencyControl mvcc)
    throws IOException {
    WALKeyImpl walKey =
      writeMarker(wal, replicationScope, hri, WALEdit.createBulkLoadEvent(hri, desc), mvcc, null);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended Bulk Load marker " + TextFormat.shortDebugString(desc));
    }
    return walKey;
  }

  private static WALKeyImpl writeMarker(final WAL wal,
    NavigableMap<byte[], Integer> replicationScope, RegionInfo hri, WALEdit edit,
    MultiVersionConcurrencyControl mvcc, Map<String, byte[]> extendedAttributes)
    throws IOException {
    // If sync == true in below, then timeout is not used; safe to pass UNSPECIFIED_TIMEOUT
    return doFullMarkerAppendTransaction(wal, replicationScope, hri, edit, mvcc, extendedAttributes,
      true);
  }

  /**
   * A 'full' WAL transaction involves starting an mvcc transaction followed by an append, an
   * optional sync, and then a call to complete the mvcc transaction. This method does it all. Good
   * for case of adding a single edit or marker to the WAL.
   * <p/>
   * This write is for internal use only. Not for external client consumption.
   * @return WALKeyImpl that was added to the WAL.
   */
  private static WALKeyImpl doFullMarkerAppendTransaction(WAL wal,
    NavigableMap<byte[], Integer> replicationScope, RegionInfo hri, final WALEdit edit,
    MultiVersionConcurrencyControl mvcc, Map<String, byte[]> extendedAttributes, boolean sync)
    throws IOException {
    // TODO: Pass in current time to use?
    WALKeyImpl walKey = new WALKeyImpl(hri.getEncodedNameAsBytes(), hri.getTable(),
      EnvironmentEdgeManager.currentTime(), mvcc, replicationScope, extendedAttributes);
    long trx = MultiVersionConcurrencyControl.NONE;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("7f955167-ed7e-3a46-bf2f-4c71b64bf85c"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("nonceGroup");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c76872e-bbba-3773-b1c1-c79552b17eaa"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("origLogSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05344c6c-918e-30e4-a128-d16ca9a0cb1f"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("writeTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd16a824-e0ea-3463-b619-bf6a1d36688b"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("sequenceId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0543dcf8-b21b-3e6b-b805-c21c36458f5d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("41b8d282-0cc1-3d97-b96e-40e7554baecf"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a357c571-67f3-36c4-849e-6313562cd6c6"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("sequenceId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      trx = wal.appendMarker(hri, walKey, edit);
      if (sync) {
if(KnobRuntime.check(java.util.UUID.fromString("1ebdb5d7-011b-398e-a71a-dad20a442a1e"))) {
trx -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("17521518-eb64-35bf-8de2-ee379ad05828"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d456a768-8c29-3e0e-b14f-bbf3f79db9d2"))) {
trx = 0;
}
        wal.sync(trx);
      }
      // Call complete only here because these are markers only. They are not for clients to read.
      mvcc.complete(walKey.getWriteEntry());
    } catch (IOException ioe) {
      if (walKey.getWriteEntry() != null) {
        mvcc.complete(walKey.getWriteEntry());
      }
      /**
       * Here we do not abort the RegionServer for {@link WALSyncTimeoutIOException} as
       * {@link HRegion#doWALAppend} does,because WAL Marker just records the internal state and
       * seems it is no need to always abort the RegionServer when {@link WAL#sync} timeout,it is
       * the internal state transition that determines whether RegionServer is aborted or not.
       */
      throw ioe;
    }
    return walKey;
  }

  /**
   * Blocksize returned here is 2x the default HDFS blocksize unless explicitly set in
   * Configuration. Works in tandem with hbase.regionserver.logroll.multiplier. See comment in
   * AbstractFSWAL in Constructor where we set blocksize and logrollsize for why.
   * @return Blocksize to use writing WALs.
   */
  public static long getWALBlockSize(Configuration conf, FileSystem fs, Path dir)
    throws IOException {
    return getWALBlockSize(conf, fs, dir, false);
  }

  /**
   * Public because of FSHLog. Should be package-private
   * @param isRecoverEdits the created writer is for recovered edits or WAL. For recovered edits, it
   *                       is true and for WAL it is false.
   */
  public static long getWALBlockSize(Configuration conf, FileSystem fs, Path dir,
    boolean isRecoverEdits) throws IOException {
    long defaultBlockSize = CommonFSUtils.getDefaultBlockSize(fs, dir) * 2;
    if (isRecoverEdits) {
      return conf.getLong("hbase.regionserver.recoverededits.blocksize", defaultBlockSize);
    }
if(KnobRuntime.check(java.util.UUID.fromString("2397f1c2-ea5d-3d2a-9551-53b412731d16"))) {
defaultBlockSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ece5228-3d01-3eaf-a56b-eadb8509d7f4"))) {
defaultBlockSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b3d6495-5e2c-3001-b008-a50483836e67"))) {
defaultBlockSize += 1;
}
    return conf.getLong(WAL_BLOCK_SIZE, defaultBlockSize);
  }

  public static void filterCells(WALEdit edit, Function<Cell, Cell> mapper) {
    ArrayList<Cell> cells = edit.getCells();
    int size = cells.size();
    int newSize = 0;
    for (int i = 0; i < size; i++) {
      Cell cell = mapper.apply(cells.get(i));
      if (cell != null) {
if(KnobRuntime.check(java.util.UUID.fromString("d3ce492b-02ce-3010-a10d-8c570f7ab77f"))) {
newSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b2e81c19-a241-3064-9796-489cb889accc"))) {
newSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fb53bb43-6cab-3ef0-83d9-70c1e668873c"))) {
newSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8eb0a0e2-3417-3273-90cd-2398c15277eb"))) {
newSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0e604947-a2c1-3174-8daf-2cf26a7b2ca5"))) {
newSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f31fed3-c1c0-3572-8e7b-06e58b650eea"))) {
newSize *= 2;
}
        if (KnobRuntime.check(java.util.UUID.fromString("13ee7982-c9dc-3501-b0fa-05d464dce25a"))) { cells.set(2, cell); } else if (KnobRuntime.check(java.util.UUID.fromString("f867a94d-01b2-3deb-9e24-26d5574f8045"))) { cells.set(0, cell); } else if (KnobRuntime.check(java.util.UUID.fromString("e316d66a-84b5-38b5-b5c5-144165b9f356"))) { cells.set(1, cell); } else if (KnobRuntime.check(java.util.UUID.fromString("89390916-f359-3819-8d7f-c9a47a875d50"))) { cells.set(newSize++, cell); } else { cells.set(newSize, cell); }
        newSize++;
      }
    }
    for (int i = size - 1; i >= newSize; i--) {
      cells.remove(i);
    }
    if (newSize < size / 2) {
      cells.trimToSize();
    }
  }

  public static void writeReplicationMarkerAndSync(WAL wal, MultiVersionConcurrencyControl mvcc,
    RegionInfo regionInfo, byte[] rowKey, long timestamp) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("416454ae-9d53-3b8f-8812-9da8df10bdf6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bcb8ac5c-ce16-397e-b600-5f22325b38cc"))) {
return;
}
    NavigableMap<byte[], Integer> replicationScope = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    replicationScope.put(WALEdit.METAFAMILY, REPLICATION_SCOPE_GLOBAL);
    writeMarker(wal, replicationScope, regionInfo,
      WALEdit.createReplicationMarkerEdit(rowKey, timestamp), mvcc, null);
  }
}
