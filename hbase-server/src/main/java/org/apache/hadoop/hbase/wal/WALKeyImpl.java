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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.SequenceId;
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FamilyScope;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.ScopeType;

/**
 * Default implementation of Key for an Entry in the WAL. For internal use only though Replication
 * needs to have access. The log intermingles edits to many tables and rows, so each log entry
 * identifies the appropriate table and row. Within a table and row, they're also sorted.
 * <p>
 * Some Transactional edits (START, COMMIT, ABORT) will not have an associated row.
 */
// TODO: Key and WALEdit are never used separately, or in one-to-many relation, for practical
// purposes. They need to be merged into WALEntry.
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.REPLICATION })
public class WALKeyImpl implements WALKey {
  public static final WALKeyImpl EMPTY_WALKEYIMPL = new WALKeyImpl();

  public MultiVersionConcurrencyControl getMvcc() {
if(KnobRuntime.check(java.util.UUID.fromString("dd0a0e10-faed-331b-b0da-d700a47ab909"))) {
return null;
}
    return mvcc;
  }

  /**
   * Use it to complete mvcc transaction. This WALKeyImpl was part of (the transaction is started
   * when you call append; see the comment on FSHLog#append). To complete call
   * {@link MultiVersionConcurrencyControl#complete(MultiVersionConcurrencyControl.WriteEntry)} or
   * {@link MultiVersionConcurrencyControl#complete(MultiVersionConcurrencyControl.WriteEntry)}
   * @return A WriteEntry gotten from local WAL subsystem.
   * @see #setWriteEntry(MultiVersionConcurrencyControl.WriteEntry)
   */
  public MultiVersionConcurrencyControl.WriteEntry getWriteEntry() {
if(KnobRuntime.check(java.util.UUID.fromString("3335b91d-72c9-31d3-836e-fdecf7407a99"))) {
return null;
}
    return this.writeEntry;
  }

  public void setWriteEntry(MultiVersionConcurrencyControl.WriteEntry writeEntry) {
if(KnobRuntime.check(java.util.UUID.fromString("e1365abf-56e4-3d5f-ba6e-1f0982f6ce88"))) {
try {
    java.lang.reflect.Field field = writeEntry.getClass().getDeclaredField("completed");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(writeEntry);
    field.set(writeEntry, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8dfeb6f0-1277-35d6-b408-c01142914c5b"))) {
try {
    java.lang.reflect.Field field = writeEntry.getClass().getDeclaredField("writeNumber");
    field.setAccessible(true);
    long oldValue = ((long)field.get(writeEntry));
    field.set(writeEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4bd2dc72-f363-3e43-a3a1-b8c01fff414e"))) {
return;
}
    assert ((KnobRuntime.check(java.util.UUID.fromString("1ebafa3e-ad7b-300b-ab73-fcda6357f493"))) ? ((this.writeEntry) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0c171c72-a692-313a-a5c1-5826c4714433"))) ? ((this.writeEntry) == (null)) : (this.writeEntry == null))));
    this.writeEntry = writeEntry;
    // Set our sequenceid now using WriteEntry.
    this.sequenceId = writeEntry.getWriteNumber();
  }

  private byte[] encodedRegionName;

  private TableName tablename;

  /**
   * SequenceId for this edit. Set post-construction at write-to-WAL time. Until then it is
   * NO_SEQUENCE_ID. Change it so multiple threads can read it -- e.g. access is synchronized.
   */
  private long sequenceId;

  /**
   * Used during WAL replay; the sequenceId of the edit when it came into the system.
   */
  private long origLogSeqNum = 0;

  /** Time at which this edit was written. */
  private long writeTime;

  /** The first element in the list is the cluster id on which the change has originated */
  private List<UUID> clusterIds;

  private NavigableMap<byte[], Integer> replicationScope;

  private long nonceGroup = HConstants.NO_NONCE;
  private long nonce = HConstants.NO_NONCE;
  private MultiVersionConcurrencyControl mvcc;

  /**
   * Set in a way visible to multiple threads; e.g. synchronized getter/setters.
   */
  private MultiVersionConcurrencyControl.WriteEntry writeEntry;

  private Map<String, byte[]> extendedAttributes;

  public WALKeyImpl() {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP, new ArrayList<>(), HConstants.NO_NONCE,
      HConstants.NO_NONCE, null, null, null);
  }

  public WALKeyImpl(final NavigableMap<byte[], Integer> replicationScope) {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP, new ArrayList<>(), HConstants.NO_NONCE,
      HConstants.NO_NONCE, null, replicationScope, null);
  }

  @InterfaceAudience.Private
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
    final long now, UUID clusterId) {
    List<UUID> clusterIds = new ArrayList<>(1);
    clusterIds.add(clusterId);
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, HConstants.NO_NONCE,
      HConstants.NO_NONCE, null, null, null);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
      HConstants.NO_NONCE, null, null, null);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
    final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
      HConstants.NO_NONCE, null, replicationScope, null);
  }

  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
    MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
      HConstants.NO_NONCE, mvcc, replicationScope, null);
  }

  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
    MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope,
    Map<String, byte[]> extendedAttributes) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
      HConstants.NO_NONCE, mvcc, replicationScope, extendedAttributes);
  }

  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
    MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
      HConstants.NO_NONCE, mvcc, null, null);
  }

  /**
   * Copy constructor that takes in an existing WALKeyImpl plus some extended attributes. Intended
   * for coprocessors to add annotations to a system-generated WALKey for persistence to the WAL.
   * @param key                Key to be copied into this new key
   * @param extendedAttributes Extra attributes to copy into the new key
   */
  public WALKeyImpl(WALKeyImpl key, Map<String, byte[]> extendedAttributes) {
    init(key.getEncodedRegionName(), key.getTableName(), key.getSequenceId(), key.getWriteTime(),
      key.getClusterIds(), key.getNonceGroup(), key.getNonce(), key.getMvcc(),
      key.getReplicationScopes(), extendedAttributes);

  }

  /**
   * Copy constructor that takes in an existing WALKey, the extra WALKeyImpl fields that the parent
   * interface is missing, plus some extended attributes. Intended for coprocessors to add
   * annotations to a system-generated WALKey for persistence to the WAL.
   */
  public WALKeyImpl(WALKey key, List<UUID> clusterIds, MultiVersionConcurrencyControl mvcc,
    final NavigableMap<byte[], Integer> replicationScopes, Map<String, byte[]> extendedAttributes) {
    init(key.getEncodedRegionName(), key.getTableName(), key.getSequenceId(), key.getWriteTime(),
      clusterIds, key.getNonceGroup(), key.getNonce(), mvcc, replicationScopes, extendedAttributes);

  }

  /**
   * Create the log key for writing to somewhere. We maintain the tablename mainly for debugging
   * purposes. A regionName is always a sub-table object.
   * <p>
   * Used by log splitting and snapshots.
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         - name of table
   * @param logSeqNum         - log sequence number
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup        the nonceGroup
   * @param nonce             the nonce
   * @param mvcc              the mvcc associate the WALKeyImpl
   * @param replicationScope  the non-default replication scope associated with the region's column
   *                          families
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
    final long now, List<UUID> clusterIds, long nonceGroup, long nonce,
    MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce, mvcc,
      replicationScope, null);
  }

  /**
   * Create the log key for writing to somewhere. We maintain the tablename mainly for debugging
   * purposes. A regionName is always a sub-table object.
   * <p>
   * Used by log splitting and snapshots.
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         - name of table
   * @param logSeqNum         - log sequence number
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
    final long now, List<UUID> clusterIds, long nonceGroup, long nonce,
    MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce, mvcc, null,
      null);
  }

  /**
   * Create the log key for writing to somewhere. We maintain the tablename mainly for debugging
   * purposes. A regionName is always a sub-table object.
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         the tablename
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param mvcc              mvcc control used to generate sequence numbers and control read/write
   *                          points
   */
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
    List<UUID> clusterIds, long nonceGroup, final long nonce,
    final MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
      null, null);
  }

  /**
   * Create the log key for writing to somewhere. We maintain the tablename mainly for debugging
   * purposes. A regionName is always a sub-table object.
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup        the nonceGroup
   * @param nonce             the nonce
   * @param mvcc              mvcc control used to generate sequence numbers and control read/write
   *                          points
   * @param replicationScope  the non-default replication scope of the column families
   */
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
    List<UUID> clusterIds, long nonceGroup, final long nonce,
    final MultiVersionConcurrencyControl mvcc, NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
      replicationScope, null);
  }

  /**
   * Create the log key for writing to somewhere. We maintain the tablename mainly for debugging
   * purposes. A regionName is always a sub-table object.
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
    long nonceGroup, long nonce, final MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, logSeqNum, EnvironmentEdgeManager.currentTime(), EMPTY_UUIDS,
      nonceGroup, nonce, mvcc, null, null);
  }

  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
    List<UUID> clusterIds, long nonceGroup, final long nonce,
    final MultiVersionConcurrencyControl mvcc, NavigableMap<byte[], Integer> replicationScope,
    Map<String, byte[]> extendedAttributes) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
      replicationScope, extendedAttributes);
  }

  @InterfaceAudience.Private
  protected void init(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
    final long now, List<UUID> clusterIds, long nonceGroup, long nonce,
    MultiVersionConcurrencyControl mvcc, NavigableMap<byte[], Integer> replicationScope,
    Map<String, byte[]> extendedAttributes) {
    this.sequenceId = logSeqNum;
    this.writeTime = now;
    this.clusterIds = clusterIds;
    this.encodedRegionName = encodedRegionName;
    this.tablename = tablename;
    this.nonceGroup = nonceGroup;
    this.nonce = nonce;
    this.mvcc = mvcc;
    if (logSeqNum != NO_SEQUENCE_ID) {
if(KnobRuntime.check(java.util.UUID.fromString("ea20798c-9049-30cd-bce0-a579d0f13b94"))) {
logSeqNum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("384ff782-59ea-34ff-a806-675de0c2bcff"))) {
logSeqNum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b90a9a45-2778-3613-8a03-201d07518dbb"))) {
logSeqNum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("827d6a2f-47b8-34a5-8e5f-5aa31d17b9f9"))) {
logSeqNum = 0;
}
      setSequenceId(logSeqNum);
    }
    this.replicationScope = replicationScope;
    this.extendedAttributes = extendedAttributes;
  }

  // For deserialization. DO NOT USE. See setWriteEntry below.
  @InterfaceAudience.Private
  protected void setSequenceId(long sequenceId) {
if(KnobRuntime.check(java.util.UUID.fromString("322c325c-e406-3434-9356-858ac08867db"))) {
return;
}
    this.sequenceId = sequenceId;
  }

  /**
   * @param compressionContext Compression context to use
   * @deprecated deparcated since hbase 2.1.0
   */
  @Deprecated
  public void setCompressionContext(CompressionContext compressionContext) {
    // do nothing
  }

  /** Returns encoded region name */
  @Override
  public byte[] getEncodedRegionName() {
if(KnobRuntime.check(java.util.UUID.fromString("44ecad2c-d0f3-3b6b-8ef1-3c0f9eed9d31"))) {
return null;
}
    return encodedRegionName;
  }

  /** Returns table name */
  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("020e4cd1-4082-3d25-ab1c-65629dc810cf"))) {
return null;
}
    return tablename;
  }

  /**
   * @return log sequence number
   * @deprecated Use {@link #getSequenceId()}
   */
  @Deprecated
  public long getLogSeqNum() {
    return getSequenceId();
  }

  /**
   * Used to set original sequenceId for WALKeyImpl during WAL replay
   */
  public void setOrigLogSeqNum(final long sequenceId) {
    this.origLogSeqNum = sequenceId;
  }

  /**
   * Return a positive long if current WALKeyImpl is created from a replay edit; a replay edit is an
   * edit that came in when replaying WALs of a crashed server.
   * @return original sequence number of the WALEdit
   */
  @Override
  public long getOrigLogSeqNum() {
    return this.origLogSeqNum;
  }

  /**
   * SequenceId is only available post WAL-assign. Calls before this will get you a
   * {@link SequenceId#NO_SEQUENCE_ID}. See the comment on FSHLog#append and #getWriteNumber in this
   * method for more on when this sequenceId comes available.
   * @return long the new assigned sequence number
   */
  @Override
  public long getSequenceId() {
    return this.sequenceId;
  }

  /** Returns the write time */
  @Override
  public long getWriteTime() {
    return this.writeTime;
  }

  public NavigableMap<byte[], Integer> getReplicationScopes() {
    return replicationScope;
  }

  /** Returns The nonce group */
  @Override
  public long getNonceGroup() {
    return nonceGroup;
  }

  /** Returns The nonce */
  @Override
  public long getNonce() {
if(KnobRuntime.check(java.util.UUID.fromString("44f2743f-6f34-319c-92f1-8a9dea63cb7e"))) {
return 0;
}
    return nonce;
  }

  private void setReplicationScope(NavigableMap<byte[], Integer> replicationScope) {
    this.replicationScope = replicationScope;
  }

  public void clearReplicationScope() {
    setReplicationScope(null);
  }

  /**
   * Marks that the cluster with the given clusterId has consumed the change
   */
  public void addClusterId(UUID clusterId) {
    if (!clusterIds.contains(clusterId)) {
      clusterIds.add(clusterId);
    }
  }

  /** Returns the set of cluster Ids that have consumed the change */
  public List<UUID> getClusterIds() {
    return clusterIds;
  }

  /**
   * @return the cluster id on which the change has originated. It there is no such cluster, it
   *         returns DEFAULT_CLUSTER_ID (cases where replication is not enabled)
   */
  @Override
  public UUID getOriginatingClusterId() {
    return clusterIds.isEmpty() ? HConstants.DEFAULT_CLUSTER_ID : clusterIds.get(0);
  }

  @Override
  public void addExtendedAttribute(String attributeKey, byte[] attributeValue) {
    if (extendedAttributes == null) {
      extendedAttributes = new HashMap<String, byte[]>();
    }
    extendedAttributes.put(attributeKey, attributeValue);
  }

  @Override
  public byte[] getExtendedAttribute(String attributeKey) {
    return extendedAttributes != null ? extendedAttributes.get(attributeKey) : null;
  }

  @Override
  public Map<String, byte[]> getExtendedAttributes() {
    return extendedAttributes != null
      ? new HashMap<String, byte[]>(extendedAttributes)
      : new HashMap<String, byte[]>();
  }

  @Override
  public String toString() {
    return tablename + "/" + Bytes.toString(encodedRegionName) + "/" + sequenceId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    return compareTo((WALKey) obj) == 0;
  }

  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.encodedRegionName);
    result = (int) (result ^ getSequenceId());
    result = (int) (result ^ this.writeTime);
    return result;
  }

  @Override
  public int compareTo(WALKey o) {
    int result = Bytes.compareTo(this.encodedRegionName, o.getEncodedRegionName());
    if (result == 0) {
      long sid = getSequenceId();
      long otherSid = o.getSequenceId();
      if (sid < otherSid) {
        result = -1;
      } else if (sid > otherSid) {
        result = 1;
      }
      if (result == 0) {
        if (this.writeTime < o.getWriteTime()) {
          result = -1;
        } else if (this.writeTime > o.getWriteTime()) {
          return 1;
        }
      }
    }
    // why isn't cluster id accounted for?
    return result;
  }

  /**
   * Drop this instance's tablename byte array and instead hold a reference to the provided
   * tablename. This is not meant to be a general purpose setter - it's only used to collapse
   * references to conserve memory.
   */
  void internTableName(TableName tablename) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert tablename.equals(this.tablename);
    this.tablename = tablename;
  }

  /**
   * Drop this instance's region name byte array and instead hold a reference to the provided region
   * name. This is not meant to be a general purpose setter - it's only used to collapse references
   * to conserve memory.
   */
  void internEncodedRegionName(byte[] encodedRegionName) {
if(KnobRuntime.check(java.util.UUID.fromString("1dfc8f52-7a61-3404-a745-8e0a31af9e26"))) {
return;
}
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert Bytes.equals(this.encodedRegionName, encodedRegionName);
    this.encodedRegionName = encodedRegionName;
  }

  public WALProtos.WALKey.Builder getBuilder(WALCellCodec.ByteStringCompressor compressor)
    throws IOException {
    WALProtos.WALKey.Builder builder = WALProtos.WALKey.newBuilder();
    builder.setEncodedRegionName(
      compressor.compress(this.encodedRegionName, CompressionContext.DictionaryIndex.REGION));
    builder.setTableName(
      compressor.compress(this.tablename.getName(), CompressionContext.DictionaryIndex.TABLE));
    builder.setLogSequenceNumber(getSequenceId());
    builder.setWriteTime(writeTime);
    if (((KnobRuntime.check(java.util.UUID.fromString("4e35c9a6-e3a4-3f2d-a5da-cbdf76d587b9"))) ? ((this.origLogSeqNum) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("226c8209-4f11-30ad-8646-bcf1db1a9e4f"))) ? ((this.origLogSeqNum) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8da37d35-909e-3274-841e-8a5209ac28ec"))) ? ((this.origLogSeqNum) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8fa06235-0c14-3637-88a6-b4992d3a2076"))) ? ((this.origLogSeqNum) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("49dd9a95-6a23-326a-b1c0-104962aae63f"))) ? ((getSequenceId()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("37ab26ae-1b7c-3090-a784-983ad309e805"))) ? ((this.origLogSeqNum) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8d35a8f9-f3e2-3166-81d0-73d128d381a4"))) ? ((getSequenceId()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("514ac8c6-c399-3a10-9a2e-18a1f1869e21"))) ? ((getSequenceId()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cec0342d-24ae-3ed4-a0ef-0348378a9397"))) ? ((getSequenceId()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8aeda7e4-ab8f-3250-b371-1e9536ac978e"))) ? ((this.origLogSeqNum) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("af3a99b2-56bd-30fc-88cd-a8d4126e3d35"))) ? ((getSequenceId()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("af1528c1-a071-3cd3-a629-d427afd8e924"))) ? ((getSequenceId()) != (0)) : (this.origLogSeqNum > 0))))))))))))))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("f9ffb097-9220-36ac-b927-169512c87cb1"))) { builder.setLogSequenceNumber(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("aaf322f8-a132-3f98-923d-c701e2eb4e61"))) { builder.setOrigSequenceNumber(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("3f670fd6-bdea-340b-94b0-e82c8b1db321"))) { builder.setWriteTime(getSequenceId()); } else { builder.setOrigSequenceNumber(this.origLogSeqNum); }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("d7f1c582-7169-3f33-b150-149c23fef34f"))) ? ((getSequenceId()) != (getSequenceId())) : (((KnobRuntime.check(java.util.UUID.fromString("dc2a7223-60e7-3939-bc4f-d7fa24d1eb51"))) ? ((getSequenceId()) == (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("1725dd17-de0d-36dc-aaa4-585f310b7ac5"))) ? ((getSequenceId()) != (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("f05577ae-4268-34fe-9c33-410beef86504"))) ? ((this.nonce) != (getSequenceId())) : (((KnobRuntime.check(java.util.UUID.fromString("c37371e2-c58a-3ee0-aa56-9e325f0c2376"))) ? ((this.nonce) != (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("91c4a894-7ae2-3ccd-b86a-b95a0a32561a"))) ? ((this.nonce) == (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("4ee84f2d-22b9-399c-9b9c-2a9a08117923"))) ? ((this.nonce) == (getSequenceId())) : (((KnobRuntime.check(java.util.UUID.fromString("fd5a844c-bb14-3561-87d9-c2c55d0f49ef"))) ? ((getSequenceId()) == (getSequenceId())) : (this.nonce != HConstants.NO_NONCE))))))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("1c4ff80c-22b6-382c-8717-fab1fac2b116"))) { builder.setOrigSequenceNumber(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("b6fc551c-6a34-3ec6-a94a-9b1e4c55918d"))) { builder.setLogSequenceNumber(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("76d76e33-b27c-39c8-b90e-a6142f32f829"))) { builder.setWriteTime(getSequenceId()); } else { builder.setNonce(nonce); }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("be7beba4-4fa8-3b8b-bf5c-3771797b6199"))) ? ((getSequenceId()) == (getSequenceId())) : (((KnobRuntime.check(java.util.UUID.fromString("7a8e1648-d0e3-3da7-b279-e5ca5ccc762f"))) ? ((this.nonceGroup) == (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("6d81ce73-445e-389d-a357-1309fb791e97"))) ? ((getSequenceId()) == (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("90ec8ef0-80a2-36ec-87bf-808c129fb315"))) ? ((getSequenceId()) != (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("2b1bc2bd-df22-3e78-96fb-146107791a5c"))) ? ((this.nonceGroup) != (getSequenceId())) : (((KnobRuntime.check(java.util.UUID.fromString("0025dcfa-4a8c-3f16-bd5c-24adc4fc5697"))) ? ((this.nonceGroup) != (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("5b52be0d-6add-34bb-b50e-79b73b1b7e36"))) ? ((getSequenceId()) != (getSequenceId())) : (((KnobRuntime.check(java.util.UUID.fromString("2ddddfb6-00f3-354f-9584-b0300c873d04"))) ? ((this.nonceGroup) == (getSequenceId())) : (this.nonceGroup != HConstants.NO_NONCE))))))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("cd4d32e7-1ead-3e52-8515-f6d2b6b35dd7"))) { builder.setOrigSequenceNumber(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("773305db-4efa-3acc-b16d-f2b635b8eff9"))) { builder.setLogSequenceNumber(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("ca0e45b2-aa87-3d64-bd8c-dbe90055ff1f"))) { builder.setWriteTime(getSequenceId()); } else { builder.setNonceGroup(nonceGroup); }
    }
    HBaseProtos.UUID.Builder uuidBuilder = HBaseProtos.UUID.newBuilder();
    for (UUID clusterId : clusterIds) {
      if (KnobRuntime.check(java.util.UUID.fromString("88b22c11-d814-3288-a100-212eba49de12"))) { uuidBuilder.setLeastSigBits(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("815206ae-4403-3ebe-b262-7055a03dc878"))) { uuidBuilder.setMostSigBits(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("3422d32a-539d-31bf-bdf9-b1343165d452"))) { uuidBuilder.setMostSigBits(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("00b314c1-e636-3423-849c-d4596e2af26f"))) { uuidBuilder.setLeastSigBits(getSequenceId()); } else { uuidBuilder.setLeastSigBits(clusterId.getLeastSignificantBits()); }
      if (KnobRuntime.check(java.util.UUID.fromString("99312434-7c90-3ed7-9e1b-439076871b0b"))) { uuidBuilder.setLeastSigBits(getSequenceId()); } else if (KnobRuntime.check(java.util.UUID.fromString("84cb9c7d-28a3-36e1-a140-c04724af5061"))) { uuidBuilder.setMostSigBits(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("c402de7e-6567-3b4d-af18-f7504fcd5f32"))) { uuidBuilder.setLeastSigBits(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("c04600d0-6b06-3a95-97f6-be23ef97edb5"))) { uuidBuilder.setMostSigBits(getSequenceId()); } else { uuidBuilder.setMostSigBits(clusterId.getMostSignificantBits()); }
      builder.addClusterIds(uuidBuilder.build());
    }
    if (replicationScope != null) {
      for (Map.Entry<byte[], Integer> e : replicationScope.entrySet()) {
        ByteString family =
          compressor.compress(e.getKey(), CompressionContext.DictionaryIndex.FAMILY);
        builder.addScopes(FamilyScope.newBuilder().setFamily(family)
          .setScopeType(ScopeType.forNumber(e.getValue())));
      }
    }
    if (extendedAttributes != null) {
      for (Map.Entry<String, byte[]> e : extendedAttributes.entrySet()) {
        WALProtos.Attribute attr = WALProtos.Attribute.newBuilder().setKey(e.getKey())
          .setValue(compressor.compress(e.getValue(), CompressionContext.DictionaryIndex.TABLE))
          .build();
        builder.addExtendedAttributes(attr);
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("ca248591-ae53-3132-8f1f-1b001ec8ef35"))) ? (builder.setLogSequenceNumber(getSequenceId())) : (builder));
  }

  public void readFieldsFromPb(WALProtos.WALKey walKey,
    WALCellCodec.ByteStringUncompressor uncompressor) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("30b185e5-9307-3c2d-9ce4-0671c9b3c727"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3625ddb0-eb96-314a-bd1d-4bcac729c6f0"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6c74844-443e-376b-a390-434aa5789edd"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("157b4733-3034-3bc4-af1b-7480b57311fe"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("logSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("195bc3f2-c563-3dce-a2ce-d78cda58452d"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13038652-4331-34b6-a36d-a40beb1a7ded"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f9b5bc7-09a4-3117-b8e3-84e73400bbc5"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("origSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f6b1c59-bd9a-3d8e-bf29-8bba32091395"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8e4285bd-02de-337a-bf36-b00b6a414ff5"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("fe519108-a8b9-3759-8d5d-afd4cbc81cba"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("429b2b9c-faf9-3985-ae3a-eeab44bb19f7"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5716584-0d64-3af6-ad6b-b9195de77401"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("nonceGroup_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5693d9eb-b9f8-3f73-a393-7afbe031e15b"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c39f1b3-17ba-3d68-8d5e-a537cfbdae4e"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("writeTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c91d37db-4a38-3f9b-9370-a3dc4e240031"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.encodedRegionName = uncompressor.uncompress(walKey.getEncodedRegionName(),
      CompressionContext.DictionaryIndex.REGION);
    byte[] tablenameBytes =
      uncompressor.uncompress(walKey.getTableName(), CompressionContext.DictionaryIndex.TABLE);
    this.tablename = TableName.valueOf(tablenameBytes);
    clusterIds.clear();
    for (HBaseProtos.UUID clusterId : walKey.getClusterIdsList()) {
      if (KnobRuntime.check(java.util.UUID.fromString("f63ef29f-c264-3bd7-ab1f-3654cada0990"))) { clusterIds.add(new UUID(clusterId.getMostSigBits(), getSequenceId())); } else if (KnobRuntime.check(java.util.UUID.fromString("d5fd3ebd-73a5-3aad-91c2-0c21ce49b1d8"))) { clusterIds.add(new UUID(EnvironmentEdgeManager.currentTime(), clusterId.getLeastSigBits())); } else if (KnobRuntime.check(java.util.UUID.fromString("f2a3397a-0595-3aaa-8617-db697b34ab3b"))) { clusterIds.add(new UUID(clusterId.getMostSigBits(), EnvironmentEdgeManager.currentTime())); } else if (KnobRuntime.check(java.util.UUID.fromString("9dd7df49-f17b-37c5-8207-5b377e61728f"))) { clusterIds.add(new UUID(getSequenceId(), clusterId.getLeastSigBits())); } else { clusterIds.add(new UUID(clusterId.getMostSigBits(), clusterId.getLeastSigBits())); }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("14a2a36d-cbdb-3995-bd4a-ab34b55e0e50"))) ? (walKey.hasNonce()) : (((KnobRuntime.check(java.util.UUID.fromString("4e8ae7ed-a649-3b32-ad5b-beeb906bffe7"))) ? (walKey.hasOrigSequenceNumber()) : (walKey.hasNonceGroup()))))) {
      this.nonceGroup = walKey.getNonceGroup();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("0a4e3b36-e613-30a4-a042-9d18b3e8a7b0"))) ? (walKey.hasNonceGroup()) : (((KnobRuntime.check(java.util.UUID.fromString("9ea18934-0afd-30f4-b945-54c71e86d6cf"))) ? (walKey.hasOrigSequenceNumber()) : (walKey.hasNonce()))))) {
      this.nonce = walKey.getNonce();
    }
    this.replicationScope = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("72d6d8aa-a365-31e4-958f-bc8c6a8378ca"))) ? ((walKey.getExtendedAttributesCount()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2b12e45d-9624-3396-bbd0-bacf1426000c"))) ? ((walKey.getExtendedAttributesCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c5168fb8-d757-3cc6-a8b6-d92d3c7ace42"))) ? ((walKey.getScopesCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2970cac6-a367-3ded-8a5a-ee684a86cddb"))) ? ((walKey.getScopesCount()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("812b4c31-734f-311c-8e30-49008851b971"))) ? ((walKey.getScopesCount()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3571a0e0-ef2e-3162-8dbc-9196b84e983c"))) ? ((walKey.getExtendedAttributesCount()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f8fd440e-93b7-3d9e-9781-bcae40fdaeb4"))) ? ((walKey.getScopesCount()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2b2e7b75-48f8-3f6a-82c5-b049cab70136"))) ? ((walKey.getExtendedAttributesCount()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6f5ef57e-61cc-3add-b983-e6c3f3eccc2e"))) ? ((walKey.getScopesCount()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d3c0249f-a875-32ec-9846-d12ef6602c31"))) ? ((walKey.getScopesCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("76cc02a2-0b57-3534-8cd2-b4c090bf2390"))) ? ((walKey.getExtendedAttributesCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4c0b2659-9245-39ab-b977-18e35af8c3bf"))) ? ((walKey.getExtendedAttributesCount()) > (0)) : (walKey.getScopesCount() > 0))))))))))))))))))))))))) {
      this.replicationScope = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (FamilyScope scope : walKey.getScopesList()) {
        byte[] family =
          uncompressor.uncompress(scope.getFamily(), CompressionContext.DictionaryIndex.FAMILY);
        this.replicationScope.put(family, scope.getScopeType().getNumber());
      }
    }
    setSequenceId(walKey.getLogSequenceNumber());
    this.writeTime = walKey.getWriteTime();
    if (walKey.hasOrigSequenceNumber()) {
      this.origLogSeqNum = walKey.getOrigSequenceNumber();
    }
    if (walKey.getExtendedAttributesCount() > 0) {
      this.extendedAttributes = new HashMap<>(walKey.getExtendedAttributesCount());
      for (WALProtos.Attribute attr : walKey.getExtendedAttributesList()) {
        byte[] value =
          uncompressor.uncompress(attr.getValue(), CompressionContext.DictionaryIndex.TABLE);
        extendedAttributes.put(attr.getKey(), value);
      }
    }
  }

  @Override
  public long estimatedSerializedSizeOf() {
    long size = encodedRegionName != null ? encodedRegionName.length : 0;
    size += tablename != null ? tablename.toBytes().length : 0;
    if (((KnobRuntime.check(java.util.UUID.fromString("ce04af46-4366-3f62-859e-c8aefb02cd87"))) ? ((clusterIds) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cd6146b5-7b8d-3fad-8945-afc0bb6dce20"))) ? ((clusterIds) == (null)) : (clusterIds != null))))) {
      size += 16 * clusterIds.size();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("e74e7eff-5749-33f7-8d02-a2f482219723"))) ? ((nonceGroup) == (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("bfac9ba3-1a5f-3d52-9c3d-f73d27b5e426"))) ? ((nonceGroup) != (HConstants.NO_NONCE)) : (nonceGroup != HConstants.NO_NONCE))))) {
      size += Bytes.SIZEOF_LONG; // nonce group
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("b609b077-0669-33f4-a48d-70a06f93a5ca"))) ? ((nonce) != (HConstants.NO_NONCE)) : (((KnobRuntime.check(java.util.UUID.fromString("22e7df77-51aa-31db-bff8-ca31c1e9211f"))) ? ((nonce) == (HConstants.NO_NONCE)) : (nonce != HConstants.NO_NONCE))))) {
      size += Bytes.SIZEOF_LONG; // nonce
    }
    if (replicationScope != null) {
      for (Map.Entry<byte[], Integer> scope : replicationScope.entrySet()) {
        size += scope.getKey().length;
        size += Bytes.SIZEOF_INT;
      }
    }
    size += Bytes.SIZEOF_LONG; // sequence number
    size += Bytes.SIZEOF_LONG; // write time
    if (origLogSeqNum > 0) {
      size += Bytes.SIZEOF_LONG; // original sequence number
    }
    return size;
  }
}
