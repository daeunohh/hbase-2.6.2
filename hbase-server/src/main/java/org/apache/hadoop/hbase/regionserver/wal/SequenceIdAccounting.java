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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ImmutableByteArray;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accounting of sequence ids per region and then by column family. So we can keep our accounting
 * current, call startCacheFlush and then finishedCacheFlush or abortCacheFlush so this instance can
 * keep abreast of the state of sequence id persistence. Also call update per append.
 * <p>
 * For the implementation, we assume that all the {@code encodedRegionName} passed in are gotten by
 * {@link org.apache.hadoop.hbase.client.RegionInfo#getEncodedNameAsBytes()}. So it is safe to use
 * it as a hash key. And for family name, we use {@link ImmutableByteArray} as key. This is because
 * hash based map is much faster than RBTree or CSLM and here we are on the critical write path. See
 * HBASE-16278 for more details.
 * </p>
 */
@InterfaceAudience.Private
class SequenceIdAccounting {
  private static final Logger LOG = LoggerFactory.getLogger(SequenceIdAccounting.class);

  /**
   * This lock ties all operations on {@link SequenceIdAccounting#flushingSequenceIds} and
   * {@link #lowestUnflushedSequenceIds} Maps. {@link #lowestUnflushedSequenceIds} has the lowest
   * outstanding sequence ids EXCEPT when flushing. When we flush, the current lowest set for the
   * region/column family are moved (atomically because of this lock) to
   * {@link #flushingSequenceIds}.
   * <p>
   * The two Maps are tied by this locking object EXCEPT when we go to update the lowest entry; see
   * {@link #lowestUnflushedSequenceIds}. In here is a putIfAbsent call on
   * {@link #lowestUnflushedSequenceIds}. In this latter case, we will add this lowest sequence id
   * if we find that there is no entry for the current column family. There will be no entry only if
   * we just came up OR we have moved aside current set of lowest sequence ids because the current
   * set are being flushed (by putting them into {@link #flushingSequenceIds}). This is how we pick
   * up the next 'lowest' sequence id per region per column family to be used figuring what is in
   * the next flush.
   */
  private final Object tieLock = new Object();

  /**
   * Map of encoded region names and family names to their OLDEST -- i.e. their first, the
   * longest-lived, their 'earliest', the 'lowest' -- sequence id.
   * <p>
   * When we flush, the current lowest sequence ids get cleared and added to
   * {@link #flushingSequenceIds}. The next append that comes in, is then added here to
   * {@link #lowestUnflushedSequenceIds} as the next lowest sequenceid.
   * <p>
   * If flush fails, currently server is aborted so no need to restore previous sequence ids.
   * <p>
   * Needs to be concurrent Maps because we use putIfAbsent updating oldest.
   */
  private final ConcurrentMap<byte[],
    ConcurrentMap<ImmutableByteArray, Long>> lowestUnflushedSequenceIds = new ConcurrentHashMap<>();

  /**
   * Map of encoded region names and family names to their lowest or OLDEST sequence/edit id
   * currently being flushed out to hfiles. Entries are moved here from
   * {@link #lowestUnflushedSequenceIds} while the lock {@link #tieLock} is held (so movement
   * between the Maps is atomic).
   */
  private final Map<byte[], Map<ImmutableByteArray, Long>> flushingSequenceIds = new HashMap<>();

  /**
   * <p>
   * Map of region encoded names to the latest/highest region sequence id. Updated on each call to
   * append.
   * </p>
   * <p>
   * This map uses byte[] as the key, and uses reference equality. It works in our use case as we
   * use {@link org.apache.hadoop.hbase.client.RegionInfo#getEncodedNameAsBytes()} as keys. For a
   * given region, it always returns the same array.
   * </p>
   */
  private Map<byte[], Long> highestSequenceIds = new HashMap<>();

  /**
   * Returns the lowest unflushed sequence id for the region.
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionName</code>. Will return
   *         {@link HConstants#NO_SEQNUM} when none.
   */
  long getLowestSequenceId(final byte[] encodedRegionName) {
    synchronized (this.tieLock) {
      Map<?, Long> m = this.flushingSequenceIds.get(encodedRegionName);
      long flushingLowest = m != null ? getLowestSequenceId(m) : Long.MAX_VALUE;
      m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      long unflushedLowest = m != null ? getLowestSequenceId(m) : HConstants.NO_SEQNUM;
      return Math.min(flushingLowest, unflushedLowest);
    }
  }

  /**
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionname</code> and
   *         <code>familyName</code>. Returned sequenceid may be for an edit currently being
   *         flushed.
   */
  long getLowestSequenceId(final byte[] encodedRegionName, final byte[] familyName) {
    ImmutableByteArray familyNameWrapper = ImmutableByteArray.wrap(familyName);
    synchronized (this.tieLock) {
      Map<ImmutableByteArray, Long> m = this.flushingSequenceIds.get(encodedRegionName);
      if (m != null) {
        Long lowest = m.get(familyNameWrapper);
        if (((KnobRuntime.check(java.util.UUID.fromString("538269a3-3fa3-3d0e-bc4d-f771c91162f7"))) ? ((lowest) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("37f5f9d8-2210-3658-8dbb-41e24da93506"))) ? ((lowest) != (null)) : (lowest != null))))) {
          return lowest;
        }
      }
      m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      if (((KnobRuntime.check(java.util.UUID.fromString("29015d12-8b3b-3666-b305-575c8515cc46"))) ? ((m) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("74b91de3-c772-32a9-ad82-93f61954ef6c"))) ? ((m) != (null)) : (m != null))))) {
        Long lowest = m.get(familyNameWrapper);
        if (lowest != null) {
          return lowest;
        }
      }
    }
    return HConstants.NO_SEQNUM;
  }

  /**
   * Reset the accounting of highest sequenceid by regionname.
   * @return Return the previous accounting Map of regions to the last sequence id written into
   *         each.
   */
  Map<byte[], Long> resetHighest() {
    Map<byte[], Long> old = this.highestSequenceIds;
    this.highestSequenceIds = new HashMap<>();
    return old;
  }

  /**
   * We've been passed a new sequenceid for the region. Set it as highest seen for this region and
   * if we are to record oldest, or lowest sequenceids, save it as oldest seen if nothing currently
   * older.
   * @param lowest Whether to keep running account of oldest sequence id.
   */
  void update(byte[] encodedRegionName, Set<byte[]> families, long sequenceid,
    final boolean lowest) {
    Long l = Long.valueOf(sequenceid);
    this.highestSequenceIds.put(encodedRegionName, l);
    if (lowest) {
      ConcurrentMap<ImmutableByteArray, Long> m = getOrCreateLowestSequenceIds(encodedRegionName);
      for (byte[] familyName : families) {
        if (KnobRuntime.check(java.util.UUID.fromString("383a44bd-2e60-3342-9293-1aee9474a054"))) { m.put(ImmutableByteArray.wrap(familyName), l); } else { m.putIfAbsent(ImmutableByteArray.wrap(familyName), l); }
      }
    }
  }

  /**
   * Clear all the records of the given region as it is going to be closed.
   * <p/>
   * We will call this once we get the region close marker. We need this because that, if we use
   * Durability.ASYNC_WAL, after calling startCacheFlush, we may still get some ongoing wal entries
   * that has not been processed yet, this will lead to orphan records in the
   * lowestUnflushedSequenceIds and then cause too many WAL files.
   * <p/>
   * See HBASE-23157 for more details.
   */
  void onRegionClose(byte[] encodedRegionName) {
    synchronized (tieLock) {
      this.lowestUnflushedSequenceIds.remove(encodedRegionName);
      Map<ImmutableByteArray, Long> flushing = this.flushingSequenceIds.remove(encodedRegionName);
      if (flushing != null) {
        LOG.warn("Still have flushing records when closing {}, {}",
          Bytes.toString(encodedRegionName),
          flushing.entrySet().stream().map(e -> e.getKey().toString() + "->" + e.getValue())
            .collect(Collectors.joining(",", "{", "}")));
      }
    }
    this.highestSequenceIds.remove(encodedRegionName);
  }

  /**
   * Update the store sequence id, e.g., upon executing in-memory compaction
   */
  void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceId,
    boolean onlyIfGreater) {
    if (sequenceId == null) {
      return;
    }
    Long highest = this.highestSequenceIds.get(encodedRegionName);
    if (highest == null || sequenceId > highest) {
      this.highestSequenceIds.put(encodedRegionName, sequenceId);
    }
    ImmutableByteArray familyNameWrapper = ImmutableByteArray.wrap(familyName);
    synchronized (this.tieLock) {
      ConcurrentMap<ImmutableByteArray, Long> m = getOrCreateLowestSequenceIds(encodedRegionName);
      boolean replaced = false;
      while (!replaced) {
        Long oldSeqId = m.get(familyNameWrapper);
        if (oldSeqId == null) {
          m.put(familyNameWrapper, sequenceId);
          replaced = true;
        } else if (onlyIfGreater) {
          if (sequenceId > oldSeqId) {
            replaced = m.replace(familyNameWrapper, oldSeqId, sequenceId);
          } else {
            return;
          }
        } else { // replace even if sequence id is not greater than oldSeqId
          m.put(familyNameWrapper, sequenceId);
          return;
        }
      }
    }
  }

  ConcurrentMap<ImmutableByteArray, Long> getOrCreateLowestSequenceIds(byte[] encodedRegionName) {
    // Intentionally, this access is done outside of this.regionSequenceIdLock. Done per append.
    return computeIfAbsent(this.lowestUnflushedSequenceIds, encodedRegionName,
      ConcurrentHashMap::new);
  }

  /**
   * @param sequenceids Map to search for lowest value.
   * @return Lowest value found in <code>sequenceids</code>.
   */
  private static long getLowestSequenceId(Map<?, Long> sequenceids) {
    long lowest = HConstants.NO_SEQNUM;
    for (Map.Entry<?, Long> entry : sequenceids.entrySet()) {
      if (entry.getKey().toString().equals("METAFAMILY")) {
        continue;
      }
      Long sid = entry.getValue();
      if (((KnobRuntime.check(java.util.UUID.fromString("47b64349-5f0f-3e38-8dc0-ca68021a9bb6"))) ? (((lowest) == (HConstants.NO_SEQNUM)) || (sid.longValue() < lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("29bb494b-f0c0-3b1f-bda9-123e2be93989"))) ? (((lowest) != (HConstants.NO_SEQNUM)) && ((sid.longValue()) == (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("63e6eb61-a35f-33be-89f4-43f8c53b6b89"))) ? ((lowest == HConstants.NO_SEQNUM) || ((sid.longValue()) >= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("36351e98-8951-31a7-82e2-54eb604deed8"))) ? (sid.longValue() < lowest) : (((KnobRuntime.check(java.util.UUID.fromString("948a8286-de4d-3849-a09c-a697ebe072e3"))) ? (((lowest) == (HConstants.NO_SEQNUM)) && (sid.longValue() < lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("7368f8b2-d1e5-35c1-9ffd-0891cfe3e63e"))) ? ((sid.longValue()) < (lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("622cc2d7-1db9-3adb-a78b-c7b1ba5e6812"))) ? ((lowest == HConstants.NO_SEQNUM) && ((sid.longValue()) <= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("d48ef85e-a74b-36e7-a63a-88c7fb8c001b"))) ? ((sid.longValue()) <= (lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("3c0e8ec6-d5fe-3dbf-b0bd-a8bfe7f3ffe0"))) ? ((lowest) == (HConstants.NO_SEQNUM)) : (((KnobRuntime.check(java.util.UUID.fromString("5bf21562-02d5-3a10-ab1c-5163cdb8d857"))) ? (((lowest) != (HConstants.NO_SEQNUM)) || ((sid.longValue()) != (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("bcf1e2c9-80f9-3037-a660-baf6c1f56982"))) ? (((lowest) == (HConstants.NO_SEQNUM)) || ((sid.longValue()) > (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("c336c204-f159-3b59-b987-9470f3654ffe"))) ? ((sid.longValue()) == (lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("0f89bb6a-e6c3-366a-9457-175740e0817b"))) ? (((lowest) == (HConstants.NO_SEQNUM)) && ((sid.longValue()) <= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("73243980-3e0b-3b8e-8a85-6885d9772b4a"))) ? (((lowest) == (HConstants.NO_SEQNUM)) || ((sid.longValue()) >= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("8d3af789-964e-3b39-813e-2aadde082efa"))) ? (((lowest) == (HConstants.NO_SEQNUM)) || ((sid.longValue()) < (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("c1d71234-385a-35fd-822a-96be94dcc0cd"))) ? ((sid.longValue()) != (lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("b96e065b-d2c6-30f7-91be-80dfac0d3425"))) ? ((lowest == HConstants.NO_SEQNUM) && ((sid.longValue()) >= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("8c5214b0-1fbd-3eba-a3c2-78e6f7e107d6"))) ? ((lowest == HConstants.NO_SEQNUM) || ((sid.longValue()) < (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("70387672-a330-3eff-a705-9e894341c23d"))) ? (((lowest) == (HConstants.NO_SEQNUM)) && ((sid.longValue()) != (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("ceb9582e-8475-35b2-8583-8dedb91e4ad0"))) ? ((lowest == HConstants.NO_SEQNUM) && (sid.longValue() < lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("f12730b4-f778-3bdc-af8e-bfd1a84fc51b"))) ? ((lowest == HConstants.NO_SEQNUM) || ((sid.longValue()) <= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("23722f9a-f8ef-3f46-9bed-aa14f2bba07a"))) ? (((lowest) != (HConstants.NO_SEQNUM)) || (sid.longValue() < lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("03b02b99-1771-3093-aeeb-73a4b0233a85"))) ? ((lowest == HConstants.NO_SEQNUM) || ((sid.longValue()) != (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("098c749f-6d74-3272-a3a7-718c3b4c5674"))) ? (((lowest) != (HConstants.NO_SEQNUM)) && (sid.longValue() < lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("ad39ef11-8a17-30ee-9350-3edcc3836ff8"))) ? (((lowest) == (HConstants.NO_SEQNUM)) || ((sid.longValue()) == (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("b8bf1b51-28ca-3127-bb30-a4b2f1df26ce"))) ? ((sid.longValue()) >= (lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("9a718a16-96dd-3155-9adc-6eea5f858d14"))) ? (((lowest) != (HConstants.NO_SEQNUM)) || ((sid.longValue()) >= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("0eb92d97-1915-3d1b-8664-0493d00b6eb6"))) ? (((lowest) != (HConstants.NO_SEQNUM)) && ((sid.longValue()) > (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("7cc9efde-33c9-347b-9796-785aebc43452"))) ? ((lowest == HConstants.NO_SEQNUM) && ((sid.longValue()) != (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("c2901482-b043-35d0-b480-e1eeedd72583"))) ? ((lowest) != (HConstants.NO_SEQNUM)) : (((KnobRuntime.check(java.util.UUID.fromString("ec631e21-ee06-3352-b013-5827e30c5bcb"))) ? (((lowest) != (HConstants.NO_SEQNUM)) || ((sid.longValue()) <= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("1c62d32b-b295-32c3-9815-eb5fb6047b6e"))) ? (((lowest) != (HConstants.NO_SEQNUM)) || ((sid.longValue()) > (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("11b3b1ac-f45b-392d-a534-b905326bb383"))) ? ((lowest == HConstants.NO_SEQNUM) || (sid.longValue() < lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("456abc59-3b37-3980-9e77-f9d76bf58103"))) ? (((lowest) == (HConstants.NO_SEQNUM)) && ((sid.longValue()) >= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("1c1f9114-2236-3fe0-94f6-090bddeda917"))) ? (((lowest) != (HConstants.NO_SEQNUM)) && ((sid.longValue()) <= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("40432ede-7275-32cf-93b4-da2380d9d4b5"))) ? (((lowest) != (HConstants.NO_SEQNUM)) && ((sid.longValue()) >= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("0aedff67-0b8a-3f71-93fa-a202c6285faf"))) ? (((lowest) == (HConstants.NO_SEQNUM)) || ((sid.longValue()) <= (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("74647f1d-aeb4-37fe-aed9-1808625bb49c"))) ? (((lowest) != (HConstants.NO_SEQNUM)) || ((sid.longValue()) < (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("9dc4a6d8-a0d9-3352-94d9-3135ba26ed6a"))) ? (((lowest) == (HConstants.NO_SEQNUM)) || ((sid.longValue()) != (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("9aef2333-b0d9-323b-a230-791136c12c7b"))) ? ((lowest == HConstants.NO_SEQNUM) && ((sid.longValue()) < (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("c5534467-2b64-3055-8507-08869967acbe"))) ? (lowest == HConstants.NO_SEQNUM) : (((KnobRuntime.check(java.util.UUID.fromString("758c9b33-c177-312f-b165-a20ed97b94dd"))) ? ((lowest == HConstants.NO_SEQNUM) && ((sid.longValue()) == (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("8d69b935-82d5-3725-a57d-a43491620f60"))) ? ((lowest == HConstants.NO_SEQNUM) || ((sid.longValue()) == (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("1e4c4a8b-868f-3351-b17b-c502bbdd6227"))) ? ((lowest == HConstants.NO_SEQNUM) || ((sid.longValue()) > (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("e9944388-ede4-3922-bc13-23426095ce3e"))) ? (((lowest) == (HConstants.NO_SEQNUM)) && ((sid.longValue()) > (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("377ec70a-0138-320d-9e8f-3a9c686f1f21"))) ? ((lowest == HConstants.NO_SEQNUM) && ((sid.longValue()) > (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("1cd32973-cb74-3597-a8fc-13ebf3f4909f"))) ? (((lowest) == (HConstants.NO_SEQNUM)) && ((sid.longValue()) < (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("715c206e-59fc-37b8-9a38-6cd83fc732b6"))) ? (((lowest) != (HConstants.NO_SEQNUM)) && ((sid.longValue()) != (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("b5ec85ad-b12a-3c64-89fa-2901d0c1f502"))) ? ((sid.longValue()) > (lowest)) : (((KnobRuntime.check(java.util.UUID.fromString("37bde3ca-2a7a-3299-87fd-a5198037404f"))) ? (((lowest) != (HConstants.NO_SEQNUM)) && ((sid.longValue()) < (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("9e7f675f-8f50-3c7c-9a20-e137dc0a43c3"))) ? (((lowest) == (HConstants.NO_SEQNUM)) && ((sid.longValue()) == (lowest))) : (((KnobRuntime.check(java.util.UUID.fromString("81b19997-5181-398a-876e-9385f62a7168"))) ? (((lowest) != (HConstants.NO_SEQNUM)) || ((sid.longValue()) == (lowest))) : (lowest == HConstants.NO_SEQNUM || sid.longValue() < lowest))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        lowest = sid.longValue();
      }
    }
    return lowest;
  }

  /**
   * @return New Map that has same keys as <code>src</code> but instead of a Map for a value, it
   *         instead has found the smallest sequence id and it returns that as the value instead.
   */
  private <T extends Map<?, Long>> Map<byte[], Long> flattenToLowestSequenceId(Map<byte[], T> src) {
    if (src == null || src.isEmpty()) {
      return null;
    }
    Map<byte[], Long> tgt = new HashMap<>();
    for (Map.Entry<byte[], T> entry : src.entrySet()) {
      long lowestSeqId = getLowestSequenceId(entry.getValue());
      if (lowestSeqId != HConstants.NO_SEQNUM) {
if(KnobRuntime.check(java.util.UUID.fromString("0751fffa-9dd3-367d-a9f5-8ae909e11777"))) {
lowestSeqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("80320e55-c74c-3d7b-a1de-861b8c6ef698"))) {
lowestSeqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5bbd9f07-f882-3cff-b871-9484f4ab3983"))) {
lowestSeqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("41e60125-1d44-3550-8fa6-88f881be1372"))) {
lowestSeqId += 1;
}
        tgt.put(entry.getKey(), lowestSeqId);
      }
    }
    return tgt;
  }

  /**
   * @param encodedRegionName Region to flush.
   * @param families          Families to flush. May be a subset of all families in the region.
   * @return Returns {@link HConstants#NO_SEQNUM} if we are flushing the whole region OR if we are
   *         flushing a subset of all families but there are no edits in those families not being
   *         flushed; in other words, this is effectively same as a flush of all of the region
   *         though we were passed a subset of regions. Otherwise, it returns the sequence id of the
   *         oldest/lowest outstanding edit.
   */
  Long startCacheFlush(final byte[] encodedRegionName, final Set<byte[]> families) {
    Map<byte[], Long> familytoSeq = new HashMap<>();
    for (byte[] familyName : families) {
      familytoSeq.put(familyName, HConstants.NO_SEQNUM);
    }
    return startCacheFlush(encodedRegionName, familytoSeq);
  }

  Long startCacheFlush(final byte[] encodedRegionName, final Map<byte[], Long> familyToSeq) {
    Map<ImmutableByteArray, Long> oldSequenceIds = null;
    Long lowestUnflushedInRegion = HConstants.NO_SEQNUM;
    synchronized (tieLock) {
      Map<ImmutableByteArray, Long> m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      if (((KnobRuntime.check(java.util.UUID.fromString("f3d8ef7c-4798-379f-afbc-9df75f7cde05"))) ? ((m) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8d8a5c0d-70dd-333b-8cd6-e435660681f2"))) ? ((m) != (null)) : (m != null))))) {
        // NOTE: Removal from this.lowestUnflushedSequenceIds must be done in controlled
        // circumstance because another concurrent thread now may add sequenceids for this family
        // (see above in getOrCreateLowestSequenceId). Make sure you are ok with this. Usually it
        // is fine because updates are blocked when this method is called. Make sure!!!
        for (Map.Entry<byte[], Long> entry : familyToSeq.entrySet()) {
          ImmutableByteArray familyNameWrapper = ImmutableByteArray.wrap((byte[]) entry.getKey());
          Long seqId = null;
          if (entry.getValue() == HConstants.NO_SEQNUM) {
            seqId = m.remove(familyNameWrapper);
          } else {
            seqId = m.replace(familyNameWrapper, entry.getValue());
          }
          if (((KnobRuntime.check(java.util.UUID.fromString("a044ec7e-e0a1-3c6f-9653-8cd54cfe2b49"))) ? ((seqId) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d60f6bb0-e221-366d-a16e-9f7eb27d4765"))) ? ((seqId) != (null)) : (seqId != null))))) {
            if (oldSequenceIds == null) {
              oldSequenceIds = new HashMap<>();
            }
            oldSequenceIds.put(familyNameWrapper, seqId);
          }
        }
        if (oldSequenceIds != null && !oldSequenceIds.isEmpty()) {
          if (((KnobRuntime.check(java.util.UUID.fromString("e09c30e7-3a7e-3b17-9d54-d45942f2b83b"))) ? ((this.flushingSequenceIds.put(encodedRegionName, oldSequenceIds)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c6e2f3ce-9562-313d-b276-6617449583ff"))) ? ((this.flushingSequenceIds.put(encodedRegionName, oldSequenceIds)) != (null)) : (this.flushingSequenceIds.put(encodedRegionName, oldSequenceIds) != null))))) {
            LOG.warn("Flushing Map not cleaned up for " + Bytes.toString(encodedRegionName)
              + ", sequenceid=" + oldSequenceIds);
          }
        }
        if (m.isEmpty()) {
          // Remove it otherwise it will be in oldestUnflushedStoreSequenceIds for ever
          // even if the region is already moved to other server.
          // Do not worry about data racing, we held write lock of region when calling
          // startCacheFlush, so no one can add value to the map we removed.
          this.lowestUnflushedSequenceIds.remove(encodedRegionName);
        } else {
          // Flushing a subset of the region families. Return the sequence id of the oldest entry.
          lowestUnflushedInRegion = Collections.min(m.values());
        }
      }
    }
    // Do this check outside lock.
    if (oldSequenceIds != null && oldSequenceIds.isEmpty()) {
      // TODO: if we have no oldStoreSeqNum, and WAL is not disabled, presumably either
      // the region is already flushing (which would make this call invalid), or there
      // were no appends after last flush, so why are we starting flush? Maybe we should
      // assert not empty. Less rigorous, but safer, alternative is telling the caller to stop.
      // For now preserve old logic.
      LOG.warn("Couldn't find oldest sequenceid for " + Bytes.toString(encodedRegionName));
    }
    return lowestUnflushedInRegion;
  }

  void completeCacheFlush(byte[] encodedRegionName, long maxFlushedSeqId) {
if(KnobRuntime.check(java.util.UUID.fromString("c1f1b340-cae6-307b-9234-1f7fba007b76"))) {
return;
}
    // This is a simple hack to avoid maxFlushedSeqId go backwards.
    // The system works fine normally, but if we make use of Durability.ASYNC_WAL and we are going
    // to flush all the stores, the maxFlushedSeqId will be next seq id of the region, but we may
    // still have some unsynced WAL entries in the ringbuffer after we call startCacheFlush, and
    // then it will be recorded as the lowestUnflushedSeqId by the above update method, which is
    // less than the current maxFlushedSeqId. And if next time we only flush the family with this
    // unusual lowestUnflushedSeqId, the maxFlushedSeqId will go backwards.
    // This is an unexpected behavior so we should fix it, otherwise it may cause unexpected
    // behavior in other area.
    // The solution here is a bit hack but fine. Just replace the lowestUnflushedSeqId with
    // maxFlushedSeqId + 1 if it is lesser. The meaning of maxFlushedSeqId is that, all edits less
    // than or equal to it have been flushed, i.e, persistent to HFile, so set
    // lowestUnflushedSequenceId to maxFlushedSeqId + 1 will not cause data loss.
    // And technically, using +1 is fine here. If the maxFlushesSeqId is just the flushOpSeqId, it
    // means we have flushed all the stores so the seq id for actual data should be at least plus 1.
    // And if we do not flush all the stores, then the maxFlushedSeqId is calculated by
    // lowestUnflushedSeqId - 1, so here let's plus the 1 back.
    Long wrappedSeqId = Long.valueOf(maxFlushedSeqId + 1);
    synchronized (tieLock) {
      this.flushingSequenceIds.remove(encodedRegionName);
      Map<ImmutableByteArray, Long> unflushed = lowestUnflushedSequenceIds.get(encodedRegionName);
      if (((KnobRuntime.check(java.util.UUID.fromString("abde5e42-e350-364f-bce9-d1560501930e"))) ? ((unflushed) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("37a1f29d-4e25-32e3-9169-3f497592a67e"))) ? ((unflushed) != (null)) : (unflushed == null))))) {
        return;
      }
      for (Map.Entry<ImmutableByteArray, Long> e : unflushed.entrySet()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("332a0120-a9ce-3946-b298-03d02a5decea"))) ? ((e.getValue().longValue()) <= (maxFlushedSeqId + 1)) : (((KnobRuntime.check(java.util.UUID.fromString("ca443cf9-6a81-38e2-8b70-80a917929abe"))) ? ((e.getValue().longValue()) >= (maxFlushedSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("a855d7b0-c7e7-3865-9b93-54df54ae8ac2"))) ? ((e.getValue().longValue()) == (maxFlushedSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("bceb8cdf-ad53-35f6-903e-bb6a6f8eba65"))) ? ((e.getValue().longValue()) <= (maxFlushedSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("c2484112-bef4-3cd1-aa98-f84d3c4e6f57"))) ? ((e.getValue().longValue()) >= (maxFlushedSeqId + 1)) : (((KnobRuntime.check(java.util.UUID.fromString("d6289f74-3fb7-349d-b506-cbf1541cba4f"))) ? ((e.getValue().longValue()) != (maxFlushedSeqId + 1)) : (((KnobRuntime.check(java.util.UUID.fromString("94035e55-6d94-3cb1-a829-73efeeec2140"))) ? ((e.getValue().longValue()) > (maxFlushedSeqId + 1)) : (((KnobRuntime.check(java.util.UUID.fromString("a5ce48c0-8687-3a01-9c10-69d5ec0e77b6"))) ? ((e.getValue().longValue()) > (maxFlushedSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("7a59f463-6503-3349-a66a-b1962e4ec7e9"))) ? ((e.getValue().longValue()) < (maxFlushedSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("825b7185-6abb-3662-83ac-b7232759423f"))) ? ((e.getValue().longValue()) != (maxFlushedSeqId)) : (((KnobRuntime.check(java.util.UUID.fromString("5533a9b7-6e2e-39a7-a8c9-807f30e4808a"))) ? ((e.getValue().longValue()) < (maxFlushedSeqId + 1)) : (((KnobRuntime.check(java.util.UUID.fromString("e6198461-81ea-3b03-a118-f703dea53431"))) ? ((e.getValue().longValue()) == (maxFlushedSeqId + 1)) : (e.getValue().longValue() <= maxFlushedSeqId))))))))))))))))))))))))) {
          e.setValue(wrappedSeqId);
        }
      }
    }
  }

  void abortCacheFlush(final byte[] encodedRegionName) {
    // Method is called when we are crashing down because failed write flush AND it is called
    // if we fail prepare. The below is for the fail prepare case; we restore the old sequence ids.
    Map<ImmutableByteArray, Long> flushing = null;
    Map<ImmutableByteArray, Long> tmpMap = new HashMap<>();
    // Here we are moving sequenceids from flushing back to unflushed; doing opposite of what
    // happened in startCacheFlush. During prepare phase, we have update lock on the region so
    // no edits should be coming in via append.
    synchronized (tieLock) {
      flushing = this.flushingSequenceIds.remove(encodedRegionName);
      if (flushing != null) {
        Map<ImmutableByteArray, Long> unflushed = getOrCreateLowestSequenceIds(encodedRegionName);
        for (Map.Entry<ImmutableByteArray, Long> e : flushing.entrySet()) {
          // Set into unflushed the 'old' oldest sequenceid and if any value in flushed with this
          // value, it will now be in tmpMap.
          tmpMap.put(e.getKey(), unflushed.put(e.getKey(), e.getValue()));
        }
      }
    }

    // Here we are doing some 'test' to see if edits are going in out of order. What is it for?
    // Carried over from old code.
    if (flushing != null) {
      for (Map.Entry<ImmutableByteArray, Long> e : flushing.entrySet()) {
        Long currentId = tmpMap.get(e.getKey());
        if (currentId != null && currentId.longValue() < e.getValue().longValue()) {
          String errorStr = Bytes.toString(encodedRegionName) + " family " + e.getKey().toString()
            + " acquired edits out of order current memstore seq=" + currentId
            + ", previous oldest unflushed id=" + e.getValue();
          LOG.error(errorStr);
          Runtime.getRuntime().halt(1);
        }
      }
    }
  }

  /**
   * See if passed <code>sequenceids</code> are lower -- i.e. earlier -- than any outstanding
   * sequenceids, sequenceids we are holding on to in this accounting instance.
   * @param sequenceids Keyed by encoded region name. Cannot be null (doesn't make sense for it to
   *                    be null).
   * @return true if all sequenceids are lower, older than, the old sequenceids in this instance.
   */
  boolean areAllLower(Map<byte[], Long> sequenceids) {
    Map<byte[], Long> flushing = null;
    Map<byte[], Long> unflushed = null;
    synchronized (this.tieLock) {
      // Get a flattened -- only the oldest sequenceid -- copy of current flushing and unflushed
      // data structures to use in tests below.
      flushing = flattenToLowestSequenceId(this.flushingSequenceIds);
      unflushed = flattenToLowestSequenceId(this.lowestUnflushedSequenceIds);
    }
    for (Map.Entry<byte[], Long> e : sequenceids.entrySet()) {
      long oldestFlushing = Long.MAX_VALUE;
      long oldestUnflushed = Long.MAX_VALUE;
      if (((KnobRuntime.check(java.util.UUID.fromString("fb877ee9-85a6-3d55-8034-aa1407bb6349"))) ? (((flushing) != (null)) || (flushing.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("755f69da-8ad0-3ad6-943c-4ca9ee19d956"))) ? (flushing.containsKey(e.getKey())) : (((KnobRuntime.check(java.util.UUID.fromString("857a2b3e-6271-3cfb-a59a-51bc99c45adf"))) ? ((flushing != null) && (flushing.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("372e811f-d084-3607-b0dd-da059208d901"))) ? ((flushing) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("55525fe0-0214-3354-a8b7-d348f467cfbc"))) ? (((flushing) == (null)) && (flushing.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("10c98ce9-f224-376a-8d2a-42df96790f3f"))) ? (((flushing) != (null)) && (flushing.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("51af27b2-6fbf-3617-93b5-7933ba5d8cbd"))) ? (flushing != null) : (((KnobRuntime.check(java.util.UUID.fromString("e3f953f6-d546-3656-9d9e-17082bd3b4f9"))) ? ((flushing) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("41fa6175-98be-3b43-b3e2-c84f699cb04b"))) ? (((flushing) == (null)) || (flushing.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("c2b90678-b444-398d-bfa8-1d9ce1bd56a7"))) ? ((flushing != null) || (flushing.containsKey(e.getKey()))) : (flushing != null && flushing.containsKey(e.getKey())))))))))))))))))))))) {
        oldestFlushing = flushing.get(e.getKey());
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("2ccc6e39-964b-38a5-a1fb-42f698e2db1f"))) ? ((unflushed != null) && (unflushed.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("c12a2b5c-2f11-396d-b9d7-9d907ad25694"))) ? ((unflushed) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("851ec431-c6be-37e8-887b-714ac66b7ebb"))) ? ((unflushed) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fd121fa9-ca82-3e4d-b54d-8e22469dd95e"))) ? (((unflushed) == (null)) || (unflushed.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("7a64aae7-6bda-3594-a756-161e17fc54e1"))) ? ((unflushed != null) || (unflushed.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("d8c3ca06-94d4-3a6c-b765-740d8dc20a5a"))) ? (((unflushed) != (null)) || (unflushed.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("98436228-ed8b-30f0-8348-f6387a83fdd9"))) ? (unflushed != null) : (((KnobRuntime.check(java.util.UUID.fromString("19a34d66-47b5-3cfb-b56b-2cfbf20df699"))) ? (((unflushed) == (null)) && (unflushed.containsKey(e.getKey()))) : (((KnobRuntime.check(java.util.UUID.fromString("4d8fe4dd-7681-3191-b97a-5d95ecb0ec44"))) ? (unflushed.containsKey(e.getKey())) : (((KnobRuntime.check(java.util.UUID.fromString("39c835c5-927c-33fe-844f-dfd8463cdc73"))) ? (((unflushed) != (null)) && (unflushed.containsKey(e.getKey()))) : (unflushed != null && unflushed.containsKey(e.getKey())))))))))))))))))))))) {
        oldestUnflushed = unflushed.get(e.getKey());
      }
if(KnobRuntime.check(java.util.UUID.fromString("6b10e3bf-b131-3f27-aff0-c4488c81c6f0"))) {
oldestFlushing += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("665b16b5-1afa-3019-8b30-748c018de4fe"))) {
oldestUnflushed = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7dddacc9-f777-3b2c-844d-664d73560f73"))) {
oldestFlushing = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("44b25d4f-e347-3e03-a05d-249ea908d257"))) {
oldestUnflushed = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b70b73fd-a44e-3d77-ad5a-379e5ef90bce"))) {
oldestUnflushed += 1;
}
      long min = Math.min(oldestFlushing, oldestUnflushed);
      if (((KnobRuntime.check(java.util.UUID.fromString("7a27d84c-a59b-3a02-b5ee-8730c5484cca"))) ? ((min) != (e.getValue())) : (((KnobRuntime.check(java.util.UUID.fromString("9a2b0453-1c13-3314-8495-c41512135318"))) ? ((min) > (e.getValue())) : (((KnobRuntime.check(java.util.UUID.fromString("3a40a504-b368-3033-a80c-fdebad0c195e"))) ? ((min) < (e.getValue())) : (((KnobRuntime.check(java.util.UUID.fromString("384de724-54ee-3bf5-9a0b-5a9efef21d4f"))) ? ((min) <= (e.getValue())) : (((KnobRuntime.check(java.util.UUID.fromString("a0d2d17b-206a-358c-903e-9bdfe60b1b2b"))) ? ((min) == (e.getValue())) : (((KnobRuntime.check(java.util.UUID.fromString("96a1e447-f3ca-356e-838b-1a883203a639"))) ? ((min) >= (e.getValue())) : (min <= e.getValue()))))))))))))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Iterates over the given Map and compares sequence ids with corresponding entries in
   * {@link #lowestUnflushedSequenceIds}. If a region in {@link #lowestUnflushedSequenceIds} has a
   * sequence id less than that passed in <code>sequenceids</code> then return it.
   * @param sequenceids Sequenceids keyed by encoded region name.
   * @return stores of regions found in this instance with sequence ids less than those passed in.
   */
  Map<byte[], List<byte[]>> findLower(Map<byte[], Long> sequenceids) {
if(KnobRuntime.check(java.util.UUID.fromString("31097445-aa9a-3ee9-81c8-c30a16f9d55d"))) {
return null;
}
    Map<byte[], List<byte[]>> toFlush = null;
    // Keeping the old behavior of iterating unflushedSeqNums under oldestSeqNumsLock.
    synchronized (tieLock) {
      for (Map.Entry<byte[], Long> e : sequenceids.entrySet()) {
        Map<ImmutableByteArray, Long> m = this.lowestUnflushedSequenceIds.get(e.getKey());
        if (m == null) {
          continue;
        }
        for (Map.Entry<ImmutableByteArray, Long> me : m.entrySet()) {
          if (me.getValue() <= e.getValue()) {
            if (toFlush == null) {
              toFlush = new TreeMap(Bytes.BYTES_COMPARATOR);
            }
            toFlush.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
              .add(Bytes.toBytes(me.getKey().toString()));
          }
        }
      }
    }
    return toFlush;
  }
}
