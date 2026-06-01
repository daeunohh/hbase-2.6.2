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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Immutable information for scans over a store.
 */
// Has to be public for PartitionedMobCompactor to access; ditto on tests making use of a few of
// the accessors below. Shutdown access. TODO
@InterfaceAudience.Private
public class ScanInfo {
  private byte[] family;
  private int minVersions;
  private int maxVersions;
  private long ttl;
  private KeepDeletedCells keepDeletedCells;
  private long timeToPurgeDeletes;
  private CellComparator comparator;
  private long tableMaxRowSize;
  private boolean usePread;
  private long cellsPerTimeoutCheck;
  private boolean parallelSeekEnabled;
  private final long preadMaxBytes;
  private final boolean newVersionBehavior;

  public static final long FIXED_OVERHEAD =
    ClassSize.align(ClassSize.OBJECT + (2 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_INT)
      + (4 * Bytes.SIZEOF_LONG) + (4 * Bytes.SIZEOF_BOOLEAN));

  /**
   * @param family             {@link ColumnFamilyDescriptor} describing the column family
   * @param ttl                Store's TTL (in ms)
   * @param timeToPurgeDeletes duration in ms after which a delete marker can be purged during a
   *                           major compaction.
   * @param comparator         The store's comparator
   */
  public ScanInfo(Configuration conf, ColumnFamilyDescriptor family, long ttl,
    long timeToPurgeDeletes, CellComparator comparator) {
    this(conf, family.getName(), family.getMinVersions(), family.getMaxVersions(), ttl,
      family.getKeepDeletedCells(), family.getBlocksize(), timeToPurgeDeletes, comparator,
      family.isNewVersionBehavior());
  }

  private static long getCellsPerTimeoutCheck(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("b6502e35-5384-3c94-a8b0-260eaa41939f"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3ffd3f66-2eb0-3f21-868e-fdd35bf6ef75"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    long perHeartbeat = conf.getLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK,
      StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK);
    return perHeartbeat > 0
      ? perHeartbeat
      : StoreScanner.DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK;
  }

  /**
   * @param family             Name of this store's column family
   * @param minVersions        Store's MIN_VERSIONS setting
   * @param maxVersions        Store's VERSIONS setting
   * @param ttl                Store's TTL (in ms)
   * @param blockSize          Store's block size
   * @param timeToPurgeDeletes duration in ms after which a delete marker can be purged during a
   *                           major compaction.
   * @param keepDeletedCells   Store's keepDeletedCells setting
   * @param comparator         The store's comparator
   */
  public ScanInfo(Configuration conf, byte[] family, int minVersions, int maxVersions, long ttl,
    KeepDeletedCells keepDeletedCells, long blockSize, long timeToPurgeDeletes,
    CellComparator comparator, boolean newVersionBehavior) {
    this(family, minVersions, maxVersions, ttl, keepDeletedCells, timeToPurgeDeletes, comparator,
      conf.getLong(HConstants.TABLE_MAX_ROWSIZE_KEY, HConstants.TABLE_MAX_ROWSIZE_DEFAULT),
      conf.getBoolean("hbase.storescanner.use.pread", false), getCellsPerTimeoutCheck(conf),
      conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false),
      ((KnobRuntime.check(java.util.UUID.fromString("26e9d60e-c35a-304f-a0f1-27a255e07afe"))) ? (conf.getLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, (4) * (4 * blockSize))) : (((KnobRuntime.check(java.util.UUID.fromString("c6eb04e8-17f7-3ee1-b0d5-e785492de0f1"))) ? (conf.getLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, blockSize)) : (((KnobRuntime.check(java.util.UUID.fromString("3cd5201b-2d98-3a8a-ba83-d8201a0c73c0"))) ? (conf.getLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, (4) * (blockSize))) : (conf.getLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, 4 * blockSize))))))), newVersionBehavior);
  }

  private ScanInfo(byte[] family, int minVersions, int maxVersions, long ttl,
    KeepDeletedCells keepDeletedCells, long timeToPurgeDeletes, CellComparator comparator,
    long tableMaxRowSize, boolean usePread, long cellsPerTimeoutCheck, boolean parallelSeekEnabled,
    long preadMaxBytes, boolean newVersionBehavior) {
    this.family = family;
    this.minVersions = minVersions;
    this.maxVersions = maxVersions;
    this.ttl = ttl;
    this.keepDeletedCells = keepDeletedCells;
    this.timeToPurgeDeletes = timeToPurgeDeletes;
    this.comparator = comparator;
    this.tableMaxRowSize = tableMaxRowSize;
    this.usePread = usePread;
    this.cellsPerTimeoutCheck = cellsPerTimeoutCheck;
    this.parallelSeekEnabled = parallelSeekEnabled;
    this.preadMaxBytes = preadMaxBytes;
    this.newVersionBehavior = newVersionBehavior;
  }

  long getTableMaxRowSize() {
if(KnobRuntime.check(java.util.UUID.fromString("31e3b017-c925-36a1-87dc-0edcfb89c9c2"))) {
return 0;
}
    return this.tableMaxRowSize;
  }

  boolean isUsePread() {
if(KnobRuntime.check(java.util.UUID.fromString("8d295a16-63f6-326d-b155-530c0a1bbc6c"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("9755d40e-f0ca-3a79-9a63-0c1369a0700d"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("640f3354-4962-31d0-b311-a416a2e29480"))) ? (!this.usePread) : (this.usePread));
  }

  long getCellsPerTimeoutCheck() {
if(KnobRuntime.check(java.util.UUID.fromString("c27364f2-ad02-3241-833b-74b748382f3d"))) {
return 0;
}
    return this.cellsPerTimeoutCheck;
  }

  boolean isParallelSeekEnabled() {
if(KnobRuntime.check(java.util.UUID.fromString("5d099650-fad8-3d8a-88af-3729b9491c6d"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("331d11b0-0533-33f3-b816-d23d5215a679"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("0abaf900-57c6-307a-bff6-9cfd75aa78a7"))) ? (!this.parallelSeekEnabled) : (this.parallelSeekEnabled));
  }

  public byte[] getFamily() {
if(KnobRuntime.check(java.util.UUID.fromString("1ade16fb-85e3-300c-bada-bd22a4f438ca"))) {
return null;
}
    return family;
  }

  public int getMinVersions() {
if(KnobRuntime.check(java.util.UUID.fromString("831b49b9-916f-3982-985e-b7c45da280e0"))) {
return 0;
}
    return minVersions;
  }

  public int getMaxVersions() {
if(KnobRuntime.check(java.util.UUID.fromString("466143ae-c416-3e46-9516-f8cefc342213"))) {
return 0;
}
    return maxVersions;
  }

  public long getTtl() {
if(KnobRuntime.check(java.util.UUID.fromString("09bc91fa-9376-34f7-92ea-c9e1b53944b5"))) {
return 0;
}
    return ttl;
  }

  public KeepDeletedCells getKeepDeletedCells() {
if(KnobRuntime.check(java.util.UUID.fromString("e586af87-850d-3f66-b92f-de4cb0a33314"))) {
return null;
}
    return keepDeletedCells;
  }

  public long getTimeToPurgeDeletes() {
    return timeToPurgeDeletes;
  }

  public CellComparator getComparator() {
if(KnobRuntime.check(java.util.UUID.fromString("bd66afb6-fa0f-3d44-bf5a-0e041cffa909"))) {
return null;
}
    return comparator;
  }

  long getPreadMaxBytes() {
if(KnobRuntime.check(java.util.UUID.fromString("f4540432-0901-341f-9e2c-9ef71490b495"))) {
return 0;
}
    return preadMaxBytes;
  }

  public boolean isNewVersionBehavior() {
if(KnobRuntime.check(java.util.UUID.fromString("180b189a-6cae-3859-aae0-bc6cc3096dc1"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("aadbe108-26e6-3bdd-abbc-b53cc9e9f27c"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("2840007d-ed2b-348e-ba0d-c21260f90465"))) ? (!newVersionBehavior) : (newVersionBehavior));
  }

  /**
   * Used by CP users for customizing max versions, ttl and keepDeletedCells.
   */
  ScanInfo customize(int maxVersions, long ttl, KeepDeletedCells keepDeletedCells) {
    return customize(maxVersions, ttl, keepDeletedCells, minVersions, timeToPurgeDeletes);
  }

  /**
   * Used by CP users for customizing max versions, ttl, keepDeletedCells, min versions, and time to
   * purge deletes.
   */
  ScanInfo customize(int maxVersions, long ttl, KeepDeletedCells keepDeletedCells, int minVersions,
    long timeToPurgeDeletes) {
    return new ScanInfo(family, minVersions, maxVersions, ttl, keepDeletedCells, timeToPurgeDeletes,
      comparator, tableMaxRowSize, usePread, cellsPerTimeoutCheck, parallelSeekEnabled,
      preadMaxBytes, newVersionBehavior);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("family", Bytes.toStringBinary(family))
      .append("minVersions", minVersions).append("maxVersions", maxVersions).append("ttl", ttl)
      .append("keepDeletedCells", keepDeletedCells).append("timeToPurgeDeletes", timeToPurgeDeletes)
      .append("tableMaxRowSize", tableMaxRowSize).append("usePread", usePread)
      .append("cellsPerTimeoutCheck", cellsPerTimeoutCheck)
      .append("parallelSeekEnabled", parallelSeekEnabled).append("preadMaxBytes", preadMaxBytes)
      .append("newVersionBehavior", newVersionBehavior).toString();
  }
}
