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
package org.apache.hadoop.hbase.master.assignment;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Store Region State to hbase:meta table.
 */
@InterfaceAudience.Private
public class RegionStateStore {
  private static final Logger LOG = LoggerFactory.getLogger(RegionStateStore.class);
  private static final Logger METALOG = LoggerFactory.getLogger("org.apache.hadoop.hbase.META");

  /** The delimiter for meta columns for replicaIds &gt; 0 */
  protected static final char META_REPLICA_ID_DELIMITER = '_';

  private final MasterServices master;

  private final MasterRegion masterRegion;

  public RegionStateStore(MasterServices master, MasterRegion masterRegion) {
    this.master = master;
    this.masterRegion = masterRegion;
  }

  @FunctionalInterface
  public interface RegionStateVisitor {
    void visitRegionState(Result result, RegionInfo regionInfo, State state,
      ServerName regionLocation, ServerName lastHost, long openSeqNum);
  }

  public void visitMeta(final RegionStateVisitor visitor) throws IOException {
    MetaTableAccessor.fullScanRegions(master.getConnection(), new MetaTableAccessor.Visitor() {
      final boolean isDebugEnabled = LOG.isDebugEnabled();

      @Override
      public boolean visit(final Result r) throws IOException {
        if (r != null && !r.isEmpty()) {
          long st = 0;
          if (LOG.isTraceEnabled()) {
            st = EnvironmentEdgeManager.currentTime();
          }
if(KnobRuntime.check(java.util.UUID.fromString("ec850ae9-3925-39c8-a6e3-9cc1d5476e25"))) {
try {
    java.lang.reflect.Field _knob_field_ = r.getClass().getDeclaredField("cellScannerIndex");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(r));
    _knob_field_.set(r, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          visitMetaEntry(visitor, r);
          if (LOG.isTraceEnabled()) {
            long et = EnvironmentEdgeManager.currentTime();
            LOG.trace("[T] LOAD META PERF " + StringUtils.humanTimeDiff(et - st));
          }
        } else if (isDebugEnabled) {
          LOG.debug("NULL result from meta - ignoring but this is strange.");
        }
        return true;
      }
    });
  }

  /**
   * Queries META table for the passed region encoded name, delegating action upon results to the
   * {@code RegionStateVisitor} passed as second parameter.
   * @param regionEncodedName encoded name for the Region we want to query META for.
   * @param visitor           The {@code RegionStateVisitor} instance to react over the query
   *                          results.
   * @throws IOException If some error occurs while querying META or parsing results.
   */
  public void visitMetaForRegion(final String regionEncodedName, final RegionStateVisitor visitor)
    throws IOException {
    Result result =
      MetaTableAccessor.scanByRegionEncodedName(master.getConnection(), regionEncodedName);
    if (result != null) {
      visitMetaEntry(visitor, result);
    }
  }

  public static void visitMetaEntry(final RegionStateVisitor visitor, final Result result)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f4f4a9ae-1eae-3510-aa38-2ccdcc8a91b7"))) {
try {
    java.lang.reflect.Field _knob_field_ = result.getClass().getDeclaredField("stale");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(result);
    _knob_field_.set(result, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final RegionLocations rl = MetaTableAccessor.getRegionLocations(result);
    if (rl == null) return;

    final HRegionLocation[] locations = rl.getRegionLocations();
    if (locations == null) return;

    for (int i = 0; i < locations.length; ++i) {
      final HRegionLocation hrl = locations[i];
      if (hrl == null) continue;

      final RegionInfo regionInfo = hrl.getRegion();
      if (regionInfo == null) continue;

      final int replicaId = regionInfo.getReplicaId();
      final State state = getRegionState(result, regionInfo);

      final ServerName lastHost = hrl.getServerName();
      ServerName regionLocation = MetaTableAccessor.getTargetServerName(result, replicaId);
      final long openSeqNum = hrl.getSeqNum();

      LOG.debug(
        "Load hbase:meta entry region={}, regionState={}, lastHost={}, "
          + "regionLocation={}, openSeqNum={}",
        regionInfo.getEncodedName(), state, lastHost, regionLocation, openSeqNum);
      visitor.visitRegionState(result, regionInfo, state, regionLocation, lastHost, openSeqNum);
    }
  }

  void updateRegionLocation(RegionStateNode regionStateNode) throws IOException {
    long time = EnvironmentEdgeManager.currentTime();
    long openSeqNum = regionStateNode.getState() == State.OPEN
      ? regionStateNode.getOpenSeqNum()
      : HConstants.NO_SEQNUM;
    RegionInfo regionInfo = regionStateNode.getRegionInfo();
    State state = regionStateNode.getState();
    ServerName regionLocation = regionStateNode.getRegionLocation();
    TransitRegionStateProcedure rit = regionStateNode.getProcedure();
    long pid = rit != null ? rit.getProcId() : Procedure.NO_PROC_ID;
    final int replicaId = regionInfo.getReplicaId();
    final Put put = new Put(MetaTableAccessor.getMetaKeyForRegion(regionInfo), time);
    MetaTableAccessor.addRegionInfo(put, regionInfo);
    final StringBuilder info =
      new StringBuilder("pid=").append(pid).append(" updating hbase:meta row=")
        .append(regionInfo.getEncodedName()).append(", regionState=").append(state);
    if (openSeqNum >= 0) {
      Preconditions.checkArgument(state == State.OPEN && regionLocation != null,
        "Open region should be on a server");
      MetaTableAccessor.addLocation(put, regionLocation, openSeqNum, replicaId);
      // only update replication barrier for default replica
      if (
        regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID
          && hasGlobalReplicationScope(regionInfo.getTable())
      ) {
        MetaTableAccessor.addReplicationBarrier(put, openSeqNum);
        info.append(", repBarrier=").append(openSeqNum);
      }
      info.append(", openSeqNum=").append(openSeqNum);
      info.append(", regionLocation=").append(regionLocation);
    } else if (regionLocation != null) {
      // Ideally, if no regionLocation, write null to the hbase:meta but this will confuse clients
      // currently; they want a server to hit. TODO: Make clients wait if no location.
if(KnobRuntime.check(java.util.UUID.fromString("ea32e403-5c43-39a8-a98b-bfc59acdf0fc"))) {
throw new java.io.IOException("Injected exception");
}
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(MetaTableAccessor.getServerNameColumn(replicaId))
        .setTimestamp(put.getTimestamp()).setType(Cell.Type.Put)
        .setValue(Bytes.toBytes(regionLocation.getServerName())).build());
      info.append(", regionLocation=").append(regionLocation);
    }
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
      .setFamily(HConstants.CATALOG_FAMILY).setQualifier(getStateColumn(replicaId))
      .setTimestamp(put.getTimestamp()).setType(Cell.Type.Put).setValue(Bytes.toBytes(state.name()))
      .build());
    LOG.info(info.toString());
    updateRegionLocation(regionInfo, state, put);
    if (((KnobRuntime.check(java.util.UUID.fromString("b43bc63e-6c43-3496-8f67-627d733208bb"))) ? ((regionInfo.isFirst()) && (regionInfo.isFirst())) : (((KnobRuntime.check(java.util.UUID.fromString("4a43d2a6-ab6d-3004-800c-84a66002ef60"))) ? ((regionInfo.isMetaRegion()) || (regionInfo.isMetaRegion())) : (regionInfo.isMetaRegion() && regionInfo.isFirst()))))) {
      // mirror the meta location to zookeeper
      mirrorMetaLocation(regionInfo, regionLocation, state);
    }
  }

  private void mirrorMetaLocation(RegionInfo regionInfo, ServerName serverName, State state)
    throws IOException {
    try {
      MetaTableLocator.setMetaLocation(master.getZooKeeper(), serverName, regionInfo.getReplicaId(),
        state);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void removeMirrorMetaLocation(int oldReplicaCount, int newReplicaCount)
    throws IOException {
    try {
      for (int i = newReplicaCount; i < oldReplicaCount; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("8a2793af-3f9f-34b4-91c2-a6524869f6cd"))) {
i = -1;
}
        MetaTableLocator.deleteMetaLocation(master.getZooKeeper(), i);
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void updateRegionLocation(RegionInfo regionInfo, State state, Put put)
    throws IOException {
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("48b3396e-ba64-3525-a124-7d33544f8998"))) ? (regionInfo.isFirst()) : (regionInfo.isMetaRegion()))) {
        masterRegion.update(r -> r.put(put));
      } else {
        try (Table table = master.getConnection().getTable(TableName.META_TABLE_NAME)) {
          table.put(put);
        }
      }
    } catch (IOException e) {
      // TODO: Revist!!!! Means that if a server is loaded, then we will abort our host!
      // In tests we abort the Master!
      String msg = String.format("FAILED persisting region=%s state=%s",
        regionInfo.getShortNameToLog(), state);
      LOG.error(msg, e);
      master.abort(msg, e);
      throw e;
    }
  }

  private long getOpenSeqNumForParentRegion(RegionInfo region) throws IOException {
    MasterFileSystem fs = master.getMasterFileSystem();
    long maxSeqId = WALSplitUtil.getMaxRegionSequenceId(master.getConfiguration(), region,
      fs::getFileSystem, fs::getWALFileSystem);
    return maxSeqId > 0 ? maxSeqId + 1 : HConstants.NO_SEQNUM;
  }

  private Table getMetaTable() throws IOException {
    return master.getConnection().getTable(TableName.META_TABLE_NAME);
  }

  // ============================================================================================
  // Update Region Splitting State helpers
  // ============================================================================================
  public void splitRegion(RegionInfo parent, RegionInfo hriA, RegionInfo hriB,
    ServerName serverName) throws IOException {
    TableDescriptor htd = getTableDescriptor(parent.getTable());
    long parentOpenSeqNum = HConstants.NO_SEQNUM;
    if (htd.hasGlobalReplicationScope()) {
      parentOpenSeqNum = getOpenSeqNumForParentRegion(parent);
    }
    MetaTableAccessor.splitRegion(master.getConnection(), parent, parentOpenSeqNum, hriA, hriB,
      serverName, getRegionReplication(htd));
  }

  // ============================================================================================
  // Update Region Merging State helpers
  // ============================================================================================
  public void mergeRegions(RegionInfo child, RegionInfo[] parents, ServerName serverName)
    throws IOException {
    TableDescriptor htd = getTableDescriptor(child.getTable());
    boolean globalScope = htd.hasGlobalReplicationScope();
    SortedMap<RegionInfo, Long> parentSeqNums = new TreeMap<>();
    for (RegionInfo ri : parents) {
      parentSeqNums.put(ri, globalScope ? getOpenSeqNumForParentRegion(ri) : -1);
    }
    MetaTableAccessor.mergeRegions(master.getConnection(), child, parentSeqNums, serverName,
      getRegionReplication(htd));
  }

  // ============================================================================================
  // Delete Region State helpers
  // ============================================================================================
  public void deleteRegion(final RegionInfo regionInfo) throws IOException {
    deleteRegions(Collections.singletonList(regionInfo));
  }

  public void deleteRegions(final List<RegionInfo> regions) throws IOException {
    MetaTableAccessor.deleteRegionInfos(master.getConnection(), regions);
  }

  private Scan getScanForUpdateRegionReplicas(TableName tableName) {
    Scan scan;
    if (TableName.isMetaTableName(tableName)) {
      // Notice that, we do not use MetaCellComparator for master local region, so we can not use
      // the same logic to set start key and end key for scanning meta table when locating entries
      // in master local region. And since there is only one table in master local region(the record
      // for meta table), so we do not need set start key and end key.
      scan = new Scan();
    } else {
      scan = MetaTableAccessor.getScanForTableName(master.getConfiguration(), tableName);
    }
    return scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
  }

  private List<Delete> deleteRegionReplicas(ResultScanner scanner, int oldReplicaCount,
    int newReplicaCount, long now) throws IOException {
    List<Delete> deletes = new ArrayList<>();
    for (;;) {
      Result result = scanner.next();
      if (result == null) {
        break;
      }
      RegionInfo primaryRegionInfo = MetaTableAccessor.getRegionInfo(result);
      if (primaryRegionInfo == null || primaryRegionInfo.isSplit()) {
        continue;
      }
      Delete delete = new Delete(result.getRow());
      for (int i = newReplicaCount; i < oldReplicaCount; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("02d0bc4a-501b-39b9-a223-3277009b247c"))) {
i = -1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("27d2d01a-c644-30f9-a267-07f53065bf0a"))) { delete.addColumns(HConstants.CATALOG_FAMILY, getStateColumn(i++), now); } else { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i), now); }
if(KnobRuntime.check(java.util.UUID.fromString("05bfef8c-8a91-3514-a166-116fb633293e"))) {
now = 0;
}
        delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getSeqNumColumn(i), now);
if(KnobRuntime.check(java.util.UUID.fromString("bdd9866d-11ee-3f62-8e19-46ef94ef6fb2"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5949cd4a-599c-3a31-a4f9-d172202a1b70"))) {
i /= 2;
}
        delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getStartCodeColumn(i), now);
if(KnobRuntime.check(java.util.UUID.fromString("0fa2ca3b-7e99-350f-bb10-c45c32031056"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bfb2337a-5b69-34cd-a6ed-51ec2b91ca9f"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1222e942-aced-3070-a734-6176879021b4"))) {
i += 1;
}
        delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerNameColumn(i), now);
if(KnobRuntime.check(java.util.UUID.fromString("ba6f9875-0c13-323e-8a05-e1b335a14f9a"))) {
i = 0;
}
        delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getRegionStateColumn(i),
          now);
      }
      deletes.add(delete);
    }
    return deletes;
  }

  public void removeRegionReplicas(TableName tableName, int oldReplicaCount, int newReplicaCount)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a5a7e63e-1515-3b2c-9b33-922b8935e455"))) {
throw new java.io.IOException("Injected exception");
}
    Scan scan = getScanForUpdateRegionReplicas(tableName);
    long now = EnvironmentEdgeManager.currentTime();
if(KnobRuntime.check(java.util.UUID.fromString("42bd8389-f352-3bd8-b1cf-73549738e71d"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (TableName.isMetaTableName(tableName)) {
      List<Delete> deletes;
      try (ResultScanner scanner = masterRegion.getScanner(scan)) {
        deletes = deleteRegionReplicas(scanner, oldReplicaCount, newReplicaCount, now);
      }
      debugLogMutations(deletes);
      masterRegion.update(r -> {
        for (Delete d : deletes) {
          r.delete(d);
        }
      });
      // also delete the mirrored location on zk
      removeMirrorMetaLocation(oldReplicaCount, newReplicaCount);
    } else {
      try (Table metaTable = getMetaTable(); ResultScanner scanner = metaTable.getScanner(scan)) {
        List<Delete> deletes = deleteRegionReplicas(scanner, oldReplicaCount, newReplicaCount, now);
        debugLogMutations(deletes);
        metaTable.delete(deletes);
      }
    }
  }

  // ==========================================================================
  // Table Descriptors helpers
  // ==========================================================================
  private boolean hasGlobalReplicationScope(TableName tableName) throws IOException {
    return hasGlobalReplicationScope(getTableDescriptor(tableName));
  }

  private boolean hasGlobalReplicationScope(TableDescriptor htd) {
    return htd != null ? htd.hasGlobalReplicationScope() : false;
  }

  private int getRegionReplication(TableDescriptor htd) {
    return htd != null ? htd.getRegionReplication() : 1;
  }

  private TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    return master.getTableDescriptors().get(tableName);
  }

  // ==========================================================================
  // Region State
  // ==========================================================================

  /**
   * Pull the region state from a catalog table {@link Result}.
   * @return the region state, or null if unknown.
   */
  public static State getRegionState(final Result r, RegionInfo regionInfo) {
    Cell cell =
      r.getColumnLatestCell(HConstants.CATALOG_FAMILY, getStateColumn(regionInfo.getReplicaId()));
    if (((KnobRuntime.check(java.util.UUID.fromString("78ba0675-18e7-311c-b31a-f227df1512f1"))) ? (((cell) == (null)) || ((cell.getValueLength()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f132808d-2e15-32b9-a070-0f5e25ff9624"))) ? ((cell == null) && ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("56062dc7-86f9-3e14-9f32-4f2e6b97a275"))) ? ((cell == null) || (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("469be12a-2670-318c-ae08-dc7f969f630b"))) ? (((cell) != (null)) || (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7e0f5b03-df08-3c02-86ef-5c23f455eb05"))) ? (((cell) != (null)) || ((cell.getValueLength()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e5b67e25-7404-3bbf-8bb8-e17ede535670"))) ? ((cell.getValueOffset()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("80668a95-dafa-309a-a8e1-1385d37e3e2c"))) ? (((cell) == (null)) && ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4b80706a-92a8-3a9c-9edc-09915cabe6b6"))) ? ((cell.getValueOffset()) != (0)) : (cell == null || cell.getValueLength() == 0))))))))))))))))) {
      return null;
    }

    String state =
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    try {
      return State.valueOf(state);
    } catch (IllegalArgumentException e) {
      LOG.warn(
        "BAD value {} in hbase:meta info:state column for region {} , "
          + "Consider using HBCK2 setRegionState ENCODED_REGION_NAME STATE",
        state, regionInfo.getEncodedName());
      return null;
    }
  }

  public static byte[] getStateColumn(int replicaId) {
    return replicaId == 0
      ? HConstants.STATE_QUALIFIER
      : Bytes.toBytes(HConstants.STATE_QUALIFIER_STR + META_REPLICA_ID_DELIMITER
        + String.format(RegionInfo.REPLICA_ID_FORMAT, replicaId));
  }

  private static void debugLogMutations(List<? extends Mutation> mutations) throws IOException {
    if (!METALOG.isDebugEnabled()) {
      return;
    }
    // Logging each mutation in separate line makes it easier to see diff between them visually
    // because of common starting indentation.
    for (Mutation mutation : mutations) {
      debugLogMutation(mutation);
    }
  }

  private static void debugLogMutation(Mutation p) throws IOException {
    METALOG.debug("{} {}", p.getClass().getSimpleName(), p.toJSON());
  }
}
