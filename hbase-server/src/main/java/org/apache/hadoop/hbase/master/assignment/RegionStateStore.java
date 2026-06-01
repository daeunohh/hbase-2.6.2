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
if(KnobRuntime.check(java.util.UUID.fromString("fcd45408-37d2-392d-8287-20dab4886cfa"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd2fafe2-2959-335c-a65b-ec2bd2988d8e"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6be64fa6-7e50-39ea-8ef3-64f20efbb840"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("271d0b98-b9d7-30c6-b6d6-de1e15dd7690"))) {
try {
    java.lang.reflect.Field field = r.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(r));
    field.set(r, oldValue * 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("917df488-2d5b-338d-96da-282c980ebd56"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9af1b033-249a-33ce-8dd6-0919e621f9e6"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cafecd1-b82d-3b7c-89f9-3fbd79a5d858"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3f9cf955-631f-3a81-9a6f-f6597390b74e"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("mayHaveMoreCellsInRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(result);
    field.set(result, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf0630d8-6a98-3c1c-bbc1-40d3b8d2954c"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a51c6f5-575c-3a41-ac56-4fa6c94943f5"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("readonly");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(result);
    field.set(result, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51534215-5a4b-3e5b-9055-76d6a9a0841b"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6a05324b-8dd6-3add-9bac-51812d87d061"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("cellScannerIndex");
    field.setAccessible(true);
    int oldValue = ((int)field.get(result));
    field.set(result, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41ccf068-50cb-3720-b83f-695520bf346a"))) {
try {
    java.lang.reflect.Field field = result.getClass().getDeclaredField("stale");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(result);
    field.set(result, !oldValue);
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
if(KnobRuntime.check(java.util.UUID.fromString("86ebdd20-2ca4-3bd8-a2f8-18886393f115"))) {
openSeqNum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a64818f3-4a97-33b2-a55b-9dd2992902fd"))) {
openSeqNum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e46a454d-e3a8-3b07-bed9-a866c6e8d62f"))) {
openSeqNum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cca4fbb3-08a7-3b59-8449-919cd620f0a2"))) {
openSeqNum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d6378b7-b7a6-345a-9395-e1443172f768"))) {
throw new java.io.IOException("Injected exception");
}
        MetaTableAccessor.addReplicationBarrier(put, openSeqNum);
if(KnobRuntime.check(java.util.UUID.fromString("6a9af180-fb99-30d8-bca3-ebfebb28ac13"))) {
openSeqNum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fe3922ec-cd56-3b54-a6d1-472a0c8e1500"))) {
openSeqNum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("51713bb3-a427-36f7-be9c-76124ec34f2d"))) {
openSeqNum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("22db4f3d-6559-38b7-9034-29caf7e864b7"))) {
openSeqNum = 0;
}
        info.append(", repBarrier=").append(openSeqNum);
      }
if(KnobRuntime.check(java.util.UUID.fromString("8410ffed-1a33-3cc9-a8c0-1f9e47089cc1"))) {
openSeqNum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dbb5f0ca-6e44-3b9a-b273-28dec7e8a38b"))) {
openSeqNum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1075709b-d46e-3d53-a878-e3afea1d7600"))) {
openSeqNum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7d3a1fc3-22d9-34ed-9cef-f69ef40fd5c4"))) {
openSeqNum += 1;
}
      info.append(", openSeqNum=").append(openSeqNum);
if(KnobRuntime.check(java.util.UUID.fromString("b7e02057-8d3b-34b4-9597-729b531813e4"))) {
try {
    java.lang.reflect.Field field = regionLocation.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLocation));
    field.set(regionLocation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      info.append(", regionLocation=").append(regionLocation);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("bd2b6405-b006-3260-ad77-5722f6ceed4c"))) ? ((regionLocation) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("21d4c4db-99b6-34d2-9938-e3a020a5cc03"))) ? ((regionLocation) != (null)) : (regionLocation != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39f48796-edd7-30c5-a7bd-cdd92f9c2891"))) {
try {
    java.lang.reflect.Field field = regionLocation.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLocation));
    field.set(regionLocation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      info.append(", regionLocation=").append(regionLocation);
    }
if(KnobRuntime.check(java.util.UUID.fromString("21f476ae-0925-3845-b32d-5811f3c6f74b"))) {
throw new java.io.IOException("Injected exception");
}
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
      .setFamily(HConstants.CATALOG_FAMILY).setQualifier(getStateColumn(replicaId))
      .setTimestamp(put.getTimestamp()).setType(Cell.Type.Put).setValue(Bytes.toBytes(state.name()))
      .build());
    if (KnobRuntime.check(java.util.UUID.fromString("1249285e-59f8-346a-9e2f-c542f59ce8ed"))) { LOG.debug(info.toString()); } else if (KnobRuntime.check(java.util.UUID.fromString("cd274464-9e0a-31e8-bdc6-d141343a03ff"))) { LOG.trace(info.toString()); } else { LOG.info(info.toString()); }
    updateRegionLocation(regionInfo, state, put);
    if (((KnobRuntime.check(java.util.UUID.fromString("b5d37543-3c7e-3489-870a-f2f45cf64a15"))) ? (regionInfo.isFirst()) : (((KnobRuntime.check(java.util.UUID.fromString("099ea0f3-9c1d-3aa0-b4b5-27efb07717e4"))) ? ((regionInfo.isFirst()) || (regionInfo.isFirst())) : (((KnobRuntime.check(java.util.UUID.fromString("ccc1d863-8947-353a-877e-219af55bdde6"))) ? ((regionInfo.isMetaRegion()) && (regionInfo.isFirst())) : (((KnobRuntime.check(java.util.UUID.fromString("b43bc63e-6c43-3496-8f67-627d733208bb"))) ? ((regionInfo.isFirst()) && (regionInfo.isFirst())) : (((KnobRuntime.check(java.util.UUID.fromString("542b6fce-15db-3012-87f1-0fa76693c483"))) ? ((regionInfo.isFirst()) && (regionInfo.isMetaRegion())) : (((KnobRuntime.check(java.util.UUID.fromString("94108010-c376-3329-8bf5-210cbb83ec07"))) ? ((regionInfo.isMetaRegion()) || (regionInfo.isFirst())) : (((KnobRuntime.check(java.util.UUID.fromString("4ee89fc5-a94f-30a9-9a43-34bfedfaf9b2"))) ? ((regionInfo.isMetaRegion()) && (regionInfo.isMetaRegion())) : (((KnobRuntime.check(java.util.UUID.fromString("6a644edc-ddc0-3d8e-b79e-c6ffc27e33b1"))) ? ((regionInfo.isFirst()) || (regionInfo.isMetaRegion())) : (((KnobRuntime.check(java.util.UUID.fromString("11fb7408-b52e-3eef-be62-3ac13f600ba8"))) ? (regionInfo.isMetaRegion()) : (((KnobRuntime.check(java.util.UUID.fromString("4a43d2a6-ab6d-3004-800c-84a66002ef60"))) ? ((regionInfo.isMetaRegion()) || (regionInfo.isMetaRegion())) : (regionInfo.isMetaRegion() && regionInfo.isFirst()))))))))))))))))))))) {
      // mirror the meta location to zookeeper
      mirrorMetaLocation(regionInfo, regionLocation, state);
    }
  }

  private void mirrorMetaLocation(RegionInfo regionInfo, ServerName serverName, State state)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b89f14e3-f9ee-30f1-a394-450c77f895c7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ae34cb97-1d1c-3d9e-89a5-8039577e7fe9"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98f6a3c3-3d75-3556-9cc3-d13085803dae"))) {
return;
}
    try {
if(KnobRuntime.check(java.util.UUID.fromString("b17a364e-08f8-34ab-bebc-c7600341660e"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      MetaTableLocator.setMetaLocation(master.getZooKeeper(), serverName, regionInfo.getReplicaId(),
        state);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void removeMirrorMetaLocation(int oldReplicaCount, int newReplicaCount)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5f5a7091-0550-3c03-adca-2227e61c2b96"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("314bc82a-3e1c-3d26-b17f-66374f758718"))) {
return;
}
    try {
      for (int i = newReplicaCount; i < oldReplicaCount; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("346afef1-54b4-3b32-b6b1-13c800bcfa3b"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("10707f60-f83e-33b3-831a-364622112107"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("8a2793af-3f9f-34b4-91c2-a6524869f6cd"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("08cd90e6-4680-3634-8c43-948c78ad6245"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("787accf7-3add-3891-a713-34c26f267341"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("88e1dcbf-64a4-3436-88fd-4cc35911fabb"))) {
i -= 1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("80a556d6-063d-3b3f-9d08-4395ecc4238e"))) { MetaTableLocator.deleteMetaLocation(master.getZooKeeper(), i++); } else { MetaTableLocator.deleteMetaLocation(master.getZooKeeper(), i); }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void updateRegionLocation(RegionInfo regionInfo, State state, Put put)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c4145f52-c08b-399e-8591-3ad36ed4a0e9"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6bfc142e-6cf1-3c3c-b55a-12564cf6a3e5"))) {
return;
}
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("48b3396e-ba64-3525-a124-7d33544f8998"))) ? (regionInfo.isFirst()) : (regionInfo.isMetaRegion()))) {
if(KnobRuntime.check(java.util.UUID.fromString("9ed1b9f1-940e-3004-a935-57f177a3f0b9"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("fbdc116f-1be9-3047-acfe-f445e7822537"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("2e3d75d3-2cbb-3bf6-863f-8eaae21ee113"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("73ca0ba9-c2b9-39bf-9677-a86676c69f25"))) {
return null;
}
    List<Delete> deletes = new ArrayList<>();
    for (;;) {
if(KnobRuntime.check(java.util.UUID.fromString("e605d105-235f-3ea9-a5e7-b48da4dd002e"))) {
throw new java.io.IOException("Injected exception");
}
      Result result = scanner.next();
      if (((KnobRuntime.check(java.util.UUID.fromString("2882b3be-1035-3927-a9a5-573553f3ffe4"))) ? ((result) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("59de0d21-897c-3ada-bb45-a6caeeea8e3e"))) ? ((result) != (null)) : (result == null))))) {
        break;
      }
      RegionInfo primaryRegionInfo = MetaTableAccessor.getRegionInfo(result);
      if (primaryRegionInfo == null || primaryRegionInfo.isSplit()) {
        continue;
      }
      Delete delete = new Delete(result.getRow());
      for (int i = newReplicaCount; i < oldReplicaCount; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("e4658393-3d06-38f7-900d-5f2780849fd0"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d224dea5-3a51-356c-bf04-533b3816ed4a"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("600a05e8-0743-3e6f-9091-f6755f00ccc0"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("21953452-33b8-3751-bb17-d4e82e271103"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("02d0bc4a-501b-39b9-a223-3277009b247c"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6f0bb35f-9803-32d8-9cad-edd8d54da521"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("202578a8-f06d-380b-9c9c-76a7505d1e75"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("484e2b02-3392-37a7-bc71-0b7a802b49b7"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b7cc1e91-e6be-3a7c-8185-bcd13d4625e0"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("367d7e74-8530-334a-9d97-6aefb4f82064"))) {
i = 0;
}
        if (KnobRuntime.check(java.util.UUID.fromString("6bf9443d-8b1c-33aa-90ad-618f11fe900f"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("27d2d01a-c644-30f9-a267-07f53065bf0a"))) { delete.addColumns(HConstants.CATALOG_FAMILY, getStateColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("f49fc7a4-05f2-3576-8516-30350df5fca6"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerNameColumn(i++), now); } else { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i), now); }
if(KnobRuntime.check(java.util.UUID.fromString("43e42389-481f-36dd-aa37-b469206e333b"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("05bfef8c-8a91-3514-a166-116fb633293e"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5a94dc8-0577-38df-a306-184911a53d7b"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("72de22d1-11c1-3a9a-977e-4ee4b11efcf1"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2bbaaaa6-59a7-356d-af0a-cdf2130df404"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f2aa1b5e-7f51-3f9c-8b5c-a19a38663b53"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c6a051bc-e47d-333d-97be-f9a63545f6a9"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("771af608-d8db-3eed-ae6e-ee1754c2717d"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("72aefe50-ee34-3fd0-9fa0-486b4ff0dcc0"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("66e03cbc-4f27-36de-83e0-57549cec520f"))) {
now = -1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("e5e9bc8e-0300-3add-89c9-ee40af61dd2a"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerNameColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("ba1d6928-69aa-3621-b78d-38ff18e9328e"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("743a6427-e21f-3e24-9618-1724cf500ed3"))) { delete.addColumns(HConstants.CATALOG_FAMILY, getStateColumn(i++), now); } else { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getSeqNumColumn(i), now); }
if(KnobRuntime.check(java.util.UUID.fromString("bd4991de-4896-3562-9540-e1cbe55d124f"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("af57ef04-e60a-31cf-817c-b0c078d876e6"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea809e54-f99b-305f-9b47-2276330a8726"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4645cf04-0519-34af-a801-12f713d055fd"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba5d9108-f5b1-358a-aeb3-e90add9b75b9"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6ccbc0b6-8c17-3c0c-8981-e0599c9f4d58"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f31e823b-ee83-322a-85bb-8edce4559a00"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("61d425f9-8970-35d7-aa2f-950bf5961995"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bdd9866d-11ee-3f62-8e19-46ef94ef6fb2"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5949cd4a-599c-3a31-a4f9-d172202a1b70"))) {
i /= 2;
}
        if (KnobRuntime.check(java.util.UUID.fromString("228d8ab5-86a2-3b9a-a412-e5bca6863e17"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerNameColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("34f168bc-a6c2-3a25-93ed-ca722e03de81"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("a1af5e66-e1df-391a-a432-3468e2ed25cc"))) { delete.addColumns(HConstants.CATALOG_FAMILY, getStateColumn(i++), now); } else { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getStartCodeColumn(i), now); }
if(KnobRuntime.check(java.util.UUID.fromString("ab204054-c457-3c5f-8168-1dbe9d2cf568"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fbdf44e8-d0f9-30d9-bb94-c07cb5c56d58"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("82a3e137-d3fb-3c36-9183-054802d0984e"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("95bdad64-94fc-390c-b94b-d4a6dd7724c8"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("69b8e0ce-92e5-389f-9dfd-944ca29a7c6b"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1222e942-aced-3070-a734-6176879021b4"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7074d854-0b77-3d52-b07f-2ee1331c3ad1"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0fa2ca3b-7e99-350f-bb10-c45c32031056"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bfb2337a-5b69-34cd-a6ed-51ec2b91ca9f"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("39663556-00a0-3c4b-8f31-64f8c6be942c"))) {
i = 0;
}
        if (KnobRuntime.check(java.util.UUID.fromString("7cb66752-bb41-3ede-9848-d571e6b79b36"))) { delete.addColumns(HConstants.CATALOG_FAMILY, getStateColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("16c57e9c-3f77-3109-9bd1-47ee0d441315"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(i++), now); } else if (KnobRuntime.check(java.util.UUID.fromString("db6ae0bc-1681-3927-86b8-4020db8c0b79"))) { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerNameColumn(i++), now); } else { delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerNameColumn(i), now); }
if(KnobRuntime.check(java.util.UUID.fromString("91524dfa-13c7-3362-9ad3-1465b9c04cb3"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0460d925-dd46-3fcf-a76d-877f4646fdf8"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5c475504-d883-3279-be45-bb0cfea340f6"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("418ade4f-e448-32fa-a1fd-994e10a74001"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6e37e643-2be6-3360-ae27-26e647df8d3b"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3fffd565-edb5-3826-a4b8-24b0b49e9563"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d9bd15c-3928-38a7-8f2b-f2c8a56aa323"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bef9f5de-6d60-3c3a-8ac0-2d738cf1a53f"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba6f9875-0c13-323e-8a05-e1b335a14f9a"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4e00ba56-02ba-3889-baf3-e368972d4da6"))) {
now = 0;
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
if(KnobRuntime.check(java.util.UUID.fromString("88e92323-87e8-327e-a598-a6b2477f100d"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5a7e63e-1515-3b2c-9b33-922b8935e455"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("996e8706-9826-3316-a69c-b8c507695d17"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("68e68234-9493-3682-9b64-8757ed79627d"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc1b3ab8-691c-3004-8d1a-d8f99c4115f8"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f225019e-89a5-3bca-a0e2-c94b4515b0dd"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Scan scan = getScanForUpdateRegionReplicas(tableName);
    long now = EnvironmentEdgeManager.currentTime();
if(KnobRuntime.check(java.util.UUID.fromString("0f9a5031-253d-39b9-92f6-918760116174"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("91502970-ebb0-3e2f-bfab-21b372bc7477"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b98d73a4-b844-3d02-8d63-131ce209c219"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f2579731-55ce-312e-bc16-929ea4ee4683"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("712ddc3c-3c50-3f0e-853b-2240fe13b970"))) {
throw new java.io.IOException("Injected exception");
}
      masterRegion.update(r -> {
        for (Delete d : deletes) {
if(KnobRuntime.check(java.util.UUID.fromString("20b60c0e-95f5-3e38-8852-6cf6a600c933"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("3d660150-5be6-3b00-9f36-74ccb58c3a8c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("35c82c09-4282-3b29-8c43-e6b44772186d"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("034922f6-0cbd-3148-9a05-fe35779a8e82"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("727bbe7c-24c7-3580-a97f-19123809db7e"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33f8a657-0252-3ed9-b8af-d2b68bce77b4"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("335bc3c3-2150-3937-a416-f52e8cd75060"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1718f2e9-e65e-3158-9f63-789b09fcd5cb"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return hasGlobalReplicationScope(getTableDescriptor(tableName));
  }

  private boolean hasGlobalReplicationScope(TableDescriptor htd) {
if(KnobRuntime.check(java.util.UUID.fromString("58e50153-d4b5-3c84-91c8-b804d3a8543b"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9f19613a-64ce-3391-946e-9c2d4b5653a5"))) {
return true;
}
    return htd != null ? htd.hasGlobalReplicationScope() : false;
  }

  private int getRegionReplication(TableDescriptor htd) {
if(KnobRuntime.check(java.util.UUID.fromString("9a7db754-bdce-336d-be74-9d0feb51ffa8"))) {
return 0;
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("104b138d-72f9-3c41-8438-72b6ca3fdaef"))) ? (((cell) == (null)) && ((cell.getValueLength()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e92b415a-4fb6-305c-85bf-64d20c58ff76"))) ? (((cell) == (null)) && ((cell.getValueOffset()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0d4794fb-e271-3042-9875-7d16ceac67a3"))) ? (((cell) != (null)) && ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f15fd0e4-ed30-332d-a6e1-164a7b79235f"))) ? ((cell == null) && (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("de16bbaf-48ce-3105-8ca7-bc1d480bcc3d"))) ? ((cell == null) || ((cell.getValueLength()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7e0f5b03-df08-3c02-86ef-5c23f455eb05"))) ? (((cell) != (null)) || ((cell.getValueLength()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5fe85899-2203-3b7e-ac5f-17e314c41483"))) ? (cell == null) : (((KnobRuntime.check(java.util.UUID.fromString("a19f5288-4381-34d2-abfe-6c219812067a"))) ? ((cell.getValueLength()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4b80706a-92a8-3a9c-9edc-09915cabe6b6"))) ? ((cell.getValueOffset()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f132808d-2e15-32b9-a070-0f5e25ff9624"))) ? ((cell == null) && ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9e13ba36-2780-3dd7-bfc8-4f382601f303"))) ? ((cell == null) && ((cell.getValueLength()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("85573b36-566c-378e-9558-628138728a1f"))) ? (((cell) != (null)) || ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("736b80c0-d94a-3510-86c0-519793322d58"))) ? (((cell) != (null)) && (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("56062dc7-86f9-3e14-9f32-4f2e6b97a275"))) ? ((cell == null) || (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("73ff9389-2929-349b-b667-eae205da81de"))) ? ((cell.getValueLength()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("469be12a-2670-318c-ae08-dc7f969f630b"))) ? (((cell) != (null)) || (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("21cd0c69-bc6c-312b-a837-cfc6481a40c5"))) ? ((cell) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("80f4958b-5a60-392d-ad0f-19907839d5e3"))) ? (((cell) == (null)) && (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("23dba6d5-d584-3572-8dab-c6692979be9d"))) ? (((cell) != (null)) && ((cell.getValueLength()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("40a1d987-10f6-3db0-b09a-0e0f23318c71"))) ? (((cell) == (null)) || ((cell.getValueOffset()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2ce4959-49a6-3694-915f-8d005004a269"))) ? (((cell) == (null)) || ((cell.getValueLength()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8a89865e-64fa-3636-93fd-8578c168d851"))) ? (((cell) != (null)) || ((cell.getValueLength()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4326e6f3-5eff-342a-9c0b-f736a07c1cd0"))) ? ((cell == null) || ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aea1ca5b-2ba0-3ea3-8ce9-23f36fd3b350"))) ? (((cell) != (null)) && ((cell.getValueLength()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0806fef0-1aea-3213-a0e0-801e7b18b984"))) ? (cell.getValueLength() == 0) : (((KnobRuntime.check(java.util.UUID.fromString("a3beaec1-2f5d-3af8-abf4-4a3144638dd1"))) ? ((cell == null) && ((cell.getValueOffset()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4118db7e-5e6a-334c-b452-8a4e3c63d7ca"))) ? ((cell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("49ffb0bc-fd17-3b34-8beb-10f704b880d8"))) ? (((cell) != (null)) || ((cell.getValueOffset()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("12401a97-dff0-397c-a432-b93d6ce45fad"))) ? ((cell == null) && ((cell.getValueLength()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e5b67e25-7404-3bbf-8bb8-e17ede535670"))) ? ((cell.getValueOffset()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("46f0a85c-880f-3780-b747-4d8c5a2b9dea"))) ? ((cell == null) || ((cell.getValueLength()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8346f233-258f-3fa1-95d7-c9c43efe0096"))) ? (((cell) == (null)) || (cell.getValueLength() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("904ca4b5-62db-36d6-9975-ad1fab218d15"))) ? ((cell == null) || ((cell.getValueOffset()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("78ba0675-18e7-311c-b31a-f227df1512f1"))) ? (((cell) == (null)) || ((cell.getValueLength()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("80668a95-dafa-309a-a8e1-1385d37e3e2c"))) ? (((cell) == (null)) && ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c1572b73-54ba-31fc-8eb1-6c0eaa3b19fe"))) ? (((cell) == (null)) && ((cell.getValueLength()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("abacb433-fba1-3a64-8715-ea4b78135511"))) ? (((cell) == (null)) || ((cell.getValueOffset()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("79914723-53f2-3f77-bc96-32eb94dfbead"))) ? (((cell) != (null)) && ((cell.getValueOffset()) != (0))) : (cell == null || cell.getValueLength() == 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
