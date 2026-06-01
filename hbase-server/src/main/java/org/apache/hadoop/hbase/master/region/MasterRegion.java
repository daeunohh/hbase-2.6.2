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
package org.apache.hadoop.hbase.master.region;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.HConstants.HREGION_LOGDIR_NAME;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALSyncTimeoutIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.math.IntMath;

/**
 * A region that stores data in a separated directory, which can be used to store master local data.
 * <p/>
 * FileSystem layout:
 *
 * <pre>
 * hbase
 *   |
 *   --&lt;region dir&gt;
 *       |
 *       --data
 *       |  |
 *       |  --/&lt;ns&gt/&lt;table&gt/&lt;encoded-region-name&gt; <---- The region data
 *       |      |
 *       |      --replay <---- The edits to replay
 *       |
 *       --WALs
 *          |
 *          --&lt;master-server-name&gt; <---- The WAL dir for active master
 *          |
 *          --&lt;master-server-name&gt;-dead <---- The WAL dir for dead master
 * </pre>
 *
 * Notice that, you can use different root file system and WAL file system. Then the above directory
 * will be on two file systems, the root file system will have the data directory while the WAL
 * filesystem will have the WALs directory. The archived HFile will be moved to the global HFile
 * archived directory with the {@link MasterRegionParams#archivedWalSuffix()} suffix. The archived
 * WAL will be moved to the global WAL archived directory with the
 * {@link MasterRegionParams#archivedHFileSuffix()} suffix.
 */
@InterfaceAudience.Private
public final class MasterRegion {

  private static final Logger LOG = LoggerFactory.getLogger(MasterRegion.class);

  private static final String REPLAY_EDITS_DIR = "recovered.wals";

  private static final String DEAD_WAL_DIR_SUFFIX = "-dead";

  static final String INITIALIZING_FLAG = ".initializing";

  static final String INITIALIZED_FLAG = ".initialized";

  private static final int REGION_ID = 1;

  private final Server server;

  private final WALFactory walFactory;

  final HRegion region;

  final MasterRegionFlusherAndCompactor flusherAndCompactor;

  private MasterRegionWALRoller walRoller;

  private MasterRegion(Server server, HRegion region, WALFactory walFactory,
    MasterRegionFlusherAndCompactor flusherAndCompactor, MasterRegionWALRoller walRoller) {
    this.server = server;
    this.region = region;
    this.walFactory = walFactory;
    this.flusherAndCompactor = flusherAndCompactor;
    this.walRoller = walRoller;
  }

  private void closeRegion(boolean abort) {
    try {
if(KnobRuntime.check(java.util.UUID.fromString("651b665a-58f1-38b9-99f9-57dadabb7b0e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bf1072b8-63d6-35e2-a8f5-03fc4b8604e9"))) {
abort = !abort;
}
if(KnobRuntime.check(java.util.UUID.fromString("c7a84860-4558-398c-bac6-12e35d845962"))) {
abort = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("01c741ba-e1d7-357a-bdd1-b40ad7a867b3"))) {
abort = false;
}
      if (KnobRuntime.check(java.util.UUID.fromString("1dd7303b-d78a-34ab-8fee-fd7d065aecf0"))) { region.close(!abort); } else { region.close(abort); }
    } catch (IOException e) {
      LOG.warn("Failed to close region", e);
    }
  }

  private void shutdownWAL() {
if(KnobRuntime.check(java.util.UUID.fromString("9667b088-67c1-33df-a37d-32dd52516c42"))) {
return;
}
    try {
if(KnobRuntime.check(java.util.UUID.fromString("a97a7d3e-b338-3edc-ab2c-dfb50473381e"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("e5f4f2e1-cadf-330c-ae89-8eff55922d52"))) { shutdownWAL(); } else { walFactory.shutdown(); }
    } catch (IOException e) {
      LOG.warn("Failed to shutdown WAL", e);
    }
  }

  public void update(UpdateMasterRegion action) throws IOException {
    try {
      action.update(region);
      if (KnobRuntime.check(java.util.UUID.fromString("dcbafd8d-f9fe-30f5-90da-4cc4adbe1640"))) { flusherAndCompactor.resetChangesAfterLastFlush(); } else if (KnobRuntime.check(java.util.UUID.fromString("08054bbd-cd9d-3c34-8efd-ecf5a8e233cb"))) { flusherAndCompactor.recordLastFlushTime(); } else { flusherAndCompactor.onUpdate(); }
    } catch (WALSyncTimeoutIOException e) {
      LOG.error(HBaseMarkers.FATAL, "WAL sync timeout. Aborting server.");
      server.abort("WAL sync timeout", e);
      throw e;
    }
  }

  public Result get(Get get) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3c37b0d7-264e-3cf2-8cdc-61ce7a5fbb09"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35ef44f9-e111-3451-8814-f841ff6fb89a"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d926a302-ecd8-3c73-a92c-7aa70015eb7b"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(get);
    field.set(get, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2e690d1-3b5e-347d-89e7-056fbe881dac"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f00c80cc-95ee-3eef-91d2-b4a60810cc61"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bfdd6335-f7af-35a8-83bc-f71559e0abc8"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("434a380b-b09b-392a-9f19-7510b7b313a0"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80813a43-c990-330c-abad-81e2f4afe1bd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b508e445-e191-3a5b-8aeb-506f3dab4a7b"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c10348c1-a14f-3a6c-8471-3a8e11eaecd3"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46d2ad65-781c-376b-8711-eaff32b3f549"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e9adfa1-72e4-3c59-9f04-38a1150b592e"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71cbc2b5-72cb-3951-98dc-3f269c8e3718"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d22c5fac-17a5-32e2-9927-6b7e8ccbc462"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac952e08-7f45-3b10-ace5-98daf94f9521"))) {
try {
    java.lang.reflect.Field field = get.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(get));
    field.set(get, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return region.get(get);
  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    return new RegionScannerAsResultScanner(region.getScanner(scan));
  }

  public RegionScanner getRegionScanner(Scan scan) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c424c773-5e48-3fcc-8648-89bbecebff0f"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a77d6bff-9f23-32cc-8580-dedbdd2247a5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07ab0fc0-7587-3164-b78d-b34c09419c3e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("52dd0c67-3cc6-376d-85df-b87af3092599"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68d6c4d4-7bce-3eb0-8cb8-4885eebe38fc"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0026eea-1a3d-3043-b395-eb88c206ebe3"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1c2c7a3-72fb-3849-8a47-701f08aaeecb"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("24f67a8f-2ecc-3301-9026-bc0a4e3627ad"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eea26a1d-9dde-3b0c-b1ff-50d167380fba"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7c45d09-ccaf-3f9d-9af5-ebc3126d9506"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scan);
    field.set(scan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b2a4dbf-10de-3aff-bbb8-b913c290670d"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a9e9ca8-542c-31e2-974c-0410649ff000"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("mvccReadPoint");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("347f30dd-8b5a-3d97-b869-f198440034f5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c184f59-08a2-39fe-b008-c68bacea6b5e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d34f87aa-097d-3988-8154-c899563fa126"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("454a678a-fda0-3fce-97aa-bfdc2eb9e5e8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("45eaf2d4-eed1-338d-921f-06b62cd9dfa9"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9964a242-dd33-397c-9670-39d8d38cb60d"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2de2855b-f8ea-32c8-9828-0c8aec751ecb"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9975d1be-9007-3b9a-a65a-1910abae6703"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8a7f322-01dc-3569-89ec-5b880d181c01"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0dfc388f-443b-3c35-b5d7-46ed6e962a02"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33d0d480-7c8c-3525-8573-e07782196ae7"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("619f3cab-79a6-360f-b849-a121a6a70330"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56874c77-86f5-3dab-a053-59ef5134353c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6a7db4d-9e45-3ed8-b423-c72811c15fe1"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76378b07-6ca7-395e-a636-4f0cf88d2300"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("776447ca-019e-3b03-91da-6360d4c0f5f5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4238a27e-81cd-3d75-ac2b-9aa6e5cba423"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return region.getScanner(scan);
  }

  public FlushResult flush(boolean force) throws IOException {
    try {
      flusherAndCompactor.resetChangesAfterLastFlush();
      FlushResult flushResult = region.flush(force);
      flusherAndCompactor.recordLastFlushTime();
      return flushResult;
    } catch (WALSyncTimeoutIOException e) {
      LOG.error(HBaseMarkers.FATAL, "WAL sync timeout. Aborting server.");
      server.abort("WAL sync timeout", e);
      throw e;
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public void requestRollAll() {
    walRoller.requestRollAll();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public void waitUntilWalRollFinished() throws InterruptedException {
    walRoller.waitUntilWalRollFinished();
  }

  public void close(boolean abort) {
    LOG.info("Closing local region {}, isAbort={}", region.getRegionInfo(), abort);
    if (flusherAndCompactor != null) {
      flusherAndCompactor.close();
    }
    // if abort, we shutdown wal first to fail the ongoing updates to the region, and then close the
    // region, otherwise there will be dead lock.
    if (abort) {
      shutdownWAL();
      closeRegion(true);
    } else {
      closeRegion(false);
      shutdownWAL();
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("8cb32275-2269-36e1-9eb9-488659afd525"))) ? ((walRoller) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fb0d1788-7023-3ac5-8892-e812561be01b"))) ? ((walRoller) != (null)) : (walRoller != null))))) {
      walRoller.close();
    }
  }

  private static WAL createWAL(WALFactory walFactory, MasterRegionWALRoller walRoller,
    String serverName, FileSystem walFs, Path walRootDir, RegionInfo regionInfo)
    throws IOException {
    String logName = AbstractFSWALProvider.getWALDirectoryName(serverName);
    Path walDir = new Path(walRootDir, logName);
    LOG.debug("WALDir={}", walDir);
    if (walFs.exists(walDir)) {
      throw new HBaseIOException(
        "Already created wal directory at " + walDir + " for local region " + regionInfo);
    }
    if (!walFs.mkdirs(walDir)) {
      throw new IOException(
        "Can not create wal directory " + walDir + " for local region " + regionInfo);
    }
    WAL wal = walFactory.getWAL(regionInfo);
    walRoller.addWAL(wal);
    return wal;
  }

  private static HRegion bootstrap(Configuration conf, TableDescriptor td, FileSystem fs,
    Path rootDir, FileSystem walFs, Path walRootDir, WALFactory walFactory,
    MasterRegionWALRoller walRoller, String serverName, boolean touchInitializingFlag)
    throws IOException {
    TableName tn = td.getTableName();
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tn).setRegionId(REGION_ID).build();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tn);
    // persist table descriptor
    FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, td, true);
    HRegion.createHRegion(conf, regionInfo, fs, tableDir, td).close();
    Path initializedFlag = new Path(tableDir, INITIALIZED_FLAG);
    if (!fs.mkdirs(initializedFlag)) {
      throw new IOException("Can not touch initialized flag: " + initializedFlag);
    }
    Path initializingFlag = new Path(tableDir, INITIALIZING_FLAG);
    if (!fs.delete(initializingFlag, true)) {
      LOG.warn("failed to clean up initializing flag: " + initializingFlag);
    }
    WAL wal = createWAL(walFactory, walRoller, serverName, walFs, walRootDir, regionInfo);
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, td, wal, null, null);
  }

  private static RegionInfo loadRegionInfo(FileSystem fs, Path tableDir) throws IOException {
    // on branch-2, the RegionInfo.isEncodedRegionName will returns true for .initializing and
    // .initialized, see HBASE-25368. Since RegionInfo is IA.Public, changing the implementation may
    // raise compatibility concerns, so here we just skip them by our own.
    Path regionDir = fs.listStatus(tableDir, p -> !p.getName().startsWith(".")
      && RegionInfo.isEncodedRegionName(Bytes.toBytes(p.getName())))[0].getPath();
    return HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
  }

  private static HRegion open(Configuration conf, TableDescriptor td, RegionInfo regionInfo,
    FileSystem fs, Path rootDir, FileSystem walFs, Path walRootDir, WALFactory walFactory,
    MasterRegionWALRoller walRoller, String serverName) throws IOException {
    Path tableDir = CommonFSUtils.getTableDir(rootDir, td.getTableName());
    Path walRegionDir = FSUtils.getRegionDirFromRootDir(walRootDir, regionInfo);
    Path replayEditsDir = new Path(walRegionDir, REPLAY_EDITS_DIR);
    if (((KnobRuntime.check(java.util.UUID.fromString("fc8c1d22-9020-35b3-867c-e0d139e75630"))) ? ((!walFs.exists(replayEditsDir)) && (!walFs.mkdirs(replayEditsDir))) : (((KnobRuntime.check(java.util.UUID.fromString("e6662d6b-942c-3f68-b2eb-f8229b4d8007"))) ? ((!walFs.exists(replayEditsDir)) || (!walFs.mkdirs(replayEditsDir))) : (((KnobRuntime.check(java.util.UUID.fromString("6b48324f-1e32-3645-841b-b3dfa229a0b7"))) ? (!walFs.mkdirs(replayEditsDir)) : (((KnobRuntime.check(java.util.UUID.fromString("74d2f730-2b08-3d1c-8f57-9c3cadda5f4b"))) ? (!walFs.exists(replayEditsDir)) : (!walFs.exists(replayEditsDir) && !walFs.mkdirs(replayEditsDir)))))))))) {
      throw new IOException("Failed to create replay directory: " + replayEditsDir);
    }

    // Replay any WALs for the Master Region before opening it.
    Path walsDir = new Path(walRootDir, HREGION_LOGDIR_NAME);
    // In open(...), we expect that the WAL directory for the MasterRegion to already exist.
    // This is in contrast to bootstrap() where we create the MasterRegion data and WAL dir.
    // However, it's possible that users directly remove the WAL directory. We expect walsDir
    // to always exist in normal situations, but we should guard against users changing the
    // filesystem outside of HBase's line of sight.
    if (((KnobRuntime.check(java.util.UUID.fromString("24b36691-93cd-3bd8-95cd-ee1ff4db7fdd"))) ? (walFs.mkdirs(walsDir)) : (walFs.exists(walsDir)))) {
      replayWALs(conf, walFs, walRootDir, walsDir, regionInfo, serverName, replayEditsDir);
    } else {
      LOG.error(
        "UNEXPECTED: WAL directory for MasterRegion is missing." + " {} is unexpectedly missing.",
        walsDir);
    }

    // Create a new WAL
    WAL wal = createWAL(walFactory, walRoller, serverName, walFs, walRootDir, regionInfo);
    conf.set(HRegion.SPECIAL_RECOVERED_EDITS_DIR,
      replayEditsDir.makeQualified(walFs.getUri(), walFs.getWorkingDirectory()).toString());
    // we do not do WAL splitting here so it is possible to have uncleanly closed WAL files, so we
    // need to ignore EOFException.
    conf.setBoolean(HRegion.RECOVERED_EDITS_IGNORE_EOF, true);
    return HRegion.openHRegionFromTableDir(conf, fs, tableDir, regionInfo, td, wal, null, null);
  }

  private static void replayWALs(Configuration conf, FileSystem walFs, Path walRootDir,
    Path walsDir, RegionInfo regionInfo, String serverName, Path replayEditsDir)
    throws IOException {
    for (FileStatus walDir : walFs.listStatus(walsDir)) {
      if (!walDir.isDirectory()) {
        continue;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("8a9153e1-c189-3901-8212-0586a1c6f0bf"))) ? (walDir.getPath().getName().endsWith(serverName)) : (walDir.getPath().getName().startsWith(serverName)))) {
        LOG.warn("This should not happen in real production as we have not created our WAL "
          + "directory yet, ignore if you are running a local region related UT");
      }
      Path deadWALDir;
      if (!walDir.getPath().getName().endsWith(DEAD_WAL_DIR_SUFFIX)) {
        deadWALDir =
          new Path(walDir.getPath().getParent(), walDir.getPath().getName() + DEAD_WAL_DIR_SUFFIX);
        if (!walFs.rename(walDir.getPath(), deadWALDir)) {
          throw new IOException("Can not rename " + walDir + " to " + deadWALDir
            + " when recovering lease of proc store");
        }
        LOG.info("Renamed {} to {} as it is dead", walDir.getPath(), deadWALDir);
      } else {
        deadWALDir = walDir.getPath();
        if (KnobRuntime.check(java.util.UUID.fromString("a3913786-96a1-3558-9d4a-b0e8455c0918"))) { LOG.debug("{} is already marked as dead", deadWALDir); } else if (KnobRuntime.check(java.util.UUID.fromString("753c58dd-4580-32f0-a5c7-dcfe7db6bfc3"))) { LOG.error("{} is already marked as dead", deadWALDir); } else { LOG.info("{} is already marked as dead", deadWALDir); }
      }
if(KnobRuntime.check(java.util.UUID.fromString("6cd9c0ef-d06d-3405-ba61-46695b401864"))) {
throw new java.io.FileNotFoundException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("36489b59-9520-3d78-af91-8d8962ef2eb1"))) {
throw new java.io.IOException("Injected exception");
}
      for (FileStatus walFile : walFs.listStatus(deadWALDir)) {
        Path replayEditsFile = new Path(replayEditsDir, walFile.getPath().getName());
        RecoverLeaseFSUtils.recoverFileLease(walFs, walFile.getPath(), conf);
        if (!walFs.rename(walFile.getPath(), replayEditsFile)) {
          throw new IOException("Can not rename " + walFile.getPath() + " to " + replayEditsFile
            + " when recovering lease for local region");
        }
        LOG.info("Renamed {} to {}", walFile.getPath(), replayEditsFile);
      }
      LOG.info("Delete empty local region wal dir {}", deadWALDir);
      walFs.delete(deadWALDir, true);
    }
  }

  private static void tryMigrate(Configuration conf, FileSystem fs, Path tableDir,
    RegionInfo regionInfo, TableDescriptor oldTd, TableDescriptor newTd) throws IOException {
    Class<? extends StoreFileTracker> oldSft =
      StoreFileTrackerFactory.getTrackerClass(oldTd.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
    Class<? extends StoreFileTracker> newSft =
      StoreFileTrackerFactory.getTrackerClass(newTd.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
    if (oldSft.equals(newSft)) {
      LOG.debug("old store file tracker {} is the same with new store file tracker, skip migration",
        StoreFileTrackerFactory.getStoreFileTrackerName(oldSft));
      if (!oldTd.equals(newTd)) {
        // we may change other things such as adding a new family, so here we still need to persist
        // the new table descriptor
        LOG.info("Update table descriptor from {} to {}", oldTd, newTd);
        FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, newTd, true);
      }
      return;
    }
    LOG.info("Migrate store file tracker from {} to {}", oldSft.getSimpleName(),
      newSft.getSimpleName());
    HRegionFileSystem hfs =
      HRegionFileSystem.openRegionFromFileSystem(conf, fs, tableDir, regionInfo, false);
    for (ColumnFamilyDescriptor oldCfd : oldTd.getColumnFamilies()) {
      StoreFileTracker oldTracker = StoreFileTrackerFactory.create(conf, oldTd, oldCfd, hfs);
      StoreFileTracker newTracker = StoreFileTrackerFactory.create(conf, oldTd, oldCfd, hfs);
      List<StoreFileInfo> files = oldTracker.load();
      LOG.debug("Store file list for {}: {}", oldCfd.getNameAsString(), files);
      newTracker.set(oldTracker.load());
    }
    // persist the new table descriptor after migration
    LOG.info("Update table descriptor from {} to {}", oldTd, newTd);
    FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, newTd, true);
  }

  public static MasterRegion create(MasterRegionParams params) throws IOException {
    TableDescriptor td = params.tableDescriptor();
    LOG.info("Create or load local region for table " + td);
    Server server = params.server();
    Configuration baseConf = server.getConfiguration();
    FileSystem fs = CommonFSUtils.getRootDirFileSystem(baseConf);
    FileSystem walFs = CommonFSUtils.getWALFileSystem(baseConf);
    Path globalRootDir = CommonFSUtils.getRootDir(baseConf);
    Path globalWALRootDir = CommonFSUtils.getWALRootDir(baseConf);
    Path rootDir = new Path(globalRootDir, params.regionDirName());
    Path walRootDir = new Path(globalWALRootDir, params.regionDirName());
    // we will override some configurations so create a new one.
    Configuration conf = new Configuration(baseConf);
    CommonFSUtils.setRootDir(conf, rootDir);
    CommonFSUtils.setWALRootDir(conf, walRootDir);
    MasterRegionFlusherAndCompactor.setupConf(conf, params.flushSize(), params.flushPerChanges(),
      params.flushIntervalMs());
    conf.setInt(AbstractFSWAL.MAX_LOGS, params.maxWals());
    if (params.useHsync() != null) {
      conf.setBoolean(HRegion.WAL_HSYNC_CONF_KEY, params.useHsync());
    }
    if (params.useMetaCellComparator() != null) {
      conf.setBoolean(HRegion.USE_META_CELL_COMPARATOR, params.useMetaCellComparator());
    }
    conf.setInt(AbstractFSWAL.RING_BUFFER_SLOT_COUNT,
      IntMath.ceilingPowerOfTwo(params.ringBufferSlotCount()));

    MasterRegionWALRoller walRoller = MasterRegionWALRoller.create(
      td.getTableName() + "-WAL-Roller", conf, server, walFs, walRootDir, globalWALRootDir,
      params.archivedWalSuffix(), params.rollPeriodMs(), params.flushSize());
    walRoller.start();

    WALFactory walFactory = new WALFactory(conf, server.getServerName().toString());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, td.getTableName());
    Path initializingFlag = new Path(tableDir, INITIALIZING_FLAG);
    Path initializedFlag = new Path(tableDir, INITIALIZED_FLAG);
    HRegion region;
    if (!fs.exists(tableDir)) {
      // bootstrap, no doubt
      if (!fs.mkdirs(initializedFlag)) {
        throw new IOException("Can not touch initialized flag");
      }
      region = bootstrap(conf, td, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
        server.getServerName().toString(), true);
    } else {
      if (!fs.exists(initializedFlag)) {
        if (!fs.exists(initializingFlag)) {
          // should be old style, where we do not have the initializing or initialized file, persist
          // the table descriptor, touch the initialized flag and then open the region.
          // the store file tracker must be DEFAULT
          LOG.info("No {} or {} file, try upgrading", INITIALIZING_FLAG, INITIALIZED_FLAG);
          TableDescriptor oldTd =
            TableDescriptorBuilder.newBuilder(td).setValue(StoreFileTrackerFactory.TRACKER_IMPL,
              StoreFileTrackerFactory.Trackers.DEFAULT.name()).build();
          FSTableDescriptors.createTableDescriptorForTableDirectory(fs, tableDir, oldTd, true);
          if (!fs.mkdirs(initializedFlag)) {
            throw new IOException("Can not touch initialized flag: " + initializedFlag);
          }
          RegionInfo regionInfo = loadRegionInfo(fs, tableDir);
          tryMigrate(conf, fs, tableDir, regionInfo, oldTd, td);
          region = open(conf, td, regionInfo, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
            server.getServerName().toString());
        } else {
          // delete all contents besides the initializing flag, here we can make sure tableDir
          // exists(unless someone delete it manually...), so we do not do null check here.
          for (FileStatus status : fs.listStatus(tableDir)) {
            if (!status.getPath().getName().equals(INITIALIZING_FLAG)) {
              fs.delete(status.getPath(), true);
            }
          }
          region = bootstrap(conf, td, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
            server.getServerName().toString(), false);
        }
      } else {
        if (((KnobRuntime.check(java.util.UUID.fromString("4a511b97-a2c1-3e7b-98e8-cbfdb0551f6a"))) ? (fs.exists(initializingFlag)) : (((KnobRuntime.check(java.util.UUID.fromString("b6a87e9c-c747-3b71-bbc3-f2a2610bf1c0"))) ? ((fs.mkdirs(initializingFlag)) || (!fs.delete(initializingFlag, true))) : (((KnobRuntime.check(java.util.UUID.fromString("7e35d058-c3dc-381c-a1af-640de1a08efa"))) ? ((fs.exists(initializingFlag)) && (!fs.delete(initializingFlag, true))) : (((KnobRuntime.check(java.util.UUID.fromString("8ce778a8-6634-3284-b2a8-8e824b607d54"))) ? (!fs.delete(initializingFlag, true)) : (((KnobRuntime.check(java.util.UUID.fromString("c998c3ec-bded-392d-98f1-ebb6c67545e8"))) ? (fs.mkdirs(initializingFlag)) : (((KnobRuntime.check(java.util.UUID.fromString("1a6001e6-b493-33fb-a148-47284e042534"))) ? ((fs.exists(initializingFlag)) || (!fs.delete(initializingFlag, true))) : (((KnobRuntime.check(java.util.UUID.fromString("667acdae-7d36-35c6-b5e6-4860c4a23837"))) ? ((fs.mkdirs(initializingFlag)) && (!fs.delete(initializingFlag, true))) : (fs.exists(initializingFlag) && !fs.delete(initializingFlag, true)))))))))))))))) {
          if (KnobRuntime.check(java.util.UUID.fromString("895b7efd-16a4-38df-a09f-8c0d6c98e34d"))) { LOG.info(("failed to clean up initializing flag: ") + (initializingFlag)); } else if (KnobRuntime.check(java.util.UUID.fromString("6fdc0166-fb9d-3c10-baa1-c9a439cc2bc9"))) { LOG.warn(("failed to clean up initializing flag: ") + (initializingFlag)); } else if (KnobRuntime.check(java.util.UUID.fromString("4a751a8a-32a4-355b-afc8-da794946c5b4"))) { LOG.warn("failed to clean up initializing flag: "); } else if (KnobRuntime.check(java.util.UUID.fromString("7b7a4a3a-dda5-3b9b-95c6-35f3b3e08cf7"))) { LOG.info("failed to clean up initializing flag: "); } else { LOG.warn("failed to clean up initializing flag: " + initializingFlag); }
if(KnobRuntime.check(java.util.UUID.fromString("4e15207a-68e7-3f34-944b-c5f57832588a"))) {
throw new java.io.IOException("Injected exception");
}
        }
        // open it, make sure to load the table descriptor from fs
if(KnobRuntime.check(java.util.UUID.fromString("425d25e6-d4e4-3a12-a6a9-d2d0cfcd08e3"))) {
throw new java.io.IOException("Injected exception");
}
        TableDescriptor oldTd = FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir);
if(KnobRuntime.check(java.util.UUID.fromString("258ada98-9a64-34fa-835e-45c6ebbe8cc1"))) {
throw new java.io.IOException("Injected exception");
}
        RegionInfo regionInfo = loadRegionInfo(fs, tableDir);
if(KnobRuntime.check(java.util.UUID.fromString("e36cc883-5c01-362a-9e9a-945f2df46900"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("319fff2c-fe52-3d11-8ca3-344ba9b7fe99"))) {
throw new java.io.IOException("Injected exception");
}
        tryMigrate(conf, fs, tableDir, regionInfo, oldTd, td);
if(KnobRuntime.check(java.util.UUID.fromString("9e89a922-8444-354f-83f7-25f1307e3a38"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5df3e89c-a740-3d7b-a921-701b33440539"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e65f7a75-cec0-3799-882f-e90c0711d245"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57bd9b31-4443-3d24-8b99-dcd00608ba73"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d03e2c60-e268-3550-a010-08631fe1d455"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5443bf1-0066-3484-94e6-dcc447b52d90"))) {
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
        region = open(conf, td, regionInfo, fs, rootDir, walFs, walRootDir, walFactory, walRoller,
          server.getServerName().toString());
      }
    }

if(KnobRuntime.check(java.util.UUID.fromString("dc00f431-117c-371f-959b-e867a175a6fc"))) {
throw new java.io.IOException("Injected exception");
}
    Path globalArchiveDir = HFileArchiveUtil.getArchivePath(baseConf);
    MasterRegionFlusherAndCompactor flusherAndCompactor = new MasterRegionFlusherAndCompactor(conf,
      server, region, params.flushSize(), params.flushPerChanges(), params.flushIntervalMs(),
      params.compactMin(), globalArchiveDir, params.archivedHFileSuffix());
if(KnobRuntime.check(java.util.UUID.fromString("1d59105f-be00-3954-bf7a-82797d398164"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactRequest");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(flusherAndCompactor);
    field.set(flusherAndCompactor, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a8b2cd8-8875-31ce-ba64-39c9290f2fad"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("lastFlushTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c90eeda5-e7f5-36da-969f-e3ef7b12eb27"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("closed");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(flusherAndCompactor);
    field.set(flusherAndCompactor, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16c23638-178c-31de-b037-e133c6c4a8bf"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("858100b7-89a5-3d20-833a-a754488b96b8"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bdebfbb0-dd0b-3436-9300-50306d6a64ae"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f773a8aa-44af-32ea-a972-1eba862e171b"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushRequest");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(flusherAndCompactor);
    field.set(flusherAndCompactor, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41b39893-29eb-3d93-9a9e-f626d2c31bb5"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushIntervalMs");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0419782e-011a-3f58-9b3c-c6831e191aa8"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3f152d48-a13b-38c0-817c-d9312ff9939b"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67ef524c-07c1-3abb-b0b9-66642a742904"))) {
try {
    java.lang.reflect.Field field = flusherAndCompactor.getClass().getDeclaredField("compactMin");
    field.setAccessible(true);
    int oldValue = ((int)field.get(flusherAndCompactor));
    field.set(flusherAndCompactor, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    walRoller.setFlusherAndCompactor(flusherAndCompactor);
if(KnobRuntime.check(java.util.UUID.fromString("17246a31-2abc-3ce2-90c2-3bdd75bdedc5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("284b901f-cb50-3207-8dc1-161c9a83c89c"))) {
throw new java.io.IOException("Injected exception");
}
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    if (!fs.mkdirs(archiveDir)) {
      LOG.warn("Failed to create archive directory {}. Usually this should not happen but it will"
        + " be created again when we actually archive the hfiles later, so continue", archiveDir);
    }
    return new MasterRegion(server, region, walFactory, flusherAndCompactor, walRoller);
  }
}
