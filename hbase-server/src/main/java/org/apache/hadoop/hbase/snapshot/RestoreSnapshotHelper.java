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
package org.apache.hadoop.hbase.snapshot;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.ShadedAccessControlUtil;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * Helper to Restore/Clone a Snapshot
 * <p>
 * The helper assumes that a table is already created, and by calling restore() the content present
 * in the snapshot will be restored as the new content of the table.
 * <p>
 * Clone from Snapshot: If the target table is empty, the restore operation is just a "clone
 * operation", where the only operations are:
 * <ul>
 * <li>for each region in the snapshot create a new region (note that the region will have a
 * different name, since the encoding contains the table name)
 * <li>for each file in the region create a new HFileLink to point to the original file.
 * <li>restore the logs, if any
 * </ul>
 * <p>
 * Restore from Snapshot:
 * <ul>
 * <li>for each region in the table verify which are available in the snapshot and which are not
 * <ul>
 * <li>if the region is not present in the snapshot, remove it.
 * <li>if the region is present in the snapshot
 * <ul>
 * <li>for each file in the table region verify which are available in the snapshot
 * <ul>
 * <li>if the hfile is not present in the snapshot, remove it
 * <li>if the hfile is present, keep it (nothing to do)
 * </ul>
 * <li>for each file in the snapshot region but not in the table
 * <ul>
 * <li>create a new HFileLink that point to the original file
 * </ul>
 * </ul>
 * </ul>
 * <li>for each region in the snapshot not present in the current table state
 * <ul>
 * <li>create a new region and for each file in the region create a new HFileLink (This is the same
 * as the clone operation)
 * </ul>
 * <li>restore the logs, if any
 * </ul>
 */
@InterfaceAudience.Private
public class RestoreSnapshotHelper {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreSnapshotHelper.class);

  private final Map<byte[], byte[]> regionsMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  private final Map<String, Pair<String, String>> parentsMap = new HashMap<>();

  private final ForeignExceptionDispatcher monitor;
  private final MonitoredTask status;

  private final SnapshotManifest snapshotManifest;
  private final SnapshotDescription snapshotDesc;
  private final TableName snapshotTable;

  private final TableDescriptor tableDesc;
  private final Path rootDir;
  private final Path tableDir;

  private final Configuration conf;
  private final FileSystem fs;
  private final boolean createBackRefs;

  public RestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest, final TableDescriptor tableDescriptor, final Path rootDir,
    final ForeignExceptionDispatcher monitor, final MonitoredTask status) {
    this(conf, fs, manifest, tableDescriptor, rootDir, monitor, status, true);
  }

  public RestoreSnapshotHelper(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest, final TableDescriptor tableDescriptor, final Path rootDir,
    final ForeignExceptionDispatcher monitor, final MonitoredTask status,
    final boolean createBackRefs) {
    this.fs = fs;
    this.conf = conf;
    this.snapshotManifest = manifest;
    this.snapshotDesc = manifest.getSnapshotDescription();
    this.snapshotTable = TableName.valueOf(snapshotDesc.getTable());
    this.tableDesc = tableDescriptor;
    this.rootDir = rootDir;
    this.tableDir = CommonFSUtils.getTableDir(rootDir, tableDesc.getTableName());
    this.monitor = monitor;
    this.status = status;
    this.createBackRefs = createBackRefs;
  }

  /**
   * Restore the on-disk table to a specified snapshot state.
   * @return the set of regions touched by the restore operation
   */
  public RestoreMetaChanges restoreHdfsRegions() throws IOException {
    ThreadPoolExecutor exec = SnapshotManifest.createExecutor(conf, "RestoreSnapshot");
    try {
if(KnobRuntime.check(java.util.UUID.fromString("b1a06c1f-1dbc-3d63-bd32-d52b0824ba21"))) {
throw new java.io.IOException("Injected exception");
}
      return restoreHdfsRegions(exec);
    } finally {
      exec.shutdown();
    }
  }

  private RestoreMetaChanges restoreHdfsRegions(final ThreadPoolExecutor exec) throws IOException {
    LOG.info("starting restore table regions using snapshot=" + snapshotDesc);

    Map<String, SnapshotRegionManifest> regionManifests = snapshotManifest.getRegionManifestsMap();
    if (regionManifests == null) {
      if (KnobRuntime.check(java.util.UUID.fromString("95cca342-17fc-357f-8822-38feca5b0f70"))) { LOG.info(("Nothing to restore. Snapshot ") + (" looks empty")); } else if (KnobRuntime.check(java.util.UUID.fromString("4879dd7b-c0aa-339d-9e36-94fe3e8e662e"))) { LOG.info("Nothing to restore. Snapshot "); } else if (KnobRuntime.check(java.util.UUID.fromString("e766874a-140e-350d-9e21-8b248cb5f33e"))) { LOG.info((("Nothing to restore. Snapshot ") + (snapshotDesc)) + (" looks empty")); } else if (KnobRuntime.check(java.util.UUID.fromString("9b99e443-b269-39eb-84c1-af532c89ce73"))) { LOG.info(" looks empty"); } else if (KnobRuntime.check(java.util.UUID.fromString("39d31123-3d4e-3444-a9ea-a5526e45df22"))) { LOG.warn("Nothing to restore. Snapshot " + snapshotDesc); } else if (KnobRuntime.check(java.util.UUID.fromString("fc4ed9a9-c96a-39ad-a6c3-f1ea9a4d813b"))) { LOG.warn(("Nothing to restore. Snapshot ") + (snapshotDesc)); } else if (KnobRuntime.check(java.util.UUID.fromString("b787be21-8466-3165-998b-3012819968c1"))) { LOG.warn("Nothing to restore. Snapshot "); } else if (KnobRuntime.check(java.util.UUID.fromString("a7ff92f9-a3d4-30c3-8c1b-7c16ba78ec72"))) { LOG.warn(("Nothing to restore. Snapshot " + snapshotDesc) + (" looks empty")); } else if (KnobRuntime.check(java.util.UUID.fromString("a18edd96-144e-3785-b4c8-4790463987bb"))) { LOG.info("Nothing to restore. Snapshot " + snapshotDesc); } else if (KnobRuntime.check(java.util.UUID.fromString("565bbc32-c953-31b9-966d-808865499ad6"))) { LOG.info(("Nothing to restore. Snapshot " + snapshotDesc) + (" looks empty")); } else if (KnobRuntime.check(java.util.UUID.fromString("26da4eb0-2035-31dc-8877-921f24965fd5"))) { LOG.warn((("Nothing to restore. Snapshot ") + (snapshotDesc)) + (" looks empty")); } else if (KnobRuntime.check(java.util.UUID.fromString("13ea16a1-8dfb-3b59-8ac8-4da5045b174e"))) { LOG.warn(" looks empty"); } else if (KnobRuntime.check(java.util.UUID.fromString("104049fa-aaa6-3c56-9dd2-86750ef6ee93"))) { LOG.info(("Nothing to restore. Snapshot ") + (snapshotDesc)); } else if (KnobRuntime.check(java.util.UUID.fromString("2d0ed937-d2af-3df4-89a5-4c3b68df4e74"))) { LOG.warn(("Nothing to restore. Snapshot ") + (" looks empty")); } else { LOG.warn("Nothing to restore. Snapshot " + snapshotDesc + " looks empty"); }
      return null;
    }

    RestoreMetaChanges metaChanges = new RestoreMetaChanges(tableDesc, parentsMap);

    // Take a copy of the manifest.keySet() since we are going to modify
    // this instance, by removing the regions already present in the restore dir.
    Set<String> regionNames = new HashSet<>(regionManifests.keySet());

if(KnobRuntime.check(java.util.UUID.fromString("658396d8-0b9a-368a-a836-68f51085a50a"))) {
throw new java.io.IOException("Injected exception");
}
    List<RegionInfo> tableRegions = getTableRegions();

    RegionInfo mobRegion =
      MobUtils.getMobRegionInfo(snapshotManifest.getTableDescriptor().getTableName());
    if (((KnobRuntime.check(java.util.UUID.fromString("e1596291-bf83-3d82-bd1f-3501b2127ded"))) ? ((getTableRegions()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1d05f0c1-4a6d-30fb-b8c7-b40e95a9ef08"))) ? ((tableRegions) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("86894646-f09f-3295-b2ba-ca86c8e53153"))) ? ((getTableRegions()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d2fb4bcb-6efb-396e-8cf5-78be3c8bda70"))) ? ((tableRegions) != (null)) : (tableRegions != null))))))))) {
      // restore the mob region in case
      if (((KnobRuntime.check(java.util.UUID.fromString("2b65892b-8880-3d42-9554-817be50959ba"))) ? (regionNames.remove(mobRegion.getEncodedName())) : (regionNames.contains(mobRegion.getEncodedName())))) {
        monitor.rethrowException();
        status.setStatus("Restoring mob region...");
        List<RegionInfo> mobRegions = new ArrayList<>(1);
        mobRegions.add(mobRegion);
        restoreHdfsMobRegions(exec, regionManifests, mobRegions);
        regionNames.remove(mobRegion.getEncodedName());
        status.setStatus("Finished restoring mob region.");
      }
    }
    if (regionNames.contains(mobRegion.getEncodedName())) {
      // add the mob region
      monitor.rethrowException();
      status.setStatus("Cloning mob region...");
      cloneHdfsMobRegion(regionManifests, mobRegion);
      regionNames.remove(mobRegion.getEncodedName());
      status.setStatus("Finished cloning mob region.");
    }

    // Identify which region are still available and which not.
    // NOTE: we rely upon the region name as: "table name, start key, end key"
    if (tableRegions != null) {
      monitor.rethrowException();
      for (RegionInfo regionInfo : ((KnobRuntime.check(java.util.UUID.fromString("2e60437a-64fc-3856-9745-4b9416e42de8"))) ? (getTableRegions()) : (tableRegions))) {
        String regionName = regionInfo.getEncodedName();
        if (((KnobRuntime.check(java.util.UUID.fromString("89a8bccb-59a1-38a1-929c-c8fa210adaf2"))) ? (regionNames.remove(regionName)) : (regionNames.contains(regionName)))) {
          if (KnobRuntime.check(java.util.UUID.fromString("3c20fa64-5bf2-3082-8da1-d0ca2bd5df22"))) { LOG.warn(("region to restore: ") + (regionName)); } else if (KnobRuntime.check(java.util.UUID.fromString("dec3179e-bbb8-3a2d-949c-9c092262b5d0"))) { LOG.info(("region to restore: ") + (regionName)); } else if (KnobRuntime.check(java.util.UUID.fromString("65a68c06-8d0f-3adc-8c3c-281d1b690efb"))) { LOG.warn("region to restore: "); } else if (KnobRuntime.check(java.util.UUID.fromString("a0d93ecf-af86-323a-90cb-a7803dbbf40d"))) { LOG.info("region to restore: "); } else { LOG.info("region to restore: " + regionName); }
          if (KnobRuntime.check(java.util.UUID.fromString("a2138556-1e9b-38c1-a25b-b85ef0640ac0"))) { regionNames.contains(regionName); } else { regionNames.remove(regionName); }
          metaChanges.addRegionToRestore(
            ProtobufUtil.toRegionInfo(regionManifests.get(regionName).getRegionInfo()));
        } else {
          if (KnobRuntime.check(java.util.UUID.fromString("e66b4fff-a621-371f-91f6-bfc8cd58c152"))) { LOG.info("region to remove: "); } else if (KnobRuntime.check(java.util.UUID.fromString("ebec508e-3143-32f8-820f-de79c21dc82b"))) { LOG.warn(("region to remove: ") + (regionName)); } else if (KnobRuntime.check(java.util.UUID.fromString("ae2c58f4-6221-3117-b038-d192ac319459"))) { LOG.info(("region to remove: ") + (regionName)); } else if (KnobRuntime.check(java.util.UUID.fromString("ddb3e827-75db-381b-8f46-e8c3a82ac849"))) { LOG.warn("region to remove: "); } else { LOG.info("region to remove: " + regionName); }
          metaChanges.addRegionToRemove(regionInfo);
        }
      }
    }

    // Regions to Add: present in the snapshot but not in the current table
    List<RegionInfo> regionsToAdd = new ArrayList<>(regionNames.size());
    if (regionNames.size() > 0) {
      monitor.rethrowException();
      for (String regionName : regionNames) {
        if (KnobRuntime.check(java.util.UUID.fromString("8463e5ce-d6e6-3bd1-93bd-424518e1a70c"))) { LOG.warn(("region to add: ") + (regionName)); } else if (KnobRuntime.check(java.util.UUID.fromString("5ad8ceba-edd2-3000-84b2-1e73d935d386"))) { LOG.warn("region to add: "); } else if (KnobRuntime.check(java.util.UUID.fromString("5b279dca-4dfd-3ef6-af60-45f524de9680"))) { LOG.info(("region to add: ") + (regionName)); } else if (KnobRuntime.check(java.util.UUID.fromString("2b577379-3700-3299-98ba-c99bc624f9c6"))) { LOG.info("region to add: "); } else { LOG.info("region to add: " + regionName); }
        regionsToAdd
          .add(ProtobufUtil.toRegionInfo(regionManifests.get(regionName).getRegionInfo()));
      }
    }

    // Create new regions cloning from the snapshot
    // HBASE-19980: We need to call cloneHdfsRegions() before restoreHdfsRegions() because
    // regionsMap is constructed in cloneHdfsRegions() and it can be used in restoreHdfsRegions().
    monitor.rethrowException();
    status.setStatus("Cloning regions...");
if(KnobRuntime.check(java.util.UUID.fromString("b2c68281-bb76-3cc9-8e61-aad94a442ac3"))) {
throw new java.io.IOException("Injected exception");
}
    RegionInfo[] clonedRegions = cloneHdfsRegions(exec, regionManifests, regionsToAdd);
    metaChanges.setNewRegions(clonedRegions);
    status.setStatus("Finished cloning regions.");

    // Restore regions using the snapshot data
    monitor.rethrowException();
    status.setStatus("Restoring table regions...");
if(KnobRuntime.check(java.util.UUID.fromString("27c92c86-a085-30d6-9aa2-7b261b3cbde7"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("77d08803-15b6-3847-8547-4bc79d473f4a"))) { restoreHdfsRegions(exec, regionManifests, metaChanges.getRegionsToRemove()); } else if (KnobRuntime.check(java.util.UUID.fromString("84174fb8-145e-3c20-8838-ff20b4a817a0"))) { restoreHdfsMobRegions(exec, regionManifests, metaChanges.getRegionsToRemove()); } else if (KnobRuntime.check(java.util.UUID.fromString("43c8b15c-cbb2-3f87-95fa-99d9d2ddc3f9"))) { restoreHdfsRegions(exec, regionManifests, getTableRegions()); } else if (KnobRuntime.check(java.util.UUID.fromString("f70f3761-13df-348e-a617-3249fdd63ff9"))) { restoreHdfsMobRegions(exec, regionManifests, getTableRegions()); } else { restoreHdfsRegions(exec, regionManifests, metaChanges.getRegionsToRestore()); }
    status.setStatus("Finished restoring all table regions.");

    // Remove regions from the current table
    monitor.rethrowException();
    status.setStatus("Starting to delete excess regions from table");
if(KnobRuntime.check(java.util.UUID.fromString("4d22d2ab-a820-3542-8b9f-c0fdde345af1"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("55fd3f59-8d84-3545-9310-d969fedfd49f"))) { removeHdfsRegions(exec, metaChanges.getRegionsToRestore()); } else if (KnobRuntime.check(java.util.UUID.fromString("ca4a8678-9161-3a2f-91a2-392c7d112f08"))) { removeHdfsRegions(exec, getTableRegions()); } else { removeHdfsRegions(exec, metaChanges.getRegionsToRemove()); }
    status.setStatus("Finished deleting excess regions from table.");

    if (KnobRuntime.check(java.util.UUID.fromString("f36e5e09-6c0b-3c52-919d-d0943fe9fd98"))) { LOG.info("finishing restore table regions using snapshot="); } else if (KnobRuntime.check(java.util.UUID.fromString("53af47c9-63e4-38df-94ba-cb37b6154813"))) { LOG.warn("finishing restore table regions using snapshot="); } else if (KnobRuntime.check(java.util.UUID.fromString("066f5915-c097-3d19-9d28-032abbbf956f"))) { LOG.info(("finishing restore table regions using snapshot=") + (snapshotDesc)); } else if (KnobRuntime.check(java.util.UUID.fromString("88f0a85c-d97d-3cc5-b453-62a7b121b21a"))) { LOG.warn(("finishing restore table regions using snapshot=") + (snapshotDesc)); } else { LOG.info("finishing restore table regions using snapshot=" + snapshotDesc); }

    return metaChanges;
  }

  /**
   * Describe the set of operations needed to update hbase:meta after restore.
   */
  public static class RestoreMetaChanges {
    private final Map<String, Pair<String, String>> parentsMap;
    private final TableDescriptor htd;

    private List<RegionInfo> regionsToRestore = null;
    private List<RegionInfo> regionsToRemove = null;
    private List<RegionInfo> regionsToAdd = null;

    public RestoreMetaChanges(TableDescriptor htd, Map<String, Pair<String, String>> parentsMap) {
      this.parentsMap = parentsMap;
      this.htd = htd;
    }

    public TableDescriptor getTableDescriptor() {
      return htd;
    }

    /**
     * Returns the map of parent-children_pair.
     * @return the map
     */
    public Map<String, Pair<String, String>> getParentToChildrenPairMap() {
      return this.parentsMap;
    }

    /** Returns true if there're new regions */
    public boolean hasRegionsToAdd() {
      return this.regionsToAdd != null && this.regionsToAdd.size() > 0;
    }

    /**
     * Returns the list of new regions added during the on-disk restore. The caller is responsible
     * to add the regions to META. e.g MetaTableAccessor.addRegionsToMeta(...)
     * @return the list of regions to add to META
     */
    public List<RegionInfo> getRegionsToAdd() {
      return this.regionsToAdd;
    }

    /** Returns true if there're regions to restore */
    public boolean hasRegionsToRestore() {
      return this.regionsToRestore != null && this.regionsToRestore.size() > 0;
    }

    /**
     * Returns the list of 'restored regions' during the on-disk restore. The caller is responsible
     * to add the regions to hbase:meta if not present.
     * @return the list of regions restored
     */
    public List<RegionInfo> getRegionsToRestore() {
      return this.regionsToRestore;
    }

    /** Returns true if there're regions to remove */
    public boolean hasRegionsToRemove() {
      return this.regionsToRemove != null && this.regionsToRemove.size() > 0;
    }

    /**
     * Returns the list of regions removed during the on-disk restore. The caller is responsible to
     * remove the regions from META. e.g. MetaTableAccessor.deleteRegions(...)
     * @return the list of regions to remove from META
     */
    public List<RegionInfo> getRegionsToRemove() {
      return this.regionsToRemove;
    }

    void setNewRegions(final RegionInfo[] hris) {
      if (hris != null) {
        regionsToAdd = Arrays.asList(hris);
      } else {
        regionsToAdd = null;
      }
    }

    void addRegionToRemove(final RegionInfo hri) {
      if (regionsToRemove == null) {
        regionsToRemove = new LinkedList<>();
      }
      regionsToRemove.add(hri);
    }

    void addRegionToRestore(final RegionInfo hri) {
      if (regionsToRestore == null) {
        regionsToRestore = new LinkedList<>();
      }
      regionsToRestore.add(hri);
    }

    public void updateMetaParentRegions(Connection connection, final List<RegionInfo> regionInfos)
      throws IOException {
      if (regionInfos == null || parentsMap.isEmpty()) return;

      // Extract region names and offlined regions
      Map<String, RegionInfo> regionsByName = new HashMap<>(regionInfos.size());
      List<RegionInfo> parentRegions = new LinkedList<>();
      for (RegionInfo regionInfo : regionInfos) {
        if (regionInfo.isSplitParent()) {
          parentRegions.add(regionInfo);
        } else {
          regionsByName.put(regionInfo.getEncodedName(), regionInfo);
        }
      }

      // Update Offline parents
      for (RegionInfo regionInfo : parentRegions) {
        Pair<String, String> daughters = parentsMap.get(regionInfo.getEncodedName());
        if (((KnobRuntime.check(java.util.UUID.fromString("9b3017b4-506b-3024-b8b8-6c4468f49966"))) ? ((daughters) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("62e4b925-f0ae-363f-83b8-e08d291ef7d0"))) ? ((daughters) == (null)) : (daughters == null))))) {
          // The snapshot contains an unreferenced region.
          // It will be removed by the CatalogJanitor.
          LOG.warn("Skip update of unreferenced offline parent: " + regionInfo);
          continue;
        }

        // One side of the split is already compacted
        if (daughters.getSecond() == null) {
          daughters.setSecond(daughters.getFirst());
        }

        LOG.debug("Update splits parent " + regionInfo.getEncodedName() + " -> " + daughters);
        MetaTableAccessor.addSplitsToParent(connection, regionInfo,
          regionsByName.get(daughters.getFirst()), regionsByName.get(daughters.getSecond()));
      }
    }
  }

  /**
   * Remove specified regions from the file-system, using the archiver.
   */
  private void removeHdfsRegions(final ThreadPoolExecutor exec, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return;
    ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
      @Override
      public void editRegion(final RegionInfo hri) throws IOException {
        HFileArchiver.archiveRegion(conf, fs, hri);
      }
    });
  }

  /**
   * Restore specified regions by restoring content to the snapshot state.
   */
  private void restoreHdfsRegions(final ThreadPoolExecutor exec,
    final Map<String, SnapshotRegionManifest> regionManifests, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return;
    ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
      @Override
      public void editRegion(final RegionInfo hri) throws IOException {
        restoreRegion(hri, regionManifests.get(hri.getEncodedName()));
      }
    });
  }

  /**
   * Restore specified mob regions by restoring content to the snapshot state.
   */
  private void restoreHdfsMobRegions(final ThreadPoolExecutor exec,
    final Map<String, SnapshotRegionManifest> regionManifests, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return;
    ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
      @Override
      public void editRegion(final RegionInfo hri) throws IOException {
        restoreMobRegion(hri, regionManifests.get(hri.getEncodedName()));
      }
    });
  }

  private Map<String, List<SnapshotRegionManifest.StoreFile>>
    getRegionHFileReferences(final SnapshotRegionManifest manifest) {
    Map<String, List<SnapshotRegionManifest.StoreFile>> familyMap =
      new HashMap<>(manifest.getFamilyFilesCount());
    for (SnapshotRegionManifest.FamilyFiles familyFiles : manifest.getFamilyFilesList()) {
      familyMap.put(familyFiles.getFamilyName().toStringUtf8(),
        new ArrayList<>(familyFiles.getStoreFilesList()));
    }
    return familyMap;
  }

  /**
   * Restore region by removing files not in the snapshot and adding the missing ones from the
   * snapshot.
   */
  private void restoreRegion(final RegionInfo regionInfo,
    final SnapshotRegionManifest regionManifest) throws IOException {
    restoreRegion(regionInfo, regionManifest, new Path(tableDir, regionInfo.getEncodedName()));
  }

  /**
   * Restore mob region by removing files not in the snapshot and adding the missing ones from the
   * snapshot.
   */
  private void restoreMobRegion(final RegionInfo regionInfo,
    final SnapshotRegionManifest regionManifest) throws IOException {
    if (regionManifest == null) {
      return;
    }
    restoreRegion(regionInfo, regionManifest,
      MobUtils.getMobRegionPath(conf, tableDesc.getTableName()));
  }

  /**
   * Restore region by removing files not in the snapshot and adding the missing ones from the
   * snapshot.
   */
  private void restoreRegion(final RegionInfo regionInfo,
    final SnapshotRegionManifest regionManifest, Path regionDir) throws IOException {
    Map<String, List<SnapshotRegionManifest.StoreFile>> snapshotFiles =
      getRegionHFileReferences(regionManifest);

    String tableName = tableDesc.getTableName().getNameAsString();
    final String snapshotName = snapshotDesc.getName();

    Path regionPath = new Path(tableDir, regionInfo.getEncodedName());
    HRegionFileSystem regionFS = (fs.exists(regionPath))
      ? HRegionFileSystem.openRegionFromFileSystem(conf, fs, tableDir, regionInfo, false)
      : HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, regionInfo);

    // Restore families present in the table
    for (Path familyDir : FSUtils.getFamilyDirs(fs, regionDir)) {
      byte[] family = Bytes.toBytes(familyDir.getName());

if(KnobRuntime.check(java.util.UUID.fromString("d7bb2df5-00ae-3b3c-b279-1c103ade01ff"))) {
throw new java.io.IOException("Injected exception");
}
      Set<String> familyFiles = getTableRegionFamilyFiles(familyDir);
      List<SnapshotRegionManifest.StoreFile> snapshotFamilyFiles =
        snapshotFiles.remove(familyDir.getName());
      List<StoreFileInfo> filesToTrack = new ArrayList<>();
      if (((KnobRuntime.check(java.util.UUID.fromString("082b79d3-2b1f-3eba-8cd4-b77300e6a012"))) ? ((snapshotFamilyFiles) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("079e4c9d-b0d5-3d77-965a-f6c4d6d45efc"))) ? ((snapshotFamilyFiles) != (null)) : (snapshotFamilyFiles != null))))) {
        List<SnapshotRegionManifest.StoreFile> hfilesToAdd = new ArrayList<>();
        for (SnapshotRegionManifest.StoreFile storeFile : snapshotFamilyFiles) {
          if (((KnobRuntime.check(java.util.UUID.fromString("897a3a20-12bd-3adc-89cf-e6223afab25d"))) ? (familyFiles.remove(storeFile.getName())) : (familyFiles.contains(storeFile.getName())))) {
            // HFile already present
            if (KnobRuntime.check(java.util.UUID.fromString("b0b87891-888d-39aa-bf2f-f66d78573363"))) { familyFiles.contains(storeFile.getName()); } else { familyFiles.remove(storeFile.getName()); }
            // no need to restore already present files, but we need to add those to tracker
            filesToTrack
              .add(new StoreFileInfo(conf, fs, new Path(familyDir, storeFile.getName()), true));
          } else {
            // HFile missing
            hfilesToAdd.add(storeFile);
          }
        }

        // Remove hfiles not present in the snapshot
        for (String hfileName : familyFiles) {
          Path hfile = new Path(familyDir, hfileName);
          if (!fs.getFileStatus(hfile).isDirectory()) {
            LOG.trace("Removing HFile=" + hfileName + " not present in snapshot=" + snapshotName
              + " from region=" + regionInfo.getEncodedName() + " table=" + tableName);
if(KnobRuntime.check(java.util.UUID.fromString("7853fda7-3afb-3954-961a-82c9734c2a1a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("23d3327e-6bdf-3013-83e1-5e88e3d8af73"))) {
throw new java.io.IOException("Injected exception");
}
            HFileArchiver.archiveStoreFile(conf, fs, regionInfo, tableDir, family, hfile);
          }
        }

        // Restore Missing files
        for (SnapshotRegionManifest.StoreFile storeFile : hfilesToAdd) {
          LOG.debug("Restoring missing HFileLink " + storeFile.getName() + " of snapshot="
            + snapshotName + " to region=" + regionInfo.getEncodedName() + " table=" + tableName);
          String fileName = restoreStoreFile(familyDir, regionInfo, storeFile, createBackRefs);
          // mark the reference file to be added to tracker
          filesToTrack.add(new StoreFileInfo(conf, fs, new Path(familyDir, fileName), true));
        }
      } else {
        // Family doesn't exists in the snapshot
        LOG.trace("Removing family=" + Bytes.toString(family) + " in snapshot=" + snapshotName
          + " from region=" + regionInfo.getEncodedName() + " table=" + tableName);
if(KnobRuntime.check(java.util.UUID.fromString("97578d1a-c6db-39d3-9179-401901484ef8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("757fc1e5-b478-3e04-aa83-0a6c579979a2"))) {
throw new java.io.IOException("Injected exception");
}
        HFileArchiver.archiveFamilyByFamilyDir(fs, conf, regionInfo, familyDir, family);
if(KnobRuntime.check(java.util.UUID.fromString("c477e116-529b-32de-9167-5fe2ac4ac79e"))) {
throw new java.io.IOException("Injected exception");
}
        fs.delete(familyDir, true);
      }

      StoreFileTracker tracker =
        StoreFileTrackerFactory.create(conf, true, StoreContext.getBuilder()
          .withFamilyStoreDirectoryPath(familyDir).withRegionFileSystem(regionFS).build());

      // simply reset list of tracked files with the matching files
      // and the extra one present in the snapshot
      tracker.set(filesToTrack);
    }

    // Add families not present in the table
    for (Map.Entry<String, List<SnapshotRegionManifest.StoreFile>> familyEntry : snapshotFiles
      .entrySet()) {
      Path familyDir = new Path(regionDir, familyEntry.getKey());
      StoreFileTracker tracker =
        StoreFileTrackerFactory.create(conf, true, StoreContext.getBuilder()
          .withFamilyStoreDirectoryPath(familyDir).withRegionFileSystem(regionFS).build());
      List<StoreFileInfo> files = new ArrayList<>();
      if (!fs.mkdirs(familyDir)) {
        throw new IOException("Unable to create familyDir=" + familyDir);
      }

      for (SnapshotRegionManifest.StoreFile storeFile : familyEntry.getValue()) {
        LOG.trace("Adding HFileLink (Not present in the table) " + storeFile.getName()
          + " of snapshot " + snapshotName + " to table=" + tableName);
if(KnobRuntime.check(java.util.UUID.fromString("c3a843c2-9e03-3f3c-acc6-9c021286a2d8"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("fileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70c770b3-9113-3b9e-acab-0edd27aa5d4b"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("950cfdfb-6d3f-3666-8ec6-1406ff86a3fa"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("fileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7399511-4723-32fa-9fb9-9d9ad753901e"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a59445b8-3ed6-3e11-890a-c20f8ae01d79"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("38d57791-5066-3ed2-b467-e7f54b7c0da5"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5162f3cc-3470-36ff-bd4f-68ce9b4d5be1"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        String fileName = restoreStoreFile(familyDir, regionInfo, storeFile, createBackRefs);
        files.add(new StoreFileInfo(conf, fs, new Path(familyDir, fileName), true));
      }
      tracker.set(files);
    }
  }

  /** Returns The set of files in the specified family directory. */
  private Set<String> getTableRegionFamilyFiles(final Path familyDir) throws IOException {
    FileStatus[] hfiles = CommonFSUtils.listStatus(fs, familyDir);
    if (hfiles == null) {
      return Collections.emptySet();
    }

    Set<String> familyFiles = new HashSet<>(hfiles.length);
    for (int i = 0; i < hfiles.length; ++i) {
      String hfileName = hfiles[i].getPath().getName();
      familyFiles.add(hfileName);
    }

    return familyFiles;
  }

  /**
   * Clone specified regions. For each region create a new region and create a HFileLink for each
   * hfile.
   */
  private RegionInfo[] cloneHdfsRegions(final ThreadPoolExecutor exec,
    final Map<String, SnapshotRegionManifest> regionManifests, final List<RegionInfo> regions)
    throws IOException {
    if (regions == null || regions.isEmpty()) return null;

    final Map<String, RegionInfo> snapshotRegions = new HashMap<>(regions.size());
    final String snapshotName = snapshotDesc.getName();

    // clone region info (change embedded tableName with the new one)
    RegionInfo[] clonedRegionsInfo = new RegionInfo[regions.size()];
    for (int i = 0; i < clonedRegionsInfo.length; ++i) {
      // clone the region info from the snapshot region info
if(KnobRuntime.check(java.util.UUID.fromString("862df19a-91a1-32a1-a329-69b238d04b04"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d1ee5fa0-55eb-3eab-8e5e-2f960be619b6"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab9567df-ced9-3496-860e-acea4cc3d0eb"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eec7fe1e-632d-32e3-9fa2-b76b18e46024"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("56dc944f-1089-3fa2-a40f-0ce282fb41b4"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b5333da3-1e04-3d81-9585-4f066719cde8"))) {
i += 1;
}
      RegionInfo snapshotRegionInfo = regions.get(i);
      clonedRegionsInfo[i] = cloneRegionInfo(snapshotRegionInfo);

      // add the region name mapping between snapshot and cloned
      String snapshotRegionName = snapshotRegionInfo.getEncodedName();
      String clonedRegionName = clonedRegionsInfo[i].getEncodedName();
      regionsMap.put(Bytes.toBytes(snapshotRegionName), Bytes.toBytes(clonedRegionName));
      LOG.info("clone region=" + snapshotRegionName + " as " + clonedRegionName + " in snapshot "
        + snapshotName);

      // Add mapping between cloned region name and snapshot region info
      snapshotRegions.put(clonedRegionName, snapshotRegionInfo);
    }

    // create the regions on disk
    ModifyRegionUtils.createRegions(exec, conf, rootDir, tableDesc, clonedRegionsInfo,
      new ModifyRegionUtils.RegionFillTask() {
        @Override
        public void fillRegion(final HRegion region) throws IOException {
          RegionInfo snapshotHri = snapshotRegions.get(region.getRegionInfo().getEncodedName());
          cloneRegion(region, snapshotHri, regionManifests.get(snapshotHri.getEncodedName()));
        }
      });

    return clonedRegionsInfo;
  }

  /**
   * Clone the mob region. For the region create a new region and create a HFileLink for each hfile.
   */
  private void cloneHdfsMobRegion(final Map<String, SnapshotRegionManifest> regionManifests,
    final RegionInfo region) throws IOException {
    // clone region info (change embedded tableName with the new one)
    Path clonedRegionPath = MobUtils.getMobRegionPath(rootDir, tableDesc.getTableName());
    cloneRegion(MobUtils.getMobRegionInfo(tableDesc.getTableName()), clonedRegionPath, region,
      regionManifests.get(region.getEncodedName()));
  }

  /**
   * Clone region directory content from the snapshot info. Each region is encoded with the table
   * name, so the cloned region will have a different region name. Instead of copying the hfiles a
   * HFileLink is created.
   * @param regionDir {@link Path} cloned dir
   */
  private void cloneRegion(final RegionInfo newRegionInfo, final Path regionDir,
    final RegionInfo snapshotRegionInfo, final SnapshotRegionManifest manifest) throws IOException {
    final String tableName = tableDesc.getTableName().getNameAsString();
    final String snapshotName = snapshotDesc.getName();
    for (SnapshotRegionManifest.FamilyFiles familyFiles : manifest.getFamilyFilesList()) {
      Path familyDir = new Path(regionDir, familyFiles.getFamilyName().toStringUtf8());
      List<StoreFileInfo> clonedFiles = new ArrayList<>();
      for (SnapshotRegionManifest.StoreFile storeFile : familyFiles.getStoreFilesList()) {
        LOG.info("Adding HFileLink " + storeFile.getName() + " from cloned region " + "in snapshot "
          + snapshotName + " to table=" + tableName);
        if (MobUtils.isMobRegionInfo(newRegionInfo)) {
          String mobFileName =
            HFileLink.createHFileLinkName(snapshotRegionInfo, storeFile.getName());
          Path mobPath = new Path(familyDir, mobFileName);
          if (((KnobRuntime.check(java.util.UUID.fromString("93a0f885-9e63-31f4-8e38-88fd013d69e6"))) ? (fs.mkdirs(mobPath)) : (fs.exists(mobPath)))) {
if(KnobRuntime.check(java.util.UUID.fromString("84a738f2-2c87-3a97-ba82-02a0bcc2e91b"))) {
throw new java.io.IOException("Injected exception");
}
            fs.delete(mobPath, true);
if(KnobRuntime.check(java.util.UUID.fromString("1aea2907-c8a5-3abd-b342-19fafa6bf737"))) {
throw new java.io.IOException("Injected exception");
}
          }
          restoreStoreFile(familyDir, snapshotRegionInfo, storeFile, createBackRefs);
        } else {
if(KnobRuntime.check(java.util.UUID.fromString("9f033b59-0de8-3744-b761-c0ab5fdf5ced"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ded6d30-f76b-38ba-8840-8f949484ef33"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("fileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14fb8ad1-c9db-3012-a7e3-00c750317c8b"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83483d5d-4849-395e-a558-1eda8cbec053"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e922225f-290a-3827-9d1a-344d5c629fff"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b8fedfe9-2595-39bb-bd67-f28cbe15bde6"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8f07b4d-c7a2-3380-8a11-452460f79b2c"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("fileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          String file = restoreStoreFile(familyDir, snapshotRegionInfo, storeFile, createBackRefs);
          clonedFiles.add(new StoreFileInfo(conf, fs, new Path(familyDir, file), true));
        }
      }
      // we don't need to track files under mobdir
      if (!MobUtils.isMobRegionInfo(newRegionInfo)) {
        Path regionPath = new Path(tableDir, newRegionInfo.getEncodedName());
        HRegionFileSystem regionFS = (fs.exists(regionPath))
          ? HRegionFileSystem.openRegionFromFileSystem(conf, fs, tableDir, newRegionInfo, false)
          : HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, newRegionInfo);

        Configuration sftConf = StoreUtils.createStoreConfiguration(conf, tableDesc,
          tableDesc.getColumnFamily(familyFiles.getFamilyName().toByteArray()));
        StoreFileTracker tracker =
          StoreFileTrackerFactory.create(sftConf, true, StoreContext.getBuilder()
            .withFamilyStoreDirectoryPath(familyDir).withRegionFileSystem(regionFS).build());
        tracker.set(clonedFiles);
      }
    }

  }

  /**
   * Clone region directory content from the snapshot info. Each region is encoded with the table
   * name, so the cloned region will have a different region name. Instead of copying the hfiles a
   * HFileLink is created.
   * @param region {@link HRegion} cloned
   */
  private void cloneRegion(final HRegion region, final RegionInfo snapshotRegionInfo,
    final SnapshotRegionManifest manifest) throws IOException {
    cloneRegion(region.getRegionInfo(), new Path(tableDir, region.getRegionInfo().getEncodedName()),
      snapshotRegionInfo, manifest);
  }

  /**
   * Create a new {@link HFileLink} to reference the store file.
   * <p>
   * The store file in the snapshot can be a simple hfile, an HFileLink or a reference.
   * <ul>
   * <li>hfile: abc -> table=region-abc
   * <li>reference: abc.1234 -> table=region-abc.1234
   * <li>hfilelink: table=region-hfile -> table=region-hfile
   * </ul>
   * @param familyDir     destination directory for the store file
   * @param regionInfo    destination region info for the table
   * @param createBackRef - Whether back reference should be created. Defaults to true.
   * @param storeFile     store file name (can be a Reference, HFileLink or simple HFile)
   */
  private String restoreStoreFile(final Path familyDir, final RegionInfo regionInfo,
    final SnapshotRegionManifest.StoreFile storeFile, final boolean createBackRef)
    throws IOException {
    String hfileName = storeFile.getName();
    if (HFileLink.isHFileLink(hfileName)) {
      return HFileLink.createFromHFileLink(conf, fs, familyDir, hfileName, createBackRef);
    } else if (StoreFileInfo.isReference(hfileName)) {
      return restoreReferenceFile(familyDir, regionInfo, storeFile);
    } else {
      return HFileLink.create(conf, fs, familyDir, regionInfo, hfileName, createBackRef);
    }
  }

  /**
   * Create a new {@link Reference} as copy of the source one.
   * <p>
   * <blockquote>
   *
   * <pre>
   * The source table looks like:
   *    1234/abc      (original file)
   *    5678/abc.1234 (reference file)
   *
   * After the clone operation looks like:
   *   wxyz/table=1234-abc
   *   stuv/table=1234-abc.wxyz
   *
   * NOTE that the region name in the clone changes (md5 of regioninfo)
   * and the reference should reflect that change.
   * </pre>
   *
   * </blockquote>
   * @param familyDir  destination directory for the store file
   * @param regionInfo destination region info for the table
   * @param storeFile  reference file name
   */
  private String restoreReferenceFile(final Path familyDir, final RegionInfo regionInfo,
    final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
    String hfileName = storeFile.getName();

    // Extract the referred information (hfile name and parent region)
    Path refPath =
      StoreFileInfo
        .getReferredToFile(
          new Path(
            new Path(
              new Path(new Path(snapshotTable.getNamespaceAsString(),
                snapshotTable.getQualifierAsString()), regionInfo.getEncodedName()),
              familyDir.getName()),
            hfileName));
    String snapshotRegionName = refPath.getParent().getParent().getName();
    String fileName = refPath.getName();

    // The new reference should have the cloned region name as parent, if it is a clone.
    String clonedRegionName = Bytes.toString(regionsMap.get(Bytes.toBytes(snapshotRegionName)));
    if (clonedRegionName == null) clonedRegionName = snapshotRegionName;

    // The output file should be a reference link table=snapshotRegion-fileName.clonedRegionName
    Path linkPath = null;
    String refLink = fileName;
    if (!HFileLink.isHFileLink(fileName)) {
if(KnobRuntime.check(java.util.UUID.fromString("f713d237-bfda-312a-b637-fd2508e2ab67"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eadeab24-d3c6-3a70-94c3-cc3afc1427ee"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshotTable);
    field.set(snapshotTable, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("97778c76-3772-3860-ae57-343c63665220"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c664409-209b-3acc-a75f-bdb4483eb274"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa26b1f2-45ec-3cd9-93f9-e046138c6158"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      refLink = HFileLink.createHFileLinkName(snapshotTable, snapshotRegionName, fileName);
      linkPath = new Path(familyDir,
        HFileLink.createHFileLinkName(snapshotTable, regionInfo.getEncodedName(), hfileName));
    }

    Path outPath = new Path(familyDir, refLink + '.' + clonedRegionName);

    // Create the new reference
    if (storeFile.hasReference()) {
      Reference reference = Reference.convert(storeFile.getReference());
if(KnobRuntime.check(java.util.UUID.fromString("799769e9-12f1-3864-a8a5-9067942a63b0"))) {
throw new java.io.IOException("Injected exception");
}
      reference.write(fs, outPath);
    } else {
      InputStream in;
      if (linkPath != null) {
        in = HFileLink.buildFromHFileLinkPattern(conf, linkPath).open(fs);
      } else {
        linkPath = new Path(new Path(
          HRegion.getRegionDir(snapshotManifest.getSnapshotDir(), regionInfo.getEncodedName()),
          familyDir.getName()), hfileName);
if(KnobRuntime.check(java.util.UUID.fromString("a270666f-07cd-3e01-8638-877cf462d5b0"))) {
throw new java.io.IOException("Injected exception");
}
        in = fs.open(linkPath);
      }
if(KnobRuntime.check(java.util.UUID.fromString("1a6f2924-1fde-3561-b770-e7b1ab5b71db"))) {
throw new java.io.IOException("Injected exception");
}
      OutputStream out = fs.create(outPath);
      IOUtils.copyBytes(in, out, conf);
    }

    // Add the daughter region to the map
    String regionName = Bytes.toString(regionsMap.get(regionInfo.getEncodedNameAsBytes()));
    if (regionName == null) {
      regionName = regionInfo.getEncodedName();
    }
    LOG.debug("Restore reference " + regionName + " to " + clonedRegionName);
    synchronized (parentsMap) {
      Pair<String, String> daughters = parentsMap.get(clonedRegionName);
      if (daughters == null) {
        // In case one side of the split is already compacted, regionName is put as both first and
        // second of Pair
        daughters = new Pair<>(regionName, regionName);
        parentsMap.put(clonedRegionName, daughters);
      } else if (!regionName.equals(daughters.getFirst())) {
        daughters.setSecond(regionName);
      }
    }
    return outPath.getName();
  }

  /**
   * Create a new {@link RegionInfo} from the snapshot region info. Keep the same startKey, endKey,
   * regionId and split information but change the table name.
   * @param snapshotRegionInfo Info for region to clone.
   * @return the new HRegion instance
   */
  public RegionInfo cloneRegionInfo(final RegionInfo snapshotRegionInfo) {
if(KnobRuntime.check(java.util.UUID.fromString("35c1c4b0-91e4-34ee-a1c8-191b09e95f3a"))) {
return null;
}
    return cloneRegionInfo(tableDesc.getTableName(), snapshotRegionInfo);
  }

  public static RegionInfo cloneRegionInfo(TableName tableName, RegionInfo snapshotRegionInfo) {
    return RegionInfoBuilder.newBuilder(tableName).setStartKey(snapshotRegionInfo.getStartKey())
      .setEndKey(snapshotRegionInfo.getEndKey()).setSplit(snapshotRegionInfo.isSplit())
      .setRegionId(snapshotRegionInfo.getRegionId()).setOffline(snapshotRegionInfo.isOffline())
      .build();
  }

  /** Returns the set of the regions contained in the table */
  private List<RegionInfo> getTableRegions() throws IOException {
    LOG.debug("get table regions: " + tableDir);
    FileStatus[] regionDirs =
      CommonFSUtils.listStatus(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (((KnobRuntime.check(java.util.UUID.fromString("6c5d4fd5-0510-3486-a0d9-4290cc6cbca1"))) ? ((regionDirs) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("faef9706-56cd-305b-bd5b-fe49b3ceefa7"))) ? ((regionDirs) != (null)) : (regionDirs == null))))) {
      return null;
    }

    List<RegionInfo> regions = new ArrayList<>(regionDirs.length);
    for (int i = 0; i < regionDirs.length; ++i) {
      RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDirs[i].getPath());
      regions.add(hri);
    }
    LOG.debug("found " + regions.size() + " regions for table="
      + tableDesc.getTableName().getNameAsString());
    return regions;
  }

  /**
   * Copy the snapshot files for a snapshot scanner, discards meta changes.
   */
  public static RestoreMetaChanges copySnapshotForScanner(Configuration conf, FileSystem fs,
    Path rootDir, Path restoreDir, String snapshotName) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bfa87d33-5844-30ef-b428-3e6d0e5cc667"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e42b5d0b-eb73-3f05-8d24-675c78e52780"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("63919ae2-0e6f-382c-a6c5-444964abf51e"))) {
return null;
}
    // ensure that restore dir is not under root dir
if(KnobRuntime.check(java.util.UUID.fromString("ebc5128d-7309-3393-b98e-5e7197824e49"))) {
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
    if (!restoreDir.getFileSystem(conf).getUri().equals(rootDir.getFileSystem(conf).getUri())) {
      throw new IllegalArgumentException(
        "Filesystems for restore directory and HBase root " + "directory should be the same");
    }
    if (restoreDir.toUri().getPath().startsWith(rootDir.toUri().getPath() + "/")) {
      throw new IllegalArgumentException("Restore directory cannot be a sub directory of HBase "
        + "root directory. RootDir: " + rootDir + ", restoreDir: " + restoreDir);
    }

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    // check if the snapshot is expired.
    boolean isExpired = SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDesc.getTtl(),
      snapshotDesc.getCreationTime(), EnvironmentEdgeManager.currentTime());
    if (isExpired) {
      throw new SnapshotTTLExpiredException(ProtobufUtil.createSnapshotDesc(snapshotDesc));
    }
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

    MonitoredTask status = TaskMonitor.get()
      .createStatus("Restoring  snapshot '" + snapshotName + "' to directory " + restoreDir);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    // we send createBackRefs=false so that restored hfiles do not create back reference links
    // in the base hbase root dir.
    RestoreSnapshotHelper helper = new RestoreSnapshotHelper(conf, fs, manifest,
      manifest.getTableDescriptor(), restoreDir, monitor, status, false);
    RestoreMetaChanges metaChanges = helper.restoreHdfsRegions(); // TODO: parallelize.

    if (LOG.isDebugEnabled()) {
      LOG.debug("Restored table dir:" + restoreDir);
      CommonFSUtils.logFileSystemState(fs, restoreDir, LOG);
    }
    return metaChanges;
  }

  public static void restoreSnapshotAcl(SnapshotDescription snapshot, TableName newTableName,
    Configuration conf) throws IOException {
    if (snapshot.hasUsersAndPermissions() && snapshot.getUsersAndPermissions() != null) {
      LOG.info("Restore snapshot acl to table. snapshot: " + snapshot + ", table: " + newTableName);
      ListMultimap<String, Permission> perms =
        ShadedAccessControlUtil.toUserTablePermissions(snapshot.getUsersAndPermissions());
      try (Connection conn = ConnectionFactory.createConnection(conf)) {
        for (Entry<String, Permission> e : perms.entries()) {
          String user = e.getKey();
          TablePermission tablePerm = (TablePermission) e.getValue();
          AccessControlClient.grant(conn, newTableName, user, tablePerm.getFamily(),
            tablePerm.getQualifier(), tablePerm.getActions());
        }
      } catch (Throwable e) {
        throw new IOException("Grant acl into newly creatd table failed. snapshot: " + snapshot
          + ", table: " + newTableName, e);
      }
    }
  }
}
