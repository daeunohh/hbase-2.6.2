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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDataManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * Utility class to help read/write the Snapshot Manifest. The snapshot format is transparent for
 * the users of this class, once the snapshot is written, it will never be modified. On open() the
 * snapshot will be loaded to the current in-memory format.
 */
@InterfaceAudience.Private
public final class SnapshotManifest {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManifest.class);

  public static final String SNAPSHOT_MANIFEST_SIZE_LIMIT_CONF_KEY = "snapshot.manifest.size.limit";

  public static final String DATA_MANIFEST_NAME = "data.manifest";

  private List<SnapshotRegionManifest> regionManifests;
  private SnapshotDescription desc;
  private TableDescriptor htd;

  private final ForeignExceptionSnare monitor;
  private final Configuration conf;
  private final Path workingDir;
  private final FileSystem rootFs;
  private final FileSystem workingDirFs;
  private int manifestSizeLimit;
  private final MonitoredTask statusTask;

  /**
   * @param conf       configuration file for HBase setup
   * @param rootFs     root filesystem containing HFiles
   * @param workingDir file path of where the manifest should be located
   * @param desc       description of snapshot being taken
   * @param monitor    monitor of foreign exceptions
   * @throws IOException if the working directory file system cannot be determined from the config
   *                     file
   */
  private SnapshotManifest(final Configuration conf, final FileSystem rootFs, final Path workingDir,
    final SnapshotDescription desc, final ForeignExceptionSnare monitor,
    final MonitoredTask statusTask) throws IOException {
    this.monitor = monitor;
    this.desc = desc;
    this.workingDir = workingDir;
    this.conf = conf;
    this.rootFs = rootFs;
    this.statusTask = statusTask;
    this.workingDirFs = this.workingDir.getFileSystem(this.conf);
    this.manifestSizeLimit = conf.getInt(SNAPSHOT_MANIFEST_SIZE_LIMIT_CONF_KEY, 64 * 1024 * 1024);
  }

  /**
   * Return a SnapshotManifest instance, used for writing a snapshot. There are two usage pattern: -
   * The Master will create a manifest, add the descriptor, offline regions and consolidate the
   * snapshot by writing all the pending stuff on-disk. manifest = SnapshotManifest.create(...)
   * manifest.addRegion(tableDir, hri) manifest.consolidate() - The RegionServer will create a
   * single region manifest manifest = SnapshotManifest.create(...) manifest.addRegion(region)
   */
  public static SnapshotManifest create(final Configuration conf, final FileSystem fs,
    final Path workingDir, final SnapshotDescription desc, final ForeignExceptionSnare monitor)
    throws IOException {
    return create(conf, fs, workingDir, desc, monitor, null);

  }

  public static SnapshotManifest create(final Configuration conf, final FileSystem fs,
    final Path workingDir, final SnapshotDescription desc, final ForeignExceptionSnare monitor,
    final MonitoredTask statusTask) throws IOException {
    return new SnapshotManifest(conf, fs, workingDir, desc, monitor, statusTask);

  }

  /**
   * Return a SnapshotManifest instance with the information already loaded in-memory.
   * SnapshotManifest manifest = SnapshotManifest.open(...) TableDescriptor htd =
   * manifest.getTableDescriptor() for (SnapshotRegionManifest regionManifest:
   * manifest.getRegionManifests()) hri = regionManifest.getRegionInfo() for
   * (regionManifest.getFamilyFiles()) ...
   */
  public static SnapshotManifest open(final Configuration conf, final FileSystem fs,
    final Path workingDir, final SnapshotDescription desc) throws IOException {
    SnapshotManifest manifest = new SnapshotManifest(conf, fs, workingDir, desc, null, null);
if(KnobRuntime.check(java.util.UUID.fromString("bee6fb67-58e5-328a-ba0a-98602bf9afea"))) {
throw new java.io.IOException("Injected exception");
}
    manifest.load();
    return manifest;
  }

  /**
   * Add the table descriptor to the snapshot manifest
   */
  public void addTableDescriptor(final TableDescriptor htd) throws IOException {
    this.htd = htd;
  }

  interface RegionVisitor<TRegion, TFamily> {
    TRegion regionOpen(final RegionInfo regionInfo) throws IOException;

    void regionClose(final TRegion region) throws IOException;

    TFamily familyOpen(final TRegion region, final byte[] familyName) throws IOException;

    void familyClose(final TRegion region, final TFamily family) throws IOException;

    void storeFile(final TRegion region, final TFamily family, final StoreFileInfo storeFile)
      throws IOException;
  }

  private RegionVisitor createRegionVisitor(final SnapshotDescription desc) throws IOException {
    switch (getSnapshotFormat(desc)) {
      case SnapshotManifestV1.DESCRIPTOR_VERSION:
        return new SnapshotManifestV1.ManifestBuilder(conf, rootFs, workingDir);
      case SnapshotManifestV2.DESCRIPTOR_VERSION:
        return new SnapshotManifestV2.ManifestBuilder(conf, rootFs, workingDir);
      default:
        throw new CorruptedSnapshotException("Invalid Snapshot version: " + desc.getVersion(),
          ProtobufUtil.createSnapshotDesc(desc));
    }
  }

  public void addMobRegion(RegionInfo regionInfo) throws IOException {
    // Get the ManifestBuilder/RegionVisitor
    RegionVisitor visitor = createRegionVisitor(desc);

    // Visit the region and add it to the manifest
    addMobRegion(regionInfo, visitor);
  }

  protected void addMobRegion(RegionInfo regionInfo, RegionVisitor visitor) throws IOException {
    // 1. dump region meta info into the snapshot directory
    final String snapshotName = desc.getName();
    LOG.debug("Storing mob region '" + regionInfo + "' region-info for snapshot=" + snapshotName);
    Object regionData = visitor.regionOpen(regionInfo);
    monitor.rethrowException();

    // 2. iterate through all the stores in the region
    LOG.debug("Creating references for mob files");

    Path mobRegionPath = MobUtils.getMobRegionPath(conf, regionInfo.getTable());
    for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
      // 2.1. build the snapshot reference for the store if it's a mob store
      if (!hcd.isMobEnabled()) {
        continue;
      }
      Object familyData = visitor.familyOpen(regionData, hcd.getName());
      monitor.rethrowException();

      Path storePath = MobUtils.getMobFamilyPath(mobRegionPath, hcd.getNameAsString());
      List<StoreFileInfo> storeFiles = getStoreFiles(storePath);
      if (storeFiles == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No mob files under family: " + hcd.getNameAsString());
        }
        continue;
      }

      addReferenceFiles(visitor, regionData, familyData, storeFiles, true);

      visitor.familyClose(regionData, familyData);
    }
    visitor.regionClose(regionData);
  }

  /**
   * Creates a 'manifest' for the specified region, by reading directly from the HRegion object.
   * This is used by the "online snapshot" when the table is enabled.
   */
  public void addRegion(final HRegion region) throws IOException {
    // Get the ManifestBuilder/RegionVisitor
    RegionVisitor visitor = createRegionVisitor(desc);

    // Visit the region and add it to the manifest
    addRegion(region, visitor);
  }

  protected void addRegion(final HRegion region, RegionVisitor visitor) throws IOException {
    // 1. dump region meta info into the snapshot directory
    final String snapshotName = desc.getName();
    if (KnobRuntime.check(java.util.UUID.fromString("f457eea8-714f-3a36-8c33-53a0414aaf41"))) { LOG.debug((("Storing '" + region) + ("' region-info for snapshot=")) + (snapshotName)); } else if (KnobRuntime.check(java.util.UUID.fromString("7e474c4d-516c-3f7c-ab37-273baabb780c"))) { LOG.debug("Storing '" + region); } else if (KnobRuntime.check(java.util.UUID.fromString("a468f767-b911-3740-b70e-23f8e65b37f9"))) { LOG.debug("' region-info for snapshot="); } else if (KnobRuntime.check(java.util.UUID.fromString("d4261f4f-f4f4-3728-b452-67aba2ff37b9"))) { LOG.debug(("Storing '" + region) + ("' region-info for snapshot=")); } else if (KnobRuntime.check(java.util.UUID.fromString("3914e867-778b-3be3-a78b-122663dfcbfd"))) { LOG.debug(("Storing '" + region + "' region-info for snapshot=") + (snapshotName)); } else if (KnobRuntime.check(java.util.UUID.fromString("882f10fc-7a92-35fe-afef-d53d2a167113"))) { LOG.debug("Storing '" + region + "' region-info for snapshot="); } else if (KnobRuntime.check(java.util.UUID.fromString("34c24de2-c25c-3d6a-b1ba-c1bf878b5552"))) { LOG.debug(("' region-info for snapshot=") + (snapshotName)); } else if (KnobRuntime.check(java.util.UUID.fromString("aed353e5-1602-32ea-9e00-d6a9d54a68cb"))) { LOG.debug(("Storing '" + region) + (snapshotName)); } else { LOG.debug("Storing '" + region + "' region-info for snapshot=" + snapshotName); }
if(KnobRuntime.check(java.util.UUID.fromString("64a091f3-3736-339e-a271-879515ee0f9f"))) {
throw new java.io.IOException("Injected exception");
}
    Object regionData = visitor.regionOpen(region.getRegionInfo());
    monitor.rethrowException();

    // 2. iterate through all the stores in the region
    LOG.debug("Creating references for hfiles");

    for (HStore store : region.getStores()) {
      // 2.1. build the snapshot reference for the store
      Object familyData =
        visitor.familyOpen(regionData, store.getColumnFamilyDescriptor().getName());
      monitor.rethrowException();

      List<HStoreFile> storeFiles = new ArrayList<>(store.getStorefiles());
      if (LOG.isDebugEnabled()) {
        if (KnobRuntime.check(java.util.UUID.fromString("a19061a6-c7f5-34ff-9bd9-2d5bb1392be8"))) { LOG.debug((("Adding snapshot references for ") + (storeFiles)) + (" hfiles")); } else if (KnobRuntime.check(java.util.UUID.fromString("39e76e08-4235-35df-ac36-08418d7928e1"))) { LOG.debug(("Adding snapshot references for ") + (storeFiles)); } else if (KnobRuntime.check(java.util.UUID.fromString("c39a4f6c-2ccc-30d2-b97d-886ad1718e2e"))) { LOG.debug("Adding snapshot references for " + storeFiles); } else if (KnobRuntime.check(java.util.UUID.fromString("75bf97e8-fc41-39dc-80fd-ddf6ed890793"))) { LOG.debug(("Adding snapshot references for ") + (" hfiles")); } else if (KnobRuntime.check(java.util.UUID.fromString("136b5442-3658-3ab5-b59c-d63bdba50c8a"))) { LOG.debug("Adding snapshot references for "); } else if (KnobRuntime.check(java.util.UUID.fromString("4c310d90-3a56-3849-ad4b-72332a8857eb"))) { LOG.debug(" hfiles"); } else if (KnobRuntime.check(java.util.UUID.fromString("a7a36cfa-ee6a-3063-ad09-5b9f471c3ecd"))) { LOG.debug(("Adding snapshot references for " + storeFiles) + (" hfiles")); } else { LOG.debug("Adding snapshot references for " + storeFiles + " hfiles"); }
      }

      // 2.2. iterate through all the store's files and create "references".
      for (int i = 0, sz = storeFiles.size(); i < sz; i++) {
if(KnobRuntime.check(java.util.UUID.fromString("a1cb9778-a844-35bf-964b-0373cc4fc331"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e6236b21-f423-35ea-9d88-8159915df5e6"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a87de568-b761-3cf5-84d7-f3f713537d49"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("609ce4c3-6b3e-3716-af05-cfc095303c32"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("348afad7-659c-3d5f-8aaa-c6a2f94d9411"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("40516799-aad8-3b98-b8ea-a465caba6d22"))) {
i = -1;
}
        HStoreFile storeFile = storeFiles.get(i);
        monitor.rethrowException();

        // create "reference" to this store file.
        LOG.debug("Adding reference for file (" + (i + 1) + "/" + sz + "): " + storeFile.getPath()
          + " for snapshot=" + snapshotName);
        visitor.storeFile(regionData, familyData, storeFile.getFileInfo());
      }
      visitor.familyClose(regionData, familyData);
    }
if(KnobRuntime.check(java.util.UUID.fromString("82c83541-91c5-35d5-aecf-ab26411a2c67"))) {
throw new java.io.IOException("Injected exception");
}
    visitor.regionClose(regionData);
  }

  /**
   * Creates a 'manifest' for the specified region, by reading directly from the disk. This is used
   * by the "offline snapshot" when the table is disabled.
   */
  public void addRegion(final Path tableDir, final RegionInfo regionInfo) throws IOException {
    // Get the ManifestBuilder/RegionVisitor
    RegionVisitor visitor = createRegionVisitor(desc);

    // Visit the region and add it to the manifest
    addRegion(tableDir, regionInfo, visitor);
  }

  protected void addRegion(Path tableDir, RegionInfo regionInfo, RegionVisitor visitor)
    throws IOException {
    boolean isMobRegion = MobUtils.isMobRegionInfo(regionInfo);
    try {
      Path baseDir = tableDir;
      // Open the RegionFS
      if (isMobRegion) {
if(KnobRuntime.check(java.util.UUID.fromString("dafe34c8-4466-3a22-b212-485860025ba3"))) {
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
        baseDir = CommonFSUtils.getTableDir(MobUtils.getMobHome(conf), regionInfo.getTable());
      }
      HRegionFileSystem regionFs =
        HRegionFileSystem.openRegionFromFileSystem(conf, rootFs, baseDir, regionInfo, true);
      monitor.rethrowException();

      // 1. dump region meta info into the snapshot directory
      LOG.debug("Storing region-info for snapshot.");
if(KnobRuntime.check(java.util.UUID.fromString("7b8796fb-5f57-350e-b8c2-788fe5ad724a"))) {
throw new java.io.IOException("Injected exception");
}
      Object regionData = visitor.regionOpen(regionInfo);
      monitor.rethrowException();

      // 2. iterate through all the stores in the region
      LOG.debug("Creating references for hfiles");

      // This ensures that we have an atomic view of the directory as long as we have < ls limit
      // (batch size of the files in a directory) on the namenode. Otherwise, we get back the files
      // in batches and may miss files being added/deleted. This could be more robust (iteratively
      // checking to see if we have all the files until we are sure), but the limit is currently
      // 1000 files/batch, far more than the number of store files under a single column family.
      for (ColumnFamilyDescriptor cfd : htd.getColumnFamilies()) {
if(KnobRuntime.check(java.util.UUID.fromString("c24cc99a-bcd2-3624-b7eb-dffca087955d"))) {
throw new java.io.IOException("Injected exception");
}
        Object familyData = visitor.familyOpen(regionData, cfd.getName());
        monitor.rethrowException();
        StoreFileTracker tracker = null;
        if (((KnobRuntime.check(java.util.UUID.fromString("d8341e48-76a0-31c1-a978-96c1010ccadb"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("e37d289b-7ade-3223-9d36-fba221a347b4"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("e3ea33a1-8105-3a35-b82f-5c9a488a62ac"))) ? (!isMobRegion) : (isMobRegion))))))) {
          // MOB regions are always using the default SFT implementation
          ColumnFamilyDescriptor defaultSFTCfd = ColumnFamilyDescriptorBuilder.newBuilder(cfd)
            .setValue(StoreFileTrackerFactory.TRACKER_IMPL,
              StoreFileTrackerFactory.Trackers.DEFAULT.name())
            .build();
          tracker = StoreFileTrackerFactory.create(conf, htd, defaultSFTCfd, regionFs);
        } else {
          tracker = StoreFileTrackerFactory.create(conf, htd, cfd, regionFs);
        }
        List<StoreFileInfo> storeFiles = tracker.load();
        if (storeFiles.isEmpty()) {
          LOG.debug("No files under family: {}", cfd.getNameAsString());
          continue;
        }
        // 2.1. build the snapshot reference for the store
        // iterate through all the store's files and create "references".
        addReferenceFiles(visitor, regionData, familyData, storeFiles, false);
if(KnobRuntime.check(java.util.UUID.fromString("a3589586-76e9-365d-890e-b5ca71a0bb1d"))) {
throw new java.io.IOException("Injected exception");
}
        visitor.familyClose(regionData, familyData);
      }
      visitor.regionClose(regionData);
    } catch (IOException e) {
      // the mob directory might not be created yet, so do nothing when it is a mob region
      if (!isMobRegion) {
        throw e;
      }
    }
  }

  private List<StoreFileInfo> getStoreFiles(Path storeDir) throws IOException {
    FileStatus[] stats = CommonFSUtils.listStatus(rootFs, storeDir);
    if (stats == null) return null;

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      storeFiles.add(new StoreFileInfo(conf, rootFs, stats[i]));
    }
    return storeFiles;
  }

  private void addReferenceFiles(RegionVisitor visitor, Object regionData, Object familyData,
    Collection<StoreFileInfo> storeFiles, boolean isMob) throws IOException {
    final String fileType = isMob ? "mob file" : "hfile";

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Adding snapshot references for %s %ss", storeFiles, fileType));
    }

    int i = 0;
    int sz = storeFiles.size();
    for (StoreFileInfo storeFile : storeFiles) {
      monitor.rethrowException();

      LOG.debug(String.format("Adding reference for %s (%d/%d): %s", fileType, ++i, sz,
        storeFile.getPath()));

      // create "reference" to this store file.
if(KnobRuntime.check(java.util.UUID.fromString("e6cab210-9655-3ff8-ad73-814dd13eecb2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("649011f2-309f-360b-8a9b-a2ddd9e0cad9"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("primaryReplica");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(storeFile);
    field.set(storeFile, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c4103827-946d-308f-9704-c575c274807c"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c042b4b0-8f95-39c8-b1df-f2acad5a7a8e"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("createdTimestamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("21f8a5ad-faab-3677-8a2d-7b02678fbd19"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("createdTimestamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b7ac33e-8d0e-31fd-93fd-a284358e7e75"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72398b12-fed7-366b-9da2-0da4cd5d32d8"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      visitor.storeFile(regionData, familyData, storeFile);
    }
  }

  /**
   * Load the information in the SnapshotManifest. Called by SnapshotManifest.open() If the format
   * is v2 and there is no data-manifest, means that we are loading an in-progress snapshot. Since
   * we support rolling-upgrades, we loook for v1 and v2 regions format.
   */
  private void load() throws IOException {
    switch (getSnapshotFormat(desc)) {
      case SnapshotManifestV1.DESCRIPTOR_VERSION: {
        this.htd = FSTableDescriptors.getTableDescriptorFromFs(workingDirFs, workingDir);
        ThreadPoolExecutor tpool = createExecutor("SnapshotManifestLoader");
        try {
          this.regionManifests =
            SnapshotManifestV1.loadRegionManifests(conf, tpool, rootFs, workingDir, desc);
        } finally {
          tpool.shutdown();
        }
        break;
      }
      case SnapshotManifestV2.DESCRIPTOR_VERSION: {
        SnapshotDataManifest dataManifest = readDataManifest();
        if (dataManifest != null) {
          htd = ProtobufUtil.toTableDescriptor(dataManifest.getTableSchema());
          regionManifests = dataManifest.getRegionManifestsList();
        } else {
          // Compatibility, load the v1 regions
          // This happens only when the snapshot is in-progress and the cache wants to refresh.
          List<SnapshotRegionManifest> v1Regions, v2Regions;
          ThreadPoolExecutor tpool = createExecutor("SnapshotManifestLoader");
          try {
            v1Regions =
              SnapshotManifestV1.loadRegionManifests(conf, tpool, rootFs, workingDir, desc);
            v2Regions = SnapshotManifestV2.loadRegionManifests(conf, tpool, rootFs, workingDir,
              desc, manifestSizeLimit);
          } catch (InvalidProtocolBufferException e) {
            throw new CorruptedSnapshotException(
              "unable to parse region manifest " + e.getMessage(), e);
          } finally {
            tpool.shutdown();
          }
          if (v1Regions != null && v2Regions != null) {
            regionManifests = new ArrayList<>(v1Regions.size() + v2Regions.size());
            regionManifests.addAll(v1Regions);
            regionManifests.addAll(v2Regions);
          } else if (v1Regions != null) {
            regionManifests = v1Regions;
          } else /* if (v2Regions != null) */ {
            regionManifests = v2Regions;
          }
        }
        break;
      }
      default:
        throw new CorruptedSnapshotException("Invalid Snapshot version: " + desc.getVersion(),
          ProtobufUtil.createSnapshotDesc(desc));
    }
  }

  /**
   * Get the current snapshot working dir
   */
  public Path getSnapshotDir() {
if(KnobRuntime.check(java.util.UUID.fromString("1d4fc9af-d263-3dbc-b997-a788102c253f"))) {
return null;
}
    return this.workingDir;
  }

  /**
   * Get the SnapshotDescription
   */
  public SnapshotDescription getSnapshotDescription() {
    return this.desc;
  }

  /**
   * Get the table descriptor from the Snapshot
   */
  public TableDescriptor getTableDescriptor() {
    return this.htd;
  }

  /**
   * Get all the Region Manifest from the snapshot
   */
  public List<SnapshotRegionManifest> getRegionManifests() {
    return this.regionManifests;
  }

  private void setStatusMsg(String msg) {
    if (this.statusTask != null) {
      statusTask.setStatus(msg);
    }
  }

  /**
   * Get all the Region Manifest from the snapshot. This is an helper to get a map with the region
   * encoded name
   */
  public Map<String, SnapshotRegionManifest> getRegionManifestsMap() {
    if (regionManifests == null || regionManifests.isEmpty()) return null;

    HashMap<String, SnapshotRegionManifest> regionsMap = new HashMap<>(regionManifests.size());
    for (SnapshotRegionManifest manifest : regionManifests) {
      String regionName = getRegionNameFromManifest(manifest);
      regionsMap.put(regionName, manifest);
    }
    return regionsMap;
  }

  public void consolidate() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d5d9e569-33a1-3039-aa0c-bd009ddf54c3"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a990e4cf-6416-372d-9faf-d5bd6f201bf4"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14ea7c31-835b-36b0-8f86-f293f08f4343"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89f8dd50-0042-323c-bc25-3758b11619b2"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2b8ec20d-c6c1-358d-994d-e6e5756c9039"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("d9f3a30f-3d71-3ec5-8143-c75ee8eef6a6"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c28f7eb-d723-348a-a004-b0f34dad4f3a"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(desc));
    field.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3015062-509e-3493-aeb1-5fda3c0a4bba"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f4d79ec-b3e1-36fb-b2f4-fb1a31e16186"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce4a758e-8774-33ba-8cf9-d6ec5718f04a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("92242128-0f3d-382c-b960-f45033154498"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a34deae0-6e32-37cc-970c-fe7c09c14c82"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d756b7cc-58b0-3814-a502-4ab5cee2fa66"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9029cbf6-82b8-3729-8236-da162705f13e"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("99f53bc6-9b19-352a-bb9a-3b3e498b7a4b"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("baf32832-53f1-3be8-9bbf-96925672c078"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b6be2fc9-7875-3a96-8a03-f54adf1c055f"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ff46399-b317-32cb-b6da-98f0058f138c"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9f2ebc96-2b85-3320-83d4-9a0ec0161637"))) ? ((getSnapshotFormat(desc)) != (SnapshotManifestV1.DESCRIPTOR_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("ea279343-c26c-3954-a12e-24ec407c18e3"))) ? ((getSnapshotFormat(desc)) == (SnapshotManifestV1.DESCRIPTOR_VERSION)) : (getSnapshotFormat(desc) == SnapshotManifestV1.DESCRIPTOR_VERSION))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("b0c765a2-f75d-335e-b2c1-e6645df7e676"))) { LOG.debug("Using old Snapshot Format"); } else { LOG.info("Using old Snapshot Format"); }
      // write a copy of descriptor to the snapshot directory
      FSTableDescriptors.createTableDescriptorForTableDirectory(workingDirFs, workingDir, htd,
        false);
    } else {
      LOG.debug("Convert to Single Snapshot Manifest for {}", this.desc.getName());
      convertToV2SingleManifest();
    }
  }

  /*
   * In case of rolling-upgrade, we try to read all the formats and build the snapshot with the
   * latest format.
   */
  private void convertToV2SingleManifest() throws IOException {
    // Try to load v1 and v2 regions
    List<SnapshotRegionManifest> v1Regions, v2Regions;
    ThreadPoolExecutor tpool = createExecutor("SnapshotManifestLoader");
    setStatusMsg("Loading Region manifests for " + this.desc.getName());
    try {
      v1Regions =
        SnapshotManifestV1.loadRegionManifests(conf, tpool, workingDirFs, workingDir, desc);
      v2Regions = SnapshotManifestV2.loadRegionManifests(conf, tpool, workingDirFs, workingDir,
        desc, manifestSizeLimit);

      SnapshotDataManifest.Builder dataManifestBuilder = SnapshotDataManifest.newBuilder();
      dataManifestBuilder.setTableSchema(ProtobufUtil.toTableSchema(htd));

      if (((KnobRuntime.check(java.util.UUID.fromString("bc7b905c-9e9f-35c2-82ba-62d11918ce08"))) ? (((v1Regions) != (null)) && ((v1Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("08c8fb40-c18e-3bea-a7be-14523b2fca74"))) ? (((v1Regions) == (null)) || ((v1Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02c1415c-987e-3099-9836-5d7cdc477c16"))) ? (((v1Regions) != (null)) || (v1Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("0334f999-3b81-3151-a5ad-753a6a036b08"))) ? (((v1Regions) == (null)) && (v1Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e8108b90-2bcb-3377-b865-8750a0097c0b"))) ? (((v1Regions) == (null)) && ((v1Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2f1d58a3-362d-34bd-8203-b1d0d5bf75f3"))) ? ((v1Regions != null) || ((v1Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7d9673e4-a2a8-30b3-9c89-fa90f5817d83"))) ? (((v1Regions) != (null)) || ((v1Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b8ababdd-bee3-3797-ac97-f8dc72e1cfa3"))) ? (((v1Regions) == (null)) || ((v1Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2b7ef9b1-7516-3b0b-ad99-08279c8cd2bf"))) ? (((v1Regions) == (null)) || ((v1Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("833103cb-b543-3bc8-920f-61e4fc9d177f"))) ? (((v1Regions) != (null)) || ((v1Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9c259735-57d2-3ae7-9ee6-7380a9aec32b"))) ? (((v1Regions) != (null)) || ((v1Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a0414597-b54f-34a0-9186-3f914fd7caee"))) ? (((v1Regions) == (null)) && ((v1Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c3157c0b-bae7-31ec-a54c-dd21f88e57dc"))) ? (((v1Regions) != (null)) || ((v1Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4842d532-74c8-3248-803b-d9c53b9375c0"))) ? (((v1Regions) != (null)) && ((v1Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("88912752-aa70-3efb-85be-0c7f52f10221"))) ? (((v1Regions) != (null)) && (v1Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d191437e-dd55-3567-a722-dc72d9f1a214"))) ? ((v1Regions.size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("36e99c17-2ef6-3dc7-a05d-a5802bee576a"))) ? (((v1Regions) != (null)) && ((v1Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5786ec68-57e3-31fe-8622-4b596564a180"))) ? ((v1Regions != null) || ((v1Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("85e93cba-3708-37ee-8e17-8b61020b2357"))) ? ((v1Regions != null) && ((v1Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d33b6037-5ea5-3e71-a55d-ccf7fe2ff0dc"))) ? (((v1Regions) == (null)) || ((v1Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("72455b8b-382a-3705-b017-4e709d5856c6"))) ? ((v1Regions.size()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("80c04053-a0cb-3343-9b50-5ea7186e49c0"))) ? ((v1Regions != null) && ((v1Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8339ccec-d9b0-37e2-ae19-2bb8b873d347"))) ? ((v1Regions != null) || ((v1Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9e6b18f1-6fd9-394e-b9ed-636544cdc92f"))) ? ((v1Regions) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("21ddce15-1588-3f0a-9829-ba419d67a702"))) ? ((v1Regions.size()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("83c7b9af-b267-3d1c-b68d-23054d0afcc2"))) ? ((v1Regions != null) && ((v1Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("db55c365-605c-36e9-a5bd-0ca1c696c93b"))) ? (((v1Regions) != (null)) && ((v1Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("73c68e6f-3902-38ec-afe1-2134a8f9ab22"))) ? (((v1Regions) == (null)) && ((v1Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f0fb48fc-79a5-3fbc-92ba-4ce94dd1fa6c"))) ? (((v1Regions) != (null)) && ((v1Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("69b26b76-0c9f-34ac-9866-3ba76c3af3d4"))) ? (((v1Regions) == (null)) || (v1Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("023fbdcb-2eaa-38e2-9cb9-f156bdae851d"))) ? (((v1Regions) == (null)) || ((v1Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1cc65c4d-9c58-31be-ad31-d9f4126e901f"))) ? ((v1Regions.size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6cead44d-fb7d-3154-9d82-c80e6d87170f"))) ? (((v1Regions) != (null)) && ((v1Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c1f5e0b2-9650-36ab-96ce-23bfb419283a"))) ? ((v1Regions != null) && (v1Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("4c98edff-92b3-36bd-a3b1-df9ddad923ce"))) ? (((v1Regions) == (null)) && ((v1Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("853b3f16-2083-3374-a672-bcbf24f4d0da"))) ? (((v1Regions) == (null)) && ((v1Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a9cadd85-6f81-3c1e-b57a-ca6ce9afdc5a"))) ? ((v1Regions != null) && ((v1Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("82cf485b-1ec8-34a6-872a-e63500f616dd"))) ? ((v1Regions != null) && ((v1Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("63c16b2f-25f1-3591-9f99-d6d3eec498d1"))) ? ((v1Regions != null) || ((v1Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("620a6965-de81-379e-97ad-4677918ccc2c"))) ? ((v1Regions != null) || (v1Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6dcb2bd0-e4e0-36ca-b394-e4639332baed"))) ? (v1Regions != null) : (((KnobRuntime.check(java.util.UUID.fromString("76b622e5-f41d-3e8c-80d5-d8959fc4f6ed"))) ? (((v1Regions) == (null)) && ((v1Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("05a30b28-939a-300d-b285-26d1e7d71a4f"))) ? ((v1Regions.size()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("554c9fc3-fe25-352d-8553-f6af86069c48"))) ? ((v1Regions.size()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("89703386-10c1-3685-ba1d-06c00355645e"))) ? (((v1Regions) != (null)) || ((v1Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("324b9247-509e-3ce9-bbd0-baf46c07b5c2"))) ? ((v1Regions != null) || ((v1Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("66563ac6-616b-3c35-ad5d-9e7962994ddf"))) ? (((v1Regions) != (null)) || ((v1Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5447f5fc-fd73-3c3c-a8c7-448bc5fc01c8"))) ? (v1Regions.size() > 0) : (((KnobRuntime.check(java.util.UUID.fromString("dec44b22-9deb-3050-a790-9a413757590a"))) ? ((v1Regions != null) || ((v1Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3b179a1e-5e0b-3ed9-a760-ff129b66314a"))) ? ((v1Regions) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e361b324-02e1-321e-93c7-4acdfe64c0bd"))) ? ((v1Regions != null) && ((v1Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("489744bb-0989-3c53-ba02-302c2438718c"))) ? (((v1Regions) == (null)) || ((v1Regions.size()) == (0))) : (v1Regions != null && v1Regions.size() > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        dataManifestBuilder.addAllRegionManifests(v1Regions);
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("51454b70-3d56-31c5-939e-12dd481256ca"))) ? ((v2Regions != null) || ((v2Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("942085c6-6dbc-3350-a31b-7353550a3dd3"))) ? ((v2Regions.size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("814903ed-8fa8-3ed8-85c7-7c92f38aa9a5"))) ? (((v2Regions) == (null)) || ((v2Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6298c0d0-483d-3177-a83d-75c1008b4173"))) ? (((v2Regions) == (null)) || ((v2Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("54b60d2c-4e7e-3017-a16d-f70eef8afa54"))) ? ((v2Regions.size()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4d5d5b4c-ea9d-3cc2-999e-aecdca2b08cb"))) ? ((v2Regions != null) || ((v2Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8879b39e-86b8-3c41-8ac8-4da8c05e134b"))) ? ((v2Regions != null) || ((v2Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dd9ff756-9386-343b-b668-f2799c89afa4"))) ? ((v2Regions.size()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7a44a23e-9917-34ca-8916-1801fb619584"))) ? ((v2Regions != null) && ((v2Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac2e8776-695f-3367-b72e-79b7582f0870"))) ? ((v2Regions.size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c30a30cd-699c-35c1-a50e-7245b52ede4f"))) ? (((v2Regions) == (null)) || ((v2Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d3063149-1276-38b0-ba22-006ecb2b0bfe"))) ? (((v2Regions) == (null)) && ((v2Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("046f6ef0-ebee-3b79-8430-9e56daade856"))) ? ((v2Regions != null) && (v2Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("cfb81b62-6343-3241-9978-9a949732eaf6"))) ? (((v2Regions) == (null)) && ((v2Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3adc3895-0212-3a25-965a-6354fa03c0d3"))) ? (((v2Regions) == (null)) && ((v2Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("edc4545a-c236-305f-971e-20072afbde56"))) ? ((v2Regions != null) || ((v2Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a62f0b2-9bb6-3a28-8a25-1e000a4006ca"))) ? (((v2Regions) != (null)) || ((v2Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("96b87bd5-e3ab-3406-a610-7dc83e806448"))) ? ((v2Regions) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b66c36db-9d0b-3423-b85a-f502be35b661"))) ? ((v2Regions != null) && ((v2Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bbf98d49-9353-3486-a175-4d5163eb098c"))) ? (((v2Regions) != (null)) && ((v2Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("06fd3e4f-5590-3e81-b3c0-00413d8e5986"))) ? ((v2Regions != null) && ((v2Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("601e5927-f248-348c-b1dc-8466e06bc58a"))) ? (((v2Regions) != (null)) || ((v2Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b9e13a42-b7b9-3200-b9c3-bc2ea67d5422"))) ? (((v2Regions) != (null)) || ((v2Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("31105ced-f322-3427-b992-d33493dbd6f2"))) ? (((v2Regions) != (null)) || ((v2Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4388d58c-842c-33a8-8ee3-783cddadd7fe"))) ? (((v2Regions) == (null)) || ((v2Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fa96d3d2-ac1f-31aa-a414-b340dbeafe38"))) ? (((v2Regions) != (null)) && ((v2Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("99b34e8e-d0c7-3c9c-a781-066c30c7fd89"))) ? (((v2Regions) == (null)) && ((v2Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bf9ebe97-9b1c-35a3-aae1-2779dea0a2bb"))) ? (((v2Regions) != (null)) || (v2Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b5f820e3-06f7-3764-900c-82325475cc12"))) ? (v2Regions.size() > 0) : (((KnobRuntime.check(java.util.UUID.fromString("5eed7f78-e7f8-35f1-9482-cc7bf561db95"))) ? ((v2Regions != null) && ((v2Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9426cc6d-cd70-31f7-a81d-ebf06ef3a1b9"))) ? (((v2Regions) != (null)) && ((v2Regions.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f5e9affb-fb99-3084-b8f5-d23170b2aace"))) ? (((v2Regions) != (null)) || ((v2Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cb6f18da-f155-3c0b-b813-056dcf4e85bb"))) ? ((v2Regions != null) && ((v2Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0b42cf7a-b9ce-3562-a59f-556b600382fb"))) ? ((v2Regions != null) || ((v2Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d36450bb-42ee-32f6-95d1-dcbe6b10d888"))) ? (((v2Regions) == (null)) && ((v2Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("71764a68-aca2-3d48-b812-6d0e6fa399ba"))) ? ((v2Regions != null) || ((v2Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7e7d0e3b-fd3a-348c-b783-35bf15e2bbf1"))) ? ((v2Regions != null) && ((v2Regions.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0fd63058-5c01-3f83-a94b-905b8a70f669"))) ? (((v2Regions) == (null)) || ((v2Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f738215e-2365-356f-98da-4af049054139"))) ? ((v2Regions) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b55dbc89-a02a-3e22-bec0-c8be6c642476"))) ? (((v2Regions) != (null)) || ((v2Regions.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("60de843a-65d2-346a-aec2-45a9075104d2"))) ? (((v2Regions) == (null)) || (v2Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e0d639a6-c260-3e2d-ad7a-f4f3f3a385e8"))) ? (((v2Regions) == (null)) || ((v2Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ebacd021-4056-3c00-929b-8d5f59f2cc43"))) ? (((v2Regions) != (null)) && (v2Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3df02bee-f214-3e8e-b1c9-717a5702595a"))) ? ((v2Regions.size()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c8b0acae-2c86-3378-9f55-96d5c333dac6"))) ? (((v2Regions) != (null)) && ((v2Regions.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("539bc51d-26f4-3256-a05a-484bd7afcb36"))) ? (((v2Regions) != (null)) && ((v2Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d4a73565-5324-3c27-92a1-5c5a3d09cffe"))) ? (((v2Regions) == (null)) && ((v2Regions.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3c87ff78-cf0a-3cb6-90ea-a9271dabf6d5"))) ? (v2Regions != null) : (((KnobRuntime.check(java.util.UUID.fromString("682324cc-d910-3c8a-9c3b-44ca21ca68c9"))) ? (((v2Regions) == (null)) && (v2Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7b0558bb-841b-3d05-95b4-6c0ed166707e"))) ? ((v2Regions != null) || (v2Regions.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("aa790f35-233c-3b0b-9c25-530407670dac"))) ? (((v2Regions) != (null)) && ((v2Regions.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cdfd3b5f-11f8-3103-b4d2-ba783b46af54"))) ? ((v2Regions.size()) > (0)) : (v2Regions != null && v2Regions.size() > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        dataManifestBuilder.addAllRegionManifests(v2Regions);
      }

      // Write the v2 Data Manifest.
      // Once the data-manifest is written, the snapshot can be considered complete.
      // Currently snapshots are written in a "temporary" directory and later
      // moved to the "complated" snapshot directory.
      setStatusMsg("Writing data manifest for " + this.desc.getName());
      SnapshotDataManifest dataManifest = dataManifestBuilder.build();
      writeDataManifest(dataManifest);
      this.regionManifests = dataManifest.getRegionManifestsList();

      // Remove the region manifests. Everything is now in the data-manifest.
      // The delete operation is "relaxed", unless we get an exception we keep going.
      // The extra files in the snapshot directory will not give any problem,
      // since they have the same content as the data manifest, and even by re-reading
      // them we will get the same information.
      int totalDeletes = 0;
      ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(tpool);
      if (v1Regions != null) {
        for (SnapshotRegionManifest regionManifest : v1Regions) {
          ++totalDeletes;
          completionService.submit(() -> {
if(KnobRuntime.check(java.util.UUID.fromString("a4515b06-f15e-34bd-8e30-0f2a7bbce3ff"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b654d0b-09df-3fb9-9664-bcaad95ed411"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f4f6097-1954-356e-af78-ac2fb030d372"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("492d0b01-14be-3a79-99f7-03cec931b781"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("96c9a0ec-0828-33c7-b1a1-607f3bcef935"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50b26c60-1ed6-3096-877c-5b10d6366d25"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("86441713-661c-31e2-a42b-f0504189f94a"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a608979-f2e6-3c39-af04-ea04d9527e57"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a459ca0-6134-3a78-af53-3fac5b3850ad"))) {
try {
    java.lang.reflect.Field field = regionManifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionManifest));
    field.set(regionManifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            SnapshotManifestV1.deleteRegionManifest(workingDirFs, workingDir, regionManifest);
            return null;
          });
        }
      }
      if (v2Regions != null) {
        for (SnapshotRegionManifest regionManifest : v2Regions) {
          ++totalDeletes;
          completionService.submit(() -> {
            SnapshotManifestV2.deleteRegionManifest(workingDirFs, workingDir, regionManifest);
            return null;
          });
        }
      }
      // Wait for the deletes to finish.
      for (int i = 0; i < totalDeletes; i++) {
        try {
          completionService.take().get();
        } catch (InterruptedException ie) {
          throw new InterruptedIOException(ie.getMessage());
        } catch (ExecutionException e) {
          throw new IOException("Error deleting region manifests", e.getCause());
        }
      }
    } finally {
      tpool.shutdown();
    }
  }

  /*
   * Write the SnapshotDataManifest file
   */
  private void writeDataManifest(final SnapshotDataManifest manifest) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("fca283b9-7de0-3813-8b57-a6913951af51"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("677f7dfd-7d26-3b29-9022-390b7b2c6296"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e3b04df-361e-3259-813e-0bd5fbb6a905"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9eeff7ad-301f-3ffa-8aae-cb970a8ac7d7"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c08b3cbd-5958-3c10-a5c5-2f7ab953aac1"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9e39c15-ac85-3411-99f5-f965c85405fe"))) {
return;
}
    try (
      FSDataOutputStream stream = workingDirFs.create(new Path(workingDir, DATA_MANIFEST_NAME))) {
      manifest.writeTo(stream);
    }
  }

  /*
   * Read the SnapshotDataManifest file
   */
  private SnapshotDataManifest readDataManifest() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("288bab03-eb06-3123-9c28-569632f1cdc3"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("223b224b-27e2-3432-87ae-bbb1c50b3afb"))) {
throw new java.io.IOException("Injected exception");
}
    try (FSDataInputStream in = workingDirFs.open(new Path(workingDir, DATA_MANIFEST_NAME))) {
      CodedInputStream cin = CodedInputStream.newInstance(in);
      cin.setSizeLimit(manifestSizeLimit);
      return SnapshotDataManifest.parseFrom(cin);
    } catch (FileNotFoundException e) {
      return null;
    } catch (InvalidProtocolBufferException e) {
      throw new CorruptedSnapshotException("unable to parse data manifest " + e.getMessage(), e);
    }
  }

  private ThreadPoolExecutor createExecutor(final String name) {
    return createExecutor(conf, name);
  }

  public static ThreadPoolExecutor createExecutor(final Configuration conf, final String name) {
    int maxThreads = conf.getInt("hbase.snapshot.thread.pool.max", 8);
    return Threads.getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
      new ThreadFactoryBuilder().setNameFormat(name + "-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
  }

  /**
   * Extract the region encoded name from the region manifest
   */
  static String getRegionNameFromManifest(final SnapshotRegionManifest manifest) {
    byte[] regionName =
      RegionInfo.createRegionName(ProtobufUtil.toTableName(manifest.getRegionInfo().getTableName()),
        manifest.getRegionInfo().getStartKey().toByteArray(),
        manifest.getRegionInfo().getRegionId(), true);
    return RegionInfo.encodeRegionName(regionName);
  }

  /*
   * Return the snapshot format
   */
  private static int getSnapshotFormat(final SnapshotDescription desc) {
    return desc.hasVersion() ? desc.getVersion() : SnapshotManifestV1.DESCRIPTOR_VERSION;
  }
}
