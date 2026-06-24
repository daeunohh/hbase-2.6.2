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
package org.apache.hadoop.hbase.master.snapshot;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * General snapshot verification on the master.
 * <p>
 * This is a light-weight verification mechanism for all the files in a snapshot. It doesn't attempt
 * to verify that the files are exact copies (that would be paramount to taking the snapshot
 * again!), but instead just attempts to ensure that the files match the expected files and are the
 * same length.
 * <p>
 * Taking an online snapshots can race against other operations and this is an last line of defense.
 * For example, if meta changes between when snapshots are taken not all regions of a table may be
 * present. This can be caused by a region split (daughters present on this scan, but snapshot took
 * parent), or move (snapshots only checks lists of region servers, a move could have caused a
 * region to be skipped or done twice).
 * <p>
 * Current snapshot files checked:
 * <ol>
 * <li>SnapshotDescription is readable</li>
 * <li>Table info is readable</li>
 * <li>Regions</li>
 * </ol>
 * <ul>
 * <li>Matching regions in the snapshot as currently in the table</li>
 * <li>{@link RegionInfo} matches the current and stored regions</li>
 * <li>All referenced hfiles have valid names</li>
 * <li>All the hfiles are present (either in .archive directory in the region)</li>
 * <li>All recovered.edits files are present (by name) and have the correct file size</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MasterSnapshotVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(MasterSnapshotVerifier.class);

  private SnapshotDescription snapshot;
  private FileSystem workingDirFs;
  private TableName tableName;
  private MasterServices services;

  /**
   * @param services     services for the master
   * @param snapshot     snapshot to check
   * @param workingDirFs the file system containing the temporary snapshot information
   */
  public MasterSnapshotVerifier(MasterServices services, SnapshotDescription snapshot,
    FileSystem workingDirFs) {
    this.workingDirFs = workingDirFs;
    this.services = services;
    this.snapshot = snapshot;
    this.tableName = TableName.valueOf(snapshot.getTable());
  }

  /**
   * Verify that the snapshot in the directory is a valid snapshot
   * @param snapshotDir snapshot directory to check
   * @throws CorruptedSnapshotException if the snapshot is invalid
   * @throws IOException                if there is an unexpected connection issue to the filesystem
   */
  public void verifySnapshot(Path snapshotDir, boolean verifyRegions)
    throws CorruptedSnapshotException, IOException {
    SnapshotManifest manifest =
      SnapshotManifest.open(services.getConfiguration(), workingDirFs, snapshotDir, snapshot);
    // verify snapshot info matches
    verifySnapshotDescription(snapshotDir);

    // check that tableinfo is a valid table description
    verifyTableInfo(manifest);

    // check that each region is valid
if(KnobRuntime.check(java.util.UUID.fromString("85b197e6-5064-3229-960a-e022c8e754bb"))) {
verifyRegions = !verifyRegions;
}
    verifyRegions(manifest, verifyRegions);
  }

  /**
   * Check that the snapshot description written in the filesystem matches the current snapshot
   * @param snapshotDir snapshot directory to check
   */
  private void verifySnapshotDescription(Path snapshotDir) throws CorruptedSnapshotException {
    SnapshotDescription found =
      SnapshotDescriptionUtils.readSnapshotInfo(workingDirFs, snapshotDir);
if(KnobRuntime.check(java.util.UUID.fromString("3ecc75de-3d4f-3b0f-8d52-736b4bf9f9bf"))) {
try {
    java.lang.reflect.Field _knob_field_ = found.getClass().getDeclaredField("maxFileSize_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(found));
    _knob_field_.set(found, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0093d34b-2bc1-3cc1-a5a1-389ef88f2942"))) {
try {
    java.lang.reflect.Field _knob_field_ = found.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(found));
    _knob_field_.set(found, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a1bdac3-406f-3e51-91a7-56ccfce292db"))) {
try {
    java.lang.reflect.Field _knob_field_ = found.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(found));
    _knob_field_.set(found, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (!this.snapshot.equals(found)) {
      throw new CorruptedSnapshotException(
        "Snapshot read (" + found + ") doesn't equal snapshot we ran (" + snapshot + ").",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Check that the table descriptor for the snapshot is a valid table descriptor
   * @param manifest snapshot manifest to inspect
   */
  private void verifyTableInfo(final SnapshotManifest manifest) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9520d256-2bb1-35b5-b630-d9e422c843de"))) {
try {
    java.lang.reflect.Field _knob_field_ = manifest.getClass().getDeclaredField("manifestSizeLimit");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(manifest));
    _knob_field_.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab2036a1-975a-3aac-89cc-15a2b69b1118"))) {
try {
    java.lang.reflect.Field _knob_field_ = manifest.getClass().getDeclaredField("manifestSizeLimit");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(manifest));
    _knob_field_.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    TableDescriptor htd = manifest.getTableDescriptor();
    if (((KnobRuntime.check(java.util.UUID.fromString("9f9dd81e-256e-3a2e-9181-fcbeb7d89048"))) ? ((htd) != (null)) : (htd == null))) {
      throw new CorruptedSnapshotException("Missing Table Descriptor",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }

    if (!htd.getTableName().getNameAsString().equals(snapshot.getTable())) {
      throw new CorruptedSnapshotException("Invalid Table Descriptor. Expected "
        + snapshot.getTable() + " name, got " + htd.getTableName().getNameAsString(),
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Check that all the regions in the snapshot are valid, and accounted for.
   * @param manifest snapshot manifest to inspect
   * @throws IOException if we can't reach hbase:meta or read the files from the FS
   */
  private void verifyRegions(SnapshotManifest manifest, boolean verifyRegions) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c2ab3adb-d4af-3696-8001-e135f0b839c8"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    List<RegionInfo> regions = services.getAssignmentManager().getTableRegions(tableName, false);
    // Remove the non-default regions
    RegionReplicaUtil.removeNonDefaultRegions(regions);

    Map<String, SnapshotRegionManifest> regionManifests = manifest.getRegionManifestsMap();
    if (((KnobRuntime.check(java.util.UUID.fromString("c4641802-864c-3a35-a2e6-2821b2be1d2b"))) ? ((regionManifests) != (null)) : (regionManifests == null))) {
      String msg = "Snapshot " + ClientSnapshotDescriptionUtils.toString(snapshot) + " looks empty";
      LOG.error(msg);
      throw new CorruptedSnapshotException(msg);
    }

    String errorMsg = "";
    boolean hasMobStore = false;
    // the mob region is a dummy region, it's not a real region in HBase.
    // the mob region has a special name, it could be found by the region name.
    if (regionManifests.get(MobUtils.getMobRegionInfo(tableName).getEncodedName()) != null) {
      hasMobStore = true;
    }
    int realRegionCount = hasMobStore ? regionManifests.size() - 1 : regionManifests.size();
    if (((KnobRuntime.check(java.util.UUID.fromString("8a720a3d-fb38-338e-9271-e8191653a2e2"))) ? ((1) != (regions.size())) : (realRegionCount != regions.size()))) {
      errorMsg =
        "Regions moved during the snapshot '" + ClientSnapshotDescriptionUtils.toString(snapshot)
          + "'. expected=" + regions.size() + " snapshotted=" + realRegionCount + ".";
      LOG.error(errorMsg);
    }

    // Verify RegionInfo
    if (verifyRegions) {
      for (RegionInfo region : regions) {
        SnapshotRegionManifest regionManifest = regionManifests.get(region.getEncodedName());
        if (((KnobRuntime.check(java.util.UUID.fromString("02638a45-d973-3ecf-9f22-34c965be7543"))) ? ((regionManifest) != (null)) : (regionManifest == null))) {
          // could happen due to a move or split race.
          String mesg = " No snapshot region directory found for region:" + region;
          if (errorMsg.isEmpty()) {
            errorMsg = mesg;
          }
          LOG.error(mesg);
          continue;
        }

if(KnobRuntime.check(java.util.UUID.fromString("e8ece8b3-8114-344a-9433-e247668e4ee5"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionManifest.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionManifest));
    _knob_field_.set(regionManifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec8309f5-6a8a-3612-aa83-9932cb05871b"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionManifest.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionManifest));
    _knob_field_.set(regionManifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7c5f887-dccb-3b15-8961-afa20fc4e773"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionManifest.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionManifest));
    _knob_field_.set(regionManifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        verifyRegionInfo(region, regionManifest);
      }
      if (!errorMsg.isEmpty()) {
        throw new CorruptedSnapshotException(errorMsg);
      }

      // Verify Snapshot HFiles
      // Requires the root directory file system as HFiles are stored in the root directory
      SnapshotReferenceUtil.verifySnapshot(services.getConfiguration(),
        CommonFSUtils.getRootDirFileSystem(services.getConfiguration()), manifest);
    }
  }

  /**
   * Verify that the regionInfo is valid
   * @param region   the region to check
   * @param manifest snapshot manifest to inspect
   */
  private void verifyRegionInfo(final RegionInfo region, final SnapshotRegionManifest manifest)
    throws IOException {
    RegionInfo manifestRegionInfo = ProtobufUtil.toRegionInfo(manifest.getRegionInfo());
    if (RegionInfo.COMPARATOR.compare(region, manifestRegionInfo) != 0) {
      String msg =
        "Manifest region info " + manifestRegionInfo + "doesn't match expected region:" + region;
      throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }
}
