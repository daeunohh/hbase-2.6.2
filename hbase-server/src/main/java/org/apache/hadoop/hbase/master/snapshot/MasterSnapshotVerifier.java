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
if(KnobRuntime.check(java.util.UUID.fromString("be65cf9c-7e32-347a-80c8-8ea9ed9e6dd6"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b6088e6f-981f-330c-97e6-71dc4c592d4d"))) {
throw new java.io.IOException("Injected exception");
}
    SnapshotManifest manifest =
      SnapshotManifest.open(services.getConfiguration(), workingDirFs, snapshotDir, snapshot);
    // verify snapshot info matches
    verifySnapshotDescription(snapshotDir);

    // check that tableinfo is a valid table description
if(KnobRuntime.check(java.util.UUID.fromString("2fbe1bb2-9bc0-319e-bb8a-3aa016997146"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86fbe577-b23a-3808-b05e-de91797631e7"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17d2e94a-1f27-3f48-9f86-97108f8b2bba"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1797b2d0-b9a5-3593-98af-41fb48bc75a4"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e2d31b1-cfa1-351a-9719-a1e6334b663e"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    verifyTableInfo(manifest);

    // check that each region is valid
if(KnobRuntime.check(java.util.UUID.fromString("ed2efefc-433e-3fbe-a4e9-b3e1e48f9d48"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("85b197e6-5064-3229-960a-e022c8e754bb"))) {
verifyRegions = !verifyRegions;
}
if(KnobRuntime.check(java.util.UUID.fromString("011bf471-7a07-3ab1-8acb-35526eb67906"))) {
verifyRegions = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d0aefc2-abe9-32a1-818d-50c50388a4f7"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c93bad5-cd2a-31f1-9215-46276a778be1"))) {
verifyRegions = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("9f69b9b7-b3e7-325c-b40c-8bd90144f260"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc77474c-b0a6-3612-8337-93134424c146"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5f7a00ea-e95d-3d99-ad9a-c247dae18600"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("6f0b27e1-aa3c-3532-bc27-8f6dd65277f8"))) { verifyRegions(manifest, !verifyRegions); } else { verifyRegions(manifest, verifyRegions); }
  }

  /**
   * Check that the snapshot description written in the filesystem matches the current snapshot
   * @param snapshotDir snapshot directory to check
   */
  private void verifySnapshotDescription(Path snapshotDir) throws CorruptedSnapshotException {
if(KnobRuntime.check(java.util.UUID.fromString("520a5cc3-6f10-386c-9a52-1e91b2f6053e"))) {
return;
}
    SnapshotDescription found =
      SnapshotDescriptionUtils.readSnapshotInfo(workingDirFs, snapshotDir);
if(KnobRuntime.check(java.util.UUID.fromString("e9102095-7072-3d90-81b4-06822015c357"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1dfa5856-d8e9-3ec0-b085-18f14611a5e9"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7e2203b-5fdd-3dbf-8658-2bc655928594"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9bf878d-1196-3576-ad48-2a9f7fa9ad8b"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(found));
    field.set(found, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("335bb19b-56dd-3d6b-9915-c27a5ece3ebb"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("163deea5-5f47-375d-98fc-45d4de593350"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("725e436a-69f5-3351-827b-b2b67e9be2cb"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15d4fb28-f1ce-33d2-bd0d-480d63c5a8ed"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83c79f18-d5f2-3a44-a5e4-51b96091e3c3"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(found));
    field.set(found, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65c83414-4e95-3319-9bf2-e7dd1418e5db"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3ab2e61-eda1-3662-a109-9a1b728cf5aa"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(found));
    field.set(found, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f132d71c-4c04-3604-957e-a3a890553498"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f907ecb9-41ce-3097-98a1-5bf6adc112b0"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71a9a5d3-a964-3af3-9783-32e03d3fecd0"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84b4bf1e-9dc6-39c7-b839-8aac7366920e"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(found));
    field.set(found, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45c5aec2-0621-350b-967b-d0872f76460a"))) {
try {
    java.lang.reflect.Field field = found.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(found));
    field.set(found, oldValue * 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("a29984a8-cabb-32cd-ae28-b77db1c71517"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f55e362a-6cf8-3d92-aa6c-ff73eec9b944"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("3eaffe23-42f8-3180-848e-3d4985a2253e"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71cd66d4-a1e5-3b6d-8e9c-882db291a888"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6ab1b317-6081-316f-aef9-28f6ecfa10e8"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2208fce3-8141-39e9-82c9-cd4c8755086f"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    TableDescriptor htd = manifest.getTableDescriptor();
    if (((KnobRuntime.check(java.util.UUID.fromString("a63546d1-352b-30fa-828f-6a820df40554"))) ? ((htd) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9f9dd81e-256e-3a2e-9181-fcbeb7d89048"))) ? ((htd) != (null)) : (htd == null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7d99cbed-3ab3-3c89-8f52-4e6d71e40add"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3e2455d3-4ce5-3a68-a694-d0a2155726a2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d1c6eff4-a579-3e46-a1bd-9118c6a90b77"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56834854-e31c-3411-8558-64b5c0efe46d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f9318ca2-2661-3634-9ed1-00a08fec4bcc"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8a34ac7-136e-385e-a779-f830073f31d4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f846ff51-094c-3616-9056-5c6cb3b27ce3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ae383a69-30c0-3ac6-9f0a-652dc129764a"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b734814-8b53-3076-bc00-134016a12116"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f18911a-a430-37a4-9645-07b9f15434f4"))) {
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
    List<RegionInfo> regions = services.getAssignmentManager().getTableRegions(tableName, false);
    // Remove the non-default regions
    RegionReplicaUtil.removeNonDefaultRegions(regions);

    Map<String, SnapshotRegionManifest> regionManifests = manifest.getRegionManifestsMap();
    if (((KnobRuntime.check(java.util.UUID.fromString("c4641802-864c-3a35-a2e6-2821b2be1d2b"))) ? ((regionManifests) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fcf3989d-5b79-353f-8385-1f234e682cef"))) ? ((regionManifests) == (null)) : (regionManifests == null))))) {
      String msg = "Snapshot " + ClientSnapshotDescriptionUtils.toString(snapshot) + " looks empty";
      LOG.error(msg);
      throw new CorruptedSnapshotException(msg);
    }

    String errorMsg = "";
    boolean hasMobStore = false;
    // the mob region is a dummy region, it's not a real region in HBase.
    // the mob region has a special name, it could be found by the region name.
if(KnobRuntime.check(java.util.UUID.fromString("3c2a5767-cd8a-38b1-aafa-d1a6bd148a58"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba12d926-8119-335b-8350-433bb22d1ef6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bc9e426b-8358-3950-998f-008fe8c9431f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e371c78c-5010-3639-80c2-85e0336bf10a"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("69c35dbb-c92d-37d4-bf9b-5eae5bd690bd"))) ? ((regionManifests.get(MobUtils.getMobRegionInfo(tableName).getEncodedName())) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("37d71162-c3a8-393b-9451-05fc31af1db8"))) ? ((regionManifests.get(MobUtils.getMobRegionInfo(tableName).getEncodedName())) == (null)) : (regionManifests.get(MobUtils.getMobRegionInfo(tableName).getEncodedName()) != null))))) {
      hasMobStore = true;
    }
    int realRegionCount = hasMobStore ? regionManifests.size() - 1 : regionManifests.size();
    if (((KnobRuntime.check(java.util.UUID.fromString("dbd77989-a703-320c-8749-39fb906fa6c8"))) ? ((realRegionCount) != (regions.size())) : (((KnobRuntime.check(java.util.UUID.fromString("8a720a3d-fb38-338e-9271-e8191653a2e2"))) ? ((1) != (regions.size())) : (((KnobRuntime.check(java.util.UUID.fromString("8cffc98a-754f-3cd0-b73f-72dea5be95dd"))) ? ((1) == (regions.size())) : (((KnobRuntime.check(java.util.UUID.fromString("18ccc9a0-e407-3128-86f3-04bba153aa5b"))) ? ((realRegionCount) == (regions.size())) : (realRegionCount != regions.size()))))))))) {
      errorMsg =
        "Regions moved during the snapshot '" + ClientSnapshotDescriptionUtils.toString(snapshot)
          + "'. expected=" + regions.size() + " snapshotted=" + realRegionCount + ".";
      LOG.error(errorMsg);
    }

    // Verify RegionInfo
    if (((KnobRuntime.check(java.util.UUID.fromString("a5466dff-dc2e-3ffe-9460-a1417bcaf6c0"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("ad0d3770-c1c5-32e8-bab3-3353274c6d10"))) ? (!verifyRegions) : (((KnobRuntime.check(java.util.UUID.fromString("91364ca8-9c4f-3736-b0e8-a2fbb46bd111"))) ? (false) : (verifyRegions))))))) {
      for (RegionInfo region : regions) {
        SnapshotRegionManifest regionManifest = regionManifests.get(region.getEncodedName());
        if (((KnobRuntime.check(java.util.UUID.fromString("77c62e62-3b47-36ed-a6cf-81fd8f480b0d"))) ? ((regionManifest) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("02638a45-d973-3ecf-9f22-34c965be7543"))) ? ((regionManifest) != (null)) : (regionManifest == null))))) {
          // could happen due to a move or split race.
          String mesg = " No snapshot region directory found for region:" + region;
          if (errorMsg.isEmpty()) {
            errorMsg = mesg;
          }
          LOG.error(mesg);
          continue;
        }

if(KnobRuntime.check(java.util.UUID.fromString("f78ba9cc-2056-306a-8866-59345c983eb3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7383c235-77a9-3a93-8465-352671416191"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a920258a-8877-3b31-b9a7-e514dfacf34c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6b1f1b20-c0f1-3b06-9e68-9556948adccd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7fc77fc7-ffb8-36ba-8c94-1461db4cc259"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7a92aa0c-f075-37af-ad48-e49d3fb38010"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b64cb938-797f-3dd2-b42e-00e9f180544d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5c930c04-7deb-3baf-ba92-9ec34e9a954e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7ad4e046-7f02-3e69-aca0-5b2f77ee8d17"))) {
throw new java.io.IOException("Injected exception");
}
        verifyRegionInfo(region, regionManifest);
      }
      if (!errorMsg.isEmpty()) {
        throw new CorruptedSnapshotException(errorMsg);
      }

      // Verify Snapshot HFiles
      // Requires the root directory file system as HFiles are stored in the root directory
if(KnobRuntime.check(java.util.UUID.fromString("ae5c17bc-1177-395e-923e-92749eab9837"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("853efaed-58ab-3ba0-ad01-22093671481e"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9dad0fd8-6444-3b8b-85c4-6db666dcb98d"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5cde149-4ed9-334f-94d2-03c8911bd83c"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("manifestSizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("781be762-44d4-31a7-9b2e-f7d053d96ac2"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("9e90f15e-8f25-3bd6-9bc2-aee555aa6a6c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f4d6cfdb-7152-3336-8ace-8cda3ad694b9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("168752ce-db01-31a9-9e1e-1a04bd497a7e"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa583b7e-6dbb-3c2a-ae3d-5493a64d6c3d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1660d90c-f039-3c51-9ffe-c20808b5d93d"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9fa79570-59c3-3f88-b870-e4b8c078ab9d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("16f912f1-b484-3197-9eb8-6d5eb8363daf"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("005ef5c2-4e2c-3549-a2f2-6f848561a632"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d0a4b3b-9f45-31de-b7d7-1547ebf3e799"))) {
try {
    java.lang.reflect.Field field = manifest.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(manifest));
    field.set(manifest, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28d0ae97-50c4-3be1-a02f-349538f53a6e"))) {
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
    RegionInfo manifestRegionInfo = ProtobufUtil.toRegionInfo(manifest.getRegionInfo());
    if (((KnobRuntime.check(java.util.UUID.fromString("ca273564-ded3-3e94-b9fd-90fe0425572e"))) ? ((RegionInfo.COMPARATOR.compare(region, manifestRegionInfo)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a564ae47-04eb-3651-87da-2e3bcc61631e"))) ? ((RegionInfo.COMPARATOR.compare(region, manifestRegionInfo)) == (0)) : (RegionInfo.COMPARATOR.compare(region, manifestRegionInfo) != 0))))) {
      String msg =
        "Manifest region info " + manifestRegionInfo + "doesn't match expected region:" + region;
      throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }
}
