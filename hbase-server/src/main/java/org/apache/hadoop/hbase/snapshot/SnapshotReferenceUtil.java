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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * Utility methods for interacting with the snapshot referenced files.
 */
@InterfaceAudience.Private
public final class SnapshotReferenceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotReferenceUtil.class);

  public interface StoreFileVisitor {
    void storeFile(final RegionInfo regionInfo, final String familyName,
      final SnapshotRegionManifest.StoreFile storeFile) throws IOException;
  }

  public interface SnapshotVisitor extends StoreFileVisitor {
  }

  private SnapshotReferenceUtil() {
    // private constructor for utility class
  }

  /**
   * Iterate over the snapshot store files
   * @param conf        The current {@link Configuration} instance.
   * @param fs          {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor     callback object to get the referenced files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitReferencedFiles(final Configuration conf, final FileSystem fs,
    final Path snapshotDir, final SnapshotVisitor visitor) throws IOException {
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    visitReferencedFiles(conf, fs, snapshotDir, desc, visitor);
  }

  /**
   * Iterate over the snapshot store files, restored.edits and logs
   * @param conf        The current {@link Configuration} instance.
   * @param fs          {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param desc        the {@link SnapshotDescription} of the snapshot to verify
   * @param visitor     callback object to get the referenced files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitReferencedFiles(final Configuration conf, final FileSystem fs,
    final Path snapshotDir, final SnapshotDescription desc, final SnapshotVisitor visitor)
    throws IOException {
    visitTableStoreFiles(conf, fs, snapshotDir, desc, visitor);
  }

  /**
   * © Iterate over the snapshot store files
   * @param conf        The current {@link Configuration} instance.
   * @param fs          {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param desc        the {@link SnapshotDescription} of the snapshot to verify
   * @param visitor     callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  static void visitTableStoreFiles(final Configuration conf, final FileSystem fs,
    final Path snapshotDir, final SnapshotDescription desc, final StoreFileVisitor visitor)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1ebd06fb-d011-39c6-b36f-a88da2385476"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e54e69e6-c4d4-360c-8998-4984195c10f5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("59ffa88b-16a4-3414-b7b9-4c77b205193f"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("09ae72e6-760c-3b4a-a73f-0977872a68ac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7473a6bc-682a-3e36-b77a-c6e9d82a8648"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("12eba472-e636-3ef7-b125-8ea21f6e33fa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d8be6297-06e4-3204-bd0d-f6cf7f8d53b9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d6c1376d-b14c-3613-8e95-eeeb358b2666"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a67b0164-ee02-314b-9be7-b7d6aa6ea6cd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("867aacc3-1a0c-3684-9778-9f6239224f5a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("65278f31-51ab-3db6-8d67-0ad8bc37c124"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7d893262-b579-3b26-91ed-75273bb3ade3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ee40a511-e56b-3b13-a1cb-109c872929d3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fd7ca3c5-d487-3df2-a4b9-4fbbadee64c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c3080af2-d55f-3db1-8abc-9a40615ec071"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9e47e464-1f2d-3706-a829-a9e1fcdd58c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6441cb55-0898-3de4-a2de-6f0e13cea861"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4489cdeb-8e20-34e2-994b-1586593b64d3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("65018f10-7574-3e14-aedf-6414b2208a6f"))) {
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
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, desc);
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (((KnobRuntime.check(java.util.UUID.fromString("9dee2a9c-8977-3102-9a10-1c2c33a12490"))) ? (regionManifests == null) : (((KnobRuntime.check(java.util.UUID.fromString("e842b9cb-7fa0-36e0-99d1-77fbad67dfba"))) ? (((regionManifests) != (null)) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("cf4760f6-6c2a-396c-8ed2-253c2a919fcc"))) ? ((regionManifests == null) && (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("5e36f6fe-902d-3e8a-848e-d010c79426a9"))) ? ((regionManifests) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7a5e7dcb-ed05-3371-bcc9-31e33c2133f5"))) ? (((regionManifests) == (null)) && (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("9713e5c1-a9b9-3bd3-b2c7-3a2f90288596"))) ? ((regionManifests == null) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("3a2e1622-3686-3554-8134-46b03606b589"))) ? (regionManifests.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("39f88156-c0c1-3d10-8164-1832a4fd5669"))) ? (((regionManifests) != (null)) && (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("e8773ccd-3153-33f7-b321-3e5a9825a911"))) ? ((regionManifests) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5d958da9-36a5-3697-9d10-0aacf042ac0f"))) ? (((regionManifests) == (null)) || (regionManifests.isEmpty())) : (regionManifests == null || regionManifests.isEmpty()))))))))))))))))))))) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    for (SnapshotRegionManifest regionManifest : regionManifests) {
if(KnobRuntime.check(java.util.UUID.fromString("9eafc39e-c08c-33f4-82e5-c807a03108ff"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8c345ae8-8013-3e5d-93e3-4e2501359257"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4fa36682-945b-3e91-8fb6-74997e496b27"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d5e3c328-306d-31de-9632-0b4a8b434cab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("17a6f1b3-78c1-3548-8b93-750a83c9db31"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eabb10d3-0be4-3345-96c9-26255d8c7304"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("edf0f98a-527d-316d-aef4-ef0e45a8f607"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("849ae2e1-d1b3-3773-8ffe-79316a1de5dd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a2206d96-d36b-3696-a509-a69b4af11459"))) {
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
      visitRegionStoreFiles(regionManifest, visitor);
    }
  }

  /**
   * Iterate over the snapshot store files in the specified region
   * @param manifest snapshot manifest to inspect
   * @param visitor  callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRegionStoreFiles(final SnapshotRegionManifest manifest,
    final StoreFileVisitor visitor) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d93a6f9b-9b1a-332b-ae5f-8c13aeb55caa"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("840f63a1-f51d-39e8-9e52-6fcc7bd0bb39"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3164150b-4ea9-3256-af10-cd38dfec763e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("76374cff-e0e3-3dee-a1c2-2d851c2a8529"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("25ef8abf-76a2-3e78-8501-425c25c7edbd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0abaca6b-4217-3721-afb3-04447383e63e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("743bfc38-08c5-3f31-adaf-9e3b2be93c9b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9c9227a4-30b6-3e9a-acff-92e0da006b04"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8613d9a-d730-34d1-828b-d9fe7fc2ce72"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5f0b6da0-1b07-3963-a9ff-933130fd9fb4"))) {
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
    RegionInfo regionInfo = ProtobufUtil.toRegionInfo(manifest.getRegionInfo());
    for (SnapshotRegionManifest.FamilyFiles familyFiles : manifest.getFamilyFilesList()) {
      String familyName = familyFiles.getFamilyName().toStringUtf8();
      for (SnapshotRegionManifest.StoreFile storeFile : familyFiles.getStoreFilesList()) {
if(KnobRuntime.check(java.util.UUID.fromString("832cdb2b-5cab-33e0-817f-330d380a01b1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6cac58cc-50dd-3f46-adb3-2994e96a1c0e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d04dc617-35e8-3e44-85ac-f5b0c3310d58"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f37ba07a-1e8b-3ed6-b1fe-539b56e8a748"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("27038d6f-d6eb-3654-b813-da95762281cc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0e9a7940-d9db-3a15-9bd5-b9fbf8fbf69b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("99874cd2-08a3-3542-93ce-6657c3c571c0"))) {
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
        visitor.storeFile(regionInfo, familyName, storeFile);
      }
    }
  }

  /**
   * Verify the validity of the snapshot
   * @param conf         The current {@link Configuration} instance.
   * @param fs           {@link FileSystem}
   * @param snapshotDir  {@link Path} to the Snapshot directory of the snapshot to verify
   * @param snapshotDesc the {@link SnapshotDescription} of the snapshot to verify
   * @throws CorruptedSnapshotException if the snapshot is corrupted
   * @throws IOException                if an error occurred while scanning the directory
   */
  public static void verifySnapshot(final Configuration conf, final FileSystem fs,
    final Path snapshotDir, final SnapshotDescription snapshotDesc) throws IOException {
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    verifySnapshot(conf, fs, manifest);
  }

  /**
   * Verify the validity of the snapshot
   * @param conf     The current {@link Configuration} instance.
   * @param fs       {@link FileSystem}
   * @param manifest snapshot manifest to inspect
   * @throws CorruptedSnapshotException if the snapshot is corrupted
   * @throws IOException                if an error occurred while scanning the directory
   */
  public static void verifySnapshot(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("30e75c0f-646c-35b9-8ecd-2b98e3ef5392"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("972a2658-a6aa-3eb6-b55b-1c803d5b2ba4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c2a5cf39-3591-39df-ac14-00896515492f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("efe40bcc-d65d-3340-adab-a3a06483372d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc5975e7-98c4-3e22-9521-c40fa392b6bd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c4e985fe-b484-3edf-8125-f88eb425ed74"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e838b72d-a4a2-36e3-9f07-899c491818a7"))) {
return;
}
    final SnapshotDescription snapshotDesc = manifest.getSnapshotDescription();
    final Path snapshotDir = manifest.getSnapshotDir();
if(KnobRuntime.check(java.util.UUID.fromString("f16b27d1-8057-3c5b-9790-520095170b66"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0e642637-9a55-3942-b4a7-5123a757f157"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("744c301b-e6bd-3c88-914f-3f3e16476093"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f874fe6d-a36f-3cef-aec0-dc34b7eac2b8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc81f153-61d6-3f37-87d9-0591de1e44cb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8b7198ae-9236-3366-a908-14283ec42b4c"))) {
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
    concurrentVisitReferencedFiles(conf, fs, manifest, "VerifySnapshot", new StoreFileVisitor() {
      @Override
      public void storeFile(final RegionInfo regionInfo, final String family,
        final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("81f767bf-c2bf-363a-ac8d-6899de10cd0a"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("020a859c-0022-3343-963c-e4e6aae47aa6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4f3d74b9-2b30-3cae-9f14-a0bcbcdc169f"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a23e494a-6199-3df0-9d1f-0df2c0dd35d8"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84251973-ad9b-3af5-a4a9-18c628fc4a0a"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d213f3a8-59ae-341e-83b6-1cf01588c6c4"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d897977-3842-33a4-8fb3-90196277b502"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e715a156-fa23-32fa-959f-1faf172a3983"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8437afa8-fdbf-3538-bf9d-89062c0927ff"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e152771d-b01d-3a00-94e8-4a1a68039dda"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("887173ad-fa6b-30c2-b913-fc51c5894a63"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1562530a-7b4a-37c6-a0b2-080d4413166c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("969148e6-803c-3963-8e2c-c36e3aeb0289"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9c59e209-694d-3e55-9413-68cada434198"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a750cca-1ca7-3664-8dee-9142ffa3f26f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8bced1eb-c7fa-38eb-ac18-75771b25623d"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83445708-e8fc-3570-92fd-705dddafa031"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("045ff39c-7b83-3dbb-878f-3c68ce0ed0e6"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d158024-b4d3-3ac8-8a99-ead9651ba291"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5978dcaf-3516-3549-a17a-634775d18198"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("91d77b40-bb36-3c18-b3e8-b7b29c45ff65"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8bccd1b-63a4-3889-ae3b-d90976f4e529"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b99436a9-fec7-3bb9-a68c-61cbfa1d490d"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68230fee-3684-3a9c-b210-b9bed600d704"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc1e2e97-b3d1-3590-b31b-31353a5d7d1d"))) {
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
        verifyStoreFile(conf, fs, snapshotDir, snapshotDesc, regionInfo, family, storeFile);
      }
    });
  }

  /**
   * Verify the validity of the snapshot.
   * @param visitor user-specified store file visitor
   */
  public static void verifySnapshot(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest, final StoreFileVisitor visitor) throws IOException {
    concurrentVisitReferencedFiles(conf, fs, manifest, "VerifySnapshot", visitor);
  }

  public static void concurrentVisitReferencedFiles(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest, final String desc, final StoreFileVisitor visitor)
    throws IOException {

    final Path snapshotDir = manifest.getSnapshotDir();
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (((KnobRuntime.check(java.util.UUID.fromString("30e8a7db-7e8b-3626-a0bf-c914c585e688"))) ? (regionManifests == null) : (((KnobRuntime.check(java.util.UUID.fromString("7f3a3a80-b000-38c2-b98b-4d30291bb5f1"))) ? (regionManifests.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("7ee69e22-33e9-306f-ac58-c41c1db6c0f9"))) ? ((regionManifests) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8bd8ad48-1f0b-3bef-ac6e-6814c7d4dd23"))) ? (((regionManifests) != (null)) && (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("5b40bfd5-4ff1-3c54-867d-df748d09e4ec"))) ? (((regionManifests) == (null)) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("d6da7964-c51c-342f-8f02-a1057c19ab23"))) ? ((regionManifests == null) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("c6c2094a-979f-3956-9c31-f39883f1b322"))) ? ((regionManifests) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("88c29a3a-fffd-3a10-b1de-8ddf86c3fcb0"))) ? (((regionManifests) != (null)) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("3091fe3e-6f5a-3748-aa5c-c2c17debe7fb"))) ? ((regionManifests == null) && (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("cb1672e3-0dac-3e47-ade6-298bd57a2f6f"))) ? (((regionManifests) == (null)) && (regionManifests.isEmpty())) : (regionManifests == null || regionManifests.isEmpty()))))))))))))))))))))) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

if(KnobRuntime.check(java.util.UUID.fromString("71879b7f-7734-33c6-8b1c-4509e589e2d3"))) {
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
    ExecutorService exec = SnapshotManifest.createExecutor(conf, desc);

    try {
if(KnobRuntime.check(java.util.UUID.fromString("a81d898e-a61a-3516-a950-bb65832abb12"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("47235371-169b-3d2d-9af4-beb7d62837a2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0cb2e8ad-717c-30d0-87a2-5fe778983c21"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("edfdb9c8-10c0-37b7-a386-d38e90d08991"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1e86831e-4584-3245-928b-8f4708601b4d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1c2a6d54-207d-337e-ae82-eaf6edb2e92b"))) {
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
      concurrentVisitReferencedFiles(conf, fs, manifest, exec, visitor);
    } finally {
      exec.shutdown();
    }
  }

  public static void concurrentVisitReferencedFiles(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest, final ExecutorService exec, final StoreFileVisitor visitor)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b72a8b01-a776-365d-b9ed-3767152cfb2a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("76074db0-75d2-3a9a-9b24-0c6c9ce68d8f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8376b4fa-f164-3835-8312-ccc9a712f99e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ce1d27b9-810c-3fef-9573-9b82b4c96831"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("29b45315-d295-30e9-ab63-a4d86403bddc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("75a6d980-b6e8-33ae-a162-48a8186b0be7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9197252c-b427-34ff-ade9-10f4ccd0a5ae"))) {
throw new java.io.IOException("Injected exception");
}
    final SnapshotDescription snapshotDesc = manifest.getSnapshotDescription();
    final Path snapshotDir = manifest.getSnapshotDir();

    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (((KnobRuntime.check(java.util.UUID.fromString("a5debe77-f829-3cbb-9e7e-da00e98cbb7f"))) ? (((regionManifests) == (null)) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("8571f752-cb8a-319d-976a-5d95254c6214"))) ? (regionManifests.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("8a647fd2-2f6e-32f8-9003-f954c15950d5"))) ? ((regionManifests == null) && (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("9eadfa69-1e10-315c-87ca-e71bc28e1f08"))) ? ((regionManifests == null) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("76b5a30b-f280-3209-9e8f-b12c150713b4"))) ? ((regionManifests) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0f2a62a3-5526-3b41-bc0f-8ee29e0b4a4d"))) ? (((regionManifests) != (null)) || (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("a2a9d08f-f2ff-33ef-b1da-bcbdb67d8cd1"))) ? (((regionManifests) == (null)) && (regionManifests.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("69cebe2b-0c9b-35a0-b47b-7b307fc02b4c"))) ? ((regionManifests) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("03f77932-d911-3f2a-931e-ef62f2f18387"))) ? (regionManifests == null) : (((KnobRuntime.check(java.util.UUID.fromString("b48ef16e-f51b-3c1d-ab46-46302a06b102"))) ? (((regionManifests) != (null)) && (regionManifests.isEmpty())) : (regionManifests == null || regionManifests.isEmpty()))))))))))))))))))))) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(exec);

    for (final SnapshotRegionManifest regionManifest : regionManifests) {
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d67a800e-4bd9-3121-b093-ce9cfe93d86c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dd93eea1-e0a9-3fc2-920e-e8964ba3d1ba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4c03f289-4c4b-3ce9-aa75-7747395eb956"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6184c253-eeee-3091-a7a8-7abb14f9dc1e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2071c38d-dea2-3acf-a02c-ad0fc230a933"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("92ffcede-0b55-3daa-82b6-fded55bf8f42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ff11c19b-731b-3397-87f7-6b3aebd0927b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("def81c88-32d6-3c17-b5ca-a83c0bf9e49d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("eb7f8272-b5b4-3f31-abf0-ee7720c7c56c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("dcc75680-ed46-3818-92d3-bca6c5e4982a"))) {
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
          visitRegionStoreFiles(regionManifest, visitor);
          return null;
        }
      });
    }
    try {
      for (int i = 0; i < regionManifests.size(); ++i) {
if(KnobRuntime.check(java.util.UUID.fromString("a8fd215b-e9a7-3d8b-be24-7a21d20c8e28"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        completionService.take().get();
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof CorruptedSnapshotException) {
        throw new CorruptedSnapshotException(e.getCause().getMessage(),
          ProtobufUtil.createSnapshotDesc(snapshotDesc));
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  /**
   * Verify the validity of the snapshot store file
   * @param conf        The current {@link Configuration} instance.
   * @param fs          {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory of the snapshot to verify
   * @param snapshot    the {@link SnapshotDescription} of the snapshot to verify
   * @param regionInfo  {@link RegionInfo} of the region that contains the store file
   * @param family      family that contains the store file
   * @param storeFile   the store file to verify
   * @throws CorruptedSnapshotException if the snapshot is corrupted
   * @throws IOException                if an error occurred while scanning the directory
   */
  public static void verifyStoreFile(final Configuration conf, final FileSystem fs,
    final Path snapshotDir, final SnapshotDescription snapshot, final RegionInfo regionInfo,
    final String family, final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e5fd440a-0f96-3490-8675-e3f1a251508c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("22aaab5d-b4a7-3fd7-acca-6b743b2959d4"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3bdb303e-1965-3453-9998-86656d305c91"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be0216af-fdde-3c01-b16e-e9b010653ab4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("92c681e8-93bb-3b0a-99b7-79e9d8294f07"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("564743a7-0877-3082-9f7a-85181b7c6366"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b5efcc0-df91-385d-913c-0f7ca6a8dba1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db2f65ac-02d5-3206-b1f7-115a6ec293b4"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("080efc82-5e23-3cfd-9228-cc14665ce7cf"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb347444-037d-32c0-9fc4-d131f1fe5557"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("691077f6-401b-39d2-99f7-11fb23316416"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7f9b4b2-155a-309c-a75d-26829c27d437"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25a88cc8-7ff3-3b4e-a65a-cf3d6876b168"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39ec2c9a-a5b6-3ce4-9d1a-8add7d98598b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7185411-4faa-381f-b232-938e0e20788a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8cade73d-cd06-3661-b4da-23c04658b9a6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ae20ba3-6c05-3e3c-a10d-a79f7c37f883"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efe933ad-803f-3509-8c56-6cba60eb0f16"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("49da9a20-68d6-301e-9c00-831908043b1a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8aa0035a-4be8-3c0f-9528-49af2348ec1f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6cde214f-6cc7-3eda-825f-c1c3cf3d68fd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8e508108-7934-3c13-9c8d-ddc3aa0beace"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e7475cec-b2dd-3953-b020-924bf3a3c47b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("188f79f2-2717-36af-81ac-fc2ff0686d2b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d85dfdfd-ae10-3c61-8848-6b1b648f2f79"))) {
return;
}
    TableName table = TableName.valueOf(snapshot.getTable());
    String fileName = storeFile.getName();

    Path refPath = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("c85accf2-82d1-3533-9d14-1f3fb8681c24"))) ? (HFileLink.isHFileLink(fileName)) : (StoreFileInfo.isReference(fileName)))) {
      // If is a reference file check if the parent file is present in the snapshot
      refPath = new Path(new Path(regionInfo.getEncodedName(), family), fileName);
      refPath = StoreFileInfo.getReferredToFile(refPath);
      String refRegion = refPath.getParent().getParent().getName();
if(KnobRuntime.check(java.util.UUID.fromString("65cce419-c582-39dc-952f-d43817cd37fd"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7b942b9-053a-328e-9fd4-2babba7986f8"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79d45da2-7821-36db-95c4-dfadac478cb2"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de5fa7c8-c6a7-3806-adc7-2a2b634dba38"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6414a511-1622-3c3b-9231-1c5db8fa86d7"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      refPath = HFileLink.createPath(table, refRegion, family, refPath.getName());
if(KnobRuntime.check(java.util.UUID.fromString("d158b707-f1ca-31a5-8c63-0e868b39d7b1"))) {
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
      if (!HFileLink.buildFromHFileLinkPattern(conf, refPath).exists(fs)) {
        throw new CorruptedSnapshotException(
          "Missing parent hfile for: " + fileName + " path=" + refPath,
          ProtobufUtil.createSnapshotDesc(snapshot));
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("9b0d4073-6717-3152-839c-a26e248fa8e9"))) ? (storeFile.hasFileSize()) : (storeFile.hasReference()))) {
        // We don't really need to look for the file on-disk
        // we already have the Reference information embedded here.
        return;
      }
    }

    Path linkPath;
    if (((KnobRuntime.check(java.util.UUID.fromString("d5c2a670-96a2-3ba3-8b1f-6fa3ac3f509d"))) ? ((refPath.getParent()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0eb30ed5-0d9d-3615-aa1b-4fea11250ee0"))) ? (((refPath) == (null)) || (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("4dba5c12-1b92-3879-b7a2-c8d9b16caab2"))) ? (((refPath.getParent()) != (null)) || (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("69941c1a-f1b8-3b53-aebb-6d6c1fc31b7f"))) ? (((refPath.getParent().getParent()) == (null)) && (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("6eae5507-950b-32b8-a36b-c1ed75cf966e"))) ? (((refPath.getParent().getParent()) != (null)) && (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("996a35c9-2a0e-3ac4-8c97-f29685a4a5a5"))) ? (((refPath) != (null)) && (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("ea20b50c-3b24-3320-9c08-4db9ff32d9ef"))) ? (((refPath.getParent().getParent()) == (null)) && (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("37eab570-96a1-3e88-9696-f6103dd4c3f9"))) ? (((refPath.getParent().getParent()) != (null)) || (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("89039b0a-a53e-3ad3-9cf6-17c0c3e7c19a"))) ? ((refPath != null) && (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("fc258345-77b7-33a3-b9cd-61a360108d7b"))) ? ((refPath != null) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("014e9001-4c56-35c6-ae3f-64eb50d177fc"))) ? (HFileLink.isHFileLink(refPath)) : (((KnobRuntime.check(java.util.UUID.fromString("a9ae3dc9-b37b-33ff-8258-9c8d9d3114d2"))) ? (HFileLink.isHFileLink(refPath.getParent())) : (((KnobRuntime.check(java.util.UUID.fromString("a53580a1-7d4a-350d-a075-b6c9ff02555c"))) ? (((refPath.getParent()) != (null)) && (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("0b8ddea9-5bb6-30b6-8352-94db23578d64"))) ? (((refPath.getParent()) == (null)) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("121e2614-17b5-348d-a074-4842483d4105"))) ? (((refPath.getParent()) == (null)) && (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("3a98bdef-d8c1-3fd4-a9c1-3b4cefbccc5a"))) ? ((refPath.getParent().getParent()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4e0da58f-c6f0-3e48-8868-2a1ac0258493"))) ? (((refPath.getParent()) != (null)) || (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("90023885-d313-3391-b5a5-e038152e12af"))) ? (((refPath.getParent()) != (null)) && (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("5aedc152-516f-3cca-ad45-7a45fe070c67"))) ? (HFileLink.isHFileLink(refPath.getParent().getParent())) : (((KnobRuntime.check(java.util.UUID.fromString("293da41d-4a31-3b77-8b7b-6c1cb8cb0634"))) ? (((refPath.getParent().getParent()) == (null)) || (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("e28eac78-0962-32b8-b5ca-f090da7a9573"))) ? (((refPath.getParent()) != (null)) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("ebb1fb90-e0ce-3f39-b2c9-c596927ffe44"))) ? ((refPath != null) || (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("267d3636-736b-3e49-b294-c9591eae706d"))) ? (((refPath) == (null)) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("4c1e97ec-9b98-31bd-8200-bf3b0df1f41d"))) ? (((refPath.getParent().getParent()) != (null)) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("b7b489c9-0a39-3e08-8c1e-ddd1c5732062"))) ? ((refPath != null) || (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("0fa8939f-8a4c-3110-bc84-6684c8c68427"))) ? (((refPath.getParent()) != (null)) || (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("1f611a7f-8589-3570-9867-dafecad159c7"))) ? (((refPath.getParent()) == (null)) || (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("2d8afce9-9700-33f9-97b3-53c6b62a5344"))) ? (((refPath.getParent()) == (null)) || (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("91ed2a01-76d1-36a6-b6a9-055c2cc5c707"))) ? (((refPath) == (null)) && (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("37106abf-b029-3aa0-bf48-1b111719bf15"))) ? ((refPath != null) || (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("b253607f-9d18-3696-b470-1cc03aafbe19"))) ? (((refPath) != (null)) || (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("cc85de75-0c3a-3e16-a8cf-aaf0206af5e6"))) ? ((refPath.getParent()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b8f6cbcc-a744-3a4e-a205-ed6786a44ad0"))) ? (((refPath) != (null)) && (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("f3e3d791-33e0-3f6a-866b-4cf21398b564"))) ? (((refPath) == (null)) || (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("7e352be9-bc70-33db-9f76-ddf02995a1a4"))) ? (((refPath) != (null)) || (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("db846ae9-09de-3739-ad6e-ad2942079972"))) ? ((refPath) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("eb521609-b1c6-3de9-bc45-bd1d21b9e410"))) ? (((refPath.getParent().getParent()) == (null)) || (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("6f324adc-7daf-3a7d-a05a-cfce8f3028f3"))) ? (((refPath) == (null)) && (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("be50dcb8-ec77-33ce-994a-8323e540a6f9"))) ? ((refPath != null) && (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("068f1e7d-cddd-30f3-853e-dfaa1175e0c7"))) ? ((refPath.getParent().getParent()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ac8c9a84-3120-3de5-b2be-c4132e357465"))) ? ((refPath) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c94f4f8b-0ce3-3a90-8aea-e13e743db432"))) ? (((refPath.getParent().getParent()) != (null)) && (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("3749e02f-7506-3703-9769-ede22ddf6b35"))) ? (((refPath.getParent().getParent()) == (null)) || (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("83f90b72-01de-3437-95fa-fa01bcdfee30"))) ? (((refPath.getParent().getParent()) != (null)) || (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("442b1d83-ae72-3d69-ab28-75078d7a0ff8"))) ? (((refPath) != (null)) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("fd539c0d-d3ef-3d36-b3c1-85298a2110e4"))) ? (((refPath.getParent()) == (null)) || (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("8f9dd4d0-a2e9-39d7-b27f-6f99f70e4c8e"))) ? (((refPath) == (null)) || (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("25e4b020-6463-3148-bdac-96eea6e851f1"))) ? (((refPath.getParent()) == (null)) && (HFileLink.isHFileLink(refPath))) : (((KnobRuntime.check(java.util.UUID.fromString("e66c9b84-8d51-3cee-907d-bd523f2479dd"))) ? (((refPath.getParent().getParent()) == (null)) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("ea522f45-1b11-3024-8e30-c40fd4b41707"))) ? (((refPath.getParent().getParent()) != (null)) || (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("089b6513-06ba-3e1c-a291-08bd1cc20088"))) ? (((refPath) != (null)) || (HFileLink.isHFileLink(refPath.getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("0a72bf8f-b53c-305a-b748-087b2e192f49"))) ? (refPath != null) : (refPath != null && HFileLink.isHFileLink(refPath)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      linkPath = new Path(family, refPath.getName());
    } else if (((KnobRuntime.check(java.util.UUID.fromString("337ad730-8ecf-3a5e-bbb4-bd9071c39b93"))) ? (StoreFileInfo.isReference(fileName)) : (HFileLink.isHFileLink(fileName)))) {
      linkPath = new Path(family, fileName);
    } else {
      linkPath = new Path(family,
        HFileLink.createHFileLinkName(table, regionInfo.getEncodedName(), fileName));
    }

    // check if the linked file exists (in the archive, or in the table dir)
    HFileLink link = null;
    if (MobUtils.isMobRegionInfo(regionInfo)) {
      // for mob region
      link = HFileLink.buildFromHFileLinkPattern(MobUtils.getQualifiedMobRootDir(conf),
        HFileArchiveUtil.getArchivePath(conf), linkPath);
    } else {
      // not mob region
if(KnobRuntime.check(java.util.UUID.fromString("efd0a36e-5487-33c0-ae9f-bd9955b35d88"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4a72ae8b-40a6-321b-98ca-c91ed29fad69"))) {
throw new java.io.IOException("Injected exception");
}
      link = HFileLink.buildFromHFileLinkPattern(conf, linkPath);
    }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("a698ff19-9d58-3879-82a0-6bf979873c32"))) {
throw new java.io.IOException("Injected exception");
}
      FileStatus fstat = link.getFileStatus(fs);
      if (((KnobRuntime.check(java.util.UUID.fromString("1db2e0ec-aadc-37f1-99a4-84bdc7b46293"))) ? (storeFile.hasFileSize()) : (((KnobRuntime.check(java.util.UUID.fromString("f9690110-83f9-351d-a74c-d6fe0850cb6b"))) ? (storeFile.getFileSize() != fstat.getLen()) : (((KnobRuntime.check(java.util.UUID.fromString("574545f9-2906-3d8a-9701-3bec3b03479b"))) ? ((storeFile.hasFileSize()) || ((storeFile.getFileSize()) != (fstat.getLen()))) : (((KnobRuntime.check(java.util.UUID.fromString("90f9c27c-e677-3a65-ba94-e4e43d7c97c8"))) ? ((storeFile.hasFileSize()) || ((storeFile.getFileSize()) == (fstat.getLen()))) : (((KnobRuntime.check(java.util.UUID.fromString("b6284f87-b48d-3b60-98e5-222f94ae9e61"))) ? ((storeFile.hasReference()) && (storeFile.getFileSize() != fstat.getLen())) : (((KnobRuntime.check(java.util.UUID.fromString("f31bc12c-0605-3ccb-a24a-db57d28ad837"))) ? (storeFile.hasReference()) : (((KnobRuntime.check(java.util.UUID.fromString("0e16edc3-2a50-3ec6-8ea9-3b32f111ec05"))) ? ((storeFile.hasFileSize()) && ((storeFile.getFileSize()) != (fstat.getLen()))) : (((KnobRuntime.check(java.util.UUID.fromString("59b39a50-4feb-3f0c-9440-f397089b585d"))) ? ((storeFile.getFileSize()) == (fstat.getLen())) : (((KnobRuntime.check(java.util.UUID.fromString("ee483d77-7ac3-3c91-967c-20ca5804150a"))) ? ((storeFile.hasReference()) || ((storeFile.getFileSize()) != (fstat.getLen()))) : (((KnobRuntime.check(java.util.UUID.fromString("897a5f5d-110e-3af7-bd90-d4228638998a"))) ? ((storeFile.hasReference()) || (storeFile.getFileSize() != fstat.getLen())) : (((KnobRuntime.check(java.util.UUID.fromString("c8d75c3d-8c1e-3050-af97-abf25c6787a9"))) ? ((storeFile.hasFileSize()) && (storeFile.getFileSize() != fstat.getLen())) : (((KnobRuntime.check(java.util.UUID.fromString("38fcea8d-f755-3049-a15e-0c525feca910"))) ? ((storeFile.hasFileSize()) || (storeFile.getFileSize() != fstat.getLen())) : (((KnobRuntime.check(java.util.UUID.fromString("d006f939-8ac4-31eb-a3aa-65637f4b5fe9"))) ? ((storeFile.hasReference()) && ((storeFile.getFileSize()) == (fstat.getLen()))) : (((KnobRuntime.check(java.util.UUID.fromString("afe7310a-cba1-380b-947c-03b85fd9b631"))) ? ((storeFile.hasReference()) && ((storeFile.getFileSize()) != (fstat.getLen()))) : (((KnobRuntime.check(java.util.UUID.fromString("36ac4ef8-5748-3725-ab81-cf912ddf64af"))) ? ((storeFile.getFileSize()) != (fstat.getLen())) : (((KnobRuntime.check(java.util.UUID.fromString("bf0aedc5-7f1d-3e17-8e4f-cb0b366ec40b"))) ? ((storeFile.hasFileSize()) && ((storeFile.getFileSize()) == (fstat.getLen()))) : (((KnobRuntime.check(java.util.UUID.fromString("f308fefe-f23b-33a6-856d-64a092580729"))) ? ((storeFile.hasReference()) || ((storeFile.getFileSize()) == (fstat.getLen()))) : (storeFile.hasFileSize() && storeFile.getFileSize() != fstat.getLen()))))))))))))))))))))))))))))))))))) {
        String msg = "hfile: " + fileName + " size does not match with the expected one. "
          + " found=" + fstat.getLen() + " expected=" + storeFile.getFileSize();
        LOG.error(msg);
        throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
      }
    } catch (FileNotFoundException e) {
      String msg = "Can't find hfile: " + fileName + " in the real (" + link.getOriginPath()
        + ") or archive (" + link.getArchivePath() + ") directory for the primary table.";
      if (KnobRuntime.check(java.util.UUID.fromString("ba90146b-2d71-3226-bf3f-19be667d054a"))) { LOG.debug(msg); } else { LOG.error(msg); }
if(KnobRuntime.check(java.util.UUID.fromString("a0fadf96-b9f6-3131-8873-6a2d2c932bba"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82775e88-ffc7-309c-8570-3fcd29ad4f35"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4992aaf-42e0-32a5-9a00-439c510f94ba"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4a53b8e-12ef-389e-8959-8d3adfe32647"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("553ff2b0-3963-341b-828d-4b263dcae5c2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("abce504d-1973-3383-bfea-f8fe780e86e4"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e16aa312-df1e-355d-bb9e-1f9614232490"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("52038bfb-79a6-3c47-bb0d-1660cd73fe57"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7572d7f-ac82-38b7-a78e-8d2279027145"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f4b7898-8009-3c76-89b1-82514a979001"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a6d2f62-9b1c-3816-be6d-d312a751f10b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1356edb-8376-3130-af74-1c7e4d6285a9"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca688d80-f11c-35c5-9466-e07f5c925965"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ab61e39-a9f9-3a73-9da4-942f33522e41"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4cf389a6-af1e-3cf5-a574-7bff250b80e1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c44ae76-f34e-38c5-be69-113db94924d4"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Returns the store file names in the snapshot.
   * @param conf        The current {@link Configuration} instance.
   * @param fs          {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hfiles in the specified snaphot
   */
  public static Set<String> getHFileNames(final Configuration conf, final FileSystem fs,
    final Path snapshotDir) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0aaf3f36-7b83-3363-92fe-e6644b43393b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("35ffe0b5-a5d3-3597-b4e1-89e294433cb8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("08289409-355f-348d-8a17-029bd408ddd9"))) {
throw new java.io.IOException("Injected exception");
}
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
if(KnobRuntime.check(java.util.UUID.fromString("9cad418e-dcc1-3d07-b4c3-e0f3a1e965d5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("67d0271e-a274-3aee-a015-eb7712529abc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("97121d38-848f-3457-ad5a-a139672925fb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b4d2cc63-0e72-37a9-8c69-3b15b40c8007"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("dc8796d2-2b71-3639-9686-ebcc81634dec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("488b16c0-b994-3d0a-8bce-2ad17966b855"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fd37676-9f99-32da-8c52-f0cdd0a828e3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c4e743f7-789e-3108-862f-402ebbb45165"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("44c00a30-266a-3ab0-94f1-49bdc78c6111"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("53e8398b-055b-301c-8e2b-d301991a5ba1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f31d6d4-e132-36bd-88f6-ae2597aea1c4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1fd6f936-c124-3396-91fb-cbe2c288548b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d6d898ce-5c66-3453-a234-24bc313913ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("56be2939-f89d-31eb-9dce-7448123a75ef"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b75baa08-eb6b-33b0-aeb8-032f42d56246"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d7a20c94-8e9c-3e01-8a8b-e400f4cc0803"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9793230f-0faf-3cf8-853f-9eaaae0f5e64"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2dd1a94a-454a-379b-8479-c92738be52df"))) {
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
    return getHFileNames(conf, fs, snapshotDir, desc);
  }

  /**
   * Returns the store file names in the snapshot.
   * @param conf         The current {@link Configuration} instance.
   * @param fs           {@link FileSystem}
   * @param snapshotDir  {@link Path} to the Snapshot directory
   * @param snapshotDesc the {@link SnapshotDescription} of the snapshot to inspect
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hfiles in the specified snaphot
   */
  private static Set<String> getHFileNames(final Configuration conf, final FileSystem fs,
    final Path snapshotDir, final SnapshotDescription snapshotDesc) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("53760e57-91b7-34a2-9557-ea7a77463b78"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8bda428e-2101-30b9-94ab-b7f63f241b8e"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa7743ee-d184-3832-b98a-bfb5aab47fdf"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2653d574-513e-3690-a5a9-d15d99240449"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5142c242-8cbe-301e-a6a4-865f97fa3b79"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3426647-1ebc-3e8b-a45f-580980339809"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f019b7a-a971-3621-bbd3-86b011401e65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("175c39fc-3f24-3791-af51-9a155995da53"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2603d87e-0026-389d-b450-1147775c3178"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b646ea57-bac3-35e0-bf52-5617c8046774"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12bf9a8f-9642-32e6-b469-8fe3211f35fc"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("948e643b-abc6-3426-b070-e2464b81cc50"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59ed0bf8-5468-3309-8d0e-f72b580e8fe4"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e42005e-cb44-3cec-9848-7d70c65a7dc6"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3810a4f3-0836-3360-bddf-d028bdae5d48"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b8b7cce-6711-3247-bce7-d74d4574c2c7"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("690a1533-0abb-3206-a787-d6ee6e91bcef"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1d191fb5-4041-3a2f-a2e1-707a2c217241"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4dcddcf5-7ebe-3a76-8da4-4f6d0e850946"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final Set<String> names = new HashSet<>();
if(KnobRuntime.check(java.util.UUID.fromString("84648fa3-6c6a-3ecc-9210-6434a8806a4f"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b61bbc9-4898-3ca7-a497-9a0a265ce6ad"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4517ea07-6ded-3982-a361-7e74a1288107"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("417a5c10-253e-3356-bcea-86e957b55788"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("721e5b1a-cae1-38d4-a70e-ad1dd898c9a3"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56109f05-b2fd-36b0-a7fc-57c88347b552"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("59dcb027-6769-36ed-91f7-b299cdf9f549"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48cab4fb-e031-3fbe-bcd8-c1c62b6fe502"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f2eae3f9-c8fe-3b2c-af86-481b76592fbd"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec83a4c9-eceb-33de-a7e8-7f3a5f7eca26"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5130302c-39d2-3a8a-bbc3-9f1da33e9a92"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31cbc915-8586-3c55-87aa-02b678985fe8"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("abf15c13-c92a-372e-87f6-b151bc95ccf6"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3945b0a-a387-3fda-9945-02ada5a38523"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("781f1662-cc1c-3121-abe5-2ba2f2005aa8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("37e79652-83ef-393e-b6a0-83d0751ed0df"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e98f78b3-0ff0-3b7d-8701-b76c761ed5be"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c16de01-590b-3c82-a8ef-ebbd9d33f4fc"))) {
try {
    java.lang.reflect.Field field = snapshotDesc.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotDesc));
    field.set(snapshotDesc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    visitTableStoreFiles(conf, fs, snapshotDir, snapshotDesc, new StoreFileVisitor() {
      @Override
      public void storeFile(final RegionInfo regionInfo, final String family,
        final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
        String hfile = storeFile.getName();
        if (HFileLink.isHFileLink(hfile)) {
          names.add(HFileLink.getReferencedHFileName(hfile));
        } else if (StoreFileInfo.isReference(hfile)) {
          Path refPath =
            StoreFileInfo.getReferredToFile(new Path(new Path(
              new Path(new Path(regionInfo.getTable().getNamespaceAsString(),
                regionInfo.getTable().getQualifierAsString()), regionInfo.getEncodedName()),
              family), hfile));
          names.add(hfile);
          names.add(refPath.getName());
          if (HFileLink.isHFileLink(refPath.getName())) {
            names.add(HFileLink.getReferencedHFileName(refPath.getName()));
          }
        } else {
          names.add(hfile);
        }
      }
    });
    return names;
  }
}
