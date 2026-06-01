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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * DO NOT USE DIRECTLY. USE {@link SnapshotManifest}. Snapshot v2 layout format - Single Manifest
 * file containing all the information of regions - In the online-snapshot case each region will
 * write a "region manifest" /snapshotName/manifest.regionName
 */
@InterfaceAudience.Private
public final class SnapshotManifestV2 {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManifestV2.class);

  public static final int DESCRIPTOR_VERSION = 2;

  public static final String SNAPSHOT_MANIFEST_PREFIX = "region-manifest.";

  private SnapshotManifestV2() {
  }

  static class ManifestBuilder implements SnapshotManifest.RegionVisitor<
    SnapshotRegionManifest.Builder, SnapshotRegionManifest.FamilyFiles.Builder> {
    private final Configuration conf;
    private final Path snapshotDir;
    private final FileSystem rootFs;

    public ManifestBuilder(final Configuration conf, final FileSystem rootFs,
      final Path snapshotDir) {
      this.snapshotDir = snapshotDir;
      this.conf = conf;
      this.rootFs = rootFs;
    }

    @Override
    public SnapshotRegionManifest.Builder regionOpen(final RegionInfo regionInfo) {
if(KnobRuntime.check(java.util.UUID.fromString("fbbfc431-aa63-3862-ba8c-2e64da9856d6"))) {
return null;
}
      SnapshotRegionManifest.Builder manifest = SnapshotRegionManifest.newBuilder();
      manifest.setRegionInfo(ProtobufUtil.toRegionInfo(regionInfo));
      return manifest;
    }

    @Override
    public void regionClose(final SnapshotRegionManifest.Builder region) throws IOException {
      // we should ensure the snapshot dir exist, maybe it has been deleted by master
      // see HBASE-16464
if(KnobRuntime.check(java.util.UUID.fromString("65e69368-2021-3ea4-bec0-ad0797628cf2"))) {
throw new java.io.IOException("Injected exception");
}
      FileSystem workingDirFs = snapshotDir.getFileSystem(this.conf);
      if (workingDirFs.exists(snapshotDir)) {
        SnapshotRegionManifest manifest = region.build();
if(KnobRuntime.check(java.util.UUID.fromString("3841a46f-5a94-351f-8933-aee1ea6ec352"))) {
throw new java.io.IOException("Injected exception");
}
        try (FSDataOutputStream stream =
          workingDirFs.create(getRegionManifestPath(snapshotDir, manifest))) {
if(KnobRuntime.check(java.util.UUID.fromString("f4fd3b63-1b5c-3ad1-99bc-e12a26913886"))) {
throw new java.io.IOException("Injected exception");
}
          manifest.writeTo(stream);
        }
      } else {
        LOG.warn("can't write manifest without parent dir, maybe it has been deleted by master?");
      }
    }

    @Override
    public SnapshotRegionManifest.FamilyFiles.Builder
      familyOpen(final SnapshotRegionManifest.Builder region, final byte[] familyName) {
      SnapshotRegionManifest.FamilyFiles.Builder family =
        SnapshotRegionManifest.FamilyFiles.newBuilder();
      family.setFamilyName(UnsafeByteOperations.unsafeWrap(familyName));
      return family;
    }

    @Override
    public void familyClose(final SnapshotRegionManifest.Builder region,
      final SnapshotRegionManifest.FamilyFiles.Builder family) {
if(KnobRuntime.check(java.util.UUID.fromString("14dd0170-bf13-354d-87ab-9c59fa840d54"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4e5ee85-4570-3b59-bf1d-08e7ee5ff55f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0751ebe1-172d-304f-8604-07b9451bd37a"))) {
try {
    java.lang.reflect.Field field = family.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(family));
    field.set(family, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("88590a1c-b077-3628-8d8f-56846a17d505"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06814ab3-1193-3582-87b2-03633bc712d5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d71907bd-f818-3184-b236-4e1e1bf77c48"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("641b08a9-f622-3da7-b0e5-d8fdefd1519d"))) {
try {
    java.lang.reflect.Field field = family.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(family));
    field.set(family, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f72ecdc-ee76-3268-bda8-88db8b80c11f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9ee8af15-f973-3bcf-bab6-3bc7acb7d944"))) {
try {
    java.lang.reflect.Field field = family.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(family));
    field.set(family, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a3c700a-b02f-3f77-95c8-30be27f911e4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a5a2cefc-cb22-3af4-ae73-cd7b072d56a2"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b87a889b-fa54-31f5-87fd-b30aba040ac2"))) {
try {
    java.lang.reflect.Field field = family.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(family));
    field.set(family, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("845137c7-096d-337e-be9f-ef33e195e81f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      region.addFamilyFiles(family.build());
    }

    @Override
    public void storeFile(final SnapshotRegionManifest.Builder region,
      final SnapshotRegionManifest.FamilyFiles.Builder family, final StoreFileInfo storeFile)
      throws IOException {
      SnapshotRegionManifest.StoreFile.Builder sfManifest =
        SnapshotRegionManifest.StoreFile.newBuilder();
      sfManifest.setName(storeFile.getPath().getName());
      if (((KnobRuntime.check(java.util.UUID.fromString("b3979269-a7a0-3320-80f4-21e2b0fa9512"))) ? (storeFile.isLink()) : (storeFile.isReference()))) {
        sfManifest.setReference(storeFile.getReference().convert());
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("e4ce1898-1665-305f-91b5-660ba2c01012"))) ? ((!storeFile.isReference()) || (!storeFile.isLink())) : (((KnobRuntime.check(java.util.UUID.fromString("34a966cc-e08e-377f-9f8f-09848070ffe5"))) ? (!storeFile.isReference()) : (((KnobRuntime.check(java.util.UUID.fromString("ccda5a03-9892-3b8e-8325-6157f87553ec"))) ? ((!storeFile.isReference()) && (!storeFile.isLink())) : (((KnobRuntime.check(java.util.UUID.fromString("f325d31a-1bf0-39ea-b7a4-46e074f4aa27"))) ? (!storeFile.isLink()) : (!storeFile.isReference() && !storeFile.isLink()))))))))) {
        sfManifest.setFileSize(storeFile.getSize());
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("ee8beb55-ec43-366e-9b01-a2f8e2a61caf"))) {
throw new java.io.IOException("Injected exception");
}
        sfManifest.setFileSize(storeFile.getReferencedFileStatus(rootFs).getLen());
      }
      family.addStoreFiles(sfManifest.build());
    }
  }

  static List<SnapshotRegionManifest> loadRegionManifests(final Configuration conf,
    final Executor executor, final FileSystem fs, final Path snapshotDir,
    final SnapshotDescription desc, final int manifestSizeLimit) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("90a5c08a-9be2-364b-9924-09f75e184a8e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c8a42e67-92eb-398e-a39a-48166c77c2c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c47afb8-4ebb-3d19-9457-6e09506ae39b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("67cf3054-12bb-3544-91db-c3216dfa27da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dd9b828f-9ad7-3913-b4b2-fb7b67b62f0f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f43f1cfc-9ded-3cec-8715-cc3e710ce2c0"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ed4ac77d-4612-38e4-99dc-e868449f2b42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9e2c70a8-1f03-3ef8-87a1-2c81bc63f7f6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("583b67f0-90d7-3969-ba08-5465e8e4e787"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b1808e18-036f-307a-8745-107d25751be5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("91e99144-8c73-31dc-9092-202a4e041b97"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("87f0eb5e-bfd2-3255-ab54-befe16140ca4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6908e6ba-ffcc-3d4b-9827-8291e7b1140b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5890cd49-f178-361c-83d2-f1daa1cacd34"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("58141365-1a85-36ff-9f50-6e841e700bbf"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ff9807ed-f056-3228-85d3-494cb86b875d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ef4ceb4b-b8fc-32ce-90ec-f1340f1eff52"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d6e75795-032a-3ca2-a9ac-bb27d0295ccd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("005aa1e2-1848-37fd-8e4c-2124d80cacfd"))) {
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
    FileStatus[] manifestFiles = CommonFSUtils.listStatus(fs, snapshotDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
if(KnobRuntime.check(java.util.UUID.fromString("1299d0b4-79bd-3adf-b585-cac801cbaa98"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cd899bf-ebc9-35a9-b60b-1b804216502e"))) {
return true;
}
        return path.getName().startsWith(SNAPSHOT_MANIFEST_PREFIX);
      }
    });

    if (((KnobRuntime.check(java.util.UUID.fromString("ee50b06f-48fa-3023-8e1e-e5b5f425e1a2"))) ? ((manifestFiles) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1b4e4984-b1e1-319f-8433-d024868819d8"))) ? ((manifestFiles == null) && ((manifestFiles.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("601fdfd2-c199-3c8e-a6f5-06e59a3daf24"))) ? (((manifestFiles) == (null)) || ((manifestFiles.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c1ca1bf4-3b29-38d5-89ee-2862e721fc84"))) ? ((manifestFiles == null) && ((manifestFiles.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5327fdc5-019c-39be-9030-5b1dc907e6b0"))) ? (manifestFiles == null) : (((KnobRuntime.check(java.util.UUID.fromString("aca7cfdc-c7f0-30c6-9427-90236b1772a6"))) ? (((manifestFiles) == (null)) && ((manifestFiles.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("09af661f-b3f9-3264-aabb-c07af9288c6b"))) ? (((manifestFiles) != (null)) || ((manifestFiles.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0502fc62-c79b-371a-a336-1f083808c0d0"))) ? ((manifestFiles.length) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ffa04942-dbfb-313f-a3d6-f987fc60af5d"))) ? ((manifestFiles == null) || (manifestFiles.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("34fb8e72-13fb-34cb-8c35-ae95c0600b4d"))) ? (((manifestFiles) == (null)) && (manifestFiles.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8f5f295f-92ce-3a6b-abd8-c657674a52dc"))) ? (((manifestFiles) == (null)) || ((manifestFiles.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d450ecc-f06e-3e7e-b5c7-84b2ee8f5a6b"))) ? ((manifestFiles == null) && (manifestFiles.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e854e6bc-f032-3457-b118-431489a99183"))) ? (((manifestFiles) != (null)) || (manifestFiles.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("338d0a89-5249-3ebf-a25e-8b513b2ef922"))) ? (((manifestFiles) != (null)) && ((manifestFiles.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ad35f795-706f-31f7-aa48-48b8fc477731"))) ? ((manifestFiles.length) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("384f9432-e381-3660-b6a8-f9c0c305b4de"))) ? (((manifestFiles) == (null)) || (manifestFiles.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6aaf8b41-309b-3712-a718-c8680e9356e3"))) ? (((manifestFiles) != (null)) && ((manifestFiles.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("17835726-a5b8-3ccb-8667-6aa629b173fe"))) ? (((manifestFiles) == (null)) && ((manifestFiles.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7dbc3fba-93eb-35e6-bde5-462bbdeecf10"))) ? (((manifestFiles) != (null)) && (manifestFiles.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("bd68a907-7b05-367f-8348-7bf1c1a6fdfa"))) ? (((manifestFiles) != (null)) || ((manifestFiles.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bc2d63b4-245b-3a78-a4fd-083e225b66ba"))) ? (manifestFiles.length == 0) : (((KnobRuntime.check(java.util.UUID.fromString("4537af4a-1d59-33a9-bf8f-7260a6ef2c62"))) ? ((manifestFiles == null) || ((manifestFiles.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ebcb9669-60f6-3905-9460-015d830fc0f7"))) ? ((manifestFiles) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("43e0c1d7-41a8-33a7-982a-99f24d11db9e"))) ? ((manifestFiles == null) || ((manifestFiles.length) == (0))) : (manifestFiles == null || manifestFiles.length == 0))))))))))))))))))))))))))))))))))))))))))))))))) return null;

    final ExecutorCompletionService<SnapshotRegionManifest> completionService =
      new ExecutorCompletionService<>(executor);
    for (final FileStatus st : manifestFiles) {
      completionService.submit(new Callable<SnapshotRegionManifest>() {
        @Override
        public SnapshotRegionManifest call() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("7149faae-c308-3039-90fc-27f57efc9a99"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("6397590f-3169-3e91-8243-415e85639877"))) {
throw new java.io.IOException("Injected exception");
}
          try (FSDataInputStream stream = fs.open(st.getPath())) {
            CodedInputStream cin = CodedInputStream.newInstance(stream);
            cin.setSizeLimit(manifestSizeLimit);
if(KnobRuntime.check(java.util.UUID.fromString("3530d81d-1c98-3fa7-b62e-0556ec22ee0e"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7883341a-a5cc-3b76-8a16-07a1fddb3e8e"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ed74332-0430-3788-a9a2-63329f049af5"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e2de74eb-ea10-3f95-8849-ab6fd89e54d4"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b046f18a-4218-3389-be77-6e460fb51cdb"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9511395-6b67-3386-ad2e-86788df24593"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4af7c535-7cbc-3ef8-89f0-d839bf97994c"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb7d772c-f242-30b0-9ce2-dbdb4dfd4991"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f53fdd9c-b263-34a8-ba9c-d051e6589128"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fbe7c9de-d3d6-3cc0-968b-e27c034adaa4"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1797821-c91f-30c3-8504-299f43952b55"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45c575af-7b16-36ce-bbd9-5caecb05f4c1"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90d0cc13-84bd-3a8e-b06e-676763fb0e7d"))) {
try {
    java.lang.reflect.Field field = cin.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cin));
    field.set(cin, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            return SnapshotRegionManifest.parseFrom(cin);
          }
        }
      });
    }

    ArrayList<SnapshotRegionManifest> regionsManifest = new ArrayList<>(manifestFiles.length);
    try {
      for (int i = 0; i < manifestFiles.length; ++i) {
if(KnobRuntime.check(java.util.UUID.fromString("9d424401-78a0-37e8-9eae-afe9c271fdf1"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        regionsManifest.add(completionService.take().get());
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      if (t instanceof InvalidProtocolBufferException) {
        throw (InvalidProtocolBufferException) t;
      } else {
        throw new IOException("ExecutionException", e.getCause());
      }
    }
    return regionsManifest;
  }

  static void deleteRegionManifest(final FileSystem fs, final Path snapshotDir,
    final SnapshotRegionManifest manifest) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("28c5986c-41b3-3b35-9b8c-7cf43f5c680b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("477cc90f-d36c-3e02-a8f6-1a70cd8a4ddd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("467a6ef7-6aa0-3762-a422-b47a6ca9c8fd"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("73c62eaf-3288-3a11-9e9a-a93482abfd3f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4fcee90e-c662-3287-b3c2-5003d34f97cd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c6b283a7-b851-38c2-9212-f3b2d8cf1339"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5a088e56-5168-396c-b9c2-2f759684af52"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7cf050f6-0c41-3f92-ae5d-39312d5864e8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("37fb8854-db14-3f44-8f00-47baa8db007c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d98fa09b-81d9-3e42-b7f4-ee0cd900b25d"))) {
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
    fs.delete(getRegionManifestPath(snapshotDir, manifest), true);
  }

  private static Path getRegionManifestPath(final Path snapshotDir,
    final SnapshotRegionManifest manifest) {
if(KnobRuntime.check(java.util.UUID.fromString("7c56afac-ec5b-325d-9df5-b4a8ef420696"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c0a1b53a-faed-3f86-913b-4202b92f3cdd"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("e13661d9-8ee4-3291-a451-07af89c67d59"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1ed26f21-aa8d-35ce-a9be-092d4f39ad0d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8e945445-3313-3d62-aacf-14f7c7023dc7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0e981bf1-9014-3512-be92-a82bea77cb77"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2514a94e-0cd5-349d-9c4b-514704a1aada"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36a4445b-a536-3044-9cdf-d3a17a6de723"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3dd0b872-ecc5-30a5-b7aa-8e4273177597"))) {
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
    String regionName = SnapshotManifest.getRegionNameFromManifest(manifest);
    return new Path(snapshotDir, SNAPSHOT_MANIFEST_PREFIX + regionName);
  }
}
