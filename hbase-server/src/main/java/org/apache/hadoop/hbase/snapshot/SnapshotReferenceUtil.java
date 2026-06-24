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
if(KnobRuntime.check(java.util.UUID.fromString("b147f9a6-6619-32aa-b612-fbd16ffdf9a6"))) {
try {
    java.lang.reflect.Field _knob_field_ = desc.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(desc));
    _knob_field_.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef3ee41a-3ed9-34ed-86a8-4a5dc8554233"))) {
try {
    java.lang.reflect.Field _knob_field_ = desc.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(desc));
    _knob_field_.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, desc);
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (((KnobRuntime.check(java.util.UUID.fromString("cf4760f6-6c2a-396c-8ed2-253c2a919fcc"))) ? ((regionManifests == null) && (regionManifests.isEmpty())) : (regionManifests == null || regionManifests.isEmpty()))) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    for (SnapshotRegionManifest regionManifest : regionManifests) {
if(KnobRuntime.check(java.util.UUID.fromString("caa0ebc1-539f-339c-a921-1434fc26ecf5"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionManifest.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionManifest));
    _knob_field_.set(regionManifest, oldValue / 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("1c14754b-3c8b-388c-8613-697f4f8b817c"))) {
try {
    java.lang.reflect.Field _knob_field_ = manifest.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(manifest));
    _knob_field_.set(manifest, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    RegionInfo regionInfo = ProtobufUtil.toRegionInfo(manifest.getRegionInfo());
    for (SnapshotRegionManifest.FamilyFiles familyFiles : manifest.getFamilyFilesList()) {
      String familyName = familyFiles.getFamilyName().toStringUtf8();
      for (SnapshotRegionManifest.StoreFile storeFile : familyFiles.getStoreFilesList()) {
if(KnobRuntime.check(java.util.UUID.fromString("493c5d52-f8e5-365a-a261-a6b79261054e"))) {
try {
    java.lang.reflect.Field _knob_field_ = storeFile.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(storeFile));
    _knob_field_.set(storeFile, oldValue * 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("597e35d3-0fb4-3053-a5be-36eea362a912"))) {
try {
    java.lang.reflect.Field _knob_field_ = conf.getClass().getDeclaredField("loadDefaults");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(conf);
    _knob_field_.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final SnapshotDescription snapshotDesc = manifest.getSnapshotDescription();
    final Path snapshotDir = manifest.getSnapshotDir();
    concurrentVisitReferencedFiles(conf, fs, manifest, "VerifySnapshot", new StoreFileVisitor() {
      @Override
      public void storeFile(final RegionInfo regionInfo, final String family,
        final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b457d68e-56b6-38e1-b040-25949b18f238"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshotDesc.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshotDesc));
    _knob_field_.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("88f4095b-de7c-3dae-9255-cc932d0fb4cd"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshotDesc.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshotDesc));
    _knob_field_.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("add7a914-1a6c-37c8-97fa-f7e81f82c960"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshotDesc.getClass().getDeclaredField("creationTime_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshotDesc));
    _knob_field_.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b73cde1-80c5-3b09-8992-5fad3ab00723"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshotDesc.getClass().getDeclaredField("maxFileSize_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshotDesc));
    _knob_field_.set(snapshotDesc, oldValue - 1);
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
    if (regionManifests == null || regionManifests.isEmpty()) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    ExecutorService exec = SnapshotManifest.createExecutor(conf, desc);

    try {
      concurrentVisitReferencedFiles(conf, fs, manifest, exec, visitor);
    } finally {
      exec.shutdown();
    }
  }

  public static void concurrentVisitReferencedFiles(final Configuration conf, final FileSystem fs,
    final SnapshotManifest manifest, final ExecutorService exec, final StoreFileVisitor visitor)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a6d8d175-68c8-3d81-a149-3c849c901c80"))) {
try {
    java.lang.reflect.Field _knob_field_ = conf.getClass().getDeclaredField("loadDefaults");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(conf);
    _knob_field_.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final SnapshotDescription snapshotDesc = manifest.getSnapshotDescription();
    final Path snapshotDir = manifest.getSnapshotDir();

    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (((KnobRuntime.check(java.util.UUID.fromString("b48ef16e-f51b-3c1d-ab46-46302a06b102"))) ? (((regionManifests) != (null)) && (regionManifests.isEmpty())) : (regionManifests == null || regionManifests.isEmpty()))) {
      LOG.debug("No manifest files present: " + snapshotDir);
      return;
    }

    final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(exec);

    for (final SnapshotRegionManifest regionManifest : regionManifests) {
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("def81c88-32d6-3c17-b5ca-a83c0bf9e49d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b40acec9-ad33-3a65-9f66-0f255cde807d"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionManifest.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionManifest));
    _knob_field_.set(regionManifest, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("3117f59a-0ae3-3082-9ba1-e438ab8b6f2c"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aabfd7bd-30df-325e-9eac-74ebe08d6f18"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("creationTime_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    TableName table = TableName.valueOf(snapshot.getTable());
    String fileName = storeFile.getName();

    Path refPath = null;
    if (StoreFileInfo.isReference(fileName)) {
      // If is a reference file check if the parent file is present in the snapshot
      refPath = new Path(new Path(regionInfo.getEncodedName(), family), fileName);
      refPath = StoreFileInfo.getReferredToFile(refPath);
      String refRegion = refPath.getParent().getParent().getName();
      refPath = HFileLink.createPath(table, refRegion, family, refPath.getName());
      if (!HFileLink.buildFromHFileLinkPattern(conf, refPath).exists(fs)) {
        throw new CorruptedSnapshotException(
          "Missing parent hfile for: " + fileName + " path=" + refPath,
          ProtobufUtil.createSnapshotDesc(snapshot));
      }

      if (storeFile.hasReference()) {
        // We don't really need to look for the file on-disk
        // we already have the Reference information embedded here.
        return;
      }
    }

    Path linkPath;
    if (((KnobRuntime.check(java.util.UUID.fromString("014e9001-4c56-35c6-ae3f-64eb50d177fc"))) ? (HFileLink.isHFileLink(refPath)) : (((KnobRuntime.check(java.util.UUID.fromString("3a98bdef-d8c1-3fd4-a9c1-3b4cefbccc5a"))) ? ((refPath.getParent().getParent()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0b8ddea9-5bb6-30b6-8352-94db23578d64"))) ? (((refPath.getParent()) == (null)) && (HFileLink.isHFileLink(refPath.getParent().getParent()))) : (((KnobRuntime.check(java.util.UUID.fromString("a9ae3dc9-b37b-33ff-8258-9c8d9d3114d2"))) ? (HFileLink.isHFileLink(refPath.getParent())) : (((KnobRuntime.check(java.util.UUID.fromString("a53580a1-7d4a-350d-a075-b6c9ff02555c"))) ? (((refPath.getParent()) != (null)) && (HFileLink.isHFileLink(refPath.getParent()))) : (refPath != null && HFileLink.isHFileLink(refPath)))))))))))) {
      linkPath = new Path(family, refPath.getName());
    } else if (HFileLink.isHFileLink(fileName)) {
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
      link = HFileLink.buildFromHFileLinkPattern(conf, linkPath);
    }
    try {
      FileStatus fstat = link.getFileStatus(fs);
      if (storeFile.hasFileSize() && storeFile.getFileSize() != fstat.getLen()) {
        String msg = "hfile: " + fileName + " size does not match with the expected one. "
          + " found=" + fstat.getLen() + " expected=" + storeFile.getFileSize();
        LOG.error(msg);
        throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
      }
    } catch (FileNotFoundException e) {
      String msg = "Can't find hfile: " + fileName + " in the real (" + link.getOriginPath()
        + ") or archive (" + link.getArchivePath() + ") directory for the primary table.";
      LOG.error(msg);
if(KnobRuntime.check(java.util.UUID.fromString("40482c4a-1e45-3b5a-828f-fdff83daa61f"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b460f1f4-25ae-31a7-b91a-77674f2e029a"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1b1ed7b-1a61-307d-8b9d-a0c4c50929a5"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue / 2);
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
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
if(KnobRuntime.check(java.util.UUID.fromString("e034b79c-1e62-3351-94cb-1926999924db"))) {
try {
    java.lang.reflect.Field _knob_field_ = desc.getClass().getDeclaredField("version_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(desc));
    _knob_field_.set(desc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b4d2cc63-0e72-37a9-8c69-3b15b40c8007"))) {
throw new java.io.IOException("Injected exception");
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
if(KnobRuntime.check(java.util.UUID.fromString("edbdf58b-6658-3dc0-b3fc-94511a91e23c"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshotDesc.getClass().getDeclaredField("creationTime_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(snapshotDesc));
    _knob_field_.set(snapshotDesc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68359d3c-0bb4-356c-9d57-8770d552c7cb"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshotDesc.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshotDesc));
    _knob_field_.set(snapshotDesc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final Set<String> names = new HashSet<>();
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
