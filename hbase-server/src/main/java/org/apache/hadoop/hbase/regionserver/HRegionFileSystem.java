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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.io.HFileLink.LINK_NAME_PATTERN;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * View to an on-disk Region. Provides the set of methods necessary to interact with the on-disk
 * region data.
 */
@InterfaceAudience.Private
public class HRegionFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionFileSystem.class);

  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

  /** Temporary subdirectory of the region directory used for merges. */
  public static final String REGION_MERGES_DIR = ".merges";

  /** Temporary subdirectory of the region directory used for splits. */
  public static final String REGION_SPLITS_DIR = ".splits";

  /** Temporary subdirectory of the region directory used for compaction output. */
  static final String REGION_TEMP_DIR = ".tmp";

  private final RegionInfo regionInfo;
  // regionInfo for interacting with FS (getting encodedName, etc)
  final RegionInfo regionInfoForFs;
  final Configuration conf;
  private final Path tableDir;
  final FileSystem fs;
  private final Path regionDir;

  /**
   * In order to handle NN connectivity hiccups, one need to retry non-idempotent operation at the
   * client level.
   */
  private final int hdfsClientRetriesNumber;
  private final int baseSleepBeforeRetries;
  private static final int DEFAULT_HDFS_CLIENT_RETRIES_NUMBER = 10;
  private static final int DEFAULT_BASE_SLEEP_BEFORE_RETRIES = 1000;

  /**
   * Create a view to the on-disk region
   * @param conf       the {@link Configuration} to use
   * @param fs         {@link FileSystem} that contains the region
   * @param tableDir   {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region
   */
  HRegionFileSystem(final Configuration conf, final FileSystem fs, final Path tableDir,
    final RegionInfo regionInfo) {
    this.fs = fs;
    this.conf = conf;
    this.tableDir = Objects.requireNonNull(tableDir, "tableDir is null");
    this.regionInfo = Objects.requireNonNull(regionInfo, "regionInfo is null");
    this.regionInfoForFs = ServerRegionReplicaUtil.getRegionInfoForFs(regionInfo);
    this.regionDir = FSUtils.getRegionDirFromTableDir(tableDir, regionInfo);
    this.hdfsClientRetriesNumber =
      conf.getInt("hdfs.client.retries.number", DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
    this.baseSleepBeforeRetries =
      conf.getInt("hdfs.client.sleep.before.retries", DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
  }

  /** Returns the underlying {@link FileSystem} */
  public FileSystem getFileSystem() {
    return this.fs;
  }

  /** Returns the {@link RegionInfo} that describe this on-disk region view */
  public RegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  public RegionInfo getRegionInfoForFS() {
    return this.regionInfoForFs;
  }

  /** Returns {@link Path} to the region's root directory. */
  public Path getTableDir() {
    return this.tableDir;
  }

  /** Returns {@link Path} to the region directory. */
  public Path getRegionDir() {
    return regionDir;
  }

  // ===========================================================================
  // Temp Helpers
  // ===========================================================================
  /** Returns {@link Path} to the region's temp directory, used for file creations */
  public Path getTempDir() {
    return new Path(getRegionDir(), REGION_TEMP_DIR);
  }

  /**
   * Clean up any temp detritus that may have been left around from previous operation attempts.
   */
  void cleanupTempDir() throws IOException {
    deleteDir(getTempDir());
  }

  // ===========================================================================
  // Store/StoreFile Helpers
  // ===========================================================================
  /**
   * Returns the directory path of the specified family
   * @param familyName Column Family Name
   * @return {@link Path} to the directory of the specified family
   */
  public Path getStoreDir(final String familyName) {
    return new Path(this.getRegionDir(), familyName);
  }

  /**
   * @param tabledir {@link Path} to where the table is being stored
   * @param hri      {@link RegionInfo} for the region.
   * @param family   {@link ColumnFamilyDescriptor} describing the column family
   * @return Path to family/Store home directory.
   */
  public static Path getStoreHomedir(final Path tabledir, final RegionInfo hri,
    final byte[] family) {
    return getStoreHomedir(tabledir, hri.getEncodedName(), family);
  }

  /**
   * @param tabledir    {@link Path} to where the table is being stored
   * @param encodedName Encoded region name.
   * @param family      {@link ColumnFamilyDescriptor} describing the column family
   * @return Path to family/Store home directory.
   */
  public static Path getStoreHomedir(final Path tabledir, final String encodedName,
    final byte[] family) {
    return new Path(tabledir, new Path(encodedName, Bytes.toString(family)));
  }

  /**
   * Create the store directory for the specified family name
   * @param familyName Column Family Name
   * @return {@link Path} to the directory of the specified family
   * @throws IOException if the directory creation fails.
   */
  Path createStoreDir(final String familyName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    if (((KnobRuntime.check(java.util.UUID.fromString("0da26a86-ce3e-31c3-a012-5d1afa56a749"))) ? (!fs.exists(storeDir)) : (((KnobRuntime.check(java.util.UUID.fromString("07311ed3-d4ef-37c2-8956-153afe67a68f"))) ? (!createDir(storeDir)) : (((KnobRuntime.check(java.util.UUID.fromString("75c9425c-b45a-3319-9b92-7c8a54c9e06c"))) ? ((!fs.exists(storeDir)) && (!createDir(storeDir))) : (((KnobRuntime.check(java.util.UUID.fromString("937a5d0d-6f48-35a6-a0e6-9ed5c09bdb27"))) ? ((!fs.exists(storeDir)) || (!createDir(storeDir))) : (!fs.exists(storeDir) && !createDir(storeDir))))))))))
      throw new IOException("Failed creating " + storeDir);
    return storeDir;
  }

  /**
   * Set the directory of CF to the specified storage policy. <br>
   * <i>"LAZY_PERSIST"</i>, <i>"ALL_SSD"</i>, <i>"ONE_SSD"</i>, <i>"HOT"</i>, <i>"WARM"</i>,
   * <i>"COLD"</i> <br>
   * <br>
   * See {@link org.apache.hadoop.hdfs.protocol.HdfsConstants} for more details.
   * @param familyName The name of column family.
   * @param policyName The name of the storage policy: 'HOT', 'COLD', etc. See hadoop 2.6+
   *                   org.apache.hadoop.hdfs.protocol.HdfsConstants for possible list e.g 'COLD',
   *                   'WARM', 'HOT', 'ONE_SSD', 'ALL_SSD', 'LAZY_PERSIST'.
   */
  public void setStoragePolicy(String familyName, String policyName) {
    CommonFSUtils.setStoragePolicy(this.fs, getStoreDir(familyName), policyName);
  }

  /**
   * Set storage policy for a whole region. <br>
   * <i>"LAZY_PERSIST"</i>, <i>"ALL_SSD"</i>, <i>"ONE_SSD"</i>, <i>"HOT"</i>, <i>"WARM"</i>,
   * <i>"COLD"</i> <br>
   * <br>
   * See {@link org.apache.hadoop.hdfs.protocol.HdfsConstants} for more details.
   * @param policyName The name of the storage policy: 'HOT', 'COLD', etc. See hadoop 2.6+
   *                   org.apache.hadoop.hdfs.protocol.HdfsConstants for possible list e.g 'COLD',
   *                   'WARM', 'HOT', 'ONE_SSD', 'ALL_SSD', 'LAZY_PERSIST'.
   */
  public void setStoragePolicy(String policyName) {
    CommonFSUtils.setStoragePolicy(this.fs, getRegionDir(), policyName);
  }

  /**
   * Get the storage policy of the directory of CF.
   * @param familyName The name of column family.
   * @return Storage policy name, or {@code null} if not using {@link HFileSystem} or exception
   *         thrown when trying to get policy
   */
  @Nullable
  public String getStoragePolicyName(String familyName) {
    if (this.fs instanceof HFileSystem) {
      Path storeDir = getStoreDir(familyName);
      return ((HFileSystem) this.fs).getStoragePolicyName(storeDir);
    }

    return null;
  }

  /**
   * Returns the store files available for the family. This methods performs the filtering based on
   * the valid store files.
   * @param familyName Column Family Name
   * @return a set of {@link StoreFileInfo} for the specified family.
   */
  public List<StoreFileInfo> getStoreFiles(final String familyName) throws IOException {
    return getStoreFiles(familyName, true);
  }

  /**
   * Returns the store files available for the family. This methods performs the filtering based on
   * the valid store files.
   * @param familyName Column Family Name
   * @return a set of {@link StoreFileInfo} for the specified family.
   */
  public List<StoreFileInfo> getStoreFiles(final String familyName, final boolean validate)
    throws IOException {
    Path familyDir = getStoreDir(familyName);
if(KnobRuntime.check(java.util.UUID.fromString("babd979b-044b-3ad6-a414-b711126278c2"))) {
throw new java.io.IOException("Injected exception");
}
    FileStatus[] files = CommonFSUtils.listStatus(this.fs, familyDir);
    if (((KnobRuntime.check(java.util.UUID.fromString("b349ee4f-bc6f-3987-8a37-5528946ff97d"))) ? ((files) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("781fa117-8412-3671-9727-e36edbb7f288"))) ? ((files) == (null)) : (files == null))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("bdb0764f-c2fd-398a-a39d-254ec6572ad7"))) ? (LOG.isDebugEnabled()) : (LOG.isTraceEnabled()))) {
        LOG.trace("No StoreFiles for: " + familyDir);
      }
      return null;
    }

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<>(files.length);
    for (FileStatus status : files) {
      if (validate && !StoreFileInfo.isValid(status)) {
        // recovered.hfiles directory is expected inside CF path when hbase.wal.split.to.hfile to
if(KnobRuntime.check(java.util.UUID.fromString("cbb1a635-f32f-33fb-9817-6ee3d4c351d5"))) {
throw new java.io.IOException("Injected exception");
}
        // true, refer HBASE-23740
        if (!HConstants.RECOVERED_HFILES_DIR.equals(status.getPath().getName())) {
          LOG.warn("Invalid StoreFile: {}", status.getPath());
        }
        continue;
      }
      StoreFileInfo info = ServerRegionReplicaUtil.getStoreFileInfo(conf, fs, regionInfo,
        regionInfoForFs, familyName, status.getPath());
      storeFiles.add(info);

    }
    return storeFiles;
  }

  /**
   * Returns the store files' LocatedFileStatus which available for the family. This methods
   * performs the filtering based on the valid store files.
   * @param familyName Column Family Name
   * @return a list of store files' LocatedFileStatus for the specified family.
   */
  public static List<LocatedFileStatus> getStoreFilesLocatedStatus(final HRegionFileSystem regionfs,
    final String familyName, final boolean validate) throws IOException {
    Path familyDir = regionfs.getStoreDir(familyName);
    List<LocatedFileStatus> locatedFileStatuses =
      CommonFSUtils.listLocatedStatus(regionfs.getFileSystem(), familyDir);
    if (locatedFileStatuses == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No StoreFiles for: " + familyDir);
      }
      return null;
    }

    List<LocatedFileStatus> validStoreFiles = Lists.newArrayList();
    for (LocatedFileStatus status : locatedFileStatuses) {
      if (validate && !StoreFileInfo.isValid(status)) {
        // recovered.hfiles directory is expected inside CF path when hbase.wal.split.to.hfile to
        // true, refer HBASE-23740
        if (!HConstants.RECOVERED_HFILES_DIR.equals(status.getPath().getName())) {
          LOG.warn("Invalid StoreFile: {}", status.getPath());
        }
      } else {
        validStoreFiles.add(status);
      }
    }
    return validStoreFiles;
  }

  /**
   * Return Qualified Path of the specified family/file
   * @param familyName Column Family Name
   * @param fileName   File Name
   * @return The qualified Path for the specified family/file
   */
  Path getStoreFilePath(final String familyName, final String fileName) {
    Path familyDir = getStoreDir(familyName);
    return new Path(familyDir, fileName).makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  /**
   * Return the store file information of the specified family/file.
   * @param familyName Column Family Name
   * @param fileName   File Name
   * @return The {@link StoreFileInfo} for the specified family/file
   */
  StoreFileInfo getStoreFileInfo(final String familyName, final String fileName)
    throws IOException {
    Path familyDir = getStoreDir(familyName);
    return ServerRegionReplicaUtil.getStoreFileInfo(conf, fs, regionInfo, regionInfoForFs,
      familyName, new Path(familyDir, fileName));
  }

  /**
   * Returns true if the specified family has reference files
   * @param familyName Column Family Name
   * @return true if family contains reference files
   */
  public boolean hasReferences(final String familyName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    FileStatus[] files = CommonFSUtils.listStatus(fs, storeDir);
    if (files != null) {
      for (FileStatus stat : files) {
        if (stat.isDirectory()) {
          continue;
        }
        if (StoreFileInfo.isReference(stat.getPath())) {
          LOG.trace("Reference {}", stat.getPath());
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check whether region has Reference file
   * @param htd table desciptor of the region
   * @return true if region has reference file
   */
  public boolean hasReferences(final TableDescriptor htd) throws IOException {
    for (ColumnFamilyDescriptor family : htd.getColumnFamilies()) {
      if (hasReferences(family.getNameAsString())) {
        return true;
      }
    }
    return false;
  }

  /** Returns the set of families present on disk n */
  public Collection<String> getFamilies() throws IOException {
    FileStatus[] fds =
      CommonFSUtils.listStatus(fs, getRegionDir(), new FSUtils.FamilyDirFilter(fs));
    if (fds == null) return null;

    ArrayList<String> families = new ArrayList<>(fds.length);
    for (FileStatus status : fds) {
      families.add(status.getPath().getName());
    }

    return families;
  }

  /**
   * Remove the region family from disk, archiving the store files.
   * @param familyName Column Family Name
   * @throws IOException if an error occours during the archiving
   */
  public void deleteFamily(final String familyName) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("34027808-4db1-3480-8685-fc16c8580ddb"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("55f0fddd-0375-3540-917f-91017ac9a20e"))) {
throw new java.io.IOException("Injected exception");
}
    // archive family store files
    HFileArchiver.archiveFamily(fs, conf, regionInfoForFs, tableDir, Bytes.toBytes(familyName));

    // delete the family folder
    Path familyDir = getStoreDir(familyName);
    if (fs.exists(familyDir) && !deleteDir(familyDir))
      throw new IOException("Could not delete family " + familyName + " from FileSystem for region "
        + regionInfoForFs.getRegionNameAsString() + "(" + regionInfoForFs.getEncodedName() + ")");
  }

  /**
   * Generate a unique file name, used by createTempName() and commitStoreFile()
   * @param suffix extra information to append to the generated name
   * @return Unique file name
   */
  private static String generateUniqueName(final String suffix) {
    String name = UUID.randomUUID().toString().replaceAll("-", "");
    if (((KnobRuntime.check(java.util.UUID.fromString("8261a580-ea68-38a5-b0f8-a4c42562caad"))) ? ((suffix) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c94ac5cb-1ed7-3b45-bbce-785a208367b3"))) ? ((suffix) == (null)) : (suffix != null))))) name += suffix;
    return name;
  }

  /**
   * Generate a unique temporary Path. Used in conjuction with commitStoreFile() to get a safer file
   * creation. <code>
   * Path file = fs.createTempName();
   * ...StoreFile.Writer(file)...
   * fs.commitStoreFile("family", file);
   * </code>
   * @return Unique {@link Path} of the temporary file
   */
  public Path createTempName() {
    return createTempName(null);
  }

  /**
   * Generate a unique temporary Path. Used in conjuction with commitStoreFile() to get a safer file
   * creation. <code>
   * Path file = fs.createTempName();
   * ...StoreFile.Writer(file)...
   * fs.commitStoreFile("family", file);
   * </code>
   * @param suffix extra information to append to the generated name
   * @return Unique {@link Path} of the temporary file
   */
  public Path createTempName(final String suffix) {
    return new Path(getTempDir(), generateUniqueName(suffix));
  }

  /**
   * Move the file from a build/temp location to the main family store directory.
   * @param familyName Family that will gain the file
   * @param buildPath  {@link Path} to the file to commit.
   * @return The new {@link Path} of the committed file
   */
  public Path commitStoreFile(final String familyName, final Path buildPath) throws IOException {
    Path dstPath = preCommitStoreFile(familyName, buildPath, -1, false);
    return commitStoreFile(buildPath, dstPath);
  }

  /**
   * Generate the filename in the main family store directory for moving the file from a build/temp
   * location.
   * @param familyName      Family that will gain the file
   * @param buildPath       {@link Path} to the file to commit.
   * @param seqNum          Sequence Number to append to the file name (less then 0 if no sequence
   *                        number)
   * @param generateNewName False if you want to keep the buildPath name
   * @return The new {@link Path} of the to be committed file
   */
  private Path preCommitStoreFile(final String familyName, final Path buildPath, final long seqNum,
    final boolean generateNewName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    if (!fs.exists(storeDir) && !createDir(storeDir))
      throw new IOException("Failed creating " + storeDir);

    String name = buildPath.getName();
    if (generateNewName) {
      name = generateUniqueName((seqNum < 0) ? null : StoreFileInfo.formatBulkloadSeqId(seqNum));
    }
    Path dstPath = new Path(storeDir, name);
    if (!fs.exists(buildPath)) {
      throw new FileNotFoundException(buildPath.toString());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Committing " + buildPath + " as " + dstPath);
    }
    return dstPath;
  }

  /*
   * Moves file from staging dir to region dir
   * @param buildPath {@link Path} to the file to commit.
   * @param dstPath {@link Path} to the file under region dir
   * @return The {@link Path} of the committed file
   */
  Path commitStoreFile(final Path buildPath, Path dstPath) throws IOException {
    // rename is not necessary in case of direct-insert stores
    if (buildPath.equals(dstPath)) {
      return dstPath;
    }
    // buildPath exists, therefore not doing an exists() check.
    if (!rename(buildPath, dstPath)) {
      throw new IOException("Failed rename of " + buildPath + " to " + dstPath);
    }
    return dstPath;
  }

  /**
   * Archives the specified store file from the specified family.
   * @param familyName Family that contains the store files
   * @param filePath   {@link Path} to the store file to remove
   * @throws IOException if the archiving fails
   */
  public void removeStoreFile(final String familyName, final Path filePath) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5777a734-39d1-3e58-9d57-848426dcef95"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c1237c94-a87c-3e2e-81c4-05e8e80a4852"))) {
return;
}
    HFileArchiver.archiveStoreFile(this.conf, this.fs, this.regionInfoForFs, this.tableDir,
      Bytes.toBytes(familyName), filePath);
  }

  /**
   * Closes and archives the specified store files from the specified family.
   * @param familyName Family that contains the store files
   * @param storeFiles set of store files to remove
   * @throws IOException if the archiving fails
   */
  public void removeStoreFiles(String familyName, Collection<HStoreFile> storeFiles)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("73a2500d-141b-3890-8550-bd07b033c3bc"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c2c01443-ba11-3670-bb92-ed41e8cbd80a"))) {
return;
}
    HFileArchiver.archiveStoreFiles(this.conf, this.fs, this.regionInfoForFs, this.tableDir,
      Bytes.toBytes(familyName), storeFiles);
  }

  /**
   * Bulk load: Add a specified store file to the specified family. If the source file is on the
   * same different file-system is moved from the source location to the destination location,
   * otherwise is copied over.
   * @param familyName Family that will gain the file
   * @param srcPath    {@link Path} to the file to import
   * @param seqNum     Bulk Load sequence number
   * @return The destination {@link Path} of the bulk loaded file
   */
  Pair<Path, Path> bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5843645f-2f36-3312-9031-361dafa91c6c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3ea3487c-f823-350d-8fd9-5ce48c0bddad"))) {
return null;
}
    // Copy the file if it's on another filesystem
    FileSystem srcFs = srcPath.getFileSystem(conf);
    srcPath = srcFs.resolvePath(srcPath);
    FileSystem realSrcFs = srcPath.getFileSystem(conf);
    FileSystem desFs = fs instanceof HFileSystem ? ((HFileSystem) fs).getBackingFs() : fs;

    // We can't compare FileSystem instances as equals() includes UGI instance
    // as part of the comparison and won't work when doing SecureBulkLoad
    // TODO deal with viewFS
    if (!FSUtils.isSameHdfs(conf, realSrcFs, desFs)) {
      LOG.info("Bulk-load file " + srcPath + " is on different filesystem than "
        + "the destination store. Copying file over to destination filesystem.");
      Path tmpPath = createTempName();
      FileUtil.copy(realSrcFs, srcPath, fs, tmpPath, false, conf);
      LOG.info("Copied " + srcPath + " to temporary path on destination filesystem: " + tmpPath);
      srcPath = tmpPath;
    }

    return new Pair<>(srcPath, preCommitStoreFile(familyName, srcPath, seqNum, true));
  }

  // ===========================================================================
  // Splits Helpers
  // ===========================================================================

  public Path getSplitsDir(final RegionInfo hri) {
    return new Path(getTableDir(), hri.getEncodedName());
  }

  /**
   * Remove daughter region
   * @param regionInfo daughter {@link RegionInfo}
   */
  void cleanupDaughterRegion(final RegionInfo regionInfo) throws IOException {
    Path regionDir = new Path(this.tableDir, regionInfo.getEncodedName());
    if (((KnobRuntime.check(java.util.UUID.fromString("19127c35-bcf6-344a-a354-7b21f1e398b4"))) ? ((this.fs.exists(regionDir)) && (!deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("12c3bdf6-68f4-3c7e-85ae-e32003381fe7"))) ? ((createDir(regionDir)) && (!deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("0e345594-02e3-35f3-a82f-f4759c9dbf64"))) ? (StoreFileInfo.isReference(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("ca5600fd-6106-3593-a329-f6b1e44372c5"))) ? ((StoreFileInfo.isReference(regionDir)) || (!deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("ae1f7b19-219c-3209-8828-3240a5353d1c"))) ? (!deleteDir(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("fbcee021-4eb4-329c-926f-89045fd425c0"))) ? ((deleteDir(regionDir)) || (!deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("473fe985-8f89-3193-8aeb-94dccba1da74"))) ? (this.fs.exists(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("7bd1f27f-93ce-31f2-9a6a-79a88da1fbfd"))) ? (deleteDir(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("251168e3-f3be-347e-840d-dc8c9f9d600d"))) ? ((createDir(regionDir)) || (!deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("e9245f30-ff38-3617-b24b-6434c2b432d9"))) ? (createDir(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("b9b4ce87-f87b-3ba1-a0eb-e4ee31e966a2"))) ? ((deleteDir(regionDir)) && (!deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("505617fb-cdfa-3917-af59-0539bf330ae0"))) ? ((StoreFileInfo.isReference(regionDir)) && (!deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("c268490d-aaf7-388b-98af-6427d2c96da4"))) ? ((this.fs.exists(regionDir)) || (!deleteDir(regionDir))) : (this.fs.exists(regionDir) && !deleteDir(regionDir)))))))))))))))))))))))))))) {
      throw new IOException("Failed delete of " + regionDir);
    }
  }

  /**
   * Commit a daughter region, moving it from the split temporary directory to the proper location
   * in the filesystem.
   * @param regionInfo daughter {@link org.apache.hadoop.hbase.client.RegionInfo}
   */
  public Path commitDaughterRegion(final RegionInfo regionInfo, List<Path> allRegionFiles,
    MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ab3673b0-b43c-3715-b7b8-96773119f197"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("54f0fb18-d293-39d0-8fa7-0634424e7d12"))) {
throw new java.io.IOException("Injected exception");
}
    Path regionDir = this.getSplitsDir(regionInfo);
    if (((KnobRuntime.check(java.util.UUID.fromString("f04472e5-aba4-3142-b491-07ac2a8270c3"))) ? (deleteDir(getTableDir())) : (((KnobRuntime.check(java.util.UUID.fromString("2ba31824-818f-3c08-b2ee-53c6c09a98df"))) ? (createDir(getTableDir())) : (((KnobRuntime.check(java.util.UUID.fromString("f0c4bae3-71fe-3d10-adb2-c5ef553a3c8c"))) ? (fs.exists(getTableDir())) : (fs.exists(regionDir)))))))) {
      // Write HRI to a file in case we need to recover hbase:meta
      Path regionInfoFile = new Path(regionDir, REGION_INFO_FILE);
      byte[] regionInfoContent = getRegionInfoFileContent(regionInfo);
      writeRegionInfoFileContent(conf, fs, regionInfoFile, regionInfoContent);
      HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
        env.getMasterConfiguration(), fs, getTableDir(), regionInfo, false);
      insertRegionFilesIntoStoreTracker(allRegionFiles, env, regionFs);
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("ea41724d-2435-3104-90f0-9d14a2294396"))) ? (getTableDir()) : (regionDir));
  }

  private void insertRegionFilesIntoStoreTracker(List<Path> allFiles, MasterProcedureEnv env,
    HRegionFileSystem regionFs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("14332bd6-5cd7-307b-9c45-9cf60a37fe1e"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a783187-07f4-33ab-8d5d-4f579500378e"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0616a96c-977d-3894-98d5-1ef70b033cc8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("23b8e4d7-6def-369e-90d2-2b01fa677ec6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("874c7b98-9332-3adf-a491-dd8f1ed04714"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("85f9cb1e-4b58-36de-90e4-e13727205ca1"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e706d639-2624-3e0b-b5d3-b261a6ef55ed"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("394f294e-5569-399a-8a2a-bc1d16a89d8f"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5cb1b5e-d9ae-3ca7-8da4-0579cd0ac9aa"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("713bb369-5212-3562-b540-cc488d250f10"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    TableDescriptor tblDesc =
      env.getMasterServices().getTableDescriptors().get(regionInfo.getTable());
    // we need to map trackers per store
    Map<String, StoreFileTracker> trackerMap = new HashMap<>();
    // we need to map store files per store
    Map<String, List<StoreFileInfo>> fileInfoMap = new HashMap<>();
    for (Path file : allFiles) {
      String familyName = file.getParent().getName();
      trackerMap.computeIfAbsent(familyName, t -> StoreFileTrackerFactory.create(conf, tblDesc,
        tblDesc.getColumnFamily(Bytes.toBytes(familyName)), regionFs));
      fileInfoMap.computeIfAbsent(familyName, l -> new ArrayList<>());
      List<StoreFileInfo> infos = fileInfoMap.get(familyName);
      infos.add(new StoreFileInfo(conf, fs, file, true));
    }
    for (Map.Entry<String, StoreFileTracker> entry : trackerMap.entrySet()) {
      entry.getValue().add(fileInfoMap.get(entry.getKey()));
    }
  }

  /**
   * Creates region split daughter directories under the table dir. If the daughter regions already
   * exist, for example, in the case of a recovery from a previous failed split procedure, this
   * method deletes the given region dir recursively, then recreates it again.
   */
  public void createSplitsDir(RegionInfo daughterA, RegionInfo daughterB) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5080a92b-2664-329e-a4e0-bb37ac362ef9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("2389f185-dd0e-365b-8f1d-047d03737048"))) {
throw new java.io.IOException("Injected exception");
}
    Path daughterADir = getSplitsDir(daughterA);
    if (((KnobRuntime.check(java.util.UUID.fromString("1425fe2b-6451-3d1b-bb7a-754e4bab4839"))) ? ((deleteDir(daughterADir)) || (!deleteDir(daughterADir))) : (((KnobRuntime.check(java.util.UUID.fromString("c00dd36e-59c6-3514-b147-1b2444dc4f5b"))) ? ((createDir(daughterADir)) || (!deleteDir(daughterADir))) : (((KnobRuntime.check(java.util.UUID.fromString("005ebb78-317d-342b-a4ee-e65c6dfa0c96"))) ? (createDir(daughterADir)) : (((KnobRuntime.check(java.util.UUID.fromString("b0277969-dd2a-3143-9433-84b434be6757"))) ? ((fs.exists(daughterADir)) && (!deleteDir(daughterADir))) : (((KnobRuntime.check(java.util.UUID.fromString("fdff1dbd-5c25-349b-87c6-ef30c03db499"))) ? ((fs.exists(daughterADir)) || (!deleteDir(daughterADir))) : (((KnobRuntime.check(java.util.UUID.fromString("79a45e7b-0b18-3833-898f-93c869801d0d"))) ? ((createDir(daughterADir)) && (!deleteDir(daughterADir))) : (((KnobRuntime.check(java.util.UUID.fromString("89f4e614-a6ed-33c0-879c-749cdbd6fd82"))) ? (fs.exists(daughterADir)) : (((KnobRuntime.check(java.util.UUID.fromString("e8b2326b-70c0-3061-b93d-0f59b1fdda03"))) ? (!deleteDir(daughterADir)) : (((KnobRuntime.check(java.util.UUID.fromString("8f73626c-2d9f-3edf-a06c-e80d6ba9eb86"))) ? (deleteDir(daughterADir)) : (((KnobRuntime.check(java.util.UUID.fromString("09423594-b158-3e8e-98b5-7b56df95a2e7"))) ? ((deleteDir(daughterADir)) && (!deleteDir(daughterADir))) : (fs.exists(daughterADir) && !deleteDir(daughterADir)))))))))))))))))))))) {
      throw new IOException("Failed deletion of " + daughterADir + " before creating them again.");

    }
    if (!createDir(daughterADir)) {
      throw new IOException("Failed create of " + daughterADir);
    }
    Path daughterBDir = getSplitsDir(daughterB);
    if (fs.exists(daughterBDir) && !deleteDir(daughterBDir)) {
      throw new IOException("Failed deletion of " + daughterBDir + " before creating them again.");

    }
    if (!createDir(daughterBDir)) {
      throw new IOException("Failed create of " + daughterBDir);
    }
  }

  /**
   * Write out a split reference. Package local so it doesnt leak out of regionserver.
   * @param hri         {@link RegionInfo} of the destination
   * @param familyName  Column Family Name
   * @param f           File to split.
   * @param splitRow    Split Row
   * @param top         True if we are referring to the top half of the hfile.
   * @param splitPolicy A split policy instance; be careful! May not be full populated; e.g. if this
   *                    method is invoked on the Master side, then the RegionSplitPolicy will NOT
   *                    have a reference to a Region.
   * @return Path to created reference.
   */
  public Path splitStoreFile(RegionInfo hri, String familyName, HStoreFile f, byte[] splitRow,
    boolean top, RegionSplitPolicy splitPolicy) throws IOException {
    Path splitDir = new Path(getSplitsDir(hri), familyName);
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_REGEX regex above. The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String parentRegionName = regionInfoForFs.getEncodedName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(splitDir, f.getPath().getName() + "." + parentRegionName);
    if (((KnobRuntime.check(java.util.UUID.fromString("df7b0c3b-fc51-3464-95fb-21358bb5ae5f"))) ? (createDir(p)) : (((KnobRuntime.check(java.util.UUID.fromString("32f6e628-f057-33ef-b5c4-08ec5763e00c"))) ? (deleteDir(p)) : (fs.exists(p)))))) {
      LOG.warn("Found an already existing split file for {}. Assuming this is a recovery.", p);
      return p;
    }
    boolean createLinkFile = false;
    if (splitPolicy == null || !splitPolicy.skipStoreFileRangeCheck(familyName)) {
      // Check whether the split row lies in the range of the store file
      // If it is outside the range, return directly.
      f.initReader();
      try {
        Cell splitKey = PrivateCellUtil.createFirstOnRow(splitRow);
        Optional<Cell> lastKey = f.getLastKey();
        Optional<Cell> firstKey = f.getFirstKey();
        if (top) {
          // check if larger than last key.
          // If lastKey is null means storefile is empty.
          if (!lastKey.isPresent()) {
            return null;
          }
          if (f.getComparator().compare(splitKey, lastKey.get()) > 0) {
            return null;
          }
          if (firstKey.isPresent() && f.getComparator().compare(splitKey, firstKey.get()) <= 0) {
            LOG.debug("Will create HFileLink file for {}, top=true", f.getPath());
            createLinkFile = true;
          }
        } else {
          // check if smaller than first key
          // If firstKey is null means storefile is empty.
          if (!firstKey.isPresent()) {
            return null;
          }
          if (f.getComparator().compare(splitKey, firstKey.get()) < 0) {
            return null;
          }
          if (lastKey.isPresent() && f.getComparator().compare(splitKey, lastKey.get()) >= 0) {
            LOG.debug("Will create HFileLink file for {}, top=false", f.getPath());
            createLinkFile = true;
          }
        }
      } finally {
        f.closeStoreFile(f.getCacheConf() != null ? f.getCacheConf().shouldEvictOnClose() : true);
      }
    }
    if (createLinkFile) {
      // create HFileLink file instead of Reference file for child
      String hfileName = f.getPath().getName();
      TableName linkedTable = regionInfoForFs.getTable();
      String linkedRegion = regionInfoForFs.getEncodedName();
      try {
        if (HFileLink.isHFileLink(hfileName)) {
          Matcher m = LINK_NAME_PATTERN.matcher(hfileName);
          if (!m.matches()) {
            throw new IllegalArgumentException(hfileName + " is not a valid HFileLink name!");
          }
          linkedTable = TableName.valueOf(m.group(1), m.group(2));
          linkedRegion = m.group(3);
          hfileName = m.group(4);
        }
        // must create back reference here
        HFileLink.create(conf, fs, splitDir, familyName, hri.getTable().getNameAsString(),
          hri.getEncodedName(), linkedTable, linkedRegion, hfileName, true);
        Path path =
          new Path(splitDir, HFileLink.createHFileLinkName(linkedTable, linkedRegion, hfileName));
        LOG.info("Created linkFile:" + path.toString() + " for child: " + hri.getEncodedName()
          + ", parent: " + regionInfoForFs.getEncodedName());
        return path;
      } catch (IOException e) {
        // if create HFileLink file failed, then just skip the error and create Reference file
        LOG.error("Create link file for " + hfileName + " for child " + hri.getEncodedName()
          + "failed, will create Reference file", e);
      }
    }
    // A reference to the bottom half of the hsf store file.
    Reference r =
      top ? Reference.createTopReference(splitRow) : Reference.createBottomReference(splitRow);
    return r.write(fs, p);
  }

  // ===========================================================================
  // Merge Helpers
  // ===========================================================================

  Path getMergesDir(final RegionInfo hri) {
    return new Path(getTableDir(), hri.getEncodedName());
  }

  /**
   * Remove merged region
   * @param mergedRegion {@link RegionInfo}
   */
  public void cleanupMergedRegion(final RegionInfo mergedRegion) throws IOException {
    Path regionDir = new Path(this.tableDir, mergedRegion.getEncodedName());
    if (this.fs.exists(regionDir) && !this.fs.delete(regionDir, true)) {
      throw new IOException("Failed delete of " + regionDir);
    }
  }

  static boolean mkdirs(FileSystem fs, Configuration conf, Path dir) throws IOException {
    if (
      FSUtils.isDistributedFileSystem(fs)
        || !conf.getBoolean(HConstants.ENABLE_DATA_FILE_UMASK, false)
    ) {
      return fs.mkdirs(dir);
    }
    FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
if(KnobRuntime.check(java.util.UUID.fromString("05f5a950-28b9-32d6-8d1f-41fff03de1d5"))) {
throw new java.io.IOException("Injected exception");
}
    return fs.mkdirs(dir, perms);
  }

  /**
   * Write out a merge reference under the given merges directory.
   * @param mergingRegion {@link RegionInfo} for one of the regions being merged.
   * @param familyName    Column Family Name
   * @param f             File to create reference.
   * @return Path to created reference.
   * @throws IOException if the merge write fails.
   */
  public Path mergeStoreFile(RegionInfo mergingRegion, String familyName, HStoreFile f)
    throws IOException {
    Path referenceDir = new Path(getMergesDir(regionInfoForFs), familyName);
    // A whole reference to the store file.
    Reference r = Reference.createTopReference(mergingRegion.getStartKey());
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_REGEX regex above. The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String mergingRegionName = mergingRegion.getEncodedName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(referenceDir, f.getPath().getName() + "." + mergingRegionName);
    return r.write(fs, p);
  }

  /**
   * Commit a merged region, making it ready for use.
   */
  public void commitMergedRegion(List<Path> allMergedFiles, MasterProcedureEnv env)
    throws IOException {
    Path regionDir = getMergesDir(regionInfoForFs);
    if (((KnobRuntime.check(java.util.UUID.fromString("7316b4e2-da75-3d63-8458-4c55bcaf602a"))) ? ((regionDir != null) || (fs.exists(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("0cda1c88-59b6-3074-a1b2-285572241934"))) ? (((regionDir) != (null)) && (fs.exists(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("dfec7a65-89be-3940-8c88-2d0be4ab42b1"))) ? ((regionDir != null) && (fs.exists(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("1451c401-f35a-380e-a8e2-e328a52e983a"))) ? (((regionDir) == (null)) || (deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("41dd6f82-ed87-3ad8-85d5-1756458be83b"))) ? ((regionDir != null) && (deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("6115a85d-56b2-346b-a8ac-098af6d9f238"))) ? (deleteDir(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("c1fab925-f924-389e-973b-c884251f080c"))) ? ((regionDir != null) && (createDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("40ba607d-7fb4-3e02-9c97-5c3ab5408a84"))) ? ((regionDir != null) || (createDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("77bb1f10-bbcb-3101-ae44-b10f4170d7bc"))) ? (createDir(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("5ca7be62-e861-3ef8-99da-93ca766fd2d6"))) ? (((regionDir) == (null)) || (fs.exists(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("1c229175-409d-3890-969a-f00c446e55ee"))) ? (regionDir != null) : (((KnobRuntime.check(java.util.UUID.fromString("832812bb-113a-3365-b574-ecd71a50557f"))) ? ((regionDir) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3a67834f-f980-3982-a5aa-1f650e57a35a"))) ? (((regionDir) == (null)) && (fs.exists(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("ac832f14-3c3c-35ab-911d-ef1695e11f72"))) ? (((regionDir) == (null)) && (deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("48c4b3ff-ae72-3162-8755-3c6cde4ef8b1"))) ? (((regionDir) != (null)) || (fs.exists(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("bebe2acb-6238-35b1-b6e7-cb5f3968ff30"))) ? (((regionDir) != (null)) || (createDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("30158ca7-43e3-39ee-afcc-f953ec1213f0"))) ? (((regionDir) == (null)) && (createDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("932d2b08-69ca-308b-8803-1f42d4625142"))) ? (((regionDir) == (null)) || (createDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("eac8053d-5ee0-3916-b2e2-ffcd861ab083"))) ? ((regionDir != null) || (deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("9a811afd-d140-3b3f-af88-6b06b32e2e7f"))) ? (((regionDir) != (null)) && (deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("cc0dbc75-ec6a-3bf5-996f-ae92a7688dd6"))) ? (((regionDir) != (null)) && (createDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("bb38883a-f710-378d-89f1-d7ced8d4878b"))) ? (((regionDir) != (null)) || (deleteDir(regionDir))) : (((KnobRuntime.check(java.util.UUID.fromString("9b4490d7-78ba-339e-b638-cf0ad2c29102"))) ? (fs.exists(regionDir)) : (((KnobRuntime.check(java.util.UUID.fromString("61d319c8-931b-3ec3-b338-6f008f0f6512"))) ? ((regionDir) != (null)) : (regionDir != null && fs.exists(regionDir)))))))))))))))))))))))))))))))))))))))))))))))))) {
      // Write HRI to a file in case we need to recover hbase:meta
if(KnobRuntime.check(java.util.UUID.fromString("b89cb705-c21c-35d0-8b96-c79644626113"))) {
throw new java.io.IOException("Injected exception");
}
      Path regionInfoFile = new Path(regionDir, REGION_INFO_FILE);
      byte[] regionInfoContent = getRegionInfoFileContent(regionInfo);
      writeRegionInfoFileContent(conf, fs, regionInfoFile, regionInfoContent);
      insertRegionFilesIntoStoreTracker(allMergedFiles, env, this);
    }
  }

  // ===========================================================================
  // Create/Open/Delete Helpers
  // ===========================================================================

  /** Returns Content of the file we write out to the filesystem under a region */
  private static byte[] getRegionInfoFileContent(final RegionInfo hri) throws IOException {
    return RegionInfo.toDelimitedByteArray(hri);
  }

  /**
   * Create a {@link RegionInfo} from the serialized version on-disk.
   * @param fs        {@link FileSystem} that contains the Region Info file
   * @param regionDir {@link Path} to the Region Directory that contains the Info file
   * @return An {@link RegionInfo} instance gotten from the Region Info file.
   * @throws IOException if an error occurred during file open/read operation.
   */
  public static RegionInfo loadRegionInfoFileContent(final FileSystem fs, final Path regionDir)
    throws IOException {
    FSDataInputStream in = fs.open(new Path(regionDir, REGION_INFO_FILE));
    try {
      return RegionInfo.parseFrom(in);
    } finally {
      in.close();
    }
  }

  /**
   * Write the .regioninfo file on-disk.
   * <p/>
   * Overwrites if exists already.
   */
  private static void writeRegionInfoFileContent(final Configuration conf, final FileSystem fs,
    final Path regionInfoFile, final byte[] content) throws IOException {
    // First check to get the permissions
    FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // Write the RegionInfo file content
    try (FSDataOutputStream out = FSUtils.create(conf, fs, regionInfoFile, perms, null)) {
      out.write(content);
    }
  }

  /**
   * Write out an info file under the stored region directory. Useful recovering mangled regions. If
   * the regionInfo already exists on-disk, then we fast exit.
   */
  void checkRegionInfoOnFilesystem() throws IOException {
    // Compose the content of the file so we can compare to length in filesystem. If not same,
    // rewrite it (it may have been written in the old format using Writables instead of pb). The
    // pb version is much shorter -- we write now w/o the toString version -- so checking length
    // only should be sufficient. I don't want to read the file every time to check if it pb
    // serialized.
    byte[] content = getRegionInfoFileContent(regionInfoForFs);

    // Verify if the region directory exists before opening a region. We need to do this since if
    // the region directory doesn't exist we will re-create the region directory and a new HRI
    // when HRegion.openHRegion() is called.
    try {
if(KnobRuntime.check(java.util.UUID.fromString("dec98c5c-c9de-3e7a-a27e-a113116234d9"))) {
throw new java.io.IOException("Injected exception");
}
      FileStatus status = fs.getFileStatus(getRegionDir());
    } catch (FileNotFoundException e) {
      LOG.warn(getRegionDir() + " doesn't exist for region: " + regionInfoForFs.getEncodedName()
        + " on table " + regionInfo.getTable());
    }

    try {
      Path regionInfoFile = new Path(getRegionDir(), REGION_INFO_FILE);
      FileStatus status = fs.getFileStatus(regionInfoFile);
      if (status != null && status.getLen() == content.length) {
        // Then assume the content good and move on.
        // NOTE: that the length is not sufficient to define the the content matches.
        return;
      }

      LOG.info("Rewriting .regioninfo file at: " + regionInfoFile);
      if (!fs.delete(regionInfoFile, false)) {
        throw new IOException("Unable to remove existing " + regionInfoFile);
      }
    } catch (FileNotFoundException e) {
      LOG.warn(REGION_INFO_FILE + " file not found for region: " + regionInfoForFs.getEncodedName()
        + " on table " + regionInfo.getTable());
    }

    // Write HRI to a file in case we need to recover hbase:meta
    writeRegionInfoOnFilesystem(content, true);
  }

  /**
   * Write out an info file under the region directory. Useful recovering mangled regions.
   * @param useTempDir indicate whether or not using the region .tmp dir for a safer file creation.
   */
  private void writeRegionInfoOnFilesystem(boolean useTempDir) throws IOException {
    byte[] content = getRegionInfoFileContent(regionInfoForFs);
if(KnobRuntime.check(java.util.UUID.fromString("c19781fe-8cc9-34eb-be3b-30b0f0b8564a"))) {
useTempDir = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("1236a235-2c03-3fe0-8de3-d4eb53f9f2d0"))) {
useTempDir = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("cf8f0db7-d854-3549-8d39-bc3e54dc7bf1"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("da390ac5-fa07-32a1-b294-c9a6f8f72350"))) { writeRegionInfoOnFilesystem(content, !useTempDir); } else { writeRegionInfoOnFilesystem(content, useTempDir); }
  }

  /**
   * Write out an info file under the region directory. Useful recovering mangled regions.
   * @param regionInfoContent serialized version of the {@link RegionInfo}
   * @param useTempDir        indicate whether or not using the region .tmp dir for a safer file
   *                          creation.
   */
  private void writeRegionInfoOnFilesystem(final byte[] regionInfoContent, final boolean useTempDir)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("80e68bdf-9c1a-3d56-91e0-04846c1ff4d1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("060b3d1f-5cb7-3e24-bf7d-e7c4ed0db19d"))) {
return;
}
    Path regionInfoFile = new Path(getRegionDir(), REGION_INFO_FILE);
    if (((KnobRuntime.check(java.util.UUID.fromString("4770b067-3868-376a-96f8-22d0d2dc5549"))) ? (!useTempDir) : (((KnobRuntime.check(java.util.UUID.fromString("ae5265cd-adc2-3f3d-957d-0d9eba9c1707"))) ? (true) : (useTempDir))))) {
      // Create in tmpDir and then move into place in case we crash after
      // create but before close. If we don't successfully close the file,
      // subsequent region reopens will fail the below because create is
      // registered in NN.

      // And then create the file
      Path tmpPath = new Path(getTempDir(), REGION_INFO_FILE);

      // If datanode crashes or if the RS goes down just before the close is called while trying to
      // close the created regioninfo file in the .tmp directory then on next
      // creation we will be getting AlreadyCreatedException.
      // Hence delete and create the file if exists.
      if (CommonFSUtils.isExists(fs, tmpPath)) {
        CommonFSUtils.delete(fs, tmpPath, true);
      }

      // Write HRI to a file in case we need to recover hbase:meta
      writeRegionInfoFileContent(conf, fs, tmpPath, regionInfoContent);

      // Move the created file to the original path
      if (fs.exists(tmpPath) && !rename(tmpPath, regionInfoFile)) {
        throw new IOException("Unable to rename " + tmpPath + " to " + regionInfoFile);
      }
    } else {
      // Write HRI to a file in case we need to recover hbase:meta
      writeRegionInfoFileContent(conf, fs, regionInfoFile, regionInfoContent);
    }
  }

  /**
   * Create a new Region on file-system.
   * @param conf       the {@link Configuration} to use
   * @param fs         {@link FileSystem} from which to add the region
   * @param tableDir   {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region to be added
   * @throws IOException if the region creation fails due to a FileSystem exception.
   */
  public static HRegionFileSystem createRegionOnFileSystem(final Configuration conf,
    final FileSystem fs, final Path tableDir, final RegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);

    // We only create a .regioninfo and the region directory if this is the default region replica
    if (regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      Path regionDir = regionFs.getRegionDir();
      if (fs.exists(regionDir)) {
        LOG.warn("Trying to create a region that already exists on disk: " + regionDir);
      } else {
        // Create the region directory
        if (!createDirOnFileSystem(fs, conf, regionDir)) {
          LOG.warn("Unable to create the region directory: " + regionDir);
          throw new IOException("Unable to create region directory: " + regionDir);
        }
      }

      // Write HRI to a file in case we need to recover hbase:meta
      regionFs.writeRegionInfoOnFilesystem(false);
    } else {
      if (LOG.isDebugEnabled())
        LOG.debug("Skipping creation of .regioninfo file for " + regionInfo);
    }
    return regionFs;
  }

  /**
   * Open Region from file-system.
   * @param conf       the {@link Configuration} to use
   * @param fs         {@link FileSystem} from which to add the region
   * @param tableDir   {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region to be added
   * @param readOnly   True if you don't want to edit the region data
   * @throws IOException if the region creation fails due to a FileSystem exception.
   */
  public static HRegionFileSystem openRegionFromFileSystem(final Configuration conf,
    final FileSystem fs, final Path tableDir, final RegionInfo regionInfo, boolean readOnly)
    throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regionDir = regionFs.getRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("Trying to open a region that do not exists on disk: " + regionDir);
      throw new IOException("The specified region do not exists on disk: " + regionDir);
    }

    if (!readOnly) {
      // Cleanup temporary directories
      regionFs.cleanupTempDir();

      // If it doesn't exists, Write HRI to a file, in case we need to recover hbase:meta
      // Only create HRI if we are the default replica
      if (regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
        regionFs.checkRegionInfoOnFilesystem();
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping creation of .regioninfo file for " + regionInfo);
        }
      }
    }

    return regionFs;
  }

  /**
   * Remove the region from the table directory, archiving the region's hfiles.
   * @param conf       the {@link Configuration} to use
   * @param fs         {@link FileSystem} from which to remove the region
   * @param tableDir   {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region to be deleted
   * @throws IOException if the request cannot be completed
   */
  public static void deleteRegionFromFileSystem(final Configuration conf, final FileSystem fs,
    final Path tableDir, final RegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regionDir = regionFs.getRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("Trying to delete a region that do not exists on disk: " + regionDir);
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETING region " + regionDir);
    }

    // Archive region
    Path rootDir = CommonFSUtils.getRootDir(conf);
if(KnobRuntime.check(java.util.UUID.fromString("07299229-3f53-36ca-a485-c6646820cc28"))) {
throw new java.io.IOException("Injected exception");
}
    HFileArchiver.archiveRegion(fs, rootDir, tableDir, regionDir);

    // Delete empty region dir
    if (!fs.delete(regionDir, true)) {
      LOG.warn("Failed delete of " + regionDir);
    }
  }

  /**
   * Creates a directory. Assumes the user has already checked for this directory existence.
   * @return the result of fs.mkdirs(). In case underlying fs throws an IOException, it checks
   *         whether the directory exists or not, and returns true if it exists.
   */
  boolean createDir(Path dir) throws IOException {
    int i = 0;
    IOException lastIOE = null;
    do {
      try {
        return mkdirs(fs, conf, dir);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(dir)) return true; // directory is present
        try {
          sleepBeforeRetry("Create Directory", i + 1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException) new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createDir", lastIOE);
  }

  /**
   * Renames a directory. Assumes the user has already checked for this directory existence.
   * @return true if rename is successful.
   */
  boolean rename(Path srcpath, Path dstPath) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.rename(srcpath, dstPath);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(srcpath) && fs.exists(dstPath)) return true; // successful move
        // dir is not there, retry after some time.
        try {
          sleepBeforeRetry("Rename Directory", i + 1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException) new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in rename", lastIOE);
  }

  /**
   * Deletes a directory. Assumes the user has already checked for this directory existence.
   * @return true if the directory is deleted.
   */
  boolean deleteDir(Path dir) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.delete(dir, true);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(dir)) return true;
        // dir is there, retry deleting after some time.
        try {
          sleepBeforeRetry("Delete Directory", i + 1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException) new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in DeleteDir", lastIOE);
  }

  /**
   * sleeping logic; handles the interrupt exception.
   */
  private void sleepBeforeRetry(String msg, int sleepMultiplier) throws InterruptedException {
    sleepBeforeRetry(msg, sleepMultiplier, baseSleepBeforeRetries, hdfsClientRetriesNumber);
  }

  /**
   * Creates a directory for a filesystem and configuration object. Assumes the user has already
   * checked for this directory existence.
   * @return the result of fs.mkdirs(). In case underlying fs throws an IOException, it checks
   *         whether the directory exists or not, and returns true if it exists.
   */
  private static boolean createDirOnFileSystem(FileSystem fs, Configuration conf, Path dir)
    throws IOException {
    int i = 0;
    IOException lastIOE = null;
    int hdfsClientRetriesNumber =
      conf.getInt("hdfs.client.retries.number", DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
    int baseSleepBeforeRetries =
      conf.getInt("hdfs.client.sleep.before.retries", DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
    do {
      try {
        return fs.mkdirs(dir);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(dir)) return true; // directory is present
        try {
          sleepBeforeRetry("Create Directory", i + 1, baseSleepBeforeRetries,
            hdfsClientRetriesNumber);
        } catch (InterruptedException e) {
          throw (InterruptedIOException) new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in createDir", lastIOE);
  }

  /**
   * sleeping logic for static methods; handles the interrupt exception. Keeping a static version
   * for this to avoid re-looking for the integer values.
   */
  private static void sleepBeforeRetry(String msg, int sleepMultiplier, int baseSleepBeforeRetries,
    int hdfsClientRetriesNumber) throws InterruptedException {
    if (sleepMultiplier > hdfsClientRetriesNumber) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(msg + ", retries exhausted");
      }
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(msg + ", sleeping " + baseSleepBeforeRetries + " times " + sleepMultiplier);
    }
    Thread.sleep((long) baseSleepBeforeRetries * sleepMultiplier);
  }
}
