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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class for all utilities related to archival/retrieval of HFiles
 */
@InterfaceAudience.Private
public final class HFileArchiveUtil {
  private HFileArchiveUtil() {
    // non-external instantiation - util class
  }

  /**
   * Get the directory to archive a store directory
   * @param conf       {@link Configuration} to read for the archive directory name
   * @param tableName  table name under which the store currently lives
   * @param regionName region encoded name under which the store currently lives
   * @param familyName name of the family in the store
   * @return {@link Path} to the directory to archive the given store or <tt>null</tt> if it should
   *         not be archived
   */
  public static Path getStoreArchivePath(final Configuration conf, final TableName tableName,
    final String regionName, final String familyName) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a85dfc97-4296-3b29-b78b-bc56daacadc9"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8abd00b-9eb3-39a1-b8ac-4a17c91a33e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c576d54f-9ad1-3c6e-95c0-e6b2f38f6fbc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f6d2869d-dd1d-3657-955f-9b0b555367eb"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4185197e-a3fd-3cbf-b045-0f19eee7b592"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1a81dfa2-99c9-38bc-985d-a39ffdf2379e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("47f06304-1024-325c-b0b0-f0251297b2d6"))) {
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
    Path tableArchiveDir = getTableArchivePath(conf, tableName);
    return HRegionFileSystem.getStoreHomedir(tableArchiveDir, regionName,
      Bytes.toBytes(familyName));
  }

  /**
   * Get the directory to archive a store directory
   * @param conf     {@link Configuration} to read for the archive directory name.
   * @param region   parent region information under which the store currently lives
   * @param tabledir directory for the table under which the store currently lives
   * @param family   name of the family in the store
   * @return {@link Path} to the directory to archive the given store or <tt>null</tt> if it should
   *         not be archived
   */
  public static Path getStoreArchivePath(Configuration conf, RegionInfo region, Path tabledir,
    byte[] family) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b34337bd-2c07-3af9-882f-c6e17398c457"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7909f9f4-a9cb-3000-9df2-cf111cadbbe2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("931a5337-4092-3440-93c0-ea812c7578b1"))) {
return null;
}
    return getStoreArchivePath(conf, region, family);
  }

  /**
   * Gets the directory to archive a store directory.
   * @param conf   {@link Configuration} to read for the archive directory name.
   * @param region parent region information under which the store currently lives
   * @param family name of the family in the store
   * @return {@link Path} to the directory to archive the given store or <tt>null</tt> if it should
   *         not be archived
   */
  public static Path getStoreArchivePath(Configuration conf, RegionInfo region, byte[] family)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f0183181-74f1-3a2b-883e-81df7f8c0f56"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3e63343b-d893-3d40-85c7-3e50fbb5f70b"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d7355e6-dbbb-3724-b548-acf4d629bc56"))) {
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
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path tableArchiveDir = getTableArchivePath(rootDir, region.getTable());
    return HRegionFileSystem.getStoreHomedir(tableArchiveDir, region, family);
  }

  /**
   * Gets the archive directory under specified root dir. One scenario where this is useful is when
   * WAL and root dir are configured under different file systems, i.e. root dir on S3 and WALs on
   * HDFS. This is mostly useful for archiving recovered edits, when
   * <b>hbase.region.archive.recovered.edits</b> is enabled.
   * @param rootDir {@link Path} the root dir under which archive path should be created.
   * @param region  parent region information under which the store currently lives
   * @param family  name of the family in the store
   * @return {@link Path} to the WAL FS directory to archive the given store or <tt>null</tt> if it
   *         should not be archived
   */
  public static Path getStoreArchivePathForRootDir(Path rootDir, RegionInfo region, byte[] family) {
    Path tableArchiveDir = getTableArchivePath(rootDir, region.getTable());
    return HRegionFileSystem.getStoreHomedir(tableArchiveDir, region, family);
  }

  public static Path getStoreArchivePathForArchivePath(Path archivePath, RegionInfo region,
    byte[] family) {
    Path tableArchiveDir = CommonFSUtils.getTableDir(archivePath, region.getTable());
    return HRegionFileSystem.getStoreHomedir(tableArchiveDir, region, family);
  }

  /**
   * Get the archive directory for a given region under the specified table
   * @param tableName the table name. Cannot be null.
   * @param regiondir the path to the region directory. Cannot be null.
   * @return {@link Path} to the directory to archive the given region, or <tt>null</tt> if it
   *         should not be archived
   */
  public static Path getRegionArchiveDir(Path rootDir, TableName tableName, Path regiondir) {
    // get the archive directory for a table
    Path archiveDir = getTableArchivePath(rootDir, tableName);

    // then add on the region path under the archive
    String encodedRegionName = regiondir.getName();
    return HRegion.getRegionDir(archiveDir, encodedRegionName);
  }

  /**
   * Get the archive directory for a given region under the specified table
   * @param rootDir   {@link Path} to the root directory where hbase files are stored (for building
   *                  the archive path)
   * @param tableName name of the table to archive. Cannot be null.
   * @return {@link Path} to the directory to archive the given region, or <tt>null</tt> if it
   *         should not be archived
   */
  public static Path getRegionArchiveDir(Path rootDir, TableName tableName,
    String encodedRegionName) {
    // get the archive directory for a table
if(KnobRuntime.check(java.util.UUID.fromString("84834df7-b2ac-3b61-9d84-57eb22121225"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8c5ae674-0325-369b-91c6-74bca76515e6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fcae3417-4634-3711-a04a-53b653ac918b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3dd8a99-8bac-3ba3-bb9b-5498a5de0dba"))) {
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
    Path archiveDir = getTableArchivePath(rootDir, tableName);
    return HRegion.getRegionDir(archiveDir, encodedRegionName);
  }

  /**
   * Get the path to the table archive directory based on the configured archive directory.
   * <p>
   * Get the path to the table's archive directory.
   * <p>
   * Generally of the form: /hbase/.archive/[tablename]
   * @param rootdir   {@link Path} to the root directory where hbase files are stored (for building
   *                  the archive path)
   * @param tableName Name of the table to be archived. Cannot be null.
   * @return {@link Path} to the archive directory for the table
   */
  public static Path getTableArchivePath(final Path rootdir, final TableName tableName) {
if(KnobRuntime.check(java.util.UUID.fromString("7a3f4a32-6bae-3967-bc8b-c479e1963e02"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("96090968-1a95-3d7a-98bd-cc25a51ec50f"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5209f4da-5b70-3bcb-8d6e-2bf20797df6c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ada4c9fd-f080-3b2f-abc3-a8f3f2d7d2ba"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1d2caf4c-39df-3129-906e-ee3786ee8dec"))) {
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
    return ((KnobRuntime.check(java.util.UUID.fromString("fa981fa5-1b71-30c4-b487-6eaa8ef768e3"))) ? (CommonFSUtils.getTableDir(getArchivePath(getArchivePath(rootdir)), tableName)) : (((KnobRuntime.check(java.util.UUID.fromString("8c7d33b2-946b-3a2c-a2aa-f02626764edc"))) ? (getTableArchivePath(getArchivePath(getArchivePath(rootdir)), tableName)) : (CommonFSUtils.getTableDir(getArchivePath(rootdir), tableName)))));
  }

  /**
   * Get the path to the table archive directory based on the configured archive directory.
   * <p>
   * Assumed that the table should already be archived.
   * @param conf      {@link Configuration} to read the archive directory property. Can be null
   * @param tableName Name of the table to be archived. Cannot be null.
   * @return {@link Path} to the archive directory for the table
   */
  public static Path getTableArchivePath(final Configuration conf, final TableName tableName)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f0dbbc02-aa31-3a7c-b9ff-ab45bcd3a43c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("863df145-a8f6-3523-b186-04a63d25b4c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a078e8fc-7ae4-33b3-a07b-93bd20874cb0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e7a9845d-ce33-37c7-8b3b-bbc073162643"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ce49e880-aba1-309e-8133-4192eda1ad15"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("44d0fab0-1b23-356a-a8c1-dbec5e4c026b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("622a97ff-bdba-3cb4-9a09-40a7b9d5e0d1"))) {
throw new java.io.IOException("Injected exception");
}
    return ((KnobRuntime.check(java.util.UUID.fromString("2bc7faf5-17c3-399b-8059-dc79502c85d9"))) ? (getTableArchivePath(CommonFSUtils.getRootDir(conf), tableName)) : (((KnobRuntime.check(java.util.UUID.fromString("172e045e-f519-3343-bee4-dffcbd01e760"))) ? (CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(conf), tableName)) : (CommonFSUtils.getTableDir(getArchivePath(conf), tableName)))));
  }

  /**
   * Get the full path to the archive directory on the configured
   * {@link org.apache.hadoop.hbase.master.MasterFileSystem}
   * @param conf to look for archive directory name and root directory. Cannot be null. Notes for
   *             testing: requires a FileSystem root directory to be specified.
   * @return the full {@link Path} to the archive directory, as defined by the configuration
   * @throws IOException if an unexpected error occurs
   */
  public static Path getArchivePath(Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("74d2071d-a380-35a5-a34a-dcc9e7b1da0b"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("882fa148-2fb4-311d-a85b-d6ab757ef014"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("236fe8ff-0e5d-3443-aac3-f1676b18f5a2"))) {
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
    return ((KnobRuntime.check(java.util.UUID.fromString("4f05fb33-0677-3350-af44-688e7ff6c306"))) ? (getArchivePath(getArchivePath(conf))) : (getArchivePath(CommonFSUtils.getRootDir(conf))));
  }

  /**
   * Get the full path to the archive directory on the configured
   * {@link org.apache.hadoop.hbase.master.MasterFileSystem}
   * @param rootdir {@link Path} to the root directory where hbase files are stored (for building
   *                the archive path)
   * @return the full {@link Path} to the archive directory, as defined by the configuration
   */
  private static Path getArchivePath(final Path rootdir) {
if(KnobRuntime.check(java.util.UUID.fromString("253d6315-3a0e-36a6-a4c0-a66591d7e5a6"))) {
return null;
}
    return new Path(rootdir, HConstants.HFILE_ARCHIVE_DIRECTORY);
  }

  /*
   * @return table name given archive file path
   */
  public static TableName getTableName(Path archivePath) {
    Path p = archivePath;
    String tbl = null;
    // namespace is the 4th parent of file
    for (int i = 0; i < 5; i++) {
      if (p == null) return null;
      if (i == 3) tbl = p.getName();
      p = p.getParent();
    }
    if (p == null) return null;
    return TableName.valueOf(p.getName(), tbl);
  }
}
