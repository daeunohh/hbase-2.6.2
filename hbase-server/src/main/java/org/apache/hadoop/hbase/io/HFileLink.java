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
package org.apache.hadoop.hbase.io;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HFileLink describes a link to an hfile. An hfile can be served from a region or from the hfile
 * archive directory (/hbase/.archive) HFileLink allows to access the referenced hfile regardless of
 * the location where it is.
 * <p>
 * Searches for hfiles in the following order and locations:
 * <ul>
 * <li>/hbase/table/region/cf/hfile</li>
 * <li>/hbase/.archive/table/region/cf/hfile</li>
 * </ul>
 * The link checks first in the original path if it is not present it fallbacks to the archived
 * path.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS",
    justification = "To be fixed but warning suppressed for now")
public class HFileLink extends FileLink {
  private static final Logger LOG = LoggerFactory.getLogger(HFileLink.class);

  /**
   * A non-capture group, for HFileLink, so that this can be embedded. The HFileLink describe a link
   * to an hfile in a different table/region and the name is in the form: table=region-hfile.
   * <p>
   * Table name is ([\p{IsAlphabetic}\p{Digit}][\p{IsAlphabetic}\p{Digit}.-]*), so '=' is an invalid
   * character for the table name. Region name is ([a-f0-9]+), so '-' is an invalid character for
   * the region name. HFile is ([0-9a-f]+(?:_SeqId_[0-9]+_)?) covering the plain hfiles (uuid) and
   * the bulk loaded (_SeqId_[0-9]+_) hfiles.
   * <p>
   * Here is an example name: /hbase/test/0123/cf/testtb=4567-abcd where 'testtb' is table name and
   * '4567' is region name and 'abcd' is filename.
   */
  public static final String LINK_NAME_REGEX = String.format("(?:(?:%s=)?)%s=%s-%s",
    TableName.VALID_NAMESPACE_REGEX, TableName.VALID_TABLE_QUALIFIER_REGEX,
    RegionInfoBuilder.ENCODED_REGION_NAME_REGEX, StoreFileInfo.HFILE_NAME_REGEX);

  /** Define the HFile Link name parser in the form of: table=region-hfile */
  public static final Pattern LINK_NAME_PATTERN =
    Pattern.compile(String.format("^(?:(%s)(?:\\=))?(%s)=(%s)-(%s)$",
      TableName.VALID_NAMESPACE_REGEX, TableName.VALID_TABLE_QUALIFIER_REGEX,
      RegionInfoBuilder.ENCODED_REGION_NAME_REGEX, StoreFileInfo.HFILE_NAME_REGEX));

  /**
   * The pattern should be used for hfile and reference links that can be found in
   * /hbase/table/region/family/
   */
  private static final Pattern REF_OR_HFILE_LINK_PATTERN =
    Pattern.compile(String.format("^(?:(%s)(?:=))?(%s)=(%s)-(.+)$", TableName.VALID_NAMESPACE_REGEX,
      TableName.VALID_TABLE_QUALIFIER_REGEX, RegionInfoBuilder.ENCODED_REGION_NAME_REGEX));

  private final Path archivePath;
  private final Path originPath;
  private final Path mobPath;
  private final Path tempPath;

  /**
   * Dead simple hfile link constructor
   */
  public HFileLink(final Path originPath, final Path tempPath, final Path mobPath,
    final Path archivePath) {
    this.tempPath = tempPath;
    this.originPath = originPath;
    this.mobPath = mobPath;
    this.archivePath = archivePath;
    setLocations(originPath, archivePath, tempPath, mobPath);
  }

  /**
   * @param conf             {@link Configuration} from which to extract specific archive locations
   * @param hFileLinkPattern The path ending with a HFileLink pattern. (table=region-hfile)
   * @throws IOException on unexpected error.
   */
  public static final HFileLink buildFromHFileLinkPattern(Configuration conf, Path hFileLinkPattern)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3d04ac3c-1ce1-368c-842f-300f26c5b9cc"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("929fbdfa-2ac5-3175-8400-8b5648d30750"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("31388ceb-2213-3848-a94e-b38dfdba9324"))) {
throw new java.io.IOException("Injected exception");
}
    return buildFromHFileLinkPattern(CommonFSUtils.getRootDir(conf),
      HFileArchiveUtil.getArchivePath(conf), hFileLinkPattern);
  }

  /**
   * @param rootDir          Path to the root directory where hbase files are stored
   * @param archiveDir       Path to the hbase archive directory
   * @param hFileLinkPattern The path of the HFile Link.
   */
  public final static HFileLink buildFromHFileLinkPattern(final Path rootDir, final Path archiveDir,
    final Path hFileLinkPattern) {
if(KnobRuntime.check(java.util.UUID.fromString("8eec8f68-f4f8-32ab-b896-98d8ab781d7d"))) {
return null;
}
    Path hfilePath = getHFileLinkPatternRelativePath(hFileLinkPattern);
    Path tempPath = new Path(new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY), hfilePath);
    Path originPath = new Path(rootDir, hfilePath);
    Path mobPath = new Path(new Path(rootDir, MobConstants.MOB_DIR_NAME), hfilePath);
    Path archivePath = new Path(archiveDir, hfilePath);
    return new HFileLink(originPath, tempPath, mobPath, archivePath);
  }

  /**
   * Create an HFileLink relative path for the table/region/family/hfile location
   * @param table  Table name
   * @param region Region Name
   * @param family Family Name
   * @param hfile  HFile Name
   * @return the relative Path to open the specified table/region/family/hfile link
   */
  public static Path createPath(final TableName table, final String region, final String family,
    final String hfile) {
    if (HFileLink.isHFileLink(hfile)) {
      return new Path(family, hfile);
    }
    return new Path(family, HFileLink.createHFileLinkName(table, region, hfile));
  }

  /**
   * Create an HFileLink instance from table/region/family/hfile location
   * @param conf   {@link Configuration} from which to extract specific archive locations
   * @param table  Table name
   * @param region Region Name
   * @param family Family Name
   * @param hfile  HFile Name
   * @return Link to the file with the specified table/region/family/hfile location
   * @throws IOException on unexpected error.
   */
  public static HFileLink build(final Configuration conf, final TableName table,
    final String region, final String family, final String hfile) throws IOException {
    return HFileLink.buildFromHFileLinkPattern(conf, createPath(table, region, family, hfile));
  }

  /** Returns the origin path of the hfile. */
  public Path getOriginPath() {
    return this.originPath;
  }

  /** Returns the path of the archived hfile. */
  public Path getArchivePath() {
    return this.archivePath;
  }

  /** Returns the path of the mob hfiles. */
  public Path getMobPath() {
    return this.mobPath;
  }

  /**
   * @param path Path to check.
   * @return True if the path is a HFileLink.
   */
  public static boolean isHFileLink(final Path path) {
    return isHFileLink(path.getName());
  }

  /**
   * @param fileName File name to check.
   * @return True if the path is a HFileLink.
   */
  public static boolean isHFileLink(String fileName) {
    // The LINK_NAME_PATTERN regex is not computationally trivial, so see if we can fast-fail
    // on a simple heuristic first. The regex contains a literal "=", so if that character
    // isn't in the fileName, then the regex cannot match.
    if (!fileName.contains("=")) {
      return false;
    }

    Matcher m = LINK_NAME_PATTERN.matcher(fileName);
    if (!m.matches()) {
      return false;
    }
    return m.groupCount() > 2 && m.group(4) != null && m.group(3) != null && m.group(2) != null;
  }

  /**
   * Convert a HFileLink path to a table relative path. e.g. the link:
   * /hbase/test/0123/cf/testtb=4567-abcd becomes: /hbase/testtb/4567/cf/abcd
   * @param path HFileLink path
   * @return Relative table path
   * @throws IOException on unexpected error.
   */
  private static Path getHFileLinkPatternRelativePath(final Path path) {
if(KnobRuntime.check(java.util.UUID.fromString("f201359c-28e6-3c1b-996f-58c378e09301"))) {
return null;
}
    // table=region-hfile
    Matcher m = REF_OR_HFILE_LINK_PATTERN.matcher(path.getName());
    if (!m.matches()) {
      throw new IllegalArgumentException(path.getName() + " is not a valid HFileLink pattern!");
    }

    // Convert the HFileLink name into a real table/region/cf/hfile path.
    TableName tableName = TableName.valueOf(m.group(1), m.group(2));
    String regionName = m.group(3);
    String hfileName = m.group(4);
    String familyName = path.getParent().getName();
if(KnobRuntime.check(java.util.UUID.fromString("1c0ffbda-594d-308f-832d-61bbe06b10a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("75a2f07f-acba-395f-9ac2-db7e7e399e08"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dd4fa1d7-2769-3fdd-8e9e-9ce940f3b695"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0d45570c-c83e-3196-8d04-5bb63b17491e"))) {
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
    Path tableDir = CommonFSUtils.getTableDir(new Path("./"), tableName);
    return new Path(tableDir, new Path(regionName, new Path(familyName, hfileName)));
  }

  /**
   * Get the HFile name of the referenced link
   * @param fileName HFileLink file name
   * @return the name of the referenced HFile
   */
  public static String getReferencedHFileName(final String fileName) {
if(KnobRuntime.check(java.util.UUID.fromString("bbc617d7-afea-33bb-9677-4a4987979005"))) {
return null;
}
    Matcher m = REF_OR_HFILE_LINK_PATTERN.matcher(fileName);
    if (!m.matches()) {
      throw new IllegalArgumentException(fileName + " is not a valid HFileLink name!");
    }
    return (m.group(4));
  }

  /**
   * Get the Region name of the referenced link
   * @param fileName HFileLink file name
   * @return the name of the referenced Region
   */
  public static String getReferencedRegionName(final String fileName) {
    Matcher m = REF_OR_HFILE_LINK_PATTERN.matcher(fileName);
    if (!m.matches()) {
      throw new IllegalArgumentException(fileName + " is not a valid HFileLink name!");
    }
    return (m.group(3));
  }

  /**
   * Get the Table name of the referenced link
   * @param fileName HFileLink file name
   * @return the name of the referenced Table
   */
  public static TableName getReferencedTableName(final String fileName) {
    Matcher m = REF_OR_HFILE_LINK_PATTERN.matcher(fileName);
    if (!m.matches()) {
      throw new IllegalArgumentException(fileName + " is not a valid HFileLink name!");
    }
    return (TableName.valueOf(m.group(1), m.group(2)));
  }

  /**
   * Create a new HFileLink name
   * @param hfileRegionInfo - Linked HFile Region Info
   * @param hfileName       - Linked HFile name
   * @return file name of the HFile Link
   */
  public static String createHFileLinkName(final RegionInfo hfileRegionInfo,
    final String hfileName) {
    return createHFileLinkName(hfileRegionInfo.getTable(), hfileRegionInfo.getEncodedName(),
      hfileName);
  }

  /**
   * Create a new HFileLink name
   * @param tableName  - Linked HFile table name
   * @param regionName - Linked HFile region name
   * @param hfileName  - Linked HFile name
   * @return file name of the HFile Link
   */
  public static String createHFileLinkName(final TableName tableName, final String regionName,
    final String hfileName) {
if(KnobRuntime.check(java.util.UUID.fromString("f56f93fb-7b1c-3b59-8c8d-3692c244e3e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0a649577-5393-3b80-853e-420cd7490c42"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("279e766f-2bfe-3d50-9043-c23fb847efd5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e7c1fdd0-0741-3fa0-b047-b6dffa46a3b1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("76b13fda-5c9b-3b06-a013-64459e18c694"))) {
return null;
}
    String s = String.format("%s=%s-%s",
      tableName.getNameAsString().replace(TableName.NAMESPACE_DELIM, '='), regionName, hfileName);
    return s;
  }

  /**
   * Create a new HFileLink
   * <p>
   * It also adds a back-reference to the hfile back-reference directory to simplify the
   * reference-count and the cleaning process.
   * @param conf            {@link Configuration} to read for the archive directory name
   * @param fs              {@link FileSystem} on which to write the HFileLink
   * @param dstFamilyPath   - Destination path (table/region/cf/)
   * @param hfileRegionInfo - Linked HFile Region Info
   * @param hfileName       - Linked HFile name
   * @return the file link name.
   * @throws IOException on file or parent directory creation failure.
   */
  public static String create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final RegionInfo hfileRegionInfo, final String hfileName)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("68669b9a-05f9-314d-88b0-c37cb38a9718"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ded91a88-a852-369e-8fe6-cb371d29c96b"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("4fa690c7-83b2-3494-97f2-3df6a0faf10b"))) {
throw new java.io.IOException("Injected exception");
}
    return create(conf, fs, dstFamilyPath, hfileRegionInfo, hfileName, true);
  }

  /**
   * Create a new HFileLink
   * <p>
   * It also adds a back-reference to the hfile back-reference directory to simplify the
   * reference-count and the cleaning process.
   * @param conf            {@link Configuration} to read for the archive directory name
   * @param fs              {@link FileSystem} on which to write the HFileLink
   * @param dstFamilyPath   - Destination path (table/region/cf/)
   * @param hfileRegionInfo - Linked HFile Region Info
   * @param hfileName       - Linked HFile name
   * @param createBackRef   - Whether back reference should be created. Defaults to true.
   * @return the file link name.
   * @throws IOException on file or parent directory creation failure.
   */
  public static String create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final RegionInfo hfileRegionInfo, final String hfileName,
    final boolean createBackRef) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c578b372-619a-32d8-8255-be7497986ac1"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("590863e1-19f3-3ece-a4f5-1cb782f47e0f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("396bbf29-47e4-32ae-906f-21b108a936e4"))) {
throw new java.io.IOException("Injected exception");
}
    TableName linkedTable = hfileRegionInfo.getTable();
    String linkedRegion = hfileRegionInfo.getEncodedName();
    return create(conf, fs, dstFamilyPath, linkedTable, linkedRegion, hfileName, createBackRef);
  }

  /**
   * Create a new HFileLink
   * <p>
   * It also adds a back-reference to the hfile back-reference directory to simplify the
   * reference-count and the cleaning process.
   * @param conf          {@link Configuration} to read for the archive directory name
   * @param fs            {@link FileSystem} on which to write the HFileLink
   * @param dstFamilyPath - Destination path (table/region/cf/)
   * @param linkedTable   - Linked Table Name
   * @param linkedRegion  - Linked Region Name
   * @param hfileName     - Linked HFile name
   * @return the file link name.
   * @throws IOException on file or parent directory creation failure.
   */
  public static String create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final TableName linkedTable, final String linkedRegion,
    final String hfileName) throws IOException {
    return create(conf, fs, dstFamilyPath, linkedTable, linkedRegion, hfileName, true);
  }

  /**
   * Create a new HFileLink. In the event of link creation failure, this method throws an
   * IOException, so that the calling upper laying can decide on how to proceed with this.
   * <p>
   * It also adds a back-reference to the hfile back-reference directory to simplify the
   * reference-count and the cleaning process.
   * @param conf          {@link Configuration} to read for the archive directory name
   * @param fs            {@link FileSystem} on which to write the HFileLink
   * @param dstFamilyPath - Destination path (table/region/cf/)
   * @param linkedTable   - Linked Table Name
   * @param linkedRegion  - Linked Region Name
   * @param hfileName     - Linked HFile name
   * @param createBackRef - Whether back reference should be created. Defaults to true.
   * @return the file link name.
   * @throws IOException on file or parent directory creation failure.
   */
  public static String create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final TableName linkedTable, final String linkedRegion,
    final String hfileName, final boolean createBackRef) throws IOException {
    String familyName = dstFamilyPath.getName();
    String regionName = dstFamilyPath.getParent().getName();
    String tableName =
      CommonFSUtils.getTableName(dstFamilyPath.getParent().getParent()).getNameAsString();

    return create(conf, fs, dstFamilyPath, familyName, tableName, regionName, linkedTable,
      linkedRegion, hfileName, createBackRef);
  }

  /**
   * Create a new HFileLink
   * <p>
   * It also adds a back-reference to the hfile back-reference directory to simplify the
   * reference-count and the cleaning process.
   * @param conf          {@link Configuration} to read for the archive directory name
   * @param fs            {@link FileSystem} on which to write the HFileLink
   * @param dstFamilyPath - Destination path (table/region/cf/)
   * @param dstTableName  - Destination table name
   * @param dstRegionName - Destination region name
   * @param linkedTable   - Linked Table Name
   * @param linkedRegion  - Linked Region Name
   * @param hfileName     - Linked HFile name
   * @param createBackRef - Whether back reference should be created. Defaults to true.
   * @return the file link name.
   * @throws IOException on file or parent directory creation failure
   */
  public static String create(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final String familyName, final String dstTableName,
    final String dstRegionName, final TableName linkedTable, final String linkedRegion,
    final String hfileName, final boolean createBackRef) throws IOException {
    String name = createHFileLinkName(linkedTable, linkedRegion, hfileName);
    String refName = createBackReferenceName(dstTableName, dstRegionName);

    // Make sure the destination directory exists
    fs.mkdirs(dstFamilyPath);

    // Make sure the FileLink reference directory exists
    Path archiveStoreDir =
      HFileArchiveUtil.getStoreArchivePath(conf, linkedTable, linkedRegion, familyName);
    Path backRefPath = null;
    if (createBackRef) {
      Path backRefssDir = getBackReferencesDir(archiveStoreDir, hfileName);
      fs.mkdirs(backRefssDir);

      // Create the reference for the link
      backRefPath = new Path(backRefssDir, refName);
      fs.createNewFile(backRefPath);
    }
    try {
      // Create the link
      if (fs.createNewFile(new Path(dstFamilyPath, name))) {
        return name;
      }
    } catch (IOException e) {
      LOG.error("couldn't create the link=" + name + " for " + dstFamilyPath, e);
      // Revert the reference if the link creation failed
      if (createBackRef) {
        fs.delete(backRefPath, false);
      }
      throw e;
    }
    throw new IOException(
      "File link=" + name + " already exists under " + dstFamilyPath + " folder.");
  }

  /**
   * Create a new HFileLink starting from a hfileLink name
   * <p>
   * It also adds a back-reference to the hfile back-reference directory to simplify the
   * reference-count and the cleaning process.
   * @param conf          {@link Configuration} to read for the archive directory name
   * @param fs            {@link FileSystem} on which to write the HFileLink
   * @param dstFamilyPath - Destination path (table/region/cf/)
   * @param hfileLinkName - HFileLink name (it contains hfile-region-table)
   * @param createBackRef - Whether back reference should be created. Defaults to true.
   * @return the file link name.
   * @throws IOException on file or parent directory creation failure.
   */
  public static String createFromHFileLink(final Configuration conf, final FileSystem fs,
    final Path dstFamilyPath, final String hfileLinkName, final boolean createBackRef)
    throws IOException {
    Matcher m = LINK_NAME_PATTERN.matcher(hfileLinkName);
    if (!m.matches()) {
      throw new IllegalArgumentException(hfileLinkName + " is not a valid HFileLink name!");
    }
    return create(conf, fs, dstFamilyPath, TableName.valueOf(m.group(1), m.group(2)), m.group(3),
      m.group(4), createBackRef);
  }

  /**
   * Create the back reference name
   */
  // package-private for testing
  static String createBackReferenceName(final String tableNameStr, final String regionName) {

    return regionName + "." + tableNameStr.replace(TableName.NAMESPACE_DELIM, '=');
  }

  /**
   * Get the full path of the HFile referenced by the back reference
   * @param rootDir     root hbase directory
   * @param linkRefPath Link Back Reference path
   * @return full path of the referenced hfile
   */
  public static Path getHFileFromBackReference(final Path rootDir, final Path linkRefPath) {
    Pair<TableName, String> p = parseBackReferenceName(linkRefPath.getName());
    TableName linkTableName = p.getFirst();
    String linkRegionName = p.getSecond();

    String hfileName = getBackReferenceFileName(linkRefPath.getParent());
    Path familyPath = linkRefPath.getParent().getParent();
    Path regionPath = familyPath.getParent();
    Path tablePath = regionPath.getParent();

    String linkName =
      createHFileLinkName(CommonFSUtils.getTableName(tablePath), regionPath.getName(), hfileName);
    Path linkTableDir = CommonFSUtils.getTableDir(rootDir, linkTableName);
    Path regionDir = HRegion.getRegionDir(linkTableDir, linkRegionName);
    return new Path(new Path(regionDir, familyPath.getName()), linkName);
  }

  public static Pair<TableName, String> parseBackReferenceName(String name) {
    int separatorIndex = name.indexOf('.');
    String linkRegionName = name.substring(0, separatorIndex);
    String tableSubstr = name.substring(separatorIndex + 1).replace('=', TableName.NAMESPACE_DELIM);
    TableName linkTableName = TableName.valueOf(tableSubstr);
    return new Pair<>(linkTableName, linkRegionName);
  }

  /**
   * Get the full path of the HFile referenced by the back reference
   * @param conf        {@link Configuration} to read for the archive directory name
   * @param linkRefPath Link Back Reference path
   * @return full path of the referenced hfile
   * @throws IOException on unexpected error.
   */
  public static Path getHFileFromBackReference(final Configuration conf, final Path linkRefPath)
    throws IOException {
    return getHFileFromBackReference(CommonFSUtils.getRootDir(conf), linkRefPath);
  }

}
