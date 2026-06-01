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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describe a StoreFile (hfile, reference, link)
 */
@InterfaceAudience.Private
public class StoreFileInfo implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileInfo.class);

  /**
   * A non-capture group, for hfiles, so that this can be embedded. HFiles are uuid ([0-9a-z]+).
   * Bulk loaded hfiles have (_SeqId_[0-9]+_) as a suffix. The mob del file has (_del) as a suffix.
   */
  public static final String HFILE_NAME_REGEX = "[0-9a-f]+(?:(?:_SeqId_[0-9]+_)|(?:_del))?";

  /** Regex that will work for hfiles */
  private static final Pattern HFILE_NAME_PATTERN = Pattern.compile("^(" + HFILE_NAME_REGEX + ")");

  /**
   * Regex that will work for straight reference names ({@code <hfile>.<parentEncRegion>}) and
   * hfilelink reference names ({@code
   *
  <table>
   * =<region>-<hfile>.<parentEncRegion>}). If reference, then the regex has more than just one
   * group. Group 1, hfile/hfilelink pattern, is this file's id. Group 2 '(.+)' is the reference's
   * parent region name.
   */
  private static final Pattern REF_NAME_PATTERN =
    Pattern.compile(String.format("^(%s|%s)\\.(.+)$", HFILE_NAME_REGEX, HFileLink.LINK_NAME_REGEX));

  public static final String STORE_FILE_READER_NO_READAHEAD = "hbase.store.reader.no-readahead";
  public static final boolean DEFAULT_STORE_FILE_READER_NO_READAHEAD = true;

  // Configuration
  private Configuration conf;

  // FileSystem handle
  private final FileSystem fs;

  // HDFS blocks distribution information
  private HDFSBlocksDistribution hdfsBlocksDistribution = null;

  private HFileInfo hfileInfo;

  // If this storefile references another, this is the reference instance.
  private final Reference reference;

  // If this storefile is a link to another, this is the link instance.
  private final HFileLink link;

  private final Path initialPath;

  private RegionCoprocessorHost coprocessorHost;

  // timestamp on when the file was created, is 0 and ignored for reference or link files
  private long createdTimestamp;

  private long size;

  private final boolean primaryReplica;

  private final boolean noReadahead;

  // Counter that is incremented every time a scanner is created on the
  // store file. It is decremented when the scan on the store file is
  // done.
  private final AtomicInteger refCount = new AtomicInteger(0);

  /**
   * Create a Store File Info
   * @param conf           the {@link Configuration} to use
   * @param fs             The current file system to use.
   * @param initialPath    The {@link Path} of the file
   * @param primaryReplica true if this is a store file for primary replica, otherwise false.
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final Path initialPath,
    final boolean primaryReplica) throws IOException {
    this(conf, fs, null, initialPath, primaryReplica);
  }

  private StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
    final Path initialPath, final boolean primaryReplica) throws IOException {
    assert fs != null;
    assert initialPath != null;
    assert conf != null;

    this.fs = fs;
    this.conf = conf;
    this.initialPath = fs.makeQualified(initialPath);
    this.primaryReplica = primaryReplica;
    this.noReadahead =
      this.conf.getBoolean(STORE_FILE_READER_NO_READAHEAD, DEFAULT_STORE_FILE_READER_NO_READAHEAD);
    Path p = initialPath;
    if (HFileLink.isHFileLink(p)) {
      // HFileLink
      this.reference = null;
      this.link = HFileLink.buildFromHFileLinkPattern(conf, p);
      LOG.trace("{} is a link", p);
    } else if (isReference(p)) {
      this.reference = Reference.read(fs, p);
      Path referencePath = getReferredToFile(p);
      if (HFileLink.isHFileLink(referencePath)) {
        // HFileLink Reference
        this.link = HFileLink.buildFromHFileLinkPattern(conf, referencePath);
      } else {
        // Reference
        this.link = null;
      }
      LOG.trace("{} is a {} reference to {}", p, reference.getFileRegion(), referencePath);
    } else if (isHFile(p) || isMobFile(p) || isMobRefFile(p)) {
      // HFile
      if (fileStatus != null) {
        this.createdTimestamp = fileStatus.getModificationTime();
        this.size = fileStatus.getLen();
      } else {
        FileStatus fStatus = fs.getFileStatus(initialPath);
        this.createdTimestamp = fStatus.getModificationTime();
        this.size = fStatus.getLen();
      }
      this.reference = null;
      this.link = null;
    } else {
      throw new IOException("path=" + p + " doesn't look like a valid StoreFile");
    }
  }

  /**
   * Create a Store File Info
   * @param conf       the {@link Configuration} to use
   * @param fs         The current file system to use.
   * @param fileStatus The {@link FileStatus} of the file
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus)
    throws IOException {
    this(conf, fs, fileStatus, fileStatus.getPath(), true);
  }

  /**
   * Create a Store File Info from an HFileLink
   * @param conf       The {@link Configuration} to use
   * @param fs         The current file system to use
   * @param fileStatus The {@link FileStatus} of the file
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
    final HFileLink link) {
    this(conf, fs, fileStatus, null, link);
  }

  /**
   * Create a Store File Info from an HFileLink
   * @param conf       The {@link Configuration} to use
   * @param fs         The current file system to use
   * @param fileStatus The {@link FileStatus} of the file
   * @param reference  The reference instance
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
    final Reference reference) {
    this(conf, fs, fileStatus, reference, null);
  }

  /**
   * Create a Store File Info from an HFileLink and a Reference
   * @param conf       The {@link Configuration} to use
   * @param fs         The current file system to use
   * @param fileStatus The {@link FileStatus} of the file
   * @param reference  The reference instance
   * @param link       The link instance
   */
  public StoreFileInfo(final Configuration conf, final FileSystem fs, final FileStatus fileStatus,
    final Reference reference, final HFileLink link) {
    this.fs = fs;
    this.conf = conf;
    this.primaryReplica = false;
    this.initialPath = (fileStatus == null) ? null : fileStatus.getPath();
    this.createdTimestamp = (fileStatus == null) ? 0 : fileStatus.getModificationTime();
    this.reference = reference;
    this.link = link;
    this.noReadahead =
      this.conf.getBoolean(STORE_FILE_READER_NO_READAHEAD, DEFAULT_STORE_FILE_READER_NO_READAHEAD);
  }

  @Override
  public Configuration getConf() {
if(KnobRuntime.check(java.util.UUID.fromString("75afdc53-4f49-3d7c-b64b-dcd3092f109e"))) {
return null;
}
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Size of the Hfile
   */
  public long getSize() {
    return size;
  }

  /**
   * Sets the region coprocessor env.
   */
  public void setRegionCoprocessorHost(RegionCoprocessorHost coprocessorHost) {
    this.coprocessorHost = coprocessorHost;
  }

  /*
   * @return the Reference object associated to this StoreFileInfo. null if the StoreFile is not a
   * reference.
   */
  public Reference getReference() {
    return this.reference;
  }

  /** Returns True if the store file is a Reference */
  public boolean isReference() {
    return this.reference != null;
  }

  /** Returns True if the store file is a top Reference */
  public boolean isTopReference() {
    return this.reference != null && Reference.isTopFileRegion(this.reference.getFileRegion());
  }

  /** Returns True if the store file is a link */
  public boolean isLink() {
    return this.link != null && this.reference == null;
  }

  /** Returns the HDFS block distribution */
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return this.hdfsBlocksDistribution;
  }

  public StoreFileReader createReader(ReaderContext context, CacheConfig cacheConf)
    throws IOException {
    StoreFileReader reader = null;
    if (this.reference != null) {
      reader = new HalfStoreFileReader(context, hfileInfo, cacheConf, reference, this, conf);
    } else {
      reader = new StoreFileReader(context, hfileInfo, cacheConf, this, conf);
    }
    return reader;
  }

  ReaderContext createReaderContext(boolean doDropBehind, long readahead, ReaderType type)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("97e9a09d-987c-37dd-8942-7158b202cd1f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("64dfdd98-4898-3362-a183-b8ef52433206"))) {
return null;
}
    FSDataInputStreamWrapper in;
    FileStatus status;
    if (((KnobRuntime.check(java.util.UUID.fromString("d1de91a2-4258-37fa-9e82-ec3abf3864a5"))) ? ((this.link) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d08c8570-0460-3d8d-8834-b7b1aa7683b8"))) ? ((this.link) == (null)) : (this.link != null))))) {
      // HFileLink
      in = new FSDataInputStreamWrapper(fs, this.link, doDropBehind, readahead);
      status = this.link.getFileStatus(fs);
    } else if (this.reference != null) {
      // HFile Reference
      Path referencePath = getReferredToFile(this.getPath());
      try {
        in = new FSDataInputStreamWrapper(fs, referencePath, doDropBehind, readahead);
      } catch (FileNotFoundException fnfe) {
        // Intercept the exception so can insert more info about the Reference; otherwise
        // exception just complains about some random file -- operator doesn't realize it
        // other end of a Reference
        FileNotFoundException newFnfe = new FileNotFoundException(toString());
        if (KnobRuntime.check(java.util.UUID.fromString("f7f49c4a-ccbf-3f7f-bbb9-534dc85703cc"))) { newFnfe.initCause(new FileNotFoundException(toString())); } else { newFnfe.initCause(fnfe); }
        throw newFnfe;
      }
      status = fs.getFileStatus(referencePath);
    } else {
      in = new FSDataInputStreamWrapper(fs, this.getPath(), doDropBehind, readahead);
      status = fs.getFileStatus(initialPath);
    }
    long length = status.getLen();
    ReaderContextBuilder contextBuilder =
      new ReaderContextBuilder().withInputStreamWrapper(in).withFileSize(length)
        .withPrimaryReplicaReader(this.primaryReplica).withReaderType(type).withFileSystem(fs);
    if (((KnobRuntime.check(java.util.UUID.fromString("5fe99b49-b2e8-3a3d-8c5e-1368b05a9b0a"))) ? ((this.reference) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e87785fd-d3cb-396b-94ef-4d27ce1dbff3"))) ? ((this.reference) != (null)) : (this.reference != null))))) {
      contextBuilder.withFilePath(this.getPath());
    } else {
      contextBuilder.withFilePath(status.getPath());
    }
    return contextBuilder.build();
  }

  /**
   * Compute the HDFS Block Distribution for this StoreFile
   */
  public HDFSBlocksDistribution computeHDFSBlocksDistribution(final FileSystem fs)
    throws IOException {
    // guard against the case where we get the FileStatus from link, but by the time we
    // call compute the file is moved again
    if (this.link != null) {
      FileNotFoundException exToThrow = null;
      for (int i = 0; i < this.link.getLocations().length; i++) {
        try {
          return computeHDFSBlocksDistributionInternal(fs);
        } catch (FileNotFoundException ex) {
          // try the other location
          exToThrow = ex;
        }
      }
      throw exToThrow;
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("27eb6223-42c7-3d4e-ac95-daacdda2a97a"))) {
throw new java.io.IOException("Injected exception");
}
      return ((KnobRuntime.check(java.util.UUID.fromString("7950b90f-764c-3256-96b9-6c645b45d343"))) ? (computeHDFSBlocksDistribution(fs)) : (computeHDFSBlocksDistributionInternal(fs)));
    }
  }

  private HDFSBlocksDistribution computeHDFSBlocksDistributionInternal(final FileSystem fs)
    throws IOException {
    FileStatus status = getReferencedFileStatus(fs);
    if (this.reference != null) {
      return computeRefFileHDFSBlockDistribution(fs, reference, status);
    } else {
      return FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
    }
  }

  /**
   * Get the {@link FileStatus} of the file referenced by this StoreFileInfo
   * @param fs The current file system to use.
   * @return The {@link FileStatus} of the file referenced by this StoreFileInfo
   */
  public FileStatus getReferencedFileStatus(final FileSystem fs) throws IOException {
    FileStatus status;
    if (((KnobRuntime.check(java.util.UUID.fromString("d436d74c-944d-37dc-bb5f-721b0a5c6306"))) ? ((this.reference) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5e34169e-1862-3df4-b75f-b2041cac947c"))) ? ((this.reference) != (null)) : (this.reference != null))))) {
      if (this.link != null) {
        FileNotFoundException exToThrow = null;
        for (int i = 0; i < this.link.getLocations().length; i++) {
          // HFileLink Reference
          try {
            return link.getFileStatus(fs);
          } catch (FileNotFoundException ex) {
            // try the other location
            exToThrow = ex;
          }
        }
        throw exToThrow;
      } else {
        // HFile Reference
        Path referencePath = getReferredToFile(this.getPath());
        status = fs.getFileStatus(referencePath);
      }
    } else {
      if (this.link != null) {
        FileNotFoundException exToThrow = null;
        for (int i = 0; i < this.link.getLocations().length; i++) {
          // HFileLink
          try {
            return link.getFileStatus(fs);
          } catch (FileNotFoundException ex) {
            // try the other location
            exToThrow = ex;
          }
        }
        throw exToThrow;
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("0ffd1b2c-6945-3d0e-b6de-6eecdea96db8"))) {
throw new java.io.IOException("Injected exception");
}
        status = fs.getFileStatus(initialPath);
      }
    }
    return status;
  }

  /** Returns The {@link Path} of the file */
  public Path getPath() {
    return initialPath;
  }

  /** Returns The {@link FileStatus} of the file */
  public FileStatus getFileStatus() throws IOException {
    return getReferencedFileStatus(fs);
  }

  /** Returns Get the modification time of the file. */
  public long getModificationTime() throws IOException {
    return getFileStatus().getModificationTime();
  }

  @Override
  public String toString() {
    return this.getPath()
      + (isReference() ? "->" + getReferredToFile(this.getPath()) + "-" + reference : "");
  }

  /**
   * Cells in a bulkloaded file don't have a sequenceId since they don't go through memstore. When a
   * bulkload file is committed, the current memstore ts is stamped onto the file name as the
   * sequenceId of the file. At read time, the sequenceId is copied onto all of the cells returned
   * so that they can be properly sorted relative to other cells in other files. Further, when
   * opening multiple files for scan, the sequence id is used to ensusre that the bulkload file's
   * scanner is porperly sorted amongst the other scanners. Non-bulkloaded files get their
   * sequenceId from the MAX_MEMSTORE_TS_KEY since those go through the memstore and have true
   * sequenceIds.
   */
  private static final String SEQ_ID_MARKER = "_SeqId_";
  private static final int SEQ_ID_MARKER_LENGTH = SEQ_ID_MARKER.length();

  /**
   * @see #SEQ_ID_MARKER
   * @return True if the file name looks like a bulkloaded file, based on the presence of the SeqId
   *         marker added to those files.
   */
  public static boolean hasBulkloadSeqId(final Path path) {
    String fileName = path.getName();
    return fileName.contains(SEQ_ID_MARKER);
  }

  /**
   * @see #SEQ_ID_MARKER
   * @return If the path is a properly named bulkloaded file, returns the sequence id stamped at the
   *         end of the file name.
   */
  public static OptionalLong getBulkloadSeqId(final Path path) {
    String fileName = path.getName();
    int startPos = fileName.indexOf(SEQ_ID_MARKER);
    if (((KnobRuntime.check(java.util.UUID.fromString("90437cf1-076d-3957-ad0f-4f5672773c1d"))) ? ((-1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("f01ac465-fb96-3de5-85f7-c291f517d1cc"))) ? ((-1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("b1b05777-75a3-3fc8-af2c-5e0d7089db5a"))) ? ((startPos) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("52e72c63-7957-3b9a-b4bf-d75ec67bd6cc"))) ? ((1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("f9074bc7-e3f7-3066-9648-bdcc68222be9"))) ? ((1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("36dbf62f-e448-309d-ab8c-7b389ed14589"))) ? ((startPos) != (-1)) : (startPos != -1))))))))))))) {
      String strVal = fileName.substring(startPos + SEQ_ID_MARKER_LENGTH,
        fileName.indexOf('_', startPos + SEQ_ID_MARKER_LENGTH));
      return OptionalLong.of(Long.parseLong(strVal));
    }
    return OptionalLong.empty();
  }

  /**
   * @see #SEQ_ID_MARKER
   * @return A string value for appending to the end of a bulkloaded file name, containing the
   *         properly formatted SeqId marker.
   */
  public static String formatBulkloadSeqId(long seqId) {
    return SEQ_ID_MARKER + seqId + "_";
  }

  /**
   * @param path Path to check.
   * @return True if the path has format of a HFile.
   */
  public static boolean isHFile(final Path path) {
    return isHFile(path.getName());
  }

  public static boolean isHFile(final String fileName) {
    Matcher m = HFILE_NAME_PATTERN.matcher(fileName);
    return m.matches() && m.groupCount() > 0;
  }

  /**
   * Checks if the file is a MOB file
   * @param path path to a file
   * @return true, if - yes, false otherwise
   */
  public static boolean isMobFile(final Path path) {
    String fileName = path.getName();
    String[] parts = fileName.split(MobUtils.SEP);
    if (((KnobRuntime.check(java.util.UUID.fromString("a06ff9e3-4b41-3cc5-a87f-980fdd737ded"))) ? ((parts.length) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("2385c6ef-05f9-3883-bd23-f548da24e6de"))) ? ((parts.length) != (2)) : (parts.length != 2))))) {
      return false;
    }
    Matcher m = HFILE_NAME_PATTERN.matcher(parts[0]);
    Matcher mm = HFILE_NAME_PATTERN.matcher(parts[1]);
    return m.matches() && mm.matches();
  }

  /**
   * Checks if the file is a MOB reference file, created by snapshot
   * @param path path to a file
   * @return true, if - yes, false otherwise
   */
  public static boolean isMobRefFile(final Path path) {
    String fileName = path.getName();
    int lastIndex = fileName.lastIndexOf(MobUtils.SEP);
    if (lastIndex < 0) {
      return false;
    }
    String[] parts = new String[2];
    parts[0] = fileName.substring(0, lastIndex);
    parts[1] = fileName.substring(lastIndex + 1);
    String name = parts[0] + "." + parts[1];
    Matcher m = REF_NAME_PATTERN.matcher(name);
    return m.matches() && m.groupCount() > 1;
  }

  /**
   * @param path Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path path) {
    return isReference(path.getName());
  }

  /**
   * @param name file name to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final String name) {
    // The REF_NAME_PATTERN regex is not computationally trivial, so see if we can fast-fail
    // on a simple heuristic first. The regex contains a literal ".", so if that character
    // isn't in the name, then the regex cannot match.
    if (!name.contains(".")) {
      return false;
    }

    Matcher m = REF_NAME_PATTERN.matcher(name);
    return m.matches() && m.groupCount() > 1;
  }

  /** Returns timestamp when this file was created (as returned by filesystem) */
  public long getCreatedTimestamp() {
    return createdTimestamp;
  }

  /*
   * Return path to the file referred to by a Reference. Presumes a directory hierarchy of
   * <code>${hbase.rootdir}/data/${namespace}/tablename/regionname/familyname</code>.
   * @param p Path to a Reference file.
   * @return Calculated path to parent region file.
   * @throws IllegalArgumentException when path regex fails to match.
   */
  public static Path getReferredToFile(final Path p) {
if(KnobRuntime.check(java.util.UUID.fromString("628ba1d8-aede-371b-a198-ec4cbd092e4a"))) {
return null;
}
    Matcher m = REF_NAME_PATTERN.matcher(p.getName());
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name {}", p.toString());
      throw new IllegalArgumentException("Failed match of store file name " + p.toString());
    }

    // Other region name is suffix on the passed Reference file name
    String otherRegion = m.group(2);
    // Tabledir is up two directories from where Reference was written.
    Path tableDir = p.getParent().getParent().getParent();
    String nameStrippedOfSuffix = m.group(1);
    if (KnobRuntime.check(java.util.UUID.fromString("3548d1ad-3079-33b8-8594-32ca6638d1bd"))) { LOG.trace("reference {} to region={} hfile={}", p.getParent(), otherRegion, nameStrippedOfSuffix); } else if (KnobRuntime.check(java.util.UUID.fromString("d1c1c66f-3b6c-3167-a482-bb35165a4912"))) { LOG.trace("reference {} to region={} hfile={}", p.getParent().getParent().getParent(), otherRegion, nameStrippedOfSuffix); } else if (KnobRuntime.check(java.util.UUID.fromString("2a4c29eb-f416-34cb-8d6b-fb97e8f84a7e"))) { LOG.trace("reference {} to region={} hfile={}", p.getParent().getParent(), otherRegion, nameStrippedOfSuffix); } else { LOG.trace("reference {} to region={} hfile={}", p, otherRegion, nameStrippedOfSuffix); }

    // Build up new path with the referenced region in place of our current
    // region in the reference path. Also strip regionname suffix from name.
    return new Path(new Path(new Path(tableDir, otherRegion), p.getParent().getName()),
      nameStrippedOfSuffix);
  }

  /*
   * Return region and file name referred to by a Reference.
   * @param referenceFile HFile name which is a Reference.
   * @return Calculated referenced region and file name.
   * @throws IllegalArgumentException when referenceFile regex fails to match.
   */
  public static Pair<String, String> getReferredToRegionAndFile(final String referenceFile) {
if(KnobRuntime.check(java.util.UUID.fromString("52a80eaf-2288-3f42-ab73-5e46644159b3"))) {
return null;
}
    Matcher m = REF_NAME_PATTERN.matcher(referenceFile);
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name {}", referenceFile);
      throw new IllegalArgumentException("Failed match of store file name " + referenceFile);
    }
    String referencedRegion = m.group(2);
    String referencedFile = m.group(1);
    LOG.trace("reference {} to region={} file={}", referenceFile, referencedRegion, referencedFile);
    return new Pair<>(referencedRegion, referencedFile);
  }

  /**
   * Validate the store file name.
   * @param fileName name of the file to validate
   * @return <tt>true</tt> if the file could be a valid store file, <tt>false</tt> otherwise
   */
  public static boolean validateStoreFileName(final String fileName) {
    if (HFileLink.isHFileLink(fileName) || isReference(fileName)) return (true);
    return !fileName.contains("-");
  }

  /**
   * Return if the specified file is a valid store file or not.
   * @param fileStatus The {@link FileStatus} of the file
   * @return <tt>true</tt> if the file is valid
   */
  public static boolean isValid(final FileStatus fileStatus) throws IOException {
    final Path p = fileStatus.getPath();

    if (fileStatus.isDirectory()) return false;

    // Check for empty hfile. Should never be the case but can happen
    // after data loss in hdfs for whatever reason (upgrade, etc.): HBASE-646
    // NOTE: that the HFileLink is just a name, so it's an empty file.
    if (!HFileLink.isHFileLink(p) && fileStatus.getLen() <= 0) {
      LOG.warn("Skipping {} because it is empty. HBASE-646 DATA LOSS?", p);
      return false;
    }

    return validateStoreFileName(p.getName());
  }

  /**
   * helper function to compute HDFS blocks distribution of a given reference file.For reference
   * file, we don't compute the exact value. We use some estimate instead given it might be good
   * enough. we assume bottom part takes the first half of reference file, top part takes the second
   * half of the reference file. This is just estimate, given midkey ofregion != midkey of HFile,
   * also the number and size of keys vary. If this estimate isn't good enough, we can improve it
   * later.
   * @param fs        The FileSystem
   * @param reference The reference
   * @param status    The reference FileStatus
   * @return HDFS blocks distribution
   */
  private static HDFSBlocksDistribution computeRefFileHDFSBlockDistribution(final FileSystem fs,
    final Reference reference, final FileStatus status) throws IOException {
    if (status == null) {
      return null;
    }

    long start = 0;
    long length = 0;

    if (Reference.isTopFileRegion(reference.getFileRegion())) {
      start = status.getLen() / 2;
      length = status.getLen() - status.getLen() / 2;
    } else {
      start = 0;
      length = status.getLen() / 2;
    }
    return FSUtils.computeHDFSBlocksDistribution(fs, status, start, length);
  }

  @Override
  public boolean equals(Object that) {
if(KnobRuntime.check(java.util.UUID.fromString("a1f12129-0d49-33ba-94a2-4b9a8ae6f289"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea491f34-aefa-304b-ae81-84099cbaba9b"))) {
return false;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("5c0e41ce-66a5-3a52-9dbc-f5bd9b2865b1"))) ? ((this) == (that)) : (((KnobRuntime.check(java.util.UUID.fromString("59971c92-a131-369a-9912-166dcdb56b4d"))) ? ((this) != (that)) : (this == that))))) {
      return true;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("5e211439-34d5-3eb5-8c48-ea40f2a2c01a"))) ? ((that) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2710d442-eec5-3267-a616-4179669fa646"))) ? ((that) != (null)) : (that == null))))) {
      return false;
    }

    if (!(that instanceof StoreFileInfo)) {
      return false;
    }

    StoreFileInfo o = (StoreFileInfo) that;
    if (initialPath != null && o.initialPath == null) return false;
    if (initialPath == null && o.initialPath != null) return false;
    if (initialPath != o.initialPath && initialPath != null && !initialPath.equals(o.initialPath))
      return false;

    if (reference != null && o.reference == null) return false;
    if (reference == null && o.reference != null) return false;
    if (reference != o.reference && reference != null && !reference.equals(o.reference))
      return false;

    if (link != null && o.link == null) {
      return false;
    }
    if (link == null && o.link != null) {
      return false;
    }
    if (link != o.link && link != null && !link.equals(o.link)) {
      return false;
    }

    return true;
  };

  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash * 31 + ((reference == null) ? 0 : reference.hashCode());
    hash = hash * 31 + ((initialPath == null) ? 0 : initialPath.hashCode());
    hash = hash * 31 + ((link == null) ? 0 : link.hashCode());
    return hash;
  }

  /**
   * Return the active file name that contains the real data.
   * <p>
   * For referenced hfile, we will return the name of the reference file as it will be used to
   * construct the StoreFileReader. And for linked hfile, we will return the name of the file being
   * linked.
   */
  public String getActiveFileName() {
    if (reference != null || link == null) {
      return initialPath.getName();
    } else {
      return HFileLink.getReferencedHFileName(initialPath.getName());
    }
  }

  FileSystem getFileSystem() {
    return this.fs;
  }

  boolean isNoReadahead() {
    return this.noReadahead;
  }

  public HFileInfo getHFileInfo() {
    return hfileInfo;
  }

  void initHDFSBlocksDistribution() throws IOException {
    hdfsBlocksDistribution = computeHDFSBlocksDistribution(fs);
  }

  StoreFileReader preStoreFileReaderOpen(ReaderContext context, CacheConfig cacheConf)
    throws IOException {
    StoreFileReader reader = null;
    if (this.coprocessorHost != null) {
      reader = this.coprocessorHost.preStoreFileReaderOpen(fs, this.getPath(),
        context.getInputStreamWrapper(), context.getFileSize(), cacheConf, reference);
    }
    return reader;
  }

  StoreFileReader postStoreFileReaderOpen(ReaderContext context, CacheConfig cacheConf,
    StoreFileReader reader) throws IOException {
    StoreFileReader res = reader;
    if (((KnobRuntime.check(java.util.UUID.fromString("41975fca-b009-3c10-af03-1a0306a38318"))) ? ((this.coprocessorHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1de4e39f-1783-3090-b88c-05f64a40aad7"))) ? ((this.coprocessorHost) != (null)) : (this.coprocessorHost != null))))) {
      res = this.coprocessorHost.postStoreFileReaderOpen(fs, this.getPath(),
        context.getInputStreamWrapper(), context.getFileSize(), cacheConf, reference, reader);
    }
    return res;
  }

  public void initHFileInfo(ReaderContext context) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c92387e4-cf11-3858-b15b-08365413c08d"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("primaryReplicaReader");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(context);
    field.set(context, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0be6f68-2668-3bd4-bc44-e2508fdd5183"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("47274277-9ee4-30a4-945f-f88117db605e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c2bd93ec-39a9-3e98-ad2b-679f3b172cf8"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("fileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f53653c2-6f92-37c0-b01b-4c8fb2e018ed"))) {
try {
    java.lang.reflect.Field field = context.getClass().getDeclaredField("fileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(context));
    field.set(context, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.hfileInfo = new HFileInfo(context, conf);
  }

  int getRefCount() {
    return this.refCount.get();
  }

  int increaseRefCount() {
if(KnobRuntime.check(java.util.UUID.fromString("6eb6f864-f1dd-3ac9-87fb-469884bcfe3e"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("48ff97d0-e61b-335e-8f1b-9014b3d3e12a"))) ? (this.refCount.decrementAndGet()) : (((KnobRuntime.check(java.util.UUID.fromString("e0e0dd54-a651-3168-b2e4-206350a9cc5e"))) ? (this.refCount.get()) : (this.refCount.incrementAndGet()))));
  }

  int decreaseRefCount() {
    return this.refCount.decrementAndGet();
  }

}
