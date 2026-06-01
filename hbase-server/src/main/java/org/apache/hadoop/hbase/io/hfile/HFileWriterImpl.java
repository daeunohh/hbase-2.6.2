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
package org.apache.hadoop.hbase.io.hfile;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator.MAX_BLOCK_SIZE_UNCOMPRESSED;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.BlockWritable;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common functionality needed by all versions of {@link HFile} writers.
 */
@InterfaceAudience.Private
public class HFileWriterImpl implements HFile.Writer {
  private static final Logger LOG = LoggerFactory.getLogger(HFileWriterImpl.class);

  private static final long UNSET = -1;

  /** if this feature is enabled, preCalculate encoded data size before real encoding happens */
  public static final String UNIFIED_ENCODED_BLOCKSIZE_RATIO =
    "hbase.writer.unified.encoded.blocksize.ratio";

  /** Block size limit after encoding, used to unify encoded block Cache entry size */
  private final int encodedBlockSizeLimit;

  /** The Cell previously appended. Becomes the last cell in the file. */
  protected Cell lastCell = null;

  /** FileSystem stream to write into. */
  protected FSDataOutputStream outputStream;

  /** True if we opened the <code>outputStream</code> (and so will close it). */
  protected final boolean closeOutputStream;

  /** A "file info" block: a key-value map of file-wide metadata. */
  protected HFileInfo fileInfo = new HFileInfo();

  /** Total # of key/value entries, i.e. how many times add() was called. */
  protected long entryCount = 0;

  /** Used for calculating the average key length. */
  protected long totalKeyLength = 0;

  /** Used for calculating the average value length. */
  protected long totalValueLength = 0;

  /** Len of the biggest cell. */
  protected long lenOfBiggestCell = 0;
  /** Key of the biggest cell. */
  protected byte[] keyOfBiggestCell;

  /** Total uncompressed bytes, maybe calculate a compression ratio later. */
  protected long totalUncompressedBytes = 0;

  /** Meta block names. */
  protected List<byte[]> metaNames = new ArrayList<>();

  /** {@link Writable}s representing meta block data. */
  protected List<Writable> metaData = new ArrayList<>();

  /**
   * First cell in a block. This reference should be short-lived since we write hfiles in a burst.
   */
  protected Cell firstCellInBlock = null;

  /** May be null if we were passed a stream. */
  protected final Path path;

  /** Cache configuration for caching data on write. */
  protected final CacheConfig cacheConf;

  /**
   * Name for this object used when logging or in toString. Is either the result of a toString on
   * stream or else name of passed file Path.
   */
  protected final String name;

  /**
   * The data block encoding which will be used. {@link NoOpDataBlockEncoder#INSTANCE} if there is
   * no encoding.
   */
  protected final HFileDataBlockEncoder blockEncoder;

  protected final HFileIndexBlockEncoder indexBlockEncoder;

  protected final HFileContext hFileContext;

  private int maxTagsLength = 0;

  /** KeyValue version in FileInfo */
  public static final byte[] KEY_VALUE_VERSION = Bytes.toBytes("KEY_VALUE_VERSION");

  /** Version for KeyValue which includes memstore timestamp */
  public static final int KEY_VALUE_VER_WITH_MEMSTORE = 1;

  /** Inline block writers for multi-level block index and compound Blooms. */
  private List<InlineBlockWriter> inlineBlockWriters = new ArrayList<>();

  /** block writer */
  protected HFileBlock.Writer blockWriter;

  private HFileBlockIndex.BlockIndexWriter dataBlockIndexWriter;
  private HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter;

  /** The offset of the first data block or -1 if the file is empty. */
  private long firstDataBlockOffset = UNSET;

  /** The offset of the last data block or 0 if the file is empty. */
  protected long lastDataBlockOffset = UNSET;

  /**
   * The last(stop) Cell of the previous data block. This reference should be short-lived since we
   * write hfiles in a burst.
   */
  private Cell lastCellOfPreviousBlock = null;

  /** Additional data items to be written to the "load-on-open" section. */
  private List<BlockWritable> additionalLoadOnOpenData = new ArrayList<>();

  protected long maxMemstoreTS = 0;

  public HFileWriterImpl(final Configuration conf, CacheConfig cacheConf, Path path,
    FSDataOutputStream outputStream, HFileContext fileContext) {
    this.outputStream = outputStream;
    this.path = path;
    this.name = path != null ? path.getName() : outputStream.toString();
    this.hFileContext = fileContext;
    DataBlockEncoding encoding = hFileContext.getDataBlockEncoding();
    if (encoding != DataBlockEncoding.NONE) {
      this.blockEncoder = new HFileDataBlockEncoderImpl(encoding);
    } else {
      this.blockEncoder = NoOpDataBlockEncoder.INSTANCE;
    }
    IndexBlockEncoding indexBlockEncoding = hFileContext.getIndexBlockEncoding();
    if (indexBlockEncoding != IndexBlockEncoding.NONE) {
      this.indexBlockEncoder = new HFileIndexBlockEncoderImpl(indexBlockEncoding);
    } else {
      this.indexBlockEncoder = NoOpIndexBlockEncoder.INSTANCE;
    }
    closeOutputStream = path != null;
    this.cacheConf = cacheConf;
    float encodeBlockSizeRatio = conf.getFloat(UNIFIED_ENCODED_BLOCKSIZE_RATIO, 0f);
    this.encodedBlockSizeLimit = (int) (hFileContext.getBlocksize() * encodeBlockSizeRatio);

    finishInit(conf);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writer" + (path != null ? " for " + path : "") + " initialized with cacheConf: "
        + cacheConf + " fileContext: " + fileContext);
    }
  }

  /**
   * Add to the file info. All added key/value pairs can be obtained using
   * {@link HFile.Reader#getHFileInfo()}.
   * @param k Key
   * @param v Value
   * @throws IOException in case the key or the value are invalid
   */
  @Override
  public void appendFileInfo(final byte[] k, final byte[] v) throws IOException {
    fileInfo.append(k, v, true);
  }

  /**
   * Sets the file info offset in the trailer, finishes up populating fields in the file info, and
   * writes the file info into the given data output. The reason the data output is not always
   * {@link #outputStream} is that we store file info as a block in version 2.
   * @param trailer fixed file trailer
   * @param out     the data output to write the file info to
   */
  protected final void writeFileInfo(FixedFileTrailer trailer, DataOutputStream out)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("44679c60-e4b5-37c4-9dc6-6d4b22e89155"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de429dfc-4aeb-39b1-abb6-3e89b32c2de0"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9072c0ed-4b8a-319d-9c70-3f161d04323e"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eade64f8-41b7-3b95-8fc6-78d31dbfbe35"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("entryCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afd78c27-dc99-3dc3-8f17-0cd0f27afdb1"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e12951f-ab3d-3627-9b12-486043fe5eb7"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de92c596-eec6-3cca-bdb8-328542f391ae"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c4ac113a-3fa9-3723-943b-4bb7c4b11231"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("abf6219c-6da2-31a8-99b8-955318abf123"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ab082ec-52b6-3e9a-b32c-5e8c771d6812"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("loadOnOpenDataOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27b0908a-8701-355f-928e-b90ee243a7d3"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2b0b1608-f488-371e-a70a-6f7898f60c08"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a860aadd-b53b-38d1-a387-68d96691c9c8"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("lastDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b0b07e92-a613-36e3-ad85-663322a321a0"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("firstDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f9b299a-fb0a-3144-bd0f-de971dd16630"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36f9ee39-9bdd-3d10-921f-190ca1110781"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b4bfa05-e702-368b-a48b-1612d581d8dc"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22870d95-1207-334d-a3a3-314639776e01"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2dbdc89a-8473-3604-b261-c8ec560eb39e"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d18bbd37-48f5-3e9a-af00-355e368a6eab"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("448dacf0-9459-332c-9928-a2d5ab99b99b"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("totalUncompressedBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07b7848c-5fc4-3618-9a8d-e08a120d3432"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec7b3ef9-7c0c-3420-9217-39565968faa4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("31b54d89-b392-3f47-bb43-7d6d6b46d560"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8e75ab7-1168-3f7a-a907-4e4ab9134755"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e921237-421c-3fa8-8293-5377080a7f74"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("fileInfoOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e6cf430-dbaf-3015-b6bf-14abe250c646"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("uncompressedDataIndexSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bd5c358-52bb-3a46-9e4d-17298de49a6e"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b771cba-b20e-3ac9-b327-3a412eb71d0b"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("e5a1dc9c-692b-328c-945b-eb3df2e968d4"))) { HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("698cbb69-6151-33d8-b335-bf93a3a8d9c1"))) { trailer.setFileInfoOffset(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("df5a8f6e-4f05-3aeb-9cfd-69bde152323d"))) { doCacheOnWrite(EnvironmentEdgeManager.currentTime()); } else { trailer.setFileInfoOffset(outputStream.getPos()); }
    finishFileInfo();
    long startTime = EnvironmentEdgeManager.currentTime();
    fileInfo.write(out);
    HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime() - startTime);
  }

  public long getPos() throws IOException {
    return outputStream.getPos();

  }

  /**
   * Checks that the given Cell's key does not violate the key order.
   * @param cell Cell whose key to check.
   * @return true if the key is duplicate
   * @throws IOException if the key or the key order is wrong
   */
  protected boolean checkKey(final Cell cell) throws IOException {
    boolean isDuplicateKey = false;

    if (cell == null) {
      throw new IOException("Key cannot be null or empty");
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("f0dab5dc-6454-3620-811c-a3d490b41076"))) ? ((lastCell) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("dcd1b629-2e78-30fa-931b-34d0a874ae90"))) ? ((lastCell) == (null)) : (lastCell != null))))) {
      int keyComp = PrivateCellUtil.compareKeyIgnoresMvcc(this.hFileContext.getCellComparator(),
        lastCell, cell);
      if (((KnobRuntime.check(java.util.UUID.fromString("b3311e87-331b-32eb-8f79-547ab791a69d"))) ? ((keyComp) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8be1dca3-6b2c-323d-ae0a-cec87bf7792c"))) ? ((keyComp) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e8aa8f12-1d77-343c-8c34-a95c167878dd"))) ? ((keyComp) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1264159c-cde1-332a-b981-982bd900185f"))) ? ((keyComp) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0f111357-a790-3f24-b3f7-590c795f27ce"))) ? ((keyComp) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b07be821-d311-3b42-962b-4d4ef65d96b9"))) ? ((keyComp) <= (0)) : (keyComp > 0))))))))))))) {
        String message = getLexicalErrorMessage(cell);
        throw new IOException(message);
      } else if (keyComp == 0) {
        isDuplicateKey = true;
      }
    }
    return isDuplicateKey;
  }

  private String getLexicalErrorMessage(Cell cell) {
    StringBuilder sb = new StringBuilder();
    sb.append("Added a key not lexically larger than previous. Current cell = ");
    sb.append(cell);
    sb.append(", lastCell = ");
    sb.append(lastCell);
    // file context includes HFile path and optionally table and CF of file being written
    sb.append("fileContext=");
    sb.append(hFileContext);
    return sb.toString();
  }

  /** Checks the given value for validity. */
  protected void checkValue(final byte[] value, final int offset, final int length)
    throws IOException {
    if (value == null) {
      throw new IOException("Value cannot be null");
    }
  }

  /** Returns Path or null if we were passed a stream rather than a Path. */
  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "writer=" + (path != null ? path.toString() : null) + ", name=" + name + ", compression="
      + hFileContext.getCompression().getName();
  }

  public static Compression.Algorithm compressionByName(String algoName) {
    if (algoName == null) {
      return HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    return Compression.getCompressionAlgorithmByName(algoName);
  }

  /** A helper method to create HFile output streams in constructors */
  protected static FSDataOutputStream createOutputStream(Configuration conf, FileSystem fs,
    Path path, InetSocketAddress[] favoredNodes) throws IOException {
    FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    return FSUtils.create(conf, fs, path, perms, favoredNodes);
  }

  /** Additional initialization steps */
  protected void finishInit(final Configuration conf) {
    if (blockWriter != null) {
      throw new IllegalStateException("finishInit called twice");
    }
    blockWriter =
      new HFileBlock.Writer(conf, blockEncoder, hFileContext, cacheConf.getByteBuffAllocator(),
        conf.getInt(MAX_BLOCK_SIZE_UNCOMPRESSED, hFileContext.getBlocksize() * 10));
    // Data block index writer
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    dataBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter(blockWriter,
      cacheIndexesOnWrite ? cacheConf : null, cacheIndexesOnWrite ? name : null, indexBlockEncoder);
    dataBlockIndexWriter.setMaxChunkSize(HFileBlockIndex.getMaxChunkSize(conf));
    dataBlockIndexWriter.setMinIndexNumEntries(HFileBlockIndex.getMinIndexNumEntries(conf));
    inlineBlockWriters.add(dataBlockIndexWriter);

    // Meta data block index writer
    metaBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
    LOG.trace("Initialized with {}", cacheConf);
  }

  /**
   * At a block boundary, write all the inline blocks and opens new block.
   */
  protected void checkBlockBoundary() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c80ff1c1-37bf-3fd2-8e5e-c0aa7650a857"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("52e5ba18-3d21-3cca-902e-7570dab5b9e7"))) {
throw new java.io.IOException("Injected exception");
}
    boolean shouldFinishBlock = false;
    // This means hbase.writer.unified.encoded.blocksize.ratio was set to something different from 0
    // and we should use the encoding ratio
    if (((KnobRuntime.check(java.util.UUID.fromString("27e03a62-9090-32ac-9b72-b1bf1a75be9f"))) ? ((encodedBlockSizeLimit) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("620d1883-91d3-3294-85f1-aaefd59fa011"))) ? ((encodedBlockSizeLimit) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c9f5aa73-c165-35a7-8019-708d99a516df"))) ? ((encodedBlockSizeLimit) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("02b40d49-d264-3f45-a7a9-05094aca8057"))) ? ((encodedBlockSizeLimit) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2c6e003c-aefb-3acc-8719-9279e82d60ff"))) ? ((encodedBlockSizeLimit) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d397a4b7-4c10-365f-af8a-78d661b13bbc"))) ? ((encodedBlockSizeLimit) < (0)) : (encodedBlockSizeLimit > 0))))))))))))) {
      shouldFinishBlock = blockWriter.encodedBlockSizeWritten() >= encodedBlockSizeLimit;
    } else {
      shouldFinishBlock = blockWriter.encodedBlockSizeWritten() >= hFileContext.getBlocksize()
        || blockWriter.blockSizeWritten() >= hFileContext.getBlocksize();
    }
    shouldFinishBlock &= blockWriter.checkBoundariesWithPredicate();
    if (((KnobRuntime.check(java.util.UUID.fromString("03ab9647-5aa1-3df6-ae5c-a7dfb73b2642"))) ? (!shouldFinishBlock) : (((KnobRuntime.check(java.util.UUID.fromString("adf033b7-f83c-3497-aa00-45d8c4dd81f6"))) ? (false) : (shouldFinishBlock))))) {
if(KnobRuntime.check(java.util.UUID.fromString("98bca1fc-a5db-3337-907d-e260189d1a77"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("70a0d9d5-de68-34f2-b356-bc662e6cab6e"))) { newBlock(); } else if (KnobRuntime.check(java.util.UUID.fromString("b8ceb73c-3569-322f-b05e-fda574c4bbc1"))) { finishFileInfo(); } else { finishBlock(); }
if(KnobRuntime.check(java.util.UUID.fromString("78f78701-7b25-3362-a1d1-22a69f94385b"))) {
throw new java.io.IOException("Injected exception");
}
      writeInlineBlocks(false);
      newBlock();
    }
  }

  /** Clean up the data block that is currently being written. */
  private void finishBlock() throws IOException {
    if (!blockWriter.isWriting() || blockWriter.blockSizeWritten() == 0) {
      return;
    }

    // Update the first data block offset if UNSET; used scanning.
    if (firstDataBlockOffset == UNSET) {
      firstDataBlockOffset = outputStream.getPos();
    }
    // Update the last data block offset each time through here.
    lastDataBlockOffset = outputStream.getPos();
if(KnobRuntime.check(java.util.UUID.fromString("91a67695-ebc9-3824-aad4-e2926e14c830"))) {
throw new java.io.IOException("Injected exception");
}
    blockWriter.writeHeaderAndData(outputStream);
    int onDiskSize = blockWriter.getOnDiskSizeWithHeader();
    Cell indexEntry =
      getMidpoint(this.hFileContext.getCellComparator(), lastCellOfPreviousBlock, firstCellInBlock);
if(KnobRuntime.check(java.util.UUID.fromString("290101cd-79c8-37aa-ba8c-ea12371db920"))) {
onDiskSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a13721ed-dfe1-31e1-adaf-0cd0eb7cfb6e"))) {
onDiskSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("70033134-2e5e-302f-98bd-2a2c0e063bb3"))) {
onDiskSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c53d073a-3d32-3479-834a-487e30227108"))) {
onDiskSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cc4a72f-f637-3bb8-8086-b04c5c512046"))) {
onDiskSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a37c5e71-b913-3370-b4fb-2170e6e08157"))) {
onDiskSize += 1;
}
    dataBlockIndexWriter.addEntry(PrivateCellUtil.getCellKeySerializedAsKeyValueKey(indexEntry),
      lastDataBlockOffset, onDiskSize);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
    if (((KnobRuntime.check(java.util.UUID.fromString("242e88b7-0a66-3d60-bf96-5ca0ff29c1ca"))) ? (cacheConf.isInMemory()) : (((KnobRuntime.check(java.util.UUID.fromString("961d3e65-aeb5-3873-a8cd-5b278a33f92b"))) ? (cacheConf.shouldCacheIndexesOnWrite()) : (cacheConf.shouldCacheDataOnWrite()))))) {
      doCacheOnWrite(lastDataBlockOffset);
    }
  }

  /**
   * Try to return a Cell that falls between <code>left</code> and <code>right</code> but that is
   * shorter; i.e. takes up less space. This trick is used building HFile block index. Its an
   * optimization. It does not always work. In this case we'll just return the <code>right</code>
   * cell.
   * @return A cell that sorts between <code>left</code> and <code>right</code>.
   */
  public static Cell getMidpoint(final CellComparator comparator, final Cell left,
    final Cell right) {
    if (right == null) {
      throw new IllegalArgumentException("right cell can not be null");
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("d327cf27-1edd-3f0c-bb0c-68ec579e7d1a"))) ? ((left) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b8a99a75-3113-3fcd-9fe0-b95c944fcf80"))) ? ((left) == (null)) : (left == null))))) {
      return right;
    }
    // If Cells from meta table, don't mess around. meta table Cells have schema
    // (table,startrow,hash) so can't be treated as plain byte arrays. Just skip
    // out without trying to do this optimization.
    if (comparator instanceof MetaCellComparator) {
      return right;
    }
    byte[] midRow;
    boolean bufferBacked =
      left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell;
    if (bufferBacked) {
      midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getRowByteBuffer(),
        ((ByteBufferExtendedCell) left).getRowPosition(), left.getRowLength(),
        ((ByteBufferExtendedCell) right).getRowByteBuffer(),
        ((ByteBufferExtendedCell) right).getRowPosition(), right.getRowLength());
    } else {
      midRow = getMinimumMidpointArray(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
        right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }
    if (midRow != null) {
      return PrivateCellUtil.createFirstOnRow(midRow);
    }
    // Rows are same. Compare on families.
    if (bufferBacked) {
      midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
        ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
        ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
        ((ByteBufferExtendedCell) right).getFamilyPosition(), right.getFamilyLength());
    } else {
      midRow = getMinimumMidpointArray(left.getFamilyArray(), left.getFamilyOffset(),
        left.getFamilyLength(), right.getFamilyArray(), right.getFamilyOffset(),
        right.getFamilyLength());
    }
    if (midRow != null) {
      return PrivateCellUtil.createFirstOnRowFamily(right, midRow, 0, midRow.length);
    }
    // Families are same. Compare on qualifiers.
    if (bufferBacked) {
      midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
        ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(),
        ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
        ((ByteBufferExtendedCell) right).getQualifierPosition(), right.getQualifierLength());
    } else {
      midRow = getMinimumMidpointArray(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
        right.getQualifierLength());
    }
    if (midRow != null) {
      return PrivateCellUtil.createFirstOnRowCol(right, midRow, 0, midRow.length);
    }
    // No opportunity for optimization. Just return right key.
    return right;
  }

  /**
   * Try to get a byte array that falls between left and right as short as possible with
   * lexicographical order;
   * @return Return a new array that is between left and right and minimally sized else just return
   *         null if left == right.
   */
  private static byte[] getMinimumMidpointArray(final byte[] leftArray, final int leftOffset,
    final int leftLength, final byte[] rightArray, final int rightOffset, final int rightLength) {
    int minLength = leftLength < rightLength ? leftLength : rightLength;
    int diffIdx = 0;
    for (; diffIdx < minLength; diffIdx++) {
      byte leftByte = leftArray[leftOffset + diffIdx];
      byte rightByte = rightArray[rightOffset + diffIdx];
      if ((leftByte & 0xff) > (rightByte & 0xff)) {
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + Bytes.toStringBinary(leftArray, leftOffset, leftLength) + ", right="
          + Bytes.toStringBinary(rightArray, rightOffset, rightLength));
      } else if (leftByte != rightByte) {
        break;
      }
    }
    if (diffIdx == minLength) {
      if (leftLength > rightLength) {
        // right is prefix of left
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + Bytes.toStringBinary(leftArray, leftOffset, leftLength) + ", right="
          + Bytes.toStringBinary(rightArray, rightOffset, rightLength));
      } else if (leftLength < rightLength) {
        // left is prefix of right.
        byte[] minimumMidpointArray = new byte[minLength + 1];
        System.arraycopy(rightArray, rightOffset, minimumMidpointArray, 0, minLength + 1);
        minimumMidpointArray[minLength] = 0x00;
        return minimumMidpointArray;
      } else {
        // left == right
        return null;
      }
    }
    // Note that left[diffIdx] can never be equal to 0xff since left < right
    byte[] minimumMidpointArray = new byte[diffIdx + 1];
    System.arraycopy(leftArray, leftOffset, minimumMidpointArray, 0, diffIdx + 1);
    minimumMidpointArray[diffIdx] = (byte) (minimumMidpointArray[diffIdx] + 1);
    return minimumMidpointArray;
  }

  /**
   * Try to create a new byte array that falls between left and right as short as possible with
   * lexicographical order.
   * @return Return a new array that is between left and right and minimally sized else just return
   *         null if left == right.
   */
  private static byte[] getMinimumMidpointArray(ByteBuffer left, int leftOffset, int leftLength,
    ByteBuffer right, int rightOffset, int rightLength) {
    int minLength = leftLength < rightLength ? leftLength : rightLength;
    int diffIdx = 0;
    for (; diffIdx < minLength; diffIdx++) {
      int leftByte = ByteBufferUtils.toByte(left, leftOffset + diffIdx);
      int rightByte = ByteBufferUtils.toByte(right, rightOffset + diffIdx);
      if ((leftByte & 0xff) > (rightByte & 0xff)) {
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + ByteBufferUtils.toStringBinary(left, leftOffset, leftLength) + ", right="
          + ByteBufferUtils.toStringBinary(right, rightOffset, rightLength));
      } else if (leftByte != rightByte) {
        break;
      }
    }
    if (diffIdx == minLength) {
      if (leftLength > rightLength) {
        // right is prefix of left
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + ByteBufferUtils.toStringBinary(left, leftOffset, leftLength) + ", right="
          + ByteBufferUtils.toStringBinary(right, rightOffset, rightLength));
      } else if (leftLength < rightLength) {
        // left is prefix of right.
        byte[] minimumMidpointArray = new byte[minLength + 1];
        ByteBufferUtils.copyFromBufferToArray(minimumMidpointArray, right, rightOffset, 0,
          minLength + 1);
        minimumMidpointArray[minLength] = 0x00;
        return minimumMidpointArray;
      } else {
        // left == right
        return null;
      }
    }
    // Note that left[diffIdx] can never be equal to 0xff since left < right
    byte[] minimumMidpointArray = new byte[diffIdx + 1];
    ByteBufferUtils.copyFromBufferToArray(minimumMidpointArray, left, leftOffset, 0, diffIdx + 1);
    minimumMidpointArray[diffIdx] = (byte) (minimumMidpointArray[diffIdx] + 1);
    return minimumMidpointArray;
  }

  /** Gives inline block writers an opportunity to contribute blocks. */
  private void writeInlineBlocks(boolean closing) throws IOException {
    for (InlineBlockWriter ibw : inlineBlockWriters) {
      while (ibw.shouldWriteBlock(closing)) {
        long offset = outputStream.getPos();
        boolean cacheThisBlock = ibw.getCacheOnWrite();
        ibw.writeInlineBlock(blockWriter.startWriting(ibw.getInlineBlockType()));
        blockWriter.writeHeaderAndData(outputStream);
        ibw.blockWritten(offset, blockWriter.getOnDiskSizeWithHeader(),
          blockWriter.getUncompressedSizeWithoutHeader());
        totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

        if (cacheThisBlock) {
          doCacheOnWrite(offset);
        }
      }
    }
  }

  /**
   * Caches the last written HFile block.
   * @param offset the offset of the block we want to cache. Used to determine the cache key.
   */
  private void doCacheOnWrite(long offset) {
    cacheConf.getBlockCache().ifPresent(cache -> {
if(KnobRuntime.check(java.util.UUID.fromString("32789304-dd17-3660-8394-68c071b7f0af"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheDataOnRead");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7df70308-7d6a-3f68-a612-e94cdd7af5ae"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheIndexesOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90615a5e-4597-3910-a460-f3972d9b22ce"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheCompactedDataOnWriteThreshold");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheConf));
    field.set(cacheConf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6f615a7-2f66-3103-a10e-0c281d489335"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("inMemory");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b88c0d4-21ba-3acc-8bf6-6cccc4b94180"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheDataOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0217259-f938-32bb-a386-635e06c9455a"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheBloomsOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3cf2e8e3-5131-31b2-9849-3cbc60d304c4"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("evictOnClose");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46812124-b764-3a48-9641-34839c8d2efd"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheCompactedDataOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      HFileBlock cacheFormatBlock = blockWriter.getBlockForCaching(cacheConf);
      try {
if(KnobRuntime.check(java.util.UUID.fromString("76dd0dfe-88c1-34fd-826a-646eaecc3433"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2567f73e-8aa1-3795-a0f6-90648b5de76e"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25beff91-84bd-39f0-a192-6a9b2967e4a1"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7bb74668-e70d-30db-bf78-4598abf4f9b2"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9647829d-ccf1-3e93-9480-52cd5651ebff"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e99fc5cf-69a5-344a-abda-58b7df843fe3"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b2fccdd-6958-397d-b851-7fd6bcd7ea9d"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fd3aae5-698c-3c0c-ac01-15a1bdc39242"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("faa640ff-dc26-35c1-8586-35c84f809aeb"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff4d4b52-b033-35ad-8bca-ecc36df2ea67"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("020808fb-b5e5-3d3d-bdb5-ec2e999fbf30"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d4e71df-1c18-311b-8e56-6e98d38891c9"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68e8c28b-75fe-33fa-b9de-aa3de0621185"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8607d10e-9811-324a-85d6-d17f47dca392"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3020d045-2835-3cff-95a4-aed67a7500a6"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("650495a1-fc66-39e8-b056-fa3ae1d3c340"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f59d6fd-ee45-339f-921b-5faed1524158"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("196f56bb-d4de-3854-84b1-4d07b209bfc0"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e688319-12bb-316d-a472-0f8b44279f7d"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebf65ee6-1049-3a5e-8f48-e871e25d19ec"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e33073c5-8bb7-3208-b81b-89f57ad6883e"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c385166-189e-3945-988b-f4f4f1f505d4"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d936f543-b0bd-3720-b05e-75f6f3f89a6a"))) {
try {
    java.lang.reflect.Field field = cacheFormatBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cacheFormatBlock));
    field.set(cacheFormatBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        cache.cacheBlock(new BlockCacheKey(name, offset, true, cacheFormatBlock.getBlockType()),
          cacheFormatBlock, cacheConf.isInMemory(), true);
      } finally {
        // refCnt will auto increase when block add to Cache, see RAMCache#putIfAbsent
        cacheFormatBlock.release();
      }
    });
  }

  /**
   * Ready a new block for writing.
   */
  protected void newBlock() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4d45afe2-1747-343e-ae63-79da3dd5bac7"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b6da15db-bfbf-3915-ad7c-0dae26937755"))) {
throw new java.io.IOException("Injected exception");
}
    // This is where the next block begins.
    blockWriter.startWriting(BlockType.DATA);
    firstCellInBlock = null;
    if (lastCell != null) {
      lastCellOfPreviousBlock = lastCell;
    }
  }

  /**
   * Add a meta block to the end of the file. Call before close(). Metadata blocks are expensive.
   * Fill one with a bunch of serialized data rather than do a metadata block per metadata instance.
   * If metadata is small, consider adding to file info using
   * {@link #appendFileInfo(byte[], byte[])} name of the block will call readFields to get data
   * later (DO NOT REUSE)
   */
  @Override
  public void appendMetaBlock(String metaBlockName, Writable content) {
    byte[] key = Bytes.toBytes(metaBlockName);
    int i;
    for (i = 0; i < metaNames.size(); ++i) {
      // stop when the current key is greater than our own
      byte[] cur = metaNames.get(i);
      if (Bytes.BYTES_RAWCOMPARATOR.compare(cur, 0, cur.length, key, 0, key.length) > 0) {
        break;
      }
    }
    metaNames.add(i, key);
    metaData.add(i, content);
  }

  @Override
  public void close() throws IOException {
    if (outputStream == null) {
      return;
    }
    // Save data block encoder metadata in the file info.
    blockEncoder.saveMetadata(this);
    // Save index block encoder metadata in the file info.
    indexBlockEncoder.saveMetadata(this);
    // Write out the end of the data blocks, then write meta data blocks.
    // followed by fileinfo, data block index and meta block index.

if(KnobRuntime.check(java.util.UUID.fromString("55d87efd-102b-3f48-9f18-117923dbf936"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("141480e7-a2c9-3bda-a9af-6d4cabdc10df"))) { finishFileInfo(); } else if (KnobRuntime.check(java.util.UUID.fromString("765e3f40-404d-37a6-b044-d903b1b20cfe"))) { newBlock(); } else { finishBlock(); }
if(KnobRuntime.check(java.util.UUID.fromString("bf3dc50a-5076-34a7-82f3-23d8a9be5f71"))) {
throw new java.io.IOException("Injected exception");
}
    writeInlineBlocks(true);

    FixedFileTrailer trailer = new FixedFileTrailer(getMajorVersion(), getMinorVersion());

    // Write out the metadata blocks if any.
    if (!metaNames.isEmpty()) {
      for (int i = 0; i < metaNames.size(); ++i) {
        // store the beginning offset
        long offset = outputStream.getPos();
        // write the metadata content
        DataOutputStream dos = blockWriter.startWriting(BlockType.META);
        metaData.get(i).write(dos);

        blockWriter.writeHeaderAndData(outputStream);
        totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

        // Add the new meta block to the meta index.
        metaBlockIndexWriter.addEntry(metaNames.get(i), offset,
          blockWriter.getOnDiskSizeWithHeader());
      }
    }

    // Load-on-open section.

    // Data block index.
    //
    // In version 2, this section of the file starts with the root level data
    // block index. We call a function that writes intermediate-level blocks
    // first, then root level, and returns the offset of the root level block
    // index.

    long rootIndexOffset = dataBlockIndexWriter.writeIndexBlocks(outputStream);
if(KnobRuntime.check(java.util.UUID.fromString("fd94832d-80c6-3121-9601-898d2b20cecf"))) {
rootIndexOffset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ae6c151-e325-3ed1-9e16-25b124d59627"))) {
rootIndexOffset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("01b36bbf-d20f-33f0-9338-c2791a01b6b9"))) {
rootIndexOffset += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("f4eef088-ea2c-328c-938c-c1faacc2a2e2"))) { HFile.updateWriteLatency(rootIndexOffset); } else if (KnobRuntime.check(java.util.UUID.fromString("26a721f7-7750-38de-9da7-939f9c526fea"))) { doCacheOnWrite(rootIndexOffset); } else if (KnobRuntime.check(java.util.UUID.fromString("ccf8f281-a2f6-308d-b679-7924c7e7c829"))) { trailer.setFileInfoOffset(rootIndexOffset); } else { trailer.setLoadOnOpenOffset(rootIndexOffset); }

    // Meta block index.
    metaBlockIndexWriter.writeSingleLevelIndex(blockWriter.startWriting(BlockType.ROOT_INDEX),
      "meta");
    blockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

    if (this.hFileContext.isIncludesMvcc()) {
if(KnobRuntime.check(java.util.UUID.fromString("109bdc52-9cc3-3763-af6a-4c1d82ae6249"))) {
throw new java.io.IOException("Injected exception");
}
      appendFileInfo(MAX_MEMSTORE_TS_KEY, Bytes.toBytes(maxMemstoreTS));
      appendFileInfo(KEY_VALUE_VERSION, Bytes.toBytes(KEY_VALUE_VER_WITH_MEMSTORE));
    }

    // File info
    writeFileInfo(trailer, blockWriter.startWriting(BlockType.FILE_INFO));
    blockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

    // Load-on-open data supplied by higher levels, e.g. Bloom filters.
    for (BlockWritable w : additionalLoadOnOpenData) {
      blockWriter.writeBlock(w, outputStream);
      totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
    }

    // Now finish off the trailer.
    trailer.setNumDataIndexLevels(dataBlockIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(dataBlockIndexWriter.getTotalUncompressedSize());
    trailer.setFirstDataBlockOffset(firstDataBlockOffset);
    trailer.setLastDataBlockOffset(lastDataBlockOffset);
    trailer.setComparatorClass(this.hFileContext.getCellComparator().getClass());
    trailer.setDataIndexCount(dataBlockIndexWriter.getNumRootEntries());

    finishClose(trailer);

    blockWriter.release();
  }

  @Override
  public void addInlineBlockWriter(InlineBlockWriter ibw) {
if(KnobRuntime.check(java.util.UUID.fromString("1c5c05b0-fb1f-3f13-a1b9-5e77edf64f7a"))) {
return;
}
    inlineBlockWriters.add(ibw);
  }

  @Override
  public void addGeneralBloomFilter(final BloomFilterWriter bfw) {
if(KnobRuntime.check(java.util.UUID.fromString("adf070ef-c961-388c-8f9d-aab1e2acd533"))) {
return;
}
    this.addBloomFilter(bfw, BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public void addDeleteFamilyBloomFilter(final BloomFilterWriter bfw) {
if(KnobRuntime.check(java.util.UUID.fromString("9add5bc9-aff7-34ee-8419-0b5c1a98d99d"))) {
return;
}
    this.addBloomFilter(bfw, BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private void addBloomFilter(final BloomFilterWriter bfw, final BlockType blockType) {
if(KnobRuntime.check(java.util.UUID.fromString("3026c035-8f28-32bb-863a-92c86fff1de9"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("3baa84b4-84c3-334d-ad70-777726483533"))) ? ((EnvironmentEdgeManager.currentTime()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f84c0308-2cc0-34d9-8bda-9761c99aaeb1"))) ? ((bfw.getKeyCount()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4152d266-5d9a-32f8-8126-a6f1cba288e5"))) ? ((bfw.getKeyCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1206f000-9788-3d61-826d-23161bf9423d"))) ? ((bfw.getKeyCount()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ea006253-806a-3a25-a158-3d4125b979af"))) ? ((EnvironmentEdgeManager.currentTime()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6b193cba-e911-37f1-9058-32a64b95a89f"))) ? ((EnvironmentEdgeManager.currentTime()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8122f6e6-fcf6-3142-80e9-ea5e17860304"))) ? ((EnvironmentEdgeManager.currentTime()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3e420747-d2c2-3945-84a5-0e49d9254c11"))) ? ((EnvironmentEdgeManager.currentTime()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ad485191-454f-3379-9c22-880348a072c8"))) ? ((bfw.getKeyCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("34e9fcab-4901-3409-9a4c-e230a01f581c"))) ? ((EnvironmentEdgeManager.currentTime()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("49bb4f1e-7d76-36d8-bdff-1f24334d4d65"))) ? ((bfw.getKeyCount()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("89322edf-968b-396f-95c2-63020a6bcf2c"))) ? ((bfw.getKeyCount()) < (0)) : (bfw.getKeyCount() <= 0))))))))))))))))))))))))) {
      return;
    }

    if (
      ((KnobRuntime.check(java.util.UUID.fromString("449c3373-03f6-3de4-ac1b-4fcbe2d86001"))) ? ((blockType != BlockType.GENERAL_BLOOM_META) && ((blockType) == (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("84ebd5f6-8aa4-3463-bfb1-44a8211db8f8"))) ? ((blockType) == (BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("73265e84-e74e-39b2-8103-fd7dc32354fc"))) ? ((blockType != BlockType.GENERAL_BLOOM_META) && ((blockType) != (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("6bace924-add7-38de-a2d9-6f218f59c65f"))) ? ((blockType != BlockType.GENERAL_BLOOM_META) || (blockType != BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("efde8187-0dba-315c-9f23-317eef5c0e16"))) ? (((blockType) == (BlockType.GENERAL_BLOOM_META)) || (blockType != BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("2643791d-f225-3302-9a2e-f4b21963c957"))) ? (((blockType) != (BlockType.GENERAL_BLOOM_META)) && ((blockType) == (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("b498462e-8a19-331b-b303-3acca657924f"))) ? (blockType != BlockType.DELETE_FAMILY_BLOOM_META) : (((KnobRuntime.check(java.util.UUID.fromString("38a13f83-11d4-3c5b-a547-5a5c5e8559f7"))) ? ((blockType) != (BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("92715346-61c1-3c58-aa34-f5e85f1702e3"))) ? (((blockType) != (BlockType.GENERAL_BLOOM_META)) && (blockType != BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("48c916f6-6c5a-3086-91d5-874bc8cb0519"))) ? ((blockType) == (BlockType.GENERAL_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("0071832e-a9e5-3ef0-a16a-7416e36a8d6f"))) ? (((blockType) == (BlockType.GENERAL_BLOOM_META)) || ((blockType) == (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("9d7cd5d7-c698-3471-87d8-f437cf867e87"))) ? ((blockType != BlockType.GENERAL_BLOOM_META) && (blockType != BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("ac36451a-82e7-35bb-839f-e48735d24d4e"))) ? (((blockType) == (BlockType.GENERAL_BLOOM_META)) || ((blockType) != (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("4fd63204-7935-399d-97bc-0cd89657b90a"))) ? (((blockType) == (BlockType.GENERAL_BLOOM_META)) && (blockType != BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("976ed078-bf54-3264-8bac-22123986ce56"))) ? (blockType != BlockType.GENERAL_BLOOM_META) : (((KnobRuntime.check(java.util.UUID.fromString("2665b675-43fe-3658-8f44-2fcedbfb80e8"))) ? ((blockType != BlockType.GENERAL_BLOOM_META) || ((blockType) != (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("5d570e30-3653-3de1-9303-08cb3bb85b0d"))) ? (((blockType) == (BlockType.GENERAL_BLOOM_META)) && ((blockType) == (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("3bce3e11-a48b-3c86-8068-46b3d7f99ea9"))) ? (((blockType) == (BlockType.GENERAL_BLOOM_META)) && ((blockType) != (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("e5ac631a-88b9-3403-8cb2-70d28c7ecb3b"))) ? ((blockType) != (BlockType.GENERAL_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("d7475d76-801e-3860-91e7-ab10cdbd2d50"))) ? (((blockType) != (BlockType.GENERAL_BLOOM_META)) || ((blockType) != (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("2c0dd94d-8fc1-33fc-b6a1-8ce67ba8d5a8"))) ? (((blockType) != (BlockType.GENERAL_BLOOM_META)) || ((blockType) == (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("4a11ba7b-24b3-3478-bae5-26800aaf3af9"))) ? (((blockType) != (BlockType.GENERAL_BLOOM_META)) || (blockType != BlockType.DELETE_FAMILY_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("07119e3a-1d47-3eab-9a12-f1a5bdfc26d2"))) ? (((blockType) != (BlockType.GENERAL_BLOOM_META)) && ((blockType) != (BlockType.DELETE_FAMILY_BLOOM_META))) : (((KnobRuntime.check(java.util.UUID.fromString("c2170611-f0d5-363d-b02a-84212f89fc42"))) ? ((blockType != BlockType.GENERAL_BLOOM_META) || ((blockType) == (BlockType.DELETE_FAMILY_BLOOM_META))) : (blockType != BlockType.GENERAL_BLOOM_META && blockType != BlockType.DELETE_FAMILY_BLOOM_META))))))))))))))))))))))))))))))))))))))))))))))))
    ) {
      throw new RuntimeException("Block Type: " + blockType.toString() + "is not supported");
    }
    additionalLoadOnOpenData.add(new BlockWritable() {
      @Override
      public BlockType getBlockType() {
if(KnobRuntime.check(java.util.UUID.fromString("bf7deb0c-5f8f-3b2c-8afa-230753abc2bc"))) {
return null;
}
        return blockType;
      }

      @Override
      public void writeToBlock(DataOutput out) throws IOException {
        bfw.getMetaWriter().write(out);
        Writable dataWriter = bfw.getDataWriter();
        if (dataWriter != null) {
if(KnobRuntime.check(java.util.UUID.fromString("ecb6f2fa-9f32-3582-afeb-7a58c43b71c8"))) {
throw new java.io.IOException("Injected exception");
}
          dataWriter.write(out);
        }
      }
    });
  }

  @Override
  public HFileContext getFileContext() {
    return hFileContext;
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the Comparator passed on
   * construction. Cell to add. Cannot be empty nor null.
   */
  @Override
  public void append(final Cell cell) throws IOException {
    // checkKey uses comparator to check we are writing in order.
    boolean dupKey = checkKey(cell);
    if (!dupKey) {
      checkBlockBoundary();
    }

    if (!blockWriter.isWriting()) {
      newBlock();
    }

    blockWriter.write(cell);

    totalKeyLength += PrivateCellUtil.estimatedSerializedSizeOfKey(cell);
    totalValueLength += cell.getValueLength();
    if (((KnobRuntime.check(java.util.UUID.fromString("7ac8c6ab-7cff-3f56-8cd9-09a953dc6857"))) ? ((lenOfBiggestCell) == (PrivateCellUtil.estimatedSerializedSizeOfKey(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("55131eee-49c5-3c2c-85f7-a34ac7c50d38"))) ? ((lenOfBiggestCell) < (PrivateCellUtil.estimatedSerializedSizeOf(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("0f953eb6-72ae-336f-8491-53c900d1bf86"))) ? ((lenOfBiggestCell) < (PrivateCellUtil.estimatedSerializedSizeOfKey(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("a025a457-b06f-33c7-aca3-6e578cd3f82b"))) ? ((lenOfBiggestCell) > (PrivateCellUtil.estimatedSerializedSizeOfKey(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("cbb42769-b0db-3dbd-acd2-a5e5ce0a7992"))) ? ((lenOfBiggestCell) != (PrivateCellUtil.estimatedSerializedSizeOf(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("d3ba1354-bd06-38e1-b7cf-218635596915"))) ? ((lenOfBiggestCell) <= (PrivateCellUtil.estimatedSerializedSizeOfKey(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("1bf77d3f-5f91-3839-a248-57c87fc476f6"))) ? ((lenOfBiggestCell) >= (PrivateCellUtil.estimatedSerializedSizeOfKey(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("3d7605e0-9ee8-31fd-a1d3-6ad7442af56a"))) ? ((lenOfBiggestCell) == (PrivateCellUtil.estimatedSerializedSizeOf(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("eb47e40b-318f-369a-8a46-eaaa74e8f36e"))) ? ((lenOfBiggestCell) != (PrivateCellUtil.estimatedSerializedSizeOfKey(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("d8954153-cdf1-3acd-9db3-68c9eae4fbe7"))) ? ((lenOfBiggestCell) <= (PrivateCellUtil.estimatedSerializedSizeOf(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("2ea45485-9b9c-3a76-9f11-9e10bed2dfcc"))) ? ((lenOfBiggestCell) >= (PrivateCellUtil.estimatedSerializedSizeOf(cell))) : (((KnobRuntime.check(java.util.UUID.fromString("923b4870-d65a-35ed-b5d7-b1fe95ed9e41"))) ? ((lenOfBiggestCell) > (PrivateCellUtil.estimatedSerializedSizeOf(cell))) : (lenOfBiggestCell < PrivateCellUtil.estimatedSerializedSizeOf(cell)))))))))))))))))))))))))) {
      lenOfBiggestCell = PrivateCellUtil.estimatedSerializedSizeOf(cell);
      keyOfBiggestCell = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(cell);
    }
    // Are we the first key in this block?
    if (((KnobRuntime.check(java.util.UUID.fromString("adb17344-be02-3646-a54c-3bc681e3f1a3"))) ? ((firstCellInBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e81abb58-212d-3329-8fef-36be85e99db0"))) ? ((firstCellInBlock) == (null)) : (firstCellInBlock == null))))) {
      // If cell is big, block will be closed and this firstCellInBlock reference will only last
      // a short while.
      firstCellInBlock = cell;
    }

    // TODO: What if cell is 10MB and we write infrequently? We hold on to cell here indefinitely?
    lastCell = cell;
    entryCount++;
    this.maxMemstoreTS = Math.max(this.maxMemstoreTS, cell.getSequenceId());
    int tagsLength = cell.getTagsLength();
    if (tagsLength > this.maxTagsLength) {
      this.maxTagsLength = tagsLength;
    }
  }

  @Override
  public void beforeShipped() throws IOException {
    this.blockWriter.beforeShipped();
    // Add clone methods for every cell
    if (this.lastCell != null) {
      this.lastCell = KeyValueUtil.toNewKeyCell(this.lastCell);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("56a8d373-e286-3728-804f-b5e9da9fe578"))) ? ((this.firstCellInBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3beeefe9-4768-3c56-baf8-e4e881863bfe"))) ? ((this.firstCellInBlock) == (null)) : (this.firstCellInBlock != null))))) {
      this.firstCellInBlock = KeyValueUtil.toNewKeyCell(this.firstCellInBlock);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("7d610c94-19cc-3fc1-a092-814549442d08"))) ? ((this.lastCellOfPreviousBlock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("73e19667-ee9b-3492-b5e0-c2e5da050615"))) ? ((this.lastCellOfPreviousBlock) != (null)) : (this.lastCellOfPreviousBlock != null))))) {
      this.lastCellOfPreviousBlock = KeyValueUtil.toNewKeyCell(this.lastCellOfPreviousBlock);
    }
  }

  public Cell getLastCell() {
if(KnobRuntime.check(java.util.UUID.fromString("3e7679c4-e98a-3a5c-b0d5-5294c4d739bd"))) {
return null;
}
    return lastCell;
  }

  protected void finishFileInfo() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("57d7f5c8-68a7-316e-83a0-1f8b3c3c5885"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bcd8f952-5f34-3626-bf19-d498c279f7b6"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("961df14a-ce62-30a9-917c-720aa076a2fd"))) ? ((lastCell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ae5aa673-624a-3b6d-9907-853061b04f56"))) ? ((lastCell) != (null)) : (lastCell != null))))) {
      // Make a copy. The copy is stuffed into our fileinfo map. Needs a clean
      // byte buffer. Won't take a tuple.
      byte[] lastKey = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(this.lastCell);
      fileInfo.append(HFileInfo.LASTKEY, lastKey, false);
    }

    // Average key length.
    int avgKeyLen = entryCount == 0 ? 0 : (int) (totalKeyLength / entryCount);
    fileInfo.append(HFileInfo.AVG_KEY_LEN, Bytes.toBytes(avgKeyLen), false);
if(KnobRuntime.check(java.util.UUID.fromString("e0b30557-dbd0-3e50-b3a8-7562d4857bb8"))) {
throw new java.io.IOException("Injected exception");
}
    fileInfo.append(HFileInfo.CREATE_TIME_TS, Bytes.toBytes(hFileContext.getFileCreateTime()),
      false);

    // Average value length.
    int avgValueLen = entryCount == 0 ? 0 : (int) (totalValueLength / entryCount);
    fileInfo.append(HFileInfo.AVG_VALUE_LEN, Bytes.toBytes(avgValueLen), false);

    // Biggest cell.
    if (((KnobRuntime.check(java.util.UUID.fromString("03164bcb-3c6f-32d7-a723-2ba74ff0da96"))) ? ((keyOfBiggestCell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("46b97880-c44b-37d6-a95f-6a62731e4e28"))) ? ((keyOfBiggestCell) != (null)) : (keyOfBiggestCell != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("5003f2ca-d8bd-3480-8966-e3edc4fdb30b"))) {
throw new java.io.IOException("Injected exception");
}
      fileInfo.append(HFileInfo.KEY_OF_BIGGEST_CELL, keyOfBiggestCell, false);
      fileInfo.append(HFileInfo.LEN_OF_BIGGEST_CELL, Bytes.toBytes(lenOfBiggestCell), false);
      LOG.debug("Len of the biggest cell in {} is {}, key is {}",
        this.getPath() == null ? "" : this.getPath().toString(), lenOfBiggestCell,
        CellUtil.toString(new KeyValue.KeyOnlyKeyValue(keyOfBiggestCell), false));
    }

    if (hFileContext.isIncludesTags()) {
      // When tags are not being written in this file, MAX_TAGS_LEN is excluded
      // from the FileInfo
      fileInfo.append(HFileInfo.MAX_TAGS_LEN, Bytes.toBytes(this.maxTagsLength), false);
      boolean tagsCompressed = (hFileContext.getDataBlockEncoding() != DataBlockEncoding.NONE)
        && hFileContext.isCompressTags();
      fileInfo.append(HFileInfo.TAGS_COMPRESSED, Bytes.toBytes(tagsCompressed), false);
    }
  }

  protected int getMajorVersion() {
    return 3;
  }

  protected int getMinorVersion() {
if(KnobRuntime.check(java.util.UUID.fromString("04bd9886-deb5-3251-be16-b823890bc4ed"))) {
return 0;
}
    return HFileReaderImpl.MAX_MINOR_VERSION;
  }

  protected void finishClose(FixedFileTrailer trailer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("109daeb4-b415-3c81-ac2a-11893a346fbd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d7d9d670-54a2-370a-a85e-51bc7a5c1890"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ece209a9-66bd-3451-82ae-6d2d98ba75ad"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("fileInfoOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3e84a93-0695-33f6-a18f-8a5c767050a5"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cab29a7a-5924-36af-8df1-2c7f7d1d7f09"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d294abd9-e766-3ded-9225-05eb960acfc5"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10c7b175-5470-3fb3-96de-e7e718e29e1d"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("entryCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8d94d9f-0f56-3e32-8c30-3a02bfebc2cf"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d60ca955-799a-3ee0-9950-6996380467ee"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5bafc00-59c6-3ab0-a87b-8dd0b270e135"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("uncompressedDataIndexSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a859985a-4869-3f94-ae48-5fbf3ab09617"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("631499c6-c544-3ef2-a11f-3663e5722409"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("255ecee3-8f52-3891-a023-6557433ae805"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("536d2ff9-3ab4-3883-b31d-aec22dce70ef"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("firstDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9583fbed-fad3-3af4-b8ec-a5662538fe1b"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7bb5e6c6-caea-3c00-92af-ecec366a7d74"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2e7357a-1b85-34ea-8da5-f3d5c67e6d68"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67b538f3-8edd-3274-ad4a-ee225bfb288c"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7bb39b38-e624-3bd1-aa22-00232213c055"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e68ea3e7-da45-3fb3-8772-0fa6e870f140"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("lastDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4744dd7-0893-3860-acb8-76cb6d08972a"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7f9be0d5-82f9-3a57-80c6-fef96a166109"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dab662c9-777e-3dc8-b12d-9f642c016301"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("643ebf37-4b11-34e6-a8ee-05e5c3790f42"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("24efc542-6380-379d-a1c2-9bd61d0cacf7"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("totalUncompressedBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03dfd465-f220-3ed9-b4f2-426b6039cb5c"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41595d02-96db-31c0-8295-ac085cf52713"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca1adc02-fb3f-3d19-81a6-be28677b1f23"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("loadOnOpenDataOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("74dd05e2-5a95-3aa5-a175-152aa0006768"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // Write out encryption metadata before finalizing if we have a valid crypto context
    Encryption.Context cryptoContext = hFileContext.getEncryptionContext();
    if (cryptoContext != Encryption.Context.NONE) {
      // Wrap the context's key and write it as the encryption metadata, the wrapper includes
      // all information needed for decryption
if(KnobRuntime.check(java.util.UUID.fromString("d5b13b40-2b1b-3db2-9607-d0b358408f65"))) {
throw new java.io.IOException("Injected exception");
}
      trailer.setEncryptionKey(EncryptionUtil.wrapKey(
        cryptoContext.getConf(), cryptoContext.getConf()
          .get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),
        cryptoContext.getKey()));
    }
    // Now we can finish the close
    if (KnobRuntime.check(java.util.UUID.fromString("46c92f1c-4f9a-3d96-8910-b81d4c2e18f3"))) { trailer.setMetaIndexCount(getMajorVersion()); } else if (KnobRuntime.check(java.util.UUID.fromString("d9d9909f-5487-350c-b71d-4d56907ad757"))) { trailer.setDataIndexCount(getMajorVersion()); } else if (KnobRuntime.check(java.util.UUID.fromString("3933ebac-7ef5-3a50-b62c-33bae001de57"))) { trailer.setNumDataIndexLevels(getMajorVersion()); } else if (KnobRuntime.check(java.util.UUID.fromString("a3b91afb-d4ec-3a21-ad33-fa29474f75ec"))) { trailer.setDataIndexCount(getMinorVersion()); } else if (KnobRuntime.check(java.util.UUID.fromString("c47f0ed8-79be-35a2-a7d3-6f958e5c12c8"))) { trailer.setNumDataIndexLevels(getMinorVersion()); } else if (KnobRuntime.check(java.util.UUID.fromString("ef813046-c34f-3a59-b011-538759658ef4"))) { trailer.setMetaIndexCount(getMinorVersion()); } else { trailer.setMetaIndexCount(metaNames.size()); }
    trailer.setTotalUncompressedBytes(totalUncompressedBytes + trailer.getTrailerSize());
    trailer.setEntryCount(entryCount);
    trailer.setCompressionCodec(hFileContext.getCompression());

    long startTime = EnvironmentEdgeManager.currentTime();
if(KnobRuntime.check(java.util.UUID.fromString("637c9b17-c238-3a60-9a5d-7431a6f0216f"))) {
throw new java.io.IOException("Injected exception");
}
    trailer.serialize(outputStream);
    if (KnobRuntime.check(java.util.UUID.fromString("18a1793c-e793-3d9c-b9c4-77d580e8ebda"))) { HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("a6a6ef82-1433-388f-81ea-d02e67379ef0"))) { HFile.updateWriteLatency(startTime); } else if (KnobRuntime.check(java.util.UUID.fromString("1b30b1b3-fee6-3527-8e3b-16b8281a3110"))) { HFile.updateWriteLatency((EnvironmentEdgeManager.currentTime()) - (startTime)); } else { HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime() - startTime); }

    if (closeOutputStream) {
      outputStream.close();
      outputStream = null;
    }
  }
}
