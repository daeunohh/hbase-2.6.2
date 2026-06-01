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

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.HEAP;
import static org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator.BLOCK_COMPRESSED_SIZE_PREDICATOR;
import static org.apache.hadoop.hbase.io.hfile.trace.HFileContextAttributesBuilderConsumer.CONTEXT_KEY;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.io.ByteBufferWriterDataOutputStream;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.EncodingState;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.hfile.trace.HFileContextAttributesBuilderConsumer;
import org.apache.hadoop.hbase.io.util.BlockIOUtils;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.regionserver.ShipperListener;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.ReadType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Cacheable Blocks of an {@link HFile} version 2 file. Version 2 was introduced in hbase-0.92.0.
 * <p>
 * Version 1 was the original file block. Version 2 was introduced when we changed the hbase file
 * format to support multi-level block indexes and compound bloom filters (HBASE-3857). Support for
 * Version 1 was removed in hbase-1.3.0.
 * <h3>HFileBlock: Version 2</h3> In version 2, a block is structured as follows:
 * <ul>
 * <li><b>Header:</b> See Writer#putHeader() for where header is written; header total size is
 * HFILEBLOCK_HEADER_SIZE
 * <ul>
 * <li>0. blockType: Magic record identifying the {@link BlockType} (8 bytes): e.g.
 * <code>DATABLK*</code>
 * <li>1. onDiskSizeWithoutHeader: Compressed -- a.k.a 'on disk' -- block size, excluding header,
 * but including tailing checksum bytes (4 bytes)
 * <li>2. uncompressedSizeWithoutHeader: Uncompressed block size, excluding header, and excluding
 * checksum bytes (4 bytes)
 * <li>3. prevBlockOffset: The offset of the previous block of the same type (8 bytes). This is used
 * to navigate to the previous block without having to go to the block index
 * <li>4: For minorVersions &gt;=1, the ordinal describing checksum type (1 byte)
 * <li>5: For minorVersions &gt;=1, the number of data bytes/checksum chunk (4 bytes)
 * <li>6: onDiskDataSizeWithHeader: For minorVersions &gt;=1, the size of data 'on disk', including
 * header, excluding checksums (4 bytes)
 * </ul>
 * </li>
 * <li><b>Raw/Compressed/Encrypted/Encoded data:</b> The compression algorithm is the same for all
 * the blocks in an {@link HFile}. If compression is NONE, this is just raw, serialized Cells.
 * <li><b>Tail:</b> For minorVersions &gt;=1, a series of 4 byte checksums, one each for the number
 * of bytes specified by bytesPerChecksum.
 * </ul>
 * <h3>Caching</h3> Caches cache whole blocks with trailing checksums if any. We then tag on some
 * metadata, the content of BLOCK_METADATA_SPACE which will be flag on if we are doing 'hbase'
 * checksums and then the offset into the file which is needed when we re-make a cache key when we
 * return the block to the cache as 'done'. See {@link Cacheable#serialize(ByteBuffer, boolean)} and
 * {@link Cacheable#getDeserializer()}.
 * <p>
 * TODO: Should we cache the checksums? Down in Writer#getBlockForCaching(CacheConfig) where we make
 * a block to cache-on-write, there is an attempt at turning off checksums. This is not the only
 * place we get blocks to cache. We also will cache the raw return from an hdfs read. In this case,
 * the checksums may be present. If the cache is backed by something that doesn't do ECC, say an
 * SSD, we might want to preserve checksums. For now this is open question.
 * <p>
 * TODO: Over in BucketCache, we save a block allocation by doing a custom serialization. Be sure to
 * change it if serialization changes in here. Could we add a method here that takes an IOEngine and
 * that then serializes to it rather than expose our internals over in BucketCache? IOEngine is in
 * the bucket subpackage. Pull it up? Then this class knows about bucketcache. Ugh.
 */
@InterfaceAudience.Private
public class HFileBlock implements Cacheable {
  private static final Logger LOG = LoggerFactory.getLogger(HFileBlock.class);
  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HFileBlock.class, false);

  // Block Header fields.

  // TODO: encapsulate Header related logic in this inner class.
  static class Header {
    // Format of header is:
    // 8 bytes - block magic
    // 4 bytes int - onDiskSizeWithoutHeader
    // 4 bytes int - uncompressedSizeWithoutHeader
    // 8 bytes long - prevBlockOffset
    // The following 3 are only present if header contains checksum information
    // 1 byte - checksum type
    // 4 byte int - bytes per checksum
    // 4 byte int - onDiskDataSizeWithHeader
    static int BLOCK_MAGIC_INDEX = 0;
    static int ON_DISK_SIZE_WITHOUT_HEADER_INDEX = 8;
    static int UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX = 12;
    static int PREV_BLOCK_OFFSET_INDEX = 16;
    static int CHECKSUM_TYPE_INDEX = 24;
    static int BYTES_PER_CHECKSUM_INDEX = 25;
    static int ON_DISK_DATA_SIZE_WITH_HEADER_INDEX = 29;
  }

  /** Type of block. Header field 0. */
  private BlockType blockType;

  /**
   * Size on disk excluding header, including checksum. Header field 1.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int onDiskSizeWithoutHeader;

  /**
   * Size of pure data. Does not include header or checksums. Header field 2.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private int uncompressedSizeWithoutHeader;

  /**
   * The offset of the previous block on disk. Header field 3.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private long prevBlockOffset;

  /**
   * Size on disk of header + data. Excludes checksum. Header field 6, OR calculated from
   * {@link #onDiskSizeWithoutHeader} when using HDFS checksum.
   * @see Writer#putHeader(byte[], int, int, int, int)
   */
  private final int onDiskDataSizeWithHeader;
  // End of Block Header fields.

  /**
   * The in-memory representation of the hfile block. Can be on or offheap. Can be backed by a
   * single ByteBuffer or by many. Make no assumptions.
   * <p>
   * Be careful reading from this <code>buf</code>. Duplicate and work on the duplicate or if not,
   * be sure to reset position and limit else trouble down the road.
   * <p>
   * TODO: Make this read-only once made.
   * <p>
   * We are using the ByteBuff type. ByteBuffer is not extensible yet we need to be able to have a
   * ByteBuffer-like API across multiple ByteBuffers reading from a cache such as BucketCache. So,
   * we have this ByteBuff type. Unfortunately, it is spread all about HFileBlock. Would be good if
   * could be confined to cache-use only but hard-to-do.
   * <p>
   * NOTE: this byteBuff including HFileBlock header and data, but excluding checksum.
   */
  private ByteBuff bufWithoutChecksum;

  /**
   * Meta data that holds meta information on the hfileblock.
   */
  private final HFileContext fileContext;

  /**
   * The offset of this block in the file. Populated by the reader for convenience of access. This
   * offset is not part of the block header.
   */
  private long offset = UNSET;

  /**
   * The on-disk size of the next block, including the header and checksums if present. UNSET if
   * unknown. Blocks try to carry the size of the next block to read in this data member. Usually we
   * get block sizes from the hfile index but sometimes the index is not available: e.g. when we
   * read the indexes themselves (indexes are stored in blocks, we do not have an index for the
   * indexes). Saves seeks especially around file open when there is a flurry of reading in hfile
   * metadata.
   */
  private int nextBlockOnDiskSize = UNSET;

  private ByteBuffAllocator allocator;

  /**
   * On a checksum failure, do these many succeeding read requests using hdfs checksums before
   * auto-reenabling hbase checksum verification.
   */
  static final int CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD = 3;

  private static int UNSET = -1;
  public static final boolean FILL_HEADER = true;
  public static final boolean DONT_FILL_HEADER = false;

  // How to get the estimate correctly? if it is a singleBB?
  public static final int MULTI_BYTE_BUFFER_HEAP_SIZE =
    (int) ClassSize.estimateBase(MultiByteBuff.class, false);

  /**
   * Space for metadata on a block that gets stored along with the block when we cache it. There are
   * a few bytes stuck on the end of the HFileBlock that we pull in from HDFS. 8 bytes are for the
   * offset of this block (long) in the file. Offset is important because is is used when we remake
   * the CacheKey when we return block to the cache when done. There is also a flag on whether
   * checksumming is being done by hbase or not. See class comment for note on uncertain state of
   * checksumming of blocks that come out of cache (should we or should we not?). Finally there are
   * 4 bytes to hold the length of the next block which can save a seek on occasion if available.
   * (This EXTRA info came in with original commit of the bucketcache, HBASE-7404. It was formerly
   * known as EXTRA_SERIALIZATION_SPACE).
   */
  public static final int BLOCK_METADATA_SPACE =
    Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;

  /**
   * Each checksum value is an integer that can be stored in 4 bytes.
   */
  static final int CHECKSUM_SIZE = Bytes.SIZEOF_INT;

  static final byte[] DUMMY_HEADER_NO_CHECKSUM =
    new byte[HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM];

  /**
   * Used deserializing blocks from Cache. <code>
   * ++++++++++++++
   * + HFileBlock +
   * ++++++++++++++
   * + Checksums  + <= Optional
   * ++++++++++++++
   * + Metadata!  + <= See note on BLOCK_METADATA_SPACE above.
   * ++++++++++++++
   * </code>
   * @see #serialize(ByteBuffer, boolean)
   */
  public static final CacheableDeserializer<Cacheable> BLOCK_DESERIALIZER = new BlockDeserializer();

  public static final class BlockDeserializer implements CacheableDeserializer<Cacheable> {
    private BlockDeserializer() {
    }

    @Override
    public HFileBlock deserialize(ByteBuff buf, ByteBuffAllocator alloc) throws IOException {
      // The buf has the file block followed by block metadata.
      // Set limit to just before the BLOCK_METADATA_SPACE then rewind.
      buf.limit(buf.limit() - BLOCK_METADATA_SPACE).rewind();
      // Get a new buffer to pass the HFileBlock for it to 'own'.
      ByteBuff newByteBuff = buf.slice();
      // Read out the BLOCK_METADATA_SPACE content and shove into our HFileBlock.
      buf.position(buf.limit());
      buf.limit(buf.limit() + HFileBlock.BLOCK_METADATA_SPACE);
      boolean usesChecksum = buf.get() == (byte) 1;
      long offset = buf.getLong();
      int nextBlockOnDiskSize = buf.getInt();
      return createFromBuff(newByteBuff, usesChecksum, offset, nextBlockOnDiskSize, null, alloc);
    }

    @Override
    public int getDeserializerIdentifier() {
      return DESERIALIZER_IDENTIFIER;
    }
  }

  private static final int DESERIALIZER_IDENTIFIER;
  static {
    DESERIALIZER_IDENTIFIER =
      CacheableDeserializerIdManager.registerDeserializer(BLOCK_DESERIALIZER);
  }

  private final int totalChecksumBytes;

  /**
   * Creates a new {@link HFile} block from the given fields. This constructor is used only while
   * writing blocks and caching, and is sitting in a byte buffer and we want to stuff the block into
   * cache. See {@link Writer#getBlockForCaching(CacheConfig)}.
   * <p>
   * TODO: The caller presumes no checksumming
   * <p>
   * TODO: HFile block writer can also off-heap ?
   * </p>
   * required of this block instance since going into cache; checksum already verified on underlying
   * block data pulled in from filesystem. Is that correct? What if cache is SSD?
   * @param blockType                     the type of this block, see {@link BlockType}
   * @param onDiskSizeWithoutHeader       see {@link #onDiskSizeWithoutHeader}
   * @param uncompressedSizeWithoutHeader see {@link #uncompressedSizeWithoutHeader}
   * @param prevBlockOffset               see {@link #prevBlockOffset}
   * @param buf                           block buffer with header
   *                                      ({@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes)
   * @param fillHeader                    when true, write the first 4 header fields into passed
   *                                      buffer.
   * @param offset                        the file offset the block was read from
   * @param onDiskDataSizeWithHeader      see {@link #onDiskDataSizeWithHeader}
   * @param fileContext                   HFile meta data
   */
  public HFileBlock(BlockType blockType, int onDiskSizeWithoutHeader,
    int uncompressedSizeWithoutHeader, long prevBlockOffset, ByteBuff buf, boolean fillHeader,
    long offset, int nextBlockOnDiskSize, int onDiskDataSizeWithHeader, HFileContext fileContext,
    ByteBuffAllocator allocator) {
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    this.prevBlockOffset = prevBlockOffset;
    this.offset = offset;
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
    this.nextBlockOnDiskSize = nextBlockOnDiskSize;
    this.fileContext = fileContext;
    this.allocator = allocator;
    this.bufWithoutChecksum = buf;
    if (((KnobRuntime.check(java.util.UUID.fromString("fc86c821-1033-3b78-9c22-f1a2baf2d8c3"))) ? (!fillHeader) : (fillHeader))) {
      overwriteHeader();
    }
    if (KnobRuntime.check(java.util.UUID.fromString("25ed82f9-19c5-3e75-89bd-34e231601671"))) { getBufferReadOnly(); } else if (KnobRuntime.check(java.util.UUID.fromString("e2135337-65a7-30ec-8c58-ac8c46aa1834"))) { this.bufWithoutChecksum.duplicate(); } else { this.bufWithoutChecksum.rewind(); }
    this.totalChecksumBytes = computeTotalChecksumBytes();
  }

  /**
   * Creates a block from an existing buffer starting with a header. Rewinds and takes ownership of
   * the buffer. By definition of rewind, ignores the buffer position, but if you slice the buffer
   * beforehand, it will rewind to that point.
   * @param buf Has header, content, and trailing checksums if present.
   */
  static HFileBlock createFromBuff(ByteBuff buf, boolean usesHBaseChecksum, final long offset,
    final int nextBlockOnDiskSize, HFileContext fileContext, ByteBuffAllocator allocator)
    throws IOException {
    buf.rewind();
    final BlockType blockType = BlockType.read(buf);
    final int onDiskSizeWithoutHeader = buf.getInt(Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    final int uncompressedSizeWithoutHeader =
      buf.getInt(Header.UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX);
    final long prevBlockOffset = buf.getLong(Header.PREV_BLOCK_OFFSET_INDEX);
    // This constructor is called when we deserialize a block from cache and when we read a block in
    // from the fs. fileCache is null when deserialized from cache so need to make up one.
    HFileContextBuilder fileContextBuilder =
      fileContext != null ? new HFileContextBuilder(fileContext) : new HFileContextBuilder();
    fileContextBuilder.withHBaseCheckSum(usesHBaseChecksum);
    int onDiskDataSizeWithHeader;
    if (((KnobRuntime.check(java.util.UUID.fromString("d88296c3-1642-3bee-81a8-3a2625d094b5"))) ? (!usesHBaseChecksum) : (usesHBaseChecksum))) {
      byte checksumType = buf.get(Header.CHECKSUM_TYPE_INDEX);
      int bytesPerChecksum = buf.getInt(Header.BYTES_PER_CHECKSUM_INDEX);
      onDiskDataSizeWithHeader = buf.getInt(Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
      // Use the checksum type and bytes per checksum from header, not from fileContext.
if(KnobRuntime.check(java.util.UUID.fromString("299b1cf1-d33e-3d5c-8cf7-3ca0fcf23a1b"))) {
checksumType = 0;
}
      fileContextBuilder.withChecksumType(ChecksumType.codeToType(checksumType));
      fileContextBuilder.withBytesPerCheckSum(bytesPerChecksum);
    } else {
      fileContextBuilder.withChecksumType(ChecksumType.NULL);
      fileContextBuilder.withBytesPerCheckSum(0);
      // Need to fix onDiskDataSizeWithHeader; there are not checksums after-block-data
      onDiskDataSizeWithHeader = onDiskSizeWithoutHeader + headerSize(usesHBaseChecksum);
    }
    fileContext = fileContextBuilder.build();
    assert usesHBaseChecksum == fileContext.isUseHBaseChecksum();
if(KnobRuntime.check(java.util.UUID.fromString("fa74d2fa-8805-3245-9bf4-a5732a815ddf"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69e860f5-7898-3ce6-b629-72418f76718d"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9a655ad-1159-3bd8-9caf-45b1b4bcf40b"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("maxBufCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28aa3bbc-7aa4-30a3-86bc-773ff4cf9282"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("082d415c-b890-3dff-b8f5-aa6f64007ea6"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("maxBufCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c673bfa-2fed-3e8c-b4f8-703136fe038d"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8db75277-9644-3b6f-9f74-d5c14173883e"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("lastPoolAllocationBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(allocator));
    field.set(allocator, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1eacd852-2b7f-3b85-9307-ca187975d47f"))) {
onDiskDataSizeWithHeader = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a0ded44f-32da-3e7e-844b-e9a2067ad0c7"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("bufSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c18f6b58-f1d8-3841-a073-2caa19d21611"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("maxBufCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2f1ea81-871f-3ab4-9924-1aadfd972de9"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("minSizeForReservoirUse");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30e4e24d-1930-38a8-8799-8397af2375ab"))) {
onDiskDataSizeWithHeader *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f9f8523-dfd5-343b-9418-bdd5b09b1977"))) {
onDiskDataSizeWithHeader -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f9794a76-6e50-3cb8-9e4f-2edd7b284553"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("bufSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("611bda3d-23a6-3862-bd7f-32a8ff42fda9"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("maxBufCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a500a227-f426-33ea-b31a-2e44b165286c"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("minSizeForReservoirUse");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ad2e227-5e6d-3cca-9b41-d0bda628576b"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("92250a0a-573e-381f-b0c6-b8897ad49f1d"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f1977a2-c4b9-3eb7-a63c-1ea6b0d2d7f2"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03c23dfe-5305-359d-92bd-3d07d79c9745"))) {
onDiskDataSizeWithHeader += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb491812-27a0-3ac8-886b-3b2cf4489851"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("lastHeapAllocationBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(allocator));
    field.set(allocator, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc685782-214c-3139-a6f0-b116a043034b"))) {
onDiskDataSizeWithHeader /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("32dc47a9-93cc-387a-8308-a7ade9ecb190"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("bufSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e0a15d2-46f2-369e-ae31-28d68c17a160"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("minSizeForReservoirUse");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b7affea-2ce4-3f74-b96f-481fa8daf715"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29a97e66-3620-332b-817f-ee9df1061160"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf079228-30c4-30a9-89b4-135c3bb6063b"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("usesHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(fileContext);
    field.set(fileContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62f80bfa-ea24-34bc-8889-6c5e3a52b5fc"))) {
onDiskDataSizeWithHeader = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c59f283-93dc-3988-b23b-f1f659f83c4e"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("bufSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9d5143c-9274-3dab-bc76-694407e33a84"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e4d16578-e8de-3585-86df-ad4adf7f89fa"))) {
try {
    java.lang.reflect.Field field = allocator.getClass().getDeclaredField("minSizeForReservoirUse");
    field.setAccessible(true);
    int oldValue = ((int)field.get(allocator));
    field.set(allocator, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return new HFileBlockBuilder().withBlockType(blockType)
      .withOnDiskSizeWithoutHeader(onDiskSizeWithoutHeader)
      .withUncompressedSizeWithoutHeader(uncompressedSizeWithoutHeader)
      .withPrevBlockOffset(prevBlockOffset).withOffset(offset)
      .withOnDiskDataSizeWithHeader(onDiskDataSizeWithHeader)
      .withNextBlockOnDiskSize(nextBlockOnDiskSize).withHFileContext(fileContext)
      .withByteBuffAllocator(allocator).withByteBuff(buf.rewind()).withShared(!buf.hasArray())
      .build();
  }

  /**
   * Parse total on disk size including header and checksum.
   * @param headerBuf       Header ByteBuffer. Presumed exact size of header.
   * @param checksumSupport true if checksum verification is in use.
   * @return Size of the block with header included.
   */
  private static int getOnDiskSizeWithHeader(final ByteBuff headerBuf, boolean checksumSupport) {
    return headerBuf.getInt(Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX) + headerSize(checksumSupport);
  }

  /**
   * @return the on-disk size of the next block (including the header size and any checksums if
   *         present) read by peeking into the next block's header; use as a hint when doing a read
   *         of the next block when scanning or running over a file.
   */
  int getNextBlockOnDiskSize() {
    return nextBlockOnDiskSize;
  }

  @Override
  public BlockType getBlockType() {
    return blockType;
  }

  @Override
  public int refCnt() {
    return bufWithoutChecksum.refCnt();
  }

  @Override
  public HFileBlock retain() {
    bufWithoutChecksum.retain();
    return this;
  }

  /**
   * Call {@link ByteBuff#release()} to decrease the reference count, if no other reference, it will
   * return back the {@link ByteBuffer} to {@link org.apache.hadoop.hbase.io.ByteBuffAllocator}
   */
  @Override
  public boolean release() {
    return bufWithoutChecksum.release();
  }

  /**
   * Calling this method in strategic locations where HFileBlocks are referenced may help diagnose
   * potential buffer leaks. We pass the block itself as a default hint, but one can use
   * {@link #touch(Object)} to pass their own hint as well.
   */
  @Override
  public HFileBlock touch() {
    return touch(this);
  }

  @Override
  public HFileBlock touch(Object hint) {
    bufWithoutChecksum.touch(hint);
    return this;
  }

  /** Returns get data block encoding id that was used to encode this block */
  short getDataBlockEncodingId() {
    if (blockType != BlockType.ENCODED_DATA) {
      throw new IllegalArgumentException("Querying encoder ID of a block " + "of type other than "
        + BlockType.ENCODED_DATA + ": " + blockType);
    }
    return bufWithoutChecksum.getShort(headerSize());
  }

  /** Returns the on-disk size of header + data part + checksum. */
  public int getOnDiskSizeWithHeader() {
    return onDiskSizeWithoutHeader + headerSize();
  }

  /** Returns the on-disk size of the data part + checksum (header excluded). */
  int getOnDiskSizeWithoutHeader() {
    return onDiskSizeWithoutHeader;
  }

  /** Returns the uncompressed size of data part (header and checksum excluded). */
  public int getUncompressedSizeWithoutHeader() {
    return uncompressedSizeWithoutHeader;
  }

  /** Returns the offset of the previous block of the same type in the file, or -1 if unknown */
  long getPrevBlockOffset() {
    return prevBlockOffset;
  }

  /**
   * Rewinds {@code buf} and writes first 4 header fields. {@code buf} position is modified as
   * side-effect.
   */
  private void overwriteHeader() {
    bufWithoutChecksum.rewind();
    blockType.write(bufWithoutChecksum);
    bufWithoutChecksum.putInt(onDiskSizeWithoutHeader);
    bufWithoutChecksum.putInt(uncompressedSizeWithoutHeader);
    bufWithoutChecksum.putLong(prevBlockOffset);
    if (((KnobRuntime.check(java.util.UUID.fromString("6d8f4826-4e55-3dd9-a47d-c1418377416b"))) ? (isUnpacked()) : (this.fileContext.isUseHBaseChecksum()))) {
      bufWithoutChecksum.put(fileContext.getChecksumType().getCode());
      if (KnobRuntime.check(java.util.UUID.fromString("1f36d2d2-366c-3a3e-bc61-514f0479f25e"))) { bufWithoutChecksum.putInt(headerSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("0aa246ec-615d-379c-a424-d903cb9c11a4"))) { bufWithoutChecksum.putInt(computeTotalChecksumBytes()); } else { bufWithoutChecksum.putInt(fileContext.getBytesPerChecksum()); }
      bufWithoutChecksum.putInt(onDiskDataSizeWithHeader);
    }
  }

  /**
   * Returns a buffer that does not include the header and checksum.
   * @return the buffer with header skipped and checksum omitted.
   */
  public ByteBuff getBufferWithoutHeader() {
    ByteBuff dup = getBufferReadOnly();
    return dup.position(headerSize()).slice();
  }

  /**
   * Returns a read-only duplicate of the buffer this block stores internally ready to be read.
   * Clients must not modify the buffer object though they may set position and limit on the
   * returned buffer since we pass back a duplicate. This method has to be public because it is used
   * in {@link CompoundBloomFilter} to avoid object creation on every Bloom filter lookup, but has
   * to be used with caution. Buffer holds header, block content, and any follow-on checksums if
   * present.
   * @return the buffer of this block for read-only operations,the buffer includes header,but not
   *         checksum.
   */
  public ByteBuff getBufferReadOnly() {
    // TODO: ByteBuf does not support asReadOnlyBuffer(). Fix.
    ByteBuff dup = this.bufWithoutChecksum.duplicate();
    assert dup.position() == 0;
    return dup;
  }

  public ByteBuffAllocator getByteBuffAllocator() {
    return this.allocator;
  }

  private void sanityCheckAssertion(long valueFromBuf, long valueFromField, String fieldName)
    throws IOException {
    if (valueFromBuf != valueFromField) {
      throw new AssertionError(fieldName + " in the buffer (" + valueFromBuf
        + ") is different from that in the field (" + valueFromField + ")");
    }
  }

  private void sanityCheckAssertion(BlockType valueFromBuf, BlockType valueFromField)
    throws IOException {
    if (valueFromBuf != valueFromField) {
      throw new IOException("Block type stored in the buffer: " + valueFromBuf
        + ", block type field: " + valueFromField);
    }
  }

  /**
   * Checks if the block is internally consistent, i.e. the first
   * {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes of the buffer contain a valid header consistent
   * with the fields. Assumes a packed block structure. This function is primary for testing and
   * debugging, and is not thread-safe, because it alters the internal buffer pointer. Used by tests
   * only.
   */
  void sanityCheck() throws IOException {
    // Duplicate so no side-effects
    ByteBuff dup = this.bufWithoutChecksum.duplicate().rewind();
    sanityCheckAssertion(BlockType.read(dup), blockType);

    sanityCheckAssertion(dup.getInt(), onDiskSizeWithoutHeader, "onDiskSizeWithoutHeader");

    sanityCheckAssertion(dup.getInt(), uncompressedSizeWithoutHeader,
      "uncompressedSizeWithoutHeader");

    sanityCheckAssertion(dup.getLong(), prevBlockOffset, "prevBlockOffset");
    if (this.fileContext.isUseHBaseChecksum()) {
      sanityCheckAssertion(dup.get(), this.fileContext.getChecksumType().getCode(), "checksumType");
      sanityCheckAssertion(dup.getInt(), this.fileContext.getBytesPerChecksum(),
        "bytesPerChecksum");
      sanityCheckAssertion(dup.getInt(), onDiskDataSizeWithHeader, "onDiskDataSizeWithHeader");
    }

    if (dup.limit() != onDiskDataSizeWithHeader) {
      throw new AssertionError(
        "Expected limit " + onDiskDataSizeWithHeader + ", got " + dup.limit());
    }

    // We might optionally allocate HFILEBLOCK_HEADER_SIZE more bytes to read the next
    // block's header, so there are two sensible values for buffer capacity.
    int hdrSize = headerSize();
    dup.rewind();
    if (
      dup.remaining() != onDiskDataSizeWithHeader
        && dup.remaining() != onDiskDataSizeWithHeader + hdrSize
    ) {
      throw new AssertionError("Invalid buffer capacity: " + dup.remaining() + ", expected "
        + onDiskDataSizeWithHeader + " or " + (onDiskDataSizeWithHeader + hdrSize));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("[").append("blockType=").append(blockType)
      .append(", fileOffset=").append(offset).append(", headerSize=").append(headerSize())
      .append(", onDiskSizeWithoutHeader=").append(onDiskSizeWithoutHeader)
      .append(", uncompressedSizeWithoutHeader=").append(uncompressedSizeWithoutHeader)
      .append(", prevBlockOffset=").append(prevBlockOffset).append(", isUseHBaseChecksum=")
      .append(fileContext.isUseHBaseChecksum());
    if (fileContext.isUseHBaseChecksum()) {
      sb.append(", checksumType=").append(ChecksumType.codeToType(this.bufWithoutChecksum.get(24)))
        .append(", bytesPerChecksum=").append(this.bufWithoutChecksum.getInt(24 + 1))
        .append(", onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader);
    } else {
      sb.append(", onDiskDataSizeWithHeader=").append(onDiskDataSizeWithHeader).append("(")
        .append(onDiskSizeWithoutHeader).append("+")
        .append(HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM).append(")");
    }
    String dataBegin;
    if (bufWithoutChecksum.hasArray()) {
      dataBegin = Bytes.toStringBinary(bufWithoutChecksum.array(),
        bufWithoutChecksum.arrayOffset() + headerSize(),
        Math.min(32, bufWithoutChecksum.limit() - bufWithoutChecksum.arrayOffset() - headerSize()));
    } else {
      ByteBuff bufWithoutHeader = getBufferWithoutHeader();
      byte[] dataBeginBytes =
        new byte[Math.min(32, bufWithoutHeader.limit() - bufWithoutHeader.position())];
      bufWithoutHeader.get(dataBeginBytes);
      dataBegin = Bytes.toStringBinary(dataBeginBytes);
    }
    sb.append(", getOnDiskSizeWithHeader=").append(getOnDiskSizeWithHeader())
      .append(", totalChecksumBytes=").append(totalChecksumBytes()).append(", isUnpacked=")
      .append(isUnpacked()).append(", buf=[").append(bufWithoutChecksum).append("]")
      .append(", dataBeginsWith=").append(dataBegin).append(", fileContext=").append(fileContext)
      .append(", nextBlockOnDiskSize=").append(nextBlockOnDiskSize).append("]");
    return sb.toString();
  }

  /**
   * Retrieves the decompressed/decrypted view of this block. An encoded block remains in its
   * encoded structure. Internal structures are shared between instances where applicable.
   */
  HFileBlock unpack(HFileContext fileContext, FSReader reader) throws IOException {
    if (!fileContext.isCompressedOrEncrypted()) {
      // TODO: cannot use our own fileContext here because HFileBlock(ByteBuffer, boolean),
      // which is used for block serialization to L2 cache, does not preserve encoding and
      // encryption details.
      return this;
    }

    ByteBuff newBuf = allocateBufferForUnpacking(); // allocates space for the decompressed block
    HFileBlock unpacked = shallowClone(this, newBuf);

    boolean succ = false;
    final Context context =
      Context.current().with(CONTEXT_KEY, new HFileContextAttributesBuilderConsumer(fileContext));
    try (Scope ignored = context.makeCurrent()) {
      HFileBlockDecodingContext ctx = blockType == BlockType.ENCODED_DATA
        ? reader.getBlockDecodingContext()
        : reader.getDefaultBlockDecodingContext();
      // Create a duplicated buffer without the header part.
      int headerSize = this.headerSize();
      ByteBuff dup = this.bufWithoutChecksum.duplicate();
      dup.position(headerSize);
      dup = dup.slice();
      // Decode the dup into unpacked#buf
      ctx.prepareDecoding(unpacked.getOnDiskDataSizeWithHeader() - headerSize,
        unpacked.getUncompressedSizeWithoutHeader(), unpacked.getBufferWithoutHeader(), dup);
      succ = true;
      return unpacked;
    } finally {
      if (!succ) {
        unpacked.release();
      }
    }
  }

  /**
   * Always allocates a new buffer of the correct size. Copies header bytes from the existing
   * buffer. Does not change header fields. Reserve room to keep checksum bytes too.
   */
  private ByteBuff allocateBufferForUnpacking() {
    int headerSize = headerSize();
    int capacityNeeded = headerSize + uncompressedSizeWithoutHeader;

    ByteBuff source = bufWithoutChecksum.duplicate();
    ByteBuff newBuf = allocator.allocate(capacityNeeded);

    // Copy header bytes into newBuf.
    source.position(0);
    newBuf.put(0, source, 0, headerSize);

    // set limit to exclude next block's header
    newBuf.limit(capacityNeeded);
    return newBuf;
  }

  /**
   * Return true when this block's buffer has been unpacked, false otherwise. Note this is a
   * calculated heuristic, not tracked attribute of the block.
   */
  public boolean isUnpacked() {
    final int headerSize = headerSize();
    final int expectedCapacity = headerSize + uncompressedSizeWithoutHeader;
    final int bufCapacity = bufWithoutChecksum.remaining();
    return bufCapacity == expectedCapacity || bufCapacity == expectedCapacity + headerSize;
  }

  /**
   * Cannot be {@link #UNSET}. Must be a legitimate value. Used re-making the {@link BlockCacheKey}
   * when block is returned to the cache.
   * @return the offset of this block in the file it was read from
   */
  long getOffset() {
    if (offset < 0) {
      throw new IllegalStateException("HFile block offset not initialized properly");
    }
    return offset;
  }

  /** Returns a byte stream reading the data(excluding header and checksum) of this block */
  DataInputStream getByteStream() {
    ByteBuff dup = this.bufWithoutChecksum.duplicate();
    dup.position(this.headerSize());
    return new DataInputStream(new ByteBuffInputStream(dup));
  }

  @Override
  public long heapSize() {
    long size = FIXED_OVERHEAD;
    size += fileContext.heapSize();
    if (bufWithoutChecksum != null) {
      // Deep overhead of the byte buffer. Needs to be aligned separately.
      size += ClassSize.align(bufWithoutChecksum.capacity() + MULTI_BYTE_BUFFER_HEAP_SIZE);
    }
if(KnobRuntime.check(java.util.UUID.fromString("4ba9a64c-c27a-3629-b6aa-d25de81a55bd"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a517b20f-a29b-32f0-9144-db3279a1c9af"))) {
size -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("de691bff-7b7a-3a77-af57-f40ef35d8e00"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25b95f4a-dcb3-30b8-860c-54eef8d6cb9f"))) {
size = 0;
}
    return ClassSize.align(size);
  }

  /**
   * Will be override by {@link SharedMemHFileBlock} or {@link ExclusiveMemHFileBlock}. Return true
   * by default.
   */
  public boolean isSharedMem() {
    return true;
  }

  /**
   * Unified version 2 {@link HFile} block writer. The intended usage pattern is as follows:
   * <ol>
   * <li>Construct an {@link HFileBlock.Writer}, providing a compression algorithm.
   * <li>Call {@link Writer#startWriting} and get a data stream to write to.
   * <li>Write your data into the stream.
   * <li>Call Writer#writeHeaderAndData(FSDataOutputStream) as many times as you need to. store the
   * serialized block into an external stream.
   * <li>Repeat to write more blocks.
   * </ol>
   * <p>
   */
  static class Writer implements ShipperListener {
    private enum State {
      INIT,
      WRITING,
      BLOCK_READY
    };

    private int maxSizeUnCompressed;

    private BlockCompressedSizePredicator compressedSizePredicator;

    /** Writer state. Used to ensure the correct usage protocol. */
    private State state = State.INIT;

    /** Data block encoder used for data blocks */
    private final HFileDataBlockEncoder dataBlockEncoder;

    private HFileBlockEncodingContext dataBlockEncodingCtx;

    /** block encoding context for non-data blocks */
    private HFileBlockDefaultEncodingContext defaultBlockEncodingCtx;

    /**
     * The stream we use to accumulate data into a block in an uncompressed format. We reset this
     * stream at the end of each block and reuse it. The header is written as the first
     * {@link HConstants#HFILEBLOCK_HEADER_SIZE} bytes into this stream.
     */
    private ByteArrayOutputStream baosInMemory;

    /**
     * Current block type. Set in {@link #startWriting(BlockType)}. Could be changed in
     * {@link #finishBlock()} from {@link BlockType#DATA} to {@link BlockType#ENCODED_DATA}.
     */
    private BlockType blockType;

    /**
     * A stream that we write uncompressed bytes to, which compresses them and writes them to
     * {@link #baosInMemory}.
     */
    private DataOutputStream userDataStream;

    /**
     * Bytes to be written to the file system, including the header. Compressed if compression is
     * turned on. It also includes the checksum data that immediately follows the block data.
     * (header + data + checksums)
     */
    private ByteArrayOutputStream onDiskBlockBytesWithHeader;

    /**
     * The size of the checksum data on disk. It is used only if data is not compressed. If data is
     * compressed, then the checksums are already part of onDiskBytesWithHeader. If data is
     * uncompressed, then this variable stores the checksum data for this block.
     */
    private byte[] onDiskChecksum = HConstants.EMPTY_BYTE_ARRAY;

    /**
     * Current block's start offset in the {@link HFile}. Set in
     * {@link #writeHeaderAndData(FSDataOutputStream)}.
     */
    private long startOffset;

    /**
     * Offset of previous block by block type. Updated when the next block is started.
     */
    private long[] prevOffsetByType;

    /** The offset of the previous block of the same type */
    private long prevOffset;
    /** Meta data that holds information about the hfileblock **/
    private HFileContext fileContext;

    private final ByteBuffAllocator allocator;

    @Override
    public void beforeShipped() {
      if (getEncodingState() != null) {
        getEncodingState().beforeShipped();
      }
    }

    EncodingState getEncodingState() {
      return dataBlockEncodingCtx.getEncodingState();
    }

    /**
     * @param dataBlockEncoder data block encoding algorithm to use
     */
    public Writer(Configuration conf, HFileDataBlockEncoder dataBlockEncoder,
      HFileContext fileContext) {
      this(conf, dataBlockEncoder, fileContext, ByteBuffAllocator.HEAP, fileContext.getBlocksize());
    }

    public Writer(Configuration conf, HFileDataBlockEncoder dataBlockEncoder,
      HFileContext fileContext, ByteBuffAllocator allocator, int maxSizeUnCompressed) {
      if (fileContext.getBytesPerChecksum() < HConstants.HFILEBLOCK_HEADER_SIZE) {
        throw new RuntimeException("Unsupported value of bytesPerChecksum. " + " Minimum is "
          + HConstants.HFILEBLOCK_HEADER_SIZE + " but the configured value is "
          + fileContext.getBytesPerChecksum());
      }
      this.allocator = allocator;
      this.dataBlockEncoder =
        dataBlockEncoder != null ? dataBlockEncoder : NoOpDataBlockEncoder.INSTANCE;
      this.dataBlockEncodingCtx = this.dataBlockEncoder.newDataBlockEncodingContext(conf,
        HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
      // TODO: This should be lazily instantiated
      this.defaultBlockEncodingCtx = new HFileBlockDefaultEncodingContext(conf, null,
        HConstants.HFILEBLOCK_DUMMY_HEADER, fileContext);
      // TODO: Set BAOS initial size. Use fileContext.getBlocksize() and add for header/checksum
      baosInMemory = new ByteArrayOutputStream();
      prevOffsetByType = new long[BlockType.values().length];
      for (int i = 0; i < prevOffsetByType.length; ++i) {
        prevOffsetByType[i] = UNSET;
      }
      // TODO: Why fileContext saved away when we have dataBlockEncoder and/or
      // defaultDataBlockEncoder?
      this.fileContext = fileContext;
      this.compressedSizePredicator = (BlockCompressedSizePredicator) ReflectionUtils.newInstance(
        conf.getClass(BLOCK_COMPRESSED_SIZE_PREDICATOR, UncompressedBlockSizePredicator.class),
        new Configuration(conf));
      this.maxSizeUnCompressed = maxSizeUnCompressed;
    }

    /**
     * Starts writing into the block. The previous block's data is discarded.
     * @return the stream the user can write their data into
     */
    DataOutputStream startWriting(BlockType newBlockType) throws IOException {
      if (state == State.BLOCK_READY && startOffset != -1) {
        // We had a previous block that was written to a stream at a specific
        // offset. Save that offset as the last offset of a block of that type.
        prevOffsetByType[blockType.getId()] = startOffset;
      }

      startOffset = -1;
      blockType = newBlockType;

      baosInMemory.reset();
if(KnobRuntime.check(java.util.UUID.fromString("6a4fd7c1-a9be-3bbb-9a6e-bb9a28d639e0"))) {
throw new java.io.IOException("Injected exception");
}
      baosInMemory.write(HConstants.HFILEBLOCK_DUMMY_HEADER);

      state = State.WRITING;

      // We will compress it later in finishBlock()
      userDataStream = new ByteBufferWriterDataOutputStream(baosInMemory);
      if (((KnobRuntime.check(java.util.UUID.fromString("4977f1dd-871d-3083-8803-58d47d3d3761"))) ? ((newBlockType) != (BlockType.DATA)) : (((KnobRuntime.check(java.util.UUID.fromString("fe4f6a8f-99c1-3cca-825c-d086187219ac"))) ? ((newBlockType) == (BlockType.DATA)) : (newBlockType == BlockType.DATA))))) {
        this.dataBlockEncoder.startBlockEncoding(dataBlockEncodingCtx, userDataStream);
      }
      return userDataStream;
    }

    /**
     * Writes the Cell to this block
     */
    void write(Cell cell) throws IOException {
      expectState(State.WRITING);
      this.dataBlockEncoder.encode(cell, dataBlockEncodingCtx, this.userDataStream);
    }

    /**
     * Transitions the block writer from the "writing" state to the "block ready" state. Does
     * nothing if a block is already finished.
     */
    void ensureBlockReady() throws IOException {
      Preconditions.checkState(state != State.INIT, "Unexpected state: " + state);

      if (state == State.BLOCK_READY) {
        return;
      }

      // This will set state to BLOCK_READY.
      finishBlock();
    }

    public boolean checkBoundariesWithPredicate() {
if(KnobRuntime.check(java.util.UUID.fromString("d881e8a0-ba68-3578-a0b5-3103bfb6b5fa"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("b5aa13db-429a-3f7b-a5be-a6a7fc01cdbb"))) {
return false;
}
      int rawBlockSize = encodedBlockSizeWritten();
      if (((KnobRuntime.check(java.util.UUID.fromString("7f4edc0b-c0ae-329d-9a2b-6e8c8587bae3"))) ? ((rawBlockSize) != (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("89c6f46f-baff-35bb-809c-5927fd18bef0"))) ? ((encodedBlockSizeWritten()) == (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("b024443f-1d7c-3d46-9b64-9d5ed281cb8e"))) ? ((encodedBlockSizeWritten()) > (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("f1f7e6ed-40c0-39d5-b5f6-97168747a85d"))) ? ((encodedBlockSizeWritten()) <= (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("d9d67033-04a5-36e8-85e0-5fc229a1caf7"))) ? ((encodedBlockSizeWritten()) < (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("c18b702c-b101-3a2e-9435-13f9532b5e7a"))) ? ((rawBlockSize) <= (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("7f0c3646-ae72-3c17-9496-68a76610701a"))) ? ((rawBlockSize) <= (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("78e05c15-b89d-39d2-bed8-3612ac0063bd"))) ? ((encodedBlockSizeWritten()) == (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("ec7abbf7-af2e-308a-bffe-4db718511db8"))) ? ((rawBlockSize) == (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("48644dac-b793-3dfa-8de3-39fdd428b38a"))) ? ((rawBlockSize) > (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("04044517-e19c-3481-a439-f8665061cb58"))) ? ((rawBlockSize) > (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("58087357-df81-354f-a68a-cd57cfd0d68a"))) ? ((encodedBlockSizeWritten()) >= (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("f039e744-33ec-3038-9072-b1d3f276a8fd"))) ? ((rawBlockSize) >= (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("01214eab-e15d-3d61-b974-91e4eb980d72"))) ? ((encodedBlockSizeWritten()) >= (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("1d43e863-9bd5-3674-87be-2677ed51b6be"))) ? ((encodedBlockSizeWritten()) > (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("94964cf8-4f0f-3479-a562-8437fa8aa0be"))) ? ((rawBlockSize) < (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("e98718a1-a4ce-3628-9862-bbdf42f45f50"))) ? ((rawBlockSize) < (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("4e580bf6-c41b-3c83-b661-3d83529d5b2c"))) ? ((encodedBlockSizeWritten()) < (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("ff59fe86-8e72-3940-a5db-de6c451e4c95"))) ? ((rawBlockSize) >= (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("021b5d30-fe41-323a-8b14-a528c7e5d138"))) ? ((encodedBlockSizeWritten()) != (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("02c54a3d-3192-337f-9549-43c7a527d6d8"))) ? ((rawBlockSize) != (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("3035f316-d4c5-386f-a2ad-a6ff174c320b"))) ? ((rawBlockSize) == (maxSizeUnCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("f3cd0985-57b0-3bad-a7e9-91c1d8a10546"))) ? ((encodedBlockSizeWritten()) <= (encodedBlockSizeWritten())) : (((KnobRuntime.check(java.util.UUID.fromString("2aa74738-b91d-37d6-9329-2a81a4ce5ced"))) ? ((encodedBlockSizeWritten()) != (maxSizeUnCompressed)) : (rawBlockSize >= maxSizeUnCompressed))))))))))))))))))))))))))))))))))))))))))))))))) {
        return true;
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("e23adcb4-75f7-30b9-8cc4-778eb3806421"))) {
rawBlockSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d4cfd91-e91c-3159-b395-7b56afd39b1b"))) {
rawBlockSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("caaea94b-b498-3dca-a677-0922216885e9"))) {
rawBlockSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9996342d-c330-36fe-9764-aadfb816d03e"))) {
rawBlockSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ee3224db-59ba-3d92-8628-22ce515e561e"))) {
rawBlockSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b6083843-fd2b-3b05-b76e-528e5651541b"))) {
rawBlockSize -= 1;
}
        return ((KnobRuntime.check(java.util.UUID.fromString("374a5730-5d8b-38d8-a0c5-caa0d4815621"))) ? (compressedSizePredicator.shouldFinishBlock(encodedBlockSizeWritten())) : (compressedSizePredicator.shouldFinishBlock(rawBlockSize)));
      }
    }

    /**
     * Finish up writing of the block. Flushes the compressing stream (if using compression), fills
     * out the header, does any compression/encryption of bytes to flush out to disk, and manages
     * the cache on write content, if applicable. Sets block write state to "block ready".
     */
    private void finishBlock() throws IOException {
      if (blockType == BlockType.DATA) {
        this.dataBlockEncoder.endBlockEncoding(dataBlockEncodingCtx, userDataStream,
          baosInMemory.getBuffer(), blockType);
        blockType = dataBlockEncodingCtx.getBlockType();
      }
if(KnobRuntime.check(java.util.UUID.fromString("d59bd65f-b227-338c-a1e4-435374ac9a41"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("1d02f2e8-5944-32be-a158-81c6f7a58b30"))) { finishBlock(); } else if (KnobRuntime.check(java.util.UUID.fromString("a372f93f-35fd-36cf-bc0f-1f4c9b4e0cfc"))) { ensureBlockReady(); } else { userDataStream.flush(); }
      prevOffset = prevOffsetByType[blockType.getId()];

      // We need to cache the unencoded/uncompressed size before changing the block state
      int rawBlockSize = 0;
      if (((KnobRuntime.check(java.util.UUID.fromString("ec4656bf-9333-311c-897b-3eb0626b5129"))) ? ((this.getEncodingState()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ba43c2ed-48b3-3dbf-87cb-90862f1a0cfd"))) ? ((getEncodingState()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("113b82e0-0ad3-3b42-9555-9130156c4d8a"))) ? ((this.getEncodingState()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c7223dc7-1dbb-338c-a2cc-8975ed5125b4"))) ? ((getEncodingState()) == (null)) : (this.getEncodingState() != null))))))))) {
        rawBlockSize = encodedBlockSizeWritten();
      }
      // We need to set state before we can package the block up for cache-on-write. In a way, the
      // block is ready, but not yet encoded or compressed.
      state = State.BLOCK_READY;
      Bytes compressAndEncryptDat;
      if (blockType == BlockType.DATA || blockType == BlockType.ENCODED_DATA) {
        compressAndEncryptDat =
          dataBlockEncodingCtx.compressAndEncrypt(baosInMemory.getBuffer(), 0, baosInMemory.size());
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("4fdb4d9a-278c-3270-8d7a-f90e0577d034"))) {
throw new java.io.IOException("Injected exception");
}
        compressAndEncryptDat = defaultBlockEncodingCtx.compressAndEncrypt(baosInMemory.getBuffer(),
          0, baosInMemory.size());
      }
      if (compressAndEncryptDat == null) {
        compressAndEncryptDat = new Bytes(baosInMemory.getBuffer(), 0, baosInMemory.size());
      }
      if (onDiskBlockBytesWithHeader == null) {
        onDiskBlockBytesWithHeader = new ByteArrayOutputStream(compressAndEncryptDat.getLength());
      }
      onDiskBlockBytesWithHeader.reset();
      onDiskBlockBytesWithHeader.write(compressAndEncryptDat.get(),
        compressAndEncryptDat.getOffset(), compressAndEncryptDat.getLength());
      // Update raw and compressed sizes in the predicate
if(KnobRuntime.check(java.util.UUID.fromString("aafa4325-696f-3d5d-b68f-8bc1457fc6a6"))) {
rawBlockSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a11f445f-0557-3052-ad91-0dafc92e44cb"))) {
rawBlockSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a1007569-e098-3fc0-9fdb-c90d3b9b7d1b"))) {
rawBlockSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b410e060-40e1-3718-b0c8-2f09f0ffc623"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("603382e0-0933-3ef9-b0d2-fbc83c2d0a4a"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25b55240-a11d-3968-8073-e9e600a293c3"))) {
rawBlockSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1dd3d016-cb27-33d8-aac1-2843d7df3494"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bd43abb5-b4be-39d0-96e8-b6929113b2a8"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c69016ee-b436-32f1-8a04-8aac47648327"))) {
rawBlockSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("56498784-ecab-352d-a824-6ce420a41470"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("92f5d6fa-105c-32d0-99fd-344608cad078"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2933700-9872-3c25-8298-954b4542e1e2"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5ce1628-a6c6-30d2-a2a2-3bd8afd00b68"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("usesHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(fileContext);
    field.set(fileContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf980d2d-c1ae-303c-bb12-42c280e7c248"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82e77f5a-34b6-315d-9381-022c406a891f"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c675ff2-4518-3936-b523-894f2328c18f"))) {
rawBlockSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d1c9e566-b399-35a4-955f-349591b38120"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      compressedSizePredicator.updateLatestBlockSizes(fileContext, rawBlockSize,
        onDiskBlockBytesWithHeader.size());

      // Calculate how many bytes we need for checksum on the tail of the block.
      int numBytes = (int) ChecksumUtil.numBytes(onDiskBlockBytesWithHeader.size(),
        fileContext.getBytesPerChecksum());

      // Put the header for the on disk bytes; header currently is unfilled-out
if(KnobRuntime.check(java.util.UUID.fromString("0551b3f4-5ce1-3376-9526-8e39e2210d83"))) {
try {
    java.lang.reflect.Field field = onDiskBlockBytesWithHeader.getClass().getDeclaredField("pos");
    field.setAccessible(true);
    int oldValue = ((int)field.get(onDiskBlockBytesWithHeader));
    field.set(onDiskBlockBytesWithHeader, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2c6d3ef-8fa5-3c51-bc0d-d982f2f7192b"))) {
try {
    java.lang.reflect.Field field = onDiskBlockBytesWithHeader.getClass().getDeclaredField("pos");
    field.setAccessible(true);
    int oldValue = ((int)field.get(onDiskBlockBytesWithHeader));
    field.set(onDiskBlockBytesWithHeader, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ee431c4-6973-3f1d-a9e1-51e2199ab5e8"))) {
try {
    java.lang.reflect.Field field = onDiskBlockBytesWithHeader.getClass().getDeclaredField("pos");
    field.setAccessible(true);
    int oldValue = ((int)field.get(onDiskBlockBytesWithHeader));
    field.set(onDiskBlockBytesWithHeader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51f00572-2598-33c5-a296-da5fe04c2a3b"))) {
try {
    java.lang.reflect.Field field = onDiskBlockBytesWithHeader.getClass().getDeclaredField("pos");
    field.setAccessible(true);
    int oldValue = ((int)field.get(onDiskBlockBytesWithHeader));
    field.set(onDiskBlockBytesWithHeader, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      putHeader(onDiskBlockBytesWithHeader, onDiskBlockBytesWithHeader.size() + numBytes,
        baosInMemory.size(), onDiskBlockBytesWithHeader.size());

      if (onDiskChecksum.length != numBytes) {
        onDiskChecksum = new byte[numBytes];
      }
      ChecksumUtil.generateChecksums(onDiskBlockBytesWithHeader.getBuffer(), 0,
        onDiskBlockBytesWithHeader.size(), onDiskChecksum, 0, fileContext.getChecksumType(),
        fileContext.getBytesPerChecksum());
    }

    /**
     * Put the header into the given byte array at the given offset.
     * @param onDiskSize       size of the block on disk header + data + checksum
     * @param uncompressedSize size of the block after decompression (but before optional data block
     *                         decoding) including header
     * @param onDiskDataSize   size of the block on disk with header and data but not including the
     *                         checksums
     */
    private void putHeader(byte[] dest, int offset, int onDiskSize, int uncompressedSize,
      int onDiskDataSize) {
      offset = blockType.put(dest, offset);
      offset = Bytes.putInt(dest, offset, onDiskSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      offset = Bytes.putInt(dest, offset, uncompressedSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      offset = Bytes.putLong(dest, offset, prevOffset);
if(KnobRuntime.check(java.util.UUID.fromString("e46d127a-b4de-3c69-bd3e-58432d483c6e"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2e202953-674a-3d30-81a4-641b8a0b0952"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("59ebb3eb-9bac-3be5-8cf7-4f112be9f2e3"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("45446733-e8fb-37bd-8e78-c5520c1620ec"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("51456df5-954e-3ed1-8867-fe8d21a4f851"))) {
offset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1fbfc8a7-5d2a-3885-9458-f9d962e038aa"))) {
offset += 1;
}
      offset = Bytes.putByte(dest, offset, fileContext.getChecksumType().getCode());
if(KnobRuntime.check(java.util.UUID.fromString("c7377dba-578f-345b-a5df-b9440b926a78"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f4fc2ae3-72a5-3296-9478-d5e19fb9a5d3"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("be6b6675-f518-3f99-bf31-38a983789cc8"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3153d903-0a7c-3075-907b-bc4c26329adb"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("db9df04d-43df-3982-922a-6fa3b23849be"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f543f018-e4f2-378d-a254-e37c1dee1e0c"))) {
offset *= 2;
}
      offset = Bytes.putInt(dest, offset, fileContext.getBytesPerChecksum());
if(KnobRuntime.check(java.util.UUID.fromString("c09cdd0e-d01e-307b-b913-84dcc60a3386"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("856023ca-b9b9-361c-b31e-62da6807b0bb"))) {
onDiskDataSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("69aaf64b-2446-3984-b5de-9bad9784cd61"))) {
onDiskDataSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3b15f3d5-3932-3e1e-acd9-2d563cdf70fd"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2bf9d3b4-cbd8-3384-80e7-e8a258ea0eb1"))) {
onDiskDataSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cacd258-3242-33eb-b7ce-0850634d4d91"))) {
onDiskDataSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("46355958-ebf0-3578-bda9-bc72ece14020"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a1966f9-052a-3be8-aad0-702489283c9a"))) {
onDiskDataSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d1c166b-142c-3dd1-b825-77d98e0e85d8"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8ddcbc74-fbbc-32c7-b2bb-91ac4433c08f"))) {
offset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("67beccfc-aa65-3b5c-8b4c-bde8069a242a"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2152b6f-5fc3-3ccb-b161-214b6b441585"))) {
onDiskDataSize = -1;
}
      Bytes.putInt(dest, offset, onDiskDataSize);
    }

    private void putHeader(ByteBuff buff, int onDiskSize, int uncompressedSize,
      int onDiskDataSize) {
if(KnobRuntime.check(java.util.UUID.fromString("de3ccc37-a593-34e6-bffe-33d1da4fcc4d"))) {
return;
}
      if (KnobRuntime.check(java.util.UUID.fromString("9fcd0646-9ef6-3b95-a310-c41f81c29531"))) { cloneUncompressedBufferWithHeader(); } else if (KnobRuntime.check(java.util.UUID.fromString("3ff758b4-fa08-306f-bef1-3fd118697325"))) { cloneOnDiskBufferWithHeader(); } else { buff.rewind(); }
      blockType.write(buff);
      buff.putInt(onDiskSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      buff.putInt(uncompressedSize - HConstants.HFILEBLOCK_HEADER_SIZE);
      buff.putLong(prevOffset);
      buff.put(fileContext.getChecksumType().getCode());
      buff.putInt(fileContext.getBytesPerChecksum());
      buff.putInt(onDiskDataSize);
    }

    private void putHeader(ByteArrayOutputStream dest, int onDiskSize, int uncompressedSize,
      int onDiskDataSize) {
      putHeader(dest.getBuffer(), 0, onDiskSize, uncompressedSize, onDiskDataSize);
    }

    /**
     * Similar to {@link #writeHeaderAndData(FSDataOutputStream)}, but records the offset of this
     * block so that it can be referenced in the next block of the same type.
     */
    void writeHeaderAndData(FSDataOutputStream out) throws IOException {
      long offset = out.getPos();
      if (startOffset != UNSET && offset != startOffset) {
        throw new IOException("A " + blockType + " block written to a "
          + "stream twice, first at offset " + startOffset + ", then at " + offset);
      }
      startOffset = offset;
      finishBlockAndWriteHeaderAndData(out);
    }

    /**
     * Writes the header and the compressed data of this block (or uncompressed data when not using
     * compression) into the given stream. Can be called in the "writing" state or in the "block
     * ready" state. If called in the "writing" state, transitions the writer to the "block ready"
     * state.
     * @param out the output stream to write the
     */
    protected void finishBlockAndWriteHeaderAndData(DataOutputStream out) throws IOException {
      ensureBlockReady();
      long startTime = EnvironmentEdgeManager.currentTime();
      out.write(onDiskBlockBytesWithHeader.getBuffer(), 0, onDiskBlockBytesWithHeader.size());
      out.write(onDiskChecksum);
      if (KnobRuntime.check(java.util.UUID.fromString("7f600eae-f05c-3216-980c-5c615d6b6d43"))) { HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("80fc3521-7181-3b38-b8cb-8fb89c08a80a"))) { HFile.updateWriteLatency((EnvironmentEdgeManager.currentTime()) - (startTime)); } else if (KnobRuntime.check(java.util.UUID.fromString("433d5caa-8dc8-3bec-8661-2ef5ee26aaed"))) { HFile.updateWriteLatency(startTime); } else { HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime() - startTime); }
    }

    /**
     * Returns the header or the compressed data (or uncompressed data when not using compression)
     * as a byte array. Can be called in the "writing" state or in the "block ready" state. If
     * called in the "writing" state, transitions the writer to the "block ready" state. This
     * returns the header + data + checksums stored on disk.
     * @return header and data as they would be stored on disk in a byte array
     */
    byte[] getHeaderAndDataForTest() throws IOException {
      ensureBlockReady();
      // This is not very optimal, because we are doing an extra copy.
      // But this method is used only by unit tests.
      byte[] output = new byte[onDiskBlockBytesWithHeader.size() + onDiskChecksum.length];
      System.arraycopy(onDiskBlockBytesWithHeader.getBuffer(), 0, output, 0,
        onDiskBlockBytesWithHeader.size());
      System.arraycopy(onDiskChecksum, 0, output, onDiskBlockBytesWithHeader.size(),
        onDiskChecksum.length);
      return output;
    }

    /**
     * Releases resources used by this writer.
     */
    void release() {
      if (dataBlockEncodingCtx != null) {
        dataBlockEncodingCtx.close();
        dataBlockEncodingCtx = null;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("1f31be29-f14d-3a53-8483-31b04a413d00"))) ? ((defaultBlockEncodingCtx) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("249395dc-ebd0-3b5e-afd7-8a9f1a6db4b9"))) ? ((defaultBlockEncodingCtx) != (null)) : (defaultBlockEncodingCtx != null))))) {
        defaultBlockEncodingCtx.close();
        defaultBlockEncodingCtx = null;
      }
    }

    /**
     * Returns the on-disk size of the data portion of the block. This is the compressed size if
     * compression is enabled. Can only be called in the "block ready" state. Header is not
     * compressed, and its size is not included in the return value.
     * @return the on-disk size of the block, not including the header.
     */
    int getOnDiskSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBlockBytesWithHeader.size() + onDiskChecksum.length
        - HConstants.HFILEBLOCK_HEADER_SIZE;
    }

    /**
     * Returns the on-disk size of the block. Can only be called in the "block ready" state.
     * @return the on-disk size of the block ready to be written, including the header size, the
     *         data and the checksum data.
     */
    int getOnDiskSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return onDiskBlockBytesWithHeader.size() + onDiskChecksum.length;
    }

    /**
     * The uncompressed size of the block data. Does not include header size.
     */
    int getUncompressedSizeWithoutHeader() {
      expectState(State.BLOCK_READY);
      return baosInMemory.size() - HConstants.HFILEBLOCK_HEADER_SIZE;
    }

    /**
     * The uncompressed size of the block data, including header size.
     */
    public int getUncompressedSizeWithHeader() {
      expectState(State.BLOCK_READY);
      return baosInMemory.size();
    }

    /** Returns true if a block is being written */
    boolean isWriting() {
      return state == State.WRITING;
    }

    /**
     * Returns the number of bytes written into the current block so far, or zero if not writing the
     * block at the moment. Note that this will return zero in the "block ready" state as well.
     * @return the number of bytes written
     */
    public int encodedBlockSizeWritten() {
      return state != State.WRITING ? 0 : this.getEncodingState().getEncodedDataSizeWritten();
    }

    /**
     * Returns the number of bytes written into the current block so far, or zero if not writing the
     * block at the moment. Note that this will return zero in the "block ready" state as well.
     * @return the number of bytes written
     */
    public int blockSizeWritten() {
      return state != State.WRITING ? 0 : this.getEncodingState().getUnencodedDataSizeWritten();
    }

    /**
     * Clones the header followed by the uncompressed data, even if using compression. This is
     * needed for storing uncompressed blocks in the block cache. Can be called in the "writing"
     * state or the "block ready" state. Returns only the header and data, does not include checksum
     * data.
     * @return Returns an uncompressed block ByteBuff for caching on write
     */
    ByteBuff cloneUncompressedBufferWithHeader() {
      expectState(State.BLOCK_READY);
      ByteBuff bytebuff = allocator.allocate(baosInMemory.size());
      baosInMemory.toByteBuff(bytebuff);
      int numBytes = (int) ChecksumUtil.numBytes(onDiskBlockBytesWithHeader.size(),
        fileContext.getBytesPerChecksum());
      putHeader(bytebuff, onDiskBlockBytesWithHeader.size() + numBytes, baosInMemory.size(),
        onDiskBlockBytesWithHeader.size());
      bytebuff.rewind();
      return bytebuff;
    }

    /**
     * Clones the header followed by the on-disk (compressed/encoded/encrypted) data. This is needed
     * for storing packed blocks in the block cache. Returns only the header and data, Does not
     * include checksum data.
     * @return Returns a copy of block bytes for caching on write
     */
    private ByteBuff cloneOnDiskBufferWithHeader() {
      expectState(State.BLOCK_READY);
      ByteBuff bytebuff = allocator.allocate(onDiskBlockBytesWithHeader.size());
      onDiskBlockBytesWithHeader.toByteBuff(bytebuff);
      bytebuff.rewind();
      return bytebuff;
    }

    private void expectState(State expectedState) {
      if (state != expectedState) {
        throw new IllegalStateException(
          "Expected state: " + expectedState + ", actual state: " + state);
      }
    }

    /**
     * Takes the given {@link BlockWritable} instance, creates a new block of its appropriate type,
     * writes the writable into this block, and flushes the block into the output stream. The writer
     * is instructed not to buffer uncompressed bytes for cache-on-write.
     * @param bw  the block-writable object to write as a block
     * @param out the file system output stream
     */
    void writeBlock(BlockWritable bw, FSDataOutputStream out) throws IOException {
      bw.writeToBlock(startWriting(bw.getBlockType()));
      writeHeaderAndData(out);
    }

    /**
     * Creates a new HFileBlock. Checksums have already been validated, so the byte buffer passed
     * into the constructor of this newly created block does not have checksum data even though the
     * header minor version is MINOR_VERSION_WITH_CHECKSUM. This is indicated by setting a 0 value
     * in bytesPerChecksum. This method copies the on-disk or uncompressed data to build the
     * HFileBlock which is used only while writing blocks and caching.
     * <p>
     * TODO: Should there be an option where a cache can ask that hbase preserve block checksums for
     * checking after a block comes out of the cache? Otehrwise, cache is responsible for blocks
     * being wholesome (ECC memory or if file-backed, it does checksumming).
     */
    HFileBlock getBlockForCaching(CacheConfig cacheConf) {
      HFileContext newContext = new HFileContextBuilder().withBlockSize(fileContext.getBlocksize())
        .withBytesPerCheckSum(0).withChecksumType(ChecksumType.NULL) // no checksums in cached data
        .withCompression(fileContext.getCompression())
        .withDataBlockEncoding(fileContext.getDataBlockEncoding())
        .withHBaseCheckSum(fileContext.isUseHBaseChecksum())
        .withCompressTags(fileContext.isCompressTags())
        .withIncludesMvcc(fileContext.isIncludesMvcc())
        .withIncludesTags(fileContext.isIncludesTags())
        .withColumnFamily(fileContext.getColumnFamily()).withTableName(fileContext.getTableName())
        .build();
      // Build the HFileBlock.
      HFileBlockBuilder builder = new HFileBlockBuilder();
      ByteBuff buff;
      if (cacheConf.shouldCacheCompressed(blockType.getCategory())) {
        buff = cloneOnDiskBufferWithHeader();
      } else {
        buff = cloneUncompressedBufferWithHeader();
      }
      return builder.withBlockType(blockType)
        .withOnDiskSizeWithoutHeader(getOnDiskSizeWithoutHeader())
        .withUncompressedSizeWithoutHeader(getUncompressedSizeWithoutHeader())
        .withPrevBlockOffset(prevOffset).withByteBuff(buff).withFillHeader(FILL_HEADER)
        .withOffset(startOffset).withNextBlockOnDiskSize(UNSET)
        .withOnDiskDataSizeWithHeader(onDiskBlockBytesWithHeader.size() + onDiskChecksum.length)
        .withHFileContext(newContext).withByteBuffAllocator(cacheConf.getByteBuffAllocator())
        .withShared(!buff.hasArray()).build();
    }
  }

  /** Something that can be written into a block. */
  interface BlockWritable {
    /** The type of block this data should use. */
    BlockType getBlockType();

    /**
     * Writes the block to the provided stream. Must not write any magic records.
     * @param out a stream to write uncompressed data into
     */
    void writeToBlock(DataOutput out) throws IOException;
  }

  /**
   * Iterator for reading {@link HFileBlock}s in load-on-open-section, such as root data index
   * block, meta index block, file info block etc.
   */
  interface BlockIterator {
    /**
     * Get the next block, or null if there are no more blocks to iterate.
     */
    HFileBlock nextBlock() throws IOException;

    /**
     * Similar to {@link #nextBlock()} but checks block type, throws an exception if incorrect, and
     * returns the HFile block
     */
    HFileBlock nextBlockWithBlockType(BlockType blockType) throws IOException;

    /**
     * Now we use the {@link ByteBuffAllocator} to manage the nio ByteBuffers for HFileBlocks, so we
     * must deallocate all of the ByteBuffers in the end life. the BlockIterator's life cycle is
     * starting from opening an HFileReader and stopped when the HFileReader#close, so we will keep
     * track all the read blocks until we call {@link BlockIterator#freeBlocks()} when closing the
     * HFileReader. Sum bytes of those blocks in load-on-open section should be quite small, so
     * tracking them should be OK.
     */
    void freeBlocks();
  }

  /** An HFile block reader with iteration ability. */
  interface FSReader {
    /**
     * Reads the block at the given offset in the file with the given on-disk size and uncompressed
     * size.
     * @param offset        of the file to read
     * @param onDiskSize    the on-disk size of the entire block, including all applicable headers,
     *                      or -1 if unknown
     * @param pread         true to use pread, otherwise use the stream read.
     * @param updateMetrics update the metrics or not.
     * @param intoHeap      allocate the block's ByteBuff by {@link ByteBuffAllocator} or JVM heap.
     *                      For LRUBlockCache, we must ensure that the block to cache is an heap
     *                      one, because the memory occupation is based on heap now, also for
     *                      {@link CombinedBlockCache}, we use the heap LRUBlockCache as L1 cache to
     *                      cache small blocks such as IndexBlock or MetaBlock for faster access. So
     *                      introduce an flag here to decide whether allocate from JVM heap or not
     *                      so that we can avoid an extra off-heap to heap memory copy when using
     *                      LRUBlockCache. For most cases, we known what's the expected block type
     *                      we'll read, while for some special case (Example:
     *                      HFileReaderImpl#readNextDataBlock()), we cannot pre-decide what's the
     *                      expected block type, then we can only allocate block's ByteBuff from
     *                      {@link ByteBuffAllocator} firstly, and then when caching it in
     *                      {@link LruBlockCache} we'll check whether the ByteBuff is from heap or
     *                      not, if not then we'll clone it to an heap one and cache it.
     * @return the newly read block
     */
    HFileBlock readBlockData(long offset, long onDiskSize, boolean pread, boolean updateMetrics,
      boolean intoHeap) throws IOException;

    /**
     * Creates a block iterator over the given portion of the {@link HFile}. The iterator returns
     * blocks starting with offset such that offset &lt;= startOffset &lt; endOffset. Returned
     * blocks are always unpacked. Used when no hfile index available; e.g. reading in the hfile
     * index blocks themselves on file open.
     * @param startOffset the offset of the block to start iteration with
     * @param endOffset   the offset to end iteration at (exclusive)
     * @return an iterator of blocks between the two given offsets
     */
    BlockIterator blockRange(long startOffset, long endOffset);

    /** Closes the backing streams */
    void closeStreams() throws IOException;

    /** Get a decoder for {@link BlockType#ENCODED_DATA} blocks from this file. */
    HFileBlockDecodingContext getBlockDecodingContext();

    /** Get the default decoder for blocks from this file. */
    HFileBlockDecodingContext getDefaultBlockDecodingContext();

    void setIncludesMemStoreTS(boolean includesMemstoreTS);

    void setDataBlockEncoder(HFileDataBlockEncoder encoder, Configuration conf);

    /**
     * To close the stream's socket. Note: This can be concurrently called from multiple threads and
     * implementation should take care of thread safety.
     */
    void unbufferStream();
  }

  /**
   * Data-structure to use caching the header of the NEXT block. Only works if next read that comes
   * in here is next in sequence in this block. When we read, we read current block and the next
   * blocks' header. We do this so we have the length of the next block to read if the hfile index
   * is not available (rare, at hfile open only).
   */
  private static class PrefetchedHeader {
    long offset = -1;
    byte[] header = new byte[HConstants.HFILEBLOCK_HEADER_SIZE];
    final ByteBuff buf = new SingleByteBuff(ByteBuffer.wrap(header, 0, header.length));

    @Override
    public String toString() {
      return "offset=" + this.offset + ", header=" + Bytes.toStringBinary(header);
    }
  }

  /**
   * Reads version 2 HFile blocks from the filesystem.
   */
  static class FSReaderImpl implements FSReader {
    /**
     * The file system stream of the underlying {@link HFile} that does or doesn't do checksum
     * validations in the filesystem
     */
    private FSDataInputStreamWrapper streamWrapper;

    private HFileBlockDecodingContext encodedBlockDecodingCtx;

    /** Default context used when BlockType != {@link BlockType#ENCODED_DATA}. */
    private final HFileBlockDefaultDecodingContext defaultDecodingCtx;

    /**
     * Cache of the NEXT header after this. Check it is indeed next blocks header before using it.
     * TODO: Review. This overread into next block to fetch next blocks header seems unnecessary
     * given we usually get the block size from the hfile index. Review!
     */
    private AtomicReference<PrefetchedHeader> prefetchedHeader =
      new AtomicReference<>(new PrefetchedHeader());

    /** The size of the file we are reading from, or -1 if unknown. */
    private long fileSize;

    /** The size of the header */
    protected final int hdrSize;

    /** The filesystem used to access data */
    private HFileSystem hfs;

    private HFileContext fileContext;
    // Cache the fileName
    private String pathName;

    private final ByteBuffAllocator allocator;

    private final Lock streamLock = new ReentrantLock();

    private final boolean isPreadAllBytes;

    private final long readWarnTime;

    /**
     * If reading block cost time in milliseconds more than the threshold, a warning will be logged.
     */
    public static final String FS_READER_WARN_TIME_MS = "hbase.fs.reader.warn.time.ms";

    FSReaderImpl(ReaderContext readerContext, HFileContext fileContext, ByteBuffAllocator allocator,
      Configuration conf) throws IOException {
      this.fileSize = readerContext.getFileSize();
      this.hfs = readerContext.getFileSystem();
      if (((KnobRuntime.check(java.util.UUID.fromString("a17b807f-38e6-3548-a7b3-37fd2b35945f"))) ? ((readerContext.getFilePath()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("145d1bbc-d7ad-3104-bc70-a37de3b5e866"))) ? ((readerContext.getFilePath()) != (null)) : (readerContext.getFilePath() != null))))) {
        this.pathName = readerContext.getFilePath().toString();
      }
      this.fileContext = fileContext;
      this.hdrSize = headerSize(fileContext.isUseHBaseChecksum());
      this.allocator = allocator;

      this.streamWrapper = readerContext.getInputStreamWrapper();
      // Older versions of HBase didn't support checksum.
      this.streamWrapper.prepareForBlockReader(!fileContext.isUseHBaseChecksum());
      defaultDecodingCtx = new HFileBlockDefaultDecodingContext(conf, fileContext);
      encodedBlockDecodingCtx = defaultDecodingCtx;
      isPreadAllBytes = readerContext.isPreadAllBytes();
      // Default warn threshold set to -1, it means skipping record the read block slow warning log.
      readWarnTime = conf.getLong(FS_READER_WARN_TIME_MS, -1L);
    }

    @Override
    public BlockIterator blockRange(final long startOffset, final long endOffset) {
      final FSReader owner = this; // handle for inner class
      return new BlockIterator() {
        private volatile boolean freed = false;
        // Tracking all read blocks until we call freeBlocks.
        private List<HFileBlock> blockTracker = new ArrayList<>();
        private long offset = startOffset;
        // Cache length of next block. Current block has the length of next block in it.
        private long length = -1;

        @Override
        public HFileBlock nextBlock() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b4032b43-929a-31ee-8381-df5eb0edde80"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("cba927a9-df82-3a2f-a6b5-99da00137d11"))) {
return null;
}
          if (offset >= endOffset) {
            return null;
          }
if(KnobRuntime.check(java.util.UUID.fromString("a79264e3-9738-307a-b3df-4cc7b3215e70"))) {
throw new java.io.IOException("Injected exception");
}
          HFileBlock b = readBlockData(offset, length, false, false, true);
          offset += b.getOnDiskSizeWithHeader();
          length = b.getNextBlockOnDiskSize();
          HFileBlock uncompressed = b.unpack(fileContext, owner);
          if (uncompressed != b) {
            b.release(); // Need to release the compressed Block now.
          }
          blockTracker.add(uncompressed);
          return uncompressed;
        }

        @Override
        public HFileBlock nextBlockWithBlockType(BlockType blockType) throws IOException {
          HFileBlock blk = nextBlock();
          if (blk.getBlockType() != blockType) {
            throw new IOException(
              "Expected block of type " + blockType + " but found " + blk.getBlockType());
          }
          return blk;
        }

        @Override
        public void freeBlocks() {
if(KnobRuntime.check(java.util.UUID.fromString("e8d9e717-9772-35fa-bde9-895eebed16c9"))) {
return;
}
          if (freed) {
            return;
          }
          blockTracker.forEach(HFileBlock::release);
          blockTracker = null;
          freed = true;
        }
      };
    }

    /**
     * Does a positional read or a seek and read into the given byte buffer. We need take care that
     * we will call the {@link ByteBuff#release()} for every exit to deallocate the ByteBuffers,
     * otherwise the memory leak may happen.
     * @param dest              destination buffer
     * @param size              size of read
     * @param peekIntoNextBlock whether to read the next block's on-disk size
     * @param fileOffset        position in the stream to read at
     * @param pread             whether we should do a positional read
     * @param istream           The input source of data
     * @return true to indicate the destination buffer include the next block header, otherwise only
     *         include the current block data without the next block header.
     * @throws IOException if any IO error happen.
     */
    protected boolean readAtOffset(FSDataInputStream istream, ByteBuff dest, int size,
      boolean peekIntoNextBlock, long fileOffset, boolean pread) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6a273dcf-284e-3f80-bbe9-497cc63cb53b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("146fc405-cc25-3b00-a1b7-66efe266aaed"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("99805c2f-1054-3eab-a3e8-4330dc592997"))) {
return false;
}
      if (!pread) {
        // Seek + read. Better for scanning.
        istream.seek(fileOffset);
        long realOffset = istream.getPos();
        if (realOffset != fileOffset) {
          throw new IOException("Tried to seek to " + fileOffset + " to read " + size
            + " bytes, but pos=" + realOffset + " after seek");
        }
        if (!peekIntoNextBlock) {
          BlockIOUtils.readFully(dest, istream, size);
          return false;
        }

        // Try to read the next block header
        if (!BlockIOUtils.readWithExtra(dest, istream, size, hdrSize)) {
          // did not read the next block header.
          return false;
        }
      } else {
        // Positional read. Better for random reads; or when the streamLock is already locked.
        int extraSize = peekIntoNextBlock ? hdrSize : 0;
        if (
          !BlockIOUtils.preadWithExtra(dest, istream, fileOffset, size, extraSize, isPreadAllBytes)
        ) {
          // did not read the next block header.
          return false;
        }
      }
      assert peekIntoNextBlock;
      return true;
    }

    /**
     * Reads a version 2 block (version 1 blocks not supported and not expected). Tries to do as
     * little memory allocation as possible, using the provided on-disk size.
     * @param offset                the offset in the stream to read at
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including the header, or -1 if
     *                              unknown; i.e. when iterating over blocks reading in the file
     *                              metadata info.
     * @param pread                 whether to use a positional read
     * @param updateMetrics         whether to update the metrics
     * @param intoHeap              allocate ByteBuff of block from heap or off-heap.
     * @see FSReader#readBlockData(long, long, boolean, boolean, boolean) for more details about the
     *      useHeap.
     */
    @Override
    public HFileBlock readBlockData(long offset, long onDiskSizeWithHeaderL, boolean pread,
      boolean updateMetrics, boolean intoHeap) throws IOException {
      // Get a copy of the current state of whether to validate
      // hbase checksums or not for this read call. This is not
      // thread-safe but the one constraint is that if we decide
      // to skip hbase checksum verification then we are
      // guaranteed to use hdfs checksum verification.
      boolean doVerificationThruHBaseChecksum = streamWrapper.shouldUseHBaseChecksum();
if(KnobRuntime.check(java.util.UUID.fromString("0381d3bc-3b60-36e6-a270-c9268d72a778"))) {
doVerificationThruHBaseChecksum = !doVerificationThruHBaseChecksum;
}
if(KnobRuntime.check(java.util.UUID.fromString("45c269cb-6235-39a6-a097-537d43e7ae48"))) {
doVerificationThruHBaseChecksum = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("b667fc38-348a-3e86-b83c-d6bd6b988572"))) {
doVerificationThruHBaseChecksum = false;
}
      FSDataInputStream is = streamWrapper.getStream(doVerificationThruHBaseChecksum);
      final Context context = Context.current().with(CONTEXT_KEY,
        new HFileContextAttributesBuilderConsumer(fileContext)
          .setSkipChecksum(doVerificationThruHBaseChecksum)
          .setReadType(pread ? ReadType.POSITIONAL_READ : ReadType.SEEK_PLUS_READ));
      try (Scope ignored = context.makeCurrent()) {
        HFileBlock blk = readBlockDataInternal(is, offset, onDiskSizeWithHeaderL, pread,
          doVerificationThruHBaseChecksum, updateMetrics, intoHeap);
        if (blk == null) {
          HFile.LOG.warn("HBase checksum verification failed for file {} at offset {} filesize {}."
            + " Retrying read with HDFS checksums turned on...", pathName, offset, fileSize);

          if (!doVerificationThruHBaseChecksum) {
            String msg = "HBase checksum verification failed for file " + pathName + " at offset "
              + offset + " filesize " + fileSize + " but this cannot happen because doVerify is "
              + doVerificationThruHBaseChecksum;
            HFile.LOG.warn(msg);
            throw new IOException(msg); // cannot happen case here
          }
          HFile.CHECKSUM_FAILURES.increment(); // update metrics

          // If we have a checksum failure, we fall back into a mode where
          // the next few reads use HDFS level checksums. We aim to make the
          // next CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD reads avoid
          // hbase checksum verification, but since this value is set without
          // holding any locks, it can so happen that we might actually do
          // a few more than precisely this number.
          is = this.streamWrapper.fallbackToFsChecksum(CHECKSUM_VERIFICATION_NUM_IO_THRESHOLD);
          doVerificationThruHBaseChecksum = false;
          blk = readBlockDataInternal(is, offset, onDiskSizeWithHeaderL, pread,
            doVerificationThruHBaseChecksum, updateMetrics, intoHeap);
          if (blk != null) {
            HFile.LOG.warn(
              "HDFS checksum verification succeeded for file {} at offset {} filesize" + " {}",
              pathName, offset, fileSize);
          }
        }
        if (blk == null && !doVerificationThruHBaseChecksum) {
          String msg =
            "readBlockData failed, possibly due to " + "checksum verification failed for file "
              + pathName + " at offset " + offset + " filesize " + fileSize;
          HFile.LOG.warn(msg);
          throw new IOException(msg);
        }

        // If there is a checksum mismatch earlier, then retry with
        // HBase checksums switched off and use HDFS checksum verification.
        // This triggers HDFS to detect and fix corrupt replicas. The
        // next checksumOffCount read requests will use HDFS checksums.
        // The decrementing of this.checksumOffCount is not thread-safe,
        // but it is harmless because eventually checksumOffCount will be
        // a negative number.
        streamWrapper.checksumOk();
        return blk;
      }
    }

    /**
     * Check that {@code value} read from a block header seems reasonable, within a large margin of
     * error.
     * @return {@code true} if the value is safe to proceed, {@code false} otherwise.
     */
    private boolean checkOnDiskSizeWithHeader(int value) {
if(KnobRuntime.check(java.util.UUID.fromString("6a094d27-c769-3d12-9bc6-d6e863131338"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("c609c25b-e353-30f3-a6b5-b35b5506bcfe"))) {
return true;
}
      if (((KnobRuntime.check(java.util.UUID.fromString("bd8dce40-9de6-34d4-9247-d88be50b2c6f"))) ? ((value) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3169fb9e-172d-3caf-9546-6aff0ead81f3"))) ? ((value) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e813a164-d435-3472-a68f-b1294d4f7f1d"))) ? ((value) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e05b0ec1-0d54-399a-8a3a-5f05cf8f9faf"))) ? ((value) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("caa0da6f-e5e3-3c8a-80ba-6caca9eaf652"))) ? ((value) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e543f410-1b59-3d21-8589-75b67b4b8a63"))) ? ((value) < (0)) : (value < 0))))))))))))) {
        if (LOG.isTraceEnabled()) {
if(KnobRuntime.check(java.util.UUID.fromString("3e3be8a6-94ed-3893-a7da-c8da0dc45674"))) {
value = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("76b68555-1afd-3c58-a40f-38c68ea7fbd1"))) {
value -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e70f3e3-785a-39d5-b2c2-12d8a55636da"))) {
value = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb699070-7d28-3338-a4f5-3e776a51d23c"))) {
value += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ffe03433-e6b0-327f-977c-50b1a92e99f0"))) {
value /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b564fa3b-6328-3f48-9838-a6b1ea6e8e91"))) {
value *= 2;
}
          LOG.trace(
            "onDiskSizeWithHeader={}; value represents a size, so it should never be negative.",
            value);
        }
        return false;
      }
      if (value - hdrSize < 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("onDiskSizeWithHeader={}, hdrSize={}; don't accept a value that is negative"
            + " after the header size is excluded.", value, hdrSize);
        }
        return false;
      }
      return true;
    }

    /**
     * Check that {@code value} provided by the calling context seems reasonable, within a large
     * margin of error.
     * @return {@code true} if the value is safe to proceed, {@code false} otherwise.
     */
    private boolean checkCallerProvidedOnDiskSizeWithHeader(long value) {
      // same validation logic as is used by Math.toIntExact(long)
      int intValue = (int) value;
      if (intValue != value) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("onDiskSizeWithHeaderL={}; value exceeds int size limits.", value);
        }
        return false;
      }
      if (intValue == -1) {
        // a magic value we expect to see.
        return true;
      }
      return checkOnDiskSizeWithHeader(intValue);
    }

    /**
     * Check atomic reference cache for this block's header. Cache only good if next read coming
     * through is next in sequence in the block. We read next block's header on the tail of reading
     * the previous block to save a seek. Otherwise, we have to do a seek to read the header before
     * we can pull in the block OR we have to backup the stream because we over-read (the next
     * block's header).
     * @see PrefetchedHeader
     * @return The cached block header or null if not found.
     * @see #cacheNextBlockHeader(long, ByteBuff, int, int)
     */
    private ByteBuff getCachedHeader(final long offset) {
      PrefetchedHeader ph = this.prefetchedHeader.get();
      return ph != null && ph.offset == offset ? ph.buf : null;
    }

    /**
     * Save away the next blocks header in atomic reference.
     * @see #getCachedHeader(long)
     * @see PrefetchedHeader
     */
    private void cacheNextBlockHeader(final long offset, ByteBuff onDiskBlock,
      int onDiskSizeWithHeader, int headerLength) {
      PrefetchedHeader ph = new PrefetchedHeader();
      ph.offset = offset;
      onDiskBlock.get(onDiskSizeWithHeader, ph.header, 0, headerLength);
      this.prefetchedHeader.set(ph);
    }

    /**
     * Clear the cached value when its integrity is suspect.
     */
    private void invalidateNextBlockHeader() {
      prefetchedHeader.set(null);
    }

    private int getNextBlockOnDiskSize(ByteBuff onDiskBlock, int onDiskSizeWithHeader) {
      return onDiskBlock.getIntAfterPosition(onDiskSizeWithHeader + BlockType.MAGIC_LENGTH)
        + hdrSize;
    }

    private ByteBuff allocate(int size, boolean intoHeap) {
if(KnobRuntime.check(java.util.UUID.fromString("a430d5fc-ec81-3292-829b-3e7f0f107cf5"))) {
size *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("47b5ccd8-e962-3928-8697-a4cde6749680"))) {
size /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("725f299e-8d26-382b-8b41-fb0dc6b2ae89"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("80ef472c-817f-3e85-b0ef-692928c1fb00"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9f642a63-6cb8-3aa8-9edf-8fe69db8cc83"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("467e7a7b-8d4e-36b9-8d78-62f78003ddc1"))) {
size -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("093bc2cd-d6ea-3b2f-9faf-f43422d6e09e"))) {
size = 0;
}
      return intoHeap ? HEAP.allocate(size) : allocator.allocate(size);
    }

    /**
     * Reads a version 2 block.
     * @param offset                the offset in the stream to read at.
     * @param onDiskSizeWithHeaderL the on-disk size of the block, including the header and
     *                              checksums if present or -1 if unknown (as a long). Can be -1 if
     *                              we are doing raw iteration of blocks as when loading up file
     *                              metadata; i.e. the first read of a new file. Usually non-null
     *                              gotten from the file index.
     * @param pread                 whether to use a positional read
     * @param verifyChecksum        Whether to use HBase checksums. If HBase checksum is switched
     *                              off, then use HDFS checksum. Can also flip on/off reading same
     *                              file if we hit a troublesome patch in an hfile.
     * @param updateMetrics         whether need to update the metrics.
     * @param intoHeap              allocate the ByteBuff of block from heap or off-heap.
     * @return the HFileBlock or null if there is a HBase checksum mismatch
     */
    protected HFileBlock readBlockDataInternal(FSDataInputStream is, long offset,
      long onDiskSizeWithHeaderL, boolean pread, boolean verifyChecksum, boolean updateMetrics,
      boolean intoHeap) throws IOException {
      final Span span = Span.current();
      final AttributesBuilder attributesBuilder = Attributes.builder();
      Optional.of(Context.current()).map(val -> val.get(CONTEXT_KEY))
        .ifPresent(c -> c.accept(attributesBuilder));
      if (offset < 0) {
        throw new IOException("Invalid offset=" + offset + " trying to read " + "block (onDiskSize="
          + onDiskSizeWithHeaderL + ")");
      }
      if (!checkCallerProvidedOnDiskSizeWithHeader(onDiskSizeWithHeaderL)) {
        LOG.trace("Caller provided invalid onDiskSizeWithHeaderL={}", onDiskSizeWithHeaderL);
        onDiskSizeWithHeaderL = -1;
      }
      int onDiskSizeWithHeader = (int) onDiskSizeWithHeaderL;

      // Try to use the cached header. Will serve us in rare case where onDiskSizeWithHeaderL==-1
      // and will save us having to seek the stream backwards to reread the header we
      // read the last time through here.
if(KnobRuntime.check(java.util.UUID.fromString("069b470e-bf3e-39a0-bf43-d7373147f035"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a6c8101f-4bf1-3130-9a58-08d1ffbd5e90"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("867e56b3-1046-3dff-bc6b-11fe8e0e0072"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c94d9eb8-029e-348f-94f8-897b8d243040"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8169200a-b6fd-3890-94f9-d831075474ba"))) {
offset = 0;
}
      ByteBuff headerBuf = getCachedHeader(offset);
      LOG.trace(
        "Reading {} at offset={}, pread={}, verifyChecksum={}, cachedHeader={}, "
          + "onDiskSizeWithHeader={}",
        this.fileContext.getHFileName(), offset, pread, verifyChecksum, headerBuf,
        onDiskSizeWithHeader);
      // This is NOT same as verifyChecksum. This latter is whether to do hbase
      // checksums. Can change with circumstances. The below flag is whether the
      // file has support for checksums (version 2+).
      boolean checksumSupport = this.fileContext.isUseHBaseChecksum();
      long startTime = EnvironmentEdgeManager.currentTime();
      if (onDiskSizeWithHeader == -1) {
        // The caller does not know the block size. Need to get it from the header. If header was
        // not cached (see getCachedHeader above), need to seek to pull it in. This is costly
        // and should happen very rarely. Currently happens on open of a hfile reader where we
        // read the trailer blocks to pull in the indices. Otherwise, we are reading block sizes
        // out of the hfile index. To check, enable TRACE in this file and you'll get an exception
        // in a LOG every time we seek. See HBASE-17072 for more detail.
        if (((KnobRuntime.check(java.util.UUID.fromString("b8e35b25-e3bc-3ac8-8f74-3e06a58c6e37"))) ? ((headerBuf.rewind()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("49a5035c-690d-3a9f-a87f-5e99dadde1dc"))) ? ((headerBuf.rewind()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1981fc02-c4bc-3f98-9bd0-ba87a1217784"))) ? ((headerBuf) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("caeab590-b6a8-3d5e-81de-8c6abbf5eaab"))) ? ((headerBuf) == (null)) : (headerBuf == null))))))))) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Extra seek to get block size!", new RuntimeException());
          }
          span.addEvent("Extra seek to get block size!", attributesBuilder.build());
          headerBuf = HEAP.allocate(hdrSize);
if(KnobRuntime.check(java.util.UUID.fromString("51a36312-032e-3a83-8b9f-fe5bc789454c"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("abdd266a-3fef-37b2-8f1a-43a77c280e42"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9c5b3b5c-7432-3d40-a822-bc092c903215"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd8c069a-fefb-386c-9a71-cb86b7b027fb"))) {
pread = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("bfae86a4-02fe-37ce-9057-d174884b06e6"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("46882e3b-02f4-3e21-8c84-1b4665084dd3"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eecf7aa4-fbbd-3e4c-a03d-672b84cfc64e"))) {
pread = !pread;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ea8197f-71e7-31dc-85e9-9ef56da5f31a"))) {
pread = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("ddd62bfa-5f5e-3df1-b89b-b49af0ae79ee"))) {
offset -= 1;
}
          if (KnobRuntime.check(java.util.UUID.fromString("36cb816a-eaa0-31da-a0f9-779d99dea348"))) { readAtOffset(is, headerBuf, 1, false, offset, pread); } else if (KnobRuntime.check(java.util.UUID.fromString("5cd2a3ae-f274-3258-913f-9192e0e22173"))) { readAtOffset(is, headerBuf, 0, false, offset, pread); } else if (KnobRuntime.check(java.util.UUID.fromString("20acbd1e-c71d-3cd6-b816-014235f003fc"))) { readAtOffset(is, headerBuf, hdrSize, false, offset, true); } else if (KnobRuntime.check(java.util.UUID.fromString("8cb7b80c-f2f3-3287-81da-b147f882b166"))) { readAtOffset(is, headerBuf.rewind(), hdrSize, false, offset, pread); } else if (KnobRuntime.check(java.util.UUID.fromString("d616f243-3557-3244-99b7-f1348f9f84a7"))) { readAtOffset(is, headerBuf, hdrSize, false, offset, false); } else if (KnobRuntime.check(java.util.UUID.fromString("18bcf8f3-7874-3942-be03-a03ee6d1d3c8"))) { readAtOffset(is, headerBuf, -1, false, offset, pread); } else if (KnobRuntime.check(java.util.UUID.fromString("9caf0299-7fac-3da8-b164-2916c2a7125d"))) { readAtOffset(is, headerBuf, hdrSize, false, offset, !pread); } else { readAtOffset(is, headerBuf, hdrSize, false, offset, pread); }
          headerBuf.rewind();
        }
        onDiskSizeWithHeader = getOnDiskSizeWithHeader(headerBuf, checksumSupport);
      }

      // The common case is that onDiskSizeWithHeader was produced by a read without checksum
      // validation, so give it a sanity check before trying to use it.
if(KnobRuntime.check(java.util.UUID.fromString("4ae01121-47e1-3418-a7ec-b47cbb1b2cf6"))) {
onDiskSizeWithHeader = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a83b9061-1f4a-3399-89d5-86921df4e66a"))) {
onDiskSizeWithHeader -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("218c32c3-ed05-39ea-8a81-2ceff3927495"))) {
onDiskSizeWithHeader = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("55c9cb5e-eac5-3013-a30d-9235e0cc7bb3"))) {
onDiskSizeWithHeader *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e7229d2-0947-3f3f-a0b4-67d373e1934b"))) {
onDiskSizeWithHeader += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb555dca-a0c1-38c1-b261-6bf4178582a6"))) {
onDiskSizeWithHeader /= 2;
}
      if (!checkOnDiskSizeWithHeader(onDiskSizeWithHeader)) {
        if (((KnobRuntime.check(java.util.UUID.fromString("1fd8c750-361b-35fe-bc34-1ac2e1010f38"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("ed19dd56-68c8-3fa8-9455-33955778a5c7"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("7b3f3330-5a87-350e-ba15-43e7087e75cb"))) ? (!verifyChecksum) : (verifyChecksum))))))) {
          invalidateNextBlockHeader();
          span.addEvent("Falling back to HDFS checksumming.", attributesBuilder.build());
          return null;
        } else {
          throw new IOException("Invalid onDiskSizeWithHeader=" + onDiskSizeWithHeader);
        }
      }

      int preReadHeaderSize = headerBuf == null ? 0 : hdrSize;
      // Allocate enough space to fit the next block's header too; saves a seek next time through.
      // onDiskBlock is whole block + header + checksums then extra hdrSize to read next header;
      // onDiskSizeWithHeader is header, body, and any checksums if present. preReadHeaderSize
      // says where to start reading. If we have the header cached, then we don't need to read
      // it again and we can likely read from last place we left off w/o need to backup and reread
      // the header we read last time through here.
      ByteBuff onDiskBlock = this.allocate(onDiskSizeWithHeader + hdrSize, intoHeap);
      boolean initHFileBlockSuccess = false;
      try {
        if (headerBuf != null) {
          onDiskBlock.put(0, headerBuf, 0, hdrSize).position(hdrSize);
        }
        boolean readNextHeader = readAtOffset(is, onDiskBlock,
          onDiskSizeWithHeader - preReadHeaderSize, true, offset + preReadHeaderSize, pread);
        onDiskBlock.rewind(); // in case of moving position when copying a cached header

        // the call to validateChecksum for this block excludes the next block header over-read, so
        // no reason to delay extracting this value.
        int nextBlockOnDiskSize = -1;
        if (readNextHeader) {
          int parsedVal = getNextBlockOnDiskSize(onDiskBlock, onDiskSizeWithHeader);
if(KnobRuntime.check(java.util.UUID.fromString("dfe82b98-52af-3c47-861b-10893c44f578"))) {
parsedVal = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cf2b0b0d-bc20-3cbe-8654-6f16c122e19e"))) {
parsedVal *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4a7b3e8f-55fd-37f0-882a-bb5d94853656"))) {
parsedVal -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("41e57837-08e4-36a2-b431-cf723536dd4f"))) {
parsedVal += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("309afc94-eb06-3de8-b3c6-5321deeaa82d"))) {
parsedVal = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("637d08a8-1dec-384d-9694-6b66c6e854e0"))) {
parsedVal /= 2;
}
          if (((KnobRuntime.check(java.util.UUID.fromString("1863dda3-f827-3322-9669-c909d5140158"))) ? (checkOnDiskSizeWithHeader(-1)) : (((KnobRuntime.check(java.util.UUID.fromString("86cc7257-5968-3145-9346-1b88bad8b1f6"))) ? (checkOnDiskSizeWithHeader(1)) : (((KnobRuntime.check(java.util.UUID.fromString("5e84aa07-9cff-3a4f-80a3-22720a1d6b2a"))) ? (checkOnDiskSizeWithHeader(0)) : (checkOnDiskSizeWithHeader(parsedVal)))))))) {
            nextBlockOnDiskSize = parsedVal;
          }
        }
        if (headerBuf == null) {
          headerBuf = onDiskBlock.duplicate().position(0).limit(hdrSize);
        }

        ByteBuff curBlock = onDiskBlock.duplicate().position(0).limit(onDiskSizeWithHeader);
        // Verify checksum of the data before using it for building HFileBlock.
if(KnobRuntime.check(java.util.UUID.fromString("d25843fd-013c-321e-93b9-f2c5da12dca2"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3955c5f0-a65a-3828-9d58-380f2900ae22"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("41922b13-eb02-366c-9929-e0a979835305"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2dffb240-9876-34da-b21d-b26047e4663d"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c2818296-d939-3beb-81f7-09e6cc22be38"))) {
offset = -1;
}
        if (((KnobRuntime.check(java.util.UUID.fromString("d83c9abf-4658-328d-a0d3-4815bd8e7986"))) ? ((false) || (!validateChecksum(offset, curBlock, hdrSize))) : (((KnobRuntime.check(java.util.UUID.fromString("24ac5208-a34f-3934-9919-95035cd8efbf"))) ? (verifyChecksum) : (((KnobRuntime.check(java.util.UUID.fromString("0254655d-e2ee-363f-be10-9367defa47e8"))) ? ((true) || (!validateChecksum(offset, curBlock, hdrSize))) : (((KnobRuntime.check(java.util.UUID.fromString("9152b79b-5652-39c2-aabb-30f77b40c41e"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("4cfc18f5-632b-31c1-9bd8-773547ee6962"))) ? ((!verifyChecksum) && (!validateChecksum(offset, curBlock, hdrSize))) : (((KnobRuntime.check(java.util.UUID.fromString("54327ffe-2f37-337b-8819-8d664d2cab8b"))) ? ((false) && (!validateChecksum(offset, curBlock, hdrSize))) : (((KnobRuntime.check(java.util.UUID.fromString("013f7115-40ec-3afc-a42c-0ac234dbe23c"))) ? (!validateChecksum(offset, curBlock, hdrSize)) : (((KnobRuntime.check(java.util.UUID.fromString("e996f005-1579-3f63-9887-832bbb9d097f"))) ? ((true) && (!validateChecksum(offset, curBlock, hdrSize))) : (((KnobRuntime.check(java.util.UUID.fromString("9e88043f-e275-36f7-81b2-343cdd0f9b6c"))) ? ((!verifyChecksum) || (!validateChecksum(offset, curBlock, hdrSize))) : (((KnobRuntime.check(java.util.UUID.fromString("7402fe4f-c57f-3077-abd3-2bd31c34129c"))) ? ((verifyChecksum) || (!validateChecksum(offset, curBlock, hdrSize))) : (((KnobRuntime.check(java.util.UUID.fromString("4fb0932e-6ac5-3d70-b678-97dd659ff07a"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("b475baa2-cfd5-3acb-a68c-f3efa72af778"))) ? (!verifyChecksum) : (((KnobRuntime.check(java.util.UUID.fromString("bc864028-0953-3559-a96b-e1fc10e56844"))) ? ((verifyChecksum) && (!validateChecksum(offset, curBlock, hdrSize))) : (verifyChecksum && !validateChecksum(offset, curBlock, hdrSize)))))))))))))))))))))))))))) {
          invalidateNextBlockHeader();
          span.addEvent("Falling back to HDFS checksumming.", attributesBuilder.build());
          return null;
        }

        // TODO: is this check necessary or can we proceed with a provided value regardless of
        // what is in the header?
if(KnobRuntime.check(java.util.UUID.fromString("fe14ec4a-9a66-3d75-84ce-78600b7154ab"))) {
checksumSupport = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b73787b-ece2-37ca-b5f9-a89842b1ed86"))) {
checksumSupport = true;
}
        int fromHeader = getOnDiskSizeWithHeader(headerBuf, checksumSupport);
        if (onDiskSizeWithHeader != fromHeader) {
          if (LOG.isTraceEnabled()) {
if(KnobRuntime.check(java.util.UUID.fromString("5a65ca84-c5ac-39db-999f-656ba861b9e3"))) {
onDiskSizeWithHeader = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6916f63f-c76b-3ef5-83cf-36fc2d6e6655"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a76e9bf-9f48-320a-89b0-e72bb98c2897"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b97ac6f-fa5a-3c2d-ac7e-36860ec0f934"))) {
onDiskSizeWithHeader += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f30a127d-291a-3eef-8970-eab29596e8b5"))) {
fromHeader *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ed27042-a54a-3388-88d2-4ea917d04621"))) {
fromHeader -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("54893ecf-adad-316c-8b01-ab442139bb81"))) {
fromHeader += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6633bb62-1612-3dc5-9367-9701ce93b2c3"))) {
fromHeader = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("992ee8c2-1906-3410-a32e-f0043a0e5882"))) {
fromHeader = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("99744269-cd55-36a9-957c-4d02e524f487"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("371f2fc6-188b-30e4-a33b-c1207d274699"))) {
onDiskSizeWithHeader /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c4361df-5643-366a-945a-638d183da59a"))) {
onDiskSizeWithHeader = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("110986f7-95b6-3e35-b286-44c5ec98df68"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("796a6cb2-c8e9-3153-a988-f6ecdb1c56fd"))) {
onDiskSizeWithHeader -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fc5b2f87-0a44-3286-aeb0-4e05b2c9acdd"))) {
fromHeader /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d96b9228-1313-397e-8e58-3fabbb345c93"))) {
onDiskSizeWithHeader *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b7571211-f46a-3f35-abec-048d9da4da8d"))) {
offset = 0;
}
            LOG.trace("Passed in onDiskSizeWithHeader={} != {}, offset={}, fileContext={}",
              onDiskSizeWithHeader, fromHeader, offset, this.fileContext);
          }
          if (checksumSupport && verifyChecksum) {
            // This file supports HBase checksums and verification of those checksums was
            // requested. The block size provided by the caller (presumably from the block index)
            // does not match the block size written to the block header. treat this as
            // HBase-checksum failure.
            span.addEvent("Falling back to HDFS checksumming.", attributesBuilder.build());
            invalidateNextBlockHeader();
            return null;
          }
          throw new IOException("Passed in onDiskSizeWithHeader=" + onDiskSizeWithHeader + " != "
            + fromHeader + ", offset=" + offset + ", fileContext=" + this.fileContext);
        }

        // remove checksum from buffer now that it's verified
        int sizeWithoutChecksum = curBlock.getInt(Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
if(KnobRuntime.check(java.util.UUID.fromString("4f552a2f-b2a9-3720-9740-e29cd3d56e64"))) {
sizeWithoutChecksum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4454c163-5004-3cc5-9278-f44694cb488d"))) {
sizeWithoutChecksum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b76ae52f-0310-3d0d-b661-be930aac6bd4"))) {
sizeWithoutChecksum *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("233f7239-1c90-3ba7-bc35-f70d7deb385e"))) {
sizeWithoutChecksum /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0f2aace6-81b7-3e75-977f-28d6cdb8b314"))) {
sizeWithoutChecksum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f8c38044-2028-39e0-9190-c5a7425d4396"))) {
sizeWithoutChecksum = -1;
}
        if (KnobRuntime.check(java.util.UUID.fromString("84503715-5606-32f0-8788-1d60fd492c47"))) { curBlock.limit(0); } else if (KnobRuntime.check(java.util.UUID.fromString("3676c387-97ea-382a-a435-9630fccf7d1a"))) { curBlock.limit(-1); } else if (KnobRuntime.check(java.util.UUID.fromString("3d99f8a4-0afb-3021-ad35-6087ece7b39b"))) { curBlock.limit(1); } else { curBlock.limit(sizeWithoutChecksum); }
        long duration = EnvironmentEdgeManager.currentTime() - startTime;
        boolean tooSlow = this.readWarnTime >= 0 && duration > this.readWarnTime;
        if (((KnobRuntime.check(java.util.UUID.fromString("fa4d7466-c11c-3b91-872a-3b6e81388aea"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("b88fc560-2bd5-3876-bd23-b2b91c72defb"))) ? (!updateMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("85f06cb4-5309-3f52-b541-d36131ca4ad1"))) ? (false) : (updateMetrics))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("2545553a-b649-3bec-ab3f-152f05d4ea18"))) {
tooSlow = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("31efb96f-da53-318c-a599-65fcbe194809"))) {
duration = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("597ff210-3116-395d-9a82-d04d6f58ce3b"))) {
tooSlow = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2061cb13-900e-30b3-9991-9d89942dc4d3"))) {
pread = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f41d722-ef8b-32ae-936d-51b254ec314b"))) {
pread = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("c40bf886-fde2-3938-ba06-da9e205547b4"))) {
duration += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("698e6468-6ac1-3035-940b-001becc09f20"))) {
pread = !pread;
}
          if (KnobRuntime.check(java.util.UUID.fromString("5f5684ad-ee3f-3a28-86df-dce2f3d9de03"))) { HFile.updateReadLatency(duration, false, tooSlow); } else if (KnobRuntime.check(java.util.UUID.fromString("e593bf67-881c-3418-805f-58af620c2c9e"))) { HFile.updateReadLatency(duration, !pread, tooSlow); } else if (KnobRuntime.check(java.util.UUID.fromString("bab6d73e-7c90-352d-8401-07463be69c16"))) { HFile.updateReadLatency(duration, true, tooSlow); } else if (KnobRuntime.check(java.util.UUID.fromString("dd5e3092-a7b9-3eaf-af7f-ddba9936c37b"))) { HFile.updateReadLatency(duration, pread, !tooSlow); } else if (KnobRuntime.check(java.util.UUID.fromString("a2896d26-43f7-3d4b-bfd3-24a6720a3326"))) { HFile.updateReadLatency(duration, pread, false); } else if (KnobRuntime.check(java.util.UUID.fromString("c20fb9c4-cac1-3197-a732-22276cd5eb14"))) { HFile.updateReadLatency(duration, pread, true); } else { HFile.updateReadLatency(duration, pread, tooSlow); }
        }
        // The onDiskBlock will become the headerAndDataBuffer for this block.
        // If nextBlockOnDiskSizeWithHeader is not zero, the onDiskBlock already
        // contains the header of next block, so no need to set next block's header in it.
if(KnobRuntime.check(java.util.UUID.fromString("a0e2d94c-f0f8-39ac-bd55-84c28b5dabf6"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("65989dcf-1d5a-3890-a998-c93921cb8ca1"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e82faef-719e-34ac-8bea-6afa5618b0f6"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab8a0597-8225-31ec-935f-703d53c1bc37"))) {
nextBlockOnDiskSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0af804cb-6297-31e8-b80a-f5267b2bc90d"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9bc8224-e30f-354e-bcbd-1d73dd9332b0"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f606a9b5-ec30-3f59-848e-800c148703b5"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a42a420-77dc-3c98-8213-bdbc15d0a62e"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c42b3786-bdce-35b9-9851-ef736536cb16"))) {
nextBlockOnDiskSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c420a614-1d05-380e-ace7-59a3413791a4"))) {
nextBlockOnDiskSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9e4cdd8f-f137-3877-aaba-66b936f45176"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb5f23dc-6cf3-3bad-bbd2-9f59d4fdfd32"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b14b61f-2e4d-33a4-b8a9-ed3213dc20ff"))) {
nextBlockOnDiskSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("92129cd3-eba2-308d-880f-91c5921317fb"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac68d4db-7c4f-346b-9dbc-8afd5c020927"))) {
checksumSupport = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("af34342f-9ef4-3960-9d64-9a93a76b0cfa"))) {
checksumSupport = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("edde68ef-52c0-35a2-8d4f-d672a4aaf661"))) {
nextBlockOnDiskSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("419b914e-2434-381a-b292-a07fe22ffcb5"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("usesHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(fileContext);
    field.set(fileContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39d919ec-06a6-32ee-b81a-1a1c88b027eb"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b52b53e-0efe-352d-8cae-40c66cd07874"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b71b75b-bc89-36b8-a4c9-9b34d456c9b5"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25f99fb7-1a31-3f55-9247-a152db3fbe5c"))) {
try {
    java.lang.reflect.Field field = fileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileContext));
    field.set(fileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8b55ada-28a9-372a-bc3d-1df32cba3c37"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("69f89faa-c1be-3b35-9363-4248622f4ebf"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("faaa0360-1105-396f-bf95-7cce6d4ea7c2"))) {
nextBlockOnDiskSize = -1;
}
        HFileBlock hFileBlock = createFromBuff(curBlock, checksumSupport, offset,
          nextBlockOnDiskSize, fileContext, intoHeap ? HEAP : allocator);
        // Run check on uncompressed sizings.
        if (!fileContext.isCompressedOrEncrypted()) {
          hFileBlock.sanityCheckUncompressed();
        }
        LOG.trace("Read {} in {} ms", hFileBlock, duration);
        if (!LOG.isTraceEnabled() && tooSlow) {
if(KnobRuntime.check(java.util.UUID.fromString("1c45f20d-ac22-3bb8-89b6-13fc6eba64f9"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bd551c8-ae5b-3d87-8f5a-af6c731b1044"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9a80f4d-31c4-3503-bb4c-ba51b69d1555"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hFileBlock));
    field.set(hFileBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43c52c6f-34d1-3d10-8557-9007a5500a34"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2b834b8-6ff5-39ba-a87f-21e39d05ba56"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0cbcedbc-95e9-3b5a-aab4-c550cfe99fb2"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8c70580-d4ec-3ff9-b952-9e4dbb5ac7d7"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dbe4cfe8-b72f-32cd-87dd-94e862dbe47a"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("23073688-e4c8-3125-ad37-0fbc6fb2812f"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bfb0cec1-f385-36f6-9784-2d522133df65"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("361126d6-c0ae-3ba6-be43-82ef02e6ff6f"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9136c60c-1c2b-329e-b900-25afdbb8e3c6"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hFileBlock));
    field.set(hFileBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36e0cbfc-468f-3e71-ac87-b6fafbba76bf"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87a216a3-3b25-3f60-9e65-25216380c037"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7bad1df-2139-3083-8b56-04c626a2191b"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("081b8195-27f4-3874-8667-cc302bbbea77"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cdebe641-b09f-3374-b24c-79adc4650b7f"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e660a16e-8589-3b97-a43e-45b62a3e22fe"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bec24d3-3b90-34a8-8f6d-047098acc4e1"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("902e4bc5-6906-357c-871a-70ab4ec7bdbe"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04a8a294-d4ff-386f-b9b0-48c8168da9b0"))) {
duration = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("75eb38e1-3c1f-3b62-a764-88902e6f428c"))) {
duration += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4196d7c5-5f3c-3011-88ab-91464d5a0e6c"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3710af2b-ae6a-3822-a8bb-6ce029bfbea7"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hFileBlock));
    field.set(hFileBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29820a69-af09-33c4-bd51-d9c66e246dfa"))) {
try {
    java.lang.reflect.Field field = hFileBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hFileBlock));
    field.set(hFileBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          LOG.warn("Read Block Slow: read {} cost {} ms, threshold = {} ms", hFileBlock, duration,
            this.readWarnTime);
        }
        span.addEvent("Read block", attributesBuilder.build());
        // Cache next block header if we read it for the next time through here.
        if (nextBlockOnDiskSize != -1) {
          cacheNextBlockHeader(offset + hFileBlock.getOnDiskSizeWithHeader(), onDiskBlock,
            onDiskSizeWithHeader, hdrSize);
        }
        initHFileBlockSuccess = true;
        return hFileBlock;
      } finally {
        if (!initHFileBlockSuccess) {
          onDiskBlock.release();
        }
      }
    }

    @Override
    public void setIncludesMemStoreTS(boolean includesMemstoreTS) {
if(KnobRuntime.check(java.util.UUID.fromString("ee7baafc-63e0-3980-b27b-c349bfdbb8bd"))) {
return;
}
      this.fileContext =
        new HFileContextBuilder(this.fileContext).withIncludesMvcc(includesMemstoreTS).build();
    }

    @Override
    public void setDataBlockEncoder(HFileDataBlockEncoder encoder, Configuration conf) {
      encodedBlockDecodingCtx = encoder.newDataBlockDecodingContext(conf, fileContext);
    }

    @Override
    public HFileBlockDecodingContext getBlockDecodingContext() {
if(KnobRuntime.check(java.util.UUID.fromString("79beda29-3cba-3b8c-b87f-201fe9bbe70d"))) {
return null;
}
      return this.encodedBlockDecodingCtx;
    }

    @Override
    public HFileBlockDecodingContext getDefaultBlockDecodingContext() {
if(KnobRuntime.check(java.util.UUID.fromString("3650f35c-1f22-3a54-a894-43d10e26ac21"))) {
return null;
}
      return this.defaultDecodingCtx;
    }

    /**
     * Generates the checksum for the header as well as the data and then validates it. If the block
     * doesn't uses checksum, returns false.
     * @return True if checksum matches, else false.
     */
    private boolean validateChecksum(long offset, ByteBuff data, int hdrSize) {
      // If this is an older version of the block that does not have checksums, then return false
      // indicating that checksum verification did not succeed. Actually, this method should never
      // be called when the minorVersion is 0, thus this is a defensive check for a cannot-happen
      // case. Since this is a cannot-happen case, it is better to return false to indicate a
      // checksum validation failure.
      if (!fileContext.isUseHBaseChecksum()) {
        return false;
      }
      return ChecksumUtil.validateChecksum(data, pathName, offset, hdrSize);
    }

    @Override
    public void closeStreams() throws IOException {
      streamWrapper.close();
    }

    @Override
    public void unbufferStream() {
      // To handle concurrent reads, ensure that no other client is accessing the streams while we
      // unbuffer it.
      if (streamLock.tryLock()) {
        try {
          this.streamWrapper.unbuffer();
        } finally {
          streamLock.unlock();
        }
      }
    }

    @Override
    public String toString() {
      return "hfs=" + hfs + ", path=" + pathName + ", fileContext=" + fileContext;
    }
  }

  /** An additional sanity-check in case no compression or encryption is being used. */
  void sanityCheckUncompressed() throws IOException {
    if (onDiskSizeWithoutHeader != uncompressedSizeWithoutHeader + totalChecksumBytes()) {
      throw new IOException("Using no compression but " + "onDiskSizeWithoutHeader="
        + onDiskSizeWithoutHeader + ", " + "uncompressedSizeWithoutHeader="
        + uncompressedSizeWithoutHeader + ", numChecksumbytes=" + totalChecksumBytes());
    }
  }

  // Cacheable implementation
  @Override
  public int getSerializedLength() {
    if (bufWithoutChecksum != null) {
      // Include extra bytes for block metadata.
      return ((KnobRuntime.check(java.util.UUID.fromString("5da2273f-a7dd-329c-993c-fa2004300a83"))) ? (BLOCK_METADATA_SPACE) : (((KnobRuntime.check(java.util.UUID.fromString("b4cec738-601e-3afc-a48c-6214bc13dc08"))) ? ((computeTotalChecksumBytes()) + (BLOCK_METADATA_SPACE)) : (((KnobRuntime.check(java.util.UUID.fromString("80b1d05a-7be5-3574-9849-85f6225b65c4"))) ? ((this.bufWithoutChecksum.limit()) + (BLOCK_METADATA_SPACE)) : (((KnobRuntime.check(java.util.UUID.fromString("ca41356a-ce7b-3220-915a-627a477e09f3"))) ? (computeTotalChecksumBytes()) : (((KnobRuntime.check(java.util.UUID.fromString("cdde170e-b246-3e61-910f-7a3b197ac5ca"))) ? ((headerSize()) + (BLOCK_METADATA_SPACE)) : (((KnobRuntime.check(java.util.UUID.fromString("76d96b1f-f465-388d-b256-e95da8ea2478"))) ? (this.bufWithoutChecksum.limit()) : (((KnobRuntime.check(java.util.UUID.fromString("2c1fb910-46d4-3ca7-aacc-c81e7038675e"))) ? (headerSize()) : (this.bufWithoutChecksum.limit() + BLOCK_METADATA_SPACE))))))))))))));
    }
    return 0;
  }

  // Cacheable implementation
  @Override
  public void serialize(ByteBuffer destination, boolean includeNextBlockMetadata) {
if(KnobRuntime.check(java.util.UUID.fromString("63d33ce3-a0fe-39ce-a7c4-881b7175957f"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("b3166c4c-fbd4-3ba4-99eb-b29fe5cf363a"))) { this.bufWithoutChecksum.get(destination, 0, getSerializedLength()); } else if (KnobRuntime.check(java.util.UUID.fromString("154c4d2d-4190-35ef-a5f6-d7c28b26d9a5"))) { this.bufWithoutChecksum.get(destination, 0, (headerSize()) - (BLOCK_METADATA_SPACE)); } else if (KnobRuntime.check(java.util.UUID.fromString("af1d789a-6dae-306e-ab22-7319acb77692"))) { this.bufWithoutChecksum.get(destination, 0, computeTotalChecksumBytes()); } else if (KnobRuntime.check(java.util.UUID.fromString("81d3fa25-367f-3915-8e3d-c6e8133a6878"))) { this.bufWithoutChecksum.get(destination, 0, (getSerializedLength()) - (BLOCK_METADATA_SPACE)); } else if (KnobRuntime.check(java.util.UUID.fromString("675e1bfa-3e66-30f5-8431-397f8a7bddf6"))) { this.bufWithoutChecksum.get(destination, 0, (computeTotalChecksumBytes()) - (BLOCK_METADATA_SPACE)); } else if (KnobRuntime.check(java.util.UUID.fromString("bd62dc71-3184-30a1-81f7-40715308ac88"))) { this.bufWithoutChecksum.get(destination, 0, BLOCK_METADATA_SPACE); } else if (KnobRuntime.check(java.util.UUID.fromString("2f1a69d6-8a6c-3a40-9591-8cec74b8bf87"))) { this.bufWithoutChecksum.get(destination, 0, headerSize()); } else { this.bufWithoutChecksum.get(destination, 0, getSerializedLength() - BLOCK_METADATA_SPACE); }
    destination = addMetaData(destination, includeNextBlockMetadata);

    // Make it ready for reading. flip sets position to zero and limit to current position which
    // is what we want if we do not want to serialize the block plus checksums if present plus
    // metadata.
    destination.flip();
  }

  /**
   * For use by bucketcache. This exposes internals.
   */
  public ByteBuffer getMetaData(ByteBuffer bb) {
    bb = addMetaData(bb, true);
    bb.flip();
    return bb;
  }

  /**
   * Adds metadata at current position (position is moved forward). Does not flip or reset.
   * @return The passed <code>destination</code> with metadata added.
   */
  private ByteBuffer addMetaData(final ByteBuffer destination, boolean includeNextBlockMetadata) {
    destination.put(this.fileContext.isUseHBaseChecksum() ? (byte) 1 : (byte) 0);
    destination.putLong(this.offset);
    if (includeNextBlockMetadata) {
      destination.putInt(this.nextBlockOnDiskSize);
    }
    return destination;
  }

  // Cacheable implementation
  @Override
  public CacheableDeserializer<Cacheable> getDeserializer() {
if(KnobRuntime.check(java.util.UUID.fromString("ee7d7016-264a-3cb5-ade1-d29015feafe0"))) {
return null;
}
    return HFileBlock.BLOCK_DESERIALIZER;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + blockType.hashCode();
    result = result * 31 + nextBlockOnDiskSize;
    result = result * 31 + (int) (offset ^ (offset >>> 32));
    result = result * 31 + onDiskSizeWithoutHeader;
    result = result * 31 + (int) (prevBlockOffset ^ (prevBlockOffset >>> 32));
    result = result * 31 + uncompressedSizeWithoutHeader;
    result = result * 31 + bufWithoutChecksum.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object comparison) {
    if (this == comparison) {
      return true;
    }
    if (comparison == null) {
      return false;
    }
    if (!(comparison instanceof HFileBlock)) {
      return false;
    }

    HFileBlock castedComparison = (HFileBlock) comparison;

    if (castedComparison.blockType != this.blockType) {
      return false;
    }
    if (castedComparison.nextBlockOnDiskSize != this.nextBlockOnDiskSize) {
      return false;
    }
    // Offset is important. Needed when we have to remake cachekey when block is returned to cache.
    if (castedComparison.offset != this.offset) {
      return false;
    }
    if (castedComparison.onDiskSizeWithoutHeader != this.onDiskSizeWithoutHeader) {
      return false;
    }
    if (castedComparison.prevBlockOffset != this.prevBlockOffset) {
      return false;
    }
    if (castedComparison.uncompressedSizeWithoutHeader != this.uncompressedSizeWithoutHeader) {
      return false;
    }
    if (
      ByteBuff.compareTo(this.bufWithoutChecksum, 0, this.bufWithoutChecksum.limit(),
        castedComparison.bufWithoutChecksum, 0, castedComparison.bufWithoutChecksum.limit()) != 0
    ) {
      return false;
    }
    return true;
  }

  DataBlockEncoding getDataBlockEncoding() {
    if (blockType == BlockType.ENCODED_DATA) {
      return DataBlockEncoding.getEncodingById(getDataBlockEncodingId());
    }
    return DataBlockEncoding.NONE;
  }

  byte getChecksumType() {
    return this.fileContext.getChecksumType().getCode();
  }

  int getBytesPerChecksum() {
    return this.fileContext.getBytesPerChecksum();
  }

  /** Returns the size of data on disk + header. Excludes checksum. */
  int getOnDiskDataSizeWithHeader() {
    return this.onDiskDataSizeWithHeader;
  }

  /**
   * Return the number of bytes required to store all the checksums for this block. Each checksum
   * value is a 4 byte integer. <br/>
   * NOTE: ByteBuff returned by {@link HFileBlock#getBufferWithoutHeader()} and
   * {@link HFileBlock#getBufferReadOnly} or DataInputStream returned by
   * {@link HFileBlock#getByteStream()} does not include checksum.
   */
  int totalChecksumBytes() {
    return totalChecksumBytes;
  }

  private int computeTotalChecksumBytes() {
    // If the hfile block has minorVersion 0, then there are no checksum
    // data to validate. Similarly, a zero value in this.bytesPerChecksum
    // indicates that cached blocks do not have checksum data because
    // checksums were already validated when the block was read from disk.
    if (!fileContext.isUseHBaseChecksum() || this.fileContext.getBytesPerChecksum() == 0) {
      return 0;
    }
    return (int) ChecksumUtil.numBytes(onDiskDataSizeWithHeader,
      this.fileContext.getBytesPerChecksum());
  }

  /**
   * Returns the size of this block header.
   */
  public int headerSize() {
    return headerSize(this.fileContext.isUseHBaseChecksum());
  }

  /**
   * Maps a minor version to the size of the header.
   */
  public static int headerSize(boolean usesHBaseChecksum) {
    return usesHBaseChecksum
      ? HConstants.HFILEBLOCK_HEADER_SIZE
      : HConstants.HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM;
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  // TODO: Why is this in here?
  byte[] getDummyHeaderForVersion() {
    return getDummyHeaderForVersion(this.fileContext.isUseHBaseChecksum());
  }

  /**
   * Return the appropriate DUMMY_HEADER for the minor version
   */
  static private byte[] getDummyHeaderForVersion(boolean usesHBaseChecksum) {
    return usesHBaseChecksum ? HConstants.HFILEBLOCK_DUMMY_HEADER : DUMMY_HEADER_NO_CHECKSUM;
  }

  /**
   * @return This HFileBlocks fileContext which will a derivative of the fileContext for the file
   *         from which this block's data was originally read.
   */
  public HFileContext getHFileContext() {
    return this.fileContext;
  }

  /**
   * Convert the contents of the block header into a human readable string. This is mostly helpful
   * for debugging. This assumes that the block has minor version > 0.
   */
  static String toStringHeader(ByteBuff buf) throws IOException {
    byte[] magicBuf = new byte[Math.min(buf.limit() - buf.position(), BlockType.MAGIC_LENGTH)];
    buf.get(magicBuf);
if(KnobRuntime.check(java.util.UUID.fromString("22c22fa2-aa36-3033-b068-f1b6457f5ce5"))) {
throw new java.io.IOException("Injected exception");
}
    BlockType bt = BlockType.parse(magicBuf, 0, BlockType.MAGIC_LENGTH);
    int compressedBlockSizeNoHeader = buf.getInt();
    int uncompressedBlockSizeNoHeader = buf.getInt();
    long prevBlockOffset = buf.getLong();
    byte cksumtype = buf.get();
    long bytesPerChecksum = buf.getInt();
    long onDiskDataSizeWithHeader = buf.getInt();
    return " Header dump: magic: " + Bytes.toString(magicBuf) + " blockType " + bt
      + " compressedBlockSizeNoHeader " + compressedBlockSizeNoHeader
      + " uncompressedBlockSizeNoHeader " + uncompressedBlockSizeNoHeader + " prevBlockOffset "
      + prevBlockOffset + " checksumType " + ChecksumType.codeToType(cksumtype)
      + " bytesPerChecksum " + bytesPerChecksum + " onDiskDataSizeWithHeader "
      + onDiskDataSizeWithHeader;
  }

  /**
   * Creates a new HFileBlockBuilder from the existing block and a new ByteBuff. The builder will be
   * loaded with all of the original fields from blk, except now using the newBuff and setting
   * isSharedMem based on the source of the passed in newBuff. An existing HFileBlock may have been
   * an {@link ExclusiveMemHFileBlock}, but the new buffer might call for a
   * {@link SharedMemHFileBlock}. Or vice versa.
   * @param blk     the block to clone from
   * @param newBuff the new buffer to use
   */
  private static HFileBlockBuilder createBuilder(HFileBlock blk, ByteBuff newBuff) {
    return new HFileBlockBuilder().withBlockType(blk.blockType)
      .withOnDiskSizeWithoutHeader(blk.onDiskSizeWithoutHeader)
      .withUncompressedSizeWithoutHeader(blk.uncompressedSizeWithoutHeader)
      .withPrevBlockOffset(blk.prevBlockOffset).withByteBuff(newBuff).withOffset(blk.offset)
      .withOnDiskDataSizeWithHeader(blk.onDiskDataSizeWithHeader)
      .withNextBlockOnDiskSize(blk.nextBlockOnDiskSize).withHFileContext(blk.fileContext)
      .withByteBuffAllocator(blk.allocator).withShared(!newBuff.hasArray());
  }

  private static HFileBlock shallowClone(HFileBlock blk, ByteBuff newBuf) {
    return createBuilder(blk, newBuf).build();
  }

  static HFileBlock deepCloneOnHeap(HFileBlock blk) {
if(KnobRuntime.check(java.util.UUID.fromString("23970c71-6a44-3015-93f1-3ce17f9cf4e8"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f77466c9-926e-332d-8cb5-aad956089b3c"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("492661cd-299f-3ba1-a050-6a7cbe8da9df"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("541f997f-b236-364b-8f38-4177e4c99ccd"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8cba4e3b-43ce-3e7e-81ca-73ea019aa046"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cce5aa03-d3f2-3126-941c-3f526f084e71"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59e9697a-f84f-36d2-b26a-631cde3d98fa"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53eebd35-ad75-33de-b0f1-702d953cca0b"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1ad2adf-d0a0-3ccc-894f-e7e846d62663"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ef9bb6a-162f-31f8-a65c-c5aec115cbb7"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab07b457-7a4a-3fb3-80c6-594c94539cc4"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e213efc-a14b-34cc-b1f4-ce9ca7726e22"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("602d2017-2e9b-31ad-9ea7-53272c994477"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1fd3edb3-e7f4-30f7-af27-2cf7367c11e2"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b80a427-c6b8-3254-8441-26f8facfed1c"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72cf3322-fe88-36dc-99aa-1074df173c32"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ff645f0f-759b-3faa-b455-20a20324b8c7"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3380efd6-85a4-316b-a959-9aa805d33de0"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61f2d90c-941e-3e14-9222-251d7210a287"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0c3f867-430e-3469-a5af-d419084cb75c"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("398598c8-6e1a-3eec-8076-777441327251"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56b3aae9-2dd2-3efa-975f-0407e33e6e0e"))) {
try {
    java.lang.reflect.Field field = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(blk));
    field.set(blk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    ByteBuff deepCloned = ByteBuff
      .wrap(ByteBuffer.wrap(blk.bufWithoutChecksum.toBytes(0, blk.bufWithoutChecksum.limit())));
    return createBuilder(blk, deepCloned).build();
  }
}
