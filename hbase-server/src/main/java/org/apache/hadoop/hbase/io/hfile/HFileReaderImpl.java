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

import static org.apache.hadoop.hbase.regionserver.CompactSplit.HBASE_REGION_SERVER_ENABLE_COMPACTION;
import static org.apache.hadoop.hbase.trace.HBaseSemanticAttributes.BLOCK_CACHE_KEY_KEY;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.IntConsumer;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ByteBufferKeyOnlyKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.SizeCachedByteBufferKeyValue;
import org.apache.hadoop.hbase.SizeCachedKeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsByteBufferKeyValue;
import org.apache.hadoop.hbase.SizeCachedNoTagsKeyValue;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation that can handle all hfile versions of {@link HFile.Reader}.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class HFileReaderImpl implements HFile.Reader, Configurable {
  // This class is HFileReaderV3 + HFileReaderV2 + AbstractHFileReader all squashed together into
  // one file. Ditto for all the HFileReader.ScannerV? implementations. I was running up against
  // the MaxInlineLevel limit because too many tiers involved reading from an hfile. Was also hard
  // to navigate the source code when so many classes participating in read.
  private static final Logger LOG = LoggerFactory.getLogger(HFileReaderImpl.class);

  /** Data block index reader keeping the root data index in memory */
  protected HFileBlockIndex.CellBasedKeyBlockIndexReader dataBlockIndexReader;

  /** Meta block index reader -- always single level */
  protected HFileBlockIndex.ByteArrayKeyBlockIndexReader metaBlockIndexReader;

  protected FixedFileTrailer trailer;

  private final boolean primaryReplicaReader;

  /**
   * What kind of data block encoding should be used while reading, writing, and handling cache.
   */
  protected HFileDataBlockEncoder dataBlockEncoder = NoOpDataBlockEncoder.INSTANCE;

  /** Block cache configuration. */
  protected final CacheConfig cacheConf;

  protected ReaderContext context;

  protected final HFileInfo fileInfo;

  /** Path of file */
  protected final Path path;

  /** File name to be used for block names */
  protected final String name;

  private Configuration conf;

  protected HFileContext hfileContext;

  /** Filesystem-level block reader. */
  protected HFileBlock.FSReader fsBlockReader;

  /**
   * A "sparse lock" implementation allowing to lock on a particular block identified by offset. The
   * purpose of this is to avoid two clients loading the same block, and have all but one client
   * wait to get the block from the cache.
   */
  private IdLock offsetLock = new IdLock();

  /** Minimum minor version supported by this HFile format */
  static final int MIN_MINOR_VERSION = 0;

  /** Maximum minor version supported by this HFile format */
  // We went to version 2 when we moved to pb'ing fileinfo and the trailer on
  // the file. This version can read Writables version 1.
  static final int MAX_MINOR_VERSION = 3;

  /** Minor versions starting with this number have faked index key */
  static final int MINOR_VERSION_WITH_FAKED_KEY = 3;

  /**
   * Opens a HFile.
   * @param context   Reader context info
   * @param fileInfo  HFile info
   * @param cacheConf Cache configuration.
   * @param conf      Configuration
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  public HFileReaderImpl(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
    Configuration conf) throws IOException {
    this.cacheConf = cacheConf;
    this.context = context;
    this.path = context.getFilePath();
    this.name = path.getName();
    this.conf = conf;
    this.primaryReplicaReader = context.isPrimaryReplicaReader();
    this.fileInfo = fileInfo;
    this.trailer = fileInfo.getTrailer();
    this.hfileContext = fileInfo.getHFileContext();
    this.fsBlockReader =
      new HFileBlock.FSReaderImpl(context, hfileContext, cacheConf.getByteBuffAllocator(), conf);
    this.dataBlockEncoder = HFileDataBlockEncoderImpl.createFromFileInfo(fileInfo);
    fsBlockReader.setDataBlockEncoder(dataBlockEncoder, conf);
    dataBlockIndexReader = fileInfo.getDataBlockIndexReader();
    metaBlockIndexReader = fileInfo.getMetaBlockIndexReader();
  }

  @SuppressWarnings("serial")
  public static class BlockIndexNotLoadedException extends IllegalStateException {
    public BlockIndexNotLoadedException(Path path) {
      // Add a message in case anyone relies on it as opposed to class name.
      super(path + " block index not loaded");
    }
  }

  private Optional<String> toStringFirstKey() {
    return getFirstKey().map(CellUtil::getCellKeyAsString);
  }

  private Optional<String> toStringLastKey() {
    return getLastKey().map(CellUtil::getCellKeyAsString);
  }

  @Override
  public String toString() {
    return "reader=" + path.toString()
      + (!isFileInfoLoaded()
        ? ""
        : ", compression=" + trailer.getCompressionCodec().getName() + ", cacheConf=" + cacheConf
          + ", firstKey=" + toStringFirstKey() + ", lastKey=" + toStringLastKey())
      + ", avgKeyLen=" + fileInfo.getAvgKeyLen() + ", avgValueLen=" + fileInfo.getAvgValueLen()
      + ", entries=" + trailer.getEntryCount() + ", length=" + context.getFileSize();
  }

  @Override
  public long length() {
    return context.getFileSize();
  }

  /**
   * @return the first key in the file. May be null if file has no entries. Note that this is not
   *         the first row key, but rather the byte form of the first KeyValue.
   */
  @Override
  public Optional<Cell> getFirstKey() {
    if (dataBlockIndexReader == null) {
      throw new BlockIndexNotLoadedException(path);
    }
    return dataBlockIndexReader.isEmpty()
      ? Optional.empty()
      : Optional.of(dataBlockIndexReader.getRootBlockKey(0));
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after Ryan's patch goes in to
   * eliminate {@link KeyValue} here.
   * @return the first row key, or null if the file is empty.
   */
  @Override
  public Optional<byte[]> getFirstRowKey() {
    // We have to copy the row part to form the row key alone
    return getFirstKey().map(CellUtil::cloneRow);
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after Ryan's patch goes in to
   * eliminate {@link KeyValue} here.
   * @return the last row key, or null if the file is empty.
   */
  @Override
  public Optional<byte[]> getLastRowKey() {
    // We have to copy the row part to form the row key alone
    return getLastKey().map(CellUtil::cloneRow);
  }

  /** Returns number of KV entries in this HFile */
  @Override
  public long getEntries() {
    return trailer.getEntryCount();
  }

  /** Returns comparator */
  @Override
  public CellComparator getComparator() {
    return this.hfileContext.getCellComparator();
  }

  public Compression.Algorithm getCompressionAlgorithm() {
    return trailer.getCompressionCodec();
  }

  /**
   * @return the total heap size of data and meta block indexes in bytes. Does not take into account
   *         non-root blocks of a multilevel data index.
   */
  @Override
  public long indexSize() {
    return (dataBlockIndexReader != null ? dataBlockIndexReader.heapSize() : 0)
      + ((metaBlockIndexReader != null) ? metaBlockIndexReader.heapSize() : 0);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setDataBlockEncoder(HFileDataBlockEncoder dataBlockEncoder) {
    this.dataBlockEncoder = dataBlockEncoder;
    this.fsBlockReader.setDataBlockEncoder(dataBlockEncoder, conf);
  }

  @Override
  public void setDataBlockIndexReader(HFileBlockIndex.CellBasedKeyBlockIndexReader reader) {
    this.dataBlockIndexReader = reader;
  }

  @Override
  public HFileBlockIndex.CellBasedKeyBlockIndexReader getDataBlockIndexReader() {
if(KnobRuntime.check(java.util.UUID.fromString("462689e1-7c63-3083-9760-f917f7cdc7bb"))) {
return null;
}
    return dataBlockIndexReader;
  }

  @Override
  public void setMetaBlockIndexReader(HFileBlockIndex.ByteArrayKeyBlockIndexReader reader) {
    this.metaBlockIndexReader = reader;
  }

  @Override
  public HFileBlockIndex.ByteArrayKeyBlockIndexReader getMetaBlockIndexReader() {
    return metaBlockIndexReader;
  }

  @Override
  public FixedFileTrailer getTrailer() {
    return trailer;
  }

  @Override
  public ReaderContext getContext() {
    return this.context;
  }

  @Override
  public HFileInfo getHFileInfo() {
    return this.fileInfo;
  }

  @Override
  public boolean isPrimaryReplicaReader() {
    return primaryReplicaReader;
  }

  /**
   * An exception thrown when an operation requiring a scanner to be seeked is invoked on a scanner
   * that is not seeked.
   */
  @SuppressWarnings("serial")
  public static class NotSeekedException extends IllegalStateException {
    public NotSeekedException(Path path) {
      super(path + " not seeked to a key/value");
    }
  }

  protected static class HFileScannerImpl implements HFileScanner {
    private ByteBuff blockBuffer;
    protected final boolean cacheBlocks;
    protected final boolean pread;
    protected final boolean isCompaction;
    private int currKeyLen;
    private int currValueLen;
    private int currMemstoreTSLen;
    private long currMemstoreTS;
    protected final HFile.Reader reader;
    private int currTagsLen;
    private short rowLen;
    // buffer backed keyonlyKV
    private ByteBufferKeyOnlyKeyValue bufBackedKeyOnlyKv = new ByteBufferKeyOnlyKeyValue();
    // A pair for reusing in blockSeek() so that we don't garbage lot of objects
    final ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<>();

    /**
     * The next indexed key is to keep track of the indexed key of the next data block. If the
     * nextIndexedKey is HConstants.NO_NEXT_INDEXED_KEY, it means that the current data block is the
     * last data block. If the nextIndexedKey is null, it means the nextIndexedKey has not been
     * loaded yet.
     */
    protected Cell nextIndexedKey;
    // Current block being used. NOTICE: DON't release curBlock separately except in shipped() or
    // close() methods. Because the shipped() or close() will do the release finally, even if any
    // exception occur the curBlock will be released by the close() method (see
    // RegionScannerImpl#handleException). Call the releaseIfNotCurBlock() to release the
    // unreferenced block please.
    protected HFileBlock curBlock;
    // Whether we returned a result for curBlock's size in recordBlockSize().
    // gets reset whenever curBlock is changed.
    private boolean providedCurrentBlockSize = false;
    // Previous blocks that were used in the course of the read
    protected final ArrayList<HFileBlock> prevBlocks = new ArrayList<>();

    public HFileScannerImpl(final HFile.Reader reader, final boolean cacheBlocks,
      final boolean pread, final boolean isCompaction) {
      this.reader = reader;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }

    void updateCurrBlockRef(HFileBlock block) {
      if (block != null && curBlock != null && block.getOffset() == curBlock.getOffset()) {
        return;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("a4326d82-18f5-32ff-86c2-9c98aaae65ed"))) ? (((this.curBlock) != (null)) || (this.curBlock.isSharedMem())) : (((KnobRuntime.check(java.util.UUID.fromString("a7656207-1d6d-32e2-ba0c-64fdeec502a9"))) ? (((this.curBlock) != (null)) || (this.curBlock.release())) : (this.curBlock != null && this.curBlock.isSharedMem()))))) {
        prevBlocks.add(this.curBlock);
      }
      this.curBlock = block;
      this.providedCurrentBlockSize = false;
    }

    void reset() {
      // We don't have to keep ref to heap block
      if (this.curBlock != null && this.curBlock.isSharedMem()) {
        this.prevBlocks.add(this.curBlock);
      }
      this.curBlock = null;
    }

    private void returnBlocks(boolean returnAll) {
      this.prevBlocks.forEach(HFileBlock::release);
      this.prevBlocks.clear();
      if (returnAll && this.curBlock != null) {
        this.curBlock.release();
        this.curBlock = null;
      }
    }

    @Override
    public boolean isSeeked() {
if(KnobRuntime.check(java.util.UUID.fromString("87711326-ab22-3713-9433-335898fd1590"))) {
return true;
}
      return blockBuffer != null;
    }

    @Override
    public String toString() {
      return "HFileScanner for reader " + String.valueOf(getReader());
    }

    protected void assertSeeked() {
      if (!isSeeked()) {
        throw new NotSeekedException(reader.getPath());
      }
    }

    @Override
    public HFile.Reader getReader() {
      return reader;
    }

    // From non encoded HFiles, we always read back KeyValue or its descendant.(Note: When HFile
    // block is in DBB, it will be OffheapKV). So all parts of the Cell is in a contiguous
    // array/buffer. How many bytes we should wrap to make the KV is what this method returns.
    private int getKVBufSize() {
      int kvBufSize = KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen;
      if (currTagsLen > 0) {
        kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return kvBufSize;
    }

    @Override
    public void close() {
      if (!pread) {
        // For seek + pread stream socket should be closed when the scanner is closed. HBASE-9393
        reader.unbufferStream();
      }
      this.returnBlocks(true);
    }

    @Override
    public void recordBlockSize(IntConsumer blockSizeConsumer) {
      if (!providedCurrentBlockSize && curBlock != null) {
        providedCurrentBlockSize = true;
        blockSizeConsumer.accept(curBlock.getUncompressedSizeWithoutHeader());
      }
    }

    // Returns the #bytes in HFile for the current cell. Used to skip these many bytes in current
    // HFile block's buffer so as to position to the next cell.
    private int getCurCellSerializedSize() {
      int curCellSize = KEY_VALUE_LEN_SIZE + currKeyLen + currValueLen + currMemstoreTSLen;
      if (this.reader.getFileContext().isIncludesTags()) {
        curCellSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return curCellSize;
    }

    protected void readKeyValueLen() {
      // This is a hot method. We go out of our way to make this method short so it can be
      // inlined and is not too big to compile. We also manage position in ByteBuffer ourselves
      // because it is faster than going via range-checked ByteBuffer methods or going through a
      // byte buffer array a byte at a time.
      // Get a long at a time rather than read two individual ints. In micro-benchmarking, even
      // with the extra bit-fiddling, this is order-of-magnitude faster than getting two ints.
      // Trying to imitate what was done - need to profile if this is better or
      // earlier way is better by doing mark and reset?
      // But ensure that you read long instead of two ints
      long ll = blockBuffer.getLongAfterPosition(0);
      // Read top half as an int of key length and bottom int as value length
      this.currKeyLen = (int) (ll >> Integer.SIZE);
      this.currValueLen = (int) (Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
      checkKeyValueLen();
      this.rowLen = blockBuffer.getShortAfterPosition(Bytes.SIZEOF_LONG);
      // Move position past the key and value lengths and then beyond the key and value
      int p = (Bytes.SIZEOF_LONG + currKeyLen + currValueLen);
      if (reader.getFileContext().isIncludesTags()) {
        // Tags length is a short.
if(KnobRuntime.check(java.util.UUID.fromString("366cd530-9c61-3652-af65-9a0eb0ed1dd6"))) {
p = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("416c3e2f-5110-35ba-892f-246036dce11b"))) {
p = -1;
}
        this.currTagsLen = blockBuffer.getShortAfterPosition(p);
        checkTagsLen();
        p += (Bytes.SIZEOF_SHORT + currTagsLen);
      }
if(KnobRuntime.check(java.util.UUID.fromString("67433e13-85d0-34e7-9b06-733e569657b7"))) {
p = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("559820eb-55f0-3250-8fe1-efd1b9e05d42"))) {
p += 1;
}
      readMvccVersion(p);
    }

    private final void checkTagsLen() {
      if (checkLen(this.currTagsLen)) {
        throw new IllegalStateException(
          "Invalid currTagsLen " + this.currTagsLen + ". Block offset: " + curBlock.getOffset()
            + ", block length: " + this.blockBuffer.limit() + ", position: "
            + this.blockBuffer.position() + " (without header)." + " path=" + reader.getPath());
      }
    }

    /**
     * Read mvcc. Does checks to see if we even need to read the mvcc at all.
     */
    protected void readMvccVersion(final int offsetFromPos) {
      // See if we even need to decode mvcc.
      if (!this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
        return;
      }
      if (!this.reader.getHFileInfo().isDecodeMemstoreTS()) {
        currMemstoreTS = 0;
        currMemstoreTSLen = 1;
        return;
      }
      _readMvccVersion(offsetFromPos);
    }

    /**
     * Actually do the mvcc read. Does no checks.
     */
    private void _readMvccVersion(int offsetFromPos) {
      // This is Bytes#bytesToVint inlined so can save a few instructions in this hot method; i.e.
      // previous if one-byte vint, we'd redo the vint call to find int size.
      // Also the method is kept small so can be inlined.
      byte firstByte = blockBuffer.getByteAfterPosition(offsetFromPos);
      int len = WritableUtils.decodeVIntSize(firstByte);
      if (len == 1) {
        this.currMemstoreTS = firstByte;
      } else {
        int remaining = len - 1;
        long i = 0;
        offsetFromPos++;
        if (remaining >= Bytes.SIZEOF_INT) {
          // The int read has to be converted to unsigned long so the & op
          i = (blockBuffer.getIntAfterPosition(offsetFromPos) & 0x00000000ffffffffL);
          remaining -= Bytes.SIZEOF_INT;
          offsetFromPos += Bytes.SIZEOF_INT;
        }
        if (remaining >= Bytes.SIZEOF_SHORT) {
          short s = blockBuffer.getShortAfterPosition(offsetFromPos);
          i = i << 16;
          i = i | (s & 0xFFFF);
          remaining -= Bytes.SIZEOF_SHORT;
          offsetFromPos += Bytes.SIZEOF_SHORT;
        }
        for (int idx = 0; idx < remaining; idx++) {
          byte b = blockBuffer.getByteAfterPosition(offsetFromPos + idx);
          i = i << 8;
          i = i | (b & 0xFF);
        }
        currMemstoreTS = (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
      }
      this.currMemstoreTSLen = len;
    }

    /**
     * Within a loaded block, seek looking for the last key that is smaller than (or equal to?) the
     * key we are interested in. A note on the seekBefore: if you have seekBefore = true, AND the
     * first key in the block = key, then you'll get thrown exceptions. The caller has to check for
     * that case and load the previous block as appropriate. the key to find find the key before the
     * given key in case of exact match.
     * @return 0 in case of an exact key match, 1 in case of an inexact match, -2 in case of an
     *         inexact match and furthermore, the input key less than the first key of current
     *         block(e.g. using a faked index key)
     */
    protected int blockSeek(Cell key, boolean seekBefore) {
      int klen, vlen, tlen = 0;
      int lastKeyValueSize = -1;
      int offsetFromPos;
      do {
        offsetFromPos = 0;
        // Better to ensure that we use the BB Utils here
        long ll = blockBuffer.getLongAfterPosition(offsetFromPos);
        klen = (int) (ll >> Integer.SIZE);
        vlen = (int) (Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
        if (checkKeyLen(klen) || checkLen(vlen)) {
          throw new IllegalStateException(
            "Invalid klen " + klen + " or vlen " + vlen + ". Block offset: " + curBlock.getOffset()
              + ", block length: " + blockBuffer.limit() + ", position: " + blockBuffer.position()
              + " (without header)." + " path=" + reader.getPath());
        }
        offsetFromPos += Bytes.SIZEOF_LONG;
if(KnobRuntime.check(java.util.UUID.fromString("7a42bb79-bbc8-3e5f-9dd8-4cdb1e1d3f81"))) {
offsetFromPos = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5924c857-8965-3e61-bdbc-2d83c3811ba4"))) {
offsetFromPos -= 1;
}
        this.rowLen = blockBuffer.getShortAfterPosition(offsetFromPos);
if(KnobRuntime.check(java.util.UUID.fromString("5190aec9-75e0-35a7-9487-e22c3298708a"))) {
try {
    java.lang.reflect.Field _knob_field_ = pair.getClass().getDeclaredField("second");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(pair));
    _knob_field_.set(pair, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("be34269e-3580-30a8-bbb2-9f95170b3bc6"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (0), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("604cc07a-e376-3068-b998-85dc87ac709b"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (8), klen, pair); } else { blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, klen, pair); }
        if (KnobRuntime.check(java.util.UUID.fromString("dc9d546a-701b-350d-8be1-7b7b5c2a0a1c"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), 0, rowLen); } else { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), klen, rowLen); }
        int comp =
          PrivateCellUtil.compareKeyIgnoresMvcc(reader.getComparator(), key, bufBackedKeyOnlyKv);
        offsetFromPos += klen + vlen;
        if (this.reader.getFileContext().isIncludesTags()) {
          // Read short as unsigned, high byte first
          tlen = ((blockBuffer.getByteAfterPosition(offsetFromPos) & 0xff) << 8)
            ^ (blockBuffer.getByteAfterPosition(offsetFromPos + 1) & 0xff);
if(KnobRuntime.check(java.util.UUID.fromString("b4fb0788-33b3-37ec-bd41-0d0479b95e29"))) {
tlen /= 2;
}
          if (((KnobRuntime.check(java.util.UUID.fromString("bb5cbd4c-2245-3148-a9ba-a5a150bf2195"))) ? (checkLen(0xff)) : (((KnobRuntime.check(java.util.UUID.fromString("5e41eb5b-c88d-341d-b5e0-f4efbbdc6dbd"))) ? (checkLen(8)) : (checkLen(tlen)))))) {
            throw new IllegalStateException("Invalid tlen " + tlen + ". Block offset: "
              + curBlock.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
              + blockBuffer.position() + " (without header)." + " path=" + reader.getPath());
          }
          // add the two bytes read for the tags.
          offsetFromPos += tlen + (Bytes.SIZEOF_SHORT);
        }
        if (this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
          // Directly read the mvcc based on current position
          readMvccVersion(offsetFromPos);
        }
        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                + "at the first key of the block: key=" + CellUtil.getCellKeyAsString(key)
                + ", blockOffset=" + curBlock.getOffset() + ", onDiskSize="
                + curBlock.getOnDiskSizeWithHeader() + ", path=" + reader.getPath());
            }
            blockBuffer.moveBack(lastKeyValueSize);
            readKeyValueLen();
            return 1; // non exact match.
          }
          currKeyLen = klen;
          currValueLen = vlen;
          currTagsLen = tlen;
          return 0; // indicate exact match
        } else if (comp < 0) {
          if (lastKeyValueSize > 0) {
            blockBuffer.moveBack(lastKeyValueSize);
          }
          readKeyValueLen();
          if (lastKeyValueSize == -1 && blockBuffer.position() == 0) {
            return HConstants.INDEX_KEY_MAGIC;
          }
          return 1;
        }
        // The size of this key/value tuple, including key/value length fields.
        lastKeyValueSize = klen + vlen + currMemstoreTSLen + KEY_VALUE_LEN_SIZE;
        // include tag length also if tags included with KV
        if (reader.getFileContext().isIncludesTags()) {
          lastKeyValueSize += tlen + Bytes.SIZEOF_SHORT;
        }
        blockBuffer.skip(lastKeyValueSize);
      } while (blockBuffer.hasRemaining());

      // Seek to the last key we successfully read. This will happen if this is
      // the last key/value pair in the file, in which case the following call
      // to next() has to return false.
      blockBuffer.moveBack(lastKeyValueSize);
      readKeyValueLen();
      return 1; // didn't exactly find it.
    }

    @Override
    public Cell getNextIndexedKey() {
      return nextIndexedKey;
    }

    @Override
    public int seekTo(Cell key) throws IOException {
      return seekTo(key, true);
    }

    @Override
    public int reseekTo(Cell key) throws IOException {
      int compared;
      if (isSeeked()) {
        compared = compareKey(reader.getComparator(), key);
        if (compared < 1) {
          // If the required key is less than or equal to current key, then
          // don't do anything.
          return compared;
        } else {
          // The comparison with no_next_index_key has to be checked
          if (
            this.nextIndexedKey != null && (this.nextIndexedKey
                == KeyValueScanner.NO_NEXT_INDEXED_KEY
              || PrivateCellUtil.compareKeyIgnoresMvcc(reader.getComparator(), key, nextIndexedKey)
                  < 0)
          ) {
            // The reader shall continue to scan the current data block instead
            // of querying the
            // block index as long as it knows the target key is strictly
            // smaller than
            // the next indexed key or the current data block is the last data
            // block.
            return loadBlockAndSeekToKey(this.curBlock, nextIndexedKey, false, key, false);
          }
        }
      }
      // Don't rewind on a reseek operation, because reseek implies that we are
      // always going forward in the file.
      return seekTo(key, false);
    }

    /**
     * An internal API function. Seek to the given key, optionally rewinding to the first key of the
     * block before doing the seek.
     * @param key    - a cell representing the key that we need to fetch
     * @param rewind whether to rewind to the first key of the block before doing the seek. If this
     *               is false, we are assuming we never go back, otherwise the result is undefined.
     * @return -1 if the key is earlier than the first key of the file, 0 if we are at the given
     *         key, 1 if we are past the given key -2 if the key is earlier than the first key of
     *         the file while using a faked index key
     */
    public int seekTo(Cell key, boolean rewind) throws IOException {
      HFileBlockIndex.BlockIndexReader indexReader = reader.getDataBlockIndexReader();
      BlockWithScanInfo blockWithScanInfo = indexReader.loadDataBlockWithScanInfo(key, curBlock,
        cacheBlocks, pread, isCompaction, getEffectiveDataBlockEncoding(), reader);
      if (blockWithScanInfo == null || blockWithScanInfo.getHFileBlock() == null) {
        // This happens if the key e.g. falls before the beginning of the file.
        return -1;
      }
      return loadBlockAndSeekToKey(blockWithScanInfo.getHFileBlock(),
        blockWithScanInfo.getNextIndexedKey(), rewind, key, false);
    }

    @Override
    public boolean seekBefore(Cell key) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f7a3affe-49fc-3e27-88a8-42c16de8ac1e"))) {
try {
    java.lang.reflect.Field _knob_field_ = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(curBlock));
    _knob_field_.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      HFileBlock seekToBlock = reader.getDataBlockIndexReader().seekToDataBlock(key, curBlock,
        cacheBlocks, pread, isCompaction, reader.getEffectiveEncodingInCache(isCompaction), reader);
      if (((KnobRuntime.check(java.util.UUID.fromString("15fb38f1-daf1-3dcd-ad3b-c5a53dd10f77"))) ? ((seekToBlock) != (null)) : (seekToBlock == null))) {
        return false;
      }
if(KnobRuntime.check(java.util.UUID.fromString("b77b2fd0-b5db-3443-a0bf-100eb17c07a5"))) {
try {
    java.lang.reflect.Field _knob_field_ = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(seekToBlock));
    _knob_field_.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      Cell firstKey = getFirstKeyCellInBlock(seekToBlock);
      if (PrivateCellUtil.compareKeyIgnoresMvcc(reader.getComparator(), firstKey, key) >= 0) {
        long previousBlockOffset = seekToBlock.getPrevBlockOffset();
        // The key we are interested in
        if (previousBlockOffset == -1) {
          // we have a 'problem', the key we want is the first of the file.
if(KnobRuntime.check(java.util.UUID.fromString("5741fa8e-e88d-3316-b555-6d89ca8956e2"))) {
try {
    java.lang.reflect.Field _knob_field_ = seekToBlock.getClass().getDeclaredField("offset");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(seekToBlock));
    _knob_field_.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          releaseIfNotCurBlock(seekToBlock);
          return false;
        }

        // The first key in the current block 'seekToBlock' is greater than the given
        // seekBefore key. We will go ahead by reading the next block that satisfies the
        // given key. Return the current block before reading the next one.
        releaseIfNotCurBlock(seekToBlock);
        // It is important that we compute and pass onDiskSize to the block
        // reader so that it does not have to read the header separately to
        // figure out the size. Currently, we do not have a way to do this
        // correctly in the general case however.
        // TODO: See https://issues.apache.org/jira/browse/HBASE-14576
        int prevBlockSize = -1;
        seekToBlock = reader.readBlock(previousBlockOffset, prevBlockSize, cacheBlocks, pread,
          isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
        // TODO shortcut: seek forward in this block to the last key of the
        // block.
      }
      loadBlockAndSeekToKey(seekToBlock, firstKey, true, key, true);
      return true;
    }

    /**
     * The curBlock will be released by shipping or close method, so only need to consider releasing
     * the block, which was read from HFile before and not referenced by curBlock.
     */
    protected void releaseIfNotCurBlock(HFileBlock block) {
      if (curBlock != block) {
        block.release();
      }
    }

    /**
     * Scans blocks in the "scanned" section of the {@link HFile} until the next data block is
     * found.
     * @return the next block, or null if there are no more data blocks
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_ON_SOME_PATH",
        justification = "Yeah, unnecessary null check; could do w/ clean up")
    protected HFileBlock readNextDataBlock() throws IOException {
      long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
      if (curBlock == null) {
        return null;
      }
      HFileBlock block = this.curBlock;
      do {
        if (block.getOffset() >= lastDataBlockOffset) {
          releaseIfNotCurBlock(block);
          return null;
        }
        if (block.getOffset() < 0) {
          releaseIfNotCurBlock(block);
          throw new IOException("Invalid block offset=" + block + ", path=" + reader.getPath());
        }
        // We are reading the next block without block type validation, because
        // it might turn out to be a non-data block.
        block = reader.readBlock(block.getOffset() + block.getOnDiskSizeWithHeader(),
          block.getNextBlockOnDiskSize(), cacheBlocks, pread, isCompaction, true, null,
          getEffectiveDataBlockEncoding());
        if (block != null && !block.getBlockType().isData()) {
          // Whatever block we read we will be returning it unless
          // it is a datablock. Just in case the blocks are non data blocks
          block.release();
        }
      } while (!block.getBlockType().isData());
      return block;
    }

    public DataBlockEncoding getEffectiveDataBlockEncoding() {
      return this.reader.getEffectiveEncodingInCache(isCompaction);
    }

    @Override
    public Cell getCell() {
      if (!isSeeked()) {
        return null;
      }

      Cell ret;
      int cellBufSize = getKVBufSize();
      long seqId = 0L;
      if (this.reader.getHFileInfo().shouldIncludeMemStoreTS()) {
        seqId = currMemstoreTS;
      }
      if (blockBuffer.hasArray()) {
        // TODO : reduce the varieties of KV here. Check if based on a boolean
        // we can handle the 'no tags' case.
        if (currTagsLen > 0) {
          ret = new SizeCachedKeyValue(blockBuffer.array(),
            blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId, currKeyLen,
            rowLen);
        } else {
          ret = new SizeCachedNoTagsKeyValue(blockBuffer.array(),
            blockBuffer.arrayOffset() + blockBuffer.position(), cellBufSize, seqId, currKeyLen,
            rowLen);
        }
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("8cb927cd-71ce-35c3-8218-c6668930427c"))) {
cellBufSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("518062bd-c0ab-31f9-b30d-ea466e955854"))) {
cellBufSize /= 2;
}
        ByteBuffer buf = blockBuffer.asSubByteBuffer(cellBufSize);
        if (buf.isDirect()) {
          ret = currTagsLen > 0
            ? new SizeCachedByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId, currKeyLen,
              rowLen)
            : new SizeCachedNoTagsByteBufferKeyValue(buf, buf.position(), cellBufSize, seqId,
              currKeyLen, rowLen);
        } else {
          if (currTagsLen > 0) {
            ret = new SizeCachedKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
              cellBufSize, seqId, currKeyLen, rowLen);
          } else {
            ret = new SizeCachedNoTagsKeyValue(buf.array(), buf.arrayOffset() + buf.position(),
              cellBufSize, seqId, currKeyLen, rowLen);
          }
        }
      }
      return ret;
    }

    @Override
    public Cell getKey() {
      assertSeeked();
      // Create a new object so that this getKey is cached as firstKey, lastKey
      ObjectIntPair<ByteBuffer> keyPair = new ObjectIntPair<>();
      blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, keyPair);
      ByteBuffer keyBuf = keyPair.getFirst();
      if (keyBuf.hasArray()) {
        return new KeyValue.KeyOnlyKeyValue(keyBuf.array(),
          keyBuf.arrayOffset() + keyPair.getSecond(), currKeyLen);
      } else {
        // Better to do a copy here instead of holding on to this BB so that
        // we could release the blocks referring to this key. This key is specifically used
        // in HalfStoreFileReader to get the firstkey and lastkey by creating a new scanner
        // every time. So holding onto the BB (incase of DBB) is not advised here.
        byte[] key = new byte[currKeyLen];
        ByteBufferUtils.copyFromBufferToArray(key, keyBuf, keyPair.getSecond(), 0, currKeyLen);
        return new KeyValue.KeyOnlyKeyValue(key, 0, currKeyLen);
      }
    }

    @Override
    public ByteBuffer getValue() {
      assertSeeked();
      // Okie to create new Pair. Not used in hot path
      ObjectIntPair<ByteBuffer> valuePair = new ObjectIntPair<>();
      this.blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE + currKeyLen,
        currValueLen, valuePair);
      ByteBuffer valBuf = valuePair.getFirst().duplicate();
      valBuf.position(valuePair.getSecond());
      valBuf.limit(currValueLen + valuePair.getSecond());
      return valBuf.slice();
    }

    protected void setNonSeekedState() {
      reset();
      blockBuffer = null;
      currKeyLen = 0;
      currValueLen = 0;
      currMemstoreTS = 0;
      currMemstoreTSLen = 0;
      currTagsLen = 0;
    }

    /**
     * Set the position on current backing blockBuffer.
     */
    private void positionThisBlockBuffer() {
if(KnobRuntime.check(java.util.UUID.fromString("2da622e6-e695-3da4-8cb4-28fa8bbed2fc"))) {
return;
}
      try {
        if (KnobRuntime.check(java.util.UUID.fromString("ec5aa0fa-c3a8-3aff-848e-47d864ab09cf"))) { blockBuffer.skip(getKVBufSize()); } else { blockBuffer.skip(getCurCellSerializedSize()); }
      } catch (IllegalArgumentException e) {
        LOG.error("Current pos = " + blockBuffer.position() + "; currKeyLen = " + currKeyLen
          + "; currValLen = " + currValueLen + "; block limit = " + blockBuffer.limit()
          + "; currBlock currBlockOffset = " + this.curBlock.getOffset() + "; path="
          + reader.getPath());
        throw e;
      }
    }

    /**
     * Set our selves up for the next 'next' invocation, set up next block.
     * @return True is more to read else false if at the end.
     */
    private boolean positionForNextBlock() throws IOException {
      // Methods are small so they get inlined because they are 'hot'.
      long lastDataBlockOffset = reader.getTrailer().getLastDataBlockOffset();
      if (this.curBlock.getOffset() >= lastDataBlockOffset) {
        setNonSeekedState();
        return false;
      }
      return isNextBlock();
    }

    private boolean isNextBlock() throws IOException {
      // Methods are small so they get inlined because they are 'hot'.
      HFileBlock nextBlock = readNextDataBlock();
      if (nextBlock == null) {
        setNonSeekedState();
        return false;
      }
      updateCurrentBlock(nextBlock);
      return true;
    }

    private final boolean _next() throws IOException {
      // Small method so can be inlined. It is a hot one.
      if (blockBuffer.remaining() <= 0) {
        return positionForNextBlock();
      }

      // We are still in the same block.
      readKeyValueLen();
      return true;
    }

    /**
     * Go to the next key/value in the block section. Loads the next block if necessary. If
     * successful, {@link #getKey()} and {@link #getValue()} can be called.
     * @return true if successfully navigated to the next key/value
     */
    @Override
    public boolean next() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("00713367-c3ad-3edf-a585-debee6794191"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("f13b8f15-a57f-3724-b28f-e993f8fec400"))) {
throw new java.io.IOException("Injected exception");
}
      // This is a hot method so extreme measures taken to ensure it is small and inlineable.
      // Checked by setting: -XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining -XX:+PrintCompilation
      assertSeeked();
      positionThisBlockBuffer();
      return _next();
    }

    /**
     * Positions this scanner at the start of the file.
     * @return false if empty file; i.e. a call to next would return false and the current key and
     *         value are undefined.
     */
    @Override
    public boolean seekTo() throws IOException {
      if (reader == null) {
        return false;
      }

      if (reader.getTrailer().getEntryCount() == 0) {
        // No data blocks.
        return false;
      }

      long firstDataBlockOffset = reader.getTrailer().getFirstDataBlockOffset();
      if (((KnobRuntime.check(java.util.UUID.fromString("91e5cb3f-1899-35e1-8618-611cd623b4c2"))) ? (((curBlock) != (null)) || (curBlock.getOffset() == firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("b0e48c54-dd9b-33ff-9f4e-4b77bfe4a902"))) ? (curBlock.getOffset() == firstDataBlockOffset) : (((KnobRuntime.check(java.util.UUID.fromString("8b7dc004-5cfa-3e08-88a0-0e7c6432370e"))) ? (((curBlock) == (null)) || ((curBlock.getOffset()) == (firstDataBlockOffset))) : (curBlock != null && curBlock.getOffset() == firstDataBlockOffset))))))) {
        return processFirstDataBlock();
      }

if(KnobRuntime.check(java.util.UUID.fromString("0af0ddee-fc8b-37fd-b974-53bcc86a167b"))) {
firstDataBlockOffset += 1;
}
      readAndUpdateNewBlock(firstDataBlockOffset);
      return true;
    }

    protected boolean processFirstDataBlock() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("71b4ad5c-807e-300b-a133-83eed3b15b43"))) {
return false;
}
      blockBuffer.rewind();
      readKeyValueLen();
      return true;
    }

    protected void readAndUpdateNewBlock(long firstDataBlockOffset) throws IOException {
      HFileBlock newBlock = reader.readBlock(firstDataBlockOffset, -1, cacheBlocks, pread,
        isCompaction, true, BlockType.DATA, getEffectiveDataBlockEncoding());
      if (newBlock.getOffset() < 0) {
        releaseIfNotCurBlock(newBlock);
        throw new IOException(
          "Invalid offset=" + newBlock.getOffset() + ", path=" + reader.getPath());
      }
      updateCurrentBlock(newBlock);
    }

    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey, boolean rewind,
      Cell key, boolean seekBefore) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("eb082d98-f559-39ef-9f7f-19fb3ab5aa8d"))) {
try {
    java.lang.reflect.Field _knob_field_ = seekToBlock.getClass().getDeclaredField("offset");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(seekToBlock));
    _knob_field_.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("00db7507-3986-3d9f-84d5-bd9aadfacdb5"))) {
try {
    java.lang.reflect.Field _knob_field_ = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(seekToBlock));
    _knob_field_.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1708f68-5cf7-3c70-84f8-58cfd55613de"))) {
try {
    java.lang.reflect.Field _knob_field_ = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(seekToBlock));
    _knob_field_.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3d8b844-36cd-345b-ba6b-944b359a16c9"))) {
try {
    java.lang.reflect.Field _knob_field_ = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(seekToBlock));
    _knob_field_.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (((KnobRuntime.check(java.util.UUID.fromString("8ba74458-14f8-3179-9d02-a40dff5e5620"))) ? (((this.curBlock) != (null)) && ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("a2f52bdb-1649-3530-b85e-1b80cb70591e"))) ? (((this.curBlock) != (null)) || ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("9f8cb602-41d2-3d42-856c-57d1218b6aff"))) ? (((this.curBlock) == (null)) && ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("5895e482-eb9f-320e-be90-262600eb7305"))) ? (this.curBlock == null) : (((KnobRuntime.check(java.util.UUID.fromString("dd5f9606-49a5-3f29-a478-8ed9a408e5fa"))) ? (((this.curBlock) == (null)) || (this.curBlock.getOffset() != seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("a9e83cf3-4f1c-3a17-8e36-7a8ba68c6f11"))) ? ((this.curBlock == null) || ((this.curBlock.getOffset()) != (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("afd46cf3-d8d8-3181-a025-7d439d48a45a"))) ? (((this.curBlock) != (null)) || ((this.curBlock.getOffset()) == (seekToBlock.getOffset()))) : (this.curBlock == null || this.curBlock.getOffset() != seekToBlock.getOffset()))))))))))))))) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        blockBuffer.rewind();
      }
      // Update the nextIndexedKey
      this.nextIndexedKey = nextIndexedKey;
      return blockSeek(key, seekBefore);
    }

    /** Returns True if v &lt;= 0 or v &gt; current block buffer limit. */
    protected final boolean checkKeyLen(final int v) {
      return ((KnobRuntime.check(java.util.UUID.fromString("a3049ea6-cdd4-393e-9ed5-ed2f68bb0eb8"))) ? (((v) != (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("24432396-4757-3b0f-9e0a-cc84390aea0f"))) ? (((v) >= (0)) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e3e29593-9c18-3cf9-a16a-a25219461bc9"))) ? (((v) >= (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4e8cf0c6-ae19-3f38-ae0a-99598eafd268"))) ? (((v) >= (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4dbf10c8-f425-3ae7-a951-5443089f58a5"))) ? (((v) != (0)) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c9d4e7e5-47b6-31b3-b6a8-f948eb00d1be"))) ? (((v) < (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7d03e8b6-c348-3207-8c11-0bb5312f7203"))) ? (((v) == (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d12ffbc7-048c-3dc5-ba36-696a0d00d470"))) ? (((v) == (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a7e00f9c-fd94-3de0-907e-38e21910b6ef"))) ? (((v) < (0)) && ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("52b9cbfd-3bdd-3c26-85a9-6fa25aaab6e5"))) ? ((v <= 0) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("31ad87c3-b9ca-3bfc-bfda-774dc68df33d"))) ? ((v <= 0) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9b010a1c-b743-3ea6-8a9f-8344ebea300a"))) ? (((v) <= (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("04e07709-24df-3d51-9ac6-193f8b0af415"))) ? (((v) == (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("6757a1b3-a17d-364a-a452-379a71c937c0"))) ? (((v) > (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("12c8368e-953b-3827-a1b8-f19a8637d886"))) ? (((v) > (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("30478b24-f6ae-3f1e-b2f3-7abde64b9a0a"))) ? (((v) <= (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f628811e-07a6-31c9-a893-606b123d596b"))) ? (((v) < (0)) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("cf0c29d9-1edc-3345-81bb-aa67324bf4fc"))) ? ((0) == (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("67677623-b554-373c-9f13-5498901dee1e"))) ? ((v <= 0) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("6075fb67-5a80-3a64-8274-c4754b46f557"))) ? ((v) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7afa5ba6-d99e-3232-a3d2-3039736ddaa4"))) ? (((v) != (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("34bfe1cf-6e55-3fa3-9c93-fc8d33b2e145"))) ? (((v) < (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a66875a8-e018-3abd-8ac6-cc6f1b67f2d3"))) ? (((v) < (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("201cfec4-b2c4-3241-96cf-7592ced012ba"))) ? (v <= 0) : (((KnobRuntime.check(java.util.UUID.fromString("e96ec5a6-01ed-34ce-932b-044efedfb302"))) ? ((v <= 0) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("6b59e256-09d8-3789-a0e2-454f60426326"))) ? (((v) > (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("dedfe266-77ee-3e8e-b285-fac6a58e5f8f"))) ? (((v) >= (0)) || ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e0e7806f-a512-3a98-813f-b77132b2f2ce"))) ? (((v) < (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("cd8c1033-8a58-38f3-80da-5ed201f36b87"))) ? (((v) > (0)) && ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("bbb47563-a87d-3e16-bbfa-961345c3252d"))) ? (((v) <= (0)) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c652b22e-392c-3139-bfc5-003f10d4eae6"))) ? (((v) >= (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("5807fc00-0a3a-379f-8f8c-9206dad4e5a3"))) ? (((v) != (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("563257fd-6726-355c-95e8-d31ce37a8135"))) ? ((0) < (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("bb16d69b-a614-30c1-a393-8932a27f381f"))) ? (((v) >= (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("9ca71282-0795-3de8-96bc-9586412125ba"))) ? ((0) != (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("1a354050-ecdd-3fa0-9f53-5cd594554b5e"))) ? (((v) <= (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4104acc3-bb75-3f50-b970-e55a7eed615b"))) ? ((0) >= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("4c8f1a2a-0a5a-3614-ac66-a602101f123a"))) ? (((v) > (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("cc5df0f4-e4ac-3bf4-a4f1-b94fa6fe2e3e"))) ? (v > this.blockBuffer.limit()) : (((KnobRuntime.check(java.util.UUID.fromString("5b9326c5-b4fb-3ca0-96eb-6f9759ed6a0a"))) ? (((v) <= (0)) && ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("5e95e360-2715-340f-93f9-39de74b234bc"))) ? (((v) > (0)) && ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("1ef00662-762d-3249-ad9b-c86b3499f5ad"))) ? (((v) == (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("a1ac2015-37bf-3c78-b9d1-c8f522349596"))) ? (((v) == (0)) && (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("a9f83f07-c027-363c-8422-ad0c4aec306e"))) ? ((v) >= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("76804977-180f-33c2-a102-a513ce472c09"))) ? (((v) != (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("785eb82a-22ba-3aa5-a2c6-df26c60df036"))) ? ((v <= 0) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("81887c77-d5d2-3c60-b21e-6ed50eeacb4a"))) ? (((v) != (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5474e8c1-71fe-337a-a662-d7ae0ffc2104"))) ? (((v) != (0)) && ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("34050ed5-7649-3338-8a56-59bdcab0c2d9"))) ? (((v) == (0)) || ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c1b9267a-80cc-3ecb-87d9-88206348e84e"))) ? (((v) > (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ce7d3892-2036-378e-adc6-eb495cc3c64f"))) ? ((v) == (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("dc2e20f3-d069-3c99-b65c-6a2d0a5eedfd"))) ? (((v) == (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c26719b0-321c-32a6-b122-75bff578b019"))) ? (((v) > (0)) && ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e009b398-40f1-37e3-a660-bdb2cb265f6d"))) ? (((v) >= (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("038a7a67-67f7-3319-85ac-8e61d9a5c76a"))) ? (((v) != (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("15168490-684c-3900-9f6d-47cd1d03b304"))) ? (((v) != (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4981c0bd-02fa-3792-b7fa-4b0a2f2b690b"))) ? (((v) > (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("1c502421-d35d-3cb7-a8cb-b77766b3b33e"))) ? (((v) >= (0)) && (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("0f58d228-53dc-3f09-98c9-599a4ec14963"))) ? (((v) > (0)) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("805eee77-99e5-37eb-834f-bc965a7814ae"))) ? (((v) < (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("66005e0f-7a55-35c2-8e70-95094f93ae5c"))) ? (((v) >= (0)) || ((v) > (getKVBufSize()))) : (v <= 0 || v > this.blockBuffer.limit()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
    }

    /** Returns True if v &lt; 0 or v &gt; current block buffer limit. */
    protected final boolean checkLen(final int v) {
      return ((KnobRuntime.check(java.util.UUID.fromString("8bdbd858-389d-302d-9bb4-75eac0ed2a35"))) ? ((v < 0) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("53e16bcf-5f76-338a-bba5-7e1d7cc2cced"))) ? (((v) != (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("30f7d084-89a9-3858-8b68-f45a8604028e"))) ? (((v) >= (0)) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("58b628b6-9c31-33bc-8c99-38ea527b0139"))) ? (((v) <= (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("47cc64a5-c1c9-3919-8a2e-3781195b8d47"))) ? (((v) > (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("3cac0609-ca9d-3229-93d5-353b85199a4b"))) ? (((v) == (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("27f5b4d9-c50c-36ad-ba3e-e612cb5ad185"))) ? ((v) >= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("1bf5a7f4-894a-340b-bc9e-fbbc26c2c63e"))) ? (((v) > (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d883f4fa-9a8d-31ce-9920-5856349aed1d"))) ? (((v) < (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ccbe50ab-dcda-3fdc-b2d5-3f4d28bd86cc"))) ? (((v) < (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ab2f7dbc-d48f-3bde-a0bc-97cb8da600fe"))) ? (((v) != (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("799507a7-7c07-3834-9223-b2fb9398eba2"))) ? (((v) == (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("17f3613c-190b-35c9-9f63-9f36bb09e7e2"))) ? (((v) <= (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("13fffc22-add5-3d5d-b108-c5d51c0e2d25"))) ? (((v) != (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("14fa625c-998e-3d43-90c8-41a8d598db50"))) ? (((v) == (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("028d0786-f620-3b47-a45a-05c832d22d49"))) ? (((v) >= (0)) && ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e9cea280-975e-3293-85ae-97b29b7ef044"))) ? (((v) > (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e8cc88d4-510e-3ca2-a7c0-b3e79430348d"))) ? (((v) >= (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d51c373e-2359-38e6-8682-15acbd93bb35"))) ? ((v < 0) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3468e27b-e659-3c8c-8949-987188174281"))) ? (((v) < (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("dc89365f-3236-305d-ad1a-f435f1cdead5"))) ? (((v) == (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("b30e2327-4d5a-3a24-8b25-9404c779d455"))) ? ((0) < (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("c5e5498d-f686-3ce5-9ecc-3d84b2c52936"))) ? (((v) != (0)) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("477fd9e3-de59-3ddf-bafb-2fea11c81d63"))) ? ((v < 0) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("14ff9e00-fb42-3d6b-89e1-122423a56518"))) ? (((v) == (0)) || ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("cec23e19-bb49-32e4-a64f-7e3ce08ad70c"))) ? (((v) != (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("8c9c9602-9d96-3164-a36b-9acb989b39e9"))) ? (((v) == (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ccd0303f-b768-3f7d-9865-9360e2d92e3d"))) ? (((v) >= (0)) || ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f3d0b8fd-0b85-342d-9a8c-d3a4fb621d32"))) ? ((v) > (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("3f40eec6-1f79-3307-9097-4a2f8e63b977"))) ? (((v) >= (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ed2d3a9b-948a-3dac-bd56-3025362c7c88"))) ? (((v) < (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("0f69b84c-6f74-371b-b8b2-b841d817962a"))) ? (((v) >= (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("239f18d0-1585-3933-b2cc-20381219db79"))) ? (((v) < (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("bf7320c4-8ca8-3fb9-af48-2938586b120d"))) ? (((v) == (0)) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("93e522fe-cf8f-3693-8324-43561a09874a"))) ? (((v) == (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a1c9d541-588a-3e83-aa24-5b1f4aadca57"))) ? (((v) >= (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("daa7be72-c63f-34b6-b1bf-699f4613a376"))) ? (((v) < (0)) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7956552c-4bca-3390-a994-efdd275bb197"))) ? ((v < 0) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("8f7edfb4-a9e9-3a95-9665-a216013044fe"))) ? (((v) != (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d1e71dce-cbce-3ad3-bf82-e1c68ae67c78"))) ? (((v) <= (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("63bf1c01-a234-3d17-853d-487471796ab6"))) ? (((v) < (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("1bb45326-c0fe-3015-98e7-127816f80157"))) ? ((v < 0) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("972c9612-5365-3142-98bd-91da3ac04651"))) ? ((v < 0) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f5483e22-d0c0-347f-82f5-a17bd4b81098"))) ? ((v < 0) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c8cefa30-ba3f-319d-a68f-0614d2100b0a"))) ? (((v) == (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("33b3fc18-5dfb-39c6-9c44-9e21d1c5c7fc"))) ? (((v) <= (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7957c553-e961-3d47-b289-0f5819178d6d"))) ? (((v) >= (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("b1a5b3c6-811a-35a0-b3c9-e39147d53ba0"))) ? (((v) <= (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f88b4ba5-1a98-3958-a8bd-7bb41a3c7ab6"))) ? (((v) >= (0)) && ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("88803923-54e0-3113-9d4b-395da0c72308"))) ? (((v) == (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("8fb640d3-db7b-35bd-958e-7ac5f2cb71a4"))) ? (((v) >= (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ee1c20c5-f9fa-3984-8639-e63d750792dd"))) ? (((v) == (0)) || ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("9292b43a-2ea1-3ee3-9f58-9f435296a74d"))) ? ((v < 0) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("88b45bc7-a974-3e21-ab49-d4bc6c3e8c62"))) ? (((v) > (0)) && ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("fb235134-a682-30cd-96c1-b1178d0a0e2e"))) ? (((v) < (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f03049f2-ce15-3f7c-a08c-e507a581f1d1"))) ? ((v < 0) || ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("24dcc329-3bfb-3477-bcf6-9a5dad2e12b9"))) ? ((v) >= (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("94b90897-9902-384d-9106-b390c66a5c07"))) ? ((0) == (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("0d251261-2ec7-398d-a439-340aab1690f5"))) ? (((v) <= (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("14ce2a3c-4a9d-3669-809d-3c85be02f255"))) ? (((v) >= (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("59d9e17e-b0d0-3978-ac52-8f6a58182e97"))) ? ((v < 0) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("3a1a714e-e66f-3fbe-a48c-1ae2008c74b3"))) ? ((0) > (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("55b9d10c-2b9f-3402-835e-dfd92c28c799"))) ? (((v) > (0)) || ((v) == (this.blockBuffer.limit()))) : (v < 0 || v > this.blockBuffer.limit()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
    }

    /**
     * Check key and value lengths are wholesome.
     */
    protected final void checkKeyValueLen() {
      if (checkKeyLen(this.currKeyLen) || checkLen(this.currValueLen)) {
        throw new IllegalStateException("Invalid currKeyLen " + this.currKeyLen
          + " or currValueLen " + this.currValueLen + ". Block offset: " + this.curBlock.getOffset()
          + ", block length: " + this.blockBuffer.limit() + ", position: "
          + this.blockBuffer.position() + " (without header)." + ", path=" + reader.getPath());
      }
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to the the first
     * key/value pair.
     * @param newBlock the block read by {@link HFileReaderImpl#readBlock}, it's a totally new block
     *                 with new allocated {@link ByteBuff}, so if no further reference to this
     *                 block, we should release it carefully.
     */
    protected void updateCurrentBlock(HFileBlock newBlock) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d68f2ed2-d3c8-3cd6-a166-511f62537a17"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("offset");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab2ec96b-53e9-360c-92e8-cc8ae735ebd4"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f59f57b3-c50e-3682-9414-ee79a6b9f271"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      try {
        if (newBlock.getBlockType() != BlockType.DATA) {
          throw new IllegalStateException(
            "ScannerV2 works only on data blocks, got " + newBlock.getBlockType() + "; "
              + "HFileName=" + reader.getPath() + ", " + "dataBlockEncoder="
              + reader.getDataBlockEncoding() + ", " + "isCompaction=" + isCompaction);
        }
        updateCurrBlockRef(newBlock);
        blockBuffer = newBlock.getBufferWithoutHeader();
        readKeyValueLen();
      } finally {
if(KnobRuntime.check(java.util.UUID.fromString("6809e974-74af-3ff0-aa5c-2aab615f2a9a"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0412e8aa-0a2e-39c9-a7e2-8150f554c1cd"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb725ac0-13f7-3d89-81f3-b0eeb0587b5f"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0991fecc-32bc-3aed-8f14-3359dce47e8b"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        releaseIfNotCurBlock(newBlock);
      }
      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
if(KnobRuntime.check(java.util.UUID.fromString("118b82cb-d584-3bc4-8a62-8aad9365068e"))) {
try {
    java.lang.reflect.Field _knob_field_ = curBlock.getClass().getDeclaredField("prevBlockOffset");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(curBlock));
    _knob_field_.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("94b4dbfd-5e3c-3e9f-9e81-1bc6e4f32001"))) {
try {
    java.lang.reflect.Field _knob_field_ = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(curBlock));
    _knob_field_.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      ByteBuff buffer = curBlock.getBufferWithoutHeader();
      // It is safe to manipulate this buffer because we own the buffer object.
      buffer.rewind();
      int klen = buffer.getInt();
      buffer.skip(Bytes.SIZEOF_INT);// Skip value len part
      ByteBuffer keyBuff = buffer.asSubByteBuffer(klen);
      if (keyBuff.hasArray()) {
        return new KeyValue.KeyOnlyKeyValue(keyBuff.array(),
          keyBuff.arrayOffset() + keyBuff.position(), klen);
      } else {
        return new ByteBufferKeyOnlyKeyValue(keyBuff, keyBuff.position(), klen);
      }
    }

    @Override
    public String getKeyString() {
      return CellUtil.toString(getKey(), false);
    }

    @Override
    public String getValueString() {
      return ByteBufferUtils.toStringBinary(getValue());
    }

    public int compareKey(CellComparator comparator, Cell key) {
      blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, pair);
      if (KnobRuntime.check(java.util.UUID.fromString("d58bd34f-af84-3de4-a609-784d0af915f3"))) { this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), getCurCellSerializedSize(), currKeyLen, rowLen); } else { this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), currKeyLen, rowLen); }
      return PrivateCellUtil.compareKeyIgnoresMvcc(comparator, key, this.bufBackedKeyOnlyKv);
    }

    @Override
    public void shipped() throws IOException {
      this.returnBlocks(false);
    }
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return dataBlockEncoder.getDataBlockEncoding();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /** Minor versions in HFile starting with this number have hbase checksums */
  public static final int MINOR_VERSION_WITH_CHECKSUM = 1;
  /** In HFile minor version that does not support checksums */
  public static final int MINOR_VERSION_NO_CHECKSUM = 0;

  /** HFile minor version that introduced pbuf filetrailer */
  public static final int PBUF_TRAILER_MINOR_VERSION = 2;

  /**
   * The size of a (key length, value length) tuple that prefixes each entry in a data block.
   */
  public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

  /**
   * Retrieve block from cache. Validates the retrieved block's type vs {@code expectedBlockType}
   * and its encoding vs. {@code expectedDataBlockEncoding}. Unpacks the block as necessary.
   */
  private HFileBlock getCachedBlock(BlockCacheKey cacheKey, boolean cacheBlock, boolean useLock,
    boolean updateCacheMetrics, BlockType expectedBlockType,
    DataBlockEncoding expectedDataBlockEncoding) throws IOException {
    // Check cache for block. If found return.
    BlockCache cache = cacheConf.getBlockCache().orElse(null);
    if (cache != null) {
      HFileBlock cachedBlock =
        (HFileBlock) cache.getBlock(cacheKey, cacheBlock, useLock, updateCacheMetrics);
      if (cachedBlock != null) {
        if (cacheConf.shouldCacheCompressed(cachedBlock.getBlockType().getCategory())) {
          HFileBlock compressedBlock = cachedBlock;
if(KnobRuntime.check(java.util.UUID.fromString("64cab956-5b7f-35dd-9252-da387b4efeda"))) {
try {
    java.lang.reflect.Field _knob_field_ = hfileContext.getClass().getDeclaredField("blockSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(hfileContext));
    _knob_field_.set(hfileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          cachedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
          // In case of compressed block after unpacking we can release the compressed block
          if (compressedBlock != cachedBlock) {
            compressedBlock.release();
          }
        }
        try {
          validateBlockType(cachedBlock, expectedBlockType);
        } catch (IOException e) {
          returnAndEvictBlock(cache, cacheKey, cachedBlock);
          throw e;
        }

        if (expectedDataBlockEncoding == null) {
          return cachedBlock;
        }
        DataBlockEncoding actualDataBlockEncoding = cachedBlock.getDataBlockEncoding();
        // Block types other than data blocks always have
        // DataBlockEncoding.NONE. To avoid false negative cache misses, only
        // perform this check if cached block is a data block.
        if (
          cachedBlock.getBlockType().isData()
            && !actualDataBlockEncoding.equals(expectedDataBlockEncoding)
        ) {
          // This mismatch may happen if a Scanner, which is used for say a
          // compaction, tries to read an encoded block from the block cache.
          // The reverse might happen when an EncodedScanner tries to read
          // un-encoded blocks which were cached earlier.
          //
          // Because returning a data block with an implicit BlockType mismatch
          // will cause the requesting scanner to throw a disk read should be
          // forced here. This will potentially cause a significant number of
          // cache misses, so update so we should keep track of this as it might
          // justify the work on a CompoundScanner.
          if (
            !expectedDataBlockEncoding.equals(DataBlockEncoding.NONE)
              && !actualDataBlockEncoding.equals(DataBlockEncoding.NONE)
          ) {
            // If the block is encoded but the encoding does not match the
            // expected encoding it is likely the encoding was changed but the
            // block was not yet evicted. Evictions on file close happen async
            // so blocks with the old encoding still linger in cache for some
            // period of time. This event should be rare as it only happens on
            // schema definition change.
            LOG.info(
              "Evicting cached block with key {} because data block encoding mismatch; "
                + "expected {}, actual {}, path={}",
              cacheKey, actualDataBlockEncoding, expectedDataBlockEncoding, path);
            // This is an error scenario. so here we need to release the block.
            returnAndEvictBlock(cache, cacheKey, cachedBlock);
          }
          return null;
        }
        return cachedBlock;
      }
    }
    return null;
  }

  private void returnAndEvictBlock(BlockCache cache, BlockCacheKey cacheKey, Cacheable block) {
    block.release();
    cache.evictBlock(cacheKey);
  }

  /**
   * @param cacheBlock Add block to cache, if found
   * @return block wrapped in a ByteBuffer, with header skipped
   */
  @Override
  public HFileBlock getMetaBlock(String metaBlockName, boolean cacheBlock) throws IOException {
    if (trailer.getMetaIndexCount() == 0) {
      return null; // there are no meta blocks
    }
    if (metaBlockIndexReader == null) {
      throw new IOException(path + " meta index not loaded");
    }

    byte[] mbname = Bytes.toBytes(metaBlockName);
    int block = metaBlockIndexReader.rootBlockContainingKey(mbname, 0, mbname.length);
    if (block == -1) {
      return null;
    }
    long blockSize = metaBlockIndexReader.getRootBlockDataSize(block);

    // Per meta key from any given file, synchronize reads for said block. This
    // is OK to do for meta blocks because the meta block index is always
    // single-level.
    synchronized (metaBlockIndexReader.getRootBlockKey(block)) {
      // Check cache for block. If found return.
      long metaBlockOffset = metaBlockIndexReader.getRootBlockOffset(block);
      BlockCacheKey cacheKey =
        new BlockCacheKey(name, metaBlockOffset, this.isPrimaryReplicaReader(), BlockType.META);

      cacheBlock &= cacheConf.shouldCacheBlockOnRead(BlockType.META.getCategory());
      HFileBlock cachedBlock =
        getCachedBlock(cacheKey, cacheBlock, false, true, BlockType.META, null);
      if (cachedBlock != null) {
        assert cachedBlock.isUnpacked() : "Packed block leak.";
        // Return a distinct 'shallow copy' of the block,
        // so pos does not get messed by the scanner
        return cachedBlock;
      }
      // Cache Miss, please load.

      HFileBlock compressedBlock =
        fsBlockReader.readBlockData(metaBlockOffset, blockSize, true, false, true);
      HFileBlock uncompressedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
      if (compressedBlock != uncompressedBlock) {
        compressedBlock.release();
      }

      // Cache the block
      if (cacheBlock) {
        cacheConf.getBlockCache().ifPresent(
          cache -> cache.cacheBlock(cacheKey, uncompressedBlock, cacheConf.isInMemory()));
      }
      return uncompressedBlock;
    }
  }

  /**
   * Whether we use heap or not depends on our intent to cache the block. We want to avoid
   * allocating to off-heap if we intend to cache into the on-heap L1 cache. Otherwise, it's more
   * efficient to allocate to off-heap since we can control GC ourselves for those. So our decision
   * here breaks down as follows: <br>
   * If block cache is disabled, don't use heap. If we're not using the CombinedBlockCache, use heap
   * unless caching is disabled for the request. Otherwise, only use heap if caching is enabled and
   * the expected block type is not DATA (which goes to off-heap L2 in combined cache).
   * @see org.apache.hadoop.hbase.io.hfile.HFileBlock.FSReader#readBlockData(long, long, boolean,
   *      boolean, boolean)
   */
  private boolean shouldUseHeap(BlockType expectedBlockType, boolean cacheBlock) {
    if (!cacheConf.getBlockCache().isPresent()) {
      return false;
    }

    // we only cache a block if cacheBlock is true and caching-on-read is enabled in CacheConfig
    // we can really only check for that if have an expectedBlockType
    if (expectedBlockType != null) {
      cacheBlock &= cacheConf.shouldCacheBlockOnRead(expectedBlockType.getCategory());
    }

    if (!cacheConf.isCombinedBlockCache()) {
      // Block to cache in LruBlockCache must be an heap one, if caching enabled. So just allocate
      // block memory from heap for saving an extra off-heap to heap copying in that case.
      return cacheBlock;
    }

    return cacheBlock && expectedBlockType != null && !expectedBlockType.isData();
  }

  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize, final boolean cacheBlock,
    boolean pread, final boolean isCompaction, boolean updateCacheMetrics,
    BlockType expectedBlockType, DataBlockEncoding expectedDataBlockEncoding) throws IOException {
    return readBlock(dataBlockOffset, onDiskBlockSize, cacheBlock, pread, isCompaction,
      updateCacheMetrics, expectedBlockType, expectedDataBlockEncoding, false);
  }

  @Override
  public HFileBlock readBlock(long dataBlockOffset, long onDiskBlockSize, final boolean cacheBlock,
    boolean pread, final boolean isCompaction, boolean updateCacheMetrics,
    BlockType expectedBlockType, DataBlockEncoding expectedDataBlockEncoding, boolean cacheOnly)
    throws IOException {
    if (dataBlockIndexReader == null) {
      throw new IOException(path + " block index not loaded");
    }
    long trailerOffset = trailer.getLoadOnOpenDataOffset();
    if (dataBlockOffset < 0 || dataBlockOffset >= trailerOffset) {
      throw new IOException("Requested block is out of range: " + dataBlockOffset
        + ", lastDataBlockOffset: " + trailer.getLastDataBlockOffset()
        + ", trailer.getLoadOnOpenDataOffset: " + trailerOffset + ", path=" + path);
    }
    // For any given block from any given file, synchronize reads for said
    // block.
    // Without a cache, this synchronizing is needless overhead, but really
    // the other choice is to duplicate work (which the cache would prevent you
    // from doing).

    BlockCacheKey cacheKey =
      new BlockCacheKey(path, dataBlockOffset, this.isPrimaryReplicaReader(), expectedBlockType);

    boolean cacheable = cacheBlock && cacheIfCompactionsOff();

    boolean useLock = false;
    IdLock.Entry lockEntry = null;
    final Span span = Span.current();
    // BlockCacheKey#toString() is quite expensive to call, so if tracing isn't enabled, don't
    // record
    Attributes attributes = span.isRecording()
      ? Attributes.of(BLOCK_CACHE_KEY_KEY, cacheKey.toString())
      : Attributes.empty();
    try {
      while (true) {
        // Check cache for block. If found return.
        if (cacheConf.shouldReadBlockFromCache(expectedBlockType) && !cacheOnly) {
          if (useLock) {
            lockEntry = offsetLock.getLockEntry(dataBlockOffset);
          }
          // Try and get the block from the block cache. If the useLock variable is true then this
          // is the second time through the loop and it should not be counted as a block cache miss.
          HFileBlock cachedBlock = getCachedBlock(cacheKey, cacheBlock, useLock, updateCacheMetrics,
            expectedBlockType, expectedDataBlockEncoding);
          if (cachedBlock != null) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Block for file {} is coming from Cache {}",
                Bytes.toString(cachedBlock.getHFileContext().getTableName()), cachedBlock);
            }
            span.addEvent("block cache hit", attributes);
            assert cachedBlock.isUnpacked() : "Packed block leak.";
            if (cachedBlock.getBlockType().isData()) {
              if (((KnobRuntime.check(java.util.UUID.fromString("676d41d5-3e7b-3031-a750-f142d8a4ae04"))) ? (!updateCacheMetrics) : (updateCacheMetrics))) {
                HFile.DATABLOCK_READ_COUNT.increment();
              }
              // Validate encoding type for data blocks. We include encoding
              // type in the cache key, and we expect it to match on a cache hit.
              if (cachedBlock.getDataBlockEncoding() != dataBlockEncoder.getDataBlockEncoding()) {
                // Remember to release the block when in exceptional path.
                cacheConf.getBlockCache().ifPresent(cache -> {
                  returnAndEvictBlock(cache, cacheKey, cachedBlock);
                });
                throw new IOException("Cached block under key " + cacheKey + " "
                  + "has wrong encoding: " + cachedBlock.getDataBlockEncoding() + " (expected: "
                  + dataBlockEncoder.getDataBlockEncoding() + "), path=" + path);
              }
            }
            // Cache-hit. Return!
            return cachedBlock;
          }

          if (((KnobRuntime.check(java.util.UUID.fromString("e5779408-6e68-3afb-83ed-c312c3239c48"))) ? (((!useLock) && (cacheable)) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("34ce8fba-3c9b-3161-82e5-c6a151dbb4fa"))) ? (!cacheable) : (((KnobRuntime.check(java.util.UUID.fromString("d882445b-d92e-3ed1-a312-dd33ab858968"))) ? (((!useLock) || (!cacheable)) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("fef5f936-0dd1-3081-9dbd-47d3b3055492"))) ? ((!cacheable) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (!useLock && cacheable && cacheConf.shouldLockOnCacheMiss(expectedBlockType)))))))))) {
            // check cache again with lock
            useLock = true;
            continue;
          }
          // Carry on, please load.
        }

        span.addEvent("block cache miss", attributes);
        // Load block from filesystem.
        HFileBlock hfileBlock = fsBlockReader.readBlockData(dataBlockOffset, onDiskBlockSize, pread,
          !isCompaction, shouldUseHeap(expectedBlockType, cacheable));
        try {
          validateBlockType(hfileBlock, expectedBlockType);
        } catch (IOException e) {
          hfileBlock.release();
          throw e;
        }
        BlockType.BlockCategory category = hfileBlock.getBlockType().getCategory();
        final boolean cacheCompressed = cacheConf.shouldCacheCompressed(category);
        final boolean cacheOnRead = cacheConf.shouldCacheBlockOnRead(category);

        // Don't need the unpacked block back and we're storing the block in the cache compressed
        if (((KnobRuntime.check(java.util.UUID.fromString("fdd2b027-7caf-33ea-a022-c132a9a727f0"))) ? (((cacheOnly) || (cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("b4992bf3-5fb2-3499-aecc-00898bdcf084"))) ? (((!cacheOnly) && (!cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("1f008a1d-f6bb-35e1-83df-34fe395db490"))) ? (((!cacheOnly) || (cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("b70f0319-a1de-39f9-9d1f-25d07e61ee4a"))) ? ((cacheCompressed) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("d8d2df7d-1ade-3eb3-927e-662e6853b3f4"))) ? ((cacheOnly) || (cacheCompressed)) : (cacheOnly && cacheCompressed && cacheOnRead))))))))))) {
          cacheConf.getBlockCache().ifPresent(cache -> {
            LOG.debug("Skipping decompression of block {} in prefetch", cacheKey);
            // Cache the block if necessary
            if (cacheable && cacheConf.shouldCacheBlockOnRead(category)) {
              cache.cacheBlock(cacheKey, hfileBlock, cacheConf.isInMemory(), cacheOnly);
            }
          });

          if (updateCacheMetrics && hfileBlock.getBlockType().isData()) {
            HFile.DATABLOCK_READ_COUNT.increment();
          }
          return hfileBlock;
        }
        HFileBlock unpacked = hfileBlock.unpack(hfileContext, fsBlockReader);
        // Cache the block if necessary
        cacheConf.getBlockCache().ifPresent(cache -> {
          if (cacheable && cacheConf.shouldCacheBlockOnRead(category)) {
            // Using the wait on cache during compaction and prefetching.
            cache.cacheBlock(cacheKey, cacheCompressed ? hfileBlock : unpacked,
              cacheConf.isInMemory(), cacheOnly);
          }
        });
        if (unpacked != hfileBlock) {
          // End of life here if hfileBlock is an independent block.
          hfileBlock.release();
        }
        if (updateCacheMetrics && hfileBlock.getBlockType().isData()) {
          HFile.DATABLOCK_READ_COUNT.increment();
        }

        return unpacked;
      }
    } finally {
      if (lockEntry != null) {
if(KnobRuntime.check(java.util.UUID.fromString("3db08705-39d8-3c97-9816-f47655c1874d"))) {
try {
    java.lang.reflect.Field _knob_field_ = lockEntry.getClass().getDeclaredField("numWaiters");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(lockEntry));
    _knob_field_.set(lockEntry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        offsetLock.releaseLockEntry(lockEntry);
      }
    }
  }

  @Override
  public boolean hasMVCCInfo() {
    return ((KnobRuntime.check(java.util.UUID.fromString("bc15eba9-563f-3155-b0dc-72adcdf3ebd5"))) ? ((fileInfo.shouldIncludeMemStoreTS()) || (fileInfo.isDecodeMemstoreTS())) : (fileInfo.shouldIncludeMemStoreTS() && fileInfo.isDecodeMemstoreTS()));
  }

  /**
   * Compares the actual type of a block retrieved from cache or disk with its expected type and
   * throws an exception in case of a mismatch. Expected block type of {@link BlockType#DATA} is
   * considered to match the actual block type [@link {@link BlockType#ENCODED_DATA} as well.
   * @param block             a block retrieved from cache or disk
   * @param expectedBlockType the expected block type, or null to skip the check
   */
  private void validateBlockType(HFileBlock block, BlockType expectedBlockType) throws IOException {
    if (expectedBlockType == null) {
      return;
    }
    BlockType actualBlockType = block.getBlockType();
    if (expectedBlockType.isData() && actualBlockType.isData()) {
      // We consider DATA to match ENCODED_DATA for the purpose of this
      // verification.
      return;
    }
    if (actualBlockType != expectedBlockType) {
      throw new IOException("Expected block type " + expectedBlockType + ", " + "but got "
        + actualBlockType + ": " + block + ", path=" + path);
    }
  }

  /**
   * @return Last key as cell in the file. May be null if file has no entries. Note that this is not
   *         the last row key, but it is the Cell representation of the last key
   */
  @Override
  public Optional<Cell> getLastKey() {
    return dataBlockIndexReader.isEmpty()
      ? Optional.empty()
      : Optional.of(fileInfo.getLastKeyCell());
  }

  /**
   * @return Midkey for this file. We work with block boundaries only so returned midkey is an
   *         approximation only.
   */
  @Override
  public Optional<Cell> midKey() throws IOException {
    return Optional.ofNullable(dataBlockIndexReader.midkey(this));
  }

  @Override
  public void close() throws IOException {
    close(cacheConf.shouldEvictOnClose());
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    return dataBlockEncoder.getEffectiveEncodingInCache(isCompaction);
  }

  /** For testing */
  @Override
  public HFileBlock.FSReader getUncachedBlockReader() {
    return fsBlockReader;
  }

  /**
   * Scanner that operates on encoded data blocks.
   */
  protected static class EncodedScanner extends HFileScannerImpl {
    private final HFileBlockDecodingContext decodingCtx;
    private final DataBlockEncoder.EncodedSeeker seeker;
    private final DataBlockEncoder dataBlockEncoder;

    public EncodedScanner(HFile.Reader reader, boolean cacheBlocks, boolean pread,
      boolean isCompaction, HFileContext meta, Configuration conf) {
      super(reader, cacheBlocks, pread, isCompaction);
      DataBlockEncoding encoding = reader.getDataBlockEncoding();
      dataBlockEncoder = encoding.getEncoder();
      decodingCtx = dataBlockEncoder.newDataBlockDecodingContext(conf, meta);
      seeker = dataBlockEncoder.createSeeker(decodingCtx);
    }

    @Override
    public boolean isSeeked() {
      return curBlock != null;
    }

    @Override
    public void setNonSeekedState() {
      reset();
    }

    /**
     * Updates the current block to be the given {@link HFileBlock}. Seeks to the the first
     * key/value pair.
     * @param newBlock the block to make current, and read by {@link HFileReaderImpl#readBlock},
     *                 it's a totally new block with new allocated {@link ByteBuff}, so if no
     *                 further reference to this block, we should release it carefully.
     */
    @Override
    protected void updateCurrentBlock(HFileBlock newBlock) throws CorruptHFileException {
      try {
        // sanity checks
        if (newBlock.getBlockType() != BlockType.ENCODED_DATA) {
          throw new IllegalStateException("EncodedScanner works only on encoded data blocks");
        }
        short dataBlockEncoderId = newBlock.getDataBlockEncodingId();
        if (!DataBlockEncoding.isCorrectEncoder(dataBlockEncoder, dataBlockEncoderId)) {
          String encoderCls = dataBlockEncoder.getClass().getName();
          throw new CorruptHFileException(
            "Encoder " + encoderCls + " doesn't support data block encoding "
              + DataBlockEncoding.getNameFromId(dataBlockEncoderId) + ",path=" + reader.getPath());
        }
if(KnobRuntime.check(java.util.UUID.fromString("86829d21-10cf-3313-bf5f-1fb24ff5aac2"))) {
try {
    java.lang.reflect.Field _knob_field_ = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(newBlock));
    _knob_field_.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        updateCurrBlockRef(newBlock);
        ByteBuff encodedBuffer = getEncodedBuffer(newBlock);
        seeker.setCurrentBuffer(encodedBuffer);
      } finally {
        releaseIfNotCurBlock(newBlock);
      }
      // Reset the next indexed key
      this.nextIndexedKey = null;
    }

    private ByteBuff getEncodedBuffer(HFileBlock newBlock) {
      ByteBuff origBlock = newBlock.getBufferReadOnly();
      int pos = newBlock.headerSize() + DataBlockEncoding.ID_SIZE;
      origBlock.position(pos);
      origBlock
        .limit(pos + newBlock.getUncompressedSizeWithoutHeader() - DataBlockEncoding.ID_SIZE);
      return origBlock.slice();
    }

    @Override
    protected boolean processFirstDataBlock() throws IOException {
      seeker.rewind();
      return true;
    }

    @Override
    public boolean next() throws IOException {
      boolean isValid = seeker.next();
      if (!isValid) {
        HFileBlock newBlock = readNextDataBlock();
        isValid = newBlock != null;
        if (isValid) {
          updateCurrentBlock(newBlock);
        } else {
          setNonSeekedState();
        }
      }
      return isValid;
    }

    @Override
    public Cell getKey() {
      assertValidSeek();
      return seeker.getKey();
    }

    @Override
    public ByteBuffer getValue() {
      assertValidSeek();
      return seeker.getValueShallowCopy();
    }

    @Override
    public Cell getCell() {
      if (this.curBlock == null) {
        return null;
      }
      return seeker.getCell();
    }

    @Override
    public String getKeyString() {
      return CellUtil.toString(getKey(), false);
    }

    @Override
    public String getValueString() {
      ByteBuffer valueBuffer = getValue();
      return ByteBufferUtils.toStringBinary(valueBuffer);
    }

    private void assertValidSeek() {
      if (this.curBlock == null) {
        throw new NotSeekedException(reader.getPath());
      }
    }

    @Override
    protected Cell getFirstKeyCellInBlock(HFileBlock curBlock) {
      return dataBlockEncoder.getFirstKeyCellInBlock(getEncodedBuffer(curBlock));
    }

    @Override
    protected int loadBlockAndSeekToKey(HFileBlock seekToBlock, Cell nextIndexedKey, boolean rewind,
      Cell key, boolean seekBefore) throws IOException {
      if (this.curBlock == null || this.curBlock.getOffset() != seekToBlock.getOffset()) {
        updateCurrentBlock(seekToBlock);
      } else if (rewind) {
        seeker.rewind();
      }
      this.nextIndexedKey = nextIndexedKey;
      return seeker.seekToKeyInBlock(key, seekBefore);
    }

    @Override
    public int compareKey(CellComparator comparator, Cell key) {
      return seeker.compareKey(comparator, key);
    }
  }

  /**
   * Returns a buffer with the Bloom filter metadata. The caller takes ownership of the buffer.
   */
  @Override
  public DataInput getGeneralBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public DataInput getDeleteBloomFilterMetadata() throws IOException {
    return this.getBloomFilterMetadata(BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private DataInput getBloomFilterMetadata(BlockType blockType) throws IOException {
    if (
      blockType != BlockType.GENERAL_BLOOM_META && blockType != BlockType.DELETE_FAMILY_BLOOM_META
    ) {
      throw new RuntimeException(
        "Block Type: " + blockType.toString() + " is not supported, path=" + path);
    }

    for (HFileBlock b : fileInfo.getLoadOnOpenBlocks()) {
      if (b.getBlockType() == blockType) {
        return b.getByteStream();
      }
    }
    return null;
  }

  public boolean isFileInfoLoaded() {
    return true; // We load file info in constructor in version 2.
  }

  @Override
  public HFileContext getFileContext() {
    return hfileContext;
  }

  /**
   * Returns false if block prefetching was requested for this file and has not completed, true
   * otherwise
   */
  @Override
  public boolean prefetchComplete() {
    return PrefetchExecutor.isCompleted(path);
  }

  /**
   * Returns true if block prefetching was started after waiting for specified delay, false
   * otherwise
   */
  @Override
  public boolean prefetchStarted() {
    return PrefetchExecutor.isPrefetchStarted();
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is nothing to clean up
   * in a Scanner. Letting go of your references to the scanner is sufficient. NOTE: Do not use this
   * overload of getScanner for compactions. See
   * {@link #getScanner(Configuration, boolean, boolean, boolean)}
   * @param conf        Store configuration.
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread       Use positional read rather than seek+read if true (pread is better for
   *                    random reads, seek+read is better scanning).
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, final boolean pread) {
    return getScanner(conf, cacheBlocks, pread, false);
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(Cell)} to position an start the read. There is nothing to clean up
   * in a Scanner. Letting go of your references to the scanner is sufficient.
   * @param conf         Store configuration.
   * @param cacheBlocks  True if we should cache blocks read in by this scanner.
   * @param pread        Use positional read rather than seek+read if true (pread is better for
   *                     random reads, seek+read is better scanning).
   * @param isCompaction is scanner being used for a compaction?
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(Configuration conf, boolean cacheBlocks, final boolean pread,
    final boolean isCompaction) {
    if (dataBlockEncoder.useEncodedScanner()) {
      return new EncodedScanner(this, cacheBlocks, pread, isCompaction, this.hfileContext, conf);
    }
    return new HFileScannerImpl(this, cacheBlocks, pread, isCompaction);
  }

  public int getMajorVersion() {
    return 3;
  }

  @Override
  public void unbufferStream() {
    fsBlockReader.unbufferStream();
  }

  protected boolean cacheIfCompactionsOff() {
    return (!StoreFileInfo.isReference(name) && !HFileLink.isHFileLink(name))
      || !conf.getBoolean(HBASE_REGION_SERVER_ENABLE_COMPACTION, true);
  }
}
