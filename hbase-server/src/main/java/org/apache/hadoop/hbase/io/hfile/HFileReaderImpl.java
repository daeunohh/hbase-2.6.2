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
if(KnobRuntime.check(java.util.UUID.fromString("dcd1c88a-1b04-356e-afa5-419359e29d4c"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("474653d0-7c92-3195-9491-bd1de7178d7a"))) {
return;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("8fd7b856-ba64-3c9a-a2db-2b1d4d3418ea"))) {
return null;
}
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
      if (((KnobRuntime.check(java.util.UUID.fromString("46f798ee-7019-3849-beea-cd2d9e12eb6a"))) ? (this.curBlock.release()) : (((KnobRuntime.check(java.util.UUID.fromString("61ce596a-5cda-3042-96ce-4a049da7c374"))) ? (((this.curBlock) != (null)) && (this.curBlock.release())) : (((KnobRuntime.check(java.util.UUID.fromString("403420ca-af07-37b3-9350-e316c05741cd"))) ? (((this.curBlock) == (null)) || (this.curBlock.release())) : (((KnobRuntime.check(java.util.UUID.fromString("09a549f1-5532-3fb4-bb8c-5438a6c4eba4"))) ? ((this.curBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("37be7a5f-49bc-3bf2-a65d-aba67441ed5e"))) ? (((this.curBlock) != (null)) && (this.curBlock.isSharedMem())) : (((KnobRuntime.check(java.util.UUID.fromString("33304537-5068-330b-bca0-5f2f063dd73e"))) ? ((this.curBlock != null) && (this.curBlock.isSharedMem())) : (((KnobRuntime.check(java.util.UUID.fromString("24951dca-ae58-3f92-8853-7f28c2663c75"))) ? (((this.curBlock) == (null)) && (this.curBlock.release())) : (((KnobRuntime.check(java.util.UUID.fromString("95a09131-6b0b-3d1e-9a7a-45e70e0401b8"))) ? (this.curBlock.isSharedMem()) : (((KnobRuntime.check(java.util.UUID.fromString("50f193a1-6ab3-35c0-9a25-8f3b962d7959"))) ? (((this.curBlock) == (null)) && (this.curBlock.isSharedMem())) : (((KnobRuntime.check(java.util.UUID.fromString("a4326d82-18f5-32ff-86c2-9c98aaae65ed"))) ? (((this.curBlock) != (null)) || (this.curBlock.isSharedMem())) : (((KnobRuntime.check(java.util.UUID.fromString("4d10c486-01ae-360e-9bfc-052ed8751460"))) ? (((this.curBlock) == (null)) || (this.curBlock.isSharedMem())) : (((KnobRuntime.check(java.util.UUID.fromString("bbddd302-cf4d-38cf-a971-ad89c928b68d"))) ? ((this.curBlock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("394423b2-9d8a-3500-a18b-3dac43bab901"))) ? ((this.curBlock != null) || (this.curBlock.isSharedMem())) : (((KnobRuntime.check(java.util.UUID.fromString("ee31da26-f070-34ab-b8a5-6c03c7d869fb"))) ? ((this.curBlock != null) || (this.curBlock.release())) : (((KnobRuntime.check(java.util.UUID.fromString("90e7069f-6500-33c7-bb49-8281cb1e51ff"))) ? ((this.curBlock != null) && (this.curBlock.release())) : (((KnobRuntime.check(java.util.UUID.fromString("a24d7f7f-6274-3fb6-bb3a-05ff78f30386"))) ? (this.curBlock != null) : (((KnobRuntime.check(java.util.UUID.fromString("a7656207-1d6d-32e2-ba0c-64fdeec502a9"))) ? (((this.curBlock) != (null)) || (this.curBlock.release())) : (this.curBlock != null && this.curBlock.isSharedMem()))))))))))))))))))))))))))))))))))) {
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
        if (KnobRuntime.check(java.util.UUID.fromString("888710e3-ef77-3cb6-94ca-98e562d5e0e0"))) { this.curBlock.isSharedMem(); } else { this.curBlock.release(); }
        this.curBlock = null;
      }
    }

    @Override
    public boolean isSeeked() {
if(KnobRuntime.check(java.util.UUID.fromString("3193e80a-0698-39a8-95ff-57f9bad9270b"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("87711326-ab22-3713-9433-335898fd1590"))) {
return true;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("e684c00d-73c3-3ce5-8322-329565bf0f09"))) ? ((blockBuffer) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c7c673aa-44bf-3fae-82eb-8e9be86b31cb"))) ? ((blockBuffer) == (null)) : (blockBuffer != null))));
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
        if (KnobRuntime.check(java.util.UUID.fromString("381c6578-6c7a-37f6-8139-04347f73b23d"))) { checkTagsLen(); } else if (KnobRuntime.check(java.util.UUID.fromString("e09cd27d-1753-3b45-bafb-49405b24eb62"))) { checkKeyValueLen(); } else { reader.unbufferStream(); }
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
      if (((KnobRuntime.check(java.util.UUID.fromString("9cb74bd1-77a3-3691-aa0d-282ced58e600"))) ? (isSeeked()) : (this.reader.getFileContext().isIncludesTags()))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2aba15cd-0c63-3ebe-9f31-98ef58c05b0f"))) {
p += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("416c3e2f-5110-35ba-892f-246036dce11b"))) {
p = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a669bce8-8b34-3f0f-96f5-79e4f8e36780"))) {
p -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("509b89ca-1373-32f7-bb58-1e67646c9822"))) {
p /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("366cd530-9c61-3652-af65-9a0eb0ed1dd6"))) {
p = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c5ed70d0-3426-3412-a669-4053ae0e40dd"))) {
p *= 2;
}
        this.currTagsLen = blockBuffer.getShortAfterPosition(p);
        checkTagsLen();
        p += (Bytes.SIZEOF_SHORT + currTagsLen);
      }
if(KnobRuntime.check(java.util.UUID.fromString("559820eb-55f0-3250-8fe1-efd1b9e05d42"))) {
p += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b6c143aa-8a02-3dd4-99d7-123c4eacb816"))) {
p -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0d6ca67c-8ac0-3ae7-b002-db69365f9fed"))) {
p /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e900fc29-409a-3836-b6ad-ac71f1c9e810"))) {
p = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("24e01d9d-36e0-34a9-9cc0-ffd6af444443"))) {
p *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("67433e13-85d0-34e7-9b06-733e569657b7"))) {
p = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("d7392cf3-b211-3ac5-88a0-38e96a2aa541"))) { readMvccVersion(0); } else if (KnobRuntime.check(java.util.UUID.fromString("237bb779-d218-3fce-8624-da4902f4694a"))) { _readMvccVersion(0); } else { readMvccVersion(p); }
    }

    private final void checkTagsLen() {
if(KnobRuntime.check(java.util.UUID.fromString("7059b894-2ef3-3b52-99fd-1a477d594597"))) {
return;
}
      if (((KnobRuntime.check(java.util.UUID.fromString("0f38cd8c-e09c-3328-9d03-ff068691497b"))) ? (checkKeyLen(this.currTagsLen)) : (checkLen(this.currTagsLen)))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b76f2245-f8ed-3039-b20c-983c268fee85"))) {
offsetFromPos /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b4944174-d1b2-3e18-a9e9-932d67f327d4"))) {
offsetFromPos += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("136ad5d7-b2b5-3155-a82f-26283dc809ad"))) {
offsetFromPos *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a555697-a5fb-3e80-9695-6ce47599c9e2"))) {
offsetFromPos = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bbfb06d0-1023-34b6-bea8-9a961bef1572"))) {
offsetFromPos -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("89ff80a1-59cf-3566-934f-628fbf3fa041"))) {
offsetFromPos = 0;
}
      byte firstByte = blockBuffer.getByteAfterPosition(offsetFromPos);
if(KnobRuntime.check(java.util.UUID.fromString("19db2972-34d4-32ce-8a42-c430a8149edb"))) {
firstByte = 0;
}
      int len = WritableUtils.decodeVIntSize(firstByte);
      if (len == 1) {
        this.currMemstoreTS = firstByte;
      } else {
        int remaining = len - 1;
        long i = 0;
        offsetFromPos++;
        if (remaining >= Bytes.SIZEOF_INT) {
          // The int read has to be converted to unsigned long so the & op
if(KnobRuntime.check(java.util.UUID.fromString("fa05d598-dab4-3bbc-a134-d5f037154ddb"))) {
offsetFromPos += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f1d36681-525a-3a03-82b4-b3f749be002d"))) {
offsetFromPos /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("97bda9c1-6308-3030-9228-e7a0b073dc08"))) {
offsetFromPos = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cc7fb953-61c6-3d43-bfb2-ac48804c1b87"))) {
offsetFromPos = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("06da7441-aae6-3788-88fd-9ccef3871a8f"))) {
offsetFromPos *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7c8b2799-90c6-35c2-af40-3a4d64912e1e"))) {
offsetFromPos -= 1;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("a9adc071-0688-3830-a018-b2e46eb888fa"))) {
offsetFromPos /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("695847ff-7dac-316f-8965-97b0efce9a76"))) {
offsetFromPos = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a933eee-1012-38cb-a46b-e0e2f4117ec6"))) {
offsetFromPos += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5924c857-8965-3e61-bdbc-2d83c3811ba4"))) {
offsetFromPos -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("89577bb9-4468-3560-acac-2f6e10996b8a"))) {
offsetFromPos *= 2;
}
        this.rowLen = blockBuffer.getShortAfterPosition(offsetFromPos);
if(KnobRuntime.check(java.util.UUID.fromString("7dc03f76-c6ce-30bb-9a6a-a0e5ee2b3336"))) {
klen += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e9270973-ed42-37ad-986e-e3d5e12b54f5"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d170f63e-29f5-3f3a-89e5-fd30de6de178"))) {
klen = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f7fd9cc4-2f21-36b6-bd93-fb9a668ca165"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efc169fd-9206-3bc5-bcd8-3b18869c43e6"))) {
klen -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c55011ce-0494-33c1-a974-8e73507ed1c2"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("140d4f62-2641-3a6f-bc76-101284282302"))) {
klen *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("39a6c102-b2ba-3ee5-ada5-88d61fb8bc95"))) {
klen = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("87fc91aa-697b-3d37-8e05-9bfb22f5077a"))) {
klen /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea0d8d2f-2cd0-3b82-b8c4-d264e5e0331e"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("f4e0d7da-4af5-36bb-803f-77c03ac5789c"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (-1), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("402520b7-cac0-361d-8da6-c125fe0eeeb9"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (0xff), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("ca0f4f86-a6d3-3e7b-8561-519d3699bfdd"))) { blockBuffer.asSubByteBuffer(1, klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("d22bc9e3-ad59-3717-a348-797a17daf866"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (offsetFromPos), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("3aa2daae-54f1-34e1-a562-b1a9bdf4bfab"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (offsetFromPos + 1), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("2a85b425-0cbb-3734-bfa1-9a7b8fe12746"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (1), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("d436133b-6fd8-3e45-b671-9a36512f3305"))) { blockBuffer.asSubByteBuffer(0xff, klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("8edfeea9-6923-3d53-b208-4d58cc6b4cc5"))) { blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, -1, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("489cd445-5737-3fcf-8863-d62306777b9e"))) { blockBuffer.asSubByteBuffer(8, klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("876f664e-0039-38fe-bbc5-d8502726696a"))) { blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, 0xff, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("ac9099f7-280f-397d-9756-ac09956c377e"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (offsetFromPos + 1), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("a028048a-0ece-3fa7-92fb-c49561d06369"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (0), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("d42a2018-8750-3a03-970e-108363ce970c"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (offsetFromPos), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("a319e318-4cc2-3db4-ae3e-6754b05f17ab"))) { blockBuffer.asSubByteBuffer(offsetFromPos + 1, klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("51f72648-ac71-3ffd-88a1-ca9b072bfbe4"))) { blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, 1, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("fd78fb0c-8ae9-31f3-9555-097ccccd65b2"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (8), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("4934b758-a859-3450-a6a8-a3e33eb49430"))) { blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, 8, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("94942276-d1ba-312e-8684-8566ff7a5ade"))) { blockBuffer.asSubByteBuffer(blockBuffer.limit(), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("dd7c8cb5-e3a2-33fb-b6ff-fc0b7d7583c7"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (-1), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("b85b0c61-1fcd-35ae-8741-85171cb24eca"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (0xff), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("cf741ff1-0587-3047-936c-bda6c4ad998a"))) { blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, 0, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("5b3ddd81-81fa-33c0-8f49-33331f0c020a"))) { blockBuffer.asSubByteBuffer(blockBuffer.position(), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("604cc07a-e376-3068-b998-85dc87ac709b"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (8), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("44ccf58c-3e17-3523-a246-8e1f5dd4798d"))) { blockBuffer.asSubByteBuffer(offsetFromPos, klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("ea08952b-a8a7-3573-8c66-ac27156be3fd"))) { blockBuffer.asSubByteBuffer(0, klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("be34269e-3580-30a8-bbb2-9f95170b3bc6"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (0), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("b6b6cbe0-ae7b-336d-a8f2-784f5a509e89"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (1), klen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("c7722f90-3f57-3238-8381-7000da095232"))) { blockBuffer.asSubByteBuffer(-1, klen, pair); } else { blockBuffer.asSubByteBuffer(blockBuffer.position() + offsetFromPos, klen, pair); }
if(KnobRuntime.check(java.util.UUID.fromString("980ddadf-a872-3f9a-9e4e-b788b6ce760b"))) {
klen *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8fa8d2ac-beb1-3b25-bce4-92aecd7f7f14"))) {
klen -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b387aa23-cf20-38f8-91ec-8e4db0f15e1c"))) {
klen /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c3559725-71b6-3f89-b2a5-f143aa5d070c"))) {
klen += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f2eebda-dbb5-37d4-926d-711d4e03d12d"))) {
klen = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("75e60c9c-bb21-3b4f-83c6-7d97dd38bbc5"))) {
klen = 0;
}
        if (KnobRuntime.check(java.util.UUID.fromString("2f96ac13-3b00-3917-be3b-3c1cda861ee9"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), getKVBufSize(), klen, rowLen); } else if (KnobRuntime.check(java.util.UUID.fromString("6e4ca10f-3c51-383e-bf28-905d56ad04e8"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), -1, rowLen); } else if (KnobRuntime.check(java.util.UUID.fromString("77d1d10c-872f-3ee4-9e6a-44a31f2ece1d"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), 1, rowLen); } else if (KnobRuntime.check(java.util.UUID.fromString("c91a367c-485a-3a7e-bbe3-bb3fa529f138"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), 0xff, rowLen); } else if (KnobRuntime.check(java.util.UUID.fromString("dc9d546a-701b-350d-8be1-7b7b5c2a0a1c"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), 0, rowLen); } else if (KnobRuntime.check(java.util.UUID.fromString("1fbf7008-e3b1-307e-b6ed-21456a828eea"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), getCurCellSerializedSize(), klen, rowLen); } else if (KnobRuntime.check(java.util.UUID.fromString("773281e0-f0bd-3afc-a40c-82f68f5a92ca"))) { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), 8, rowLen); } else { bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), klen, rowLen); }
        int comp =
          PrivateCellUtil.compareKeyIgnoresMvcc(reader.getComparator(), key, bufBackedKeyOnlyKv);
        offsetFromPos += klen + vlen;
        if (this.reader.getFileContext().isIncludesTags()) {
          // Read short as unsigned, high byte first
          tlen = ((blockBuffer.getByteAfterPosition(offsetFromPos) & 0xff) << 8)
            ^ (blockBuffer.getByteAfterPosition(offsetFromPos + 1) & 0xff);
if(KnobRuntime.check(java.util.UUID.fromString("971089f7-15e6-3563-a7db-df911e6a8578"))) {
tlen += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1cd0d0ad-597b-308a-9ed4-635f5ff3af53"))) {
tlen = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b4fb0788-33b3-37ec-bd41-0d0479b95e29"))) {
tlen /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e7336cd4-21bd-3399-ab40-984913222678"))) {
tlen = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f9fb95de-3657-356c-af01-4fcc11886d51"))) {
tlen *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7dc819c2-3941-3ace-b85c-04cc9ef273b8"))) {
tlen -= 1;
}
          if (((KnobRuntime.check(java.util.UUID.fromString("bb5cbd4c-2245-3148-a9ba-a5a150bf2195"))) ? (checkLen(0xff)) : (((KnobRuntime.check(java.util.UUID.fromString("057a415b-d279-3188-a5ed-b73286a7c5e3"))) ? (checkKeyLen(1)) : (((KnobRuntime.check(java.util.UUID.fromString("fb0c0797-7134-3bf5-9b2e-81b45a6ed68f"))) ? (checkKeyLen(0xff)) : (((KnobRuntime.check(java.util.UUID.fromString("5e41eb5b-c88d-341d-b5e0-f4efbbdc6dbd"))) ? (checkLen(8)) : (((KnobRuntime.check(java.util.UUID.fromString("45a7887f-4591-37af-b1d7-fa611e889394"))) ? (checkLen(0)) : (((KnobRuntime.check(java.util.UUID.fromString("884bc31b-827b-3b54-989c-892d6ae240b9"))) ? (checkKeyLen(0)) : (((KnobRuntime.check(java.util.UUID.fromString("d675f509-5f5e-34be-9e96-13f4268cced3"))) ? (checkLen(-1)) : (((KnobRuntime.check(java.util.UUID.fromString("9029b81e-7129-3684-be81-7b1b3a7608fd"))) ? (checkKeyLen(-1)) : (((KnobRuntime.check(java.util.UUID.fromString("2d5673de-b80f-3065-b8d8-3fae41630239"))) ? (checkKeyLen(8)) : (((KnobRuntime.check(java.util.UUID.fromString("44992612-2d38-34cd-b7eb-be2aff301076"))) ? (checkLen(1)) : (checkLen(tlen)))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bfec4bfb-e511-3c04-a18a-9d1d486926a2"))) {
lastKeyValueSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e80b0553-690b-31c6-8904-1914f57a754f"))) {
lastKeyValueSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d6d4caef-e7ee-3efa-a193-0275ed62bcbc"))) {
lastKeyValueSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("09d7c0c1-e3ac-3ddd-bc95-2768a90c46e4"))) {
lastKeyValueSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a766885b-a737-324b-8c93-bb7b1c856ee6"))) {
lastKeyValueSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ca6a087c-d841-3071-b091-e88cfc8748e5"))) {
lastKeyValueSize -= 1;
}
            if (KnobRuntime.check(java.util.UUID.fromString("01ce690c-2390-31e7-9d1e-e294492f739f"))) { blockBuffer.skip(-1); } else if (KnobRuntime.check(java.util.UUID.fromString("56c36778-656e-3b3d-b8ed-0676ea74eeb6"))) { blockBuffer.skip(0); } else if (KnobRuntime.check(java.util.UUID.fromString("2903f299-cc13-3611-b4e4-daa64111fbb0"))) { blockBuffer.skip(1); } else if (KnobRuntime.check(java.util.UUID.fromString("3c4e1487-53d1-3bec-b3e0-ef6189094d6c"))) { blockBuffer.moveBack(0xff); } else if (KnobRuntime.check(java.util.UUID.fromString("33089eae-a3bc-3ee0-86d1-5aaff1a906d6"))) { blockBuffer.moveBack(0); } else if (KnobRuntime.check(java.util.UUID.fromString("1947d5ab-cf21-3734-b310-bb0cc848ed96"))) { blockBuffer.moveBack(-1); } else if (KnobRuntime.check(java.util.UUID.fromString("a359c81d-fabc-3fe0-be7a-56f6515251d1"))) { blockBuffer.skip(8); } else if (KnobRuntime.check(java.util.UUID.fromString("5ef6058c-1233-3354-be1e-81b16118d302"))) { blockBuffer.moveBack(8); } else if (KnobRuntime.check(java.util.UUID.fromString("bb07a395-8eb5-345e-bac4-9058fb3220ca"))) { blockBuffer.skip(0xff); } else if (KnobRuntime.check(java.util.UUID.fromString("6d2392e8-38a0-3aa6-b834-479e5eef8bd9"))) { blockBuffer.moveBack(1); } else { blockBuffer.moveBack(lastKeyValueSize); }
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
      if (KnobRuntime.check(java.util.UUID.fromString("af531126-dee2-3d47-a41b-1853f87b5ca3"))) { checkTagsLen(); } else if (KnobRuntime.check(java.util.UUID.fromString("40ab753f-dffd-391a-9afc-3ebdc6812e50"))) { checkKeyValueLen(); } else { readKeyValueLen(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("862db758-f0fb-319c-a826-b0835909b2cd"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90a0b85d-8bcf-318f-bed9-4c10b597e687"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ed2279b-d673-3735-b8b7-30c4c42bb3b1"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8951c9f-c701-341c-ad2a-f1ece6b2aacd"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6660316-0b25-3f5c-b4a7-9860b0ef0009"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42a3177d-ac5a-3415-ad87-634646987e83"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53b9f190-653f-3f59-84b4-c4890657b09e"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8821ff21-64f9-32e2-b0c7-5baada98977d"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d51180b3-aaef-3fc7-8390-445ef8d8c9fe"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d80ffaf-ab76-3d25-af81-9def2f881d60"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc019551-58dc-3392-bd12-1cb05390b193"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("eb50e7b6-2d76-35f3-a12e-de13ca4a781b"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7fbae8d6-4b40-3ba9-bc2a-d164889021a9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("a565b162-eccb-3cf9-8798-6a8ca10994c6"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3290251-924e-36af-afd6-01066af798a9"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba142967-d18b-3167-bb10-9451b2061257"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b912d5e4-38d8-3139-bdc1-c800ac6ad9e8"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d663f7c-9ccf-3e0f-ac07-1ff36ff80a08"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("795db195-d982-3627-9b1e-9f690c281ed3"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7ad6f75-807a-3718-a10e-f23c007cb284"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41fcf6d4-0992-3ac3-b629-47d8cb52ea37"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80f5bd47-4113-3f67-aa85-4d5ee454c499"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e066f4d9-d362-346c-aec9-989340dc767f"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("453c924f-79de-3a7b-a8a3-e628d924a86f"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("644d08cb-bbb8-3f5d-8edc-6977b242277a"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3be2c9b7-2a21-3944-bffb-84f2f4c6f2d6"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      HFileBlock seekToBlock = reader.getDataBlockIndexReader().seekToDataBlock(key, curBlock,
        cacheBlocks, pread, isCompaction, reader.getEffectiveEncodingInCache(isCompaction), reader);
      if (((KnobRuntime.check(java.util.UUID.fromString("0e876c4c-0014-36c4-a91c-89763cf04e4e"))) ? ((seekToBlock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("15fb38f1-daf1-3dcd-ad3b-c5a53dd10f77"))) ? ((seekToBlock) != (null)) : (seekToBlock == null))))) {
        return false;
      }
if(KnobRuntime.check(java.util.UUID.fromString("16384ee5-100b-3c00-b966-1ec82424c8e9"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb6aebf8-bd53-3c28-9098-44f9a3b4e9aa"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6546797-c041-3f23-9ab5-8d17225f9ca4"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e247336e-076e-3b83-ba53-f27530327ff3"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c341e40b-7fed-35a5-a61c-a08ecdf88644"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c41bba4e-0da9-3cf4-9bde-ef4c747c0923"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b362a6c-5591-312a-ac7a-4948397418c1"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("331f0f76-8e6c-38b4-961c-56f3954e10ea"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78868c74-6bc8-3763-bccc-3d56e4517b8a"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04b0d2e6-e319-3243-9d72-78b301560606"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc9e3b25-65ee-3721-aad2-34365cfc87d0"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("34fa7427-f4b6-3aac-880b-68b07097a448"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d33bedca-093a-3bdb-9844-320e0b8d8940"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("973888fb-5946-32f0-8f49-bf408779be97"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2d74767-b92f-3e2b-b2e0-dd2a33995831"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("530542aa-0889-3435-a6a4-6c4cb8251973"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("751a5400-86f0-33c0-a749-cec6e90e0974"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55271ebf-6dce-30e1-9498-476c84661d3f"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0291265-f6b4-38f3-b7d5-feee25193d57"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("272df08e-b7e0-36a9-ada4-af81bf577a34"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1855a8b3-4111-3fc2-8c45-9ebef62a34fe"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("886277f9-0675-362b-9d0c-cd991a6063c4"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93a27120-61a4-38ce-b5d7-8c964e5cf64b"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("9117729a-9527-3ac0-a129-4095518936ff"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98e79dfc-b1ae-3f55-b9eb-9ce5fa93e7e0"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f057d606-e67d-38fa-8c2e-50a408eadbe6"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9240e07-4e4d-3252-92ef-60747e2b308f"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7289181d-4fe9-3245-8c6c-b02a7e3c3ab8"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce40d080-eb20-3a14-9733-a1a1cb7fb699"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e2305360-2908-343b-b848-72809e884229"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0981c2b8-47e7-3064-8958-59a7d93e6772"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6370b4d-b32f-3f59-a415-48ede66b7c19"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0ca75cfb-bac7-300b-8773-ba795dca3534"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("620cb86c-4360-3fca-9e11-6bd481b3b668"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2a66452-0f54-3b18-8073-edb7981dc081"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82093e63-18f2-31dc-91aa-ab626fb69733"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e75c5e4c-a8f9-3775-9aec-d456011b250c"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afbedab9-0dc9-32cb-8aee-1479fcc1edc7"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d18b109e-b96a-332d-a17c-1edde1b770e0"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("591b5029-548e-3ef3-935d-0ddcab9b62d2"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7998cf9c-930e-365b-8fe0-439d7b45e94b"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cefe572a-0be5-3367-8225-307228a782e3"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("738e8189-bc2e-3569-94ed-09f97f9ba1ea"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25c6da31-01c6-3549-a5a4-feff4d68c37a"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63543067-3e57-3792-91d8-ca0501f785d2"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe9f71c9-c65c-35b8-9f8f-ba7feaf2dad8"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
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
        if (KnobRuntime.check(java.util.UUID.fromString("d68c85ca-8e25-32c4-8790-79233cf9b555"))) { isSeeked(); } else { block.release(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("b7b1cea0-99f8-334d-9e8c-b839e193b89c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("016d1858-0d61-3c81-8515-1e69617f79a3"))) {
throw new java.io.IOException("Injected exception");
}
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
      if (((KnobRuntime.check(java.util.UUID.fromString("4268125d-1bb6-3e65-ba0c-9925f7ed092e"))) ? (isSeeked()) : (((KnobRuntime.check(java.util.UUID.fromString("09576ba4-9078-32a3-8d8d-b959facd4e88"))) ? (blockBuffer.hasRemaining()) : (blockBuffer.hasArray()))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1a023661-f389-300d-8776-4dce71ba2a7e"))) {
cellBufSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("518062bd-c0ab-31f9-b30d-ea466e955854"))) {
cellBufSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8cb927cd-71ce-35c3-8218-c6668930427c"))) {
cellBufSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b5252b4b-b021-3bfa-85af-fe84371b7d03"))) {
cellBufSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e54a764c-daa7-3ea3-bf76-20a68f0674e9"))) {
cellBufSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8708950-9c37-3d0a-9876-548f8559feaa"))) {
cellBufSize -= 1;
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
      if (((KnobRuntime.check(java.util.UUID.fromString("e516d96f-7f6b-3911-b968-93cd69a32aab"))) ? (isSeeked()) : (keyBuf.hasArray()))) {
        return new KeyValue.KeyOnlyKeyValue(keyBuf.array(),
          keyBuf.arrayOffset() + keyPair.getSecond(), currKeyLen);
      } else {
        // Better to do a copy here instead of holding on to this BB so that
        // we could release the blocks referring to this key. This key is specifically used
        // in HalfStoreFileReader to get the firstkey and lastkey by creating a new scanner
        // every time. So holding onto the BB (incase of DBB) is not advised here.
        byte[] key = new byte[currKeyLen];
        if (KnobRuntime.check(java.util.UUID.fromString("184c9bb9-b1b7-31e5-95f5-10c2360b3944"))) { ByteBufferUtils.copyFromBufferToArray(key, keyBuf, getKVBufSize(), 0, currKeyLen); } else { ByteBufferUtils.copyFromBufferToArray(key, keyBuf, keyPair.getSecond(), 0, currKeyLen); }
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
        if (KnobRuntime.check(java.util.UUID.fromString("3d21aad6-f95a-3d49-9500-131d8467009a"))) { blockBuffer.moveBack(getKVBufSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("ec5aa0fa-c3a8-3aff-848e-47d864ab09cf"))) { blockBuffer.skip(getKVBufSize()); } else { blockBuffer.skip(getCurCellSerializedSize()); }
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
      if (((KnobRuntime.check(java.util.UUID.fromString("eab808bb-8d79-339b-9119-88bcfc84224c"))) ? ((this.curBlock.getOffset()) <= (lastDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("203feb22-046a-3c4f-aa03-e063f906797f"))) ? ((this.curBlock.getOffset()) != (lastDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("82f56463-6ff2-399b-ac33-a61b564dd13c"))) ? ((this.curBlock.getOffset()) >= (lastDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("0f8dba60-6b99-3c2b-be0d-345c95144486"))) ? ((this.curBlock.getOffset()) < (lastDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("d204a292-6e71-3155-8ef5-d7d15eaa955d"))) ? ((this.curBlock.getOffset()) == (lastDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("99b04858-40b6-3438-89cc-49f98872a8ba"))) ? ((this.curBlock.getOffset()) > (lastDataBlockOffset)) : (this.curBlock.getOffset() >= lastDataBlockOffset))))))))))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("151ccf80-8743-34d6-b149-ce64fe69cbbc"))) { checkTagsLen(); } else if (KnobRuntime.check(java.util.UUID.fromString("9b4e8fd4-676b-367c-b6b9-4577e3e624b7"))) { readKeyValueLen(); } else if (KnobRuntime.check(java.util.UUID.fromString("4ee315db-c124-31e8-9feb-1aa156730403"))) { checkKeyValueLen(); } else { setNonSeekedState(); }
        return false;
      }
      return isNextBlock();
    }

    private boolean isNextBlock() throws IOException {
      // Methods are small so they get inlined because they are 'hot'.
if(KnobRuntime.check(java.util.UUID.fromString("af05f679-5d4a-3b7a-af4b-7d838f91bf0f"))) {
throw new java.io.IOException("Injected exception");
}
      HFileBlock nextBlock = readNextDataBlock();
      if (((KnobRuntime.check(java.util.UUID.fromString("063239d6-a5f7-3596-8540-1730f396e3c8"))) ? ((nextBlock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4968b0b2-4387-3e96-804d-7100409bf9f8"))) ? ((readNextDataBlock()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("60ed9599-7202-3aab-b250-d064b4c42c00"))) ? ((readNextDataBlock()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3cd6f4e2-c4b5-3bd2-a963-6175a439f89d"))) ? ((nextBlock) != (null)) : (nextBlock == null))))))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("f8d91ccd-b467-351d-b057-e82ae990469a"))) { checkKeyValueLen(); } else if (KnobRuntime.check(java.util.UUID.fromString("090013da-4813-3ac7-9732-cff28bd4bca1"))) { readKeyValueLen(); } else if (KnobRuntime.check(java.util.UUID.fromString("b775a9ac-21c4-3bcc-a97f-67f41b727633"))) { checkTagsLen(); } else { setNonSeekedState(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("f13b8f15-a57f-3724-b28f-e993f8fec400"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("497b8dad-735c-3d29-bee1-6da180a29938"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("00713367-c3ad-3edf-a585-debee6794191"))) {
return false;
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
if(KnobRuntime.check(java.util.UUID.fromString("3575e13b-0f27-38f0-bc26-b29e14941740"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8cc28eb9-a5dd-339c-a023-990f95ddef0d"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("291294eb-f296-31ec-9ab7-9379eb43cb93"))) {
return true;
}
      if (((KnobRuntime.check(java.util.UUID.fromString("b564d3fc-2b46-3afb-b5eb-4a4712d8ae02"))) ? ((reader) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f8d5e13f-02dd-319c-b9f1-b02ac81c75fd"))) ? ((reader) != (null)) : (reader == null))))) {
        return false;
      }

      if (reader.getTrailer().getEntryCount() == 0) {
        // No data blocks.
        return false;
      }

      long firstDataBlockOffset = reader.getTrailer().getFirstDataBlockOffset();
      if (((KnobRuntime.check(java.util.UUID.fromString("b4ed85d4-e3f0-3e56-a6c6-8642d89e63eb"))) ? ((curBlock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4772c751-1bb4-36ef-b058-b04f52041259"))) ? (((curBlock) == (null)) && ((curBlock.getOffset()) == (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3584491c-6bf5-3e44-8631-83d005667f11"))) ? (((curBlock) != (null)) || ((curBlock.getOffset()) == (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("678376f8-c44b-3b20-b600-6fd80cab7943"))) ? (((curBlock) == (null)) || (curBlock.getOffset() == firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("97e06b2b-9fab-3ef9-b67f-adf885a53b3b"))) ? ((curBlock.getOffset()) != (firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("d9c41dc2-ab60-3337-adf7-40eac5386ed3"))) ? (((curBlock) != (null)) && ((curBlock.getOffset()) == (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("d5991e4b-77d9-3cf4-8eab-7697bb28078b"))) ? ((curBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("37cfc0c0-d180-31d2-b79f-df259ba6270c"))) ? (((curBlock) != (null)) || ((curBlock.getOffset()) != (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("58d08034-0ffb-3888-9e30-a4dc5fb4a6a7"))) ? (((curBlock) == (null)) && ((curBlock.getOffset()) != (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cdc8af3a-ef61-3d17-a702-ca3aa615efef"))) ? ((curBlock != null) || (curBlock.getOffset() == firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("bdf63fa4-04b9-30b6-87e4-29767799b014"))) ? ((curBlock != null) || ((curBlock.getOffset()) != (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8b7dc004-5cfa-3e08-88a0-0e7c6432370e"))) ? (((curBlock) == (null)) || ((curBlock.getOffset()) == (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b0e48c54-dd9b-33ff-9f4e-4b77bfe4a902"))) ? (curBlock.getOffset() == firstDataBlockOffset) : (((KnobRuntime.check(java.util.UUID.fromString("91e5cb3f-1899-35e1-8618-611cd623b4c2"))) ? (((curBlock) != (null)) || (curBlock.getOffset() == firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("190cc6bb-b158-350e-9d03-4c6ea690d62a"))) ? ((curBlock != null) && ((curBlock.getOffset()) != (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("207c55ad-2a0a-362b-8af0-52ef63a977cf"))) ? (((curBlock) != (null)) && (curBlock.getOffset() == firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("f8c55ea3-4ba4-38c8-abbc-3634e811a463"))) ? (((curBlock) != (null)) && ((curBlock.getOffset()) != (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("877a57bc-2d7f-311e-9fd2-f50ce0182e84"))) ? ((curBlock != null) && (curBlock.getOffset() == firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("76456fc0-7afb-3b21-ac41-b4278242d8e8"))) ? ((curBlock != null) || ((curBlock.getOffset()) == (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("491312d9-b76f-39c4-9534-eda550f90048"))) ? (curBlock != null) : (((KnobRuntime.check(java.util.UUID.fromString("e415739f-25f6-3e49-8638-2f76e74d0f79"))) ? ((curBlock.getOffset()) == (firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("7f893257-2a4c-3332-87f0-37d7cfb83345"))) ? (((curBlock) == (null)) && (curBlock.getOffset() == firstDataBlockOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("84c916ab-3bcd-31f8-bb04-df5865e15520"))) ? (((curBlock) == (null)) || ((curBlock.getOffset()) != (firstDataBlockOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("0cc55822-f962-3e46-9347-bc27c795eea4"))) ? ((curBlock != null) && ((curBlock.getOffset()) == (firstDataBlockOffset))) : (curBlock != null && curBlock.getOffset() == firstDataBlockOffset))))))))))))))))))))))))))))))))))))))))))))))))) {
        return processFirstDataBlock();
      }

if(KnobRuntime.check(java.util.UUID.fromString("eda6fb4c-6843-3b52-8f1f-43fa64cc6811"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("89116bca-bd88-3e73-bab5-a58f1f6da2b4"))) {
firstDataBlockOffset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0af0ddee-fc8b-37fd-b974-53bcc86a167b"))) {
firstDataBlockOffset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("062b7da2-712d-321c-b876-c0f936c820fb"))) {
firstDataBlockOffset = -1;
}
      readAndUpdateNewBlock(firstDataBlockOffset);
      return true;
    }

    protected boolean processFirstDataBlock() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d84750ff-477a-377f-a7e5-31cc52af5fad"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("5798eb0f-7ecc-393f-9064-d7d1ba296cce"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("71b4ad5c-807e-300b-a133-83eed3b15b43"))) {
return false;
}
      blockBuffer.rewind();
      if (KnobRuntime.check(java.util.UUID.fromString("6b080993-55d2-3b7f-92d7-84868bc13102"))) { checkKeyValueLen(); } else if (KnobRuntime.check(java.util.UUID.fromString("36ac9750-4da1-3a86-b03f-bab8e8c9c92a"))) { checkTagsLen(); } else { readKeyValueLen(); }
      return true;
    }

    protected void readAndUpdateNewBlock(long firstDataBlockOffset) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("7ec43e2a-5b5a-3b15-98e4-5f39e996bf9a"))) {
firstDataBlockOffset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("86e4ffb4-5e45-31a3-9d8e-fff16de4df7d"))) {
firstDataBlockOffset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cac0211a-4ee5-38a6-842c-6a07edc65bdd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e88923bf-f96b-3469-bd86-ddecf21735b9"))) {
firstDataBlockOffset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d07ada0b-e650-3d6b-b1b3-ae888e872062"))) {
return;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("d5008e1e-02e3-32dd-81c5-11c4ffbed98a"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ee864fcf-ecaa-37a9-9771-9f7b1329fcf4"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afd31f9a-8f25-36d3-b5de-422cf6f56b16"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58eeff06-9130-37c1-90e7-6c2a36f7b8c4"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56161c5b-01b1-3946-af89-bc45c87cef1a"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45409a7c-5541-3ca4-a71b-54da3cf5bbdf"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72498e35-2c1b-3fac-a622-69c6d3ca7d3b"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c241c0e5-b1a1-3a4a-adac-f4dfa8509137"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("957a6818-724c-3f31-8256-4112d68258cc"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f1549b3c-0071-3bd9-9383-f5a8e9cc777a"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43e25238-5f29-32ef-846c-81f29f4c2235"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b011bee0-ed58-3f59-976d-205f108cd18a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("74f2cf8b-f49a-340b-b7c3-05d35f33ce68"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("85da54de-4862-3a56-94c9-fd8c7e482553"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e06e3fcd-eea8-3f2b-a239-47b255fafec1"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ae33d94-b2b7-3853-93c8-0e354afbbc67"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("097e70f5-2d78-37c7-be8f-4f0f34a643ae"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6044393b-a585-3355-94bb-51068af32bc2"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b029564-c325-340b-a7c3-23d624741ed6"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d3c718f-fe59-37f6-9130-b8c63a406ecb"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b97257ce-1770-3443-8674-cdf3ee54e13c"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("74c15ea0-2951-3e9c-8b01-5aa7117bf3f8"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1783c56-b01e-3f45-b872-27462e54a3ce"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7f9a88d8-76dc-3439-9e79-4282fc8031bd"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8a76923-8780-38a8-b6c5-0f546e16cc4e"))) {
try {
    java.lang.reflect.Field field = seekToBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(seekToBlock));
    field.set(seekToBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (((KnobRuntime.check(java.util.UUID.fromString("c424683a-cf94-3dba-9d06-a44a0c0dd4c4"))) ? ((this.curBlock == null) || ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("4e94450f-0896-3795-a91a-464d5591104f"))) ? ((this.curBlock == null) && ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("034a7dd5-5933-3b1b-ba6e-c7ddeab9f06c"))) ? ((this.curBlock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d953032d-5e46-305d-8333-e444c9a78cbd"))) ? ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("6f819580-0672-3946-a0ac-4eb8c6dc3526"))) ? ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("afd46cf3-d8d8-3181-a025-7d439d48a45a"))) ? (((this.curBlock) != (null)) || ((this.curBlock.getOffset()) == (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("c6e24de2-705b-39c5-8984-fcf3d38a4aa6"))) ? (((this.curBlock) != (null)) || ((this.curBlock.getOffset()) != (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("bdbcabfe-75c0-3ed4-b106-642f4c016ce5"))) ? ((this.curBlock == null) || ((this.curBlock.getOffset()) == (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("1e26af5c-ad70-3e7c-b218-3e846593a827"))) ? (((this.curBlock) == (null)) && ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("8ba74458-14f8-3179-9d02-a40dff5e5620"))) ? (((this.curBlock) != (null)) && ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("185dc0f5-524e-386a-a552-28c40afafff3"))) ? (this.curBlock.getOffset() != seekToBlock.getOffset()) : (((KnobRuntime.check(java.util.UUID.fromString("48bc2465-1ca3-3169-8764-4a8db1161bb7"))) ? (((this.curBlock) == (null)) && (this.curBlock.getOffset() != seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("376593e0-69fb-307a-809c-a0344c7a2ccc"))) ? (((this.curBlock) != (null)) && ((this.curBlock.getOffset()) != (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("8e76394a-fcf4-3222-b76f-1a3d428bbab7"))) ? (((this.curBlock) == (null)) || ((this.curBlock.getOffset()) == (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("782954f3-a65f-31f2-8d13-f22876372660"))) ? (((this.curBlock) != (null)) && ((this.curBlock.getOffset()) == (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("5895e482-eb9f-320e-be90-262600eb7305"))) ? (this.curBlock == null) : (((KnobRuntime.check(java.util.UUID.fromString("de7d395e-d6db-38c2-8367-590576716bf7"))) ? ((this.curBlock.getOffset()) != (seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("6ad27622-a38d-3418-bbb0-af5e95f915dd"))) ? (((this.curBlock) != (null)) || (this.curBlock.getOffset() != seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("d7d24d47-d69b-3313-9e34-9d7841f237de"))) ? (((this.curBlock) != (null)) || ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("9f8cb602-41d2-3d42-856c-57d1218b6aff"))) ? (((this.curBlock) == (null)) && ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("dd5f9606-49a5-3f29-a478-8ed9a408e5fa"))) ? (((this.curBlock) == (null)) || (this.curBlock.getOffset() != seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("601a9a8c-e70a-3173-a0af-4b4baaa2922a"))) ? (((this.curBlock) != (null)) && ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("d405106f-342b-35bc-bc78-82f726ae8edb"))) ? ((this.curBlock == null) && ((this.curBlock.getOffset()) == (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("cff751cd-6067-3c16-9182-e33f19a0a1e5"))) ? ((this.curBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("187d4399-6d9e-3723-a143-99beadedaeb4"))) ? ((this.curBlock.getOffset()) == (seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("5e7eb4da-0d6e-349b-82b8-2239492e90f5"))) ? ((this.curBlock == null) && (this.curBlock.getOffset() != seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("1b048e59-9731-3768-8b2a-d5be8d6a78e5"))) ? (((this.curBlock) == (null)) || ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("3f31d7ac-d479-3597-909b-242783e942aa"))) ? ((this.curBlock == null) || (this.curBlock.getOffset() != seekToBlock.getOffset())) : (((KnobRuntime.check(java.util.UUID.fromString("a2f52bdb-1649-3530-b85e-1b80cb70591e"))) ? (((this.curBlock) != (null)) || ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("f732f769-218b-34cb-8b42-dcbb2306ff13"))) ? ((this.curBlock == null) || ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("8a2defe4-6c67-355c-a2c8-211b6427d409"))) ? (((this.curBlock) == (null)) && ((this.curBlock.getOffset()) == (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("402802ae-ab8c-3042-9d6b-da3df1158ac8"))) ? (((this.curBlock) == (null)) || ((this.curBlock.getOffset()) != (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("6f926672-6ee3-3021-b8fc-e01fa28f89cc"))) ? ((this.curBlock == null) && ((this.curBlock.getOffset()) != (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("40aa74e6-75c4-3426-98e1-030aa67d2beb"))) ? (((this.curBlock) == (null)) || ((this.curBlock.getOffset()) == (seekToBlock.getPrevBlockOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("ec2c5706-14b6-3919-8f5f-9c1898bd33f8"))) ? ((this.curBlock == null) && ((this.curBlock.getOffset()) != (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("6a407568-3284-3a11-bdfd-e49e896c0e96"))) ? (((this.curBlock) == (null)) && ((this.curBlock.getOffset()) != (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("a9e83cf3-4f1c-3a17-8e36-7a8ba68c6f11"))) ? ((this.curBlock == null) || ((this.curBlock.getOffset()) != (seekToBlock.getOffset()))) : (((KnobRuntime.check(java.util.UUID.fromString("c7b3f772-b2a4-3d8b-abb5-de5875d5a9b4"))) ? (((this.curBlock) != (null)) && (this.curBlock.getOffset() != seekToBlock.getOffset())) : (this.curBlock == null || this.curBlock.getOffset() != seekToBlock.getOffset()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ed421e11-d493-3499-973e-4042d1ad5e92"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("e89ec91e-d16f-3b9f-b88a-ddb9ab51e3d5"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("80922956-7cdf-3615-a690-6e7a9c516b44"))) ? (((v) >= (0)) && ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("34bfe1cf-6e55-3fa3-9c93-fc8d33b2e145"))) ? (((v) < (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("fc2793c2-8cce-3c13-a1c8-beaf09f8713d"))) ? (((v) < (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("561da523-1bda-3560-8b79-396f0adea7f8"))) ? (((v) != (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("dd980288-a71e-34bc-aada-5accf8a8bf43"))) ? (((v) < (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("76b70d16-62f5-3683-9a48-3496283a6a10"))) ? ((v <= 0) && (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("95353728-9d2e-33d6-92d1-9ad4153d1287"))) ? ((v <= 0) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("cc2c23db-fcda-3fbc-abc1-dc6136a2b667"))) ? (((v) <= (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("a9f83f07-c027-363c-8422-ad0c4aec306e"))) ? ((v) >= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("398b3955-f473-3a2b-b8e8-0ff881b556e4"))) ? (((v) >= (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("fd908d57-0b64-3cf1-961b-ef8f7e96718b"))) ? (((v) > (0)) || ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a0921f39-6bf8-3a4e-b439-61c2d75bd46f"))) ? (((v) <= (0)) || ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("bf7af67f-69ca-352a-90b9-18c2058eaea3"))) ? (((v) == (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e4425829-fbfe-38d7-b635-d2b732cfeca9"))) ? (((v) != (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("70f7ac78-1eaa-35bc-9107-5d6f11c219c1"))) ? (((v) == (0)) && ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f720d978-055b-338f-8fc5-836f756bd93c"))) ? (((v) != (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e3e29593-9c18-3cf9-a16a-a25219461bc9"))) ? (((v) >= (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("794f6d01-d420-34bd-bc39-7fc9f758d941"))) ? ((0) <= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("f45fe981-ce20-30ba-9620-79c22ddd364c"))) ? (((v) >= (0)) && ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("3d7f08cd-dc38-3f11-9690-b8669f87882d"))) ? (((v) != (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("50f09f29-0e5a-367f-ac5e-7f832c0fbb14"))) ? (((v) == (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f9e500b2-64f2-3953-ab9b-d364fc9af2ae"))) ? (((v) == (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("19802f6d-b7b5-34dd-9b85-80dea7ac07ce"))) ? (((v) != (0)) || ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("50a744e0-05e8-39de-bb96-4acc27b482e8"))) ? (((v) < (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("9adcf52c-a930-3cc4-9dfc-a51b77a4d5d7"))) ? (((v) < (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("de92b6c3-4fee-3437-be68-4e352bba43ac"))) ? (((v) < (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("bb16d69b-a614-30c1-a393-8932a27f381f"))) ? (((v) >= (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("cc29949b-0192-3aa4-a8b3-c62d1c4edea2"))) ? (((v) <= (0)) || ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4981c0bd-02fa-3792-b7fa-4b0a2f2b690b"))) ? (((v) > (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("68a7374d-516f-3e47-b963-6e7b3059c7ef"))) ? (((v) >= (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7f3bb1f4-4184-3386-aa8d-82ee4372c90d"))) ? (((v) <= (0)) || ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d6181850-95e2-3aaf-8580-3f12868eceef"))) ? ((v <= 0) && ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("263a834a-70d9-3e62-9f55-09a9f09a85f1"))) ? (((v) < (0)) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("41ef968e-7a0c-361a-847d-1038590f31d1"))) ? (((v) == (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("37aaaa45-65ae-3c63-98bc-b5406e10c765"))) ? (((v) != (0)) && ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c00c8acc-522e-3700-ae86-b8319c868200"))) ? (((v) == (0)) && ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("5380ef59-0050-3b41-bb1b-4bf2348d3c76"))) ? (((v) < (0)) && (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("67677623-b554-373c-9f13-5498901dee1e"))) ? ((v <= 0) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("31ad87c3-b9ca-3bfc-bfda-774dc68df33d"))) ? ((v <= 0) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9b78b214-882c-36ab-a4d3-4c15d6898b6a"))) ? (((v) > (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("2f389c32-58f3-35de-a0dd-5e632ce27a96"))) ? (((v) != (0)) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("fdff2bd3-6f93-3369-9321-781cb0fd7b76"))) ? (((v) != (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("182b0fd7-638e-384d-906e-b2297c4c4d69"))) ? (((v) != (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("95f67fb2-98ea-3578-84c1-3e81f93ce71e"))) ? ((v) <= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("d5468bdc-f7ad-3334-94b6-67dcb0dc8ff8"))) ? (((v) > (0)) || ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("de613591-4dbf-33d2-a39c-e8cbfa3b59fe"))) ? (((v) >= (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("b9781c74-4093-3671-bae8-17e26a26dbd2"))) ? (((v) < (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("8f0a5805-199e-3792-ad1f-78d4b8261a8b"))) ? (((v) <= (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("fbef5cef-a5e1-30f0-87b4-51d359778904"))) ? (((v) > (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("10551fdb-52bc-3f14-b853-e5dfd818626b"))) ? (((v) > (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2d6ecf1f-3fb8-3d23-a66a-78aa3f815021"))) ? (((v) == (0)) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("31dcaabf-7b04-3ec3-a8cc-14d5c818361e"))) ? (((v) != (0)) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("68607a71-86de-3992-9e83-f0935bcf4164"))) ? (((v) != (0)) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("dc2e20f3-d069-3c99-b65c-6a2d0a5eedfd"))) ? (((v) == (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c9e1cfdd-79f5-3b03-aaf7-a01722c987dd"))) ? ((v <= 0) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("de946251-fd8d-32d7-8f9b-cc2d66d22290"))) ? (((v) == (0)) || ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f2b64fbc-45eb-3153-a26c-a78380c2b88f"))) ? ((v <= 0) && ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("043c8721-99f5-33d7-9cc3-82672589c8bc"))) ? ((0) > (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("628a101a-7952-3063-9719-b75951b98cfc"))) ? (((v) <= (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d0a897c7-0cfe-39b6-b932-3f10fd9cee7d"))) ? (((v) > (0)) && ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4ebe5268-f1af-3e7b-a104-e14ff7990ea9"))) ? (((v) >= (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("dc265136-dff1-37fa-8a6f-46240f087edd"))) ? (((v) <= (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("1ef00662-762d-3249-ad9b-c86b3499f5ad"))) ? (((v) == (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("a3049ea6-cdd4-393e-9ed5-ed2f68bb0eb8"))) ? (((v) != (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4c88c62f-a9f3-36df-ae0f-01394d2ce304"))) ? (((v) <= (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("805eee77-99e5-37eb-834f-bc965a7814ae"))) ? (((v) < (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("b8f39082-452f-36b0-8450-750181a88988"))) ? (((v) < (0)) && ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7f2cd69a-be96-3d41-8182-d9bc238daa00"))) ? (((v) == (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("aea12cb5-9245-31a7-8774-bccf0cdcff05"))) ? (((v) < (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ec6cd3b2-4440-3ed9-be93-21e304b861a2"))) ? (((v) <= (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("70eb54f8-5724-3c11-885b-45408c2b71f7"))) ? (((v) != (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("1a8487bf-36a6-313f-bad3-bc30d53b4716"))) ? (((v) <= (0)) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("935d8ae6-34db-3ab1-b0d4-384ab343fe78"))) ? ((v <= 0) && ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c8ac961b-d7da-3ab8-80f1-bc1fbfd6d19f"))) ? (((v) < (0)) || ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a1f3b71f-5fda-3b01-921f-de80719bd630"))) ? ((v) >= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("09874df6-25d4-3142-8559-8f6f24206edf"))) ? ((v <= 0) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("5e08e908-dd49-38a2-89d0-acbadef21c04"))) ? (((v) == (0)) && ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9106b5b9-47bd-3d0a-931b-fd817cc0c0cf"))) ? (((v) != (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("43ab88f4-396e-35e7-8bb3-3f05e8ca5da5"))) ? (((v) > (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("6b6b4780-b475-3dce-bfa1-e978d9d6eb5d"))) ? (((v) == (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("bbbe4e10-d977-36b4-b693-bdcb484c17c3"))) ? (((v) >= (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e96ec5a6-01ed-34ce-932b-044efedfb302"))) ? ((v <= 0) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f20b7bf5-9bd5-3df4-9476-b71f514715da"))) ? (((v) != (0)) || ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("cea81bf8-1b82-390f-924e-eefaad4ce835"))) ? (((v) > (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("6fab8ed7-073f-31e9-9d77-d76daf410483"))) ? (((v) != (0)) || ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9eac6c14-cc62-3ecc-866b-c446a497801d"))) ? (((v) != (0)) && ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4a54a60b-6155-3f10-b1cd-624f1192bfa8"))) ? ((0) >= (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("e765c9fc-2131-3914-b9f4-c2822f4d4b86"))) ? ((0) != (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("6a075f99-766a-3f9a-9254-e615d354f397"))) ? (((v) >= (0)) || ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3341fde3-6969-35a2-baf6-6a129d76c64b"))) ? ((0) >= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("4473207c-1e70-34c2-b676-70bda27338a5"))) ? (((v) == (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("5cd501e7-7ac8-3156-9cf9-e593ae226978"))) ? (((v) < (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f21c4baf-cf1a-3592-ac54-e4db35d541c5"))) ? (((v) != (0)) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("2ac4e79a-1381-32be-a71a-caf2c4bfeaa3"))) ? ((v <= 0) || ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4cb9961e-ff42-3099-95bb-410181468714"))) ? ((v <= 0) || ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4764bdac-af48-35b1-ad13-1c4460cd3772"))) ? (((v) <= (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("4104acc3-bb75-3f50-b970-e55a7eed615b"))) ? ((0) >= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("33481f2a-daa4-3719-82ff-969efeaab485"))) ? ((0) > (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("2579a83b-e3bc-356e-b096-2267be2c1a31"))) ? (((v) < (0)) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a71a1d81-4605-3ca1-ad63-27b22e451018"))) ? ((v <= 0) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("1a1620f6-b4f7-3a62-ad78-d640f56721c6"))) ? (((v) <= (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("6aca6caa-ee58-3aa7-b3a0-5a79ff252a0e"))) ? (((v) <= (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d30353ec-48dd-3af9-afae-77bf0f7ce997"))) ? (((v) <= (0)) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("943f96eb-99f6-3158-aa8a-62a2601d174f"))) ? (((v) <= (0)) && ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d5d297ee-bfff-38cc-9c26-ef7b7b093a09"))) ? (((v) == (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e62204e9-5982-3012-8e96-1d8c5eaf54cb"))) ? (((v) == (0)) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d0e1ccaa-f9d5-32d9-9ecb-7c40e84f4a72"))) ? (((v) < (0)) || (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("24432396-4757-3b0f-9e0a-cc84390aea0f"))) ? (((v) >= (0)) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("1e10ca5e-ff44-3380-a21c-03eb3f1b8dd3"))) ? (((v) >= (0)) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("48c3b56f-1efb-3d0d-99c4-ec6a2d550177"))) ? (((v) == (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ee33da93-3779-32db-b0ff-19ee37d79fdb"))) ? (((v) <= (0)) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ebdbc9e8-062a-3c92-ae71-3aebbdfb8590"))) ? ((v <= 0) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("dceea084-83be-30c3-8a36-c6f30bc8cc27"))) ? (((v) < (0)) || ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d73c63d2-a476-3cb4-84f1-5303f9bb039c"))) ? (((v) <= (0)) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("6d08a13a-4cca-3b25-b40f-5e77994f0432"))) ? (((v) == (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("de533ee5-d418-3866-aa04-e5a2b46ab2a1"))) ? (((v) < (0)) && ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("72c5f397-b77a-36df-863d-fcf40993c57e"))) ? (((v) < (0)) || ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2e9cfc7f-14fd-3cb8-966c-9c6917640859"))) ? (((v) > (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("88613101-5d5a-3138-964e-bd5f8e4bb06f"))) ? (((v) < (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d059213e-2f54-3c9b-b6d0-b39c44d08a2b"))) ? (((v) > (0)) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7d6eecb7-03c4-3d6d-957b-5064d6634368"))) ? (((v) < (0)) && ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("05b12cc0-f260-3846-81cf-c19564bd685a"))) ? (((v) > (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c79f19a7-9c4f-3f27-87e7-725350d8b826"))) ? ((v <= 0) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("77dbf2d6-77a3-319c-83a5-c4125ee99527"))) ? (((v) <= (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("59c65301-fcd4-3056-b2bb-4438c0c1e1f1"))) ? (((v) <= (0)) || (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("12c8368e-953b-3827-a1b8-f19a8637d886"))) ? (((v) > (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("cc31f65c-20b0-30a9-8447-b5d461172ae1"))) ? (((v) == (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("563257fd-6726-355c-95e8-d31ce37a8135"))) ? ((0) < (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("ea395ba8-f7c8-37ff-a8da-265279c55895"))) ? (((v) == (0)) || ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e8255771-720a-3bd6-b3eb-999a6d88af2b"))) ? (((v) <= (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7570aced-68f3-389a-b3c2-047c4eab6da0"))) ? (((v) >= (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("47c6d584-f95b-3adc-a249-932c03dee563"))) ? (((v) != (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("6271d440-62e8-3ce7-9efe-1477e958b3bc"))) ? ((v) < (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("b377bfe0-f7a2-3c7c-9d46-deb718541295"))) ? ((v) > (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("bcdb0f08-2152-348a-b0f0-4e8ae48f3b7a"))) ? (((v) <= (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("df51e6ae-9a81-30d7-a9b4-ed1358583c88"))) ? (((v) == (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("1faae645-fa22-3c9c-aab4-8bf01fe7c8e0"))) ? (((v) >= (0)) && ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d792ff14-6544-3027-8d7d-4babced912dc"))) ? ((v <= 0) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3591f84b-7527-377b-8b02-de5965c92d13"))) ? (((v) > (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a67fe4c0-9b31-3abf-90a5-6013588fa365"))) ? (((v) == (0)) && ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e3ea96a0-c376-3252-a608-180625263fed"))) ? (((v) >= (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d02633ba-fdd9-3425-aeb5-b4f1080f689c"))) ? (((v) < (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("81344f63-075f-3434-aafc-2665288af9e6"))) ? (((v) != (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9239f772-9c41-3afc-8b6e-800047078b0a"))) ? (((v) > (0)) && ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7d03e8b6-c348-3207-8c11-0bb5312f7203"))) ? (((v) == (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("125ea4c2-bf23-3587-9232-b9b611008774"))) ? (((v) > (0)) || ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("5b9326c5-b4fb-3ca0-96eb-6f9759ed6a0a"))) ? (((v) <= (0)) && ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("03341533-624d-3e51-8218-5e4184561be4"))) ? (((v) <= (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ceb17753-6fbe-33da-a3f0-7617f3312f8d"))) ? ((v) != (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("166fc716-219b-3bf0-8fb7-9e10e567bc03"))) ? (((v) <= (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d900069c-28c3-3103-ac89-598f86889818"))) ? (((v) != (0)) && ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("1629e8ff-8c60-38ff-b00e-eba20171c8c6"))) ? ((v <= 0) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("b5b07367-7409-32e8-bf88-8eb6c6040eb0"))) ? ((0) < (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("216bdf05-c1cb-3b0d-a210-db4a3cb9d4fd"))) ? ((v <= 0) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("01ec0a66-2678-3685-bedc-47b02d291dee"))) ? (((v) > (0)) && ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4c8f1a2a-0a5a-3614-ac66-a602101f123a"))) ? (((v) > (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("79c8635f-45b6-3e71-bc38-c5bc0cc2c90e"))) ? (((v) < (0)) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e41a16df-5cc3-3746-8aed-729c0c106d80"))) ? (((v) != (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9c31a1d7-e8e3-320c-8a90-80248fc46f4e"))) ? (((v) <= (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("6e74775e-7946-3a5b-82e4-c79d98bbaecf"))) ? (((v) <= (0)) || ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("9d51ef8c-db48-3757-8636-7075304f17e0"))) ? (((v) != (0)) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f981e216-7836-3414-b074-562ad39f2d42"))) ? ((v <= 0) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ef70f1c1-b410-3992-aaba-cdddd175cc65"))) ? (((v) == (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("afe272f5-2ef8-349a-8669-5204235b609b"))) ? ((v <= 0) || ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d33f4e85-dd14-309c-a476-c8a54ed45586"))) ? (((v) != (0)) || ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d1af1ea4-b536-34c2-b359-df68948d93fb"))) ? ((v <= 0) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("fd63cb45-941c-3745-9e76-3db8ad457791"))) ? (((v) != (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e58d8327-be21-38b9-acee-69528d4085b4"))) ? (((v) >= (0)) && ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7d08bc7d-8b95-3899-87b2-96ef0cee214a"))) ? ((v <= 0) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("89392a63-707a-314d-a921-bcfceca99be5"))) ? (((v) >= (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ac46566a-3a93-3541-ac08-52d68f7a7423"))) ? (((v) != (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("880a0c8a-a130-367f-903f-44fb1f788e7d"))) ? (((v) >= (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("dd180def-8d6c-3cfd-899f-a0f35fd19ea9"))) ? (((v) <= (0)) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4f2367b8-4cde-38d4-87a2-ff1486358cd0"))) ? (((v) != (0)) && ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e009b398-40f1-37e3-a660-bdb2cb265f6d"))) ? (((v) >= (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("376e87a3-8bca-30b3-a83d-8b781be6b451"))) ? (((v) < (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("15168490-684c-3900-9f6d-47cd1d03b304"))) ? (((v) != (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("2834e120-f546-37d9-8c08-22c6e3c13da7"))) ? (((v) > (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("5829f55a-d969-3747-8304-319fc8117597"))) ? ((v <= 0) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c65342d1-d224-3a45-a6b9-c5fc18db8fa2"))) ? (((v) >= (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9fd5f8fc-0ce0-3225-9f1a-c09800d0eb70"))) ? (((v) <= (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a30a9301-1db3-3288-b4d3-c7778543a6ce"))) ? (((v) > (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("3f27583d-6023-3dbd-a6a3-48864c5f2749"))) ? ((v <= 0) && ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("db2cc786-6cef-3104-8d92-3b5110ae46a4"))) ? (((v) <= (0)) && ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f3a9c655-1eab-3f1f-a603-0f5908b660e9"))) ? (((v) == (0)) || ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("a7e00f9c-fd94-3de0-907e-38e21910b6ef"))) ? (((v) < (0)) && ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c075fdba-0e69-3704-8c18-d7f152221038"))) ? (((v) <= (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7b0f629e-c259-3e78-b342-f6cd47758045"))) ? (((v) != (0)) || ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("58a1e4f9-a022-3f74-ba8d-69c1ebd6909c"))) ? (((v) > (0)) || ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c4477d94-5acb-3260-a616-e662bbee99ae"))) ? (((v) <= (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2fced86f-2a5e-3a77-a88e-f441667fe3a4"))) ? (((v) <= (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ed56d4d4-f17f-3e4e-a5d6-b2b077ef09e1"))) ? (((v) < (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("ce7d3892-2036-378e-adc6-eb495cc3c64f"))) ? ((v) == (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("0da7013f-9635-3b46-8a88-49c838c79324"))) ? (((v) > (0)) || ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("67d74c6a-496c-3c1e-9b05-74b7a8de7c95"))) ? (((v) == (0)) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("5c4b1c3f-d6fa-3350-90a2-dc0046c99a14"))) ? ((v <= 0) || ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("aa0233f5-77c4-3aa6-9ae8-2ac9acb7038c"))) ? (((v) < (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("09ea8778-f628-3366-bf9d-c552913461ac"))) ? ((v <= 0) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d7dd68cd-8fb3-3dc4-b10e-145376427862"))) ? (((v) == (0)) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c06637a4-7492-3960-8fff-12a5c18611e5"))) ? (((v) >= (0)) && ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("07b4d95d-605a-311b-ac2f-2c7309e3d206"))) ? ((v <= 0) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a1ac2015-37bf-3c78-b9d1-c8f522349596"))) ? (((v) == (0)) && (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("3bf04f6f-1800-3730-bf0c-6f6d4d9ee895"))) ? (((v) >= (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("94cdc5bb-9ca5-3a85-ae70-05a6a5408753"))) ? (((v) <= (0)) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("42c37d69-a7ce-3458-9329-a5e668735a77"))) ? (((v) >= (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2905268b-abbb-399f-8479-bbd799176b89"))) ? ((v <= 0) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("0d54cac0-2cb5-31f5-9e24-7d7f9b0da9b4"))) ? (((v) <= (0)) && ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("fefabf13-69d0-3b18-b26b-d2f378d2420b"))) ? (((v) >= (0)) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("0bfaac00-6915-3bcd-9a30-e7586b7162ce"))) ? (((v) >= (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("dcc7d5b4-f759-3b71-92ad-391fabfed9f3"))) ? (((v) != (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("2aabd9ec-c1f3-3ca3-b196-10f7d717de9f"))) ? (((v) != (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e0e7806f-a512-3a98-813f-b77132b2f2ce"))) ? (((v) < (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("851e019a-6e7f-3562-8f70-54c93c68d77f"))) ? (((v) > (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f304d107-174d-376f-928e-de586264655f"))) ? (((v) > (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ce34ab4b-a92f-3110-9736-76d778af8aa8"))) ? (((v) == (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e57d7e2d-0df1-34a6-8d2b-95891e0eced4"))) ? (((v) == (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("bfba2929-4279-33cb-ba34-62f67a6bf978"))) ? (((v) < (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("2cb18ac0-b3a1-3534-b9e6-16d2fe7f96e3"))) ? (((v) >= (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("50cc24ec-5292-36be-a3b1-2ef2e5cb5bf7"))) ? (((v) > (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("fb9a980d-adb2-3dab-8bf1-bc6bc38b3004"))) ? ((v) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1bc0a2c5-224e-3c5b-8a60-be3d2607a70c"))) ? (((v) <= (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("455b4145-3e86-3b2b-a7d9-1326e47b1113"))) ? (((v) <= (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("af7ee4a3-9e25-3eb5-85b5-6599784d844a"))) ? ((v <= 0) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c1b9267a-80cc-3ecb-87d9-88206348e84e"))) ? (((v) > (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e4b81e36-9a47-3fbd-a816-3208588ba6eb"))) ? ((v) < (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("daeab687-f85e-3811-8b10-8753424a5c86"))) ? (((v) != (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9103a376-44ac-38e3-b40b-981214834136"))) ? (((v) != (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("18fe3d9c-e38b-3bbc-ba4a-701a2642a5ca"))) ? ((v <= 0) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("3ae81739-16ad-3b58-b6dd-99c71f686ca1"))) ? (((v) < (0)) || ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("bced1be6-0921-34dd-a2b2-0c3176a4cc1a"))) ? (((v) >= (0)) && ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("0e4fd3fd-3f59-3f80-a02c-8011cc540a40"))) ? ((0) > (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("90c87a8b-8a19-3ae9-85ca-fa63a1034b6a"))) ? ((v <= 0) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("32cf0870-35d1-393c-a78b-ba920d16d208"))) ? (((v) <= (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("8375c915-e37b-3dc2-b006-db9f5f85f6dd"))) ? ((v <= 0) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a628750f-a538-3086-b1e4-a8d6d69fcf1f"))) ? (((v) > (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3c901fd9-26a8-3a4c-b64b-f7a0ff3ef774"))) ? (((v) < (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9b010a1c-b743-3ea6-8a9f-8344ebea300a"))) ? (((v) <= (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7435fccf-6a7a-33e7-ad06-ace1b3ada05a"))) ? (((v) != (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("1c8e85f7-7602-309e-b5eb-055d0fb729f3"))) ? (((v) == (0)) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("642b8430-3eda-3640-98d3-6fb9448180bb"))) ? (((v) != (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("47bd8864-e872-3515-b48b-c2ce239797c9"))) ? (((v) == (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("32078cd5-0fc9-3330-8fbe-5afe23dc9c94"))) ? (((v) >= (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("62d07acf-7515-389c-9a2f-a9ddf0d17162"))) ? (((v) <= (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e09b13b9-f368-3358-9ebb-293f6b896873"))) ? (((v) == (0)) && ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c911af07-155e-31ee-aaed-2e475cc7a97b"))) ? ((v) > (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("0cd4cd1c-f02a-3c3d-bedf-4844a3aee63f"))) ? (((v) >= (0)) || ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e67c5505-7861-3a5a-b2a8-1d498866edad"))) ? (((v) == (0)) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("bb822616-25e3-31b3-8704-09190b15dcb2"))) ? ((v <= 0) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("b9c1bf7d-7d8a-367c-b38c-32306b3d6dc8"))) ? ((v <= 0) || (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("3c346d6f-576d-38fa-97fc-13454dd06331"))) ? (((v) <= (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("b7048684-69ae-3915-9586-15fcbc391c9b"))) ? (((v) == (0)) || ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4c8f06c1-7c5a-3f4a-a674-9125bf4f2083"))) ? (((v) != (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d7f9c048-584a-33d3-a9c9-cbaeb25288a8"))) ? (((v) <= (0)) && ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("009576b1-dc69-3a5a-9cc9-8c5073135596"))) ? ((v <= 0) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("6c936ffb-982a-3ddb-8f29-d2d99ece43b4"))) ? (((v) < (0)) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("31bb63fe-4fd0-35a1-8354-9dfebab4e3ae"))) ? ((0) <= (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("e977fb2f-d9aa-3719-9937-ae530977f9ef"))) ? (((v) == (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("671af32b-cfe9-3020-958d-6d956c8f5f5b"))) ? ((0) <= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("c4fb95fa-e985-304a-a7d0-a4303f88288c"))) ? ((v <= 0) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("ff441a91-e947-352e-9427-edbbd3e846b7"))) ? (((v) < (0)) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("51005c45-1b62-3902-98f3-0dc39522f64d"))) ? (((v) < (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("644ecff7-669a-3184-a1bb-e519813942d7"))) ? (((v) > (0)) || ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7603c4c4-8111-3a07-a8c3-b5bbf8d7e715"))) ? (((v) >= (0)) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("dc53f663-243b-3124-80b6-5ca40e0c3736"))) ? (((v) > (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("788aa19f-a46f-3421-ab70-7ad1187c670b"))) ? (((v) <= (0)) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2058f61d-3279-3c0a-8792-0a878194fe8a"))) ? ((v) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a96ec01c-1bb5-3603-9dd7-06b0e011aef1"))) ? (((v) == (0)) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("dd8b6e31-34eb-3cfe-a968-4d50cc6b2242"))) ? (((v) <= (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("36764cbf-a5ac-329e-8adc-70e7bf47cbeb"))) ? (((v) <= (0)) || ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("306348b3-8542-30e9-8ee1-719c9301c74f"))) ? (((v) <= (0)) || ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("064997e7-5c35-319b-a660-e03500663315"))) ? (((v) > (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7aadb51b-25f0-31e5-a17e-bee315bcff06"))) ? (((v) == (0)) || ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("fbf6adda-658d-3544-827d-287baaf32df9"))) ? (((v) == (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("fefdcc91-5e9e-3635-8251-267bc42928f9"))) ? (((v) != (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("60ae9562-2976-38a8-9472-218138b06e66"))) ? (((v) == (0)) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4835eb2f-cec8-37ee-834f-7c841afa6327"))) ? (((v) < (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ab14bf34-8b22-375b-8162-7276ad5a541e"))) ? ((v) <= (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("3d90c2f4-b573-3bd5-bb20-b25f4512d18e"))) ? (((v) <= (0)) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7702d0ff-aa63-3855-a855-9b546393133b"))) ? (((v) > (0)) && ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("20238167-7410-3e8c-b6a8-3a9ba3b2674f"))) ? (((v) > (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3ea2ce00-3246-330e-ad68-c6ff5ef82d2b"))) ? (((v) < (0)) && ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("125d702a-6a09-3177-9994-8c62414806e7"))) ? (((v) <= (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9306a03c-8e16-3069-97a7-0a95b6b831e0"))) ? (((v) != (0)) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("1fbbb346-d09f-3b1b-8f65-ace0daebd143"))) ? (((v) >= (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4010c3cd-c67e-376d-b3f6-e23d52d84490"))) ? (((v) == (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ef0c5a8a-9b2f-3eae-9e20-fcc8bf216530"))) ? (((v) < (0)) && ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ee6bc156-2275-3bff-a36d-39d87468e456"))) ? (((v) < (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9804367e-aa07-3e9f-8d3b-2a154cfa639c"))) ? (((v) != (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2b9fd471-20e5-303c-a590-b5e5c12965c3"))) ? (((v) == (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ecd80efe-c086-32b0-a3e9-b47055979f77"))) ? (((v) != (0)) || ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("785eb82a-22ba-3aa5-a2c6-df26c60df036"))) ? ((v <= 0) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f4602aa2-2bfd-3bd1-926a-49b7d46fdfda"))) ? ((v) > (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("79e79c14-559d-3124-b86f-3ea4036e798e"))) ? (((v) < (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9e09be58-6e24-38fe-81be-06df883642cf"))) ? (((v) < (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("732f66e6-30b2-303d-bba0-a762f7e0ea6a"))) ? ((v <= 0) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("cb2c425c-b4f9-3800-a15a-c5da65705013"))) ? (((v) > (0)) && (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("ac720619-fa07-3df8-aa3e-4f94d52e5aa3"))) ? (((v) == (0)) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("11c98e92-6fc3-3aa8-b6a5-5cea6bf2cc53"))) ? (((v) == (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ec8ed6d7-d4b3-3cf8-986a-b6fc97536b24"))) ? (((v) < (0)) && ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("52b9cbfd-3bdd-3c26-85a9-6fa25aaab6e5"))) ? ((v <= 0) || ((0) != (getKVBufSize()))) : (v <= 0 || v > this.blockBuffer.limit()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
    }

    /** Returns True if v &lt; 0 or v &gt; current block buffer limit. */
    protected final boolean checkLen(final int v) {
if(KnobRuntime.check(java.util.UUID.fromString("58200146-85e3-3352-a336-dd6b94c9a391"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("305351c8-80ce-3429-bf0a-8b4958aa261e"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("58b628b6-9c31-33bc-8c99-38ea527b0139"))) ? (((v) <= (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5863c519-390d-3ea0-8447-78c38e441eee"))) ? ((v < 0) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("1b31147b-8d9c-33d4-9af7-288a8326cf7b"))) ? (((v) > (0)) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("fd08f2bf-4280-3a07-a572-84e0103844b6"))) ? (((v) > (0)) && ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("3c92de96-3230-31bc-a733-ab2e5154f780"))) ? (((v) == (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f5a0c8f6-516e-3261-8288-fcade039ef3c"))) ? (((v) >= (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("33b3fc18-5dfb-39c6-9c44-9e21d1c5c7fc"))) ? (((v) <= (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4e1b8d25-9c2d-3d2c-84a5-f00af21dd08b"))) ? (((v) != (0)) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("a7078ac0-4a61-32ac-bd04-0f272eaa80ee"))) ? (((v) < (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9bf501f2-13d1-39d5-b8b5-e0f33a94fc5e"))) ? (((v) <= (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("86dac43b-c62a-3271-9f43-a11a85e3e07c"))) ? (((v) < (0)) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("24858550-d94e-3e92-96a1-a65e617cad44"))) ? (((v) == (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("54315b8c-98c9-30dd-8cc4-25ee2f9e213c"))) ? (((v) < (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("bbe3c760-5299-3f4e-9135-69b758a69534"))) ? (((v) > (0)) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f87c7e8c-b946-37bf-af14-91a7ee785f6b"))) ? (((v) < (0)) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("d740544f-135e-36d8-a50d-fe1f690ba780"))) ? (((v) > (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ba641357-e2cd-39b2-a4d7-57e71668f2f6"))) ? (((v) > (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("9056be31-525e-34ad-96ae-a4e8b3399ad6"))) ? ((0) <= (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("04f602cb-7469-3f02-8220-a8dbc536604d"))) ? (((v) >= (0)) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("11d06865-70c5-32e2-a3f1-337720559a72"))) ? (((v) > (0)) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("61271e99-bdd9-3f0d-bd7e-db4ac973d099"))) ? (((v) < (0)) && ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("db990c6b-a982-38d8-88a1-aa2debf5a3bf"))) ? (((v) <= (0)) && ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("55a22412-2196-3b63-a867-903b1761a702"))) ? (((v) < (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("5cc2bef9-5369-35c0-af3e-14c101d847fb"))) ? (((v) <= (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("67d33b54-7414-3995-9396-ee62549b92aa"))) ? (((v) != (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("b116038d-bd19-321e-8bb0-a0b15330aaf2"))) ? (((v) < (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("1b905ef9-2cbb-352d-8d99-fb398c213512"))) ? ((0) == (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("574514d4-98ba-3d2e-a8dc-50b67d467c6b"))) ? (((v) >= (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("8c9c9602-9d96-3164-a36b-9acb989b39e9"))) ? (((v) == (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e57d3127-0e30-388e-88c7-9f918c29fc4c"))) ? ((v < 0) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a6814e03-cab9-3271-8e2b-fe6f05453b43"))) ? (((v) < (0)) && ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("31e4de5e-126f-3a79-b693-b9d95a9df1c6"))) ? ((v) <= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("cdae7ead-c46c-3a8b-a430-c54e1dc2244d"))) ? (((v) != (0)) && ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("59420089-7d90-38e2-a682-38c9a9372ac0"))) ? ((0) != (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("ed4e9af0-fdb3-31c1-b63f-234f6cf28702"))) ? ((v < 0) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("82048e7b-9284-325e-9d7f-2362c3ab4712"))) ? (((v) <= (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("73809755-61b2-3365-91ad-a439085ad158"))) ? (((v) == (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5a787ad9-dbc5-39b6-a938-45608141b4f4"))) ? (((v) != (0)) || ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a5e4c7f0-00c5-3cd1-8991-724e3b804473"))) ? (((v) >= (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9b5be07a-cdc9-3c0a-9ded-e59916f63c91"))) ? (((v) <= (0)) || (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("62f65891-163a-3bed-8d43-482d7797d7b0"))) ? (((v) <= (0)) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2b09a323-6778-3401-a08e-cbc66158749b"))) ? ((v < 0) || ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c7d848cc-83ab-3d4d-a5df-0653ddb4c891"))) ? (((v) == (0)) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a75e7533-c24e-3da8-a543-781f90c986c2"))) ? (((v) != (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("fe5c6044-4e05-3902-9e43-d6da20c36b3c"))) ? ((v) >= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("f84d4473-9ca0-3507-9428-054aeaf261a2"))) ? (((v) >= (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ee3933df-3c41-376f-b488-b85a348a175f"))) ? (((v) > (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d1e71dce-cbce-3ad3-bf82-e1c68ae67c78"))) ? (((v) <= (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("2d4450c6-7d2f-39a9-a0c3-8954886b26e0"))) ? ((v < 0) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("6ed30d7a-446d-384e-82b9-6ace8f320b72"))) ? ((v < 0) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("976da64a-5a46-3eb6-8097-22d686a9363b"))) ? ((v < 0) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("72ec6c71-d97b-38b9-a0ed-2f24e675dc84"))) ? (((v) >= (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("0ee35432-e992-336b-9f53-a4c472578808"))) ? (((v) >= (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("11658ac8-4712-3810-a98f-9da272c7a967"))) ? (((v) != (0)) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("dedfb693-3c93-3d9c-827a-dc30d67db030"))) ? (((v) != (0)) || ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("94b90897-9902-384d-9106-b390c66a5c07"))) ? ((0) == (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("56000725-44a3-3808-a4bb-2b7183ebc86b"))) ? (((v) <= (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7956552c-4bca-3390-a994-efdd275bb197"))) ? ((v < 0) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ac6e1b92-a1d6-388a-9aee-e78bd7ea650e"))) ? (((v) == (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("477fd9e3-de59-3ddf-bafb-2fea11c81d63"))) ? ((v < 0) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a5a1b4ea-0bad-3fb9-b4b1-1d339ced5ebc"))) ? (((v) > (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("98e45fe6-1eb3-3451-b526-e86d217b90db"))) ? (((v) != (0)) && ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("3686a37f-517d-3f72-b19f-66ee5f83bbd8"))) ? (((v) < (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("3a97d062-a84e-3687-a8c0-ac0915733554"))) ? ((v < 0) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("8f7edfb4-a9e9-3a95-9665-a216013044fe"))) ? (((v) != (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("6fa67332-56d9-343d-8530-1bcaae80f0d4"))) ? ((v) == (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("a5c4c346-63ad-3f08-a3d0-340c1b9432cf"))) ? (((v) < (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("55b9d10c-2b9f-3402-835e-dfd92c28c799"))) ? (((v) > (0)) || ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9ef69e93-3fb1-3e50-af5d-27b5f49428cc"))) ? (((v) > (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("3b9e6653-2d47-3dff-b016-6bc654505796"))) ? (((v) < (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c5cde836-d033-38a8-a313-004b7183cd2c"))) ? (((v) <= (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5af7b05c-bbbd-300e-afaa-592a2b0be52c"))) ? (((v) <= (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c1fa191b-b6f9-371f-8b93-2f797a069d0d"))) ? (((v) == (0)) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5ddb68aa-0e5b-3de5-b2b1-6297d59faa0c"))) ? (((v) >= (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7d5a2f80-67ca-3bf0-8485-9ed2f997ea23"))) ? (((v) >= (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("dd688cdd-c1b8-302e-b100-078a6ddb8922"))) ? (((v) >= (0)) && ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("92703041-319d-37bc-bfec-eb6b605357c9"))) ? (((v) != (0)) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9e8ce460-0e3d-3def-83dd-6e7edd84b23c"))) ? (((v) <= (0)) && ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("2299df72-e908-32c0-a36c-c332a180f695"))) ? (((v) >= (0)) || ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("3cac0609-ca9d-3229-93d5-353b85199a4b"))) ? (((v) == (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("ac5eaada-019c-36a2-95fe-2d82d58c3521"))) ? (((v) == (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("aa251a44-c5e5-3e40-9c60-8ab89df54bf3"))) ? ((v < 0) && ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a1c9d541-588a-3e83-aa24-5b1f4aadca57"))) ? (((v) >= (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("84368151-87f2-349a-af4e-e9916b3ab7ac"))) ? (((v) != (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("cf027484-639a-360f-b4e4-77eef0efc12d"))) ? (((v) == (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("577f203b-4180-3b61-a3b6-c01af2af66b0"))) ? (((v) > (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("fbdcf8ee-3df7-381f-9e71-ccef6c645c7f"))) ? (((v) <= (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9cdb55c9-23f5-39ee-99ac-d013ad03685a"))) ? (((v) != (0)) || ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("76ee1458-6916-3bb1-ba32-6ca8cabf773f"))) ? ((v < 0) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("dd121e9f-bb8a-3956-b60c-30d56b2c6eec"))) ? (((v) == (0)) && (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("7bcc9e2d-a61e-356e-932c-6fad1c63f6ac"))) ? ((v < 0) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("b5e058fa-8768-391e-9f51-e8f40ca51921"))) ? (((v) <= (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("20002797-3448-3046-93f1-788f9c415fcc"))) ? ((v < 0) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ccbe50ab-dcda-3fdc-b2d5-3f4d28bd86cc"))) ? (((v) < (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("0e4fcdf7-fb8f-3200-a309-b749198ee6f5"))) ? (((v) >= (0)) || (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("de12d9d8-561f-3293-b787-d435a945fd38"))) ? (((v) != (0)) && ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("69400ad7-c086-3429-a6e0-20fbf8b5ccdd"))) ? (((v) == (0)) || ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e45fa881-8bbc-3905-beb6-7370cf0e0a1b"))) ? (((v) >= (0)) || ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9c273203-f4fb-3550-ad3b-fce085bb8030"))) ? (((v) != (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("4e970b5b-b264-3cbc-bc4b-db386fc4c9d0"))) ? (((v) < (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("01a21cfe-f1e1-3deb-9cc2-41744263ff71"))) ? (((v) != (0)) || ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("de6b90cb-025f-3469-9434-6e7a7acf2d0d"))) ? (((v) >= (0)) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("6452267f-c748-3b54-aa08-f207f18fa9ee"))) ? (((v) >= (0)) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("41b6e61f-b49d-30b6-b0e7-493e20df1afc"))) ? (((v) == (0)) || ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7746f568-f276-31b0-b5e0-79f4b96247ed"))) ? ((v < 0) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("14ff9e00-fb42-3d6b-89e1-122423a56518"))) ? (((v) == (0)) || ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("48d31203-ca17-3d0d-857f-69d6b97139d0"))) ? (((v) != (0)) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("27f5b4d9-c50c-36ad-ba3e-e612cb5ad185"))) ? ((v) >= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("3ab0cc7d-1c01-3846-8a41-1f2fec04a8de"))) ? (((v) >= (0)) || ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("efaeabdf-962a-3ce6-9ab6-3929ffbe5e54"))) ? (((v) == (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("bd3ea859-f6c8-346a-aece-23057f823667"))) ? (((v) <= (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ce4e2718-47dd-35d9-9ab4-d88f8b72812e"))) ? (((v) <= (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("5fedbd7f-a36c-3533-9f22-5385234ec222"))) ? (((v) >= (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("39a51b26-8565-3ad6-aeef-08ba847ba720"))) ? (((v) > (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5c914461-abaa-32ff-b414-ea940cd99758"))) ? (((v) != (0)) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("8fb640d3-db7b-35bd-958e-7ac5f2cb71a4"))) ? (((v) >= (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9ea49206-a188-36c2-a162-a2344c6f9e67"))) ? (((v) > (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e6437676-6a4b-3ec3-b9ac-c186d3bbb000"))) ? (((v) >= (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("0379b143-cab8-323d-b975-9d6f69dc9fec"))) ? (((v) > (0)) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("b28dd96f-c62f-356a-a6db-7a906fb402df"))) ? (((v) > (0)) || ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ae86d057-1b16-3bbb-8ef9-41d6fa2e9d1d"))) ? (((v) <= (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("24a48a68-494c-39b0-a4c6-de4952d3fd12"))) ? (((v) == (0)) && ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d1d5ae1e-8db9-3ae7-a298-9d1f5895d367"))) ? (((v) >= (0)) && ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("b6ca94e4-92ab-303e-aa01-87cbd0408744"))) ? (((v) > (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("fd4d1536-7120-3714-bf58-afdf46b11c70"))) ? (((v) >= (0)) || ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a7992054-6c6a-3d3b-93e3-9d53c6f21436"))) ? (((v) < (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("9232acb4-9843-3f37-8790-f5a59edddb71"))) ? ((v) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9292b43a-2ea1-3ee3-9f58-9f435296a74d"))) ? ((v < 0) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("bfc569ee-3d4b-3242-9531-e0592aed7668"))) ? (((v) >= (0)) && ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7ca20983-5717-3856-8897-ed02d13d8b28"))) ? (((v) <= (0)) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f619027c-cff8-3332-a7bd-507d888f29f0"))) ? (((v) < (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("cec23e19-bb49-32e4-a64f-7e3ce08ad70c"))) ? (((v) != (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("16e00a0a-d817-3ed5-8c5d-9e20f3aae830"))) ? (((v) < (0)) || ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("f187081f-6bb1-340f-bc8e-797a463e793d"))) ? (((v) < (0)) && ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("45745a99-3341-3029-921c-954251563260"))) ? (((v) < (0)) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c9fb3dd1-2fba-3eda-9f0a-030e8042d986"))) ? ((v < 0) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c8900a30-8baf-31de-84d5-40c98002d030"))) ? (((v) == (0)) || ((v) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("049f12bc-7cfa-38ce-b3f6-6116d0731c51"))) ? (((v) > (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("6cb415b1-9581-39c6-87f6-45b9e3aac827"))) ? (((v) > (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("00458a8b-15f0-3c1e-818a-e1ca7a1bd11f"))) ? (((v) == (0)) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("aa082f0e-6b83-3e6b-a3ef-0f36c00599ff"))) ? (((v) == (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("765a3ffa-48a7-3ed3-a690-aeac1ea29ce3"))) ? ((0) != (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("4108e432-2965-3f16-bd88-d8c542dac9a4"))) ? (((v) > (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ba8164cc-2b74-37ab-a301-a23e7672ab64"))) ? (((v) != (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("6565f2e8-c209-34d9-8ae4-3e601ebfa9c0"))) ? (((v) >= (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("729fcbd6-1323-3d8e-af48-deb61dff5e0f"))) ? ((v < 0) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("becc026b-66b8-3d7a-8103-d81003aaab8f"))) ? (((v) != (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("41f19388-44e8-3b1c-a5f7-8a8df3a87a35"))) ? ((0) < (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("0c49ca06-cdc9-366c-adad-531baa21b162"))) ? (((v) >= (0)) && ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("3b1433cc-a4ae-3b68-8931-251ea3b94e55"))) ? (((v) == (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("943c60f5-f6dc-37ba-a228-a40c85c4e965"))) ? (((v) == (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("462b977d-66bb-3acb-b2d9-48735b51fa8e"))) ? (((v) != (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("fe3d077d-baad-3597-853f-494aea09fdf0"))) ? (((v) <= (0)) && ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d51c373e-2359-38e6-8682-15acbd93bb35"))) ? ((v < 0) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("299b97de-6d0e-3fee-8cd2-fd25fd14c39e"))) ? (((v) == (0)) || ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("0c3c0e64-d3e9-3fc9-99ad-f0c4091a3f38"))) ? (((v) > (0)) && ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("0d251261-2ec7-398d-a439-340aab1690f5"))) ? (((v) <= (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("540133c8-325b-3f60-99c6-06ddc3ee3c1b"))) ? (((v) < (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e2964958-0f82-3182-a9b0-3e277284b906"))) ? ((v) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3468e27b-e659-3c8c-8949-987188174281"))) ? (((v) < (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("c8cefa30-ba3f-319d-a68f-0614d2100b0a"))) ? (((v) == (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("14ce2a3c-4a9d-3669-809d-3c85be02f255"))) ? (((v) >= (0)) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("773480bd-1651-314d-8604-8b22029d22da"))) ? (((v) == (0)) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("cf9eb3aa-942f-38f6-9b79-4ac814beec1a"))) ? (((v) > (0)) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c47ac855-d9e9-35e7-a5e9-bdbf98006e06"))) ? (((v) > (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("bc3556c6-3c80-33ae-84e6-361619319a8e"))) ? (((v) == (0)) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f310e84e-37c3-3c19-ae5f-84b4f56d3c9a"))) ? ((v) != (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("96cb0eb8-2f9b-367c-8d51-7175bc96575d"))) ? (((v) > (0)) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("ab2f7dbc-d48f-3bde-a0bc-97cb8da600fe"))) ? (((v) != (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c4198fc0-dc77-3102-972d-e22f1852bb91"))) ? (((v) != (0)) && ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c25fa8fa-9060-35dc-8d93-1dc4895c7bce"))) ? (((v) != (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("b75b971b-7b21-3954-a287-20e83903c86f"))) ? ((v) > (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("799507a7-7c07-3834-9223-b2fb9398eba2"))) ? (((v) == (0)) && ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("23f0401f-5739-364b-961a-bd7b2ce11c6a"))) ? (((v) < (0)) && ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f88b4ba5-1a98-3958-a8bd-7bb41a3c7ab6"))) ? (((v) >= (0)) && ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("47cc64a5-c1c9-3919-8a2e-3781195b8d47"))) ? (((v) > (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("35b69b8e-1913-34a1-a8d2-756fdacdf23b"))) ? (((v) < (0)) || ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("8fde6e08-cc4b-3e25-9741-a94972209a5a"))) ? (((v) == (0)) && ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("1bb45326-c0fe-3015-98e7-127816f80157"))) ? ((v < 0) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("0cbc9df0-ad79-31a3-856c-ce98aa9115f7"))) ? ((v < 0) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("bcce9f09-ba5b-3e0d-8538-5c6daf60fd5c"))) ? (((v) > (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4d418e50-f634-3b4a-b651-e974456e2c3d"))) ? (((v) == (0)) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("720abb20-eada-3838-8850-c62e943b8ebc"))) ? ((v < 0) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d883f4fa-9a8d-31ce-9920-5856349aed1d"))) ? (((v) < (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("23aec456-45e1-3956-8e24-2cbbebc837df"))) ? (((v) < (0)) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("63bf1c01-a234-3d17-853d-487471796ab6"))) ? (((v) < (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("28011ca5-8b7e-343f-b319-59a71f2b1e2e"))) ? (((v) < (0)) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("27c7f89a-244d-3d48-bf3b-538ad08c399c"))) ? ((v < 0) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e8c38c5c-24f7-3cec-a331-a73c48b1a1ea"))) ? (((v) < (0)) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4ce9dafd-c963-3c09-9d9f-612415560158"))) ? (((v) < (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e9cea280-975e-3293-85ae-97b29b7ef044"))) ? (((v) > (0)) && ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("acbd0058-720c-3c88-92ee-2c4a5b896773"))) ? (((v) < (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ca76ff21-a2b0-3e58-9537-024d42e80e49"))) ? (((v) <= (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("06b90c94-0ae2-3743-bd16-ab521a7f6537"))) ? (((v) == (0)) && ((v) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("601b8954-57ad-35a2-aec3-e7d41d144b41"))) ? ((0) <= (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("59d9e17e-b0d0-3978-ac52-8f6a58182e97"))) ? ((v < 0) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("13b7e23a-a924-3d5d-bece-ecc3377323d1"))) ? (((v) == (0)) || (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("51fe2a90-945a-3f42-8dd0-1eee6fb23cb4"))) ? (((v) < (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7fa2485c-1b31-319a-9cbe-70226844aa0f"))) ? (((v) <= (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("9ae115d2-3f6e-30ba-9a8e-8a6bc7277282"))) ? (((v) < (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("264508d7-9fc8-3073-bfa8-f1319d817b6a"))) ? ((0) <= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("d09e0512-36a1-3176-9db4-2f2a9593e348"))) ? (((v) >= (0)) && ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("13fffc22-add5-3d5d-b108-c5d51c0e2d25"))) ? (((v) != (0)) || ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("74ae943b-d0d0-381a-b2a5-b62a9770d81b"))) ? (((v) == (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("b7a241cb-f7a6-32e8-91db-48b1c3bc1145"))) ? (((v) < (0)) && ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("af67a3cf-3eeb-3175-826c-53e120896113"))) ? (((v) > (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5d072d2e-49bb-3323-8b05-d56cdd257144"))) ? (((v) >= (0)) && ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("4a2ecd7e-eacc-3927-baa7-a33722fa614b"))) ? ((v < 0) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("607cc13e-9f59-3cd3-bb49-f926a6eec97e"))) ? (((v) != (0)) || ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("6d460af5-9c81-350e-aa21-e189688eed87"))) ? (((v) != (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("67c4c01c-4d5d-3da9-a207-cf21192fb5de"))) ? (((v) >= (0)) || ((0) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f4676537-3a97-36af-87ab-e69c401177fe"))) ? (((v) <= (0)) && ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("8720671a-8fb6-31e3-ba23-da5d633b319b"))) ? (((v) < (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("f3964807-61ad-3392-86cf-e2a10fbdcf65"))) ? (((v) > (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("53e16bcf-5f76-338a-bba5-7e1d7cc2cced"))) ? (((v) != (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("8bdbd858-389d-302d-9bb4-75eac0ed2a35"))) ? ((v < 0) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("21e49f83-02ac-3660-92a8-4a44ac8b5367"))) ? (((v) == (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d5e700b8-636e-392d-b012-b23266d4f8b7"))) ? (((v) < (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("028a7c2f-35d3-370d-b2cd-5f82ed4b0e1c"))) ? (((v) < (0)) || ((0) >= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e9d1202c-f40e-372b-b19b-c4591c99ad04"))) ? (((v) >= (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("a2a4c681-db78-3a83-b712-7f74ed801ed2"))) ? (((v) == (0)) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f3e0cc50-9564-3d8e-bbda-74972ab3b815"))) ? (((v) <= (0)) && ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("065841be-e078-34c2-98d9-0e73f51a2ab1"))) ? (((v) <= (0)) || ((0) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("14f0b29b-7fea-3266-8228-bb3c581511ce"))) ? (((v) != (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c65504c0-3ce9-3b82-9001-89a713c61063"))) ? (((v) == (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("42aeeb75-0bb3-31ab-8a40-f3d17519c0ed"))) ? (((v) <= (0)) && ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("5f16aaf1-6e40-3168-8293-8ff68714b9d0"))) ? ((0) >= (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("923e4a96-30fe-3676-8d13-432538be428f"))) ? ((0) >= (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("6f2ed808-0351-340b-a519-0010e8e2f5c4"))) ? (((v) >= (0)) || ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("79c10a9f-6f04-3e8a-a8c9-611bcd50b8a8"))) ? ((v < 0) || ((0) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("18b4ba42-9b23-3e00-8db6-ace2273cb114"))) ? (((v) < (0)) || ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3b46964c-4fce-3435-9068-5a7afa61a8e6"))) ? ((v < 0) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("e2efc632-0946-3f6d-b634-4c7fc20c0a40"))) ? (((v) == (0)) && ((v) >= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("500d7965-746c-3c9b-804a-4e035676962b"))) ? (((v) != (0)) || ((v) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("21c1b424-cf7b-3454-b289-a37c96502b99"))) ? (((v) != (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("84f7e737-efdd-39c4-828a-c772f6981a20"))) ? ((v < 0) || ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e198bda3-d421-348e-b13b-e1207c535146"))) ? (((v) == (0)) && ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("d976712e-e953-3195-9008-0dd7e0e2d680"))) ? (((v) >= (0)) && ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("daa7be72-c63f-34b6-b1bf-699f4613a376"))) ? (((v) < (0)) && ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("bc0ee8e5-96be-36d9-a2b7-5857044b9393"))) ? (((v) > (0)) || ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7b874ec5-1d80-3a0d-a58f-8fbdb7ab1266"))) ? ((v < 0) || ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3c565bf9-6a5c-3218-9441-87bdb020bb17"))) ? (((v) >= (0)) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("2c357d42-0a20-3191-b6e5-ee63a6d343a6"))) ? ((v < 0) || ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("739c4f9e-20fb-3c6e-9bdd-ceaffe565a8e"))) ? (((v) > (0)) || ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("445863a0-9018-34bd-9cd8-442d830c4ac2"))) ? (((v) == (0)) && ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("0986005c-a030-39ea-b960-015da90c249f"))) ? ((v) == (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("8d934a8c-f19e-38c6-b797-9522fa0ddffa"))) ? (((v) > (0)) && ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("17f3613c-190b-35c9-9f63-9f36bb09e7e2"))) ? (((v) <= (0)) || ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9292eb0f-0e04-375f-97e1-d6dfe5de4dc5"))) ? (((v) >= (0)) && ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("73394d30-9743-3f77-b31e-6b764eebae8b"))) ? (((v) >= (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ee1c20c5-f9fa-3984-8639-e63d750792dd"))) ? (((v) == (0)) || ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("dc040be0-f43a-3461-8060-9bf22e92c3a0"))) ? (((v) == (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("4b5efb8e-7bb0-3caf-9f42-3c7bcccdb15c"))) ? (((v) == (0)) || ((v) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ae04b421-7635-32c3-a159-113bc344c30f"))) ? ((v < 0) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("7957c553-e961-3d47-b289-0f5819178d6d"))) ? (((v) >= (0)) && ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("42cf453b-2512-3c24-ba75-4f5dae6468eb"))) ? ((v < 0) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("9531713e-5acf-35e9-b26b-6615602d52cc"))) ? (((v) <= (0)) || ((0) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("7dab1cc6-48c1-339d-a085-da6df8d910f6"))) ? (((v) == (0)) && ((0) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("6f4b6e16-d93c-3d6e-a1b9-b4818dc9feb3"))) ? ((v < 0) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("b49d2a3e-5da0-327c-9380-ab151c0e9647"))) ? ((v < 0) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("48448296-aa6e-3be0-8796-f7984e378dec"))) ? (((v) > (0)) && ((v) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("c28536ca-3f86-3a3d-9d68-5cd95f636f77"))) ? (((v) != (0)) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("656aa615-5a51-3482-bf8e-344c9b1b8a14"))) ? (((v) >= (0)) && ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("03fe0ef0-981a-370e-844b-df28b56fd8e9"))) ? ((v) < (getKVBufSize())) : (((KnobRuntime.check(java.util.UUID.fromString("8ff92c73-4e09-32a7-bf02-5005b06884e0"))) ? (((v) < (0)) || ((0) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7849f993-841f-3d00-adf8-a49cfb403b87"))) ? ((v < 0) && ((0) == (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("235cd99e-3fdc-3ec0-8044-12d0e3f05830"))) ? (((v) > (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("239f18d0-1585-3933-b2cc-20381219db79"))) ? (((v) < (0)) && ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("bbff8fb2-d5e1-36a7-8b16-68197a51a1d1"))) ? ((v < 0) || ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("234bd3a1-2545-3199-8935-0c113374ba46"))) ? (((v) >= (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("3a4864c7-97c1-3472-8e7c-fe7a19b203c0"))) ? (((v) > (0)) && ((0) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ddc0ff76-4839-3056-98a8-ed88a4cc6664"))) ? ((v < 0) || ((v) < (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("5031a8e6-45e4-3cbd-9920-0329372e73af"))) ? (((v) >= (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("243b7cb8-b672-36be-818a-e198a18babb8"))) ? (((v) >= (0)) && ((0) <= (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("ad4b85ea-1c47-327f-b070-540d79a00a88"))) ? (((v) > (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("b9ed1de6-081e-36db-a798-c85300c067d6"))) ? ((v < 0) || ((0) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("a03eeef2-853e-3fad-8ae7-dda19c12552e"))) ? (((v) > (0)) || (v > this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("60718540-a5f4-3e70-86df-5fefd883cc70"))) ? (((v) <= (0)) && ((0) <= (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("5615e532-0c73-3715-8259-9e6015f0be10"))) ? (((v) <= (0)) && ((0) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("258086e7-af8a-30a7-a73c-5acf9ec314d1"))) ? (((v) != (0)) || ((v) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f9d4561c-82c0-3881-be7d-c4b3095b2113"))) ? ((v < 0) || ((0) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("e8cc88d4-510e-3ca2-a7c0-b3e79430348d"))) ? (((v) >= (0)) || ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9cd8eb6f-e29a-32d6-b232-6471fa5dfda3"))) ? (((v) < (0)) && ((v) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("26d39a77-1687-37f6-b6e2-59f841620c68"))) ? (((v) == (0)) || ((0) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("41b208e7-7fb8-3980-b78c-9adf8ed46640"))) ? (((v) <= (0)) && ((0) > (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("2dc1c308-bd6c-33e5-9c84-20a1636410df"))) ? ((v < 0) || ((v) < (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("ec9a0deb-8a0b-3c0f-8320-67186f5bd7d9"))) ? (((v) >= (0)) && ((v) > (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("a2af6e1e-5d76-3e36-9c96-1493cdab6e58"))) ? (((v) == (0)) || ((v) != (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("3a1a714e-e66f-3fbe-a48c-1ae2008c74b3"))) ? ((0) > (this.blockBuffer.limit())) : (((KnobRuntime.check(java.util.UUID.fromString("f477ae47-a805-3d8e-b244-d18a60b16258"))) ? ((v) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fa179e5f-a51c-3e3b-ba8a-70f7cc193331"))) ? (((v) <= (0)) && ((v) == (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("750c69ec-57b5-3e6e-b116-6712323d5027"))) ? (((v) >= (0)) || ((0) == (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e745acfa-5524-3ead-8dc9-cc3d1a3704d1"))) ? (((v) != (0)) || ((v) < (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("dafe078f-c78c-3dd0-bfbf-5fbeddb35dba"))) ? ((0) != (this.blockBuffer.position())) : (((KnobRuntime.check(java.util.UUID.fromString("84696f3c-2a85-3c24-b3c3-001c3acba00c"))) ? (((v) >= (0)) || ((v) >= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("b5433de1-e878-36c4-a57f-e1031f74167f"))) ? (((v) <= (0)) || ((v) != (this.blockBuffer.position()))) : (((KnobRuntime.check(java.util.UUID.fromString("3f40eec6-1f79-3307-9097-4a2f8e63b977"))) ? (((v) >= (0)) && ((v) <= (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c5e5498d-f686-3ce5-9ecc-3d84b2c52936"))) ? (((v) != (0)) && ((v) > (this.blockBuffer.limit()))) : (((KnobRuntime.check(java.util.UUID.fromString("585d77b7-3dc1-3b78-9846-a1a5983c1e65"))) ? (((v) != (0)) && ((0) != (getKVBufSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("a8da2926-b2d0-304a-a1a0-e7bf8a888440"))) ? (((v) <= (0)) || ((0) == (this.blockBuffer.position()))) : (v < 0 || v > this.blockBuffer.limit()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
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
if(KnobRuntime.check(java.util.UUID.fromString("c025fae8-6464-3abb-acb5-ef1cca2e795a"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bdf65dd6-dcc1-3ef8-b1ff-b01f9dba5213"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf7fad91-092b-3ae7-a82e-f10266d753bf"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f22ee17b-d9c9-3240-bab4-c2f5ff684b9e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ec4b3f97-15c1-3866-93f5-e599e1a119e9"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e0d7976-0526-35bd-86ca-88db6a637b7d"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8e6c74af-613d-3e39-990c-bdbbb28e1212"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3fa78deb-0e03-32d3-8007-875914b86b1a"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f2bd7e2-2a73-3954-b043-5d98f54989cd"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64079e26-bcea-3b72-ba4e-094e6e6dfa9b"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e4ad727-c546-3d11-a5c4-1465d74a979b"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37cb5be6-a241-3c2d-89df-6c232b41d3d3"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7622d020-5770-3e44-bb26-4e0ca98b768c"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("99bc9ad4-bc43-32f8-8997-e907578219c3"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c599ccf2-1455-3ad8-8ba9-8b24d02d8bf5"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a46f05b2-add6-3a8e-86cb-dc4e4687ddd8"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3360ea08-783d-3d74-bb8e-73a7b130b5c9"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43a255cf-e694-39c9-b794-540f7ef313a0"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a36edeb7-ed65-37d7-b221-868333048f9a"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be74a471-07ac-3c1b-b1a6-6f68858af8b9"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d6440fd-e030-3be4-b3e9-a016a9c33501"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10385095-2f49-3bdd-a3d2-1164f87673f5"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7566e0d3-d024-3329-a9ad-84fba47eb5c3"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b5b5cdb-b6be-35c8-9f9e-586de0d2b960"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a80e532f-a1fd-3887-8615-f9ea967659fc"))) {
throw new java.io.IOException("Injected exception");
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
if(KnobRuntime.check(java.util.UUID.fromString("8a2f3c58-b9ec-30c9-912d-8f5138dc52af"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bec2f078-dd00-31cc-aedd-d6fa8a39472e"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66d3cf3d-457d-35fd-bffa-7061586d2289"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3e9873d-e298-3976-8f7b-17668dbf1248"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b4d88d0a-1104-37a2-ac0f-e45769a67eaf"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e14e925d-5709-3be8-b333-1b151bc190c9"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0caeb7ba-f7ee-3618-b474-094e9b18e155"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("846a4f6e-e687-38f2-98c0-ba9b05fad61e"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aed28cf6-a04a-3485-92f0-08cac37a5188"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3905c423-edc8-321d-888e-1627efe16464"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b0ed6b1-db28-34e9-a392-da8292e836ae"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5cf2a159-3095-32a6-b4d9-5fabcfd61c49"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b62b7cd4-2d20-3722-80ed-cd7482daef0a"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2fb5f49c-1010-3086-8182-e85271e18e7b"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8813ddde-4d36-3e71-ae3c-e296918fac46"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f1c7f95d-dab5-3620-8748-5cbd42fd8059"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1611b289-d4c0-3b3a-8be8-1e04e38728f2"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25e6b2cd-c1e3-32c4-a73b-7ef47f1af527"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7e11111-0a33-3ded-b8ad-fe2a86656f78"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51160279-f873-3d44-afd4-091a8d46ad2f"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f775427-21f6-3751-86a9-0d898b8240d7"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("935cc244-32be-3539-9269-177b28ed1033"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3cb341ee-d768-3ee9-9686-9be39c691d6c"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("e7db91e4-2404-39c7-a72b-f21d4915b73d"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf3cc3ad-1b83-3e1b-b977-d061d25c0bcc"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd8c2d09-dbe9-32cc-88a6-3ad9a39867cc"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dde3c098-caab-3820-8cf8-ffedc846df21"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b4b3741b-e0ce-397d-978f-d025e021297e"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a673dfab-ffa0-330c-9b29-e4d9a244b457"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b00ced9c-3adb-3b73-b24f-56decdde62f7"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba7fef00-60cf-3d02-bcfe-e10ce3b0e474"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28f080e0-1c90-3b41-bce7-234b08940739"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f45c1a3-e591-3776-8f5e-6341f68c5379"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e65fb2af-3af4-315a-a45e-42fdeed57ca7"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f745e095-dca3-30ee-b4ad-f757e5e9e9fb"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5696d54-4510-3672-b68e-a1e19aae4df9"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("391b54dc-ded4-339c-ac7b-8198aa14b73c"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37bd69af-9972-3b21-8b48-0ac51112db67"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e7cdba6-d0aa-38fc-804c-c88cb06a7d0b"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11087717-6d92-3e80-97a7-c8a56b2d2977"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9eba4c28-505a-34ca-a51c-ea016a311e97"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b75b768e-7b24-3c53-aa6f-ef41dce422dc"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc77dc0a-b46e-35f4-a8d8-e2841127d9d5"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69cd2d36-7658-3019-8ee6-17bc2199ea9f"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfe99797-47ac-3386-8408-c47d4b6bed50"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e10c947-6127-3656-b41f-599b50d98335"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a28fce2b-e756-3cf3-bd39-4e7aa8f5118b"))) {
try {
    java.lang.reflect.Field field = curBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(curBlock));
    field.set(curBlock, oldValue + 1);
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
      if (((KnobRuntime.check(java.util.UUID.fromString("84f97cef-754c-3c99-a125-d620a78b8b51"))) ? (isSeeked()) : (keyBuff.hasArray()))) {
        return new KeyValue.KeyOnlyKeyValue(keyBuff.array(),
          keyBuff.arrayOffset() + keyBuff.position(), klen);
      } else {
        return new ByteBufferKeyOnlyKeyValue(keyBuff, keyBuff.position(), klen);
      }
    }

    @Override
    public String getKeyString() {
if(KnobRuntime.check(java.util.UUID.fromString("3b60d0e9-9514-36fa-9272-2549be027b23"))) {
return null;
}
      return CellUtil.toString(getKey(), false);
    }

    @Override
    public String getValueString() {
      return ByteBufferUtils.toStringBinary(getValue());
    }

    public int compareKey(CellComparator comparator, Cell key) {
if(KnobRuntime.check(java.util.UUID.fromString("167e46da-8297-3d0a-a351-df44393155fb"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f33152f8-6317-31bf-8299-2e2ff4a99852"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ba6500f-4b6a-3929-aca2-96d4c31898fc"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cbc2a7b8-97ed-37c6-a8a3-249fcc06f7dc"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b337957-c030-39f5-b8d3-a29f776402d5"))) {
try {
    java.lang.reflect.Field field = pair.getClass().getDeclaredField("second");
    field.setAccessible(true);
    int oldValue = ((int)field.get(pair));
    field.set(pair, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("9f608708-b1be-35d2-b009-4feeb2c28672"))) { blockBuffer.asSubByteBuffer(KEY_VALUE_LEN_SIZE, currKeyLen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("40ac6b49-2578-31b8-bda9-a71d5a59dda6"))) { blockBuffer.asSubByteBuffer((blockBuffer.position()) + (KEY_VALUE_LEN_SIZE), currKeyLen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("54f6f4dd-fca5-3043-a754-b52615b4766d"))) { blockBuffer.asSubByteBuffer(blockBuffer.position(), currKeyLen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("4894d825-c4be-3b27-96fb-860b38232565"))) { blockBuffer.asSubByteBuffer((blockBuffer.limit()) + (KEY_VALUE_LEN_SIZE), currKeyLen, pair); } else if (KnobRuntime.check(java.util.UUID.fromString("ff20dcbe-2147-30b4-8453-55519f5fa0e1"))) { blockBuffer.asSubByteBuffer(blockBuffer.limit(), currKeyLen, pair); } else { blockBuffer.asSubByteBuffer(blockBuffer.position() + KEY_VALUE_LEN_SIZE, currKeyLen, pair); }
      if (KnobRuntime.check(java.util.UUID.fromString("dbc4ab04-807b-3c7a-a54a-105c578bca99"))) { this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), getKVBufSize(), currKeyLen, rowLen); } else if (KnobRuntime.check(java.util.UUID.fromString("d58bd34f-af84-3de4-a609-784d0af915f3"))) { this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), getCurCellSerializedSize(), currKeyLen, rowLen); } else { this.bufBackedKeyOnlyKv.setKey(pair.getFirst(), pair.getSecond(), currKeyLen, rowLen); }
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
if(KnobRuntime.check(java.util.UUID.fromString("3bd8bd6c-6ce1-38ae-8b9f-eedceb5dd062"))) {
return null;
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("fa43878b-971e-3ba7-8a8a-683009dcbb57"))) ? ((cache) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2dc7002d-7b3f-3eb4-b190-9f96f8d3297a"))) ? ((cache) == (null)) : (cache != null))))) {
      HFileBlock cachedBlock =
        (HFileBlock) cache.getBlock(cacheKey, cacheBlock, useLock, updateCacheMetrics);
      if (((KnobRuntime.check(java.util.UUID.fromString("3bcd0628-cfa0-3984-a814-bb1fbdc0eff4"))) ? ((cachedBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d7aafb14-f170-3bd3-96b1-7022378d4192"))) ? ((cachedBlock) == (null)) : (cachedBlock != null))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("07cc3518-32bf-3a96-ad5f-69ec3ff7dffd"))) ? (cacheConf.shouldCacheBlockOnRead(cachedBlock.getBlockType().getCategory())) : (cacheConf.shouldCacheCompressed(cachedBlock.getBlockType().getCategory())))) {
          HFileBlock compressedBlock = cachedBlock;
if(KnobRuntime.check(java.util.UUID.fromString("23a05d5c-39a4-3b82-86d3-34ff9d3bc46a"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11371868-dddb-3eb8-8867-aea678b93387"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87ff6135-d3df-347c-a86b-0451feb849c4"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hfileContext));
    field.set(hfileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("19aa1270-b92f-3538-bd42-afc8bf31dffa"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95e957d0-9cb2-3fa8-91cd-6b959089834d"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("usesHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(hfileContext);
    field.set(hfileContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("928a35b3-19c2-3813-ad67-c039f0d22a9f"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5bbcc0e-50a7-3069-add8-1790cfdf69e1"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5ced8265-c44c-3f2d-866d-c0e99484b121"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hfileContext));
    field.set(hfileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef23e9e1-85f6-378f-9c3b-ecd4c3b846f2"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2969371a-d4c7-3ddd-9039-2f5a6fd7912e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d6c7559d-c14e-347e-9508-d82623b66866"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11df4717-1e74-3d2c-a907-d0503df59b7d"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          cachedBlock = compressedBlock.unpack(hfileContext, fsBlockReader);
          // In case of compressed block after unpacking we can release the compressed block
          if (((KnobRuntime.check(java.util.UUID.fromString("72001a09-39ca-3afd-8c2f-e29e01a4192c"))) ? ((compressedBlock) != (cachedBlock)) : (((KnobRuntime.check(java.util.UUID.fromString("e32c020e-1a01-300b-b1f7-bbe714d0e9d6"))) ? ((compressedBlock) == (cachedBlock)) : (compressedBlock != cachedBlock))))) {
            if (KnobRuntime.check(java.util.UUID.fromString("86629268-4d1d-304d-802b-75ce4190c08e"))) { isFileInfoLoaded(); } else { compressedBlock.release(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("0dcbc092-bf76-33a9-ad32-44b552c56194"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c738240e-60fe-3f90-b9d9-d83a7a535b54"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd957b3e-d17f-3947-9fae-9b01c2517404"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("132f9492-b6ee-3d12-a5dd-d4b1da77b0af"))) { isFileInfoLoaded(); } else { block.release(); }
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
        assert ((KnobRuntime.check(java.util.UUID.fromString("6d365af4-bf24-33fd-8e2c-1abb9f79b9b9"))) ? (isFileInfoLoaded()) : (cachedBlock.isUnpacked())) : "Packed block leak.";
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

          if (((KnobRuntime.check(java.util.UUID.fromString("5dba8f6f-5cbb-38dc-90f3-8dd5cf1e7d0d"))) ? (((!useLock) && (!cacheable)) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("52cb1d4d-cec1-3ac4-ab9a-7ba9453d03ce"))) ? ((!useLock && cacheable) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("bca44bb9-5747-3b14-98f4-20f01dab9577"))) ? (!useLock && cacheable) : (((KnobRuntime.check(java.util.UUID.fromString("42701119-fce4-3d63-a757-71c11193a084"))) ? ((cacheable) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("6c749b75-f241-345a-ae2a-f33d80f9ac0b"))) ? ((!useLock && cacheable) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("9a1a96d7-e146-3fcd-b7b2-8622c5cd5645"))) ? (((!useLock) && (!cacheable)) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("477353e4-c596-3b38-bd03-f7759b7d941a"))) ? (((!useLock) && (cacheable)) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("12916261-9c99-3dfd-b21e-50d7a050b414"))) ? (((!useLock) || (!cacheable)) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("34ce8fba-3c9b-3161-82e5-c6a151dbb4fa"))) ? (!cacheable) : (((KnobRuntime.check(java.util.UUID.fromString("d3dd7323-7eea-37dd-adc9-e5a94ee313e9"))) ? (((!useLock) && (cacheable)) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("07c587c4-7fbb-3692-81c5-a3a20a12e5e7"))) ? ((!useLock) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("0ba3c12c-b31e-3581-9095-2414e516dc80"))) ? ((!cacheable) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("4f36158d-e5f1-3217-a0bd-ded0a84ed804"))) ? ((cacheable) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("8c600595-381f-3dc3-a864-500b112e6956"))) ? ((!useLock) && (cacheable)) : (((KnobRuntime.check(java.util.UUID.fromString("2357e441-8841-339c-910b-d8554f9b558e"))) ? ((cacheable) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("74fbec6d-f381-3fa5-8c24-9789d97dceaa"))) ? (((!useLock) && (!cacheable)) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("c7420cb7-2d85-31bd-8ab1-149e5515302b"))) ? (cacheConf.shouldReadBlockFromCache(expectedBlockType)) : (((KnobRuntime.check(java.util.UUID.fromString("aa0a6a23-9e09-39ea-93d0-c2447d3b5878"))) ? (((!useLock) || (cacheable)) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("378a4e9e-c930-3a80-a66c-90589d7654ca"))) ? ((!useLock) || (cacheable)) : (((KnobRuntime.check(java.util.UUID.fromString("9266d20e-1f35-3c76-a019-131ae14e26eb"))) ? ((!useLock) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("ab768c5c-1bbd-3494-af38-32ca8485766c"))) ? (cacheConf.shouldLockOnCacheMiss(expectedBlockType)) : (((KnobRuntime.check(java.util.UUID.fromString("d882445b-d92e-3ed1-a312-dd33ab858968"))) ? (((!useLock) || (!cacheable)) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("853330d2-f5db-3297-aada-52ef07d4aca1"))) ? (!useLock) : (((KnobRuntime.check(java.util.UUID.fromString("575c1b6d-e10e-3637-9a4c-9c951cf119a1"))) ? (((!useLock) || (cacheable)) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("a690710c-a080-32d3-be0f-b015532faedc"))) ? (cacheable) : (((KnobRuntime.check(java.util.UUID.fromString("20fa6669-6c6f-349a-b478-6ccdd7a3be75"))) ? ((!cacheable) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("e5779408-6e68-3afb-83ed-c312c3239c48"))) ? (((!useLock) && (cacheable)) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("7343002d-105b-31e0-82cd-36226d112efd"))) ? ((!useLock && cacheable) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("212232b0-01a1-3efc-807d-7125fc69db19"))) ? ((cacheable) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("dcf74a74-dd93-3230-8a61-218803b78cd1"))) ? (((!useLock) || (!cacheable)) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("fef5f936-0dd1-3081-9dbd-47d3b3055492"))) ? ((!cacheable) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("ab579718-e699-358b-934f-108d9cd86563"))) ? (((!useLock) || (cacheable)) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("abf7b5de-e3e4-3a8f-b6f2-8bbe417e03eb"))) ? ((!cacheable) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("9c47a803-c3f1-339c-bff0-c362f4d1f559"))) ? (((!useLock) && (!cacheable)) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("a4946239-b595-3b3e-8ea8-f0933925b67e"))) ? ((!useLock) || (!cacheable)) : (((KnobRuntime.check(java.util.UUID.fromString("ae9a44aa-ecab-33ee-9daf-6c9e502fae05"))) ? (((!useLock) && (cacheable)) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("5db4e1a9-0a5e-3370-b7bd-5869e49cbdb7"))) ? ((!useLock) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("873031f8-46e6-3cbd-909f-df477c58781f"))) ? ((!useLock) && (!cacheable)) : (((KnobRuntime.check(java.util.UUID.fromString("54df71aa-66e0-3d24-9757-213b613ecf2f"))) ? (((!useLock) || (!cacheable)) || (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("4dba1b3e-6857-3fda-923d-236854296e54"))) ? (((!useLock) || (cacheable)) || (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("74a25c15-a1b2-38d2-815e-953c2da43a43"))) ? ((!useLock) && (cacheConf.shouldLockOnCacheMiss(expectedBlockType))) : (((KnobRuntime.check(java.util.UUID.fromString("3ee08e73-bb7b-300d-978a-daa6328f39e2"))) ? ((!useLock && cacheable) && (cacheConf.shouldReadBlockFromCache(expectedBlockType))) : (!useLock && cacheable && cacheConf.shouldLockOnCacheMiss(expectedBlockType)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
            // check cache again with lock
            useLock = true;
            continue;
          }
          // Carry on, please load.
        }

        span.addEvent("block cache miss", attributes);
        // Load block from filesystem.
if(KnobRuntime.check(java.util.UUID.fromString("4bd66494-f263-31fc-b7e2-0881388a813f"))) {
onDiskBlockSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("96a8848d-7a98-3179-851c-d600600c24d2"))) {
onDiskBlockSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0c4fe6ab-7187-3bc6-91ea-dd6098943d30"))) {
pread = !pread;
}
if(KnobRuntime.check(java.util.UUID.fromString("bd09c36c-b253-3502-bf65-bc906471b42b"))) {
dataBlockOffset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6e89b83f-2594-3997-9f2e-a51b75c50931"))) {
onDiskBlockSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7fd3819f-39eb-3150-bdf3-cf693a8fefca"))) {
pread = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("dcea8ea6-7c36-3fe6-ab29-1147c8f98b5b"))) {
onDiskBlockSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e02460da-bdd8-3a1c-ba12-4013b44c6ec0"))) {
dataBlockOffset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f54c890-178b-3e85-bc41-c5a3cc5b93b8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8772b45f-183d-3254-b5dd-ac49110bec3e"))) {
pread = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("21857d02-1b90-319a-8a54-6a862d21a404"))) {
dataBlockOffset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("65fea5a6-daac-3cdf-a359-86a7e379977a"))) {
dataBlockOffset -= 1;
}
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
        if (((KnobRuntime.check(java.util.UUID.fromString("1cbe0a25-0457-3ad6-937f-f9605abff23c"))) ? ((cacheOnly && cacheCompressed) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("3decc493-4eea-3322-82c1-5ce6f7310ac0"))) ? (!cacheOnly) : (((KnobRuntime.check(java.util.UUID.fromString("634b828c-521d-357d-97a5-11e0364b2e0b"))) ? (((!cacheOnly) && (!cacheCompressed)) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("29076673-3461-324d-9e05-6a338c20205d"))) ? (((!cacheOnly) && (cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("d06b0975-ad3c-36e0-a15e-b58ab7c99e5f"))) ? (((!cacheOnly) || (!cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("f36e4f74-3753-3aad-af2a-c64ec8b6cf00"))) ? ((cacheOnly) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("087a580a-cd82-38ad-a0fc-ab5ba7b2827e"))) ? (((cacheOnly) && (cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("a94a0c43-d439-32b8-8101-eaa423d295c4"))) ? ((cacheCompressed) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("3a08ec36-1ff0-30c2-a752-e484d60241ad"))) ? (((cacheOnly) || (!cacheCompressed)) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("a13029cf-450b-38eb-8bfc-351412820d32"))) ? ((cacheOnly) && (!cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("2c1f2d7e-eadf-3be3-92ef-c6ad806a2db7"))) ? ((!cacheOnly) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("6e14e9b9-1bd4-32d4-a080-ad0eec2a664f"))) ? (((!cacheOnly) || (!cacheCompressed)) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("f9f665be-8669-3ff8-b433-2d23e5d76324"))) ? (cacheOnly) : (((KnobRuntime.check(java.util.UUID.fromString("ef54396a-4638-3ebe-aa36-38b9f766adb9"))) ? ((!cacheOnly) || (cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("920d1a90-b1e3-3574-9216-ed185fe39cc4"))) ? (((!cacheOnly) && (!cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("973cf008-e0b9-3da4-bf9c-1a828bf78ace"))) ? ((!cacheCompressed) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("5526e06b-f55b-3ee5-bc30-778f9b7c7a9d"))) ? ((!cacheCompressed) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("d0cafd98-0e93-302a-a7c1-37ab4fc7f48c"))) ? (((!cacheOnly) && (cacheCompressed)) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("016c2ef0-2e65-3a74-bf18-ffe4ef89a078"))) ? (cacheCompressed) : (((KnobRuntime.check(java.util.UUID.fromString("62d2da91-eabf-3003-818f-9e47f406bd79"))) ? ((cacheOnly) || (!cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("ff690d01-6122-31e3-9ece-f5e25201dac1"))) ? (!cacheOnRead) : (((KnobRuntime.check(java.util.UUID.fromString("b4992bf3-5fb2-3499-aecc-00898bdcf084"))) ? (((!cacheOnly) && (!cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("6100bb36-a1a5-378a-8dcc-b4942107c746"))) ? ((cacheCompressed) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("a6520c2f-420d-3a3a-a01a-75e25a9a3e63"))) ? (((!cacheOnly) || (!cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("3667edf6-7c3f-3f1a-804f-9f7b81947161"))) ? (!cacheCompressed) : (((KnobRuntime.check(java.util.UUID.fromString("8b38f622-95d8-30fd-a7eb-218944013878"))) ? ((!cacheCompressed) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("2939cef9-a38c-33f7-a41d-682a3344f324"))) ? (((!cacheOnly) && (cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("38d16fa6-a499-3d7b-8d96-cb03477e5217"))) ? (((cacheOnly) && (!cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("d10ada63-d826-3bf4-8bad-3836d161d23c"))) ? (((!cacheOnly) && (!cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("1f008a1d-f6bb-35e1-83df-34fe395db490"))) ? (((!cacheOnly) || (cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("23042dd7-388c-3790-b926-991716963c3b"))) ? ((!cacheOnly) || (!cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("c87d509a-95ca-369d-9a00-155965a4a4e7"))) ? (((cacheOnly) && (!cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("44fe4da2-ce14-3038-9aaa-c3d8fbc0f9d2"))) ? ((cacheOnly) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("f40fdc2d-52b9-3f13-a511-ad31d094eda1"))) ? (((cacheOnly) || (cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("d8d2df7d-1ade-3eb3-927e-662e6853b3f4"))) ? ((cacheOnly) || (cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("326d6068-7368-3ab6-88ec-8ed8d00f36b1"))) ? (((!cacheOnly) || (cacheCompressed)) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("ea46239e-0b78-34fe-a78c-8b550cbe7e5a"))) ? ((cacheOnly && cacheCompressed) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("85f81a5e-8c5d-3f0f-962b-79985915ea7c"))) ? (((cacheOnly) || (!cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("d2f46bf4-95ef-3ca5-ba79-614956caec5f"))) ? ((cacheOnly) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("e06d6f22-2d60-3923-8f92-a351b48a749d"))) ? (cacheOnRead) : (((KnobRuntime.check(java.util.UUID.fromString("bc75a122-44a6-3855-b552-7b155633a195"))) ? ((!cacheOnly) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("4c0f7e4c-ca88-3ef2-86b3-975b7ca7449e"))) ? (((cacheOnly) && (!cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("47624ce3-e05b-336a-8b6b-d83e9b8b4715"))) ? ((cacheOnly && cacheCompressed) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("488f336e-18d9-394f-bb4d-0435aaf867ff"))) ? (((!cacheOnly) || (cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("d5992434-63cd-31a7-b275-aaa3c478c098"))) ? ((!cacheCompressed) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("8a9646b7-940e-38a5-9df1-8ccb1ad7cf63"))) ? ((cacheCompressed) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("b70f0319-a1de-39f9-9d1f-25d07e61ee4a"))) ? ((cacheCompressed) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("e612f4d6-8536-3995-b189-b98e3dbdbefe"))) ? (cacheOnly && cacheCompressed) : (((KnobRuntime.check(java.util.UUID.fromString("03c735a5-3008-3c10-b66d-810efc2c5427"))) ? ((!cacheOnly) && (!cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("db51253b-c624-3e36-8b4d-18641fc5a372"))) ? (((!cacheOnly) || (!cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("20bbe526-10d7-3cf1-a068-84d2fbb7d0dd"))) ? ((cacheOnly) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("559a8243-2e4a-3fc2-81ef-27a7385a6340"))) ? (((cacheOnly) && (cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("4145a828-aa95-331c-8852-203229e6b547"))) ? ((cacheOnly) && (cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("5e4e08c6-4c9e-33c0-9b70-bbdd119aa096"))) ? ((!cacheOnly) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("bf9c035f-35d9-3fe2-8a65-8b2d80e279a2"))) ? (((!cacheOnly) && (cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("1d9644fc-fced-3f1d-a25b-6aa2e93b0d86"))) ? (((cacheOnly) || (!cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("9c13ea2a-cbc2-3962-8c16-9a4a4a8016ca"))) ? (((cacheOnly) || (cacheCompressed)) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("fdd2b027-7caf-33ea-a022-c132a9a727f0"))) ? (((cacheOnly) || (cacheCompressed)) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("bf86ed1f-b959-3a5a-8b55-4e79057d284f"))) ? (((cacheOnly) && (cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("1fc2db6b-ff59-3b88-bb1f-0313451362a9"))) ? (((cacheOnly) || (!cacheCompressed)) || (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("f3b147ab-a153-3abb-b19b-f37f977358c3"))) ? ((!cacheOnly) && (cacheCompressed)) : (((KnobRuntime.check(java.util.UUID.fromString("04a4da8e-c6c4-3952-bff1-833ef9faf9a6"))) ? (((!cacheOnly) || (cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("1d243e7d-358c-3097-ac42-630fc25296ce"))) ? ((cacheOnly && cacheCompressed) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("b93dd8d9-7569-39a1-8f87-7e7ddaa6b6ea"))) ? (((cacheOnly) || (cacheCompressed)) && (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("242f2d5c-a59b-3f06-9926-42493cbfa3dc"))) ? ((!cacheOnly) || (cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("80ee117b-dc00-3df3-9977-17f8aa1e69e3"))) ? (((cacheOnly) && (cacheCompressed)) && (!cacheOnRead)) : (((KnobRuntime.check(java.util.UUID.fromString("31e608b0-7326-3372-a43f-789eaa77afe0"))) ? (((cacheOnly) && (!cacheCompressed)) && (!cacheOnRead)) : (cacheOnly && cacheCompressed && cacheOnRead))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          cacheConf.getBlockCache().ifPresent(cache -> {
if(KnobRuntime.check(java.util.UUID.fromString("15f7a46f-df81-332f-93ca-97f7b5e510aa"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de8f0d18-f153-3080-89b2-5406a5b7b902"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
if(KnobRuntime.check(java.util.UUID.fromString("00facae5-ea80-39c0-89ec-37db30a877f3"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b729bcd-4a7f-3314-863b-520f60860eb8"))) {
try {
    java.lang.reflect.Field field = cacheKey.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheKey));
    field.set(cacheKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            cache.cacheBlock(cacheKey, cacheCompressed ? hfileBlock : unpacked,
              cacheConf.isInMemory(), cacheOnly);
          }
        });
        if (unpacked != hfileBlock) {
          // End of life here if hfileBlock is an independent block.
          if (KnobRuntime.check(java.util.UUID.fromString("406ed1d0-ca87-3e0b-961a-0699808a11d2"))) { isFileInfoLoaded(); } else { hfileBlock.release(); }
        }
        if (updateCacheMetrics && hfileBlock.getBlockType().isData()) {
          HFile.DATABLOCK_READ_COUNT.increment();
        }

        return unpacked;
      }
    } finally {
      if (lockEntry != null) {
if(KnobRuntime.check(java.util.UUID.fromString("3b63c0dc-a4a3-358e-a651-ae360d2cafa0"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("id");
    field.setAccessible(true);
    long oldValue = ((long)field.get(lockEntry));
    field.set(lockEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa06f2e5-edd6-3d82-8b78-94e49d88ef4d"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("26cc8ddf-7961-3d42-8eef-c2f40f80c4fe"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("354f97ab-5b0a-32f5-acfe-0534ffc6006f"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("47373b60-02c6-3a18-a24e-047276dd6fd5"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("numWaiters");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lockEntry));
    field.set(lockEntry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fef3d599-4801-3d8a-8bfd-93f00be7bc67"))) {
try {
    java.lang.reflect.Field field = lockEntry.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(lockEntry);
    field.set(lockEntry, !oldValue);
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
if(KnobRuntime.check(java.util.UUID.fromString("41db2a2b-7b8e-3173-a641-02232f181392"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d727191-ce37-3b9c-859e-797c3962b0dd"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("9d2c44b4-d975-3351-be1a-eb283faf2f5b"))) ? ((fileInfo.shouldIncludeMemStoreTS()) && (isFileInfoLoaded())) : (((KnobRuntime.check(java.util.UUID.fromString("0ec2c482-f727-38f2-9725-66b8decdb65f"))) ? (fileInfo.shouldIncludeMemStoreTS()) : (((KnobRuntime.check(java.util.UUID.fromString("8b7f628f-21cb-34e0-b3fc-c9fc86ea3ab9"))) ? ((fileInfo.shouldIncludeMemStoreTS()) || (isFileInfoLoaded())) : (((KnobRuntime.check(java.util.UUID.fromString("3577791c-3afd-3223-9b8b-cc34cc49cc78"))) ? ((isFileInfoLoaded()) && (isFileInfoLoaded())) : (((KnobRuntime.check(java.util.UUID.fromString("18e2d93d-2cc9-34e1-a376-6880ac8157bf"))) ? ((isFileInfoLoaded()) || (fileInfo.isDecodeMemstoreTS())) : (((KnobRuntime.check(java.util.UUID.fromString("6c0d8670-6c5f-326c-ba5e-02effbd7c20f"))) ? (isFileInfoLoaded()) : (((KnobRuntime.check(java.util.UUID.fromString("ab7533b6-d8c9-310a-bbb0-6b27a8be3492"))) ? ((fileInfo.shouldIncludeMemStoreTS()) && (fileInfo.isDecodeMemstoreTS())) : (((KnobRuntime.check(java.util.UUID.fromString("bc15eba9-563f-3155-b0dc-72adcdf3ebd5"))) ? ((fileInfo.shouldIncludeMemStoreTS()) || (fileInfo.isDecodeMemstoreTS())) : (((KnobRuntime.check(java.util.UUID.fromString("88a9e90c-b593-378e-baae-49e1315c2b0d"))) ? ((isFileInfoLoaded()) || (isFileInfoLoaded())) : (((KnobRuntime.check(java.util.UUID.fromString("2aeb2e6b-e0cb-3659-bb7c-60d178ebfdcd"))) ? ((isFileInfoLoaded()) && (fileInfo.isDecodeMemstoreTS())) : (((KnobRuntime.check(java.util.UUID.fromString("dd28d5ce-e8fb-33a7-baa8-fd9ed992a443"))) ? (fileInfo.isDecodeMemstoreTS()) : (fileInfo.shouldIncludeMemStoreTS() && fileInfo.isDecodeMemstoreTS()))))))))))))))))))))));
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
    if (((KnobRuntime.check(java.util.UUID.fromString("7a57138f-171f-3c14-8132-1fbac4240721"))) ? ((actualBlockType) != (expectedBlockType)) : (((KnobRuntime.check(java.util.UUID.fromString("e3b8b557-5970-3436-8b1e-a7b37c985602"))) ? ((actualBlockType) == (expectedBlockType)) : (actualBlockType != expectedBlockType))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b76daaa-7546-3644-a56a-13ad06080d6d"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("7c9f9072-2e3c-3114-95c7-79b4b144ac6d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a7080daa-9107-3f31-a62a-437e609f5fcb"))) {
throw new java.io.IOException("Injected exception");
}
    return ((KnobRuntime.check(java.util.UUID.fromString("f8288edd-c1fb-33b1-b4c5-d9d4672b1338"))) ? (Optional.of(dataBlockIndexReader.midkey(this))) : (Optional.ofNullable(dataBlockIndexReader.midkey(this))));
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
if(KnobRuntime.check(java.util.UUID.fromString("03c90071-8fd3-3354-8a92-50d43e4a0a8a"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9646007-4b82-3848-a0db-129a7a11a8be"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("19e05938-93ea-3a7e-9b94-877b8c1bcf03"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("91bded7a-c001-3633-b556-248fc22df653"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b33a2186-e365-3011-b780-ee0703ea3368"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed10c26a-b9ee-37c3-8afd-4ecea407011e"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("159a40c3-f357-3def-9e24-daa83f204eb4"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49fb82eb-5b34-34d2-9190-9b18878e97f0"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e22a7a25-cc67-3b53-8796-353adf3937ca"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc6be9eb-24a2-3e9f-aa66-f0773f3d72be"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42ffb2c6-5cd3-332a-8245-02f7fa868bb8"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4767089a-af89-3ad8-acb9-d44c79876600"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3750af8-dbe4-3f99-b575-9568b4575140"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb5d74f0-39ea-307b-aa5f-5f12a6ea447a"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64d6c6a7-bcb2-3e5a-9be4-97c6467a17ec"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58775e12-f931-3af5-98ef-ce6e4073980e"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d14ad1e6-e5f0-3adb-8e22-721e8adcfc0c"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de05aaa2-64c1-3c6c-b520-4d675e040b6a"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b4d409a-cf5b-32d1-a5fd-8ec2667329f7"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84b605bf-8bc1-3882-a422-e3e0957085de"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("910aa916-dc2f-36e8-8e7d-ae299cc8bbaa"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61ffcfce-5fc0-3a2e-9e33-e9ca7000d656"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(newBlock));
    field.set(newBlock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f22f3f85-007c-367f-a5c4-6472a78cb8e5"))) {
try {
    java.lang.reflect.Field field = newBlock.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(newBlock));
    field.set(newBlock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("5726333d-5d6b-3cb5-9741-4178ecd5e3ec"))) { releaseIfNotCurBlock(newBlock); } else if (KnobRuntime.check(java.util.UUID.fromString("7f2c56b2-e161-388f-ab44-4d604b5d2f85"))) { updateCurrentBlock(newBlock); } else { updateCurrBlockRef(newBlock); }
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
