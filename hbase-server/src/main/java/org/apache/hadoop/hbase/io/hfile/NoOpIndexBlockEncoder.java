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

import static org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.MID_KEY_METADATA_SIZE;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Does not perform any kind of encoding/decoding.
 */
@InterfaceAudience.Private
public class NoOpIndexBlockEncoder implements HFileIndexBlockEncoder {

  public static final NoOpIndexBlockEncoder INSTANCE = new NoOpIndexBlockEncoder();

  /** Cannot be instantiated. Use {@link #INSTANCE} instead. */
  private NoOpIndexBlockEncoder() {
  }

  @Override
  public void saveMetadata(HFile.Writer writer) {
  }

  @Override
  public void encode(BlockIndexChunk blockIndexChunk, boolean rootIndexBlock, DataOutput out)
    throws IOException {
    if (rootIndexBlock) {
      writeRoot(blockIndexChunk, out);
    } else {
      writeNonRoot(blockIndexChunk, out);
    }
  }

  /**
   * Writes the block index chunk in the non-root index block format. This format contains the
   * number of entries, an index of integer offsets for quick binary search on variable-length
   * records, and tuples of block offset, on-disk block size, and the first key for each entry.
   */
  private void writeNonRoot(BlockIndexChunk blockIndexChunk, DataOutput out) throws IOException {
    // The number of entries in the block.
    out.writeInt(blockIndexChunk.getNumEntries());

    if (
      blockIndexChunk.getSecondaryIndexOffsetMarks().size() != blockIndexChunk.getBlockKeys().size()
    ) {
      throw new IOException("Corrupted block index chunk writer: "
        + blockIndexChunk.getBlockKeys().size() + " entries but "
        + blockIndexChunk.getSecondaryIndexOffsetMarks().size() + " secondary index items");
    }

    // For each entry, write a "secondary index" of relative offsets to the
    // entries from the end of the secondary index. This works, because at
    // read time we read the number of entries and know where the secondary
    // index ends.
    for (int currentSecondaryIndex : blockIndexChunk.getSecondaryIndexOffsetMarks())
      out.writeInt(currentSecondaryIndex);

    // We include one other element in the secondary index to calculate the
    // size of each entry more easily by subtracting secondary index elements.
    out.writeInt(blockIndexChunk.getCurTotalNonRootEntrySize());

    for (int i = 0; i < blockIndexChunk.getNumEntries(); ++i) {
      out.writeLong(blockIndexChunk.getBlockOffset(i));
      out.writeInt(blockIndexChunk.getOnDiskDataSize(i));
      out.write(blockIndexChunk.getBlockKey(i));
    }
  }

  /**
   * Writes this chunk into the given output stream in the root block index format. This format is
   * similar to the {@link HFile} version 1 block index format, except that we store on-disk size of
   * the block instead of its uncompressed size.
   * @param out the data output stream to write the block index to. Typically a stream writing into
   *            an {@link HFile} block.
   */
  private void writeRoot(BlockIndexChunk blockIndexChunk, DataOutput out) throws IOException {
    for (int i = 0; i < blockIndexChunk.getNumEntries(); ++i) {
      out.writeLong(blockIndexChunk.getBlockOffset(i));
if(KnobRuntime.check(java.util.UUID.fromString("36ea64a1-661e-3ac9-a229-c3e69be00b9e"))) {
i = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("efe4ee6d-f861-3e92-86ba-a092f54504e6"))) { out.writeInt(ClassSize.align(0)); } else { out.writeInt(blockIndexChunk.getOnDiskDataSize(i)); }
      Bytes.writeByteArray(out, blockIndexChunk.getBlockKey(i));
    }
  }

  @Override
  public IndexBlockEncoding getIndexBlockEncoding() {
    return IndexBlockEncoding.NONE;
  }

  @Override
  public EncodedSeeker createSeeker() {
    return new NoOpEncodedSeeker();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  protected static class NoOpEncodedSeeker implements EncodedSeeker {

    protected long[] blockOffsets;
    protected int[] blockDataSizes;
    protected int rootCount = 0;

    // Mid-key metadata.
    protected long midLeafBlockOffset = -1;
    protected int midLeafBlockOnDiskSize = -1;
    protected int midKeyEntry = -1;

    private Cell[] blockKeys;
    private CellComparator comparator;
    protected int searchTreeLevel;

    /** Pre-computed mid-key */
    private AtomicReference<Cell> midKey = new AtomicReference<>();

    @Override
    public long heapSize() {
      long heapSize = ClassSize.align(ClassSize.OBJECT);

      // Mid-key metadata.
      heapSize += MID_KEY_METADATA_SIZE;

      if (blockOffsets != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockOffsets.length * Bytes.SIZEOF_LONG);
      }

      if (blockDataSizes != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockDataSizes.length * Bytes.SIZEOF_INT);
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("ae9b1908-9b3b-36ea-9c79-0eea051b98eb"))) ? ((blockKeys) == (null)) : (blockKeys != null))) {
        heapSize += ClassSize.REFERENCE;
        // Adding array + references overhead
        heapSize += ClassSize.align(ClassSize.ARRAY + blockKeys.length * ClassSize.REFERENCE);

        // Adding blockKeys
        for (Cell key : blockKeys) {
          heapSize += ClassSize.align(key.heapSize());
        }
      }
      // Add comparator and the midkey atomicreference
      heapSize += 2 * ClassSize.REFERENCE;
      // Add rootCount and searchTreeLevel
      heapSize += 2 * Bytes.SIZEOF_INT;

      return ClassSize.align(heapSize);
    }

    @Override
    public boolean isEmpty() {
      return blockKeys.length == 0;
    }

    @Override
    public Cell getRootBlockKey(int i) {
      return blockKeys[i];
    }

    @Override
    public int getRootBlockCount() {
      return rootCount;
    }

    @Override
    public void initRootIndex(HFileBlock blk, int numEntries, CellComparator comparator,
      int treeLevel) throws IOException {
      this.comparator = comparator;
      this.searchTreeLevel = treeLevel;
      init(blk, numEntries);
    }

    private void init(HFileBlock blk, int numEntries) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("91b64b7a-f6f2-3906-89db-36be2b98ff83"))) {
try {
    java.lang.reflect.Field _knob_field_ = blk.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(blk));
    _knob_field_.set(blk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("156b1382-4bd1-33c1-af0c-8df35e7cfa16"))) {
try {
    java.lang.reflect.Field _knob_field_ = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(blk));
    _knob_field_.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9adaee27-f067-3973-a756-8eb8f7995add"))) {
try {
    java.lang.reflect.Field _knob_field_ = blk.getClass().getDeclaredField("nextBlockOnDiskSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(blk));
    _knob_field_.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ffe9aca2-5e53-3094-938d-5c2f828746fd"))) {
try {
    java.lang.reflect.Field _knob_field_ = blk.getClass().getDeclaredField("totalChecksumBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(blk));
    _knob_field_.set(blk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3bfe7f61-5887-332a-8570-c90f3ee50f6c"))) {
try {
    java.lang.reflect.Field _knob_field_ = blk.getClass().getDeclaredField("totalChecksumBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(blk));
    _knob_field_.set(blk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c158de2-0b58-3267-900a-49c1fa0f0583"))) {
throw new java.io.IOException("Injected exception");
}
      DataInputStream in = readRootIndex(blk, numEntries);
      // HFileBlock.getByteStream() returns a byte stream for reading the data(excluding checksum)
      // of root index block, so after reading the root index there is no need to subtract the
      // checksum bytes.
      if (in.available() < MID_KEY_METADATA_SIZE) {
        // No mid-key metadata available.
        return;
      }
      midLeafBlockOffset = in.readLong();
      midLeafBlockOnDiskSize = in.readInt();
      midKeyEntry = in.readInt();
    }

    private DataInputStream readRootIndex(HFileBlock blk, final int numEntries) throws IOException {
      DataInputStream in = blk.getByteStream();
      readRootIndex(in, numEntries);
      return in;
    }

    private void readRootIndex(DataInput in, final int numEntries) throws IOException {
      blockOffsets = new long[numEntries];
      initialize(numEntries);
      blockDataSizes = new int[numEntries];

      // If index size is zero, no index was written.
      if (numEntries > 0) {
        for (int i = 0; i < numEntries; ++i) {
          long offset = in.readLong();
          int dataSize = in.readInt();
          byte[] key = Bytes.readByteArray(in);
if(KnobRuntime.check(java.util.UUID.fromString("08f0c853-35f7-37fb-919a-cf79940dea9f"))) {
dataSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9717e220-bfbf-319d-92aa-911a38c7d8de"))) {
dataSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("29459793-67bc-35ec-b0cf-f488f093e24d"))) {
dataSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e1082f5d-34a8-3cda-9606-f0e7d83676b2"))) {
offset -= 1;
}
          add(key, offset, dataSize);
        }
      }
    }

    private void initialize(int numEntries) {
if(KnobRuntime.check(java.util.UUID.fromString("8d28ad35-c16b-3ad5-ab1d-3bbc6f41688d"))) {
return;
}
      blockKeys = new Cell[numEntries];
    }

    private void add(final byte[] key, final long offset, final int dataSize) {
      blockOffsets[rootCount] = offset;
      // Create the blockKeys as Cells once when the reader is opened
      blockKeys[rootCount] = new KeyValue.KeyOnlyKeyValue(key, 0, key.length);
      blockDataSizes[rootCount] = dataSize;
      rootCount++;
    }

    @Override
    public Cell midkey(HFile.CachingBlockReader cachingBlockReader) throws IOException {
      if (rootCount == 0) throw new IOException("HFile empty");

      Cell targetMidKey = this.midKey.get();
      if (targetMidKey != null) {
        return targetMidKey;
      }

      if (midLeafBlockOffset >= 0) {
        if (cachingBlockReader == null) {
          throw new IOException(
            "Have to read the middle leaf block but " + "no block reader available");
        }

        // Caching, using pread, assuming this is not a compaction.
        HFileBlock midLeafBlock = cachingBlockReader.readBlock(midLeafBlockOffset,
          midLeafBlockOnDiskSize, true, true, false, true, BlockType.LEAF_INDEX, null);
        try {
          byte[] bytes = HFileBlockIndex.BlockIndexReader
            .getNonRootIndexedKey(midLeafBlock.getBufferWithoutHeader(), midKeyEntry);
          assert bytes != null;
          targetMidKey = new KeyValue.KeyOnlyKeyValue(bytes, 0, bytes.length);
        } finally {
          midLeafBlock.release();
        }
      } else {
        // The middle of the root-level index.
        targetMidKey = blockKeys[rootCount / 2];
      }

      this.midKey.set(targetMidKey);
      return targetMidKey;
    }

    @Override
    public BlockWithScanInfo loadDataBlockWithScanInfo(Cell key, HFileBlock currentBlock,
      boolean cacheBlocks, boolean pread, boolean isCompaction,
      DataBlockEncoding expectedDataBlockEncoding, HFile.CachingBlockReader cachingBlockReader)
      throws IOException {
      int rootLevelIndex = rootBlockContainingKey(key);
      if (rootLevelIndex < 0 || rootLevelIndex >= blockOffsets.length) {
        return null;
      }

      // the next indexed key
      Cell nextIndexedKey = null;

      // Read the next-level (intermediate or leaf) index block.
      long currentOffset = blockOffsets[rootLevelIndex];
      int currentOnDiskSize = blockDataSizes[rootLevelIndex];

      if (rootLevelIndex < blockKeys.length - 1) {
        nextIndexedKey = blockKeys[rootLevelIndex + 1];
      } else {
        nextIndexedKey = KeyValueScanner.NO_NEXT_INDEXED_KEY;
      }

      int lookupLevel = 1; // How many levels deep we are in our lookup.
      int index = -1;

      HFileBlock block = null;
      KeyValue.KeyOnlyKeyValue tmpNextIndexKV = new KeyValue.KeyOnlyKeyValue();
      while (true) {
        try {
          // Must initialize it with null here, because if don't and once an exception happen in
          // readBlock, then we'll release the previous assigned block twice in the finally block.
          // (See HBASE-22422)
          block = null;
          if (currentBlock != null && currentBlock.getOffset() == currentOffset) {
            // Avoid reading the same block again, even with caching turned off.
            // This is crucial for compaction-type workload which might have
            // caching turned off. This is like a one-block cache inside the
            // scanner.
            block = currentBlock;
          } else {
            // Call HFile's caching block reader API. We always cache index
            // blocks, otherwise we might get terrible performance.
            boolean shouldCache = cacheBlocks || (lookupLevel < searchTreeLevel);
            BlockType expectedBlockType;
            if (lookupLevel < searchTreeLevel - 1) {
              expectedBlockType = BlockType.INTERMEDIATE_INDEX;
            } else if (lookupLevel == searchTreeLevel - 1) {
              expectedBlockType = BlockType.LEAF_INDEX;
            } else {
              // this also accounts for ENCODED_DATA
              expectedBlockType = BlockType.DATA;
            }
            block = cachingBlockReader.readBlock(currentOffset, currentOnDiskSize, shouldCache,
              pread, isCompaction, true, expectedBlockType, expectedDataBlockEncoding);
          }

          if (block == null) {
            throw new IOException("Failed to read block at offset " + currentOffset
              + ", onDiskSize=" + currentOnDiskSize);
          }

          // Found a data block, break the loop and check our level in the tree.
          if (block.getBlockType().isData()) {
            break;
          }

          // Not a data block. This must be a leaf-level or intermediate-level
          // index block. We don't allow going deeper than searchTreeLevel.
          if (++lookupLevel > searchTreeLevel) {
            throw new IOException("Search Tree Level overflow: lookupLevel=" + lookupLevel
              + ", searchTreeLevel=" + searchTreeLevel);
          }

          // Locate the entry corresponding to the given key in the non-root
          // (leaf or intermediate-level) index block.
          ByteBuff buffer = block.getBufferWithoutHeader();
          index = HFileBlockIndex.BlockIndexReader.locateNonRootIndexEntry(buffer, key, comparator);
          if (index == -1) {
            // This has to be changed
            // For now change this to key value
            throw new IOException("The key " + CellUtil.getCellKeyAsString(key) + " is before the"
              + " first key of the non-root index block " + block);
          }

          currentOffset = buffer.getLong();
          currentOnDiskSize = buffer.getInt();

          // Only update next indexed key if there is a next indexed key in the current level
          byte[] nonRootIndexedKey =
            HFileBlockIndex.BlockIndexReader.getNonRootIndexedKey(buffer, index + 1);
          if (nonRootIndexedKey != null) {
            tmpNextIndexKV.setKey(nonRootIndexedKey, 0, nonRootIndexedKey.length);
            nextIndexedKey = tmpNextIndexKV;
          }
        } finally {
          if (block != null && !block.getBlockType().isData()) {
            // Release the block immediately if it is not the data block
            block.release();
          }
        }
      }

      if (lookupLevel != searchTreeLevel) {
        assert block.getBlockType().isData();
        // Though we have retrieved a data block we have found an issue
        // in the retrieved data block. Hence returned the block so that
        // the ref count can be decremented
        if (((KnobRuntime.check(java.util.UUID.fromString("fb0c2bd5-7c86-3eca-b8a5-952885df9f65"))) ? ((block) == (null)) : (block != null))) {
          block.release();
        }
        throw new IOException("Reached a data block at level " + lookupLevel
          + " but the number of levels is " + searchTreeLevel);
      }

      // set the next indexed key for the current block.
      return new BlockWithScanInfo(block, nextIndexedKey);
    }

    @Override
    public int rootBlockContainingKey(Cell key) {
      // Here the comparator should not be null as this happens for the root-level block
      int pos = Bytes.binarySearch(blockKeys, key, comparator);
      // pos is between -(blockKeys.length + 1) to blockKeys.length - 1, see
      // binarySearch's javadoc.

      if (pos >= 0) {
        // This means this is an exact match with an element of blockKeys.
        assert pos < blockKeys.length;
        return pos;
      }

      // Otherwise, pos = -(i + 1), where blockKeys[i - 1] < key < blockKeys[i],
      // and i is in [0, blockKeys.length]. We are returning j = i - 1 such that
      // blockKeys[j] <= key < blockKeys[j + 1]. In particular, j = -1 if
      // key < blockKeys[0], meaning the file does not contain the given key.

      int i = -pos - 1;
      assert 0 <= i && i <= blockKeys.length;
      return i - 1;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("size=" + rootCount).append("\n");
      for (int i = 0; i < rootCount; i++) {
        sb.append("key=").append((blockKeys[i])).append("\n  offset=").append(blockOffsets[i])
          .append(", dataSize=" + blockDataSizes[i]).append("\n");
      }
      return sb.toString();
    }
  }
}
