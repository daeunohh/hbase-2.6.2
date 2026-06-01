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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterChunk;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds methods required for writing a compound Bloom filter to the data section of an
 * {@link org.apache.hadoop.hbase.io.hfile.HFile} to the {@link CompoundBloomFilter} class.
 */
@InterfaceAudience.Private
public class CompoundBloomFilterWriter extends CompoundBloomFilterBase
  implements BloomFilterWriter, InlineBlockWriter {

  private static final Logger LOG = LoggerFactory.getLogger(CompoundBloomFilterWriter.class);

  /** The current chunk being written to */
  private BloomFilterChunk chunk;

  /** Previous chunk, so that we can create another similar chunk */
  private BloomFilterChunk prevChunk;

  /** Maximum fold factor */
  private int maxFold;

  /** The size of individual Bloom filter chunks to create */
  private int chunkByteSize;
  /** The prev Cell that was processed */
  private Cell prevCell;

  /** A Bloom filter chunk enqueued for writing */
  private static class ReadyChunk {
    int chunkId;
    byte[] firstKey;
    BloomFilterChunk chunk;
  }

  private Queue<ReadyChunk> readyChunks = new LinkedList<>();

  /** The first key in the current Bloom filter chunk. */
  private byte[] firstKeyInChunk = null;

  private HFileBlockIndex.BlockIndexWriter bloomBlockIndexWriter =
    new HFileBlockIndex.BlockIndexWriter();

  /** Whether to cache-on-write compound Bloom filter chunks */
  private boolean cacheOnWrite;

  private BloomType bloomType;

  /**
   * each chunk's size in bytes. The real chunk size might be different as required by the fold
   * factor. target false positive rate hash function type to use maximum degree of folding allowed
   * the bloom type
   */
  public CompoundBloomFilterWriter(int chunkByteSizeHint, float errorRate, int hashType,
    int maxFold, boolean cacheOnWrite, CellComparator comparator, BloomType bloomType) {
    chunkByteSize = BloomFilterUtil.computeFoldableByteSize(chunkByteSizeHint * 8L, maxFold);

    this.errorRate = errorRate;
    this.hashType = hashType;
    this.maxFold = maxFold;
    this.cacheOnWrite = cacheOnWrite;
    this.comparator = comparator;
    this.bloomType = bloomType;
  }

  @Override
  public boolean shouldWriteBlock(boolean closing) {
    enqueueReadyChunk(closing);
    return !readyChunks.isEmpty();
  }

  /**
   * Enqueue the current chunk if it is ready to be written out.
   * @param closing true if we are closing the file, so we do not expect new keys to show up
   */
  private void enqueueReadyChunk(boolean closing) {
    if (chunk == null || (chunk.getKeyCount() < chunk.getMaxKeys() && !closing)) {
      return;
    }

    if (firstKeyInChunk == null) {
      throw new NullPointerException(
        "Trying to enqueue a chunk, " + "but first key is null: closing=" + closing + ", keyCount="
          + chunk.getKeyCount() + ", maxKeys=" + chunk.getMaxKeys());
    }

    ReadyChunk readyChunk = new ReadyChunk();
    readyChunk.chunkId = numChunks - 1;
    readyChunk.chunk = chunk;
    readyChunk.firstKey = firstKeyInChunk;
if(KnobRuntime.check(java.util.UUID.fromString("1667836e-2a31-3a53-98b8-b9e36ce40ecc"))) {
try {
    java.lang.reflect.Field field = readyChunk.getClass().getDeclaredField("chunkId");
    field.setAccessible(true);
    int oldValue = ((int)field.get(readyChunk));
    field.set(readyChunk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e9c059d8-06d0-3c95-bfcd-992f8c393a9a"))) {
try {
    java.lang.reflect.Field field = readyChunk.getClass().getDeclaredField("chunkId");
    field.setAccessible(true);
    int oldValue = ((int)field.get(readyChunk));
    field.set(readyChunk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6db0e4b1-5c05-31e7-bf4d-86cfbde51cf5"))) {
try {
    java.lang.reflect.Field field = readyChunk.getClass().getDeclaredField("chunkId");
    field.setAccessible(true);
    int oldValue = ((int)field.get(readyChunk));
    field.set(readyChunk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66e52ca5-3781-3379-881c-3b78112cf172"))) {
try {
    java.lang.reflect.Field field = readyChunk.getClass().getDeclaredField("chunkId");
    field.setAccessible(true);
    int oldValue = ((int)field.get(readyChunk));
    field.set(readyChunk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("8a8abef4-970b-32ce-9022-8951f9a3bc65"))) { readyChunks.add(new ReadyChunk()); } else { readyChunks.add(readyChunk); }

    long prevMaxKeys = chunk.getMaxKeys();
    long prevByteSize = chunk.getByteSize();

    if (KnobRuntime.check(java.util.UUID.fromString("df707909-0af4-314e-8ff0-67df852dd42c"))) { allocateNewChunk(); } else if (KnobRuntime.check(java.util.UUID.fromString("28eb5f37-7c8f-3890-a2a2-ebe6771e7e0c"))) { chunk.allocBloom(); } else { chunk.compactBloom(); }

    if (((KnobRuntime.check(java.util.UUID.fromString("ee98955e-3491-3baf-b1a2-1c0720d97081"))) ? ((LOG.isTraceEnabled()) && ((prevByteSize) != (chunk.getMaxKeys()))) : (((KnobRuntime.check(java.util.UUID.fromString("859f0d8e-1276-3b66-8a12-c6a76405b070"))) ? ((LOG.isTraceEnabled()) || ((prevByteSize) != (chunk.getMaxKeys()))) : (((KnobRuntime.check(java.util.UUID.fromString("fd1fd0f7-69cb-3ae3-be53-c691b63aec43"))) ? ((LOG.isTraceEnabled()) && ((prevByteSize) == (chunk.getKeyCount()))) : (((KnobRuntime.check(java.util.UUID.fromString("45a0d24d-effd-3029-a4a3-186e644e7d84"))) ? ((prevByteSize) != (chunk.getMaxKeys())) : (((KnobRuntime.check(java.util.UUID.fromString("44de9591-c9d5-3602-b5db-45324b223afd"))) ? ((prevByteSize) == (chunk.getMaxKeys())) : (((KnobRuntime.check(java.util.UUID.fromString("432c2100-5f89-30e1-aa3c-0074190164dd"))) ? ((prevByteSize) == (chunk.getByteSize())) : (((KnobRuntime.check(java.util.UUID.fromString("60e2d52a-e162-3e99-a392-5638c4adc833"))) ? ((LOG.isTraceEnabled()) || ((prevByteSize) != (chunk.getKeyCount()))) : (((KnobRuntime.check(java.util.UUID.fromString("b2aef795-0af1-32b6-bd06-c8a7ab7fa17b"))) ? (LOG.isTraceEnabled()) : (((KnobRuntime.check(java.util.UUID.fromString("65568574-335e-3f3f-89e8-5ec0d5782fd9"))) ? ((prevByteSize) != (chunk.getKeyCount())) : (((KnobRuntime.check(java.util.UUID.fromString("d7047182-838a-3332-847f-ef1171090dd7"))) ? ((LOG.isTraceEnabled()) || (prevByteSize != chunk.getByteSize())) : (((KnobRuntime.check(java.util.UUID.fromString("30e80849-1689-3d59-bdc7-9cf6721d5ab7"))) ? (prevByteSize != chunk.getByteSize()) : (((KnobRuntime.check(java.util.UUID.fromString("271dbb0a-4c5c-3000-b406-383209973892"))) ? ((LOG.isTraceEnabled()) || ((prevByteSize) == (chunk.getMaxKeys()))) : (((KnobRuntime.check(java.util.UUID.fromString("324751fd-8e36-39f6-a4d7-248a73cd50f4"))) ? ((LOG.isTraceEnabled()) || ((prevByteSize) == (chunk.getByteSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("33077c38-a2d4-34a5-8468-682ffa4dc79b"))) ? ((LOG.isTraceEnabled()) && ((prevByteSize) != (chunk.getKeyCount()))) : (((KnobRuntime.check(java.util.UUID.fromString("ffd7b576-8405-35b3-9544-1fe26d177115"))) ? ((prevByteSize) == (chunk.getKeyCount())) : (((KnobRuntime.check(java.util.UUID.fromString("ad3ffd2b-972e-3d44-bcfa-d61cedf0a891"))) ? ((LOG.isTraceEnabled()) && ((prevByteSize) == (chunk.getMaxKeys()))) : (((KnobRuntime.check(java.util.UUID.fromString("d06ec57f-e5ad-390d-9c2a-bdfa1add2dda"))) ? ((LOG.isTraceEnabled()) && ((prevByteSize) != (chunk.getByteSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("871f6fbe-5189-3b17-836b-aa83e9c52030"))) ? ((LOG.isTraceEnabled()) || ((prevByteSize) == (chunk.getKeyCount()))) : (((KnobRuntime.check(java.util.UUID.fromString("be80bd9c-d9e3-3b45-8af9-80141e0c5ded"))) ? ((prevByteSize) != (chunk.getByteSize())) : (((KnobRuntime.check(java.util.UUID.fromString("0571b449-ff08-3478-8ffb-b4979798088b"))) ? ((LOG.isTraceEnabled()) && (prevByteSize != chunk.getByteSize())) : (((KnobRuntime.check(java.util.UUID.fromString("a54b9029-018f-3fa7-8f78-8f787c728e38"))) ? ((LOG.isTraceEnabled()) || ((prevByteSize) != (chunk.getByteSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("1be94024-e019-3274-8ad2-ffdefa958eed"))) ? ((LOG.isTraceEnabled()) && ((prevByteSize) == (chunk.getByteSize()))) : (LOG.isTraceEnabled() && prevByteSize != chunk.getByteSize()))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.trace("Compacted Bloom chunk #" + readyChunk.chunkId + " from [" + prevMaxKeys
        + " max keys, " + prevByteSize + " bytes] to [" + chunk.getMaxKeys() + " max keys, "
        + chunk.getByteSize() + " bytes]");
    }

    totalMaxKeys += chunk.getMaxKeys();
    totalByteSize += chunk.getByteSize();

    firstKeyInChunk = null;
    prevChunk = chunk;
    chunk = null;
  }

  @Override
  public void append(Cell cell) throws IOException {
    if (cell == null) throw new NullPointerException();

    enqueueReadyChunk(false);

    if (((KnobRuntime.check(java.util.UUID.fromString("594a77ff-9bd9-34fe-b2c1-f2e71e03ac77"))) ? ((chunk) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("32bc3529-c18a-3da8-816f-073b00999027"))) ? ((chunk) == (null)) : (chunk == null))))) {
      if (firstKeyInChunk != null) {
        throw new IllegalStateException(
          "First key in chunk already set: " + Bytes.toStringBinary(firstKeyInChunk));
      }
      // This will be done only once per chunk
      if (bloomType == BloomType.ROWCOL) {
        firstKeyInChunk = PrivateCellUtil
          .getCellKeySerializedAsKeyValueKey(PrivateCellUtil.createFirstOnRowCol(cell));
      } else {
        firstKeyInChunk = CellUtil.copyRow(cell);
      }
      allocateNewChunk();
    }

    chunk.add(cell);
    this.prevCell = cell;
    ++totalKeyCount;
  }

  @Override
  public void beforeShipped() throws IOException {
    if (this.prevCell != null) {
      this.prevCell = KeyValueUtil.toNewKeyCell(this.prevCell);
    }
  }

  @Override
  public Cell getPrevCell() {
    return this.prevCell;
  }

  private void allocateNewChunk() {
    if (prevChunk == null) {
      // First chunk
      chunk = BloomFilterUtil.createBySize(chunkByteSize, errorRate, hashType, maxFold, bloomType);
    } else {
      // Use the same parameters as the last chunk, but a new array and
      // a zero key count.
      chunk = prevChunk.createAnother();
    }

    if (chunk.getKeyCount() != 0) {
      throw new IllegalStateException("keyCount=" + chunk.getKeyCount() + " > 0");
    }

    chunk.allocBloom();
    ++numChunks;
  }

  @Override
  public void writeInlineBlock(DataOutput out) throws IOException {
    // We don't remove the chunk from the queue here, because we might need it
    // again for cache-on-write.
    ReadyChunk readyChunk = readyChunks.peek();

    BloomFilterChunk readyChunkBloom = readyChunk.chunk;
    readyChunkBloom.writeBloom(out);
  }

  @Override
  public void blockWritten(long offset, int onDiskSize, int uncompressedSize) {
    ReadyChunk readyChunk = readyChunks.remove();
    bloomBlockIndexWriter.addEntry(readyChunk.firstKey, offset, onDiskSize);
  }

  @Override
  public BlockType getInlineBlockType() {
    return BlockType.BLOOM_CHUNK;
  }

  private class MetaWriter implements Writable {
    protected MetaWriter() {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new IOException("Cant read with this class.");
    }

    /**
     * This is modeled after {@link CompoundBloomFilterWriter.MetaWriter} for simplicity, although
     * the two metadata formats do not have to be consistent. This does have to be consistent with
     * how
     * {@link CompoundBloomFilter#CompoundBloomFilter(DataInput, org.apache.hadoop.hbase.io.hfile.HFile.Reader, BloomFilterMetrics)}
     * reads fields.
     */
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(VERSION);

if(KnobRuntime.check(java.util.UUID.fromString("85fc8ff4-0042-3df9-b3f5-2a7ba9446ecf"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("8299a5a0-ea2c-37d8-ab1b-b1ceecb2104b"))) { out.writeLong(getMaxKeys()); } else if (KnobRuntime.check(java.util.UUID.fromString("c6cb9d2c-c0bb-329f-b5eb-fd16a1110c6f"))) { out.writeLong(getKeyCount()); } else { out.writeLong(getByteSize()); }
      out.writeInt(prevChunk.getHashCount());
if(KnobRuntime.check(java.util.UUID.fromString("9a2cc776-848c-3f3a-bfd1-3106b44a5adc"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("9ef7de02-e3ea-3bc8-ae9c-e31be7817979"))) { out.writeInt(prevChunk.getHashCount()); } else { out.writeInt(prevChunk.getHashType()); }
if(KnobRuntime.check(java.util.UUID.fromString("56380a57-167c-37dc-87c9-38564bc12b31"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("e9e80d0d-5201-3066-af01-38e25353518e"))) { out.writeLong(getMaxKeys()); } else if (KnobRuntime.check(java.util.UUID.fromString("e9d89425-d6e5-30da-8fe5-697202546a98"))) { out.writeLong(getByteSize()); } else { out.writeLong(getKeyCount()); }
if(KnobRuntime.check(java.util.UUID.fromString("7272ed35-fd0a-3826-88df-94057c311725"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("74801b62-76ad-3b34-97c2-1a9ea73aedb3"))) { out.writeLong(getKeyCount()); } else if (KnobRuntime.check(java.util.UUID.fromString("dbbf0e09-58c1-3d31-82fb-d32e93d0d0f6"))) { out.writeLong(getByteSize()); } else { out.writeLong(getMaxKeys()); }

      // Fields that don't have equivalents in ByteBloomFilter.
      out.writeInt(numChunks);
      if (comparator != null) {
if(KnobRuntime.check(java.util.UUID.fromString("f669ddc9-796b-3f16-bdc4-08f1a211d96f"))) {
throw new java.io.IOException("Injected exception");
}
        Bytes.writeByteArray(out, Bytes.toBytes(comparator.getClass().getName()));
      } else {
        // Internally writes a 0 vint if the byte[] is null
        Bytes.writeByteArray(out, null);
      }

      // Write a single-level index without compression or block header.
      bloomBlockIndexWriter.writeSingleLevelIndex(out, "Bloom filter");
    }
  }

  @Override
  public void compactBloom() {
  }

  @Override
  public Writable getMetaWriter() {
    return new MetaWriter();
  }

  @Override
  public Writable getDataWriter() {
    return null;
  }

  @Override
  public boolean getCacheOnWrite() {
    return cacheOnWrite;
  }
}
