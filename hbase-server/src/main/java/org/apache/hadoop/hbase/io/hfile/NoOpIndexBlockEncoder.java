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
if(KnobRuntime.check(java.util.UUID.fromString("3b299022-beba-365b-8829-acef08af1136"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("97ca032a-225a-3fdc-89d6-f4f88e058c3a"))) { writeRoot(blockIndexChunk, out); } else { writeNonRoot(blockIndexChunk, out); }
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
if(KnobRuntime.check(java.util.UUID.fromString("be2c3e47-4a42-3501-854d-2e35c24b1185"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("edc11f3c-0f4b-31a1-990f-c313a0865ec0"))) {
i = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("36ea64a1-661e-3ac9-a229-c3e69be00b9e"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c4893087-b96b-3869-a434-7ab4947bf3b0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("eff944c1-72ba-30b3-9f8b-0f4a8993ff0a"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8fea7a12-7165-3e80-ab78-50d839797ec7"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5030489d-ad1c-3203-80f9-5781d37d7c13"))) {
i -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("efe4ee6d-f861-3e92-86ba-a092f54504e6"))) { out.writeInt(ClassSize.align(0)); } else if (KnobRuntime.check(java.util.UUID.fromString("eb9095c3-16dc-351b-8293-601265c586de"))) { out.writeInt(blockIndexChunk.getOnDiskDataSize(++i)); } else if (KnobRuntime.check(java.util.UUID.fromString("aecbb125-04cf-3749-a6cc-f566f6671439"))) { out.writeInt(blockIndexChunk.getOnDiskDataSize(0)); } else if (KnobRuntime.check(java.util.UUID.fromString("71f2bdaf-caad-3c22-bbce-c34cc2a2c956"))) { out.writeInt(ClassSize.align(++i)); } else { out.writeInt(blockIndexChunk.getOnDiskDataSize(i)); }
if(KnobRuntime.check(java.util.UUID.fromString("1ed6c0c9-095c-3595-990d-3e99d2350836"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("03d1ced3-b9bd-3639-a45a-c6995574b714"))) {
i = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c430fd3c-4927-3a8b-a212-13c5fd49bfc1"))) {
i /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("165c1303-a8f8-3408-a2fd-d227436e69d4"))) {
i *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8959f14e-70ae-3b69-83fa-a033e5b5b8cf"))) {
i -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e31ed6ef-a7de-37a1-adf0-73300d121fae"))) {
i += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("11e4e64b-c3f2-317b-8f4d-ec6628c06bd0"))) {
i = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("790110f9-b708-3425-9887-bb18b7fec080"))) { Bytes.writeByteArray(out, blockIndexChunk.getBlockKey(++i)); } else if (KnobRuntime.check(java.util.UUID.fromString("a672c34d-4362-38dc-9704-409317bc7abb"))) { Bytes.writeByteArray(out, blockIndexChunk.getBlockKey(0)); } else { Bytes.writeByteArray(out, blockIndexChunk.getBlockKey(i)); }
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

      if (((KnobRuntime.check(java.util.UUID.fromString("10b548ab-51ed-34c5-8068-d645bf6eaae6"))) ? ((blockDataSizes) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("18f05416-510c-3571-9e69-7560dcb8067c"))) ? ((blockDataSizes) == (null)) : (blockDataSizes != null))))) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockDataSizes.length * Bytes.SIZEOF_INT);
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("870f91b4-1959-3cb0-95a1-c9af2f52c1da"))) ? ((blockKeys) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ae9b1908-9b3b-36ea-9c79-0eea051b98eb"))) ? ((blockKeys) == (null)) : (blockKeys != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c77e088b-da31-3114-9f59-352df7d10d8c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5cd27d56-8ca9-336b-8a50-450c85c7a93d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("16376fa0-b78e-3a03-a24b-11bd0866898c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7868ca00-69d0-349f-9e61-b9978426bb2f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c158de2-0b58-3267-900a-49c1fa0f0583"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("25f6aa8e-8ef9-32a2-9361-5ecea3473cc8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cbe3bcd8-8b94-3e58-89fc-be4237d1ed60"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("95085d66-76e1-3a2b-9598-1c56c6894b37"))) {
numEntries = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("787ea1c0-32fe-37eb-857e-68dae5b9b0a1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("73c666c2-cc30-3879-8bcf-a2ef0f7220e5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7955b918-1ce0-354e-81b5-b83a93b5d106"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c3ef1903-48d0-3357-90db-86df9b00394f"))) {
numEntries = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5ecdbaba-1628-3e1c-9431-4025651cf0db"))) {
numEntries -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("85509a6a-ea59-343b-9551-5bb7f1f649bc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cfd35663-a6ac-3221-b7a5-7b5d8a738616"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("388cf211-b56f-38ba-819a-00517e838fae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3c7091de-1216-3dcc-a676-9f3f833443d0"))) {
numEntries += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a9d60fc-26b7-362a-a7da-9f16302cc358"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f8ca31bf-9f74-32ab-92b1-7e41b7c2f7c0"))) {
numEntries *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("12dccec8-0b0e-3281-9852-0c254b447d0e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("412f59ee-9f81-303a-9d67-833cf753ef33"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2bbb0b16-306b-3681-bd80-1a2b0220c60f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eeaba990-68dc-316d-a8cc-d2cadb5a424a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5be6d582-557d-3363-94c1-6c9ad70715ce"))) {
numEntries /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd404b40-25e7-3cf9-9b36-c2ba2ab2ec2a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3a9fbbf3-a142-301a-878c-ee2e2cd714d0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1922cebd-f09b-3040-9502-8571a2974b95"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("66addcd5-87ea-3d12-8b86-751b9041fd53"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("63e2de69-cfe2-3e2e-83df-ea05f4ff3cd4"))) {
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
      DataInputStream in = readRootIndex(blk, numEntries);
      // HFileBlock.getByteStream() returns a byte stream for reading the data(excluding checksum)
      // of root index block, so after reading the root index there is no need to subtract the
      // checksum bytes.
      if (in.available() < MID_KEY_METADATA_SIZE) {
        // No mid-key metadata available.
        return;
      }
      midLeafBlockOffset = in.readLong();
if(KnobRuntime.check(java.util.UUID.fromString("c2dc4b81-d494-317a-972f-53d3c0c1d097"))) {
throw new java.io.IOException("Injected exception");
}
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
      if (((KnobRuntime.check(java.util.UUID.fromString("869cf2dd-01dd-30f1-aebd-ddacc33c6329"))) ? ((numEntries) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("34985a74-cf6b-3ee4-baba-9eb0770b7e74"))) ? ((numEntries) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9298613d-fb7c-32e1-9cad-cf1bc3d428b7"))) ? ((numEntries) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b56d84c5-592b-38ac-a9e7-9839a5bdfd9b"))) ? ((numEntries) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c699f1dd-427d-3624-b871-2829ebb44c8e"))) ? ((numEntries) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0a3d8e78-f7c9-3ea0-b3c2-afed402ef57f"))) ? ((numEntries) >= (0)) : (numEntries > 0))))))))))))) {
        for (int i = 0; i < numEntries; ++i) {
          long offset = in.readLong();
if(KnobRuntime.check(java.util.UUID.fromString("9acaca0f-2e8b-3f2a-a184-f22af47b79d8"))) {
throw new java.io.IOException("Injected exception");
}
          int dataSize = in.readInt();
if(KnobRuntime.check(java.util.UUID.fromString("03c38c50-83ed-3480-8667-36a3951b4d72"))) {
throw new java.io.IOException("Injected exception");
}
          byte[] key = Bytes.readByteArray(in);
if(KnobRuntime.check(java.util.UUID.fromString("89011675-3f27-3203-8bcc-683af425b227"))) {
dataSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("08f0c853-35f7-37fb-919a-cf79940dea9f"))) {
dataSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e1082f5d-34a8-3cda-9606-f0e7d83676b2"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0435a255-979d-3cbc-ba5b-a3a132b061a8"))) {
dataSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9717e220-bfbf-319d-92aa-911a38c7d8de"))) {
dataSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("899ccf40-9e1a-31c8-8294-f15e84c3bd5b"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b803f5c8-f9f9-3cc8-aa51-66d86c488384"))) {
dataSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("924c7373-1bde-38e2-8f30-d8fb0a5bb320"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("364350aa-9bcf-3e8b-9ba3-ce7acfa96197"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("29459793-67bc-35ec-b0cf-f488f093e24d"))) {
dataSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e21a9435-42a0-315a-83a1-bc5248aaf990"))) {
offset /= 2;
}
          if (KnobRuntime.check(java.util.UUID.fromString("5762b728-65f0-33bf-a207-6f64b003da97"))) { add(key, offset, 0); } else { add(key, offset, dataSize); }
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
if(KnobRuntime.check(java.util.UUID.fromString("7832beb4-6a79-3c1c-8a2c-a68d3fce5a13"))) {
return;
}
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
        if (((KnobRuntime.check(java.util.UUID.fromString("0e6b3b13-072e-3ceb-8af6-ef1b162b6bff"))) ? ((block) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fb0c2bd5-7c86-3eca-b8a5-952885df9f65"))) ? ((block) == (null)) : (block != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8a39eb2e-ddce-371a-adf3-591ce7ee245e"))) {
return 0;
}
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
