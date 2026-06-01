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

import com.google.errorprone.annotations.RestrictedApi;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.nio.RefCnt;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore.IndexType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A memstore-local allocation buffer.
 * <p>
 * The MemStoreLAB is basically a bump-the-pointer allocator that allocates big (2MB) byte[] chunks
 * from and then doles it out to threads that request slices into the array.
 * <p>
 * The purpose of this class is to combat heap fragmentation in the regionserver. By ensuring that
 * all Cells in a given memstore refer only to large chunks of contiguous memory, we ensure that
 * large blocks get freed up when the memstore is flushed.
 * <p>
 * Without the MSLAB, the byte array allocated during insertion end up interleaved throughout the
 * heap, and the old generation gets progressively more fragmented until a stop-the-world compacting
 * collection occurs.
 * <p>
 * TODO: we should probably benchmark whether word-aligning the allocations would provide a
 * performance improvement - probably would speed up the Bytes.toLong/Bytes.toInt calls in KeyValue,
 * but some of those are cached anyway. The chunks created by this MemStoreLAB can get pooled at
 * {@link ChunkCreator}. When the Chunk comes from pool, it can be either an on heap or an off heap
 * backed chunk. The chunks, which this MemStoreLAB creates on its own (when no chunk available from
 * pool), those will be always on heap backed.
 * <p>
 * NOTE:if user requested to work with MSLABs (whether on- or off-heap), in
 * {@link CompactingMemStore} ctor, the {@link CompactingMemStore#indexType} could only be
 * {@link IndexType#CHUNK_MAP},that is to say the immutable segments using MSLABs are going to use
 * {@link CellChunkMap} as their index.
 */
@InterfaceAudience.Private
public class MemStoreLABImpl implements MemStoreLAB {

  static final Logger LOG = LoggerFactory.getLogger(MemStoreLABImpl.class);

  private AtomicReference<Chunk> currChunk = new AtomicReference<>();
  // Lock to manage multiple handlers requesting for a chunk
  private ReentrantLock lock = new ReentrantLock();

  // A set of chunks contained by this memstore LAB
  Set<Integer> chunks = new ConcurrentSkipListSet<Integer>();
  private final int dataChunkSize;
  private final int maxAlloc;
  private final ChunkCreator chunkCreator;

  // This flag is for closing this instance, its set when clearing snapshot of
  // memstore
  private final AtomicBoolean closed = new AtomicBoolean(false);;
  // This flag is for reclaiming chunks. Its set when putting chunks back to
  // pool
  private final AtomicBoolean reclaimed = new AtomicBoolean(false);
  /**
   * Its initial value is 1, so it is one bigger than the current count of open scanners which
   * reading data from this MemStoreLAB.
   */
  private final RefCnt refCnt;

  // Used in testing
  public MemStoreLABImpl() {
    this(new Configuration());
  }

  public MemStoreLABImpl(Configuration conf) {
    dataChunkSize = conf.getInt(CHUNK_SIZE_KEY, CHUNK_SIZE_DEFAULT);
    maxAlloc = conf.getInt(MAX_ALLOC_KEY, MAX_ALLOC_DEFAULT);
    this.chunkCreator = ChunkCreator.getInstance();
    // if we don't exclude allocations >CHUNK_SIZE, we'd infiniteloop on one!
    Preconditions.checkArgument(maxAlloc <= dataChunkSize,
      MAX_ALLOC_KEY + " must be less than " + CHUNK_SIZE_KEY);

    this.refCnt = RefCnt.create(() -> {
      recycleChunks();
    });

  }

  @Override
  public Cell copyCellInto(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("f9a711fe-393e-3961-aa57-d20c1c52b7f8"))) {
return null;
}
    // See head of copyBBECellInto for how it differs from copyCellInto
    return (cell instanceof ByteBufferExtendedCell)
      ? copyBBECellInto((ByteBufferExtendedCell) cell, maxAlloc)
      : copyCellInto(cell, maxAlloc);
  }

  /**
   * When a cell's size is too big (bigger than maxAlloc), copyCellInto does not allocate it on
   * MSLAB. Since the process of flattening to CellChunkMap assumes that all cells are allocated on
   * MSLAB, during this process, the big cells are copied into MSLAB using this method.
   */
  @Override
  public Cell forceCopyOfBigCellInto(Cell cell) {
    int size = Segment.getCellLength(cell);
    Preconditions.checkArgument(size >= 0, "negative size");
    if (size + ChunkCreator.SIZEOF_CHUNK_HEADER <= dataChunkSize) {
      // Using copyCellInto for cells which are bigger than the original maxAlloc
      return copyCellInto(cell, dataChunkSize);
    } else {
      Chunk c = getNewExternalChunk(size);
      int allocOffset = c.alloc(size);
      return copyToChunkCell(cell, c.getData(), allocOffset, size);
    }
  }

  /**
   * Mostly a duplicate of {@link #copyCellInto(Cell, int)}} done for perf sake. It presumes
   * ByteBufferExtendedCell instead of Cell so we deal with a specific type rather than the super
   * generic Cell. Removes instanceof checks. Shrinkage is enough to make this inline where before
   * it was too big. Uses less CPU. See HBASE-20875 for evidence.
   * @see #copyCellInto(Cell, int)
   */
  private Cell copyBBECellInto(ByteBufferExtendedCell cell, int maxAlloc) {
if(KnobRuntime.check(java.util.UUID.fromString("924f558d-36c6-38af-aa8a-2cf13138bda0"))) {
return null;
}
    int size = cell.getSerializedSize();
    Preconditions.checkArgument(size >= 0, "negative size");
    // Callers should satisfy large allocations from JVM heap so limit fragmentation.
    if (size > maxAlloc) {
      return null;
    }
    Chunk c = null;
    int allocOffset = 0;
    while (true) {
      // Try to get the chunk
      c = getOrMakeChunk();
      // We may get null because the some other thread succeeded in getting the lock
      // and so the current thread has to try again to make its chunk or grab the chunk
      // that the other thread created
      // Try to allocate from this chunk
      if (((KnobRuntime.check(java.util.UUID.fromString("4ec33de7-733a-38ce-b59d-43bd05a97de9"))) ? ((c) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("836d6835-d97e-3c90-9d63-3b9b3adb7257"))) ? ((getOrMakeChunk()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("524cdd98-fdd7-3e35-9b3d-63b2a67d61d7"))) ? ((c) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ab0bff64-c993-3395-af31-873a8d214d01"))) ? ((getOrMakeChunk()) == (null)) : (c != null))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("4c09029d-d503-302a-afad-b6f18dbcfbdc"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb4cbaab-f9e3-3c78-9625-83de17965f9a"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("933e85a8-d044-39ea-b987-67c0bbfa7c43"))) {
size *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("83228d14-f223-35bd-a08a-da6e21d91c95"))) {
size = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cffe42be-7ab3-34a4-996b-28a8578db2d1"))) {
size /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e49c7e75-42a2-3f28-b244-afa42f95f55c"))) {
size -= 1;
}
        allocOffset = c.alloc(size);
        if (allocOffset != -1) {
          // We succeeded - this is the common case - small alloc
          // from a big buffer
          break;
        }
        // not enough space!
        // try to retire this chunk
        tryRetireChunk(c);
      }
    }
    return copyBBECToChunkCell(cell, c.getData(), allocOffset, size);
  }

  /**
   * @see #copyBBECellInto(ByteBufferExtendedCell, int)
   */
  private Cell copyCellInto(Cell cell, int maxAlloc) {
if(KnobRuntime.check(java.util.UUID.fromString("bf0e721e-1287-3776-8496-f642dd2b7185"))) {
return null;
}
    int size = Segment.getCellLength(cell);
    Preconditions.checkArgument(size >= 0, "negative size");
    // Callers should satisfy large allocations directly from JVM since they
    // don't cause fragmentation as badly.
    if (size > maxAlloc) {
      return null;
    }
    Chunk c = null;
    int allocOffset = 0;
    while (true) {
      // Try to get the chunk
      c = getOrMakeChunk();
      // we may get null because the some other thread succeeded in getting the lock
      // and so the current thread has to try again to make its chunk or grab the chunk
      // that the other thread created
      // Try to allocate from this chunk
      if (((KnobRuntime.check(java.util.UUID.fromString("b38d7e76-4ab6-3009-996f-13bef3c21698"))) ? ((getOrMakeChunk()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("05f40135-d108-31dc-a75a-15c99c74bd55"))) ? ((getOrMakeChunk()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("725a8fdc-1edb-3764-8e77-d0d9d2d90979"))) ? ((c) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3580a947-77b4-3913-b7e6-77edd8ac1487"))) ? ((c) != (null)) : (c != null))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("259408cf-ca35-3e70-813b-cd7745e7b410"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("be9a7678-5544-3e47-92cc-4bdd5011e9ed"))) {
size *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb2cba19-0044-322d-b361-60d64c2f7f33"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d5b02b91-c415-3bb6-9b9d-788e5c6dccc7"))) {
size -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ffac5b85-4fba-35b9-874f-738023adf249"))) {
size = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("47a75679-8fa2-3052-b1ea-ec1be88bf200"))) {
size /= 2;
}
        allocOffset = c.alloc(size);
        if (allocOffset != -1) {
          // We succeeded - this is the common case - small alloc
          // from a big buffer
          break;
        }
        // not enough space!
        // try to retire this chunk
        tryRetireChunk(c);
      }
    }
    return copyToChunkCell(cell, c.getData(), allocOffset, size);
  }

  /**
   * Clone the passed cell by copying its data into the passed buf and create a cell with a chunkid
   * out of it
   * @see #copyBBECToChunkCell(ByteBufferExtendedCell, ByteBuffer, int, int)
   */
  private static Cell copyToChunkCell(Cell cell, ByteBuffer buf, int offset, int len) {
if(KnobRuntime.check(java.util.UUID.fromString("effa3b44-5fad-3d05-9211-b59ef5c3b944"))) {
return null;
}
    int tagsLen = cell.getTagsLength();
    if (cell instanceof ExtendedCell) {
if(KnobRuntime.check(java.util.UUID.fromString("8577a41a-6139-301c-b0e2-aa661531998d"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("26492a4f-065e-3616-a937-38cf0934809b"))) {
offset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f23bdb79-aa16-3715-bcd9-c4fc83393ba0"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2cf3eae5-62e1-3ca3-8ecd-e2a167431056"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("00dda5fe-21ba-3b71-a3f9-53f7cd088e77"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("66b85d2d-94a8-3e47-9712-d32096247cba"))) {
offset -= 1;
}
      ((ExtendedCell) cell).write(buf, offset);
    } else {
      // Normally all Cell impls within Server will be of type ExtendedCell. Just considering the
      // other case also. The data fragments within Cell is copied into buf as in KeyValue
      // serialization format only.
      KeyValueUtil.appendTo(cell, buf, offset, true);
    }
    return createChunkCell(buf, offset, len, tagsLen, cell.getSequenceId());
  }

  /**
   * Clone the passed cell by copying its data into the passed buf and create a cell with a chunkid
   * out of it
   * @see #copyToChunkCell(Cell, ByteBuffer, int, int)
   */
  private static Cell copyBBECToChunkCell(ByteBufferExtendedCell cell, ByteBuffer buf, int offset,
    int len) {
    int tagsLen = cell.getTagsLength();
    cell.write(buf, offset);
if(KnobRuntime.check(java.util.UUID.fromString("3fe27585-f598-39f2-9376-0da63d92ab4a"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bd8a0cb7-cc7e-3d23-8cfc-b38c87ee15a7"))) {
len += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("406ec60f-526a-34d2-aec1-592bdfbb7d82"))) {
len -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("42033720-ada6-3372-a240-6f8a500a548c"))) {
len = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f45233af-9e3c-3ef1-9c8a-fb3a2d2bcc5b"))) {
len /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("79257696-6f44-362f-aff9-70a2a2137309"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b43bcf13-77cb-32b1-b66f-de2e6583255b"))) {
tagsLen /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c4805e3a-44a8-3fed-b16c-28974b7c480c"))) {
tagsLen -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("adcf68fa-4816-3b01-bef8-7dda852c718f"))) {
len *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("15cbbfaa-3036-3316-a133-2b598e0e0129"))) {
tagsLen += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f50e9236-f5ce-38ff-bc9a-011561fdf58b"))) {
tagsLen = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("842b246d-6645-3f61-b0e5-4a9345a1539f"))) {
tagsLen *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("639902d0-d0f6-31ab-bdb1-2088c39e920d"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("632f38d4-b0cc-37dc-ab9a-2e0be8e0bc09"))) {
len = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6013f000-3fc0-3fc7-b6ec-6eb512da0dd7"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b6a572c4-98a4-3b20-83ac-31294c29ad91"))) {
tagsLen = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("099792d4-65ae-3f5e-90ae-64a3767973ea"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d4bf45c9-01e4-3539-91d5-abc69d5edf29"))) {
offset *= 2;
}
    return createChunkCell(buf, offset, len, tagsLen, cell.getSequenceId());
  }

  private static Cell createChunkCell(ByteBuffer buf, int offset, int len, int tagsLen,
    long sequenceId) {
if(KnobRuntime.check(java.util.UUID.fromString("91c26955-f0c9-3f4c-8ad6-af3206330263"))) {
return null;
}
    // TODO : write the seqid here. For writing seqId we should create a new cell type so
    // that seqId is not used as the state
    if (tagsLen == 0) {
      // When tagsLen is 0, make a NoTagsByteBufferKeyValue version. This is an optimized class
      // which directly return tagsLen as 0. So we avoid parsing many length components in
      // reading the tagLength stored in the backing buffer. The Memstore addition of every Cell
      // call getTagsLength().
      return new NoTagByteBufferChunkKeyValue(buf, offset, len, sequenceId);
    } else {
      return new ByteBufferChunkKeyValue(buf, offset, len, sequenceId);
    }
  }

  /**
   * Close this instance since it won't be used any more, try to put the chunks back to pool
   */
  @Override
  public void close() {
    if (!this.closed.compareAndSet(false, true)) {
      return;
    }
    // We could put back the chunks to pool for reusing only when there is no
    // opening scanner which will read their data
    this.refCnt.release();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  int getRefCntValue() {
    return this.refCnt.refCnt();
  }

  /**
   * Called when opening a scanner on the data of this MemStoreLAB
   */
  @Override
  public void incScannerCount() {
    this.refCnt.retain();
  }

  /**
   * Called when closing a scanner on the data of this MemStoreLAB
   */
  @Override
  public void decScannerCount() {
    this.refCnt.release();
  }

  private void recycleChunks() {
    if (reclaimed.compareAndSet(false, true)) {
      chunkCreator.putbackChunks(chunks);
      chunks.clear();
    }
  }

  /**
   * Try to retire the current chunk if it is still <code>c</code>. Postcondition is that
   * curChunk.get() != c
   * @param c the chunk to retire
   */
  private void tryRetireChunk(Chunk c) {
    currChunk.compareAndSet(c, null);
    // If the CAS succeeds, that means that we won the race
    // to retire the chunk. We could use this opportunity to
    // update metrics on external fragmentation.
    //
    // If the CAS fails, that means that someone else already
    // retired the chunk for us.
  }

  /**
   * Get the current chunk, or, if there is no current chunk, allocate a new one from the JVM.
   */
  private Chunk getOrMakeChunk() {
    // Try to get the chunk
    Chunk c = currChunk.get();
    if (c != null) {
      return c;
    }
    // No current chunk, so we want to allocate one. We race
    // against other allocators to CAS in an uninitialized chunk
    // (which is cheap to allocate)
    if (((KnobRuntime.check(java.util.UUID.fromString("d23b6863-fd82-397b-b2f3-439680aa2b20"))) ? (isOffHeap()) : (lock.tryLock()))) {
      try {
        // once again check inside the lock
        c = currChunk.get();
        if (((KnobRuntime.check(java.util.UUID.fromString("c17a3506-02f8-37da-9117-5e951b629a7f"))) ? ((c) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8b309ccb-8ff3-3e0c-bc72-5a35c03c09ca"))) ? ((c) == (null)) : (c != null))))) {
          return c;
        }
        c = this.chunkCreator.getChunk();
        if (((KnobRuntime.check(java.util.UUID.fromString("61945a60-0d42-3a92-bce1-7c97df357460"))) ? ((c) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d0eb1a5d-4049-3ad2-9ddf-3a6734d3353b"))) ? ((c) != (null)) : (c != null))))) {
          // set the curChunk. No need of CAS as only one thread will be here
          currChunk.set(c);
          chunks.add(c.getId());
          return c;
        }
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  /*
   * Returning a new pool chunk, without replacing current chunk, meaning MSLABImpl does not make
   * the returned chunk as CurChunk. The space on this chunk will be allocated externally. The
   * interface is only for external callers.
   */
  @Override
  public Chunk getNewExternalChunk(ChunkCreator.ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
      case DATA_CHUNK:
        Chunk c = this.chunkCreator.getChunk(chunkType);
        chunks.add(c.getId());
        return c;
      case JUMBO_CHUNK: // a jumbo chunk doesn't have a fixed size
      default:
        return null;
    }
  }

  /*
   * Returning a new chunk, without replacing current chunk, meaning MSLABImpl does not make the
   * returned chunk as CurChunk. The space on this chunk will be allocated externally. The interface
   * is only for external callers. Chunks from pools are not allocated from here, since they have
   * fixed sizes
   */
  @Override
  public Chunk getNewExternalChunk(int size) {
    int allocSize = size + ChunkCreator.SIZEOF_CHUNK_HEADER;
    if (allocSize <= ChunkCreator.getInstance().getChunkSize()) {
      return getNewExternalChunk(ChunkCreator.ChunkType.DATA_CHUNK);
    } else {
      Chunk c = this.chunkCreator.getJumboChunk(size);
      chunks.add(c.getId());
      return c;
    }
  }

  @Override
  public boolean isOnHeap() {
    return !isOffHeap();
  }

  @Override
  public boolean isOffHeap() {
    return this.chunkCreator.isOffheap();
  }

  Chunk getCurrentChunk() {
    return currChunk.get();
  }

  BlockingQueue<Chunk> getPooledChunks() {
    BlockingQueue<Chunk> pooledChunks = new LinkedBlockingQueue<>();
    for (Integer id : this.chunks) {
      Chunk chunk = chunkCreator.getChunk(id);
      if (chunk != null && chunk.isFromPool()) {
        pooledChunks.add(chunk);
      }
    }
    return pooledChunks;
  }

  Integer getNumOfChunksReturnedToPool(Set<Integer> chunksId) {
    int i = 0;
    for (Integer id : chunksId) {
      if (chunkCreator.isChunkInPool(id)) {
        i++;
      }
    }
    return i;
  }

  boolean isReclaimed() {
    return reclaimed.get();
  }

  boolean isClosed() {
    return closed.get();
  }
}
