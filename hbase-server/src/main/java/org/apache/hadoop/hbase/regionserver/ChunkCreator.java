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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.HeapMemoryTuneObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Does the management of memstoreLAB chunk creations. A monotonically incrementing id is associated
 * with every chunk
 */
@InterfaceAudience.Private
public class ChunkCreator {
  private static final Logger LOG = LoggerFactory.getLogger(ChunkCreator.class);
  // monotonically increasing chunkid. Starts at 1.
  private AtomicInteger chunkID = new AtomicInteger(1);
  // maps the chunk against the monotonically increasing chunk id. We need to preserve the
  // natural ordering of the key
  // CellChunkMap creation should convert the weak ref to hard reference

  // chunk id of each chunk is the first integer written on each chunk,
  // the header size need to be changed in case chunk id size is changed
  public static final int SIZEOF_CHUNK_HEADER = Bytes.SIZEOF_INT;

  /**
   * Types of chunks, based on their sizes
   */
  public enum ChunkType {
    // An index chunk is a small chunk, allocated from the index chunks pool.
    // Its size is fixed and is 10% of the size of a data chunk.
    INDEX_CHUNK,
    // A data chunk is a regular chunk, allocated from the data chunks pool.
    // Its size is fixed and given as input to the ChunkCreator c'tor.
    DATA_CHUNK,
    // A jumbo chunk isn't allocated from pool. Its size is bigger than the size of a
    // data chunk, and is determined per chunk (meaning, there is no fixed jumbo size).
    JUMBO_CHUNK
  }

  // mapping from chunk IDs to chunks
  private Map<Integer, Chunk> chunkIdMap = new ConcurrentHashMap<Integer, Chunk>();

  private final boolean offheap;
  static ChunkCreator instance;
  static boolean chunkPoolDisabled = false;
  private MemStoreChunkPool dataChunksPool;
  private final int chunkSize;
  private int indexChunkSize;
  private MemStoreChunkPool indexChunksPool;

  ChunkCreator(int chunkSize, boolean offheap, long globalMemStoreSize, float poolSizePercentage,
    float initialCountPercentage, HeapMemoryManager heapMemoryManager,
    float indexChunkSizePercentage) {
    this.offheap = offheap;
    this.chunkSize = chunkSize; // in case pools are not allocated
    initializePools(chunkSize, globalMemStoreSize, poolSizePercentage, indexChunkSizePercentage,
      initialCountPercentage, heapMemoryManager);
  }

  private void initializePools(int chunkSize, long globalMemStoreSize, float poolSizePercentage,
    float indexChunkSizePercentage, float initialCountPercentage,
    HeapMemoryManager heapMemoryManager) {
if(KnobRuntime.check(java.util.UUID.fromString("acf3ccdd-e91a-3641-b68b-80820558f50c"))) {
chunkSize = 0;
}
    this.dataChunksPool = initializePool("data", globalMemStoreSize,
      (1 - indexChunkSizePercentage) * poolSizePercentage, initialCountPercentage, chunkSize,
      ChunkType.DATA_CHUNK, heapMemoryManager);
    // The index chunks pool is needed only when the index type is CCM.
    // Since the pools are not created at all when the index type isn't CCM,
    // we don't need to check it here.
    this.indexChunkSize = (int) (indexChunkSizePercentage * chunkSize);
    this.indexChunksPool =
      initializePool("index", globalMemStoreSize, indexChunkSizePercentage * poolSizePercentage,
        initialCountPercentage, this.indexChunkSize, ChunkType.INDEX_CHUNK, heapMemoryManager);
  }

  /**
   * Initializes the instance of ChunkCreator
   * @param chunkSize              the chunkSize
   * @param offheap                indicates if the chunk is to be created offheap or not
   * @param globalMemStoreSize     the global memstore size
   * @param poolSizePercentage     pool size percentage
   * @param initialCountPercentage the initial count of the chunk pool if any
   * @param heapMemoryManager      the heapmemory manager
   * @return singleton MSLABChunkCreator
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = { "LI_LAZY_INIT_STATIC", "MS_EXPOSE_REP" },
      justification = "Method is called by single thread at the starting of RS")
  public static ChunkCreator initialize(int chunkSize, boolean offheap, long globalMemStoreSize,
    float poolSizePercentage, float initialCountPercentage, HeapMemoryManager heapMemoryManager,
    float indexChunkSizePercent) {
    if (instance != null) {
      return instance;
    }
    instance = new ChunkCreator(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
      initialCountPercentage, heapMemoryManager, indexChunkSizePercent);
    return instance;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MS_EXPOSE_REP",
      justification = "singleton pattern")
  public static ChunkCreator getInstance() {
    return instance;
  }

  /**
   * Creates and inits a data chunk. The default implementation.
   * @return the chunk that was initialized
   */
  Chunk getChunk() {
    return getChunk(ChunkType.DATA_CHUNK);
  }

  /**
   * Creates and inits a chunk with specific type.
   * @return the chunk that was initialized
   */
  Chunk getChunk(ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool == null) {
          if (indexChunkSize <= 0) {
            throw new IllegalArgumentException(
              "chunkType is INDEX_CHUNK but indexChunkSize is:[" + this.indexChunkSize + "]");
          }
          return getChunk(chunkType, indexChunkSize);
        } else {
          return getChunk(chunkType, indexChunksPool.getChunkSize());
        }
      case DATA_CHUNK:
        if (((KnobRuntime.check(java.util.UUID.fromString("80600e08-aca3-30c6-9b9a-f7f9564b95c0"))) ? ((dataChunksPool) == (null)) : (dataChunksPool == null))) {
          return getChunk(chunkType, chunkSize);
        } else {
          return getChunk(chunkType, dataChunksPool.getChunkSize());
        }
      default:
        throw new IllegalArgumentException("chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }
  }

  /**
   * Creates and inits a chunk.
   * @return the chunk that was initialized
   * @param size the size of the chunk to be allocated, in bytes
   */
  Chunk getChunk(ChunkType chunkType, int size) {
    Chunk chunk = null;
    MemStoreChunkPool pool = null;

    // if it is one of the pools
    if (dataChunksPool != null && chunkType == ChunkType.DATA_CHUNK) {
      pool = dataChunksPool;
    } else if (indexChunksPool != null && chunkType == ChunkType.INDEX_CHUNK) {
      pool = indexChunksPool;
    }

    // if we have a pool
    if (pool != null) {
      // the pool creates the chunk internally. The chunk#init() call happens here
      chunk = pool.getChunk();
      // the pool has run out of maxCount
      if (chunk == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("The chunk pool is full. Reached maxCount= " + pool.getMaxCount()
            + ". Creating chunk onheap.");
        }
      }
    }

    if (chunk == null) {
      chunk = createChunk(false, chunkType, size);
    }

    // now we need to actually do the expensive memory allocation step in case of a new chunk,
    // else only the offset is set to the beginning of the chunk to accept allocations
    chunk.init();
    return chunk;
  }

  /**
   * Creates and inits a chunk of a special size, bigger than a regular chunk size. Such a chunk
   * will never come from pool and will always be on demand allocated.
   * @return the chunk that was initialized
   * @param jumboSize the special size to be used
   */
  Chunk getJumboChunk(int jumboSize) {
    int allocSize = jumboSize + SIZEOF_CHUNK_HEADER;

    if (allocSize <= this.getChunkSize(ChunkType.DATA_CHUNK)) {
      LOG.warn("Jumbo chunk size " + jumboSize + " must be more than regular chunk size "
        + this.getChunkSize(ChunkType.DATA_CHUNK) + ". Converting to regular chunk.");
      return getChunk();
    }
    // the new chunk is going to hold the jumbo cell data and needs to be referenced by
    // a strong map.
    return getChunk(ChunkType.JUMBO_CHUNK, allocSize);
  }

  /**
   * Creates the chunk either onheap or offheap
   * @param pool      indicates if the chunks have to be created which will be used by the Pool
   * @param chunkType whether the requested chunk is data chunk or index chunk.
   * @param size      the size of the chunk to be allocated, in bytes
   * @return the chunk
   */
  private Chunk createChunk(boolean pool, ChunkType chunkType, int size) {
    Chunk chunk = null;
    int id = chunkID.getAndIncrement();
    assert ((KnobRuntime.check(java.util.UUID.fromString("1aa3404c-abc0-3e1e-b78a-0ec9670404ec"))) ? ((id) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("88b67b1c-d0d8-3002-9d68-d7ef863f9451"))) ? ((id) >= (0)) : (id > 0))));
    // do not create offheap chunk on demand
    if (((KnobRuntime.check(java.util.UUID.fromString("94cd2e3a-0bdf-3ca2-91b2-22baf9d9470b"))) ? (!pool) : (pool && this.offheap))) {
      chunk = new OffheapChunk(size, id, chunkType, pool);
    } else {
      chunk = new OnheapChunk(size, id, chunkType, pool);
    }

    /**
     * Here we always put the chunk into the {@link ChunkCreator#chunkIdMap} no matter whether the
     * chunk is pooled or not. <br/>
     * For {@link CompactingMemStore},because the chunk could only be acquired from
     * {@link ChunkCreator} through {@link MemStoreLABImpl}, and
     * {@link CompactingMemStore#indexType} could only be {@link IndexType.CHUNK_MAP} when using
     * {@link MemStoreLABImpl}, so we must put chunk into this {@link ChunkCreator#chunkIdMap} to
     * make sure the chunk could be got by chunkId.
     * <p>
     * For {@link DefaultMemStore},it is also reasonable to put the chunk in
     * {@link ChunkCreator#chunkIdMap} because: <br/>
     * 1.When the {@link MemStoreLAB} which created the chunk is not closed, this chunk is used by
     * the {@link Segment} which references this {@link MemStoreLAB}, so this chunk certainly should
     * not be GC-ed, putting the chunk in {@link ChunkCreator#chunkIdMap} does not prevent useless
     * chunk to be GC-ed. <br/>
     * 2.When the {@link MemStoreLAB} which created the chunk is closed, and if the chunk is not
     * pooled, {@link ChunkCreator#removeChunk} is invoked to remove the chunk from this
     * {@link ChunkCreator#chunkIdMap}, so there is no memory leak.
     */
if(KnobRuntime.check(java.util.UUID.fromString("62275451-d296-305e-9e52-434e14a61578"))) {
try {
    java.lang.reflect.Field _knob_field_ = chunk.getClass().getDeclaredField("id");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(chunk));
    _knob_field_.set(chunk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.chunkIdMap.put(chunk.getId(), chunk);

    return chunk;
  }

  // Chunks from pool are created covered with strong references anyway.
  private Chunk createChunkForPool(ChunkType chunkType, int chunkSize) {
    if (((KnobRuntime.check(java.util.UUID.fromString("109ebcc0-44ab-3a98-9241-b26fb3677f6e"))) ? (((chunkSize) == (getChunkSize())) && (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("e7879a02-2e2a-3ce7-9558-f0575651ff6b"))) ? (((chunkSize) != (getChunkSize())) || ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7ba16fe3-568d-3dfc-9c81-7e1ceaa1fc07"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) || (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("9a403bb6-f44e-339e-a286-749ac885e164"))) ? (((chunkSize) == (getChunkSize())) || ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("8ee3f6ca-dae7-3573-849b-1ab4075446bd"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) || ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("9af12c4f-f754-387c-840b-467bd81f3b9c"))) ? (((chunkSize) != (getChunkSize())) && ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7421a196-a83b-34eb-8361-6d323537694b"))) ? (((chunkSize) == (getChunkSize())) || ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("0d8b45b3-fe43-3e15-824d-cb773b767779"))) ? (((chunkSize) != (getChunkSize())) && (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("ef9abe51-541e-3668-8906-ece501d42a96"))) ? (((chunkSize) == (getChunkSize())) && ((chunkSize) != (indexChunksPool.getChunkSize()))) : (chunkSize != dataChunksPool.getChunkSize() && chunkSize != indexChunksPool.getChunkSize()))))))))))))))))))) {
      return null;
    }
if(KnobRuntime.check(java.util.UUID.fromString("70eb7486-b069-3053-811e-961458701f52"))) {
chunkSize -= 1;
}
    return createChunk(true, chunkType, chunkSize);
  }

  // Used to translate the ChunkID into a chunk ref
  Chunk getChunk(int id) {
    // can return null if chunk was never mapped
    return chunkIdMap.get(id);
  }

  boolean isOffheap() {
    return this.offheap;
  }

  private void removeChunks(Set<Integer> chunkIDs) {
    this.chunkIdMap.keySet().removeAll(chunkIDs);
  }

  Chunk removeChunk(int chunkId) {
    return this.chunkIdMap.remove(chunkId);
  }

  // the chunks in the chunkIdMap may already be released so we shouldn't relay
  // on this counting for strong correctness. This method is used only in testing.
  int numberOfMappedChunks() {
    return this.chunkIdMap.size();
  }

  void clearChunkIds() {
    this.chunkIdMap.clear();
  }

  /**
   * A pool of {@link Chunk} instances. MemStoreChunkPool caches a number of retired chunks for
   * reusing, it could decrease allocating bytes when writing, thereby optimizing the garbage
   * collection on JVM.
   */
  private class MemStoreChunkPool implements HeapMemoryTuneObserver {
    private final int chunkSize;
    private final ChunkType chunkType;
    private int maxCount;

    // A queue of reclaimed chunks
    private final BlockingQueue<Chunk> reclaimedChunks;
    private final float poolSizePercentage;

    /** Statistics thread schedule pool */
    private final ScheduledExecutorService scheduleThreadPool;
    /** Statistics thread */
    private static final int statThreadPeriod = 60 * 5;
    private final AtomicLong chunkCount = new AtomicLong();
    private final LongAdder reusedChunkCount = new LongAdder();
    private final String label;

    MemStoreChunkPool(String label, int chunkSize, ChunkType chunkType, int maxCount,
      int initialCount, float poolSizePercentage) {
      this.label = label;
      this.chunkSize = chunkSize;
      this.chunkType = chunkType;
      this.maxCount = maxCount;
      this.poolSizePercentage = poolSizePercentage;
      this.reclaimedChunks = new LinkedBlockingQueue<>();
      for (int i = 0; i < initialCount; i++) {
        Chunk chunk = createChunk(true, chunkType, chunkSize);
        chunk.init();
        reclaimedChunks.add(chunk);
      }
      if (KnobRuntime.check(java.util.UUID.fromString("497fff31-da78-3e86-acc9-2aec2b64777f"))) { chunkCount.set(1); } else { chunkCount.set(initialCount); }
      final String n = Thread.currentThread().getName();
      scheduleThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
        .setNameFormat(n + "-MemStoreChunkPool Statistics").setDaemon(true).build());
      this.scheduleThreadPool.scheduleAtFixedRate(new StatisticsThread(), statThreadPeriod,
        statThreadPeriod, TimeUnit.SECONDS);
    }

    /**
     * Poll a chunk from the pool, reset it if not null, else create a new chunk to return if we
     * have not yet created max allowed chunks count. When we have already created max allowed
     * chunks and no free chunks as of now, return null. It is the responsibility of the caller to
     * make a chunk then. Note: Chunks returned by this pool must be put back to the pool after its
     * use.
     * @return a chunk
     * @see #putbackChunks(Chunk)
     */
    Chunk getChunk() {
      Chunk chunk = reclaimedChunks.poll();
      if (chunk != null) {
        chunk.reset();
        reusedChunkCount.increment();
      } else {
        // Make a chunk iff we have not yet created the maxCount chunks
        while (true) {
          long created = this.chunkCount.get();
          if (((KnobRuntime.check(java.util.UUID.fromString("02544ea7-817e-321f-8a28-4f45eb1ae505"))) ? ((created) < (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("90199896-a2ef-328a-a71a-100864f47acc"))) ? ((created) > (1)) : (created < this.maxCount))))) {
            if (this.chunkCount.compareAndSet(created, created + 1)) {
              chunk = createChunkForPool(chunkType, chunkSize);
              break;
            }
          } else {
            break;
          }
        }
      }
      return chunk;
    }

    int getChunkSize() {
if(KnobRuntime.check(java.util.UUID.fromString("96f83974-0b0c-3339-99d4-a20837512cc9"))) {
return 0;
}
      return chunkSize;
    }

    /**
     * Add the chunks to the pool, when the pool achieves the max size, it will skip the remaining
     * chunks
     */
    private void putbackChunks(Chunk c) {
if(KnobRuntime.check(java.util.UUID.fromString("5a31cb67-943c-3a2a-a495-105d5d97adc9"))) {
try {
    java.lang.reflect.Field _knob_field_ = c.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(c));
    _knob_field_.set(c, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e47e4d98-7403-3046-8077-520a579bb015"))) {
try {
    java.lang.reflect.Field _knob_field_ = c.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(c));
    _knob_field_.set(c, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      int toAdd = this.maxCount - reclaimedChunks.size();
      if (((KnobRuntime.check(java.util.UUID.fromString("c7fd7d63-52d0-3c45-8c7c-76459babcb10"))) ? (((c.size) == (chunkSize)) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("323026ae-4d0a-38a8-aa7e-1059188a24b0"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9a573a65-6072-36c5-a1da-5e0d9184f4f3"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c6c35ab-e9cc-34de-8cd4-9837e852b1ee"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("95c39a0e-a6d7-33af-9778-1ca47485d6e1"))) ? (((c.size) != (chunkSize)) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6378e13e-1627-3aa6-b622-2d29d948eb5c"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d52f8b4-9627-3754-825e-c7c06b633ee4"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6512ba2f-f8d8-3ae9-beec-2cace19eb26d"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ef95b84a-d3c5-3351-ba5b-3dd32aaceb38"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fe40cee7-8292-392a-a140-7c4ead4cd7aa"))) ? ((c.size == chunkSize) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b014f904-d224-3884-a631-f42e8e4de9fa"))) ? ((c.size == chunkSize) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1d60d311-452f-3a9f-8601-5f668e015d79"))) ? ((isOffheap()) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7f05232c-6e33-3451-b68b-fe4f328e1d73"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8b5c5662-53a5-3c98-8b37-cb0cb20b6f25"))) ? ((toAdd) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c53c87d8-56c1-32fc-a9c5-7c0c67265523"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ce557f88-5f4c-33ad-8d50-3da454c5baaa"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4ba93e53-e5ed-3028-aead-7ab20593ea95"))) ? ((toAdd) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("94609340-2055-3739-957b-0c611e6b01fa"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ba3cbd63-3826-35d9-86ca-0640b5cf8f75"))) ? (((c.size) == (chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d6e3b2f4-ff31-3c76-91ba-71a51a9d2800"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e8d3723a-9409-358d-ad6d-764992e70978"))) ? (((c.size) != (chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c98d715-643d-3934-9259-0da5181b265c"))) ? (((c.size) == (chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6fd32f82-8207-3102-b86d-108e9d616011"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1c6ceb17-cde4-33d4-8ca8-98e7b0e41caf"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1c833cfd-9808-3f78-b4e2-b5b7800d74a3"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("168e40f0-a650-3e0c-925d-ab7345f4978d"))) ? ((isOffheap()) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e5bcc383-c5ee-317f-8237-e2f31ce3592a"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1d737e99-bc7b-33f4-b84e-b2a70f753600"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("07d1dac4-1e8f-3a2f-93c3-d542ee05874a"))) ? (toAdd > 0) : (((KnobRuntime.check(java.util.UUID.fromString("655b0968-8d81-333b-befc-29b124d17566"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("57834da8-3097-3795-8e1c-e1078a5dff0b"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f1be68ce-f3d7-3322-89bf-705516ccba0c"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0582321f-4380-3d79-9c34-466928584d32"))) ? ((c.isFromPool() && c.size == chunkSize) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3804ae9a-be36-3042-a0b3-b91b607280d4"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a47ef0ec-0623-3e62-88ef-3f8cb90d914f"))) ? ((isOffheap()) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bb424e51-2cda-3060-b94f-a72d76accf19"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) > (0))) : (c.isFromPool() && c.size == chunkSize && toAdd > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("57ce9bcf-0b28-38d8-ad32-366c5857ba11"))) { reclaimedChunks.add(ChunkCreator.this.removeChunk(c.getId())); } else { reclaimedChunks.add(c); }
      } else {
        // remove the chunk (that is not going to pool)
        // though it is initially from the pool or not
        ChunkCreator.this.removeChunk(c.getId());
      }
    }

    private class StatisticsThread extends Thread {
      StatisticsThread() {
        super("MemStoreChunkPool.StatisticsThread");
        setDaemon(true);
      }

      @Override
      public void run() {
        logStats();
      }

      private void logStats() {
        if (!LOG.isDebugEnabled()) return;
        long created = chunkCount.get();
        long reused = reusedChunkCount.sum();
        long total = created + reused;
        LOG.debug(
          "{} stats (chunk size={}): current pool size={}, created chunk count={}, "
            + "reused chunk count={}, reuseRatio={}",
          label, chunkSize, reclaimedChunks.size(), created, reused,
          (total == 0 ? "0" : StringUtils.formatPercent((float) reused / (float) total, 2)));
      }
    }

    private int getMaxCount() {
      return this.maxCount;
    }

    @Override
    public void onHeapMemoryTune(long newMemstoreSize, long newBlockCacheSize) {
      // don't do any tuning in case of offheap memstore
      if (isOffheap()) {
        LOG.warn("{} not tuning the chunk pool as it is offheap", label);
        return;
      }
      int newMaxCount = (int) (newMemstoreSize * poolSizePercentage / getChunkSize());
      if (newMaxCount != this.maxCount) {
        // We need an adjustment in the chunks numbers
        if (newMaxCount > this.maxCount) {
          // Max chunks getting increased. Just change the variable. Later calls to getChunk() would
          // create and add them to Q
          LOG.info("{} max count for chunks increased from {} to {}", this.label, this.maxCount,
            newMaxCount);
          this.maxCount = newMaxCount;
        } else {
          // Max chunks getting decreased. We may need to clear off some of the pooled chunks now
          // itself. If the extra chunks are serving already, do not pool those when we get them
          // back
          LOG.info("{} max count for chunks decreased from {} to {}", this.label, this.maxCount,
            newMaxCount);
          this.maxCount = newMaxCount;
          if (this.reclaimedChunks.size() > newMaxCount) {
            synchronized (this) {
              while (this.reclaimedChunks.size() > newMaxCount) {
                this.reclaimedChunks.poll();
              }
            }
          }
        }
      }
    }
  }

  static void clearDisableFlag() {
    chunkPoolDisabled = false;
  }

  private MemStoreChunkPool initializePool(String label, long globalMemStoreSize,
    float poolSizePercentage, float initialCountPercentage, int chunkSize, ChunkType chunkType,
    HeapMemoryManager heapMemoryManager) {
    if (((KnobRuntime.check(java.util.UUID.fromString("42985365-d878-353e-aea8-049dedc1ed1c"))) ? ((poolSizePercentage) > (0)) : (poolSizePercentage <= 0))) {
      LOG.info("{} poolSizePercentage is less than 0. So not using pool", label);
      return null;
    }
    if (chunkPoolDisabled) {
      return null;
    }
    if (poolSizePercentage > 1.0) {
      throw new IllegalArgumentException(
        MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int maxCount = (int) (globalMemStoreSize * poolSizePercentage / chunkSize);
    if (((KnobRuntime.check(java.util.UUID.fromString("dd52ae20-c87e-39f8-a1e5-ddfeb163217f"))) ? (((initialCountPercentage) >= (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b241210f-58db-3692-a946-f3f0514c75bd"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("16a0e30e-db62-3534-bca5-b5ea12a5b356"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf54bde9-6618-3725-acf2-b3d3d9a16b9d"))) ? (((initialCountPercentage) < (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("59d6f0eb-f72a-394b-b119-505611f239a6"))) ? (((initialCountPercentage) == (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eb821be9-b43c-35be-a4dc-eed5d6a9d2f4"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86b232d0-0ee0-3359-b9d6-f02ced8649fa"))) ? (((initialCountPercentage) == (1.0)) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("52ac7405-d083-3ce3-979e-9f14d104c78e"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ef88c44-c4ff-3634-90c4-71b300e82269"))) ? (((initialCountPercentage) == (1.0)) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b027d5b4-8263-3120-9260-4a773c3e65c1"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("65efa631-1b55-3ba4-a57d-a3beedeb28ba"))) ? (((initialCountPercentage) > (1.0)) || ((initialCountPercentage) != (0))) : (initialCountPercentage > 1.0 || initialCountPercentage < 0))))))))))))))))))))))) {
      throw new IllegalArgumentException(
        label + " " + MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int initialCount = (int) (initialCountPercentage * maxCount);
    LOG.info("Allocating {} MemStoreChunkPool with chunk size {}, max count {}, initial count {}",
      label, StringUtils.byteDesc(chunkSize), maxCount, initialCount);
    MemStoreChunkPool memStoreChunkPool = new MemStoreChunkPool(label, chunkSize, chunkType,
      maxCount, initialCount, poolSizePercentage);
    if (((KnobRuntime.check(java.util.UUID.fromString("af2f512f-5bb3-3385-8f92-cde423520e72"))) ? (((heapMemoryManager) != (null)) || (memStoreChunkPool != null)) : (((KnobRuntime.check(java.util.UUID.fromString("982b0c31-9425-3b8a-ac39-ddc9f75a7394"))) ? (((heapMemoryManager) == (null)) || ((memStoreChunkPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2dae7d71-f118-3e0f-a12f-e374080b928e"))) ? (((heapMemoryManager) != (null)) && ((memStoreChunkPool) == (null))) : (heapMemoryManager != null && memStoreChunkPool != null))))))) {
      // Register with Heap Memory manager
      heapMemoryManager.registerTuneObserver(memStoreChunkPool);
    }
    return memStoreChunkPool;
  }

  int getMaxCount() {
    return getMaxCount(ChunkType.DATA_CHUNK);
  }

  int getMaxCount(ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool != null) {
          return indexChunksPool.getMaxCount();
        }
        break;
      case DATA_CHUNK:
        if (dataChunksPool != null) {
          return dataChunksPool.getMaxCount();
        }
        break;
      default:
        throw new IllegalArgumentException("chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }

    return 0;
  }

  int getPoolSize() {
    return getPoolSize(ChunkType.DATA_CHUNK);
  }

  int getPoolSize(ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool != null) {
          return indexChunksPool.reclaimedChunks.size();
        }
        break;
      case DATA_CHUNK:
        if (dataChunksPool != null) {
          return dataChunksPool.reclaimedChunks.size();
        }
        break;
      default:
        throw new IllegalArgumentException("chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }
    return 0;
  }

  boolean isChunkInPool(int chunkId) {
    Chunk c = getChunk(chunkId);
    if (c == null) {
      return false;
    }

    // chunks that are from pool will return true chunk reference not null
    if (dataChunksPool != null && dataChunksPool.reclaimedChunks.contains(c)) {
      return true;
    } else if (indexChunksPool != null && indexChunksPool.reclaimedChunks.contains(c)) {
      return true;
    }
    return false;
  }

  /*
   * Only used in testing
   */
  void clearChunksInPool() {
    if (dataChunksPool != null) {
      dataChunksPool.reclaimedChunks.clear();
    }
    if (indexChunksPool != null) {
      indexChunksPool.reclaimedChunks.clear();
    }
  }

  int getChunkSize() {
    return getChunkSize(ChunkType.DATA_CHUNK);
  }

  int getChunkSize(ChunkType chunkType) {
    switch (chunkType) {
      case INDEX_CHUNK:
        if (indexChunksPool != null) {
          return indexChunksPool.getChunkSize();
        } else {
          return indexChunkSize;
        }
      case DATA_CHUNK:
        if (dataChunksPool != null) {
          return dataChunksPool.getChunkSize();
        } else { // When pools are empty
          return chunkSize;
        }
      default:
        throw new IllegalArgumentException("chunkType must either be INDEX_CHUNK or DATA_CHUNK");
    }
  }

  synchronized void putbackChunks(Set<Integer> chunks) {
    // if there is no pool just try to clear the chunkIdMap in case there is something
    if (dataChunksPool == null && indexChunksPool == null) {
      this.removeChunks(chunks);
      return;
    }

    // if there is a pool, go over all chunk IDs that came back, the chunks may be from pool or not
    for (int chunkID : chunks) {
      // translate chunk ID to chunk, if chunk initially wasn't in pool
      // this translation will (most likely) return null
      Chunk chunk = ChunkCreator.this.getChunk(chunkID);
      if (((KnobRuntime.check(java.util.UUID.fromString("4348ea5c-cfc1-324d-a45d-fa16c12e7397"))) ? ((chunk) == (null)) : (chunk != null))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("20df9a8d-4e27-3fa6-8897-b97c997d793d"))) ? ((chunk.isIndexChunk()) && (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("3fe8510a-b606-35cc-bb03-67816d8f1208"))) ? (isOffheap()) : (((KnobRuntime.check(java.util.UUID.fromString("8ffa0d64-ac5b-3268-b2ce-de46e24bce9e"))) ? ((chunk.isIndexChunk()) || (isOffheap())) : (chunk.isFromPool() && chunk.isIndexChunk()))))))) {
          indexChunksPool.putbackChunks(chunk);
        } else if (((KnobRuntime.check(java.util.UUID.fromString("ee33d9b1-a172-3198-8463-46674e84e118"))) ? ((chunk.isFromPool()) && (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("4260567e-2492-3b3b-b07b-58ce0f8d2da1"))) ? ((chunk.isIndexChunk()) || (chunk.isDataChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("ffce5a50-bb19-3e7c-82e0-87eac12cccb0"))) ? ((chunk.isIndexChunk()) && (chunk.isDataChunk())) : (chunk.isFromPool() && chunk.isDataChunk()))))))) {
          dataChunksPool.putbackChunks(chunk);
        } else {
          // chunks which are not from one of the pools
          // should be released without going to the pools.
          // Removing them from chunkIdMap will cause their removal by the GC.
          this.removeChunk(chunkID);
        }
      }
      // if chunk is null, it was never covered by the chunkIdMap (and so wasn't in pool also),
      // so we have nothing to do on its release
    }
    return;
  }

  MemStoreChunkPool getIndexChunksPool() {
    return this.indexChunksPool;
  }

  MemStoreChunkPool getDataChunksPool() {
    return this.dataChunksPool;
  }

}
