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
if(KnobRuntime.check(java.util.UUID.fromString("498d3bfb-75ad-3262-8eee-24601975774c"))) {
globalMemStoreSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("db74a592-a39e-3024-9b52-98c6067ec63d"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("maxHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("183d12c0-ec10-3f90-b47a-1c52abd6ae26"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("bc41ccef-480c-3c1f-9b06-4d0e05e1b8de"))) {
chunkSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a9cafefc-c9df-31bf-8830-8f5105d0f264"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("maxHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3ba4b75-1270-3792-84df-c622df6597d9"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ddd317c3-fa2a-3c94-9ef1-9cb0254f31cb"))) {
globalMemStoreSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c58bc0f-7bcb-3bc4-9e3a-89004a42e52a"))) {
globalMemStoreSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b7542b0b-a091-35df-b6d4-660a72e21b25"))) {
chunkSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("62ce6452-7fc4-3247-80da-d006d5684b10"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("acf3ccdd-e91a-3641-b68b-80820558f50c"))) {
chunkSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a110bce9-5510-3537-9932-38866a6d07ef"))) {
chunkSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b1443e17-00d0-3510-adb6-1be790f7bf69"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0fe9872-a5b1-3964-971e-4c0c2cbca54e"))) {
chunkSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c5b51328-4331-3a5a-9195-cabdebf230a9"))) {
globalMemStoreSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fb6f857e-57a5-31fa-a56a-d189db05b6bf"))) {
initialCountPercentage = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bda93694-a22c-3275-b046-89e68760cea2"))) {
chunkSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e350b58b-6478-3b9a-9a78-6cc04a229526"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
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
if(KnobRuntime.check(java.util.UUID.fromString("b8e9975e-7244-3416-b28e-36031c1cfdaf"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("maxHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bfd1481f-4118-3169-8325-4470b165eab7"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b6ad85ad-d5b7-34da-8105-4385604958f6"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55d19cff-3b2c-35d5-99aa-ee1b8116f44f"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("maxHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2e84810e-8de5-303f-980d-587ed2c81ac2"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae740390-517c-3995-9abf-d888b7aaaebc"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9beff97e-4c85-3252-9c8e-87fd93d0f3f4"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("4327ce13-211a-3bc7-9dae-faba7672d379"))) ? ((instance) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2c80c719-844e-36e6-b1ec-c2f0a4d2e98e"))) ? ((instance) != (null)) : (instance != null))))) {
      return instance;
    }
    instance = new ChunkCreator(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
      initialCountPercentage, heapMemoryManager, indexChunkSizePercent);
    return instance;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MS_EXPOSE_REP",
      justification = "singleton pattern")
  public static ChunkCreator getInstance() {
if(KnobRuntime.check(java.util.UUID.fromString("d0c6fa6f-2ef1-31ba-893b-3fe01e3bb91c"))) {
return null;
}
    return instance;
  }

  /**
   * Creates and inits a data chunk. The default implementation.
   * @return the chunk that was initialized
   */
  Chunk getChunk() {
if(KnobRuntime.check(java.util.UUID.fromString("6d70b3ce-1c36-391e-9aef-fece93a7164a"))) {
return null;
}
    return getChunk(ChunkType.DATA_CHUNK);
  }

  /**
   * Creates and inits a chunk with specific type.
   * @return the chunk that was initialized
   */
  Chunk getChunk(ChunkType chunkType) {
if(KnobRuntime.check(java.util.UUID.fromString("c4ee8e95-e178-32d9-bf8d-06765456bfb0"))) {
return null;
}
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
        if (((KnobRuntime.check(java.util.UUID.fromString("80600e08-aca3-30c6-9b9a-f7f9564b95c0"))) ? ((dataChunksPool) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2ddc635c-ebb7-3cfb-a843-e36aa7b467f2"))) ? ((dataChunksPool) != (null)) : (dataChunksPool == null))))) {
          return getChunk(chunkType, chunkSize);
        } else {
          return ((KnobRuntime.check(java.util.UUID.fromString("cafe8987-5dc6-3ecf-bd39-5b27f3e57d3f"))) ? (getChunk(chunkType, getChunkSize())) : (getChunk(chunkType, dataChunksPool.getChunkSize())));
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
if(KnobRuntime.check(java.util.UUID.fromString("7d0e901d-7dd0-38b6-ac1a-8cd9aa957599"))) {
return null;
}
    Chunk chunk = null;
    MemStoreChunkPool pool = null;

    // if it is one of the pools
    if (((KnobRuntime.check(java.util.UUID.fromString("06a1517c-220c-399d-a87a-3996e85902ef"))) ? ((dataChunksPool) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d3df20d1-dc25-3c1c-a21e-7d08b01c2eb0"))) ? (((dataChunksPool) != (null)) || (chunkType == ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("2fe56ec8-e948-3c81-bd0c-9afbcab9bb36"))) ? ((dataChunksPool != null) || ((chunkType) != (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("cee9974e-5fd5-370d-8396-fb615a798d4b"))) ? ((dataChunksPool != null) || ((chunkType) == (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("ad0f3cf8-307b-32c7-97f9-7587d0037721"))) ? (((dataChunksPool) == (null)) && ((chunkType) != (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("fdb3db88-790b-3e09-85c0-9763cbafc33a"))) ? (((dataChunksPool) != (null)) || ((chunkType) == (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("c6c87865-724a-34d3-9bd9-a906b2a44b1e"))) ? (((dataChunksPool) != (null)) && (chunkType == ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("cd7884af-539e-3af0-bec8-70bc014b7f2a"))) ? ((dataChunksPool != null) || (chunkType == ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("3aba2da6-6720-3a7b-a72f-b80746b66a5e"))) ? ((dataChunksPool != null) && (chunkType == ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("5ef72ad3-164e-3495-a9ed-30391ba90933"))) ? (((dataChunksPool) != (null)) && ((chunkType) != (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("367e5b38-4512-3b04-a12e-f4517287500e"))) ? (((dataChunksPool) == (null)) || (chunkType == ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("f7867090-00bc-391c-94e2-b56a357074ba"))) ? ((dataChunksPool != null) && ((chunkType) != (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("e03e0d36-6e5e-3222-966b-40b21b776ecb"))) ? (((dataChunksPool) == (null)) || ((chunkType) == (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("c8f2a125-dc88-3c22-9a35-a97f2c8d249f"))) ? ((dataChunksPool) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6b78a592-10a6-39b8-b408-7d6b18d9c3e7"))) ? (((dataChunksPool) == (null)) || ((chunkType) != (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("65dc2a79-3a56-3e8b-9bde-43abd9f2a543"))) ? (chunkType == ChunkType.DATA_CHUNK) : (((KnobRuntime.check(java.util.UUID.fromString("9f970c40-e8ee-3ca0-a6a3-d82f7af0ca9c"))) ? ((chunkType) != (ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("1eacce8b-47a6-36bb-9a27-b65ca96b856e"))) ? (((dataChunksPool) == (null)) && (chunkType == ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("22ac2abb-4617-3224-aa81-81766cfe5f7c"))) ? (((dataChunksPool) == (null)) && ((chunkType) == (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("f571bea9-d398-3fe3-afd4-c296749b81ea"))) ? (dataChunksPool != null) : (((KnobRuntime.check(java.util.UUID.fromString("2d638345-f474-34e9-9d4b-e083d9389d5b"))) ? ((chunkType) == (ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("4c8eaee2-ce10-3bef-9887-6abc7453b054"))) ? (((dataChunksPool) != (null)) || ((chunkType) != (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("ad901fc9-f1c5-3a42-a943-89d17acffbee"))) ? ((dataChunksPool != null) && ((chunkType) == (ChunkType.DATA_CHUNK))) : (((KnobRuntime.check(java.util.UUID.fromString("79587a92-8a78-3de9-b01e-c427926e41a1"))) ? (((dataChunksPool) != (null)) && ((chunkType) == (ChunkType.DATA_CHUNK))) : (dataChunksPool != null && chunkType == ChunkType.DATA_CHUNK))))))))))))))))))))))))))))))))))))))))))))))))) {
      pool = dataChunksPool;
    } else if (indexChunksPool != null && chunkType == ChunkType.INDEX_CHUNK) {
      pool = indexChunksPool;
    }

    // if we have a pool
    if (((KnobRuntime.check(java.util.UUID.fromString("c3fa6586-6051-3d55-adcc-65e09f930bba"))) ? ((pool) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cea9f1ce-deb6-36c7-ab8a-b54731ce8dcc"))) ? ((pool) != (null)) : (pool != null))))) {
      // the pool creates the chunk internally. The chunk#init() call happens here
      chunk = pool.getChunk();
      // the pool has run out of maxCount
      if (((KnobRuntime.check(java.util.UUID.fromString("b8fdbcfb-43d6-362e-8a3a-eedd57c1fff0"))) ? ((chunk) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("70f2d32b-72eb-380f-b438-8c1a297ca758"))) ? ((chunk) != (null)) : (chunk == null))))) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("The chunk pool is full. Reached maxCount= " + pool.getMaxCount()
            + ". Creating chunk onheap.");
        }
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("c54b23d2-2ca2-3a29-b1de-98c9fa96217b"))) ? ((chunk) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a8c36bbf-f2e5-340e-b0d7-4461df2eec56"))) ? ((chunk) == (null)) : (chunk == null))))) {
      chunk = createChunk(false, chunkType, size);
    }

    // now we need to actually do the expensive memory allocation step in case of a new chunk,
    // else only the offset is set to the beginning of the chunk to accept allocations
    if (KnobRuntime.check(java.util.UUID.fromString("f9b1b490-82b8-36b5-9b49-0db80c611fb4"))) { chunk.reset(); } else { chunk.init(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("9950e083-e101-3e3d-9527-f96bdc630607"))) {
return null;
}
    Chunk chunk = null;
    int id = chunkID.getAndIncrement();
    assert ((KnobRuntime.check(java.util.UUID.fromString("0390275f-dae8-32e4-91d4-452346e6632a"))) ? ((id) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4e40f839-f7f4-38f5-90ac-9de82cd94bf2"))) ? ((id) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("88b67b1c-d0d8-3002-9d68-d7ef863f9451"))) ? ((id) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1aa3404c-abc0-3e1e-b78a-0ec9670404ec"))) ? ((id) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8853d948-6de0-36c8-9585-40377ad6d319"))) ? ((id) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cfb2c44b-a9ef-3b45-80c9-d06a9b37f130"))) ? ((id) != (0)) : (id > 0))))))))))));
    // do not create offheap chunk on demand
    if (((KnobRuntime.check(java.util.UUID.fromString("19e3c64d-f571-367c-9d86-5b5b3249dece"))) ? ((pool) || (this.offheap)) : (((KnobRuntime.check(java.util.UUID.fromString("5fa0bf06-f15b-375a-82d2-0cd5ccdc936c"))) ? ((pool) || (!this.offheap)) : (((KnobRuntime.check(java.util.UUID.fromString("94cd2e3a-0bdf-3ca2-91b2-22baf9d9470b"))) ? (!pool) : (((KnobRuntime.check(java.util.UUID.fromString("7d1716db-e5d3-343f-b8c7-a2d1fa366601"))) ? ((pool) && (this.offheap)) : (((KnobRuntime.check(java.util.UUID.fromString("c1da779b-234d-30bc-9eba-99f95890df7c"))) ? (pool) : (((KnobRuntime.check(java.util.UUID.fromString("5fa229cf-8550-3ba9-88d8-51f117f91ec3"))) ? ((!pool) && (!this.offheap)) : (((KnobRuntime.check(java.util.UUID.fromString("209ac72d-e86c-33f5-9e7f-769deccde0ad"))) ? ((!pool) && (this.offheap)) : (((KnobRuntime.check(java.util.UUID.fromString("78c6f625-18a6-3b83-9cb8-ae83d8b9078d"))) ? (!this.offheap) : (((KnobRuntime.check(java.util.UUID.fromString("c7710a91-5fb1-3677-acfd-3e6ecacfb817"))) ? ((!pool) || (!this.offheap)) : (((KnobRuntime.check(java.util.UUID.fromString("099dba12-d51d-315e-9641-3f47866eafc3"))) ? ((!pool) || (this.offheap)) : (((KnobRuntime.check(java.util.UUID.fromString("99c1f2dc-8778-365f-8b3f-f76522b25454"))) ? (this.offheap) : (((KnobRuntime.check(java.util.UUID.fromString("8b00d765-3653-35e1-885c-e21cea8862a0"))) ? ((pool) && (!this.offheap)) : (pool && this.offheap))))))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("538f67fe-f61b-3c0d-bd86-bbae61cc79d7"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e0b53fa-994c-38d8-8621-b05107923ff5"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b0bae6c-23e3-391c-86bf-a8838482d34b"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61e998dc-0e60-3da8-8544-0a26fb433851"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("952a0e02-0e5e-39de-b506-7d311ba5d091"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2af2af9a-e459-36f0-8143-03fed8b04551"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6aae178f-4f26-3a8a-bc54-2220cd097b94"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("722348d4-a8b6-393f-b6a3-b93d39de1787"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("7e7ad7cc-a269-3d14-88cb-b7aa69e5b1c1"))) { this.chunkIdMap.put(getChunkSize(), chunk); } else { this.chunkIdMap.put(chunk.getId(), chunk); }

    return chunk;
  }

  // Chunks from pool are created covered with strong references anyway.
  private Chunk createChunkForPool(ChunkType chunkType, int chunkSize) {
if(KnobRuntime.check(java.util.UUID.fromString("53abff32-e0b2-3dd1-bd67-a219da2ce1a8"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("797a04dd-c6d5-3a92-93bd-d5f90101bc34"))) ? (((chunkSize) != (getChunkSize())) && ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("23c15aa5-8b41-370f-9085-ab8b48ceaaea"))) ? ((chunkSize) == (getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("5db93d41-4298-312a-b749-539d4c511e1b"))) ? (((chunkSize) != (getChunkSize())) && ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7bbccf2d-6cf3-36ca-8ffb-0bf216a9ee1e"))) ? ((chunkSize != dataChunksPool.getChunkSize()) && (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("65e5799c-0a88-372e-a132-a076af65c45f"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) || ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("523f939c-62e8-345f-878d-12cc95178abf"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) && (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("8a9c4b9b-f232-3b9e-b071-7a8f1628eaf8"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) && ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("8ee3f6ca-dae7-3573-849b-1ab4075446bd"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) || ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("53e08ae8-c0ed-38b1-a5f0-15fcd15c964a"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) || ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("33d39121-c779-34dc-8ad2-8fbe04af3e0d"))) ? ((chunkSize != dataChunksPool.getChunkSize()) && ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("68083d0f-9662-3e4f-ae36-2dc0f49a6d04"))) ? ((chunkSize) != (indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("9af12c4f-f754-387c-840b-467bd81f3b9c"))) ? (((chunkSize) != (getChunkSize())) && ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ba00e40c-e1a5-36d7-9df9-2ad1e419f288"))) ? ((chunkSize) != (dataChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("79be5e9a-598f-31b9-8c84-f3060460b33d"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) || ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("2c346240-961d-3f18-aac2-75c9e592bef6"))) ? ((chunkSize) == (dataChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("57d95ace-bd5f-3a5f-b958-0a84ffacc2ba"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) || ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7ba16fe3-568d-3dfc-9c81-7e1ceaa1fc07"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) || (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("3b3f78be-1b6d-3b87-89d4-e84c589a3b24"))) ? ((chunkSize != dataChunksPool.getChunkSize()) || (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("6d41d8b5-c08f-38c5-9d4f-39f091ba9a09"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) && (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("1b91b48b-e2c9-3953-a379-88181f47892a"))) ? (((chunkSize) != (getChunkSize())) && ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("bb4d85ff-d353-3a98-bb7f-61b71ab16dc0"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) && ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("109ebcc0-44ab-3a98-9241-b26fb3677f6e"))) ? (((chunkSize) == (getChunkSize())) && (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("30f989d0-646b-3f87-b2d5-112bf24608a5"))) ? (((chunkSize) == (getChunkSize())) && ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d8456acb-0188-3663-a505-50c1e97d9c28"))) ? ((chunkSize) == (indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("6155b728-1cc8-3833-843a-6fed112f2b66"))) ? ((chunkSize != dataChunksPool.getChunkSize()) || ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("0f35564e-13e4-3ef8-9de0-6bc0b5411cba"))) ? (((chunkSize) != (getChunkSize())) || ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("045461d9-0bee-35a8-ac81-ec590b55bd5d"))) ? ((chunkSize != dataChunksPool.getChunkSize()) || ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e1db9569-11a4-3ef1-8c79-eb98895954f3"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) && ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ad2a143e-8d83-343d-af9a-222fecc96166"))) ? (((chunkSize) == (getChunkSize())) || ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("b302abf1-1c24-39ba-8384-1cec5da9e730"))) ? (chunkSize != indexChunksPool.getChunkSize()) : (((KnobRuntime.check(java.util.UUID.fromString("f18a40f7-f968-355f-b82f-3598ff64ad57"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) && ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("03c75bc5-f5a0-3743-83c1-ff557fa01c7a"))) ? (((chunkSize) != (getChunkSize())) || (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("a3ed97c0-f6d0-3c51-95a8-99cc8d6347d4"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) && ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("f7a78e19-301b-3aef-854c-673ee750dcc0"))) ? (((chunkSize) == (getChunkSize())) || (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("9a403bb6-f44e-339e-a286-749ac885e164"))) ? (((chunkSize) == (getChunkSize())) || ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("47836558-9e3d-3e8b-92b9-04fab0e60ad1"))) ? (((chunkSize) == (getChunkSize())) || ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("8fc42724-fffb-3504-b77e-096e5373cef6"))) ? (((chunkSize) != (getChunkSize())) || ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("cd33b4a6-04bd-393f-9a6e-68f477f3da75"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) && ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("99a837ae-939e-3aaa-94bb-57a6fe4bb3e1"))) ? ((chunkSize != dataChunksPool.getChunkSize()) && ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3e8d662c-9fd5-31da-bf52-cc15a88f430f"))) ? ((chunkSize != dataChunksPool.getChunkSize()) && ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d2bc0b53-758a-3a98-8fce-7395dae06e17"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) || (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("9e0679e5-125a-326c-b2aa-5db754314f2c"))) ? (((chunkSize) != (dataChunksPool.getChunkSize())) || ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("bc35b53c-1b54-35ac-983c-ba698e9a95f0"))) ? (chunkSize != dataChunksPool.getChunkSize()) : (((KnobRuntime.check(java.util.UUID.fromString("0d8b45b3-fe43-3e15-824d-cb773b767779"))) ? (((chunkSize) != (getChunkSize())) && (chunkSize != indexChunksPool.getChunkSize())) : (((KnobRuntime.check(java.util.UUID.fromString("c37a95ef-34e6-34e9-903f-76e7fb9fb7d3"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) && ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("ef9abe51-541e-3668-8906-ece501d42a96"))) ? (((chunkSize) == (getChunkSize())) && ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("780e0b34-8a44-393c-9956-aded83bdadfa"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) || ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("bb77e707-94d8-3b21-bead-5a53a2d1c807"))) ? ((chunkSize != dataChunksPool.getChunkSize()) || ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("541f50af-665c-3f24-8457-f1cd30f5e0e2"))) ? (((chunkSize) == (getChunkSize())) && ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("3c7db6ea-fd80-3dfd-bf09-2453b55c7ef1"))) ? ((chunkSize != dataChunksPool.getChunkSize()) || ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("7421a196-a83b-34eb-8361-6d323537694b"))) ? (((chunkSize) == (getChunkSize())) || ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c0bc1999-4ad1-329c-8371-3754a36d4cba"))) ? (((chunkSize) == (getChunkSize())) && ((chunkSize) == (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("b7fc9fe4-27cc-3d75-bcb8-ca201ca45091"))) ? (((chunkSize) != (getChunkSize())) || ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("e7879a02-2e2a-3ce7-9558-f0575651ff6b"))) ? (((chunkSize) != (getChunkSize())) || ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("92490b16-0235-34fc-ae5a-5202d640261c"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) && ((chunkSize) != (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("c427b603-5761-3dc7-91ee-d6bb2bdeb6f1"))) ? ((chunkSize != dataChunksPool.getChunkSize()) && ((chunkSize) != (getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("34e862f0-87b9-3c4c-b29c-3c5974f518fd"))) ? (((chunkSize) == (dataChunksPool.getChunkSize())) || ((chunkSize) == (indexChunksPool.getChunkSize()))) : (((KnobRuntime.check(java.util.UUID.fromString("d20c3dfc-d935-3f9a-a004-c920ba8d0ec2"))) ? ((chunkSize) != (getChunkSize())) : (chunkSize != dataChunksPool.getChunkSize() && chunkSize != indexChunksPool.getChunkSize()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return null;
    }
if(KnobRuntime.check(java.util.UUID.fromString("70eb7486-b069-3053-811e-961458701f52"))) {
chunkSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6477d44e-0ec8-3f70-80a0-ecdf17d407a2"))) {
chunkSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e03b3b34-97e8-324e-aadc-dbc008d31444"))) {
chunkSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("44e588ae-edc2-30bc-8f76-eb5e4f3d370a"))) {
chunkSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb59ab18-269a-31a0-a3e5-3dbc21731f18"))) {
chunkSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("382bc60f-b6fa-3f14-b0c2-105b7a6b4197"))) {
chunkSize = -1;
}
    return createChunk(true, chunkType, chunkSize);
  }

  // Used to translate the ChunkID into a chunk ref
  Chunk getChunk(int id) {
    // can return null if chunk was never mapped
if(KnobRuntime.check(java.util.UUID.fromString("cd875d6b-924e-36d8-be3f-39f1edd6f57d"))) {
id -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3b75ecad-10e7-378b-a3af-fed39e773476"))) {
id = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("312538a3-61fe-3b96-898b-9d72e8c53a21"))) {
id /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4afcdee4-296c-309a-b86b-f563ba4cc82c"))) {
id += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c5cf690-173d-345c-826f-acda6172a824"))) {
id = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4bfb2d65-7426-38c6-97ff-68e88a50559b"))) {
id *= 2;
}
    return chunkIdMap.get(id);
  }

  boolean isOffheap() {
if(KnobRuntime.check(java.util.UUID.fromString("01ce85be-56d2-3481-83a6-6cd226fa112f"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("fcef82a7-d184-3263-961a-64dba20308e8"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("342b851f-5d21-3a9e-9dd9-cc33b3f3f0dd"))) ? (!this.offheap) : (this.offheap));
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
      if (KnobRuntime.check(java.util.UUID.fromString("27d05c3e-9c4f-3da1-ad48-ed4b80063d1e"))) { chunkCount.set(0); } else if (KnobRuntime.check(java.util.UUID.fromString("497fff31-da78-3e86-acc9-2aec2b64777f"))) { chunkCount.set(1); } else { chunkCount.set(initialCount); }
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
      if (((KnobRuntime.check(java.util.UUID.fromString("5c914b77-e630-386f-87c8-db45f4e6cda3"))) ? ((chunk) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("abc6324f-8729-3b56-bbea-b5fa15538335"))) ? ((chunk) != (null)) : (chunk != null))))) {
        if (KnobRuntime.check(java.util.UUID.fromString("06f18e5c-c40e-31d7-8db3-f7ba034f6165"))) { chunk.init(); } else { chunk.reset(); }
        reusedChunkCount.increment();
      } else {
        // Make a chunk iff we have not yet created the maxCount chunks
        while (true) {
          long created = this.chunkCount.get();
          if (((KnobRuntime.check(java.util.UUID.fromString("d33dd2c3-107f-35e0-ae5d-2ebf78f5957c"))) ? ((created) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("c02324e0-10a3-3060-a867-b8306eafc933"))) ? ((created + 1) <= (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("afda856f-b1c9-3c31-9f92-b697c0bc74a4"))) ? ((created + 1) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e9dbab9f-d597-3f7b-8d7a-bd35321cb4d0"))) ? ((created) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("8c8f9423-ffc3-32cd-bcea-1a89e8fcb049"))) ? ((created + 1) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("b0d36df1-b802-38c7-aa7e-615b35584c1a"))) ? ((created + 1) > (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("0deadd68-32da-3f1e-92f3-4ef67a14dc4b"))) ? ((created + 1) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("a7fdad6d-dcef-38ec-a70e-442532499ed7"))) ? ((created) == (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("46db1e11-26c3-3ec0-8ddc-4882bcbc48b1"))) ? ((created) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("109730db-332c-3e90-860f-c23b186f89ab"))) ? ((created + 1) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("a7a3a600-1c26-3048-9c2f-7b88c14badfa"))) ? ((created + 1) >= (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("c1681f64-034a-334e-9b95-f0e7fd696ce1"))) ? ((created) > (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("90199896-a2ef-328a-a71a-100864f47acc"))) ? ((created) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("960ef317-8d75-31b5-ab95-f334e7298293"))) ? ((created) >= (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("dda784fe-5760-3deb-8e1b-d5f455414dfd"))) ? ((created + 1) != (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("eac2c927-035c-34b5-923c-64fa3e6bb085"))) ? ((created + 1) < (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("7149625b-eaaf-3cd0-918c-53e775e69f38"))) ? ((created + 1) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("80e741c1-9b03-350c-ba09-0f3786c0785a"))) ? ((created + 1) == (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("2f74bdd4-973c-3a75-960b-c5a91825ebbc"))) ? ((created) != (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("cc0ba304-9d47-3305-bc2a-e05a0ddbd39a"))) ? ((created + 1) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("5d12693e-d6dc-31df-86b0-9fe5af81bea8"))) ? ((created) <= (this.maxCount)) : (((KnobRuntime.check(java.util.UUID.fromString("667f1536-1e28-383a-ace3-2fa151a9ebd7"))) ? ((created) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("566a24e5-3914-3e98-b2f1-dd5f9f445dcb"))) ? ((created) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("02544ea7-817e-321f-8a28-4f45eb1ae505"))) ? ((created) < (this.maxCount)) : (created < this.maxCount))))))))))))))))))))))))))))))))))))))))))))))))) {
            if (((KnobRuntime.check(java.util.UUID.fromString("cfb2efce-1a3f-3a37-998b-d4a4d5e7a70e"))) ? (this.chunkCount.compareAndSet(created + 1, created + 1)) : (((KnobRuntime.check(java.util.UUID.fromString("b0b8b80b-9921-3efc-a0c0-884968143737"))) ? (this.chunkCount.compareAndSet(created, (created + 1) + (1))) : (((KnobRuntime.check(java.util.UUID.fromString("d4aae1bd-0358-3802-bd6a-025d92eadc61"))) ? (this.chunkCount.compareAndSet(created, (created) + (1))) : (((KnobRuntime.check(java.util.UUID.fromString("6edbb0a1-adf3-37bb-8438-7f54fd73d5cc"))) ? (this.chunkCount.compareAndSet(created, created)) : (this.chunkCount.compareAndSet(created, created + 1)))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("51a6309f-bc28-3bfa-926c-88598e3575ba"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13abe36f-3dc9-3d7a-ac7f-6da080c9d4cc"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4b92966-dedd-39de-85dc-1ec0521f79fe"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c49a2156-1a95-395a-aa57-873313654820"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f48cc7aa-56b4-3e0b-b16a-4b453bcc18c5"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38c01fff-2f34-36f4-b0b5-1490e246520f"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e939cc02-7f07-3c88-aafa-f874c3cf3fd1"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1bf8254d-c52c-3f7f-b42a-d14a2ce98f3d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b341c99f-aa75-39e9-8d64-1212b50111c9"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      int toAdd = this.maxCount - reclaimedChunks.size();
      if (((KnobRuntime.check(java.util.UUID.fromString("a8304da2-cd43-3da1-bee5-99d865fc9451"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1d737e99-bc7b-33f4-b84e-b2a70f753600"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("51c83235-1d36-35ad-ba9f-63f0b3fb3581"))) ? ((c.isFromPool()) || ((c.size) == (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("aa10815f-b120-3fe7-840f-c22d4c4da190"))) ? (c.isFromPool()) : (((KnobRuntime.check(java.util.UUID.fromString("788626da-55f1-3d68-a560-db9bbd0aced7"))) ? ((c.isFromPool() && c.size == chunkSize) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d352fcc4-49f4-3ba1-87d6-64a346b99075"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ce557f88-5f4c-33ad-8d50-3da454c5baaa"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f1c50021-daa1-3885-b943-622bd3a9fa76"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("27bb1f60-1c0e-3d3f-a5e8-51e3a46bd19b"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7ad116a1-da5b-3ba6-a83e-8503eee0918d"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f6e0b2c6-955f-3b0c-9963-dd1e90ae5820"))) ? ((c.size == chunkSize) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf2d44b5-19ca-3ef0-b194-28162e7b723d"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c7fd7d63-52d0-3c45-8c7c-76459babcb10"))) ? (((c.size) == (chunkSize)) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ead7f2bf-2baa-3dc0-b50e-407b6bcae05e"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dfb9ab21-11dd-3f1f-be02-9caf7af25c4b"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b61832a1-d014-3c9d-a7a7-106a6b9b3783"))) ? (((c.size) != (chunkSize)) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eed61e06-a3c6-3c0e-9f3d-3cfd530ad524"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("13b3fe6a-e17c-32e2-8404-d64b6f7f0bd3"))) ? ((c.size == chunkSize) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1c6ceb17-cde4-33d4-8ca8-98e7b0e41caf"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2301c2db-1845-3c35-9251-955937c2bf39"))) ? ((c.size == chunkSize) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f961c427-ec65-3cb3-8c55-35b5e5b14556"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("36a1df3f-8568-309c-b4f3-0fbb4f082bee"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("304e0c63-6be4-3396-af00-fd2d7161be67"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1d60d311-452f-3a9f-8601-5f668e015d79"))) ? ((isOffheap()) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7d774af4-ea8d-331e-98eb-78b932a9a077"))) ? ((c.size == chunkSize) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("710a8c67-9cc0-33d6-a936-3bf3394eda75"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("cbd8f6d8-e74f-382d-8d61-b1767698d5fc"))) ? (((c.size) != (chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91f1e1c8-1c7d-3e27-879e-bb0542299c61"))) ? (((c.isFromPool()) && (c.size == chunkSize)) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f521b1d1-f861-331b-871d-20ce4cb0e03f"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("647357da-715e-3eb1-8cc9-5d42354b8301"))) ? ((toAdd) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8b5c5662-53a5-3c98-8b37-cb0cb20b6f25"))) ? ((toAdd) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6a891e9f-28fb-348f-8900-560f49eaba95"))) ? (((isOffheap()) || (c.size == chunkSize)) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("80db3d7e-bfc1-32f8-a06a-bc066ab43a67"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("06010d97-b876-37a4-ae3c-b2d535678f72"))) ? (((c.isFromPool()) && (c.size == chunkSize)) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("40584b22-6b0b-3b70-8eb5-024f97e00bfa"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("16d51c3b-d3cd-3299-91ff-a55805ed7baa"))) ? ((toAdd) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("303bb36d-3f4f-31f4-94a1-1269492ae3e0"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8d629e70-53ac-3982-9eb5-e18a9472ff97"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("150f5153-d424-3767-ba3c-6ea4d57d9582"))) ? ((c.isFromPool() && c.size == chunkSize) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e4cbd3f-ea93-3a70-97cf-c88184f8c339"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b1e9fd15-514d-33f2-ac95-6c933147bb6a"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("775ef892-e0b0-35aa-89f1-cfe1c2dcab3f"))) ? ((c.isFromPool()) && ((c.size) == (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("8362c895-7ca1-3478-9077-907b199d83ee"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e7eee0ac-7c67-3d47-9ba5-72290bbae85e"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cbca9f0a-75f2-38ad-bb74-b1f1cc3517cd"))) ? ((c.isFromPool()) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d265526b-7dc6-3771-b3f9-03cbde533ef6"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2383bd2d-8221-323f-945e-54fe7cd99141"))) ? ((c.size == chunkSize) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("94609340-2055-3739-957b-0c611e6b01fa"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e78af4de-7543-3a2b-9b88-5a319e8ada14"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6557cd35-2cec-3fe0-8c4e-d040f3cb254a"))) ? ((isOffheap()) || ((c.size) == (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("21556554-a5bf-3a5e-9987-4a161e9faef2"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dbc27029-528c-3d57-b8a9-e5d4b0d21397"))) ? (((c.isFromPool()) && (c.size == chunkSize)) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7dae3688-5caf-3de8-8558-81d0e7944e87"))) ? ((c.isFromPool()) || ((c.size) != (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("5c4510fa-d90b-3f31-bad1-ee7d142c5881"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3ad96383-912a-37af-a43f-c561f34879c4"))) ? ((isOffheap()) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b621f561-590c-3e13-95db-ebf397d98b6f"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39ff0469-a60b-3ba3-af91-dc417149caa6"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e2634728-c040-3845-93dd-d422dd8f18a5"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b014f904-d224-3884-a631-f42e8e4de9fa"))) ? ((c.size == chunkSize) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0ef5ea74-3071-3d25-a659-bb7f016ef3a0"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0582321f-4380-3d79-9c34-466928584d32"))) ? ((c.isFromPool() && c.size == chunkSize) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("20d68213-82ea-3139-982a-7e55899cf21c"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7708b27a-ae28-365f-a97a-eb67303bb8b9"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d62c705-397a-3618-8b3a-a9650a9b91d6"))) ? (((c.size) == (chunkSize)) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f3a4f3dc-8b98-3e1c-93ea-8f072e0797f1"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e93824ee-d6c6-3c4d-a327-596520c8019a"))) ? (((isOffheap()) && (c.size == chunkSize)) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("66931655-130f-3a83-8ee8-9586c0ff702b"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("523a8c9b-618c-321c-89be-a12c56b3efad"))) ? ((c.isFromPool()) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4cb11c64-d7fa-3edb-9f08-800654e5ad15"))) ? ((c.isFromPool() && c.size == chunkSize) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ab7acd8d-7531-3643-9a04-64dfa2faa6fc"))) ? ((c.isFromPool()) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0da09edf-ebc4-38c2-9391-c55e148b9a41"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dadc292c-8563-3b8b-a9a1-882d395d10c6"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ead1377-1e1d-3008-a73c-4bdcc88df7f3"))) ? ((c.size == chunkSize) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3804ae9a-be36-3042-a0b3-b91b607280d4"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a75baced-b094-3417-aefd-8a4e213c1e71"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c53c87d8-56c1-32fc-a9c5-7c0c67265523"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f5de312b-2585-3f5b-af25-66d48d756ef1"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6627c069-5ed6-3a97-83d1-5514669b9c9b"))) ? (((isOffheap()) || (c.size == chunkSize)) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5da765f1-a404-35c8-bbe7-5b7dbdabfe0c"))) ? (isOffheap()) : (((KnobRuntime.check(java.util.UUID.fromString("c3ff175a-f1f8-3ec5-a9df-87e10fe1047a"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca3ff238-f66d-3254-ac52-d2d4308da84c"))) ? (((c.size) == (chunkSize)) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7763bdc6-a717-3efe-9b9d-f6ca4bdd426a"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3cfc26ec-5e14-31a4-96c0-aca3a61dbbe7"))) ? ((c.size == chunkSize) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d45ec41e-fa7a-3dc0-ab01-1c9cb5799a0b"))) ? (((c.size) != (chunkSize)) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("597bdcc3-4ea0-3ba6-bf88-d50988b9d634"))) ? (c.isFromPool() && c.size == chunkSize) : (((KnobRuntime.check(java.util.UUID.fromString("241cf615-161f-38d6-b047-0038c1056f75"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3e232456-b3dc-3ebd-855b-78fcf370a8fc"))) ? (((c.size) != (chunkSize)) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8e76bf78-2aaf-3310-8459-360d73e29d6c"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7c2377b4-6b34-321a-bb44-a112a3455c5f"))) ? ((c.isFromPool() && c.size == chunkSize) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("270189a7-d0ec-34d5-b085-a468238fdde8"))) ? ((isOffheap()) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("79230166-2fa0-3362-b11c-f3d647b5ace5"))) ? (((isOffheap()) || (c.size == chunkSize)) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("58fbbcdb-c715-32d0-ae53-f7d04d39d729"))) ? ((c.size == chunkSize) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("92df26c7-6d03-3a81-bec3-5f4cf8ff2a80"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ddc76e82-31b3-3a27-9a66-1f8373a458d1"))) ? ((c.isFromPool()) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ff5f5b96-88f3-344e-876e-20dbf20fabcb"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6378e13e-1627-3aa6-b622-2d29d948eb5c"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1274a509-42b2-3990-a97f-7230c4a84245"))) ? (((isOffheap()) && (c.size == chunkSize)) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c182861c-558c-341a-8e7d-33169b9bf22e"))) ? ((isOffheap()) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac72ef22-6aa4-3b90-b38a-ae7ce0926f7f"))) ? ((c.isFromPool()) && (c.size == chunkSize)) : (((KnobRuntime.check(java.util.UUID.fromString("a4f4c6d4-8775-38e9-8286-bfc65956c82f"))) ? (((isOffheap()) || (c.size == chunkSize)) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("513cdea6-d4cb-3887-8986-320e88f89297"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1aed71ce-069a-3b9e-a751-4beb8941bffe"))) ? (((c.size) != (chunkSize)) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cbfa62ed-3a23-359a-91a1-31ca5341cfad"))) ? (((c.size) == (chunkSize)) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c3c32728-1759-3f83-8f77-4c9ddaf5a9b6"))) ? (((c.isFromPool()) && (c.size == chunkSize)) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d6e3b2f4-ff31-3c76-91ba-71a51a9d2800"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5fec3c60-453c-313a-be1e-e999ddf84fbe"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a47ef0ec-0623-3e62-88ef-3f8cb90d914f"))) ? ((isOffheap()) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c2a28cb-2aa2-3002-ac5a-198c95c563f9"))) ? (((isOffheap()) || (c.size == chunkSize)) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("bb424e51-2cda-3060-b94f-a72d76accf19"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ed526e26-4e09-314d-a5a9-3d3de67df20e"))) ? ((isOffheap()) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2984617e-b068-3d74-b3b3-db647a692b8f"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("95c39a0e-a6d7-33af-9778-1ca47485d6e1"))) ? (((c.size) != (chunkSize)) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f08d9f3-0bcb-3869-b33e-be008bc80753"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b47c113e-eb98-3ee4-afda-de1042634e42"))) ? (((c.size) == (chunkSize)) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d5883407-26e6-348d-a9b7-d0915afceef2"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e31a928e-1dc0-3d68-bc95-19eca078249d"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a20e302d-efa4-3527-90db-ccd56bb75d1c"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("71e5e0f6-dc3a-3fd4-b654-18bab790556b"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c35ced6-f5e2-308b-92fa-a12e55f6a578"))) ? ((c.isFromPool() && c.size == chunkSize) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3df0fde7-7ecf-3d50-be53-006e9aca6f4c"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c98d715-643d-3934-9259-0da5181b265c"))) ? (((c.size) == (chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bc64860c-d1f0-3d02-957c-7250a65c9593"))) ? ((isOffheap()) && ((c.size) == (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("22ecc35a-8885-3aa4-80f8-ac777552def6"))) ? (((isOffheap()) && (c.size == chunkSize)) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("623ab75a-7c81-3960-8add-3124e24ecc0f"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9e665bc6-61bf-3c97-a891-7c3a575a55b4"))) ? ((c.isFromPool()) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a75bcabf-8dd4-3377-bab3-1870c04cba96"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ae33bd72-25bc-3090-8ce2-88a99d7bc9ce"))) ? ((c.isFromPool()) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1dc00917-8940-3222-8989-bb7e1240d554"))) ? (((c.isFromPool()) && (c.size == chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91ebeaa1-d7b5-3e03-91d3-0899f51b4c66"))) ? ((c.isFromPool()) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38c93517-fc69-37aa-9502-5b7f41ddd29f"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("379771de-df5d-3180-8d26-17d518635182"))) ? ((c.size == chunkSize) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a4c2330e-b198-37f4-94fb-6bbfe8c3df3a"))) ? ((c.size == chunkSize) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e7024115-9467-375c-bf20-765cb7894b9f"))) ? (((isOffheap()) && (c.size == chunkSize)) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("789429b7-50b7-34cc-85bd-a72b5646f06d"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ecb050c5-e141-3e35-956e-162988c889f7"))) ? (((isOffheap()) || (c.size == chunkSize)) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b3b90745-c1cf-3c69-8d2b-34d55f96518c"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0c6ce9e6-8541-3c29-97e8-cf14e6ec711a"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("adc515bc-88e7-3757-afc4-9501f26161f7"))) ? ((c.isFromPool()) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("71e352b1-2bc2-348e-86ad-97e3f2f89c2d"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3eaccb1e-4594-38d7-ac15-4827a25abcc2"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b373f33c-89f7-3250-ac71-f151ed8758b5"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c73780bb-2b21-3290-994e-f7b6d292100b"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6fd32f82-8207-3102-b86d-108e9d616011"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("323026ae-4d0a-38a8-aa7e-1059188a24b0"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("324d701f-34a6-30dc-894e-6c1a491330cd"))) ? ((c.isFromPool() && c.size == chunkSize) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e8b9aaab-7bb8-3691-84be-22c5d3f6f638"))) ? ((isOffheap()) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8c78284-31d3-3279-b9f1-3f41f8473f5d"))) ? (((c.size) != (chunkSize)) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ada4b493-f277-3169-9676-48c3f9914d68"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ebbcce89-9513-316c-b0ba-5006cb418809"))) ? (((isOffheap()) && (c.size == chunkSize)) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("be84408e-c189-389c-9a32-b83a918be95a"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3baad67f-56de-3d94-a812-17a9db77d860"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d7c90a8b-8b75-3c6d-a1da-7dffcdb2ee53"))) ? ((c.isFromPool() && c.size == chunkSize) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("79ce9cb5-8f9c-3cc3-aaae-e5f94781e469"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("406327a8-2341-3ae4-8a1b-46dd9bd8a59c"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("20a2c106-9db4-3263-a47f-bc16a90140f1"))) ? ((c.isFromPool()) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f1be68ce-f3d7-3322-89bf-705516ccba0c"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("759b6b67-20ba-3515-9867-5f56433b8095"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("79fcdf5f-6fb1-3e39-9cc7-9a20ec6fec94"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c0104bb6-2aeb-36d9-ae3a-be2f293803c9"))) ? (((isOffheap()) && (c.size == chunkSize)) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("da778eaf-36db-3bdf-8416-006a58c7f096"))) ? (((isOffheap()) || (c.size == chunkSize)) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ad02b0d-07eb-3162-a12e-07b349337488"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("18b87b75-b0f6-3ec6-abdc-a6774b47dc8c"))) ? (((c.size) == (chunkSize)) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d8e193f6-710b-3ecd-acb6-4f3884d8037f"))) ? (((c.isFromPool()) && (c.size == chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dca728d7-784a-356c-9dcf-7c335a28b1e7"))) ? (((c.size) != (chunkSize)) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9cf0f2d3-1eea-3334-a814-bfa617d3210a"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ee66dfae-2720-3bbe-873c-dab0a267837d"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a7e78b8d-c15f-3955-b7d3-8b5a84f86ce8"))) ? ((c.size) == (chunkSize)) : (((KnobRuntime.check(java.util.UUID.fromString("33fec461-1207-3592-945a-cf7d7c718c89"))) ? (((isOffheap()) || (c.size == chunkSize)) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f7ac87f4-e22d-37ff-a2bb-1f9646029811"))) ? ((c.isFromPool() && c.size == chunkSize) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("53a954ba-2c8f-3842-9e50-c2082513c419"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("72ee7b21-ced2-33ba-aa8f-3c16891a9195"))) ? (((c.isFromPool()) && (c.size == chunkSize)) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a9414df5-019b-33e3-906b-8a98383d4e9a"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("29c60d4e-d16a-3041-bb8c-85c341db4fe6"))) ? ((isOffheap()) && ((c.size) != (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("f8243e5a-6c28-3079-a447-ff8c3f4adf09"))) ? ((c.isFromPool()) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fe40cee7-8292-392a-a140-7c4ead4cd7aa"))) ? ((c.size == chunkSize) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7557f927-16ca-3989-a625-c77d8381c65f"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("47a607f1-e222-3f94-b6ea-19197b8e3f5e"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de7ae37a-f481-3823-a216-3bb1bfe09709"))) ? ((isOffheap()) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7369a52d-28ee-3705-8516-a645e6127cfe"))) ? (((isOffheap()) || (c.size == chunkSize)) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("72a674ca-cdad-3c6d-983c-0cbd8a57be5b"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52cce02d-27bb-3db9-a043-2c1a1cbe4d78"))) ? ((c.isFromPool() && c.size == chunkSize) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e8d2598c-1a28-3eb6-87e7-00f2186accec"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d9e642b2-59be-3a7a-9195-57290bece4b9"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2bf6a636-99e0-3721-b0d5-652e339f5cbd"))) ? ((c.size == chunkSize) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f5ab1582-1b81-3c44-baf6-82427d72e34c"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a171d782-7e31-31a3-9f1d-003c76824477"))) ? ((c.isFromPool() && c.size == chunkSize) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("06e51147-0e3b-3576-8b32-56853e947c69"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7d27fc2c-c77d-3535-bffd-2c12b9953382"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("25317378-11fb-30f4-aafc-dd33aecfdf2f"))) ? ((toAdd) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9a573a65-6072-36c5-a1da-5e0d9184f4f3"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("942dbb6d-6da2-35fa-a6da-6a022ba2e95a"))) ? (((c.size) == (chunkSize)) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0528f294-55a1-389b-ae75-8a85e133d426"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("830ec91d-d9d4-33d9-8abc-93213bea9176"))) ? (((c.isFromPool()) && (c.size == chunkSize)) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ba3cbd63-3826-35d9-86ca-0640b5cf8f75"))) ? (((c.size) == (chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc4522d1-cb32-3adb-aa72-a79424ed741f"))) ? (((c.size) == (chunkSize)) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b07fc25f-504d-3341-af13-2360f4c4a67a"))) ? (((isOffheap()) || (c.size == chunkSize)) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8477e7aa-6a9e-354d-a62a-0d942cf328a2"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d64c401b-b481-3053-b1c8-6c0dbd33e334"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("41207387-41ca-3d01-a076-e7aa7c638dda"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cfb1e9c4-47c0-341c-ad1c-918fbe3787c3"))) ? (((c.size) == (chunkSize)) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1d0893fd-47ab-3719-b7d5-fea96320cfd3"))) ? ((isOffheap()) || (c.size == chunkSize)) : (((KnobRuntime.check(java.util.UUID.fromString("bb351d67-cae9-3251-86ae-99e20f912489"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ba5cae90-c2a0-3d0a-bc05-1c8a5e462fcf"))) ? (((isOffheap()) || (c.size == chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0a9264ed-f820-3dfb-8789-51391ab6982b"))) ? ((isOffheap()) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1343915b-bbb3-3b3b-9ffc-488c0607c8a8"))) ? ((c.size == chunkSize) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e3f060ea-d510-38a9-be5a-156427b28b49"))) ? (((c.size) != (chunkSize)) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c58a016-56fd-3dfb-9882-67f1ca93425e"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6be27918-fce0-3247-a56b-d4ddb8403b8b"))) ? (((c.size) != (chunkSize)) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("dd23db15-ab88-3831-bf4e-0e3cf1c57d6a"))) ? ((c.isFromPool()) || (c.size == chunkSize)) : (((KnobRuntime.check(java.util.UUID.fromString("837fc36a-9039-3fbf-8eec-281e050cedac"))) ? (((isOffheap()) || (c.size == chunkSize)) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a954b3c9-945f-3409-91cd-ee46884f20ea"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("490f6be0-0179-32d1-82f6-06af3000d7b8"))) ? ((isOffheap()) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("99aef8a4-0ae0-326c-a917-8c008811f9b5"))) ? (((c.size) == (chunkSize)) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e24c98bb-51c3-348c-8c70-fff04fade549"))) ? ((c.isFromPool()) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a6e25cbb-0c83-3bda-a9f0-93e97e62b8eb"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b1c3025b-c4a5-3707-a2a7-1295b6fe418e"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("f6dbc3a0-a4a8-3ad7-a8fc-7fb61271620f"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6512ba2f-f8d8-3ae9-beec-2cace19eb26d"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c4deb16b-e887-3326-925b-25ee9cfb7f2c"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5ba27d3b-ce6d-376c-aef2-489b597696a7"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("57834da8-3097-3795-8e1c-e1078a5dff0b"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("655b0968-8d81-333b-befc-29b124d17566"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cee5cc30-a13f-3df3-b1f1-57cfca665852"))) ? (((isOffheap()) && (c.size == chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("796817cf-76d4-3ac7-8c3f-77865c84dda3"))) ? ((c.isFromPool() && c.size == chunkSize) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("89417072-22a3-311f-8e75-58a3ce2bbdc0"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("baffc719-df59-3a3f-9db5-d0d217aa993b"))) ? (((c.isFromPool()) && (c.size == chunkSize)) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("07d1dac4-1e8f-3a2f-93c3-d542ee05874a"))) ? (toAdd > 0) : (((KnobRuntime.check(java.util.UUID.fromString("1c833cfd-9808-3f78-b4e2-b5b7800d74a3"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a9316c64-c169-3c59-be2e-95efbc08f1a1"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("773f18f0-59ec-3318-a27a-8cd02ee185e1"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2e6f5a67-2acf-3ede-acb3-74a93760be7b"))) ? (((isOffheap()) && (c.size == chunkSize)) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7f05232c-6e33-3451-b68b-fe4f328e1d73"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("5cfab713-b842-3e79-a51e-feff0aa5882e"))) ? ((c.isFromPool()) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("1bcf6c65-58ce-3034-9976-5a385fbc56fc"))) ? ((c.isFromPool() && c.size == chunkSize) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("01bebb09-a091-35e8-9967-acd52a2608b7"))) ? (((c.isFromPool()) && (c.size == chunkSize)) && ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("168e40f0-a650-3e0c-925d-ab7345f4978d"))) ? ((isOffheap()) && ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f6ea9cd6-f48e-3b72-835f-f00c9556fab1"))) ? ((toAdd) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("69a3923c-d354-3e6e-922b-bbc972646cff"))) ? (((c.size) == (chunkSize)) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("376d5e32-5630-3d7c-b6d4-5abe6a5ab375"))) ? ((c.isFromPool()) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c6c35ab-e9cc-34de-8cd4-9837e852b1ee"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b74612cb-1c0f-38a3-b4a6-022fb1c6149f"))) ? ((c.isFromPool()) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("530d5520-c5c2-3af0-a6e5-50fb4a735398"))) ? (((isOffheap()) || (c.size == chunkSize)) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("79717bd7-0ea6-3dc3-8c1f-726287491ae4"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("33c03d9c-6ddf-383e-b2d9-57270c6285d5"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d08a009e-40bf-376d-aa85-86451a853806"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("979f886a-0e15-36fb-a764-cb288335be3c"))) ? ((c.isFromPool() && c.size == chunkSize) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ef95b84a-d3c5-3351-ba5b-3dd32aaceb38"))) ? (((isOffheap()) && ((c.size) == (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2952b29-5405-3654-bf83-fd1da7ce47c2"))) ? (((c.size) != (chunkSize)) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("492dfa57-a0e1-30f4-a914-5d33e93b34ee"))) ? (((c.size) != (chunkSize)) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4f422cec-5885-34ff-8750-173f225f1e85"))) ? (((isOffheap()) && (c.size == chunkSize)) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc0439b2-4046-3cdf-8a83-d253e2a37724"))) ? ((isOffheap()) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b4a33094-fd3c-3e06-9884-c8f90382bd0f"))) ? ((isOffheap()) || ((c.size) != (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("4ba93e53-e5ed-3028-aead-7ab20593ea95"))) ? ((toAdd) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8c21e02d-3550-360a-bb89-e56a053fe896"))) ? ((isOffheap()) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("98c3bc07-02ec-3307-91cf-4fec20583da0"))) ? (((isOffheap()) || (c.size == chunkSize)) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("02c873a3-8690-3d9b-8bf0-18bd217a5628"))) ? (((c.isFromPool()) && (c.size == chunkSize)) && ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c6f9f9a8-0e61-3501-a7d1-fff70c435108"))) ? ((c.isFromPool()) && ((c.size) != (chunkSize))) : (((KnobRuntime.check(java.util.UUID.fromString("acd4446e-421b-3379-82ba-f8cc8d7a9e85"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) || ((toAdd) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("61b4e504-1d0f-36fc-a7f8-579cd62fbfdc"))) ? ((isOffheap()) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("0f8892ab-9049-3146-bd42-9198a37d046d"))) ? (c.size == chunkSize) : (((KnobRuntime.check(java.util.UUID.fromString("6737f3e8-8e13-370e-a9ff-42f49d531a07"))) ? (((c.isFromPool()) || (c.size == chunkSize)) && ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac3ae66e-1d8c-3367-ac84-b2a303374884"))) ? (((c.isFromPool()) && (c.size == chunkSize)) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8008665d-24b5-3f65-a617-d669448dfc6a"))) ? (((c.isFromPool()) && ((c.size) != (chunkSize))) && ((toAdd) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7e56dc1a-dcdf-32f4-b795-1b5991a3b606"))) ? (((c.isFromPool()) || ((c.size) == (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b71975a9-5902-3728-bea1-84100f64acb8"))) ? (((isOffheap()) || ((c.size) == (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f7bc344d-2112-33aa-8002-db5d1bbe4afc"))) ? (((c.size) != (chunkSize)) || ((toAdd) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a4f14c3d-15e2-3b9d-bb23-a355ef8c2deb"))) ? (((c.isFromPool()) && ((c.size) == (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d9c66178-1710-3cb4-aa28-f51f46dc2594"))) ? (((c.isFromPool()) && (c.size == chunkSize)) || ((toAdd) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e8d3723a-9409-358d-ad6d-764992e70978"))) ? (((c.size) != (chunkSize)) && ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d52f8b4-9627-3754-825e-c7c06b633ee4"))) ? (((c.isFromPool()) || ((c.size) != (chunkSize))) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("139e40ab-629d-3406-a5cf-6d62ec65e601"))) ? ((isOffheap()) && (c.size == chunkSize)) : (((KnobRuntime.check(java.util.UUID.fromString("b697ce28-c8c4-3495-8d72-c85c5c722eff"))) ? (((c.isFromPool()) && (c.size == chunkSize)) || ((toAdd) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e5bcc383-c5ee-317f-8237-e2f31ce3592a"))) ? (((c.isFromPool()) || (c.size == chunkSize)) || (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2efb55ec-08cc-3492-bca4-6cf4dac771df"))) ? (((isOffheap()) || ((c.size) != (chunkSize))) || ((toAdd) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f68ceac7-29ff-3432-b6a3-5f9ccaaa205d"))) ? ((c.size) != (chunkSize)) : (((KnobRuntime.check(java.util.UUID.fromString("22271e1d-443f-3aad-8884-a1b058a668fe"))) ? (((isOffheap()) && ((c.size) != (chunkSize))) && (toAdd > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("31c5dacb-d35e-3ae4-a3b6-b41525f9f04f"))) ? (((c.size) == (chunkSize)) && ((toAdd) >= (0))) : (c.isFromPool() && c.size == chunkSize && toAdd > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("9dd68295-6882-3d56-988e-4128eed6a870"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8feecffc-2fc0-372c-b1a2-6d03e575e3b7"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c85969a4-a1af-3ae9-8a58-7330e794c536"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d6b6889-2476-33c4-8daf-81e2b5429ae0"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ee7b87b-3e6e-3988-84ee-9ed614229187"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5e96ed0-5153-322d-b9e5-9f3130c50844"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cee20e89-a566-3ac2-9604-d5647f55bf48"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45a26ac9-0f92-30fe-829f-d7f10328ec86"))) {
try {
    java.lang.reflect.Field field = c.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(c));
    field.set(c, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
if(KnobRuntime.check(java.util.UUID.fromString("c3b78299-ffdc-3cd9-b88c-23f06af9e437"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20b926a2-22f4-397e-b32b-3822e6bd2ba3"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27d43e05-5a85-36fd-a81b-953fea0c0600"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9ffed1f1-82be-3420-9753-5ad1ccc26f34"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("maxHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a804a29f-62a1-363e-952b-605727ab2a72"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("maxHeapSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5712a7bd-61ea-3af8-8b8a-2414f9cc0f1a"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4eaf4e2-9024-3a66-88d6-c2c5d1bbdb27"))) {
try {
    java.lang.reflect.Field field = heapMemoryManager.getClass().getDeclaredField("defaultChorePeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(heapMemoryManager));
    field.set(heapMemoryManager, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("42985365-d878-353e-aea8-049dedc1ed1c"))) ? ((poolSizePercentage) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2eb0096c-225c-3dbb-a953-99acbef820fe"))) ? ((poolSizePercentage) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("17f7ec28-a874-30ca-b1e7-8955d276af6b"))) ? ((poolSizePercentage) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2d8d556c-008b-3d0a-bc06-d8ecba729ffc"))) ? ((poolSizePercentage) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("80d0a772-d554-3077-868f-049d837f9354"))) ? ((poolSizePercentage) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6e772602-70c8-3940-97aa-16f017828381"))) ? ((poolSizePercentage) != (0)) : (poolSizePercentage <= 0))))))))))))) {
      LOG.info("{} poolSizePercentage is less than 0. So not using pool", label);
      return null;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("996588dc-a830-38d6-95fe-87281dd751d0"))) ? (!chunkPoolDisabled) : (chunkPoolDisabled))) {
      return null;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("7f44f218-8a0d-31cd-ae26-0acba9124c3f"))) ? ((poolSizePercentage) > (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("e298279b-403b-331c-9fd7-819c2f353277"))) ? ((poolSizePercentage) == (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("53206f0a-12b8-37e2-9670-2621b60f8daf"))) ? ((poolSizePercentage) >= (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("97af73a6-b0bc-388b-9b54-b1f4f0a18c91"))) ? ((poolSizePercentage) < (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("fd604a77-6761-3f44-9621-56b83295cd92"))) ? ((poolSizePercentage) <= (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("ff050427-ce00-383b-8506-065d7d8164b6"))) ? ((poolSizePercentage) != (1.0)) : (poolSizePercentage > 1.0))))))))))))) {
      throw new IllegalArgumentException(
        MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int maxCount = (int) (globalMemStoreSize * poolSizePercentage / chunkSize);
    if (((KnobRuntime.check(java.util.UUID.fromString("907465eb-25d4-33f5-baa9-f7e679e08b95"))) ? (((initialCountPercentage) <= (1.0)) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("39a1d157-ac84-3d90-ab2c-9ade49eb11b7"))) ? ((initialCountPercentage > 1.0) || ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0d62e857-04d4-318b-a23c-92aa542bce6a"))) ? (((initialCountPercentage) > (1.0)) || (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ca8b3150-f945-3a76-bd0d-6330a2ba24f4"))) ? (((initialCountPercentage) <= (1.0)) || ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b32d7d22-b491-30f0-874f-8d0d7816e01f"))) ? (((initialCountPercentage) == (1.0)) && ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2ad8488f-6e7c-3207-98ba-b3edbd84e7c6"))) ? (((initialCountPercentage) == (1.0)) || ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7c519b04-5b85-3ca5-af5b-575701404673"))) ? ((initialCountPercentage > 1.0) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86b18f5e-fd5f-323b-aadb-dc03fc8d53a2"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cefe3054-503a-316e-aeb2-e3309ef7b779"))) ? (((initialCountPercentage) == (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d231fd64-b01b-3f40-b4da-0207c1c84f1f"))) ? (((initialCountPercentage) > (1.0)) || ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d6c71926-1d3f-3117-ad63-45fd4da38075"))) ? (((initialCountPercentage) != (1.0)) || ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("449f2237-32a2-3ec3-8da9-2fbd00890ed0"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bd62ee45-6f3c-3e99-9e40-6cd914c0a2b1"))) ? ((initialCountPercentage > 1.0) || ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b027d5b4-8263-3120-9260-4a773c3e65c1"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("edcb3247-b7e9-3f9d-91b3-4f5fa87a8cbd"))) ? (((initialCountPercentage) >= (1.0)) || ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aea17ad8-18dd-39b3-9dab-44589c53f77a"))) ? ((initialCountPercentage > 1.0) && ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("962f36d6-5bfb-313c-b9a8-f5bea02d3fce"))) ? ((initialCountPercentage > 1.0) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("28f4f4ef-2f22-3204-ad92-304fcf1d3522"))) ? ((initialCountPercentage) > (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("e66963eb-54b9-3f74-967c-b704fc89d6d6"))) ? (((initialCountPercentage) < (1.0)) || (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b91f72a0-37b4-30f0-b009-a286becfde15"))) ? (((initialCountPercentage) >= (1.0)) || ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4f9c9213-1f87-34d9-adc1-7acbbdd26272"))) ? ((initialCountPercentage) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a4d022d0-c469-31ca-8c43-b6ac58d024f3"))) ? (((initialCountPercentage) < (1.0)) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("34486bcb-12ae-3dc8-8062-b1e402438c6c"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("59d6f0eb-f72a-394b-b119-505611f239a6"))) ? (((initialCountPercentage) == (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dd52ae20-c87e-39f8-a1e5-ddfeb163217f"))) ? (((initialCountPercentage) >= (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6f32847d-b038-3c3d-90b6-5adda0fe00fe"))) ? ((initialCountPercentage) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2609997d-4ae9-3bbd-8670-8c65f6aa645d"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02dc1095-9de7-31b9-9c31-154f89e3aad4"))) ? (((initialCountPercentage) > (1.0)) || ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52ac7405-d083-3ce3-979e-9f14d104c78e"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("56050fc2-2612-324f-99b5-4e18ebcb8a5b"))) ? (((initialCountPercentage) == (1.0)) || ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ea427c3-0579-33fa-a995-d19175d2ef5c"))) ? (((initialCountPercentage) < (1.0)) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9734d6ef-7c9e-3981-bb91-38785aa7f640"))) ? (((initialCountPercentage) != (1.0)) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8951cde9-4a3f-3858-b3cf-04700daf87f0"))) ? (((initialCountPercentage) != (1.0)) || ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("66c6c187-b7f1-3b60-815f-4b8eda479d57"))) ? (((initialCountPercentage) == (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("61e3668f-fdc9-3530-841c-76a58e75d00d"))) ? ((initialCountPercentage > 1.0) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("45f8dfca-1531-3543-91ae-9d5f878cd204"))) ? ((initialCountPercentage) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1f166f72-0e9d-3bb8-8907-f4159a7b78fa"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0a9d2920-2cff-310c-8245-5b17558ecb3d"))) ? (((initialCountPercentage) <= (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8583e7b0-6315-3abc-95c5-7ffff98a4371"))) ? ((initialCountPercentage > 1.0) && ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ebb8a2d3-e9da-3019-97b0-8c1a7d80aab7"))) ? ((initialCountPercentage) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e6e9b619-c7ef-3b67-8dfc-a27f9585e302"))) ? (((initialCountPercentage) > (1.0)) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("94049c2e-e1f7-3c59-8551-3287d0c3e8a3"))) ? (((initialCountPercentage) >= (1.0)) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6ce23439-9413-3d41-9b13-3c0727c1a280"))) ? (((initialCountPercentage) <= (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("57da47e5-9216-3cdc-a391-e19a3996f8d1"))) ? (((initialCountPercentage) < (1.0)) || ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("640a718d-6a81-357b-8291-4f692f6f5f29"))) ? (((initialCountPercentage) <= (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca2a0518-cf1a-31b3-9173-7bde141e8626"))) ? ((initialCountPercentage) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("18dbc146-b06a-3652-91a8-76a4b802b5cd"))) ? (initialCountPercentage > 1.0) : (((KnobRuntime.check(java.util.UUID.fromString("65efa631-1b55-3ba4-a57d-a3beedeb28ba"))) ? (((initialCountPercentage) > (1.0)) || ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dfaa6c5a-7d1c-30d2-90b9-84ebd98e3821"))) ? (((initialCountPercentage) <= (1.0)) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("43b34d53-6cb3-3bce-9512-954d9646e9e6"))) ? (((initialCountPercentage) <= (1.0)) && ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eaf1d4e5-e44c-3b56-944f-c68130cbca45"))) ? ((initialCountPercentage > 1.0) || (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d3dfe8eb-f624-30b8-98f9-6b91c1a6e01f"))) ? (((initialCountPercentage) >= (1.0)) && ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("55f7e66d-3d00-3e02-a390-bbbec4952beb"))) ? (((initialCountPercentage) != (1.0)) || (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3b0c5072-e941-3209-aac6-2e2fd968a94e"))) ? (initialCountPercentage < 0) : (((KnobRuntime.check(java.util.UUID.fromString("b3383689-496d-3adc-9c2e-10ce3f64e14f"))) ? (((initialCountPercentage) < (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ef88c44-c4ff-3634-90c4-71b300e82269"))) ? (((initialCountPercentage) == (1.0)) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f0049b72-6823-3cb7-ae74-80a0f6c1fbce"))) ? (((initialCountPercentage) == (1.0)) || ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8dfe20f7-ad62-3af0-b6fd-2c5e38814536"))) ? (((initialCountPercentage) < (1.0)) && ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("98c09758-4493-3112-821a-8a0e34dab159"))) ? (((initialCountPercentage) <= (1.0)) || ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("53008ce7-f281-3775-89d1-f4937169ca80"))) ? (((initialCountPercentage) < (1.0)) || ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cdddc45d-bd2f-36ea-8afa-805a8d5338a8"))) ? (((initialCountPercentage) >= (1.0)) && ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("acafe99e-3d10-35fc-aec5-05b1c28a8b3d"))) ? (((initialCountPercentage) >= (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bff23715-e31b-3c76-8b53-d34e5ca1cf2a"))) ? (((initialCountPercentage) < (1.0)) && ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9ba8768c-bbe0-3071-917a-2d1533c40984"))) ? ((initialCountPercentage > 1.0) || ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eb821be9-b43c-35be-a4dc-eed5d6a9d2f4"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c8186ef1-96ee-3c80-9baa-83e64913bd75"))) ? (((initialCountPercentage) == (1.0)) || (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("dd01d277-5e4f-3b6b-9409-8c13fe9ced19"))) ? (((initialCountPercentage) < (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a1726cc0-c5ba-3865-b410-63dde67c9b70"))) ? (((initialCountPercentage) > (1.0)) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a8c0217f-a3f4-3157-875d-7d9359adc342"))) ? (((initialCountPercentage) == (1.0)) && ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ce83a7bd-063d-35ce-98dc-fe2fbf7c9806"))) ? (((initialCountPercentage) >= (1.0)) || ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ce231a6a-51a5-346b-ac9c-5c9bb45adc6b"))) ? (((initialCountPercentage) <= (1.0)) || ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e2c5bd4c-0509-39c9-b958-27d0c38fc94d"))) ? (((initialCountPercentage) <= (1.0)) || ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b241210f-58db-3692-a946-f3f0514c75bd"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1b2e011f-4f16-386f-b152-337070d4e6de"))) ? ((initialCountPercentage) < (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("e0520945-8a28-301a-9b4d-0c028b467c8c"))) ? ((initialCountPercentage > 1.0) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("be27f038-a835-3449-841b-ec5e96d46c82"))) ? (((initialCountPercentage) != (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c7603316-78ad-39b6-bc10-1cca1c417257"))) ? ((initialCountPercentage > 1.0) || ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("764a72a2-3cbe-3e3b-a3c8-648a91b8526d"))) ? (((initialCountPercentage) <= (1.0)) && ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e9640473-d263-3d0f-95cf-934e55b35c6f"))) ? (((initialCountPercentage) < (1.0)) && ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("23d30e4f-caa9-3a73-b05b-15d3e7159d00"))) ? (((initialCountPercentage) >= (1.0)) && ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("665eded2-f22d-3d8a-8e6f-fcbef1c9ad5a"))) ? (((initialCountPercentage) >= (1.0)) || (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("246e8432-917b-3c6b-ae28-fc2eac9d58aa"))) ? ((initialCountPercentage) <= (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("2c096e7d-fd44-3f6c-af82-c3152ef68c5b"))) ? (((initialCountPercentage) <= (1.0)) && ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("94966265-196a-3588-87d2-00a09dbfb013"))) ? (((initialCountPercentage) > (1.0)) && ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("016c70c9-0ed9-3237-9a21-2218196f2c2d"))) ? (((initialCountPercentage) > (1.0)) || ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("16a0e30e-db62-3534-bca5-b5ea12a5b356"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1a08563d-bd50-3bda-a2be-f31b8d8454eb"))) ? ((initialCountPercentage) != (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("d3de4fdd-a52a-333c-acf4-af0175ba4ca7"))) ? (((initialCountPercentage) <= (1.0)) || (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("5dada56d-40ba-3416-8644-4d5c094f525b"))) ? ((initialCountPercentage > 1.0) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("387f6e7c-0036-3a27-8469-22bcc7588077"))) ? (((initialCountPercentage) <= (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("86b232d0-0ee0-3359-b9d6-f02ced8649fa"))) ? (((initialCountPercentage) == (1.0)) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("4ee7bd90-4a2f-3d5f-ad04-3dc127f6391f"))) ? ((initialCountPercentage > 1.0) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("dca0491f-e940-3e00-a0c2-fece139c8a17"))) ? (((initialCountPercentage) < (1.0)) && ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("09e14780-13c8-3532-a90e-312cc3d5753d"))) ? ((initialCountPercentage) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("41706ef5-db73-34ce-a6f5-08d7dbbc5644"))) ? (((initialCountPercentage) == (1.0)) && ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf54bde9-6618-3725-acf2-b3d3d9a16b9d"))) ? (((initialCountPercentage) < (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e19336e1-2802-3ed5-8da4-8e783e3c95be"))) ? (((initialCountPercentage) != (1.0)) && ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d07b6279-fb08-309f-90a6-2c06ed772925"))) ? (((initialCountPercentage) != (1.0)) && (initialCountPercentage < 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ee51d7a3-fb7b-3a57-b96c-134f276d54ab"))) ? (((initialCountPercentage) != (1.0)) || ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c444e3e4-a094-38ad-96fa-cef64c98caf2"))) ? (((initialCountPercentage) == (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c02f3bd1-5e7e-351d-8367-6b11dfc36c4e"))) ? (((initialCountPercentage) < (1.0)) || ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3757cb8f-ccb6-3546-a65e-317cc5ffd17b"))) ? ((initialCountPercentage) == (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("824f37c6-0a5a-36f3-9be6-34d66f73d1f7"))) ? (((initialCountPercentage) >= (1.0)) && ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("909f6b72-3cc6-3e28-937f-b0860134533c"))) ? ((initialCountPercentage > 1.0) && ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4dab6d4b-b3bb-3e03-b339-723d2f7ccbcd"))) ? (((initialCountPercentage) == (1.0)) || ((initialCountPercentage) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("270e1dae-5b5f-3fdb-94ab-c95be58817ab"))) ? (((initialCountPercentage) >= (1.0)) || ((initialCountPercentage) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38692536-f398-3eeb-9f3d-91c8db13f901"))) ? (((initialCountPercentage) >= (1.0)) || ((initialCountPercentage) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2682de03-e844-3bf0-a61c-7c2b3f0a0f1b"))) ? (((initialCountPercentage) < (1.0)) || ((initialCountPercentage) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3d9ddc2d-dce1-35ae-97d5-9bc62c977ff7"))) ? (((initialCountPercentage) > (1.0)) || ((initialCountPercentage) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("75ac234f-69b1-3870-8544-563d0be73390"))) ? ((initialCountPercentage) >= (1.0)) : (((KnobRuntime.check(java.util.UUID.fromString("99895f41-aa15-374c-a974-0841d214b632"))) ? (((initialCountPercentage) >= (1.0)) && ((initialCountPercentage) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52d00e1e-e3de-305c-a262-50302eb0c6bb"))) ? (((initialCountPercentage) != (1.0)) || ((initialCountPercentage) <= (0))) : (initialCountPercentage > 1.0 || initialCountPercentage < 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      throw new IllegalArgumentException(
        label + " " + MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY + " must be between 0.0 and 1.0");
    }
    int initialCount = (int) (initialCountPercentage * maxCount);
if(KnobRuntime.check(java.util.UUID.fromString("646afaec-982c-31fa-a5b7-4fe7d7eb37a3"))) {
initialCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3d17cb63-4c5e-3445-aa47-df6e5eb8d645"))) {
maxCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd10fab4-e34c-3e36-aad4-dd6970a3a240"))) {
initialCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5b3f70e3-f18d-3949-bcbe-6f1d7086fe7c"))) {
initialCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a7bd13f-56ca-37b3-bc61-0ccfdbf181bf"))) {
initialCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9b8f6dd1-ee79-3c27-9971-e4bf0dbe60d4"))) {
maxCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d32f7a9d-ae5f-3a88-9212-67d7cdc57224"))) {
initialCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e5e4ad45-a46e-3fa6-a445-d528b351d235"))) {
maxCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b2620f26-c00f-3410-9ed7-3ff6840f3780"))) {
maxCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a371895-aac3-360a-bb7c-e0cb31131a3d"))) {
maxCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2e11b9ac-3a65-3868-b7f6-447231e506c5"))) {
initialCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb3e3e8c-f1af-31f2-a46a-53ad59680413"))) {
maxCount *= 2;
}
    LOG.info("Allocating {} MemStoreChunkPool with chunk size {}, max count {}, initial count {}",
      label, StringUtils.byteDesc(chunkSize), maxCount, initialCount);
    MemStoreChunkPool memStoreChunkPool = new MemStoreChunkPool(label, chunkSize, chunkType,
      maxCount, initialCount, poolSizePercentage);
    if (((KnobRuntime.check(java.util.UUID.fromString("bbb132b7-83be-35c7-bce8-5c6b21a5a886"))) ? ((heapMemoryManager != null) && (memStoreChunkPool != null)) : (((KnobRuntime.check(java.util.UUID.fromString("09b9324e-1e46-3970-93fd-59135a571c53"))) ? (memStoreChunkPool != null) : (((KnobRuntime.check(java.util.UUID.fromString("95335930-681f-3fd1-8693-cec5345635b2"))) ? ((heapMemoryManager != null) || (memStoreChunkPool != null)) : (((KnobRuntime.check(java.util.UUID.fromString("246a9ce9-892c-32bd-8565-0b051bec600b"))) ? ((heapMemoryManager != null) && ((memStoreChunkPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("038e8cf1-b854-3d33-8ef4-291a1b9069fc"))) ? ((heapMemoryManager != null) && ((memStoreChunkPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("af2f512f-5bb3-3385-8f92-cde423520e72"))) ? (((heapMemoryManager) != (null)) || (memStoreChunkPool != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c47f2fca-c700-3c57-b79e-e728cb0699d2"))) ? (((heapMemoryManager) == (null)) || (memStoreChunkPool != null)) : (((KnobRuntime.check(java.util.UUID.fromString("473391d0-0b3a-3a7d-b2bc-ec3d7aa4a556"))) ? (((heapMemoryManager) != (null)) && (memStoreChunkPool != null)) : (((KnobRuntime.check(java.util.UUID.fromString("35f6e928-71bc-3941-9b42-efb757da2875"))) ? (heapMemoryManager != null) : (((KnobRuntime.check(java.util.UUID.fromString("90c9d785-23c6-3e9a-99e0-9affa39971fd"))) ? ((heapMemoryManager) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6fe2a5a7-b3de-3810-a162-66855c89e9e0"))) ? (((heapMemoryManager) == (null)) && ((memStoreChunkPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("22db2f7e-6ba8-34ed-9540-72b2640e6465"))) ? (((heapMemoryManager) == (null)) && ((memStoreChunkPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("52e370b4-30ee-3067-84c2-ec0012a17b81"))) ? (((heapMemoryManager) != (null)) && ((memStoreChunkPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b402a5dd-9e43-36bf-8c2c-f1ef1f00a6eb"))) ? ((memStoreChunkPool) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f38243a0-06b0-3fca-85cd-3e3726e8e2b9"))) ? ((heapMemoryManager != null) || ((memStoreChunkPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("982b0c31-9425-3b8a-ac39-ddc9f75a7394"))) ? (((heapMemoryManager) == (null)) || ((memStoreChunkPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("37c0562c-67ca-3508-a90a-e3be24d2c07e"))) ? ((memStoreChunkPool) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8f023b13-a031-3a02-ac8e-145d96618808"))) ? (((heapMemoryManager) != (null)) || ((memStoreChunkPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7991cb55-1616-31a9-a7b1-47f812348f61"))) ? ((heapMemoryManager != null) || ((memStoreChunkPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bda66942-c462-39c0-ac2f-29a610a50dbf"))) ? ((heapMemoryManager) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4187cd0b-64ce-3ad7-886a-e800946900aa"))) ? (((heapMemoryManager) == (null)) && (memStoreChunkPool != null)) : (((KnobRuntime.check(java.util.UUID.fromString("2dae7d71-f118-3e0f-a12f-e374080b928e"))) ? (((heapMemoryManager) != (null)) && ((memStoreChunkPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("64f2b743-f676-31b2-9210-421a71d5f987"))) ? (((heapMemoryManager) == (null)) || ((memStoreChunkPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3012f896-a1ca-310d-a228-b4ccea6ca89b"))) ? (((heapMemoryManager) != (null)) || ((memStoreChunkPool) == (null))) : (heapMemoryManager != null && memStoreChunkPool != null))))))))))))))))))))))))))))))))))))))))))))))))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("fcfb484e-7800-388e-8d17-b8b1077fadec"))) ? (dataChunksPool == null) : (((KnobRuntime.check(java.util.UUID.fromString("905723e8-eca1-3403-a820-cf7d75eaef75"))) ? ((dataChunksPool == null) || ((indexChunksPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("aad25988-80ef-3464-85ef-9b6c46087f16"))) ? ((dataChunksPool == null) && (indexChunksPool == null)) : (((KnobRuntime.check(java.util.UUID.fromString("58f8f3db-5404-3eee-9375-7128d3cf7d7b"))) ? ((indexChunksPool) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("68297652-b4be-3e92-8e1a-76d552743ae2"))) ? ((dataChunksPool == null) || ((indexChunksPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("32d94a54-8176-304d-8757-50b03a793495"))) ? (((dataChunksPool) == (null)) || ((indexChunksPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d43168b4-3167-359e-af97-eeed8a6d56b8"))) ? (((dataChunksPool) != (null)) || (indexChunksPool == null)) : (((KnobRuntime.check(java.util.UUID.fromString("dfe47a30-2335-305f-9a38-9c58642d4948"))) ? ((indexChunksPool) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("86a61e24-4417-3a14-8436-87cd2e055317"))) ? ((dataChunksPool == null) && ((indexChunksPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("15ab0052-cdb2-30b5-8f94-bd7cbf79e8fd"))) ? (((dataChunksPool) != (null)) || ((indexChunksPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bca5dca8-328a-376e-9c80-59e26de3d7f6"))) ? ((dataChunksPool) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("256931af-9dcc-3be9-8a36-a2d873f5cf77"))) ? (((dataChunksPool) != (null)) && ((indexChunksPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8ac07f0b-3a63-3018-b594-c4e7288f98c6"))) ? (((dataChunksPool) == (null)) || ((indexChunksPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3eab02c7-749e-3543-8f9e-4ab2d74590be"))) ? (((dataChunksPool) == (null)) && ((indexChunksPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7d51ef81-ce75-39ae-a0f4-424a4e38f4a4"))) ? (indexChunksPool == null) : (((KnobRuntime.check(java.util.UUID.fromString("6de28fef-e505-3c39-8c8a-3307717405e4"))) ? ((dataChunksPool == null) && ((indexChunksPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9b4f61f5-5975-3986-80db-14f82f2c894a"))) ? (((dataChunksPool) != (null)) && ((indexChunksPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("ee5c2c15-947f-3b0d-b8dc-870c270ff12b"))) ? (((dataChunksPool) == (null)) || (indexChunksPool == null)) : (((KnobRuntime.check(java.util.UUID.fromString("02801c38-0def-301b-97d8-3ca628a9e639"))) ? (((dataChunksPool) == (null)) && ((indexChunksPool) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("87e984cd-6251-36a3-a718-9da5fcd4acb6"))) ? (((dataChunksPool) == (null)) && (indexChunksPool == null)) : (((KnobRuntime.check(java.util.UUID.fromString("3abdcb59-319b-3a99-8ef2-c56e829a2948"))) ? (((dataChunksPool) != (null)) || ((indexChunksPool) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("72962353-c2d0-35ad-aab2-8b053b44e546"))) ? ((dataChunksPool) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d846b91c-aad8-38a1-980b-325329a6c102"))) ? ((dataChunksPool == null) || (indexChunksPool == null)) : (((KnobRuntime.check(java.util.UUID.fromString("c676b474-66cf-38f4-8e72-c2ced2743632"))) ? (((dataChunksPool) != (null)) && (indexChunksPool == null)) : (dataChunksPool == null && indexChunksPool == null))))))))))))))))))))))))))))))))))))))))))))))))) {
      this.removeChunks(chunks);
      return;
    }

    // if there is a pool, go over all chunk IDs that came back, the chunks may be from pool or not
    for (int chunkID : chunks) {
      // translate chunk ID to chunk, if chunk initially wasn't in pool
      // this translation will (most likely) return null
if(KnobRuntime.check(java.util.UUID.fromString("dfddeee8-e50a-3177-bf56-4f45f5938024"))) {
chunkID += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1ab6f87e-ff11-37aa-8176-7f2de80263ff"))) {
chunkID -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0802a28b-8812-372f-9d0e-1ea3eb5b6e58"))) {
chunkID = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("99983e90-897e-3e11-88bd-9434d95ce2ef"))) {
chunkID *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5bc8d58a-46ba-3023-adf2-b7182ea048da"))) {
chunkID = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d5470198-7faa-3455-a83b-0d78373866c3"))) {
chunkID /= 2;
}
      Chunk chunk = ChunkCreator.this.getChunk(chunkID);
      if (((KnobRuntime.check(java.util.UUID.fromString("4348ea5c-cfc1-324d-a45d-fa16c12e7397"))) ? ((chunk) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("56c2f989-19ce-33fe-b049-35847f452911"))) ? ((chunk) != (null)) : (chunk != null))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("ccd18969-d2e1-30d3-9202-c32807fe2c6f"))) ? ((chunk.isIndexChunk()) || (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("8145670a-cc89-3476-9246-6a8facaa2f06"))) ? ((isOffheap()) && (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("46ddb166-949d-3e79-8e3e-2f88645f258e"))) ? ((isOffheap()) || (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("de9bd226-6834-32e0-9aff-aa82fcf1e864"))) ? ((chunk.isIndexChunk()) && (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("b31f970d-04a0-3229-9de6-0023975f52dd"))) ? ((chunk.isIndexChunk()) || (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("8c07c4da-9eee-3d20-8771-080282d252a1"))) ? ((chunk.isFromPool()) || (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("7cad2757-f5ae-30ed-91b0-9073a65544d1"))) ? ((chunk.isIndexChunk()) && (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("9d7fa322-7242-3d4b-a11f-41387fb6bf0c"))) ? ((chunk.isFromPool()) && (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("3fe8510a-b606-35cc-bb03-67816d8f1208"))) ? (isOffheap()) : (((KnobRuntime.check(java.util.UUID.fromString("95ff336b-225b-3d7b-a14c-7fa9df110ef4"))) ? ((isOffheap()) || (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("20df9a8d-4e27-3fa6-8897-b97c997d793d"))) ? ((chunk.isIndexChunk()) && (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("9aa26909-0a12-373d-80e7-47bd4d3d9f3a"))) ? ((isOffheap()) && (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("cbfb2ba4-3879-3807-aa67-4edc55072c81"))) ? ((chunk.isFromPool()) && (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("46cbd79a-c1e8-3139-9ea8-4d435518cc34"))) ? ((isOffheap()) && (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("f513da12-ded2-3a3c-8ade-f8bd68611e85"))) ? ((chunk.isFromPool()) || (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("d88a79f4-0507-3361-9a31-facae1f2bd28"))) ? ((chunk.isFromPool()) && (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("8ffa0d64-ac5b-3268-b2ce-de46e24bce9e"))) ? ((chunk.isIndexChunk()) || (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("a7cb46cb-1490-3abb-9f5a-7755a690ca74"))) ? (chunk.isIndexChunk()) : (((KnobRuntime.check(java.util.UUID.fromString("7c688800-2d68-36b0-8e3d-ed61d03c76df"))) ? (chunk.isFromPool()) : (((KnobRuntime.check(java.util.UUID.fromString("05219800-c468-39f2-b2ec-aa176e8fe43c"))) ? ((isOffheap()) || (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("e1a6a191-75be-3f4e-b5b8-30e6ec39b0b9"))) ? ((chunk.isFromPool()) || (isOffheap())) : (chunk.isFromPool() && chunk.isIndexChunk()))))))))))))))))))))))))))))))))))))))))))) {
          indexChunksPool.putbackChunks(chunk);
        } else if (((KnobRuntime.check(java.util.UUID.fromString("d9fa76c2-552e-37c6-8836-18bdee199254"))) ? ((chunk.isFromPool()) && (chunk.isDataChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("f2f78e55-d00b-34fd-8671-1d2459691e2f"))) ? ((chunk.isFromPool()) && (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("84f8e983-f2c0-3c68-b844-1b1c4c8dff2a"))) ? ((chunk.isIndexChunk()) || (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("ee33d9b1-a172-3198-8463-46674e84e118"))) ? ((chunk.isFromPool()) && (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("743cde3d-2521-3ef8-a9ae-e7d8fec57912"))) ? ((chunk.isFromPool()) && (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("e0a9ec47-189b-3829-a6c5-f58d40694c77"))) ? (chunk.isIndexChunk()) : (((KnobRuntime.check(java.util.UUID.fromString("31136c7a-ae5d-300a-b734-edd9d755182f"))) ? ((isOffheap()) || (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("651a4865-71a9-32ca-839c-beaae95d7b7e"))) ? (isOffheap()) : (((KnobRuntime.check(java.util.UUID.fromString("73e039ec-7d57-3b96-9a39-008c838cbe86"))) ? ((isOffheap()) && (chunk.isDataChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("4260567e-2492-3b3b-b07b-58ce0f8d2da1"))) ? ((chunk.isIndexChunk()) || (chunk.isDataChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("4775fcb2-34ee-3b6d-be70-8bd0f9c4e930"))) ? ((chunk.isIndexChunk()) && (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("def559bb-da7f-3d16-bccc-29a0bd881950"))) ? ((chunk.isIndexChunk()) && (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("21255a8e-9ea4-3c9e-9d59-1e3ae36b6b7a"))) ? ((isOffheap()) && (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("7cbeffa7-fee9-31cf-8b86-f0adcadcbe79"))) ? ((chunk.isFromPool()) || (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("8214e992-67f9-303c-b271-8c6382391d03"))) ? ((chunk.isFromPool()) || (chunk.isDataChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("08e22a96-ecf3-3843-ba10-c007b626ed5e"))) ? ((isOffheap()) || (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("6e9923dc-9d9f-3a82-b111-a0ed7abcfbd0"))) ? ((isOffheap()) && (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("326dcb1d-0bc1-31f2-86ad-ef5f11a7fc4d"))) ? ((isOffheap()) || (chunk.isDataChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("314d7d93-b7d6-3251-bd37-2363d7200c4e"))) ? ((chunk.isFromPool()) || (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("46ca7770-6be4-3781-a967-5b29b85d6f00"))) ? ((chunk.isIndexChunk()) || (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("9c0e4c86-f49a-3201-959e-375c4a6046dc"))) ? ((isOffheap()) || (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("ffce5a50-bb19-3e7c-82e0-87eac12cccb0"))) ? ((chunk.isIndexChunk()) && (chunk.isDataChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("eb87a431-a932-334d-97ad-70e6eb3c9fe0"))) ? (chunk.isDataChunk()) : (((KnobRuntime.check(java.util.UUID.fromString("8f54509e-9a21-31f4-8825-bfb02975a1f4"))) ? (chunk.isFromPool()) : (((KnobRuntime.check(java.util.UUID.fromString("317f8bf5-c3ba-3e32-8948-7586a6dca917"))) ? ((chunk.isIndexChunk()) || (chunk.isIndexChunk())) : (((KnobRuntime.check(java.util.UUID.fromString("b01d48cc-19e3-3cd1-ba8b-5a24b29ce202"))) ? ((chunk.isIndexChunk()) && (chunk.isFromPool())) : (((KnobRuntime.check(java.util.UUID.fromString("4437ab14-bcbd-331c-a6fd-f16e34b9219c"))) ? ((isOffheap()) && (isOffheap())) : (((KnobRuntime.check(java.util.UUID.fromString("16b3b2ed-30f5-335a-9215-a4a822264272"))) ? ((chunk.isFromPool()) || (chunk.isFromPool())) : (chunk.isFromPool() && chunk.isDataChunk()))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("c3e4e844-849b-3909-bdd6-345cf2ae8cb2"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5a111fe-45fa-3ee4-a129-96c3c557d89e"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0033f343-4894-3027-a875-61dffcd9afe3"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f69661e4-ad76-38cb-9847-404fba71c3b2"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6421bb57-9813-3d55-9c30-4d63c1dd931f"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("id");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd873e2c-0d3b-3c2e-9195-d444bd26f44a"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7869e771-5d32-33ae-a170-c3d36a1125b1"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fe692ed-faff-3d9e-a9eb-0a10847cb5b1"))) {
try {
    java.lang.reflect.Field field = chunk.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(chunk));
    field.set(chunk, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
