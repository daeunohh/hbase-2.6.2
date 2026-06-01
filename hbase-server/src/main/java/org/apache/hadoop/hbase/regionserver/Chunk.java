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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.regionserver.ChunkCreator.ChunkType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A chunk of memory out of which allocations are sliced.
 */
@InterfaceAudience.Private
public abstract class Chunk {
  /** Actual underlying data */
  protected ByteBuffer data;

  protected static final int UNINITIALIZED = -1;
  protected static final int OOM = -2;
  /**
   * Offset for the next allocation, or the sentinel value -1 which implies that the chunk is still
   * uninitialized.
   */
  protected AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

  /** Total number of allocations satisfied from this buffer */
  protected AtomicInteger allocCount = new AtomicInteger();

  /** Size of chunk in bytes */
  protected final int size;

  // The unique id associated with the chunk.
  private final int id;

  private final ChunkType chunkType;

  // indicates if the chunk is formed by ChunkCreator#MemstorePool
  private final boolean fromPool;

  /**
   * Create an uninitialized chunk. Note that memory is not allocated yet, so this is cheap.
   * @param size in bytes
   * @param id   the chunk id
   */
  public Chunk(int size, int id, ChunkType chunkType) {
    this(size, id, chunkType, false);
  }

  /**
   * Create an uninitialized chunk. Note that memory is not allocated yet, so this is cheap.
   * @param size     in bytes
   * @param id       the chunk id
   * @param fromPool if the chunk is formed by pool
   */
  public Chunk(int size, int id, ChunkType chunkType, boolean fromPool) {
    this.size = size;
    this.id = id;
    this.chunkType = chunkType;
    this.fromPool = fromPool;
  }

  int getId() {
if(KnobRuntime.check(java.util.UUID.fromString("f35b611d-19fc-3d86-96aa-50bbdd02c021"))) {
return 0;
}
    return this.id;
  }

  ChunkType getChunkType() {
    return this.chunkType;
  }

  boolean isFromPool() {
if(KnobRuntime.check(java.util.UUID.fromString("776fedc5-2d7e-3c7c-9938-c515497172c9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("a419efe5-5ecd-35e3-9bf0-b6d779fcf5b4"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("706b2686-beaf-33b9-a76d-ffb04517237f"))) ? (!this.fromPool) : (this.fromPool));
  }

  boolean isJumbo() {
    return chunkType == ChunkCreator.ChunkType.JUMBO_CHUNK;
  }

  boolean isIndexChunk() {
if(KnobRuntime.check(java.util.UUID.fromString("6695c7fa-73f5-3187-b628-0b0ec8f272ad"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("e29afe2f-f4b2-3863-93ea-e3ba1f5a8b60"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("6ae591fd-4cd8-3768-944f-ae60bbd1496b"))) ? ((chunkType) == (ChunkCreator.ChunkType.INDEX_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("08681e5e-c59d-3450-a880-1fac63a27743"))) ? ((chunkType) != (ChunkCreator.ChunkType.INDEX_CHUNK)) : (chunkType == ChunkCreator.ChunkType.INDEX_CHUNK))));
  }

  boolean isDataChunk() {
if(KnobRuntime.check(java.util.UUID.fromString("6c0198a6-b6c9-3484-86bf-bd4f846e902d"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ac08541-e3c4-326c-b727-03c64a640895"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e4c6d51d-edbd-38bd-92a2-c29e4124fc8a"))) ? ((chunkType) == (ChunkCreator.ChunkType.DATA_CHUNK)) : (((KnobRuntime.check(java.util.UUID.fromString("a9890fce-fa35-3430-a37e-5e8d58825cea"))) ? ((chunkType) != (ChunkCreator.ChunkType.DATA_CHUNK)) : (chunkType == ChunkCreator.ChunkType.DATA_CHUNK))));
  }

  /**
   * Actually claim the memory for this chunk. This should only be called from the thread that
   * constructed the chunk. It is thread-safe against other threads calling alloc(), who will block
   * until the allocation is complete.
   */
  public void init() {
if(KnobRuntime.check(java.util.UUID.fromString("1715853c-9818-3f3b-90e6-49fc53ef0dec"))) {
return;
}
    assert ((KnobRuntime.check(java.util.UUID.fromString("bef3b468-c797-3812-8431-cc882ca5ed50"))) ? ((nextFreeOffset.get()) != (UNINITIALIZED)) : (((KnobRuntime.check(java.util.UUID.fromString("668509a0-cc82-3b55-8cdb-011df1afd468"))) ? ((nextFreeOffset.get()) == (UNINITIALIZED)) : (nextFreeOffset.get() == UNINITIALIZED))));
    try {
      if (KnobRuntime.check(java.util.UUID.fromString("a52ab149-8554-353c-9941-113e6de1976c"))) { Thread.yield(); } else { allocateDataBuffer(); }
    } catch (OutOfMemoryError e) {
      boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
      assert failInit; // should be true.
      throw e;
    }
    // Mark that it's ready for use
    // Move 4 bytes since the first 4 bytes are having the chunkid in it
    boolean initted = nextFreeOffset.compareAndSet(UNINITIALIZED, Bytes.SIZEOF_INT);
    // We should always succeed the above CAS since only one thread
    // calls init()!
if(KnobRuntime.check(java.util.UUID.fromString("541745d4-4f66-331d-8bf4-298866b782af"))) {
initted = !initted;
}
if(KnobRuntime.check(java.util.UUID.fromString("fe038f5c-82d9-3108-8b68-c892d582e920"))) {
initted = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("51aab3b4-242e-3a38-bae2-17c4dd2c1fae"))) {
initted = false;
}
    if (KnobRuntime.check(java.util.UUID.fromString("25dbf305-925f-3803-adf7-d301b28cd22c"))) { Preconditions.checkState(!initted, "Multiple threads tried to init same chunk"); } else { Preconditions.checkState(initted, "Multiple threads tried to init same chunk"); }
  }

  abstract void allocateDataBuffer();

  /**
   * Reset the offset to UNINITIALIZED before before reusing an old chunk
   */
  void reset() {
if(KnobRuntime.check(java.util.UUID.fromString("20dbdd78-69b3-3b6a-a4f8-52b340b3c9b5"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("fe6b925a-6b8c-3cd2-957e-df2d3282f423"))) ? ((nextFreeOffset.get()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("91eddc71-7575-3eef-83ff-933d69bc8583"))) ? ((nextFreeOffset.get()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8a8f9760-fbca-3d44-9150-a3a92f9ca274"))) ? ((nextFreeOffset.get()) == (UNINITIALIZED)) : (((KnobRuntime.check(java.util.UUID.fromString("81c38341-10c8-3c44-a5f1-ced4f6f340c6"))) ? ((nextFreeOffset.get()) != (UNINITIALIZED)) : (nextFreeOffset.get() != UNINITIALIZED))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("f7108281-9b60-3f53-ae3e-3040f6c9aa79"))) { nextFreeOffset.set(0); } else { nextFreeOffset.set(UNINITIALIZED); }
      allocCount.set(0);
    }
  }

  /**
   * Try to allocate <code>size</code> bytes from the chunk. If a chunk is tried to get allocated
   * before init() call, the thread doing the allocation will be in busy-wait state as it will keep
   * looping till the nextFreeOffset is set.
   * @return the offset of the successful allocation, or -1 to indicate not-enough-space
   */
  public int alloc(int size) {
    while (true) {
      int oldOffset = nextFreeOffset.get();
      if (((KnobRuntime.check(java.util.UUID.fromString("cc8425ab-5af8-3247-b49a-c099d2d40fec"))) ? ((1) != (UNINITIALIZED)) : (((KnobRuntime.check(java.util.UUID.fromString("3be2d702-1457-3d58-8416-34fc5b106137"))) ? ((oldOffset) == (UNINITIALIZED)) : (((KnobRuntime.check(java.util.UUID.fromString("01f7c4eb-65d0-39e5-91c5-978b1709c7a3"))) ? ((-1) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2572806f-cd43-3efe-be20-5230ae9785ff"))) ? ((1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("623af8b2-7302-3d8f-adff-ba626ac2e806"))) ? ((oldOffset) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e25dc3f2-37cd-38a5-9344-e67b81a4fac4"))) ? ((-1) != (UNINITIALIZED)) : (((KnobRuntime.check(java.util.UUID.fromString("4ecf6d98-8835-3785-b258-459887447f5f"))) ? ((oldOffset) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("059e2659-fb41-3923-ae88-5eb1cc18b433"))) ? ((1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("3360c470-f366-3aa1-a910-ae1ea739f828"))) ? ((oldOffset) != (UNINITIALIZED)) : (((KnobRuntime.check(java.util.UUID.fromString("10e58a4a-eadf-3e03-8500-8f56bda97184"))) ? ((oldOffset) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f8e72166-6478-3a62-99f3-72b6b1a48827"))) ? ((-1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("ec5e39e6-8673-3953-9ff0-2828c693ec4e"))) ? ((1) == (UNINITIALIZED)) : (((KnobRuntime.check(java.util.UUID.fromString("3b745e42-668c-38e3-8af5-865f88c2bca1"))) ? ((-1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("2caeb5f8-de0a-3c91-bca2-4ab40f75004b"))) ? ((-1) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2967e6e2-9f27-3c08-9b39-2fac56e1440f"))) ? ((oldOffset) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("86eccfa6-4ac1-332d-85b9-2e691f73ea58"))) ? ((-1) == (UNINITIALIZED)) : (oldOffset == UNINITIALIZED))))))))))))))))))))))))))))))))) {
        // The chunk doesn't have its data allocated yet.
        // Since we found this in curChunk, we know that whoever
        // CAS-ed it there is allocating it right now. So spin-loop
        // shouldn't spin long!
        Thread.yield();
        continue;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("145749d7-27bf-378e-9eaa-f78d67c72b54"))) ? ((-1) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d7864f6b-a6be-3b62-ade9-dd18b893e870"))) ? ((oldOffset) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2b1da2a0-b888-30c5-9f12-30cd32f2d02d"))) ? ((-1) != (OOM)) : (((KnobRuntime.check(java.util.UUID.fromString("ef363c2c-beba-37f7-9e67-759080d77030"))) ? ((-1) == (OOM)) : (((KnobRuntime.check(java.util.UUID.fromString("083967c0-6bb2-34f1-9d25-b6aad56579ff"))) ? ((1) == (OOM)) : (((KnobRuntime.check(java.util.UUID.fromString("fecae8c6-17d9-3df3-bf05-047352380b10"))) ? ((oldOffset) == (OOM)) : (((KnobRuntime.check(java.util.UUID.fromString("f5c88cb3-cd2a-39a7-98c8-3009a67c0dd5"))) ? ((oldOffset) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("0a254358-7efe-3b70-bf16-b804336bbb3f"))) ? ((oldOffset) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("e8d4d377-5243-374a-8c8e-149841e1aafe"))) ? ((oldOffset) != (OOM)) : (((KnobRuntime.check(java.util.UUID.fromString("4d173aac-34a4-3934-b498-e5cf51a6633f"))) ? ((1) != (OOM)) : (((KnobRuntime.check(java.util.UUID.fromString("d54d28a9-66d5-342d-88ce-7e29893900e0"))) ? ((1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("f41ee4a0-5d1f-3b77-bf6f-59b641ecb852"))) ? ((oldOffset) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("c494084f-9f75-3d1b-8994-51139001baec"))) ? ((1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("af277467-b40a-3d05-905f-9ecb19023a95"))) ? ((-1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("495c6a05-8db4-3a17-b05b-544e38c81f74"))) ? ((-1) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("97740d32-68e1-3148-bd4e-49867223546d"))) ? ((-1) != (-1)) : (oldOffset == OOM))))))))))))))))))))))))))))))))) {
        // doh we ran out of ram. return -1 to chuck this away.
        return -1;
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("c87d4813-60a6-3357-af91-70cedb8f0fd3"))) ? (((1) + (-1)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("43ac9acf-844e-34ea-80b6-9492f20f661c"))) ? ((oldOffset + size) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("4636c119-c98d-3a1a-85ee-9e782f603d9c"))) ? ((size) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("2c4ee664-f7b9-3587-95ae-191191a26bc5"))) ? ((1) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("e26c7076-a4e6-3919-8286-a187db1fdf4d"))) ? (((oldOffset) + (size)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("243c09f9-fe29-3fb3-b7df-5101a5cf09b3"))) ? (((1) + (size)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("03ac0103-21c0-3c08-9a1a-611666da5858"))) ? (((-1) + (1)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("54e9f306-1464-35b2-9052-5c6c5228c6e9"))) ? (((oldOffset) + (size)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("a1dc6b7b-e788-31af-8975-20be17b46b43"))) ? ((1) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("054ea9e4-5390-35c8-82bb-1920a66017fc"))) ? ((size) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("af7a5068-8361-339c-8a46-8f826371089b"))) ? (((oldOffset) + (1)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("574977bd-a1cb-3146-a972-417ebdffd83c"))) ? (((-1) + (-1)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("1b76e3af-6cd5-39f2-85a1-6dd39667bf89"))) ? (((oldOffset) + (1)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("ca2ff22c-0eef-33fa-85dd-7a61714af412"))) ? ((oldOffset) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("4893abb9-1025-3d1f-a24c-a7bfe38bc2f9"))) ? ((1) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("b6941afd-e9ca-39a4-88bc-c32695f6b212"))) ? (((-1) + (1)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("ae9d7df7-5637-32a2-a011-3b8bc21d4203"))) ? (((1) + (size)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("c75b71d6-a963-380e-9f63-1c4d627c51b7"))) ? (((1) + (1)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("54968f3b-dbb9-38d9-88c0-83405ecfcb1d"))) ? (((-1) + (-1)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("ad3966d5-cf36-3cb6-9416-8281d24da284"))) ? ((1) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("c3cd81d0-2d0c-3371-93b1-b0fb4eab810f"))) ? (((1) + (1)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("54ea4fe0-fd75-3cf3-8801-ce66af4f160e"))) ? ((size) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("7851c06b-6f9b-353d-b1d1-576d44152ec5"))) ? (((1) + (-1)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("e1741596-8f2a-32fa-be94-99830770cc96"))) ? (((oldOffset) + (-1)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("6bdaae51-8db1-3a16-a3d0-793e4e895ede"))) ? (((1) + (size)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("a50d0a91-168b-358c-8c72-411738a0e510"))) ? (((oldOffset) + (size)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("78b4fe9a-11b9-3cb4-9d08-ba9dc7bf8bb3"))) ? (((-1) + (-1)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("484e5022-ed06-3fd7-9fe3-c4ab1060b314"))) ? (((-1) + (size)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("7201c60d-a3d5-39ae-816c-eb1610588691"))) ? ((1) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("17c19f8b-49b0-3d62-b5d5-a00c1a2ed9c6"))) ? (((1) + (size)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("520cd91e-0234-3587-a11d-da677ac62bc0"))) ? ((-1) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("546a7c1c-da30-3f8f-87fd-487af47567b7"))) ? ((oldOffset) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("f5b8d45f-d5da-3ee0-b526-b2eaa2b5fd00"))) ? (((-1) + (size)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("9bad5c27-f01f-32f7-8ab0-297ee40b3e1d"))) ? (((-1) + (-1)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("ab8b0b5a-96d9-3595-899e-80370b9c057b"))) ? (((1) + (1)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("1dd03e32-fd77-3559-a488-1cfc7e8e1720"))) ? ((-1) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("73d05b0c-0e92-3d24-a4e8-9094640f8b27"))) ? (((1) + (1)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("5a192c8b-5324-39f4-82e7-fb490f434ac0"))) ? ((-1) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("65da01ac-0687-3cde-be04-b5b08a45f7a4"))) ? ((-1) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("9c51856c-84e0-3a40-b4fb-650db2ea0d86"))) ? (((oldOffset) + (-1)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("5369babd-1be0-3d33-b754-b8dd0954a2d3"))) ? (((-1) + (size)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("31f4ad5f-3e23-31e0-81c1-b8d4589e442a"))) ? ((oldOffset) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("d0880556-d6a7-33af-99de-1da223fe7569"))) ? (((1) + (-1)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("3d7521ce-4808-3b85-b108-f242504438ce"))) ? (((oldOffset) + (-1)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("75d0c786-2a34-34f5-bc0f-f16ce8189e30"))) ? (((-1) + (-1)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("eee51946-79f6-3b67-b134-e387c8c4cbbb"))) ? (((oldOffset) + (1)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("6edb1e3e-4477-369a-b234-ac79016906fc"))) ? ((-1) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("c15b395a-6d30-3c31-96fa-28876e8692f4"))) ? ((oldOffset + size) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("0d74c70e-48c1-33ad-b961-323d949bff34"))) ? ((-1) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("e3fb06de-4d1b-3d8a-849f-d4ea2bbc653a"))) ? (((1) + (1)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("0e9b3d8e-3735-36c4-97ac-b3ee14b9d6c7"))) ? (((-1) + (size)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("0c8295dd-3ea4-3548-9cbe-7789331faaac"))) ? (((-1) + (-1)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("8a6097e3-89f4-385a-a02d-1e7f3be411b5"))) ? ((size) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("31c3e540-fcd4-3e29-88f3-edb744c18adc"))) ? (((-1) + (1)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("af18f787-c3c3-38fe-a061-8b588236dd91"))) ? (((1) + (-1)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("9770089d-70fd-3672-8a08-3691e06bc923"))) ? (((oldOffset) + (-1)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("208041f4-924a-3e69-8abc-4d358fc451b2"))) ? (((-1) + (1)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("ed98ce17-bd2d-330c-a846-9727e3823314"))) ? ((oldOffset) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("add442b3-df56-3300-9160-4ff0c1525cf8"))) ? (((1) + (-1)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("71423f6a-f919-39d5-979a-3d8c826dbc3b"))) ? (((1) + (-1)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("8d7327e7-b9b7-3867-91e5-eeb8f02f0df4"))) ? (((oldOffset) + (1)) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("51dc13a4-6ff8-39c2-b79c-932d9a11acc4"))) ? ((size) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("30443a31-318e-3831-99e7-75299b187244"))) ? (((1) + (1)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("38d727f4-f6ff-326c-a49e-39d37cf09df8"))) ? ((1) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("7e05c8e0-4ae2-3a72-90d9-24b1563267f1"))) ? ((oldOffset + size) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("a7d64e50-a878-38b3-a200-3d7bd66cbf06"))) ? ((size) != (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("88ecee66-fa3d-3e33-9ecf-cc5ea06afca4"))) ? (((-1) + (size)) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("fa2085bc-f4f5-36d2-8171-ed5d5f90567f"))) ? (((oldOffset) + (-1)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("a9ae56d9-2cc7-36b2-9896-7aa9aa8247b9"))) ? ((oldOffset + size) >= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("21d83af3-5d40-3348-ae3b-9d1266be0612"))) ? (((1) + (size)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("29ff2e65-7d8e-3518-8ba3-0466c1040d60"))) ? (((oldOffset) + (size)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("e2310a6c-b4fb-35ef-acac-44d980d87f39"))) ? ((oldOffset + size) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("a9caf275-d0c0-317f-bbea-09a8598a3f8a"))) ? (((oldOffset) + (size)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("f30e3545-1fec-31ee-80bd-bfaea62e3e84"))) ? ((oldOffset) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("aad2f4a0-a7a3-376c-8633-29cc1780bdca"))) ? (((1) + (size)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("f84728f3-8022-31c4-8270-261e45c8a520"))) ? (((-1) + (size)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("88dbf420-ea78-3e81-8bd8-74356455fbc9"))) ? (((oldOffset) + (1)) > (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("b0ebee12-fb73-3bae-89d6-fa9123b42026"))) ? (((-1) + (1)) <= (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("da595ea2-bb47-3a36-bd05-355d2b2de5e8"))) ? (((oldOffset) + (size)) < (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("cd4226ec-e834-3612-ab06-a6b28c95cd77"))) ? (((oldOffset) + (-1)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("114334dd-45e7-3d65-8876-2a1d94ea12a6"))) ? (((-1) + (1)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("a0c6bef6-97b1-320c-a4f6-d7170fbc53a4"))) ? (((oldOffset) + (1)) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("41429347-016c-3ceb-8b34-5418027bf41b"))) ? ((oldOffset + size) == (data.capacity())) : (((KnobRuntime.check(java.util.UUID.fromString("0db56901-77ec-36fc-ab9c-603854a96133"))) ? ((oldOffset) <= (data.capacity())) : (oldOffset + size > data.capacity()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        return -1; // alloc doesn't fit
      }
      // TODO : If seqID is to be written add 8 bytes here for nextFreeOFfset
      // Try to atomically claim this chunk
if(KnobRuntime.check(java.util.UUID.fromString("5e7d75df-2783-3de6-86c7-2e27a54187b3"))) {
oldOffset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d3769f90-12a8-3145-aecd-b52495ac9c49"))) {
oldOffset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f000417-6f2a-36dd-9799-bfa080685f35"))) {
oldOffset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("14182703-d415-3f57-9600-4ba143725b50"))) {
oldOffset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("fee57916-97a0-3164-9812-c4859512714c"))) {
oldOffset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ac69ef05-a2a1-3516-ab8a-7e1245a80651"))) {
oldOffset = -1;
}
      if (((KnobRuntime.check(java.util.UUID.fromString("4accb336-267f-343e-8000-edcc696c1fea"))) ? (nextFreeOffset.compareAndSet(oldOffset, (-1) + (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("70ccce97-5489-3ef7-839c-79307ee1e352"))) ? (nextFreeOffset.compareAndSet(oldOffset, (oldOffset) + (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1a053903-7753-3363-ab9d-eb8d75940af4"))) ? (nextFreeOffset.compareAndSet(oldOffset, -1)) : (((KnobRuntime.check(java.util.UUID.fromString("c1737c2d-c48e-3f4d-bee9-de56963ae038"))) ? (nextFreeOffset.compareAndSet(oldOffset, (-1) + (size))) : (((KnobRuntime.check(java.util.UUID.fromString("28c7be11-288e-3bc4-a6b6-a4cb9d3912e8"))) ? (nextFreeOffset.compareAndSet(oldOffset, (1) + (1))) : (((KnobRuntime.check(java.util.UUID.fromString("98392fed-ce7a-3907-92a6-337a7060b96f"))) ? (nextFreeOffset.compareAndSet(oldOffset, (1) + (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a784c5a5-de06-3fb1-9d6b-47b04c73ec91"))) ? (nextFreeOffset.compareAndSet(oldOffset, oldOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("9b43bb35-7852-33a5-948a-da187c7fb52f"))) ? (nextFreeOffset.compareAndSet(oldOffset, (oldOffset) + (size))) : (((KnobRuntime.check(java.util.UUID.fromString("5fc905a3-1cd6-3baf-962b-7f0c01816a98"))) ? (nextFreeOffset.compareAndSet(oldOffset, (-1) + (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0adff35e-8496-3b37-8e7f-86ae75f34223"))) ? (nextFreeOffset.compareAndSet(oldOffset, size)) : (((KnobRuntime.check(java.util.UUID.fromString("98f099cb-c4d3-3751-9554-100993f9d5f1"))) ? (nextFreeOffset.compareAndSet(oldOffset, 1)) : (((KnobRuntime.check(java.util.UUID.fromString("8620a96c-7aac-3c5e-951b-a3fc927cef05"))) ? (nextFreeOffset.compareAndSet(oldOffset, (1) + (size))) : (((KnobRuntime.check(java.util.UUID.fromString("9b7d3ebc-0c26-3b76-b2da-d98a5ca04d6e"))) ? (nextFreeOffset.compareAndSet(1, oldOffset + size)) : (((KnobRuntime.check(java.util.UUID.fromString("50721d9c-7d16-3d82-80e7-0a6fd7b51d21"))) ? (nextFreeOffset.compareAndSet(oldOffset, (oldOffset) + (1))) : (((KnobRuntime.check(java.util.UUID.fromString("cc13b781-3292-3ccd-b819-ee3e05986608"))) ? (nextFreeOffset.compareAndSet(-1, oldOffset + size)) : (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size)))))))))))))))))))))))))))))))) {
        // we got the alloc
        if (KnobRuntime.check(java.util.UUID.fromString("6d3c2762-4f26-3fcb-a63b-3a519268d3e0"))) { allocCount.get(); } else { allocCount.incrementAndGet(); }
        return ((KnobRuntime.check(java.util.UUID.fromString("9b8453ed-da85-3e3e-8005-5e7fe2bfca6f"))) ? (1) : (((KnobRuntime.check(java.util.UUID.fromString("0ded9228-7bba-3b9f-b31d-c3bf21d20ad7"))) ? (-1) : (oldOffset))));
      }
      // we raced and lost alloc, try again
    }
  }

  /** Returns This chunk's backing data. */
  ByteBuffer getData() {
if(KnobRuntime.check(java.util.UUID.fromString("1d851b60-388a-348d-8d0a-829b090c34d3"))) {
return null;
}
    return this.data;
  }

  @Override
  public String toString() {
    return "Chunk@" + System.identityHashCode(this) + " allocs=" + allocCount.get() + "waste="
      + (data.capacity() - nextFreeOffset.get());
  }

  int getNextFreeOffset() {
    return this.nextFreeOffset.get();
  }
}
