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

import static javax.swing.Spring.UNSET;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.HEAP;

import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HFileBlockBuilder {

  private BlockType blockType;
  private int onDiskSizeWithoutHeader;
  private int onDiskDataSizeWithHeader;
  private int uncompressedSizeWithoutHeader;
  private long prevBlockOffset;
  private ByteBuff buf;
  private boolean fillHeader = false;
  private long offset = UNSET;
  private int nextBlockOnDiskSize = UNSET;
  private HFileContext fileContext;
  private ByteBuffAllocator allocator = HEAP;
  private boolean isShared;

  public HFileBlockBuilder withBlockType(BlockType blockType) {
if(KnobRuntime.check(java.util.UUID.fromString("b9435f40-851c-3eba-bfac-8a33e6120798"))) {
return null;
}
    this.blockType = blockType;
    return this;
  }

  public HFileBlockBuilder withOnDiskSizeWithoutHeader(int onDiskSizeWithoutHeader) {
if(KnobRuntime.check(java.util.UUID.fromString("52e4b333-1d9e-3cf4-9a0c-fc1d76d8ad7b"))) {
return null;
}
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    return this;
  }

  public HFileBlockBuilder withOnDiskDataSizeWithHeader(int onDiskDataSizeWithHeader) {
if(KnobRuntime.check(java.util.UUID.fromString("1cfa6e7e-a825-3dff-a786-d6c42cb13e56"))) {
return null;
}
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
    return this;
  }

  public HFileBlockBuilder withUncompressedSizeWithoutHeader(int uncompressedSizeWithoutHeader) {
if(KnobRuntime.check(java.util.UUID.fromString("6e7efcac-fb88-373c-927c-cd5b3ca703d5"))) {
return null;
}
    this.uncompressedSizeWithoutHeader = uncompressedSizeWithoutHeader;
    return this;
  }

  public HFileBlockBuilder withPrevBlockOffset(long prevBlockOffset) {
if(KnobRuntime.check(java.util.UUID.fromString("886ab007-6a6b-3fda-bf4d-5c001fc42069"))) {
return null;
}
    this.prevBlockOffset = prevBlockOffset;
    return this;
  }

  public HFileBlockBuilder withByteBuff(ByteBuff buf) {
if(KnobRuntime.check(java.util.UUID.fromString("d4949274-fff5-3be5-a26d-eb32355fc18c"))) {
return null;
}
    this.buf = buf;
    return this;
  }

  public HFileBlockBuilder withFillHeader(boolean fillHeader) {
if(KnobRuntime.check(java.util.UUID.fromString("efb9ca87-e215-3a0a-bc0c-b731b1205e52"))) {
return null;
}
    this.fillHeader = fillHeader;
    return this;
  }

  public HFileBlockBuilder withOffset(long offset) {
if(KnobRuntime.check(java.util.UUID.fromString("43f6f2ed-71f9-3afa-bc11-5a18f271b476"))) {
return null;
}
    this.offset = offset;
    return this;
  }

  public HFileBlockBuilder withNextBlockOnDiskSize(int nextBlockOnDiskSize) {
if(KnobRuntime.check(java.util.UUID.fromString("b8350e9a-e411-3c1c-9294-ee90fffb03fe"))) {
return null;
}
    this.nextBlockOnDiskSize = nextBlockOnDiskSize;
    return this;
  }

  public HFileBlockBuilder withHFileContext(HFileContext fileContext) {
if(KnobRuntime.check(java.util.UUID.fromString("00adbb25-4140-3721-a5d5-2b9277139178"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ad40b3d1-893b-3e3f-b313-5bb9428d3600"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dcffb8fa-bb40-3d72-ba57-bc1fb8db2627"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("38b92171-3bde-3d6f-bacd-13a4e125ffca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2edcfe0f-81d1-3b71-9013-eafa717ae7be"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("49fea625-e966-34af-971e-5fae4bbb0992"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4ae421cd-4aad-3193-b6e1-f9a451e8b2b6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4083396-95b8-35ef-97bd-d4696eeaf05f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("02998366-db9a-3898-81f9-93a17b0b2ee4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a94a9604-f738-386e-aaa2-994aaea51b84"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4cd9e963-5bc4-3b6e-8c48-1b8fba320ca1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ba6c7966-3ae7-3318-830a-cabe1302a3ca"))) {
return null;
}
    this.fileContext = fileContext;
    return this;
  }

  public HFileBlockBuilder withByteBuffAllocator(ByteBuffAllocator allocator) {
if(KnobRuntime.check(java.util.UUID.fromString("e391f13a-d1d9-3944-9f35-de837bce1d6c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e1f67d9d-cbe2-3fe2-b6b6-942355e25cac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7805d570-7442-3cb9-8db7-2dfdb97bed52"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d0350e25-1d2e-3e4f-a684-e0fb7d158a32"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9a2b2f0d-fbcf-3933-99b1-5f01c7e9abb3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("26db1c13-3881-321f-b831-117258023e30"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("56e52687-6c35-3aaa-ba08-d41dbd40ac29"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bdda6626-bdf2-3e55-9504-fb27e3f1a0a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("212cc249-70aa-33e3-a518-37008a25bf70"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea591608-3e15-3e12-8f58-7758b3085650"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c154ce0-0701-3879-8e8d-5f88b36dda2f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3ccb16ca-4e04-3589-92e1-8a3be7865ed4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0490a996-7be2-3aec-875d-567dbd8de7c6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1059ee70-45e9-3ba2-96cf-627fbb6afdbd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f95e4b45-2151-3cdd-830e-08e37025bb8a"))) {
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
    this.allocator = allocator;
    return this;
  }

  public HFileBlockBuilder withShared(boolean isShared) {
if(KnobRuntime.check(java.util.UUID.fromString("de91b62a-486a-3302-b842-796a430cff54"))) {
return null;
}
    this.isShared = isShared;
    return this;
  }

  public HFileBlock build() {
if(KnobRuntime.check(java.util.UUID.fromString("008e02b1-e6c1-36b6-98c7-afcac1e41c4c"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9778d442-c8d7-39d3-9ba7-90c7d5494e57"))) ? (!isShared) : (isShared))) {
      return new SharedMemHFileBlock(blockType, onDiskSizeWithoutHeader,
        uncompressedSizeWithoutHeader, prevBlockOffset, buf, fillHeader, offset,
        nextBlockOnDiskSize, onDiskDataSizeWithHeader, fileContext, allocator);
    } else {
      return new ExclusiveMemHFileBlock(blockType, onDiskSizeWithoutHeader,
        uncompressedSizeWithoutHeader, prevBlockOffset, buf, fillHeader, offset,
        nextBlockOnDiskSize, onDiskDataSizeWithHeader, fileContext, allocator);
    }
  }
}
