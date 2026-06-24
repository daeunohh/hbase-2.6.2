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
    this.blockType = blockType;
    return this;
  }

  public HFileBlockBuilder withOnDiskSizeWithoutHeader(int onDiskSizeWithoutHeader) {
    this.onDiskSizeWithoutHeader = onDiskSizeWithoutHeader;
    return this;
  }

  public HFileBlockBuilder withOnDiskDataSizeWithHeader(int onDiskDataSizeWithHeader) {
    this.onDiskDataSizeWithHeader = onDiskDataSizeWithHeader;
    return this;
  }

  public HFileBlockBuilder withUncompressedSizeWithoutHeader(int uncompressedSizeWithoutHeader) {
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
    this.buf = buf;
    return this;
  }

  public HFileBlockBuilder withFillHeader(boolean fillHeader) {
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
    this.nextBlockOnDiskSize = nextBlockOnDiskSize;
    return this;
  }

  public HFileBlockBuilder withHFileContext(HFileContext fileContext) {
if(KnobRuntime.check(java.util.UUID.fromString("9764b46e-27fa-3815-9ff6-3175fcb0036f"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileContext.getClass().getDeclaredField("blockSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fileContext));
    _knob_field_.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.fileContext = fileContext;
    return this;
  }

  public HFileBlockBuilder withByteBuffAllocator(ByteBuffAllocator allocator) {
if(KnobRuntime.check(java.util.UUID.fromString("073068bd-6d00-39fd-8405-7e76cf688dbb"))) {
try {
    java.lang.reflect.Field _knob_field_ = allocator.getClass().getDeclaredField("maxBufCount");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(allocator));
    _knob_field_.set(allocator, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31b2260a-7cad-3826-a8ca-ae32d78cd5c4"))) {
try {
    java.lang.reflect.Field _knob_field_ = allocator.getClass().getDeclaredField("bufSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(allocator));
    _knob_field_.set(allocator, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0a5176c-4d0b-375c-8343-deff91c8d200"))) {
try {
    java.lang.reflect.Field _knob_field_ = allocator.getClass().getDeclaredField("bufSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(allocator));
    _knob_field_.set(allocator, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.allocator = allocator;
    return this;
  }

  public HFileBlockBuilder withShared(boolean isShared) {
    this.isShared = isShared;
    return this;
  }

  public HFileBlock build() {
    if (isShared) {
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
