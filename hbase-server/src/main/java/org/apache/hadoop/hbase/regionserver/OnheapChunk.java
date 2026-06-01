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
import org.apache.hadoop.hbase.regionserver.ChunkCreator.ChunkType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An on heap chunk implementation.
 */
@InterfaceAudience.Private
public class OnheapChunk extends Chunk {

  OnheapChunk(int size, int id, ChunkType chunkType) {
    super(size, id, chunkType);
  }

  OnheapChunk(int size, int id, ChunkType chunkType, boolean fromPool) {
    super(size, id, chunkType, fromPool);
  }

  @Override
  void allocateDataBuffer() {
if(KnobRuntime.check(java.util.UUID.fromString("2249ee05-0f7f-3e26-9f1e-309225448569"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9c7e646e-12a6-35ee-8ca3-d68d96ec266c"))) ? ((data) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("70ddf8d9-57ac-3f09-9f63-df2d17be00ac"))) ? ((data) != (null)) : (data == null))))) {
      data = ByteBuffer.allocate(this.size);
      data.putInt(0, this.getId());
    }
  }
}
