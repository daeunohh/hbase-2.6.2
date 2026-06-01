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

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Do different kinds of index block encoding according to column family options.
 */
@InterfaceAudience.Private
public class HFileIndexBlockEncoderImpl implements HFileIndexBlockEncoder {
  private final IndexBlockEncoding indexBlockEncoding;

  /**
   * Do index block encoding with specified options.
   * @param encoding What kind of data block encoding will be used.
   */
  public HFileIndexBlockEncoderImpl(IndexBlockEncoding encoding) {
    this.indexBlockEncoding = encoding != null ? encoding : IndexBlockEncoding.NONE;
  }

  public static HFileIndexBlockEncoder createFromFileInfo(HFileInfo fileInfo) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("fbf186fc-1a6f-3603-876d-019457b3ab48"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgKeyLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e9cf9f19-56e1-3a7d-b78a-76a1eceefa19"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("392f293c-68b6-3ccd-88ae-2d520575d60d"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgKeyLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32de781e-3284-3eb4-a717-5e3bf90b56d0"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgValueLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("abb59f03-7ebf-3013-9d59-a66b1bfb4b9a"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgValueLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7061c11-5f36-3ab2-af51-e6507e097bf5"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgValueLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9060e31c-1113-3a7c-bed2-e6eb5e04622c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("3ed2eea5-8483-3eb0-a56b-718dc1b3181c"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgValueLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ca3b96e-9eeb-34b6-85b4-bfb26b300a80"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgKeyLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ddc65ef2-2fc8-338d-96df-e06fc91d6417"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("lenOfBiggestCell");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fileInfo));
    field.set(fileInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db4a8895-2bd0-3233-9858-2b0f87668d1f"))) {
try {
    java.lang.reflect.Field field = fileInfo.getClass().getDeclaredField("avgKeyLen");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fileInfo));
    field.set(fileInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    IndexBlockEncoding encoding = IndexBlockEncoding.NONE;
    byte[] dataBlockEncodingType = fileInfo.get(INDEX_BLOCK_ENCODING);
    if (((KnobRuntime.check(java.util.UUID.fromString("49ac405a-6807-31a6-aa9a-698304a70154"))) ? ((dataBlockEncodingType) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9312ad28-5675-3a60-a700-8d8167ddceec"))) ? ((dataBlockEncodingType) != (null)) : (dataBlockEncodingType != null))))) {
      String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
      try {
        encoding = IndexBlockEncoding.valueOf(dataBlockEncodingStr);
      } catch (IllegalArgumentException ex) {
        throw new IOException(
          "Invalid data block encoding type in file info: " + dataBlockEncodingStr, ex);
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("8c229446-1118-3860-a411-f2ace2748e18"))) ? ((encoding) != (IndexBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("47e9b161-c708-3921-b87f-929a3326e1b0"))) ? ((encoding) == (IndexBlockEncoding.NONE)) : (encoding == IndexBlockEncoding.NONE))))) {
      return NoOpIndexBlockEncoder.INSTANCE;
    }
    return new HFileIndexBlockEncoderImpl(encoding);
  }

  @Override
  public void saveMetadata(HFile.Writer writer) throws IOException {
    writer.appendFileInfo(INDEX_BLOCK_ENCODING, indexBlockEncoding.getNameInBytes());
  }

  @Override
  public IndexBlockEncoding getIndexBlockEncoding() {
    return indexBlockEncoding;
  }

  @Override
  public void encode(BlockIndexChunk blockIndexChunk, boolean rootIndexBlock, DataOutput out)
    throws IOException {
    // TODO
  }

  @Override
  public EncodedSeeker createSeeker() {
    return null;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(indexBlockEncoding=" + indexBlockEncoding + ")";
  }
}
