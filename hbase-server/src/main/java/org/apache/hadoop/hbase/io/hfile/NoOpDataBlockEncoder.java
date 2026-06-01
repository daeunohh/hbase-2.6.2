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

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.EncodingState;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.encoding.NoneEncoder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Does not perform any kind of encoding/decoding.
 */
@InterfaceAudience.Private
public class NoOpDataBlockEncoder implements HFileDataBlockEncoder {

  public static final NoOpDataBlockEncoder INSTANCE = new NoOpDataBlockEncoder();

  private static class NoneEncodingState extends EncodingState {
    NoneEncoder encoder = null;
  }

  /** Cannot be instantiated. Use {@link #INSTANCE} instead. */
  private NoOpDataBlockEncoder() {
  }

  @Override
  public void encode(Cell cell, HFileBlockEncodingContext encodingCtx, DataOutputStream out)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f51c2242-3d7a-3059-b23d-3854ea0f7d69"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c7feede3-069b-3946-85d9-cf6d79ea3702"))) {
throw new java.io.IOException("Injected exception");
}
    NoneEncodingState state = (NoneEncodingState) encodingCtx.getEncodingState();
    NoneEncoder encoder = state.encoder;
if(KnobRuntime.check(java.util.UUID.fromString("8d76d047-865b-3fd5-87ca-7c17324af8e8"))) {
throw new java.io.IOException("Injected exception");
}
    int size = encoder.write(cell);
if(KnobRuntime.check(java.util.UUID.fromString("b98ea5cc-4b04-33a8-b143-ab4386430b0a"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("64861957-6e54-31f1-9a9b-791db0b886a9"))) {
size = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f2f1ba54-a81d-326c-963a-12dd7b130cd9"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("74ec64fd-4bb0-3e27-8dcc-b36c63b47d01"))) {
size -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d9dd9d56-ceff-3360-8138-521448ddb664"))) {
size /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd28913f-a2e6-3c08-9274-eb9e26f48350"))) {
size *= 2;
}
    state.postCellEncode(size, size);
  }

  @Override
  public boolean useEncodedScanner() {
if(KnobRuntime.check(java.util.UUID.fromString("1aa2e486-101b-3d7c-a387-c16abe4719e2"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("8dad56fe-7485-3115-be3a-f189fb06b2b8"))) {
return true;
}
    return false;
  }

  @Override
  public void saveMetadata(HFile.Writer writer) {
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
if(KnobRuntime.check(java.util.UUID.fromString("c04d3680-6938-3b7b-a597-d7e3ee0b7a47"))) {
return null;
}
    return DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
if(KnobRuntime.check(java.util.UUID.fromString("84b50f4c-81f4-306a-9c4d-51bfccfd2669"))) {
return null;
}
    return DataBlockEncoding.NONE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(Configuration conf,
    byte[] dummyHeader, HFileContext meta) {
if(KnobRuntime.check(java.util.UUID.fromString("f6f4c3ad-1ba2-3edc-8ece-e9bc19b0ddb0"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("usesHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(meta);
    field.set(meta, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("abd0c758-b234-31aa-816b-467370836d1e"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32823ff0-1c3a-3543-b84e-265e515a467d"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("includesTags");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(meta);
    field.set(meta, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f841ee1a-3a3a-32a6-b4f2-0ad3a28dac9f"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e08d64a-dde5-3b81-adf4-886a037d77dc"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(meta));
    field.set(meta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d7edce8-96eb-3ab8-878c-856839f83af2"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5cada95b-c196-3667-b87d-0dd278114a72"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("66c2d542-5ea6-32a2-b05c-69b70e3728b7"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("049902d6-8b4f-3138-8206-f1c4cd9ba736"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3f4d9bc1-8f91-31c2-b2f5-86a58f61fa9d"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("112297d7-8bac-333c-bcf4-7c8e08947d21"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c12261ea-d388-3728-ac6f-e74832661858"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da1731ac-3934-3e12-b70e-6a1abff910f3"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb4cdf26-0d29-389e-9d19-735375477145"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(meta));
    field.set(meta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return new HFileBlockDefaultEncodingContext(conf, null, dummyHeader, meta);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(Configuration conf,
    HFileContext meta) {
if(KnobRuntime.check(java.util.UUID.fromString("00183f97-6c9a-3df8-bee1-9e8bde09353a"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("846043d4-9b74-3bb3-b3e3-318c530bd31f"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a3ce3f7-dc75-3444-84ab-a4e5e8193f64"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("320548fe-a848-3038-9257-78f680dcc982"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("78afdec5-c5ca-3bdb-aa83-9afb8f2de5c4"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cc2e69d0-de36-3e90-99c9-5fc27111bbc6"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("f2630cd4-9547-34b3-8551-30baf8b59fd8"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("includesTags");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(meta);
    field.set(meta, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6095197-7434-3159-b810-fc6de51decf5"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(meta));
    field.set(meta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec526645-c5e5-3c56-92bd-6a05a50f9748"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(meta));
    field.set(meta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb1b1ce1-f3b7-3875-a065-4135643a6d06"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bcf4f39e-92f6-348b-8ac8-dbc62bb6f80e"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbac6e8c-91a6-3586-943b-ee0ab204e6a3"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad509cbf-3937-3b8b-aced-ec443bf6de28"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(meta));
    field.set(meta, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87b83855-c148-3884-9465-56ff5bb8d1f2"))) {
try {
    java.lang.reflect.Field field = meta.getClass().getDeclaredField("usesHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(meta);
    field.set(meta, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return new HFileBlockDefaultDecodingContext(conf, meta);
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext blkEncodingCtx, DataOutputStream out)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("41883e03-69a8-3226-b99e-abc05af30679"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("09223518-112e-319b-8207-745711995691"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("95e81651-ac7b-30e7-b024-558b694f23ff"))) ? ((blkEncodingCtx.getClass()) != (HFileBlockDefaultEncodingContext.class)) : (((KnobRuntime.check(java.util.UUID.fromString("70087126-c95c-3ebc-9dac-607952277d70"))) ? ((blkEncodingCtx.getClass()) == (HFileBlockDefaultEncodingContext.class)) : (blkEncodingCtx.getClass() != HFileBlockDefaultEncodingContext.class))))) {
      throw new IOException(this.getClass().getName() + " only accepts "
        + HFileBlockDefaultEncodingContext.class.getName() + " as the " + "encoding context.");
    }

    HFileBlockDefaultEncodingContext encodingCtx =
      (HFileBlockDefaultEncodingContext) blkEncodingCtx;
if(KnobRuntime.check(java.util.UUID.fromString("3142e607-8d7d-39e4-959f-c7d0f3cb4ef7"))) {
throw new java.io.IOException("Injected exception");
}
    encodingCtx.prepareEncoding(out);

    NoneEncoder encoder = new NoneEncoder(out, encodingCtx);
    NoneEncodingState state = new NoneEncodingState();
    state.encoder = encoder;
    blkEncodingCtx.setEncodingState(state);
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
    byte[] uncompressedBytesWithHeader, BlockType blockType) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d3173efb-5047-388d-8a25-747ca3ae1f56"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("016c7ba8-57ae-32d5-b7f5-442122524c06"))) {
return;
}
    encodingCtx.postEncoding(BlockType.DATA);
  }
}
