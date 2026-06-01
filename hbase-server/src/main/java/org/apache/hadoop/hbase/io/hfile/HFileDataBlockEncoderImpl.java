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
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Do different kinds of data block encoding according to column family options.
 */
@InterfaceAudience.Private
public class HFileDataBlockEncoderImpl implements HFileDataBlockEncoder {
  private final DataBlockEncoding encoding;

  /**
   * Do data block encoding with specified options.
   * @param encoding What kind of data block encoding will be used.
   */
  public HFileDataBlockEncoderImpl(DataBlockEncoding encoding) {
    this.encoding = encoding != null ? encoding : DataBlockEncoding.NONE;
  }

  public static HFileDataBlockEncoder createFromFileInfo(HFileInfo fileInfo) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("171917fd-ce06-33a1-a042-5d15cbb29700"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e4905944-75d4-34ac-9d26-908a50c429f2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2fde236f-129f-3790-967e-c89111ebe944"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a875c73d-785c-30dd-9515-71cde3f7337d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("dbc14775-eafe-3b5a-a325-66e646240379"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("23a15c82-e8e2-3bee-9907-c9da56e440f7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8651a923-bd3a-32d4-9382-1b9827087918"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("92dd1949-faac-31c8-ad05-c7fa39e2dbef"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("68a655c7-2fee-359e-a412-23b2aa1050f0"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("85b162ef-3a04-3979-8013-706c016c50c9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79ac5f55-5fe6-307b-b363-2560a43db8f7"))) {
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
    DataBlockEncoding encoding = DataBlockEncoding.NONE;
    byte[] dataBlockEncodingType = fileInfo.get(DATA_BLOCK_ENCODING);
    if (((KnobRuntime.check(java.util.UUID.fromString("72315a48-5691-337d-a9e1-8e0174a490cd"))) ? ((dataBlockEncodingType) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("57192f03-623d-3880-bad1-27c57aecf1aa"))) ? ((dataBlockEncodingType) == (null)) : (dataBlockEncodingType != null))))) {
      String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
      try {
        encoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
      } catch (IllegalArgumentException ex) {
        throw new IOException(
          "Invalid data block encoding type in file info: " + dataBlockEncodingStr, ex);
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("1dcffb9a-3c74-3784-b95c-38983b1f6ab7"))) ? ((encoding) == (DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("fc7eee49-0682-3181-a09b-bdc54dc38bd6"))) ? ((encoding) != (DataBlockEncoding.NONE)) : (encoding == DataBlockEncoding.NONE))))) {
      return NoOpDataBlockEncoder.INSTANCE;
    }
    return new HFileDataBlockEncoderImpl(encoding);
  }

  @Override
  public void saveMetadata(HFile.Writer writer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("dc23773b-eb0d-37d0-b662-7d198d95cfd6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6321b4ae-0106-363d-bbea-3ae70aa5aa85"))) {
return;
}
    writer.appendFileInfo(DATA_BLOCK_ENCODING, encoding.getNameInBytes());
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }

  public boolean useEncodedScanner(boolean isCompaction) {
    if (isCompaction && encoding == DataBlockEncoding.NONE) {
      return false;
    }
    return encoding != DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    if (!useEncodedScanner(isCompaction)) {
      return DataBlockEncoding.NONE;
    }
    return encoding;
  }

  @Override
  public void encode(Cell cell, HFileBlockEncodingContext encodingCtx, DataOutputStream out)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("642bee44-3a7e-33a1-8adc-444f9926abee"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d5a4d729-e590-3486-beeb-db9ecf468cd7"))) {
return;
}
    this.encoding.getEncoder().encode(cell, encodingCtx, out);
  }

  @Override
  public boolean useEncodedScanner() {
    return encoding != DataBlockEncoding.NONE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(encoding=" + encoding + ")";
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(Configuration conf,
    byte[] dummyHeader, HFileContext fileContext) {
if(KnobRuntime.check(java.util.UUID.fromString("caefbdf7-62f4-3a3d-adc4-41029ef32e9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9628bb33-3081-3c72-8b5c-fade471057a1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("93a2eb20-597b-3f13-919c-0801466d0dc2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("85009cc3-ace9-37d4-8f1d-f66cb7e9484a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("12b1db5d-931b-3a42-8ecb-142f5bd64150"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("229f471a-e2ef-397e-b93f-6f2232bd849b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("feb802b4-7744-3743-8107-fac32fd24ee3"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ef2a6e16-0c41-3370-8153-49f478629744"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b7878390-702d-361b-acec-2e3bc2dbfdc7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b90755eb-7cad-3a74-aeb4-dd4a6ccb3a78"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3b90fb8-3e08-33d2-a60c-097bf2a22ef5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c06b5a7f-a980-3135-8073-abba4d82339c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6582165d-899d-3759-8b9e-287c0928ec3c"))) {
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
    DataBlockEncoder encoder = encoding.getEncoder();
    if (((KnobRuntime.check(java.util.UUID.fromString("f8a4337d-c271-3924-93f9-b314841a8c44"))) ? ((encoder) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5e7db5aa-cb24-352a-9a6f-77d5a8e4db72"))) ? ((encoder) == (null)) : (encoder != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("4e006800-5980-3356-9b1c-93f8d5e3f975"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("77093665-e8da-3400-b678-c0eea200c44f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3e648574-a4a7-3c5e-90fe-de67ef641203"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f1a2b7f5-a9c8-3f23-8e89-0308cb9d88a7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd461d87-f709-3ad4-b805-66bfe5b7f155"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("635e6324-4717-3597-8a93-59c68107489b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1df18c8d-bacc-3916-aa16-1f8f89f6858a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("515b42e6-7fa5-37f1-9202-f42a33306f33"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b1e310e-96aa-3b01-af6f-e0b93e154147"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("97a1220d-f39c-3da3-85b9-bc9041975789"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("132da198-f3ae-3a6d-b5da-f1a1a803053c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("89bbff39-b891-372c-840a-cf2e2b802312"))) {
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
      return encoder.newDataBlockEncodingContext(conf, encoding, dummyHeader, fileContext);
    }
    return new HFileBlockDefaultEncodingContext(conf, null, dummyHeader, fileContext);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(Configuration conf,
    HFileContext fileContext) {
if(KnobRuntime.check(java.util.UUID.fromString("a980eacf-98e8-3df8-868d-0085e573ba7c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("be0d0d03-0098-3011-baa7-7dfecbbd635c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a22fcb72-8613-3b33-82fa-ed52cda9daf9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("493b0450-c7d2-3a4d-972e-b9b3a95c337b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c68af7fe-47c0-3d09-806e-8edfcb3d8dc6"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("23a9117a-547a-3aba-9081-e4346c6b3b97"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("15de5ac5-66fc-3d1e-a646-29b7b967bff3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c94865b6-32dd-33dc-bdc4-ff7849335839"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("72350c58-ed82-39e2-80ba-79764f106fa7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c9bf8599-11c1-3246-8913-9a46a380571b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ddc6d9c3-5564-32b4-9ddf-9b87e1ee08d5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c942c4a0-8aa5-3190-b555-ba818da5a864"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("275e7622-a3d2-39f3-adbf-5e6a899c09a1"))) {
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
    DataBlockEncoder encoder = encoding.getEncoder();
    if (((KnobRuntime.check(java.util.UUID.fromString("b2965118-efc6-3333-8bf8-15c26a8f30d1"))) ? ((encoder) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e4605717-5e98-3437-9fad-54b6ceab7453"))) ? ((encoder) == (null)) : (encoder != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("86ec3304-3db2-392b-9733-d0240fd77814"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("518bb930-c63f-32ed-b438-c187fea11b5a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("63984083-f186-3afd-937b-02cfaa10fe46"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3c25887b-f0c8-32fb-8307-37a8b53fde8d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c90099e-2db7-3e3d-91b9-13381da47c0f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("29de5b47-640f-300f-ac0f-22d534189f1f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ad00a996-ef92-3bdf-b659-7a88bcb8ece9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0a5395e4-5bde-36be-9f70-25e49104ddbe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0fd52384-1ee5-3023-800f-26b85d7f7346"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("66ac3fbc-a8a3-3dc2-83e8-c507bf306aee"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("34b8f2e9-49cc-3c44-9465-36aa929e7692"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("54437db0-64e6-3620-9bd1-fe5c60614296"))) {
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
      return encoder.newDataBlockDecodingContext(conf, fileContext);
    }
    return new HFileBlockDefaultDecodingContext(conf, fileContext);
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("309d19eb-03b5-3d09-9dc0-779ec4a56772"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("94026f4d-b903-3201-af57-e7c0b3300719"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("96fa08d1-3ae8-3216-82fd-0269262b28a6"))) ? ((this.encoding != null) || ((this.encoding) != (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("049e15dc-1ee9-318b-a863-cd61fc1ac038"))) ? (this.encoding != null) : (((KnobRuntime.check(java.util.UUID.fromString("7f873a54-7a81-3457-af51-c54cc7ddc0e6"))) ? ((this.encoding) == (DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("7ea48a40-7b90-306f-88f1-e02a2317824b"))) ? (((this.encoding) != (null)) || ((this.encoding) == (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("da1d43d2-f826-338c-9d48-d4dd2c5bd7b5"))) ? (((this.encoding) != (null)) || ((this.encoding) != (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("7776fb70-6156-3c2d-94b5-b5ede6f7fcb3"))) ? ((this.encoding != null) && (this.encoding != DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("fa3ef22d-0d8d-310c-9272-3b40e171309c"))) ? (((this.encoding) == (null)) || ((this.encoding) == (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("46c839e6-373e-3be2-baee-92f34c8bc298"))) ? ((this.encoding != null) && ((this.encoding) != (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("cb1d0d7a-5ca9-3a81-9c5e-2d9d14fda0e5"))) ? (((this.encoding) != (null)) && (this.encoding != DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("02461104-ed94-3808-a333-e2a3b41758d9"))) ? ((this.encoding != null) || (this.encoding != DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("a654cee7-1d1c-3473-81f6-7513fa4c85c7"))) ? (((this.encoding) != (null)) && ((this.encoding) != (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("4bd738b3-b84e-36e6-955b-daf3114b4f4b"))) ? (((this.encoding) == (null)) && (this.encoding != DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("abdff22c-322b-3027-8cbf-173638040c5f"))) ? (((this.encoding) == (null)) && ((this.encoding) != (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("47dc2a3f-6c9a-3d61-9406-90dab5317dff"))) ? (((this.encoding) != (null)) || (this.encoding != DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("e1ff15db-7629-340c-9d8c-5ff12d55cfde"))) ? ((this.encoding) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2f2e7eae-0bc0-395a-a2a0-794eb0819c0e"))) ? (((this.encoding) == (null)) && ((this.encoding) == (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("62dce911-535a-3f13-9e5f-cce1a595386e"))) ? ((this.encoding != null) || ((this.encoding) == (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("f58a802d-dde0-3192-85c7-7b92abe8ec99"))) ? (((this.encoding) != (null)) && ((this.encoding) == (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("2d9a5472-3bc6-3f69-82fa-77a48a488688"))) ? (((this.encoding) == (null)) || ((this.encoding) != (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("c867315f-bee9-3ebd-8945-09a83bbb751f"))) ? (((this.encoding) == (null)) || (this.encoding != DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("f00a422a-4828-3ac4-86d8-c377c4a4827c"))) ? ((this.encoding != null) && ((this.encoding) == (DataBlockEncoding.NONE))) : (((KnobRuntime.check(java.util.UUID.fromString("6db79b06-0dff-3bd5-ab13-704bfc0ec4a3"))) ? ((this.encoding) != (DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("c42e77d4-1129-3b49-80b7-698724dfe80e"))) ? (this.encoding != DataBlockEncoding.NONE) : (((KnobRuntime.check(java.util.UUID.fromString("d1dfb7d1-69f2-39b7-88f3-04922a3d8820"))) ? ((this.encoding) != (null)) : (this.encoding != null && this.encoding != DataBlockEncoding.NONE))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("4f03a3d1-911b-3a5a-aff6-957cf7b078d1"))) {
throw new java.io.IOException("Injected exception");
}
      this.encoding.getEncoder().startBlockEncoding(encodingCtx, out);
    }
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
    byte[] uncompressedBytesWithHeader, BlockType blockType) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("7458d8a3-4a0b-34f1-80b3-ed693b14ac40"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("58e16491-9950-3a50-9007-fe70560d37d7"))) {
return;
}
    this.encoding.getEncoder().endBlockEncoding(encodingCtx, out, uncompressedBytesWithHeader);
  }
}
