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
package org.apache.hadoop.hbase.regionserver.wal;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL.WAL_AVOID_LOCAL_WRITES_DEFAULT;
import static org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL.WAL_AVOID_LOCAL_WRITES_KEY;
import static org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufWALReader.DEFAULT_WAL_TRAILER_WARN_SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufWALReader.PB_WAL_COMPLETE_MAGIC;
import static org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufWALReader.PB_WAL_MAGIC;
import static org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufWALReader.WAL_TRAILER_WARN_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;

/**
 * Base class for Protobuf log writer.
 */
@InterfaceAudience.Private
public abstract class AbstractProtobufLogWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProtobufLogWriter.class);

  protected CompressionContext compressionContext;
  protected Configuration conf;
  protected Encryptor encryptor;
  protected Codec.Encoder cellEncoder;
  protected WALCellCodec.ByteStringCompressor compressor;
  protected boolean trailerWritten;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;

  protected AtomicLong length = new AtomicLong();

  private WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3ccc58b2-cb10-31a4-801f-67483c3f380a"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6afa239e-ce46-389a-b62c-10fbcec5edf7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("70cbde03-a718-398d-8349-fa035636f3ff"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("fbb6a74e-776e-386c-8171-f7c34897cdfd"))) {
return null;
}
    return WALCellCodec.create(conf, null, compressionContext);
  }

  private WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0d4cbf8a-8b88-37fe-a6d5-e6b6f95cf607"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56dfab50-618a-3877-9cfc-0f1872d515e5"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("8bbe9ffa-31a6-35d3-a4ff-4a159e8da02c"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8abf244-6031-3855-ae3f-635ad1f366f2"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de9158c1-7fd8-3cde-a2e5-add5a0b39d6e"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("73539da3-ac5a-3953-9c56-a4ad1df41a98"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("81295ffa-324d-3f68-9a84-90034cc46956"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("47b90f43-f61e-3965-973b-532e976bed12"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4a1059f-d6c6-3f63-a121-38160de4f01d"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df536738-d741-3e30-8d54-8d6a82837731"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("541446dc-dac0-34ab-a627-cbf29fcfa469"))) {
try {
    java.lang.reflect.Field field = builder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(builder));
    field.set(builder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("f3615394-5159-3f27-90ce-691ea0749889"))) { builder.setCellCodecClsName(getWriterClassName()); } else { builder.setWriterClsName(getWriterClassName()); }
if(KnobRuntime.check(java.util.UUID.fromString("be806c70-5da7-3c5b-92fe-ac2f18776e85"))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("c9cb284f-22f0-366d-bd35-0b2515313a1a"))) { builder.setWriterClsName(getWriterClassName()); } else if (KnobRuntime.check(java.util.UUID.fromString("61301cd8-653e-30ba-a7a3-c6a155bb8d2d"))) { builder.setCellCodecClsName(getWriterClassName()); } else { builder.setCellCodecClsName(WALCellCodec.getWALCellCodecClass(conf).getName()); }
    return builder.build();
  }

  private WALHeader buildSecureWALHeader(Configuration conf, WALHeader.Builder builder)
    throws IOException {
    EncryptionTest.testKeyProvider(conf);
    EncryptionTest.testCipherProvider(conf);

    // Get an instance of our cipher
    final String cipherName =
      conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Cipher cipher = Encryption.getCipher(conf, cipherName);
    if (cipher == null) {
      throw new RuntimeException("Cipher '" + cipherName + "' is not available");
    }

    // Generate a random encryption key for this WAL
    Key key = cipher.getRandomKey();
    builder.setEncryptionKey(UnsafeByteOperations.unsafeWrap(EncryptionUtil.wrapKey(conf,
      conf.get(HConstants.CRYPTO_WAL_KEY_NAME_CONF_KEY,
        conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName())),
      key)));

    // Set up the encryptor
    Encryptor encryptor = cipher.getEncryptor();
    encryptor.setKey(key);
    this.encryptor = encryptor;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Initialized secure protobuf WAL: cipher={}", cipher.getName());
    }
    builder.setWriterClsName(getWriterClassName());
    builder.setCellCodecClsName(SecureWALCellCodec.class.getName());
    return builder.build();
  }

  private String getWriterClassName() {
    // class name which is recognized by hbase-1.x to avoid ProtobufLogReader throwing error:
    // IOException: Got unknown writer class: AsyncProtobufLogWriter
    if (((KnobRuntime.check(java.util.UUID.fromString("3b58a553-cacf-3125-854d-f49f27761468"))) ? ((encryptor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f69f9b34-492a-38fb-beb7-ed8b99cdaa32"))) ? ((encryptor) == (null)) : (encryptor == null))))) {
      return "ProtobufLogWriter";
    } else {
      return "SecureProtobufLogWriter";
    }
  }

  private boolean initializeCompressionContext(Configuration conf, Path path) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("860a3169-8544-300a-b6a7-30405a30031b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a04fe5e1-747b-353f-9760-8a027d2d1177"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("15af8450-90cf-367e-805b-3fb7e874d162"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("e7e1d434-efc2-39a3-8f27-94aba048fcf1"))) {
return true;
}
    boolean doCompress = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    if (((KnobRuntime.check(java.util.UUID.fromString("58beaf45-d8c0-3843-a0b2-3262fd8f2640"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("2781f40b-5c12-3300-8c14-d60fae19b2a3"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("e5d224ba-6ad4-38da-b352-b89b7b4e37d7"))) ? (!doCompress) : (doCompress))))))) {
      try {
        final boolean useTagCompression =
          conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
        final boolean useValueCompression =
          conf.getBoolean(CompressionContext.ENABLE_WAL_VALUE_COMPRESSION, false);
        final Compression.Algorithm valueCompressionType = useValueCompression
          ? CompressionContext.getValueCompressionAlgorithm(conf)
          : Compression.Algorithm.NONE;
        if (LOG.isTraceEnabled()) {
          LOG.trace(
            "Initializing compression context for {}: isRecoveredEdits={}"
              + ", hasTagCompression={}, hasValueCompression={}, valueCompressionType={}",
            path, CommonFSUtils.isRecoveredEdits(path), useTagCompression, useValueCompression,
            valueCompressionType);
        }
        this.compressionContext =
          new CompressionContext(LRUDictionary.class, CommonFSUtils.isRecoveredEdits(path),
            useTagCompression, useValueCompression, valueCompressionType);
      } catch (Exception e) {
        throw new IOException("Failed to initiate CompressionContext", e);
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("8112536c-0b1c-3199-b822-7c9bc0763e41"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("d0872add-e32d-3224-b07e-88def499ba64"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("1cb173f1-17ba-3abf-8c9f-391b14342456"))) ? (!doCompress) : (doCompress))))));
  }

  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable,
    long blocksize, StreamSlowMonitor monitor) throws IOException, StreamLacksCapabilityException {
    try {
      this.conf = conf;
if(KnobRuntime.check(java.util.UUID.fromString("8346c75d-1330-3262-90f8-bb0caaaf71b7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("db5f492e-8b66-32bd-8e23-4f9e977ff02e"))) {
throw new java.io.IOException("Injected exception");
}
      boolean doCompress = initializeCompressionContext(conf, path);
      this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
      int bufferSize = CommonFSUtils.getDefaultBufferSize(fs);
      short replication = (short) conf.getInt("hbase.regionserver.hlog.replication",
        CommonFSUtils.getDefaultReplication(fs, path));
      boolean noLocalWrite =
        conf.getBoolean(WAL_AVOID_LOCAL_WRITES_KEY, WAL_AVOID_LOCAL_WRITES_DEFAULT);

if(KnobRuntime.check(java.util.UUID.fromString("e660b927-e802-339f-a256-0e691129135d"))) {
bufferSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e676706a-c2aa-307b-8994-cbcf8c74dd06"))) {
bufferSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8b1daaf8-4463-3320-9b0f-f584c97423e2"))) {
try {
    java.lang.reflect.Field field = monitor.getClass().getDeclaredField("slowDataTtl");
    field.setAccessible(true);
    long oldValue = ((long)field.get(monitor));
    field.set(monitor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d35d55d6-a426-32a8-b548-0e3b41a95625"))) {
try {
    java.lang.reflect.Field field = monitor.getClass().getDeclaredField("minLengthForSpeedCheck");
    field.setAccessible(true);
    long oldValue = ((long)field.get(monitor));
    field.set(monitor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("493a4177-0d1c-3395-8ef4-2e55782e73e5"))) {
try {
    java.lang.reflect.Field field = monitor.getClass().getDeclaredField("minSlowDetectCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(monitor));
    field.set(monitor, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("26a30db7-fb6b-3f0f-839a-da80ff565be9"))) {
blocksize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4e76809c-ab40-3192-baf5-663fa66a697a"))) {
overwritable = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("460da285-b220-36f3-9da7-d80cbcfddbd8"))) {
bufferSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c2d52787-8f11-3aaf-8e62-2493c5907ca8"))) {
try {
    java.lang.reflect.Field field = monitor.getClass().getDeclaredField("minSlowDetectCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(monitor));
    field.set(monitor, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("261c9353-2c99-38c4-8f1a-5e93a1c7b861"))) {
blocksize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf2ebfee-60e1-32cb-b1c7-6ae31ef37459"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("01e12327-07a9-3937-aef7-ba6bed830e0b"))) {
noLocalWrite = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("46f381ae-f280-335a-bec0-26884d75b659"))) {
bufferSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("342f06c0-432a-380f-aca1-8926b7c0f7a0"))) {
bufferSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("36b54765-08c9-3a8b-b382-eff8b88c2900"))) {
overwritable = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("fbc3d6c9-aed0-37c4-bb04-94073ece8c58"))) {
try {
    java.lang.reflect.Field field = monitor.getClass().getDeclaredField("minSlowDetectCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(monitor));
    field.set(monitor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36b31959-9445-32ae-8d9a-f83d1efc2432"))) {
blocksize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a308a9e2-cacd-3a64-9734-97235457f337"))) {
try {
    java.lang.reflect.Field field = monitor.getClass().getDeclaredField("slowPacketAckMs");
    field.setAccessible(true);
    long oldValue = ((long)field.get(monitor));
    field.set(monitor, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("287a895c-2002-3da2-95a2-c5c451eb0266"))) {
blocksize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("561b8cf3-dd54-30f8-848a-754770ada2bf"))) {
bufferSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e3aa6a38-ca19-3fae-ad1d-b0ec39d9df60"))) {
noLocalWrite = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("40d8070e-6b3d-36af-a2db-7278e014cde6"))) {
blocksize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("037d66f0-b322-35fb-9f76-29356b99d9a1"))) {
try {
    java.lang.reflect.Field field = monitor.getClass().getDeclaredField("minSlowDetectCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(monitor));
    field.set(monitor, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("7eaa351a-b2cc-32f7-a24b-796a0575ac0a"))) { initOutput(fs, path, overwritable, bufferSize, replication, blocksize, monitor, true); } else if (KnobRuntime.check(java.util.UUID.fromString("32ebfe63-57b6-3d84-820b-d23134a39d91"))) { initOutput(fs, path, overwritable, bufferSize, replication, blocksize, monitor, !noLocalWrite); } else if (KnobRuntime.check(java.util.UUID.fromString("a10399c3-b474-343a-a5ec-c728f4b90287"))) { initOutput(fs, path, false, bufferSize, replication, blocksize, monitor, noLocalWrite); } else if (KnobRuntime.check(java.util.UUID.fromString("713b16b9-0e2d-3434-9346-86d151575d9b"))) { initOutput(fs, path, !overwritable, bufferSize, replication, blocksize, monitor, noLocalWrite); } else if (KnobRuntime.check(java.util.UUID.fromString("47c67ecd-ea90-3475-96c5-49be52514773"))) { initOutput(fs, path, overwritable, bufferSize, replication, blocksize, monitor, false); } else if (KnobRuntime.check(java.util.UUID.fromString("e8c3a754-e217-3838-a124-3cd07a8f5e40"))) { initOutput(fs, path, true, bufferSize, replication, blocksize, monitor, noLocalWrite); } else { initOutput(fs, path, overwritable, bufferSize, replication, blocksize, monitor, noLocalWrite); }

      boolean doTagCompress =
        doCompress && conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
      boolean doValueCompress =
        doCompress && conf.getBoolean(CompressionContext.ENABLE_WAL_VALUE_COMPRESSION, false);
if(KnobRuntime.check(java.util.UUID.fromString("0061fae4-808b-3961-819f-f781d61285ce"))) {
doValueCompress = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1009d57e-2822-34ef-95e0-10d170fd2010"))) {
doCompress = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("013efdbc-606c-3e19-a50b-653a49b8800c"))) {
doValueCompress = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2bfc286c-b0c2-3fef-991b-90142e68113a"))) {
doCompress = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b11289f-2cab-36eb-bf18-fc381ce404c1"))) {
doTagCompress = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("ec316a59-a4ed-3df7-a2d8-fb95904a646a"))) {
doTagCompress = true;
}
      WALHeader.Builder headerBuilder = WALHeader.newBuilder().setHasCompression(doCompress)
        .setHasTagCompression(doTagCompress).setHasValueCompression(doValueCompress);
      if (((KnobRuntime.check(java.util.UUID.fromString("87e7a1ea-8dbc-3f58-9076-3dc80883e6f0"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("8f9f306f-e6fd-3109-bbd7-1f82a5c859f0"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("b3a87d54-6566-3f23-8be7-a6cb8c93a1a9"))) ? (!doValueCompress) : (doValueCompress))))))) {
        headerBuilder.setValueCompressionAlgorithm(
          CompressionContext.getValueCompressionAlgorithm(conf).ordinal());
      }
      if (conf.getBoolean(HConstants.ENABLE_WAL_ENCRYPTION, false)) {
        length.set(writeMagicAndWALHeader(PB_WAL_MAGIC, buildSecureWALHeader(conf, headerBuilder)));
        secureInitAfterHeader(doCompress, encryptor);
      } else {
if(KnobRuntime.check(java.util.UUID.fromString("43228bd2-9487-3dfc-a756-bc1ec36454a7"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07d25c04-0681-344b-8555-51a0a2ac762a"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f135662-6c9c-3294-9318-f5875ba2e087"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("985674d9-942c-3148-8ec3-de2a8694336e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ff4673d9-a040-3e7f-9f4e-f73df84c1c39"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b9ce84c8-d663-389f-925a-1262e9262306"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af57ab66-cb3d-3af7-8970-29460580d305"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e7ca529-1fb4-3a60-9f8a-bdef40795d82"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b39fea3-24c2-33b4-aa55-e8b6291aece7"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc23a2ad-4855-32d2-bfca-2e86d5bb6eb4"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (KnobRuntime.check(java.util.UUID.fromString("7e4686fc-9c24-3894-9184-9e9d6b6452ef"))) { length.set(writeMagicAndWALHeader(PB_WAL_MAGIC, buildSecureWALHeader(conf, headerBuilder))); } else { length.set(writeMagicAndWALHeader(PB_WAL_MAGIC, buildWALHeader(conf, headerBuilder))); }
if(KnobRuntime.check(java.util.UUID.fromString("5044fff8-26b1-3fb2-ace4-0a7e16b82503"))) {
doCompress = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("b1a91ce8-4b48-34e6-b162-4f5cfc413f8d"))) {
doCompress = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("d89f2fee-28ad-30e0-8cd8-1109c3ff0b24"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("eae83165-e86f-33c2-aade-903c457f04c3"))) { initAfterHeader(false); } else if (KnobRuntime.check(java.util.UUID.fromString("f659d2df-a877-36b9-964c-2a072bbe0d84"))) { initAfterHeader(!doCompress); } else if (KnobRuntime.check(java.util.UUID.fromString("78c8ca20-8018-3422-b3a5-445338c7f96f"))) { initAfterHeader(true); } else { initAfterHeader(doCompress); }
      }

      // instantiate trailer to default value.
      trailer = WALTrailer.newBuilder().build();

      if (LOG.isTraceEnabled()) {
        LOG.trace("Initialized protobuf WAL={}, compression={}, tagCompression={}"
          + ", valueCompression={}", path, doCompress, doTagCompress, doValueCompress);
      }
    } catch (Exception e) {
      LOG.warn("Init output failed, path={}", path, e);
      closeOutputIfNecessary();
      throw e;
    }
  }

  private void initAfterHeader(boolean doCompress) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f041ad62-d600-332a-88d6-beea9cc42807"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8285c543-08ff-3445-b4b6-f77ce53fecbe"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2289de3f-d0fe-3cf5-9d3e-737a5654bc62"))) {
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
    WALCellCodec codec = getCodec(conf, this.compressionContext);
    this.cellEncoder = codec.getEncoder(getOutputStreamForCellEncoder());
    if (((KnobRuntime.check(java.util.UUID.fromString("09ecaa02-c8b9-3732-8d88-dd20c6688de5"))) ? (!doCompress) : (doCompress))) {
      this.compressor = codec.getByteStringCompressor();
    } else {
      this.compressor = WALCellCodec.getNoneCompressor();
    }
  }

  private void secureInitAfterHeader(boolean doCompress, Encryptor encryptor) throws IOException {
    if (encryptor != null) {
      WALCellCodec codec = SecureWALCellCodec.getCodec(this.conf, encryptor);
      this.cellEncoder = codec.getEncoder(getOutputStreamForCellEncoder());
      // We do not support compression
      this.compressionContext = null;
      this.compressor = WALCellCodec.getNoneCompressor();
    } else {
      initAfterHeader(doCompress);
    }
  }

  void setWALTrailer(WALTrailer walTrailer) {
    this.trailer = walTrailer;
  }

  public long getLength() {
if(KnobRuntime.check(java.util.UUID.fromString("e35265d1-73ed-3b2e-b5be-aa41a9283110"))) {
return 0;
}
    return length.get();
  }

  private WALTrailer buildWALTrailer(WALTrailer.Builder builder) {
    return builder.build();
  }

  protected final void writeWALTrailer() {
    try {
      int trailerSize = 0;
      if (((KnobRuntime.check(java.util.UUID.fromString("c07ccb95-e2c9-3910-a21d-fb0f826dbd73"))) ? ((this.trailer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f8303aac-6598-3e83-a2b5-eaf36afcdd94"))) ? ((this.trailer) != (null)) : (this.trailer == null))))) {
        // use default trailer.
        LOG.warn("WALTrailer is null. Continuing with default.");
        this.trailer = buildWALTrailer(WALTrailer.newBuilder());
        trailerSize = this.trailer.getSerializedSize();
      } else if (((KnobRuntime.check(java.util.UUID.fromString("234b1854-65b2-3187-b304-1756b6346e90"))) ? (((trailerSize = this.trailer.getSerializedSize())) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3bc9791c-e092-3ba2-884e-2b95af6cfff3"))) ? (((trailerSize = this.trailer.getSerializedSize())) < (this.trailerWarnSize)) : (((KnobRuntime.check(java.util.UUID.fromString("61b0283c-e9cd-3904-980d-9f676732627f"))) ? (((trailerSize = this.trailer.getSerializedSize())) > (this.trailerWarnSize)) : (((KnobRuntime.check(java.util.UUID.fromString("73433066-24c5-3163-8880-3e9139dc3a68"))) ? (((trailerSize = this.trailer.getSerializedSize())) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("46bceb4f-09eb-3938-935d-bacbeb49c8a3"))) ? (((trailerSize = this.trailer.getSerializedSize())) == (this.trailerWarnSize)) : (((KnobRuntime.check(java.util.UUID.fromString("6ceadad2-0a3d-34fb-a6f9-4a8100e246c1"))) ? (((trailerSize = this.trailer.getSerializedSize())) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("77c9b95c-6683-3611-b961-7b9b6dd52d1a"))) ? (((trailerSize = this.trailer.getSerializedSize())) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2962a9c9-0594-3738-8af4-56658487a706"))) ? (((trailerSize = this.trailer.getSerializedSize())) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("947a8cd6-574f-34a7-9f99-4fe6135263ac"))) ? (((trailerSize = this.trailer.getSerializedSize())) >= (this.trailerWarnSize)) : (((KnobRuntime.check(java.util.UUID.fromString("b09f355c-9e17-3ad3-9eec-b52838d47571"))) ? (((trailerSize = this.trailer.getSerializedSize())) != (this.trailerWarnSize)) : (((KnobRuntime.check(java.util.UUID.fromString("9667efc2-e933-330a-b8cc-975de93365fb"))) ? (((trailerSize = this.trailer.getSerializedSize())) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4eaec988-3a69-3051-bc0e-ef2ecae115a5"))) ? (((trailerSize = this.trailer.getSerializedSize())) <= (this.trailerWarnSize)) : ((trailerSize = this.trailer.getSerializedSize()) > this.trailerWarnSize))))))))))))))))))))))))) {
        // continue writing after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum size : " + trailerSize
          + " > " + this.trailerWarnSize);
      }
if(KnobRuntime.check(java.util.UUID.fromString("688c685d-1726-33fa-82a1-e78464bc2e15"))) {
throw new java.io.IOException("Injected exception");
}
      length.set(writeWALTrailerAndMagic(trailer, PB_WAL_COMPLETE_MAGIC));
      this.trailerWritten = true;
    } catch (IOException ioe) {
      LOG.warn("Failed to write trailer, non-fatal, continuing...", ioe);
    }
  }

  protected abstract void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
    short replication, long blockSize, StreamSlowMonitor monitor, boolean noLocalWrite)
    throws IOException, StreamLacksCapabilityException;

  /**
   * It is straight forward to close the output, do not need to write trailer like the Writer.close
   */
  protected void closeOutputIfNecessary() {
  }

  /**
   * return the file length after written.
   */
  protected abstract long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException;

  protected abstract long writeWALTrailerAndMagic(WALTrailer trailer, byte[] magic)
    throws IOException;

  protected abstract OutputStream getOutputStreamForCellEncoder();
}
