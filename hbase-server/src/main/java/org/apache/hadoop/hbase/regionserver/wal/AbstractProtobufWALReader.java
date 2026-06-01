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

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.Key;
import java.security.KeyException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;

/**
 * Base class for reading protobuf based wal reader
 */
@InterfaceAudience.Private
public abstract class AbstractProtobufWALReader
  implements AbstractFSWALProvider.Initializer, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProtobufWALReader.class);

  // public for WALFactory until we move everything to o.a.h.h.wal
  public static final byte[] PB_WAL_MAGIC = Bytes.toBytes("PWAL");

  // public for TestWALSplit
  public static final byte[] PB_WAL_COMPLETE_MAGIC = Bytes.toBytes("LAWP");

  /**
   * Configuration name of WAL Trailer's warning size. If a waltrailer's size is greater than the
   * configured size, providers should log a warning. e.g. this is used with Protobuf reader/writer.
   */
  static final String WAL_TRAILER_WARN_SIZE = "hbase.regionserver.waltrailer.warn.size";
  static final int DEFAULT_WAL_TRAILER_WARN_SIZE = 1024 * 1024; // 1MB

  private static final List<String> WRITER_CLS_NAMES = ImmutableList.of(
    ProtobufLogWriter.class.getSimpleName(), AsyncProtobufLogWriter.class.getSimpleName(),
    "SecureProtobufLogWriter", "SecureAsyncProtobufLogWriter");

  protected Configuration conf;

  protected FileSystem fs;

  protected Path path;

  protected long fileLength;

  protected FSDataInputStream inputStream;

  protected CompressionContext compressionCtx;
  protected boolean hasCompression = false;
  protected boolean hasTagCompression = false;
  protected boolean hasValueCompression = false;
  protected Compression.Algorithm valueCompressionType;

  protected Codec.Decoder cellDecoder;
  protected WALCellCodec.ByteStringUncompressor byteStringUncompressor;

  protected long walEditsStopOffset;
  protected boolean trailerPresent;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;

  // cell codec classname
  protected String codecClsName;

  protected Decryptor decryptor;

  /**
   * Get or create the input stream used by cell decoder.
   * <p/>
   * For implementing replication, we may need to limit the bytes we can read, so here we provide a
   * method so subclasses can wrap the original input stream.
   */
  protected abstract InputStream getCellCodecInputStream(FSDataInputStream stream);

  /**
   * Skip to the given position.
   */
  protected abstract void skipTo(long position) throws IOException;

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, long startPosition)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1dd7579a-ba41-3d17-8253-eef22e690aa4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("297b9fd7-95ba-3417-a659-30606c87c571"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bd36cdef-6223-3ad1-bef2-8dbf79f9b045"))) {
return;
}
    this.conf = conf;
    this.path = path;
    this.fs = fs;
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);

if(KnobRuntime.check(java.util.UUID.fromString("74dcd301-7575-38e5-9dcb-983517a09f3d"))) {
throw new java.io.IOException("Injected exception");
}
    Pair<FSDataInputStream, FileStatus> pair = open();
    FSDataInputStream stream = pair.getFirst();
    FileStatus stat = pair.getSecond();
    boolean initSucceeded = false;
    try {
      // read the header
if(KnobRuntime.check(java.util.UUID.fromString("b042a290-e25a-3c00-a146-90dc17dc5963"))) {
throw new java.io.IOException("Injected exception");
}
      WALProtos.WALHeader header = readHeader(stream);
      // initialize metadata and fields
if(KnobRuntime.check(java.util.UUID.fromString("76c26709-b647-3d85-a95f-131fdbd45e1c"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14cfb162-c3cc-30c2-a3e5-99775995f38e"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51de5916-939c-3c46-b08c-8cf2755073f4"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3c2c5c3-fcad-3029-98a7-93b4e8d4743d"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6ebd0fa-f75f-3f0e-8396-f0613dc927e6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d63c6c0b-c2d5-3772-8e6b-4f8818ff3724"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5ad71849-b257-3f5f-8fd2-cbd3a3b53255"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6ba1e75-5c40-39d4-bd09-fa221599d928"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5e5af5a-afc5-3044-9982-aba72b00f3fb"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("1b409ff4-1577-34f8-bc9a-142a60c86e7c"))) { initCompression(header); } else { initDecryptor(header); }
if(KnobRuntime.check(java.util.UUID.fromString("655c75fc-a5bc-34f1-a228-4faae9250c28"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("318a4eb2-d121-377f-abf7-20cf48dabd79"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41f2e0d0-2d1c-36de-b251-af002db8e9de"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("614562a9-3971-36cf-b6f0-52d56866fcb4"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48c3b9f2-dd82-37f6-b544-4a3539b93930"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("73e6234b-7201-3633-acbc-2def1a84d158"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95ad2889-3569-3c29-8b28-ef7da3e18164"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5718630c-968e-3798-9e11-6203238e38c6"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2295b2bb-840e-319e-b172-89a96920074f"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("56f8fe26-e319-3ecf-8558-c9074d6968dc"))) { initDecryptor(header); } else { initCompression(header); }
if(KnobRuntime.check(java.util.UUID.fromString("612cfc83-f3ad-3faf-b4e6-60d8d7dde1dc"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("277df2f4-46db-3573-9c7a-147937ce29c5"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2e6abf4e-2a65-3022-9bc1-1dd3feb8dee4"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("34228a0c-9408-3957-8a01-6caa6a6ed174"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("44c6ccb2-e397-3fa5-b4f5-999026ff8fd1"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e2899ed-04be-3975-9e59-62334aada42c"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9fbb78a-76c0-334b-8567-719ae6177e16"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8971b17-ca68-3279-af95-2332976c2c88"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0816ef16-0d62-3073-b38a-6628ca2fbd0d"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      initWALCellCodec(header, getCellCodecInputStream(stream));

      // read trailer if available
if(KnobRuntime.check(java.util.UUID.fromString("9008edd8-ce62-3e2b-ae8f-76c2a4bf824d"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5dfbc7a-903f-3111-a4a1-c23c1b84ba28"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d61d631-bc2b-39f0-9eba-2e573730a93e"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b42d1af5-899f-37ee-baab-fc4e34218f3f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("aab8230e-8cc4-335b-8188-a46d87b8c9b8"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d510283-ee25-33e0-8cbf-f4263a36cd4f"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      readTrailer(stream, stat);

      // this is intentional as we do not want the above methods to use the inputStream field. For
      // implementation tailing reader, we need to wrap the input stream when creating cell decoder,
      // so we need to make sure in the above methods we do not accidentally use the stored
      // inputStream directly and cause trouble. If a method needs to use an input stream, we just
      // pass the input stream in, like readHeader and readTrailer.
      this.inputStream = stream;

      // seek to the given position if it is not -1
      if (((KnobRuntime.check(java.util.UUID.fromString("54397267-c85b-3681-983e-05598df0ad53"))) ? (startPosition >= 0) : (((KnobRuntime.check(java.util.UUID.fromString("5a61fc11-0c53-3265-9453-fe612f16253c"))) ? (startPosition != inputStream.getPos()) : (((KnobRuntime.check(java.util.UUID.fromString("6d13abe5-d3bb-37a5-811b-81432051aec3"))) ? ((startPosition) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4c880b28-c808-33c3-9ad1-0cceb5b3c799"))) ? ((startPosition >= 0) && ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("5cdd8247-b2e2-38c7-8cdf-78451d2b8ffc"))) ? (((startPosition) != (0)) || ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("be8a952b-74ec-312e-88c4-f3e25327af3b"))) ? ((startPosition) == (inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("1689c40f-11e9-34fb-9747-236a5d5d53b7"))) ? (((startPosition) >= (0)) || (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("c86d0dd3-aede-3368-a515-b04f6b4d3e07"))) ? ((startPosition) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ad3cfb41-cd12-3f58-a095-073fd70adc0c"))) ? (((startPosition) >= (0)) && ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("f17778ff-159d-30aa-b910-a0ca87b20694"))) ? (((startPosition) < (0)) && ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("b7938e66-e73f-324e-bf6c-7b761dea90b0"))) ? (((startPosition) != (0)) || (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("d0353394-f28b-3e3f-828c-8e4bed79d992"))) ? ((startPosition >= 0) && ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("4a6966e6-f2c5-3994-87e9-0bc2a0a0eb7b"))) ? (((startPosition) == (0)) || ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("a07517c7-be0f-33f2-a31c-accc9ac6f858"))) ? ((startPosition) != (getPosition())) : (((KnobRuntime.check(java.util.UUID.fromString("a5d2b0ff-4c93-39b7-bf95-3e88c139d609"))) ? (((startPosition) < (0)) && ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("5d7e052e-4614-3a17-91a8-16825b75cb47"))) ? ((startPosition >= 0) || ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("e2c1b9e3-d4d2-3445-98e1-ee3f4c0d14c8"))) ? ((startPosition) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b624d896-f6ab-3ab5-8c80-1123520057ce"))) ? (((startPosition) == (0)) || (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("b90b658e-0882-35b5-bc04-1c64f4f33848"))) ? (((startPosition) == (0)) || ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("a38256f2-2d05-3d27-91be-73ea1b0fd44e"))) ? ((startPosition) == (getPosition())) : (((KnobRuntime.check(java.util.UUID.fromString("1fe7fde5-56ec-337d-a133-4040847f7c15"))) ? (((startPosition) > (0)) && ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("a9c3ff76-68e6-35ce-b12f-c76d73f521dd"))) ? (((startPosition) <= (0)) || ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("dad37bb5-3a53-3aeb-896b-d2608ae82e12"))) ? (((startPosition) >= (0)) || ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("1a2a6a0a-32fa-3047-949e-c305a3ee953a"))) ? (((startPosition) > (0)) && (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("30962e7f-f4da-33f0-9345-6d95e1873f8e"))) ? (((startPosition) <= (0)) && ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("a1c0aad2-3b84-3443-b9ed-f028767d2a3d"))) ? (((startPosition) < (0)) && ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("422b474e-76dd-3c27-b81f-291101cd3a23"))) ? ((startPosition >= 0) || ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("0f3cedc4-b012-32f9-9fc2-4f136278c32e"))) ? (((startPosition) >= (0)) && ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("97e2a8cf-89f4-32f3-a1c7-fd6f3c4da461"))) ? (((startPosition) != (0)) && (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("9c7951bd-7631-3c5e-8c72-b605e367a691"))) ? (((startPosition) > (0)) || ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("88357b23-09a7-335b-8ada-3957c5e57d7f"))) ? (((startPosition) <= (0)) && ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("e07e9cba-ea45-31fd-9c75-183fec9f36ca"))) ? (((startPosition) == (0)) && ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("a68645f0-d24c-3017-b63d-7e7879b053dd"))) ? ((startPosition >= 0) && (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("7f1cf6e1-6c15-3da8-b3dc-1e255f2e0891"))) ? (((startPosition) != (0)) && ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("b2273aa0-f999-3dc1-b61e-2beba643c9e1"))) ? (((startPosition) <= (0)) || (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("ec1a9adf-87f9-3a5a-b96a-14416a2ffaf9"))) ? (((startPosition) < (0)) || ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("bd074ba2-0643-3327-888a-7c1c319a1149"))) ? (((startPosition) >= (0)) && ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("c55859bf-f9ae-3048-ace1-df4176895866"))) ? (((startPosition) >= (0)) && (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("305ab113-0415-3192-9fd6-d3acb8fd0653"))) ? (((startPosition) < (0)) && ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("4569bf67-533c-311a-9c26-bde413124215"))) ? (((startPosition) != (0)) || ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("013608e2-4788-30c2-a909-733d729b758b"))) ? ((startPosition >= 0) && ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("be17d6d5-85a5-383f-8503-338c2ff870fc"))) ? (((startPosition) > (0)) && ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("07c0ad1a-2d03-37e7-a418-a51ab32020ee"))) ? (((startPosition) == (0)) || ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("088fa7c0-5bad-37f7-88b9-f772c3d754b4"))) ? (((startPosition) == (0)) && ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("22dc6169-1b27-35ad-bb21-8be57960a1e2"))) ? (((startPosition) == (0)) && ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("8df67fe7-9038-3580-a237-2596b05875c2"))) ? (((startPosition) > (0)) || ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("4a5fd2cf-b93c-3225-9549-350e0aa2f8d8"))) ? (((startPosition) == (0)) || ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("4f19604b-220c-368e-9a1b-f80d197f9f4a"))) ? ((startPosition) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ca35cf2d-6d98-3df2-a633-79342da485a2"))) ? ((startPosition) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("477673c4-6279-30bc-8f93-d83b6932b24d"))) ? (((startPosition) <= (0)) && ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("ae24c910-c796-35e5-9650-8b0d383af092"))) ? ((startPosition >= 0) || (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("0a1387b3-541d-3962-8fe3-c0c15774a977"))) ? ((startPosition) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("041be6f2-d931-3738-b5b5-05633be9e842"))) ? (((startPosition) >= (0)) || ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("b32d17b8-903c-3a86-bfd1-5b4470082f24"))) ? (((startPosition) <= (0)) && ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("87f4dffb-31d6-37ae-92a2-3ff44d3e0806"))) ? (((startPosition) >= (0)) || ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("f24a1e0b-1adb-3d15-92a0-819a15b3c50c"))) ? (((startPosition) < (0)) || ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("bbf6fa34-a47d-3632-a1b5-53dbf50d2304"))) ? (((startPosition) != (0)) && ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("549d31cd-d449-327b-ad2e-de3c5f6bdbf5"))) ? ((startPosition >= 0) && ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("6f804433-cd8b-3696-a7c7-43465c1b5bc8"))) ? (((startPosition) <= (0)) || ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("ea304ac2-5984-333c-8f56-3622bc0201de"))) ? (((startPosition) >= (0)) && ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("6a33af5f-0a50-3714-96e6-1b2624867fee"))) ? (((startPosition) != (0)) || ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("f899585b-21fe-3372-aa22-662571e6669d"))) ? (((startPosition) <= (0)) || ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("d30f1182-513b-3151-b31e-39d855bdcee1"))) ? (((startPosition) < (0)) || (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("d0f15717-88b2-328c-b057-6d47ec4651a5"))) ? (((startPosition) < (0)) || ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("2db6aaed-fcec-3a75-a1d9-67e65c2a54b6"))) ? (((startPosition) < (0)) || ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("02aff9d4-f72e-3d25-aaf2-51ca70b77750"))) ? (((startPosition) <= (0)) && (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("eafef715-e3cd-37f9-ae9d-f683e000afc6"))) ? ((startPosition >= 0) || ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("b12eac34-2cff-361a-b445-d9c98522a0b0"))) ? ((startPosition) != (inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("2c7ec098-12e6-3b0c-bd16-864d87aa4094"))) ? (((startPosition) != (0)) && ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("b137ed50-0226-313c-ab5e-72c258301a5d"))) ? (((startPosition) != (0)) && ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("94f5b611-9d2a-3bb6-83e8-6e4bd7d63e09"))) ? (((startPosition) >= (0)) || ((startPosition) != (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("3e5bdc2e-cbca-3d6f-b6b6-2f23bb3d24c5"))) ? (((startPosition) == (0)) && (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("7916d18e-d38e-3a5c-839c-a13f7be2a48c"))) ? (((startPosition) > (0)) && ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("397f3f64-fd53-3661-a5c8-4dcbaa670977"))) ? (((startPosition) != (0)) || ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("e069e084-f0a6-3814-b959-8e9392d2533c"))) ? (((startPosition) > (0)) || ((startPosition) == (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("dc8ad125-8f02-3e8e-aec1-8297658dbf25"))) ? (((startPosition) < (0)) && (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("6970f761-859f-32c6-b650-247210e487a5"))) ? (((startPosition) > (0)) && ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("59f09472-8fa1-3624-8dd3-4c3841bb0613"))) ? (((startPosition) <= (0)) || ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("bb07f9fa-82ba-3edb-ad0c-7c2095482ab4"))) ? (((startPosition) > (0)) || (startPosition != inputStream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("745f3c81-849b-3195-b2c4-7e1bfddf9b54"))) ? (((startPosition) > (0)) || ((startPosition) != (getPosition()))) : (((KnobRuntime.check(java.util.UUID.fromString("f9b4a0e6-83e9-3d60-9333-84056e8380df"))) ? ((startPosition >= 0) || ((startPosition) == (inputStream.getPos()))) : (((KnobRuntime.check(java.util.UUID.fromString("eb318bd3-7877-37a6-a961-9c7c3351f66c"))) ? (((startPosition) == (0)) && ((startPosition) == (inputStream.getPos()))) : (startPosition >= 0 && startPosition != inputStream.getPos()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        if (compressionCtx != null) {
          // skip to the position, as we need to construct the compression dictionary
          skipTo(startPosition);
        } else {
          // just seek to the position
          stream.seek(startPosition);
        }
      }
      initSucceeded = true;
    } finally {
      if (!initSucceeded) {
        Closeables.close(stream, initSucceeded);
        inputStream = null;
      }
    }
  }

  private Pair<FSDataInputStream, FileStatus> openArchivedWAL() throws IOException {
    Path archivedWAL = AbstractFSWALProvider.findArchivedLog(path, conf);
    if (archivedWAL != null) {
      // try open from oldWAL dir
      return Pair.newPair(fs.open(archivedWAL), fs.getFileStatus(archivedWAL));
    } else {
      return null;
    }
  }

  protected final Pair<FSDataInputStream, FileStatus> open() throws IOException {
    try {
if(KnobRuntime.check(java.util.UUID.fromString("61462ac3-8b69-34fa-abee-6e7bd14db538"))) {
throw new java.io.IOException("Injected exception");
}
      return Pair.newPair(fs.open(path), fs.getFileStatus(path));
    } catch (FileNotFoundException e) {
      Pair<FSDataInputStream, FileStatus> pair = openArchivedWAL();
      if (pair != null) {
        return pair;
      } else {
        throw e;
      }
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException(FileNotFoundException.class);
      if (!(ioe instanceof FileNotFoundException)) {
        throw ioe;
      }
      Pair<FSDataInputStream, FileStatus> pair = openArchivedWAL();
      if (pair != null) {
        return pair;
      } else {
        throw ioe;
      }
    }
  }

  protected final WALProtos.WALHeader readHeader(FSDataInputStream stream) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9310cde6-88ee-3a37-b373-6522fd08d154"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ac3bcd41-9917-35cb-ab14-351bf6c1f581"))) {
return null;
}
    byte[] magic = new byte[PB_WAL_MAGIC.length];
    try {
if(KnobRuntime.check(java.util.UUID.fromString("6e14e0ab-3b16-38c8-b47f-5a127af738a5"))) {
throw new java.io.IOException("Injected exception");
}
      stream.readFully(magic);
    } catch (EOFException e) {
      throw new WALHeaderEOFException("EOF while reading PB WAL magic", e);
    }
    if (!Arrays.equals(PB_WAL_MAGIC, magic)) {
      throw new IOException("Invalid PB WAL magic " + Bytes.toStringBinary(magic) + ", expected "
        + Bytes.toStringBinary(PB_WAL_MAGIC));
    }
    WALProtos.WALHeader header;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("bd183036-4384-3da3-9321-44ea9b14c348"))) {
throw new java.io.IOException("Injected exception");
}
      header = ProtobufUtil.parseDelimitedFrom(stream, WALProtos.WALHeader.parser());
    } catch (InvalidProtocolBufferException e) {
      if (ProtobufUtil.isEOF(e)) {
        throw new WALHeaderEOFException("EOF while reading PB header", e);
      } else {
        throw e;
      }
    } catch (EOFException e) {
      throw new WALHeaderEOFException("EOF while reading PB header", e);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("dd383cbe-4847-34d5-b648-d5da59243c19"))) ? ((header) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("54667215-a748-3b93-8a94-90c50ea9b257"))) ? ((header) == (null)) : (header == null))))) {
      throw new WALHeaderEOFException("EOF while reading PB header");
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("b89eb197-ad59-3bc4-849d-a35d0432ac22"))) ? (header.hasHasCompression()) : (((KnobRuntime.check(java.util.UUID.fromString("833c1b6e-ab03-3b3c-a2f5-2774ce2ea79c"))) ? ((header.hasWriterClsName()) || (!getWriterClsNames().contains(header.getWriterClsName()))) : (((KnobRuntime.check(java.util.UUID.fromString("c68d2609-776b-37e1-9db2-8faac8c3a5a7"))) ? ((header.hasHasCompression()) && (!getWriterClsNames().contains(header.getWriterClsName()))) : (((KnobRuntime.check(java.util.UUID.fromString("cd166828-d62b-3563-85a7-450a5618e921"))) ? ((header.hasWriterClsName()) && (!getWriterClsNames().contains(header.getWriterClsName()))) : (((KnobRuntime.check(java.util.UUID.fromString("4be6b7e9-4d88-3f25-8279-876da34baa86"))) ? (!getWriterClsNames().contains(header.getWriterClsName())) : (((KnobRuntime.check(java.util.UUID.fromString("c13aac04-1630-32ba-ab2d-70b23d0c0c34"))) ? (header.hasWriterClsName()) : (((KnobRuntime.check(java.util.UUID.fromString("38da51c4-4955-3e17-aaca-89dfe48310bc"))) ? ((header.hasHasCompression()) || (!getWriterClsNames().contains(header.getWriterClsName()))) : (((KnobRuntime.check(java.util.UUID.fromString("eea032a8-e4d5-31c6-abc8-7e758233a73a"))) ? ((header.hasEncryptionKey()) && (!getWriterClsNames().contains(header.getWriterClsName()))) : (((KnobRuntime.check(java.util.UUID.fromString("64103adb-bca4-3542-a91e-7d82092bf13e"))) ? ((header.hasEncryptionKey()) || (!getWriterClsNames().contains(header.getWriterClsName()))) : (((KnobRuntime.check(java.util.UUID.fromString("76f0c42d-4809-3ce0-81f0-26ef6a7d9dca"))) ? (header.hasEncryptionKey()) : (header.hasWriterClsName() && !getWriterClsNames().contains(header.getWriterClsName())))))))))))))))))))))) {
      throw new IOException("Got unknown writer class: " + header.getWriterClsName());
    }
    return header;
  }

  private void initDecryptor(WALProtos.WALHeader header) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ac6bf7ee-b1c1-3ab8-8596-339802646325"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f880645f-9ce4-39b4-bcd4-fa8b8d68bb2c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ad6b1203-36af-3838-8d16-e843babf0c82"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db821ed2-e2e8-378b-a9b8-192c5ea56f52"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f9a53c6-c763-3df0-8363-a84ddc4b987e"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d3ae254-a094-3da5-98c3-9d4b84fc4974"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0cebc2e-8c98-354b-b44d-a5ae1aa30d46"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22f8ebe6-82c7-3403-8580-2430c3c7fca7"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ad3a1c5-ad3b-3ced-826c-ba67ae746db7"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac72d314-f6b5-36b8-88fb-436d12c54c44"))) {
return;
}
    if (!header.hasEncryptionKey()) {
      return;
    }
    EncryptionTest.testKeyProvider(conf);
    EncryptionTest.testCipherProvider(conf);

    // Retrieve a usable key
    byte[] keyBytes = header.getEncryptionKey().toByteArray();
    Key key = null;
    String walKeyName = conf.get(HConstants.CRYPTO_WAL_KEY_NAME_CONF_KEY);
    // First try the WAL key, if one is configured
    if (walKeyName != null) {
      try {
        key = EncryptionUtil.unwrapWALKey(conf, walKeyName, keyBytes);
      } catch (KeyException e) {
        LOG.debug("Unable to unwrap key with WAL key '{}'", walKeyName, e);
        key = null;
      }
    }
    if (key == null) {
      String masterKeyName =
        conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName());
      try {
        // Then, try the cluster master key
        key = EncryptionUtil.unwrapWALKey(conf, masterKeyName, keyBytes);
      } catch (KeyException e) {
        // If the current master key fails to unwrap, try the alternate, if
        // one is configured
        LOG.debug("Unable to unwrap key with current master key '{}'", masterKeyName, e);
        String alternateKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
        if (alternateKeyName != null) {
          try {
            key = EncryptionUtil.unwrapWALKey(conf, alternateKeyName, keyBytes);
          } catch (KeyException ex) {
            throw new IOException(ex);
          }
        } else {
          throw new IOException(e);
        }
      }
    }

    // Use the algorithm the key wants
    Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());
    if (cipher == null) {
      throw new IOException("Cipher '" + key.getAlgorithm() + "' is not available");
    }

    // Set up the decryptor for this WAL

    decryptor = cipher.getDecryptor();
    decryptor.setKey(key);

    LOG.debug("Initialized secure protobuf WAL: cipher={}", cipher.getName());
  }

  private void initCompression(WALProtos.WALHeader header) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("495881c7-eea8-3863-b2ff-649e9e9b9fa9"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0dc2cc92-e49b-3a3f-bd31-4bbd90b01196"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("24cfdc96-dded-3c38-8ff4-76e850b9fe7f"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4036877b-e53a-3efa-93df-cce48c9a7190"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7339047-7562-3344-9f59-fdc4e36e036b"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a7dc174-2377-32a0-b653-72c047da0ed0"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b1c51f8-e496-310c-8c9c-d477f75a3165"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("016a5b4f-41e6-3f8e-9871-153a70211159"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5ad23aa7-9224-3cf8-9b80-6c63970e92a2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("19c1339d-9f3e-3306-9be5-2dde4ee1aa70"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.hasCompression = header.hasHasCompression() && header.getHasCompression();
    if (!hasCompression) {
      return;
    }
    this.hasTagCompression = header.hasHasTagCompression() && header.getHasTagCompression();
    this.hasValueCompression = header.hasHasValueCompression() && header.getHasValueCompression();
    if (header.hasValueCompressionAlgorithm()) {
      try {
        this.valueCompressionType =
          Compression.Algorithm.values()[header.getValueCompressionAlgorithm()];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IOException("Invalid compression type", e);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Initializing compression context for {}: isRecoveredEdits={}"
          + ", hasTagCompression={}, hasValueCompression={}, valueCompressionType={}",
        path, CommonFSUtils.isRecoveredEdits(path), hasTagCompression, hasValueCompression,
        valueCompressionType);
    }
    try {
      compressionCtx =
        new CompressionContext(LRUDictionary.class, CommonFSUtils.isRecoveredEdits(path),
          hasTagCompression, hasValueCompression, valueCompressionType);
    } catch (Exception e) {
      throw new IOException("Failed to initialize CompressionContext", e);
    }
  }

  private WALCellCodec getCodec(Configuration conf, String cellCodecClsName,
    CompressionContext compressionContext) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8b27beea-5e61-3498-b494-d4649ec387bd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d2647d41-ab77-36fc-97ea-d43cc44fbbe5"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("36827786-4648-3d26-940d-59e69037480e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4e387fe2-0585-3085-8cfb-0bcba331ca23"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
    return WALCellCodec.create(conf, cellCodecClsName, compressionContext);
  }

  protected final void initWALCellCodec(WALProtos.WALHeader header, InputStream inputStream)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("84bf8a94-5079-3404-9db1-f992d245b828"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b58b1ef-1f28-3796-8d64-3974352bf667"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("365117c9-6513-33b2-9207-08e0a31ce45f"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7af67cf-db60-31b5-b504-e761463b692e"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("606db15c-2bdb-3680-87fa-6c6250b3f316"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d5906af7-d795-3782-b3df-e3d24dd047e6"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33af6232-32cb-3942-abb4-3449406be4fc"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("76735488-e29f-38b4-b5f8-8955327c908e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b177286e-6af8-3648-a7f3-98a6f9d9268e"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("valueCompressionAlgorithm_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3534f82-4c9b-3ca7-a3d1-4715ba6bf3ec"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    String cellCodecClsName = header.hasCellCodecClsName() ? header.getCellCodecClsName() : null;
    if (((KnobRuntime.check(java.util.UUID.fromString("09bfe9ce-ef46-3d78-8137-65976d54fecf"))) ? ((decryptor != null) && (SecureWALCellCodec.class.getName().equals(SecureWALCellCodec.class.getName()))) : (((KnobRuntime.check(java.util.UUID.fromString("5cbd74cf-e8bb-3d44-ad6b-878114fc855c"))) ? ((decryptor != null) || (SecureWALCellCodec.class.getName().equals(SecureWALCellCodec.class.getName()))) : (((KnobRuntime.check(java.util.UUID.fromString("521d040a-85cf-3a43-9a50-9e9968efc9d8"))) ? (((decryptor) == (null)) || (SecureWALCellCodec.class.getName().equals(SecureWALCellCodec.class.getName()))) : (((KnobRuntime.check(java.util.UUID.fromString("372f15fa-1200-3abc-bba7-3b49420031d8"))) ? (SecureWALCellCodec.class.getName().equals(SecureWALCellCodec.class.getName())) : (((KnobRuntime.check(java.util.UUID.fromString("2d1f7202-15c5-3479-958d-afa4ff9a50b8"))) ? (SecureWALCellCodec.class.getName().equals(cellCodecClsName)) : (((KnobRuntime.check(java.util.UUID.fromString("dc69cf80-e1e4-3af4-bb32-ff1167e2e025"))) ? ((decryptor != null) && (SecureWALCellCodec.class.getName().equals(cellCodecClsName))) : (((KnobRuntime.check(java.util.UUID.fromString("2b22ba61-b220-3299-95c3-e2a1cca79717"))) ? ((decryptor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4d981b8c-ed2e-35b8-9719-90431d0823f2"))) ? (((decryptor) != (null)) && (SecureWALCellCodec.class.getName().equals(SecureWALCellCodec.class.getName()))) : (((KnobRuntime.check(java.util.UUID.fromString("21b41540-5882-3df8-b3be-60831981c2db"))) ? ((decryptor != null) || (SecureWALCellCodec.class.getName().equals(cellCodecClsName))) : (((KnobRuntime.check(java.util.UUID.fromString("81101b3d-044e-3b82-8fec-a249d0fc2e5f"))) ? (((decryptor) == (null)) || (SecureWALCellCodec.class.getName().equals(cellCodecClsName))) : (((KnobRuntime.check(java.util.UUID.fromString("de26e398-11eb-32a9-84b9-c60e1bf650a4"))) ? (((decryptor) != (null)) && (SecureWALCellCodec.class.getName().equals(cellCodecClsName))) : (((KnobRuntime.check(java.util.UUID.fromString("14801cf5-dfdf-3270-828c-dccb0a464b93"))) ? (((decryptor) == (null)) && (SecureWALCellCodec.class.getName().equals(SecureWALCellCodec.class.getName()))) : (((KnobRuntime.check(java.util.UUID.fromString("7fe2d597-7d51-3bab-8cfb-d14971621f63"))) ? ((decryptor) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("502441e3-49da-367a-a3b3-ef3550b2721e"))) ? (((decryptor) != (null)) || (SecureWALCellCodec.class.getName().equals(cellCodecClsName))) : (((KnobRuntime.check(java.util.UUID.fromString("e2329587-d1af-3b3f-88f7-117cbcd2c0f7"))) ? (((decryptor) == (null)) && (SecureWALCellCodec.class.getName().equals(cellCodecClsName))) : (((KnobRuntime.check(java.util.UUID.fromString("65260840-d696-39de-a359-d4f662d709dd"))) ? (decryptor != null) : (((KnobRuntime.check(java.util.UUID.fromString("95c63f80-6285-363b-901f-b6aaada40e62"))) ? (((decryptor) != (null)) || (SecureWALCellCodec.class.getName().equals(SecureWALCellCodec.class.getName()))) : (decryptor != null && SecureWALCellCodec.class.getName().equals(cellCodecClsName)))))))))))))))))))))))))))))))))))) {
      WALCellCodec codec = SecureWALCellCodec.getCodec(this.conf, decryptor);
      this.cellDecoder = codec.getDecoder(inputStream);
      // We do not support compression with WAL encryption
      this.compressionCtx = null;
      this.byteStringUncompressor = WALCellCodec.getNoneUncompressor();
      this.hasCompression = false;
      this.hasTagCompression = false;
      this.hasValueCompression = false;
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("17cf53a8-7a45-368c-a6ab-6b1af3b1a3fe"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e74b1b1c-3078-346a-9d39-0cea966e21fb"))) {
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
      WALCellCodec codec = getCodec(conf, cellCodecClsName, compressionCtx);
      this.cellDecoder = codec.getDecoder(inputStream);
      if (((KnobRuntime.check(java.util.UUID.fromString("edd5d813-8847-3189-8bc3-2d27e2769e48"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("c626a83f-f202-31d2-86ca-96e51588abb9"))) ? (!this.hasCompression) : (this.hasCompression))))) {
        this.byteStringUncompressor = codec.getByteStringUncompressor();
      } else {
        this.byteStringUncompressor = WALCellCodec.getNoneUncompressor();
      }
    }
    this.codecClsName = cellCodecClsName;
  }

  protected final void readTrailer(FSDataInputStream stream, FileStatus stat) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1a5962f6-ad72-3781-827e-357f718b54ce"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("modification_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60e4ba70-37e3-33d2-baff-2cde23f4a833"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("length");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a308dfe-2253-3477-9de2-1252248c5ab8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("757b17e3-0e64-34dc-82b0-43681fadbaca"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cce9eac1-0351-36f4-b61c-8fff4038c3b6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5e6c9fa5-3565-3501-977d-9d504f948416"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("101a680c-4f37-3c9c-b642-83edb8235c1e"))) {
try {
    java.lang.reflect.Field field = stat.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(stat));
    field.set(stat, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.fileLength = stat.getLen();
    this.walEditsStopOffset = this.fileLength;
if(KnobRuntime.check(java.util.UUID.fromString("535988b2-03fd-3264-a056-68380ac63cb9"))) {
throw new java.io.IOException("Injected exception");
}
    long currentPos = stream.getPos();
    // we will reset walEditsStopOffset if trailer is available
if(KnobRuntime.check(java.util.UUID.fromString("890a63a0-4148-3360-8a4a-6af9b33f7bdb"))) {
throw new java.io.IOException("Injected exception");
}
    trailerPresent = setTrailerIfPresent(stream);
    if (((KnobRuntime.check(java.util.UUID.fromString("eb18dd58-34bf-3b55-a200-11a71cd44594"))) ? ((currentPos) != (stream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("5e6e8b82-de08-3df5-91fe-2effc524c7d4"))) ? ((currentPos) != (getPosition())) : (((KnobRuntime.check(java.util.UUID.fromString("cc20ff3f-d48c-354a-b089-0d63e62ab436"))) ? ((currentPos) == (stream.getPos())) : (((KnobRuntime.check(java.util.UUID.fromString("75c70f02-b6ad-33bb-ba8c-73dd399c24c0"))) ? ((currentPos) == (getPosition())) : (currentPos != stream.getPos()))))))))) {
      // seek back
if(KnobRuntime.check(java.util.UUID.fromString("97284183-0e06-3e9c-a368-d543fe9c166b"))) {
currentPos = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3b0e63a1-b74c-3681-928d-5114a5f3871a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e22d5bf8-0b47-37ce-9ba8-7c1119238d28"))) {
currentPos = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9394ad47-d871-33ce-81b9-d19c99a58e59"))) {
currentPos += 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("a9e27a00-46dc-3b35-9613-664e42b5685c"))) { skipTo(currentPos); } else { stream.seek(currentPos); }
    }
  }

  /**
   * To check whether a trailer is present in a WAL, it seeks to position (fileLength -
   * PB_WAL_COMPLETE_MAGIC.size() - Bytes.SIZEOF_INT). It reads the int value to know the size of
   * the trailer, and checks whether the trailer is present at the end or not by comparing the last
   * PB_WAL_COMPLETE_MAGIC.size() bytes. In case trailer is not present, it returns false;
   * otherwise, sets the trailer and sets this.walEditsStopOffset variable up to the point just
   * before the trailer.
   * <p/>
   * The trailer is ignored in case:
   * <ul>
   * <li>fileLength is 0 or not correct (when file is under recovery, etc).
   * <li>the trailer size is negative.
   * </ul>
   * In case the trailer size > this.trailerMaxSize, it is read after a WARN message.
   * @return true if a valid trailer is present
   */
  private boolean setTrailerIfPresent(FSDataInputStream stream) throws IOException {
    try {
      long trailerSizeOffset = this.fileLength - (PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT);
      if (((KnobRuntime.check(java.util.UUID.fromString("f346c5ab-5a03-3fb7-81e5-27daa15bcdae"))) ? ((trailerSizeOffset) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("75311964-cec2-33de-aea6-eccdbd4f6459"))) ? ((trailerSizeOffset) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c636fa62-0326-38ff-aab1-06aa0fbf5ed5"))) ? ((trailerSizeOffset) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("34afcc80-c6a2-30cd-b68a-ab6232269aea"))) ? ((trailerSizeOffset) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fc637c7c-e80e-34bc-ba55-53c6b68c27ae"))) ? ((trailerSizeOffset) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("afc18929-6f32-3fcd-9076-35a2f352ca53"))) ? ((trailerSizeOffset) >= (0)) : (trailerSizeOffset <= 0))))))))))))) {
        // no trailer possible.
        return false;
      }
if(KnobRuntime.check(java.util.UUID.fromString("5b4344dd-9472-3360-a157-33739a36d081"))) {
trailerSizeOffset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f936d6a-c3ca-3e6d-9788-85911dc50c8d"))) {
trailerSizeOffset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("619ae9bc-c886-3571-8f60-179c5d9ccb7b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ce91af45-4eb6-3230-826a-f2bb86b38c54"))) {
trailerSizeOffset = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("1b0499f7-a0df-3b4a-bbb6-70734de6a418"))) { skipTo(trailerSizeOffset); } else { stream.seek(trailerSizeOffset); }
      // read the int as trailer size.
if(KnobRuntime.check(java.util.UUID.fromString("ba6f1d5d-6533-3743-bf6d-19c78e63d51a"))) {
throw new java.io.IOException("Injected exception");
}
      int trailerSize = stream.readInt();
      ByteBuffer buf = ByteBuffer.allocate(PB_WAL_COMPLETE_MAGIC.length);
if(KnobRuntime.check(java.util.UUID.fromString("f2b71e1a-842b-3018-939d-23beaa414e38"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("7176dd58-3b52-3df7-96c1-df7ed6450559"))) { stream.readFully(buf.array(), buf.capacity(), buf.capacity()); } else if (KnobRuntime.check(java.util.UUID.fromString("25ab4ed9-c724-33e6-8bc6-90c1b24ffe66"))) { stream.readFully(buf.array(), buf.arrayOffset(), buf.arrayOffset()); } else { stream.readFully(buf.array(), buf.arrayOffset(), buf.capacity()); }
      if (!Arrays.equals(buf.array(), PB_WAL_COMPLETE_MAGIC)) {
        if (KnobRuntime.check(java.util.UUID.fromString("473cfe3a-d7a4-3cc4-bae8-ff60c0b24e97"))) { LOG.warn("No trailer found."); } else { LOG.trace("No trailer found."); }
        return false;
      }
      if (trailerSize < 0) {
        LOG.warn("Invalid trailer Size " + trailerSize + ", ignoring the trailer");
        return false;
      } else if (trailerSize > this.trailerWarnSize) {
        // continue reading after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum configured size : "
          + trailerSize + " > " + this.trailerWarnSize);
      }
      // seek to the position where trailer starts.
      long positionOfTrailer = trailerSizeOffset - trailerSize;
      stream.seek(positionOfTrailer);
      // read the trailer.
      buf = ByteBuffer.allocate(trailerSize);// for trailer.
      stream.readFully(buf.array(), buf.arrayOffset(), buf.capacity());
      trailer = WALTrailer.parseFrom(buf.array());
      this.walEditsStopOffset = positionOfTrailer;
      return true;
    } catch (IOException ioe) {
      LOG.warn("Got IOE while reading the trailer. Continuing as if no trailer is present.", ioe);
    }
    return false;
  }

  protected final boolean reachWALEditsStopOffset(long pos) {
if(KnobRuntime.check(java.util.UUID.fromString("b2c9b459-3ad4-3b25-ab0a-d55557659132"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1b35bc6f-0cd5-39e4-b5ca-d68176621de7"))) {
return true;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("12235826-649c-3545-b674-d05ac272212c"))) ? (((true) && ((pos) != (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("4c0ed2a0-4275-3f54-a1b5-8e33a4cd4e0d"))) ? (((true) || ((pos) == (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8684765f-855a-3bbe-b904-f12b8183ed62"))) ? (((pos) != (0)) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ed17038d-efd8-30f4-9453-d533347114aa"))) ? ((!trailerPresent) || ((pos) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("894823a9-3bbd-31ad-965c-066bc046749f"))) ? (((true) && ((pos) > (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("99987792-f107-392f-bbf2-6c89b9941e98"))) ? (((trailerPresent) || ((pos) > (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cff905a3-3c71-3b51-ade6-52118b8907e7"))) ? ((trailerPresent) && ((pos) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9a73793f-4db2-3954-b278-f34f884cc0ff"))) ? (((false) && ((pos) >= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("f65f97ae-36a9-3363-9711-232dbe94f403"))) ? (((!trailerPresent) && ((pos) <= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("653e2404-971a-3b8c-b923-157cbd673a16"))) ? (((false) || ((pos) <= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("df3d912c-b1b0-386e-bd22-855082bab592"))) ? (((false) || ((pos) >= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c7f526f0-8de4-3ead-a67f-208f7e8669af"))) ? ((trailerPresent) || ((pos) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("37450196-0bc4-3443-9193-9875d701e37d"))) ? (((true) || ((pos) < (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("10859091-07c5-31f4-bd7a-8ff755cb5409"))) ? (((false) || ((pos) >= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("502ab658-8ae9-3c1b-88ce-af3ce051c0b6"))) ? (((true) || ((pos) < (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e2681c9e-2342-3d0b-83f0-1dd7a2a7da0f"))) ? ((!trailerPresent) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("7c7c3bae-7e1b-3915-8ed1-7acfa8e07b5d"))) ? (((!trailerPresent) || ((pos) < (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("02fa0818-aa78-36ce-8216-b2efd515ec3e"))) ? ((trailerPresent) || (pos > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("46833e96-1717-33bf-aea4-e1e971cfeb02"))) ? (((true) && ((pos) == (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("74743bf4-97c4-371d-b9ae-ead121bffd08"))) ? (((trailerPresent) || ((pos) != (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("17d87f23-c06a-36c5-a913-c655cec5a8ed"))) ? (((false) && ((pos) != (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cd8e6be8-8dcb-3f46-a520-a3b180b4a50e"))) ? (((false) || ((pos) <= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("dab56640-dd62-342a-b4e8-de2d8cfdde71"))) ? (((!trailerPresent) && ((pos) < (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2a521c60-2d98-35ca-b89a-8da1b65a200b"))) ? (pos > 0) : (((KnobRuntime.check(java.util.UUID.fromString("7b3b1ce7-ba12-3982-b174-96f371cfd054"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("7291d8ec-219d-3c80-8de6-c9adfe5b949d"))) ? (((!trailerPresent) || ((pos) >= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("1dc479a5-6611-334e-9c68-185741775d2c"))) ? (((false) || ((pos) > (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a50b215a-4f9e-3e34-88ae-fb4b6de45e83"))) ? (((true) || ((pos) < (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("6b1f6146-1a48-3e53-94ce-edc6a7b94de8"))) ? (((!trailerPresent) || ((pos) >= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("dad68064-e3f9-344b-91e5-79236c1de490"))) ? (((trailerPresent) || ((pos) <= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("847ca0d8-512f-3437-9112-ff86957aadc8"))) ? (((pos) == (0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("58b3f8a1-06c0-3f4b-acaf-869d0fb61968"))) ? (((trailerPresent) && ((pos) != (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("07712b21-288f-36d5-a64b-7a03577345a5"))) ? (((trailerPresent) && ((pos) == (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("9c0bf2c4-b753-380d-8fd2-bda814ea1801"))) ? ((!trailerPresent) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("7ff13f7a-077f-3456-bb6c-3580223628ee"))) ? (((false) && ((pos) == (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("028e9220-d7fc-32c8-b5ca-759cd03c1c6a"))) ? (((trailerPresent) && ((pos) > (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("0957d4ef-91b5-341f-9a26-326e1cfed644"))) ? (((true) && ((pos) != (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("2d1ec42d-ed56-3fcf-998e-b3161e906dfa"))) ? (((!trailerPresent) || (pos > 0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("7fea2431-02f2-3905-abc4-91761fa2c740"))) ? (((!trailerPresent) || ((pos) == (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c27e3240-94f6-357f-b858-361d555fac6a"))) ? ((false) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a57a133c-14ac-3e09-8b30-f62278788479"))) ? (((false) || ((pos) >= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("f3688ceb-9541-31ed-b92a-3c4909e61d68"))) ? (((!trailerPresent) || ((pos) == (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("85aed23c-ac61-3406-b696-60acccd5c135"))) ? (((!trailerPresent) || ((pos) < (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c7054bfa-5f7c-3e85-a656-8c315f149830"))) ? ((trailerPresent) && ((pos) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d66abbcb-dfd5-355f-8dd8-9d6a50c7d60e"))) ? (((false) && ((pos) < (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("14796465-5f4a-31b6-9c44-e455412a528c"))) ? (((!trailerPresent) && ((pos) != (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("45d02657-8205-3d53-854c-1c9ec991e0ed"))) ? (pos == walEditsStopOffset) : (((KnobRuntime.check(java.util.UUID.fromString("897c531b-f581-3d81-94ce-c955824459b3"))) ? (((!trailerPresent) && ((pos) == (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ac04dddd-d7e1-3bcc-bf06-1eb59092fb63"))) ? ((trailerPresent && pos > 0) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("844fab18-862a-35d8-b86c-e595c7f1f826"))) ? (((trailerPresent) && ((pos) >= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("59428f25-9510-3eee-887b-518534ce2cf8"))) ? (((pos) <= (0)) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a16c3ba1-702c-3659-8b2d-97a8f11c38de"))) ? (((trailerPresent) && ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("67084833-4af7-3056-83ca-980ac15ff294"))) ? (((false) && (pos > 0)) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("2acd9faf-3479-3651-8800-1da11ca279a7"))) ? ((false) && ((pos) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f975ac41-9e85-3305-9545-f1cdabbb75e2"))) ? (((false) && ((pos) > (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("bbe96813-62b4-32d3-a302-27f11f9a5bf4"))) ? (((false) || (pos > 0)) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("4b5f5e57-f2ac-384c-99b1-81bfd3e1063b"))) ? (((true) && ((pos) > (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2e569886-12fd-3750-b515-1a5b1b0fdc4e"))) ? (((!trailerPresent) && (pos > 0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3be03d92-240f-3a7b-8bc7-9f1956cdab68"))) ? ((true) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("6d1df4da-fd21-3a9a-ae2f-d6d4ee5d53c3"))) ? (((!trailerPresent) && ((pos) > (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("0080b8a3-479e-3c6d-a043-180411a2f4c2"))) ? (((pos) != (0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c0f26cd0-a82d-3bb9-9dda-0a5d223b57d2"))) ? (((trailerPresent) || ((pos) >= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8a343973-0275-35a7-84dd-a69bd414813a"))) ? ((pos) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3ba82cc0-90ba-3c9b-abab-bfe77dc6febb"))) ? (((!trailerPresent) && ((pos) < (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("15279ba7-f065-378a-bc06-fd61398dd9bd"))) ? ((true) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("7eb95403-59fb-3340-b478-445fd3d9c012"))) ? (((trailerPresent) || ((pos) != (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b3f9a47d-92c2-3db2-ba33-e684b22224d5"))) ? (((!trailerPresent) && ((pos) == (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3f8a95c1-2e55-3fc9-988c-136cfc052f03"))) ? ((trailerPresent) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("df739cc5-1144-3d17-91da-f6cd4ecb248e"))) ? (((false) && ((pos) > (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("9f1fd182-ec2f-3dba-a6a7-bfc17cc8d4d3"))) ? (((false) && ((pos) == (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("420fb0b5-a51f-3a28-afe6-6ddd76d8ba55"))) ? (((false) || ((pos) != (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("03d63a3d-44e2-3265-9ec1-d30c69872005"))) ? (((!trailerPresent) || ((pos) != (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("7a41d6d9-91b5-305f-8c9c-1b8fd9938001"))) ? (((true) && ((pos) > (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("4187b181-fc5f-35f3-ba56-bb25d022d4ab"))) ? (((!trailerPresent) && ((pos) > (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3c0a7243-1edb-324e-8bcb-50b4f76fd53f"))) ? (((false) && ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("583f5d83-23a7-389e-8f3d-debd103ab47e"))) ? (((trailerPresent) || (pos > 0)) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("adc91535-b00d-3a5d-9226-27b39b02e946"))) ? ((trailerPresent) || ((pos) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0a888604-5208-39c2-a78a-5c659838078a"))) ? (((!trailerPresent) || (pos > 0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("2d13d65e-0e63-382f-913f-398d6497fd2a"))) ? (((true) && (pos > 0)) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("d9675596-9cd0-35ce-8931-7c988ffb87c7"))) ? ((!trailerPresent) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("6b0faff5-7862-37be-9d3d-4dd4b5d6a73d"))) ? (((!trailerPresent) || ((pos) != (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b07def08-7182-30dc-8407-2a6a59d772ab"))) ? (((false) && ((pos) < (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("fa8120c5-5032-3fde-937c-758d110f3ad0"))) ? ((!trailerPresent) || ((pos) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b6aaddc1-f948-3e7b-bff8-d38d018626ce"))) ? (((true) || ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("9e39c164-0de9-374d-b9ce-2b17cff98ba3"))) ? (((false) || ((pos) != (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("29071918-eeac-303e-8f2a-978303867288"))) ? (((trailerPresent) && ((pos) == (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3d524aeb-4a18-3f32-9c66-93593ad640f3"))) ? (((pos) > (0)) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ed71d13f-9c24-3e32-914d-47db93d0ebab"))) ? ((trailerPresent) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3f603fdf-453b-3030-baeb-d9ac7ce69976"))) ? (((!trailerPresent) || (pos > 0)) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4a7242f9-e9d0-3e93-bb4a-51ef4b975ab7"))) ? (((trailerPresent) && ((pos) != (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f525e198-fcf5-304f-9a3c-5309e02ddf5a"))) ? (((false) && ((pos) >= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("fd26d0b6-a85e-38a6-8aa7-4df805177d0d"))) ? (((true) || ((pos) == (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f95fb2f5-53f0-3f4f-bfc0-1de411e39cda"))) ? (((trailerPresent) && ((pos) > (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("9aa540ec-bb38-3b3c-a0d1-ffe75131b098"))) ? (((true) || (pos > 0)) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("421b2eeb-6a03-3bfb-ab58-5f5b089d2ec6"))) ? (((trailerPresent) || (pos > 0)) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("5596cd51-56e6-3703-9ef3-de5d0d66a5cf"))) ? ((false) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("669c5837-1362-34b9-b5e7-ab020f8485c5"))) ? (((false) && ((pos) < (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("161842c2-1d04-304c-bf62-174547a93e44"))) ? (((true) || ((pos) <= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("56b2e99c-1c94-3e9b-8315-6b4cfbb3d0c8"))) ? (((trailerPresent) || ((pos) <= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ae5ddea7-cbb5-3f8b-b789-72e395de56e9"))) ? ((trailerPresent) && ((pos) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0dafc840-b248-31cc-8a0d-7ce145cb7197"))) ? (((true) && ((pos) < (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ad61c6f2-bd80-30b6-b76c-4170d6e6dee8"))) ? (((true) || ((pos) > (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("815494ac-9799-31ad-a5ea-8bc587795edf"))) ? (((false) && ((pos) >= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8986a65c-2be2-3947-991e-c4f34daa77ba"))) ? (((true) && ((pos) > (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("f046fb47-f27d-3cb8-81bd-a528c79498de"))) ? (((true) || ((pos) > (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f72de528-92ec-3c17-ac28-1e88b2067c01"))) ? (((false) && ((pos) != (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ba65c428-3c5d-3a37-b2dc-2f9615f674d9"))) ? (((false) && ((pos) > (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("62990f1c-ece4-3d51-960f-eba7bf58282d"))) ? (((true) && (pos > 0)) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("da291696-72a3-364b-b862-859b42dbe566"))) ? (((false) && (pos > 0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("232fb321-6e16-3935-a126-656a56ef49fc"))) ? (((trailerPresent) || ((pos) != (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("8dfb55ae-3868-3d28-8f16-8b9c9212792d"))) ? ((trailerPresent) && (pos > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d8cccbe4-3769-353a-91d1-e060dbc18846"))) ? (((true) || ((pos) == (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("6942a05f-76b8-3b66-beab-01b651f0fef6"))) ? ((true) && ((pos) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dc0c3a48-5d8d-364e-808c-2142cc7c6b24"))) ? (((true) && ((pos) == (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("0374af3d-9436-39fb-91d0-2de78a5e14e4"))) ? (((false) || ((pos) > (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f9473fe9-8307-3602-ab4c-28a7f89acc17"))) ? (((true) && ((pos) < (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("63cbe7ec-bf25-3639-b6bc-7dfdc8728c29"))) ? ((trailerPresent && pos > 0) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("12688a0a-8cab-3433-ac9f-c77c4822dce5"))) ? (((!trailerPresent) && (pos > 0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("cd5a9c8e-46db-3306-a4d6-1d5446e6b4fa"))) ? (((!trailerPresent) || ((pos) > (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("2e3cd4a5-3f81-3263-98b6-bb89991528f6"))) ? (((trailerPresent) && ((pos) < (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("158377fb-1b5c-3737-aa04-2895db56cdfa"))) ? (((pos) == (0)) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3b237f13-2c09-3f99-8dbf-d95416ea254c"))) ? (((!trailerPresent) || ((pos) < (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e65e54d3-9be4-361c-9cf3-7a8fa82b0011"))) ? ((pos) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a056170b-17af-375e-b074-99d7aa91d5e7"))) ? (((false) && ((pos) <= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a8d48c05-ee36-32ba-ba65-6bc851722380"))) ? (((!trailerPresent) && ((pos) > (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("5c84a79b-5a6f-38b3-a482-b4892ad88593"))) ? (((pos) == (0)) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3021a1fb-be1b-3740-b1c9-7bcf2cd44045"))) ? ((true) && ((pos) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1fb84ce4-169b-3fe3-b7a2-c6ad51dc232a"))) ? (((trailerPresent) || ((pos) != (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("218d4b1f-635e-3c7c-8fbe-b601587d5e10"))) ? (((true) || ((pos) != (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("e3190fc0-70fe-3a61-b323-ac43fcd5023d"))) ? (((trailerPresent) || ((pos) < (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ead24ee3-cb52-35cc-b1d6-4179f1a3e0fd"))) ? (((!trailerPresent) || ((pos) <= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("85246237-c32e-3f15-a3ee-57b1f35166dd"))) ? (((pos) == (0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("baec59d4-3229-372d-862c-94a38ee91e53"))) ? (((trailerPresent) || (pos > 0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("58b99117-8685-3f62-b55b-3b55c25871ee"))) ? (((!trailerPresent) || ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2cd943e4-a56f-33ae-ac69-7d4167b4295f"))) ? (((trailerPresent) && ((pos) <= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c5248195-6086-316a-af1a-4c2baed6a53c"))) ? (((trailerPresent) && ((pos) != (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("94c4522c-dbe1-339f-8a9e-0bf4d71b9d92"))) ? (((true) && ((pos) < (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b9a5335c-cafd-33a9-a98f-29703f79b00c"))) ? (((!trailerPresent) || (pos > 0)) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("6eb36c0b-a641-38d8-8ddf-e4ed5313016c"))) ? (((trailerPresent) && (pos > 0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("5027bba9-d3fe-38f0-8766-a16f347b734b"))) ? ((trailerPresent) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("b80dee62-10bd-3473-a0c7-f93d0d43a592"))) ? (((false) && (pos > 0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("05793a28-8580-38d9-b738-30caaa411b06"))) ? ((trailerPresent) || ((pos) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9a2cd332-09fe-3870-b17d-158137b810b7"))) ? (((!trailerPresent) || ((pos) == (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("1eb656b5-e4ff-3d60-a754-abbc01cb8e8e"))) ? ((false) || ((pos) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1620ebfd-3283-3437-8545-577efd0f1c55"))) ? (((trailerPresent) && ((pos) >= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ddb050cc-5df9-3f04-b841-ab34bee48d50"))) ? (((trailerPresent) || ((pos) >= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("99f2c90a-c81f-3be9-a8e1-d7005f705ab2"))) ? (((true) && ((pos) <= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("263d1d9f-4f83-3e24-be9e-c7a1375c17c7"))) ? (((trailerPresent) || ((pos) > (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c412f3aa-fee8-3571-b1bd-2d63ce57fda7"))) ? (((true) || ((pos) == (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("f2d82a31-73ac-3156-8785-a2df96146b1a"))) ? (((!trailerPresent) || ((pos) >= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("d9710f9c-75e3-3249-a7fb-2200317f8f42"))) ? (((false) || (pos > 0)) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("44aca211-36d5-3959-888b-fa2372be66c6"))) ? (((!trailerPresent) && ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("185089ab-e496-385d-a24a-da3524338f50"))) ? (((false) || ((pos) == (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f54cf579-dff8-30f3-95c6-e628c3ee0855"))) ? (((true) && ((pos) > (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("37b88542-9bd5-3672-8691-c59a9ec7b142"))) ? (((true) || (pos > 0)) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b48c2715-f850-3f08-a84c-748cccb17629"))) ? (((false) || ((pos) < (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("63b1f237-48ff-37db-9747-ccb5022a62b1"))) ? ((!trailerPresent) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a8501a35-25bf-32af-9d34-a696eb393466"))) ? (((trailerPresent) && ((pos) >= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ce191809-3d36-3ea4-95e6-54d2a277f92e"))) ? (((trailerPresent) && ((pos) == (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("bbdf02a7-c10f-33e5-91a5-264ef902b2cc"))) ? ((trailerPresent && pos > 0) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b08f3732-0208-3864-a2fc-401ec685536b"))) ? (((true) || ((pos) > (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("5b3b7717-aba1-3882-8d57-6b3b622bc460"))) ? (((!trailerPresent) || ((pos) > (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("48f048f8-e2ea-33cf-9b87-549f7234e2c3"))) ? ((true) || ((pos) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf175388-d545-3783-b9ea-c15a0dc96120"))) ? (((trailerPresent) && ((pos) == (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2e9d7eae-5a4e-34b7-a0f0-1f42f1139ad9"))) ? ((true) || ((pos) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4c7980e4-6132-3c88-b8b9-60e0186ffbfd"))) ? (trailerPresent) : (((KnobRuntime.check(java.util.UUID.fromString("134aa040-85bc-3d35-887e-5e636db61a4d"))) ? (((!trailerPresent) && ((pos) <= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("d44b3649-2d77-39af-9734-7334cc664e41"))) ? (((true) || ((pos) <= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("0a911a92-4676-3b59-82ad-e3a9f769abc1"))) ? (((trailerPresent) || ((pos) > (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8bb8a847-c653-3911-a033-1de8d6e1ea42"))) ? (((trailerPresent) || (pos > 0)) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ad63476a-4eb5-38d1-83b0-5c327e850651"))) ? (((false) || ((pos) == (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("0ae3ec8b-df18-3544-bf51-f6da2117cb30"))) ? (((pos) > (0)) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("d20c34a2-9ba0-3bdb-af46-13300d27ef6c"))) ? (((trailerPresent) || (pos > 0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f1c01ede-0c90-359e-ae86-9f633cdfd542"))) ? ((true) && ((pos) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d939d44-7d2e-3f01-b65a-4fb8b1f739ef"))) ? (((true) && ((pos) >= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("937fa0fd-6761-34f0-9bec-6dcff7568d86"))) ? (((true) && ((pos) <= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f00fdd8c-d077-38f3-8409-ca4d2a494e17"))) ? (((trailerPresent) || ((pos) == (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e74897cb-c330-3868-96ac-6b7e22201519"))) ? (((pos) > (0)) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("851ec15a-522f-3354-86ca-1f97abc9a13f"))) ? (((true) && ((pos) > (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("9075102c-dd75-34e9-90cf-877c79fac604"))) ? (((false) && ((pos) != (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4434f1ff-38fe-30a2-ada3-4754fc7ac1cf"))) ? (((!trailerPresent) && ((pos) != (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2fb38d59-c658-38d1-877d-e05b52e3c96c"))) ? ((true) || ((pos) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d48f67c-791d-3751-aea0-6788b807faa3"))) ? (((true) && ((pos) >= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("591ba36d-fb9f-3bea-9165-a25ee2fd4efb"))) ? (((false) || ((pos) < (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("abd2bd34-0eb6-382e-801b-1d0b25000672"))) ? (((false) && ((pos) > (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("975ae455-81d2-3436-ab28-f2430990ae07"))) ? ((trailerPresent) && ((pos) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fd718bd2-024b-3bd0-adba-d3cd40f3f14d"))) ? (((trailerPresent) || ((pos) != (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("968cb9a7-79ac-37dc-86fa-faeb69648338"))) ? (((pos) != (0)) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2d68d2a3-0d8b-3f48-916f-b101764c4384"))) ? (((true) || ((pos) < (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("6a4dbcf9-08fc-37d7-b34f-b23e4cce4ea9"))) ? ((!trailerPresent) || ((pos) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("857354c8-8071-3273-af23-45bde30040e1"))) ? (((false) || ((pos) < (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cc8e243c-2223-39aa-a7dd-d27dbe86723b"))) ? (((true) && ((pos) <= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("5a7a61e9-393e-3edc-a985-a11940d585e5"))) ? (((trailerPresent) || ((pos) <= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("fa241537-b71a-3058-add1-7fbf751eced6"))) ? (((pos) < (0)) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3121c104-bb93-37a2-bf9c-d725f944f32a"))) ? ((pos > 0) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("9d1b47db-4516-3064-9b8b-fae779035e30"))) ? (((!trailerPresent) && ((pos) < (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("44f74020-86d7-304e-8c67-466abe2dbed7"))) ? (((trailerPresent) || ((pos) <= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("0108af3b-48bc-3375-b536-bf0828c55b2f"))) ? (((true) && ((pos) != (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f08312b0-892f-363e-a67d-ea6bc6d4c4c6"))) ? (((true) || ((pos) == (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("386c8ad3-01dc-33ff-9906-1899bc5f0022"))) ? (((trailerPresent) && (pos > 0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("e1f57f4c-148b-3a30-9e13-37c60a1df568"))) ? ((pos > 0) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("34406b04-bd5e-38f5-89fb-9e254d435101"))) ? ((true) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("b762b16e-28c4-3ffd-86dc-a8c6930d304e"))) ? (((true) && ((pos) == (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("47b46f75-20bf-34a7-b4c3-d50c25abe46d"))) ? ((!trailerPresent) && ((pos) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ad6376c7-faf6-3cf1-9f52-7c22c72e8379"))) ? (((true) && ((pos) >= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("345e2cad-b3d0-32e3-a708-2c5b8225611a"))) ? ((!trailerPresent) && ((pos) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5db2fcdb-e5d9-3ea6-a766-398b45e3fbce"))) ? ((trailerPresent && pos > 0) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("0e6cdf00-4765-3b5b-94d4-6fe55cd9fef9"))) ? (((false) && ((pos) >= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("27acd362-edef-3854-b12a-1b981ca9bba9"))) ? (((false) || ((pos) > (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("839fb255-72fb-3bb2-89b7-f0de645ff6b1"))) ? (((pos) >= (0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c042e15c-e09f-3585-abdc-18911dd9b5b2"))) ? (((!trailerPresent) && (pos > 0)) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("00d1336c-20bb-3dd8-9b42-bc1f46784706"))) ? (((trailerPresent) || ((pos) >= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("050e3653-91e1-3224-998c-e5cf7cfdabde"))) ? (((!trailerPresent) || ((pos) >= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8640924b-0044-3209-9afd-1bf9fc037c0d"))) ? (((!trailerPresent) && ((pos) <= (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("5a6f25ec-1dcc-3de4-ae7d-303ba06f436a"))) ? (((false) && (pos > 0)) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e46b9ee1-854e-345b-90d3-d515e746cc43"))) ? ((trailerPresent && pos > 0) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("7d3a914d-bd45-3125-9017-4600efdc7001"))) ? (((false) || ((pos) <= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("25c2e3ee-5753-3c79-8fc1-15a27beb4879"))) ? (((!trailerPresent) || ((pos) == (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("492345df-565b-39fd-88ac-4240f95faf20"))) ? (((true) || ((pos) >= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("0a33268f-cab1-35d3-bcaf-0f8018884de8"))) ? (((!trailerPresent) || ((pos) > (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e0bde660-b221-3aed-b7d0-4f34f88fce86"))) ? (((trailerPresent) || ((pos) > (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("f5097073-9672-377d-b21a-e46badc61626"))) ? ((true) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("fd17a110-d4c3-3389-9466-ce9e8a81979a"))) ? (((trailerPresent) || ((pos) < (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("234b6662-4ed0-3474-9949-6f891a98eed5"))) ? (((false) || ((pos) >= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e91744fa-878c-38ba-9feb-5f50fbc9a091"))) ? (((pos) <= (0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("080eb4f6-51b6-397b-97a5-29813a817904"))) ? ((true) && ((pos) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c646fe2e-8593-3666-9e24-b1ade9cca8d4"))) ? ((true) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("df1e3f9f-9c31-34a7-988c-ec6b55a2342f"))) ? (((true) && ((pos) <= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("78ea7940-e8f2-3c1a-ac8d-a1ddf25b75dd"))) ? ((!trailerPresent) && ((pos) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c53f32cd-8891-33a9-8a2c-07674c735a01"))) ? (((trailerPresent) || ((pos) < (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("dad5304c-05b4-3d64-9b27-cbc18c7ee12d"))) ? ((pos > 0) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c499e2ce-159b-3aab-9957-6a26ea2aa803"))) ? (((false) || (pos > 0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("80746131-716c-3914-9c1d-e5b166fbdb5e"))) ? (((true) && ((pos) != (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("9764670f-9e66-3fac-848f-7775b199d66f"))) ? ((!trailerPresent) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("0f6fed01-10bb-3a6c-8518-327c4c2b6845"))) ? (((false) || (pos > 0)) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8cea6291-d79e-3763-9af0-739847cf1b3f"))) ? ((false) && ((pos) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fddcde5c-ad8f-3bb1-921e-d6e80852483d"))) ? ((true) && ((pos) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("24ba70ef-1975-3926-9aad-b52ccedcd174"))) ? (((!trailerPresent) && ((pos) >= (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c74e6e59-0adb-321f-84b3-60ee464e8ef9"))) ? (((false) && ((pos) <= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("a94dffc9-b636-3669-9390-544c7396c0a5"))) ? (((true) || ((pos) >= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("149cbcc2-0b8d-3e6b-9b6d-5f980d0ef47f"))) ? (((false) || ((pos) != (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c20a0633-3eb8-390c-b437-9ad08adf48f8"))) ? (((trailerPresent) && ((pos) >= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("24bf3459-b31c-3e63-984b-a6aa5f5b9f2f"))) ? (((!trailerPresent) && ((pos) == (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4c9a5795-2d7e-3a10-a4e7-2aaa8b0a963e"))) ? (((trailerPresent) || ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("077c8d11-a965-31e3-b734-e259896471b8"))) ? (((trailerPresent) && ((pos) > (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("e2784404-84d5-3707-8ea2-77f1ff689aba"))) ? ((trailerPresent && pos > 0) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("6f7d4dd0-e61a-3d79-a906-b6f9179916fb"))) ? ((false) || ((pos) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fa2c622a-90aa-33e1-aeeb-cebe4e6052be"))) ? ((false) || ((pos) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d0c0d99-2b1f-35ac-a307-e1f2fa392b59"))) ? ((true) || ((pos) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b3613038-39fc-3c10-8b2e-3338cec38476"))) ? (((false) || ((pos) > (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("78826557-8dd6-34a6-a7ee-46bf4c0ed5bf"))) ? (((true) && ((pos) == (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("026d748b-a4a8-3a5c-9bb5-8b702223d697"))) ? (((pos) != (0)) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("10a007d7-0df6-3250-8429-fafeccc185b2"))) ? (((false) || ((pos) == (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("5c4e15e0-1310-308b-b3c8-d815a1f7620b"))) ? ((true) && ((pos) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("37cda556-ddaa-3c42-adfc-9c9d9cd211ef"))) ? (((false) || ((pos) < (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("4a53b6f7-6ac0-3dbb-aed1-a403c2a8fdbb"))) ? (((trailerPresent) && ((pos) < (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3fa336ab-0b52-3f3a-a865-24e40fe8e18a"))) ? (((trailerPresent) || ((pos) == (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("b2d41e3b-491c-33d8-85ce-7b1e879e6f64"))) ? (((false) && ((pos) < (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("abd8b862-8c59-36e9-a86a-90e04d59fd5e"))) ? (((trailerPresent) && ((pos) < (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c0a7e167-15c6-322d-b83e-00749fe03fe3"))) ? (((false) || (pos > 0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c030475b-3dec-3a6f-b1bd-b74465248e2f"))) ? (((trailerPresent) || ((pos) < (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("94cd3f7c-23d0-3c45-b7f6-23a431673d47"))) ? (((trailerPresent) && ((pos) >= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("66a30e1c-6e11-38bf-81cf-785f80237da9"))) ? (((false) || ((pos) <= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("60eaa680-a43d-3524-800d-d0c0e38b7a1a"))) ? (((true) && (pos > 0)) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("f04d3344-0f4d-3b07-a980-472f284d734e"))) ? ((false) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("2254a1ab-cce0-3b45-adc6-b840621e7581"))) ? ((false) && ((pos) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ec474e15-6b69-3be7-9a9c-38ad1d72534d"))) ? (((true) || (pos > 0)) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("52064ebc-f962-3c59-8d11-3afbf055aed3"))) ? (((trailerPresent) || ((pos) < (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b4e1092b-9369-3713-8bed-2708f7c799a6"))) ? (((!trailerPresent) || ((pos) >= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("9273d4b8-7cf4-3085-9b7b-6a43297c36b6"))) ? (((false) || ((pos) != (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f4296982-3784-3fa5-8eea-61004059ffe7"))) ? (((false) && ((pos) == (0))) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("aa71b31e-14c1-389b-8a41-60082c9f6555"))) ? (((pos) >= (0)) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("cad18ba2-8e2b-36f8-9c28-5417ff9b8918"))) ? (((!trailerPresent) || ((pos) != (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("40d31a92-c6a3-395a-9649-268465cabbcc"))) ? (((true) || ((pos) < (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ead0101b-2476-3188-b1d8-8d04b6798be1"))) ? (((!trailerPresent) && ((pos) < (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("545a4ca3-ba48-3f34-8f16-2fe8d0656e3a"))) ? (((true) && ((pos) >= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("cb6763be-1bf2-309e-b367-b6cb0d3b2507"))) ? ((!trailerPresent) && (pos > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a1a1359d-827d-3021-a9c9-b8a0e45e2260"))) ? (((true) || ((pos) >= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4d0ec531-3378-3b55-9534-63be02aae674"))) ? (((true) && (pos > 0)) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a2b5d003-782a-35b4-a8b2-dc8edea094d8"))) ? (((trailerPresent) || ((pos) >= (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ec3e435a-5d7f-3728-9880-715cce04e35b"))) ? ((pos > 0) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2888d2d6-4e43-307e-b175-6d793997d52e"))) ? ((true) || ((pos) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("089b4eb5-a256-3bd3-9f6a-b58754747da0"))) ? (((!trailerPresent) && (pos > 0)) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("2dcda9e6-017b-357c-b0ef-f03199cc4cc4"))) ? (((!trailerPresent) || ((pos) == (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("715a71c8-3114-3fcd-b9f5-adb8a3fe3437"))) ? (((!trailerPresent) || ((pos) <= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("da8c9f3e-1418-3ece-a1bd-195a6dd1975f"))) ? (((!trailerPresent) || ((pos) != (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("d2be9a00-293e-399c-b9d7-1077a408c4a7"))) ? (((false) && ((pos) == (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("cd0d055d-a639-3481-bc4e-1a2ef3f2672b"))) ? (((!trailerPresent) && ((pos) != (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ba36b0f4-2037-32c1-9554-cebd85b6270e"))) ? (((false) && ((pos) > (0))) && (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("638bb684-caf2-385e-b34f-9412b8e227b4"))) ? (((trailerPresent) || ((pos) < (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("79e1962d-771c-3a32-b7ef-9ae18b00db63"))) ? (((true) && ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3bbc3dc6-4d52-355d-9a80-e4db3b254a4f"))) ? ((false) || ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("68f69de1-da42-3d44-a045-81fbc5fdbe44"))) ? (((true) || ((pos) != (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("63b070f2-9475-3eca-930e-c1f29b641ceb"))) ? (((false) || ((pos) <= (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e07a3ce1-56a4-35b5-a714-a167e69f9d3a"))) ? (((false) && ((pos) == (0))) && ((pos) == (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("5e0706b1-58c1-30d9-835e-d2430d68385f"))) ? (((trailerPresent) && ((pos) >= (0))) || ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8de7c0fb-7fc5-3936-a160-ea880759dd1d"))) ? (((true) || ((pos) <= (0))) || (pos == walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("74eac52b-b305-3c2f-b10b-1bb687c40444"))) ? (((trailerPresent) || ((pos) > (0))) && ((pos) != (walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("eb2de573-bbbd-3f3b-9e65-6972e530a236"))) ? ((trailerPresent) || ((pos) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bfdcb5e8-94a5-3cdb-9008-7b0eaded22de"))) ? (((trailerPresent) || (pos > 0)) || ((pos) != (walEditsStopOffset))) : (trailerPresent && pos > 0 && pos == walEditsStopOffset))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.trace("Reached end of expected edits area at offset {}", pos);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns names of the accepted writer classes
   */
  public List<String> getWriterClsNames() {
if(KnobRuntime.check(java.util.UUID.fromString("c834510b-0f6a-39a9-b35a-a54de0af5031"))) {
return null;
}
    return WRITER_CLS_NAMES;
  }

  /**
   * Returns the cell codec classname
   */
  public String getCodecClsName() {
    return codecClsName;
  }

  public long getPosition() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ebaa0d50-b696-31c6-93c2-a548c0f332e3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7517d269-5643-3f25-97a7-6fbd4655d7ad"))) {
return 0;
}
    return inputStream != null ? inputStream.getPos() : -1;
  }

  public long trailerSize() {
if(KnobRuntime.check(java.util.UUID.fromString("eed020d2-6cf3-37dd-b6c0-8895434dc9cb"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ed7d3221-0396-3c7c-b46e-f8eae2dcd444"))) ? (!trailerPresent) : (trailerPresent))) {
      // sizeof PB_WAL_COMPLETE_MAGIC + sizeof trailerSize + trailer
      final long calculatedSize =
        (long) PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT + trailer.getSerializedSize();
      final long expectedSize = fileLength - walEditsStopOffset;
      if (expectedSize != calculatedSize) {
        LOG.warn("After parsing the trailer, we expect the total footer to be {} bytes, but we "
          + "calculate it as being {}", expectedSize, calculatedSize);
      }
      return expectedSize;
    } else {
      return -1L;
    }
  }

  protected final String getPositionQuietly() {
    try {
      long pos = getPosition();
      return pos >= 0 ? Long.toString(pos) : "<unknown>";
    } catch (Exception e) {
      LOG.warn("failed to get position, ignoring", e);
      return "<unknown>";
    }
  }

  protected final IOException extractHiddenEof(Exception ex) {
    // There are two problems we are dealing with here. Hadoop stream throws generic exception
    // for EOF, not EOFException; and scanner further hides it inside RuntimeException.
    IOException ioEx = null;
    if (ex instanceof EOFException) {
      return (EOFException) ex;
    } else if (ex instanceof IOException) {
      ioEx = (IOException) ex;
    } else if (
      ex instanceof RuntimeException && ex.getCause() != null
        && ex.getCause() instanceof IOException
    ) {
      ioEx = (IOException) ex.getCause();
    }
    if ((ioEx != null) && (ioEx.getMessage() != null)) {
      if (ioEx.getMessage().contains("EOF")) {
        return ioEx;
      }
      return null;
    }
    return null;
  }

  /**
   * This is used to determine whether we have already reached the WALTrailer. As the size and magic
   * are at the end of the WAL file, it is possible that these two options are missing while
   * writing, so we will consider there is no trailer. And when we actually reach the WALTrailer, we
   * will try to decode it as WALKey and we will fail but the error could be varied as it is parsing
   * WALTrailer actually.
   * @return whether this is a WALTrailer and we should throw EOF to upper layer the file is done
   */
  protected final boolean isWALTrailer(long startPosition) throws IOException {
    // We have nothing in the WALTrailer PB message now so its size is just an int length size and a
    // magic at the end
    int trailerSize = PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT;
    if (fileLength - startPosition >= trailerSize) {
      // We still have more than trailerSize bytes before reaching the EOF so this is not a trailer.
      // We also test for == here because if this is a valid trailer, we can read it while opening
      // the reader, so we should not reach here
      return false;
    }
    inputStream.seek(startPosition);
    for (int i = 0; i < 4; i++) {
      int r = inputStream.read();
      if (r == -1) {
        // we have reached EOF while reading the length, and all bytes read are 0, so we assume this
        // is a partial trailer
        return true;
      }
      if (r != 0) {
        // the length is not 0, should not be a trailer
        return false;
      }
    }
    for (int i = 0; i < PB_WAL_COMPLETE_MAGIC.length; i++) {
      int r = inputStream.read();
      if (r == -1) {
        // we have reached EOF while reading the magic, and all bytes read are matched, so we assume
        // this is a partial trailer
        return true;
      }
      if (r != (PB_WAL_COMPLETE_MAGIC[i] & 0xFF)) {
        // does not match magic, should not be a trailer
        return false;
      }
    }
    // in fact, we should not reach here, as this means the trailer bytes are all matched and
    // complete, then we should not call this method...
    return true;
  }

  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("6e35c3ac-8f7c-39d5-9ecf-c2c73996e2d7"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("84122dee-f5ce-34f9-ab93-23025ddb45be"))) ? ((inputStream) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6a134b0c-405f-3c93-b034-9ce22f11fa65"))) ? ((inputStream) == (null)) : (inputStream != null))))) {
      IOUtils.closeQuietly(inputStream);
      inputStream = null;
    }
  }
}
