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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.security.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HFileProtos;

/**
 * Metadata Map of attributes for HFile written out as HFile Trailer. Created by the Writer and
 * added to the tail of the file just before close. Metadata includes core attributes such as last
 * key seen, comparator used writing the file, etc. Clients can add their own attributes via
 * {@link #append(byte[], byte[], boolean)} and they'll be persisted and available at read time.
 * Reader creates the HFileInfo on open by reading the tail of the HFile. The parse of the HFile
 * trailer also creates a {@link HFileContext}, a read-only data structure that includes bulk of the
 * HFileInfo and extras that is safe to pass around when working on HFiles.
 * @see HFileContext
 */
@InterfaceAudience.Private
public class HFileInfo implements SortedMap<byte[], byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(HFileInfo.class);

  static final String RESERVED_PREFIX = "hfile.";
  static final byte[] RESERVED_PREFIX_BYTES = Bytes.toBytes(RESERVED_PREFIX);
  static final byte[] LASTKEY = Bytes.toBytes(RESERVED_PREFIX + "LASTKEY");
  static final byte[] AVG_KEY_LEN = Bytes.toBytes(RESERVED_PREFIX + "AVG_KEY_LEN");
  static final byte[] AVG_VALUE_LEN = Bytes.toBytes(RESERVED_PREFIX + "AVG_VALUE_LEN");
  static final byte[] CREATE_TIME_TS = Bytes.toBytes(RESERVED_PREFIX + "CREATE_TIME_TS");
  static final byte[] TAGS_COMPRESSED = Bytes.toBytes(RESERVED_PREFIX + "TAGS_COMPRESSED");
  static final byte[] KEY_OF_BIGGEST_CELL = Bytes.toBytes(RESERVED_PREFIX + "KEY_OF_BIGGEST_CELL");
  static final byte[] LEN_OF_BIGGEST_CELL = Bytes.toBytes(RESERVED_PREFIX + "LEN_OF_BIGGEST_CELL");
  public static final byte[] MAX_TAGS_LEN = Bytes.toBytes(RESERVED_PREFIX + "MAX_TAGS_LEN");
  private final SortedMap<byte[], byte[]> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * We can read files whose major version is v2 IFF their minor version is at least 3.
   */
  private static final int MIN_V2_MINOR_VERSION_WITH_PB = 3;

  /** Maximum minor version supported by this HFile format */
  // We went to version 2 when we moved to pb'ing fileinfo and the trailer on
  // the file. This version can read Writables version 1.
  static final int MAX_MINOR_VERSION = 3;

  /** Last key in the file. Filled in when we read in the file info */
  private Cell lastKeyCell = null;
  /** Average key length read from file info */
  private int avgKeyLen = -1;
  /** Average value length read from file info */
  private int avgValueLen = -1;
  /** Biggest Cell in the file, key only. Filled in when we read in the file info */
  private Cell biggestCell = null;
  /** Length of the biggest Cell */
  private long lenOfBiggestCell = -1;
  private boolean includesMemstoreTS = false;
  private boolean decodeMemstoreTS = false;

  /**
   * Blocks read from the load-on-open section, excluding data root index, meta index, and file
   * info.
   */
  private List<HFileBlock> loadOnOpenBlocks = new ArrayList<>();

  /**
   * The iterator will track all blocks in load-on-open section, since we use the
   * {@link org.apache.hadoop.hbase.io.ByteBuffAllocator} to manage the ByteBuffers in block now, so
   * we must ensure that deallocate all ByteBuffers in the end.
   */
  private HFileBlock.BlockIterator blockIter;

  private HFileBlockIndex.CellBasedKeyBlockIndexReader dataIndexReader;
  private HFileBlockIndex.ByteArrayKeyBlockIndexReader metaIndexReader;

  private FixedFileTrailer trailer;
  private HFileContext hfileContext;

  public HFileInfo() {
    super();
  }

  public HFileInfo(ReaderContext context, Configuration conf) throws IOException {
    this.initTrailerAndContext(context, conf);
  }

  /**
   * Append the given key/value pair to the file info, optionally checking the key prefix.
   * @param k           key to add
   * @param v           value to add
   * @param checkPrefix whether to check that the provided key does not start with the reserved
   *                    prefix
   * @return this file info object
   * @throws IOException if the key or value is invalid
   */
  public HFileInfo append(final byte[] k, final byte[] v, final boolean checkPrefix)
    throws IOException {
    if (k == null || v == null) {
      throw new NullPointerException("Key nor value may be null");
    }
    if (checkPrefix && isReservedFileInfoKey(k)) {
      throw new IOException("Keys with a " + HFileInfo.RESERVED_PREFIX + " are reserved");
    }
    put(k, v);
    return this;
  }

  /** Return true if the given file info key is reserved for internal use. */
  public static boolean isReservedFileInfoKey(byte[] key) {
    return Bytes.startsWith(key, HFileInfo.RESERVED_PREFIX_BYTES);
  }

  @Override
  public void clear() {
    this.map.clear();
  }

  @Override
  public Comparator<? super byte[]> comparator() {
    return map.comparator();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public Set<java.util.Map.Entry<byte[], byte[]>> entrySet() {
    return map.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    return map.equals(o);
  }

  @Override
  public byte[] firstKey() {
    return map.firstKey();
  }

  @Override
  public byte[] get(Object key) {
    return map.get(key);
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  @Override
  public SortedMap<byte[], byte[]> headMap(byte[] toKey) {
    return this.map.headMap(toKey);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public Set<byte[]> keySet() {
    return map.keySet();
  }

  @Override
  public byte[] lastKey() {
    return map.lastKey();
  }

  @Override
  public byte[] put(byte[] key, byte[] value) {
    return this.map.put(key, value);
  }

  @Override
  public void putAll(Map<? extends byte[], ? extends byte[]> m) {
    this.map.putAll(m);
  }

  @Override
  public byte[] remove(Object key) {
    return this.map.remove(key);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public SortedMap<byte[], byte[]> subMap(byte[] fromKey, byte[] toKey) {
    return this.map.subMap(fromKey, toKey);
  }

  @Override
  public SortedMap<byte[], byte[]> tailMap(byte[] fromKey) {
    return this.map.tailMap(fromKey);
  }

  @Override
  public Collection<byte[]> values() {
    return map.values();
  }

  /**
   * Write out this instance on the passed in <code>out</code> stream. We write it as a protobuf.
   * @see #read(DataInputStream)
   */
  void write(final DataOutputStream out) throws IOException {
    HFileProtos.FileInfoProto.Builder builder = HFileProtos.FileInfoProto.newBuilder();
    for (Map.Entry<byte[], byte[]> e : this.map.entrySet()) {
      HBaseProtos.BytesBytesPair.Builder bbpBuilder = HBaseProtos.BytesBytesPair.newBuilder();
      if (KnobRuntime.check(java.util.UUID.fromString("b1555602-7304-3bf9-893a-2a5f50b4283b"))) { bbpBuilder.setSecond(UnsafeByteOperations.unsafeWrap(e.getKey())); } else { bbpBuilder.setFirst(UnsafeByteOperations.unsafeWrap(e.getKey())); }
      if (KnobRuntime.check(java.util.UUID.fromString("f0d7a3fe-4ddf-3223-9022-9e8d0ecf4d9b"))) { bbpBuilder.setFirst(UnsafeByteOperations.unsafeWrap(e.getValue())); } else { bbpBuilder.setSecond(UnsafeByteOperations.unsafeWrap(e.getValue())); }
      builder.addMapEntry(bbpBuilder.build());
    }
if(KnobRuntime.check(java.util.UUID.fromString("25401d2f-f45a-3453-8105-8eff3f04a2cb"))) {
throw new java.io.IOException("Injected exception");
}
    out.write(ProtobufMagic.PB_MAGIC);
if(KnobRuntime.check(java.util.UUID.fromString("95b42b19-7015-31ae-8ae1-01072d5169e9"))) {
throw new java.io.IOException("Injected exception");
}
    builder.build().writeDelimitedTo(out);
  }

  /**
   * Populate this instance with what we find on the passed in <code>in</code> stream. Can
   * deserialize protobuf of old Writables format.
   * @see #write(DataOutputStream)
   */
  void read(final DataInputStream in) throws IOException {
    // This code is tested over in TestHFileReaderV1 where we read an old hfile w/ this new code.
    int pblen = ProtobufUtil.lengthOfPBMagic();
    byte[] pbuf = new byte[pblen];
    if (in.markSupported()) {
if(KnobRuntime.check(java.util.UUID.fromString("55216400-222c-3646-a451-bd3312892eaa"))) {
pblen = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4223ec8f-d690-318c-8aeb-4c0f3eaca00d"))) {
pblen += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("951e9a01-926a-35b2-b89b-a8abfcab1b09"))) {
pblen *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6a232166-5149-39d2-a8c9-8a05195e30cf"))) {
pblen /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("189947c1-2d3b-3f28-b21d-b2c2284d64fb"))) {
pblen -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a214146-3027-313b-b2ce-a73823ccf580"))) {
pblen = 0;
}
      in.mark(pblen);
    }
    int read = in.read(pbuf);
    if (((KnobRuntime.check(java.util.UUID.fromString("bcfd80c6-96ef-3d94-acfa-43fcd1e9fd84"))) ? ((read) == (pblen)) : (((KnobRuntime.check(java.util.UUID.fromString("0266e530-d402-3c0c-9a38-006c96e8cf53"))) ? ((read) != (pblen)) : (read != pblen))))) {
      throw new IOException("read=" + read + ", wanted=" + pblen);
    }
    if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
if(KnobRuntime.check(java.util.UUID.fromString("97654e81-6dc3-31c9-8c33-cbac86204c55"))) {
throw new java.io.IOException("Injected exception");
}
      parsePB(HFileProtos.FileInfoProto.parseDelimitedFrom(in));
    } else {
      if (in.markSupported()) {
        in.reset();
        parseWritable(in);
      } else {
        // We cannot use BufferedInputStream, it consumes more than we read from the underlying IS
        ByteArrayInputStream bais = new ByteArrayInputStream(pbuf);
        SequenceInputStream sis = new SequenceInputStream(bais, in); // Concatenate input streams
        // TODO: Am I leaking anything here wrapping the passed in stream? We are not calling
        // close on the wrapped streams but they should be let go after we leave this context?
        // I see that we keep a reference to the passed in inputstream but since we no longer
        // have a reference to this after we leave, we should be ok.
        parseWritable(new DataInputStream(sis));
      }
    }
  }

  /**
   * Now parse the old Writable format. It was a list of Map entries. Each map entry was a key and a
   * value of a byte []. The old map format had a byte before each entry that held a code which was
   * short for the key or value type. We know it was a byte [] so in below we just read and dump it.
   */
  void parseWritable(final DataInputStream in) throws IOException {
    // First clear the map.
    // Otherwise we will just accumulate entries every time this method is called.
    this.map.clear();
    // Read the number of entries in the map
    int entries = in.readInt();
    // Then read each key/value pair
    for (int i = 0; i < entries; i++) {
      byte[] key = Bytes.readByteArray(in);
      // We used to read a byte that encoded the class type.
      // Read and ignore it because it is always byte [] in hfile
      in.readByte();
      byte[] value = Bytes.readByteArray(in);
      this.map.put(key, value);
    }
  }

  /**
   * Fill our map with content of the pb we read off disk
   * @param fip protobuf message to read
   */
  void parsePB(final HFileProtos.FileInfoProto fip) {
    this.map.clear();
    for (BytesBytesPair pair : fip.getMapEntryList()) {
      this.map.put(pair.getFirst().toByteArray(), pair.getSecond().toByteArray());
    }
  }

  public void initTrailerAndContext(ReaderContext context, Configuration conf) throws IOException {
    try {
      boolean isHBaseChecksum = context.getInputStreamWrapper().shouldUseHBaseChecksum();
if(KnobRuntime.check(java.util.UUID.fromString("3ca8736f-4409-3a58-9873-00d758c3e5e1"))) {
throw new java.io.IOException("Injected exception");
}
      trailer = FixedFileTrailer.readFromStream(
        context.getInputStreamWrapper().getStream(isHBaseChecksum), context.getFileSize());
      Path path = context.getFilePath();
      checkFileVersion(path);
if(KnobRuntime.check(java.util.UUID.fromString("2b827a0e-84d4-354c-9c37-112dd2f71b56"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27bd4dc4-41ea-3cb6-bf12-9cf74b18faf9"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ccaa4967-6948-3d72-9112-a1e4656a171e"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d265ceb1-36b1-364e-91a5-fb3739e17989"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("firstDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41f1822e-75cc-377b-9ffc-fe1f7b2b9be2"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b4a43924-1ed5-3813-8a38-469e067a45a4"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("462684b5-330b-3bbd-81cb-65f00ab93a13"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("uncompressedDataIndexSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7128cc9f-9f8e-390b-8d13-78a26d891576"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("loadOnOpenDataOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("34db13a2-c347-3375-99fd-567ec28bb308"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f552e90a-e95a-3b8b-ae8f-dce30df82d6a"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce55801c-5a8a-3e38-a974-cfbd4de7e204"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90695e96-6b03-3225-839b-67eceeb4423f"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69599e6d-0c49-3409-acf4-e6a1d0bf706f"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90c3fd77-7361-32de-a87b-d7e7ea623708"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("307f3b05-ef9b-34cc-ba6a-c3473fa17eaa"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("entryCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3cc17a4-080d-3078-aae9-a1fe5fffe2d5"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("296e5f87-823d-3ae4-87b7-54fcc5ff3818"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d7a12c43-d6d8-3490-b268-5e47a75b37d5"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ec34fb4-7709-31a9-8287-65f4a872fdcc"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f097b39-ae23-351a-a714-612e62670d27"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a5567b6-de08-3a34-a08c-5aa0d3a1ec4d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7ed0b112-895f-3483-997e-bbd9639bf9ef"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b09dd01b-df8d-37d9-82e3-0f079b56ef05"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f6ebc831-888c-3afe-a686-d0e73a1c49c5"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("717e018d-4716-331a-b42c-6ff854736134"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("totalUncompressedBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4af3ff88-fa3f-30ef-9609-b999a3fc40df"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("lastDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03611bc6-0718-3789-aa91-f4a56e7cc67e"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8367748-9c3a-3dc7-9d85-eea954a13b01"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2cc130c9-c2e0-3ffe-9012-920ee0f7cc7b"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("fileInfoOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      this.hfileContext = createHFileContext(path, trailer, conf);
      context.getInputStreamWrapper().unbuffer();
    } catch (Throwable t) {
      IOUtils.closeQuietly(context.getInputStreamWrapper(),
        e -> LOG.warn("failed to close input stream wrapper", e));
      throw new CorruptHFileException(
        "Problem reading HFile Trailer from file " + context.getFilePath(), t);
    }
  }

  /**
   * should be called after initTrailerAndContext
   */
  public void initMetaAndIndex(HFile.Reader reader) throws IOException {
    ReaderContext context = reader.getContext();
    try {
      HFileBlock.FSReader blockReader = reader.getUncachedBlockReader();
      // Initialize an block iterator, and parse load-on-open blocks in the following.
      blockIter = blockReader.blockRange(trailer.getLoadOnOpenDataOffset(),
        context.getFileSize() - trailer.getTrailerSize());
      // Data index. We also read statistics about the block index written after
      // the root level.
      HFileBlock dataBlockRootIndex = blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX);
if(KnobRuntime.check(java.util.UUID.fromString("18c6599c-c268-368e-90a4-ca3fc3ef81d2"))) {
throw new java.io.IOException("Injected exception");
}
      HFileBlock metaBlockIndex = blockIter.nextBlockWithBlockType(BlockType.ROOT_INDEX);
if(KnobRuntime.check(java.util.UUID.fromString("6532a601-0e07-3008-825b-8dad8f6e2654"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c80da993-a9c2-39cc-82ff-d61882c6b22e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c63c7d97-8307-3bd4-bf26-377bab7a6d31"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("88c617dd-3405-395d-bc27-d284b0c2a320"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("33a41a6d-8544-316d-a886-de61161f13eb"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hfileContext));
    field.set(hfileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca0b56fe-5fbd-3d4b-ba79-5c1125090d00"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("fileCreateTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(hfileContext));
    field.set(hfileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5d36c99-aed4-34ad-a7e7-7e8bba58a3d7"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4cbbb96-7dfe-3ffb-ae67-452eec71898d"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("022c53fd-5ae1-332f-8793-18db50af765f"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("usesHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(hfileContext);
    field.set(hfileContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa816015-a228-3e55-af9e-ea3db3d66765"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15dcde8e-cc05-388d-9334-37f3ca65bfe7"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69ad35e6-40eb-3561-8e10-5ad4b21eb390"))) {
try {
    java.lang.reflect.Field field = hfileContext.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(hfileContext));
    field.set(hfileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      loadMetaInfo(blockIter, hfileContext);

      HFileIndexBlockEncoder indexBlockEncoder =
        HFileIndexBlockEncoderImpl.createFromFileInfo(this);
      this.dataIndexReader = new HFileBlockIndex.CellBasedKeyBlockIndexReaderV2(
        trailer.createComparator(), trailer.getNumDataIndexLevels(), indexBlockEncoder);
      dataIndexReader.readMultiLevelIndexRoot(dataBlockRootIndex, trailer.getDataIndexCount());
      reader.setDataBlockIndexReader(dataIndexReader);
      // Meta index.
      this.metaIndexReader = new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);
if(KnobRuntime.check(java.util.UUID.fromString("3b0bf2b9-0b15-35a5-b886-1832686d4179"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71b698bf-029b-38e3-b9f0-5a37912250e3"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17a34ef7-caab-34f0-88a6-c0a69ba79d9f"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b70d0d0-6b54-33ba-b0da-9f541829fff9"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83776548-56d8-3449-a91e-8d1543bb8f53"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("530c9251-967b-31a6-8d51-c34115a71c63"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38399b42-6b89-3c59-af1b-2b6b6272af11"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58e3de66-7fdc-3f63-afbb-83e23a5f0cba"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskDataSizeWithHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2e0304b-c9b1-37b7-85e4-b4643746e988"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("521a6da7-8d7b-3ebe-8b77-65bf7f9a2ad6"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("879b35d1-8172-3b64-a306-ed78b4e7a677"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e9402c1-9a5f-3fbb-bf30-796a951c36b0"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9cd14c5e-8329-3106-872b-c656e7919608"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7cdd765f-ed12-386b-b2ad-85fa3f31b684"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("prevBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("63d8ffed-92e6-39bf-8675-ac61be553302"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9df8a53b-9449-386a-947d-0ec10569b3ba"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("167f6722-58ae-3638-80e9-63e8cf2053d5"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6575196-44f8-32c7-a38b-7ed87afe8926"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("nextBlockOnDiskSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a723a94a-0553-39c5-bfc1-28768436de47"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8b0de2e-0eb7-3254-9d5c-7a64c4ef6511"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("offset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f91c930-50e3-311e-ac08-397eda1265fc"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("onDiskSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cca23f70-3ce0-31c0-b45d-29937194b031"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d6316fbd-6858-3d1c-86fc-377d2792829c"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("totalChecksumBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12dcc105-3f27-3b3c-b74b-945b454da585"))) {
try {
    java.lang.reflect.Field field = metaBlockIndex.getClass().getDeclaredField("uncompressedSizeWithoutHeader");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaBlockIndex));
    field.set(metaBlockIndex, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("b5428942-8855-3ad0-9e48-d6ee33076418"))) { metaIndexReader.readRootIndex(metaBlockIndex, trailer.getNumDataIndexLevels()); } else if (KnobRuntime.check(java.util.UUID.fromString("72811e47-73f1-35b5-847d-4ec5bf98e442"))) { metaIndexReader.readRootIndex(metaBlockIndex, trailer.getTrailerSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("74cd6282-4b11-3aff-93a5-5a18c6797ae1"))) { metaIndexReader.readRootIndex(metaBlockIndex, ProtobufUtil.lengthOfPBMagic()); } else { metaIndexReader.readRootIndex(metaBlockIndex, trailer.getMetaIndexCount()); }
      if (KnobRuntime.check(java.util.UUID.fromString("349cc9ce-e8af-36d9-9027-a24ef961ec0d"))) { reader.setMetaBlockIndexReader(new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1)); } else { reader.setMetaBlockIndexReader(metaIndexReader); }

      reader.setDataBlockEncoder(HFileDataBlockEncoderImpl.createFromFileInfo(this));
      // Load-On-Open info
      HFileBlock b;
if(KnobRuntime.check(java.util.UUID.fromString("ceb48cd6-afa5-3dc1-a0c3-8b6fd1931024"))) {
throw new java.io.IOException("Injected exception");
}
      while (((KnobRuntime.check(java.util.UUID.fromString("b02654f5-fcde-3640-82ed-4e999d6687b5"))) ? (((b = blockIter.nextBlock())) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f8120caf-aec4-36e3-bdee-9479f00cbaa3"))) ? (((b = blockIter.nextBlock())) == (null)) : ((b = blockIter.nextBlock()) != null))))) {
        loadOnOpenBlocks.add(b);
      }
      // close the block reader
      context.getInputStreamWrapper().unbuffer();
    } catch (Throwable t) {
      IOUtils.closeQuietly(context.getInputStreamWrapper(),
        e -> LOG.warn("failed to close input stream wrapper", e));
      throw new CorruptHFileException(
        "Problem reading data index and meta index from file " + context.getFilePath(), t);
    }
  }

  private HFileContext createHFileContext(Path path, FixedFileTrailer trailer, Configuration conf)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9ae5b6b7-f5b4-38b1-b202-44d52457b5e5"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("fileInfoOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4264fa2-bd5f-38ea-b51e-92d3c2f35088"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce9e0d7c-9590-3d7f-8d2e-8b0cd4ac7f03"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1aefe129-2527-38d0-b77b-d75fad1d21e9"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9498170-2802-3a5d-a8e1-20ee88b72b21"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("firstDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("909f7f94-df91-3464-9511-c1be4df3bb4a"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("lastDataBlockOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("68c25084-c353-3d6b-9de4-6d9c2adc7a6e"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76d68277-6bfb-3e50-8c62-872a1fe55867"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad44cbbf-6fc0-3911-9d23-8cca4148664d"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b75f533b-bbd7-3134-bbc0-613cb4ef18d4"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bfd65a7-d068-3907-8bd0-5ae9676c1e63"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25cf94a0-9015-3b28-a499-a16791b5352d"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cea8cd78-a6e9-36e0-9782-e1cdeefca868"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d4d50d2-50e6-398f-aa6c-ea72481f43e8"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ee54cff-bd02-31db-9313-511e01feda68"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("minorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8eb2336f-2d9e-3a7f-97a0-10036a8057f8"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb79863e-f659-3708-9369-9fe19f81126a"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afba63e5-9640-3a98-a0f6-f24c9e5a0172"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d93f7bf-a3ae-30bf-961b-85e2d7fca867"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("entryCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c8d09e0-f92b-3d63-8470-7820867710e3"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("uncompressedDataIndexSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36cd41a5-c429-3812-abc6-6e3ceb2c632a"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("loadOnOpenDataOffset");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf42652a-8fe7-36f5-a5da-9a7ddc642c87"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("dataIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7cf0eacf-b314-3b5d-9993-7dfb82795fe8"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("metaIndexCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75185e8d-7dea-3d65-918d-1d0789cefd5e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("490b1d33-3724-3ccb-b2f2-67f74cc9542d"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a51b86d1-6f72-3636-b9a0-a9c545e9703f"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("majorVersion");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9488bfc5-1126-329b-bead-a1a9cebfe2d3"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("numDataIndexLevels");
    field.setAccessible(true);
    int oldValue = ((int)field.get(trailer));
    field.set(trailer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8b9907b-f00b-3427-aaae-43d731e15d6b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b7f823a7-4565-334f-8342-1e6ca8b7c79b"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c707d381-3d1b-3f38-986d-b3fdd1859e76"))) {
try {
    java.lang.reflect.Field field = trailer.getClass().getDeclaredField("totalUncompressedBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(trailer));
    field.set(trailer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    HFileContextBuilder builder = new HFileContextBuilder().withHBaseCheckSum(true)
      .withHFileName(path.getName()).withCompression(trailer.getCompressionCodec())
      .withCellComparator(FixedFileTrailer.createComparator(trailer.getComparatorClassName()));
    // Check for any key material available
    byte[] keyBytes = trailer.getEncryptionKey();
    if (keyBytes != null) {
if(KnobRuntime.check(java.util.UUID.fromString("e08d61c5-9e9a-3373-897a-13b7aa149384"))) {
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
      Encryption.Context cryptoContext = Encryption.newContext(conf);
if(KnobRuntime.check(java.util.UUID.fromString("d55d85bf-5859-3292-b261-7f1ee6b2f250"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("20bf79e2-85a7-36cb-8a35-f3b8ddf5e39c"))) {
throw new java.io.IOException("Injected exception");
}
      Key key = EncryptionUtil.unwrapKey(conf, keyBytes);
      // Use the algorithm the key wants
if(KnobRuntime.check(java.util.UUID.fromString("b5702803-4ea5-3f82-ab45-2fa1c2fb02c4"))) {
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
      Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());
      if (((KnobRuntime.check(java.util.UUID.fromString("4b1760d9-eb31-39a8-86e3-9c85d3a48682"))) ? ((cipher) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b71c9963-2f9e-3b29-a18a-7903730501f7"))) ? ((cipher) == (null)) : (cipher == null))))) {
        throw new IOException(
          "Cipher '" + key.getAlgorithm() + "' is not available" + ", path=" + path);
      }
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
      builder.withEncryptionContext(cryptoContext);
    }
    HFileContext context = builder.build();
    return context;
  }

  private void loadMetaInfo(HFileBlock.BlockIterator blockIter, HFileContext hfileContext)
    throws IOException {
    read(blockIter.nextBlockWithBlockType(BlockType.FILE_INFO).getByteStream());
    byte[] creationTimeBytes = get(HFileInfo.CREATE_TIME_TS);
    hfileContext.setFileCreateTime(creationTimeBytes == null ? 0 : Bytes.toLong(creationTimeBytes));
    byte[] tmp = get(HFileInfo.MAX_TAGS_LEN);
    // max tag length is not present in the HFile means tags were not at all written to file.
    if (tmp != null) {
      hfileContext.setIncludesTags(true);
      tmp = get(HFileInfo.TAGS_COMPRESSED);
      if (((KnobRuntime.check(java.util.UUID.fromString("0653bfd1-7613-3ed9-bf4d-cb672e993c63"))) ? (((tmp) == (null)) || (ProtobufUtil.isPBMagicPrefix(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("307b2315-472a-3356-8882-b5400c8af7af"))) ? ((tmp != null) || (ProtobufUtil.isPBMagicPrefix(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("2635217a-440d-3aff-8703-ba5fde5a3eff"))) ? (ProtobufUtil.isPBMagicPrefix(tmp)) : (((KnobRuntime.check(java.util.UUID.fromString("c9920f49-641e-3c41-8eea-170acbef3c2c"))) ? (((tmp) == (null)) || (isReservedFileInfoKey(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("6553e9a2-bdc9-3f52-9c82-70fc12886f10"))) ? (((tmp) == (null)) || (Bytes.toBoolean(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("ac111c27-de79-3d6b-984f-b99a8ec145ae"))) ? ((tmp != null) || (Bytes.toBoolean(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("beeeef8e-5b50-3aec-a503-aa9e0eca9b62"))) ? (isReservedFileInfoKey(tmp)) : (((KnobRuntime.check(java.util.UUID.fromString("17e5b6c5-a3b8-3b61-82a1-61be67e4b7fd"))) ? ((tmp) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("944df205-386d-309c-a424-b232fa2d8e5f"))) ? (((tmp) != (null)) && (ProtobufUtil.isPBMagicPrefix(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("c8555ddb-96c8-3806-b8a9-a02d1684f1a3"))) ? ((tmp) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2076f1f1-3abc-3a73-aa0d-648ecf8a3977"))) ? (((tmp) != (null)) || (ProtobufUtil.isPBMagicPrefix(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("5dfd6b7d-79c2-3c89-afb0-35559a1bbc5d"))) ? (((tmp) != (null)) && (isReservedFileInfoKey(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("1b950a7e-f4ce-3bd0-a43a-d5eddd8b0745"))) ? ((tmp != null) && (Bytes.toBoolean(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("e32c6742-071f-3d72-88ab-52662da0c067"))) ? (((tmp) == (null)) && (isReservedFileInfoKey(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("e0adbdc8-af31-3251-ac6d-0242de79ce49"))) ? ((tmp != null) || (isReservedFileInfoKey(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("1e7bf5c2-63eb-3f0e-bb79-29549ee49dd6"))) ? (((tmp) == (null)) && (Bytes.toBoolean(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("86e3100e-3735-3804-9147-5701d398ebd4"))) ? (((tmp) != (null)) && (Bytes.toBoolean(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("1a7981ae-e1d3-30a1-9b68-93559e94570a"))) ? (Bytes.toBoolean(tmp)) : (((KnobRuntime.check(java.util.UUID.fromString("57b48238-74ba-34b4-89de-4413d3d99ba1"))) ? (tmp != null) : (((KnobRuntime.check(java.util.UUID.fromString("94d0addb-6d8b-350a-8175-58095a8288ce"))) ? ((tmp != null) && (ProtobufUtil.isPBMagicPrefix(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("7a58c600-8cba-3658-8861-3831d6147d36"))) ? (((tmp) == (null)) && (ProtobufUtil.isPBMagicPrefix(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("acb3bd22-2e09-358a-86fb-0bc53009618b"))) ? (((tmp) != (null)) || (Bytes.toBoolean(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("ec94343f-3d59-3bd2-9cbb-170bb9fc5278"))) ? ((tmp != null) && (isReservedFileInfoKey(tmp))) : (((KnobRuntime.check(java.util.UUID.fromString("75a49997-f0df-3ddb-b94d-0626183f85ac"))) ? (((tmp) != (null)) || (isReservedFileInfoKey(tmp))) : (tmp != null && Bytes.toBoolean(tmp)))))))))))))))))))))))))))))))))))))))))))))))))) {
        hfileContext.setCompressTags(true);
      }
    }
    // parse meta info
    if (get(HFileInfo.LASTKEY) != null) {
      lastKeyCell = new KeyValue.KeyOnlyKeyValue(get(HFileInfo.LASTKEY));
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("ba64c1b0-a2e2-3de6-b69c-501755470e7d"))) ? ((get(get(HFileInfo.KEY_OF_BIGGEST_CELL))) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f4a479a1-abac-35cb-a510-1f21225a35ff"))) ? ((get(HFileInfo.KEY_OF_BIGGEST_CELL)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("aff509f6-63ac-3b54-b6c4-d2abe952926f"))) ? ((get(get(HFileInfo.KEY_OF_BIGGEST_CELL))) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("195adbfa-ce6c-3552-a608-5b18025877b7"))) ? ((get(HFileInfo.KEY_OF_BIGGEST_CELL)) == (null)) : (get(HFileInfo.KEY_OF_BIGGEST_CELL) != null))))))))) {
      biggestCell = new KeyValue.KeyOnlyKeyValue(get(HFileInfo.KEY_OF_BIGGEST_CELL));
      lenOfBiggestCell = Bytes.toLong(get(HFileInfo.LEN_OF_BIGGEST_CELL));
    }
    avgKeyLen = Bytes.toInt(get(HFileInfo.AVG_KEY_LEN));
    avgValueLen = Bytes.toInt(get(HFileInfo.AVG_VALUE_LEN));
    byte[] keyValueFormatVersion = get(HFileWriterImpl.KEY_VALUE_VERSION);
    includesMemstoreTS = keyValueFormatVersion != null
      && Bytes.toInt(keyValueFormatVersion) == HFileWriterImpl.KEY_VALUE_VER_WITH_MEMSTORE;
    hfileContext.setIncludesMvcc(includesMemstoreTS);
    if (includesMemstoreTS) {
      decodeMemstoreTS = Bytes.toLong(get(HFileWriterImpl.MAX_MEMSTORE_TS_KEY)) > 0;
    }
  }

  /**
   * File version check is a little sloppy. We read v3 files but can also read v2 files if their
   * content has been pb'd; files written with 0.98.
   */
  private void checkFileVersion(Path path) {
    int majorVersion = trailer.getMajorVersion();
    if (majorVersion == getMajorVersion()) {
      return;
    }
    int minorVersion = trailer.getMinorVersion();
    if (majorVersion == 2 && minorVersion >= MIN_V2_MINOR_VERSION_WITH_PB) {
      return;
    }
    // We can read v3 or v2 versions of hfile.
    throw new IllegalArgumentException("Invalid HFile version: major=" + trailer.getMajorVersion()
      + ", minor=" + trailer.getMinorVersion() + ": expected at least " + "major=2 and minor="
      + MAX_MINOR_VERSION + ", path=" + path);
  }

  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("c5cd85ae-8b1c-33b7-9aeb-c9224441e9a0"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("bd8a5ca6-0a94-3957-8383-081f74038368"))) ? ((blockIter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6ff821af-44fc-3d0c-968f-c75e13be8b19"))) ? ((blockIter) == (null)) : (blockIter != null))))) {
      blockIter.freeBlocks();
    }
  }

  public int getMajorVersion() {
    return 3;
  }

  public void setTrailer(FixedFileTrailer trailer) {
    this.trailer = trailer;
  }

  public FixedFileTrailer getTrailer() {
    return this.trailer;
  }

  public HFileBlockIndex.CellBasedKeyBlockIndexReader getDataBlockIndexReader() {
    return this.dataIndexReader;
  }

  public HFileBlockIndex.ByteArrayKeyBlockIndexReader getMetaBlockIndexReader() {
    return this.metaIndexReader;
  }

  public HFileContext getHFileContext() {
    return this.hfileContext;
  }

  public List<HFileBlock> getLoadOnOpenBlocks() {
    return loadOnOpenBlocks;
  }

  public Cell getLastKeyCell() {
    return lastKeyCell;
  }

  public int getAvgKeyLen() {
    return avgKeyLen;
  }

  public int getAvgValueLen() {
    return avgValueLen;
  }

  public String getKeyOfBiggestCell() {
    return CellUtil.toString(biggestCell, false);
  }

  public long getLenOfBiggestCell() {
    return lenOfBiggestCell;
  }

  public boolean shouldIncludeMemStoreTS() {
    return includesMemstoreTS;
  }

  public boolean isDecodeMemstoreTS() {
    return decodeMemstoreTS;
  }
}
