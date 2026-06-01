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
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.InnerStoreCellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HFileProtos;

/**
 * The {@link HFile} has a fixed trailer which contains offsets to other variable parts of the file.
 * Also includes basic metadata on this file. The trailer size is fixed within a given {@link HFile}
 * format version only, but we always store the version number as the last four-byte integer of the
 * file. The version number itself is split into two portions, a major version and a minor version.
 * The last three bytes of a file are the major version and a single preceding byte is the minor
 * number. The major version determines which readers/writers to use to read/write a hfile while a
 * minor version determines smaller changes in hfile format that do not need a new reader/writer
 * type.
 */
@InterfaceAudience.Private
public class FixedFileTrailer {
  private static final Logger LOG = LoggerFactory.getLogger(FixedFileTrailer.class);

  /**
   * We store the comparator class name as a fixed-length field in the trailer.
   */
  private static final int MAX_COMPARATOR_NAME_LENGTH = 128;

  /**
   * Offset to the fileinfo data, a small block of vitals. Necessary in v1 but only potentially
   * useful for pretty-printing in v2.
   */
  private long fileInfoOffset;

  /**
   * In version 1, the offset to the data block index. Starting from version 2, the meaning of this
   * field is the offset to the section of the file that should be loaded at the time the file is
   * being opened: i.e. on open we load the root index, file info, etc. See
   * http://hbase.apache.org/book.html#_hfile_format_2 in the reference guide.
   */
  private long loadOnOpenDataOffset;

  /**
   * The number of entries in the root data index.
   */
  private int dataIndexCount;

  /**
   * Total uncompressed size of all blocks of the data index
   */
  private long uncompressedDataIndexSize;

  /**
   * The number of entries in the meta index
   */
  private int metaIndexCount;

  /**
   * The total uncompressed size of keys/values stored in the file.
   */
  private long totalUncompressedBytes;

  /**
   * The number of key/value pairs in the file. This field was int in version 1, but is now long.
   */
  private long entryCount;

  /**
   * The compression codec used for all blocks.
   */
  private Compression.Algorithm compressionCodec = Compression.Algorithm.NONE;

  /**
   * The number of levels in the potentially multi-level data index. Used from version 2 onwards.
   */
  private int numDataIndexLevels;

  /**
   * The offset of the first data block.
   */
  private long firstDataBlockOffset;

  /**
   * It is guaranteed that no key/value data blocks start after this offset in the file.
   */
  private long lastDataBlockOffset;

  /**
   * Raw key comparator class name in version 3
   */
  // We could write the actual class name from 2.0 onwards and handle BC
  private String comparatorClassName =
    InnerStoreCellComparator.INNER_STORE_COMPARATOR.getClass().getName();

  /**
   * The encryption key
   */
  private byte[] encryptionKey;

  /**
   * The {@link HFile} format major version.
   */
  private final int majorVersion;

  /**
   * The {@link HFile} format minor version.
   */
  private final int minorVersion;

  FixedFileTrailer(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    HFile.checkFormatVersion(majorVersion);
  }

  private static int[] computeTrailerSizeByVersion() {
    int[] versionToSize = new int[HFile.MAX_FORMAT_VERSION + 1];
    // We support only 2 major versions now. ie. V2, V3
    versionToSize[2] = 212;
    for (int version = 3; version <= HFile.MAX_FORMAT_VERSION; version++) {
      // Max FFT size for V3 and above is taken as 4KB for future enhancements
      // if any.
      // Unless the trailer size exceeds 4K this can continue
      versionToSize[version] = 1024 * 4;
    }
    return versionToSize;
  }

  private static int getMaxTrailerSize() {
    int maxSize = 0;
    for (int version = HFile.MIN_FORMAT_VERSION; version <= HFile.MAX_FORMAT_VERSION; ++version) {
if(KnobRuntime.check(java.util.UUID.fromString("2ace6e1d-f229-3835-b278-8baa783eafd6"))) {
version *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("53ac5124-99b7-3e0e-9b73-9c67138d04a9"))) {
version = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd2557e1-ad29-36ec-8171-74fb6a2523a8"))) {
version -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("284c26c1-cd21-31dc-8447-95ef7fb33906"))) {
maxSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1e034b0e-c408-3fda-811f-cbcd1eb60aff"))) {
maxSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6ef72342-0716-3025-bab8-359851c06a55"))) {
maxSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bed9ae76-4808-3526-a734-111f63c5950a"))) {
version = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("eda1ac87-ebe1-3e17-984d-18716dc2c262"))) {
maxSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9d0f7a5-557e-35d6-9e19-cc2f47b7865b"))) {
version /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b33e1ef-e866-3908-9e66-156255864dc7"))) {
maxSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0fcefa7d-f7f7-3a4e-803e-dfe1f32fc754"))) {
maxSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("50399d5c-94ae-38b4-af9d-4d993dc00924"))) {
version += 1;
}
      maxSize = Math.max(getTrailerSize(version), maxSize);
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("f24906e5-e1ce-3d1a-9e7b-ffd9eb3b933a"))) ? (0) : (maxSize));
  }

  private static final int[] TRAILER_SIZE = computeTrailerSizeByVersion();
  private static final int MAX_TRAILER_SIZE = getMaxTrailerSize();

  private static final int NOT_PB_SIZE = BlockType.MAGIC_LENGTH + Bytes.SIZEOF_INT;

  static int getTrailerSize(int version) {
    return TRAILER_SIZE[version];
  }

  public int getTrailerSize() {
if(KnobRuntime.check(java.util.UUID.fromString("16c722b3-55ae-33cd-b177-023e5a3ac2f7"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("7268aa61-da34-3170-b35a-859c0a7f28cf"))) ? (extractMajorVersion(getTrailerSize(majorVersion))) : (((KnobRuntime.check(java.util.UUID.fromString("7c484e21-5e58-325e-8000-7e0bee56f80b"))) ? (getTrailerSize(getTrailerSize(majorVersion))) : (getTrailerSize(majorVersion)))));
  }

  /**
   * Write the trailer to a data stream. We support writing version 1 for testing and for
   * determining version 1 trailer size. It is also easy to see what fields changed in version 2.
   */
  void serialize(DataOutputStream outputStream) throws IOException {
    HFile.checkFormatVersion(majorVersion);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream baosDos = new DataOutputStream(baos);

    BlockType.TRAILER.write(baosDos);
    serializeAsPB(baosDos);

    // The last 4 bytes of the file encode the major and minor version universally
    baosDos.writeInt(materializeVersion(majorVersion, minorVersion));

    baos.writeTo(outputStream);
  }

  HFileProtos.FileTrailerProto toProtobuf() {
if(KnobRuntime.check(java.util.UUID.fromString("5dc38975-9bf5-3598-9208-b5605ef43d53"))) {
return null;
}
    HFileProtos.FileTrailerProto.Builder builder = HFileProtos.FileTrailerProto.newBuilder()
      .setFileInfoOffset(fileInfoOffset).setLoadOnOpenDataOffset(loadOnOpenDataOffset)
      .setUncompressedDataIndexSize(uncompressedDataIndexSize)
      .setTotalUncompressedBytes(totalUncompressedBytes).setDataIndexCount(dataIndexCount)
      .setMetaIndexCount(metaIndexCount).setEntryCount(entryCount)
      .setNumDataIndexLevels(numDataIndexLevels).setFirstDataBlockOffset(firstDataBlockOffset)
      .setLastDataBlockOffset(lastDataBlockOffset)
      .setComparatorClassName(getHBase1CompatibleName(comparatorClassName))
      .setCompressionCodec(compressionCodec.ordinal());
    if (((KnobRuntime.check(java.util.UUID.fromString("65e89ada-cd4c-31d9-9547-86ccc08ddbe1"))) ? ((encryptionKey) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("68a9fd30-e31d-333f-92c6-7455266c586e"))) ? ((encryptionKey) == (null)) : (encryptionKey != null))))) {
      builder.setEncryptionKey(UnsafeByteOperations.unsafeWrap(encryptionKey));
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("4e5221b4-e3ad-3f78-88d2-0da9bb7db247"))) ? (toProtobuf()) : (builder.build()));
  }

  /**
   * Write trailer data as protobuf. NOTE: we run a translation on the comparator name and will
   * serialize the old hbase-1.x where it makes sense. See {@link #getHBase1CompatibleName(String)}.
   */
  void serializeAsPB(DataOutputStream output) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1783207b-4e2a-3274-8dfc-321d193abded"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c12274a-2c75-33e0-ac3f-68c40b34a7c8"))) {
throw new java.io.IOException("Injected exception");
}
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // We need this extra copy unfortunately to determine the final size of the
    // delimited output, see use of baos.size() below.
if(KnobRuntime.check(java.util.UUID.fromString("f8038e06-07f0-316f-a68b-9ac7dde67317"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("96f707ab-7518-328c-9646-026699f1e018"))) { toProtobuf().writeDelimitedTo(new ByteArrayOutputStream()); } else { toProtobuf().writeDelimitedTo(baos); }
    baos.writeTo(output);
    // Pad to make up the difference between variable PB encoding length and the
    // length when encoded as writable under earlier V2 formats. Failure to pad
    // properly or if the PB encoding is too big would mean the trailer wont be read
    // in properly by HFile.
    int padding = getTrailerSize() - NOT_PB_SIZE - baos.size();
    if (padding < 0) {
      throw new IOException("Pbuf encoding size exceeded fixed trailer size limit");
    }
    for (int i = 0; i < padding; i++) {
      output.write(0);
    }
  }

  /**
   * Deserialize the fixed file trailer from the given stream. The version needs to already be
   * specified. Make sure this is consistent with {@link #serialize(DataOutputStream)}.
   */
  void deserialize(DataInputStream inputStream) throws IOException {
    HFile.checkFormatVersion(majorVersion);

if(KnobRuntime.check(java.util.UUID.fromString("8218da9c-1e3d-392a-9cf2-bc4ca60f1b20"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("c1bd46bd-446c-3301-a11c-0f457451a21d"))) { deserializeFromPB(inputStream); } else { BlockType.TRAILER.readAndCheck(inputStream); }

    if (
      majorVersion > 2
        || (majorVersion == 2 && minorVersion >= HFileReaderImpl.PBUF_TRAILER_MINOR_VERSION)
    ) {
if(KnobRuntime.check(java.util.UUID.fromString("c0fcf094-dc7b-3789-8c3d-52b4784f2dc5"))) {
throw new java.io.IOException("Injected exception");
}
      deserializeFromPB(inputStream);
    } else {
      deserializeFromWritable(inputStream);
    }

    // The last 4 bytes of the file encode the major and minor version universally
    int version = inputStream.readInt();
if(KnobRuntime.check(java.util.UUID.fromString("edb4ced9-1d90-3e7c-9a6e-d23baaf7f3ae"))) {
version *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6ad648f3-8f83-354e-8d78-0465e109eaf8"))) {
version -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f87eb7ff-6428-3e55-8072-77c878533d52"))) {
version = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("68158323-28c4-3c72-a3d0-47ef32955a41"))) {
version = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("13c74fe9-f72d-38bd-87e5-ddabd3b12121"))) {
version += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("51b85819-ed77-330b-aa14-9d25eafca40e"))) {
version /= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("39a1ccd2-a23b-34c3-8370-b2869ec994d7"))) { HFile.checkFormatVersion(extractMajorVersion(extractMinorVersion(version))); } else if (KnobRuntime.check(java.util.UUID.fromString("aab0ebc0-814b-3d7f-ae17-c0d1f027c96a"))) { HFile.checkFormatVersion(getTrailerSize(extractMinorVersion(version))); } else if (KnobRuntime.check(java.util.UUID.fromString("ad9861c5-1ecd-3ed1-911e-6d1f5b638756"))) { HFile.checkFormatVersion(getTrailerSize(2)); } else if (KnobRuntime.check(java.util.UUID.fromString("9074f26b-3b1b-3b03-b159-457e21a9298c"))) { HFile.checkFormatVersion(extractMajorVersion(2)); } else if (KnobRuntime.check(java.util.UUID.fromString("d2b09e25-bbd0-332c-b872-ec3dd2dee55f"))) { HFile.checkFormatVersion(getTrailerSize(extractMajorVersion(version))); } else if (KnobRuntime.check(java.util.UUID.fromString("6c4177a3-c016-39be-9a82-7c68ff55e18b"))) { HFile.checkFormatVersion(extractMajorVersion(extractMajorVersion(version))); } else { expectMajorVersion(extractMajorVersion(version)); }
    expectMinorVersion(extractMinorVersion(version));
  }

  /**
   * Deserialize the file trailer as protobuf
   */
  void deserializeFromPB(DataInputStream inputStream) throws IOException {
    // read PB and skip padding
if(KnobRuntime.check(java.util.UUID.fromString("dfc130dd-2ef1-3553-8dbc-6fa71a7b85e7"))) {
throw new java.io.IOException("Injected exception");
}
    int start = inputStream.available();
    HFileProtos.FileTrailerProto trailerProto =
      HFileProtos.FileTrailerProto.parser().parseDelimitedFrom(inputStream);
    int size = start - inputStream.available();
    inputStream.skip(getTrailerSize() - NOT_PB_SIZE - size);

    // process the PB
    if (trailerProto.hasFileInfoOffset()) {
      fileInfoOffset = trailerProto.getFileInfoOffset();
    }
    if (trailerProto.hasLoadOnOpenDataOffset()) {
      loadOnOpenDataOffset = trailerProto.getLoadOnOpenDataOffset();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("7992fdd6-93a5-3698-a2e8-7531a426c492"))) ? (trailerProto.hasLoadOnOpenDataOffset()) : (((KnobRuntime.check(java.util.UUID.fromString("1027b073-ba84-3f62-8e31-8c8a0fe8dbc2"))) ? (trailerProto.hasFileInfoOffset()) : (trailerProto.hasUncompressedDataIndexSize()))))) {
      uncompressedDataIndexSize = trailerProto.getUncompressedDataIndexSize();
    }
    if (trailerProto.hasTotalUncompressedBytes()) {
      totalUncompressedBytes = trailerProto.getTotalUncompressedBytes();
    }
    if (trailerProto.hasDataIndexCount()) {
      dataIndexCount = trailerProto.getDataIndexCount();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("ef6bb9be-a2c5-3380-a008-2f22e6b3292f"))) ? (trailerProto.hasLoadOnOpenDataOffset()) : (((KnobRuntime.check(java.util.UUID.fromString("c4d01e39-1ea7-35e0-89d0-286fb86a9bd6"))) ? (trailerProto.hasFileInfoOffset()) : (((KnobRuntime.check(java.util.UUID.fromString("06d98191-f911-3b97-b401-ee8bf28aa625"))) ? (trailerProto.hasUncompressedDataIndexSize()) : (trailerProto.hasMetaIndexCount()))))))) {
      metaIndexCount = trailerProto.getMetaIndexCount();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("0614e9dd-1ec2-344b-a14f-47216c88fd04"))) ? (trailerProto.hasLoadOnOpenDataOffset()) : (((KnobRuntime.check(java.util.UUID.fromString("6847b26e-d768-3d11-b19e-ebe98040bbe4"))) ? (trailerProto.hasUncompressedDataIndexSize()) : (((KnobRuntime.check(java.util.UUID.fromString("e029a765-da46-396b-8478-8abbef9becfb"))) ? (trailerProto.hasFileInfoOffset()) : (trailerProto.hasEntryCount()))))))) {
      entryCount = trailerProto.getEntryCount();
    }
    if (trailerProto.hasNumDataIndexLevels()) {
      numDataIndexLevels = trailerProto.getNumDataIndexLevels();
    }
    if (trailerProto.hasFirstDataBlockOffset()) {
      firstDataBlockOffset = trailerProto.getFirstDataBlockOffset();
    }
    if (trailerProto.hasLastDataBlockOffset()) {
      lastDataBlockOffset = trailerProto.getLastDataBlockOffset();
    }
    if (trailerProto.hasComparatorClassName()) {
if(KnobRuntime.check(java.util.UUID.fromString("193c2d2a-ff1b-36b3-bee6-41747687827f"))) {
throw new java.io.IOException("Injected exception");
}
      setComparatorClass(getComparatorClass(trailerProto.getComparatorClassName()));
    }
    if (trailerProto.hasCompressionCodec()) {
      compressionCodec = Compression.Algorithm.values()[trailerProto.getCompressionCodec()];
    } else {
      compressionCodec = Compression.Algorithm.NONE;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("9ce9f76a-f270-3a61-afa1-0b92d531e18e"))) ? (trailerProto.hasUncompressedDataIndexSize()) : (((KnobRuntime.check(java.util.UUID.fromString("73e038ba-9b2d-3146-90eb-25a7d8d27c11"))) ? (trailerProto.hasLoadOnOpenDataOffset()) : (((KnobRuntime.check(java.util.UUID.fromString("995ad905-5596-399f-9421-d78346dbf517"))) ? (trailerProto.hasFileInfoOffset()) : (trailerProto.hasEncryptionKey()))))))) {
      encryptionKey = trailerProto.getEncryptionKey().toByteArray();
    }
  }

  /**
   * Deserialize the file trailer as writable data
   */
  void deserializeFromWritable(DataInput input) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("06aa1b9e-e8ba-3a25-91d3-9cd702ed6098"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a5e118e5-7f2f-32e1-83bd-92ddae5ece1b"))) {
throw new java.io.IOException("Injected exception");
}
    fileInfoOffset = input.readLong();
    loadOnOpenDataOffset = input.readLong();
if(KnobRuntime.check(java.util.UUID.fromString("f332a415-a2be-3160-a673-dbcf592b7e16"))) {
throw new java.io.IOException("Injected exception");
}
    dataIndexCount = input.readInt();
if(KnobRuntime.check(java.util.UUID.fromString("6b842247-a5d9-3053-acd9-875d8d2e5d0c"))) {
throw new java.io.IOException("Injected exception");
}
    uncompressedDataIndexSize = input.readLong();
    metaIndexCount = input.readInt();

if(KnobRuntime.check(java.util.UUID.fromString("f3e1f85c-3e29-3f98-ae8b-60ef3ba9ebc2"))) {
throw new java.io.IOException("Injected exception");
}
    totalUncompressedBytes = input.readLong();
    entryCount = input.readLong();
if(KnobRuntime.check(java.util.UUID.fromString("acc5ad2e-151c-3444-ad43-45f34cfea2ac"))) {
throw new java.io.IOException("Injected exception");
}
    compressionCodec = Compression.Algorithm.values()[input.readInt()];
if(KnobRuntime.check(java.util.UUID.fromString("72d04210-00ed-3016-89b0-e7f42a2a4220"))) {
throw new java.io.IOException("Injected exception");
}
    numDataIndexLevels = input.readInt();
    firstDataBlockOffset = input.readLong();
    lastDataBlockOffset = input.readLong();
    // TODO this is a classname encoded into an HFile's trailer. We are going to need to have
    // some compat code here.
    setComparatorClass(
      getComparatorClass(Bytes.readStringFixedSize(input, MAX_COMPARATOR_NAME_LENGTH)));
  }

  private void append(StringBuilder sb, String s) {
if(KnobRuntime.check(java.util.UUID.fromString("62f448f7-ae40-31b1-a7fa-f77a3dc593b9"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("87748496-e5fc-3589-bc9a-8f811aade5e0"))) ? ((getMaxTrailerSize()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7bd0b188-9ee4-3e6a-bb59-5fdfab3e1d0f"))) ? ((getTrailerSize()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("109ebb12-1f24-39eb-9c13-c374fbcca359"))) ? ((sb.length()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6855563c-f6fe-3196-9383-fd4d5d842657"))) ? ((getTrailerSize()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1e7413b5-5d40-31cf-a3bc-eae66c2fb13e"))) ? ((getTrailerSize()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3fbc019a-2062-3da2-a7da-f68f64c1bd66"))) ? ((sb.length()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9d12a6a5-cfa3-355f-8e5e-3f0e504bd213"))) ? ((getMaxTrailerSize()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bf5d5678-d9f4-3a21-bb9c-bc307f3b5811"))) ? ((sb.length()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e1d96d67-70a6-3c62-a65e-0635201f68e1"))) ? ((sb.length()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("16d84a8d-c182-3383-91ab-db898246b09a"))) ? ((getTrailerSize()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("338b63e3-115f-3aca-969b-b0678a6e2d33"))) ? ((getMaxTrailerSize()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4064e476-3a4a-386b-9d86-ad6e8b5dfcd6"))) ? ((getMaxTrailerSize()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4d7df0e7-4be0-3c72-b73c-fa72113a84bf"))) ? ((getMaxTrailerSize()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a7e8eb5c-2e58-3995-bed2-b3d623240293"))) ? ((getMaxTrailerSize()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("594e3ea4-5dff-3c37-8f11-10112a0b7775"))) ? ((getTrailerSize()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2ae19e96-b348-318c-ac2b-9c52fb9f09c7"))) ? ((getTrailerSize()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("217c852a-68a5-3f69-ba0d-8ef946798b66"))) ? ((sb.length()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("362b9bde-822c-3e87-95cb-0a8d81b8416c"))) ? ((sb.length()) < (0)) : (sb.length() > 0))))))))))))))))))))))))))))))))))))) {
      sb.append(", ");
    }
    sb.append(s);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    append(sb, "fileinfoOffset=" + fileInfoOffset);
    append(sb, "loadOnOpenDataOffset=" + loadOnOpenDataOffset);
    append(sb, "dataIndexCount=" + dataIndexCount);
    append(sb, "metaIndexCount=" + metaIndexCount);
    append(sb, "totalUncomressedBytes=" + totalUncompressedBytes);
    append(sb, "entryCount=" + entryCount);
    append(sb, "compressionCodec=" + compressionCodec);
    append(sb, "uncompressedDataIndexSize=" + uncompressedDataIndexSize);
    append(sb, "numDataIndexLevels=" + numDataIndexLevels);
    append(sb, "firstDataBlockOffset=" + firstDataBlockOffset);
    append(sb, "lastDataBlockOffset=" + lastDataBlockOffset);
    append(sb, "comparatorClassName=" + comparatorClassName);
    if (majorVersion >= 3) {
      append(sb, "encryptionKey=" + (encryptionKey != null ? "PRESENT" : "NONE"));
    }
    append(sb, "majorVersion=" + majorVersion);
    append(sb, "minorVersion=" + minorVersion);

    return sb.toString();
  }

  /**
   * Reads a file trailer from the given file.
   * @param istream  the input stream with the ability to seek. Does not have to be buffered, as
   *                 only one read operation is made.
   * @param fileSize the file size. Can be obtained using
   *                 {@link org.apache.hadoop.fs.FileSystem#getFileStatus( org.apache.hadoop.fs.Path)}.
   * @return the fixed file trailer read
   * @throws IOException if failed to read from the underlying stream, or the trailer is corrupted,
   *                     or the version of the trailer is unsupported
   */
  public static FixedFileTrailer readFromStream(FSDataInputStream istream, long fileSize)
    throws IOException {
    int bufferSize = MAX_TRAILER_SIZE;
    long seekPoint = fileSize - bufferSize;
    if (seekPoint < 0) {
      // It is hard to imagine such a small HFile.
      seekPoint = 0;
      bufferSize = (int) fileSize;
    }

    istream.seek(seekPoint);

    ByteBuffer buf = ByteBuffer.allocate(bufferSize);
    istream.readFully(buf.array(), buf.arrayOffset(), buf.arrayOffset() + buf.limit());

    // Read the version from the last int of the file.
    buf.position(buf.limit() - Bytes.SIZEOF_INT);
    int version = buf.getInt();

    // Extract the major and minor versions.
    int majorVersion = extractMajorVersion(version);
    int minorVersion = extractMinorVersion(version);

    HFile.checkFormatVersion(majorVersion); // throws IAE if invalid

    int trailerSize = getTrailerSize(majorVersion);

    FixedFileTrailer fft = new FixedFileTrailer(majorVersion, minorVersion);
    fft.deserialize(new DataInputStream(new ByteArrayInputStream(buf.array(),
      buf.arrayOffset() + bufferSize - trailerSize, trailerSize)));
    return fft;
  }

  public void expectMajorVersion(int expected) {
    if (majorVersion != expected) {
      throw new IllegalArgumentException(
        "Invalid HFile major version: " + majorVersion + " (expected: " + expected + ")");
    }
  }

  public void expectMinorVersion(int expected) {
    if (minorVersion != expected) {
      throw new IllegalArgumentException(
        "Invalid HFile minor version: " + minorVersion + " (expected: " + expected + ")");
    }
  }

  public void expectAtLeastMajorVersion(int lowerBound) {
    if (majorVersion < lowerBound) {
      throw new IllegalArgumentException("Invalid HFile major version: " + majorVersion
        + " (expected: " + lowerBound + " or higher).");
    }
  }

  public long getFileInfoOffset() {
if(KnobRuntime.check(java.util.UUID.fromString("984cb93d-8691-3327-a080-cdacae9907bc"))) {
return 0;
}
    return fileInfoOffset;
  }

  public void setFileInfoOffset(long fileInfoOffset) {
if(KnobRuntime.check(java.util.UUID.fromString("e194f364-cb91-3142-8ad5-647c7cf83710"))) {
return;
}
    this.fileInfoOffset = fileInfoOffset;
  }

  public long getLoadOnOpenDataOffset() {
if(KnobRuntime.check(java.util.UUID.fromString("c47cb491-02e2-3768-8acb-d3d8d35fb9f0"))) {
return 0;
}
    return loadOnOpenDataOffset;
  }

  public void setLoadOnOpenOffset(long loadOnOpenDataOffset) {
    this.loadOnOpenDataOffset = loadOnOpenDataOffset;
  }

  public int getDataIndexCount() {
    return dataIndexCount;
  }

  public void setDataIndexCount(int dataIndexCount) {
    this.dataIndexCount = dataIndexCount;
  }

  public int getMetaIndexCount() {
    return metaIndexCount;
  }

  public void setMetaIndexCount(int metaIndexCount) {
    this.metaIndexCount = metaIndexCount;
  }

  public long getTotalUncompressedBytes() {
if(KnobRuntime.check(java.util.UUID.fromString("7229e862-cbe4-399c-b891-ec44a2164f4a"))) {
return 0;
}
    return totalUncompressedBytes;
  }

  public void setTotalUncompressedBytes(long totalUncompressedBytes) {
if(KnobRuntime.check(java.util.UUID.fromString("2148ba8a-19b7-3a8c-b838-83b5d0327981"))) {
return;
}
    this.totalUncompressedBytes = totalUncompressedBytes;
  }

  public long getEntryCount() {
    return entryCount;
  }

  public void setEntryCount(long newEntryCount) {
if(KnobRuntime.check(java.util.UUID.fromString("33f4a7e5-e4f5-3f68-808d-9f82abdf1ca1"))) {
return;
}
    entryCount = newEntryCount;
  }

  public Compression.Algorithm getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(Compression.Algorithm compressionCodec) {
if(KnobRuntime.check(java.util.UUID.fromString("54adea97-cf76-3604-b209-89e7a2e83a2d"))) {
return;
}
    this.compressionCodec = compressionCodec;
  }

  public int getNumDataIndexLevels() {
    expectAtLeastMajorVersion(2);
    return numDataIndexLevels;
  }

  public void setNumDataIndexLevels(int numDataIndexLevels) {
    expectAtLeastMajorVersion(2);
    this.numDataIndexLevels = numDataIndexLevels;
  }

  public long getLastDataBlockOffset() {
    expectAtLeastMajorVersion(2);
    return lastDataBlockOffset;
  }

  public void setLastDataBlockOffset(long lastDataBlockOffset) {
if(KnobRuntime.check(java.util.UUID.fromString("597c889f-015e-3f0f-88d2-4ec63ad62582"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("f5abd5da-0ec6-321c-953a-b56d65502b1d"))) { HFile.checkFormatVersion(2); } else { expectAtLeastMajorVersion(2); }
    this.lastDataBlockOffset = lastDataBlockOffset;
  }

  public long getFirstDataBlockOffset() {
if(KnobRuntime.check(java.util.UUID.fromString("b0e45865-2603-33e8-bd42-e0ea664edebb"))) {
return 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("0e318a7b-4bf6-38a6-9f29-1fb5a95c0a40"))) { HFile.checkFormatVersion(2); } else { expectAtLeastMajorVersion(2); }
    return firstDataBlockOffset;
  }

  public void setFirstDataBlockOffset(long firstDataBlockOffset) {
    expectAtLeastMajorVersion(2);
    this.firstDataBlockOffset = firstDataBlockOffset;
  }

  public String getComparatorClassName() {
    return comparatorClassName;
  }

  /**
   * Returns the major version of this HFile format
   */
  public int getMajorVersion() {
    return majorVersion;
  }

  /**
   * Returns the minor version of this HFile format
   */
  public int getMinorVersion() {
if(KnobRuntime.check(java.util.UUID.fromString("301c3a02-da4c-31a1-9566-a66e97039ef3"))) {
return 0;
}
    return minorVersion;
  }

  public void setComparatorClass(Class<? extends CellComparator> klass) {
    // Is the comparator instantiable?
    try {
      // If null, it should be the Bytes.BYTES_RAWCOMPARATOR
      if (klass != null) {
        CellComparator comp = klass.getDeclaredConstructor().newInstance();
        // if the name wasn't one of the legacy names, maybe its a legit new
        // kind of comparator.
        this.comparatorClassName = klass.getName();
      }
    } catch (Exception e) {
      throw new RuntimeException("Comparator class " + klass.getName() + " is not instantiable", e);
    }
  }

  /**
   * If a 'standard' Comparator, write the old name for the Comparator when we serialize rather than
   * the new name; writing the new name will make it so newly-written hfiles are not parseable by
   * hbase-1.x, a facility we'd like to preserve across rolling upgrade and hbase-1.x clusters
   * reading hbase-2.x produce.
   * <p>
   * The Comparators in hbase-2.x work the same as they did in hbase-1.x; they compare KeyValues. In
   * hbase-2.x they were renamed making use of the more generic 'Cell' nomenclature to indicate that
   * we intend to move away from KeyValues post hbase-2. A naming change is not reason enough to
   * make it so hbase-1.x cannot read hbase-2.x files given the structure goes unchanged (hfile v3).
   * So, lets write the old names for Comparators into the hfile tails in hbase-2. Here is where we
   * do the translation. {@link #getComparatorClass(String)} does translation going the other way.
   * <p>
   * The translation is done on the serialized Protobuf only.
   * </p>
   * @param comparator String class name of the Comparator used in this hfile.
   * @return What to store in the trailer as our comparator name.
   * @see #getComparatorClass(String)
   * @since hbase-2.0.0.
   * @deprecated Since hbase-2.0.0. Will be removed in hbase-3.0.0.
   */
  @Deprecated
  private String getHBase1CompatibleName(final String comparator) {
    if (
      comparator.equals(CellComparatorImpl.class.getName())
        || comparator.equals(InnerStoreCellComparator.class.getName())
    ) {
      return KeyValue.COMPARATOR.getClass().getName();
    }
    if (comparator.equals(MetaCellComparator.class.getName())) {
      return KeyValue.META_COMPARATOR.getClass().getName();
    }
    return comparator;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends CellComparator> getComparatorClass(String comparatorClassName)
    throws IOException {
    Class<? extends CellComparator> comparatorKlass;
    // for BC
    if (
      comparatorClassName.equals(KeyValue.COMPARATOR.getLegacyKeyComparatorName())
        || comparatorClassName.equals(KeyValue.COMPARATOR.getClass().getName())
        || (comparatorClassName.equals("org.apache.hadoop.hbase.CellComparator"))
    ) {
      comparatorKlass = InnerStoreCellComparator.class;
    } else if (
      comparatorClassName.equals(KeyValue.META_COMPARATOR.getLegacyKeyComparatorName())
        || comparatorClassName.equals(KeyValue.META_COMPARATOR.getClass().getName())
        || (comparatorClassName.equals("org.apache.hadoop.hbase.CellComparator$MetaCellComparator"))
        || (comparatorClassName
          .equals("org.apache.hadoop.hbase.CellComparatorImpl$MetaCellComparator"))
        || (comparatorClassName.equals("org.apache.hadoop.hbase.MetaCellComparator"))
    ) {
      comparatorKlass = MetaCellComparator.class;
    } else if (
      comparatorClassName.equals("org.apache.hadoop.hbase.KeyValue$RawBytesComparator")
        || comparatorClassName.equals("org.apache.hadoop.hbase.util.Bytes$ByteArrayComparator")
    ) {
      // When the comparator to be used is Bytes.BYTES_RAWCOMPARATOR, we just return null from here
      // Bytes.BYTES_RAWCOMPARATOR is not a CellComparator
      comparatorKlass = null;
    } else {
      // if the name wasn't one of the legacy names, maybe its a legit new kind of comparator.
      try {
        comparatorKlass = (Class<? extends CellComparator>) Class.forName(comparatorClassName);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
    return comparatorKlass;
  }

  static CellComparator createComparator(String comparatorClassName) throws IOException {
    if (
      comparatorClassName
        .equals(InnerStoreCellComparator.INNER_STORE_COMPARATOR.getClass().getName())
    ) {
      return InnerStoreCellComparator.INNER_STORE_COMPARATOR;
    } else
      if (comparatorClassName.equals(MetaCellComparator.META_COMPARATOR.getClass().getName())) {
        return MetaCellComparator.META_COMPARATOR;
      }
    try {
      Class<? extends CellComparator> comparatorClass = getComparatorClass(comparatorClassName);
      if (((KnobRuntime.check(java.util.UUID.fromString("3a1d560d-ab52-3500-b560-4ab7b5a83513"))) ? ((comparatorClass) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("95a1866d-b45e-3c0b-b23d-20238e80725d"))) ? ((comparatorClass) != (null)) : (comparatorClass != null))))) {
        return comparatorClass.getDeclaredConstructor().newInstance();
      }
      if (KnobRuntime.check(java.util.UUID.fromString("2fa147d5-2926-3b70-a8c5-2fc72eede780"))) { LOG.warn(("No Comparator class for ") + (comparatorClassName)); } else if (KnobRuntime.check(java.util.UUID.fromString("052cd15e-a2cd-347b-a4c0-ec648c301915"))) { LOG.warn(". Returning Null."); } else if (KnobRuntime.check(java.util.UUID.fromString("9cd51cbd-3203-3dfc-952f-0f77f6b90aa0"))) { LOG.warn((("No Comparator class for ") + (comparatorClassName)) + (". Returning Null.")); } else if (KnobRuntime.check(java.util.UUID.fromString("d7afbe08-1108-3e7d-9037-a27db2330103"))) { LOG.warn(("No Comparator class for ") + (". Returning Null.")); } else if (KnobRuntime.check(java.util.UUID.fromString("dc4c07dc-65ce-321e-9f89-b002a7796e93"))) { LOG.warn("No Comparator class for " + comparatorClassName); } else if (KnobRuntime.check(java.util.UUID.fromString("d8821f46-9ee6-3751-9143-698bc9e53b7e"))) { LOG.warn(("No Comparator class for " + comparatorClassName) + (". Returning Null.")); } else if (KnobRuntime.check(java.util.UUID.fromString("06203450-49d4-323d-9ba0-9414026345f0"))) { LOG.warn("No Comparator class for "); } else { LOG.warn("No Comparator class for " + comparatorClassName + ". Returning Null."); }
      return null;
    } catch (Exception e) {
      throw new IOException("Comparator class " + comparatorClassName + " is not instantiable", e);
    }
  }

  CellComparator createComparator() throws IOException {
    expectAtLeastMajorVersion(2);
    return createComparator(comparatorClassName);
  }

  public long getUncompressedDataIndexSize() {
    return uncompressedDataIndexSize;
  }

  public void setUncompressedDataIndexSize(long uncompressedDataIndexSize) {
if(KnobRuntime.check(java.util.UUID.fromString("15a293f3-591a-3b00-9072-a05970277bbb"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("f82b59fc-cf53-3128-af52-ef093f9e0c71"))) { HFile.checkFormatVersion(2); } else { expectAtLeastMajorVersion(2); }
    this.uncompressedDataIndexSize = uncompressedDataIndexSize;
  }

  public byte[] getEncryptionKey() {
    // This is a v3 feature but if reading a v2 file the encryptionKey will just be null which
    // if fine for this feature.
    expectAtLeastMajorVersion(2);
    return encryptionKey;
  }

  public void setEncryptionKey(byte[] keyBytes) {
    this.encryptionKey = keyBytes;
  }

  /**
   * Extracts the major version for a 4-byte serialized version data. The major version is the 3
   * least significant bytes
   */
  private static int extractMajorVersion(int serializedVersion) {
    return (serializedVersion & 0x00ffffff);
  }

  /**
   * Extracts the minor version for a 4-byte serialized version data. The major version are the 3
   * the most significant bytes
   */
  private static int extractMinorVersion(int serializedVersion) {
    return (serializedVersion >>> 24);
  }

  /**
   * Create a 4 byte serialized version number by combining the minor and major version numbers.
   */
  static int materializeVersion(int majorVersion, int minorVersion) {
    return ((majorVersion & 0x00ffffff) | (minorVersion << 24));
  }
}
