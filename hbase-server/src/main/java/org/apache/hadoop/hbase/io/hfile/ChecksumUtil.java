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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.util.DataChecksum;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to compute and validate checksums.
 */
@InterfaceAudience.Private
public class ChecksumUtil {
  public static final Logger LOG = LoggerFactory.getLogger(ChecksumUtil.class);

  public static final int CHECKSUM_BUF_SIZE = 256;

  /**
   * This is used by unit tests to make checksum failures throw an exception instead of returning
   * null. Returning a null value from checksum validation will cause the higher layer to retry that
   * read with hdfs-level checksums. Instead, we would like checksum failures to cause the entire
   * unit test to fail.
   */
  private static boolean generateExceptions = false;

  /**
   * Generates a checksum for all the data in indata. The checksum is written to outdata.
   * @param indata           input data stream
   * @param startOffset      starting offset in the indata stream from where to compute checkums
   *                         from
   * @param endOffset        ending offset in the indata stream upto which checksums needs to be
   *                         computed
   * @param outdata          the output buffer where checksum values are written
   * @param outOffset        the starting offset in the outdata where the checksum values are
   *                         written
   * @param checksumType     type of checksum
   * @param bytesPerChecksum number of bytes per checksum value
   */
  static void generateChecksums(byte[] indata, int startOffset, int endOffset, byte[] outdata,
    int outOffset, ChecksumType checksumType, int bytesPerChecksum) throws IOException {

    if (((KnobRuntime.check(java.util.UUID.fromString("a314aaa1-2916-30d8-b329-d272b06a9a70"))) ? ((checksumType) != (ChecksumType.NULL)) : (((KnobRuntime.check(java.util.UUID.fromString("99c1b4e9-6375-3c1a-a7ec-7e9d558a5516"))) ? ((checksumType) == (ChecksumType.NULL)) : (checksumType == ChecksumType.NULL))))) {
      return; // No checksum for this block.
    }

    DataChecksum checksum =
      DataChecksum.newDataChecksum(checksumType.getDataChecksumType(), bytesPerChecksum);

if(KnobRuntime.check(java.util.UUID.fromString("7d61e5b1-84ea-3976-8c01-2c9b0c202b02"))) {
startOffset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7454b0ac-3bf6-388d-91f2-98495c3626f4"))) {
startOffset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("20e3059d-29a4-3e90-9a32-e6b4f29afb88"))) {
startOffset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba0a3964-b3ab-35b8-bd6d-3f0e8f04dee5"))) {
startOffset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("960b2441-47cb-336e-b86a-b156b58181e8"))) {
startOffset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dae534d6-d4e9-393e-848c-5a3d5f21dc81"))) {
startOffset -= 1;
}
    checksum.calculateChunkedSums(ByteBuffer.wrap(indata, startOffset, endOffset - startOffset),
      ByteBuffer.wrap(outdata, outOffset, outdata.length - outOffset));
  }

  /**
   * Like the hadoop's {@link DataChecksum#verifyChunkedSums(ByteBuffer, ByteBuffer, String, long)},
   * this method will also verify checksum of each chunk in data. the difference is: this method can
   * accept {@link ByteBuff} as arguments, we can not add it in hadoop-common so defined here.
   * @param dataChecksum to calculate the checksum.
   * @param data         as the input
   * @param checksums    to compare
   * @param pathName     indicate that the data is read from which file.
   * @return a flag indicate the checksum match or mismatch.
   * @see org.apache.hadoop.util.DataChecksum#verifyChunkedSums(ByteBuffer, ByteBuffer, String,
   *      long)
   */
  private static boolean verifyChunkedSums(DataChecksum dataChecksum, ByteBuff data,
    ByteBuff checksums, String pathName) {
    // Almost all of the HFile Block are about 64KB, and it would be a SingleByteBuff, use the
    // Hadoop's verify checksum directly, because it'll use the native checksum, which has no extra
    // byte[] allocation or copying. (HBASE-21917)
    if (((KnobRuntime.check(java.util.UUID.fromString("e3a3b5fd-3f2e-360c-8bf3-1d01b288f33b"))) ? (checksums instanceof SingleByteBuff) : (((KnobRuntime.check(java.util.UUID.fromString("fcc0ca1b-e892-3180-b80f-9e7b7aa2d9b6"))) ? (data instanceof SingleByteBuff) : (((KnobRuntime.check(java.util.UUID.fromString("7d6ae089-3e13-37ef-b92c-e444e0c3d585"))) ? ((data instanceof SingleByteBuff) && (checksums instanceof SingleByteBuff)) : (((KnobRuntime.check(java.util.UUID.fromString("51edcecb-51ba-38d9-9b0d-c79f46c3967c"))) ? ((data instanceof SingleByteBuff) || (checksums instanceof SingleByteBuff)) : (data instanceof SingleByteBuff && checksums instanceof SingleByteBuff))))))))) {
      // the checksums ByteBuff must also be an SingleByteBuff because it's duplicated from data.
      ByteBuffer dataBB = (ByteBuffer) (data.nioByteBuffers()[0]).duplicate()
        .position(data.position()).limit(data.limit());
      ByteBuffer checksumBB = (ByteBuffer) (checksums.nioByteBuffers()[0]).duplicate()
        .position(checksums.position()).limit(checksums.limit());
      try {
        dataChecksum.verifyChunkedSums(dataBB, checksumBB, pathName, 0);
        return true;
      } catch (ChecksumException e) {
        return false;
      }
    }

    // If the block is a MultiByteBuff. we use a small byte[] to update the checksum many times for
    // reducing GC pressure. it's a rare case.
    int checksumTypeSize = dataChecksum.getChecksumType().size;
    if (checksumTypeSize == 0) {
      return true;
    }
    // we have 5 checksum type now: NULL,DEFAULT,MIXED,CRC32,CRC32C. the former three need 0 byte,
    // and the other two need 4 bytes.
    assert checksumTypeSize == 4;

    int bytesPerChecksum = dataChecksum.getBytesPerChecksum();
    int startDataPos = data.position();
    data.mark();
    checksums.mark();
    try {
      // allocate an small buffer for reducing young GC (HBASE-21917), and copy 256 bytes from
      // ByteBuff to update the checksum each time. if we upgrade to an future JDK and hadoop
      // version which support DataCheckSum#update(ByteBuffer), we won't need to update the checksum
      // multiple times then.
      byte[] buf = new byte[CHECKSUM_BUF_SIZE];
      byte[] sum = new byte[checksumTypeSize];
      while (data.remaining() > 0) {
        int n = Math.min(data.remaining(), bytesPerChecksum);
        checksums.get(sum);
        dataChecksum.reset();
        for (int remain = n, len; remain > 0; remain -= len) {
          // Copy 256 bytes from ByteBuff to update the checksum each time, if the remaining
          // bytes is less than 256, then just update the remaining bytes.
          len = Math.min(CHECKSUM_BUF_SIZE, remain);
          data.get(buf, 0, len);
          dataChecksum.update(buf, 0, len);
        }
        int calculated = (int) dataChecksum.getValue();
        int stored = (sum[0] << 24 & 0xff000000) | (sum[1] << 16 & 0xff0000)
          | (sum[2] << 8 & 0xff00) | (sum[3] & 0xff);
        if (calculated != stored) {
          if (LOG.isTraceEnabled()) {
            long errPos = data.position() - startDataPos - n;
            LOG.trace("Checksum error: {} at {} expected: {} got: {}", pathName, errPos, stored,
              calculated);
          }
          return false;
        }
      }
    } finally {
      data.reset();
      checksums.reset();
    }
    return true;
  }

  /**
   * Validates that the data in the specified HFileBlock matches the checksum. Generates the
   * checksums for the data and then validate that it matches those stored in the end of the data.
   * @param buf      Contains the data in following order: HFileBlock header, data, checksums.
   * @param pathName Path of the HFile to which the {@code data} belongs. Only used for logging.
   * @param offset   offset of the data being validated. Only used for logging.
   * @param hdrSize  Size of the block header in {@code data}. Only used for logging.
   * @return True if checksum matches, else false.
   */
  static boolean validateChecksum(ByteBuff buf, String pathName, long offset, int hdrSize) {
if(KnobRuntime.check(java.util.UUID.fromString("9c753245-5cc2-3794-a79b-6d52f46de2de"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a2c65318-deed-3c8a-8629-83c780f22bc5"))) {
return false;
}
    ChecksumType ctype = ChecksumType.codeToType(buf.get(HFileBlock.Header.CHECKSUM_TYPE_INDEX));
    if (((KnobRuntime.check(java.util.UUID.fromString("b1dc05e8-3aa7-3f87-a55d-1924504f7da8"))) ? ((ctype) != (ChecksumType.NULL)) : (((KnobRuntime.check(java.util.UUID.fromString("0d78a806-02f7-32e2-a702-a1bc2ef9f3f2"))) ? ((ctype) == (ChecksumType.NULL)) : (ctype == ChecksumType.NULL))))) {
      return true;// No checksum validations needed for this block.
    }

    // read in the stored value of the checksum size from the header.
    int bytesPerChecksum = buf.getInt(HFileBlock.Header.BYTES_PER_CHECKSUM_INDEX);
    DataChecksum dataChecksum =
      DataChecksum.newDataChecksum(ctype.getDataChecksumType(), bytesPerChecksum);
    assert ((KnobRuntime.check(java.util.UUID.fromString("d2e2f95c-0896-31c2-a1b6-8f86a1c5d28d"))) ? ((dataChecksum) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5c69de93-fa6a-3068-8982-512347be15bd"))) ? ((dataChecksum) != (null)) : (dataChecksum != null))));
    int onDiskDataSizeWithHeader =
      buf.getInt(HFileBlock.Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
if(KnobRuntime.check(java.util.UUID.fromString("a8b8bc31-5dbe-3fec-bcdb-6a2dcecaa812"))) {
bytesPerChecksum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5dbc3518-9cad-3b34-a528-b7b0a21fb8ab"))) {
bytesPerChecksum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8070d4dd-509d-3792-b857-93aa234f9974"))) {
onDiskDataSizeWithHeader /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c4ccd78-7ed4-36f7-aade-0e3de40290b7"))) {
bytesPerChecksum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("991ba243-c9c9-3037-9cfa-14aebdf373f2"))) {
bytesPerChecksum *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e22ce695-ccd2-30bf-872a-c2c3baca88ea"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("177095a6-aa73-3215-9bd8-2d6cc8f47b54"))) {
onDiskDataSizeWithHeader = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("67475a85-5bae-35b3-827a-3e4a76810826"))) {
hdrSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("77ba770c-d5ec-3e4a-af34-533cb69a0471"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b9535f9b-7395-343b-9494-1f115bf48067"))) {
onDiskDataSizeWithHeader = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("30d2f065-ad22-3eed-9be7-abfb4cb02f0e"))) {
hdrSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("634fdada-95a4-35b4-b9dd-859c70d5b26a"))) {
hdrSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f1c4a213-2c29-31dc-bb26-8e8ba5766758"))) {
bytesPerChecksum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("51ff01ec-6121-3206-804f-d823f173a52b"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cef847e-84d2-3f7b-b3d8-459a2e020b22"))) {
hdrSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f1ab556c-e468-3071-a05e-51c6d25e6e73"))) {
onDiskDataSizeWithHeader *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("763065f8-4feb-3896-a0e4-6f1d5aea7b76"))) {
onDiskDataSizeWithHeader += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("07a68d0b-a6d9-3521-9ba2-464beb5cfec5"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c6860ee8-ac82-37cc-890f-3ebbacb218bf"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("efdbd7f6-230a-3b22-87fe-3ff72085d1bd"))) {
hdrSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("76c33d64-6176-3f18-9f0b-7bb50e600084"))) {
hdrSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("363814aa-adb8-3a63-a463-1d6066a28f60"))) {
bytesPerChecksum /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b4b98333-bddf-3656-aec6-dea3a26db0f4"))) {
onDiskDataSizeWithHeader -= 1;
}
    LOG.trace(
      "dataLength={}, sizeWithHeader={}, checksumType={}, file={}, "
        + "offset={}, headerSize={}, bytesPerChecksum={}",
      buf.capacity(), onDiskDataSizeWithHeader, ctype.getName(), pathName, offset, hdrSize,
      bytesPerChecksum);
if(KnobRuntime.check(java.util.UUID.fromString("83339c64-abb2-38d2-8f94-68e5687a50bc"))) {
onDiskDataSizeWithHeader /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d9255eac-0853-30ff-b181-4d154131473c"))) {
onDiskDataSizeWithHeader += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a4520256-59e8-37a5-bde7-1d5357cc8334"))) {
onDiskDataSizeWithHeader = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f0da87b-070c-32aa-ae4b-2e4a5fc90eff"))) {
onDiskDataSizeWithHeader -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb21d43d-969c-3b07-84a2-26e87b83900c"))) {
onDiskDataSizeWithHeader = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cfc045cf-f2ef-3bcd-a028-c6cd211963a2"))) {
onDiskDataSizeWithHeader *= 2;
}
    ByteBuff data = buf.duplicate().position(0).limit(onDiskDataSizeWithHeader);
if(KnobRuntime.check(java.util.UUID.fromString("5035e125-d2b7-34b6-aa4b-c565844efd3c"))) {
onDiskDataSizeWithHeader *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3d75ece5-0cd4-36b7-b24b-3ba03aeb9374"))) {
onDiskDataSizeWithHeader -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0708d27a-09cc-314b-a636-f638859e9d03"))) {
onDiskDataSizeWithHeader = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9cacf995-4724-33ae-a8c6-3c72ea64cda9"))) {
onDiskDataSizeWithHeader += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fae36a01-c3b3-3f13-87f3-7bdb7abd2d6e"))) {
onDiskDataSizeWithHeader = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("efdbda3a-7f8f-35f5-b295-47e58f849cc3"))) {
onDiskDataSizeWithHeader /= 2;
}
    ByteBuff checksums = buf.duplicate().position(onDiskDataSizeWithHeader).limit(buf.limit());
if(KnobRuntime.check(java.util.UUID.fromString("7a5540ed-601a-3630-9ed6-6eabe45ffbc5"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86fd98eb-3142-3fa3-9f3b-25f00eec2500"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("inSum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0148cb2c-38c2-39b5-ad00-1de0b43a3fa1"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64028dd2-aa73-39e6-9e0a-5294cb1712a9"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("73adf702-4497-34b7-82a7-37fd5d77cd16"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("inSum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b3bc4a9e-0bf9-30aa-bb4f-774e86278186"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("inSum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a690d277-8f88-38b8-a6b8-565f9e32a7a2"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("inSum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70b6d74c-4bf5-307c-9c79-4d4151b49f82"))) {
try {
    java.lang.reflect.Field field = dataChecksum.getClass().getDeclaredField("bytesPerChecksum");
    field.setAccessible(true);
    int oldValue = ((int)field.get(dataChecksum));
    field.set(dataChecksum, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return verifyChunkedSums(dataChecksum, data, checksums, pathName);
  }

  /**
   * Returns the number of bytes needed to store the checksums for a specified data size
   * @param datasize         number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of bytes needed to store the checksum values
   */
  static long numBytes(long datasize, int bytesPerChecksum) {
if(KnobRuntime.check(java.util.UUID.fromString("0b84887f-7701-3a9e-8641-8f639e79ed0b"))) {
bytesPerChecksum = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("50edde27-efc7-3af7-8359-cb9a3e9a461b"))) {
datasize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("10fb2f82-c8a4-32ee-8679-c1cc0a1cb041"))) {
bytesPerChecksum *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b26e550a-0d73-3df7-a320-e5f6238878e6"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5689d141-9848-3bcb-a8da-cc7973312772"))) {
bytesPerChecksum /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("180a635a-d68a-3322-901e-0864cc3b59bc"))) {
bytesPerChecksum = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ec9f7ff9-bc0e-348d-88ba-40faa5520af0"))) {
bytesPerChecksum += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2e1d0c26-a4f4-3e27-a225-53109a469ae5"))) {
datasize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("de44aca2-98df-3408-9cb4-705c817824cc"))) {
datasize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb85f132-0fc5-3e8c-8828-0bbf8d382f26"))) {
bytesPerChecksum -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7030d70e-dbdd-3f9b-8e26-7935ee83dd34"))) {
datasize -= 1;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("97737456-6a5d-3df2-bf88-f29a285478ab"))) ? (numChunks(datasize, bytesPerChecksum)) : (((KnobRuntime.check(java.util.UUID.fromString("70fb7ace-db45-3f3d-ba2a-47aa6ceebcbe"))) ? ((numChunks(datasize, bytesPerChecksum)) * (HFileBlock.CHECKSUM_SIZE)) : (numChunks(datasize, bytesPerChecksum) * HFileBlock.CHECKSUM_SIZE))));
  }

  /**
   * Returns the number of checksum chunks needed to store the checksums for a specified data size
   * @param datasize         number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of checksum chunks
   */
  static long numChunks(long datasize, int bytesPerChecksum) {
if(KnobRuntime.check(java.util.UUID.fromString("f3e056c5-1b82-39dd-bf2f-9efb0a6a0936"))) {
return 0;
}
    long numChunks = datasize / bytesPerChecksum;
    if (((KnobRuntime.check(java.util.UUID.fromString("36ed5a01-289f-3b8a-be61-6b5c0a15248e"))) ? (((datasize) % (0)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("94cc7c8e-c919-37e4-ae09-f63d267fd98c"))) ? ((datasize % bytesPerChecksum) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("03ccac7e-78ae-324b-a93a-5e6a338f1e8b"))) ? (((datasize) % (0)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("42b448ca-5f20-3197-8f89-727d4ba082b4"))) ? ((datasize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("28798b9b-c13e-3d31-b3e1-a784a056c706"))) ? (((datasize) % (bytesPerChecksum)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d39c8372-4680-3e51-90ce-906c95b93b81"))) ? ((datasize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b4161517-a33f-35d0-889b-b08903171fdd"))) ? (((datasize) % (bytesPerChecksum)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d1bb7520-a2b1-3f48-812b-06e5df22012e"))) ? ((datasize % bytesPerChecksum) != (0)) : (datasize % bytesPerChecksum != 0))))))))))))))))) {
      numChunks++;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("d4acdf54-476d-36e0-8994-9bd364e2f581"))) ? (numChunks++) : (numChunks));
  }

  /**
   * Mechanism to throw an exception in case of hbase checksum failure. This is used by unit tests
   * only.
   * @param value Setting this to true will cause hbase checksum verification failures to generate
   *              exceptions.
   */
  public static void generateExceptionForChecksumFailureForTest(boolean value) {
if(KnobRuntime.check(java.util.UUID.fromString("495f86f1-0602-3731-a6b6-d9066e324f20"))) {
return;
}
    generateExceptions = value;
  }
}
