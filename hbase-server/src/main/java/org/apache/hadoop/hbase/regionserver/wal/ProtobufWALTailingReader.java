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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.io.DelegatingInputStream;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALTailingReader;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * A WAL reader for replication. It supports reset so can be used to tail a WAL file which is being
 * written currently.
 */
@InterfaceAudience.Private
public class ProtobufWALTailingReader extends AbstractProtobufWALReader
  implements WALTailingReader {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWALTailingReader.class);

  private DelegatingInputStream delegatingInput;

  private static final class ReadWALKeyResult {
    final State state;
    final Entry entry;
    final int followingKvCount;

    public ReadWALKeyResult(State state, Entry entry, int followingKvCount) {
      this.state = state;
      this.entry = entry;
      this.followingKvCount = followingKvCount;
    }
  }

  private static final ReadWALKeyResult KEY_ERROR_AND_RESET =
    new ReadWALKeyResult(State.ERROR_AND_RESET, null, 0);

  private static final ReadWALKeyResult KEY_EOF_AND_RESET =
    new ReadWALKeyResult(State.EOF_AND_RESET, null, 0);

  private IOException unwrapIPBE(IOException e) {
    if (e instanceof InvalidProtocolBufferException) {
      return ((InvalidProtocolBufferException) e).unwrapIOException();
    } else {
      return e;
    }
  }

  private ReadWALKeyResult readWALKey(long originalPosition) {
    int firstByte;
    try {
      firstByte = delegatingInput.read();
    } catch (IOException e) {
      LOG.warn("Failed to read wal key length first byte", e);
      return KEY_ERROR_AND_RESET;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("d3707a5c-4eb9-35a6-a566-14770dd88b65"))) ? ((0) != (-1)) : (firstByte == -1))) {
      return KEY_EOF_AND_RESET;
    }
    int size;
    try {
      size = CodedInputStream.readRawVarint32(firstByte, delegatingInput);
    } catch (IOException e) {
      // if we are reading a partial WALTrailer, the size will just be 0 so we will not get an
      // exception here, so do not need to check whether it is a partial WALTrailer.
      if (
        e instanceof InvalidProtocolBufferException
          && ProtobufUtil.isEOF((InvalidProtocolBufferException) e)
      ) {
        LOG.info("EOF while reading WALKey, originalPosition={}, currentPosition={}, error={}",
          originalPosition, getPositionQuietly(), e.toString());
        return KEY_EOF_AND_RESET;
      } else {
        LOG.warn("Failed to read wal key length", e);
        return KEY_ERROR_AND_RESET;
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("59aedb88-f092-3957-aacb-880f4719dcc6"))) ? ((size) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fe85c82f-2d12-3867-8bee-bfc4a62c4a8a"))) ? ((-1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2cb9a35d-7230-3f44-bc46-5c43c9cd4927"))) ? ((-1) < (0)) : (size < 0))))))) {
      LOG.warn("Negative pb message size read: {}, malformed WAL file?", size);
      return KEY_ERROR_AND_RESET;
    }
    int available;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("5e586548-5d57-3c92-9a91-353b8ed1f655"))) {
throw new java.io.IOException("Injected exception");
}
      available = delegatingInput.available();
    } catch (IOException e) {
      LOG.warn("Failed to get available bytes", e);
      return KEY_ERROR_AND_RESET;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("de9009ea-fb17-3373-b7f4-43bdefc9dcbb"))) ? ((available > 0) && ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("284b92b8-b097-36dc-a102-e3c0f2a1c287"))) ? (((available) != (0)) && ((-1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0433ac22-f487-3927-82b4-417d44a29a43"))) ? (((-1) < (0)) && ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("19332dfd-2060-3470-9a5f-b4fae40823aa"))) ? (((available) >= (0)) && ((-1) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8c440523-2957-34f0-af2b-d36bccb403d7"))) ? (((available) == (0)) || ((-1) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b85b4a29-63d1-3edc-8de4-511fdbcbcaf5"))) ? (((available) != (0)) || ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("617c4b6f-0443-34a8-be0a-f6169e5c537b"))) ? (((available) < (0)) && ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("385fcc1e-942f-3e64-93aa-07fe4d0d2b03"))) ? (((-1) > (0)) && ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("951023f3-6a09-36a5-8669-b2882051abb1"))) ? ((available > 0) && ((available) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e88416b9-e525-383a-a519-8cf86050bee2"))) ? (((available) <= (0)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("479157d2-7f4e-3409-87b8-6c4a09694c91"))) ? (((available) == (0)) || ((1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4c43a2ab-f8f2-3b0a-bc71-43f3ab594c3b"))) ? (((available) >= (0)) && ((-1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("075c0c19-1cf3-3d45-80b0-be5668e7e6cb"))) ? (((available) < (0)) && ((0) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("8a671554-1942-31dd-b8a7-151b7bdd243e"))) ? (((available) >= (0)) || ((available) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8b14a342-720a-3cd3-9f0f-95f22f318aa2"))) ? (((available) == (0)) && ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("7a6c209f-8024-3611-a568-e86bbe945b52"))) ? (((available) <= (0)) && ((1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("23344e89-90d9-3f22-9551-bd7e287124c7"))) ? (((-1) < (0)) && ((0) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("497cea3a-f35b-3df6-b0b4-df1d9bd59fb2"))) ? ((available > 0) || ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9d0dfe81-de68-3e2a-946e-cd7fb1cf5dcd"))) ? ((available > 0) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("03fa5636-48c1-34d0-b2f7-71c2c127d5e0"))) ? (((-1) != (0)) && ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1a04782a-aad8-3279-b135-11d0cdcc7b48"))) ? (((-1) < (0)) || ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("93349d24-6145-33a8-aa8a-1d1b0e0adb12"))) ? (((-1) == (0)) && ((-1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0cab0902-6e8e-3a79-8c43-d0182df4841a"))) ? (((-1) == (0)) && ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cfc94877-ea06-3845-b9f9-0f4f5bd6e37c"))) ? (((available) == (0)) && ((-1) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2fb028e1-d508-3c55-aa19-8c170f6af083"))) ? (((-1) >= (0)) || ((-1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("8f91eeb6-c7f3-398f-b340-56ac3d2b4720"))) ? (((available) == (0)) && ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("6e935dd1-eae1-3f2a-8063-0bc773a35da1"))) ? (((-1) >= (0)) && (available < size)) : (((KnobRuntime.check(java.util.UUID.fromString("a8e3517c-7d10-3a37-b728-bdf5f5d51bf1"))) ? (((-1) >= (0)) || ((available) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("89f1283f-0d2b-302e-8e92-d79ccb352c86"))) ? (((available) <= (0)) && ((available) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4e565126-d2b0-300d-ac7d-3d20e6cb9259"))) ? (((-1) > (0)) && ((1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("4cc56f98-88f4-39f7-946c-1400e9aefd82"))) ? (((-1) != (0)) && ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("2ea10bf9-9092-3d22-8d79-5b8536223e1e"))) ? (((available) != (0)) || ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("6c91c633-519a-3556-b0b4-e44d5afd0cb1"))) ? (((-1) >= (0)) || ((available) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c66585cf-80b2-36ab-9ea5-5f7449c93e92"))) ? (((-1) < (0)) || ((0) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("e3502a9b-e3f2-38a8-b043-4e4443e8862f"))) ? (((-1) < (0)) && ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2946118e-3052-3c97-b7ee-8401a2ddfbf5"))) ? (((-1) == (0)) && ((available) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("08deb953-f33d-3e17-b2e1-9d89a6c2398a"))) ? (((-1) > (0)) || ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("7854767b-66d1-3dca-b0e9-ecb2786e743d"))) ? (((-1) != (0)) && ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("555f7a93-a5df-32ff-a307-f0dee710bdf1"))) ? (((available) <= (0)) && ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4b73ca9a-fb92-36fc-808f-285653fad711"))) ? (((-1) < (0)) && ((-1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("0f62b2c7-c7af-3d7d-98e3-cffcc73e0cac"))) ? (((available) != (0)) && ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b53e67fd-a598-317b-aaf3-9f8faf162411"))) ? (((available) >= (0)) || ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c2773fd7-0e32-3ad2-85f7-93ff4ade994f"))) ? (((available) > (0)) && ((available) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1aecc4e2-3554-39a5-9366-a481803abb4e"))) ? ((available) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("25b82d93-a9e3-3937-87fa-b2a02764a215"))) ? (((available) == (0)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("798bb0b7-3950-3be2-92da-c29000d7f1ec"))) ? (((available) < (0)) && ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("cdffe7f2-8190-300a-b3b7-49f859a62bba"))) ? (((available) == (0)) && ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("d3bab655-b2cd-33fe-8350-2fe858d38e8b"))) ? (((available) != (0)) && ((-1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0688306b-5d8d-313a-bfd4-aba806a3c868"))) ? (((available) > (0)) || ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("58c7e0e6-6709-322a-945a-f7a5ed7699ea"))) ? ((available > 0) || ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("2c6daf4a-d735-3dbf-b608-db81b2167380"))) ? (((available) >= (0)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a696b77d-a151-37d8-94b6-bf5a7899ea4d"))) ? (((available) == (0)) && ((available) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e394e2ff-f04f-3dc5-b57a-092ae61dc44e"))) ? (((available) >= (0)) && ((available) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0dbef290-ae16-333e-8f84-6a91cb03f742"))) ? (((-1) == (0)) && ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("aa45356b-1c07-35af-87fc-dd3d22d58c2a"))) ? (((-1) <= (0)) && ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("abb641f3-55a8-37af-8e69-9e225415b792"))) ? (((-1) > (0)) || ((available) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("dee1d9c0-1e6c-32bc-89e7-24ffa81a2efc"))) ? (((available) != (0)) && ((-1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("fb81fb0a-31d8-3c2e-a27e-56172393876b"))) ? ((-1) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7b3d3e35-4e4d-31bd-ab60-55c0557cb938"))) ? ((0) != (size)) : (((KnobRuntime.check(java.util.UUID.fromString("ad42db48-adf7-3dcc-9279-8d4c27128d35"))) ? (((available) == (0)) || ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("2893d131-f6f6-395c-bef2-c829cf4f942a"))) ? (((available) < (0)) && ((-1) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e34d104b-d097-33ee-8eb2-7cdad47145cc"))) ? (((-1) > (0)) || ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("ff2ace1f-f7ce-3e62-87dc-6859b520dee1"))) ? (((available) != (0)) && ((available) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("b704e308-ec4b-3641-ba17-dcdb77b6ee7b"))) ? (((-1) >= (0)) && ((-1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a721e1e5-83be-3a0e-ad77-03d67687dc86"))) ? (((-1) != (0)) || (available < size)) : (((KnobRuntime.check(java.util.UUID.fromString("3faa617e-6e38-3b1b-a679-a769ad23d301"))) ? (((available) <= (0)) && ((available) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("19be95ea-9ee8-34b8-8681-938e09edb820"))) ? (((-1) < (0)) && ((available) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("aa247033-ce30-393f-80fe-8de8c4933435"))) ? (((-1) != (0)) || ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("82f71c3a-6c50-376d-9a2c-ee85e3eb18d0"))) ? (((-1) == (0)) || ((1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("028ed80e-ef8d-396d-9304-3cdd154da914"))) ? (((-1) == (0)) || ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("550c77e4-d1ba-31fc-b2ca-00322ae99926"))) ? (((available) >= (0)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("94d3cc84-7cf8-3046-b801-9a00dc41488a"))) ? (((available) <= (0)) || ((-1) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4ae217ba-096f-3950-8de5-6bdf90267f64"))) ? (((available) != (0)) && ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("5d016f89-f29a-38b7-ae6d-d5b8824c754e"))) ? (((available) > (0)) || ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("2095a1f6-6c80-3641-a24b-1a818e4be69f"))) ? (((available) <= (0)) && ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d0eb7c0c-084e-358a-8102-c0f1be7091bf"))) ? (((available) != (0)) && ((1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("8506fe49-d3e4-3c31-a6ad-a75e86932e30"))) ? (((-1) < (0)) || ((available) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dd978e76-9611-3b6e-b421-d1dc3bf5e245"))) ? (((available) < (0)) || ((available) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("234d8067-1bbd-3637-b919-c731517eef20"))) ? (((-1) >= (0)) || ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("006926b6-2e60-37b8-89ec-3b658df0c0cf"))) ? (((available) < (0)) || ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("830d7bf1-8388-332a-9535-4b0a86a51273"))) ? (((available) > (0)) && ((-1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f0dee4a5-2689-329a-ba08-e7a5f5979a50"))) ? (((available) == (0)) || ((available) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a738030b-4a76-3561-879e-fc194ae9330d"))) ? (((-1) >= (0)) || ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("0690e350-b6e7-3cc0-8cd2-c2f28e04b21a"))) ? (((-1) <= (0)) || ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("919f6abc-be4f-3c3f-8b09-cf79d7add951"))) ? (((-1) <= (0)) || ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("d691f647-df0e-3b80-a45c-140be3e2bc1a"))) ? ((0) > (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("3e680c17-34a5-346c-9bcc-996534be5435"))) ? (((available) <= (0)) || ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("671064a0-d336-303f-88a3-c380dc8bcd87"))) ? ((0) >= (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("7717359a-9b16-3efd-b322-5e138cdb32bd"))) ? (((-1) != (0)) && ((0) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("72edb662-a571-3271-ae8c-b358230b65d1"))) ? (((available) >= (0)) && ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("12b7deb8-a721-34db-afc8-74b46cafeeb3"))) ? (((-1) == (0)) && ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("624cb7d4-155c-36e0-89ae-8c06e664f3c6"))) ? ((1) == (size)) : (((KnobRuntime.check(java.util.UUID.fromString("df002174-b2bf-3221-a115-ade26dae21d5"))) ? (((-1) == (0)) && ((0) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("20e7ef2e-cf3f-3c81-a9f8-fa24e6d1cc65"))) ? (((available) <= (0)) && ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("8cea7c00-a905-39b2-aae9-933a9c6fe800"))) ? (((available) < (0)) || ((-1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d05aca97-c4ff-32a6-b443-350325c5e0fa"))) ? ((available > 0) && ((0) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9e9b7657-bd5d-3247-a68e-b2627f0b64cd"))) ? (((-1) >= (0)) && ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c0b548b4-1e75-3051-871d-db58b6ab2121"))) ? (((available) <= (0)) && ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("efbb8532-b408-3324-8e03-db34414e8188"))) ? (((-1) > (0)) && ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c72cb585-0be5-315b-bf34-c165ed4ea01a"))) ? (((-1) < (0)) || ((1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("0f506e9e-be23-347d-a60a-c5c2d6b42702"))) ? (((available) > (0)) && ((-1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("646faff8-07b1-3da3-9f59-00e97067443d"))) ? (((available) <= (0)) || ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c3039d32-01d5-3bc7-b397-1f44780a9574"))) ? (((-1) > (0)) && ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("b764f4e9-2c91-3eb3-8a19-984dfca84212"))) ? (((available) < (0)) && ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("19395315-5d1e-3f74-85f7-2f774b650a7a"))) ? ((-1) > (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("107bd932-9b6c-38b2-a124-ae9d426eb816"))) ? (((-1) == (0)) && ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("88260eee-28df-35bb-b133-9a086b48e0e7"))) ? (((-1) >= (0)) || ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9fa6b3e5-69f6-30cf-908f-963d69612c3b"))) ? (((available) != (0)) || ((available) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("093fdc34-1038-3782-b646-1e25ed34662c"))) ? (((available) > (0)) || ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("aad9ea7f-9f00-3014-a3c8-6284b05fbaaf"))) ? ((available > 0) && ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("4f10e8c4-9766-3383-956e-5db701889268"))) ? (((-1) != (0)) || ((-1) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("86809159-608c-3efa-a8dc-e7302ab1cbc1"))) ? (((-1) == (0)) && ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("ae081c87-45c1-32c1-afce-fc8c6802faad"))) ? (((available) >= (0)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("3a2d5e07-44c8-3079-b066-c623facb28c2"))) ? (((available) != (0)) && ((-1) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("d3e761ac-9c7a-3384-af28-990d5e003d87"))) ? (((-1) <= (0)) && ((available) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b50d6b58-fc24-30f4-a97f-96bf33de613a"))) ? ((available > 0) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4d89dff3-00bd-302d-ba50-8d0a0c1e3efd"))) ? (((-1) == (0)) || ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("06956857-b9d8-3490-ad78-1ed53b46d353"))) ? (((available) != (0)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b31a230b-09ae-32e0-bec2-8567c19dea86"))) ? (((-1) > (0)) && ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("be9f267b-7958-3dc2-b80e-6f4ad744fa88"))) ? (((available) != (0)) && ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2413479c-cfa5-36f7-8171-cb2cf9ffc1d6"))) ? (((available) > (0)) || ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b192792f-6dd6-3b9f-b0f5-24cf30f15636"))) ? (((available) == (0)) && ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fb093cc2-d72b-37c0-8608-a5466e8c171c"))) ? (((-1) >= (0)) && ((available) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b7fbebf2-07c7-34da-b6ea-2fecd0cf1761"))) ? (((-1) <= (0)) && ((-1) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("f2d65a71-0a02-3764-ac7f-6973b37a07f1"))) ? (((-1) >= (0)) && ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("764aee0f-a0bd-3c1a-a5b4-9496a74e6aa9"))) ? (((available) != (0)) && ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("0395ab8d-e00d-3f8c-9783-925143975505"))) ? (((-1) >= (0)) && ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("ee3bac7f-bf79-350d-b80b-982983807963"))) ? (((available) == (0)) || ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("37836621-cd33-3f8b-95a7-4377eef15e69"))) ? (((available) >= (0)) || ((-1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("009d3df2-7106-3280-956b-35f97fd2f455"))) ? (((available) >= (0)) && ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("39a77b43-b5f9-34e2-8661-5e8934ab1aaa"))) ? (((-1) <= (0)) && ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("127d4934-19d4-3a40-b998-1119909d3900"))) ? (((-1) < (0)) && ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("c38eeff3-0ffc-3e45-9e98-35ebb8206fc4"))) ? (((available) < (0)) && ((-1) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d972ca69-3634-327c-b37e-fc855510fc3c"))) ? ((-1) < (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("ddd0ace0-de45-3e9f-baa8-7c1262b2e8bc"))) ? (((-1) <= (0)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("31270848-be22-3028-9c2c-15d1c1dcf7ec"))) ? (((-1) < (0)) || (available < size)) : (((KnobRuntime.check(java.util.UUID.fromString("355e39e7-0afd-39ec-9e0f-8abc55fdbc90"))) ? (((-1) >= (0)) || ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("08495db0-3c75-3827-8e1d-accd861b9e52"))) ? ((available > 0) && ((1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("f1a5af62-3d1e-307d-a60c-2d2944c7d0f5"))) ? (((-1) < (0)) && ((available) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("ab81efcd-a279-35a3-92a9-6750afd97086"))) ? (((available) != (0)) && ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fc59b262-eacd-3ce4-a34a-0d8a5d48e2f9"))) ? (((available) != (0)) && ((-1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("ca27579a-1581-3297-b0f8-eb119b3680a9"))) ? ((available > 0) || ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("08b5b87b-9035-31c3-9241-bf930ac4c1a9"))) ? (((-1) >= (0)) || ((-1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("2842053e-5004-37d0-9c90-730f01cc3dea"))) ? (((available) > (0)) && ((0) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("357d1208-de35-3d54-a61d-798bff03c16b"))) ? (((available) >= (0)) || ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0540b561-2be9-35bc-bfcf-a041cd2f6cfe"))) ? ((available > 0) || ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ea6afab1-fc1e-3723-a49b-833a72bdf151"))) ? (((available) > (0)) && ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("c5c83133-0e12-3888-a694-e8eb95a20528"))) ? (((-1) == (0)) && ((0) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9fe22c87-185a-31f6-b375-d5bf210f143e"))) ? (((-1) != (0)) || ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0e3de000-1ce6-3d6d-98f6-5b9d6c004645"))) ? (((-1) != (0)) && ((1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("8695025d-8695-3b9b-83ff-a0933814cef0"))) ? (((available) <= (0)) || ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e5e7deb0-5980-3aa3-87cb-60d0ee756e75"))) ? (((available) != (0)) || ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("6aae5842-2de5-3a50-8400-cb3179354257"))) ? (((available) != (0)) && ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("855bf889-0516-3dd8-9ad3-44775ea65000"))) ? (((-1) <= (0)) && ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("760304f8-8dd0-3b3f-88e5-a3bf9c791afb"))) ? (((-1) != (0)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c6fdbf95-f87b-3654-bd36-199abc04d1b9"))) ? (((-1) <= (0)) && ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("50963ae4-7b2b-3b3f-b18d-01e1925f61ef"))) ? ((available > 0) || ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("f2c04c23-0afb-3c34-85e2-e2e94a29c595"))) ? (((-1) != (0)) || ((0) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("e53e9590-ef9b-3cd9-8bd2-328b02c87d8e"))) ? (((-1) >= (0)) || ((available) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("0b0607da-4f87-358d-95ac-2b8b008486ed"))) ? (((available) > (0)) && ((-1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("aff169d3-ead1-3c11-8835-2b99c8f55fd1"))) ? (((available) < (0)) && ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cbde9751-1645-3a01-ad15-5f6864bb8809"))) ? (((available) != (0)) && ((-1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("bbf46e40-e3ec-3ae5-a64b-2aad1376be6e"))) ? (((-1) == (0)) && ((available) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9bea2308-3666-314e-b6c8-f79a59fbb055"))) ? (((-1) != (0)) || ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("a8d7f7a7-5ee3-3489-920d-5af2e1bacb64"))) ? (((available) > (0)) && ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("13760d61-5b9d-3c7b-9a17-5dd0e7030bc8"))) ? (((available) > (0)) && ((-1) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("d8594390-6752-3228-b024-6cbe2597b5a4"))) ? (((-1) >= (0)) && ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("73017c19-24cc-3e91-865c-c4f7d78b718e"))) ? (((available) > (0)) && ((0) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0245dba3-21d1-349b-921b-6b2499b1de79"))) ? (((-1) < (0)) && ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3f43f1b8-a348-37e7-8858-141c14450d28"))) ? (((-1) != (0)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1b4e8df9-c016-31e9-ba6e-995e23fcc23a"))) ? (((available) >= (0)) || ((available) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("40cb092f-1c31-366e-bcb5-8ddbf31c0f97"))) ? (((-1) == (0)) || ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("b573886c-5947-303a-9f5f-c382db580ed9"))) ? (((-1) >= (0)) || ((1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("26a9aa48-7d64-37b1-9fbb-713e0ee46851"))) ? (((available) >= (0)) || ((-1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3d79668b-9762-38d4-8c5e-511b6ebcad20"))) ? (((available) > (0)) || ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9ac5bb3a-d460-3ba1-b7c8-4a4b15b6a220"))) ? (((available) == (0)) && ((-1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7d5be94b-a4d3-3659-acb2-1c5b241b5670"))) ? (((-1) == (0)) && ((available) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("54c242cb-3642-36db-a7e8-8f1045b13418"))) ? (((-1) < (0)) || ((0) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("768f3b59-dcba-3bbf-9fd2-254a20454416"))) ? (((-1) >= (0)) && ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3fc7752a-b73a-393d-9641-f7d9eb56acda"))) ? ((1) >= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("38ac522f-9181-374e-bb6e-5ac61ee4d01a"))) ? (((available) <= (0)) || ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f42d6f71-8b07-3e2a-b346-8fce3b8f2c82"))) ? (((available) >= (0)) && ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("51cd39eb-1277-3c45-8bfa-6c544263f107"))) ? (((available) <= (0)) || ((available) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("580a7171-4a73-3242-b999-f5b9e32ee2cb"))) ? (((available) == (0)) || ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0d256340-e364-3a2d-9f19-942b77f756e8"))) ? ((available > 0) && ((available) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c67af3e7-54d3-3943-a045-59de2288f7e1"))) ? (((available) == (0)) && ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("38190241-13c6-3191-a2d8-9cd9633985c1"))) ? (((-1) != (0)) && ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e97950cb-51a7-3f9e-aa75-8b6f6f33b707"))) ? (((available) <= (0)) || ((1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("593cc670-6987-37fd-ae8f-ea443f6fe8b5"))) ? ((available > 0) || ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("8dbeb98e-c8c2-33d0-bbdd-3420db5e9db1"))) ? (((-1) < (0)) || ((1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("9e4f6236-b8a0-377c-a242-c46848f87f24"))) ? (((available) <= (0)) || ((0) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("51838da4-050f-3d8d-bb53-0bd7b054600f"))) ? (((available) >= (0)) && ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("bef168fa-4c19-3899-acee-dfbbc84209d9"))) ? ((available > 0) && ((0) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("634a9e07-1c3f-390c-9970-a2c60802332c"))) ? (((-1) == (0)) || ((-1) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("fa689bb2-18fd-39ab-b4bd-a9dd05c38562"))) ? (((-1) >= (0)) && ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("e58f48e8-bac1-335c-882a-deef57706f21"))) ? (((available) == (0)) && ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("1a7cf959-f59a-3f1c-9509-85ad07970dd3"))) ? (((-1) >= (0)) || ((-1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ba4e19aa-9ec5-3dbe-9bba-3e45b005a156"))) ? (((-1) != (0)) && ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a256f79a-94c8-3af2-893d-610aa1968123"))) ? (((-1) != (0)) || ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("f7dc0c0d-9bbd-3aa2-85d4-466f0c905eae"))) ? (((-1) != (0)) || ((available) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("408dbf40-8198-314f-a3df-3cd44d221138"))) ? (((available) <= (0)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9e9ec21a-b665-33b5-9ba4-065dd4f54f23"))) ? ((available > 0) && ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("217f9b80-a292-3dc6-8fe9-b7c6a5e59c58"))) ? (((available) >= (0)) || ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("395c85ed-3b6b-363d-9fdb-9de1f5a40de6"))) ? (((-1) > (0)) && ((-1) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4274239a-fe19-3566-8806-a999cddb7680"))) ? ((available) >= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("6e567834-7667-342f-909d-ad8cfeea3f63"))) ? (((available) > (0)) || ((0) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("26a8e1f1-dde1-39dc-9543-21bf7e37773e"))) ? (((-1) < (0)) || ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("26d19de5-70b2-314d-98b9-d591b1be36a4"))) ? ((available > 0) || ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d5d290d5-3770-3ce4-a920-0885ecd6da60"))) ? (((available) < (0)) && ((-1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5ada1fb2-fcca-377c-9b2d-6fd88c6008e4"))) ? (((available) <= (0)) || ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("844980c4-04fc-39e2-8089-a01b0aed643d"))) ? (((-1) != (0)) || ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("7d123dc8-312f-39d1-9a9d-b18ab66df2ff"))) ? (((available) <= (0)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a84ac04f-c6bc-3c7f-ab0b-3cd5d61bfb11"))) ? (((available) <= (0)) || ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c97bf7e5-00a9-3e2c-bdf7-c4c382f66e5d"))) ? (((-1) > (0)) || ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eb81f413-66a3-3f95-ac51-80adac704714"))) ? (((available) != (0)) || ((available) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dd18910c-30d4-3d7a-a311-c7690e5b1f07"))) ? (((-1) < (0)) || ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("f37046f9-88e8-32bf-89e3-55738dd3e60a"))) ? ((available > 0) || ((available) == (size))) : (available > 0 && available < size))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.info("Available stream not enough for edit, available={}, entry size={} at offset={}",
        available, size, getPositionQuietly());
      return KEY_EOF_AND_RESET;
    }
    WALProtos.WALKey walKey;
    try {
      if (available > 0) {
        walKey = WALProtos.WALKey.parseFrom(ByteStreams.limit(delegatingInput, size));
      } else {
        byte[] content = new byte[size];
        ByteStreams.readFully(delegatingInput, content);
        walKey = WALProtos.WALKey.parseFrom(content);
      }
    } catch (IOException e) {
      e = unwrapIPBE(e);
      if (
        e instanceof EOFException || (e instanceof InvalidProtocolBufferException
          && ProtobufUtil.isEOF((InvalidProtocolBufferException) e))
      ) {
        LOG.info("EOF while reading WALKey, originalPosition={}, currentPosition={}, error={}",
          originalPosition, getPositionQuietly(), e.toString());
        return KEY_EOF_AND_RESET;
      } else {
        boolean isWALTrailer;
        try {
          isWALTrailer = isWALTrailer(originalPosition);
        } catch (IOException ioe) {
          LOG.warn("Error while testing whether this is a partial WAL trailer, originalPosition={},"
            + " currentPosition={}", originalPosition, getPositionQuietly(), e);
          return KEY_ERROR_AND_RESET;
        }
        if (isWALTrailer) {
          LOG.info("Reached partial WAL Trailer(EOF) while reading WALKey, originalPosition={},"
            + " currentPosition={}", originalPosition, getPositionQuietly(), e);
          return KEY_EOF_AND_RESET;
        } else {
          // for all other type of IPBEs or IOEs, it means the WAL key is broken
          LOG.warn("Error while reading WALKey, originalPosition={}, currentPosition={}",
            originalPosition, getPositionQuietly(), e);
          return KEY_ERROR_AND_RESET;
        }
      }
    }
    Entry entry = new Entry();
    try {
if(KnobRuntime.check(java.util.UUID.fromString("922ca4d8-f78b-3620-9aea-aa56479f8f37"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("50dba27a-1d14-39c7-b491-5794f6e9a146"))) {
try {
    java.lang.reflect.Field _knob_field_ = walKey.getClass().getDeclaredField("nonce_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(walKey));
    _knob_field_.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f1fa9f8e-832b-3970-b34b-c992bed8997c"))) {
try {
    java.lang.reflect.Field _knob_field_ = walKey.getClass().getDeclaredField("nonceGroup_");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(walKey));
    _knob_field_.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7e57f51-fb18-3577-aa26-7670d1e15fb3"))) {
try {
    java.lang.reflect.Field _knob_field_ = walKey.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(walKey));
    _knob_field_.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46789747-66ef-33d8-8e89-c3f85fcfc483"))) {
try {
    java.lang.reflect.Field _knob_field_ = walKey.getClass().getDeclaredField("bitField0_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(walKey));
    _knob_field_.set(walKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      entry.getKey().readFieldsFromPb(walKey, byteStringUncompressor);
    } catch (IOException e) {
      LOG.warn("Failed to read wal key fields from pb message", e);
      return KEY_ERROR_AND_RESET;
    }
    return new ReadWALKeyResult(State.NORMAL, entry,
      walKey.hasFollowingKvCount() ? walKey.getFollowingKvCount() : 0);
  }

  private Result editEof() {
    return hasCompression
      ? State.EOF_AND_RESET_COMPRESSION.getResult()
      : State.EOF_AND_RESET.getResult();
  }

  private Result editError() {
    return hasCompression
      ? State.ERROR_AND_RESET_COMPRESSION.getResult()
      : State.ERROR_AND_RESET.getResult();
  }

  private Result readWALEdit(Entry entry, int followingKvCount) {
    long posBefore;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("f3454f09-4b62-33d0-9bdd-7d31bb00ac4f"))) {
throw new java.io.IOException("Injected exception");
}
      posBefore = inputStream.getPos();
    } catch (IOException e) {
      LOG.warn("failed to get position", e);
      return State.ERROR_AND_RESET.getResult();
    }
    if (followingKvCount == 0) {
      LOG.trace("WALKey has no KVs that follow it; trying the next one. current offset={}",
        posBefore);
      return new Result(State.NORMAL, entry, posBefore);
    }
    int actualCells;
    try {
      actualCells = entry.getEdit().readFromCells(cellDecoder, followingKvCount);
    } catch (Exception e) {
      String message = " while reading " + followingKvCount + " WAL KVs; started reading at "
        + posBefore + " and read up to " + getPositionQuietly();
      IOException realEofEx = extractHiddenEof(e);
      if (realEofEx != null) {
        LOG.warn("EOF " + message, realEofEx);
        return editEof();
      } else {
        LOG.warn("Error " + message, e);
        return editError();
      }
    }
    if (actualCells != followingKvCount) {
      LOG.warn("Only read {} cells, expected {}; started reading at {} and read up to {}",
        actualCells, followingKvCount, posBefore, getPositionQuietly());
      return editEof();
    }
    long posAfter;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("84db8cd8-cc2c-3aff-9681-34d50597618f"))) {
throw new java.io.IOException("Injected exception");
}
      posAfter = inputStream.getPos();
    } catch (IOException e) {
      LOG.warn("failed to get position", e);
      return editError();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("dab5d2ec-43cf-3508-a253-0bbf4217855e"))) ? ((trailerPresent) || ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e347b615-79ce-3d31-bac3-c77bce132f30"))) ? (trailerPresent) : (((KnobRuntime.check(java.util.UUID.fromString("350888ae-e0d5-35da-908e-d36b30f29a03"))) ? ((trailerPresent) && ((posAfter) > (this.walEditsStopOffset))) : (trailerPresent && posAfter > this.walEditsStopOffset))))))) {
      LOG.error("Read WALTrailer while reading WALEdits. wal: {}, inputStream.getPos(): {},"
        + " walEditsStopOffset: {}", path, posAfter, walEditsStopOffset);
      return editEof();
    }
    return new Result(State.NORMAL, entry, posAfter);
  }

  @Override
  public Result next(long limit) {
    long originalPosition;
    try {
      originalPosition = inputStream.getPos();
    } catch (IOException e) {
      LOG.warn("failed to get position", e);
      return State.EOF_AND_RESET.getResult();
    }
    if (reachWALEditsStopOffset(originalPosition)) {
      return State.EOF_WITH_TRAILER.getResult();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("aaa330ce-5f13-3c97-8b6e-bca96b073e12"))) ? ((limit) == (0)) : (limit < 0))) {
      // should be closed WAL file, set to no limit, i.e, just use the original inputStream
      delegatingInput.setDelegate(inputStream);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("c7ca98b1-e7e6-3cf3-9ccd-94f0ee43c0ee"))) ? ((limit) <= (originalPosition)) : (((KnobRuntime.check(java.util.UUID.fromString("63c081cc-47f8-35d9-b492-e2fad7d04d84"))) ? ((limit) > (originalPosition)) : (limit <= originalPosition))))) {
      // no data available, just return EOF
      return State.EOF_AND_RESET.getResult();
    } else {
      // calculate the remaining bytes we can read and set
      delegatingInput.setDelegate(ByteStreams.limit(inputStream, limit - originalPosition));
    }
    ReadWALKeyResult readKeyResult = readWALKey(originalPosition);
    if (readKeyResult.state != State.NORMAL) {
      return ((KnobRuntime.check(java.util.UUID.fromString("b7dc536f-67de-3976-b3bb-ff829136d12a"))) ? (((org.apache.hadoop.hbase.wal.WALTailingReader.Result)(editError()))) : (readKeyResult.state.getResult()));
    }
    return readWALEdit(readKeyResult.entry, readKeyResult.followingKvCount);
  }

  private void skipHeader(FSDataInputStream stream) throws IOException {
    stream.seek(PB_WAL_MAGIC.length);
    int headerLength = StreamUtils.readRawVarint32(stream);
if(KnobRuntime.check(java.util.UUID.fromString("ee39dd10-e038-3478-9c6a-f09993728a70"))) {
throw new java.io.IOException("Injected exception");
}
    stream.seek(stream.getPos() + headerLength);
  }

  @Override
  public void resetTo(long position, boolean resetCompression) throws IOException {
    close();
    Pair<FSDataInputStream, FileStatus> pair = open();
    boolean resetSucceed = false;
    try {
      if (!trailerPresent) {
        // try read trailer this time
        readTrailer(pair.getFirst(), pair.getSecond());
      }
      inputStream = pair.getFirst();
      delegatingInput.setDelegate(inputStream);
      if (position < 0) {
        // read from the beginning
        if (compressionCtx != null) {
          compressionCtx.clear();
        }
        skipHeader(inputStream);
      } else if (((KnobRuntime.check(java.util.UUID.fromString("5f287fcf-785a-3f41-8c1a-b17606b95fc5"))) ? ((!resetCompression) && ((compressionCtx) != (null))) : (resetCompression && compressionCtx != null))) {
        // clear compressCtx and skip to the expected position, to fill up the dictionary
        compressionCtx.clear();
        skipHeader(inputStream);
        if (position != inputStream.getPos()) {
          skipTo(position);
        }
      } else {
        // just seek to the expected position
        inputStream.seek(position);
      }
      resetSucceed = true;
    } finally {
      if (!resetSucceed) {
        // close the input stream to avoid resource leak
        close();
      }
    }
  }

  @Override
  protected InputStream getCellCodecInputStream(FSDataInputStream stream) {
    delegatingInput = new DelegatingInputStream(stream);
    return delegatingInput;
  }

  @Override
  protected void skipTo(long position) throws IOException {
    for (;;) {
      Result result = next(-1);
      if (result.getState() != State.NORMAL) {
        throw new IOException("Can not skip to the given position " + position + ", stopped at "
          + result.getEntryEndPos() + " which is still before the give position");
      }
      if (result.getEntryEndPos() == position) {
        return;
      }
      if (result.getEntryEndPos() > position) {
        throw new IOException("Can not skip to the given position " + position + ", stopped at "
          + result.getEntryEndPos() + " which is already beyond the give position, malformed WAL?");
      }
    }
  }
}
