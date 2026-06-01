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
if(KnobRuntime.check(java.util.UUID.fromString("0c7b3b79-982e-3f52-9247-7f72ae068126"))) {
throw new java.io.IOException("Injected exception");
}
      firstByte = delegatingInput.read();
    } catch (IOException e) {
      LOG.warn("Failed to read wal key length first byte", e);
      return KEY_ERROR_AND_RESET;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("67fcbec6-9346-3ee3-adae-8404a5a6f96d"))) ? ((firstByte) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("dcb7ac57-92f8-36a7-abad-1513c0ba3757"))) ? ((-1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("efe6fd8e-94dd-3745-a477-a3ca9ad05eb1"))) ? ((1) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("28dea4ec-b567-3038-a17b-35b6af2edca8"))) ? ((0) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("9cbae1cb-4b1b-3a91-97d4-45aaba045408"))) ? ((firstByte) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("7a94a308-2c25-32f4-b823-54f4b12e2355"))) ? ((1) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("d3707a5c-4eb9-35a6-a566-14770dd88b65"))) ? ((0) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("9dae62a1-916a-395d-8ca8-2d6fa237a055"))) ? ((-1) != (-1)) : (firstByte == -1))))))))))))))))) {
      return KEY_EOF_AND_RESET;
    }
    int size;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("14f92bfe-7121-341e-a944-43ae93eee7ef"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f5722852-eec0-3fb8-9230-ce86fbeea914"))) {
firstByte = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ccc85736-e975-349b-8c6b-e1c49739dd8b"))) {
firstByte -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9bddb432-039b-3ed9-9103-8009b257ea19"))) {
firstByte += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("251f2fe3-142c-3614-a863-b2355544244a"))) {
firstByte *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1a768a27-cc22-3e58-9ea9-166ec78f4348"))) {
firstByte = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b242966e-bc4b-3ed6-a62a-b7db06cf0ece"))) {
firstByte /= 2;
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("6de91c69-b776-331f-9a2a-c5635a3abb23"))) ? ((-1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f62fad4b-4e9e-3d22-a645-b2043746f0f9"))) ? ((-1) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("72d09fa4-e8d0-33b2-ac20-7c5114e5d7b2"))) ? ((size) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("59aedb88-f092-3957-aacb-880f4719dcc6"))) ? ((size) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2cb9a35d-7230-3f44-bc46-5c43c9cd4927"))) ? ((-1) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("43cb19b2-87f3-365c-b135-b90bcbc77891"))) ? ((-1) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("daec4a02-5df3-3b2a-948a-dc09e39da2d4"))) ? ((size) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bc3ef182-551a-3c20-afb0-65d3f97e2dd3"))) ? ((size) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("91dbeabc-23da-3582-b90f-686d21629f43"))) ? ((-1) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fe85c82f-2d12-3867-8bee-bfc4a62c4a8a"))) ? ((-1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("979878f6-8ec1-36ab-82ec-d1d2d7251bc4"))) ? ((size) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("68c6b070-1300-306b-b3cb-823ff3e17655"))) ? ((size) < (0)) : (size < 0))))))))))))))))))))))))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("afd9bc8f-8381-3937-a655-061e4cf93f88"))) ? (((-1) != (0)) && ((-1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("8c440523-2957-34f0-af2b-d36bccb403d7"))) ? (((available) == (0)) || ((-1) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("74837add-2094-3d91-8550-5a3df274f86a"))) ? (((-1) < (0)) || ((available) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("5a1158a3-212e-3ea9-8414-57c48c1dc4af"))) ? (((available) == (0)) && ((0) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("21186e1d-1cd9-3ab6-99ee-4f02bbe9a67e"))) ? (((available) <= (0)) || ((available) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("f6e52aeb-d96d-3bd6-8ce6-af0b661d5962"))) ? (((-1) != (0)) || ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ce0c019f-352c-3d49-8067-28b2963bdf4f"))) ? (((-1) > (0)) || ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("404078c2-e1a6-369a-908f-eea5666f3321"))) ? (((available) >= (0)) || ((0) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("bb930e71-b689-39a0-90da-48ea18865810"))) ? (((available) < (0)) && ((available) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("6d5cbaef-5f30-3480-8b12-399354da7d65"))) ? (((available) > (0)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("aa189ccc-82c3-3e84-936a-3ff9d749cfd6"))) ? (((-1) <= (0)) && ((0) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("3d79668b-9762-38d4-8c5e-511b6ebcad20"))) ? (((available) > (0)) || ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("061b94f2-c94f-31ed-9da0-6eb1438b1653"))) ? (((-1) <= (0)) && ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3b563e81-7a29-34d4-a451-9a426b745b93"))) ? ((available > 0) || ((1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("93b98eb6-dd89-3ecc-96e2-4e5a71176244"))) ? (((-1) == (0)) || ((available) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("61ee32aa-800c-3b78-8e1c-55e700a14e29"))) ? (((available) == (0)) && ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("1c474ea0-d555-3993-884b-21ce87191956"))) ? (((available) < (0)) || ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a2d2462e-fc0e-3501-96e4-3ca20498f5bb"))) ? (((available) == (0)) && ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("63b88c7c-7bdf-3da3-8e7d-017ed87c0cf1"))) ? (((available) < (0)) || ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("5827df2d-81e6-35c9-ab49-f9046d1f2112"))) ? (((available) == (0)) && ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b0e9c7df-5211-3e7e-ab91-e9cfccd19ed7"))) ? (((available) <= (0)) && ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cb1ce2ed-1ac1-3d01-8d1c-861562b80ded"))) ? (((-1) >= (0)) && ((available) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c5c5773a-ec60-31e9-ae0f-965a0295ef0c"))) ? (((available) <= (0)) && ((available) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("61799d69-8a24-3a19-86c7-ec1fb6037d93"))) ? (((-1) >= (0)) && ((available) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("db40f889-ecc8-3b08-bff8-6871c3b4d180"))) ? (((available) >= (0)) || ((-1) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("88dca5a5-1b70-3dfa-8e79-45586b62646d"))) ? (((available) != (0)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ca2399b9-b988-3f3f-b130-61398b8e9561"))) ? (((available) <= (0)) || ((1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("33493d72-becb-3057-8f33-90080d7b24c6"))) ? ((available > 0) && ((available) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("5189dc3e-34a6-3f12-82c2-d1ebb6d85234"))) ? (((-1) != (0)) && ((0) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("6be61c63-3e15-3aea-8f08-06e5c5900dfd"))) ? (((-1) == (0)) && ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("57655cdb-a3c8-342b-adc4-a4a483c7f65c"))) ? (((available) >= (0)) && ((-1) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("cd874e88-5dee-3b2e-bd1b-932ad209a017"))) ? (((-1) > (0)) || ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("49eb50d4-d6da-3a0d-addc-43231e6474e0"))) ? (((-1) > (0)) || ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("8dc18987-42ab-3b47-a57b-80dfda5d1a55"))) ? (((available) <= (0)) || ((-1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f656f27b-db3e-3e10-bc28-2355215e6c4f"))) ? ((available > 0) && ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("cbf4a3fa-131f-31f8-b1b9-b3c12130aa3c"))) ? (((-1) == (0)) && ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f3f2ab0e-bea9-3b5a-9af2-1296e680b5f2"))) ? (((-1) < (0)) || ((-1) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("eb464339-c584-315d-a9b8-c09742c94189"))) ? (((available) < (0)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("781690f3-4cf8-30da-a4dc-b1af68d22975"))) ? (((available) > (0)) && ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a4262caa-78b9-3c64-8778-ab44075ff09a"))) ? (((-1) == (0)) && ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("3be2d5ec-88a3-399f-8464-b34962041895"))) ? ((1) != (size)) : (((KnobRuntime.check(java.util.UUID.fromString("7b77f375-521c-3518-8e09-c38f7fe65967"))) ? ((available) <= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("72edb662-a571-3271-ae8c-b358230b65d1"))) ? (((available) >= (0)) && ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("3a46689f-a364-3e91-85a8-0a98c442dc97"))) ? (((available) >= (0)) && ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0581f93a-84e9-384d-b531-f976a4b46b59"))) ? (((available) > (0)) || ((available) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5d1482c1-208d-35f7-a65e-130ef8e87daf"))) ? (((-1) == (0)) || ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("217f9b80-a292-3dc6-8fe9-b7c6a5e59c58"))) ? (((available) >= (0)) || ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("e64bb157-9945-376c-a8c0-ce8f116cd3c6"))) ? (((-1) <= (0)) || ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("8cce247f-9a97-3ff4-997c-2c261b8cd41a"))) ? (((available) >= (0)) || ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8dd1027b-60db-3def-8ad2-a1834d5b9483"))) ? ((available > 0) && ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("84c0cb6f-c19e-3c1a-b510-becfb012655e"))) ? (((-1) <= (0)) || ((-1) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("0bd4c40f-f6fb-3427-86bf-d013662f7d35"))) ? (((available) > (0)) && ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("9e8d2fd8-4666-3050-8490-ec0cb912800a"))) ? (((-1) < (0)) || ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("d5dbde12-ab0b-3e86-a155-223ec31febc2"))) ? (((available) == (0)) || ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("093fdc34-1038-3782-b646-1e25ed34662c"))) ? (((available) > (0)) || ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("66afee22-0b0b-3309-bae6-b9d19e92e7aa"))) ? ((available > 0) && ((available) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("555324ce-aeb1-3a02-9166-a2c77a1e7f4a"))) ? (((available) > (0)) && ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("370040e4-d253-329c-a0d4-2376daa05499"))) ? (((available) < (0)) || ((-1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("aa247033-ce30-393f-80fe-8de8c4933435"))) ? (((-1) != (0)) || ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("20bc36d6-d56a-3cb8-8b88-4cccb1800dec"))) ? (((-1) != (0)) || ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4ab24441-8563-36ef-bfd7-b2e2bf8f1e91"))) ? ((available > 0) || ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("2f43e4c1-a6e2-320b-8969-b09013be0685"))) ? (((available) < (0)) && ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("8c1cea9a-26fa-353e-99df-50619a0eebe1"))) ? ((available > 0) || ((1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("5c12646a-f7bc-33a0-831d-c363730430df"))) ? (((available) > (0)) && ((1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("5521d525-051a-35d8-a909-44a751ad7035"))) ? ((available > 0) && ((available) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("7af67caa-1427-3779-b4a2-d845b8c60621"))) ? (((-1) >= (0)) || ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4b0508b6-c6e9-3475-8987-a68860ca2479"))) ? (((-1) < (0)) && ((0) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("48d7b087-f6ed-3df9-bd38-6c13a8d90a6b"))) ? (((available) == (0)) || ((1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c00919fa-524a-39b4-953d-1da4d38c208f"))) ? (((-1) <= (0)) && ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e37b1671-e00f-353c-ab70-609ad667f204"))) ? (((-1) >= (0)) || ((-1) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("424faf70-53f1-3556-a141-659f651f0c8d"))) ? (((-1) != (0)) || ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("ac8deadd-e91a-38c1-b45f-8d4b27dad7da"))) ? (((available) <= (0)) || ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("8338984f-29c2-3bc8-b446-f26f22bb2e1e"))) ? (((available) >= (0)) || ((0) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("d8594390-6752-3228-b024-6cbe2597b5a4"))) ? (((-1) >= (0)) && ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("4f3eb02c-5d7f-3b2c-9c1d-f5a8cb494607"))) ? (((-1) != (0)) && ((available) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d1d8039-b278-3651-aa7b-1d61e2efe60c"))) ? (((available) != (0)) || ((-1) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("82f005b1-3130-3bd4-9dc5-296a4247c94e"))) ? (((-1) <= (0)) || ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a3a86a1e-2e5c-3ad0-8a8a-0be243504cdc"))) ? ((available > 0) || ((available) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("afe02200-2fac-3af7-ba73-a149a7bbdd74"))) ? (((-1) <= (0)) && ((1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a7c7de41-8f51-3771-b097-5bcac5f3f107"))) ? (((-1) == (0)) || ((available) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0ef79852-5555-330e-a9fb-b849e37f1250"))) ? (((-1) == (0)) && ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("54f4faaf-b1a3-3f7b-91ea-7bdd557a006f"))) ? ((available) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2946118e-3052-3c97-b7ee-8401a2ddfbf5"))) ? (((-1) == (0)) && ((available) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("2ab0d194-1aed-3bbb-9e81-d843f0ce2645"))) ? (((available) != (0)) && ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("87dae8ed-cfd8-3c99-a39f-7cee504b6e3e"))) ? (((-1) <= (0)) || ((available) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("50c0c43e-b167-3982-aa1d-2ec5f75580c1"))) ? (((-1) > (0)) || ((-1) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("006926b6-2e60-37b8-89ec-3b658df0c0cf"))) ? (((available) < (0)) || ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0fbe6a9c-85c7-3b7d-8f84-c21b1fb4e906"))) ? (((-1) <= (0)) || ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("99bf3782-4beb-395d-a036-5606c0b5f67c"))) ? (((available) == (0)) && ((-1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("582a0c8c-573c-3104-9a82-358a6bc1c59e"))) ? (((available) <= (0)) && ((-1) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("70b4573f-df18-3a26-9d6c-651fb201c227"))) ? (((-1) != (0)) && ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("b4a75068-3c7e-33fd-9987-64c5b505dcac"))) ? (((available) <= (0)) && ((-1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("5848c5ed-5467-36b4-a078-632293ed7b7d"))) ? (((available) >= (0)) || ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9847ef82-0772-39ad-a2e8-3428c496b8ba"))) ? (((-1) == (0)) && ((available) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("d4e7159a-a80e-397c-98c1-91a59ce72f08"))) ? (((available) < (0)) || ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("47262f0f-3294-3ebb-9146-506e8816c48d"))) ? (((-1) != (0)) || ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("f4792b1e-5f07-3928-a9cb-1edc24d2a4c6"))) ? (((-1) >= (0)) || ((-1) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d638597-b085-3197-96a4-7efb8dd98c7f"))) ? (((available) > (0)) || ((0) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("8bdaa114-bbfa-3d25-bb08-31f2592c2748"))) ? (((-1) > (0)) && ((-1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("d553f891-d4ef-3d1e-90d2-48067064de63"))) ? (((available) >= (0)) || ((available) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f12dc947-b2cb-3e9b-bd3e-f56c00ce713a"))) ? (((-1) > (0)) || ((available) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("40f49111-5f6b-3d64-be00-5332cfe574ea"))) ? ((0) < (size)) : (((KnobRuntime.check(java.util.UUID.fromString("fe98513b-2813-3231-802f-f7ea2e679714"))) ? (((-1) == (0)) && ((available) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9546badb-05e6-3094-b2a6-4ba31a7c13f6"))) ? (((available) > (0)) && ((0) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("9b27dd4e-2701-3250-b896-1195b638f3f7"))) ? ((available) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("3fc7752a-b73a-393d-9641-f7d9eb56acda"))) ? ((1) >= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("7f8e844c-4075-36b7-8af5-4ea3bdeac390"))) ? (((-1) == (0)) || ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3d02b681-8f0d-35e9-a968-710afb0b6709"))) ? (((-1) <= (0)) || ((0) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("8df31174-353b-38eb-b92c-dad2d111b896"))) ? (((-1) >= (0)) && ((available) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9a1c37ec-6dd2-3ce3-b9b2-c84c112e6ef0"))) ? (((available) < (0)) || ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("5783f684-a3ef-3eb1-8dac-5a1c9256d036"))) ? (((available) > (0)) || ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("26e0f93e-d68f-3ee7-ba5a-c2cc8fbe4e29"))) ? (((available) >= (0)) && ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("cb627a61-4808-3676-934a-439623f58ee0"))) ? (((-1) <= (0)) || ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ea5c4153-f09a-31ae-a03a-f711f145a08f"))) ? (((available) <= (0)) || ((0) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("fb614ed4-f247-3752-86f5-e7e358109ffa"))) ? (((available) < (0)) || ((available) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("b071f0ea-950b-38e0-8a2a-d646a386b5ec"))) ? (((-1) != (0)) && ((-1) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("919f6abc-be4f-3c3f-8b09-cf79d7add951"))) ? (((-1) <= (0)) || ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("5b3b0b7d-78a7-3c25-a4d4-ec6c01bcab04"))) ? (((available) >= (0)) || ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("ae081c87-45c1-32c1-afce-fc8c6802faad"))) ? (((available) >= (0)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e8e6907a-684c-3427-9196-0fbe640abfca"))) ? (((-1) > (0)) && ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d5cc6f1-d82c-3690-9205-4c0f1e7f27b1"))) ? ((available) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("6aae5842-2de5-3a50-8400-cb3179354257"))) ? (((available) != (0)) && ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("cb5af4cb-c739-3340-897b-76ccfd67c61d"))) ? (((-1) < (0)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("72fc6d04-9c67-3744-a0ec-c3b605e68c19"))) ? (((-1) != (0)) || ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("30451e7a-65cc-3d3f-90ab-3d72a12cad62"))) ? (((available) > (0)) || ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("339ab97b-f358-345a-a99a-d234b74d577b"))) ? (((-1) < (0)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("deabd665-4b05-3ddf-93f7-4bbccaa4f4ab"))) ? (((-1) < (0)) && ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a7ac8fc9-304b-315b-89f7-cec9b920d20f"))) ? (((available) >= (0)) && ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cfe4f7c6-2a05-3b2d-8c07-229320f2279f"))) ? (((-1) > (0)) && ((-1) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5fc36fa5-93fc-3033-b37b-7eb78e08b659"))) ? (((available) <= (0)) || ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("56512985-776d-3e1b-ae60-17e33fcd84e3"))) ? ((available > 0) && ((-1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("4e7e9d58-dd8a-3cbe-a30c-c0499ea9ed5f"))) ? ((available > 0) && ((-1) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39fd4eff-b602-3bcc-a4c4-6834cce04ea8"))) ? (((available) != (0)) && ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("fad63a18-3b24-3548-9503-e4c323f3388c"))) ? (((-1) < (0)) || ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4cb70b1a-f8bc-336e-ae2c-7d3d66e2e755"))) ? (((-1) <= (0)) || ((0) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f973c37d-7993-3e98-b847-a9db8c6de5f0"))) ? (((available) != (0)) || ((0) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("89be859d-a4a0-31d0-b38a-ab2f699f229d"))) ? (((-1) != (0)) || ((0) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4b69071d-a662-3e4f-b095-170361e01285"))) ? (((-1) > (0)) && ((available) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("b2ca70de-403d-38ae-abb2-ea7ee2c3593f"))) ? (((available) == (0)) || ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c7f1f063-5ca7-3334-a467-31b6406cc8b7"))) ? (((available) >= (0)) && ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("fa3ebb2b-0881-3b23-b6e4-d27c8a72e8be"))) ? (((available) == (0)) && ((available) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("9e4f6236-b8a0-377c-a242-c46848f87f24"))) ? (((available) <= (0)) || ((0) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("80b828af-3bc9-3ff5-9108-d4c72759ab0b"))) ? ((available) == (size)) : (((KnobRuntime.check(java.util.UUID.fromString("e0b5f415-e52a-352b-acd1-e57f92b2405f"))) ? (((-1) <= (0)) && ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("ac775457-7f39-36ee-9bf8-58608abd9869"))) ? (((-1) >= (0)) && ((1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("61261526-1cc8-3769-b25c-8cd79fab297c"))) ? (((available) <= (0)) || ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("638b6dfd-53cb-3943-8c00-17b92709818a"))) ? ((0) >= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("b5feb9eb-48c3-3871-b395-d6b77829085d"))) ? (((available) == (0)) && ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("f21fc938-c6cb-3f18-8ae1-7f5087302147"))) ? (((available) == (0)) || ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c45524c0-eac3-331b-9e08-046c7d23cef7"))) ? (((-1) >= (0)) && ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("5964298c-246f-3c5b-8e47-82a004058ad1"))) ? (((available) != (0)) && ((1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("795624e2-3913-3bb4-a043-69c21a2df243"))) ? ((available > 0) || ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("bbf46e40-e3ec-3ae5-a64b-2aad1376be6e"))) ? (((-1) == (0)) && ((available) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0cab0902-6e8e-3a79-8c43-d0182df4841a"))) ? (((-1) == (0)) && ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a195fff6-fa2c-30b3-a219-84dace89e319"))) ? (((available) == (0)) || ((available) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("357d1208-de35-3d54-a61d-798bff03c16b"))) ? (((available) >= (0)) || ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e2d4ecef-41f5-3cb7-9c2a-59e90bb988d9"))) ? (((available) < (0)) && ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a8959d15-a019-3fcc-9616-442ccb99ee70"))) ? (((available) > (0)) && ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("6695ef91-28e4-3864-a47b-bfc3c1320ada"))) ? ((available > 0) || ((0) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("67b6ace6-a5cc-39f0-b98f-d17aec274764"))) ? (((-1) <= (0)) && ((1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("bcadf0c4-18ef-3b26-b117-6b80b14f8e29"))) ? (((available) >= (0)) || ((0) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dae7b433-97bb-3109-8419-34459226a7c0"))) ? (((available) != (0)) && ((1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("1a04782a-aad8-3279-b135-11d0cdcc7b48"))) ? (((-1) < (0)) || ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("433bec06-a1ab-394b-b41a-81601bee0603"))) ? (((-1) != (0)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("6d1c2734-ad7a-3306-b412-c563154888a3"))) ? (((available) != (0)) || ((available) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("40e76e61-7273-388a-b45a-60d3dd5cc49e"))) ? (((available) >= (0)) && ((available) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("75e3e7f3-953a-3a16-a0b0-4f7b2bb42408"))) ? (((-1) == (0)) && ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("4a4b51b7-3f43-393f-8c64-902f848dcded"))) ? (((-1) > (0)) && ((1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("9f156a72-f67d-389f-a5ad-cc73da0d6a3f"))) ? (((available) < (0)) || ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ea6afab1-fc1e-3723-a49b-833a72bdf151"))) ? (((available) > (0)) && ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("9ce252df-0e5c-33c7-9124-baed9c8d835c"))) ? (((-1) < (0)) && ((0) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("23ba1b99-8f58-3bf4-8e54-0c47c2e0d0bd"))) ? (((available) < (0)) || ((available) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("097c2ab1-6ac9-392f-ac7c-0385f20d8561"))) ? (((-1) > (0)) || ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("ad7a544a-a606-306a-9c16-e63d943d7051"))) ? (((-1) <= (0)) || ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("70cb80ac-99b3-370e-8e2b-6ab4c74dffa8"))) ? (((available) == (0)) || ((available) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0a12dd3b-fbdd-3f7d-bbad-ee5f9c2c1f63"))) ? (((-1) == (0)) || ((-1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("d332c220-bf32-3016-909e-9bbf5a6a9bb9"))) ? (((-1) == (0)) || ((0) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("e3748fd2-5a02-358e-bd0f-56a1d1975857"))) ? (((-1) == (0)) && ((-1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("bedffb2a-39ec-311c-aca4-0d1f8652c126"))) ? (((available) == (0)) && ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("0c16595d-9a32-3c01-9387-046c834bca4c"))) ? (((available) > (0)) && ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("23344e89-90d9-3f22-9551-bd7e287124c7"))) ? (((-1) < (0)) && ((0) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("45d99868-2b63-3f5e-8628-ec414cd420f2"))) ? (((available) != (0)) || ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("9be6ec45-66f9-33c0-b486-e5440ab6da44"))) ? (((available) >= (0)) || ((-1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("bbbb713c-0c8e-3c6a-82f3-0f6cf08533fc"))) ? (((-1) != (0)) || ((available) == (1))) : (((KnobRuntime.check(java.util.UUID.fromString("2d1146f0-6d11-3a28-b975-babd97caa365"))) ? (((-1) != (0)) && (available < size)) : (((KnobRuntime.check(java.util.UUID.fromString("374aedb3-f3a6-3a55-a60e-7f8a6e1e8572"))) ? (((available) >= (0)) || ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bd592b24-9621-35fb-86c7-02afc2a21a04"))) ? ((-1) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("0f5ce162-9d3c-3d18-bcf1-23f93b55ea1f"))) ? (((available) == (0)) && ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("c07ca8f6-3091-30df-9f78-388e9b447b9d"))) ? (((-1) > (0)) || ((available) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cccfdebd-f696-3578-8dde-604157e63ee6"))) ? (((-1) > (0)) && ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7a67e9cf-b397-32a1-bcfc-a5d59dd6f73b"))) ? (((-1) > (0)) && ((available) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("12dbe4bb-a932-3280-9656-bab6f5af8c3a"))) ? (((available) == (0)) || ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("395c85ed-3b6b-363d-9fdb-9de1f5a40de6"))) ? (((-1) > (0)) && ((-1) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f094bb1a-db52-3b3d-8fec-98109408f017"))) ? (((-1) > (0)) && ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4908115e-ddb6-3860-abab-76bc3f90ea3e"))) ? (((-1) == (0)) || ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("f5919515-9af0-3d0d-9e24-015819e7f1b1"))) ? (((available) <= (0)) && ((-1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("20d99319-4873-3e8d-8c86-4e76be0ab9bf"))) ? (((available) == (0)) || ((0) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("4a4347b2-583a-357c-abfc-7e2a8c1aab95"))) ? (((available) < (0)) || (available < size)) : (((KnobRuntime.check(java.util.UUID.fromString("36ff592c-53c4-3988-bdff-1288715adfce"))) ? (((-1) >= (0)) && ((available) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("74f1dc2e-35cf-31a2-956a-513ca42a7a71"))) ? (((available) <= (0)) || ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("252a9d69-5452-3cc8-9bb9-9c7596d821b7"))) ? ((available > 0) && ((-1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("fdb8ec2b-1c44-3096-aaed-8e2a119e42ad"))) ? (((-1) <= (0)) && ((1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("7422ffec-97fb-3554-9ae6-51806da47f30"))) ? (((-1) == (0)) || ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("e6e78354-68f7-3854-84cf-7a9370f880d2"))) ? (((available) >= (0)) || ((1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("bc736d1b-a981-34d1-a71c-5f78587cd57d"))) ? (((available) == (0)) || ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("d93fd71d-9af0-3156-bf7f-57605fab24fe"))) ? (((-1) <= (0)) || ((1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("7d49ce12-c96f-3108-8dd7-f0d26d0505ed"))) ? (((-1) < (0)) && ((available) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("3c13ef9f-7238-32c1-bc7b-0f490db7d547"))) ? (((-1) > (0)) || ((available) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fb81fb0a-31d8-3c2e-a27e-56172393876b"))) ? ((-1) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1b4e8df9-c016-31e9-ba6e-995e23fcc23a"))) ? (((available) >= (0)) || ((available) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e7d5190c-63b0-3c04-a80c-c58be687f034"))) ? (((-1) <= (0)) && ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("107fed3a-0ca5-39e5-add0-8723b2882539"))) ? (((available) < (0)) && ((1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("0d702d80-fc71-3c14-9797-1381ac8d822d"))) ? (((available) != (0)) || ((-1) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eaf9cc2d-62dd-37a4-903f-ea3486ac6c8e"))) ? (((available) == (0)) || ((1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c90b2e61-688f-3f24-b8d8-e38700a6b9f2"))) ? (((-1) > (0)) && ((0) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a21e9e2b-5b6c-3706-ac2a-fdbb13a31fae"))) ? (((available) > (0)) && ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("ceeed2e0-8257-3088-b028-6f3ea37a3cef"))) ? (((-1) > (0)) || ((1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("26fb2561-c751-38bf-997d-345f044cc8d4"))) ? (((-1) >= (0)) && ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("54fbc9cd-4129-3f3f-b59d-488fb9fa0255"))) ? ((available > 0) && ((-1) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f157e75-3e85-3ddc-9a7d-c8d833959603"))) ? (((available) != (0)) || ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("40d01217-9afd-3941-b9c9-aeecd438d7a6"))) ? (((-1) <= (0)) || ((available) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("30c7bf5c-a98a-358c-9aa1-a31e04cabadd"))) ? (((available) >= (0)) || ((-1) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c6f823af-0996-38d9-8363-c45ece791f15"))) ? (((available) > (0)) && ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("481392c1-8efc-3b0a-9a46-429e30bd2fed"))) ? (((available) == (0)) || ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("781b6ca1-79a5-3ee5-ad28-d42ed9fd5c59"))) ? (((-1) > (0)) || ((available) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("30825148-0370-30ca-8540-875d677f7670"))) ? (((available) <= (0)) && ((available) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("5ae7882d-b3b5-30f2-8cc2-1a94a28677ae"))) ? ((available) >= (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("7d123dc8-312f-39d1-9a9d-b18ab66df2ff"))) ? (((available) <= (0)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("2a36b456-f20a-3c88-87f0-04fca2b89fde"))) ? (((available) != (0)) || ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("7085f38e-b137-3e70-8631-a33e6c6fb7a1"))) ? ((1) <= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("64ad199a-7661-3e9c-9d78-1ba3af367451"))) ? (((available) <= (0)) || ((available) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a7050a0b-af53-316a-bc8c-70e954c654b7"))) ? (((-1) < (0)) && ((available) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("705bdcd6-54fa-3fa2-8a25-a157d7fbc3b1"))) ? ((available > 0) || ((available) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("fe164a42-0d6e-3bc5-807a-d7150fd7b2e1"))) ? (((-1) != (0)) || ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("abe10135-c3ca-3b75-9ff5-c051ae34e61f"))) ? (((available) > (0)) && ((available) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3b8ee85-a5e3-3927-a123-1b3bd56d25c6"))) ? (((-1) > (0)) && ((0) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("4c5891d9-73a8-346a-ae09-d3b72a2d6c68"))) ? (((available) > (0)) || ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("4108db06-dc6d-3f50-8d81-3e0332016d17"))) ? (((-1) >= (0)) || ((0) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("fae4009a-682f-3a76-804a-14030a58995f"))) ? (((available) <= (0)) || ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("cf04c11b-4615-3f9e-9de4-832485517e39"))) ? (((available) <= (0)) && ((available) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("39cefcd7-4516-3f13-ad77-48b333d27717"))) ? (((-1) > (0)) || ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("e8da0a9d-3180-33f4-88db-eb66cd372de4"))) ? (((-1) <= (0)) && ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("009d3df2-7106-3280-956b-35f97fd2f455"))) ? (((available) >= (0)) && ((0) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("15fe0315-6037-3916-8c86-cf7e0c2cd330"))) ? (((available) <= (0)) && ((0) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("28629e01-b945-3cdf-a4f6-5d73c2796970"))) ? ((-1) != (size)) : (((KnobRuntime.check(java.util.UUID.fromString("40aeb3ad-13a0-3450-b4bd-b32f92b57fad"))) ? (available > 0) : (((KnobRuntime.check(java.util.UUID.fromString("55190135-1a2e-3cdc-b043-a03f1365f760"))) ? (((-1) >= (0)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("579cbda5-5935-311b-b2ce-58cbb58d12ce"))) ? (((-1) == (0)) || ((0) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a738030b-4a76-3561-879e-fc194ae9330d"))) ? (((-1) >= (0)) || ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("8f4ab677-e8be-3aa2-83e4-606018f3ddee"))) ? (((available) < (0)) && ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("5c7b50f4-118a-3e06-9247-58389929802d"))) ? (((-1) < (0)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("646faff8-07b1-3da3-9f59-00e97067443d"))) ? (((available) <= (0)) || ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("c5e357b3-67b9-34e5-8b5c-d785a51f939a"))) ? (((available) <= (0)) || ((available) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("7a6c209f-8024-3611-a568-e86bbe945b52"))) ? (((available) <= (0)) && ((1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("cb4b6b52-a643-3c72-b140-d2ba11f9ddd0"))) ? (((-1) <= (0)) || ((available) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("cbebc1ef-8b6d-3713-ac4a-6648e11bcba8"))) ? (((available) >= (0)) || ((-1) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("c7ac1e5d-a35f-34e9-8d4f-2eca9d74fb5c"))) ? (((available) == (0)) || ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("b9a9f9da-a0d6-3a0f-8e9a-88dae7989d62"))) ? (((available) >= (0)) || ((-1) < (1))) : (((KnobRuntime.check(java.util.UUID.fromString("bdb9ad58-a1af-3ac1-9bd8-e255ce169444"))) ? (((-1) <= (0)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("3ab869b3-1189-3b3d-9656-5611ce4659ef"))) ? (((available) <= (0)) && ((-1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1646765d-72f3-30f4-8962-70bc84ffc3b5"))) ? (((available) <= (0)) && ((1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("94fa3905-27d7-393d-b009-86d5c0d37a83"))) ? (((-1) != (0)) || ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b308343e-75d6-3bb6-83b2-f5a301f9edd0"))) ? (((-1) >= (0)) || ((-1) > (1))) : (((KnobRuntime.check(java.util.UUID.fromString("fbe875b3-456c-3280-b699-d7dd9ccf1426"))) ? (((-1) != (0)) && ((available) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("af7bd171-2fa8-34a9-8fc6-e431d3129263"))) ? (((-1) != (0)) || ((-1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("06ea52db-e664-37f5-ba8b-b446c0f57431"))) ? (((-1) == (0)) && ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("a040258d-1a13-3c4c-ba60-aac8f36954ab"))) ? ((available > 0) || ((-1) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("6a039f5d-c9b9-3dd3-a2f7-dcd88f7541a0"))) ? (((-1) != (0)) && ((-1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("4a167b1a-64b5-31be-bda6-7287eb30a1e3"))) ? (((-1) >= (0)) || ((-1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("c0b548b4-1e75-3051-871d-db58b6ab2121"))) ? (((available) <= (0)) && ((-1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("12f76606-1c9e-30de-a203-60ec3fe8e4b3"))) ? (((available) < (0)) || ((1) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("71b9fb75-b378-3228-a934-553a0525ade7"))) ? (((-1) > (0)) && ((available) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("069c4795-e534-3d3a-9e7f-4d0f78ce4318"))) ? (((available) != (0)) || ((0) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a5e406cc-c3e2-3902-b237-01b71623de1d"))) ? ((available > 0) && ((1) < (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("adc9a93e-8701-3271-83b6-a64d876853fd"))) ? (((-1) == (0)) && ((1) > (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("af97f125-f1b0-301a-8ab1-5ba113f0b7d5"))) ? (((-1) == (0)) && ((-1) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("c56e6375-39ee-35e1-a5a2-1ba780608e1d"))) ? (((available) == (0)) || ((0) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a6661bda-2a61-3d34-a6c0-d044b96905a9"))) ? ((available > 0) && ((-1) != (1))) : (((KnobRuntime.check(java.util.UUID.fromString("6e6ebe11-78f0-376c-aaf6-f1939a628269"))) ? (((-1) < (0)) && ((1) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("5cd7bd0e-4c92-3243-8143-7e093109289f"))) ? (((available) <= (0)) && ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("d2d888f5-f38a-387b-bd7a-1e67a613b4eb"))) ? (((available) > (0)) && ((-1) == (size))) : (((KnobRuntime.check(java.util.UUID.fromString("5f260824-55f9-3fed-a219-7ab7bfd83be1"))) ? ((available > 0) && ((-1) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("6b3be115-b370-31e4-b33e-2ffa6ced4261"))) ? ((available > 0) || ((available) <= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("ea35f0b1-457c-37ef-ad18-12ab588e5b5d"))) ? (((available) >= (0)) || ((1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("3016149e-b309-355d-864a-7bdac387afb7"))) ? (((-1) >= (0)) || ((0) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("d5017083-b428-37d6-a5f1-c0e7eb26dcd1"))) ? (((-1) < (0)) || ((-1) <= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("a0d70bf2-bacb-3267-a9d0-e5876014f652"))) ? (((available) >= (0)) && ((-1) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("144fbc3f-d640-37c2-a9dd-c8bfbad6ce66"))) ? (((-1) == (0)) && ((1) < (size))) : (((KnobRuntime.check(java.util.UUID.fromString("fc663900-45c7-3b74-87c7-ffdcc5b3c9fe"))) ? (((available) <= (0)) || ((available) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("849abe9c-552e-38fd-907e-c03dfb2fc61e"))) ? (((available) >= (0)) || ((available) >= (size))) : (((KnobRuntime.check(java.util.UUID.fromString("72b0b1e5-2d79-3e16-9d26-8f11ab7347e6"))) ? ((available > 0) && ((-1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de14b285-e078-3c75-9097-52c92d633057"))) ? ((available > 0) || ((0) <= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("3d9096be-d154-3f8b-b308-845edc5f9125"))) ? (((available) >= (0)) || ((0) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("d802065c-d502-38ba-8e65-8eecaf1ce174"))) ? (((available) == (0)) && ((-1) >= (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("67c9d803-f3b3-37d0-8455-272e27c2b026"))) ? (((available) <= (0)) && ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("043ce2ad-38ab-37d1-b6ef-ab7fbbb84e8c"))) ? (((available) == (0)) && ((1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("e26666f6-8180-3828-a828-cf9f258da059"))) ? (((available) < (0)) && ((-1) != (size))) : (((KnobRuntime.check(java.util.UUID.fromString("f841d190-feec-3edf-bccc-bfdfea1bc169"))) ? (((-1) <= (0)) || ((available) >= (1))) : (((KnobRuntime.check(java.util.UUID.fromString("45b5d490-0f1b-330d-a55f-c72456312289"))) ? (((available) >= (0)) || ((available) > (size))) : (((KnobRuntime.check(java.util.UUID.fromString("664dcd30-8bf4-36b9-a391-6359a5093813"))) ? (((available) >= (0)) && ((-1) == (-1))) : (available > 0 && available < size))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.info("Available stream not enough for edit, available={}, entry size={} at offset={}",
        available, size, getPositionQuietly());
      return KEY_EOF_AND_RESET;
    }
    WALProtos.WALKey walKey;
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("40995e80-026d-3b68-9e13-9af328ff477c"))) ? ((-1) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("06ceaf1c-c276-3d6f-a7c5-d1eeeb2b7caf"))) ? ((-1) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7173d4db-8b55-372d-89ed-b671b4232869"))) ? ((available) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("22d58582-3e60-3299-b73f-aedebba4e391"))) ? ((-1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ed6c43a8-47e4-3b03-a6ec-cee22d19ebee"))) ? ((available) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("624320ff-efc9-3b5c-aaa1-cd77d2b14df8"))) ? ((available) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("aab388c7-59bb-300b-bab9-4c60be3358c0"))) ? ((available) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bcef5b06-8516-3f16-9d4a-cf8c0fe3eddc"))) ? ((available) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("683686c7-b709-3e6b-b4bb-89444970d26c"))) ? ((available) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("be38a526-8fc7-3c7b-bb35-d8c74eb7b06c"))) ? ((-1) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1d4c6eca-2da8-3d28-8606-41419a93bf5c"))) ? ((-1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("00b5e757-7681-3f71-8325-4c5c7e5da419"))) ? ((-1) <= (0)) : (available > 0))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("7d62771d-9d99-30f0-ace9-c85a062c95d7"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b3377a7-d9ca-3f85-af9a-d15cef388257"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3511c6b3-0d26-342b-a990-34ce185e2820"))) {
size -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("19a4281e-60cf-3d00-bf4a-974142e5114c"))) {
size *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6fe9e8c4-d674-3870-97a8-963bdc0aaae4"))) {
size /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ea527ca-e43d-32b2-b1df-3fbbbce9980b"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("84b46955-9115-3e71-afbb-1c971ba54c7c"))) {
size = 0;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("ca357177-b6e5-3708-9275-8ccffd6eb239"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06e61d98-6669-3e16-ad6d-bf7f4edb14a8"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d70edd70-cba6-3439-bc29-44abe8bde7dd"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("nonceGroup_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b66b73be-6cf9-3991-8075-195e932c3e5d"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1209fb9a-36f4-3f86-9f1c-1812754c0410"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("writeTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("09fd4292-4314-3ae9-9851-c130c868552c"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("nonce_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("93d4df0f-2358-3d92-bb55-89d0875641f9"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f5658a6-97fc-36ca-b1a6-0a99f9ad9626"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("caac2999-60cb-3c7b-9da0-fc785a5ac08c"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("logSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a051bfb2-f293-3372-b21a-05c15088ab7b"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("922ca4d8-f78b-3620-9aea-aa56479f8f37"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b5fe56e2-cbec-344c-8cf7-fcfdfc0802ba"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("origSequenceNumber_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(walKey));
    field.set(walKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17b9c7a5-f92c-356b-836f-27ca7985d460"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9ae6d51-ebf2-37e9-bdd1-62698076173c"))) {
try {
    java.lang.reflect.Field field = walKey.getClass().getDeclaredField("followingKvCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walKey));
    field.set(walKey, oldValue - 1);
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
    if (((KnobRuntime.check(java.util.UUID.fromString("0c2c64bb-2a2b-30f1-97ba-009343f077ee"))) ? ((followingKvCount) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b3210ebd-55e2-3367-bc6a-4ac640a09ace"))) ? ((followingKvCount) != (0)) : (followingKvCount == 0))))) {
      LOG.trace("WALKey has no KVs that follow it; trying the next one. current offset={}",
        posBefore);
      return new Result(State.NORMAL, entry, posBefore);
    }
    int actualCells;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("c83f24f7-8507-3522-9788-f077a9e7c3c1"))) {
followingKvCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d343ff25-22b4-3422-9312-0be173c466e6"))) {
followingKvCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6978fae4-129e-3d21-9f9e-4ac1dbca2f6c"))) {
followingKvCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("62f44aee-c765-3585-9a06-c11624d92287"))) {
followingKvCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bd3e123c-4359-3fc2-b87d-64707054cc8a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c65104a1-d8d3-39cb-8eea-340d495a5d99"))) {
followingKvCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c54ee6d-26e9-358a-95a2-4466f80314dd"))) {
followingKvCount = 0;
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("8ce304b1-2b07-38fd-9ae7-32a275ed8340"))) ? ((actualCells) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c51f4dd0-a969-3d2a-86bc-e709f8db00a9"))) ? ((actualCells) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d36b81a6-859d-37be-a6a5-94fda7381a06"))) ? ((0) == (followingKvCount)) : (((KnobRuntime.check(java.util.UUID.fromString("7e882d1f-a8bd-36d7-9f47-1a876c162e70"))) ? ((actualCells) == (followingKvCount)) : (((KnobRuntime.check(java.util.UUID.fromString("1af55efc-566a-3119-a041-2eb5f0ef66af"))) ? ((actualCells) != (followingKvCount)) : (((KnobRuntime.check(java.util.UUID.fromString("86e3eb4e-2665-3f66-9959-c368879cc45a"))) ? ((0) != (followingKvCount)) : (actualCells != followingKvCount))))))))))))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("800ae5d8-01f9-35fc-9ac0-f1177936968b"))) ? (posAfter > this.walEditsStopOffset) : (((KnobRuntime.check(java.util.UUID.fromString("350888ae-e0d5-35da-908e-d36b30f29a03"))) ? ((trailerPresent) && ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("21e3a661-dc1e-30a5-997a-bb0bee7fae24"))) ? ((posAfter) != (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("9ef6221b-7c99-3a5e-9e3a-ef15bcd4b2df"))) ? ((trailerPresent) && ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f06ba948-5041-3617-ac0d-18493af9df94"))) ? ((trailerPresent) || (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("c450634f-258d-37a0-bd42-540af35804fd"))) ? ((!trailerPresent) && ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("669b6ace-c7db-3f73-9a8b-76d732a0b7fa"))) ? ((!trailerPresent) && ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4d32546b-f955-357e-aac7-3aa2db8d2faf"))) ? ((trailerPresent) && ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("9c33ee06-b424-3de0-9b7f-e69e1c9057e8"))) ? ((!trailerPresent) && ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ae72393a-39c1-33a1-bd95-deea6b651cf0"))) ? ((!trailerPresent) && ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("28ce32a2-93bf-3b0e-a848-c630c5816e77"))) ? ((posAfter) <= (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("760bf014-743b-3b32-9a0d-42131af63f37"))) ? ((!trailerPresent) || ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("1afffe5c-248b-3224-a547-cc67a6970722"))) ? ((!trailerPresent) || ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("6716b513-aff8-3132-beae-bfb5a660ceb1"))) ? ((trailerPresent) && (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("bc8f50eb-85c9-387b-a27a-3dd95ddfe03a"))) ? ((trailerPresent) && ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f7af7278-a4b4-36ef-bbd6-a862ddb55bed"))) ? ((!trailerPresent) || ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ae95daf4-e268-311e-a25a-d771938cffc8"))) ? ((trailerPresent) || ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("67e27d97-faa8-3553-be80-ca3533bce46a"))) ? ((trailerPresent) || ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("dab5d2ec-43cf-3508-a253-0bbf4217855e"))) ? ((trailerPresent) || ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3dde22b0-7e78-3ecf-abde-c498434e2681"))) ? ((posAfter) < (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("b8090855-7736-3281-9b08-730031c0f775"))) ? ((posAfter) >= (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("444afb4d-7948-3eea-aad4-4609e1576d41"))) ? ((!trailerPresent) || ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("bd7b870f-0d44-3683-ad6c-30e0a50ee119"))) ? ((posAfter) == (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ac0b9a5c-2625-3adf-8517-dc14e70cf9b3"))) ? ((!trailerPresent) && ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e61112f0-2c70-386c-ab92-290e8dae8299"))) ? ((posAfter) > (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("331bfa1d-afc6-39a2-b019-29f1b2b8f9bc"))) ? ((trailerPresent) || ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c96fa1c2-7605-31cf-a8df-5d6b33a3d2a8"))) ? ((trailerPresent) && ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("7e8e5fb0-9014-35b5-83db-d7f4a2adfe14"))) ? ((!trailerPresent) || ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("b6329b5d-a47f-3e23-85b3-e972f034d73e"))) ? ((!trailerPresent) && ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("56237f42-04ef-30c6-9230-189da764b42f"))) ? (!trailerPresent) : (((KnobRuntime.check(java.util.UUID.fromString("7b1f6053-6d5f-30ce-99c5-b4af9eb62415"))) ? ((!trailerPresent) || ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3ef17651-8993-338e-b890-667fc040f016"))) ? ((!trailerPresent) || (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("1544d095-814b-3927-888a-b4fe097a50ef"))) ? ((trailerPresent) && ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a2ba4929-bfff-345a-a182-c3e7733ffc38"))) ? ((trailerPresent) || ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e347b615-79ce-3d31-bac3-c77bce132f30"))) ? (trailerPresent) : (((KnobRuntime.check(java.util.UUID.fromString("649fccef-ac9e-31a0-9299-06b2067e09f4"))) ? ((trailerPresent) || ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("ce78e99a-6e1d-3a41-8f3e-ab7d5ef44964"))) ? ((!trailerPresent) && (posAfter > this.walEditsStopOffset)) : (trailerPresent && posAfter > this.walEditsStopOffset))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("49de0659-ad4b-3260-91e6-2c53877b1a04"))) {
throw new java.io.IOException("Injected exception");
}
      originalPosition = inputStream.getPos();
    } catch (IOException e) {
      LOG.warn("failed to get position", e);
      return State.EOF_AND_RESET.getResult();
    }
if(KnobRuntime.check(java.util.UUID.fromString("00eca9fe-3b12-3df8-a4e6-a038e9ac64a0"))) {
originalPosition += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("58af20c3-c4a4-34e9-bc94-9fae3d386a91"))) {
originalPosition = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("72f0acfc-b6f7-3c3d-97c7-f6dbaa97b1f5"))) {
originalPosition = -1;
}
    if (reachWALEditsStopOffset(originalPosition)) {
      return State.EOF_WITH_TRAILER.getResult();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("9f7f578c-9429-3856-b80d-ca4e5bc36d95"))) ? ((limit) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c56e27ae-2664-35d5-ba2e-371207fa3469"))) ? ((limit) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("aaa330ce-5f13-3c97-8b6e-bca96b073e12"))) ? ((limit) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bc7cddad-49fe-3006-9c89-353ad5bc1624"))) ? ((limit) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a56a9d91-cbee-3646-93bb-7e7f647a2c12"))) ? ((limit) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("374f68c1-67a7-33fb-b2a9-ea164803f99a"))) ? ((limit) < (0)) : (limit < 0))))))))))))) {
      // should be closed WAL file, set to no limit, i.e, just use the original inputStream
      delegatingInput.setDelegate(inputStream);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("63c081cc-47f8-35d9-b492-e2fad7d04d84"))) ? ((limit) > (originalPosition)) : (((KnobRuntime.check(java.util.UUID.fromString("c7ca98b1-e7e6-3cf3-9ccd-94f0ee43c0ee"))) ? ((limit) <= (originalPosition)) : (((KnobRuntime.check(java.util.UUID.fromString("68378c34-d12a-341b-b1d4-83dfc9342237"))) ? ((limit) < (originalPosition)) : (((KnobRuntime.check(java.util.UUID.fromString("5a5acc40-e9fe-3fe7-89de-1196c741e314"))) ? ((limit) != (originalPosition)) : (((KnobRuntime.check(java.util.UUID.fromString("43ce6e35-b428-3769-a2b7-0ef6aa7072d4"))) ? ((limit) >= (originalPosition)) : (((KnobRuntime.check(java.util.UUID.fromString("9eece8c5-13d2-391a-b21c-21a0fd7c62ec"))) ? ((limit) == (originalPosition)) : (limit <= originalPosition))))))))))))) {
      // no data available, just return EOF
      return ((KnobRuntime.check(java.util.UUID.fromString("466ef508-c3b1-3f72-8399-c9381f492682"))) ? (editEof()) : (((KnobRuntime.check(java.util.UUID.fromString("5f4f3e7a-beb9-361b-a6c3-9af810d82fcd"))) ? (editError()) : (State.EOF_AND_RESET.getResult()))));
    } else {
      // calculate the remaining bytes we can read and set
      if (KnobRuntime.check(java.util.UUID.fromString("b270d3e6-df34-38e9-9310-15df75b299f5"))) { delegatingInput.setDelegate(ByteStreams.limit(inputStream, (limit) - (originalPosition))); } else if (KnobRuntime.check(java.util.UUID.fromString("bdb48108-ff63-3f71-97d8-a03f9ffa2c31"))) { delegatingInput.setDelegate(ByteStreams.limit(inputStream, originalPosition)); } else if (KnobRuntime.check(java.util.UUID.fromString("da0842b0-ecb5-31c4-a7e2-165cb3bdc843"))) { delegatingInput.setDelegate(ByteStreams.limit(inputStream, limit)); } else { delegatingInput.setDelegate(ByteStreams.limit(inputStream, limit - originalPosition)); }
    }
if(KnobRuntime.check(java.util.UUID.fromString("9d7f43ad-4bca-322a-9187-daea6024e5b9"))) {
originalPosition = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1689986a-dc3a-3784-be78-d788a8e05d2c"))) {
originalPosition = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ff3eaea-f537-31d2-9667-9f311918ebef"))) {
originalPosition += 1;
}
    ReadWALKeyResult readKeyResult = readWALKey(originalPosition);
    if (((KnobRuntime.check(java.util.UUID.fromString("6ec03199-f7b1-3632-b7f8-d9d4ac0a3947"))) ? ((readKeyResult.state) != (State.NORMAL)) : (((KnobRuntime.check(java.util.UUID.fromString("566ef31f-56d8-3658-b258-89d0c05a9771"))) ? ((readKeyResult.state) == (State.NORMAL)) : (readKeyResult.state != State.NORMAL))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("51409195-6097-347b-be10-1c72999f038d"))) ? (editEof()) : (((KnobRuntime.check(java.util.UUID.fromString("b7dc536f-67de-3976-b3bb-ff829136d12a"))) ? (editError()) : (readKeyResult.state.getResult()))));
    }
    return readWALEdit(readKeyResult.entry, readKeyResult.followingKvCount);
  }

  private void skipHeader(FSDataInputStream stream) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b4882e38-1c91-3487-a19b-4045f85ca667"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("af7feed6-5325-312b-95f0-7cd7db0b8564"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("bbd5e5bf-f6b6-38b4-97e1-128dbdd308b8"))) { skipTo(PB_WAL_MAGIC.length); } else { stream.seek(PB_WAL_MAGIC.length); }
if(KnobRuntime.check(java.util.UUID.fromString("095a2e24-82b2-30c0-b211-9cfc175b787c"))) {
throw new java.io.IOException("Injected exception");
}
    int headerLength = StreamUtils.readRawVarint32(stream);
if(KnobRuntime.check(java.util.UUID.fromString("ee39dd10-e038-3478-9c6a-f09993728a70"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("0063386d-cc06-364c-8a72-5480b8c0a7f3"))) { stream.seek((stream.getPos()) + (headerLength)); } else if (KnobRuntime.check(java.util.UUID.fromString("3fd88b41-f6c6-32f1-b4fc-5d320ecfc3fc"))) { skipTo((stream.getPos()) + (headerLength)); } else if (KnobRuntime.check(java.util.UUID.fromString("a8130dd5-5a50-323f-a1cb-782d056f643c"))) { skipTo(stream.getPos()); } else if (KnobRuntime.check(java.util.UUID.fromString("d940c1f2-268e-3689-bea1-a5770507bbec"))) { stream.seek(stream.getPos()); } else { stream.seek(stream.getPos() + headerLength); }
  }

  @Override
  public void resetTo(long position, boolean resetCompression) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3b91a1f2-3d6d-3db7-95ff-351201ca1814"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6be4220a-9a44-3451-aed8-a823fb6bbeeb"))) {
return;
}
    close();
if(KnobRuntime.check(java.util.UUID.fromString("48861213-7772-3ddd-9d22-3e5715d4d2d9"))) {
throw new java.io.IOException("Injected exception");
}
    Pair<FSDataInputStream, FileStatus> pair = open();
    boolean resetSucceed = false;
    try {
      if (!trailerPresent) {
        // try read trailer this time
if(KnobRuntime.check(java.util.UUID.fromString("0f8cfd65-e43f-3c8e-a7d9-9a917f951234"))) {
throw new java.io.IOException("Injected exception");
}
        readTrailer(pair.getFirst(), pair.getSecond());
      }
      inputStream = pair.getFirst();
      delegatingInput.setDelegate(inputStream);
      if (((KnobRuntime.check(java.util.UUID.fromString("9cbac503-0145-3cb3-866f-4e7d16de23f2"))) ? ((position) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bca56cfe-1bc6-3236-8cec-5be740c4d2f0"))) ? ((position) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("74dd7d5f-a09a-3022-a282-ab73e8f1c199"))) ? ((position) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3997537d-4247-3960-a9da-d03a7cbe5100"))) ? ((position) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("954d0f20-bc63-3740-9f69-f8cdb52fee10"))) ? ((position) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5274c787-d829-3323-a350-ad59866f17f6"))) ? ((position) != (0)) : (position < 0))))))))))))) {
        // read from the beginning
        if (((KnobRuntime.check(java.util.UUID.fromString("94b515c4-e680-35aa-ab3c-2531b74afb02"))) ? ((compressionCtx) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8a1ca8b7-4441-398a-b1b5-f30a64755253"))) ? ((compressionCtx) == (null)) : (compressionCtx != null))))) {
          compressionCtx.clear();
        }
if(KnobRuntime.check(java.util.UUID.fromString("8bed4fcf-bf21-3c01-bc44-6fc076b9c62c"))) {
throw new java.io.IOException("Injected exception");
}
        skipHeader(inputStream);
      } else if (((KnobRuntime.check(java.util.UUID.fromString("d489f570-4771-3dff-89ee-d14549fe42c6"))) ? ((resetCompression) && (compressionCtx != null)) : (((KnobRuntime.check(java.util.UUID.fromString("afdaf3b9-a192-3c43-95fb-8dffa9e46805"))) ? ((!resetCompression) || ((compressionCtx) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("01e49660-531a-36fd-be59-b6a84aea379d"))) ? ((!resetCompression) && (compressionCtx != null)) : (((KnobRuntime.check(java.util.UUID.fromString("5f287fcf-785a-3f41-8c1a-b17606b95fc5"))) ? ((!resetCompression) && ((compressionCtx) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1e34242f-973b-361b-885f-08a3350713d7"))) ? ((compressionCtx) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fa4a9099-e480-3beb-a19f-5e1e61810206"))) ? ((compressionCtx) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8a2a604c-1eb1-3a80-9ed0-22ac34c1a25e"))) ? (!resetCompression) : (((KnobRuntime.check(java.util.UUID.fromString("abbb1e48-5ac4-3ce7-8117-682477aac668"))) ? (compressionCtx != null) : (((KnobRuntime.check(java.util.UUID.fromString("f58b54a9-c9ca-3e01-90cc-3a4a0fd44cad"))) ? ((resetCompression) && ((compressionCtx) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9d5b9172-c1f1-3115-b6ce-df5245da9486"))) ? ((!resetCompression) || ((compressionCtx) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4a0195f5-bcec-32eb-a136-86e3988c4504"))) ? ((resetCompression) || (compressionCtx != null)) : (((KnobRuntime.check(java.util.UUID.fromString("fe222744-7214-3b18-93c2-c2ca6a96685b"))) ? ((!resetCompression) || (compressionCtx != null)) : (((KnobRuntime.check(java.util.UUID.fromString("180bdb3d-654e-33c3-8948-2b837d76c4b8"))) ? ((resetCompression) || ((compressionCtx) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4a6f61a4-9e3c-33cc-bdde-1e37ec87525d"))) ? ((!resetCompression) && ((compressionCtx) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("33ec6726-36d5-38c1-8e15-eaffbed77218"))) ? (resetCompression) : (((KnobRuntime.check(java.util.UUID.fromString("211aef38-6a70-3661-8288-19bec8564982"))) ? ((resetCompression) && ((compressionCtx) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8fff2367-d8e0-36d1-af5c-8befdb20aa24"))) ? ((resetCompression) || ((compressionCtx) == (null))) : (resetCompression && compressionCtx != null))))))))))))))))))))))))))))))))))) {
        // clear compressCtx and skip to the expected position, to fill up the dictionary
        compressionCtx.clear();
        skipHeader(inputStream);
        if (position != inputStream.getPos()) {
          skipTo(position);
        }
      } else {
        // just seek to the expected position
if(KnobRuntime.check(java.util.UUID.fromString("535eba9d-3879-33cf-83db-b3c4d5025849"))) {
position += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d4ef8e33-62ab-3cf3-9d5a-c9ca55ae5253"))) {
position = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("37f7d0b8-2d10-3afb-82af-40cab0d91a11"))) {
position = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7444ed7b-a224-3c63-9976-5fb5e50663d7"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("18a957dd-2438-3361-b53e-24198f4f5d69"))) { skipTo(position); } else { inputStream.seek(position); }
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
if(KnobRuntime.check(java.util.UUID.fromString("a63d73ba-86cf-3210-ab6e-40094432303e"))) {
return null;
}
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
