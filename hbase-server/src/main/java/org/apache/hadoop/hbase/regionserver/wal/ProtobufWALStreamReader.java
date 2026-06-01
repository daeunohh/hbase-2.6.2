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
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALStreamReader;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * A one way stream reader for reading protobuf based WAL file.
 */
@InterfaceAudience.Private
public class ProtobufWALStreamReader extends AbstractProtobufWALReader
  implements WALStreamReader, AbstractFSWALProvider.Initializer {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWALStreamReader.class);

  @Override
  public Entry next(Entry reuse) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("84f9f85e-20b3-3206-8e26-6d031ae13678"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("917e85ce-faaf-3f4e-a14b-989b38ee11a0"))) {
return null;
}
    long originalPosition = getPosition();
if(KnobRuntime.check(java.util.UUID.fromString("c1ef9c0a-48eb-36d1-8f4a-fcd9b3ee1809"))) {
originalPosition = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1fb9a0ff-b83e-31f9-960a-7a7254cd66da"))) {
originalPosition = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6853b81f-9f59-3520-9020-e698f11b9590"))) {
originalPosition += 1;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("a0eec073-440a-3edf-ae04-3910860bb9ba"))) ? (isWALTrailer(originalPosition)) : (reachWALEditsStopOffset(originalPosition)))) {
      return null;
    }
    WALProtos.WALKey walKey;
    try {
      // for one way stream reader, we do not care about what is the exact position where we hit the
      // EOF or IOE, so just use the helper method to parse WALKey, in tailing reader, we will try
      // to read the varint size by ourselves
if(KnobRuntime.check(java.util.UUID.fromString("01918264-3cc6-3df9-b4ba-0b3a620af1d1"))) {
throw new java.io.IOException("Injected exception");
}
      walKey = ProtobufUtil.parseDelimitedFrom(inputStream, WALProtos.WALKey.parser());
    } catch (InvalidProtocolBufferException e) {
      if (ProtobufUtil.isEOF(e) || isWALTrailer(originalPosition)) {
        // InvalidProtocolBufferException.truncatedMessage, should throw EOF
        // or we have started to read the partial WALTrailer
        throw (EOFException) new EOFException("EOF while reading WALKey, originalPosition="
          + originalPosition + ", currentPosition=" + inputStream.getPos()).initCause(e);
      } else {
        // For all other type of IPBEs, it means the WAL key is broken, throw IOException out to let
        // the upper layer know, unless we have already reached the partial WALTrailer
        throw (IOException) new IOException("Error while reading WALKey, originalPosition="
          + originalPosition + ", currentPosition=" + inputStream.getPos()).initCause(e);
      }
    }
    Entry entry = reuse;
    if (((KnobRuntime.check(java.util.UUID.fromString("7cfeac19-d87d-3fc6-a101-a1336865d1ae"))) ? ((entry) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c413bb36-b171-3540-920c-4a64e56d7f1b"))) ? ((entry) != (null)) : (entry == null))))) {
      entry = new Entry();
    }
if(KnobRuntime.check(java.util.UUID.fromString("2fa3da63-9b34-32b9-bad5-b553418de88c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("22c40660-d61d-31df-af39-d63e0acc871e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1838ddf8-4479-3b8a-a492-2d51068a06a8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f32e1d46-0f94-31e2-8c93-2786d1e5aab7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b0f579c1-7aac-3936-bda5-3721d51ef0f7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("18f29c4a-d885-3311-9f26-b6791dd5f5c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9dc3414d-2ed5-348c-90e6-d15b038d544c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e6daca4e-560e-3454-94df-aded725e2da1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36a54a99-9968-3b2a-a495-50ef95a8429a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e8a6c594-316b-3380-b08a-9c6a6be0a42e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4e82b1be-e1e9-3b19-be16-4be52ba748a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4fae07a-d811-30bd-9586-00dcfd14e34c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("de45e9b4-2ac6-3df4-97bc-1b426c23cb20"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa7ae3f6-e911-3fdb-8825-62814ca1ecaa"))) {
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
    entry.getKey().readFieldsFromPb(walKey, byteStringUncompressor);
    if (((KnobRuntime.check(java.util.UUID.fromString("a737be5c-3863-3c16-8ce5-0d66d703a2c8"))) ? (walKey.getFollowingKvCount() == 0) : (((KnobRuntime.check(java.util.UUID.fromString("d1c28f3c-5d97-306d-b398-452c9b86f92d"))) ? (!walKey.hasFollowingKvCount()) : (((KnobRuntime.check(java.util.UUID.fromString("482bb0cc-fc0d-3396-9313-e14524c9c747"))) ? ((walKey.getFollowingKvCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7e29a26f-ac74-36e9-885e-04efd952600c"))) ? ((!walKey.hasFollowingKvCount()) || (walKey.getFollowingKvCount() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e8aa8cc3-14ea-3b77-a801-f938816dba14"))) ? ((!walKey.hasFollowingKvCount()) || ((walKey.getFollowingKvCount()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ff299c31-b69c-32d9-b50c-9758aafe9da0"))) ? ((!walKey.hasFollowingKvCount()) && (walKey.getFollowingKvCount() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("642ad0d2-09aa-3644-ae8c-86ccf7ec039a"))) ? ((!walKey.hasFollowingKvCount()) || ((walKey.getFollowingKvCount()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6a84b3ba-c20e-3d51-85ab-6c787449ec2f"))) ? ((!walKey.hasFollowingKvCount()) && ((walKey.getFollowingKvCount()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e1fd6e1e-91dc-3891-8d30-4cd7e1bdf6f1"))) ? ((walKey.getFollowingKvCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("12c72d4f-3bde-3a3e-af17-582f227cc316"))) ? ((!walKey.hasFollowingKvCount()) && ((walKey.getFollowingKvCount()) == (0))) : (!walKey.hasFollowingKvCount() || walKey.getFollowingKvCount() == 0))))))))))))))))))))) {
      LOG.trace("WALKey has no KVs that follow it; trying the next one. current offset={}",
        inputStream.getPos());
      return entry;
    }
    int expectedCells = walKey.getFollowingKvCount();
if(KnobRuntime.check(java.util.UUID.fromString("d91754a2-9f38-31c4-a157-c84bb8634812"))) {
throw new java.io.IOException("Injected exception");
}
    long posBefore = getPosition();
    int actualCells;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("87058805-0d19-3309-9c92-a9d9ffba7d2d"))) {
expectedCells *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f3e03ec1-6387-3ac4-a5b6-6868868c33b5"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f065e7e3-e672-3ee1-a1fc-b0d193d475dc"))) {
expectedCells += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("afb49cbe-1ef8-35f3-beb7-26e10b2b7304"))) {
expectedCells /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("991b3234-a902-3698-83d0-d1d28d827d21"))) {
expectedCells -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d3a25c2f-1335-31a0-a684-f89b17aa732d"))) {
expectedCells = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("66cbd0e8-9f02-3026-af2c-6a7a14370747"))) {
expectedCells = -1;
}
      actualCells = entry.getEdit().readFromCells(cellDecoder, expectedCells);
    } catch (Exception e) {
      String message = " while reading " + expectedCells + " WAL KVs; started reading at "
        + posBefore + " and read up to " + getPositionQuietly();
      IOException realEofEx = extractHiddenEof(e);
      if (realEofEx != null) {
        throw (EOFException) new EOFException("EOF " + message).initCause(realEofEx);
      } else {
        // do not throw EOFException as it could be other type of errors, throwing EOF will cause
        // the upper layer to consider the file has been fully read and cause data loss.
        throw new IOException("Error " + message, e);
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("2ebb4982-65f1-39c5-a021-6804c915540f"))) ? ((expectedCells) != (actualCells)) : (((KnobRuntime.check(java.util.UUID.fromString("90b50f64-f413-3709-b1fb-86020e14898e"))) ? ((expectedCells) == (actualCells)) : (expectedCells != actualCells))))) {
      throw new EOFException("Only read " + actualCells + " cells, expected " + expectedCells
        + "; started reading at " + posBefore + " and read up to " + getPositionQuietly());
    }
if(KnobRuntime.check(java.util.UUID.fromString("8e14f2bf-e39f-384a-a3c4-b99990a376fb"))) {
throw new java.io.IOException("Injected exception");
}
    long posAfter = this.inputStream.getPos();
    if (((KnobRuntime.check(java.util.UUID.fromString("b75e94fc-6c8c-3de5-bc0a-12d319cfca04"))) ? ((!trailerPresent) || ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("27b53bb5-25eb-3b58-9246-bcdc34220fb1"))) ? ((!trailerPresent) || ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("1727c742-3c28-3af3-b3ed-c4ca8c473b4d"))) ? ((trailerPresent) || ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("45fa3b69-230c-3ab8-9bdf-19e596e9e4bd"))) ? ((!trailerPresent) || ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("c4532f1d-c0d0-3c8d-859b-6c4eae0ceaf4"))) ? ((posAfter) >= (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("8b48d96f-a77c-3357-9fb7-c95ab2bcf1eb"))) ? ((trailerPresent) && ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("34fc3717-4b89-3ded-aa72-6f3100f925c6"))) ? ((posAfter) > (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("e5b36f6d-0ebe-3312-843a-c9d9baf54598"))) ? ((!trailerPresent) && ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("274d900e-b9bb-33ca-8a39-8f000b3abf04"))) ? ((!trailerPresent) && ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("8ca8033d-eddd-3635-be2e-7bdb99fdcd33"))) ? ((!trailerPresent) && (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("ff870b6c-6185-371c-aca3-612e8c94c0ec"))) ? (posAfter > this.walEditsStopOffset) : (((KnobRuntime.check(java.util.UUID.fromString("12398d2d-f714-34d2-bf30-aececc14f2d7"))) ? ((trailerPresent) || ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("15f11c7c-5918-3aa1-8312-6832267a4f6f"))) ? ((trailerPresent) && ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("a85af0ba-d8d7-3e5d-af3f-c8d75cf92b65"))) ? ((trailerPresent) && ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("07e2b9a3-6c07-3356-9a7d-de6db80d32d2"))) ? ((trailerPresent) || ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e88458f9-1c2a-3467-9ddc-407a0217d475"))) ? ((!trailerPresent) || ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("63459342-d13b-32fa-a300-132ab8e2b706"))) ? ((trailerPresent) && ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("7127f74d-c796-3e4e-8e4b-94e37f63f83e"))) ? ((!trailerPresent) && ((posAfter) <= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e630dfc6-d12f-3b1b-b318-f59587597e0a"))) ? ((posAfter) <= (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("84aaccbf-161e-38fa-8b4a-da1e8983b085"))) ? ((!trailerPresent) && ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("f18957c7-3579-360d-ab18-59e06d8a1b16"))) ? ((trailerPresent) && ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("efae4025-8267-3626-acde-0b61f630ffbb"))) ? ((!trailerPresent) || (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("4b8f5f75-2b81-3035-962e-e0f0f3f2a9f3"))) ? ((posAfter) == (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3727fe08-8b1f-33cd-bbe1-2b2baecb364b"))) ? ((posAfter) < (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("d1f8eede-80ab-3fcc-bfef-b71ef99a8f03"))) ? ((trailerPresent) || (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3e4d2c1a-53fb-39d2-a834-4093a1b22a12"))) ? ((!trailerPresent) && ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("674b2270-f04f-305c-a13b-b3f76d26192c"))) ? ((!trailerPresent) || ((posAfter) == (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("75cff31f-5f71-329f-9912-cebdcf055a2c"))) ? ((trailerPresent) && ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("54c7f7ce-fd94-3258-8999-109a45515e3b"))) ? ((!trailerPresent) && ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("370403ee-d410-3a35-972f-a743e10f7cf1"))) ? ((trailerPresent) || ((posAfter) != (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("3be561d4-28cf-343c-9197-bba92755cc7a"))) ? ((trailerPresent) || ((posAfter) < (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4edab150-ac66-35bd-aa0b-5f9b72f02ab6"))) ? ((posAfter) != (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3a1e30a4-c5bd-33e7-a540-dd2ec95df6d7"))) ? ((trailerPresent) && (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("1cbad10b-1302-3d9b-aea7-06bde03339a0"))) ? ((!trailerPresent) || ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("e5e5f887-3abc-31c2-b2bb-49d11e93220c"))) ? (trailerPresent) : (((KnobRuntime.check(java.util.UUID.fromString("08e00ccf-7a6b-3f70-b01d-b7ac889b56ae"))) ? ((trailerPresent) || ((posAfter) > (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("9c320933-6bb9-3c2a-88e7-f4582869004e"))) ? (!trailerPresent) : (trailerPresent && posAfter > this.walEditsStopOffset))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.error("Read WALTrailer while reading WALEdits. wal: {}, inputStream.getPos(): {},"
        + " walEditsStopOffset: {}", path, posAfter, walEditsStopOffset);
      throw new EOFException("Read WALTrailer while reading WALEdits; started reading at "
        + posBefore + " and read up to " + posAfter);
    }
    return entry;
  }

  @Override
  protected InputStream getCellCodecInputStream(FSDataInputStream stream) {
    // just return the original input stream
    return stream;
  }

  @Override
  protected void skipTo(long position) throws IOException {
    Entry entry = new Entry();
    for (;;) {
      entry = next(entry);
      if (entry == null) {
        throw new EOFException("Can not skip to the given position " + position
          + " as we have already reached the end of file");
      }
      long pos = inputStream.getPos();
      if (pos > position) {
        throw new IOException("Can not skip to the given position " + position + ", stopped at "
          + pos + " which is already beyond the give position, malformed WAL?");
      }
      if (pos == position) {
        return;
      }
    }
  }
}
