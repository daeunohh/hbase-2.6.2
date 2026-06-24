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
    long originalPosition = getPosition();
    if (reachWALEditsStopOffset(originalPosition)) {
      return null;
    }
    WALProtos.WALKey walKey;
    try {
      // for one way stream reader, we do not care about what is the exact position where we hit the
      // EOF or IOE, so just use the helper method to parse WALKey, in tailing reader, we will try
      // to read the varint size by ourselves
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
    if (entry == null) {
      entry = new Entry();
    }
if(KnobRuntime.check(java.util.UUID.fromString("03261f4c-c7be-3b13-b6be-2e22b5b48629"))) {
try {
    java.lang.reflect.Field _knob_field_ = walKey.getClass().getDeclaredField("followingKvCount_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(walKey));
    _knob_field_.set(walKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    entry.getKey().readFieldsFromPb(walKey, byteStringUncompressor);
    if (((KnobRuntime.check(java.util.UUID.fromString("12c72d4f-3bde-3a3e-af17-582f227cc316"))) ? ((!walKey.hasFollowingKvCount()) && ((walKey.getFollowingKvCount()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ff299c31-b69c-32d9-b50c-9758aafe9da0"))) ? ((!walKey.hasFollowingKvCount()) && (walKey.getFollowingKvCount() == 0)) : (!walKey.hasFollowingKvCount() || walKey.getFollowingKvCount() == 0))))) {
      LOG.trace("WALKey has no KVs that follow it; trying the next one. current offset={}",
        inputStream.getPos());
      return entry;
    }
    int expectedCells = walKey.getFollowingKvCount();
    long posBefore = getPosition();
    int actualCells;
    try {
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
    if (expectedCells != actualCells) {
      throw new EOFException("Only read " + actualCells + " cells, expected " + expectedCells
        + "; started reading at " + posBefore + " and read up to " + getPositionQuietly());
    }
    long posAfter = this.inputStream.getPos();
    if (((KnobRuntime.check(java.util.UUID.fromString("d1f8eede-80ab-3fcc-bfef-b71ef99a8f03"))) ? ((trailerPresent) || (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3a1e30a4-c5bd-33e7-a540-dd2ec95df6d7"))) ? ((trailerPresent) && (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3e4d2c1a-53fb-39d2-a834-4093a1b22a12"))) ? ((!trailerPresent) && ((posAfter) >= (this.walEditsStopOffset))) : (((KnobRuntime.check(java.util.UUID.fromString("4edab150-ac66-35bd-aa0b-5f9b72f02ab6"))) ? ((posAfter) != (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("efae4025-8267-3626-acde-0b61f630ffbb"))) ? ((!trailerPresent) || (posAfter > this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("3727fe08-8b1f-33cd-bbe1-2b2baecb364b"))) ? ((posAfter) < (this.walEditsStopOffset)) : (((KnobRuntime.check(java.util.UUID.fromString("1cbad10b-1302-3d9b-aea7-06bde03339a0"))) ? ((!trailerPresent) || ((posAfter) > (this.walEditsStopOffset))) : (trailerPresent && posAfter > this.walEditsStopOffset))))))))))))))) {
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
