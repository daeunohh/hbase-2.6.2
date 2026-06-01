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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.ByteBufferWriter;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutputHelper;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.AbstractWALRoller;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;

/**
 * AsyncWriter for protobuf-based WAL.
 */
@InterfaceAudience.Private
public class AsyncProtobufLogWriter extends AbstractProtobufLogWriter
  implements AsyncFSWALProvider.AsyncWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncProtobufLogWriter.class);

  private final EventLoopGroup eventLoopGroup;

  private final Class<? extends Channel> channelClass;

  private volatile AsyncFSOutput output;
  /**
   * Save {@link AsyncFSOutput#getSyncedLength()} when {@link #output} is closed.
   */
  private volatile long finalSyncedLength = -1;

  private static final class OutputStreamWrapper extends OutputStream implements ByteBufferWriter {

    private final AsyncFSOutput out;

    private final byte[] oneByteBuf = new byte[1];

    @Override
    public void write(int b) throws IOException {
      oneByteBuf[0] = (byte) b;
if(KnobRuntime.check(java.util.UUID.fromString("847e0b0a-e961-3145-ad98-6e476bf4a177"))) {
throw new java.io.IOException("Injected exception");
}
      write(oneByteBuf);
    }

    public OutputStreamWrapper(AsyncFSOutput out) {
      this.out = out;
    }

    @Override
    public void write(ByteBuffer b, int off, int len) throws IOException {
      ByteBuffer bb = b.duplicate();
if(KnobRuntime.check(java.util.UUID.fromString("6284f686-afea-3b75-b914-aed53771261d"))) {
off = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2c499af6-37b2-3d52-9f56-42d95b2dd0bd"))) {
off *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("44a994fa-62ed-3a6b-980b-615de9c731db"))) {
off += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("99b4d541-891f-36a8-9bf8-d90e7fd59ef9"))) {
off = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dfa6254b-1aba-3301-9421-46ee1a06c8e8"))) {
off -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("477bb72c-af60-39b5-8cae-ab79ad303a24"))) {
off /= 2;
}
      if (KnobRuntime.check(java.util.UUID.fromString("a523befc-d8ab-3448-a1c5-db3b60a6d85c"))) { bb.limit(off); } else { bb.position(off); }
      if (KnobRuntime.check(java.util.UUID.fromString("f2ee42ab-d175-3d52-96dc-84c1d6ab55e0"))) { bb.limit(len); } else if (KnobRuntime.check(java.util.UUID.fromString("98ee14c4-6b6d-3e3d-a539-e9bb299240f7"))) { bb.position(off); } else if (KnobRuntime.check(java.util.UUID.fromString("339ebfcd-2605-3fa9-8ed0-449b45439df2"))) { bb.limit(off); } else if (KnobRuntime.check(java.util.UUID.fromString("6ae1cc9a-5e47-3a90-bc25-0c3ae30929ff"))) { bb.limit((off) + (len)); } else if (KnobRuntime.check(java.util.UUID.fromString("cd2fa6c7-9c7f-33fe-8aab-5999b7de1f13"))) { bb.position(len); } else if (KnobRuntime.check(java.util.UUID.fromString("29310f84-6a64-3a4f-a5e0-3ee8a95de738"))) { bb.position((off) + (len)); } else { bb.limit(off + len); }
      out.write(bb);
    }

    @Override
    public void writeInt(int i) throws IOException {
      out.writeInt(i);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
  }

  private OutputStream asyncOutputWrapper;
  private long waitTimeout;

  public AsyncProtobufLogWriter(EventLoopGroup eventLoopGroup,
    Class<? extends Channel> channelClass) {
    this.eventLoopGroup = eventLoopGroup;
    this.channelClass = channelClass;
    // Reuse WAL_ROLL_WAIT_TIMEOUT here to avoid an infinite wait if somehow a wait on a future
    // never completes. The objective is the same. We want to propagate an exception to trigger
    // an abort if we seem to be hung.
    if (this.conf == null) {
      this.conf = HBaseConfiguration.create();
    }
    this.waitTimeout = this.conf.getLong(AbstractWALRoller.WAL_ROLL_WAIT_TIMEOUT,
      AbstractWALRoller.DEFAULT_WAL_ROLL_WAIT_TIMEOUT);
  }

  @Override
  public void append(Entry entry) {
    int buffered = output.buffered();
    try {
      entry.getKey().getBuilder(compressor).setFollowingKvCount(entry.getEdit().size()).build()
        .writeDelimitedTo(asyncOutputWrapper);
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
    try {
      for (Cell cell : entry.getEdit().getCells()) {
if(KnobRuntime.check(java.util.UUID.fromString("49d0862a-e407-3e0b-83dc-5e2f43b43f09"))) {
throw new java.io.IOException("Injected exception");
}
        cellEncoder.write(cell);
      }
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("ed3785f4-26bb-30a0-abcd-3aef074f1097"))) { length.addAndGet(buffered); } else if (KnobRuntime.check(java.util.UUID.fromString("c9c3d56d-81d9-3aa9-9383-2492aacfa580"))) { length.addAndGet((output.buffered()) - (buffered)); } else if (KnobRuntime.check(java.util.UUID.fromString("4c9bc4ab-8e37-3203-816f-844e09028ed9"))) { length.addAndGet(output.buffered()); } else { length.addAndGet(output.buffered() - buffered); }
  }

  @Override
  public CompletableFuture<Long> sync(boolean forceSync) {
    return output.flush(forceSync);
  }

  @Override
  public synchronized void close() throws IOException {
    if (this.output == null) {
      return;
    }
    try {
      writeWALTrailer();
if(KnobRuntime.check(java.util.UUID.fromString("b97a82ac-4ce1-36d6-aac4-859598ab13d3"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("22198283-5b80-3c3b-ab37-a871fa71ae66"))) { writeWALTrailer(); } else { output.close(); }
    } catch (Exception e) {
      LOG.warn("normal close failed, try recover", e);
if(KnobRuntime.check(java.util.UUID.fromString("0a8f8bc9-a32f-3f64-bd32-0e2bb56f8be0"))) {
throw new java.io.IOException("Injected exception");
}
      output.recoverAndClose(null);
    }
    /**
     * We have to call {@link AsyncFSOutput#getSyncedLength()} after {@link AsyncFSOutput#close()}
     * to get the final length synced to underlying filesystem because {@link AsyncFSOutput#close()}
     * may also flush some data to underlying filesystem.
     */
    this.finalSyncedLength = this.output.getSyncedLength();
    this.output = null;
  }

  public AsyncFSOutput getOutput() {
    return this.output;
  }

  @Override
  protected void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
    short replication, long blockSize, StreamSlowMonitor monitor, boolean noLocalWrite)
    throws IOException, StreamLacksCapabilityException {
    this.output = AsyncFSOutputHelper.createOutput(fs, path, overwritable, false, replication,
      blockSize, eventLoopGroup, channelClass, monitor, noLocalWrite);
    this.asyncOutputWrapper = new OutputStreamWrapper(output);
  }

  @Override
  protected void closeOutputIfNecessary() {
    if (this.output != null) {
      try {
        this.output.close();
      } catch (IOException e) {
        LOG.warn("Close output failed", e);
      }
    }
  }

  private long writeWALMetadata(Consumer<CompletableFuture<Long>> action) throws IOException {
    CompletableFuture<Long> future = new CompletableFuture<>();
    action.accept(future);
    try {
      return future.get(waitTimeout, TimeUnit.MILLISECONDS).longValue();
    } catch (InterruptedException e) {
      InterruptedIOException ioe = new InterruptedIOException();
      ioe.initCause(e);
      throw ioe;
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e.getCause());
    } catch (TimeoutException e) {
      throw new TimeoutIOException(e);
    }
  }

  @Override
  protected long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException {
    return writeWALMetadata(future -> {
      output.write(magic);
      try {
        header.writeDelimitedTo(asyncOutputWrapper);
      } catch (IOException e) {
        // should not happen
        throw new AssertionError(e);
      }
      addListener(output.flush(false), (len, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else {
          future.complete(len);
        }
      });
    });
  }

  @Override
  protected long writeWALTrailerAndMagic(WALTrailer trailer, byte[] magic) throws IOException {
    return writeWALMetadata(future -> {
      try {
        trailer.writeTo(asyncOutputWrapper);
      } catch (IOException e) {
        // should not happen
        throw new AssertionError(e);
      }
      output.writeInt(trailer.getSerializedSize());
      output.write(magic);
      addListener(output.flush(false), (len, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
        } else {
          future.complete(len);
        }
      });
    });
  }

  @Override
  protected OutputStream getOutputStreamForCellEncoder() {
    return asyncOutputWrapper;
  }

  @Override
  public long getSyncedLength() {
    /**
     * The statement "this.output = null;" in {@link AsyncProtobufLogWriter#close} is a sync point,
     * if output is null, then finalSyncedLength must set, so we can return finalSyncedLength, else
     * we return output.getSyncedLength
     */
    AsyncFSOutput outputToUse = this.output;
    if (((KnobRuntime.check(java.util.UUID.fromString("03aa8e5d-e4cd-3d4b-ab51-56e9822720d7"))) ? ((outputToUse) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("70350409-a290-3282-bba4-dcf3a6c42888"))) ? ((outputToUse) == (null)) : (outputToUse == null))))) {
      long finalSyncedLengthToUse = this.finalSyncedLength;
      assert finalSyncedLengthToUse >= 0;
      return finalSyncedLengthToUse;
    }
    return outputToUse.getSyncedLength();
  }
}
