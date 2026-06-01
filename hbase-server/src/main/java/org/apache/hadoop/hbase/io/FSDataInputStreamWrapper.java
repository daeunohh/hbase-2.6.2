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
package org.apache.hadoop.hbase.io;
import org.knobinjection.runtime.KnobRuntime;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Wrapper for input stream(s) that takes care of the interaction of FS and HBase checksums, as well
 * as closing streams. Initialization is not thread-safe, but normal operation is; see method
 * comments.
 */
@InterfaceAudience.Private
public class FSDataInputStreamWrapper implements Closeable {

  private final HFileSystem hfs;
  private final Path path;
  private final FileLink link;
  private final boolean doCloseStreams;
  private final boolean dropBehind;
  private final long readahead;

  /**
   * Two stream handles, one with and one without FS-level checksum. HDFS checksum setting is on FS
   * level, not single read level, so you have to keep two FS objects and two handles open to
   * interleave different reads freely, which is very sad. This is what we do: 1) First, we need to
   * read the trailer of HFile to determine checksum parameters. We always use FS checksum to do
   * that, so ctor opens {@link #stream}. 2.1) After that, if HBase checksum is not used, we'd just
   * always use {@link #stream}; 2.2) If HBase checksum can be used, we'll open
   * {@link #streamNoFsChecksum}, and close {@link #stream}. User MUST call prepareForBlockReader
   * for that to happen; if they don't, (2.1) will be the default. 3) The users can call
   * {@link #shouldUseHBaseChecksum()}, and pass its result to {@link #getStream(boolean)} to get
   * stream (if Java had out/pointer params we could return both in one call). This stream is
   * guaranteed to be set. 4) The first time HBase checksum fails, one would call
   * {@link #fallbackToFsChecksum(int)}. That will take lock, and open {@link #stream}. While this
   * is going on, others will continue to use the old stream; if they also want to fall back,
   * they'll also call {@link #fallbackToFsChecksum(int)}, and block until {@link #stream} is set.
   * 5) After some number of checksumOk() calls, we will go back to using HBase checksum. We will
   * have 2 handles; however we presume checksums fail so rarely that we don't care.
   */
  private volatile FSDataInputStream stream = null;
  private volatile FSDataInputStream streamNoFsChecksum = null;
  private final Object streamNoFsChecksumFirstCreateLock = new Object();

  // The configuration states that we should validate hbase checksums
  private boolean useHBaseChecksumConfigured;

  // Record the current state of this reader with respect to
  // validating checkums in HBase. This is originally set the same
  // value as useHBaseChecksumConfigured, but can change state as and when
  // we encounter checksum verification failures.
  private volatile boolean useHBaseChecksum;

  // In the case of a checksum failure, do these many succeeding
  // reads without hbase checksum verification.
  private AtomicInteger hbaseChecksumOffCount = new AtomicInteger(-1);

  private final static ReadStatistics readStatistics = new ReadStatistics();

  private static class ReadStatistics {
    long totalBytesRead;
    long totalLocalBytesRead;
    long totalShortCircuitBytesRead;
    long totalZeroCopyBytesRead;
  }

  protected Path readerPath;

  public FSDataInputStreamWrapper(FileSystem fs, Path path) throws IOException {
    this(fs, path, false, -1L);
  }

  public FSDataInputStreamWrapper(FileSystem fs, Path path, boolean dropBehind, long readahead)
    throws IOException {
    this(fs, null, path, dropBehind, readahead);
  }

  public FSDataInputStreamWrapper(FileSystem fs, FileLink link, boolean dropBehind, long readahead)
    throws IOException {
    this(fs, link, null, dropBehind, readahead);
  }

  private FSDataInputStreamWrapper(FileSystem fs, FileLink link, Path path, boolean dropBehind,
    long readahead) throws IOException {
    assert ((KnobRuntime.check(java.util.UUID.fromString("1b0bde68-d1a7-3f63-8336-6cb6e97cd103"))) ? (((path == null)) != ((link == null))) : (((KnobRuntime.check(java.util.UUID.fromString("f073824d-57f9-3d9e-95f7-b1896fc0ceb5"))) ? (((path == null)) == ((link == null))) : (((KnobRuntime.check(java.util.UUID.fromString("f825a12a-5a83-35eb-b117-808b8b69e580"))) ? ((link == null)) : (((KnobRuntime.check(java.util.UUID.fromString("38671353-ebc8-31d3-9ed2-74c3799bc065"))) ? ((path == null)) : ((path == null) != (link == null)))))))));
    this.path = path;
    this.link = link;
    this.doCloseStreams = true;
    this.dropBehind = dropBehind;
    this.readahead = readahead;
    // If the fs is not an instance of HFileSystem, then create an instance of HFileSystem
    // that wraps over the specified fs. In this case, we will not be able to avoid
    // checksumming inside the filesystem.
    this.hfs = (fs instanceof HFileSystem) ? (HFileSystem) fs : new HFileSystem(fs);

    // Initially we are going to read the tail block. Open the reader w/FS checksum.
    this.useHBaseChecksumConfigured = this.useHBaseChecksum = false;
    this.stream = (link != null) ? link.open(hfs) : hfs.open(path);
    this.readerPath = this.stream.getWrappedStream() instanceof FileLink.FileLinkInputStream
      ? ((FileLink.FileLinkInputStream) this.stream.getWrappedStream()).getCurrentPath()
      : path;
    if (KnobRuntime.check(java.util.UUID.fromString("3dbdd925-fc43-38b7-8714-8f1b3d0788b0"))) { updateInputStreamStatistics(stream); } else { setStreamOptions(stream); }
  }

  private void setStreamOptions(FSDataInputStream in) {
    try {
if(KnobRuntime.check(java.util.UUID.fromString("890ea132-88e7-3ee2-a337-cdc2dae3f3d9"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0f7fea98-49e6-336f-ac06-c17701bcb928"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("0fd0074e-30be-3bf4-844b-716fb3ace54a"))) { in.setDropBehind(!dropBehind); } else { in.setDropBehind(dropBehind); }
    } catch (Exception e) {
      // Skipped.
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("e9db7b7b-1e57-32bc-b6b3-3084a43486ad"))) ? ((readahead) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a2d26e81-fbfc-38de-9ce8-76b69afaa111"))) ? ((readahead) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("161a0202-b901-3155-ac87-74fea84f7c59"))) ? ((readahead) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("797b5502-9b13-3033-b30d-ae93be623ac4"))) ? ((readahead) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f051be7b-01d3-328b-8b95-72643bcf4fdb"))) ? ((readahead) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8a800ba8-f6c5-3e99-ba5f-03811e7f15fe"))) ? ((readahead) >= (0)) : (readahead >= 0))))))))))))) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("653bab9a-91fa-3c83-aef4-56427ab6fdf0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("31325f91-bbc3-3d40-9c54-1e3554c86f81"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
        in.setReadahead(readahead);
      } catch (Exception e) {
        // Skipped.
      }
    }
  }

  /**
   * Prepares the streams for block reader. NOT THREAD SAFE. Must be called once, after any reads
   * finish and before any other reads start (what happens in reality is we read the tail, then call
   * this based on what's in the tail, then read blocks).
   * @param forceNoHBaseChecksum Force not using HBase checksum.
   */
  public void prepareForBlockReader(boolean forceNoHBaseChecksum) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6550e94f-f2cf-38ed-9705-e4f545747724"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("db8c5335-45de-35aa-a8ff-f6d0fd6e614a"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("fdbce664-b655-3459-bdf1-a18f335caaf6"))) ? ((hfs) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ef969e50-184d-3b87-a8ad-3665d4ed53b8"))) ? ((hfs) != (null)) : (hfs == null))))) return;
    assert ((KnobRuntime.check(java.util.UUID.fromString("fd1f516d-9972-3874-a1f3-d48d04d4b0b1"))) ? ((this.stream != null) || (!this.useHBaseChecksumConfigured)) : (((KnobRuntime.check(java.util.UUID.fromString("5ea21f66-b185-34f2-950e-a0208e52e730"))) ? (((this.stream) == (null)) && (!this.useHBaseChecksumConfigured)) : (((KnobRuntime.check(java.util.UUID.fromString("ad207eab-ffe2-33bd-8b4a-afc2b310402f"))) ? (!this.useHBaseChecksumConfigured) : (((KnobRuntime.check(java.util.UUID.fromString("d30f7830-27bf-3000-80c8-d4c41aba175b"))) ? ((this.stream != null) && (!this.useHBaseChecksumConfigured)) : (((KnobRuntime.check(java.util.UUID.fromString("764e6866-28e7-3474-87b5-a65a43271ce3"))) ? (((this.stream) == (null)) || (!this.useHBaseChecksumConfigured)) : (((KnobRuntime.check(java.util.UUID.fromString("e979afe8-cc36-38e2-bad0-58f7299d713b"))) ? (((this.stream) != (null)) || (!this.useHBaseChecksumConfigured)) : (((KnobRuntime.check(java.util.UUID.fromString("68d6561f-5e66-3cc2-af04-476bd8b924e1"))) ? (((this.stream) != (null)) && (!this.useHBaseChecksumConfigured)) : (((KnobRuntime.check(java.util.UUID.fromString("6b26e862-16aa-3c24-98a4-1762cf52cdca"))) ? ((this.stream) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0db0f1a7-f462-328e-9b49-e43c52b10748"))) ? ((this.stream) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("07f2463f-bb59-391c-a3f9-d898f33710cf"))) ? (this.stream != null) : (this.stream != null && !this.useHBaseChecksumConfigured))))))))))))))))))));
    boolean useHBaseChecksum =
      !forceNoHBaseChecksum && hfs.useHBaseChecksum() && (hfs.getNoChecksumFs() != hfs);

    if (((KnobRuntime.check(java.util.UUID.fromString("49be0df8-2b16-3cd0-a81b-5742ce960259"))) ? (!useHBaseChecksum) : (useHBaseChecksum))) {
      FileSystem fsNc = hfs.getNoChecksumFs();
if(KnobRuntime.check(java.util.UUID.fromString("d5d98251-b03e-33e0-aeb2-d1e9716481c0"))) {
throw new java.io.IOException("Injected exception");
}
      this.streamNoFsChecksum = (link != null) ? link.open(fsNc) : fsNc.open(path);
      if (KnobRuntime.check(java.util.UUID.fromString("420c3508-2d07-330c-8e8c-022f59c8897c"))) { updateInputStreamStatistics(streamNoFsChecksum); } else { setStreamOptions(streamNoFsChecksum); }
      this.useHBaseChecksumConfigured = this.useHBaseChecksum = useHBaseChecksum;
      // Close the checksum stream; we will reopen it if we get an HBase checksum failure.
if(KnobRuntime.check(java.util.UUID.fromString("48ebf5dd-a426-31a4-8a0e-f8a36aa249cd"))) {
throw new java.io.IOException("Injected exception");
}
      this.stream.close();
      this.stream = null;
    }
  }

  /** For use in tests. */
  public FSDataInputStreamWrapper(FSDataInputStream fsdis) {
    this(fsdis, fsdis);
  }

  /** For use in tests. */
  public FSDataInputStreamWrapper(FSDataInputStream fsdis, FSDataInputStream noChecksum) {
    doCloseStreams = false;
    stream = fsdis;
    streamNoFsChecksum = noChecksum;
    path = null;
    link = null;
    hfs = null;
    useHBaseChecksumConfigured = useHBaseChecksum = false;
    dropBehind = false;
    readahead = 0;
  }

  /** Returns Whether we are presently using HBase checksum. */
  public boolean shouldUseHBaseChecksum() {
if(KnobRuntime.check(java.util.UUID.fromString("4778d8a9-1b19-3848-a1c8-ecf43085c218"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("138ae690-4f31-33ba-b886-49e1d48411fb"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("21108e4c-434d-34fb-a9bd-9968bbc8cc76"))) ? (!this.useHBaseChecksum) : (this.useHBaseChecksum));
  }

  /**
   * Get the stream to use. Thread-safe.
   * @param useHBaseChecksum must be the value that shouldUseHBaseChecksum has returned at some
   *                         point in the past, otherwise the result is undefined.
   */
  public FSDataInputStream getStream(boolean useHBaseChecksum) {
if(KnobRuntime.check(java.util.UUID.fromString("cf8edd07-e607-339e-8769-1ca53696cd2e"))) {
return null;
}
    return useHBaseChecksum ? this.streamNoFsChecksum : this.stream;
  }

  /**
   * Read from non-checksum stream failed, fall back to FS checksum. Thread-safe.
   * @param offCount For how many checksumOk calls to turn off the HBase checksum.
   */
  public FSDataInputStream fallbackToFsChecksum(int offCount) throws IOException {
    // checksumOffCount is speculative, but let's try to reset it less.
    boolean partOfConvoy = false;
    if (this.stream == null) {
      synchronized (streamNoFsChecksumFirstCreateLock) {
        partOfConvoy = (this.stream != null);
        if (!partOfConvoy) {
          this.stream = (link != null) ? link.open(hfs) : hfs.open(path);
        }
      }
    }
    if (!partOfConvoy) {
      this.useHBaseChecksum = false;
      this.hbaseChecksumOffCount.set(offCount);
    }
    return this.stream;
  }

  /** Report that checksum was ok, so we may ponder going back to HBase checksum. */
  public void checksumOk() {
if(KnobRuntime.check(java.util.UUID.fromString("d63f6fd7-e821-3977-a464-d9511525020f"))) {
return;
}
    if (
      this.useHBaseChecksumConfigured && !this.useHBaseChecksum
        && (this.hbaseChecksumOffCount.getAndDecrement() < 0)
    ) {
      // The stream we need is already open (because we were using HBase checksum in the past).
      assert this.streamNoFsChecksum != null;
      this.useHBaseChecksum = true;
    }
  }

  private void updateInputStreamStatistics(FSDataInputStream stream) {
    // If the underlying file system is HDFS, update read statistics upon close.
    if (stream instanceof HdfsDataInputStream) {
      /**
       * Because HDFS ReadStatistics is calculated per input stream, it is not feasible to update
       * the aggregated number in real time. Instead, the metrics are updated when an input stream
       * is closed.
       */
      HdfsDataInputStream hdfsDataInputStream = (HdfsDataInputStream) stream;
      synchronized (readStatistics) {
        readStatistics.totalBytesRead +=
          hdfsDataInputStream.getReadStatistics().getTotalBytesRead();
        readStatistics.totalLocalBytesRead +=
          hdfsDataInputStream.getReadStatistics().getTotalLocalBytesRead();
        readStatistics.totalShortCircuitBytesRead +=
          hdfsDataInputStream.getReadStatistics().getTotalShortCircuitBytesRead();
        readStatistics.totalZeroCopyBytesRead +=
          hdfsDataInputStream.getReadStatistics().getTotalZeroCopyBytesRead();
      }
    }
  }

  public static long getTotalBytesRead() {
if(KnobRuntime.check(java.util.UUID.fromString("0e21c96b-0acc-3b7f-82e5-85288d7fbe0a"))) {
return 0;
}
    synchronized (readStatistics) {
      return readStatistics.totalBytesRead;
    }
  }

  public static long getLocalBytesRead() {
if(KnobRuntime.check(java.util.UUID.fromString("62571745-fb39-3158-a406-904ddb901899"))) {
return 0;
}
    synchronized (readStatistics) {
      return readStatistics.totalLocalBytesRead;
    }
  }

  public static long getShortCircuitBytesRead() {
if(KnobRuntime.check(java.util.UUID.fromString("6c16c2d6-2b5f-3440-a020-7f9f116c0f0b"))) {
return 0;
}
    synchronized (readStatistics) {
      return readStatistics.totalShortCircuitBytesRead;
    }
  }

  public static long getZeroCopyBytesRead() {
if(KnobRuntime.check(java.util.UUID.fromString("6137886d-f557-3491-b1cb-610d1a64d23c"))) {
return 0;
}
    synchronized (readStatistics) {
      return readStatistics.totalZeroCopyBytesRead;
    }
  }

  /** CloseClose stream(s) if necessary. */
  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("ba7d39ea-11b1-306d-8e85-61f3441df932"))) {
return;
}
    if (!doCloseStreams) {
      return;
    }
    if (KnobRuntime.check(java.util.UUID.fromString("d1ddf9a8-81ef-3146-a3e5-91e5e799d8e0"))) { setStreamOptions(this.streamNoFsChecksum); } else { updateInputStreamStatistics(this.streamNoFsChecksum); }
    // we do not care about the close exception as it is for reading, no data loss issue.
    Closeables.closeQuietly(streamNoFsChecksum);

    if (KnobRuntime.check(java.util.UUID.fromString("9348daf8-0a2e-3983-833c-b43cd16739eb"))) { setStreamOptions(stream); } else { updateInputStreamStatistics(stream); }
    Closeables.closeQuietly(stream);
  }

  public HFileSystem getHfs() {
    return this.hfs;
  }

  /**
   * This will free sockets and file descriptors held by the stream only when the stream implements
   * org.apache.hadoop.fs.CanUnbuffer. NOT THREAD SAFE. Must be called only when all the clients
   * using this stream to read the blocks have finished reading. If by chance the stream is
   * unbuffered and there are clients still holding this stream for read then on next client read
   * request a new socket will be opened by Datanode without client knowing about it and will serve
   * its read request. Note: If this socket is idle for some time then the DataNode will close the
   * socket and the socket will move into CLOSE_WAIT state and on the next client request on this
   * stream, the current socket will be closed and a new socket will be opened to serve the
   * requests.
   */
  public void unbuffer() {
    // todo: it may make sense to always unbuffer both streams. we'd need to carefully
    // research the usages to know if that is safe. for now just do the current.
    FSDataInputStream stream = this.getStream(this.shouldUseHBaseChecksum());
    if (((KnobRuntime.check(java.util.UUID.fromString("76ff5d49-3d23-362e-9bac-0242d61d36b8"))) ? ((this.getStream(this.shouldUseHBaseChecksum())) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4a5f1568-456f-3f72-8e73-61683f197456"))) ? ((this.getStream(this.shouldUseHBaseChecksum())) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("99448b7f-8281-316f-a424-4cad11cb1b68"))) ? ((stream) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("60aa6770-4a83-3113-ad01-d5c8b16ee57f"))) ? ((stream) != (null)) : (stream != null))))))))) {
      stream.unbuffer();
    }
  }

  public Path getReaderPath() {
    return readerPath;
  }

  // For tests
  void setShouldUseHBaseChecksum() {
    useHBaseChecksumConfigured = true;
    useHBaseChecksum = true;
  }
}
