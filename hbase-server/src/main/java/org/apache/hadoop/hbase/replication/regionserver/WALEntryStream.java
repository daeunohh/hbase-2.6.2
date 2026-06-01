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
package org.apache.hadoop.hbase.replication.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.OptionalLong;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufWALReader;
import org.apache.hadoop.hbase.regionserver.wal.WALHeaderEOFException;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALStreamReader;
import org.apache.hadoop.hbase.wal.WALTailingReader;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming access to WAL entries. This class is given a queue of WAL {@link Path}, and continually
 * iterates through all the WAL {@link Entry} in the queue. When it's done reading from a Path, it
 * dequeues it and starts reading from the next.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class WALEntryStream implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(WALEntryStream.class);

  private WALTailingReader reader;
  private WALTailingReader.State state;
  private Path currentPath;
  // cache of next entry for hasNext()
  private Entry currentEntry;
  // position for the current entry. As now we support peek, which means that the upper layer may
  // choose to return before reading the current entry, so it is not safe to return the value below
  // in getPosition.
  private long currentPositionOfEntry = 0;
  // position after reading current entry
  private long currentPositionOfReader = 0;
  private final ReplicationSourceLogQueue logQueue;
  private final String walGroupId;
  private final FileSystem fs;
  private final Configuration conf;
  private final WALFileLengthProvider walFileLengthProvider;
  private final MetricsSource metrics;

  // we should be able to skip empty WAL files, but for safety, we still provide this config
  // see HBASE-18137 for more details
  private boolean eofAutoRecovery;

  /**
   * Create an entry stream over the given queue at the given start position
   * @param logQueue              the queue of WAL paths
   * @param conf                  the {@link Configuration} to use to create {@link WALStreamReader}
   *                              for this stream
   * @param startPosition         the position in the first WAL to start reading at
   * @param walFileLengthProvider provides the length of the WAL file
   * @param serverName            the server name which all WALs belong to
   * @param metrics               the replication metrics
   */
  public WALEntryStream(ReplicationSourceLogQueue logQueue, FileSystem fs, Configuration conf,
    long startPosition, WALFileLengthProvider walFileLengthProvider, MetricsSource metrics,
    String walGroupId) {
    this.logQueue = logQueue;
    this.fs = fs;
    this.conf = conf;
    this.currentPositionOfEntry = startPosition;
    this.walFileLengthProvider = walFileLengthProvider;
    this.metrics = metrics;
    this.walGroupId = walGroupId;
    this.eofAutoRecovery = conf.getBoolean("replication.source.eof.autorecovery", false);
  }

  public enum HasNext {
    /** means there is a new entry and you could use peek or next to get current entry */
    YES,
    /**
     * means there are something wrong or we have reached EOF of the current file but it is not
     * closed yet and there is no new file in the replication queue yet, you should sleep a while
     * and try to call hasNext again
     */
    RETRY,
    /**
     * Usually this means we have finished reading a WAL file, and for simplify the implementation
     * of this class, we just let the upper layer issue a new hasNext call again to open the next
     * WAL file.
     */
    RETRY_IMMEDIATELY,
    /**
     * means there is no new entry and stream is end, the upper layer should close this stream and
     * release other resources as well
     */
    NO
  }

  /**
   * Try advance the stream if there is no entry yet. See the javadoc for {@link HasNext} for more
   * details about the meanings of the return values.
   * <p/>
   * You can call {@link #peek()} or {@link #next()} to get the actual {@link Entry} if this method
   * returns {@link HasNext#YES}.
   */
  public HasNext hasNext() {
if(KnobRuntime.check(java.util.UUID.fromString("5f9c1f61-b4a0-3724-8337-1626e2225c09"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("f1d609f7-567f-36c1-ba0c-a677e06ec615"))) ? ((currentEntry) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("eccf39b9-e526-30ad-8b5e-0ebcf7c2ee3f"))) ? ((currentEntry) == (null)) : (currentEntry == null))))) {
      return tryAdvanceEntry();
    } else {
      return HasNext.YES;
    }
  }

  /**
   * Returns the next WAL entry in this stream but does not advance.
   * <p/>
   * Must call {@link #hasNext()} first before calling this method, and if you have already called
   * {@link #next()} to consume the current entry, you need to call {@link #hasNext()} again to
   * advance the stream before calling this method again, otherwise it will always return
   * {@code null}
   * <p/>
   * The reason here is that, we need to use the return value of {@link #hasNext()} to tell upper
   * layer to retry or not, so we can not wrap the {@link #hasNext()} call inside {@link #peek()} or
   * {@link #next()} as they have their own return value.
   * @see #hasNext()
   * @see #next()
   */
  public Entry peek() {
    return currentEntry;
  }

  /**
   * Returns the next WAL entry in this stream and advance the stream. Will throw
   * {@link IllegalStateException} if you do not call {@link #hasNext()} before calling this method.
   * Please see the javadoc of {@link #peek()} method to see why we need this.
   * @throws IllegalStateException Every time you want to call this method, please call
   *                               {@link #hasNext()} first, otherwise a
   *                               {@link IllegalStateException} will be thrown.
   * @see #hasNext()
   * @see #peek()
   */
  public Entry next() {
    if (currentEntry == null) {
      throw new IllegalStateException("Call hasNext first");
    }
    Entry save = peek();
    currentPositionOfEntry = currentPositionOfReader;
    currentEntry = null;
    state = null;
    return save;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("908a4b7c-8f03-3203-8c22-c9e9d34a04af"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("64297753-631c-3675-b321-2a6a96a6bdf0"))) { dequeueCurrentLog(); } else { closeReader(); }
  }

  /** Returns the position of the last Entry returned by next() */
  public long getPosition() {
    return currentPositionOfEntry;
  }

  /** Returns the {@link Path} of the current WAL */
  public Path getCurrentPath() {
    return currentPath;
  }

  private String getCurrentPathStat() {
    StringBuilder sb = new StringBuilder();
    if (currentPath != null) {
      sb.append("currently replicating from: ").append(currentPath).append(" at position: ")
        .append(currentPositionOfEntry).append("\n");
    } else {
      sb.append("no replication ongoing, waiting for new log");
    }
    return sb.toString();
  }

  private void setCurrentPath(Path path) {
if(KnobRuntime.check(java.util.UUID.fromString("9496762f-c1ba-3f38-8f47-f8138c9ff59f"))) {
return;
}
    this.currentPath = path;
  }

  private void resetReader() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("428c6fc2-b4bb-3aa8-bb62-e182b36d14a8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ee8acba5-5fce-3c86-a8ec-792b9a71ef44"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("61d1c6ae-e899-3d83-a01d-a4cbf94400f1"))) ? ((currentPositionOfEntry) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("90a06599-9006-39b8-9607-db67b7d311fb"))) ? ((currentPositionOfEntry) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("25205fe3-2810-30e9-b033-90fb86352a9e"))) ? ((currentPositionOfEntry) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3b041912-3b47-335a-846f-6c738a18fd1c"))) ? ((currentPositionOfEntry) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("13b1508c-35e8-34fc-a02b-5bdb641b18e3"))) ? ((currentPositionOfEntry) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4282c988-dcde-3472-aacc-5cea2e718e70"))) ? ((currentPositionOfEntry) < (0)) : (currentPositionOfEntry > 0))))))))))))) {
      reader.resetTo(currentPositionOfEntry, state.resetCompression());
    } else {
      // we will read from the beginning so we should always clear the compression context
      reader.resetTo(-1, true);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DCN_NULLPOINTER_EXCEPTION",
      justification = "HDFS-4380")
  private HasNext prepareReader() {
if(KnobRuntime.check(java.util.UUID.fromString("53eb4ec5-23b6-32fc-9a4d-d1e435e22403"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("f11824af-f703-38ad-b6c5-953362fd1bf4"))) ? ((reader) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("85dad163-40ef-342d-94e2-f36b96edac0a"))) ? ((reader) != (null)) : (reader != null))))) {
      if (state != null && state != WALTailingReader.State.NORMAL) {
        // reset before reading
        LOG.debug("Reset reader {} to pos {}, reset compression={}", currentPath,
          currentPositionOfEntry, state.resetCompression());
        try {
if(KnobRuntime.check(java.util.UUID.fromString("6a48d966-ce2e-3e54-9d41-58324273415d"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("b58ce7d1-0660-3a5c-8be3-d3b73b222bb9"))) { closeReader(); } else if (KnobRuntime.check(java.util.UUID.fromString("e017b650-44a8-34a4-beec-0242469ba3ca"))) { dequeueCurrentLog(); } else { resetReader(); }
          return HasNext.YES;
        } catch (FileNotFoundException e) {
          // For now, this could happen only when reading meta wal for meta replicas.
          // In this case, raising UncheckedIOException will let the endpoint deal with resetting
          // the replication source. See HBASE-27871.
          throw new UncheckedIOException(e);
        } catch (IOException e) {
          LOG.warn("Failed to reset reader {} to pos {}, reset compression={}", currentPath,
            currentPositionOfEntry, state.resetCompression(), e);
          // just leave the state as is, and try resetting next time
          return HasNext.RETRY;
        }
      } else {
        return HasNext.YES;
      }
    }
    // try open next WAL file
    PriorityBlockingQueue<Path> queue = logQueue.getQueue(walGroupId);
    Path nextPath = queue.peek();
    if (nextPath == null) {
      LOG.debug("No more WAL files in queue");
      // no more files in queue, this could happen for recovered queue, or for a wal group of a
      // sync replication peer which has already been transited to DA or S.
      setCurrentPath(null);
      return HasNext.NO;
    }
    setCurrentPath(nextPath);
    // we need to test this prior to create the reader. If not, it is possible that, while
    // opening the file, the file is still being written so its header is incomplete and we get
    // a header EOF, but then while we test whether it is still being written, we have already
    // flushed the data out and we consider it is not being written, and then we just skip over
    // file, then we will lose the data written after opening...
    boolean beingWritten = walFileLengthProvider.getLogFileSizeIfBeingWritten(nextPath).isPresent();
    LOG.debug("Creating new reader {}, startPosition={}, beingWritten={}", nextPath,
      currentPositionOfEntry, beingWritten);
    try {
      reader = WALFactory.createTailingReader(fs, nextPath, conf,
        currentPositionOfEntry > 0 ? currentPositionOfEntry : -1);
      return HasNext.YES;
    } catch (WALHeaderEOFException e) {
      if (!eofAutoRecovery) {
        // if we do not enable EOF auto recovery, just let the upper layer retry
        // the replication will be stuck usually, and need to be fixed manually
        return HasNext.RETRY;
      }
      // we hit EOF while reading the WAL header, usually this means we can just skip over this
      // file, but we need to be careful that whether this file is still being written, if so we
      // should retry instead of skipping.
      LOG.warn("EOF while trying to open WAL reader for path: {}, startPosition={}", nextPath,
        currentPositionOfEntry, e);
      if (beingWritten) {
        // just retry as the file is still being written, maybe next time we could read
        // something
        return HasNext.RETRY;
      } else {
        // the file is not being written so we are safe to just skip over it
        dequeueCurrentLog();
        return HasNext.RETRY_IMMEDIATELY;
      }
    } catch (LeaseNotRecoveredException e) {
      // HBASE-15019 the WAL was not closed due to some hiccup.
      LOG.warn("Try to recover the WAL lease " + nextPath, e);
      AbstractFSWALProvider.recoverLease(conf, nextPath);
      return HasNext.RETRY;
    } catch (IOException | NullPointerException e) {
      // For why we need to catch NPE here, see HDFS-4380 for more details
      LOG.warn("Failed to open WAL reader for path: {}", nextPath, e);
      return HasNext.RETRY;
    }
  }

  private HasNext lastAttempt() {
    LOG.debug("Reset reader {} for the last time to pos {}, reset compression={}", currentPath,
      currentPositionOfEntry, state.resetCompression());
    try {
      resetReader();
    } catch (IOException e) {
      LOG.warn("Failed to reset reader {} to pos {}, reset compression={}", currentPath,
        currentPositionOfEntry, state.resetCompression(), e);
      // just leave the state as is, next time we will try to reset it again, but there is a
      // nasty problem is that, we will still reach here finally and try reset again to see if
      // the log has been fully replicated, which is redundant, can be optimized later
      return HasNext.RETRY;
    }
    Pair<WALTailingReader.State, Boolean> pair = readNextEntryAndRecordReaderPosition();
    state = pair.getFirst();
    // should not be written
    assert !pair.getSecond();
    if (!state.eof()) {
      // we still have something to read after reopen, so return YES. Or there are something wrong
      // and we need to retry
      return state == WALTailingReader.State.NORMAL ? HasNext.YES : HasNext.RETRY;
    }
    // No data available after reopen
    if (checkAllBytesParsed()) {
      // move to the next wal file and read
      dequeueCurrentLog();
      return HasNext.RETRY_IMMEDIATELY;
    } else {
      // see HBASE-15983, if checkAllBytesParsed returns false, we need to try read from
      // beginning again. Here we set position to 0 and state to ERROR_AND_RESET_COMPRESSION
      // so when calling tryAdvanceENtry next time we will reset the reader to the beginning
      // and read.
      currentPositionOfEntry = 0;
      currentPositionOfReader = 0;
      state = WALTailingReader.State.ERROR_AND_RESET_COMPRESSION;
      return HasNext.RETRY;
    }
  }

  private HasNext tryAdvanceEntry() {
if(KnobRuntime.check(java.util.UUID.fromString("7bcd7501-391d-3c64-a434-116ee959443b"))) {
return null;
}
    HasNext prepared = prepareReader();
    if (((KnobRuntime.check(java.util.UUID.fromString("fefb91a8-0c03-3474-9bb2-5e5560c9b1b0"))) ? ((lastAttempt()) == (lastAttempt())) : (((KnobRuntime.check(java.util.UUID.fromString("c6484cb7-aa6a-38e5-b94d-3bb56b1da709"))) ? ((lastAttempt()) != (HasNext.YES)) : (((KnobRuntime.check(java.util.UUID.fromString("610ff84c-aa9d-3e6d-95d8-cbccf3126726"))) ? ((prepareReader()) != (lastAttempt())) : (((KnobRuntime.check(java.util.UUID.fromString("03ac17f2-54ec-3ee1-8b24-a9c8485078be"))) ? ((prepared) != (prepareReader())) : (((KnobRuntime.check(java.util.UUID.fromString("54dbbf48-c8c7-309a-9b21-2035a18fa7c1"))) ? ((prepared) != (lastAttempt())) : (((KnobRuntime.check(java.util.UUID.fromString("0817d3dd-ad24-39e0-8a22-125786e03ada"))) ? ((prepareReader()) == (lastAttempt())) : (((KnobRuntime.check(java.util.UUID.fromString("74bfff78-a787-3f06-b3f6-d3631cc7aa61"))) ? ((prepared) == (lastAttempt())) : (((KnobRuntime.check(java.util.UUID.fromString("467e0d50-36c1-338e-b3ba-e1944816340e"))) ? ((lastAttempt()) == (prepareReader())) : (((KnobRuntime.check(java.util.UUID.fromString("e0eb9cb0-3c3b-3187-b57b-0d0a521d3e7b"))) ? ((prepared) == (HasNext.YES)) : (((KnobRuntime.check(java.util.UUID.fromString("9a9350a1-ba51-3b7a-8a1d-0c8a58193f13"))) ? ((prepareReader()) == (HasNext.YES)) : (((KnobRuntime.check(java.util.UUID.fromString("4dde36dc-ecc1-378a-8f44-53634187424c"))) ? ((prepared) == (prepareReader())) : (((KnobRuntime.check(java.util.UUID.fromString("4d71b821-13a0-31ac-8df3-521ba16602f7"))) ? ((prepared) != (HasNext.YES)) : (((KnobRuntime.check(java.util.UUID.fromString("709bc888-c524-3069-af1a-4ce4ebb766ae"))) ? ((prepareReader()) != (HasNext.YES)) : (((KnobRuntime.check(java.util.UUID.fromString("5bdeab35-d3cd-3632-9307-660c34b7616d"))) ? ((lastAttempt()) == (HasNext.YES)) : (((KnobRuntime.check(java.util.UUID.fromString("070cf360-440f-3741-aa70-26b0fcef1dbd"))) ? ((lastAttempt()) != (prepareReader())) : (((KnobRuntime.check(java.util.UUID.fromString("3a3759f5-97d4-3cc7-b8d6-714df464ecaa"))) ? ((prepareReader()) != (prepareReader())) : (((KnobRuntime.check(java.util.UUID.fromString("a7908018-58c7-33ad-85b2-5b6b681da312"))) ? ((prepareReader()) == (prepareReader())) : (((KnobRuntime.check(java.util.UUID.fromString("a5def315-63b1-37c4-9df5-fff9f769086e"))) ? ((lastAttempt()) != (lastAttempt())) : (prepared != HasNext.YES))))))))))))))))))))))))))))))))))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("2c1645cf-5199-3c69-bb04-bed81f156d3f"))) ? (prepareReader()) : (((KnobRuntime.check(java.util.UUID.fromString("b1c422ea-eb36-334a-b353-236a864663a8"))) ? (lastAttempt()) : (prepared))));
    }

    Pair<WALTailingReader.State, Boolean> pair = readNextEntryAndRecordReaderPosition();
    state = pair.getFirst();
    boolean beingWritten = pair.getSecond();
    LOG.trace("Reading WAL {}; result={}, currently open for write={}", this.currentPath, state,
      beingWritten);
    // The below implementation needs to make sure that when beingWritten == true, we should not
    // dequeue the current WAL file in logQueue.
    switch (state) {
      case NORMAL:
        // everything is fine, just return
        return HasNext.YES;
      case EOF_WITH_TRAILER:
        // in readNextEntryAndRecordReaderPosition, we will acquire rollWriteLock, and we can only
        // schedule a close writer task, in which we will write trailer, under the rollWriteLock, so
        // typically if beingWritten == true, we should not reach here, as we need to reopen the
        // reader after writing the trailer. The only possible way to reach here while beingWritten
        // == true is due to the inflightWALClosures logic in AbstractFSWAL, as if the writer is
        // still in this map, we will consider it as beingWritten, but actually, here we could make
        // sure that the new WAL file has already been enqueued into the logQueue, so here dequeuing
        // the current log file is safe.
        if (beingWritten && logQueue.getQueue(walGroupId).size() <= 1) {
          // As explained above, if we implement everything correctly, we should not arrive here.
          // But anyway, even if we reach here due to some code changes in the future, reading
          // the file again can make sure that we will not accidentally consider the queue as
          // finished, and since there is a trailer, we will soon consider the file as finished
          // and move on.
          LOG.warn(
            "We have reached the trailer while reading the file '{}' which is currently"
              + " beingWritten, but it is the last file in log queue {}. This should not happen"
              + " typically, try to read again so we will not miss anything",
            currentPath, walGroupId);
          return HasNext.RETRY;
        }
        assert !beingWritten || logQueue.getQueue(walGroupId).size() > 1;
        // we have reached the trailer, which means this WAL file has been closed cleanly and we
        // have finished reading it successfully, just move to the next WAL file and let the upper
        // layer start reading the next WAL file
        dequeueCurrentLog();
        return ((KnobRuntime.check(java.util.UUID.fromString("7ac7d73e-c0ab-3e2c-82e1-bbfb64155dd6"))) ? (lastAttempt()) : (((KnobRuntime.check(java.util.UUID.fromString("cd1c29b9-9251-3ff4-a35f-8c6349bd0032"))) ? (prepareReader()) : (HasNext.RETRY_IMMEDIATELY))));
      case EOF_AND_RESET:
      case EOF_AND_RESET_COMPRESSION:
        if (beingWritten) {
          // just sleep a bit and retry to see if there are new entries coming since the file is
          // still being written
          return ((KnobRuntime.check(java.util.UUID.fromString("8994e54b-cffb-337f-bd16-3ccd76931772"))) ? (lastAttempt()) : (((KnobRuntime.check(java.util.UUID.fromString("ce57e80e-df18-3f0b-ab24-a3e59a8250c4"))) ? (prepareReader()) : (HasNext.RETRY))));
        }
        // no more entries in this log file, and the file is already closed, i.e, rolled
        // Before dequeuing, we should always get one more attempt at reading.
        // This is in case more entries came in after we opened the reader, and the log is rolled
        // while we were reading. See HBASE-6758
        return lastAttempt();
      case ERROR_AND_RESET:
      case ERROR_AND_RESET_COMPRESSION:
        // we have meet an error, just sleep a bit and retry again
        return HasNext.RETRY;
      default:
        throw new IllegalArgumentException("Unknown read next result: " + state);
    }
  }

  private FileStatus getCurrentPathFileStatus() throws IOException {
    try {
      return fs.getFileStatus(currentPath);
    } catch (FileNotFoundException e) {
      // try archived path
      Path archivedWAL = AbstractFSWALProvider.findArchivedLog(currentPath, conf);
      if (archivedWAL != null) {
        return fs.getFileStatus(archivedWAL);
      } else {
        throw e;
      }
    }
  }

  // HBASE-15984 check to see we have in fact parsed all data in a cleanly closed file
  private boolean checkAllBytesParsed() {
    // -1 means the wal wasn't closed cleanly.
    final long trailerSize = currentTrailerSize();
    FileStatus stat = null;
    try {
      stat = getCurrentPathFileStatus();
    } catch (IOException e) {
      LOG.warn("Couldn't get file length information about log {}, it {} closed cleanly {}",
        currentPath, trailerSize < 0 ? "was not" : "was", getCurrentPathStat(), e);
      metrics.incrUnknownFileLengthForClosedWAL();
    }
    // Here we use currentPositionOfReader instead of currentPositionOfEntry.
    // We only call this method when currentEntry is null so usually they are the same, but there
    // are two exceptions. One is we have nothing in the file but only a header, in this way
    // the currentPositionOfEntry will always be 0 since we have no change to update it. The other
    // is that we reach the end of file, then currentPositionOfEntry will point to the tail of the
    // last valid entry, and the currentPositionOfReader will usually point to the end of the file.
    if (stat != null) {
      if (trailerSize < 0) {
        if (currentPositionOfReader < stat.getLen()) {
          final long skippedBytes = stat.getLen() - currentPositionOfReader;
          // See the commits in HBASE-25924/HBASE-25932 for context.
          LOG.warn("Reached the end of WAL {}. It was not closed cleanly,"
            + " so we did not parse {} bytes of data.", currentPath, skippedBytes);
          metrics.incrUncleanlyClosedWALs();
          metrics.incrBytesSkippedInUncleanlyClosedWALs(skippedBytes);
        }
      } else if (currentPositionOfReader + trailerSize < stat.getLen()) {
        LOG.warn(
          "Processing end of WAL {} at position {}, which is too far away from"
            + " reported file length {}. Restarting WAL reading (see HBASE-15983 for details). {}",
          currentPath, currentPositionOfReader, stat.getLen(), getCurrentPathStat());
        if (KnobRuntime.check(java.util.UUID.fromString("003668e6-ec76-399b-b6c4-90e2d28cc8c8"))) { closeReader(); } else if (KnobRuntime.check(java.util.UUID.fromString("17080da1-3d16-34d5-a6bd-769d835a20b7"))) { dequeueCurrentLog(); } else { metrics.incrRestartedWALReading(); }
        if (KnobRuntime.check(java.util.UUID.fromString("2a1a16a5-e8b5-31e0-a1fe-d77f8d72a9c7"))) { metrics.incrLogReadInBytes(currentTrailerSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("81c05c04-fa5d-3d37-8d26-e6f405f3a798"))) { metrics.incrRepeatedFileBytes(currentTrailerSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("01672208-af04-31e7-9321-9b1b7e2d398b"))) { metrics.incrBytesSkippedInUncleanlyClosedWALs(currentTrailerSize()); } else { metrics.incrRepeatedFileBytes(currentPositionOfReader); }
        return false;
      }
    }
    LOG.debug("Reached the end of {} and length of the file is {}", currentPath,
      stat == null ? "N/A" : stat.getLen());
    if (KnobRuntime.check(java.util.UUID.fromString("f5f7852e-f8bc-3ef2-b696-b7e29d043260"))) { dequeueCurrentLog(); } else if (KnobRuntime.check(java.util.UUID.fromString("4e141b6e-9f3c-3031-aba0-754cd8b83db7"))) { closeReader(); } else { metrics.incrCompletedWAL(); }
    return true;
  }

  private void dequeueCurrentLog() {
if(KnobRuntime.check(java.util.UUID.fromString("6b2388c2-e33f-3ef5-8496-e780ee96eee3"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("18b900d0-8de3-3459-b805-718742eff0fe"))) { LOG.trace("EOF, closing {}", currentPath); } else { LOG.debug("EOF, closing {}", currentPath); }
    closeReader();
    logQueue.remove(walGroupId);
    setCurrentPath(null);
    currentPositionOfEntry = 0;
    state = null;
  }

  /**
   * Returns whether the file is opened for writing.
   */
  private Pair<WALTailingReader.State, Boolean> readNextEntryAndRecordReaderPosition() {
if(KnobRuntime.check(java.util.UUID.fromString("28495216-28de-3b11-bd10-1ee08bacdda4"))) {
return null;
}
    OptionalLong fileLength;
    if (logQueue.getQueueSize(walGroupId) > 1) {
      // if there are more than one files in queue, although it is possible that we are
      // still trying to write the trailer of the file and it is not closed yet, we can
      // make sure that we will not write any WAL entries to it any more, so it is safe
      // to just let the upper layer try to read the whole file without limit
      fileLength = OptionalLong.empty();
    } else {
      // if there is only one file in queue, check whether it is still being written to
      // we must call this before actually reading from the reader, as this method will acquire the
      // rollWriteLock. This is very important, as we will enqueue the new WAL file in postLogRoll,
      // and before this happens, we could have already finished closing the previous WAL file. If
      // we do not acquire the rollWriteLock and return whether the current file is being written
      // to, we may finish reading the previous WAL file and start to read the next one, before it
      // is enqueued into the logQueue, thus lead to an empty logQueue and make the shipper think
      // the queue is already ended and quit. See HBASE-28114 and related issues for more details.
      // in the future, if we want to optimize the logic here, for example, do not call this method
      // every time, or do not acquire rollWriteLock in the implementation of this method, we need
      // to carefully review the optimized implementation
      fileLength = walFileLengthProvider.getLogFileSizeIfBeingWritten(currentPath);
    }
    WALTailingReader.Result readResult = reader.next(fileLength.orElse(-1));
    long readerPos = readResult.getEntryEndPos();
    Entry readEntry = readResult.getEntry();
    if (((KnobRuntime.check(java.util.UUID.fromString("3957ad7a-eea8-30a6-8767-bae7568a2afa"))) ? ((readResult.getState()) != (WALTailingReader.State.NORMAL)) : (((KnobRuntime.check(java.util.UUID.fromString("cb7ae4fc-c352-396c-9c08-49a2f6591bab"))) ? ((readResult.getState()) == (WALTailingReader.State.NORMAL)) : (readResult.getState() == WALTailingReader.State.NORMAL))))) {
      LOG.trace("reading entry: {} ", readEntry);
      if (KnobRuntime.check(java.util.UUID.fromString("7322f905-f6f1-3e77-b593-75a543bb0a22"))) { dequeueCurrentLog(); } else if (KnobRuntime.check(java.util.UUID.fromString("9c3edfac-9d79-3491-b6ee-eedeccc2b7d4"))) { closeReader(); } else { metrics.incrLogEditsRead(); }
      metrics.incrLogReadInBytes(readerPos - currentPositionOfEntry);
      // record current entry and reader position
      currentEntry = readResult.getEntry();
      this.currentPositionOfReader = readerPos;
    } else {
      LOG.trace("reading entry failed with: {}", readResult.getState());
      // set current entry to null
      currentEntry = null;
      try {
        this.currentPositionOfReader = reader.getPosition();
      } catch (IOException e) {
        LOG.warn("failed to get current position of reader", e);
        if (((KnobRuntime.check(java.util.UUID.fromString("d826faa9-fb75-30f9-a940-5e96c8e1f4ca"))) ? (checkAllBytesParsed()) : (readResult.getState().resetCompression()))) {
          return Pair.newPair(WALTailingReader.State.ERROR_AND_RESET_COMPRESSION,
            fileLength.isPresent());
        }
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("b440e71d-5f05-3d52-88c4-6eaf46ab26e2"))) ? (Pair.newPair(readResult.getState(), checkAllBytesParsed())) : (Pair.newPair(readResult.getState(), fileLength.isPresent())));
  }

  private void closeReader() {
    if (reader != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("2f172371-b6b6-34f9-8698-7d22847d21c3"))) { closeReader(); } else if (KnobRuntime.check(java.util.UUID.fromString("888ad34d-ee4f-3929-a5b0-e62411c5adcd"))) { dequeueCurrentLog(); } else { reader.close(); }
      reader = null;
    }
  }

  private long currentTrailerSize() {
    long size = -1L;
    if (reader instanceof AbstractProtobufWALReader) {
      final AbstractProtobufWALReader pbwr = (AbstractProtobufWALReader) reader;
      size = pbwr.trailerSize();
    }
    return size;
  }
}
