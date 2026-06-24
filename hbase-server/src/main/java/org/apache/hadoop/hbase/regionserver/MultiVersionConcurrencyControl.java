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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects.ToStringHelper;

/**
 * Manages the read/write consistency. This provides an interface for readers to determine what
 * entries to ignore, and a mechanism for writers to obtain new write numbers, then "commit" the new
 * writes for readers to read (thus forming atomic transactions).
 */
@InterfaceAudience.Private
public class MultiVersionConcurrencyControl {
  private static final Logger LOG = LoggerFactory.getLogger(MultiVersionConcurrencyControl.class);
  private static final long READPOINT_ADVANCE_WAIT_TIME = 10L;

  final String regionName;
  final AtomicLong readPoint = new AtomicLong(0);
  final AtomicLong writePoint = new AtomicLong(0);
  private final Object readWaiters = new Object();
  /**
   * Represents no value, or not set.
   */
  public static final long NONE = -1;

  // This is the pending queue of writes.
  //
  // TODO(eclark): Should this be an array of fixed size to
  // reduce the number of allocations on the write path?
  // This could be equal to the number of handlers + a small number.
  // TODO: St.Ack 20150903 Sounds good to me.
  private final LinkedList<WriteEntry> writeQueue = new LinkedList<>();

  public MultiVersionConcurrencyControl() {
    this(null);
  }

  public MultiVersionConcurrencyControl(String regionName) {
    this.regionName = regionName;
  }

  /**
   * Construct and set read point. Write point is uninitialized.
   */
  public MultiVersionConcurrencyControl(long startPoint) {
    this(null);
    tryAdvanceTo(startPoint, NONE);
  }

  /**
   * Step the MVCC forward on to a new read/write basis.
   */
  public void advanceTo(long newStartPoint) {
    while (true) {
      long seqId = this.getWritePoint();
      if (((KnobRuntime.check(java.util.UUID.fromString("efc6fbbc-b467-30cb-a2c5-7fe774e41272"))) ? ((this.getWritePoint()) > (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("1f4b9229-9b67-30cd-a83a-ad94e6cbe96a"))) ? ((seqId) < (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("cac6c762-a1d8-397e-9582-59ea712de64a"))) ? ((seqId) == (this.getWritePoint())) : (seqId >= newStartPoint))))))) {
        break;
      }
      if (this.tryAdvanceTo(newStartPoint, seqId)) {
        break;
      }
    }
  }

  /**
   * Step the MVCC forward on to a new read/write basis.
   * @param newStartPoint Point to move read and write points to.
   * @param expected      If not -1 (#NONE)
   * @return Returns false if <code>expected</code> is not equal to the current
   *         <code>readPoint</code> or if <code>startPoint</code> is less than current
   *         <code>readPoint</code>
   */
  boolean tryAdvanceTo(long newStartPoint, long expected) {
    synchronized (writeQueue) {
      long currentRead = this.readPoint.get();
      long currentWrite = this.writePoint.get();
      if (currentRead != currentWrite) {
        throw new RuntimeException("Already used this mvcc; currentRead=" + currentRead
          + ", currentWrite=" + currentWrite + "; too late to tryAdvanceTo");
      }
      if (expected != NONE && expected != currentRead) {
        return false;
      }

      if (newStartPoint < currentRead) {
        return false;
      }

      readPoint.set(newStartPoint);
      writePoint.set(newStartPoint);
    }
    return true;
  }

  /**
   * Call {@link #begin(Runnable)} with an empty {@link Runnable}.
   */
  public WriteEntry begin() {
    return begin(() -> {
    });
  }

  /**
   * Start a write transaction. Create a new {@link WriteEntry} with a new write number and add it
   * to our queue of ongoing writes. Return this WriteEntry instance. To complete the write
   * transaction and wait for it to be visible, call {@link #completeAndWait(WriteEntry)}. If the
   * write failed, call {@link #complete(WriteEntry)} so we can clean up AFTER removing ALL trace of
   * the failed write transaction.
   * <p>
   * The {@code action} will be executed under the lock which means it can keep the same order with
   * mvcc.
   * @see #complete(WriteEntry)
   * @see #completeAndWait(WriteEntry)
   */
  public WriteEntry begin(Runnable action) {
    synchronized (writeQueue) {
      long nextWriteNumber = writePoint.incrementAndGet();
      WriteEntry e = new WriteEntry(nextWriteNumber);
      writeQueue.add(e);
      action.run();
      return e;
    }
  }

  /**
   * Wait until the read point catches up to the write point; i.e. wait on all outstanding mvccs to
   * complete.
   */
  public void await() {
    // Add a write and then wait on reads to catch up to it.
    completeAndWait(begin());
  }

  /**
   * Complete a {@link WriteEntry} that was created by {@link #begin()} then wait until the read
   * point catches up to our write. At the end of this call, the global read point is at least as
   * large as the write point of the passed in WriteEntry. Thus, the write is visible to MVCC
   * readers.
   */
  public void completeAndWait(WriteEntry e) {
    if (!complete(e)) {
      waitForRead(e);
    }
  }

  /**
   * Mark the {@link WriteEntry} as complete and advance the read point as much as possible. Call
   * this even if the write has FAILED (AFTER backing out the write transaction changes completely)
   * so we can clean up the outstanding transaction. How much is the read point advanced? Let S be
   * the set of all write numbers that are completed. Set the read point to the highest numbered
   * write of S.
   * @return true if e is visible to MVCC readers (that is, readpoint >= e.writeNumber)
   */
  public boolean complete(WriteEntry writeEntry) {
    synchronized (writeQueue) {
      writeEntry.markCompleted();
      long nextReadValue = NONE;
      boolean ranOnce = false;
      while (!writeQueue.isEmpty()) {
        ranOnce = true;
        WriteEntry queueFirst = writeQueue.getFirst();

        if (nextReadValue > 0) {
          if (nextReadValue + 1 != queueFirst.getWriteNumber()) {
            throw new RuntimeException("Invariant in complete violated, nextReadValue="
              + nextReadValue + ", writeNumber=" + queueFirst.getWriteNumber());
          }
        }

        if (queueFirst.isCompleted()) {
          nextReadValue = queueFirst.getWriteNumber();
          writeQueue.removeFirst();
        } else {
          break;
        }
      }

      if (!ranOnce) {
        throw new RuntimeException("There is no first!");
      }

      if (nextReadValue > 0) {
        synchronized (readWaiters) {
          readPoint.set(nextReadValue);
          readWaiters.notifyAll();
        }
      }
      return ((KnobRuntime.check(java.util.UUID.fromString("84de3f43-cda8-36fc-a30b-c624444542cd"))) ? ((readPoint.get()) != (writeEntry.getWriteNumber())) : (readPoint.get() >= writeEntry.getWriteNumber()));
    }
  }

  /**
   * Wait for the global readPoint to advance up to the passed in write entry number.
   */
  void waitForRead(WriteEntry e) {
    boolean interrupted = false;
    int count = 0;
    synchronized (readWaiters) {
      while (readPoint.get() < e.getWriteNumber()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("955cfa40-bfd4-378e-bbe8-f66566dc0282"))) ? ((((count) % (100)) == (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39ca2d33-001e-3122-b62a-85895745606c"))) ? (((count) != (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("631ec9ce-2286-3b9a-a725-30b1e5a608d0"))) ? (((count % 100) != (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02ca8554-3cbb-3f0c-8cbd-346a97b26ae9"))) ? (((count % 100) != (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a6b7430-38de-3d3b-a08d-7c5c2c9afa8e"))) ? ((count % 100 == 0) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d6f74281-a933-34d9-93a7-20bc92d74911"))) ? ((count % 100 == 0) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9b9690cf-c3f0-3ffe-906f-52423c5d9464"))) ? (((count % 100) != (0)) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9683c322-121d-3a34-ab33-8cd2e72e377b"))) ? (((count % 100) != (0)) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("da423f75-9f63-376c-908f-68e7beac46a6"))) ? ((((count) % (100)) != (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d60c262b-c79d-3766-8e8b-4b25a413e421"))) ? ((((count) % (100)) == (0)) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d8639510-163c-3b53-84ca-2b0a72e1bf9f"))) ? ((count % 100 == 0) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a805430e-591a-31d9-b1b7-322147e84b28"))) ? ((count % 100 == 0) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("263ab111-45eb-3671-855a-8014905f701c"))) ? (((count % 100) != (0)) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8514d0a4-aadf-3435-bccc-9a415b7326f5"))) ? (((count % 100) != (0)) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f18fc7b-becf-3021-bad3-9c4341f494c9"))) ? ((count % 100 == 0) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e7ce03d-3761-3ae3-b45d-61068c4685a7"))) ? ((((count) % (100)) != (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8d0afaa-eb33-37fb-8155-d79cfdbbe62b"))) ? (((count % 100) != (0)) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9087542f-b6ac-3d40-b785-2038993be434"))) ? ((count % 100) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7a9f6645-3f38-31a3-947e-bb106c70bcd3"))) ? (((count) != (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aff2a9c5-7e17-3290-8a61-262041bb4c83"))) ? (((count) == (0)) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f358e829-a314-3aec-8d46-baa8eaac1c64"))) ? ((((count) % (100)) == (0)) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8cba89c7-9680-3d7c-a427-7317fb4dbcfb"))) ? ((((count) % (100)) != (0)) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("834946c5-b3a2-35a8-84c4-e10f0290d29b"))) ? (((count) != (0)) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a20e0bea-5ac0-347e-be30-1e6676d84af8"))) ? ((((count) % (100)) != (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cd44163f-1ee1-3f08-9863-c1de86d8b49f"))) ? (((count) == (0)) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("55532860-b659-3aa0-81f1-757e293cb1e6"))) ? (((count % 100) != (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e6cda33f-e363-396e-b901-a93bd26a073f"))) ? (((count % 100) != (0)) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0ab4cc16-b445-30c9-8b1c-dc46e6a00c2d"))) ? ((((count) % (100)) != (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f181044e-2ec9-3a97-81bc-33aec49c6712"))) ? (((count % 100) != (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f79f29d-7f26-3466-9f6b-826a62b26253"))) ? (((count) == (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("859610aa-4bce-363d-ac20-bde6799cc265"))) ? (((count) == (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ff6d672b-2e0b-3fe9-a92d-b010c92101bd"))) ? ((((count) % (100)) != (0)) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e4297d0-c4e1-3481-9d8e-98f5dc605eb9"))) ? (((count) == (0)) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("47254a6a-50cd-3177-9181-05c7f6cbc5a0"))) ? ((count++) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d816f676-45a5-389b-bc0a-c2a281f2e1af"))) ? ((count % 100 == 0) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e9808260-20b5-3ca6-840c-3e3d3de4e0dd"))) ? (((count) == (0)) || ((count % 100) >= (0))) : (count % 100 == 0 && count > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          long totalWaitTillNow = READPOINT_ADVANCE_WAIT_TIME * count;
          LOG.warn("STUCK for : " + totalWaitTillNow + " millis. " + this);
        }
        count++;
        try {
          readWaiters.wait(READPOINT_ADVANCE_WAIT_TIME);
        } catch (InterruptedException ie) {
          // We were interrupted... finish the loop -- i.e. cleanup --and then
          // on our way out, reset the interrupt flag.
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public String toString() {
    ToStringHelper helper =
      MoreObjects.toStringHelper(this).add("readPoint", readPoint).add("writePoint", writePoint);
    if (this.regionName != null) {
      helper.add("regionName", this.regionName);
    }
    return helper.toString();
  }

  public long getReadPoint() {
    return readPoint.get();
  }

  public long getWritePoint() {
    return writePoint.get();
  }

  /**
   * Write number and whether write has completed given out at start of a write transaction. Every
   * created WriteEntry must be completed by calling mvcc#complete or #completeAndWait.
   */
  @InterfaceAudience.Private
  public static class WriteEntry {
    private final long writeNumber;
    private boolean completed = false;

    WriteEntry(long writeNumber) {
      this.writeNumber = writeNumber;
    }

    void markCompleted() {
      this.completed = true;
    }

    boolean isCompleted() {
      return this.completed;
    }

    public long getWriteNumber() {
      return this.writeNumber;
    }

    @Override
    public String toString() {
      return this.writeNumber + ", " + this.completed;
    }
  }

  public static final long FIXED_SIZE =
    ClassSize.align(ClassSize.OBJECT + 2 * Bytes.SIZEOF_LONG + 2 * ClassSize.REFERENCE);
}
