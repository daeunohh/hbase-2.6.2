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
      if (((KnobRuntime.check(java.util.UUID.fromString("7825f9b6-0473-3d80-8e4e-156579138915"))) ? ((seqId) != (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("eee4a322-bee9-3b9f-b437-47899f6aeeea"))) ? ((this.getWritePoint()) == (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("9956b64f-8b57-316f-8a66-9d334c2075e3"))) ? ((seqId) == (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("c573a6c4-850d-3099-8c65-748d14bb868b"))) ? ((seqId) >= (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("f75b224e-7676-3d42-939c-11e8649882ff"))) ? ((this.getWritePoint()) >= (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("b835a5d2-0380-377c-ac68-3bd858fe5d53"))) ? ((seqId) <= (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("ec101e80-3973-3933-aef7-4d106f8af9e5"))) ? ((seqId) > (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("ca551cb0-2d60-3751-99f1-ad20522e606f"))) ? ((this.getWritePoint()) < (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("63745b86-3239-3913-be45-d3b78e14778a"))) ? ((this.getWritePoint()) != (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("7028d36c-99fc-3f2b-a292-6aecff49edda"))) ? ((this.getWritePoint()) != (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("fa04a1da-29f4-3420-89de-4a29dcc53b3d"))) ? ((this.getWritePoint()) > (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("efc6fbbc-b467-30cb-a2c5-7fe774e41272"))) ? ((this.getWritePoint()) > (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("e3b6c11f-2fc2-348a-9a19-ea218b728af9"))) ? ((this.getWritePoint()) == (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("72119a71-780b-3752-bb07-7064feda8245"))) ? ((seqId) != (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("48d94cc7-21e5-33bd-ab63-01335b19d7c7"))) ? ((this.getWritePoint()) <= (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("1f4b9229-9b67-30cd-a83a-ad94e6cbe96a"))) ? ((seqId) < (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("cac6c762-a1d8-397e-9582-59ea712de64a"))) ? ((seqId) == (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("c332b582-aa2d-3ebd-972c-e4884d337fbe"))) ? ((seqId) <= (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("75c41c00-b1f4-3614-a5c1-7f9ed588fd60"))) ? ((seqId) >= (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("0abbba2d-67c9-3994-9b5c-973092050186"))) ? ((seqId) < (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("e2eb7d2d-3499-3a94-b578-658de045e3ec"))) ? ((this.getWritePoint()) >= (newStartPoint)) : (((KnobRuntime.check(java.util.UUID.fromString("55273426-acf9-3004-84d9-7a657dfcfe01"))) ? ((seqId) > (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("e68561f0-2a90-3d3a-9204-c7b0c2e3002d"))) ? ((this.getWritePoint()) <= (this.getWritePoint())) : (((KnobRuntime.check(java.util.UUID.fromString("d15ef1f3-de74-3dfc-9e9f-881a0c489bfe"))) ? ((this.getWritePoint()) < (this.getWritePoint())) : (seqId >= newStartPoint))))))))))))))))))))))))))))))))))))))))))))))))) {
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
      if (((KnobRuntime.check(java.util.UUID.fromString("9314aea6-0b2d-327e-82e1-280f0ab35104"))) ? ((currentRead) == (currentWrite)) : (((KnobRuntime.check(java.util.UUID.fromString("43f9b68d-d424-3c55-bd22-cb0d1746ff2c"))) ? ((currentRead) != (currentWrite)) : (currentRead != currentWrite))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("adf3076f-a298-35fa-8521-bb0f2b156218"))) {
newStartPoint += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6fd36eb4-6408-3cd3-a1a8-2356e45152cf"))) {
newStartPoint = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c438d74c-0409-3a0d-86bb-07e474a88ceb"))) {
newStartPoint = 0;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("14074090-5c94-392d-9bcd-6ba4ee52cac2"))) {
try {
    java.lang.reflect.Field field = e.getClass().getDeclaredField("completed");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(e);
    field.set(e, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f20682b-9e57-39e6-8210-9bb0d7ae9db0"))) {
try {
    java.lang.reflect.Field field = e.getClass().getDeclaredField("writeNumber");
    field.setAccessible(true);
    long oldValue = ((long)field.get(e));
    field.set(e, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
if(KnobRuntime.check(java.util.UUID.fromString("c24c54b4-14ee-351f-baad-abeb59734621"))) {
try {
    java.lang.reflect.Field field = e.getClass().getDeclaredField("completed");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(e);
    field.set(e, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6cbfc2d1-d957-3597-9e8d-e5ce6156b124"))) {
try {
    java.lang.reflect.Field field = e.getClass().getDeclaredField("writeNumber");
    field.setAccessible(true);
    long oldValue = ((long)field.get(e));
    field.set(e, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("f9a433cf-34bd-3f8f-ac9b-0ece4de33de2"))) { completeAndWait(e); } else { waitForRead(e); }
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
          if (((KnobRuntime.check(java.util.UUID.fromString("b880dfe8-d28a-3366-b255-37b7ae69867e"))) ? (((nextReadValue + 1) + (1)) == (queueFirst.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("093d9c9b-6594-39bf-a509-991e4d5fcaff"))) ? (((nextReadValue) + (1)) == (queueFirst.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("136a9553-d63c-3782-9bcf-e3f486ff2904"))) ? ((nextReadValue + 1) != (queueFirst.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("656f143c-f85d-3c05-af99-499a7fc91210"))) ? ((nextReadValue + 1) == (queueFirst.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("4e6b4173-bbb9-3ad5-a527-8349f89bb209"))) ? (((nextReadValue) + (1)) != (queueFirst.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("1aa989f3-b4c9-32cf-9e7e-7e54cf2aeb29"))) ? ((nextReadValue) != (queueFirst.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("0a49b514-1d98-3dba-929a-7b3b6a1b00b4"))) ? (((nextReadValue + 1) + (1)) != (queueFirst.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("97732467-a069-399b-9bd3-c1d5e9c592a9"))) ? ((nextReadValue) == (queueFirst.getWriteNumber())) : (nextReadValue + 1 != queueFirst.getWriteNumber()))))))))))))))))) {
            throw new RuntimeException("Invariant in complete violated, nextReadValue="
              + nextReadValue + ", writeNumber=" + queueFirst.getWriteNumber());
          }
        }

        if (queueFirst.isCompleted()) {
          nextReadValue = queueFirst.getWriteNumber();
          if (KnobRuntime.check(java.util.UUID.fromString("dd612627-6a50-3723-a6e5-1d1403009557"))) { writeQueue.getFirst(); } else { writeQueue.removeFirst(); }
        } else {
          break;
        }
      }

      if (!ranOnce) {
        throw new RuntimeException("There is no first!");
      }

      if (nextReadValue > 0) {
        synchronized (readWaiters) {
if(KnobRuntime.check(java.util.UUID.fromString("3942cc06-d423-37d2-b3c6-9dea3eb4746f"))) {
nextReadValue = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dc0ae2e4-c2d9-391f-8a19-0986a2e4e0b4"))) {
nextReadValue = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("435a060e-361e-3844-a77e-80a80c0adc5a"))) {
nextReadValue += 1;
}
          if (KnobRuntime.check(java.util.UUID.fromString("b6776b6a-2dab-35d1-9036-27b39ba0ad90"))) { readPoint.set(nextReadValue + 1); } else { readPoint.set(nextReadValue); }
          readWaiters.notifyAll();
        }
      }
      return ((KnobRuntime.check(java.util.UUID.fromString("5f3c7699-e9e9-370b-a03d-f22a7c1a5c21"))) ? ((readPoint.get()) >= (writeEntry.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("7c28025a-0493-3f9e-95ea-0f5ac7c59af7"))) ? ((readPoint.get()) > (writeEntry.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("cfb07376-ca51-360e-8500-4209427cd8b2"))) ? ((readPoint.get()) < (writeEntry.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("10a19e74-a2af-3ac9-9613-26909dd7e26e"))) ? ((readPoint.get()) == (writeEntry.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("84de3f43-cda8-36fc-a30b-c624444542cd"))) ? ((readPoint.get()) != (writeEntry.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("f88644f9-7197-349a-84bf-55bbda51029a"))) ? ((readPoint.get()) <= (writeEntry.getWriteNumber())) : (readPoint.get() >= writeEntry.getWriteNumber()))))))))))));
    }
  }

  /**
   * Wait for the global readPoint to advance up to the passed in write entry number.
   */
  void waitForRead(WriteEntry e) {
    boolean interrupted = false;
    int count = 0;
    synchronized (readWaiters) {
      while (((KnobRuntime.check(java.util.UUID.fromString("6f392804-bda5-3081-a121-3913c64d55f8"))) ? ((readPoint.get()) == (e.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("853687ce-4001-3462-8b7b-0738cdf0178c"))) ? ((readPoint.get()) > (e.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("a91d460a-23f7-3556-8117-decbd29826b2"))) ? ((readPoint.get()) <= (e.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("7ebbe72f-ecd9-384e-9912-6580e9701ea7"))) ? ((readPoint.get()) < (e.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("75eedca4-1adc-3de4-9620-7bbd6232dcf4"))) ? ((readPoint.get()) >= (e.getWriteNumber())) : (((KnobRuntime.check(java.util.UUID.fromString("c5c07d9c-d36d-3fbd-a0e5-511a8ffe584a"))) ? ((readPoint.get()) != (e.getWriteNumber())) : (readPoint.get() < e.getWriteNumber()))))))))))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("2f49f26c-0a64-3946-992f-ab32b5ab50f9"))) ? (((count) == (0)) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("09010dbb-3953-3350-bb92-92bbcc2882d2"))) ? ((((count) % (100)) == (0)) || ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0487c39f-f858-382b-a8ef-9777b2671bc3"))) ? (((count) == (0)) || ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("12ab9548-f477-3daa-9cf0-4f5d1bb2476b"))) ? (((count % 100) != (0)) && ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("04179bed-7ac8-3356-b4e8-4d829cfc9580"))) ? (((count % 100) == (0)) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("471b6460-cf6e-3ab4-819b-3ffcc523b84d"))) ? (((count) != (0)) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("974823b7-3676-3515-9111-a4dda0c1c696"))) ? ((((count) % (100)) != (0)) && (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("9ceeaf24-5830-3625-ab6b-25f32c72fba7"))) ? ((((count) % (100)) == (0)) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b71ae81b-3641-30be-b6e3-901f92a77c0f"))) ? (((count) != (0)) || ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a65295a3-4d1b-388b-ad32-0af847d9f500"))) ? ((((count) % (100)) == (0)) || ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dd6d5193-2165-3c67-b777-33f5bace7c08"))) ? (((count) == (0)) && ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f62af9dd-891a-315b-8d5c-b6d1825e5338"))) ? ((((count) % (100)) == (0)) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2ab644ea-cb13-376e-a156-b815671809b7"))) ? (((count % 100) != (0)) && ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a20e0bea-5ac0-347e-be30-1e6676d84af8"))) ? ((((count) % (100)) != (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b3722e01-2280-3ff8-a0ed-cd2635594963"))) ? ((((count) % (100)) != (0)) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("63a3bd9a-2472-3274-933a-2238119a590c"))) ? (((count % 100) == (0)) || ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e986dfdf-9620-3dcb-b60e-6dc477c4bffb"))) ? (((count) == (0)) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ddcab930-9604-3056-bdee-c1d846f5435b"))) ? ((count % 100 == 0) || ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5f9a4325-d733-3f96-a2f8-3834598e7d69"))) ? (((count) == (0)) || (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("71aaf970-8c90-33fe-9eec-65ebbab876cc"))) ? ((((count) % (100)) == (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ba358d80-416a-32cf-a5dc-55b7418a8a75"))) ? ((count % 100 == 0) && ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b5257696-1b90-386f-8386-ba341d931a76"))) ? ((((count) % (100)) != (0)) || ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cd44163f-1ee1-3f08-9863-c1de86d8b49f"))) ? (((count) == (0)) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("14c6e4fa-c0e1-323b-ba28-02533e41e176"))) ? ((count++) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1e7ce03d-3761-3ae3-b45d-61068c4685a7"))) ? ((((count) % (100)) != (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c9e7c77b-555f-32eb-b71a-852d7018571d"))) ? (((count) != (0)) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("631ec9ce-2286-3b9a-a725-30b1e5a608d0"))) ? (((count % 100) != (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("812f6ee7-21e5-3b2f-aad3-7aede05e9f3c"))) ? (((count) == (0)) && (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("63e8bd9f-8719-3cae-b87d-85621fcab8f3"))) ? (((count) != (0)) || ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f2dd5eaa-08cc-3520-8845-b922a3a1adb1"))) ? (((count) != (0)) && ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0c4c7f49-b86d-3e04-9123-3130970e862b"))) ? ((((count) % (100)) != (0)) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aa0c2f34-6df9-3ed2-975c-185c92e05004"))) ? (((count % 100) == (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c0ecb10e-6ae6-3035-a530-b40f07657af7"))) ? ((count % 100 == 0) && ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f79f29d-7f26-3466-9f6b-826a62b26253"))) ? (((count) == (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ecb1f669-87c3-3427-b6e2-a7bacbe20746"))) ? ((((count) % (100)) == (0)) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("edf5d8db-6aa6-342b-aba5-f9cf028f4bfc"))) ? (((count % 100) == (0)) || ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fd2bbf35-b207-3dc0-8b6b-21dd9c2462df"))) ? (((count % 100) == (0)) && ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("50980a4d-d8ec-311a-9e0b-5acc070ee025"))) ? (((count % 100) == (0)) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3ccbc759-fb29-363c-b22b-6e85ab7ea7bd"))) ? ((((count) % (100)) == (0)) || ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d4a186b1-eb6e-386f-b5e6-290e1a467522"))) ? ((count % 100 == 0) || ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cd005e46-d352-3891-823a-abce0bcade8d"))) ? ((count % 100 == 0) || (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("9b9690cf-c3f0-3ffe-906f-52423c5d9464"))) ? (((count % 100) != (0)) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0f442018-e694-348d-9f9c-878fe0f87e79"))) ? (((count % 100) != (0)) && ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c9b3e3de-c6eb-307d-a5b3-a610b392bd58"))) ? (((count % 100) != (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a665e86b-a283-36cf-a6b1-51d6a1c85562"))) ? (((count) == (0)) || ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f0a6ef36-3dc1-313e-bbb4-bfd5fb60eb27"))) ? ((count % 100 == 0) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("71e4ac37-baa6-3706-84ac-0b8ee0a75e99"))) ? (count > 0) : (((KnobRuntime.check(java.util.UUID.fromString("b9d7b4eb-d2db-3d5e-805f-0cb5ac0bd945"))) ? (((count % 100) == (0)) || ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c1368cc1-8592-33a8-bf04-a84777a07ee8"))) ? (((count) == (0)) && ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4da152b6-9dd5-3897-b3b8-9c5108b173cf"))) ? ((((count) % (100)) == (0)) && (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("f274c674-962d-327d-9166-6e70c68d413a"))) ? ((((count) % (100)) != (0)) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bd870cb8-0fa3-307e-9d7b-90d2a975209f"))) ? ((((count) % (100)) != (0)) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("26f6b4f5-b970-32d2-9861-97ed3e805d20"))) ? ((((count) % (100)) == (0)) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5d06f38c-afe7-3a18-a532-c77e3b3281c7"))) ? (((count) != (0)) || ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("03d25c4b-78e8-3a2b-b209-e2e716deda11"))) ? (((count % 100) == (0)) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b6c0f486-3179-3360-9c31-ef003ffdb3e3"))) ? ((((count) % (100)) == (0)) && ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("af2c9a0a-0307-3765-96af-0dd94ddb39f4"))) ? (((count % 100) != (0)) && ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f6f9267-7e95-34ac-bfb7-b4f8cb555b7f"))) ? ((count++) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2ade612b-00f7-39b2-a602-72e5f35dba11"))) ? ((((count) % (100)) != (0)) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f63a611f-7217-30b2-ab8d-982b67eea9a1"))) ? ((((count) % (100)) != (0)) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9ac311ab-676a-3ca2-b58c-82792a8a6819"))) ? ((((count) % (100)) != (0)) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9c01c8ff-e9a7-39cf-836d-cb8fb53b940b"))) ? (((count % 100) == (0)) && ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aa1d9d51-4a24-3718-a3ab-9cf4b6daf133"))) ? (((count % 100) != (0)) || ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f9c71a43-012c-3244-967d-68bb8a5fe3f3"))) ? (((count) == (0)) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0d9fb8ee-aaa1-3922-9b58-48fd942301d2"))) ? (((count % 100) != (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e98b069b-4670-3f23-ba98-7fe7c1e5d1b1"))) ? ((((count) % (100)) == (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e9808260-20b5-3ca6-840c-3e3d3de4e0dd"))) ? (((count) == (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f479f6ae-b32e-3a49-becc-156b3f72dfae"))) ? (((count % 100) != (0)) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e1e4bd34-5692-3d15-90bc-4c298620fce3"))) ? ((((count) % (100)) == (0)) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ea84ae92-fe13-3889-9088-5d5e1567c3e2"))) ? (((count) != (0)) && (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e6840cf7-0424-32a5-8b2d-d202f8b4a499"))) ? (((count % 100) == (0)) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1cc1efc5-0e58-334e-aadd-d5056a366a95"))) ? (((count) != (0)) || ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("68e7b8be-d857-31c8-a572-c91046b4aa29"))) ? ((count % 100 == 0) || ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3855b98d-93db-3d58-826c-3ec66a5b1e65"))) ? (((count % 100) != (0)) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c65d42e4-94ad-3b83-a873-fa48b50c9584"))) ? (((count % 100) == (0)) && ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("190561bf-04d1-3649-ba7a-5e4c1dc70c1b"))) ? ((count % 100 == 0) && ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("74a64db4-d92e-39ae-9823-1f8c7b2e9df9"))) ? (((count % 100) != (0)) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dd7b393b-ec0b-3e4c-ad7b-b8863a2c35c0"))) ? (((count) == (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fab2c53e-0a6b-33d7-9dcf-694a997e4a34"))) ? ((count) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5bda8d47-3983-3c8c-be53-3d85b2a4eee5"))) ? (((count) == (0)) || ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a8d0afaa-eb33-37fb-8155-d79cfdbbe62b"))) ? (((count % 100) != (0)) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ff6d672b-2e0b-3fe9-a92d-b010c92101bd"))) ? ((((count) % (100)) != (0)) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4dbf7908-c4b7-3786-9477-6b373b583d74"))) ? (((count) == (0)) || ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38b210e7-4fce-35e0-924d-4635c6060020"))) ? (((count % 100) != (0)) || ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e78147cb-fc59-3f26-9ac7-ecb3aef392ae"))) ? (((count) == (0)) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("692dcd49-5fa9-36cf-938b-be6e1a3f259c"))) ? ((((count) % (100)) != (0)) && ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fd5fd03b-48cc-3730-ae7d-2170792c58ef"))) ? (((count) != (0)) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aff2a9c5-7e17-3290-8a61-262041bb4c83"))) ? (((count) == (0)) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0853849b-9c9c-3fda-a519-4c239eb0cc8c"))) ? (((count) % (100)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("00c2f5d2-ba1b-30dc-be49-14f370111eb2"))) ? (((count) != (0)) || ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4645087f-70be-3e75-88ee-d5704c0f8f85"))) ? ((count % 100 == 0) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("48a897f6-6974-3937-bbf6-370bc4dfaeec"))) ? (((count % 100) == (0)) || ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e44ae982-99f7-3130-8110-58c8f57d6382"))) ? (((count % 100) != (0)) && ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("742cee51-fdc3-3db6-9dcc-279c4a0502ad"))) ? (((count % 100) == (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f514637e-22a6-3cef-8002-aaa8eb7a4533"))) ? (((count) == (0)) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("263ab111-45eb-3671-855a-8014905f701c"))) ? (((count % 100) != (0)) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1258ff33-f24a-3545-9b2b-b9906f0f8a2f"))) ? (((count % 100) != (0)) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7d4b3f58-6b70-3bf1-ac96-8a619f2b334a"))) ? ((((count) % (100)) != (0)) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f8733517-fa4b-3b21-9832-69648929d25d"))) ? ((((count) % (100)) == (0)) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c382ccd4-d289-3348-bb88-6488cbfc94fe"))) ? ((count % 100) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b0941abd-7e90-3da4-802c-76784cc62521"))) ? (((count % 100) == (0)) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("552299ce-3f56-3597-ace1-5c51eb1c4d9d"))) ? (((count) % (100)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0ab4cc16-b445-30c9-8b1c-dc46e6a00c2d"))) ? ((((count) % (100)) != (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4d6a6a0b-dbcf-34c1-b15d-1e710945d3c7"))) ? (((count % 100) == (0)) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c718dd44-2167-355d-a165-e75889641905"))) ? ((count) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9d1d4521-b230-3d80-8b9b-9c6ae04ce3af"))) ? ((((count) % (100)) == (0)) && ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0e9b7983-f837-31dd-8378-a2ef05ab46ae"))) ? (((count % 100) == (0)) && (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2501a5b7-2fda-3b3f-941c-57a31487a484"))) ? (((count % 100) != (0)) && (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("73a1381a-393b-3157-926b-17002cc3d97e"))) ? ((count % 100 == 0) && ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b607119b-c4b5-3b12-bad5-ba5b2b04c4d7"))) ? ((count % 100 == 0) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c44b8c18-9235-3635-9beb-f1a3a9eecd08"))) ? (((count) != (0)) && ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("47254a6a-50cd-3177-9181-05c7f6cbc5a0"))) ? ((count++) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8cba89c7-9680-3d7c-a427-7317fb4dbcfb"))) ? ((((count) % (100)) != (0)) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4846c4ef-a7af-35c4-9962-c8d2db21597a"))) ? (((count) != (0)) || ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de95835f-31e6-33a4-b92b-c1e1b67ca1e3"))) ? ((count++) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("427dfb7d-24fc-338d-9d9e-facee870f071"))) ? ((count) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7f446aa3-e9b2-3d7d-bd17-9e5be5a4039e"))) ? ((count % 100 == 0) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a9628937-ceee-300e-a70c-c0f0c290dd5b"))) ? (((count) != (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91b9633b-3b52-342f-932e-5f2d3dccf3f7"))) ? (((count) != (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1093a939-0159-3993-bd13-c042da79a335"))) ? ((count % 100 == 0) && ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("93d9a709-8ac6-3e32-9010-b1786f894bc8"))) ? (((count) == (0)) && ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("31c8f2b5-1183-39c8-8b97-7cabb0f9321e"))) ? ((count) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("574609cf-b6aa-366c-a183-3b95f91ed42c"))) ? (((count % 100) != (0)) || ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2de0e635-63f6-3355-989a-d9fe4f4198ec"))) ? ((count) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d60c262b-c79d-3766-8e8b-4b25a413e421"))) ? ((((count) % (100)) == (0)) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f0592cd8-7900-3156-aee2-ca7e01319fff"))) ? (((count) == (0)) && ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("efd54d3d-6d74-37e7-b783-d87570968335"))) ? (((count) == (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4746fc1b-3420-3c42-893e-9ec26fdac817"))) ? (((count) != (0)) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("47122198-ef2c-3c53-8694-2352596ab7d5"))) ? ((((count) % (100)) != (0)) && ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("da423f75-9f63-376c-908f-68e7beac46a6"))) ? ((((count) % (100)) != (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4c07d094-f538-31ac-bf6a-3e57d6fc01ff"))) ? ((count++) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b872e7a8-5e07-31b7-bc03-de4db4db495c"))) ? ((((count) % (100)) != (0)) && ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("226bd2bc-53b2-345c-9857-9742fd8092ee"))) ? ((((count) % (100)) != (0)) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cf76b572-1cdc-3435-a630-a891158b9fdb"))) ? (((count) == (0)) && ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("af560d5c-add0-34b1-a56a-5e8f576e0ae9"))) ? ((count % 100 == 0) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b1995ac5-cafa-3614-b325-aa33b7404a11"))) ? (((count) == (0)) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a6b7430-38de-3d3b-a08d-7c5c2c9afa8e"))) ? ((count % 100 == 0) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("647fa4f6-32db-3ef6-b5e3-de341fe9c9cb"))) ? (((count % 100) == (0)) && ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("834946c5-b3a2-35a8-84c4-e10f0290d29b"))) ? (((count) != (0)) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1d1e9941-1939-3869-bcbe-7492f91985a6"))) ? ((((count) % (100)) != (0)) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b37a68a1-c7e1-3b22-9dfa-e586edf6f96a"))) ? (((count) == (0)) || ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5de693f7-f623-3213-82b1-961d0b829b07"))) ? (((count) != (0)) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f59a0e5-5a2c-3682-8651-53fabfd7884b"))) ? (((count) != (0)) && ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e12e638c-59f2-371d-a559-78a35977b0d6"))) ? ((((count) % (100)) == (0)) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca967af0-a572-3824-854e-40380425f0b9"))) ? ((count % 100) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a589d2c0-87de-3a06-bd14-c83e75dd1962"))) ? (((count % 100) == (0)) || ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("63defdf4-8c2f-3b44-82d3-a990976b8197"))) ? (((count) != (0)) || (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d8639510-163c-3b53-84ca-2b0a72e1bf9f"))) ? ((count % 100 == 0) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("113a53dd-2cf7-3880-a442-cbc7e7b317b4"))) ? ((count % 100 == 0) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("57036d38-7d7e-3bad-a6ea-4783c6650cbc"))) ? (((count % 100) != (0)) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4b7550f2-d30e-3419-88de-c3799454e219"))) ? ((((count) % (100)) == (0)) && ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("18964856-a787-3682-8a83-5d6979fbf2a7"))) ? (((count % 100) == (0)) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1d34e194-771e-35cc-a63e-289b737da8d8"))) ? ((count % 100) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dee335be-251a-387e-9254-02479a235ea7"))) ? (((count % 100) != (0)) || (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("57447c16-14fc-3eb1-9440-a317ef69f136"))) ? ((((count) % (100)) == (0)) && ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("804c3f0f-95af-3f04-97c8-f92e3773e0d2"))) ? ((((count) % (100)) == (0)) && ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("984027be-db3c-307a-856d-be7474fcae34"))) ? (((count % 100) != (0)) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("58f5fbc8-fe79-3fa2-8d5e-bfe806fc8e53"))) ? (((count % 100) != (0)) || ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a805430e-591a-31d9-b1b7-322147e84b28"))) ? ((count % 100 == 0) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("31d9ffa4-cf6a-3e8f-adb0-b92b3446c973"))) ? ((count % 100 == 0) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b8e2967d-4233-3fc8-90da-a78baa53eaa0"))) ? ((((count) % (100)) == (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("55fb7b5e-eed4-388a-9b9b-f01a3527610f"))) ? ((((count) % (100)) == (0)) || (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7c7e180b-0176-3489-9daa-442e69d44791"))) ? ((count % 100) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("59e3adf0-4210-33aa-8fd1-6669d0d51222"))) ? ((((count) % (100)) != (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9fc3daef-6539-3416-b610-d623ec10338c"))) ? ((count % 100 == 0) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("31f3431f-7762-329e-9c9d-24f3597b67d0"))) ? (((count % 100) == (0)) || ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("02ca8554-3cbb-3f0c-8cbd-346a97b26ae9"))) ? (((count % 100) != (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b76f28c2-60cf-319f-af1a-9a8f0ffe3a00"))) ? ((count % 100 == 0) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("472fa69e-57d5-3a91-af8c-782d76acb67c"))) ? (((count % 100) == (0)) && ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("40d4e2d4-2a21-3ef2-a6aa-db9d02e20fe1"))) ? ((count % 100 == 0) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9e3e20bf-9d39-3a15-8ca9-89df884b11d7"))) ? (((count) != (0)) || ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3f0e4c2-fa25-3d6f-8980-39b62431eb7d"))) ? ((count % 100 == 0) && ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("62af5a8e-2e76-301b-87c4-eb541b466b54"))) ? ((((count) % (100)) != (0)) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f2441a3f-aef5-3144-bcd0-f08c75fd932d"))) ? ((((count) % (100)) == (0)) || ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d0e273ce-ff7a-303e-9c9c-f880086ff27e"))) ? ((((count) % (100)) != (0)) || ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("808e0b74-11b6-328a-bd7a-76504456100f"))) ? (((count) == (0)) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38960efc-9256-3992-a69f-dbd113f691e0"))) ? ((((count) % (100)) == (0)) && ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d6f74281-a933-34d9-93a7-20bc92d74911"))) ? ((count % 100 == 0) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c7afa56e-f77a-3e4f-9289-d6a8577ab762"))) ? ((((count) % (100)) != (0)) && ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0df03832-6aad-3a5d-a377-8113a8be78c8"))) ? (((count % 100) != (0)) && ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b3066ba4-8d59-3471-95b3-af5f62d81a06"))) ? (((count) != (0)) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a82d9c76-a165-33a3-b1e3-ddf27d4d80e8"))) ? (((count % 100) == (0)) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2453feb2-c29f-3d3f-888d-eafcd6300123"))) ? ((((count) % (100)) == (0)) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f3385a95-d831-33df-b0b9-18d2f51b53ac"))) ? (((count) == (0)) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9087542f-b6ac-3d40-b785-2038993be434"))) ? ((count % 100) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ddc2c729-84bf-30c1-827a-da8bc7f18701"))) ? ((((count) % (100)) != (0)) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("59978673-0278-3c73-87fb-704a605ccc14"))) ? ((count % 100) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d821c132-135e-3a17-8664-226687163dc2"))) ? ((((count) % (100)) == (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("64f53a07-2ab0-3227-8477-661a2f2f2a9e"))) ? (((count) == (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e83c6e65-6b43-3cf3-bfe9-df8d0bcc11e1"))) ? (((count % 100) == (0)) && ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2d017654-958b-395a-bd7d-c02c87d42581"))) ? ((count % 100 == 0) || ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("56aec4a5-a385-392d-be01-aa91b0a1299f"))) ? ((count % 100 == 0) || ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1c12d528-d793-390c-8c7d-3c2d18e43246"))) ? ((count % 100 == 0) && (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("9957602f-c949-39a2-955b-160be16d397e"))) ? (((count % 100) == (0)) || ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a3d71a2-059d-3698-a530-c0b6563e1356"))) ? (count % 100 == 0) : (((KnobRuntime.check(java.util.UUID.fromString("e6cda33f-e363-396e-b901-a93bd26a073f"))) ? (((count % 100) != (0)) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e8484e78-f95a-3c77-91b7-2aaf887b6abb"))) ? (((count % 100) == (0)) && ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8b49a795-bd25-313a-8bcc-e18ff15872c2"))) ? (((count % 100) == (0)) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6dcae484-92e7-3cf7-abb7-71cad1bed3f3"))) ? ((((count) % (100)) == (0)) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8514d0a4-aadf-3435-bccc-9a415b7326f5"))) ? (((count % 100) != (0)) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("df8d422e-3e06-3a7d-b79a-927c7be8eb58"))) ? (((count % 100) == (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8013e479-edf1-3805-913c-dd9c0a7d93af"))) ? ((((count) % (100)) == (0)) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e87e6570-c9a3-316a-9fc9-97d7fb038e96"))) ? (((count % 100) != (0)) || ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2837c79-80cc-3e1a-8e26-7c4ce9d861b6"))) ? ((count % 100 == 0) && ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e826321c-b98d-392b-b296-1f66c1ca1fff"))) ? (((count % 100) != (0)) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6b59a3ea-3eff-3464-9e7a-d347409d810f"))) ? ((count % 100 == 0) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("361a1ce3-5e08-3c63-a02f-a1285c938b71"))) ? ((count) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f909f982-7a8b-3507-a49e-a13f70ea157b"))) ? (((count % 100) != (0)) && ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f181044e-2ec9-3a97-81bc-33aec49c6712"))) ? (((count % 100) != (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("687b4808-2b21-3dd2-8659-810580be1efd"))) ? ((((count) % (100)) != (0)) && ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b3db35bd-2911-3766-b383-cbf2fba2e3df"))) ? ((((count) % (100)) == (0)) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e3ea6ea3-3d2d-3356-a61d-a978dc2d2367"))) ? (((count) != (0)) && ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f51e34d2-e557-331b-aeeb-bf90d43a8b39"))) ? ((((count) % (100)) == (0)) || ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("868b9841-dbaa-31c3-86b0-2bc2130a91e5"))) ? (((count % 100) != (0)) && ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f470e2fa-8a32-3d9d-aedf-28eba767cf9a"))) ? ((((count) % (100)) != (0)) || ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6bdb9caf-99d3-3f08-835a-d712cb94f300"))) ? (((count) == (0)) && ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("da97e2f2-d6cf-3c44-848c-9275eff4bcfa"))) ? ((((count) % (100)) != (0)) || ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4da6ec4a-a09d-3175-bade-225ae2d03eea"))) ? ((((count) % (100)) == (0)) && ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3e5a817a-fef1-3bf0-9d35-838687a57422"))) ? ((count % 100 == 0) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("da8649ff-60dd-3632-bd46-15af9fbe82a1"))) ? ((((count) % (100)) != (0)) || ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1005da82-2627-3929-81f2-e44b610d025a"))) ? (((count) == (0)) && ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d816f676-45a5-389b-bc0a-c2a281f2e1af"))) ? ((count % 100 == 0) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9dbd4a49-f356-3b3a-9ae2-dcb488df93f4"))) ? (((count) != (0)) && ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("10a632a2-095f-30b7-a586-cb607e4845f2"))) ? ((count % 100 == 0) || ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c6999a75-583c-38bb-9801-3d59a9da93cf"))) ? ((((count) % (100)) != (0)) || ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8bc39321-e944-376a-a3db-75e078ec186e"))) ? (((count % 100) == (0)) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("859610aa-4bce-363d-ac20-bde6799cc265"))) ? (((count) == (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4cccc14e-577a-39d4-b9a4-ed984d731f9d"))) ? ((((count) % (100)) == (0)) && ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6f526eb0-5496-3b75-a614-8f94e3822ffb"))) ? (((count) == (0)) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d616d117-2c03-3a63-b135-ad31a4fe04ed"))) ? (((count) == (0)) || ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f3e3157a-0b2f-3273-b7ec-81eed578bf4c"))) ? ((count % 100 == 0) && ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f358e829-a314-3aec-8d46-baa8eaac1c64"))) ? ((((count) % (100)) == (0)) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cbbae064-6ce3-3b79-bb77-9520faa23ae2"))) ? (((count) != (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dd9d3acb-d7a5-314a-84c7-f68fc5a13530"))) ? (((count % 100) != (0)) || ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fa6705e2-6e3f-337d-a1e4-5308232359d3"))) ? (((count) != (0)) && ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("51cb2c4a-8f48-3bac-b7f8-d6dec0f6ebd0"))) ? ((((count) % (100)) == (0)) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9871603f-a556-3af0-a6f5-9383a3f85d45"))) ? (((count % 100) == (0)) || (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("38163201-b72a-3ea2-9987-e3c688e8c408"))) ? (((count % 100) == (0)) || ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d982fdce-bec6-3e3f-8a3b-67d78e267483"))) ? (((count) != (0)) && ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("368c58c2-ee3c-3daa-a5d1-904e85e1f8eb"))) ? (((count) != (0)) || ((count++) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("61a206ae-9692-3986-99a2-55ee70712d86"))) ? ((count % 100 == 0) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4c0023b5-e49b-3de0-9cc0-8f67863ad710"))) ? (((count) != (0)) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("acd49855-9b04-3288-adb6-d3a8f4623279"))) ? (((count) == (0)) || ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("39ca2d33-001e-3122-b62a-85895745606c"))) ? (((count) != (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0ef4c40a-8537-3c51-91c1-33074fa67e3d"))) ? (((count % 100) == (0)) || ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2d24099-8a49-3b1c-9eea-50a302a49f19"))) ? (((count) == (0)) && ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e754d2f9-fc7b-3dcf-92c1-0065109b8fe0"))) ? ((count++) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9683c322-121d-3a34-ab33-8cd2e72e377b"))) ? (((count % 100) != (0)) || ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f18fc7b-becf-3021-bad3-9c4341f494c9"))) ? ((count % 100 == 0) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5e0c23c9-5f78-31e2-9a06-2038a4fcf719"))) ? ((((count) % (100)) == (0)) && ((count++) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a52ad47-645a-387a-af82-d142d9757fd6"))) ? ((((count) % (100)) != (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7f75cbed-9057-3a63-b5e3-b9b59a1c4ef7"))) ? ((((count) % (100)) == (0)) || ((count++) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("948dcd95-8b4e-3fd2-b248-401fb62b77f3"))) ? ((((count) % (100)) != (0)) || ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("28d54c28-956f-3f64-ba44-c7c6e01f38ef"))) ? (((count % 100) != (0)) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2a11e07a-77df-3e97-9137-13faefa72790"))) ? (((count % 100) == (0)) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2c38589e-22ca-3452-a02c-804c1413269c"))) ? ((count % 100 == 0) || ((count) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a076e179-f1d0-3a7f-8007-08d47117190c"))) ? (((count % 100) == (0)) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de1687b0-ed39-3fa3-a223-2bfbf29a4467"))) ? ((((count) % (100)) != (0)) && ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("93874d58-bbbc-3340-b15d-906b24da96cd"))) ? (((count) != (0)) && ((count) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("62b8074d-008c-3ea3-8a71-d2014da37b54"))) ? ((((count) % (100)) == (0)) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c2047f29-c731-3dec-b9af-945f58882b32"))) ? (((count % 100) != (0)) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("955cfa40-bfd4-378e-bbe8-f66566dc0282"))) ? ((((count) % (100)) == (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3e497a44-d85e-33c6-9f47-a9057190f23c"))) ? (((count) != (0)) && ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8b9fa421-4e35-31f6-b6d9-37dbda07c342"))) ? ((((count) % (100)) != (0)) && ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("42cbe353-5948-3fbb-b4e2-cd9f236c907d"))) ? (((count) == (0)) && ((count % 100) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("143dcd00-ec71-3506-9425-7078de4d915c"))) ? (((count) != (0)) || ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5ee4d71e-133e-3250-ae27-c388f4f868d2"))) ? (((count % 100) == (0)) && ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7a9f6645-3f38-31a3-947e-bb106c70bcd3"))) ? (((count) != (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("91d4bd68-c6af-3b32-b5fa-e06349d3e1bc"))) ? ((((count) % (100)) != (0)) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e4ba0925-b1bf-304a-8f4c-8bf0c2fd0d27"))) ? ((count % 100 == 0) || ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("55532860-b659-3aa0-81f1-757e293cb1e6"))) ? (((count % 100) != (0)) || ((count % 100) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3ebde163-ecca-3175-99a6-bc6d8eea9071"))) ? ((((count) % (100)) != (0)) && ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2372db6a-092e-326f-a1f4-fc0df268a653"))) ? (((count % 100) == (0)) && ((count++) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("68f01ee6-14c0-3b6a-a600-2db513037055"))) ? (((count) != (0)) || ((count % 100) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e4297d0-c4e1-3481-9d8e-98f5dc605eb9"))) ? (((count) == (0)) || ((count % 100) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("20c43024-d198-31d4-a04a-b84fd96b84b5"))) ? (((count % 100) != (0)) || ((count % 100) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ee9fbbfa-a1e2-37c7-8dca-eb48c259101a"))) ? (((count) == (0)) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("71da403a-edbe-32e9-a1b9-029942153f99"))) ? (((count % 100) == (0)) && ((count % 100) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1d1aa710-38a8-3fa4-abe1-c8397a20dc7f"))) ? ((((count) % (100)) == (0)) || ((count) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e231f2ef-60f1-3e22-ba77-2db8ee19703a"))) ? ((((count) % (100)) != (0)) || (count > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("9bc6450e-db55-38c1-9eeb-1d14c9cbe37c"))) ? ((count % 100 == 0) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e921608b-8cb2-3c76-aa57-18f5b4a2a387"))) ? (((count % 100) == (0)) && ((count) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("313270a7-3b7a-3937-bd6b-259d1544841f"))) ? (((count % 100) == (0)) || ((count++) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cd17081b-16cb-3011-b4ed-9201851fed4a"))) ? (((count) != (0)) && ((count++) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("72e8e820-7406-34f0-aa69-bbc6adf2603b"))) ? (((count) == (0)) || ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eb34979d-2b4d-374c-b2da-5825cb8b3197"))) ? (((count) != (0)) && ((count) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("128e5d93-5629-3528-b9b4-db9b99f0ddca"))) ? (((count) != (0)) && ((count) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("87bf512a-210f-357d-8cb7-d7c77f2b431a"))) ? (((count) != (0)) && ((count % 100) <= (0))) : (count % 100 == 0 && count > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          long totalWaitTillNow = READPOINT_ADVANCE_WAIT_TIME * count;
          if (KnobRuntime.check(java.util.UUID.fromString("9b1cd757-a97a-3293-9bd6-32ab24cb4cc2"))) { LOG.warn("STUCK for : " + totalWaitTillNow + " millis. "); } else if (KnobRuntime.check(java.util.UUID.fromString("7b9dce8d-116c-319c-8e14-cfb38e3284ec"))) { LOG.warn(("STUCK for : " + totalWaitTillNow + " millis. ") + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("a71e13d1-8b85-3136-8cdc-aad5f0586316"))) { LOG.warn(" millis. "); } else if (KnobRuntime.check(java.util.UUID.fromString("997d64b9-de79-3c8b-a018-be46229d0ad1"))) { LOG.warn(("STUCK for : " + totalWaitTillNow) + (" millis. ")); } else if (KnobRuntime.check(java.util.UUID.fromString("bcc93659-1d17-349b-b61d-23e65808737b"))) { LOG.warn(("STUCK for : " + totalWaitTillNow) + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("95f6f709-fc7f-3797-8d3c-8a5c0bfa9a0e"))) { LOG.warn((("STUCK for : " + totalWaitTillNow) + (" millis. ")) + (this)); } else if (KnobRuntime.check(java.util.UUID.fromString("a5c52800-a0a7-33e2-b9d1-871e261f902c"))) { LOG.warn("STUCK for : " + totalWaitTillNow); } else if (KnobRuntime.check(java.util.UUID.fromString("5e307c0f-6886-3453-a982-9d463fc137ac"))) { LOG.warn((" millis. ") + (this)); } else { LOG.warn("STUCK for : " + totalWaitTillNow + " millis. " + this); }
        }
        count++;
        try {
if(KnobRuntime.check(java.util.UUID.fromString("3ef35636-3341-34fe-9178-34c9f29a7619"))) {
throw new java.lang.InterruptedException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("9e576da2-8aa5-384f-9ea7-4ad441357654"))) {
return;
}
      this.completed = true;
    }

    boolean isCompleted() {
if(KnobRuntime.check(java.util.UUID.fromString("e49f90c6-ae82-33f5-8aa2-56fc150b757e"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("d00df3bc-0533-3eea-97f3-18acc31cfb9b"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("e9602068-edbe-357e-9670-0fb9ef1a81ac"))) ? (!this.completed) : (this.completed));
    }

    public long getWriteNumber() {
if(KnobRuntime.check(java.util.UUID.fromString("e6c347fd-0304-3573-b945-d5f3d324e3c0"))) {
return 0;
}
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
