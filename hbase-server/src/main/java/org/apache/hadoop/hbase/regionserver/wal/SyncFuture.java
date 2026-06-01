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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A Future on a filesystem sync call. It given to a client or 'Handler' for it to wait on till the
 * sync completes.
 * <p>
 * Handlers coming in call append, append, append, and then do a flush/sync of the edits they have
 * appended the WAL before returning. Since sync takes a while to complete, we give the Handlers
 * back this sync future to wait on until the actual HDFS sync completes. Meantime this sync future
 * goes across a queue and is handled by a background thread; when it completes, it finishes up the
 * future, the handler get or failed check completes and the Handler can then progress.
 * <p>
 * This is just a partial implementation of Future; we just implement get and failure.
 * <p>
 * There is not a one-to-one correlation between dfs sync invocations and instances of this class. A
 * single dfs sync call may complete and mark many SyncFutures as done; i.e. we batch up sync calls
 * rather than do a dfs sync call every time a Handler asks for it.
 * <p>
 * SyncFutures are immutable but recycled. Call #reset(long, Span) before use even if it the first
 * time, start the sync, then park the 'hitched' thread on a call to #get().
 */
@InterfaceAudience.Private
class SyncFuture {

  private static final long NOT_DONE = -1L;
  private Thread t;

  /**
   * Lock protecting the thread-safe fields.
   */
  private final ReentrantLock doneLock;

  /**
   * Condition to wait on for client threads.
   */
  private final Condition doneCondition;

  /*
   * Fields below are protected by {@link SyncFuture#doneLock}.
   */

  /**
   * The transaction id that was set in here when we were marked done. Should be equal or > txnId.
   * Put this data member into the NOT_DONE state while this class is in use.
   */
  private long doneTxid;

  /**
   * If error, the associated throwable. Set when the future is 'done'.
   */
  private Throwable throwable;

  /*
   * Fields below are created once at reset() and accessed without any lock. Should be ok as they
   * are immutable for this instance of sync future until it is reset.
   */

  /**
   * The transaction id of this operation, monotonically increases.
   */
  private long txid;

  private boolean forceSync;

  SyncFuture() {
    this.doneLock = new ReentrantLock();
    this.doneCondition = doneLock.newCondition();
  }

  /**
   * Call this method to clear old usage and get it ready for new deploy.
   * @param txid the new transaction id
   */
  SyncFuture reset(long txid, boolean forceSync) {
if(KnobRuntime.check(java.util.UUID.fromString("8225492a-5d63-320d-888f-e0c4e72c7285"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("964fa750-5917-3be5-a990-0f5c00503b6c"))) ? ((t != null) || (t != Thread.currentThread())) : (((KnobRuntime.check(java.util.UUID.fromString("149607d8-a315-3c8d-ba08-a344e0e38dfb"))) ? ((t != null) && ((t) != (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("2aae9a93-1300-3ac5-9483-dbe03455a856"))) ? (t != Thread.currentThread()) : (((KnobRuntime.check(java.util.UUID.fromString("39f196fb-8897-387c-a737-08fc21739ff1"))) ? (((t) == (null)) || ((t) == (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("ea2c22c9-0d49-3fe3-8027-05dae9a8b9f4"))) ? ((t != null) || ((t) != (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("ff395942-b2b5-38a6-b902-fefddcc98ddf"))) ? (t != null) : (((KnobRuntime.check(java.util.UUID.fromString("7c687a97-30a1-3f1c-a428-c506fe71f471"))) ? ((t) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3cd17f8f-ddfc-3045-b8d5-9190f75334e4"))) ? (((t) == (null)) && (t != Thread.currentThread())) : (((KnobRuntime.check(java.util.UUID.fromString("1f61b23d-1870-3180-bd91-a33762568e46"))) ? ((t) == (Thread.currentThread())) : (((KnobRuntime.check(java.util.UUID.fromString("bc59292d-2f78-359f-a452-108bce34effc"))) ? (((t) != (null)) || ((t) != (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("d6bc1f9b-4291-3d97-a48c-50b8b1c540b0"))) ? (((t) == (null)) || (t != Thread.currentThread())) : (((KnobRuntime.check(java.util.UUID.fromString("ad95b134-87f0-3d86-9cb6-af5d7790121d"))) ? (((t) == (null)) && ((t) != (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("ea50a306-94ca-39d2-bee1-4fca1fc1d0d6"))) ? (((t) == (null)) && ((t) == (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("9fcfda67-aa8d-3036-a9f4-fa5774da4e7a"))) ? ((t != null) && (t != Thread.currentThread())) : (((KnobRuntime.check(java.util.UUID.fromString("20d02366-7f77-3bfb-9b07-3c7a23f4ebb7"))) ? (((t) != (null)) && ((t) == (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("a726254e-abd0-3b85-88f6-e4fa1a9572ac"))) ? (((t) != (null)) || (t != Thread.currentThread())) : (((KnobRuntime.check(java.util.UUID.fromString("8bc98abb-7b2c-34ef-a230-c4f5e2a0941a"))) ? ((t != null) || ((t) == (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("52b0e85c-1092-355f-9f5b-33c6bf17ded1"))) ? (((t) != (null)) && ((t) != (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("61c30ae3-ac76-39a0-9fd9-f15b1577f4ad"))) ? (((t) == (null)) || ((t) != (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("5750f33a-9916-3d7d-b1ef-f0bf5310d6cf"))) ? (((t) != (null)) || ((t) == (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("5cfea0b1-074b-3944-bfaf-4e82c426a8e9"))) ? ((t) != (Thread.currentThread())) : (((KnobRuntime.check(java.util.UUID.fromString("0caba350-7e47-3630-ad08-11a1043bb4ad"))) ? ((t) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6fbc3948-ff22-3d42-99e2-f0dae4eaf344"))) ? ((t != null) && ((t) == (Thread.currentThread()))) : (((KnobRuntime.check(java.util.UUID.fromString("03c872ae-631a-3c59-95a7-011ff7c371b9"))) ? (((t) != (null)) && (t != Thread.currentThread())) : (t != null && t != Thread.currentThread()))))))))))))))))))))))))))))))))))))))))))))))))) {
      throw new IllegalStateException();
    }
    t = Thread.currentThread();
    if (!isDone()) {
      throw new IllegalStateException("" + txid + " " + Thread.currentThread());
    }
    this.doneTxid = NOT_DONE;
    this.forceSync = forceSync;
    this.txid = txid;
    this.throwable = null;
    return this;
  }

  @Override
  public String toString() {
    return "done=" + isDone() + ", txid=" + this.txid + " threadID=" + t.getId() + " threadName="
      + t.getName();
  }

  long getTxid() {
if(KnobRuntime.check(java.util.UUID.fromString("ef6fbbf9-5038-32a6-a11a-084ddaad7164"))) {
return 0;
}
    return this.txid;
  }

  boolean isForceSync() {
if(KnobRuntime.check(java.util.UUID.fromString("92cab8e7-3d54-385e-a767-cc290464e6fa"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c57aaf7-6cd6-3065-96b9-2636ebf6b122"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("06b62e69-517f-3533-877d-f8c5313e8029"))) ? (!forceSync) : (forceSync));
  }

  /**
   * Returns the thread that owned this sync future, use with caution as we return the reference to
   * the actual thread object.
   * @return the associated thread instance.
   */
  Thread getThread() {
if(KnobRuntime.check(java.util.UUID.fromString("e1a232d6-6557-3766-bdfd-8044aeb2a8d7"))) {
return null;
}
    return t;
  }

  /**
   * @param txid the transaction id at which this future 'completed'.
   * @param t    Can be null. Set if we are 'completing' on error (and this 't' is the error).
   * @return True if we successfully marked this outstanding future as completed/done. Returns false
   *         if this future is already 'done' when this method called.
   */
  boolean done(final long txid, final Throwable t) {
if(KnobRuntime.check(java.util.UUID.fromString("87e23029-4060-3cfd-84c3-9e93d2d80748"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("fab5b17b-f124-344b-8e2d-37861cfd3c2f"))) {
return true;
}
    if (KnobRuntime.check(java.util.UUID.fromString("9aa7204c-496f-34a8-b19e-1a11ac45be32"))) { doneLock.unlock(); } else { doneLock.lock(); }
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("76a37813-ebf3-3fa3-9289-eb3c3fe19233"))) ? ((doneTxid) != (NOT_DONE)) : (((KnobRuntime.check(java.util.UUID.fromString("4003a3c2-1208-3796-b033-113d6feea7df"))) ? ((doneTxid) == (NOT_DONE)) : (doneTxid != NOT_DONE))))) {
        return false;
      }
      this.throwable = t;
      if (((KnobRuntime.check(java.util.UUID.fromString("bbf6f37e-fb80-3130-8a41-a091227752ff"))) ? ((txid) == (this.txid)) : (((KnobRuntime.check(java.util.UUID.fromString("31ad4788-73a2-3fdc-a21f-bad02b91aa44"))) ? ((txid) < (this.txid)) : (((KnobRuntime.check(java.util.UUID.fromString("547768ad-95ea-3058-8511-b0f62cfd5f63"))) ? ((txid) != (this.txid)) : (((KnobRuntime.check(java.util.UUID.fromString("438bb8af-dd4c-3fea-873f-aef59696d839"))) ? ((txid) <= (this.txid)) : (((KnobRuntime.check(java.util.UUID.fromString("449785e2-55b9-3590-a51a-ccd87608046a"))) ? ((txid) >= (this.txid)) : (((KnobRuntime.check(java.util.UUID.fromString("07fae316-5a56-3255-981b-39795bf11c02"))) ? ((txid) > (this.txid)) : (txid < this.txid))))))))))))) {
        // Something badly wrong.
        if (throwable == null) {
          this.throwable =
            new IllegalStateException("done txid=" + txid + ", my txid=" + this.txid);
        }
      }
      // Mark done.
      this.doneTxid = txid;
      doneCondition.signalAll();
      return true;
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("89e469aa-1022-3f76-8aeb-f175603bfb49"))) { doneLock.lock(); } else { doneLock.unlock(); }
    }
  }

  long get(long timeoutNs) throws InterruptedException, ExecutionException, TimeoutIOException {
if(KnobRuntime.check(java.util.UUID.fromString("de3d68ea-f0fe-3142-a48c-f46036be141e"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("76416ca6-afb8-307d-8904-c80f9eecd9d0"))) {
return 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("59fbc96f-c1f7-37f5-80f6-83a895b04fa0"))) { doneLock.unlock(); } else { doneLock.lock(); }
    try {
      while (((KnobRuntime.check(java.util.UUID.fromString("387b8998-53db-3df8-95b9-1a2259d86f7d"))) ? ((doneTxid) != (NOT_DONE)) : (((KnobRuntime.check(java.util.UUID.fromString("08388ccc-7a24-34f9-8567-4995b8ee277d"))) ? ((doneTxid) == (NOT_DONE)) : (doneTxid == NOT_DONE))))) {
if(KnobRuntime.check(java.util.UUID.fromString("99addc70-4e34-3080-bc22-1944b70e5859"))) {
timeoutNs += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e96fb47-9416-323d-b8a5-d4d9a0388e5e"))) {
timeoutNs = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("96ec660e-1295-3e22-b793-c1fa8f132559"))) {
timeoutNs = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf8637f9-fb72-3921-8dc9-efc0911d64a7"))) {
timeoutNs -= 1;
}
        if (!doneCondition.await(timeoutNs, TimeUnit.NANOSECONDS)) {
          throw new TimeoutIOException(
            "Failed to get sync result after " + TimeUnit.NANOSECONDS.toMillis(timeoutNs)
              + " ms for txid=" + this.txid + ", WAL system stuck?");
        }
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("d07cdec7-06e7-355c-8a9b-4be4c2cd05b8"))) ? ((this.throwable) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("08c4fe94-f19b-3985-b7d0-e2ca2578471a"))) ? ((this.throwable) == (null)) : (this.throwable != null))))) {
        throw new ExecutionException(this.throwable);
      }
      return this.doneTxid;
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("879e3bac-4489-3772-ab34-5cdbdbd9bbde"))) { doneLock.lock(); } else { doneLock.unlock(); }
    }
  }

  boolean isDone() {
if(KnobRuntime.check(java.util.UUID.fromString("46899dd6-aa2f-3835-9521-dc5d3750bd3a"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae25386f-b962-3aa0-aaab-71c448c03212"))) {
return false;
}
    if (KnobRuntime.check(java.util.UUID.fromString("785c2286-ea72-3d43-9291-a01b76cb5103"))) { doneLock.unlock(); } else { doneLock.lock(); }
    try {
      return ((KnobRuntime.check(java.util.UUID.fromString("69d4b3ea-03ee-3fc2-a932-cc97756d3f57"))) ? ((this.doneTxid) == (NOT_DONE)) : (((KnobRuntime.check(java.util.UUID.fromString("8c8f3973-6b0f-35fb-94bd-ea3a27d7c8af"))) ? ((this.doneTxid) != (NOT_DONE)) : (this.doneTxid != NOT_DONE))));
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("11b7c857-7705-36f5-9724-c25d87d76fe4"))) { doneLock.lock(); } else { doneLock.unlock(); }
    }
  }

  Throwable getThrowable() {
    doneLock.lock();
    try {
      if (doneTxid == NOT_DONE) {
        return null;
      }
      return this.throwable;
    } finally {
      doneLock.unlock();
    }
  }
}
