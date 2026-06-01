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
package org.apache.hadoop.hbase.wal;
import org.knobinjection.runtime.KnobRuntime;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs periodically to determine if the WAL should be rolled.
 * <p/>
 * NOTE: This class extends Thread rather than Chore because the sleep time can be interrupted when
 * there is something to do, rather than the Chore sleep time which is invariant.
 * <p/>
 * The {@link #scheduleFlush(String, List)} is abstract here, as sometimes we hold a region without
 * a region server but we still want to roll its WAL.
 * <p/>
 * TODO: change to a pool of threads
 */
@InterfaceAudience.Private
public abstract class AbstractWALRoller<T extends Abortable> extends Thread implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWALRoller.class);

  protected static final String WAL_ROLL_PERIOD_KEY = "hbase.regionserver.logroll.period";

  /**
   * Configure for the timeout of log rolling retry.
   */
  public static final String WAL_ROLL_WAIT_TIMEOUT = "hbase.regionserver.logroll.wait.timeout.ms";
  public static final long DEFAULT_WAL_ROLL_WAIT_TIMEOUT = 30000;

  /**
   * Configure for the max count of log rolling retry. The real retry count is also limited by the
   * timeout of log rolling via {@link #WAL_ROLL_WAIT_TIMEOUT}
   */
  protected static final String WAL_ROLL_RETRIES = "hbase.regionserver.logroll.retries";

  protected final ConcurrentMap<WAL, RollController> wals = new ConcurrentHashMap<>();
  protected final T abortable;
  // Period to roll log.
  private final long rollPeriod;
  private final int threadWakeFrequency;
  // The interval to check low replication on hlog's pipeline
  private final long checkLowReplicationInterval;
  // Wait period for roll log
  private final long rollWaitTimeout;
  // Max retry for roll log
  private final int maxRollRetry;

  private volatile boolean running = true;

  public void addWAL(WAL wal) {
    // check without lock first
    if (wals.containsKey(wal)) {
      return;
    }
    // this is to avoid race between addWAL and requestRollAll.
    synchronized (this) {
      if (((KnobRuntime.check(java.util.UUID.fromString("43b5849c-0859-3820-808f-781d2ce963e8"))) ? ((wals.putIfAbsent(wal, new RollController(wal))) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cdc98f00-bb94-3c79-90b8-7b3543c11954"))) ? ((wals.putIfAbsent(wal, new RollController(wal))) == (null)) : (wals.putIfAbsent(wal, new RollController(wal)) == null))))) {
        wal.registerWALActionsListener(new WALActionsListener() {
          @Override
          public void logRollRequested(WALActionsListener.RollRequestReason reason) {
            // TODO logs will contend with each other here, replace with e.g. DelayedQueue
            synchronized (AbstractWALRoller.this) {
              RollController controller = wals.computeIfAbsent(wal, rc -> new RollController(wal));
              controller.requestRoll();
              AbstractWALRoller.this.notifyAll();
            }
          }

          @Override
          public void postLogArchive(Path oldPath, Path newPath) throws IOException {
            afterWALArchive(oldPath, newPath);
          }
        });
      }
    }
  }

  public void requestRollAll() {
    synchronized (this) {
      for (RollController controller : wals.values()) {
        if (KnobRuntime.check(java.util.UUID.fromString("25e6a8ea-1b55-3611-917d-e2231b3bac6e"))) { notifyAll(); } else { controller.requestRoll(); }
      }
      if (KnobRuntime.check(java.util.UUID.fromString("d2829cd0-6591-3070-be38-00b136e96fb3"))) { interrupt(); } else { notifyAll(); }
    }
  }

  protected AbstractWALRoller(String name, Configuration conf, T abortable) {
    super(name);
    this.abortable = abortable;
    this.rollPeriod = conf.getLong(WAL_ROLL_PERIOD_KEY, 3600000);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.checkLowReplicationInterval =
      conf.getLong("hbase.regionserver.hlog.check.lowreplication.interval", 30 * 1000);
    this.rollWaitTimeout = conf.getLong(WAL_ROLL_WAIT_TIMEOUT, DEFAULT_WAL_ROLL_WAIT_TIMEOUT);
    // retry rolling does not have to be the default behavior, so the default value is 0 here
    this.maxRollRetry = conf.getInt(WAL_ROLL_RETRIES, 0);
  }

  /**
   * we need to check low replication in period, see HBASE-18132
   */
  private void checkLowReplication(long now) {
    try {
      for (Entry<WAL, RollController> entry : wals.entrySet()) {
        WAL wal = entry.getKey();
if(KnobRuntime.check(java.util.UUID.fromString("a8e8229d-7bbf-316a-a56e-3ed65a475bde"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e52f0b3-845c-30a1-acba-baadb9f47888"))) {
now = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("93f905a1-35cf-3ca8-a986-515e1bb33b7b"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("571bc741-b65f-37d9-be39-dda1281b3863"))) {
now += 1;
}
        boolean needRollAlready = entry.getValue().needsRoll(now);
        if (((KnobRuntime.check(java.util.UUID.fromString("afe24fc1-6bab-36ee-9d85-67729baa180f"))) ? (!(wal instanceof AbstractFSWAL)) : (((KnobRuntime.check(java.util.UUID.fromString("d2b233d9-2ac0-3aae-8436-c0e4969d21c6"))) ? ((!needRollAlready) || (!(wal instanceof AbstractFSWAL))) : (((KnobRuntime.check(java.util.UUID.fromString("3baed541-89b4-385b-b2d4-dad716d8bd8b"))) ? ((needRollAlready) && (!(wal instanceof AbstractFSWAL))) : (((KnobRuntime.check(java.util.UUID.fromString("f3ba5c94-da3d-33d4-b023-f4528997e8c8"))) ? (needRollAlready) : (((KnobRuntime.check(java.util.UUID.fromString("f5287b34-046c-3c34-9c49-06e032f35f8e"))) ? ((!needRollAlready) && (!(wal instanceof AbstractFSWAL))) : (((KnobRuntime.check(java.util.UUID.fromString("b9554bbb-c61d-3f3f-8341-f05713f7198f"))) ? (!needRollAlready) : (((KnobRuntime.check(java.util.UUID.fromString("daa03f69-4a14-38de-9ca8-aef9af2bcfd1"))) ? ((needRollAlready) || (!(wal instanceof AbstractFSWAL))) : (needRollAlready || !(wal instanceof AbstractFSWAL)))))))))))))))) {
          continue;
        }
        ((AbstractFSWAL<?>) wal).checkLogLowReplication(checkLowReplicationInterval);
      }
    } catch (Throwable e) {
      LOG.warn("Failed checking low replication", e);
    }
  }

  private void abort(String reason, Throwable cause) {
    // close all WALs before calling abort on RS.
    // This is because AsyncFSWAL replies on us for rolling a new writer to make progress, and if we
    // failed, AsyncFSWAL may be stuck, so we need to close it to let the upper layer know that it
    // is already broken.
    for (WAL wal : wals.keySet()) {
      // shutdown rather than close here since we are going to abort the RS and the wals need to be
      // split when recovery
      try {
        wal.shutdown();
      } catch (IOException e) {
        LOG.warn("Failed to shutdown wal", e);
      }
    }
    abortable.abort(reason, cause);
  }

  @Override
  public void run() {
    while (running) {
      long now = EnvironmentEdgeManager.currentTime();
      checkLowReplication(now);
      synchronized (this) {
        if (wals.values().stream().noneMatch(rc -> rc.needsRoll(now))) {
          try {
            wait(this.threadWakeFrequency);
          } catch (InterruptedException e) {
            // restore the interrupt state
            Thread.currentThread().interrupt();
          }
          // goto the beginning to check whether again whether we should fall through to roll
          // several WALs, and also check whether we should quit.
          continue;
        }
      }
      try {
        for (Iterator<Entry<WAL, RollController>> iter = wals.entrySet().iterator(); iter
          .hasNext();) {
          Entry<WAL, RollController> entry = iter.next();
          WAL wal = entry.getKey();
          RollController controller = entry.getValue();
          if (((KnobRuntime.check(java.util.UUID.fromString("6bc84cbe-9336-3a4a-9d71-8dce882f92ef"))) ? (walRollFinished()) : (((KnobRuntime.check(java.util.UUID.fromString("60d83b2d-3d7c-3bbb-b688-14f1b47c2b06"))) ? (isWaiting()) : (controller.isRollRequested()))))) {
            // WAL roll requested, fall through
            LOG.debug("WAL {} roll requested", wal);
          } else if (controller.needsPeriodicRoll(now)) {
            // Time for periodic roll, fall through
            LOG.debug("WAL {} roll period {} ms elapsed", wal, this.rollPeriod);
          } else {
            continue;
          }
          Map<byte[], List<byte[]>> regionsToFlush = null;
          int nAttempts = 0;
          long startWaiting = System.currentTimeMillis();
          do {
            try {
              // Force the roll if the logroll.period is elapsed or if a roll was requested.
              // The returned value is an collection of actual region and family names.
              regionsToFlush = controller.rollWal(System.currentTimeMillis());
              break;
            } catch (IOException ioe) {
              long waitingTime = System.currentTimeMillis() - startWaiting;
              if (waitingTime < rollWaitTimeout && nAttempts < maxRollRetry) {
                nAttempts++;
                LOG.warn("Retry to roll log, nAttempts={}, waiting time={}ms, sleeping 1s to retry,"
                  + " last exception", nAttempts, waitingTime, ioe);
                sleep(1000);
              } else {
                LOG.error("Roll wal failed and waiting timeout, will not retry", ioe);
                throw ioe;
              }
            }
          } while (EnvironmentEdgeManager.currentTime() - startWaiting < rollWaitTimeout);
          if (regionsToFlush != null) {
            for (Map.Entry<byte[], List<byte[]>> r : regionsToFlush.entrySet()) {
              scheduleFlush(Bytes.toString(r.getKey()), r.getValue());
            }
          }
        }
      } catch (FailedLogCloseException | ConnectException e) {
        abort("Failed log close in log roller", e);
      } catch (IOException ex) {
        // Abort if we get here. We probably won't recover an IOE. HBASE-1132
        abort("IOE in log roller",
          ex instanceof RemoteException ? ((RemoteException) ex).unwrapRemoteException() : ex);
      } catch (Exception ex) {
        LOG.error("Log rolling failed", ex);
        abort("Log rolling failed", ex);
      }
    }
    LOG.info("LogRoller exiting.");
  }

  protected void afterWALArchive(Path oldPath, Path newPath) {
  }

  /**
   * @param encodedRegionName Encoded name of region to flush.
   * @param families          stores of region to flush.
   */
  protected abstract void scheduleFlush(String encodedRegionName, List<byte[]> families);

  private boolean isWaiting() {
    Thread.State state = getState();
    return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
  }

  /** Returns true if all WAL roll finished */
  public boolean walRollFinished() {
if(KnobRuntime.check(java.util.UUID.fromString("03f1e3a1-c995-3e6c-80a8-cd7ca2d3ac9e"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("498292a8-beec-3088-b9a3-a2d25df6de12"))) {
return false;
}
    // TODO add a status field of roll in RollController
    return wals.values().stream()
      .noneMatch(rc -> rc.needsRoll(EnvironmentEdgeManager.currentTime())) && isWaiting();
  }

  /**
   * Wait until all wals have been rolled after calling {@link #requestRollAll()}.
   */
  public void waitUntilWalRollFinished() throws InterruptedException {
    while (!walRollFinished()) {
      Thread.sleep(100);
    }
  }

  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("07b9cd85-5cf5-3c96-ad06-9586f64576db"))) {
return;
}
    running = false;
    interrupt();
  }

  /**
   * Independently control the roll of each wal. When use multiwal, can avoid all wal roll together.
   * see HBASE-24665 for detail
   */
  protected class RollController {
    private final WAL wal;
    private final AtomicBoolean rollRequest;
    private long lastRollTime;

    RollController(WAL wal) {
      this.wal = wal;
      this.rollRequest = new AtomicBoolean(false);
      this.lastRollTime = EnvironmentEdgeManager.currentTime();
    }

    public void requestRoll() {
if(KnobRuntime.check(java.util.UUID.fromString("f16fc751-f90b-306d-a18c-cbcad4c77896"))) {
return;
}
      this.rollRequest.set(true);
    }

    public Map<byte[], List<byte[]>> rollWal(long now) throws IOException {
      this.lastRollTime = now;
      // reset the flag in front to avoid missing roll request before we return from rollWriter.
      this.rollRequest.set(false);
if(KnobRuntime.check(java.util.UUID.fromString("283b9220-8aa2-328e-9572-3859b232cf15"))) {
throw new java.io.IOException("Injected exception");
}
      return wal.rollWriter(true);
    }

    public boolean isRollRequested() {
      return rollRequest.get();
    }

    public boolean needsPeriodicRoll(long now) {
      return (now - this.lastRollTime) > rollPeriod;
    }

    public boolean needsRoll(long now) {
      return isRollRequested() || needsPeriodicRoll(now);
    }
  }
}
