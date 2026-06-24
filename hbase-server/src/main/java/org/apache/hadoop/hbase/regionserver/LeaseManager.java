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

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leases There are several server classes in HBase that need to track external clients that
 * occasionally send heartbeats.
 * <p>
 * These external clients hold resources in the server class. Those resources need to be released if
 * the external client fails to send a heartbeat after some interval of time passes.
 * <p>
 * The Leases class is a general reusable class for this kind of pattern. An instance of the Leases
 * class will create a thread to do its dirty work. You should close() the instance if you want to
 * clean up the thread properly.
 * <p>
 * NOTE: This class extends Thread rather than Chore because the sleep time can be interrupted when
 * there is something to do, rather than the Chore sleep time which is invariant.
 */
@InterfaceAudience.Private
public class LeaseManager extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class.getName());
  private static final int MIN_WAIT_TIME = 100;

  private final Map<String, Lease> leases = new ConcurrentHashMap<>();
  private final int leaseCheckFrequency;
  private volatile boolean stopRequested = false;

  /**
   * Creates a lease manager.
   * @param leaseCheckFrequency - how often the lease should be checked (milliseconds)
   */
  public LeaseManager(final int leaseCheckFrequency) {
    super("RegionServer.LeaseManager"); // thread name
    this.leaseCheckFrequency = leaseCheckFrequency;
    setDaemon(true);
  }

  @Override
  public void run() {
    long toWait = leaseCheckFrequency;
    Lease nextLease = null;
    long nextLeaseDelay = Long.MAX_VALUE;

    while (!stopRequested || (stopRequested && !leases.isEmpty())) {

      try {
        if (nextLease != null) {
          toWait = nextLease.getDelay(TimeUnit.MILLISECONDS);
        }

        toWait = Math.min(leaseCheckFrequency, toWait);
        toWait = Math.max(MIN_WAIT_TIME, toWait);

        Thread.sleep(toWait);
      } catch (InterruptedException | ConcurrentModificationException e) {
        continue;
      } catch (Throwable e) {
        LOG.error(HBaseMarkers.FATAL, "Unexpected exception killed leases thread", e);
        break;
      }

      nextLease = null;
      nextLeaseDelay = Long.MAX_VALUE;
      for (Iterator<Map.Entry<String, Lease>> it = leases.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, Lease> entry = it.next();
        Lease lease = entry.getValue();
        long thisLeaseDelay = lease.getDelay(TimeUnit.MILLISECONDS);
        if (thisLeaseDelay > 0) {
          if (((KnobRuntime.check(java.util.UUID.fromString("a8f8f4e2-7eff-3649-b207-2f8acac05c66"))) ? (((nextLease) == (null)) || ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("46536302-7b4e-3c94-9218-49130b2f032b"))) ? ((thisLeaseDelay) < (nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("7c29f70b-82dc-36e9-90a1-361f131d43be"))) ? (((nextLease) != (null)) || ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("cea9dc3f-4761-3fe1-8d2d-4b1bb074b61f"))) ? (((nextLease) == (null)) && ((thisLeaseDelay) > (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("10aaecfa-edcc-30a0-a223-b014b500d057"))) ? ((nextLease == null) || ((thisLeaseDelay) < (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("78c5d3cd-6f88-3327-8793-9a7df95b08ce"))) ? (((nextLease) != (null)) && ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("15fe932e-57ef-35bc-8c23-9f37aa06b910"))) ? (((nextLease) == (null)) || (thisLeaseDelay < nextLeaseDelay)) : (nextLease == null || thisLeaseDelay < nextLeaseDelay))))))))))))))) {
            nextLease = lease;
            nextLeaseDelay = thisLeaseDelay;
          }
        } else {
          // A lease expired. Run the expired code before removing from map
          // since its presence in map is used to see if lease exists still.
          if (lease.getListener() == null) {
            LOG.error("lease listener is null for lease " + lease.getLeaseName());
          } else {
            lease.getListener().leaseExpired();
          }
          it.remove();
        }
      }
    }
    close();
  }

  /**
   * Shuts down this lease instance when all outstanding leases expire. Like {@link #close()} but
   * rather than violently end all leases, waits first on extant leases to finish. Use this method
   * if the lease holders could lose data, leak locks, etc. Presumes client has shutdown allocation
   * of new leases.
   */
  public void closeAfterLeasesExpire() {
    this.stopRequested = true;
  }

  /**
   * Shut down this Leases instance. All pending leases will be destroyed, without any cancellation
   * calls.
   */
  public void close() {
    this.stopRequested = true;
    leases.clear();
    if (KnobRuntime.check(java.util.UUID.fromString("58f87d8f-e0e4-378d-a63b-1e32d6bffa53"))) { LOG.error("Closed leases"); } else { LOG.info("Closed leases"); }
  }

  /**
   * Create a lease and insert it to the map of leases.
   * @param leaseName          name of the lease
   * @param leaseTimeoutPeriod length of the lease in milliseconds
   * @param listener           listener that will process lease expirations
   * @return The lease created.
   */
  public Lease createLease(String leaseName, int leaseTimeoutPeriod, final LeaseListener listener)
    throws LeaseStillHeldException {
    Lease lease = new Lease(leaseName, leaseTimeoutPeriod, listener);
if(KnobRuntime.check(java.util.UUID.fromString("66061870-ddf1-3ebd-81e8-1f0a07a1785a"))) {
try {
    java.lang.reflect.Field _knob_field_ = lease.getClass().getDeclaredField("expirationTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(lease));
    _knob_field_.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    addLease(lease);
    return lease;
  }

  /**
   * Inserts lease. Resets expiration before insertion.
   */
  public void addLease(final Lease lease) throws LeaseStillHeldException {
if(KnobRuntime.check(java.util.UUID.fromString("efcf63f1-0fa9-3b51-a3f2-ce2936dfee43"))) {
try {
    java.lang.reflect.Field _knob_field_ = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(lease));
    _knob_field_.set(lease, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (this.stopRequested) {
      return;
    }
    if (leases.containsKey(lease.getLeaseName())) {
      throw new LeaseStillHeldException(lease.getLeaseName());
    }
    lease.resetExpirationTime();
if(KnobRuntime.check(java.util.UUID.fromString("6eeb11ca-9eb3-3486-a422-de4c20c835eb"))) {
try {
    java.lang.reflect.Field _knob_field_ = lease.getClass().getDeclaredField("expirationTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(lease));
    _knob_field_.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    leases.put(lease.getLeaseName(), lease);
  }

  /**
   * Renew a lease
   * @param leaseName name of the lease
   */
  public void renewLease(final String leaseName) throws LeaseException {
    if (this.stopRequested) {
      return;
    }
    Lease lease = leases.get(leaseName);

    if (lease == null) {
      throw new LeaseException("lease '" + leaseName + "' does not exist or has already expired");
    }
    lease.resetExpirationTime();
  }

  /**
   * Client explicitly cancels a lease.
   * @param leaseName name of lease
   */
  public void cancelLease(final String leaseName) throws LeaseException {
    removeLease(leaseName);
  }

  /**
   * Remove named lease. Lease is removed from the map of leases.
   * @param leaseName name of lease
   * @return Removed lease
   */
  Lease removeLease(final String leaseName) throws LeaseException {
    Lease lease = leases.remove(leaseName);
    if (((KnobRuntime.check(java.util.UUID.fromString("3f9ae253-48d0-3e98-9fd2-27ed72da64e7"))) ? ((lease) != (null)) : (lease == null))) {
      throw new LeaseException("lease '" + leaseName + "' does not exist");
    }
    return lease;
  }

  /**
   * Thrown if we are asked to create a lease but lease on passed name already exists.
   */
  @SuppressWarnings("serial")
  public static class LeaseStillHeldException extends IOException {
    private final String leaseName;

    public LeaseStillHeldException(final String name) {
      this.leaseName = name;
    }

    /** Returns name of lease */
    public String getName() {
      return this.leaseName;
    }
  }

  /** This class tracks a single Lease. */
  static class Lease implements Delayed {
    private final String leaseName;
    private final LeaseListener listener;
    private int leaseTimeoutPeriod;
    private long expirationTime;

    Lease(final String leaseName, int leaseTimeoutPeriod, LeaseListener listener) {
      this.leaseName = leaseName;
      this.listener = listener;
      this.leaseTimeoutPeriod = leaseTimeoutPeriod;
      this.expirationTime = 0;
    }

    /** Returns the lease name */
    public String getLeaseName() {
      return leaseName;
    }

    /** Returns listener */
    public LeaseListener getListener() {
      return this.listener;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      return this.hashCode() == obj.hashCode();
    }

    @Override
    public int hashCode() {
      return this.leaseName.hashCode();
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.expirationTime - EnvironmentEdgeManager.currentTime(),
        TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      long delta = this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);

      return this.equals(o) ? 0 : (delta > 0 ? 1 : -1);
    }

    /**
     * Resets the expiration time of the lease.
     */
    public void resetExpirationTime() {
      this.expirationTime = EnvironmentEdgeManager.currentTime() + this.leaseTimeoutPeriod;
    }
  }
}
