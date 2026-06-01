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
if(KnobRuntime.check(java.util.UUID.fromString("4d8f0815-a128-31aa-af44-66e548b04678"))) {
return;
}
    long toWait = leaseCheckFrequency;
    Lease nextLease = null;
    long nextLeaseDelay = Long.MAX_VALUE;

    while (((KnobRuntime.check(java.util.UUID.fromString("13fe426f-9872-3b6d-9a95-02bb1b884695"))) ? ((!stopRequested) && ((stopRequested && !leases.isEmpty()))) : (((KnobRuntime.check(java.util.UUID.fromString("11c0b085-a2d6-344f-a069-e81212209185"))) ? (!stopRequested) : (((KnobRuntime.check(java.util.UUID.fromString("7e1b380e-7c46-3263-8c8f-01d49426d352"))) ? ((stopRequested && !leases.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("06d99985-4310-387d-9fc6-4f24db7c1bdd"))) ? ((!stopRequested) || ((stopRequested && !leases.isEmpty()))) : (!stopRequested || (stopRequested && !leases.isEmpty())))))))))) {

      try {
        if (((KnobRuntime.check(java.util.UUID.fromString("8c2ee800-6fcc-3fc4-9ab8-e65aecfa1046"))) ? ((nextLease) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("eaa50341-0112-3dd0-866f-abddebac93b1"))) ? ((nextLease) != (null)) : (nextLease != null))))) {
          toWait = nextLease.getDelay(TimeUnit.MILLISECONDS);
        }

if(KnobRuntime.check(java.util.UUID.fromString("983ec90c-1410-3d27-869a-f64ee64d488e"))) {
toWait = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("23fae4ea-3740-3020-902d-fd70564191a1"))) {
toWait += 1;
}
        toWait = Math.min(leaseCheckFrequency, toWait);
if(KnobRuntime.check(java.util.UUID.fromString("f69087c2-4e0a-35ff-9173-34d0059d2b4d"))) {
toWait = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cc1461f5-123f-3ee6-bc16-5d2c99c0d3c4"))) {
toWait += 1;
}
        toWait = Math.max(MIN_WAIT_TIME, toWait);

if(KnobRuntime.check(java.util.UUID.fromString("2b25fb9f-07d6-3de6-99ba-68acd5ea6d51"))) {
toWait = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a04c6924-9e1c-318b-9d47-29424aa57294"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("86245e11-a2e9-3d23-806d-7170d69c79b0"))) {
toWait += 1;
}
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
        if (((KnobRuntime.check(java.util.UUID.fromString("0012c598-ebd3-3c8a-af61-29633d252127"))) ? ((thisLeaseDelay) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("34b13c2e-bde8-374e-adb0-f06a3f97d04d"))) ? ((thisLeaseDelay) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2952e5e5-5510-3c88-aefc-c4e063bbcfa8"))) ? ((thisLeaseDelay) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ed8f3c08-40cf-3699-9c1c-6731ab93c9d9"))) ? ((thisLeaseDelay) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0df7b623-2e26-3e7f-a3b0-b4c8202c811f"))) ? ((thisLeaseDelay) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cb1c6f12-b999-3f49-909f-ea4b9876c8e2"))) ? ((thisLeaseDelay) < (0)) : (thisLeaseDelay > 0))))))))))))) {
          if (((KnobRuntime.check(java.util.UUID.fromString("980f3414-e3af-3ee1-8bc8-9f9cc620e81e"))) ? ((nextLease == null) || ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("521ca42d-6ffc-35f4-8ac2-7ebdb517af70"))) ? (((nextLease) == (null)) || ((thisLeaseDelay) == (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("e6a03f25-26e1-320c-a6c7-634f4eecf9bd"))) ? ((nextLease) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f3af27fd-b218-3fca-b5ff-23a6cf0ca828"))) ? ((nextLease) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("13459acd-0bec-32e8-b54c-9858e1780ed5"))) ? (nextLease == null) : (((KnobRuntime.check(java.util.UUID.fromString("719695b8-cc7c-33dd-91c1-d2a0acc95dce"))) ? (((nextLease) == (null)) || ((thisLeaseDelay) > (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("19741716-b253-3644-b5fa-1a7459671844"))) ? ((nextLease == null) && ((thisLeaseDelay) == (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("15af069f-3ae5-3f5a-a40b-c48918989bf2"))) ? ((thisLeaseDelay) != (nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("a8f8f4e2-7eff-3649-b207-2f8acac05c66"))) ? (((nextLease) == (null)) || ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("ec2ae527-555a-346c-aa5d-f3b9c240169a"))) ? (((nextLease) != (null)) && ((thisLeaseDelay) == (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("c9a4d271-99c6-3477-9d4f-7195bda2a541"))) ? (((nextLease) != (null)) || ((thisLeaseDelay) == (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("8a9c4bbf-99ad-3179-b8e2-2f137dd9a716"))) ? (((nextLease) == (null)) || ((thisLeaseDelay) < (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("a0549839-fe17-3d58-a839-d62bf0923334"))) ? ((nextLease == null) && ((thisLeaseDelay) <= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("e6f016a0-25b2-3147-bc55-ed83d8b7ce07"))) ? ((nextLease == null) || (thisLeaseDelay < nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("eb509395-005f-33ae-b9cd-9e39a7967e50"))) ? ((nextLease == null) || ((thisLeaseDelay) <= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("fc36686a-0def-381b-9b29-66b4abb32995"))) ? ((nextLease == null) && ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("87418b40-c8e8-30ed-ae7e-c91897b5a450"))) ? (((nextLease) != (null)) && ((thisLeaseDelay) < (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("68751b79-01f0-37f9-9e8b-f47aa287d20d"))) ? (((nextLease) == (null)) && ((thisLeaseDelay) >= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("971c5263-40f0-3553-979c-b9442f1f8e03"))) ? (((nextLease) == (null)) && ((thisLeaseDelay) == (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("7c29f70b-82dc-36e9-90a1-361f131d43be"))) ? (((nextLease) != (null)) || ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("26b71b79-4c1d-3a58-8bdd-7f600f524f07"))) ? (((nextLease) == (null)) && ((thisLeaseDelay) <= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("e0feaec3-1828-3d3d-8cda-0ca7ff744ed8"))) ? (((nextLease) != (null)) || (thisLeaseDelay < nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("fa2313eb-a5da-3af6-8a8c-c5e5d30facca"))) ? ((thisLeaseDelay) >= (nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("69e93e81-c4ce-3489-bba3-f26e9322f6d5"))) ? ((nextLease == null) && ((thisLeaseDelay) >= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("041165ab-5107-3b57-a6fb-2798bae96884"))) ? (((nextLease) != (null)) || ((thisLeaseDelay) <= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("96b28aec-b090-3f65-bf28-57597c7117f0"))) ? ((nextLease == null) || ((thisLeaseDelay) > (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("bd046958-c265-30a1-be7e-cc554c13f944"))) ? ((nextLease == null) && ((thisLeaseDelay) > (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("10aaecfa-edcc-30a0-a223-b014b500d057"))) ? ((nextLease == null) || ((thisLeaseDelay) < (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("77607578-8db7-3637-8349-3e5a827e899f"))) ? (((nextLease) != (null)) || ((thisLeaseDelay) > (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("e83e6c32-f81c-3394-9369-2d3c26771187"))) ? (((nextLease) != (null)) || ((thisLeaseDelay) >= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("3f0dad85-7ba6-3afd-af36-e981b0a4f8c9"))) ? (((nextLease) != (null)) || ((thisLeaseDelay) < (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("46536302-7b4e-3c94-9218-49130b2f032b"))) ? ((thisLeaseDelay) < (nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("643427d9-72ce-3d31-ba5f-72f4dbd21821"))) ? (((nextLease) != (null)) && (thisLeaseDelay < nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("78c5d3cd-6f88-3327-8793-9a7df95b08ce"))) ? (((nextLease) != (null)) && ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("56c70429-71d0-38bb-adff-699316cc686e"))) ? (((nextLease) != (null)) && ((thisLeaseDelay) <= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("0eeee0f9-e8cc-347a-a420-4b6e8330e14d"))) ? ((thisLeaseDelay) <= (nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("b2157855-aff0-37fa-9b7a-c69ea391d911"))) ? (thisLeaseDelay < nextLeaseDelay) : (((KnobRuntime.check(java.util.UUID.fromString("4fb8bcb3-a0d9-399f-9d1b-2c2df96fcad3"))) ? (((nextLease) == (null)) && (thisLeaseDelay < nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("a24e2d55-c288-3996-937c-bf3dca423cf1"))) ? ((nextLease == null) && ((thisLeaseDelay) < (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("e07f8477-3740-39d9-9acb-9287ddb8b47b"))) ? ((thisLeaseDelay) > (nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("15fe932e-57ef-35bc-8c23-9f37aa06b910"))) ? (((nextLease) == (null)) || (thisLeaseDelay < nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("366018bc-9002-344e-81b2-fce51695dabd"))) ? ((nextLease == null) || ((thisLeaseDelay) >= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("5ee90377-983c-37ae-a435-724bca205b71"))) ? (((nextLease) != (null)) && ((thisLeaseDelay) > (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("2d036382-3d0e-32cd-ab50-a64059e23c36"))) ? (((nextLease) == (null)) && ((thisLeaseDelay) < (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("894645e6-2f2b-30e6-9fd8-5721f9b0423d"))) ? (((nextLease) == (null)) || ((thisLeaseDelay) <= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("cd1815d3-ab03-3512-865c-771bb13c3336"))) ? ((nextLease == null) || ((thisLeaseDelay) == (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("745d73e9-cdbe-3671-ac63-c67ee1f569d3"))) ? (((nextLease) == (null)) || ((thisLeaseDelay) >= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("cea9dc3f-4761-3fe1-8d2d-4b1bb074b61f"))) ? (((nextLease) == (null)) && ((thisLeaseDelay) > (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("eb6f3ec4-bd6d-3720-8895-0e5b377338d7"))) ? (((nextLease) == (null)) && ((thisLeaseDelay) != (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("13db5d41-393f-34bb-9af2-1dc38c76312f"))) ? ((thisLeaseDelay) == (nextLeaseDelay)) : (((KnobRuntime.check(java.util.UUID.fromString("b968fedb-8ae2-3112-8c07-df4c304cf3af"))) ? (((nextLease) != (null)) && ((thisLeaseDelay) >= (nextLeaseDelay))) : (((KnobRuntime.check(java.util.UUID.fromString("174f5a50-ee44-3f3a-b32f-7fd8ce43c006"))) ? ((nextLease == null) && (thisLeaseDelay < nextLeaseDelay)) : (nextLease == null || thisLeaseDelay < nextLeaseDelay))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7f3f0862-276b-3631-ac7c-6a9a9e043063"))) {
return;
}
    this.stopRequested = true;
  }

  /**
   * Shut down this Leases instance. All pending leases will be destroyed, without any cancellation
   * calls.
   */
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("775c3302-fc38-35c3-b168-885f25136ff0"))) {
return;
}
    this.stopRequested = true;
    if (KnobRuntime.check(java.util.UUID.fromString("8488a24a-acaf-33b4-bd0d-aead5b4eeaa8"))) { close(); } else { leases.clear(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("c588042f-db91-3db8-aa75-132f63ab9058"))) {
return null;
}
    Lease lease = new Lease(leaseName, leaseTimeoutPeriod, listener);
if(KnobRuntime.check(java.util.UUID.fromString("d1e75f28-1de8-3990-8cbf-ac42d674f0c7"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32236a0e-d384-3035-97a4-88dfd2b4a089"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e89371d7-bd53-3c33-af35-aaf2d3f5148f"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("expirationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(lease));
    field.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e27757d-59e0-3c4c-9443-9c64816e18f1"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1966ecd-5f4b-39ae-9f69-03fdb0431a7a"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue / 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("c89c8a21-763b-3191-b083-d733a5323f87"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("381413ef-e494-3787-9643-14b80d9f1a7c"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("881e0f8c-ad32-3c59-90b6-9c36babf2770"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("500b0312-c8e4-32a1-a322-56822f0fd7a1"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f82558a1-0730-39f2-8fb8-a45de56894f0"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("expirationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(lease));
    field.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5b8c59d-8528-34f2-b308-79e34b86d467"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("29930452-dbd5-346f-8a83-ab5396d5d2b7"))) ? (!this.stopRequested) : (this.stopRequested))) {
      return;
    }
    if (leases.containsKey(lease.getLeaseName())) {
      throw new LeaseStillHeldException(lease.getLeaseName());
    }
    if (KnobRuntime.check(java.util.UUID.fromString("dce0814b-7af2-33b6-9502-45883bd216fe"))) { close(); } else { lease.resetExpirationTime(); }
if(KnobRuntime.check(java.util.UUID.fromString("2a4b31df-e1a0-3b2f-a337-262d0d09ac17"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("expirationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(lease));
    field.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3016bba7-9834-3796-9b8e-bdbcfd454f79"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5e590105-e415-362a-a1fa-1c1737787ea0"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aabf539d-c8af-3632-ab5b-ed593f7d83d1"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15d12afd-45a3-3733-a26d-a8eb22f50bb0"))) {
try {
    java.lang.reflect.Field field = lease.getClass().getDeclaredField("leaseTimeoutPeriod");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lease));
    field.set(lease, oldValue * 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("8ababfe5-adcb-3a48-9718-3afc17b1aa5d"))) {
return null;
}
    Lease lease = leases.remove(leaseName);
    if (((KnobRuntime.check(java.util.UUID.fromString("3f9ae253-48d0-3e98-9fd2-27ed72da64e7"))) ? ((lease) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("64e8911b-bd68-3b09-8452-0e3dccbded45"))) ? ((lease) == (null)) : (lease == null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c01daa5f-bb9c-313f-81a8-51bbb23e0149"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("72dd9edd-ad9b-3f33-bacf-4f1fda57f6be"))) {
return 0;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("75df058b-a001-3742-840d-45c52132ae12"))) {
return;
}
      this.expirationTime = EnvironmentEdgeManager.currentTime() + this.leaseTimeoutPeriod;
    }
  }
}
