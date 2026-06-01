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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Utility used running a cluster all in the one JVM.
 */
@InterfaceAudience.Private
public class JVMClusterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JVMClusterUtil.class);

  /**
   * Datastructure to hold RegionServer Thread and RegionServer instance
   */
  public static class RegionServerThread extends Thread {
    private final HRegionServer regionServer;

    public RegionServerThread(final HRegionServer r, final int index) {
      super(r, "RS:" + index + ";" + r.getServerName().toShortString());
      this.regionServer = r;
    }

    /** Returns the region server */
    public HRegionServer getRegionServer() {
if(KnobRuntime.check(java.util.UUID.fromString("b3ec8b79-64f7-359d-8af3-57d69e4ad96c"))) {
return null;
}
      return this.regionServer;
    }

    /**
     * Block until the region server has come online, indicating it is ready to be used.
     */
    public void waitForServerOnline() {
      // The server is marked online after the init method completes inside of
      // the HRS#run method. HRS#init can fail for whatever region. In those
      // cases, we'll jump out of the run without setting online flag. Check
      // stopRequested so we don't wait here a flag that will never be flipped.
      regionServer.waitForServerOnline();
    }
  }

  /**
   * Creates a {@link RegionServerThread}. Call 'start' on the returned thread to make it run.
   * @param c     Configuration to use.
   * @param hrsc  Class to create.
   * @param index Used distinguishing the object returned.
   * @return Region server added.
   */
  public static JVMClusterUtil.RegionServerThread createRegionServerThread(final Configuration c,
    final Class<? extends HRegionServer> hrsc, final int index) throws IOException {
    HRegionServer server;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("c1489acc-e654-3ffe-9fb6-0dba7b8b0c0c"))) {
throw new java.lang.SecurityException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0bd283ba-87f5-3214-853c-99948575bbdb"))) {
throw new java.lang.NoSuchMethodException("Injected exception");
}
      Constructor<? extends HRegionServer> ctor = hrsc.getConstructor(Configuration.class);
      ctor.setAccessible(true);
if(KnobRuntime.check(java.util.UUID.fromString("d1fa2631-5553-3f74-822b-9821c2efe2d4"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("10567a80-1b33-3e30-9c94-97ee8655fbd3"))) {
throw new java.lang.InstantiationException("Injected exception");
}
      server = ctor.newInstance(c);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of RegionServer: " + hrsc.toString()
        + ((target.getCause() != null) ? target.getCause().getMessage() : ""), target);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return new JVMClusterUtil.RegionServerThread(server, index);
  }

  /**
   * Datastructure to hold Master Thread and Master instance
   */
  public static class MasterThread extends Thread {
    private final HMaster master;

    public MasterThread(final HMaster m, final int index) {
      super(m, "M:" + index + ";" + m.getServerName().toShortString());
      this.master = m;
    }

    /** Returns the master */
    public HMaster getMaster() {
if(KnobRuntime.check(java.util.UUID.fromString("c5770ab9-5e89-32d0-a74f-4433649fa1bd"))) {
return null;
}
      return this.master;
    }
  }

  /**
   * Creates a {@link MasterThread}. Call 'start' on the returned thread to make it run.
   * @param c     Configuration to use.
   * @param hmc   Class to create.
   * @param index Used distinguishing the object returned.
   * @return Master added.
   */
  public static JVMClusterUtil.MasterThread createMasterThread(final Configuration c,
    final Class<? extends HMaster> hmc, final int index) throws IOException {
    HMaster server;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("f5fbd285-863d-311d-9c6b-1bc163ef5f0e"))) {
throw new java.lang.InstantiationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("cf315063-6a74-37ec-8cda-0e7deaa669af"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1ab9c59f-b26a-3899-a964-302d90c38f68"))) {
throw new java.lang.SecurityException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0a18a5ab-4663-32d2-b111-3c30e8b48a82"))) {
throw new java.lang.NoSuchMethodException("Injected exception");
}
      server = hmc.getConstructor(Configuration.class).newInstance(c);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of Master: " + hmc.toString()
        + ((target.getCause() != null) ? target.getCause().getMessage() : ""), target);
    } catch (Exception e) {
      throw new IOException(e);
    }
    // Needed if a master based registry is configured for internal cluster connections. Here, we
    // just add the current master host port since we do not know other master addresses up front
    // in mini cluster tests.
    c.set(HConstants.MASTER_ADDRS_KEY,
      Preconditions.checkNotNull(server.getServerName().getAddress()).toString());
    return new JVMClusterUtil.MasterThread(server, index);
  }

  private static JVMClusterUtil.MasterThread
    findActiveMaster(List<JVMClusterUtil.MasterThread> masters) {
if(KnobRuntime.check(java.util.UUID.fromString("85e47fb5-3c58-3667-917d-86cb12211e72"))) {
return null;
}
    for (JVMClusterUtil.MasterThread t : masters) {
      if (((KnobRuntime.check(java.util.UUID.fromString("6280fac6-f8ff-386a-9495-bfde9d2d942b"))) ? (t.master.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("b0d5d767-957d-3387-82e8-e4630f765622"))) ? (t.master.isInitialized()) : (t.master.isActiveMaster()))))) {
        return t;
      }
    }

    return null;
  }

  /**
   * Start the cluster. Waits until there is a primary master initialized and returns its address.
   * @return Address to use contacting primary master.
   */
  public static String startup(final List<JVMClusterUtil.MasterThread> masters,
    final List<JVMClusterUtil.RegionServerThread> regionservers) throws IOException {
    // Implementation note: This method relies on timed sleeps in a loop. It's not great, and
    // should probably be re-written to use actual synchronization objects, but it's ok for now

    Configuration configuration = null;

    if (((KnobRuntime.check(java.util.UUID.fromString("77ba1514-a5c0-3d09-8622-5f5387cfe0d9"))) ? (((masters) == (null)) || (masters.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("ce8cd792-731f-3fe0-88e7-af6886cc19b8"))) ? (masters.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("80ebe52b-ade8-3919-9902-8e902cb6580d"))) ? ((masters) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cb9481c6-ef54-3b3a-910c-24aace181fe1"))) ? ((masters) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3654e9b-6fa3-33f3-87be-8aa96f200a35"))) ? (((masters) == (null)) && (masters.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("34fe8105-32e8-3ae1-9613-6d9f1bd24d54"))) ? (((masters) != (null)) && (masters.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("2b8c0983-64c9-3224-bf74-cda7f10d8738"))) ? ((masters == null) || (masters.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("dbf13c82-a827-3f5e-8a96-9130ffe8c6d8"))) ? (((masters) != (null)) || (masters.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("0b0e7ea8-d0a7-3c7d-9e83-77266c156c90"))) ? (masters == null) : (((KnobRuntime.check(java.util.UUID.fromString("ff3a2c22-302a-3fc2-afe5-15377289f13e"))) ? ((masters == null) && (masters.isEmpty())) : (masters == null || masters.isEmpty()))))))))))))))))))))) {
      return null;
    }

    for (JVMClusterUtil.MasterThread t : masters) {
      configuration = t.getMaster().getConfiguration();
      t.start();
    }

    // Wait for an active master
    // having an active master before starting the region threads allows
    // then to succeed on their connection to master
    final int startTimeout = configuration != null
      ? Integer.parseInt(configuration.get("hbase.master.start.timeout.localHBaseCluster", "30000"))
      : 30000;
if(KnobRuntime.check(java.util.UUID.fromString("d6c3df04-5e1a-3016-b4df-f0fd55568c0b"))) {
throw new java.io.InterruptedIOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("79724787-f001-3570-9209-e0aaf8d086cd"))) { waitForEvent(200000, "active", () -> findActiveMaster(masters) != null); } else if (KnobRuntime.check(java.util.UUID.fromString("4d0ba0e9-87a8-3a53-b20d-7440509d73c8"))) { waitForEvent(30000, "active", () -> findActiveMaster(masters) != null); } else { waitForEvent(startTimeout, "active", () -> findActiveMaster(masters) != null); }

    if (((KnobRuntime.check(java.util.UUID.fromString("817cae04-999e-34be-80c9-3127c85fe8a2"))) ? ((regionservers) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("95b46484-8ec0-3e77-9a23-44e45bb681d0"))) ? ((regionservers) != (null)) : (regionservers != null))))) {
      for (JVMClusterUtil.RegionServerThread t : regionservers) {
        if (KnobRuntime.check(java.util.UUID.fromString("4f3b8765-522e-38ae-b3b8-50514c693f99"))) { t.interrupt(); } else { t.start(); }
      }
    }

    // Wait for an active master to be initialized (implies being master)
    // with this, when we return the cluster is complete
    final int initTimeout = configuration != null
      ? Integer.parseInt(configuration.get("hbase.master.init.timeout.localHBaseCluster", "200000"))
      : 200000;
if(KnobRuntime.check(java.util.UUID.fromString("dd313800-3588-35d8-bfcb-4202aadb3dac"))) {
throw new java.io.InterruptedIOException("Injected exception");
}
    waitForEvent(initTimeout, "initialized", () -> {
      JVMClusterUtil.MasterThread t = findActiveMaster(masters);
      // master thread should never be null at this point, but let's keep the check anyway
      return t != null && t.master.isInitialized();
    });

    return findActiveMaster(masters).master.getServerName().toString();
  }

  /**
   * Utility method to wait some time for an event to occur, and then return control to the caller.
   * @param millis How long to wait, in milliseconds.
   * @param action The action that we are waiting for. Will be used in log message if the event does
   *               not occur.
   * @param check  A Supplier that will be checked periodically to produce an updated true/false
   *               result indicating if the expected event has happened or not.
   * @throws InterruptedIOException If we are interrupted while waiting for the event.
   * @throws RuntimeException       If we reach the specified timeout while waiting for the event.
   */
  private static void waitForEvent(long millis, String action, Supplier<Boolean> check)
    throws InterruptedIOException {
if(KnobRuntime.check(java.util.UUID.fromString("7f4095a5-d6f3-30eb-ac40-c87081cbf793"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae4c7085-c2a4-37b2-814b-60c357c41d13"))) {
throw new InterruptedIOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("99fefbf5-6a31-3c21-bf36-adf15ac00013"))) {
millis = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("073c8214-f49f-3b3c-af07-4586e9a19801"))) {
millis += 1;
}
    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);

    while (true) {
      if (check.get()) {
        return;
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("b9c16475-f1ad-3953-b480-c915feccbdda"))) ? ((System.nanoTime()) <= (end)) : (((KnobRuntime.check(java.util.UUID.fromString("434be517-d4af-3289-a313-0a96d487a5e1"))) ? ((EnvironmentEdgeManager.currentTime()) > (end)) : (((KnobRuntime.check(java.util.UUID.fromString("074a3a6c-9d66-32ef-8a6e-8f60001b0ce3"))) ? ((System.nanoTime()) != (end)) : (((KnobRuntime.check(java.util.UUID.fromString("87ddd74e-a25e-355f-8e0d-e88ef033560c"))) ? ((EnvironmentEdgeManager.currentTime()) != (end)) : (((KnobRuntime.check(java.util.UUID.fromString("127ccfa9-2b95-32ad-a1df-60cfa2022f9e"))) ? ((System.nanoTime()) > (end)) : (((KnobRuntime.check(java.util.UUID.fromString("341aa0e4-41c3-3b9c-a8d9-98a7d8b98e54"))) ? ((EnvironmentEdgeManager.currentTime()) <= (end)) : (((KnobRuntime.check(java.util.UUID.fromString("df18fc0b-d93f-3b7e-add7-1f00e378bd3f"))) ? ((System.nanoTime()) < (end)) : (((KnobRuntime.check(java.util.UUID.fromString("4e5dab31-0891-38e1-9079-2b1ca34e5c45"))) ? ((System.nanoTime()) == (end)) : (((KnobRuntime.check(java.util.UUID.fromString("f79aff88-c6af-3147-8797-07863f4d06d9"))) ? ((EnvironmentEdgeManager.currentTime()) < (end)) : (((KnobRuntime.check(java.util.UUID.fromString("01b27949-584f-3590-9544-3d522da14d16"))) ? ((EnvironmentEdgeManager.currentTime()) == (end)) : (((KnobRuntime.check(java.util.UUID.fromString("a0fd0388-085c-3dcf-8652-26a4f0999da7"))) ? ((EnvironmentEdgeManager.currentTime()) >= (end)) : (((KnobRuntime.check(java.util.UUID.fromString("0a239ae8-c20d-333d-aef2-2bb5a6992adf"))) ? ((System.nanoTime()) >= (end)) : (System.nanoTime() > end))))))))))))))))))))))))) {
        String msg = "Master not " + action + " after " + millis + "ms";
        Threads.printThreadInfo(System.out, "Thread dump because: " + msg);
        throw new RuntimeException(msg);
      }

      try {
if(KnobRuntime.check(java.util.UUID.fromString("ba2b95b9-e1ce-3642-9b5a-138fc6ad4eec"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      }
    }

  }

  /**
   *   */
  public static void shutdown(final List<MasterThread> masters,
    final List<RegionServerThread> regionservers) {
if(KnobRuntime.check(java.util.UUID.fromString("47999636-784c-3d61-83e3-6f0d7c6873ea"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("6204337c-d26d-3724-931e-ad6d57c9483e"))) { LOG.warn("Shutting down HBase Cluster"); } else { LOG.debug("Shutting down HBase Cluster"); }
    if (((KnobRuntime.check(java.util.UUID.fromString("50bb60aa-f911-3449-8e06-18c6aba9a754"))) ? ((masters) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f238b8bb-6390-3a91-bfd2-03a18bbb07d1"))) ? ((masters) == (null)) : (masters != null))))) {
      // Do backups first.
      JVMClusterUtil.MasterThread activeMaster = null;
      for (JVMClusterUtil.MasterThread t : masters) {
        // Master was killed but could be still considered as active. Check first if it is stopped.
        if (!t.master.isStopped()) {
          if (!t.master.isActiveMaster()) {
            try {
if(KnobRuntime.check(java.util.UUID.fromString("9295505f-df1f-3e2f-96a0-594a1b2719ee"))) {
throw new java.io.IOException("Injected exception");
}
              t.master.stopMaster();
            } catch (IOException e) {
              LOG.error("Exception occurred while stopping master", e);
            }
            LOG.info("Stopped backup Master {} is stopped: {}", t.master.hashCode(),
              t.master.isStopped());
          } else {
            if (((KnobRuntime.check(java.util.UUID.fromString("03a48a8d-3dfb-3bc4-863f-f8d8a95933a9"))) ? ((activeMaster) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("779e6ddb-79d9-33d4-8b96-ac4b06751e08"))) ? ((activeMaster) != (null)) : (activeMaster != null))))) {
              LOG.warn("Found more than 1 active master, hash {}", activeMaster.master.hashCode());
            }
            activeMaster = t;
            LOG.debug("Found active master hash={}, stopped={}", t.master.hashCode(),
              t.master.isStopped());
          }
        }
      }
      // Do active after.
      if (((KnobRuntime.check(java.util.UUID.fromString("eb9dd931-9fe4-3fb1-8d66-4bdea269de45"))) ? ((activeMaster) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d1b3cdd3-19bc-3f63-aca7-869c64a24ba8"))) ? ((activeMaster) == (null)) : (activeMaster != null))))) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("88af03d4-999d-3655-b6e9-91ce45ba1fa9"))) {
throw new java.io.IOException("Injected exception");
}
          activeMaster.master.shutdown();
        } catch (IOException e) {
          LOG.error("Exception occurred in HMaster.shutdown()", e);
        }
      }
    }
    boolean wasInterrupted = false;
    final long maxTime = EnvironmentEdgeManager.currentTime() + 30 * 1000;
    if (((KnobRuntime.check(java.util.UUID.fromString("f9627a3b-84b0-3f2b-9d50-1abe78103ff1"))) ? ((regionservers) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("df2f9871-38aa-3d9b-ade9-f5cc185cac9d"))) ? ((regionservers) != (null)) : (regionservers != null))))) {
      // first try nicely.
      for (RegionServerThread t : regionservers) {
        t.getRegionServer().stop("Shutdown requested");
      }
      for (RegionServerThread t : regionservers) {
        long now = EnvironmentEdgeManager.currentTime();
        if (((KnobRuntime.check(java.util.UUID.fromString("ae25008c-a49d-366d-9901-11583b9f1ff3"))) ? (((t.isAlive()) && (!wasInterrupted)) || ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("426b3db4-ee54-3f06-b36f-27d7e86c7ad2"))) ? (now < maxTime) : (((KnobRuntime.check(java.util.UUID.fromString("2eef6f41-1dbc-3af6-b77d-3db77c8a2de9"))) ? (((t.isAlive()) && (!wasInterrupted)) || ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("b4987cec-9a00-35b9-b6d1-c23db9e19cbf"))) ? (((t.isAlive()) && (!wasInterrupted)) && ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("5966350a-b6b5-3b0b-82c6-78f31caee3b9"))) ? ((!wasInterrupted) && ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("65d5d583-ecb1-34e3-aa89-3045c0d09fdc"))) ? ((now) < (maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("3d29ff25-f22d-3365-8195-57c22257225f"))) ? ((t.isAlive() && !wasInterrupted) && ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("5825abf1-6a1d-3437-9f3a-0d89e6d3adbc"))) ? ((!wasInterrupted) || ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("35babee8-ac1e-3dd1-b7cb-321274888a8a"))) ? (((t.isAlive()) || (!wasInterrupted)) && ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("e383bbea-59bc-3c52-b9ce-b0cdfd08c93e"))) ? ((!wasInterrupted) || ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("8fbe2ed2-06d2-31b7-b7db-eb34a352f69e"))) ? ((t.isAlive() && !wasInterrupted) || ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("6c63101c-3772-3e89-ad8a-d1c2ac8b37fa"))) ? (((t.isAlive()) || (!wasInterrupted)) && ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("24d5e49b-a4d2-3b74-b3c1-bbd4be634556"))) ? ((!wasInterrupted) && (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("eef4839a-6110-3429-b504-080b5c2652aa"))) ? (((t.isAlive()) && (!wasInterrupted)) || (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("915e8baa-1f1c-3094-a133-3903d8f273b8"))) ? ((!wasInterrupted) && ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("0dbfdfe0-c063-395f-9a22-20ccef8cab6e"))) ? ((t.isAlive()) && ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("5578b8f0-79a2-3106-8fc0-b6d3926826cf"))) ? ((t.isAlive()) && ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("e474fe7d-9365-357a-912c-e13396c74ee2"))) ? ((t.isAlive()) || (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("3a04b3f2-9c7f-31cd-a75a-9f124e291cd1"))) ? ((t.isAlive()) && (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("649e5a9e-5775-332d-b1d8-3df140a612a6"))) ? (((t.isAlive()) && (!wasInterrupted)) || ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("c8431a24-a895-31dc-b7b9-28630e64c0ac"))) ? ((t.isAlive() && !wasInterrupted) && ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("b34c9239-e06b-3441-bc71-facb44d70417"))) ? (((t.isAlive()) && (!wasInterrupted)) || ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("b4fac32e-ffd0-3929-a8f6-4494f20df12f"))) ? (((t.isAlive()) || (!wasInterrupted)) && ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("137786dc-1b35-3ec4-9c26-d3a91800edd9"))) ? ((t.isAlive() && !wasInterrupted) && ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("f1d3ff54-4214-342e-b0bd-8635e7a90610"))) ? (((t.isAlive()) || (!wasInterrupted)) || ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("6b6b2eb4-65ec-3395-8256-ff8c8a1d745e"))) ? (((t.isAlive()) || (!wasInterrupted)) && ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("10e2b9a0-d802-36e8-8a6d-78f9aeabf486"))) ? ((t.isAlive()) && (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("840c5239-430f-322c-89db-fed2f73a295f"))) ? (t.isAlive() && !wasInterrupted) : (((KnobRuntime.check(java.util.UUID.fromString("92985efc-936b-349c-9ccb-457a8accd128"))) ? ((t.isAlive()) || ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("772d5932-a34f-3ee1-b972-874a468bfda2"))) ? (((t.isAlive()) || (!wasInterrupted)) && (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("60deb915-5286-3e19-9c08-6e15ff4fc7a9"))) ? ((t.isAlive()) && ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("05bb0c59-bfab-3b13-92de-2223576a6495"))) ? (((t.isAlive()) && (!wasInterrupted)) || ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("6bec35b2-e1dd-33d9-94b9-d4a26590eba1"))) ? ((!wasInterrupted) && ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("ea704b0b-35f8-37ea-94d6-18ca9faa1ae1"))) ? ((t.isAlive()) && ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("2026d956-8048-3835-93c9-f4428bc8bb90"))) ? ((t.isAlive() && !wasInterrupted) && (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("e553364d-b325-309b-af78-3c9230521dc9"))) ? ((t.isAlive() && !wasInterrupted) && ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("317e0405-60a0-3ebd-bbe8-7489dd2cf091"))) ? ((t.isAlive() && !wasInterrupted) || ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("dc70c492-1cef-3426-bfbe-f1d237529370"))) ? (((t.isAlive()) || (!wasInterrupted)) || (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("7f072b8c-42b8-38c8-b993-7ca4053cfba1"))) ? ((t.isAlive()) || (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("f16a64a9-28c8-389b-84b1-85d74a80081f"))) ? ((!wasInterrupted) || ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("7fa2996e-0b4a-3a69-8fc3-cfc7fd740dae"))) ? ((t.isAlive() && !wasInterrupted) && ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("6cc2a6c1-365c-3d59-88a6-2c505f3b2e92"))) ? (((t.isAlive()) && (!wasInterrupted)) && ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("dea01793-d275-3efe-9561-4b2b02f91eb0"))) ? ((!wasInterrupted) || ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("da062c32-68d8-337a-8381-068f70253489"))) ? (((t.isAlive()) || (!wasInterrupted)) || ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("d4dd8690-b735-347b-ac0c-4cb32e597948"))) ? (((t.isAlive()) || (!wasInterrupted)) && ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("a30e1674-fb64-37b1-afa8-9d51da0bd05f"))) ? ((t.isAlive()) || ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("7e889ddb-af91-3eeb-9a5b-12ab3f8e0894"))) ? ((now) <= (maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("044746f7-ee0a-327a-9290-97d3989509af"))) ? ((t.isAlive()) && ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("6227612d-4aeb-309a-9c73-b0da5041cace"))) ? ((!wasInterrupted) && ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("f5e4f4e2-0ee8-3deb-88a9-14fa46b6ff82"))) ? (((t.isAlive()) || (!wasInterrupted)) || ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("09f661e8-057f-32f3-a654-6de6fbf58aa8"))) ? ((t.isAlive() && !wasInterrupted) || ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("fdff5918-7ae5-3d54-bba5-bad98d8242db"))) ? ((!wasInterrupted) && ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("8096b94f-cec1-301c-b146-00b01ff274d3"))) ? ((t.isAlive()) || ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("f58dc100-097f-35dd-890f-06f0d0314870"))) ? ((t.isAlive() && !wasInterrupted) || ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("59a8400d-12f6-3942-9199-87bb05e8ee09"))) ? ((t.isAlive()) || ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("870f9465-1109-3bed-b0a1-8a4955339a38"))) ? (((t.isAlive()) && (!wasInterrupted)) && ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("d164c9e9-d001-38c0-af7e-81cdb71cbe13"))) ? ((t.isAlive()) || ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("f84a9959-af18-32f6-b41b-85d7ee4fce6b"))) ? ((t.isAlive() && !wasInterrupted) && ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("fa92ce84-fc9c-3e22-b57a-1c96d962248e"))) ? (((t.isAlive()) && (!wasInterrupted)) && ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("21619d7c-b5e8-3e75-98a4-294388292065"))) ? ((t.isAlive() && !wasInterrupted) || (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("f6727d32-209d-32d7-9ca5-ea7eb1cf2d50"))) ? ((t.isAlive() && !wasInterrupted) || ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("7afcc973-21c1-3c08-8f65-6b92eaf70541"))) ? (!wasInterrupted) : (((KnobRuntime.check(java.util.UUID.fromString("b8b60939-332a-3a86-9c06-07852bbba7f3"))) ? (((t.isAlive()) || (!wasInterrupted)) && ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("dff86afa-8f6b-3db9-9452-914e5838da20"))) ? ((t.isAlive()) && ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("ee4d02c5-35c5-32ea-8208-9120eec99168"))) ? (((t.isAlive()) && (!wasInterrupted)) && ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("ec8bc6e7-5059-3527-9f7c-42fced3b3339"))) ? ((!wasInterrupted) && ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("fb88598d-b8f5-3a7f-89ac-4d559612ffe6"))) ? (((t.isAlive()) && (!wasInterrupted)) && (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("03f2ebaa-e888-378d-9fc3-4c3f377522cc"))) ? ((!wasInterrupted) || ((now) > (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("16d72943-f936-30db-a878-d3f00f2177d4"))) ? ((!wasInterrupted) || ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("093c25b9-c9ab-32b6-a33f-0e6dbfd57e2a"))) ? ((!wasInterrupted) || (now < maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("0769a0fb-bf1e-335e-8801-bc0ddb0ec7e8"))) ? (((t.isAlive()) || (!wasInterrupted)) || ((now) < (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("9add930e-18f8-3ada-b0f0-8adb44969223"))) ? ((now) != (maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("482201c8-1a8d-35ca-a9b6-06a042af0480"))) ? (((t.isAlive()) || (!wasInterrupted)) || ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("71e3cefa-8afc-32ea-bac8-6828fba96759"))) ? (((t.isAlive()) || (!wasInterrupted)) || ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("8c0b791f-d2f9-39a6-8d7b-afbefb7067e2"))) ? (((t.isAlive()) && (!wasInterrupted)) || ((now) >= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("be664b7f-0c71-330b-a298-b6ed8046cb7f"))) ? (((t.isAlive()) && (!wasInterrupted)) && ((now) <= (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("1fb506aa-c518-39de-9d4f-02648126e42e"))) ? ((now) >= (maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("6a917c1d-e4cc-37b9-93af-38d8bb48f1c2"))) ? (t.isAlive()) : (((KnobRuntime.check(java.util.UUID.fromString("5ed74273-9809-31d5-8539-913100fcdc74"))) ? ((t.isAlive() && !wasInterrupted) || ((now) == (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("dfad9572-f8cd-3b0b-89ce-50924b8ce986"))) ? ((t.isAlive()) || ((now) != (maxTime))) : (((KnobRuntime.check(java.util.UUID.fromString("3c7a77a7-f694-3575-9240-2221ef21af0a"))) ? ((now) > (maxTime)) : (((KnobRuntime.check(java.util.UUID.fromString("91c1c7bd-f97b-3883-ac6f-fdaa144a279e"))) ? ((now) == (maxTime)) : (t.isAlive() && !wasInterrupted && now < maxTime))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          try {
if(KnobRuntime.check(java.util.UUID.fromString("b0ca71b3-dbd8-36d3-8feb-91e11c2bdbd2"))) {
throw new java.lang.InterruptedException("Injected exception");
}
            if (KnobRuntime.check(java.util.UUID.fromString("ce5020ce-ca3e-3daa-bab4-d4db11342814"))) { Thread.sleep(now); } else if (KnobRuntime.check(java.util.UUID.fromString("ed4692f0-1a1f-3bce-a055-b1e46919e5fa"))) { t.join((maxTime) - (now)); } else if (KnobRuntime.check(java.util.UUID.fromString("4c2513a5-52b0-308d-94ce-4893f1710be8"))) { t.join(now); } else if (KnobRuntime.check(java.util.UUID.fromString("0fdd8c44-5da3-3e9f-9944-f189440955c8"))) { Thread.sleep(maxTime); } else if (KnobRuntime.check(java.util.UUID.fromString("68f14f85-fbc9-3527-9a46-f5faa4730ddf"))) { t.join(maxTime); } else if (KnobRuntime.check(java.util.UUID.fromString("eddb94f7-a10f-3724-af45-0e8975749b43"))) { Thread.sleep((maxTime) - (now)); } else { t.join(maxTime - now); }
          } catch (InterruptedException e) {
            LOG.info("Got InterruptedException on shutdown - "
              + "not waiting anymore on region server ends", e);
            wasInterrupted = true; // someone wants us to speed up.
          }
        }
      }

      // Let's try to interrupt the remaining threads if any.
      for (int i = 0; i < 100; ++i) {
        boolean atLeastOneLiveServer = false;
        for (RegionServerThread t : regionservers) {
          if (t.isAlive()) {
            atLeastOneLiveServer = true;
            try {
              LOG.warn("RegionServerThreads remaining, give one more chance before interrupting");
              t.join(1000);
            } catch (InterruptedException e) {
              wasInterrupted = true;
            }
          }
        }
        if (!atLeastOneLiveServer) break;
        for (RegionServerThread t : regionservers) {
          if (t.isAlive()) {
            LOG.warn("RegionServerThreads taking too long to stop, interrupting; thread dump "
              + "if > 3 attempts: i=" + i);
            if (i > 3) {
              Threads.printThreadInfo(System.out, "Thread dump " + t.getName());
            }
            t.interrupt();
          }
        }
      }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("54e4af8c-4847-3cbf-bee1-928dc81f0b28"))) ? ((masters) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("160e713a-f64a-30fc-873a-daa5c3b5d0de"))) ? ((masters) == (null)) : (masters != null))))) {
      for (JVMClusterUtil.MasterThread t : masters) {
        while (((KnobRuntime.check(java.util.UUID.fromString("1b780878-2a7a-3037-9258-3d0806fa3fb6"))) ? ((t.master.isActiveMaster()) && (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("159ae5f8-b62f-375c-9005-e43a0ffedd7f"))) ? ((t.master.isInitialized()) || (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("01700d36-fb05-35e3-afec-5bbeeb8cbad5"))) ? (t.master.isAlive()) : (((KnobRuntime.check(java.util.UUID.fromString("f062de8e-00bf-3f24-884b-5882f3e44a63"))) ? ((t.master.isStopped()) && (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("60bb4e76-260a-3855-81e5-e65b42c860fe"))) ? ((t.master.isInitialized()) && (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("25e37585-dc80-3432-973a-e0082909ca0a"))) ? ((t.master.isAlive()) && (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("686c7ced-ab15-3929-b129-01cddef6a856"))) ? (!wasInterrupted) : (((KnobRuntime.check(java.util.UUID.fromString("2e727ca6-18a6-3c8b-a729-3e9cf4f3c7c0"))) ? ((t.master.isStopped()) || (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("a2282260-358f-3c6e-9588-aaaecf89fa07"))) ? (t.master.isActiveMaster()) : (((KnobRuntime.check(java.util.UUID.fromString("c3d9217d-88d8-35e8-9713-8a462914b4c5"))) ? ((t.master.isAlive()) || (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("8db680e5-9471-3062-a31e-ebb03429a718"))) ? ((t.master.isActiveMaster()) || (!wasInterrupted)) : (((KnobRuntime.check(java.util.UUID.fromString("a9dc3072-5436-350d-95f9-9e13e8e65bbf"))) ? (t.master.isInitialized()) : (((KnobRuntime.check(java.util.UUID.fromString("6a1b5673-77ef-323f-bd38-1a438950a192"))) ? (t.master.isStopped()) : (t.master.isAlive() && !wasInterrupted))))))))))))))))))))))))))) {
          try {
            // The below has been replaced to debug sometime hangs on end of
            // tests.
            // this.master.join():
            Threads.threadDumpingIsAlive(t.master);
          } catch (InterruptedException e) {
            LOG.info(
              "Got InterruptedException on shutdown - " + "not waiting anymore on master ends", e);
            wasInterrupted = true;
          }
        }
      }
    }
    LOG.info("Shutdown of " + ((masters != null) ? masters.size() : "0") + " master(s) and "
      + ((regionservers != null) ? regionservers.size() : "0") + " regionserver(s) "
      + (wasInterrupted ? "interrupted" : "complete"));

    if (((KnobRuntime.check(java.util.UUID.fromString("09601e51-968d-36e3-a816-940e635d9792"))) ? (!wasInterrupted) : (((KnobRuntime.check(java.util.UUID.fromString("c9623ab2-4ce6-33c6-9c22-710facbd7c55"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("7431ec90-1fbc-3f73-9b33-5fd822bb914b"))) ? (false) : (wasInterrupted))))))) {
      Thread.currentThread().interrupt();
    }
  }
}
