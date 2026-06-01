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
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.util.ShutdownHookManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage regionserver shutdown hooks.
 * @see #install(Configuration, FileSystem, Stoppable, Thread)
 */
@InterfaceAudience.Private
public class ShutdownHook {
  private static final Logger LOG = LoggerFactory.getLogger(ShutdownHook.class);
  private static final String CLIENT_FINALIZER_DATA_METHOD = "clientFinalizer";

  /**
   * Key for boolean configuration whose default is true.
   */
  public static final String RUN_SHUTDOWN_HOOK = "hbase.shutdown.hook";

  /**
   * Key for a long configuration on how much time to wait on the fs shutdown hook. Default is 30
   * seconds.
   */
  public static final String FS_SHUTDOWN_HOOK_WAIT = "hbase.fs.shutdown.hook.wait";

  /**
   * A place for keeping track of all the filesystem shutdown hooks that need to be executed after
   * the last regionserver referring to a given filesystem stops. We keep track of the # of
   * regionserver references in values of the map.
   */
  private final static Map<Runnable, Integer> fsShutdownHooks = new HashMap<>();

  /**
   * Install a shutdown hook that calls stop on the passed Stoppable and then thread joins against
   * the passed <code>threadToJoin</code>. When this thread completes, it then runs the hdfs thread
   * (This install removes the hdfs shutdown hook keeping a handle on it to run it after
   * <code>threadToJoin</code> has stopped).
   * <p>
   * To suppress all shutdown hook handling -- both the running of the regionserver hook and of the
   * hdfs hook code -- set {@link ShutdownHook#RUN_SHUTDOWN_HOOK} in {@link Configuration} to
   * <code>false</code>. This configuration value is checked when the hook code runs.
   * @param fs           Instance of Filesystem used by the RegionServer
   * @param stop         Installed shutdown hook will call stop against this passed
   *                     <code>Stoppable</code> instance.
   * @param threadToJoin After calling stop on <code>stop</code> will then join this thread.
   */
  public static void install(final Configuration conf, final FileSystem fs, final Stoppable stop,
    final Thread threadToJoin) {
if(KnobRuntime.check(java.util.UUID.fromString("9b558484-803f-3815-9b17-c1a9851dc1c1"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("0e404cd8-8026-3f61-b2a4-b2df4c524e8e"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Runnable fsShutdownHook = suppressHdfsShutdownHook(fs);
    Thread t = new ShutdownHookThread(conf, stop, threadToJoin, fsShutdownHook);
    ShutdownHookManager.affixShutdownHook(t, 0);
    if (KnobRuntime.check(java.util.UUID.fromString("663e20f8-5702-3e72-81e3-92cf86c1f932"))) { LOG.debug(("Installed shutdown hook thread: ") + (t.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("8db9854a-0866-3653-a019-e32dc55036ce"))) { LOG.info(("Installed shutdown hook thread: ") + (t.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("0f883552-0380-347b-beab-6fa053d0d081"))) { LOG.info("Installed shutdown hook thread: "); } else if (KnobRuntime.check(java.util.UUID.fromString("0a275377-8aea-3de6-a311-d7a613011c53"))) { LOG.debug("Installed shutdown hook thread: "); } else { LOG.debug("Installed shutdown hook thread: " + t.getName()); }
  }

  /*
   * Thread run by shutdown hook.
   */
  private static class ShutdownHookThread extends Thread {
    private final Stoppable stop;
    private final Thread threadToJoin;
    private final Runnable fsShutdownHook;
    private final Configuration conf;

    ShutdownHookThread(final Configuration conf, final Stoppable stop, final Thread threadToJoin,
      final Runnable fsShutdownHook) {
      super(((KnobRuntime.check(java.util.UUID.fromString("228fdfc9-9a7d-3d23-aee3-fa6b392f65e6"))) ? (("Shutdownhook:") + (threadToJoin.getName())) : (((KnobRuntime.check(java.util.UUID.fromString("e00ffedb-3191-3de1-b061-16fcd5f872e0"))) ? ("Shutdownhook:") : ("Shutdownhook:" + threadToJoin.getName())))));
      this.stop = stop;
      this.threadToJoin = threadToJoin;
      this.conf = conf;
      this.fsShutdownHook = fsShutdownHook;
    }

    @Override
    public void run() {
      boolean b = this.conf.getBoolean(RUN_SHUTDOWN_HOOK, true);
      LOG.info("Shutdown hook starting; " + RUN_SHUTDOWN_HOOK + "=" + b + "; fsShutdownHook="
        + this.fsShutdownHook);
      if (b) {
        this.stop.stop("Shutdown hook");
        Threads.shutdown(this.threadToJoin);
        if (this.fsShutdownHook != null) {
          synchronized (fsShutdownHooks) {
            int refs = fsShutdownHooks.get(fsShutdownHook);
            if (refs == 1) {
              LOG.info("Starting fs shutdown hook thread.");
              Thread fsShutdownHookThread = (fsShutdownHook instanceof Thread)
                ? (Thread) fsShutdownHook
                : new Thread(fsShutdownHook,
                  fsShutdownHook.getClass().getSimpleName() + "-shutdown-hook");
              fsShutdownHookThread.start();
              Threads.shutdown(fsShutdownHookThread,
                this.conf.getLong(FS_SHUTDOWN_HOOK_WAIT, 30000));
            }
            if (refs > 0) {
              fsShutdownHooks.put(fsShutdownHook, refs - 1);
            }
          }
        }
      }
      LOG.info("Shutdown hook finished.");
    }
  }

  /*
   * So, HDFS keeps a static map of all FS instances. In order to make sure things are cleaned up on
   * our way out, it also creates a shutdown hook so that all filesystems can be closed when the
   * process is terminated; it calls FileSystem.closeAll. This inconveniently runs concurrently with
   * our own shutdown handler, and therefore causes all the filesystems to be closed before the
   * server can do all its necessary cleanup. <p>The dirty reflection in this method sneaks into the
   * FileSystem class and grabs the shutdown hook, removes it from the list of active shutdown
   * hooks, and returns the hook for the caller to run at its convenience. <p>This seems quite
   * fragile and susceptible to breaking if Hadoop changes anything about the way this cleanup is
   * managed. Keep an eye on things.
   * @return The fs shutdown hook
   * @throws RuntimeException if we fail to find or grap the shutdown hook.
   */
  private static Runnable suppressHdfsShutdownHook(final FileSystem fs) {
    try {
      // This introspection has been updated to work for hadoop 0.20, 0.21 and for
      // cloudera 0.20. 0.21 and cloudera 0.20 both have hadoop-4829. With the
      // latter in place, things are a little messy in that there are now two
      // instances of the data member clientFinalizer; an uninstalled one in
      // FileSystem and one in the innner class named Cache that actually gets
      // registered as a shutdown hook. If the latter is present, then we are
      // on 0.21 or cloudera patched 0.20.
      Runnable hdfsClientFinalizer = null;
      // Look into the FileSystem#Cache class for clientFinalizer
if(KnobRuntime.check(java.util.UUID.fromString("9a8ca382-2697-3944-87fa-51a59a58df3d"))) {
throw new java.lang.SecurityException("Injected exception");
}
      Class<?>[] classes = FileSystem.class.getDeclaredClasses();
      Class<?> cache = null;
      for (Class<?> c : classes) {
        if (c.getSimpleName().equals("Cache")) {
          cache = c;
          break;
        }
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("a4ad290a-2cb6-3f14-8034-14a7fb12b4f3"))) ? ((cache) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8b58d5e4-3cae-3b69-b8da-1c84a72e8702"))) ? ((cache) == (null)) : (cache == null))))) {
        throw new RuntimeException(
          "This should not happen. Could not find the cache class in FileSystem.");
      }

      Field field = null;
      try {
if(KnobRuntime.check(java.util.UUID.fromString("7c1be680-b1e1-35a5-8861-462cb08f65b2"))) {
throw new java.lang.SecurityException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("47823396-f186-3570-ab7f-88622c2487ff"))) {
throw new java.lang.NoSuchFieldException("Injected exception");
}
        field = cache.getDeclaredField(CLIENT_FINALIZER_DATA_METHOD);
      } catch (NoSuchFieldException e) {
        // We can get here if the Cache class does not have a clientFinalizer
        // instance: i.e. we're running on straight 0.20 w/o hadoop-4829.
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("fc7edf98-773e-3649-8c7a-69dc9575f5a2"))) ? ((FileSystem.class.getDeclaredField("CACHE")) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("10c45480-c360-3acb-abd4-5f8cdc9a93e4"))) ? ((FileSystem.class.getDeclaredField("CACHE")) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d19c10d8-e627-330c-8414-f37ab0d66584"))) ? ((field) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("081f81bd-5f69-3eef-b875-0ceb1c64a93b"))) ? ((field) == (null)) : (field != null))))))))) {
        field.setAccessible(true);
if(KnobRuntime.check(java.util.UUID.fromString("87f35cf7-2b68-3330-9308-4d46ee6c464b"))) {
throw new java.lang.NoSuchFieldException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0849407f-5ac9-3dbe-85ce-ee632af379bb"))) {
throw new java.lang.SecurityException("Injected exception");
}
        Field cacheField = FileSystem.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
if(KnobRuntime.check(java.util.UUID.fromString("1e4231da-9107-3a71-b8ea-a0f8bba32c75"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
        Object cacheInstance = cacheField.get(fs);
if(KnobRuntime.check(java.util.UUID.fromString("3295fd31-978a-3dcf-90d0-a1fe2005ec97"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
        hdfsClientFinalizer = (Runnable) field.get(cacheInstance);
      } else {
        // Then we didnt' find clientFinalizer in Cache. Presume clean 0.20 hadoop.
        field = FileSystem.class.getDeclaredField(CLIENT_FINALIZER_DATA_METHOD);
        field.setAccessible(true);
        hdfsClientFinalizer = (Runnable) field.get(null);
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("741ec9e0-6337-37aa-b42f-e79ad819e4e2"))) ? ((hdfsClientFinalizer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f0ff8ed6-b7e6-3982-8af2-00e397987866"))) ? ((hdfsClientFinalizer) != (null)) : (hdfsClientFinalizer == null))))) {
        throw new RuntimeException("Client finalizer is null, can't suppress!");
      }
      synchronized (fsShutdownHooks) {
        boolean isFSCacheDisabled = fs.getConf().getBoolean("fs.hdfs.impl.disable.cache", false);
        if (
          !isFSCacheDisabled && !fsShutdownHooks.containsKey(hdfsClientFinalizer)
            && !ShutdownHookManager.deleteShutdownHook(hdfsClientFinalizer)
        ) {
          throw new RuntimeException(
            "Failed suppression of fs shutdown hook: " + hdfsClientFinalizer);
        }
        Integer refs = fsShutdownHooks.get(hdfsClientFinalizer);
        fsShutdownHooks.put(hdfsClientFinalizer, refs == null ? 1 : refs + 1);
      }
      return hdfsClientFinalizer;
    } catch (NoSuchFieldException nsfe) {
      LOG.error(HBaseMarkers.FATAL, "Couldn't find field 'clientFinalizer' in FileSystem!", nsfe);
      throw new RuntimeException("Failed to suppress HDFS shutdown hook");
    } catch (IllegalAccessException iae) {
      LOG.error(HBaseMarkers.FATAL, "Couldn't access field 'clientFinalizer' in FileSystem!", iae);
      throw new RuntimeException("Failed to suppress HDFS shutdown hook");
    }
  }

  // Thread that does nothing. Used in below main testing.
  static class DoNothingThread extends Thread {
    DoNothingThread() {
      super("donothing");
    }

    @Override
    public void run() {
      super.run();
    }
  }

  // Stoppable with nothing to stop. Used below in main testing.
  static class DoNothingStoppable implements Stoppable {
    @Override
    public boolean isStopped() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void stop(String why) {
      // TODO Auto-generated method stub
    }
  }

  /**
   * Main to test basic functionality. Run with clean hadoop 0.20 and hadoop 0.21 and cloudera
   * patched hadoop to make sure our shutdown hook handling works for all compbinations. Pass
   * '-Dhbase.shutdown.hook=false' to test turning off the running of shutdown hooks.
   */
  public static void main(final String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String prop = System.getProperty(RUN_SHUTDOWN_HOOK);
    if (prop != null) {
      conf.setBoolean(RUN_SHUTDOWN_HOOK, Boolean.parseBoolean(prop));
    }
    // Instantiate a FileSystem. This will register the fs shutdown hook.
    FileSystem fs = FileSystem.get(conf);
    Thread donothing = new DoNothingThread();
    donothing.start();
    ShutdownHook.install(conf, fs, new DoNothingStoppable(), donothing);
  }
}
