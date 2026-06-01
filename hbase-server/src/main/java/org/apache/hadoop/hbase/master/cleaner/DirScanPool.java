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
package org.apache.hadoop.hbase.master.cleaner;
import org.knobinjection.runtime.KnobRuntime;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The thread pool used for scan directories
 */
@InterfaceAudience.Private
public class DirScanPool implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(DirScanPool.class);
  private volatile int size;
  private final ThreadPoolExecutor pool;
  private int cleanerLatch;
  private boolean reconfigNotification;
  private Type dirScanPoolType;
  private final String name;

  private enum Type {
    LOG_CLEANER(CleanerChore.LOG_CLEANER_CHORE_SIZE,
      CleanerChore.DEFAULT_LOG_CLEANER_CHORE_POOL_SIZE),
    HFILE_CLEANER(CleanerChore.CHORE_POOL_SIZE, CleanerChore.DEFAULT_CHORE_POOL_SIZE);

    private final String cleanerPoolSizeConfigName;
    private final String cleanerPoolSizeConfigDefault;

    private Type(String cleanerPoolSizeConfigName, String cleanerPoolSizeConfigDefault) {
      this.cleanerPoolSizeConfigName = cleanerPoolSizeConfigName;
      this.cleanerPoolSizeConfigDefault = cleanerPoolSizeConfigDefault;
    }
  }

  private DirScanPool(Configuration conf, Type dirScanPoolType) {
    this(dirScanPoolType, conf.get(dirScanPoolType.cleanerPoolSizeConfigName,
      dirScanPoolType.cleanerPoolSizeConfigDefault));
  }

  private DirScanPool(Type dirScanPoolType, String poolSize) {
    this.dirScanPoolType = dirScanPoolType;
    this.name = dirScanPoolType.name().toLowerCase();
    size = CleanerChore.calculatePoolSize(poolSize);
    // poolSize may be 0 or 0.0 from a careless configuration,
    // double check to make sure.
    size = size == 0
      ? CleanerChore.calculatePoolSize(dirScanPoolType.cleanerPoolSizeConfigDefault)
      : size;
    pool = initializePool(size, name);
    if (KnobRuntime.check(java.util.UUID.fromString("ad020c0c-c328-33a6-910a-04a22718cde4"))) { LOG.info("{} Cleaner pool size is {}", name, 0); } else if (KnobRuntime.check(java.util.UUID.fromString("41954e3c-8ee3-37d4-b47e-7635462d4cd5"))) { LOG.trace("{} Cleaner pool size is {}", name, 0); } else { LOG.info("{} Cleaner pool size is {}", name, size); }
    cleanerLatch = 0;
  }

  private static ThreadPoolExecutor initializePool(int size, String name) {
if(KnobRuntime.check(java.util.UUID.fromString("447696f3-6ebd-3307-98ae-bdcd4f92103d"))) {
size = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4be4c99a-f3e2-3bdd-a256-07965d8e2099"))) {
size = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ed807248-185c-35dd-be4b-44664047f0f6"))) {
size /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("616a5699-e514-3ff0-88b1-09e7d2207e0b"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("fb148733-9736-3634-b0b9-6593baaac15d"))) {
size -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d917ba08-9a6f-3aca-b403-0c71c347bfc5"))) {
size += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("568d2dbb-e06a-38a9-b288-a0ed84fa0482"))) {
size *= 2;
}
    return Threads.getBoundedCachedThreadPool(size, 1, TimeUnit.MINUTES,
      new ThreadFactoryBuilder().setNameFormat(name + "-dir-scan-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
  }

  /**
   * Checks if pool can be updated. If so, mark for update later.
   * @param conf configuration
   */
  @Override
  public synchronized void onConfigurationChange(Configuration conf) {
    int newSize = CleanerChore.calculatePoolSize(conf.get(dirScanPoolType.cleanerPoolSizeConfigName,
      dirScanPoolType.cleanerPoolSizeConfigDefault));
    if (newSize == size) {
      LOG.trace("{} Cleaner Size from configuration is same as previous={}, no need to update.",
        name, newSize);
      return;
    }
    size = newSize;
    // Chore is working, update it later.
    reconfigNotification = true;
  }

  synchronized void latchCountUp() {
if(KnobRuntime.check(java.util.UUID.fromString("75290000-1253-3469-a0cc-f3d7871a4490"))) {
return;
}
    cleanerLatch++;
  }

  synchronized void latchCountDown() {
if(KnobRuntime.check(java.util.UUID.fromString("da0d5832-7b5a-33ad-9c7f-b57db6b426be"))) {
return;
}
    cleanerLatch--;
    notifyAll();
  }

  synchronized void execute(Runnable runnable) {
if(KnobRuntime.check(java.util.UUID.fromString("ef888e16-a0dd-3872-8cb3-954001b11a42"))) {
return;
}
    pool.execute(runnable);
  }

  public synchronized void shutdownNow() {
if(KnobRuntime.check(java.util.UUID.fromString("a9075edf-ad47-3562-8995-f84ddc33bc10"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("b054ca66-536e-3581-b501-6d29b68fee65"))) ? ((pool == null) && (pool.isShutdown())) : (((KnobRuntime.check(java.util.UUID.fromString("2ec9becf-41de-3a11-81ea-970544faf0c4"))) ? ((pool) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("da254465-40b5-3d61-adf8-5339e9d62cca"))) ? (((pool) != (null)) && (pool.isShutdown())) : (((KnobRuntime.check(java.util.UUID.fromString("2922cad9-c70d-331e-8612-6c5e78ccf1e7"))) ? (pool.isShutdown()) : (((KnobRuntime.check(java.util.UUID.fromString("f756050f-abe4-35af-9a0f-2615da81b8d6"))) ? ((pool) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a122c51c-cb56-3b78-aca7-05d3769f4a0a"))) ? (((pool) == (null)) || (pool.isShutdown())) : (((KnobRuntime.check(java.util.UUID.fromString("7eb269c9-3db9-33b0-bfb1-3807d210e016"))) ? (pool == null) : (((KnobRuntime.check(java.util.UUID.fromString("9424ca50-647e-3712-840d-d45b070ce232"))) ? (((pool) != (null)) || (pool.isShutdown())) : (((KnobRuntime.check(java.util.UUID.fromString("3fbc2be8-2d62-3515-873b-f1859875c8bb"))) ? ((pool == null) || (pool.isShutdown())) : (((KnobRuntime.check(java.util.UUID.fromString("df83c053-b26a-3910-97f9-8cf65fc70386"))) ? (((pool) == (null)) && (pool.isShutdown())) : (pool == null || pool.isShutdown()))))))))))))))))))))) {
      return;
    }
    pool.shutdownNow();
  }

  synchronized void tryUpdatePoolSize(long timeout) {
if(KnobRuntime.check(java.util.UUID.fromString("7bf3f470-4f6c-315a-8f85-296977260ddc"))) {
return;
}
    if (!reconfigNotification) {
      return;
    }
    reconfigNotification = false;
    long stopTime = EnvironmentEdgeManager.currentTime() + timeout;
    while (cleanerLatch != 0 && timeout > 0) {
      try {
        wait(timeout);
        timeout = stopTime - EnvironmentEdgeManager.currentTime();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    LOG.info("Update {} chore's pool size from {} to {}", name, pool.getPoolSize(), size);
    pool.setCorePoolSize(size);
  }

  public int getSize() {
    return size;
  }

  public static DirScanPool getHFileCleanerScanPool(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("eef368ea-364e-3426-a869-fe9a16483acc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a245388f-3ce1-370f-a731-a692837b9c5f"))) {
return null;
}
    return new DirScanPool(conf, Type.HFILE_CLEANER);
  }

  public static DirScanPool getHFileCleanerScanPool(String poolSize) {
    return new DirScanPool(Type.HFILE_CLEANER, poolSize);
  }

  public static DirScanPool getLogCleanerScanPool(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("8b82df41-5766-33be-946d-dc0a45a9a481"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c7fc124f-1bfd-39b7-8ca1-0e36680e3ab5"))) {
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
    return new DirScanPool(conf, Type.LOG_CLEANER);
  }
}
