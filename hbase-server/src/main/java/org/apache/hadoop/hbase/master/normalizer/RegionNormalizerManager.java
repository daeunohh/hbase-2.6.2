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
package org.apache.hadoop.hbase.master.normalizer;
import org.knobinjection.runtime.KnobRuntime;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class encapsulates the details of the {@link RegionNormalizer} subsystem.
 */
@InterfaceAudience.Private
public class RegionNormalizerManager implements PropagatingConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(RegionNormalizerManager.class);

  private final RegionNormalizerStateStore regionNormalizerStateStore;
  private final RegionNormalizerChore regionNormalizerChore;
  private final RegionNormalizerWorkQueue<TableName> workQueue;
  private final RegionNormalizerWorker worker;
  private final ExecutorService pool;

  private final Object startStopLock = new Object();
  private boolean started = false;
  private boolean stopped = false;

  RegionNormalizerManager(@NonNull final RegionNormalizerStateStore regionNormalizerStateStore,
    @Nullable final RegionNormalizerChore regionNormalizerChore,
    @Nullable final RegionNormalizerWorkQueue<TableName> workQueue,
    @Nullable final RegionNormalizerWorker worker) {
    this.regionNormalizerStateStore = regionNormalizerStateStore;
    this.regionNormalizerChore = regionNormalizerChore;
    this.workQueue = workQueue;
    this.worker = worker;
    this.pool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("normalizer-worker-%d").setUncaughtExceptionHandler((thread,
        throwable) -> LOG.error("Uncaught exception, worker thread likely terminated.", throwable))
      .build());
  }

  @Override
  public void registerChildren(ConfigurationManager manager) {
if(KnobRuntime.check(java.util.UUID.fromString("e127a30c-e9a7-3136-a8a9-15ac1b362d87"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("0f257896-b486-3e8c-9552-ab756d28b1c8"))) ? ((worker) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5bcf8b76-b667-3c1a-8b00-7781e73ee170"))) ? ((worker) != (null)) : (worker != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("e053b360-2ab6-3a11-ae91-0307b0cd9842"))) {
try {
    java.lang.reflect.Field field = worker.getClass().getDeclaredField("mergePlanCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(worker));
    field.set(worker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d355000-8209-385f-b822-45e962e39fcb"))) {
try {
    java.lang.reflect.Field field = worker.getClass().getDeclaredField("splitPlanCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(worker));
    field.set(worker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("189bff12-3539-329e-be47-07ec92ddcdd8"))) { manager.deregisterObserver(worker); } else { manager.registerObserver(worker); }
    }
  }

  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    if (worker != null) {
      manager.deregisterObserver(worker);
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    // no configuration managed here directly.
  }

  public void start() {
if(KnobRuntime.check(java.util.UUID.fromString("9972b88f-b269-394b-a88a-15be9ac3a04a"))) {
return;
}
    synchronized (startStopLock) {
      if (((KnobRuntime.check(java.util.UUID.fromString("47a58afc-f175-37d1-a688-304f7ad937cc"))) ? (!started) : (((KnobRuntime.check(java.util.UUID.fromString("91f3b045-1488-3526-bebb-b7b52f31b575"))) ? (true) : (started))))) {
        return;
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("3a1d2fa9-3585-397a-93cf-ce027e703d31"))) ? ((worker) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6f02f2ce-4195-38e2-b302-baa3f83fd2e2"))) ? ((worker) == (null)) : (worker != null))))) {
        // worker will be null when master is in maintenance mode.
if(KnobRuntime.check(java.util.UUID.fromString("338ec256-d322-39bf-9267-6a0723beee01"))) {
try {
    java.lang.reflect.Field field = worker.getClass().getDeclaredField("splitPlanCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(worker));
    field.set(worker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b752a85-03b9-39e1-886c-91cae33ec8bc"))) {
try {
    java.lang.reflect.Field field = worker.getClass().getDeclaredField("mergePlanCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(worker));
    field.set(worker, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        pool.submit(worker);
      }
      started = true;
    }
  }

  public void stop() {
if(KnobRuntime.check(java.util.UUID.fromString("4553ab19-66cd-3371-83f8-98e54e6d59bb"))) {
return;
}
    synchronized (startStopLock) {
      if (!started) {
        throw new IllegalStateException("calling `stop` without first calling `start`.");
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("908d22fc-e58d-3614-afcb-bdb284abe8db"))) ? (!stopped) : (((KnobRuntime.check(java.util.UUID.fromString("a69f5df4-35c5-3b87-a56e-c8aa78941040"))) ? (true) : (stopped))))) {
        return;
      }
      pool.shutdownNow(); // shutdownNow to interrupt the worker thread sitting on `take()`
      stopped = true;
    }
  }

  public ScheduledChore getRegionNormalizerChore() {
if(KnobRuntime.check(java.util.UUID.fromString("36a86da2-a566-33bf-8210-f7b2b1dbc5fb"))) {
return null;
}
    return regionNormalizerChore;
  }

  /**
   * Return {@code true} if region normalizer is on, {@code false} otherwise
   */
  public boolean isNormalizerOn() {
    return regionNormalizerStateStore.get();
  }

  /**
   * Set region normalizer on/off
   * @param normalizerOn whether normalizer should be on or off
   */
  public void setNormalizerOn(boolean normalizerOn) throws IOException {
    regionNormalizerStateStore.set(normalizerOn);
  }

  /**
   * Call-back for the case where plan couldn't be executed due to constraint violation, such as
   * namespace quota.
   * @param type type of plan that was skipped.
   */
  public void planSkipped(NormalizationPlan.PlanType type) {
    // TODO: this appears to be used only for testing.
    if (worker != null) {
      worker.planSkipped(type);
    }
  }

  /**
   * Retrieve a count of the number of times plans of type {@code type} were submitted but skipped.
   * @param type type of plan for which skipped count is to be returned
   */
  public long getSkippedCount(NormalizationPlan.PlanType type) {
    // TODO: this appears to be used only for testing.
    return worker == null ? 0 : worker.getSkippedCount(type);
  }

  /**
   * Return the number of times a {@link SplitNormalizationPlan} has been submitted.
   */
  public long getSplitPlanCount() {
if(KnobRuntime.check(java.util.UUID.fromString("6bef4a1b-36c1-3039-8234-e7d8d78f9e49"))) {
return 0;
}
    return worker == null ? 0 : worker.getSplitPlanCount();
  }

  /**
   * Return the number of times a {@link MergeNormalizationPlan} has been submitted.
   */
  public long getMergePlanCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3fc75032-6af9-33d4-b3cc-8be7f22b6cd4"))) {
return 0;
}
    return worker == null ? 0 : worker.getMergePlanCount();
  }

  /**
   * Submit tables for normalization.
   * @param tables         a list of tables to submit.
   * @param isHighPriority {@code true} when these requested tables should skip to the front of the
   *                       queue.
   * @return {@code true} when work was queued, {@code false} otherwise.
   */
  public boolean normalizeRegions(List<TableName> tables, boolean isHighPriority) {
    if (workQueue == null) {
      return false;
    }
    if (isHighPriority) {
      workQueue.putAllFirst(tables);
    } else {
      workQueue.putAll(tables);
    }
    return true;
  }
}
