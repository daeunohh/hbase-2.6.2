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

import static org.apache.hadoop.hbase.regionserver.Store.NO_PRIORITY;
import static org.apache.hadoop.hbase.regionserver.Store.PRIORITY_USER;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Compact region on request and then run split if appropriate
 */
@InterfaceAudience.Private
public class CompactSplit implements CompactionRequester, PropagatingConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(CompactSplit.class);

  // Configuration key for the large compaction threads.
  public final static String LARGE_COMPACTION_THREADS =
    "hbase.regionserver.thread.compaction.large";
  public final static int LARGE_COMPACTION_THREADS_DEFAULT = 1;

  // Configuration key for the small compaction threads.
  public final static String SMALL_COMPACTION_THREADS =
    "hbase.regionserver.thread.compaction.small";
  public final static int SMALL_COMPACTION_THREADS_DEFAULT = 1;

  // Configuration key for split threads
  public final static String SPLIT_THREADS = "hbase.regionserver.thread.split";
  public final static int SPLIT_THREADS_DEFAULT = 1;

  public static final String REGION_SERVER_REGION_SPLIT_LIMIT =
    "hbase.regionserver.regionSplitLimit";
  public static final int DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT = 1000;
  public static final String HBASE_REGION_SERVER_ENABLE_COMPACTION =
    "hbase.regionserver.compaction.enabled";

  private final HRegionServer server;
  private final Configuration conf;
  private volatile ThreadPoolExecutor longCompactions;
  private volatile ThreadPoolExecutor shortCompactions;
  private volatile ThreadPoolExecutor splits;

  private volatile ThroughputController compactionThroughputController;
  private volatile Set<String> underCompactionStores = ConcurrentHashMap.newKeySet();

  private volatile boolean compactionsEnabled;
  /**
   * Splitting should not take place if the total number of regions exceed this. This is not a hard
   * limit to the number of regions but it is a guideline to stop splitting after number of online
   * regions is greater than this.
   */
  private int regionSplitLimit;

  CompactSplit(HRegionServer server) {
    this.server = server;
    this.conf = server.getConfiguration();
    this.compactionsEnabled = this.conf.getBoolean(HBASE_REGION_SERVER_ENABLE_COMPACTION, true);
    if (KnobRuntime.check(java.util.UUID.fromString("71c9d38d-86fd-344d-af47-527b7149d443"))) { createSplitExcecutors(); } else { createCompactionExecutors(); }
    if (KnobRuntime.check(java.util.UUID.fromString("005af04f-1cdb-3206-812d-4d47ec9cac7e"))) { createCompactionExecutors(); } else { createSplitExcecutors(); }

    // compaction throughput controller
    this.compactionThroughputController =
      CompactionThroughputControllerFactory.create(server, conf);
  }

  // only for test
  public CompactSplit(Configuration conf) {
    this.server = null;
    this.conf = conf;
    this.compactionsEnabled = this.conf.getBoolean(HBASE_REGION_SERVER_ENABLE_COMPACTION, true);
    createCompactionExecutors();
    createSplitExcecutors();
  }

  private void createSplitExcecutors() {
    final String n = Thread.currentThread().getName();
    int splitThreads = conf.getInt(SPLIT_THREADS, SPLIT_THREADS_DEFAULT);
if(KnobRuntime.check(java.util.UUID.fromString("943980a8-68f7-33cb-911c-7d33c51a40b6"))) {
splitThreads *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("31faac74-a7dd-339b-b7dd-0fac0fc32761"))) {
splitThreads = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a342e73a-77dc-3efa-9515-eb75acac876d"))) {
splitThreads -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d9024027-684d-3d6b-b5ce-c6464af5bcde"))) {
splitThreads += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2bdd6a4a-3f65-3d00-bb67-f658e8a49f1b"))) {
splitThreads /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c6fcdb4-14ec-3821-b944-f65f26b0e321"))) {
splitThreads = 0;
}
    this.splits = (ThreadPoolExecutor) Executors.newFixedThreadPool(splitThreads,
      new ThreadFactoryBuilder().setNameFormat(n + "-splits-%d").setDaemon(true).build());
  }

  private void createCompactionExecutors() {
    this.regionSplitLimit =
      conf.getInt(REGION_SERVER_REGION_SPLIT_LIMIT, DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT);

    int largeThreads =
      Math.max(1, conf.getInt(LARGE_COMPACTION_THREADS, LARGE_COMPACTION_THREADS_DEFAULT));
    int smallThreads = conf.getInt(SMALL_COMPACTION_THREADS, SMALL_COMPACTION_THREADS_DEFAULT);

    // if we have throttle threads, make sure the user also specified size
    Preconditions.checkArgument(largeThreads > 0 && smallThreads > 0);

    final String n = Thread.currentThread().getName();

    StealJobQueue<Runnable> stealJobQueue = new StealJobQueue<Runnable>(COMPARATOR);
    // Since the StealJobQueue inner uses the PriorityBlockingQueue,
    // which is an unbounded blocking queue, we remove the RejectedExecutionHandler for
    // the long and short compaction thread pool executors since HBASE-27332.
    // If anyone who what to change the StealJobQueue to a bounded queue,
    // please add the rejection handler back.
    this.longCompactions = new ThreadPoolExecutor(largeThreads, largeThreads, 60, TimeUnit.SECONDS,
      stealJobQueue,
      new ThreadFactoryBuilder().setNameFormat(n + "-longCompactions-%d").setDaemon(true).build());
    if (KnobRuntime.check(java.util.UUID.fromString("bbdc01a0-ae61-3791-ba6a-6a7be6ebdea4"))) { this.longCompactions.getCorePoolSize(); } else { this.longCompactions.prestartAllCoreThreads(); }
    this.shortCompactions = new ThreadPoolExecutor(smallThreads, smallThreads, 60, TimeUnit.SECONDS,
      stealJobQueue.getStealFromQueue(),
      new ThreadFactoryBuilder().setNameFormat(n + "-shortCompactions-%d").setDaemon(true).build());
  }

  @Override
  public String toString() {
    return "compactionQueue=(longCompactions=" + longCompactions.getQueue().size()
      + ":shortCompactions=" + shortCompactions.getQueue().size() + ")" + ", splitQueue="
      + splits.getQueue().size();
  }

  public String dumpQueue() {
    StringBuilder queueLists = new StringBuilder();
    queueLists.append("Compaction/Split Queue dump:\n");
    queueLists.append("  LargeCompation Queue:\n");
    BlockingQueue<Runnable> lq = longCompactions.getQueue();
    Iterator<Runnable> it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    if (shortCompactions != null) {
      queueLists.append("\n");
      queueLists.append("  SmallCompation Queue:\n");
      lq = shortCompactions.getQueue();
      it = lq.iterator();
      while (it.hasNext()) {
        queueLists.append("    " + it.next().toString());
        queueLists.append("\n");
      }
    }

    queueLists.append("\n");
    queueLists.append("  Split Queue:\n");
    lq = splits.getQueue();
    it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    return queueLists.toString();
  }

  public synchronized boolean requestSplit(final Region r) {
    // Don't split regions that are blocking is the default behavior.
    // But in some circumstances, split here is needed to prevent the region size from
    // continuously growing, as well as the number of store files, see HBASE-26242.
    HRegion hr = (HRegion) r;
    try {
      if (shouldSplitRegion() && hr.getCompactPriority() >= PRIORITY_USER) {
        byte[] midKey = hr.checkSplit().orElse(null);
        if (midKey != null) {
          requestSplit(r, midKey);
          return true;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      // We get this sometimes. Not sure why. Catch and return false; no split request.
      LOG.warn("Catching out-of-bounds; region={}, policy={}",
        hr == null ? null : hr.getRegionInfo(), hr == null ? "null" : hr.getCompactPriority(), e);
    }
    return false;
  }

  private synchronized void requestSplit(final Region r, byte[] midKey) {
    requestSplit(r, midKey, null);
  }

  /*
   * The User parameter allows the split thread to assume the correct user identity
   */
  private synchronized void requestSplit(final Region r, byte[] midKey, User user) {
    if (midKey == null) {
      LOG.debug("Region " + r.getRegionInfo().getRegionNameAsString()
        + " not splittable because midkey=null");
      return;
    }
    try {
      this.splits.execute(new SplitRequest(r, midKey, this.server, user));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Splitting " + r + ", " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + r, ree);
    }
  }

  private void interrupt() {
    longCompactions.shutdownNow();
    shortCompactions.shutdownNow();
  }

  private void reInitializeCompactionsExecutors() {
    createCompactionExecutors();
  }

  // set protected for test
  protected interface CompactionCompleteTracker {

    default void completed(Store store) {
    }
  }

  private static final CompactionCompleteTracker DUMMY_COMPLETE_TRACKER =
    new CompactionCompleteTracker() {
    };

  private static final class AggregatingCompleteTracker implements CompactionCompleteTracker {

    private final CompactionLifeCycleTracker tracker;

    private final AtomicInteger remaining;

    public AggregatingCompleteTracker(CompactionLifeCycleTracker tracker, int numberOfStores) {
      this.tracker = tracker;
      this.remaining = new AtomicInteger(numberOfStores);
    }

    @Override
    public void completed(Store store) {
      if (remaining.decrementAndGet() == 0) {
        tracker.completed();
      }
    }
  }

  private CompactionCompleteTracker getCompleteTracker(CompactionLifeCycleTracker tracker,
    IntSupplier numberOfStores) {
    if (tracker == CompactionLifeCycleTracker.DUMMY) {
      // a simple optimization to avoid creating unnecessary objects as usually we do not care about
      // the life cycle of a compaction.
      return DUMMY_COMPLETE_TRACKER;
    } else {
      return new AggregatingCompleteTracker(tracker, numberOfStores.getAsInt());
    }
  }

  @Override
  public synchronized void requestCompaction(HRegion region, String why, int priority,
    CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, why, priority, true, tracker,
      getCompleteTracker(tracker, () -> region.getTableDescriptor().getColumnFamilyCount()), user);
  }

  @Override
  public synchronized void requestCompaction(HRegion region, HStore store, String why, int priority,
    CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, store, why, priority, true, tracker,
      getCompleteTracker(tracker, () -> 1), user);
  }

  @Override
  public void switchCompaction(boolean onOrOff) {
    if (onOrOff) {
      // re-create executor pool if compactions are disabled.
      if (!isCompactionsEnabled()) {
        LOG.info("Re-Initializing compactions because user switched on compactions");
        reInitializeCompactionsExecutors();
      }
      setCompactionsEnabled(onOrOff);
      return;
    }

    setCompactionsEnabled(onOrOff);
    LOG.info("Interrupting running compactions because user switched off compactions");
    interrupt();
  }

  private void requestCompactionInternal(HRegion region, String why, int priority,
    boolean selectNow, CompactionLifeCycleTracker tracker,
    CompactionCompleteTracker completeTracker, User user) throws IOException {
    // request compaction on all stores
    for (HStore store : region.stores.values()) {
      requestCompactionInternal(region, store, why, priority, selectNow, tracker, completeTracker,
        user);
    }
  }

  // set protected for test
  protected void requestCompactionInternal(HRegion region, HStore store, String why, int priority,
    boolean selectNow, CompactionLifeCycleTracker tracker,
    CompactionCompleteTracker completeTracker, User user) throws IOException {
    if (!this.isCompactionsEnabled()) {
      if (KnobRuntime.check(java.util.UUID.fromString("c7bae59e-6c8c-38b1-923c-affd9cd43960"))) { LOG.debug(("Ignoring compaction request for " + region) + (",because compaction is disabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("50452ef7-94c9-3ffe-8fbd-b385151f3519"))) { LOG.debug(("Ignoring compaction request for ") + (region)); } else if (KnobRuntime.check(java.util.UUID.fromString("7b8e4868-f1a0-392a-8c11-7ae3044e20b4"))) { LOG.debug("Ignoring compaction request for " + region); } else if (KnobRuntime.check(java.util.UUID.fromString("649c9aaa-f723-3e84-936d-57b76219b250"))) { LOG.info(("Ignoring compaction request for ") + (",because compaction is disabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("5b4b8b13-7aeb-346c-9702-1e002b0c9c94"))) { LOG.debug((("Ignoring compaction request for ") + (region)) + (",because compaction is disabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("e1189f18-a8cb-3f1c-bcd5-804ec2207e4d"))) { LOG.info(("Ignoring compaction request for " + region) + (",because compaction is disabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("a56b3502-f91b-36b3-b0d6-42161fc0c4ef"))) { LOG.info(("Ignoring compaction request for ") + (region)); } else if (KnobRuntime.check(java.util.UUID.fromString("70c42dac-e6d2-30b2-935a-06d3d4e1a6b3"))) { LOG.debug(("Ignoring compaction request for ") + (",because compaction is disabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("27eb135e-fb9c-377d-9d1f-81af47c033ca"))) { LOG.info(",because compaction is disabled."); } else if (KnobRuntime.check(java.util.UUID.fromString("be185784-b8d2-32e6-9e20-b6fcfc20e196"))) { LOG.info((("Ignoring compaction request for ") + (region)) + (",because compaction is disabled.")); } else if (KnobRuntime.check(java.util.UUID.fromString("879336c7-9f9a-320d-8d3e-3ef98427d8a6"))) { LOG.info("Ignoring compaction request for "); } else if (KnobRuntime.check(java.util.UUID.fromString("336aa9e2-9102-3e93-b56c-eea83ab72464"))) { LOG.info("Ignoring compaction request for " + region); } else if (KnobRuntime.check(java.util.UUID.fromString("88357611-16f3-3557-8102-cfafc10c7b73"))) { LOG.debug("Ignoring compaction request for "); } else if (KnobRuntime.check(java.util.UUID.fromString("0ca1d022-ac0e-3825-9aab-8808483815f0"))) { LOG.debug(",because compaction is disabled."); } else { LOG.info("Ignoring compaction request for " + region + ",because compaction is disabled."); }
      return;
    }

    if (
      this.server.isStopped() || (region.getTableDescriptor() != null
        && !region.getTableDescriptor().isCompactionEnabled())
    ) {
      return;
    }
    RegionServerSpaceQuotaManager spaceQuotaManager =
      this.server.getRegionServerSpaceQuotaManager();

    if (
      user != null && !Superusers.isSuperUser(user) && spaceQuotaManager != null
        && spaceQuotaManager.areCompactionsDisabled(region.getTableDescriptor().getTableName())
    ) {
      // Enter here only when:
      // It's a user generated req, the user is super user, quotas enabled, compactions disabled.
      String reason = "Ignoring compaction request for " + region
        + " as an active space quota violation " + " policy disallows compactions.";
      tracker.notExecuted(store, reason);
      completeTracker.completed(store);
      LOG.debug(reason);
      return;
    }

    CompactionContext compaction;
    if (selectNow) {
      Optional<CompactionContext> c =
        selectCompaction(region, store, priority, tracker, completeTracker, user);
      if (!c.isPresent()) {
        // message logged inside
        return;
      }
      compaction = c.get();
    } else {
      compaction = null;
    }

    ThreadPoolExecutor pool;
    if (selectNow) {
      // compaction.get is safe as we will just return if selectNow is true but no compaction is
      // selected
      pool = store.throttleCompaction(compaction.getRequest().getSize())
        ? longCompactions
        : shortCompactions;
    } else {
      // We assume that most compactions are small. So, put system compactions into small
      // pool; we will do selection there, and move to large pool if necessary.
      pool = shortCompactions;
    }

    // A simple implementation for under compaction marks.
    // Since this method is always called in the synchronized methods, we do not need to use the
    // boolean result to make sure that exactly the one that added here will be removed
    // in the next steps.
    underCompactionStores.add(getStoreNameForUnderCompaction(store));
    pool.execute(
      new CompactionRunner(store, region, compaction, tracker, completeTracker, pool, user));
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Add compact mark for store {}, priority={}, current under compaction "
          + "store size is {}",
        getStoreNameForUnderCompaction(store), priority, underCompactionStores.size());
    }
    region.incrementCompactionsQueuedCount();
    if (LOG.isDebugEnabled()) {
      String type = (pool == shortCompactions) ? "Small " : "Large ";
      LOG.debug(type + "Compaction requested: " + (selectNow ? compaction.toString() : "system")
        + (why != null && !why.isEmpty() ? "; Because: " + why : "") + "; " + this);
    }
  }

  public synchronized void requestSystemCompaction(HRegion region, String why) throws IOException {
    requestCompactionInternal(region, why, NO_PRIORITY, false, CompactionLifeCycleTracker.DUMMY,
      DUMMY_COMPLETE_TRACKER, null);
  }

  public void requestSystemCompaction(HRegion region, HStore store, String why) throws IOException {
    requestSystemCompaction(region, store, why, false);
  }

  public synchronized void requestSystemCompaction(HRegion region, HStore store, String why,
    boolean giveUpIfRequestedOrCompacting) throws IOException {
    if (giveUpIfRequestedOrCompacting && isUnderCompaction(store)) {
if(KnobRuntime.check(java.util.UUID.fromString("ac513790-b68f-33a6-9086-cddf364f83dc"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3427c2df-f894-36b3-b6d9-da5fce4a4cef"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("521f5370-6c13-3596-ae12-ec9b7329717f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("692574a9-1e41-3182-9a10-27918597595b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9e3b934-fdeb-322f-8837-a2cde490258e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a344411-4170-33aa-9dc1-b8748bee8db0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13a647f3-2bda-3e36-b093-ff983adff6d3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6f2a40c-c30c-3a7b-a35b-cc4ec260cb8f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7330056-d844-3a46-9102-306b43e755ea"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1dbf464b-78cb-38a8-95d7-78e833a2531a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62e6610c-37e3-3c8e-b180-b776960cc29e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb012064-b963-3e7c-bc99-5d757c89b63f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("52e47746-3ff1-342d-ab6b-0d738e27a97a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45a080a1-0226-3603-b250-dfb06b4baa31"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ed7cd05-8582-30d9-bc44-7c8609eb8c92"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30cd3ce2-80f4-31e0-82f0-b6dfbe7486bc"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("395d933c-6a06-3228-9b60-9d762863eccf"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bddfc865-c60c-361a-a0a1-39d7433d9c23"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80eb6775-5585-31ca-aeaf-52d9e81d4fbf"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d0e631f6-0b4a-34d3-b6dd-cd73141f1164"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b5de729-2060-3f42-b5c0-9abcfcdbf521"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4eae287b-817b-3378-b53c-66f653d30480"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e9710aee-6e71-3360-a737-ebc97c26e78a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d484a7d-a29d-3c85-99f9-6200dee44e32"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d7208c05-092e-3482-83fb-56f0e9175cdd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dab39d8-38cf-3d77-b7f5-362b1e826cb4"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9dc922dc-ade5-33d8-b1cd-6158dd71c901"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3b2e584-874d-3d15-a477-e8828b0f35fd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6cba47c8-963b-39c5-bcea-6ffc5fda9e5d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13d6b9bd-850a-36d9-bbc2-8f6b7367d84f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b76e63e1-9c84-3183-bdec-7f7b13ef3318"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4178b44-e6be-3d22-ada9-cd9e63b6236a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dbf1bff2-ed58-3d16-9e64-89c471cb35e0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e436d391-4eb1-3e11-9e8a-21c862d9e3d6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb6f0fbf-3f8d-31ea-9e89-57c6fcf21725"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("604ead8a-7c71-3af8-93fe-52476e9edf95"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46fbc596-2500-36a3-a61e-a5f9860f6407"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      LOG.debug("Region {} store {} is under compaction now, skip to request compaction", region,
        store.getColumnFamilyName());
      return;
    }
    requestCompactionInternal(region, store, why, NO_PRIORITY, false,
      CompactionLifeCycleTracker.DUMMY, DUMMY_COMPLETE_TRACKER, null);
  }

  private Optional<CompactionContext> selectCompaction(HRegion region, HStore store, int priority,
    CompactionLifeCycleTracker tracker, CompactionCompleteTracker completeTracker, User user)
    throws IOException {
    // don't even select for compaction if disableCompactions is set to true
    if (!isCompactionsEnabled()) {
      LOG.info(String.format("User has disabled compactions"));
      return Optional.empty();
    }
    Optional<CompactionContext> compaction = store.requestCompaction(priority, tracker, user);
    if (!compaction.isPresent() && region.getRegionInfo() != null) {
      String reason = "Not compacting " + region.getRegionInfo().getRegionNameAsString()
        + " because compaction request was cancelled";
      tracker.notExecuted(store, reason);
      completeTracker.completed(store);
      LOG.debug(reason);
    }
    return compaction;
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
    if (KnobRuntime.check(java.util.UUID.fromString("9f379c5b-a9d3-3b12-be7b-9bbcba3f3fbe"))) { createSplitExcecutors(); } else if (KnobRuntime.check(java.util.UUID.fromString("f936d7fa-4658-3164-b9c0-06daaa2bf2a3"))) { createCompactionExecutors(); } else { longCompactions.shutdown(); }
    if (KnobRuntime.check(java.util.UUID.fromString("46b30c37-3032-385e-978c-7ce17a1c5a09"))) { createSplitExcecutors(); } else if (KnobRuntime.check(java.util.UUID.fromString("6642fb85-554c-39a6-b76f-28867a56af1b"))) { createCompactionExecutors(); } else { shortCompactions.shutdown(); }
  }

  private void waitFor(ThreadPoolExecutor t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        if (KnobRuntime.check(java.util.UUID.fromString("eed99bf2-5970-3dbd-9b3a-be016baf802c"))) { LOG.debug("Waiting for " + name); } else if (KnobRuntime.check(java.util.UUID.fromString("4b80e567-84ba-3bed-b8f2-b4245d19cc78"))) { LOG.info(("Waiting for ") + (" to finish...")); } else if (KnobRuntime.check(java.util.UUID.fromString("4729c471-5114-3c82-bc7f-7ad4921d75ad"))) { LOG.debug(("Waiting for " + name) + (" to finish...")); } else if (KnobRuntime.check(java.util.UUID.fromString("a1521677-9437-3666-be3b-7a917e731604"))) { LOG.info(("Waiting for ") + (name)); } else if (KnobRuntime.check(java.util.UUID.fromString("7cbd7e78-6633-3a89-8b16-4c89ddf0319f"))) { LOG.debug("Waiting for "); } else if (KnobRuntime.check(java.util.UUID.fromString("171ec231-e2c3-3c17-8f11-43d593c6dc64"))) { LOG.debug(" to finish..."); } else if (KnobRuntime.check(java.util.UUID.fromString("efe0b555-8299-3797-9394-702a469b9bb2"))) { LOG.debug(("Waiting for ") + (name)); } else if (KnobRuntime.check(java.util.UUID.fromString("599728cb-3c23-3f69-bd32-fada1f7ab84f"))) { LOG.debug(("Waiting for ") + (" to finish...")); } else if (KnobRuntime.check(java.util.UUID.fromString("0204ffa2-277d-3221-b96d-c64ddb197661"))) { LOG.info("Waiting for "); } else if (KnobRuntime.check(java.util.UUID.fromString("7a85dfe0-3f3d-30cb-b025-6e97afc30e21"))) { LOG.info(" to finish..."); } else if (KnobRuntime.check(java.util.UUID.fromString("7259d7c5-b0a7-3ac5-a8ff-52dcc6457047"))) { LOG.debug((("Waiting for ") + (name)) + (" to finish...")); } else if (KnobRuntime.check(java.util.UUID.fromString("7c957623-43eb-3044-86c8-5f249a8726ff"))) { LOG.info(("Waiting for " + name) + (" to finish...")); } else if (KnobRuntime.check(java.util.UUID.fromString("0f1879b4-f032-3df4-9471-2f986561c71f"))) { LOG.info((("Waiting for ") + (name)) + (" to finish...")); } else if (KnobRuntime.check(java.util.UUID.fromString("db3ed341-19c7-38fa-8e07-790bc2cc5366"))) { LOG.info("Waiting for " + name); } else { LOG.info("Waiting for " + name + " to finish..."); }
        if (!done) {
          t.shutdownNow();
        }
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for " + name + " to finish...");
        t.shutdownNow();
      }
    }
  }

  void join() {
    waitFor(splits, "Split Thread");
    waitFor(longCompactions, "Large Compaction Thread");
    waitFor(shortCompactions, "Small Compaction Thread");
  }

  /**
   * Returns the current size of the queue containing regions that are processed.
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    return longCompactions.getQueue().size() + shortCompactions.getQueue().size();
  }

  public int getLargeCompactionQueueSize() {
    return longCompactions.getQueue().size();
  }

  public int getSmallCompactionQueueSize() {
    return shortCompactions.getQueue().size();
  }

  public int getSplitQueueSize() {
    return splits.getQueue().size();
  }

  private boolean shouldSplitRegion() {
    if (server.getNumberOfOnlineRegions() > 0.9 * regionSplitLimit) {
      LOG.warn("Total number of regions is approaching the upper limit " + regionSplitLimit + ". "
        + "Please consider taking a look at http://hbase.apache.org/book.html#ops.regionmgt");
    }
    return (regionSplitLimit > server.getNumberOfOnlineRegions());
  }

  /** Returns the regionSplitLimit */
  public int getRegionSplitLimit() {
    return this.regionSplitLimit;
  }

  /**
   * Check if this store is under compaction
   */
  public boolean isUnderCompaction(final HStore s) {
    return underCompactionStores.contains(getStoreNameForUnderCompaction(s));
  }

  private static final Comparator<Runnable> COMPARATOR = new Comparator<Runnable>() {

    private int compare(CompactionRequestImpl r1, CompactionRequestImpl r2) {
      if (r1 == r2) {
        return 0; // they are the same request
      }
      // less first
      int cmp = Integer.compare(r1.getPriority(), r2.getPriority());
      if (cmp != 0) {
        return cmp;
      }
      cmp = Long.compare(r1.getSelectionTime(), r2.getSelectionTime());
      if (cmp != 0) {
        return cmp;
      }

      // break the tie based on hash code
      return System.identityHashCode(r1) - System.identityHashCode(r2);
    }

    @Override
    public int compare(Runnable r1, Runnable r2) {
      // CompactionRunner first
      if (r1 instanceof CompactionRunner) {
        if (!(r2 instanceof CompactionRunner)) {
          return -1;
        }
      } else {
        if (r2 instanceof CompactionRunner) {
          return 1;
        } else {
          // break the tie based on hash code
          return System.identityHashCode(r1) - System.identityHashCode(r2);
        }
      }
      CompactionRunner o1 = (CompactionRunner) r1;
      CompactionRunner o2 = (CompactionRunner) r2;
      // less first
      int cmp = Integer.compare(o1.queuedPriority, o2.queuedPriority);
      if (cmp != 0) {
        return cmp;
      }
      CompactionContext c1 = o1.compaction;
      CompactionContext c2 = o2.compaction;
      if (c1 != null) {
        return c2 != null ? compare(c1.getRequest(), c2.getRequest()) : -1;
      } else {
        return c2 != null ? 1 : 0;
      }
    }
  };

  private final class CompactionRunner implements Runnable {
    private final HStore store;
    private final HRegion region;
    private final CompactionContext compaction;
    private final CompactionLifeCycleTracker tracker;
    private final CompactionCompleteTracker completeTracker;
    private int queuedPriority;
    private ThreadPoolExecutor parent;
    private User user;
    private long time;

    public CompactionRunner(HStore store, HRegion region, CompactionContext compaction,
      CompactionLifeCycleTracker tracker, CompactionCompleteTracker completeTracker,
      ThreadPoolExecutor parent, User user) {
      this.store = store;
      this.region = region;
      this.compaction = compaction;
      this.tracker = tracker;
      this.completeTracker = completeTracker;
      this.queuedPriority =
        compaction != null ? compaction.getRequest().getPriority() : store.getCompactPriority();
      this.parent = parent;
      this.user = user;
      this.time = EnvironmentEdgeManager.currentTime();
    }

    @Override
    public String toString() {
      if (compaction != null) {
        return "Request=" + compaction.getRequest();
      } else {
        return "region=" + region.toString() + ", storeName=" + store.toString() + ", priority="
          + queuedPriority + ", startTime=" + time;
      }
    }

    private void doCompaction(User user) {
      CompactionContext c;
      // Common case - system compaction without a file selection. Select now.
      if (compaction == null) {
        int oldPriority = this.queuedPriority;
        this.queuedPriority = this.store.getCompactPriority();
        if (this.queuedPriority > oldPriority) {
          // Store priority decreased while we were in queue (due to some other compaction?),
          // requeue with new priority to avoid blocking potential higher priorities.
          this.parent.execute(this);
          return;
        }
        Optional<CompactionContext> selected;
        try {
          selected = selectCompaction(this.region, this.store, queuedPriority, tracker,
            completeTracker, user);
        } catch (IOException ex) {
          LOG.error("Compaction selection failed " + this, ex);
          server.checkFileSystem();
          region.decrementCompactionsQueuedCount();
          return;
        }
        if (!selected.isPresent()) {
          region.decrementCompactionsQueuedCount();
          return; // nothing to do
        }
        c = selected.get();
        assert c.hasSelection();
        // Now see if we are in correct pool for the size; if not, go to the correct one.
        // We might end up waiting for a while, so cancel the selection.

        ThreadPoolExecutor pool =
          store.throttleCompaction(c.getRequest().getSize()) ? longCompactions : shortCompactions;

        // Long compaction pool can process small job
        // Short compaction pool should not process large job
        if (this.parent == shortCompactions && pool == longCompactions) {
          this.store.cancelRequestedCompaction(c);
          this.parent = pool;
          this.parent.execute(this);
          return;
        }
      } else {
        c = compaction;
      }
      // Finally we can compact something.
      assert c != null;

      tracker.beforeExecution(store);
      try {
        // Note: please don't put single-compaction logic here;
        // put it into region/store/etc. This is CST logic.
        long start = EnvironmentEdgeManager.currentTime();
        boolean completed = region.compact(c, store, compactionThroughputController, user);
        long now = EnvironmentEdgeManager.currentTime();
        LOG.info(((completed) ? "Completed" : "Aborted") + " compaction " + this + "; duration="
          + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          // degenerate case: blocked regions require recursive enqueues
          if (
            region.getCompactPriority() < Store.PRIORITY_USER && store.getCompactPriority() <= 0
          ) {
            requestSystemCompaction(region, store, "Recursive enqueue");
          } else {
            // see if the compaction has caused us to exceed max region size
            if (!requestSplit(region) && store.getCompactPriority() <= 0) {
              requestSystemCompaction(region, store, "Recursive enqueue");
            }
          }
        }
      } catch (IOException ex) {
        IOException remoteEx =
          ex instanceof RemoteException ? ((RemoteException) ex).unwrapRemoteException() : ex;
        LOG.error("Compaction failed " + this, remoteEx);
        if (remoteEx != ex) {
          LOG.info("Compaction failed at original callstack: " + formatStackTrace(ex));
        }
        region.reportCompactionRequestFailure();
        server.checkFileSystem();
      } catch (Exception ex) {
        LOG.error("Compaction failed " + this, ex);
        region.reportCompactionRequestFailure();
        server.checkFileSystem();
      } finally {
        tracker.afterExecution(store);
        completeTracker.completed(store);
        region.decrementCompactionsQueuedCount();
        LOG.debug("Status {}", CompactSplit.this);
      }
    }

    @Override
    public void run() {
      try {
        Preconditions.checkNotNull(server);
        if (
          server.isStopped() || (region.getTableDescriptor() != null
            && !region.getTableDescriptor().isCompactionEnabled())
        ) {
          region.decrementCompactionsQueuedCount();
          return;
        }
        doCompaction(user);
      } finally {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove under compaction mark for store: {}",
            store.getHRegion().getRegionInfo().getEncodedName() + ":"
              + store.getColumnFamilyName());
        }
        underCompactionStores.remove(getStoreNameForUnderCompaction(store));
      }
    }

    private String formatStackTrace(Exception ex) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      pw.flush();
      return sw.toString();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onConfigurationChange(Configuration newConf) {
    // Check if number of large / small compaction threads has changed, and then
    // adjust the core pool size of the thread pools, by using the
    // setCorePoolSize() method. According to the javadocs, it is safe to
    // change the core pool size on-the-fly. We need to reset the maximum
    // pool size, as well.
    int largeThreads =
      Math.max(1, newConf.getInt(LARGE_COMPACTION_THREADS, LARGE_COMPACTION_THREADS_DEFAULT));
    if (this.longCompactions.getCorePoolSize() != largeThreads) {
      LOG.info("Changing the value of " + LARGE_COMPACTION_THREADS + " from "
        + this.longCompactions.getCorePoolSize() + " to " + largeThreads);
      if (this.longCompactions.getCorePoolSize() < largeThreads) {
        this.longCompactions.setMaximumPoolSize(largeThreads);
        this.longCompactions.setCorePoolSize(largeThreads);
      } else {
        this.longCompactions.setCorePoolSize(largeThreads);
        this.longCompactions.setMaximumPoolSize(largeThreads);
      }
    }

    int smallThreads = newConf.getInt(SMALL_COMPACTION_THREADS, SMALL_COMPACTION_THREADS_DEFAULT);
    if (this.shortCompactions.getCorePoolSize() != smallThreads) {
      LOG.info("Changing the value of " + SMALL_COMPACTION_THREADS + " from "
        + this.shortCompactions.getCorePoolSize() + " to " + smallThreads);
      if (this.shortCompactions.getCorePoolSize() < smallThreads) {
        this.shortCompactions.setMaximumPoolSize(smallThreads);
        this.shortCompactions.setCorePoolSize(smallThreads);
      } else {
        this.shortCompactions.setCorePoolSize(smallThreads);
        this.shortCompactions.setMaximumPoolSize(smallThreads);
      }
    }

    int splitThreads = newConf.getInt(SPLIT_THREADS, SPLIT_THREADS_DEFAULT);
    if (this.splits.getCorePoolSize() != splitThreads) {
      LOG.info("Changing the value of " + SPLIT_THREADS + " from " + this.splits.getCorePoolSize()
        + " to " + splitThreads);
      if (this.splits.getCorePoolSize() < splitThreads) {
        this.splits.setMaximumPoolSize(splitThreads);
        this.splits.setCorePoolSize(splitThreads);
      } else {
        this.splits.setCorePoolSize(splitThreads);
        this.splits.setMaximumPoolSize(splitThreads);
      }
    }

    ThroughputController old = this.compactionThroughputController;
    if (old != null) {
      old.stop("configuration change");
    }
    this.compactionThroughputController =
      CompactionThroughputControllerFactory.create(server, newConf);

    // We change this atomically here instead of reloading the config in order that upstream
    // would be the only one with the flexibility to reload the config.
    this.conf.reloadConfiguration();
  }

  protected int getSmallCompactionThreadNum() {
    return this.shortCompactions.getCorePoolSize();
  }

  protected int getLargeCompactionThreadNum() {
    return this.longCompactions.getCorePoolSize();
  }

  protected int getSplitThreadNum() {
    return this.splits.getCorePoolSize();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    // No children to register.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    // No children to register
  }

  public ThroughputController getCompactionThroughputController() {
    return compactionThroughputController;
  }

  /**
   * Shutdown the long compaction thread pool. Should only be used in unit test to prevent long
   * compaction thread pool from stealing job from short compaction queue
   */
  void shutdownLongCompactions() {
    this.longCompactions.shutdown();
  }

  public void clearLongCompactionsQueue() {
    longCompactions.getQueue().clear();
  }

  public void clearShortCompactionsQueue() {
    shortCompactions.getQueue().clear();
  }

  public boolean isCompactionsEnabled() {
    return compactionsEnabled;
  }

  public void setCompactionsEnabled(boolean compactionsEnabled) {
    this.compactionsEnabled = compactionsEnabled;
    this.conf.setBoolean(HBASE_REGION_SERVER_ENABLE_COMPACTION, compactionsEnabled);
  }

  /** Returns the longCompactions thread pool executor */
  ThreadPoolExecutor getLongCompactions() {
    return longCompactions;
  }

  /** Returns the shortCompactions thread pool executor */
  ThreadPoolExecutor getShortCompactions() {
    return shortCompactions;
  }

  private String getStoreNameForUnderCompaction(HStore store) {
    return String.format("%s:%s",
      store.getHRegion() != null ? store.getHRegion().getRegionInfo().getEncodedName() : "",
      store.getColumnFamilyName());
  }

}
