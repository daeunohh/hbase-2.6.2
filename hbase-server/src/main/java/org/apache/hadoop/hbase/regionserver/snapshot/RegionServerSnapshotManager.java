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
package org.apache.hadoop.hbase.regionserver.snapshot;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.snapshot.MasterSnapshotVerifier;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManager;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.SubprocedureFactory;
import org.apache.hadoop.hbase.procedure.ZKProcedureMemberRpcs;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * This manager class handles the work dealing with snapshots for a {@link HRegionServer}.
 * <p>
 * This provides the mechanism necessary to kick off a online snapshot specific {@link Subprocedure}
 * that is responsible for the regions being served by this region server. If any failures occur
 * with the subprocedure, the RegionSeverSnapshotManager's subprocedure handler,
 * {@link ProcedureMember}, notifies the master's ProcedureCoordinator to abort all others.
 * <p>
 * On startup, requires {@link #start()} to be called.
 * <p>
 * On shutdown, requires {@link #stop(boolean)} to be called
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Unstable
public class RegionServerSnapshotManager extends RegionServerProcedureManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerSnapshotManager.class);

  /** Maximum number of snapshot region tasks that can run concurrently */
  private static final String CONCURENT_SNAPSHOT_TASKS_KEY =
    "hbase.snapshot.region.concurrentTasks";
  private static final int DEFAULT_CONCURRENT_SNAPSHOT_TASKS = 3;

  /** Conf key for number of request threads to start snapshots on regionservers */
  public static final String SNAPSHOT_REQUEST_THREADS_KEY = "hbase.snapshot.region.pool.threads";
  /** # of threads for snapshotting regions on the rs. */
  public static final int SNAPSHOT_REQUEST_THREADS_DEFAULT = 10;

  /** Conf key for max time to keep threads in snapshot request pool waiting */
  public static final String SNAPSHOT_TIMEOUT_MILLIS_KEY = "hbase.snapshot.region.timeout";
  /** Keep threads alive in request pool for max of 300 seconds */
  public static final long SNAPSHOT_TIMEOUT_MILLIS_DEFAULT = 5 * 60000;

  /** Conf key for millis between checks to see if snapshot completed or if there are errors */
  public static final String SNAPSHOT_REQUEST_WAKE_MILLIS_KEY =
    "hbase.snapshot.region.wakefrequency";
  /** Default amount of time to check for errors while regions finish snapshotting */
  private static final long SNAPSHOT_REQUEST_WAKE_MILLIS_DEFAULT = 500;

  private RegionServerServices rss;
  private ProcedureMemberRpcs memberRpcs;
  private ProcedureMember member;

  /**
   * Exposed for testing.
   * @param conf       HBase configuration.
   * @param parent     parent running the snapshot handler
   * @param memberRpc  use specified memberRpc instance
   * @param procMember use specified ProcedureMember
   */
  RegionServerSnapshotManager(Configuration conf, HRegionServer parent,
    ProcedureMemberRpcs memberRpc, ProcedureMember procMember) {
    this.rss = parent;
    this.memberRpcs = memberRpc;
    this.member = procMember;
  }

  public RegionServerSnapshotManager() {
  }

  /**
   * Start accepting snapshot requests.
   */
  @Override
  public void start() {
if(KnobRuntime.check(java.util.UUID.fromString("8fe9fb09-bb35-32c4-ae94-0626d0bb28d4"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("2ff8d2f3-59af-3e8f-a5ac-47802d745813"))) { LOG.debug("Start Snapshot Manager "); } else if (KnobRuntime.check(java.util.UUID.fromString("130e1b18-a02f-3f8f-9d30-7ee1e2c0b094"))) { LOG.info("Start Snapshot Manager "); } else if (KnobRuntime.check(java.util.UUID.fromString("6f452290-50c3-3c5b-bc49-76c38f99a5e8"))) { LOG.info(("Start Snapshot Manager ") + (rss.getServerName().toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("25436957-78a4-3c8a-971f-4202e93dcb0d"))) { LOG.debug(("Start Snapshot Manager ") + (rss.getServerName().toString())); } else { LOG.debug("Start Snapshot Manager " + rss.getServerName().toString()); }
    this.memberRpcs.start(rss.getServerName().toString(), member);
  }

  /**
   * Close <tt>this</tt> and all running snapshot tasks
   * @param force forcefully stop all running tasks
   */
  @Override
  public void stop(boolean force) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0ff75b55-81f6-39bc-b0c7-d390896abf42"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0c16ebc5-1f3a-34ff-9170-76322ec5c733"))) {
return;
}
    String mode = force ? "abruptly" : "gracefully";
    if (KnobRuntime.check(java.util.UUID.fromString("c1c5bf2e-9de1-3c25-a418-15bcede9d338"))) { LOG.debug(("Stopping RegionServerSnapshotManager ") + (mode)); } else if (KnobRuntime.check(java.util.UUID.fromString("52152c04-fc9b-3c65-806d-0db0ba4e494f"))) { LOG.debug((("Stopping RegionServerSnapshotManager ") + (mode)) + (".")); } else if (KnobRuntime.check(java.util.UUID.fromString("fee886aa-eb5d-3389-8b3f-d5d9420b7718"))) { LOG.info(("Stopping RegionServerSnapshotManager ") + (".")); } else if (KnobRuntime.check(java.util.UUID.fromString("6bdc8109-ea55-3827-8bdb-db474adadb68"))) { LOG.info(("Stopping RegionServerSnapshotManager " + mode) + (".")); } else if (KnobRuntime.check(java.util.UUID.fromString("d7b7069e-3c61-3811-9117-ba8025ff3f83"))) { LOG.info(("Stopping RegionServerSnapshotManager ") + (mode)); } else if (KnobRuntime.check(java.util.UUID.fromString("09a04961-cf5a-3a94-a151-a112a288b052"))) { LOG.debug(("Stopping RegionServerSnapshotManager " + mode) + (".")); } else if (KnobRuntime.check(java.util.UUID.fromString("6047a325-76e7-35b5-b08a-7de071f57e3e"))) { LOG.debug("Stopping RegionServerSnapshotManager "); } else if (KnobRuntime.check(java.util.UUID.fromString("2141a379-f35b-3f21-9016-afa732bfdab2"))) { LOG.info("Stopping RegionServerSnapshotManager "); } else if (KnobRuntime.check(java.util.UUID.fromString("1743b3d6-36b1-318c-a590-fa17074daf5f"))) { LOG.info("."); } else if (KnobRuntime.check(java.util.UUID.fromString("7d27159c-19a4-337e-8840-b148b1578c93"))) { LOG.debug(("Stopping RegionServerSnapshotManager ") + (".")); } else if (KnobRuntime.check(java.util.UUID.fromString("6ae07c41-b3f9-3319-9637-882c4008d0df"))) { LOG.info((("Stopping RegionServerSnapshotManager ") + (mode)) + (".")); } else if (KnobRuntime.check(java.util.UUID.fromString("1451fb4e-bd85-3e7a-a290-e99c17e0fc5a"))) { LOG.info("Stopping RegionServerSnapshotManager " + mode); } else if (KnobRuntime.check(java.util.UUID.fromString("39073a0e-143d-3823-b932-8b281a2b99aa"))) { LOG.debug("Stopping RegionServerSnapshotManager " + mode); } else if (KnobRuntime.check(java.util.UUID.fromString("0ea1b7f5-0819-3292-8207-4779e02df1fa"))) { LOG.debug("."); } else { LOG.info("Stopping RegionServerSnapshotManager " + mode + "."); }

    try {
if(KnobRuntime.check(java.util.UUID.fromString("337cef10-c2da-3fbd-b69e-00341e9cf89d"))) {
throw new java.io.IOException("Injected exception");
}
      this.member.close();
    } finally {
if(KnobRuntime.check(java.util.UUID.fromString("3b89cd8a-9c2f-3174-8b57-d69fe389d242"))) {
throw new java.io.IOException("Injected exception");
}
      this.memberRpcs.close();
    }
  }

  /**
   * If in a running state, creates the specified subprocedure for handling an online snapshot.
   * Because this gets the local list of regions to snapshot and not the set the master had, there
   * is a possibility of a race where regions may be missed. This detected by the master in the
   * snapshot verification step.
   * @return Subprocedure to submit to the ProcedureMember.
   */
  public Subprocedure buildSubprocedure(SnapshotDescription snapshot) {

    // don't run a snapshot if the parent is stop(ping)
    if (rss.isStopping() || rss.isStopped()) {
      throw new IllegalStateException(
        "Can't start snapshot on RS: " + rss.getServerName() + ", because stopping/stopped!");
    }

    // check to see if this server is hosting any regions for the snapshots
    // check to see if we have regions for the snapshot
    List<HRegion> involvedRegions;
    try {
      involvedRegions = getRegionsToSnapshot(snapshot);
    } catch (IOException e1) {
      throw new IllegalStateException("Failed to figure out if we should handle a snapshot - "
        + "something has gone awry with the online regions.", e1);
    }

    // We need to run the subprocedure even if we have no relevant regions. The coordinator
    // expects participation in the procedure and without sending message the snapshot attempt
    // will hang and fail.

    LOG.debug("Launching subprocedure for snapshot " + snapshot.getName() + " from table "
      + snapshot.getTable() + " type " + snapshot.getType());
    ForeignExceptionDispatcher exnDispatcher = new ForeignExceptionDispatcher(snapshot.getName());
    Configuration conf = rss.getConfiguration();
    long timeoutMillis = conf.getLong(SNAPSHOT_TIMEOUT_MILLIS_KEY, SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
    long wakeMillis =
      conf.getLong(SNAPSHOT_REQUEST_WAKE_MILLIS_KEY, SNAPSHOT_REQUEST_WAKE_MILLIS_DEFAULT);

    switch (snapshot.getType()) {
      case FLUSH:
        SnapshotSubprocedurePool taskManager =
          new SnapshotSubprocedurePool(rss.getServerName().toString(), conf, rss);
        return new FlushSnapshotSubprocedure(member, exnDispatcher, wakeMillis, timeoutMillis,
          involvedRegions, snapshot, taskManager);
      case SKIPFLUSH:
        /*
         * This is to take an online-snapshot without force a coordinated flush to prevent pause The
         * snapshot type is defined inside the snapshot description. FlushSnapshotSubprocedure
         * should be renamed to distributedSnapshotSubprocedure, and the flush() behavior can be
         * turned on/off based on the flush type. To minimized the code change, class name is not
         * changed.
         */
        SnapshotSubprocedurePool taskManager2 =
          new SnapshotSubprocedurePool(rss.getServerName().toString(), conf, rss);
        return new FlushSnapshotSubprocedure(member, exnDispatcher, wakeMillis, timeoutMillis,
          involvedRegions, snapshot, taskManager2);

      default:
        throw new UnsupportedOperationException("Unrecognized snapshot type:" + snapshot.getType());
    }
  }

  /**
   * Determine if the snapshot should be handled on this server NOTE: This is racy -- the master
   * expects a list of regionservers. This means if a region moves somewhere between the calls we'll
   * miss some regions. For example, a region move during a snapshot could result in a region to be
   * skipped or done twice. This is manageable because the {@link MasterSnapshotVerifier} will
   * double check the region lists after the online portion of the snapshot completes and will
   * explicitly fail the snapshot.
   * @return the list of online regions. Empty list is returned if no regions are responsible for
   *         the given snapshot.
   */
  private List<HRegion> getRegionsToSnapshot(SnapshotDescription snapshot) throws IOException {
    List<HRegion> onlineRegions =
      (List<HRegion>) rss.getRegions(TableName.valueOf(snapshot.getTable()));
    Iterator<HRegion> iterator = onlineRegions.iterator();
    // remove the non-default regions
    while (iterator.hasNext()) {
      HRegion r = iterator.next();
      if (!RegionReplicaUtil.isDefaultReplica(r.getRegionInfo())) {
        iterator.remove();
      }
    }
    return onlineRegions;
  }

  /**
   * Build the actual snapshot runner that will do all the 'hard' work
   */
  public class SnapshotSubprocedureBuilder implements SubprocedureFactory {

    @Override
    public Subprocedure buildSubprocedure(String name, byte[] data) {
      try {
        // unwrap the snapshot information
        SnapshotDescription snapshot = SnapshotDescription.parseFrom(data);
        return RegionServerSnapshotManager.this.buildSubprocedure(snapshot);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not read snapshot information from request.");
      }
    }

  }

  /**
   * We use the SnapshotSubprocedurePool, a class specific thread pool instead of
   * {@link org.apache.hadoop.hbase.executor.ExecutorService}. It uses a
   * {@link java.util.concurrent.ExecutorCompletionService} which provides queuing of completed
   * tasks which lets us efficiently cancel pending tasks upon the earliest operation failures.
   * HBase's ExecutorService (different from {@link java.util.concurrent.ExecutorService}) isn't
   * really built for coordinated tasks where multiple threads as part of one larger task. In RS's
   * the HBase Executor services are only used for open and close and not other threadpooled
   * operations such as compactions and replication sinks.
   */
  static class SnapshotSubprocedurePool {
    private final Abortable abortable;
    private final ExecutorCompletionService<Void> taskPool;
    private final ThreadPoolExecutor executor;
    private volatile boolean stopped;
    private final List<Future<Void>> futures = new ArrayList<>();
    private final String name;

    SnapshotSubprocedurePool(String name, Configuration conf, Abortable abortable) {
      this.abortable = abortable;
      // configure the executor service
      long keepAlive = conf.getLong(RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_KEY,
        RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
      int threads = conf.getInt(CONCURENT_SNAPSHOT_TASKS_KEY, DEFAULT_CONCURRENT_SNAPSHOT_TASKS);
      this.name = name;
      executor = Threads.getBoundedCachedThreadPool(threads, keepAlive, TimeUnit.MILLISECONDS,
        new ThreadFactoryBuilder().setNameFormat("rs(" + name + ")-snapshot-pool-%d")
          .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
      taskPool = new ExecutorCompletionService<>(executor);
    }

    boolean hasTasks() {
      return futures.size() != 0;
    }

    /**
     * Submit a task to the pool. NOTE: all must be submitted before you can safely
     * {@link #waitForOutstandingTasks()}. This version does not support issuing tasks from multiple
     * concurrent table snapshots requests.
     */
    void submitTask(final Callable<Void> task) {
      Future<Void> f = this.taskPool.submit(task);
      futures.add(f);
    }

    /**
     * Wait for all of the currently outstanding tasks submitted via {@link #submitTask(Callable)}.
     * This *must* be called after all tasks are submitted via submitTask.
     * @return <tt>true</tt> on success, <tt>false</tt> otherwise
     * @throws SnapshotCreationException if the snapshot failed while we were waiting
     */
    boolean waitForOutstandingTasks() throws ForeignException, InterruptedException {
      LOG.debug("Waiting for local region snapshots to finish.");

      int sz = futures.size();
      try {
        // Using the completion service to process the futures that finish first first.
        for (int i = 0; i < sz; i++) {
          Future<Void> f = taskPool.take();
          f.get();
          if (!futures.remove(f)) {
            LOG.warn("unexpected future" + f);
          }
          LOG.debug("Completed " + (i + 1) + "/" + sz + " local region snapshots.");
        }
        LOG.debug("Completed " + sz + " local region snapshots.");
        return true;
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in SnapshotSubprocedurePool", e);
        if (!stopped) {
          Thread.currentThread().interrupt();
          throw new ForeignException("SnapshotSubprocedurePool", e);
        }
        // we are stopped so we can just exit.
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ForeignException) {
          LOG.warn("Rethrowing ForeignException from SnapshotSubprocedurePool", e);
          throw (ForeignException) e.getCause();
        } else if (cause instanceof DroppedSnapshotException) {
          // we have to abort the region server according to contract of flush
          abortable.abort("Received DroppedSnapshotException, aborting", cause);
        }
        LOG.warn("Got Exception in SnapshotSubprocedurePool", e);
        throw new ForeignException(name, e.getCause());
      } finally {
        cancelTasks();
      }
      return false;
    }

    /**
     * This attempts to cancel out all pending and in progress tasks (interruptions issues)
     */
    void cancelTasks() throws InterruptedException {
      Collection<Future<Void>> tasks = futures;
      LOG.debug("cancelling " + tasks.size() + " tasks for snapshot " + name);
      for (Future<Void> f : tasks) {
        // TODO Ideally we'd interrupt hbase threads when we cancel. However it seems that there
        // are places in the HBase code where row/region locks are taken and not released in a
        // finally block. Thus we cancel without interrupting. Cancellations will be slower to
        // complete but we won't suffer from unreleased locks due to poor code discipline.
        f.cancel(false);
      }

      // evict remaining tasks and futures from taskPool.
      futures.clear();
      while (taskPool.poll() != null) {
      }
      stop();
    }

    /**
     * Abruptly shutdown the thread pool. Call when exiting a region server.
     */
    void stop() {
      if (this.stopped) return;

      this.stopped = true;
      this.executor.shutdown();
    }
  }

  /**
   * Create a default snapshot handler - uses a zookeeper based member controller.
   * @param rss region server running the handler
   * @throws KeeperException if the zookeeper cluster cannot be reached
   */
  @Override
  public void initialize(RegionServerServices rss) throws KeeperException {
if(KnobRuntime.check(java.util.UUID.fromString("b5b30bb2-02a0-391b-be08-781e243fc493"))) {
return;
}
    this.rss = rss;
    ZKWatcher zkw = rss.getZooKeeper();
    this.memberRpcs =
      new ZKProcedureMemberRpcs(zkw, SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION);

    // read in the snapshot request configuration properties
    Configuration conf = rss.getConfiguration();
    long keepAlive = conf.getLong(SNAPSHOT_TIMEOUT_MILLIS_KEY, SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
    int opThreads = conf.getInt(SNAPSHOT_REQUEST_THREADS_KEY, SNAPSHOT_REQUEST_THREADS_DEFAULT);

    // create the actual snapshot procedure member
    ThreadPoolExecutor pool =
      ProcedureMember.defaultPool(rss.getServerName().toString(), opThreads, keepAlive);
    this.member = new ProcedureMember(memberRpcs, pool, new SnapshotSubprocedureBuilder());
  }

  @Override
  public String getProcedureSignature() {
if(KnobRuntime.check(java.util.UUID.fromString("23babaca-634a-3c64-808c-2036cb71cd32"))) {
return null;
}
    return SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION;
  }

}
