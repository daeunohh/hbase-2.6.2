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
package org.apache.hadoop.hbase.master.snapshot;
import org.knobinjection.runtime.KnobRuntime;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.WorkerAssigner;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner;
import org.apache.hadoop.hbase.master.procedure.CloneSnapshotProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.RestoreSnapshotProcedure;
import org.apache.hadoop.hbase.master.procedure.SnapshotProcedure;
import org.apache.hadoop.hbase.master.procedure.SnapshotVerifyProcedure;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ZKProcedureCoordinator;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerValidationUtils;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclCleaner;
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclHelper;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotExistsException;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.TablePartiallyOpenException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription.Type;

/**
 * This class manages the procedure of taking and restoring snapshots. There is only one
 * SnapshotManager for the master.
 * <p>
 * The class provides methods for monitoring in-progress snapshot actions.
 * <p>
 * Note: Currently there can only be one snapshot being taken at a time over the cluster. This is a
 * simplification in the current implementation.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Unstable
public class SnapshotManager extends MasterProcedureManager implements Stoppable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

  /** By default, check to see if the snapshot is complete every WAKE MILLIS (ms) */
  private static final int SNAPSHOT_WAKE_MILLIS_DEFAULT = 500;

  /**
   * Wait time before removing a finished sentinel from the in-progress map NOTE: This is used as a
   * safety auto cleanup. The snapshot and restore handlers map entries are removed when a user asks
   * if a snapshot or restore is completed. This operation is part of the HBaseAdmin
   * snapshot/restore API flow. In case something fails on the client side and the snapshot/restore
   * state is not reclaimed after a default timeout, the entry is removed from the in-progress map.
   * At this point, if the user asks for the snapshot/restore status, the result will be snapshot
   * done if exists or failed if it doesn't exists.
   */
  public static final String HBASE_SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLIS =
    "hbase.snapshot.sentinels.cleanup.timeoutMillis";
  public static final long SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLS_DEFAULT = 60 * 1000L;

  /** Enable or disable snapshot support */
  public static final String HBASE_SNAPSHOT_ENABLED = "hbase.snapshot.enabled";

  /**
   * Conf key for # of ms elapsed between checks for snapshot errors while waiting for completion.
   */
  private static final String SNAPSHOT_WAKE_MILLIS_KEY = "hbase.snapshot.master.wakeMillis";

  /** Name of the operation to use in the controller */
  public static final String ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION = "online-snapshot";

  /** Conf key for # of threads used by the SnapshotManager thread pool */
  public static final String SNAPSHOT_POOL_THREADS_KEY = "hbase.snapshot.master.threads";

  /** number of current operations running on the master */
  public static final int SNAPSHOT_POOL_THREADS_DEFAULT = 1;

  /** Conf key for preserving original max file size configs */
  public static final String SNAPSHOT_MAX_FILE_SIZE_PRESERVE =
    "hbase.snapshot.max.filesize.preserve";

  /** Enable or disable snapshot procedure */
  public static final String SNAPSHOT_PROCEDURE_ENABLED = "hbase.snapshot.procedure.enabled";

  public static final boolean SNAPSHOT_PROCEDURE_ENABLED_DEFAULT = true;

  private boolean stopped;
  private MasterServices master; // Needed by TableEventHandlers
  private ProcedureCoordinator coordinator;

  // Is snapshot feature enabled?
  private boolean isSnapshotSupported = false;

  // Snapshot handlers map, with table name as key.
  // The map is always accessed and modified under the object lock using synchronized.
  // snapshotTable() will insert an Handler in the table.
  // isSnapshotDone() will remove the handler requested if the operation is finished.
  private final Map<TableName, SnapshotSentinel> snapshotHandlers = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setNameFormat("SnapshotHandlerChoreCleaner").setDaemon(true).build());
  private ScheduledFuture<?> snapshotHandlerChoreCleanerTask;

  // Restore map, with table name as key, procedure ID as value.
  // The map is always accessed and modified under the object lock using synchronized.
  // restoreSnapshot()/cloneSnapshot() will insert a procedure ID in the map.
  //
  // TODO: just as the Apache HBase 1.x implementation, this map would not survive master
  // restart/failover. This is just a stopgap implementation until implementation of taking
  // snapshot using Procedure-V2.
  private Map<TableName, Long> restoreTableToProcIdMap = new HashMap<>();

  // SnapshotDescription -> SnapshotProcId
  private final ConcurrentHashMap<SnapshotDescription, Long> snapshotToProcIdMap =
    new ConcurrentHashMap<>();

  private WorkerAssigner verifyWorkerAssigner;

  private Path rootDir;
  private ExecutorService executorService;

  /**
   * Read write lock between taking snapshot and snapshot HFile cleaner. The cleaner should skip to
   * check the HFiles if any snapshot is in progress, otherwise it may clean a HFile which would
   * belongs to the newly creating snapshot. So we should grab the write lock first when cleaner
   * start to work. (See HBASE-21387)
   */
  private ReentrantReadWriteLock takingSnapshotLock = new ReentrantReadWriteLock(true);

  public SnapshotManager() {
  }

  /**
   * Fully specify all necessary components of a snapshot manager. Exposed for testing.
   * @param master      services for the master where the manager is running
   * @param coordinator procedure coordinator instance. exposed for testing.
   * @param pool        HBase ExecutorServcie instance, exposed for testing.
   */
  @InterfaceAudience.Private
  SnapshotManager(final MasterServices master, ProcedureCoordinator coordinator,
    ExecutorService pool, int sentinelCleanInterval)
    throws IOException, UnsupportedOperationException {
    this.master = master;

    this.rootDir = master.getMasterFileSystem().getRootDir();
    Configuration conf = master.getConfiguration();
    checkSnapshotSupport(conf, master.getMasterFileSystem());

    this.coordinator = coordinator;
    this.executorService = pool;
    resetTempDir();
    snapshotHandlerChoreCleanerTask = this.scheduleThreadPool.scheduleAtFixedRate(
      this::cleanupSentinels, sentinelCleanInterval, sentinelCleanInterval, TimeUnit.SECONDS);
  }

  /**
   * Gets the list of all completed snapshots.
   * @return list of SnapshotDescriptions
   * @throws IOException File system exception
   */
  public List<SnapshotDescription> getCompletedSnapshots() throws IOException {
    return getCompletedSnapshots(SnapshotDescriptionUtils.getSnapshotsDir(rootDir), true);
  }

  /**
   * Gets the list of all completed snapshots.
   * @param snapshotDir snapshot directory
   * @param withCpCall  Whether to call CP hooks
   * @return list of SnapshotDescriptions
   * @throws IOException File system exception
   */
  private List<SnapshotDescription> getCompletedSnapshots(Path snapshotDir, boolean withCpCall)
    throws IOException {
    List<SnapshotDescription> snapshotDescs = new ArrayList<>();
    // first create the snapshot root path and check to see if it exists
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    if (snapshotDir == null) snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);

    // if there are no snapshots, return an empty list
    if (!fs.exists(snapshotDir)) {
      return snapshotDescs;
    }

    // ignore all the snapshots in progress
    FileStatus[] snapshots = fs.listStatus(snapshotDir,
      new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    withCpCall = withCpCall && cpHost != null;
    // loop through all the completed snapshots
    for (FileStatus snapshot : snapshots) {
      Path info = new Path(snapshot.getPath(), SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
      // if the snapshot is bad
      if (!fs.exists(info)) {
        LOG.error("Snapshot information for " + snapshot.getPath() + " doesn't exist");
        continue;
      }
      FSDataInputStream in = null;
      try {
if(KnobRuntime.check(java.util.UUID.fromString("48fb314b-14f1-3741-aa7f-e5810f2be36e"))) {
throw new java.io.IOException("Injected exception");
}
        in = fs.open(info);
        SnapshotDescription desc = SnapshotDescription.parseFrom(in);
        org.apache.hadoop.hbase.client.SnapshotDescription descPOJO =
          (withCpCall) ? ProtobufUtil.createSnapshotDesc(desc) : null;
        if (withCpCall) {
          try {
            cpHost.preListSnapshot(descPOJO);
          } catch (AccessDeniedException e) {
            LOG.warn("Current user does not have access to " + desc.getName() + " snapshot. "
              + "Either you should be owner of this snapshot or admin user.");
            // Skip this and try for next snapshot
            continue;
          }
        }
        snapshotDescs.add(desc);

        // call coproc post hook
        if (((KnobRuntime.check(java.util.UUID.fromString("2bd9c25a-7bac-3694-8647-ec4ab10ccfa4"))) ? ((withCpCall)) : (((KnobRuntime.check(java.util.UUID.fromString("eb8dd386-a2bc-310f-b26d-c60f7593b061"))) ? (!withCpCall) : (withCpCall))))) {
          cpHost.postListSnapshot(descPOJO);
        }
      } catch (IOException e) {
        LOG.warn("Found a corrupted snapshot " + snapshot.getPath(), e);
      } finally {
        if (((KnobRuntime.check(java.util.UUID.fromString("91f913a4-e136-3e31-895e-84009071346a"))) ? ((in) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0aa273f2-93f1-3ea6-85f6-42204a011fe0"))) ? ((in) == (null)) : (in != null))))) {
          in.close();
        }
      }
    }
    return snapshotDescs;
  }

  /**
   * Cleans up any zk-coordinated snapshots in the snapshot/.tmp directory that were left from
   * failed snapshot attempts. For unfinished procedure2-coordinated snapshots, keep the working
   * directory.
   * @throws IOException if we can't reach the filesystem
   */
  private void resetTempDir() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2d50a9cc-6f30-31b9-a09e-1a260850e305"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("0f3b0404-4d6c-3a67-b752-b0ec52552710"))) {
throw new java.io.IOException("Injected exception");
}
    Set<String> workingProcedureCoordinatedSnapshotNames =
      snapshotToProcIdMap.keySet().stream().map(s -> s.getName()).collect(Collectors.toSet());

    Path tmpdir =
      SnapshotDescriptionUtils.getWorkingSnapshotDir(rootDir, master.getConfiguration());
if(KnobRuntime.check(java.util.UUID.fromString("8acea7c0-142b-396a-8269-5069cf1f1b2a"))) {
throw new java.io.IOException("Injected exception");
}
    FileSystem tmpFs = tmpdir.getFileSystem(master.getConfiguration());
if(KnobRuntime.check(java.util.UUID.fromString("f81f08ac-5c6e-3bc0-9632-78b40ea6be19"))) {
throw new java.io.IOException("Injected exception");
}
    FileStatus[] workingSnapshotDirs = CommonFSUtils.listStatus(tmpFs, tmpdir);
    if (((KnobRuntime.check(java.util.UUID.fromString("5ec5293e-98e1-397d-91f6-13963699912b"))) ? ((workingSnapshotDirs) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("15ad2469-e74b-30d7-a997-f55eb2f2fb78"))) ? ((workingSnapshotDirs) != (null)) : (workingSnapshotDirs == null))))) {
      return;
    }
    for (FileStatus workingSnapshotDir : workingSnapshotDirs) {
      String workingSnapshotName = workingSnapshotDir.getPath().getName();
      if (!workingProcedureCoordinatedSnapshotNames.contains(workingSnapshotName)) {
        try {
          if (tmpFs.delete(workingSnapshotDir.getPath(), true)) {
            LOG.info("delete unfinished zk-coordinated snapshot working directory {}",
              workingSnapshotDir.getPath());
          } else {
            LOG.warn("Couldn't delete unfinished zk-coordinated snapshot working directory {}",
              workingSnapshotDir.getPath());
          }
        } catch (IOException e) {
          LOG.warn("Couldn't delete unfinished zk-coordinated snapshot working directory {}",
            workingSnapshotDir.getPath(), e);
        }
      } else {
        LOG.debug("find working directory of unfinished procedure {}", workingSnapshotName);
      }
    }
  }

  /**
   * Delete the specified snapshot
   * @throws SnapshotDoesNotExistException If the specified snapshot does not exist.
   * @throws IOException                   For filesystem IOExceptions
   */
  public void deleteSnapshot(SnapshotDescription snapshot) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b6cd209f-1d3e-3e72-a010-a74aec2f6f16"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("05a66713-c69b-367a-bf5b-7f2b63288b75"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa0c12d4-0919-377c-ba29-ea78f9b7ff42"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("deff3fbc-cd70-3a2e-b116-2d8a43718cf1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b32c89de-c84c-3d8c-ab8e-2fd206807999"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c9e458c-0320-3807-95ac-bed6a6f8737c"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b3da815-2cd9-33c0-8238-9d25dd9e6c9b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d56514cc-e569-3772-89de-1d96c3ab6ad3"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bba44cd5-4ed4-3b3b-92c3-5c681461440d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e27714b9-b8d5-3010-bec1-739ee819a113"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9daac610-6412-31c6-bb2f-dcadb80237d2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9affc53b-f641-38d3-a3d3-140982c9b0fd"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3bb98fbe-28b8-3aac-bdf2-62f8447b3d51"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14dfffab-3f69-3d11-848e-64952c14345b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42111201-925f-314b-bc83-c363c1ebe1e7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e6cb5c39-1ca9-30f9-a374-9cf171f31bda"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d79518d-ac4b-3fa9-b216-e815842151c6"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01f7c721-8534-3418-9075-6a40ed2a9193"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // check to see if it is completed
    if (!isSnapshotCompleted(snapshot)) {
      throw new SnapshotDoesNotExistException(ProtobufUtil.createSnapshotDesc(snapshot));
    }

    String snapshotName = snapshot.getName();
    // first create the snapshot description and check to see if it exists
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    // Get snapshot info from file system. The one passed as parameter is a "fake" snapshotInfo with
    // just the "name" and it does not contains the "real" snapshot information
    snapshot = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    // call coproc pre hook
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("629f6000-e0cd-3fa3-9622-1bc2597aa84b"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e7ace8b4-cd12-37e1-aedf-81aa45b51fc9"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("b83f84a0-edd2-37e6-9a8d-42b2e82e6576"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("470f8b85-b161-320e-a82e-b5801cd55fdc"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b08c451-d569-3009-a517-a549883b8845"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("103ab079-e5dd-3632-aec8-9b24acca840f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("db76357d-0de6-3189-b084-cc606031026f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46c76351-b180-3da3-b82e-9826ab58fa95"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b927e223-14e1-3147-ad51-10b6496b1b2e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dae90349-367b-381a-9560-8d02b704c297"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c2dd907-6bc6-3b84-90b9-b31edd4c09ca"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f63183c4-9d2d-34db-80b0-2899a8abb14d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("235fcb52-7c28-3c09-84e1-d649a7932bd7"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ebf8f4e-24a9-3f34-8a39-845aa9c627b1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dd2bd45-9793-379b-8b6d-77df217fc8d5"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20de5a03-b3e1-37be-8719-c642978dbd3a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd22213d-1a1e-3b00-8e9d-46291ff14d83"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d7e3e62-218c-3970-8c09-7ac361a716d2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
      cpHost.preDeleteSnapshot(snapshotPOJO);
    }

    LOG.debug("Deleting snapshot: " + snapshotName);
    // delete the existing snapshot
    if (!fs.delete(snapshotDir, true)) {
      throw new HBaseSnapshotException("Failed to delete snapshot directory: " + snapshotDir);
    }

    // call coproc post hook
    if (((KnobRuntime.check(java.util.UUID.fromString("4dbcf57f-8545-3d78-b647-e67606e7797a"))) ? ((cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fd35055f-b876-3da4-baa2-bc78847dda97"))) ? ((cpHost) != (null)) : (cpHost != null))))) {
      cpHost.postDeleteSnapshot(snapshotPOJO);
    }

  }

  /**
   * Check if the specified snapshot is done
   * @return true if snapshot is ready to be restored, false if it is still being taken.
   * @throws IOException              IOException if error from HDFS or RPC
   * @throws UnknownSnapshotException if snapshot is invalid or does not exist.
   */
  public boolean isSnapshotDone(SnapshotDescription expected) throws IOException {
    // check the request to make sure it has a snapshot
    if (expected == null) {
      throw new UnknownSnapshotException(
        "No snapshot name passed in request, can't figure out which snapshot you want to check.");
    }

    Long procId = snapshotToProcIdMap.get(expected);
    if (procId != null) {
      if (master.getMasterProcedureExecutor().isRunning()) {
        return master.getMasterProcedureExecutor().isFinished(procId);
      } else {
        return false;
      }
    }

    String ssString = ClientSnapshotDescriptionUtils.toString(expected);

    // check to see if the sentinel exists,
    // and if the task is complete removes it from the in-progress snapshots map.
    SnapshotSentinel handler = removeSentinelIfFinished(this.snapshotHandlers, expected);

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    if (handler == null) {
      // If there's no handler in the in-progress map, it means one of the following:
      // - someone has already requested the snapshot state
      // - the requested snapshot was completed long time ago (cleanupSentinels() timeout)
      // - the snapshot was never requested
      // In those cases returns to the user the "done state" if the snapshots exists on disk,
      // otherwise raise an exception saying that the snapshot is not running and doesn't exist.
      if (!isSnapshotCompleted(expected)) {
        throw new UnknownSnapshotException("Snapshot " + ssString
          + " is not currently running or one of the known completed snapshots.");
      }
      // was done, return true;
      return true;
    }

    // pass on any failure we find in the sentinel
    try {
      handler.rethrowExceptionIfFailed();
    } catch (ForeignException e) {
      // Give some procedure info on an exception.
      String status;
      Procedure p = coordinator.getProcedure(expected.getName());
      if (p != null) {
        status = p.getStatus();
      } else {
        status = expected.getName() + " not found in proclist " + coordinator.getProcedureNames();
      }
      throw new HBaseSnapshotException("Snapshot " + ssString + " had an error.  " + status, e,
        ProtobufUtil.createSnapshotDesc(expected));
    }

    // check to see if we are done
    if (handler.isFinished()) {
      LOG.debug("Snapshot '" + ssString + "' has completed, notifying client.");
      return true;
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Snapshoting '" + ssString + "' is still in progress!");
    }
    return false;
  }

  /**
   * Check to see if there is a snapshot in progress with the same name or on the same table.
   * Currently we have a limitation only allowing a single snapshot per table at a time. Also we
   * don't allow snapshot with the same name.
   * @param snapshot   description of the snapshot being checked.
   * @param checkTable check if the table is already taking a snapshot.
   * @return <tt>true</tt> if there is a snapshot in progress with the same name or on the same
   *         table.
   */
  synchronized boolean isTakingSnapshot(final SnapshotDescription snapshot, boolean checkTable) {
    if (checkTable) {
      TableName snapshotTable = TableName.valueOf(snapshot.getTable());
      if (isTakingSnapshot(snapshotTable)) {
        return true;
      }
    }
    Iterator<Map.Entry<TableName, SnapshotSentinel>> it = snapshotHandlers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, SnapshotSentinel> entry = it.next();
      SnapshotSentinel sentinel = entry.getValue();
      if (snapshot.getName().equals(sentinel.getSnapshot().getName()) && !sentinel.isFinished()) {
        return true;
      }
    }
    Iterator<Map.Entry<SnapshotDescription, Long>> spIt = snapshotToProcIdMap.entrySet().iterator();
    while (spIt.hasNext()) {
      Map.Entry<SnapshotDescription, Long> entry = spIt.next();
      if (
        snapshot.getName().equals(entry.getKey().getName())
          && !master.getMasterProcedureExecutor().isFinished(entry.getValue())
      ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check to see if the specified table has a snapshot in progress. Currently we have a limitation
   * only allowing a single snapshot per table at a time.
   * @param tableName name of the table being snapshotted.
   * @return <tt>true</tt> if there is a snapshot in progress on the specified table.
   */
  public boolean isTakingSnapshot(final TableName tableName) {
    return isTakingSnapshot(tableName, false);
  }

  public boolean isTableTakingAnySnapshot(final TableName tableName) {
    return isTakingSnapshot(tableName, true);
  }

  /**
   * Check to see if the specified table has a snapshot in progress. Since we introduce the
   * SnapshotProcedure, it is a little bit different from before. For zk-coordinated snapshot, we
   * can just consider tables in snapshotHandlers only, but for
   * {@link org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure} and
   * {@link org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure}, we need to
   * consider tables in snapshotToProcIdMap also, for the snapshot procedure, we don't need to check
   * if table in snapshot.
   * @param tableName      name of the table being snapshotted.
   * @param checkProcedure true if we should check tables in snapshotToProcIdMap
   * @return <tt>true</tt> if there is a snapshot in progress on the specified table.
   */
  private synchronized boolean isTakingSnapshot(TableName tableName, boolean checkProcedure) {
if(KnobRuntime.check(java.util.UUID.fromString("2ece230d-eb9b-3797-b8af-4106baca80e9"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d55b3dc8-4251-3790-abc1-34f19321042c"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9c90938b-3382-3e55-89bb-ad9ec7df4588"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4018dc5c-aa63-388c-b7c2-b2700d440c34"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3de86b70-0474-3d70-9395-d87eb078f232"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a4194ab-613a-33d6-ad62-bee3d9bbcb36"))) {
return true;
}
    SnapshotSentinel handler = this.snapshotHandlers.get(tableName);
    if (handler != null && !handler.isFinished()) {
      return true;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("07e269b6-4071-368c-9c56-8c5e4c7ed46e"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("52f86d99-afba-375e-b707-d6aeee00b9b4"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("76a41c5c-6a57-3f56-964a-65d2bf0068f0"))) ? (!checkProcedure) : (checkProcedure))))))) {
      for (Map.Entry<SnapshotDescription, Long> entry : snapshotToProcIdMap.entrySet()) {
if(KnobRuntime.check(java.util.UUID.fromString("1c1f9aa3-9c44-3d14-8b84-1678a39f84cb"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27da96a6-3df3-3274-8c9e-ef8df4d4b62d"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3af8e2da-a31d-3d45-9a51-26ea449899a8"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfcb50d8-619d-302d-9e91-d385559a41a1"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (
          TableName.valueOf(entry.getKey().getTable()).equals(tableName)
            && !master.getMasterProcedureExecutor().isFinished(entry.getValue())
        ) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check to make sure that we are OK to run the passed snapshot. Checks to make sure that we
   * aren't already running a snapshot or restore on the requested table.
   * @param snapshot description of the snapshot we want to start
   * @throws HBaseSnapshotException if the filesystem could not be prepared to start the snapshot
   */
  public synchronized void prepareWorkingDirectory(SnapshotDescription snapshot)
    throws HBaseSnapshotException {
if(KnobRuntime.check(java.util.UUID.fromString("96984f72-30e3-38b8-a18b-a098745fd040"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5cfb3fe-97f4-340c-9680-6c1e46562313"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d7f5ace7-c8fe-313a-b418-b1ee517321e2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aea5eb92-1b70-3bbe-8d19-7d1dc4635f81"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("424cc68b-bea1-30f4-bc87-60e66c248cc6"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b02a1d63-7f0b-37ad-a917-2233630d6860"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eec9c873-2d47-34f2-b2db-ca50e77dc8f1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("039008ef-e4d0-3941-a97e-3426b2d5ee6a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84fd413a-0eab-3631-8943-ec970866de85"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cff3b332-d88a-390d-a0ad-5a6bd4a9b398"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80e3c509-d84c-3970-91f1-3fa949029950"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9fb04ac-ccef-30c4-9988-8a21bf62eeb1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b76a12e-00e6-3c27-8c4e-18d4aba30315"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("026fdcce-b9e2-3304-8faa-c76eab5f625a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0133a40f-410c-36e4-a519-24acd60380c5"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("74396f3b-fc29-3732-95cf-46cf63dd75d6"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51916d4b-b9ff-3062-bde5-b51a95d00b57"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Path workingDir =
      SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir, master.getConfiguration());

    try {
      FileSystem workingDirFS = workingDir.getFileSystem(master.getConfiguration());
      // delete the working directory, since we aren't running the snapshot. Likely leftovers
      // from a failed attempt.
      workingDirFS.delete(workingDir, true);

      // recreate the working directory for the snapshot
      if (!workingDirFS.mkdirs(workingDir)) {
        throw new SnapshotCreationException(
          "Couldn't create working directory (" + workingDir + ") for snapshot",
          ProtobufUtil.createSnapshotDesc(snapshot));
      }
      updateWorkingDirAclsIfRequired(workingDir, workingDirFS);
    } catch (HBaseSnapshotException e) {
      throw e;
    } catch (IOException e) {
      throw new SnapshotCreationException(
        "Exception while checking to see if snapshot could be started.", e,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * If the parent dir of the snapshot working dir (e.g. /hbase/.hbase-snapshot) has non-empty ACLs,
   * use them for the current working dir (e.g. /hbase/.hbase-snapshot/.tmp/{snapshot-name}) so that
   * regardless of whether the snapshot commit phase performs atomic rename or non-atomic copy of
   * the working dir to new snapshot dir, the ACLs are retained.
   * @param workingDir   working dir to build the snapshot.
   * @param workingDirFS working dir file system.
   * @throws IOException If ACL read/modify operation fails.
   */
  private static void updateWorkingDirAclsIfRequired(Path workingDir, FileSystem workingDirFS)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9fe724d6-4805-3d81-87a0-2808e3820810"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("6bcf977c-6e93-3b97-a182-ee2b6848aac0"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("8971ad42-e3b4-3841-905c-41030f083279"))) ? ((workingDir.getParent()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fdf025cb-36a9-3af4-9e07-8f04444a5e16"))) ? (((workingDir.getParent()) != (null)) || ((workingDir.getParent().getParent()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bfed20f8-278e-3c74-b90d-6597b102efd9"))) ? (((workingDir.getParent()) != (null)) && ((workingDir.getParent().getParent()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("82485216-f85c-3ede-8ba3-f95d21c5b06b"))) ? ((workingDir.getParent().getParent()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d88de1ae-675b-3dd1-b1e0-d7a9a7e051d3"))) ? (((workingDir.getParent()) == (null)) && ((workingDir.getParent().getParent()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("07a9f466-c558-3528-8f88-4333532b0e9d"))) ? (((workingDir.getParent()) == (null)) || ((workingDir.getParent().getParent()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4126fcbc-2203-3445-82d1-88b4b36fbf87"))) ? (((workingDir.getParent()) != (null)) && (workingDir.getParent().getParent() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("9af9292c-cde4-39a7-ae3d-0b0b13a5f020"))) ? ((workingDir.getParent() == null) || ((workingDir.getParent().getParent()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("8b3d6867-e4b4-37cb-abc5-a3898e12875d"))) ? (((workingDir.getParent()) == (null)) && (workingDir.getParent().getParent() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("fbeea46f-27c4-3f25-bef9-bcd664c783b0"))) ? (((workingDir.getParent()) == (null)) && ((workingDir.getParent().getParent()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b29273cd-dd2a-3e56-9e82-4d2b0c4623ba"))) ? ((workingDir.getParent() == null) && ((workingDir.getParent().getParent()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("15bff022-6e22-3fb2-a1c3-9b6225a6c7dc"))) ? ((workingDir.getParent()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("45e1d3ec-a800-37fd-a344-5f69da7836e8"))) ? ((workingDir.getParent() == null) && (workingDir.getParent().getParent() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("24066737-d945-324a-939d-f124d9124cae"))) ? (workingDir.getParent().getParent() == null) : (((KnobRuntime.check(java.util.UUID.fromString("0df3784c-008e-3083-a0fd-b76865f3a581"))) ? ((workingDir.getParent() == null) || ((workingDir.getParent().getParent()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b0d80585-32c3-31fc-a95b-8d5d34689bdd"))) ? ((workingDir.getParent() == null) && ((workingDir.getParent().getParent()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("edb23088-0863-33ae-ba4b-fa98cb16a4d1"))) ? (workingDir.getParent() == null) : (((KnobRuntime.check(java.util.UUID.fromString("ba550c3b-029d-3df9-93f1-bfb169fef7ab"))) ? (((workingDir.getParent()) == (null)) || ((workingDir.getParent().getParent()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a4b2e3ca-5600-369e-aa8d-e3b5599f17ab"))) ? ((workingDir.getParent() == null) || (workingDir.getParent().getParent() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("33510890-f843-3edb-98f9-111dc703e89c"))) ? (((workingDir.getParent()) != (null)) && ((workingDir.getParent().getParent()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("cf996e1d-77b6-3cac-b784-b18d9f2acf62"))) ? (((workingDir.getParent()) != (null)) || ((workingDir.getParent().getParent()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("573c1568-4aa1-34e1-a07d-2e6bee42e3be"))) ? (((workingDir.getParent()) != (null)) || (workingDir.getParent().getParent() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("2818b0bd-fa31-369f-be77-387e2371aa37"))) ? (((workingDir.getParent()) == (null)) || (workingDir.getParent().getParent() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("3df2b859-d1ce-35b1-806d-4e03e25ab36e"))) ? ((workingDir.getParent().getParent()) != (null)) : (workingDir.getParent() == null || workingDir.getParent().getParent() == null))))))))))))))))))))))))))))))))))))))))))))))))) {
      return;
    }
    AclStatus snapshotWorkingParentDirStatus;
    try {
      snapshotWorkingParentDirStatus =
        workingDirFS.getAclStatus(workingDir.getParent().getParent());
    } catch (IOException | UnsupportedOperationException e) {
      LOG.warn("Unable to retrieve ACL status for path: {}, current working dir path: {}",
        workingDir.getParent().getParent(), workingDir, e);
      return;
    }
    List<AclEntry> snapshotWorkingParentDirAclStatusEntries =
      snapshotWorkingParentDirStatus.getEntries();
    if (
      snapshotWorkingParentDirAclStatusEntries != null
        && snapshotWorkingParentDirAclStatusEntries.size() > 0
    ) {
if(KnobRuntime.check(java.util.UUID.fromString("b0258958-6987-317e-99bb-0f275b81a902"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("a854998c-dd51-3436-998a-fb7ede46ff94"))) { workingDirFS.modifyAclEntries(workingDir.getParent().getParent(), snapshotWorkingParentDirAclStatusEntries); } else if (KnobRuntime.check(java.util.UUID.fromString("64e78648-2907-37ae-aaf5-4cdd6749a415"))) { workingDirFS.modifyAclEntries(workingDir.getParent(), snapshotWorkingParentDirAclStatusEntries); } else { workingDirFS.modifyAclEntries(workingDir, snapshotWorkingParentDirAclStatusEntries); }
    }
  }

  /**
   * Take a snapshot of a disabled table.
   * @param snapshot description of the snapshot to take. Modified to be {@link Type#DISABLED}.
   * @throws IOException if the snapshot could not be started or filesystem for snapshot temporary
   *                     directory could not be determined
   */
  private synchronized void snapshotDisabledTable(SnapshotDescription snapshot) throws IOException {
    // setup the snapshot
    prepareWorkingDirectory(snapshot);

    // set the snapshot to be a disabled snapshot, since the client doesn't know about that
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();

    // Take the snapshot of the disabled table
    DisabledTableSnapshotHandler handler = new DisabledTableSnapshotHandler(snapshot, master, this);
    snapshotTable(snapshot, handler);
  }

  /**
   * Take a snapshot of an enabled table.
   * @param snapshot description of the snapshot to take.
   * @throws IOException if the snapshot could not be started or filesystem for snapshot temporary
   *                     directory could not be determined
   */
  private synchronized void snapshotEnabledTable(SnapshotDescription snapshot) throws IOException {
    // setup the snapshot
    prepareWorkingDirectory(snapshot);

    // Take the snapshot of the enabled table
    EnabledTableSnapshotHandler handler = new EnabledTableSnapshotHandler(snapshot, master, this);
    snapshotTable(snapshot, handler);
  }

  /**
   * Take a snapshot using the specified handler. On failure the snapshot temporary working
   * directory is removed. NOTE: prepareToTakeSnapshot() called before this one takes care of the
   * rejecting the snapshot request if the table is busy with another snapshot/restore operation.
   * @param snapshot the snapshot description
   * @param handler  the snapshot handler
   */
  private synchronized void snapshotTable(SnapshotDescription snapshot,
    final TakeSnapshotHandler handler) throws IOException {
    try {
      handler.prepare();
      this.executorService.submit(handler);
      this.snapshotHandlers.put(TableName.valueOf(snapshot.getTable()), handler);
    } catch (Exception e) {
      // cleanup the working directory by trying to delete it from the fs.
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir,
        master.getConfiguration());
      FileSystem workingDirFs = workingDir.getFileSystem(master.getConfiguration());
      try {
        if (!workingDirFs.delete(workingDir, true)) {
          LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:"
            + ClientSnapshotDescriptionUtils.toString(snapshot));
        }
      } catch (IOException e1) {
        LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:"
          + ClientSnapshotDescriptionUtils.toString(snapshot));
      }
      // fail the snapshot
      throw new SnapshotCreationException("Could not build snapshot handler", e,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  public ReadWriteLock getTakingSnapshotLock() {
    return this.takingSnapshotLock;
  }

  /**
   * The snapshot operation processing as following: <br>
   * 1. Create a Snapshot Handler, and do some initialization; <br>
   * 2. Put the handler into snapshotHandlers <br>
   * So when we consider if any snapshot is taking, we should consider both the takingSnapshotLock
   * and snapshotHandlers;
   * @return true to indicate that there're some running snapshots.
   */
  public synchronized boolean isTakingAnySnapshot() {
    return this.takingSnapshotLock.getReadHoldCount() > 0 || this.snapshotHandlers.size() > 0
      || this.snapshotToProcIdMap.size() > 0;
  }

  /**
   * Take a snapshot based on the enabled/disabled state of the table.
   * @throws HBaseSnapshotException when a snapshot specific exception occurs.
   * @throws IOException            when some sort of generic IO exception occurs.
   */
  public void takeSnapshot(SnapshotDescription snapshot) throws IOException {
    this.takingSnapshotLock.readLock().lock();
    try {
      takeSnapshotInternal(snapshot);
    } finally {
      this.takingSnapshotLock.readLock().unlock();
    }
  }

  public long takeSnapshot(SnapshotDescription snapshot, long nonceGroup, long nonce)
    throws IOException {
    this.takingSnapshotLock.readLock().lock();
    try {
if(KnobRuntime.check(java.util.UUID.fromString("75fbd6c4-6f4c-3e23-9b8f-c010f63d95d5"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a56c611-0c25-3ca8-9e99-16af438dc15d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("494c095d-d7c6-30bb-b423-671dfc19e4b3"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e8eb9fe-f664-3cf9-8dcb-e0f5308f331b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5c3febc-0607-312d-ae1a-193fff73af4f"))) {
nonce = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a062c6b-9f01-3f58-bcb7-d86e3ac448ea"))) {
nonceGroup += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c774b0c-839f-37d2-b312-836f46ca9624"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50b89e0c-3930-32f3-852c-8e33b19e6c70"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de62d6c7-a92e-3926-8232-24e5a80caede"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d9d0617-0470-3e58-ae35-124a7c074baa"))) {
nonceGroup = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a3135b20-79e5-3322-84c8-74bce51a9cc1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06822214-99a7-307f-a83a-ab7ecf5dbd68"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f767745-3c7d-3da9-a5c2-ec6c77b3b1fd"))) {
nonceGroup = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bfd3c75c-3ebf-3891-bfa2-2a03ce554238"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b11fe35f-15f0-3ac1-9bfb-c8b0f3d249d6"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb76f766-911b-3a9e-a960-5ce9ec4f4cd1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a7faf14-1aac-3f0b-a4eb-93887f3c5884"))) {
nonce = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0b730d8c-bd5b-381d-8d0f-0c395af39c11"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb6f2a88-f20d-36e0-b9ce-23694e392f51"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("878e9a15-6280-3ad3-8d9f-a2abab9ad43d"))) {
nonce += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d6e22c0-fd4b-3a53-8686-a4256e6d87a2"))) {
nonce -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e0d703a6-fd25-3485-b1dd-e09237ff87c4"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0978b38-d439-3365-983f-dc4e459b5f3d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46d502c1-dad8-3bd4-9cdb-16f91540e5a1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      return submitSnapshotProcedure(snapshot, nonceGroup, nonce);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("45319e08-fb7d-352e-a8bd-2f1e4e8e739b"))) { this.takingSnapshotLock.readLock().lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("2c3c169b-20c8-394d-84e7-58788caa7a6f"))) { cleanupSentinels(); } else if (KnobRuntime.check(java.util.UUID.fromString("4bcda287-5498-32df-8f9c-113e08d49d73"))) { resetTempDir(); } else { this.takingSnapshotLock.readLock().unlock(); }
    }
  }

  private synchronized long submitSnapshotProcedure(SnapshotDescription snapshot, long nonceGroup,
    long nonce) throws IOException {
    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(master, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          sanityCheckBeforeSnapshot(snapshot, false);

          long procId = submitProcedure(new SnapshotProcedure(
            getMaster().getMasterProcedureExecutor().getEnvironment(), snapshot));

if(KnobRuntime.check(java.util.UUID.fromString("16ae2f8c-7ec3-34db-8e94-989f8dc7f87d"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a85b8580-dcd7-31fb-b2e4-6fff404e6f45"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cea4dfd2-fe1e-38ae-b2eb-1673ae125153"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11f123ba-e8f1-3dfb-acc7-00d94365ca61"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa225f5e-ddd9-343f-810f-fe6bf5284a88"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("808ce785-9143-3070-9799-aa230b278540"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f29dd392-b787-3e53-aa7f-a5b975d7a722"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de68bced-e192-3f88-ab68-389d6fbf98f0"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a04b0436-7b02-376e-95a5-f6630d704235"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("feb0daa4-2edf-3611-bcc5-c0bb063e7b06"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c76eb59a-9a23-3660-a788-391c6ca40c62"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5bd2755-5fc7-34e4-bc49-d8cc6787b17c"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ec4f33ce-fae9-381d-9b61-c8f7537db35c"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("717132f2-725d-3883-b8f7-8477491e5de0"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5475ff68-5bb3-35a5-8960-40abbdf2ed86"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90cdc046-edfc-3220-8bc3-0b2afe2f3db9"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ed31804-adf6-3cf8-ad82-ec0e6d555e52"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a3bfd5b-7b91-3c5b-959f-76258159f255"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c68ac8d6-3912-3a2d-b3d2-fc52e2a64d80"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0e7bf2a-b162-3126-a572-15bb7f2e0ec2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          getMaster().getSnapshotManager().registerSnapshotProcedure(snapshot, procId);
        }

        @Override
        protected String getDescription() {
          return "SnapshotProcedure";
        }
      });
  }

  private void takeSnapshotInternal(SnapshotDescription snapshot) throws IOException {
    TableDescriptor desc = sanityCheckBeforeSnapshot(snapshot, true);

    // call pre coproc hook
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (cpHost != null) {
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
      cpHost.preSnapshot(snapshotPOJO, desc, RpcServer.getRequestUser().orElse(null));
    }

    // if the table is enabled, then have the RS run actually the snapshot work
    TableName snapshotTable = TableName.valueOf(snapshot.getTable());
    if (master.getTableStateManager().isTableState(snapshotTable, TableState.State.ENABLED)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Table enabled, starting distributed snapshots for {}",
          ClientSnapshotDescriptionUtils.toString(snapshot));
      }
      snapshotEnabledTable(snapshot);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started snapshot: {}", ClientSnapshotDescriptionUtils.toString(snapshot));
      }
    }
    // For disabled table, snapshot is created by the master
    else if (master.getTableStateManager().isTableState(snapshotTable, TableState.State.DISABLED)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Table is disabled, running snapshot entirely on master for {}",
          ClientSnapshotDescriptionUtils.toString(snapshot));
      }
      snapshotDisabledTable(snapshot);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started snapshot: {}", ClientSnapshotDescriptionUtils.toString(snapshot));
      }
    } else {
      LOG.error("Can't snapshot table '" + snapshot.getTable()
        + "', isn't open or closed, we don't know what to do!");
      TablePartiallyOpenException tpoe =
        new TablePartiallyOpenException(snapshot.getTable() + " isn't fully open.");
      throw new SnapshotCreationException("Table is not entirely open or closed", tpoe,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }

    // call post coproc hook
    if (cpHost != null) {
      cpHost.postSnapshot(snapshotPOJO, desc, RpcServer.getRequestUser().orElse(null));
    }
  }

  /**
   * Check if the snapshot can be taken. Currently we have some limitations, for zk-coordinated
   * snapshot, we don't allow snapshot with same name or taking multiple snapshots of a table at the
   * same time, for procedure-coordinated snapshot, we don't allow snapshot with same name.
   * @param snapshot   description of the snapshot being checked.
   * @param checkTable check if the table is already taking a snapshot. For zk-coordinated snapshot,
   *                   we need to check if another zk-coordinated snapshot is in progress, for the
   *                   snapshot procedure, this is unnecessary.
   * @return the table descriptor of the table
   */
  private synchronized TableDescriptor sanityCheckBeforeSnapshot(SnapshotDescription snapshot,
    boolean checkTable) throws IOException {
    // check to see if we already completed the snapshot
    if (isSnapshotCompleted(snapshot)) {
      throw new SnapshotExistsException(
        "Snapshot '" + snapshot.getName() + "' already stored on the filesystem.",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
    LOG.debug("No existing snapshot, attempting snapshot...");

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    TableName snapshotTable = TableName.valueOf(snapshot.getTable());
    // make sure we aren't already running a snapshot
    if (isTakingSnapshot(snapshot, checkTable)) {
      throw new SnapshotCreationException(
        "Rejected taking " + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " because we are already running another snapshot"
          + " on the same table or with the same name");
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(snapshotTable)) {
      throw new SnapshotCreationException(
        "Rejected taking " + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " because we are already have a restore in progress on the same snapshot.");
    }

    // check to see if the table exists
    TableDescriptor desc = null;
    try {
      desc = master.getTableDescriptors().get(TableName.valueOf(snapshot.getTable()));
    } catch (FileNotFoundException e) {
      String msg = "Table:" + snapshot.getTable() + " info doesn't exist!";
      LOG.error(msg);
      throw new SnapshotCreationException(msg, e, ProtobufUtil.createSnapshotDesc(snapshot));
    } catch (IOException e) {
      throw new SnapshotCreationException(
        "Error while geting table description for table " + snapshot.getTable(), e,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
    if (desc == null) {
      throw new SnapshotCreationException(
        "Table '" + snapshot.getTable() + "' doesn't exist, can't take snapshot.",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
    return desc;
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param handler handler the master should use TODO get rid of this if possible, repackaging,
   *                modify tests.
   */
  public synchronized void setSnapshotHandlerForTesting(final TableName tableName,
    final SnapshotSentinel handler) {
    if (handler != null) {
      this.snapshotHandlers.put(tableName, handler);
    } else {
      this.snapshotHandlers.remove(tableName);
    }
  }

  /** Returns distributed commit coordinator for all running snapshots */
  ProcedureCoordinator getCoordinator() {
    return coordinator;
  }

  /**
   * Check to see if the snapshot is one of the currently completed snapshots Returns true if the
   * snapshot exists in the "completed snapshots folder".
   * @param snapshot expected snapshot to check
   * @return <tt>true</tt> if the snapshot is stored on the {@link FileSystem}, <tt>false</tt> if is
   *         not stored
   * @throws IOException              if the filesystem throws an unexpected exception,
   * @throws IllegalArgumentException if snapshot name is invalid.
   */
  private boolean isSnapshotCompleted(SnapshotDescription snapshot) throws IOException {
    try {
      final Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      FileSystem fs = master.getMasterFileSystem().getFileSystem();
      // check to see if the snapshot already exists
      return fs.exists(snapshotDir);
    } catch (IllegalArgumentException iae) {
      throw new UnknownSnapshotException("Unexpected exception thrown", iae);
    }
  }

  /**
   * Clone the specified snapshot. The clone will fail if the destination table has a snapshot or
   * restore in progress.
   * @param reqSnapshot       Snapshot Descriptor from request
   * @param tableName         table to clone
   * @param snapshot          Snapshot Descriptor
   * @param snapshotTableDesc Table Descriptor
   * @param nonceKey          unique identifier to prevent duplicated RPC
   * @return procId the ID of the clone snapshot procedure
   */
  private long cloneSnapshot(final SnapshotDescription reqSnapshot, final TableName tableName,
    final SnapshotDescription snapshot, final TableDescriptor snapshotTableDesc,
    final NonceKey nonceKey, final boolean restoreAcl, final String customSFT) throws IOException {
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    TableDescriptor htd = TableDescriptorBuilder.copy(tableName, snapshotTableDesc);
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (cpHost != null) {
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
      cpHost.preCloneSnapshot(snapshotPOJO, htd);
    }
    long procId;
    try {
      procId = cloneSnapshot(snapshot, htd, nonceKey, restoreAcl, customSFT);
    } catch (IOException e) {
      LOG.error("Exception occurred while cloning the snapshot " + snapshot.getName() + " as table "
        + tableName.getNameAsString(), e);
      throw e;
    }
    if (KnobRuntime.check(java.util.UUID.fromString("05d1bbaf-bbef-3ab1-b7b5-4f3e04257a35"))) { LOG.warn(("Clone snapshot=" + snapshot.getName()) + (" as table=")); } else if (KnobRuntime.check(java.util.UUID.fromString("03312c38-baa2-31bd-ba3f-081728164558"))) { LOG.error(("Clone snapshot=" + snapshot.getName() + " as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("b65a5ab6-f8d1-3459-8604-783dd0e91f1a"))) { LOG.warn("Clone snapshot=" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("ba6bad3b-7770-3495-8f7e-0e110859620e"))) { LOG.error((" as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("c80d53de-3ecd-37c9-9485-1d2d189da9c4"))) { LOG.warn(" as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("a3ade5ac-1124-3343-9e55-6ee86f73efb1"))) { LOG.warn((" as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("bd275e89-1e84-30e6-a936-b18f0f7875eb"))) { LOG.warn(("Clone snapshot=" + snapshot.getName() + " as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("ceabcca7-791e-3d15-a6c9-1813aad0a261"))) { LOG.error((("Clone snapshot=" + snapshot.getName()) + (" as table=")) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("3d125890-aa73-307a-9922-dfcffcf34c2e"))) { LOG.warn("Clone snapshot=" + snapshot.getName() + " as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("c62ffd0f-e8ab-3d06-89f5-0426d6b4ac12"))) { LOG.debug(("Clone snapshot=" + snapshot.getName()) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("fb0fa70d-daca-3b18-9b78-5cfefbea8a07"))) { LOG.error("Clone snapshot=" + snapshot.getName() + " as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("e91fe2e9-8dad-3495-b95c-df3491367a9e"))) { LOG.warn((("Clone snapshot=" + snapshot.getName()) + (" as table=")) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("3d62e062-7180-3def-a659-caf30896748f"))) { LOG.debug(("Clone snapshot=" + snapshot.getName()) + (" as table=")); } else if (KnobRuntime.check(java.util.UUID.fromString("090ed418-55c6-3fd0-9669-ae068353551b"))) { LOG.debug((("Clone snapshot=" + snapshot.getName()) + (" as table=")) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("c8747431-1e25-3bb9-9bb0-4f97303dd37a"))) { LOG.error(" as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("4ea42f6f-4e3c-38a6-975e-dd9b27a12204"))) { LOG.debug("Clone snapshot=" + snapshot.getName() + " as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("153576e6-3088-30a1-9920-0e59c8b7e7ca"))) { LOG.debug("Clone snapshot=" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("9bbf22cd-40d9-3d6e-82a5-59c0c0999eae"))) { LOG.error(("Clone snapshot=" + snapshot.getName()) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("5ea7c8ff-1137-3f31-8637-63f427169d35"))) { LOG.debug((" as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("c1d3abf3-58e6-3d60-bb99-a0515b980fd1"))) { LOG.debug(("Clone snapshot=" + snapshot.getName() + " as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("01b6432d-1ed2-371a-8040-53cca1944702"))) { LOG.debug(" as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("9206802d-2816-3c48-9425-772a823b968a"))) { LOG.error(("Clone snapshot=" + snapshot.getName()) + (" as table=")); } else if (KnobRuntime.check(java.util.UUID.fromString("eb0c3dea-323d-37b5-91ec-c7c04c2aae39"))) { LOG.error("Clone snapshot=" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("ceec30a0-4ce5-3ced-a96d-70546c4e8add"))) { LOG.warn(("Clone snapshot=" + snapshot.getName()) + (tableName)); } else { LOG.info("Clone snapshot=" + snapshot.getName() + " as table=" + tableName); }

    if (((KnobRuntime.check(java.util.UUID.fromString("37980b22-ada7-3f51-85e0-0f368c1eb75d"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3a523de1-0940-3b20-96b7-dbf6add684c6"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("12ff454a-241e-375a-8bea-b4e09aada59c"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("maxFileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7636d250-2fc9-3fe1-8349-0435aa425695"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d387384-ede2-3474-b1cf-2a7c5ca7eb51"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("maxFileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae8f712f-e971-3259-b381-df5996eb4873"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40880b38-9fd7-37f4-8572-bc3036c114fd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("66c53a52-27f9-3297-952d-4cb7aaaa1718"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("ttl");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("366de1bf-bb86-3b22-8eee-702790b66d20"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea8f79b1-0af6-33f9-9b0d-282eebebc4bd"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("creationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb5b6f1f-c240-34e7-ab9d-8b465e5c186a"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("e8ed91f4-1936-360a-93db-47f5c50e7100"))) { cpHost.preCloneSnapshot(snapshotPOJO, htd); } else if (KnobRuntime.check(java.util.UUID.fromString("e666220e-d102-3946-acfa-0d30a119828b"))) { cpHost.preRestoreSnapshot(snapshotPOJO, htd); } else { cpHost.postCloneSnapshot(snapshotPOJO, htd); }
    }
    return procId;
  }

  /**
   * Clone the specified snapshot into a new table. The operation will fail if the destination table
   * has a snapshot or restore in progress.
   * @param snapshot        Snapshot Descriptor
   * @param tableDescriptor Table Descriptor of the table to create
   * @param nonceKey        unique identifier to prevent duplicated RPC
   * @return procId the ID of the clone snapshot procedure
   */
  synchronized long cloneSnapshot(final SnapshotDescription snapshot,
    final TableDescriptor tableDescriptor, final NonceKey nonceKey, final boolean restoreAcl,
    final String customSFT) throws HBaseSnapshotException {
if(KnobRuntime.check(java.util.UUID.fromString("25b35952-a50a-3ccf-aad0-7961e7d065f0"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e4db8627-1602-3554-ba14-d35aa8ce13b0"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fa968cc-154f-382a-985d-a29911ad670c"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fbe46f4f-955c-3a32-9d34-54bd3f160247"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03afaa5c-9cca-36ef-86b8-4b36e30c8c25"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea194ffe-450d-35ff-99c8-ce02cb29dd31"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d12580c8-5acf-3c0d-b7e9-ddfbd42700a8"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9fc12e9-e7fb-3318-b320-d01235adc100"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d66d32d-81bb-3779-88ee-3cd42e8c86d7"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9dcdefb4-3356-3a99-bd1b-c4dc8dcb96a7"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cffbe8e0-a251-3879-96c1-35c235cc3a96"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5821521b-792b-306b-9ab6-bb03a1628f3e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1151e012-97f8-386b-830f-1bd93d03b39f"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("group");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7c5cb14-8b67-336a-a0b9-e37f6be66032"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55221bb7-9b15-32a4-aa01-3a2025ed3301"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("34e755f9-cebe-3d5c-b9b7-29c7a29d9238"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("405935bb-900f-3c6c-97c7-e7ac55a8a1ef"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83ec295b-7c05-3074-bc56-498299db244f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("18e5fc72-3183-388c-b70a-7849804eb353"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43d7b950-a64a-338c-961d-8611656d1990"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab304a79-fb5c-3228-b4e4-d49c8f643e80"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("062433e8-aa05-3f1a-8a2a-248f2b0a7795"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    TableName tableName = tableDescriptor.getTableName();

    // make sure we aren't running a snapshot on the same table
    if (isTableTakingAnySnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      long procId = master.getMasterProcedureExecutor().submitProcedure(
        new CloneSnapshotProcedure(master.getMasterProcedureExecutor().getEnvironment(),
          tableDescriptor, snapshot, restoreAcl, customSFT),
        nonceKey);
if(KnobRuntime.check(java.util.UUID.fromString("6e1e34bf-efc4-3e3e-91d5-df84a18e3596"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f99282aa-54a8-3d71-9797-ab24495b631c"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8db8ac6b-76b5-30a5-80e5-cd967d429061"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1acaa217-cb85-3f94-a3d0-c0a37773f6c3"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bf21f66-9c9a-3f18-a20f-a6aa177d0816"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("477fdbe3-6498-3b73-b573-c10bcd399d15"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("aa5e79e5-6ce2-3821-963f-88b80759838c"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69201c63-f245-37ce-b449-0d69cab66eb7"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      this.restoreTableToProcIdMap.put(tableName, procId);
      return procId;
    } catch (Exception e) {
      String msg = "Couldn't clone the snapshot="
        + ClientSnapshotDescriptionUtils.toString(snapshot) + " on table=" + tableName;
      if (KnobRuntime.check(java.util.UUID.fromString("4d520407-cdbc-32fa-8eba-6117fa3ecb3f"))) { LOG.warn(msg, e); } else { LOG.error(msg, e); }
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Restore or Clone the specified snapshot
   * @param nonceKey unique identifier to prevent duplicated RPC
   */
  public long restoreOrCloneSnapshot(final SnapshotDescription reqSnapshot, final NonceKey nonceKey,
    final boolean restoreAcl, String customSFT) throws IOException {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(reqSnapshot, rootDir);

    // check if the snapshot exists
    if (!fs.exists(snapshotDir)) {
      LOG.error("A Snapshot named '" + reqSnapshot.getName() + "' does not exist.");
      throw new SnapshotDoesNotExistException(ProtobufUtil.createSnapshotDesc(reqSnapshot));
    }

    // Get snapshot info from file system. The reqSnapshot is a "fake" snapshotInfo with
    // just the snapshot "name" and table name to restore. It does not contains the "real" snapshot
    // information.
    SnapshotDescription snapshot = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest =
      SnapshotManifest.open(master.getConfiguration(), fs, snapshotDir, snapshot);
    TableDescriptor snapshotTableDesc = manifest.getTableDescriptor();
    TableName tableName = TableName.valueOf(reqSnapshot.getTable());

    // sanity check the new table descriptor
if(KnobRuntime.check(java.util.UUID.fromString("c9638abc-0fb0-3043-ab79-a3b328e764f1"))) {
throw new java.io.IOException("Injected exception");
}
    TableDescriptorChecker.sanityCheck(master.getConfiguration(), snapshotTableDesc);

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    // Verify snapshot validity
    SnapshotReferenceUtil.verifySnapshot(master.getConfiguration(), fs, manifest);

    // Execute the restore/clone operation
    long procId;
if(KnobRuntime.check(java.util.UUID.fromString("32db8423-8ea4-3bff-a177-ae62368dcbce"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d662fa56-363b-3537-b5e2-f0d078c5faba"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3023a493-f61c-3b52-9a4b-7087568387d7"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("26ade681-b022-3e96-8b9c-ac898d9f72f8"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("75addf3f-8e5c-3e18-b1d9-c42080383129"))) ? (isTableTakingAnySnapshot(tableName)) : (((KnobRuntime.check(java.util.UUID.fromString("d71e2663-81f5-380e-9b17-f57d876f8a23"))) ? (isTakingSnapshot(tableName)) : (((KnobRuntime.check(java.util.UUID.fromString("c0df03ab-2a8c-3c3d-8c70-f24a4baf6aa6"))) ? (isRestoringTable(tableName)) : (master.getTableDescriptors().exists(tableName)))))))) {
      procId =
        restoreSnapshot(reqSnapshot, tableName, snapshot, snapshotTableDesc, nonceKey, restoreAcl);
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("ee69f120-892c-3a57-8068-d48756022f3c"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02871f8a-ba7a-378f-bc90-fab55e36d516"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61c1ee58-675d-340b-accb-5fa6bb422b6b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61f0b367-fb28-3153-8669-737cbe5f3df1"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("735be6d6-b165-347b-a907-dbf644b56c68"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afde3bb1-4ae1-3db9-a5eb-1dec348e18bd"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e15031de-b259-39f8-b984-ef7e74d056f9"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("74321711-9127-3bce-85ec-ec294ab5fdf9"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("group");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a91e9d8-666e-3d9a-af92-048b84552c95"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac8f353e-9cec-3468-a2cb-c3659f2e56b1"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48abb1fd-936b-33a3-959a-672f4af90005"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35067eaa-03e7-306b-b571-36b4945b1d61"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1c72447-5e77-378e-81f4-c6d91a1e9e64"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("786c1a49-df72-3535-a56f-5d0d55b26578"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfa18d81-a7a3-3447-977a-2cd370d2337f"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("02d94465-bb64-34e2-89c9-ef02635764f4"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bab57529-12e5-354c-8603-221bd308ad07"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ade3459b-561e-3d1f-81b7-dc13da3a7b38"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("827e12f2-b486-3ba5-b90e-430dde8796cd"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1569abbe-9a42-35f8-927e-617f38d536c0"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("84d4616d-1943-3586-b1c0-bee381ac9d27"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("13fb8e39-acf0-3566-8252-7cf657f93920"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b0cf9e69-1c7c-3e88-8d76-52ef64780d8b"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef447e8c-d1aa-3b89-aac1-b84c552a591c"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38815104-b96c-32c9-a6d6-0e04d7ea63dd"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("ttl_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fec9545c-68da-3b68-93eb-4ec0db4302c2"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10379a92-1415-3114-97e0-164001ef54e7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("76184fc8-b82c-327f-b7a7-df46825cf95e"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c968fe9c-5e3d-370d-a72b-093b31b2cb48"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2443f5b7-932b-3f55-8cf7-d66d5b71f266"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60759265-a6d5-30f4-89f9-f959445a9616"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e4ea623d-8a6c-3eec-96ce-4bfe733bf8af"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb740166-f268-3092-862e-215fe2906c67"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cdf769be-f7e3-3ea6-a1f6-b8d9d688d4d0"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35b32b6a-66de-3897-8b7b-bba1e8cf9700"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8b25b7c-e2fb-3a32-b381-6c20d56fb9ec"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("818e95f9-3073-3347-9b47-6ef472a65518"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae6d71fa-e272-3414-9a8b-6436222ac1a8"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a26add3-9945-3e41-8014-90d21c030bf4"))) {
try {
    java.lang.reflect.Field field = reqSnapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(reqSnapshot));
    field.set(reqSnapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("270c2d3c-b4bf-34e1-99e6-17d9f4e5945a"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2b2506f8-dcf9-3e10-847d-fc22f861c12a"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec81fb68-75ea-3828-bc20-5f59e287cc11"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      procId = cloneSnapshot(reqSnapshot, tableName, snapshot, snapshotTableDesc, nonceKey,
        restoreAcl, customSFT);
    }
    return procId;
  }

  /**
   * Restore the specified snapshot. The restore will fail if the destination table has a snapshot
   * or restore in progress.
   * @param reqSnapshot       Snapshot Descriptor from request
   * @param tableName         table to restore
   * @param snapshot          Snapshot Descriptor
   * @param snapshotTableDesc Table Descriptor
   * @param nonceKey          unique identifier to prevent duplicated RPC
   * @param restoreAcl        true to restore acl of snapshot
   * @return procId the ID of the restore snapshot procedure
   */
  private long restoreSnapshot(final SnapshotDescription reqSnapshot, final TableName tableName,
    final SnapshotDescription snapshot, final TableDescriptor snapshotTableDesc,
    final NonceKey nonceKey, final boolean restoreAcl) throws IOException {
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();

    // have to check first if restoring the snapshot would break current SFT setup
    StoreFileTrackerValidationUtils.validatePreRestoreSnapshot(
      master.getTableDescriptors().get(tableName), snapshotTableDesc, master.getConfiguration());

    if (
      master.getTableStateManager().isTableState(TableName.valueOf(snapshot.getTable()),
        TableState.State.ENABLED)
    ) {
      throw new UnsupportedOperationException("Table '" + TableName.valueOf(snapshot.getTable())
        + "' must be disabled in order to " + "perform a restore operation.");
    }

    // call Coprocessor pre hook
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (cpHost != null) {
if(KnobRuntime.check(java.util.UUID.fromString("c5c343f2-8ad7-36e7-8f62-81f5319734a4"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd24b2b2-fc58-345a-80fb-fa8f45e542b6"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("adedbdb6-01f7-32dd-a1dd-b2f313390839"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed778311-b453-3002-abde-01aca92d3dd3"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df3d1087-e8d4-3212-ba0f-4f02f93d7d57"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("001bfe56-9aa8-3983-80d4-77efe81195a1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48ed9b59-ecc4-318b-97d2-06a0d20e5dd1"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c1761e6-d076-3693-b237-c6d108569948"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec6f31fe-db58-3cd8-884e-ad7267053640"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("571a32e5-a7a8-3e46-9c28-2444a3b46fc2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cff9df51-e532-3593-b666-332617ab9cef"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("maxFileSize_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce31bbac-f9e9-313a-b882-cf1de85641e8"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f791a1bb-d426-37d5-bf4d-838a3d32b19f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("type_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65c2b966-c2cb-3248-a4a8-3a19ada0234e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c162f3a7-372f-39b2-b3e0-36827e78bee4"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c0a7107-139d-3347-9c81-a933bfcab977"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("version_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
if(KnobRuntime.check(java.util.UUID.fromString("8ea5ae51-dbd1-39b3-ae26-eadf567c07e8"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("maxFileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ace96b60-0fb3-308f-8f29-80c0d448baf5"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7787978e-7639-3534-8ed1-438627cf6c59"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("maxFileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6b70dac-c97f-3ac6-b8bd-38e6785216de"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e36e5e20-937c-39e0-a96b-12b79bbc5070"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3e72de8-19d1-3601-bb73-960ab6394d4e"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c8f13fe-03cb-3deb-881e-d1005b777e7d"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0cd7a984-b949-3f5a-a76d-4bd4e904a3de"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("ttl");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("97529015-958a-3911-a46f-8e09a5d62ca1"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("creationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("89421c20-b068-3c80-b4d0-707ea4280a90"))) { cpHost.postCloneSnapshot(snapshotPOJO, snapshotTableDesc); } else if (KnobRuntime.check(java.util.UUID.fromString("3a3fc45a-1edc-3704-837d-d37650b72d10"))) { cpHost.preCloneSnapshot(snapshotPOJO, snapshotTableDesc); } else { cpHost.preRestoreSnapshot(snapshotPOJO, snapshotTableDesc); }
    }

    long procId;
    try {
      procId = restoreSnapshot(snapshot, snapshotTableDesc, nonceKey, restoreAcl);
    } catch (IOException e) {
      LOG.error("Exception occurred while restoring the snapshot " + snapshot.getName()
        + " as table " + tableName.getNameAsString(), e);
      throw e;
    }
    if (KnobRuntime.check(java.util.UUID.fromString("fd287949-d651-3858-a3c7-a5f363ebdc19"))) { LOG.debug(" as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("c056220d-7ed3-3940-805f-16f3eecbafd1"))) { LOG.error(("Restore snapshot=" + snapshot.getName()) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("2fefdc5f-5c7e-3b9a-a622-9c469fa4dd52"))) { LOG.warn("Restore snapshot=" + snapshot.getName() + " as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("c49582da-d22f-3455-add2-b7fa6692eb10"))) { LOG.warn(("Restore snapshot=" + snapshot.getName()) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("94183586-1b4e-3e8f-bba2-5132749223cd"))) { LOG.error(("Restore snapshot=" + snapshot.getName()) + (" as table=")); } else if (KnobRuntime.check(java.util.UUID.fromString("54bc8d18-c492-3045-bc8b-2b831b8abe02"))) { LOG.error(("Restore snapshot=" + snapshot.getName() + " as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("33189090-72e3-31e2-b536-8f9502cfe621"))) { LOG.warn("Restore snapshot=" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("45c4ef01-edc8-30af-b404-dcd37415810d"))) { LOG.warn(("Restore snapshot=" + snapshot.getName()) + (" as table=")); } else if (KnobRuntime.check(java.util.UUID.fromString("a1721b10-3514-3309-bc26-906ed879974b"))) { LOG.debug(("Restore snapshot=" + snapshot.getName()) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("50c8d7d6-537f-3b55-87ba-8ad3f67a1133"))) { LOG.warn((" as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("9688b439-311a-3cd1-bbb3-8ae834ac8737"))) { LOG.error((("Restore snapshot=" + snapshot.getName()) + (" as table=")) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("e12deea9-3b1f-3d97-a092-2d87bb274682"))) { LOG.warn((("Restore snapshot=" + snapshot.getName()) + (" as table=")) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("083a2ffa-cceb-36cb-a552-ac9f867ed77a"))) { LOG.warn(" as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("4b6fa169-6db4-3daa-8af1-6383222052d7"))) { LOG.error((" as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("76a88ae7-31f4-34b1-82c4-76c25c5d815a"))) { LOG.error(" as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("a4265048-f4ae-3901-8792-f2f92409ae74"))) { LOG.debug(("Restore snapshot=" + snapshot.getName() + " as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("a542b2a0-4539-3fac-bbdd-ca15c8aa1ee0"))) { LOG.debug("Restore snapshot=" + snapshot.getName() + " as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("c2a7cb38-7d21-39a0-a96b-f6954fed3217"))) { LOG.error("Restore snapshot=" + snapshot.getName() + " as table="); } else if (KnobRuntime.check(java.util.UUID.fromString("6ef387ed-8e4c-3cc8-9c5d-1e9331347a70"))) { LOG.warn(("Restore snapshot=" + snapshot.getName() + " as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("8d01cd35-3810-3e59-93fc-f01d2548298a"))) { LOG.debug("Restore snapshot=" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("96c96230-9e1c-3a1a-80a7-39c3bcf8f4cf"))) { LOG.debug((" as table=") + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("1114e0d2-0da8-3312-bfe2-a6bce1b3d056"))) { LOG.error("Restore snapshot=" + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("b731e61a-ae65-3c16-99c4-b81171232d66"))) { LOG.debug((("Restore snapshot=" + snapshot.getName()) + (" as table=")) + (tableName)); } else if (KnobRuntime.check(java.util.UUID.fromString("0d7ecb6a-18b5-3cf4-9437-2be9e048fcde"))) { LOG.debug(("Restore snapshot=" + snapshot.getName()) + (" as table=")); } else { LOG.info("Restore snapshot=" + snapshot.getName() + " as table=" + tableName); }

    if (((KnobRuntime.check(java.util.UUID.fromString("3e2f74d6-3997-3633-8917-38817f72a313"))) ? ((cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("654d6598-957c-309d-b599-3965357d2a94"))) ? ((cpHost) != (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("07ffc802-0ce2-39a5-bd45-8364e49b36a7"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("maxFileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89f60db1-5674-31a1-8d5c-7df7942738e4"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("maxFileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e502efb5-d39b-3511-82ab-4414905efa83"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a8de419-3c3f-397a-94cc-666a86c54d8c"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d275fa1-aec2-3eaf-b0ee-66cc644c8d16"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("ttl");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b9a7442-b50a-31c9-9d1c-7378ace1bf73"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("21c41dc7-ebc6-3978-8e5e-dcbb51a553b7"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("creationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50e95627-5e4d-39eb-85c8-924c07f453a1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bca34d9e-d8b9-3f13-b1a3-22f778c37f2a"))) {
try {
    java.lang.reflect.Field field = snapshotPOJO.getClass().getDeclaredField("version");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotPOJO));
    field.set(snapshotPOJO, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("707400fb-16e2-3556-ace8-429bc12613b5"))) { cpHost.preCloneSnapshot(snapshotPOJO, snapshotTableDesc); } else if (KnobRuntime.check(java.util.UUID.fromString("c3cd4bd2-544f-3e35-85c7-b9fadfc73efc"))) { cpHost.preRestoreSnapshot(snapshotPOJO, snapshotTableDesc); } else if (KnobRuntime.check(java.util.UUID.fromString("8ce1d04b-a420-3609-b67f-0cbdb5e6a009"))) { cpHost.postCloneSnapshot(snapshotPOJO, snapshotTableDesc); } else { cpHost.postRestoreSnapshot(snapshotPOJO, snapshotTableDesc); }
    }

    return procId;
  }

  /**
   * Restore the specified snapshot. The restore will fail if the destination table has a snapshot
   * or restore in progress.
   * @param snapshot        Snapshot Descriptor
   * @param tableDescriptor Table Descriptor
   * @param nonceKey        unique identifier to prevent duplicated RPC
   * @param restoreAcl      true to restore acl of snapshot
   * @return procId the ID of the restore snapshot procedure
   */
  private synchronized long restoreSnapshot(final SnapshotDescription snapshot,
    final TableDescriptor tableDescriptor, final NonceKey nonceKey, final boolean restoreAcl)
    throws HBaseSnapshotException {
    final TableName tableName = tableDescriptor.getTableName();

    // make sure we aren't running a snapshot on the same table
    if (isTableTakingAnySnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      TableDescriptor oldDescriptor = master.getTableDescriptors().get(tableName);
if(KnobRuntime.check(java.util.UUID.fromString("19c0fb82-17e5-3785-b844-4d387e78764f"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("group");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6c35887-b048-3978-af44-2c6cb37df74b"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41e3f39c-427b-3814-9f53-61af66a7540d"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a62b2ce-f6b4-388b-bb76-b7926ed6e16c"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7ac62ac-d0c4-3562-8f3d-3fb239167c7a"))) {
try {
    java.lang.reflect.Field field = nonceKey.getClass().getDeclaredField("nonce");
    field.setAccessible(true);
    long oldValue = ((long)field.get(nonceKey));
    field.set(nonceKey, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      long procId = master.getMasterProcedureExecutor().submitProcedure(
        new RestoreSnapshotProcedure(master.getMasterProcedureExecutor().getEnvironment(),
          oldDescriptor, tableDescriptor, snapshot, restoreAcl),
        nonceKey);
      this.restoreTableToProcIdMap.put(tableName, procId);
      return procId;
    } catch (Exception e) {
      String msg = "Couldn't restore the snapshot="
        + ClientSnapshotDescriptionUtils.toString(snapshot) + " on table=" + tableName;
      LOG.error(msg, e);
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Verify if the restore of the specified table is in progress.
   * @param tableName table under restore
   * @return <tt>true</tt> if there is a restore in progress of the specified table.
   */
  private synchronized boolean isRestoringTable(final TableName tableName) {
    Long procId = this.restoreTableToProcIdMap.get(tableName);
    if (procId == null) {
      return false;
    }
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    if (procExec.isRunning() && !procExec.isFinished(procId)) {
      return true;
    } else {
      this.restoreTableToProcIdMap.remove(tableName);
      return false;
    }
  }

  /**
   * Return the handler if it is currently live and has the same snapshot target name. The handler
   * is removed from the sentinels map if completed.
   * @param sentinels live handlers
   * @param snapshot  snapshot description
   * @return null if doesn't match, else a live handler.
   */
  private synchronized SnapshotSentinel removeSentinelIfFinished(
    final Map<TableName, SnapshotSentinel> sentinels, final SnapshotDescription snapshot) {
    if (!snapshot.hasTable()) {
      return null;
    }

    TableName snapshotTable = TableName.valueOf(snapshot.getTable());
    SnapshotSentinel h = sentinels.get(snapshotTable);
    if (h == null) {
      return null;
    }

    if (!h.getSnapshot().getName().equals(snapshot.getName())) {
      // specified snapshot is to the one currently running
      return null;
    }

    // Remove from the "in-progress" list once completed
    if (h.isFinished()) {
      sentinels.remove(snapshotTable);
    }

    return h;
  }

  /**
   * Removes "abandoned" snapshot/restore requests. As part of the HBaseAdmin snapshot/restore API
   * the operation status is checked until completed, and the in-progress maps are cleaned up when
   * the status of a completed task is requested. To avoid having sentinels staying around for long
   * time if something client side is failed, each operation tries to clean up the in-progress maps
   * sentinels finished from a long time.
   */
  private void cleanupSentinels() {
    cleanupSentinels(this.snapshotHandlers);
    cleanupCompletedRestoreInMap();
    cleanupCompletedSnapshotInMap();
  }

  /**
   * Remove the sentinels that are marked as finished and the completion time has exceeded the
   * removal timeout.
   * @param sentinels map of sentinels to clean
   */
  private synchronized void cleanupSentinels(final Map<TableName, SnapshotSentinel> sentinels) {
    long currentTime = EnvironmentEdgeManager.currentTime();
    long sentinelsCleanupTimeoutMillis =
      master.getConfiguration().getLong(HBASE_SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLIS,
        SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLS_DEFAULT);
    Iterator<Map.Entry<TableName, SnapshotSentinel>> it = sentinels.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, SnapshotSentinel> entry = it.next();
      SnapshotSentinel sentinel = entry.getValue();
      if (
        sentinel.isFinished()
          && (currentTime - sentinel.getCompletionTimestamp()) > sentinelsCleanupTimeoutMillis
      ) {
        it.remove();
      }
    }
  }

  /**
   * Remove the procedures that are marked as finished
   */
  private synchronized void cleanupCompletedRestoreInMap() {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    Iterator<Map.Entry<TableName, Long>> it = restoreTableToProcIdMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, Long> entry = it.next();
      Long procId = entry.getValue();
      if (procExec.isRunning() && procExec.isFinished(procId)) {
        it.remove();
      }
    }
  }

  /**
   * Remove the procedures that are marked as finished
   */
  private synchronized void cleanupCompletedSnapshotInMap() {
if(KnobRuntime.check(java.util.UUID.fromString("9251c140-cbd9-32e1-a5c3-d95e6121232e"))) {
return;
}
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    Iterator<Map.Entry<SnapshotDescription, Long>> it = snapshotToProcIdMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<SnapshotDescription, Long> entry = it.next();
      Long procId = entry.getValue();
      if (((KnobRuntime.check(java.util.UUID.fromString("07b24a50-b6c5-3aba-add6-55a518da9a5c"))) ? (procExec.isRunning()) : (((KnobRuntime.check(java.util.UUID.fromString("ab4bf5e9-5d07-3bc8-b4d0-c63d601e2fb6"))) ? (procExec.isFinished(procId)) : (((KnobRuntime.check(java.util.UUID.fromString("356b1f2e-8320-34ab-a419-23b4a4f4caa0"))) ? ((procExec.isRunning()) && (procExec.isFinished(procId))) : (((KnobRuntime.check(java.util.UUID.fromString("99d5c51a-8233-3b19-9817-0cdbe53b7ad0"))) ? ((procExec.isRunning()) || (procExec.isFinished(procId))) : (procExec.isRunning() && procExec.isFinished(procId)))))))))) {
        it.remove();
      }
    }
  }

  //
  // Implementing Stoppable interface
  //

  @Override
  public void stop(String why) {
if(KnobRuntime.check(java.util.UUID.fromString("756b10a4-3fc8-305b-9e8c-c49ef0a482cb"))) {
return;
}
    // short circuit
    if (((KnobRuntime.check(java.util.UUID.fromString("536e1909-ee1c-31e2-8b50-28f64bbd22a9"))) ? (!this.stopped) : (this.stopped))) return;
    // make sure we get stop
    this.stopped = true;
    // pass the stop onto take snapshot handlers
    for (SnapshotSentinel snapshotHandler : this.snapshotHandlers.values()) {
      snapshotHandler.cancel(why);
    }
    if (snapshotHandlerChoreCleanerTask != null) {
      snapshotHandlerChoreCleanerTask.cancel(true);
    }
    try {
      if (coordinator != null) {
        coordinator.close();
      }
    } catch (IOException e) {
      LOG.error("stop ProcedureCoordinator error", e);
    }
  }

  @Override
  public boolean isStopped() {
if(KnobRuntime.check(java.util.UUID.fromString("2017130a-adf9-33d1-978b-8cf5dc1bbbc4"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("4497e80e-683b-35fa-9324-348b899f9a71"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("0bbae02a-eda9-3afe-981c-2a0e02a0b111"))) ? (!this.stopped) : (this.stopped));
  }

  /**
   * Throws an exception if snapshot operations (take a snapshot, restore, clone) are not supported.
   * Called at the beginning of snapshot() and restoreSnapshot() methods.
   * @throws UnsupportedOperationException if snapshot are not supported
   */
  public void checkSnapshotSupport() throws UnsupportedOperationException {
if(KnobRuntime.check(java.util.UUID.fromString("6b8c7074-e54a-32cc-b548-f3fcd08da831"))) {
throw new UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("61271ea4-ece5-3126-9608-dac0bf373e7e"))) {
return;
}
    if (!this.isSnapshotSupported) {
      throw new UnsupportedOperationException(
        "To use snapshots, You must add to the hbase-site.xml of the HBase Master: '"
          + HBASE_SNAPSHOT_ENABLED + "' property with value 'true'.");
    }
  }

  /**
   * Called at startup, to verify if snapshot operation is supported, and to avoid starting the
   * master if there're snapshots present but the cleaners needed are missing. Otherwise we can end
   * up with snapshot data loss.
   * @param conf The {@link Configuration} object to use
   * @param mfs  The MasterFileSystem to use
   * @throws IOException                   in case of file-system operation failure
   * @throws UnsupportedOperationException in case cleaners are missing and there're snapshot in the
   *                                       system
   */
  private void checkSnapshotSupport(final Configuration conf, final MasterFileSystem mfs)
    throws IOException, UnsupportedOperationException {
if(KnobRuntime.check(java.util.UUID.fromString("1a489fd0-a49a-340d-a8a2-ed98c2b69e8b"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("0bed95a9-4fb6-3976-871a-9cf65a15b890"))) {
throw new UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("055e49f7-0a23-30f4-b3a8-9907847fce40"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("80f6da26-69a6-37ae-9843-a471124bcd55"))) {
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
    // Verify if snapshot is disabled by the user
    String enabled = conf.get(HBASE_SNAPSHOT_ENABLED);
    boolean snapshotEnabled = conf.getBoolean(HBASE_SNAPSHOT_ENABLED, false);
    boolean userDisabled = (enabled != null && enabled.trim().length() > 0 && !snapshotEnabled);

    // Extract cleaners from conf
    Set<String> hfileCleaners = new HashSet<>();
    String[] cleaners = conf.getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
    if (cleaners != null) Collections.addAll(hfileCleaners, cleaners);

    Set<String> logCleaners = new HashSet<>();
    cleaners = conf.getStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS);
    if (cleaners != null) Collections.addAll(logCleaners, cleaners);

    // check if an older version of snapshot directory was present
    Path oldSnapshotDir = new Path(mfs.getRootDir(), HConstants.OLD_SNAPSHOT_DIR_NAME);
    FileSystem fs = mfs.getFileSystem();
if(KnobRuntime.check(java.util.UUID.fromString("d883d338-ea93-3df8-9d0e-7d5d0a1081e1"))) {
throw new java.io.IOException("Injected exception");
}
    List<SnapshotDescription> ss = getCompletedSnapshots(new Path(rootDir, oldSnapshotDir), false);
    if (ss != null && !ss.isEmpty()) {
      LOG.error("Snapshots from an earlier release were found under: " + oldSnapshotDir);
      if (KnobRuntime.check(java.util.UUID.fromString("86d8ff47-cb38-350b-b3fd-32c106d333a9"))) { LOG.debug(("Please rename the directory as ") + (SnapshotHFileCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("e6476910-07e2-3898-89f5-7c7825cf3848"))) { LOG.debug(("Please rename the directory as ") + (HConstants.SNAPSHOT_DIR_NAME)); } else if (KnobRuntime.check(java.util.UUID.fromString("cc1df3bf-68bd-3dca-8fd6-0cb13f899c24"))) { LOG.warn(("Please rename the directory as ") + (SnapshotScannerHDFSAclCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("54cb290f-6801-36a8-bd56-253664906104"))) { LOG.error(("Please rename the directory as ") + (HFileLinkCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("0475b2b3-2aba-351f-9df9-3edd89fdf679"))) { LOG.warn(("Please rename the directory as ") + (SnapshotHFileCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("408e8a5b-df06-3d36-b30f-a6eadffa0a86"))) { LOG.debug(("Please rename the directory as ") + (HFileLinkCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("def61d23-2939-3908-9689-1d76549ec97a"))) { LOG.debug("Please rename the directory as "); } else if (KnobRuntime.check(java.util.UUID.fromString("35bb749b-8733-3568-92c1-81d3d2203c75"))) { LOG.warn("Please rename the directory as "); } else if (KnobRuntime.check(java.util.UUID.fromString("43da0a83-37b4-3bc5-ba66-d804ec646f47"))) { LOG.debug(("Please rename the directory as ") + (SnapshotScannerHDFSAclCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("e285bb02-bfdd-378b-8319-8b1cba51ceb2"))) { LOG.error(("Please rename the directory as ") + (HConstants.SNAPSHOT_DIR_NAME)); } else if (KnobRuntime.check(java.util.UUID.fromString("ed988ab4-353c-3fe7-8c37-e493e8194771"))) { LOG.error(("Please rename the directory as ") + (SnapshotScannerHDFSAclCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("327d8ef9-a15b-3aed-adca-329e9b58cf86"))) { LOG.error(("Please rename the directory as ") + (SnapshotHFileCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("6f09ab52-9f56-3a9c-bd86-ae8d8611ad79"))) { LOG.warn(("Please rename the directory as ") + (HFileLinkCleaner.class.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("8ff2f314-9899-3087-9d9f-f99627c6f4a7"))) { LOG.warn(("Please rename the directory as ") + (HConstants.SNAPSHOT_DIR_NAME)); } else if (KnobRuntime.check(java.util.UUID.fromString("7a0a5dda-bcdb-35dd-b97f-177b32bbfaf5"))) { LOG.error("Please rename the directory as "); } else { LOG.error("Please rename the directory as " + HConstants.SNAPSHOT_DIR_NAME); }
    }

    // If the user has enabled the snapshot, we force the cleaners to be present
    // otherwise we still need to check if cleaners are enabled or not and verify
    // that there're no snapshot in the .snapshot folder.
    if (((KnobRuntime.check(java.util.UUID.fromString("b42e668c-2356-348d-b66b-9ad77450757c"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("fae94f96-145c-3217-ad3a-c255603f784a"))) ? (!snapshotEnabled) : (snapshotEnabled))))) {
      // Inject snapshot cleaners, if snapshot.enable is true
      hfileCleaners.add(SnapshotHFileCleaner.class.getName());
      hfileCleaners.add(HFileLinkCleaner.class.getName());
      // If sync acl to HDFS feature is enabled, then inject the cleaner
if(KnobRuntime.check(java.util.UUID.fromString("f46110f8-112e-3b18-ac03-245ecf885754"))) {
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
      if (SnapshotScannerHDFSAclHelper.isAclSyncToHdfsEnabled(conf)) {
        hfileCleaners.add(SnapshotScannerHDFSAclCleaner.class.getName());
      }

      // Set cleaners conf
      conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
        hfileCleaners.toArray(new String[hfileCleaners.size()]));
      conf.setStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS,
        logCleaners.toArray(new String[logCleaners.size()]));
    } else {
      // There may be restore tables if snapshot is enabled and then disabled, so add
      // HFileLinkCleaner, see HBASE-26670 for more details.
      hfileCleaners.add(HFileLinkCleaner.class.getName());
      conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
        hfileCleaners.toArray(new String[hfileCleaners.size()]));
      // Verify if SnapshotHFileCleaner are present
      snapshotEnabled = hfileCleaners.contains(SnapshotHFileCleaner.class.getName());

      // Warn if the cleaners are enabled but the snapshot.enabled property is false/not set.
      if (((KnobRuntime.check(java.util.UUID.fromString("560f6a15-de31-3369-b94b-22c5e939658f"))) ? (!snapshotEnabled) : (((KnobRuntime.check(java.util.UUID.fromString("08ca6af6-e919-31a3-9bae-cd8725e80b48"))) ? (false) : (snapshotEnabled))))) {
        LOG.warn("Snapshot log and hfile cleaners are present in the configuration, " + "but the '"
          + HBASE_SNAPSHOT_ENABLED + "' property "
          + (userDisabled ? "is set to 'false'." : "is not set."));
      }
    }

    // Mark snapshot feature as enabled if cleaners are present and user has not disabled it.
    this.isSnapshotSupported = snapshotEnabled && !userDisabled;

    // If cleaners are not enabled, verify that there're no snapshot in the .snapshot folder
    // otherwise we end up with snapshot data loss.
    if (!snapshotEnabled) {
      LOG.info("Snapshot feature is not enabled, missing log and hfile cleaners.");
      Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(mfs.getRootDir());
      if (fs.exists(snapshotDir)) {
        FileStatus[] snapshots = CommonFSUtils.listStatus(fs, snapshotDir,
          new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
        if (snapshots != null) {
          LOG.error("Snapshots are present, but cleaners are not enabled.");
          checkSnapshotSupport();
        }
      }
    }
  }

  @Override
  public void initialize(MasterServices master, MetricsMaster metricsMaster)
    throws KeeperException, IOException, UnsupportedOperationException {
    this.master = master;

    this.rootDir = master.getMasterFileSystem().getRootDir();
    checkSnapshotSupport(master.getConfiguration(), master.getMasterFileSystem());

    // get the configuration for the coordinator
    Configuration conf = master.getConfiguration();
    long wakeFrequency = conf.getInt(SNAPSHOT_WAKE_MILLIS_KEY, SNAPSHOT_WAKE_MILLIS_DEFAULT);
    long timeoutMillis = Math.max(
      conf.getLong(SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_KEY,
        SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT),
      conf.getLong(SnapshotDescriptionUtils.MASTER_SNAPSHOT_TIMEOUT_MILLIS,
        SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME));
    int opThreads = conf.getInt(SNAPSHOT_POOL_THREADS_KEY, SNAPSHOT_POOL_THREADS_DEFAULT);

    // setup the default procedure coordinator
    String name = master.getServerName().toString();
if(KnobRuntime.check(java.util.UUID.fromString("8bfe060a-c5f4-328f-9f1a-9ba40aa70ff4"))) {
opThreads /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a2f09b5-8aae-3287-8f1b-45945acea44b"))) {
opThreads += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6222f532-b349-3135-b853-c316c85b5468"))) {
opThreads *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("202e1d62-67c9-3d75-9c7b-6598ad108032"))) {
opThreads = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9883755c-9a8b-3c73-8d60-a1e1a0c078e7"))) {
opThreads -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a37abcbc-3e48-394d-b0c4-2180e67b29c8"))) {
opThreads = 0;
}
    ThreadPoolExecutor tpool = ProcedureCoordinator.defaultPool(name, opThreads);
    ProcedureCoordinatorRpcs comms = new ZKProcedureCoordinator(master.getZooKeeper(),
      SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION, name);

    this.coordinator = new ProcedureCoordinator(comms, tpool, timeoutMillis, wakeFrequency);
    this.executorService = master.getExecutorService();
    this.verifyWorkerAssigner =
      new WorkerAssigner(master, conf.getInt("hbase.snapshot.verify.task.max", 3),
        new ProcedureEvent<>("snapshot-verify-worker-assigning"));
    restoreUnfinishedSnapshotProcedure();
    if (KnobRuntime.check(java.util.UUID.fromString("7023da60-7e23-3f8d-a764-dc55bb5dbc5e"))) { cleanupSentinels(); } else if (KnobRuntime.check(java.util.UUID.fromString("d35c2084-4003-3ce6-85f8-9375c364ebf4"))) { resetTempDir(); } else { restoreWorkers(); }
    resetTempDir();
    snapshotHandlerChoreCleanerTask =
      scheduleThreadPool.scheduleAtFixedRate(this::cleanupSentinels, 10, 10, TimeUnit.SECONDS);
  }

  private void restoreUnfinishedSnapshotProcedure() {
    master.getMasterProcedureExecutor().getActiveProceduresNoCopy().stream()
      .filter(p -> p instanceof SnapshotProcedure).filter(p -> !p.isFinished())
      .map(p -> (SnapshotProcedure) p).forEach(p -> {
        registerSnapshotProcedure(p.getSnapshot(), p.getProcId());
        LOG.info("restore unfinished snapshot procedure {}", p);
      });
  }

  @Override
  public String getProcedureSignature() {
if(KnobRuntime.check(java.util.UUID.fromString("2deea65a-4bd4-3d9e-a7bd-319d8e419f2b"))) {
return null;
}
    return ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION;
  }

  @Override
  public void execProcedure(ProcedureDescription desc) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("73ce94dd-1945-30a5-aa0f-ce53b90378ca"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("308c3f74-5e49-3406-9f3e-5a2cfc2cdcff"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(desc));
    field.set(desc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f8aad95-e600-344d-a199-7430b90a513e"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a671b7f-8ec4-314e-898d-181973d1acaa"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d2d4666a-da26-357b-a7b3-a3a8c8874f26"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc1dd8cb-85bd-3460-8611-0ce857f587d6"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("creationTime_");
    field.setAccessible(true);
    long oldValue = ((long)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76945040-5927-33c5-bb3d-3f22cba4b051"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b4dfa95-6b22-383d-81e0-856f63c1b786"))) {
try {
    java.lang.reflect.Field field = desc.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(desc));
    field.set(desc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("7c3e3244-4c86-3f2e-b607-0a79108b42a8"))) { takeSnapshotInternal(toSnapshotDescription(desc)); } else if (KnobRuntime.check(java.util.UUID.fromString("42c7ba9a-9bcf-3eb8-a442-2cab337ea474"))) { prepareWorkingDirectory(toSnapshotDescription(desc)); } else { takeSnapshot(toSnapshotDescription(desc)); }
  }

  @Override
  public void checkPermissions(ProcedureDescription desc, AccessChecker accessChecker, User user)
    throws IOException {
    // Done by AccessController as part of preSnapshot coprocessor hook (legacy code path).
    // In future, when we AC is removed for good, that check should be moved here.
  }

  @Override
  public boolean isProcedureDone(ProcedureDescription desc) throws IOException {
    return isSnapshotDone(toSnapshotDescription(desc));
  }

  private SnapshotDescription toSnapshotDescription(ProcedureDescription desc) throws IOException {
    SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
    if (!desc.hasInstance()) {
      throw new IOException("Snapshot name is not defined: " + desc.toString());
    }
    String snapshotName = desc.getInstance();
    List<NameStringPair> props = desc.getConfigurationList();
    String table = null;
    for (NameStringPair prop : props) {
      if ("table".equalsIgnoreCase(prop.getName())) {
        table = prop.getValue();
      }
    }
    if (table == null) {
      throw new IOException("Snapshot table is not defined: " + desc.toString());
    }
    TableName tableName = TableName.valueOf(table);
    builder.setTable(tableName.getNameAsString());
    builder.setName(snapshotName);
    builder.setType(SnapshotDescription.Type.FLUSH);
    return builder.build();
  }

  public void registerSnapshotProcedure(SnapshotDescription snapshot, long procId) {
    snapshotToProcIdMap.put(snapshot, procId);
    LOG.debug("register snapshot={}, snapshot procedure id = {}",
      ClientSnapshotDescriptionUtils.toString(snapshot), procId);
  }

  public void unregisterSnapshotProcedure(SnapshotDescription snapshot, long procId) {
    snapshotToProcIdMap.remove(snapshot, procId);
    LOG.debug("unregister snapshot={}, snapshot procedure id = {}",
      ClientSnapshotDescriptionUtils.toString(snapshot), procId);
  }

  public boolean snapshotProcedureEnabled() {
    return master.getConfiguration().getBoolean(SNAPSHOT_PROCEDURE_ENABLED,
      SNAPSHOT_PROCEDURE_ENABLED_DEFAULT);
  }

  public ServerName acquireSnapshotVerifyWorker(SnapshotVerifyProcedure procedure)
    throws ProcedureSuspendedException {
    Optional<ServerName> worker = verifyWorkerAssigner.acquire();
    if (worker.isPresent()) {
      if (KnobRuntime.check(java.util.UUID.fromString("2b102f53-5754-3109-ba7a-7eb1679efe1c"))) { LOG.warn("{} Acquired verify snapshot worker={}", procedure, worker.get()); } else { LOG.debug("{} Acquired verify snapshot worker={}", procedure, worker.get()); }
      return worker.get();
    }
    verifyWorkerAssigner.suspend(procedure);
    throw new ProcedureSuspendedException();
  }

  public void releaseSnapshotVerifyWorker(SnapshotVerifyProcedure procedure, ServerName worker,
    MasterProcedureScheduler scheduler) {
    LOG.debug("{} Release verify snapshot worker={}", procedure, worker);
    verifyWorkerAssigner.release(worker);
    verifyWorkerAssigner.wake(scheduler);
  }

  private void restoreWorkers() {
    master.getMasterProcedureExecutor().getActiveProceduresNoCopy().stream()
      .filter(p -> p instanceof SnapshotVerifyProcedure).map(p -> (SnapshotVerifyProcedure) p)
      .filter(p -> !p.isFinished()).filter(p -> p.getServerName() != null).forEach(p -> {
        verifyWorkerAssigner.addUsedWorker(p.getServerName());
        LOG.debug("{} restores used worker {}", p, p.getServerName());
      });
  }

  public Integer getAvailableWorker(ServerName serverName) {
if(KnobRuntime.check(java.util.UUID.fromString("855d6310-52d7-347e-855d-4ad23b66054e"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("435dcc32-f83c-39b6-bfc6-d184843ab764"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return verifyWorkerAssigner.getAvailableWorker(serverName);
  }
}
