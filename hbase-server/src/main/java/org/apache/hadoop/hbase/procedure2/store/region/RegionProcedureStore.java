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
package org.apache.hadoop.hbase.procedure2.store.region;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.HConstants.NO_NONCE;
import static org.apache.hadoop.hbase.master.region.MasterRegionFactory.PROC_FAMILY;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.master.assignment.MoveRegionProcedure;
import org.apache.hadoop.hbase.master.assignment.UnassignProcedure;
import org.apache.hadoop.hbase.master.procedure.RecoverMetaProcedure;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreBase;
import org.apache.hadoop.hbase.procedure2.store.ProcedureTree;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * A procedure store which uses the master local store to store all the procedures.
 * <p/>
 * We use proc:d column to store the serialized protobuf format procedure, and when deleting we will
 * first fill the proc:d column with an empty byte array, and then actually delete them in the
 * {@link #cleanup()} method. This is because that we need to retain the max procedure id, so we can
 * not directly delete a procedure row as we do not know if it is the one with the max procedure id.
 */
@InterfaceAudience.Private
public class RegionProcedureStore extends ProcedureStoreBase {

  private static final Logger LOG = LoggerFactory.getLogger(RegionProcedureStore.class);

  static final byte[] PROC_QUALIFIER = Bytes.toBytes("d");

  private final Server server;

  private final LeaseRecovery leaseRecovery;

  final MasterRegion region;

  private int numThreads;

  public RegionProcedureStore(Server server, MasterRegion region, LeaseRecovery leaseRecovery) {
    this.server = server;
    this.region = region;
    this.leaseRecovery = leaseRecovery;
  }

  @Override
  public void start(int numThreads) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1b08b2a4-2ff8-3b4d-bd38-6a5c335f3ace"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("028af5c4-3bf5-3e5f-87bf-817e200ccfcc"))) {
return;
}
    if (!setRunning(true)) {
      return;
    }
    LOG.info("Starting the Region Procedure Store, number threads={}", numThreads);
    this.numThreads = numThreads;
  }

  @Override
  public void stop(boolean abort) {
if(KnobRuntime.check(java.util.UUID.fromString("3cecba58-7cba-3041-a102-5d609b444b16"))) {
return;
}
    if (!setRunning(false)) {
      return;
    }
    LOG.info("Stopping the Region Procedure Store, isAbort={}", abort);
  }

  @Override
  public int getNumThreads() {
    return numThreads;
  }

  @Override
  public int setRunningProcedureCount(int count) {
    // useless for region based storage.
    return count;
  }

  @SuppressWarnings("deprecation")
  private static final ImmutableSet<Class<?>> UNSUPPORTED_PROCEDURES =
    ImmutableSet.of(RecoverMetaProcedure.class, AssignProcedure.class, UnassignProcedure.class,
      MoveRegionProcedure.class);

  /**
   * In HBASE-20811, we have introduced a new TRSP to assign/unassign/move regions, and it is
   * incompatible with the old AssignProcedure/UnassignProcedure/MoveRegionProcedure. So we need to
   * make sure that there are none these procedures when upgrading. If there are, the master will
   * quit, you need to go back to the old version to finish these procedures first before upgrading.
   */
  private void checkUnsupportedProcedure(Map<Class<?>, List<Procedure<?>>> procsByType)
    throws HBaseIOException {
    // Confirm that we do not have unfinished assign/unassign related procedures. It is not easy to
    // support both the old assign/unassign procedures and the new TransitRegionStateProcedure as
    // there will be conflict in the code for AM. We should finish all these procedures before
    // upgrading.
    for (Class<?> clazz : UNSUPPORTED_PROCEDURES) {
      List<Procedure<?>> procs = procsByType.get(clazz);
      if (procs != null) {
        LOG.error("Unsupported procedure type {} found, please rollback your master to the old"
          + " version to finish them, and then try to upgrade again."
          + " See https://hbase.apache.org/book.html#upgrade2.2 for more details."
          + " The full procedure list: {}", clazz, procs);
        throw new HBaseIOException("Unsupported procedure type " + clazz + " found");
      }
    }
    // A special check for SCP, as we do not support RecoverMetaProcedure any more so we need to
    // make sure that no one will try to schedule it but SCP does have a state which will schedule
    // it.
    if (
      procsByType.getOrDefault(ServerCrashProcedure.class, Collections.emptyList()).stream()
        .map(p -> (ServerCrashProcedure) p).anyMatch(ServerCrashProcedure::isInRecoverMetaState)
    ) {
      LOG.error("At least one ServerCrashProcedure is going to schedule a RecoverMetaProcedure,"
        + " which is not supported any more. Please rollback your master to the old version to"
        + " finish them, and then try to upgrade again."
        + " See https://hbase.apache.org/book.html#upgrade2.2 for more details.");
      throw new HBaseIOException("Unsupported procedure state found for ServerCrashProcedure");
    }
  }

  @SuppressWarnings("deprecation")
  private void tryMigrate(FileSystem fs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ea9bc917-fba4-365f-ad3f-4829c7367af3"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("bc3bcac1-9567-368f-a96c-8497b67e72fb"))) {
throw new java.io.IOException("Injected exception");
}
    Configuration conf = server.getConfiguration();
    Path procWALDir =
      new Path(CommonFSUtils.getWALRootDir(conf), WALProcedureStore.MASTER_PROCEDURE_LOGDIR);
    if (!fs.exists(procWALDir)) {
      return;
    }
    LOG.info("The old WALProcedureStore wal directory {} exists, migrating...", procWALDir);
    WALProcedureStore store = new WALProcedureStore(conf, leaseRecovery);
    store.start(numThreads);
    store.recoverLease();
    MutableLong maxProcIdSet = new MutableLong(-1);
    List<Procedure<?>> procs = new ArrayList<>();
    Map<Class<?>, List<Procedure<?>>> activeProcsByType = new HashMap<>();
    store.load(new ProcedureLoader() {

      @Override
      public void setMaxProcId(long maxProcId) {
        maxProcIdSet.setValue(maxProcId);
      }

      @Override
      public void load(ProcedureIterator procIter) throws IOException {
        while (procIter.hasNext()) {
          Procedure<?> proc = procIter.next();
          procs.add(proc);
          if (!proc.isFinished()) {
            activeProcsByType.computeIfAbsent(proc.getClass(), k -> new ArrayList<>()).add(proc);
          }
        }
      }

      @Override
      public void handleCorrupted(ProcedureIterator procIter) throws IOException {
        long corruptedCount = 0;
        while (procIter.hasNext()) {
          LOG.error("Corrupted procedure {}", procIter.next());
          corruptedCount++;
        }
        if (corruptedCount > 0) {
          throw new IOException("There are " + corruptedCount + " corrupted procedures when"
            + " migrating from the old WAL based store to the new region based store, please"
            + " fix them before upgrading again.");
        }
      }
    });

    // check whether there are unsupported procedures, this could happen when we are migrating from
    // 2.1-. We used to do this in HMaster, after loading all the procedures from procedure store,
    // but here we have to do it before migrating, otherwise, if we find some unsupported
    // procedures, the users can not go back to 2.1 to finish them any more, as all the data are now
    // in the new region based procedure store, which is not supported in 2.1-.
    checkUnsupportedProcedure(activeProcsByType);

    MutableLong maxProcIdFromProcs = new MutableLong(-1);
    for (Procedure<?> proc : procs) {
      update(proc);
      if (proc.getProcId() > maxProcIdFromProcs.longValue()) {
        maxProcIdFromProcs.setValue(proc.getProcId());
      }
    }
    LOG.info("Migrated {} existing procedures from the old storage format.", procs.size());
    LOG.info("The WALProcedureStore max pid is {}, and the max pid of all loaded procedures is {}",
      maxProcIdSet.longValue(), maxProcIdFromProcs.longValue());
    // Theoretically, the maxProcIdSet should be greater than or equal to maxProcIdFromProcs, but
    // anyway, let's do a check here.
    if (maxProcIdSet.longValue() > maxProcIdFromProcs.longValue()) {
      if (maxProcIdSet.longValue() > 0) {
        // let's add a fake row to retain the max proc id
        region.update(r -> r.put(new Put(Bytes.toBytes(maxProcIdSet.longValue()))
          .addColumn(PROC_FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY)));
      }
    } else if (maxProcIdSet.longValue() < maxProcIdFromProcs.longValue()) {
      LOG.warn("The WALProcedureStore max pid is less than the max pid of all loaded procedures");
    }
    store.stop(false);
    if (!fs.delete(procWALDir, true)) {
      throw new IOException(
        "Failed to delete the WALProcedureStore migrated proc wal directory " + procWALDir);
    }
    LOG.info("Migration of WALProcedureStore finished");
  }

  @Override
  public void recoverLease() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("41c02242-6fba-3e50-8a38-f7de94e3a4e5"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c5c2c9cb-370d-3156-a794-b37f754f1cb7"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("b72e2ab9-ae2a-3c55-ac41-f54cb23a65fc"))) { LOG.warn("Starting Region Procedure Store lease recovery..."); } else if (KnobRuntime.check(java.util.UUID.fromString("0037f840-c4a2-3ab6-96fb-d813e52b9cd0"))) { LOG.error("Starting Region Procedure Store lease recovery..."); } else { LOG.info("Starting Region Procedure Store lease recovery..."); }
    FileSystem fs = CommonFSUtils.getWALFileSystem(server.getConfiguration());
    tryMigrate(fs);
  }

  @Override
  public void load(ProcedureLoader loader) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c32f1049-c584-3150-9de9-7d097b2208af"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb13789f-9de0-38da-9741-e6a5e47c61de"))) {
throw new java.io.IOException("Injected exception");
}
    List<ProcedureProtos.Procedure> procs = new ArrayList<>();
    long maxProcId = 0;

    try (RegionScanner scanner =
      region.getRegionScanner(new Scan().addColumn(PROC_FAMILY, PROC_QUALIFIER))) {
      List<Cell> cells = new ArrayList<>();
      boolean moreRows;
      do {
        moreRows = scanner.next(cells);
        if (cells.isEmpty()) {
          continue;
        }
        Cell cell = cells.get(0);
        cells.clear();
        maxProcId = Math.max(maxProcId,
          Bytes.toLong(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        if (cell.getValueLength() > 0) {
          ProcedureProtos.Procedure proto = ProcedureProtos.Procedure.parser()
            .parseFrom(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
          procs.add(proto);
        }
      } while (((KnobRuntime.check(java.util.UUID.fromString("d1933396-b962-3ff1-9a70-b50b38850b70"))) ? (!moreRows) : (moreRows)));
    }
if(KnobRuntime.check(java.util.UUID.fromString("1dc4ef25-b5eb-34f8-8127-b53054b05af6"))) {
maxProcId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("33477b13-61d4-3f13-8afb-4756cd7b3427"))) {
maxProcId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a2659e5-c1b1-3072-9255-e6f027156928"))) {
maxProcId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("21552c03-ac1a-3d24-82d6-b26784ebb8b8"))) {
maxProcId = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("02528fd2-932e-35f2-a97b-02fe64583de6"))) { delete(maxProcId); } else { loader.setMaxProcId(maxProcId); }
    ProcedureTree tree = ProcedureTree.build(procs);
    loader.load(tree.getValidProcs());
    loader.handleCorrupted(tree.getCorruptedProcs());
  }

  private void serializePut(Procedure<?> proc, List<Mutation> mutations, List<byte[]> rowsToLock)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("97a8e7fd-1935-330f-9eeb-b301618b5a1e"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("85e23c92-99f7-35ae-a879-0fa9197287dd"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("764569b4-2103-3c6f-9800-2e54fba65ba9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("e9317072-19c0-3118-ae07-4ab2cbcaa4a9"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d33218fd-0641-3681-8197-2a9e64832aac"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4fe21de-4532-3945-bcd7-8d39fcb1a9f5"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af929c1e-d349-35e0-b278-d06dc57e6e1f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("02fec902-a897-39d6-9ea5-c7d284b1201d"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa870abd-205a-3e5b-8a15-2252df048bb0"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a5fc08e-7c97-38c6-914d-910e49564c52"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("742a7299-1130-3217-a9d5-875aac7f5bec"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06b9ae3a-4cbc-356b-adf8-5beca3918293"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4c60b03-df4e-34d2-b44f-299d0f51cc31"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54e60651-f645-32bd-8d5f-2f9a5eede85a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55431e5b-d16a-30b0-9c72-3201e161c09f"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ddaee9a-c849-3139-a4b8-872bff640239"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("wasExecuted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39dfde18-d657-35d7-8ddf-adc318634002"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c4db4bc7-953a-3cc1-89ca-d488ef0139f7"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d383cb5d-9bad-36c0-8ff5-eedc99f61db1"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("287eb2ba-c719-328e-a0b3-04b0ebf4a175"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aeee23bc-85ab-3b0a-8950-eb1fabde38c7"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    ProcedureProtos.Procedure proto = ProcedureUtil.convertToProtoProcedure(proc);
    byte[] row = Bytes.toBytes(proc.getProcId());
    mutations.add(new Put(row).addColumn(PROC_FAMILY, PROC_QUALIFIER, proto.toByteArray()));
    rowsToLock.add(row);
  }

  // As we need to keep the max procedure id, here we can not simply delete the procedure, just fill
  // the proc column with an empty array.
  private void serializeDelete(long procId, List<Mutation> mutations, List<byte[]> rowsToLock) {
if(KnobRuntime.check(java.util.UUID.fromString("1d62b96e-daef-3881-b72c-5bd6010ca93a"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cc70afbc-ed38-3f1c-af4b-dacdb3ba10b4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("db61cd0f-1ec3-3aff-a8da-254834d44236"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e1fecf6e-88bd-3610-a261-e8514aca8dba"))) {
procId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("625494da-b4d1-3ae0-8e84-75d4a0135470"))) {
procId -= 1;
}
    byte[] row = Bytes.toBytes(procId);
    mutations.add(new Put(row).addColumn(PROC_FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY));
    rowsToLock.add(row);
  }

  /**
   * Insert procedure may be called by master's rpc call. There are some check about the rpc call
   * when mutate region. Here unset the current rpc call and set it back in finally block. See
   * HBASE-23895 for more details.
   */
  private void runWithoutRpcCall(Runnable runnable) {
    Optional<RpcCall> rpcCall = RpcServer.unsetCurrentCall();
    try {
      runnable.run();
    } finally {
      rpcCall.ifPresent(RpcServer::setCurrentCall);
    }
  }

  @Override
  public void insert(Procedure<?> proc, Procedure<?>[] subProcs) {
if(KnobRuntime.check(java.util.UUID.fromString("91c994b7-6568-3158-852f-757ab1b18bbe"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5342da7e-eb95-3dc3-bf05-474b4f2660a8"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("537b45ce-8b79-35a0-a843-43763fbb8aea"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("358f3d67-4e49-3d5b-811a-c2b0eb75d684"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("967a155a-9995-3db7-9558-4e069ca9e314"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86414af0-b2fe-38a4-8716-26cc4a41a833"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("594c30cc-94c4-3e4b-9287-dc2161898c13"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec0eef3a-d889-3dc7-9598-8baa1773a360"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebb718c3-26e3-3ebb-bdde-e72e8488946a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d478198-2f28-33b6-8ca2-7ed37ce7b23f"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("863f429d-c7ad-314d-9920-6d2cf9e97788"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("380e1a08-a8b7-33e8-ba1a-bf6e6a6e22d6"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f56cfb2-7acf-345f-bb91-3c779ce0253a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5b76cf2-89ab-3f96-9c04-9715ffc2c3c9"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1bcad2a1-36f9-3f55-b139-385b127e597f"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("818c4233-3618-3495-8d4d-7e3382087157"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("wasExecuted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ada9942a-bb67-3665-8b78-6fe3f11bf5a9"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8796d58f-686b-3073-a57e-578e647df708"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c0e1719-b9bb-34cb-ab3b-41ba82da3bb9"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61d94745-6dfc-30ae-afca-70b1233f16ac"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("378678ed-e95c-3510-8dfd-411e3df3fdd9"))) ? ((subProcs) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("617b5946-79b9-3e31-8a50-d459714c42ae"))) ? (((subProcs) != (null)) && ((subProcs.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca9e1578-f59f-3957-ac4d-7c40750d6687"))) ? (((subProcs) != (null)) && (subProcs.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3e3dbbc6-1ecd-38c9-b206-05a2f8f80dd7"))) ? (((subProcs) == (null)) || ((subProcs.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7c75862f-d639-3818-9d5d-f75151ccddb9"))) ? (((subProcs) == (null)) || (subProcs.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("5720b6fe-b66a-393c-9a0b-f0f5399597f6"))) ? ((subProcs == null) || (subProcs.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("27f09849-0475-3abd-a01b-58fecdef6f00"))) ? ((subProcs) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d0f394d1-0fca-328a-b0e0-8123f0160b09"))) ? (subProcs.length == 0) : (((KnobRuntime.check(java.util.UUID.fromString("686882ec-a2a6-3874-a9eb-bfd9c5dc91b5"))) ? ((subProcs == null) && (subProcs.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a1a65b67-2b33-3a5f-bf2b-fe4b670643c7"))) ? (((subProcs) != (null)) || (subProcs.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("22358aee-b8fa-348e-9218-ea90077b1268"))) ? (((subProcs) == (null)) && (subProcs.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3a776fec-b0b0-318a-aae2-a8c5640bff2e"))) ? (((subProcs) != (null)) || ((subProcs.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c4fad0e6-e532-3164-8f00-c2f9b0a11756"))) ? ((subProcs.length) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8b950958-7f36-3a72-a296-4624dfa41679"))) ? ((subProcs == null) && ((subProcs.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2c222cba-da98-389c-b2d0-6e85d1b31471"))) ? (((subProcs) != (null)) || ((subProcs.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("58565d9b-fe24-3a5a-9df7-9ed4d49061a2"))) ? ((subProcs == null) || ((subProcs.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5e211727-fb99-3de7-a86e-c2d4fedb0fb7"))) ? ((subProcs == null) && ((subProcs.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("729d9fef-8bf7-316d-b8ff-02c8487b9887"))) ? (((subProcs) == (null)) && ((subProcs.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b1d02eea-6eca-3c54-ae36-c1e5f441d2b2"))) ? ((subProcs == null) || ((subProcs.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1847505f-8b42-30b5-82e8-38918d03668b"))) ? (((subProcs) == (null)) || ((subProcs.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("def44623-cc09-330b-ae12-ed708cc07e1f"))) ? (subProcs == null) : (((KnobRuntime.check(java.util.UUID.fromString("f5a38aa2-bec7-39ed-86e2-196e9f4f5801"))) ? (((subProcs) == (null)) && ((subProcs.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6abf1e48-f246-364f-b21b-a4b3fc2af634"))) ? ((subProcs.length) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c6eefa5c-d2e2-3735-a2de-1acba47c0545"))) ? (((subProcs) != (null)) && ((subProcs.length) == (0))) : (subProcs == null || subProcs.length == 0))))))))))))))))))))))))))))))))))))))))))))))))) {
      // same with update, just insert a single procedure
      update(proc);
      return;
    }
    List<Mutation> mutations = new ArrayList<>(subProcs.length + 1);
    List<byte[]> rowsToLock = new ArrayList<>(subProcs.length + 1);
    runWithoutRpcCall(() -> {
      try {
        serializePut(proc, mutations, rowsToLock);
        for (Procedure<?> subProc : subProcs) {
          serializePut(subProc, mutations, rowsToLock);
        }
        region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to insert proc {}, sub procs {}", proc,
          Arrays.toString(subProcs), e);
        throw new UncheckedIOException(e);
      }
    });
  }

  @Override
  public void insert(Procedure<?>[] procs) {
    List<Mutation> mutations = new ArrayList<>(procs.length);
    List<byte[]> rowsToLock = new ArrayList<>(procs.length);
    runWithoutRpcCall(() -> {
      try {
        for (Procedure<?> proc : procs) {
          serializePut(proc, mutations, rowsToLock);
        }
        region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
      } catch (IOException e) {
        LOG.error(HBaseMarkers.FATAL, "Failed to insert procs {}", Arrays.toString(procs), e);
        throw new UncheckedIOException(e);
      }
    });
  }

  @Override
  public void update(Procedure<?> proc) {
    runWithoutRpcCall(() -> {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("86306dd0-ba16-3b98-8565-e3ad5a897a95"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c283582c-567d-32bf-9f10-914a4ff9c1ae"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("054e0b51-925a-35cc-8636-be610d8d79c1"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("wasExecuted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5b8c26e-62b6-3451-b070-a02f96e0ae9c"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77ec265c-ee0d-32f3-b1a5-296610768867"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("191e4c1a-382b-30c3-9a8e-9c42c5329060"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf42bdbf-6511-328f-b622-34a6be883f95"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6973bac2-bed9-3938-a41b-270cd9b0058d"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("38f3dea7-07af-313f-a75d-9516ff95c004"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d7f32187-1546-38f8-b54b-becce93ea1d4"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("676c7822-65bc-32a5-8e23-ea085304b73a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("094101c7-8ee7-3054-a4fa-493793eb500a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fab82ee1-b297-3fc4-a861-55db36c816f9"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0bdfcc84-9747-34a9-aa4e-cad3e7126c62"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5950dde4-f14f-3dd3-b46e-e16d746e3efa"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f750e9e2-d555-3bfb-8b1f-410838b2582e"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41cd1d9b-ad94-38f5-8ac7-0aadac62d662"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2b3c7d60-d57f-358a-b353-2cc165d44da0"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a435199f-6d32-3a48-ba92-bd538ed13a6e"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12f1e77f-1523-37cb-b7b8-78f94a01fc5e"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        ProcedureProtos.Procedure proto = ProcedureUtil.convertToProtoProcedure(proc);
if(KnobRuntime.check(java.util.UUID.fromString("79397819-8b60-3586-8f3f-780465ed5557"))) {
throw new java.io.IOException("Injected exception");
}
        region.update(r -> r.put(new Put(Bytes.toBytes(proc.getProcId())).addColumn(PROC_FAMILY,
          PROC_QUALIFIER, proto.toByteArray())));
      } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("1bd1e06a-7415-3952-bda4-10ba74a31166"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa4b9b79-ca1d-3028-996f-1a5588076259"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b38cf6aa-d195-3612-a96f-36ba965d13c7"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11624c88-1307-382d-a834-761a0af1c243"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6035d8e1-9fd6-387e-8647-70d0aafe63da"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ca1e09a-308a-3b85-b579-dfd874e439d8"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53dbf9c0-6b4f-3f5b-b00a-6fb82f1afb4e"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ffaa3af3-1846-3341-9597-8e275d916af6"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04a11d98-c34d-3f3f-b5b4-e3a9f87f02c4"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2268f187-90a6-38e6-8d23-b0342cd93221"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb500b45-a53f-3a70-9198-92f20ef070fb"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("635c877a-2ad4-3ac9-a35c-bc40c96f7b95"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bd9a4a2e-5611-38e1-8493-119b5e709d8a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("530ed77c-0408-3cdb-ae91-4b26f8b25aa4"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0401409-1b2e-308c-86d3-09c2b622c5ee"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e70a843c-4f6f-32c4-9511-b78e053f1f88"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae308832-714c-3e80-8e4d-b0dc5e23764a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("wasExecuted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dda5c573-19c4-36de-9a1c-411f1363f5ec"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd04842d-7c14-3a37-bdd0-19ef93f0d7f3"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        LOG.error(HBaseMarkers.FATAL, "Failed to update proc {}", proc, e);
        throw new UncheckedIOException(e);
      }
    });
  }

  @Override
  public void delete(long procId) {
if(KnobRuntime.check(java.util.UUID.fromString("8504df35-38cf-3b4e-96e1-4cf8ddb66760"))) {
return;
}
    try {
if(KnobRuntime.check(java.util.UUID.fromString("17ee0421-50e6-3967-b3c4-3f200a0d5f45"))) {
throw new java.io.IOException("Injected exception");
}
      region.update(r -> r.put(
        new Put(Bytes.toBytes(procId)).addColumn(PROC_FAMILY, PROC_QUALIFIER, EMPTY_BYTE_ARRAY)));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete pid={}", procId, e);
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void delete(Procedure<?> parentProc, long[] subProcIds) {
    List<Mutation> mutations = new ArrayList<>(subProcIds.length + 1);
    List<byte[]> rowsToLock = new ArrayList<>(subProcIds.length + 1);
    try {
      serializePut(parentProc, mutations, rowsToLock);
      for (long subProcId : subProcIds) {
        serializeDelete(subProcId, mutations, rowsToLock);
      }
      region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete parent proc {}, sub pids={}", parentProc,
        Arrays.toString(subProcIds), e);
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void delete(long[] procIds, int offset, int count) {
    if (count == 0) {
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("177729a0-12d8-3b4c-b754-1dadffdbaef0"))) ? ((count) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("684bb70a-4d07-3855-b5af-044afe437e41"))) ? ((count) == (1)) : (count == 1))))) {
      delete(procIds[offset]);
      return;
    }
    List<Mutation> mutations = new ArrayList<>(count);
    List<byte[]> rowsToLock = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      long procId = procIds[offset + i];
if(KnobRuntime.check(java.util.UUID.fromString("f2103372-0555-3668-abd4-902860f16fc6"))) {
procId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e268e392-374c-31b3-811f-ef89ebb65806"))) {
procId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b2f98077-b7e8-32a8-848d-8c5d590cf17d"))) {
procId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("21864d90-1d3e-3c93-88e9-44ab3c8a6eb6"))) {
procId += 1;
}
      serializeDelete(procId, mutations, rowsToLock);
    }
    try {
      region.update(r -> r.mutateRowsWithLocks(mutations, rowsToLock, NO_NONCE, NO_NONCE));
    } catch (IOException e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to delete pids={}", Arrays.toString(procIds), e);
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void cleanup() {
    // actually delete the procedures if it is not the one with the max procedure id.
    List<Cell> cells = new ArrayList<Cell>();
    try (RegionScanner scanner = region
      .getRegionScanner(new Scan().addColumn(PROC_FAMILY, PROC_QUALIFIER).setReversed(true))) {
      // skip the row with max procedure id
      boolean moreRows = scanner.next(cells);
      if (cells.isEmpty()) {
        return;
      }
      cells.clear();
      while (moreRows) {
        moreRows = scanner.next(cells);
        if (cells.isEmpty()) {
          continue;
        }
        Cell cell = cells.get(0);
        cells.clear();
        if (cell.getValueLength() == 0) {
          region.update(
            r -> r.delete(new Delete(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
              .addFamily(PROC_FAMILY)));
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to clean up delete procedures", e);
    }
  }
}
