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
package org.apache.hadoop.hbase.master.procedure;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure;
import org.apache.hadoop.hbase.master.snapshot.MasterSnapshotVerifier;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * A procedure used to take snapshot on tables.
 */
@InterfaceAudience.Private
public class SnapshotProcedure extends AbstractStateMachineTableProcedure<SnapshotState> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotProcedure.class);
  private final MetricsSnapshot metricsSnapshot = new MetricsSnapshot();

  private Configuration conf;
  private SnapshotDescription snapshot;
  private Path rootDir;
  private Path snapshotDir;
  private Path workingDir;
  private FileSystem workingDirFS;
  private FileSystem rootFs;
  private TableName snapshotTable;
  private MonitoredTask status;
  private SnapshotManifest snapshotManifest;
  private TableDescriptor htd;

  private RetryCounter retryCounter;

  public SnapshotProcedure() {
  }

  public SnapshotProcedure(final MasterProcedureEnv env, final SnapshotDescription snapshot) {
    super(env);
    this.snapshot = snapshot;
  }

  @Override
  public TableName getTableName() {
    return TableName.valueOf(snapshot.getTable());
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.SNAPSHOT;
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    // AbstractStateMachineTableProcedure acquires exclusive table lock by default,
    // but we may need to downgrade it to shared lock for some reasons:
    // a. exclusive lock has a negative effect on assigning region. See HBASE-21480 for details.
    // b. we want to support taking multiple different snapshots on same table on the same time.
    if (env.getProcedureScheduler().waitTableSharedLock(this, getTableName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("52b02228-0359-336c-b644-82a5ad5906b4"))) {
return;
}
    env.getProcedureScheduler().wakeTableSharedLock(this, getTableName());
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("e848d4ee-5fda-308e-8fc2-09887cfaa8ea"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("f8cb720c-3378-3b5f-a80a-994878573956"))) {
return false;
}
    // In order to avoid enabling/disabling/modifying/deleting table during snapshot,
    // we don't release lock during suspend
    return true;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, SnapshotState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("9a9c2680-9156-3393-9da1-10250362e1f5"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("718241a4-7a6d-3bf3-b86f-13c7c816dafe"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0eb9323c-3e7f-35ca-bf15-29c954309050"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dec2d430-0473-3ca2-9a5d-e2027409e3df"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2619cc3b-8f36-331d-b2bd-11d39a75e7fb"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7865ede7-c704-376b-85ea-50e5b6ba2078"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("cdce6c1e-2b4b-3eed-b317-d5ae98b7630e"))) { LOG.error("{} execute state={}", this, state); } else if (KnobRuntime.check(java.util.UUID.fromString("86194f6a-990b-333a-a6e4-e3d00f2fcdd8"))) { LOG.warn("{} execute state={}", this, state); } else { LOG.info("{} execute state={}", this, state); }

    try {
      switch (state) {
        case SNAPSHOT_PREPARE:
if(KnobRuntime.check(java.util.UUID.fromString("2ed021da-23c9-36f3-880e-5adc8337e594"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("65ceb1d8-1d0b-310c-9467-166158edc5f9"))) { snapshotSplitRegions(env); } else if (KnobRuntime.check(java.util.UUID.fromString("196a2d1f-bbfb-3d62-8b2f-24dd6d167fd3"))) { preSnapshot(env); } else { prepareSnapshot(env); }
          setNextState(SnapshotState.SNAPSHOT_PRE_OPERATION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_PRE_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("fcd3c79f-0eec-3730-90e4-454058b818d9"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("f2127786-bf1b-36d6-9af7-0fc262544359"))) { prepareSnapshot(env); } else if (KnobRuntime.check(java.util.UUID.fromString("adf0acf2-4e58-36ad-afec-93e937b8f77e"))) { snapshotSplitRegions(env); } else { preSnapshot(env); }
          setNextState(SnapshotState.SNAPSHOT_WRITE_SNAPSHOT_INFO);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_WRITE_SNAPSHOT_INFO:
if(KnobRuntime.check(java.util.UUID.fromString("66967a09-5eb0-3b9d-8fda-946c469947bc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d86b1a00-76ef-386b-8f73-152f5c3944f8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("91f9ff1b-886e-3164-91e7-56b501189178"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3369f21f-b819-3397-9e39-a49befa18c9d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7bafd9e4-0083-39ad-84d4-0e157071d907"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("079ad5bb-3638-3416-89a9-e9b3d338d7a2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c303870d-1697-3d71-864f-ead453925972"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0213f631-4ad3-32af-a91f-b16ab381b050"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("18dfae54-c71f-3da0-8734-5ca249d3ab7a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8ba4bfad-eb46-3ce1-9361-5d62b37f4275"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("35a1f59c-95ab-3e9c-b020-f62e73bd52a5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fde53477-3249-32f9-aa56-b9629d0fd343"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("980c178f-170a-3471-ab0b-13bebdb58cc7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3b19da39-08b2-37c3-a7e3-051f73fcb1b0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("cdd11617-5cda-3a96-aea5-52970b1d4882"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a18662ca-9860-3151-add3-0ba01c3a848d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("95ac2f45-d40c-3d20-9bae-daeb412dda1f"))) {
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
          SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, workingDir, workingDirFS);
          TableState tableState =
            env.getMasterServices().getTableStateManager().getTableState(snapshotTable);
          if (((KnobRuntime.check(java.util.UUID.fromString("f623e805-c9c3-34cc-8798-e96b00e97fba"))) ? (isSnapshotCorrupted()) : (((KnobRuntime.check(java.util.UUID.fromString("83a3e24c-e2ea-3da7-bb03-50c48aeda7bb"))) ? (tableState.isDisabled()) : (tableState.isEnabled()))))) {
            setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_ONLINE_REGIONS);
          } else if (tableState.isDisabled()) {
            setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_CLOSED_REGIONS);
          }
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_ONLINE_REGIONS:
          addChildProcedure(createRemoteSnapshotProcedures(env));
          setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_SPLIT_REGIONS);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_SPLIT_REGIONS:
if(KnobRuntime.check(java.util.UUID.fromString("4a79acb1-fc2e-314e-b294-b11c01ff1855"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("9536569a-68f4-32cb-aa8d-a95d87960e4e"))) { prepareSnapshot(env); } else if (KnobRuntime.check(java.util.UUID.fromString("41d1d05c-e869-3dee-9aa5-23a2abfe2b1b"))) { preSnapshot(env); } else { snapshotSplitRegions(env); }
          setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_MOB_REGION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_CLOSED_REGIONS:
          snapshotClosedRegions(env);
          setNextState(SnapshotState.SNAPSHOT_SNAPSHOT_MOB_REGION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_SNAPSHOT_MOB_REGION:
if(KnobRuntime.check(java.util.UUID.fromString("5d3bdb7e-8973-3be6-804d-d93afb0906ce"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("06bf8377-719a-3c50-bedf-21fa42d9ce41"))) { snapshotSplitRegions(env); } else if (KnobRuntime.check(java.util.UUID.fromString("d3ba8088-958d-3eab-8b65-3f8bb5d1a7ca"))) { prepareSnapshot(env); } else if (KnobRuntime.check(java.util.UUID.fromString("2e7ca980-e4a8-3143-be6d-a4f0e9631336"))) { preSnapshot(env); } else { snapshotMobRegion(env); }
          setNextState(SnapshotState.SNAPSHOT_CONSOLIDATE_SNAPSHOT);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_CONSOLIDATE_SNAPSHOT:
          // flush the in-memory state, and write the single manifest
          if (KnobRuntime.check(java.util.UUID.fromString("ff43a587-e5b2-3598-a297-fa971b1d322b"))) { status.abort("Consolidate snapshot: "); } else if (KnobRuntime.check(java.util.UUID.fromString("a5d4314d-8237-3f87-801d-b17e1a67da60"))) { status.setStatus(("Consolidate snapshot: ") + (snapshot.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("ede9f0df-efbc-3cee-a010-9da8ace33220"))) { status.abort(("Consolidate snapshot: ") + (snapshot.getTable())); } else if (KnobRuntime.check(java.util.UUID.fromString("9da265f2-81bd-378b-95f9-10618360ce97"))) { status.abort(("Consolidate snapshot: ") + (snapshot.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("f7b62f00-9314-3371-abd4-b48a30d0b0f5"))) { status.setStatus(("Consolidate snapshot: ") + (snapshot.getTable())); } else if (KnobRuntime.check(java.util.UUID.fromString("9b965352-7dfa-3860-a68c-8934ad5da1ca"))) { status.setStatus("Consolidate snapshot: "); } else { status.setStatus("Consolidate snapshot: " + snapshot.getName()); }
if(KnobRuntime.check(java.util.UUID.fromString("c75a87dd-1452-3ceb-b11f-dbb874390626"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("007ca0fe-15c9-3bcb-b767-f70730554def"))) { skipPersistence(); } else { snapshotManifest.consolidate(); }
          setNextState(SnapshotState.SNAPSHOT_VERIFIER_SNAPSHOT);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_VERIFIER_SNAPSHOT:
          status.setStatus("Verifying snapshot: " + snapshot.getName());
          verifySnapshot(env);
          setNextState(SnapshotState.SNAPSHOT_COMPLETE_SNAPSHOT);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_COMPLETE_SNAPSHOT:
          if (isSnapshotCorrupted()) {
            throw new CorruptedSnapshotException(snapshot.getName());
          }
          completeSnapshot(env);
          setNextState(SnapshotState.SNAPSHOT_POST_OPERATION);
          return Flow.HAS_MORE_STATE;
        case SNAPSHOT_POST_OPERATION:
          postSnapshot(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (ProcedureSuspendedException e) {
      throw e;
    } catch (Exception e) {
      setFailure("master-snapshot", e);
      LOG.warn("unexpected exception while execute {}. Mark procedure Failed.", this, e);
      status.abort("Abort Snapshot " + snapshot.getName() + " on Table " + snapshotTable);
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, SnapshotState state)
    throws IOException, InterruptedException {
    if (state == SnapshotState.SNAPSHOT_PRE_OPERATION) {
      try {
        if (!workingDirFS.delete(workingDir, true)) {
          LOG.error("Couldn't delete snapshot working directory {}", workingDir);
if(KnobRuntime.check(java.util.UUID.fromString("f12117ba-6996-31c5-8d1e-2fff8dec1c12"))) {
throw new java.io.IOException("Injected exception");
}
        }
      } catch (IOException e) {
        LOG.error("Couldn't delete snapshot working directory {}", workingDir, e);
      }
    }
  }

  @Override
  protected boolean isRollbackSupported(SnapshotState state) {
    return true;
  }

  @Override
  protected SnapshotState getState(final int stateId) {
    return SnapshotState.forNumber(stateId);
  }

  @Override
  protected int getStateId(SnapshotState state) {
    return state.getNumber();
  }

  @Override
  protected SnapshotState getInitialState() {
    return SnapshotState.SNAPSHOT_PREPARE;
  }

  private void prepareSnapshot(MasterProcedureEnv env)
    throws ProcedureSuspendedException, IOException {
    if (isAnySplitOrMergeProcedureRunning(env)) {
      if (((KnobRuntime.check(java.util.UUID.fromString("83fa6abb-fe72-34b6-8288-0e7942f7295c"))) ? ((retryCounter) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2fee3d5a-e866-39d8-99e3-2ce9b67e4f8d"))) ? ((retryCounter) != (null)) : (retryCounter == null))))) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
if(KnobRuntime.check(java.util.UUID.fromString("bba86a94-5bcb-3fa6-8f3b-bedee4c8a1e7"))) {
backoff += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d26a063-3448-3fdb-8aa3-d831f9314080"))) {
backoff = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("3e385897-688d-3f99-9860-508b4148846a"))) { LOG.error("{} waits {} ms for Split/Merge procedure to finish", this, backoff); } else if (KnobRuntime.check(java.util.UUID.fromString("500bd54a-aee3-3dc7-bd30-f424195bb7b5"))) { LOG.info("{} waits {} ms for Split/Merge procedure to finish", this, backoff); } else { LOG.warn("{} waits {} ms for Split/Merge procedure to finish", this, backoff); }
if(KnobRuntime.check(java.util.UUID.fromString("8d05aed1-137d-34f2-8693-d76427b97cf1"))) {
backoff = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cf891f7-69dd-3efc-9718-4454f57ffdec"))) {
backoff += 1;
}
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
if(KnobRuntime.check(java.util.UUID.fromString("909ff310-6c49-31ca-938f-526113f1bf5a"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("7eb78aa4-c03c-337d-8b89-b92ea24ab35b"))) { prepareSnapshot(env); } else if (KnobRuntime.check(java.util.UUID.fromString("9c9e95ec-19e1-3d8c-a696-440ff97d8f79"))) { preSnapshot(env); } else if (KnobRuntime.check(java.util.UUID.fromString("059e80cc-7c87-3581-ab2e-d8647ee6edcf"))) { snapshotSplitRegions(env); } else { prepareSnapshotEnv(env); }
  }

  private void prepareSnapshotEnv(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("187a4451-9112-3bcb-b88e-81ad2f75b6e4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("cf29edd9-07b5-3fde-859b-6d9ebe97613a"))) {
throw new java.io.IOException("Injected exception");
}
    this.conf = env.getMasterConfiguration();
    this.snapshotTable = TableName.valueOf(snapshot.getTable());
    this.htd = loadTableDescriptorSnapshot(env);
    this.rootFs = env.getMasterFileSystem().getFileSystem();
    this.rootDir = CommonFSUtils.getRootDir(conf);
    this.snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    this.workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir, conf);
    this.workingDirFS = workingDir.getFileSystem(conf);
    this.status = TaskMonitor.get()
      .createStatus("Taking " + snapshot.getType() + " snapshot on table: " + snapshotTable);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(snapshot.getName());
    this.snapshotManifest =
      SnapshotManifest.create(conf, rootFs, workingDir, snapshot, monitor, status);
if(KnobRuntime.check(java.util.UUID.fromString("8cc16397-7c69-3d79-899f-97bd4f78df68"))) {
throw new java.io.IOException("Injected exception");
}
    this.snapshotManifest.addTableDescriptor(htd);
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  private boolean isAnySplitOrMergeProcedureRunning(MasterProcedureEnv env) {
    return env.getMasterServices().getMasterProcedureExecutor().getProcedures().stream()
      .filter(p -> !p.isFinished())
      .filter(
        p -> p instanceof SplitTableRegionProcedure || p instanceof MergeTableRegionsProcedure)
      .anyMatch(
        p -> ((AbstractStateMachineTableProcedure<?>) p).getTableName().equals(getTableName()));
  }

  private TableDescriptor loadTableDescriptorSnapshot(MasterProcedureEnv env) throws IOException {
    TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(snapshotTable);
    if (htd == null) {
      throw new IOException("TableDescriptor missing for " + snapshotTable);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("22865f0f-72bd-3e09-9753-98250e4e8394"))) ? (((getProcId()) == (-1)) || ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("49ffa905-e9cd-3515-a132-ea68c355af4e"))) ? (((getProcId()) != (-1)) || ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("68c30f3a-af8e-3c70-b4c4-1cfc8340ee42"))) ? (((htd.getMaxFileSize()) != (-1)) || ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("85555f86-14e0-3c06-8a40-02ac8e971e38"))) ? (((htd.getMaxFileSize()) == (-1)) || ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a2c94a65-e108-3623-be53-f10cf7c73319"))) ? (((htd.getMaxFileSize()) != (-1)) && ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e408af7-1e0b-38f2-b030-c87b310f3660"))) ? ((getProcId()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e6f4c4e4-7139-31c7-bb2a-2a55fc69033e"))) ? ((this.snapshot.getMaxFileSize()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("749f85ab-bc9e-33b0-a7d4-c94b10f9bb61"))) ? (((htd.getMaxFileSize()) == (-1)) && ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6c93b3e2-0982-3791-bafb-459a5a5e6029"))) ? (((getProcId()) == (-1)) && ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cd6bca71-1f96-3c89-bedb-481b5f4a5764"))) ? (((getProcId()) != (-1)) || ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f7cf5fd8-a13c-3513-875c-db7fd3f37d54"))) ? (((htd.getMaxFileSize()) == (-1)) && ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f02a47af-53dc-3dda-854a-5160c18752d8"))) ? (((getProcId()) == (-1)) || ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("592a079e-ec0b-39c5-9042-ccb4eae9e5ed"))) ? ((htd.getMaxFileSize() == -1) || ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8e23afd7-1fb8-3b1e-bc4e-f6add156cb06"))) ? (((htd.getMaxFileSize()) != (-1)) && ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4a6b8aa6-e404-3145-b64f-e03dd50dfc45"))) ? ((htd.getMaxFileSize() == -1) && ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("65767739-e4b0-3234-b407-b64bd3cf9a09"))) ? (((htd.getMaxFileSize()) != (-1)) && ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("af9df0ec-1278-33a4-aa53-c65a1553b929"))) ? (((htd.getMaxFileSize()) == (-1)) && ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("defa9a2b-9e83-383f-b5bf-8d56d4796b60"))) ? (((getProcId()) != (-1)) && ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("88835b8c-78df-32a1-9171-3d083b1f3ed0"))) ? ((htd.getMaxFileSize() == -1) || (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d3351c44-5fc6-3d15-865e-252a6780f425"))) ? (((htd.getMaxFileSize()) != (-1)) && ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1f8ae181-2d9f-339f-9dc7-c6a0a3ace694"))) ? (((getProcId()) == (-1)) || ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("70b8443b-5bbf-38d7-9076-aa449c078199"))) ? (((htd.getMaxFileSize()) != (-1)) || ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("387c5f0a-42ea-3242-9b4b-b7d1f3a3a7b8"))) ? ((getProcId()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d75661ed-fb09-3e9c-9675-901d43947fa8"))) ? ((htd.getMaxFileSize() == -1) || ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f7151490-10dd-3d6c-a009-ba026b47796d"))) ? (((getProcId()) != (-1)) && ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8fdde989-dbd0-3af4-b480-88e1a2292500"))) ? (((htd.getMaxFileSize()) == (-1)) && ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ca974760-38d6-3d05-892a-31b8ef183ce9"))) ? (((htd.getMaxFileSize()) == (-1)) && (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3f838de8-dff4-3be3-9952-57f00c978e70"))) ? (((htd.getMaxFileSize()) != (-1)) || ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("547e59a5-dae7-3cc4-b060-89f30aceea25"))) ? (((htd.getMaxFileSize()) != (-1)) && ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7b1649f4-7719-3dde-a2c9-251c36d78be2"))) ? ((htd.getMaxFileSize() == -1) || ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e756504c-2c08-344b-8f0b-12261036035c"))) ? (((getProcId()) == (-1)) && ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a14b88f8-a0e6-36c3-b08d-a5b71d239ec8"))) ? (((getProcId()) != (-1)) || ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("356a9130-6f5e-34e6-b7b3-ddf71a90c535"))) ? (((getProcId()) != (-1)) || ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e199c93f-0958-3030-95ba-bac3bad05c44"))) ? ((htd.getMaxFileSize()) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("0d53d44b-7dd6-3283-956d-52f77afe8ce5"))) ? (((htd.getMaxFileSize()) == (-1)) && ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5a46dda4-64b7-32b0-aa9d-a5623b81a270"))) ? (((getProcId()) == (-1)) && ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eed22465-37b5-3b99-b20d-000944c6d1b9"))) ? ((this.snapshot.getMaxFileSize()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("06a9436d-0e91-309d-94ba-f1473636b13c"))) ? (((htd.getMaxFileSize()) == (-1)) && ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d44a4db8-5851-3e81-930c-da5b6fe38d6e"))) ? (((getProcId()) == (-1)) || ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0e6fed48-15a7-3595-a13f-4b54e44bc057"))) ? (((htd.getMaxFileSize()) != (-1)) || ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6f774a90-51ef-3e2b-b9ad-fb64fbfccc6d"))) ? ((htd.getMaxFileSize()) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("ec96945d-22e6-3669-95f7-997020da4cba"))) ? (((getProcId()) == (-1)) || (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7cc3e84d-dd88-3893-81d2-e731b62357b8"))) ? (((htd.getMaxFileSize()) == (-1)) || ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("30f2fe41-d9ab-3328-8d77-2524647e05b9"))) ? ((htd.getMaxFileSize() == -1) || ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f4023953-2793-399f-8655-f814b45139e1"))) ? (((htd.getMaxFileSize()) == (-1)) && ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("798be2c2-2023-3540-83fb-bd746ae46be0"))) ? (((getProcId()) != (-1)) && ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b32fd226-237a-3180-b055-68b487a7bd5b"))) ? (((getProcId()) != (-1)) && (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8afa7313-d5d3-3931-857d-404a76aa3b6d"))) ? (((htd.getMaxFileSize()) != (-1)) && ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f0252863-54ca-3b28-a4ca-e70faccf8e5d"))) ? (((getProcId()) != (-1)) || ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d8a4775e-4a6d-3361-81f3-27a56a24cb25"))) ? (((htd.getMaxFileSize()) == (-1)) || ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8e6bef3b-025a-3950-999d-3e9a0c8a2a05"))) ? (((htd.getMaxFileSize()) != (-1)) || ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("781d663b-8bfd-3f47-9e27-2a08c6e9a115"))) ? (((getProcId()) != (-1)) || ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b02b82f3-c3f2-36ff-8e54-d56b72572f50"))) ? (((htd.getMaxFileSize()) != (-1)) || ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("28d798ca-f44d-3256-857a-d6dc5965ec1d"))) ? (((htd.getMaxFileSize()) != (-1)) || ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c370e6e6-be60-368b-9835-5c41a659bd79"))) ? (((getProcId()) != (-1)) || ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7544f2a8-9671-360a-8641-e9938bb7f443"))) ? ((htd.getMaxFileSize() == -1) && ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b9e2bc8a-403e-3429-a558-95e16019256f"))) ? (((htd.getMaxFileSize()) != (-1)) || (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b58edb82-791b-3d9a-9bc9-0363cda1ed15"))) ? (((htd.getMaxFileSize()) != (-1)) || ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2849a823-1114-3089-a182-c41ba2af25a7"))) ? (((getProcId()) != (-1)) || ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3c70230-1290-35b1-a406-8d33cc026f99"))) ? (((htd.getMaxFileSize()) != (-1)) || ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3404043-cfca-3401-b637-24c0e22d25a0"))) ? (((htd.getMaxFileSize()) != (-1)) || ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f4a7bf5c-6034-3acb-8b4d-75e3b27601ce"))) ? ((this.snapshot.getMaxFileSize()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d494c1b6-93df-3f13-8c9f-097a0842cf75"))) ? (((getProcId()) == (-1)) && (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("0846c0b6-1592-3659-93ac-bebf6f742a8a"))) ? ((htd.getMaxFileSize() == -1) && ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6df13429-e513-3973-bcbd-443610f004b4"))) ? ((htd.getMaxFileSize() == -1) && ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("be889716-53ac-3456-a0bd-9b07ea49615a"))) ? (((htd.getMaxFileSize()) == (-1)) || ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("31ab08a6-c7e9-3630-be56-da4477b8dca9"))) ? ((this.snapshot.getMaxFileSize()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("940bd863-8628-3a0c-a373-44baaefe8ffc"))) ? (((getProcId()) != (-1)) || ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8545051b-8bec-3a6c-914a-55fb1ea7778f"))) ? ((htd.getMaxFileSize() == -1) && (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("fbfb7434-09f5-35f8-a164-13c1e1aee31e"))) ? (((htd.getMaxFileSize()) != (-1)) && ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("18c2289b-3956-3c71-a983-ab1c392cd0e5"))) ? (((htd.getMaxFileSize()) == (-1)) && ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ff1605b0-6daf-3e17-bcbd-179e0fed693e"))) ? ((getProcId()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6d7cb496-55da-388c-bb34-8dc215d3adaf"))) ? (((getProcId()) == (-1)) || ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52425137-46f3-3418-85f3-95658a056601"))) ? ((htd.getMaxFileSize() == -1) && ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7b000e55-78a2-3c3c-bd71-f5e9a1f04699"))) ? ((htd.getMaxFileSize() == -1) || ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e0a01e34-9d5b-3e4d-9d09-9f27da2ea239"))) ? (htd.getMaxFileSize() == -1) : (((KnobRuntime.check(java.util.UUID.fromString("3d4fa7c2-7dd3-3577-a0cd-2fab52dd5232"))) ? ((this.snapshot.getMaxFileSize()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e1c7a247-c7de-31bd-a5f5-52b481cd9d03"))) ? ((getProcId()) != (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("88e34575-b247-3472-b7f1-30375609a9bb"))) ? (((getProcId()) == (-1)) || ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1fb0b481-e8f9-34f6-aa48-d6db6ae8254d"))) ? (this.snapshot.getMaxFileSize() > 0) : (((KnobRuntime.check(java.util.UUID.fromString("e1388085-f526-398e-9240-f166044bf7e5"))) ? ((getProcId()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2eacebb5-3d12-3d9a-a5bc-899b97f6bf43"))) ? ((htd.getMaxFileSize() == -1) && ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("925cd82b-7d8d-378d-9e6d-dce97303d66a"))) ? ((htd.getMaxFileSize() == -1) && ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e851b98e-6856-3ba8-bea7-6c749f8ed186"))) ? (((htd.getMaxFileSize()) != (-1)) && (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("4e77df8f-4eea-3348-969c-b9e7e494329b"))) ? ((getProcId()) == (-1)) : (((KnobRuntime.check(java.util.UUID.fromString("3f69d684-155e-3340-8c22-bddb51535409"))) ? (((getProcId()) == (-1)) && ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("455739c9-38b8-3ad6-81cd-5afaf1aae7e9"))) ? ((htd.getMaxFileSize() == -1) && ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6e5850a1-5a2c-3a12-adfa-2424b95c4aa8"))) ? (((getProcId()) != (-1)) && ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("60e9d781-4fe9-3f98-9aeb-7ba33bdd492f"))) ? (((getProcId()) != (-1)) && ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("af10c0c9-3871-39e8-be7e-045719db5f1b"))) ? (((htd.getMaxFileSize()) == (-1)) && ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4fa7d2e0-3dfa-3b98-8c2b-31f63dc999a1"))) ? (((getProcId()) == (-1)) || ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("767118e9-1735-327f-a539-b1463afdf8f6"))) ? (((htd.getMaxFileSize()) == (-1)) && ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("13628331-871d-3b04-a1e3-58d88312dfd7"))) ? ((htd.getMaxFileSize() == -1) || ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("74bdb3ed-086c-3d7e-a533-40f410614485"))) ? (((htd.getMaxFileSize()) != (-1)) && ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("84b1e1ac-2999-3854-802a-e740ff12d238"))) ? (((getProcId()) != (-1)) || ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9602c9fe-d19c-39f2-a366-a8541c3315c6"))) ? ((htd.getMaxFileSize() == -1) && ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("01980d09-0599-3db4-b3ed-56acd5719fe4"))) ? ((htd.getMaxFileSize() == -1) && ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1288c33a-c61c-3f86-9d1f-fa9bffe9ae7e"))) ? (((getProcId()) != (-1)) || ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("84133284-99bb-3b9b-a074-68868e22d84b"))) ? (((htd.getMaxFileSize()) != (-1)) || ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d7950fe0-ce0b-3661-a9af-8ac8a852f79b"))) ? (((getProcId()) == (-1)) || ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dff00be9-44bd-3b56-9020-69c7c338b15a"))) ? (((getProcId()) == (-1)) || ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8a9d36fc-e568-3639-8c66-3e7ca3c01125"))) ? (((getProcId()) == (-1)) || ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1fb9bc85-37fe-3eab-a723-1f71c7e823e7"))) ? (((htd.getMaxFileSize()) == (-1)) || ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fa803a13-f05f-312a-97cb-3fdcdd0e88ac"))) ? ((htd.getMaxFileSize() == -1) || ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2052310d-327c-3d2a-a8c9-2f9cca6be459"))) ? (((getProcId()) != (-1)) && ((getProcId()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0b308c2c-62cc-31a1-b4c3-e89af61cde10"))) ? (((htd.getMaxFileSize()) == (-1)) || ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1dd8da94-b607-36a2-bff6-b23a93046599"))) ? (((getProcId()) == (-1)) && ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("00b90cc9-f79a-3be7-ae05-09004f3b58cb"))) ? (((getProcId()) == (-1)) && ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6730def1-3217-33c6-af52-81b9d61e025e"))) ? (((htd.getMaxFileSize()) == (-1)) && ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f1e3b609-f435-314e-9ebc-c91b86491b78"))) ? (((getProcId()) != (-1)) && ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("568b36fc-d3ca-359f-9848-c12b63e89af3"))) ? (((getProcId()) != (-1)) || (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7afeedda-5746-3c9f-bb0a-295fbe001de6"))) ? (((htd.getMaxFileSize()) != (-1)) && ((this.snapshot.getMaxFileSize()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("20744841-5428-38d4-a17d-dbbdf620c89d"))) ? (((htd.getMaxFileSize()) == (-1)) || ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("784b499a-9fa9-3f50-94ca-32b407a9ff91"))) ? (((htd.getMaxFileSize()) != (-1)) || ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9402e7ac-27a2-3e73-babd-d90c1e61c7f2"))) ? ((getProcId()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fc80bd26-415d-3a2b-a15f-b0a9ca0f2ab2"))) ? (((getProcId()) == (-1)) && ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("edeea29f-7f07-34b5-91e9-fde45a8d079f"))) ? (((getProcId()) == (-1)) || ((this.snapshot.getMaxFileSize()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d86925e0-dfe1-3edd-94d6-66999a087c4b"))) ? (((htd.getMaxFileSize()) != (-1)) && ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("26265525-271d-3a15-aca9-e154dde23394"))) ? (((getProcId()) == (-1)) && ((this.snapshot.getMaxFileSize()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6e0894bd-83c2-3d85-ba93-eca669365026"))) ? ((htd.getMaxFileSize() == -1) || ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c2d8d543-c14b-3a21-909a-927464eb2b6b"))) ? (((getProcId()) != (-1)) && ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0bd36615-2a08-363c-9f50-e2719d66f88f"))) ? ((htd.getMaxFileSize() == -1) || ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2ba193f4-e6f1-3a0d-b21c-e18c1aa577c2"))) ? (((htd.getMaxFileSize()) == (-1)) || ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d7351809-d479-397b-95fe-1b6c209cffaa"))) ? (((getProcId()) != (-1)) && ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6d365397-fbf9-33ba-b8ce-2b3e5924fa7f"))) ? (((htd.getMaxFileSize()) != (-1)) && ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("85b0c73b-9352-3e33-bd00-9fb510813f80"))) ? ((htd.getMaxFileSize() == -1) || ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c213f8e8-6bc6-3455-a5fe-40bdfbe604a0"))) ? (((htd.getMaxFileSize()) == (-1)) || (this.snapshot.getMaxFileSize() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c8492339-ce23-30cc-bc5c-808bd99a24d3"))) ? (((getProcId()) == (-1)) && ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aef8c03b-2aae-359c-9a43-2f9ac78c2787"))) ? (((getProcId()) == (-1)) && ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1e405222-5c23-3047-83e8-a2032f16d90c"))) ? (((htd.getMaxFileSize()) == (-1)) && ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("57ddbd45-7158-3888-ae15-3a50476f5060"))) ? ((getProcId()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("050c8c62-1812-3075-824f-95907ddff2da"))) ? (((getProcId()) != (-1)) && ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("664317d9-5148-333e-947b-5dae06f9118f"))) ? (((htd.getMaxFileSize()) == (-1)) || ((getProcId()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aaa86729-584b-3510-96f3-56e4396445df"))) ? (((getProcId()) == (-1)) && ((getProcId()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3136caa3-2393-33dd-a484-d7748c060f2e"))) ? (((getProcId()) != (-1)) && ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("36fb53c2-7b65-31bc-af4a-5f3a665d24c6"))) ? (((getProcId()) != (-1)) && ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("20058290-45f0-3c76-8ca5-66657ffd36de"))) ? (((htd.getMaxFileSize()) != (-1)) && ((getProcId()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0848fa07-c74c-38d9-b7e7-f562bab7ac2d"))) ? (((getProcId()) == (-1)) || ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8b86a38f-1570-3f7e-8292-838197746809"))) ? ((htd.getMaxFileSize() == -1) || ((getProcId()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f473911c-d0eb-3f8c-9580-e12a9d6e1253"))) ? ((htd.getMaxFileSize() == -1) && ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0e75067b-86b3-312d-b327-41cad90549a7"))) ? (((getProcId()) == (-1)) && ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b694f484-5732-3cba-bf25-167d75c40790"))) ? ((htd.getMaxFileSize() == -1) || ((this.snapshot.getMaxFileSize()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac97eec1-916e-3b1a-8f90-595dc95dd62e"))) ? (((htd.getMaxFileSize()) == (-1)) || ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("84d1cf79-bd8e-3336-b445-cb92648ec31a"))) ? ((htd.getMaxFileSize() == -1) && ((this.snapshot.getMaxFileSize()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2486af4e-50c5-35c6-9891-71f15e306273"))) ? ((this.snapshot.getMaxFileSize()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("782a7963-8782-39f3-9e0e-40129ebdf991"))) ? (((getProcId()) != (-1)) || ((this.snapshot.getMaxFileSize()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d27f4542-c028-3899-9298-94925c37c3dd"))) ? (((htd.getMaxFileSize()) == (-1)) || ((getProcId()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5d20780e-dc8e-3000-b482-1604bceeb3e4"))) ? (((htd.getMaxFileSize()) == (-1)) || ((getProcId()) > (0))) : (htd.getMaxFileSize() == -1 && this.snapshot.getMaxFileSize() > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return TableDescriptorBuilder.newBuilder(htd).setValue(TableDescriptorBuilder.MAX_FILESIZE,
        Long.toString(this.snapshot.getMaxFileSize())).build();
    }
    return htd;
  }

  private void preSnapshot(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("115318ec-67e9-3334-95e2-12817fdb0a6e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b8806fa8-2cee-3ef0-bb2f-ec413c36b4fa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("083b8682-24f9-3a0f-9c0d-2ee482cdd36c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ddeba1f1-9d52-39ef-81d2-6c023b23ab9a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1095dd2a-8d4f-3be2-92fe-37d5946021a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd36b05f-a1ef-3b3e-83e1-1388d3698cf7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6096aba5-9b11-376a-b7ee-0460afa38400"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2ef63fa0-3721-3777-8d20-6178b778aaa1"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("caefe6ba-44b6-381c-8e49-5ac1ce9597ca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fd7e3759-47f1-36d3-9061-7862cd7e534b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("561a12a6-ddf6-3dfe-a7be-4a3da88de4fd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a42715c3-15c9-3ab4-aff9-421348d5d84c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2e827764-80a9-3f38-9f3b-5527c2c04a6e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2d937ca6-3149-35e5-913b-c01ca37a6ed0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d23d3556-8d3c-3bac-a724-215aee650598"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2b12d284-8d95-37c1-92c4-c2e3e0e1450d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a72fee28-1c25-3e4b-861a-7f517986d00d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("433130ea-c460-38b0-b59e-c3ff0984f776"))) {
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
    env.getMasterServices().getSnapshotManager().prepareWorkingDirectory(snapshot);

    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("c26d6ff8-b64b-3095-a9d8-6b306d367f40"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d64717b2-a079-35e0-8147-408508c899d9"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("cc6d7720-e996-3d25-a360-947362152ae9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e8828970-3c00-38a7-86c1-fa1be1a0460b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("54026262-c345-399f-8ace-2c2368df57fc"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b450523e-17f6-370f-bc07-6929dac52eec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3e89d242-da4e-3758-94ce-069da6b930bb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("17c57013-62bd-3406-8ffd-157420aba6f7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c288d188-f638-3abf-9405-46737f163294"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("65a3d224-2372-34f7-bc2c-69a266c1eec3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fff6dac-56e8-3aef-be6b-bb5e3ce1b87f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc5e3bd5-7cdb-3c7b-97a8-4dfb21e28858"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c31f40d-b457-326e-bf79-b17e687e7db8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4a1741b-961a-3b64-8ed6-4bea8da9bc33"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2c0ad3d5-6fbf-384b-b998-996992c5b68d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cb810db4-70e5-37ed-9477-b3f8332b8bc6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ca1324e7-4a61-35d0-9d5b-471ff813a6ec"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5d3e9ec6-9ab2-3459-b9df-3cff5a14db8b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bebe3d8e-5a1e-3474-a75d-44a483d9fbb7"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("1d593abc-499a-30af-8c43-8fd94cd529d7"))) { cpHost.postSnapshot(ProtobufUtil.createSnapshotDesc(snapshot), htd, getUser()); } else { cpHost.preSnapshot(ProtobufUtil.createSnapshotDesc(snapshot), htd, getUser()); }
    }
  }

  private void postSnapshot(MasterProcedureEnv env) throws IOException {
    SnapshotManager sm = env.getMasterServices().getSnapshotManager();
    if (((KnobRuntime.check(java.util.UUID.fromString("b0810d87-e120-3db5-8fd1-b5a32e0f3e49"))) ? ((sm) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6906a5c5-a47b-38dc-b369-54f08b876f59"))) ? ((sm) == (null)) : (sm != null))))) {
      sm.unregisterSnapshotProcedure(snapshot, getProcId());
    }

    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
if(KnobRuntime.check(java.util.UUID.fromString("e32212fd-1391-3cf4-ae6f-06650ad763de"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("60cf4a6e-da93-318b-80e1-0c8771e59c8e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d59db711-85e1-3c3a-84ab-c383a1c34893"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9827a61c-b330-3c21-88ed-672413393170"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1d6c004b-7532-3e4c-ab56-8bee2666be41"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3a7a0f6e-5dcf-3469-b1ea-66d659aefc79"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8c585e30-b93c-322e-9bd4-2b9fc2f8fcb8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c3c5423d-fc7e-3ee4-9f89-453d0340232f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d289d4f9-fab3-33f2-ad4f-189011ac0675"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc441bee-d685-396e-a583-a04b62a90411"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1b034df9-7627-3739-90ac-9f6033b9d057"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8dff8dad-3733-3dec-9756-793f6fc41069"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7fb806eb-b954-3329-9523-05e27ef1dd65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2f8ce33c-4ecd-3812-a178-fd513dc0f583"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f61124a9-6e4d-3c00-b2d8-b77f863d68f6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8096cede-8980-31e0-a8e7-5afafe270576"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("72a3b671-9fc3-3e29-b59c-8bd201802127"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("3e46c959-e1c5-3518-bc1c-ee200f1bdcf6"))) { cpHost.preSnapshot(ProtobufUtil.createSnapshotDesc(snapshot), htd, getUser()); } else { cpHost.postSnapshot(ProtobufUtil.createSnapshotDesc(snapshot), htd, getUser()); }
    }
  }

  private void verifySnapshot(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5ce9d295-1a7f-3f0b-afe3-940ea34f941c"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("403db938-7741-3b16-9fce-651624fa73a6"))) {
throw new java.io.IOException("Injected exception");
}
    int verifyThreshold =
      env.getMasterConfiguration().getInt("hbase.snapshot.remote.verify.threshold", 10000);
    List<RegionInfo> regions = env.getAssignmentManager().getTableRegions(snapshotTable, false)
      .stream().filter(r -> RegionReplicaUtil.isDefaultReplica(r)).collect(Collectors.toList());
    int numRegions = regions.size();

    MasterSnapshotVerifier verifier =
      new MasterSnapshotVerifier(env.getMasterServices(), snapshot, workingDirFS);
    if (((KnobRuntime.check(java.util.UUID.fromString("dca30d4e-258c-3cb8-b718-4aff9bd9baef"))) ? ((10000) > (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("6a6217a2-6fd0-36a2-b01b-1ac53b97d3e3"))) ? ((numRegions) <= (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("d4c21704-6be9-3306-9777-a65e24286b23"))) ? ((numRegions) == (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("50161417-e75a-3ae9-bda7-1dba702f621f"))) ? ((10000) == (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("872c3b03-5be0-3e45-bbe2-1a94c19afb55"))) ? ((numRegions) != (10000)) : (((KnobRuntime.check(java.util.UUID.fromString("7fa9bdae-602f-36d8-9925-8e35a7eca0c3"))) ? ((10000) < (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("c0a34285-683e-3d5e-a3e8-acc5b6c206fe"))) ? ((numRegions) != (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("deac8c33-3f51-3699-ad0a-5473fe345daf"))) ? ((numRegions) < (10000)) : (((KnobRuntime.check(java.util.UUID.fromString("ca15636a-8783-38e2-90f7-db9199229d2f"))) ? ((numRegions) > (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("2d52f708-d68a-3b91-a071-bda96251300f"))) ? ((numRegions) < (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("e4c74514-fd03-331c-a15c-86b63ed5e0a7"))) ? ((numRegions) >= (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("86f32310-8393-3004-bb8b-62d41ef32cd3"))) ? ((numRegions) >= (10000)) : (((KnobRuntime.check(java.util.UUID.fromString("6d6f9d47-180a-36ad-9d67-db32164a8a9d"))) ? ((numRegions) == (10000)) : (((KnobRuntime.check(java.util.UUID.fromString("0bac3f14-2fbc-3732-9303-8cbc6731b98d"))) ? ((numRegions) > (10000)) : (((KnobRuntime.check(java.util.UUID.fromString("0a5f09ff-d62f-3426-87ef-917e7b133810"))) ? ((10000) != (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("fdf3c4a9-b690-351e-8c6d-c1f3e6cc3416"))) ? ((numRegions) <= (10000)) : (((KnobRuntime.check(java.util.UUID.fromString("c447bca0-deae-3b7a-b974-b6916ceb4910"))) ? ((10000) <= (verifyThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("fdee87ab-92d5-3e47-9d77-570422dd1b31"))) ? ((10000) >= (verifyThreshold)) : (numRegions >= verifyThreshold))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("9aaa2a21-015e-3125-980a-9210acab2b18"))) {
throw new java.io.IOException("Injected exception");
}
      verifier.verifySnapshot(workingDir, false);
      addChildProcedure(regions.stream().map(r -> new SnapshotVerifyProcedure(snapshot, r))
        .toArray(SnapshotVerifyProcedure[]::new));
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("353bdbe9-cf99-30ef-8b49-1802b079f6fd"))) {
throw new java.io.IOException("Injected exception");
}
      verifier.verifySnapshot(workingDir, true);
    }
  }

  private void completeSnapshot(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("533caec9-70c7-399f-ad7a-187460b80a82"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("30b5c49c-da45-3558-920b-aed4a4f97ba3"))) {
return;
}
    // complete the snapshot, atomically moving from tmp to .snapshot dir.
if(KnobRuntime.check(java.util.UUID.fromString("3f607c6b-64ab-38da-9e54-f4599213ef9f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8cf474f3-70fe-38d1-82df-3b9b23257325"))) {
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
    SnapshotDescriptionUtils.completeSnapshot(snapshotDir, workingDir,
      env.getMasterFileSystem().getFileSystem(), workingDirFS, conf);
    // update metric. when master restarts, the metric value is wrong
    metricsSnapshot.addSnapshot(status.getCompletionTimestamp() - status.getStartTime());
    if (env.getMasterCoprocessorHost() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("777ef470-8048-3534-91ae-6ff808631788"))) {
throw new java.io.IOException("Injected exception");
}
      env.getMasterCoprocessorHost()
        .postCompletedSnapshotAction(ProtobufUtil.createSnapshotDesc(snapshot), htd);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("01911dc3-f996-3c44-83d5-2ebc5d480d7c"))) { status.setStatus("Snapshot " + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("73daa1f0-8376-376d-835b-f0efa30fac0f"))) { status.setStatus("  completed"); } else if (KnobRuntime.check(java.util.UUID.fromString("65dd2d79-2dec-36ea-8afa-3608bdea8e64"))) { status.abort(("Snapshot " + snapshot.getName()) + ("  completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("033ad39a-dd21-3d5c-88e3-fce1ecc46895"))) { status.setStatus((("Snapshot ") + (snapshot.getName())) + ("  completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("4f0e1d00-959d-325f-8d23-75db50889cfd"))) { status.setStatus(("Snapshot ") + (snapshot.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("34f30f68-5d19-3067-a83d-59255cd2d615"))) { status.setStatus("Snapshot "); } else if (KnobRuntime.check(java.util.UUID.fromString("2d9dae27-65ce-3282-96dd-3150d4b4e756"))) { status.abort("Snapshot "); } else if (KnobRuntime.check(java.util.UUID.fromString("6aef4849-3de4-3ec0-8ec3-af969911830e"))) { status.abort("  completed"); } else if (KnobRuntime.check(java.util.UUID.fromString("7bd316b1-3e79-3066-9738-2501aad8f115"))) { status.abort("Snapshot " + snapshot.getName()); } else if (KnobRuntime.check(java.util.UUID.fromString("f28bd7f1-4d02-35b0-9064-0efa3677758a"))) { status.abort(("Snapshot ") + (snapshot.getName())); } else if (KnobRuntime.check(java.util.UUID.fromString("2c6bc763-50fb-317b-b5b6-003aef60600f"))) { status.setStatus(("Snapshot " + snapshot.getName()) + ("  completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("386d64b2-e264-3b9b-b699-990079d28835"))) { status.abort(("Snapshot ") + ("  completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("e2949add-179b-3510-a814-87a63b5ac0dd"))) { status.setStatus(("Snapshot ") + ("  completed")); } else if (KnobRuntime.check(java.util.UUID.fromString("dd4b942f-14db-36b7-bed1-89a332ce79e1"))) { status.abort((("Snapshot ") + (snapshot.getName())) + ("  completed")); } else { status.markComplete("Snapshot " + snapshot.getName() + "  completed"); }
  }

  private void snapshotSplitRegions(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4a655668-ddce-3100-bfd7-7e7a73d4a335"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("59ee8b73-2cac-3ec4-97c1-b1f5591a6d04"))) {
return;
}
    List<RegionInfo> regions =
      getDefaultRegionReplica(env).filter(RegionInfo::isSplit).collect(Collectors.toList());
    snapshotSplitOrClosedRegions(env, regions, "SplitRegionsSnapshotPool");
  }

  private void snapshotClosedRegions(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6cad6e14-491d-3d38-a425-c238afbe0b00"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4c6d52d4-20eb-391f-b47f-bcd79339f69c"))) {
return;
}
    List<RegionInfo> regions = getDefaultRegionReplica(env).collect(Collectors.toList());
if(KnobRuntime.check(java.util.UUID.fromString("57d60054-f4bb-3751-8c7e-393c38b0a71d"))) {
throw new java.io.IOException("Injected exception");
}
    snapshotSplitOrClosedRegions(env, regions, "ClosedRegionsSnapshotPool");
  }

  private Stream<RegionInfo> getDefaultRegionReplica(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("4abd45b8-9047-3d21-b29f-09bed376ef55"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("d71cda0c-2a2e-363e-be2c-afeb1fc7deec"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b25d8328-69ba-33f6-b896-7b5b90103848"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshotTable);
    field.set(snapshotTable, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95e8a351-7f75-34a5-86af-2e006d96406e"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d09df4a-a144-31d4-8d60-c42b66b13728"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("157ad1d3-957b-3c81-b9ea-9d57544e1686"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return env.getAssignmentManager().getTableRegions(snapshotTable, false).stream()
      .filter(r -> RegionReplicaUtil.isDefaultReplica(r));
  }

  private void snapshotSplitOrClosedRegions(MasterProcedureEnv env, List<RegionInfo> regions,
    String threadPoolName) throws IOException {
    ThreadPoolExecutor exec =
      SnapshotManifest.createExecutor(env.getMasterConfiguration(), threadPoolName);
    try {
      ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
        @Override
        public void editRegion(final RegionInfo region) throws IOException {
          snapshotManifest.addRegion(CommonFSUtils.getTableDir(rootDir, snapshotTable), region);
          LOG.info("take snapshot region={}, table={}", region, snapshotTable);
        }
      });
    } finally {
      exec.shutdown();
    }
    status.setStatus("Completed referencing closed/split regions of table: " + snapshotTable);
  }

  private void snapshotMobRegion(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5ed6ca19-f3b5-39fd-9587-6729e9667f5a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4de70a2c-13ea-375d-8b96-1a6c379933a9"))) {
return;
}
    if (!MobUtils.hasMobColumns(htd)) {
      return;
    }
    ThreadPoolExecutor exec =
      SnapshotManifest.createExecutor(env.getMasterConfiguration(), "MobRegionSnapshotPool");
    RegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(htd.getTableName());
    try {
      ModifyRegionUtils.editRegions(exec, Collections.singleton(mobRegionInfo),
        new ModifyRegionUtils.RegionEditTask() {
          @Override
          public void editRegion(final RegionInfo region) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ef9fdf36-b84e-3162-b04f-75d539744ead"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ce986a2a-3096-3860-81ba-4583836abe93"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d647f414-3bec-3380-98d6-ccf48a389c07"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("662f7649-7924-3096-af1f-8c4bab49577e"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4431c5f5-ef2a-3b48-95d2-ecfbdfd98a75"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshotTable));
    field.set(snapshotTable, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b3766a2-3655-3354-a51f-ac6c2a5b280e"))) {
try {
    java.lang.reflect.Field field = snapshotTable.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshotTable);
    field.set(snapshotTable, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            snapshotManifest.addRegion(CommonFSUtils.getTableDir(rootDir, snapshotTable), region);
          }
        });
    } finally {
      exec.shutdown();
    }
    status.setStatus("Completed referencing HFiles for the mob region of table: " + snapshotTable);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer
      .serialize(SnapshotProcedureStateData.newBuilder().setSnapshot(this.snapshot).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    SnapshotProcedureStateData data = serializer.deserialize(SnapshotProcedureStateData.class);
    this.snapshot = data.getSnapshot();
  }

  private Procedure<MasterProcedureEnv>[] createRemoteSnapshotProcedures(MasterProcedureEnv env) {
    return env.getAssignmentManager().getTableRegions(snapshotTable, true).stream()
      .filter(r -> RegionReplicaUtil.isDefaultReplica(r))
      .map(r -> new SnapshotRegionProcedure(snapshot, r)).toArray(SnapshotRegionProcedure[]::new);
  }

  @Override
  public void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getName()).append(", id=").append(getProcId()).append(", snapshot=")
      .append(ClientSnapshotDescriptionUtils.toString(snapshot));
  }

  public SnapshotDescription getSnapshotDesc() {
    return snapshot;
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    if (getCurrentState() == getInitialState()) {
      // if we are in the initial state, it is unnecessary to call prepareSnapshotEnv().
      return;
    }
    try {
      prepareSnapshotEnv(env);
      boolean snapshotProcedureEnabled = conf.getBoolean(SnapshotManager.SNAPSHOT_PROCEDURE_ENABLED,
        SnapshotManager.SNAPSHOT_PROCEDURE_ENABLED_DEFAULT);
      if (!snapshotProcedureEnabled) {
        throw new IOException("SnapshotProcedure is DISABLED");
      }
    } catch (IOException e) {
      LOG.error("Failed replaying {}, mark procedure as FAILED", this, e);
      setFailure("master-snapshot", e);
    }
  }

  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  public synchronized void markSnapshotCorrupted() throws IOException {
    Path flagFile = SnapshotDescriptionUtils.getCorruptedFlagFileForSnapshot(workingDir);
    if (!workingDirFS.exists(flagFile)) {
      workingDirFS.create(flagFile).close();
      LOG.info("touch corrupted snapshot flag file {} for {}", flagFile, snapshot.getName());
    }
  }

  public boolean isSnapshotCorrupted() throws IOException {
    return workingDirFS
      .exists(SnapshotDescriptionUtils.getCorruptedFlagFileForSnapshot(workingDir));
  }
}
