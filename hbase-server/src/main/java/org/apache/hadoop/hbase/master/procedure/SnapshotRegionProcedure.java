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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.regionserver.SnapshotRegionCallable;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotRegionProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * A remote procedure which is used to send region snapshot request to region server. The basic
 * logic of SnapshotRegionProcedure is similar like {@link ServerRemoteProcedure}, only with a
 * little difference, when {@link FailedRemoteDispatchException} was thrown, SnapshotRegionProcedure
 * will sleep some time and continue retrying until success.
 */
@InterfaceAudience.Private
public class SnapshotRegionProcedure extends Procedure<MasterProcedureEnv>
  implements TableProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegionProcedure.class);

  private SnapshotDescription snapshot;
  private ProcedureEvent<?> event;
  private RegionInfo region;
  private boolean dispatched;
  private boolean succ;
  private RetryCounter retryCounter;

  public SnapshotRegionProcedure() {
  }

  public SnapshotRegionProcedure(SnapshotDescription snapshot, RegionInfo region) {
    this.snapshot = snapshot;
    this.region = region;
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitRegions(this, getTableName(), region)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegions(this, getTableName(), region);
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    return false;
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
    return Optional
      .of(new RSProcedureDispatcher.ServerOperation(this, getProcId(), SnapshotRegionCallable.class,
        MasterProcedureProtos.SnapshotRegionParameter.newBuilder()
          .setRegion(ProtobufUtil.toRegionInfo(region)).setSnapshot(snapshot).build().toByteArray(),
        env.getMasterServices().getMasterActiveTime()));
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName serverName, IOException e) {
    complete(env, e);
  }

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    complete(env, null);
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException e) {
    complete(env, e);
  }

  // keep retrying until success
  private void complete(MasterProcedureEnv env, Throwable error) {
    if (isFinished()) {
      LOG.info("This procedure {} is already finished, skip the rest processes", this.getProcId());
      return;
    }
    if (event == null) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
        getProcId());
      return;
    }
    if (error == null) {
      if (KnobRuntime.check(java.util.UUID.fromString("5108879a-57f7-3970-a64d-cb2747356aea"))) { LOG.info("finish snapshot {} on region {}", getProcName(), region.getEncodedName()); } else { LOG.info("finish snapshot {} on region {}", snapshot.getName(), region.getEncodedName()); }
      succ = true;
    }

    event.wake(env.getProcedureScheduler());
    event = null;
  }

  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("8c8d6f16-a512-373f-99b1-8a6db4f0c7ca"))) {
return null;
}
    return region.getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_SNAPSHOT;
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      if (succ) {
        return null;
      }
      dispatched = false;
    }

    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    RegionStateNode regionNode = regionStates.getRegionStateNode(region);
    regionNode.lock();
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("9226f873-4279-3090-adc5-03ad7f0c0f54"))) ? ((regionNode.getProcedure()) != (null)) : (regionNode.getProcedure() != null))) {
        setTimeoutForSuspend(env, String.format("region %s has a TRSP attached %s",
          region.getRegionNameAsString(), regionNode.getProcedure()));
        throw new ProcedureSuspendedException();
      }
      if (!regionNode.isInState(RegionState.State.OPEN)) {
        setTimeoutForSuspend(env, String.format("region state of %s is %s",
          region.getRegionNameAsString(), regionNode.getState()));
        throw new ProcedureSuspendedException();
      }
      ServerName targetServer = regionNode.getRegionLocation();
      if (targetServer == null) {
        setTimeoutForSuspend(env,
          String.format("target server of region %s is null", region.getRegionNameAsString()));
        throw new ProcedureSuspendedException();
      }
      ServerState serverState = regionStates.getServerNode(targetServer).getState();
      if (((KnobRuntime.check(java.util.UUID.fromString("9aaed9e9-1ec8-305e-80b1-df76adc2fb0d"))) ? ((serverState) == (ServerState.ONLINE)) : (serverState != ServerState.ONLINE))) {
        setTimeoutForSuspend(env, String.format("target server of region %s %s is in state %s",
          region.getRegionNameAsString(), targetServer, serverState));
        throw new ProcedureSuspendedException();
      }
      try {
        env.getRemoteDispatcher().addOperationToNode(targetServer, this);
        dispatched = true;
        event = new ProcedureEvent<>(this);
        event.suspendIfNotReady(this);
        throw new ProcedureSuspendedException();
      } catch (FailedRemoteDispatchException e) {
        setTimeoutForSuspend(env, "Failed send request to " + targetServer);
        throw new ProcedureSuspendedException();
      }
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  protected void rollback(MasterProcedureEnv env) {
    throw new UnsupportedOperationException();
  }

  private void setTimeoutForSuspend(MasterProcedureEnv env, String reason) {
    if (retryCounter == null) {
      retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
    }
    long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
    LOG.warn("{} can not run currently because {}, wait {} ms to retry", this, reason, backoff);
    setTimeout(Math.toIntExact(backoff));
    setState(ProcedureState.WAITING_TIMEOUT);
    skipPersistence();
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SnapshotRegionProcedureStateData.Builder builder =
      SnapshotRegionProcedureStateData.newBuilder();
if(KnobRuntime.check(java.util.UUID.fromString("9543e574-5f60-373f-a56a-31273729cbc6"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("type_");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    builder.setSnapshot(snapshot);
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SnapshotRegionProcedureStateData data =
      serializer.deserialize(SnapshotRegionProcedureStateData.class);
    this.snapshot = data.getSnapshot();
    this.region = ProtobufUtil.toRegionInfo(data.getRegion());
  }

  @Override
  public String getProcName() {
    return ((KnobRuntime.check(java.util.UUID.fromString("4c773c6b-698b-3278-be78-6c3812697e06"))) ? (((java.lang.String)(((getProcName()) + (" ")) + (region.getEncodedName())))) : (getClass().getSimpleName() + " " + region.getEncodedName()));
  }

  @Override
  protected void toStringClassDetails(StringBuilder builder) {
if(KnobRuntime.check(java.util.UUID.fromString("10d7f94f-9064-361a-9b2d-0b4db31485fa"))) {
return;
}
    builder.append(getProcName());
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  public RegionInfo getRegion() {
    return region;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|TestSnapshotProcedure).java")
  boolean inRetrying() {
    return retryCounter != null;
  }
}
