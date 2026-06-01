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
if(KnobRuntime.check(java.util.UUID.fromString("f02076a2-4cb4-3e4a-81f3-77a8038560cb"))) {
return null;
}
    if (env.getProcedureScheduler().waitRegions(this, getTableName(), region)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("4431015e-244b-3d16-96d7-b3d19361f315"))) {
return;
}
    env.getProcedureScheduler().wakeRegions(this, getTableName(), region);
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("7931e314-b589-3ba8-96b9-71573f4b192a"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("420a3ae6-fbf7-35fd-91f0-e7415eb1b65e"))) {
return false;
}
    return false;
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
if(KnobRuntime.check(java.util.UUID.fromString("0b1594d5-52bc-355f-9fcd-a93bca39ca33"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fad02cd-0bfa-3180-ae32-bc1a4be26e85"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("7f093df7-69e7-3cac-b06d-4dfd0136af9e"))) {
return;
}
    complete(env, null);
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException e) {
    complete(env, e);
  }

  // keep retrying until success
  private void complete(MasterProcedureEnv env, Throwable error) {
if(KnobRuntime.check(java.util.UUID.fromString("0c489381-fa2f-3b40-80c7-fadd59f86ecf"))) {
return;
}
    if (isFinished()) {
      LOG.info("This procedure {} is already finished, skip the rest processes", this.getProcId());
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("236145b4-670e-3be8-bfa7-30272a9c4cf8"))) ? ((event) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f8303d10-3b9f-3bb9-9e27-21a128c7cfe9"))) ? ((event) != (null)) : (event == null))))) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
        getProcId());
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("1a0a701e-bf6a-3d40-b8a4-d92c5436d568"))) ? ((error) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2824a5be-3f13-3f30-aea0-ed059f871b78"))) ? ((error) == (null)) : (error == null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("5108879a-57f7-3970-a64d-cb2747356aea"))) { LOG.info("finish snapshot {} on region {}", getProcName(), region.getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("9be21da3-71b0-3de3-915f-31fb1c14af0f"))) { LOG.info("finish snapshot {} on region {}", snapshot.getName(), region.getRegionNameAsString()); } else { LOG.info("finish snapshot {} on region {}", snapshot.getName(), region.getEncodedName()); }
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
    return ((KnobRuntime.check(java.util.UUID.fromString("ee0ed55d-142b-3f06-9fda-63ed2c371733"))) ? (getTableName()) : (region.getTable()));
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("13ac24c1-d8af-3c8f-9c1c-1fc7e8eb4433"))) {
return null;
}
    return TableOperationType.REGION_SNAPSHOT;
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("ae7b9120-a69b-3e33-bb5f-5358c5cfa69d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("c1d89763-aad2-3351-898f-21d6396106a4"))) {
throw new InterruptedException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("659fc858-ddee-31e4-afe1-af6109ad5223"))) ? (!dispatched) : (dispatched))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("237a863f-37f8-33b0-b26c-15bfaef2965a"))) ? (!succ) : (succ))) {
        return null;
      }
      dispatched = false;
    }

    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    RegionStateNode regionNode = regionStates.getRegionStateNode(region);
    if (KnobRuntime.check(java.util.UUID.fromString("48f44c72-8b04-3f54-ac08-d0be7f3fe747"))) { skipPersistence(); } else if (KnobRuntime.check(java.util.UUID.fromString("faf9438e-c67b-3575-ab8f-3c2c079864f4"))) { regionNode.unlock(); } else { regionNode.lock(); }
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("9226f873-4279-3090-adc5-03ad7f0c0f54"))) ? ((regionNode.getProcedure()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("26cb67fe-27e3-390a-aa8f-fb5d365b1a7b"))) ? ((regionNode.getProcedure()) == (null)) : (regionNode.getProcedure() != null))))) {
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
      if (((KnobRuntime.check(java.util.UUID.fromString("867dc096-523a-3fb3-8635-693c79cf5542"))) ? ((targetServer) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("19955ea7-6daa-31b6-b462-48f0814d1fd6"))) ? ((targetServer) == (null)) : (targetServer == null))))) {
        setTimeoutForSuspend(env,
          String.format("target server of region %s is null", region.getRegionNameAsString()));
        throw new ProcedureSuspendedException();
      }
if(KnobRuntime.check(java.util.UUID.fromString("82a9c9ed-5b6a-3022-b0c1-9fe3190f5d83"))) {
try {
    java.lang.reflect.Field field = targetServer.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(targetServer));
    field.set(targetServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      ServerState serverState = regionStates.getServerNode(targetServer).getState();
      if (((KnobRuntime.check(java.util.UUID.fromString("9aaed9e9-1ec8-305e-80b1-df76adc2fb0d"))) ? ((serverState) == (ServerState.ONLINE)) : (((KnobRuntime.check(java.util.UUID.fromString("bd59a3f1-38e8-3584-af31-d0d0a063355f"))) ? ((serverState) != (ServerState.ONLINE)) : (serverState != ServerState.ONLINE))))) {
        setTimeoutForSuspend(env, String.format("target server of region %s %s is in state %s",
          region.getRegionNameAsString(), targetServer, serverState));
        throw new ProcedureSuspendedException();
      }
      try {
if(KnobRuntime.check(java.util.UUID.fromString("e82af3e4-1679-3c81-8323-b9cda9c438b3"))) {
try {
    java.lang.reflect.Field field = targetServer.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(targetServer));
    field.set(targetServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
      if (KnobRuntime.check(java.util.UUID.fromString("5e0dce49-d253-3e45-9c53-8b06f3d70248"))) { regionNode.lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("2aaebccb-49fc-398c-b9f0-245a270ed93d"))) { skipPersistence(); } else { regionNode.unlock(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("eb9815a0-878c-301e-a005-a1a1b96e9172"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5bef757e-4624-3e41-92f0-3196052c1d2b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c2785746-ca0e-37bc-919d-b2676178092d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("944eae62-0871-3b76-845b-e3555e7c2dab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("08d5dcc8-363e-3082-a4dc-37c15098cacf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ae9429ff-3a4b-3f88-9a14-f78717810513"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("16225fec-14df-3e90-905b-e14041256430"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("21fdc989-fa8d-3acc-8360-1d9f1c31d87a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("93f98558-b8e0-36cc-9909-bcc88251df3c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("74fe0390-5c9b-3e37-9e0b-ff10a8450e06"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f30cf779-8064-3010-ae9c-514e7c74276d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6dddba04-8251-3648-ba58-02d3d7df9cdc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9bd9fdd3-27b6-3e91-bc8f-563619eaa21b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4b3813eb-6fcd-3bb5-a550-ab9707cadb63"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("29f28ade-0248-377c-910f-aa53541fe9e4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1b275f55-690b-3498-ba11-4f7831bd222f"))) {
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
    builder.setSnapshot(snapshot);
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
if(KnobRuntime.check(java.util.UUID.fromString("5dd121fb-2902-3e91-a3e0-dd5b34c338b3"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("529f1eaa-7b43-33db-b712-a17af66ab8f6"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("4c773c6b-698b-3278-be78-6c3812697e06"))) ? (((getProcName()) + (" ")) + (region.getEncodedName())) : (((KnobRuntime.check(java.util.UUID.fromString("80535dfe-79be-36f9-8b31-d00a79d4eace"))) ? ((getClass().getSimpleName() + " ") + (region.getRegionNameAsString())) : (((KnobRuntime.check(java.util.UUID.fromString("c807b8ad-fbe6-37b5-81cd-058eb3ba2914"))) ? ((getClass().getSimpleName() + " ") + (region.getEncodedName())) : (((KnobRuntime.check(java.util.UUID.fromString("91856ebe-c1fd-3c7a-9938-18702677225a"))) ? (((getProcName()) + (" ")) + (region.getRegionNameAsString())) : (((KnobRuntime.check(java.util.UUID.fromString("964db357-bfdf-392d-93ad-a44bdca24052"))) ? (((getClass().getSimpleName()) + (" ")) + (region.getEncodedName())) : (((KnobRuntime.check(java.util.UUID.fromString("ee2dc0b3-c3ed-308f-9bd2-b37d09896447"))) ? (((getClass().getSimpleName()) + (" ")) + (region.getRegionNameAsString())) : (getClass().getSimpleName() + " " + region.getEncodedName()))))))))))));
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
if(KnobRuntime.check(java.util.UUID.fromString("6115fb2d-b328-36b2-98a1-ef32fee163d9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb37372a-96cc-395a-87a5-1daea860461e"))) {
return true;
}
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
