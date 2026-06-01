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
package org.apache.hadoop.hbase.master.assignment;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionRemoteProcedureBaseState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionRemoteProcedureBaseStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The base class for the remote procedures used to open/close a region.
 * <p/>
 * Notice that here we do not care about the result of the remote call, if the remote call is
 * finished, either succeeded or not, we will always finish the procedure. The parent procedure
 * should take care of the result and try to reschedule if the result is not good.
 */
@InterfaceAudience.Private
public abstract class RegionRemoteProcedureBase extends Procedure<MasterProcedureEnv>
  implements TableProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionRemoteProcedureBase.class);

  protected RegionInfo region;

  protected ServerName targetServer;

  private RegionRemoteProcedureBaseState state =
    RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH;

  private TransitionCode transitionCode;

  private long seqId;

  private RetryCounter retryCounter;

  protected RegionRemoteProcedureBase() {
  }

  protected RegionRemoteProcedureBase(TransitRegionStateProcedure parent, RegionInfo region,
    ServerName targetServer) {
    this.region = region;
    this.targetServer = targetServer;
    parent.attachRemoteProc(this);
  }

  @Override
  public Optional<RemoteProcedureDispatcher.RemoteOperation> remoteCallBuild(MasterProcedureEnv env,
    ServerName remote) {
    // REPORT_SUCCEED means that this remote open/close request already executed in RegionServer.
    // So return empty operation and RSProcedureDispatcher no need to send it again.
    if (state == RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_REPORT_SUCCEED) {
      return Optional.empty();
    }
    return Optional.of(newRemoteOperation(env));
  }

  protected abstract RemoteProcedureDispatcher.RemoteOperation
    newRemoteOperation(MasterProcedureEnv env);

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    // should not be called since we use reportRegionStateTransition to report the result
    throw new UnsupportedOperationException();
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    // should not be called since we use reportRegionStateTransition to report the result
    throw new UnsupportedOperationException();
  }

  private RegionStateNode getRegionNode(MasterProcedureEnv env) {
    return env.getAssignmentManager().getRegionStates().getRegionStateNode(region);
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName remote, IOException exception) {
    RegionStateNode regionNode = getRegionNode(env);
    regionNode.lock();
    try {
      if (!env.getMasterServices().getServerManager().isServerOnline(remote)) {
        // the SCP will interrupt us, give up
        LOG.debug("{} for region {}, targetServer {} is dead, SCP will interrupt us, give up", this,
          regionNode, remote);
        return;
      }
      if (state != RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH) {
        // not sure how can this happen but anyway let's add a check here to avoid waking the wrong
        // procedure...
        LOG.warn("{} for region {}, targetServer={} has already been woken up, ignore", this,
          regionNode, remote);
        return;
      }
      LOG.warn("The remote operation {} for region {} to server {} failed", this, regionNode,
        remote, exception);
      // It is OK to not persist the state here, as we do not need to change the region state if the
      // remote call is failed. If the master crashed before we actually execute the procedure and
      // persist the new state, it is fine to retry on the same target server again.
      state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH_FAIL;
      regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  public TableName getTableName() {
    return region.getTable();
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    if (TableName.isMetaTableName(getTableName())) {
      return false;
    }
    // First we need meta to be loaded, and second, if meta is not online then we will likely to
    // fail when updating meta so we wait until it is assigned.
    AssignmentManager am = env.getAssignmentManager();
    return am.waitMetaLoaded(this) || am.waitMetaAssigned(this, region);
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  // do some checks to see if the report is valid
  protected abstract void checkTransition(RegionStateNode regionNode, TransitionCode transitionCode,
    long seqId) throws UnexpectedStateException;

  // change the in memory state of the regionNode, but do not update meta.
  protected abstract void updateTransitionWithoutPersistingToMeta(MasterProcedureEnv env,
    RegionStateNode regionNode, TransitionCode transitionCode, long seqId) throws IOException;

  // A bit strange but the procedure store will throw RuntimeException if we can not persist the
  // state, so upper layer should take care of this...
  private void persistAndWake(MasterProcedureEnv env, RegionStateNode regionNode) {
    // The synchronization here is to guard with ProcedureExecutor.executeRollback, as here we will
    // not hold the procedure execution lock, but we should not persist a procedure in ROLLEDBACK
    // state to the procedure store.
    // The ProcedureStore.update must be inside the lock, so here the check for procedure state and
    // update could be atomic. In ProcedureExecutor.cleanupAfterRollbackOneStep, we will set the
    // state to ROLLEDBACK, which will hold the same lock too as the Procedure.setState method is
    // synchronized. This is the key to keep us safe.
    synchronized (this) {
      if (getState() == ProcedureState.ROLLEDBACK) {
        LOG.warn("Procedure {} has already been rolled back, skip persistent", this);
        return;
      }
      env.getMasterServices().getMasterProcedureExecutor().getStore().update(this);
    }
    regionNode.getProcedureEvent().wake(env.getProcedureScheduler());
  }

  // should be called with RegionStateNode locked, to avoid race with the execute method below
  void reportTransition(MasterProcedureEnv env, RegionStateNode regionNode, ServerName serverName,
    TransitionCode transitionCode, long seqId) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4e44084a-9bfb-36a4-a580-ac40a543bb71"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("811a6327-628d-3924-9fe8-671aeb1c9519"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a614eb87-6556-351b-ac9e-82ac97fa8c53"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("17d8ba1b-7672-3651-a2a6-5adaedb23bf7"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("332fb572-71b0-36a2-9f00-bbf3d5154f31"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6fa6ae2d-6175-363b-9274-0c298809c1d2"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11eeabea-5a64-3dfe-803e-67ba5616077d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("e392d3c1-29fc-34c5-bfaa-2484fb2e2b7c"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b9afb5b-4cf4-3605-b0c5-648196c7f6e7"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("b913657f-a1d8-38e2-94a2-6285d2ac65ca"))) ? ((state) == (RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH)) : (((KnobRuntime.check(java.util.UUID.fromString("058fe44e-c8ef-3e44-a6f8-54c5e363ccce"))) ? ((state) != (RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH)) : (state != RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH))))) {
      // should be a retry
      return;
    }
    if (!targetServer.equals(serverName)) {
      throw new UnexpectedStateException("Received report from " + serverName + ", expected "
        + targetServer + ", " + regionNode + ", proc=" + this);
    }
    checkTransition(regionNode, transitionCode, seqId);
    // this state means we have received the report from RS, does not mean the result is fine, as we
    // may received a FAILED_OPEN.
    this.state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_REPORT_SUCCEED;
    this.transitionCode = transitionCode;
    this.seqId = seqId;
    // Persist the transition code and openSeqNum(if provided).
    // We should not update the hbase:meta directly as this may cause races when master restarts,
    // as the old active master may incorrectly report back to RS and cause the new master to hang
    // on a OpenRegionProcedure forever. See HBASE-22060 and HBASE-22074 for more details.
    boolean succ = false;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("b78b2f57-1be1-31e8-becd-0efb08b9e66d"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e732b652-5e21-392c-a65f-7d91a786f100"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      persistAndWake(env, regionNode);
      succ = true;
    } finally {
      if (!succ) {
        this.state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_DISPATCH;
        this.transitionCode = null;
        this.seqId = HConstants.NO_SEQNUM;
      }
    }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("b4e1ae83-6bba-3338-9c1e-e1eaa5b1db19"))) {
seqId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b86d3959-986c-39a3-ab4f-5a6565f809a9"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("219fed3b-967c-36ed-8331-3de56596721c"))) {
seqId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae1b86ed-36ff-3989-b38d-c49c1bd34862"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef3c1aa8-2fb6-33e9-9759-1b0dff424c2d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("86f0380b-65c1-3118-a738-2968c9b4d543"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("466b8cdc-bab2-37ec-95c3-55295d0e8018"))) {
seqId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("551cb26f-bb87-37dd-ab2e-645e8c21e7ac"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87b59b39-11c7-35c1-9105-ef127d781398"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56428683-f25b-31d7-8e8a-3d2015e602ca"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      updateTransitionWithoutPersistingToMeta(env, regionNode, transitionCode, seqId);
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
  }

  void serverCrashed(MasterProcedureEnv env, RegionStateNode regionNode, ServerName serverName) {
if(KnobRuntime.check(java.util.UUID.fromString("816999dc-e7da-3074-935c-7e93c7c011eb"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df7884eb-3724-3d17-9723-0f17f6845044"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("493b623b-17b2-3faf-b762-7b61c25e2302"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("24115899-8543-3705-9c4a-435c9ba50995"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("6cf2c43b-279f-300a-800d-54a9bd65f8e8"))) ? ((state) != (RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_SERVER_CRASH)) : (((KnobRuntime.check(java.util.UUID.fromString("5c37d743-041d-3be8-ad9b-8aa740831ad4"))) ? ((state) == (RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_SERVER_CRASH)) : (state == RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_SERVER_CRASH))))) {
      // should be a retry
      return;
    }
    RegionRemoteProcedureBaseState oldState = state;
    // it is possible that the state is in REGION_REMOTE_PROCEDURE_SERVER_CRASH, think of this
    // sequence
    // 1. region is open on the target server and the above reportTransition call is succeeded
    // 2. before we are woken up and update the meta, the target server crashes, and then we arrive
    // here
    this.state = RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_SERVER_CRASH;
    boolean succ = false;
    try {
      persistAndWake(env, regionNode);
      succ = true;
    } finally {
      if (!succ) {
        this.state = oldState;
      }
    }
  }

  protected abstract void restoreSucceedState(AssignmentManager am, RegionStateNode regionNode,
    long seqId) throws IOException;

  void stateLoaded(AssignmentManager am, RegionStateNode regionNode) {
    if (state == RegionRemoteProcedureBaseState.REGION_REMOTE_PROCEDURE_REPORT_SUCCEED) {
      try {
        restoreSucceedState(am, regionNode, seqId);
      } catch (IOException e) {
        // should not happen as we are just restoring the state
        throw new AssertionError(e);
      }
    }
  }

  private TransitRegionStateProcedure getParent(MasterProcedureEnv env) {
    return (TransitRegionStateProcedure) env.getMasterServices().getMasterProcedureExecutor()
      .getProcedure(getParentProcId());
  }

  private void unattach(MasterProcedureEnv env) {
    getParent(env).unattachRemoteProc(this);
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    RegionStateNode regionNode = getRegionNode(env);
    if (KnobRuntime.check(java.util.UUID.fromString("245984a8-5b5b-3216-b26b-7538ef8202fa"))) { regionNode.unlock(); } else { regionNode.lock(); }
    try {
      switch (state) {
        case REGION_REMOTE_PROCEDURE_DISPATCH: {
          // The code which wakes us up also needs to lock the RSN so here we do not need to
          // synchronize
          // on the event.
          ProcedureEvent<?> event = regionNode.getProcedureEvent();
          try {
if(KnobRuntime.check(java.util.UUID.fromString("eab5a88f-af29-387e-bc79-56b21e5e4103"))) {
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
          } catch (FailedRemoteDispatchException e) {
            LOG.warn("Can not add remote operation {} for region {} to server {}, this usually "
              + "because the server is alread dead, give up and mark the procedure as complete, "
              + "the parent procedure will take care of this.", this, region, targetServer, e);
            unattach(env);
            return null;
          }
          event.suspend();
          event.suspendIfNotReady(this);
          throw new ProcedureSuspendedException();
        }
        case REGION_REMOTE_PROCEDURE_REPORT_SUCCEED:
if(KnobRuntime.check(java.util.UUID.fromString("08c83987-63c6-3711-a985-7f217d331060"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fde06767-aa96-3d4c-ac91-9c3b9e8e6607"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6d6f0938-2060-3cad-9229-d9605ed640b1"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("73ad5672-43d5-39e1-97d2-6949b44623b3"))) { env.getAssignmentManager().regionClosedAbnormally(regionNode); } else { env.getAssignmentManager().persistToMeta(regionNode); }
          unattach(env);
          return null;
        case REGION_REMOTE_PROCEDURE_DISPATCH_FAIL:
          // the remote call is failed so we do not need to change the region state, just return.
          unattach(env);
          return null;
        case REGION_REMOTE_PROCEDURE_SERVER_CRASH:
if(KnobRuntime.check(java.util.UUID.fromString("2126fb86-a77a-3c01-a0e3-01ea6b2ed06a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("771cfed2-a1ea-3712-9c98-43053962287a"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e8a8b5e-a381-3b69-be69-b484f8917256"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("23080963-c3ab-3bf4-8f62-8182c200c181"))) { env.getAssignmentManager().persistToMeta(regionNode); } else { env.getAssignmentManager().regionClosedAbnormally(regionNode); }
          unattach(env);
          return null;
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    } catch (IOException e) {
      if (((KnobRuntime.check(java.util.UUID.fromString("05059bc2-7222-3d9a-bbb7-8374098c6da6"))) ? ((retryCounter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("514e2270-311e-3f9f-a5bc-5636b7a3fc30"))) ? ((retryCounter) == (null)) : (retryCounter == null))))) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn("Failed updating meta, suspend {}secs {}; {};", backoff / 1000, this, regionNode, e);
if(KnobRuntime.check(java.util.UUID.fromString("2a871377-f1b0-3c31-8d25-d10609ffc966"))) {
backoff += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea99ac44-0a6e-31bd-a873-186b3a9898df"))) {
backoff = 0;
}
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false; // 'false' means that this procedure handled the timeout
  }

  @Override
  public boolean storeInDispatchedQueue() {
if(KnobRuntime.check(java.util.UUID.fromString("da6e4629-a048-30f3-b785-9296f7a8b348"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("ecfcf730-91df-3daf-89a7-96b710580817"))) {
return true;
}
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("45d35f67-720f-3f4d-84e3-fc82c740a754"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("69abd09a-637f-3c75-b558-7199ec81ebc2"))) {
return;
}
    RegionRemoteProcedureBaseStateData.Builder builder =
      RegionRemoteProcedureBaseStateData.newBuilder().setRegion(ProtobufUtil.toRegionInfo(region))
        .setTargetServer(ProtobufUtil.toServerName(targetServer)).setState(state);
    if (transitionCode != null) {
if(KnobRuntime.check(java.util.UUID.fromString("cc0879c1-13db-3206-ad69-3c46777aed4e"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0ec46b59-0243-3808-8e26-5fa6703a9a65"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70be4d56-8d62-3965-ab16-f1f34b860872"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3621abdb-5666-3189-b901-490ccf3f5f54"))) {
try {
    java.lang.reflect.Field field = transitionCode.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(transitionCode));
    field.set(transitionCode, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      builder.setTransitionCode(transitionCode);
      builder.setSeqId(seqId);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RegionRemoteProcedureBaseStateData data =
      serializer.deserialize(RegionRemoteProcedureBaseStateData.class);
    region = ProtobufUtil.toRegionInfo(data.getRegion());
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    // 'state' may not be present if we are reading an 'old' form of this pb Message.
    if (data.hasState()) {
      state = data.getState();
    }
    if (data.hasTransitionCode()) {
      transitionCode = data.getTransitionCode();
      seqId = data.getSeqId();
    }
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    getParent(env).attachRemoteProc(this);
  }

  @Override
  public String getProcName() {
    return getClass().getSimpleName() + " " + region.getEncodedName();
  }

  @Override
  protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getProcName());
    if (targetServer != null) {
      builder.append(", server=");
      builder.append(this.targetServer);
    }
    if (this.retryCounter != null) {
      builder.append(", retry=");
      builder.append(this.retryCounter);
    }
  }
}
