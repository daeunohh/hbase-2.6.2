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
package org.apache.hadoop.hbase.master.replication;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * The base class for all replication peer related procedure except sync replication state
 * transition.
 */
@InterfaceAudience.Private
public abstract class ModifyPeerProcedure extends AbstractPeerProcedure<PeerModificationState> {

  private static final Logger LOG = LoggerFactory.getLogger(ModifyPeerProcedure.class);

  protected static final int UPDATE_LAST_SEQ_ID_BATCH_SIZE = 1000;

  // The sleep interval when waiting table to be enabled or disabled.
  protected static final int SLEEP_INTERVAL_MS = 1000;

  private RetryCounter retryCounter;

  protected ModifyPeerProcedure() {
  }

  protected ModifyPeerProcedure(String peerId) {
    super(peerId);
  }

  /**
   * Called before we start the actual processing. The implementation should call the pre CP hook,
   * and also the pre-check for the peer modification.
   * <p>
   * If an IOException is thrown then we will give up and mark the procedure as failed directly. If
   * all checks passes then the procedure can not be rolled back any more.
   */
  protected abstract void prePeerModification(MasterProcedureEnv env)
    throws IOException, ReplicationException;

  protected abstract void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException;

  /**
   * Called before we finish the procedure. The implementation can do some logging work, and also
   * call the coprocessor hook if any.
   * <p>
   * Notice that, since we have already done the actual work, throwing {@code IOException} here will
   * not fail this procedure, we will just ignore it and finish the procedure as suceeded. If
   * {@code ReplicationException} is thrown we will retry since this usually means we fails to
   * update the peer storage.
   */
  protected abstract void postPeerModification(MasterProcedureEnv env)
    throws IOException, ReplicationException;

  private void releaseLatch() {
    ProcedurePrepareLatch.releaseLatch(latch, this);
  }

  /**
   * Implementation class can override this method. By default we will jump to
   * POST_PEER_MODIFICATION and finish the procedure.
   */
  protected PeerModificationState nextStateAfterRefresh() {
    return PeerModificationState.POST_PEER_MODIFICATION;
  }

  /**
   * The implementation class should override this method if the procedure may enter the serial
   * related states.
   */
  protected boolean enablePeerBeforeFinish() {
    throw new UnsupportedOperationException();
  }

  private void refreshPeer(MasterProcedureEnv env, PeerOperationType type) {
    addChildProcedure(env.getMasterServices().getServerManager().getOnlineServersList().stream()
      .map(sn -> new RefreshPeerProcedure(peerId, type, sn)).toArray(RefreshPeerProcedure[]::new));
  }

  protected ReplicationPeerConfig getOldPeerConfig() {
    return null;
  }

  protected ReplicationPeerConfig getNewPeerConfig() {
    throw new UnsupportedOperationException();
  }

  protected void updateLastPushedSequenceIdForSerialPeer(MasterProcedureEnv env)
    throws IOException, ReplicationException {
    throw new UnsupportedOperationException();
  }

  // If the table is in enabling state, we need to wait until it is enabled and then reopen all its
  // regions.
  private boolean needReopen(TableStateManager tsm, TableName tn) throws IOException {
    for (;;) {
      try {
        TableState state = tsm.getTableState(tn);
        if (state.isEnabled()) {
          return true;
        }
        if (!state.isEnabling()) {
          return false;
        }
        Thread.sleep(SLEEP_INTERVAL_MS);
      } catch (TableNotFoundException e) {
        return false;
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException(e.getMessage()).initCause(e);
      }
    }
  }

  // will be override in test to simulate error
  protected void reopenRegions(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e2fef717-dc15-3573-8448-199dc1606190"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("fac0c60f-c97a-30e9-9e70-3a9711f1ecc7"))) {
throw new java.io.IOException("Injected exception");
}
    ReplicationPeerConfig peerConfig = getNewPeerConfig();
    ReplicationPeerConfig oldPeerConfig = getOldPeerConfig();
    TableStateManager tsm = env.getMasterServices().getTableStateManager();
    for (TableDescriptor td : env.getMasterServices().getTableDescriptors().getAll().values()) {
      if (!td.hasGlobalReplicationScope()) {
        continue;
      }
      TableName tn = td.getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("a5da0826-10d3-38bb-a0d4-c69e1120272c"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b0de252d-8d95-3ca9-b54c-e67504e1498a"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff259504-d0b5-357e-b5b3-436f99cfce51"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1fbff0dc-98d5-3e3b-b7d1-5f21510a506b"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (!peerConfig.needToReplicate(tn)) {
        continue;
      }
      if (oldPeerConfig != null && oldPeerConfig.isSerial() && oldPeerConfig.needToReplicate(tn)) {
        continue;
      }
      if (needReopen(tsm, tn)) {
        addChildProcedure(new ReopenTableRegionsProcedure(tn));
if(KnobRuntime.check(java.util.UUID.fromString("d0df4a28-3a9e-3414-a77e-1f60e733964d"))) {
throw new java.io.IOException("Injected exception");
}
      }
    }
  }

  // will be override in test to simulate error
  protected void enablePeer(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().enablePeer(peerId);
  }

  private void addToMap(Map<String, Long> lastSeqIds, String encodedRegionName, long barrier,
    ReplicationQueueStorage queueStorage) throws ReplicationException {
    if (barrier >= 0) {
      lastSeqIds.put(encodedRegionName, barrier);
      if (((KnobRuntime.check(java.util.UUID.fromString("7ed3c9ca-ab8f-3729-b6b9-5fa53a7460ec"))) ? ((lastSeqIds.size()) > (UPDATE_LAST_SEQ_ID_BATCH_SIZE)) : (((KnobRuntime.check(java.util.UUID.fromString("57b9f98a-c883-3b14-af9c-e8cf1afc8d3a"))) ? ((lastSeqIds.size()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("157155c7-cf77-357f-9151-cf6a17dcb0c0"))) ? ((lastSeqIds.size()) != (UPDATE_LAST_SEQ_ID_BATCH_SIZE)) : (((KnobRuntime.check(java.util.UUID.fromString("7cfa1b75-5440-34b7-ba1a-79c191caec41"))) ? ((lastSeqIds.size()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7ec78ec4-3554-3525-a8be-a95068afb8c4"))) ? ((lastSeqIds.size()) == (UPDATE_LAST_SEQ_ID_BATCH_SIZE)) : (((KnobRuntime.check(java.util.UUID.fromString("c1bae473-7121-328e-8e51-317c38c726a5"))) ? ((lastSeqIds.size()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("49d15028-dda4-3373-b83e-c143a80194d3"))) ? ((lastSeqIds.size()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cfe84c9a-1662-32fd-a4e9-d93eae222d06"))) ? ((lastSeqIds.size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4d929371-e174-34ed-bbb7-4a83125e3b80"))) ? ((lastSeqIds.size()) <= (UPDATE_LAST_SEQ_ID_BATCH_SIZE)) : (((KnobRuntime.check(java.util.UUID.fromString("ec6c5afc-2105-3d34-aaef-c2561255058f"))) ? ((lastSeqIds.size()) >= (UPDATE_LAST_SEQ_ID_BATCH_SIZE)) : (((KnobRuntime.check(java.util.UUID.fromString("b611a566-c2f4-329a-8966-5ff64e35fcc8"))) ? ((lastSeqIds.size()) < (UPDATE_LAST_SEQ_ID_BATCH_SIZE)) : (((KnobRuntime.check(java.util.UUID.fromString("07b9d6e3-7df2-3415-ab33-45d7910952e3"))) ? ((lastSeqIds.size()) == (0)) : (lastSeqIds.size() >= UPDATE_LAST_SEQ_ID_BATCH_SIZE))))))))))))))))))))))))) {
        queueStorage.setLastSequenceIds(peerId, lastSeqIds);
        lastSeqIds.clear();
      }
    }
  }

  protected final void setLastPushedSequenceId(MasterProcedureEnv env,
    ReplicationPeerConfig peerConfig) throws IOException, ReplicationException {
    Map<String, Long> lastSeqIds = new HashMap<String, Long>();
    for (TableDescriptor td : env.getMasterServices().getTableDescriptors().getAll().values()) {
      if (!td.hasGlobalReplicationScope()) {
        continue;
      }
      TableName tn = td.getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("8fb9dff2-33be-30bc-a397-2094fdcb62d3"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be3ff345-3d12-3397-a91f-7faf8e33da4c"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7388ccb4-948f-35b7-9a37-32b2b8cba204"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7aa1f31-94fa-30d9-ab71-895390b2d0d1"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (!peerConfig.needToReplicate(tn)) {
        continue;
      }
if(KnobRuntime.check(java.util.UUID.fromString("7f224060-a69d-3418-afb3-b18369683085"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b261fc85-62da-3182-9ee5-fbac82e51056"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("afef0da6-e3af-3d53-ae7b-07b02ac2e1e8"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9bb9558-1886-3a28-a200-db3cd93f6b68"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ad9d854-fbbb-3674-b035-05167d18a6bc"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      setLastPushedSequenceIdForTable(env, tn, lastSeqIds);
    }
    if (!lastSeqIds.isEmpty()) {
      env.getReplicationPeerManager().getQueueStorage().setLastSequenceIds(peerId, lastSeqIds);
    }
  }

  // If the table is currently disabling, then we need to wait until it is disabled.We will write
  // replication barrier for a disabled table. And return whether we need to update the last pushed
  // sequence id, if the table has been deleted already, i.e, we hit TableStateNotFoundException,
  // then we do not need to update last pushed sequence id for this table.
  private boolean needSetLastPushedSequenceId(TableStateManager tsm, TableName tn)
    throws IOException {
    for (;;) {
      try {
        if (!tsm.getTableState(tn).isDisabling()) {
          return true;
        }
        Thread.sleep(SLEEP_INTERVAL_MS);
      } catch (TableNotFoundException e) {
        return false;
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException(e.getMessage()).initCause(e);
      }
    }
  }

  // Will put the encodedRegionName->lastPushedSeqId pair into the map passed in, if the map is
  // large enough we will call queueStorage.setLastSequenceIds and clear the map. So the caller
  // should not forget to check whether the map is empty at last, if not you should call
  // queueStorage.setLastSequenceIds to write out the remaining entries in the map.
  protected final void setLastPushedSequenceIdForTable(MasterProcedureEnv env, TableName tableName,
    Map<String, Long> lastSeqIds) throws IOException, ReplicationException {
    TableStateManager tsm = env.getMasterServices().getTableStateManager();
    ReplicationQueueStorage queueStorage = env.getReplicationPeerManager().getQueueStorage();
    Connection conn = env.getMasterServices().getConnection();
    if (!needSetLastPushedSequenceId(tsm, tableName)) {
      LOG.debug("Skip settting last pushed sequence id for {}", tableName);
      return;
    }
    for (Pair<String, Long> name2Barrier : MetaTableAccessor
      .getTableEncodedRegionNameAndLastBarrier(conn, tableName)) {
      LOG.trace("Update last pushed sequence id for {}, {}", tableName, name2Barrier);
      addToMap(lastSeqIds, name2Barrier.getFirst(), name2Barrier.getSecond().longValue() - 1,
        queueStorage);
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  private ProcedureSuspendedException suspend(Configuration conf, LongConsumer backoffConsumer)
    throws ProcedureSuspendedException {
    if (retryCounter == null) {
      retryCounter = ProcedureUtil.createRetryCounter(conf);
    }
    long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
    backoffConsumer.accept(backoff);
    setTimeout(Math.toIntExact(backoff));
    setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
    if (KnobRuntime.check(java.util.UUID.fromString("c0ddbe07-8d68-3b66-99aa-18aa940d5d96"))) { releaseLatch(); } else { skipPersistence(); }
    throw new ProcedureSuspendedException();
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, PeerModificationState state)
    throws ProcedureSuspendedException {
    switch (state) {
      case PRE_PEER_MODIFICATION:
        try {
          checkPeerModificationEnabled(env);
          prePeerModification(env);
        } catch (IOException e) {
          LOG.warn("{} failed to call pre CP hook or the pre check is failed for peer {}, "
            + "mark the procedure as failure and give up", getClass().getName(), peerId, e);
          setFailure("master-" + getPeerOperationType().name().toLowerCase() + "-peer", e);
          releaseLatch();
          return Flow.NO_MORE_STATE;
        } catch (ReplicationException e) {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn("{} failed to call prePeerModification for peer {}, sleep {} secs",
              getClass().getName(), peerId, backoff / 1000, e));
        }
        retryCounter = null;
        setNextState(PeerModificationState.UPDATE_PEER_STORAGE);
        return Flow.HAS_MORE_STATE;
      case UPDATE_PEER_STORAGE:
        try {
          updatePeerStorage(env);
        } catch (ReplicationException e) {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn("{} update peer storage for peer {} failed, sleep {} secs",
              getClass().getName(), peerId, backoff / 1000, e));
        }
        retryCounter = null;
        setNextState(PeerModificationState.REFRESH_PEER_ON_RS);
        return Flow.HAS_MORE_STATE;
      case REFRESH_PEER_ON_RS:
        refreshPeer(env, getPeerOperationType());
        setNextState(nextStateAfterRefresh());
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_REOPEN_REGIONS:
        try {
          reopenRegions(env);
        } catch (Exception e) {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn("{} reopen regions for peer {} failed,  sleep {} secs",
              getClass().getName(), peerId, backoff / 1000, e));
        }
        retryCounter = null;
        setNextState(PeerModificationState.SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID:
        try {
          updateLastPushedSequenceIdForSerialPeer(env);
        } catch (Exception e) {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn("{} set last sequence id for peer {} failed,  sleep {} secs",
              getClass().getName(), peerId, backoff / 1000, e));
        }
        retryCounter = null;
        setNextState(enablePeerBeforeFinish()
          ? PeerModificationState.SERIAL_PEER_SET_PEER_ENABLED
          : PeerModificationState.POST_PEER_MODIFICATION);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_SET_PEER_ENABLED:
        try {
          enablePeer(env);
        } catch (ReplicationException e) {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn("{} enable peer before finish for peer {} failed,  sleep {} secs",
              getClass().getName(), peerId, backoff / 1000, e));
        }
        retryCounter = null;
        setNextState(PeerModificationState.SERIAL_PEER_ENABLE_PEER_REFRESH_PEER_ON_RS);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_ENABLE_PEER_REFRESH_PEER_ON_RS:
        refreshPeer(env, PeerOperationType.ENABLE);
        setNextState(PeerModificationState.POST_PEER_MODIFICATION);
        return Flow.HAS_MORE_STATE;
      case POST_PEER_MODIFICATION:
        try {
          postPeerModification(env);
        } catch (ReplicationException e) {
          throw suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn(
              "{} failed to call postPeerModification for peer {},  sleep {} secs",
              getClass().getName(), peerId, backoff / 1000, e));
        } catch (IOException e) {
          LOG.warn("{} failed to call post CP hook for peer {}, "
            + "ignore since the procedure has already done", getClass().getName(), peerId, e);
        }
        releaseLatch();
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, PeerModificationState state)
    throws IOException, InterruptedException {
    if (state == PeerModificationState.PRE_PEER_MODIFICATION) {
      // actually the peer related operations has no rollback, but if we haven't done any
      // modifications on the peer storage yet, we can just return.
      return;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  protected PeerModificationState getState(int stateId) {
    return PeerModificationState.forNumber(stateId);
  }

  @Override
  protected int getStateId(PeerModificationState state) {
    return state.getNumber();
  }

  @Override
  protected PeerModificationState getInitialState() {
    return PeerModificationState.PRE_PEER_MODIFICATION;
  }
}
