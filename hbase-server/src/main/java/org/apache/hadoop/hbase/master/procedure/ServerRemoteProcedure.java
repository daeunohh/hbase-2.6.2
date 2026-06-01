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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;

@InterfaceAudience.Private
/**
 * The base class for Procedures that run {@link java.util.concurrent.Callable}s on a (remote)
 * RegionServer; e.g. asking a RegionServer to split a WAL file as a sub-procedure of the
 * ServerCrashProcedure recovery process.
 * <p>
 * To implement a new Procedure type, extend this class and override remoteCallBuild() and
 * complete(). The dispatch and callback will be handled for you here, internally.
 * <p>
 * The Procedure works as follows. It uses {@link RSProcedureDispatcher}, the same system used
 * dispatching Region OPEN and CLOSE RPCs, to pass a Callable to a RegionServer. Examples include
 * {@link org.apache.hadoop.hbase.regionserver.SplitWALCallable} and
 * {@link org.apache.hadoop.hbase.replication.regionserver.RefreshPeerCallable}. Rather than
 * assign/unassign, the Master calls #executeProcedures against the remote RegionServer wrapping a
 * Callable in a {@link ExecuteProceduresRequest}. Upon successful dispatch, the Procedure then
 * suspends itself on the Master-side and relinqushes its executor worker. On receipt, the
 * RegionServer submits the Callable to its executor service. When the Callable completes, it adds
 * itself to a queue on the RegionServer side for processing by a background thread, the
 * {@link RemoteProcedureResultReporter}. It picks up the completed Callable from the queue and RPCs
 * the master at #reportProcedureDone with the procedure id and whether success or failure. The
 * master calls complete() setting success or failure state and then reschedules the suspended
 * Procedure so it can finish.
 * <p>
 * Here are some details on operation:
 * <p>
 * If adding the operation to the dispatcher fails, addOperationToNode will throw
 * FailedRemoteDispatchException, and this Procedure will return 'null'. The Procedure Executor will
 * then mark this procedure as 'complete' (though we failed to dispatch our task). In this case, the
 * upper layer of this procedure must have a way to check if this Procedure really succeeded or not
 * and have appropriate handling.
 * <p>
 * If sending the operation to remote RS failed, dispatcher will call remoteCallFailed() to handle
 * this which calls remoteOperationDone with the exception. If the targetServer crashed but this
 * procedure has no response or if we receive failed response, then dispatcher will call
 * remoteOperationFailed() which also calls remoteOperationDone with the exception. If the operation
 * is successful, then remoteOperationCompleted will be called and actually calls the
 * remoteOperationDone without exception. In remoteOperationDone, we'll check if the procedure is
 * already get wake up by others. Then developer could implement complete() based on their own
 * purpose. But basic logic is that if operation succeed, set succ to true and do the clean work. If
 * operation failed and require to resend it to the same server, leave the succ as false. If
 * operation failed and require to resend it to another server, set succ to true and upper layer
 * should be able to find out this operation not work and send a operation to another server.
 */
public abstract class ServerRemoteProcedure extends Procedure<MasterProcedureEnv>
  implements RemoteProcedureDispatcher.RemoteProcedure<MasterProcedureEnv, ServerName> {
  protected static final Logger LOG = LoggerFactory.getLogger(ServerRemoteProcedure.class);
  protected ProcedureEvent<?> event;
  protected ServerName targetServer;
  // after remoteProcedureDone we require error field to decide the next state
  protected Throwable remoteError;
  protected MasterProcedureProtos.ServerRemoteProcedureState state =
    MasterProcedureProtos.ServerRemoteProcedureState.SERVER_REMOTE_PROCEDURE_DISPATCH;

  protected abstract boolean complete(MasterProcedureEnv env, Throwable error);

  @Override
  protected synchronized Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("3db1870c-1b81-34be-bea2-9076dc42dd36"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("31489f87-3f36-3539-8f31-6ec140576745"))) {
return null;
}
    if (
      state != MasterProcedureProtos.ServerRemoteProcedureState.SERVER_REMOTE_PROCEDURE_DISPATCH
    ) {
      if (complete(env, this.remoteError)) {
        return null;
      }
      state = MasterProcedureProtos.ServerRemoteProcedureState.SERVER_REMOTE_PROCEDURE_DISPATCH;
    }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("95cd64ac-0761-388c-8ba3-c5fc38473f53"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("9e6b9ecd-867e-3648-950e-80288f51a0ea"))) { env.getRemoteDispatcher().removeCompletedOperation(targetServer, this); } else { env.getRemoteDispatcher().addOperationToNode(targetServer, this); }
    } catch (FailedRemoteDispatchException frde) {
      LOG.warn("Can not send remote operation {} to {}, this operation will "
        + "be retried to send to another server", this.getProcId(), targetServer);
      return null;
    }
    event = new ProcedureEvent<>(this);
    event.suspendIfNotReady(this);
    throw new ProcedureSuspendedException();
  }

  @Override
  protected synchronized void completionCleanup(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("a3d94682-7a54-3a67-96a4-b00ac82e418f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2a93a965-d165-3697-b7b5-9c9d05f3e454"))) {
return;
}
    env.getRemoteDispatcher().removeCompletedOperation(targetServer, this);
  }

  @Override
  public synchronized void remoteCallFailed(MasterProcedureEnv env, ServerName serverName,
    IOException exception) {
    state = MasterProcedureProtos.ServerRemoteProcedureState.SERVER_REMOTE_PROCEDURE_DISPATCH_FAIL;
    remoteOperationDone(env, exception);
  }

  @Override
  public synchronized void remoteOperationCompleted(MasterProcedureEnv env) {
    state = MasterProcedureProtos.ServerRemoteProcedureState.SERVER_REMOTE_PROCEDURE_REPORT_SUCCEED;
    remoteOperationDone(env, null);
  }

  @Override
  public synchronized void remoteOperationFailed(MasterProcedureEnv env,
    RemoteProcedureException error) {
    state = MasterProcedureProtos.ServerRemoteProcedureState.SERVER_REMOTE_PROCEDURE_REPORT_FAILED;
    remoteOperationDone(env, error);
  }

  synchronized void remoteOperationDone(MasterProcedureEnv env, Throwable error) {
    if (this.isFinished()) {
      if (KnobRuntime.check(java.util.UUID.fromString("6dd0b694-e90f-389d-be9e-c93d78922200"))) { LOG.warn("This procedure {} is already finished, skip the rest processes", getProcId()); } else if (KnobRuntime.check(java.util.UUID.fromString("ac8b69c8-7f56-394c-ab5a-ed33769c30dc"))) { LOG.info("This procedure {} is already finished, skip the rest processes", getProcId()); } else { LOG.info("This procedure {} is already finished, skip the rest processes", this.getProcId()); }
      return;
    }
    if (event == null) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
        getProcId());
      return;
    }
    this.remoteError = error;
    // below persistence is added so that if report goes to last active master, it throws exception
    env.getMasterServices().getMasterProcedureExecutor().getStore().update(this);
    event.wake(env.getProcedureScheduler());
    event = null;
  }
}
