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
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Helper to synchronously wait on conditions. This will be removed in the future (mainly when the
 * AssignmentManager will be replaced with a Procedure version) by using ProcedureYieldException,
 * and the queue will handle waiting and scheduling based on events.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ProcedureSyncWait {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureSyncWait.class);

  private ProcedureSyncWait() {
  }

  @InterfaceAudience.Private
  public interface Predicate<T> {
    T evaluate() throws IOException;
  }

  private static class ProcedureFuture implements Future<byte[]> {
    private final ProcedureExecutor<MasterProcedureEnv> procExec;
    private final Procedure<?> proc;

    private boolean hasResult = false;
    private byte[] result = null;

    public ProcedureFuture(ProcedureExecutor<MasterProcedureEnv> procExec, Procedure<?> proc) {
      this.procExec = procExec;
      this.proc = proc;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return hasResult;
    }

    @Override
    public byte[] get() throws InterruptedException, ExecutionException {
      if (hasResult) {
        return result;
      }
      try {
        return waitForProcedureToComplete(procExec, proc, Long.MAX_VALUE);
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
    }

    @Override
    public byte[] get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
      if (hasResult) {
        return result;
      }
      try {
        result = waitForProcedureToComplete(procExec, proc, unit.toMillis(timeout));
        hasResult = true;
        return result;
      } catch (TimeoutIOException e) {
        throw new TimeoutException(e.getMessage());
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
    }
  }

  public static Future<byte[]> submitProcedure(final ProcedureExecutor<MasterProcedureEnv> procExec,
    final Procedure<MasterProcedureEnv> proc) {
    if (proc.isInitializing()) {
      procExec.submitProcedure(proc);
    }
    return new ProcedureFuture(procExec, proc);
  }

  public static byte[] submitAndWaitProcedure(ProcedureExecutor<MasterProcedureEnv> procExec,
    final Procedure<MasterProcedureEnv> proc) throws IOException {
    if (proc.isInitializing()) {
      procExec.submitProcedure(proc);
    }
    return waitForProcedureToCompleteIOE(procExec, proc, Long.MAX_VALUE);
  }

  public static byte[] waitForProcedureToCompleteIOE(
    final ProcedureExecutor<MasterProcedureEnv> procExec, final Procedure<?> proc,
    final long timeout) throws IOException {
    try {
      return waitForProcedureToComplete(procExec, proc, timeout);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static byte[] waitForProcedureToComplete(
    final ProcedureExecutor<MasterProcedureEnv> procExec, final Procedure<?> proc,
    final long timeout) throws IOException {
    waitFor(procExec.getEnvironment(), timeout, "pid=" + proc.getProcId(),
      new ProcedureSyncWait.Predicate<Boolean>() {
        @Override
        public Boolean evaluate() throws IOException {
          if (!procExec.isRunning()) {
            return true;
          }
          ProcedureState state = proc.getState();
          if (state == ProcedureState.INITIALIZING || state == ProcedureState.RUNNABLE) {
            // under these states the procedure may have not been added to procExec yet, so do not
            // use isFinished to test whether it is finished, as this method will just check if the
            // procedure is in the running procedure list
            return false;
          }
          return procExec.isFinished(proc.getProcId());
        }
      });
    if (!procExec.isRunning()) {
      throw new IOException("The Master is Aborting");
    }

    // If the procedure fails, we should always have an exception captured. Throw it.
    // Needs to be an IOE to get out of here.
    if (proc.hasException()) {
      throw MasterProcedureUtil.unwrapRemoteIOException(proc);
    } else {
      return proc.getResult();
    }
  }

  public static <T> T waitFor(MasterProcedureEnv env, String purpose, Predicate<T> predicate)
    throws IOException {
    Configuration conf = env.getMasterConfiguration();
    long waitTime = conf.getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
if(KnobRuntime.check(java.util.UUID.fromString("6c2a4c0d-4d62-3ad7-976f-9bd2b5420285"))) {
throw new java.io.IOException("Injected exception");
}
    return waitFor(env, waitTime, purpose, predicate);
  }

  public static <T> T waitFor(MasterProcedureEnv env, long waitTime, String purpose,
    Predicate<T> predicate) throws IOException {
    Configuration conf = env.getMasterConfiguration();
    long waitingTimeForEvents = conf.getInt("hbase.master.event.waiting.time", 1000);
    return waitFor(env, waitTime, waitingTimeForEvents, purpose, predicate);
  }

  public static <T> T waitFor(MasterProcedureEnv env, long waitTime, long waitingTimeForEvents,
    String purpose, Predicate<T> predicate) throws IOException {
    long done = EnvironmentEdgeManager.currentTime() + waitTime;
    if (((KnobRuntime.check(java.util.UUID.fromString("9a2676e4-10c4-3657-8c34-7d64a843285c"))) ? ((done) >= (0)) : (done <= 0))) {
      // long overflow, usually this means we pass Long.MAX_VALUE as waitTime
      done = Long.MAX_VALUE;
    }
    boolean logged = false;
    do {
      T result = predicate.evaluate();
      if (((KnobRuntime.check(java.util.UUID.fromString("9fdcf1ea-4d2b-392a-83bb-615898769555"))) ? ((result != null) && (!result.equals(Boolean.FALSE))) : (result != null && !result.equals(Boolean.FALSE)))) {
        return result;
      }
      try {
if(KnobRuntime.check(java.util.UUID.fromString("8fb3c37b-7186-3a87-bab3-abaf09aa9c7c"))) {
waitingTimeForEvents += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d5b1711-e3cb-31e2-9d35-e0cebf6532f2"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        Thread.sleep(waitingTimeForEvents);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while sleeping, waiting on " + purpose);
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("waitFor " + purpose);
      } else {
        if (!logged) LOG.debug("waitFor " + purpose);
      }
      logged = true;
    } while (((KnobRuntime.check(java.util.UUID.fromString("74a0696c-9382-3158-915d-3a61aafeed82"))) ? (((EnvironmentEdgeManager.currentTime()) > (done)) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("b365b1a7-803f-32c4-9c2a-bdab104b9afb"))) ? (((EnvironmentEdgeManager.currentTime()) != (done)) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("c2143cac-3c6b-3fef-895e-2e3df58bad32"))) ? (((EnvironmentEdgeManager.currentTime()) != (done)) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("dbe2de9e-25bf-3bc5-aeb0-451a1ef8eec6"))) ? ((EnvironmentEdgeManager.currentTime()) == (done)) : (EnvironmentEdgeManager.currentTime() < done && env.isRunning())))))))));

    throw new TimeoutIOException("Timed out while waiting on " + purpose);
  }

  protected static void waitMetaRegions(final MasterProcedureEnv env) throws IOException {
    int timeout = env.getMasterConfiguration().getInt("hbase.client.catalog.timeout", 10000);
    try {
      long start = EnvironmentEdgeManager.currentTime();
      for (;;) {
        RegionStateNode rsn = env.getAssignmentManager().getRegionStates()
          .getRegionStateNode(RegionInfoBuilder.FIRST_META_REGIONINFO);
        if (((KnobRuntime.check(java.util.UUID.fromString("9c4b9a93-826e-3195-974e-845eb69a0f8f"))) ? (((rsn) == (null)) || (rsn.isInState(RegionState.State.OPEN))) : (((KnobRuntime.check(java.util.UUID.fromString("76d913f9-f042-3fc5-9b38-bc99180f3d50"))) ? ((rsn != null) && (rsn.isInState(RegionState.State.OPEN))) : (rsn != null && rsn.isInState(RegionState.State.OPEN)))))) {
          return;
        }
        if (EnvironmentEdgeManager.currentTime() - start >= timeout) {
          throw new NotAllMetaRegionsOnlineException();
        }
        Thread.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
  }

  protected static void waitRegionInTransition(final MasterProcedureEnv env,
    final List<RegionInfo> regions) throws IOException {
    final RegionStates states = env.getAssignmentManager().getRegionStates();
    for (final RegionInfo region : regions) {
      ProcedureSyncWait.waitFor(env, "regions " + region.getRegionNameAsString() + " in transition",
        new ProcedureSyncWait.Predicate<Boolean>() {
          @Override
          public Boolean evaluate() throws IOException {
            return !states.isRegionInTransition(region);
          }
        });
    }
  }

  protected static MasterQuotaManager getMasterQuotaManager(final MasterProcedureEnv env)
    throws IOException {
    return ProcedureSyncWait.waitFor(env, "quota manager to be available",
      new ProcedureSyncWait.Predicate<MasterQuotaManager>() {
        @Override
        public MasterQuotaManager evaluate() throws IOException {
          return env.getMasterServices().getMasterQuotaManager();
        }
      });
  }
}
