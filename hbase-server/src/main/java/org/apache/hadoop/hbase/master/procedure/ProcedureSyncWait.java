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
if(KnobRuntime.check(java.util.UUID.fromString("2e839fc6-fb9d-33f9-99cf-401fb9931374"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("87affe82-e032-3a3e-8add-248c2d73e98e"))) {
return null;
}
    Configuration conf = env.getMasterConfiguration();
    long waitTime = conf.getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
if(KnobRuntime.check(java.util.UUID.fromString("6c2a4c0d-4d62-3ad7-976f-9bd2b5420285"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e0acc713-ce4a-3b94-9743-9e8750edb596"))) {
waitTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1f98cf91-83f1-36b1-802e-4fc9a91447e7"))) {
waitTime = -1;
}
    return waitFor(env, waitTime, purpose, predicate);
  }

  public static <T> T waitFor(MasterProcedureEnv env, long waitTime, String purpose,
    Predicate<T> predicate) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f77749be-80fa-36f1-a3d7-f08c8a2f3e81"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("06cf64eb-840b-30e9-bffb-6f7500f0dc71"))) {
return null;
}
    Configuration conf = env.getMasterConfiguration();
    long waitingTimeForEvents = conf.getInt("hbase.master.event.waiting.time", 1000);
if(KnobRuntime.check(java.util.UUID.fromString("e018498a-49c0-321b-932a-27f78752eba0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("eeede970-06e6-342e-8531-c86dc517a8ab"))) {
waitTime = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a5306c11-da00-3ac4-ad40-f94c70f9a0cd"))) {
waitingTimeForEvents += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0344cc76-4611-326e-ba50-419dc81ae5cf"))) {
waitTime = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ee473dc6-5db8-3c9c-84fe-438839ecb5bb"))) {
waitingTimeForEvents = 0;
}
    return waitFor(env, waitTime, waitingTimeForEvents, purpose, predicate);
  }

  public static <T> T waitFor(MasterProcedureEnv env, long waitTime, long waitingTimeForEvents,
    String purpose, Predicate<T> predicate) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("67818287-a9c9-3ec2-957c-5834700b1db0"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a8d59724-a4d4-3e83-a11e-b7fd054119bf"))) {
throw new java.io.IOException("Injected exception");
}
    long done = EnvironmentEdgeManager.currentTime() + waitTime;
    if (((KnobRuntime.check(java.util.UUID.fromString("2f92af4f-82fb-3f42-b049-4260770d2941"))) ? ((done) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d76bef9d-8e87-34b6-861b-7ff246e1537b"))) ? ((done) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f8d541ad-06ef-3094-a843-02416864cf5e"))) ? ((done) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9a2676e4-10c4-3657-8c34-7d64a843285c"))) ? ((done) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("11f1d790-2adf-3fd1-9685-fffe64780d47"))) ? ((done) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3108e0a9-a0b3-33d4-8ba4-10dbeba014db"))) ? ((done) > (0)) : (done <= 0))))))))))))) {
      // long overflow, usually this means we pass Long.MAX_VALUE as waitTime
      done = Long.MAX_VALUE;
    }
    boolean logged = false;
    do {
if(KnobRuntime.check(java.util.UUID.fromString("3ea3ecab-97b9-335c-baa6-cb57b6414fe7"))) {
throw new java.io.IOException("Injected exception");
}
      T result = predicate.evaluate();
      if (((KnobRuntime.check(java.util.UUID.fromString("6ad70e39-9d95-344b-80c4-a9f15468a7dc"))) ? ((result) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9fdcf1ea-4d2b-392a-83bb-615898769555"))) ? ((result != null) && (!result.equals(Boolean.FALSE))) : (((KnobRuntime.check(java.util.UUID.fromString("b13391c7-0483-3064-a960-9037842a8145"))) ? (result != null) : (((KnobRuntime.check(java.util.UUID.fromString("3b5d94ad-a2f3-3ef7-9658-8e57125a2f71"))) ? (((result) != (null)) && (!result.equals(Boolean.FALSE))) : (((KnobRuntime.check(java.util.UUID.fromString("be22761c-8cf8-345a-9ad1-c5013a7d5e1c"))) ? ((result) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("319af535-8876-30f5-b429-2762810d766b"))) ? (((result) == (null)) && (!result.equals(Boolean.FALSE))) : (((KnobRuntime.check(java.util.UUID.fromString("0c00a839-2070-3bf0-8914-edce963609d0"))) ? (((result) == (null)) || (!result.equals(Boolean.FALSE))) : (((KnobRuntime.check(java.util.UUID.fromString("f917838f-553c-3a3a-adb1-104909fe3c25"))) ? (!result.equals(Boolean.FALSE)) : (((KnobRuntime.check(java.util.UUID.fromString("1dbf34d9-ed5e-311f-8585-7043a579a945"))) ? (((result) != (null)) || (!result.equals(Boolean.FALSE))) : (((KnobRuntime.check(java.util.UUID.fromString("98fb034f-80c2-3431-a794-a15addad70ef"))) ? ((result != null) || (!result.equals(Boolean.FALSE))) : (result != null && !result.equals(Boolean.FALSE)))))))))))))))))))))) {
        return result;
      }
      try {
if(KnobRuntime.check(java.util.UUID.fromString("8d5b1711-e3cb-31e2-9d35-e0cebf6532f2"))) {
throw new java.lang.InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8fb3c37b-7186-3a87-bab3-abaf09aa9c7c"))) {
waitingTimeForEvents += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5af85011-8c56-3b25-950f-06b6247a0942"))) {
waitingTimeForEvents = 0;
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
    } while (((KnobRuntime.check(java.util.UUID.fromString("f47aea01-09f8-348b-b99a-23d883df14a1"))) ? ((EnvironmentEdgeManager.currentTime()) < (done)) : (((KnobRuntime.check(java.util.UUID.fromString("e2e4cb09-56e1-3ce7-b50f-ee0560c6a2a7"))) ? (((EnvironmentEdgeManager.currentTime()) <= (done)) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("b365b1a7-803f-32c4-9c2a-bdab104b9afb"))) ? (((EnvironmentEdgeManager.currentTime()) != (done)) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("f49bb66d-4119-326e-a972-848d1a443020"))) ? (((EnvironmentEdgeManager.currentTime()) >= (done)) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("7139c596-b4f0-35b5-8671-7ceac177b697"))) ? (((EnvironmentEdgeManager.currentTime()) > (done)) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("67e33afa-cd29-30ea-89b5-df44f5c6d7ed"))) ? (((EnvironmentEdgeManager.currentTime()) < (done)) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("d0e21347-f784-30f6-afc9-115557fe2240"))) ? (((EnvironmentEdgeManager.currentTime()) >= (done)) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("46c9d135-a8cc-320f-97b3-d92cc684e20a"))) ? ((EnvironmentEdgeManager.currentTime() < done) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("252f279d-d9cf-3e12-8d92-ced71c30c4ca"))) ? ((EnvironmentEdgeManager.currentTime()) >= (done)) : (((KnobRuntime.check(java.util.UUID.fromString("2e16e60c-3ed8-38b6-9c55-8fca1a8b8c38"))) ? (((EnvironmentEdgeManager.currentTime()) < (done)) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("1ee1fab8-d950-3c80-b374-a6758808a3c5"))) ? (((EnvironmentEdgeManager.currentTime()) == (done)) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("56594802-a7eb-38a4-9a11-e5d826ff3119"))) ? ((EnvironmentEdgeManager.currentTime()) > (done)) : (((KnobRuntime.check(java.util.UUID.fromString("09ee0ac2-fd8b-3725-9f25-e0c751f4f6ce"))) ? (env.isRunning()) : (((KnobRuntime.check(java.util.UUID.fromString("dbe2de9e-25bf-3bc5-aeb0-451a1ef8eec6"))) ? ((EnvironmentEdgeManager.currentTime()) == (done)) : (((KnobRuntime.check(java.util.UUID.fromString("1c9c66eb-cdba-36e7-a2ec-2fea223602ad"))) ? (((EnvironmentEdgeManager.currentTime()) <= (done)) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("9a7d4b10-e416-3dbc-b73c-f60633b26969"))) ? (EnvironmentEdgeManager.currentTime() < done) : (((KnobRuntime.check(java.util.UUID.fromString("fbef4185-ce43-397a-8dd9-16c97d8fa775"))) ? ((EnvironmentEdgeManager.currentTime()) != (done)) : (((KnobRuntime.check(java.util.UUID.fromString("c2143cac-3c6b-3fef-895e-2e3df58bad32"))) ? (((EnvironmentEdgeManager.currentTime()) != (done)) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("94d2c2ec-6ae8-3710-9d7f-6c886c5afa36"))) ? ((EnvironmentEdgeManager.currentTime() < done) && (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("848ccf6e-65a5-3da9-9591-71be611031b4"))) ? ((EnvironmentEdgeManager.currentTime()) <= (done)) : (((KnobRuntime.check(java.util.UUID.fromString("9e2e0d24-2b9a-3607-b31d-a313ea74f3fc"))) ? (((EnvironmentEdgeManager.currentTime()) == (done)) || (env.isRunning())) : (((KnobRuntime.check(java.util.UUID.fromString("74a0696c-9382-3158-915d-3a61aafeed82"))) ? (((EnvironmentEdgeManager.currentTime()) > (done)) || (env.isRunning())) : (EnvironmentEdgeManager.currentTime() < done && env.isRunning())))))))))))))))))))))))))))))))))))))))))))));

    throw new TimeoutIOException("Timed out while waiting on " + purpose);
  }

  protected static void waitMetaRegions(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1ae0d768-e463-31b2-a066-189afb4314b7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("848e0887-8491-3cd6-8a1e-74f4a565cfff"))) {
return;
}
    int timeout = env.getMasterConfiguration().getInt("hbase.client.catalog.timeout", 10000);
    try {
      long start = EnvironmentEdgeManager.currentTime();
      for (;;) {
        RegionStateNode rsn = env.getAssignmentManager().getRegionStates()
          .getRegionStateNode(RegionInfoBuilder.FIRST_META_REGIONINFO);
        if (((KnobRuntime.check(java.util.UUID.fromString("2257abbb-a3ea-30a1-85b9-12cc29937aab"))) ? (((rsn) != (null)) || (rsn.isInState(RegionState.State.OPEN))) : (((KnobRuntime.check(java.util.UUID.fromString("0ff35c29-a005-3bea-adc1-85e9796bddee"))) ? (((rsn) == (null)) && (rsn.isInState(RegionState.State.OPEN))) : (((KnobRuntime.check(java.util.UUID.fromString("76d913f9-f042-3fc5-9b38-bc99180f3d50"))) ? ((rsn != null) && (rsn.isInState(RegionState.State.OPEN))) : (((KnobRuntime.check(java.util.UUID.fromString("a90f368d-6b17-3590-8f5f-e54491f9a240"))) ? (rsn != null) : (((KnobRuntime.check(java.util.UUID.fromString("9c4b9a93-826e-3195-974e-845eb69a0f8f"))) ? (((rsn) == (null)) || (rsn.isInState(RegionState.State.OPEN))) : (((KnobRuntime.check(java.util.UUID.fromString("241ebd1a-95ed-3509-b266-5086a85d7242"))) ? ((rsn) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9cb9d717-3c5b-3858-aadd-b23a14a6ebfb"))) ? ((rsn != null) || (rsn.isInState(RegionState.State.OPEN))) : (((KnobRuntime.check(java.util.UUID.fromString("0590f302-10d6-3c71-9323-ad07906b9b6a"))) ? (rsn.isInState(RegionState.State.OPEN)) : (((KnobRuntime.check(java.util.UUID.fromString("46cadb88-7f30-36ad-b289-c844e06867ba"))) ? (((rsn) != (null)) && (rsn.isInState(RegionState.State.OPEN))) : (((KnobRuntime.check(java.util.UUID.fromString("3d84fdda-09cb-309d-883a-0480922fd1ff"))) ? ((rsn) != (null)) : (rsn != null && rsn.isInState(RegionState.State.OPEN)))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9d66131d-a519-3da3-827d-2191fd8d33b4"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("64aa8724-cda3-367e-bf9c-1ac8defd95e5"))) {
throw new java.io.IOException("Injected exception");
}
    final RegionStates states = env.getAssignmentManager().getRegionStates();
    for (final RegionInfo region : regions) {
if(KnobRuntime.check(java.util.UUID.fromString("08b6e486-a681-30d6-9e38-5e991828bdbb"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("16a1946b-9858-306e-aeb9-eb446eb75005"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("94a08017-09ff-34fe-ab56-76e342eae87e"))) {
return null;
}
    return ProcedureSyncWait.waitFor(env, "quota manager to be available",
      new ProcedureSyncWait.Predicate<MasterQuotaManager>() {
        @Override
        public MasterQuotaManager evaluate() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("38ef5d41-2bb9-3f37-a864-aface42cee8a"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("f2aea4c5-3c0d-3fb2-baf5-00a884ac3fa9"))) {
throw new java.io.IOException("Injected exception");
}
          return env.getMasterServices().getMasterQuotaManager();
        }
      });
  }
}
