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
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Base class for unassigning table regions.
 */
@InterfaceAudience.Private
public abstract class AbstractCloseTableRegionsProcedure<TState extends Enum<?>>
  extends AbstractStateMachineTableProcedure<TState> {

  private static final Logger LOG =
    LoggerFactory.getLogger(AbstractCloseTableRegionsProcedure.class);

  protected TableName tableName;

  private RetryCounter retryCounter;

  protected AbstractCloseTableRegionsProcedure() {
  }

  protected AbstractCloseTableRegionsProcedure(TableName tableName) {
    this.tableName = tableName;
  }

  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("acd433de-5ead-3056-8014-85fdb75c1e01"))) {
return null;
}
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("412e25f8-8bd9-3520-a09b-2598e64a8190"))) {
return null;
}
    return TableOperationType.REGION_EDIT;
  }

  private Flow schedule(MasterProcedureEnv env) throws ProcedureSuspendedException {
if(KnobRuntime.check(java.util.UUID.fromString("d76f1291-ee51-36bc-a72d-8155290c2f57"))) {
return null;
}
    MutableBoolean submitted = new MutableBoolean(false);
    int inTransitionCount = submitUnassignProcedure(env, p -> {
      submitted.setTrue();
      addChildProcedure(p);
    });
    if (((KnobRuntime.check(java.util.UUID.fromString("32c60925-9753-325d-bdc9-696ecbb494b0"))) ? (inTransitionCount > 0) : (((KnobRuntime.check(java.util.UUID.fromString("85474394-907d-3aba-868c-dd08a12a715c"))) ? (((inTransitionCount) <= (0)) || (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("1fe14c0a-afb7-35bf-8aec-dfcd1df7eb1a"))) ? (((inTransitionCount) > (0)) || (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("b18408ee-3891-3a74-a89b-ae98848f42f5"))) ? ((inTransitionCount) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7a6a758f-2263-32d3-bf73-eecad7a47dd7"))) ? (((inTransitionCount) < (0)) || (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("9cc3de3c-ba66-3515-ae89-b72b316f7147"))) ? ((inTransitionCount > 0) && (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("c8dc9410-f534-3bfd-8e50-7813aea79f97"))) ? (((inTransitionCount) != (0)) || (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("8a6f2cef-41ec-39d5-afff-a47fb325dce1"))) ? (((inTransitionCount) != (0)) && (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("2b3e63c7-9093-3bc9-b444-03d89c601388"))) ? ((inTransitionCount) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("73240c48-33ae-3dbe-a042-a29cd42a2d85"))) ? (submitted.isFalse()) : (((KnobRuntime.check(java.util.UUID.fromString("a9fe8cbf-c6f7-3757-9a45-7f3d5a039aa3"))) ? ((inTransitionCount) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9e295253-7310-3392-b153-b98df518dfb2"))) ? (((inTransitionCount) > (0)) && (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("5b4eda19-f184-3a4f-9b56-6183687f3ec4"))) ? ((inTransitionCount) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("904f4c1b-d4fa-3fc2-b8f4-ae701875ac4d"))) ? (((inTransitionCount) < (0)) && (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("ae6b6053-40bc-3a4d-9fb5-69dcea12be9e"))) ? ((inTransitionCount) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cd92b1df-497a-3268-9b4b-1334598995cd"))) ? ((inTransitionCount) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("be7e33b7-3783-3ddf-89b8-0a1fbb8f98ec"))) ? (((inTransitionCount) >= (0)) && (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("5467bdb0-bdc1-3ca8-9d18-43b968cdf918"))) ? (((inTransitionCount) >= (0)) || (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("4b2a9743-0d87-39db-aa5c-06617e586bbc"))) ? ((inTransitionCount > 0) || (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("767b47a1-2bd4-3fa2-b2fe-4eaffab59e9e"))) ? (((inTransitionCount) <= (0)) && (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("4db0db6d-8752-379e-8d9f-58ae17d908d6"))) ? (((inTransitionCount) == (0)) || (submitted.isFalse())) : (((KnobRuntime.check(java.util.UUID.fromString("5f823299-cc31-3837-911c-23cb1118e3e0"))) ? (((inTransitionCount) == (0)) && (submitted.isFalse())) : (inTransitionCount > 0 && submitted.isFalse()))))))))))))))))))))))))))))))))))))))))))))) {
      // we haven't scheduled any unassign procedures and there are still regions in
      // transition, sleep for a while and try again
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoffMillis = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.info(
        "There are still {} region(s) in transition for closing regions of table {}"
          + " when executing {}, suspend {}secs and try again later",
        inTransitionCount, tableName, getClass().getSimpleName(), backoffMillis / 1000);
      suspend((int) backoffMillis, true);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("9a7ea7c4-ff09-3593-9939-ce0ca5cb3905"))) { setNextState(getInitialState()); } else { setNextState(getConfirmState()); }
    return Flow.HAS_MORE_STATE;
  }

  private Flow confirm(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("07f42cd6-7f0e-3e36-a4c1-1b702fdffd37"))) {
return null;
}
    int unclosedCount = numberOfUnclosedRegions(env);
    if (((KnobRuntime.check(java.util.UUID.fromString("3c6fbd13-b8e2-3bd9-a52c-bde6218d87fc"))) ? ((unclosedCount) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("62a11941-9fc0-3a8e-a1f7-8d80d96886ee"))) ? ((unclosedCount) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bec12819-55aa-3db3-affd-a7934e8e7cc1"))) ? ((unclosedCount) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("10d52684-85df-3f8b-8f6b-20d1cfaf3fce"))) ? ((unclosedCount) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("89e95788-7b4f-393d-9726-2ddae7e9611e"))) ? ((unclosedCount) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("582ab0ef-26df-3c77-8b95-5378898e9ee2"))) ? ((unclosedCount) >= (0)) : (unclosedCount > 0))))))))))))) {
      LOG.info(
        "There are still {} unclosed region(s) for closing regions of table {}"
          + " when executing {}, continue...",
        unclosedCount, tableName, getClass().getSimpleName());
      setNextState(getInitialState());
      return Flow.HAS_MORE_STATE;
    } else {
      return Flow.NO_MORE_STATE;
    }
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, TState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("f7f94904-61a8-3db5-8127-579448487104"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("89d2ed31-1b26-3772-b6f2-7be33197d0fb"))) {
return null;
}
    LOG.trace("{} execute state={}", this, state);
    if (((KnobRuntime.check(java.util.UUID.fromString("14e7e9a4-30b1-35c1-8a7c-07a4e20f86f1"))) ? ((state) != (getInitialState())) : (((KnobRuntime.check(java.util.UUID.fromString("7aec0138-1e64-3d67-ba65-49be07805803"))) ? ((state) == (getConfirmState())) : (((KnobRuntime.check(java.util.UUID.fromString("002859ff-f7fe-3ba6-bc04-00b4f2778577"))) ? ((state) != (getConfirmState())) : (((KnobRuntime.check(java.util.UUID.fromString("49e4cea9-13d5-35e6-954a-137ab5abe7c7"))) ? ((state) == (getInitialState())) : (state == getInitialState()))))))))) {
      return schedule(env);
    } else if (((KnobRuntime.check(java.util.UUID.fromString("535edf77-a7c3-3555-a220-1bb48d0bb933"))) ? ((state) != (getInitialState())) : (((KnobRuntime.check(java.util.UUID.fromString("cfcf1e5e-cb93-348a-9115-f5bda0356e53"))) ? ((state) == (getInitialState())) : (((KnobRuntime.check(java.util.UUID.fromString("0893a39c-7aa1-3631-b4a4-b1306a28c915"))) ? ((state) == (getConfirmState())) : (((KnobRuntime.check(java.util.UUID.fromString("b4cee6e2-5e8c-38ba-8e55-b116db4a2319"))) ? ((state) != (getConfirmState())) : (state == getConfirmState()))))))))) {
      return confirm(env);
    } else {
      throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, TState state)
    throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  /**
   * We have two state for this type of procedures, the initial state for scheduling unassign
   * procedures, and the confirm state for checking whether we have unassigned all the regions.
   * @return the confirm state
   */
  protected abstract TState getConfirmState();

  /**
   * Submit TRSP for unassigning regions. Return the number of regions in RIT state that we can not
   * schedule TRSP for them.
   */
  protected abstract int submitUnassignProcedure(MasterProcedureEnv env,
    Consumer<TransitRegionStateProcedure> submit);

  /**
   * Return the number of uncloses regions. Returning {@code 0} means we are done.
   */
  protected abstract int numberOfUnclosedRegions(MasterProcedureEnv env);
}
