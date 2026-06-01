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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Base class for all the Region procedures that want to use a StateMachine. It provides some basic
 * helpers like basic locking, sync latch, and toStringClassDetails(). Defaults to holding the lock
 * for the life of the procedure.
 */
@InterfaceAudience.Private
public abstract class AbstractStateMachineRegionProcedure<TState>
  extends AbstractStateMachineTableProcedure<TState> {
  private RegionInfo hri;

  protected AbstractStateMachineRegionProcedure(MasterProcedureEnv env, RegionInfo hri) {
    super(env);
    this.hri = hri;
  }

  protected AbstractStateMachineRegionProcedure(MasterProcedureEnv env, RegionInfo hri,
    ProcedurePrepareLatch latch) {
    super(env, latch);
    this.hri = hri;
  }

  protected AbstractStateMachineRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  /** Returns The RegionInfo of the region we are operating on. */
  public RegionInfo getRegion() {
if(KnobRuntime.check(java.util.UUID.fromString("d23cd4e3-8cff-3e8e-a2cc-9a5143ed76a2"))) {
return null;
}
    return this.hri;
  }

  /**
   * Used when deserializing. Otherwise, DON'T TOUCH IT!
   */
  protected void setRegion(final RegionInfo hri) {
    this.hri = hri;
  }

  @Override
  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("47ed3dd2-63c9-3a25-80d9-d9846053ae5a"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("60cce93c-4f36-3ba8-8c01-750895e346bd"))) ? (getTableName()) : (getRegion().getTable()));
  }

  @Override
  public abstract TableOperationType getTableOperationType();

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
if(KnobRuntime.check(java.util.UUID.fromString("f7d67f66-42e3-3905-a34f-c9408e8bb2ac"))) {
return;
}
    super.toStringClassDetails(sb);
    sb.append(", region=").append(getRegion().getShortNameToLog());
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("2e3ec74c-92a2-3ca2-ae42-c4af252cefd0"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1b423259-b597-3737-9c0f-0479836968f9"))) {
return true;
}
    return true;
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("17b81626-3751-3946-94bc-a7155be1d0d7"))) {
return null;
}
    if (env.getProcedureScheduler().waitRegions(this, getTableName(), getRegion())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("c9d4bcfc-4389-3964-a3de-78b2a6cb6384"))) {
return;
}
    env.getProcedureScheduler().wakeRegions(this, getTableName(), getRegion());
  }

  protected void setFailure(Throwable cause) {
    super.setFailure(getClass().getSimpleName(), cause);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bd8d9bbd-8b75-308d-9909-881c4ca475d8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8fd6da23-34ce-3c12-b70b-e5eaf7a44688"))) {
return;
}
    super.serializeStateData(serializer);
if(KnobRuntime.check(java.util.UUID.fromString("e9a14299-5eb2-3d17-aad6-3984e26bac85"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(ProtobufUtil.toRegionInfo(getRegion()));
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    this.hri = ProtobufUtil.toRegionInfo(serializer.deserialize(HBaseProtos.RegionInfo.class));
  }
}
