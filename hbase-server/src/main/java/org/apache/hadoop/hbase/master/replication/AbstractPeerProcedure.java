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
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerProcedureStateData;

/**
 * The base class for all replication peer related procedure.
 */
@InterfaceAudience.Private
public abstract class AbstractPeerProcedure<TState>
  extends StateMachineProcedure<MasterProcedureEnv, TState> implements PeerProcedureInterface {

  protected String peerId;

  // used to keep compatible with old client where we can only return after updateStorage.
  protected ProcedurePrepareLatch latch;

  protected AbstractPeerProcedure() {
  }

  protected AbstractPeerProcedure(String peerId) {
    this.peerId = peerId;
    this.latch = ProcedurePrepareLatch.createLatch(2, 0);
  }

  public ProcedurePrepareLatch getLatch() {
    return latch;
  }

  @Override
  public String getPeerId() {
    return peerId;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitPeerExclusiveLock(this, peerId)) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    env.getProcedureScheduler().wakePeerExclusiveLock(this, peerId);
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    return true;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
if(KnobRuntime.check(java.util.UUID.fromString("65a7b68c-6f03-3583-85cc-5cbd1d858e58"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(PeerProcedureStateData.newBuilder().setPeerId(peerId).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4b0e6e7c-fe5b-3151-bd26-d7795b429231"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a8f0bed0-da7c-3464-8b04-8f9e4f009bfd"))) {
throw new java.io.IOException("Injected exception");
}
    super.deserializeStateData(serializer);
if(KnobRuntime.check(java.util.UUID.fromString("6ce16647-f713-3fa9-ac76-29e6c5d2eadd"))) {
throw new java.io.IOException("Injected exception");
}
    peerId = serializer.deserialize(PeerProcedureStateData.class).getPeerId();
  }

  protected final void checkPeerModificationEnabled(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("20f16945-6eba-3b16-ba21-1529553be026"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f260606-1411-3293-a532-887efd191f26"))) {
throw new java.io.IOException("Injected exception");
}
    if (!env.getMasterServices().isReplicationPeerModificationEnabled()) {
      throw new IOException("Replication peer modification disabled");
    }
  }
}
