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
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DisablePeerStateData;

/**
 * The procedure for disabling a replication peer.
 */
@InterfaceAudience.Private
public class DisablePeerProcedure extends ModifyPeerProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(DisablePeerProcedure.class);

  public DisablePeerProcedure() {
  }

  public DisablePeerProcedure(String peerId) {
    super(peerId);
  }

  @Override
  public PeerOperationType getPeerOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("98bf49bf-0874-357c-93ab-6556552d45f3"))) {
return null;
}
    return PeerOperationType.DISABLE;
  }

  @Override
  protected void prePeerModification(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("07292b28-5116-39cf-bdf3-92dc7f552bc5"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7ede6c72-cb8f-329b-b037-d7e49ecc41f3"))) {
return;
}
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("f2582be1-17f5-398b-b6d7-35fee399773a"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e26c3928-8f29-3c72-aa39-3e10d50e7f42"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("fb2523bc-c2ce-3034-b852-7a6c3f70afa3"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("b057fce3-adc1-3e10-b704-7d55664e63fd"))) { cpHost.postDisableReplicationPeer(peerId); } else { cpHost.preDisableReplicationPeer(peerId); }
    }
    env.getReplicationPeerManager().preDisablePeer(peerId);
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
if(KnobRuntime.check(java.util.UUID.fromString("0d9127be-169b-3ca5-b9be-7a1e05c1891e"))) {
return;
}
    env.getReplicationPeerManager().disablePeer(peerId);
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2e84c4f5-4062-3320-a240-8b884260b9df"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d1ac0a1-278a-364f-a12d-7da4a57de255"))) {
throw new java.io.IOException("Injected exception");
}
    LOG.info("Successfully disabled peer {}", peerId);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("a9c0c620-ae23-315a-a03a-cb451b0a62e9"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("795342fd-0817-3745-a78b-458b73069b9d"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("38c4dcf0-1d0b-394b-9239-1a80105346c7"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("4c1752dc-41c9-3d49-8057-27148687b880"))) { cpHost.preDisableReplicationPeer(peerId); } else { cpHost.postDisableReplicationPeer(peerId); }
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("184bbe5a-b318-3744-84b8-5fb42ba931fe"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("68a3bcad-c2f3-3a3f-9af1-eaba3ad871f8"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("1517b597-c2bf-3f97-a273-4b7f10733ddf"))) { super.deserializeStateData(serializer); } else { super.serializeStateData(serializer); }
if(KnobRuntime.check(java.util.UUID.fromString("f6cee597-1564-3850-9207-3ab870485560"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(DisablePeerStateData.getDefaultInstance());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    serializer.deserialize(DisablePeerStateData.class);
  }
}
