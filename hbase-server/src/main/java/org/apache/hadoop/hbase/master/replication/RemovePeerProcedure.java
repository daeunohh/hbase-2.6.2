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
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RemovePeerStateData;

/**
 * The procedure for removing a replication peer.
 */
@InterfaceAudience.Private
public class RemovePeerProcedure extends ModifyPeerProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(RemovePeerProcedure.class);

  private ReplicationPeerConfig peerConfig;

  public RemovePeerProcedure() {
  }

  public RemovePeerProcedure(String peerId) {
    super(peerId);
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.REMOVE;
  }

  @Override
  protected void prePeerModification(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("5ef388ff-676d-3534-a2de-0df9b26ec5e3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9a33f021-d90a-309d-89ba-e3bbe1022011"))) {
return;
}
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preRemoveReplicationPeer(peerId);
    }
    peerConfig = env.getReplicationPeerManager().preRemovePeer(peerId);
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
if(KnobRuntime.check(java.util.UUID.fromString("7f498b3a-08ea-36b3-85ed-f8dceed5f29d"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("745483fa-9d73-3956-803d-63ac2c0aba86"))) { env.getReplicationPeerManager().removeAllLastPushedSeqIds(peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("955e2b8e-341c-3767-8e7e-436219506ac7"))) { env.getReplicationPeerManager().removeAllQueuesAndHFileRefs(peerId); } else { env.getReplicationPeerManager().removePeer(peerId); }
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env)
    throws IOException, ReplicationException {
    env.getReplicationPeerManager().removeAllQueuesAndHFileRefs(peerId);
    if (peerConfig.isSerial()) {
      env.getReplicationPeerManager().removeAllLastPushedSeqIds(peerId);
    }
    LOG.info("Successfully removed peer {}", peerId);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("a8471cdb-bf83-3301-a308-7d52a3b89be8"))) ? ((cpHost) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("075bb06f-949a-3d7f-9b59-5714e1698ca4"))) ? ((cpHost) == (null)) : (cpHost != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("f1032574-0e9c-3d03-a935-80e2c0424348"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("176f734e-f5d7-3ca5-87e8-b23af8d25e2b"))) { cpHost.preRemoveReplicationPeer(peerId); } else { cpHost.postRemoveReplicationPeer(peerId); }
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RemovePeerStateData.Builder builder = RemovePeerStateData.newBuilder();
    if (peerConfig != null) {
      builder.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9fa97d0c-f962-3858-8827-3fbb86773316"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("acdf0b9d-ec50-33cb-9230-b98efb4c2cab"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("405c9027-a4ee-3553-9674-a7dd4a22205b"))) { super.serializeStateData(serializer); } else { super.deserializeStateData(serializer); }
if(KnobRuntime.check(java.util.UUID.fromString("9c902b46-f0fd-3188-910c-eeafd85be227"))) {
throw new java.io.IOException("Injected exception");
}
    RemovePeerStateData data = serializer.deserialize(RemovePeerStateData.class);
    if (data.hasPeerConfig()) {
      this.peerConfig = ReplicationPeerConfigUtil.convert(data.getPeerConfig());
    }
  }
}
