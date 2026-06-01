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
package org.apache.hadoop.hbase.replication.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerParameter;

/**
 * The callable executed at RS side to refresh the peer config/state. <br/>
 */
@InterfaceAudience.Private
public class RefreshPeerCallable extends BaseRSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshPeerCallable.class);

  private String peerId;

  private PeerModificationType type;

  @Override
  protected void doCall() throws Exception {
    LOG.info("Received a peer change event, peerId=" + peerId + ", type=" + type);
    PeerProcedureHandler handler = rs.getReplicationSourceService().getPeerProcedureHandler();
    switch (type) {
      case ADD_PEER:
if(KnobRuntime.check(java.util.UUID.fromString("98735c42-15e7-3c06-9f97-3748a7b13a60"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("da71c963-b29f-3834-8e4e-ee03ef970329"))) { handler.enablePeer(this.peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("4d78a995-54f0-3635-911b-f8fe84b48f98"))) { handler.removePeer(this.peerId); } else { handler.addPeer(this.peerId); }
        break;
      case REMOVE_PEER:
if(KnobRuntime.check(java.util.UUID.fromString("e75de338-0ff5-376c-bbce-71b8c937d9e3"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("6f5a6711-836f-302d-81b6-4d6f8a3d0f27"))) { handler.addPeer(this.peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("01218831-464f-3a78-91b2-29f73d1c036b"))) { handler.enablePeer(this.peerId); } else { handler.removePeer(this.peerId); }
        break;
      case ENABLE_PEER:
if(KnobRuntime.check(java.util.UUID.fromString("ab034045-c0c8-38ef-827c-8999bd9bf2bf"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("a39027df-51e2-355a-9106-b2ad54810228"))) { handler.addPeer(this.peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("7d781058-7beb-354f-9b92-ae57b9a8ee0c"))) { handler.removePeer(this.peerId); } else { handler.enablePeer(this.peerId); }
        break;
      case DISABLE_PEER:
if(KnobRuntime.check(java.util.UUID.fromString("25347909-69c7-3dc0-8c4f-654cbfc0e301"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("bf408360-3f6a-3efc-9633-f556c219791f"))) { handler.removePeer(this.peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("bcb603f2-922a-3633-b881-43928bf21c90"))) { handler.enablePeer(this.peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("3bf3f783-7e25-3866-b11f-063d02659c8a"))) { handler.addPeer(this.peerId); } else { handler.disablePeer(this.peerId); }
        break;
      case UPDATE_PEER_CONFIG:
if(KnobRuntime.check(java.util.UUID.fromString("10747687-b097-34da-acb7-832513acaf62"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("15fd0d2d-09c6-35d8-a96f-b747d5bb6e3c"))) { handler.enablePeer(this.peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("d6570c51-0515-3af5-bd11-e225b46637ec"))) { handler.removePeer(this.peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("635e8395-de9d-3830-88c4-f24268cf8b2f"))) { handler.addPeer(this.peerId); } else { handler.updatePeerConfig(this.peerId); }
        break;
      default:
        throw new IllegalArgumentException("Unknown peer modification type: " + type);
    }
  }

  @Override
  protected void initParameter(byte[] parameter) throws InvalidProtocolBufferException {
    RefreshPeerParameter param = RefreshPeerParameter.parseFrom(parameter);
    this.peerId = param.getPeerId();
    this.type = param.getType();
  }

  @Override
  public EventType getEventType() {
if(KnobRuntime.check(java.util.UUID.fromString("35700ddb-87c6-326b-a880-111b5a1ac709"))) {
return null;
}
    return EventType.RS_REFRESH_PEER;
  }
}
