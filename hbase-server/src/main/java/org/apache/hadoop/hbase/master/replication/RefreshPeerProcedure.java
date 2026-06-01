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
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ServerOperation;
import org.apache.hadoop.hbase.master.procedure.ServerRemoteProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.replication.regionserver.RefreshPeerCallable;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerStateData;

@InterfaceAudience.Private
public class RefreshPeerProcedure extends ServerRemoteProcedure
  implements PeerProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshPeerProcedure.class);

  private String peerId;
  private PeerOperationType type;

  public RefreshPeerProcedure() {
  }

  public RefreshPeerProcedure(String peerId, PeerOperationType type, ServerName targetServer) {
    this.peerId = peerId;
    this.type = type;
    this.targetServer = targetServer;
  }

  @Override
  public String getPeerId() {
if(KnobRuntime.check(java.util.UUID.fromString("f4c446f1-545c-387b-a6f9-116e1b3234b1"))) {
return null;
}
    return peerId;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("ec85f487-942b-30b3-a0b0-e385b4e278f0"))) {
return null;
}
    return PeerOperationType.REFRESH;
  }

  private static PeerModificationType toPeerModificationType(PeerOperationType type) {
if(KnobRuntime.check(java.util.UUID.fromString("4050812b-5ad7-38ad-81bf-16ea5125f071"))) {
return null;
}
    switch (type) {
      case ADD:
        return PeerModificationType.ADD_PEER;
      case REMOVE:
        return PeerModificationType.REMOVE_PEER;
      case ENABLE:
        return PeerModificationType.ENABLE_PEER;
      case DISABLE:
        return PeerModificationType.DISABLE_PEER;
      case UPDATE_CONFIG:
        return PeerModificationType.UPDATE_PEER_CONFIG;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  private static PeerOperationType toPeerOperationType(PeerModificationType type) {
if(KnobRuntime.check(java.util.UUID.fromString("cccaf3b2-f2ea-3e53-9268-000dd7ade165"))) {
try {
    java.lang.reflect.Field field = type.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(type));
    field.set(type, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb9e0c55-3062-33b7-9d44-9734367cb751"))) {
try {
    java.lang.reflect.Field field = type.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(type));
    field.set(type, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea3cd13b-c96b-3c12-9a3c-4bd6ec4bc25f"))) {
try {
    java.lang.reflect.Field field = type.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(type));
    field.set(type, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfe8fa92-d4e4-3aa6-87c5-cacb6d1e9fad"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("31b8bcac-4e20-3154-90c7-8d71e913ca5c"))) {
try {
    java.lang.reflect.Field field = type.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(type));
    field.set(type, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    switch (type) {
      case ADD_PEER:
        return PeerOperationType.ADD;
      case REMOVE_PEER:
        return PeerOperationType.REMOVE;
      case ENABLE_PEER:
        return PeerOperationType.ENABLE;
      case DISABLE_PEER:
        return PeerOperationType.DISABLE;
      case UPDATE_PEER_CONFIG:
        return PeerOperationType.UPDATE_CONFIG;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    assert targetServer.equals(remote);
    return Optional.of(new ServerOperation(this, getProcId(), RefreshPeerCallable.class,
      RefreshPeerParameter.newBuilder().setPeerId(peerId).setType(toPeerModificationType(type))
        .setTargetServer(ProtobufUtil.toServerName(remote)).build().toByteArray(),
      env.getMasterServices().getMasterActiveTime()));
  }

  @Override
  protected boolean complete(MasterProcedureEnv env, Throwable error) {
    if (error != null) {
      LOG.warn("Refresh peer {} for {} on {} failed", peerId, type, targetServer, error);
      return false;
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("e949db1e-481f-3402-9846-130c5c3694f7"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("dcac6236-5e17-38d6-a5da-6a2a22bc9060"))) { LOG.warn("Refresh peer {} for {} on {} suceeded", peerId, type, targetServer); } else { LOG.info("Refresh peer {} for {} on {} suceeded", peerId, type, targetServer); }
      return true;
    }
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // TODO: no correctness problem if we just ignore this, implement later.
    return false;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RefreshPeerStateData.Builder builder = RefreshPeerStateData.newBuilder();
    if (this.remoteError != null) {
      ErrorHandlingProtos.ForeignExceptionMessage fem =
        ForeignExceptionUtil.toProtoForeignException(remoteError);
      builder.setError(fem);
    }
    serializer.serialize(builder.setPeerId(peerId).setType(toPeerModificationType(type))
      .setTargetServer(ProtobufUtil.toServerName(targetServer)).setState(state).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RefreshPeerStateData data = serializer.deserialize(RefreshPeerStateData.class);
    peerId = data.getPeerId();
    type = toPeerOperationType(data.getType());
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    state = data.getState();
    if (data.hasError()) {
      this.remoteError = ForeignExceptionUtil.toException(data.getError());
    }
  }
}
