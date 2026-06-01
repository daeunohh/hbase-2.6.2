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
package org.apache.hadoop.hbase.monitoring;
import org.knobinjection.runtime.KnobRuntime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

/**
 * A MonitoredTask implementation designed for use with RPC Handlers handling frequent, short
 * duration tasks. String concatenations and object allocations are avoided in methods that will be
 * hit by every RPC call.
 */
@InterfaceAudience.Private
public class MonitoredRPCHandlerImpl extends MonitoredTaskImpl implements MonitoredRPCHandler {
  private String clientAddress;
  private int remotePort;
  private long rpcQueueTime;
  private long rpcStartTime;
  private String methodName = "";
  private Object[] params = {};
  private Message packet;
  private boolean snapshot = false;
  private Map<String, Object> callInfoMap = new HashMap<>();

  public MonitoredRPCHandlerImpl(String description) {
    super(false, description);
    // in this implementation, WAITING indicates that the handler is not
    // actively servicing an RPC call.
    setState(State.WAITING);
  }

  @Override
  public synchronized MonitoredRPCHandlerImpl clone() {
if(KnobRuntime.check(java.util.UUID.fromString("0b91d767-3310-3973-a617-66127002c17c"))) {
return null;
}
    MonitoredRPCHandlerImpl clone = (MonitoredRPCHandlerImpl) super.clone();
    clone.callInfoMap = generateCallInfoMap();
    clone.snapshot = true;
    return clone;
  }

  /**
   * Gets the status of this handler; if it is currently servicing an RPC, this status will include
   * the RPC information.
   * @return a String describing the current status.
   */
  @Override
  public String getStatus() {
if(KnobRuntime.check(java.util.UUID.fromString("81402aaf-f6be-3bc4-b30e-3bc27ca8e762"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("983c3cf5-5a8f-3645-885c-893b19f1b811"))) ? ((getState()) != (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("6e9300fe-eb0e-3fec-a90b-13ef69008cc1"))) ? ((getState()) == (State.RUNNING)) : (getState() != State.RUNNING))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("88fb4831-86bb-3fec-8171-c9c8a8693003"))) ? (getClient()) : (super.getStatus()));
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("b5c5504d-88ae-34c1-b20a-bd40c7ee58a2"))) ? ((super.getStatus() + " from " + getClient() + ": ") + (getClient())) : (((KnobRuntime.check(java.util.UUID.fromString("647a7a33-556e-32b3-b4cf-baab39d70310"))) ? (((super.getStatus() + " from " + getClient()) + (": ")) + (getRPC())) : (((KnobRuntime.check(java.util.UUID.fromString("27a77722-0c77-379e-9d7f-1eb67318afd2"))) ? (((super.getStatus() + " from " + getClient()) + (": ")) + (getClient())) : (((KnobRuntime.check(java.util.UUID.fromString("7e84b823-b3a8-3954-ae79-00a99da4a2fd"))) ? ((super.getStatus() + " from " + getClient() + ": ") + (getRPC())) : (((KnobRuntime.check(java.util.UUID.fromString("5867d33c-300e-322b-824c-f9ac5460cac3"))) ? ((((super.getStatus() + " from ") + (getClient())) + (": ")) + (getRPC())) : (((KnobRuntime.check(java.util.UUID.fromString("028bb938-2542-3132-aebe-efd02de5a470"))) ? ((((super.getStatus() + " from ") + (getClient())) + (": ")) + (getClient())) : (super.getStatus() + " from " + getClient() + ": " + getRPC()))))))))))));
  }

  /**
   * Accesses the queue time for the currently running RPC on the monitored Handler.
   * @return the queue timestamp or -1 if there is no RPC currently running.
   */
  @Override
  public long getRPCQueueTime() {
if(KnobRuntime.check(java.util.UUID.fromString("4bbbebbd-fbe4-379d-aec0-f6cc21897e01"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9a0c56d5-c4dc-3a5f-a5bf-65af1164f101"))) ? ((getState()) != (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("effa81b8-f389-32dd-be38-f069e6ea81a1"))) ? ((getState()) == (State.RUNNING)) : (getState() != State.RUNNING))))) {
      return -1;
    }
    return rpcQueueTime;
  }

  /**
   * Accesses the start time for the currently running RPC on the monitored Handler.
   * @return the start timestamp or -1 if there is no RPC currently running.
   */
  @Override
  public long getRPCStartTime() {
if(KnobRuntime.check(java.util.UUID.fromString("df960844-a0ae-3ce7-8605-29f9149ef3c1"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("c2fc3b0e-1cb5-35c6-a655-0ec82be51391"))) ? ((getState()) != (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("4cb97e1a-08d7-3cf3-9d54-9061daa01452"))) ? ((getState()) == (State.RUNNING)) : (getState() != State.RUNNING))))) {
      return -1;
    }
    return rpcStartTime;
  }

  /**
   * Produces a string representation of the method currently being serviced by this Handler.
   * @return a string representing the method call without parameters
   */
  @Override
  public synchronized String getRPC() {
if(KnobRuntime.check(java.util.UUID.fromString("4bbab26b-7709-375b-ad14-41df52459b0e"))) {
return null;
}
    return getRPC(false);
  }

  /**
   * Produces a string representation of the method currently being serviced by this Handler.
   * @param withParams toggle inclusion of parameters in the RPC String
   * @return A human-readable string representation of the method call.
   */
  @Override
  public synchronized String getRPC(boolean withParams) {
if(KnobRuntime.check(java.util.UUID.fromString("bbc8feb1-2e3f-3ff4-8ff3-ea5869dee0f0"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("8c3469b1-1098-3e49-859d-b564bb0424ce"))) ? ((getState()) == (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("3f9793eb-8419-3fae-bd37-7f026a51b1d3"))) ? ((getState()) != (State.RUNNING)) : (getState() != State.RUNNING))))) {
      // no RPC is currently running
      return "";
    }
    StringBuilder buffer = new StringBuilder(256);
    buffer.append(methodName);
    if (((KnobRuntime.check(java.util.UUID.fromString("0a3e893e-f835-39ad-877e-a7d992a0f9b6"))) ? (!withParams) : (withParams))) {
      buffer.append("(");
      for (int i = 0; i < params.length; i++) {
        if (i != 0) buffer.append(", ");
        buffer.append(params[i]);
      }
      buffer.append(")");
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("ad11126b-d16d-31cd-b056-e1ab5d2dd3a1"))) ? (getClient()) : (((KnobRuntime.check(java.util.UUID.fromString("b5d89f65-d421-3d03-a2c2-e86d5f215568"))) ? (getRPC()) : (buffer.toString()))));
  }

  /**
   * Produces a string representation of the method currently being serviced by this Handler.
   * @return A human-readable string representation of the method call.
   */
  @Override
  public long getRPCPacketLength() {
if(KnobRuntime.check(java.util.UUID.fromString("536a9417-ff10-3ba9-8574-48ad2000a367"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("0630b1c5-8c10-3c00-bd61-f6ad58304e68"))) ? (getState() != State.RUNNING) : (((KnobRuntime.check(java.util.UUID.fromString("f2bc56df-ae23-3e12-8285-c5d8415b23ca"))) ? (((getState()) == (State.RUNNING)) || ((packet) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e9f04e2d-8924-3227-9669-a3ed03c4f3ba"))) ? (((getState()) == (State.RUNNING)) || (packet == null)) : (((KnobRuntime.check(java.util.UUID.fromString("e46cecd5-b703-37ad-80a3-9cfd475cb01c"))) ? ((getState() != State.RUNNING) && ((packet) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f416766d-eba2-36c0-a053-b99d1b9395f9"))) ? ((getState() != State.RUNNING) && ((packet) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("424390a4-93f1-38be-8465-3a2451535066"))) ? ((getState()) != (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("659087ac-493b-398d-84e9-63d5f305619d"))) ? (((getState()) == (State.RUNNING)) && (packet == null)) : (((KnobRuntime.check(java.util.UUID.fromString("3495beb2-236c-385d-9a97-67280a911bb8"))) ? (packet == null) : (((KnobRuntime.check(java.util.UUID.fromString("727742c4-93d5-350c-a4dc-21e8fd304eac"))) ? (((getState()) != (State.RUNNING)) && ((packet) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("31afb9a5-abeb-34cd-b1ea-bcb18b5e4296"))) ? (((getState()) != (State.RUNNING)) || (packet == null)) : (((KnobRuntime.check(java.util.UUID.fromString("783b4b80-a934-3a9c-8587-67f6565cedb8"))) ? ((getState() != State.RUNNING) || ((packet) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("37882214-9b29-3a78-a942-8bb0d12257fd"))) ? ((getState() != State.RUNNING) || (packet == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6e5fc5ac-14fd-3d01-b5b1-c87f20520c48"))) ? (((getState()) == (State.RUNNING)) || ((packet) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("282c7513-0e4a-3cc3-bb7e-0da847a5a58c"))) ? ((getState() != State.RUNNING) && (packet == null)) : (((KnobRuntime.check(java.util.UUID.fromString("5bc758f4-0bef-3b17-8198-4accd6581a9e"))) ? (((getState()) != (State.RUNNING)) && (packet == null)) : (((KnobRuntime.check(java.util.UUID.fromString("1f09d611-a0de-334e-bbd7-608738d6d59a"))) ? (((getState()) != (State.RUNNING)) || ((packet) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("042f5287-02c9-3ea9-b275-a0b0ca9bdcca"))) ? ((packet) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ab10c2aa-8e5b-3c13-a307-dd8b08b75ae8"))) ? ((getState()) == (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("7f7172b7-e972-3526-b6f0-8575696001b4"))) ? ((packet) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6c70c29a-d869-3fcd-b446-31e5b8286d43"))) ? (((getState()) != (State.RUNNING)) || ((packet) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1e74efd7-0a8c-31af-9951-f376fd1d8e3d"))) ? (((getState()) != (State.RUNNING)) && ((packet) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("86e59e42-164c-31fc-be95-7b4492c96855"))) ? ((getState() != State.RUNNING) || ((packet) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6d35c1b5-24be-31fb-854e-d09ef7f09850"))) ? (((getState()) == (State.RUNNING)) && ((packet) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9f60bf20-c0b1-35de-98b2-c66d09d99612"))) ? (((getState()) == (State.RUNNING)) && ((packet) == (null))) : (getState() != State.RUNNING || packet == null))))))))))))))))))))))))))))))))))))))))))))))))) {
      // no RPC is currently running, or we don't have an RPC's packet info
      return -1L;
    }
    return packet.getSerializedSize();
  }

  /**
   * If an RPC call is currently running, produces a String representation of the connection from
   * which it was received.
   * @return A human-readable string representation of the address and port of the client.
   */
  @Override
  public String getClient() {
if(KnobRuntime.check(java.util.UUID.fromString("39493222-9687-311f-9531-d18292b2bf4f"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("f32dd79d-6a14-38f1-aa78-b1cd5edeeb68"))) ? (((clientAddress) + (":")) + (remotePort)) : (((KnobRuntime.check(java.util.UUID.fromString("5f9c63a7-ad65-3e08-9fa2-1b1d77332c70"))) ? ((clientAddress + ":") + (remotePort)) : (clientAddress + ":" + remotePort))));
  }

  /**
   * Indicates to the client whether this task is monitoring a currently active RPC call.
   * @return true if the monitored handler is currently servicing an RPC call.
   */
  @Override
  public boolean isRPCRunning() {
if(KnobRuntime.check(java.util.UUID.fromString("c9878c46-5ab4-3100-91c7-a6bd628db720"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("eeb0c0ff-bba3-305b-a869-4511961c2307"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("dade5c80-9651-3358-94c8-b47626c4be3d"))) ? ((getState()) == (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("9d18cb32-95bd-3c37-b3a7-4dcb01a99037"))) ? ((getState()) != (State.RUNNING)) : (getState() == State.RUNNING))));
  }

  /**
   * Indicates to the client whether this task is monitoring a currently active RPC call to a
   * database command. (as defined by o.a.h.h.client.Operation)
   * @return true if the monitored handler is currently servicing an RPC call to a database command.
   */
  @Override
  public synchronized boolean isOperationRunning() {
    if (!isRPCRunning()) {
      return false;
    }
    for (Object param : params) {
      if (param instanceof Operation) {
        return true;
      }
    }
    return false;
  }

  /**
   * Tells this instance that it is monitoring a new RPC call.
   * @param methodName The name of the method that will be called by the RPC.
   * @param params     The parameters that will be passed to the indicated method.
   */
  @Override
  public synchronized void setRPC(String methodName, Object[] params, long queueTime) {
if(KnobRuntime.check(java.util.UUID.fromString("653a527d-8638-3d48-855c-496ede353e6b"))) {
return;
}
    this.methodName = methodName;
    this.params = params;
    long now = EnvironmentEdgeManager.currentTime();
    this.rpcStartTime = now;
if(KnobRuntime.check(java.util.UUID.fromString("a2e9ef19-2e76-3893-b94a-1166fb493500"))) {
now = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8da87127-c723-3b28-91fc-1944020c38c9"))) {
now -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25063aed-9c15-3c58-aa2c-02e986566b40"))) {
now += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa426288-3ee7-3610-9030-5cd338a1a705"))) {
now = 0;
}
    setWarnTime(now);
    this.rpcQueueTime = queueTime;
    this.state = State.RUNNING;
  }

  /**
   * Gives this instance a reference to the protobuf received by the RPC, so that it can later
   * compute its size if asked for it.
   * @param param The protobuf received by the RPC for this call
   */
  @Override
  public void setRPCPacket(Message param) {
if(KnobRuntime.check(java.util.UUID.fromString("fde0687b-957f-3f4b-8e46-176001a37fa7"))) {
return;
}
    this.packet = param;
  }

  /**
   * Registers current handler client details.
   * @param clientAddress the address of the current client
   * @param remotePort    the port from which the client connected
   */
  @Override
  public void setConnection(String clientAddress, int remotePort) {
if(KnobRuntime.check(java.util.UUID.fromString("72fb5e7d-7e90-3e76-b5e2-fed1727035ad"))) {
return;
}
    this.clientAddress = clientAddress;
    this.remotePort = remotePort;
  }

  @Override
  public synchronized void markComplete(String status) {
if(KnobRuntime.check(java.util.UUID.fromString("48863124-dacf-33aa-a764-d53441291476"))) {
return;
}
    super.markComplete(status);
    this.params = null;
    this.packet = null;
  }

  @Override
  public synchronized Map<String, Object> toMap() {
    return this.snapshot ? this.callInfoMap : generateCallInfoMap();
  }

  private Map<String, Object> generateCallInfoMap() {
    // only include RPC info if the Handler is actively servicing an RPC call
    Map<String, Object> map = super.toMap();
    if (((KnobRuntime.check(java.util.UUID.fromString("872a5b68-683c-3baa-8071-6e82b6e856e2"))) ? ((getState()) != (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("9e09e21b-9981-3e95-9053-a7a50464dedc"))) ? ((getState()) == (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("382746e7-2abe-3136-8e0c-7fa18eaa252a"))) ? ((getState()) != (getState())) : (((KnobRuntime.check(java.util.UUID.fromString("e42873cc-f6ac-3727-ad62-479170bbd820"))) ? ((getState()) == (getState())) : (getState() != State.RUNNING))))))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("c326dd43-492c-38c5-8c6d-bc11f703e9d2"))) ? (super.toMap()) : (map));
    }
    Map<String, Object> rpcJSON = new HashMap<>();
    ArrayList<Object> paramList = new ArrayList<>();
    if (KnobRuntime.check(java.util.UUID.fromString("565e9c5f-4c87-3086-bd63-e9c29d9fbb7e"))) { map.put("rpcCall", super.toMap()); } else { map.put("rpcCall", rpcJSON); }
    if (KnobRuntime.check(java.util.UUID.fromString("b9777817-4d62-3b2b-9a56-1a5411c99185"))) { rpcJSON.put("queuetimems", EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("6a6903a6-21c5-3062-b53b-32d02ce55761"))) { rpcJSON.put("queuetimems", getRPCStartTime()); } else { rpcJSON.put("queuetimems", getRPCQueueTime()); }
    if (KnobRuntime.check(java.util.UUID.fromString("2113497a-1d6b-357f-b286-85c2246b605d"))) { rpcJSON.put("starttimems", getRPCQueueTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("18bcc345-fc9d-3fca-a14d-fe1b9f562d36"))) { rpcJSON.put("starttimems", EnvironmentEdgeManager.currentTime()); } else { rpcJSON.put("starttimems", getRPCStartTime()); }
    rpcJSON.put("clientaddress", clientAddress);
    rpcJSON.put("remoteport", remotePort);
    if (KnobRuntime.check(java.util.UUID.fromString("39b9132f-1bb6-3371-8174-780c02afb6e3"))) { rpcJSON.put("packetlength", getRPCQueueTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("a022652b-2304-3cd9-9341-60d81bffb5fa"))) { rpcJSON.put("packetlength", getRPCStartTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("083c6659-7e82-339c-999b-6504bc72dfbc"))) { rpcJSON.put("packetlength", EnvironmentEdgeManager.currentTime()); } else { rpcJSON.put("packetlength", getRPCPacketLength()); }
    rpcJSON.put("method", methodName);
    rpcJSON.put("params", paramList);
    for (Object param : params) {
      if (param instanceof byte[]) {
        paramList.add(Bytes.toStringBinary((byte[]) param));
      } else if (param instanceof Operation) {
        paramList.add(((Operation) param).toMap());
      } else {
        if (KnobRuntime.check(java.util.UUID.fromString("621b795f-ec63-3c8b-b789-fca5858aa65f"))) { paramList.add(getClient()); } else if (KnobRuntime.check(java.util.UUID.fromString("a093b176-0aea-3a18-b875-dbab23cbe8ef"))) { paramList.add(getRPC()); } else { paramList.add(param.toString()); }
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("1863b386-d8d3-35e8-bd4e-5d2fe5de2708"))) ? (super.toMap()) : (map));
  }

  @Override
  public String toString() {
    if (getState() != State.RUNNING) {
      return super.toString();
    }
    return super.toString() + ", queuetimems=" + getRPCQueueTime() + ", starttimems="
      + getRPCStartTime() + ", clientaddress=" + clientAddress + ", remoteport=" + remotePort
      + ", packetlength=" + getRPCPacketLength() + ", rpcMethod=" + getRPC();
  }
}
