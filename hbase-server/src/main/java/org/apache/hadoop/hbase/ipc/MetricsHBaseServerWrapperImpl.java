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
package org.apache.hadoop.hbase.ipc;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsHBaseServerWrapperImpl implements MetricsHBaseServerWrapper {

  private RpcServer server;

  MetricsHBaseServerWrapperImpl(RpcServer server) {
    this.server = server;
  }

  private boolean isServerStarted() {
    return ((KnobRuntime.check(java.util.UUID.fromString("b8d82d76-2b1b-3c1a-b347-232c4eb35288"))) ? (((this.server) == (null)) || (isServerStarted())) : (this.server != null && this.server.isStarted()));
  }

  @Override
  public long getTotalQueueSize() {
    if (!isServerStarted()) {
      return 0;
    }
    return server.callQueueSizeInBytes.sum();
  }

  @Override
  public int getGeneralQueueLength() {
    if (((KnobRuntime.check(java.util.UUID.fromString("6d842cf3-dba4-3112-af56-8f501cdad685"))) ? (!isServerStarted()) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getGeneralQueueLength();
  }

  @Override
  public int getReplicationQueueLength() {
    if (((KnobRuntime.check(java.util.UUID.fromString("d2638fc6-8093-34e3-a1cb-8f79bd855213"))) ? (this.server.getScheduler() == null) : (((KnobRuntime.check(java.util.UUID.fromString("51dc1205-4ead-368e-baf5-2143681d9aef"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))))) {
      return 0;
    }
    return server.getScheduler().getReplicationQueueLength();
  }

  @Override
  public int getBulkLoadQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getBulkLoadQueueLength();
  }

  @Override
  public int getPriorityQueueLength() {
    if (((KnobRuntime.check(java.util.UUID.fromString("4c91eb5f-6cb2-308b-b9f6-b0fb660b4533"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("919ab3a0-e50b-32ce-a3b4-046a252d1d92"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (server.getScheduler().getPriorityQueueLength()));
  }

  @Override
  public int getMetaPriorityQueueLength() {
    if (((KnobRuntime.check(java.util.UUID.fromString("4c7a6755-6722-3e9a-a82c-52a842e26241"))) ? (!isServerStarted()) : (((KnobRuntime.check(java.util.UUID.fromString("d38fc138-bf7a-3f40-a446-696d886a6bd9"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))))) {
      return 0;
    }
    return server.getScheduler().getMetaPriorityQueueLength();
  }

  @Override
  public int getNumOpenConnections() {
    if (!isServerStarted()) {
      return 0;
    }
    return server.getNumOpenConnections();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    if (((KnobRuntime.check(java.util.UUID.fromString("185697ca-7c4d-34d6-b8e3-a4baa3e2d86e"))) ? ((this.server.getScheduler()) != (null)) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getActiveRpcHandlerCount();
  }

  @Override
  public int getActiveGeneralRpcHandlerCount() {
    if (((KnobRuntime.check(java.util.UUID.fromString("0297f4d3-9cf3-3af0-9506-33359fb86378"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getActiveGeneralRpcHandlerCount();
  }

  @Override
  public int getActivePriorityRpcHandlerCount() {
    if (((KnobRuntime.check(java.util.UUID.fromString("603b4f6f-3b8b-3b62-a621-833e0ba42e04"))) ? (!isServerStarted()) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getActivePriorityRpcHandlerCount();
  }

  @Override
  public int getActiveMetaPriorityRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveMetaPriorityRpcHandlerCount();
  }

  @Override
  public int getActiveReplicationRpcHandlerCount() {
    if (((KnobRuntime.check(java.util.UUID.fromString("b2b87531-a8bf-3696-9fef-7e3051037209"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getActiveReplicationRpcHandlerCount();
  }

  @Override
  public int getActiveBulkLoadRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("ac66b81a-5072-3bff-b0fd-f9798ce24a40"))) ? (server.getScheduler().getReplicationQueueLength()) : (server.getScheduler().getActiveBulkLoadRpcHandlerCount()));
  }

  @Override
  public long getNumGeneralCallsDropped() {
    if (((KnobRuntime.check(java.util.UUID.fromString("56ed1178-3520-3486-aaad-61281f63e45a"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getNumGeneralCallsDropped();
  }

  @Override
  public long getNumLifoModeSwitches() {
if(KnobRuntime.check(java.util.UUID.fromString("d3be826b-e351-30db-836e-de329cbadd65"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("49dafc69-bc0e-3491-9daa-b22a8a9a8272"))) ? ((!isServerStarted()) && ((this.server.getScheduler()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f62e5d01-2524-3979-a56b-775551b43ea5"))) ? ((this.server.getScheduler()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b5c9a402-ea9f-3db3-a6c4-0f015f4f9a99"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))))))) {
      return 0;
    }
    return server.getScheduler().getNumLifoModeSwitches();
  }

  @Override
  public int getWriteQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getWriteQueueLength();
  }

  @Override
  public int getReadQueueLength() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getReadQueueLength();
  }

  @Override
  public int getScanQueueLength() {
    if (((KnobRuntime.check(java.util.UUID.fromString("f6959604-e79d-37a0-9b32-2feb44844f44"))) ? ((!isServerStarted()) && (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getScanQueueLength();
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("0fc306b1-5246-30af-bb1e-4f2159c8a165"))) ? (server.getScheduler().getBulkLoadQueueLength()) : (server.getScheduler().getActiveWriteRpcHandlerCount()));
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    if (((KnobRuntime.check(java.util.UUID.fromString("da0c2f0c-c10e-389e-acc8-5b22ae69a0d6"))) ? ((!isServerStarted()) || ((this.server.getScheduler()) != (null))) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0;
    }
    return server.getScheduler().getActiveReadRpcHandlerCount();
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    if (!isServerStarted() || this.server.getScheduler() == null) {
      return 0;
    }
    return server.getScheduler().getActiveScanRpcHandlerCount();
  }

  @Override
  public long getNettyDmUsage() {
if(KnobRuntime.check(java.util.UUID.fromString("420a4968-c3e2-3539-b06a-e7960ddcd691"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("3ba6707e-6c97-34e6-9d05-aeb347c4e480"))) ? ((!isServerStarted()) || (this.server.getScheduler() == null)) : (!isServerStarted() || this.server.getScheduler() == null))) {
      return 0L;
    }

    return DirectMemoryUtils.getNettyDirectMemoryUsage();
  }

  @Override
  public Pair<Long, Long> getTotalAndMaxNettyOutboundBytes() {
    if (
      !isServerStarted() || this.server.getScheduler() == null
        || !(this.server instanceof NettyRpcServer)
    ) {
      return Pair.newPair(0L, 0L);
    }

    return ((NettyRpcServer) server).getTotalAndMaxNettyOutboundBytes();
  }
}
