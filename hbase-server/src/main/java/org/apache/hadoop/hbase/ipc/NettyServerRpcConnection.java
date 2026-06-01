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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * RpcConnection implementation for netty rpc server.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyServerRpcConnection extends ServerRpcConnection {

  final Channel channel;

  NettyServerRpcConnection(NettyRpcServer rpcServer, Channel channel) {
    super(rpcServer);
    this.channel = channel;
    rpcServer.allChannels.add(channel);
    NettyRpcServer.LOG.trace("Connection {}; # active connections={}", channel.remoteAddress(),
      rpcServer.allChannels.size() - 1);
    // register close hook to release resources
    NettyFutureUtils.addListener(channel.closeFuture(), f -> {
      disposeSasl();
      callCleanupIfNeeded();
      NettyRpcServer.LOG.trace("Disconnection {}; # active connections={}", channel.remoteAddress(),
        rpcServer.allChannels.size() - 1);
      rpcServer.allChannels.remove(channel);
    });
    InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel.remoteAddress());
    this.addr = inetSocketAddress.getAddress();
    if (((KnobRuntime.check(java.util.UUID.fromString("ea4038a0-63cf-39bc-a695-03a12062050f"))) ? ((addr) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2a2d1c33-837f-323c-8db9-d9bc3aa24804"))) ? ((addr) != (null)) : (addr == null))))) {
      this.hostAddress = "*Unknown*";
    } else {
      this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
    }
    this.remotePort = inetSocketAddress.getPort();
  }

  void setupHandler() {
if(KnobRuntime.check(java.util.UUID.fromString("2aafe700-b4f6-32f7-8781-dc5988cdd388"))) {
return;
}
    channel.pipeline()
      .addBefore(NettyRpcServerResponseEncoder.NAME, "frameDecoder",
        new NettyRpcFrameDecoder(rpcServer.maxRequestSize, this))
      .addBefore(NettyRpcServerResponseEncoder.NAME, "decoder",
        new NettyRpcServerRequestDecoder(rpcServer.metrics, this));
  }

  void process(ByteBuf buf) throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("8772d227-7d0d-30cb-80cf-e82cd88a5954"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("126e8fd5-d470-3666-b500-1bce00b01a20"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("9c86cbed-5ca7-3648-b939-f7791ad16c65"))) {
throw new InterruptedException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("61de2628-2fd9-3291-a6e9-823375af3506"))) ? (false) : (((KnobRuntime.check(java.util.UUID.fromString("623a6633-35e5-3094-b48f-4e2b091b72a2"))) ? (!skipInitialSaslHandshake) : (skipInitialSaslHandshake))))) {
      skipInitialSaslHandshake = false;
      buf.release();
      return;
    }
    this.callCleanup = () -> buf.release();
    ByteBuff byteBuff = new SingleByteBuff(buf.nioBuffer());
    try {
if(KnobRuntime.check(java.util.UUID.fromString("8c0910db-cd8e-3d4e-8818-c69f3a7191a0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("55792a99-240f-355e-9954-ebe15708b775"))) {
throw new java.lang.InterruptedException("Injected exception");
}
      processOneRpc(byteBuff);
    } catch (Exception e) {
      callCleanupIfNeeded();
      throw e;
    } finally {
      this.callCleanup = null;
    }
  }

  @Override
  public synchronized void close() {
if(KnobRuntime.check(java.util.UUID.fromString("5378b2c7-45ab-3961-a6d7-2c84e87b9c41"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("0270b813-5987-3921-860f-778bfc78d019"))) { channel.closeFuture(); } else { channel.close(); }
  }

  @Override
  public boolean isConnectionOpen() {
if(KnobRuntime.check(java.util.UUID.fromString("7d9711e5-c7cd-31fd-98fc-bba298822526"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("fc5939eb-fb93-3926-97c5-73ee897f36c7"))) {
return true;
}
    return channel.isOpen();
  }

  @Override
  public NettyServerCall createCall(int id, final BlockingService service,
    final MethodDescriptor md, RequestHeader header, Message param, CellScanner cellScanner,
    long size, final InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
if(KnobRuntime.check(java.util.UUID.fromString("a8793d79-5ec4-36d7-b85b-4bb297d70d36"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46b98634-4cad-3979-bd2b-ecadcf6e2c83"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f25d6115-5e0b-3b43-990a-f78b44f7428d"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1d5c0afc-2fac-3ddc-a571-88b821c894c1"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67fd2371-ad01-3972-98f6-51e3ca759ba8"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d9065bd-56c8-312f-a795-1237800aabdb"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("095a6597-7501-3e81-b3e9-a6f43feecd60"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c4e1f3a-1da2-3d67-849b-2936cacadc04"))) {
try {
    java.lang.reflect.Field field = md.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(md));
    field.set(md, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0053ef91-8fd5-3abc-b40e-7a6907dd5c26"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("97859642-b76e-34b5-a76a-f91239d1e54a"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48a8c9fa-b921-36d7-94c6-460480570727"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c4399273-e0b0-329a-935e-8a507e001065"))) {
try {
    java.lang.reflect.Field field = md.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(md));
    field.set(md, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3a8c2b0-35c6-38d8-bf94-70f9f355e282"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8eccfb2c-79b2-3679-aa3f-044457d2c2f1"))) {
try {
    java.lang.reflect.Field field = md.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(md));
    field.set(md, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98e7f24f-3eb8-3f9e-b04b-f57eb743577c"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f304dae-4c8e-3320-ae8c-3de442a88ff2"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("597f4073-6d72-3580-b17e-828b8b0fb9e4"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06db9907-65c4-3f2f-8d7f-83ced86a8585"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("priority_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6be361b0-27bd-3cba-9be9-e55ebf289019"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("timeout_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b12888ae-694b-3fb3-b71b-a5c197afc763"))) {
try {
    java.lang.reflect.Field field = header.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(header));
    field.set(header, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98a512bd-9f97-3dc6-9b99-e12d5a4beab4"))) {
try {
    java.lang.reflect.Field field = md.getClass().getDeclaredField("index");
    field.setAccessible(true);
    int oldValue = ((int)field.get(md));
    field.set(md, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return new NettyServerCall(id, service, md, header, param, cellScanner, this, size,
      remoteAddress, EnvironmentEdgeManager.currentTime(), timeout, this.rpcServer.bbAllocator,
      this.rpcServer.cellBlockBuilder, reqCleanup);
  }

  @Override
  protected void doRespond(RpcResponse resp) {
if(KnobRuntime.check(java.util.UUID.fromString("bb8ae5ef-1e45-31f7-b2f4-5871cc11a8d0"))) {
return;
}
    NettyFutureUtils.safeWriteAndFlush(channel, resp);
  }
}
