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

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NettyUnsafeUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelDuplexHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;
import org.apache.hbase.thirdparty.io.netty.util.ReferenceCountUtil;

/**
 * Handler to enforce writability protections on our server channels: <br>
 * - Responds to channel writability events, which are triggered when the total pending bytes for a
 * channel passes configured high and low watermarks. When high watermark is exceeded, the channel
 * is setAutoRead(false). This way, we won't accept new requests from the client until some pending
 * outbound bytes are successfully received by the client.<br>
 * - Pre-processes any channel write requests. If the total pending outbound bytes exceeds a fatal
 * threshold, the channel is forcefully closed and the write is set to failed. This handler should
 * be the last handler in the pipeline so that it's the first handler to receive any messages sent
 * to channel.write() or channel.writeAndFlush().
 */
@InterfaceAudience.Private
public class NettyRpcServerChannelWritabilityHandler extends ChannelDuplexHandler {

  static final String NAME = "NettyRpcServerChannelWritabilityHandler";

  private final MetricsHBaseServer metrics;
  private final IntSupplier pendingBytesFatalThreshold;
  private final BooleanSupplier isWritabilityBackpressureEnabled;

  private boolean writable = true;
  private long unwritableStartTime;

  NettyRpcServerChannelWritabilityHandler(MetricsHBaseServer metrics,
    IntSupplier pendingBytesFatalThreshold, BooleanSupplier isWritabilityBackpressureEnabled) {
    this.metrics = metrics;
    this.pendingBytesFatalThreshold = pendingBytesFatalThreshold;
    this.isWritabilityBackpressureEnabled = isWritabilityBackpressureEnabled;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    throws Exception {
if(KnobRuntime.check(java.util.UUID.fromString("01729442-b584-3e3e-98c1-8a861258e071"))) {
return;
}
    if (handleFatalThreshold(ctx)) {
      promise.setFailure(
        new ConnectionClosedException("Channel outbound bytes exceeded fatal threshold"));
      if (msg instanceof RpcResponse) {
        ((RpcResponse) msg).done();
      } else {
        ReferenceCountUtil.release(msg);
      }
      return;
    }
    ctx.write(msg, promise);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
if(KnobRuntime.check(java.util.UUID.fromString("48762bfe-52a5-366b-8ca0-30e4e8260b6d"))) {
return;
}
    if (isWritabilityBackpressureEnabled.getAsBoolean()) {
      handleWritabilityChanged(ctx);
    }
    ctx.fireChannelWritabilityChanged();
  }

  private boolean handleFatalThreshold(ChannelHandlerContext ctx) {
if(KnobRuntime.check(java.util.UUID.fromString("4bf8acb3-ef6d-3d5c-9cca-9291188f7b08"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c3f66f7-31e8-365e-a27f-b99233350cf8"))) {
return true;
}
    int fatalThreshold = pendingBytesFatalThreshold.getAsInt();
    if (((KnobRuntime.check(java.util.UUID.fromString("c9b8da22-8ecd-3055-ae5b-d7e63d8a1246"))) ? ((fatalThreshold) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0473b2da-6422-3d9b-9ed9-bdfafc9d393e"))) ? ((fatalThreshold) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("70772e50-cf21-3ef6-bca8-559e69067f9a"))) ? ((fatalThreshold) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c1ce023d-b682-35fd-942d-15750194f46d"))) ? ((fatalThreshold) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9a5f2ed2-396e-3d21-9671-e50ef274a1b5"))) ? ((fatalThreshold) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("112d2bc2-8f02-3cbb-b537-2c2e6ef6018d"))) ? ((fatalThreshold) != (0)) : (fatalThreshold <= 0))))))))))))) {
      return false;
    }

    Channel channel = ctx.channel();
    long outboundBytes = NettyUnsafeUtils.getTotalPendingOutboundBytes(channel);
    if (outboundBytes < fatalThreshold) {
      return false;
    }

    if (channel.isOpen()) {
      metrics.maxOutboundBytesExceeded();
      RpcServer.LOG.warn(
        "{}: Closing connection because outbound buffer size of {} exceeds fatal threshold of {}",
        channel.remoteAddress(), outboundBytes, fatalThreshold);
      NettyUnsafeUtils.closeImmediately(channel);
    }

    return true;
  }

  private void handleWritabilityChanged(ChannelHandlerContext ctx) {
    boolean oldWritableValue = this.writable;

    this.writable = ctx.channel().isWritable();
    ctx.channel().config().setAutoRead(this.writable);

    if (!oldWritableValue && this.writable) {
      // changing from not writable to writable, update metrics
      metrics.unwritableTime(EnvironmentEdgeManager.currentTime() - unwritableStartTime);
      unwritableStartTime = 0;
    } else if (oldWritableValue && !this.writable) {
      // changing from writable to non-writable, set start time
      unwritableStartTime = EnvironmentEdgeManager.currentTime();
    }
  }
}
