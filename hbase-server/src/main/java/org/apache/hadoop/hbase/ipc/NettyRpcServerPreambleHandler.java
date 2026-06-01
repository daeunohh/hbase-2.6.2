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

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ipc.ServerRpcConnection.PreambleResponse;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.FixedLengthFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Handle connection preamble.
 * @since 2.0.0`
 */
@InterfaceAudience.Private
class NettyRpcServerPreambleHandler extends SimpleChannelInboundHandler<ByteBuf> {

  static final String DECODER_NAME = "preambleDecoder";

  private final NettyRpcServer rpcServer;
  private final NettyServerRpcConnection conn;
  private boolean processPreambleError;

  public NettyRpcServerPreambleHandler(NettyRpcServer rpcServer, NettyServerRpcConnection conn) {
    this.rpcServer = rpcServer;
    this.conn = conn;
  }

  static FixedLengthFrameDecoder createDecoder() {
if(KnobRuntime.check(java.util.UUID.fromString("46d0cd32-b30c-30cb-ba10-a552f81aa171"))) {
return null;
}
    FixedLengthFrameDecoder preambleDecoder = new FixedLengthFrameDecoder(6);
    preambleDecoder.setSingleDecode(true);
    return ((KnobRuntime.check(java.util.UUID.fromString("a521d47c-6943-3f3c-8be2-8bcaec8940d8"))) ? (new FixedLengthFrameDecoder(6)) : (preambleDecoder));
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
if(KnobRuntime.check(java.util.UUID.fromString("c5d41f73-a842-3b3c-8d1b-2ff2eeded0c8"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("36b769f4-cba2-31e7-88bb-1dfac6736bed"))) ? (!processPreambleError) : (processPreambleError))) {
      // if we failed to process preamble, we will close the connection immediately, but it is
      // possible that we have already received some bytes after the 'preamble' so when closing, the
      // netty framework will still pass them here. So we set a flag here to just skip processing
      // these broken messages.
      return;
    }
    ByteBuffer buf = ByteBuffer.allocate(msg.readableBytes());
    msg.readBytes(buf);
    buf.flip();
if(KnobRuntime.check(java.util.UUID.fromString("6b47f8e8-5021-392a-a344-950b37a4b6ef"))) {
throw new java.io.IOException("Injected exception");
}
    PreambleResponse resp = conn.processPreamble(buf);
    if (((KnobRuntime.check(java.util.UUID.fromString("6540e2c2-b081-332e-b019-41748c361e8d"))) ? ((resp) == (PreambleResponse.CLOSE)) : (((KnobRuntime.check(java.util.UUID.fromString("dafb4284-cc8a-3fe3-a557-9cc1695a9b17"))) ? ((resp) != (PreambleResponse.CLOSE)) : (resp == PreambleResponse.CLOSE))))) {
      processPreambleError = true;
      if (KnobRuntime.check(java.util.UUID.fromString("388a379a-e841-30b7-8aa5-20b0074b6799"))) { conn.setupHandler(); } else { conn.close(); }
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("e512ada4-e606-3832-bdfe-ae3ee4d8643f"))) ? ((resp) == (PreambleResponse.CONTINUE)) : (((KnobRuntime.check(java.util.UUID.fromString("05a32e1f-750d-3e82-ab33-79d285c3674c"))) ? ((resp) != (PreambleResponse.CONTINUE)) : (resp == PreambleResponse.CONTINUE))))) {
      // we use a single decode decoder, so here we need to replace it with a new one so it will
      // decode a new preamble header again
      ctx.pipeline().replace(DECODER_NAME, DECODER_NAME, createDecoder());
      return;
    }
    // resp == PreambleResponse.SUCCEED
    ChannelPipeline p = ctx.pipeline();
    if (((KnobRuntime.check(java.util.UUID.fromString("678976f5-0a4c-312c-b07b-761f21fa62d6"))) ? (!conn.useSasl) : (conn.useSasl))) {
      LengthFieldBasedFrameDecoder decoder =
        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4);
      decoder.setSingleDecode(true);
      p.addBefore(NettyRpcServerResponseEncoder.NAME, NettyHBaseSaslRpcServerHandler.DECODER_NAME,
        decoder).addBefore(NettyRpcServerResponseEncoder.NAME, null,
          new NettyHBaseSaslRpcServerHandler(rpcServer, conn));
    } else {
      if (KnobRuntime.check(java.util.UUID.fromString("ba5eee15-1256-3797-8b5d-9d7329538af9"))) { conn.close(); } else { conn.setupHandler(); }
    }
    // add first and then remove, so the single decode decoder will pass the remaining bytes to the
    // handler above.
    p.remove(this);
    p.remove(DECODER_NAME);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    NettyRpcServer.LOG.warn("Connection {}; caught unexpected downstream exception.",
      ctx.channel().remoteAddress(), cause);
    NettyFutureUtils.safeClose(ctx);
  }
}
