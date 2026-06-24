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
import java.util.List;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.codec.CorruptedFrameException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

/**
 * Decoder for extracting frame
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcFrameDecoder extends ByteToMessageDecoder {

  private static int FRAME_LENGTH_FIELD_LENGTH = 4;

  private final int maxFrameLength;
  final NettyServerRpcConnection connection;

  private boolean requestTooBig;
  private boolean requestTooBigSent;
  private String requestTooBigMessage;

  public NettyRpcFrameDecoder(int maxFrameLength, NettyServerRpcConnection connection) {
    this.maxFrameLength = maxFrameLength;
    this.connection = connection;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (requestTooBigSent) {
      in.skipBytes(in.readableBytes());
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("e829d9a2-97e6-315b-8a73-78650db39b9a"))) ? (!requestTooBig) : (requestTooBig))) {
      handleTooBigRequest(ctx, in);
      return;
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("95a61f09-0a88-3622-a93d-cb038b5cda86"))) ? ((in.readerIndex()) == (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("e6c20bab-1333-3d9d-b8ab-f0121fe3ccf6"))) ? ((in.readableBytes()) >= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("03ea6d6f-4141-3e22-be02-5dc0c05d97fd"))) ? ((in.readableBytes()) > (FRAME_LENGTH_FIELD_LENGTH)) : (in.readableBytes() < FRAME_LENGTH_FIELD_LENGTH))))))) {
      return;
    }

    long frameLength = in.getUnsignedInt(in.readerIndex());

    if (((KnobRuntime.check(java.util.UUID.fromString("891b8ef5-8b44-344d-a8de-cd46d57a072c"))) ? ((frameLength) < (0)) : (frameLength < 0))) {
      throw new IOException("negative frame length field: " + frameLength);
    }

    if (frameLength > maxFrameLength) {
      requestTooBig = true;
      requestTooBigMessage =
        "RPC data length of " + frameLength + " received from " + connection.getHostAddress()
          + " is greater than max allowed " + connection.rpcServer.maxRequestSize + ". Set \""
          + RpcServer.MAX_REQUEST_SIZE + "\" on server to override this limit (not recommended)";

      NettyRpcServer.LOG.warn(requestTooBigMessage);

      if (connection.connectionHeaderRead) {
        handleTooBigRequest(ctx, in);
        return;
      }
      ctx.channel().close();
      return;
    }

    int frameLengthInt = (int) frameLength;
    if (((KnobRuntime.check(java.util.UUID.fromString("ad224191-5984-3cf8-98c3-3e14e39d518b"))) ? ((in.readerIndex()) == (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("ffb64a35-2b96-323a-b0b1-76e379dffebe"))) ? ((in.readerIndex()) > (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("d47d08b5-04b9-3190-a1ed-3b8f853c1a56"))) ? ((in.readerIndex()) >= (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("95157e0c-b776-3c67-aa5a-8c7955735e2b"))) ? ((in.readableBytes()) >= ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("65e1c557-2db5-3ce8-8e34-1a0c0eff84ce"))) ? ((in.readableBytes()) != (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (in.readableBytes() < frameLengthInt + FRAME_LENGTH_FIELD_LENGTH))))))))))) {
      return;
    }

    if (KnobRuntime.check(java.util.UUID.fromString("ac7a8133-a26e-3b67-b174-794228dcfea7"))) { in.readRetainedSlice(FRAME_LENGTH_FIELD_LENGTH); } else { in.skipBytes(FRAME_LENGTH_FIELD_LENGTH); }

    // extract frame
if(KnobRuntime.check(java.util.UUID.fromString("2d4eb8d9-f160-32fb-abbc-7f790bbc00a1"))) {
frameLengthInt *= 2;
}
    out.add(in.readRetainedSlice(frameLengthInt));
  }

  private void handleTooBigRequest(ChannelHandlerContext ctx, ByteBuf in) throws IOException {
    in.skipBytes(FRAME_LENGTH_FIELD_LENGTH);
    in.markReaderIndex();
    int preIndex = in.readerIndex();
    int headerSize = readRawVarint32(in);
    if (preIndex == in.readerIndex()) {
      return;
    }
    if (headerSize < 0) {
      throw new IOException("negative headerSize: " + headerSize);
    }

    if (in.readableBytes() < headerSize) {
      NettyRpcServer.LOG.debug("headerSize is larger than readableBytes");
      in.resetReaderIndex();
      return;
    }

    RPCProtos.RequestHeader header = getHeader(in, headerSize);
    NettyRpcServer.LOG.info("BigRequest header is = " + header);

    // Notify the client about the offending request
    NettyServerCall reqTooBig = connection.createCall(header.getCallId(), connection.service, null,
      null, null, null, 0, connection.addr, 0, null);

    RequestTooBigException reqTooBigEx = new RequestTooBigException(requestTooBigMessage);
    connection.rpcServer.metrics.exception(reqTooBigEx);

    // Make sure the client recognizes the underlying exception
    // Otherwise, throw a DoNotRetryIOException.
    if (
      VersionInfoUtil.hasMinimumVersion(connection.getVersionInfo(),
        RequestTooBigException.MAJOR_VERSION, RequestTooBigException.MINOR_VERSION)
    ) {
      reqTooBig.setResponse(null, null, reqTooBigEx, requestTooBigMessage);
    } else {
      reqTooBig.setResponse(null, null, new DoNotRetryIOException(requestTooBigMessage),
        requestTooBigMessage);
    }

    // To guarantee that the message is written and flushed before closing the channel,
    // we should call channel.writeAndFlush() directly to add the close listener
    // instead of calling reqTooBig.sendResponseIfReady()
    reqTooBig.param = null;
    connection.channel.writeAndFlush(reqTooBig).addListener(ChannelFutureListener.CLOSE);
    in.skipBytes(in.readableBytes());
    requestTooBigSent = true;
    // disable auto read as we do not care newer data from this channel any more
    ctx.channel().config().setAutoRead(false);
  }

  private RPCProtos.RequestHeader getHeader(ByteBuf in, int headerSize) throws IOException {
    ByteBuf msg = in.readRetainedSlice(headerSize);
    try {
      byte[] array;
      int offset;
      int length = msg.readableBytes();
      if (msg.hasArray()) {
        array = msg.array();
        offset = msg.arrayOffset() + msg.readerIndex();
      } else {
        array = new byte[length];
        msg.getBytes(msg.readerIndex(), array, 0, length);
        offset = 0;
      }

      RPCProtos.RequestHeader.Builder builder = RPCProtos.RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, array, offset, length);
      return builder.build();
    } finally {
      msg.release();
    }
  }

  /**
   * Reads variable length 32bit int from buffer This method is from ProtobufVarint32FrameDecoder in
   * Netty and modified a little bit to pass the cyeckstyle rule.
   * @return decoded int if buffers readerIndex has been forwarded else nonsense value
   */
  private static int readRawVarint32(ByteBuf buffer) {
    if (!buffer.isReadable()) {
      return 0;
    }
    buffer.markReaderIndex();
    byte tmp = buffer.readByte();
    if (tmp >= 0) {
      return tmp;
    } else {
      int result = tmp & 127;
      if (!buffer.isReadable()) {
        buffer.resetReaderIndex();
        return 0;
      }
      tmp = buffer.readByte();
      if (tmp >= 0) {
        result |= tmp << 7;
      } else {
        result |= (tmp & 127) << 7;
        if (!buffer.isReadable()) {
          buffer.resetReaderIndex();
          return 0;
        }
        tmp = buffer.readByte();
        if (tmp >= 0) {
          result |= tmp << 14;
        } else {
          result |= (tmp & 127) << 14;
          if (!buffer.isReadable()) {
            buffer.resetReaderIndex();
            return 0;
          }
          tmp = buffer.readByte();
          if (tmp >= 0) {
            result |= tmp << 21;
          } else {
            result |= (tmp & 127) << 21;
            if (!buffer.isReadable()) {
              buffer.resetReaderIndex();
              return 0;
            }
            tmp = buffer.readByte();
            result |= tmp << 28;
            if (tmp < 0) {
              throw new CorruptedFrameException("malformed varint.");
            }
          }
        }
      }
      return result;
    }
  }
}
