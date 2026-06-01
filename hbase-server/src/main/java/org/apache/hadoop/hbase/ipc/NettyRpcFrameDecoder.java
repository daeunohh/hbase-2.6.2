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
if(KnobRuntime.check(java.util.UUID.fromString("7c61c5b5-14e2-3f92-8a2a-935dc8313566"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9c78816a-eec4-3549-9d5d-ecd2fb5a42b3"))) ? (!requestTooBigSent) : (requestTooBigSent))) {
      in.skipBytes(in.readableBytes());
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("e829d9a2-97e6-315b-8a73-78650db39b9a"))) ? (!requestTooBig) : (requestTooBig))) {
      handleTooBigRequest(ctx, in);
      return;
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("e6c20bab-1333-3d9d-b8ab-f0121fe3ccf6"))) ? ((in.readableBytes()) >= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("0059fcf9-d22d-318c-9d56-7aa11fbcd7a8"))) ? ((in.readerIndex()) >= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("03ea6d6f-4141-3e22-be02-5dc0c05d97fd"))) ? ((in.readableBytes()) > (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("e0a97f94-eb37-3894-862f-bf3bc10024d5"))) ? ((in.readableBytes()) <= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("3030af65-cb62-308c-9af6-51e5ae815ec4"))) ? ((in.readerIndex()) != (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("790674a2-fc05-37b7-b8d8-6644af24dd7d"))) ? ((in.readableBytes()) != (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("28a211c0-7d8a-34d7-836d-c05bfc19b898"))) ? ((in.readerIndex()) <= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("b957a0c7-b9bf-3540-b107-a5c1400468b1"))) ? ((in.readableBytes()) == (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("a300cb80-df0a-3155-b57b-8965c0114d72"))) ? ((in.readableBytes()) < (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("95a61f09-0a88-3622-a93d-cb038b5cda86"))) ? ((in.readerIndex()) == (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("7d4a8ecd-5559-3491-b2d8-3e07c08ef5e0"))) ? ((in.readerIndex()) > (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("e78eeea4-36a5-3c08-9f2e-e3408fa38cd8"))) ? ((in.readerIndex()) < (FRAME_LENGTH_FIELD_LENGTH)) : (in.readableBytes() < FRAME_LENGTH_FIELD_LENGTH))))))))))))))))))))))))) {
      return;
    }

    long frameLength = in.getUnsignedInt(in.readerIndex());

    if (((KnobRuntime.check(java.util.UUID.fromString("52c33656-0482-3d10-a2c6-6cf57c44fdf6"))) ? ((frameLength) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("92026562-241b-385f-9477-a33938de3e6d"))) ? ((frameLength) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("891b8ef5-8b44-344d-a8de-cd46d57a072c"))) ? ((frameLength) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("720e91b8-5764-3c83-ad2c-334879ad5f72"))) ? ((frameLength) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("164ccfbc-a992-32c4-b1a5-8a72024e2e66"))) ? ((frameLength) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c501e36f-dd1e-3793-8fc8-aaab2684c8e7"))) ? ((frameLength) <= (0)) : (frameLength < 0))))))))))))) {
      throw new IOException("negative frame length field: " + frameLength);
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("ae8006f9-cf6d-304d-ab28-60c5e1a1b735"))) ? ((frameLength) > (maxFrameLength)) : (((KnobRuntime.check(java.util.UUID.fromString("f3c7c805-a813-3c18-b49c-6c28cdcec501"))) ? ((frameLength) < (maxFrameLength)) : (((KnobRuntime.check(java.util.UUID.fromString("72be31db-703a-3087-83c2-68d58c51292e"))) ? ((frameLength) >= (maxFrameLength)) : (((KnobRuntime.check(java.util.UUID.fromString("3e314ab6-69ef-34ed-86b3-c70ff5dd5d43"))) ? ((frameLength) != (maxFrameLength)) : (((KnobRuntime.check(java.util.UUID.fromString("1f82981d-8059-3b7c-b081-7f73d8229791"))) ? ((frameLength) <= (maxFrameLength)) : (((KnobRuntime.check(java.util.UUID.fromString("3ec02387-092a-328a-8051-b112d373f6c1"))) ? ((frameLength) == (maxFrameLength)) : (frameLength > maxFrameLength))))))))))))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("da5303da-bf54-396c-93a9-2bbf08aa7028"))) ? ((in.readableBytes()) <= ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("5c733668-5a42-3717-903c-97578728aaa3"))) ? ((in.readerIndex()) < (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("bd1aeac0-1b99-3be8-99a8-0bcf1252941d"))) ? ((in.readableBytes()) > (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("cf2e8a58-78c0-35e8-b1da-174146c9663f"))) ? ((in.readerIndex()) == ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("05da733a-650b-3fd1-bf6a-ced78143ba1e"))) ? ((in.readableBytes()) < (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("0a2ad38c-3737-3bc0-bd6b-699253d65392"))) ? ((in.readableBytes()) >= (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("a94245d7-9652-309c-bbe4-fe8c69b0990c"))) ? ((in.readableBytes()) != (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("ecfe043c-f080-3432-8ffa-42734111bac3"))) ? ((in.readableBytes()) == (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("ec77bb89-44f5-37f4-8581-22daf6e8096d"))) ? ((in.readerIndex()) > (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("6b95502f-d5ee-3ae8-bf70-3a4e5dc4ec5c"))) ? ((in.readerIndex()) == (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("00990f89-8aec-3819-a083-77319b99869d"))) ? ((in.readerIndex()) != (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("c885a393-a206-3831-a582-46e2175ccd29"))) ? ((in.readableBytes()) > (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("d47d08b5-04b9-3190-a1ed-3b8f853c1a56"))) ? ((in.readerIndex()) >= (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("65e1d285-22f0-3f37-bbde-c514d42a4787"))) ? ((in.readableBytes()) > ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("84ca4965-9071-33a4-b91d-a663047b6f19"))) ? ((in.readerIndex()) != ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("95157e0c-b776-3c67-aa5a-8c7955735e2b"))) ? ((in.readableBytes()) >= ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("d9bf2e22-03b4-394e-a40c-a11429cd2438"))) ? ((in.readerIndex()) <= ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("53540506-a7de-3299-93ec-873195d4b40c"))) ? ((in.readerIndex()) > ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("ffb64a35-2b96-323a-b0b1-76e379dffebe"))) ? ((in.readerIndex()) > (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("c3255c56-836c-3593-b15b-d0ec7309d202"))) ? ((in.readableBytes()) <= (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("3d962235-c07b-3301-8abf-3846589b0acb"))) ? ((in.readableBytes()) == ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("97d2aff4-d287-3f60-a354-131c5a20b409"))) ? ((in.readerIndex()) <= (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("fb8cf1b7-52e9-34cb-ad04-5f31b301f1d8"))) ? ((in.readableBytes()) > (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("290b5618-36dd-3ae9-a040-fd214e5aa512"))) ? ((in.readableBytes()) <= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("958d7a9a-d2b5-3f12-99f3-b6806172f4bb"))) ? ((in.readerIndex()) == (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("c1348b3e-b2cd-316f-803e-b217585c850f"))) ? ((in.readerIndex()) < ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("1ee8233f-e543-398d-9468-77c4638f8614"))) ? ((in.readableBytes()) >= (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("efd50d03-f7aa-3402-9e58-e76f574b9304"))) ? ((in.readerIndex()) != (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("ad224191-5984-3cf8-98c3-3e14e39d518b"))) ? ((in.readerIndex()) == (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("123e1118-3a7e-3cb3-8284-25db949e9b54"))) ? ((in.readableBytes()) == (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("c78f97d6-bef5-3eea-abce-91afb7a7f3f2"))) ? ((in.readableBytes()) < (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("65e1c557-2db5-3ce8-8e34-1a0c0eff84ce"))) ? ((in.readableBytes()) != (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("ef0137b9-6662-3065-a986-13c49ebe1b77"))) ? ((in.readerIndex()) >= (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("a6d6ac7b-9c7a-3fdc-9de4-e546c82d7079"))) ? ((in.readableBytes()) < (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("aea985e0-b223-3fce-b90d-ae93b7065c07"))) ? ((in.readerIndex()) >= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("6bec7b71-fc60-38db-b011-35b3d76c002c"))) ? ((in.readableBytes()) <= (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("23ecc889-99c2-30c6-a58d-b4ce69e75a93"))) ? ((in.readerIndex()) >= ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("0c259dff-8b19-392b-bdc1-3048773d0a55"))) ? ((in.readerIndex()) < (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("8b1991d3-0c9d-3c11-b23a-6a9a985c887c"))) ? ((in.readableBytes()) != (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("d6bcd04c-953b-3490-a51f-01c969cc5bee"))) ? ((in.readerIndex()) <= (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("33a24e6d-3413-3180-a39a-9bf924a4cb23"))) ? ((in.readerIndex()) != (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("9b963b4a-bfcd-34e6-bc4d-fb2b86b6d064"))) ? ((in.readableBytes()) != ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("3e3f361a-2e3b-3750-95ac-62dbdefe71c0"))) ? ((in.readerIndex()) < (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("3992e510-432f-3459-97fc-1325997cd6fd"))) ? ((in.readableBytes()) == (frameLengthInt + FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("8ac88415-b533-3b3d-b49e-0e8b7a9928ed"))) ? ((in.readerIndex()) > (frameLengthInt)) : (((KnobRuntime.check(java.util.UUID.fromString("a8244288-7e41-36ae-b4b0-9a89f50b85ad"))) ? ((in.readableBytes()) < ((frameLengthInt) + (FRAME_LENGTH_FIELD_LENGTH))) : (((KnobRuntime.check(java.util.UUID.fromString("3ac0efbf-ee2e-3639-b6e8-62da2d04b053"))) ? ((in.readerIndex()) <= (FRAME_LENGTH_FIELD_LENGTH)) : (((KnobRuntime.check(java.util.UUID.fromString("d7771d52-a272-379e-b406-c6e80fe93403"))) ? ((in.readableBytes()) >= (FRAME_LENGTH_FIELD_LENGTH)) : (in.readableBytes() < frameLengthInt + FRAME_LENGTH_FIELD_LENGTH))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return;
    }

    if (KnobRuntime.check(java.util.UUID.fromString("ac7a8133-a26e-3b67-b174-794228dcfea7"))) { in.readRetainedSlice(FRAME_LENGTH_FIELD_LENGTH); } else { in.skipBytes(FRAME_LENGTH_FIELD_LENGTH); }

    // extract frame
if(KnobRuntime.check(java.util.UUID.fromString("18788468-afc9-392e-bc61-5b5dd43406c2"))) {
frameLengthInt = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7774cea2-441d-3589-8609-89d9cb902bb9"))) {
frameLengthInt /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9fe79ce2-f3f3-3b15-8cc8-7a4555f7b998"))) {
frameLengthInt += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("21eea189-9856-3e94-893b-22637266c440"))) {
frameLengthInt -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a772974c-8696-3c85-bd23-4b2744af79c6"))) {
frameLengthInt = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d4eb8d9-f160-32fb-abbc-7f790bbc00a1"))) {
frameLengthInt *= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("94e2693c-464a-3d94-8860-99d27d08255f"))) { out.add(in.skipBytes(frameLengthInt)); } else { out.add(in.readRetainedSlice(frameLengthInt)); }
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
