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

import com.google.errorprone.annotations.RestrictedApi;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBufferListOutputStream;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;

/**
 * Datastructure that holds all necessary to a method invocation and then afterward, carries the
 * result.
 */
@InterfaceAudience.Private
public abstract class ServerCall<T extends ServerRpcConnection> implements RpcCall, RpcResponse {

  protected final int id; // the client's call id
  protected final BlockingService service;
  protected final MethodDescriptor md;
  protected final RequestHeader header;
  protected Message param; // the parameter passed
  // Optional cell data passed outside of protobufs.
  protected final CellScanner cellScanner;
  protected final T connection; // connection to client
  protected final long receiveTime; // the time received when response is null
  // the time served when response is not null
  protected final int timeout;
  protected long startTime;
  protected final long deadline;// the deadline to handle this call, if exceed we can drop it.

  protected final ByteBuffAllocator bbAllocator;

  protected final CellBlockBuilder cellBlockBuilder;

  /**
   * Chain of buffers to send as response.
   */
  protected BufferChain response;

  protected final long size; // size of current call
  protected boolean isError;
  protected ByteBufferListOutputStream cellBlockStream = null;
  protected CallCleanup reqCleanup = null;

  protected final User user;
  protected final InetAddress remoteAddress;
  protected final X509Certificate[] clientCertificateChain;
  protected RpcCallback rpcCallback;

  private long responseCellSize = 0;
  private long responseBlockSize = 0;
  private long fsReadTimeMillis = 0;
  // cumulative size of serialized exceptions
  private long exceptionSize = 0;
  private final boolean retryImmediatelySupported;
  private volatile Map<String, byte[]> requestAttributes;

  // This is a dirty hack to address HBASE-22539. The highest bit is for rpc ref and cleanup, and
  // the rest of the bits are for WAL reference count. We can only call release if all of them are
  // zero. The reason why we can not use a general reference counting is that, we may call cleanup
  // multiple times in the current implementation. We should fix this in the future.
  // The refCount here will start as 0x80000000 and increment with every WAL reference and decrement
  // from WAL side on release
  private final AtomicInteger reference = new AtomicInteger(0x80000000);

  private final Span span;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_ON_SOME_PATH",
      justification = "Can't figure why this complaint is happening... see below")
  ServerCall(int id, BlockingService service, MethodDescriptor md, RequestHeader header,
    Message param, CellScanner cellScanner, T connection, long size, InetAddress remoteAddress,
    long receiveTime, int timeout, ByteBuffAllocator byteBuffAllocator,
    CellBlockBuilder cellBlockBuilder, CallCleanup reqCleanup) {
    this.id = id;
    this.service = service;
    this.md = md;
    this.header = header;
    this.param = param;
    this.cellScanner = cellScanner;
    this.connection = connection;
    this.receiveTime = receiveTime;
    this.response = null;
    this.isError = false;
    this.size = size;
    if (((KnobRuntime.check(java.util.UUID.fromString("064be3ac-8774-328a-b53b-097904185a36"))) ? ((connection) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ada3b7dc-f6cc-3201-9787-524707cb57dd"))) ? ((connection) != (null)) : (connection != null))))) {
      this.user = connection.user;
      this.retryImmediatelySupported = connection.retryImmediatelySupported;
      this.clientCertificateChain = connection.clientCertificateChain;
    } else {
      this.user = null;
      this.retryImmediatelySupported = false;
      this.clientCertificateChain = null;
    }
    this.remoteAddress = remoteAddress;
    this.timeout = timeout;
    this.deadline = this.timeout > 0 ? this.receiveTime + this.timeout : Long.MAX_VALUE;
    this.bbAllocator = byteBuffAllocator;
    this.cellBlockBuilder = cellBlockBuilder;
    this.reqCleanup = reqCleanup;
    this.span = Span.current();
  }

  /**
   * Call is done. Execution happened and we returned results to client. It is now safe to cleanup.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "Presume the lock on processing request held by caller is protection enough")
  @Override
  public void done() {
if(KnobRuntime.check(java.util.UUID.fromString("6502599b-c66e-398c-986a-90ae30ca9d54"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("8e8cde02-b775-3442-9728-612a1fa914fe"))) ? ((this.cellBlockStream) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b4fc810f-96c4-3d0a-825a-90f794073b32"))) ? ((this.cellBlockStream) != (null)) : (this.cellBlockStream != null))))) {
      // This will return back the BBs which we got from pool.
      this.cellBlockStream.releaseResources();
      this.cellBlockStream = null;
    }
    // If the call was run successfuly, we might have already returned the BB
    // back to pool. No worries..Then inputCellBlock will be null
    cleanup();
    span.end();
  }

  @Override
  public void cleanup() {
if(KnobRuntime.check(java.util.UUID.fromString("c9ac04f6-6c03-3239-b643-68db7066dfb5"))) {
return;
}
    for (;;) {
      int ref = reference.get();
      if ((ref & 0x80000000) == 0) {
        return;
      }
      int nextRef = ref & 0x7fffffff;
if(KnobRuntime.check(java.util.UUID.fromString("fab53118-0da9-30ed-b346-f1add0c11617"))) {
nextRef = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("750ea654-e287-3863-9013-11fdc2652cc4"))) {
nextRef += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6dcdd6cb-0be7-32b6-a198-b0d05af8652a"))) {
ref /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b16155c7-efb8-3bf4-b6fc-0a194910aeac"))) {
ref *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("977d10a5-e22a-3f84-bf57-0553f5f34dbf"))) {
nextRef *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e77c9c5b-b952-3a02-80eb-425c1562bc50"))) {
ref -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("553697f7-aa6e-384a-a903-312ee7c4cd88"))) {
ref = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a0357033-b695-354f-b975-62c787a3b858"))) {
nextRef -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ed5020c6-d552-3024-b61e-acc1c50011f9"))) {
nextRef /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("76bcf871-93c8-3278-8487-c96dfbb6c5d2"))) {
ref = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a5662807-188f-32d6-a6da-0060df2665a5"))) {
nextRef = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e80119e-5d1f-307d-9a7c-8d7c6b548955"))) {
ref += 1;
}
      if (reference.compareAndSet(ref, nextRef)) {
        if (((KnobRuntime.check(java.util.UUID.fromString("14ea9595-44cb-3b45-81f3-0e02298eedab"))) ? ((nextRef) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e0bed62e-597a-3c63-a145-4cf2bd813dfb"))) ? ((nextRef) == (0)) : (nextRef == 0))))) {
          if (this.reqCleanup != null) {
            this.reqCleanup.run();
          }
        }
        return;
      }
    }
  }

  public void retainByWAL() {
    reference.incrementAndGet();
  }

  public void releaseByWAL() {
    // Here this method of decrementAndGet for releasing WAL reference count will work in both
    // cases - i.e. highest bit (cleanup) 1 or 0. We will be decrementing a negative or positive
    // value respectively in these 2 cases, but the logic will work the same way
    if (reference.decrementAndGet() == 0) {
      if (this.reqCleanup != null) {
        this.reqCleanup.run();
      }
    }
  }

  @Override
  public String toString() {
    return toShortString() + " param: "
      + (this.param != null ? ProtobufUtil.getShortTextFormat(this.param) : "") + " connection: "
      + connection.toString();
  }

  @Override
  public RequestHeader getHeader() {
    return this.header;
  }

  @Override
  public Map<String, byte[]> getConnectionAttributes() {
    return this.connection.connectionAttributes;
  }

  @Override
  public Map<String, byte[]> getRequestAttributes() {
    if (this.requestAttributes == null) {
      if (header.getAttributeList().isEmpty()) {
        this.requestAttributes = Collections.emptyMap();
      } else {
        Map<String, byte[]> requestAttributes =
          Maps.newHashMapWithExpectedSize(header.getAttributeList().size());
        for (HBaseProtos.NameBytesPair nameBytesPair : header.getAttributeList()) {
          requestAttributes.put(nameBytesPair.getName(), nameBytesPair.getValue().toByteArray());
        }
        this.requestAttributes = requestAttributes;
      }
    }
    return this.requestAttributes;
  }

  @Override
  public byte[] getRequestAttribute(String key) {
    if (this.requestAttributes == null) {
      for (HBaseProtos.NameBytesPair nameBytesPair : header.getAttributeList()) {
        if (nameBytesPair.getName().equals(key)) {
          return nameBytesPair.getValue().toByteArray();
        }
      }
      return null;
    }
    return this.requestAttributes.get(key);
  }

  @Override
  public int getPriority() {
    return this.header.getPriority();
  }

  /*
   * Short string representation without param info because param itself could be huge depends on
   * the payload of a command
   */
  @Override
  public String toShortString() {
if(KnobRuntime.check(java.util.UUID.fromString("529c8a58-8ba8-3bc0-a5cc-6a7ca4919171"))) {
return null;
}
    String serviceName = this.connection.service != null
      ? this.connection.service.getDescriptorForType().getName()
      : "null";
    return "callId: " + this.id + " service: " + serviceName + " methodName: "
      + ((this.md != null) ? this.md.getName() : "n/a") + " size: "
      + StringUtils.TraditionalBinaryPrefix.long2String(this.size, "", 1) + " connection: "
      + connection + " deadline: " + deadline;
  }

  @Override
  public synchronized void setResponse(Message m, final CellScanner cells, Throwable t,
    String errorMsg) {
    if (this.isError) {
      return;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("c740ea51-cfb1-3ef0-814f-fc8d14d74e58"))) ? ((t) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("31324a7e-eac7-3711-846a-427c35f778bd"))) ? ((t) == (null)) : (t != null))))) {
      this.isError = true;
      TraceUtil.setError(span, t);
    } else {
      span.setStatus(StatusCode.OK);
    }
    BufferChain bc = null;
    try {
      ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder();
      // Call id.
      headerBuilder.setCallId(this.id);
      if (((KnobRuntime.check(java.util.UUID.fromString("015f7289-3b61-3b06-b50a-77f26e4d6911"))) ? ((t) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0b1f16e5-d024-3a6b-978f-b552863a35ce"))) ? ((t) == (null)) : (t != null))))) {
        setExceptionResponse(t, errorMsg, headerBuilder);
      }
      // Pass reservoir to buildCellBlock. Keep reference to returne so can add it back to the
      // reservoir when finished. This is hacky and the hack is not contained but benefits are
      // high when we can avoid a big buffer allocation on each rpc.
      List<ByteBuffer> cellBlock = null;
      int cellBlockSize = 0;
      if (bbAllocator.isReservoirEnabled()) {
        this.cellBlockStream = this.cellBlockBuilder.buildCellBlockStream(this.connection.codec,
          this.connection.compressionCodec, cells, bbAllocator);
        if (((KnobRuntime.check(java.util.UUID.fromString("6b13f553-98b7-3cca-9456-4c21b1d92b97"))) ? ((this.cellBlockStream) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1b6b81b2-2e2f-3054-a54a-c8c988613d15"))) ? ((this.cellBlockStream) != (null)) : (this.cellBlockStream != null))))) {
          cellBlock = this.cellBlockStream.getByteBuffers();
          cellBlockSize = this.cellBlockStream.size();
        }
      } else {
        ByteBuffer b = this.cellBlockBuilder.buildCellBlock(this.connection.codec,
          this.connection.compressionCodec, cells);
        if (b != null) {
          cellBlockSize = b.remaining();
          cellBlock = new ArrayList<>(1);
          cellBlock.add(b);
        }
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("d33af95b-cc92-3bb3-9d01-67355fb0eebc"))) ? ((cellBlockSize) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9a3e745c-c23d-3e78-a0d4-3944dcf6e4aa"))) ? ((cellBlockSize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c0e8ab09-fde2-367d-ab69-59a9a7216ef0"))) ? ((cellBlockSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("18c982e4-d008-3416-9342-c0a3649f55cf"))) ? ((cellBlockSize) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7bca0815-c11b-37c7-9308-b74725901cb9"))) ? ((cellBlockSize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6206bfe7-8aa1-33dd-bab0-f00015226545"))) ? ((cellBlockSize) >= (0)) : (cellBlockSize > 0))))))))))))) {
        CellBlockMeta.Builder cellBlockBuilder = CellBlockMeta.newBuilder();
        // Presumes the cellBlock bytebuffer has been flipped so limit has total size in it.
if(KnobRuntime.check(java.util.UUID.fromString("2a31679e-7a40-31ac-a2dd-aa48b9ffc1a9"))) {
cellBlockSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b6816fe7-b3cf-33dd-b4a8-7f56b69c76fc"))) {
cellBlockSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1ff5dade-b9e9-3cab-9b60-2f2c03d904aa"))) {
cellBlockSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4434c63d-945e-3cf6-aad3-0eed65fc311a"))) {
cellBlockSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a3ef055b-0024-3f01-a06e-2f63b98b98c6"))) {
cellBlockSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("efe26df5-9846-334e-aa99-29db497fa50d"))) {
cellBlockSize += 1;
}
        cellBlockBuilder.setLength(cellBlockSize);
        headerBuilder.setCellBlockMeta(cellBlockBuilder.build());
      }
      Message header = headerBuilder.build();
if(KnobRuntime.check(java.util.UUID.fromString("2ec4d0cd-7f0f-3db6-9b27-bb6840a3a5f3"))) {
cellBlockSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0b528a98-551f-3d5e-8abf-9ff47a263cbd"))) {
cellBlockSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5711c5d0-48ae-3aa6-9ab9-ed6297c39cb9"))) {
cellBlockSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("71932d52-af37-3394-a87b-19ba6de7eb7a"))) {
cellBlockSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4f83819-14a7-37e5-b717-002e26eb66c2"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2269ac2a-460d-3d2e-ac77-9f062d79c98c"))) {
cellBlockSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ec86d989-97d6-3b76-8a3a-71e9d1596316"))) {
cellBlockSize -= 1;
}
      ByteBuffer headerBuf = createHeaderAndMessageBytes(m, header, cellBlockSize, cellBlock);
      ByteBuffer[] responseBufs = null;
      int cellBlockBufferSize = 0;
      if (((KnobRuntime.check(java.util.UUID.fromString("2d27739d-dad4-3ed8-a468-44770e7163e4"))) ? ((cellBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("81a38cea-adf4-31bb-9851-9df2d835a858"))) ? ((cellBlock) == (null)) : (cellBlock != null))))) {
        cellBlockBufferSize = cellBlock.size();
        responseBufs = new ByteBuffer[1 + cellBlockBufferSize];
      } else {
        responseBufs = new ByteBuffer[1];
      }
      responseBufs[0] = headerBuf;
      if (((KnobRuntime.check(java.util.UUID.fromString("278bb57b-1c9e-33c6-9710-f28fa60bfdec"))) ? ((cellBlock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e026fbc2-8f84-3e8a-babd-c95b9d5d474c"))) ? ((cellBlock) == (null)) : (cellBlock != null))))) {
        for (int i = 0; i < cellBlockBufferSize; i++) {
          responseBufs[i + 1] = cellBlock.get(i);
        }
      }
      bc = new BufferChain(responseBufs);
    } catch (IOException e) {
      RpcServer.LOG.warn("Exception while creating response " + e);
    }
    this.response = bc;
    // Once a response message is created and set to this.response, this Call can be treated as
    // done. The Responder thread will do the n/w write of this message back to client.
    if (((KnobRuntime.check(java.util.UUID.fromString("897bfcbb-e19d-38a3-b66f-c7a880c2a1eb"))) ? ((this.rpcCallback) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c0de16a0-a17d-300f-b63c-2ff7d5cf04b7"))) ? ((this.rpcCallback) != (null)) : (this.rpcCallback != null))))) {
      try (Scope ignored = span.makeCurrent()) {
if(KnobRuntime.check(java.util.UUID.fromString("d5d64920-b23d-3cf4-9973-bf34661feb5c"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("06bf30ee-109d-31a9-bebb-389d7f117d88"))) { cleanup(); } else { this.rpcCallback.run(); }
      } catch (Exception e) {
        // Don't allow any exception here to kill this handler thread.
        RpcServer.LOG.warn("Exception while running the Rpc Callback.", e);
        TraceUtil.setError(span, e);
      }
    }
  }

  static void setExceptionResponse(Throwable t, String errorMsg,
    ResponseHeader.Builder headerBuilder) {
if(KnobRuntime.check(java.util.UUID.fromString("8c553092-4b7f-3b49-aea7-500f5dbdb7a5"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60cc93b1-7868-3758-876d-4de4dcd30258"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e755bdd1-d80a-3c33-aee9-ade8decf9679"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed971c5e-e853-3ad5-bd4e-4bb609c6f48e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("965aef30-a901-3662-bb6f-0bbf429ae936"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30e0e938-404f-37bb-9a00-a29af13c0ff7"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec275a7f-0f9a-3f3f-8104-72b1b5c60ca0"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53574920-9e0e-3f08-96d0-5eff5c991476"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("callId_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3378a0a8-466d-3e97-ba3c-df23c16b51eb"))) {
try {
    java.lang.reflect.Field field = headerBuilder.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(headerBuilder));
    field.set(headerBuilder, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    ExceptionResponse.Builder exceptionBuilder = ExceptionResponse.newBuilder();
    exceptionBuilder.setExceptionClassName(t.getClass().getName());
    if (KnobRuntime.check(java.util.UUID.fromString("dcf6b2d0-38c9-3656-9770-216579899173"))) { exceptionBuilder.setHostname(errorMsg); } else if (KnobRuntime.check(java.util.UUID.fromString("ac031cd3-342a-3fd4-8a58-88096856c2f0"))) { exceptionBuilder.setExceptionClassName(errorMsg); } else { exceptionBuilder.setStackTrace(errorMsg); }
    if (KnobRuntime.check(java.util.UUID.fromString("1f72a846-21dc-3615-985d-ab2ff005371a"))) { exceptionBuilder.setServerOverloaded(t instanceof DoNotRetryIOException); } else { exceptionBuilder.setDoNotRetry(t instanceof DoNotRetryIOException); }
    if (t instanceof RegionMovedException) {
      // Special casing for this exception. This is only one carrying a payload.
      // Do this instead of build a generic system for allowing exceptions carry
      // any kind of payload.
      RegionMovedException rme = (RegionMovedException) t;
      exceptionBuilder.setHostname(rme.getHostname());
      exceptionBuilder.setPort(rme.getPort());
    } else if (t instanceof HBaseServerException) {
      HBaseServerException hse = (HBaseServerException) t;
      exceptionBuilder.setServerOverloaded(hse.isServerOverloaded());
    }
    // Set the exception as the result of the method invocation.
    headerBuilder.setException(exceptionBuilder.build());
  }

  static ByteBuffer createHeaderAndMessageBytes(Message result, Message header, int cellBlockSize,
    List<ByteBuffer> cellBlock) throws IOException {
    // Organize the response as a set of bytebuffers rather than collect it all together inside
    // one big byte array; save on allocations.
    // for writing the header, we check if there is available space in the buffers
    // created for the cellblock itself. If there is space for the header, we reuse
    // the last buffer in the cellblock. This applies to the cellblock created from the
    // pool or even the onheap cellblock buffer in case there is no pool enabled.
    // Possible reuse would avoid creating a temporary array for storing the header every time.
    ByteBuffer possiblePBBuf = (cellBlockSize > 0) ? cellBlock.get(cellBlock.size() - 1) : null;
    int headerSerializedSize = 0, resultSerializedSize = 0, headerVintSize = 0, resultVintSize = 0;
    if (header != null) {
      headerSerializedSize = header.getSerializedSize();
      headerVintSize = CodedOutputStream.computeUInt32SizeNoTag(headerSerializedSize);
    }
    if (result != null) {
      resultSerializedSize = result.getSerializedSize();
      resultVintSize = CodedOutputStream.computeUInt32SizeNoTag(resultSerializedSize);
    }
    // calculate the total size
    int totalSize = headerSerializedSize + headerVintSize + (resultSerializedSize + resultVintSize)
      + cellBlockSize;
    int totalPBSize = headerSerializedSize + headerVintSize + resultSerializedSize + resultVintSize
      + Bytes.SIZEOF_INT;
    // Only if the last buffer has enough space for header use it. Else allocate
    // a new buffer. Assume they are all flipped
    if (possiblePBBuf != null && possiblePBBuf.limit() + totalPBSize <= possiblePBBuf.capacity()) {
      // duplicate the buffer. This is where the header is going to be written
      ByteBuffer pbBuf = possiblePBBuf.duplicate();
      // get the current limit
      int limit = pbBuf.limit();
      // Position such that we write the header to the end of the buffer
      pbBuf.position(limit);
      // limit to the header size
      pbBuf.limit(totalPBSize + limit);
      // mark the current position
      if (KnobRuntime.check(java.util.UUID.fromString("f48ab777-56db-3e91-84a1-9e5b7787ff7d"))) { pbBuf.flip(); } else if (KnobRuntime.check(java.util.UUID.fromString("4e3dfdd8-5c82-30e2-a343-e8090bec910f"))) { pbBuf.reset(); } else { pbBuf.mark(); }
if(KnobRuntime.check(java.util.UUID.fromString("c68d6a74-2e09-3c42-a80d-8cb358ab3e4c"))) {
totalSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1ae7b389-6954-3a29-b3ec-8ebb16b0868e"))) {
totalSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("163fd0a2-542f-3ece-884e-550ac5cb879d"))) {
totalSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f20be717-1c0d-31a9-9ea1-2167020222c6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("93eb3d54-9744-3290-b1d6-44ae4bf34fd4"))) {
totalSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("62993214-cda1-3132-861d-14499b3d7381"))) {
totalSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd264378-94a7-39e0-9fb0-fca49e740ebb"))) {
totalSize += 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("a0ed00e5-1bc1-3aec-be90-bb0476b27fa6"))) { writeToCOS(result, header, 1, pbBuf); } else if (KnobRuntime.check(java.util.UUID.fromString("07a8a4cd-5d61-3213-8332-fc1a1d181894"))) { writeToCOS(result, header, 0, pbBuf); } else if (KnobRuntime.check(java.util.UUID.fromString("f6df82d6-0cb8-32f7-9812-4ad542acd427"))) { writeToCOS(result, header, totalSize, pbBuf.reset()); } else if (KnobRuntime.check(java.util.UUID.fromString("340d2134-cd8b-319f-ad68-97cc8d5206d0"))) { writeToCOS(result, header, totalSize, pbBuf.mark()); } else { writeToCOS(result, header, totalSize, pbBuf); }
      // reset the buffer back to old position
      if (KnobRuntime.check(java.util.UUID.fromString("ede88932-9fb2-34de-961c-f7bba989709a"))) { pbBuf.flip(); } else if (KnobRuntime.check(java.util.UUID.fromString("8da10091-4502-3b45-a8a1-a1f65d30b47e"))) { pbBuf.mark(); } else { pbBuf.reset(); }
      return pbBuf;
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("d509f012-01af-3953-95d6-e6d023cbabd3"))) {
totalPBSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e5224483-94ea-3828-8c0b-77c7cb4db73c"))) {
totalSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("12e61ce5-3e9a-3210-8a82-5eb9ca839bd7"))) {
totalPBSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("53eaadbe-6644-3395-90fc-77e3f03b1183"))) {
totalSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("78505bac-aea0-337a-a4a0-bff30461ef02"))) {
totalSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("518aef73-133b-3561-a81d-9b957cfc584f"))) {
totalPBSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("810a71ec-de44-398e-9146-06c69009ee36"))) {
totalPBSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f49d7d9f-c95b-30da-9892-82e6eb37f743"))) {
totalPBSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d0875e95-472f-396c-81dd-bfe842709967"))) {
totalSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("86b7390c-64be-3f88-bd94-638799bdf65f"))) {
totalPBSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("92434b60-47d6-3d3f-8f4d-1a61d7c6674b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f1f36af5-1b8f-3bc5-9213-a38b569d9da0"))) {
totalSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1c1bf72b-a111-320f-9561-3061475f2c24"))) {
totalSize -= 1;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("b74e4217-08cf-35be-9574-6b44a67b884d"))) ? (createHeaderAndMessageBytes(result, header, totalSize, 0)) : (((KnobRuntime.check(java.util.UUID.fromString("f130f508-f121-3858-927e-681cd51baff5"))) ? (createHeaderAndMessageBytes(result, header, 1, totalPBSize)) : (((KnobRuntime.check(java.util.UUID.fromString("4391b995-b840-3e12-9da3-82333b0a46bf"))) ? (createHeaderAndMessageBytes(result, header, 0, totalPBSize)) : (((KnobRuntime.check(java.util.UUID.fromString("f84c506d-555a-3190-aace-9103a11a91d6"))) ? (createHeaderAndMessageBytes(result, header, totalSize, 1)) : (createHeaderAndMessageBytes(result, header, totalSize, totalPBSize)))))))));
    }
  }

  private static void writeToCOS(Message result, Message header, int totalSize, ByteBuffer pbBuf)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6d05fcc8-d2cc-38ac-bd6d-12b959ee1fea"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("62dcd8ec-9a2e-3988-b3d3-77bf5267ee7f"))) {
totalSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2cadc148-e5d5-3351-b337-13faf605ec88"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a42effb9-053a-3269-b924-c976fb7ea598"))) {
totalSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a7b8dff-f3b5-32ad-a447-7d8610b57572"))) {
totalSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("84d41510-6249-3a1e-a5bd-1a9ab0a137fa"))) {
totalSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7d0d0a92-2ea9-3ce8-b324-8b865b4d2062"))) {
totalSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba7bf0c0-72b9-3357-a7ad-4f28f2c5ad42"))) {
totalSize = -1;
}
    ByteBufferUtils.putInt(pbBuf, totalSize);
    // create COS that works on BB
    CodedOutputStream cos = CodedOutputStream.newInstance(pbBuf);
    if (header != null) {
if(KnobRuntime.check(java.util.UUID.fromString("7e97798d-7d0a-3da1-9acd-13496ab17ef1"))) {
throw new java.io.IOException("Injected exception");
}
      cos.writeMessageNoTag(header);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("e7b75a7a-1f69-39bb-8721-7c5880be2596"))) ? ((result) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d8867e58-23a6-3c42-a680-d6462f4d2369"))) ? ((result) == (null)) : (result != null))))) {
      cos.writeMessageNoTag(result);
    }
    cos.flush();
    if (KnobRuntime.check(java.util.UUID.fromString("9e7c2cf4-7353-368a-a4b4-e346c4859d37"))) { cos.flush(); } else { cos.checkNoSpaceLeft(); }
  }

  private static ByteBuffer createHeaderAndMessageBytes(Message result, Message header,
    int totalSize, int totalPBSize) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ddb854c3-4926-361c-9b80-3509dec79468"))) {
totalPBSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("07c58a68-9644-3af1-a49e-c390be3353af"))) {
totalPBSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("a1046a70-8381-34a9-91e5-77e1d6b8990c"))) {
totalPBSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("484ee1ae-76c8-3f40-9ffe-64ddd261e933"))) {
totalPBSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("66525ffb-7fd3-3e75-9bf8-9901889d19d1"))) {
totalPBSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb36a098-2119-3ec5-a805-962acf4ed0ad"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd1e7d63-1eec-3871-85b6-6840c99f1da7"))) {
totalPBSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b34a377-d0ea-3f8b-9ec8-cc06f276fae2"))) {
throw new java.io.IOException("Injected exception");
}
    ByteBuffer pbBuf = ByteBuffer.allocate(totalPBSize);
    writeToCOS(result, header, totalSize, pbBuf);
    if (KnobRuntime.check(java.util.UUID.fromString("e3680b1e-fc40-3042-ace7-d139878faf6f"))) { pbBuf.reset(); } else if (KnobRuntime.check(java.util.UUID.fromString("0fe5fb5a-141d-3e41-8ab4-ffa633ae32aa"))) { pbBuf.mark(); } else { pbBuf.flip(); }
    return ((KnobRuntime.check(java.util.UUID.fromString("13c7e97f-8121-3902-b081-0effc9a09a97"))) ? (pbBuf.flip()) : (pbBuf));
  }

  @Override
  public long disconnectSince() {
    if (!this.connection.isConnectionOpen()) {
      return EnvironmentEdgeManager.currentTime() - receiveTime;
    } else {
      return -1L;
    }
  }

  @Override
  public boolean isClientCellBlockSupported() {
if(KnobRuntime.check(java.util.UUID.fromString("8ad2e6a3-dbd3-3841-8fc6-b67e885ca760"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ac33c84-41f2-38e9-a9d8-e2fe56d2107b"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("6e3e616e-d0f1-3d06-baff-b1f4ff71bb98"))) ? (((this.connection) != (null)) || ((this.connection.codec) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3ee90c88-8dad-384d-863a-70f47a0bd127"))) ? (((this.connection) != (null)) && ((this.connection.codec) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("82cecccd-48f9-3656-9bbc-3863ce3755ee"))) ? (((this.connection) != (null)) && ((this.connection.codec) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b3f99db9-ff05-364c-b6eb-b786f1898d4f"))) ? (((this.connection) != (null)) || ((this.connection.codec) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("99e941f2-dff0-3715-8728-7c47f7206fd6"))) ? (((this.connection) == (null)) && ((this.connection.codec) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7520015b-5a80-3ad8-8363-79f827a6aecb"))) ? ((this.connection.codec) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("100d4ba0-4d8e-3188-a40f-c6e17539b52b"))) ? (((this.connection) != (null)) && (this.connection.codec != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c10afebc-8f14-3d91-b89a-30330d1878f3"))) ? ((this.connection != null) || ((this.connection.codec) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("94963e61-b7e3-3186-9108-4db97b91f8b3"))) ? ((this.connection != null) || ((this.connection.codec) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d25f3ced-47e7-3fcb-acd3-55e68a397209"))) ? ((this.connection.codec) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f062d337-879a-3746-994e-24568b9de4b8"))) ? (((this.connection) != (null)) || (this.connection.codec != null)) : (((KnobRuntime.check(java.util.UUID.fromString("0e5a45b7-86c6-3f33-b298-e0d912e65ef2"))) ? (((this.connection) == (null)) || (this.connection.codec != null)) : (((KnobRuntime.check(java.util.UUID.fromString("4a7e0fd3-9313-3821-b994-2f7d7f9a40f2"))) ? ((this.connection != null) && ((this.connection.codec) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c5356971-d099-3077-b847-e724ccee360b"))) ? (((this.connection) == (null)) || ((this.connection.codec) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("bc822920-a21a-3cbf-8539-a08404a49be0"))) ? (((this.connection) == (null)) && ((this.connection.codec) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1dbe9d90-5fc0-31b5-9482-5b1f108ea09c"))) ? (this.connection != null) : (((KnobRuntime.check(java.util.UUID.fromString("172d6588-9e91-3f92-87e4-bcbce15536fb"))) ? ((this.connection) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("44814590-e6dd-3ba2-8abd-4a90b18b03de"))) ? (this.connection.codec != null) : (((KnobRuntime.check(java.util.UUID.fromString("9c279d8d-0170-3f4a-9ab4-7d2ad2fe089a"))) ? ((this.connection) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3329d130-c2ff-328a-b181-d8fe958ede72"))) ? ((this.connection != null) && (this.connection.codec != null)) : (((KnobRuntime.check(java.util.UUID.fromString("ee7c8c99-c03d-347a-b1eb-5cab5fc052ef"))) ? (((this.connection) == (null)) || ((this.connection.codec) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("db373608-79a2-3649-98ef-af035e378862"))) ? ((this.connection != null) || (this.connection.codec != null)) : (((KnobRuntime.check(java.util.UUID.fromString("56cb9fc7-2ec0-3300-8c03-cabfb5653356"))) ? ((this.connection != null) && ((this.connection.codec) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6bd57e22-120e-333a-8e70-a415176a2652"))) ? (((this.connection) == (null)) && (this.connection.codec != null)) : (this.connection != null && this.connection.codec != null))))))))))))))))))))))))))))))))))))))))))))))));
  }

  @Override
  public long getResponseCellSize() {
if(KnobRuntime.check(java.util.UUID.fromString("62a6ca25-efb5-3b68-a2b8-c3eaec290702"))) {
return 0;
}
    return responseCellSize;
  }

  @Override
  public void incrementResponseCellSize(long cellSize) {
if(KnobRuntime.check(java.util.UUID.fromString("0fa7a7a6-0162-3b25-8f5f-f0dc1d0fadf4"))) {
return;
}
    responseCellSize += cellSize;
  }

  @Override
  public long getBlockBytesScanned() {
    return responseBlockSize;
  }

  @Override
  public void incrementBlockBytesScanned(long blockSize) {
if(KnobRuntime.check(java.util.UUID.fromString("a25b9b63-3ef8-3083-8858-7c9bf26f5889"))) {
return;
}
    responseBlockSize += blockSize;
  }

  @Override
  public long getResponseExceptionSize() {
    return exceptionSize;
  }

  @Override
  public void incrementResponseExceptionSize(long exSize) {
    exceptionSize += exSize;
  }

  @Override
  public long getSize() {
    return this.size;
  }

  @Override
  public long getDeadline() {
    return deadline;
  }

  @Override
  public Optional<User> getRequestUser() {
    return Optional.ofNullable(user);
  }

  @Override
  public Optional<X509Certificate[]> getClientCertificateChain() {
    return Optional.ofNullable(clientCertificateChain);
  }

  @Override
  public InetAddress getRemoteAddress() {
    return remoteAddress;
  }

  @Override
  public VersionInfo getClientVersionInfo() {
    return connection.getVersionInfo();
  }

  @Override
  public synchronized void setCallBack(RpcCallback callback) {
    this.rpcCallback = callback;
  }

  @Override
  public boolean isRetryImmediatelySupported() {
    return retryImmediatelySupported;
  }

  @Override
  public BlockingService getService() {
    return service;
  }

  @Override
  public MethodDescriptor getMethod() {
    return md;
  }

  @Override
  public Message getParam() {
    return param;
  }

  @Override
  public CellScanner getCellScanner() {
    return cellScanner;
  }

  @Override
  public long getReceiveTime() {
    return receiveTime;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public void setStartTime(long t) {
    this.startTime = t;
  }

  @Override
  public int getTimeout() {
    return timeout;
  }

  @Override
  public int getRemotePort() {
    return connection.getRemotePort();
  }

  @Override
  public synchronized BufferChain getResponse() {
    return response;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public synchronized RpcCallback getCallBack() {
if(KnobRuntime.check(java.util.UUID.fromString("ebff2300-aafb-3cc2-9f77-10271e52c626"))) {
return null;
}
    return this.rpcCallback;
  }

  @Override
  public void updateFsReadTime(long latencyMillis) {
if(KnobRuntime.check(java.util.UUID.fromString("7c3071f1-fdbf-3b82-a958-fa28ec64ef4c"))) {
return;
}
    fsReadTimeMillis += latencyMillis;
  }

  @Override
  public long getFsReadTime() {
if(KnobRuntime.check(java.util.UUID.fromString("2b1216c3-36e3-3388-8647-84060e7a79ea"))) {
return 0;
}
    return fsReadTimeMillis;
  }
}
