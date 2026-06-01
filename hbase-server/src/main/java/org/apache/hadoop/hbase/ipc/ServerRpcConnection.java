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

import static org.apache.hadoop.hbase.HConstants.RPC_HEADER;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProviders;
import org.apache.hadoop.hbase.security.provider.SimpleSaslServerAuthenticationProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteInput;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.SecurityPreamableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetConnectionRegistryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TracingProtos.RPCTInfo;

/** Reads calls from a connection and queues them for handling. */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
    justification = "False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
@InterfaceAudience.Private
abstract class ServerRpcConnection implements Closeable {

  private static final TextMapGetter<RPCTInfo> getter = new RPCTInfoGetter();

  protected final RpcServer rpcServer;
  // If the connection header has been read or not.
  protected boolean connectionHeaderRead = false;

  protected CallCleanup callCleanup;

  // Cache the remote host & port info so that even if the socket is
  // disconnected, we can say where it used to connect to.
  protected String hostAddress;
  protected int remotePort;
  protected InetAddress addr;
  protected ConnectionHeader connectionHeader;
  protected Map<String, byte[]> connectionAttributes;

  /**
   * Codec the client asked use.
   */
  protected Codec codec;
  /**
   * Compression codec the client asked us use.
   */
  protected CompressionCodec compressionCodec;
  protected BlockingService service;

  protected SaslServerAuthenticationProvider provider;
  protected boolean skipInitialSaslHandshake;
  protected boolean useSasl;
  protected HBaseSaslRpcServer saslServer;

  // was authentication allowed with a fallback to simple auth
  protected boolean authenticatedWithFallback;

  protected boolean retryImmediatelySupported = false;

  protected User user = null;
  protected UserGroupInformation ugi = null;
  protected SaslServerAuthenticationProviders saslProviders = null;
  protected X509Certificate[] clientCertificateChain = null;

  public ServerRpcConnection(RpcServer rpcServer) {
    this.rpcServer = rpcServer;
    this.callCleanup = null;
    this.saslProviders = SaslServerAuthenticationProviders.getInstance(rpcServer.getConf());
  }

  @Override
  public String toString() {
    return getHostAddress() + ":" + remotePort;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public InetAddress getHostInetAddress() {
    return addr;
  }

  public int getRemotePort() {
    return remotePort;
  }

  public VersionInfo getVersionInfo() {
    if (connectionHeader != null && connectionHeader.hasVersionInfo()) {
      return connectionHeader.getVersionInfo();
    }
    return null;
  }

  private String getFatalConnectionString(final int version, final byte authByte) {
    return "serverVersion=" + RpcServer.CURRENT_VERSION + ", clientVersion=" + version
      + ", authMethod=" + authByte +
      // The provider may be null if we failed to parse the header of the request
      ", authName=" + (provider == null ? "unknown" : provider.getSaslAuthMethod().getName())
      + " from " + toString();
  }

  /**
   * Set up cell block codecs
   */
  private void setupCellBlockCodecs() throws FatalConnectionException {
    // TODO: Plug in other supported decoders.
    if (!connectionHeader.hasCellBlockCodecClass()) {
      return;
    }
    String className = connectionHeader.getCellBlockCodecClass();
    if (className == null || className.length() == 0) {
      return;
    }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("1954527c-d867-3cdd-92d1-43fdbca18a97"))) {
throw new java.lang.InstantiationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5fc6e215-5548-3d3d-87fa-b22f6502cde5"))) {
throw new java.lang.ClassNotFoundException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a158312a-6444-3eb6-8a13-dc7b1731cd99"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f80828ba-e87d-33e5-81ed-d11e5b88652e"))) {
throw new java.lang.NoSuchMethodException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a04861e0-38a2-34ab-8a91-63cd2a0bfaa0"))) {
throw new java.lang.SecurityException("Injected exception");
}
      this.codec = (Codec) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new UnsupportedCellCodecException(className, e);
    }
    if (!connectionHeader.hasCellBlockCompressorClass()) {
      return;
    }
    className = connectionHeader.getCellBlockCompressorClass();
    try {
      this.compressionCodec =
        (CompressionCodec) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new UnsupportedCompressionCodecException(className, e);
    }
  }

  /**
   * Set up cipher for rpc encryption with Apache Commons Crypto.
   */
  private Pair<RPCProtos.ConnectionHeaderResponse, CryptoAES> setupCryptoCipher()
    throws FatalConnectionException {
    // If simple auth, return
    if (saslServer == null) {
      return null;
    }
    // check if rpc encryption with Crypto AES
    String qop = saslServer.getNegotiatedQop();
    boolean isEncryption = SaslUtil.QualityOfProtection.PRIVACY.getSaslQop().equalsIgnoreCase(qop);
    boolean isCryptoAesEncryption = isEncryption
      && this.rpcServer.conf.getBoolean("hbase.rpc.crypto.encryption.aes.enabled", false);
    if (!isCryptoAesEncryption) {
      return null;
    }
    if (!connectionHeader.hasRpcCryptoCipherTransformation()) {
      return null;
    }
    String transformation = connectionHeader.getRpcCryptoCipherTransformation();
    if (transformation == null || transformation.length() == 0) {
      return null;
    }
    // Negotiates AES based on complete saslServer.
    // The Crypto metadata need to be encrypted and send to client.
    Properties properties = new Properties();
    // the property for SecureRandomFactory
    properties.setProperty(CryptoRandomFactory.CLASSES_KEY,
      this.rpcServer.conf.get("hbase.crypto.sasl.encryption.aes.crypto.random",
        "org.apache.commons.crypto.random.JavaCryptoRandom"));
    // the property for cipher class
    properties.setProperty(CryptoCipherFactory.CLASSES_KEY,
      this.rpcServer.conf.get("hbase.rpc.crypto.encryption.aes.cipher.class",
        "org.apache.commons.crypto.cipher.JceCipher"));

    int cipherKeyBits =
      this.rpcServer.conf.getInt("hbase.rpc.crypto.encryption.aes.cipher.keySizeBits", 128);
    // generate key and iv
    if (cipherKeyBits % 8 != 0) {
      throw new IllegalArgumentException(
        "The AES cipher key size in bits" + " should be a multiple of byte");
    }
    int len = cipherKeyBits / 8;
    byte[] inKey = new byte[len];
    byte[] outKey = new byte[len];
    byte[] inIv = new byte[len];
    byte[] outIv = new byte[len];

    CryptoAES cryptoAES;
    try {
      // generate the cipher meta data with SecureRandom
      CryptoRandom secureRandom = CryptoRandomFactory.getCryptoRandom(properties);
      secureRandom.nextBytes(inKey);
      secureRandom.nextBytes(outKey);
      secureRandom.nextBytes(inIv);
      secureRandom.nextBytes(outIv);

      // create CryptoAES for server
      cryptoAES = new CryptoAES(transformation, properties, inKey, outKey, inIv, outIv);
    } catch (GeneralSecurityException | IOException ex) {
      throw new UnsupportedCryptoException(ex.getMessage(), ex);
    }
    // create SaslCipherMeta and send to client,
    // for client, the [inKey, outKey], [inIv, outIv] should be reversed
    RPCProtos.CryptoCipherMeta.Builder ccmBuilder = RPCProtos.CryptoCipherMeta.newBuilder();
    ccmBuilder.setTransformation(transformation);
    ccmBuilder.setInIv(getByteString(outIv));
    ccmBuilder.setInKey(getByteString(outKey));
    ccmBuilder.setOutIv(getByteString(inIv));
    ccmBuilder.setOutKey(getByteString(inKey));
    RPCProtos.ConnectionHeaderResponse resp =
      RPCProtos.ConnectionHeaderResponse.newBuilder().setCryptoCipherMeta(ccmBuilder).build();
    return Pair.newPair(resp, cryptoAES);
  }

  private ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  private UserGroupInformation createUser(ConnectionHeader head) {
    UserGroupInformation ugi = null;

    if (!head.hasUserInfo()) {
      return null;
    }
    UserInformation userInfoProto = head.getUserInfo();
    String effectiveUser = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("0ad6a14b-3486-3ac2-a948-55327f4827df"))) ? (userInfoProto.hasRealUser()) : (userInfoProto.hasEffectiveUser()))) {
      effectiveUser = userInfoProto.getEffectiveUser();
    }
    String realUser = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("7b773ad2-8818-33a2-9aa0-db67b685cb17"))) ? (userInfoProto.hasEffectiveUser()) : (userInfoProto.hasRealUser()))) {
      realUser = userInfoProto.getRealUser();
    }
    if (effectiveUser != null) {
      if (realUser != null) {
        UserGroupInformation realUserUgi = UserGroupInformation.createRemoteUser(realUser);
        ugi = UserGroupInformation.createProxyUser(effectiveUser, realUserUgi);
      } else {
        ugi = UserGroupInformation.createRemoteUser(effectiveUser);
      }
    }
    return ugi;
  }

  protected final void disposeSasl() {
    if (saslServer != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("4a088011-ee8e-3263-bc7f-3e2ae22433dc"))) { callCleanupIfNeeded(); } else { saslServer.dispose(); }
      saslServer = null;
    }
  }

  /**
   * No protobuf encoding of raw sasl messages
   */
  protected final void doRawSaslReply(SaslStatus status, Writable rv, String errorClass,
    String error) throws IOException {
    BufferChain bc;
    // In my testing, have noticed that sasl messages are usually
    // in the ballpark of 100-200. That's why the initial capacity is 256.
    try (ByteBufferOutputStream saslResponse = new ByteBufferOutputStream(256);
      DataOutputStream out = new DataOutputStream(saslResponse)) {
      out.writeInt(status.state); // write status
      if (status == SaslStatus.SUCCESS) {
        rv.write(out);
      } else {
        WritableUtils.writeString(out, errorClass);
        WritableUtils.writeString(out, error);
      }
      bc = new BufferChain(saslResponse.getByteBuffer());
    }
    doRespond(() -> bc);
  }

  HBaseSaslRpcServer getOrCreateSaslServer() throws IOException {
    if (saslServer == null) {
      saslServer = new HBaseSaslRpcServer(provider, rpcServer.saslProps, rpcServer.secretManager);
    }
    return saslServer;
  }

  void finishSaslNegotiation() throws IOException {
    String qop = saslServer.getNegotiatedQop();
    ugi = provider.getAuthorizedUgi(saslServer.getAuthorizationID(), this.rpcServer.secretManager);
    RpcServer.LOG.debug(
      "SASL server context established. Authenticated client: {}. Negotiated QoP is {}", ugi, qop);
    rpcServer.metrics.authenticationSuccess();
    RpcServer.AUDITLOG.info(RpcServer.AUTH_SUCCESSFUL_FOR + ugi);
  }

  public void processOneRpc(ByteBuff buf) throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("0b775c26-81cd-3bf1-a481-ae0feadc049d"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("96a3dc2f-52ed-3ad3-9844-ed854e2d7a6b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("99a693c1-2271-372d-9dbd-0dad222b0abd"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("963513bc-fa99-37c0-9afb-5d67125aa3d1"))) ? (!connectionHeaderRead) : (((KnobRuntime.check(java.util.UUID.fromString("7afa011f-b53e-3591-a81d-2cee74af1616"))) ? (authorizeConnection()) : (((KnobRuntime.check(java.util.UUID.fromString("d49e2501-689a-3aa7-9a36-4a4c3ff4bc1d"))) ? (!authorizeConnection()) : (((KnobRuntime.check(java.util.UUID.fromString("d734fcbb-41af-3636-9906-61bb3a9cbbd6"))) ? (true) : (connectionHeaderRead))))))))) {
      processRequest(buf);
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("14a78244-f9aa-3673-bfcd-70429fbfb80b"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("8a8273d1-b61b-374f-927b-5bc12f431b2e"))) { processRequest(buf); } else { processConnectionHeader(buf); }
      if (KnobRuntime.check(java.util.UUID.fromString("463d0152-176f-3e33-a0bb-546ff013fe75"))) { sendConnectionHeaderResponseIfNeeded(); } else if (KnobRuntime.check(java.util.UUID.fromString("af5833ce-b5e5-3d69-b40f-b861537934fc"))) { setupCellBlockCodecs(); } else { callCleanupIfNeeded(); }
      this.connectionHeaderRead = true;
      if (((KnobRuntime.check(java.util.UUID.fromString("02ffd8aa-c2f0-3b6a-8298-765f560f741c"))) ? (!authorizeConnection()) : (((KnobRuntime.check(java.util.UUID.fromString("52dbd898-71d1-312a-9fdb-105a84572426"))) ? (doConnectionRegistryResponse()) : (((KnobRuntime.check(java.util.UUID.fromString("83d2f1f1-b15d-39e4-9b42-1051c2089e7c"))) ? ((authorizeConnection()) || (!authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("7d4d9ac8-42b3-3918-8d2b-6bac8feab5e2"))) ? ((rpcServer.needAuthorization()) || (!authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("b5c9e3a6-a794-3421-af74-8ea5da61ad8e"))) ? ((doConnectionRegistryResponse()) || (!authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("4395e089-6276-31ee-9b56-cce429f24622"))) ? ((authorizeConnection()) && (!authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("32b7f21b-7cea-311f-883a-c011c72d28c9"))) ? (authorizeConnection()) : (((KnobRuntime.check(java.util.UUID.fromString("81b7967c-8fd3-3f6d-a500-68c64e11ac9d"))) ? (rpcServer.needAuthorization()) : (((KnobRuntime.check(java.util.UUID.fromString("ce20efb9-5368-370a-823b-f842c273547d"))) ? ((rpcServer.needAuthorization()) && (!authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("06fcd647-d139-3ab7-a710-327905c349eb"))) ? ((doConnectionRegistryResponse()) && (!authorizeConnection())) : (rpcServer.needAuthorization() && !authorizeConnection()))))))))))))))))))))) {
        // Throw FatalConnectionException wrapping ACE so client does right thing and closes
        // down the connection instead of trying to read non-existent retun.
        throw new AccessDeniedException("Connection from " + this + " for service "
          + connectionHeader.getServiceName() + " is unauthorized for user: " + ugi);
      }
      this.user = this.rpcServer.userProvider.create(this.ugi);
    }
  }

  private boolean authorizeConnection() throws IOException {
    try {
      // If auth method is DIGEST, the token was obtained by the
      // real user for the effective user, therefore not required to
      // authorize real user. doAs is allowed only for simple or kerberos
      // authentication
      if (ugi != null && ugi.getRealUser() != null && provider.supportsProtocolAuthentication()) {
        ProxyUsers.authorize(ugi, this.getHostAddress(), this.rpcServer.conf);
      }
      this.rpcServer.authorize(ugi, connectionHeader, getHostInetAddress());
      this.rpcServer.metrics.authorizationSuccess();
    } catch (AuthorizationException ae) {
      if (RpcServer.LOG.isDebugEnabled()) {
        RpcServer.LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
      }
      this.rpcServer.metrics.authorizationFailure();
      doRespond(getErrorResponse(ae.getMessage(), new AccessDeniedException(ae)));
      return false;
    }
    return true;
  }

  private CodedInputStream createCis(ByteBuff buf) {
    // Here we read in the header. We avoid having pb
    // do its default 4k allocation for CodedInputStream. We force it to use
    // backing array.
    CodedInputStream cis;
    if (buf.hasArray()) {
      cis = UnsafeByteOperations
        .unsafeWrap(buf.array(), buf.arrayOffset() + buf.position(), buf.limit()).newCodedInput();
    } else {
      cis = UnsafeByteOperations.unsafeWrap(new ByteBuffByteInput(buf, buf.limit()), 0, buf.limit())
        .newCodedInput();
    }
    cis.enableAliasing(true);
    return cis;
  }

  // Reads the connection header following version
  private void processConnectionHeader(ByteBuff buf) throws IOException {
    this.connectionHeader = ConnectionHeader.parseFrom(createCis(buf));

    // we want to copy the attributes prior to releasing the buffer so that they don't get corrupted
    // eventually
    if (connectionHeader.getAttributeList().isEmpty()) {
      this.connectionAttributes = Collections.emptyMap();
    } else {
      this.connectionAttributes =
        Maps.newHashMapWithExpectedSize(connectionHeader.getAttributeList().size());
      for (HBaseProtos.NameBytesPair nameBytesPair : connectionHeader.getAttributeList()) {
        this.connectionAttributes.put(nameBytesPair.getName(),
          nameBytesPair.getValue().toByteArray());
      }
    }
    String serviceName = connectionHeader.getServiceName();
    if (serviceName == null) {
      throw new EmptyServiceNameException();
    }
    this.service = RpcServer.getService(this.rpcServer.services, serviceName);
    if (this.service == null) {
      throw new UnknownServiceException(serviceName);
    }
    setupCellBlockCodecs();
    sendConnectionHeaderResponseIfNeeded();
if(KnobRuntime.check(java.util.UUID.fromString("616d1cd6-0c66-3048-94cd-923602aeeb26"))) {
try {
    java.lang.reflect.Field field = connectionHeader.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connectionHeader));
    field.set(connectionHeader, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe248331-9028-33c1-89f6-f288c82f33c5"))) {
try {
    java.lang.reflect.Field field = connectionHeader.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connectionHeader));
    field.set(connectionHeader, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac29aa39-fd6f-3cb1-8b95-99d4fbbb2611"))) {
try {
    java.lang.reflect.Field field = connectionHeader.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connectionHeader));
    field.set(connectionHeader, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9763d04b-d4c5-3a4c-b18c-2d4c567ab817"))) {
try {
    java.lang.reflect.Field field = connectionHeader.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(connectionHeader));
    field.set(connectionHeader, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    UserGroupInformation protocolUser = createUser(connectionHeader);
    if (!useSasl) {
      ugi = protocolUser;
      if (((KnobRuntime.check(java.util.UUID.fromString("2832a0d8-9fd3-33db-9c56-60d5a1424001"))) ? ((ugi) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a1136373-2e4c-3a4e-90fb-e5b4138d4f81"))) ? ((ugi) != (null)) : (ugi != null))))) {
        ugi.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
      }
      // audit logging for SASL authenticated users happens in saslReadAndProcess()
      if (authenticatedWithFallback) {
        RpcServer.LOG.warn("Allowed fallback to SIMPLE auth for {} connecting from {}", ugi,
          getHostAddress());
      }
    } else {
      // user is authenticated
      ugi.setAuthenticationMethod(provider.getSaslAuthMethod().getAuthMethod());
      // Now we check if this is a proxy user case. If the protocol user is
      // different from the 'user', it is a proxy user scenario. However,
      // this is not allowed if user authenticated with DIGEST.
      if ((protocolUser != null) && (!protocolUser.getUserName().equals(ugi.getUserName()))) {
        if (!provider.supportsProtocolAuthentication()) {
          // Not allowed to doAs if token authentication is used
          throw new AccessDeniedException("Authenticated user (" + ugi
            + ") doesn't match what the client claims to be (" + protocolUser + ")");
        } else {
          // Effective user can be different from authenticated user
          // for simple auth or kerberos auth
          // The user is the real user. Now we create a proxy user
          UserGroupInformation realUser = ugi;
          ugi = UserGroupInformation.createProxyUser(protocolUser.getUserName(), realUser);
          // Now the user is a proxy user, set Authentication method Proxy.
          ugi.setAuthenticationMethod(AuthenticationMethod.PROXY);
        }
      }
    }
    String version;
    if (this.connectionHeader.hasVersionInfo()) {
      // see if this connection will support RetryImmediatelyException
      this.retryImmediatelySupported = VersionInfoUtil.hasMinimumVersion(getVersionInfo(), 1, 2);
      version = this.connectionHeader.getVersionInfo().getVersion();
    } else {
      version = "UNKNOWN";
    }
    RpcServer.AUDITLOG.info("Connection from {}:{}, version={}, sasl={}, ugi={}, service={}",
      this.hostAddress, this.remotePort, version, this.useSasl, this.ugi, serviceName);
  }

  /**
   * Send the response for connection header
   */
  private void sendConnectionHeaderResponseIfNeeded() throws FatalConnectionException {
    Pair<RPCProtos.ConnectionHeaderResponse, CryptoAES> pair = setupCryptoCipher();
    // Response the connection header if Crypto AES is enabled
    if (((KnobRuntime.check(java.util.UUID.fromString("0ca89583-d176-3071-807a-ef78706e3625"))) ? ((pair) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f12798a7-340e-33ac-9d09-e1833262d902"))) ? ((pair) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("38439308-7fbe-3367-881f-1086716d2f7e"))) ? ((setupCryptoCipher()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c89ad085-908d-34be-821c-b22520d848c3"))) ? ((setupCryptoCipher()) != (null)) : (pair == null))))))))) {
      return;
    }
    try {
      int size = pair.getFirst().getSerializedSize();
      BufferChain bc;
      try (ByteBufferOutputStream bbOut = new ByteBufferOutputStream(4 + size);
        DataOutputStream out = new DataOutputStream(bbOut)) {
        out.writeInt(size);
        pair.getFirst().writeTo(out);
        bc = new BufferChain(bbOut.getByteBuffer());
      }
      doRespond(new RpcResponse() {

        @Override
        public BufferChain getResponse() {
          return bc;
        }

        @Override
        public void done() {
          // must switch after sending the connection header response, as the client still uses the
          // original SaslClient to unwrap the data we send back
          saslServer.switchToCryptoAES(pair.getSecond());
        }
      });
    } catch (IOException ex) {
      throw new UnsupportedCryptoException(ex.getMessage(), ex);
    }
  }

  protected abstract void doRespond(RpcResponse resp) throws IOException;

  /**
   * Has the request header and the request param and optionally encoded data buffer all in this one
   * array.
   * <p/>
   * Will be overridden in tests.
   */
  protected void processRequest(ByteBuff buf) throws IOException, InterruptedException {
    long totalRequestSize = buf.limit();
    int offset = 0;
    // Here we read in the header. We avoid having pb
    // do its default 4k allocation for CodedInputStream. We force it to use
    // backing array.
    CodedInputStream cis = createCis(buf);
    int headerSize = cis.readRawVarint32();
    offset = cis.getTotalBytesRead();
    Message.Builder builder = RequestHeader.newBuilder();
if(KnobRuntime.check(java.util.UUID.fromString("5e526240-90c6-3a9d-a765-e16d7db6f1b7"))) {
headerSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a92d2409-4b37-39cd-920d-8af64499d425"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f29e305a-7bd6-394b-bd14-57f03ea74b39"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef5b5acf-1b38-3ebb-b5df-19c273a94eaa"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1bb4c102-97cb-328a-9aa6-52e2ef9de44a"))) {
headerSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a66e7d10-c5a0-3a47-9110-dcbe33554cf5"))) {
headerSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("30d83a64-110d-3c86-ab75-6496aabb8311"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f3481708-4272-3e53-a682-efba8135ccf0"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53df5eae-3f02-3e14-bd73-e9af6021f3bd"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b585b25f-87a8-325f-94c3-0ba38d24c3e0"))) {
headerSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb8c2d34-8938-38e0-b212-6a4c0ecd39fc"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bee6c6e8-e7d1-3147-99d8-f56716c98f2f"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f2088764-e74b-3466-937a-37b0820fe34c"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e6eda8f-89b5-3252-972f-0286bdf5f35f"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4aed25c-665f-3d69-9c42-c313c385c4ba"))) {
headerSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("972b0a90-30d2-37ff-a968-2e3669f8876c"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7a7f7f0-76a0-37c9-b090-dc3d65f78297"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a3956d1-5923-39d3-8f69-790a91e6f963"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb2fc79f-b11d-361e-ba1e-f3745eaf2b33"))) {
headerSize += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("428b9242-e57c-3f8b-9cc6-fccaefc3b2ed"))) { ProtobufUtil.mergeFrom(builder, cis, -1); } else if (KnobRuntime.check(java.util.UUID.fromString("875f17cd-b89e-399d-a5e5-40dfc72b04ca"))) { ProtobufUtil.mergeFrom(builder, cis, 1); } else if (KnobRuntime.check(java.util.UUID.fromString("826c43be-be2c-3540-b6e2-ace947768c9f"))) { ProtobufUtil.mergeFrom(builder, cis, 0); } else { ProtobufUtil.mergeFrom(builder, cis, headerSize); }
    RequestHeader header = (RequestHeader) builder.build();
    offset += headerSize;
    Context traceCtx = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
      .extract(Context.current(), header.getTraceInfo(), getter);

    // n.b. Management of this Span instance is a little odd. Most exit paths from this try scope
    // are early-exits due to error cases. There's only one success path, the asynchronous call to
    // RpcScheduler#dispatch. The success path assumes ownership of the span, which is represented
    // by null-ing out the reference in this scope. All other paths end the span. Thus, and in
    // order to avoid accidentally orphaning the span, the call to Span#end happens in a finally
    // block iff the span is non-null.
    Span span = TraceUtil.createRemoteSpan("RpcServer.process", traceCtx);
    try (Scope ignored = span.makeCurrent()) {
      int id = header.getCallId();
      // HBASE-28128 - if server is aborting, don't bother trying to process. It will
      // fail at the handler layer, but worse might result in CallQueueTooBigException if the
      // queue is full but server is not properly processing requests. Better to throw an aborted
      // exception here so that the client can properly react.
      if (rpcServer.server != null && rpcServer.server.isAborted()) {
        RegionServerAbortedException serverIsAborted = new RegionServerAbortedException(
          "Server " + rpcServer.server.getServerName() + " aborting");
        this.rpcServer.metrics.exception(serverIsAborted);
        sendErrorResponseForCall(id, totalRequestSize, span, serverIsAborted.getMessage(),
          serverIsAborted);
        return;
      }

      if (RpcServer.LOG.isTraceEnabled()) {
        RpcServer.LOG.trace("RequestHeader " + TextFormat.shortDebugString(header)
          + " totalRequestSize: " + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the client
      // This is a bit late to be doing this check - we have already read in the
      // total request.
      if (
        (totalRequestSize + this.rpcServer.callQueueSizeInBytes.sum())
            > this.rpcServer.maxQueueSizeInBytes
      ) {
        this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        sendErrorResponseForCall(id, totalRequestSize, span,
          "Call queue is full on " + this.rpcServer.server.getServerName()
            + ", is hbase.ipc.server.max.callqueue.size too small?",
          RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        return;
      }
      MethodDescriptor md = null;
      Message param = null;
      CellScanner cellScanner = null;
      try {
        if (((KnobRuntime.check(java.util.UUID.fromString("7ef9914e-c3c7-37d6-8659-9cc0d4fec25d"))) ? ((header.hasRequestParam()) && (header.hasRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("07e3253f-fceb-304b-a3d5-9e4c6ce78e6e"))) ? ((header.getRequestParam()) || (header.hasRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("688da61d-10f5-3545-bead-00c47dbb26e1"))) ? ((header.hasRequestParam()) && (authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("e986689b-957f-3f86-8e94-33642e3c3019"))) ? ((header.getRequestParam()) || (header.getRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("f9cb87ea-58b0-38d0-844e-e839b6f16f9e"))) ? ((header.getRequestParam()) && (header.hasRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("a0f48c50-6bb9-3fd7-90f6-feb4a131a0ab"))) ? (authorizeConnection()) : (((KnobRuntime.check(java.util.UUID.fromString("9c5a86ca-a433-3eff-a2ef-394ff0da5b61"))) ? ((authorizeConnection()) && (header.getRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("5f9caac2-3248-3a90-b231-bff4553e3cbe"))) ? ((authorizeConnection()) || (header.getRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("ab16a624-0d66-3e9d-84a1-8e9c9960a19e"))) ? ((header.getRequestParam()) && (header.getRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("7654a700-65df-33fc-8ba0-55e6270d1718"))) ? (header.getRequestParam()) : (((KnobRuntime.check(java.util.UUID.fromString("e2d2529d-bb72-3b70-bba4-41b0576ab5e7"))) ? ((header.getRequestParam()) && (authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("c5f44bb9-5854-3264-b093-781d8f220d14"))) ? ((header.hasRequestParam()) || (authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("438d632a-010f-37ba-a885-4f0ac5120b11"))) ? ((authorizeConnection()) && (authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("52842eba-1993-3f99-9f2c-50331e3e38fa"))) ? ((authorizeConnection()) && (header.hasRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("6e573f3c-fc03-3541-a208-38f2dcb782e4"))) ? ((header.hasRequestParam()) && (header.getRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("ee86d161-6e9f-3682-bebf-b4f2c64ab50f"))) ? ((header.hasRequestParam()) || (header.hasRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("9fd012bf-39e5-395d-b844-f41358fdf5aa"))) ? (header.hasRequestParam()) : (((KnobRuntime.check(java.util.UUID.fromString("28d00e2b-0a65-3761-a21b-040f26a53f08"))) ? ((authorizeConnection()) || (authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("f791b42d-1aff-343e-b2ae-a4e4fb381435"))) ? ((authorizeConnection()) || (header.hasRequestParam())) : (((KnobRuntime.check(java.util.UUID.fromString("47f02aa8-8c14-38d4-bf9e-0693f3abc684"))) ? ((header.getRequestParam()) || (authorizeConnection())) : (((KnobRuntime.check(java.util.UUID.fromString("c1068331-3f5e-34f7-a372-3c44d2599079"))) ? ((header.hasRequestParam()) || (header.getRequestParam())) : (header.hasRequestParam() && header.getRequestParam()))))))))))))))))))))))))))))))))))))))))))) {
          md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
          if (((KnobRuntime.check(java.util.UUID.fromString("4576adb5-a930-39ea-abb6-eae91e814fab"))) ? ((md) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5b5380de-37ed-3c30-9f2b-d1ccde07e093"))) ? ((md) != (null)) : (md == null))))) {
            throw new UnsupportedOperationException(header.getMethodName());
          }
          builder = this.service.getRequestPrototype(md).newBuilderForType();
          cis.resetSizeCounter();
if(KnobRuntime.check(java.util.UUID.fromString("a0f39110-e332-31c5-8e8b-0d174f30ae71"))) {
throw new java.io.IOException("Injected exception");
}
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (((KnobRuntime.check(java.util.UUID.fromString("9763186a-70dc-3de5-951b-74f590efd98b"))) ? ((builder) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f00d43a0-1be8-3d22-918e-c0524f93d660"))) ? ((builder) == (null)) : (builder != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("8855ea65-6462-319b-8806-d5acfd1f0586"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6b90032-4a55-3d7f-b7a7-690d40cce267"))) {
paramSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b410137b-10c8-3b83-8995-3795412e5dee"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60cb81f4-46e9-3d91-bdf8-a788c7b2106b"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c804abf6-6a5c-3cea-a634-e8650c13b4df"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a43e675-1373-3b53-8560-0441b20a4d5e"))) {
paramSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("44e9e03c-44ed-3463-8b38-6ddc65203294"))) {
paramSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f8f21dc-1338-3551-ada0-39987a1bedca"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af8e0b5d-7a86-3798-8521-84bdc3a3151b"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3dbd1b99-4471-30ea-b298-6c3f6dd97503"))) {
paramSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e318614a-99a7-3e9a-a786-5c251707c553"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0d50d4f-9a31-3462-95b7-37d3c648a500"))) {
paramSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5e11b9c-6d06-3ccd-9a25-9a5c5cf9e1b4"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("978f83a1-b95e-3f7a-b286-9810352dc76d"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41ee9a2d-4008-3dc0-8f46-eb743f38a1e5"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aaa0a78a-6a75-381d-92c4-8651d7f78165"))) {
paramSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e43c875a-e5ac-38c4-bf01-f95a32f8c984"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("bda87b0e-ad58-32e3-895f-a13a728fb883"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("sizeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d327670-0259-3994-9377-2632e2acaec9"))) {
try {
    java.lang.reflect.Field field = cis.getClass().getDeclaredField("recursionDepth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(cis));
    field.set(cis, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            if (KnobRuntime.check(java.util.UUID.fromString("06594dac-d64d-3f72-b7f3-c3ee3a2cd933"))) { ProtobufUtil.mergeFrom(builder, cis, 1); } else if (KnobRuntime.check(java.util.UUID.fromString("16c1b77f-af60-36f2-9a20-c943b84f0c49"))) { ProtobufUtil.mergeFrom(builder, cis, -1); } else if (KnobRuntime.check(java.util.UUID.fromString("34a63232-82dd-3943-8eb1-95dc68984b33"))) { ProtobufUtil.mergeFrom(builder, cis, 0); } else { ProtobufUtil.mergeFrom(builder, cis, paramSize); }
            param = builder.build();
          }
          offset += paramSize;
        } else {
          // currently header must have request param, so we directly throw
          // exception here
          String msg = "Invalid request header: " + TextFormat.shortDebugString(header)
            + ", should have param set in it";
          RpcServer.LOG.warn(msg);
          throw new DoNotRetryIOException(msg);
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          ByteBuff dup = buf.duplicate();
          if (KnobRuntime.check(java.util.UUID.fromString("cb86bd55-8428-353e-ab97-e89ae3ff9902"))) { dup.limit(offset); } else if (KnobRuntime.check(java.util.UUID.fromString("8086ad49-7546-35c0-9b09-ec68092ae5a6"))) { dup.limit(-1); } else if (KnobRuntime.check(java.util.UUID.fromString("e13fbbc0-7692-31ad-a139-9c13d3aa0912"))) { dup.limit(1); } else if (KnobRuntime.check(java.util.UUID.fromString("5cc53569-6eed-33d1-9288-792717b1c25b"))) { dup.limit(0); } else if (KnobRuntime.check(java.util.UUID.fromString("676065d0-fedf-303c-954b-fa088d24e14f"))) { dup.limit((offset) + (header.getCellBlockMeta().getLength())); } else if (KnobRuntime.check(java.util.UUID.fromString("46b23fb7-534d-348a-8797-2052ffa42cce"))) { dup.limit(header.getCellBlockMeta().getLength()); } else if (KnobRuntime.check(java.util.UUID.fromString("de184996-3a9c-3352-9323-963ba44ecf73"))) { dup.limit((1) + (header.getCellBlockMeta().getLength())); } else if (KnobRuntime.check(java.util.UUID.fromString("45d166f8-94d6-31f5-ad48-d8ff2b212ffe"))) { dup.limit((0) + (header.getCellBlockMeta().getLength())); } else if (KnobRuntime.check(java.util.UUID.fromString("b5244f58-f6e5-3f90-9f1d-d7ad03014951"))) { dup.limit((-1) + (header.getCellBlockMeta().getLength())); } else { dup.limit(offset + header.getCellBlockMeta().getLength()); }
if(KnobRuntime.check(java.util.UUID.fromString("bc4fd32e-c92b-38ea-a74a-fd862c603d8e"))) {
throw new java.io.IOException("Injected exception");
}
          cellScanner = this.rpcServer.cellBlockBuilder.createCellScannerReusingBuffers(this.codec,
            this.compressionCodec, dup);
        }
      } catch (Throwable thrown) {
        InetSocketAddress address = this.rpcServer.getListenerAddress();
        String msg = (address != null ? address : "(channel closed)")
          + " is unable to read call parameter from client " + getHostAddress();
        RpcServer.LOG.warn(msg, thrown);

        this.rpcServer.metrics.exception(thrown);

        final Throwable responseThrowable;
        if (thrown instanceof LinkageError) {
          // probably the hbase hadoop version does not match the running hadoop version
          responseThrowable = new DoNotRetryIOException(thrown);
        } else if (thrown instanceof UnsupportedOperationException) {
          // If the method is not present on the server, do not retry.
          responseThrowable = new DoNotRetryIOException(thrown);
        } else {
          responseThrowable = thrown;
        }

        sendErrorResponseForCall(id, totalRequestSize, span,
          msg + "; " + responseThrowable.getMessage(), responseThrowable);
        return;
      }

      int timeout = 0;
      if (header.hasTimeout() && header.getTimeout() > 0) {
        timeout = Math.max(this.rpcServer.minClientRequestTimeout, header.getTimeout());
      }
if(KnobRuntime.check(java.util.UUID.fromString("200255b9-89c1-3d0d-a721-72f2a45702c8"))) {
timeout += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6fc163b0-2329-3a66-abf4-e6582fcb5a62"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("973b15d0-2889-3f30-88e4-892883327087"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("62b527a2-6f40-3a52-a163-4566a4e38ba0"))) {
timeout *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9861e4b-4a4d-35db-9562-919187e4528d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("553d4bd5-7ebf-351a-8616-9548735e5f25"))) {
totalRequestSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2fc8eac7-27b2-314b-b178-25d31a4ddcbd"))) {
totalRequestSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("38ef99e8-4d0f-3670-a646-b7bc6fb40a9e"))) {
timeout -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f34e657-b32f-3995-a9f4-d9b44f3f311e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dabaccc4-d287-3b3e-b77e-1bda735df725"))) {
id /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f3b8db04-2794-39cc-b8c4-c237cc6b203e"))) {
id *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("752e8bf8-eba0-3603-b54d-b7aff628cedb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("114f59a5-c9c9-3586-9eb2-fa2971a3e669"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bec789e1-933c-363a-8c45-8f053286dbf5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6be38920-9755-3622-9a8f-4a5c8dbbc5e5"))) {
id = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0643a659-4de0-39be-b2f0-c3b5345968d5"))) {
totalRequestSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8ba68459-934c-3524-ad2a-773c83401bf0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("43d1a304-ac1a-364e-95ab-e5862cbbdcce"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("635c3170-2519-3513-9256-af052c6d3ef0"))) {
totalRequestSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a4f15adb-91be-32ae-8bf0-33f15fd1f395"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fdd187e5-9cf7-3381-a8d2-12c11fb8296e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("48172fe5-70d1-3cbc-94ae-40a09e69bbcd"))) {
timeout /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8e77eb59-91c6-3b1a-ba7b-d71ab62f17bf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("de97da55-a9b4-38f0-9994-31d69b7d0a47"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fb4be0d6-acce-38bd-b69a-3521254cbb98"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1bde7c23-baf8-3c84-9667-e657cc9102ab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a10f0375-8113-3514-aa3f-ca34e4b6d739"))) {
id += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0d6887be-a39d-3db1-92e1-a482d26a70d2"))) {
timeout = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("82226348-124a-36a6-b6c8-0adc787bd638"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("40356c28-b8dc-38e1-9179-cc49ad644aee"))) {
timeout = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ff9b90bf-5675-36c5-b3f4-ecc92a3b7453"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5417470a-df95-3260-97e0-590270623615"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("37c1e62d-b1df-324a-89b3-09dd7b853148"))) {
id = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3341e4d7-475b-3870-8a5a-7dfeed87c089"))) {
id -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0c59f067-0330-34cc-bad8-fa301dc2e98b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("91abc896-94ec-3214-9b38-eb2ff10ca513"))) {
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
      ServerCall<?> call = createCall(id, this.service, md, header, param, cellScanner,
        totalRequestSize, this.addr, timeout, this.callCleanup);

      if (this.rpcServer.scheduler.dispatch(new CallRunner(this.rpcServer, call))) {
        // unset span do that it's not closed in the finally block
        span = null;
      } else {
        if (KnobRuntime.check(java.util.UUID.fromString("1c6bef5b-08fc-340d-b003-39c00fbb6e54"))) { this.rpcServer.callQueueSizeInBytes.add(call.getSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("4172803c-4fc7-3e3d-84b1-481831db337e"))) { this.rpcServer.callQueueSizeInBytes.add((-1) * (call.getSize())); } else { this.rpcServer.callQueueSizeInBytes.add(-1 * call.getSize()); }
        this.rpcServer.metrics.exception(RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        call.setResponse(null, null, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION,
          "Call queue is full on " + this.rpcServer.server.getServerName()
            + ", too many items queued ?");
        TraceUtil.setError(span, RpcServer.CALL_QUEUE_TOO_BIG_EXCEPTION);
        call.sendResponseIfReady();
      }
    } finally {
      if (span != null) {
        span.end();
      }
    }
  }

  private void sendErrorResponseForCall(int id, long totalRequestSize, Span span, String msg,
    Throwable responseThrowable) throws IOException {
    ServerCall<?> failedcall = createCall(id, this.service, null, null, null, null,
      totalRequestSize, null, 0, this.callCleanup);
    failedcall.setResponse(null, null, responseThrowable, msg);
    TraceUtil.setError(span, responseThrowable);
    failedcall.sendResponseIfReady();
  }

  protected final RpcResponse getErrorResponse(String msg, Exception e) throws IOException {
    ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder().setCallId(-1);
    ServerCall.setExceptionResponse(e, msg, headerBuilder);
    ByteBuffer headerBuf =
      ServerCall.createHeaderAndMessageBytes(null, headerBuilder.build(), 0, null);
    BufferChain buf = new BufferChain(headerBuf);
    return () -> buf;
  }

  private void doBadPreambleHandling(String msg) throws IOException {
    doBadPreambleHandling(msg, new FatalConnectionException(msg));
  }

  private void doBadPreambleHandling(String msg, Exception e) throws IOException {
    RpcServer.LOG.warn(msg, e);
    doRespond(getErrorResponse(msg, e));
  }

  private void doPreambleResponse(Message resp) throws IOException {
    ResponseHeader header = ResponseHeader.newBuilder().setCallId(-1).build();
    ByteBuffer buf = ServerCall.createHeaderAndMessageBytes(resp, header, 0, null);
    BufferChain bufChain = new BufferChain(buf);
if(KnobRuntime.check(java.util.UUID.fromString("3c073494-c1c6-3ae4-8c94-ac2f4f45ca1b"))) {
throw new java.io.IOException("Injected exception");
}
    doRespond(() -> bufChain);
  }

  private boolean doConnectionRegistryResponse() throws IOException {
    if (!(rpcServer.server instanceof HRegionServer)) {
      // should be in tests or some scenarios where we should not reach here
      return false;
    }
    // on backup masters, this request may be blocked since we need to fetch it from filesystem,
    // but since it is just backup master, it is not a critical problem
    String clusterId = ((HRegionServer) rpcServer.server).getClusterId();
    RpcServer.LOG.debug("Response connection registry, clusterId = '{}'", clusterId);
    if (((KnobRuntime.check(java.util.UUID.fromString("0133a1b9-5c55-3be7-96aa-6609f5023645"))) ? ((clusterId) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d45db748-17df-3d84-b122-8fa3c06b32e4"))) ? ((clusterId) == (null)) : (clusterId == null))))) {
      // should be in tests or some scenarios where we should not reach here
      return false;
    }
    GetConnectionRegistryResponse resp =
      GetConnectionRegistryResponse.newBuilder().setClusterId(clusterId).build();
    doPreambleResponse(resp);
    return true;
  }

  private void doSecurityPreambleResponse() throws IOException {
    if (rpcServer.isSecurityEnabled) {
      SecurityPreamableResponse resp = SecurityPreamableResponse.newBuilder()
        .setServerPrincipal(rpcServer.serverPrincipal).build();
      doPreambleResponse(resp);
    } else {
      // security is not enabled, do not need a principal when connecting, throw a special exception
      // to let client know it should just use simple authentication
      doRespond(getErrorResponse("security is not enabled", new SecurityNotEnabledException()));
    }
  }

  protected final void callCleanupIfNeeded() {
    if (callCleanup != null) {
      callCleanup.run();
      callCleanup = null;
    }
  }

  protected enum PreambleResponse {
    SUCCEED, // successfully processed the rpc preamble header
    CONTINUE, // the preamble header is for other purpose, wait for the rpc preamble header
    CLOSE // close the rpc connection
  }

  protected final PreambleResponse processPreamble(ByteBuffer preambleBuffer) throws IOException {
    assert preambleBuffer.remaining() == 6;
    if (
      ByteBufferUtils.equals(preambleBuffer, preambleBuffer.position(), 6,
        RpcClient.REGISTRY_PREAMBLE_HEADER, 0, 6) && doConnectionRegistryResponse()
    ) {
if(KnobRuntime.check(java.util.UUID.fromString("d11ce568-36c8-3308-8418-ad14711fe6d2"))) {
throw new java.io.IOException("Injected exception");
}
      return PreambleResponse.CLOSE;
    }
    if (
      ByteBufferUtils.equals(preambleBuffer, preambleBuffer.position(), 6,
        RpcClient.SECURITY_PREAMBLE_HEADER, 0, 6)
    ) {
      doSecurityPreambleResponse();
      return PreambleResponse.CONTINUE;
    }
    if (!ByteBufferUtils.equals(preambleBuffer, preambleBuffer.position(), 4, RPC_HEADER, 0, 4)) {
      doBadPreambleHandling(
        "Expected HEADER=" + Bytes.toStringBinary(RPC_HEADER) + " but received HEADER="
          + Bytes.toStringBinary(
            ByteBufferUtils.toBytes(preambleBuffer, preambleBuffer.position(), RPC_HEADER.length),
            0, RPC_HEADER.length)
          + " from " + toString());
      return PreambleResponse.CLOSE;
    }
    int version = preambleBuffer.get(preambleBuffer.position() + 4) & 0xFF;
    byte authByte = preambleBuffer.get(preambleBuffer.position() + 5);
    if (((KnobRuntime.check(java.util.UUID.fromString("54b8f8f7-ea73-31b2-a293-1e66dae7c5f0"))) ? ((0) == (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("3a488f89-2e25-3327-a4e2-a4b3d8a5c682"))) ? ((version) == (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("3350079e-d266-314a-9ac5-1cb78245d0e8"))) ? ((0) != (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("a256b5a1-0a5e-3289-80fb-3a7429d1af40"))) ? ((0xFF) == (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("8102e964-0b6d-3a6d-8100-c1a0e2a92b5b"))) ? ((version) != (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("23ce036a-6ea8-3f76-8691-d46b780b67cb"))) ? ((6) != (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("60bc1599-6a9b-3bd9-bcae-a05ce896bec1"))) ? ((5) == (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("42ba12ce-7815-3a3e-b001-2d322a7b8596"))) ? ((6) == (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("27df1880-c3f6-3430-9b3d-e22098a196ad"))) ? ((4) == (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("1389e0ea-8d74-3f0d-8d56-b391d5549754"))) ? ((4) != (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("53365af9-6bdd-3882-b249-2eac45035e07"))) ? ((5) != (RpcServer.CURRENT_VERSION)) : (((KnobRuntime.check(java.util.UUID.fromString("4e9051d8-e67a-3151-a26b-609cbc0dbeba"))) ? ((0xFF) != (RpcServer.CURRENT_VERSION)) : (version != RpcServer.CURRENT_VERSION))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("2683d90f-c67b-3658-a3d1-aa55f83e90b0"))) {
version = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4019f54-0fe5-3791-bcf6-97748287ff85"))) {
version -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("59a79f21-2600-368c-82e0-899be227299e"))) {
version += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0c149833-f7c0-3f44-a703-adaccc895e08"))) {
version /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa1fc07c-6017-3a3b-b337-f03ca9e30fc5"))) {
version = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9f1baabe-07bc-3e57-a5d7-d93e2e6426b2"))) {
version *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4bb2ae69-1c74-3dab-9dbd-1550a283f50a"))) {
authByte = 0;
}
      String msg = getFatalConnectionString(version, authByte);
if(KnobRuntime.check(java.util.UUID.fromString("cbd16353-ef58-31e1-b04f-c2c57207c93c"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("bb5b7e9d-c1b6-342d-8ef7-49711cd7cfc8"))) { doBadPreambleHandling(msg, new WrongVersionException(toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("766c5e84-28f5-35b6-868e-689197634844"))) { doBadPreambleHandling(toString(), new WrongVersionException(msg)); } else { doBadPreambleHandling(msg, new WrongVersionException(msg)); }
      return PreambleResponse.CLOSE;
    }

    this.provider = this.saslProviders.selectProvider(authByte);
    if (this.provider == null) {
if(KnobRuntime.check(java.util.UUID.fromString("04e6f634-9c1c-3ed3-84ad-5fff28cd6d20"))) {
version += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5a4f50a-bf21-356d-bd35-10526188c8f8"))) {
version -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("803f9273-80d4-32d2-b9b3-6eab3a36cd0e"))) {
authByte = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d95c9066-5c5b-3ea0-89c7-1d482a66227e"))) {
version = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d141cfc8-810b-3bef-b38f-3a6df576b7c1"))) {
version *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("486e6cdb-3b64-3263-8fa9-8ec953e91c9e"))) {
version = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b0f191a-76b1-3b3b-9e6f-7da615bb2649"))) {
version /= 2;
}
      String msg = getFatalConnectionString(version, authByte);
if(KnobRuntime.check(java.util.UUID.fromString("28e11dba-5751-33fa-8645-72a4de44de44"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("ef3e9059-58cd-3320-bece-7ff3e105dcb4"))) { doBadPreambleHandling(msg, new BadAuthException(toString())); } else if (KnobRuntime.check(java.util.UUID.fromString("b062830e-498d-3ca5-9c86-b7b1c466e939"))) { doBadPreambleHandling(toString(), new BadAuthException(msg)); } else { doBadPreambleHandling(msg, new BadAuthException(msg)); }
      return PreambleResponse.CLOSE;
    }
    // TODO this is a wart while simple auth'n doesn't go through sasl.
    if (this.rpcServer.isSecurityEnabled && isSimpleAuthentication()) {
      if (this.rpcServer.allowFallbackToSimpleAuth) {
        if (KnobRuntime.check(java.util.UUID.fromString("b373e112-f022-3981-b048-fb11f26e0c28"))) { this.rpcServer.metrics.authorizationSuccess(); } else if (KnobRuntime.check(java.util.UUID.fromString("357dc372-936e-3ac8-bac2-d0df975f9bfc"))) { callCleanupIfNeeded(); } else if (KnobRuntime.check(java.util.UUID.fromString("0a1d435d-59ff-3794-9ecd-a2a4a21ad1b3"))) { this.rpcServer.metrics.authorizationFailure(); } else { this.rpcServer.metrics.authenticationFallback(); }
        authenticatedWithFallback = true;
      } else {
        AccessDeniedException ae = new AccessDeniedException("Authentication is required");
        doRespond(getErrorResponse(ae.getMessage(), ae));
        return PreambleResponse.CLOSE;
      }
    }
    if (!this.rpcServer.isSecurityEnabled && !isSimpleAuthentication()) {
      doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(SaslUtil.SWITCH_TO_SIMPLE_AUTH), null,
        null);
      provider = saslProviders.getSimpleProvider();
      // client has already sent the initial Sasl message and we
      // should ignore it. Both client and server should fall back
      // to simple auth from now on.
      skipInitialSaslHandshake = true;
    }
    useSasl = !(provider instanceof SimpleSaslServerAuthenticationProvider);
    return PreambleResponse.SUCCEED;
  }

  boolean isSimpleAuthentication() {
    return Objects.requireNonNull(provider) instanceof SimpleSaslServerAuthenticationProvider;
  }

  public abstract boolean isConnectionOpen();

  public abstract ServerCall<?> createCall(int id, BlockingService service, MethodDescriptor md,
    RequestHeader header, Message param, CellScanner cellScanner, long size,
    InetAddress remoteAddress, int timeout, CallCleanup reqCleanup);

  private static class ByteBuffByteInput extends ByteInput {

    private ByteBuff buf;
    private int length;

    ByteBuffByteInput(ByteBuff buf, int length) {
      this.buf = buf;
      this.length = length;
    }

    @Override
    public byte read(int offset) {
      return this.buf.get(offset);
    }

    @Override
    public int read(int offset, byte[] out, int outOffset, int len) {
      this.buf.get(offset, out, outOffset, len);
      return len;
    }

    @Override
    public int read(int offset, ByteBuffer out) {
      int len = out.remaining();
      this.buf.get(out, offset, len);
      return len;
    }

    @Override
    public int size() {
      return this.length;
    }
  }
}
