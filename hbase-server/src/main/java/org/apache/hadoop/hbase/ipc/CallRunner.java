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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.server.trace.IpcServerSpanBuilder;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

/**
 * The request processing logic, which is usually executed in thread pools provided by an
 * {@link RpcScheduler}. Call {@link #run()} to actually execute the contained RpcServer.Call
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class CallRunner {

  private static final CallDroppedException CALL_DROPPED_EXCEPTION = new CallDroppedException();

  private RpcCall call;
  private RpcServerInterface rpcServer;
  private MonitoredRPCHandler status;
  private final Span span;
  private volatile boolean successful;

  /**
   * On construction, adds the size of this call to the running count of outstanding call sizes.
   * Presumption is that we are put on a queue while we wait on an executor to run us. During this
   * time we occupy heap.
   */
  // The constructor is shutdown so only RpcServer in this class can make one of these.
  CallRunner(final RpcServerInterface rpcServer, final RpcCall call) {
    this.call = call;
    this.rpcServer = rpcServer;
    this.span = Span.current();
    // Add size of the call to queue size.
    if (((KnobRuntime.check(java.util.UUID.fromString("8b91b824-93b1-38d5-856b-0ac19b6b1969"))) ? ((call != null) || ((rpcServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e0666e94-d849-32e9-916a-e4bba08f372e"))) ? (((call) == (null)) && (rpcServer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("9aece0f1-3eb9-3869-9896-77d55a7938dc"))) ? ((rpcServer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("01939d21-e726-3524-ac63-79f849080aa2"))) ? ((rpcServer) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cd31bffb-9fad-335b-a39a-64f5f8f2d19f"))) ? ((call != null) && (rpcServer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("fca2578d-4139-3b8d-a9c0-85e7f0d79454"))) ? (rpcServer != null) : (((KnobRuntime.check(java.util.UUID.fromString("87c952ad-a7b3-348f-bcd6-d8747f8c4c76"))) ? (((call) == (null)) || ((rpcServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1dbe6095-9eaa-3a75-b285-ed0510fb86b6"))) ? (((call) == (null)) && ((rpcServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("630979f3-c332-33b2-8d44-04a16cabac0e"))) ? (((call) != (null)) || ((rpcServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c4258b46-01fb-30aa-8d0e-afb15e86b5b8"))) ? (((call) != (null)) && ((rpcServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6388b0a5-3856-3c43-90d7-0e2f9f6a1a48"))) ? (call != null) : (((KnobRuntime.check(java.util.UUID.fromString("696b6b2d-3689-34b5-a9b0-bdf5f176af5f"))) ? ((call != null) || (rpcServer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("8c83875a-d4f9-3d0a-b5f1-69a2173e218a"))) ? (((call) != (null)) && ((rpcServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1e9e5a0d-0884-3588-bfcb-6fe40cf3cebd"))) ? ((call) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ebdbe035-0b79-37d0-b802-1e3f0f6365ac"))) ? ((call) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2fdd5438-c58a-3f2f-9362-89d264b57b90"))) ? (((call) == (null)) && ((rpcServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("f9573f46-c510-396d-8954-09152fa6d1eb"))) ? (((call) != (null)) || (rpcServer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("09fb561e-cf87-3531-95de-cdcd1cb39d4a"))) ? ((call != null) || ((rpcServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("34816a00-419d-3515-9f65-dfa78e629aaf"))) ? ((call != null) && ((rpcServer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("202ed7ae-18c3-32b1-9772-5eec4586318f"))) ? (((call) != (null)) && (rpcServer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("6d19e9e7-668c-33f1-ab09-a8211f94893b"))) ? (((call) == (null)) || (rpcServer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("cc38dc9a-4e35-344b-8b49-85639793ff61"))) ? (((call) != (null)) || ((rpcServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1bdbe3c9-dfff-3604-a1e6-5aa9ae65c993"))) ? ((call != null) && ((rpcServer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("cf72d2a3-5d8f-3a48-a439-8aa214ce8211"))) ? (((call) == (null)) || ((rpcServer) != (null))) : (call != null && rpcServer != null))))))))))))))))))))))))))))))))))))))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("f9276600-f80a-3409-b9ad-f8598dd643da"))) { this.rpcServer.addCallSize(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("58963aa9-dd35-35e2-bba9-d50f0ae0a418"))) { this.rpcServer.addCallSize(call.disconnectSince()); } else { this.rpcServer.addCallSize(call.getSize()); }
    }
  }

  public RpcCall getRpcCall() {
if(KnobRuntime.check(java.util.UUID.fromString("43a79348-47fa-3a81-bbdd-1bc7a6b39b67"))) {
return null;
}
    return call;
  }

  public void setStatus(MonitoredRPCHandler status) {
if(KnobRuntime.check(java.util.UUID.fromString("78cb2a21-4be0-3195-b4a8-607ce57263e4"))) {
return;
}
    this.status = status;
  }

  /**
   * Cleanup after ourselves... let go of references.
   */
  private void cleanup() {
if(KnobRuntime.check(java.util.UUID.fromString("bfaf30cf-d829-3671-b2ab-bb6dea27b023"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("b3345258-3583-30a5-98ea-12d189f0ed76"))) { cleanup(); } else { this.call.cleanup(); }
    this.call = null;
    this.rpcServer = null;
  }

  public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("7fe0fc11-b0a6-31ca-81fc-14f83c7863f7"))) {
return;
}
    try (Scope ignored = span.makeCurrent()) {
      if (((KnobRuntime.check(java.util.UUID.fromString("35be285e-117c-3cf7-8944-e712dce5c0ee"))) ? ((call.getSize()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d9e60c3f-5a13-38a9-a2e1-f6bf7acfa187"))) ? ((EnvironmentEdgeManager.currentTime()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9f40844a-cc51-334d-89a8-eba2c680d2d9"))) ? ((call.getSize()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("64a35250-e173-31d9-880d-62f9e303717f"))) ? ((call.getSize()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b16729ed-d927-3f98-a314-aeb86bddf2f5"))) ? ((call.disconnectSince()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e952b539-78f5-3d48-af5b-325d468e318a"))) ? ((call.disconnectSince()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("441a5fa5-d7a3-39eb-9080-4d1118c13b44"))) ? ((EnvironmentEdgeManager.currentTime()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("180e3ee6-7ddf-3104-b8e9-6165b805f787"))) ? ((call.disconnectSince()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("55b82882-41c1-3f16-bebd-e25fc2ea8f3a"))) ? ((call.getSize()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1d4f24d3-cc1d-3c01-b415-91312a624143"))) ? ((call.disconnectSince()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6c9a39aa-1762-3578-b7aa-7ccbf4c1a554"))) ? ((call.getSize()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("75b3b9a7-a896-3806-8e11-6868e4aa69ea"))) ? ((call.getSize()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("17302422-e919-364b-a1ea-de5a0f7e924c"))) ? ((EnvironmentEdgeManager.currentTime()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d7f4640f-1588-3a5a-9c48-ed629fc4c8e3"))) ? ((EnvironmentEdgeManager.currentTime()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("72c50bb9-3d05-33b6-96ac-67519caf39af"))) ? ((call.disconnectSince()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("15698378-7437-33e0-9322-d3943a2c1fb4"))) ? ((EnvironmentEdgeManager.currentTime()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("491a19e1-e959-387f-8e61-94c7fe3c4f69"))) ? ((call.disconnectSince()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d8944921-04b2-37e4-85f4-bc5c6803dcb0"))) ? ((EnvironmentEdgeManager.currentTime()) == (0)) : (call.disconnectSince() >= 0))))))))))))))))))))))))))))))))))))) {
        RpcServer.LOG.debug("{}: skipped {}", Thread.currentThread().getName(), call);
        span.addEvent("Client disconnect detected");
        span.setStatus(StatusCode.OK);
        return;
      }
      call.setStartTime(EnvironmentEdgeManager.currentTime());
      if (((KnobRuntime.check(java.util.UUID.fromString("8248cfe5-6e74-32e8-b753-e7487945b05b"))) ? ((call.getSize()) > (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("42cdf9ed-fdea-379b-b420-32166c18096d"))) ? ((EnvironmentEdgeManager.currentTime()) <= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("6f3a1c00-3744-3fa9-b8ae-8c75222f998c"))) ? ((call.disconnectSince()) > (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("c060354b-2739-368a-bbde-ad7aacc92a3b"))) ? ((EnvironmentEdgeManager.currentTime()) >= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("6880aac4-e5b5-319e-9746-2e8387a1d03c"))) ? ((call.disconnectSince()) != (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("85c7b7ca-7971-347e-bc0c-02493b5ab7b0"))) ? ((call.getSize()) >= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("5090f796-def7-34e8-b128-d023f886bcf3"))) ? ((call.disconnectSince()) == (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("f06db7ee-a7c4-32b8-ad9b-fc334378abe3"))) ? ((EnvironmentEdgeManager.currentTime()) < (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("a9c37dbe-5885-39fa-be5e-f129b057580e"))) ? ((call.getStartTime()) < (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("ac1c4f57-b228-3b6a-9875-302cb24370eb"))) ? ((call.disconnectSince()) <= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("048dc9d2-ee7f-3a08-beb1-3b517783c7f5"))) ? ((call.disconnectSince()) >= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("5ecaaa86-db91-3367-9109-e37be531555e"))) ? ((call.disconnectSince()) < (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("727b4bae-e134-3d92-ab18-6961f3ad2a24"))) ? ((call.getStartTime()) < (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("c201680f-4e82-395e-b0af-9bd026223be7"))) ? ((call.getSize()) <= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("2b16deb5-a5f1-35dd-a3f2-8259ca003db1"))) ? ((call.getStartTime()) > (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("ea2ca589-ac26-3586-aeac-48917d413794"))) ? ((EnvironmentEdgeManager.currentTime()) == (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("23e65d34-b7cb-3c81-b9f5-9878639725d5"))) ? ((call.disconnectSince()) != (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("a83e8237-059a-3763-b989-2876d5f9dd71"))) ? ((call.getSize()) > (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("61ea6d7e-b606-3e2f-bee3-ae445b932acd"))) ? ((call.disconnectSince()) < (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("6620512d-9545-3d9b-ae79-83d50ab3e1ae"))) ? ((call.getStartTime()) <= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("b5022efa-9d4c-30e0-b5f0-7e2d5db9386f"))) ? ((call.getSize()) == (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("ff08907c-7860-3de6-853d-9ff6ae7a263e"))) ? ((EnvironmentEdgeManager.currentTime()) != (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("350be0a4-7f49-323e-be05-312a24f89cd1"))) ? ((call.getSize()) == (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("660bc20e-55e2-33f5-a60f-95e40b22195d"))) ? ((call.getSize()) >= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("cbd32343-cb8f-39df-aeb2-80ed3b48f5fa"))) ? ((call.getStartTime()) < (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("ee76ca77-807c-3984-b2d1-8c1c7a4d9a7f"))) ? ((EnvironmentEdgeManager.currentTime()) >= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("2dd7b58f-b874-3662-a127-7288a754eace"))) ? ((EnvironmentEdgeManager.currentTime()) == (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("a9c29ade-3ecb-326b-b134-c0ab77461cb4"))) ? ((call.getSize()) != (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("1c8d1698-0cef-3461-b09d-a5f4abf50084"))) ? ((call.getStartTime()) != (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("89ef48ec-0b8b-39d5-be8f-cd6acef67df7"))) ? ((EnvironmentEdgeManager.currentTime()) > (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("b7088b88-bdc2-36cb-97cf-d0d9f72a6c61"))) ? ((call.disconnectSince()) > (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("d065e66d-9724-30e4-a61f-7789c8b43d3d"))) ? ((call.getSize()) == (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("a2e4aab2-74a9-3272-b004-df5e805a62fe"))) ? ((call.getStartTime()) != (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("90e3e41f-d03c-3909-95be-8b91b8ad1ab9"))) ? ((call.disconnectSince()) > (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("f134b29c-6518-31a0-919b-453b8658b29b"))) ? ((EnvironmentEdgeManager.currentTime()) == (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("ebdab877-ea15-36e2-a13e-ec5cdb0c115c"))) ? ((call.getSize()) != (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("75293eeb-66a5-3b3d-a33b-72c618e08ea8"))) ? ((call.getSize()) != (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("c066c611-862e-325a-9d24-ac8eed788aca"))) ? ((call.getStartTime()) > (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("ef21b980-46db-3833-9324-8d6dff521e5a"))) ? ((call.getStartTime()) == (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("b34d9fb4-2659-38d4-aa64-67a91ffa84a3"))) ? ((call.getStartTime()) >= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("1fdd7e62-30fb-33a2-a8c0-680117914d4e"))) ? ((call.getStartTime()) == (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("a4b95e63-d778-30c8-ae6e-c61786fd26df"))) ? ((call.getStartTime()) > (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("0b2f1d2c-167b-36fd-8e26-1e3c70707f9e"))) ? ((call.getStartTime()) >= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("cb03f380-57d6-336d-aba8-220f28c85401"))) ? ((call.getStartTime()) <= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("1018fde0-ba43-37cb-b733-5cc817fb7cad"))) ? ((call.disconnectSince()) > (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("50b42c8d-c20b-3885-ba5a-16591c01ced4"))) ? ((call.getSize()) < (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("5b4ceb76-f6dd-3676-852c-ee0efc9cdabf"))) ? ((call.disconnectSince()) <= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("1514e2ca-9ff9-3529-92e7-d1737a1faba0"))) ? ((call.getSize()) <= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("38a45627-6b09-383e-b80e-a08dee7a708e"))) ? ((call.getSize()) > (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("2b491b0a-3158-3746-a4ba-e044483e7a72"))) ? ((call.disconnectSince()) == (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("a3767887-854c-3382-b54c-de2822ef6f1f"))) ? ((EnvironmentEdgeManager.currentTime()) != (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("13188fea-016b-36ff-84e4-3b7853eec3c3"))) ? ((call.disconnectSince()) >= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("49bf1e30-7852-3f7f-b571-42faf9c56857"))) ? ((call.getStartTime()) < (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("b1556f31-4c36-3e15-baaa-e9d90334c11e"))) ? ((EnvironmentEdgeManager.currentTime()) <= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("85e816ce-e177-3c1f-bb7a-bb880d9c2756"))) ? ((call.disconnectSince()) != (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("db41116a-a89c-3e9a-a50a-c1e5eba5abb4"))) ? ((call.disconnectSince()) <= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("7af6257f-3b23-31b1-9bf9-5c80cae6f7a4"))) ? ((call.disconnectSince()) <= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("23543923-693a-3824-95aa-ef07f4950929"))) ? ((EnvironmentEdgeManager.currentTime()) > (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("bc55cfdf-fefd-375c-a0fe-a1849b5452e8"))) ? ((call.getSize()) >= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("b3b9d80d-5425-399b-aff9-f7865f29c604"))) ? ((EnvironmentEdgeManager.currentTime()) > (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("aed2fdd9-c745-3e1b-adce-aadcf47a42fb"))) ? ((EnvironmentEdgeManager.currentTime()) <= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("13536ded-d08e-381f-9bb9-27f495db11f5"))) ? ((call.getSize()) != (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("eab59471-0d13-389a-afd4-2ab764d956bd"))) ? ((EnvironmentEdgeManager.currentTime()) < (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("dea6d63c-0123-3c99-a400-60d8654f54ca"))) ? ((EnvironmentEdgeManager.currentTime()) != (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("7aba7a12-4be4-30a8-9f64-49af50dabc3d"))) ? ((call.getSize()) >= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("d2658412-2c2b-390d-873e-84081eef7b70"))) ? ((EnvironmentEdgeManager.currentTime()) >= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("3f128663-b149-336d-b063-0bbb94981350"))) ? ((EnvironmentEdgeManager.currentTime()) == (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("a6f3bc59-b440-32c9-ba01-577fcde03b16"))) ? ((call.getSize()) == (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("202cb669-3595-3eb0-8723-daecaee23966"))) ? ((call.disconnectSince()) == (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("3a97760e-50a2-37f4-8379-71096b865e72"))) ? ((call.disconnectSince()) != (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("70834946-2e2d-375c-8b9a-3766a9b526ce"))) ? ((call.disconnectSince()) < (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("64d3fd03-2d75-3941-8247-c56735b0ba36"))) ? ((call.disconnectSince()) == (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("bdeed80b-6f95-3fc1-b973-1bdbb824c514"))) ? ((EnvironmentEdgeManager.currentTime()) <= (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("b91d580b-cbca-37fa-b6a8-8e2cacf69fde"))) ? ((call.disconnectSince()) >= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("18f789db-5f62-3d56-9d5b-6429961060ab"))) ? ((call.getSize()) < (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("6ae28837-3ae2-3ead-a78b-0a432871c371"))) ? ((call.getSize()) < (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("258d707b-959f-398b-9e60-a4376d2d2fd5"))) ? ((call.getSize()) <= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("5ee44f2e-464e-3f40-ac51-faaf185f2a51"))) ? ((EnvironmentEdgeManager.currentTime()) < (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("3d3c0fa3-0720-39ce-8ce4-08dca26541e7"))) ? ((call.disconnectSince()) >= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("0e55f321-0b6e-3fc2-975f-5dc01f9871e4"))) ? ((call.getSize()) > (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("bb06eea0-f4ec-3bcc-8302-493333b36b2d"))) ? ((call.getStartTime()) <= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("d196e199-528e-37e6-a9d3-3fb5ada4c5a1"))) ? ((EnvironmentEdgeManager.currentTime()) < (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("9040d621-4521-37d1-9b28-031a9526f46c"))) ? ((call.getStartTime()) != (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("0de85e05-2f90-3288-80d9-705c9579fe22"))) ? ((call.getSize()) < (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("b5d4541d-e566-3c3d-a0cd-ea176f5f44aa"))) ? ((call.getSize()) <= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("ab155b74-7184-3ab2-90b2-7efa7b37fb79"))) ? ((EnvironmentEdgeManager.currentTime()) > (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("e8e10724-1819-3dbf-a841-311d0f57fd03"))) ? ((call.getStartTime()) >= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("6040d6ef-529d-3721-bf1c-82ce59bedf4a"))) ? ((call.getStartTime()) > (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("e3e6c4f9-0926-3552-8262-20b007fcc98f"))) ? ((call.getStartTime()) == (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("85d90963-8a50-3475-9a93-4ddca3e34fa2"))) ? ((call.getStartTime()) >= (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("ce4f3a53-02dc-3f1a-a186-4db8c55c264e"))) ? ((call.getStartTime()) == (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("d065b606-671b-33d0-9bae-6e6977e6511a"))) ? ((call.getStartTime()) <= (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("0b6d5473-1326-38a6-9907-fe32feabedf8"))) ? ((EnvironmentEdgeManager.currentTime()) >= (call.getDeadline())) : (((KnobRuntime.check(java.util.UUID.fromString("6b471fe6-46bc-3c5f-9d0d-3720508466f1"))) ? ((EnvironmentEdgeManager.currentTime()) != (call.getSize())) : (((KnobRuntime.check(java.util.UUID.fromString("3c05f0fc-ead6-3620-a62c-a2d894c97010"))) ? ((call.disconnectSince()) < (call.disconnectSince())) : (((KnobRuntime.check(java.util.UUID.fromString("52053e1e-ffca-3133-a6f3-37a5e07f1bc0"))) ? ((call.getStartTime()) != (call.getDeadline())) : (call.getStartTime() > call.getDeadline()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        RpcServer.LOG.warn("Dropping timed out call: {}", call);
        this.rpcServer.getMetrics().callTimedOut();
        span.addEvent("Call deadline exceeded");
        span.setStatus(StatusCode.OK);
        return;
      }
      if (KnobRuntime.check(java.util.UUID.fromString("22d640a7-da2a-3a02-ade0-69a287a01d03"))) { this.status.markComplete("Setting up call"); } else { this.status.setStatus("Setting up call"); }
      this.status.setConnection(call.getRemoteAddress().getHostAddress(), call.getRemotePort());
      if (RpcServer.LOG.isTraceEnabled()) {
        RpcServer.LOG.trace("{} executing as {}", call.toShortString(),
          call.getRequestUser().map(User::getName).orElse("NULL principal"));
      }
      Throwable errorThrowable = null;
      String error = null;
      Pair<Message, CellScanner> resultPair = null;
      RpcServer.CurCall.set(call);
      final Span ipcServerSpan = new IpcServerSpanBuilder(call).build();
      try (Scope ignored1 = ipcServerSpan.makeCurrent()) {
        if (!this.rpcServer.isStarted()) {
          InetSocketAddress address = rpcServer.getListenerAddress();
          throw new ServerNotRunningYetException(
            "Server " + (address != null ? address : "(channel closed)") + " is not running yet");
        }
        // make the call
if(KnobRuntime.check(java.util.UUID.fromString("1cc13058-fb16-3c82-90a5-87c115a425a3"))) {
throw new java.io.IOException("Injected exception");
}
        resultPair = this.rpcServer.call(call, this.status);
      } catch (TimeoutIOException e) {
        RpcServer.LOG.warn("Can not complete this request in time, drop it: {}", call);
        TraceUtil.setError(ipcServerSpan, e);
        return;
      } catch (Throwable e) {
        TraceUtil.setError(ipcServerSpan, e);
        if (e instanceof ServerNotRunningYetException) {
          // If ServerNotRunningYetException, don't spew stack trace.
          if (RpcServer.LOG.isTraceEnabled()) {
            RpcServer.LOG.trace(call.toShortString(), e);
          }
        } else {
          // Don't dump full exception.. just String version
          if (KnobRuntime.check(java.util.UUID.fromString("8c3e1a49-835a-3e98-8e9e-8566be4d4ff6"))) { RpcServer.LOG.trace("{}, exception={}", call.toShortString(), e); } else { RpcServer.LOG.debug("{}, exception={}", call.toShortString(), e); }
        }
        errorThrowable = e;
        error = StringUtils.stringifyException(e);
        if (e instanceof Error) {
          throw (Error) e;
        }
      } finally {
        RpcServer.CurCall.set(null);
        if (((KnobRuntime.check(java.util.UUID.fromString("39c4467b-b8fa-3eec-973f-7272b330c2f8"))) ? ((resultPair) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bfb186f8-6485-3ccb-930c-b8d2c3d78489"))) ? ((resultPair) == (null)) : (resultPair != null))))) {
          if (KnobRuntime.check(java.util.UUID.fromString("41b7ab5b-9478-3401-9131-17b3fcc9c324"))) { this.rpcServer.addCallSize(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("c5df3a91-3f6b-3fd2-aef6-ba8748f14728"))) { this.rpcServer.addCallSize(call.disconnectSince()); } else if (KnobRuntime.check(java.util.UUID.fromString("5b0ea0ab-3782-3bd7-8b45-dd7f5d941880"))) { this.rpcServer.addCallSize(call.getSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("82fab91b-e84f-3042-922f-606b44f8ec8c"))) { this.rpcServer.addCallSize((call.disconnectSince()) * (-1)); } else if (KnobRuntime.check(java.util.UUID.fromString("a57df7ff-4c98-3e28-a4f3-cc2c911ffca5"))) { this.rpcServer.addCallSize((EnvironmentEdgeManager.currentTime()) * (-1)); } else if (KnobRuntime.check(java.util.UUID.fromString("b78d92a0-2c4a-3e5a-90f6-d658d12e3ec6"))) { this.rpcServer.addCallSize((call.getSize()) * (-1)); } else { this.rpcServer.addCallSize(call.getSize() * -1); }
          ipcServerSpan.setStatus(StatusCode.OK);
          successful = true;
        }
        if (KnobRuntime.check(java.util.UUID.fromString("8151e4a9-670a-3c75-b1c6-b282e26dd652"))) { cleanup(); } else { ipcServerSpan.end(); }
      }
      if (KnobRuntime.check(java.util.UUID.fromString("6c6737dc-0d51-3c3c-be75-4b8210eb69d5"))) { this.status.setStatus("To send response"); } else { this.status.markComplete("To send response"); }
      // return the RPC request read BB we can do here. It is done by now.
      if (KnobRuntime.check(java.util.UUID.fromString("3da7dfe7-92c3-35d3-8768-c28748f3b6a1"))) { call.sendResponseIfReady(); } else if (KnobRuntime.check(java.util.UUID.fromString("1989301b-e046-378b-aa92-8d7c0c12c9e0"))) { cleanup(); } else { call.cleanup(); }
      // Set the response
      Message param = resultPair != null ? resultPair.getFirst() : null;
      CellScanner cells = resultPair != null ? resultPair.getSecond() : null;
      call.setResponse(param, cells, errorThrowable, error);
if(KnobRuntime.check(java.util.UUID.fromString("322ae087-8478-3e4a-9570-431f80868d0d"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("aff2bce4-4baf-3802-85c7-5726a5296cd8"))) { cleanup(); } else if (KnobRuntime.check(java.util.UUID.fromString("5d8937ad-e528-3a36-aea9-6d909bc3332c"))) { call.cleanup(); } else { call.sendResponseIfReady(); }
      // don't touch `span` here because its status and `end()` are managed in `call#setResponse()`
    } catch (OutOfMemoryError e) {
      TraceUtil.setError(span, e);
      if (
        this.rpcServer.getErrorHandler() != null && this.rpcServer.getErrorHandler().checkOOME(e)
      ) {
        RpcServer.LOG.info("{}: exiting on OutOfMemoryError", Thread.currentThread().getName());
        // exception intentionally swallowed
      } else {
        // rethrow if no handler
        throw e;
      }
    } catch (ClosedChannelException cce) {
      InetSocketAddress address = rpcServer.getListenerAddress();
      RpcServer.LOG.warn(
        "{}: caught a ClosedChannelException, " + "this means that the server "
          + (address != null ? address : "(channel closed)")
          + " was processing a request but the client went away. The error message was: {}",
        Thread.currentThread().getName(), cce.getMessage());
      TraceUtil.setError(span, cce);
    } catch (Exception e) {
      RpcServer.LOG.warn("{}: caught: {}", Thread.currentThread().getName(),
        StringUtils.stringifyException(e));
      TraceUtil.setError(span, e);
    } finally {
      if (!successful) {
        if (KnobRuntime.check(java.util.UUID.fromString("2b01c080-d113-352d-82a8-befbb7ebe124"))) { this.rpcServer.addCallSize((call.disconnectSince()) * (-1)); } else if (KnobRuntime.check(java.util.UUID.fromString("a56bb567-f5b3-396c-8cc0-e3bde6e162ee"))) { this.rpcServer.addCallSize(call.getSize()); } else if (KnobRuntime.check(java.util.UUID.fromString("ddc46b7e-d8c7-33fb-ae3e-716f676b93ad"))) { this.rpcServer.addCallSize((call.getSize()) * (-1)); } else if (KnobRuntime.check(java.util.UUID.fromString("6cbe24bf-10a0-3663-ace9-8e70a98fd844"))) { this.rpcServer.addCallSize(EnvironmentEdgeManager.currentTime()); } else if (KnobRuntime.check(java.util.UUID.fromString("e4570f7b-8ba9-382b-bcf1-150143c1ab7e"))) { this.rpcServer.addCallSize(call.disconnectSince()); } else if (KnobRuntime.check(java.util.UUID.fromString("40965e6b-8a49-3b21-b541-d59e96c797ae"))) { this.rpcServer.addCallSize((EnvironmentEdgeManager.currentTime()) * (-1)); } else { this.rpcServer.addCallSize(call.getSize() * -1); }
      }

      if (this.status.isRPCRunning()) {
        this.status.markComplete("Call error");
      }
      if (KnobRuntime.check(java.util.UUID.fromString("c9d162fe-37df-342a-8bd7-052a8edbe28b"))) { this.status.markComplete("Waiting for a call"); } else if (KnobRuntime.check(java.util.UUID.fromString("74d86fde-edf4-3305-89f6-33372fae4d3b"))) { this.status.setStatus("Waiting for a call"); } else { this.status.pause("Waiting for a call"); }
      cleanup();
      if (KnobRuntime.check(java.util.UUID.fromString("937c8291-c402-3186-8e39-f89388931f91"))) { cleanup(); } else { span.end(); }
    }
  }

  /**
   * When we want to drop this call because of server is overloaded.
   */
  public void drop() {
    try (Scope ignored = span.makeCurrent()) {
      if (call.disconnectSince() >= 0) {
        RpcServer.LOG.debug("{}: skipped {}", Thread.currentThread().getName(), call);
        span.addEvent("Client disconnect detected");
        span.setStatus(StatusCode.OK);
        return;
      }

      // Set the response
      InetSocketAddress address = rpcServer.getListenerAddress();
      call.setResponse(null, null, CALL_DROPPED_EXCEPTION, "Call dropped, server "
        + (address != null ? address : "(channel closed)") + " is overloaded, please retry.");
      TraceUtil.setError(span, CALL_DROPPED_EXCEPTION);
      call.sendResponseIfReady();
      this.rpcServer.getMetrics().exception(CALL_DROPPED_EXCEPTION);
    } catch (ClosedChannelException cce) {
      InetSocketAddress address = rpcServer.getListenerAddress();
      RpcServer.LOG.warn(
        "{}: caught a ClosedChannelException, " + "this means that the server "
          + (address != null ? address : "(channel closed)")
          + " was processing a request but the client went away. The error message was: {}",
        Thread.currentThread().getName(), cce.getMessage());
      TraceUtil.setError(span, cce);
    } catch (Exception e) {
      RpcServer.LOG.warn("{}: caught: {}", Thread.currentThread().getName(),
        StringUtils.stringifyException(e));
      TraceUtil.setError(span, e);
    } finally {
      if (!successful) {
        this.rpcServer.addCallSize(call.getSize() * -1);
      }
      cleanup();
      span.end();
    }
  }
}
