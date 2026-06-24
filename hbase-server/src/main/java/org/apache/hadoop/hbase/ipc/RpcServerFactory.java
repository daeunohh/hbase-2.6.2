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
import java.net.InetSocketAddress;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.ServiceDescriptor;

@InterfaceAudience.Private
public class RpcServerFactory {

  public static final Logger LOG = LoggerFactory.getLogger(RpcServerFactory.class);

  public static final String CUSTOM_RPC_SERVER_IMPL_CONF_KEY = "hbase.rpc.server.impl";

  /**
   * Private Constructor
   */
  private RpcServerFactory() {
  }

  public static RpcServer createRpcServer(final Server server, final String name,
    final List<BlockingServiceAndInterface> services, final InetSocketAddress bindAddress,
    Configuration conf, RpcScheduler scheduler) throws IOException {
    return createRpcServer(server, name, services, bindAddress, conf, scheduler, true);
  }

  public static RpcServer createRpcServer(final Server server, final String name,
    final List<BlockingServiceAndInterface> services, final InetSocketAddress bindAddress,
    Configuration conf, RpcScheduler scheduler, boolean reservoirEnabled) throws IOException {
    String rpcServerClass =
      conf.get(CUSTOM_RPC_SERVER_IMPL_CONF_KEY, NettyRpcServer.class.getName());
    StringBuilder servicesList = new StringBuilder();
    for (BlockingServiceAndInterface s : services) {
      ServiceDescriptor sd = s.getBlockingService().getDescriptorForType();
      if (sd == null) continue; // Can be null for certain tests like TestTokenAuthentication
      if (((KnobRuntime.check(java.util.UUID.fromString("fb0c0d43-51b7-3473-a39a-a78e0f0ecef5"))) ? ((servicesList.length()) < (0)) : (servicesList.length() > 0))) servicesList.append(", ");
      servicesList.append(sd.getFullName());
    }
    if (KnobRuntime.check(java.util.UUID.fromString("e9551b27-345d-305e-8327-964015828891"))) { LOG.info((("Creating " + rpcServerClass) + (" hosting ")) + (servicesList.append(", "))); } else if (KnobRuntime.check(java.util.UUID.fromString("1442c73a-7590-3071-8a65-b33e7172faa3"))) { LOG.info(("Creating " + rpcServerClass + " hosting ") + (new StringBuilder())); } else { LOG.info("Creating " + rpcServerClass + " hosting " + servicesList); }
    return ReflectionUtils.instantiateWithCustomCtor(rpcServerClass,
      new Class[] { Server.class, String.class, List.class, InetSocketAddress.class,
        Configuration.class, RpcScheduler.class, boolean.class },
      new Object[] { server, name, services, bindAddress, conf, scheduler, reservoirEnabled });
  }
}
