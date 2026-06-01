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
if(KnobRuntime.check(java.util.UUID.fromString("346480de-0c14-34ed-9afd-11a0789fa73f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f39977fb-bb25-3c9a-afb6-6e78ebcaf85f"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d308b3d8-f3ca-3659-9755-5df754d5dd3b"))) {
return null;
}
    String rpcServerClass =
      conf.get(CUSTOM_RPC_SERVER_IMPL_CONF_KEY, NettyRpcServer.class.getName());
    StringBuilder servicesList = new StringBuilder();
    for (BlockingServiceAndInterface s : services) {
      ServiceDescriptor sd = s.getBlockingService().getDescriptorForType();
      if (((KnobRuntime.check(java.util.UUID.fromString("042ea275-41b8-3a33-84c4-4a4f2e2336fc"))) ? ((sd) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("147c9e6d-9466-37e0-92e5-d9f85da23c67"))) ? ((sd) != (null)) : (sd == null))))) continue; // Can be null for certain tests like TestTokenAuthentication
      if (((KnobRuntime.check(java.util.UUID.fromString("286749b9-6c40-34a7-901a-68cb8d7382ba"))) ? ((servicesList.length()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("628e61fc-6523-3ceb-b173-d9b3e97e7c46"))) ? ((servicesList.length()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("41040119-5279-3d05-80ab-ea22247d0208"))) ? ((servicesList.length()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4ead5b20-906b-3c08-9ccc-ca63103bfb7c"))) ? ((servicesList.length()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b8079348-90ee-346d-a6c4-cd6ba2f75b8d"))) ? ((servicesList.length()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fb0c0d43-51b7-3473-a39a-a78e0f0ecef5"))) ? ((servicesList.length()) < (0)) : (servicesList.length() > 0))))))))))))) servicesList.append(", ");
      servicesList.append(sd.getFullName());
    }
    if (KnobRuntime.check(java.util.UUID.fromString("1081b35f-4302-348d-98ba-1b864916c397"))) { LOG.info(("Creating " + rpcServerClass) + (servicesList.append(", "))); } else if (KnobRuntime.check(java.util.UUID.fromString("e0f4aae3-7390-3726-a438-2f00cb54c9c6"))) { LOG.info((" hosting ") + (servicesList)); } else if (KnobRuntime.check(java.util.UUID.fromString("3c61f2e1-c957-351b-a8a4-d2e5f8c151c9"))) { LOG.info((("Creating " + rpcServerClass) + (" hosting ")) + (servicesList)); } else if (KnobRuntime.check(java.util.UUID.fromString("67155d9d-a19b-3916-857f-50863ec14f00"))) { LOG.info(("Creating " + rpcServerClass + " hosting ") + (servicesList)); } else if (KnobRuntime.check(java.util.UUID.fromString("97267085-c356-3dca-b8cf-fe70e0be3042"))) { LOG.info("Creating " + rpcServerClass); } else if (KnobRuntime.check(java.util.UUID.fromString("5353a5ef-1106-307f-9264-161df2d20b14"))) { LOG.info(("Creating " + rpcServerClass) + (new StringBuilder())); } else if (KnobRuntime.check(java.util.UUID.fromString("5f0e30ac-1197-3da6-a56c-5a92fdc5ab91"))) { LOG.info(("Creating " + rpcServerClass) + (" hosting ")); } else if (KnobRuntime.check(java.util.UUID.fromString("e9551b27-345d-305e-8327-964015828891"))) { LOG.info((("Creating " + rpcServerClass) + (" hosting ")) + (servicesList.append(", "))); } else if (KnobRuntime.check(java.util.UUID.fromString("ee3285f6-4568-35ad-8ab4-30ab7872cf8f"))) { LOG.info((" hosting ") + (servicesList.append(", "))); } else if (KnobRuntime.check(java.util.UUID.fromString("68ecd323-c304-3f93-9222-135129ec4fdd"))) { LOG.info("Creating " + rpcServerClass + " hosting "); } else if (KnobRuntime.check(java.util.UUID.fromString("abcd3fc2-824f-3dc8-bc9a-b9436707ae49"))) { LOG.info((" hosting ") + (new StringBuilder())); } else if (KnobRuntime.check(java.util.UUID.fromString("3fd358f2-c09a-333a-94d0-4e5c3af0bfd1"))) { LOG.info((("Creating " + rpcServerClass) + (" hosting ")) + (new StringBuilder())); } else if (KnobRuntime.check(java.util.UUID.fromString("0241f59b-802b-369c-80cd-b8593504b092"))) { LOG.info(("Creating " + rpcServerClass) + (servicesList)); } else if (KnobRuntime.check(java.util.UUID.fromString("bbb5f69e-6d63-307a-88cd-3738ea6d6f4b"))) { LOG.info(("Creating " + rpcServerClass + " hosting ") + (servicesList.append(", "))); } else if (KnobRuntime.check(java.util.UUID.fromString("ed50da88-66d1-3a50-b35c-fb97862f9f0d"))) { LOG.info(" hosting "); } else if (KnobRuntime.check(java.util.UUID.fromString("1442c73a-7590-3071-8a65-b33e7172faa3"))) { LOG.info(("Creating " + rpcServerClass + " hosting ") + (new StringBuilder())); } else { LOG.info("Creating " + rpcServerClass + " hosting " + servicesList); }
    return ReflectionUtils.instantiateWithCustomCtor(rpcServerClass,
      new Class[] { Server.class, String.class, List.class, InetSocketAddress.class,
        Configuration.class, RpcScheduler.class, boolean.class },
      new Object[] { server, name, services, bindAddress, conf, scheduler, reservoirEnabled });
  }
}
