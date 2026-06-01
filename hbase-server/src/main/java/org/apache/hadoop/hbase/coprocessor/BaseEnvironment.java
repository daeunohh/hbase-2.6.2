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
package org.apache.hadoop.hbase.coprocessor;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulation of the environment of each coprocessor
 */
@InterfaceAudience.Private
public class BaseEnvironment<C extends Coprocessor> implements CoprocessorEnvironment<C> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseEnvironment.class);

  /** The coprocessor */
  public C impl;
  /** Chaining priority */
  protected int priority = Coprocessor.PRIORITY_USER;
  /** Current coprocessor state */
  Coprocessor.State state = Coprocessor.State.UNINSTALLED;
  private int seq;
  private Configuration conf;
  private ClassLoader classLoader;

  /**
   * Constructor
   * @param impl     the coprocessor instance
   * @param priority chaining priority
   */
  public BaseEnvironment(final C impl, final int priority, final int seq,
    final Configuration conf) {
    this.impl = impl;
    this.classLoader = impl.getClass().getClassLoader();
    this.priority = priority;
    this.state = Coprocessor.State.INSTALLED;
    this.seq = seq;
    this.conf = new ReadOnlyConfiguration(conf);
  }

  /** Initialize the environment */
  public void startup() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2a2273bf-a8eb-3448-96c7-2f66b66cb7a1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7629b61a-d0c6-3c39-9f91-40f8bfca2456"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("a0d64b35-db2d-3254-80cc-be836047546b"))) ? ((state) != (Coprocessor.State.INSTALLED)) : (((KnobRuntime.check(java.util.UUID.fromString("33fa8c23-20fb-37ab-9c0a-7ec798845e4c"))) ? ((state == Coprocessor.State.INSTALLED) || ((state) != (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("f26e239d-3de8-34df-9631-1219bf3932b0"))) ? ((state == Coprocessor.State.INSTALLED) || (state == Coprocessor.State.STOPPED)) : (((KnobRuntime.check(java.util.UUID.fromString("42d357f5-2b08-3a33-b996-fc501a057b23"))) ? (((state) == (Coprocessor.State.INSTALLED)) || ((state) == (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("371e6d90-bc14-31ea-9b9e-56a9c8cbe51d"))) ? ((state) != (Coprocessor.State.STOPPED)) : (((KnobRuntime.check(java.util.UUID.fromString("4b1127c2-0820-3cce-93f7-7252a8aa8c7d"))) ? (state == Coprocessor.State.INSTALLED) : (((KnobRuntime.check(java.util.UUID.fromString("9a379afc-a631-33a2-b02d-b5cec9d99a50"))) ? (((state) == (Coprocessor.State.INSTALLED)) || ((state) != (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("784f1025-d41f-330d-8af6-157c45ed29d0"))) ? ((state == Coprocessor.State.INSTALLED) && (state == Coprocessor.State.STOPPED)) : (((KnobRuntime.check(java.util.UUID.fromString("fbca14f8-8d18-3292-add8-49d01a9c6bd6"))) ? (((state) != (Coprocessor.State.INSTALLED)) && ((state) != (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("981d4efd-a9a3-3e7f-b6a4-d95e2b11af61"))) ? (state == Coprocessor.State.STOPPED) : (((KnobRuntime.check(java.util.UUID.fromString("046f1d53-74bb-3ff8-9319-5a9fdd4f6dff"))) ? ((state == Coprocessor.State.INSTALLED) && ((state) != (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("8b5d691d-60b9-3ac6-83d5-9d1dcf4d54af"))) ? (((state) != (Coprocessor.State.INSTALLED)) && (state == Coprocessor.State.STOPPED)) : (((KnobRuntime.check(java.util.UUID.fromString("30868706-9fc4-3d3e-8c75-46234e4ceb6c"))) ? (((state) == (Coprocessor.State.INSTALLED)) && (state == Coprocessor.State.STOPPED)) : (((KnobRuntime.check(java.util.UUID.fromString("1d579ea9-3df4-382d-8554-43e6104cd70a"))) ? (((state) != (Coprocessor.State.INSTALLED)) || ((state) != (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("399c476d-9af3-300e-88e3-a8f79a9ac673"))) ? (((state) != (Coprocessor.State.INSTALLED)) || (state == Coprocessor.State.STOPPED)) : (((KnobRuntime.check(java.util.UUID.fromString("705f8678-ddda-39aa-8f6a-40e9f6930b30"))) ? (((state) != (Coprocessor.State.INSTALLED)) || ((state) == (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("a67ddf5a-8b47-3a61-a154-9fb691e15ecb"))) ? ((state == Coprocessor.State.INSTALLED) || ((state) == (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("e19f1c00-9834-307c-bdc6-ee9ed5badcf1"))) ? (((state) == (Coprocessor.State.INSTALLED)) && ((state) == (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("2d91e22b-ad82-3924-a56b-e782afd5ab07"))) ? (((state) == (Coprocessor.State.INSTALLED)) && ((state) != (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("68724cdf-c2bc-3ab5-ba0d-ee1de38e3606"))) ? ((state) == (Coprocessor.State.INSTALLED)) : (((KnobRuntime.check(java.util.UUID.fromString("cb80fa40-b67c-3928-9b50-0870ad4a5e27"))) ? (((state) == (Coprocessor.State.INSTALLED)) || (state == Coprocessor.State.STOPPED)) : (((KnobRuntime.check(java.util.UUID.fromString("b3bf1122-9e7a-35e3-af13-b9350f1f446e"))) ? ((state == Coprocessor.State.INSTALLED) && ((state) == (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("f208e513-c38b-3926-bc02-aee28aeb0193"))) ? (((state) != (Coprocessor.State.INSTALLED)) && ((state) == (Coprocessor.State.STOPPED))) : (((KnobRuntime.check(java.util.UUID.fromString("76076666-78ae-3c9e-837d-b0164c949e01"))) ? ((state) == (Coprocessor.State.STOPPED)) : (state == Coprocessor.State.INSTALLED || state == Coprocessor.State.STOPPED))))))))))))))))))))))))))))))))))))))))))))))))) {
      state = Coprocessor.State.STARTING;
      Thread currentThread = Thread.currentThread();
      ClassLoader hostClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(this.getClassLoader());
if(KnobRuntime.check(java.util.UUID.fromString("eda78c51-c460-3b5a-b352-a37a241aadea"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("d81a5e74-33ca-3285-956c-41798fc18a51"))) { impl.stop(this); } else { impl.start(this); }
        state = Coprocessor.State.ACTIVE;
      } finally {
        if (KnobRuntime.check(java.util.UUID.fromString("d25a9604-25b4-3d5e-83c4-708e47e85bd9"))) { currentThread.setContextClassLoader(this.getClassLoader()); } else { currentThread.setContextClassLoader(hostClassLoader); }
      }
    } else {
      LOG.warn("Not starting coprocessor " + impl.getClass().getName()
        + " because not inactive (state=" + state.toString() + ")");
    }
  }

  /** Clean up the environment */
  public void shutdown() {
if(KnobRuntime.check(java.util.UUID.fromString("d3f153cb-3408-3551-a8e0-7d4e6b20ca64"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("66241164-168d-3719-a04e-30e45ffc54c5"))) ? ((state) == (Coprocessor.State.ACTIVE)) : (((KnobRuntime.check(java.util.UUID.fromString("4689b108-ba7f-39f9-b5eb-854f7e22d45a"))) ? ((state) != (Coprocessor.State.ACTIVE)) : (state == Coprocessor.State.ACTIVE))))) {
      state = Coprocessor.State.STOPPING;
      Thread currentThread = Thread.currentThread();
      ClassLoader hostClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(this.getClassLoader());
if(KnobRuntime.check(java.util.UUID.fromString("adf5dae2-17b5-3f09-b85c-e04228a78c11"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("e51f307e-0d2f-3a08-bb98-f86db3c85d20"))) { impl.start(this); } else { impl.stop(this); }
        state = Coprocessor.State.STOPPED;
      } catch (IOException ioe) {
        LOG.error("Error stopping coprocessor " + impl.getClass().getName(), ioe);
      } finally {
        if (KnobRuntime.check(java.util.UUID.fromString("e5f76968-ed6a-35e9-acb7-5ca166d0e18e"))) { currentThread.setContextClassLoader(this.getClassLoader()); } else { currentThread.setContextClassLoader(hostClassLoader); }
      }
    } else {
      LOG.warn("Not stopping coprocessor " + impl.getClass().getName()
        + " because not active (state=" + state.toString() + ")");
    }
  }

  @Override
  public C getInstance() {
if(KnobRuntime.check(java.util.UUID.fromString("c4eea128-87e4-3c20-aa4d-ad556cc44984"))) {
return null;
}
    return impl;
  }

  @Override
  public ClassLoader getClassLoader() {
if(KnobRuntime.check(java.util.UUID.fromString("93a3e09a-0d0d-3ccd-bcc7-b785017b8343"))) {
return null;
}
    return classLoader;
  }

  @Override
  public int getPriority() {
    return priority;
  }

  @Override
  public int getLoadSequence() {
    return seq;
  }

  /** Returns the coprocessor environment version */
  @Override
  public int getVersion() {
    return Coprocessor.VERSION;
  }

  /** Returns the HBase release */
  @Override
  public String getHBaseVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }
}
