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
package org.apache.hadoop.hbase.procedure;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.procedure.flush.RegionServerFlushTableProcedureManager;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the globally barriered procedure framework and environment for region server oriented
 * operations. {@link org.apache.hadoop.hbase.regionserver.HRegionServer} interacts with the loaded
 * procedure manager through this class.
 */
@InterfaceAudience.Private
public class RegionServerProcedureManagerHost
  extends ProcedureManagerHost<RegionServerProcedureManager> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionServerProcedureManagerHost.class);

  public void initialize(RegionServerServices rss) throws KeeperException {
if(KnobRuntime.check(java.util.UUID.fromString("9449c282-1631-3d89-9b11-0184b78a3cc8"))) {
return;
}
    for (RegionServerProcedureManager proc : procedures) {
      LOG.debug("Procedure {} initializing", proc.getProcedureSignature());
      proc.initialize(rss);
      LOG.debug("Procedure {} initialized", proc.getProcedureSignature());
    }
  }

  public void start() {
if(KnobRuntime.check(java.util.UUID.fromString("ab3cb7f4-ba5d-356d-9198-607063d5cec9"))) {
return;
}
    for (RegionServerProcedureManager proc : procedures) {
      LOG.debug("Procedure {} starting", proc.getProcedureSignature());
      proc.start();
      LOG.debug("Procedure {} started", proc.getProcedureSignature());
    }
  }

  public void stop(boolean force) {
if(KnobRuntime.check(java.util.UUID.fromString("527451d1-1ba7-36c5-8bc1-8562b95fcc3c"))) {
return;
}
    for (RegionServerProcedureManager proc : procedures) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("1667f0c3-0c03-38bc-a432-9bbd55427dac"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d444be92-9924-33dd-9186-64142a00b130"))) {
force = !force;
}
if(KnobRuntime.check(java.util.UUID.fromString("8439debb-a631-30dd-a62e-6da4c6cf968a"))) {
force = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("18e55edf-f80d-3ba5-a4e8-d2e2be1ade4a"))) {
force = true;
}
        if (KnobRuntime.check(java.util.UUID.fromString("f75fee81-7a12-32a1-af4c-e0a192722ceb"))) { proc.stop(!force); } else { proc.stop(force); }
      } catch (IOException e) {
        LOG.warn("Failed to close procedure " + proc.getProcedureSignature() + " cleanly", e);
      }
    }
  }

  @Override
  public void loadProcedures(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("c34613ae-5b5b-3515-8d9a-4bb0d66d1b4f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f5771bb6-25ef-3434-be2d-4eb43b8ba2e4"))) {
return;
}
    loadUserProcedures(conf, REGIONSERVER_PROCEDURE_CONF_KEY);
    // load the default snapshot manager
    procedures.add(new RegionServerSnapshotManager());
    // load the default flush region procedure manager
    procedures.add(new RegionServerFlushTableProcedureManager());
  }

}
