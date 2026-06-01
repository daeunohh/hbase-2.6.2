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
import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Provides the globally barriered procedure framework and environment for master oriented
 * operations. {@link org.apache.hadoop.hbase.master.HMaster} interacts with the loaded procedure
 * manager through this class.
 */
@InterfaceAudience.Private
public class MasterProcedureManagerHost extends ProcedureManagerHost<MasterProcedureManager> {

  private Hashtable<String, MasterProcedureManager> procedureMgrMap = new Hashtable<>();

  @Override
  public void loadProcedures(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("b28909b7-4315-34cc-8981-8a9a26c1b761"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("294639d5-b05e-307b-8bb1-fbe70e1c78cf"))) {
return;
}
    loadUserProcedures(conf, MASTER_PROCEDURE_CONF_KEY);
    for (MasterProcedureManager mpm : getProcedureManagers()) {
      procedureMgrMap.put(mpm.getProcedureSignature(), mpm);
    }
  }

  public void initialize(MasterServices master, final MetricsMaster metricsMaster)
    throws KeeperException, IOException, UnsupportedOperationException {
if(KnobRuntime.check(java.util.UUID.fromString("996d8b67-5e63-3116-a249-4a63763b383f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b89a8522-7b91-3786-b57b-6395c59f9e08"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("74afe43e-277b-3413-ad5a-0b9e0253d37c"))) {
throw new UnsupportedOperationException("Injected exception");
}
    for (MasterProcedureManager mpm : getProcedureManagers()) {
if(KnobRuntime.check(java.util.UUID.fromString("3f341cfa-bd35-31b1-ae20-7dedd6577f2d"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4ece5c5b-b081-3877-8bc0-c04680a2cd54"))) {
throw new java.io.IOException("Injected exception");
}
      mpm.initialize(master, metricsMaster);
    }
  }

  public void stop(String why) {
if(KnobRuntime.check(java.util.UUID.fromString("ce366f06-4b63-33bb-bb58-f70d0a1ac618"))) {
return;
}
    for (MasterProcedureManager mpm : getProcedureManagers()) {
      mpm.stop(why);
    }
  }

  public MasterProcedureManager getProcedureManager(String signature) {
    return procedureMgrMap.get(signature);
  }
}
