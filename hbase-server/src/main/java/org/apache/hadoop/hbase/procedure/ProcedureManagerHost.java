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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the common setup framework and runtime services for globally barriered procedure
 * invocation from HBase services.
 * @param <E> the specific procedure management extension that a concrete implementation provides
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ProcedureManagerHost<E extends ProcedureManager> {

  public static final String REGIONSERVER_PROCEDURE_CONF_KEY =
    "hbase.procedure.regionserver.classes";
  public static final String MASTER_PROCEDURE_CONF_KEY = "hbase.procedure.master.classes";

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureManagerHost.class);

  protected Set<E> procedures = new HashSet<>();

  /**
   * Load system procedures. Read the class names from configuration. Called by constructor.
   */
  protected void loadUserProcedures(Configuration conf, String confKey) {
if(KnobRuntime.check(java.util.UUID.fromString("52f7a582-4226-3a9e-8d60-5f97c265e777"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5cca90e-c4ad-322a-a0ff-fc15ed415731"))) {
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
    Class<?> implClass = null;

    // load default procedures from configure file
    String[] defaultProcClasses = conf.getStrings(confKey);
    if (((KnobRuntime.check(java.util.UUID.fromString("c934af03-2853-3350-95ba-cfbf2342d960"))) ? ((defaultProcClasses == null) || ((defaultProcClasses.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("15006d6f-bc61-39b3-83b0-95501b4cd927"))) ? (((defaultProcClasses) != (null)) && ((defaultProcClasses.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8bdee280-5e16-3766-b4dd-9320a467334e"))) ? (defaultProcClasses.length == 0) : (((KnobRuntime.check(java.util.UUID.fromString("242ddd2d-600b-3262-bc40-675cba3bb8a3"))) ? ((defaultProcClasses == null) || (defaultProcClasses.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("d8d3814d-07ed-3844-81c8-3de657716a3e"))) ? (((defaultProcClasses) == (null)) || (defaultProcClasses.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("72414c6e-b177-313e-bf55-c43635fc4078"))) ? (((defaultProcClasses) == (null)) || ((defaultProcClasses.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a208b4ea-4254-3510-a5da-da943cfe9318"))) ? (((defaultProcClasses) != (null)) || (defaultProcClasses.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ceb57f87-0682-3501-8e09-a63717c30ec2"))) ? (((defaultProcClasses) != (null)) && ((defaultProcClasses.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("97a172ee-ac2b-3847-8e3d-60858edba713"))) ? (((defaultProcClasses) != (null)) || ((defaultProcClasses.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f70eba19-bf3d-368b-be3f-a0f2dc79c291"))) ? (((defaultProcClasses) == (null)) && ((defaultProcClasses.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("38070be3-deeb-31fe-a8ac-cb6d7875cd24"))) ? ((defaultProcClasses.length) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8ef0594d-7980-3b59-8ef7-8167873e7473"))) ? ((defaultProcClasses == null) && ((defaultProcClasses.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("15d84470-0ffc-3aa5-a56f-2dc25a72297c"))) ? (((defaultProcClasses) == (null)) && ((defaultProcClasses.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("77fb9e67-7331-39db-9e6b-3ce85588cac1"))) ? (((defaultProcClasses) == (null)) || ((defaultProcClasses.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("df231947-07e5-371a-9658-b0825e82009f"))) ? ((defaultProcClasses == null) && (defaultProcClasses.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b376da39-7aa4-385f-bb4b-0c7c0895e1f7"))) ? ((defaultProcClasses == null) || ((defaultProcClasses.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2c1f6819-e613-3929-a8d0-28c94c435590"))) ? ((defaultProcClasses == null) && ((defaultProcClasses.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0ef7c642-b456-3c93-91cd-7a3b6a6faff4"))) ? (defaultProcClasses == null) : (((KnobRuntime.check(java.util.UUID.fromString("84b214ed-dff3-3708-a77d-d6ea4d4ef080"))) ? ((defaultProcClasses) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c3eb42e3-aac8-34a7-aa42-828f8c74b0cd"))) ? (((defaultProcClasses) != (null)) && (defaultProcClasses.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2c5bb98b-dd8b-399b-87c1-3cf7f0564109"))) ? (((defaultProcClasses) == (null)) && (defaultProcClasses.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("f8aaa1b1-65a3-3764-8b64-48645bd15449"))) ? ((defaultProcClasses.length) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("89746f74-105c-39c0-be74-adefce8e017c"))) ? ((defaultProcClasses) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("358b219f-7126-39a7-b7e0-d66d704433cb"))) ? (((defaultProcClasses) != (null)) || ((defaultProcClasses.length) != (0))) : (defaultProcClasses == null || defaultProcClasses.length == 0))))))))))))))))))))))))))))))))))))))))))))))))) return;

    List<E> configured = new ArrayList<>();
    for (String className : defaultProcClasses) {
      className = className.trim();
      ClassLoader cl = this.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(cl);
      try {
        implClass = cl.loadClass(className);
        configured.add(loadInstance(implClass));
        LOG.info("User procedure " + className + " was loaded successfully.");
      } catch (ClassNotFoundException e) {
        LOG.warn("Class " + className + " cannot be found. " + e.getMessage());
      } catch (IOException e) {
        LOG.warn("Load procedure " + className + " failed. " + e.getMessage());
      }
    }

    // add entire set to the collection
    procedures.addAll(configured);
  }

  @SuppressWarnings("unchecked")
  public E loadInstance(Class<?> implClass) throws IOException {
    // create the instance
    E impl;
    Object o = null;
    try {
      o = implClass.getDeclaredConstructor().newInstance();
      impl = (E) o;
    } catch (Exception e) {
      throw new IOException(e);
    }

    return impl;
  }

  // Register a procedure manager object
  public void register(E obj) {
if(KnobRuntime.check(java.util.UUID.fromString("1b8db354-3b83-3321-8704-cb6fb49514e1"))) {
return;
}
    procedures.add(obj);
  }

  public Set<E> getProcedureManagers() {
if(KnobRuntime.check(java.util.UUID.fromString("30a28700-462d-3477-982b-f236136ff707"))) {
return null;
}
    Set<E> returnValue = new HashSet<>();
    for (E e : procedures) {
      returnValue.add(e);
    }
    return returnValue;
  }

  public abstract void loadProcedures(Configuration conf);
}
