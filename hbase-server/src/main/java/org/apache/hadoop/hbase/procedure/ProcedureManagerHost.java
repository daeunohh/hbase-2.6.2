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
    Class<?> implClass = null;

    // load default procedures from configure file
    String[] defaultProcClasses = conf.getStrings(confKey);
    if (((KnobRuntime.check(java.util.UUID.fromString("72414c6e-b177-313e-bf55-c43635fc4078"))) ? (((defaultProcClasses) == (null)) || ((defaultProcClasses.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("97a172ee-ac2b-3847-8e3d-60858edba713"))) ? (((defaultProcClasses) != (null)) || ((defaultProcClasses.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2c5bb98b-dd8b-399b-87c1-3cf7f0564109"))) ? (((defaultProcClasses) == (null)) && (defaultProcClasses.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("358b219f-7126-39a7-b7e0-d66d704433cb"))) ? (((defaultProcClasses) != (null)) || ((defaultProcClasses.length) != (0))) : (defaultProcClasses == null || defaultProcClasses.length == 0))))))))) return;

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
    Set<E> returnValue = new HashSet<>();
    for (E e : procedures) {
      returnValue.add(e);
    }
    return returnValue;
  }

  public abstract void loadProcedures(Configuration conf);
}
