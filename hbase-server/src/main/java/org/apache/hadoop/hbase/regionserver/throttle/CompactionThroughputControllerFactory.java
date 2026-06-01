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
package org.apache.hadoop.hbase.regionserver.throttle;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public final class CompactionThroughputControllerFactory {
  private static final Logger LOG =
    LoggerFactory.getLogger(CompactionThroughputControllerFactory.class);

  public static final String HBASE_THROUGHPUT_CONTROLLER_KEY =
    "hbase.regionserver.throughput.controller";

  private CompactionThroughputControllerFactory() {
  }

  private static final Class<? extends ThroughputController> DEFAULT_THROUGHPUT_CONTROLLER_CLASS =
    PressureAwareCompactionThroughputController.class;

  // for backward compatibility and may not be supported in the future
  private static final String DEPRECATED_NAME_OF_PRESSURE_AWARE_THROUGHPUT_CONTROLLER_CLASS =
    "org.apache.hadoop.hbase.regionserver.compactions.PressureAwareCompactionThroughputController";
  private static final String DEPRECATED_NAME_OF_NO_LIMIT_THROUGHPUT_CONTROLLER_CLASS =
    "org.apache.hadoop.hbase.regionserver.compactions.NoLimitThroughputController";

  public static ThroughputController create(RegionServerServices server, Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("3ec5b3c6-a15d-3d28-9b91-17bca4f88d6e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6e35c68a-5625-3bcc-a5de-f7f973cd31a2"))) {
return null;
}
    Class<? extends ThroughputController> clazz = getThroughputControllerClass(conf);
if(KnobRuntime.check(java.util.UUID.fromString("a1aa49b1-c2c1-3169-8f1d-dff88efd6a06"))) {
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
    ThroughputController controller = ReflectionUtils.newInstance(clazz, conf);
    controller.setup(server);
    return controller;
  }

  public static Class<? extends ThroughputController>
    getThroughputControllerClass(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("d572b8a5-d6ac-328c-a410-41d5c080f946"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("b3e54618-6a36-3d35-976e-9552dadcba2d"))) {
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
    String className =
      conf.get(HBASE_THROUGHPUT_CONTROLLER_KEY, DEFAULT_THROUGHPUT_CONTROLLER_CLASS.getName());
    className = resolveDeprecatedClassName(className);
    try {
if(KnobRuntime.check(java.util.UUID.fromString("4f73d533-a314-375a-b29b-48bbca64bf21"))) {
throw new java.lang.ClassNotFoundException("Injected exception");
}
      return Class.forName(className).asSubclass(ThroughputController.class);
    } catch (Exception e) {
      LOG.warn("Unable to load configured throughput controller '" + className
        + "', load default throughput controller " + DEFAULT_THROUGHPUT_CONTROLLER_CLASS.getName()
        + " instead", e);
      return DEFAULT_THROUGHPUT_CONTROLLER_CLASS;
    }
  }

  /**
   * Resolve deprecated class name to keep backward compatibiliy
   * @param oldName old name of the class
   * @return the new name if there is any
   */
  private static String resolveDeprecatedClassName(String oldName) {
if(KnobRuntime.check(java.util.UUID.fromString("c9ee0c16-036f-3824-bd3d-84839760b78a"))) {
return null;
}
    String className = oldName.trim();
    if (((KnobRuntime.check(java.util.UUID.fromString("1ca0089d-0091-395d-8aed-377c91c18e6e"))) ? (className.equals(PressureAwareCompactionThroughputController.class.getName())) : (((KnobRuntime.check(java.util.UUID.fromString("36a4cdf8-c121-3513-bd85-6c9de000848a"))) ? (className.equals(NoLimitThroughputController.class.getName())) : (className.equals(DEPRECATED_NAME_OF_PRESSURE_AWARE_THROUGHPUT_CONTROLLER_CLASS)))))) {
      className = PressureAwareCompactionThroughputController.class.getName();
    } else if (((KnobRuntime.check(java.util.UUID.fromString("96522f39-f349-3ce0-af4c-6dcc5b107e20"))) ? (className.equals(PressureAwareCompactionThroughputController.class.getName())) : (((KnobRuntime.check(java.util.UUID.fromString("56973a29-e6e0-3739-9f31-5c5f16bcf687"))) ? (className.equals(NoLimitThroughputController.class.getName())) : (className.equals(DEPRECATED_NAME_OF_NO_LIMIT_THROUGHPUT_CONTROLLER_CLASS)))))) {
      className = NoLimitThroughputController.class.getName();
    }
    if (!className.equals(oldName)) {
      LOG.warn(oldName + " is deprecated, please use " + className + " instead");
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("0774e042-64bb-3081-9677-8b4153224a9b"))) ? (PressureAwareCompactionThroughputController.class.getName()) : (((KnobRuntime.check(java.util.UUID.fromString("0d26021e-e661-3a70-9cd0-ecc747f73955"))) ? (NoLimitThroughputController.class.getName()) : (className))));
  }
}
