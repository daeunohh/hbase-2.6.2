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
public final class FlushThroughputControllerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(FlushThroughputControllerFactory.class);

  public static final String HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY =
    "hbase.regionserver.flush.throughput.controller";

  private static final Class<
    ? extends ThroughputController> DEFAULT_FLUSH_THROUGHPUT_CONTROLLER_CLASS =
      NoLimitThroughputController.class;

  private FlushThroughputControllerFactory() {
  }

  public static ThroughputController create(RegionServerServices server, Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("642551ac-6518-3620-a04d-af0507f22904"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("36f035a8-02b4-3f33-8e3e-2f15f9e3490b"))) {
return null;
}
    Class<? extends ThroughputController> clazz = getThroughputControllerClass(conf);
if(KnobRuntime.check(java.util.UUID.fromString("a5a37bb0-08ff-36ba-bf87-92ca5ddabf85"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("926827b6-8980-365d-a334-8ca5d6b1b7d2"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("b73de101-3d78-3cf2-b2d8-edc613eaf4bf"))) {
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
    String className = conf.get(HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
      DEFAULT_FLUSH_THROUGHPUT_CONTROLLER_CLASS.getName());
    try {
if(KnobRuntime.check(java.util.UUID.fromString("527acf30-1b5a-31f6-b7c9-5ab17a482baa"))) {
throw new java.lang.ClassNotFoundException("Injected exception");
}
      return Class.forName(className).asSubclass(ThroughputController.class);
    } catch (Exception e) {
      LOG.warn("Unable to load configured flush throughput controller '" + className
        + "', load default throughput controller "
        + DEFAULT_FLUSH_THROUGHPUT_CONTROLLER_CLASS.getName() + " instead", e);
      return DEFAULT_FLUSH_THROUGHPUT_CONTROLLER_CLASS;
    }
  }
}
