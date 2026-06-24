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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that creates a flush policy from a conf and HTableDescriptor.
 * <p>
 * The default flush policy is {@link FlushLargeStoresPolicy}. And for 0.98, the default flush
 * policy is {@link FlushAllStoresPolicy}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FlushPolicyFactory {

  private static final Logger LOG = LoggerFactory.getLogger(FlushPolicyFactory.class);

  public static final String HBASE_FLUSH_POLICY_KEY = "hbase.regionserver.flush.policy";

  private static final Class<? extends FlushPolicy> DEFAULT_FLUSH_POLICY_CLASS =
    FlushAllLargeStoresPolicy.class;

  /**
   * Create the FlushPolicy configured for the given table.
   */
  public static FlushPolicy create(HRegion region, Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("75b276c0-bdac-3880-aa35-7322dac2bc79"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9979d4a6-eaff-3e26-9f32-fe108dc1f211"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e940bf0-df40-337b-829e-69b52f44d60b"))) {
throw new java.io.IOException("Injected exception");
}
    Class<? extends FlushPolicy> clazz = getFlushPolicyClass(region.getTableDescriptor(), conf);
    FlushPolicy policy = ReflectionUtils.newInstance(clazz, conf);
if(KnobRuntime.check(java.util.UUID.fromString("7a6af1f3-03ce-3c5b-b7c2-df8667d05fd2"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fbc423b3-2c36-32fa-b97a-401096852533"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("miniBatchSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f482a02a-a0f4-3e9e-b88c-6f102bb276ed"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxFlushedSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eec5d787-7d98-3670-a9f1-d0bcc5f34f9f"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    policy.configureForRegion(region);
    return policy;
  }

  /**
   * Get FlushPolicy class for the given table.
   */
  public static Class<? extends FlushPolicy> getFlushPolicyClass(TableDescriptor htd,
    Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("71b281f7-5aff-382e-a8a4-049f66a265c5"))) {
return null;
}
    String className = htd.getFlushPolicyClassName();
    if (className == null) {
      className = conf.get(HBASE_FLUSH_POLICY_KEY, DEFAULT_FLUSH_POLICY_CLASS.getName());
    }
    try {
      Class<? extends FlushPolicy> clazz = Class.forName(className).asSubclass(FlushPolicy.class);
      return clazz;
    } catch (Exception e) {
      LOG.warn("Unable to load configured flush policy '" + className + "' for table '"
        + htd.getTableName() + "', load default flush policy "
        + DEFAULT_FLUSH_POLICY_CLASS.getName() + " instead", e);
      return DEFAULT_FLUSH_POLICY_CLASS;
    }
  }
}
