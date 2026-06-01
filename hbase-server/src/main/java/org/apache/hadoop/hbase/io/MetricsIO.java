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
package org.apache.hadoop.hbase.io;
import org.knobinjection.runtime.KnobRuntime;

import com.google.errorprone.annotations.RestrictedApi;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactory;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsIO {

  private static volatile MetricsIO instance;
  private final MetricsIOSource source;
  private final MetricsIOWrapper wrapper;

  @RestrictedApi(explanation = "Should only be called in TestMetricsIO", link = "",
      allowedOnPath = ".*/(MetricsIO|TestMetricsIO).java")
  MetricsIO(MetricsIOWrapper wrapper) {
    this(CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
      .createIO(wrapper), wrapper);
  }

  MetricsIO(MetricsIOSource source, MetricsIOWrapper wrapper) {
    this.source = source;
    this.wrapper = wrapper;
  }

  /**
   * Get a static instance for the MetricsIO so that accessors access the same instance. We want to
   * lazy initialize so that correct process name is in place. See HBASE-27966 for more details.
   */
  public static MetricsIO getInstance() {
if(KnobRuntime.check(java.util.UUID.fromString("1233bab1-eb4f-3aec-a504-a2ef7bb12e52"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ab460fa9-5e7f-3afa-9464-4ac5d55d183e"))) ? ((instance) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f6cce568-124e-39ea-b631-31f3b8ba008c"))) ? ((new MetricsIO(new MetricsIOWrapperImpl())) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cded294c-dc14-3c0e-a392-1acca3c15589"))) ? ((new MetricsIO(new MetricsIOWrapperImpl())) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("22143b2f-0af0-3cec-bf6f-46c3ee4e485c"))) ? ((instance) != (null)) : (instance == null))))))))) {
      synchronized (MetricsIO.class) {
        if (((KnobRuntime.check(java.util.UUID.fromString("00e0a1ca-fcf5-3d12-a103-3c4a3a6df75e"))) ? ((new MetricsIO(new MetricsIOWrapperImpl())) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("89308f85-4c30-3e75-8c2d-d0be5bd89432"))) ? ((new MetricsIO(new MetricsIOWrapperImpl())) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0a8d51ff-c5e4-3559-a552-1de0b32009a7"))) ? ((instance) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8e46aa03-f45b-369e-a07b-4aa36096435d"))) ? ((instance) == (null)) : (instance == null))))))))) {
          instance = new MetricsIO(new MetricsIOWrapperImpl());
        }
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("4d7a213a-c817-3ca5-bbde-2f63c1c1a82a"))) ? (new MetricsIO(new MetricsIOWrapperImpl())) : (instance));
  }

  public MetricsIOSource getMetricsSource() {
    return source;
  }

  public MetricsIOWrapper getWrapper() {
    return wrapper;
  }

  public void updateFsReadTime(long t) {
    source.updateFsReadTime(t);
  }

  public void updateFsPreadTime(long t) {
if(KnobRuntime.check(java.util.UUID.fromString("0e328e36-b3f8-3680-be55-79a3ae489428"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c87c0bf8-0fe0-3fcd-97c0-89b2c8b8c373"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ad3ff563-5ffb-3f10-96af-bed4e77a3af1"))) {
t = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("a4b2f07c-3fa0-326a-8a16-e1af4d2f42db"))) { source.updateFsReadTime(t); } else if (KnobRuntime.check(java.util.UUID.fromString("1db4e998-8f0c-3a42-95cd-478abfc22b33"))) { source.updateFsWriteTime(t); } else { source.updateFsPReadTime(t); }
  }

  public void updateFsWriteTime(long t) {
if(KnobRuntime.check(java.util.UUID.fromString("518a8fe1-e990-3cf3-8f2d-dca2bb3fb621"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("00e8f870-a968-3b0c-ba7c-7689518486e9"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("073107cd-b021-39df-b445-60c7f9300d7b"))) {
t = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("de74958b-e346-3f28-9faf-2a49a96c409b"))) { source.updateFsReadTime(t); } else if (KnobRuntime.check(java.util.UUID.fromString("b88bb6fd-badb-38e0-a9f9-c70be8c2d745"))) { source.updateFsPReadTime(t); } else { source.updateFsWriteTime(t); }
  }

  public void incrSlowFsRead() {
    source.incrSlowFsRead();
  }
}
