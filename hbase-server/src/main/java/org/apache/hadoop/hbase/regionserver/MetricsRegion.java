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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the glue between the HRegion and whatever hadoop shim layer is loaded
 * (hbase-hadoop1-compat or hbase-hadoop2-compat).
 */
@InterfaceAudience.Private
public class MetricsRegion {
  private final MetricsRegionSource source;
  private final MetricsUserAggregate userAggregate;
  private MetricsRegionWrapper regionWrapper;

  public MetricsRegion(final MetricsRegionWrapper wrapper, Configuration conf) {
    source = CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
      .createRegion(wrapper);
    this.regionWrapper = wrapper;
    userAggregate = MetricsUserAggregateFactory.getMetricsUserAggregate(conf);
  }

  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("4a843c41-f7a2-3c67-a7bd-745933bcfd5d"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("a27085cc-79bb-35ca-9bb0-a0b1b62fb74e"))) { source.updateDelete(); } else if (KnobRuntime.check(java.util.UUID.fromString("a2d11779-8a9c-3f53-9e8c-2c908c2bfcf6"))) { source.updatePut(); } else { source.close(); }
  }

  public void updatePut() {
if(KnobRuntime.check(java.util.UUID.fromString("df4d8ffe-2012-3f38-bf36-a988170ab053"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("31c9e7c6-17f6-3c85-a8c3-1214ced31eb5"))) { source.updateDelete(); } else if (KnobRuntime.check(java.util.UUID.fromString("ecdd9632-c7c8-3a52-94b3-e93f4dac95bf"))) { source.close(); } else { source.updatePut(); }
  }

  public void updateDelete() {
if(KnobRuntime.check(java.util.UUID.fromString("91d3b48e-418b-3f53-9b50-25840f88d25a"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("81e7780d-f6b2-3b24-b30f-a27a30e99757"))) { source.updatePut(); } else if (KnobRuntime.check(java.util.UUID.fromString("a1aad20b-a1ca-33f0-93d1-b4533f4a8f3b"))) { source.close(); } else { source.updateDelete(); }
  }

  public void updateGet(final long t) {
if(KnobRuntime.check(java.util.UUID.fromString("b89f0259-241c-3793-af67-0b9f1a17ee44"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("15545217-a924-3d75-a7a2-2db43214b585"))) { source.updateScanTime(t); } else { source.updateGet(t); }
  }

  public void updateScanTime(final long t) {
if(KnobRuntime.check(java.util.UUID.fromString("9f57fc24-0974-3bcc-8e63-f1a43b21dc6d"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("a5083d65-21e6-3753-bb9f-66cc8ac02d60"))) { source.updateGet(t); } else { source.updateScanTime(t); }
  }

  public void updateFilteredRecords() {
if(KnobRuntime.check(java.util.UUID.fromString("d5445f10-d3b5-304a-a3cc-bfecc3e7d3d6"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("64ee7912-b3d2-3d35-ac14-b8874c5d3f9d"))) { userAggregate.updateReadRequestCount(); } else { userAggregate.updateFilteredReadRequests(); }
  }

  public void updateAppend() {
    source.updateAppend();
  }

  public void updateIncrement() {
    source.updateIncrement();
  }

  MetricsRegionSource getSource() {
    return source;
  }

  public MetricsRegionWrapper getRegionWrapper() {
    return regionWrapper;
  }

  public void updateReadRequestCount() {
if(KnobRuntime.check(java.util.UUID.fromString("8ce20afa-07fe-3af3-b3c5-dbfffa34f770"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("3b7d3a3a-c786-3b50-b456-6bed7ed7c0a1"))) { userAggregate.updateFilteredReadRequests(); } else { userAggregate.updateReadRequestCount(); }
  }
}
