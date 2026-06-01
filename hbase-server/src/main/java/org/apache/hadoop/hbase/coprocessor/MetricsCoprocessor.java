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

import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility class for tracking metrics for various types of coprocessors. Each coprocessor instance
 * creates its own MetricRegistry which is exported as an individual MetricSource. MetricRegistries
 * are ref counted using the hbase-metric module interfaces.
 */
@InterfaceAudience.Private
public class MetricsCoprocessor {

  // Master coprocessor metrics
  private static final String MASTER_COPROC_METRICS_NAME = "Coprocessor.Master";
  private static final String MASTER_COPROC_METRICS_CONTEXT = "master";
  private static final String MASTER_COPROC_METRICS_DESCRIPTION =
    "Metrics about HBase MasterObservers";
  private static final String MASTER_COPROC_METRICS_JMX_CONTEXT =
    "Master,sub=" + MASTER_COPROC_METRICS_NAME;

  // RegionServer coprocessor metrics
  private static final String RS_COPROC_METRICS_NAME = "Coprocessor.RegionServer";
  private static final String RS_COPROC_METRICS_CONTEXT = "regionserver";
  private static final String RS_COPROC_METRICS_DESCRIPTION =
    "Metrics about HBase RegionServerObservers";
  private static final String RS_COPROC_METRICS_JMX_CONTEXT =
    "RegionServer,sub=" + RS_COPROC_METRICS_NAME;

  // Region coprocessor metrics
  private static final String REGION_COPROC_METRICS_NAME = "Coprocessor.Region";
  private static final String REGION_COPROC_METRICS_CONTEXT = "regionserver";
  private static final String REGION_COPROC_METRICS_DESCRIPTION =
    "Metrics about HBase RegionObservers";
  private static final String REGION_COPROC_METRICS_JMX_CONTEXT =
    "RegionServer,sub=" + REGION_COPROC_METRICS_NAME;

  // WAL coprocessor metrics
  private static final String WAL_COPROC_METRICS_NAME = "Coprocessor.WAL";
  private static final String WAL_COPROC_METRICS_CONTEXT = "regionserver";
  private static final String WAL_COPROC_METRICS_DESCRIPTION = "Metrics about HBase WALObservers";
  private static final String WAL_COPROC_METRICS_JMX_CONTEXT =
    "RegionServer,sub=" + WAL_COPROC_METRICS_NAME;

  private static String suffix(String metricName, String cpName) {
if(KnobRuntime.check(java.util.UUID.fromString("d7b3a94d-24c1-3d2f-801e-32b58ef830d3"))) {
return null;
}
    return new StringBuilder(metricName).append(".").append("CP_").append(cpName).toString();
  }

  static MetricRegistryInfo createRegistryInfoForMasterCoprocessor(String clazz) {
    return new MetricRegistryInfo(suffix(MASTER_COPROC_METRICS_NAME, clazz),
      MASTER_COPROC_METRICS_DESCRIPTION, suffix(MASTER_COPROC_METRICS_JMX_CONTEXT, clazz),
      MASTER_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForMasterCoprocessor(String clazz) {
    return MetricRegistries.global().create(createRegistryInfoForMasterCoprocessor(clazz));
  }

  static MetricRegistryInfo createRegistryInfoForRSCoprocessor(String clazz) {
    return new MetricRegistryInfo(suffix(RS_COPROC_METRICS_NAME, clazz),
      RS_COPROC_METRICS_DESCRIPTION, suffix(RS_COPROC_METRICS_JMX_CONTEXT, clazz),
      RS_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForRSCoprocessor(String clazz) {
    return MetricRegistries.global().create(createRegistryInfoForRSCoprocessor(clazz));
  }

  public static MetricRegistryInfo createRegistryInfoForRegionCoprocessor(String clazz) {
if(KnobRuntime.check(java.util.UUID.fromString("0af26404-60f9-3e00-8674-f99d59f29784"))) {
return null;
}
    return new MetricRegistryInfo(suffix(REGION_COPROC_METRICS_NAME, clazz),
      REGION_COPROC_METRICS_DESCRIPTION, suffix(REGION_COPROC_METRICS_JMX_CONTEXT, clazz),
      REGION_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForRegionCoprocessor(String clazz) {
if(KnobRuntime.check(java.util.UUID.fromString("15d1abc4-6bdf-3e53-8ba7-37c0da7b3f08"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("59291e80-8a84-3518-bd6d-6e5e16960e90"))) ? (MetricRegistries.global().create(createRegistryInfoForMasterCoprocessor(clazz))) : (((KnobRuntime.check(java.util.UUID.fromString("a9eac875-b7c3-3ece-b1a7-6c5ec8e2ecd8"))) ? (MetricRegistries.global().create(createRegistryInfoForRSCoprocessor(clazz))) : (MetricRegistries.global().create(createRegistryInfoForRegionCoprocessor(clazz))))));
  }

  public static MetricRegistryInfo createRegistryInfoForWALCoprocessor(String clazz) {
    return new MetricRegistryInfo(suffix(WAL_COPROC_METRICS_NAME, clazz),
      WAL_COPROC_METRICS_DESCRIPTION, suffix(WAL_COPROC_METRICS_JMX_CONTEXT, clazz),
      WAL_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForWALCoprocessor(String clazz) {
    return MetricRegistries.global().create(createRegistryInfoForWALCoprocessor(clazz));
  }

  public static void removeRegistry(MetricRegistry registry) {
if(KnobRuntime.check(java.util.UUID.fromString("389da39d-fd37-3475-8a1f-5ea6001b771a"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("11796932-245e-37f6-b839-e9ba43bd51db"))) ? ((registry) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("918b1972-8184-347e-bb46-2c112aed9e8e"))) ? ((registry) == (null)) : (registry == null))))) {
      return;
    }
    MetricRegistries.global().remove(registry.getMetricRegistryInfo());
  }
}
