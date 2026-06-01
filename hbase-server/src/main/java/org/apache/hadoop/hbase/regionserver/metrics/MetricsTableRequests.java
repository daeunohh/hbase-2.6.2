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
package org.apache.hadoop.hbase.regionserver.metrics;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsTableRequests {

  public static final String ENABLE_TABLE_LATENCIES_METRICS_KEY =
    "hbase.regionserver.enable.table.latencies";

  public static final boolean ENABLE_TABLE_LATENCIES_METRICS_DEFAULT = true;

  public static final String ENABLE_TABLE_QUERY_METER_METRICS_KEY =
    "hbase.regionserver.enable.table.query.meter";

  public static final boolean ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT = true;

  /**
   * The name of the metrics
   */
  private final static String METRICS_NAME = "TableRequests";

  /**
   * The name of the metrics context that metrics will be under.
   */
  private final static String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  private final static String METRICS_DESCRIPTION =
    "Metrics about Tables on a single HBase RegionServer";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  private final static String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  private final static String GET_TIME = "getTime";
  private final static String SCAN_TIME = "scanTime";
  private final static String SCAN_SIZE = "scanSize";
  private final static String PUT_TIME = "putTime";
  private final static String PUT_BATCH_TIME = "putBatchTime";
  private final static String DELETE_TIME = "deleteTime";
  private final static String DELETE_BATCH_TIME = "deleteBatchTime";
  private final static String INCREMENT_TIME = "incrementTime";
  private final static String APPEND_TIME = "appendTime";
  private final static String CHECK_AND_DELETE_TIME = "checkAndDeleteTime";
  private final static String CHECK_AND_PUT_TIME = "checkAndPutTime";
  private final static String CHECK_AND_MUTATE_TIME = "checkAndMutateTime";
  String BLOCK_BYTES_SCANNED_KEY = "blockBytesScannedCount";
  String GET_BLOCK_BYTES_SCANNED_KEY = "getBlockBytesScanned";
  String SCAN_BLOCK_BYTES_SCANNED_KEY = "scanBlockBytesScanned";
  String CHECK_AND_MUTATE_BLOCK_BYTES_SCANNED_KEY = "checkAndMutateBlockBytesScanned";
  String INCREMENT_BLOCK_BYTES_SCANNED_KEY = "incrementBlockBytesScanned";
  String APPEND_BLOCK_BYTES_SCANNED_KEY = "appendBlockBytesScanned";
  private final static String TABLE_READ_QUERY_PER_SECOND = "tableReadQueryPerSecond";
  private final static String TABLE_WRITE_QUERY_PER_SECOND = "tableWriteQueryPerSecond";

  private Histogram getTimeHistogram;
  private Histogram scanTimeHistogram;
  private Histogram scanSizeHistogram;
  private Histogram putTimeHistogram;
  private Histogram putBatchTimeHistogram;
  private Histogram deleteTimeHistogram;
  private Histogram deleteBatchTimeHistogram;
  private Histogram incrementTimeHistogram;
  private Histogram appendTimeHistogram;
  private Histogram checkAndDeleteTimeHistogram;
  private Histogram checkAndPutTimeHistogram;
  private Histogram checkAndMutateTimeHistogram;
  private Counter blockBytesScannedCount;
  private Histogram checkAndMutateBlockBytesScanned;
  private Histogram getBlockBytesScanned;
  private Histogram incrementBlockBytesScanned;
  private Histogram appendBlockBytesScanned;
  private Histogram scanBlockBytesScanned;

  private Meter readMeter;
  private Meter writeMeter;

  private MetricRegistry registry;
  private TableName tableName;
  private Configuration conf;
  private MetricRegistryInfo registryInfo;

  private boolean enableTableLatenciesMetrics;
  private boolean enabTableQueryMeterMetrics;

  public boolean isEnableTableLatenciesMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("0d0b0f76-2326-305b-861e-99a411520fee"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ce28a2cf-e764-32d3-a124-bd40d5bd60d7"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8cde34fb-2952-387c-a549-96156aec4f7e"))) ? (!enableTableLatenciesMetrics) : (enableTableLatenciesMetrics));
  }

  public boolean isEnabTableQueryMeterMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("872aeec8-47bd-335c-8fbf-f27d69387743"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("84a5af4c-7845-36cb-9937-835dc58308f9"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("4cb30396-a928-3941-89f2-53c0c657cd47"))) ? (!enabTableQueryMeterMetrics) : (enabTableQueryMeterMetrics));
  }

  public MetricsTableRequests(TableName tableName, Configuration conf) {
    init(tableName, conf);
  }

  private void init(TableName tableName, Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("056c6dc7-2a77-3d04-b3f5-7149337e3812"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("4a253df0-4ef7-3dda-9eb5-a8d318af4584"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4bbeb757-6ad0-3bef-8ae6-fa6e64187788"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("689b5846-15d1-30e8-8902-52af82b03ab3"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("100e3ea1-6202-3b49-b91e-15161f0bb50f"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bed8c9f8-a066-369e-b982-3a32c36da2ce"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.tableName = tableName;
    this.conf = conf;
    enableTableLatenciesMetrics = this.conf.getBoolean(ENABLE_TABLE_LATENCIES_METRICS_KEY,
      ENABLE_TABLE_LATENCIES_METRICS_DEFAULT);
    enabTableQueryMeterMetrics = this.conf.getBoolean(ENABLE_TABLE_QUERY_METER_METRICS_KEY,
      ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    if (((KnobRuntime.check(java.util.UUID.fromString("554e118f-6ad5-347a-9827-b6d7a2f4db2a"))) ? ((!enableTableLatenciesMetrics) || (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("b2bb002c-c941-3808-8f1f-ba18ae2fa3fa"))) ? ((enableTableLatenciesMetrics) || (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("cb8898a2-8bee-3846-9d2b-b5d7439e9c1a"))) ? (enableTableLatenciesMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("452ed9b6-eec1-3207-b256-d0f610dc582d"))) ? (!enableTableLatenciesMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("d1a8e6e6-e24d-34eb-830f-922ceb2883c7"))) ? ((!enableTableLatenciesMetrics) || (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("4fcc8b99-73b2-3714-a3a7-0a728544dc41"))) ? ((!enableTableLatenciesMetrics) && (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("da2faa3b-8fc6-367e-82c0-ec1727d5548e"))) ? ((enableTableLatenciesMetrics) && (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("07d9cab3-3553-3771-90b0-6edc82f5aea2"))) ? (!enabTableQueryMeterMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("2fe863ab-6bed-3e47-ad31-a315e9174fea"))) ? (enabTableQueryMeterMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("860bbebe-2e36-3b95-8ed6-8739a856c8a3"))) ? ((enableTableLatenciesMetrics) && (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("46e7bbd4-d8e0-390f-9ea6-be02889d8576"))) ? ((enableTableLatenciesMetrics) || (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("39b8456c-1653-30da-a4c0-54a94439ab0f"))) ? ((!enableTableLatenciesMetrics) && (enabTableQueryMeterMetrics)) : (enableTableLatenciesMetrics || enabTableQueryMeterMetrics))))))))))))))))))))))))) {
      registry = createRegistryForTableRequests();
      if (((KnobRuntime.check(java.util.UUID.fromString("dd9ab758-bf60-3f7e-9808-b0db4e0a3582"))) ? (!enableTableLatenciesMetrics) : (enableTableLatenciesMetrics))) {
        getTimeHistogram = registry.histogram(GET_TIME);
        scanTimeHistogram = registry.histogram(SCAN_TIME);
        scanSizeHistogram = registry.histogram(SCAN_SIZE);
        putTimeHistogram = registry.histogram(PUT_TIME);
        putBatchTimeHistogram = registry.histogram(PUT_BATCH_TIME);
        deleteTimeHistogram = registry.histogram(DELETE_TIME);
        deleteBatchTimeHistogram = registry.histogram(DELETE_BATCH_TIME);
        incrementTimeHistogram = registry.histogram(INCREMENT_TIME);
        appendTimeHistogram = registry.histogram(APPEND_TIME);
        checkAndDeleteTimeHistogram = registry.histogram(CHECK_AND_DELETE_TIME);
        checkAndPutTimeHistogram = registry.histogram(CHECK_AND_PUT_TIME);
        checkAndMutateTimeHistogram = registry.histogram(CHECK_AND_MUTATE_TIME);
        blockBytesScannedCount = registry.counter(BLOCK_BYTES_SCANNED_KEY);
        checkAndMutateBlockBytesScanned =
          registry.histogram(CHECK_AND_MUTATE_BLOCK_BYTES_SCANNED_KEY);
        getBlockBytesScanned = registry.histogram(GET_BLOCK_BYTES_SCANNED_KEY);
        incrementBlockBytesScanned = registry.histogram(INCREMENT_BLOCK_BYTES_SCANNED_KEY);
        appendBlockBytesScanned = registry.histogram(APPEND_BLOCK_BYTES_SCANNED_KEY);
        scanBlockBytesScanned = registry.histogram(SCAN_BLOCK_BYTES_SCANNED_KEY);
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("7f5f0aa8-5beb-3d53-be82-a83948837590"))) ? (!enabTableQueryMeterMetrics) : (enabTableQueryMeterMetrics))) {
        readMeter = registry.meter(TABLE_READ_QUERY_PER_SECOND);
        writeMeter = registry.meter(TABLE_WRITE_QUERY_PER_SECOND);
      }
    }
  }

  private MetricRegistry createRegistryForTableRequests() {
if(KnobRuntime.check(java.util.UUID.fromString("d6800166-08ed-392f-bd4d-784b99a98470"))) {
return null;
}
    return MetricRegistries.global().create(createRegistryInfoForTableRequests());
  }

  private MetricRegistryInfo createRegistryInfoForTableRequests() {
if(KnobRuntime.check(java.util.UUID.fromString("f9e5622b-4e58-31dd-a0c4-0cee47586aeb"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("26396745-7f1e-3f6f-92e4-7745e059a822"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("cfa54135-755d-364e-a82d-1271e8f60ba1"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f76bcfbd-6de4-3a7f-ace7-b255b3733ce5"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("06df03f4-30f2-35db-8bbe-6ed6a03b93e2"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    registryInfo = new MetricRegistryInfo(qualifyMetrics(METRICS_NAME, tableName),
      METRICS_DESCRIPTION, qualifyMetrics(METRICS_JMX_CONTEXT, tableName), METRICS_CONTEXT, false);
    return registryInfo;
  }

  public void removeRegistry() {
if(KnobRuntime.check(java.util.UUID.fromString("f87a092a-982b-3877-860c-fe3a89aac086"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("50b80a2e-69ac-3793-9770-f1944c506bb8"))) ? (enabTableQueryMeterMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("7f2aee61-b206-32bf-9e0d-c7f0918a087b"))) ? (enableTableLatenciesMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("779c72ee-0cb6-33ed-a552-a451792c16b1"))) ? ((!enableTableLatenciesMetrics) || (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("08f58453-d09b-32a4-9e32-d678ec79d228"))) ? ((!enableTableLatenciesMetrics) || (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("658c7c32-388c-3a48-a1bc-6b81dff562aa"))) ? (!enabTableQueryMeterMetrics) : (((KnobRuntime.check(java.util.UUID.fromString("ebe12ee1-baff-3211-9129-5e5c3c97f1db"))) ? ((enableTableLatenciesMetrics) || (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("87683855-8fab-3e5b-aee0-a00a154582f5"))) ? ((enableTableLatenciesMetrics) || (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("b6032bca-6f1f-3de1-8a78-d8b3bcaba549"))) ? ((!enableTableLatenciesMetrics) && (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("5d4827f8-f4ab-31eb-b11b-a69b276dfdb8"))) ? ((!enableTableLatenciesMetrics) && (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("867de3d1-d111-39af-a2e5-c1258441ff28"))) ? ((enableTableLatenciesMetrics) && (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("a85216e9-6e4a-3dbf-96cd-d9c9c5ddc77a"))) ? ((enableTableLatenciesMetrics) && (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("556b602e-e15d-3e52-b2c3-538a4ef2003f"))) ? (!enableTableLatenciesMetrics) : (enableTableLatenciesMetrics || enabTableQueryMeterMetrics))))))))))))))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("48e04375-ec35-32d5-9d29-fb2cf43735ec"))) { MetricRegistries.global().remove(createRegistryInfoForTableRequests()); } else { MetricRegistries.global().remove(registry.getMetricRegistryInfo()); }
    }
  }

  private static String qualifyMetrics(String prefix, TableName tableName) {
if(KnobRuntime.check(java.util.UUID.fromString("d14425b4-a403-3252-87c8-3d19ed2ba757"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7ab983f-1913-3743-9708-70d6769c876b"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("346ebb70-2875-3a59-9b31-01e3d558cbf3"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c6f10c08-4986-3fd2-92b2-53cadcccab9d"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e189c8d0-f02f-3f70-9d33-4a46191e4648"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    StringBuilder sb = new StringBuilder();
    sb.append(prefix).append("_");
    if (KnobRuntime.check(java.util.UUID.fromString("3d3cf000-ac56-30cc-8a24-9d2466a37e0c"))) { sb.append("Namespace_").append(tableName.getQualifierAsString()); } else { sb.append("Namespace_").append(tableName.getNamespaceAsString()); }
    if (KnobRuntime.check(java.util.UUID.fromString("f44a24bb-a377-374e-9376-a66cb2a39a9e"))) { sb.append("_table_").append(tableName.getNamespaceAsString()); } else { sb.append("_table_").append(tableName.getQualifierAsString()); }
    return sb.toString();
  }

  /**
   * Update the Put time histogram
   * @param t time it took
   */
  public void updatePut(long t) {
if(KnobRuntime.check(java.util.UUID.fromString("e19c7edc-1dab-3901-acb2-68434077799f"))) {
return;
}
    if (isEnableTableLatenciesMetrics()) {
if(KnobRuntime.check(java.util.UUID.fromString("8321da96-3980-39e3-a972-948bdbe6630a"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("72b45deb-9fb4-38dc-afb2-1ba985f4d810"))) {
t = 0;
}
      putTimeHistogram.update(t);
    }
  }

  /**
   * Update the batch Put time histogram
   * @param t time it took
   */
  public void updatePutBatch(long t) {
if(KnobRuntime.check(java.util.UUID.fromString("20ebfa55-d652-3e20-898a-f662a0c7eae8"))) {
return;
}
    if (isEnableTableLatenciesMetrics()) {
if(KnobRuntime.check(java.util.UUID.fromString("1b16781f-9a53-3373-9e6d-32181e13fa3f"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ee7e5c7d-7b0e-314c-9c2e-5128fe1eaa78"))) {
t = -1;
}
      putBatchTimeHistogram.update(t);
    }
  }

  /**
   * Update the Delete time histogram
   * @param t time it took
   */
  public void updateDelete(long t) {
if(KnobRuntime.check(java.util.UUID.fromString("838b0e63-b12f-3e86-9991-04ef6975db5f"))) {
return;
}
    if (isEnableTableLatenciesMetrics()) {
if(KnobRuntime.check(java.util.UUID.fromString("c0a57f1e-3c36-3755-89c2-20062a2f4849"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("71b976b4-7881-3547-a182-20b2b2e8c212"))) {
t = 0;
}
      deleteTimeHistogram.update(t);
    }
  }

  /**
   * Update the batch Delete time histogram
   * @param t time it took
   */
  public void updateDeleteBatch(long t) {
if(KnobRuntime.check(java.util.UUID.fromString("aa5d85c6-d27f-389d-a04e-5918d002e13a"))) {
return;
}
    if (isEnableTableLatenciesMetrics()) {
if(KnobRuntime.check(java.util.UUID.fromString("4cd34931-2b90-3205-977b-ec9e4757d71c"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("af9ddc38-9017-3745-8f49-4949947627ad"))) {
t = -1;
}
      deleteBatchTimeHistogram.update(t);
    }
  }

  /**
   * Update the Get time histogram .
   * @param time              time it took
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateGet(long time, long blockBytesScanned) {
if(KnobRuntime.check(java.util.UUID.fromString("039576de-ad8e-322b-a3b3-63d0e6bfd66c"))) {
return;
}
    if (isEnableTableLatenciesMetrics()) {
if(KnobRuntime.check(java.util.UUID.fromString("ce251348-f1a7-375e-bb73-d713b99bcca4"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("322ae557-c2f9-3fcd-a606-823da58741d6"))) {
time += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("18607acf-e796-3e6f-a581-d556d56a2014"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("848c4879-87de-33e9-84bf-d70b0cebbfd4"))) {
time = -1;
}
      getTimeHistogram.update(time);
      if (((KnobRuntime.check(java.util.UUID.fromString("6f516ef6-e3df-3735-b92a-fa2bee043b04"))) ? ((blockBytesScanned) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("69780b2a-0137-37df-bc2b-52c3613e75d0"))) ? ((blockBytesScanned) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8bdb2f31-6548-3ac9-b660-dfe478e2f534"))) ? ((blockBytesScanned) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("029ea3f1-f849-3e68-959c-85d19817d9ef"))) ? ((blockBytesScanned) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6049038e-ee9e-31ea-8d08-9cf855ac4f0a"))) ? ((blockBytesScanned) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2f586a6a-7bcd-33ad-9367-aeddd6682376"))) ? ((blockBytesScanned) != (0)) : (blockBytesScanned > 0))))))))))))) {
        blockBytesScannedCount.increment(blockBytesScanned);
        getBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the Increment time histogram.
   * @param time              time it took
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateIncrement(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      incrementTimeHistogram.update(time);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        incrementBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the Append time histogram.
   * @param time              time it took
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateAppend(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      appendTimeHistogram.update(time);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        appendBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the scan metrics.
   * @param time              response time of scan
   * @param responseCellSize  size of the scan resposne
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateScan(long time, long responseCellSize, long blockBytesScanned) {
if(KnobRuntime.check(java.util.UUID.fromString("6dcb3eaf-c781-3dfd-a118-162247550504"))) {
return;
}
    if (isEnableTableLatenciesMetrics()) {
if(KnobRuntime.check(java.util.UUID.fromString("40b511f1-ebad-3e81-b884-183ab60b37a7"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9fc09be1-a1c4-332e-a4b2-8be33775938f"))) {
time = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb23d248-5c0c-3556-9778-953bd96d24f8"))) {
time += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("30302216-6c5a-3d30-9c78-88357dc16c06"))) {
time = 0;
}
      scanTimeHistogram.update(time);
if(KnobRuntime.check(java.util.UUID.fromString("7a9b4c57-f992-3098-8524-f7004a86f07b"))) {
responseCellSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a67450d-38bf-3787-a419-70ea19684b65"))) {
responseCellSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dc960415-6fb1-36b5-9c48-ec9793bfc060"))) {
responseCellSize = 0;
}
      scanSizeHistogram.update(responseCellSize);
      if (((KnobRuntime.check(java.util.UUID.fromString("e95cef12-07aa-39e8-87f4-456d2d5279ca"))) ? ((blockBytesScanned) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3fd06590-8fe8-36bf-9b4e-71dd0ea8170d"))) ? ((blockBytesScanned) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("16753713-ae81-3c10-af41-e4780ef7588a"))) ? ((blockBytesScanned) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("9b7c9ee5-73b8-361c-97c4-80126a8ded6e"))) ? ((blockBytesScanned) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cb759a3c-e6e7-3a3c-940f-d5a6611179f2"))) ? ((blockBytesScanned) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ea3af796-7029-31be-94a2-24f630c56123"))) ? ((blockBytesScanned) > (0)) : (blockBytesScanned > 0))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("fa337431-79fc-36ac-8ae2-5edc154fc60b"))) {
blockBytesScanned = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6f1c5bc9-620e-37ab-958e-0579505d9b16"))) {
blockBytesScanned = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ffb3a16c-b0f9-35dc-9075-b4782545b6b4"))) {
blockBytesScanned += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("41615e74-0903-3fc0-b59d-ccb5b6106058"))) {
blockBytesScanned -= 1;
}
        blockBytesScannedCount.increment(blockBytesScanned);
if(KnobRuntime.check(java.util.UUID.fromString("b94c4f6d-ebab-39a6-a67e-e382ea962d4d"))) {
blockBytesScanned = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("97b14c2b-8751-3df6-9130-1cda3e2c7853"))) {
blockBytesScanned += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("df843b14-142a-33db-a229-25f9f6a27cc9"))) {
blockBytesScanned -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("503cb963-7fba-38fc-b628-5ed977c20211"))) {
blockBytesScanned = 0;
}
        scanBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the CheckAndDelete time histogram.
   * @param time time it took
   */
  public void updateCheckAndDelete(long time) {
    if (isEnableTableLatenciesMetrics()) {
      checkAndDeleteTimeHistogram.update(time);
    }
  }

  /**
   * Update the CheckAndPut time histogram.
   * @param time time it took
   */
  public void updateCheckAndPut(long time) {
    if (isEnableTableLatenciesMetrics()) {
      checkAndPutTimeHistogram.update(time);
    }
  }

  /**
   * Update the CheckAndMutate time histogram.
   * @param time time it took
   */
  public void updateCheckAndMutate(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      checkAndMutateTimeHistogram.update(time);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        checkAndMutateBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update table read QPS
   * @param count Number of occurrences to record
   */
  public void updateTableReadQueryMeter(long count) {
if(KnobRuntime.check(java.util.UUID.fromString("a799506b-5bc3-3a2a-b2d1-dd93f71456fc"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("80c92a75-22eb-36ef-a17a-265f4df8217a"))) ? (isEnableTableLatenciesMetrics()) : (isEnabTableQueryMeterMetrics()))) {
      readMeter.mark(count);
    }
  }

  /**
   * Update table read QPS
   */
  public void updateTableReadQueryMeter() {
    if (isEnabTableQueryMeterMetrics()) {
      readMeter.mark();
    }
  }

  /**
   * Update table write QPS
   * @param count Number of occurrences to record
   */
  public void updateTableWriteQueryMeter(long count) {
if(KnobRuntime.check(java.util.UUID.fromString("78bf43ef-a490-3b66-ba30-2c48915e8d57"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9b883968-d338-32fe-b46e-6516c336f63e"))) ? (isEnableTableLatenciesMetrics()) : (isEnabTableQueryMeterMetrics()))) {
      writeMeter.mark(count);
    }
  }

  /**
   * Update table write QPS
   */
  public void updateTableWriteQueryMeter() {
    if (isEnabTableQueryMeterMetrics()) {
      writeMeter.mark();
    }
  }

  // Visible for testing
  public MetricRegistryInfo getMetricRegistryInfo() {
    return registryInfo;
  }
}
