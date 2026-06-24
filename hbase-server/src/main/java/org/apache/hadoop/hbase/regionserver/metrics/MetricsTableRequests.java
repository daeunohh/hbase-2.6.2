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
    return ((KnobRuntime.check(java.util.UUID.fromString("8cde34fb-2952-387c-a549-96156aec4f7e"))) ? (!enableTableLatenciesMetrics) : (enableTableLatenciesMetrics));
  }

  public boolean isEnabTableQueryMeterMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("872aeec8-47bd-335c-8fbf-f27d69387743"))) {
return true;
}
    return enabTableQueryMeterMetrics;
  }

  public MetricsTableRequests(TableName tableName, Configuration conf) {
    init(tableName, conf);
  }

  private void init(TableName tableName, Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("056c6dc7-2a77-3d04-b3f5-7149337e3812"))) {
return;
}
    this.tableName = tableName;
    this.conf = conf;
    enableTableLatenciesMetrics = this.conf.getBoolean(ENABLE_TABLE_LATENCIES_METRICS_KEY,
      ENABLE_TABLE_LATENCIES_METRICS_DEFAULT);
    enabTableQueryMeterMetrics = this.conf.getBoolean(ENABLE_TABLE_QUERY_METER_METRICS_KEY,
      ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    if (((KnobRuntime.check(java.util.UUID.fromString("d1a8e6e6-e24d-34eb-830f-922ceb2883c7"))) ? ((!enableTableLatenciesMetrics) || (!enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("2fe863ab-6bed-3e47-ad31-a315e9174fea"))) ? (enabTableQueryMeterMetrics) : (enableTableLatenciesMetrics || enabTableQueryMeterMetrics))))) {
      registry = createRegistryForTableRequests();
      if (enableTableLatenciesMetrics) {
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

      if (enabTableQueryMeterMetrics) {
        readMeter = registry.meter(TABLE_READ_QUERY_PER_SECOND);
        writeMeter = registry.meter(TABLE_WRITE_QUERY_PER_SECOND);
      }
    }
  }

  private MetricRegistry createRegistryForTableRequests() {
    return MetricRegistries.global().create(createRegistryInfoForTableRequests());
  }

  private MetricRegistryInfo createRegistryInfoForTableRequests() {
if(KnobRuntime.check(java.util.UUID.fromString("b38a1dc0-2858-3517-8b4a-33022e985b81"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue + 1);
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
    if (((KnobRuntime.check(java.util.UUID.fromString("b6032bca-6f1f-3de1-8a78-d8b3bcaba549"))) ? ((!enableTableLatenciesMetrics) && (enabTableQueryMeterMetrics)) : (((KnobRuntime.check(java.util.UUID.fromString("a85216e9-6e4a-3dbf-96cd-d9c9c5ddc77a"))) ? ((enableTableLatenciesMetrics) && (!enabTableQueryMeterMetrics)) : (enableTableLatenciesMetrics || enabTableQueryMeterMetrics))))) {
      MetricRegistries.global().remove(registry.getMetricRegistryInfo());
    }
  }

  private static String qualifyMetrics(String prefix, TableName tableName) {
if(KnobRuntime.check(java.util.UUID.fromString("b89a68b5-2703-37bb-9698-a48479861be0"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29e31b13-a88e-39e8-9951-13d5e41acff3"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableName.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableName));
    _knob_field_.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    StringBuilder sb = new StringBuilder();
    sb.append(prefix).append("_");
    sb.append("Namespace_").append(tableName.getNamespaceAsString());
    sb.append("_table_").append(tableName.getQualifierAsString());
    return sb.toString();
  }

  /**
   * Update the Put time histogram
   * @param t time it took
   */
  public void updatePut(long t) {
    if (isEnableTableLatenciesMetrics()) {
      putTimeHistogram.update(t);
    }
  }

  /**
   * Update the batch Put time histogram
   * @param t time it took
   */
  public void updatePutBatch(long t) {
    if (isEnableTableLatenciesMetrics()) {
      putBatchTimeHistogram.update(t);
    }
  }

  /**
   * Update the Delete time histogram
   * @param t time it took
   */
  public void updateDelete(long t) {
    if (isEnableTableLatenciesMetrics()) {
      deleteTimeHistogram.update(t);
    }
  }

  /**
   * Update the batch Delete time histogram
   * @param t time it took
   */
  public void updateDeleteBatch(long t) {
    if (isEnableTableLatenciesMetrics()) {
      deleteBatchTimeHistogram.update(t);
    }
  }

  /**
   * Update the Get time histogram .
   * @param time              time it took
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateGet(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
if(KnobRuntime.check(java.util.UUID.fromString("ce251348-f1a7-375e-bb73-d713b99bcca4"))) {
time = 0;
}
      getTimeHistogram.update(time);
      if (((KnobRuntime.check(java.util.UUID.fromString("8bdb2f31-6548-3ac9-b660-dfe478e2f534"))) ? ((blockBytesScanned) <= (0)) : (blockBytesScanned > 0))) {
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
    if (isEnableTableLatenciesMetrics()) {
      scanTimeHistogram.update(time);
if(KnobRuntime.check(java.util.UUID.fromString("7a9b4c57-f992-3098-8524-f7004a86f07b"))) {
responseCellSize = -1;
}
      scanSizeHistogram.update(responseCellSize);
      if (((KnobRuntime.check(java.util.UUID.fromString("16753713-ae81-3c10-af41-e4780ef7588a"))) ? ((blockBytesScanned) >= (0)) : (blockBytesScanned > 0))) {
if(KnobRuntime.check(java.util.UUID.fromString("6f1c5bc9-620e-37ab-958e-0579505d9b16"))) {
blockBytesScanned = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa337431-79fc-36ac-8ae2-5edc154fc60b"))) {
blockBytesScanned = 0;
}
        blockBytesScannedCount.increment(blockBytesScanned);
if(KnobRuntime.check(java.util.UUID.fromString("b94c4f6d-ebab-39a6-a67e-e382ea962d4d"))) {
blockBytesScanned = -1;
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
    if (isEnabTableQueryMeterMetrics()) {
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
    if (isEnabTableQueryMeterMetrics()) {
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
