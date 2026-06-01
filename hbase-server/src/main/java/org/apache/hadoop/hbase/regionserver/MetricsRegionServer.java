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
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Maintains regionserver statistics and publishes them through the metrics interfaces. This class
 * has a number of metrics variables that are publicly accessible; these variables (objects) have
 * methods to update their values. Batch your updates rather than call on each instance else all
 * threads will do nothing but contend trying to maintain metric counters!
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsRegionServer {
  public static final String RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY =
    "hbase.regionserver.enable.server.query.meter";
  public static final boolean RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY_DEFAULT = true;

  public static final String SLOW_METRIC_TIME = "hbase.ipc.slow.metric.time";
  private final MetricsRegionServerSource serverSource;
  private final MetricsRegionServerWrapper regionServerWrapper;
  private final MetricsTable metricsTable;
  private final MetricsUserAggregate userAggregate;
  private MetricsRegionServerQuotaSource quotaSource;

  private MetricRegistry metricRegistry;
  private Timer bulkLoadTimer;
  // Incremented once for each call to Scan#nextRaw
  private Meter serverReadQueryMeter;
  // Incremented per write.
  private Meter serverWriteQueryMeter;
  protected long slowMetricTime;
  protected static final int DEFAULT_SLOW_METRIC_TIME = 1000; // milliseconds

  public MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper, Configuration conf,
    MetricsTable metricsTable) {
    this(regionServerWrapper,
      CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .createServer(regionServerWrapper),
      metricsTable, MetricsUserAggregateFactory.getMetricsUserAggregate(conf));

    // Create hbase-metrics module based metrics. The registry should already be registered by the
    // MetricsRegionServerSource
    metricRegistry = MetricRegistries.global().get(serverSource.getMetricRegistryInfo()).get();

    // create and use metrics from the new hbase-metrics based registry.
    bulkLoadTimer = metricRegistry.timer("Bulkload");

    slowMetricTime = conf.getLong(SLOW_METRIC_TIME, DEFAULT_SLOW_METRIC_TIME);
    quotaSource = CompatibilitySingletonFactory.getInstance(MetricsRegionServerQuotaSource.class);
    if (
      conf.getBoolean(RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY,
        RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY_DEFAULT)
    ) {
      serverReadQueryMeter = metricRegistry.meter("ServerReadQueryPerSecond");
      serverWriteQueryMeter = metricRegistry.meter("ServerWriteQueryPerSecond");
    }
  }

  MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper,
    MetricsRegionServerSource serverSource, MetricsTable metricsTable,
    MetricsUserAggregate userAggregate) {
    this.regionServerWrapper = regionServerWrapper;
    this.serverSource = serverSource;
    this.metricsTable = metricsTable;
    this.userAggregate = userAggregate;
  }

  public MetricsRegionServerSource getMetricsSource() {
if(KnobRuntime.check(java.util.UUID.fromString("f565e849-fa8e-386d-bd73-2064c5162529"))) {
return null;
}
    return serverSource;
  }

  public MetricsUserAggregate getMetricsUserAggregate() {
if(KnobRuntime.check(java.util.UUID.fromString("44cf7127-3b3f-30d3-b6ad-ea392637b85b"))) {
return null;
}
    return userAggregate;
  }

  public MetricsRegionServerWrapper getRegionServerWrapper() {
if(KnobRuntime.check(java.util.UUID.fromString("ee2e5baf-e1ee-3a91-a769-626461ba689b"))) {
return null;
}
    return regionServerWrapper;
  }

  public void updatePutBatch(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("fe591864-86a6-3adb-8b88-6c26a3cc2f40"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0dcb2723-8ab8-311a-aa64-c0647e226def"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b995d83-678d-3d27-b11b-82066800b19a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fa6496a-c143-3337-8962-4f64feeb06ab"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66b88459-cf94-3f24-8f57-4b041ae9445f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("059eb5ac-6446-3423-91eb-2d458bb79dcb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("845873da-9693-30e4-bab8-b6efea55d955"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2be6e350-02ed-3560-936d-8408bceb7827"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4a66b68-efc4-3555-98a7-0b238fbc84ab"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b3030b8-ccaa-32d2-97b6-9835b173b6f0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb4579da-2838-3c31-91db-9e29aafa8b5d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7d1f2ff-204f-3839-bd7e-136148f1a7f0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b3b8957-3879-314b-b861-c2836f2d8024"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f9c64c2-99b0-3934-a5e5-a24a354fa331"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd227aff-a381-3748-a727-474fc6bcabfb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e47d5208-3463-30b5-a06e-3ea0a370ee9c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9ca9de2-5c8d-3d6e-b8c6-596e5568eecd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9a85ef47-07c0-355c-9c31-08ef3361aaad"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("727f16af-2a92-32d8-b27d-c32b75756c76"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("194c2a96-e5b2-3505-9482-cb82a4c2571e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b7d924d-a212-3505-90c1-ec80987f57ba"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("03ed95b3-ef97-3f46-9cb8-1507d5d2eb51"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("584b92f1-1eb5-3158-bacc-352c31f376a2"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c75967a1-1d24-3e16-b420-65f914171443"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f0fb44f-a298-3f67-989d-4d6d4a441587"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("506ef045-8057-3da4-b8e1-7c479ce14324"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c21a6d75-cd95-3f07-8a38-37253da8e736"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a822f01-4ca2-30b8-81e0-1f8732ae2939"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c5c0334-6006-3c64-9dbb-e39b58a23ace"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac2105ab-2326-3b53-b2e2-6edd05e25f4b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c46423c-76a1-373a-90c3-40ab5c7cec32"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("52a4bf4b-ff08-3d0f-88dc-e9c0b71dbef8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ab0a639-24f4-3648-8776-6b30bfdc4331"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37ed30cb-1925-3774-8c2e-68cc90dee8da"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f35f54df-b7f5-3366-adb1-662745a36b21"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15db47af-2401-36d2-a416-2d6970bf745b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cf481d45-fa87-314b-bc48-5531beba48b3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("263d1b8e-5e11-3857-847c-e873f6a6deee"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("6b60509f-28a8-3bb1-9abf-4788ee6fce01"))) ? ((region.getMetricsTableRequests()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b70ee087-ad91-33d4-89b7-72868d6e0b83"))) ? ((region.getMetricsTableRequests()) != (null)) : (region.getMetricsTableRequests() != null))))) {
      region.getMetricsTableRequests().updatePutBatch(t);
    }
    serverSource.updatePutBatch(t);
  }

  public void updatePut(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("d4c8cf59-2819-3c98-a35a-7b0727cf921a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("500f93af-fce5-3c4f-b7be-b2fee45db093"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f67d24f5-962a-38e5-84db-15b2be882272"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4109378d-9ea2-3551-8d20-5e01dcd6a745"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cabc51bd-fc9f-3a7d-bddf-c98260a1b674"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ecee2c92-b7b0-3640-94c9-053e7b382e95"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5ee6be6-012e-3d91-b809-97c78c32c8fd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dae9439f-80ef-3a3c-81a2-dc444f51e805"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbe22bd4-13ef-364b-ba61-5bbccc7ff8c6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe22e2f4-a44c-3312-a874-f456979ae5a0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5a0c714-7c13-320c-a6b5-69d1baaca153"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30ed1426-7246-390e-a74d-d02f3267577b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("325f06e1-5df4-309b-b6d3-5826467c4506"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0230a1a2-d9b3-3149-a404-e2ae56972eb2"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("735d7831-66fc-30dd-bd70-55b6d38d923f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fb85805-43c9-33f9-86e5-2e0f817d54c6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b16f812d-91a0-38e7-b2b2-1b857793252e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ca87ee1-87b7-3a74-9a35-82777c0c1d0b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6dda3bc9-cc77-38cf-a658-e33f7596ffb7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ca987e75-8999-3d6e-b95f-7c050f0650eb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6bb71be8-ba6d-3f2d-abc4-ee9e7119e1e7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be6e1359-1494-3a85-aae6-7b88b797c090"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6405cab7-0c8d-329d-bf41-ab06a6760028"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9212c8c2-e42d-3678-9475-f1e18d8be6c0"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8bc1a79-793e-3e76-ab37-e6079cbaed00"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b0f3de69-6316-30dc-aff1-f76bffd436d5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("923d8066-b22d-370b-9b5a-c896f61c289b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04a6db18-e6b6-3455-ad55-e3b6c2b9c99a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e793845d-2a34-3eba-b22a-c16f1533758f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c30e933-e5cf-3ed9-925a-2bc273a6de0e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77c78b94-5a8e-31af-bf65-061f0334b6b5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("433d1dc4-f286-3804-b3f4-aa794da9a7a5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e38d16a-9c02-3188-91a2-52af9e310024"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7217b471-0b18-3042-889d-30ba29f90381"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c2ed8d1-44c5-38e3-b928-566d9b3f6c98"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ff19179-f67e-3b72-b305-40687816bb2f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4761a363-a4ed-3b2f-8ed2-f92da27b6356"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4dc63354-7961-39bc-bb60-9a719ac918f0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("3b64dfe0-9afa-3cc9-919a-6e1fa513cef2"))) ? ((region.getMetricsTableRequests()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cfc90649-f65f-3862-8a56-df1c72b36974"))) ? ((region.getMetricsTableRequests()) != (null)) : (region.getMetricsTableRequests() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("7eb86090-eaf8-3ffb-b200-b2016a28649c"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("385022d9-c3df-3fb3-b860-e45e2f6e3192"))) {
t = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("5762cfab-e892-3765-aef9-7747567b770a"))) { region.getMetricsTableRequests().updateDelete(t); } else if (KnobRuntime.check(java.util.UUID.fromString("3b88d19e-e0e7-3cfd-8cd8-dd47e7f89337"))) { region.getMetricsTableRequests().updatePutBatch(t); } else { region.getMetricsTableRequests().updatePut(t); }
    }
    if (t > slowMetricTime) {
      if (KnobRuntime.check(java.util.UUID.fromString("47ba9e64-7f12-31de-b04d-5e26ded4ac79"))) { serverSource.incrSlowDelete(); } else if (KnobRuntime.check(java.util.UUID.fromString("ccd16dc7-e47f-3f15-8f56-804b657323ca"))) { serverSource.incrSlowGet(); } else { serverSource.incrSlowPut(); }
    }
    serverSource.updatePut(t);
if(KnobRuntime.check(java.util.UUID.fromString("41b753a9-e73b-39e4-8399-8145eb7ab889"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3504a1a7-da57-3dcb-b15e-70bf27a7dbea"))) {
t = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("3636a8a7-c28d-3db3-b27a-9781d74c2f61"))) { userAggregate.updateDelete(t); } else if (KnobRuntime.check(java.util.UUID.fromString("54689674-43fa-3224-b4bb-e66850ead725"))) { userAggregate.updateCheckAndMutate(t); } else { userAggregate.updatePut(t); }
  }

  public void updateDelete(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("34fb4170-3a17-3c9e-8c68-e819e0ac3b64"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64859db8-cb7a-3cd7-8320-e1f9f13f2b39"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a5b1d7c-87a6-37b0-89f7-12d8b9f863dd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("08c488c3-2cc2-3aa3-ac47-b84a5a7ac7b6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("24090f12-c27a-3ea4-829d-2ad4a23306a0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b21ec910-b0da-3e34-8745-f59dbda712ce"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4389d7e1-117d-38cb-8167-6604c9477ca9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("94ac4651-845c-313c-aa1e-a0e73933769c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c0b7eeb-dcd8-3e26-ac26-362606e29e3f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1f0777f-c090-326f-8bda-f19cec9a78d7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8408daae-235e-3668-b180-405dc48f5755"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e99a790-91fb-3525-a6f9-afa451f7fa7e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27f51e85-7222-31d7-a861-35aabc16728e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d8ae29e0-8c31-335f-9241-cfa4f07e0dae"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c411558-9123-3e62-a5bd-51716dda4b64"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd905d45-08d4-3901-acdb-39d814d433ae"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("616831e5-2a65-3f1d-a306-abda3a87f278"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f55813eb-d7ed-33be-b618-97b55cd3296a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dccaa6f5-79dc-3654-aa10-ccb871824ba8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef731cff-71cb-3188-9721-66dc0fc1497e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5aca5fd-26b2-356b-ac2b-a98930457885"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a7e2e19-147f-3541-8f7b-e5f7582df763"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a14048a7-9ebd-3793-bf7f-7d0420990e30"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40effc09-a08b-3f7f-8ac5-2f51a06e0151"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b4ce8627-3170-368e-8f31-4800ff489b39"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82c7a431-7870-3da3-ae81-c524905f8749"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49a94384-2daf-398f-aa41-f888aedb093a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da1dbfb0-a22b-3034-808a-346018098edf"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d6629c5-dd7e-3bab-b404-553f625cdb19"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0abc98f0-675b-3653-941b-47073fae317f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f61b1f1c-8b22-3044-9d34-b7db4618b023"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b89fb35-53a4-3d58-9d6f-228167d7cdc2"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("42a67d0c-044e-36b4-a95b-feed2a4e57ae"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("00eb8a63-c3da-37d1-9a19-2e12f898aeed"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4b5465c-2225-31c7-8109-3e315f201e3a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c3ccd3b-0c0f-3d3f-b842-b815b318039f"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b58e0f82-259d-3bdd-87b8-c78eacc52b5d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7bd3f739-385d-3ac2-b8f2-0c0980af0ac0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("8e5e0a80-2ae2-3c08-9b38-75ac4cb784a8"))) ? ((region.getMetricsTableRequests()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b3bfbdff-717e-3aaa-befc-30a21a6b958e"))) ? ((region.getMetricsTableRequests()) != (null)) : (region.getMetricsTableRequests() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("45f7cffc-bd3c-3141-83bc-ae37fb7a5610"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7a73d990-0523-3cf8-a5b8-e9a1f512855d"))) {
t = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("0135031d-4b96-35c5-aaf3-363e622d7d2c"))) { region.getMetricsTableRequests().updatePutBatch(t); } else if (KnobRuntime.check(java.util.UUID.fromString("acbe0645-ed2c-3d1b-b306-c8083ee1bab7"))) { region.getMetricsTableRequests().updatePut(t); } else { region.getMetricsTableRequests().updateDelete(t); }
    }
    if (t > slowMetricTime) {
      if (KnobRuntime.check(java.util.UUID.fromString("7da9f8cc-b6e8-3141-8cd3-9d369995920e"))) { serverSource.incrSlowPut(); } else if (KnobRuntime.check(java.util.UUID.fromString("a98a16d9-ce1d-33c7-80ce-2daf9eb2124d"))) { serverSource.incrSlowGet(); } else { serverSource.incrSlowDelete(); }
    }
    serverSource.updateDelete(t);
if(KnobRuntime.check(java.util.UUID.fromString("f4b2eb9c-cd64-321f-8dbf-9405ba0b6d3a"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d54bfb36-ad61-3993-870d-a7e2bf45ee4e"))) {
t = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("ed4fe568-e5cf-36f7-adcb-28966543e0f6"))) { userAggregate.updatePut(t); } else if (KnobRuntime.check(java.util.UUID.fromString("a4504665-551c-3307-808c-82d17a25ac21"))) { userAggregate.updateCheckAndMutate(t); } else { userAggregate.updateDelete(t); }
  }

  public void updateDeleteBatch(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("d67b4f28-e836-3144-a3f3-e627d147b576"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("581aacac-9933-3a8e-af35-f71818f275f7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6701bb7-6d09-3396-9db8-2d2efd1d96a8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a39745f0-a74f-338e-9441-6493ffa58752"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2942c176-5a35-335b-8c79-b4eadd15138d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("420785a7-db5e-39b5-9d5f-c7594e64ef6e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b1b2da6-4dfe-3d25-b470-e5e808ed733b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5568372d-830b-3d67-9332-5b259765c08a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86768f8e-2b70-3408-8761-90d583102872"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("d6522cb8-020f-369c-99c3-5612da6f6d2d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3432878d-0628-37a2-affd-46158ba7aae5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d490155-b5de-3c27-aa0f-c5b46863b163"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ec42a68-67ab-335c-8f7b-716a1ad6966d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f0f1883-5aed-3326-8445-036ba5b827b6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25aca20e-317f-3c7c-84c5-b0281cc858f7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d38b605a-b252-3003-8302-c22627aa430b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8528222e-31c2-30ed-a04c-3fa30514ee71"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3d2b11db-bbdf-3264-b176-bcc9f4eab887"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be01252e-bd23-3683-a6bd-363d38f53f5d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c61086be-5612-3d10-97c6-8ad9bfa31e7d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9cc9b09-7d38-3bca-b8ed-0b5b2648edcc"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2de5512c-6474-3425-8600-dac95aefd2cb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("23c67604-7f2e-3111-b33d-62060ab71ebd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c92a942-1791-3f17-99a8-dc7d6483486b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c478e5f-fe80-3385-b348-39e9bcbfa30c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("90effa00-1e42-39e7-a0f3-594f0726eb3a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afe48088-b681-36e1-ab67-d7cb0c7ce9df"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("572df18e-0e35-3ccf-9dd1-7e64e6dfacfd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bae58266-00c0-36cc-bc1f-91a056a7192e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("92bce34e-dc07-3d16-98db-47fb0bd7bffb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d1bc81e-008a-3ba7-ace6-d2a13ddd0bdb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("64e57985-0582-3564-9b33-1e31b7730923"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5ac7112-ebbb-300f-8bbd-eca6a35c39f6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec3dc222-b92b-386d-8750-ead583ccf478"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0bc552b3-2d7f-3fad-baea-2452cb6556dd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c92ccece-1c7a-36dd-b113-125b5d597011"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1efd26a-2c98-3269-86f2-4b5223f94fe4"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c075ee9b-4035-327e-ae56-f367bbf8566c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("49f7f8b0-9ac3-3e2f-8fe4-c0d5f60becd7"))) ? ((region.getMetricsTableRequests()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("21c3cc67-e8a2-3dac-8692-dfc3aa072b8c"))) ? ((region.getMetricsTableRequests()) == (null)) : (region.getMetricsTableRequests() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("109fbdab-2eb2-392f-a219-b29bd61c2e06"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b073c750-9b60-3e2f-a315-d149592e0709"))) {
t = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("1cd52981-fe9b-39ab-ba34-2cc4f351ed21"))) { region.getMetricsTableRequests().updateDelete(t); } else if (KnobRuntime.check(java.util.UUID.fromString("cbecf5e2-356c-30e9-b3ca-b3e76e0a2d75"))) { region.getMetricsTableRequests().updatePutBatch(t); } else if (KnobRuntime.check(java.util.UUID.fromString("5142438f-9e8f-3401-bd1d-8f169156e625"))) { region.getMetricsTableRequests().updatePut(t); } else { region.getMetricsTableRequests().updateDeleteBatch(t); }
    }
    serverSource.updateDeleteBatch(t);
  }

  public void updateCheckAndDelete(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("9966374a-814a-3158-acf0-7df249f71b07"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79a91024-c63f-3797-ba45-3b66b1d9bdaf"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("108b9f58-0d23-3533-9e3a-de1ad466a3fa"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("368296d0-d8ae-3bb9-8e34-4647be2d4ba9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0cdd53c7-4487-3e32-990d-9e5ab874a8c5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12eaad6a-e0b1-3693-ac80-721e657c8810"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b873cb9a-e0ec-305b-9548-069de10acd63"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("810d16d7-b5ae-3c00-a721-0d66df8befdb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eff9d1c2-baf0-3f68-877d-ed7b1e083aa3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb713090-efbb-3af3-b8ee-a936927a8c6b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("039b9da5-6c59-3810-8cbc-70aa4b93026e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86cc0679-8de1-33fb-9f0d-ac8690543a40"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb0e3331-704b-3b53-b0a9-718204227209"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb7256e8-da88-3532-a54b-3629b911b02c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9ed5a32-3cc6-38d1-b106-7979a4a40ae3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8545faa2-ae4d-351a-aeca-3b6a39124608"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5c557021-704a-37aa-ad0f-813ad85b1591"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe602134-a6bc-304d-adf2-7999fbed15c5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("19820ce3-60fb-3f49-b259-0d7817749b19"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("875e51e6-832c-389a-b566-2e2835aaae8b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e47fb1ad-979e-3e17-b0e9-6eb39a738323"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbeb9341-c53a-36a9-b191-ba0b67c8d195"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("657d8e7d-d3b5-347b-87c9-2fbc1c88a8c3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41c6742f-4ac1-3ba8-bd88-899b5f6204e3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("97b65624-78d7-3bae-8783-d957e4b953d0"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f64d8ad3-9448-3004-b3a6-49626983bc03"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6bf00158-5fe8-33da-9890-9b5b0d727bdb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c84054f7-a8bd-3ba9-a48b-7525d84170ed"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d95fdc47-c5dc-3d50-8cc3-38bdef2da037"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3daaa2ab-0a30-3448-b189-14f7d348dfd9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a9a535e-101a-3566-ba0a-c2f698d29229"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01a9e0de-e85a-368d-97e8-025a42f7749a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ffe2738-5f37-352b-aa27-dc6706ef5822"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dcb8c6a3-54f9-3f0c-b856-e0251c0e729a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a26699dd-f916-329c-966f-135cf5f75a6d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b40cb530-7ebc-39cd-8df7-36fb314c24c3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c815207-218a-31f8-ab89-026c01193a85"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35127e93-a749-3b9b-9080-4e8fc686f6f1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("764fab18-b97e-34a8-be05-321a6d7a4012"))) ? ((region.getMetricsTableRequests()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ddc5401a-3d16-3556-bd4f-6ebd6825ca35"))) ? ((region.getMetricsTableRequests()) == (null)) : (region.getMetricsTableRequests() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("3f7d615f-d492-3e80-a419-211b2c59cbde"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("842f2f23-fcc4-30ab-9568-8b98287809ba"))) {
t = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("f62cb8d7-7f3c-33c3-be86-6cb40fc29eee"))) { region.getMetricsTableRequests().updatePutBatch(t); } else if (KnobRuntime.check(java.util.UUID.fromString("f087d2e6-98e0-388a-8ce4-d5f230c66e61"))) { region.getMetricsTableRequests().updatePut(t); } else if (KnobRuntime.check(java.util.UUID.fromString("7a792a6d-2e4e-3299-be63-ede9e21218f2"))) { region.getMetricsTableRequests().updateDelete(t); } else { region.getMetricsTableRequests().updateCheckAndDelete(t); }
    }
    serverSource.updateCheckAndDelete(t);
  }

  public void updateCheckAndPut(HRegion region, long t) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateCheckAndPut(t);
    }
    serverSource.updateCheckAndPut(t);
  }

  public void updateCheckAndMutate(HRegion region, long time, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateCheckAndMutate(time, blockBytesScanned);
    }
    serverSource.updateCheckAndMutate(time, blockBytesScanned);
    userAggregate.updateCheckAndMutate(blockBytesScanned);
  }

  public void updateGet(HRegion region, long time, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateGet(time, blockBytesScanned);
    }
    if (time > slowMetricTime) {
      if (KnobRuntime.check(java.util.UUID.fromString("d3c9cf46-0213-37a2-a407-dd28ffaf39fd"))) { serverSource.incrSlowDelete(); } else if (KnobRuntime.check(java.util.UUID.fromString("cec912ff-90d4-31fb-8ca0-5c3d41ba0924"))) { serverSource.incrSlowPut(); } else { serverSource.incrSlowGet(); }
    }
    serverSource.updateGet(time, blockBytesScanned);
if(KnobRuntime.check(java.util.UUID.fromString("00096d0c-8e4b-3298-a0ae-6cd9b913f055"))) {
blockBytesScanned = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("34702fa4-f0da-3586-9af6-ceb713c0c137"))) {
time = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f37ce974-ed68-35ef-a029-ef9153602a7c"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("77a38b96-db71-3cfe-91e9-506ef3fec803"))) {
blockBytesScanned += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("04525083-841d-35cd-9cf0-d0c8c03c6d94"))) {
blockBytesScanned -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8447a881-0bc1-34e6-a112-aab81deac418"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c13979b-cc6f-3378-82f9-108fda8cec8e"))) {
time += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b919f409-a30a-3244-8a60-fc0fe41973a3"))) {
blockBytesScanned = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("e242780b-ab9e-38c2-a931-f3f325784ccc"))) { userAggregate.updateIncrement(time, blockBytesScanned); } else if (KnobRuntime.check(java.util.UUID.fromString("a3962357-87de-3e82-835d-0105d0700fdc"))) { userAggregate.updateAppend(time, blockBytesScanned); } else { userAggregate.updateGet(time, blockBytesScanned); }
  }

  public void updateIncrement(HRegion region, long time, long blockBytesScanned) {
if(KnobRuntime.check(java.util.UUID.fromString("39763b4f-e063-3911-b2d8-5a726366de40"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fd92b51-d71b-37f6-8d0d-e20d107d4330"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed926f07-8154-3cb6-825c-3afe71c86486"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1bdf4eb4-18ef-3639-9948-bc9ac2b66cfb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f4b648b-3ce6-35ee-8a6f-85f064d2d30f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a3eafb1-48d1-3842-adf4-0b22a57778ef"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7e127a3-5947-356e-b4d7-e77b078db7ea"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ee64be3d-d8bf-32b9-9ea8-8f23684a42d6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c3449ad-19a8-314f-923b-d82f217a1d22"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("97f2c67c-541b-3797-bff4-f8e848e933e5"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd9c7dd5-9a25-32fc-921c-ba0b6641d670"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5ee59998-edcc-341d-a0ed-51efe035d011"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ec12e21-1747-3ebf-9b45-c16db11393f7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1167e53-9ee7-38cd-a1a4-6da1eeaaee62"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8cb7f670-14ec-3dac-adf0-458ce95fe74f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e95ae8e0-2077-3f1c-a8c9-38259424204e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60ab5520-f3eb-39f0-872f-78c10783519a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d710a72-04d7-3fd1-a115-86bfde6cb31a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("066f9033-48fe-382b-b6e9-8e7ae794b2b8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a41d7623-9f2b-3a27-b5d7-91444174927f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("873fd23e-7b9c-38a7-a329-87267d5d7def"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6fff0ae2-518a-3a6a-9c87-20abcec548ad"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb528a3a-20d1-3c2a-8f1b-b484aef8040e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5ef01d41-4c58-351c-9541-95e122bb4150"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c94935b6-e72e-3fb0-8e16-05d5a48b708c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("442e47a0-1431-3b76-8d7a-fa027f0026c9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c819cd0-cc79-38f1-b6a2-c201103f6159"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb6134ae-d2fd-36a3-96a5-f69e957442f3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b543d3e-057a-392e-8d75-d9bc51dbdd9c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e88fc74-6f0a-38a2-b76e-f1434831c5fe"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4863c76b-e9e0-38b0-ba1b-dda4a1ad9138"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d53f8a4f-a207-361a-8c26-0753a42f31fb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d1dabd9-69fb-3bf3-b274-c2060e2f0017"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a4f1b67-512a-3eaf-aebd-09a85ced742f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1055708-c708-3fd4-bd71-e961162c49c1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ecd99170-0051-3cea-a7d6-90f63164e868"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95d007da-f1f6-3eba-8251-2979c009f71f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29234d1c-258a-3825-899b-eca79b035523"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("c6edefc8-de90-3ca8-991c-56377b6d5186"))) ? ((region.getMetricsTableRequests()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("119a2066-1fd3-3e5e-8cd8-c8b513948b4b"))) ? ((region.getMetricsTableRequests()) != (null)) : (region.getMetricsTableRequests() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("1b51b9ff-0078-31da-8b86-915313a1a297"))) {
blockBytesScanned += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("64eccc6b-d5e2-3e7c-8fc9-cdde8036e11b"))) {
time = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2c6c51ac-e4d0-34b4-a92d-6862f3d262fb"))) {
blockBytesScanned = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("54b53abe-0aa3-3167-a43b-7b338159a5b7"))) {
blockBytesScanned -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab4e8246-80dd-36fd-a137-26274d8a5d13"))) {
time += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5ef738ee-c714-3fe4-9c34-516783cf050a"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("26b46ca5-f44a-3f14-9138-4f3009f1a466"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b47ff486-7a44-3291-8071-370f763afc12"))) {
blockBytesScanned = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("1b859fc2-dccc-3f1e-8615-66b0931d0eaa"))) { region.getMetricsTableRequests().updateCheckAndMutate(time, blockBytesScanned); } else if (KnobRuntime.check(java.util.UUID.fromString("7c767fdb-4ea6-3810-9dca-a30d4642eca4"))) { region.getMetricsTableRequests().updateGet(time, blockBytesScanned); } else { region.getMetricsTableRequests().updateIncrement(time, blockBytesScanned); }
    }
    if (time > slowMetricTime) {
      serverSource.incrSlowIncrement();
    }
    serverSource.updateIncrement(time, blockBytesScanned);
    userAggregate.updateIncrement(time, blockBytesScanned);
  }

  public void updateAppend(HRegion region, long time, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateAppend(time, blockBytesScanned);
    }
    if (time > slowMetricTime) {
      serverSource.incrSlowAppend();
    }
    serverSource.updateAppend(time, blockBytesScanned);
    userAggregate.updateAppend(time, blockBytesScanned);
  }

  public void updateReplay(long t) {
    serverSource.updateReplay(t);
    userAggregate.updateReplay(t);
  }

  public void updateScan(HRegion region, long time, long responseCellSize, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateScan(time, responseCellSize, blockBytesScanned);
    }
    serverSource.updateScan(time, responseCellSize, blockBytesScanned);
if(KnobRuntime.check(java.util.UUID.fromString("7e0352be-c549-3b6b-bee4-7ab81ca7ae4b"))) {
blockBytesScanned = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd55ba4d-9bd5-3796-a920-53ba1873ac93"))) {
time = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("27da126c-f0c9-320a-9bac-e7c9d843b3ed"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9e644938-fda5-33a8-ac41-841675abdc59"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bbb1e2c1-dbe7-3727-8cc2-9414fe6867c8"))) {
blockBytesScanned -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab0ec1e9-a8ab-35f6-986a-4624e17e9f3d"))) {
blockBytesScanned += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5c7dc8ae-9ab6-3f31-935b-c255c42afd15"))) {
blockBytesScanned = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("86ae7793-84f2-37cc-8422-7fa7c9f8209c"))) {
time += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("88bfd6c9-674f-3fce-bf65-1cf646a42e1f"))) { userAggregate.updateAppend(time, blockBytesScanned); } else if (KnobRuntime.check(java.util.UUID.fromString("55e03bd4-c1c9-3183-b518-3669ed8cbe8a"))) { userAggregate.updateIncrement(time, blockBytesScanned); } else if (KnobRuntime.check(java.util.UUID.fromString("d008e7dd-542c-3a2b-97e3-91c008933175"))) { userAggregate.updateGet(time, blockBytesScanned); } else { userAggregate.updateScan(time, blockBytesScanned); }
  }

  public void updateSplitTime(long t) {
if(KnobRuntime.check(java.util.UUID.fromString("4f6e4c68-0c1b-3f46-9c85-897d2e3dc56b"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b954b09-2fec-3d20-8c74-4698b403b02d"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd3b9494-894c-326f-aedf-361d4c8e3aaa"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("5df234e4-3cfb-3741-ad77-d4d7ca1c7354"))) { serverSource.updatePut(t); } else if (KnobRuntime.check(java.util.UUID.fromString("49b5a546-90e1-321b-9cde-c75023d4f14c"))) { serverSource.updatePutBatch(t); } else if (KnobRuntime.check(java.util.UUID.fromString("2e720412-5fa0-3c5b-913f-f0b7e364f42c"))) { serverSource.updateDelete(t); } else { serverSource.updateSplitTime(t); }
  }

  public void incrSplitRequest() {
    serverSource.incrSplitRequest();
  }

  public void incrSplitSuccess() {
    serverSource.incrSplitSuccess();
  }

  public void updateFlush(String table, long t, long memstoreSize, long fileSize) {
    serverSource.updateFlushTime(t);
    serverSource.updateFlushMemStoreSize(memstoreSize);
    serverSource.updateFlushOutputSize(fileSize);

    if (((KnobRuntime.check(java.util.UUID.fromString("6b5dd47f-3614-3ceb-bbe1-1518e6d4da67"))) ? ((table) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5337b8e8-5d1e-36e8-8b02-c6f103878e5b"))) ? ((table) != (null)) : (table != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("09f11279-32d1-3c02-98e5-f4ce52f10194"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f4b32f96-7eea-3b75-840e-2b7e20162896"))) {
t = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("2055ab09-02f7-3ffe-8e1e-5fdd4a74c495"))) { metricsTable.updateFlushOutputSize(table, t); } else if (KnobRuntime.check(java.util.UUID.fromString("113464b2-dd3c-3023-8854-1169b523f4e3"))) { metricsTable.updateFlushMemstoreSize(table, t); } else { metricsTable.updateFlushTime(table, t); }
if(KnobRuntime.check(java.util.UUID.fromString("8192c417-9224-3e9a-8863-3b7af6602257"))) {
memstoreSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ffe12047-3998-38b2-a090-b698f5248e87"))) {
memstoreSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("150ef220-4160-3a9f-ac97-1e5acd2b9689"))) {
memstoreSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("645a30c3-8b7b-3c05-ba2f-5ceb90fed769"))) {
memstoreSize -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("8df003fb-26da-3aa6-be07-2bd53c158f93"))) { metricsTable.updateFlushOutputSize(table, memstoreSize); } else if (KnobRuntime.check(java.util.UUID.fromString("9c430277-9026-3839-abb0-59aa41c1af62"))) { metricsTable.updateFlushTime(table, memstoreSize); } else { metricsTable.updateFlushMemstoreSize(table, memstoreSize); }
      metricsTable.updateFlushOutputSize(table, fileSize);
    }

  }

  public void updateCompaction(String table, boolean isMajor, long t, int inputFileCount,
    int outputFileCount, long inputBytes, long outputBytes) {
if(KnobRuntime.check(java.util.UUID.fromString("c9cabbf9-a2d7-3a02-9039-8f6542850832"))) {
isMajor = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9ba6d410-d311-3530-9bf8-5225a46ad2ea"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d572de23-4408-3bd6-8295-60b751251d88"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9dc24508-bf68-358c-9e2a-5206867bb742"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f87f9ba1-fdb2-3f87-97eb-ca6647b66ce2"))) {
isMajor = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("b5f18562-2e1b-3fbc-8763-dc4349612c5c"))) {
isMajor = !isMajor;
}
    if (KnobRuntime.check(java.util.UUID.fromString("f24dca61-8437-371b-b481-61bdcaeba722"))) { serverSource.updateCompactionTime(!isMajor, t); } else if (KnobRuntime.check(java.util.UUID.fromString("fffba14d-9682-34ae-8c16-3f31e34c28a6"))) { serverSource.updateCompactionOutputFileCount(!isMajor, t); } else if (KnobRuntime.check(java.util.UUID.fromString("afcb9c13-50f7-38b9-aafe-53aa00e48ade"))) { serverSource.updateCompactionInputFileCount(!isMajor, t); } else { serverSource.updateCompactionTime(isMajor, t); }
    serverSource.updateCompactionInputFileCount(isMajor, inputFileCount);
    serverSource.updateCompactionOutputFileCount(isMajor, outputFileCount);
    serverSource.updateCompactionInputSize(isMajor, inputBytes);
    serverSource.updateCompactionOutputSize(isMajor, outputBytes);

    if (table != null) {
      metricsTable.updateCompactionTime(table, isMajor, t);
      metricsTable.updateCompactionInputFileCount(table, isMajor, inputFileCount);
      metricsTable.updateCompactionOutputFileCount(table, isMajor, outputFileCount);
      metricsTable.updateCompactionInputSize(table, isMajor, inputBytes);
      metricsTable.updateCompactionOutputSize(table, isMajor, outputBytes);
    }
  }

  public void updateBulkLoad(long millis) {
    this.bulkLoadTimer.updateMillis(millis);
  }

  public void updateReadQueryMeter(HRegion region, long count) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateTableReadQueryMeter(count);
    }
    if (serverReadQueryMeter != null) {
      serverReadQueryMeter.mark(count);
    }
  }

  public void updateWriteQueryMeter(HRegion region, long count) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateTableWriteQueryMeter(count);
    }
    if (serverWriteQueryMeter != null) {
      serverWriteQueryMeter.mark(count);
    }
  }

  public void updateWriteQueryMeter(HRegion region) {
if(KnobRuntime.check(java.util.UUID.fromString("71a6ed2c-b548-3392-be19-4243f4c368f4"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6fbdbb92-f5f5-30d2-a493-b5aceb87dcc3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce7c9ba9-a487-39c3-9356-0b351b01dba9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1f307ec-968a-3d95-bdae-3def4bb5c5dc"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c8150c4d-29bb-3c6a-9f3f-8159edad3144"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f667efb-0978-3804-861e-94819e75720a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f41a18b-feaf-3c1d-90b0-bb73cfdebb16"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3f934a14-89f4-3169-b990-cccbbdf389c8"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43410803-6c5a-36fc-b6dd-ce1df0627569"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f97cda52-ad10-3535-be0f-38c1478741fe"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35c077af-1ba2-3f0b-9d43-7e29645579ce"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("866afe1f-04fc-3b27-9c1a-c1c1bc580812"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("597c241a-e907-3422-992f-ac74aaeaf7ba"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd3f1062-fb44-32d7-999d-117f2af55ee6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb540b8f-7b07-3c56-aab0-fb3d2a86fc62"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("280d9ace-afa5-3a68-b278-01a5a2b2680c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eeee756f-4e5b-3e03-a1f0-27bc81a936c2"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aaf3c426-3608-3254-a8ae-3157329227be"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b3aa0c2c-feed-3e29-84f2-f5da70e3bab9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b34955a-7582-3479-b0db-2c452ec3b93a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45ea9aa0-1c6c-39cb-883c-a2bfa7890f91"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aabb7640-f328-36be-9910-e46163a951eb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3123b7e6-6dd0-3fbe-9272-e165d2c296a7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ca4faad-1b43-30f5-aacf-f833477285ae"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aeb25c2d-fb26-3d19-a8f8-37552d6a90e1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ce8b73ec-03eb-3416-9fe0-80c3ebc550ac"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2f84378-f15c-323b-95be-4f9efefbff22"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("974af780-548f-3f4e-b27b-5d341038acbb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("660789ad-bec9-3acf-8024-efc2c1e9a3dd"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7714d67-bd92-31d0-90e8-4470967348be"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28bed536-d784-3471-b9e7-edd7402df848"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a45808fc-c069-34b6-a460-a5e32551e77c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75398746-7dd2-363c-b158-5c3a64af3781"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("220d6d3a-1c41-3126-bd0c-7a2115d7bfeb"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b2d4ab0-2275-350d-82dd-67d768ef0757"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40a48251-da59-3be0-98c1-85795d0aebb1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5fc74ff8-1aee-334f-9d47-471fb2507355"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a10fd0b-c7c7-3279-b83e-80db5a2b8853"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("444cb0d6-de58-38d0-b95a-be7c9f363502"))) ? ((region.getMetricsTableRequests()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9a0110d9-5370-3718-9fe7-66e4a1527c47"))) ? ((region.getMetricsTableRequests()) == (null)) : (region.getMetricsTableRequests() != null))))) {
      region.getMetricsTableRequests().updateTableWriteQueryMeter();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("81801649-59a3-386a-8105-879871b112d7"))) ? ((serverWriteQueryMeter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5d0f13ad-724a-3083-9055-9218718815e8"))) ? ((serverWriteQueryMeter) == (null)) : (serverWriteQueryMeter != null))))) {
      serverWriteQueryMeter.mark();
    }
  }

  /**
   * @see MetricsRegionServerQuotaSource#incrementNumRegionSizeReportsSent(long)
   */
  public void incrementNumRegionSizeReportsSent(long numReportsSent) {
if(KnobRuntime.check(java.util.UUID.fromString("033d26f9-1d2d-3b8a-b6db-f304c26f27cf"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("9cc9e3f3-c5b0-3877-856f-653cfaedd897"))) {
numReportsSent = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2494bb4c-a340-3e2b-8f3e-bb00d88aeccf"))) {
numReportsSent += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("65c0382b-e455-3cfc-a8de-4dc7cd1dfa07"))) { quotaSource.incrementRegionSizeReportingChoreTime(numReportsSent); } else { quotaSource.incrementNumRegionSizeReportsSent(numReportsSent); }
  }

  /**
   * @see MetricsRegionServerQuotaSource#incrementRegionSizeReportingChoreTime(long)
   */
  public void incrementRegionSizeReportingChoreTime(long time) {
    quotaSource.incrementRegionSizeReportingChoreTime(time);
  }

  public void incrScannerLeaseExpired() {
    serverSource.incrScannerLeaseExpired();
  }

}
