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
    return serverSource;
  }

  public MetricsUserAggregate getMetricsUserAggregate() {
    return userAggregate;
  }

  public MetricsRegionServerWrapper getRegionServerWrapper() {
if(KnobRuntime.check(java.util.UUID.fromString("ee2e5baf-e1ee-3a91-a769-626461ba689b"))) {
return null;
}
    return regionServerWrapper;
  }

  public void updatePutBatch(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("c23d49ea-9f1a-3bc0-bd02-f0442cbaf5e0"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("memstoreFlushSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updatePutBatch(t);
    }
    serverSource.updatePutBatch(t);
  }

  public void updatePut(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("401a5352-fafc-3114-bca3-7962a83e5887"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aeabcce5-8ead-3da0-9d67-edc8e6bed60d"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (region.getMetricsTableRequests() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("7eb86090-eaf8-3ffb-b200-b2016a28649c"))) {
t = -1;
}
      region.getMetricsTableRequests().updatePut(t);
    }
    if (t > slowMetricTime) {
      serverSource.incrSlowPut();
    }
    serverSource.updatePut(t);
    userAggregate.updatePut(t);
  }

  public void updateDelete(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("530bbe5a-4b27-3eda-becc-3cdf10ee6357"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5c3ccd3b-0c0f-3d3f-b842-b815b318039f"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d01ae94-06b7-36bd-856a-d77e6658cd7a"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c500fffd-ecc1-31c6-bb3d-e5d592d086eb"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("busyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateDelete(t);
    }
    if (t > slowMetricTime) {
      if (KnobRuntime.check(java.util.UUID.fromString("a98a16d9-ce1d-33c7-80ce-2daf9eb2124d"))) { serverSource.incrSlowGet(); } else { serverSource.incrSlowDelete(); }
    }
    serverSource.updateDelete(t);
    userAggregate.updateDelete(t);
  }

  public void updateDeleteBatch(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("f26ea14f-1314-33a9-8a6d-d0e6d258916e"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("timestampSlop");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46d10797-b9ed-3524-ad53-432c58d61758"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("647b1e3c-b51b-3416-ae1b-55b8bd87dbcf"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushPerChanges");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87f5d238-1816-3636-92b3-c5fbf96c0905"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (region.getMetricsTableRequests() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("b073c750-9b60-3e2f-a315-d149592e0709"))) {
t = -1;
}
      region.getMetricsTableRequests().updateDeleteBatch(t);
    }
    serverSource.updateDeleteBatch(t);
  }

  public void updateCheckAndDelete(HRegion region, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("0270a901-263e-3304-9510-85b9aefe435b"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c48e6b5-de36-3b1f-9e2d-d34a9fdbadf4"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowLockWaitDuration");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (region.getMetricsTableRequests() != null) {
      if (KnobRuntime.check(java.util.UUID.fromString("7a792a6d-2e4e-3299-be63-ede9e21218f2"))) { region.getMetricsTableRequests().updateDelete(t); } else { region.getMetricsTableRequests().updateCheckAndDelete(t); }
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
      serverSource.incrSlowGet();
    }
    serverSource.updateGet(time, blockBytesScanned);
if(KnobRuntime.check(java.util.UUID.fromString("00096d0c-8e4b-3298-a0ae-6cd9b913f055"))) {
blockBytesScanned = 0;
}
    userAggregate.updateGet(time, blockBytesScanned);
  }

  public void updateIncrement(HRegion region, long time, long blockBytesScanned) {
if(KnobRuntime.check(java.util.UUID.fromString("b7502450-61f9-301f-916b-35d65ec5bdff"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("minBlockSizeBytes");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("80a5e543-e12c-3cb8-8eed-5f07f0de6351"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushPerChanges");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d84c872-6965-391d-94ad-8696f25f8a76"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("isRestoredRegion");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(region);
    _knob_field_.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (region.getMetricsTableRequests() != null) {
if(KnobRuntime.check(java.util.UUID.fromString("26b46ca5-f44a-3f14-9138-4f3009f1a466"))) {
time = 0;
}
      region.getMetricsTableRequests().updateIncrement(time, blockBytesScanned);
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
    userAggregate.updateScan(time, blockBytesScanned);
  }

  public void updateSplitTime(long t) {
    serverSource.updateSplitTime(t);
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

    if (table != null) {
      metricsTable.updateFlushTime(table, t);
      metricsTable.updateFlushMemstoreSize(table, memstoreSize);
      metricsTable.updateFlushOutputSize(table, fileSize);
    }

  }

  public void updateCompaction(String table, boolean isMajor, long t, int inputFileCount,
    int outputFileCount, long inputBytes, long outputBytes) {
    if (KnobRuntime.check(java.util.UUID.fromString("fffba14d-9682-34ae-8c16-3f31e34c28a6"))) { serverSource.updateCompactionOutputFileCount(!isMajor, t); } else { serverSource.updateCompactionTime(isMajor, t); }
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
if(KnobRuntime.check(java.util.UUID.fromString("8fd495a2-cf78-32d9-b1da-ba632c625540"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxFlushedSeqId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9bcfbac-608b-3062-b255-d9183b134bbf"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("flushCheckInterval");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2234e159-6c5b-3324-95bf-ed3a55f2bb38"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ec5a722-d084-3251-91f4-ca211e4cbeea"))) {
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
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateTableWriteQueryMeter();
    }
    if (serverWriteQueryMeter != null) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2494bb4c-a340-3e2b-8f3e-bb00d88aeccf"))) {
numReportsSent += 1;
}
    quotaSource.incrementNumRegionSizeReportsSent(numReportsSent);
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
