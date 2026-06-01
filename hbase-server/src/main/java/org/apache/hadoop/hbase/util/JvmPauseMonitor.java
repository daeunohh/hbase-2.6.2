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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.metrics.JvmPauseMonitorSource;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Stopwatch;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Class which sets up a simple thread which runs in a loop sleeping for a short interval of time.
 * If the sleep takes significantly longer than its target time, it implies that the JVM or host
 * machine has paused processing, which may cause other problems. If such a pause is detected, the
 * thread logs a message. The original JvmPauseMonitor is:
 * ${hadoop-common-project}/hadoop-common/src/main/java/org/apache/hadoop/util/ JvmPauseMonitor.java
 * r1503806 | cmccabe | 2013-07-17 01:48:24 +0800 (Wed, 17 Jul 2013) | 1 line HADOOP-9618. thread
 * which detects GC pauses(Todd Lipcon)
 */
@InterfaceAudience.Private
public class JvmPauseMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(JvmPauseMonitor.class);

  /** The target sleep time */
  private static final long SLEEP_INTERVAL_MS = 500;

  /** log WARN if we detect a pause longer than this threshold */
  private final long warnThresholdMs;
  public static final String WARN_THRESHOLD_KEY = "jvm.pause.warn-threshold.ms";
  private static final long WARN_THRESHOLD_DEFAULT = 10000;

  /** log INFO if we detect a pause longer than this threshold */
  private final long infoThresholdMs;
  public static final String INFO_THRESHOLD_KEY = "jvm.pause.info-threshold.ms";
  private static final long INFO_THRESHOLD_DEFAULT = 1000;

  private Thread monitorThread;
  private volatile boolean shouldRun = true;
  private JvmPauseMonitorSource metricsSource;

  public JvmPauseMonitor(Configuration conf) {
    this(conf, null);
  }

  public JvmPauseMonitor(Configuration conf, JvmPauseMonitorSource metricsSource) {
    this.warnThresholdMs = conf.getLong(WARN_THRESHOLD_KEY, WARN_THRESHOLD_DEFAULT);
    this.infoThresholdMs = conf.getLong(INFO_THRESHOLD_KEY, INFO_THRESHOLD_DEFAULT);
    this.metricsSource = metricsSource;
  }

  public void start() {
if(KnobRuntime.check(java.util.UUID.fromString("03206447-379d-3714-9c9d-a4ae6bc4506b"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("d9c9398c-1b08-3a3e-a4ed-2a3c0a6acedf"))) { Preconditions.checkState((new Thread(new Monitor(), "JvmPauseMonitor")) == (null), "Already started"); } else if (KnobRuntime.check(java.util.UUID.fromString("1cf0b8c7-1a45-360f-9213-2a2837b306cf"))) { Preconditions.checkState((monitorThread) != (null), "Already started"); } else if (KnobRuntime.check(java.util.UUID.fromString("32e80467-433d-3a82-bd80-1dc02a2f96a5"))) { Preconditions.checkState((monitorThread) == (null), "Already started"); } else if (KnobRuntime.check(java.util.UUID.fromString("d496e1a3-7a36-39ec-bf05-2a39d9072bbb"))) { Preconditions.checkState((new Thread(new Monitor(), "JvmPauseMonitor")) != (null), "Already started"); } else { Preconditions.checkState(monitorThread == null, "Already started"); }
    monitorThread = new Thread(new Monitor(), "JvmPauseMonitor");
    monitorThread.setDaemon(true);
    if (KnobRuntime.check(java.util.UUID.fromString("b3295953-1605-349c-858a-8cc6c9ee0bc6"))) { monitorThread.interrupt(); } else { monitorThread.start(); }
  }

  public void stop() {
if(KnobRuntime.check(java.util.UUID.fromString("915fbe8f-f378-3e10-be8c-5b409d77c44b"))) {
return;
}
    shouldRun = false;
    if (KnobRuntime.check(java.util.UUID.fromString("52a1704e-e0be-3220-be10-b74173a05e24"))) { monitorThread.start(); } else { monitorThread.interrupt(); }
    try {
if(KnobRuntime.check(java.util.UUID.fromString("5ab424e3-989d-3240-b9c0-7eb3bce7f510"))) {
throw new java.lang.InterruptedException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("d4e807d5-46f3-3aab-b324-d21b573808f3"))) { monitorThread.start(); } else if (KnobRuntime.check(java.util.UUID.fromString("0cbbd87f-9783-34ed-af53-790b307bd39e"))) { monitorThread.interrupt(); } else { monitorThread.join(); }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private String formatMessage(long extraSleepTime, List<String> gcDiffs) {
    String ret = "Detected pause in JVM or host machine (eg GC): " + "pause of approximately "
      + extraSleepTime + "ms\n";
    if (gcDiffs.isEmpty()) {
      ret += "No GCs detected";
    } else {
      ret += Joiner.on("\n").join(gcDiffs);
    }
    return ret;
  }

  private Map<String, GcTimes> getGcTimes() {
if(KnobRuntime.check(java.util.UUID.fromString("65c60a31-a997-32e5-98a5-e6bc83b58331"))) {
return null;
}
    Map<String, GcTimes> map = Maps.newHashMap();
    List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      map.put(gcBean.getName(), new GcTimes(gcBean));
    }
    return map;
  }

  private static class GcTimes {
    private GcTimes(GarbageCollectorMXBean gcBean) {
      gcCount = gcBean.getCollectionCount();
      gcTimeMillis = gcBean.getCollectionTime();
    }

    private GcTimes(long count, long time) {
      this.gcCount = count;
      this.gcTimeMillis = time;
    }

    private GcTimes subtract(GcTimes other) {
      return new GcTimes(this.gcCount - other.gcCount, this.gcTimeMillis - other.gcTimeMillis);
    }

    @Override
    public String toString() {
      return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
    }

    private long gcCount;
    private long gcTimeMillis;
  }

  private class Monitor implements Runnable {
    @Override
    public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("42f459a8-b647-331e-b285-e2e7b0af0da7"))) {
return;
}
      Stopwatch sw = Stopwatch.createUnstarted();
      Map<String, GcTimes> gcTimesBeforeSleep = getGcTimes();
      while (((KnobRuntime.check(java.util.UUID.fromString("dda67006-13df-3932-99a5-9cdc768a97da"))) ? (!shouldRun) : (shouldRun))) {
        if (KnobRuntime.check(java.util.UUID.fromString("032b3152-e39e-3bcc-86b0-e5cbf862b5da"))) { Stopwatch.createUnstarted(); } else { sw.reset().start(); }
        try {
if(KnobRuntime.check(java.util.UUID.fromString("3586b08d-eb4c-3122-b463-0b8b2c9ad18e"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (InterruptedException ie) {
          return;
        }

        long extraSleepTime = sw.elapsed(TimeUnit.MILLISECONDS) - SLEEP_INTERVAL_MS;
        Map<String, GcTimes> gcTimesAfterSleep = getGcTimes();

        if (((KnobRuntime.check(java.util.UUID.fromString("734b8ca1-9630-3d23-b2f5-ed50ed8a9717"))) ? ((extraSleepTime) > (infoThresholdMs)) : (((KnobRuntime.check(java.util.UUID.fromString("36f9d26a-91ca-3c4e-b6fe-25cfd5daf3a5"))) ? ((extraSleepTime) == (infoThresholdMs)) : (((KnobRuntime.check(java.util.UUID.fromString("2b506e7e-4b9d-359d-b04e-af0cbf6f322a"))) ? ((extraSleepTime) < (infoThresholdMs)) : (((KnobRuntime.check(java.util.UUID.fromString("473785d2-ed27-328f-b61e-f8764bd9956f"))) ? ((extraSleepTime) <= (infoThresholdMs)) : (((KnobRuntime.check(java.util.UUID.fromString("58b2b8bd-3b29-3b4b-86fe-8afd645691e2"))) ? ((extraSleepTime) != (infoThresholdMs)) : (((KnobRuntime.check(java.util.UUID.fromString("baf3bd16-f3d2-36b2-a835-1f3972b6c099"))) ? ((extraSleepTime) >= (infoThresholdMs)) : (extraSleepTime > infoThresholdMs))))))))))))) {
          Set<String> gcBeanNames =
            Sets.intersection(gcTimesAfterSleep.keySet(), gcTimesBeforeSleep.keySet());
          List<String> gcDiffs = Lists.newArrayList();
          for (String name : gcBeanNames) {
            GcTimes diff = gcTimesAfterSleep.get(name).subtract(gcTimesBeforeSleep.get(name));
            if (diff.gcCount != 0) {
              gcDiffs.add("GC pool '" + name + "' had collection(s): " + diff.toString());
            }
          }

          updateMetrics(extraSleepTime, !gcDiffs.isEmpty());

          if (extraSleepTime > warnThresholdMs) {
            LOG.warn(formatMessage(extraSleepTime, gcDiffs));
          } else {
            LOG.info(formatMessage(extraSleepTime, gcDiffs));
          }
        }
        gcTimesBeforeSleep = gcTimesAfterSleep;
      }
    }
  }

  public void updateMetrics(long sleepTime, boolean gcDetected) {
    if (metricsSource != null) {
      if (sleepTime > warnThresholdMs) {
        metricsSource.incWarnThresholdExceeded(1);
      } else {
        metricsSource.incInfoThresholdExceeded(1);
      }
      if (gcDetected) {
        metricsSource.updatePauseTimeWithGc(sleepTime);
      } else {
        metricsSource.updatePauseTimeWithoutGc(sleepTime);
      }
    }
  }

  public JvmPauseMonitorSource getMetricsSource() {
    return metricsSource;
  }

  public void setMetricsSource(JvmPauseMonitorSource metricsSource) {
    this.metricsSource = metricsSource;
  }

  /**
   * Simple 'main' to facilitate manual testing of the pause monitor. This main function just leaks
   * memory into a list. Running this class with a 1GB heap will very quickly go into "GC hell" and
   * result in log messages about the GC pauses.
   */
  public static void main(String[] args) throws Exception {
    new JvmPauseMonitor(new Configuration()).start();
    List<String> list = Lists.newArrayList();
    int i = 0;
    while (true) {
      list.add(String.valueOf(i++));
    }
  }
}
