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
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsUserAggregateFactory {
  private MetricsUserAggregateFactory() {

  }

  public static final String METRIC_USER_ENABLED_CONF = "hbase.regionserver.user.metrics.enabled";
  public static final boolean DEFAULT_METRIC_USER_ENABLED_CONF = false;

  public static MetricsUserAggregate getMetricsUserAggregate(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("657be518-dd63-3aea-a96b-3d3934731fa8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cb61b937-f30a-355e-9197-e9ee6f38293a"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e04e3db7-2f88-3221-a7af-bf82b533b776"))) ? (conf.getBoolean(METRIC_USER_ENABLED_CONF, !DEFAULT_METRIC_USER_ENABLED_CONF)) : (conf.getBoolean(METRIC_USER_ENABLED_CONF, DEFAULT_METRIC_USER_ENABLED_CONF)))) {
      return new MetricsUserAggregateImpl(conf);
    } else {
      // NoOpMetricUserAggregate
      return new MetricsUserAggregate() {
        @Override
        public MetricsUserAggregateSource getSource() {
if(KnobRuntime.check(java.util.UUID.fromString("b3cb405d-c6f1-37c5-8e7b-e07dcb874889"))) {
return null;
}
          return null;
        }

        @Override
        public void updatePut(long t) {

        }

        @Override
        public void updateDelete(long t) {

        }

        @Override
        public void updateGet(long time, long blockBytesScanned) {

        }

        @Override
        public void updateIncrement(long time, long blockBytesScanned) {

        }

        @Override
        public void updateAppend(long time, long blockBytesScanned) {

        }

        @Override
        public void updateReplay(long t) {

        }

        @Override
        public void updateScan(long time, long blockBytesScanned) {

        }

        @Override
        public void updateCheckAndMutate(long blockBytesScanned) {

        }

        @Override
        public void updateFilteredReadRequests() {

        }

        @Override
        public void updateReadRequestCount() {

        }
      };
    }
  }

}
