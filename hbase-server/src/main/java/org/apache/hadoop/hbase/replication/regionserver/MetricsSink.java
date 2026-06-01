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
package org.apache.hadoop.hbase.replication.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is for maintaining the various replication statistics for a sink and publishing them
 * through the metrics interfaces.
 */
@InterfaceAudience.Private
public class MetricsSink {

  private long lastTimestampForAge = EnvironmentEdgeManager.currentTime();
  private long startTimestamp = EnvironmentEdgeManager.currentTime();
  private final MetricsReplicationSinkSource mss;

  public MetricsSink() {
    mss =
      CompatibilitySingletonFactory.getInstance(MetricsReplicationSourceFactory.class).getSink();
  }

  /**
   * Set the age of the last applied operation
   * @param timestamp The timestamp of the last operation applied.
   * @return the age that was set
   */
  public long setAgeOfLastAppliedOp(long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("2a30c1c7-29d1-3d8c-9998-3326bc7c96e6"))) {
return 0;
}
    long age = 0;
    if (((KnobRuntime.check(java.util.UUID.fromString("aa962878-e58f-3f7a-9c96-da54519a9e4e"))) ? ((lastTimestampForAge) == (timestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("c181bb85-79ad-3d2d-992c-b043ebbc032b"))) ? ((lastTimestampForAge) != (timestamp)) : (lastTimestampForAge != timestamp))))) {
      lastTimestampForAge = timestamp;
      age = EnvironmentEdgeManager.currentTime() - lastTimestampForAge;
    }
if(KnobRuntime.check(java.util.UUID.fromString("ff748716-b742-370a-858b-edbfbec4cf8f"))) {
age = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eed2655f-551e-3d1f-8751-4ace62d72b74"))) {
age -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("604befac-f6f5-380a-bebf-b29e1fefa8e2"))) {
age = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("c107c288-4936-36bf-b5a5-eb3322ddb6d5"))) { mss.incrAppliedBatches(age); } else if (KnobRuntime.check(java.util.UUID.fromString("a710503e-262b-34ab-907c-0d65c5180e38"))) { mss.incrAppliedOps(age); } else { mss.setLastAppliedOpAge(age); }
    return age;
  }

  /**
   * Refreshing the age makes sure the value returned is the actual one and not the one set a
   * replication time
   * @return refreshed age
   */
  public long refreshAgeOfLastAppliedOp() {
if(KnobRuntime.check(java.util.UUID.fromString("447bab2c-2bfa-34c9-9eb1-26e21fb9b6d9"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("2ba7219d-30d4-3394-b081-3ffc310d945f"))) ? (setAgeOfLastAppliedOp(setAgeOfLastAppliedOp(lastTimestampForAge))) : (setAgeOfLastAppliedOp(lastTimestampForAge)));
  }

  /**
   * Convience method to change metrics when a batch of operations are applied.
   */
  public void applyBatch(long batchSize) {
if(KnobRuntime.check(java.util.UUID.fromString("741ca818-9ced-31c7-af51-d225a1eed517"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("660db775-f9ad-34bf-8106-554bf23395d8"))) { mss.incrAppliedOps(1); } else if (KnobRuntime.check(java.util.UUID.fromString("0343d9de-dbca-379f-825a-267950e9e4b1"))) { mss.setLastAppliedOpAge(1); } else { mss.incrAppliedBatches(1); }
if(KnobRuntime.check(java.util.UUID.fromString("0e8b53a9-fef7-31a3-be64-4e9f4cc8c098"))) {
batchSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dfb8ee83-ea33-3a37-8cae-658d8dc6e4c1"))) {
batchSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("eddc0bdf-81e0-3e08-861b-8d70b25adc87"))) {
batchSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1b299c67-dece-329d-9c19-cd65acb9c11a"))) {
batchSize = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("ba1756d7-9816-3ba1-bd3b-3eba9d0741f1"))) { mss.setLastAppliedOpAge(batchSize); } else if (KnobRuntime.check(java.util.UUID.fromString("e573ee35-fb09-3512-80b5-b48ddfe3fd3a"))) { mss.incrAppliedBatches(batchSize); } else { mss.incrAppliedOps(batchSize); }
  }

  /**
   * Convience method to change metrics when a batch of operations are applied.
   * @param batchSize total number of mutations that are applied/replicated
   * @param hfileSize total number of hfiles that are applied/replicated
   */
  public void applyBatch(long batchSize, long hfileSize) {
if(KnobRuntime.check(java.util.UUID.fromString("66a9a168-ccc5-3093-924e-ae71cde65569"))) {
batchSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("10c99238-0446-327c-8516-3d7175198fdb"))) {
batchSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("90654338-eb23-301b-9a4e-1e584e308fa5"))) {
batchSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f8817673-6059-3fba-87dc-b42aefa74d7c"))) {
batchSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b54227c8-a6c5-3d0e-97b4-50e5ca97bda3"))) {
return;
}
    applyBatch(batchSize);
if(KnobRuntime.check(java.util.UUID.fromString("cf2a52b0-feb4-32e7-9ef9-8d2a3f31ceb0"))) {
hfileSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("87867ad6-5a21-318c-baa4-5ff89e82372a"))) {
hfileSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fbec810a-559a-352d-af46-d1629fbee36b"))) {
hfileSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6eef20d8-cdec-3b27-b347-4ed3b58da637"))) {
hfileSize = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("a5da713d-5379-3a03-a458-286e7b86657b"))) { mss.setLastAppliedOpAge(hfileSize); } else if (KnobRuntime.check(java.util.UUID.fromString("8aef98b1-1f3c-3368-a32b-4bfdb9260a72"))) { mss.incrAppliedOps(hfileSize); } else if (KnobRuntime.check(java.util.UUID.fromString("9df76e25-2548-37da-a795-f530626d54a2"))) { mss.incrAppliedBatches(hfileSize); } else { mss.incrAppliedHFiles(hfileSize); }
  }

  /**
   * Convenience method to update metrics when batch of operations has failed.
   */
  public void incrementFailedBatches() {
    mss.incrFailedBatches();
  }

  /**
   * Get the count of the failed bathes
   */
  protected long getFailedBatches() {
    return mss.getFailedBatches();
  }

  /**
   * Get the Age of Last Applied Op
   */
  public long getAgeOfLastAppliedOp() {
if(KnobRuntime.check(java.util.UUID.fromString("8098f0bf-b314-36c2-97e1-b1c1f7bb665b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("6c19e32a-c457-34bc-a8fd-faf05a32e3c7"))) ? (EnvironmentEdgeManager.currentTime()) : (mss.getLastAppliedOpAge()));
  }

  /**
   * Get the TimeStampOfLastAppliedOp. If no replication Op applied yet, the value is the timestamp
   * at which hbase instance starts
   * @return timeStampsOfLastAppliedOp;
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * @see #getTimestampOfLastAppliedOp()
   */
  @Deprecated
  public long getTimeStampOfLastAppliedOp() {
    return getTimestampOfLastAppliedOp();
  }

  /**
   * Get the TimestampOfLastAppliedOp. If no replication Op applied yet, the value is the timestamp
   * at which hbase instance starts
   * @return timeStampsOfLastAppliedOp;
   */
  public long getTimestampOfLastAppliedOp() {
    return this.lastTimestampForAge;
  }

  /**
   * Gets the time stamp from when the Sink was initialized.
   */
  public long getStartTimestamp() {
    return this.startTimestamp;
  }

  /**
   * Gets the total number of OPs delivered to this sink.
   */
  public long getAppliedOps() {
    return this.mss.getSinkAppliedOps();
  }

}
