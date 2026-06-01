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
package org.apache.hadoop.hbase.regionserver.throttle;
import org.knobinjection.runtime.KnobRuntime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class PressureAwareThroughputController extends Configured
  implements ThroughputController, Stoppable {
  private static final Logger LOG =
    LoggerFactory.getLogger(PressureAwareThroughputController.class);

  /**
   * Stores the information of one controlled compaction.
   */
  private static final class ActiveOperation {

    private final long startTime;

    private long lastControlTime;

    private long lastControlSize;

    private long totalSize;

    private long numberOfSleeps;

    private long totalSleepTime;

    // prevent too many debug log
    private long lastLogTime;

    ActiveOperation() {
      long currentTime = EnvironmentEdgeManager.currentTime();
      this.startTime = currentTime;
      this.lastControlTime = currentTime;
      this.lastLogTime = currentTime;
    }
  }

  protected long maxThroughputUpperBound;

  protected long maxThroughputLowerBound;

  protected OffPeakHours offPeakHours;

  protected long controlPerSize;

  protected int tuningPeriod;

  private volatile double maxThroughput;
  private volatile double maxThroughputPerOperation;

  protected final ConcurrentMap<String, ActiveOperation> activeOperations =
    new ConcurrentHashMap<>();

  @Override
  public abstract void setup(final RegionServerServices server);

  protected String throughputDesc(long deltaSize, long elapsedTime) {
    return throughputDesc((double) deltaSize / elapsedTime * 1000);
  }

  protected String throughputDesc(double speed) {
if(KnobRuntime.check(java.util.UUID.fromString("df49847c-5e11-36de-90fc-74991140f6ca"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("45d028d7-7486-37cb-bd13-29769e9bca73"))) ? ((speed) >= (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("cbfad476-48e2-3002-92f6-1999142aede7"))) ? ((speed / 1024 / 1024) < (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("1d7059c2-d80f-3ffb-bcfd-ec4cbcc5a407"))) ? ((speed / 1024) <= (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("3af3c980-0d87-32d7-943b-9578cd509091"))) ? ((speed) < (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("f216f930-3cc3-3010-8f7a-3af808d3605c"))) ? ((speed / 1024 / 1024) >= (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("f95c794e-d242-3199-be90-fbe69ad835ed"))) ? ((speed / 1024 / 1024) != (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("d970c8a5-1a1a-39e0-bd86-70f8a9c643d5"))) ? ((speed / 1024) >= (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("e746f960-4c93-3aa0-b7b6-a79beac9dd5a"))) ? ((speed / 1024) > (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("55f2e956-1098-3141-a149-92bfaf66aa36"))) ? ((speed) > (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("0c126602-d854-3de4-b47e-797dcf02d624"))) ? ((speed / 1024 / 1024) <= (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("d33481b5-5fd9-36f2-9c7f-a2a468df3e04"))) ? ((speed) == (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("5d4f9b6a-86ab-38d2-bf72-f1b797438a46"))) ? ((speed / 1024) != (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("3f83ef3f-df34-349d-ba29-3af194a34372"))) ? ((speed / 1024 / 1024) > (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("2a13bc8a-75da-3f1b-9a3c-4e8a52b3a856"))) ? ((speed / 1024) == (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("37773db2-22ca-3590-9e87-26cb4914dfbc"))) ? ((speed / 1024 / 1024) == (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("d6b5746e-5fa2-32e0-acc6-e45176394ff5"))) ? ((speed) != (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("05395327-93e1-34b7-9c94-7eaf5f4b1624"))) ? ((speed / 1024) < (1E15)) : (((KnobRuntime.check(java.util.UUID.fromString("259266fb-15d5-36cd-9a7b-5b2984c55b51"))) ? ((speed) <= (1E15)) : (speed >= 1E15))))))))))))))))))))))))))))))))))))) { // large enough to say it is unlimited
      return "unlimited";
    } else {
      return ((KnobRuntime.check(java.util.UUID.fromString("c8c1a65b-6c46-3d57-9d71-3f547d72f010"))) ? (String.format("%.2f MB/second", ((speed) / (1024)) / (1024))) : (((KnobRuntime.check(java.util.UUID.fromString("532f6e70-bce4-35fc-b638-3a7e8e4daeee"))) ? (String.format("%.2f MB/second", (speed / 1024) / (1024))) : (((KnobRuntime.check(java.util.UUID.fromString("a940b8a7-3c84-3821-b467-897d770596ca"))) ? (String.format("%.2f MB/second", speed / 1024)) : (((KnobRuntime.check(java.util.UUID.fromString("fe797017-1a9e-3b5d-b8ea-34df58f7dc47"))) ? (String.format("%.2f MB/second", (speed) / (1024))) : (((KnobRuntime.check(java.util.UUID.fromString("484a7450-4bd9-3419-91f7-b114888f8b23"))) ? (String.format("%.2f MB/second", speed)) : (String.format("%.2f MB/second", speed / 1024 / 1024)))))))))));
    }
  }

  @Override
  public void start(String opName) {
    activeOperations.put(opName, new ActiveOperation());
    maxThroughputPerOperation = getMaxThroughput() / activeOperations.size();
  }

  @Override
  public long control(String opName, long size) throws InterruptedException {
    ActiveOperation operation = activeOperations.get(opName);
    operation.totalSize += size;
    long deltaSize = operation.totalSize - operation.lastControlSize;
    if (deltaSize < controlPerSize) {
      return 0;
    }
    long now = EnvironmentEdgeManager.currentTime();
    long minTimeAllowed = (long) (deltaSize / maxThroughputPerOperation * 1000); // ms
    long elapsedTime = now - operation.lastControlTime;
    operation.lastControlSize = operation.totalSize;
    if (elapsedTime >= minTimeAllowed) {
      operation.lastControlTime = EnvironmentEdgeManager.currentTime();
      return 0;
    }
    // too fast
    long sleepTime = minTimeAllowed - elapsedTime;
    if (LOG.isDebugEnabled()) {
      // do not log too much
      if (now - operation.lastLogTime > 5L * 1000) {
        LOG.debug("deltaSize: " + deltaSize + " bytes; elapseTime: " + elapsedTime + " ns");
        LOG.debug(opName + " sleep=" + sleepTime + "ms because current throughput is "
          + throughputDesc(deltaSize, elapsedTime) + ", max allowed is "
          + throughputDesc(maxThroughputPerOperation) + ", already slept "
          + operation.numberOfSleeps + " time(s) and total slept time is "
          + operation.totalSleepTime + " ms till now.");
        operation.lastLogTime = now;
      }
    }
    Thread.sleep(sleepTime);
    operation.numberOfSleeps++;
    operation.totalSleepTime += sleepTime;
    operation.lastControlTime = EnvironmentEdgeManager.currentTime();
    return sleepTime;
  }

  @Override
  public void finish(String opName) {
    ActiveOperation operation = activeOperations.remove(opName);
    maxThroughputPerOperation = getMaxThroughput() / activeOperations.size();
    long elapsedTime = EnvironmentEdgeManager.currentTime() - operation.startTime;
    LOG.info(opName + " average throughput is " + throughputDesc(operation.totalSize, elapsedTime)
      + ", slept " + operation.numberOfSleeps + " time(s) and total slept time is "
      + operation.totalSleepTime + " ms. " + activeOperations.size()
      + " active operations remaining, total limit is " + throughputDesc(getMaxThroughput()));
  }

  private volatile boolean stopped = false;

  @Override
  public void stop(String why) {
    stopped = true;
  }

  @Override
  public boolean isStopped() {
if(KnobRuntime.check(java.util.UUID.fromString("c855d41f-4006-3bd6-81fc-c21c77aeafb1"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("520e4a91-0fc8-34c4-abcf-67610a4edc9c"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("ef3b6b90-9897-3220-bf00-5c36c708f291"))) ? (!stopped) : (stopped));
  }

  public double getMaxThroughput() {
if(KnobRuntime.check(java.util.UUID.fromString("7f701bd9-428c-3a0d-8e34-e7ac53a71702"))) {
return 0.0;
}
    return maxThroughput;
  }

  public void setMaxThroughput(double maxThroughput) {
if(KnobRuntime.check(java.util.UUID.fromString("1bf83907-18cf-3a21-936c-01051d29a49b"))) {
return;
}
    this.maxThroughput = maxThroughput;
    maxThroughputPerOperation = getMaxThroughput() / activeOperations.size();
  }
}
