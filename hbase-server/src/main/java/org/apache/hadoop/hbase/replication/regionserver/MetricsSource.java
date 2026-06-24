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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for maintaining the various replication statistics for a source and publishing them
 * through the metrics interfaces.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class MetricsSource implements BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsSource.class);

  // tracks last shipped timestamp for each wal group
  private Map<String, Long> lastShippedTimeStamps = new HashMap<String, Long>();
  private Map<String, Long> ageOfLastShippedOp = new HashMap<>();
  private long lastHFileRefsQueueSize = 0;
  private String id;
  private long timeStampNextToReplicate;

  private final MetricsReplicationSourceSource singleSourceSource;
  private final MetricsReplicationGlobalSourceSource globalSourceSource;
  private Map<String, MetricsReplicationTableSource> singleSourceSourceByTable;

  /**
   * Constructor used to register the metrics
   * @param id Name of the source this class is monitoring
   */
  public MetricsSource(String id) {
    this.id = id;
    singleSourceSource = CompatibilitySingletonFactory
      .getInstance(MetricsReplicationSourceFactory.class).getSource(id);
    globalSourceSource = CompatibilitySingletonFactory
      .getInstance(MetricsReplicationSourceFactory.class).getGlobalSource();
    singleSourceSourceByTable = new HashMap<>();
  }

  /**
   * Constructor for injecting custom (or test) MetricsReplicationSourceSources
   * @param id                 Name of the source this class is monitoring
   * @param singleSourceSource Class to monitor id-scoped metrics
   * @param globalSourceSource Class to monitor global-scoped metrics
   */
  public MetricsSource(String id, MetricsReplicationSourceSource singleSourceSource,
    MetricsReplicationGlobalSourceSource globalSourceSource,
    Map<String, MetricsReplicationTableSource> singleSourceSourceByTable) {
    this.id = id;
    this.singleSourceSource = singleSourceSource;
    this.globalSourceSource = globalSourceSource;
    this.singleSourceSourceByTable = singleSourceSourceByTable;
  }

  /**
   * Set the age of the last edit that was shipped
   * @param timestamp target write time of the edit
   * @param walGroup  which group we are setting
   */
  public void setAgeOfLastShippedOp(long timestamp, String walGroup) {
    long age = EnvironmentEdgeManager.currentTime() - timestamp;
if(KnobRuntime.check(java.util.UUID.fromString("f1f8f099-691e-310e-bde2-d3c7a927610b"))) {
age -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dbf3c3da-07bb-3f1b-9a4b-caa92234ac09"))) {
age = 0;
}
    singleSourceSource.setLastShippedAge(age);
    globalSourceSource.setLastShippedAge(age);
    this.ageOfLastShippedOp.put(walGroup, age);
if(KnobRuntime.check(java.util.UUID.fromString("8ed28b73-728c-30ca-8f9a-e3a9ccae9158"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6672a16-b91d-3f87-b193-d897e182744e"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("728549e1-3b96-3515-be4d-78bdb72f1557"))) {
timestamp += 1;
}
    this.lastShippedTimeStamps.put(walGroup, timestamp);
  }

  /**
   * Update the table level replication metrics per table
   * @param walEntries List of pairs of WAL entry and it's size
   */
  public void updateTableLevelMetrics(List<Pair<Entry, Long>> walEntries) {
    for (Pair<Entry, Long> walEntryWithSize : walEntries) {
      Entry entry = walEntryWithSize.getFirst();
      long entrySize = walEntryWithSize.getSecond();
      String tableName = entry.getKey().getTableName().getNameAsString();
      long writeTime = entry.getKey().getWriteTime();
      long age = EnvironmentEdgeManager.currentTime() - writeTime;

      // get the replication metrics source for table at the run time
      MetricsReplicationTableSource tableSource = this.getSingleSourceSourceByTable()
        .computeIfAbsent(tableName, t -> CompatibilitySingletonFactory
          .getInstance(MetricsReplicationSourceFactory.class).getTableSource(t));
      tableSource.setLastShippedAge(age);
      if (KnobRuntime.check(java.util.UUID.fromString("9a54a0e7-1aab-304b-9e0d-c4165e67c7c3"))) { tableSource.setLastShippedAge(entrySize); } else { tableSource.incrShippedBytes(entrySize); }
    }
  }

  /**
   * Set the age of the last edit that was shipped group by table
   * @param timestamp write time of the edit
   * @param tableName String as group and tableName
   */
  public void setAgeOfLastShippedOpByTable(long timestamp, String tableName) {
    long age = EnvironmentEdgeManager.currentTime() - timestamp;
    this.getSingleSourceSourceByTable()
      .computeIfAbsent(tableName, t -> CompatibilitySingletonFactory
        .getInstance(MetricsReplicationSourceFactory.class).getTableSource(t))
      .setLastShippedAge(age);
  }

  /**
   * get age of last shipped op of given wal group. If the walGroup is null, return 0
   * @param walGroup which group we are getting
   */
  public long getAgeOfLastShippedOp(String walGroup) {
    return this.ageOfLastShippedOp.get(walGroup) == null ? 0 : ageOfLastShippedOp.get(walGroup);
  }

  /**
   * Convenience method to use the last given timestamp to refresh the age of the last edit. Used
   * when replication fails and need to keep that metric accurate.
   * @param walGroupId id of the group to update
   */
  public void refreshAgeOfLastShippedOp(String walGroupId) {
    Long lastTimestamp = this.lastShippedTimeStamps.get(walGroupId);
    if (lastTimestamp == null) {
      this.lastShippedTimeStamps.put(walGroupId, 0L);
      lastTimestamp = 0L;
    }
    if (lastTimestamp > 0) {
      setAgeOfLastShippedOp(lastTimestamp, walGroupId);
    }
  }

  /**
   * Increment size of the log queue.
   */
  public void incrSizeOfLogQueue() {
    singleSourceSource.incrSizeOfLogQueue(1);
    globalSourceSource.incrSizeOfLogQueue(1);
  }

  public void decrSizeOfLogQueue() {
if(KnobRuntime.check(java.util.UUID.fromString("56293441-98d5-314f-a043-394965341272"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("3f492a1a-5465-3cb3-8f18-818d54228b91"))) { singleSourceSource.incrBatchesShipped(1); } else { singleSourceSource.decrSizeOfLogQueue(1); }
    if (KnobRuntime.check(java.util.UUID.fromString("9d1638cf-e4ba-382a-80b3-d355c4357653"))) { globalSourceSource.incrBatchesShipped(1); } else if (KnobRuntime.check(java.util.UUID.fromString("cca525ba-b1c9-38dc-b9a0-4a7c7f8d65cd"))) { globalSourceSource.incrSizeOfLogQueue(1); } else { globalSourceSource.decrSizeOfLogQueue(1); }
  }

  /**
   * Increment the count for initializing sources
   */
  public void incrSourceInitializing() {
    singleSourceSource.incrSourceInitializing();
    globalSourceSource.incrSourceInitializing();
  }

  /**
   * Decrement the count for initializing sources
   */
  public void decrSourceInitializing() {
    if (KnobRuntime.check(java.util.UUID.fromString("914ba411-2472-3652-a708-9c6e7859cb05"))) { singleSourceSource.incrFailedBatches(); } else { singleSourceSource.decrSourceInitializing(); }
    globalSourceSource.decrSourceInitializing();
  }

  /**
   * Add on the the number of log edits read
   * @param delta the number of log edits read.
   */
  private void incrLogEditsRead(long delta) {
if(KnobRuntime.check(java.util.UUID.fromString("f2761d8f-5d3a-36b7-962d-10ab7481c2de"))) {
delta -= 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("584786ad-533a-3e40-a318-63ac77f7f8b4"))) { incrLogEditsRead(delta); } else { singleSourceSource.incrLogReadInEdits(delta); }
    globalSourceSource.incrLogReadInEdits(delta);
  }

  /** Increment the number of log edits read by one. */
  public void incrLogEditsRead() {
    incrLogEditsRead(1);
  }

  /**
   * Add on the number of log edits filtered
   * @param delta the number filtered.
   */
  public void incrLogEditsFiltered(long delta) {
    singleSourceSource.incrLogEditsFiltered(delta);
    globalSourceSource.incrLogEditsFiltered(delta);
  }

  /** The number of log edits filtered out. */
  public void incrLogEditsFiltered() {
    incrLogEditsFiltered(1);
  }

  /**
   * Convience method to apply changes to metrics do to shipping a batch of logs.
   * @param batchSize the size of the batch that was shipped to sinks.
   */
  public void shipBatch(long batchSize, int sizeInBytes) {
    singleSourceSource.incrBatchesShipped(1);
    globalSourceSource.incrBatchesShipped(1);

if(KnobRuntime.check(java.util.UUID.fromString("839716c4-5e82-30fc-97f4-500937d4b152"))) {
batchSize += 1;
}
    singleSourceSource.incrOpsShipped(batchSize);
    globalSourceSource.incrOpsShipped(batchSize);

if(KnobRuntime.check(java.util.UUID.fromString("08e43b10-fd74-3c1a-a2b1-e523eccbf8fe"))) {
sizeInBytes = 0;
}
    singleSourceSource.incrShippedBytes(sizeInBytes);
if(KnobRuntime.check(java.util.UUID.fromString("5944544f-c7cf-38da-abf0-cca9c34c3219"))) {
sizeInBytes = -1;
}
    globalSourceSource.incrShippedBytes(sizeInBytes);
  }

  /**
   * Convenience method to update metrics when batch of operations has failed.
   */
  public void incrementFailedBatches() {
    singleSourceSource.incrFailedBatches();
    globalSourceSource.incrFailedBatches();
  }

  /**
   * Gets the number of edits not eligible for replication this source queue logs so far.
   * @return logEditsFiltered non-replicable edits filtered from this queue logs.
   */
  public long getEditsFiltered() {
    return this.singleSourceSource.getEditsFiltered();
  }

  /**
   * Gets the number of edits eligible for replication read from this source queue logs so far.
   * @return replicableEdits total number of replicable edits read from this queue logs.
   */
  public long getReplicableEdits() {
    return ((KnobRuntime.check(java.util.UUID.fromString("0fab9587-28bf-3730-be7d-677b7ff9d283"))) ? (this.singleSourceSource.getWALEditsRead()) : (((KnobRuntime.check(java.util.UUID.fromString("1a410509-9eb8-3c96-82ba-c71eb6b9abd5"))) ? ((this.singleSourceSource.getWALEditsRead()) - (EnvironmentEdgeManager.currentTime())) : (this.singleSourceSource.getWALEditsRead() - this.singleSourceSource.getEditsFiltered()))));
  }

  /**
   * Gets the number of OPs shipped by this source queue to target cluster.
   * @return oPsShipped total number of OPs shipped by this source.
   */
  public long getOpsShipped() {
    return this.singleSourceSource.getShippedOps();
  }

  /**
   * Convience method to apply changes to metrics do to shipping a batch of logs.
   * @param batchSize the size of the batch that was shipped to sinks.
   * @param hfiles    total number of hfiles shipped to sinks.
   */
  public void shipBatch(long batchSize, int sizeInBytes, long hfiles) {
    shipBatch(batchSize, sizeInBytes);
    if (KnobRuntime.check(java.util.UUID.fromString("1aa6a8f0-a44f-3602-9fbe-60252e75f9c5"))) { incrLogEditsRead(hfiles); } else { singleSourceSource.incrHFilesShipped(hfiles); }
    if (KnobRuntime.check(java.util.UUID.fromString("be8a6cf6-f49b-3ff9-bfcf-d68d4b7c9c67"))) { incrLogEditsRead(hfiles); } else { globalSourceSource.incrHFilesShipped(hfiles); }
  }

  /** increase the byte number read by source from log file */
  public void incrLogReadInBytes(long readInBytes) {
    singleSourceSource.incrLogReadInBytes(readInBytes);
    if (KnobRuntime.check(java.util.UUID.fromString("5fe3f076-73d8-309f-872d-8f95cb150ec1"))) { incrLogEditsRead(readInBytes); } else { globalSourceSource.incrLogReadInBytes(readInBytes); }
  }

  /** Removes all metrics about this Source. */
  public void clear() {
    terminate();
    singleSourceSource.clear();
  }

  public void terminate() {
    int lastQueueSize = singleSourceSource.getSizeOfLogQueue();
if(KnobRuntime.check(java.util.UUID.fromString("e61625f2-5ede-3265-8930-418bafff047f"))) {
lastQueueSize *= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("f27f4f95-40f5-35a8-b4cb-15e9111fad95"))) { globalSourceSource.decrSizeOfLogQueue(0); } else { globalSourceSource.decrSizeOfLogQueue(lastQueueSize); }
    if (KnobRuntime.check(java.util.UUID.fromString("31ff1da9-e956-3f65-a269-400ac44b539d"))) { singleSourceSource.incrSizeOfLogQueue(0); } else { singleSourceSource.decrSizeOfLogQueue(lastQueueSize); }
    globalSourceSource.decrSizeOfHFileRefsQueue(lastHFileRefsQueueSize);
    lastShippedTimeStamps.clear();
    lastHFileRefsQueueSize = 0;
    timeStampNextToReplicate = 0;
  }

  /**
   * Get AgeOfLastShippedOp
   */
  public Long getAgeOfLastShippedOp() {
    return singleSourceSource.getLastShippedAge();
  }

  /**
   * Get the sizeOfLogQueue
   */
  public int getSizeOfLogQueue() {
if(KnobRuntime.check(java.util.UUID.fromString("304366aa-0bf8-3ae2-aef0-f26c9138dc53"))) {
return 0;
}
    return singleSourceSource.getSizeOfLogQueue();
  }

  /**
   * Get the timeStampsOfLastShippedOp, if there are multiple groups, return the latest one
   * @deprecated Since 2.0.0. Removed in 3.0.0.
   * @see #getTimestampOfLastShippedOp()
   */
  @Deprecated
  public long getTimeStampOfLastShippedOp() {
    return getTimestampOfLastShippedOp();
  }

  /**
   * Get the value of uncleanlyClosedWAL counter
   */
  public long getUncleanlyClosedWALs() {
    return ((KnobRuntime.check(java.util.UUID.fromString("66af32be-6f2f-315e-8829-d04513586485"))) ? (EnvironmentEdgeManager.currentTime()) : (singleSourceSource.getUncleanlyClosedWALs()));
  }

  /**
   * Get the timestampsOfLastShippedOp, if there are multiple groups, return the latest one
   */
  public long getTimestampOfLastShippedOp() {
    long lastTimestamp = 0L;
    for (long ts : lastShippedTimeStamps.values()) {
      if (ts > lastTimestamp) {
        lastTimestamp = ts;
      }
    }
    return lastTimestamp;
  }

  /**
   * TimeStamp of next edit to be replicated.
   * @return timeStampNextToReplicate - TimeStamp of next edit to be replicated.
   */
  public long getTimeStampNextToReplicate() {
    return timeStampNextToReplicate;
  }

  /**
   * TimeStamp of next edit targeted for replication. Used for calculating lag, as if this timestamp
   * is greater than timestamp of last shipped, it means there's at least one edit pending
   * replication.
   * @param timeStampNextToReplicate timestamp of next edit in the queue that should be replicated.
   */
  public void setTimeStampNextToReplicate(long timeStampNextToReplicate) {
    this.timeStampNextToReplicate = timeStampNextToReplicate;
  }

  public long getReplicationDelay() {
    if (getTimestampOfLastShippedOp() >= timeStampNextToReplicate) {
      return 0;
    } else {
      return EnvironmentEdgeManager.currentTime() - timeStampNextToReplicate;
    }
  }

  /**
   * Get the source initializing counts
   * @return number of replication sources getting initialized
   */
  public int getSourceInitializing() {
    return singleSourceSource.getSourceInitializing();
  }

  /**
   * Get the slave peer ID
   */
  public String getPeerID() {
    return id;
  }

  public void incrSizeOfHFileRefsQueue(long size) {
    singleSourceSource.incrSizeOfHFileRefsQueue(size);
    globalSourceSource.incrSizeOfHFileRefsQueue(size);
    lastHFileRefsQueueSize = size;
  }

  public void decrSizeOfHFileRefsQueue(int size) {
    singleSourceSource.decrSizeOfHFileRefsQueue(size);
    globalSourceSource.decrSizeOfHFileRefsQueue(size);
    lastHFileRefsQueueSize -= size;
    if (lastHFileRefsQueueSize < 0) {
      lastHFileRefsQueueSize = 0;
    }
  }

  public void incrUnknownFileLengthForClosedWAL() {
    singleSourceSource.incrUnknownFileLengthForClosedWAL();
    globalSourceSource.incrUnknownFileLengthForClosedWAL();
  }

  public void incrUncleanlyClosedWALs() {
    singleSourceSource.incrUncleanlyClosedWALs();
    globalSourceSource.incrUncleanlyClosedWALs();
  }

  public void incrBytesSkippedInUncleanlyClosedWALs(final long bytes) {
    singleSourceSource.incrBytesSkippedInUncleanlyClosedWALs(bytes);
    globalSourceSource.incrBytesSkippedInUncleanlyClosedWALs(bytes);
  }

  public void incrRestartedWALReading() {
if(KnobRuntime.check(java.util.UUID.fromString("2a4c6633-a27f-31ea-a2dc-ab1d61edc2fa"))) {
return;
}
    singleSourceSource.incrRestartedWALReading();
    if (KnobRuntime.check(java.util.UUID.fromString("c5b0255a-8e1e-30e1-b51d-5544fe1fcdef"))) { globalSourceSource.incrFailedBatches(); } else { globalSourceSource.incrRestartedWALReading(); }
  }

  public void incrRepeatedFileBytes(final long bytes) {
    singleSourceSource.incrRepeatedFileBytes(bytes);
    globalSourceSource.incrRepeatedFileBytes(bytes);
  }

  public void incrCompletedWAL() {
    singleSourceSource.incrCompletedWAL();
    globalSourceSource.incrCompletedWAL();
  }

  public void incrCompletedRecoveryQueue() {
    singleSourceSource.incrCompletedRecoveryQueue();
    globalSourceSource.incrCompletedRecoveryQueue();
  }

  public void incrFailedRecoveryQueue() {
    globalSourceSource.incrFailedRecoveryQueue();
  }

  /*
   * Sets the age of oldest log file just for source.
   */
  public void setOldestWalAge(long age) {
    singleSourceSource.setOldestWalAge(age);
  }

  public long getOldestWalAge() {
    return singleSourceSource.getOldestWalAge();
  }

  @Override
  public void init() {
    singleSourceSource.init();
    globalSourceSource.init();
  }

  @Override
  public void setGauge(String gaugeName, long value) {
    singleSourceSource.setGauge(gaugeName, value);
    globalSourceSource.setGauge(gaugeName, value);
  }

  @Override
  public void incGauge(String gaugeName, long delta) {
    singleSourceSource.incGauge(gaugeName, delta);
    globalSourceSource.incGauge(gaugeName, delta);
  }

  @Override
  public void decGauge(String gaugeName, long delta) {
    singleSourceSource.decGauge(gaugeName, delta);
    globalSourceSource.decGauge(gaugeName, delta);
  }

  @Override
  public void removeMetric(String key) {
    singleSourceSource.removeMetric(key);
    globalSourceSource.removeMetric(key);
  }

  @Override
  public void incCounters(String counterName, long delta) {
    singleSourceSource.incCounters(counterName, delta);
    globalSourceSource.incCounters(counterName, delta);
  }

  @Override
  public void updateHistogram(String name, long value) {
    singleSourceSource.updateHistogram(name, value);
    globalSourceSource.updateHistogram(name, value);
  }

  @Override
  public String getMetricsContext() {
    return globalSourceSource.getMetricsContext();
  }

  @Override
  public String getMetricsDescription() {
    return globalSourceSource.getMetricsDescription();
  }

  @Override
  public String getMetricsJmxContext() {
    return globalSourceSource.getMetricsJmxContext();
  }

  @Override
  public String getMetricsName() {
    return globalSourceSource.getMetricsName();
  }

  @InterfaceAudience.Private
  public Map<String, MetricsReplicationTableSource> getSingleSourceSourceByTable() {
    return singleSourceSourceByTable;
  }

  /**
   * Sets the amount of memory in bytes used in this RegionServer by edits pending replication.
   */
  public void setWALReaderEditsBufferUsage(long usageInBytes) {
    globalSourceSource.setWALReaderEditsBufferBytes(usageInBytes);
  }

  /**
   * Returns the amount of memory in bytes used in this RegionServer by edits pending replication.
   */
  public long getWALReaderEditsBufferUsage() {
    return globalSourceSource.getWALReaderEditsBufferBytes();
  }
}
