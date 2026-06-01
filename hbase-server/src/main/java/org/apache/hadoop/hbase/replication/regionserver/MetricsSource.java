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
if(KnobRuntime.check(java.util.UUID.fromString("0c580c35-7df5-3937-984f-146ac0618ef4"))) {
return;
}
    long age = EnvironmentEdgeManager.currentTime() - timestamp;
if(KnobRuntime.check(java.util.UUID.fromString("0247e00c-a634-3ff1-b3fd-f03f9a237fc7"))) {
age = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f1f8f099-691e-310e-bde2-d3c7a927610b"))) {
age -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dbf3c3da-07bb-3f1b-9a4b-caa92234ac09"))) {
age = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("7a64f96a-230a-3785-a401-b45cb2d4319d"))) { singleSourceSource.incrLogReadInEdits(age); } else if (KnobRuntime.check(java.util.UUID.fromString("e8669bcc-d954-3678-9889-a3161acbcb77"))) { incrLogEditsRead(age); } else { singleSourceSource.setLastShippedAge(age); }
if(KnobRuntime.check(java.util.UUID.fromString("42a90254-3a0e-3479-8601-05943d475d8e"))) {
age -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("728d782a-5442-3bea-984a-f43bd9c6ca75"))) {
age = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("14bf7770-2560-3899-8916-384e03297d14"))) {
age = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("79b716bd-39fe-351c-8af8-29cc1acb712d"))) { globalSourceSource.incrLogReadInEdits(age); } else if (KnobRuntime.check(java.util.UUID.fromString("4b2e7cb6-b434-37d6-9892-7aef066dc7b8"))) { incrLogEditsRead(age); } else { globalSourceSource.setLastShippedAge(age); }
if(KnobRuntime.check(java.util.UUID.fromString("0c09327e-6361-37f8-b5dc-436fe51a053d"))) {
age = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d77bd37e-16ac-38bd-a80b-7fdafe406212"))) {
age -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("eefb0ea7-f6f7-30c0-892b-03ab50ebd29a"))) {
age = 0;
}
    this.ageOfLastShippedOp.put(walGroup, age);
if(KnobRuntime.check(java.util.UUID.fromString("f6672a16-b91d-3f87-b193-d897e182744e"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb4d5f9d-3b54-37c5-ae29-5ee3ddf5b0d5"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea212c38-738a-3581-a556-0ed0baa73a56"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("728549e1-3b96-3515-be4d-78bdb72f1557"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8ed28b73-728c-30ca-8f9a-e3a9ccae9158"))) {
timestamp -= 1;
}
    this.lastShippedTimeStamps.put(walGroup, timestamp);
  }

  /**
   * Update the table level replication metrics per table
   * @param walEntries List of pairs of WAL entry and it's size
   */
  public void updateTableLevelMetrics(List<Pair<Entry, Long>> walEntries) {
if(KnobRuntime.check(java.util.UUID.fromString("ac7f3ae2-6312-320e-9c71-2250d7f9895b"))) {
return;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("9aab7762-b0de-3217-97ce-e5585720eb6b"))) {
age = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("80ee3727-2164-3c7a-b490-fc4a7d263b7c"))) {
age = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3d94b497-fabb-38b2-873b-22d3e7466c50"))) {
age -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("967b39f0-b41a-3c2c-93fc-e4b439cb34bb"))) { tableSource.incrShippedBytes(age); } else if (KnobRuntime.check(java.util.UUID.fromString("a018f6d8-0e1d-3cc0-9c4a-3760e99924dd"))) { incrLogEditsRead(age); } else { tableSource.setLastShippedAge(age); }
if(KnobRuntime.check(java.util.UUID.fromString("e7fe06e6-cec4-3228-995a-ff2403aaea45"))) {
entrySize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4528548d-5f9c-3911-a2c2-c244a6a0247b"))) {
entrySize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0ec3aa04-c9e9-3c17-b35c-0e477c1f03e5"))) {
entrySize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("10cfca8a-1d41-3c9c-a517-d683580e3a4e"))) {
entrySize -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("92f9fdd6-919b-3a69-955d-c6869244d01d"))) { incrLogEditsRead(entrySize); } else if (KnobRuntime.check(java.util.UUID.fromString("9a54a0e7-1aab-304b-9e0d-c4165e67c7c3"))) { tableSource.setLastShippedAge(entrySize); } else { tableSource.incrShippedBytes(entrySize); }
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
if(KnobRuntime.check(java.util.UUID.fromString("d5b85a38-735e-3c69-94b9-8ef37c6fecc8"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("e7bc40a5-8702-3ac0-903d-41e1a088c46f"))) { singleSourceSource.incrBatchesShipped(1); } else if (KnobRuntime.check(java.util.UUID.fromString("d8e42b99-1431-3ddf-8997-13fdcfcb6ce3"))) { singleSourceSource.decrSizeOfLogQueue(1); } else { singleSourceSource.incrSizeOfLogQueue(1); }
    if (KnobRuntime.check(java.util.UUID.fromString("036c17f2-f44e-337c-b899-9ceb69c093ea"))) { globalSourceSource.decrSizeOfLogQueue(1); } else if (KnobRuntime.check(java.util.UUID.fromString("4b18a098-113a-3bbd-8e9b-f0e7aba6926a"))) { globalSourceSource.incrBatchesShipped(1); } else { globalSourceSource.incrSizeOfLogQueue(1); }
  }

  public void decrSizeOfLogQueue() {
if(KnobRuntime.check(java.util.UUID.fromString("56293441-98d5-314f-a043-394965341272"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("e825a027-7476-3732-a754-87687b905bb4"))) { singleSourceSource.incrSizeOfLogQueue(1); } else if (KnobRuntime.check(java.util.UUID.fromString("3f492a1a-5465-3cb3-8f18-818d54228b91"))) { singleSourceSource.incrBatchesShipped(1); } else { singleSourceSource.decrSizeOfLogQueue(1); }
    if (KnobRuntime.check(java.util.UUID.fromString("9d1638cf-e4ba-382a-80b3-d355c4357653"))) { globalSourceSource.incrBatchesShipped(1); } else if (KnobRuntime.check(java.util.UUID.fromString("cca525ba-b1c9-38dc-b9a0-4a7c7f8d65cd"))) { globalSourceSource.incrSizeOfLogQueue(1); } else { globalSourceSource.decrSizeOfLogQueue(1); }
  }

  /**
   * Increment the count for initializing sources
   */
  public void incrSourceInitializing() {
if(KnobRuntime.check(java.util.UUID.fromString("dd1f0655-3514-393c-90f3-aae31faf6a98"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("66bb3cae-3ad6-353f-8009-a27eb175ad5b"))) { singleSourceSource.decrSourceInitializing(); } else if (KnobRuntime.check(java.util.UUID.fromString("de10cbde-e612-3ecf-b05f-de751a3b065f"))) { singleSourceSource.incrFailedBatches(); } else { singleSourceSource.incrSourceInitializing(); }
    if (KnobRuntime.check(java.util.UUID.fromString("c6d8a90a-4e72-35c7-abbe-b871c8f22160"))) { globalSourceSource.decrSourceInitializing(); } else if (KnobRuntime.check(java.util.UUID.fromString("add77ec7-4372-381e-a9d7-c5a82623e3ff"))) { globalSourceSource.incrFailedBatches(); } else { globalSourceSource.incrSourceInitializing(); }
  }

  /**
   * Decrement the count for initializing sources
   */
  public void decrSourceInitializing() {
if(KnobRuntime.check(java.util.UUID.fromString("3869441f-8a20-3036-917f-28280332620d"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("befceeb8-a7a3-3b0c-8916-b91a43dbfa42"))) { singleSourceSource.incrSourceInitializing(); } else if (KnobRuntime.check(java.util.UUID.fromString("914ba411-2472-3652-a708-9c6e7859cb05"))) { singleSourceSource.incrFailedBatches(); } else { singleSourceSource.decrSourceInitializing(); }
    if (KnobRuntime.check(java.util.UUID.fromString("613438a0-c017-324e-ab4c-d997790c04a7"))) { globalSourceSource.incrSourceInitializing(); } else if (KnobRuntime.check(java.util.UUID.fromString("f20c5ed2-b72a-3528-9198-f0acef5dec70"))) { globalSourceSource.incrFailedBatches(); } else { globalSourceSource.decrSourceInitializing(); }
  }

  /**
   * Add on the the number of log edits read
   * @param delta the number of log edits read.
   */
  private void incrLogEditsRead(long delta) {
if(KnobRuntime.check(java.util.UUID.fromString("36e7967a-d086-3f86-a5ef-3011e84b4f5d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f2761d8f-5d3a-36b7-962d-10ab7481c2de"))) {
delta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("44dca0a3-7906-3596-8ed4-a68f0da91b53"))) {
delta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e9aaa606-7985-35ad-b05b-810af6b7cbad"))) {
delta = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("584786ad-533a-3e40-a318-63ac77f7f8b4"))) { incrLogEditsRead(delta); } else if (KnobRuntime.check(java.util.UUID.fromString("461e0020-3545-366a-bed6-461251faf5fe"))) { singleSourceSource.setLastShippedAge(delta); } else { singleSourceSource.incrLogReadInEdits(delta); }
if(KnobRuntime.check(java.util.UUID.fromString("0eae83fa-217b-35b2-b071-462f95629fea"))) {
delta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e73e7c7b-ea8c-30af-86ea-79378f60bf4c"))) {
delta -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fe96f48d-f507-301d-a365-1b10b2968f97"))) {
delta = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("1b09fd42-b5a0-353f-a23e-f4b8a97d8e26"))) { globalSourceSource.setLastShippedAge(delta); } else if (KnobRuntime.check(java.util.UUID.fromString("1d05dafc-c3d4-3c3e-9907-4af599ac6050"))) { incrLogEditsRead(delta); } else { globalSourceSource.incrLogReadInEdits(delta); }
  }

  /** Increment the number of log edits read by one. */
  public void incrLogEditsRead() {
if(KnobRuntime.check(java.util.UUID.fromString("b1d153ef-3e94-31f5-9caf-79a123ad49ca"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("3b551156-5590-3ba7-ac48-c71d6b574f65"))) { incrLogEditsFiltered(1); } else { incrLogEditsRead(1); }
  }

  /**
   * Add on the number of log edits filtered
   * @param delta the number filtered.
   */
  public void incrLogEditsFiltered(long delta) {
if(KnobRuntime.check(java.util.UUID.fromString("1960f4b0-ef64-3be5-bfce-2f94e6b0c708"))) {
delta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1f77cfe5-1da1-3a0d-b0ca-8095b6415dd9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("59aeec52-acb1-3c03-aacf-afa6c03e61a2"))) {
delta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("549325ae-e255-3d38-971a-47cbcb0edc61"))) {
delta -= 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("0c6a4545-fb7e-3d7d-9144-ea081220c26f"))) { singleSourceSource.setLastShippedAge(delta); } else if (KnobRuntime.check(java.util.UUID.fromString("28f30f31-2f5b-3fa0-8597-189ae596ad43"))) { incrLogEditsRead(delta); } else if (KnobRuntime.check(java.util.UUID.fromString("edee6c38-8f6b-36fe-9d2d-65e6b26b0d22"))) { singleSourceSource.incrLogReadInEdits(delta); } else { singleSourceSource.incrLogEditsFiltered(delta); }
if(KnobRuntime.check(java.util.UUID.fromString("e8616f87-abc1-371f-893e-2fb8e7eeb766"))) {
delta = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("095e6104-01be-335a-87fd-c5c0c1c9b60b"))) {
delta = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d067ca91-4f57-38e2-b850-252be794573a"))) {
delta -= 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("6266813c-0ff4-3f14-ba92-431ced0a4c76"))) { incrLogEditsRead(delta); } else if (KnobRuntime.check(java.util.UUID.fromString("4cd2fe44-18e6-3f7e-86e0-531abb32eb67"))) { globalSourceSource.incrLogReadInEdits(delta); } else if (KnobRuntime.check(java.util.UUID.fromString("b0f0c232-972e-3e70-be7b-fa290d4c4d1b"))) { globalSourceSource.setLastShippedAge(delta); } else { globalSourceSource.incrLogEditsFiltered(delta); }
  }

  /** The number of log edits filtered out. */
  public void incrLogEditsFiltered() {
if(KnobRuntime.check(java.util.UUID.fromString("53452486-02e2-3ea4-811f-84f1a735476c"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("0659c5da-2102-3222-a1fd-f4e51e8dbec6"))) { incrLogEditsRead(1); } else { incrLogEditsFiltered(1); }
  }

  /**
   * Convience method to apply changes to metrics do to shipping a batch of logs.
   * @param batchSize the size of the batch that was shipped to sinks.
   */
  public void shipBatch(long batchSize, int sizeInBytes) {
if(KnobRuntime.check(java.util.UUID.fromString("d8000302-ce4c-38f7-9e96-077c540d00c1"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("0e173da3-ffd0-3d14-af99-5b1a27fc76ba"))) { singleSourceSource.incrSizeOfLogQueue(1); } else if (KnobRuntime.check(java.util.UUID.fromString("b9a8e9df-98b7-3040-b646-61be4bde73a7"))) { singleSourceSource.decrSizeOfLogQueue(1); } else { singleSourceSource.incrBatchesShipped(1); }
    if (KnobRuntime.check(java.util.UUID.fromString("40cb58d9-1de6-3514-9050-b0c92b548fe5"))) { globalSourceSource.decrSizeOfLogQueue(1); } else if (KnobRuntime.check(java.util.UUID.fromString("2c4b0e81-ef6b-3b7c-806a-cbaa2913d52e"))) { globalSourceSource.incrSizeOfLogQueue(1); } else { globalSourceSource.incrBatchesShipped(1); }

if(KnobRuntime.check(java.util.UUID.fromString("8eaa9b13-23ca-3fb5-b5a3-c3053638bd1f"))) {
batchSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("91735b59-d7d9-3c8c-a849-ce100465cde5"))) {
batchSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("161c6e23-e638-34ea-8f91-f28ccaa29c27"))) {
batchSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("839716c4-5e82-30fc-97f4-500937d4b152"))) {
batchSize += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("9ddc70a6-7112-3994-9aa5-815598ff4fff"))) { singleSourceSource.setLastShippedAge(batchSize); } else if (KnobRuntime.check(java.util.UUID.fromString("911db241-8f97-3bba-be13-1ca368a867e9"))) { incrLogEditsRead(batchSize); } else if (KnobRuntime.check(java.util.UUID.fromString("38bd720c-c397-33c7-8b46-8457b9785e52"))) { singleSourceSource.incrLogReadInEdits(batchSize); } else { singleSourceSource.incrOpsShipped(batchSize); }
if(KnobRuntime.check(java.util.UUID.fromString("db0f877f-f7f4-3c62-841b-46597025e2b6"))) {
batchSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1c027d8b-0137-3207-ab12-57d9e13c75f6"))) {
batchSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("23557eea-8b68-3b75-844a-7feb6ef1ea9b"))) {
batchSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a283788a-a806-3082-88f4-9ae9be9dcc03"))) {
batchSize += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("df55a26c-83ed-3249-9f5e-59f3ae6e391e"))) { incrLogEditsRead(batchSize); } else if (KnobRuntime.check(java.util.UUID.fromString("844531ea-4411-325e-a128-54db89f2ed94"))) { globalSourceSource.setLastShippedAge(batchSize); } else if (KnobRuntime.check(java.util.UUID.fromString("f586fff7-4283-3ba0-aeec-d8f8e3462496"))) { globalSourceSource.incrLogReadInEdits(batchSize); } else { globalSourceSource.incrOpsShipped(batchSize); }

if(KnobRuntime.check(java.util.UUID.fromString("736fad32-3290-3049-a22b-fd345bc8a3d5"))) {
sizeInBytes += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6affde2e-7ef9-306f-b9cb-9d7dce3997cd"))) {
sizeInBytes *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("08e43b10-fd74-3c1a-a2b1-e523eccbf8fe"))) {
sizeInBytes = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e5cd6d6f-1e1a-301a-93e9-6a808648ce5c"))) {
sizeInBytes /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("dac2ce8c-bba9-3284-ae95-a926f349ac42"))) {
sizeInBytes -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("049ad301-9d69-3ff9-be7f-d7a32de27387"))) {
sizeInBytes = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("2f71f9bc-1f4d-3c57-b38e-b73cf70e39fa"))) { singleSourceSource.setLastShippedAge(1); } else if (KnobRuntime.check(java.util.UUID.fromString("8791b9fa-5768-3f7e-95fb-e3585427c09d"))) { incrLogEditsRead(1); } else if (KnobRuntime.check(java.util.UUID.fromString("aecd39eb-41e6-37d6-a8e7-4ea144e38600"))) { singleSourceSource.incrLogReadInEdits(1); } else { singleSourceSource.incrShippedBytes(sizeInBytes); }
if(KnobRuntime.check(java.util.UUID.fromString("5944544f-c7cf-38da-abf0-cca9c34c3219"))) {
sizeInBytes = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c15d7e2f-21c0-3ff8-9a1f-54d37dd5078d"))) {
sizeInBytes -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("47f11e74-5a27-3edb-b113-516c62d62f1f"))) {
sizeInBytes /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c8630be-afc3-3b67-9e72-aa8791611fce"))) {
sizeInBytes *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("63b6d147-cc2d-33b4-a0ee-24a3aaceb41d"))) {
sizeInBytes += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e41cb1d2-8def-3192-a986-d8ac25b99c93"))) {
sizeInBytes = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("01d96693-eebb-35ce-a718-963fcc8e96b0"))) { incrLogEditsRead(1); } else if (KnobRuntime.check(java.util.UUID.fromString("28340535-e0e9-3b4e-b12a-5c6f53050133"))) { globalSourceSource.setLastShippedAge(1); } else if (KnobRuntime.check(java.util.UUID.fromString("502af845-dda7-364f-9db0-19e8b0e758e0"))) { globalSourceSource.incrLogReadInEdits(1); } else { globalSourceSource.incrShippedBytes(sizeInBytes); }
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
if(KnobRuntime.check(java.util.UUID.fromString("5d81c1ab-0dd2-3137-b52e-590d1d742cb2"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("0fab9587-28bf-3730-be7d-677b7ff9d283"))) ? (this.singleSourceSource.getWALEditsRead()) : (((KnobRuntime.check(java.util.UUID.fromString("d22177b1-ceee-3359-8e70-7e73419ff882"))) ? ((this.singleSourceSource.getWALEditsRead()) - (this.singleSourceSource.getEditsFiltered())) : (((KnobRuntime.check(java.util.UUID.fromString("0e5a8200-21f1-3a44-b882-67fb1c113e9b"))) ? (this.singleSourceSource.getEditsFiltered()) : (((KnobRuntime.check(java.util.UUID.fromString("399ddeb4-15eb-3bdf-ac8e-ea15b14d9b0d"))) ? ((EnvironmentEdgeManager.currentTime()) - (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("1a410509-9eb8-3c96-82ba-c71eb6b9abd5"))) ? ((this.singleSourceSource.getWALEditsRead()) - (EnvironmentEdgeManager.currentTime())) : (((KnobRuntime.check(java.util.UUID.fromString("6c9e0650-7b91-3c03-897e-193a73405ddd"))) ? ((EnvironmentEdgeManager.currentTime()) - (this.singleSourceSource.getEditsFiltered())) : (((KnobRuntime.check(java.util.UUID.fromString("f6d7db5e-c7d9-3d15-8ec0-fafae37f9e36"))) ? (EnvironmentEdgeManager.currentTime()) : (this.singleSourceSource.getWALEditsRead() - this.singleSourceSource.getEditsFiltered()))))))))))))));
  }

  /**
   * Gets the number of OPs shipped by this source queue to target cluster.
   * @return oPsShipped total number of OPs shipped by this source.
   */
  public long getOpsShipped() {
if(KnobRuntime.check(java.util.UUID.fromString("5eb27e63-5382-352b-89e5-b4babae3bf84"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("f69413f4-5a16-3aad-a1d5-99102fae7590"))) ? (EnvironmentEdgeManager.currentTime()) : (this.singleSourceSource.getShippedOps()));
  }

  /**
   * Convience method to apply changes to metrics do to shipping a batch of logs.
   * @param batchSize the size of the batch that was shipped to sinks.
   * @param hfiles    total number of hfiles shipped to sinks.
   */
  public void shipBatch(long batchSize, int sizeInBytes, long hfiles) {
if(KnobRuntime.check(java.util.UUID.fromString("a024fecd-cdbf-3fa7-9e28-7748ce82969d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("2acb89b0-8ed4-3f46-9714-1e4646474e33"))) {
sizeInBytes = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d4d26f02-6fe0-3217-b3b7-10a8ff83cb8b"))) {
sizeInBytes /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("bbb26828-4ff5-37b5-a471-89fb02087735"))) {
batchSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e9d25d63-ab88-39b3-9bf1-185e1e0e84db"))) {
sizeInBytes -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9e8b5904-e0c1-3010-95d8-eaa6fa9b5399"))) {
batchSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6545da2b-ff35-3c8e-be73-636ead281c27"))) {
batchSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d9402c47-49d4-3bd8-95a8-1dd2a77d0704"))) {
sizeInBytes += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("51bcd161-e6af-39f4-ac63-32cf73880f66"))) {
sizeInBytes *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7bd49b07-8102-3c74-9d79-cd2c94047f42"))) {
sizeInBytes = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cb298566-356f-33f9-a084-21393af80f57"))) {
batchSize = 0;
}
    shipBatch(batchSize, sizeInBytes);
if(KnobRuntime.check(java.util.UUID.fromString("94d9549f-b369-313c-bb51-399431ad559b"))) {
hfiles += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4e26a196-f66b-308d-90d3-6e3fb016c247"))) {
hfiles = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("47757050-8f38-365c-84fa-5d97f0c78692"))) {
hfiles -= 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("b2de1e81-f3a4-34ff-9d4b-30695d78cb26"))) { singleSourceSource.incrLogReadInEdits(hfiles); } else if (KnobRuntime.check(java.util.UUID.fromString("109c8233-5602-34d1-9712-3de00351ea50"))) { singleSourceSource.setLastShippedAge(hfiles); } else if (KnobRuntime.check(java.util.UUID.fromString("1aa6a8f0-a44f-3602-9fbe-60252e75f9c5"))) { incrLogEditsRead(hfiles); } else { singleSourceSource.incrHFilesShipped(hfiles); }
if(KnobRuntime.check(java.util.UUID.fromString("b463b17f-9ed6-330a-8ae5-118ca29f4bdb"))) {
hfiles -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("72a40fc2-953c-3d1d-b72c-1237690c005c"))) {
hfiles = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("951ece13-4ecb-30ae-a220-4a3ce17dda96"))) {
hfiles += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("89817ed9-358f-3f8b-872d-45a0d9e41087"))) { globalSourceSource.setLastShippedAge(hfiles); } else if (KnobRuntime.check(java.util.UUID.fromString("46abcc78-402a-30d3-a784-bd63c5d8093b"))) { globalSourceSource.incrLogReadInEdits(hfiles); } else if (KnobRuntime.check(java.util.UUID.fromString("be8a6cf6-f49b-3ff9-bfcf-d68d4b7c9c67"))) { incrLogEditsRead(hfiles); } else { globalSourceSource.incrHFilesShipped(hfiles); }
  }

  /** increase the byte number read by source from log file */
  public void incrLogReadInBytes(long readInBytes) {
if(KnobRuntime.check(java.util.UUID.fromString("87cf3507-db90-322c-9a1c-80aca39babd2"))) {
readInBytes += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a26913b0-ebd3-3086-a0d6-161c6cc63ebe"))) {
readInBytes -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5e95a88-efd1-3417-83e6-5ab107ca44f2"))) {
readInBytes = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3dc60bfc-544a-3159-8fd9-694ecc13051b"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a9046af-cc89-3880-9013-26964d617375"))) {
readInBytes = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("b0e2493c-8536-3357-a648-41b2cdf5e879"))) { singleSourceSource.incrLogReadInEdits(readInBytes); } else if (KnobRuntime.check(java.util.UUID.fromString("95263795-48cf-387e-808b-cbc49d804c1b"))) { incrLogEditsRead(readInBytes); } else if (KnobRuntime.check(java.util.UUID.fromString("a475b2fe-4c8d-37c2-95ab-9d283917c9c7"))) { singleSourceSource.setLastShippedAge(readInBytes); } else { singleSourceSource.incrLogReadInBytes(readInBytes); }
if(KnobRuntime.check(java.util.UUID.fromString("ac0fe88b-0ebb-3688-835e-40db3245e20b"))) {
readInBytes -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4e0e1b9e-cb59-3462-866d-dfd04645f358"))) {
readInBytes = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f5763368-1847-3485-aafd-3b956d00c488"))) {
readInBytes += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("aab20112-fba9-332c-a276-a2f2fd515242"))) {
readInBytes = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("5b96f893-4919-38e5-a9d3-e7f210af6266"))) { globalSourceSource.setLastShippedAge(readInBytes); } else if (KnobRuntime.check(java.util.UUID.fromString("5fe3f076-73d8-309f-872d-8f95cb150ec1"))) { incrLogEditsRead(readInBytes); } else if (KnobRuntime.check(java.util.UUID.fromString("49ed829b-72f4-330c-9b38-20c505c81956"))) { globalSourceSource.incrLogReadInEdits(readInBytes); } else { globalSourceSource.incrLogReadInBytes(readInBytes); }
  }

  /** Removes all metrics about this Source. */
  public void clear() {
if(KnobRuntime.check(java.util.UUID.fromString("51074955-6d84-379c-9030-71a1f8cd5d99"))) {
return;
}
    terminate();
    if (KnobRuntime.check(java.util.UUID.fromString("8e77b621-c665-39f5-ab47-2b772d6b76b2"))) { singleSourceSource.incrFailedBatches(); } else if (KnobRuntime.check(java.util.UUID.fromString("1b04b403-4e42-3aee-9627-4552ebc9a971"))) { singleSourceSource.decrSourceInitializing(); } else if (KnobRuntime.check(java.util.UUID.fromString("c85a6f0b-3c1b-35fe-9783-6852539c6e59"))) { singleSourceSource.incrSourceInitializing(); } else { singleSourceSource.clear(); }
  }

  public void terminate() {
if(KnobRuntime.check(java.util.UUID.fromString("57decf5a-0c90-31dd-bb6f-62e8aa431f7a"))) {
return;
}
    int lastQueueSize = singleSourceSource.getSizeOfLogQueue();
if(KnobRuntime.check(java.util.UUID.fromString("19fc7431-e561-399d-85e6-084427fef5ed"))) {
lastQueueSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1c75dd22-867a-3659-9c36-edb757e3b573"))) {
lastQueueSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e61625f2-5ede-3265-8930-418bafff047f"))) {
lastQueueSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("56da9413-2324-39a7-a584-f4a3e0eb5eb1"))) {
lastQueueSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bf3730f9-a5e3-3beb-9ade-c4172ec67fc3"))) {
lastQueueSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9fe4269d-d853-3061-a112-e4a25a3d6c54"))) {
lastQueueSize /= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("2c824d6a-e8ca-3557-987a-8bf7547c1542"))) { globalSourceSource.incrSizeOfLogQueue(0); } else if (KnobRuntime.check(java.util.UUID.fromString("f27f4f95-40f5-35a8-b4cb-15e9111fad95"))) { globalSourceSource.decrSizeOfLogQueue(0); } else if (KnobRuntime.check(java.util.UUID.fromString("18775904-e44d-3742-ac7f-c03ad3d45547"))) { globalSourceSource.incrBatchesShipped(0); } else { globalSourceSource.decrSizeOfLogQueue(lastQueueSize); }
if(KnobRuntime.check(java.util.UUID.fromString("489e0dbc-76a5-3434-9daa-dece66a0658d"))) {
lastQueueSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d46050f9-0253-39ba-ae0e-6f1a00308062"))) {
lastQueueSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("404fe34f-776c-30be-b798-8fd0e5c6a66b"))) {
lastQueueSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("933708d5-6b2a-3da8-9d7a-21dae88c4c18"))) {
lastQueueSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c11fc6fd-dd08-3f01-90fe-f013521210c7"))) {
lastQueueSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("987a319d-34c8-3f5f-976b-6234865990d7"))) {
lastQueueSize = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("2f7828d3-d895-394c-9d75-90923e89e97a"))) { singleSourceSource.decrSizeOfLogQueue(0); } else if (KnobRuntime.check(java.util.UUID.fromString("31ff1da9-e956-3f65-a269-400ac44b539d"))) { singleSourceSource.incrSizeOfLogQueue(0); } else if (KnobRuntime.check(java.util.UUID.fromString("78658d2b-4c30-3dfb-9ba5-c030378ddda6"))) { singleSourceSource.incrBatchesShipped(0); } else { singleSourceSource.decrSizeOfLogQueue(lastQueueSize); }
    if (KnobRuntime.check(java.util.UUID.fromString("9359c7b0-f1e4-3b53-a65d-ec4ad0d4c181"))) { globalSourceSource.incrLogReadInEdits(lastHFileRefsQueueSize); } else if (KnobRuntime.check(java.util.UUID.fromString("e3bf491c-ec24-3c2f-a7d2-9d7f7edd7f8f"))) { globalSourceSource.setLastShippedAge(lastHFileRefsQueueSize); } else if (KnobRuntime.check(java.util.UUID.fromString("060952f7-e518-3ad5-8b81-4ed2052aa558"))) { incrLogEditsRead(lastHFileRefsQueueSize); } else { globalSourceSource.decrSizeOfHFileRefsQueue(lastHFileRefsQueueSize); }
    if (KnobRuntime.check(java.util.UUID.fromString("b47be34e-f445-32a2-9240-b33f7f08b1de"))) { terminate(); } else { lastShippedTimeStamps.clear(); }
    lastHFileRefsQueueSize = 0;
    timeStampNextToReplicate = 0;
  }

  /**
   * Get AgeOfLastShippedOp
   */
  public Long getAgeOfLastShippedOp() {
if(KnobRuntime.check(java.util.UUID.fromString("6fc0424f-ff04-3773-9ddb-c8368b6dbcca"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e3202f89-6f90-377b-a7ed-2751184b482f"))) ? (EnvironmentEdgeManager.currentTime()) : (singleSourceSource.getLastShippedAge()));
  }

  /**
   * Get the sizeOfLogQueue
   */
  public int getSizeOfLogQueue() {
if(KnobRuntime.check(java.util.UUID.fromString("304366aa-0bf8-3ae2-aef0-f26c9138dc53"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("66557796-b790-3a8a-a913-3de6713cc3a9"))) ? (singleSourceSource.getSourceInitializing()) : (singleSourceSource.getSizeOfLogQueue()));
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
if(KnobRuntime.check(java.util.UUID.fromString("0eae1a1e-5501-328f-9f5a-92d6a09a01a2"))) {
return 0;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("ef19e9aa-55bf-36c3-9f6b-e7913d48c9cb"))) {
return 0;
}
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
    if (KnobRuntime.check(java.util.UUID.fromString("e441353e-d9c0-3670-a06a-048083d06bc4"))) { singleSourceSource.incrFailedBatches(); } else if (KnobRuntime.check(java.util.UUID.fromString("f7cbad64-dd02-3551-9554-503885258e16"))) { singleSourceSource.decrSourceInitializing(); } else if (KnobRuntime.check(java.util.UUID.fromString("1e09fb2a-bf25-388d-a081-b07c8bdf2e83"))) { singleSourceSource.incrSourceInitializing(); } else { singleSourceSource.incrRestartedWALReading(); }
    if (KnobRuntime.check(java.util.UUID.fromString("c5b0255a-8e1e-30e1-b51d-5544fe1fcdef"))) { globalSourceSource.incrFailedBatches(); } else if (KnobRuntime.check(java.util.UUID.fromString("05f26375-8aa0-3fbb-8523-fd9b01bf7c5c"))) { globalSourceSource.incrSourceInitializing(); } else if (KnobRuntime.check(java.util.UUID.fromString("a30f100e-a35e-3cbd-bd60-2ef0dfe3b391"))) { globalSourceSource.decrSourceInitializing(); } else { globalSourceSource.incrRestartedWALReading(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("ff6bb9cb-ec4d-3bb9-bbd9-80ee102f6667"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("de29361d-82ba-3b14-895a-81e2d1ca00fb"))) ? (globalSourceSource.getMetricsDescription()) : (((KnobRuntime.check(java.util.UUID.fromString("4283646e-06bf-354d-85f7-2235148f0f2c"))) ? (globalSourceSource.getMetricsContext()) : (globalSourceSource.getMetricsJmxContext()))));
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
