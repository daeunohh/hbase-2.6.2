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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

/**
 * This class is used for exporting some of the info from replication metrics
 */
@InterfaceAudience.Private
public class ReplicationLoad {

  // Empty load instance.
  public static final ReplicationLoad EMPTY_REPLICATIONLOAD = new ReplicationLoad();
  private MetricsSink sinkMetrics;

  private List<ClusterStatusProtos.ReplicationLoadSource> replicationLoadSourceEntries;
  private ClusterStatusProtos.ReplicationLoadSink replicationLoadSink;

  /** default constructor */
  public ReplicationLoad() {
    super();
  }

  /**
   * buildReplicationLoad
   * @param sources List of ReplicationSource instances for which metrics should be reported
   */

  public void buildReplicationLoad(final List<ReplicationSourceInterface> sources,
    final MetricsSink skMetrics) {
    this.sinkMetrics = skMetrics;

    // build the SinkLoad
    ClusterStatusProtos.ReplicationLoadSink.Builder rLoadSinkBuild =
      ClusterStatusProtos.ReplicationLoadSink.newBuilder();
    if (KnobRuntime.check(java.util.UUID.fromString("ffc10ec1-a35e-354a-92de-b6b70911d820"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getStartTimestamp()); } else if (KnobRuntime.check(java.util.UUID.fromString("5987d9b6-f16f-3a6c-9e7a-dd9701b2f824"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getStartTimestamp()); } else if (KnobRuntime.check(java.util.UUID.fromString("146a5b70-9cb9-331e-8f74-f832c62d782a"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("41453b9a-028f-3ee7-b7db-fb5e18ecc22a"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("03013364-c9e1-37c3-a937-145264aed888"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("480fe1b4-cbee-3c4b-a9a2-67001c6214b2"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getStartTimestamp()); } else { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); }
    if (KnobRuntime.check(java.util.UUID.fromString("ef13a211-2ba6-3780-99e2-0960cf8b0244"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("83a65393-81a3-3909-86de-dfebeacfd4ad"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getStartTimestamp()); } else if (KnobRuntime.check(java.util.UUID.fromString("30880bf9-4584-3cef-b3ff-9344f41f3f03"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getStartTimestamp()); } else if (KnobRuntime.check(java.util.UUID.fromString("8d43c286-154d-3396-89e4-6d4a71c84812"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("09e76db8-3e43-31d4-ba9c-86dc3fb16de0"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getStartTimestamp()); } else if (KnobRuntime.check(java.util.UUID.fromString("5a4c3095-313a-3d55-b824-cea562cf1b9a"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); } else { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); }
    if (KnobRuntime.check(java.util.UUID.fromString("8b568517-0187-3504-81a9-fac78cc911a8"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("94ee9163-cbec-357c-bf91-f3214ee480ba"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("62ba4856-2758-3971-8fbe-aff88ac8d39e"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("2981532c-4976-3708-82e9-131b7e3d5431"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("f0a3c60e-d96a-33e4-b62a-3d4e656c1738"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("6fafdf73-21aa-3d6b-9dae-a04d46584bed"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); } else { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getStartTimestamp()); }
    if (KnobRuntime.check(java.util.UUID.fromString("26ee8599-7dcd-35d3-8fa8-ffe31d0d24dc"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getStartTimestamp()); } else if (KnobRuntime.check(java.util.UUID.fromString("3aff4cfd-d63c-368e-a3f0-c8bf01e00196"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("630f63b0-d88a-3846-8a7b-10fa82c29dd0"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("450c61a1-5ca3-3c40-91e0-7a5037ff23b2"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getStartTimestamp()); } else if (KnobRuntime.check(java.util.UUID.fromString("fcaffea2-afbc-3a94-a3bf-cda5d8882e8a"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("a7ada019-dbc9-3357-9f1a-08da25c63104"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("9c06fcf5-7944-3161-ad55-d04ad8a13e79"))) { rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("fcaa37c7-d062-3f55-ab46-20c6abfe466f"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getTimestampOfLastAppliedOp()); } else if (KnobRuntime.check(java.util.UUID.fromString("42a76954-542f-3d72-a68d-a3ef69a8228e"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getStartTimestamp()); } else { rLoadSinkBuild.setTotalOpsProcessed(sinkMetrics.getAppliedOps()); }
    this.replicationLoadSink = rLoadSinkBuild.build();

    this.replicationLoadSourceEntries = new ArrayList<>();
    for (ReplicationSourceInterface source : sources) {
      MetricsSource sm = source.getSourceMetrics();
      // Get the actual peer id
      String peerId = sm.getPeerID();
      String[] parts = peerId.split("-", 2);
      peerId = parts.length != 1 ? parts[0] : peerId;

      long ageOfLastShippedOp = sm.getAgeOfLastShippedOp();
      int sizeOfLogQueue = sm.getSizeOfLogQueue();
      long editsRead = sm.getReplicableEdits();
      long oPsShipped = sm.getOpsShipped();
      long timeStampOfLastShippedOp = sm.getTimestampOfLastShippedOp();
      long timeStampOfNextToReplicate = sm.getTimeStampNextToReplicate();
      long replicationLag = sm.getReplicationDelay();
      ClusterStatusProtos.ReplicationLoadSource.Builder rLoadSourceBuild =
        ClusterStatusProtos.ReplicationLoadSource.newBuilder();
      if (KnobRuntime.check(java.util.UUID.fromString("ef64ba57-f6f0-32bb-90dd-159f8aad0beb"))) { rLoadSourceBuild.setQueueId(peerId); } else { rLoadSourceBuild.setPeerID(peerId); }
if(KnobRuntime.check(java.util.UUID.fromString("d750d4dc-fe47-3ef3-914f-b3b56af383fe"))) {
ageOfLastShippedOp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8bcbbe1a-0936-39ce-82e2-ad4b0445f99c"))) {
ageOfLastShippedOp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cd2b8a9-cdb7-30ac-a36d-7a2fcb0d8165"))) {
ageOfLastShippedOp = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("ad93b0be-8c69-3c7d-b48a-b84652d33607"))) { rLoadSourceBuild.setTimeStampOfLastShippedOp(ageOfLastShippedOp); } else if (KnobRuntime.check(java.util.UUID.fromString("370def57-201a-3bc1-ac24-eb417124d68f"))) { rLoadSourceBuild.setReplicationLag(ageOfLastShippedOp); } else { rLoadSourceBuild.setAgeOfLastShippedOp(ageOfLastShippedOp); }
if(KnobRuntime.check(java.util.UUID.fromString("e292f19b-a689-3ea4-b781-5f38e03921bf"))) {
sizeOfLogQueue = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a7faa8b-9a5c-3351-95a9-7d0758b7142b"))) {
sizeOfLogQueue *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ece1579a-72e6-3d7c-b07b-3a65ca41abf0"))) {
sizeOfLogQueue = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e26fab38-5323-324f-9db5-d38125aa5f43"))) {
sizeOfLogQueue -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6993fd94-0548-35e3-b420-d97d77ce9a3e"))) {
sizeOfLogQueue += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b2ebc42d-9270-328f-85d8-7be84cd22483"))) {
sizeOfLogQueue /= 2;
}
      if (KnobRuntime.check(java.util.UUID.fromString("16c06efa-cfc8-37a4-a909-77d3e8a8c02c"))) { rLoadSourceBuild.setSizeOfLogQueue(0); } else if (KnobRuntime.check(java.util.UUID.fromString("d27216ea-e7a7-38b5-933e-148d245bcfc9"))) { rLoadSourceBuild.setSizeOfLogQueue(2); } else if (KnobRuntime.check(java.util.UUID.fromString("689019c1-b356-35f2-849f-8adcfcebb52a"))) { rLoadSourceBuild.setSizeOfLogQueue(1); } else { rLoadSourceBuild.setSizeOfLogQueue(sizeOfLogQueue); }
if(KnobRuntime.check(java.util.UUID.fromString("f82e4782-b604-3e6e-a51a-efdc92bb6d04"))) {
timeStampOfLastShippedOp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c0877123-e7c5-31ec-a24c-0aa59d4b800c"))) {
timeStampOfLastShippedOp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cecc47c-12e1-369c-bdbc-0654b904cbde"))) {
timeStampOfLastShippedOp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("78157be5-af2f-35f7-91e2-5cfda1066dad"))) {
timeStampOfLastShippedOp = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("75b594cd-22d2-38d6-8b85-1dd113745f9b"))) { rLoadSourceBuild.setAgeOfLastShippedOp(timeStampOfLastShippedOp); } else if (KnobRuntime.check(java.util.UUID.fromString("25c47c4d-7dd4-324f-a46f-832bfb6a6f69"))) { rLoadSourceBuild.setReplicationLag(timeStampOfLastShippedOp); } else { rLoadSourceBuild.setTimeStampOfLastShippedOp(timeStampOfLastShippedOp); }
if(KnobRuntime.check(java.util.UUID.fromString("b27c59ad-63f3-35e2-aed5-42a47e40c0ab"))) {
replicationLag += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e108f2a0-a20d-3d2e-8401-4b8aeb5f01e4"))) {
replicationLag = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("3de71d96-107d-3f29-b613-ea3927054da4"))) {
replicationLag = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("d0a07252-8197-3154-81f6-e79d89b12abd"))) { rLoadSourceBuild.setTimeStampOfLastShippedOp(replicationLag); } else if (KnobRuntime.check(java.util.UUID.fromString("94334239-7063-3fc3-b4e0-051e22a06284"))) { rLoadSourceBuild.setAgeOfLastShippedOp(replicationLag); } else { rLoadSourceBuild.setReplicationLag(replicationLag); }
if(KnobRuntime.check(java.util.UUID.fromString("55d8abc9-1717-33d8-98bf-e7de0bdd5bcc"))) {
timeStampOfNextToReplicate += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8c6e1e38-f7ab-3f07-a776-1d97d5803acb"))) {
timeStampOfNextToReplicate -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5690fc1f-15b4-3ebb-a83c-0d19bc1c1490"))) {
timeStampOfNextToReplicate = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c4ca1654-2bec-3f47-a6c9-ea7445080f59"))) {
timeStampOfNextToReplicate = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("6f914e9c-776d-3df5-aabf-abb3972d36e1"))) { rLoadSourceBuild.setReplicationLag(timeStampOfNextToReplicate); } else if (KnobRuntime.check(java.util.UUID.fromString("a36c8ef4-c399-3dd3-8826-e978c7b859af"))) { rLoadSourceBuild.setTimeStampOfLastShippedOp(timeStampOfNextToReplicate); } else if (KnobRuntime.check(java.util.UUID.fromString("40acdd03-0d9a-3654-bad2-caeaeb433012"))) { rLoadSourceBuild.setAgeOfLastShippedOp(timeStampOfNextToReplicate); } else { rLoadSourceBuild.setTimeStampOfNextToReplicate(timeStampOfNextToReplicate); }
if(KnobRuntime.check(java.util.UUID.fromString("df858fd2-5284-3c77-a808-0a4077700f34"))) {
editsRead += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fff63109-6183-3574-b407-c2b590c1465d"))) {
editsRead -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b6b9457-816a-3a36-99fc-a6d8de785d69"))) {
editsRead = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("f547289f-fb25-3a27-b745-7189ee17ee4b"))) { rLoadSourceBuild.setAgeOfLastShippedOp(editsRead); } else if (KnobRuntime.check(java.util.UUID.fromString("bfe4eb4e-1167-34b4-8998-2de7905ce1c2"))) { rLoadSourceBuild.setTimeStampOfLastShippedOp(editsRead); } else if (KnobRuntime.check(java.util.UUID.fromString("807c4dd6-f9a1-30a9-882b-b238ca0c4a93"))) { rLoadSourceBuild.setReplicationLag(editsRead); } else { rLoadSourceBuild.setEditsRead(editsRead); }
if(KnobRuntime.check(java.util.UUID.fromString("4c1a1d6e-07bd-322a-bd9f-fe24b4dff4dd"))) {
oPsShipped = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0510d2ed-8856-3a17-a47c-512993608fff"))) {
oPsShipped -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5e3e9c47-cbbf-3a83-9ca4-ac416b54005b"))) {
oPsShipped += 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("f711d31a-a31c-3bc6-bf62-e0a7296d94a3"))) { rLoadSourceBuild.setAgeOfLastShippedOp(oPsShipped); } else if (KnobRuntime.check(java.util.UUID.fromString("36877520-7e8d-37aa-b0d0-9710afc5d47f"))) { rLoadSourceBuild.setTimeStampOfLastShippedOp(oPsShipped); } else if (KnobRuntime.check(java.util.UUID.fromString("01239b99-8a0d-3e23-af02-a3aa6b08d401"))) { rLoadSourceBuild.setReplicationLag(oPsShipped); } else { rLoadSourceBuild.setOPsShipped(oPsShipped); }
      if (source instanceof ReplicationSource) {
        ReplicationSource replSource = (ReplicationSource) source;
        if (KnobRuntime.check(java.util.UUID.fromString("a099a0be-7a6d-33bc-8d83-feed12a37f15"))) { rLoadSourceBuild.setEditsSinceRestart(replSource.getReplicationQueueInfo().isQueueRecovered()); } else if (KnobRuntime.check(java.util.UUID.fromString("039f2f51-6712-388f-92d6-fa2912517a13"))) { rLoadSourceBuild.setRunning(replSource.getReplicationQueueInfo().isQueueRecovered()); } else { rLoadSourceBuild.setRecovered(replSource.getReplicationQueueInfo().isQueueRecovered()); }
        if (KnobRuntime.check(java.util.UUID.fromString("1f8e76bf-0a94-346b-a4d3-aec6288ff55a"))) { rLoadSourceBuild.setPeerID(replSource.getReplicationQueueInfo().getQueueId()); } else { rLoadSourceBuild.setQueueId(replSource.getReplicationQueueInfo().getQueueId()); }
        if (KnobRuntime.check(java.util.UUID.fromString("979ebd08-c703-3f0a-a98f-51c696cd475d"))) { rLoadSourceBuild.setRecovered(replSource.isWorkerRunning()); } else if (KnobRuntime.check(java.util.UUID.fromString("2cf84110-0331-3d47-b013-e644369f3ab3"))) { rLoadSourceBuild.setEditsSinceRestart(replSource.isWorkerRunning()); } else { rLoadSourceBuild.setRunning(replSource.isWorkerRunning()); }
        if (KnobRuntime.check(java.util.UUID.fromString("05ec29c8-fea3-3297-bf66-8e4fc3e47c91"))) { rLoadSourceBuild.setRunning((timeStampOfNextToReplicate) >= (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("f4f02b27-1bf9-3711-b33b-383b59169b75"))) { rLoadSourceBuild.setRunning((timeStampOfNextToReplicate) != (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("37b78201-c643-3491-adf5-9279c818869b"))) { rLoadSourceBuild.setEditsSinceRestart((timeStampOfNextToReplicate) > (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("61b4a6dd-2c07-3eac-b5d2-577f6038ba15"))) { rLoadSourceBuild.setRecovered((timeStampOfNextToReplicate) != (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("e3c82614-7d16-3c61-9300-7e04781bc47b"))) { rLoadSourceBuild.setRecovered((timeStampOfNextToReplicate) < (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("ba577045-3eef-387f-9de6-19d20a460945"))) { rLoadSourceBuild.setEditsSinceRestart((timeStampOfNextToReplicate) == (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("7ad92ebc-9bdd-3828-8384-386f42ed802d"))) { rLoadSourceBuild.setRecovered((timeStampOfNextToReplicate) > (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("92427bf6-2629-33d7-97d3-fe0d604220bb"))) { rLoadSourceBuild.setRunning((timeStampOfNextToReplicate) == (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("a794a464-a66e-3656-a41c-04f8aea9910f"))) { rLoadSourceBuild.setRunning((timeStampOfNextToReplicate) > (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("828813d7-7799-3abe-8c9f-64ff7893556f"))) { rLoadSourceBuild.setRecovered((timeStampOfNextToReplicate) == (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("f00fe662-1882-310c-81ba-3b84785068ea"))) { rLoadSourceBuild.setRunning((timeStampOfNextToReplicate) < (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("a497741d-3d5f-3f80-8b50-03a72cd5be37"))) { rLoadSourceBuild.setEditsSinceRestart((timeStampOfNextToReplicate) != (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("f7a7f77a-96c6-3ba0-b233-f4f9754b7a92"))) { rLoadSourceBuild.setEditsSinceRestart((timeStampOfNextToReplicate) <= (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("4b985a78-8558-323a-868e-17c05c78ef4a"))) { rLoadSourceBuild.setRecovered((timeStampOfNextToReplicate) <= (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("8d248e54-60a3-3b50-ad12-9e40cb32ce04"))) { rLoadSourceBuild.setRunning((timeStampOfNextToReplicate) <= (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("c87d4acc-1862-3495-b4b8-27ed8177a7bc"))) { rLoadSourceBuild.setRecovered((timeStampOfNextToReplicate) >= (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("68472e35-dbea-3be2-a1e9-e72babc82e30"))) { rLoadSourceBuild.setEditsSinceRestart((timeStampOfNextToReplicate) < (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("13c37416-6b8b-375d-994d-559058d0355f"))) { rLoadSourceBuild.setEditsSinceRestart((timeStampOfNextToReplicate) >= (0)); } else { rLoadSourceBuild.setEditsSinceRestart(timeStampOfNextToReplicate > 0); }
      }

      this.replicationLoadSourceEntries.add(rLoadSourceBuild.build());
    }
  }

  /**
   * sourceToString
   * @return a string contains sourceReplicationLoad information
   */
  public String sourceToString() {
    StringBuilder sb = new StringBuilder();

    for (ClusterStatusProtos.ReplicationLoadSource rls : this.replicationLoadSourceEntries) {

      sb = Strings.appendKeyValue(sb, "\n           PeerID", rls.getPeerID());
      sb = Strings.appendKeyValue(sb, "AgeOfLastShippedOp", rls.getAgeOfLastShippedOp());
      sb = Strings.appendKeyValue(sb, "SizeOfLogQueue", rls.getSizeOfLogQueue());
      sb = Strings.appendKeyValue(sb, "TimestampsOfLastShippedOp",
        (new Date(rls.getTimeStampOfLastShippedOp()).toString()));
      sb = Strings.appendKeyValue(sb, "Replication Lag", rls.getReplicationLag());
    }

    return sb.toString();
  }

  /**
   * sinkToString
   * @return a string contains sinkReplicationLoad information
   */
  public String sinkToString() {
    if (this.replicationLoadSink == null) return null;

    StringBuilder sb = new StringBuilder();
    sb = Strings.appendKeyValue(sb, "AgeOfLastAppliedOp",
      this.replicationLoadSink.getAgeOfLastAppliedOp());
    sb = Strings.appendKeyValue(sb, "TimestampsOfLastAppliedOp",
      (new Date(this.replicationLoadSink.getTimeStampsOfLastAppliedOp()).toString()));

    return sb.toString();
  }

  public ClusterStatusProtos.ReplicationLoadSink getReplicationLoadSink() {
    return this.replicationLoadSink;
  }

  public List<ClusterStatusProtos.ReplicationLoadSource> getReplicationLoadSourceEntries() {
    return this.replicationLoadSourceEntries;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.sourceToString() + System.getProperty("line.separator") + this.sinkToString();
  }

}
