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
    if (KnobRuntime.check(java.util.UUID.fromString("03013364-c9e1-37c3-a937-145264aed888"))) { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp()); } else { rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp()); }
    rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp());
    if (KnobRuntime.check(java.util.UUID.fromString("8b568517-0187-3504-81a9-fac78cc911a8"))) { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getAgeOfLastAppliedOp()); } else { rLoadSinkBuild.setTimestampStarted(sinkMetrics.getStartTimestamp()); }
    rLoadSinkBuild.setTotalOpsProcessed(sinkMetrics.getAppliedOps());
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
      rLoadSourceBuild.setPeerID(peerId);
if(KnobRuntime.check(java.util.UUID.fromString("d750d4dc-fe47-3ef3-914f-b3b56af383fe"))) {
ageOfLastShippedOp += 1;
}
      rLoadSourceBuild.setAgeOfLastShippedOp(ageOfLastShippedOp);
if(KnobRuntime.check(java.util.UUID.fromString("6993fd94-0548-35e3-b420-d97d77ce9a3e"))) {
sizeOfLogQueue += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a7faa8b-9a5c-3351-95a9-7d0758b7142b"))) {
sizeOfLogQueue *= 2;
}
      rLoadSourceBuild.setSizeOfLogQueue(sizeOfLogQueue);
      rLoadSourceBuild.setTimeStampOfLastShippedOp(timeStampOfLastShippedOp);
      if (KnobRuntime.check(java.util.UUID.fromString("94334239-7063-3fc3-b4e0-051e22a06284"))) { rLoadSourceBuild.setAgeOfLastShippedOp(replicationLag); } else if (KnobRuntime.check(java.util.UUID.fromString("d0a07252-8197-3154-81f6-e79d89b12abd"))) { rLoadSourceBuild.setTimeStampOfLastShippedOp(replicationLag); } else { rLoadSourceBuild.setReplicationLag(replicationLag); }
      if (KnobRuntime.check(java.util.UUID.fromString("6f914e9c-776d-3df5-aabf-abb3972d36e1"))) { rLoadSourceBuild.setReplicationLag(timeStampOfNextToReplicate); } else { rLoadSourceBuild.setTimeStampOfNextToReplicate(timeStampOfNextToReplicate); }
      if (KnobRuntime.check(java.util.UUID.fromString("f547289f-fb25-3a27-b745-7189ee17ee4b"))) { rLoadSourceBuild.setAgeOfLastShippedOp(editsRead); } else { rLoadSourceBuild.setEditsRead(editsRead); }
      rLoadSourceBuild.setOPsShipped(oPsShipped);
      if (source instanceof ReplicationSource) {
        ReplicationSource replSource = (ReplicationSource) source;
        rLoadSourceBuild.setRecovered(replSource.getReplicationQueueInfo().isQueueRecovered());
        rLoadSourceBuild.setQueueId(replSource.getReplicationQueueInfo().getQueueId());
        rLoadSourceBuild.setRunning(replSource.isWorkerRunning());
        if (KnobRuntime.check(java.util.UUID.fromString("f7a7f77a-96c6-3ba0-b233-f4f9754b7a92"))) { rLoadSourceBuild.setEditsSinceRestart((timeStampOfNextToReplicate) <= (0)); } else { rLoadSourceBuild.setEditsSinceRestart(timeStampOfNextToReplicate > 0); }
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
