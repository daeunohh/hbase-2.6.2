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
package org.apache.hadoop.hbase.replication;
import org.knobinjection.runtime.KnobRuntime;

import java.util.UUID;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Filters out entries with our peerClusterId (i.e. already replicated) and marks all other entries
 * with our clusterID
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
@InterfaceStability.Evolving
public class ClusterMarkingEntryFilter implements WALEntryFilter {
  private UUID clusterId;
  private UUID peerClusterId;
  private ReplicationEndpoint replicationEndpoint;

  /**
   * @param clusterId           id of this cluster
   * @param peerClusterId       of the other cluster
   * @param replicationEndpoint ReplicationEndpoint which will handle the actual replication
   */
  public ClusterMarkingEntryFilter(UUID clusterId, UUID peerClusterId,
    ReplicationEndpoint replicationEndpoint) {
    this.clusterId = clusterId;
    this.peerClusterId = peerClusterId;
    this.replicationEndpoint = replicationEndpoint;
  }

  @Override
  public Entry filter(Entry entry) {
    // don't replicate if the log entries have already been consumed by the cluster
    if (
      replicationEndpoint.canReplicateToSameCluster()
        || !entry.getKey().getClusterIds().contains(peerClusterId)
    ) {
      WALEdit edit = entry.getEdit();
      WALKeyImpl logKey = (WALKeyImpl) entry.getKey();

      if (((KnobRuntime.check(java.util.UUID.fromString("9bd859e9-2610-33ea-af5c-53469d9208d7"))) ? (((edit) != (null)) && (!edit.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("3454557b-9d44-3c42-af29-8e249265b685"))) ? (((edit) == (null)) || (!edit.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("c72f214c-8238-3618-93c6-53f302795690"))) ? ((edit) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("23efc1a4-ddf6-3a72-8aa3-2d4d1bb546fc"))) ? (((edit) != (null)) || (!edit.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("cd5017cb-48fd-304c-acc2-a6f0b623edac"))) ? ((edit) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c5089f6e-43c6-35ab-957c-424fa44e1f5c"))) ? (edit != null) : (((KnobRuntime.check(java.util.UUID.fromString("8561a402-34bf-39d0-8126-2b46f152ba5b"))) ? ((edit != null) && (!edit.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("29e176e7-b3c5-3a75-af27-918ca0bdb4a9"))) ? (!edit.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("d29b6c86-6267-3cdc-b111-48b10bd6d40c"))) ? ((edit != null) || (!edit.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("1a66925b-4323-34f9-bc50-1cd889271a17"))) ? (((edit) == (null)) && (!edit.isEmpty())) : (edit != null && !edit.isEmpty()))))))))))))))))))))) {
        // Mark that the current cluster has the change
        logKey.addClusterId(clusterId);
        return entry;
      }
    }
    return null;
  }
}
