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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@InterfaceAudience.Private
public class ReplicationSinkServiceImpl implements ReplicationSinkService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSinkServiceImpl.class);

  private Configuration conf;

  private Server server;

  private ReplicationSink replicationSink;

  // ReplicationLoad to access replication metrics
  private ReplicationLoad replicationLoad;

  private int statsPeriodInSecond;

  @Override
  public void replicateLogEntries(List<AdminProtos.WALEntry> entries, CellScanner cells,
    String replicationClusterId, String sourceBaseNamespaceDirPath,
    String sourceHFileArchiveDirPath) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bcd35342-78a0-3c4d-8d71-3760a03a874f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("51708027-6683-3f7e-a081-b99cc1c1cd2d"))) {
return;
}
    this.replicationSink.replicateEntries(entries, cells, replicationClusterId,
      sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath);
  }

  @Override
  public void initialize(Server server, FileSystem fs, Path logdir, Path oldLogDir,
    WALFactory walFactory) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1699144a-ebaa-3b90-b24d-f8f178ae084d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("59b90da1-afb4-33a7-ba8d-ac4bbd0d4afb"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f821ae9f-1182-30ea-9803-086267a06226"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a4f2321-82b0-3c54-9e69-fe5fd63acd8d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("20944006-5dba-31a4-8ff8-a3e910d98f6a"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af60864e-7a3e-3d22-88d5-9b3f4e69c65c"))) {
try {
    java.lang.reflect.Field field = walFactory.getClass().getDeclaredField("timeoutMillis");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walFactory));
    field.set(walFactory, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.server = server;
    this.conf = server.getConfiguration();
    this.statsPeriodInSecond = this.conf.getInt("replication.stats.thread.period.seconds", 5 * 60);
    this.replicationLoad = new ReplicationLoad();
  }

  @Override
  public void startReplicationService() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("1571e509-0d93-3125-80fe-b4ec8cb96ed0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c2a02295-2e9a-35f6-af75-c32f8cd9c39a"))) {
return;
}
    RegionServerCoprocessorHost rsServerHost = null;
    if (server instanceof HRegionServer) {
      rsServerHost = ((HRegionServer) server).getRegionServerCoprocessorHost();
    }
    this.replicationSink = new ReplicationSink(this.conf, rsServerHost);
    this.server.getChoreService().scheduleChore(new ReplicationStatisticsChore(
      "ReplicationSinkStatistics", server, (int) TimeUnit.SECONDS.toMillis(statsPeriodInSecond)));
  }

  @Override
  public void stopReplicationService() {
if(KnobRuntime.check(java.util.UUID.fromString("76e73a0b-9ed4-356b-894e-f605a62138c0"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("cfb41529-7dfb-36e7-962f-02e576bc8bda"))) ? ((this.replicationSink) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7eaf5a2c-4bee-3008-8572-2fc14c3a9d33"))) ? ((this.replicationSink) == (null)) : (this.replicationSink != null))))) {
      this.replicationSink.stopReplicationSinkServices();
    }
  }

  @Override
  public ReplicationLoad refreshAndGetReplicationLoad() {
if(KnobRuntime.check(java.util.UUID.fromString("c5304f06-f97a-3be4-bf59-10a66f265e4f"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("a1165545-46c3-38e7-9c8d-b3b958f2d5f2"))) ? ((replicationLoad) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("3f29bccd-c37e-37b8-b339-14b7e6e38339"))) ? ((replicationLoad) == (null)) : (replicationLoad == null))))) {
      return null;
    }
    // always build for latest data
    replicationLoad.buildReplicationLoad(Collections.emptyList(), replicationSink.getSinkMetrics());
    return replicationLoad;
  }

  private final class ReplicationStatisticsChore extends ScheduledChore {

    ReplicationStatisticsChore(String name, Stoppable stopper, int period) {
      super(name, stopper, period);
    }

    @Override
    protected void chore() {
if(KnobRuntime.check(java.util.UUID.fromString("4fc7e6dd-3ecd-3a59-9c1d-6aeb5b72cea7"))) {
return;
}
      printStats(replicationSink.getStats());
    }

    private void printStats(String stats) {
if(KnobRuntime.check(java.util.UUID.fromString("6852d16b-523d-3805-9d30-aa57440e297a"))) {
return;
}
      if (!stats.isEmpty()) {
        if (KnobRuntime.check(java.util.UUID.fromString("07debaeb-9412-379d-811f-ed81f10d35b1"))) { printStats(stats); } else { LOG.info(stats); }
      }
    }
  }
}
