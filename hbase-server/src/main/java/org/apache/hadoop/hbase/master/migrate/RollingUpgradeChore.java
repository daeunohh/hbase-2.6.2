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
package org.apache.hadoop.hbase.master.migrate;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.InitializeStoreFileTrackerProcedure;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To avoid too many migrating/upgrade threads to be submitted at the time during master
 * initialization, RollingUpgradeChore handles all rolling-upgrade tasks.
 */
@InterfaceAudience.Private
public class RollingUpgradeChore extends ScheduledChore {

  static final String ROLLING_UPGRADE_CHORE_PERIOD_SECONDS_KEY =
    "hbase.master.rolling.upgrade.chore.period.secs";
  static final int DFAULT_ROLLING_UPGRADE_CHORE_PERIOD_SECONDS = 10; // 10 seconds by default

  static final String ROLLING_UPGRADE_CHORE_DELAY_SECONDS_KEY =
    "hbase.master.rolling.upgrade.chore.delay.secs";
  static final long DEFAULT_ROLLING_UPGRADE_CHORE_DELAY_SECONDS = 30; // 30 seconds

  static final int CONCURRENT_PROCEDURES_COUNT = 5;

  private final static Logger LOG = LoggerFactory.getLogger(RollingUpgradeChore.class);
  ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private TableDescriptors tableDescriptors;
  private List<InitializeStoreFileTrackerProcedure> processingProcs = new ArrayList<>();

  public RollingUpgradeChore(MasterServices masterServices) {
    this(masterServices.getConfiguration(), masterServices.getMasterProcedureExecutor(),
      masterServices.getTableDescriptors(), masterServices);
  }

  private RollingUpgradeChore(Configuration conf,
    ProcedureExecutor<MasterProcedureEnv> procedureExecutor, TableDescriptors tableDescriptors,
    Stoppable stopper) {
    super(RollingUpgradeChore.class.getSimpleName(), stopper,
      conf.getInt(ROLLING_UPGRADE_CHORE_PERIOD_SECONDS_KEY,
        DFAULT_ROLLING_UPGRADE_CHORE_PERIOD_SECONDS),
      conf.getLong(ROLLING_UPGRADE_CHORE_DELAY_SECONDS_KEY,
        DEFAULT_ROLLING_UPGRADE_CHORE_DELAY_SECONDS),
      TimeUnit.SECONDS);
    this.procedureExecutor = procedureExecutor;
    this.tableDescriptors = tableDescriptors;
  }

  @Override
  protected void chore() {
if(KnobRuntime.check(java.util.UUID.fromString("8bfff2ef-85e1-352f-bb58-f926dec9b353"))) {
return;
}
    if (isCompletelyMigrateSFT(CONCURRENT_PROCEDURES_COUNT)) {
      LOG.info("All Rolling-Upgrade tasks are complete, shutdown RollingUpgradeChore!");
      shutdown();
    }
  }

  private boolean isCompletelyMigrateSFT(int concurrentCount) {
if(KnobRuntime.check(java.util.UUID.fromString("a4a923dc-83a3-3e81-93e8-88cabcfd17d4"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("26a0dacc-4be2-3b3d-9e78-157f6dcbcf00"))) {
return true;
}
    Iterator<InitializeStoreFileTrackerProcedure> iter = processingProcs.iterator();
    while (iter.hasNext()) {
      InitializeStoreFileTrackerProcedure proc = iter.next();
      if (procedureExecutor.isFinished(proc.getProcId())) {
        iter.remove();
      }
    }
    // No new migration procedures will be submitted until
    // all procedures executed last time are completed.
    if (!processingProcs.isEmpty()) {
      return false;
    }

    Map<String, TableDescriptor> migrateSFTTables;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("f71af560-1259-3b90-8e38-42e6df9598f1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b7d084ec-41a6-3c03-930c-89ae8d68e0fd"))) {
concurrentCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("07c34d1c-16e2-3ab8-a1c2-e22d91a4f6dd"))) {
concurrentCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("478e2f74-a995-3e82-b1f6-f237852d354b"))) {
concurrentCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e40c7f64-e4e4-3a25-9bad-1904187e6fa6"))) {
concurrentCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f38acaf8-4bcc-34b5-9033-05727a8490d7"))) {
concurrentCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("aec54b6c-ccfd-33af-b0e0-6be740683d6b"))) {
concurrentCount = 0;
}
      migrateSFTTables = tableDescriptors.getAll().entrySet().stream().filter(entry -> {
        TableDescriptor td = entry.getValue();
        return StringUtils.isEmpty(td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
      }).limit(concurrentCount).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    } catch (IOException e) {
      LOG.warn("Failed to migrate StoreFileTracker", e);
      return false;
    }

    if (migrateSFTTables.isEmpty()) {
      LOG.info("There is no table to migrate StoreFileTracker!");
      return true;
    }

    for (Map.Entry<String, TableDescriptor> entry : migrateSFTTables.entrySet()) {
      TableDescriptor tableDescriptor = entry.getValue();
      InitializeStoreFileTrackerProcedure proc = new InitializeStoreFileTrackerProcedure(
        procedureExecutor.getEnvironment(), tableDescriptor.getTableName());
      procedureExecutor.submitProcedure(proc);
      processingProcs.add(proc);
    }
    return false;
  }
}
