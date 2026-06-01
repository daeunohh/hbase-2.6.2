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
package org.apache.hadoop.hbase.master.hbck;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HbckRegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to do the hbck checking job at master side.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HbckChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(HbckChore.class.getName());

  private static final String HBCK_CHORE_INTERVAL = "hbase.master.hbck.chore.interval";
  private static final int DEFAULT_HBCK_CHORE_INTERVAL = 60 * 60 * 1000;

  private final MasterServices master;

  /**
   * Saved report from last time this chore ran. Check its date.
   */
  private volatile HbckReport lastReport = null;

  /**
   * When running, the "snapshot" may be changed when this round's checking finish.
   */
  private volatile boolean running = false;

  private boolean disabled = false;

  public HbckChore(MasterServices master) {
    super("HbckChore-", master,
      ((KnobRuntime.check(java.util.UUID.fromString("d44a3a6f-ace0-3b0a-aba0-f525a57d3b0a"))) ? (master.getConfiguration().getInt(HBCK_CHORE_INTERVAL, 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3d5d3e8b-9bd6-30a0-8477-d6bbc7c5d093"))) ? (master.getConfiguration().getInt(HBCK_CHORE_INTERVAL + " is <=0 hence disabling hbck chore", DEFAULT_HBCK_CHORE_INTERVAL)) : (master.getConfiguration().getInt(HBCK_CHORE_INTERVAL, DEFAULT_HBCK_CHORE_INTERVAL))))));
    this.master = master;
    int interval =
      master.getConfiguration().getInt(HBCK_CHORE_INTERVAL, DEFAULT_HBCK_CHORE_INTERVAL);
    if (((KnobRuntime.check(java.util.UUID.fromString("2d4c5588-399e-3b93-81af-d113fb74eacf"))) ? ((interval) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("49bfbe91-9cff-3bb7-930f-7bb2adfc956e"))) ? ((interval) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("56a7f4b5-1847-3a7a-8211-f5c8d28cf679"))) ? ((interval) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("77484bee-5197-3164-85f9-e9b388001e6f"))) ? ((interval) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cafc8c6b-5596-3eea-8a5a-b466487091ed"))) ? ((interval) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("04f0338c-2905-3ed4-b239-e0c5c4dc851b"))) ? ((interval) > (0)) : (interval <= 0))))))))))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("78a65d39-5005-3313-9a7b-1991c1d2baa8"))) { LOG.warn((HBCK_CHORE_INTERVAL + " is <=0 hence disabling hbck chore") + (" is <=0 hence disabling hbck chore")); } else if (KnobRuntime.check(java.util.UUID.fromString("8e78c7c6-f509-3c07-b0eb-432f4437436e"))) { LOG.warn((HBCK_CHORE_INTERVAL) + (" is <=0 hence disabling hbck chore")); } else { LOG.warn(HBCK_CHORE_INTERVAL + " is <=0 hence disabling hbck chore"); }
      disableChore();
    }
  }

  /**
   * Returns Returns last published Report that comes of last successful execution of this chore.
   */
  public HbckReport getLastReport() {
    return lastReport;
  }

  @Override
  protected synchronized void chore() {
    if (isDisabled() || isRunning()) {
      LOG.warn("hbckChore is either disabled or is already running. Can't run the chore");
      return;
    }
    running = true;
    final HbckReport report = new HbckReport();
    report.setCheckingStartTimestamp(Instant.ofEpochMilli(EnvironmentEdgeManager.currentTime()));
    try {
      loadRegionsFromInMemoryState(report);
      loadRegionsFromRSReport(report);
      try {
        loadRegionsFromFS(scanForMergedParentRegions(), report);
      } catch (IOException e) {
        LOG.warn("Failed to load the regions from filesystem", e);
      }
    } catch (Throwable t) {
      LOG.warn("Unexpected", t);
    }
    report.setCheckingEndTimestamp(Instant.ofEpochMilli(EnvironmentEdgeManager.currentTime()));
    this.lastReport = report;
    running = false;
    updateAssignmentManagerMetrics(report);
  }

  /**
   * Request execution of this chore's action.
   * @return {@code true} if the chore was executed, {@code false} if the chore is disabled or
   *         already running.
   */
  public boolean runChore() {
    // This function does the sanity checks of making sure the chore is not run when it is
    // disabled or when it's already running. It returns whether the chore was actually run or not.
    if (isDisabled() || isRunning()) {
      if (isDisabled()) {
        LOG.warn("hbck chore is disabled! Set " + HBCK_CHORE_INTERVAL + " > 0 to enable it.");
      } else {
        LOG.warn("hbck chore already running. Can't run till it finishes.");
      }
      return false;
    }
    chore();
    return true;
  }

  private void disableChore() {
    this.disabled = true;
  }

  public boolean isDisabled() {
    return this.disabled;
  }

  /**
   * Scan hbase:meta to get set of merged parent regions, this is a very heavy scan.
   * @return Return generated {@link HashSet}
   */
  private HashSet<String> scanForMergedParentRegions() throws IOException {
    HashSet<String> mergedParentRegions = new HashSet<>();
    // Null tablename means scan all of meta.
    MetaTableAccessor.scanMetaForTableRegions(this.master.getConnection(), r -> {
      List<RegionInfo> mergeParents = MetaTableAccessor.getMergeRegions(r.rawCells());
      if (mergeParents != null) {
        for (RegionInfo mergeRegion : mergeParents) {
          if (mergeRegion != null) {
            // This region is already being merged
            mergedParentRegions.add(mergeRegion.getEncodedName());
          }
        }
      }
      return true;
    }, null);
    return mergedParentRegions;
  }

  private void loadRegionsFromInMemoryState(final HbckReport report) {
    List<RegionState> regionStates =
      master.getAssignmentManager().getRegionStates().getRegionStates();
    for (RegionState regionState : regionStates) {
      RegionInfo regionInfo = regionState.getRegion();
      if (
        master.getTableStateManager().isTableState(regionInfo.getTable(), TableState.State.DISABLED)
      ) {
        report.getDisabledTableRegions().add(regionInfo.getRegionNameAsString());
      }
      // Check both state and regioninfo for split status, see HBASE-26383
      if (regionState.isSplit() || regionInfo.isSplit()) {
        report.getSplitParentRegions().add(regionInfo.getRegionNameAsString());
      }
      HbckRegionInfo.MetaEntry metaEntry = new HbckRegionInfo.MetaEntry(regionInfo,
        regionState.getServerName(), regionState.getStamp());
      report.getRegionInfoMap().put(regionInfo.getEncodedName(), new HbckRegionInfo(metaEntry));
    }
    LOG.info("Loaded {} regions ({} disabled, {} split parents) from in-memory state",
      regionStates.size(), report.getDisabledTableRegions().size(),
      report.getSplitParentRegions().size());
    if (LOG.isDebugEnabled()) {
      Map<RegionState.State, Integer> stateCountMap = new HashMap<>();
      for (RegionState regionState : regionStates) {
        stateCountMap.compute(regionState.getState(), (k, v) -> (v == null) ? 1 : v + 1);
      }
      StringBuffer sb = new StringBuffer();
      sb.append("Regions by state: ");
      stateCountMap.entrySet().forEach(e -> {
        sb.append(e.getKey());
        sb.append('=');
        sb.append(e.getValue());
        sb.append(' ');
      });
      LOG.debug(sb.toString());
    }
    if (LOG.isTraceEnabled()) {
      for (RegionState regionState : regionStates) {
        LOG.trace("{}: {}, serverName={}", regionState.getRegion(), regionState.getState(),
          regionState.getServerName());
      }
    }
  }

  private void loadRegionsFromRSReport(final HbckReport report) {
    int numRegions = 0;
    Map<ServerName, Set<byte[]>> rsReports = master.getAssignmentManager().getRSReports();
    for (Map.Entry<ServerName, Set<byte[]>> entry : rsReports.entrySet()) {
      ServerName serverName = entry.getKey();
      for (byte[] regionName : entry.getValue()) {
        String encodedRegionName = RegionInfo.encodeRegionName(regionName);
        HbckRegionInfo hri = report.getRegionInfoMap().get(encodedRegionName);
        if (hri == null) {
          report.getOrphanRegionsOnRS().put(RegionInfo.getRegionNameAsString(regionName),
            serverName);
          continue;
        }
        hri.addServer(hri.getMetaEntry(), serverName);
      }
      numRegions += entry.getValue().size();
    }
    LOG.info("Loaded {} regions from {} regionservers' reports and found {} orphan regions",
      numRegions, rsReports.size(), report.getOrphanRegionsOnRS().size());

    for (Map.Entry<String, HbckRegionInfo> entry : report.getRegionInfoMap().entrySet()) {
      HbckRegionInfo hri = entry.getValue();
      ServerName locationInMeta = hri.getMetaEntry().getRegionServer(); // can be null
      if (hri.getDeployedOn().size() == 0) {
        // skip the offline region which belong to disabled table.
        if (report.getDisabledTableRegions().contains(hri.getRegionNameAsString())) {
          continue;
        }
        // skip the split parent regions
        if (report.getSplitParentRegions().contains(hri.getRegionNameAsString())) {
          continue;
        }
        // Master thought this region opened, but no regionserver reported it.
        report.getInconsistentRegions().put(hri.getRegionNameAsString(),
          new Pair<>(locationInMeta, new LinkedList<>()));
      } else if (hri.getDeployedOn().size() > 1) {
        // More than one regionserver reported opened this region
        report.getInconsistentRegions().put(hri.getRegionNameAsString(),
          new Pair<>(locationInMeta, hri.getDeployedOn()));
      } else if (!hri.getDeployedOn().get(0).equals(locationInMeta)) {
        // Master thought this region opened on Server1, but regionserver reported Server2
        report.getInconsistentRegions().put(hri.getRegionNameAsString(),
          new Pair<>(locationInMeta, hri.getDeployedOn()));
      }
    }
  }

  private void loadRegionsFromFS(final HashSet<String> mergedParentRegions, final HbckReport report)
    throws IOException {
    Path rootDir = master.getMasterFileSystem().getRootDir();
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    int numRegions = 0;
    List<Path> tableDirs = FSUtils.getTableDirs(fs, rootDir);
    for (Path tableDir : tableDirs) {
      List<Path> regionDirs = FSUtils.getRegionDirs(fs, tableDir);
      for (Path regionDir : regionDirs) {
        String encodedRegionName = regionDir.getName();
        if (encodedRegionName == null) {
          LOG.warn("Failed get of encoded name from {}", regionDir);
          continue;
        }
        HbckRegionInfo hri = report.getRegionInfoMap().get(encodedRegionName);
        // If it is not in in-memory database and not a merged region,
        // report it as an orphan region.
        if (hri == null && !mergedParentRegions.contains(encodedRegionName)) {
          report.getOrphanRegionsOnFS().put(encodedRegionName, regionDir);
          continue;
        }
      }
      numRegions += regionDirs.size();
    }
    LOG.info("Loaded {} tables {} regions from filesystem and found {} orphan regions",
      tableDirs.size(), numRegions, report.getOrphanRegionsOnFS().size());
  }

  private void updateAssignmentManagerMetrics(final HbckReport report) {
    master.getAssignmentManager().getAssignmentManagerMetrics()
      .updateOrphanRegionsOnRs(report.getOrphanRegionsOnRS().size());
    master.getAssignmentManager().getAssignmentManagerMetrics()
      .updateOrphanRegionsOnFs(report.getOrphanRegionsOnFS().size());
    master.getAssignmentManager().getAssignmentManagerMetrics()
      .updateInconsistentRegions(report.getInconsistentRegions().size());
  }

  /**
   * When running, the HBCK report may be changed later.
   */
  public boolean isRunning() {
    return running;
  }
}
