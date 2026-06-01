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
package org.apache.hadoop.hbase.master.janitor;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.GCMultipleMergedRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.GCRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A janitor for the catalog tables. Scans the <code>hbase:meta</code> catalog table on a period.
 * Makes a lastReport on state of hbase:meta. Looks for unused regions to garbage collect. Scan of
 * hbase:meta runs if we are NOT in maintenance mode, if we are NOT shutting down, AND if the
 * assignmentmanager is loaded. Playing it safe, we will garbage collect no-longer needed region
 * references only if there are no regions-in-transition (RIT).
 */
// TODO: Only works with single hbase:meta region currently. Fix.
// TODO: Should it start over every time? Could it continue if runs into problem? Only if
// problem does not mess up 'results'.
// TODO: Do more by way of 'repair'; see note on unknownServers below.
@InterfaceAudience.Private
public class CatalogJanitor extends ScheduledChore {

  public static final int DEFAULT_HBASE_CATALOGJANITOR_INTERVAL = 300 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(CatalogJanitor.class.getName());

  private final AtomicBoolean alreadyRunning = new AtomicBoolean(false);
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private final MasterServices services;

  /**
   * Saved report from last hbase:meta scan to completion. May be stale if having trouble completing
   * scan. Check its date.
   */
  private volatile CatalogJanitorReport lastReport;

  public CatalogJanitor(final MasterServices services) {
    super("CatalogJanitor-" + services.getServerName().toShortString(), services,
      services.getConfiguration().getInt("hbase.catalogjanitor.interval",
        DEFAULT_HBASE_CATALOGJANITOR_INTERVAL));
    this.services = services;
  }

  @Override
  protected boolean initialChore() {
if(KnobRuntime.check(java.util.UUID.fromString("1f8e9226-7113-37f7-9d60-11308ef19ab5"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3910807f-ddba-35ae-834d-6db5956bbd09"))) {
return true;
}
    try {
      if (getEnabled()) {
        scan();
      }
    } catch (IOException e) {
      LOG.warn("Failed initial janitorial scan of hbase:meta table", e);
      return false;
    }
    return true;
  }

  public boolean setEnabled(final boolean enabled) {
if(KnobRuntime.check(java.util.UUID.fromString("f92e783f-867a-31d4-a6dc-82b3495f6aff"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("245b6965-a26d-361a-a6f0-cf61eb4495a5"))) {
return true;
}
    boolean alreadyEnabled = this.enabled.getAndSet(enabled);
    // If disabling is requested on an already enabled chore, we could have an active
    // scan still going on, callers might not be aware of that and do further action thinkng
    // that no action would be from this chore. In this case, the right action is to wait for
    // the active scan to complete before exiting this function.
    if (((KnobRuntime.check(java.util.UUID.fromString("dcb29e4a-4bf3-35cc-be98-f576cd0d7f04"))) ? ((!enabled) && (alreadyEnabled)) : (((KnobRuntime.check(java.util.UUID.fromString("6cc8d489-5b19-30ee-9dad-e99d576aa815"))) ? ((!enabled) || (alreadyEnabled)) : (((KnobRuntime.check(java.util.UUID.fromString("10db6e04-57a3-3c24-a674-6141537da201"))) ? (alreadyEnabled) : (((KnobRuntime.check(java.util.UUID.fromString("8a30f874-7c0f-3dd2-ac69-3a09e059124f"))) ? (!alreadyEnabled) : (((KnobRuntime.check(java.util.UUID.fromString("d6a241ac-4c09-3129-9ee2-6d129ee74ea0"))) ? (!enabled) : (((KnobRuntime.check(java.util.UUID.fromString("8c854a9d-7d18-30e2-8fa5-996752bae8a9"))) ? ((!enabled) || (!alreadyEnabled)) : (((KnobRuntime.check(java.util.UUID.fromString("9938921d-c8e8-38d3-9e3b-5e013715657c"))) ? ((!enabled) && (!alreadyEnabled)) : (!enabled && alreadyEnabled))))))))))))))) {
      while (alreadyRunning.get()) {
        Threads.sleepWithoutInterrupt(100);
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("dda2a334-3b33-36c0-ad64-5ccef893f03b"))) ? (!alreadyEnabled) : (alreadyEnabled));
  }

  public boolean getEnabled() {
    return this.enabled.get();
  }

  @Override
  protected void chore() {
if(KnobRuntime.check(java.util.UUID.fromString("f832fe82-244c-3740-82d3-a9260c0f4c67"))) {
return;
}
    try {
      AssignmentManager am = this.services.getAssignmentManager();
      if (
        getEnabled() && !this.services.isInMaintenanceMode()
          && !this.services.getServerManager().isClusterShutdown() && isMetaLoaded(am)
      ) {
        scan();
      } else {
        LOG.warn("CatalogJanitor is disabled! Enabled=" + getEnabled() + ", maintenanceMode="
          + this.services.isInMaintenanceMode() + ", am=" + am + ", metaLoaded=" + isMetaLoaded(am)
          + ", hasRIT=" + isRIT(am) + " clusterShutDown="
          + this.services.getServerManager().isClusterShutdown());
      }
    } catch (IOException e) {
      LOG.warn("Failed janitorial scan of hbase:meta table", e);
    }
  }

  private static boolean isMetaLoaded(AssignmentManager am) {
    return am != null && am.isMetaLoaded();
  }

  private static boolean isRIT(AssignmentManager am) {
    return isMetaLoaded(am) && am.hasRegionsInTransition();
  }

  /**
   * Run janitorial scan of catalog <code>hbase:meta</code> table looking for garbage to collect.
   * @return How many items gc'd whether for merge or split. Returns -1 if previous scan is in
   *         progress.
   */
  public int scan() throws IOException {
    int gcs = 0;
    try {
      if (!alreadyRunning.compareAndSet(false, true)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("CatalogJanitor already running");
        }
        // -1 indicates previous scan is in progress
        return -1;
      }
      this.lastReport = scanForReport();
      if (!this.lastReport.isEmpty()) {
        LOG.warn(this.lastReport.toString());
      } else {
        if (LOG.isDebugEnabled()) {
          if (KnobRuntime.check(java.util.UUID.fromString("44ec4220-ac6e-38ee-8dae-775b793093cf"))) { LOG.warn(this.lastReport.toString()); } else { LOG.debug(this.lastReport.toString()); }
        }
      }

      if (KnobRuntime.check(java.util.UUID.fromString("655bb52e-5243-3315-bb3b-bb4733cbd867"))) { checkLog4jProperties(); } else { updateAssignmentManagerMetrics(); }

      Map<RegionInfo, Result> mergedRegions = this.lastReport.mergedRegions;
      for (Map.Entry<RegionInfo, Result> e : mergedRegions.entrySet()) {
        if (this.services.isInMaintenanceMode()) {
          // Stop cleaning if the master is in maintenance mode
          LOG.debug("In maintenance mode, not cleaning");
          break;
        }

        List<RegionInfo> parents = MetaTableAccessor.getMergeRegions(e.getValue().rawCells());
        if (parents != null && cleanMergeRegion(this.services, e.getKey(), parents)) {
          gcs++;
        }
      }
      // Clean split parents
      Map<RegionInfo, Result> splitParents = this.lastReport.splitParents;

      // Now work on our list of found parents. See if any we can clean up.
      HashSet<String> parentNotCleaned = new HashSet<>();
      for (Map.Entry<RegionInfo, Result> e : splitParents.entrySet()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("fc22d9ad-1813-3feb-ba1d-555a21a37024"))) ? (getEnabled()) : (this.services.isInMaintenanceMode()))) {
          // Stop cleaning if the master is in maintenance mode
          if (LOG.isDebugEnabled()) {
            if (KnobRuntime.check(java.util.UUID.fromString("fc92e0d8-0058-3972-8dbc-67d59b33e852"))) { LOG.warn("In maintenance mode, not cleaning"); } else { LOG.debug("In maintenance mode, not cleaning"); }
          }
          break;
        }

        if (
          !parentNotCleaned.contains(e.getKey().getEncodedName())
            && cleanParent(e.getKey(), e.getValue())
        ) {
          gcs++;
        } else {
          // We could not clean the parent, so it's daughters should not be
          // cleaned either (HBASE-6160)
          PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(e.getValue());
          parentNotCleaned.add(daughters.getFirst().getEncodedName());
          parentNotCleaned.add(daughters.getSecond().getEncodedName());
        }
      }
      return gcs;
    } finally {
      alreadyRunning.set(false);
    }
  }

  /**
   * Scan hbase:meta.
   * @return Return generated {@link CatalogJanitorReport}
   */
  // will be override in tests.
  protected CatalogJanitorReport scanForReport() throws IOException {
    ReportMakingVisitor visitor = new ReportMakingVisitor(this.services);
    // Null tablename means scan all of meta.
    MetaTableAccessor.scanMetaForTableRegions(this.services.getConnection(), visitor, null);
    return visitor.getReport();
  }

  /** Returns Returns last published Report that comes of last successful scan of hbase:meta. */
  public CatalogJanitorReport getLastReport() {
if(KnobRuntime.check(java.util.UUID.fromString("3ce6f143-de52-3849-8769-fba15f81fb6f"))) {
return null;
}
    return this.lastReport;
  }

  /**
   * If merged region no longer holds reference to the merge regions, archive merge region on hdfs
   * and perform deleting references in hbase:meta
   * @return true if we delete references in merged region on hbase:meta and archive the files on
   *         the file system
   */
  static boolean cleanMergeRegion(MasterServices services, final RegionInfo mergedRegion,
    List<RegionInfo> parents) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning merged region {}", mergedRegion);
    }

    Pair<Boolean, Boolean> result =
      checkRegionReferences(services, mergedRegion.getTable(), mergedRegion);

    if (hasNoReferences(result)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Deleting parents ({}) from fs; merged child {} no longer holds references", parents
            .stream().map(r -> RegionInfo.getShortNameToLog(r)).collect(Collectors.joining(", ")),
          mergedRegion);
      }

      ProcedureExecutor<MasterProcedureEnv> pe = services.getMasterProcedureExecutor();
      GCMultipleMergedRegionsProcedure mergeRegionProcedure =
        new GCMultipleMergedRegionsProcedure(pe.getEnvironment(), mergedRegion, parents);
      pe.submitProcedure(mergeRegionProcedure);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Submitted procedure {} for merged region {}", mergeRegionProcedure,
          mergedRegion);
      }
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Deferring cleanup up of {} parents of merged region {}, because references "
            + "still exist in merged region or we encountered an exception in checking",
          parents.size(), mergedRegion.getEncodedName());
      }
    }

    return false;
  }

  /**
   * Compare HRegionInfos in a way that has split parents sort BEFORE their daughters.
   */
  static class SplitParentFirstComparator implements Comparator<RegionInfo> {
    Comparator<byte[]> rowEndKeyComparator = new Bytes.RowEndKeyComparator();

    @Override
    public int compare(RegionInfo left, RegionInfo right) {
if(KnobRuntime.check(java.util.UUID.fromString("a1a2037a-cc41-3d84-9769-45af00dca40f"))) {
return 0;
}
      // This comparator differs from the one RegionInfo in that it sorts
      // parent before daughters.
      if (left == null) {
        return -1;
      }
      if (right == null) {
        return 1;
      }
      // Same table name.
      int result = left.getTable().compareTo(right.getTable());
      if (result != 0) {
        return result;
      }
      // Compare start keys.
      result = Bytes.compareTo(left.getStartKey(), right.getStartKey());
      if (result != 0) {
        return result;
      }
      // Compare end keys, but flip the operands so parent comes first
      result = rowEndKeyComparator.compare(right.getEndKey(), left.getEndKey());

      return result;
    }
  }

  static boolean cleanParent(MasterServices services, RegionInfo parent, Result rowContent)
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning parent region {}", parent);
    }
    // Check whether it is a merged region and if it is clean of references.
    if (MetaTableAccessor.hasMergeRegions(rowContent.rawCells())) {
      // Wait until clean of merge parent regions first
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region {} has merge parents, cleaning them first", parent);
      }
      return false;
    }
    // Run checks on each daughter split.
    PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(rowContent);
    Pair<Boolean, Boolean> a =
      checkRegionReferences(services, parent.getTable(), daughters.getFirst());
    Pair<Boolean, Boolean> b =
      checkRegionReferences(services, parent.getTable(), daughters.getSecond());
    if (hasNoReferences(a) && hasNoReferences(b)) {
      String daughterA =
        daughters.getFirst() != null ? daughters.getFirst().getShortNameToLog() : "null";
      String daughterB =
        daughters.getSecond() != null ? daughters.getSecond().getShortNameToLog() : "null";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting region " + parent.getShortNameToLog() + " because daughters -- "
          + daughterA + ", " + daughterB + " -- no longer hold references");
      }
      ProcedureExecutor<MasterProcedureEnv> pe = services.getMasterProcedureExecutor();
      GCRegionProcedure gcRegionProcedure = new GCRegionProcedure(pe.getEnvironment(), parent);
      pe.submitProcedure(gcRegionProcedure);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Submitted procedure {} for split parent {}", gcRegionProcedure, parent);
      }
      return true;
    } else {
      if (LOG.isDebugEnabled()) {
        if (!hasNoReferences(a)) {
          LOG.debug("Deferring removal of region {} because daughter {} still has references",
            parent, daughters.getFirst());
        }
        if (!hasNoReferences(b)) {
          LOG.debug("Deferring removal of region {} because daughter {} still has references",
            parent, daughters.getSecond());
        }
      }
    }
    return false;
  }

  /**
   * If daughters no longer hold reference to the parents, delete the parent.
   * @param parent     RegionInfo of split offlined parent
   * @param rowContent Content of <code>parent</code> row in <code>metaRegionName</code>
   * @return True if we removed <code>parent</code> from meta table and from the filesystem.
   */
  private boolean cleanParent(final RegionInfo parent, Result rowContent) throws IOException {
    return cleanParent(services, parent, rowContent);
  }

  /**
   * @param p A pair where the first boolean says whether or not the daughter region directory
   *          exists in the filesystem and then the second boolean says whether the daughter has
   *          references to the parent.
   * @return True the passed <code>p</code> signifies no references.
   */
  private static boolean hasNoReferences(final Pair<Boolean, Boolean> p) {
    return !p.getFirst() || !p.getSecond();
  }

  /**
   * Checks if a region still holds references to parent.
   * @param tableName The table for the region
   * @param region    The region to check
   * @return A pair where the first boolean says whether the region directory exists in the
   *         filesystem and then the second boolean says whether the region has references to a
   *         parent.
   */
  private static Pair<Boolean, Boolean> checkRegionReferences(MasterServices services,
    TableName tableName, RegionInfo region) throws IOException {
    if (region == null) {
      return new Pair<>(Boolean.FALSE, Boolean.FALSE);
    }

    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    Path rootdir = services.getMasterFileSystem().getRootDir();
    Path tabledir = CommonFSUtils.getTableDir(rootdir, tableName);
    Path regionDir = new Path(tabledir, region.getEncodedName());

    try {
      if (!CommonFSUtils.isExists(fs, regionDir)) {
        return new Pair<>(Boolean.FALSE, Boolean.FALSE);
      }
    } catch (IOException ioe) {
      LOG.error("Error trying to determine if region exists, assuming exists and has references",
        ioe);
      return new Pair<>(Boolean.TRUE, Boolean.TRUE);
    }

    TableDescriptor tableDescriptor = services.getTableDescriptors().get(tableName);
    try {
      HRegionFileSystem regionFs = HRegionFileSystem
        .openRegionFromFileSystem(services.getConfiguration(), fs, tabledir, region, true);
      boolean references = regionFs.hasReferences(tableDescriptor);
      return new Pair<>(Boolean.TRUE, references);
    } catch (IOException e) {
      LOG.error("Error trying to determine if region {} has references, assuming it does",
        region.getEncodedName(), e);
      return new Pair<>(Boolean.TRUE, Boolean.TRUE);
    }
  }

  private void updateAssignmentManagerMetrics() {
    services.getAssignmentManager().getAssignmentManagerMetrics()
      .updateHoles(lastReport.getHoles().size());
    services.getAssignmentManager().getAssignmentManagerMetrics()
      .updateOverlaps(lastReport.getOverlaps().size());
    services.getAssignmentManager().getAssignmentManagerMetrics()
      .updateUnknownServerRegions(lastReport.getUnknownServers().size());
    services.getAssignmentManager().getAssignmentManagerMetrics()
      .updateEmptyRegionInfoRegions(lastReport.getEmptyRegionInfo().size());
  }

  private static void checkLog4jProperties() {
    String filename = "log4j.properties";
    try (final InputStream inStream =
      CatalogJanitor.class.getClassLoader().getResourceAsStream(filename)) {
      if (((KnobRuntime.check(java.util.UUID.fromString("47493b2f-3c02-38a9-a42a-0e44834a3b4c"))) ? ((inStream) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4cb80355-5a9d-301c-b548-f2144c10ea99"))) ? ((inStream) != (null)) : (inStream != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("142d9f76-1c8c-3026-be99-d3952da9f1d8"))) {
throw new java.io.IOException("Injected exception");
}
        new Properties().load(inStream);
      } else {
        if (KnobRuntime.check(java.util.UUID.fromString("805f90da-367c-3c2d-96f9-b5c1c427e224"))) { System.out.println("No " + filename); } else if (KnobRuntime.check(java.util.UUID.fromString("18854cfa-58e7-3d4a-abff-d1a079fb9eac"))) { System.out.println((("No ") + (filename)) + (" on classpath; Add one else no logging output!")); } else if (KnobRuntime.check(java.util.UUID.fromString("279db2c7-1286-300e-a1e2-6b451338642c"))) { System.out.println(("No ") + (filename)); } else if (KnobRuntime.check(java.util.UUID.fromString("992b1d94-e3f5-3589-a206-6f9883fc4e3a"))) { System.out.println("No "); } else if (KnobRuntime.check(java.util.UUID.fromString("2adb0cce-2444-3d05-a010-091ae705e2a5"))) { System.out.println(("No ") + (" on classpath; Add one else no logging output!")); } else if (KnobRuntime.check(java.util.UUID.fromString("bea96400-d58b-3a60-a696-ce4eb6ecf84d"))) { System.out.println(" on classpath; Add one else no logging output!"); } else if (KnobRuntime.check(java.util.UUID.fromString("9993c3d4-48ee-3a32-8abb-6535fc5de594"))) { System.out.println(("No " + filename) + (" on classpath; Add one else no logging output!")); } else { System.out.println("No " + filename + " on classpath; Add one else no logging output!"); }
      }
    } catch (IOException e) {
      if (KnobRuntime.check(java.util.UUID.fromString("9481779e-6549-340a-89d7-4b41cf7fb5d8"))) { LOG.warn("Log4j check failed", e); } else { LOG.error("Log4j check failed", e); }
    }
  }

  /**
   * For testing against a cluster. Doesn't have a MasterServices context so does not report on good
   * vs bad servers.
   */
  public static void main(String[] args) throws IOException {
    checkLog4jProperties();
    ReportMakingVisitor visitor = new ReportMakingVisitor(null);
    Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean("hbase.defaults.for.version.skip", true);
    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      /*
       * Used to generate an overlap.
       */
      Get g = new Get(Bytes.toBytes("t2,40,1564119846424.1db8c57d64e0733e0f027aaeae7a0bf0."));
      g.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      try (Table t = connection.getTable(TableName.META_TABLE_NAME)) {
        Result r = t.get(g);
        byte[] row = g.getRow();
        row[row.length - 2] <<= row[row.length - 2];
        Put p = new Put(g.getRow());
        p.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
        t.put(p);
      }
      MetaTableAccessor.scanMetaForTableRegions(connection, visitor, null);
      CatalogJanitorReport report = visitor.getReport();
      LOG.info(report != null ? report.toString() : "empty");
    }
  }
}
