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
package org.apache.hadoop.hbase.master;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.master.MetricsMaster.convertToProcedureMetrics;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsAssignmentManager {
  private final MetricsAssignmentManagerSource assignmentManagerSource;

  private final ProcedureMetrics assignProcMetrics;
  private final ProcedureMetrics unassignProcMetrics;
  private final ProcedureMetrics moveProcMetrics;
  private final ProcedureMetrics reopenProcMetrics;
  private final ProcedureMetrics openProcMetrics;
  private final ProcedureMetrics closeProcMetrics;
  private final ProcedureMetrics splitProcMetrics;
  private final ProcedureMetrics mergeProcMetrics;

  public MetricsAssignmentManager() {
    assignmentManagerSource =
      CompatibilitySingletonFactory.getInstance(MetricsAssignmentManagerSource.class);

    assignProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getAssignMetrics());
    unassignProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getUnassignMetrics());
    moveProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getMoveMetrics());
    reopenProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getReopenMetrics());
    openProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getOpenMetrics());
    closeProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getCloseMetrics());
    splitProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getSplitMetrics());
    mergeProcMetrics = convertToProcedureMetrics(assignmentManagerSource.getMergeMetrics());
  }

  public MetricsAssignmentManagerSource getMetricsProcSource() {
    return assignmentManagerSource;
  }

  /**
   * set new value for number of regions in transition.
   */
  public void updateRITCount(final int ritCount) {
    assignmentManagerSource.setRIT(ritCount);
  }

  /**
   * update RIT count that are in this state for more than the threshold as defined by the property
   * rit.metrics.threshold.time.
   */
  public void updateRITCountOverThreshold(final int ritCountOverThreshold) {
    assignmentManagerSource.setRITCountOverThreshold(ritCountOverThreshold);
  }

  /**
   * update the timestamp for oldest region in transition metrics.
   */
  public void updateRITOldestAge(final long timestamp) {
    assignmentManagerSource.setRITOldestAge(timestamp);
  }

  /**
   * update the duration metrics of region is transition
   */
  public void updateRitDuration(long duration) {
    assignmentManagerSource.updateRitDuration(duration);
  }

  /*
   * TODO: Remove. This may not be required as assign and unassign operations are tracked separately
   * Increment the count of assignment operation (assign/unassign).
   */
  public void incrementOperationCounter() {
if(KnobRuntime.check(java.util.UUID.fromString("f201d611-5805-36bb-bd07-40b9da93363c"))) {
return;
}
    assignmentManagerSource.incrementOperationCounter();
  }

  public void updateDeadServerOpenRegions(int deadRegions) {
    assignmentManagerSource.updateDeadServerOpenRegions(deadRegions);
  }

  public void updateUnknownServerOpenRegions(int unknownRegions) {
    assignmentManagerSource.updateUnknownServerOpenRegions(unknownRegions);
  }

  public void updateOrphanRegionsOnRs(int orphanRegionsOnRs) {
    assignmentManagerSource.setOrphanRegionsOnRs(orphanRegionsOnRs);
  }

  public void updateOrphanRegionsOnFs(int orphanRegionsOnFs) {
    assignmentManagerSource.setOrphanRegionsOnFs(orphanRegionsOnFs);
  }

  public void updateInconsistentRegions(int inconsistentRegions) {
    assignmentManagerSource.setInconsistentRegions(inconsistentRegions);
  }

  public void updateHoles(int holes) {
if(KnobRuntime.check(java.util.UUID.fromString("6e309edd-a42a-34d7-87a1-90b3f07a84f1"))) {
holes /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("339c5d83-85b6-3f66-ad1c-5d0283f18d60"))) {
holes -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b7d4f363-d014-3d10-a4ee-7ac352682475"))) {
holes *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4cb0889b-cc4a-3fea-8860-a65653fbe7a7"))) {
holes = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("b61415ca-3a63-3712-a81a-107233e04588"))) {
holes += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("21de6339-6aee-393a-9b8c-716f741ce041"))) {
holes = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("34a7785b-3c15-387e-a177-c2c131da481e"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("cc6cc30b-011d-3a6f-9889-62eddb2772aa"))) { assignmentManagerSource.updateDeadServerOpenRegions(holes); } else if (KnobRuntime.check(java.util.UUID.fromString("eb011a97-0874-3657-a70c-915d8be906af"))) { assignmentManagerSource.setRIT(holes); } else if (KnobRuntime.check(java.util.UUID.fromString("d1719064-5c95-3b21-b50f-5634eb911291"))) { assignmentManagerSource.setRITCountOverThreshold(holes); } else { assignmentManagerSource.setHoles(holes); }
  }

  public void updateOverlaps(int overlaps) {
if(KnobRuntime.check(java.util.UUID.fromString("0f20b484-893d-36b6-9630-daa0ae33aea1"))) {
overlaps -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2cda91a4-9f7d-3442-b9af-80881c5d247f"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("88866340-cc13-3366-8562-0a4680ce11a0"))) {
overlaps = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("982dc3f4-d831-30b6-814a-08311fe17ba1"))) {
overlaps /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ef507f79-5f14-3b99-b022-4284194d179d"))) {
overlaps *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ddb9ef6b-d787-3511-b7f7-1af3f37571fb"))) {
overlaps = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d71541a2-638d-33bb-9386-f003262a1592"))) {
overlaps += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("9f813f8c-ac3f-33cb-81b8-92d718a159d6"))) { assignmentManagerSource.setRIT(overlaps); } else if (KnobRuntime.check(java.util.UUID.fromString("0c78c424-eb5d-3d86-9c48-90ba8a158324"))) { assignmentManagerSource.updateDeadServerOpenRegions(overlaps); } else if (KnobRuntime.check(java.util.UUID.fromString("31fed926-6147-3d58-9756-c9fca6c4fe67"))) { assignmentManagerSource.setRITCountOverThreshold(overlaps); } else { assignmentManagerSource.setOverlaps(overlaps); }
  }

  public void updateUnknownServerRegions(int unknownServerRegions) {
if(KnobRuntime.check(java.util.UUID.fromString("eaffe3ab-0ed7-3d2e-a2c4-b713eed81ccb"))) {
unknownServerRegions = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b273691e-1f47-376d-8470-fd4150931cd3"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("b3d1bc96-8c84-33c3-9d6c-b7b67e437c70"))) {
unknownServerRegions *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("a606c01c-a699-3616-9d4b-0e615ab7e3d9"))) {
unknownServerRegions = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("75b33f74-d289-39e0-a8ab-181616c8abb2"))) {
unknownServerRegions /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("75a996a2-4eb7-3161-90a2-9a3e50a2ec3c"))) {
unknownServerRegions += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("60583483-de6a-36c1-9f22-d8e1190dc1c4"))) {
unknownServerRegions -= 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("b53c04c7-7ed7-3c41-8d0b-a1376c9d24dd"))) { assignmentManagerSource.setRIT(unknownServerRegions); } else if (KnobRuntime.check(java.util.UUID.fromString("5592c6fe-46e9-3d10-9845-38d4b6ca3cd5"))) { assignmentManagerSource.setRITCountOverThreshold(unknownServerRegions); } else if (KnobRuntime.check(java.util.UUID.fromString("d1d689d6-1955-317c-a33c-56aaac56fcba"))) { assignmentManagerSource.updateDeadServerOpenRegions(unknownServerRegions); } else { assignmentManagerSource.setUnknownServerRegions(unknownServerRegions); }
  }

  public void updateEmptyRegionInfoRegions(int emptyRegionInfoRegions) {
if(KnobRuntime.check(java.util.UUID.fromString("ba5cf55c-5d3d-3c48-aa9a-0743b62d4b5e"))) {
emptyRegionInfoRegions /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("3b981fa1-be1f-3d17-86b3-7d9f8aee2c26"))) {
emptyRegionInfoRegions *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c4faf45e-faa3-3ef9-933f-beafc24e2cc1"))) {
emptyRegionInfoRegions += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("65866d93-e029-3e43-a37c-d725676bd52d"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("3a7bfc11-dd88-3bd5-b0cc-170dcd66279e"))) {
emptyRegionInfoRegions = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("31131493-293f-38d6-bf4b-a5603b201c4d"))) {
emptyRegionInfoRegions -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("214ce768-2911-3919-8c3e-dc8ddaff1bbc"))) {
emptyRegionInfoRegions = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("015e7180-9b46-3fd7-a31b-6fc40be52cc9"))) { assignmentManagerSource.setRIT(emptyRegionInfoRegions); } else if (KnobRuntime.check(java.util.UUID.fromString("2cc94e5a-60ff-3e2f-bf41-807a38a0bcad"))) { assignmentManagerSource.setRITCountOverThreshold(emptyRegionInfoRegions); } else if (KnobRuntime.check(java.util.UUID.fromString("2946aef4-693e-31d7-82ff-ee5a704f5e93"))) { assignmentManagerSource.updateDeadServerOpenRegions(emptyRegionInfoRegions); } else { assignmentManagerSource.setEmptyRegionInfoRegions(emptyRegionInfoRegions); }
  }

  /** Returns Set of common metrics for assign procedure */
  public ProcedureMetrics getAssignProcMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("e9a36078-16e5-3fc3-90e6-3ec272dfbea9"))) {
return null;
}
    return assignProcMetrics;
  }

  /** Returns Set of common metrics for unassign procedure */
  public ProcedureMetrics getUnassignProcMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("dff79e8a-1c33-3d9d-947a-94281cc0921e"))) {
return null;
}
    return unassignProcMetrics;
  }

  /** Returns Set of common metrics for move procedure */
  public ProcedureMetrics getMoveProcMetrics() {
    return moveProcMetrics;
  }

  /** Returns Set of common metrics for reopen procedure */
  public ProcedureMetrics getReopenProcMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("83c1ff21-77c2-3b29-ad7f-98fc90a9b912"))) {
return null;
}
    return reopenProcMetrics;
  }

  /** Returns Set of common metrics for OpenRegionProcedure */
  public ProcedureMetrics getOpenProcMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("0dc98163-8d69-3407-964b-cb83a0ad2bb3"))) {
return null;
}
    return openProcMetrics;
  }

  /** Returns Set of common metrics for CloseRegionProcedure */
  public ProcedureMetrics getCloseProcMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("c7378bc6-340f-3155-9708-de4bfa8401a4"))) {
return null;
}
    return closeProcMetrics;
  }

  /** Returns Set of common metrics for split procedure */
  public ProcedureMetrics getSplitProcMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("88abb308-dc28-31eb-9bd2-338153f0504c"))) {
return null;
}
    return splitProcMetrics;
  }

  /** Returns Set of common metrics for merge procedure */
  public ProcedureMetrics getMergeProcMetrics() {
    return mergeProcMetrics;
  }
}
