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
package org.apache.hadoop.hbase.master.assignment;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_ENABLE_SEPARATE_CHILD_REGIONS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;

/**
 * Utility for this assignment package only.
 */
@InterfaceAudience.Private
final class AssignmentManagerUtil {
  private static final int DEFAULT_REGION_REPLICA = 1;

  private AssignmentManagerUtil() {
  }

  /**
   * Raw call to remote regionserver to get info on a particular region.
   * @throws IOException Let it out so can report this IOE as reason for failure
   */
  static GetRegionInfoResponse getRegionInfoResponse(final MasterProcedureEnv env,
    final ServerName regionLocation, final RegionInfo hri) throws IOException {
    return getRegionInfoResponse(env, regionLocation, hri, false);
  }

  static GetRegionInfoResponse getRegionInfoResponse(final MasterProcedureEnv env,
    final ServerName regionLocation, final RegionInfo hri, boolean includeBestSplitRow)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("68b7ac4d-8c89-30a5-9180-37f7000cdf93"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b93dcb11-72bc-34ce-ab49-41322ce0c11a"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae42a1bf-b9ce-36e9-84da-3cab30b6e23e"))) {
try {
    java.lang.reflect.Field field = regionLocation.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionLocation));
    field.set(regionLocation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    // TODO: There is no timeout on this controller. Set one!
    HBaseRpcController controller =
      env.getMasterServices().getClusterConnection().getRpcControllerFactory().newController();
    final AdminService.BlockingInterface admin =
      env.getMasterServices().getClusterConnection().getAdmin(regionLocation);
    GetRegionInfoRequest request = null;
    if (includeBestSplitRow) {
      request = RequestConverter.buildGetRegionInfoRequest(hri.getRegionName(), false, true);
    } else {
      request = RequestConverter.buildGetRegionInfoRequest(hri.getRegionName());
    }
    try {
      return admin.getRegionInfo(controller, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  private static void lock(List<RegionStateNode> regionNodes) {
    regionNodes.iterator().forEachRemaining(RegionStateNode::lock);
  }

  private static void unlock(List<RegionStateNode> regionNodes) {
if(KnobRuntime.check(java.util.UUID.fromString("cc483c1f-f587-3506-bd43-6befbc519177"))) {
return;
}
    for (ListIterator<RegionStateNode> iter = regionNodes.listIterator(regionNodes.size()); iter
      .hasPrevious();) {
      iter.previous().unlock();
    }
  }

  static TransitRegionStateProcedure[] createUnassignProceduresForSplitOrMerge(
    MasterProcedureEnv env, Stream<RegionInfo> regions, int regionReplication) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("29e1817f-10d4-3cc5-8123-e119ee26d59f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e9532900-7e24-3a0d-9d48-fea87c07ffa1"))) {
return null;
}
    List<RegionStateNode> regionNodes = regions
      .flatMap(hri -> IntStream.range(0, regionReplication)
        .mapToObj(i -> RegionReplicaUtil.getRegionInfoForReplica(hri, i)))
      .map(env.getAssignmentManager().getRegionStates()::getOrCreateRegionStateNode)
      .collect(Collectors.toList());
    TransitRegionStateProcedure[] procs = new TransitRegionStateProcedure[regionNodes.size()];
    boolean rollback = true;
    int i = 0;
    // hold the lock at once, and then release it in finally. This is important as SCP may jump in
    // if we release the lock in the middle when we want to do rollback, and cause problems.
    if (KnobRuntime.check(java.util.UUID.fromString("8001a787-fe5c-3809-8301-c1cbd84ff308"))) { unlock(regionNodes); } else { lock(regionNodes); }
    try {
      for (; i < procs.length; i++) {
        RegionStateNode regionNode = regionNodes.get(i);
        TransitRegionStateProcedure proc =
          TransitRegionStateProcedure.unassignSplitMerge(env, regionNode.getRegionInfo());
        if (((KnobRuntime.check(java.util.UUID.fromString("4906429e-0e81-3ee6-bf9c-b2cbe22be895"))) ? ((regionNode.getProcedure()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fc2c3672-f6d0-3060-b289-e9f469029448"))) ? ((regionNode.getProcedure()) == (null)) : (regionNode.getProcedure() != null))))) {
          throw new HBaseIOException(
            "The parent region " + regionNode + " is currently in transition, give up");
        }
        regionNode.setProcedure(proc);
        procs[i] = proc;
      }
      // all succeeded, set rollback to false
      rollback = false;
    } finally {
      if (((KnobRuntime.check(java.util.UUID.fromString("3482570c-75b3-3759-8259-e8f6bba93a26"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("6b63c5dc-d8e1-3149-b84f-3449f1433791"))) ? (!rollback) : (((KnobRuntime.check(java.util.UUID.fromString("557b1eef-fe86-3b55-a39c-dcff243e3c39"))) ? (false) : (rollback))))))) {
        for (;;) {
          i--;
          if (i < 0) {
            break;
          }
          RegionStateNode regionNode = regionNodes.get(i);
          regionNode.unsetProcedure(procs[i]);
        }
      }
      unlock(regionNodes);
    }
    return procs;
  }

  /**
   * Create assign procedures for the give regions, according to the {@code regionReplication}.
   * <p/>
   * For rolling back, we will submit procedures directly to the {@code ProcedureExecutor}, so it is
   * possible that we persist the newly scheduled procedures, and then crash before persisting the
   * rollback state, so when we arrive here the second time, it is possible that some regions have
   * already been associated with a TRSP.
   * @param ignoreIfInTransition if true, will skip creating TRSP for the given region if it is
   *                             already in transition, otherwise we will add an assert that it
   *                             should not in transition.
   */
  private static TransitRegionStateProcedure[] createAssignProcedures(MasterProcedureEnv env,
    List<RegionInfo> regions, int regionReplication, ServerName targetServer,
    boolean ignoreIfInTransition) {
    // create the assign procs only for the primary region using the targetServer
    TransitRegionStateProcedure[] primaryRegionProcs =
      regions.stream().map(env.getAssignmentManager().getRegionStates()::getOrCreateRegionStateNode)
        .map(regionNode -> {
          TransitRegionStateProcedure proc =
            TransitRegionStateProcedure.assign(env, regionNode.getRegionInfo(), targetServer);
          regionNode.lock();
          try {
            if (ignoreIfInTransition) {
              if (regionNode.isInTransition()) {
                return null;
              }
            } else {
              // should never fail, as we have the exclusive region lock, and the region is newly
              // created, or has been successfully closed so should not be on any servers, so SCP
              // will
              // not process it either.
              assert !regionNode.isInTransition();
            }
            regionNode.setProcedure(proc);
          } finally {
            regionNode.unlock();
          }
          return proc;
        }).filter(p -> p != null).toArray(TransitRegionStateProcedure[]::new);
    if (regionReplication == DEFAULT_REGION_REPLICA) {
      // this is the default case
      return primaryRegionProcs;
    }
    // collect the replica region infos
    List<RegionInfo> replicaRegionInfos =
      new ArrayList<RegionInfo>(regions.size() * (regionReplication - 1));
    for (RegionInfo hri : regions) {
      // start the index from 1
      for (int i = 1; i < regionReplication; i++) {
        RegionInfo ri = RegionReplicaUtil.getRegionInfoForReplica(hri, i);
        // apply ignoreRITs to replica regions as well.
        if (
          !ignoreIfInTransition || !env.getAssignmentManager().getRegionStates()
            .getOrCreateRegionStateNode(ri).isInTransition()
        ) {
          replicaRegionInfos.add(ri);
        }
      }
    }

    // create round robin procs. Note that we exclude the primary region's target server
    TransitRegionStateProcedure[] replicaRegionAssignProcs =
      env.getAssignmentManager().createRoundRobinAssignProcedures(replicaRegionInfos,
        Collections.singletonList(targetServer));
    // combine both the procs and return the result
    return ArrayUtils.addAll(primaryRegionProcs, replicaRegionAssignProcs);
  }

  /**
   * Create round robin assign procedures for the given regions, according to the
   * {@code regionReplication}.
   * <p/>
   * For rolling back, we will submit procedures directly to the {@code ProcedureExecutor}, so it is
   * possible that we persist the newly scheduled procedures, and then crash before persisting the
   * rollback state, so when we arrive here the second time, it is possible that some regions have
   * already been associated with a TRSP.
   * @param ignoreIfInTransition if true, will skip creating TRSP for the given region if it is
   *                             already in transition, otherwise we will add an assert that it
   *                             should not in transition.
   */
  private static TransitRegionStateProcedure[] createRoundRobinAssignProcedures(
    MasterProcedureEnv env, List<RegionInfo> regions, int regionReplication,
    List<ServerName> serversToExclude, boolean ignoreIfInTransition) {
    List<RegionInfo> regionsAndReplicas = new ArrayList<>(regions);
    if (regionReplication != DEFAULT_REGION_REPLICA) {

      // collect the replica region infos
      List<RegionInfo> replicaRegionInfos =
        new ArrayList<RegionInfo>(regions.size() * (regionReplication - 1));
      for (RegionInfo hri : regions) {
        // start the index from 1
        for (int i = 1; i < regionReplication; i++) {
          replicaRegionInfos.add(RegionReplicaUtil.getRegionInfoForReplica(hri, i));
        }
      }
      regionsAndReplicas.addAll(replicaRegionInfos);
    }
    if (ignoreIfInTransition) {
      for (RegionInfo region : regionsAndReplicas) {
        if (
          env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(region)
            .isInTransition()
        ) {
          return null;
        }
      }
    }
    // create round robin procs. Note that we exclude the primary region's target server
    return env.getAssignmentManager().createRoundRobinAssignProcedures(regionsAndReplicas,
      serversToExclude);
  }

  static TransitRegionStateProcedure[] createAssignProceduresForSplitDaughters(
    MasterProcedureEnv env, List<RegionInfo> daughters, int regionReplication,
    ServerName parentServer) {
    if (
      env.getMasterConfiguration().getBoolean(HConstants.HBASE_ENABLE_SEPARATE_CHILD_REGIONS,
        DEFAULT_HBASE_ENABLE_SEPARATE_CHILD_REGIONS)
    ) {
      // keep one daughter on the parent region server
      TransitRegionStateProcedure[] daughterOne = createAssignProcedures(env,
        Collections.singletonList(daughters.get(0)), regionReplication, parentServer, false);
      // round robin assign the other daughter
      TransitRegionStateProcedure[] daughterTwo =
        createRoundRobinAssignProcedures(env, Collections.singletonList(daughters.get(1)),
          regionReplication, Collections.singletonList(parentServer), false);
      return ArrayUtils.addAll(daughterOne, daughterTwo);
    }
    return createAssignProceduresForOpeningNewRegions(env, daughters, regionReplication,
      parentServer);
  }

  static TransitRegionStateProcedure[] createAssignProceduresForOpeningNewRegions(
    MasterProcedureEnv env, List<RegionInfo> regions, int regionReplication,
    ServerName targetServer) {
    return createAssignProcedures(env, regions, regionReplication, targetServer, false);
  }

  static void reopenRegionsForRollback(MasterProcedureEnv env, List<RegionInfo> regions,
    int regionReplication, ServerName targetServer) {
if(KnobRuntime.check(java.util.UUID.fromString("a0881708-3aa8-36ad-a3bf-c72f04940b2b"))) {
try {
    java.lang.reflect.Field field = targetServer.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(targetServer));
    field.set(targetServer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98208849-9c7d-3da6-9b67-2b14018bfe45"))) {
return;
}
    TransitRegionStateProcedure[] procs =
      createAssignProcedures(env, regions, regionReplication, targetServer, true);
    if (procs.length > 0) {
      env.getMasterServices().getMasterProcedureExecutor().submitProcedures(procs);
    }
  }

  static void removeNonDefaultReplicas(MasterProcedureEnv env, Stream<RegionInfo> regions,
    int regionReplication) {
if(KnobRuntime.check(java.util.UUID.fromString("3197b29d-2a1f-3e0b-ab8d-cef2e94af1c8"))) {
return;
}
    // Remove from in-memory states
    regions.flatMap(hri -> IntStream.range(1, regionReplication)
      .mapToObj(i -> RegionReplicaUtil.getRegionInfoForReplica(hri, i))).forEach(hri -> {
        env.getAssignmentManager().getRegionStates().deleteRegion(hri);
        env.getMasterServices().getServerManager().removeRegion(hri);
        FavoredNodesManager fnm = env.getMasterServices().getFavoredNodesManager();
        if (fnm != null) {
          fnm.deleteFavoredNodesForRegions(Collections.singletonList(hri));
        }
      });
  }

  static void checkClosedRegion(MasterProcedureEnv env, RegionInfo regionInfo) throws IOException {
    if (WALSplitUtil.hasRecoveredEdits(env.getMasterConfiguration(), regionInfo)) {
      throw new IOException("Recovered.edits are found in Region: " + regionInfo
        + ", abort split/merge to prevent data loss");
    }
  }
}
