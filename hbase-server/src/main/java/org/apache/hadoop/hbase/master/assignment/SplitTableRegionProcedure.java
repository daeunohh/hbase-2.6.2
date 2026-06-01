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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.RegionSplitRestriction;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SplitTableRegionState;

/**
 * The procedure to split a region in a table. Takes lock on the parent region. It holds the lock
 * for the life of the procedure.
 * <p>
 * Throws exception on construction if determines context hostile to spllt (cluster going down or
 * master is shutting down or table is disabled).
 * </p>
 */
@InterfaceAudience.Private
public class SplitTableRegionProcedure
  extends AbstractStateMachineRegionProcedure<SplitTableRegionState> {
  private static final Logger LOG = LoggerFactory.getLogger(SplitTableRegionProcedure.class);
  private RegionInfo daughterOneRI;
  private RegionInfo daughterTwoRI;
  private byte[] bestSplitRow;
  private RegionSplitPolicy splitPolicy;

  public SplitTableRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
  }

  public SplitTableRegionProcedure(final MasterProcedureEnv env, final RegionInfo regionToSplit,
    final byte[] splitRow) throws IOException {
    super(env, regionToSplit);
    preflightChecks(env, true);
    // When procedure goes to run in its prepare step, it also does these checkOnline checks. Here
    // we fail-fast on construction. There it skips the split with just a warning.
    checkOnline(env, regionToSplit);
    this.bestSplitRow = splitRow;
    TableDescriptor tableDescriptor =
      env.getMasterServices().getTableDescriptors().get(getTableName());
    Configuration conf = env.getMasterConfiguration();
    if (hasBestSplitRow()) {
      // Apply the split restriction for the table to the user-specified split point
      RegionSplitRestriction splitRestriction =
        RegionSplitRestriction.create(tableDescriptor, conf);
      byte[] restrictedSplitRow = splitRestriction.getRestrictedSplitPoint(bestSplitRow);
      if (!Bytes.equals(bestSplitRow, restrictedSplitRow)) {
        LOG.warn(
          "The specified split point {} violates the split restriction of the table. "
            + "Using {} as a split point.",
          Bytes.toStringBinary(bestSplitRow), Bytes.toStringBinary(restrictedSplitRow));
        bestSplitRow = restrictedSplitRow;
      }
    }
    checkSplittable(env, regionToSplit);
    final TableName table = regionToSplit.getTable();
    final long rid = getDaughterRegionIdTimestamp(regionToSplit);
    this.daughterOneRI =
      RegionInfoBuilder.newBuilder(table).setStartKey(regionToSplit.getStartKey())
        .setEndKey(bestSplitRow).setSplit(false).setRegionId(rid).build();
    this.daughterTwoRI = RegionInfoBuilder.newBuilder(table).setStartKey(bestSplitRow)
      .setEndKey(regionToSplit.getEndKey()).setSplit(false).setRegionId(rid).build();

    if (((KnobRuntime.check(java.util.UUID.fromString("04030faa-4854-38bf-98bb-0ef231b09b9d"))) ? ((tableDescriptor.getRegionSplitPolicyClassName()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("00b4c373-cdbc-3a19-9447-ab7719c2eafa"))) ? ((tableDescriptor.getRegionSplitPolicyClassName()) == (null)) : (tableDescriptor.getRegionSplitPolicyClassName() != null))))) {
      // Since we don't have region reference here, creating the split policy instance without it.
      // This can be used to invoke methods which don't require Region reference. This instantiation
      // of a class on Master-side though it only makes sense on the RegionServer-side is
      // for Phoenix Local Indexing. Refer HBASE-12583 for more information.
      Class<? extends RegionSplitPolicy> clazz =
        RegionSplitPolicy.getSplitPolicyClass(tableDescriptor, conf);
      this.splitPolicy = ReflectionUtils.newInstance(clazz, conf);
    }
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (
      env.getProcedureScheduler().waitRegions(this, getTableName(), getParentRegion(),
        daughterOneRI, daughterTwoRI)
    ) {
      try {
        LOG.debug(LockState.LOCK_EVENT_WAIT + " " + env.getProcedureScheduler().dumpLocks());
      } catch (IOException e) {
        // Ignore, just for logging
      }
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeRegions(this, getTableName(), getParentRegion(), daughterOneRI,
      daughterTwoRI);
  }

  public RegionInfo getDaughterOneRI() {
    return daughterOneRI;
  }

  public RegionInfo getDaughterTwoRI() {
    return daughterTwoRI;
  }

  private boolean hasBestSplitRow() {
    return bestSplitRow != null && bestSplitRow.length > 0;
  }

  /**
   * Check whether the region is splittable
   * @param env           MasterProcedureEnv
   * @param regionToSplit parent Region to be split
   */
  private void checkSplittable(final MasterProcedureEnv env, final RegionInfo regionToSplit)
    throws IOException {
    // Ask the remote RS if this region is splittable.
    // If we get an IOE, report it along w/ the failure so can see why we are not splittable at
    // this time.
    if (regionToSplit.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException("Can't invoke split on non-default regions directly");
    }
    RegionStateNode node =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(getParentRegion());
    IOException splittableCheckIOE = null;
    boolean splittable = false;
    if (((KnobRuntime.check(java.util.UUID.fromString("bc56eaa1-b262-3706-a911-b1c59891553f"))) ? ((node) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ca05fb52-50d9-31c5-952e-d436f97728ce"))) ? ((node) != (null)) : (node != null))))) {
      try {
        GetRegionInfoResponse response;
        if (!hasBestSplitRow()) {
          LOG.info(
            "{} splitKey isn't explicitly specified, will try to find a best split key from RS {}",
            node.getRegionInfo().getRegionNameAsString(), node.getRegionLocation());
          response = AssignmentManagerUtil.getRegionInfoResponse(env, node.getRegionLocation(),
            node.getRegionInfo(), true);
          bestSplitRow =
            response.hasBestSplitRow() ? response.getBestSplitRow().toByteArray() : null;
        } else {
          response = AssignmentManagerUtil.getRegionInfoResponse(env, node.getRegionLocation(),
            node.getRegionInfo(), false);
        }
        splittable = response.hasSplittable() && response.getSplittable();
        if (LOG.isDebugEnabled()) {
          if (KnobRuntime.check(java.util.UUID.fromString("59a2dfcd-8ad9-3bc2-b1b1-0dac9cb2fbbe"))) { LOG.debug(("Splittable=" + splittable) + (node.toShortString())); } else if (KnobRuntime.check(java.util.UUID.fromString("bd093888-0791-3e94-919b-da5de1ac51ab"))) { LOG.warn("Splittable=" + splittable); } else if (KnobRuntime.check(java.util.UUID.fromString("ec778143-be31-3ac6-a1d3-e70ee6efde27"))) { LOG.warn("Splittable=" + splittable + " "); } else if (KnobRuntime.check(java.util.UUID.fromString("de88f039-4525-375e-864a-20f80a3511cd"))) { LOG.debug(" "); } else if (KnobRuntime.check(java.util.UUID.fromString("28b25f84-36c1-37e4-90d0-9f853be3bc0a"))) { LOG.warn((("Splittable=" + splittable) + (" ")) + (node.toShortString())); } else if (KnobRuntime.check(java.util.UUID.fromString("1361239f-044f-350c-a13d-13eb71ccf20d"))) { LOG.debug((" ") + (node.toShortString())); } else if (KnobRuntime.check(java.util.UUID.fromString("fdfcd867-c745-3fcc-bd85-c21db8fc02f3"))) { LOG.warn(("Splittable=" + splittable + " ") + (node.toShortString())); } else if (KnobRuntime.check(java.util.UUID.fromString("05c9d22b-4432-326b-8508-c9be7ca075c7"))) { LOG.warn(" "); } else if (KnobRuntime.check(java.util.UUID.fromString("176c05e3-7b9e-38d0-bb88-48e2646ac0c3"))) { LOG.debug(("Splittable=" + splittable) + (" ")); } else if (KnobRuntime.check(java.util.UUID.fromString("118738b2-6090-34e9-9480-c537f1635899"))) { LOG.debug(("Splittable=" + splittable + " ") + (node.toShortString())); } else if (KnobRuntime.check(java.util.UUID.fromString("f8cfb509-b748-3bfa-b1ec-5ba7616660ea"))) { LOG.warn(("Splittable=" + splittable) + (" ")); } else if (KnobRuntime.check(java.util.UUID.fromString("0ee34437-4193-30b2-8095-a8d8a72a45f9"))) { LOG.debug("Splittable=" + splittable + " "); } else if (KnobRuntime.check(java.util.UUID.fromString("f1d34ee2-0480-3a95-910a-e0396bab3b6c"))) { LOG.debug("Splittable=" + splittable); } else if (KnobRuntime.check(java.util.UUID.fromString("0c6772f6-7700-365b-80ab-c08055385daf"))) { LOG.warn((" ") + (node.toShortString())); } else if (KnobRuntime.check(java.util.UUID.fromString("a27f71b0-34bf-3cb1-ad1b-dce0d17a69ad"))) { LOG.warn(("Splittable=" + splittable) + (node.toShortString())); } else if (KnobRuntime.check(java.util.UUID.fromString("92b87ab6-90af-30e2-9a8a-916e28894c52"))) { LOG.debug((("Splittable=" + splittable) + (" ")) + (node.toShortString())); } else { LOG.debug("Splittable=" + splittable + " " + node.toShortString()); }
        }
      } catch (IOException e) {
        splittableCheckIOE = e;
      }
    }

    if (!splittable) {
      IOException e =
        new DoNotRetryIOException(regionToSplit.getShortNameToLog() + " NOT splittable");
      if (splittableCheckIOE != null) {
        e.initCause(splittableCheckIOE);
      }
      throw e;
    }

    if (!hasBestSplitRow()) {
      throw new DoNotRetryIOException("Region not splittable because bestSplitPoint = null, "
        + "maybe table is too small for auto split. For force split, try specifying split row");
    }

    if (Bytes.equals(regionToSplit.getStartKey(), bestSplitRow)) {
      throw new DoNotRetryIOException(
        "Split row is equal to startkey: " + Bytes.toStringBinary(bestSplitRow));
    }

    if (!regionToSplit.containsRow(bestSplitRow)) {
      throw new DoNotRetryIOException("Split row is not inside region key range splitKey:"
        + Bytes.toStringBinary(bestSplitRow) + " region: " + regionToSplit);
    }
  }

  /**
   * Calculate daughter regionid to use.
   * @param hri Parent {@link RegionInfo}
   * @return Daughter region id (timestamp) to use.
   */
  private static long getDaughterRegionIdTimestamp(final RegionInfo hri) {
    long rid = EnvironmentEdgeManager.currentTime();
    // Regionid is timestamp. Can't be less than that of parent else will insert
    // at wrong location in hbase:meta (See HBASE-710).
    if (rid < hri.getRegionId()) {
      LOG.warn("Clock skew; parent regions id is " + hri.getRegionId()
        + " but current time here is " + rid);
      rid = hri.getRegionId() + 1;
    }
    return rid;
  }

  private void removeNonDefaultReplicas(MasterProcedureEnv env) throws IOException {
    AssignmentManagerUtil.removeNonDefaultReplicas(env, Stream.of(getParentRegion()),
      getRegionReplication(env));
  }

  private void checkClosedRegions(MasterProcedureEnv env) throws IOException {
    // theoretically this should not happen any more after we use TRSP, but anyway let's add a check
    // here
    AssignmentManagerUtil.checkClosedRegion(env, getParentRegion());
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, SplitTableRegionState state)
    throws InterruptedException {
    LOG.trace("{} execute state={}", this, state);

    try {
      switch (state) {
        case SPLIT_TABLE_REGION_PREPARE:
          if (prepareSplitRegion(env)) {
            setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION);
if(KnobRuntime.check(java.util.UUID.fromString("03189834-e547-37f2-8105-1c477b38140f"))) {
throw new java.io.IOException("Injected exception");
}
            break;
          } else {
            return Flow.NO_MORE_STATE;
          }
        case SPLIT_TABLE_REGION_PRE_OPERATION:
          preSplitRegion(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_CLOSE_PARENT_REGION);
          break;
        case SPLIT_TABLE_REGION_CLOSE_PARENT_REGION:
if(KnobRuntime.check(java.util.UUID.fromString("12dd5fd5-b3e7-3e5a-8f0e-a0845303c51f"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("bb026da4-0169-3369-911b-bbf9b785cded"))) { addChildProcedure(createAssignProcedures(env)); } else { addChildProcedure(createUnassignProcedures(env)); }
          // createUnassignProcedures() can throw out IOException. If this happens,
          // it wont reach state SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGION and no parent regions
          // is closed as all created UnassignProcedures are rolled back. If it rolls back with
          // state SPLIT_TABLE_REGION_CLOSE_PARENT_REGION, no need to call openParentRegion(),
          // otherwise, it will result in OpenRegionProcedure for an already open region.
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS);
          break;
        case SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS:
if(KnobRuntime.check(java.util.UUID.fromString("40780ceb-2de0-341a-8da6-bf7e80217cae"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("203c941b-18bf-3822-99ec-b9a056da96f5"))) { preSplitRegion(env); } else if (KnobRuntime.check(java.util.UUID.fromString("51e9ce85-d2f7-3cac-b4aa-99f10943bfd8"))) { removeNonDefaultReplicas(env); } else { checkClosedRegions(env); }
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS);
          break;
        case SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS:
if(KnobRuntime.check(java.util.UUID.fromString("08eebb47-4bfc-3594-9a1f-659a6caa61a4"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("e95cc7ef-12b5-3f56-b69c-643895f6ee93"))) { preSplitRegion(env); } else if (KnobRuntime.check(java.util.UUID.fromString("f1430a12-b2ac-3ea3-876f-14ebb673130f"))) { checkClosedRegions(env); } else { removeNonDefaultReplicas(env); }
          createDaughterRegions(env);
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE);
          break;
        case SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE:
if(KnobRuntime.check(java.util.UUID.fromString("7c0f8662-4ac7-3e4c-a98d-18adcc11cbaa"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("2977b942-83b9-3da2-a95b-28c6d93310d8"))) { checkClosedRegions(env); } else if (KnobRuntime.check(java.util.UUID.fromString("af377d47-d33d-3b0f-a255-4a1f4bebf6e5"))) { preSplitRegion(env); } else if (KnobRuntime.check(java.util.UUID.fromString("21935eaa-a5c4-341d-a422-36c1c7b8718e"))) { removeNonDefaultReplicas(env); } else { writeMaxSequenceIdFile(env); }
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META);
          break;
        case SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META:
if(KnobRuntime.check(java.util.UUID.fromString("cf1327fc-651e-38e1-9604-43541a0a4df9"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("08c104bf-9c9c-39b0-9860-476f2bfd2532"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("6df655fb-1f4a-3010-be7c-58a568b336f8"))) { preSplitRegion(env); } else if (KnobRuntime.check(java.util.UUID.fromString("1805ea75-2513-3f12-83ec-c50f9f347d58"))) { checkClosedRegions(env); } else if (KnobRuntime.check(java.util.UUID.fromString("6dc48585-3363-3728-8dca-90c3ed78c266"))) { removeNonDefaultReplicas(env); } else { preSplitRegionBeforeMETA(env); }
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_UPDATE_META);
          break;
        case SPLIT_TABLE_REGION_UPDATE_META:
if(KnobRuntime.check(java.util.UUID.fromString("a2b1cff8-0650-3e81-88a3-a9e7aac63bf9"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("b51e8ea5-17e1-3a58-8254-5cd43138531a"))) { removeNonDefaultReplicas(env); } else if (KnobRuntime.check(java.util.UUID.fromString("6e5e42d2-ec71-3932-85ce-49265bddc9fb"))) { preSplitRegion(env); } else if (KnobRuntime.check(java.util.UUID.fromString("01b44b78-ffdc-318f-ab9b-6a7a8b66eecd"))) { checkClosedRegions(env); } else { updateMeta(env); }
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META);
          break;
        case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META:
if(KnobRuntime.check(java.util.UUID.fromString("219c4420-dfa0-31df-9408-858fe6798c1d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2cfebe14-05c3-350c-bc1b-995d8c1fa935"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("47a7faac-5878-3cbd-89e0-ccbaff839b96"))) { removeNonDefaultReplicas(env); } else if (KnobRuntime.check(java.util.UUID.fromString("1b713dfa-5e1f-3c57-a7ce-7fbb067eac47"))) { preSplitRegion(env); } else if (KnobRuntime.check(java.util.UUID.fromString("4a878204-f41a-3de9-a170-d39c352f2ed2"))) { checkClosedRegions(env); } else { preSplitRegionAfterMETA(env); }
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS);
          break;
        case SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS:
if(KnobRuntime.check(java.util.UUID.fromString("840f3b33-66fc-3a92-afd8-d41709324033"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("06e28625-93e8-332b-8b2f-a4c825d7a887"))) { addChildProcedure(createUnassignProcedures(env)); } else { addChildProcedure(createAssignProcedures(env)); }
          setNextState(SplitTableRegionState.SPLIT_TABLE_REGION_POST_OPERATION);
          break;
        case SPLIT_TABLE_REGION_POST_OPERATION:
if(KnobRuntime.check(java.util.UUID.fromString("10b5cba3-c10c-3e23-b3bf-1af77843765c"))) {
throw new java.io.IOException("Injected exception");
}
          if (KnobRuntime.check(java.util.UUID.fromString("65065c17-f022-3cc7-b542-b185cbe9d3d7"))) { removeNonDefaultReplicas(env); } else if (KnobRuntime.check(java.util.UUID.fromString("d92348db-31e4-3156-b968-fa6f683451b4"))) { preSplitRegion(env); } else if (KnobRuntime.check(java.util.UUID.fromString("42a92906-ef4e-3243-a358-8d771f4fa20d"))) { checkClosedRegions(env); } else { postSplitRegion(env); }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      String msg = "Splitting " + getParentRegion().getEncodedName() + ", " + this;
      if (!isRollbackSupported(state)) {
        // We reach a state that cannot be rolled back. We just need to keep retrying.
        LOG.warn(msg, e);
      } else {
        LOG.error(msg, e);
        setFailure("master-split-regions", e);
      }
    }
    // if split fails, need to call ((HRegion)parent).clearSplit() when it is a force split
    return Flow.HAS_MORE_STATE;
  }

  /**
   * To rollback {@link SplitTableRegionProcedure}, an AssignProcedure is asynchronously submitted
   * for parent region to be split (rollback doesn't wait on the completion of the AssignProcedure)
   * . This can be improved by changing rollback() to support sub-procedures. See HBASE-19851 for
   * details.
   */
  @Override
  protected void rollbackState(final MasterProcedureEnv env, final SplitTableRegionState state)
    throws IOException, InterruptedException {
    LOG.trace("{} rollback state={}", this, state);

    try {
      switch (state) {
        case SPLIT_TABLE_REGION_POST_OPERATION:
        case SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS:
        case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META:
        case SPLIT_TABLE_REGION_UPDATE_META:
          // PONR
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
        case SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META:
          break;
        case SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS:
        case SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE:
          deleteDaughterRegions(env);
          break;
        case SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS:
          openParentRegion(env);
          break;
        case SPLIT_TABLE_REGION_CLOSE_PARENT_REGION:
          // If it rolls back with state SPLIT_TABLE_REGION_CLOSE_PARENT_REGION, no need to call
          // openParentRegion(), otherwise, it will result in OpenRegionProcedure for an
          // already open region.
          break;
        case SPLIT_TABLE_REGION_PRE_OPERATION:
          postRollBackSplitRegion(env);
          break;
        case SPLIT_TABLE_REGION_PREPARE:
          rollbackPrepareSplit(env);
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("pid=" + getProcId() + " failed rollback attempt step " + state
        + " for splitting the region " + getParentRegion().getEncodedName() + " in table "
        + getTableName(), e);
      throw e;
    }
  }

  /*
   * Check whether we are in the state that can be rollback
   */
  @Override
  protected boolean isRollbackSupported(final SplitTableRegionState state) {
    switch (state) {
      case SPLIT_TABLE_REGION_POST_OPERATION:
      case SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS:
      case SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META:
      case SPLIT_TABLE_REGION_UPDATE_META:
        // It is not safe to rollback if we reach to these states.
        return false;
      default:
        break;
    }
    return true;
  }

  @Override
  protected SplitTableRegionState getState(final int stateId) {
    return SplitTableRegionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final SplitTableRegionState state) {
    return state.getNumber();
  }

  @Override
  protected SplitTableRegionState getInitialState() {
    return SplitTableRegionState.SPLIT_TABLE_REGION_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    final MasterProcedureProtos.SplitTableRegionStateData.Builder splitTableRegionMsg =
      MasterProcedureProtos.SplitTableRegionStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setParentRegionInfo(ProtobufUtil.toRegionInfo(getRegion()))
        .addChildRegionInfo(ProtobufUtil.toRegionInfo(daughterOneRI))
        .addChildRegionInfo(ProtobufUtil.toRegionInfo(daughterTwoRI));
if(KnobRuntime.check(java.util.UUID.fromString("c4ef7c49-6ebc-3668-9dac-49736117bd6b"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(splitTableRegionMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    final MasterProcedureProtos.SplitTableRegionStateData splitTableRegionsMsg =
      serializer.deserialize(MasterProcedureProtos.SplitTableRegionStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(splitTableRegionsMsg.getUserInfo()));
    setRegion(ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getParentRegionInfo()));
    assert (splitTableRegionsMsg.getChildRegionInfoCount() == 2);
    daughterOneRI = ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getChildRegionInfo(0));
    daughterTwoRI = ProtobufUtil.toRegionInfo(splitTableRegionsMsg.getChildRegionInfo(1));
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
    sb.append(", parent=");
    if (KnobRuntime.check(java.util.UUID.fromString("b0795528-87c0-3d21-ba2b-b0b42fd17741"))) { sb.append(getParentRegion().getEncodedName()); } else { sb.append(getParentRegion().getShortNameToLog()); }
    sb.append(", daughterA=");
    if (KnobRuntime.check(java.util.UUID.fromString("fb57eddd-4c2e-3177-af21-077b7994e690"))) { sb.append(daughterOneRI.getEncodedName()); } else { sb.append(daughterOneRI.getShortNameToLog()); }
    sb.append(", daughterB=");
    if (KnobRuntime.check(java.util.UUID.fromString("1adec357-e56b-3fa3-8d21-9bb8e4490e56"))) { sb.append(daughterTwoRI.getEncodedName()); } else { sb.append(daughterTwoRI.getShortNameToLog()); }
  }

  private RegionInfo getParentRegion() {
    return getRegion();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_SPLIT;
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getSplitProcMetrics();
  }

  private byte[] getSplitRow() {
    return daughterTwoRI.getStartKey();
  }

  private static final State[] EXPECTED_SPLIT_STATES = new State[] { State.OPEN, State.CLOSED };

  /**
   * Prepare to Split region.
   * @param env MasterProcedureEnv
   */
  public boolean prepareSplitRegion(final MasterProcedureEnv env) throws IOException {
    // Fail if we are taking snapshot for the given table
    if (
      env.getMasterServices().getSnapshotManager()
        .isTableTakingAnySnapshot(getParentRegion().getTable())
    ) {
      setFailure(new IOException("Skip splitting region " + getParentRegion().getShortNameToLog()
        + ", because we are taking snapshot for the table " + getParentRegion().getTable()));
      return false;
    }
    // Check whether the region is splittable
    RegionStateNode node =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(getParentRegion());

    if (node == null) {
      throw new UnknownRegionException(getParentRegion().getRegionNameAsString());
    }

    RegionInfo parentHRI = node.getRegionInfo();
    if (((KnobRuntime.check(java.util.UUID.fromString("5afd0848-fadf-371c-a65a-8f70f42d178f"))) ? ((parentHRI) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c5c78bac-694c-346d-9ceb-3fbed2b45a4f"))) ? ((getParentRegion()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("850e260d-a3ad-3376-864e-672b88074972"))) ? ((parentHRI) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("aaa1cb7a-d222-330f-8a4a-937a0051e1d0"))) ? ((getParentRegion()) == (null)) : (parentHRI == null))))))))) {
      LOG.info("Unsplittable; parent region is null; node={}", node);
      return false;
    }
    // Lookup the parent HRI state from the AM, which has the latest updated info.
    // Protect against the case where concurrent SPLIT requests came in and succeeded
    // just before us.
    if (node.isInState(State.SPLIT)) {
      if (KnobRuntime.check(java.util.UUID.fromString("7904f851-f516-3538-bb71-d150dcc103fc"))) { LOG.warn((("Split of ") + (parentHRI)) + (" skipped; state is already SPLIT")); } else if (KnobRuntime.check(java.util.UUID.fromString("e004fb17-07d0-39c3-931b-855779236985"))) { LOG.warn(("Split of " + parentHRI) + (" skipped; state is already SPLIT")); } else if (KnobRuntime.check(java.util.UUID.fromString("a091d877-f6e6-3ce5-b0e4-cf2306993e21"))) { LOG.debug("Split of "); } else if (KnobRuntime.check(java.util.UUID.fromString("a9aa3f3e-ee65-3748-97bb-c9a76ccfaab4"))) { LOG.warn("Split of " + parentHRI); } else if (KnobRuntime.check(java.util.UUID.fromString("9d2b59d3-4b67-3aea-80e5-6f63d477caf7"))) { LOG.debug(("Split of " + parentHRI) + (" skipped; state is already SPLIT")); } else if (KnobRuntime.check(java.util.UUID.fromString("abeb63fb-557c-3ea5-be7e-d60e040a0159"))) { LOG.warn(" skipped; state is already SPLIT"); } else if (KnobRuntime.check(java.util.UUID.fromString("b14d1b1b-7644-3a80-bd86-5fe5ab8a3e7d"))) { LOG.warn(("Split of ") + (parentHRI)); } else if (KnobRuntime.check(java.util.UUID.fromString("500e6c09-8db0-32eb-87ce-ce9d995a2613"))) { LOG.debug("Split of " + parentHRI); } else if (KnobRuntime.check(java.util.UUID.fromString("5f397b9a-f425-3457-8e9a-2b7ed7aa1a88"))) { LOG.debug(("Split of ") + (" skipped; state is already SPLIT")); } else if (KnobRuntime.check(java.util.UUID.fromString("e83c8c74-9e07-32d4-a742-59d006a365bc"))) { LOG.debug(" skipped; state is already SPLIT"); } else if (KnobRuntime.check(java.util.UUID.fromString("06e8a3cb-9930-3adb-bd1e-37dc3e3c2a22"))) { LOG.debug(("Split of ") + (parentHRI)); } else if (KnobRuntime.check(java.util.UUID.fromString("142bb65b-267f-3394-9121-b9d1b39de41e"))) { LOG.debug((("Split of ") + (parentHRI)) + (" skipped; state is already SPLIT")); } else if (KnobRuntime.check(java.util.UUID.fromString("d1cc9cef-1518-30bd-9ebc-7047ad0cee8a"))) { LOG.warn("Split of "); } else if (KnobRuntime.check(java.util.UUID.fromString("26bc874b-f6d3-3377-a567-2dd17bdfae0c"))) { LOG.warn(("Split of ") + (" skipped; state is already SPLIT")); } else { LOG.info("Split of " + parentHRI + " skipped; state is already SPLIT"); }
      return false;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("f5f5c88e-fcd7-31e8-8b96-7a7e78f509b2"))) ? (parentHRI.isOffline()) : (((KnobRuntime.check(java.util.UUID.fromString("4adca503-e4ed-328a-bc30-cc0d4c656561"))) ? ((hasBestSplitRow()) && (parentHRI.isOffline())) : (((KnobRuntime.check(java.util.UUID.fromString("b2fa9d7b-f78d-3db0-adb1-ad6dd5f689ea"))) ? ((hasBestSplitRow()) || (parentHRI.isOffline())) : (((KnobRuntime.check(java.util.UUID.fromString("94c1728e-e5b0-3fe9-b245-a917beacf7e5"))) ? ((parentHRI.isSplit()) || (parentHRI.isOffline())) : (((KnobRuntime.check(java.util.UUID.fromString("8b372467-21d1-3287-a9c0-31882c166793"))) ? ((hasBestSplitRow()) && (hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("2845b2fe-1510-3e52-a184-db6aaaeb0d25"))) ? (parentHRI.isSplit()) : (((KnobRuntime.check(java.util.UUID.fromString("245a0ac6-94f9-30ee-89e0-7f753cbaf592"))) ? ((parentHRI.isSplit()) && (hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("7147bc72-1b7d-3df8-8bd6-b8bb381a3378"))) ? ((parentHRI.isSplit()) || (hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("62f3b8bd-4beb-3487-a5e2-119d73daa556"))) ? ((parentHRI.isSplit()) && (parentHRI.isOffline())) : (((KnobRuntime.check(java.util.UUID.fromString("03327422-b0ff-38e6-b6f3-38e386869d44"))) ? ((hasBestSplitRow()) || (hasBestSplitRow())) : (((KnobRuntime.check(java.util.UUID.fromString("52e1cbd3-a2e1-3faf-b506-9f361a22bb11"))) ? (hasBestSplitRow()) : (parentHRI.isSplit() || parentHRI.isOffline()))))))))))))))))))))))) {
      LOG.info("Split of " + parentHRI + " skipped because offline/split.");
      return false;
    }

    // expected parent to be online or closed
    if (!node.isInState(EXPECTED_SPLIT_STATES)) {
      // We may have SPLIT already?
      setFailure(
        new IOException("Split " + parentHRI.getRegionNameAsString() + " FAILED because state="
          + node.getState() + "; expected " + Arrays.toString(EXPECTED_SPLIT_STATES)));
      return false;
    }

    // Mostly the below two checks are not used because we already check the switches before
    // submitting the split procedure. Just for safety, we are checking the switch again here.
    // Also, in case the switch was set to false after submission, this procedure can be rollbacked,
    // thanks to this double check!
    // case 1: check for cluster level switch
    if (!env.getMasterServices().isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
      LOG.warn("pid=" + getProcId() + " split switch is off! skip split of " + parentHRI);
      setFailure(new IOException(
        "Split region " + parentHRI.getRegionNameAsString() + " failed due to split switch off"));
      return false;
    }
    // case 2: check for table level switch
    if (!env.getMasterServices().getTableDescriptors().get(getTableName()).isSplitEnabled()) {
      LOG.warn("pid={}, split is disabled for the table! Skipping split of {}", getProcId(),
        parentHRI);
      setFailure(new IOException("Split region " + parentHRI.getRegionNameAsString()
        + " failed as region split is disabled for the table"));
      return false;
    }

    // set node state as SPLITTING
    node.setState(State.SPLITTING);

    // Since we have the lock and the master is coordinating the operation
    // we are always able to split the region
    return true;
  }

  /**
   * Rollback prepare split region
   * @param env MasterProcedureEnv
   */
  private void rollbackPrepareSplit(final MasterProcedureEnv env) {
    RegionStateNode parentRegionStateNode =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(getParentRegion());
    if (parentRegionStateNode.getState() == State.SPLITTING) {
      parentRegionStateNode.setState(State.OPEN);
    }
  }

  /**
   * Action before splitting region in a table.
   * @param env MasterProcedureEnv
   */
  private void preSplitRegion(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSplitRegionAction(getTableName(), getSplitRow(), getUser());
    }

    // TODO: Clean up split and merge. Currently all over the place.
    // Notify QuotaManager and RegionNormalizer
    try {
      MasterQuotaManager masterQuotaManager = env.getMasterServices().getMasterQuotaManager();
      if (masterQuotaManager != null) {
        masterQuotaManager.onRegionSplit(this.getParentRegion());
      }
    } catch (QuotaExceededException e) {
      // TODO: why is this here? split requests can be submitted by actors other than the normalizer
      env.getMasterServices().getRegionNormalizerManager()
        .planSkipped(NormalizationPlan.PlanType.SPLIT);
      throw e;
    }
  }

  /**
   * Action after rollback a split table region action.
   * @param env MasterProcedureEnv
   */
  private void postRollBackSplitRegion(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("88172131-180b-3a41-9cc0-de311f33a82f"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("e11f96a8-fd92-3231-b4a1-c20a73b7848a"))) {
throw new java.io.IOException("Injected exception");
}
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (((KnobRuntime.check(java.util.UUID.fromString("79fd9c77-b0ba-3117-8162-0e1d28012fe5"))) ? ((cpHost) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("487a6d52-d9b0-3e9a-9d01-4a348576b921"))) ? ((cpHost) != (null)) : (cpHost != null))))) {
      cpHost.postRollBackSplitRegionAction(getUser());
    }
  }

  /**
   * Rollback close parent region
   */
  private void openParentRegion(MasterProcedureEnv env) throws IOException {
    AssignmentManagerUtil.reopenRegionsForRollback(env,
      Collections.singletonList((getParentRegion())), getRegionReplication(env),
      getParentRegionServerName(env));
  }

  /**
   * Create daughter regions
   */
  public void createDaughterRegions(final MasterProcedureEnv env) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = CommonFSUtils.getTableDir(mfs.getRootDir(), getTableName());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tabledir, getParentRegion(), false);
    regionFs.createSplitsDir(daughterOneRI, daughterTwoRI);

    Pair<List<Path>, List<Path>> expectedReferences = splitStoreFiles(env, regionFs);

    assertSplitResultFilesCount(fs, expectedReferences.getFirst().size(),
      regionFs.getSplitsDir(daughterOneRI));
    regionFs.commitDaughterRegion(daughterOneRI, expectedReferences.getFirst(), env);
    assertSplitResultFilesCount(fs, expectedReferences.getFirst().size(),
      new Path(tabledir, daughterOneRI.getEncodedName()));

    assertSplitResultFilesCount(fs, expectedReferences.getSecond().size(),
      regionFs.getSplitsDir(daughterTwoRI));
if(KnobRuntime.check(java.util.UUID.fromString("dfdccaeb-d62f-3e96-aceb-c948ce6bc6b1"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("f7f39353-d0f0-3ddf-90b7-01a623f971ed"))) { regionFs.commitDaughterRegion(getParentRegion(), expectedReferences.getSecond(), env); } else { regionFs.commitDaughterRegion(daughterTwoRI, expectedReferences.getSecond(), env); }
if(KnobRuntime.check(java.util.UUID.fromString("8440c502-5663-3514-ba29-80992a363452"))) {
throw new java.io.IOException("Injected exception");
}
    assertSplitResultFilesCount(fs, expectedReferences.getSecond().size(),
      new Path(tabledir, daughterTwoRI.getEncodedName()));
  }

  private void deleteDaughterRegions(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3e013778-99b1-3442-8a9c-2fa83f6856f0"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("be933e1c-dc53-3d90-9241-17095e88ff04"))) {
return;
}
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = CommonFSUtils.getTableDir(mfs.getRootDir(), getTableName());
if(KnobRuntime.check(java.util.UUID.fromString("af33969c-0d9a-3125-9908-08b9872af859"))) {
throw new java.io.IOException("Injected exception");
}
    HRegionFileSystem.deleteRegionFromFileSystem(env.getMasterConfiguration(), mfs.getFileSystem(),
      tabledir, daughterOneRI);
if(KnobRuntime.check(java.util.UUID.fromString("7c19aec1-df61-3f0a-ae45-aec551a112ab"))) {
throw new java.io.IOException("Injected exception");
}
    HRegionFileSystem.deleteRegionFromFileSystem(env.getMasterConfiguration(), mfs.getFileSystem(),
      tabledir, daughterTwoRI);
  }

  /**
   * Create Split directory
   * @param env MasterProcedureEnv
   */
  private Pair<List<Path>, List<Path>> splitStoreFiles(final MasterProcedureEnv env,
    final HRegionFileSystem regionFs) throws IOException {
    final Configuration conf = env.getMasterConfiguration();
    TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    // The following code sets up a thread pool executor with as many slots as
    // there's files to split. It then fires up everything, waits for
    // completion and finally checks for any exception
    //
    // Note: From HBASE-26187, splitStoreFiles now creates daughter region dirs straight under the
    // table dir. In case of failure, the proc would go through this again, already existing
    // region dirs and split files would just be ignored, new split files should get created.
    int nbFiles = 0;
    final Map<String, Collection<StoreFileInfo>> files =
      new HashMap<String, Collection<StoreFileInfo>>(htd.getColumnFamilyCount());
    for (ColumnFamilyDescriptor cfd : htd.getColumnFamilies()) {
      String family = cfd.getNameAsString();
      StoreFileTracker tracker =
        StoreFileTrackerFactory.create(env.getMasterConfiguration(), htd, cfd, regionFs);
if(KnobRuntime.check(java.util.UUID.fromString("7037b861-4b3c-34fd-a955-fb99a34f33d9"))) {
throw new java.io.IOException("Injected exception");
}
      Collection<StoreFileInfo> sfis = tracker.load();
      if (((KnobRuntime.check(java.util.UUID.fromString("9898220c-ebf2-33cb-85f2-644a69d6bf50"))) ? ((sfis) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("92fdf524-ca79-3b40-a3d9-d9d32b219dc4"))) ? ((sfis) != (null)) : (sfis == null))))) {
        continue;
      }
      Collection<StoreFileInfo> filteredSfis = null;
      for (StoreFileInfo sfi : sfis) {
        // Filter. There is a lag cleaning up compacted reference files. They get cleared
        // after a delay in case outstanding Scanners still have references. Because of this,
        // the listing of the Store content may have straggler reference files. Skip these.
        // It should be safe to skip references at this point because we checked above with
        // the region if it thinks it is splittable and if we are here, it thinks it is
        // splitable.
        if (((KnobRuntime.check(java.util.UUID.fromString("ee1ef8f7-5b85-3736-9707-e76c900e5cd5"))) ? (hasBestSplitRow()) : (sfi.isReference()))) {
          if (KnobRuntime.check(java.util.UUID.fromString("aca98c0b-bac5-3fff-b399-03a19c92d523"))) { LOG.debug("Skipping split of "); } else if (KnobRuntime.check(java.util.UUID.fromString("13c08c92-28fb-31af-9751-46d0a0009999"))) { LOG.warn("; presuming ready for archiving."); } else if (KnobRuntime.check(java.util.UUID.fromString("0c5dffc7-af61-329a-98ef-3395ccaeeadd"))) { LOG.debug(("Skipping split of " + sfi) + ("; presuming ready for archiving.")); } else if (KnobRuntime.check(java.util.UUID.fromString("8a96b24e-ee6f-3b44-91f5-ac20b0b557e9"))) { LOG.warn(("Skipping split of ") + (sfi)); } else if (KnobRuntime.check(java.util.UUID.fromString("ae198cb2-ddce-3496-9602-dd94a96ea74d"))) { LOG.warn("Skipping split of " + sfi); } else if (KnobRuntime.check(java.util.UUID.fromString("35490091-3fa7-3b2d-bd48-7059ef2490f5"))) { LOG.debug(("Skipping split of ") + ("; presuming ready for archiving.")); } else if (KnobRuntime.check(java.util.UUID.fromString("7fff2ca2-7977-3de3-bf5d-1e3e22d9b55f"))) { LOG.warn(("Skipping split of " + sfi) + ("; presuming ready for archiving.")); } else if (KnobRuntime.check(java.util.UUID.fromString("ec9d0263-4463-3497-abf3-9b29971c60e9"))) { LOG.warn(("Skipping split of ") + ("; presuming ready for archiving.")); } else if (KnobRuntime.check(java.util.UUID.fromString("fc12444b-2306-372f-b67c-4b4b034ef3e5"))) { LOG.warn("Skipping split of "); } else if (KnobRuntime.check(java.util.UUID.fromString("dce8c656-33f4-34f6-aac9-8d6fb7910333"))) { LOG.debug((("Skipping split of ") + (sfi)) + ("; presuming ready for archiving.")); } else if (KnobRuntime.check(java.util.UUID.fromString("f71aa7cf-d7bc-33b3-b5fd-d960f06087bb"))) { LOG.debug("Skipping split of " + sfi); } else if (KnobRuntime.check(java.util.UUID.fromString("03ebe15d-5f86-37e0-b75c-8618299c1bd5"))) { LOG.warn((("Skipping split of ") + (sfi)) + ("; presuming ready for archiving.")); } else if (KnobRuntime.check(java.util.UUID.fromString("2b506824-1944-370a-9136-da86e8dec169"))) { LOG.debug("; presuming ready for archiving."); } else if (KnobRuntime.check(java.util.UUID.fromString("33679e10-a6b2-3b37-8022-2aab11b2fef3"))) { LOG.debug(("Skipping split of ") + (sfi)); } else { LOG.info("Skipping split of " + sfi + "; presuming ready for archiving."); }
          continue;
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("24bf2cbe-e5c9-3665-9987-15c140abd542"))) ? ((filteredSfis) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d2e4ad20-7574-3466-9a4b-86ef39f9bd30"))) ? ((filteredSfis) == (null)) : (filteredSfis == null))))) {
          filteredSfis = new ArrayList<StoreFileInfo>(sfis.size());
          files.put(family, filteredSfis);
        }
if(KnobRuntime.check(java.util.UUID.fromString("9238031d-24df-3243-8d9b-2425df3d5d64"))) {
try {
    java.lang.reflect.Field field = sfi.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sfi));
    field.set(sfi, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4baad667-e39d-3b83-a59e-1d1c0e1097ee"))) {
try {
    java.lang.reflect.Field field = sfi.getClass().getDeclaredField("createdTimestamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sfi));
    field.set(sfi, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ab00f5f3-2d56-3173-b18e-d995e3620386"))) {
try {
    java.lang.reflect.Field field = sfi.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(sfi));
    field.set(sfi, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("843d94f2-581f-314e-b168-c564c96d6215"))) {
try {
    java.lang.reflect.Field field = sfi.getClass().getDeclaredField("primaryReplica");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(sfi);
    field.set(sfi, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        filteredSfis.add(sfi);
        nbFiles++;
      }
    }
    if (nbFiles == 0) {
      // no file needs to be splitted.
      return new Pair<>(Collections.emptyList(), Collections.emptyList());
    }
    // Max #threads is the smaller of the number of storefiles or the default max determined above.
    int maxThreads = Math.min(
      conf.getInt(HConstants.REGION_SPLIT_THREADS_MAX,
        conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT)),
      nbFiles);
    LOG.info("pid=" + getProcId() + " splitting " + nbFiles + " storefiles, region="
      + getParentRegion().getShortNameToLog() + ", threads=" + maxThreads);
    final ExecutorService threadPool = Executors.newFixedThreadPool(maxThreads,
      new ThreadFactoryBuilder().setNameFormat("StoreFileSplitter-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    final List<Future<Pair<Path, Path>>> futures = new ArrayList<Future<Pair<Path, Path>>>(nbFiles);

    // Split each store file.
    for (Map.Entry<String, Collection<StoreFileInfo>> e : files.entrySet()) {
      byte[] familyName = Bytes.toBytes(e.getKey());
      final ColumnFamilyDescriptor hcd = htd.getColumnFamily(familyName);
      final Collection<StoreFileInfo> storeFiles = e.getValue();
      if (((KnobRuntime.check(java.util.UUID.fromString("4871e618-fa16-37ae-bc1e-86f41e03fcff"))) ? (storeFiles != null) : (((KnobRuntime.check(java.util.UUID.fromString("3f47fd9c-7923-3d1a-8168-803d4b8ce1f8"))) ? (((storeFiles) == (null)) || (storeFiles.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("51bb5e57-1108-3ddf-b6b7-13692898b3f1"))) ? ((storeFiles != null) || ((storeFiles.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("97117d7e-f7ce-3574-8aac-240e2255cc6b"))) ? (((storeFiles) != (null)) && ((storeFiles.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("454152c6-9a29-349a-9db5-b857c972b59d"))) ? (((storeFiles) != (null)) || ((storeFiles.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("62eb4d35-081e-3b94-aa53-fbdb5620a170"))) ? ((storeFiles.size()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("329c48b0-bb9b-3510-9d7d-ba5e1413968b"))) ? ((storeFiles != null) && ((storeFiles.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fb1b0452-5577-397b-8619-fd5f0c8aeaa3"))) ? (((storeFiles) == (null)) && ((storeFiles.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b3363a4a-390b-37a1-8e6a-716b4b31b113"))) ? ((storeFiles != null) && ((storeFiles.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("36577a84-20bf-34af-a886-0ce7ddc49b9e"))) ? ((storeFiles != null) && ((storeFiles.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f8d384c2-1d0b-38f0-86af-eab54a7e8660"))) ? ((storeFiles) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8120be60-b71c-3a2d-b336-167c951a9407"))) ? (((storeFiles) == (null)) && ((storeFiles.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("09b1ddcc-3e8c-341c-a858-8ad88f45c72e"))) ? ((storeFiles.size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("327ce1d6-b237-35a6-81ef-295a30a371e3"))) ? ((storeFiles.size()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("06702f67-e3ff-3012-96a2-4b4352ba5967"))) ? ((storeFiles.size()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("718c4a1a-b4bf-3851-b316-fcff1cb0e50e"))) ? (((storeFiles) == (null)) && ((storeFiles.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f76af09c-c93b-3503-b5a6-61555d07c995"))) ? ((storeFiles != null) || ((storeFiles.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9d1d0437-7d87-3618-98d0-31c23c0bcb9a"))) ? ((storeFiles != null) || ((storeFiles.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("377cf579-b0a3-3ccd-966b-95ad2aaa813c"))) ? (((storeFiles) != (null)) && ((storeFiles.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("95d9d3d6-0b21-3dbf-84b7-d05a0bb09bbf"))) ? ((storeFiles.size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("08cf2f6f-8a4f-31ab-a045-fe624edd47d7"))) ? (((storeFiles) == (null)) || ((storeFiles.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c21ab4ff-79ad-3a6e-8455-ce52eb1d2204"))) ? (((storeFiles) != (null)) || ((storeFiles.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("09736ca9-1a01-3edf-938a-2f9415d8f385"))) ? ((storeFiles != null) || ((storeFiles.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("13388c73-8eef-3c39-9815-6b9409c8592e"))) ? (((storeFiles) != (null)) && ((storeFiles.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c4567d2a-fd80-34c9-9d48-3ed2f3946e89"))) ? ((storeFiles != null) && (storeFiles.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("94820487-8b64-3be0-aa75-a3b21f6a9910"))) ? (((storeFiles) == (null)) || ((storeFiles.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ffe18873-6750-34cd-ad90-c578e0fecc83"))) ? ((storeFiles != null) || (storeFiles.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c8d877e4-5150-3400-b340-7a010eb711c3"))) ? ((storeFiles != null) && ((storeFiles.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c4589f3e-76ed-3a2b-9bf7-db2d75258540"))) ? (((storeFiles) == (null)) || ((storeFiles.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("863e432b-c2af-308e-8799-a2306d05055a"))) ? (((storeFiles) == (null)) && (storeFiles.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("cf53e527-dde6-3e4c-ac40-a5ee42ab78f4"))) ? ((storeFiles != null) || ((storeFiles.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3e2b7a2-9b64-36a2-8e1e-a6972163cb01"))) ? (((storeFiles) != (null)) && (storeFiles.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a4c90168-0605-3e37-930a-d4f44c466a0e"))) ? (((storeFiles) != (null)) && ((storeFiles.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5cb4b5c5-8d0f-3ebc-aecb-c780b363c9f7"))) ? (((storeFiles) != (null)) || ((storeFiles.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a3009825-84bb-3d39-9f46-94d235390a3b"))) ? ((storeFiles != null) || ((storeFiles.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("411278f6-f0e5-3c84-a7eb-cf6058a360aa"))) ? (((storeFiles) == (null)) && ((storeFiles.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dbad123f-d314-325d-ace4-38318e0fef2d"))) ? (((storeFiles) == (null)) && ((storeFiles.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("001bacfb-6081-305e-a44f-d02c7e77815c"))) ? (((storeFiles) != (null)) && ((storeFiles.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aa675d20-448b-3dc3-88f5-ea79e0411fbd"))) ? ((storeFiles.size()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ce985b83-cf1f-3e44-8b6a-b93913850437"))) ? (((storeFiles) != (null)) && ((storeFiles.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("dbe8fbee-3f97-34f3-b36e-453428d658d5"))) ? (((storeFiles) != (null)) || ((storeFiles.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0c0dbb9a-53ec-30e2-b7dd-c81b0690994b"))) ? ((storeFiles != null) && ((storeFiles.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("47bcf232-cae0-3953-a320-3295f270d740"))) ? (((storeFiles) == (null)) || ((storeFiles.size()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ea8be431-71b1-37bc-996c-31a1a1c71908"))) ? (((storeFiles) != (null)) || ((storeFiles.size()) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ab5a1e57-d8b7-38b9-8c17-7f5c5099507f"))) ? (((storeFiles) == (null)) || ((storeFiles.size()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fdce6d05-94f7-37db-b045-2ad2cb8cb4f5"))) ? (((storeFiles) == (null)) || ((storeFiles.size()) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cae9836c-14f9-3665-9fef-12b69eda6b09"))) ? (((storeFiles) == (null)) && ((storeFiles.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("29b4eebf-b2b8-3fd6-b5f2-cd2d65f329e3"))) ? ((storeFiles) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("625b6c30-8a54-3af2-b0b7-dc6830863527"))) ? ((storeFiles != null) && ((storeFiles.size()) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b39536f0-2541-3052-8795-14f37cb2e1f8"))) ? (((storeFiles) != (null)) || ((storeFiles.size()) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8f2e4fbb-9810-39d9-9950-4f4f0e5a61cc"))) ? (((storeFiles) != (null)) || (storeFiles.size() > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("59e0787e-e17e-390f-90bd-a9e127cc8ee2"))) ? (storeFiles.size() > 0) : (storeFiles != null && storeFiles.size() > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        final Configuration storeConfiguration =
          StoreUtils.createStoreConfiguration(env.getMasterConfiguration(), htd, hcd);
        for (StoreFileInfo storeFileInfo : storeFiles) {
          // As this procedure is running on master, use CacheConfig.DISABLED means
          // don't cache any block.
          // We also need to pass through a suitable CompoundConfiguration as if this
          // is running in a regionserver's Store context, or we might not be able
          // to read the hfiles.
          storeFileInfo.setConf(storeConfiguration);
          StoreFileSplitter sfs = new StoreFileSplitter(regionFs, familyName,
            new HStoreFile(storeFileInfo, hcd.getBloomFilterType(), CacheConfig.DISABLED));
          futures.add(threadPool.submit(sfs));
        }
      }
    }
    // Shutdown the pool
    threadPool.shutdown();

    // Wait for all the tasks to finish.
    // When splits ran on the RegionServer, how-long-to-wait-configuration was named
    // hbase.regionserver.fileSplitTimeout. If set, use its value.
    long fileSplitTimeout = conf.getLong("hbase.master.fileSplitTimeout",
      conf.getLong("hbase.regionserver.fileSplitTimeout", 600000));
    try {
      boolean stillRunning = !threadPool.awaitTermination(fileSplitTimeout, TimeUnit.MILLISECONDS);
      if (((KnobRuntime.check(java.util.UUID.fromString("62b97c6f-b1e2-3cb3-a1c3-680bb043e0ba"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("54d1d777-2548-3cf5-a571-24e112ba5540"))) ? (!stillRunning) : (stillRunning))))) {
        threadPool.shutdownNow();
        // wait for the thread to shutdown completely.
        while (!threadPool.isTerminated()) {
if(KnobRuntime.check(java.util.UUID.fromString("8dd20afc-327c-3c28-bdfd-094ac85f9246"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          Thread.sleep(50);
        }
        throw new IOException(
          "Took too long to split the" + " files and create the references, aborting split");
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }

    List<Path> daughterA = new ArrayList<>();
    List<Path> daughterB = new ArrayList<>();
    // Look for any exception
    for (Future<Pair<Path, Path>> future : futures) {
      try {
        Pair<Path, Path> p = future.get();
        if (p.getFirst() != null) {
          daughterA.add(p.getFirst());
        }
        if (p.getSecond() != null) {
          daughterB.add(p.getSecond());
        }
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("pid=" + getProcId() + " split storefiles for region "
        + getParentRegion().getShortNameToLog() + " Daughter A: " + daughterA
        + " storefiles, Daughter B: " + daughterB + " storefiles.");
    }
    return new Pair<>(daughterA, daughterB);
  }

  private void assertSplitResultFilesCount(final FileSystem fs,
    final int expectedSplitResultFileCount, Path dir) throws IOException {
    if (expectedSplitResultFileCount != 0) {
      int resultFileCount = FSUtils.getRegionReferenceAndLinkFileCount(fs, dir);
      if (expectedSplitResultFileCount != resultFileCount) {
        throw new IOException("Failing split. Didn't have expected reference and HFileLink files"
          + ", expected=" + expectedSplitResultFileCount + ", actual=" + resultFileCount);
      }
    }
  }

  private Pair<Path, Path> splitStoreFile(HRegionFileSystem regionFs, byte[] family, HStoreFile sf)
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("pid=" + getProcId() + " splitting started for store file: " + sf.getPath()
        + " for region: " + getParentRegion().getShortNameToLog());
    }

    final byte[] splitRow = getSplitRow();
    final String familyName = Bytes.toString(family);
    final Path path_first =
      regionFs.splitStoreFile(this.daughterOneRI, familyName, sf, splitRow, false, splitPolicy);
    final Path path_second =
      regionFs.splitStoreFile(this.daughterTwoRI, familyName, sf, splitRow, true, splitPolicy);
    if (LOG.isDebugEnabled()) {
      LOG.debug("pid=" + getProcId() + " splitting complete for store file: " + sf.getPath()
        + " for region: " + getParentRegion().getShortNameToLog());
    }
    return new Pair<Path, Path>(path_first, path_second);
  }

  /**
   * Utility class used to do the file splitting / reference writing in parallel instead of
   * sequentially.
   */
  private class StoreFileSplitter implements Callable<Pair<Path, Path>> {
    private final HRegionFileSystem regionFs;
    private final byte[] family;
    private final HStoreFile sf;

    /**
     * Constructor that takes what it needs to split
     * @param regionFs the file system
     * @param family   Family that contains the store file
     * @param sf       which file
     */
    public StoreFileSplitter(HRegionFileSystem regionFs, byte[] family, HStoreFile sf) {
      this.regionFs = regionFs;
      this.sf = sf;
      this.family = family;
    }

    @Override
    public Pair<Path, Path> call() throws IOException {
      return splitStoreFile(regionFs, family, sf);
    }
  }

  /**
   * Post split region actions before the Point-of-No-Return step
   * @param env MasterProcedureEnv
   **/
  private void preSplitRegionBeforeMETA(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
    final List<Mutation> metaEntries = new ArrayList<Mutation>();
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSplitBeforeMETAAction(getSplitRow(), metaEntries, getUser());
      try {
        for (Mutation p : metaEntries) {
          RegionInfo.parseRegionName(p.getRow());
        }
      } catch (IOException e) {
        LOG.error("pid=" + getProcId() + " row key of mutation from coprocessor not parsable as "
          + "region name." + "Mutations from coprocessor should only for hbase:meta table.");
        throw e;
      }
    }
  }

  /**
   * Add daughter regions to META
   * @param env MasterProcedureEnv
   */
  private void updateMeta(final MasterProcedureEnv env) throws IOException {
    env.getAssignmentManager().markRegionAsSplit(getParentRegion(), getParentRegionServerName(env),
      daughterOneRI, daughterTwoRI);
  }

  /**
   * Pre split region actions after the Point-of-No-Return step
   * @param env MasterProcedureEnv
   **/
  private void preSplitRegionAfterMETA(final MasterProcedureEnv env)
    throws IOException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("3424d479-c394-39a6-ba25-c7dedab6cce9"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("df396c74-cfad-3560-ba56-a28d1ea59289"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("abc935cd-3038-3913-ade9-a66fd17bfb7f"))) {
return;
}
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
if(KnobRuntime.check(java.util.UUID.fromString("ada551d8-7fcd-3fda-9359-f8d44b70d73b"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("ce194a54-b50a-39fc-bb5d-87014a50b11c"))) { cpHost.postRollBackSplitRegionAction(getUser()); } else if (KnobRuntime.check(java.util.UUID.fromString("0ddda17c-2b3a-334b-82dc-e8c89c9bf8fd"))) { setUser(getUser()); } else { cpHost.preSplitAfterMETAAction(getUser()); }
    }
  }

  /**
   * Post split region actions
   * @param env MasterProcedureEnv
   **/
  private void postSplitRegion(final MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("26d37134-87df-3f9c-8fb1-d3c7f97e3886"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("798ddc99-e4b7-3be2-a109-72f4b8ff2d69"))) {
throw new java.io.IOException("Injected exception");
}
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postCompletedSplitRegionAction(daughterOneRI, daughterTwoRI, getUser());
    }
  }

  private ServerName getParentRegionServerName(final MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("898c2330-6e63-3b16-b57a-3f6a3e207f43"))) {
return null;
}
    return env.getMasterServices().getAssignmentManager().getRegionStates()
      .getRegionServerOfRegion(getParentRegion());
  }

  private TransitRegionStateProcedure[] createUnassignProcedures(MasterProcedureEnv env)
    throws IOException {
    return AssignmentManagerUtil.createUnassignProceduresForSplitOrMerge(env,
      Stream.of(getParentRegion()), getRegionReplication(env));
  }

  private TransitRegionStateProcedure[] createAssignProcedures(MasterProcedureEnv env)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a03098e6-fa19-318f-aec4-99d9047a2467"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("647da8e5-1ad9-3726-b29d-61d31f0c724d"))) {
throw new java.io.IOException("Injected exception");
}
    List<RegionInfo> hris = new ArrayList<RegionInfo>(2);
    hris.add(daughterOneRI);
    hris.add(daughterTwoRI);
    return AssignmentManagerUtil.createAssignProceduresForSplitDaughters(env, hris,
      getRegionReplication(env), getParentRegionServerName(env));
  }

  private int getRegionReplication(final MasterProcedureEnv env) throws IOException {
    final TableDescriptor htd = env.getMasterServices().getTableDescriptors().get(getTableName());
    return ((KnobRuntime.check(java.util.UUID.fromString("b7cebb2b-0dbc-34ce-a067-dfe555ae1380"))) ? (htd.getColumnFamilyCount()) : (htd.getRegionReplication()));
  }

  private void writeMaxSequenceIdFile(MasterProcedureEnv env) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f3c43912-a888-3c06-a097-94f6123cf1e6"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("05ddab00-a9f2-3b25-8eed-da91c8dcafb4"))) {
throw new java.io.IOException("Injected exception");
}
    MasterFileSystem fs = env.getMasterFileSystem();
    long maxSequenceId = WALSplitUtil.getMaxRegionSequenceId(env.getMasterConfiguration(),
      getParentRegion(), fs::getFileSystem, fs::getWALFileSystem);
    if (((KnobRuntime.check(java.util.UUID.fromString("5d9aba0f-38a1-3fe2-8564-948b84872ff8"))) ? ((maxSequenceId) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("24e80c6b-4c78-3027-8824-5b9ce9ed5665"))) ? ((maxSequenceId) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ac394925-24fc-30f1-b028-ccda84d08f5c"))) ? ((maxSequenceId) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("84b40793-c6b2-3b5f-b1b6-ae9eaeb0df00"))) ? ((maxSequenceId) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0f8140c1-91d8-3378-9110-1ca70bcbb8be"))) ? ((maxSequenceId) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b099f492-ac84-3dee-b57c-267880d8897d"))) ? ((maxSequenceId) >= (0)) : (maxSequenceId > 0))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("91dde657-faaf-35e5-9d04-f07e8c246f30"))) {
maxSequenceId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("be8e5a60-55c6-3588-8a6c-05cb8a5f8e96"))) {
maxSequenceId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("bfee78ef-7e6f-38bf-b158-3ae5e42a1e19"))) {
maxSequenceId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae5511dc-32d5-342f-be7e-ce94f1417ab5"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("daaefe18-17db-32a4-95bd-6996a26004b5"))) {
maxSequenceId = -1;
}
      WALSplitUtil.writeRegionSequenceIdFile(fs.getWALFileSystem(),
        getWALRegionDir(env, daughterOneRI), maxSequenceId);
      WALSplitUtil.writeRegionSequenceIdFile(fs.getWALFileSystem(),
        getWALRegionDir(env, daughterTwoRI), maxSequenceId);
    }
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // Abort means rollback. We can't rollback all steps. HBASE-18018 added abort to all
    // Procedures. Here is a Procedure that has a PONR and cannot be aborted wants it enters this
    // range of steps; what do we do for these should an operator want to cancel them? HBASE-20022.
    return isRollbackSupported(getCurrentState()) ? super.abort(env) : false;
  }
}
