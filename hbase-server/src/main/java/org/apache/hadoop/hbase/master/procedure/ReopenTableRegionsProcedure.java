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
package org.apache.hadoop.hbase.master.procedure;
import org.knobinjection.runtime.KnobRuntime;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Used for reopening the regions for a table.
 */
@InterfaceAudience.Private
public class ReopenTableRegionsProcedure
  extends AbstractStateMachineTableProcedure<ReopenTableRegionsState> {

  private static final Logger LOG = LoggerFactory.getLogger(ReopenTableRegionsProcedure.class);

  public static final String PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY =
    "hbase.reopen.table.regions.progressive.batch.backoff.ms";
  public static final long PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT = 0L;
  public static final String PROGRESSIVE_BATCH_SIZE_MAX_KEY =
    "hbase.reopen.table.regions.progressive.batch.size.max";
  public static final int PROGRESSIVE_BATCH_SIZE_MAX_DISABLED = -1;
  private static final int PROGRESSIVE_BATCH_SIZE_MAX_DEFAULT_VALUE = Integer.MAX_VALUE;

  // this minimum prevents a max which would break this procedure
  private static final int MINIMUM_BATCH_SIZE_MAX = 1;

  private TableName tableName;

  // Specify specific regions of a table to reopen.
  // if specified null, all regions of the table will be reopened.
  private List<byte[]> regionNames;

  private List<HRegionLocation> regions = Collections.emptyList();

  private List<HRegionLocation> currentRegionBatch = Collections.emptyList();

  private RetryCounter retryCounter;

  private long reopenBatchBackoffMillis;
  private int reopenBatchSize;
  private int reopenBatchSizeMax;
  private long regionsReopened = 0;
  private long batchesProcessed = 0;

  public ReopenTableRegionsProcedure() {
    this(null);
  }

  public ReopenTableRegionsProcedure(TableName tableName) {
    this(tableName, Collections.emptyList());
  }

  public ReopenTableRegionsProcedure(final TableName tableName, final List<byte[]> regionNames) {
    this(tableName, regionNames, PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT,
      PROGRESSIVE_BATCH_SIZE_MAX_DISABLED);
  }

  public ReopenTableRegionsProcedure(final TableName tableName, long reopenBatchBackoffMillis,
    int reopenBatchSizeMax) {
    this(tableName, Collections.emptyList(), reopenBatchBackoffMillis, reopenBatchSizeMax);
  }

  public ReopenTableRegionsProcedure(final TableName tableName, final List<byte[]> regionNames,
    long reopenBatchBackoffMillis, int reopenBatchSizeMax) {
    this.tableName = tableName;
    this.regionNames = regionNames;
    this.reopenBatchBackoffMillis = reopenBatchBackoffMillis;
    if (reopenBatchSizeMax == PROGRESSIVE_BATCH_SIZE_MAX_DISABLED) {
      this.reopenBatchSize = Integer.MAX_VALUE;
      this.reopenBatchSizeMax = Integer.MAX_VALUE;
    } else {
      this.reopenBatchSize = 1;
      this.reopenBatchSizeMax = Math.max(reopenBatchSizeMax, MINIMUM_BATCH_SIZE_MAX);
    }
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public long getRegionsReopened() {
if(KnobRuntime.check(java.util.UUID.fromString("ab400a03-042d-3cd3-bbeb-754ac68d2cb9"))) {
return 0;
}
    return regionsReopened;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public long getBatchesProcessed() {
if(KnobRuntime.check(java.util.UUID.fromString("133b7a7a-c0b4-359f-9a58-da21fe952d3f"))) {
return 0;
}
    return batchesProcessed;
  }

  @RestrictedApi(explanation = "Should only be called internally or in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|ReopenTableRegionsProcedure).java")
  protected int progressBatchSize() {
    int previousBatchSize = reopenBatchSize;
    reopenBatchSize = Math.min(reopenBatchSizeMax, 2 * reopenBatchSize);
    if (reopenBatchSize < previousBatchSize) {
      // the batch size should never decrease. this must be overflow, so just use max
      reopenBatchSize = reopenBatchSizeMax;
    }
    return reopenBatchSize;
  }

  private boolean canSchedule(MasterProcedureEnv env, HRegionLocation loc) {
if(KnobRuntime.check(java.util.UUID.fromString("40fc32ca-2874-3a1b-b633-3c0d54a5333a"))) {
try {
    java.lang.reflect.Field field = loc.getClass().getDeclaredField("seqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(loc));
    field.set(loc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4b8d7ba-3011-3a02-aaff-106b24c106d9"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("bff0ccb8-3c6f-34ce-9c83-1cfcb07c998d"))) {
return true;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("b2afe3f0-8099-31b1-ba15-20ed9206ff93"))) ? ((loc.getSeqNum()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e5118eb3-093e-3d44-8166-e82474cb1585"))) ? ((loc.getSeqNum()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e2d3c921-5b0b-341c-afa8-cc61e58e19bb"))) ? ((loc.getSeqNum()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8cd141e8-0618-307b-a99a-8e1c310b52de"))) ? ((loc.getSeqNum()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("61a201df-3d57-3398-8603-18ca8fc392e7"))) ? ((loc.getSeqNum()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c52c6262-5669-3ef3-823b-41e809b007c3"))) ? ((loc.getSeqNum()) != (0)) : (loc.getSeqNum() < 0))))))))))))) {
      return false;
    }
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(loc.getRegion());
    // If the region node is null, then at least in the next round we can remove this region to make
    // progress. And the second condition is a normal one, if there are no TRSP with it then we can
    // schedule one to make progress.
    return regionNode == null || !regionNode.isInTransition();
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ReopenTableRegionsState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case REOPEN_TABLE_REGIONS_GET_REGIONS:
        if (!isTableEnabled(env)) {
          LOG.info("Table {} is disabled, give up reopening its regions", tableName);
          return Flow.NO_MORE_STATE;
        }
        List<HRegionLocation> tableRegions =
          env.getAssignmentManager().getRegionStates().getRegionsOfTableForReopen(tableName);
        regions = getRegionLocationsForReopen(tableRegions);
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_REOPEN_REGIONS:
        // if we didn't finish reopening the last batch yet, let's keep trying until we do.
        // at that point, the batch will be empty and we can generate a new batch
        if (!regions.isEmpty() && currentRegionBatch.isEmpty()) {
          currentRegionBatch = regions.stream().limit(reopenBatchSize).collect(Collectors.toList());
          batchesProcessed++;
        }
        for (HRegionLocation loc : currentRegionBatch) {
          RegionStateNode regionNode =
            env.getAssignmentManager().getRegionStates().getRegionStateNode(loc.getRegion());
          // this possible, maybe the region has already been merged or split, see HBASE-20921
          if (regionNode == null) {
            continue;
          }
          TransitRegionStateProcedure proc;
          regionNode.lock();
          try {
            if (regionNode.getProcedure() != null) {
              continue;
            }
            proc = TransitRegionStateProcedure.reopen(env, regionNode.getRegionInfo());
            regionNode.setProcedure(proc);
          } finally {
            regionNode.unlock();
          }
          addChildProcedure(proc);
          regionsReopened++;
        }
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_CONFIRM_REOPENED);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_CONFIRM_REOPENED:
        // update region lists based on what's been reopened
        regions = filterReopened(env, regions);
        currentRegionBatch = filterReopened(env, currentRegionBatch);

        // existing batch didn't fully reopen, so try to resolve that first.
        // since this is a retry, don't do the batch backoff
        if (!currentRegionBatch.isEmpty()) {
          return reopenIfSchedulable(env, currentRegionBatch, false);
        }

        if (regions.isEmpty()) {
          return Flow.NO_MORE_STATE;
        }

        // current batch is finished, schedule more regions
        return reopenIfSchedulable(env, regions, true);
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  private List<HRegionLocation> filterReopened(MasterProcedureEnv env,
    List<HRegionLocation> regionsToCheck) {
    return regionsToCheck.stream().map(env.getAssignmentManager().getRegionStates()::checkReopened)
      .filter(l -> l != null).collect(Collectors.toList());
  }

  private Flow reopenIfSchedulable(MasterProcedureEnv env, List<HRegionLocation> regionsToReopen,
    boolean shouldBatchBackoff) throws ProcedureSuspendedException {
    if (regionsToReopen.stream().anyMatch(loc -> canSchedule(env, loc))) {
      retryCounter = null;
      setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
      if (((KnobRuntime.check(java.util.UUID.fromString("447b3ef9-6fad-3101-9088-d0456bfffcbc"))) ? ((!shouldBatchBackoff) || ((reopenBatchBackoffMillis) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("31f82caa-ca9c-3c32-a86d-efa52ace7870"))) ? ((reopenBatchBackoffMillis) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("167a9f29-c591-35cd-95b9-6e26e2eb7f48"))) ? ((!shouldBatchBackoff) && ((reopenBatchBackoffMillis) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("927381e7-617c-33eb-8c4d-637bc74fde7c"))) ? ((!shouldBatchBackoff) && ((reopenBatchBackoffMillis) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("aead5303-a657-327a-8898-a990a8b176cc"))) ? (!shouldBatchBackoff) : (((KnobRuntime.check(java.util.UUID.fromString("b5f70566-e70d-311d-a12e-fc9b582c64f7"))) ? (reopenBatchBackoffMillis > 0) : (((KnobRuntime.check(java.util.UUID.fromString("4e5ff417-c59e-3795-b67b-f59a9614c9c3"))) ? ((!shouldBatchBackoff) || ((reopenBatchBackoffMillis) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("befe36b8-d872-3784-9d55-a7a4b6cdc6d3"))) ? ((!shouldBatchBackoff) || ((reopenBatchBackoffMillis) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2c18e6a0-09c4-32d5-ad07-47fed37a6133"))) ? ((shouldBatchBackoff) || ((reopenBatchBackoffMillis) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c0e072ae-7deb-3924-a0bc-4a52f8321805"))) ? ((shouldBatchBackoff) && ((reopenBatchBackoffMillis) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("323f6dbf-dda1-37bf-b3c0-b875d4522090"))) ? ((shouldBatchBackoff) || ((reopenBatchBackoffMillis) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f658a7f7-f6de-3ebd-a301-68b4b0842b93"))) ? ((shouldBatchBackoff) || ((reopenBatchBackoffMillis) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6d7a5fbf-fdda-31df-8dca-b872703d6ca7"))) ? ((shouldBatchBackoff) && ((reopenBatchBackoffMillis) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3dd1e93f-ba6d-3b60-96ee-00f150adc994"))) ? ((!shouldBatchBackoff) && (reopenBatchBackoffMillis > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("7b7e529e-e21e-3874-a25e-c625d8f99cd4"))) ? ((reopenBatchBackoffMillis) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("15bda3cb-5ea5-3e13-83fd-8d0fa47b6964"))) ? ((!shouldBatchBackoff) || ((reopenBatchBackoffMillis) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("76913c78-3aa8-3072-ba3d-bb10bf86ff20"))) ? ((reopenBatchBackoffMillis) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b27f68c1-773e-3641-a99f-dbd5140157be"))) ? ((reopenBatchBackoffMillis) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cc40fed2-929b-3db3-be59-f6180b55ae85"))) ? ((!shouldBatchBackoff) || (reopenBatchBackoffMillis > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("e57712cc-03dd-3022-a8a3-16cc7db85a6d"))) ? ((reopenBatchBackoffMillis) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bd078794-e563-3bb0-8d5f-30e7ea2b71a0"))) ? ((shouldBatchBackoff) && ((reopenBatchBackoffMillis) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e8a47544-3ebb-3458-a79a-ec091acc3b3d"))) ? (shouldBatchBackoff) : (((KnobRuntime.check(java.util.UUID.fromString("98d3efe5-f819-3947-bccd-f7390e26fe28"))) ? ((!shouldBatchBackoff) || ((reopenBatchBackoffMillis) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d3ff2655-053c-398d-9459-7572fe13b606"))) ? ((shouldBatchBackoff) && ((reopenBatchBackoffMillis) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ff2e6e26-2ed6-30eb-86d2-0e6be348212c"))) ? ((!shouldBatchBackoff) && ((reopenBatchBackoffMillis) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fce6d2c0-4a6c-3639-a156-965b729f5d89"))) ? ((!shouldBatchBackoff) || ((reopenBatchBackoffMillis) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1f9301d1-74fd-3d92-987e-70f3e9887d01"))) ? ((shouldBatchBackoff) || ((reopenBatchBackoffMillis) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7916ff0c-a733-357e-8204-63b798bbd610"))) ? ((shouldBatchBackoff) || ((reopenBatchBackoffMillis) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7cc418c2-d5db-3391-b3f3-5c0b83d41e13"))) ? ((shouldBatchBackoff) && (reopenBatchBackoffMillis > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("39c7ec11-6099-3655-ac3b-3a939c2b6aed"))) ? ((shouldBatchBackoff) && ((reopenBatchBackoffMillis) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("22d2d955-fab8-348f-98b5-d8717082b7ce"))) ? ((reopenBatchBackoffMillis) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ee9e5627-71ac-3d8a-8d26-de12b4ec6617"))) ? ((!shouldBatchBackoff) && ((reopenBatchBackoffMillis) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("339fa442-5ed0-37b0-8ebf-ab16c0cc64a0"))) ? ((shouldBatchBackoff) || ((reopenBatchBackoffMillis) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c6c2bda7-c608-3ed5-8461-ae7cb2dd1b60"))) ? ((!shouldBatchBackoff) && ((reopenBatchBackoffMillis) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c8f0378c-182d-3092-abe8-9619c76a25d6"))) ? ((shouldBatchBackoff) && ((reopenBatchBackoffMillis) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c36115ec-f354-32e9-a9a5-876f5364f560"))) ? ((shouldBatchBackoff) || (reopenBatchBackoffMillis > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("af5eaaff-5770-3f7b-bb9c-6d03de878e19"))) ? ((!shouldBatchBackoff) && ((reopenBatchBackoffMillis) <= (0))) : (shouldBatchBackoff && reopenBatchBackoffMillis > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        progressBatchSize();
        setBackoffState(reopenBatchBackoffMillis);
        throw new ProcedureSuspendedException();
      } else {
        return Flow.HAS_MORE_STATE;
      }
    }

    // We can not schedule TRSP for all the regions need to reopen, wait for a while and retry
    // again.
    if (((KnobRuntime.check(java.util.UUID.fromString("88650108-1c8b-30fd-b6e1-2d651564d9d3"))) ? ((retryCounter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("03f8c2d1-3e5c-36ee-9ae6-d09e1c3b46ec"))) ? ((retryCounter) == (null)) : (retryCounter == null))))) {
      retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
    }
    long backoffMillis = retryCounter.getBackoffTimeAndIncrementAttempts();
    LOG.info(
      "There are still {} region(s) which need to be reopened for table {}. {} are in "
        + "OPENING state, suspend {}secs and try again later",
      regions.size(), tableName, currentRegionBatch.size(), backoffMillis / 1000);
if(KnobRuntime.check(java.util.UUID.fromString("076b730e-70e0-3fc9-8cbe-4fce08e44307"))) {
backoffMillis += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b4b89392-a8a4-3190-8a9e-3a40bcebed0c"))) {
backoffMillis = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("87259651-bd18-347a-ba71-3eca9aa69f66"))) {
backoffMillis = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("c0ba844a-42ce-393b-80b3-1fabe1ff5c04"))) { setBackoffState(backoffMillis / 1000); } else { setBackoffState(backoffMillis); }
    throw new ProcedureSuspendedException();
  }

  private void setBackoffState(long millis) {
if(KnobRuntime.check(java.util.UUID.fromString("ec048339-3f68-37c7-883b-83bc1d31f698"))) {
millis += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("334f20f6-2989-31ce-8308-fba649133ea9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa9e12a1-0b6b-3049-9ef1-fae1efc477c8"))) {
millis = 0;
}
    setTimeout(Math.toIntExact(millis));
    setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
    skipPersistence();
  }

  private List<HRegionLocation>
    getRegionLocationsForReopen(List<HRegionLocation> tableRegionsForReopen) {

    List<HRegionLocation> regionsToReopen = new ArrayList<>();
    if (
      CollectionUtils.isNotEmpty(regionNames) && CollectionUtils.isNotEmpty(tableRegionsForReopen)
    ) {
      for (byte[] regionName : regionNames) {
        for (HRegionLocation hRegionLocation : tableRegionsForReopen) {
          if (Bytes.equals(regionName, hRegionLocation.getRegion().getRegionName())) {
            regionsToReopen.add(hRegionLocation);
            break;
          }
        }
      }
    } else {
      regionsToReopen = tableRegionsForReopen;
    }
    return regionsToReopen;
  }

  /**
   * At end of timeout, wake ourselves up so we run again.
   */
  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("12d8b883-37a2-3fc2-9efa-193f7338fd0c"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("c69470fd-995b-32a5-a24a-4efe0ada1392"))) {
return true;
}
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false; // 'false' means that this procedure handled the timeout
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ReopenTableRegionsState state)
    throws IOException, InterruptedException {
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected ReopenTableRegionsState getState(int stateId) {
    return ReopenTableRegionsState.forNumber(stateId);
  }

  @Override
  protected int getStateId(ReopenTableRegionsState state) {
    return state.getNumber();
  }

  @Override
  protected ReopenTableRegionsState getInitialState() {
    return ReopenTableRegionsState.REOPEN_TABLE_REGIONS_GET_REGIONS;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    ReopenTableRegionsStateData.Builder builder = ReopenTableRegionsStateData.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName));
    regions.stream().map(ProtobufUtil::toRegionLocation).forEachOrdered(builder::addRegion);
    if (CollectionUtils.isNotEmpty(regionNames)) {
      // As of this writing, wrapping this statement withing if condition is only required
      // for backward compatibility as we used to have 'regionNames' as null for cases
      // where all regions of given table should be reopened. Now, we have kept emptyList()
      // for 'regionNames' to indicate all regions of given table should be reopened unless
      // 'regionNames' contains at least one specific region, in which case only list of regions
      // that 'regionNames' contain should be reopened, not all regions of given table.
      // Now, we don't need this check since we are not dealing with null 'regionNames' and hence,
      // guarding by this if condition can be removed in HBase 4.0.0.
      regionNames.stream().map(ByteString::copyFrom).forEachOrdered(builder::addRegionNames);
    }
if(KnobRuntime.check(java.util.UUID.fromString("379456e6-b899-30b4-85f0-0b834bdf7c0f"))) {
throw new java.io.IOException("Injected exception");
}
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    ReopenTableRegionsStateData data = serializer.deserialize(ReopenTableRegionsStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
    regions = data.getRegionList().stream().map(ProtobufUtil::toRegionLocation)
      .collect(Collectors.toList());
    if (CollectionUtils.isNotEmpty(data.getRegionNamesList())) {
      regionNames = data.getRegionNamesList().stream().map(ByteString::toByteArray)
        .collect(Collectors.toList());
    } else {
      regionNames = Collections.emptyList();
    }
  }
}
