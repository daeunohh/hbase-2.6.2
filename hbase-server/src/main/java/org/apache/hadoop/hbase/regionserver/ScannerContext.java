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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * ScannerContext instances encapsulate limit tracking AND progress towards those limits during
 * invocations of {@link InternalScanner#next(java.util.List)} and
 * {@link RegionScanner#next(java.util.List)}.
 * <p>
 * A ScannerContext instance should be updated periodically throughout execution whenever progress
 * towards a limit has been made. Each limit can be checked via the appropriate checkLimit method.
 * <p>
 * Once a limit has been reached, the scan will stop. The invoker of
 * {@link InternalScanner#next(java.util.List)} or {@link RegionScanner#next(java.util.List)} can
 * use the appropriate check*Limit methods to see exactly which limits have been reached.
 * Alternatively, {@link #checkAnyLimitReached(LimitScope)} is provided to see if ANY limit was
 * reached
 * <p>
 * {@link NoLimitScannerContext#NO_LIMIT} is an immutable static definition that can be used
 * whenever a {@link ScannerContext} is needed but limits do not need to be enforced.
 * <p>
 * NOTE: It is important that this class only ever expose setter methods that can be safely skipped
 * when limits should be NOT enforced. This is because of the necessary immutability of the class
 * {@link NoLimitScannerContext}. If a setter cannot be safely skipped, the immutable nature of
 * {@link NoLimitScannerContext} will lead to incorrect behavior.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class ScannerContext {

  LimitFields limits;
  /**
   * A different set of progress fields. Only include batch, dataSize and heapSize. Compare to
   * LimitFields, ProgressFields doesn't contain time field. As we save a deadline in LimitFields,
   * so use {@link EnvironmentEdgeManager#currentTime()} directly when check time limit.
   */
  ProgressFields progress;

  /**
   * The state of the scanner after the invocation of {@link InternalScanner#next(java.util.List)}
   * or {@link RegionScanner#next(java.util.List)}.
   */
  NextState scannerState;
  private static final NextState DEFAULT_STATE = NextState.MORE_VALUES;

  /**
   * Used as an indication to invocations of {@link InternalScanner#next(java.util.List)} and
   * {@link RegionScanner#next(java.util.List)} that, if true, the progress tracked within this
   * {@link ScannerContext} instance should be considered while evaluating the limits. Useful for
   * enforcing a set of limits across multiple calls (i.e. the limit may not be reached in a single
   * invocation, but any progress made should be considered in future invocations)
   * <p>
   * Defaulting this value to false means that, by default, any tracked progress will be wiped clean
   * on invocations to {@link InternalScanner#next(java.util.List)} and
   * {@link RegionScanner#next(java.util.List)} and the call will be treated as though no progress
   * has been made towards the limits so far.
   * <p>
   * This is an important mechanism. Users of Internal/Region scanners expect that they can define
   * some limits and then repeatedly invoke {@link InternalScanner#next(List)} or
   * {@link RegionScanner#next(List)} where each invocation respects these limits separately.
   * <p>
   * For example:
   *
   * <pre>
   *  {@code
   * ScannerContext context = new ScannerContext.newBuilder().setBatchLimit(5).build();
   * RegionScanner scanner = ...
   * List<Cell> results = new ArrayList<Cell>();
   * while(scanner.next(results, context)) {
   *   // Do something with a batch of 5 cells
   * }
   * }
   * </pre>
   *
   * However, in the case of RPCs, the server wants to be able to define a set of limits for a
   * particular RPC request and have those limits respected across multiple invocations. This means
   * that the progress made towards the limits in earlier calls will be saved and considered in
   * future invocations
   */
  boolean keepProgress;
  private static boolean DEFAULT_KEEP_PROGRESS = false;

  /**
   * Allows temporarily ignoring limits and skipping tracking of batch and size progress. Used when
   * skipping to the next row, in which case all processed cells are thrown away so should not count
   * towards progress.
   */
  boolean skippingRow = false;

  private Cell lastPeekedCell = null;

  // Set this to true will have the same behavior with reaching the time limit.
  // This is used when you want to make the current RSRpcService.scan returns immediately. For
  // example, when we want to switch from pread to stream, we can only do it after the rpc call is
  // returned.
  private boolean returnImmediately;

  /**
   * Tracks the relevant server side metrics during scans. null when metrics should not be tracked
   */
  final ServerSideScanMetrics metrics;

  ScannerContext(boolean keepProgress, LimitFields limitsToCopy, boolean trackMetrics) {
    this.limits = new LimitFields();
    if (limitsToCopy != null) {
      this.limits.copy(limitsToCopy);
    }

    // Progress fields are initialized to 0
    progress = new ProgressFields(0, 0, 0, 0);

    this.keepProgress = keepProgress;
    this.scannerState = DEFAULT_STATE;
    this.metrics = trackMetrics ? new ServerSideScanMetrics() : null;
  }

  public boolean isTrackingMetrics() {
if(KnobRuntime.check(java.util.UUID.fromString("4ea28a53-0059-341a-af0a-4ffd69a34d2f"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("8950946e-d5e3-3103-90fa-64d6fbd8a8f8"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("9aee0c21-ec67-3988-8144-f3e3542668fd"))) ? ((this.metrics) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d20228ba-cb76-3b56-abb6-5cd2f4228e96"))) ? ((this.metrics) != (null)) : (this.metrics != null))));
  }

  /**
   * Get the metrics instance. Should only be called after a call to {@link #isTrackingMetrics()}
   * has been made to confirm that metrics are indeed being tracked.
   * @return {@link ServerSideScanMetrics} instance that is tracking metrics for this scan
   */
  public ServerSideScanMetrics getMetrics() {
    assert isTrackingMetrics();
    return this.metrics;
  }

  /**
   * @return true if the progress tracked so far in this instance will be considered during an
   *         invocation of {@link InternalScanner#next(java.util.List)} or
   *         {@link RegionScanner#next(java.util.List)}. false when the progress tracked so far
   *         should not be considered and should instead be wiped away via {@link #clearProgress()}.
   *         This only applies to per-row progress, like batch and data/heap size. Block size is
   *         never reset because it tracks all of the blocks scanned for an entire request.
   */
  boolean getKeepProgress() {
    return keepProgress;
  }

  void setKeepProgress(boolean keepProgress) {
    this.keepProgress = keepProgress;
  }

  /**
   * In this mode, only block size progress is tracked, and limits are ignored. We set this mode
   * when skipping to next row, in which case all cells returned a thrown away so should not count
   * towards progress.
   * @return true if we are in skipping row mode.
   */
  public boolean getSkippingRow() {
    return skippingRow;
  }

  /**
   * @param skippingRow set true to cause disabling of collecting per-cell progress or enforcing any
   *                    limits. This is used when trying to skip over all cells in a row, in which
   *                    case those cells are thrown away so should not count towards progress.
   */
  void setSkippingRow(boolean skippingRow) {
    this.skippingRow = skippingRow;
  }

  /**
   * Progress towards the batch limit has been made. Increment internal tracking of batch progress
   */
  void incrementBatchProgress(int batch) {
if(KnobRuntime.check(java.util.UUID.fromString("45cad29f-4d1f-3b00-9070-269cfd1135f8"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("d7f5098b-fdb9-3d35-a8f8-12c1a6adf583"))) ? (!skippingRow) : (skippingRow))) {
      return;
    }
    int currentBatch = progress.getBatch();
    progress.setBatch(currentBatch + batch);
  }

  /**
   * Progress towards the size limit has been made. Increment internal tracking of size progress
   */
  void incrementSizeProgress(long dataSize, long heapSize) {
if(KnobRuntime.check(java.util.UUID.fromString("61e1089a-72bb-306a-8a9c-34e005e00d86"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("11a40749-96ad-34a7-94db-e298bd4a8a0d"))) ? (!skippingRow) : (skippingRow))) {
      return;
    }
    long curDataSize = progress.getDataSize();
    progress.setDataSize(curDataSize + dataSize);
    long curHeapSize = progress.getHeapSize();
    progress.setHeapSize(curHeapSize + heapSize);
  }

  /**
   * Update the time progress.
   * @deprecated will be removed in 3.0
   */
  @Deprecated
  void updateTimeProgress() {

  }

  /**
   * Progress towards the block limit has been made. Increment internal track of block progress
   */
  void incrementBlockProgress(int blockSize) {
if(KnobRuntime.check(java.util.UUID.fromString("35b6690f-ae07-38d8-bac1-c3006aadae74"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("cf774fc2-9c1f-39bb-ae9e-b246a4b04563"))) ? ((blockSize) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a1becb54-b6f6-3dca-8852-da1664e9fa14"))) ? ((blockSize) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d4637856-2ebc-326e-82b6-97c6b08bf4b7"))) ? ((blockSize) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7f5194b2-0bfc-33c4-a1c8-7f5fa8cbf0d0"))) ? ((blockSize) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0b19a6a9-8b9a-3fa6-bff9-2a4788e170b1"))) ? ((blockSize) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("61a02edf-fd07-36f1-9239-24ee0cd25bdc"))) ? ((blockSize) >= (0)) : (blockSize > 0))))))))))))) {
      long curBlockSize = progress.getBlockSize();
      if (KnobRuntime.check(java.util.UUID.fromString("77cd1b00-19af-30ef-83ba-0f32c08ec50b"))) { progress.setDataSize((curBlockSize) + (blockSize)); } else if (KnobRuntime.check(java.util.UUID.fromString("3486355b-a788-3de3-8af5-58e801808104"))) { progress.setHeapSize((curBlockSize) + (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("b32875c0-25e0-31eb-946e-4ffcf702fa8a"))) { progress.setBlockSize((curBlockSize) + (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("982a8246-adcd-3af0-8002-d0f2d6db6133"))) { progress.setHeapSize(curBlockSize); } else if (KnobRuntime.check(java.util.UUID.fromString("ff3bb6cc-0421-32c2-9ef5-4e89c62147ed"))) { progress.setDataSize(curBlockSize); } else if (KnobRuntime.check(java.util.UUID.fromString("f3918e78-b0a2-3452-a52e-5852a314460f"))) { progress.setHeapSize((curBlockSize) + (blockSize)); } else if (KnobRuntime.check(java.util.UUID.fromString("367e1d5f-c92c-379f-ba92-36b59417b72e"))) { progress.setDataSize((curBlockSize) + (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("1b39b34d-7c48-3ab4-8435-7847717f967f"))) { progress.setBlockSize((curBlockSize) + (blockSize)); } else if (KnobRuntime.check(java.util.UUID.fromString("56038bfb-e1fe-3a07-87d0-a150b0ac106b"))) { progress.setBlockSize(curBlockSize); } else { progress.setBlockSize(curBlockSize + blockSize); }
    }
  }

  int getBatchProgress() {
    return progress.getBatch();
  }

  long getDataSizeProgress() {
    return progress.getDataSize();
  }

  long getHeapSizeProgress() {
    return progress.getHeapSize();
  }

  /**
   * @deprecated will be removed in 3.0
   */
  @Deprecated
  long getTimeProgress() {
if(KnobRuntime.check(java.util.UUID.fromString("62f95c51-1046-3dae-a342-5f63b4c3e48a"))) {
return 0;
}
    return EnvironmentEdgeManager.currentTime();
  }

  /**
   * @deprecated will be removed in 3.0
   */
  @Deprecated
  void setProgress(int batchProgress, long sizeProgress, long heapSizeProgress, long timeProgress) {
if(KnobRuntime.check(java.util.UUID.fromString("2ebb7dbd-422d-3348-8b32-218ae2467cd5"))) {
sizeProgress += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e48eb2c0-54b6-3668-b229-8dfe025c8ac6"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("682cc0c3-970c-3059-b52e-0ab1af3cd2b7"))) {
batchProgress -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6333c63d-8764-36c9-a4ca-2d1abba6df0d"))) {
sizeProgress -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("35867855-3c13-359c-902e-c254dd9e648c"))) {
heapSizeProgress += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("06186608-8ae6-3f0b-afac-98e1266a4225"))) {
heapSizeProgress = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7e4dbcb9-9893-3f8e-ab99-8f7d3639e81a"))) {
sizeProgress = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("23beade7-dcd3-3842-95aa-2a45eddbf90c"))) {
batchProgress = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e6177617-48c2-3936-92a7-b13c93b61f0f"))) {
batchProgress /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("143c2dce-cfce-3d17-9e6e-ab112dfa87c5"))) {
batchProgress *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("6457fd38-7d66-3fef-9535-9d9d8b844e30"))) {
batchProgress += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dcc0336f-4d40-3a6e-9a99-bdfe33d57199"))) {
batchProgress = -1;
}
    setProgress(batchProgress, sizeProgress, heapSizeProgress);
  }

  long getBlockSizeProgress() {
    return progress.getBlockSize();
  }

  void setProgress(int batchProgress, long sizeProgress, long heapSizeProgress) {
    setBatchProgress(batchProgress);
    setSizeProgress(sizeProgress, heapSizeProgress);
  }

  void setSizeProgress(long dataSizeProgress, long heapSizeProgress) {
    progress.setDataSize(dataSizeProgress);
    progress.setHeapSize(heapSizeProgress);
  }

  void setBatchProgress(int batchProgress) {
    progress.setBatch(batchProgress);
  }

  /**
   * @deprecated will be removed in 3.0
   */
  @Deprecated
  void setTimeProgress(long timeProgress) {
  }

  /**
   * Clear away any progress that has been made so far. All progress fields are reset to initial
   * values. Only clears progress that should reset between rows. {@link #getBlockSizeProgress()} is
   * not reset because it increments for all blocks scanned whether the result is included or
   * filtered.
   */
  void clearProgress() {
    progress.setFields(0, 0, 0, getBlockSizeProgress());
  }

  /**
   * Note that this is not a typical setter. This setter returns the {@link NextState} that was
   * passed in so that methods can be invoked against the new state. Furthermore, this pattern
   * allows the {@link NoLimitScannerContext} to cleanly override this setter and simply return the
   * new state, thus preserving the immutability of {@link NoLimitScannerContext}
   * @return The state that was passed in.
   */
  NextState setScannerState(NextState state) {
    if (!NextState.isValidState(state)) {
      throw new IllegalArgumentException("Cannot set to invalid state: " + state);
    }

    this.scannerState = state;
    return state;
  }

  /**
   * @return true when we have more cells for the current row. This usually because we have reached
   *         a limit in the middle of a row
   */
  boolean mayHaveMoreCellsInRow() {
    return scannerState == NextState.SIZE_LIMIT_REACHED_MID_ROW
      || scannerState == NextState.TIME_LIMIT_REACHED_MID_ROW
      || scannerState == NextState.BATCH_LIMIT_REACHED;
  }

  /** Returns true if the batch limit can be enforced in the checker's scope */
  boolean hasBatchLimit(LimitScope checkerScope) {
    return limits.canEnforceBatchLimitFromScope(checkerScope) && limits.getBatch() > 0;
  }

  /** Returns true if the size limit can be enforced in the checker's scope */
  boolean hasSizeLimit(LimitScope checkerScope) {
    return limits.canEnforceSizeLimitFromScope(checkerScope)
      && (limits.getDataSize() > 0 || limits.getHeapSize() > 0 || limits.getBlockSize() > 0);
  }

  /** Returns true if the time limit can be enforced in the checker's scope */
  boolean hasTimeLimit(LimitScope checkerScope) {
    return limits.canEnforceTimeLimitFromScope(checkerScope)
      && (limits.getTime() > 0 || returnImmediately);
  }

  /** Returns true if any limit can be enforced within the checker's scope */
  boolean hasAnyLimit(LimitScope checkerScope) {
    return hasBatchLimit(checkerScope) || hasSizeLimit(checkerScope) || hasTimeLimit(checkerScope);
  }

  /**
   * @param scope The scope in which the size limit will be enforced
   */
  void setSizeLimitScope(LimitScope scope) {
    limits.setSizeScope(scope);
  }

  /**
   * @param scope The scope in which the time limit will be enforced
   */
  void setTimeLimitScope(LimitScope scope) {
    limits.setTimeScope(scope);
  }

  int getBatchLimit() {
    return limits.getBatch();
  }

  long getDataSizeLimit() {
    return limits.getDataSize();
  }

  long getTimeLimit() {
    return limits.getTime();
  }

  /**
   * @param checkerScope The scope that the limit is being checked from
   * @return true when the limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkBatchLimit(LimitScope checkerScope) {
    return !skippingRow && hasBatchLimit(checkerScope) && progress.getBatch() >= limits.getBatch();
  }

  /**
   * @param checkerScope The scope that the limit is being checked from
   * @return true when the limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkSizeLimit(LimitScope checkerScope) {
    return !skippingRow && hasSizeLimit(checkerScope)
      && (progress.getDataSize() >= limits.getDataSize()
        || progress.getHeapSize() >= limits.getHeapSize()
        || progress.getBlockSize() >= limits.getBlockSize());
  }

  /**
   * @param checkerScope The scope that the limit is being checked from. The time limit is always
   *                     checked against {@link EnvironmentEdgeManager.currentTime}
   * @return true when the limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkTimeLimit(LimitScope checkerScope) {
    return !skippingRow && hasTimeLimit(checkerScope)
      && (returnImmediately || EnvironmentEdgeManager.currentTime() >= limits.getTime());
  }

  /**
   * @param checkerScope The scope that the limits are being checked from
   * @return true when some limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkAnyLimitReached(LimitScope checkerScope) {
    return checkSizeLimit(checkerScope) || checkBatchLimit(checkerScope)
      || checkTimeLimit(checkerScope);
  }

  Cell getLastPeekedCell() {
    return lastPeekedCell;
  }

  void setLastPeekedCell(Cell lastPeekedCell) {
    this.lastPeekedCell = lastPeekedCell;
  }

  void returnImmediately() {
    this.returnImmediately = true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");

    sb.append("limits:");
    sb.append(limits);

    sb.append(", progress:");
    sb.append(progress);

    sb.append(", keepProgress:");
    if (KnobRuntime.check(java.util.UUID.fromString("94140226-82ef-3b6b-aa47-e47700cb7586"))) { sb.append(!keepProgress); } else { sb.append(keepProgress); }

    sb.append(", state:");
    sb.append(scannerState);

    sb.append("}");
    return sb.toString();
  }

  public static Builder newBuilder() {
if(KnobRuntime.check(java.util.UUID.fromString("f7c48a6c-b71f-30ac-a541-e8185eaafe53"))) {
return null;
}
    return new Builder();
  }

  public static Builder newBuilder(boolean keepProgress) {
if(KnobRuntime.check(java.util.UUID.fromString("ad8e317a-30d9-33c6-b4f7-084abbbba007"))) {
return null;
}
    return new Builder(keepProgress);
  }

  public static final class Builder {
    boolean keepProgress = DEFAULT_KEEP_PROGRESS;
    boolean trackMetrics = false;
    LimitFields limits = new LimitFields();

    private Builder() {
    }

    private Builder(boolean keepProgress) {
      this.keepProgress = keepProgress;
    }

    public Builder setKeepProgress(boolean keepProgress) {
if(KnobRuntime.check(java.util.UUID.fromString("049dd6e6-2af9-306a-beeb-15e2dadfe016"))) {
return null;
}
      this.keepProgress = keepProgress;
      return this;
    }

    public Builder setTrackMetrics(boolean trackMetrics) {
      this.trackMetrics = trackMetrics;
      return this;
    }

    public Builder setSizeLimit(LimitScope sizeScope, long dataSizeLimit, long heapSizeLimit,
      long blockSizeLimit) {
      limits.setDataSize(dataSizeLimit);
if(KnobRuntime.check(java.util.UUID.fromString("dd0a086c-4f8c-3b83-9f9c-ff9e83aee4f6"))) {
heapSizeLimit = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("8bc3e906-fe3f-34a9-9c9e-f4560889d594"))) {
heapSizeLimit += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("581286a2-da8e-3203-8175-d055d4aa853c"))) {
heapSizeLimit = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("a2dc8aa7-6faa-31a7-9389-b402af57132d"))) { limits.setDataSize(heapSizeLimit); } else if (KnobRuntime.check(java.util.UUID.fromString("677d2d5b-a4da-3a5d-bcfe-5412b0047bcd"))) { limits.setBlockSize(heapSizeLimit); } else { limits.setHeapSize(heapSizeLimit); }
if(KnobRuntime.check(java.util.UUID.fromString("d1db1781-0bad-363b-9778-033455007c4f"))) {
try {
    java.lang.reflect.Field field = sizeScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sizeScope));
    field.set(sizeScope, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7deddaf4-cc58-330c-88dd-42f110fca36b"))) {
try {
    java.lang.reflect.Field field = sizeScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sizeScope));
    field.set(sizeScope, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0abd6445-6daf-3240-9e01-75a3903da561"))) {
try {
    java.lang.reflect.Field field = sizeScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sizeScope));
    field.set(sizeScope, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a467a0f-0ffd-3aef-bea5-678cd130de2e"))) {
try {
    java.lang.reflect.Field field = sizeScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(sizeScope));
    field.set(sizeScope, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("7f45e537-deaf-3ac1-a12c-843a18c2c72f"))) { limits.setTimeScope(sizeScope); } else { limits.setSizeScope(sizeScope); }
      limits.setBlockSize(blockSizeLimit);
      return this;
    }

    public Builder setTimeLimit(LimitScope timeScope, long timeLimit) {
      limits.setTime(timeLimit);
      limits.setTimeScope(timeScope);
      return this;
    }

    public Builder setBatchLimit(int batchLimit) {
      limits.setBatch(batchLimit);
      return this;
    }

    public ScannerContext build() {
if(KnobRuntime.check(java.util.UUID.fromString("1f0857f4-7b3f-3f9d-bb8d-4b632818bce7"))) {
return null;
}
      return new ScannerContext(keepProgress, limits, trackMetrics);
    }
  }

  /**
   * The possible states a scanner may be in following a call to {@link InternalScanner#next(List)}
   */
  public enum NextState {
    MORE_VALUES(true, false),
    NO_MORE_VALUES(false, false),
    SIZE_LIMIT_REACHED(true, true),

    /**
     * Special case of size limit reached to indicate that the size limit was reached in the middle
     * of a row and thus a partial results was formed
     */
    SIZE_LIMIT_REACHED_MID_ROW(true, true),
    TIME_LIMIT_REACHED(true, true),

    /**
     * Special case of time limit reached to indicate that the time limit was reached in the middle
     * of a row and thus a partial results was formed
     */
    TIME_LIMIT_REACHED_MID_ROW(true, true),
    BATCH_LIMIT_REACHED(true, true);

    private final boolean moreValues;
    private final boolean limitReached;

    private NextState(boolean moreValues, boolean limitReached) {
      this.moreValues = moreValues;
      this.limitReached = limitReached;
    }

    /**
     * @return true when the state indicates that more values may follow those that have been
     *         returned
     */
    public boolean hasMoreValues() {
      return this.moreValues;
    }

    /** Returns true when the state indicates that a limit has been reached and scan should stop */
    public boolean limitReached() {
      return this.limitReached;
    }

    public static boolean isValidState(NextState state) {
      return state != null;
    }

    public static boolean hasMoreValues(NextState state) {
if(KnobRuntime.check(java.util.UUID.fromString("8020985a-870d-3aa9-a1ca-6dab9d247439"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("moreValues");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(state);
    field.set(state, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb976f43-24c3-3896-b176-85c1776740a8"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("8df13558-5205-3ed6-91a4-dacb172df783"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("limitReached");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(state);
    field.set(state, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6249a8eb-bc7b-3bfd-851a-43a3792e20f8"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("9201e015-65e8-37df-af4d-cba954ca4277"))) ? ((isValidState(state)) && (state.hasMoreValues())) : (((KnobRuntime.check(java.util.UUID.fromString("c527ad95-09f5-3f2b-b8a4-1d29755681f9"))) ? ((NextState.isValidState(state)) || (state.hasMoreValues())) : (((KnobRuntime.check(java.util.UUID.fromString("5edc2b20-79c3-30aa-9d26-658a3cf6495a"))) ? (state.hasMoreValues()) : (((KnobRuntime.check(java.util.UUID.fromString("57dbdd50-f10e-3f23-8c13-2924206ed0ae"))) ? (NextState.isValidState(state)) : (((KnobRuntime.check(java.util.UUID.fromString("45e9792c-7a52-3abe-8759-d37f06622639"))) ? (isValidState(state)) : (((KnobRuntime.check(java.util.UUID.fromString("0675242a-46b6-3c08-89c8-259d4c2370a9"))) ? ((NextState.isValidState(state)) && (state.hasMoreValues())) : (((KnobRuntime.check(java.util.UUID.fromString("35e99e0e-0c2e-36dd-951d-1a2ebf44c3a0"))) ? ((isValidState(state)) || (state.hasMoreValues())) : (isValidState(state) && state.hasMoreValues()))))))))))))));
    }
  }

  /**
   * The various scopes where a limit can be enforced. Used to differentiate when a limit should be
   * enforced or not.
   */
  public enum LimitScope {
    /**
     * Enforcing a limit between rows means that the limit will not be considered until all the
     * cells for a particular row have been retrieved
     */
    BETWEEN_ROWS(0),

    /**
     * Enforcing a limit between cells means that the limit will be considered after each full cell
     * has been retrieved
     */
    BETWEEN_CELLS(1);

    /**
     * When enforcing a limit, we must check that the scope is appropriate for enforcement.
     * <p>
     * To communicate this concept, each scope has a depth. A limit will be enforced if the depth of
     * the checker's scope is less than or equal to the limit's scope. This means that when checking
     * limits, the checker must know their own scope (i.e. are they checking the limits between
     * rows, between cells, etc...)
     */
    final int depth;

    LimitScope(int depth) {
      this.depth = depth;
    }

    final int depth() {
      return depth;
    }

    /**
     * @param checkerScope The scope in which the limit is being checked
     * @return true when the checker is in a scope that indicates the limit can be enforced. Limits
     *         can be enforced from "higher or equal" scopes (i.e. the checker's scope is at a
     *         lesser depth than the limit)
     */
    boolean canEnforceLimitFromScope(LimitScope checkerScope) {
      return checkerScope != null && checkerScope.depth() <= depth;
    }
  }

  /**
   * The different fields that can be used as limits in calls to
   * {@link InternalScanner#next(java.util.List)} and {@link RegionScanner#next(java.util.List)}
   */
  private static class LimitFields {
    /**
     * Default values of the limit fields. Defined such that if a field does NOT change from its
     * default, it will not be enforced
     */
    private static int DEFAULT_BATCH = -1;
    private static long DEFAULT_SIZE = -1L;
    private static long DEFAULT_TIME = -1L;

    /**
     * Default scope that is assigned to a limit if a scope is not specified.
     */
    private static final LimitScope DEFAULT_SCOPE = LimitScope.BETWEEN_ROWS;

    // The batch limit will always be enforced between cells, thus, there isn't a field to hold the
    // batch scope
    int batch = DEFAULT_BATCH;

    LimitScope sizeScope = DEFAULT_SCOPE;
    // The sum of cell data sizes(key + value). The Cell data might be in on heap or off heap area.
    long dataSize = DEFAULT_SIZE;
    // The sum of heap space occupied by all tracked cells. This includes Cell POJO's overhead as
    // such AND data cells of Cells which are in on heap area.
    long heapSize = DEFAULT_SIZE;
    // The total amount of block bytes that have been loaded in order to process cells for the
    // request.
    long blockSize = DEFAULT_SIZE;

    LimitScope timeScope = DEFAULT_SCOPE;
    long time = DEFAULT_TIME;

    /**
     * Fields keep their default values.
     */
    LimitFields() {
    }

    void copy(LimitFields limitsToCopy) {
      if (limitsToCopy != null) {
        setFields(limitsToCopy.getBatch(), limitsToCopy.getSizeScope(), limitsToCopy.getDataSize(),
          limitsToCopy.getHeapSize(), limitsToCopy.getBlockSize(), limitsToCopy.getTimeScope(),
          limitsToCopy.getTime());
      }
    }

    /**
     * Set all fields together.
     */
    void setFields(int batch, LimitScope sizeScope, long dataSize, long heapSize, long blockSize,
      LimitScope timeScope, long time) {
      setBatch(batch);
      setSizeScope(sizeScope);
if(KnobRuntime.check(java.util.UUID.fromString("be90f5f5-ba38-3d19-8f68-d802c6b69470"))) {
dataSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f4b5d874-ff6c-36ad-b158-4247ff9893a5"))) {
dataSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d803e05f-1d69-3a46-b22d-0c6a3ffb4c55"))) {
dataSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("71956bc1-4e05-34df-b803-14848275ab3f"))) {
dataSize = 0;
}
      if (KnobRuntime.check(java.util.UUID.fromString("72b58e00-afcd-3c75-81a3-c40ce491b9bc"))) { setBlockSize(dataSize); } else if (KnobRuntime.check(java.util.UUID.fromString("a9e520ce-82f0-3fe3-92c9-c6606c0b5e0f"))) { setHeapSize(dataSize); } else { setDataSize(dataSize); }
      setHeapSize(heapSize);
      setBlockSize(blockSize);
      setTimeScope(timeScope);
if(KnobRuntime.check(java.util.UUID.fromString("7852071d-fddc-35c7-a64e-9f49d123b228"))) {
time += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("48571e39-d583-39a6-85b1-b59a1948325b"))) {
time = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b23c428a-334f-3f7f-b795-b6ac55d3ec19"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2066a2d8-96d9-32cb-934c-eabbd092005b"))) {
time -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("0ebd712d-dffe-302f-971b-7b4a3050370e"))) { setBlockSize(time); } else if (KnobRuntime.check(java.util.UUID.fromString("e0963439-1c77-358c-b7a6-02b9a670e9fc"))) { setDataSize(time); } else if (KnobRuntime.check(java.util.UUID.fromString("8c1bab9a-a03b-37f0-8c68-2ea99ed0a982"))) { setHeapSize(time); } else { setTime(time); }
    }

    int getBatch() {
      return this.batch;
    }

    void setBatch(int batch) {
      this.batch = batch;
    }

    /** Returns true when the limit can be enforced from the scope of the checker */
    boolean canEnforceBatchLimitFromScope(LimitScope checkerScope) {
if(KnobRuntime.check(java.util.UUID.fromString("76e57982-6fb8-30f0-8e22-4cffc33d663b"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("229d8743-83da-37bc-bf45-0030cf2c0c4e"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("544f46c7-ad87-317a-808c-69440c5e379e"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b9d77e6-20af-3c99-a0b9-f557be69a7e3"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb628e95-ab01-3baf-a3d1-25256aae5ac3"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0399405d-d6e9-3948-bc1f-87ca83577330"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      return LimitScope.BETWEEN_CELLS.canEnforceLimitFromScope(checkerScope);
    }

    long getDataSize() {
if(KnobRuntime.check(java.util.UUID.fromString("5ba1caf9-3212-3358-8540-e2d410f0f31b"))) {
return 0;
}
      return this.dataSize;
    }

    long getHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("adc01996-7402-309e-83d3-d8b6834af4c0"))) {
return 0;
}
      return this.heapSize;
    }

    long getBlockSize() {
if(KnobRuntime.check(java.util.UUID.fromString("cfcdb23e-3cb0-31b9-b752-37f920740545"))) {
return 0;
}
      return this.blockSize;
    }

    void setDataSize(long dataSize) {
if(KnobRuntime.check(java.util.UUID.fromString("8f4b6b52-3e8e-3f38-b82e-97648a7903c1"))) {
return;
}
      this.dataSize = dataSize;
    }

    void setHeapSize(long heapSize) {
      this.heapSize = heapSize;
    }

    void setBlockSize(long blockSize) {
      this.blockSize = blockSize;
    }

    /** Returns {@link LimitScope} indicating scope in which the size limit is enforced */
    LimitScope getSizeScope() {
if(KnobRuntime.check(java.util.UUID.fromString("67c6a69c-3c2d-35c5-a436-45a6d22a97c0"))) {
return null;
}
      return this.sizeScope;
    }

    /**
     * Change the scope in which the size limit is enforced
     */
    void setSizeScope(LimitScope scope) {
      this.sizeScope = scope;
    }

    /** Returns true when the limit can be enforced from the scope of the checker */
    boolean canEnforceSizeLimitFromScope(LimitScope checkerScope) {
if(KnobRuntime.check(java.util.UUID.fromString("1ee8e4f9-9a8e-3f7b-b99e-3aa408662aa1"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("50ddc829-7da7-3c83-bef8-0e790023377b"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c913a404-6a80-31ce-9d99-502cd994694a"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51ba782a-8046-3166-8fd1-0ee326e806fa"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("46e7eb3d-a8e1-34fc-8e63-f0b8766a4658"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f5a0b6d-6a74-3b23-8a10-e682c02c0ef0"))) {
try {
    java.lang.reflect.Field field = checkerScope.getClass().getDeclaredField("depth");
    field.setAccessible(true);
    int oldValue = ((int)field.get(checkerScope));
    field.set(checkerScope, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      return this.sizeScope.canEnforceLimitFromScope(checkerScope);
    }

    long getTime() {
      return this.time;
    }

    void setTime(long time) {
      this.time = time;
    }

    /** Returns {@link LimitScope} indicating scope in which the time limit is enforced */
    LimitScope getTimeScope() {
if(KnobRuntime.check(java.util.UUID.fromString("90a214e0-a35a-3300-ab54-5141cda04483"))) {
return null;
}
      return this.timeScope;
    }

    /**
     * Change the scope in which the time limit is enforced
     */
    void setTimeScope(LimitScope scope) {
      this.timeScope = scope;
    }

    /** Returns true when the limit can be enforced from the scope of the checker */
    boolean canEnforceTimeLimitFromScope(LimitScope checkerScope) {
      return this.timeScope.canEnforceLimitFromScope(checkerScope);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");

      sb.append("batch:");
      sb.append(batch);

      sb.append(", dataSize:");
      sb.append(dataSize);

      sb.append(", heapSize:");
      sb.append(heapSize);

      sb.append(", blockSize:");
      sb.append(blockSize);

      sb.append(", sizeScope:");
      sb.append(sizeScope);

      sb.append(", time:");
      sb.append(time);

      sb.append(", timeScope:");
      sb.append(timeScope);

      sb.append("}");
      return sb.toString();
    }
  }

  private static class ProgressFields {

    private static int DEFAULT_BATCH = -1;
    private static long DEFAULT_SIZE = -1L;

    // The batch limit will always be enforced between cells, thus, there isn't a field to hold the
    // batch scope
    int batch = DEFAULT_BATCH;

    // The sum of cell data sizes(key + value). The Cell data might be in on heap or off heap area.
    long dataSize = DEFAULT_SIZE;
    // The sum of heap space occupied by all tracked cells. This includes Cell POJO's overhead as
    // such AND data cells of Cells which are in on heap area.
    long heapSize = DEFAULT_SIZE;
    // The total amount of block bytes that have been loaded in order to process cells for the
    // request.
    long blockSize = DEFAULT_SIZE;

    ProgressFields(int batch, long size, long heapSize, long blockSize) {
      setFields(batch, size, heapSize, blockSize);
    }

    /**
     * Set all fields together.
     */
    void setFields(int batch, long dataSize, long heapSize, long blockSize) {
      setBatch(batch);
      setDataSize(dataSize);
      setHeapSize(heapSize);
if(KnobRuntime.check(java.util.UUID.fromString("f5314a38-a3ce-3d6e-93dd-6c62efd9ba6a"))) {
blockSize /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("bfd5da0b-bbc9-3ca4-b264-8b8536be2e3e"))) {
blockSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7c51db07-99c6-3419-8115-465df78c897f"))) {
blockSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6494b26b-3ba9-3f85-b5b9-f8627515b92b"))) {
blockSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("029a07d5-e124-3533-aa94-9e58bfb1508c"))) {
blockSize -= 1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("e6ec5a10-50b5-3c42-82bc-b02362d4982a"))) { setDataSize(blockSize); } else if (KnobRuntime.check(java.util.UUID.fromString("ba8d494f-4d6a-3377-9ac3-2d08055dbaed"))) { setHeapSize(blockSize); } else { setBlockSize(blockSize); }
    }

    int getBatch() {
      return this.batch;
    }

    void setBatch(int batch) {
      this.batch = batch;
    }

    long getDataSize() {
      return this.dataSize;
    }

    long getHeapSize() {
      return this.heapSize;
    }

    long getBlockSize() {
      return this.blockSize;
    }

    void setDataSize(long dataSize) {
      this.dataSize = dataSize;
    }

    void setBlockSize(long blockSize) {
      this.blockSize = blockSize;
    }

    void setHeapSize(long heapSize) {
      this.heapSize = heapSize;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");

      sb.append("batch:");
      sb.append(batch);

      sb.append(", dataSize:");
      sb.append(dataSize);

      sb.append(", heapSize:");
      sb.append(heapSize);

      sb.append(", blockSize:");
      sb.append(blockSize);

      sb.append("}");
      return sb.toString();
    }
  }
}
