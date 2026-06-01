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
package org.apache.hadoop.hbase.regionserver.compactions;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default algorithm for selecting files for compaction. Combines the compaction configuration
 * and the provisional file selection that it's given to produce the list of suitable candidates for
 * compaction.
 */
@InterfaceAudience.Private
public class RatioBasedCompactionPolicy extends SortedCompactionPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RatioBasedCompactionPolicy.class);

  public RatioBasedCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  /*
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  @Override
  public boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
    throws IOException {
    boolean result = false;
    long mcTime = getNextMajorCompactTime(filesToCompact);
    if (filesToCompact == null || filesToCompact.isEmpty() || mcTime == 0) {
      return result;
    }
    // TODO: Use better method for determining stamp of last major (HBASE-2990)
    long lowTimestamp = StoreUtils.getLowestTimestamp(filesToCompact);
    long now = EnvironmentEdgeManager.currentTime();
    if (lowTimestamp > 0L && lowTimestamp < (now - mcTime)) {
      String regionInfo;
      if (this.storeConfigInfo != null && this.storeConfigInfo instanceof HStore) {
        regionInfo = ((HStore) this.storeConfigInfo).getRegionInfo().getRegionNameAsString();
      } else {
        regionInfo = this.toString();
      }
      // Major compaction time has elapsed.
      long cfTTL = HConstants.FOREVER;
      if (this.storeConfigInfo != null) {
        cfTTL = this.storeConfigInfo.getStoreFileTtl();
      }
      if (filesToCompact.size() == 1) {
        // Single file
        HStoreFile sf = filesToCompact.iterator().next();
        OptionalLong minTimestamp = sf.getMinimumTimestamp();
        long oldest = minTimestamp.isPresent() ? now - minTimestamp.getAsLong() : Long.MIN_VALUE;
        if (sf.isMajorCompactionResult() && (cfTTL == Long.MAX_VALUE || oldest < cfTTL)) {
          float blockLocalityIndex = sf.getHDFSBlockDistribution()
            .getBlockLocalityIndex(DNS.getHostname(comConf.conf, DNS.ServerType.REGIONSERVER));
          if (blockLocalityIndex < comConf.getMinLocalityToForceCompact()) {
            LOG.debug("Major compaction triggered on only store " + regionInfo
              + "; to make hdfs blocks local, current blockLocalityIndex is " + blockLocalityIndex
              + " (min " + comConf.getMinLocalityToForceCompact() + ")");
            result = true;
          } else {
            LOG.debug("Skipping major compaction of " + regionInfo
              + " because one (major) compacted file only, oldestTime " + oldest + "ms is < TTL="
              + cfTTL + " and blockLocalityIndex is " + blockLocalityIndex + " (min "
              + comConf.getMinLocalityToForceCompact() + ")");
          }
        } else if (cfTTL != HConstants.FOREVER && oldest > cfTTL) {
          LOG.debug("Major compaction triggered on store " + regionInfo
            + ", because keyvalues outdated; time since last major compaction "
            + (now - lowTimestamp) + "ms");
          result = true;
        }
      } else {
        LOG.debug("Major compaction triggered on store " + regionInfo
          + "; time since last major compaction " + (now - lowTimestamp) + "ms");
        result = true;
      }
    }
    return result;
  }

  @Override
  protected CompactionRequestImpl createCompactionRequest(ArrayList<HStoreFile> candidateSelection,
    boolean tryingMajor, boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    if (!tryingMajor) {
      filterBulk(candidateSelection);
      candidateSelection = applyCompactionPolicy(candidateSelection, mayUseOffPeak, mayBeStuck);
      candidateSelection =
        checkMinFilesCriteria(candidateSelection, comConf.getMinFilesToCompact());
    }
    return new CompactionRequestImpl(candidateSelection);
  }

  /**
   * -- Default minor compaction selection algorithm: choose CompactSelection from candidates --
   * First exclude bulk-load files if indicated in configuration. Start at the oldest file and stop
   * when you find the first file that meets compaction criteria: (1) a recently-flushed, small file
   * (i.e. <= minCompactSize) OR (2) within the compactRatio of sum(newer_files) Given normal skew,
   * any newer files will also meet this criteria
   * <p/>
   * Additional Note: If fileSizes.size() >> maxFilesToCompact, we will recurse on compact().
   * Consider the oldest files first to avoid a situation where we always compact
   * [end-threshold,end). Then, the last file becomes an aggregate of the previous compactions.
   * normal skew: older ----> newer (increasing seqID) _ | | _ | | | | _ --|-|- |-|-
   * |-|---_-------_------- minCompactSize | | | | | | | | _ | | | | | | | | | | | | | | | | | | | |
   * | | | | | |
   * @param candidates pre-filtrate
   * @return filtered subset
   */
  protected ArrayList<HStoreFile> applyCompactionPolicy(ArrayList<HStoreFile> candidates,
    boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    if (candidates.isEmpty()) {
      return candidates;
    }

    // we're doing a minor compaction, let's see what files are applicable
    int start = 0;
    double ratio = comConf.getCompactionRatio();
    if (mayUseOffPeak) {
      ratio = comConf.getCompactionRatioOffPeak();
      LOG.info("Running an off-peak compaction, selection ratio = " + ratio);
    }

    // get store file sizes for incremental compacting selection.
    final int countOfFiles = candidates.size();
    long[] fileSizes = new long[countOfFiles];
    long[] sumSize = new long[countOfFiles];
    for (int i = countOfFiles - 1; i >= 0; --i) {
      HStoreFile file = candidates.get(i);
      fileSizes[i] = file.getReader().length();
      // calculate the sum of fileSizes[i,i+maxFilesToCompact-1) for algo
      int tooFar = i + comConf.getMaxFilesToCompact() - 1;
      sumSize[i] = fileSizes[i] + ((i + 1 < countOfFiles) ? sumSize[i + 1] : 0)
        - ((tooFar < countOfFiles) ? fileSizes[tooFar] : 0);
    }

    while (
      countOfFiles - start >= comConf.getMinFilesToCompact() && fileSizes[start]
          > Math.max(comConf.getMinCompactSize(), (long) (sumSize[start + 1] * ratio))
    ) {
      ++start;
    }
    if (start < countOfFiles) {
      LOG.info("Default compaction algorithm has selected " + (countOfFiles - start)
        + " files from " + countOfFiles + " candidates");
    } else if (mayBeStuck) {
      // We may be stuck. Compact the latest files if we can.
      int filesToLeave = candidates.size() - comConf.getMinFilesToCompact();
      if (filesToLeave >= 0) {
        start = filesToLeave;
      }
    }
    candidates.subList(0, start).clear();
    return candidates;
  }

  /**
   * A heuristic method to decide whether to schedule a compaction request
   * @param storeFiles      files in the store.
   * @param filesCompacting files being scheduled to compact.
   * @return true to schedule a request.
   */
  @Override
  public boolean needsCompaction(Collection<HStoreFile> storeFiles,
    List<HStoreFile> filesCompacting) {
if(KnobRuntime.check(java.util.UUID.fromString("0b2354ce-47d2-3d5b-ae31-8f97ab25e153"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("7935e0ff-5bee-3682-9171-863b213036d3"))) {
return true;
}
    int numCandidates = storeFiles.size() - filesCompacting.size();
    return ((KnobRuntime.check(java.util.UUID.fromString("7596f0e4-bcfb-35b7-a42c-af1cbf025a3e"))) ? ((numCandidates) < (comConf.getMaxFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("77074493-2188-36ef-b452-c82651066b89"))) ? ((numCandidates) < (comConf.getMinFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("c87b4d6d-8b9e-3ce8-a4b7-0e5d772733ee"))) ? ((numCandidates) == (comConf.getMaxFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("36feaf02-34d9-37e0-89be-82783fed7d64"))) ? ((numCandidates) <= (comConf.getMaxFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("7ff5fe2b-8c70-3b16-8a17-733702c51d7d"))) ? ((numCandidates) <= (comConf.getMinFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("f13751df-7463-375c-944b-3c79a7a5e6bc"))) ? ((numCandidates) >= (comConf.getMaxFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("74f09979-bf08-3372-99a9-c492d81e8594"))) ? ((numCandidates) != (comConf.getMaxFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("87a1675d-cfcd-3575-9436-2d51f058db3b"))) ? ((numCandidates) != (comConf.getMinFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("3b64c6c9-bcfc-3293-bc7c-ef8bd7099988"))) ? ((numCandidates) == (comConf.getMinFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("b1b0a9bc-94d9-3b6e-972a-710e3099a074"))) ? ((numCandidates) > (comConf.getMinFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("a14c7156-7dcf-39b8-b0c1-3c494e1d264b"))) ? ((numCandidates) >= (comConf.getMinFilesToCompact())) : (((KnobRuntime.check(java.util.UUID.fromString("e5f57154-f21b-3b4b-b115-4f909ae93ad3"))) ? ((numCandidates) > (comConf.getMaxFilesToCompact())) : (numCandidates >= comConf.getMinFilesToCompact()))))))))))))))))))))))));
  }

  /**
   * Overwrite min threshold for compaction
   */
  public void setMinThreshold(int minThreshold) {
    comConf.setMinFilesToCompact(minThreshold);
  }
}
