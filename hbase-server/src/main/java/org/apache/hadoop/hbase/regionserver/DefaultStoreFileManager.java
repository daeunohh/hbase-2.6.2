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

import static org.apache.hadoop.hbase.regionserver.StoreFileWriter.shouldEnableHistoricalCompactionFiles;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Default implementation of StoreFileManager. Not thread-safe.
 */
@InterfaceAudience.Private
class DefaultStoreFileManager implements StoreFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStoreFileManager.class);

  private final CellComparator cellComparator;
  private final CompactionConfiguration comConf;
  private final int blockingFileCount;
  private final Comparator<HStoreFile> storeFileComparator;

  static class StoreFileList {
    /**
     * List of store files inside this store. This is an immutable list that is atomically replaced
     * when its contents change.
     */
    final ImmutableList<HStoreFile> all;
    /**
     * List of store files that include the latest cells inside this store. This is an immutable
     * list that is atomically replaced when its contents change.
     */
    @Nullable
    final ImmutableList<HStoreFile> live;

    StoreFileList(ImmutableList<HStoreFile> storeFiles, ImmutableList<HStoreFile> liveStoreFiles) {
      this.all = storeFiles;
      this.live = liveStoreFiles;
    }
  }

  private volatile StoreFileList storeFiles;

  /**
   * List of compacted files inside this store that needs to be excluded in reads because further
   * new reads will be using only the newly created files out of compaction. These compacted files
   * will be deleted/cleared once all the existing readers on these compacted files are done.
   */
  private volatile ImmutableList<HStoreFile> compactedfiles = ImmutableList.of();
  private final boolean enableLiveFileTracking;

  public DefaultStoreFileManager(CellComparator cellComparator,
    Comparator<HStoreFile> storeFileComparator, Configuration conf,
    CompactionConfiguration comConf) {
    this.cellComparator = cellComparator;
    this.storeFileComparator = storeFileComparator;
    this.comConf = comConf;
    blockingFileCount =
      conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT);
    enableLiveFileTracking = shouldEnableHistoricalCompactionFiles(conf);
    storeFiles =
      new StoreFileList(ImmutableList.of(), enableLiveFileTracking ? ImmutableList.of() : null);
  }

  private List<HStoreFile> getLiveFiles(Collection<HStoreFile> storeFiles) throws IOException {
    List<HStoreFile> liveFiles = new ArrayList<>(storeFiles.size());
    for (HStoreFile file : storeFiles) {
      file.initReader();
      if (!file.isHistorical()) {
        liveFiles.add(file);
      }
    }
    return liveFiles;
  }

  @Override
  public void loadFiles(List<HStoreFile> storeFiles) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("994ddc77-fda9-3ab5-9a29-d5c37efab593"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ede62c6f-dd2f-3e0f-9c5f-74db48758452"))) {
throw new java.io.IOException("Injected exception");
}
    this.storeFiles = new StoreFileList(ImmutableList.sortedCopyOf(storeFileComparator, storeFiles),
      enableLiveFileTracking
        ? ImmutableList.sortedCopyOf(storeFileComparator, getLiveFiles(storeFiles))
        : null);
  }

  @Override
  public final Collection<HStoreFile> getStoreFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("ac6c01de-a24a-36a2-9529-e9e25d119250"))) {
return null;
}
    return storeFiles.all;
  }

  @Override
  public Collection<HStoreFile> getCompactedfiles() {
if(KnobRuntime.check(java.util.UUID.fromString("622bc245-837d-3856-9272-8c5fba346638"))) {
return null;
}
    return compactedfiles;
  }

  @Override
  public void insertNewFiles(Collection<HStoreFile> sfs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("775cf8fc-2de1-30e8-8391-08bcafacc391"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d08a25a-0610-3c5e-b9c7-6e806cdb4c89"))) {
throw new java.io.IOException("Injected exception");
}
    storeFiles = new StoreFileList(
      ImmutableList.sortedCopyOf(storeFileComparator, Iterables.concat(storeFiles.all, sfs)),
      enableLiveFileTracking
        ? ImmutableList.sortedCopyOf(storeFileComparator,
          Iterables.concat(storeFiles.live, getLiveFiles(sfs)))
        : null);
  }

  @Override
  public ImmutableCollection<HStoreFile> clearFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("40e4d93d-ae49-3d3f-95bd-78f00111e0d1"))) {
return null;
}
    ImmutableList<HStoreFile> result = storeFiles.all;
    storeFiles =
      new StoreFileList(ImmutableList.of(), enableLiveFileTracking ? ImmutableList.of() : null);
    return result;
  }

  @Override
  public Collection<HStoreFile> clearCompactedFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("b746ad96-8e93-392b-8ea4-3a8deb8285e4"))) {
return null;
}
    List<HStoreFile> result = compactedfiles;
    compactedfiles = ImmutableList.of();
    return result;
  }

  @Override
  public final int getStorefileCount() {
if(KnobRuntime.check(java.util.UUID.fromString("55f6b1d3-92bb-3335-8b60-7603e0e6405c"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("6cd76a52-75b2-3ce8-a385-9e66fe55ac99"))) ? (getStorefileCount()) : (storeFiles.all.size()));
  }

  @Override
  public final int getCompactedFilesCount() {
    return compactedfiles.size();
  }

  @Override
  public void addCompactionResults(Collection<HStoreFile> newCompactedfiles,
    Collection<HStoreFile> results) throws IOException {
    ImmutableList<HStoreFile> liveStoreFiles = null;
    if (enableLiveFileTracking) {
      liveStoreFiles = ImmutableList.sortedCopyOf(storeFileComparator,
        Iterables.concat(Iterables.filter(storeFiles.live, sf -> !newCompactedfiles.contains(sf)),
          getLiveFiles(results)));
    }
    storeFiles =
      new StoreFileList(
        ImmutableList
          .sortedCopyOf(storeFileComparator,
            Iterables.concat(
              Iterables.filter(storeFiles.all, sf -> !newCompactedfiles.contains(sf)), results)),
        liveStoreFiles);
    // Mark the files as compactedAway once the storefiles and compactedfiles list is finalized
    // Let a background thread close the actual reader on these compacted files and also
    // ensure to evict the blocks from block cache so that they are no longer in
    // cache
    newCompactedfiles.forEach(HStoreFile::markCompactedAway);
    compactedfiles = ImmutableList.sortedCopyOf(storeFileComparator,
      Iterables.concat(compactedfiles, newCompactedfiles));
  }

  @Override
  public void removeCompactedFiles(Collection<HStoreFile> removedCompactedfiles) {
    compactedfiles = compactedfiles.stream().filter(sf -> !removedCompactedfiles.contains(sf))
      .sorted(storeFileComparator).collect(ImmutableList.toImmutableList());
  }

  @Override
  public final Iterator<HStoreFile> getCandidateFilesForRowKeyBefore(KeyValue targetKey) {
    return storeFiles.all.reverse().iterator();
  }

  @Override
  public Iterator<HStoreFile> updateCandidateFilesForRowKeyBefore(
    Iterator<HStoreFile> candidateFiles, KeyValue targetKey, Cell candidate) {
    // Default store has nothing useful to do here.
    // TODO: move this comment when implementing Level:
    // Level store can trim the list by range, removing all the files which cannot have
    // any useful candidates less than "candidate".
    return candidateFiles;
  }

  @Override
  public final Optional<byte[]> getSplitPoint() throws IOException {
    return StoreUtils.getSplitPoint(storeFiles.all, cellComparator);
  }

  @Override
  public Collection<HStoreFile> getFilesForScan(byte[] startRow, boolean includeStartRow,
    byte[] stopRow, boolean includeStopRow, boolean onlyLatestVersion) {
if(KnobRuntime.check(java.util.UUID.fromString("1acbc6e2-ff39-3539-a0e7-09c5dd7319a2"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9d7f3236-804c-36f4-8764-091085ecae70"))) ? ((onlyLatestVersion) && (enableLiveFileTracking)) : (((KnobRuntime.check(java.util.UUID.fromString("337aa275-2885-3b95-a979-1d3d7784c5f3"))) ? (onlyLatestVersion) : (((KnobRuntime.check(java.util.UUID.fromString("7b1c4dca-c692-3fbe-9b1d-9ee993aa9103"))) ? ((!onlyLatestVersion) && (enableLiveFileTracking)) : (((KnobRuntime.check(java.util.UUID.fromString("98ed62bd-3ee1-3901-820f-248521d7997e"))) ? (enableLiveFileTracking) : (((KnobRuntime.check(java.util.UUID.fromString("63aee74e-87f3-3223-93ea-9701fb7010d1"))) ? (!onlyLatestVersion) : (((KnobRuntime.check(java.util.UUID.fromString("e7e31ccf-e836-3fe6-a5c9-85689f5e05ff"))) ? ((onlyLatestVersion) || (enableLiveFileTracking)) : (((KnobRuntime.check(java.util.UUID.fromString("92b3c600-69c1-3e42-8e6f-4a6220d7a0c7"))) ? ((!onlyLatestVersion) || (!enableLiveFileTracking)) : (((KnobRuntime.check(java.util.UUID.fromString("820d2267-1577-34c9-af48-83a3633ced42"))) ? (!enableLiveFileTracking) : (((KnobRuntime.check(java.util.UUID.fromString("a63e7ee1-6fe6-306a-8c33-8671ef758e95"))) ? ((onlyLatestVersion) || (!enableLiveFileTracking)) : (((KnobRuntime.check(java.util.UUID.fromString("c289d466-be93-377b-bb29-5bb4502232c6"))) ? ((!onlyLatestVersion) || (enableLiveFileTracking)) : (((KnobRuntime.check(java.util.UUID.fromString("2244c525-2082-3e31-8e17-333f98ec8d8e"))) ? ((!onlyLatestVersion) && (!enableLiveFileTracking)) : (((KnobRuntime.check(java.util.UUID.fromString("df43c0e4-afdf-3f69-b81a-c01215b91b78"))) ? ((onlyLatestVersion) && (!enableLiveFileTracking)) : (onlyLatestVersion && enableLiveFileTracking))))))))))))))))))))))))) {
      return storeFiles.live;
    }
    // We cannot provide any useful input and already have the files sorted by seqNum.
    return getStoreFiles();
  }

  @Override
  public int getStoreCompactionPriority() {
    int priority = blockingFileCount - storeFiles.all.size();
    return (priority == HStore.PRIORITY_USER) ? priority + 1 : priority;
  }

  @Override
  public Collection<HStoreFile> getUnneededFiles(long maxTs, List<HStoreFile> filesCompacting) {
    ImmutableList<HStoreFile> files = storeFiles.all;
    // 1) We can never get rid of the last file which has the maximum seqid.
    // 2) Files that are not the latest can't become one due to (1), so the rest are fair game.
    return files.stream().limit(Math.max(0, files.size() - 1)).filter(sf -> {
      long fileTs = sf.getReader().getMaxTimestamp();
      if (fileTs < maxTs && !filesCompacting.contains(sf)) {
        LOG.info("Found an expired store file {} whose maxTimestamp is {}, which is below {}",
          sf.getPath(), fileTs, maxTs);
        return true;
      } else {
        return false;
      }
    }).collect(Collectors.toList());
  }

  @Override
  public double getCompactionPressure() {
    int storefileCount = getStorefileCount();
    int minFilesToCompact = comConf.getMinFilesToCompact();
    if (storefileCount <= minFilesToCompact) {
      return 0.0;
    }
    return (double) (storefileCount - minFilesToCompact) / (blockingFileCount - minFilesToCompact);
  }

  @Override
  public Comparator<HStoreFile> getStoreFileComparator() {
    return storeFileComparator;
  }
}
