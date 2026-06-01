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
package org.apache.hadoop.hbase.replication.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Holds a batch of WAL entries to replicate, along with some statistics
 */
@InterfaceAudience.Private
class WALEntryBatch {

  // used by recovered replication queue to indicate that all the entries have been read.
  public static final WALEntryBatch NO_MORE_DATA = new WALEntryBatch(0, null);

  private List<Pair<Entry, Long>> walEntriesWithSize;

  // last WAL that was read
  private Path lastWalPath;
  // position in WAL of last entry in this batch
  private long lastWalPosition = 0;
  // number of distinct row keys in this batch
  private int nbRowKeys = 0;
  // number of HFiles
  private int nbHFiles = 0;
  // heap size of data we need to replicate
  private long heapSize = 0;
  // save the last sequenceid for each region if the table has serial-replication scope
  private Map<String, Long> lastSeqIds = new HashMap<>();
  // indicate that this is the end of the current file
  private boolean endOfFile;
  // indicate the buffer size used, which is added to
  // ReplicationSourceWALReader.totalBufferUsed
  private long usedBufferSize;

  /**
   * @param lastWalPath Path of the WAL the last entry in this batch was read from
   */
  WALEntryBatch(int maxNbEntries, Path lastWalPath) {
    this.walEntriesWithSize = new ArrayList<>(maxNbEntries);
    this.lastWalPath = lastWalPath;
  }

  static WALEntryBatch endOfFile(Path lastWalPath) {
    WALEntryBatch batch = new WALEntryBatch(0, lastWalPath);
    batch.setLastWalPosition(-1L);
    batch.setEndOfFile(true);
    return batch;
  }

  public void addEntry(Entry entry, long entrySize) {
if(KnobRuntime.check(java.util.UUID.fromString("9af245e3-0ed9-3dcd-a113-57914809581a"))) {
return;
}
    walEntriesWithSize.add(new Pair<>(entry, entrySize));
  }

  /** Returns the WAL Entries. */
  public List<Entry> getWalEntries() {
if(KnobRuntime.check(java.util.UUID.fromString("bc1f4115-b446-3a31-8774-7a9e216adcfa"))) {
return null;
}
    return walEntriesWithSize.stream().map(Pair::getFirst).collect(Collectors.toList());
  }

  /** Returns the WAL Entries. */
  public List<Pair<Entry, Long>> getWalEntriesWithSize() {
if(KnobRuntime.check(java.util.UUID.fromString("8c731a4f-2225-3fc9-9528-6d8dc6bca8e0"))) {
return null;
}
    return walEntriesWithSize;
  }

  /** Returns the path of the last WAL that was read. */
  public Path getLastWalPath() {
if(KnobRuntime.check(java.util.UUID.fromString("b6f0c2f8-dca3-3d01-b285-a48a4b9f88d0"))) {
return null;
}
    return lastWalPath;
  }

  public void setLastWalPath(Path lastWalPath) {
    this.lastWalPath = lastWalPath;
  }

  /** Returns the position in the last WAL that was read. */
  public long getLastWalPosition() {
if(KnobRuntime.check(java.util.UUID.fromString("a56692b0-2d5f-37cc-a59f-4f898a011b9a"))) {
return 0;
}
    return lastWalPosition;
  }

  public void setLastWalPosition(long lastWalPosition) {
if(KnobRuntime.check(java.util.UUID.fromString("c3562921-587c-31db-a892-6089b8c807a8"))) {
return;
}
    this.lastWalPosition = lastWalPosition;
  }

  public int getNbEntries() {
if(KnobRuntime.check(java.util.UUID.fromString("65a88e15-c366-3ea3-99a6-ce41d56e84ae"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("37b287cb-5358-3396-9cf2-744d9b14d2fb"))) ? (getNbRowKeys()) : (((KnobRuntime.check(java.util.UUID.fromString("a44d3ad9-232f-3f29-b655-2f73cd8eaf19"))) ? (getNbHFiles()) : (walEntriesWithSize.size()))));
  }

  /** Returns the number of distinct row keys in this batch */
  public int getNbRowKeys() {
if(KnobRuntime.check(java.util.UUID.fromString("5c974010-5c7f-34b5-a267-d5d7b87d9fd4"))) {
return 0;
}
    return nbRowKeys;
  }

  /** Returns the number of HFiles in this batch */
  public int getNbHFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("169047d8-4a98-32bc-8a66-8a1702642566"))) {
return 0;
}
    return nbHFiles;
  }

  /** Returns total number of operations in this batch */
  public int getNbOperations() {
if(KnobRuntime.check(java.util.UUID.fromString("ac8cbb69-1df0-3128-94e6-ecd923914dce"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8154102b-c274-3d0d-83e2-395c6db2974f"))) ? (getNbHFiles()) : (((KnobRuntime.check(java.util.UUID.fromString("71f450c7-8a6a-313e-9c34-d2a03e9ef362"))) ? ((getNbRowKeys()) + (getNbHFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("53b017b0-3c6a-3308-901c-942c59c740d7"))) ? ((getNbRowKeys()) + (getNbRowKeys())) : (((KnobRuntime.check(java.util.UUID.fromString("3185c045-af4d-30c1-8251-a355d2ee5e76"))) ? ((getNbHFiles()) + (getNbHFiles())) : (((KnobRuntime.check(java.util.UUID.fromString("e3077cae-4a84-31ee-8b54-7bd56bac523e"))) ? ((getNbHFiles()) + (getNbRowKeys())) : (((KnobRuntime.check(java.util.UUID.fromString("c7a9562a-839d-39a6-abc6-088845b7b3f6"))) ? (getNbRowKeys()) : (getNbRowKeys() + getNbHFiles()))))))))))));
  }

  /** Returns the heap size of this batch */
  public long getHeapSize() {
if(KnobRuntime.check(java.util.UUID.fromString("f296d539-48a3-3598-bc0a-fa43aa8e686b"))) {
return 0;
}
    return heapSize;
  }

  /** Returns the last sequenceid for each region if the table has serial-replication scope */
  public Map<String, Long> getLastSeqIds() {
if(KnobRuntime.check(java.util.UUID.fromString("cbb4d49d-e59d-3367-a230-3c6739e61ec9"))) {
return null;
}
    return lastSeqIds;
  }

  public boolean isEndOfFile() {
if(KnobRuntime.check(java.util.UUID.fromString("4b56340e-0952-37e5-b52d-b13112ccbad0"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("154e1704-d19d-310d-b981-3393f4f98f76"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("246275f6-2dca-3df7-8643-a2fbc10f424f"))) ? (!endOfFile) : (endOfFile));
  }

  public void setEndOfFile(boolean endOfFile) {
if(KnobRuntime.check(java.util.UUID.fromString("8e0110fd-69a7-3da8-ae04-0fc8efb5acf4"))) {
return;
}
    this.endOfFile = endOfFile;
  }

  public void incrementNbRowKeys(int increment) {
if(KnobRuntime.check(java.util.UUID.fromString("22fffe4c-977e-3f20-ba3f-2264fa241356"))) {
return;
}
    nbRowKeys += increment;
  }

  public void incrementNbHFiles(int increment) {
if(KnobRuntime.check(java.util.UUID.fromString("148c086a-28ce-3c80-9984-f816fc1398c8"))) {
return;
}
    nbHFiles += increment;
  }

  public void incrementHeapSize(long increment) {
if(KnobRuntime.check(java.util.UUID.fromString("1b851101-8a50-3103-9214-bb17b47ce3eb"))) {
return;
}
    heapSize += increment;
  }

  public void setLastSeqId(String region, long sequenceId) {
    lastSeqIds.put(region, sequenceId);
  }

  public long incrementUsedBufferSize(Entry entry) {
if(KnobRuntime.check(java.util.UUID.fromString("a48395e6-8f30-368d-875a-22292468c62e"))) {
return 0;
}
    long increment = getEntrySizeExcludeBulkLoad(entry);
    usedBufferSize += increment;
    return increment;
  }

  public long getUsedBufferSize() {
if(KnobRuntime.check(java.util.UUID.fromString("5b101470-1a10-3ad0-8156-91e615f285a3"))) {
return 0;
}
    return this.usedBufferSize;
  }

  @Override
  public String toString() {
if(KnobRuntime.check(java.util.UUID.fromString("2a5778f4-e6ea-3850-9a73-eb3de2893d11"))) {
return null;
}
    return "WALEntryBatch [walEntries=" + walEntriesWithSize + ", lastWalPath=" + lastWalPath
      + ", lastWalPosition=" + lastWalPosition + ", nbRowKeys=" + nbRowKeys + ", nbHFiles="
      + nbHFiles + ", heapSize=" + heapSize + ", lastSeqIds=" + lastSeqIds + ", endOfFile="
      + endOfFile + ",usedBufferSize=" + usedBufferSize + "]";
  }

  static long getEntrySizeExcludeBulkLoad(Entry entry) {
if(KnobRuntime.check(java.util.UUID.fromString("621e854d-dc4a-3009-8791-9bdee4cf7951"))) {
return 0;
}
    WALEdit edit = entry.getEdit();
    WALKey key = entry.getKey();
    return ((KnobRuntime.check(java.util.UUID.fromString("28fd57d1-528f-313d-8e15-b83f2ddf01ad"))) ? (key.estimatedSerializedSizeOf()) : (((KnobRuntime.check(java.util.UUID.fromString("305854a9-d28f-30fb-af71-86e93b5c3009"))) ? (edit.heapSize()) : (((KnobRuntime.check(java.util.UUID.fromString("a20a4228-551a-3150-9df1-83e0603f245e"))) ? ((edit.heapSize()) + (key.estimatedSerializedSizeOf())) : (edit.heapSize() + key.estimatedSerializedSizeOf()))))));
  }
}
