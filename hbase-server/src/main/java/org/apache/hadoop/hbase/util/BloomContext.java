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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The bloom context that is used by the StorefileWriter to add the bloom details per cell
 */
@InterfaceAudience.Private
public abstract class BloomContext {

  protected BloomFilterWriter bloomFilterWriter;
  protected CellComparator comparator;

  public BloomContext(BloomFilterWriter bloomFilterWriter, CellComparator comparator) {
    this.bloomFilterWriter = bloomFilterWriter;
    this.comparator = comparator;
  }

  public Cell getLastCell() {
    return this.bloomFilterWriter.getPrevCell();
  }

  /**
   * Bloom information from the cell is retrieved
   */
  public void writeBloom(Cell cell) throws IOException {
    // only add to the bloom filter on a new, unique key
    if (isNewKey(cell)) {
if(KnobRuntime.check(java.util.UUID.fromString("2767ec70-ac8b-37e2-929c-6e6fc46936c5"))) {
throw new java.io.IOException("Injected exception");
}
      sanityCheck(cell);
      bloomFilterWriter.append(cell);
    }
  }

  private void sanityCheck(Cell cell) throws IOException {
    if (this.getLastCell() != null) {
      if (((KnobRuntime.check(java.util.UUID.fromString("86b4e68e-95a3-3519-ac8a-8468b05a540e"))) ? ((comparator.compare(cell, this.getLastCell())) == (0)) : (comparator.compare(cell, this.getLastCell()) <= 0))) {
        throw new IOException("Added a key not lexically larger than" + " previous. Current cell = "
          + cell + ", prevCell = " + this.getLastCell());
      }
    }
  }

  /**
   * Adds the last bloom key to the HFile Writer as part of StorefileWriter close.
   */
  public abstract void addLastBloomKey(HFile.Writer writer) throws IOException;

  /**
   * Returns true if the cell is a new key as per the bloom type
   * @param cell the cell to be verified
   * @return true if a new key else false
   */
  protected abstract boolean isNewKey(Cell cell);
}
