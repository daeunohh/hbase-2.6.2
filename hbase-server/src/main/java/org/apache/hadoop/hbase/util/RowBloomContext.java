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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.LAST_BLOOM_KEY;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handles ROW bloom related context. It works with both ByteBufferedCell and byte[] backed cells
 */
@InterfaceAudience.Private
public class RowBloomContext extends BloomContext {

  public RowBloomContext(BloomFilterWriter bloomFilterWriter, CellComparator comparator) {
    super(bloomFilterWriter, comparator);
  }

  @Override
  public void addLastBloomKey(Writer writer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4adf1e40-c029-3297-9724-caf54469426f"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9a761b7a-a20c-37de-99ad-f2699db3bdae"))) ? ((this.getLastCell()) == (null)) : (this.getLastCell() != null))) {
      byte[] key = CellUtil.copyRow(this.getLastCell());
if(KnobRuntime.check(java.util.UUID.fromString("02c23b0a-6182-3d72-9b27-0422fd4936ba"))) {
throw new java.io.IOException("Injected exception");
}
      writer.appendFileInfo(LAST_BLOOM_KEY, key);
    }
  }

  @Override
  protected boolean isNewKey(Cell cell) {
    if (this.getLastCell() != null) {
      return !CellUtil.matchingRows(cell, this.getLastCell());
    }
    return true;
  }
}
