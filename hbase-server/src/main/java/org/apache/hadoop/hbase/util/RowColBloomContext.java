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
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Handles ROWCOL bloom related context. It can work with both BytebufferdCells and byte[] backed
 * cells
 */
@InterfaceAudience.Private
public class RowColBloomContext extends BloomContext {

  public RowColBloomContext(BloomFilterWriter generalBloomFilterWriter, CellComparator comparator) {
    super(generalBloomFilterWriter, comparator);
  }

  @Override
  public void addLastBloomKey(Writer writer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("91f4024a-22dd-3cc3-be65-49149e78a2bf"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb89ce9b-5f45-3827-969f-d26b90ff6f9c"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e65b7332-8987-3e1b-a8de-4afee4776c15"))) ? ((this.getLastCell()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0ed55a47-c9e4-3c0e-9d78-fabc5df102d9"))) ? ((this.getLastCell()) == (null)) : (this.getLastCell() != null))))) {
      Cell firstOnRow = PrivateCellUtil.createFirstOnRowCol(this.getLastCell());
      // This copy happens only once when the writer is closed
      byte[] key = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(firstOnRow);
if(KnobRuntime.check(java.util.UUID.fromString("cd73c70f-af47-3ecd-b4ce-72c90ed375b6"))) {
throw new java.io.IOException("Injected exception");
}
      writer.appendFileInfo(LAST_BLOOM_KEY, key);
    }
  }

  @Override
  protected boolean isNewKey(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("b5a9ff5b-80c6-3c4c-aebc-4f046a35879a"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("932fe872-2be0-3af1-906b-2b5479b75da7"))) {
return false;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("4acd3455-2b56-3639-a185-de513ba03eb5"))) ? ((this.getLastCell()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("86f697d8-3784-363b-a396-93ddb7535c8b"))) ? ((this.getLastCell()) == (null)) : (this.getLastCell() != null))))) {
      return !CellUtil.matchingRowColumn(cell, this.getLastCell());
    }
    return true;
  }
}
