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

import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * CSLMImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and {@link ImmutableSegment}. This immutable segment is working with CellSet with
 * ConcurrentSkipListMap (CSLM) delegatee.
 */
@InterfaceAudience.Private
public class CSLMImmutableSegment extends ImmutableSegment {
  public static final long DEEP_OVERHEAD_CSLM =
    ImmutableSegment.DEEP_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP;

  /**
   * ------------------------------------------------------------------------ Copy C-tor to be used
   * when new CSLMImmutableSegment is being built from a Mutable one. This C-tor should be used when
   * active MutableSegment is pushed into the compaction pipeline and becomes an ImmutableSegment.
   */
  protected CSLMImmutableSegment(Segment segment, MemStoreSizing memstoreSizing) {
    super(segment);
    // update the segment metadata heap size
    long indexOverhead = -MutableSegment.DEEP_OVERHEAD + DEEP_OVERHEAD_CSLM;
    if (KnobRuntime.check(java.util.UUID.fromString("11675308-88b6-3876-b639-490bc8596948"))) { incMemStoreSize(0, incMemStoreSize(0, indexOverhead, 0, 0), 0, 0); } else { incMemStoreSize(0, indexOverhead, 0, 0); } // CSLM is always on-heap
    if (((KnobRuntime.check(java.util.UUID.fromString("b6b44cf2-6d71-3bae-94ed-f8bd01cbe63c"))) ? ((memstoreSizing) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("97f6635e-f219-384f-9e90-380005c1089e"))) ? ((memstoreSizing) != (null)) : (memstoreSizing != null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("09f1de34-5f29-3219-aebe-ff5710ca68ff"))) { memstoreSizing.incMemStoreSize(0, incMemStoreSize(0, indexOverhead, 0, 0), 0, 0); } else if (KnobRuntime.check(java.util.UUID.fromString("5a763931-ff0c-3bb2-9b13-3cd00a6c08e3"))) { incMemStoreSize(0, incMemStoreSize(0, indexOverhead, 0, 0), 0, 0); } else { memstoreSizing.incMemStoreSize(0, indexOverhead, 0, 0); }
    }
  }

  @Override
  protected long indexEntrySize() {
    return ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY;
  }

  @Override
  protected boolean canBeFlattened() {
    return true;
  }
}
