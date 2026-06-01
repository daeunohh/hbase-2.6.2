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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A singleton store segment factory. Generate concrete store segments.
 */
@InterfaceAudience.Private
public final class SegmentFactory {

  private SegmentFactory() {
  }

  private static SegmentFactory instance = new SegmentFactory();

  public static SegmentFactory instance() {
    return instance;
  }

  // create composite immutable segment from a list of segments
  // for snapshot consisting of multiple segments
  public CompositeImmutableSegment createCompositeImmutableSegment(final CellComparator comparator,
    List<ImmutableSegment> segments) {
    return new CompositeImmutableSegment(comparator, segments);
  }

  // create new flat immutable segment from compacting old immutable segments
  // for compaction
  public ImmutableSegment createImmutableSegmentByCompaction(final Configuration conf,
    final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells,
    CompactingMemStore.IndexType idxType, MemStoreCompactionStrategy.Action action)
    throws IOException {

    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
    return createImmutableSegment(conf, comparator, iterator, memStoreLAB, numOfCells, action,
      idxType);
  }

  /**
   * create empty immutable segment for initializations This ImmutableSegment is used as a place
   * holder for snapshot in Memstore. It won't flush later, So it is not necessary to record the
   * initial size for it.
   * @param comparator comparator
   */
  public ImmutableSegment createImmutableSegment(CellComparator comparator) {
    MutableSegment segment = generateMutableSegment(null, comparator, null, null);
    return createImmutableSegment(segment, null);
  }

  // create not-flat immutable segment from mutable segment
  public ImmutableSegment createImmutableSegment(MutableSegment segment,
    MemStoreSizing memstoreSizing) {
    return new CSLMImmutableSegment(segment, memstoreSizing);
  }

  // create mutable segment
  public MutableSegment createMutableSegment(final Configuration conf, CellComparator comparator,
    MemStoreSizing memstoreSizing) {
    MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
if(KnobRuntime.check(java.util.UUID.fromString("5861056a-0b57-371f-9bb2-7d456ea80abd"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("CHUNK_SIZE_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9a0230e5-c70f-3000-bff6-4bd7df5c8d38"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("CHUNK_SIZE_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27aa0c42-cc63-34ef-b6cf-6200b4fc4835"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd4403ee-93e5-3926-a7ad-d81ba0c28222"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("MAX_ALLOC_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f26ee875-74f2-3f0a-a7c0-974c258316fa"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("CHUNK_SIZE_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e7c3f32-36eb-3039-8cd1-9fb80f94f138"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("CHUNK_SIZE_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d9ca040-ab0a-3d29-9c36-e264abaa69ae"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("MAX_ALLOC_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a8cfa6d-e4fd-3bc2-a568-8a576a0c0261"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("MAX_ALLOC_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e14b9400-9080-36e0-9ab5-942c872352bb"))) {
try {
    java.lang.reflect.Field field = memStoreLAB.getClass().getDeclaredField("MAX_ALLOC_DEFAULT");
    field.setAccessible(true);
    int oldValue = ((int)field.get(memStoreLAB));
    field.set(memStoreLAB, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return generateMutableSegment(conf, comparator, memStoreLAB, memstoreSizing);
  }

  // create new flat immutable segment from merging old immutable segments
  // for merge
  public ImmutableSegment createImmutableSegmentByMerge(final Configuration conf,
    final CellComparator comparator, MemStoreSegmentsIterator iterator, int numOfCells,
    List<ImmutableSegment> segments, CompactingMemStore.IndexType idxType,
    MemStoreCompactionStrategy.Action action) throws IOException {

    MemStoreLAB memStoreLAB = getMergedMemStoreLAB(conf, segments);
    return createImmutableSegment(conf, comparator, iterator, memStoreLAB, numOfCells, action,
      idxType);

  }

  // create flat immutable segment from non-flat immutable segment
  // for flattening
  public ImmutableSegment createImmutableSegmentByFlattening(CSLMImmutableSegment segment,
    CompactingMemStore.IndexType idxType, MemStoreSizing memstoreSizing,
    MemStoreCompactionStrategy.Action action) {
    ImmutableSegment res = null;
    switch (idxType) {
      case CHUNK_MAP:
        res = new CellChunkImmutableSegment(segment, memstoreSizing, action);
        break;
      case CSLM_MAP:
        assert false; // non-flat segment can not be the result of flattening
        break;
      case ARRAY_MAP:
        res = new CellArrayImmutableSegment(segment, memstoreSizing, action);
        break;
    }
    return res;
  }

  // ****** private methods to instantiate concrete store segments **********//
  private ImmutableSegment createImmutableSegment(final Configuration conf,
    final CellComparator comparator, MemStoreSegmentsIterator iterator, MemStoreLAB memStoreLAB,
    int numOfCells, MemStoreCompactionStrategy.Action action,
    CompactingMemStore.IndexType idxType) {

    ImmutableSegment res = null;
    switch (idxType) {
      case CHUNK_MAP:
        res = new CellChunkImmutableSegment(comparator, iterator, memStoreLAB, numOfCells, action);
        break;
      case CSLM_MAP:
        assert false; // non-flat segment can not be created here
        break;
      case ARRAY_MAP:
        res = new CellArrayImmutableSegment(comparator, iterator, memStoreLAB, numOfCells, action);
        break;
    }
    return res;
  }

  private MutableSegment generateMutableSegment(final Configuration conf, CellComparator comparator,
    MemStoreLAB memStoreLAB, MemStoreSizing memstoreSizing) {
    // TBD use configuration to set type of segment
    CellSet set = new CellSet(comparator);
    return new MutableSegment(set, comparator, memStoreLAB, memstoreSizing);
  }

  private MemStoreLAB getMergedMemStoreLAB(Configuration conf, List<ImmutableSegment> segments) {
    List<MemStoreLAB> mslabs = new ArrayList<>();
    if (!conf.getBoolean(MemStoreLAB.USEMSLAB_KEY, MemStoreLAB.USEMSLAB_DEFAULT)) {
      return null;
    }
    for (ImmutableSegment segment : segments) {
      mslabs.add(segment.getMemStoreLAB());
    }
    return new ImmutableMemStoreLAB(mslabs);
  }
}
