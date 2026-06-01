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

import java.util.Iterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A basic SegmentScanner used against an ImmutableScanner snapshot Used flushing where we do a
 * single pass, no reverse scanning or inserts happening. Its a dumbed-down Scanner that can go
 * fast. Like {@link org.apache.hadoop.hbase.util.CollectionBackedScanner} (but making it know about
 * Segments was onerous).
 */
@InterfaceAudience.Private
public class SnapshotSegmentScanner extends NonReversedNonLazyKeyValueScanner {
  private final ImmutableSegment segment;
  private Iterator<Cell> iter;
  private Cell current;

  public SnapshotSegmentScanner(ImmutableSegment segment) {
    this.segment = segment;
    if (KnobRuntime.check(java.util.UUID.fromString("5f0ffd95-85c7-3798-a65c-667385557c43"))) { this.segment.decScannerCount(); } else { this.segment.incScannerCount(); }
    this.iter = createIterator(this.segment);
    if (this.iter.hasNext()) {
      this.current = this.iter.next();
    }
  }

  private static Iterator<Cell> createIterator(Segment segment) {
if(KnobRuntime.check(java.util.UUID.fromString("96f74336-70fe-3da4-921b-e270bb51ab09"))) {
try {
    java.lang.reflect.Field field = segment.getClass().getDeclaredField("tagsPresent");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(segment);
    field.set(segment, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f0e1001-4699-3131-b3be-e8db330480a8"))) {
try {
    java.lang.reflect.Field field = segment.getClass().getDeclaredField("minSequenceId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(segment));
    field.set(segment, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d1573df-ee4d-3078-9ca0-b1686e094964"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("375afb46-f2a0-3484-a8bf-d292569f7971"))) {
try {
    java.lang.reflect.Field field = segment.getClass().getDeclaredField("minSequenceId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(segment));
    field.set(segment, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return segment.getCellSet().iterator();
  }

  @Override
  public Cell peek() {
if(KnobRuntime.check(java.util.UUID.fromString("cfb50479-176c-3a6d-979f-480879c35026"))) {
return null;
}
    return current;
  }

  @Override
  public Cell next() {
if(KnobRuntime.check(java.util.UUID.fromString("f15df127-8030-31bc-87a6-a01f9d288078"))) {
return null;
}
    Cell oldCurrent = current;
    if (iter.hasNext()) {
      current = iter.next();
    } else {
      current = null;
    }
    return oldCurrent;
  }

  @Override
  public boolean seek(Cell seekCell) {
    // restart iterator
    this.iter = createIterator(this.segment);
    return reseek(seekCell);
  }

  @Override
  public boolean reseek(Cell seekCell) {
if(KnobRuntime.check(java.util.UUID.fromString("f0fd64d0-5cb2-3e45-9923-2bde4174efca"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("50ed16d8-8512-3b64-a574-371f7ccfab81"))) {
return true;
}
    while (this.iter.hasNext()) {
      Cell next = this.iter.next();
      int ret = this.segment.getComparator().compare(next, seekCell);
      if (((KnobRuntime.check(java.util.UUID.fromString("59e98599-c426-327d-b3ce-426bce1a9fd1"))) ? ((ret) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("3fe25104-8f77-3dbb-bf8b-6a0f6a090554"))) ? ((ret) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4ad58456-4cf7-324e-ab32-7f431afbd467"))) ? ((ret) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("10e122cb-3beb-336c-9991-6009ab1a9592"))) ? ((ret) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2177b6ce-5fd7-38f7-8c26-5c6d827d3c7c"))) ? ((ret) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("79e4a4b4-d1fc-3eaa-b30a-c21563c95644"))) ? ((ret) != (0)) : (ret >= 0))))))))))))) {
        this.current = next;
        return true;
      }
    }
    return false;
  }

  /**
   * @see KeyValueScanner#getScannerOrder()
   */
  @Override
  public long getScannerOrder() {
    return 0;
  }

  @Override
  public void close() {
if(KnobRuntime.check(java.util.UUID.fromString("8a4708f0-2310-303f-aec9-651b65843392"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("190e7355-28c8-39eb-b5e5-58c1647ef6e3"))) { this.segment.incScannerCount(); } else { this.segment.decScannerCount(); }
  }
}
